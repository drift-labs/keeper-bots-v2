import { SlotSubscriber } from '@drift-labs/sdk';
import {
	Connection,
	Keypair,
	LAMPORTS_PER_SOL,
	PublicKey,
	VersionedTransaction,
} from '@solana/web3.js';
import {
	SearcherClient,
	searcherClient,
} from 'jito-ts/dist/sdk/block-engine/searcher';
import { Bundle } from 'jito-ts/dist/sdk/block-engine/types';
import { logger } from './logger';
import { BundleResult } from 'jito-ts/dist/gen/block-engine/bundle';
import { LRUCache } from 'lru-cache';
import WebSocket from 'ws';
import { bs58 } from '@project-serum/anchor/dist/cjs/utils/bytes';

export const jitoBundlePriceEndpoint =
	'ws://bundles-api-rest.jito.wtf/api/v1/bundles/tip_stream';

const logPrefix = '[BundleSender]';
const MS_DELAY_BEFORE_CHECK_INCLUSION = 30_000;
const MAX_TXS_TO_CHECK = 50;

export type TipStream = {
	time: string;
	ts: number; // millisecond timestamp
	landed_tips_25th_percentile: number; // in SOL
	landed_tips_50th_percentile: number; // in SOL
	landed_tips_75th_percentile: number; // in SOL
	landed_tips_95th_percentile: number; // in SOL
	landed_tips_99th_percentile: number; // in SOL
	ema_landed_tips_50th_percentile: number; // in SOL
};

export type DropReason = 'pruned' | 'blockhash_expired' | 'blockhash_not_found';

export type BundleStats = {
	accepted: number;
	stateAuctionBidRejected: number;
	winningBatchBidRejected: number;
	simulationFailure: number;
	internalError: number;
	droppedBundle: number;

	/// extra stats
	droppedPruned: number;
	droppedBlockhashExpired: number;
	droppedBlockhashNotFound: number;
};

export class BundleSender {
	private ws: WebSocket | undefined;
	private searcherClient: SearcherClient;
	private leaderScheduleIntervalId: NodeJS.Timeout | undefined;
	private checkSentTxsIntervalId: NodeJS.Timeout | undefined;
	private isSubscribed = false;
	private shuttingDown = false;
	private jitoTipAccounts: PublicKey[] = [];
	private nextJitoLeader?: {
		currentSlot: number;
		nextLeaderSlot: number;
		nextLeaderIdentity: string;
	};
	private updatingJitoSchedule = false;
	private checkingSentTxs = false;

	// if there is a big difference, probably jito ws connection is bad, should resub
	private bundlesSent = 0;
	private bundleResultsReceived = 0;

	// `bundleIdToTx` will be populated immediately after sending a bundle.
	private bundleIdToTx: LRUCache<string, { tx: string; ts: number }>;
	// `sentTxCache` will only be populated after a bundle result is received.
	// reason being that sometimes results come really late (like minutes after sending)
	// unsure if this is a jito issue or this bot is inefficient and holding onto things
	// for that long. Check txs from this map to see if they landed.
	private sentTxCache: LRUCache<string, number>;

	/// -1 for each accepted bundle, +1 for each rejected (due to bid, don't count sim errors).
	private failBundleCount = 0;
	private countLandedBundles = 0;
	private countDroppedbundles = 0;

	private lastTipStream: TipStream | undefined;
	private bundleStats: BundleStats = {
		accepted: 0,
		stateAuctionBidRejected: 0,
		winningBatchBidRejected: 0,
		simulationFailure: 0,
		internalError: 0,
		droppedBundle: 0,

		// custom stats
		droppedPruned: 0,
		droppedBlockhashExpired: 0,
		droppedBlockhashNotFound: 0,
	};

	constructor(
		private connection: Connection,
		jitoBlockEngineUrl: string,
		jitoAuthKeypair: Keypair,
		private tipPayerKeypair: Keypair,
		private slotSubscriber: SlotSubscriber,

		/// tip algo params
		public strategy: 'non-jito-only' | 'jito-only' | 'hybrid' = 'jito-only',
		private minBundleTip = 10_000, // cant be lower than this
		private maxBundleTip = 100_000,
		private maxFailBundleCount = 100, // at 100 failed txs, can expect tip to become maxBundleTip
		private tipMultiplier = 3 // bigger == more superlinear, delay the ramp up to prevent overpaying too soon
	) {
		this.searcherClient = searcherClient(jitoBlockEngineUrl, jitoAuthKeypair);
		this.bundleIdToTx = new LRUCache({
			max: 500,
		});
		this.sentTxCache = new LRUCache({
			max: 500,
		});
	}

	slotsUntilNextLeader(): number | undefined {
		if (!this.nextJitoLeader) {
			return undefined;
		}
		return this.nextJitoLeader.nextLeaderSlot - this.slotSubscriber.getSlot();
	}

	getBundleStats(): BundleStats {
		return this.bundleStats;
	}

	getTipStream(): TipStream | undefined {
		return this.lastTipStream;
	}

	getBundleFailCount(): number {
		return this.failBundleCount;
	}

	getLandedCount(): number {
		return this.countLandedBundles;
	}

	getDroppedCount(): number {
		return this.countDroppedbundles;
	}

	private incRunningBundleScore(amount = 1) {
		this.failBundleCount =
			(this.failBundleCount + amount) % this.maxFailBundleCount;
	}

	private decRunningBundleScore(amount = 1) {
		this.failBundleCount = Math.max(this.failBundleCount - amount, 0);
	}

	private handleBundleResult(bundleResult: BundleResult) {
		const bundleId = bundleResult.bundleId;
		const bundlePayload = this.bundleIdToTx.get(bundleId);
		if (!bundlePayload) {
			logger.error(
				`${logPrefix}: got bundle result for unknown bundleId: ${bundleId}`
			);
		}
		const now = Date.now();

		// Update bundle score. -1 for accept, +1 for reject.
		// Large score === many failures.
		// If the count is 0, we pay min tip, if max, we pay max tip.
		if (bundleResult.accepted !== undefined) {
			this.bundleStats.accepted++;
			if (bundlePayload !== undefined) {
				if (now > bundlePayload.ts + MS_DELAY_BEFORE_CHECK_INCLUSION) {
					logger.error(
						`${logPrefix}: received a bundle result ${MS_DELAY_BEFORE_CHECK_INCLUSION} ms after sending. tx: ${bundlePayload.tx}, sent at: ${bundlePayload.ts}`
					);
				} else {
					this.sentTxCache.set(bundlePayload.tx, bundlePayload.ts);
				}
			}
		} else if (bundleResult.rejected !== undefined) {
			if (bundleResult.rejected.droppedBundle !== undefined) {
				this.bundleStats.droppedBundle++;
				const msg = bundleResult.rejected.droppedBundle.msg;
				if (msg.includes('pruned at slot')) {
					this.bundleStats.droppedPruned++;
				} else if (msg.includes('blockhash has expired')) {
					this.bundleStats.droppedBlockhashExpired++;
				} else if (msg.includes('Blockhash not found')) {
					this.bundleStats.droppedBlockhashNotFound++;
				}
			} else if (bundleResult.rejected.internalError !== undefined) {
				this.bundleStats.internalError++;
			} else if (bundleResult.rejected.simulationFailure !== undefined) {
				this.bundleStats.simulationFailure++;
			} else if (bundleResult.rejected.stateAuctionBidRejected !== undefined) {
				this.bundleStats.stateAuctionBidRejected++;
			} else if (bundleResult.rejected.winningBatchBidRejected !== undefined) {
				this.bundleStats.winningBatchBidRejected++;
			}
		}
	}
	private connectJitoTipStream() {
		if (this.ws !== undefined) {
			logger.warn(
				`${logPrefix} Called connectJitoTipStream but this.ws is already connected, disconnecting it...`
			);
			this.ws.close();
			return;
		}

		this.ws = new WebSocket(jitoBundlePriceEndpoint);
		this.bundlesSent = 0;
		this.bundleResultsReceived = 0;

		this.ws.on('message', (data: string) => {
			const tipStream = JSON.parse(data) as Array<TipStream>;
			if (tipStream.length > 0) {
				tipStream[0].ts = new Date(tipStream[0].time).getTime();
				this.lastTipStream = tipStream[0];
			}
		});
		this.ws.on('close', () => {
			logger.info(
				`${logPrefix}: jito ws closed ${
					this.shuttingDown ? 'shutting down...' : 'reconnecting in 5s...'
				}`
			);
			this.ws = undefined;
			if (!this.shuttingDown) {
				setTimeout(this.connectJitoTipStream.bind(this), 5000);
			}
		});
		this.ws.on('error', (e) => {
			logger.error(`${logPrefix}: jito ws error: ${JSON.stringify(e)}`);
		});
	}

	async subscribe() {
		if (this.isSubscribed) {
			return;
		}
		this.isSubscribed = true;
		this.shuttingDown = false;

		const tipAccounts = await this.searcherClient.getTipAccounts();
		this.jitoTipAccounts = tipAccounts.map((k) => new PublicKey(k));

		this.searcherClient.onBundleResult(
			(bundleResult: BundleResult) => {
				logger.debug(
					`${logPrefix}: got bundle result:\n${JSON.stringify(bundleResult)}`
				);
				this.bundleResultsReceived++;
				this.handleBundleResult(bundleResult);
			},
			(e) => {
				const err = e as Error;
				logger.error(
					`${logPrefix}: error getting bundle result: ${err.message}: ${e.stack}`
				);
			}
		);

		this.connectJitoTipStream();

		this.slotSubscriber.eventEmitter.on(
			'newSlot',
			this.onSlotSubscriberSlot.bind(this)
		);
		this.leaderScheduleIntervalId = setInterval(
			this.updateJitoLeaderSchedule.bind(this),
			1000
		);
		this.checkSentTxsIntervalId = setInterval(
			this.checkSentTxs.bind(this),
			10000
		);
	}

	async unsubscribe() {
		if (!this.isSubscribed) {
			return;
		}
		this.shuttingDown = true;

		if (this.ws) {
			this.ws.close();
		}

		if (this.leaderScheduleIntervalId) {
			clearInterval(this.leaderScheduleIntervalId);
			this.leaderScheduleIntervalId = undefined;
		}

		if (this.checkSentTxsIntervalId) {
			clearInterval(this.checkSentTxsIntervalId);
			this.checkSentTxsIntervalId = undefined;
		}

		this.isSubscribed = false;
	}

	private async onSlotSubscriberSlot(slot: number) {
		if (this.nextJitoLeader) {
			const jitoSlot = this.nextJitoLeader.nextLeaderSlot;
			if (slot >= jitoSlot) {
				if (slot >= jitoSlot - 2) {
					logger.info(
						`${logPrefix}: currSlot: ${slot}, next jito leader slot: ${jitoSlot} (in ${
							jitoSlot - slot
						} slots)`
					);
					this.updateJitoLeaderSchedule();
				}
			}
		}
	}

	private async checkSentTxs() {
		if (this.checkingSentTxs) {
			return;
		}
		this.checkingSentTxs = true;
		try {
			// find txs in cache ready to check
			const now = Date.now();
			const txs = [];
			for (const [tx, ts] of this.sentTxCache.entries()) {
				if (now - ts > MS_DELAY_BEFORE_CHECK_INCLUSION) {
					this.sentTxCache.delete(tx);
					txs.push(tx);
				}
				if (txs.length >= MAX_TXS_TO_CHECK) {
					break;
				}
			}
			if (txs.length === 0) {
				logger.info(`${logPrefix} no txs to check...`);
			} else {
				const resps = await this.connection.getTransactions(txs, {
					commitment: 'confirmed',
					maxSupportedTransactionVersion: 0,
				});
				const droppedTxs = resps.filter((tx) => tx === null);
				const landedTxs = resps.filter((tx) => tx !== null);

				this.countDroppedbundles += droppedTxs.length;
				this.countLandedBundles += landedTxs.length;
				const countBefore = this.failBundleCount;
				this.incRunningBundleScore(droppedTxs.length);
				this.decRunningBundleScore(landedTxs.length);

				logger.info(
					`${logPrefix} Found ${droppedTxs.length} txs dropped, ${landedTxs.length} landed landed, failCount: ${countBefore} -> ${this.failBundleCount}`
				);
			}
		} catch (e) {
			const err = e as Error;
			logger.error(
				`${logPrefix}: error checking sent txs: ${err.message}. ${err.stack}`
			);
		} finally {
			this.checkingSentTxs = false;
			logger.info(
				`${logPrefix}: running fail count: ${
					this.failBundleCount
				}, totalLandedTxs: ${this.countLandedBundles}, totalDroppedTxs: ${
					this.countDroppedbundles
				}, currentTipAmount: ${this.calculateCurrentTipAmount()}, lastJitoTipStream: ${JSON.stringify(
					this.lastTipStream
				)} bundle stats: ${JSON.stringify(this.bundleStats)}`
			);
		}
	}

	private async updateJitoLeaderSchedule() {
		if (this.updatingJitoSchedule) {
			return;
		}
		this.updatingJitoSchedule = true;
		try {
			this.nextJitoLeader = await this.searcherClient.getNextScheduledLeader();
		} catch (e) {
			const err = e as Error;
			logger.error(
				`${logPrefix}: error checking jito leader schedule: ${err.message}`
			);
		} finally {
			this.updatingJitoSchedule = false;
		}
	}

	/**
	 *
	 * @returns current tip based on running score
	 */
	calculateCurrentTipAmount() {
		return Math.floor(
			Math.max(
				this.lastTipStream?.landed_tips_25th_percentile ?? 0 * LAMPORTS_PER_SOL,
				this.minBundleTip,
				Math.min(
					this.maxBundleTip,
					Math.pow(
						this.failBundleCount / this.maxFailBundleCount,
						this.tipMultiplier
					) * this.maxBundleTip
				)
			)
		);
	}

	async sendTransactions(
		signedTxs: VersionedTransaction[],
		metadata?: string,
		txSig?: string
	) {
		if (signedTxs.length + 1 > 5) {
			throw new Error(`jito max bundle size is 5, got ${signedTxs.length + 1}`);
		}

		if (!this.isSubscribed) {
			logger.warn(
				`${logPrefix} You should call bundleSender.subscribe() before sendTransaction()`
			);
			await this.subscribe();
			return;
		}

		if (this.bundlesSent - this.bundleResultsReceived > 100) {
			logger.warn(
				`${logPrefix} sent ${this.bundlesSent} bundles but only redeived ${this.bundleResultsReceived} results, disconnecting jito ws...`
			);
			this.ws?.close();
			return;
		}
		this.bundlesSent++;

		// +1 for tip tx, jito max is 5
		let b: Bundle | Error = new Bundle(
			signedTxs,
			Math.max(signedTxs.length + 1, 5)
		);

		const tipAccountToUse =
			this.jitoTipAccounts[
				Math.floor(Math.random() * this.jitoTipAccounts.length)
			];

		b = b.addTipTx(
			this.tipPayerKeypair!,
			this.calculateCurrentTipAmount(),
			tipAccountToUse!,
			signedTxs[0].message.recentBlockhash
		);
		if (b instanceof Error) {
			logger.error(`${logPrefix} failed to attach tip: ${b.message})`);
			return;
		}

		try {
			// txSig used for tracking purposes, if none provided, use the sig of the first tx in the bundle
			if (!txSig) {
				txSig = bs58.encode(signedTxs[0].signatures[0]);
			}
			const bundleId = await this.searcherClient.sendBundle(b);
			const ts = Date.now();
			this.bundleIdToTx.set(bundleId, { tx: txSig, ts });
			logger.info(
				`${logPrefix} sent bundle with uuid ${bundleId} (tx: ${txSig}: ${ts}) ${metadata}`
			);
		} catch (e) {
			const err = e as Error;
			logger.error(
				`${logPrefix} failed to send bundle: ${err.message}. ${err.stack}`
			);
		}
	}
}

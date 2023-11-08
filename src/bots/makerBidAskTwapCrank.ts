import {
	DLOB,
	DriftClient,
	UserMap,
	SlotSubscriber,
	MarketType,
	PositionDirection,
	getUserStatsAccountPublicKey,
	promiseTimeout,
} from '@drift-labs/sdk';
import { Mutex } from 'async-mutex';

import { logger } from '../logger';
import { Bot } from '../types';
import { BaseBotConfig } from '../config';
import {
	TransactionSignature,
	VersionedTransaction,
	AddressLookupTableAccount,
	PublicKey,
} from '@solana/web3.js';
import { webhookMessage } from '../webhook';
import { ConfirmOptions, Signer } from '@solana/web3.js';

const CRANK_TX_MARKET_CHUNK_SIZE = 2;

export async function sendVersionedTransaction(
	driftClient: DriftClient,
	tx: VersionedTransaction,
	additionalSigners?: Array<Signer>,
	opts?: ConfirmOptions,
	timeoutMs = 5000
): Promise<TransactionSignature | null> {
	// @ts-ignore
	tx.sign((additionalSigners ?? []).concat(driftClient.provider.wallet.payer));

	if (opts === undefined) {
		opts = driftClient.provider.opts;
	}

	const rawTransaction = tx.serialize();
	let txid: TransactionSignature | null;
	try {
		txid = await promiseTimeout(
			driftClient.provider.connection.sendRawTransaction(rawTransaction, opts),
			timeoutMs
		);
	} catch (e) {
		console.error(e);
		throw e;
	}

	return txid;
}

/// split the list into chunks
function chunkArray<T>(arr: T[], chunkSize: number): T[][] {
	const chunks: T[][] = [];
	for (let i = 0; i < arr.length; i += chunkSize) {
		chunks.push(arr.slice(i, i + chunkSize));
	}
	return chunks;
}

export class MakerBidAskTwapCrank implements Bot {
	public readonly name: string;
	public readonly dryRun: boolean;
	public readonly runOnce: boolean;
	public readonly defaultIntervalMs?: number = undefined;

	private crankIntervalToMarketIds?: { [key: number]: number[] }; // Object from number to array of numbers
	private allCrankIntervalGroups?: number[];
	private maxIntervalGroup?: number; // tracks the max interval group for health checking

	private slotSubscriber: SlotSubscriber;
	private driftClient: DriftClient;
	private intervalIds: Array<NodeJS.Timer> = [];
	private userMap?: UserMap;

	private dlob?: DLOB;
	private latestDlobSlot?: number;
	private lookupTableAccount?: AddressLookupTableAccount;

	private watchdogTimerMutex = new Mutex();
	private watchdogTimerLastPatTime = Date.now();

	constructor(
		driftClient: DriftClient,
		slotSubscriber: SlotSubscriber,
		userMap: UserMap,
		config: BaseBotConfig,
		runOnce: boolean,
		crankIntervalToMarketIds?: { [key: number]: number[] }
	) {
		this.slotSubscriber = slotSubscriber;
		this.name = config.botId;
		this.dryRun = config.dryRun;
		this.runOnce = runOnce;
		this.driftClient = driftClient;
		this.userMap = userMap;
		this.crankIntervalToMarketIds = crankIntervalToMarketIds;
		if (this.crankIntervalToMarketIds) {
			this.allCrankIntervalGroups = Object.keys(
				this.crankIntervalToMarketIds
			).map((x) => parseInt(x));
			this.maxIntervalGroup = Math.max(...this.allCrankIntervalGroups!);
		}
	}

	public async init() {
		logger.info(`${this.name} initing, runOnce: ${this.runOnce}`);
		this.lookupTableAccount =
			await this.driftClient.fetchMarketLookupTableAccount();
	}

	public async reset() {
		for (const intervalId of this.intervalIds) {
			clearInterval(intervalId as NodeJS.Timeout);
		}
		this.intervalIds = [];

		await this.userMap?.unsubscribe();
	}

	public async startIntervalLoop(_intervalMs?: number): Promise<void> {
		logger.info(`${this.name} Bot started!`);
		if (this.runOnce) {
			await this.tryTwapCrank(null);
		} else {
			// start an interval for each crank interval
			for (const intervalGroup of this.allCrankIntervalGroups!) {
				const intervalId = setInterval(
					this.tryTwapCrank.bind(this, intervalGroup),
					intervalGroup
				);
				this.intervalIds.push(intervalId);
			}
		}
	}

	public async healthCheck(): Promise<boolean> {
		let healthy = false;
		await this.watchdogTimerMutex.runExclusive(async () => {
			healthy =
				this.watchdogTimerLastPatTime > Date.now() - 2 * this.maxIntervalGroup!;
		});
		return healthy;
	}

	private async initDlob() {
		try {
			this.latestDlobSlot = this.slotSubscriber.currentSlot;
			this.dlob = await this.userMap!.getDLOB(this.slotSubscriber.currentSlot);
		} catch (e) {
			logger.error(`Error loading dlob: ${e}`);
		}
	}

	private getCombinedList(makersArray: PublicKey[]) {
		const combinedList = [];

		for (const maker of makersArray) {
			const uA = this.userMap!.getUserAuthority(maker.toString());
			if (uA !== undefined) {
				const uStats = getUserStatsAccountPublicKey(
					this.driftClient.program.programId,
					uA
				);

				// Combine maker and uStats into a list and add it to the combinedList
				const combinedItem = [maker, uStats];
				combinedList.push(combinedItem);
			} else {
				logger.warn(
					'skipping maker... cannot find authority for userAccount=',
					maker.toString()
				);
			}
		}

		return combinedList;
	}

	private async tryTwapCrank(intervalGroup: number | null) {
		try {
			await this.initDlob();

			const state = this.driftClient.getStateAccount();

			let crankMarkets: number[] = [];
			if (intervalGroup === null) {
				crankMarkets = Array.from(
					{ length: state.numberOfMarkets },
					(_, index) => index
				);
			} else {
				crankMarkets = this.crankIntervalToMarketIds![intervalGroup]!;
			}
			logger.info(`Cranking interval group ${intervalGroup}: ${crankMarkets}`);

			const allChunks: number[][] = chunkArray(
				crankMarkets,
				CRANK_TX_MARKET_CHUNK_SIZE
			);

			for (const chunkOfMarketIndicies of allChunks) {
				const ixs = [];
				for (let i = 0; i < chunkOfMarketIndicies.length; i++) {
					const mi = chunkOfMarketIndicies[i];

					const oraclePriceData =
						this.driftClient.getOracleDataForPerpMarket(mi);

					const bidMakers = this.dlob!.getBestMakers({
						marketIndex: mi,
						marketType: MarketType.PERP,
						direction: PositionDirection.LONG,
						slot: this.latestDlobSlot!,
						oraclePriceData,
						numMakers: 5,
					});

					const askMakers = this.dlob!.getBestMakers({
						marketIndex: mi,
						marketType: MarketType.PERP,
						direction: PositionDirection.LONG,
						slot: this.latestDlobSlot!,
						oraclePriceData,
						numMakers: 5,
					});
					logger.info(
						`loaded makers for market ${mi}: ${bidMakers.length} bids, ${askMakers.length} asks`
					);

					const concatenatedList = [
						...this.getCombinedList(bidMakers),
						...this.getCombinedList(askMakers),
					];

					const ix = await this.driftClient.getUpdatePerpBidAskTwapIx(
						mi,
						concatenatedList as [PublicKey, PublicKey][]
					);

					ixs.push(ix);
				}

				try {
					const txSig = await sendVersionedTransaction(
						this.driftClient,
						await this.driftClient.txSender.getVersionedTransaction(
							ixs,
							[this.lookupTableAccount!],
							[],
							this.driftClient.opts
						),
						[],
						this.driftClient.opts,
						5000
					);

					logger.info(`https://solscan.io/tx/${txSig}`);
				} catch (e: any) {
					console.error(e);
					await webhookMessage(
						`[${this.name}] failed to crank funding rate:\n${
							e.logs ? (e.logs as Array<string>).join('\n') : ''
						} \n${e.stack ? e.stack : e.message}`
					);
				}
			}
		} catch (e) {
			console.error(e);
			if (e instanceof Error) {
				await webhookMessage(
					`[${this.name}]: :x: uncaught error:\n${
						e.stack ? e.stack : e.message
					}`
				);
			}
		} finally {
			logger.info('tryTwapCrank finished');
			await this.watchdogTimerMutex.runExclusive(async () => {
				this.watchdogTimerLastPatTime = Date.now();
			});
		}
	}
}

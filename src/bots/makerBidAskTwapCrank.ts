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
import { ConfirmOptions, Signer } from '@solana/web3.js';

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

export class MakerBidAskTwapCrank implements Bot {
	public readonly name: string;
	public readonly dryRun: boolean;
	public readonly runOnce: boolean;
	public readonly defaultIntervalMs: number = 60000 * 5; // once every 5 minute

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
		runOnce: boolean
	) {
		this.slotSubscriber = slotSubscriber;
		this.name = config.botId;
		this.dryRun = config.dryRun;
		this.runOnce = runOnce;
		this.driftClient = driftClient;
		this.userMap = userMap;
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

	public async startIntervalLoop(intervalMs: number): Promise<void> {
		logger.info(`${this.name} Bot started!`);
		if (this.runOnce) {
			await this.tryTwapCrank();
		} else {
			const intervalId = setInterval(this.tryTwapCrank.bind(this), intervalMs);
			this.intervalIds.push(intervalId);
		}
	}

	public async healthCheck(): Promise<boolean> {
		let healthy = false;
		await this.watchdogTimerMutex.runExclusive(async () => {
			healthy =
				this.watchdogTimerLastPatTime > Date.now() - 2 * this.defaultIntervalMs;
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

	private async tryTwapCrank() {
		await this.init();
		await this.initDlob();

		const state = await this.driftClient.getStateAccount();

		const crankMarkets: number[] = Array.from(
			{ length: state.numberOfMarkets },
			(_, index) => index
		);

		// Function to split the list into chunks
		function chunkArray<T>(arr: T[], chunkSize: number): T[][] {
			const chunks: T[][] = [];
			for (let i = 0; i < arr.length; i += chunkSize) {
				chunks.push(arr.slice(i, i + chunkSize));
			}
			return chunks;
		}

		// 4 is too large some times with error:
		// SendTransactionError: failed to send transaction: encoded solana_sdk::transaction::versioned::VersionedTransaction too large: 1664 bytes (max: encoded/raw 1644/1232)
		const chunkSize = 3;
		const chunkedLists: number[][] = chunkArray(crankMarkets, chunkSize);

		for (const chunk of chunkedLists) {
			const ixs = [];
			for (let i = 0; i < chunk.length; i++) {
				const mi = chunk[i];

				const oraclePriceData = this.driftClient.getOracleDataForPerpMarket(mi);

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

			const chunkedTx = await promiseTimeout(
				this.driftClient.txSender.getVersionedTransaction(
					ixs,
					[this.lookupTableAccount!],
					[],
					this.driftClient.opts
				),
				5000
			);
			if (chunkedTx === null) {
				logger.error(`Timed out getting versioned Transaction for tx chunk`);
				return;
			}

			try {
				const txSig = await sendVersionedTransaction(
					this.driftClient,
					chunkedTx,
					[],
					// {}, // { skipPreflight: true },
					this.driftClient.opts,
					5000
				);

				logger.info(`https://solscan.io/tx/${txSig}`);
			} catch (e) {
				console.error(e);
			}
		}
	}
}

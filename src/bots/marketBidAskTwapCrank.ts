import {
	DLOB,
	DriftClient,
	DriftEnv,
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
	let txid: TransactionSignature;
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

export class MarketBidAskTwapCrank implements Bot {
	public readonly name: string;
	public readonly dryRun: boolean;
	public readonly runOnce: boolean;
	public readonly defaultIntervalMs: number = 60000 * 5; // once every 5 minute

	private slotSubscriber: SlotSubscriber;
	private driftClient: DriftClient;
	private intervalIds: Array<NodeJS.Timer> = [];
	private userMap: UserMap;
	private driftEnv: DriftEnv;

	private dlob: DLOB;
	private latestDlobSlot: number;
	private lastDlobRefreshTime = 0;
	private lookupTableAccount: AddressLookupTableAccount;
	private pollingIntervalMs: number;

	private watchdogTimerMutex = new Mutex();
	private watchdogTimerLastPatTime = Date.now();

	constructor(
		driftClient: DriftClient,
		slotSubscriber: SlotSubscriber,
		driftEnv: DriftEnv,
		config: BaseBotConfig
	) {
		this.slotSubscriber = slotSubscriber;
		this.name = config.botId;
		this.dryRun = config.dryRun;
		this.runOnce = config.runOnce || false;
		this.driftClient = driftClient;
		this.driftEnv = driftEnv;
	}

	public async init() {
		logger.info(`${this.name} initing`);
		this.lookupTableAccount =
			await this.driftClient.fetchMarketLookupTableAccount();

		// initialize userMap instance
		this.userMap = new UserMap(
			this.driftClient,
			this.driftClient.userAccountSubscriptionConfig,
			false
		);
		await this.userMap.subscribe();

		this.pollingIntervalMs = 1000;
	}

	public async reset() {
		for (const intervalId of this.intervalIds) {
			clearInterval(intervalId);
		}
		this.intervalIds = [];

		await this.userMap.unsubscribe();
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
			this.dlob = await this.userMap.getDLOB(this.slotSubscriber.currentSlot);
			this.lastDlobRefreshTime = Date.now();
		} catch (e) {
			logger.error(`Error loading dlob: ${e}`);
			this.lastDlobRefreshTime = 0;
		}
	}

	private getCombinedList(makersArray) {
		const combinedList = [];

		for (const maker of makersArray) {
			const uA = this.userMap.getUserAuthority(maker.toString());
			if (uA !== undefined) {
				const uStats = getUserStatsAccountPublicKey(
					this.driftClient.program.programId,
					uA
				);

				// Combine maker and uStats into a list and add it to the combinedList
				const combinedItem = [maker, uStats];
				combinedList.push(combinedItem);
			} else {
				console.log(
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
		const chunkSize = 4; // You can change this to any desired chunk size
		const chunkedLists: number[][] = chunkArray(crankMarkets, chunkSize);

		for (const chunk of chunkedLists) {
			const ixs = [];
			for (let i = 0; i < chunk.length; i++) {
				const mi = chunk[i];

				const oraclePriceData = this.driftClient.getOracleDataForPerpMarket(mi);

				const bidMakers = this.dlob.getBestMakers({
					marketIndex: mi,
					marketType: MarketType.PERP,
					direction: PositionDirection.LONG,
					slot: this.latestDlobSlot,
					oraclePriceData,
					numMakers: 5,
				});

				const askMakers = this.dlob.getBestMakers({
					marketIndex: mi,
					marketType: MarketType.PERP,
					direction: PositionDirection.LONG,
					slot: this.latestDlobSlot,
					oraclePriceData,
					numMakers: 5,
				});
				console.log(
					'loaded makers... bid/ask length:',
					bidMakers.length,
					askMakers.length
				);

				const concatenatedList = [
					...this.getCombinedList(bidMakers),
					...this.getCombinedList(askMakers),
				];

				// console.log(concatenatedList);
				// console.log(concatenatedList[0]);
				const ix = await this.driftClient.getUpdatePerpBidAskTwapIx(
					mi,
					concatenatedList
				);

				ixs.push(ix);
			}

			const chunkedTx = await promiseTimeout(
				this.driftClient.txSender.getVersionedTransaction(
					ixs,
					[this.lookupTableAccount],
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

				console.log('txSig:', txSig);
				console.log(`https://solscan.io/tx/${txSig}`);
			} catch (e) {
				console.error(e);
			}
		}
	}
}

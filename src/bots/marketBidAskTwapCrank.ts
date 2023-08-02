import {
	DLOB,
	DriftClient,
	DriftEnv,
	UserMap,
	DLOBOrdersCoder,
	SlotSubscriber,
	MarketType,
	PositionDirection,
	getUserStatsAccountPublicKey,
} from '@drift-labs/sdk';
import { Mutex } from 'async-mutex';

import { logger } from '../logger';
import { Bot } from '../types';
import { BaseBotConfig } from '../config';
import {
	Connection,
	TransactionSignature,
	AddressLookupTableAccount,
} from '@solana/web3.js';

export async function printTxLogs(
	connection: Connection,
	txSig: TransactionSignature
): Promise<void> {
	console.log(
		'tx logs',
		(await connection.getTransaction(txSig, { commitment: 'confirmed' })).meta
			.logMessages
	);
}

export function driftEnvToDlobServerUrl(driftEnv: DriftEnv) {
	switch (driftEnv) {
		case 'devnet':
			return 'https://master.dlob.drift.trade/orders/idlWithSlot';
		case 'mainnet-beta':
			return 'https://dlob.drift.trade/orders/idlWithSlot';
	}
}

export async function loadDlob(
	driftEnv: DriftEnv,
	currentDlob: DLOB,
	currentDlobSlot: number
): Promise<[DLOB, number]> {
	const dlobServerUrl = driftEnvToDlobServerUrl(driftEnv);
	const resp = await fetch(dlobServerUrl);
	if (resp.status !== 200) {
		logger.error(
			`Failed to fetch DLOB IDL with slot from ${dlobServerUrl}: ${resp.statusText}`
		);
		return [currentDlob, currentDlobSlot];
	}

	const dlob = new DLOB();
	const dlobCoder = DLOBOrdersCoder.create();
	const respBody = await resp.json();
	const dlobOrdersBuffer = Buffer.from(respBody['data'], 'base64');
	const dlobOrders = dlobCoder.decode(Buffer.from(dlobOrdersBuffer));
	if (!dlob.initFromOrders(dlobOrders, currentDlobSlot)) {
		logger.error(`Failed to initialize DLOB from orders`);
		return [currentDlob, currentDlobSlot];
	}

	return [dlob, respBody['slot']];
}

export class MarketBidAskTwapCrank implements Bot {
	public readonly name: string;
	public readonly dryRun: boolean;
	public readonly runOnce: boolean;
	public readonly defaultIntervalMs: number = 600000;

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
			[this.dlob, this.latestDlobSlot] = await loadDlob(
				this.driftEnv,
				this.dlob,
				this.latestDlobSlot
			);
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

		const mi = 7;

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

		const txSig = await this.driftClient.updatePerpBidAskTwap(
			mi,
			concatenatedList
		);
		console.log(txSig);

		await printTxLogs(this.driftClient.connection, txSig);
	}
}

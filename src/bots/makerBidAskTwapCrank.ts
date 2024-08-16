import {
	DLOB,
	DriftClient,
	UserMap,
	SlotSubscriber,
	MarketType,
	PositionDirection,
	getUserStatsAccountPublicKey,
	promiseTimeout,
	isVariant,
	PriorityFeeSubscriberMap,
	DriftMarketInfo,
	isOneOfVariant,
	getVariant,
} from '@drift-labs/sdk';
import { Mutex } from 'async-mutex';

import { logger } from '../logger';
import { Bot } from '../types';
import { BaseBotConfig, GlobalConfig } from '../config';
import {
	TransactionSignature,
	VersionedTransaction,
	AddressLookupTableAccount,
	PublicKey,
	ComputeBudgetProgram,
	TransactionInstruction,
	TransactionExpiredBlockheightExceededError,
	SendTransactionError,
} from '@solana/web3.js';
import { webhookMessage } from '../webhook';
import { ConfirmOptions, Signer } from '@solana/web3.js';
import {
	getAllPythOracleUpdateIxs,
	getDriftPriorityFeeEndpoint,
	// getStaleOracleMarketIndexes,
	handleSimResultError,
	simulateAndGetTxWithCUs,
	SimulateAndGetTxWithCUsResponse,
} from '../utils';
import { PythPriceFeedSubscriber } from '../pythPriceFeedSubscriber';
import { PythPullClient } from '@drift-labs/sdk/lib/oracles/pythPullClient';

const CU_EST_MULTIPLIER = 1.4;
const DEFAULT_INTERVAL_GROUP = -1;
const TWAP_CRANK_MIN_CU = 200_000;
const MIN_PRIORITY_FEE = 10_000;

const SLOT_STALENESS_THRESHOLD_RESTART = process.env
	.SLOT_STALENESS_THRESHOLD_RESTART
	? parseInt(process.env.SLOT_STALENESS_THRESHOLD_RESTART) || 300
	: 300;
const TX_LAND_RATE_THRESHOLD = process.env.TX_LAND_RATE_THRESHOLD
	? parseFloat(process.env.TX_LAND_RATE_THRESHOLD) || 0.5
	: 0.5;

function isCriticalError(e: Error): boolean {
	// retrying on this error is standard
	if (e.message.includes('Blockhash not found')) {
		return false;
	}

	if (e.message.includes('Transaction was not confirmed in')) {
		return false;
	}
	return true;
}

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

/**
 * Builds a mapping from crank interval (ms) to perp market indexes
 * @param driftClient
 * @returns
 */
function buildCrankIntervalToMarketIds(driftClient: DriftClient): {
	crankIntervals: { [key: number]: number[] };
	driftMarkets: DriftMarketInfo[];
} {
	const crankIntervals: { [key: number]: number[] } = {};
	const driftMarkets: DriftMarketInfo[] = [];

	for (const perpMarket of driftClient.getPerpMarketAccounts()) {
		driftMarkets.push({
			marketType: 'perp',
			marketIndex: perpMarket.marketIndex,
		});
		let crankPeriodMs = 20_000;
		const isPreLaunch = false;
		if (isOneOfVariant(perpMarket.contractTier, ['a', 'b'])) {
			crankPeriodMs = 10_000;
		} else if (isVariant(perpMarket.amm.oracleSource, 'prelaunch')) {
			crankPeriodMs = 15_000;
		}
		logger.info(
			`Perp market ${perpMarket.marketIndex} contractTier: ${getVariant(
				perpMarket.contractTier
			)} isPrelaunch: ${isPreLaunch}, crankPeriodMs: ${crankPeriodMs}`
		);
		if (crankIntervals[crankPeriodMs] === undefined) {
			crankIntervals[crankPeriodMs] = [perpMarket.marketIndex];
		} else {
			crankIntervals[crankPeriodMs].push(perpMarket.marketIndex);
		}
	}

	return {
		crankIntervals,
		driftMarkets,
	};
}

export class MakerBidAskTwapCrank implements Bot {
	public readonly name: string;
	public readonly dryRun: boolean;
	public readonly runOnce: boolean;
	public readonly defaultIntervalMs?: number = undefined;

	public readonly globalConfig: GlobalConfig;

	private crankIntervalToMarketIds?: { [key: number]: number[] }; // Object from number to array of numbers
	private crankIntervalInProgress?: { [key: number]: boolean };
	private allCrankIntervalGroups?: number[];
	private maxIntervalGroup?: number; // tracks the max interval group for health checking

	private slotSubscriber: SlotSubscriber;
	private driftClient: DriftClient;
	private intervalIds: Array<NodeJS.Timer> = [];
	private userMap?: UserMap;

	private dlob?: DLOB;
	private latestDlobSlot?: number;
	private priorityFeeSubscriberMap?: PriorityFeeSubscriberMap;

	private watchdogTimerMutex = new Mutex();
	private watchdogTimerLastPatTime = Date.now();
	private pythPriceSubscriber?: PythPriceFeedSubscriber;
	private pythPullOracleClient: PythPullClient;
	private pythHealthy = true;
	private lookupTableAccounts: AddressLookupTableAccount[];

	constructor(
		driftClient: DriftClient,
		slotSubscriber: SlotSubscriber,
		userMap: UserMap,
		config: BaseBotConfig,
		globalConfig: GlobalConfig,
		runOnce: boolean,
		pythPriceSubscriber?: PythPriceFeedSubscriber,
		lookupTableAccounts: AddressLookupTableAccount[] = []
	) {
		this.slotSubscriber = slotSubscriber;
		this.name = config.botId;
		this.dryRun = config.dryRun;
		this.runOnce = runOnce;
		this.globalConfig = globalConfig;
		this.driftClient = driftClient;
		this.userMap = userMap;
		this.pythPriceSubscriber = pythPriceSubscriber;
		this.lookupTableAccounts = lookupTableAccounts;
		this.pythPullOracleClient = new PythPullClient(this.driftClient.connection);
	}

	public async init() {
		logger.info(`[${this.name}] initing, runOnce: ${this.runOnce}`);
		this.lookupTableAccounts.push(
			await this.driftClient.fetchMarketLookupTableAccount()
		);

		let driftMarkets: DriftMarketInfo[] = [];
		({ crankIntervals: this.crankIntervalToMarketIds, driftMarkets } =
			buildCrankIntervalToMarketIds(this.driftClient));
		logger.info(
			`[${this.name}] crankIntervals:\n${JSON.stringify(
				this.crankIntervalToMarketIds,
				null,
				2
			)}`
		);

		this.crankIntervalInProgress = {};
		if (this.crankIntervalToMarketIds) {
			this.allCrankIntervalGroups = Object.keys(
				this.crankIntervalToMarketIds
			).map((x) => parseInt(x));
			this.maxIntervalGroup = Math.max(...this.allCrankIntervalGroups!);
			this.watchdogTimerLastPatTime = Date.now() - this.maxIntervalGroup;

			for (const intervalGroup of this.allCrankIntervalGroups!) {
				this.crankIntervalInProgress[intervalGroup] = false;
			}
		} else {
			this.crankIntervalInProgress[DEFAULT_INTERVAL_GROUP] = false;
		}

		this.priorityFeeSubscriberMap = new PriorityFeeSubscriberMap({
			driftPriorityFeeEndpoint: getDriftPriorityFeeEndpoint('mainnet-beta'),
			driftMarkets,
			frequencyMs: 10_000,
		});
		await this.priorityFeeSubscriberMap.subscribe();
	}

	public async reset() {
		for (const intervalId of this.intervalIds) {
			clearInterval(intervalId as NodeJS.Timeout);
		}
		this.intervalIds = [];

		await this.userMap?.unsubscribe();
	}

	public async startIntervalLoop(_intervalMs?: number): Promise<void> {
		logger.info(`[${this.name}] Bot started!`);
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
				this.watchdogTimerLastPatTime > Date.now() - 5 * this.maxIntervalGroup!;
		});
		return healthy && this.pythHealthy;
	}

	private async initDlob() {
		try {
			this.latestDlobSlot = this.slotSubscriber.currentSlot;
			this.dlob = await this.userMap!.getDLOB(this.slotSubscriber.currentSlot);
		} catch (e) {
			logger.error(`[${this.name}] Error loading dlob: ${e}`);
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
					'[${this.name}] skipping maker... cannot find authority for userAccount=',
					maker.toString()
				);
			}
		}

		return combinedList;
	}

	private async sendTx(
		marketIndex: number,
		ixs: TransactionInstruction[]
	): Promise<{ success: boolean; canRetry: boolean }> {
		let simResult: SimulateAndGetTxWithCUsResponse | undefined;
		try {
			const recentBlockhash =
				await this.driftClient.connection.getLatestBlockhash('confirmed');
			simResult = await simulateAndGetTxWithCUs({
				ixs,
				connection: this.driftClient.connection,
				payerPublicKey: this.driftClient.wallet.publicKey,
				lookupTableAccounts: this.lookupTableAccounts,
				cuLimitMultiplier: CU_EST_MULTIPLIER,
				minCuLimit: TWAP_CRANK_MIN_CU,
				doSimulation: true,
				recentBlockhash: recentBlockhash.blockhash,
			});
			logger.info(
				`[${this.name}] estimated ${simResult.cuEstimate} CUs for market: ${marketIndex}`
			);

			if (simResult.simError !== null) {
				logger.error(
					`[${this.name}] Sim error (market: ${marketIndex}): ${JSON.stringify(
						simResult.simError
					)}\n${simResult.simTxLogs ? simResult.simTxLogs.join('\n') : ''}`
				);
				handleSimResultError(simResult, [], `[${this.name}]`);
				return { success: false, canRetry: false };
			} else {
				const sendTxStart = Date.now();
				const txSig = await this.driftClient.txSender.sendVersionedTransaction(
					simResult.tx,
					[],
					{
						...this.driftClient.opts,
					}
				);
				logger.info(
					`[${
						this.name
					}] makerBidAskTwapCrank sent tx for market: ${marketIndex} in ${
						Date.now() - sendTxStart
					}ms tx: https://solana.fm/tx/${txSig.txSig}, txSig: ${txSig.txSig}`
				);
			}
		} catch (err: any) {
			console.error(err);
			if (err instanceof TransactionExpiredBlockheightExceededError) {
				logger.info(
					`[${this.name}] Blockheight exceeded error, retrying with market: ${marketIndex})`
				);
			} else if (err instanceof Error) {
				if (isCriticalError(err)) {
					const e = err as SendTransactionError;
					await webhookMessage(
						`[${
							this.name
						}] failed to crank maker twaps for perp market ${marketIndex}. simulated CUs: ${simResult?.cuEstimate}, simError: ${JSON.stringify(
							simResult?.simError
						)}:\n${e.logs ? (e.logs as Array<string>).join('\n') : ''} \n${
							e.stack ? e.stack : e.message
						}`
					);
					return { success: false, canRetry: false };
				} else {
					return { success: false, canRetry: true };
				}
			}
		}
		return { success: true, canRetry: false };
	}

	private async getPythIxsFromTwapCrankInfo(
		crankMarketIndex: number
	): Promise<TransactionInstruction[]> {
		if (crankMarketIndex === undefined) {
			throw new Error('Market index not found on node');
		}
		if (!this.pythPriceSubscriber) {
			throw new Error('Pyth price subscriber not initialized');
		}
		const pythIxs = await getAllPythOracleUpdateIxs(
			this.globalConfig.driftEnv,
			crankMarketIndex,
			MarketType.PERP,
			this.pythPriceSubscriber!,
			this.driftClient,
			this.globalConfig.numNonActiveOraclesToPush ?? 0
		);
		return pythIxs;
	}

	private async tryTwapCrank(intervalGroup: number | null) {
		const state = this.driftClient.getStateAccount();
		let crankMarkets: number[] = [];
		if (intervalGroup === null) {
			crankMarkets = Array.from(
				{ length: state.numberOfMarkets },
				(_, index) => index
			);
			intervalGroup = DEFAULT_INTERVAL_GROUP;
		} else {
			crankMarkets = this.crankIntervalToMarketIds![intervalGroup]!;
		}

		const intervalInProgress = this.crankIntervalInProgress![intervalGroup]!;
		if (intervalInProgress) {
			logger.info(
				`[${this.name}] Interval ${intervalGroup} already in progress, skipping`
			);
			return;
		}

		const start = Date.now();
		let numFeedsSignalingRestart = 0;
		try {
			this.crankIntervalInProgress![intervalGroup] = true;
			await this.initDlob();

			logger.info(
				`[${this.name}] Cranking interval group ${intervalGroup}: ${crankMarkets}`
			);
			for (const mi of crankMarkets) {
				const pfs = this.priorityFeeSubscriberMap!.getPriorityFees('perp', mi);
				let microLamports = MIN_PRIORITY_FEE;
				if (pfs) {
					microLamports = Math.floor(
						pfs.high *
							this.driftClient.txSender.getSuggestedPriorityFeeMultiplier()
					);
				}
				// console.log(`market index ${mi}: microLamports: ${microLamports}`);

				const ixs = [
					ComputeBudgetProgram.setComputeUnitLimit({
						units: 1_400_000, // will be overwritten by simulateAndGetTxWithCUs
					}),
					ComputeBudgetProgram.setComputeUnitPrice({
						microLamports: microLamports ?? MIN_PRIORITY_FEE,
					}),
				];

				let pythIxsPushed = false;
				if (
					this.pythPriceSubscriber &&
					!isVariant(
						this.driftClient.getPerpMarketAccount(mi)!.amm.oracleSource,
						'prelaunch'
					)
				) {
					const pythIxs = await this.getPythIxsFromTwapCrankInfo(mi);
					ixs.push(...pythIxs);
					pythIxsPushed = true;
				}

				const oraclePriceData = this.driftClient.getOracleDataForPerpMarket(mi);

				const bidMakers = this.dlob!.getBestMakers({
					marketIndex: mi,
					marketType: MarketType.PERP,
					direction: PositionDirection.LONG,
					slot: this.latestDlobSlot!,
					oraclePriceData,
					numMakers: 2,
				});

				const askMakers = this.dlob!.getBestMakers({
					marketIndex: mi,
					marketType: MarketType.PERP,
					direction: PositionDirection.SHORT,
					slot: this.latestDlobSlot!,
					oraclePriceData,
					numMakers: 2,
				});
				logger.info(
					`[${this.name}] loaded makers for market ${mi}: ${bidMakers.length} bids, ${askMakers.length} asks`
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

				if (
					isVariant(
						this.driftClient.getPerpMarketAccount(mi)!.amm.oracleSource,
						'prelaunch'
					)
				) {
					const updatePrelaunchOracleIx =
						await this.driftClient.getUpdatePrelaunchOracleIx(mi);
					ixs.push(updatePrelaunchOracleIx);
				}

				const resp = await this.sendTx(mi, ixs);
				logger.info(
					`[${this.name}] sent tx for market: ${mi}, success: ${resp.success}`
				);

				// Check if this change caused the pyth price to update
				if (
					pythIxsPushed &&
					isOneOfVariant(
						this.driftClient.getPerpMarketAccount(mi)!.amm.oracleSource,
						['pythPull', 'pyth1KPull', 'ptyh1MPull', 'pythStableCoinPull']
					)
				) {
					const [data, currentSlot] = await Promise.all([
						this.driftClient.connection.getAccountInfo(
							this.driftClient.getPerpMarketAccount(mi)!.amm.oracle
						),
						this.driftClient.connection.getSlot(),
					]);
					if (!data) continue;
					const pythData =
						this.pythPullOracleClient.getOraclePriceDataFromBuffer(data.data);
					const slotDelay = currentSlot - pythData.slot.toNumber();
					console.log(slotDelay);
					if (slotDelay > SLOT_STALENESS_THRESHOLD_RESTART) {
						logger.info(
							`[${this.name}] oracle slot stale for market: ${mi}, slot delay: ${slotDelay}`
						);
						numFeedsSignalingRestart++;
					}
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
			this.crankIntervalInProgress![intervalGroup] = false;
			logger.info(
				`[${
					this.name
				}] tryTwapCrank finished for interval group ${intervalGroup}, took ${
					Date.now() - start
				}ms`
			);
			await this.watchdogTimerMutex.runExclusive(async () => {
				this.watchdogTimerLastPatTime = Date.now();
			});
			if (
				numFeedsSignalingRestart > 2 &&
				this.driftClient.txSender.getTxLandRate() > TX_LAND_RATE_THRESHOLD
			) {
				logger.info(
					`[${
						this.name
					}] ${numFeedsSignalingRestart} feeds signaling restart, tx land rate: ${this.driftClient.txSender.getTxLandRate()}`
				);
				await webhookMessage(
					`[${
						this.name
					}] ${numFeedsSignalingRestart} feeds signaling restart, tx land rate: ${this.driftClient.txSender.getTxLandRate()}`
				);
				this.pythHealthy = false;
			}
		}
	}
}

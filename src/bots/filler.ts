import {
	User,
	ReferrerInfo,
	isOracleValid,
	DriftClient,
	PerpMarketAccount,
	calculateAskPrice,
	calculateBidPrice,
	MakerInfo,
	isFillableByVAMM,
	calculateBaseAssetAmountForAmmToFulfill,
	isVariant,
	DLOB,
	NodeToFill,
	UserMap,
	UserStatsMap,
	MarketType,
	isOrderExpired,
	getVariant,
	PRICE_PRECISION,
	convertToNumber,
	BASE_PRECISION,
	QUOTE_PRECISION,
	WrappedEvent,
	PerpMarkets,
	OrderActionRecord,
	BulkAccountLoader,
	SlotSubscriber,
	OrderRecord,
	PublicKey,
	DLOBNode,
	UserSubscriptionConfig,
	isOneOfVariant,
	DLOBSubscriber,
} from '@drift-labs/sdk';
import { TxSigAndSlot } from '@drift-labs/sdk/lib/tx/types';
import { Mutex, tryAcquire, E_ALREADY_LOCKED } from 'async-mutex';

import {
	SendTransactionError,
	Transaction,
	TransactionResponse,
	TransactionSignature,
	TransactionInstruction,
	ComputeBudgetProgram,
	GetVersionedTransactionConfig,
	AddressLookupTableAccount,
	Keypair,
} from '@solana/web3.js';

import { SearcherClient } from 'jito-ts/dist/sdk/block-engine/searcher';
import { Bundle } from 'jito-ts/dist/sdk/block-engine/types';

import { PrometheusExporter } from '@opentelemetry/exporter-prometheus';
import {
	ExplicitBucketHistogramAggregation,
	InstrumentType,
	MeterProvider,
	View,
} from '@opentelemetry/sdk-metrics-base';
import {
	Meter,
	ObservableGauge,
	Counter,
	BatchObservableResult,
	Histogram,
} from '@opentelemetry/api-metrics';

import { logger } from '../logger';
import { Bot } from '../types';
import { FillerConfig } from '../config';
import { RuntimeSpec, metricAttrFromUserAccount } from '../metrics';
import { webhookMessage } from '../webhook';
import {
	isEndIxLog,
	isErrFillingLog,
	isErrStaleOracle,
	isFillIxLog,
	isIxLog,
	isMakerBreachedMaintenanceMarginLog,
	isOrderDoesNotExistLog,
	isTakerBreachedMaintenanceMarginLog,
} from './common/txLogParse';
import { getErrorCode } from '../error';

const MAX_TX_PACK_SIZE = 1100; //1232;
const CU_PER_FILL = 260_000; // CU cost for a successful fill
const BURST_CU_PER_FILL = 350_000; // CU cost for a successful fill
const MAX_CU_PER_TX = 1_400_000; // seems like this is all budget program gives us...on devnet
const TX_COUNT_COOLDOWN_ON_BURST = 10; // send this many tx before resetting burst mode
const FILL_ORDER_THROTTLE_BACKOFF = 10000; // the time to wait before trying to fill a throttled (error filling) node again
const FILL_ORDER_COOLDOWN_BACKOFF = 2000; // the time to wait before trying to a node in the filling map again
const USER_MAP_RESYNC_COOLDOWN_SLOTS = 50;

const errorCodesToSuppress = [
	6081, // 0x17c1 Error Number: 6081. Error Message: MarketWrongMutability.
	6239, // 0x185F Error Number: 6239. Error Message: RevertFill.
];

enum METRIC_TYPES {
	sdk_call_duration_histogram = 'sdk_call_duration_histogram',
	try_fill_duration_histogram = 'try_fill_duration_histogram',
	runtime_specs = 'runtime_specs',
	total_collateral = 'total_collateral',
	last_try_fill_time = 'last_try_fill_time',
	unrealized_pnl = 'unrealized_pnl',
	mutex_busy = 'mutex_busy',
	attempted_fills = 'attempted_fills',
	successful_fills = 'successful_fills',
	observed_fills_count = 'observed_fills_count',
	tx_sim_error_count = 'tx_sim_error_count',
	user_map_user_account_keys = 'user_map_user_account_keys',
	user_stats_map_authority_keys = 'user_stats_map_authority_keys',
}

function logMessageForNodeToFill(node: NodeToFill, prefix?: string): string {
	const takerNode = node.node;
	const takerOrder = takerNode.order;
	let msg = '';
	if (prefix) {
		msg += `${prefix}\n`;
	}
	msg += `taker on market ${
		takerOrder.marketIndex
	}: ${takerNode.userAccount.toBase58()}-${takerOrder.orderId} ${getVariant(
		takerOrder.direction
	)} ${convertToNumber(
		takerOrder.baseAssetAmountFilled,
		BASE_PRECISION
	)}/${convertToNumber(
		takerOrder.baseAssetAmount,
		BASE_PRECISION
	)} @ ${convertToNumber(
		takerOrder.price,
		PRICE_PRECISION
	)} (orderType: ${getVariant(takerOrder.orderType)})\n`;
	msg += `makers:\n`;
	if (node.makerNodes.length > 0) {
		for (let i = 0; i < node.makerNodes.length; i++) {
			const makerNode = node.makerNodes[i];
			const makerOrder = makerNode.order;
			msg += `  [${i}] market ${
				makerOrder.marketIndex
			}: ${makerNode.userAccount.toBase58()}-${makerOrder.orderId} ${getVariant(
				makerOrder.direction
			)} ${convertToNumber(
				makerOrder.baseAssetAmountFilled,
				BASE_PRECISION
			)}/${convertToNumber(
				makerOrder.baseAssetAmount,
				BASE_PRECISION
			)} @ ${convertToNumber(
				makerOrder.price,
				PRICE_PRECISION
			)} (orderType: ${getVariant(makerOrder.orderType)})\n`;
		}
	} else {
		msg += `  vAMM`;
	}
	return msg;
}

export class FillerBot implements Bot {
	public readonly name: string;
	public readonly dryRun: boolean;
	public readonly defaultIntervalMs: number = 6000;

	private slotSubscriber: SlotSubscriber;
	private bulkAccountLoader: BulkAccountLoader | undefined;
	private userStatsMapSubscriptionConfig: UserSubscriptionConfig;
	private driftClient: DriftClient;
	private pollingIntervalMs: number;
	private transactionVersion?: number;
	private revertOnFailure?: boolean;
	private lookupTableAccount: AddressLookupTableAccount;
	private jitoSearcherClient?: SearcherClient;
	private jitoAuthKeypair?: Keypair;
	private jitoTipAccount?: PublicKey;
	private jitoLeaderNextSlot?: number;
	private jitoLeaderNextSlotMutex = new Mutex();
	private tipPayerKeypair?: Keypair;

	private dlobSubscriber: DLOBSubscriber;

	private userMapMutex = new Mutex();
	private userMap: UserMap;
	private userStatsMap: UserStatsMap;
	private lastSeenNumberOfSubAccounts: number;
	private lastSeenNumberOfAuthorities: number;

	private periodicTaskMutex = new Mutex();

	private watchdogTimerMutex = new Mutex();
	private watchdogTimerLastPatTime = Date.now();

	private lastSlotReyncUserMapsMutex = new Mutex();
	private lastSlotResyncUserMaps = 0;

	private intervalIds: Array<NodeJS.Timer> = [];
	private throttledNodes = new Map<string, number>();
	private fillingNodes = new Map<string, number>();
	private useBurstCULimit = false;
	private fillTxSinceBurstCU = 0;
	private fillTxId = 0;

	// metrics
	private metricsInitialized = false;
	private metricsPort: number | undefined;
	private meter: Meter;
	private exporter: PrometheusExporter;
	private bootTimeMs: number;

	private runtimeSpecsGauge: ObservableGauge;
	private runtimeSpec: RuntimeSpec;
	private sdkCallDurationHistogram: Histogram;
	private tryFillDurationHistogram: Histogram;
	private lastTryFillTimeGauge: ObservableGauge;
	private totalCollateralGauge: ObservableGauge;
	private unrealizedPnLGauge: ObservableGauge;
	private mutexBusyCounter: Counter;
	private attemptedFillsCounter: Counter;
	private successfulFillsCounter: Counter;
	private observedFillsCountCounter: Counter;
	private txSimErrorCounter: Counter;
	private userMapUserAccountKeysGauge: ObservableGauge;
	private userStatsMapAuthorityKeysGauge: ObservableGauge;

	constructor(
		slotSubscriber: SlotSubscriber,
		bulkAccountLoader: BulkAccountLoader | undefined,
		driftClient: DriftClient,
		runtimeSpec: RuntimeSpec,
		config: FillerConfig,
		jitoSearcherClient?: SearcherClient,
		jitoAuthKeypair?: Keypair,
		tipPayerKeypair?: Keypair
	) {
		this.name = config.botId;
		this.dryRun = config.dryRun;
		this.slotSubscriber = slotSubscriber;
		this.driftClient = driftClient;
		this.bulkAccountLoader = bulkAccountLoader;
		if (this.bulkAccountLoader) {
			this.userStatsMapSubscriptionConfig = {
				type: 'polling',
				accountLoader: new BulkAccountLoader(
					this.bulkAccountLoader.connection,
					this.bulkAccountLoader.commitment,
					0 // no polling
				),
			};
		} else {
			this.userStatsMapSubscriptionConfig =
				this.driftClient.userAccountSubscriptionConfig;
		}
		this.runtimeSpec = runtimeSpec;
		this.pollingIntervalMs =
			config.fillerPollingInterval ?? this.defaultIntervalMs;

		this.metricsPort = config.metricsPort;
		if (this.metricsPort) {
			this.initializeMetrics();
		}

		this.transactionVersion = config.transactionVersion ?? undefined;
		logger.info(
			`${this.name}: using transactionVersion: ${this.transactionVersion}`
		);

		this.revertOnFailure = config.revertOnFailure ?? true;
		logger.info(`${this.name}: revertOnFailure: ${this.revertOnFailure}`);

		this.jitoSearcherClient = jitoSearcherClient;
		this.jitoAuthKeypair = jitoAuthKeypair;
		this.tipPayerKeypair = tipPayerKeypair;
		const jitoEnabled = this.jitoSearcherClient && this.jitoAuthKeypair;
		if (jitoEnabled) {
			this.jitoSearcherClient.getTipAccounts().then(async (tipAccounts) => {
				this.jitoTipAccount = new PublicKey(
					tipAccounts[Math.floor(Math.random() * tipAccounts.length)]
				);
				logger.info(
					`${this.name}: jito tip account: ${this.jitoTipAccount.toBase58()}`
				);
				this.jitoLeaderNextSlot = (
					await this.jitoSearcherClient.getNextScheduledLeader()
				).nextLeaderSlot;
			});

			this.slotSubscriber.eventEmitter.on('newSlot', async (slot: number) => {
				if (this.jitoLeaderNextSlot) {
					if (slot > this.jitoLeaderNextSlot) {
						try {
							await tryAcquire(this.jitoLeaderNextSlotMutex).runExclusive(
								async () => {
									logger.warn('LEADER REACHEd, GETTING NEXT SLOT');
									this.jitoLeaderNextSlot = (
										await this.jitoSearcherClient.getNextScheduledLeader()
									).nextLeaderSlot;
								}
							);
						} catch (e) {
							if (e !== E_ALREADY_LOCKED) {
								throw new Error(e);
							}
						}
					}
				}
			});
		}
		logger.info(
			`${this.name}: jito enabled: ${
				!!this.jitoSearcherClient && !!this.jitoAuthKeypair
			}`
		);
	}

	private initializeMetrics() {
		if (this.metricsInitialized) {
			logger.error('Tried to initilaize metrics multiple times');
			return;
		}
		this.metricsInitialized = true;

		const { endpoint: defaultEndpoint } = PrometheusExporter.DEFAULT_OPTIONS;
		this.exporter = new PrometheusExporter(
			{
				port: this.metricsPort,
				endpoint: defaultEndpoint,
			},
			() => {
				logger.info(
					`prometheus scrape endpoint started: http://localhost:${this.metricsPort}${defaultEndpoint}`
				);
			}
		);
		const meterName = this.name;
		const meterProvider = new MeterProvider({
			views: [
				new View({
					instrumentName: METRIC_TYPES.sdk_call_duration_histogram,
					instrumentType: InstrumentType.HISTOGRAM,
					meterName: meterName,
					aggregation: new ExplicitBucketHistogramAggregation(
						Array.from(new Array(20), (_, i) => 0 + i * 100),
						true
					),
				}),
				new View({
					instrumentName: METRIC_TYPES.try_fill_duration_histogram,
					instrumentType: InstrumentType.HISTOGRAM,
					meterName: meterName,
					aggregation: new ExplicitBucketHistogramAggregation(
						Array.from(new Array(20), (_, i) => 0 + i * 5),
						true
					),
				}),
			],
		});

		meterProvider.addMetricReader(this.exporter);
		this.meter = meterProvider.getMeter(meterName);

		this.bootTimeMs = Date.now();

		this.runtimeSpecsGauge = this.meter.createObservableGauge(
			METRIC_TYPES.runtime_specs,
			{
				description: 'Runtime sepcification of this program',
			}
		);
		this.runtimeSpecsGauge.addCallback((obs) => {
			obs.observe(this.bootTimeMs, this.runtimeSpec);
		});
		this.totalCollateralGauge = this.meter.createObservableGauge(
			METRIC_TYPES.total_collateral,
			{
				description: 'Total collateral of the account',
			}
		);
		this.lastTryFillTimeGauge = this.meter.createObservableGauge(
			METRIC_TYPES.last_try_fill_time,
			{
				description: 'Last time that fill was attempted',
			}
		);
		this.unrealizedPnLGauge = this.meter.createObservableGauge(
			METRIC_TYPES.unrealized_pnl,
			{
				description: 'The account unrealized PnL',
			}
		);

		this.mutexBusyCounter = this.meter.createCounter(METRIC_TYPES.mutex_busy, {
			description: 'Count of times the mutex was busy',
		});
		this.successfulFillsCounter = this.meter.createCounter(
			METRIC_TYPES.successful_fills,
			{
				description: 'Count of fills that we successfully landed',
			}
		);
		this.attemptedFillsCounter = this.meter.createCounter(
			METRIC_TYPES.attempted_fills,
			{
				description: 'Count of fills we attempted',
			}
		);
		this.observedFillsCountCounter = this.meter.createCounter(
			METRIC_TYPES.observed_fills_count,
			{
				description: 'Count of fills observed in the market',
			}
		);
		this.txSimErrorCounter = this.meter.createCounter(
			METRIC_TYPES.tx_sim_error_count,
			{
				description: 'Count of errors from simulating transactions',
			}
		);
		this.userMapUserAccountKeysGauge = this.meter.createObservableGauge(
			METRIC_TYPES.user_map_user_account_keys,
			{
				description: 'number of user account keys in UserMap',
			}
		);
		this.userMapUserAccountKeysGauge.addCallback(async (obs) => {
			obs.observe(this.userMap.size());
		});

		this.userStatsMapAuthorityKeysGauge = this.meter.createObservableGauge(
			METRIC_TYPES.user_stats_map_authority_keys,
			{
				description: 'number of authority keys in UserStatsMap',
			}
		);
		this.userStatsMapAuthorityKeysGauge.addCallback(async (obs) => {
			obs.observe(this.userStatsMap.size());
		});

		this.sdkCallDurationHistogram = this.meter.createHistogram(
			METRIC_TYPES.sdk_call_duration_histogram,
			{
				description: 'Distribution of sdk method calls',
				unit: 'ms',
			}
		);
		this.tryFillDurationHistogram = this.meter.createHistogram(
			METRIC_TYPES.try_fill_duration_histogram,
			{
				description: 'Distribution of tryFills',
				unit: 'ms',
			}
		);

		this.lastTryFillTimeGauge.addCallback(async (obs) => {
			await this.watchdogTimerMutex.runExclusive(async () => {
				const user = this.driftClient.getUser();
				obs.observe(
					this.watchdogTimerLastPatTime,
					metricAttrFromUserAccount(
						user.userAccountPublicKey,
						user.getUserAccount()
					)
				);
			});
		});

		this.meter.addBatchObservableCallback(
			async (batchObservableResult: BatchObservableResult) => {
				for (const user of this.driftClient.getUsers()) {
					const userAccount = user.getUserAccount();

					batchObservableResult.observe(
						this.totalCollateralGauge,
						convertToNumber(user.getTotalCollateral(), QUOTE_PRECISION),
						metricAttrFromUserAccount(user.userAccountPublicKey, userAccount)
					);

					batchObservableResult.observe(
						this.unrealizedPnLGauge,
						convertToNumber(user.getUnrealizedPNL(), QUOTE_PRECISION),
						metricAttrFromUserAccount(user.userAccountPublicKey, userAccount)
					);
				}
			},
			[this.totalCollateralGauge, this.unrealizedPnLGauge]
		);
	}

	public async init() {
		logger.info(`${this.name} initing`);

		await this.userMapMutex.runExclusive(async () => {
			this.userMap = new UserMap(
				this.driftClient,
				this.driftClient.userAccountSubscriptionConfig,
				false
			);
			this.userStatsMap = new UserStatsMap(
				this.driftClient,
				this.userStatsMapSubscriptionConfig
			);

			await this.userMap.sync();
			await this.userStatsMap.sync();
			logger.info(
				`initial userMap size: ${this.userMap.size()}, userStatsMap: ${this.userStatsMap.size()}`
			);

			this.lastSeenNumberOfSubAccounts = this.driftClient
				.getStateAccount()
				.numberOfSubAccounts.toNumber();
			this.lastSeenNumberOfAuthorities = this.driftClient
				.getStateAccount()
				.numberOfAuthorities.toNumber();
		});

		this.dlobSubscriber = new DLOBSubscriber({
			dlobSource: this.userMap,
			slotSource: this.slotSubscriber,
			updateFrequency: this.pollingIntervalMs - 500,
		});
		await this.dlobSubscriber.subscribe();

		this.lookupTableAccount =
			await this.driftClient.fetchMarketLookupTableAccount();

		await webhookMessage(`[${this.name}]: started`);
	}

	public async reset() {}

	public async startIntervalLoop(_intervalMs: number) {
		const intervalId = setInterval(
			this.tryFill.bind(this),
			this.pollingIntervalMs
		);
		this.intervalIds.push(intervalId);

		logger.info(`${this.name} Bot started!`);
	}

	public async healthCheck(): Promise<boolean> {
		let healthy = false;
		await this.watchdogTimerMutex.runExclusive(async () => {
			healthy =
				this.watchdogTimerLastPatTime > Date.now() - 5 * this.pollingIntervalMs;
			if (!healthy) {
				logger.warn(
					`watchdog timer last pat time ${this.watchdogTimerLastPatTime} is too old`
				);
			}
		});

		const stateAccount = this.driftClient.getStateAccount();
		const userMapResyncRequired =
			this.lastSeenNumberOfSubAccounts !==
				stateAccount.numberOfSubAccounts.toNumber() ||
			this.lastSeenNumberOfAuthorities !==
				stateAccount.numberOfAuthorities.toNumber();
		if (userMapResyncRequired) {
			logger.warn(
				`${
					this.name
				} user map resync required, userMap size: ${this.userMap.size()}, stateAccount.numberOfSubAccounts: ${stateAccount.numberOfSubAccounts.toNumber()}, userStatsMap size: ${this.userStatsMap.size()}, stateAccount.numberOfAuthorities: ${stateAccount.numberOfAuthorities.toNumber()}`
			);
		}

		healthy = healthy && !userMapResyncRequired;

		return healthy;
	}

	public async trigger(record: WrappedEvent<any>) {
		logger.debug(
			`filler seen record (slot: ${record.slot}): ${record.eventType}`
		);
		if (record.order) {
			logger.debug(` . ${record.user} - ${record.order.orderId}`);
		}
		// potentially a race here, but the lock is really slow :/
		// await this.userMapMutex.runExclusive(async () => {
		await this.userMap.updateWithEventRecord(record);
		await this.userStatsMap.updateWithEventRecord(record, this.userMap);
		// });

		if (record.eventType === 'OrderRecord') {
			const orderRecord = record as OrderRecord;
			const marketType = getVariant(orderRecord.order.marketType);
			if (marketType === 'perp') {
				await this.tryFill(orderRecord);
			}
		} else if (record.eventType === 'OrderActionRecord') {
			const actionRecord = record as OrderActionRecord;
			if (getVariant(actionRecord.action) === 'fill') {
				const marketType = getVariant(actionRecord.marketType);
				if (marketType === 'perp') {
					this.observedFillsCountCounter.add(1, {
						market:
							PerpMarkets[this.runtimeSpec.driftEnv][actionRecord.marketIndex]
								.symbol,
					});
				}
			}
		}
	}

	public viewDlob(): DLOB {
		return this.dlobSubscriber.getDLOB();
	}

	/**
	 * Checks that userMap and userStatsMap are up in sync with , if not, signal that we should update them next block.
	 */
	private async resyncUserMapsIfRequired() {
		const stateAccount = this.driftClient.getStateAccount();
		const resyncRequired =
			this.lastSeenNumberOfSubAccounts !==
				stateAccount.numberOfSubAccounts.toNumber() ||
			this.lastSeenNumberOfAuthorities !==
				stateAccount.numberOfAuthorities.toNumber();

		if (resyncRequired) {
			await this.lastSlotReyncUserMapsMutex.runExclusive(async () => {
				let doResync = false;
				const start = Date.now();
				if (!this.bulkAccountLoader) {
					const nextResyncSlot =
						this.lastSlotResyncUserMaps + USER_MAP_RESYNC_COOLDOWN_SLOTS;
					if (nextResyncSlot >= this.slotSubscriber.currentSlot) {
						const slotsRemaining =
							nextResyncSlot - this.slotSubscriber.currentSlot;
						if (slotsRemaining % 10 === 0) {
							logger.info(
								`Resyncing UserMaps in cooldown, ${slotsRemaining} more slots to go`
							);
						}
						return;
					} else {
						logger.info(
							`Resyncing UserMaps immediately (no BulkAccountLoader)`
						);
						doResync = true;
						this.lastSlotResyncUserMaps = this.slotSubscriber.currentSlot;
					}
				} else {
					const nextResyncSlot =
						this.lastSlotResyncUserMaps + USER_MAP_RESYNC_COOLDOWN_SLOTS;
					if (nextResyncSlot >= this.bulkAccountLoader.mostRecentSlot) {
						const slotsRemaining =
							nextResyncSlot - this.bulkAccountLoader.mostRecentSlot;
						if (slotsRemaining % 10 === 0) {
							logger.info(
								`Resyncing UserMaps in cooldown, ${slotsRemaining} more slots to go`
							);
						}
						return;
					} else {
						doResync = true;
						this.lastSlotResyncUserMaps = this.bulkAccountLoader.mostRecentSlot;
					}
				}

				if (doResync) {
					logger.info(`Resyncing UserMap`);
					const userMapSizeBefore = this.userMap.size();
					const userStatsMapSizeBefore = this.userStatsMap.size();
					this.userMap.sync().then(() => {
						this.userStatsMap
							.sync()
							.then(async () => {
								await this.userMapMutex.runExclusive(async () => {
									const usersAdded = this.userMap.size() - userMapSizeBefore;
									console.log('users added', usersAdded);
									const userStatsAdded =
										this.userStatsMap.size() - userStatsMapSizeBefore;
									console.log('user stats added', userStatsAdded);

									this.lastSeenNumberOfSubAccounts =
										stateAccount.numberOfSubAccounts.toNumber();
									this.lastSeenNumberOfAuthorities =
										stateAccount.numberOfAuthorities.toNumber();
								});
							})
							.finally(() => {
								logger.info(`UserMaps resynced in ${Date.now() - start}ms`);
							});
					});
				}
			});
		}
	}

	private async getPerpFillableNodesForMarket(
		market: PerpMarketAccount
	): Promise<Array<NodeToFill>> {
		const marketIndex = market.marketIndex;

		const oraclePriceData =
			this.driftClient.getOracleDataForPerpMarket(marketIndex);

		const vAsk = calculateAskPrice(market, oraclePriceData);
		const vBid = calculateBidPrice(market, oraclePriceData);

		return this.dlobSubscriber
			.getDLOB()
			.findNodesToFill(
				marketIndex,
				vBid,
				vAsk,
				this.slotSubscriber.currentSlot,
				Date.now() / 1000,
				MarketType.PERP,
				oraclePriceData,
				this.driftClient.getStateAccount(),
				this.driftClient.getPerpMarketAccount(marketIndex)
			);
	}

	private getNodeToFillSignature(node: NodeToFill): string {
		if (!node.node.userAccount) {
			return '~';
		}
		return this.getFillSignatureFromUserAccountAndOrderId(
			node.node.userAccount.toBase58(),
			node.node.order.orderId.toString()
		);
	}

	private getFillSignatureFromUserAccountAndOrderId(
		userAccount: string,
		orderId: string
	): string {
		return `${userAccount}-${orderId}`;
	}

	/**
	 * Checks if the node is still throttled, if not, clears it from the throttledNodes map
	 * @param throttleKey key in throttleMap
	 * @returns  true if throttleKey is still throttled, false if throttleKey is no longer throttled
	 */
	private isThrottledNodeStillThrottled(throttleKey: string): boolean {
		const lastFillAttempt = this.throttledNodes.get(throttleKey);
		if (lastFillAttempt + FILL_ORDER_THROTTLE_BACKOFF > Date.now()) {
			return true;
		} else {
			this.clearThrottledNode(throttleKey);
			return false;
		}
	}

	private isDLOBNodeThrottled(dlobNode: DLOBNode): boolean {
		const userAccountPubkey = dlobNode.userAccount.toBase58();
		if (this.throttledNodes.has(userAccountPubkey)) {
			if (this.isThrottledNodeStillThrottled(userAccountPubkey)) {
				return true;
			} else {
				return false;
			}
		}
		const orderSignature = this.getFillSignatureFromUserAccountAndOrderId(
			dlobNode.userAccount.toBase58(),
			dlobNode.order.orderId.toString()
		);
		if (this.throttledNodes.has(orderSignature)) {
			if (this.isThrottledNodeStillThrottled(orderSignature)) {
				return true;
			} else {
				return false;
			}
		}

		return false;
	}

	private clearThrottledNode(signature: string) {
		this.throttledNodes.delete(signature);
	}

	private filterFillableNodes(nodeToFill: NodeToFill): boolean {
		if (nodeToFill.node.isVammNode()) {
			logger.warn(
				`filtered out a vAMM node on market ${nodeToFill.node.order.marketIndex} for user ${nodeToFill.node.userAccount}-${nodeToFill.node.order.orderId}`
			);
			return false;
		}

		if (nodeToFill.node.haveFilled) {
			logger.warn(
				`filtered out filled node on market ${nodeToFill.node.order.marketIndex} for user ${nodeToFill.node.userAccount}-${nodeToFill.node.order.orderId}`
			);
			return false;
		}

		const now = Date.now();
		const nodeToFillSignature = this.getNodeToFillSignature(nodeToFill);
		if (this.fillingNodes.has(nodeToFillSignature)) {
			const timeStartedToFillNode = this.fillingNodes.get(nodeToFillSignature);
			if (timeStartedToFillNode + FILL_ORDER_COOLDOWN_BACKOFF > now) {
				// still cooling down on this node, filter it out
				return false;
			}
		}

		// check if taker node is throttled
		if (this.isDLOBNodeThrottled(nodeToFill.node)) {
			return false;
		}

		const marketIndex = nodeToFill.node.order.marketIndex;
		const oraclePriceData =
			this.driftClient.getOracleDataForPerpMarket(marketIndex);

		if (isOrderExpired(nodeToFill.node.order, Date.now() / 1000)) {
			if (isOneOfVariant(nodeToFill.node.order.orderType, ['limit'])) {
				// do not try to fill (expire) limit orders b/c they will auto expire when filled against
				// or the user places a new order
				return false;
			}
			logger.warn(
				`order is expired on market ${
					nodeToFill.node.order.marketIndex
				} for user ${nodeToFill.node.userAccount}-${
					nodeToFill.node.order.orderId
				} (${getVariant(nodeToFill.node.order.orderType)})`
			);
			return true;
		}

		if (
			nodeToFill.makerNodes.length === 0 &&
			isVariant(nodeToFill.node.order.marketType, 'perp') &&
			!isFillableByVAMM(
				nodeToFill.node.order,
				this.driftClient.getPerpMarketAccount(
					nodeToFill.node.order.marketIndex
				),
				oraclePriceData,
				this.slotSubscriber.currentSlot,
				Date.now() / 1000
			)
		) {
			logger.warn(
				`filtered out unfillable node on market ${nodeToFill.node.order.marketIndex} for user ${nodeToFill.node.userAccount}-${nodeToFill.node.order.orderId}`
			);
			logger.warn(` . no maker node: ${nodeToFill.makerNodes.length === 0}`);
			logger.warn(
				` . is perp: ${isVariant(nodeToFill.node.order.marketType, 'perp')}`
			);
			logger.warn(
				` . is not fillable by vamm: ${!isFillableByVAMM(
					nodeToFill.node.order,
					this.driftClient.getPerpMarketAccount(
						nodeToFill.node.order.marketIndex
					),
					oraclePriceData,
					this.slotSubscriber.currentSlot,
					Date.now() / 1000
				)}`
			);
			logger.warn(
				` .     calculateBaseAssetAmountForAmmToFulfill: ${calculateBaseAssetAmountForAmmToFulfill(
					nodeToFill.node.order,
					this.driftClient.getPerpMarketAccount(
						nodeToFill.node.order.marketIndex
					),
					oraclePriceData,
					this.slotSubscriber.currentSlot
				).toString()}`
			);
			return false;
		}

		// if making with vAMM, ensure valid oracle
		if (nodeToFill.makerNodes.length === 0) {
			const oracleIsValid = isOracleValid(
				this.driftClient.getPerpMarketAccount(nodeToFill.node.order.marketIndex)
					.amm,
				oraclePriceData,
				this.driftClient.getStateAccount().oracleGuardRails,
				this.slotSubscriber.currentSlot
			);
			if (!oracleIsValid) {
				logger.error(`Oracle is not valid for market ${marketIndex}`);
				return false;
			}
		}

		return true;
	}

	private async getNodeFillInfo(nodeToFill: NodeToFill): Promise<{
		makerInfos: Array<MakerInfo> | undefined;
		takerUser: User;
		referrerInfo: ReferrerInfo;
		marketType: MarketType;
	}> {
		const makerInfos: Array<MakerInfo> | undefined = [];
		let takerUser: User;
		let referrerInfo: ReferrerInfo;
		await tryAcquire(this.userMapMutex).runExclusive(async () => {
			// set to track whether maker account has already been included
			const makersIncluded = new Set<string>();
			if (nodeToFill.makerNodes.length > 0) {
				for (const makerNode of nodeToFill.makerNodes) {
					if (this.isDLOBNodeThrottled(makerNode)) {
						continue;
					}

					const makerAccount = makerNode.userAccount.toBase58();
					if (makersIncluded.has(makerAccount)) {
						continue;
					}

					const makerUserAccount = (
						await this.userMap.mustGet(makerAccount)
					).getUserAccount();
					const makerAuthority = makerUserAccount.authority;
					const makerUserStats = (
						await this.userStatsMap.mustGet(makerAuthority.toString())
					).userStatsAccountPublicKey;
					makerInfos.push({
						maker: makerNode.userAccount,
						makerUserAccount: makerUserAccount,
						order: makerNode.order,
						makerStats: makerUserStats,
					});
					makersIncluded.add(makerAccount);
				}
			}

			takerUser = await this.userMap.mustGet(
				nodeToFill.node.userAccount.toString()
			);
			referrerInfo = (
				await this.userStatsMap.mustGet(
					takerUser.getUserAccount().authority.toString()
				)
			).getReferrerInfo();
		});

		return Promise.resolve({
			makerInfos,
			takerUser,
			referrerInfo,
			marketType: nodeToFill.node.order.marketType,
		});
	}

	/**
	 * Returns the number of bytes occupied by this array if it were serialized in compact-u16-format.
	 * NOTE: assumes each element of the array is 1 byte (not sure if this holds?)
	 *
	 * https://docs.solana.com/developing/programming-model/transactions#compact-u16-format
	 *
	 * https://stackoverflow.com/a/69951832
	 *  hex     |  compact-u16
	 *  --------+------------
	 *  0x0000  |  [0x00]
	 *  0x0001  |  [0x01]
	 *  0x007f  |  [0x7f]
	 *  0x0080  |  [0x80 0x01]
	 *  0x3fff  |  [0xff 0x7f]
	 *  0x4000  |  [0x80 0x80 0x01]
	 *  0xc000  |  [0x80 0x80 0x03]
	 *  0xffff  |  [0xff 0xff 0x03])
	 */
	private calcCompactU16EncodedSize(array: any[], elemSize = 1): number {
		if (array.length > 0x3fff) {
			return 3 + array.length * elemSize;
		} else if (array.length > 0x7f) {
			return 2 + array.length * elemSize;
		} else {
			return 1 + (array.length * elemSize || 1);
		}
	}

	/**
	 * Instruction are made of 3 parts:
	 * - index of accounts where programId resides (1 byte)
	 * - affected accounts    (compact-u16-format byte array)
	 * - raw instruction data (compact-u16-format byte array)
	 * @param ix The instruction to calculate size for.
	 */
	private calcIxEncodedSize(ix: TransactionInstruction): number {
		return (
			1 +
			this.calcCompactU16EncodedSize(new Array(ix.keys.length), 1) +
			this.calcCompactU16EncodedSize(new Array(ix.data.byteLength), 1)
		);
	}

	private async sleep(ms: number) {
		return new Promise((resolve) => setTimeout(resolve, ms));
	}

	/**
	 * Iterates through a tx's logs and handles it appropriately 3e.g. throttling users, updating metrics, etc.)
	 *
	 * @param nodesFilled nodes that we sent a transaction to fill
	 * @param logs logs from tx.meta.logMessages or this.clearingHouse.program._events._eventParser.parseLogs
	 *
	 * @returns number of nodes successfully filled
	 */
	private async handleTransactionLogs(
		nodesFilled: Array<NodeToFill>,
		logs: string[]
	): Promise<number> {
		let inFillIx = false;
		let errorThisFillIx = false;
		let ixIdx = -1; // skip ComputeBudgetProgram
		let successCount = 0;
		let burstedCU = false;
		for (const log of logs) {
			if (log === null) {
				logger.error(`log is null`);
				continue;
			}

			if (log.includes('exceeded maximum number of instructions allowed')) {
				// temporary burst CU limit
				logger.warn(`Using bursted CU limit`);
				this.useBurstCULimit = true;
				this.fillTxSinceBurstCU = 0;
				burstedCU = true;
				continue;
			}

			if (isEndIxLog(this.driftClient.program.programId.toBase58(), log)) {
				if (!errorThisFillIx) {
					successCount++;
				}

				inFillIx = false;
				errorThisFillIx = false;
				continue;
			}

			if (isIxLog(log)) {
				if (isFillIxLog(log)) {
					inFillIx = true;
					errorThisFillIx = false;
					ixIdx++;

					// can also print this from parsing the log record in upcoming
					const nodeFilled = nodesFilled[ixIdx];
					logger.info(
						logMessageForNodeToFill(
							nodeFilled,
							`Processing tx log for assoc node ${ixIdx}`
						)
					);
				} else {
					inFillIx = false;
				}
				continue;
			}

			if (!inFillIx) {
				// this is not a log for a fill instruction
				continue;
			}

			// try to handle the log line
			const orderIdDoesNotExist = isOrderDoesNotExistLog(log);
			if (orderIdDoesNotExist) {
				const filledNode = nodesFilled[ixIdx];
				logger.error(
					`assoc node (ixIdx: ${ixIdx}): ${filledNode.node.userAccount.toString()}, ${
						filledNode.node.order.orderId
					}; does not exist (filled by someone else); ${log}`
				);
				this.clearThrottledNode(this.getNodeToFillSignature(filledNode));
				errorThisFillIx = true;
				continue;
			}

			const makerBreachedMaintenanceMargin =
				isMakerBreachedMaintenanceMarginLog(log);
			if (makerBreachedMaintenanceMargin !== null) {
				logger.error(
					`Throttling maker breached maintenance margin: ${makerBreachedMaintenanceMargin}`
				);
				this.throttledNodes.set(makerBreachedMaintenanceMargin, Date.now());
				const tx = new Transaction();
				tx.add(
					ComputeBudgetProgram.requestUnits({
						units: 1_000_000,
						additionalFee: 0,
					})
				);
				tx.add(
					await this.driftClient.getForceCancelOrdersIx(
						new PublicKey(makerBreachedMaintenanceMargin),
						(
							await this.userMap.mustGet(makerBreachedMaintenanceMargin)
						).getUserAccount()
					)
				);
				this.driftClient.txSender
					.send(tx, [], this.driftClient.opts)
					.then((txSig) => {
						logger.info(
							`Force cancelled orders for makers due to breach of maintenance margin. Tx: ${txSig}`
						);
					})
					.catch((e) => {
						console.error(e);
						logger.error(`Failed to send ForceCancelOrder Ixs (error above):`);
						webhookMessage(
							`[${this.name}]: :x: error processing fill tx logs:\n${
								e.stack ? e.stack : e.message
							}`
						);
					});

				errorThisFillIx = true;
				continue;
			}

			const takerBreachedMaintenanceMargin =
				isTakerBreachedMaintenanceMarginLog(log);
			if (takerBreachedMaintenanceMargin) {
				const filledNode = nodesFilled[ixIdx];
				const takerNodeSignature = filledNode.node.userAccount.toBase58();
				logger.error(
					`taker breach maint. margin, assoc node (ixIdx: ${ixIdx}): ${filledNode.node.userAccount.toString()}, ${
						filledNode.node.order.orderId
					}; (throttling ${takerNodeSignature} and force cancelling orders); ${log}`
				);
				this.throttledNodes.set(takerNodeSignature, Date.now());
				errorThisFillIx = true;

				const tx = new Transaction();
				tx.add(
					ComputeBudgetProgram.requestUnits({
						units: 1_000_000,
						additionalFee: 0,
					})
				);
				tx.add(
					await this.driftClient.getForceCancelOrdersIx(
						filledNode.node.userAccount,
						(
							await this.userMap.mustGet(filledNode.node.userAccount.toString())
						).getUserAccount()
					)
				);

				this.driftClient.txSender
					.send(tx, [], this.driftClient.opts)
					.then((txSig) => {
						logger.info(
							`Force cancelled orders for user ${filledNode.node.userAccount.toBase58()} due to breach of maintenance margin. Tx: ${txSig}`
						);
					})
					.catch((e) => {
						console.error(e);
						logger.error(`Failed to send ForceCancelOrder Ixs (error above):`);
						webhookMessage(
							`[${this.name}]: :x: error processing fill tx logs:\n${
								e.stack ? e.stack : e.message
							}`
						);
					});

				continue;
			}

			const errFillingLog = isErrFillingLog(log);
			if (errFillingLog) {
				const orderId = errFillingLog[0];
				const userAcc = errFillingLog[1];
				const extractedSig = this.getFillSignatureFromUserAccountAndOrderId(
					userAcc,
					orderId
				);
				this.throttledNodes.set(extractedSig, Date.now());

				const filledNode = nodesFilled[ixIdx];
				const assocNodeSig = this.getNodeToFillSignature(filledNode);
				logger.warn(
					`Throttling node due to fill error. extractedSig: ${extractedSig}, assocNodeSig: ${assocNodeSig}, assocNodeIdx: ${ixIdx}`
				);
				errorThisFillIx = true;
				continue;
			}

			if (isErrStaleOracle(log)) {
				logger.error(`Stale oracle error: ${log}`);
				errorThisFillIx = true;
				continue;
			}
		}

		if (!burstedCU) {
			if (this.fillTxSinceBurstCU > TX_COUNT_COOLDOWN_ON_BURST) {
				this.useBurstCULimit = false;
			}
			this.fillTxSinceBurstCU += 1;
		}

		return successCount;
	}

	private async processBulkFillTxLogs(
		nodesFilled: Array<NodeToFill>,
		txSig: TransactionSignature
	): Promise<number> {
		let tx: TransactionResponse | null = null;
		let attempts = 0;
		const config: GetVersionedTransactionConfig = {
			commitment: 'confirmed',
			maxSupportedTransactionVersion: 0,
		};
		while (tx === null && attempts < 10) {
			logger.info(`waiting for ${txSig} to be confirmed`);
			tx = await this.driftClient.connection.getTransaction(txSig, config);
			attempts++;
			await this.sleep(1000);
		}

		if (tx === null) {
			logger.error(`tx ${txSig} not found`);
			return 0;
		}

		return this.handleTransactionLogs(nodesFilled, tx.meta.logMessages);
	}

	private removeFillingNodes(nodes: Array<NodeToFill>) {
		for (const node of nodes) {
			this.fillingNodes.delete(this.getNodeToFillSignature(node));
		}
	}

	private async sendFillTx(
		fillTxId: number,
		nodesSent: Array<NodeToFill>,
		ixs: Array<TransactionInstruction>
	) {
		let txResp: Promise<TxSigAndSlot>;
		const txStart = Date.now();
		if (this.jitoSearcherClient && this.jitoAuthKeypair) {
			if (!isNaN(this.transactionVersion)) {
				logger.warn(
					`${this.name} should use unversioned tx for jito until https://github.com/jito-labs/jito-ts/pull/7`
				);
				return ['', 0];
			}
			const slotsUntilNextLeader =
				this.jitoLeaderNextSlot - this.slotSubscriber.getSlot();
			logger.info(
				`next jito leader is in ${slotsUntilNextLeader} slots (fillTxId: ${fillTxId})`
			);
			if (slotsUntilNextLeader > 2) {
				logger.info(
					`next jito leader is too far away, skipping... (fillTxId: ${fillTxId})`
				);
				return ['', 0];
			}
			const blockHash =
				await this.driftClient.provider.connection.getLatestBlockhash(
					'processed'
				);

			const tx = new Transaction();
			for (const ix of ixs) {
				tx.add(ix);
			}

			tx.feePayer = this.driftClient.provider.wallet.publicKey;
			tx.recentBlockhash = blockHash.blockhash;

			const signedTx = await this.driftClient.provider.wallet.signTransaction(
				tx
			);
			let b: Bundle | Error = new Bundle([signedTx], 2);
			b = b.attachTip(
				this.tipPayerKeypair,
				100_000, // TODO: make this configurable?
				this.jitoTipAccount,
				blockHash.blockhash,
				blockHash.lastValidBlockHeight
			);
			if (b instanceof Error) {
				logger.error(
					`failed to attach tip: ${b.message} (fillTxId: ${fillTxId})`
				);
				return ['', 0];
			}
			this.jitoSearcherClient
				.sendBundle(b)
				.then((uuid) => {
					logger.info(
						`${this.name} sent bundle with uuid ${uuid} (fillTxId: ${fillTxId})`
					);
				})
				.catch((err) => {
					logger.error(
						`failed to send bundle: ${err.message} (fillTxId: ${fillTxId})`
					);
				});
		} else if (isNaN(this.transactionVersion)) {
			const tx = new Transaction();
			for (const ix of ixs) {
				tx.add(ix);
			}
			txResp = this.driftClient.txSender.send(tx, [], this.driftClient.opts);
		} else if (this.transactionVersion === 0) {
			txResp = this.driftClient.txSender.sendVersionedTransaction(
				ixs,
				[this.lookupTableAccount],
				[],
				this.driftClient.opts
			);
		} else {
			throw new Error(
				`unsupported transaction version ${this.transactionVersion}`
			);
		}
		if (txResp) {
			txResp
				.then((resp: TxSigAndSlot) => {
					const duration = Date.now() - txStart;
					logger.info(
						`sent tx: ${resp.txSig}, took: ${duration}ms (fillTxId: ${fillTxId})`
					);

					const user = this.driftClient.getUser();
					this.sdkCallDurationHistogram.record(duration, {
						...metricAttrFromUserAccount(
							user.getUserAccountPublicKey(),
							user.getUserAccount()
						),
						method: 'sendTx',
					});

					const parseLogsStart = Date.now();
					this.processBulkFillTxLogs(nodesSent, resp.txSig)
						.then((successfulFills) => {
							const processBulkFillLogsDuration = Date.now() - parseLogsStart;
							logger.info(
								`parse logs took ${processBulkFillLogsDuration}ms, filled ${successfulFills} (fillTxId: ${fillTxId})`
							);

							// record successful fills
							const user = this.driftClient.getUser();
							this.successfulFillsCounter.add(
								successfulFills,
								metricAttrFromUserAccount(
									user.userAccountPublicKey,
									user.getUserAccount()
								)
							);
						})
						.catch((e) => {
							console.error(e);
							logger.error(
								`Failed to process fill tx logs (error above) (fillTxId: ${fillTxId}):`
							);
							webhookMessage(
								`[${this.name}]: :x: error processing fill tx logs:\n${
									e.stack ? e.stack : e.message
								}`
							);
						});
				})
				.catch(async (e) => {
					console.error(e);
					logger.error(
						`Failed to send packed tx (error above) (fillTxId: ${fillTxId}):`
					);
					const simError = e as SendTransactionError;

					if (simError.logs && simError.logs.length > 0) {
						const start = Date.now();
						await this.handleTransactionLogs(nodesSent, simError.logs);
						logger.error(
							`Failed to send tx, sim error tx logs took: ${
								Date.now() - start
							}ms (fillTxId: ${fillTxId})`
						);

						const errorCode = getErrorCode(e);

						if (
							!errorCodesToSuppress.includes(errorCode) &&
							!(e as Error).message.includes('Transaction was not confirmed')
						) {
							if (errorCode) {
								this.txSimErrorCounter.add(1, {
									errorCode: errorCode.toString(),
								});
							}
							webhookMessage(
								`[${this.name}]: :x: error simulating tx:\n${
									simError.logs ? simError.logs.join('\n') : ''
								}\n${e.stack || e}`
							);
						}
					}
				})
				.finally(() => {
					this.removeFillingNodes(nodesSent);
				});
		}
	}

	/**
	 * It's difficult to estimate CU cost of multi maker ix, so we'll just send it in its own transaction
	 * @param node node with multiple makers
	 */
	private async tryFillMultiMakerPerpNodes(nodeToFill: NodeToFill) {
		const ixs: Array<TransactionInstruction> = [];
		const fillTxId = this.fillTxId++;
		logger.info(
			logMessageForNodeToFill(
				nodeToFill,
				`Filling multi maker perp node with ${nodeToFill.makerNodes.length} makers (fillTxId: ${fillTxId})`
			)
		);

		try {
			// first ix is compute budget
			const computeBudgetIx = ComputeBudgetProgram.requestUnits({
				units: 10_000_000,
				additionalFee: 0,
			});
			ixs.push(computeBudgetIx);

			const { makerInfos, takerUser, referrerInfo, marketType } =
				await this.getNodeFillInfo(nodeToFill);

			if (!isVariant(marketType, 'perp')) {
				throw new Error('expected perp market type');
			}

			ixs.push(
				await this.driftClient.getFillPerpOrderIx(
					takerUser.getUserAccountPublicKey(),
					takerUser.getUserAccount(),
					nodeToFill.node.order,
					makerInfos,
					referrerInfo
				)
			);

			this.fillingNodes.set(
				this.getNodeToFillSignature(nodeToFill),
				Date.now()
			);

			if (this.revertOnFailure) {
				ixs.push(await this.driftClient.getRevertFillIx());
			}

			this.sendFillTx(fillTxId, [nodeToFill], ixs);
		} catch (e) {
			logger.error(
				`Error filling multi maker perp node (fillTxId: ${fillTxId}): ${
					e.stack ? e.stack : e.message
				}`
			);
		}
	}

	private async tryBulkFillPerpNodes(
		nodesToFill: Array<NodeToFill>
	): Promise<number> {
		const ixs: Array<TransactionInstruction> = [];

		/**
		 * At all times, the running Tx size is:
		 * - signatures (compact-u16 array, 64 bytes per elem)
		 * - message header (3 bytes)
		 * - affected accounts (compact-u16 array, 32 bytes per elem)
		 * - previous block hash (32 bytes)
		 * - message instructions (
		 * 		- progamIdIdx (1 byte)
		 * 		- accountsIdx (compact-u16, 1 byte per elem)
		 *		- instruction data (compact-u16, 1 byte per elem)
		 */
		let runningTxSize = 0;
		let runningCUUsed = 0;

		const uniqueAccounts = new Set<string>();
		uniqueAccounts.add(this.driftClient.provider.wallet.publicKey.toString()); // fee payer goes first

		// first ix is compute budget
		const computeBudgetIx = ComputeBudgetProgram.requestUnits({
			units: 10_000_000,
			additionalFee: 0,
		});
		computeBudgetIx.keys.forEach((key) =>
			uniqueAccounts.add(key.pubkey.toString())
		);
		uniqueAccounts.add(computeBudgetIx.programId.toString());
		ixs.push(computeBudgetIx);

		// initialize the barebones transaction
		// signatures
		runningTxSize += this.calcCompactU16EncodedSize(new Array(1), 64);
		// message header
		runningTxSize += 3;
		// accounts
		runningTxSize += this.calcCompactU16EncodedSize(
			new Array(uniqueAccounts.size),
			32
		);
		// block hash
		runningTxSize += 32;
		runningTxSize += this.calcIxEncodedSize(computeBudgetIx);

		const txPackerStart = Date.now();
		const nodesSent: Array<NodeToFill> = [];
		let idxUsed = 0;
		const fillTxId = this.fillTxId++;
		for (const [idx, nodeToFill] of nodesToFill.entries()) {
			if (nodeToFill.makerNodes.length > 1) {
				this.tryFillMultiMakerPerpNodes(nodeToFill);
				nodesSent.push(nodeToFill);
				continue;
			}
			logger.info(
				logMessageForNodeToFill(
					nodeToFill,
					`Filling perp node ${idx} (fillTxId: ${fillTxId})`
				)
			);

			const { makerInfos, takerUser, referrerInfo, marketType } =
				await this.getNodeFillInfo(nodeToFill);

			if (!isVariant(marketType, 'perp')) {
				throw new Error('expected perp market type');
			}

			const ix = await this.driftClient.getFillPerpOrderIx(
				takerUser.getUserAccountPublicKey(),
				takerUser.getUserAccount(),
				nodeToFill.node.order,
				makerInfos,
				referrerInfo
			);

			if (!ix) {
				logger.error(`failed to generate an ix`);
				break;
			}

			this.fillingNodes.set(
				this.getNodeToFillSignature(nodeToFill),
				Date.now()
			);

			// first estimate new tx size with this additional ix and new accounts
			const ixKeys = ix.keys.map((key) => key.pubkey);
			const newAccounts = ixKeys
				.concat(ix.programId)
				.filter((key) => !uniqueAccounts.has(key.toString()));
			const newIxCost = this.calcIxEncodedSize(ix);
			const additionalAccountsCost =
				newAccounts.length > 0
					? this.calcCompactU16EncodedSize(newAccounts, 32) - 1
					: 0;

			// We have to use MAX_TX_PACK_SIZE because it appears we cannot send tx with a size of exactly 1232 bytes.
			// Also, some logs may get truncated near the end of the tx, so we need to leave some room for that.
			const cuToUsePerFill = this.useBurstCULimit
				? BURST_CU_PER_FILL
				: CU_PER_FILL;
			if (
				runningTxSize + newIxCost + additionalAccountsCost >=
					MAX_TX_PACK_SIZE ||
				runningCUUsed + cuToUsePerFill >= MAX_CU_PER_TX
			) {
				logger.info(
					`Fully packed fill tx: est. tx size ${
						runningTxSize + newIxCost + additionalAccountsCost
					}, max: ${MAX_TX_PACK_SIZE}, est. CU used: expected ${
						runningCUUsed + cuToUsePerFill
					}, max: ${MAX_CU_PER_TX}, (fillTxId: ${fillTxId})`
				);
				break;
			}

			// add to tx
			logger.info(
				`including taker ${takerUser
					.getUserAccountPublicKey()
					.toString()}-${nodeToFill.node.order.orderId.toString()} (fillTxId: ${fillTxId})`
			);
			ixs.push(ix);
			runningTxSize += newIxCost + additionalAccountsCost;
			runningCUUsed += cuToUsePerFill;

			newAccounts.forEach((key) => uniqueAccounts.add(key.toString()));
			idxUsed++;
			nodesSent.push(nodeToFill);
		}

		logger.debug(
			`txPacker took ${Date.now() - txPackerStart}ms (fillTxId: ${fillTxId})`
		);

		if (nodesSent.length === 0) {
			return 0;
		}

		logger.info(
			`sending tx, ${
				uniqueAccounts.size
			} unique accounts, total ix: ${idxUsed}, calcd tx size: ${runningTxSize}, took ${
				Date.now() - txPackerStart
			}ms (fillTxId: ${fillTxId})`
		);

		if (this.revertOnFailure) {
			ixs.push(await this.driftClient.getRevertFillIx());
		}

		this.sendFillTx(fillTxId, nodesSent, ixs);

		return nodesSent.length;
	}

	private async tryFill(orderRecord?: OrderRecord) {
		const startTime = Date.now();
		let ran = false;
		try {
			await tryAcquire(this.periodicTaskMutex).runExclusive(async () => {
				const dlob = this.dlobSubscriber.getDLOB();
				if (orderRecord && dlob) {
					dlob.insertOrder(
						orderRecord.order,
						orderRecord.user,
						orderRecord.order.slot.toNumber()
					);
				}

				await this.resyncUserMapsIfRequired();

				// 1) get all fillable nodes
				let fillableNodes: Array<NodeToFill> = [];
				for (const market of this.driftClient.getPerpMarketAccounts()) {
					try {
						fillableNodes = fillableNodes.concat(
							await this.getPerpFillableNodesForMarket(market)
						);
						logger.debug(
							`got ${fillableNodes.length} fillable nodes on market ${market.marketIndex}`
						);
					} catch (e) {
						console.error(e);
						webhookMessage(
							`[${this.name}]: :x: Failed to get fillable nodes for market ${
								market.marketIndex
							}:\n${e.stack ? e.stack : e.message}`
						);
						continue;
					}
				}

				// filter out nodes that we know cannot be filled
				const seenNodes = new Set<string>();
				const filteredNodes = fillableNodes.filter((node) => {
					const sig = this.getNodeToFillSignature(node);
					if (seenNodes.has(sig)) {
						return false;
					}
					seenNodes.add(sig);
					return this.filterFillableNodes(node);
				});
				logger.debug(
					`filtered ${fillableNodes.length} to ${filteredNodes.length}`
				);

				// fill the perp nodes
				let filledNodeCount = 0;
				while (filledNodeCount < filteredNodes.length) {
					const attemptedFills = await this.tryBulkFillPerpNodes(
						filteredNodes.slice(filledNodeCount)
					);
					filledNodeCount += attemptedFills;

					// record fill attempts
					const user = this.driftClient.getUser();
					this.attemptedFillsCounter.add(
						attemptedFills,
						metricAttrFromUserAccount(
							user.userAccountPublicKey,
							user.getUserAccount()
						)
					);
				}

				ran = true;
			});
		} catch (e) {
			if (e === E_ALREADY_LOCKED) {
				const user = this.driftClient.getUser();
				this.mutexBusyCounter.add(
					1,
					metricAttrFromUserAccount(
						user.getUserAccountPublicKey(),
						user.getUserAccount()
					)
				);
			} else {
				webhookMessage(
					`[${this.name}]: :x: uncaught error:\n${
						e.stack ? e.stack : e.message
					}`
				);
				throw e;
			}
		} finally {
			if (ran) {
				const duration = Date.now() - startTime;
				const user = this.driftClient.getUser();
				this.tryFillDurationHistogram.record(
					duration,
					metricAttrFromUserAccount(
						user.getUserAccountPublicKey(),
						user.getUserAccount()
					)
				);
				logger.debug(`tryFill done, took ${duration}ms`);

				await this.watchdogTimerMutex.runExclusive(async () => {
					this.watchdogTimerLastPatTime = Date.now();
				});
			}
		}
	}
}

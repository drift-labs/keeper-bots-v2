import {
	DriftEnv,
	User,
	ReferrerInfo,
	DriftClient,
	SpotMarketAccount,
	MakerInfo,
	isVariant,
	DLOB,
	NodeToFill,
	UserMap,
	UserStatsMap,
	MarketType,
	initialize,
	SerumSubscriber,
	PollingDriftClientAccountSubscriber,
	SerumFulfillmentConfigMap,
	SerumV3FulfillmentConfigAccount,
	OrderActionRecord,
	getVariant,
	SpotMarkets,
	BulkAccountLoader,
	OrderRecord,
	convertToNumber,
	BASE_PRECISION,
	PRICE_PRECISION,
	WrappedEvent,
	DLOBNode,
	UserSubscriptionConfig,
} from '@drift-labs/sdk';
import {
	Mutex,
	tryAcquire,
	withTimeout,
	E_ALREADY_LOCKED,
	MutexInterface,
} from 'async-mutex';

import {
	AddressLookupTableAccount,
	ComputeBudgetProgram,
	GetVersionedTransactionConfig,
	PublicKey,
	Transaction,
	TransactionInstruction,
	TransactionResponse,
} from '@solana/web3.js';

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
	Histogram,
} from '@opentelemetry/api-metrics';

import { logger } from '../logger';
import { Bot } from '../types';
import { RuntimeSpec, metricAttrFromUserAccount } from '../metrics';
import { webhookMessage } from '../webhook';
import { getErrorCode } from '../error';
import {
	isEndIxLog,
	isFillIxLog,
	isIxLog,
	isMakerBreachedMaintenanceMarginLog,
	isOrderDoesNotExistLog,
	isTakerBreachedMaintenanceMarginLog,
} from './common/txLogParse';
import { TxSigAndSlot } from '@drift-labs/sdk/lib/tx/types';
import { FillerConfig } from '../config';

/**
 * Size of throttled nodes to get to before pruning the map
 */
const THROTTLED_NODE_SIZE_TO_PRUNE = 10;

/**
 * Time to wait before trying a node again
 */
const FILL_ORDER_BACKOFF = 10000;
const USER_MAP_RESYNC_COOLDOWN_SLOTS = 50;

/**
 * Constants to determine if we should add a priority fee to a transaction
 */
const pendingTxKink1 = 5; // number of pending tx before we start adding a priority fee starting at minPriorityFee
const pendingTxKink2 = 10; // number of pending tx after which we clamp the priority fee to maxPriorityFee

// the max priority fee we will add on top of the 5000 lamport base fee.
const minPriorityFee = 5000;
const maxPriorityFee = 10000;

const dlobMutexError = new Error('dlobMutex timeout');

const errorCodesToSuppress = [
	6061, // 0x17AD Error Number: 6061. Error Message: Order does not exist.
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
	user_map_user_account_keys = 'user_map_user_account_keys',
	user_stats_map_authority_keys = 'user_stats_map_authority_keys',
	pending_transactions = 'pending_transactions',
}

function getMakerNodeFromNodeToFill(
	nodeToFill: NodeToFill
): DLOBNode | undefined {
	if (nodeToFill.makerNodes.length === 0) {
		return undefined;
	}

	if (nodeToFill.makerNodes.length > 1) {
		logger.error(
			`Found more than one maker node for spot nodeToFill: ${JSON.stringify(
				nodeToFill
			)}`
		);
		return undefined;
	}

	return nodeToFill.makerNodes[0];
}

export class SpotFillerBot implements Bot {
	public readonly name: string;
	public readonly dryRun: boolean;
	public readonly defaultIntervalMs: number = 5000;

	private bulkAccountLoader: BulkAccountLoader | undefined;
	private userStatsMapSubscriptionConfig: UserSubscriptionConfig;
	private driftClient: DriftClient;
	private pollingIntervalMs: number;
	private transactionVersion: number;
	private lookupTableAccount: AddressLookupTableAccount;

	private dlobMutex: MutexInterface;
	private dlob: DLOB;

	private userMapMutex = new Mutex();
	private userMap: UserMap;
	private userStatsMap: UserStatsMap;
	private lastSeenNumberOfSubAccounts: number;
	private lastSeenNumberOfAuthorities: number;

	private serumFulfillmentConfigMap: SerumFulfillmentConfigMap;
	private serumSubscribers: Map<number, SerumSubscriber>;

	private periodicTaskMutex = new Mutex();

	private watchdogTimerMutex = new Mutex();
	private watchdogTimerLastPatTime = Date.now();

	private lastSlotReyncUserMapsMutex = new Mutex();
	private lastSlotResyncUserMaps = 0;

	private intervalIds: Array<NodeJS.Timer> = [];
	private throttledNodes = new Map<string, number>();
	private pendingTransactionsBuffer = new SharedArrayBuffer(16); // 16 byte shared buffer
	private pendingTransactionsArray = new Uint8Array(
		this.pendingTransactionsBuffer
	);

	// metrics
	private metricsInitialized = false;
	private metricsPort: number | undefined;
	private meter: Meter;
	private exporter: PrometheusExporter;
	private bootTimeMs: number;

	private runtimeSpecsGauge: ObservableGauge;
	private runtimeSpec: RuntimeSpec;
	private mutexBusyCounter: Counter;
	private attemptedFillsCounter: Counter;
	private observedFillsCountCounter: Counter;
	private successfulFillsCounter: Counter;
	private sdkCallDurationHistogram: Histogram;
	private tryFillDurationHistogram: Histogram;
	private lastTryFillTimeGauge: ObservableGauge;
	private userMapUserAccountKeysGauge: ObservableGauge;
	private userStatsMapAuthorityKeysGauge: ObservableGauge;
	private pendingTransactionsGauge: ObservableGauge;

	constructor(
		bulkAccountLoader: BulkAccountLoader | undefined,
		driftClient: DriftClient,
		runtimeSpec: RuntimeSpec,
		config: FillerConfig
	) {
		if (!bulkAccountLoader) {
			throw new Error(
				'SpotFiller only works in polling mode (cannot run with --websocket flag) bulkAccountLoader is required'
			);
		}
		this.name = config.botId;
		this.dryRun = config.dryRun;
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

		this.serumFulfillmentConfigMap = new SerumFulfillmentConfigMap(driftClient);
		this.serumSubscribers = new Map<number, SerumSubscriber>();

		this.dlobMutex = withTimeout(
			new Mutex(),
			10 * this.pollingIntervalMs,
			dlobMutexError
		);

		this.metricsPort = config.metricsPort;
		if (this.metricsPort) {
			this.initializeMetrics();
		}

		// load the pending tx atomic
		for (const spotMarket of SpotMarkets[
			this.runtimeSpec.driftEnv as DriftEnv
		]) {
			if (spotMarket.serumMarket) {
				Atomics.store(this.pendingTransactionsArray, spotMarket.marketIndex, 0);
			}
		}

		this.transactionVersion = config.transactionVersion ?? undefined;
		logger.info(
			`${this.name}: using transactionVersion: ${this.transactionVersion}`
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
		this.lastTryFillTimeGauge = this.meter.createObservableGauge(
			METRIC_TYPES.last_try_fill_time,
			{
				description: 'Last time that fill was attempted',
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

		this.pendingTransactionsGauge = this.meter.createObservableGauge(
			METRIC_TYPES.pending_transactions,
			{
				description:
					'number of pending transactions we are currently waiting on',
			}
		);
		this.pendingTransactionsGauge.addCallback(async (obs) => {
			for (const spotMarket of SpotMarkets[
				this.runtimeSpec.driftEnv as DriftEnv
			]) {
				if (spotMarket.serumMarket) {
					const marketIndex = spotMarket.marketIndex;
					const symbol = spotMarket.symbol;
					obs.observe(
						Atomics.load(this.pendingTransactionsArray, marketIndex),
						{
							marketIndex: marketIndex,
							symbol: symbol,
						}
					);
				}
			}
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
	}

	public async init() {
		logger.info(`${this.name} initing`);

		const initPromises: Array<Promise<any>> = [];

		initPromises.push(
			this.userMapMutex.runExclusive(async () => {
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

				this.lastSeenNumberOfSubAccounts = this.driftClient
					.getStateAccount()
					.numberOfSubAccounts.toNumber();
				this.lastSeenNumberOfAuthorities = this.driftClient
					.getStateAccount()
					.numberOfAuthorities.toNumber();
			})
		);

		const config = initialize({ env: this.runtimeSpec.driftEnv as DriftEnv });
		for (const spotMarketConfig of config.SPOT_MARKETS) {
			if (spotMarketConfig.serumMarket) {
				// set up fulfillment config
				initPromises.push(
					this.serumFulfillmentConfigMap.add(
						spotMarketConfig.marketIndex,
						spotMarketConfig.serumMarket
					)
				);

				// set up serum price subscriber
				const serumSubscriber = new SerumSubscriber({
					connection: this.driftClient.connection,
					programId: new PublicKey(config.SERUM_V3),
					marketAddress: spotMarketConfig.serumMarket,
					accountSubscription: {
						type: 'polling',
						accountLoader: (
							this.driftClient
								.accountSubscriber as PollingDriftClientAccountSubscriber
						).accountLoader,
					},
				});
				initPromises.push(serumSubscriber.subscribe());
				this.serumSubscribers.set(
					spotMarketConfig.marketIndex,
					serumSubscriber
				);
			}
		}

		await Promise.all(initPromises);

		this.lookupTableAccount =
			await this.driftClient.fetchMarketLookupTableAccount();

		await webhookMessage(`[${this.name}]: started`);
	}

	public async reset() {}

	public async startIntervalLoop(_intervalMs: number) {
		const intervalId = setInterval(
			this.trySpotFill.bind(this),
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
				logger.warn(`${this.name} watchdog timer expired`);
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
		// logger.info(
		// 	`Spot filler seen record (slot: ${record.slot}): ${record.eventType}`
		// );

		// potentially a race here, but the lock is really slow :/
		// await this.userMapMutex.runExclusive(async () => {
		await this.userMap.updateWithEventRecord(record);
		await this.userStatsMap.updateWithEventRecord(record, this.userMap);
		// });

		if (record.eventType === 'OrderRecord') {
			const orderRecord = record as OrderRecord;
			const marketType = getVariant(orderRecord.order.marketType);
			if (marketType === 'spot') {
				await this.trySpotFill(orderRecord);
			}
		} else if (record.eventType === 'OrderActionRecord') {
			const actionRecord = record as OrderActionRecord;

			if (getVariant(actionRecord.action) === 'fill') {
				const marketType = getVariant(actionRecord.marketType);
				if (marketType === 'spot') {
					this.observedFillsCountCounter.add(1, {
						market:
							SpotMarkets[this.runtimeSpec.driftEnv][actionRecord.marketIndex]
								.symbol,
					});
				}
			}
		}
	}

	public viewDlob(): DLOB {
		return this.dlob;
	}

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
					logger.info(`Resyncing UserMaps immediately (no BulkAccountLoader)`);
					doResync = true;
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
					this.userMap.sync().then(() => {
						this.userStatsMap
							.sync()
							.then(async () => {
								await this.userMapMutex.runExclusive(async () => {
									this.lastSeenNumberOfSubAccounts = this.driftClient
										.getStateAccount()
										.numberOfSubAccounts.toNumber();
									this.lastSeenNumberOfAuthorities = this.driftClient
										.getStateAccount()
										.numberOfAuthorities.toNumber();
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

	private async getSpotFillableNodesForMarket(
		market: SpotMarketAccount
	): Promise<Array<NodeToFill>> {
		let nodes: Array<NodeToFill> = [];

		const oraclePriceData = this.driftClient.getOracleDataForSpotMarket(
			market.marketIndex
		);

		// TODO: check oracle validity when ready

		await this.dlobMutex.runExclusive(async () => {
			const serumSubscriber = this.serumSubscribers.get(market.marketIndex);
			const serumBestBid = serumSubscriber?.getBestBid();
			const serumBestAsk = serumSubscriber?.getBestAsk();

			nodes = this.dlob.findNodesToFill(
				market.marketIndex,
				serumBestBid,
				serumBestAsk,
				oraclePriceData.slot.toNumber(),
				Date.now() / 1000,
				MarketType.SPOT,
				oraclePriceData,
				this.driftClient.getStateAccount(),
				this.driftClient.getSpotMarketAccount(market.marketIndex)
			);
		});

		return nodes;
	}

	private getNodeToFillSignature(node: NodeToFill): string {
		if (!node.node.userAccount) {
			return '~';
		}
		return `${node.node.userAccount.toString()}-${node.node.order.orderId.toString()}`;
	}

	private async getNodeFillInfo(nodeToFill: NodeToFill): Promise<{
		makerInfo: MakerInfo | undefined;
		chUser: User;
		referrerInfo: ReferrerInfo;
		marketType: MarketType;
	}> {
		let makerInfo: MakerInfo | undefined;
		let chUser: User;
		let referrerInfo: ReferrerInfo;

		try {
			await this.userMapMutex.runExclusive(async () => {
				const makerNode = getMakerNodeFromNodeToFill(nodeToFill);
				if (makerNode) {
					const makerUserAccount = (
						await this.userMap.mustGet(makerNode.userAccount.toString())
					).getUserAccount();
					const makerAuthority = makerUserAccount.authority;
					const makerUserStats = (
						await this.userStatsMap.mustGet(makerAuthority.toString())
					).userStatsAccountPublicKey;
					makerInfo = {
						maker: makerNode.userAccount,
						makerUserAccount: makerUserAccount,
						order: makerNode.order,
						makerStats: makerUserStats,
					};
				}

				chUser = await this.userMap.mustGet(
					nodeToFill.node.userAccount.toString()
				);
				referrerInfo = (
					await this.userStatsMap.mustGet(
						chUser.getUserAccount().authority.toString()
					)
				).getReferrerInfo();
			});
		} catch (e) {
			if (e != E_ALREADY_LOCKED) {
				throw new Error(`Error locking userMapMutex to fill node: ${e}`);
			}
		}

		return Promise.resolve({
			makerInfo,
			chUser,
			referrerInfo,
			marketType: nodeToFill.node.order.marketType,
		});
	}

	private nodeIsThrottled(nodeSignature: string): boolean {
		if (this.throttledNodes.has(nodeSignature)) {
			const lastFillAttempt = this.throttledNodes.get(nodeSignature);
			if (lastFillAttempt + FILL_ORDER_BACKOFF > Date.now()) {
				return true;
			}
		}
		return false;
	}
	private throttleNode(nodeSignature: string) {
		this.throttledNodes.set(nodeSignature, Date.now());
	}

	private unthrottleNode(nodeSignature: string) {
		this.throttledNodes.delete(nodeSignature);
	}

	private incPendingTransactions(spotMarketIndex: number): number {
		return Atomics.add(this.pendingTransactionsArray, spotMarketIndex, 1) + 1;
	}

	private decPendingTransactions(spotMarketIndex: number): number {
		return Atomics.sub(this.pendingTransactionsArray, spotMarketIndex, 1) - 1;
	}

	/**
	 *
	 * Priority fee paid (in addition to 5000 lamport base fee)
	 *
	 *                 /---------   <-- maxPriorityFee
	 *                / ^ pendingTxKink2
	 *               /
	 *              /   <-- minPriorityFee
	 *             |
	 * ------------|    <-- 0 priority fee
	 *             ^ pendingTxKink1
	 *
	 * ---- number of pending txs ----->
	 *
	 *
	 * PriorityFee = (computeUnitLimit * computeUnitPrice * 1e-6) lamports
	 * BaseFee = 5000 lamports
	 * TotalFeePaid = BaseFee + PriorityFee lamports
	 *
	 * @return computeUnitPrice in micro lamports (can be passed into ComputeBudgetProgram.setComputeUnitPrice)
	 */
	private calcComputeUnitPrice(
		currPendingTx: number,
		computeUnitLimit: number
	): number {
		if (currPendingTx < pendingTxKink1) {
			return 1;
		} else if (
			currPendingTx >= pendingTxKink1 &&
			currPendingTx < pendingTxKink2
		) {
			const m =
				(maxPriorityFee - minPriorityFee) / (pendingTxKink2 - pendingTxKink1);
			const b = minPriorityFee - m * pendingTxKink1;
			const priorityFee = m * currPendingTx + b;
			return priorityFee / (computeUnitLimit * 1e-6);
		} else {
			return maxPriorityFee / (computeUnitLimit * 1e-6);
		}
	}

	private async sleep(ms: number) {
		return new Promise((resolve) => setTimeout(resolve, ms));
	}

	private getFillSignatureFromUserAccountAndOrderId(
		userAccount: string,
		orderId: string
	): string {
		return `${userAccount}-${orderId}`;
	}

	/**
	 * Iterates through a tx's logs and handles it appropriately 3e.g. throttling users, updating metrics, etc.)
	 *
	 * @param nodesFilled nodes that we sent a transaction to fill
	 * @param logs logs from tx.meta.logMessages or this.driftClient.program._events._eventParser.parseLogs
	 *
	 * @returns number of nodes successfully filled
	 */
	private async handleTransactionLogs(
		nodeFilled: NodeToFill,
		logs: string[]
	): Promise<number> {
		let inFillIx = false;
		let errorThisFillIx = false;
		let successCount = 0;
		for (const log of logs) {
			if (log === null) {
				logger.error(`log is null`);
				continue;
			}

			if (isEndIxLog(this.driftClient.program.programId.toBase58(), log)) {
				if (!errorThisFillIx) {
					this.successfulFillsCounter.add(1, {
						market:
							SpotMarkets[this.runtimeSpec.driftEnv][
								nodeFilled.node.order.marketIndex
							].symbol,
					});
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

					// can also print this from parsing the log record in upcoming
					const makerNode = getMakerNodeFromNodeToFill(nodeFilled);
					if (makerNode) {
						logger.info(
							`Processing spot fill tx log:\ntaker: ${nodeFilled.node.userAccount.toBase58()}-${
								nodeFilled.node.order.orderId
							} ${convertToNumber(
								nodeFilled.node.order.baseAssetAmountFilled,
								BASE_PRECISION
							)}/${convertToNumber(
								nodeFilled.node.order.baseAssetAmount,
								BASE_PRECISION
							)} @ ${convertToNumber(
								nodeFilled.node.order.price,
								PRICE_PRECISION
							)}\nmaker: ${makerNode.userAccount.toBase58()}-${
								makerNode.order.orderId
							} ${convertToNumber(
								makerNode.order.baseAssetAmountFilled,
								BASE_PRECISION
							)}/${convertToNumber(
								makerNode.order.baseAssetAmount,
								BASE_PRECISION
							)} @ ${convertToNumber(makerNode.order.price, PRICE_PRECISION)}`
						);
					} else {
						logger.info(
							`Processing spot fill tx log:\ntaker: ${nodeFilled.node.userAccount.toBase58()}-${
								nodeFilled.node.order.orderId
							} ${convertToNumber(
								nodeFilled.node.order.baseAssetAmountFilled,
								BASE_PRECISION
							)}/${convertToNumber(
								nodeFilled.node.order.baseAssetAmount,
								BASE_PRECISION
							)} @ ${convertToNumber(
								nodeFilled.node.order.price,
								PRICE_PRECISION
							)}\nmaker: OpenBook`
						);
					}
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
				logger.error(
					`spot node filled: ${nodeFilled.node.userAccount.toString()}, ${
						nodeFilled.node.order.orderId
					}; does not exist (filled by someone else); ${log}`
				);
				this.throttledNodes.delete(this.getNodeToFillSignature(nodeFilled));
				errorThisFillIx = true;
				continue;
			}

			const makerBreachedMaintenanceMargin =
				isMakerBreachedMaintenanceMarginLog(log);
			if (makerBreachedMaintenanceMargin) {
				const makerNode = getMakerNodeFromNodeToFill(nodeFilled);
				if (!makerNode) {
					logger.error(
						`Got maker breached maint. margin log, but don't have a maker node: ${log}\n${JSON.stringify(
							nodeFilled,
							null,
							2
						)}`
					);
					continue;
				}
				const makerNodeSignature =
					this.getFillSignatureFromUserAccountAndOrderId(
						makerNode.userAccount.toString(),
						makerNode.order.orderId.toString()
					);
				logger.error(
					`maker breach maint. margin, assoc node: ${makerNode.userAccount.toString()}, ${
						makerNode.order.orderId
					}; (throttling ${makerNodeSignature}); ${log}`
				);
				this.throttledNodes.set(makerNodeSignature, Date.now());
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
						makerNode.userAccount,
						(
							await this.userMap.mustGet(makerNode.userAccount.toString())
						).getUserAccount()
					)
				);
				this.driftClient.txSender
					.send(tx, [], this.driftClient.opts)
					.then((txSig) => {
						logger.info(
							`Force cancelled orders for maker ${makerNode.userAccount.toBase58()} due to breach of maintenance margin. Tx: ${txSig}`
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

			const takerBreachedMaintenanceMargin =
				isTakerBreachedMaintenanceMarginLog(log);
			if (takerBreachedMaintenanceMargin) {
				const takerNodeSignature =
					this.getFillSignatureFromUserAccountAndOrderId(
						nodeFilled.node.userAccount.toString(),
						nodeFilled.node.order.orderId.toString()
					);
				logger.error(
					`taker breach maint. margin, assoc node: ${nodeFilled.node.userAccount.toString()}, ${
						nodeFilled.node.order.orderId
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
						nodeFilled.node.userAccount,
						(
							await this.userMap.mustGet(nodeFilled.node.userAccount.toString())
						).getUserAccount()
					)
				);

				this.driftClient.txSender
					.send(tx, [], this.driftClient.opts)
					.then((txSig) => {
						logger.info(
							`Force cancelled orders for user ${nodeFilled.node.userAccount.toBase58()} due to breach of maintenance margin. Tx: ${txSig}`
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
		}

		return successCount;
	}

	private async processBulkFillTxLogs(nodeToFill: NodeToFill, txSig: string) {
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
			// sleep 1s
			await this.sleep(1000);
		}

		if (tx === null) {
			logger.error(`tx ${txSig} not found`);
			return 0;
		}

		return this.handleTransactionLogs(nodeToFill, tx.meta.logMessages);
	}

	private async tryFillSpotNode(nodeToFill: NodeToFill) {
		const nodeSignature = this.getNodeToFillSignature(nodeToFill);
		if (this.nodeIsThrottled(nodeSignature)) {
			logger.info(`Throttling ${nodeSignature}`);
			return Promise.resolve(undefined);
		}
		this.throttleNode(nodeSignature);

		logger.info(
			`filling spot node: ${nodeToFill.node.userAccount.toString()}, ${
				nodeToFill.node.order.orderId
			}`
		);

		const { makerInfo, chUser, referrerInfo, marketType } =
			await this.getNodeFillInfo(nodeToFill);

		if (!isVariant(marketType, 'spot')) {
			throw new Error('expected spot market type');
		}

		// TODO: confirm if order.baseAssetAmount can use BASE_PRECISION for spot order
		const makerNode = getMakerNodeFromNodeToFill(nodeToFill);
		if (makerNode) {
			logger.info(
				`filling spot node:\ntaker: ${nodeToFill.node.userAccount.toBase58()}-${
					nodeToFill.node.order.orderId
				} ${convertToNumber(
					nodeToFill.node.order.baseAssetAmountFilled,
					BASE_PRECISION
				)}/${convertToNumber(
					nodeToFill.node.order.baseAssetAmount,
					BASE_PRECISION
				)} @ ${convertToNumber(
					nodeToFill.node.order.price,
					PRICE_PRECISION
				)}\nmaker: ${makerNode.userAccount.toBase58()}-${
					makerNode.order.orderId
				} ${convertToNumber(
					makerNode.order.baseAssetAmountFilled,
					BASE_PRECISION
				)}/${convertToNumber(
					makerNode.order.baseAssetAmount,
					BASE_PRECISION
				)} @ ${convertToNumber(makerNode.order.price, PRICE_PRECISION)}`
			);
		} else {
			logger.info(
				`filling spot node\ntaker: ${nodeToFill.node.userAccount.toBase58()}-${
					nodeToFill.node.order.orderId
				} ${convertToNumber(
					nodeToFill.node.order.baseAssetAmountFilled,
					BASE_PRECISION
				)}/${convertToNumber(
					nodeToFill.node.order.baseAssetAmount,
					BASE_PRECISION
				)} @ ${convertToNumber(
					nodeToFill.node.order.price,
					PRICE_PRECISION
				)}\nmaker: OpenBook`
			);
		}

		let serumFulfillmentConfig: SerumV3FulfillmentConfigAccount = undefined;
		if (makerInfo === undefined) {
			serumFulfillmentConfig = this.serumFulfillmentConfigMap.get(
				nodeToFill.node.order.marketIndex
			);
		}

		const currPendingTxs = this.incPendingTransactions(
			nodeToFill.node.order.marketIndex
		);
		const computeUnits = 1_000_000;
		const computeUnitsPrice = this.calcComputeUnitPrice(
			currPendingTxs,
			computeUnits
		);
		logger.info(
			`sending - currPendingTxs: ${currPendingTxs}, computeUnits: ${computeUnits}, computeUnitsPrice: ${computeUnitsPrice}`
		);

		const ixs: Array<TransactionInstruction> = [];
		ixs.push(ComputeBudgetProgram.setComputeUnitLimit({ units: computeUnits }));
		ixs.push(
			ComputeBudgetProgram.setComputeUnitPrice({
				microLamports: computeUnitsPrice,
			})
		);
		ixs.push(
			await this.driftClient.getFillSpotOrderIx(
				chUser.getUserAccountPublicKey(),
				chUser.getUserAccount(),
				nodeToFill.node.order,
				serumFulfillmentConfig,
				makerInfo,
				referrerInfo
			)
		);

		let txResp: Promise<TxSigAndSlot>;
		const txStart = Date.now();
		if (isNaN(this.transactionVersion)) {
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
		txResp
			.then(async (txSig) => {
				logger.info(`Filled spot order ${nodeSignature}, TX: ${txSig}`);

				const pendingTxs = this.decPendingTransactions(
					nodeToFill.node.order.marketIndex
				);
				logger.info(`done - currPendingTxs: ${pendingTxs}`);

				const duration = Date.now() - txStart;
				const user = this.driftClient.getUser();
				this.sdkCallDurationHistogram.record(duration, {
					...metricAttrFromUserAccount(
						user.getUserAccountPublicKey(),
						user.getUserAccount()
					),
					method: 'fillSpotOrder',
				});

				await this.processBulkFillTxLogs(nodeToFill, txSig.txSig);
			})
			.catch(async (e) => {
				const pendingTxs = this.decPendingTransactions(
					nodeToFill.node.order.marketIndex
				);
				const errorCode = getErrorCode(e);

				logger.info(`sim error - currPendingTxs: ${pendingTxs}`);
				logger.error(`Failed to fill spot order (errorCode: ${errorCode}):`);
				console.error(e);

				if (e.logs) {
					await this.handleTransactionLogs(nodeToFill, e.logs);
				}

				if (
					!errorCodesToSuppress.includes(errorCode) &&
					!(e as Error).message.includes('Transaction was not confirmed')
				) {
					webhookMessage(
						`[${
							this.name
						}]: :x: error trying to fill spot orders:\n\nSim logs:\n${
							e.logs ? (e.logs as Array<string>).join('\n') : ''
						}\n\n${e.stack ? e.stack : e.message}`
					);
				}
			})
			.finally(() => {
				this.unthrottleNode(nodeSignature);
			});
	}

	private async trySpotFill(orderRecord?: OrderRecord) {
		const startTime = Date.now();
		let ran = false;

		try {
			await tryAcquire(this.periodicTaskMutex).runExclusive(async () => {
				await this.dlobMutex.runExclusive(async () => {
					if (this.dlob) {
						this.dlob.clear();
						delete this.dlob;
					}
					this.dlob = new DLOB();
					try {
						await tryAcquire(this.userMapMutex).runExclusive(async () => {
							await this.dlob.initFromUserMap(
								this.userMap,
								this.bulkAccountLoader.mostRecentSlot
							);
						});
					} catch (e) {
						if (e != E_ALREADY_LOCKED) {
							throw new Error(`Failed to init DLOB from usermap: ${e}`);
						}
					}
					if (orderRecord) {
						this.dlob.insertOrder(
							orderRecord.order,
							orderRecord.user,
							this.bulkAccountLoader.mostRecentSlot
						);
					}
				});

				await this.resyncUserMapsIfRequired();

				if (this.throttledNodes.size > THROTTLED_NODE_SIZE_TO_PRUNE) {
					for (const [key, value] of this.throttledNodes.entries()) {
						if (value + 2 * FILL_ORDER_BACKOFF > Date.now()) {
							this.throttledNodes.delete(key);
						}
					}
				}

				// 1) get all fillable nodes
				let fillableNodes: Array<NodeToFill> = [];
				for (const market of this.driftClient.getSpotMarketAccounts()) {
					fillableNodes = fillableNodes.concat(
						await this.getSpotFillableNodesForMarket(market)
					);
				}

				const user = this.driftClient.getUser();
				this.attemptedFillsCounter.add(
					fillableNodes.length,
					metricAttrFromUserAccount(
						user.userAccountPublicKey,
						user.getUserAccount()
					)
				);

				for (const nodeToFill of fillableNodes) {
					this.tryFillSpotNode(nodeToFill);
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
			} else if (e === dlobMutexError) {
				logger.error(`${this.name} dlobMutexError timeout`);
			} else {
				logger.error('some other error:');
				console.error(e);
				webhookMessage(
					`[${this.name}]: :x: error trying to run main loop:\n${
						e.stack ? e.stack : e.message
					}`
				);
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
				logger.debug(`trySpotFill done, took ${duration}ms`);

				await this.watchdogTimerMutex.runExclusive(async () => {
					this.watchdogTimerLastPatTime = Date.now();
				});
			}
		}
	}
}

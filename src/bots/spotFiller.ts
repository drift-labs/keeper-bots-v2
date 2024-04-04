import {
	DriftEnv,
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
	SerumFulfillmentConfigMap,
	SerumV3FulfillmentConfigAccount,
	OrderActionRecord,
	OrderRecord,
	WrappedEvent,
	DLOBNode,
	DLOBSubscriber,
	PhoenixFulfillmentConfigMap,
	PhoenixSubscriber,
	BN,
	PhoenixV1FulfillmentConfigAccount,
	EventSubscriber,
	TEN,
	NodeToTrigger,
	BulkAccountLoader,
	PollingDriftClientAccountSubscriber,
	OrderSubscriber,
	UserAccount,
	PriorityFeeSubscriber,
	DataAndSlot,
	BlockhashSubscriber,
} from '@drift-labs/sdk';
import { Mutex, tryAcquire, E_ALREADY_LOCKED } from 'async-mutex';

import {
	AddressLookupTableAccount,
	ComputeBudgetProgram,
	PublicKey,
	SendTransactionError,
	TransactionInstruction,
	VersionedTransaction,
	VersionedTransactionResponse,
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
import { FillerConfig } from '../config';
import {
	getFillSignatureFromUserAccountAndOrderId,
	getNodeToFillSignature,
	getNodeToTriggerSignature,
	getTransactionAccountMetas,
	handleSimResultError,
	logMessageForNodeToFill,
	simulateAndGetTxWithCUs,
	SimulateAndGetTxWithCUsResponse,
} from '../utils';
import { BundleSender } from '../bundleSender';
import { MakerNodeMap } from './filler';
import { selectMakers } from '../makerSelection';

const THROTTLED_NODE_SIZE_TO_PRUNE = 10; // Size of throttled nodes to get to before pruning the map
const FILL_ORDER_THROTTLE_BACKOFF = 1000; // the time to wait before trying to fill a throttled (error filling) node again
const TRIGGER_ORDER_COOLDOWN_MS = 1000; // the time to wait before trying to a node in the triggering map again
const SIM_CU_ESTIMATE_MULTIPLIER = 1.15;
const SLOTS_UNTIL_JITO_LEADER_TO_SEND = 4;
const CONFIRM_TX_ATTEMPTS = 2;
const MAX_MAKERS_PER_FILL = 6; // max number of unique makers to include per fill
const MAX_ACCOUNTS_PER_TX = 64; // solana limit, track https://github.com/solana-labs/solana/issues/27241

const errorCodesToSuppress = [
	6061, // 0x17AD Error Number: 6061. Error Message: Order does not exist.
	// 6078, // 0x17BE Error Number: 6078. Error Message: PerpMarketNotFound
	6239, // 0x185F Error Number: 6239. Error Message: RevertFill.
	6023, // 0x1787 Error Number: 6023. Error Message: PriceBandsBreached.

	6111, // Error Message: OrderNotTriggerable.
	6112, // Error Message: OrderDidNotSatisfyTriggerCondition.
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
	attempted_triggers = 'attempted_triggers',
	successful_fills = 'successful_fills',
	observed_fills_count = 'observed_fills_count',
	user_map_user_account_keys = 'user_map_user_account_keys',
	user_stats_map_authority_keys = 'user_stats_map_authority_keys',
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

type FallbackLiquiditySource = 'serum' | 'phoenix';

type NodesToFillWithContext = {
	nodesToFill: NodeToFill[];
	fallbackAskSource?: FallbackLiquiditySource;
	fallbackBidSource?: FallbackLiquiditySource;
};

export type DLOBProvider = {
	subscribe(): Promise<void>;
	getDLOB(slot: number): Promise<DLOB>;
	getUniqueAuthorities(): PublicKey[];
	getUserAccounts(): Generator<{
		userAccount: UserAccount;
		publicKey: PublicKey;
	}>;
	getUserAccount(publicKey: PublicKey): UserAccount | undefined;
	size(): number;
	fetch(): Promise<void>;
};

export function getDLOBProviderFromOrderSubscriber(
	orderSubscriber: OrderSubscriber
): DLOBProvider {
	return {
		subscribe: async () => {
			await orderSubscriber.subscribe();
		},
		getDLOB: async (slot: number) => {
			return await orderSubscriber.getDLOB(slot);
		},
		getUniqueAuthorities: () => {
			const authorities = new Set<string>();
			for (const { userAccount } of orderSubscriber.usersAccounts.values()) {
				authorities.add(userAccount.authority.toBase58());
			}
			const pubkeys = Array.from(authorities).map((a) => new PublicKey(a));
			return pubkeys;
		},
		getUserAccounts: function* () {
			for (const [
				key,
				{ userAccount },
			] of orderSubscriber.usersAccounts.entries()) {
				yield { userAccount: userAccount, publicKey: new PublicKey(key) };
			}
		},
		getUserAccount: (publicKey) => {
			return orderSubscriber.usersAccounts.get(publicKey.toString())
				?.userAccount;
		},
		size(): number {
			return orderSubscriber.usersAccounts.size;
		},
		fetch() {
			return orderSubscriber.fetch();
		},
	};
}

export class SpotFillerBot implements Bot {
	public readonly name: string;
	public readonly dryRun: boolean;
	public readonly defaultIntervalMs: number = 2000;

	private driftClient: DriftClient;
	private eventSubscriber?: EventSubscriber;
	private blockhashSubscriber: BlockhashSubscriber;
	private pollingIntervalMs: number;
	private driftLutAccount?: AddressLookupTableAccount;
	private driftSpotLutAccount?: AddressLookupTableAccount;
	private fillTxId = 0;

	private dlobSubscriber?: DLOBSubscriber;

	private userMap: UserMap;
	private orderSubscriber: OrderSubscriber;
	private userStatsMap?: UserStatsMap;

	private serumFulfillmentConfigMap: SerumFulfillmentConfigMap;
	private serumSubscribers: Map<number, SerumSubscriber>;

	private phoenixFulfillmentConfigMap: PhoenixFulfillmentConfigMap;
	private phoenixSubscribers: Map<number, PhoenixSubscriber>;

	private periodicTaskMutex = new Mutex();

	private watchdogTimerMutex = new Mutex();
	private watchdogTimerLastPatTime = Date.now();

	private intervalIds: Array<NodeJS.Timer> = [];
	private throttledNodes = new Map<string, number>();
	private triggeringNodes = new Map<string, number>();

	private priorityFeeSubscriber: PriorityFeeSubscriber;
	private revertOnFailure: boolean;
	private simulateTxForCUEstimate?: boolean;
	private bundleSender?: BundleSender;

	// metrics
	private metricsInitialized = false;
	private metricsPort?: number;
	private meter?: Meter;
	private exporter?: PrometheusExporter;
	private bootTimeMs?: number;

	private runtimeSpecsGauge?: ObservableGauge;
	private runtimeSpec: RuntimeSpec;
	private mutexBusyCounter?: Counter;
	private attemptedFillsCounter?: Counter;
	private attemptedTriggersCounter?: Counter;
	private observedFillsCountCounter?: Counter;
	private successfulFillsCounter?: Counter;
	private sdkCallDurationHistogram?: Histogram;
	private tryFillDurationHistogram?: Histogram;
	private lastTryFillTimeGauge?: ObservableGauge;
	private userMapUserAccountKeysGauge?: ObservableGauge;
	private userStatsMapAuthorityKeysGauge?: ObservableGauge;

	constructor(
		driftClient: DriftClient,
		userMap: UserMap,
		runtimeSpec: RuntimeSpec,
		config: FillerConfig,
		priorityFeeSubscriber: PriorityFeeSubscriber,
		blockhashSubscriber: BlockhashSubscriber,
		eventSubscriber?: EventSubscriber,
		bundleSender?: BundleSender
	) {
		this.name = config.botId;
		this.dryRun = config.dryRun;
		this.driftClient = driftClient;
		this.eventSubscriber = eventSubscriber;
		this.runtimeSpec = runtimeSpec;
		this.pollingIntervalMs =
			config.fillerPollingInterval ?? this.defaultIntervalMs;

		this.serumFulfillmentConfigMap = new SerumFulfillmentConfigMap(driftClient);
		this.serumSubscribers = new Map<number, SerumSubscriber>();

		this.phoenixFulfillmentConfigMap = new PhoenixFulfillmentConfigMap(
			driftClient
		);
		this.phoenixSubscribers = new Map<number, PhoenixSubscriber>();

		this.metricsPort = config.metricsPort;
		if (this.metricsPort) {
			this.initializeMetrics();
		}

		this.priorityFeeSubscriber = priorityFeeSubscriber;
		this.priorityFeeSubscriber.updateAddresses([
			new PublicKey('8BnEgHoWFysVcuFFX7QztDmzuH8r5ZFvyP3sYwn1XTh6'), // Openbook SOL/USDC
			new PublicKey('4DoNfFBfF7UokCC2FQzriy7yHK6DY6NVdYpuekQ5pRgg'), // Phoenix SOL/USDC
			new PublicKey('6gMq3mRCKf8aP3ttTyYhuijVZ2LGi14oDsBbkgubfLB3'), // Drift USDC market
		]);

		this.revertOnFailure = config.revertOnFailure ?? true;
		this.simulateTxForCUEstimate = config.simulateTxForCUEstimate ?? true;
		logger.info(
			`${this.name}: revertOnFailure: ${this.revertOnFailure}, simulateTxForCUEstimate: ${this.simulateTxForCUEstimate}`
		);

		this.userMap = userMap;

		if (this.driftClient.userAccountSubscriptionConfig.type === 'websocket') {
			this.orderSubscriber = new OrderSubscriber({
				driftClient: this.driftClient,
				subscriptionConfig: {
					type: 'websocket',
					skipInitialLoad: false,
					commitment: this.driftClient.opts?.commitment,
					resyncIntervalMs: 10_000,
				},
			});
		} else {
			this.orderSubscriber = new OrderSubscriber({
				driftClient: this.driftClient,
				subscriptionConfig: {
					type: 'polling',

					// we find crossing orders from the OrderSubscriber, so we need to poll twice
					// as frequently as main filler interval
					frequency: this.pollingIntervalMs / 2,

					commitment: this.driftClient.opts?.commitment,
				},
			});
		}

		this.bundleSender = bundleSender;
		this.blockhashSubscriber = blockhashSubscriber;
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
			obs.observe(this.bootTimeMs!, this.runtimeSpec);
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
		this.attemptedTriggersCounter = this.meter.createCounter(
			METRIC_TYPES.attempted_triggers,
			{
				description: 'Count of triggers we attempted',
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
			obs.observe(this.userMap!.size());
		});

		this.userStatsMapAuthorityKeysGauge = this.meter.createObservableGauge(
			METRIC_TYPES.user_stats_map_authority_keys,
			{
				description: 'number of authority keys in UserStatsMap',
			}
		);
		this.userStatsMapAuthorityKeysGauge.addCallback(async (obs) => {
			obs.observe(this.userStatsMap!.size());
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

		const orderSubscriberInitStart = Date.now();
		logger.info(`Initializing OrderSubscriber...`);
		await this.orderSubscriber.subscribe();
		logger.info(
			`Initialized OrderSubscriber in ${
				Date.now() - orderSubscriberInitStart
			}ms`
		);

		const dlobProvider = getDLOBProviderFromOrderSubscriber(
			this.orderSubscriber
		);

		const userStatsMapStart = Date.now();
		logger.info(`Initializing UserStatsMap...`);
		this.userStatsMap = new UserStatsMap(this.driftClient);

		// disable the initial sync since there are way too many authorties now
		// userStats will be lazily loaded (this.userStatsMap.mustGet) as fills occur.
		// await this.userStatsMap.sync(dlobProvider.getUniqueAuthorities());

		logger.info(
			`Initialized UserStatsMap in ${Date.now() - userStatsMapStart}ms`
		);

		const dlobSubscriberStart = Date.now();
		logger.info(`Initializing DLOBSubscriber...`);
		this.dlobSubscriber = new DLOBSubscriber({
			dlobSource: dlobProvider,
			slotSource: this.orderSubscriber,
			updateFrequency: this.pollingIntervalMs - 500,
			driftClient: this.driftClient,
		});
		await this.dlobSubscriber.subscribe();
		logger.info(
			`Initialized DLOBSubscriber in ${Date.now() - dlobSubscriberStart}`
		);

		const config = initialize({ env: this.runtimeSpec.driftEnv as DriftEnv });
		const marketSetupPromises = config.SPOT_MARKETS.map(
			async (spotMarketConfig) => {
				let accountSubscription:
					| {
							type: 'polling';
							accountLoader: BulkAccountLoader;
					  }
					| {
							type: 'websocket';
					  };
				if (
					(
						this.driftClient
							.accountSubscriber as PollingDriftClientAccountSubscriber
					).accountLoader
				) {
					accountSubscription = {
						type: 'polling',
						accountLoader: (
							this.driftClient
								.accountSubscriber as PollingDriftClientAccountSubscriber
						).accountLoader,
					};
				} else {
					accountSubscription = {
						type: 'websocket',
					};
				}

				const subscribePromises = [];
				if (spotMarketConfig.serumMarket) {
					// set up fulfillment config
					await this.serumFulfillmentConfigMap.add(
						spotMarketConfig.marketIndex,
						spotMarketConfig.serumMarket
					);

					const serumConfigAccount =
						await this.driftClient.getSerumV3FulfillmentConfig(
							spotMarketConfig.serumMarket
						);

					if (isVariant(serumConfigAccount.status, 'enabled')) {
						// set up serum price subscriber
						const serumSubscriber = new SerumSubscriber({
							connection: this.driftClient.connection,
							programId: new PublicKey(config.SERUM_V3),
							marketAddress: spotMarketConfig.serumMarket,
							accountSubscription,
						});
						logger.info(
							`Initializing SerumSubscriber for ${spotMarketConfig.symbol}...`
						);
						subscribePromises.push(
							serumSubscriber.subscribe().then(() => {
								this.serumSubscribers.set(
									spotMarketConfig.marketIndex,
									serumSubscriber
								);
							})
						);
					}
				}

				if (spotMarketConfig.phoenixMarket) {
					// set up fulfillment config
					await this.phoenixFulfillmentConfigMap.add(
						spotMarketConfig.marketIndex,
						spotMarketConfig.phoenixMarket
					);

					const phoenixConfigAccount = this.phoenixFulfillmentConfigMap.get(
						spotMarketConfig.marketIndex
					);
					if (isVariant(phoenixConfigAccount.status, 'enabled')) {
						// set up phoenix price subscriber
						const phoenixSubscriber = new PhoenixSubscriber({
							connection: this.driftClient.connection,
							programId: new PublicKey(config.PHOENIX),
							marketAddress: spotMarketConfig.phoenixMarket,
							accountSubscription,
						});
						logger.info(
							`Initializing PhoenixSubscriber for ${spotMarketConfig.symbol}...`
						);
						subscribePromises.push(
							phoenixSubscriber.subscribe().then(() => {
								this.phoenixSubscribers.set(
									spotMarketConfig.marketIndex,
									phoenixSubscriber
								);
							})
						);
					}
				}
				await Promise.all(subscribePromises);
			}
		);
		await Promise.all(marketSetupPromises);

		this.driftLutAccount =
			await this.driftClient.fetchMarketLookupTableAccount();
		if ('SERUM_LOOKUP_TABLE' in config) {
			const lutAccount = (
				await this.driftClient.connection.getAddressLookupTable(
					new PublicKey(config.SERUM_LOOKUP_TABLE as string)
				)
			).value;
			if (lutAccount) {
				this.driftSpotLutAccount = lutAccount;
			}
		}

		await webhookMessage(`[${this.name}]: started`);
	}

	public async reset() {
		for (const intervalId of this.intervalIds) {
			clearInterval(intervalId as NodeJS.Timeout);
		}
		this.intervalIds = [];

		this.eventSubscriber?.eventEmitter.removeAllListeners('newEvent');

		await this.dlobSubscriber!.unsubscribe();
		await this.userStatsMap!.unsubscribe();
		await this.orderSubscriber.unsubscribe();

		for (const serumSubscriber of this.serumSubscribers.values()) {
			await serumSubscriber.unsubscribe();
		}

		for (const phoenixSubscriber of this.phoenixSubscribers.values()) {
			await phoenixSubscriber.unsubscribe();
		}
	}

	public async startIntervalLoop(_intervalMs?: number) {
		const intervalId = setInterval(
			this.trySpotFill.bind(this),
			this.pollingIntervalMs
		);
		this.intervalIds.push(intervalId);

		this.eventSubscriber?.eventEmitter.on(
			'newEvent',
			async (record: WrappedEvent<any>) => {
				if (record.eventType === 'OrderActionRecord') {
					const actionRecord = record as OrderActionRecord;

					if (isVariant(actionRecord.action, 'fill')) {
						if (isVariant(actionRecord.marketType, 'spot')) {
							const spotMarket = this.driftClient.getSpotMarketAccount(
								actionRecord.marketIndex
							);
							if (spotMarket) {
								this.observedFillsCountCounter!.add(1, {
									market: spotMarket.name,
								});
							}
						}
					}
				}
			}
		);

		logger.info(`${this.name} Bot started!`);
	}

	protected getMaxSlot(): number {
		return Math.max(
			this.dlobSubscriber?.slotSource.getSlot() ?? 0,
			this.orderSubscriber.getSlot(),
			this.userMap!.getSlot()
		);
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

		return healthy;
	}

	private async getUserAccountAndSlotFromMap(
		key: string
	): Promise<DataAndSlot<UserAccount>> {
		const user = await this.userMap!.mustGetWithSlot(
			key,
			this.driftClient.userAccountSubscriptionConfig
		);
		return {
			data: user.data.getUserAccount(),
			slot: user.slot,
		};
	}

	private getSpotNodesForMarket(
		market: SpotMarketAccount,
		dlob: DLOB
	): {
		nodesToFill: NodesToFillWithContext;
		nodesToTrigger: Array<NodeToTrigger>;
	} {
		const oraclePriceData = this.driftClient.getOracleDataForSpotMarket(
			market.marketIndex
		);

		const serumSubscriber = this.serumSubscribers.get(market.marketIndex);
		const serumBestBid = serumSubscriber?.getBestBid();
		const serumBestAsk = serumSubscriber?.getBestAsk();

		const phoenixSubscriber = this.phoenixSubscribers.get(market.marketIndex);
		const phoenixBestBid = phoenixSubscriber?.getBestBid();
		const phoenixBestAsk = phoenixSubscriber?.getBestAsk();

		const [fallbackBidPrice, fallbackBidSource] = this.pickFallbackPrice(
			serumBestBid,
			phoenixBestBid,
			'bid'
		);

		const [fallbackAskPrice, fallbackAskSource] = this.pickFallbackPrice(
			serumBestAsk,
			phoenixBestAsk,
			'ask'
		);

		const fillSlot = this.orderSubscriber.getSlot();

		const nodesToFill = dlob.findNodesToFill(
			market.marketIndex,
			fallbackBidPrice,
			fallbackAskPrice,
			fillSlot,
			Date.now() / 1000,
			MarketType.SPOT,
			oraclePriceData,
			this.driftClient.getStateAccount(),
			this.driftClient.getSpotMarketAccount(market.marketIndex)!
		);

		const nodesToTrigger = dlob.findNodesToTrigger(
			market.marketIndex,
			fillSlot,
			oraclePriceData.price,
			MarketType.SPOT,
			this.driftClient.getStateAccount()
		);

		return {
			nodesToFill: { nodesToFill, fallbackAskSource, fallbackBidSource },
			nodesToTrigger,
		};
	}

	private pickFallbackPrice(
		serumPrice: BN | undefined,
		phoenixPrice: BN | undefined,
		side: 'bid' | 'ask'
	): [BN | undefined, FallbackLiquiditySource | undefined] {
		if (serumPrice && phoenixPrice) {
			if (side === 'bid') {
				return serumPrice.gt(phoenixPrice)
					? [serumPrice, 'serum']
					: [phoenixPrice, 'phoenix'];
			} else {
				return serumPrice.lt(phoenixPrice)
					? [serumPrice, 'serum']
					: [phoenixPrice, 'phoenix'];
			}
		}

		if (serumPrice) {
			return [serumPrice, 'serum'];
		}

		if (phoenixPrice) {
			return [phoenixPrice, 'phoenix'];
		}

		return [undefined, undefined];
	}

	private async getNodeFillInfo(nodeToFill: NodeToFill): Promise<{
		makerInfos: Array<DataAndSlot<MakerInfo>>;
		takerUserPubKey: string;
		takerUser: UserAccount;
		takerUserSlot: number;
		marketType: MarketType;
	}> {
		const makerInfos: Array<DataAndSlot<MakerInfo>> = [];

		if (nodeToFill.makerNodes.length > 0) {
			let makerNodesMap: MakerNodeMap = new Map<string, DLOBNode[]>();
			for (const makerNode of nodeToFill.makerNodes) {
				if (this.isDLOBNodeThrottled(makerNode)) {
					continue;
				}

				if (!makerNode.userAccount) {
					continue;
				}

				if (makerNodesMap.has(makerNode.userAccount!)) {
					makerNodesMap.get(makerNode.userAccount!)!.push(makerNode);
				} else {
					makerNodesMap.set(makerNode.userAccount!, [makerNode]);
				}
			}

			if (makerNodesMap.size > MAX_MAKERS_PER_FILL) {
				logger.info(`selecting from ${makerNodesMap.size} makers`);
				makerNodesMap = selectMakers(makerNodesMap);
				logger.info(`selected: ${Array.from(makerNodesMap.keys()).join(',')}`);
			}

			for (const [makerAccount, makerNodes] of makerNodesMap) {
				const makerNode = makerNodes[0];

				const makerUserAccount = await this.getUserAccountAndSlotFromMap(
					makerAccount
				);
				const makerAuthority = makerUserAccount.data.authority;
				const makerUserStats = (
					await this.userStatsMap!.mustGet(makerAuthority.toString())
				).userStatsAccountPublicKey;
				makerInfos.push({
					slot: makerUserAccount.slot,
					data: {
						maker: new PublicKey(makerAccount),
						makerUserAccount: makerUserAccount.data,
						order: makerNode.order,
						makerStats: makerUserStats,
					},
				});
			}
		}

		const node = nodeToFill.node;
		const order = node.order!;

		const user = await this.userMap!.mustGetWithSlot(
			node.userAccount!.toString(),
			this.driftClient.userAccountSubscriptionConfig
		);

		return Promise.resolve({
			makerInfos,
			takerUser: user.data.getUserAccount(),
			takerUserSlot: user.slot,
			takerUserPubKey: node.userAccount!.toString(),
			marketType: order.marketType,
		});
	}

	/**
	 * Checks if the node is still throttled, if not, clears it from the throttledNodes map
	 * @param throttleKey key in throttleMap
	 * @returns  true if throttleKey is still throttled, false if throttleKey is no longer throttled
	 */
	protected isThrottledNodeStillThrottled(throttleKey: string): boolean {
		const lastFillAttempt = this.throttledNodes.get(throttleKey) || 0;
		if (lastFillAttempt + FILL_ORDER_THROTTLE_BACKOFF > Date.now()) {
			return true;
		} else {
			this.clearThrottledNode(throttleKey);
			return false;
		}
	}

	protected isDLOBNodeThrottled(dlobNode: DLOBNode): boolean {
		if (!dlobNode.userAccount || !dlobNode.order) {
			return false;
		}

		// first check if the userAccount itself is throttled
		const userAccountPubkey = dlobNode.userAccount;
		if (this.throttledNodes.has(userAccountPubkey)) {
			if (this.isThrottledNodeStillThrottled(userAccountPubkey)) {
				return true;
			} else {
				return false;
			}
		}

		// then check if the specific order is throttled
		const orderSignature = getFillSignatureFromUserAccountAndOrderId(
			dlobNode.userAccount,
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

	private setThrottleNode(nodeSignature: string) {
		this.throttledNodes.set(nodeSignature, Date.now());
	}

	private clearThrottledNode(nodeSignature: string) {
		this.throttledNodes.delete(nodeSignature);
	}

	private pruneThrottledNode() {
		if (this.throttledNodes.size > THROTTLED_NODE_SIZE_TO_PRUNE) {
			for (const [key, value] of this.throttledNodes.entries()) {
				if (value + 2 * FILL_ORDER_THROTTLE_BACKOFF > Date.now()) {
					this.throttledNodes.delete(key);
				}
			}
		}
	}

	private removeTriggeringNodes(node: NodeToTrigger) {
		this.triggeringNodes.delete(getNodeToTriggerSignature(node));
	}

	private async sleep(ms: number) {
		return new Promise((resolve) => setTimeout(resolve, ms));
	}

	/**
	 * Iterates through a tx's logs and handles it appropriately e.g. throttling users, updating metrics, etc.)
	 *
	 * @param nodesFilled nodes that we sent a transaction to fill
	 * @param logs logs from tx.meta.logMessages or this.driftClient.program._events._eventParser.parseLogs
	 *
	 * @returns number of nodes successfully filled, and whether the tx exceeded CUs
	 */
	private async handleTransactionLogs(
		nodeFilled: NodeToFill,
		logs: string[] | null | undefined
	): Promise<{ filledNodes: number; exceededCUs: boolean }> {
		if (!logs) {
			return {
				filledNodes: 0,
				exceededCUs: false,
			};
		}

		let inFillIx = false;
		let errorThisFillIx = false;
		let successCount = 0;
		for (const log of logs) {
			if (log === null) {
				logger.error(`log is null`);
				continue;
			}

			const node = nodeFilled.node;
			const order = node.order!;

			if (isEndIxLog(this.driftClient.program.programId.toBase58(), log)) {
				if (!errorThisFillIx) {
					if (this.successfulFillsCounter) {
						this.successfulFillsCounter!.add(1, {
							market: this.driftClient.getSpotMarketAccount(order.marketIndex)!
								.name,
						});
					}
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
					`spot node filled: ${node.userAccount!.toString()}, ${
						order.orderId
					}; does not exist (filled by someone else); ${log}`
				);
				this.throttledNodes.delete(getNodeToFillSignature(nodeFilled));
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
				const order = makerNode.order!;
				const makerNodeSignature = getFillSignatureFromUserAccountAndOrderId(
					makerNode.userAccount!.toString(),
					order.orderId.toString()
				);
				logger.error(
					`maker breach maint. margin, assoc node: ${makerNode.userAccount!.toString()}, ${
						order.orderId
					}; (throttling ${makerNodeSignature}); ${log}`
				);
				this.throttledNodes.set(makerNodeSignature, Date.now());
				errorThisFillIx = true;

				this.driftClient
					.forceCancelOrders(
						new PublicKey(makerNode.userAccount!),
						(
							await this.userMap!.mustGet(
								makerNode.userAccount!.toString(),
								this.driftClient.userAccountSubscriptionConfig
							)
						).getUserAccount()
					)
					.then((txSig) => {
						logger.info(
							`Force cancelled orders for maker ${makerNode.userAccount!} due to breach of maintenance margin. Tx: ${txSig}`
						);
					})
					.catch((e) => {
						console.error(e);
						logger.error(
							`Failed to send ForceCancelOrder Tx (error above), maker (${makerNode.userAccount!.toString()}) breach maint margin:`
						);
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
				const node = nodeFilled.node!;
				const order = node.order!;
				const takerNodeSignature = getFillSignatureFromUserAccountAndOrderId(
					node.userAccount!.toString(),
					order.orderId.toString()
				);
				logger.error(
					`taker breach maint. margin, assoc node: ${node.userAccount!.toString()}, ${
						order.orderId
					}; (throttling ${takerNodeSignature} and force cancelling orders); ${log}`
				);
				this.throttledNodes.set(takerNodeSignature, Date.now());
				errorThisFillIx = true;

				this.driftClient
					.forceCancelOrders(
						new PublicKey(node.userAccount!),
						(
							await this.userMap!.mustGet(
								node.userAccount!.toString(),
								this.driftClient.userAccountSubscriptionConfig
							)
						).getUserAccount()
					)
					.then((txSig) => {
						logger.info(
							`Force cancelled orders for user ${node.userAccount!} due to breach of maintenance margin. Tx: ${txSig}`
						);
					})
					.catch((e) => {
						console.error(e);
						logger.error(
							`Failed to send ForceCancelOrder Tx (error above), taker (${node.userAccount!}) breach maint margin:`
						);
						webhookMessage(
							`[${this.name}]: :x: error processing fill tx logs:\n${
								e.stack ? e.stack : e.message
							}`
						);
					});

				continue;
			}
		}

		if (logs.length > 0) {
			if (
				logs[logs.length - 1].includes('exceeded CUs meter at BPF instruction')
			) {
				return {
					filledNodes: successCount,
					exceededCUs: true,
				};
			}
		}

		return {
			filledNodes: successCount,
			exceededCUs: false,
		};
	}

	private async processBulkFillTxLogs(
		fillTxId: number,
		nodeToFill: NodeToFill,
		txSig: string,
		tx?: VersionedTransaction
	): Promise<number> {
		let txResp: VersionedTransactionResponse | null = null;
		let attempts = 0;
		while (txResp === null && attempts < CONFIRM_TX_ATTEMPTS) {
			logger.info(
				`(fillTxId: ${fillTxId}) waiting for https://solscan.io/tx/${txSig} to be confirmed`
			);
			txResp = await this.driftClient.connection.getTransaction(txSig, {
				commitment: 'confirmed',
				maxSupportedTransactionVersion: 0,
			});

			if (txResp === null) {
				if (tx !== undefined) {
					await this.driftClient.txSender.sendVersionedTransaction(
						tx,
						[],
						this.driftClient.opts
					);
				}
				attempts++;
				await this.sleep(1000);
			}
		}

		if (txResp === null) {
			logger.error(
				`(fillTxId: ${fillTxId})tx ${txSig} not found after ${attempts}`
			);
			return 0;
		}

		return (
			await this.handleTransactionLogs(nodeToFill, txResp.meta!.logMessages!)
		).filledNodes;
	}

	private async sendTxThroughJito(
		tx: VersionedTransaction,
		metadata: number | string
	) {
		if (this.bundleSender === undefined) {
			logger.error(`Called sendTxThroughJito without jito properly enabled`);
			return;
		}
		const slotsUntilNextLeader = this.bundleSender?.slotsUntilNextLeader();
		if (slotsUntilNextLeader !== undefined) {
			this.bundleSender.sendTransaction(tx, `(fillTxId: ${metadata})`);
		}
	}

	private usingJito(): boolean {
		return this.bundleSender !== undefined;
	}

	private canSendOutsideJito(): boolean {
		return (
			!this.usingJito() ||
			this.bundleSender?.strategy === 'non-jito-only' ||
			this.bundleSender?.strategy === 'hybrid'
		);
	}

	private slotsUntilJitoLeader(): number | undefined {
		if (!this.usingJito()) {
			return undefined;
		}
		return this.bundleSender?.slotsUntilNextLeader();
	}

	private async getBlockhashForTx(): Promise<string> {
		const cachedBlockhash = this.blockhashSubscriber.getLatestBlockhash(10);
		if (cachedBlockhash) {
			return cachedBlockhash.blockhash as string;
		}

		const recentBlockhash =
			await this.driftClient.connection.getLatestBlockhash({
				commitment: 'confirmed',
			});

		return recentBlockhash.blockhash;
	}

	protected async sendFillTxAndParseLogs(
		fillTxId: number,
		nodeSent: NodeToFill,
		tx: VersionedTransaction,
		buildForBundle: boolean,
		lutAccounts: Array<AddressLookupTableAccount>
	) {
		// @ts-ignore;
		tx.sign([this.driftClient.wallet.payer]);

		const { estTxSize, accountMetas, writeAccs, txAccounts } =
			getTransactionAccountMetas(tx, lutAccounts);

		const txStart = Date.now();
		if (buildForBundle) {
			await this.sendTxThroughJito(tx, fillTxId);
		} else if (this.canSendOutsideJito()) {
			this.driftClient.txSender
				.sendVersionedTransaction(tx, [], this.driftClient.opts, true)
				.then(async (txSig) => {
					logger.info(
						`Filled spot order (fillTxId: ${fillTxId}): https://solscan.io/tx/${txSig.txSig}`
					);

					const duration = Date.now() - txStart;
					const user = this.driftClient.getUser();
					if (this.sdkCallDurationHistogram) {
						this.sdkCallDurationHistogram!.record(duration, {
							...metricAttrFromUserAccount(
								user.getUserAccountPublicKey(),
								user.getUserAccount()
							),
							method: 'fillSpotOrder',
						});
					}

					const parseLogsStart = Date.now();
					this.processBulkFillTxLogs(fillTxId, nodeSent, txSig.txSig, tx)
						.then((successfulFills) => {
							const processBulkFillLogsDuration = Date.now() - parseLogsStart;
							logger.info(
								`parse logs took ${processBulkFillLogsDuration}ms, successfulFills ${successfulFills} (fillTxId: ${fillTxId})`
							);
						})
						.catch((err) => {
							const e = err as Error;
							logger.error(
								`Failed to process fill tx logs (fillTxId: ${fillTxId}):\n${
									e.stack ? e.stack : e.message
								}`
							);
							webhookMessage(
								`[${this.name}]: :x: error processing fill tx logs:\n${
									e.stack ? e.stack : e.message
								}`
							);
						});
				})
				.catch(async (e) => {
					const simError = e as SendTransactionError;
					logger.error(
						`Failed to send packed tx txAccountKeys: ${txAccounts} (${writeAccs} writeable) (fillTxId: ${fillTxId}), error: ${simError.message}`
					);

					if (e.message.includes('too large:')) {
						logger.error(
							`[${
								this.name
							}]: :boxing_glove: Tx too large, estimated to be ${estTxSize} (fillId: ${fillTxId}). ${
								e.message
							}\n${JSON.stringify(accountMetas)}`
						);
						webhookMessage(
							`[${
								this.name
							}]: :boxing_glove: Tx too large (fillId: ${fillTxId}). ${
								e.message
							}\n${JSON.stringify(accountMetas)}`
						);
						return;
					}

					if (simError.logs && simError.logs.length > 0) {
						await this.handleTransactionLogs(nodeSent, simError.logs);

						const errorCode = getErrorCode(e);
						logger.error(
							`Failed to send tx, sim error (fillTxId: ${fillTxId}) error code: ${errorCode}`
						);

						if (
							errorCode &&
							!errorCodesToSuppress.includes(errorCode) &&
							!(e as Error).message.includes('Transaction was not confirmed')
						) {
							logger.error(
								`Failed to send tx, sim error (fillTxId: ${fillTxId}) sim logs:\n${
									simError.logs ? simError.logs.join('\n') : ''
								}\n${e.stack || e}`
							);

							webhookMessage(
								`[${this.name}]: :x: error simulating tx:\n${
									simError.logs ? simError.logs.join('\n') : ''
								}\n${e.stack || e}`
							);
						}
					}
				})
				.finally(() => {
					this.clearThrottledNode(getNodeToFillSignature(nodeSent));
				});
		}
	}

	/**
	 *
	 * @param fillTxId id of current fill
	 * @param nodeToFill taker node to fill with list of makers to use
	 * @returns true if successful, false if fail, and should retry with fewer makers
	 */
	private async fillMultiMakerSpotNodes(
		fillTxId: number,
		nodeToFill: NodeToFill,
		buildForBundle: boolean,
		spotPrecision: BN
	): Promise<boolean> {
		const ixs: Array<TransactionInstruction> = [
			ComputeBudgetProgram.setComputeUnitLimit({
				units: 1_400_000,
			}),
		];
		if (!buildForBundle) {
			ixs.push(
				ComputeBudgetProgram.setComputeUnitPrice({
					microLamports: Math.floor(
						this.priorityFeeSubscriber.getCustomStrategyResult()
					),
				})
			);
		}

		try {
			const {
				makerInfos,
				takerUser,
				takerUserPubKey,
				takerUserSlot,
				marketType,
			} = await this.getNodeFillInfo(nodeToFill);

			logger.info(
				logMessageForNodeToFill(
					nodeToFill,
					takerUserPubKey,
					takerUserSlot,
					makerInfos,
					this.getMaxSlot(),
					`Filling multi maker spot node with ${nodeToFill.makerNodes.length} makers (fillTxId: ${fillTxId})`,
					spotPrecision,
					'SHOULD_NOT_HAVE_NO_MAKERS'
				)
			);

			if (!isVariant(marketType, 'perp')) {
				throw new Error('expected perp market type');
			}

			let makerInfosToUse = makerInfos;
			const buildTxWithMakerInfos = async (
				makers: DataAndSlot<MakerInfo>[]
			): Promise<SimulateAndGetTxWithCUsResponse> => {
				ixs.push(
					await this.driftClient.getFillSpotOrderIx(
						new PublicKey(takerUserPubKey),
						takerUser,
						nodeToFill.node.order!,
						undefined,
						makers.map((m) => m.data)
					)
				);

				if (this.revertOnFailure) {
					ixs.push(await this.driftClient.getRevertFillIx());
				}
				const simResult = await simulateAndGetTxWithCUs(
					ixs,
					this.driftClient.connection,
					this.driftClient.txSender,
					[this.driftLutAccount!],
					[],
					this.driftClient.opts,
					SIM_CU_ESTIMATE_MULTIPLIER,
					true,
					this.simulateTxForCUEstimate,
					await this.getBlockhashForTx(),
					false
				);
				return simResult;
			};

			let simResult = await buildTxWithMakerInfos(makerInfosToUse);
			let txAccounts = simResult.tx.message.getAccountKeys({
				addressLookupTableAccounts: [this.driftLutAccount!],
			}).length;
			let attempt = 0;
			while (txAccounts > MAX_ACCOUNTS_PER_TX && makerInfosToUse.length > 0) {
				logger.info(
					`(fillTxId: ${fillTxId} attempt ${attempt++}) Too many accounts, remove 1 and try again (had ${
						makerInfosToUse.length
					} maker and ${txAccounts} accounts)`
				);
				makerInfosToUse = makerInfosToUse.slice(0, makerInfosToUse.length - 1);
				simResult = await buildTxWithMakerInfos(makerInfosToUse);
				txAccounts = simResult.tx.message.getAccountKeys({
					addressLookupTableAccounts: [this.driftLutAccount!],
				}).length;
			}

			if (makerInfosToUse.length === 0) {
				logger.error(
					`No makerInfos left to use for multi maker spot node (fillTxId: ${fillTxId})`
				);
				return true;
			}

			logger.info(
				`tryFillMultiMakerSpotNodes estimated CUs: ${simResult.cuEstimate} (fillTxId: ${fillTxId})`
			);

			if (simResult.simError) {
				logger.error(
					`Error simulating multi maker spot node (fillTxId: ${fillTxId}): ${JSON.stringify(
						simResult.simError
					)}\nTaker slot: ${takerUserSlot}\nMaker slots: ${makerInfosToUse
						.map((m) => `  ${m.data.maker.toBase58()}: ${m.slot}`)
						.join('\n')}`
				);
				handleSimResultError(
					simResult,
					errorCodesToSuppress,
					`${this.name}: (fillTxId: ${fillTxId})`
				);
				if (simResult.simTxLogs) {
					const { exceededCUs } = await this.handleTransactionLogs(
						nodeToFill,
						simResult.simTxLogs
					);
					if (exceededCUs) {
						return false;
					}
				}
			} else {
				if (!this.dryRun) {
					const lutAccounts: Array<AddressLookupTableAccount> = [];
					this.driftLutAccount && lutAccounts.push(this.driftLutAccount);
					this.driftSpotLutAccount &&
						lutAccounts.push(this.driftSpotLutAccount);
					this.sendFillTxAndParseLogs(
						fillTxId,
						nodeToFill,
						simResult.tx,
						buildForBundle,
						lutAccounts
					);
				} else {
					logger.info(`dry run, not sending tx (fillTxId: ${fillTxId})`);
				}
			}
		} catch (e) {
			if (e instanceof Error) {
				logger.error(
					`Error filling multi maker spot node (fillTxId: ${fillTxId}): ${
						e.stack ? e.stack : e.message
					}`
				);
			}
		}

		return true;
	}

	/**
	 * It's difficult to estimate CU cost of multi maker ix, so we'll just send it in its own transaction.
	 * This will keep retrying with a smaller set of makers until it succeeds or runs out of makers.
	 * @param nodeToFill node with multiple makers
	 */
	private async tryFillMultiMakerSpotNode(
		fillTxId: number,
		nodeToFill: NodeToFill,
		buildForBundle: boolean,
		spotPrecision: BN
	) {
		let nodeWithMakerSet = nodeToFill;
		while (
			!(await this.fillMultiMakerSpotNodes(
				fillTxId,
				nodeWithMakerSet,
				buildForBundle,
				spotPrecision
			))
		) {
			const newMakerSet = nodeWithMakerSet.makerNodes
				.sort(() => 0.5 - Math.random())
				.slice(0, Math.ceil(nodeWithMakerSet.makerNodes.length / 2));
			nodeWithMakerSet = {
				node: nodeWithMakerSet.node,
				makerNodes: newMakerSet,
			};
			if (newMakerSet.length === 0) {
				logger.error(
					`No makers left to use for multi maker spot node (fillTxId: ${fillTxId})`
				);
				return;
			}
		}
	}

	private async tryFillSpotNode(
		nodeToFill: NodeToFill,
		fillTxId: number,
		buildForBundle: boolean,
		fallbackAskSource?: FallbackLiquiditySource,
		fallbackBidSource?: FallbackLiquiditySource
	) {
		const node = nodeToFill.node!;
		const order = node.order!;
		const spotMarket = this.driftClient.getSpotMarketAccount(
			order.marketIndex
		)!;
		const spotMarketPrecision = TEN.pow(new BN(spotMarket.decimals));

		if (nodeToFill.makerNodes.length > 1) {
			// do multi maker fills in a separate tx since they're larger
			return this.tryFillMultiMakerSpotNode(
				fillTxId,
				nodeToFill,
				buildForBundle,
				spotMarketPrecision
			);
		}

		const fallbackSource = isVariant(order.direction, 'short')
			? fallbackBidSource
			: fallbackAskSource;

		const {
			makerInfos,
			takerUser,
			takerUserPubKey,
			takerUserSlot,
			marketType,
		} = await this.getNodeFillInfo(nodeToFill);

		if (!isVariant(marketType, 'spot')) {
			throw new Error('expected spot market type');
		}

		const makerInfo = makerInfos.length > 0 ? makerInfos[0].data : undefined;
		let fulfillmentConfig:
			| SerumV3FulfillmentConfigAccount
			| PhoenixV1FulfillmentConfigAccount
			| undefined = undefined;
		if (makerInfo === undefined) {
			if (fallbackSource === 'serum') {
				fulfillmentConfig = this.serumFulfillmentConfigMap.get(
					nodeToFill.node.order!.marketIndex
				);
			} else if (fallbackSource === 'phoenix') {
				fulfillmentConfig = this.phoenixFulfillmentConfigMap.get(
					nodeToFill.node.order!.marketIndex
				);
			} else {
				logger.error(
					`makerInfo doesnt exist and unknown fallback source: ${fallbackSource} (fillTxId: ${fillTxId})`
				);
			}
		}

		logger.info(
			logMessageForNodeToFill(
				nodeToFill,
				takerUserPubKey,
				takerUserSlot,
				makerInfos,
				this.getMaxSlot(),
				`Filling spot node with ${nodeToFill.makerNodes.length} makers (fillTxId: ${fillTxId})`,
				spotMarketPrecision,
				fallbackSource as string
			)
		);

		const ixs = [
			ComputeBudgetProgram.setComputeUnitLimit({
				units: 1_400_000,
			}),
		];
		if (!buildForBundle) {
			const priorityFee = Math.floor(
				this.priorityFeeSubscriber.getCustomStrategyResult()
			);
			logger.info(`(fillTxId: ${fillTxId}) Using priority fee: ${priorityFee}`);
			ixs.push(
				ComputeBudgetProgram.setComputeUnitPrice({
					microLamports: priorityFee,
				})
			);
		}

		ixs.push(
			await this.driftClient.getFillSpotOrderIx(
				new PublicKey(takerUserPubKey),
				takerUser,
				nodeToFill.node.order,
				fulfillmentConfig,
				makerInfo
			)
		);

		if (this.revertOnFailure) {
			ixs.push(await this.driftClient.getRevertFillIx());
		}

		const lutAccounts: Array<AddressLookupTableAccount> = [];
		this.driftLutAccount && lutAccounts.push(this.driftLutAccount);
		this.driftSpotLutAccount && lutAccounts.push(this.driftSpotLutAccount);
		const simResult = await simulateAndGetTxWithCUs(
			ixs,
			this.driftClient.connection,
			this.driftClient.txSender,
			lutAccounts,
			[],
			this.driftClient.opts,
			SIM_CU_ESTIMATE_MULTIPLIER,
			true,
			this.simulateTxForCUEstimate,
			undefined,
			false
		);
		logger.info(
			`tryFillSpotNode estimated CUs: ${simResult.cuEstimate} (fillTxId: ${fillTxId})`
		);

		if (this.simulateTxForCUEstimate && simResult.simError) {
			logger.error(
				`simError: ${JSON.stringify(
					simResult.simError
				)} (fillTxId: ${fillTxId})`
			);
			handleSimResultError(
				simResult,
				errorCodesToSuppress,
				`${this.name}: (fillTxId: ${fillTxId})`
			);
			if (simResult.simTxLogs) {
				await this.handleTransactionLogs(nodeToFill, simResult.simTxLogs);
			}
		} else {
			if (!this.dryRun) {
				this.sendFillTxAndParseLogs(
					fillTxId,
					nodeToFill,
					simResult.tx,
					buildForBundle,
					lutAccounts
				);
			} else {
				logger.info(`dry run, not filling spot order (fillTxId: ${fillTxId})`);
			}
		}
	}

	private filterTriggerableNodes(nodeToTrigger: NodeToTrigger): boolean {
		if (nodeToTrigger.node.haveTrigger) {
			return false;
		}

		const now = Date.now();
		const nodeToFillSignature = getNodeToTriggerSignature(nodeToTrigger);
		const timeStartedToTriggerNode =
			this.triggeringNodes.get(nodeToFillSignature);
		if (timeStartedToTriggerNode) {
			if (timeStartedToTriggerNode + TRIGGER_ORDER_COOLDOWN_MS > now) {
				return false;
			}
		}

		return true;
	}

	private async executeFillableSpotNodesForMarket(
		fillableNodes: Array<NodesToFillWithContext>,
		buildForBundle: boolean
	) {
		for (const nodesToFillWithContext of fillableNodes) {
			for (const nodeToFill of nodesToFillWithContext.nodesToFill) {
				await this.tryFillSpotNode(
					nodeToFill,
					this.fillTxId++,
					buildForBundle,
					nodesToFillWithContext.fallbackAskSource,
					nodesToFillWithContext.fallbackBidSource
				);
			}
		}
	}

	private async executeTriggerableSpotNodesForMarket(
		triggerableNodes: Array<NodeToTrigger>,
		buildForBundle: boolean
	) {
		for (const nodeToTrigger of triggerableNodes) {
			nodeToTrigger.node.haveTrigger = true;

			const nodeSignature = getNodeToTriggerSignature(nodeToTrigger);
			this.triggeringNodes.set(nodeSignature, Date.now());

			const user = await this.userMap!.mustGetWithSlot(
				nodeToTrigger.node.userAccount.toString(),
				this.driftClient.userAccountSubscriptionConfig
			);
			const userAccount = user.data.getUserAccount();

			const ixs = [];
			ixs.push(
				await this.driftClient.getTriggerOrderIx(
					new PublicKey(nodeToTrigger.node.userAccount),
					userAccount,
					nodeToTrigger.node.order
				)
			);

			if (this.revertOnFailure) {
				ixs.push(await this.driftClient.getRevertFillIx());
			}

			const simResult = await simulateAndGetTxWithCUs(
				ixs,
				this.driftClient.connection,
				this.driftClient.txSender,
				[this.driftLutAccount!],
				[],
				this.driftClient.opts,
				SIM_CU_ESTIMATE_MULTIPLIER,
				true,
				this.simulateTxForCUEstimate
			);
			logger.info(
				`executeTriggerableSpotNodesForMarket estimated CUs: ${simResult.cuEstimate}`
			);

			if (this.simulateTxForCUEstimate && simResult.simError) {
				handleSimResultError(
					simResult,
					errorCodesToSuppress,
					`${this.name}: (executeTriggerableSpotNodesForMarket)`
				);
				logger.error(
					`executeTriggerableSpotNodesForMarket simError: (simError: ${JSON.stringify(
						simResult.simError
					)})`
				);
			} else {
				if (!this.dryRun) {
					if (buildForBundle) {
						await this.sendTxThroughJito(simResult.tx, 'trigger');
						this.removeTriggeringNodes(nodeToTrigger);
					} else if (this.canSendOutsideJito()) {
						this.driftClient
							.sendTransaction(simResult.tx)
							.then((txSig) => {
								logger.info(
									`Triggered user (account: ${nodeToTrigger.node.userAccount.toString()}, userSlot: ${
										user.slot
									}) order: ${nodeToTrigger.node.order.orderId.toString()}`
								);
								logger.info(`Tx: ${txSig}`);
							})
							.catch((error) => {
								nodeToTrigger.node.haveTrigger = false;

								const errorCode = getErrorCode(error);
								if (
									errorCode &&
									!errorCodesToSuppress.includes(errorCode) &&
									!(error as Error).message.includes(
										'Transaction was not confirmed'
									)
								) {
									logger.error(
										`Error(${errorCode}) triggering order for user(account: ${nodeToTrigger.node.userAccount.toString()}) order: ${nodeToTrigger.node.order.orderId.toString()} `
									);
									logger.error(error);
									webhookMessage(
										`[${
											this.name
										}]: Error(${errorCode}) triggering order for user(account: ${nodeToTrigger.node.userAccount.toString()}) order: ${nodeToTrigger.node.order.orderId.toString()} \n${
											error.stack ? error.stack : error.message
										} `
									);
								}
							})
							.finally(() => {
								this.removeTriggeringNodes(nodeToTrigger);
							});
					}
				} else {
					logger.info(`dry run, not triggering node`);
				}
			}
		}
	}

	private async trySpotFill(orderRecord?: OrderRecord) {
		const startTime = Date.now();
		let ran = false;

		try {
			await tryAcquire(this.periodicTaskMutex).runExclusive(async () => {
				const dlob = this.dlobSubscriber!.getDLOB();
				if (orderRecord && dlob) {
					dlob.insertOrder(
						orderRecord.order,
						orderRecord.user.toBase58(),
						orderRecord.order.slot.toNumber()
					);
				}

				this.pruneThrottledNode();

				// 1) get all fillable nodes
				const fillableNodes: Array<NodesToFillWithContext> = [];
				let triggerableNodes: Array<NodeToTrigger> = [];
				for (const market of this.driftClient.getSpotMarketAccounts()) {
					if (market.marketIndex === 0) {
						continue;
					}

					const { nodesToFill, nodesToTrigger } = this.getSpotNodesForMarket(
						market,
						dlob
					);
					fillableNodes.push(nodesToFill);
					triggerableNodes = triggerableNodes.concat(nodesToTrigger);
				}
				logger.debug(`spot fillableNodes ${fillableNodes.length}`);

				// filter out nodes that we know cannot be triggered (spot nodes will be filtered in executeFillableSpotNodesForMarket)
				const filteredTriggerableNodes = triggerableNodes.filter(
					this.filterTriggerableNodes.bind(this)
				);
				logger.debug(
					`filtered triggerable nodes from ${triggerableNodes.length} to ${filteredTriggerableNodes.length} `
				);

				const slotsUntilJito = this.slotsUntilJitoLeader();
				const buildForBundle =
					this.usingJito() &&
					slotsUntilJito !== undefined &&
					slotsUntilJito < SLOTS_UNTIL_JITO_LEADER_TO_SEND;

				await Promise.all([
					this.executeFillableSpotNodesForMarket(fillableNodes, buildForBundle),
					this.executeTriggerableSpotNodesForMarket(
						filteredTriggerableNodes,
						buildForBundle
					),
				]);

				if (this.attemptedFillsCounter && this.attemptedTriggersCounter) {
					const user = this.driftClient.getUser();
					this.attemptedFillsCounter!.add(
						fillableNodes.reduce(
							(acc, curr) => acc + curr.nodesToFill.length,
							0
						),
						metricAttrFromUserAccount(
							user.userAccountPublicKey,
							user.getUserAccount()
						)
					);
					this.attemptedTriggersCounter!.add(
						filteredTriggerableNodes.length,
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
				this.mutexBusyCounter!.add(
					1,
					metricAttrFromUserAccount(
						user.getUserAccountPublicKey(),
						user.getUserAccount()
					)
				);
			} else {
				logger.error('some other error:');
				console.error(e);
				if (e instanceof Error) {
					webhookMessage(
						`[${this.name}]: error trying to run main loop: \n${
							e.stack ? e.stack : e.message
						} `
					);
				}
			}
		} finally {
			if (ran) {
				const duration = Date.now() - startTime;
				const user = this.driftClient.getUser();
				if (this.tryFillDurationHistogram) {
					this.tryFillDurationHistogram!.record(
						duration,
						metricAttrFromUserAccount(
							user.getUserAccountPublicKey(),
							user.getUserAccount()
						)
					);
				}
				logger.debug(`trySpotFill done, took ${duration} ms`);

				await this.watchdogTimerMutex.runExclusive(async () => {
					this.watchdogTimerLastPatTime = Date.now();
				});
			}
		}
	}
}

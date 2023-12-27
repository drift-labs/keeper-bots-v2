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
	SerumFulfillmentConfigMap,
	SerumV3FulfillmentConfigAccount,
	OrderActionRecord,
	OrderRecord,
	convertToNumber,
	PRICE_PRECISION,
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
} from '@drift-labs/sdk';
import { Mutex, tryAcquire, E_ALREADY_LOCKED } from 'async-mutex';

import {
	AddressLookupTableAccount,
	ComputeBudgetProgram,
	GetVersionedTransactionConfig,
	PublicKey,
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
import { FillerConfig } from '../config';
import { getNodeToFillSignature, getNodeToTriggerSignature } from '../utils';

const THROTTLED_NODE_SIZE_TO_PRUNE = 10; // Size of throttled nodes to get to before pruning the map
const FILL_ORDER_BACKOFF = 10000; // Time to wait before trying a node again
const TRIGGER_ORDER_COOLDOWN_MS = 1000; // the time to wait before trying to a node in the triggering map again
const MAX_COMPUTE_UNIT_PRICE_MICRO_LAMPORTS = 1_000_000; // cap the computeUnitPrice to pay per fill tx

const errorCodesToSuppress = [
	6061, // 0x17AD Error Number: 6061. Error Message: Order does not exist.
	6078, // 0x17BE Error Number: 6078. Error Message: PerpMarketNotFound
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
	private pollingIntervalMs: number;
	private driftLutAccount?: AddressLookupTableAccount;
	private driftSpotLutAccount?: AddressLookupTableAccount;

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
		eventSubscriber?: EventSubscriber
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

		this.priorityFeeSubscriber = new PriorityFeeSubscriber({
			connection: this.driftClient.connection,
			frequencyMs: 5000,
			addresses: [
				new PublicKey('8BnEgHoWFysVcuFFX7QztDmzuH8r5ZFvyP3sYwn1XTh6'), // Openbook SOL/USDC is good indicator of fees
			],
		});
		this.revertOnFailure = config.revertOnFailure ?? true;
		logger.info(`${this.name}: revertOnFailure: ${this.revertOnFailure}`);

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
		for (const spotMarketConfig of config.SPOT_MARKETS) {
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

			if (spotMarketConfig.serumMarket) {
				// set up fulfillment config
				await this.serumFulfillmentConfigMap.add(
					spotMarketConfig.marketIndex,
					spotMarketConfig.serumMarket
				);

				// const serumConfigAccount = this.serumFulfillmentConfigMap.get(
				// 	spotMarketConfig.marketIndex
				// );
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
					const initSerumSubscriberStart = Date.now();
					logger.info(
						`Initializing SerumSubscriber for ${spotMarketConfig.symbol}...`
					);
					await serumSubscriber.subscribe();
					this.serumSubscribers.set(
						spotMarketConfig.marketIndex,
						serumSubscriber
					);
					logger.info(
						`Initialized SerumSubscriber in ${
							Date.now() - initSerumSubscriberStart
						}ms`
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
					const initPhoenixSubscriberStart = Date.now();
					logger.info(
						`Initializing PhoenixSubscriber for ${spotMarketConfig.symbol}...`
					);
					await phoenixSubscriber.subscribe();
					this.phoenixSubscribers.set(
						spotMarketConfig.marketIndex,
						phoenixSubscriber
					);
					logger.info(
						`Initialized PhoenixSubscriber in ${
							Date.now() - initPhoenixSubscriberStart
						}ms`
					);
				}
			}
		}

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

		await this.priorityFeeSubscriber.subscribe();

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
		makerInfo: MakerInfo | undefined;
		chUser: User;
		referrerInfo: ReferrerInfo | undefined;
		marketType: MarketType;
	}> {
		let makerInfo: MakerInfo | undefined;
		const makerNode = getMakerNodeFromNodeToFill(nodeToFill);
		if (makerNode) {
			const makerUserAccount = (
				await this.userMap!.mustGet(makerNode.userAccount!.toString())
			).getUserAccount();

			const makerAuthority = makerUserAccount.authority;
			const makerUserStats = (
				await this.userStatsMap!.mustGet(makerAuthority.toString())
			).userStatsAccountPublicKey;
			makerInfo = {
				maker: makerNode.userAccount!,
				makerUserAccount: makerUserAccount,
				order: makerNode.order,
				makerStats: makerUserStats,
			};
		}

		const node = nodeToFill.node;
		const order = node.order!;

		const chUser = await this.userMap!.mustGet(node.userAccount!.toString());
		const referrerInfo = (
			await this.userStatsMap!.mustGet(
				chUser.getUserAccount().authority.toString()
			)
		).getReferrerInfo();

		return Promise.resolve({
			makerInfo,
			chUser,
			referrerInfo,
			marketType: order.marketType,
		});
	}

	private nodeIsThrottled(nodeSignature: string): boolean {
		if (this.throttledNodes.has(nodeSignature)) {
			const lastFillAttempt = this.throttledNodes.get(nodeSignature) ?? 0;
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

	private pruneThrottledNode() {
		if (this.throttledNodes.size > THROTTLED_NODE_SIZE_TO_PRUNE) {
			for (const [key, value] of this.throttledNodes.entries()) {
				if (value + 2 * FILL_ORDER_BACKOFF > Date.now()) {
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

	private getFillSignatureFromUserAccountAndOrderId(
		userAccount: string,
		orderId: string
	): string {
		return `${userAccount}-${orderId}`;
	}

	/**
	 * Iterates through a tx's logs and handles it appropriately e.g. throttling users, updating metrics, etc.)
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
				const makerNodeSignature =
					this.getFillSignatureFromUserAccountAndOrderId(
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
						makerNode.userAccount!,
						(
							await this.userMap!.mustGet(makerNode.userAccount!.toString())
						).getUserAccount()
					)
					.then((txSig) => {
						logger.info(
							`Force cancelled orders for maker ${makerNode.userAccount!.toBase58()} due to breach of maintenance margin. Tx: ${txSig}`
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
				const takerNodeSignature =
					this.getFillSignatureFromUserAccountAndOrderId(
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
						node.userAccount!,
						(
							await this.userMap!.mustGet(node.userAccount!.toString())
						).getUserAccount()
					)
					.then((txSig) => {
						logger.info(
							`Force cancelled orders for user ${node.userAccount!.toBase58()} due to breach of maintenance margin. Tx: ${txSig}`
						);
					})
					.catch((e) => {
						console.error(e);
						logger.error(
							`Failed to send ForceCancelOrder Tx (error above), taker (${node.userAccount!.toBase58()}) breach maint margin:`
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
			logger.info(`waiting for https://solscan.io/tx/${txSig} to be confirmed`);
			tx = await this.driftClient.connection.getTransaction(txSig, config);
			attempts++;
			// sleep 1s
			await this.sleep(1000);
		}

		if (tx === null) {
			logger.error(`tx ${txSig} not found`);
			return 0;
		}

		return this.handleTransactionLogs(nodeToFill, tx.meta!.logMessages!);
	}

	private async tryFillSpotNode(
		nodeToFill: NodeToFill,
		fallbackAskSource?: FallbackLiquiditySource,
		fallbackBidSource?: FallbackLiquiditySource
	) {
		const nodeSignature = getNodeToFillSignature(nodeToFill);
		if (this.nodeIsThrottled(nodeSignature)) {
			logger.info(`Throttling ${nodeSignature}`);
			return Promise.resolve(undefined);
		}
		this.throttleNode(nodeSignature);

		const node = nodeToFill.node!;
		const order = node.order!;

		logger.info(
			`filling spot node: ${node.userAccount!.toString()}, ${order.orderId}`
		);

		const fallbackSource = isVariant(order.direction, 'short')
			? fallbackBidSource
			: fallbackAskSource;

		const { makerInfo, chUser, referrerInfo, marketType } =
			await this.getNodeFillInfo(nodeToFill);

		if (!isVariant(marketType, 'spot')) {
			throw new Error('expected spot market type');
		}

		const makerNode = getMakerNodeFromNodeToFill(nodeToFill);
		const spotMarket = this.driftClient.getSpotMarketAccount(
			order.marketIndex
		)!;
		const spotMarketPrecision = TEN.pow(new BN(spotMarket.decimals));
		if (makerNode) {
			logger.info(
				`filling spot node:\ntaker: ${node.userAccount!.toBase58()}-${
					order.orderId
				} ${convertToNumber(
					order.baseAssetAmountFilled,
					spotMarketPrecision
				)}/${convertToNumber(
					order.baseAssetAmount,
					spotMarketPrecision
				)} @ ${convertToNumber(
					order.price,
					PRICE_PRECISION
				)}\nmaker: ${makerNode.userAccount!.toBase58()}-${
					makerNode.order!.orderId
				} ${convertToNumber(
					makerNode.order!.baseAssetAmountFilled,
					spotMarketPrecision
				)}/${convertToNumber(
					makerNode.order!.baseAssetAmount,
					spotMarketPrecision
				)} @ ${convertToNumber(makerNode.order!.price, PRICE_PRECISION)}`
			);
		} else {
			logger.info(
				`filling spot node\ntaker: ${node.userAccount!.toBase58()}-${
					order.orderId
				} ${convertToNumber(
					order.baseAssetAmountFilled,
					spotMarketPrecision
				)}/${convertToNumber(
					order.baseAssetAmount,
					spotMarketPrecision
				)} @ ${convertToNumber(
					order.price,
					PRICE_PRECISION
				)}\nmaker: ${fallbackSource}`
			);
		}

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
					`makerInfo doesnt exist and unknown fallback source: ${fallbackSource}`
				);
			}
		}

		const ixs = [
			ComputeBudgetProgram.setComputeUnitLimit({
				units: 1_400_000,
			}),
			ComputeBudgetProgram.setComputeUnitPrice({
				microLamports: Number(
					Math.min(
						Math.floor(this.priorityFeeSubscriber.maxPriorityFee * 1.2),
						MAX_COMPUTE_UNIT_PRICE_MICRO_LAMPORTS
					)
				),
			}),
		];

		ixs.push(
			await this.driftClient.getFillSpotOrderIx(
				chUser.getUserAccountPublicKey(),
				chUser.getUserAccount(),
				nodeToFill.node.order,
				fulfillmentConfig,
				makerInfo,
				referrerInfo
			)
		);

		if (this.revertOnFailure) {
			ixs.push(await this.driftClient.getRevertFillIx());
		}

		const txStart = Date.now();
		const lutAccounts: Array<AddressLookupTableAccount> = [];
		this.driftLutAccount && lutAccounts.push(this.driftLutAccount);
		this.driftSpotLutAccount && lutAccounts.push(this.driftSpotLutAccount);
		this.driftClient.txSender
			.sendVersionedTransaction(
				await this.driftClient.txSender.getVersionedTransaction(
					ixs,
					lutAccounts,
					[],
					this.driftClient.opts
				),
				[],
				this.driftClient.opts
			)
			.then(async (txSig) => {
				logger.info(
					`Filled spot order ${nodeSignature}: https://solscan.io/tx/${txSig.txSig}`
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

				await this.processBulkFillTxLogs(nodeToFill, txSig.txSig);
			})
			.catch(async (e) => {
				const errorCode = getErrorCode(e);
				if (!errorCode) {
					console.error(e);
				} else {
					logger.error(
						`Failed to fill spot order for ${chUser
							.getUserAccountPublicKey()
							.toBase58()} order ${nodeToFill.node.order!.orderId} on market ${
							nodeToFill.node.order!.marketIndex
						} makers: ${
							nodeToFill.makerNodes.length
						} (errorCode: ${errorCode}): `
					);
				}

				if (e.logs) {
					await this.handleTransactionLogs(nodeToFill, e.logs);
				}

				if (
					errorCode &&
					!errorCodesToSuppress.includes(errorCode) &&
					!(e as Error).message.includes('Transaction was not confirmed')
				) {
					webhookMessage(
						`[${
							this.name
						}]: :x: error trying to fill spot orders: \n\nSim logs: \n${
							e.logs ? (e.logs as Array<string>).join('\n') : ''
						}\n\n${e.stack ? e.stack : e.message}`
					);
				}
			})
			.finally(() => {
				this.unthrottleNode(nodeSignature);
			});
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
		fillableNodes: Array<NodesToFillWithContext>
	) {
		for (const nodesToFillWithContext of fillableNodes) {
			for (const nodeToFill of nodesToFillWithContext.nodesToFill) {
				await this.tryFillSpotNode(
					nodeToFill,
					nodesToFillWithContext.fallbackAskSource,
					nodesToFillWithContext.fallbackBidSource
				);
			}
		}
	}

	private async executeTriggerableSpotNodesForMarket(
		triggerableNodes: Array<NodeToTrigger>
	) {
		for (const nodeToTrigger of triggerableNodes) {
			nodeToTrigger.node.haveTrigger = true;

			const nodeSignature = getNodeToTriggerSignature(nodeToTrigger);
			this.triggeringNodes.set(nodeSignature, Date.now());

			const user = await this.userMap!.mustGet(
				nodeToTrigger.node.userAccount.toString()
			);

			const ixs = [];
			ixs.push(
				await this.driftClient.getTriggerOrderIx(
					nodeToTrigger.node.userAccount,
					user.getUserAccount(),
					nodeToTrigger.node.order
				)
			);

			if (this.revertOnFailure) {
				ixs.push(await this.driftClient.getRevertFillIx());
			}

			const tx = await this.driftClient.txSender.getVersionedTransaction(
				ixs,
				[this.driftLutAccount!],
				undefined,
				this.driftClient.opts
			);

			this.driftClient
				.sendTransaction(tx)
				.then((txSig) => {
					logger.info(
						`Triggered user(account: ${nodeToTrigger.node.userAccount.toString()}) order: ${nodeToTrigger.node.order.orderId.toString()}`
					);
					logger.info(`Tx: ${txSig}`);
				})
				.catch((error) => {
					nodeToTrigger.node.haveTrigger = false;

					const errorCode = getErrorCode(error);
					if (
						errorCode &&
						!errorCodesToSuppress.includes(errorCode) &&
						!(error as Error).message.includes('Transaction was not confirmed')
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
						orderRecord.user,
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

				await Promise.all([
					this.executeFillableSpotNodesForMarket(fillableNodes),
					this.executeTriggerableSpotNodesForMarket(filteredTriggerableNodes),
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

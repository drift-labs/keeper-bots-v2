import {
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
	BulkAccountLoader,
	SlotSubscriber,
	PublicKey,
	DLOBNode,
	UserSubscriptionConfig,
	isOneOfVariant,
	DLOBSubscriber,
	EventSubscriber,
	OrderActionRecord,
	NodeToTrigger,
	UserAccount,
	getUserAccountPublicKey,
	PriorityFeeSubscriber,
	DataAndSlot,
} from '@drift-labs/sdk';
import { TxSigAndSlot } from '@drift-labs/sdk/lib/tx/types';
import { Mutex, tryAcquire, E_ALREADY_LOCKED } from 'async-mutex';

import {
	SendTransactionError,
	TransactionResponse,
	TransactionSignature,
	TransactionInstruction,
	ComputeBudgetProgram,
	GetVersionedTransactionConfig,
	AddressLookupTableAccount,
	Keypair,
	Connection,
	VersionedTransaction,
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
import {
	SimulateAndGetTxWithCUsResponse,
	decodeName,
	getFillSignatureFromUserAccountAndOrderId,
	getNodeToFillSignature,
	getNodeToTriggerSignature,
	handleSimResultError,
	simulateAndGetTxWithCUs,
	sleepMs,
} from '../utils';
import { selectMakers } from '../makerSelection';

const MAX_TX_PACK_SIZE = 1230; //1232;
const CU_PER_FILL = 260_000; // CU cost for a successful fill
const BURST_CU_PER_FILL = 350_000; // CU cost for a successful fill
const MAX_CU_PER_TX = 1_400_000; // seems like this is all budget program gives us...on devnet
const TX_COUNT_COOLDOWN_ON_BURST = 10; // send this many tx before resetting burst mode
const FILL_ORDER_THROTTLE_BACKOFF = 10000; // the time to wait before trying to fill a throttled (error filling) node again
const FILL_ORDER_BACKOFF = 2000; // the time to wait before trying to a node in the filling map again
const THROTTLED_NODE_SIZE_TO_PRUNE = 10; // Size of throttled nodes to get to before pruning the map
const TRIGGER_ORDER_COOLDOWN_MS = 1000; // the time to wait before trying to a node in the triggering map again
export const MAX_MAKERS_PER_FILL = 6; // max number of unique makers to include per fill
const MAX_ACCOUNTS_PER_TX = 64; // solana limit, track https://github.com/solana-labs/solana/issues/27241

const SETTLE_PNL_CHUNKS = 4;
const MAX_POSITIONS_PER_USER = 8;
export const SETTLE_POSITIVE_PNL_COOLDOWN_MS = 60_000;
const SIM_CU_ESTIMATE_MULTIPLIER = 1.15;

const errorCodesToSuppress = [
	6004, // 0x1774 Error Number: 6004. Error Message: SufficientCollateral.
	6010, // 0x177a Error Number: 6010. Error Message: User Has No Position In Market.
	6081, // 0x17c1 Error Number: 6081. Error Message: MarketWrongMutability.
	// 6078, // 0x17BE Error Number: 6078. Error Message: PerpMarketNotFound
	// 6087, // 0x17c7 Error Number: 6087. Error Message: SpotMarketNotFound.
	6239, // 0x185F Error Number: 6239. Error Message: RevertFill.
	6003, // 0x1773 Error Number: 6003. Error Message: Insufficient collateral.
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
	tx_sim_error_count = 'tx_sim_error_count',
	user_map_user_account_keys = 'user_map_user_account_keys',
	user_stats_map_authority_keys = 'user_stats_map_authority_keys',
}

function logMessageForNodeToFill(
	node: NodeToFill,
	takerUser: string,
	takerUserSlot: number,
	makerInfos: Array<DataAndSlot<MakerInfo>>,
	prefix?: string
): string {
	const takerNode = node.node;
	const takerOrder = takerNode.order;
	if (!takerOrder) {
		return 'no taker order';
	}

	if (node.makerNodes.length !== makerInfos.length) {
		logger.error(`makerNodes and makerInfos length mismatch`);
	}

	let msg = '';
	if (prefix) {
		msg += `${prefix}\n`;
	}
	msg += `taker on market ${takerOrder.marketIndex}: ${takerUser}-${
		takerOrder.orderId
	} (takerSlot: ${takerUserSlot}) ${getVariant(
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
	if (makerInfos.length > 0) {
		for (let i = 0; i < makerInfos.length; i++) {
			const maker = makerInfos[i].data;
			const makerSlot = makerInfos[i].slot;
			const makerOrder = maker.order!;
			msg += `  [${i}] market ${
				makerOrder.marketIndex
			}: ${maker.maker.toBase58()}-${
				makerOrder.orderId
			} (makerSlot: ${makerSlot}) ${getVariant(
				makerOrder.direction
			)} ${convertToNumber(
				makerOrder.baseAssetAmountFilled,
				BASE_PRECISION
			)}/${convertToNumber(
				makerOrder.baseAssetAmount,
				BASE_PRECISION
			)} @ ${convertToNumber(makerOrder.price, PRICE_PRECISION)} (offset: ${
				makerOrder.oraclePriceOffset / PRICE_PRECISION.toNumber()
			}) (orderType: ${getVariant(makerOrder.orderType)})\n`;
		}
	} else {
		msg += `  vAMM`;
	}
	return msg;
}

export type MakerNodeMap = Map<string, DLOBNode[]>;

export class FillerBot implements Bot {
	public readonly name: string;
	public readonly dryRun: boolean;
	public readonly defaultIntervalMs: number = 6000;

	protected slotSubscriber: SlotSubscriber;
	private bulkAccountLoader?: BulkAccountLoader;
	protected userStatsMapSubscriptionConfig: UserSubscriptionConfig;
	protected driftClient: DriftClient;
	protected eventSubscriber?: EventSubscriber;
	protected pollingIntervalMs: number;
	protected revertOnFailure?: boolean;
	protected simulateTxForCUEstimate?: boolean;
	protected lookupTableAccount?: AddressLookupTableAccount;
	protected jitoSearcherClient?: SearcherClient;
	protected jitoAuthKeypair?: Keypair;
	protected jitoTipAccount?: PublicKey;
	protected jitoLeaderNextSlot?: number;
	protected jitoLeaderNextSlotMutex = new Mutex();
	protected tipPayerKeypair?: Keypair;

	private dlobSubscriber?: DLOBSubscriber;

	private userMap?: UserMap;
	protected userStatsMap?: UserStatsMap;

	protected periodicTaskMutex = new Mutex();

	protected watchdogTimerMutex = new Mutex();
	protected watchdogTimerLastPatTime = Date.now();

	protected intervalIds: Array<NodeJS.Timer> = [];
	protected throttledNodes = new Map<string, number>();
	protected fillingNodes = new Map<string, number>();
	protected triggeringNodes = new Map<string, number>();

	protected useBurstCULimit = false;
	protected fillTxSinceBurstCU = 0;
	protected fillTxId = 0;
	protected lastSettlePnl = Date.now() - SETTLE_POSITIVE_PNL_COOLDOWN_MS;

	protected priorityFeeSubscriber: PriorityFeeSubscriber;

	// metrics
	protected metricsInitialized = false;
	protected metricsPort?: number;
	protected meter?: Meter;
	protected exporter?: PrometheusExporter;
	protected bootTimeMs?: number;

	protected runtimeSpecsGauge?: ObservableGauge;
	protected runtimeSpec: RuntimeSpec;
	protected sdkCallDurationHistogram?: Histogram;
	protected tryFillDurationHistogram?: Histogram;
	protected lastTryFillTimeGauge?: ObservableGauge;
	protected totalCollateralGauge?: ObservableGauge;
	protected unrealizedPnLGauge?: ObservableGauge;
	protected mutexBusyCounter?: Counter;
	protected attemptedFillsCounter?: Counter;
	protected attemptedTriggersCounter?: Counter;
	protected successfulFillsCounter?: Counter;
	protected observedFillsCountCounter?: Counter;
	protected txSimErrorCounter?: Counter;
	protected userMapUserAccountKeysGauge?: ObservableGauge;
	protected userStatsMapAuthorityKeysGauge?: ObservableGauge;

	constructor(
		slotSubscriber: SlotSubscriber,
		bulkAccountLoader: BulkAccountLoader | undefined,
		driftClient: DriftClient,
		userMap: UserMap | undefined,
		eventSubscriber: EventSubscriber | undefined,
		runtimeSpec: RuntimeSpec,
		config: FillerConfig,
		priorityFeeSubscriber: PriorityFeeSubscriber,
		jitoSearcherClient?: SearcherClient,
		jitoAuthKeypair?: Keypair,
		tipPayerKeypair?: Keypair
	) {
		this.name = config.botId;
		this.dryRun = config.dryRun;
		this.slotSubscriber = slotSubscriber;
		this.driftClient = driftClient;
		this.eventSubscriber = eventSubscriber;
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
		this.userMap = userMap;

		this.revertOnFailure = config.revertOnFailure ?? true;
		this.simulateTxForCUEstimate = config.simulateTxForCUEstimate ?? true;
		logger.info(
			`${this.name}: revertOnFailure: ${this.revertOnFailure}, simulateTxForCUEstimate: ${this.simulateTxForCUEstimate}`
		);

		this.jitoSearcherClient = jitoSearcherClient;
		this.jitoAuthKeypair = jitoAuthKeypair;
		this.tipPayerKeypair = tipPayerKeypair;
		const jitoEnabled = this.jitoSearcherClient && this.jitoAuthKeypair;
		if (jitoEnabled && this.jitoSearcherClient) {
			this.jitoSearcherClient.getTipAccounts().then(async (tipAccounts) => {
				this.jitoTipAccount = new PublicKey(
					tipAccounts[Math.floor(Math.random() * tipAccounts.length)]
				);
				logger.info(
					`${this.name}: jito tip account: ${this.jitoTipAccount.toBase58()}`
				);
				this.jitoLeaderNextSlot = (
					await this.jitoSearcherClient!.getNextScheduledLeader()
				).nextLeaderSlot;
			});

			this.slotSubscriber.eventEmitter.on('newSlot', async (slot: number) => {
				if (this.jitoLeaderNextSlot) {
					if (slot > this.jitoLeaderNextSlot) {
						try {
							await tryAcquire(this.jitoLeaderNextSlotMutex).runExclusive(
								async () => {
									logger.warn('LEADER REACHED, GETTING NEXT SLOT');
									this.jitoLeaderNextSlot = (
										await this.jitoSearcherClient!.getNextScheduledLeader()
									).nextLeaderSlot;
								}
							);
						} catch (e) {
							if (e !== E_ALREADY_LOCKED) {
								throw new Error(e as string);
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

		this.priorityFeeSubscriber = priorityFeeSubscriber;
		this.priorityFeeSubscriber.updateAddresses([
			new PublicKey('8BnEgHoWFysVcuFFX7QztDmzuH8r5ZFvyP3sYwn1XTh6'), // Openbook SOL/USDC
			new PublicKey('8UJgxaiQx5nTrdDgph5FiahMmzduuLTLf5WmsPegYA6W'), // sol-perp
		]);
	}

	protected initializeMetrics() {
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
						Array.from(new Array(20), (_, i) => i + 2000),
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

		this.meter.addBatchObservableCallback(
			async (batchObservableResult: BatchObservableResult) => {
				for (const user of this.driftClient.getUsers()) {
					const userAccount = user.getUserAccount();

					batchObservableResult.observe(
						this.totalCollateralGauge!,
						convertToNumber(user.getTotalCollateral(), QUOTE_PRECISION),
						metricAttrFromUserAccount(user.userAccountPublicKey, userAccount)
					);

					batchObservableResult.observe(
						this.unrealizedPnLGauge!,
						convertToNumber(user.getUnrealizedPNL(), QUOTE_PRECISION),
						metricAttrFromUserAccount(user.userAccountPublicKey, userAccount)
					);
				}
			},
			[this.totalCollateralGauge, this.unrealizedPnLGauge]
		);
	}

	public async init() {
		const startInitUserStatsMap = Date.now();
		logger.info(
			`Initializing userStatsMap ${
				this.userMap!.getUniqueAuthorities().length
			} auths`
		);

		// sync userstats once
		const userStatsLoader = new BulkAccountLoader(
			new Connection(this.driftClient.connection.rpcEndpoint),
			'confirmed',
			0
		);
		this.userStatsMap = new UserStatsMap(this.driftClient, userStatsLoader);

		// disable the initial sync since there are way too many authorties now
		// userStats will be lazily loaded (this.userStatsMap.mustGet) as fills occur.
		// await this.userStatsMap.sync(this.userMap!.getUniqueAuthorities());

		logger.info(
			`Initialized userMap: ${this.userMap!.size()}, userStatsMap: ${this.userStatsMap.size()}, took: ${
				Date.now() - startInitUserStatsMap
			} ms`
		);

		this.dlobSubscriber = new DLOBSubscriber({
			dlobSource: this.userMap!,
			slotSource: this.slotSubscriber,
			updateFrequency: this.pollingIntervalMs - 500,
			driftClient: this.driftClient,
		});
		await this.dlobSubscriber.subscribe();

		this.lookupTableAccount =
			await this.driftClient.fetchMarketLookupTableAccount();

		await webhookMessage(`[${this.name}]: started`);
	}

	public async reset() {
		for (const intervalId of this.intervalIds) {
			clearInterval(intervalId as NodeJS.Timeout);
		}
		this.intervalIds = [];

		this.eventSubscriber?.eventEmitter.removeAllListeners('newEvent');

		await this.dlobSubscriber!.unsubscribe();
		await this.userMap!.unsubscribe();
	}

	public async startIntervalLoop(_intervalMs?: number) {
		this.intervalIds.push(
			setInterval(this.tryFill.bind(this), this.pollingIntervalMs)
		);
		this.intervalIds.push(
			setInterval(
				this.settlePnls.bind(this),
				SETTLE_POSITIVE_PNL_COOLDOWN_MS / 2
			)
		);

		// Metrics should have been initialized by now, if at all
		if (this.observedFillsCountCounter) {
			this.eventSubscriber?.eventEmitter.on(
				'newEvent',
				async (record: WrappedEvent<any>) => {
					await this.userMap!.updateWithEventRecord(record);
					await this.userStatsMap!.updateWithEventRecord(record, this.userMap);

					if (record.eventType === 'OrderActionRecord') {
						const actionRecord = record as OrderActionRecord;

						if (isVariant(actionRecord.action, 'fill')) {
							if (isVariant(actionRecord.marketType, 'perp')) {
								const perpMarket = this.driftClient.getPerpMarketAccount(
									actionRecord.marketIndex
								);
								if (perpMarket) {
									this.observedFillsCountCounter!.add(1, {
										market: decodeName(perpMarket.name),
									});
								}
							}
						}
					}
				}
			);
		}

		logger.info(
			`${this.name} Bot started! (websocket: ${
				this.bulkAccountLoader === undefined
			})`
		);
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

		return healthy;
	}

	protected async getUserAccountAndSlotFromMap(
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

	protected async getDLOB(): Promise<DLOB> {
		return this.dlobSubscriber!.getDLOB();
	}

	protected getMaxSlot(): number {
		return Math.max(this.slotSubscriber.getSlot(), this.userMap!.getSlot());
	}

	protected logSlots() {
		logger.info(
			`slotSubscriber slot: ${this.slotSubscriber.getSlot()}, userMap slot: ${this.userMap!.getSlot()}`
		);
	}

	protected getPerpNodesForMarket(
		market: PerpMarketAccount,
		dlob: DLOB
	): {
		nodesToFill: Array<NodeToFill>;
		nodesToTrigger: Array<NodeToTrigger>;
	} {
		const marketIndex = market.marketIndex;

		const oraclePriceData =
			this.driftClient.getOracleDataForPerpMarket(marketIndex);

		const vAsk = calculateAskPrice(market, oraclePriceData);
		const vBid = calculateBidPrice(market, oraclePriceData);

		const fillSlot = this.getMaxSlot();

		return {
			nodesToFill: dlob.findNodesToFill(
				marketIndex,
				vBid,
				vAsk,
				fillSlot,
				Date.now() / 1000,
				MarketType.PERP,
				oraclePriceData,
				this.driftClient.getStateAccount(),
				this.driftClient.getPerpMarketAccount(marketIndex)!
			),
			nodesToTrigger: dlob.findNodesToTrigger(
				marketIndex,
				fillSlot,
				oraclePriceData.price,
				MarketType.PERP,
				this.driftClient.getStateAccount()
			),
		};
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

	protected clearThrottledNode(signature: string) {
		this.throttledNodes.delete(signature);
	}

	protected setThrottledNode(signature: string) {
		this.throttledNodes.set(signature, Date.now());
	}

	protected removeTriggeringNodes(node: NodeToTrigger) {
		this.triggeringNodes.delete(getNodeToTriggerSignature(node));
	}

	protected pruneThrottledNode() {
		if (this.throttledNodes.size > THROTTLED_NODE_SIZE_TO_PRUNE) {
			for (const [key, value] of this.throttledNodes.entries()) {
				if (value + 2 * FILL_ORDER_BACKOFF > Date.now()) {
					this.throttledNodes.delete(key);
				}
			}
		}
	}

	protected filterFillableNodes(nodeToFill: NodeToFill): boolean {
		if (!nodeToFill.node.order) {
			return false;
		}

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
		const nodeToFillSignature = getNodeToFillSignature(nodeToFill);
		if (this.fillingNodes.has(nodeToFillSignature)) {
			const timeStartedToFillNode =
				this.fillingNodes.get(nodeToFillSignature) || 0;
			if (timeStartedToFillNode + FILL_ORDER_BACKOFF > now) {
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
				)!,
				oraclePriceData,
				this.getMaxSlot(),
				Date.now() / 1000,
				this.driftClient.getStateAccount().minPerpAuctionDuration
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
					)!,
					oraclePriceData,
					this.getMaxSlot(),
					Date.now() / 1000,
					this.driftClient.getStateAccount().minPerpAuctionDuration
				)}`
			);
			logger.warn(
				` .     calculateBaseAssetAmountForAmmToFulfill: ${calculateBaseAssetAmountForAmmToFulfill(
					nodeToFill.node.order,
					this.driftClient.getPerpMarketAccount(
						nodeToFill.node.order.marketIndex
					)!,
					oraclePriceData,
					this.getMaxSlot()
				).toString()}`
			);
			return false;
		}

		// if making with vAMM, ensure valid oracle
		if (nodeToFill.makerNodes.length === 0) {
			const perpMarket = this.driftClient.getPerpMarketAccount(
				nodeToFill.node.order.marketIndex
			);
			if (perpMarket === undefined) {
				throw new Error(
					`Perp market is undefined for marketIndex ${nodeToFill.node.order.marketIndex}`
				);
			}
			const oracleIsValid = isOracleValid(
				perpMarket.amm,
				oraclePriceData,
				this.driftClient.getStateAccount().oracleGuardRails,
				this.getMaxSlot()
			);
			if (!oracleIsValid) {
				logger.error(`Oracle is not valid for market ${marketIndex}`);
				return false;
			}
		}

		return true;
	}

	protected filterTriggerableNodes(nodeToTrigger: NodeToTrigger): boolean {
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

	protected async getNodeFillInfo(nodeToFill: NodeToFill): Promise<{
		makerInfos: Array<DataAndSlot<MakerInfo>>;
		takerUserPubKey: string;
		takerUser: UserAccount;
		takerUserSlot: number;
		referrerInfo: ReferrerInfo | undefined;
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

		const takerUserPubKey = nodeToFill.node.userAccount!.toString();
		const takerUserAcct = await this.getUserAccountAndSlotFromMap(
			takerUserPubKey
		);
		const referrerInfo = (
			await this.userStatsMap!.mustGet(takerUserAcct.data.authority.toString())
		).getReferrerInfo();

		return Promise.resolve({
			makerInfos,
			takerUserPubKey,
			takerUser: takerUserAcct.data,
			takerUserSlot: takerUserAcct.slot,
			referrerInfo,
			marketType: nodeToFill.node.order!.marketType,
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
	protected calcCompactU16EncodedSize(array: any[], elemSize = 1): number {
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
	protected calcIxEncodedSize(ix: TransactionInstruction): number {
		return (
			1 +
			this.calcCompactU16EncodedSize(new Array(ix.keys.length), 1) +
			this.calcCompactU16EncodedSize(new Array(ix.data.byteLength), 1)
		);
	}

	/**
	 * Iterates through a tx's logs and handles it appropriately (e.g. throttling users, updating metrics, etc.)
	 *
	 * @param nodesFilled nodes that we sent a transaction to fill
	 * @param logs logs from tx.meta.logMessages or this.clearingHouse.program._events._eventParser.parseLogs
	 * @returns number of nodes successfully filled, and whether the tx exceeded CUs
	 */
	protected async handleTransactionLogs(
		nodesFilled: Array<NodeToFill>,
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
				if (filledNode) {
					logger.error(
						`assoc node (ixIdx: ${ixIdx}): ${filledNode.node.userAccount!.toString()}, ${
							filledNode.node.order!.orderId
						}; does not exist (filled by someone else); ${log}`
					);
					this.setThrottledNode(getNodeToFillSignature(filledNode));
				}
				errorThisFillIx = true;
				continue;
			}

			const makerBreachedMaintenanceMargin =
				isMakerBreachedMaintenanceMarginLog(log);
			if (makerBreachedMaintenanceMargin !== null) {
				logger.error(
					`Throttling maker breached maintenance margin: ${makerBreachedMaintenanceMargin}`
				);
				this.setThrottledNode(makerBreachedMaintenanceMargin);
				this.driftClient
					.forceCancelOrders(
						new PublicKey(makerBreachedMaintenanceMargin),
						(
							await this.getUserAccountAndSlotFromMap(
								makerBreachedMaintenanceMargin
							)
						).data
					)
					.then((txSig) => {
						logger.info(
							`Force cancelled orders for makers due to breach of maintenance margin. Tx: ${txSig}`
						);
					})
					.catch((e) => {
						console.error(e);
						logger.error(
							`Failed to send ForceCancelOrder Tx for maker (${makerBreachedMaintenanceMargin}) breach margin (error above):`
						);

						const errorCode = getErrorCode(e);

						if (
							errorCode &&
							!errorCodesToSuppress.includes(errorCode) &&
							!(e as Error).message.includes('Transaction was not confirmed')
						) {
							if (errorCode && this.txSimErrorCounter) {
								this.txSimErrorCounter!.add(1, {
									errorCode: errorCode.toString(),
								});
							}
							webhookMessage(
								`[${
									this.name
								}]: :x: error forceCancelling user ${makerBreachedMaintenanceMargin} for maker breaching margin tx logs:\n${
									e.stack ? e.stack : e.message
								}`
							);
						}
					});

				errorThisFillIx = true;
				break;
			}

			const takerBreachedMaintenanceMargin =
				isTakerBreachedMaintenanceMarginLog(log);
			if (takerBreachedMaintenanceMargin && nodesFilled[ixIdx]) {
				const filledNode = nodesFilled[ixIdx];
				const takerNodeSignature = filledNode.node.userAccount!;
				logger.error(
					`taker breach maint. margin, assoc node (ixIdx: ${ixIdx}): ${filledNode.node.userAccount!.toString()}, ${
						filledNode.node.order!.orderId
					}; (throttling ${takerNodeSignature} and force cancelling orders); ${log}`
				);
				this.setThrottledNode(takerNodeSignature);
				errorThisFillIx = true;

				this.driftClient
					.forceCancelOrders(
						new PublicKey(filledNode.node.userAccount!),
						(
							await this.getUserAccountAndSlotFromMap(
								filledNode.node.userAccount!.toString()
							)
						).data
					)
					.then((txSig) => {
						logger.info(
							`Force cancelled orders for user ${filledNode.node
								.userAccount!} due to breach of maintenance margin. Tx: ${txSig}`
						);
					})
					.catch((e) => {
						const userCanceling = filledNode.node.userAccount!.toString();
						console.error(e);
						logger.error(
							`Failed to send ForceCancelOrder Tx for taker (${userCanceling} - ${
								filledNode.node.order!.orderId
							}) breach maint. margin (error above):`
						);
						const errorCode = getErrorCode(e);
						if (
							errorCode &&
							!errorCodesToSuppress.includes(errorCode) &&
							!(e as Error).message.includes('Transaction was not confirmed')
						) {
							if (errorCode && this.txSimErrorCounter) {
								this.txSimErrorCounter!.add(1, {
									errorCode: errorCode.toString(),
								});
							}
							webhookMessage(
								`[${
									this.name
								}]: :x: error forceCancelling user ${userCanceling} for taker breaching maint. margin tx logs:\n${
									e.stack ? e.stack : e.message
								}`
							);
						}
					});

				continue;
			}

			const errFillingLog = isErrFillingLog(log);
			if (errFillingLog) {
				const orderId = errFillingLog[0];
				const userAcc = errFillingLog[1];
				const extractedSig = getFillSignatureFromUserAccountAndOrderId(
					userAcc,
					orderId
				);
				this.setThrottledNode(extractedSig);

				const filledNode = nodesFilled[ixIdx];
				const assocNodeSig = getNodeToFillSignature(filledNode);
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

	protected async processBulkFillTxLogs(
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
			await sleepMs(1000);
		}

		if (tx === null) {
			logger.error(`tx ${txSig} not found`);
			return 0;
		}

		const { filledNodes } = await this.handleTransactionLogs(
			nodesFilled,
			tx.meta!.logMessages
		);
		return filledNodes;
	}

	protected removeFillingNodes(nodes: Array<NodeToFill>) {
		for (const node of nodes) {
			this.fillingNodes.delete(getNodeToFillSignature(node));
		}
	}

	protected async sendFillTxThroughJito(
		fillTxId: number,
		tx: VersionedTransaction
	) {
		const slotsUntilNextLeader = this.jitoLeaderNextSlot! - this.getMaxSlot();
		logger.info(
			`next jito leader is in ${slotsUntilNextLeader} slots (fillTxId: ${fillTxId})`
		);
		if (slotsUntilNextLeader > 2) {
			logger.info(
				`next jito leader is too far away, skipping... (fillTxId: ${fillTxId})`
			);
			return ['', 0];
		}

		// should probably do this in background
		const blockHash =
			await this.driftClient.provider.connection.getLatestBlockhash(
				'confirmed'
			);

		// @ts-ignore
		tx.sign([this.driftClient.wallet.payer]);
		// const signedTx = await this.driftClient.provider.wallet.signTransaction(tx);
		let b: Bundle | Error = new Bundle(
			// [new VersionedTransaction(signedTx.compileMessage())],
			[tx],
			2
		);
		b = b.addTipTx(
			this.tipPayerKeypair!,
			100_000, // TODO: make this configurable?
			this.jitoTipAccount!,
			blockHash.blockhash
		);
		if (b instanceof Error) {
			logger.error(
				`failed to attach tip: ${b.message} (fillTxId: ${fillTxId})`
			);
			return ['', 0];
		}
		this.jitoSearcherClient!.sendBundle(b)
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
	}

	protected async sendFillTxAndParseLogs(
		fillTxId: number,
		nodesSent: Array<NodeToFill>,
		tx: VersionedTransaction
	) {
		let txResp: Promise<TxSigAndSlot> | undefined = undefined;
		let estTxSize: number | undefined = undefined;
		let txAccounts = 0;
		let writeAccs = 0;
		const accountMetas: any[] = [];
		const txStart = Date.now();
		if (this.jitoSearcherClient && this.jitoAuthKeypair) {
			await this.sendFillTxThroughJito(fillTxId, tx);
		} else {
			estTxSize = tx.message.serialize().length;
			const acc = tx.message.getAccountKeys({
				addressLookupTableAccounts: [this.lookupTableAccount!],
			});
			txAccounts = acc.length;
			for (let i = 0; i < txAccounts; i++) {
				const meta: any = {};
				if (tx.message.isAccountWritable(i)) {
					writeAccs++;
					meta['writeable'] = true;
				}
				if (tx.message.isAccountSigner(i)) {
					meta['signer'] = true;
				}
				meta['address'] = acc.get(i)!.toBase58();
				accountMetas.push(meta);
			}

			txResp = this.driftClient.txSender.sendVersionedTransaction(
				tx,
				[],
				this.driftClient.opts
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
					if (this.sdkCallDurationHistogram) {
						this.sdkCallDurationHistogram!.record(duration, {
							...metricAttrFromUserAccount(
								user.getUserAccountPublicKey(),
								user.getUserAccount()
							),
							method: 'sendTx',
						});
					}

					const parseLogsStart = Date.now();
					this.processBulkFillTxLogs(nodesSent, resp.txSig)
						.then((successfulFills) => {
							const processBulkFillLogsDuration = Date.now() - parseLogsStart;
							logger.info(
								`parse logs took ${processBulkFillLogsDuration}ms, filled ${successfulFills} (fillTxId: ${fillTxId})`
							);

							// record successful fills
							const user = this.driftClient.getUser();
							this.successfulFillsCounter!.add(
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
						await this.handleTransactionLogs(nodesSent, simError.logs);

						const errorCode = getErrorCode(e);
						logger.error(
							`Failed to send tx, sim error (fillTxId: ${fillTxId}) error code: ${errorCode}`
						);

						if (
							errorCode &&
							!errorCodesToSuppress.includes(errorCode) &&
							!(e as Error).message.includes('Transaction was not confirmed')
						) {
							if (this.txSimErrorCounter) {
								this.txSimErrorCounter!.add(1, {
									errorCode: errorCode.toString(),
								});
							}

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
					this.removeFillingNodes(nodesSent);
				});
		}
	}

	/**
	 *
	 * @param fillTxId id of current fill
	 * @param nodeToFill taker node to fill with list of makers to use
	 * @returns true if successful, false if fail, and should retry with fewer makers
	 */
	private async fillMultiMakerPerpNodes(
		fillTxId: number,
		nodeToFill: NodeToFill
	): Promise<boolean> {
		const ixs = [
			ComputeBudgetProgram.setComputeUnitLimit({
				units: 1_400_000,
			}),
			ComputeBudgetProgram.setComputeUnitPrice({
				microLamports: Math.floor(
					this.priorityFeeSubscriber.getCustomStrategyResult()
				),
			}),
		];

		try {
			const {
				makerInfos,
				takerUser,
				takerUserPubKey,
				takerUserSlot,
				referrerInfo,
				marketType,
			} = await this.getNodeFillInfo(nodeToFill);

			logger.info(
				logMessageForNodeToFill(
					nodeToFill,
					takerUserPubKey,
					takerUserSlot,
					makerInfos,
					`Filling multi maker perp node with ${nodeToFill.makerNodes.length} makers (fillTxId: ${fillTxId})`
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
					await this.driftClient.getFillPerpOrderIx(
						await getUserAccountPublicKey(
							this.driftClient.program.programId,
							takerUser.authority,
							takerUser.subAccountId
						),
						takerUser,
						nodeToFill.node.order!,
						makers.map((m) => m.data),
						referrerInfo
					)
				);

				this.fillingNodes.set(getNodeToFillSignature(nodeToFill), Date.now());

				if (this.revertOnFailure) {
					ixs.push(await this.driftClient.getRevertFillIx());
				}
				const simResult = await simulateAndGetTxWithCUs(
					ixs,
					this.driftClient.connection,
					this.driftClient.txSender,
					[this.lookupTableAccount!],
					[],
					this.driftClient.opts,
					SIM_CU_ESTIMATE_MULTIPLIER,
					true,
					this.simulateTxForCUEstimate
				);
				return simResult;
			};

			let simResult = await buildTxWithMakerInfos(makerInfosToUse);
			let txAccounts = simResult.tx.message.getAccountKeys({
				addressLookupTableAccounts: [this.lookupTableAccount!],
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
					addressLookupTableAccounts: [this.lookupTableAccount!],
				}).length;
			}

			if (makerInfosToUse.length === 0) {
				logger.error(
					`No makerInfos left to use for multi maker perp node (fillTxId: ${fillTxId})`
				);
				return true;
			}

			logger.info(
				`tryFillMultiMakerPerpNodes estimated CUs: ${simResult.cuEstimate} (fillTxId: ${fillTxId})`
			);

			if (simResult.simError) {
				logger.error(
					`Error simulating multi maker perp node (fillTxId: ${fillTxId}): ${JSON.stringify(
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
						[nodeToFill],
						simResult.simTxLogs
					);
					if (exceededCUs) {
						return false;
					}
				}
			} else {
				if (!this.dryRun) {
					this.sendFillTxAndParseLogs(fillTxId, [nodeToFill], simResult.tx);
				} else {
					logger.info(`dry run, not sending tx (fillTxId: ${fillTxId})`);
				}
			}
		} catch (e) {
			if (e instanceof Error) {
				logger.error(
					`Error filling multi maker perp node (fillTxId: ${fillTxId}): ${
						e.stack ? e.stack : e.message
					}`
				);
			}
		}
		return true;
	}

	/**
	 * It's difficult to estimate CU cost of multi maker ix, so we'll just send it in its own transaction
	 * @param nodeToFill node with multiple makers
	 */
	protected async tryFillMultiMakerPerpNodes(nodeToFill: NodeToFill) {
		const fillTxId = this.fillTxId++;

		let nodeWithMakerSet = nodeToFill;
		while (!(await this.fillMultiMakerPerpNodes(fillTxId, nodeWithMakerSet))) {
			const newMakerSet = nodeWithMakerSet.makerNodes
				.sort(() => 0.5 - Math.random())
				.slice(0, Math.ceil(nodeWithMakerSet.makerNodes.length / 2));
			nodeWithMakerSet = {
				node: nodeWithMakerSet.node,
				makerNodes: newMakerSet,
			};
			if (newMakerSet.length === 0) {
				logger.error(
					`No makers left to use for multi maker perp node (fillTxId: ${fillTxId})`
				);
				return;
			}
		}
	}

	protected async tryBulkFillPerpNodes(
		nodesToFill: Array<NodeToFill>
	): Promise<number> {
		let nodesSent = 0;
		const marketNodeMap = new Map<number, Array<NodeToFill>>();
		for (const nodeToFill of nodesToFill) {
			const marketIndex = nodeToFill.node.order!.marketIndex;
			if (!marketNodeMap.has(marketIndex)) {
				marketNodeMap.set(marketIndex, []);
			}
			marketNodeMap.get(marketIndex)!.push(nodeToFill);
		}

		for (const nodesToFillForMarket of marketNodeMap.values()) {
			nodesSent += await this.tryBulkFillPerpNodesForMarket(
				nodesToFillForMarket
			);
		}

		return nodesSent;
	}

	protected async tryBulkFillPerpNodesForMarket(
		nodesToFill: Array<NodeToFill>
	): Promise<number> {
		const ixs = [
			ComputeBudgetProgram.setComputeUnitLimit({
				units: 1_400_000,
			}),
			ComputeBudgetProgram.setComputeUnitPrice({
				microLamports: Math.floor(
					this.priorityFeeSubscriber.lastCustomStrategyResult
				),
			}),
		];

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

		const computeBudgetIx = ixs[0];
		computeBudgetIx.keys.forEach((key) =>
			uniqueAccounts.add(key.pubkey.toString())
		);
		uniqueAccounts.add(computeBudgetIx.programId.toString());

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
		const startingIxsSize = ixs.length;
		const fillTxId = this.fillTxId++;
		for (const [idx, nodeToFill] of nodesToFill.entries()) {
			// do multi maker fills in a separate tx since they're larger
			if (nodeToFill.makerNodes.length > 1) {
				await this.tryFillMultiMakerPerpNodes(nodeToFill);
				nodesSent.push(nodeToFill);
				continue;
			}

			const {
				makerInfos,
				takerUser,
				takerUserPubKey,
				takerUserSlot,
				referrerInfo,
				marketType,
			} = await this.getNodeFillInfo(nodeToFill);

			logger.info(
				logMessageForNodeToFill(
					nodeToFill,
					takerUserPubKey,
					takerUserSlot,
					makerInfos,
					`Filling perp node ${idx} (fillTxId: ${fillTxId})`
				)
			);
			this.logSlots();

			if (!isVariant(marketType, 'perp')) {
				throw new Error('expected perp market type');
			}

			const ix = await this.driftClient.getFillPerpOrderIx(
				await getUserAccountPublicKey(
					this.driftClient.program.programId,
					takerUser.authority,
					takerUser.subAccountId
				),
				takerUser,
				nodeToFill.node.order!,
				makerInfos.map((m) => m.data),
				referrerInfo
			);

			if (!ix) {
				logger.error(`failed to generate an ix`);
				break;
			}

			this.fillingNodes.set(getNodeToFillSignature(nodeToFill), Date.now());

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
				(runningTxSize + newIxCost + additionalAccountsCost >=
					MAX_TX_PACK_SIZE ||
					runningCUUsed + cuToUsePerFill >= MAX_CU_PER_TX) &&
				ixs.length > startingIxsSize + 1 // ensure at least 1 attempted fill
			) {
				logger.info(
					`Fully packed fill tx (ixs: ${ixs.length}): est. tx size ${
						runningTxSize + newIxCost + additionalAccountsCost
					}, max: ${MAX_TX_PACK_SIZE}, est. CU used: expected ${
						runningCUUsed + cuToUsePerFill
					}, max: ${MAX_CU_PER_TX}, (fillTxId: ${fillTxId})`
				);
				break;
			}

			// add to tx
			logger.info(
				`including taker ${(
					await getUserAccountPublicKey(
						this.driftClient.program.programId,
						takerUser.authority,
						takerUser.subAccountId
					)
				).toString()}-${nodeToFill.node.order!.orderId.toString()} (slot: ${takerUserSlot}) (fillTxId: ${fillTxId}), maker: ${makerInfos
					.map((m) => `${m.data.maker.toBase58()}: ${m.slot}`)
					.join(', ')}`
			);
			ixs.push(ix);
			runningTxSize += newIxCost + additionalAccountsCost;
			runningCUUsed += cuToUsePerFill;

			newAccounts.forEach((key) => uniqueAccounts.add(key.toString()));
			idxUsed++;
			nodesSent.push(nodeToFill);
		}

		if (idxUsed === 0) {
			return nodesSent.length;
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

		const simResult = await simulateAndGetTxWithCUs(
			ixs,
			this.driftClient.connection,
			this.driftClient.txSender,
			[this.lookupTableAccount!],
			[],
			this.driftClient.opts,
			SIM_CU_ESTIMATE_MULTIPLIER,
			true,
			this.simulateTxForCUEstimate
		);
		logger.info(
			`tryBulkFillPerpNodes estimated CUs: ${simResult.cuEstimate} (fillTxId: ${fillTxId})`
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
				await this.handleTransactionLogs(nodesToFill, simResult.simTxLogs);
			}
		} else {
			if (!this.dryRun) {
				this.sendFillTxAndParseLogs(fillTxId, nodesSent, simResult.tx);
			} else {
				logger.info(`dry run, not sending tx (fillTxId: ${fillTxId})`);
			}
		}

		return nodesSent.length;
	}

	protected filterPerpNodesForMarket(
		fillableNodes: Array<NodeToFill>,
		triggerableNodes: Array<NodeToTrigger>
	): {
		filteredFillableNodes: Array<NodeToFill>;
		filteredTriggerableNodes: Array<NodeToTrigger>;
	} {
		const seenFillableNodes = new Set<string>();
		const filteredFillableNodes = fillableNodes.filter((node) => {
			const sig = getNodeToFillSignature(node);
			if (seenFillableNodes.has(sig)) {
				return false;
			}
			seenFillableNodes.add(sig);
			return this.filterFillableNodes(node);
		});

		const seenTriggerableNodes = new Set<string>();
		const filteredTriggerableNodes = triggerableNodes.filter((node) => {
			const sig = getNodeToTriggerSignature(node);
			if (seenTriggerableNodes.has(sig)) {
				return false;
			}
			seenTriggerableNodes.add(sig);
			return this.filterTriggerableNodes(node);
		});

		return {
			filteredFillableNodes,
			filteredTriggerableNodes,
		};
	}

	protected async executeFillablePerpNodesForMarket(
		fillableNodes: Array<NodeToFill>
	) {
		let filledNodeCount = 0;
		while (filledNodeCount < fillableNodes.length) {
			const attemptedFills = await this.tryBulkFillPerpNodes(
				fillableNodes.slice(filledNodeCount)
			);
			filledNodeCount += attemptedFills;

			// record fill attempts
			const user = this.driftClient.getUser();
			if (this.attemptedFillsCounter) {
				this.attemptedFillsCounter!.add(
					attemptedFills,
					metricAttrFromUserAccount(
						user.userAccountPublicKey,
						user.getUserAccount()
					)
				);
			}
		}
	}

	protected async executeTriggerablePerpNodesForMarket(
		triggerableNodes: Array<NodeToTrigger>
	) {
		for (const nodeToTrigger of triggerableNodes) {
			nodeToTrigger.node.haveTrigger = true;
			const user = await this.getUserAccountAndSlotFromMap(
				nodeToTrigger.node.userAccount.toString()
			);
			logger.info(
				`trying to trigger (account: ${nodeToTrigger.node.userAccount.toString()}, slot: ${
					user.slot
				}) order ${nodeToTrigger.node.order.orderId.toString()}`
			);

			const nodeSignature = getNodeToTriggerSignature(nodeToTrigger);
			this.triggeringNodes.set(nodeSignature, Date.now());

			const ixs = [];
			ixs.push(
				await this.driftClient.getTriggerOrderIx(
					new PublicKey(nodeToTrigger.node.userAccount),
					user.data,
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
				[this.lookupTableAccount!],
				[],
				this.driftClient.opts,
				SIM_CU_ESTIMATE_MULTIPLIER,
				true,
				this.simulateTxForCUEstimate
			);
			logger.info(
				`executeTriggerablePerpNodesForMarket estimated CUs: ${simResult.cuEstimate}`
			);

			if (this.simulateTxForCUEstimate && simResult.simError) {
				logger.error(
					`executeTriggerablePerpNodesForMarket simError: (simError: ${JSON.stringify(
						simResult.simError
					)})`
				);
				handleSimResultError(
					simResult,
					errorCodesToSuppress,
					`${this.name}: (executeTriggerablePerpNodesForMarket)`
				);
			} else {
				if (!this.dryRun) {
					this.driftClient
						.sendTransaction(simResult.tx)
						.then((txSig) => {
							logger.info(
								`Triggered user (account: ${nodeToTrigger.node.userAccount.toString()}) order: ${nodeToTrigger.node.order.orderId.toString()}`
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
									`Error (${errorCode}) triggering order for user (account: ${nodeToTrigger.node.userAccount.toString()}) order: ${nodeToTrigger.node.order.orderId.toString()}`
								);
								logger.error(error);
								webhookMessage(
									`[${
										this.name
									}]: :x: Error (${errorCode}) triggering order for user (account: ${nodeToTrigger.node.userAccount.toString()}) order: ${nodeToTrigger.node.order.orderId.toString()}\n${
										error.stack ? error.stack : error.message
									}`
								);
							}
						})
						.finally(() => {
							this.removeTriggeringNodes(nodeToTrigger);
						});
				} else {
					logger.info(`dry run, not triggering node`);
				}
			}
		}

		const user = this.driftClient.getUser();
		if (this.attemptedFillsCounter) {
			this.attemptedTriggersCounter!.add(
				triggerableNodes.length,
				metricAttrFromUserAccount(
					user.userAccountPublicKey,
					user.getUserAccount()
				)
			);
		}
	}

	protected async settlePnls() {
		const user = this.driftClient.getUser();
		const marketIds = user
			.getActivePerpPositions()
			.map((pos) => pos.marketIndex);
		const now = Date.now();
		if (marketIds.length === MAX_POSITIONS_PER_USER) {
			if (now < this.lastSettlePnl + SETTLE_POSITIVE_PNL_COOLDOWN_MS) {
				logger.info(`Want to settle positive pnl, but in cooldown...`);
			} else {
				const settlePnlPromises: Array<Promise<TxSigAndSlot>> = [];
				for (let i = 0; i < marketIds.length; i += SETTLE_PNL_CHUNKS) {
					const marketIdChunks = marketIds.slice(i, i + SETTLE_PNL_CHUNKS);
					try {
						const ixs = [
							ComputeBudgetProgram.setComputeUnitLimit({
								units: 2_000_000,
							}),
						];
						ixs.push(
							...(await this.driftClient.getSettlePNLsIxs(
								[
									{
										settleeUserAccountPublicKey: user.getUserAccountPublicKey(),
										settleeUserAccount: this.driftClient.getUserAccount()!,
									},
								],
								marketIdChunks
							))
						);

						const simResult = await simulateAndGetTxWithCUs(
							ixs,
							this.driftClient.connection,
							this.driftClient.txSender,
							[this.lookupTableAccount!],
							[],
							this.driftClient.opts,
							SIM_CU_ESTIMATE_MULTIPLIER,
							true,
							this.simulateTxForCUEstimate
						);
						logger.info(`settlePnls estimatedCUs: ${simResult.cuEstimate}`);
						if (this.simulateTxForCUEstimate && simResult.simError) {
							logger.info(
								`settlePnls simError: ${JSON.stringify(simResult.simError)}`
							);
							handleSimResultError(
								simResult,
								errorCodesToSuppress,
								`${this.name}: (settlePnls)`
							);
						} else {
							if (!this.dryRun) {
								settlePnlPromises.push(
									this.driftClient.txSender.sendVersionedTransaction(
										simResult.tx,
										[],
										this.driftClient.opts
									)
								);
							} else {
								logger.info(`dry run, skipping settlePnls)`);
							}
						}
					} catch (err) {
						if (!(err instanceof Error)) {
							return;
						}
						const errorCode = getErrorCode(err) ?? 0;
						logger.error(
							`Error code: ${errorCode} while settling pnls for markets ${JSON.stringify(
								marketIds
							)}: ${err.message}`
						);
						console.error(err);
					}
				}
				try {
					const txs = await Promise.all(settlePnlPromises);
					for (const tx of txs) {
						logger.info(
							`Settle positive PNLs tx: https://solscan/io/tx/${tx.txSig}`
						);
					}
				} catch (e) {
					logger.error(`Error settling positive pnls: ${e}`);
				}
				this.lastSettlePnl = now;
			}
		}
	}

	protected async tryFill() {
		const startTime = Date.now();
		let ran = false;
		try {
			await tryAcquire(this.periodicTaskMutex).runExclusive(async () => {
				const dlob = await this.getDLOB();

				this.pruneThrottledNode();

				// 1) get all fillable nodes
				let fillableNodes: Array<NodeToFill> = [];
				let triggerableNodes: Array<NodeToTrigger> = [];
				for (const market of this.driftClient.getPerpMarketAccounts()) {
					try {
						const { nodesToFill, nodesToTrigger } = this.getPerpNodesForMarket(
							market,
							dlob
						);
						fillableNodes = fillableNodes.concat(nodesToFill);
						triggerableNodes = triggerableNodes.concat(nodesToTrigger);
					} catch (e) {
						if (e instanceof Error) {
							console.error(e);
							webhookMessage(
								`[${this.name}]: :x: Failed to get fillable nodes for market ${
									market.marketIndex
								}:\n${e.stack ? e.stack : e.message}`
							);
						}
						continue;
					}
				}

				// filter out nodes that we know cannot be filled
				const { filteredFillableNodes, filteredTriggerableNodes } =
					this.filterPerpNodesForMarket(fillableNodes, triggerableNodes);
				logger.debug(
					`filtered fillable nodes from ${fillableNodes.length} to ${filteredFillableNodes.length}, filtered triggerable nodes from ${triggerableNodes.length} to ${filteredTriggerableNodes.length}`
				);

				// fill the perp nodes
				await Promise.all([
					this.executeFillablePerpNodesForMarket(filteredFillableNodes),
					this.executeTriggerablePerpNodesForMarket(filteredTriggerableNodes),
				]);

				// check if should settle positive pnl

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
				if (e instanceof Error) {
					webhookMessage(
						`[${this.name}]: :x: uncaught error:\n${
							e.stack ? e.stack : e.message
						}`
					);
				}
				throw e;
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
				logger.debug(`tryFill done, took ${duration}ms`);

				await this.watchdogTimerMutex.runExclusive(async () => {
					this.watchdogTimerLastPatTime = Date.now();
				});
			}
		}
	}
}

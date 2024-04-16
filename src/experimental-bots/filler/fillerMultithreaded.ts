/* eslint-disable @typescript-eslint/no-non-null-assertion */
import {
	BlockhashSubscriber,
	BulkAccountLoader,
	DataAndSlot,
	decodeUser,
	DLOBNode,
	DriftClient,
	getOrderSignature,
	getUserAccountPublicKey,
	isFillableByVAMM,
	isOneOfVariant,
	isOracleValid,
	isOrderExpired,
	isVariant,
	JupiterClient,
	MakerInfo,
	MarketType,
	NodeToFill,
	PriorityFeeSubscriber,
	ReferrerInfo,
	SlotSubscriber,
	TxSigAndSlot,
	UserAccount,
	UserStatsMap,
} from '@drift-labs/sdk';
import { FillerMultiThreadedConfig, GlobalConfig } from '../../config';
import { BundleSender } from '../../bundleSender';
import {
	AddressLookupTableAccount,
	ComputeBudgetProgram,
	Connection,
	LAMPORTS_PER_SOL,
	PublicKey,
	SendTransactionError,
	TransactionInstruction,
	TransactionSignature,
	VersionedTransaction,
} from '@solana/web3.js';
import { logger } from '../../logger';
import { getErrorCode } from '../../error';
import { selectMakers } from '../../makerSelection';
import {
	NodeToFillWithBuffer,
	SerializedNodeToTrigger,
	SerializedNodeToFill,
} from './types';
import { assert } from 'console';
import {
	getFillSignatureFromUserAccountAndOrderId,
	getNodeToFillSignature,
	logMessageForNodeToFill,
	simulateAndGetTxWithCUs,
	SimulateAndGetTxWithCUsResponse,
	sleepMs,
	swapFillerHardEarnedUSDCForSOL,
	validMinimumAmountToFill,
} from '../../utils';
import {
	spawnChildWithRetry,
	deserializeNodeToFill,
	deserializeOrder,
} from './utils';
import {
	CounterValue,
	GaugeValue,
	HistogramValue,
	metricAttrFromUserAccount,
	Metrics,
	RuntimeSpec,
} from '../../metrics';
import {
	ExplicitBucketHistogramAggregation,
	InstrumentType,
	View,
} from '@opentelemetry/sdk-metrics-base';
import {
	CONFIRM_TX_RATE_LIMIT_BACKOFF_MS,
	TX_TIMEOUT_THRESHOLD_MS,
	TxType,
} from '../../bots/filler';
import { LRUCache } from 'lru-cache';
import {
	isEndIxLog,
	isErrFillingLog,
	isErrStaleOracle,
	isFillIxLog,
	isIxLog,
	isMakerBreachedMaintenanceMarginLog,
	isOrderDoesNotExistLog,
	isTakerBreachedMaintenanceMarginLog,
} from '../../bots/common/txLogParse';
import { bs58 } from '@project-serum/anchor/dist/cjs/utils/bytes';

const logPrefix = '[Filler]';

export type MakerNodeMap = Map<string, DLOBNode[]>;

const FILL_ORDER_THROTTLE_BACKOFF = 1000; // the time to wait before trying to fill a throttled (error filling) node again
const THROTTLED_NODE_SIZE_TO_PRUNE = 10; // Size of throttled nodes to get to before pruning the map
const TRIGGER_ORDER_COOLDOWN_MS = 1000; // the time to wait before trying to a node in the triggering map again
export const MAX_MAKERS_PER_FILL = 6; // max number of unique makers to include per fill
const MAX_ACCOUNTS_PER_TX = 64; // solana limit, track https://github.com/solana-labs/solana/issues/27241

const MAX_POSITIONS_PER_USER = 8;
export const SETTLE_POSITIVE_PNL_COOLDOWN_MS = 60_000;
export const CONFIRM_TX_INTERVAL_MS = 5_000;
const SIM_CU_ESTIMATE_MULTIPLIER = 1.15;
const SLOTS_UNTIL_JITO_LEADER_TO_SEND = 4;
export const TX_CONFIRMATION_BATCH_SIZE = 100;
export const CACHED_BLOCKHASH_OFFSET = 5;
const TX_COUNT_COOLDOWN_ON_BURST = 10; // send this many tx before resetting burst mode

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
	try_fill_duration_histogram = 'try_fill_duration_histogram',
	runtime_specs = 'runtime_specs',
	last_try_fill_time = 'last_try_fill_time',
	sent_transactions = 'sent_transactions',
	landed_transactions = 'landed_transactions',
	tx_sim_error_count = 'tx_sim_error_count',
	pending_tx_sigs_to_confirm = 'pending_tx_sigs_to_confirm',
	pending_tx_sigs_loop_rate_limited = 'pending_tx_sigs_loop_rate_limited',
	evicted_pending_tx_sigs_to_confirm = 'evicted_pending_tx_sigs_to_confirm',
	estimated_tx_cu_histogram = 'estimated_tx_cu_histogram',
	simulate_tx_duration_histogram = 'simulate_tx_duration_histogram',
	expired_nodes_set_size = 'expired_nodes_set_size',

	jito_bundles_accepted = 'jito_bundles_accepted',
	jito_bundles_simulation_failure = 'jito_simulation_failure',
	jito_dropped_bundle = 'jito_dropped_bundle',
	jito_landed_tips = 'jito_landed_tips',
	jito_bundle_count = 'jito_bundle_count',
}

const getNodeToTriggerSignature = (node: SerializedNodeToTrigger): string => {
	return getOrderSignature(node.node.order.orderId, node.node.userAccount);
};

export class FillerMultithreaded {
	private name: string;
	private slotSubscriber: SlotSubscriber;
	private bundleSender?: BundleSender;
	private driftClient: DriftClient;
	private dryRun: boolean;
	private globalConfig: GlobalConfig;
	private config: FillerMultiThreadedConfig;

	private fillTxId: number = 0;
	private userStatsMap: UserStatsMap;
	private throttledNodes = new Map<string, number>();
	private fillingNodes = new Map<string, number>();
	private triggeringNodes = new Map<string, number>();
	private revertOnFailure: boolean = true;
	private lookupTableAccount?: AddressLookupTableAccount;
	private lastSettlePnl = Date.now() - SETTLE_POSITIVE_PNL_COOLDOWN_MS;
	private seenFillableOrders = new Set<string>();
	private seenTriggerableOrders = new Set<string>();
	private blockhashSubscriber: BlockhashSubscriber;
	private priorityFeeSubscriber: PriorityFeeSubscriber;

	private dlobHealthy = true;
	private orderSubscriberHealthy = true;
	private simulateTxForCUEstimate?: boolean;

	private intervalIds: NodeJS.Timeout[] = [];

	protected txConfirmationConnection: Connection;
	protected pendingTxSigsToconfirm: LRUCache<
		string,
		{
			ts: number;
			nodeFilled: Array<NodeToFillWithBuffer>;
			fillTxId: number;
			txType: TxType;
		}
	>;
	protected expiredNodesSet: LRUCache<string, boolean>;
	protected confirmLoopRunning = false;
	protected confirmLoopRateLimitTs =
		Date.now() - CONFIRM_TX_RATE_LIMIT_BACKOFF_MS;
	protected useBurstCULimit = false;
	protected fillTxSinceBurstCU = 0;

	// metrics
	protected metricsInitialized = false;
	protected metricsPort?: number;
	protected metrics?: Metrics;
	protected bootTimeMs?: number;

	protected runtimeSpec: RuntimeSpec;
	protected runtimeSpecsGauge?: GaugeValue;
	protected estTxCuHistogram?: HistogramValue;
	protected simulateTxHistogram?: HistogramValue;
	protected lastTryFillTimeGauge?: GaugeValue;
	protected sentTxsCounter?: CounterValue;
	protected attemptedTriggersCounter?: CounterValue;
	protected landedTxsCounter?: CounterValue;
	protected txSimErrorCounter?: CounterValue;
	protected pendingTxSigsToConfirmGauge?: GaugeValue;
	protected pendingTxSigsLoopRateLimitedCounter?: CounterValue;
	protected evictedPendingTxSigsToConfirmCounter?: CounterValue;
	protected expiredNodesSetSize?: GaugeValue;
	protected jitoBundlesAcceptedGauge?: GaugeValue;
	protected jitoBundlesSimulationFailureGauge?: GaugeValue;
	protected jitoDroppedBundleGauge?: GaugeValue;
	protected jitoLandedTipsGauge?: GaugeValue;
	protected jitoBundleCount?: GaugeValue;

	protected rebalanceFiller?: boolean;
	protected hasEnoughSolToFill: boolean = true;
	protected minimumAmountToFill: number;

	protected jupiterClient?: JupiterClient;

	constructor(
		globalConfig: GlobalConfig,
		config: FillerMultiThreadedConfig,
		driftClient: DriftClient,
		slotSubscriber: SlotSubscriber,
		priorityFeeSubscriber: PriorityFeeSubscriber,
		runtimeSpec: RuntimeSpec,
		bundleSender?: BundleSender
	) {
		this.globalConfig = globalConfig;
		this.name = config.botId;
		this.config = config;
		this.dryRun = config.dryRun;
		this.slotSubscriber = slotSubscriber;
		this.driftClient = driftClient;
		this.bundleSender = bundleSender;
		this.simulateTxForCUEstimate = config.simulateTxForCUEstimate ?? true;
		if (globalConfig.txConfirmationEndpoint) {
			this.txConfirmationConnection = new Connection(
				globalConfig.txConfirmationEndpoint
			);
		} else {
			this.txConfirmationConnection = this.driftClient.connection;
		}

		this.userStatsMap = new UserStatsMap(
			this.driftClient,
			new BulkAccountLoader(
				new Connection(this.driftClient.connection.rpcEndpoint),
				'confirmed',
				0
			)
		);
		this.blockhashSubscriber = new BlockhashSubscriber({
			connection: driftClient.connection,
		});
		this.priorityFeeSubscriber = priorityFeeSubscriber;
		this.priorityFeeSubscriber.updateMarketTypeAndIndex([
			{
				marketType: this.config.marketType,
				marketIndex: this.config.marketIndex,
			},
		]);
		this.runtimeSpec = runtimeSpec;
		this.initializeMetrics(config.metricsPort ?? this.globalConfig.metricsPort);

		if (
			config.rebalanceFiller &&
			this.runtimeSpec.driftEnv === 'mainnet-beta'
		) {
			this.jupiterClient = new JupiterClient({
				connection: this.driftClient.connection,
			});
		}
		this.rebalanceFiller = config.rebalanceFiller ?? true;
		logger.info(
			`${this.name}: rebalancing enabled: ${this.jupiterClient !== undefined}`
		);

		if (!validMinimumAmountToFill(config.minimumAmountToFill)) {
			this.minimumAmountToFill = 0.2 * LAMPORTS_PER_SOL;
		} else {
			// @ts-ignore
			this.minimumAmountToFill = config.minimumAmountToFill * LAMPORTS_PER_SOL;
		}

		logger.info(
			`${this.name}: minimumAmountToFill: ${this.minimumAmountToFill}`
		);

		this.pendingTxSigsToconfirm = new LRUCache<
			string,
			{
				ts: number;
				nodeFilled: Array<NodeToFillWithBuffer>;
				fillTxId: number;
				txType: TxType;
			}
		>({
			max: 10_000,
			ttl: TX_TIMEOUT_THRESHOLD_MS,
			ttlResolution: 1000,
			disposeAfter: this.recordEvictedTxSig.bind(this),
		});

		this.expiredNodesSet = new LRUCache<string, boolean>({
			max: 10_000,
			ttl: TX_TIMEOUT_THRESHOLD_MS,
			ttlResolution: 1000,
		});
	}

	async init() {
		await this.blockhashSubscriber.subscribe();

		this.lookupTableAccount =
			await this.driftClient.fetchMarketLookupTableAccount();
		assert(this.lookupTableAccount, 'Lookup table account not found');
		this.startProcesses();
	}

	private startProcesses() {
		let dlobBuilderReady = false;
		const childArgs = [
			`--market-type=${this.config.marketType}`,
			`--market-index=${this.config.marketIndex}`,
		];
		const user = this.driftClient.getUser();
		const dlobBuilderProcess = spawnChildWithRetry(
			'./src/experimental-bots/filler/dlobBuilder.ts',
			childArgs,
			'dlobBuilder',
			(msg: any) => {
				switch (msg.type) {
					case 'initialized':
						dlobBuilderReady = true;
						logger.info(
							`${logPrefix} dlobBuilderProcess initialized and acknowledged`
						);
						break;
					case 'triggerableNodes':
						if (this.dryRun) {
							logger.info(`Triggerable node received`);
						} else {
							this.triggerNodes(msg.data);
						}
						this.lastTryFillTimeGauge?.setLatestValue(
							Date.now(),
							metricAttrFromUserAccount(
								user.getUserAccountPublicKey(),
								user.getUserAccount()
							)
						);
						break;
					case 'fillableNodes':
						if (this.dryRun) {
							logger.info(`Fillable node received`);
						} else {
							this.fillNodes(msg.data);
						}
						this.lastTryFillTimeGauge?.setLatestValue(
							Date.now(),
							metricAttrFromUserAccount(
								user.getUserAccountPublicKey(),
								user.getUserAccount()
							)
						);
						break;
					case 'health':
						this.dlobHealthy = msg.data.healthy;
						break;
				}
			},
			'[FillerMultithreaded]'
		);

		const orderSubscriberProcess = spawnChildWithRetry(
			'./src/experimental-bots/filler/orderSubscriberFiltered.ts',
			childArgs,
			'orderSubscriber',
			(msg: any) => {
				switch (msg.type) {
					case 'userAccountUpdate':
						if (dlobBuilderReady) {
							dlobBuilderProcess.send(msg);
						}
						break;
					case 'health':
						this.orderSubscriberHealthy = msg.data.healthy;
						break;
				}
			},
			'[FillerMultithreaded]'
		);

		process.on('SIGINT', () => {
			logger.info(`${logPrefix} Received SIGINT, killing children`);
			dlobBuilderProcess.kill();
			orderSubscriberProcess.kill();
			process.exit(0);
		});

		logger.info(`dlobBuilder spawned with pid: ${dlobBuilderProcess.pid}`);
		logger.info(
			`orderSubscriber spawned with pid: ${orderSubscriberProcess.pid}`
		);

		this.intervalIds.push(
			setInterval(
				this.settlePnls.bind(this),
				SETTLE_POSITIVE_PNL_COOLDOWN_MS / 2
			)
		);
		this.intervalIds.push(
			setInterval(this.confirmPendingTxSigs.bind(this), CONFIRM_TX_INTERVAL_MS)
		);
		if (this.bundleSender) {
			this.intervalIds.push(
				setInterval(this.recordJitoBundleStats.bind(this), 10_000)
			);
		}
	}

	protected recordEvictedTxSig(
		_tsTxSigAdded: { ts: number; nodeFilled: Array<NodeToFillWithBuffer> },
		txSig: string,
		reason: 'evict' | 'set' | 'delete'
	) {
		if (reason === 'evict') {
			logger.info(
				`${this.name}: Evicted tx sig ${txSig} from this.txSigsToConfirm`
			);
			const user = this.driftClient.getUser();
			this.evictedPendingTxSigsToConfirmCounter?.add(1, {
				...metricAttrFromUserAccount(
					user.userAccountPublicKey,
					user.getUserAccount()
				),
			});
		}
	}

	protected initializeMetrics(metricsPort?: number) {
		if (this.globalConfig.disableMetrics) {
			logger.info(
				`${this.name}: globalConfig.disableMetrics is true, not initializing metrics`
			);
			return;
		}

		if (!metricsPort) {
			logger.info(
				`${this.name}: bot.metricsPort and global.metricsPort not set, not initializing metrics`
			);
			return;
		}

		if (this.metricsInitialized) {
			logger.error('Tried to initilaize metrics multiple times');
			return;
		}

		this.metrics = new Metrics(
			this.name,
			[
				new View({
					instrumentName: METRIC_TYPES.try_fill_duration_histogram,
					instrumentType: InstrumentType.HISTOGRAM,
					meterName: this.name,
					aggregation: new ExplicitBucketHistogramAggregation(
						Array.from(new Array(20), (_, i) => 0 + i * 5),
						true
					),
				}),
				new View({
					instrumentName: METRIC_TYPES.estimated_tx_cu_histogram,
					instrumentType: InstrumentType.HISTOGRAM,
					meterName: this.name,
					aggregation: new ExplicitBucketHistogramAggregation(
						Array.from(new Array(15), (_, i) => 0 + i * 100_000),
						true
					),
				}),
				new View({
					instrumentName: METRIC_TYPES.simulate_tx_duration_histogram,
					instrumentType: InstrumentType.HISTOGRAM,
					meterName: this.name,
					aggregation: new ExplicitBucketHistogramAggregation(
						Array.from(new Array(20), (_, i) => 50 + i * 50),
						true
					),
				}),
			],
			metricsPort!
		);
		this.bootTimeMs = Date.now();
		this.runtimeSpecsGauge = this.metrics.addGauge(
			METRIC_TYPES.runtime_specs,
			'Runtime sepcification of this program'
		);
		this.estTxCuHistogram = this.metrics.addHistogram(
			METRIC_TYPES.estimated_tx_cu_histogram,
			'Histogram of the estimated fill cu used'
		);
		this.simulateTxHistogram = this.metrics.addHistogram(
			METRIC_TYPES.simulate_tx_duration_histogram,
			'Histogram of the duration of simulateTransaction RPC calls'
		);
		this.lastTryFillTimeGauge = this.metrics.addGauge(
			METRIC_TYPES.last_try_fill_time,
			'Last time that fill was attempted'
		);
		this.landedTxsCounter = this.metrics.addCounter(
			METRIC_TYPES.landed_transactions,
			'Count of fills that we successfully landed'
		);
		this.sentTxsCounter = this.metrics.addCounter(
			METRIC_TYPES.sent_transactions,
			'Count of transactions we sent out'
		);
		this.txSimErrorCounter = this.metrics.addCounter(
			METRIC_TYPES.tx_sim_error_count,
			'Count of errors from simulating transactions'
		);
		this.pendingTxSigsToConfirmGauge = this.metrics.addGauge(
			METRIC_TYPES.pending_tx_sigs_to_confirm,
			'Count of tx sigs that are pending confirmation'
		);
		this.pendingTxSigsLoopRateLimitedCounter = this.metrics.addCounter(
			METRIC_TYPES.pending_tx_sigs_loop_rate_limited,
			'Count of times the pending tx sigs loop was rate limited'
		);
		this.evictedPendingTxSigsToConfirmCounter = this.metrics.addCounter(
			METRIC_TYPES.evicted_pending_tx_sigs_to_confirm,
			'Count of tx sigs that were evicted from the pending tx sigs to confirm cache'
		);
		this.expiredNodesSetSize = this.metrics.addGauge(
			METRIC_TYPES.expired_nodes_set_size,
			'Count of nodes that are expired'
		);
		this.jitoBundlesAcceptedGauge = this.metrics.addGauge(
			METRIC_TYPES.jito_bundles_accepted,
			'Count of jito bundles that were accepted'
		);
		this.jitoBundlesSimulationFailureGauge = this.metrics.addGauge(
			METRIC_TYPES.jito_bundles_simulation_failure,
			'Count of jito bundles that failed simulation'
		);
		this.jitoDroppedBundleGauge = this.metrics.addGauge(
			METRIC_TYPES.jito_dropped_bundle,
			'Count of jito bundles that were dropped'
		);
		this.jitoLandedTipsGauge = this.metrics.addGauge(
			METRIC_TYPES.jito_landed_tips,
			'Gauge of historic bundle tips that landed'
		);
		this.jitoBundleCount = this.metrics.addGauge(
			METRIC_TYPES.jito_bundle_count,
			'Count of jito bundles that were sent, and their status'
		);

		this.metrics?.finalizeObservables();

		this.runtimeSpecsGauge.setLatestValue(this.bootTimeMs, this.runtimeSpec);
		this.metricsInitialized = true;
	}

	public healthCheck(): boolean {
		if (!this.dlobHealthy) {
			logger.error(`${logPrefix} DLOB not healthy`);
		}
		if (!this.orderSubscriberHealthy) {
			logger.error(`${logPrefix} Order subscriber not healthy`);
		}
		return this.dlobHealthy && this.orderSubscriberHealthy;
	}

	protected recordJitoBundleStats() {
		const user = this.driftClient.getUser();
		const bundleStats = this.bundleSender?.getBundleStats();
		if (bundleStats) {
			this.jitoBundlesAcceptedGauge?.setLatestValue(bundleStats.accepted, {
				...metricAttrFromUserAccount(
					user.userAccountPublicKey,
					user.getUserAccount()
				),
			});
			this.jitoBundlesSimulationFailureGauge?.setLatestValue(
				bundleStats.simulationFailure,
				{
					...metricAttrFromUserAccount(
						user.userAccountPublicKey,
						user.getUserAccount()
					),
				}
			);
			this.jitoDroppedBundleGauge?.setLatestValue(bundleStats.droppedPruned, {
				type: 'pruned',
				...metricAttrFromUserAccount(
					user.userAccountPublicKey,
					user.getUserAccount()
				),
			});
			this.jitoDroppedBundleGauge?.setLatestValue(
				bundleStats.droppedBlockhashExpired,
				{
					type: 'blockhash_expired',
					...metricAttrFromUserAccount(
						user.userAccountPublicKey,
						user.getUserAccount()
					),
				}
			);
			this.jitoDroppedBundleGauge?.setLatestValue(
				bundleStats.droppedBlockhashNotFound,
				{
					type: 'blockhash_not_found',
					...metricAttrFromUserAccount(
						user.userAccountPublicKey,
						user.getUserAccount()
					),
				}
			);
		}

		const tipStream = this.bundleSender?.getTipStream();
		if (tipStream) {
			this.jitoLandedTipsGauge?.setLatestValue(
				tipStream.landed_tips_25th_percentile,
				{
					percentile: 'p25',
					...metricAttrFromUserAccount(
						user.userAccountPublicKey,
						user.getUserAccount()
					),
				}
			);
			this.jitoLandedTipsGauge?.setLatestValue(
				tipStream.landed_tips_50th_percentile,
				{
					percentile: 'p50',
					...metricAttrFromUserAccount(
						user.userAccountPublicKey,
						user.getUserAccount()
					),
				}
			);
			this.jitoLandedTipsGauge?.setLatestValue(
				tipStream.landed_tips_75th_percentile,
				{
					percentile: 'p75',
					...metricAttrFromUserAccount(
						user.userAccountPublicKey,
						user.getUserAccount()
					),
				}
			);
			this.jitoLandedTipsGauge?.setLatestValue(
				tipStream.landed_tips_95th_percentile,
				{
					percentile: 'p95',
					...metricAttrFromUserAccount(
						user.userAccountPublicKey,
						user.getUserAccount()
					),
				}
			);
			this.jitoLandedTipsGauge?.setLatestValue(
				tipStream.landed_tips_99th_percentile,
				{
					percentile: 'p99',
					...metricAttrFromUserAccount(
						user.userAccountPublicKey,
						user.getUserAccount()
					),
				}
			);
			this.jitoLandedTipsGauge?.setLatestValue(
				tipStream.ema_landed_tips_50th_percentile,
				{
					percentile: 'ema_p50',
					...metricAttrFromUserAccount(
						user.userAccountPublicKey,
						user.getUserAccount()
					),
				}
			);

			const bundleFailCount = this.bundleSender?.getBundleFailCount();
			const bundleLandedCount = this.bundleSender?.getLandedCount();
			const bundleDroppedCount = this.bundleSender?.getDroppedCount();
			this.jitoBundleCount?.setLatestValue(bundleFailCount ?? 0, {
				type: 'fail_count',
			});
			this.jitoBundleCount?.setLatestValue(bundleLandedCount ?? 0, {
				type: 'landed',
			});
			this.jitoBundleCount?.setLatestValue(bundleDroppedCount ?? 0, {
				type: 'dropped',
			});
		}
	}

	protected async confirmPendingTxSigs() {
		const user = this.driftClient.getUser();
		this.pendingTxSigsToConfirmGauge?.setLatestValue(
			this.pendingTxSigsToconfirm.size,
			{
				...metricAttrFromUserAccount(
					user.userAccountPublicKey,
					user.getUserAccount()
				),
			}
		);
		this.expiredNodesSetSize?.setLatestValue(this.expiredNodesSet.size, {
			...metricAttrFromUserAccount(
				user.userAccountPublicKey,
				user.getUserAccount()
			),
		});
		const nextTimeCanRun =
			this.confirmLoopRateLimitTs + CONFIRM_TX_RATE_LIMIT_BACKOFF_MS;
		if (Date.now() < nextTimeCanRun) {
			logger.warn(
				`Skipping confirm loop due to rate limit, next run in ${
					nextTimeCanRun - Date.now()
				} ms`
			);
			return;
		}
		if (this.confirmLoopRunning) {
			return;
		}
		this.confirmLoopRunning = true;
		try {
			logger.info(`Confirming tx sigs: ${this.pendingTxSigsToconfirm.size}`);
			const start = Date.now();
			const txEntries = Array.from(this.pendingTxSigsToconfirm.entries());
			for (let i = 0; i < txEntries.length; i += TX_CONFIRMATION_BATCH_SIZE) {
				const txSigsBatch = txEntries.slice(i, i + TX_CONFIRMATION_BATCH_SIZE);
				const txs = await this.txConfirmationConnection?.getTransactions(
					txSigsBatch.map((tx) => tx[0]),
					{
						commitment: 'confirmed',
						maxSupportedTransactionVersion: 0,
					}
				);
				for (let j = 0; j < txs.length; j++) {
					const txResp = txs[j];
					const txConfirmationInfo = txSigsBatch[j];
					const txSig = txConfirmationInfo[0];
					const txAge = txConfirmationInfo[1].ts - Date.now();
					const nodeFilled = txConfirmationInfo[1].nodeFilled;
					const txType = txConfirmationInfo[1].txType;
					const fillTxId = txConfirmationInfo[1].fillTxId;
					if (txResp === null) {
						logger.info(
							`Tx not found, (fillTxId: ${fillTxId}) (txType: ${txType}): ${txSig}, tx age: ${
								txAge / 1000
							} s`
						);
						if (Math.abs(txAge) > TX_TIMEOUT_THRESHOLD_MS) {
							this.pendingTxSigsToconfirm.delete(txSig);
						}
					} else {
						logger.info(
							`Tx landed (fillTxId: ${fillTxId}) (txType: ${txType}): ${txSig}, tx age: ${
								txAge / 1000
							} s`
						);
						this.pendingTxSigsToconfirm.delete(txSig);
						if (txType === 'fill') {
							const result = await this.handleTransactionLogs(
								nodeFilled,
								txResp.meta?.logMessages
							);
							if (result) {
								this.landedTxsCounter?.add(result.filledNodes, {
									type: txType,
									...metricAttrFromUserAccount(
										user.userAccountPublicKey,
										user.getUserAccount()
									),
								});
							}
						} else {
							this.landedTxsCounter?.add(1, {
								type: txType,
								...metricAttrFromUserAccount(
									user.userAccountPublicKey,
									user.getUserAccount()
								),
							});
						}
					}
					await sleepMs(500);
				}
			}
			logger.info(`Confirming tx sigs took: ${Date.now() - start} ms`);
		} catch (e) {
			const err = e as Error;
			if (err.message.includes('429')) {
				logger.info(`Confirming tx loop rate limited: ${err.message}`);
				this.confirmLoopRateLimitTs = Date.now();
				this.pendingTxSigsLoopRateLimitedCounter?.add(1, {
					...metricAttrFromUserAccount(
						user.userAccountPublicKey,
						user.getUserAccount()
					),
				});
			} else {
				logger.error(`Other error confirming tx sigs: ${err.message}`);
			}
		} finally {
			this.confirmLoopRunning = false;
		}
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

	protected removeFillingNodes(nodes: Array<NodeToFillWithBuffer>) {
		for (const node of nodes) {
			this.fillingNodes.delete(getNodeToFillSignature(node));
		}
	}

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

	protected removeTriggeringNodes(node: SerializedNodeToTrigger) {
		this.triggeringNodes.delete(getNodeToTriggerSignature(node));
	}

	protected pruneThrottledNode() {
		if (this.throttledNodes.size > THROTTLED_NODE_SIZE_TO_PRUNE) {
			for (const [key, value] of this.throttledNodes.entries()) {
				if (value + 2 * FILL_ORDER_THROTTLE_BACKOFF > Date.now()) {
					this.throttledNodes.delete(key);
				}
			}
		}
	}

	protected async sendTxThroughJito(
		tx: VersionedTransaction,
		metadata: number | string
	) {
		const blockhash = await this.getBlockhashForTx();
		tx.message.recentBlockhash = blockhash;

		// @ts-ignore;
		tx.sign([this.driftClient.wallet.payer]);

		if (this.bundleSender === undefined) {
			logger.error(
				`${logPrefix} Called sendTxThroughJito without jito properly enabled`
			);
			return;
		}
		const slotsUntilNextLeader = this.bundleSender?.slotsUntilNextLeader();
		if (slotsUntilNextLeader !== undefined) {
			this.bundleSender.sendTransaction(tx, `(fillTxId: ${metadata})`);
		}
	}

	protected slotsUntilJitoLeader(): number | undefined {
		return this.bundleSender?.slotsUntilNextLeader();
	}

	public async triggerNodes(
		serializedNodesToTrigger: SerializedNodeToTrigger[]
	) {
		if (!this.hasEnoughSolToFill) {
			logger.info(
				`Not enough SOL to fill, skipping executeTriggerablePerpNodes`
			);
			return;
		}

		logger.info(
			`${logPrefix} Triggering ${serializedNodesToTrigger.length} nodes...`
		);
		const seenTriggerableNodes = new Set<string>();
		const filteredTriggerableNodes = serializedNodesToTrigger.filter((node) => {
			const sig = getNodeToTriggerSignature(node);
			if (seenTriggerableNodes.has(sig)) {
				return false;
			}
			seenTriggerableNodes.add(sig);
			return this.filterTriggerableNodes(node);
		});
		logger.info(
			`${logPrefix} Filtered down to ${filteredTriggerableNodes.length} triggerable nodes...`
		);

		const slotsUntilJito = this.slotsUntilJitoLeader();
		const buildForBundle =
			this.globalConfig.useJito &&
			slotsUntilJito !== undefined &&
			slotsUntilJito < SLOTS_UNTIL_JITO_LEADER_TO_SEND;

		try {
			await this.executeTriggerablePerpNodes(
				filteredTriggerableNodes,
				!!buildForBundle
			);
		} catch (e) {
			if (e instanceof Error) {
				logger.error(
					`${logPrefix} Error triggering nodes: ${
						e.stack ? e.stack : e.message
					}`
				);
			}
		}
	}

	protected filterTriggerableNodes(
		nodeToTrigger: SerializedNodeToTrigger
	): boolean {
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

	async executeTriggerablePerpNodes(
		nodesToTrigger: SerializedNodeToTrigger[],
		buildForBundle: boolean
	) {
		for (const nodeToTrigger of nodesToTrigger) {
			nodeToTrigger.node.haveTrigger = true;
			// @ts-ignore
			const buffer = Buffer.from(nodeToTrigger.node.userAccountData.data);
			// @ts-ignore
			const userAccount = decodeUser(buffer);

			logger.info(
				`${logPrefix} trying to trigger (account: ${
					nodeToTrigger.node.userAccount
				}, order ${nodeToTrigger.node.order.orderId.toString()}`
			);

			const nodeSignature = getNodeToTriggerSignature(nodeToTrigger);
			if (this.seenTriggerableOrders.has(nodeSignature)) {
				logger.debug(
					`${logPrefix} already triggered order (account: ${
						nodeToTrigger.node.userAccount
					}, order ${nodeToTrigger.node.order.orderId.toString()}`
				);
				return;
			}
			this.seenTriggerableOrders.add(nodeSignature);
			this.triggeringNodes.set(nodeSignature, Date.now());

			const ixs = [];
			ixs.push(
				await this.driftClient.getTriggerOrderIx(
					new PublicKey(nodeToTrigger.node.userAccount),
					userAccount,
					deserializeOrder(nodeToTrigger.node.order)
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
				this.simulateTxForCUEstimate,
				await this.getBlockhashForTx()
			);
			const user = this.driftClient.getUser();
			this.simulateTxHistogram?.record(simResult.simTxDuration, {
				type: 'trigger',
				simError: simResult.simError !== null,
				...metricAttrFromUserAccount(
					user.userAccountPublicKey,
					user.getUserAccount()
				),
			});
			this.estTxCuHistogram?.record(simResult.cuEstimate, {
				type: 'trigger',
				simError: simResult.simError !== null,
				...metricAttrFromUserAccount(
					user.userAccountPublicKey,
					user.getUserAccount()
				),
			});

			logger.info(
				`executeTriggerablePerpNodesForMarket estimated CUs: ${simResult.cuEstimate}`
			);

			if (simResult.simError) {
				logger.error(
					`executeTriggerablePerpNodesForMarket simError: (simError: ${JSON.stringify(
						simResult.simError
					)})`
				);
			} else {
				if (this.hasEnoughSolToFill) {
					if (buildForBundle) {
						this.sendTxThroughJito(simResult.tx, 'triggerOrder');
					} else {
						const blockhash = await this.getBlockhashForTx();
						simResult.tx.message.recentBlockhash = blockhash;
						this.driftClient
							.sendTransaction(simResult.tx)
							.then((txSig) => {
								logger.info(
									`Triggered user (account: ${nodeToTrigger.node.userAccount.toString()}) order: ${nodeToTrigger.node.order.orderId.toString()}`
								);
								logger.info(`${logPrefix} Tx: ${txSig}`);
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
								}
							})
							.finally(() => {
								this.removeTriggeringNodes(nodeToTrigger);
							});
					}
				} else {
					logger.info(
						`Not enough SOL to fill, skipping executeTriggerablePerpNodes`
					);
				}
			}
		}
		const user = this.driftClient.getUser();
		this.attemptedTriggersCounter?.add(
			nodesToTrigger.length,
			metricAttrFromUserAccount(
				user.userAccountPublicKey,
				user.getUserAccount()
			)
		);
	}

	public async fillNodes(serializedNodesToFill: SerializedNodeToFill[]) {
		if (!this.hasEnoughSolToFill) {
			logger.info(`Not enough SOL to fill, skipping fillNodes`);
			return;
		}

		logger.debug(
			`${logPrefix} Filling ${serializedNodesToFill.length} nodes...`
		);
		const deserializedNodesToFill = serializedNodesToFill.map(
			deserializeNodeToFill
		);
		const seenFillableNodes = new Set<string>();
		const filteredFillableNodes = deserializedNodesToFill.filter((node) => {
			const sig = getNodeToFillSignature(node);
			if (seenFillableNodes.has(sig)) {
				return false;
			}
			seenFillableNodes.add(sig);
			return this.filterFillableNodes(node);
		});
		logger.debug(
			`${logPrefix} Filtered down to ${filteredFillableNodes.length} fillable nodes...`
		);

		const slotsUntilJito = this.slotsUntilJitoLeader();
		const buildForBundle =
			this.globalConfig.useJito &&
			slotsUntilJito !== undefined &&
			slotsUntilJito < SLOTS_UNTIL_JITO_LEADER_TO_SEND;

		try {
			await this.executeFillablePerpNodesForMarket(
				filteredFillableNodes,
				!!buildForBundle
			);
		} catch (e) {
			if (e instanceof Error) {
				logger.error(
					`${logPrefix} Error filling nodes: ${e.stack ? e.stack : e.message}`
				);
			}
		}
	}

	protected filterFillableNodes(nodeToFill: NodeToFillWithBuffer): boolean {
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
			if (timeStartedToFillNode + FILL_ORDER_THROTTLE_BACKOFF > now) {
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
				this.slotSubscriber.getSlot(),
				Date.now() / 1000,
				this.driftClient.getStateAccount().minPerpAuctionDuration
			)
		) {
			return false;
		}

		const perpMarket = this.driftClient.getPerpMarketAccount(
			nodeToFill.node.order.marketIndex
		)!;
		// if making with vAMM, ensure valid oracle
		if (
			nodeToFill.makerNodes.length === 0 &&
			!isVariant(perpMarket.amm.oracleSource, 'prelaunch')
		) {
			const oracleIsValid = isOracleValid(
				perpMarket,
				oraclePriceData,
				this.driftClient.getStateAccount().oracleGuardRails,
				this.slotSubscriber.getSlot()
			);
			if (!oracleIsValid) {
				logger.error(
					`${logPrefix} Oracle is not valid for market ${marketIndex}`
				);
				return false;
			}
		}

		return true;
	}

	async executeFillablePerpNodesForMarket(
		nodesToFill: NodeToFillWithBuffer[],
		buildForBundle: boolean
	) {
		for (const node of nodesToFill) {
			if (this.seenFillableOrders.has(getNodeToFillSignature(node))) {
				logger.debug(
					// @ts-ignore
					`${logPrefix} already filled order (account: ${
						node.node.userAccount
					}, order ${node.node.order?.orderId.toString()}`
				);
				return;
			}
			this.seenFillableOrders.add(getNodeToFillSignature(node));
			if (node.makerNodes.length > 1) {
				this.tryFillMultiMakerPerpNodes(node, buildForBundle);
			} else {
				this.tryFillPerpNode(node, buildForBundle);
			}
		}
	}

	protected async tryFillMultiMakerPerpNodes(
		nodeToFill: NodeToFillWithBuffer,
		buildForBundle: boolean
	) {
		const fillTxId = this.fillTxId++;

		let nodeWithMakerSet = nodeToFill;
		while (
			!(await this.fillMultiMakerPerpNodes(
				fillTxId,
				nodeWithMakerSet,
				buildForBundle
			))
		) {
			const newMakerSet = nodeWithMakerSet.makerNodes
				.sort(() => 0.5 - Math.random())
				.slice(0, Math.ceil(nodeWithMakerSet.makerNodes.length / 2));
			nodeWithMakerSet = {
				userAccountData: nodeWithMakerSet.userAccountData,
				makerAccountData: nodeWithMakerSet.makerAccountData,
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

	private async fillMultiMakerPerpNodes(
		fillTxId: number,
		nodeToFill: NodeToFillWithBuffer,
		buildForBundle: boolean
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
				referrerInfo,
				marketType,
			} = await this.getNodeFillInfo(nodeToFill);

			logger.info(
				logMessageForNodeToFill(
					nodeToFill,
					takerUserPubKey,
					takerUserSlot,
					makerInfos,
					this.slotSubscriber.getSlot(),
					`${logPrefix} Filling multi maker perp node with ${nodeToFill.makerNodes.length} makers (fillTxId: ${fillTxId})`
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
					this.simulateTxForCUEstimate,
					await this.getBlockhashForTx()
				);
				const user = this.driftClient.getUser();
				this.simulateTxHistogram?.record(simResult.simTxDuration, {
					type: 'multiMakerFill',
					simError: simResult.simError !== null,
					...metricAttrFromUserAccount(
						user.userAccountPublicKey,
						user.getUserAccount()
					),
				});
				this.estTxCuHistogram?.record(simResult.cuEstimate, {
					type: 'multiMakerFill',
					simError: simResult.simError !== null,
					...metricAttrFromUserAccount(
						user.userAccountPublicKey,
						user.getUserAccount()
					),
				});
				return simResult;
			};

			let simResult = await buildTxWithMakerInfos(makerInfosToUse);
			let txAccounts = simResult.tx.message.getAccountKeys({
				addressLookupTableAccounts: [this.lookupTableAccount!],
			}).length;
			let attempt = 0;
			while (txAccounts > MAX_ACCOUNTS_PER_TX && makerInfosToUse.length > 0) {
				logger.info(
					`${logPrefix} (fillTxId: ${fillTxId} attempt ${attempt++}) Too many accounts, remove 1 and try again (had ${
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
					`${logPrefix} No makerInfos left to use for multi maker perp node (fillTxId: ${fillTxId})`
				);
				return true;
			}

			logger.info(
				`${logPrefix} tryFillMultiMakerPerpNodes estimated CUs: ${simResult.cuEstimate} (fillTxId: ${fillTxId})`
			);

			if (simResult.simError) {
				logger.error(
					`${logPrefix} Error simulating multi maker perp node (fillTxId: ${fillTxId}): ${JSON.stringify(
						simResult.simError
					)}\nTaker slot: ${takerUserSlot}\nMaker slots: ${makerInfosToUse
						.map((m) => `  ${m.data.maker.toBase58()}: ${m.slot}`)
						.join('\n')}`
				);
			} else {
				if (this.hasEnoughSolToFill) {
					this.sendFillTxAndParseLogs(
						fillTxId,
						[nodeToFill],
						simResult.tx,
						buildForBundle
					);
				} else {
					logger.info(
						`Not enough SOL to fill, skipping executeFillablePerpNodesForMarket`
					);
				}
			}
		} catch (e) {
			if (e instanceof Error) {
				logger.error(
					`${logPrefix} Error filling multi maker perp node (fillTxId: ${fillTxId}): ${
						e.stack ? e.stack : e.message
					}`
				);
			}
		}
		return true;
	}

	protected async tryFillPerpNode(
		nodeToFill: NodeToFillWithBuffer,
		buildForBundle: boolean
	) {
		const ixs = [
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
		const fillTxId = this.fillTxId++;

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
				this.slotSubscriber.getSlot(),
				`Filling perp node (fillTxId: ${fillTxId})`
			)
		);

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

		ixs.push(ix);

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
			this.simulateTxForCUEstimate
		);
		logger.info(
			`tryFillPerpNode estimated CUs: ${simResult.cuEstimate} (fillTxId: ${fillTxId})`
		);

		if (simResult.simError) {
			logger.error(
				`simError: ${JSON.stringify(
					simResult.simError
				)} (fillTxId: ${fillTxId})`
			);
		} else {
			if (this.hasEnoughSolToFill) {
				this.sendFillTxAndParseLogs(
					fillTxId,
					[nodeToFill],
					simResult.tx,
					buildForBundle
				);
			} else {
				logger.info(
					`Not enough SOL to fill, skipping executeFillablePerpNodesForMarket`
				);
			}
		}
	}

	protected async sendFillTxAndParseLogs(
		fillTxId: number,
		nodesSent: Array<NodeToFillWithBuffer>,
		tx: VersionedTransaction,
		buildForBundle: boolean
	) {
		let txResp: Promise<TxSigAndSlot> | undefined = undefined;
		let estTxSize: number | undefined = undefined;
		let txAccounts = 0;
		let writeAccs = 0;
		const accountMetas: any[] = [];
		const txStart = Date.now();
		// @ts-ignore;
		tx.sign([this.driftClient.wallet.payer]);
		const txSig = bs58.encode(tx.signatures[0]);

		if (buildForBundle) {
			await this.sendTxThroughJito(tx, fillTxId);
			this.removeFillingNodes(nodesSent);
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

		this.registerTxSigToConfirm(txSig, Date.now(), nodesSent, fillTxId, 'fill');

		if (txResp) {
			txResp
				.then((resp: TxSigAndSlot) => {
					const duration = Date.now() - txStart;
					logger.info(
						`${logPrefix} sent tx: ${resp.txSig}, took: ${duration}ms (fillTxId: ${fillTxId})`
					);
				})
				.catch(async (e) => {
					const simError = e as SendTransactionError;
					logger.error(
						`${logPrefix} Failed to send packed tx txAccountKeys: ${txAccounts} (${writeAccs} writeable) (fillTxId: ${fillTxId}), error: ${simError.message}`
					);

					if (e.message.includes('too large:')) {
						logger.error(
							`${logPrefix}: :boxing_glove: Tx too large, estimated to be ${estTxSize} (fillId: ${fillTxId}). ${
								e.message
							}\n${JSON.stringify(accountMetas)}`
						);
						return;
					}

					if (simError.logs && simError.logs.length > 0) {
						const errorCode = getErrorCode(e);
						logger.error(
							`${logPrefix} Failed to send tx, sim error (fillTxId: ${fillTxId}) error code: ${errorCode}`
						);
					}
				})
				.finally(() => {
					this.removeFillingNodes(nodesSent);
				});
		}
	}

	protected async settlePnls() {
		const user = this.driftClient.getUser();
		const marketIds = user
			.getActivePerpPositions()
			.map((pos) => pos.marketIndex);
		const now = Date.now();
		if (
			marketIds.length === MAX_POSITIONS_PER_USER ||
			!this.hasEnoughSolToFill
		) {
			logger.info(
				`Settling positive PNLs for markets: ${JSON.stringify(marketIds)}`
			);
			if (now < this.lastSettlePnl + SETTLE_POSITIVE_PNL_COOLDOWN_MS) {
				logger.info(`Want to settle positive pnl, but in cooldown...`);
			} else {
				let chunk_size;
				if (marketIds.length === 1) {
					chunk_size = 1;
				} else {
					chunk_size = marketIds.length / 2;
				}
				const settlePnlPromises: Array<Promise<TxSigAndSlot>> = [];
				for (let i = 0; i < marketIds.length; i += chunk_size) {
					const marketIdChunks = marketIds.slice(i, i + chunk_size);
					try {
						const ixs = [
							ComputeBudgetProgram.setComputeUnitLimit({
								units: 1_400_000, // will be overridden by simulateTx
							}),
							ComputeBudgetProgram.setComputeUnitPrice({
								microLamports: Math.floor(
									this.priorityFeeSubscriber.getCustomStrategyResult()
								),
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
							this.simulateTxForCUEstimate,
							await this.getBlockhashForTx()
						);
						this.simulateTxHistogram?.record(simResult.simTxDuration, {
							type: 'settlePnl',
							simError: simResult.simError !== null,
							...metricAttrFromUserAccount(
								user.userAccountPublicKey,
								user.getUserAccount()
							),
						});
						this.estTxCuHistogram?.record(simResult.cuEstimate, {
							type: 'settlePnl',
							simError: simResult.simError !== null,
							...metricAttrFromUserAccount(
								user.userAccountPublicKey,
								user.getUserAccount()
							),
						});

						if (this.simulateTxForCUEstimate && simResult.simError) {
							logger.info(
								`settlePnls simError: ${JSON.stringify(simResult.simError)}`
							);
						} else {
							if (!this.dryRun) {
								const slotsUntilJito = this.slotsUntilJitoLeader();
								const buildForBundle =
									this.globalConfig.useJito &&
									slotsUntilJito !== undefined &&
									slotsUntilJito < SLOTS_UNTIL_JITO_LEADER_TO_SEND;

								// @ts-ignore;
								simResult.tx.sign([this.driftClient.wallet.payer]);
								const txSig = bs58.encode(simResult.tx.signatures[0]);
								this.registerTxSigToConfirm(
									txSig,
									Date.now(),
									[],
									-2,
									'settlePnl'
								);

								if (buildForBundle) {
									this.sendTxThroughJito(simResult.tx, 'settlePnl');
								} else {
									settlePnlPromises.push(
										this.driftClient.txSender.sendVersionedTransaction(
											simResult.tx,
											[],
											this.driftClient.opts
										)
									);
								}
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
		if (this.rebalanceFiller) {
			logger.info(`Rebalancing filler`);
			const fillerSolBalance = await this.driftClient.connection.getBalance(
				this.driftClient.authority
			);

			this.hasEnoughSolToFill = fillerSolBalance >= this.minimumAmountToFill;

			if (!this.hasEnoughSolToFill && this.jupiterClient !== undefined) {
				logger.info(`Swapping USDC for SOL to rebalance filler`);
				swapFillerHardEarnedUSDCForSOL(
					this.priorityFeeSubscriber,
					this.driftClient,
					this.jupiterClient,
					await this.getBlockhashForTx()
				).then(async () => {
					const fillerSolBalanceAfterSwap =
						await this.driftClient.connection.getBalance(
							this.driftClient.authority,
							'processed'
						);
					this.hasEnoughSolToFill =
						fillerSolBalanceAfterSwap >= this.minimumAmountToFill;
				});
			} else {
				this.hasEnoughSolToFill = true;
			}
		}
	}

	protected async getNodeFillInfo(nodeToFill: NodeToFillWithBuffer): Promise<{
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

			const makerInfoMap = new Map(JSON.parse(nodeToFill.makerAccountData));
			for (const [makerAccount, makerNodes] of makerNodesMap) {
				const makerNode = makerNodes[0];
				const makerUserAccount = decodeUser(
					// @ts-ignore
					Buffer.from(makerInfoMap.get(makerAccount)!.data)
				);
				const makerAuthority = makerUserAccount.authority;
				const makerUserStats = (
					await this.userStatsMap!.mustGet(makerAuthority.toString())
				).userStatsAccountPublicKey;
				makerInfos.push({
					slot: this.slotSubscriber.getSlot(),
					data: {
						maker: new PublicKey(makerAccount),
						makerUserAccount: makerUserAccount,
						order: makerNode.order,
						makerStats: makerUserStats,
					},
				});
			}
		}

		const takerUserPubKey = nodeToFill.node.userAccount!.toString();
		const takerUserAccount = decodeUser(
			// @ts-ignore
			Buffer.from(nodeToFill.userAccountData.data)
		);
		const referrerInfo = (
			await this.userStatsMap!.mustGet(takerUserAccount.authority.toString())
		).getReferrerInfo();

		return Promise.resolve({
			makerInfos,
			takerUserPubKey,
			takerUser: takerUserAccount,
			takerUserSlot: this.slotSubscriber.getSlot(),
			referrerInfo,
			marketType: nodeToFill.node.order!.marketType,
		});
	}

	/**
	 * Queues up the txSig to be confirmed in a slower loop, and have tx logs handled
	 * @param txSig
	 */
	protected async registerTxSigToConfirm(
		txSig: TransactionSignature,
		now: number,
		nodeFilled: Array<NodeToFillWithBuffer>,
		fillTxId: number,
		txType: TxType
	) {
		this.pendingTxSigsToconfirm.set(txSig, {
			ts: now,
			nodeFilled,
			fillTxId,
			txType,
		});
		const user = this.driftClient.getUser();
		this.sentTxsCounter?.add(1, {
			txType,
			...metricAttrFromUserAccount(
				user.userAccountPublicKey,
				user.getUserAccount()
			),
		});
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
					const isExpired = isOrderExpired(
						filledNode.node.order!,
						Date.now() / 1000
					);
					logger.error(
						`assoc node (ixIdx: ${ixIdx}): ${filledNode.node.userAccount!.toString()}, ${
							filledNode.node.order!.orderId
						}; does not exist (filled by someone else); ${log}, expired: ${isExpired}`
					);
					if (isExpired) {
						const sig = getNodeToFillSignature(filledNode);
						this.expiredNodesSet.set(sig, true);
					}
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
}

import { DriftClient, BlockhashSubscriber, UserStatsMap, SerumFulfillmentConfigMap, SerumSubscriber, PhoenixFulfillmentConfigMap, PhoenixSubscriber, PriorityFeeSubscriber, NodeToFill, JupiterClient, BulkAccountLoader, initialize, DriftEnv, PollingDriftClientAccountSubscriber, getVariant, isOneOfVariant, isVariant } from "@drift-labs/sdk";
import { Connection, AddressLookupTableAccount, LAMPORTS_PER_SOL, PublicKey } from "@solana/web3.js";
import { Mutex } from "async-mutex";
import { LRUCache } from "lru-cache";
import { TxType, CONFIRM_TX_RATE_LIMIT_BACKOFF_MS, TX_TIMEOUT_THRESHOLD_MS, CONFIRM_TX_INTERVAL_MS } from "src/bots/filler";
import { BundleSender } from "src/bundleSender";
import { FillerMultiThreadedConfig, GlobalConfig } from "src/config";
import { logger } from "src/logger";
import { RuntimeSpec, GaugeValue, HistogramValue, CounterValue, Metrics, metricAttrFromUserAccount } from "src/metrics";
import { swapFillerHardEarnedUSDCForSOL, validMinimumAmountToFill } from "src/utils";
import {
	ExplicitBucketHistogramAggregation,
	InstrumentType,
	View,
} from '@opentelemetry/sdk-metrics-base';
import { ChildProcess } from "child_process";
import { spawnChildWithRetry } from "../filler-common/utils";
import { CACHED_BLOCKHASH_OFFSET } from "../filler/fillerMultithreaded";
import { SerializedNodeToFill, SerializedNodeToTrigger } from "../filler-common/types";

enum METRIC_TYPES {
	try_fill_duration_histogram = 'try_fill_duration_histogram',
	runtime_specs = 'runtime_specs',
	last_try_fill_time = 'last_try_fill_time',
	mutex_busy = 'mutex_busy',
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

type DLOBBuilderWithProcess = {
	process: ChildProcess;
	ready: boolean;
	marketIndexes: number[];
};

const logPrefix = '[SpotFiller]';

export class SpotFillerMultithreaded {
    public readonly name: string;
	public readonly dryRun: boolean;
	public readonly defaultIntervalMs: number = 2000;

	private driftClient: DriftClient;
	/// Connection to use specifically for confirming transactions
	private txConfirmationConnection: Connection;
	private globalConfig: GlobalConfig;
	private blockhashSubscriber: BlockhashSubscriber;
	private driftLutAccount?: AddressLookupTableAccount;
	private driftSpotLutAccount?: AddressLookupTableAccount;
	private fillTxId = 0;

	private userStatsMap?: UserStatsMap;

	private serumFulfillmentConfigMap: SerumFulfillmentConfigMap;
	private serumSubscribers: Map<number, SerumSubscriber>;

	private phoenixFulfillmentConfigMap: PhoenixFulfillmentConfigMap;
	private phoenixSubscribers: Map<number, PhoenixSubscriber>;

	private periodicTaskMutex = new Mutex();

	protected hasEnoughSolToFill: boolean = true;

	private watchdogTimerMutex = new Mutex();
	private watchdogTimerLastPatTime = Date.now();

	private intervalIds: Array<NodeJS.Timer> = [];
	private throttledNodes = new Map<string, number>();
	private triggeringNodes = new Map<string, number>();

	private priorityFeeSubscriber: PriorityFeeSubscriber;
	private revertOnFailure: boolean;
	private simulateTxForCUEstimate?: boolean;
	private bundleSender?: BundleSender;

	/// stores txSigs that need to been confirmed in a slower loop, and the time they were confirmed
	protected pendingTxSigsToconfirm: LRUCache<
		string,
		{
			ts: number;
			nodeFilled?: NodeToFill;
			fillTxId: number;
			txType: TxType;
		}
	>;
	protected expiredNodesSet: LRUCache<string, boolean>;
	protected confirmLoopRunning = false;
	protected confirmLoopRateLimitTs =
		Date.now() - CONFIRM_TX_RATE_LIMIT_BACKOFF_MS;

	// metrics
	private metricsInitialized = false;
	protected metrics?: Metrics;
	private bootTimeMs?: number;

	protected runtimeSpec: RuntimeSpec;
	protected runtimeSpecsGauge?: GaugeValue;
	protected tryFillDurationHistogram?: HistogramValue;
	protected estTxCuHistogram?: HistogramValue;
	protected simulateTxHistogram?: HistogramValue;
	protected lastTryFillTimeGauge?: GaugeValue;
	protected mutexBusyCounter?: CounterValue;
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
	protected jupiterClient?: JupiterClient;

	protected minimumAmountToFill: number;

    protected dlobBuilders: Map<number, DLOBBuilderWithProcess> = new Map();

	protected marketIndexes: Array<number[]>;
	protected marketIndexesFlattened: number[];

    private config: FillerMultiThreadedConfig;

    private dlobHealthy = true;
	private orderSubscriberHealthy = true;

	constructor(
		driftClient: DriftClient,
		runtimeSpec: RuntimeSpec,
		globalConfig: GlobalConfig,
		config: FillerMultiThreadedConfig,
		priorityFeeSubscriber: PriorityFeeSubscriber,
		bundleSender?: BundleSender
	) {
		this.globalConfig = globalConfig;
        this.config = config;
		this.name = config.botId;
		this.dryRun = config.dryRun;
		this.driftClient = driftClient;
        this.marketIndexes = config.marketIndexes;
		this.marketIndexesFlattened = config.marketIndexes.flat();
		if (globalConfig.txConfirmationEndpoint) {
			this.txConfirmationConnection = new Connection(
				globalConfig.txConfirmationEndpoint
			);
		} else {
			this.txConfirmationConnection = this.driftClient.connection;
		}
		this.runtimeSpec = runtimeSpec;

		this.serumFulfillmentConfigMap = new SerumFulfillmentConfigMap(driftClient);
		this.serumSubscribers = new Map<number, SerumSubscriber>();

		this.phoenixFulfillmentConfigMap = new PhoenixFulfillmentConfigMap(
			driftClient
		);
		this.phoenixSubscribers = new Map<number, PhoenixSubscriber>();

		this.initializeMetrics(config.metricsPort ?? this.globalConfig.metricsPort);

		this.priorityFeeSubscriber = priorityFeeSubscriber;
		this.priorityFeeSubscriber.updateAddresses([
			new PublicKey('8BnEgHoWFysVcuFFX7QztDmzuH8r5ZFvyP3sYwn1XTh6'), // Openbook SOL/USDC
			new PublicKey('4DoNfFBfF7UokCC2FQzriy7yHK6DY6NVdYpuekQ5pRgg'), // Phoenix SOL/USDC
			new PublicKey('6gMq3mRCKf8aP3ttTyYhuijVZ2LGi14oDsBbkgubfLB3'), // Drift USDC market
		]);

		this.revertOnFailure = config.revertOnFailure ?? true;
		this.simulateTxForCUEstimate = config.simulateTxForCUEstimate ?? true;
		this.rebalanceFiller = config.rebalanceFiller ?? true;
		logger.info(
			`${this.name}: revertOnFailure: ${this.revertOnFailure}, simulateTxForCUEstimate: ${this.simulateTxForCUEstimate}`
		);

		if (
			config.rebalanceFiller &&
			this.runtimeSpec.driftEnv === 'mainnet-beta'
		) {
			this.jupiterClient = new JupiterClient({
				connection: this.driftClient.connection,
			});
		}
		this.rebalanceFiller = config.rebalanceFiller ?? false;
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

        this.userStatsMap = new UserStatsMap(
			this.driftClient,
			new BulkAccountLoader(
				new Connection(this.driftClient.connection.rpcEndpoint),
				'confirmed',
				0
			)
		);
		this.bundleSender = bundleSender;
		this.blockhashSubscriber = new BlockhashSubscriber({
			connection: driftClient.connection,
		});
		this.pendingTxSigsToconfirm = new LRUCache<
			string,
			{
				ts: number;
				nodeFilled?: NodeToFill;
				fillTxId: number;
				txType: TxType;
			}
		>({
			max: 5_000,
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
        logger.info(`${this.name}: Initializing`);
        await this.blockhashSubscriber.subscribe();

        const config = initialize({ env: this.runtimeSpec.driftEnv as DriftEnv });
        const marketSetupPromises = config.SPOT_MARKETS.map(
			async (spotMarketConfig) => {
				const spotMarket = this.driftClient.getSpotMarketAccount(
					spotMarketConfig.marketIndex
				);
				if (
					isOneOfVariant(spotMarket?.status, [
						'initialized',
						'fillPaused',
						'delisted',
					])
				) {
					logger.info(
						`Skipping market ${
							spotMarketConfig.symbol
						} because its SpotMarket.status is ${getVariant(
							spotMarket?.status
						)}`
					);
					return;
				}

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

        this.startProcesses();
        logger.info(`${this.name}: Initialized`);
    }
	private startProcesses() {
		logger.info(`${this.name}: Starting processes`);
		const orderSubscriberArgs = [
			`--market-type=${this.config.marketType}`,
			`--market-indexes=${this.config.marketIndexes.map(String)}`,
		];
		const user = this.driftClient.getUser();

		for (const marketIndexes of this.marketIndexes) {
			logger.info(
				`${this.name}: Spawning dlobBuilder for marketIndexes: ${marketIndexes}`
			);
			const dlobBuilderArgs = [
				`--market-type=${this.config.marketType}`,
				`--market-indexes=${marketIndexes.map(String)}`,
			];
			const dlobBuilderProcess = spawnChildWithRetry(
				'./src/experimental-bots/filler-common/dlobBuilder.ts',
				dlobBuilderArgs,
				'dlobBuilder',
				(msg: any) => {
					switch (msg.type) {
						case 'initialized':
							{
								const dlobBuilder = this.dlobBuilders.get(msg.data[0]);
								if (dlobBuilder) {
									dlobBuilder.ready = true;
									for (const marketIndex of msg.data) {
										this.dlobBuilders.set(Number(marketIndex), dlobBuilder);
									}
									logger.info(
										`${logPrefix} dlobBuilderProcess initialized and acknowledged`
									);
								}
							}
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

			for (const marketIndex of marketIndexes) {
				this.dlobBuilders.set(Number(marketIndex), {
					process: dlobBuilderProcess,
					ready: false,
					marketIndexes: marketIndexes.map(Number),
				});
			}

			logger.info(
				`dlobBuilder spawned with pid: ${dlobBuilderProcess.pid} marketIndexes: ${dlobBuilderArgs}`
			);
		}

		const routeMessageToDlobBuilder = (msg: any) => {
			const dlobBuilder = this.dlobBuilders.get(Number(msg.data.marketIndex));
			if (dlobBuilder === undefined) {
				logger.error(
					`Received message for unknown marketIndex: ${msg.data.marketIndex}`
				);
				return;
			}
			if (dlobBuilder.marketIndexes.includes(Number(msg.data.marketIndex))) {
				if (typeof dlobBuilder.process.send == 'function') {
					if (dlobBuilder.ready) {
						dlobBuilder.process.send(msg);
						return;
					}
				}
			}
		};

		const orderSubscriberProcess = spawnChildWithRetry(
			'./src/experimental-bots/filler-common/orderSubscriberFiltered.ts',
			orderSubscriberArgs,
			'orderSubscriber',
			(msg: any) => {
				switch (msg.type) {
					case 'userAccountUpdate':
						routeMessageToDlobBuilder(msg);
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
			this.dlobBuilders.forEach((value: DLOBBuilderWithProcess, _: number) => {
				value.process.kill();
			});
			orderSubscriberProcess.kill();
			process.exit(0);
		});

		logger.info(
			`orderSubscriber spawned with pid: ${orderSubscriberProcess.pid}`
		);

		this.intervalIds.push(
			setInterval(
				this.rebalanceSpotFiller.bind(this),
				60_000
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

    public async triggerNodes(
        _serializedNodesToTrigger: SerializedNodeToTrigger[]
    ) {

    }

    public async fillNodes(
        _serializedNodesToFill: SerializedNodeToFill[]
    ) {

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


    protected recordEvictedTxSig(
		_tsTxSigAdded: { ts: number; nodeFilled?: NodeToFill },
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

    private async getBlockhashForTx(): Promise<string> {
		const cachedBlockhash = this.blockhashSubscriber.getLatestBlockhash(
			CACHED_BLOCKHASH_OFFSET
		);
		if (cachedBlockhash) {
			return cachedBlockhash.blockhash as string;
		}

		const recentBlockhash =
			await this.driftClient.connection.getLatestBlockhash({
				commitment: 'confirmed',
			});

		return recentBlockhash.blockhash;
	}
    
    protected async rebalanceSpotFiller() {
        logger.info(`Rebalancing filler`);
		const fillerSolBalance = await this.driftClient.connection.getBalance(
			this.driftClient.authority
		);

		this.hasEnoughSolToFill = fillerSolBalance >= this.minimumAmountToFill;

		if (this.jupiterClient !== undefined) {
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
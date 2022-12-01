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
} from '@drift-labs/sdk';
import { Mutex, tryAcquire, withTimeout, E_ALREADY_LOCKED } from 'async-mutex';

import { PublicKey } from '@solana/web3.js';

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

/**
 * Size of throttled nodes to get to before pruning the map
 */
const THROTTLED_NODE_SIZE_TO_PRUNE = 10;

/**
 * Time to wait before trying a node again
 */
const FILL_ORDER_BACKOFF = 10000;
const USER_MAP_RESYNC_COOLDOWN_SLOTS = 300;

const dlobMutexError = new Error('dlobMutex timeout');

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
}

export class SpotFillerBot implements Bot {
	public readonly name: string;
	public readonly dryRun: boolean;
	public readonly defaultIntervalMs: number = 1000;

	private bulkAccountLoader: BulkAccountLoader | undefined;
	private driftClient: DriftClient;

	private dlobMutex = withTimeout(
		new Mutex(),
		10 * this.defaultIntervalMs,
		dlobMutexError
	);
	private dlob: DLOB;

	private userMap: UserMap;
	private userStatsMap: UserStatsMap;

	private serumFulfillmentConfigMap: SerumFulfillmentConfigMap;
	private serumSubscribers: Map<number, SerumSubscriber>;

	private periodicTaskMutex = new Mutex();

	private watchdogTimerMutex = new Mutex();
	private watchdogTimerLastPatTime = Date.now();

	private lastSlotReyncUserMapsMutex = new Mutex();
	private lastSlotResyncUserMaps = 0;

	private intervalIds: Array<NodeJS.Timer> = [];
	private throttledNodes = new Map<string, number>();

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

	constructor(
		name: string,
		dryRun: boolean,
		bulkAccountLoader: BulkAccountLoader | undefined,
		clearingHouse: DriftClient,
		runtimeSpec: RuntimeSpec,
		metricsPort?: number | undefined
	) {
		this.name = name;
		this.dryRun = dryRun;
		this.bulkAccountLoader = bulkAccountLoader;
		this.driftClient = clearingHouse;
		this.runtimeSpec = runtimeSpec;

		this.serumFulfillmentConfigMap = new SerumFulfillmentConfigMap(
			clearingHouse
		);
		this.serumSubscribers = new Map<number, SerumSubscriber>();

		this.metricsPort = metricsPort;
		if (this.metricsPort) {
			this.initializeMetrics();
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

		this.userMap = new UserMap(
			this.driftClient,
			this.driftClient.userAccountSubscriptionConfig
		);
		initPromises.push(this.userMap.fetchAllUsers());

		this.userStatsMap = new UserStatsMap(
			this.driftClient,
			this.driftClient.userAccountSubscriptionConfig
		);
		initPromises.push(this.userStatsMap.fetchAllUserStats());

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
	}

	public async reset() {}

	public async startIntervalLoop(intervalMs: number) {
		const intervalId = setInterval(this.trySpotFill.bind(this), intervalMs);
		this.intervalIds.push(intervalId);

		logger.info(`${this.name} Bot started!`);
	}

	public async healthCheck(): Promise<boolean> {
		let healthy = false;
		await this.watchdogTimerMutex.runExclusive(async () => {
			healthy =
				this.watchdogTimerLastPatTime > Date.now() - 2 * this.defaultIntervalMs;
		});
		return healthy;
	}

	public async trigger(record: any) {
		await this.userMap.updateWithEventRecord(record);
		await this.userStatsMap.updateWithEventRecord(record, this.userMap);

		if (record.eventType === 'OrderRecord') {
			await this.trySpotFill(record as OrderRecord);
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
			this.userMap.size() !== stateAccount.numberOfSubAccounts.toNumber() ||
			this.userStatsMap.size() !== stateAccount.numberOfAuthorities.toNumber();

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
						logger.info(`Resyncing UserMaps`);
						doResync = true;
						this.lastSlotResyncUserMaps = this.bulkAccountLoader.mostRecentSlot;
					}
				}

				if (doResync) {
					const newUserMap = new UserMap(
						this.driftClient,
						this.driftClient.userAccountSubscriptionConfig
					);
					const newUserStatsMap = new UserStatsMap(
						this.driftClient,
						this.driftClient.userAccountSubscriptionConfig
					);
					newUserMap.fetchAllUsers().then(() => {
						newUserStatsMap
							.fetchAllUserStats()
							.then(() => {
								delete this.userMap;
								delete this.userStatsMap;
								this.userMap = newUserMap;
								this.userStatsMap = newUserStatsMap;
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
		if (nodeToFill.makerNode) {
			const makerUserAccount = (
				await this.userMap.mustGet(nodeToFill.makerNode.userAccount.toString())
			).getUserAccount();
			const makerAuthority = makerUserAccount.authority;
			const makerUserStats = (
				await this.userStatsMap.mustGet(makerAuthority.toString())
			).userStatsAccountPublicKey;
			makerInfo = {
				maker: nodeToFill.makerNode.userAccount,
				makerUserAccount: makerUserAccount,
				order: nodeToFill.makerNode.order,
				makerStats: makerUserStats,
			};
		}

		const chUser = await this.userMap.mustGet(
			nodeToFill.node.userAccount.toString()
		);
		const referrerInfo = (
			await this.userStatsMap.mustGet(
				chUser.getUserAccount().authority.toString()
			)
		).getReferrerInfo();

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

		let serumFulfillmentConfig: SerumV3FulfillmentConfigAccount = undefined;
		if (makerInfo === undefined) {
			serumFulfillmentConfig = this.serumFulfillmentConfigMap.get(
				nodeToFill.node.order.marketIndex
			);
		}

		const start = Date.now();
		this.driftClient
			.fillSpotOrder(
				chUser.getUserAccountPublicKey(),
				chUser.getUserAccount(),
				nodeToFill.node.order,
				serumFulfillmentConfig,
				makerInfo,
				referrerInfo
			)
			.then((tx) => {
				logger.info(`Filled spot order ${nodeSignature}, TX: ${tx}`);

				const duration = Date.now() - start;
				const user = this.driftClient.getUser();
				this.sdkCallDurationHistogram.record(duration, {
					...metricAttrFromUserAccount(
						user.getUserAccountPublicKey(),
						user.getUserAccount()
					),
					method: 'fillSpotOrder',
				});
			})
			.catch((e) => {
				console.error(e);
				logger.error(`Failed to fill spot order`);
			})
			.finally(() => {
				this.unthrottleNode(nodeSignature);
			});
	}

	private async trySpotFill(orderRecord?: OrderRecord) {
		const startTime = Date.now();
		let ran = false;

		await tryAcquire(this.periodicTaskMutex)
			.runExclusive(async () => {
				await this.dlobMutex.runExclusive(async () => {
					if (this.dlob) {
						this.dlob.clear();
						delete this.dlob;
					}
					this.dlob = new DLOB();
					await this.dlob.initFromUserMap(this.userMap);
					if (orderRecord) {
						this.dlob.insertOrder(orderRecord.order, orderRecord.user);
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

				await Promise.all(
					fillableNodes.map(async (spotNodeToFill) => {
						await this.tryFillSpotNode(spotNodeToFill);
					})
				);

				ran = true;
			})
			.catch((e) => {
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
					console.log('some other error...');
					console.error(e);
				}
			})
			.finally(async () => {
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
			});
	}
}

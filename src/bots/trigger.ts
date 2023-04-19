import {
	DriftClient,
	PerpMarketAccount,
	SpotMarketAccount,
	SlotSubscriber,
	DLOB,
	NodeToTrigger,
	UserMap,
	MarketType,
	BulkAccountLoader,
	getOrderSignature,
	User,
} from '@drift-labs/sdk';
import { Mutex, tryAcquire, withTimeout, E_ALREADY_LOCKED } from 'async-mutex';

import { logger } from '../logger';
import { Bot } from '../types';
import { getErrorCode } from '../error';
import { webhookMessage } from '../webhook';
import { BaseBotConfig } from '../config';
import { PrometheusExporter } from '@opentelemetry/exporter-prometheus';
import { Counter, Histogram, Meter, ObservableGauge } from '@opentelemetry/api';
import {
	ExplicitBucketHistogramAggregation,
	InstrumentType,
	MeterProvider,
	View,
} from '@opentelemetry/sdk-metrics-base';
import { RuntimeSpec, metricAttrFromUserAccount } from '../metrics';

const dlobMutexError = new Error('dlobMutex timeout');
const USER_MAP_RESYNC_COOLDOWN_SLOTS = 50;
const TRIGGER_ORDER_COOLDOWN_MS = 10000; // time to wait between triggering an order

const errorCodesToSuppress = [
	6111, // Error Message: OrderNotTriggerable.
	6112, // Error Message: OrderDidNotSatisfyTriggerCondition.
];

enum METRIC_TYPES {
	sdk_call_duration_histogram = 'sdk_call_duration_histogram',
	try_trigger_duration_histogram = 'try_trigger_duration_histogram',
	runtime_specs = 'runtime_specs',
	mutex_busy = 'mutex_busy',
	errors = 'errors',
}

export class TriggerBot implements Bot {
	public readonly name: string;
	public readonly dryRun: boolean;
	public readonly defaultIntervalMs: number = 1000;

	private bulkAccountLoader: BulkAccountLoader | undefined;
	private driftClient: DriftClient;
	private slotSubscriber: SlotSubscriber;
	private dlobMutex = withTimeout(
		new Mutex(),
		10 * this.defaultIntervalMs,
		dlobMutexError
	);
	private dlob: DLOB;
	private triggeringNodes = new Map<string, number>();
	private periodicTaskMutex = new Mutex();
	private intervalIds: Array<NodeJS.Timer> = [];
	private userMapMutex = new Mutex();
	private userMap: UserMap;
	private lastSeenNumberOfSubAccounts: number;

	// metrics
	private metricsInitialized = false;
	private metricsPort: number | undefined;
	private exporter: PrometheusExporter;
	private meter: Meter;
	private bootTimeMs = Date.now();
	private runtimeSpecsGauge: ObservableGauge;
	private runtimeSpec: RuntimeSpec;
	private mutexBusyCounter: Counter;
	private errorCounter: Counter;
	private tryTriggerDurationHistogram: Histogram;

	private watchdogTimerMutex = new Mutex();
	private watchdogTimerLastPatTime = Date.now();

	private lastSlotReyncUserMapsMutex = new Mutex();
	private lastSlotResyncUserMaps = 0;

	constructor(
		bulkAccountLoader: BulkAccountLoader | undefined,
		driftClient: DriftClient,
		slotSubscriber: SlotSubscriber,
		runtimeSpec: RuntimeSpec,
		config: BaseBotConfig
	) {
		this.name = config.botId;
		this.dryRun = config.dryRun;
		(this.bulkAccountLoader = bulkAccountLoader),
			(this.driftClient = driftClient);
		this.runtimeSpec = runtimeSpec;
		this.slotSubscriber = slotSubscriber;

		this.metricsPort = config.metricsPort;
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
					instrumentName: METRIC_TYPES.try_trigger_duration_histogram,
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
		this.mutexBusyCounter = this.meter.createCounter(METRIC_TYPES.mutex_busy, {
			description: 'Count of times the mutex was busy',
		});
		this.errorCounter = this.meter.createCounter(METRIC_TYPES.errors, {
			description: 'Count of errors',
		});
		this.tryTriggerDurationHistogram = this.meter.createHistogram(
			METRIC_TYPES.try_trigger_duration_histogram,
			{
				description: 'Distribution of tryTrigger',
				unit: 'ms',
			}
		);
	}

	public async init() {
		logger.info(`${this.name} initing`);
		// initialize userMap instance
		await this.userMapMutex.runExclusive(async () => {
			this.userMap = new UserMap(
				this.driftClient,
				this.driftClient.userAccountSubscriptionConfig
			);
			await this.userMap.fetchAllUsers();
			this.lastSeenNumberOfSubAccounts = this.driftClient
				.getStateAccount()
				.numberOfSubAccounts.toNumber();
		});
	}

	public async reset() {}

	public async startIntervalLoop(intervalMs: number): Promise<void> {
		this.tryTrigger();
		const intervalId = setInterval(this.tryTrigger.bind(this), intervalMs);
		this.intervalIds.push(intervalId);

		logger.info(`${this.name} Bot started!`);
	}

	public async healthCheck(): Promise<boolean> {
		let healthy = false;
		await this.watchdogTimerMutex.runExclusive(async () => {
			healthy =
				this.watchdogTimerLastPatTime > Date.now() - 2 * this.defaultIntervalMs;
		});

		const stateAccount = this.driftClient.getStateAccount();
		const userMapResyncRequired =
			this.lastSeenNumberOfSubAccounts !==
			stateAccount.numberOfSubAccounts.toNumber();

		healthy = healthy && !userMapResyncRequired;

		return healthy;
	}

	public async trigger(record: any): Promise<void> {
		// potentially a race here, but the lock is really slow :/
		// await this.userMapMutex.runExclusive(async () => {
		await this.userMap.updateWithEventRecord(record);
		// });
	}

	public viewDlob(): DLOB {
		return this.dlob;
	}

	/**
	 * Checks that userMap is up in sync, if not, signal that we should update them next block.
	 */
	private async resyncUserMapsIfRequired() {
		const stateAccount = this.driftClient.getStateAccount();
		const resyncRequired =
			this.lastSeenNumberOfSubAccounts !==
			stateAccount.numberOfSubAccounts.toNumber();

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
					this.userMap
						.sync()
						.then(async () => {
							await this.userMapMutex.runExclusive(async () => {
								this.lastSeenNumberOfSubAccounts = this.driftClient
									.getStateAccount()
									.numberOfSubAccounts.toNumber();
							});
						})
						.finally(() => {
							logger.info(`UserMaps resynced in ${Date.now() - start}ms`);
						});
				}
			});
		}
	}

	private async tryTriggerForPerpMarket(market: PerpMarketAccount) {
		const marketIndex = market.marketIndex;

		try {
			const oraclePriceData =
				this.driftClient.getOracleDataForPerpMarket(marketIndex);

			let nodesToTrigger: Array<NodeToTrigger> = [];
			await this.dlobMutex.runExclusive(async () => {
				nodesToTrigger = this.dlob.findNodesToTrigger(
					marketIndex,
					this.slotSubscriber.getSlot(),
					oraclePriceData.price,
					MarketType.PERP,
					this.driftClient.getStateAccount()
				);
			});

			for (const nodeToTrigger of nodesToTrigger) {
				const now = Date.now();
				const nodeToFillSignature =
					this.getNodeToTriggerSignature(nodeToTrigger);
				const timeStartedToTriggerNode =
					this.triggeringNodes.get(nodeToFillSignature);
				if (timeStartedToTriggerNode) {
					if (timeStartedToTriggerNode + TRIGGER_ORDER_COOLDOWN_MS > now) {
						logger.warn(
							`triggering node ${nodeToFillSignature} too soon (${
								now - timeStartedToTriggerNode
							}ms since last trigger), skipping`
						);
						continue;
					}
				}

				if (nodeToTrigger.node.haveTrigger) {
					continue;
				}
				nodeToTrigger.node.haveTrigger = true;

				this.triggeringNodes.set(nodeToFillSignature, Date.now());

				logger.info(
					`trying to trigger perp order on market ${
						nodeToTrigger.node.order.marketIndex
					} (account: ${nodeToTrigger.node.userAccount.toString()}) perp order ${nodeToTrigger.node.order.orderId.toString()}`
				);

				let user: User;
				await this.userMapMutex.runExclusive(async () => {
					user = await this.userMap.mustGet(
						nodeToTrigger.node.userAccount.toString()
					);
				});
				this.driftClient
					.triggerOrder(
						nodeToTrigger.node.userAccount,
						user.getUserAccount(),
						nodeToTrigger.node.order
					)
					.then((txSig) => {
						logger.info(
							`Triggered perp user (account: ${nodeToTrigger.node.userAccount.toString()}) perp order: ${nodeToTrigger.node.order.orderId.toString()}`
						);
						logger.info(`Tx: ${txSig}`);
					})
					.catch((error) => {
						nodeToTrigger.node.haveTrigger = false;

						const errorCode = getErrorCode(error);
						if (
							!errorCodesToSuppress.includes(errorCode) &&
							!(error as Error).message.includes(
								'Transaction was not confirmed'
							)
						) {
							if (errorCode) {
								this.errorCounter.add(1, { errorCode: errorCode.toString() });
							}
							logger.error(
								`Error (${errorCode}) triggering perp user (account: ${nodeToTrigger.node.userAccount.toString()}) perp order: ${nodeToTrigger.node.order.orderId.toString()}`
							);
							logger.error(error);
							webhookMessage(
								`[${
									this.name
								}]: :x: Error (${errorCode}) triggering perp user (account: ${nodeToTrigger.node.userAccount.toString()}) perp order: ${nodeToTrigger.node.order.orderId.toString()}\n${
									error.logs ? (error.logs as Array<string>).join('\n') : ''
								}\n${error.stack ? error.stack : error.message}`
							);
						}
					})
					.finally(() => {
						this.removeTriggeringNodes([nodeToTrigger]);
					});
			}
		} catch (e) {
			logger.error(
				`Unexpected error for market ${marketIndex.toString()} during triggers`
			);
			console.error(e);
			webhookMessage(
				`[${this.name}]: :x: Uncaught error:\n${e.stack ? e.stack : e.message}}`
			);
		}
	}

	private async tryTriggerForSpotMarket(market: SpotMarketAccount) {
		const marketIndex = market.marketIndex;

		try {
			const oraclePriceData =
				this.driftClient.getOracleDataForSpotMarket(marketIndex);

			let nodesToTrigger: Array<NodeToTrigger> = [];
			await this.dlobMutex.runExclusive(async () => {
				nodesToTrigger = this.dlob.findNodesToTrigger(
					marketIndex,
					this.slotSubscriber.getSlot(),
					oraclePriceData.price,
					MarketType.SPOT,
					this.driftClient.getStateAccount()
				);
			});

			for (const nodeToTrigger of nodesToTrigger) {
				if (nodeToTrigger.node.haveTrigger) {
					continue;
				}

				nodeToTrigger.node.haveTrigger = true;

				logger.info(
					`trying to trigger (account: ${nodeToTrigger.node.userAccount.toString()}) spot order ${nodeToTrigger.node.order.orderId.toString()}`
				);

				let user: User;
				await this.userMapMutex.runExclusive(async () => {
					user = await this.userMap.mustGet(
						nodeToTrigger.node.userAccount.toString()
					);
				});
				this.driftClient
					.triggerOrder(
						nodeToTrigger.node.userAccount,
						user.getUserAccount(),
						nodeToTrigger.node.order
					)
					.then((txSig) => {
						logger.info(
							`Triggered user (account: ${nodeToTrigger.node.userAccount.toString()}) spot order: ${nodeToTrigger.node.order.orderId.toString()}`
						);
						logger.info(`Tx: ${txSig}`);
					})
					.catch((error) => {
						nodeToTrigger.node.haveTrigger = false;

						const errorCode = getErrorCode(error);
						if (
							!errorCodesToSuppress.includes(errorCode) &&
							!(error as Error).message.includes(
								'Transaction was not confirmed'
							)
						) {
							if (errorCode) {
								this.errorCounter.add(1, { errorCode: errorCode.toString() });
							}
							logger.error(
								`Error (${errorCode}) triggering spot order for user (account: ${nodeToTrigger.node.userAccount.toString()}) spot order: ${nodeToTrigger.node.order.orderId.toString()}`
							);
							logger.error(error);
							webhookMessage(
								`[${
									this.name
								}]: :x: Error (${errorCode}) triggering spot order for user (account: ${nodeToTrigger.node.userAccount.toString()}) spot order: ${nodeToTrigger.node.order.orderId.toString()}\n${
									error.stack ? error.stack : error.message
								}`
							);
						}
					});
			}
		} catch (e) {
			logger.error(
				`Unexpected error for spot market ${marketIndex.toString()} during triggers`
			);
			console.error(e);
		}
	}

	private getNodeToTriggerSignature(node: NodeToTrigger): string {
		return getOrderSignature(node.node.order.orderId, node.node.userAccount);
	}

	private removeTriggeringNodes(nodes: Array<NodeToTrigger>) {
		for (const node of nodes) {
			this.triggeringNodes.delete(this.getNodeToTriggerSignature(node));
		}
	}

	private async tryTrigger() {
		const start = Date.now();
		let ran = false;
		try {
			await tryAcquire(this.periodicTaskMutex).runExclusive(async () => {
				await this.dlobMutex.runExclusive(async () => {
					if (this.dlob) {
						this.dlob.clear();
						delete this.dlob;
					}
					this.dlob = new DLOB();
					await this.userMapMutex.runExclusive(async () => {
						await this.dlob.initFromUserMap(
							this.userMap,
							this.slotSubscriber.getSlot()
						);
					});
				});

				await this.resyncUserMapsIfRequired();

				await Promise.all([
					this.driftClient.getPerpMarketAccounts().map((marketAccount) => {
						this.tryTriggerForPerpMarket(marketAccount);
					}),
					this.driftClient.getSpotMarketAccounts().map((marketAccount) => {
						this.tryTriggerForSpotMarket(marketAccount);
					}),
				]);
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
				webhookMessage(
					`[${this.name}]: :x: Uncaught error in main loop:\n${
						e.stack ? e.stack : e.message
					}`
				);
				throw e;
			}
		} finally {
			if (ran) {
				const user = this.driftClient.getUser();

				const duration = Date.now() - start;
				this.tryTriggerDurationHistogram.record(
					duration,
					metricAttrFromUserAccount(
						user.getUserAccountPublicKey(),
						user.getUserAccount()
					)
				);

				logger.debug(`${this.name} Bot took ${duration}ms to run`);
				await this.watchdogTimerMutex.runExclusive(async () => {
					this.watchdogTimerLastPatTime = Date.now();
				});
			}
		}
	}
}

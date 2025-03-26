import {
	DriftClient,
	PerpMarketAccount,
	SpotMarketAccount,
	SlotSubscriber,
	NodeToTrigger,
	UserMap,
	MarketType,
	DLOBSubscriber,
	PublicKey,
	BlockhashSubscriber,
	PriorityFeeSubscriber,
} from '@drift-labs/sdk';
import { Mutex, tryAcquire, E_ALREADY_LOCKED } from 'async-mutex';

import { logger } from '../logger';
import { Bot } from '../types';
import { getErrorCode } from '../error';
import { webhookMessage } from '../webhook';
import { TriggerConfig } from '../config';
import { PrometheusExporter } from '@opentelemetry/exporter-prometheus';
import { Counter, Histogram, Meter, ObservableGauge } from '@opentelemetry/api';
import {
	ExplicitBucketHistogramAggregation,
	InstrumentType,
	MeterProvider,
	View,
} from '@opentelemetry/sdk-metrics-base';
import { RuntimeSpec, metricAttrFromUserAccount } from '../metrics';
import { getNodeToTriggerSignature, getVersionedTransaction } from '../utils';
import {
	AddressLookupTableAccount,
	ComputeBudgetProgram,
} from '@solana/web3.js';

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

	private driftClient: DriftClient;
	private slotSubscriber: SlotSubscriber;
	private triggerConfig: TriggerConfig;
	private dlobSubscriber?: DLOBSubscriber;
	private blockhashSubscriber: BlockhashSubscriber;
	private lookupTableAccounts?: AddressLookupTableAccount[];
	private triggeringNodes = new Map<string, number>();
	private periodicTaskMutex = new Mutex();
	private intervalIds: Array<NodeJS.Timer> = [];
	private userMap: UserMap;

	private priorityFeeSubscriber: PriorityFeeSubscriber;

	// metrics
	private metricsInitialized = false;
	private metricsPort?: number;
	private exporter?: PrometheusExporter;
	private meter?: Meter;
	private bootTimeMs = Date.now();
	private runtimeSpecsGauge?: ObservableGauge;
	private runtimeSpec: RuntimeSpec;
	private mutexBusyCounter?: Counter;
	private errorCounter?: Counter;
	private tryTriggerDurationHistogram?: Histogram;

	private watchdogTimerMutex = new Mutex();
	private watchdogTimerLastPatTime = Date.now();

	constructor(
		driftClient: DriftClient,
		slotSubscriber: SlotSubscriber,
		blockhashSubscriber: BlockhashSubscriber,
		userMap: UserMap,
		runtimeSpec: RuntimeSpec,
		config: TriggerConfig,
		priorityFeeSubscriber: PriorityFeeSubscriber
	) {
		this.name = config.botId;
		this.dryRun = config.dryRun;
		this.triggerConfig = config;
		this.driftClient = driftClient;
		this.userMap = userMap;
		this.runtimeSpec = runtimeSpec;
		this.slotSubscriber = slotSubscriber;
		this.blockhashSubscriber = blockhashSubscriber;

		this.metricsPort = config.metricsPort;
		if (this.metricsPort) {
			this.initializeMetrics();
		}
		this.priorityFeeSubscriber = priorityFeeSubscriber;
		this.priorityFeeSubscriber.updateAddresses([
			new PublicKey('8BnEgHoWFysVcuFFX7QztDmzuH8r5ZFvyP3sYwn1XTh6'), // Openbook SOL/USDC
			new PublicKey('8UJgxaiQx5nTrdDgph5FiahMmzduuLTLf5WmsPegYA6W'), // sol-perp
		]);
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
		logger.info(
			`${this.name} initing (trigger cu boost: ${this.triggerConfig.triggerPriorityFeeMultiplier})`
		);

		this.dlobSubscriber = new DLOBSubscriber({
			dlobSource: this.userMap,
			slotSource: this.slotSubscriber,
			updateFrequency: this.defaultIntervalMs - 500,
			driftClient: this.driftClient,
		});
		await this.dlobSubscriber.subscribe();

		this.lookupTableAccounts =
			await this.driftClient.fetchAllLookupTableAccounts();
	}

	public async reset() {
		for (const intervalId of this.intervalIds) {
			clearInterval(intervalId as NodeJS.Timeout);
		}
		this.intervalIds = [];

		await this.dlobSubscriber!.unsubscribe();
		await this.userMap!.unsubscribe();
	}

	public async startIntervalLoop(intervalMs?: number): Promise<void> {
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

		return healthy;
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

	private async tryTriggerForPerpMarket(market: PerpMarketAccount) {
		const marketIndex = market.marketIndex;

		try {
			const oraclePriceData =
				this.driftClient.getOracleDataForPerpMarket(marketIndex);

			const dlob = this.dlobSubscriber!.getDLOB();
			const nodesToTrigger = dlob.findNodesToTrigger(
				marketIndex,
				this.slotSubscriber.getSlot(),
				oraclePriceData.price,
				MarketType.PERP,
				this.driftClient.getStateAccount()
			);

			for (const nodeToTrigger of nodesToTrigger) {
				const now = Date.now();
				const nodeToFillSignature = getNodeToTriggerSignature(nodeToTrigger);
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

				const user = await this.userMap!.mustGet(
					nodeToTrigger.node.userAccount.toString()
				);

				const ixs = [
					ComputeBudgetProgram.setComputeUnitLimit({
						units: 100_000,
					}),
					ComputeBudgetProgram.setComputeUnitPrice({
						microLamports: Math.floor(
							this.priorityFeeSubscriber.getCustomStrategyResult() *
								this.driftClient.txSender.getSuggestedPriorityFeeMultiplier() *
								(this.triggerConfig.triggerPriorityFeeMultiplier ?? 1.0)
						),
					}),
				];
				ixs.push(
					await this.driftClient.getTriggerOrderIx(
						new PublicKey(nodeToTrigger.node.userAccount),
						user.getUserAccount(),
						nodeToTrigger.node.order
					)
				);

				ixs.push(await this.driftClient.getRevertFillIx());

				const tx = getVersionedTransaction(
					this.driftClient.wallet.publicKey,
					ixs,
					this.lookupTableAccounts!,
					await this.getBlockhashForTx()
				);

				this.driftClient
					.sendTransaction(tx)
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
							errorCode &&
							!errorCodesToSuppress.includes(errorCode) &&
							!(error as Error).message.includes(
								'Transaction was not confirmed'
							)
						) {
							if (errorCode) {
								this.errorCounter!.add(1, { errorCode: errorCode.toString() });
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
			if (e instanceof Error) {
				webhookMessage(
					`[${this.name}]: :x: Uncaught error:\n${
						e.stack ? e.stack : e.message
					}}`
				);
			}
		}
	}

	private async tryTriggerForSpotMarket(market: SpotMarketAccount) {
		const marketIndex = market.marketIndex;

		try {
			const oraclePriceData =
				this.driftClient.getOracleDataForSpotMarket(marketIndex);

			const dlob = this.dlobSubscriber!.getDLOB();
			const nodesToTrigger = dlob.findNodesToTrigger(
				marketIndex,
				this.slotSubscriber.getSlot(),
				oraclePriceData.price,
				MarketType.SPOT,
				this.driftClient.getStateAccount()
			);

			for (const nodeToTrigger of nodesToTrigger) {
				if (nodeToTrigger.node.haveTrigger) {
					continue;
				}

				nodeToTrigger.node.haveTrigger = true;

				logger.info(
					`trying to trigger (account: ${nodeToTrigger.node.userAccount.toString()}) spot order ${nodeToTrigger.node.order.orderId.toString()}`
				);

				const user = await this.userMap!.mustGet(
					nodeToTrigger.node.userAccount.toString()
				);

				const txParams = {
					computeUnits: 100_000,
					computeUnitsPrice: Math.floor(
						this.priorityFeeSubscriber.getCustomStrategyResult() *
							this.driftClient.txSender.getSuggestedPriorityFeeMultiplier() *
							(this.triggerConfig.triggerPriorityFeeMultiplier ?? 1.0)
					),
				};

				this.driftClient
					.triggerOrder(
						new PublicKey(nodeToTrigger.node.userAccount),
						user.getUserAccount(),
						nodeToTrigger.node.order,
						txParams
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
							errorCode &&
							!errorCodesToSuppress.includes(errorCode) &&
							!(error as Error).message.includes(
								'Transaction was not confirmed'
							)
						) {
							if (errorCode) {
								this.errorCounter!.add(1, { errorCode: errorCode.toString() });
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

	private removeTriggeringNodes(nodes: Array<NodeToTrigger>) {
		for (const node of nodes) {
			this.triggeringNodes.delete(getNodeToTriggerSignature(node));
		}
	}

	private async tryTrigger() {
		const start = Date.now();
		let ran = false;
		try {
			await tryAcquire(this.periodicTaskMutex).runExclusive(async () => {
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
						`[${this.name}]: :x: Uncaught error in main loop:\n${
							e.stack ? e.stack : e.message
						}`
					);
				}
				throw e;
			}
		} finally {
			if (ran) {
				const user = this.driftClient.getUser();

				const duration = Date.now() - start;
				if (this.tryTriggerDurationHistogram) {
					this.tryTriggerDurationHistogram!.record(
						duration,
						metricAttrFromUserAccount(
							user.getUserAccountPublicKey(),
							user.getUserAccount()
						)
					);
				}

				logger.debug(`${this.name} Bot took ${duration}ms to run`);
				await this.watchdogTimerMutex.runExclusive(async () => {
					this.watchdogTimerLastPatTime = Date.now();
				});
			}
		}
	}
}

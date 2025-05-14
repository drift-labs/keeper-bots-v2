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
	isVariant,
	getVariant,
	SpotMarketConfig,
	PerpMarketConfig,
	MainnetSpotMarkets,
	DevnetSpotMarkets,
	MainnetPerpMarkets,
	DevnetPerpMarkets,
	getFeedIdUint8Array,
	getPythPullOraclePublicKey,
	convertToNumber,
	BN,
	convertToBN,
	PRICE_PRECISION,
} from '@drift-labs/sdk';
import { Mutex, tryAcquire, E_ALREADY_LOCKED } from 'async-mutex';

import { logger } from '../logger';
import { Bot } from '../types';
import { getErrorCode } from '../error';
import { webhookMessage } from '../webhook';
import { GlobalConfig, TriggerConfig } from '../config';
import { FeedIdToCrankInfo } from './pythCranker';
import { PrometheusExporter } from '@opentelemetry/exporter-prometheus';
import { Counter, Histogram, Meter, ObservableGauge } from '@opentelemetry/api';
import {
	ExplicitBucketHistogramAggregation,
	InstrumentType,
	MeterProvider,
	View,
} from '@opentelemetry/sdk-metrics-base';
import { RuntimeSpec, metricAttrFromUserAccount } from '../metrics';
import {
	chunks,
	getNodeToTriggerSignature,
	getVersionedTransaction,
} from '../utils';
import {
	AddressLookupTableAccount,
	ComputeBudgetProgram,
	TransactionInstruction,
} from '@solana/web3.js';
import { PriceUpdateAccount } from '@pythnetwork/pyth-solana-receiver/lib/PythSolanaReceiver';
import { PythLazerSubscriber } from '../pythLazerSubscriber';
import { PythPriceFeedSubscriber } from '../pythPriceFeedSubscriber';

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
	trigger_attempts = 'trigger_attempts',
}

function getPythLazerFeedIdChunks(
	spotMarkets: SpotMarketConfig[],
	perpMarkets: PerpMarketConfig[],
	chunkSize = 11
): number[][] {
	const allFeedIds: number[] = [];
	for (const market of [...spotMarkets, ...perpMarkets]) {
		if (
			!getVariant(market.oracleSource).toLowerCase().includes('lazer') ||
			market.pythLazerId == undefined
		) {
			continue;
		}
		allFeedIds.push(market.pythLazerId!);
	}

	const allFeedIdsSet = new Set(allFeedIds);
	return chunks(Array.from(allFeedIdsSet), chunkSize);
}

function getPythPullFeedIdsToCrank(
	programId: PublicKey,
	spotMarkets: SpotMarketConfig[],
	perpMarkets: PerpMarketConfig[]
): [FeedIdToCrankInfo[], Map<string, string>] {
	const feedIdsToCrank: FeedIdToCrankInfo[] = [];
	const marketIdToFeedId: Map<string, string> = new Map();

	for (const market of [...spotMarkets, ...perpMarkets]) {
		const oracleSourceStr = getVariant(market.oracleSource).toLowerCase();
		if (
			!(
				oracleSourceStr.includes('pull') &&
				oracleSourceStr.includes('pyth') &&
				market.pythFeedId !== undefined
			)
		) {
			continue;
		}

		const isSpotMarket = 'precision' in market;
		const marketId = isSpotMarket
			? `spot-${market.marketIndex}`
			: `perp-${market.marketIndex}`;
		marketIdToFeedId.set(marketId, market.pythFeedId);

		const idx = feedIdsToCrank.findIndex((x) => x.feedId === market.pythFeedId);
		if (idx !== -1) {
			continue;
		}

		feedIdsToCrank.push({
			baseSymbol: market.symbol.toUpperCase(),
			feedId: market.pythFeedId,
			updateConfig: {
				timeDiffMs: 0,
				priceDiffPct: 0,
			},
			earlyUpdateConfig: {
				timeDiffMs: 0,
				priceDiffPct: 0,
			},
			accountAddress: getPythPullOraclePublicKey(
				programId,
				getFeedIdUint8Array(market.pythFeedId)
			),
		});
	}

	return [feedIdsToCrank, marketIdToFeedId];
}

export class TriggerBot implements Bot {
	public readonly name: string;
	public readonly dryRun: boolean;
	public readonly defaultIntervalMs: number = 1000;

	private driftClient: DriftClient;
	private slotSubscriber: SlotSubscriber;
	private globalConfig: GlobalConfig;
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
	private triggerCounter?: Counter;
	private tryTriggerDurationHistogram?: Histogram;

	private watchdogTimerMutex = new Mutex();
	private watchdogTimerLastPatTime = Date.now();

	private updateOracleWithTrigger: boolean = false;

	readonly pythPullDecodeFunc?: (
		name: string,
		data: Buffer
	) => PriceUpdateAccount;
	readonly pythLazerDecodeFunc?: (
		name: string,
		data: Buffer
	) => PriceUpdateAccount;

	private pythLazerClient?: PythLazerSubscriber;
	private pythPullFeedIdsToCrank: FeedIdToCrankInfo[] = [];
	private pythPullClient?: PythPriceFeedSubscriber;

	// map from marketId (i.e. perp-0 or spot-0) to pyth feed id
	private marketIdToPythPullFeedId: Map<string, string> = new Map();
	// private feedIdToMarketId: Map<string, string> = new Map();
	private pythPullFeedIdToPrice: Map<string, number> = new Map();
	// map from marketId (i.e. perp-0 or spot-0) to multiplier (1, 10, 1000, etc.)
	private marketIdToMultiplier: Map<
		string,
		{ oracleSource: 'pull' | 'lazer'; multiplier: number }
	> = new Map();

	constructor(
		driftClient: DriftClient,
		slotSubscriber: SlotSubscriber,
		blockhashSubscriber: BlockhashSubscriber,
		userMap: UserMap,
		runtimeSpec: RuntimeSpec,
		config: TriggerConfig,
		globalConfig: GlobalConfig,
		priorityFeeSubscriber: PriorityFeeSubscriber
	) {
		this.name = config.botId;
		this.dryRun = config.dryRun;
		this.triggerConfig = config;
		this.globalConfig = globalConfig;
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

		if (
			this.globalConfig.lazerEndpoints &&
			this.globalConfig.lazerToken &&
			this.globalConfig.hermesEndpoint
		) {
			logger.info('Updating pyth oracles with trigger');
			this.updateOracleWithTrigger = true;

			this.pythPullDecodeFunc = this.driftClient
				.getReceiverProgram()
				.account.priceUpdateV2.coder.accounts.decodeUnchecked.bind(
					this.driftClient.getReceiverProgram().account.priceUpdateV2.coder
						.accounts
				);
			this.pythLazerDecodeFunc =
				this.driftClient.program.account.pythLazerOracle.coder.accounts.decodeUnchecked.bind(
					this.driftClient.program.account.pythLazerOracle.coder.accounts
				);

			const spotMarkets =
				this.globalConfig.driftEnv === 'mainnet-beta'
					? MainnetSpotMarkets
					: DevnetSpotMarkets;
			const perpMarkets =
				this.globalConfig.driftEnv === 'mainnet-beta'
					? MainnetPerpMarkets
					: DevnetPerpMarkets;
			this.pythLazerClient = new PythLazerSubscriber(
				this.globalConfig.lazerEndpoints,
				this.globalConfig.lazerToken,
				getPythLazerFeedIdChunks(spotMarkets, perpMarkets, 1),
				this.globalConfig.driftEnv
			);

			this.pythPullClient = new PythPriceFeedSubscriber(
				this.globalConfig.hermesEndpoint,
				{
					timeout: 10_000,
				}
			);
			[this.pythPullFeedIdsToCrank, this.marketIdToPythPullFeedId] =
				getPythPullFeedIdsToCrank(
					this.driftClient.program.programId,
					spotMarkets,
					perpMarkets
				);

			for (const market of [...spotMarkets, ...perpMarkets]) {
				const isSpotMarket = 'precision' in market;
				const marketId = isSpotMarket
					? `spot-${market.marketIndex}`
					: `perp-${market.marketIndex}`;
				const oracleSource = getVariant(market.oracleSource);
				let oracleSourceType: 'pull' | 'lazer';
				if (oracleSource.toLowerCase().includes('lazer')) {
					oracleSourceType = 'lazer';
				} else if (oracleSource.toLowerCase().includes('pull')) {
					oracleSourceType = 'pull';
				} else {
					logger.warn(
						`Not updating oracle pre-trigger for market ${marketId} with oracleSource ${oracleSource}`
					);
					continue;
				}
				if (oracleSource.toLowerCase().includes('1k')) {
					this.marketIdToMultiplier.set(marketId, {
						oracleSource: oracleSourceType,
						multiplier: 1000,
					});
				} else if (oracleSource.toLowerCase().includes('1m')) {
					this.marketIdToMultiplier.set(marketId, {
						oracleSource: oracleSourceType,
						multiplier: 1000000,
					});
				} else {
					this.marketIdToMultiplier.set(marketId, {
						oracleSource: oracleSourceType,
						multiplier: 1,
					});
				}
			}
		} else {
			logger.info(
				'Not updating pyth oracles with trigger (globalConfig missing lazerEndpoints, lazerToken, or hermesEndpoint)'
			);
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
		this.triggerCounter = this.meter.createCounter(
			METRIC_TYPES.trigger_attempts,
			{
				description: 'Count of trigger attempts',
			}
		);
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

		if (
			this.updateOracleWithTrigger &&
			this.pythLazerClient &&
			this.pythPullClient
		) {
			await this.pythLazerClient.subscribe();
			await this.pythPullClient.subscribe(
				this.pythPullFeedIdsToCrank.map((x) => x.feedId)
			);
		}
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

	private getOffChainOraclePrice(
		marketType: MarketType,
		marketIndex: number
	): BN | undefined {
		if (!this.updateOracleWithTrigger) return undefined;

		// TODO: support spot markets, need to also update pythLazerSubscriber
		if (isVariant(marketType, 'spot')) {
			return undefined;
		}
		const marketId = `perp-${marketIndex}`;
		const oracleInfo = this.marketIdToMultiplier.get(marketId);
		if (!oracleInfo) {
			return undefined;
		}
		if (oracleInfo.oracleSource === 'pull') {
			const price = this.pythPullFeedIdToPrice.get(
				this.marketIdToPythPullFeedId.get(marketId)!
			);
			if (!price) {
				logger.warn(`No price for market ${marketId}`);
				return undefined;
			}
			return convertToBN(price * oracleInfo.multiplier, PRICE_PRECISION);
		} else {
			const price = this.pythLazerClient?.getPriceFromMarketIndex(marketIndex);
			if (!price) {
				logger.warn(`No price for market ${marketId}`);
				return undefined;
			}
			return convertToBN(price * oracleInfo.multiplier, PRICE_PRECISION);
		}
	}

	private async getOracleUpdateIxs(
		marketType: MarketType,
		marketIndex: number,
		ixs: TransactionInstruction[]
	): Promise<TransactionInstruction[]> {
		if (!this.updateOracleWithTrigger) return [];

		// TODO: support spot markets, need to also update pythLazerSubscriber
		if (isVariant(marketType, 'spot')) {
			return [];
		}
		const marketId = `perp-${marketIndex}`;
		const oracleInfo = this.marketIdToMultiplier.get(marketId);
		if (!oracleInfo) {
			return [];
		}

		if (oracleInfo.oracleSource === 'pull') {
			const feedId = this.marketIdToPythPullFeedId.get(marketId);
			if (!feedId) {
				return [];
			}
			const vaa = await this.pythPullClient?.getLatestCachedVaa(feedId);
			if (!vaa) {
				return [];
			}
			ixs.push(
				...(await this.driftClient.getPostPythPullOracleUpdateAtomicIxs(
					vaa[0],
					feedId
				))
			);
		} else {
			const msg =
				await this.pythLazerClient?.getLatestPriceMessageForMarketIndex(
					marketIndex
				);
			if (!msg) {
				return [];
			}
			const feedIds =
				this.pythLazerClient?.getPriceFeedIdsFromMarketIndex(marketIndex);
			if (!feedIds) {
				return [];
			}
			ixs.push(
				...(await this.driftClient.getPostPythLazerOracleUpdateIxs(
					feedIds,
					msg,
					ixs
				))
			);
		}

		return ixs;
	}

	private async tryTriggerForMarket(
		market: PerpMarketAccount | SpotMarketAccount,
		marketType: MarketType
	) {
		const marketIndex = market.marketIndex;
		const marketTypeStr = getVariant(marketType);

		try {
			const oraclePriceData = isVariant(marketType, 'perp')
				? this.driftClient.getOracleDataForPerpMarket(marketIndex)
				: this.driftClient.getOracleDataForSpotMarket(marketIndex);

			const offChainPrice = this.getOffChainOraclePrice(
				marketType,
				marketIndex
			);

			const dlob = this.dlobSubscriber!.getDLOB();
			const nodesToTrigger = dlob.findNodesToTrigger(
				marketIndex,
				this.slotSubscriber.getSlot(),
				offChainPrice ? offChainPrice : oraclePriceData.price,
				marketType,
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
					`trying to trigger ${marketTypeStr} order on market ${
						nodeToTrigger.node.order.marketIndex
					}. user: ${nodeToTrigger.node.userAccount.toString()}-${nodeToTrigger.node.order.orderId.toString()}. oracleUpdate: ${
						this.updateOracleWithTrigger
					}. OnChainPrice: ${convertToNumber(
						oraclePriceData.price
					)}. OffChainPrice: ${
						offChainPrice ? convertToNumber(offChainPrice) : 'N/A'
					}`
				);

				const user = await this.userMap!.mustGet(
					nodeToTrigger.node.userAccount.toString()
				);

				let cuUnits = 100_000; // base case
				const activePositions =
					user.getActivePerpPositions().length +
					user.getActiveSpotPositions().length;
				const openOrders = user.getUserAccount().openOrders;
				cuUnits += activePositions * 15_000;
				cuUnits += openOrders * 5_000;

				let ixs = [
					ComputeBudgetProgram.setComputeUnitLimit({
						units: cuUnits,
					}),
					ComputeBudgetProgram.setComputeUnitPrice({
						microLamports: Math.floor(
							this.priorityFeeSubscriber.getCustomStrategyResult() *
								this.driftClient.txSender.getSuggestedPriorityFeeMultiplier() *
								(this.triggerConfig.triggerPriorityFeeMultiplier ?? 1.0)
						),
					}),
				];
				if (offChainPrice) {
					ixs = await this.getOracleUpdateIxs(marketType, marketIndex, ixs);
				}
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
						this.triggerCounter!.add(1, {
							marketType: marketTypeStr,
							auth: this.driftClient.wallet.publicKey.toString(),
						});
						logger.info(
							`Triggered ${marketTypeStr}. user: ${nodeToTrigger.node.userAccount.toString()}-${nodeToTrigger.node.order.orderId.toString()}: ${
								txSig.txSig
							}, cuUnits: ${cuUnits}, activePositions: ${activePositions}, openOrders: ${openOrders}`
						);
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
								`Error (${errorCode}) triggering ${marketTypeStr} order for user ${nodeToTrigger.node.userAccount.toString()}-${nodeToTrigger.node.order.orderId.toString()}`
							);
							logger.error(error);
							webhookMessage(
								`[${
									this.name
								}]: :x: Error (${errorCode}) triggering ${marketTypeStr} order for user (account: ${nodeToTrigger.node.userAccount.toString()}) ${marketTypeStr} order: ${nodeToTrigger.node.order.orderId.toString()}\n${
									error.stack ? error.stack : error.message
								}`
							);
						}
					})
					.finally(() => {
						this.removeTriggeringNodes([nodeToTrigger]);
					});
			}
		} catch (e) {
			logger.error(
				`Unexpected error for ${marketTypeStr} market ${marketIndex.toString()} during triggers`
			);
			console.error(e);
			if (e instanceof Error) {
				webhookMessage(
					`[${this.name}]: :x: Uncaught error:\n${
						e.stack ? e.stack : e.message
					}`
				);
			}
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
						this.tryTriggerForMarket(marketAccount, MarketType.PERP);
					}),
					this.driftClient.getSpotMarketAccounts().map((marketAccount) => {
						this.tryTriggerForMarket(marketAccount, MarketType.SPOT);
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

import {
	Counter,
	Histogram,
	Meter,
	ObservableGauge,
	ValueType,
	ObservableResult,
	BatchObservableResult,
} from '@opentelemetry/api-metrics';
import {
	MeterProvider,
	View,
	InstrumentType,
	ExplicitBucketHistogramAggregation,
} from '@opentelemetry/sdk-metrics-base';
import { PrometheusExporter } from '@opentelemetry/exporter-prometheus';
import { logger } from './logger';

import {
	BN,
	ClearingHouse,
	convertToNumber,
	BASE_PRECISION,
	QUOTE_PRECISION,
	PublicKey,
	PerpMarkets,
	PerpPosition,
	LiquidationRecord,
	isVariant,
	MarketType,
} from '@drift-labs/sdk';

import { Mutex } from 'async-mutex';
import sizeof from 'object-sizeof';

const driftEnv = process.env.DRIFT_ENV || 'devnet';

export class Metrics {
	private exporter: PrometheusExporter;
	private meter: Meter;
	private intervalIds: Array<NodeJS.Timer> = [];

	private bootTimeMs: number;
	private upTimeGauge: ObservableGauge;

	private openOrdersLock = new Mutex();
	private openOrders: number;
	private openOrdersGauge: ObservableGauge;

	private openPositionsLock = new Mutex();
	private openPositionPerMarket: Array<PerpPosition> = [];
	private openPositionLastCumulativeFundingRateGauge: ObservableGauge;
	private openPositionBaseAssetAmountGauge: ObservableGauge;
	private openPositionQuoteAssetAmountGauge: ObservableGauge;
	private openPositionQuoteEntryAmountGauge: ObservableGauge;
	private openPositionUnsettledPnLAmountGauge: ObservableGauge;
	private openPositionOpenOrdersAmountGauge: ObservableGauge;
	private openPositionOpenBidsAmountGauge: ObservableGauge;
	private openPositionOpenAsksAmountGauge: ObservableGauge;

	private objectsToTrackSizeLock = new Mutex();
	private objectsToTrackSize = new Map<string, any>();
	private objectsToTrackSizeGauge: ObservableGauge;

	private chUserUnrealizedPNLLock = new Mutex();
	private chUserUnrealizedPNL: number;
	private chUserUnrealizedPNLGauge: ObservableGauge;

	private chUserUnrealizedFundingPNLLock = new Mutex();
	private chUserUnrealizedFundingPNL: number;
	private chUserUnrealizedFundingPNLGauge: ObservableGauge;

	private chUserInitialMarginRequirementLock = new Mutex();
	private chUserInitialMarginRequirement: number;
	private chUserInitialMarginRequirementGauge: ObservableGauge;

	private chUserMaintenanceMarginRequirementLock = new Mutex();
	private chUserMaintenanceMarginRequirement: number;
	private chUserMaintenanceMarginRequirementGauge: ObservableGauge;

	private fillableOrdersSeenLock = new Mutex();
	private fillablePerpOrdersSeenByMarket = new Map<number, number>();
	private fillableSpotOrdersSeenByMarket = new Map<number, number>();
	private fillableOrdersSeentGauge: ObservableGauge;

	private errorsCounter: Counter;
	private filledOrdersCounter: Counter;
	private perpLiquidationsCounter: Counter;
	private liquidationEventsCounter: Counter;
	private settlePnlCounter: Counter;
	private mutexBusyCounter: Counter;
	private rpcRequestsCounter: Counter;
	private rpcRequestsDurationHistogram: Histogram;

	private clearingHouse: ClearingHouse;
	private authority: PublicKey;

	constructor(clearingHouse: ClearingHouse, metricsPort?: number) {
		const { endpoint: defaultEndpoint, port: defaultPort } =
			PrometheusExporter.DEFAULT_OPTIONS;
		const port = metricsPort || defaultPort;
		this.exporter = new PrometheusExporter(
			{
				port: port,
				endpoint: defaultEndpoint,
			},
			() => {
				logger.info(
					`prometheus scrape endpoint started: http://localhost:${port}${defaultEndpoint}`
				);
			}
		);

		const meterName = 'internal-keeper-bot';
		const meterProvider = new MeterProvider({
			views: [
				new View({
					instrumentName: 'rpc_requests_duration',
					instrumentType: InstrumentType.HISTOGRAM,
					meterName: meterName,
					aggregation: new ExplicitBucketHistogramAggregation(
						Array.from(new Array(20), (_, i) => 500 + i * 500)
					),
				}),
			],
		});
		meterProvider.addMetricReader(this.exporter);
		this.meter = meterProvider.getMeter(meterName);

		this.clearingHouse = clearingHouse;
		this.authority = this.clearingHouse.provider.wallet.publicKey;

		this.bootTimeMs = Date.now();
		this.upTimeGauge = this.meter.createObservableGauge('boot_time', {
			description: 'Unix ms time of boot',
			unit: 'ms',
			valueType: ValueType.INT,
		});
		this.upTimeGauge.addCallback((observableResult: ObservableResult): void => {
			observableResult.observe(Date.now() - this.bootTimeMs);
		});

		// this.collateralValueGauge = this.meter.createObservableGauge(
		// 	'collateral_value',
		// 	{
		// 		description: 'Collateral value of the user per bank',
		// 		unit: 'USD',
		// 		valueType: ValueType.DOUBLE,
		// 	}
		// );
		// this.collateralValueGauge.addCallback(
		// 	async (observableResult: ObservableResult): Promise<void> => {
		// 		this.collateralValueLock.runExclusive(async () => {
		// 			for (let i = 0; i < this.collateralValuePerBank.length; i++) {
		// 				observableResult.observe(this.collateralValuePerBank[i], {
		// 					bankIndex: i,
		// 					bankSymbol: DevnetBanks[i].symbol,
		// 					userPubKey: this.authority.toBase58(),
		// 				});
		// 			}
		// 		});
		// 	}
		// );

		// this.solBalanceGauge = this.meter.createObservableGauge(
		// 	'lamports_balance',
		// 	{
		// 		description: 'Authority SOL balance',
		// 		unit: 'SOL',
		// 		valueType: ValueType.DOUBLE,
		// 	}
		// );
		// this.solBalanceGauge.addCallback(
		// 	async (observableResult: ObservableResult): Promise<void> => {
		// 		this.solBalanceLock.runExclusive(async () => {
		// 			observableResult.observe(this.solBalance, {
		// 				userPubKey: this.authority.toBase58(),
		// 			});
		// 		});
		// 	}
		// );

		this.openOrdersGauge = this.meter.createObservableGauge('open_orders', {
			description: 'Open orders',
			valueType: ValueType.INT,
		});
		this.openOrdersGauge.addCallback(
			async (observableResult: ObservableResult): Promise<void> => {
				await this.openOrdersLock.runExclusive(async () => {
					observableResult.observe(this.openOrders, {
						userPubKey: this.authority.toBase58(),
					});
				});
			}
		);

		this.openPositionLastCumulativeFundingRateGauge =
			this.meter.createObservableGauge(
				'open_position_last_cumulative_funding_rate',
				{
					description: 'Last cumulative funding rate of the open position',
					valueType: ValueType.DOUBLE,
				}
			);
		this.openPositionBaseAssetAmountGauge = this.meter.createObservableGauge(
			'open_position_base_asset_amount',
			{
				description: 'Base asset amount of the open position',
				valueType: ValueType.DOUBLE,
			}
		);
		this.openPositionQuoteAssetAmountGauge = this.meter.createObservableGauge(
			'open_position_quote_asset_amount',
			{
				description: 'Quote asset amount of the open position',
				valueType: ValueType.DOUBLE,
			}
		);
		this.openPositionQuoteEntryAmountGauge = this.meter.createObservableGauge(
			'open_position_quote_entry_amount',
			{
				description: 'Quote entry amount of the open position',
				valueType: ValueType.DOUBLE,
			}
		);
		this.openPositionUnsettledPnLAmountGauge = this.meter.createObservableGauge(
			'open_position_unsettled_pnl_amount',
			{
				description: 'Unsettled PnL amount of the open position',
				valueType: ValueType.DOUBLE,
			}
		);
		this.openPositionOpenOrdersAmountGauge = this.meter.createObservableGauge(
			'open_position_open_orders_amount',
			{
				description: 'Open orders amount of the open position',
				valueType: ValueType.DOUBLE,
			}
		);
		this.openPositionOpenBidsAmountGauge = this.meter.createObservableGauge(
			'open_position_open_bids_amount',
			{
				description: 'Open bids amount of the open position',
				valueType: ValueType.DOUBLE,
			}
		);
		this.openPositionOpenAsksAmountGauge = this.meter.createObservableGauge(
			'open_position_open_asks_amount',
			{
				description: 'Open asks amount of the open position',
				valueType: ValueType.DOUBLE,
			}
		);
		this.meter.addBatchObservableCallback(
			async (observableResult: BatchObservableResult) => {
				await this.openPositionsLock.runExclusive(async () => {
					for (let i = 0; i < this.openPositionPerMarket.length; i++) {
						const p = this.openPositionPerMarket[i];
						if (!p) {
							continue;
						}
						observableResult.observe(
							this.openPositionLastCumulativeFundingRateGauge,
							convertToNumber(
								p.lastCumulativeFundingRate,
								new BN(10).pow(new BN(14))
							),
							{
								marketIndex: i,
								marketSymbol: PerpMarkets[driftEnv][i].symbol,
								userPubKey: this.authority.toBase58(),
							}
						);
						observableResult.observe(
							this.openPositionBaseAssetAmountGauge,
							convertToNumber(p.baseAssetAmount, BASE_PRECISION),
							{
								marketIndex: i,
								marketSymbol: PerpMarkets[driftEnv][i].symbol,
								userPubKey: this.authority.toBase58(),
							}
						);
						observableResult.observe(
							this.openPositionQuoteAssetAmountGauge,
							convertToNumber(p.quoteAssetAmount, QUOTE_PRECISION),
							{
								marketIndex: i,
								marketSymbol: PerpMarkets[driftEnv][i].symbol,
								userPubKey: this.authority.toBase58(),
							}
						);
						observableResult.observe(
							this.openPositionQuoteEntryAmountGauge,
							convertToNumber(p.quoteEntryAmount, QUOTE_PRECISION),
							{
								marketIndex: i,
								marketSymbol: PerpMarkets[driftEnv][i].symbol,
								userPubKey: this.authority.toBase58(),
							}
						);
						// observableResult.observe(
						// 	this.openPositionUnsettledPnLAmountGauge,
						// 	convertToNumber(p.unsettledPnl, QUOTE_PRECISION),
						// 	{
						// 		marketIndex: i,
						// 		marketSymbol: PerpMarkets[driftEnv][i].symbol,
						// 		userPubKey: this.authority.toBase58(),
						// 	}
						// );
						observableResult.observe(
							this.openPositionOpenOrdersAmountGauge,
							p.openOrders,
							{
								marketIndex: i,
								marketSymbol: PerpMarkets[driftEnv][i].symbol,
								userPubKey: this.authority.toBase58(),
							}
						);
						observableResult.observe(
							this.openPositionOpenBidsAmountGauge,
							convertToNumber(p.openBids, BASE_PRECISION),
							{
								marketIndex: i,
								marketSymbol: PerpMarkets[driftEnv][i].symbol,
								userPubKey: this.authority.toBase58(),
							}
						);
						observableResult.observe(
							this.openPositionOpenAsksAmountGauge,
							convertToNumber(p.openAsks, BASE_PRECISION),
							{
								marketIndex: i,
								marketSymbol: PerpMarkets[driftEnv][i].symbol,
								userPubKey: this.authority.toBase58(),
							}
						);
					}
				});
			},
			[
				this.openPositionLastCumulativeFundingRateGauge,
				this.openPositionBaseAssetAmountGauge,
				this.openPositionQuoteAssetAmountGauge,
				this.openPositionQuoteEntryAmountGauge,
				this.openPositionUnsettledPnLAmountGauge,
				this.openPositionOpenOrdersAmountGauge,
				this.openPositionOpenBidsAmountGauge,
				this.openPositionOpenAsksAmountGauge,
			]
		);

		// this.chUserUnsettledPNLGauge = this.meter.createObservableGauge(
		// 	'user_unsettled_pnl',
		// 	{
		// 		description: 'Unsettled PnL of the user',
		// 		valueType: ValueType.DOUBLE,
		// 	}
		// );
		// this.chUserUnsettledPNLGauge.addCallback(
		// 	(observableResult: ObservableResult): void => {
		// 		this.chUserUnsettledPNLLock.runExclusive(async () => {
		// 			observableResult.observe(this.chUserUnsettledPNL, {
		// 				userPubKey: this.authority.toBase58(),
		// 			});
		// 		});
		// 	}
		// );

		this.chUserUnrealizedPNLGauge = this.meter.createObservableGauge(
			'ch_user_unrealized_pnl',
			{
				description: 'Unrealized PnL of the user',
				valueType: ValueType.DOUBLE,
			}
		);
		this.chUserUnrealizedPNLGauge.addCallback(
			async (observableResult: ObservableResult) => {
				await this.chUserUnrealizedPNLLock.runExclusive(async () => {
					observableResult.observe(this.chUserUnrealizedPNL, {
						userPubKey: this.authority.toBase58(),
					});
				});
			}
		);

		this.chUserUnrealizedFundingPNLGauge = this.meter.createObservableGauge(
			'ch_user_unrealized_funding_pnl',
			{
				description: 'Unrealized funding PnL of the user',
				valueType: ValueType.DOUBLE,
			}
		);
		this.chUserUnrealizedFundingPNLGauge.addCallback(
			async (observableResult: ObservableResult) => {
				await this.chUserUnrealizedFundingPNLLock.runExclusive(async () => {
					observableResult.observe(this.chUserUnrealizedFundingPNL, {
						userPubKey: this.authority.toBase58(),
					});
				});
			}
		);

		this.chUserInitialMarginRequirementGauge = this.meter.createObservableGauge(
			'ch_user_initial_margin_requirement',
			{
				description: 'Initial margin requirement of the user',
				valueType: ValueType.DOUBLE,
			}
		);
		this.chUserInitialMarginRequirementGauge.addCallback(
			async (observableResult: ObservableResult) => {
				await this.chUserInitialMarginRequirementLock.runExclusive(async () => {
					observableResult.observe(this.chUserInitialMarginRequirement, {
						userPubKey: this.authority.toBase58(),
					});
				});
			}
		);

		this.chUserMaintenanceMarginRequirementGauge =
			this.meter.createObservableGauge(
				'ch_user_maintenance_margin_requirement',
				{
					description: 'Maintenance margin requirement of the user',
					valueType: ValueType.DOUBLE,
				}
			);
		this.chUserMaintenanceMarginRequirementGauge.addCallback(
			async (observableResult: ObservableResult) => {
				await this.chUserMaintenanceMarginRequirementLock.runExclusive(
					async () => {
						observableResult.observe(this.chUserMaintenanceMarginRequirement, {
							userPubKey: this.authority.toBase58(),
						});
					}
				);
			}
		);

		this.objectsToTrackSizeGauge = this.meter.createObservableGauge(
			'tracked_object_sizes',
			{
				description: 'Bytes size of tracked objects',
				valueType: ValueType.INT,
			}
		);
		this.objectsToTrackSizeGauge.addCallback(
			async (observableResult: ObservableResult) => {
				await this.objectsToTrackSizeLock.runExclusive(async () => {
					// iterate over all tracked objects, name and obj
					for (const [name, obj] of this.objectsToTrackSize) {
						observableResult.observe(sizeof(obj), {
							object: name,
							userPubKey: this.authority.toBase58(),
						});
					}
				});
			}
		);

		this.errorsCounter = this.meter.createCounter('errors', {
			description: 'ClearingHouse error counter',
		});

		this.filledOrdersCounter = this.meter.createCounter('filled_orders', {
			description: 'Count of orders successfully filled',
		});

		this.perpLiquidationsCounter = this.meter.createCounter(
			'perp_liquidations',
			{
				description: 'Count of successful perp liquidations',
			}
		);

		this.liquidationEventsCounter = this.meter.createCounter(
			'liquidation_events',
			{
				description: 'Count of liquidation events',
			}
		);

		this.settlePnlCounter = this.meter.createCounter('settle_pnls', {
			description: 'Count of settle pnl txns',
		});

		this.mutexBusyCounter = this.meter.createCounter('mutex_busy', {
			description: 'Count of times the mutex was busy',
		});

		this.rpcRequestsCounter = this.meter.createCounter('rpc_requests', {
			description: 'Count of rpc requests made',
		});

		this.rpcRequestsDurationHistogram = this.meter.createHistogram(
			'rpc_requests_duration',
			{
				description: 'Duration of rpc requests',
				unit: 'ms',
			}
		);

		this.fillableOrdersSeentGauge = this.meter.createObservableGauge(
			'fillable_orders_seen',
			{
				description: 'Fillable orders seen by the order filler',
				valueType: ValueType.INT,
			}
		);
		this.fillableOrdersSeentGauge.addCallback(
			async (observableResult: ObservableResult) => {
				await this.fillableOrdersSeenLock.runExclusive(async () => {
					for (const [marketIndex, fillableOrders] of this
						.fillablePerpOrdersSeenByMarket) {
						observableResult.observe(fillableOrders, {
							market: marketIndex,
							marketType: 'perp',
						});
					}
					for (const [marketIndex, fillableOrders] of this
						.fillableSpotOrdersSeenByMarket) {
						observableResult.observe(fillableOrders, {
							market: marketIndex,
							marketType: 'spot',
						});
					}
				});
			}
		);
	}

	async init() {
		this.runPeriodicTasks();
		const intervalId = setInterval(this.runPeriodicTasks.bind(this), 5000);
		this.intervalIds.push(intervalId);
	}

	public reset(): void {
		for (const intervalId of this.intervalIds) {
			clearInterval(intervalId);
		}
		this.intervalIds = [];
	}

	recordErrorCode(errorCode: number, authority: PublicKey, bot: string) {
		this.errorsCounter.add(1, {
			errorCode: errorCode,
			user: authority.toBase58(),
			bot: bot,
		});
	}

	recordFilledOrder(authority: PublicKey, bot: string, count = 1) {
		this.filledOrdersCounter.add(count, {
			user: authority.toBase58(),
			bot: bot,
		});
	}

	recordSettlePnl(numSettled: number, marketIndex: number, bot: string) {
		this.settlePnlCounter.add(numSettled, {
			market: marketIndex,
			bot: bot,
		});
	}

	recordMutexBusy(bot: string) {
		this.mutexBusyCounter.add(1, {
			bot: bot,
		});
	}

	recordRpcRequests(method: string, bot: string) {
		this.rpcRequestsCounter.add(1, {
			method: method,
			bot: bot,
		});
	}

	recordRpcDuration(
		endpoint: string,
		method: string,
		duration: number,
		timeout: boolean,
		bot: string
	) {
		this.rpcRequestsDurationHistogram.record(duration, {
			endpoint: endpoint,
			method: method,
			timeout: timeout,
			bot: bot,
		});
	}

	recordPerpLiquidation(
		liquidator: PublicKey,
		liquidatee: PublicKey,
		bot: string
	) {
		this.perpLiquidationsCounter.add(1, {
			liquidator: liquidator.toBase58(),
			liquidatee: liquidatee.toBase58(),
			bot: bot,
		});
	}

	recordLiquidationEvent(event: LiquidationRecord, bot: string) {
		let liquidationType: string;
		let liquidatedMarketIndex: number | undefined;
		let liquidatedAssetBankIndex: number | undefined;
		let liquidatedLiabilityIndex: number | undefined;

		if (isVariant(event.liquidationType, 'liquidatePerp')) {
			liquidationType = 'liquidatePerp';
			liquidatedMarketIndex = event.liquidatePerp.marketIndex;
		} else if (isVariant(event.liquidationType, 'liquidateBorrow')) {
			liquidationType = 'liquidateBorrow';
			liquidatedAssetBankIndex = event.liquidateSpot.assetMarketIndex;
			liquidatedLiabilityIndex = event.liquidateSpot.liabilityMarketIndex;
		} else if (isVariant(event.liquidationType, 'liquidateBorrowForPerpPnl')) {
			liquidationType = 'liquidateBorrowForPerpPnl';
			liquidatedMarketIndex = event.liquidateBorrowForPerpPnl.perpMarketIndex;
			liquidatedLiabilityIndex =
				event.liquidateBorrowForPerpPnl.liabilityMarketIndex;
		} else if (isVariant(event.liquidationType, 'liquidatePerpPnlForDeposit')) {
			liquidationType = 'liquidatePerpPnlForDeposit';
			liquidatedMarketIndex = event.liquidatePerpPnlForDeposit.perpMarketIndex;
			liquidatedAssetBankIndex =
				event.liquidatePerpPnlForDeposit.assetMarketIndex;
		}

		this.liquidationEventsCounter.add(1, {
			market: liquidatedMarketIndex,
			type: liquidationType,
			marketIndex: liquidatedMarketIndex,
			assetBankIndex: liquidatedAssetBankIndex,
			liabilityBankIndex: liquidatedLiabilityIndex,
			bot: bot,
		});
	}

	async recordFillableOrdersSeen(
		marketIndex: number,
		marketType: MarketType,
		fillableOrders: number
	) {
		await this.fillableOrdersSeenLock.runExclusive(async () => {
			if (isVariant(marketType, 'perp')) {
				this.fillablePerpOrdersSeenByMarket.set(marketIndex, fillableOrders);
			} else if (isVariant(marketType, 'spot')) {
				this.fillableSpotOrdersSeenByMarket.set(marketIndex, fillableOrders);
			}
		});
	}

	async trackObjectSize(name: string, object: any) {
		await this.objectsToTrackSizeLock.runExclusive(async () => {
			this.objectsToTrackSize.set(name, object);
		});
	}

	/**
	 * Run periodic tasks.
	 *
	 * Periodically update metrics, so that when metric collections are initiated
	 * they are available immediately.
	 */
	async runPeriodicTasks() {
		const chUser = this.clearingHouse.getUser();

		// this.collateralValueLock.runExclusive(async () => {
		// 	this.collateralValuePerBank = [];
		// 	for (let i = 0; i < DevnetBanks.length; i++) {
		// 		const collateralValue = convertToNumber(
		// 			chUser.(new BN(i)),
		// 			QUOTE_PRECISION
		// 		);

		// 		this.collateralValuePerBank.push(collateralValue);
		// 	}
		// });

		// this.solBalanceLock.runExclusive(async () => {
		// 	const lamportsBal = await this.clearingHouse.connection.getBalance(
		// 		await this.clearingHouse.getUserAccountPublicKey()
		// 	);
		// 	this.solBalance = lamportsBal / 10 ** 9;
		// });

		await this.openOrdersLock.runExclusive(async () => {
			let openOrdersCount = 0;
			for (let i = 0; i < chUser.getUserAccount().orders.length; i++) {
				const o = chUser.getUserAccount().orders[i];
				if (!o.baseAssetAmount.isZero()) {
					openOrdersCount++;
				}
			}
			this.openOrders = openOrdersCount;
		});

		await this.openPositionsLock.runExclusive(async () => {
			if (this.openPositionPerMarket.length != PerpMarkets[driftEnv].length) {
				this.openPositionPerMarket = Array<PerpPosition>(
					PerpMarkets[driftEnv].length
				).fill(undefined);
			}
			for (let i = 0; i < PerpMarkets[driftEnv].length; i++) {
				let foundPositionInMarket = false;
				chUser.getUserAccount().perpPositions.forEach((p: PerpPosition) => {
					if (!foundPositionInMarket && p.marketIndex === i) {
						foundPositionInMarket = true;
						this.openPositionPerMarket[p.marketIndex] = p;
					}
				});

				if (!foundPositionInMarket) {
					this.openPositionPerMarket[i] = undefined;
				}
			}
		});

		await this.chUserUnrealizedPNLLock.runExclusive(async () => {
			this.chUserUnrealizedPNL = convertToNumber(
				chUser.getUnrealizedPNL(),
				QUOTE_PRECISION
			);
		});

		await this.chUserUnrealizedFundingPNLLock.runExclusive(async () => {
			this.chUserUnrealizedFundingPNL = convertToNumber(
				chUser.getUnrealizedFundingPNL(),
				QUOTE_PRECISION
			);
		});
		// this.chUserUnsettledPNLLock.runExclusive(async () => {
		// 	this.chUserUnsettledPNL = convertToNumber(
		// 		chUser.getUnsettledPNL(),
		// 		QUOTE_PRECISION
		// 	);
		// });
		await this.chUserInitialMarginRequirementLock.runExclusive(async () => {
			this.chUserInitialMarginRequirement = convertToNumber(
				chUser.getInitialMarginRequirement(),
				QUOTE_PRECISION
			);
		});
		await this.chUserMaintenanceMarginRequirementLock.runExclusive(async () => {
			this.chUserMaintenanceMarginRequirement = convertToNumber(
				chUser.getMaintenanceMarginRequirement(),
				QUOTE_PRECISION
			);
		});
	}
}

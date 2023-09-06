import {
	calculateAskPrice,
	calculateBidPrice,
	BN,
	isVariant,
	DriftClient,
	PerpMarketAccount,
	SlotSubscriber,
	PositionDirection,
	OrderType,
	BASE_PRECISION,
	QUOTE_PRECISION,
	convertToNumber,
	PRICE_PRECISION,
	Order,
	PerpPosition,
	User,
	ZERO,
	MarketType,
} from '@drift-labs/sdk';
import { Mutex, tryAcquire, E_ALREADY_LOCKED } from 'async-mutex';

import { logger } from '../logger';
import { Bot } from '../types';
import { RuntimeSpec, metricAttrFromUserAccount } from '../metrics';
import { PrometheusExporter } from '@opentelemetry/exporter-prometheus';
import { Counter, Histogram, Meter, ObservableGauge } from '@opentelemetry/api';
import {
	ExplicitBucketHistogramAggregation,
	InstrumentType,
	MeterProvider,
	View,
} from '@opentelemetry/sdk-metrics-base';
import { FloatingMakerConfig } from '../config';

type State = {
	marketPosition: Map<number, PerpPosition>;
	openOrders: Map<number, Array<Order>>;
};

const MARKET_UPDATE_COOLDOWN_SLOTS = 30; // wait slots before updating market position

enum METRIC_TYPES {
	sdk_call_duration_histogram = 'sdk_call_duration_histogram',
	try_make_duration_histogram = 'try_make_duration_histogram',
	runtime_specs = 'runtime_specs',
	mutex_busy = 'mutex_busy',
	errors = 'errors',
}

/**
 *
 * This bot is responsible for placing limit orders that rest on the DLOB.
 * limit price offsets are used to automatically shift the orders with the
 * oracle price, making order updating automatic.
 *
 */
export class FloatingPerpMakerBot implements Bot {
	public readonly name: string;
	public readonly dryRun: boolean;
	public defaultIntervalMs: number = 5000;
	private baseOracleOffset: number;
	private minOracleOffset: number;
	private maxOracleOffset: number;
	private orderSize: number = 1;
	private subAccountId: number = 0;
	// Maximum percentage of the total account value a position
	// is allowed to take. The bot will start skewing order
	// values as the position size approaches this percentage.
	private maxPositionExposure: number;

	private driftClient: DriftClient;
	private slotSubscriber: SlotSubscriber;
	private periodicTaskMutex = new Mutex();
	private lastSlotMarketUpdated: Map<number, number> = new Map();

	private intervalIds: Array<NodeJS.Timer> = [];

	private user: User;

	// metrics
	private metricsInitialized = false;
	private metricsPort?: number;
	private exporter?: PrometheusExporter;
	private meter?: Meter;
	private bootTimeMs = Date.now();
	private runtimeSpecsGauge?: ObservableGauge;
	private runtimeSpec?: RuntimeSpec;
	private mutexBusyCounter?: Counter;
	private tryMakeDurationHistogram?: Histogram;

	private agentState?: State;

	private marketIndices: Set<number>;
	private markets: PerpMarketAccount[] = [];
	private marketMinOrderSize: Map<number, number> = new Map();

	private watchdogTimerMutex = new Mutex();
	private watchdogTimerLastPatTime = Date.now();

	constructor(
		clearingHouse: DriftClient,
		slotSubscriber: SlotSubscriber,
		runtimeSpec: RuntimeSpec,
		config: FloatingMakerConfig
	) {
		this.name = config.botId;
		this.dryRun = config.dryRun;
		this.driftClient = clearingHouse;
		this.slotSubscriber = slotSubscriber;

		this.defaultIntervalMs = config.intervalMs ?? 5000;
		this.baseOracleOffset = config.baseOrderOffset;
		this.minOracleOffset = config.minOrderOffset;
		this.maxOracleOffset = config.maxOrderOffset;
		this.orderSize = config.orderSize ?? 1;
		this.maxPositionExposure = config.maxPositionExposure;

		// Configure user and subaccount
		this.subAccountId = config.subAccountId ?? 0;
		if (this.subAccountId != 0) {
			logger.warn(`Using subAccountId ${this.subAccountId}.`);
			logger.warn(
				`The bot will only change subaccounts once, so make sure there are no conflicting changes on other bots.`
			);
		}
		this.driftClient.switchActiveUser(this.subAccountId);
		this.user = this.driftClient.getUser();

		this.marketIndices = new Set<number>(config.perpMarketIndices);

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
					instrumentName: METRIC_TYPES.try_make_duration_histogram,
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
		this.tryMakeDurationHistogram = this.meter.createHistogram(
			METRIC_TYPES.try_make_duration_histogram,
			{
				description: 'Distribution of tryTrigger',
				unit: 'ms',
			}
		);
	}

	public async init() {
		logger.info(`${this.name} initing`);

		// There migth be orders that are unfilled, but no longer
		// match the bot's current parameters. Cancel them.
		logger.info(`Canceling all PERP orders in case there are leftover ones`);
		await this.driftClient.cancelOrders(MarketType.PERP);

		this.agentState = {
			marketPosition: new Map<number, PerpPosition>(),
			openOrders: new Map<number, Array<Order>>(),
		};
		this.updateAgentState();

		this.markets = this.driftClient
			.getPerpMarketAccounts()
			.filter((marketAccount) => {
				return this.marketIndices.has(marketAccount.marketIndex);
			});

		for (const i of this.marketIndices) {
			const ei = this.driftClient.getPerpMarketExtendedInfo(i);
			const minOrderSize = convertToNumber(ei.minOrderSize, BASE_PRECISION);
			this.marketMinOrderSize.set(i, minOrderSize);
			logger.info(`Min order size for ${i}: ${minOrderSize}`);
		}
	}

	public async reset() {
		for (const intervalId of this.intervalIds) {
			clearInterval(intervalId as NodeJS.Timeout);
		}
		this.intervalIds = [];
	}

	public async startIntervalLoop(intervalMs: number) {
		await this.updateOpenOrders();
		const intervalId = setInterval(
			this.updateOpenOrders.bind(this),
			intervalMs
		);
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

	/**
	 * Updates the agent state based on its current market positions.
	 *
	 * We want to maintain a two-sided market while being conscious of the positions
	 * taken on by the account.
	 *
	 * As open positions approach maxPositionExposure, limit orders are skewed such
	 * that the order size skews towards the side that decreases risk.
	 *
	 * It'll also update the offset numerator so that the position that decreases
	 * risk will be closer to the oracle price, and the position that increases
	 * risk will be further from the oracle price.
	 *
	 * @returns {Promise<void>}
	 */
	private updateAgentState(): void {
		this.driftClient.getUserAccount()!.perpPositions.map((p) => {
			if (p.baseAssetAmount.isZero()) {
				return;
			}
			this.agentState!.marketPosition.set(p.marketIndex, p);
		});

		// zero out the open orders
		for (const market of this.markets) {
			this.agentState!.openOrders.set(market.marketIndex, []);
		}

		this.driftClient.getUserAccount()!.orders.map((o) => {
			if (isVariant(o.status, 'init')) {
				return;
			}
			const marketIndex = o.marketIndex;
			this.agentState!.openOrders.set(marketIndex, [
				...(this.agentState!.openOrders.get(marketIndex) ?? []),
				o,
			]);
		});
	}

	private async updateOpenOrdersForMarket(marketAccount: PerpMarketAccount) {
		const currSlot = this.slotSubscriber.currentSlot;
		const marketIndex = marketAccount.marketIndex;
		const nextUpdateSlot =
			this.lastSlotMarketUpdated.get(marketIndex) ??
			0 + MARKET_UPDATE_COOLDOWN_SLOTS;

		if (nextUpdateSlot > currSlot) {
			return;
		}

		// First calculate the total exposure. We'll use it not only to skew
		// the values, but to figure out if we need to quote both sides
		// (normal situation), or only one (closing)
		const oracle = this.driftClient.getOracleDataForPerpMarket(marketIndex);
		const totalAssetValue = convertToNumber(
			this.user.getSpotMarketAssetValue(
				0, // USDC
				'Maintenance',
				true
			),
			QUOTE_PRECISION
		);
		const position = this.user.getPerpPosition(marketIndex);
		const posAmount = convertToNumber(
			position?.baseAssetAmount ?? ZERO,
			BASE_PRECISION
		);
		const perpValue = convertToNumber(
			this.user.getPerpPositionValue(marketIndex, oracle),
			QUOTE_PRECISION
		);
		const exposure = perpValue / totalAssetValue;
		// Clamping, because there's a chance that the perp has exceeded the allowed exposure
		const pctExpAllowed = Math.min(
			1,
			Math.max(1 - perpValue / (totalAssetValue * this.maxPositionExposure), 0)
		);
		// Yes, this will give a LONG direction when there's no position. It makes no difference,
		// because in that case the exposure will be 0 and it'll be evenly split.
		const exposureDir =
			Math.sign(posAmount) >= 0
				? PositionDirection.LONG
				: PositionDirection.SHORT;
		logger.info(
			`PerpPosition value: $${perpValue} with ${posAmount}. Exposure from collateral: ${(
				exposure * 100
			).toFixed(2)}%. Free allowance: ${(pctExpAllowed * 100).toFixed(2)}%`
		);
		const isLong = exposureDir == PositionDirection.LONG;
		const minOrder = this.marketMinOrderSize.get(marketIndex) ?? 0.01;
		const isClosing = pctExpAllowed * this.orderSize < minOrder;

		// Get the open orders and log
		const vAsk = calculateAskPrice(marketAccount, oracle);
		const vBid = calculateBidPrice(marketAccount, oracle);

		const openOrders = this.agentState!.openOrders.get(marketIndex) || [];
		console.log(`mkt: ${marketAccount.marketIndex} open orders:`);
		for (const [idx, o] of openOrders.entries()) {
			console.log(
				`${Object.keys(o.orderType)[0]} ${Object.keys(o.direction)[0]}`
			);
			console.log(
				`[${idx}]: baseAmountFilled: ${convertToNumber(
					o.baseAssetAmountFilled,
					BASE_PRECISION
				)}/${convertToNumber(o.baseAssetAmount, BASE_PRECISION)}`
			);
			console.log(` .        qaa: ${o.quoteAssetAmount}`);
			console.log(
				` .        price:       ${convertToNumber(o.price, PRICE_PRECISION)}`
			);
			console.log(
				` .        priceOffset: ${convertToNumber(
					new BN(o.oraclePriceOffset),
					PRICE_PRECISION
				)}`
			);
			console.log(` .        vBid: ${convertToNumber(vBid, PRICE_PRECISION)}`);
			console.log(` .        vAsk: ${convertToNumber(vAsk, PRICE_PRECISION)}`);
			console.log(
				` .        oraclePrice: ${convertToNumber(
					oracle.price,
					PRICE_PRECISION
				)}`
			);
			console.log(` .        oracleSlot:  ${oracle.slot.toString()}`);
			console.log(` .        oracleConf:  ${oracle.confidence.toString()}`);
		}

		// cancel orders if not quoting both sides of the market
		let placeNewOrders = openOrders.length === 0;
		logger.info(`Orders open: ${openOrders.length}.`);

		// TODO Evaluating by openOrders.length here is iffy, since for all we know
		// the system could have ended up in a state where we have two orders
		// quoting on the same side, but leaving it for now.
		if (
			openOrders.length > 0 &&
			openOrders.length != 2 &&
			!(isClosing && openOrders.length == 1)
		) {
			// cancel orders
			try {
				const idsToCancel = openOrders.map((o) => o.orderId);
				// TODO / IMPROVEMENT: Given we have an instruction to cancel multiple
				// orders at once, it might be best to first go through all markets
				// and cancel all outstanding orders in a single call, before moving
				// on to place new ones per market.
				await this.driftClient.cancelOrdersByIds(idsToCancel);
				placeNewOrders = true;
			} catch (e) {
				logger.error('Error canceling orders. Will NOT place new ones.');
				console.error(e);
				console.log('----');
				placeNewOrders = false;
			}
		}

		if (placeNewOrders) {
			// Calculate offsets
			const offsetCurrent =
				(this.minOracleOffset - this.baseOracleOffset) * (1 - pctExpAllowed) +
				this.baseOracleOffset;
			const offsetOther =
				this.baseOracleOffset -
				(this.baseOracleOffset - this.maxOracleOffset) * (1 - pctExpAllowed);
			const offsetLong = isLong ? offsetCurrent : offsetOther;
			const offsetShort = !isLong ? offsetCurrent : offsetOther;

			logger.info(`Offsets: Long ${offsetLong} Short: ${offsetShort}`);

			// Calculate the order size.
			//
			// Notice that if this ever gets too small - say, we have hit the exposure
			// allowance and need to skew it to one side - order placing will barf.
			//
			// How to handle this is left as an exercise. You may only want to quote one
			// side and keep that order open for a while, or keep canceling it and
			// moving it closer to the oracle (see openOrders.length comparison).
			const pctCurrent = pctExpAllowed / 2;
			const orderSizeCurrent = pctCurrent * (this.orderSize * 2);
			const orderSizeOther = (1 - pctCurrent) * (this.orderSize * 2);
			const orderSizeLong = isLong ? orderSizeCurrent : orderSizeOther;
			const orderSizeShort = !isLong ? orderSizeCurrent : orderSizeOther;

			// The lower the bias numerator, the further below a long bid would be from the oracle price
			// (or the further above a short ask)
			const biasDenom = new BN(100);

			try {
				const oracleBidSpread = oracle.price.sub(vBid);
				const oracleAskSpread = vAsk.sub(oracle.price);
				const orders = [];
				if (orderSizeLong > minOrder) {
					orders.push({
						marketIndex: marketIndex,
						orderType: OrderType.LIMIT,
						direction: PositionDirection.LONG,
						baseAssetAmount: new BN(orderSizeLong * BASE_PRECISION.toNumber()),
						oraclePriceOffset: oracleBidSpread
							.mul(new BN(offsetLong))
							.div(biasDenom)
							.neg()
							.toNumber(), // limit bid below oracle
					});
				}
				if (orderSizeShort > minOrder) {
					orders.push({
						marketIndex: marketIndex,
						orderType: OrderType.LIMIT,
						direction: PositionDirection.SHORT,
						baseAssetAmount: new BN(orderSizeShort * BASE_PRECISION.toNumber()),
						oraclePriceOffset: oracleAskSpread
							.mul(new BN(offsetShort))
							.div(biasDenom)
							.toNumber(), // limit ask above oracle
					});
				}
				await Promise.all(
					orders.map((o) => {
						logger.info(`${this.name} placing order: ${JSON.stringify(o)}`);
						this.driftClient.placePerpOrder(o);
					})
				);
			} catch (e) {
				console.log('Error placing new orders');
				console.error(e);
				console.log('----');
			}
		}

		// enforce cooldown on market
		this.lastSlotMarketUpdated.set(marketIndex, currSlot);
	}

	private async updateOpenOrders() {
		const start = Date.now();
		let ran = false;
		try {
			await tryAcquire(this.periodicTaskMutex).runExclusive(async () => {
				this.updateAgentState();

				await Promise.all(
					this.markets.map((marketAccount) => {
						console.log(
							`${this.name} updating open orders for market ${marketAccount.marketIndex}`
						);
						this.updateOpenOrdersForMarket(marketAccount);
					})
				);

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
				// Not re-throwing, as that would abort the floatingMaker
				// altogether and potentially leave it with open orders
				logger.error(`Exception processing transaction ${e}`);
			}
		} finally {
			if (ran) {
				const duration = Date.now() - start;
				const user = this.driftClient.getUser();
				this.tryMakeDurationHistogram!.record(
					duration,
					metricAttrFromUserAccount(
						user.getUserAccountPublicKey(),
						user.getUserAccount()
					)
				);
				logger.debug(`${this.name} Bot took ${Date.now() - start}ms to run`);

				await this.watchdogTimerMutex.runExclusive(async () => {
					this.watchdogTimerLastPatTime = Date.now();
				});
			}
		}
	}
}

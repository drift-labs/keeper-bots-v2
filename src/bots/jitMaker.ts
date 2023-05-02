import {
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
	PerpPosition,
	SpotPosition,
	DLOB,
	DLOBNode,
	UserMap,
	UserStatsMap,
	getOrderSignature,
	MarketType,
	PostOnlyParams,
	DLOBSubscriber,
	EventSubscriber,
	WrappedEvent,
} from '@drift-labs/sdk';
import { Mutex, tryAcquire, E_ALREADY_LOCKED } from 'async-mutex';

import { TransactionSignature, PublicKey } from '@solana/web3.js';

import { getErrorCode } from '../error';
import { logger } from '../logger';
import { Bot } from '../types';

import { PrometheusExporter } from '@opentelemetry/exporter-prometheus';
import { Counter, Histogram, Meter, ObservableGauge } from '@opentelemetry/api';
import {
	ExplicitBucketHistogramAggregation,
	InstrumentType,
	MeterProvider,
	View,
} from '@opentelemetry/sdk-metrics-base';
import { RuntimeSpec, metricAttrFromUserAccount } from '../metrics';
import { BaseBotConfig } from '../config';

type Action = {
	baseAssetAmount: BN;
	marketIndex: number;
	direction: PositionDirection;
	price: BN;
	node: DLOBNode;
};

// State enum
enum StateType {
	/** Flat there is no open position */
	NEUTRAL = 'neutral',

	/** Long position on this market */
	LONG = 'long',

	/** Short position on market */
	SHORT = 'short',

	/** Current closing a long position (shorts only) */
	CLOSING_LONG = 'closing-long',

	/** Current closing a short position (long only) */
	CLOSING_SHORT = 'closing-short',
}

type State = {
	stateType: Map<number, StateType>;
	spotMarketPosition: Map<number, SpotPosition>;
	perpMarketPosition: Map<number, PerpPosition>;
};

enum METRIC_TYPES {
	sdk_call_duration_histogram = 'sdk_call_duration_histogram',
	try_jit_duration_histogram = 'try_jit_duration_histogram',
	runtime_specs = 'runtime_specs',
	mutex_busy = 'mutex_busy',
	errors = 'errors',
}

/**
 *
 * This bot is responsible for placing small trades during an order's JIT auction
 * in order to partially fill orders and collect maker fees. The bot also tracks
 * its position on all available markets in order to limit the size of open positions.
 *
 */
export class JitMakerBot implements Bot {
	public readonly name: string;
	public readonly dryRun: boolean;
	public readonly defaultIntervalMs: number = 1000;

	private driftClient: DriftClient;
	private eventSubscriber: EventSubscriber;
	private slotSubscriber: SlotSubscriber;
	private dlobSubscriber: DLOBSubscriber;
	private periodicTaskMutex = new Mutex();
	private userMap: UserMap;
	private userStatsMap: UserStatsMap;
	private orderLastSeenBaseAmount: Map<string, BN> = new Map(); // need some way to trim this down over time

	private intervalIds: Array<NodeJS.Timer> = [];

	private agentState: State;

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
	private tryJitDurationHistogram: Histogram;

	private watchdogTimerMutex = new Mutex();
	private watchdogTimerLastPatTime = Date.now();

	/**
	 * Set true to enforce max position size
	 */
	private RESTRICT_POSITION_SIZE = false;

	/**
	 * if a position's notional value passes this percentage of account
	 * collateral, the position enters a CLOSING_* state.
	 */
	private MAX_POSITION_EXPOSURE = 0.1;

	/**
	 * The max amount of quote to spend on each order.
	 */
	private MAX_TRADE_SIZE_QUOTE = 1000;

	constructor(
		driftClient: DriftClient,
		eventSubscriber: EventSubscriber,
		slotSubscriber: SlotSubscriber,
		runtimeSpec: RuntimeSpec,
		config: BaseBotConfig
	) {
		this.name = config.botId;
		this.dryRun = config.dryRun;
		this.driftClient = driftClient;
		this.eventSubscriber = eventSubscriber;
		this.slotSubscriber = slotSubscriber;
		this.runtimeSpec = runtimeSpec;

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
					instrumentName: METRIC_TYPES.try_jit_duration_histogram,
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
		this.tryJitDurationHistogram = this.meter.createHistogram(
			METRIC_TYPES.try_jit_duration_histogram,
			{
				description: 'Distribution of tryTrigger',
				unit: 'ms',
			}
		);
	}

	public async init() {
		logger.info(`${this.name} initing`);
		const initPromises: Array<Promise<any>> = [];

		this.userMap = new UserMap(
			this.driftClient,
			this.driftClient.userAccountSubscriptionConfig,
			false
		);
		initPromises.push(this.userMap.sync());

		this.userStatsMap = new UserStatsMap(
			this.driftClient,
			this.driftClient.userAccountSubscriptionConfig
		);
		initPromises.push(this.userStatsMap.sync());

		this.dlobSubscriber = new DLOBSubscriber({
			dlobSource: this.userMap,
			slotSource: this.slotSubscriber,
			updateFrequency: this.defaultIntervalMs - 500,
			driftClient: this.driftClient,
		});
		await this.dlobSubscriber.subscribe();

		this.agentState = {
			stateType: new Map<number, StateType>(),
			spotMarketPosition: new Map<number, SpotPosition>(),
			perpMarketPosition: new Map<number, PerpPosition>(),
		};
		initPromises.push(this.updateAgentState());

		await Promise.all(initPromises);
	}

	public async reset() {
		for (const intervalId of this.intervalIds) {
			clearInterval(intervalId);
		}
		this.intervalIds = [];

		this.eventSubscriber.eventEmitter.removeAllListeners('newEvent');

		await this.dlobSubscriber.unsubscribe();
		await this.userStatsMap.unsubscribe();
		await this.userMap.unsubscribe();
	}

	public async startIntervalLoop(intervalMs: number) {
		await this.tryMake();
		const intervalId = setInterval(this.tryMake.bind(this), intervalMs);
		this.intervalIds.push(intervalId);

		this.eventSubscriber.eventEmitter.on(
			'newEvent',
			async (record: WrappedEvent<any>) => {
				await this.userMap.updateWithEventRecord(record);
				await this.userStatsMap.updateWithEventRecord(record, this.userMap);
			}
		);

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
	 * This function creates a distribution of the values in array based on the
	 * weights array. The returned array should be used in randomIndex to make
	 * a random draw from the distribution.
	 *
	 */
	private createDistribution(
		array: Array<any>,
		weights: Array<number>,
		size: number
	): Array<number> {
		const distribution = [];
		const sum = weights.reduce((a: number, b: number) => a + b);
		const quant = size / sum;
		for (let i = 0; i < array.length; ++i) {
			const limit = quant * weights[i];
			for (let j = 0; j < limit; ++j) {
				distribution.push(i);
			}
		}
		return distribution;
	}

	/**
	 * Make a random choice from distribution
	 * @param distribution array of values that can be drawn from
	 * @returns
	 */
	private randomIndex(distribution: Array<number>): number {
		const index = Math.floor(distribution.length * Math.random()); // random index
		return distribution[index];
	}

	/**
	 * Generates a random number between [min, max]
	 * @param min minimum value to generate random number from
	 * @param max maximum value to generate random number from
	 * @returns the random number
	 */
	private randomIntFromInterval(min: number, max: number): number {
		return Math.floor(Math.random() * (max - min + 1) + min);
	}

	/**
	 * Updates the agent state based on its current market positions.
	 *
	 * Our goal is to participate in JIT auctions while limiting the delta
	 * exposure of the bot.
	 *
	 * We achieve this by allowing deltas to increase until MAX_POSITION_EXPOSURE
	 * is hit, after which orders will only reduce risk until the position is
	 * closed.
	 *
	 * @returns {Promise<void>}
	 */
	private async updateAgentState(): Promise<void> {
		// TODO: SPOT
		for await (const p of this.driftClient.getUserAccount().perpPositions) {
			if (p.baseAssetAmount.isZero()) {
				continue;
			}

			// update current position based on market position
			this.agentState.perpMarketPosition.set(p.marketIndex, p);

			// update state
			let currentState = this.agentState.stateType.get(p.marketIndex);
			if (!currentState) {
				this.agentState.stateType.set(p.marketIndex, StateType.NEUTRAL);
				currentState = StateType.NEUTRAL;
			}

			let canUpdateStateBasedOnPosition = true;
			if (
				(currentState === StateType.CLOSING_LONG &&
					p.baseAssetAmount.gt(new BN(0))) ||
				(currentState === StateType.CLOSING_SHORT &&
					p.baseAssetAmount.lt(new BN(0)))
			) {
				canUpdateStateBasedOnPosition = false;
			}

			if (canUpdateStateBasedOnPosition) {
				// check if need to enter a closing state
				const accountCollateral = convertToNumber(
					this.driftClient.getUser().getTotalCollateral(),
					QUOTE_PRECISION
				);
				const positionValue = convertToNumber(
					p.quoteAssetAmount,
					QUOTE_PRECISION
				);
				const exposure = positionValue / accountCollateral;

				if (exposure >= this.MAX_POSITION_EXPOSURE) {
					// state becomes closing only
					if (p.baseAssetAmount.gt(new BN(0))) {
						this.agentState.stateType.set(
							p.marketIndex,
							StateType.CLOSING_LONG
						);
					} else {
						this.agentState.stateType.set(
							p.marketIndex,
							StateType.CLOSING_SHORT
						);
					}
				} else {
					// update state to be whatever our current position is
					if (p.baseAssetAmount.gt(new BN(0))) {
						this.agentState.stateType.set(p.marketIndex, StateType.LONG);
					} else if (p.baseAssetAmount.lt(new BN(0))) {
						this.agentState.stateType.set(p.marketIndex, StateType.SHORT);
					} else {
						this.agentState.stateType.set(p.marketIndex, StateType.NEUTRAL);
					}
				}
			}
		}
	}

	private nodeCanBeFilled(
		node: DLOBNode,
		userAccountPublicKey: PublicKey
	): boolean {
		if (node.haveFilled) {
			logger.error(
				`already made the JIT auction for ${node.userAccount} - ${node.order.orderId}`
			);
			return false;
		}

		// jitter can't fill its own orders
		if (node.userAccount.equals(userAccountPublicKey)) {
			return false;
		}

		const orderSignature = getOrderSignature(
			node.order.orderId,
			node.userAccount
		);
		const lastBaseAmountFilledSeen =
			this.orderLastSeenBaseAmount.get(orderSignature);
		if (lastBaseAmountFilledSeen?.eq(node.order.baseAssetAmountFilled)) {
			return false;
		}

		return true;
	}

	/**
	 *
	 */
	private determineJitAuctionBaseFillAmount(
		orderBaseAmountAvailable: BN,
		orderPrice: BN
	): BN {
		const priceNumber = convertToNumber(orderPrice, PRICE_PRECISION);
		const worstCaseQuoteSpend = orderBaseAmountAvailable
			.mul(orderPrice)
			.div(BASE_PRECISION.mul(PRICE_PRECISION))
			.mul(QUOTE_PRECISION);

		const minOrderQuote = 20;
		let orderQuote = minOrderQuote;
		let maxOrderQuote = convertToNumber(worstCaseQuoteSpend, QUOTE_PRECISION);

		if (maxOrderQuote > this.MAX_TRADE_SIZE_QUOTE) {
			maxOrderQuote = this.MAX_TRADE_SIZE_QUOTE;
		}

		if (maxOrderQuote >= minOrderQuote) {
			orderQuote = this.randomIntFromInterval(minOrderQuote, maxOrderQuote);
		}

		const baseFillAmountNumber = orderQuote / priceNumber;
		let baseFillAmountBN = new BN(
			baseFillAmountNumber * BASE_PRECISION.toNumber()
		);
		if (baseFillAmountBN.gt(orderBaseAmountAvailable)) {
			baseFillAmountBN = orderBaseAmountAvailable;
		}
		logger.info(
			`jitMaker will fill base amount: ${convertToNumber(
				baseFillAmountBN,
				BASE_PRECISION
			).toString()} of remaining order ${convertToNumber(
				orderBaseAmountAvailable,
				BASE_PRECISION
			)}.`
		);

		return baseFillAmountBN;
	}

	/**
	 * Draws an action based on the current state of the bot.
	 *
	 */
	private async drawAndExecuteAction(market: PerpMarketAccount, dlob: DLOB) {
		// get nodes available to fill in the jit auction
		const nodesToFill = dlob.findJitAuctionNodesToFill(
			market.marketIndex,
			this.slotSubscriber.getSlot(),
			this.driftClient.getOracleDataForPerpMarket(market.marketIndex),
			MarketType.PERP
		);

		for (const nodeToFill of nodesToFill) {
			if (
				!this.nodeCanBeFilled(
					nodeToFill.node,
					await this.driftClient.getUserAccountPublicKey()
				)
			) {
				continue;
			}

			logger.info(
				`node slot: ${
					nodeToFill.node.order.slot
				}, cur slot: ${this.slotSubscriber.getSlot()}`
			);
			this.orderLastSeenBaseAmount.set(
				getOrderSignature(
					nodeToFill.node.order.orderId,
					nodeToFill.node.userAccount
				),
				nodeToFill.node.order.baseAssetAmountFilled
			);

			logger.info(
				`${
					this.name
				} quoting order for node: ${nodeToFill.node.userAccount.toBase58()} - ${nodeToFill.node.order.orderId.toString()}, orderBaseFilled: ${convertToNumber(
					nodeToFill.node.order.baseAssetAmountFilled,
					BASE_PRECISION
				)}/${convertToNumber(
					nodeToFill.node.order.baseAssetAmount,
					BASE_PRECISION
				)}`
			);

			// calculate jit maker order params
			const orderMarketIdx = nodeToFill.node.order.marketIndex;
			const orderDirection = nodeToFill.node.order.direction;
			const jitMakerDirection = isVariant(orderDirection, 'long')
				? PositionDirection.SHORT
				: PositionDirection.LONG;

			// prevent potential jit makers from shooting themselves in the foot, fill closer to end price which
			// is more favorable to the jit maker
			const jitMakerPrice = nodeToFill.node.order.auctionStartPrice
				.add(nodeToFill.node.order.auctionEndPrice)
				.div(new BN(2));

			const jitMakerBaseAssetAmount = this.determineJitAuctionBaseFillAmount(
				nodeToFill.node.order.baseAssetAmount.sub(
					nodeToFill.node.order.baseAssetAmountFilled
				),
				jitMakerPrice
			);

			const orderSlot = nodeToFill.node.order.slot.toNumber();
			const currSlot = this.slotSubscriber.getSlot();
			const aucDur = nodeToFill.node.order.auctionDuration;
			const aucEnd = orderSlot + aucDur;

			logger.info(
				`${
					this.name
				} propose to fill jit auction on market ${orderMarketIdx}: ${JSON.stringify(
					jitMakerDirection
				)}: ${convertToNumber(jitMakerBaseAssetAmount, BASE_PRECISION).toFixed(
					4
				)}, limit price: ${convertToNumber(
					jitMakerPrice,
					PRICE_PRECISION
				).toFixed(4)}, it has been ${
					currSlot - orderSlot
				} slots since order, auction ends in ${aucEnd - currSlot} slots`
			);

			try {
				const txSig = await this.executeAction({
					baseAssetAmount: jitMakerBaseAssetAmount,
					marketIndex: nodeToFill.node.order.marketIndex,
					direction: jitMakerDirection,
					price: jitMakerPrice,
					node: nodeToFill.node,
				});

				logger.info(
					`${
						this.name
					}: JIT auction filled (account: ${nodeToFill.node.userAccount.toString()} - ${nodeToFill.node.order.orderId.toString()}), Tx: ${txSig}`
				);
				return txSig;
			} catch (error) {
				nodeToFill.node.haveFilled = false;

				// If we get an error that order does not exist, assume its been filled by somebody else and we
				// have received the history record yet
				// TODO this might not hold if events arrive out of order
				const errorCode = getErrorCode(error);
				this.errorCounter.add(1, { errorCode: errorCode.toString() });

				logger.error(
					`Error (${errorCode}) filling JIT auction (account: ${nodeToFill.node.userAccount.toString()} - ${nodeToFill.node.order.orderId.toString()})`
				);

				/* todo remove this, fix error handling
				if (errorCode === 6042) {
					this.dlob.remove(
						nodeToFill.node.order,
						nodeToFill.node.userAccount,
						() => {
							logger.error(
								`Order ${nodeToFill.node.order.orderId.toString()} not found when trying to fill. Removing from order list`
							);
						}
					);
				}
				*/

				// console.error(error);
			}
		}
	}

	private async executeAction(action: Action): Promise<TransactionSignature> {
		const currentState = this.agentState.stateType.get(action.marketIndex);

		if (this.RESTRICT_POSITION_SIZE) {
			if (
				currentState === StateType.CLOSING_LONG &&
				action.direction === PositionDirection.LONG
			) {
				logger.info(
					`${this.name}: Skipping long action on market ${action.marketIndex}, since currently CLOSING_LONG`
				);
				return;
			}
			if (
				currentState === StateType.CLOSING_SHORT &&
				action.direction === PositionDirection.SHORT
			) {
				logger.info(
					`${this.name}: Skipping short action on market ${action.marketIndex}, since currently CLOSING_SHORT`
				);
				return;
			}
		}

		const takerUserAccount = (
			await this.userMap.mustGet(action.node.userAccount.toString())
		).getUserAccount();
		const takerAuthority = takerUserAccount.authority;

		const takerUserStats = await this.userStatsMap.mustGet(
			takerAuthority.toString()
		);
		const takerUserStatsPublicKey = takerUserStats.userStatsAccountPublicKey;
		const referrerInfo = takerUserStats.getReferrerInfo();

		return await this.driftClient.placeAndMakePerpOrder(
			{
				orderType: OrderType.LIMIT,
				marketIndex: action.marketIndex,
				baseAssetAmount: action.baseAssetAmount,
				direction: action.direction,
				price: action.price,
				postOnly: PostOnlyParams.TRY_POST_ONLY,
				immediateOrCancel: true,
			},
			{
				taker: action.node.userAccount,
				order: action.node.order,
				takerStats: takerUserStatsPublicKey,
				takerUserAccount: takerUserAccount,
			},
			referrerInfo
		);
	}

	private async tryMakeJitAuctionForMarket(
		market: PerpMarketAccount,
		dlob: DLOB
	) {
		await this.updateAgentState();
		await this.drawAndExecuteAction(market, dlob);
	}

	private async tryMake() {
		const start = Date.now();
		let ran = false;
		try {
			await tryAcquire(this.periodicTaskMutex).runExclusive(async () => {
				const dlob = this.dlobSubscriber.getDLOB();
				if (!dlob) {
					logger.info(`${this.name}: DLOB not initialized yet`);
					return;
				}

				await Promise.all(
					// TODO: spot
					this.driftClient.getPerpMarketAccounts().map((marketAccount) => {
						this.tryMakeJitAuctionForMarket(marketAccount, dlob);
					})
				);

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
			} else {
				throw e;
			}
		} finally {
			if (ran) {
				const duration = Date.now() - start;
				const user = this.driftClient.getUser();
				this.tryJitDurationHistogram.record(
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

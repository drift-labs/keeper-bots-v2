import {
	calculateAskPrice,
	calculateBidPrice,
	BN,
	isVariant,
	ClearingHouse,
	PerpMarketAccount,
	SlotSubscriber,
	PositionDirection,
	OrderType,
	BASE_PRECISION,
	convertToNumber,
	PRICE_PRECISION,
	Order,
	PerpPosition,
	PerpMarkets,
} from '@drift-labs/sdk';
import { Mutex, tryAcquire, E_ALREADY_LOCKED } from 'async-mutex';

import { logger } from '../logger';
import { Bot } from '../types';
import { Metrics } from '../metrics';

type State = {
	marketPosition: Map<number, PerpPosition>;
	openOrders: Map<number, Array<Order>>;
};

const MARKET_UPDATE_COOLDOWN_SLOTS = 30; // wait slots before updating market position
const driftEnv = process.env.DRIFT_ENV || 'devnet';

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
	public readonly defaultIntervalMs: number = 5000;

	private clearingHouse: ClearingHouse;
	private slotSubscriber: SlotSubscriber;
	private periodicTaskMutex = new Mutex();
	private lastSlotMarketUpdated: Map<number, number> = new Map();

	private intervalIds: Array<NodeJS.Timer> = [];
	private metrics: Metrics | undefined;

	private agentState: State;

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

	private watchdogTimerMutex = new Mutex();
	private watchdogTimerLastPatTime = Date.now();

	constructor(
		name: string,
		dryRun: boolean,
		clearingHouse: ClearingHouse,
		slotSubscriber: SlotSubscriber,
		metrics?: Metrics | undefined
	) {
		this.name = name;
		this.dryRun = dryRun;
		this.clearingHouse = clearingHouse;
		this.slotSubscriber = slotSubscriber;
		this.metrics = metrics;
	}

	public async init() {
		logger.info(`${this.name} initing`);
		this.agentState = {
			marketPosition: new Map<number, PerpPosition>(),
			openOrders: new Map<number, Array<Order>>(),
		};
		this.updateAgentState();
	}

	public async reset() {
		for (const intervalId of this.intervalIds) {
			clearInterval(intervalId);
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

	public async trigger(_record: any): Promise<void> {}

	public viewDlob(): undefined {
		return undefined;
	}

	/**
	 * Updates the agent state based on its current market positions.
	 *
	 * We want to maintain a two-sided market while being conscious of the positions
	 * taken on by the account.
	 *
	 * As open positions approach MAX_POSITION_EXPOSURE, limit orders are skewed such
	 * that the position that decreases risk will be closer to the oracle price, and the
	 * position that increases risk will be further from the oracle price.
	 *
	 * @returns {Promise<void>}
	 */
	private updateAgentState(): void {
		this.clearingHouse.getUserAccount().perpPositions.map((p) => {
			if (p.baseAssetAmount.isZero()) {
				return;
			}
			this.agentState.marketPosition.set(p.marketIndex, p);
		});

		// zeor out the open orders
		for (const market of PerpMarkets[driftEnv]) {
			this.agentState.openOrders.set(market.marketIndex.toNumber(), []);
		}

		this.clearingHouse.getUserAccount().orders.map((o) => {
			if (isVariant(o.status, 'init')) {
				return;
			}
			const marketIndex = o.marketIndex;
			this.agentState.openOrders.set(marketIndex, [
				...this.agentState.openOrders.get(marketIndex),
				o,
			]);
		});
	}

	private async updateOpenOrdersForMarket(marketAccount: PerpMarketAccount) {
		const currSlot = this.slotSubscriber.currentSlot;
		const marketIndex = marketAccount.marketIndex;
		const nextUpdateSlot =
			this.lastSlotMarketUpdated.get(marketIndex) +
			MARKET_UPDATE_COOLDOWN_SLOTS;

		if (nextUpdateSlot > currSlot) {
			return;
		}

		const openOrders = this.agentState.openOrders.get(marketIndex);
		const oracle = this.clearingHouse.getOracleDataForMarket(marketIndex);
		const vAsk = calculateAskPrice(marketAccount, oracle);
		const vBid = calculateBidPrice(marketAccount, oracle);

		console.log(`mkt: ${marketAccount.marketIndex} open orders:`);
		for (const [idx, o] of openOrders.entries()) {
			console.log(
				`${Object.keys(o.orderType)[0]} ${Object.keys(o.direction)[0]}`
			);
			console.log(
				`[${idx}]: baa: ${convertToNumber(
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
					o.oraclePriceOffset,
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

		if (
			(openOrders.length > 0 && openOrders.length != 2) ||
			marketIndex === 0
		) {
			// cancel orders
			for (const o of openOrders) {
				const tx = await this.clearingHouse.cancelOrder(o.orderId);
				console.log(
					`${this.name} cancelling order ${this.clearingHouse
						.getUserAccount()
						.authority.toBase58()}-${o.orderId}: ${tx}`
				);
			}
			placeNewOrders = true;
		}

		if (placeNewOrders) {
			const biasNum = new BN(90);
			const biasDenom = new BN(100);

			const oracleBidSpread = oracle.price.sub(vBid);
			const tx0 = await this.clearingHouse.placeOrder({
				marketIndex: marketIndex,
				orderType: OrderType.LIMIT,
				direction: PositionDirection.LONG,
				baseAssetAmount: BASE_PRECISION.mul(new BN(100)),
				oraclePriceOffset: oracleBidSpread.mul(biasNum).div(biasDenom).neg(), // limit bid below oracle
			});
			console.log(`${this.name} placing long: ${tx0}`);

			const oracleAskSpread = vAsk.sub(oracle.price);
			const tx1 = await this.clearingHouse.placeOrder({
				marketIndex: marketIndex,
				orderType: OrderType.LIMIT,
				direction: PositionDirection.SHORT,
				baseAssetAmount: BASE_PRECISION.mul(new BN(100)),
				oraclePriceOffset: oracleAskSpread.mul(biasNum).div(biasDenom), // limit ask above oracle
			});
			console.log(`${this.name} placing short: ${tx1}`);
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
					this.clearingHouse.getPerpMarketAccounts().map((marketAccount) => {
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
				this.metrics?.recordMutexBusy(this.name);
			} else {
				throw e;
			}
		} finally {
			if (ran) {
				const duration = Date.now() - start;
				this.metrics?.recordRpcDuration(
					this.clearingHouse.connection.rpcEndpoint,
					'updateOpenOrders',
					duration,
					false,
					this.name
				);
				logger.debug(`${this.name} Bot took ${Date.now() - start}ms to run`);

				await this.watchdogTimerMutex.runExclusive(async () => {
					this.watchdogTimerLastPatTime = Date.now();
				});
			}
		}
	}
}

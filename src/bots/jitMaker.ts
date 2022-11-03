/**
 * Improvements
 *
 * - [ ] vault
 * - [x] monitoring
 * - [ ] add in full grafana dashboards and datasources from load
 * - [ ] fixed random distribution for choosing bid amount
 * - [ ] ability to re bid a specific auction
 * - [ ] ability to update variables without needing to restart the bot
 * - [ ] ability to hedge on spot market and additional markets
 */

import {
	BN,
	isVariant,
	DriftClient,
	PerpMarketAccount,
	SlotSubscriber,
	PositionDirection,
	OrderType,
	OrderRecord,
	NewUserRecord,
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
} from '@drift-labs/sdk';
import { Mutex, tryAcquire, withTimeout, E_ALREADY_LOCKED } from 'async-mutex';

import { TransactionSignature, PublicKey } from '@solana/web3.js';

import { getErrorCode } from '../error';
import { logger } from '../logger';
import { Bot } from '../types';
import { Metrics } from '../metrics';

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

const dlobMutexError = new Error('dlobMutex timeout');

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

	private clearingHouse: DriftClient;
	private slotSubscriber: SlotSubscriber;
	private dlobMutex = withTimeout(
		new Mutex(),
		10 * this.defaultIntervalMs,
		dlobMutexError
	);
	private dlob: DLOB;
	private periodicTaskMutex = new Mutex();
	private userMap: UserMap;
	private userStatsMap: UserStatsMap;
	private orderLastSeenBaseAmount: Map<string, BN> = new Map(); // need some way to trim this down over time

	private intervalIds: Array<NodeJS.Timer> = [];
	private metrics: Metrics | undefined;

	private agentState: State;

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
		name: string,
		dryRun: boolean,
		clearingHouse: DriftClient,
		slotSubscriber: SlotSubscriber,
		metrics?: Metrics | undefined
	) {
		this.name = name;
		this.dryRun = dryRun;
		this.clearingHouse = clearingHouse;
		this.slotSubscriber = slotSubscriber;
		this.metrics = metrics;
	}

	/**
	 * Initializes the bot's state.
	 * - user map
	 * - user stats map
	 * - dlob
	 * - agent state
	 */
	public async init() {
		logger.info(`${this.name} initializing...`);

		// creating an array of promises as part of initialization
		const initPromises: Array<Promise<any>> = [];

		// creating a map of users in the clearing house
		this.userMap = new UserMap(
			this.clearingHouse,
			this.clearingHouse.userAccountSubscriptionConfig
		);
		initPromises.push(this.userMap.fetchAllUsers());

		// creating a map of user stats in the clearing house
		this.userStatsMap = new UserStatsMap(
			this.clearingHouse,
			this.clearingHouse.userAccountSubscriptionConfig
		);
		initPromises.push(this.userStatsMap.fetchAllUserStats());

		// creating a new DLOB instance composed of the perp and spot market accounts
		this.dlob = new DLOB(
			this.clearingHouse.getPerpMarketAccounts(),
			this.clearingHouse.getSpotMarketAccounts(),
			this.clearingHouse.getStateAccount(),
			this.userMap,
			true
		);
		initPromises.push(this.dlob.init());

		// creating a new state object for the perp and spot markets
		this.agentState = {
			stateType: new Map<number, StateType>(),
			spotMarketPosition: new Map<number, SpotPosition>(),
			perpMarketPosition: new Map<number, PerpPosition>(),
		};
		initPromises.push(this.updateAgentState());

		// waiting for all initializing promises to resolve
		await Promise.all(initPromises); // TODO: add in some catching here to handle initialization errors
	}

	/**
	 * Resets the bot's state
	 * - deletes the user map, user stats map, and dlob
	 */
	public async reset() {
		for (const intervalId of this.intervalIds) {
			clearInterval(intervalId);
		}
		this.intervalIds = [];
		if (this.dlob) {
			this.dlob.clear();
			delete this.dlob;
		}
		delete this.userMap;
		delete this.userStatsMap;
	}

	/**
	 * Starts the bot's periodic tasks. The bot will periodically
	 * call `tryMake` and attempt to fill orders via the JIT auction
	 * @param intervalMs the interval in milliseconds to call `tryMake`
	 */
	public async startIntervalLoop(intervalMs: number) {
		await this.tryMake();
		const intervalId = setInterval(this.tryMake.bind(this), intervalMs);
		this.intervalIds.push(intervalId);

		logger.info(`${this.name} Bot started!`);
	}

	/**
	 * A simple health check on the state of the bot
	 * @returns the bot's current state
	 */
	public async healthCheck(): Promise<boolean> {
		let healthy = false;
		await this.watchdogTimerMutex.runExclusive(async () => {
			healthy =
				this.watchdogTimerLastPatTime > Date.now() - 2 * this.defaultIntervalMs;
		});
		return healthy;
	}

	/**
	 * Takes an order or new user record and updates the bot's state
	 * @param record order or user record to update the bot's state with
	 */
	public async trigger(record: any): Promise<void> {
		if (record.eventType === 'OrderRecord') {
			await this.userMap.updateWithOrderRecord(record as OrderRecord);
			await this.userStatsMap.updateWithOrderRecord(
				record as OrderRecord,
				this.userMap
			);
			await this.tryMake();
		} else if (record.eventType === 'NewUserRecord') {
			await this.userMap.mustGet((record as NewUserRecord).user.toString());
			await this.userStatsMap.mustGet(
				(record as NewUserRecord).user.toString()
			);
		}
	}

	/**
	 * Getter for the DLOB
	 * @returns the current DLOB
	 */
	public viewDlob(): DLOB {
		return this.dlob;
	}

	// TODO: understand this function more, why is a random choice being made?
	/**
	 * This function creates a distribution of the values in array based on the
	 * weights array. The returned array should be used in randomIndex to make
	 * a random draw from the distribution.
	 * @param array the array to create a distribution from
	 * @param weights the weights to use for the distribution
	 * @param size the size of the distribution
	 * @returns the distribution
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
	 * @returns a random value from the distribution
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
	 * Initially this was achieved by allowing deltas to increase until MAX_POSITION_EXPOSURE
	 * is hit, after which orders will only reduce risk until the position is
	 * closed.
	 *
	 * This is a problem because it limits the scalability of the bot.
	 * Delta exposure can be hedged on an additional exchange or if the
	 * exposure if + the bot can hedge via the spot market.
	 *
	 * // TODO: update the spot market and add it in to hedge the delta positions
	 *
	 * @returns {Promise<void>}
	 */
	private async updateAgentState(): Promise<void> {
		// update spot markets
		for await (const p of this.clearingHouse.getUserAccount().spotPositions) {
			// if position base amount is zero then we don't have a position
			if (p.scaledBalance.isZero()) {
				continue;
			}

			// update current position based on market position
			this.agentState.spotMarketPosition.set(p.marketIndex, p);

			// update state
			let currentState = this.agentState.stateType.get(p.marketIndex);
			if (!currentState) {
				this.agentState.stateType.set(p.marketIndex, StateType.NEUTRAL);
				currentState = StateType.NEUTRAL;
			}

			// TODO: Hedge the delta position logic
		}

		// update perp markets
		for await (const p of this.clearingHouse.getUserAccount().perpPositions) {
			// if position base amount is zero then we don't have a position
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

			// check if total position is greater than MAX_POSITION_EXPOSURE
			// TODO: keep this, but expand it out to first logging delta exposure and second hedging it
			if (canUpdateStateBasedOnPosition) {
				// check if need to enter a closing state
				const accountCollateral = convertToNumber(
					this.clearingHouse.getUser().getTotalCollateral(),
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

	/**
	 * Determines if an order in the DLOB (node in the tree) can be filled
	 *
	 * @param node an order in the DLOB tree
	 * @param userAccountPublicKey public key of the user account
	 * @returns if the node can be filled
	 */
	private nodeCanBeFilled(
		node: DLOBNode,
		userAccountPublicKey: PublicKey
	): boolean {
		// check if the order has already been filled/made
		if (node.haveFilled) {
			logger.error(
				`order has already made the JIT auction for ${node.userAccount} OrderID: ${node.order.orderId}`
			);
			return false;
		}

		// jitter can't fill its own orders
		if (node.userAccount.equals(userAccountPublicKey)) {
			return false;
		}

		// get the order signature from the node
		const orderSignature = getOrderSignature(
			node.order.orderId,
			node.userAccount
		);

		// get the last seen base amount given an order signature
		// if orderLastSeenBaseAmount can be trimmed down this should be faster
		const lastBaseAmountFilledSeen =
			this.orderLastSeenBaseAmount.get(orderSignature);

		// if last seen amount is the same as the current amount then we have already filled this order
		if (lastBaseAmountFilledSeen?.eq(node.order.baseAssetAmountFilled)) {
			return false;
		}

		// return that the order is ready and able to be filled
		return true;
	}

	/**
	 * Determine the base amount to fill for a given order
	 *
	 * @param orderBaseAmountAvailable the base amount available to fill
	 * @param orderPrice the price of the order
	 * @returns the base amount to fill as a BN
	 */
	private determineJitAuctionBaseFillAmount(
		orderBaseAmountAvailable: BN,
		orderPrice: BN
	): BN {
		// convert the order base amount available to a number with the correct precision
		// TODO: what is this precision?
		const priceNumber = convertToNumber(orderPrice, PRICE_PRECISION);

		// determine the worst case scenario for the base amount to fill
		// defined as the amount * orderPrice
		const worstCaseQuoteSpend = orderBaseAmountAvailable
			.mul(orderPrice)
			.div(BASE_PRECISION.mul(PRICE_PRECISION))
			.mul(QUOTE_PRECISION);

		// defining the minimum order quote
		// TODO: this should be configurable
		const minOrderQuote = 20;
		let orderQuote = minOrderQuote;
		// max quote is defined as the worst order possible
		let maxOrderQuote = convertToNumber(worstCaseQuoteSpend, QUOTE_PRECISION);

		// checking if the max quote exceeds the max trading size defined above
		if (maxOrderQuote > this.MAX_TRADE_SIZE_QUOTE) {
			maxOrderQuote = this.MAX_TRADE_SIZE_QUOTE;
		}

		// if max if greater than the minimum quote that would be offered
		// define the quote as a random number between the min and max
		// TODO: CHANGE THIS!
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

		logger.info!(
			`Oracle Price / Worst Quote: ${maxOrderQuote}\n
			Best Quote: ${orderQuote}\n
			Order Size: ${orderBaseAmountAvailable}`
		);

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
	 * Determines all markets that can be filled, their pricing
	 * and then trys to fill then
	 *
	 * @param market the market to get the perp orderbook for
	 */
	private async drawAndExecuteAction(market: PerpMarketAccount) {
		// get nodes available to fill in the jit auction
		const nodesToFill = this.dlob.findJitAuctionNodesToFill(
			market.marketIndex,
			this.slotSubscriber.getSlot(),
			MarketType.PERP
		);

		// iterate over each node, determine if it can be filled, and fill it
		for (const nodeToFill of nodesToFill) {
			// check if the node can be filled
			if (
				!this.nodeCanBeFilled(
					nodeToFill.node,
					await this.clearingHouse.getUserAccountPublicKey()
				)
			) {
				continue;
			}

			logger.info(
				`node slot: ${
					nodeToFill.node.order.slot
				}, current slot: ${this.slotSubscriber.getSlot()}`
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
			const orderMarketIdx = nodeToFill.node.market.marketIndex;
			const orderDirection = nodeToFill.node.order.direction;
			const jitMakerDirection = isVariant(orderDirection, 'long')
				? PositionDirection.SHORT
				: PositionDirection.LONG;

			// determine the auction starting price
			// this defaults to the oracle price
			// https://docs.drift.trade/just-in-time-jit-auctions#INgHr
			const jitMakerPrice = nodeToFill.node.order.auctionStartPrice;

			// determine the base amount to fill and the quote amount given the starting price
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

			// try to execute the transaction to fill the order
			// TODO: separate this into a function that can be called separate so that we can retry specific orders
			try {
				const txSig = await this.executePerpOrder({
					baseAssetAmount: jitMakerBaseAssetAmount,
					marketIndex: nodeToFill.node.order.marketIndex,
					direction: jitMakerDirection,
					price: jitMakerPrice,
					node: nodeToFill.node,
				});

				// record the order being filled into the prometheus metrics
				this.metrics?.recordFilledOrder(
					this.clearingHouse.provider.wallet.publicKey,
					this.name
				);

				logger.info(
					`${
						this.name
					}: JIT auction filled (account: ${nodeToFill.node.userAccount.toString()} - ${nodeToFill.node.order.orderId.toString()}), Tx: ${txSig}`
				);

				return txSig;
			} catch (error) {
				// If we get an error that order does not exist, assume its been filled by somebody else and we
				// have not received the history record yet
				// but given events could have arrived out of order, we need to check the orderbook to see if the order is still there
				// let orderStatus = this.nodeCanBeFilled(
				// 	nodeToFill.node,
				// 	nodeToFill.node.userAccount
				// );

				// orderStatus is true if the order is still in the orderbook
				// and is still able to be filled, otherwise it has already
				// been filled or does not exist
				// if (orderStatus) {
				// 	this.executeOrder();
				// }

				// node was not able to be filled due to a transaction error
				nodeToFill.node.haveFilled = false;

				// record error event
				const errorCode = getErrorCode(error);
				this.metrics?.recordErrorCode(
					errorCode,
					this.clearingHouse.provider.wallet.publicKey,
					this.name
				);

				logger.error(
					`Error (${errorCode}) filling JIT auction (account: ${nodeToFill.node.userAccount.toString()} - ${nodeToFill.node.order.orderId.toString()})`
				);
			}
		}
	}

	/**
	 * Execute and place a `PerpOrder`
	 *
	 * @param action order to place and execute
	 * @returns a promise that resolves to the transaction signature
	 */
	private async executePerpOrder(
		action: Action
	): Promise<TransactionSignature> {
		const currentState = this.agentState.stateType.get(action.marketIndex);

		// checking if the action should be skipped due to the current state
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

		return await this.clearingHouse.placeAndMakePerpOrder(
			{
				orderType: OrderType.LIMIT,
				marketIndex: action.marketIndex,
				baseAssetAmount: action.baseAssetAmount,
				direction: action.direction,
				price: action.price,
				postOnly: true,
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

	/**
	 * Given a perp market, update its state and begin to fill orders
	 *
	 * @param market perp market account to fill with
	 */
	private async tryMakeJitAuctionForMarket(market: PerpMarketAccount) {
		await this.updateAgentState();
		await this.drawAndExecuteAction(market);
	}

	/**
	 * Make/Fill JIT Auctions
	 * - update all markets
	 * - begin to check, price, and fill oracles for each *perp* market
	 */
	private async tryMake() {
		const start = Date.now();
		let ran = false;
		try {
			await tryAcquire(this.periodicTaskMutex).runExclusive(async () => {
				await this.dlobMutex.runExclusive(async () => {
					if (this.dlob) {
						this.dlob.clear();
						delete this.dlob;
					}

					// creating a new DLOB instance and updating its state
					this.dlob = new DLOB(
						this.clearingHouse.getPerpMarketAccounts(),
						this.clearingHouse.getSpotMarketAccounts(),
						this.clearingHouse.getStateAccount(),
						this.userMap,
						true
					);
					await this.dlob.init();
				});

				// make the JIT auction
				await Promise.all(
					this.clearingHouse.getPerpMarketAccounts().map((marketAccount) => {
						this.tryMakeJitAuctionForMarket(marketAccount);
					})
				);

				ran = true;
			});
		} catch (e) {
			if (e === E_ALREADY_LOCKED) {
				this.metrics?.recordMutexBusy(this.name);
			} else if (e === dlobMutexError) {
				logger.error(`${this.name} dlobMutexError timeout`);
			} else {
				throw e;
			}
		} finally {
			if (ran) {
				const duration = Date.now() - start;
				this.metrics?.recordRpcDuration(
					this.clearingHouse.connection.rpcEndpoint,
					'tryMake',
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

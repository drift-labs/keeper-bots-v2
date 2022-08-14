import {
	BN,
	isVariant,
	ClearingHouse,
	MarketAccount,
	SlotSubscriber,
	PositionDirection,
	OrderType,
	OrderRecord,
	BASE_PRECISION,
	QUOTE_PRECISION,
	convertToNumber,
	MARK_PRICE_PRECISION,
	UserAccount,
	UserPosition,
} from '@drift-labs/sdk';

import { Connection, TransactionSignature, PublicKey } from '@solana/web3.js';

import { getErrorCode } from '../error';
import { logger } from '../logger';
import { DLOB } from '../dlob/DLOB';
import { DLOBNode } from '../dlob/DLOBNode';
// import { UserMap } from '../userMap';
import { Bot } from '../types';
import { Metrics } from '../metrics';

// enum ActionType {
// 	NOOP = 'noop',
// 	LONG = 'long',
// 	SHORT = 'short',
// }

type Action = {
	// type: ActionType;
	baseAssetAmount: BN;
	marketIndex: BN;
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
	marketPosition: Map<number, UserPosition>;
	account: UserAccount;
};

/**
 * This bot is responsible for placing small trades during an order's JIT auction
 * in order to partially fill orders. The bot also tracks its position on all
 * available markets.
 *
 * The bot is a simple (no reward) markov decision process on each market with the
 * following States:
 *
 * (1) neutral: the bot has no position in the market
 * (2) long: the bot has a long position
 * (3) short: the bot has a short position
 *
 * and in each state, the bot may perform the following Actions:
 * (1) noop: the bot does nothing
 * (2) buy: the bot places a buy order
 * (3) sell: the bot places a sell order
 *
 * On each DLOB record, the bot does the following:
 * 1) draw random Action based on its current state
 * 2) transitions state based on some state transition function
 *
 */
export class JitMakerBot implements Bot {
	public readonly name: string;
	public readonly dryRun: boolean;
	private clearingHouse: ClearingHouse;
	private slotSubscriber: SlotSubscriber;
	private dlob: DLOB;
	private perMarketMutexFills: Array<number> = []; // TODO: use real mutex
	private intervalIds: Array<NodeJS.Timer> = [];
	// private userMap: UserMap;
	private metrics: Metrics | undefined;

	private agentState: State;

	/**
	 * if a position's notional value passes this percentage of account
	 * collateral, the position enters a CLOSING_* state.
	 */
	private MAX_POSITION_EXPOSURE = 0.1;

	/**
	 * The max amount of quote to spend on each order.
	 */
	private MAX_TRADE_SIZE_QUOTE = 1000;

	/** Probability of a noop while in long or short states */
	private PROB_NOOP = 0.4;

	/** Probability of increasing risk after taking into account PROB_NOOP */
	private PROB_INCREASE_RISK = 0.8;

	constructor(
		name: string,
		dryRun: boolean,
		clearingHouse: ClearingHouse,
		slotSubscriber: SlotSubscriber,
		connection: Connection,
		metrics?: Metrics | undefined
	) {
		this.name = name;
		this.dryRun = dryRun;
		this.clearingHouse = clearingHouse;
		this.slotSubscriber = slotSubscriber;
		// this.connection = connection;
		this.metrics = metrics;
	}

	public async init() {
		// initialize DLOB instance
		this.dlob = new DLOB(this.clearingHouse.getMarketAccounts());
		const programAccounts = await this.clearingHouse.program.account.user.all();
		for (const programAccount of programAccounts) {
			// @ts-ignore
			const userAccount: UserAccount = programAccount.account;
			const userAccountPublicKey = programAccount.publicKey;

			for (const order of userAccount.orders) {
				this.dlob.insert(order, userAccountPublicKey);
			}
		}

		// initialize userMap instance
		// this.userMap = new UserMap(this.connection, this.clearingHouse);
		// await this.userMap.fetchAllUsers();

		this.agentState = {
			stateType: new Map<number, StateType>(),
			marketPosition: new Map<number, UserPosition>(),
			account: undefined,
		};

		await this.updateAgentState();
	}

	public reset(): void {
		for (const intervalId of this.intervalIds) {
			clearInterval(intervalId);
		}
		this.intervalIds = [];
		delete this.dlob;
		// delete this.userMap;
	}

	public async startIntervalLoop(_intervalMs: number) {
		// await this.tryMake();
		// const intervalId = setInterval(this.tryMake.bind(this), intervalMs);
		// this.intervalIds.push(intervalId);

		logger.info(`${this.name} Bot started!`);
	}

	public async trigger(record: OrderRecord): Promise<void> {
		this.dlob.applyOrderRecord(record);
		// await this.userMap.updateWithOrder(record);
		await this.tryMake();
	}

	public viewDlob(): DLOB {
		return this.dlob;
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
		await this.clearingHouse.fetchAccounts();
		await this.clearingHouse.getUser().fetchAccounts();

		this.agentState.account = this.clearingHouse.getUserAccount()!;

		for await (const p of this.clearingHouse.getUserAccount().positions) {
			if (p.baseAssetAmount.isZero()) {
				continue;
			}

			// update current position based on market position
			this.agentState.marketPosition.set(p.marketIndex.toNumber(), p);

			// update state
			let currentState = this.agentState.stateType.get(
				p.marketIndex.toNumber()
			);
			if (!currentState) {
				this.agentState.stateType.set(
					p.marketIndex.toNumber(),
					StateType.NEUTRAL
				);
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
							p.marketIndex.toNumber(),
							StateType.CLOSING_LONG
						);
					} else {
						this.agentState.stateType.set(
							p.marketIndex.toNumber(),
							StateType.CLOSING_SHORT
						);
					}
				} else {
					// update state to be whatever our current position is
					if (p.baseAssetAmount.gt(new BN(0))) {
						this.agentState.stateType.set(
							p.marketIndex.toNumber(),
							StateType.LONG
						);
					} else if (p.baseAssetAmount.lt(new BN(0))) {
						this.agentState.stateType.set(
							p.marketIndex.toNumber(),
							StateType.SHORT
						);
					} else {
						this.agentState.stateType.set(
							p.marketIndex.toNumber(),
							StateType.NEUTRAL
						);
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

		if (node.userAccount.equals(userAccountPublicKey)) {
			return false;
		}
		return true;
	}

	/**
	 *
	 */
	private determineJitAuctionBaseFillAmount(
		orderBaseAmount: BN,
		orderPrice: BN
	): BN {
		const priceNumber = convertToNumber(orderPrice, MARK_PRICE_PRECISION);
		const worstCaseQuoteSpend = orderBaseAmount
			.mul(orderPrice)
			.div(BASE_PRECISION.mul(MARK_PRICE_PRECISION))
			.mul(QUOTE_PRECISION);

		const minOrderQuote = 10;
		let maxOrderQuote = convertToNumber(worstCaseQuoteSpend, QUOTE_PRECISION);
		if (maxOrderQuote > this.MAX_TRADE_SIZE_QUOTE) {
			maxOrderQuote = this.MAX_TRADE_SIZE_QUOTE;
		}

		const orderQuote = this.randomIntFromInterval(minOrderQuote, maxOrderQuote);
		const jitAuctionBaseFillAmount = orderQuote / priceNumber;
		return new BN(jitAuctionBaseFillAmount * BASE_PRECISION.toNumber());
	}

	/**
	 * Draws an action based on the current state of the bot.
	 *
	 */
	private async drawAction(market: MarketAccount): Promise<Array<Action>> {
		const actions: Array<Action> = [];

		// get nodes available to fill in the jit auction
		const nodesToFill = this.dlob.findJitAuctionNodesToFill(
			market.marketIndex,
			this.slotSubscriber.getSlot()
		);

		for await (const nodeToFill of nodesToFill) {
			if (
				!this.nodeCanBeFilled(
					nodeToFill.node,
					await this.clearingHouse.getUserAccountPublicKey()
				)
			) {
				continue;
			}

			nodeToFill.node.haveFilled = true;

			logger.info(
				`${
					this.name
				} quoting order for node: ${nodeToFill.node.userAccount.toBase58()} - ${nodeToFill.node.order.orderId.toString()}`
			);

			// calculate jit maker order params
			const orderDirection = nodeToFill.node.order.direction;
			const jitMakerDirection = isVariant(orderDirection, 'long')
				? PositionDirection.SHORT
				: PositionDirection.LONG;

			const jitMakerPrice = nodeToFill.node.order.auctionStartPrice;

			const jitMakerBaseAssetAmount = this.determineJitAuctionBaseFillAmount(
				nodeToFill.node.order.baseAssetAmount.sub(
					nodeToFill.node.order.baseAssetAmountFilled
				),
				jitMakerPrice
			);

			const tsNow = new BN(new Date().getTime() / 1000);
			const orderTs = new BN(nodeToFill.node.order.ts);
			const aucDur = new BN(nodeToFill.node.order.auctionDuration);

			logger.info(
				`${this.name} propose to fill jit auction: ${JSON.stringify(
					jitMakerDirection
				)}: ${convertToNumber(jitMakerBaseAssetAmount, BASE_PRECISION).toFixed(
					4
				)}, limit price: ${convertToNumber(
					jitMakerPrice,
					MARK_PRICE_PRECISION
				).toFixed(4)}, it has been ${tsNow
					.sub(orderTs)
					.toNumber()}s since order placed, auction ends in ${orderTs
					.add(aucDur)
					.sub(tsNow)
					.toNumber()}s`
			);
			// const orderAucStart = nodeToFill.node.order.auctionStartPrice;
			// const orderAucEnd = nodeToFill.node.order.auctionEndPrice;
			// logger.info(
			// 	`${this.name}: original order aucStartPrice: ${convertToNumber(
			// 		orderAucStart,
			// 		MARK_PRICE_PRECISION
			// 	).toFixed(4)}, aucEndPrice: ${convertToNumber(
			// 		orderAucEnd,
			// 		MARK_PRICE_PRECISION
			// 	).toFixed(4)}`
			// );

			actions.push({
				baseAssetAmount: jitMakerBaseAssetAmount,
				marketIndex: nodeToFill.node.order.marketIndex,
				direction: jitMakerDirection,
				price: jitMakerPrice,
				node: nodeToFill.node,
			});
		}

		return actions;
	}

	private async executeActions(
		market: MarketAccount,
		actions: Array<Action>
	): Promise<TransactionSignature | undefined> {
		for await (const action of actions) {
			const currentState = this.agentState.stateType.get(
				market.marketIndex.toNumber()
			);

			if (
				currentState === StateType.CLOSING_LONG &&
				action.direction === PositionDirection.LONG
			) {
				logger.info(
					`${
						this.name
					}: Skipping long action on market ${market.marketIndex.toNumber()}, since currently CLOSING_LONG`
				);
				continue;
			}
			if (
				currentState === StateType.CLOSING_SHORT &&
				action.direction === PositionDirection.SHORT
			) {
				logger.info(
					`${
						this.name
					}: Skipping short action on market ${market.marketIndex.toNumber()}, since currently CLOSING_SHORT`
				);
				continue;
			}

			await this.clearingHouse
				.placeAndMake(
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
					}
				)
				.then((txSig) => {
					this.metrics?.recordFilledOrder(
						this.clearingHouse.provider.wallet.publicKey,
						this.name
					);
					logger.info(
						`${
							this.name
						}: JIT auction filled (account: ${action.node.userAccount.toString()} - ${action.node.order.orderId.toString()}), Tx: ${txSig}`
					);
				})
				.catch((error) => {
					action.node.haveFilled = false;
					// console.error(error);
					logger.error(
						`Error filling JIT auction (account: ${action.node.userAccount.toString()} - ${action.node.order.orderId.toString()})`
					);

					// If we get an error that order does not exist, assume its been filled by somebody else and we
					// have received the history record yet
					// TODO this might not hold if events arrive out of order
					const errorCode = getErrorCode(error);
					this.metrics?.recordErrorCode(
						errorCode,
						this.clearingHouse.provider.wallet.publicKey,
						this.name
					);

					if (errorCode === 6043) {
						this.dlob.remove(action.node.order, action.node.userAccount, () => {
							logger.error(
								`Order ${action.node.order.orderId.toString()} not found when trying to fill. Removing from order list`
							);
						});
					}
					logger.error(`Error code while JIT making order: ${errorCode}`);
					// console.error(error);
				});
		}

		return;
	}

	private async tryMakeJitAuctionForMarket(market: MarketAccount) {
		await this.updateAgentState();
		const actions = await this.drawAction(market);
		await this.executeActions(market, actions);
	}

	private async tryMake() {
		for (const marketAccount of this.clearingHouse.getMarketAccounts()) {
			const marketIndex = marketAccount.marketIndex;
			if (this.perMarketMutexFills[marketIndex.toNumber()] === 1) {
				continue;
			}

			this.perMarketMutexFills[marketIndex.toNumber()] = 1;
			this.tryMakeJitAuctionForMarket(marketAccount);
			this.perMarketMutexFills[marketIndex.toNumber()] = 0;
		}
	}
}

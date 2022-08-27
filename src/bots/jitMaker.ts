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

import { TransactionSignature, PublicKey } from '@solana/web3.js';

import { getErrorCode } from '../error';
import { logger } from '../logger';
import { DLOB } from '../dlob/DLOB';
import { DLOBNode } from '../dlob/DLOBNode';
import { UserMap } from '../userMap';
import { UserStatsMap } from '../userStatsMap';
import { Bot } from '../types';
import { Metrics } from '../metrics';

type Action = {
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

	private clearingHouse: ClearingHouse;
	private slotSubscriber: SlotSubscriber;
	private dlob: DLOB;
	private userMap: UserMap;
	private userStatsMap: UserStatsMap;

	private perMarketMutexFills = new Uint8Array(new SharedArrayBuffer(8));

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
		const initPromises: Array<Promise<any>> = [];

		this.dlob = new DLOB(this.clearingHouse.getMarketAccounts(), true);
		initPromises.push(this.dlob.init(this.clearingHouse));

		this.userMap = new UserMap(
			this.clearingHouse,
			this.clearingHouse.userAccountSubscriptionConfig
		);
		initPromises.push(this.userMap.fetchAllUsers());

		this.userStatsMap = new UserStatsMap(
			this.clearingHouse,
			this.clearingHouse.userAccountSubscriptionConfig
		);
		initPromises.push(this.userStatsMap.fetchAllUserStats());

		this.agentState = {
			stateType: new Map<number, StateType>(),
			marketPosition: new Map<number, UserPosition>(),
			account: undefined,
		};
		initPromises.push(this.updateAgentState());

		await Promise.all(initPromises);
	}

	public reset(): void {
		for (const intervalId of this.intervalIds) {
			clearInterval(intervalId);
		}
		this.intervalIds = [];
		delete this.dlob;
		delete this.userMap;
		delete this.userStatsMap;
	}

	public async startIntervalLoop(intervalMs: number) {
		await this.tryMake();
		const intervalId = setInterval(this.tryMake.bind(this), intervalMs);
		this.intervalIds.push(intervalId);

		logger.info(`${this.name} Bot started!`);
	}

	public async trigger(record: any): Promise<void> {
		if (record.eventType === 'OrderRecord') {
			this.dlob.applyOrderRecord(record as OrderRecord);
			await this.userMap.updateWithOrder(record as OrderRecord);
			await this.userStatsMap.updateWithOrder(
				record as OrderRecord,
				this.userMap
			);
			await this.tryMake();
		}
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
		logger.info(
			`jitMaker want fill base amount: ${baseFillAmountBN.toString()}`
		);
		if (baseFillAmountBN.gt(orderBaseAmount)) {
			baseFillAmountBN = orderBaseAmount;
			logger.info(
				`jitMaker will fill base amount: ${baseFillAmountBN.toString()}`
			);
		}

		return baseFillAmountBN;
	}

	/**
	 * Draws an action based on the current state of the bot.
	 *
	 */
	private async drawAndExecuteAction(market: MarketAccount) {
		// get nodes available to fill in the jit auction
		const nodesToFill = this.dlob.findJitAuctionNodesToFill(
			market.marketIndex,
			this.slotSubscriber.getSlot()
		);

		for (const nodeToFill of nodesToFill) {
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
			const orderMarketIdx = nodeToFill.node.market.marketIndex.toNumber();
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
					MARK_PRICE_PRECISION
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
				nodeToFill.node.haveFilled = false;

				// If we get an error that order does not exist, assume its been filled by somebody else and we
				// have received the history record yet
				// TODO this might not hold if events arrive out of order
				const errorCode = getErrorCode(error);
				this.metrics?.recordErrorCode(
					errorCode,
					this.clearingHouse.provider.wallet.publicKey,
					this.name
				);

				logger.error(
					`Error (${errorCode}) filling JIT auction (account: ${nodeToFill.node.userAccount.toString()} - ${nodeToFill.node.order.orderId.toString()})`
				);

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

				// console.error(error);
			}
		}
	}

	private async executeAction(action: Action): Promise<TransactionSignature> {
		const currentState = this.agentState.stateType.get(
			action.marketIndex.toNumber()
		);

		if (this.RESTRICT_POSITION_SIZE) {
			if (
				currentState === StateType.CLOSING_LONG &&
				action.direction === PositionDirection.LONG
			) {
				logger.info(
					`${
						this.name
					}: Skipping long action on market ${action.marketIndex.toNumber()}, since currently CLOSING_LONG`
				);
				return;
			}
			if (
				currentState === StateType.CLOSING_SHORT &&
				action.direction === PositionDirection.SHORT
			) {
				logger.info(
					`${
						this.name
					}: Skipping short action on market ${action.marketIndex.toNumber()}, since currently CLOSING_SHORT`
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

		return await this.clearingHouse.placeAndMake(
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

	private async tryMakeJitAuctionForMarket(market: MarketAccount) {
		await this.updateAgentState();
		await this.drawAndExecuteAction(market);
	}

	private async tryMake() {
		await this.clearingHouse.fetchAccounts();
		await this.clearingHouse.getUser().fetchAccounts();

		for (const marketAccount of this.clearingHouse.getMarketAccounts()) {
			const marketIndex = marketAccount.marketIndex;
			if (
				Atomics.compareExchange(
					this.perMarketMutexFills,
					marketIndex.toNumber(),
					0,
					1
				) === 1
			) {
				continue;
			}

			this.tryMakeJitAuctionForMarket(marketAccount);

			Atomics.store(this.perMarketMutexFills, marketIndex.toNumber(), 0);
		}
	}
}

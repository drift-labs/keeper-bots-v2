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
	convertToNumber,
	MARK_PRICE_PRECISION,
} from '@drift-labs/sdk';

import { Connection } from '@solana/web3.js';

import { getErrorCode } from '../error';
import { logger } from '../logger';
import { DLOB } from '../dlob/DLOB';
import { UserMap } from '../userMap';
import { Bot } from '../types';
import { Metrics } from '../metrics';

export class JitMakerBot implements Bot {
	public readonly name: string;
	public readonly dryRun: boolean;
	private clearingHouse: ClearingHouse;
	private slotSubscriber: SlotSubscriber;
	private dlob: DLOB;
	private perMarketMutexFills: Array<number> = []; // TODO: use real mutex
	private intervalIds: Array<NodeJS.Timer> = [];
	private userMap: UserMap;
	private connection: Connection;
	private metrics: Metrics | undefined;

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
		this.connection = connection;
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
		this.userMap = new UserMap(this.connection, this.clearingHouse);
		await this.userMap.fetchAllUsers();
	}

	public reset(): void {
		for (const intervalId of this.intervalIds) {
			clearInterval(intervalId);
		}
		this.intervalIds = [];
		delete this.dlob;
		delete this.userMap;
	}

	public startIntervalLoop(intervalMs: number): void {
		this.tryMake();
		const intervalId = setInterval(this.tryMake.bind(this), intervalMs);
		this.intervalIds.push(intervalId);

		logger.info(`${this.name} Bot started!`);
	}

	public async trigger(record: OrderRecord): Promise<void> {
		this.dlob.applyOrderRecord(record);
		await this.userMap.updateWithOrder(record);
		this.tryMake();
	}

	public viewDlob(): DLOB {
		return this.dlob;
	}

	private async tryMakeJitAuctionForMarket(market: MarketAccount) {
		const marketIndex = market.marketIndex;
		if (this.perMarketMutexFills[marketIndex.toNumber()] === 1) {
			return;
		}
		this.perMarketMutexFills[marketIndex.toNumber()] = 1;

		try {
			const nodesToFill = this.dlob.findJitAuctionNodesToFill(
				marketIndex,
				this.slotSubscriber.getSlot()
			);

			for await (const nodeToFill of nodesToFill) {
				if (nodeToFill.node.haveFilled) {
					logger.error(
						`already made the JIT auction for ${nodeToFill.node.userAccount} - ${nodeToFill.node.order.orderId}`
					);
					continue;
				}

				if (
					nodeToFill.node.userAccount.equals(
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

				if (nodeToFill.makerNode) {
					logger.info(
						`${
							this.name
						} found a maker in the node: ${nodeToFill.makerNode.userAccount.toString()} - ${nodeToFill.makerNode.order.orderId.toString()}`
					);
				}

				// calculate jit maker order params
				const orderDirection = nodeToFill.node.order.direction;
				const jitMakerDirection = isVariant(orderDirection, 'long')
					? PositionDirection.SHORT
					: PositionDirection.LONG;
				const jitMakerBaseAssetAmount = nodeToFill.node.order.baseAssetAmount
					.sub(nodeToFill.node.order.baseAssetAmountFilled)
					.div(new BN(2));
				const jitMakerPrice = nodeToFill.node.order.auctionStartPrice;
				const tsNow = new BN(new Date().getTime() / 1000);
				const orderTs = new BN(nodeToFill.node.order.ts);
				const aucDur = new BN(nodeToFill.node.order.auctionDuration);

				logger.info(
					`${this.name} filling ${JSON.stringify(
						jitMakerDirection
					)}: ${convertToNumber(
						jitMakerBaseAssetAmount,
						BASE_PRECISION
					).toFixed(4)}, limit price: ${convertToNumber(
						jitMakerPrice,
						MARK_PRICE_PRECISION
					).toFixed(4)}, it has been ${tsNow
						.sub(orderTs)
						.toNumber()}s since order placed, auction ends in ${orderTs
						.add(aucDur)
						.sub(tsNow)
						.toNumber()}s`
				);
				const orderAucStart = nodeToFill.node.order.auctionStartPrice;
				const orderAucEnd = nodeToFill.node.order.auctionEndPrice;
				logger.info(
					`${this.name}: original order aucStartPrice: ${convertToNumber(
						orderAucStart,
						MARK_PRICE_PRECISION
					).toFixed(4)}, aucEndPrice: ${convertToNumber(
						orderAucEnd,
						MARK_PRICE_PRECISION
					).toFixed(4)}`
				);

				console.log('========');
				const m = nodeToFill.node.market;
				logger.info(
					`${this.name}: market base asset reserve:  ${m.amm.baseAssetReserve}`
				);
				logger.info(
					`${this.name}: market base spread:         ${m.amm.baseSpread}`
				);
				logger.info(
					`${this.name}: market quote asset reserve: ${m.amm.quoteAssetReserve}`
				);
				logger.info(`${this.name}: amm max spread:   ${m.amm.maxSpread}`);
				logger.info(`${this.name}: amm long spread:  ${m.amm.longSpread}`);
				logger.info(`${this.name}: amm short spread: ${m.amm.shortSpread}`);
				console.log('========');

				await this.clearingHouse
					.placeAndMake(
						{
							orderType: OrderType.LIMIT,
							marketIndex: nodeToFill.node.order.marketIndex,
							baseAssetAmount: jitMakerBaseAssetAmount,
							direction: jitMakerDirection,
							price: jitMakerPrice,
							postOnly: true,
							immediateOrCancel: true,
						},
						{
							taker: nodeToFill.node.userAccount,
							order: nodeToFill.node.order,
						}
					)
					.then((txSig) => {
						logger.info(
							`${
								this.name
							}: JIT auction filled (account: ${nodeToFill.node.userAccount.toString()} - ${nodeToFill.node.order.orderId.toString()}), Tx: ${txSig}`
						);
					})
					.catch((error) => {
						nodeToFill.node.haveFilled = false;
						logger.error(
							`Error filling JIT auction (account: ${nodeToFill.node.userAccount.toString()} - ${nodeToFill.node.order.orderId.toString()})`
						);

						// If we get an error that order does not exist, assume its been filled by somebody else and we
						// have received the history record yet
						// TODO this might not hold if events arrive out of order
						const errorCode = getErrorCode(error);
						this?.metrics.recordErrorCode(
							errorCode,
							this.clearingHouse.provider.wallet.publicKey,
							this.name
						);

						if (errorCode === 6043) {
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
						logger.error(`Error code: ${errorCode}`);
						console.error(error);
					});
			}
		} catch (e) {
			logger.info(
				`${
					this.name
				} Unexpected error for market ${marketIndex.toString()} during fills`
			);
			console.error(e);
		} finally {
			this.perMarketMutexFills[marketIndex.toNumber()] = 0;
		}
	}

	private tryMake() {
		for (const marketAccount of this.clearingHouse.getMarketAccounts()) {
			this.tryMakeJitAuctionForMarket(marketAccount);
		}
	}
}

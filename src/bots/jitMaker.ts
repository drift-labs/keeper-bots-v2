import {
	BN,
	isVariant,
	ClearingHouse,
	MarketAccount,
	SlotSubscriber,
	PositionDirection,
	OrderType,
	BASE_PRECISION,
	QUOTE_PRECISION,
	convertToNumber,
	MARGIN_PRECISION,
} from '@drift-labs/sdk';

import { getErrorCode } from '../error';
import { logger } from '../logger';
import { DLOB } from '../dlob/DLOB';
import { UserMap } from '../userMap';
import { Bot } from '../types';

export class JitMakerBot implements Bot {
	public readonly name: string;
	public readonly dryRun: boolean;
	private clearingHouse: ClearingHouse;
	private slotSubscriber: SlotSubscriber;
	private dlob: DLOB;
	private perMarketMutexFills: Array<number> = []; // TODO: use real mutex
	private intervalIds: Array<NodeJS.Timer> = [];
	private userMap: UserMap;

	constructor(
		name: string,
		dryRun: boolean,
		clearingHouse: ClearingHouse,
		slotSubscriber: SlotSubscriber,
		dlob: DLOB,
		userMap: UserMap
	) {
		this.name = name;
		this.dryRun = dryRun;
		this.clearingHouse = clearingHouse;
		this.slotSubscriber = slotSubscriber;
		this.dlob = dlob;
		this.userMap = userMap;
	}

	public reset(): void {
		for (const intervalId of this.intervalIds) {
			clearInterval(intervalId);
		}
		this.intervalIds = [];
	}

	public startIntervalLoop(intervalMs: number): void {
		this.tryMake();
		const intervalId = setInterval(this.tryMake.bind(this), intervalMs);
		this.intervalIds.push(intervalId);

		logger.info(`${this.name} Bot started!`);
	}

	public async trigger(): Promise<void> {
		this.tryMake();
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
						`${nodeToFill.node.userAccount} - ${nodeToFill.node.order.orderId} Node already filled!`
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
					`JIT maker quoting order for node: ${nodeToFill.node.userAccount.toBase58()} - ${nodeToFill.node.order.orderId.toString()}`
				);

				if (nodeToFill.makerNode) {
					logger.info(
						`JIT maker found a maker in the node: ${nodeToFill.makerNode.userAccount.toString()} - ${nodeToFill.makerNode.order.orderId.toString()}`
					);
				}

				// calculate jit maker order params
				const orderDirection = nodeToFill.node.order.direction;
				const jitMakerDirection = isVariant(orderDirection, 'long')
					? PositionDirection.SHORT
					: PositionDirection.LONG;
				const jitMakerBaseAssetAmount =
					nodeToFill.node.order.baseAssetAmount.div(new BN(2));
				const jitMakerPrice = nodeToFill.node.order.auctionStartPrice;
				const tsNow = new BN(new Date().getTime() / 1000);
				const orderTs = new BN(nodeToFill.node.order.ts);
				const aucDur = new BN(nodeToFill.node.order.auctionDuration);

				logger.info(
					`JIT maker filling ${JSON.stringify(
						jitMakerDirection
					)}: ${convertToNumber(
						jitMakerBaseAssetAmount,
						BASE_PRECISION
					).toFixed(4)}, limit price: ${convertToNumber(
						jitMakerPrice,
						MARGIN_PRECISION
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
					`original order aucStartPrice: ${convertToNumber(
						orderAucStart,
						QUOTE_PRECISION
					).toFixed(4)}, aucEndPrice: ${convertToNumber(
						orderAucEnd,
						QUOTE_PRECISION
					).toFixed(4)}`
				);

				console.log('========');
				const m = nodeToFill.node.market;
				logger.info(`market base asset reserve:  ${m.amm.baseAssetReserve}`);
				logger.info(`market base spread:         ${m.amm.baseSpread}`);
				logger.info(`market quote asset reserve: ${m.amm.quoteAssetReserve}`);
				logger.info(`amm max spread:   ${m.amm.maxSpread}`);
				logger.info(`amm long spread:  ${m.amm.longSpread}`);
				logger.info(`amm short spread: ${m.amm.shortSpread}`);
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
							`JIT auction filled (account: ${nodeToFill.node.userAccount.toString()} - ${nodeToFill.node.order.orderId.toString()}), Tx: ${txSig}`
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
				`Unexpected error for market ${marketIndex.toString()} during fills`
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

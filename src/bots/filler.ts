import {
	isVariant,
	isOracleValid,
	ClearingHouse,
	MarketAccount,
	SlotSubscriber,
	calculateAskPrice,
	calculateBidPrice,
	calculateBaseAssetAmountMarketCanExecute,
} from '@drift-labs/sdk';

import { getErrorCode } from '../error';
import { logger } from '../logger';
import { DLOB } from '../dlob/DLOB';
import { UserMap } from '../userMap';
import { Bot } from '../types';

export class FillerBot implements Bot {
	public readonly name: string;
	private clearingHouse: ClearingHouse;
	private slotSubscriber: SlotSubscriber;
	private dlob: DLOB;
	private perMarketMutexFills: Array<number> = []; // TODO: use real mutex
	private intervalIds: Array<NodeJS.Timer> = [];
	private userMap: UserMap;

	constructor(
		name: string,
		clearingHouse: ClearingHouse,
		slotSubscriber: SlotSubscriber,
		dlob: DLOB,
		userMap: UserMap
	) {
		this.name = name;
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

	public start(): void {
		this.tryFill();
		const intervalId = setInterval(this.tryFill.bind(this), 500);
		this.intervalIds.push(intervalId);
	}

	public async trigger(): Promise<void> {
		logger.info('Filler bot triggered');
		this.tryFill();
	}

	private async tryFillForMarket(market: MarketAccount) {
		const marketIndex = market.marketIndex;
		if (this.perMarketMutexFills[marketIndex.toNumber()] === 1) {
			return;
		}
		this.perMarketMutexFills[marketIndex.toNumber()] = 1;

		try {
			const oraclePriceData =
				this.clearingHouse.getOracleDataForMarket(marketIndex);
			const oracleIsValid = isOracleValid(
				market.amm,
				oraclePriceData,
				this.clearingHouse.getStateAccount().oracleGuardRails,
				this.slotSubscriber.getSlot()
			);

			const vAsk = calculateAskPrice(market, oraclePriceData);
			const vBid = calculateBidPrice(market, oraclePriceData);

			const nodesToFill = this.dlob.findNodesToFill(
				marketIndex,
				vBid,
				vAsk,
				this.slotSubscriber.getSlot(),
				oracleIsValid ? oraclePriceData : undefined
			);

			for (const nodeToFill of nodesToFill) {
				if (nodeToFill.node.haveFilled) {
					continue;
				}

				if (
					!nodeToFill.makerNode &&
					(isVariant(nodeToFill.node.order.orderType, 'limit') ||
						isVariant(nodeToFill.node.order.orderType, 'triggerLimit'))
				) {
					const baseAssetAmountMarketCanExecute =
						calculateBaseAssetAmountMarketCanExecute(
							market,
							nodeToFill.node.order,
							oraclePriceData
						);

					if (
						baseAssetAmountMarketCanExecute.lt(
							market.amm.baseAssetAmountStepSize
						)
					) {
						continue;
					}
				}

				nodeToFill.node.haveFilled = true;

				logger.info(
					`trying to fill (account: ${nodeToFill.node.userAccount.toString()}) order ${nodeToFill.node.order.orderId.toString()}`
				);

				if (nodeToFill.makerNode) {
					`including maker: ${nodeToFill.makerNode.userAccount.toString()}) with order ${nodeToFill.makerNode.order.orderId.toString()}`;
				}

				let makerInfo;
				if (nodeToFill.makerNode) {
					makerInfo = {
						maker: nodeToFill.makerNode.userAccount,
						order: nodeToFill.makerNode.order,
					};
				}

				const user = this.userMap.get(nodeToFill.node.userAccount.toString());
				this.clearingHouse
					.fillOrder(
						nodeToFill.node.userAccount,
						user.getUserAccount(),
						nodeToFill.node.order,
						makerInfo
					)
					.then((txSig) => {
						logger.info(
							`Filled user (account: ${nodeToFill.node.userAccount.toString()}) order: ${nodeToFill.node.order.orderId.toString()}`
						);
						logger.info(`Tx: ${txSig}`);
					})
					.catch((error) => {
						nodeToFill.node.haveFilled = false;
						logger.info(
							`Error filling user (account: ${nodeToFill.node.userAccount.toString()}) order: ${nodeToFill.node.order.orderId.toString()}`
						);
						console.error(error);

						// If we get an error that order does not exist, assume its been filled by somebody else and we
						// have received the history record yet
						// TODO this might not hold if events arrive out of order
						const errorCode = getErrorCode(error);
						if (errorCode === 6043) {
							this.dlob.remove(
								nodeToFill.node.order,
								nodeToFill.node.userAccount,
								() => {
									logger.info(
										`Order ${nodeToFill.node.order.orderId.toString()} not found when trying to fill. Removing from order list`
									);
								}
							);
							// dlob.printTopOfOrderLists(this.clearingHouse, nodeToFill.node.order.marketIndex);
						}
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

	private tryFill() {
		for (const marketAccount of this.clearingHouse.getMarketAccounts()) {
			this.tryFillForMarket(marketAccount);
		}
	}
}

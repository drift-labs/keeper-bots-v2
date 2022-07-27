import {
	isVariant,
	isOracleValid,
	ClearingHouse,
	MarketAccount,
	SlotSubscriber,
	calculateAskPrice,
	calculateBidPrice,
	calculateBaseAssetAmountMarketCanExecute,
	PositionDirection,
	OrderType,
	BASE_PRECISION,
	convertToNumber,
} from '@drift-labs/sdk';

import { getErrorCode } from '../error';
import { logger } from '../logger';
import { DLOB } from '../dlob/DLOB';
import { UserMap } from '../userMap';
import { Bot } from '../types';
import { BN } from 'bn.js';

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
			// const oraclePriceData =
			// 	this.clearingHouse.getOracleDataForMarket(marketIndex);
			// const oracleIsValid = isOracleValid(
			// 	market.amm,
			// 	oraclePriceData,
			// 	this.clearingHouse.getStateAccount().oracleGuardRails,
			// 	this.slotSubscriber.getSlot()
			// );

			// const vAsk = calculateAskPrice(market, oraclePriceData);
			// const vBid = calculateBidPrice(market, oraclePriceData);

			// TODO: don't need crossing nodes? just jit auction nodes
			// const nodesToFill = this.dlob.findNodesToFill(
			// 	marketIndex,
			// 	vBid,
			// 	vAsk,
			// 	this.slotSubscriber.getSlot(),
			// 	oracleIsValid ? oraclePriceData : undefined
			// );
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

				// not maker and (limit order OR triggerLimit)
				/*
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
                */

				// calc base amt to fill

				nodeToFill.node.haveFilled = true;

				logger.info(
					`JIT maker quoting order for node: ${nodeToFill.node.userAccount.toBase58()} - ${nodeToFill.node.order.orderId.toString()}`
				);

				if (nodeToFill.makerNode) {
					logger.info(
						`JIT maker found a maker in the node: ${nodeToFill.makerNode.userAccount.toString()} - ${nodeToFill.makerNode.order.orderId.toString()}`
					);
				}

				// let makerInfo;
				// if (nodeToFill.makerNode) {
				// 	makerInfo = {
				// 		maker: nodeToFill.makerNode.userAccount,
				// 		order: nodeToFill.makerNode.order,
				// 	};
				// }

				const orderDirection = nodeToFill.node.order.direction;
				const jitMakerDirection = isVariant(orderDirection, 'long')
					? PositionDirection.SHORT
					: PositionDirection.LONG;
				let jitMakerBaseAssetAmount = nodeToFill.node.order.baseAssetAmount.div(
					new BN(2)
				);

				// wtf?
				// [2022-07-27T06:55:44.424Z] info: JIT maker oh the steps: 2500000000000 -> 2949303843810296357342915458947350528
				// if (jitMakerBaseAssetAmount.lt(market.amm.baseAssetAmountStepSize)) {
				//     logger.info(
				//         `JIT maker oh the steps: ${jitMakerBaseAssetAmount.toString()} -> ${market.amm.baseAssetAmountStepSize}`
				//     )
				//     jitMakerBaseAssetAmount = market.amm.baseAssetAmountStepSize;
				// }

				logger.info(
					`JIT maker filling ${JSON.stringify(
						jitMakerDirection
					)}: ${convertToNumber(
						jitMakerBaseAssetAmount,
						BASE_PRECISION
					).toFixed(4)}`
				);

				// const user = this.userMap.get(nodeToFill.node.userAccount.toString());
				console.log('>>>>>');
				console.log(`taker: ${nodeToFill.node.userAccount.toString()}`);
				console.log(`order: ${JSON.stringify(nodeToFill.node.order)}`);
				console.log('>>>>>');
				await this.clearingHouse
					.placeAndMake(
						{
							orderType: OrderType.MARKET,
							marketIndex: nodeToFill.node.marketAccount.marketIndex,
							baseAssetAmount: jitMakerBaseAssetAmount,
							direction: jitMakerDirection,
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
							// dlob.printTopOfOrderLists(this.clearingHouse, nodeToFill.node.order.marketIndex);
						}
						logger.error(`Error code: ${errorCode}`);
						logger.error(error.logs);
						logger.error(error);
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

import {
	NodeToFill,
	NodeToTrigger,
	PerpMarketAccount,
	DLOB,
	calculateAskPrice,
	calculateBidPrice,
	MarketType,
	BN,
	PositionDirection,
	calculateMaxBaseAssetAmountFillable,
	calculateUpdatedAMM,
	ZERO,
} from '@drift-labs/sdk';

import { FillerLiteBot } from './fillerLite';
import { logger } from '../logger';

const MAX_NUM_MAKERS = 6;

export class FillerBulkBot extends FillerLiteBot {
	protected getPerpNodesForMarket(
		market: PerpMarketAccount,
		dlob: DLOB
	): {
		nodesToFill: Array<NodeToFill>;
		nodesToTrigger: Array<NodeToTrigger>;
	} {
		const marketIndex = market.marketIndex;

		const oraclePriceData =
			this.driftClient.getOracleDataForPerpMarket(marketIndex);

		const fillSlot = this.orderSubscriber.getSlot();

		return {
			nodesToFill: this.findNodesToFill(market, dlob),
			nodesToTrigger: dlob.findNodesToTrigger(
				marketIndex,
				fillSlot,
				oraclePriceData.price,
				MarketType.PERP,
				this.driftClient.getStateAccount()
			),
		};
	}

	protected findNodesToFill(
		market: PerpMarketAccount,
		dlob: DLOB
	): NodeToFill[] {
		const nodesToFill = [];
		const marketIndex = market.marketIndex;

		const oraclePriceData =
			this.driftClient.getOracleDataForPerpMarket(marketIndex);

		const vAsk = calculateAskPrice(market, oraclePriceData);
		const vBid = calculateBidPrice(market, oraclePriceData);

		const updatedAMM = calculateUpdatedAMM(market.amm, oraclePriceData);
		const vammCanFillLongs = calculateMaxBaseAssetAmountFillable(
			updatedAMM,
			PositionDirection.LONG
		).gte(market.amm.minOrderSize);
		const vammCanFillShorts = calculateMaxBaseAssetAmountFillable(
			updatedAMM,
			PositionDirection.SHORT
		).gte(market.amm.minOrderSize);

		const fillSlot = this.getMaxSlot();

		let bestBid = dlob.getBestBid(
			marketIndex,
			fillSlot,
			MarketType.PERP,
			oraclePriceData
		);
		// only consider vamm if it has liquidity
		if (vammCanFillShorts) {
			bestBid = bestBid ? BN.max(bestBid, vBid) : vBid;
		}

		let bestAsk = dlob.getBestAsk(
			marketIndex,
			fillSlot,
			MarketType.PERP,
			oraclePriceData
		);
		// only consider vamm if it has liquidity
		if (vammCanFillLongs) {
			bestAsk = bestAsk ? BN.min(bestAsk, vAsk) : vAsk;
		}

		if (bestAsk) {
			const takingBidsGenerator = dlob.getTakingBids(
				marketIndex,
				MarketType.PERP,
				fillSlot,
				oraclePriceData
			);

			for (const takingBid of takingBidsGenerator) {
				const takingBidPrice = takingBid.getPrice(oraclePriceData, fillSlot);
				if (!takingBidPrice || takingBidPrice.gte(bestAsk)) {
					const makerNodes = [];
					const makersSeens = new Set<string>();
					let takerBaseAmountUnfilled = takingBid.order!.baseAssetAmount.sub(
						takingBid.order!.baseAssetAmountFilled
					);
					const restingAsks = dlob.getRestingLimitAsks(
						marketIndex,
						fillSlot,
						MarketType.PERP,
						oraclePriceData
					);

					for (const restingAsk of restingAsks) {
						// only want to keep adding makers past max num makers if the taker is still crossing
						if (makersSeens.size >= MAX_NUM_MAKERS) {
							const askPrice = restingAsk.getPrice(oraclePriceData, fillSlot)!;

							if (takingBidPrice && takingBidPrice.lt(askPrice)) {
								break;
							}
						}

						// stop adding makers if all of taker size is filled and we have seen max num makers
						if (
							makersSeens.size >= MAX_NUM_MAKERS &&
							takerBaseAmountUnfilled.lte(ZERO)
						) {
							break;
						}

						const makerBaseAmountUnfilled =
							restingAsk.order!.baseAssetAmount.sub(
								restingAsk.order!.baseAssetAmountFilled
							);
						makerNodes.push(restingAsk);
						makersSeens.add(restingAsk.userAccount!);
						takerBaseAmountUnfilled = takerBaseAmountUnfilled.sub(
							makerBaseAmountUnfilled
						);
					}

					nodesToFill.push({
						node: takingBid,
						makerNodes,
					});
				}
			}
		} else {
			logger.info(`No best ask for ${marketIndex.toString()}`);
		}

		if (bestBid) {
			const takingAsksGenerator = dlob.getTakingAsks(
				marketIndex,
				MarketType.PERP,
				fillSlot,
				oraclePriceData
			);

			for (const takingAsk of takingAsksGenerator) {
				const takingAskPrice = takingAsk.getPrice(oraclePriceData, fillSlot);
				if (!takingAskPrice || takingAskPrice.lte(bestBid)) {
					const makerNodes = [];
					const makersSeens = new Set<string>();
					let takerBaseAmountUnfilled = takingAsk.order!.baseAssetAmount.sub(
						takingAsk.order!.baseAssetAmountFilled
					);
					const restingBids = dlob.getRestingLimitBids(
						marketIndex,
						fillSlot,
						MarketType.PERP,
						oraclePriceData
					);

					for (const restingBid of restingBids) {
						// only want to keep adding makers past max num makers if the taker is still crossing
						if (makersSeens.size >= MAX_NUM_MAKERS) {
							const bidPrice = restingBid.getPrice(oraclePriceData, fillSlot)!;

							if (takingAskPrice && takingAskPrice.gt(bidPrice)) {
								break;
							}
						}

						// stop adding makers if all of taker size is filled and we have seen max num makers
						if (
							makersSeens.size >= MAX_NUM_MAKERS &&
							takerBaseAmountUnfilled.lte(ZERO)
						) {
							break;
						}

						const makerBaseAmountUnfilled =
							restingBid.order!.baseAssetAmount.sub(
								restingBid.order!.baseAssetAmountFilled
							);
						makerNodes.push(restingBid);
						makersSeens.add(restingBid.userAccount!);
						takerBaseAmountUnfilled = takerBaseAmountUnfilled.sub(
							makerBaseAmountUnfilled
						);
					}

					nodesToFill.push({
						node: takingAsk,
						makerNodes,
					});
				}
			}
		} else {
			logger.info(`No best bid for ${marketIndex.toString()}`);
		}

		return nodesToFill;
	}
}

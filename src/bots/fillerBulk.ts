import {
	NodeToFill,
	NodeToTrigger,
	PerpMarketAccount,
	DLOB,
	calculateAskPrice,
	calculateBidPrice,
	MarketType,
	BN,
} from '@drift-labs/sdk';

import { FillerLiteBot } from './fillerLite';

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

		const fillSlot = this.orderSubscriber.getSlot();

		const bestDLOBBid = dlob.getBestBid(
			marketIndex,
			fillSlot,
			MarketType.PERP,
			oraclePriceData
		);
		const bestBid = bestDLOBBid ? BN.max(bestDLOBBid, vBid) : vBid;

		const seenBidMaker = new Set<string>();
		const restingBids = dlob.getRestingLimitBids(
			marketIndex,
			fillSlot,
			MarketType.PERP,
			oraclePriceData
		);
		const topRestingBids = [];
		for (const restingBid of restingBids) {
			topRestingBids.push(restingBid);
			seenBidMaker.add(restingBid.userAccount?.toString() || '');
			if (seenBidMaker.size == MAX_NUM_MAKERS) {
				break;
			}
		}

		const bestDLOBAsk = dlob.getBestAsk(
			marketIndex,
			fillSlot,
			MarketType.PERP,
			oraclePriceData
		);
		const bestAsk = bestDLOBAsk ? BN.min(bestDLOBAsk, vAsk) : vAsk;

		const seenAskMaker = new Set<string>();
		const restingAsks = dlob.getRestingLimitAsks(
			marketIndex,
			fillSlot,
			MarketType.PERP,
			oraclePriceData
		);
		const topRestingAsks = [];
		for (const restingAsk of restingAsks) {
			topRestingAsks.push(restingAsk);
			seenAskMaker.add(restingAsk.userAccount?.toString() || '');
			if (seenAskMaker.size == MAX_NUM_MAKERS) {
				break;
			}
		}

		const takingBidsGenerator = dlob.getTakingBids(
			marketIndex,
			MarketType.PERP,
			fillSlot,
			oraclePriceData
		);
		for (const takingBid of takingBidsGenerator) {
			const takingBidPrice = takingBid.getPrice(oraclePriceData, fillSlot);
			if (!takingBidPrice || takingBidPrice.gte(bestAsk)) {
				nodesToFill.push({
					node: takingBid,
					makerNodes: topRestingAsks.filter(
						(node) => !node.userAccount!.equals(takingBid.userAccount!)
					),
				});
			}
		}

		const takingAsksGenerator = dlob.getTakingAsks(
			marketIndex,
			MarketType.PERP,
			fillSlot,
			oraclePriceData
		);
		for (const takingAsk of takingAsksGenerator) {
			const takingAskPrice = takingAsk.getPrice(oraclePriceData, fillSlot);
			if (!takingAskPrice || takingAskPrice.lte(bestBid)) {
				nodesToFill.push({
					node: takingAsk,
					makerNodes: topRestingBids.filter(
						(node) => !node.userAccount!.equals(takingAsk.userAccount!)
					),
				});
			}
		}

		const crossLimitOrderNodesToFill = dlob.findCrossingRestingLimitOrders(
			marketIndex,
			fillSlot,
			MarketType.PERP,
			oraclePriceData
		);

		for (const crossLimitOrderNodeToFill of crossLimitOrderNodesToFill) {
			nodesToFill.push(crossLimitOrderNodeToFill);
		}

		return nodesToFill;
	}
}

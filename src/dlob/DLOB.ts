import {
	getOrderId,
	Node,
	NodeType,
	nodeTypeForOrder,
	OrderList,
} from './OrderList';
import {
	getLimitPrice,
	isVariant,
	MARK_PRICE_PRECISION,
	Markets,
	OraclePriceData,
	Order,
	ZERO,
} from '@drift-labs/sdk';
import { PublicKey } from '@solana/web3.js';
import { BN } from '@project-serum/anchor';

type OrderLists = {
	ask: OrderList;
	bid: OrderList;
};

export type DLOBOrderLists = {
	fixed: OrderLists;
	floating: OrderLists;
};

type OrderBookCallback = () => void;

const HUGE_BN = MARK_PRICE_PRECISION.mul(MARK_PRICE_PRECISION);

export type DLOBMatch = {
	node: Node;
	makerNode?: Node;
};

type Side = 'ask' | 'bid';

export type DLOBPrice = {
	price: BN;
	node?: Node;
	side: Side;
};

export class DLOB {
	openOrders = new Set<string>();
	orderLists = new Map<number, DLOBOrderLists>();

	public constructor() {
		for (const market of Markets) {
			this.orderLists.set(market.marketIndex.toNumber(), {
				fixed: {
					ask: new OrderList(market.marketIndex, 'asc'),
					bid: new OrderList(market.marketIndex, 'desc'),
				},
				floating: {
					ask: new OrderList(market.marketIndex, 'asc'),
					bid: new OrderList(market.marketIndex, 'desc'),
				},
			});
		}
	}

	public insert(
		order: Order,
		userAccount: PublicKey,
		onInsert?: OrderBookCallback
	): void {
		if (isVariant(order, 'init')) {
			return;
		}

		if (isVariant(order.status, 'open')) {
			this.openOrders.add(this.getOpenOrderId(order, userAccount));
		}
		this.getListForOrder(order).insert(order, userAccount);

		if (onInsert) {
			onInsert();
		}
	}

	public remove(
		order: Order,
		userAccount: PublicKey,
		onRemove?: OrderBookCallback
	): void {
		this.openOrders.delete(this.getOpenOrderId(order, userAccount));
		this.getListForOrder(order).remove(order, userAccount);

		if (onRemove) {
			onRemove();
		}
	}

	public update(
		order: Order,
		userAccount: PublicKey,
		onUpdate?: OrderBookCallback
	): void {
		this.getListForOrder(order).update(order, userAccount);
		if (onUpdate) {
			onUpdate();
		}
	}

	public disable(
		order: Order,
		userAccount: PublicKey,
		onDisable?: OrderBookCallback
	): void {
		const orderList = this.getListForOrder(order);
		if (
			this.openOrders.has(this.getOpenOrderId(order, userAccount)) &&
			orderList.has(order, userAccount)
		) {
			orderList.remove(order, userAccount);
			if (onDisable) {
				onDisable();
			}
		}
	}

	public enable(
		order: Order,
		userAccount: PublicKey,
		onEnable?: OrderBookCallback
	): void {
		const orderList = this.getListForOrder(order);
		if (
			this.openOrders.has(this.getOpenOrderId(order, userAccount)) &&
			!orderList.has(order, userAccount)
		) {
			orderList.insert(order, userAccount);

			if (onEnable) {
				onEnable();
			}
		}
	}

	public getListForOrder(order: Order): OrderList {
		const nodeType = nodeTypeForOrder(order);
		const orderLists = this.getOrderLists(
			order.marketIndex.toNumber(),
			nodeType
		);

		return isVariant(order.direction, 'long') ? orderLists.bid : orderLists.ask;
	}

	public getOrderLists(marketIndex: number, type: NodeType): OrderLists {
		return this.orderLists.get(marketIndex)[type];
	}

	public getOpenOrderId(order: Order, userAccount: PublicKey): string {
		return getOrderId(order, userAccount);
	}

	public findMatches(
		marketIndex: BN,
		vBid: BN,
		vAsk: BN,
		oraclePriceData?: OraclePriceData
	): DLOBMatch[] {
		const matches = new Array<DLOBMatch>();

		const askGenerator = this.getAsks(marketIndex, vAsk, oraclePriceData);
		const bidGenerator = this.getBids(marketIndex, vBid, oraclePriceData);

		let nextAsk = askGenerator.next();
		let nextBid = bidGenerator.next();

		while (nextAsk.done && nextBid.done) {
			const { match, crossingSide } = this.getMatch(
				nextAsk.value,
				nextBid.value
			);

			if (match) {
				matches.push(match);
				if (matches.length === 10) {
					break;
				}
			}

			if (crossingSide === 'none') {
				break;
			}

			if (crossingSide === 'ask') {
				nextAsk = askGenerator.next();
			}

			if (crossingSide === 'bid') {
				nextBid = bidGenerator.next();
			}
		}
	}

	*getAsks(
		marketIndex: BN,
		vAsk: BN,
		oraclePriceData?: OraclePriceData
	): Generator<DLOBPrice> {
		const orderLists = this.orderLists.get(marketIndex.toNumber());

		let fixedNode = orderLists.fixed.ask.head;
		let floatingNode = oraclePriceData
			? orderLists.floating.ask.head
			: undefined;
		let vAskNode = {};

		while (vAskNode && fixedNode && floatingNode) {
			const fixedNodePrice = fixedNode ? fixedNode.order.price : HUGE_BN;
			const floatingNodePrice = floatingNode
				? getLimitPrice(floatingNode.order, oraclePriceData)
				: HUGE_BN;

			if (
				fixedNode &&
				fixedNodePrice.lt(floatingNodePrice) &&
				fixedNodePrice.lt(vAsk)
			) {
				fixedNode = fixedNode.next;
				yield {
					price: fixedNodePrice,
					node: fixedNode,
					side: 'ask',
				};
			} else if (
				floatingNode &&
				floatingNodePrice.lt(fixedNodePrice) &&
				floatingNodePrice.lt(vAsk)
			) {
				floatingNode = floatingNode.next;
				yield {
					price: floatingNodePrice,
					node: floatingNode,
					side: 'ask',
				};
			} else if (
				vAskNode &&
				vAsk.lt(fixedNodePrice) &&
				vAsk.lt(floatingNodePrice)
			) {
				vAskNode = undefined;
				yield {
					price: vAsk,
					node: undefined,
					side: 'ask',
				};
			}
		}
	}

	*getBids(
		marketIndex: BN,
		vBid: BN,
		oraclePriceData?: OraclePriceData
	): Generator<DLOBPrice> {
		const orderLists = this.orderLists.get(marketIndex.toNumber());

		let fixedNode = orderLists.fixed.bid.head;
		let floatingNode = oraclePriceData
			? orderLists.floating.bid.head
			: undefined;
		let vBidNode = {};

		while (vBidNode && fixedNode && floatingNode) {
			const fixedNodePrice = fixedNode ? fixedNode.order.price : ZERO;
			const floatingNodePrice = floatingNode
				? getLimitPrice(floatingNode.order, oraclePriceData)
				: ZERO;

			if (
				fixedNode &&
				fixedNodePrice.gt(floatingNodePrice) &&
				fixedNodePrice.gt(vBid)
			) {
				fixedNode = fixedNode.next;
				yield {
					price: fixedNodePrice,
					node: fixedNode,
					side: 'bid',
				};
			} else if (
				floatingNode &&
				floatingNodePrice.gt(fixedNodePrice) &&
				floatingNodePrice.gt(vBid)
			) {
				floatingNode = floatingNode.next;
				yield {
					price: floatingNodePrice,
					node: floatingNode,
					side: 'bid',
				};
			} else if (
				vBidNode &&
				vBid.gt(fixedNodePrice) &&
				vBid.gt(floatingNodePrice)
			) {
				vBidNode = undefined;
				yield {
					price: vBid,
					node: undefined,
					side: 'bid',
				};
			}
		}
	}

	getMatch(
		ask: DLOBPrice,
		bid: DLOBPrice
	): {
		match?: DLOBMatch;
		crossingSide: 'ask' | 'bid' | 'none';
	} {
		// no cross
		if (bid.price.lt(ask.price)) {
			return {
				crossingSide: 'none',
			};
		}

		// User bid crosses the vamm ask
		if (!ask.node) {
			return {
				match: {
					node: bid.node,
				},
				crossingSide: bid.side,
			};
		}

		// User ask crosses the vamm bid
		if (!bid.node) {
			return {
				match: {
					node: ask.node,
				},
				crossingSide: ask.side,
			};
		}

		// Two maker orders cross
		if (bid.node.order.postOnly && ask.node.order.postOnly) {
			const newerSide = bid.node.order.ts.lt(ask.node.order.ts)
				? bid.side
				: ask.side;
			return {
				crossingSide: newerSide,
			};
		}

		// Bid is maker
		if (bid.node.order.postOnly) {
			return {
				match: {
					node: bid.node,
					makerNode: ask.node,
				},
				crossingSide: 'ask',
			};
		}

		// Ask is maker
		if (ask.node.order.postOnly) {
			return {
				match: {
					node: ask.node,
					makerNode: bid.node,
				},
				crossingSide: 'bid',
			};
		}

		// Both are takers
		const newerDLOBPrice = bid.node.order.ts.lt(ask.node.order.ts) ? bid : ask;
		const olderDLOBPrice = bid.node.order.ts.lt(ask.node.order.ts) ? ask : bid;
		return {
			match: {
				node: newerDLOBPrice.node,
				makerNode: olderDLOBPrice.node,
			},
			crossingSide: newerDLOBPrice.side,
		};
	}

	public getBestAsk(
		marketIndex: BN,
		vAsk: BN,
		oraclePriceData: OraclePriceData
	): BN {
		return this.getAsks(marketIndex, vAsk, oraclePriceData).next().value;
	}

	public getBestBid(
		marketIndex: BN,
		vBid: BN,
		oraclePriceData: OraclePriceData
	): BN {
		return this.getBids(marketIndex, vBid, oraclePriceData).next().value;
	}
}

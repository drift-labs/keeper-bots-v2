import { NodeType, nodeTypeForOrder, OrderList } from './OrderList';
import { isVariant, Markets, Order } from '@drift-labs/sdk';
import { PublicKey } from '@solana/web3.js';

type OrderLists = {
	ask: {
		// Taker asks, Stop Limits, Take Profit Limits
		asc: OrderList;
	};
	mark: {
		// Maker asks, Stop Markets, Take Profits, Stop Limits, Take Profit Limits
		asc: OrderList;
		// Maker bids, Stop Markets, Take Profits
		desc: OrderList;
	};
	bid: {
		// Taker bids, Stop Limits, Take Profit Limits
		desc: OrderList;
	};
};

export type DLOBOrderLists = {
	fixed: OrderLists;
	floating: OrderLists;
};

type OrderBookCallback = () => void;

export class DLOB {
	openOrders = new Set<number>();
	orderLists = new Map<number, DLOBOrderLists>();

	public constructor() {
		for (const market of Markets) {
			this.orderLists.set(market.marketIndex.toNumber(), {
				fixed: {
					ask: {
						asc: new OrderList(market.marketIndex, 'asc'),
					},
					mark: {
						asc: new OrderList(market.marketIndex, 'asc'),
						desc: new OrderList(market.marketIndex, 'desc'),
					},
					bid: {
						desc: new OrderList(market.marketIndex, 'desc'),
					},
				},
				floating: {
					ask: {
						asc: new OrderList(market.marketIndex, 'asc'),
					},
					mark: {
						asc: new OrderList(market.marketIndex, 'asc'),
						desc: new OrderList(market.marketIndex, 'desc'),
					},
					bid: {
						desc: new OrderList(market.marketIndex, 'desc'),
					},
				},
			});
		}
	}

	public insert(
		order: Order,
		userAccount: PublicKey,
		userOrders: PublicKey,
		onInsert?: OrderBookCallback
	): void {
		if (isVariant(order, 'init') || isVariant(order.orderType, 'market')) {
			return;
		}

		const orderId = order.orderId.toNumber();
		if (isVariant(order.status, 'open')) {
			this.openOrders.add(orderId);
		}
		this.getListForOrder(order).insert(order, userAccount, userOrders);

		if (onInsert) {
			onInsert();
		}
	}

	public remove(order: Order, onRemove?: OrderBookCallback): void {
		const orderId = order.orderId.toNumber();
		this.openOrders.delete(orderId);
		this.getListForOrder(order).remove(orderId);

		if (onRemove) {
			onRemove();
		}
	}

	public update(order: Order, onUpdate?: OrderBookCallback): void {
		this.getListForOrder(order).update(order);
		if (onUpdate) {
			onUpdate();
		}
	}

	public disable(order: Order, onDisable?: OrderBookCallback): void {
		const orderId = order.orderId.toNumber();
		const orderList = this.getListForOrder(order);
		if (this.openOrders.has(orderId) && orderList.has(orderId)) {
			orderList.remove(orderId);
			if (onDisable) {
				onDisable();
			}
		}
	}

	public enable(
		order: Order,
		userAccount: PublicKey,
		userOrders: PublicKey,
		onEnable?: OrderBookCallback
	): void {
		const orderId = order.orderId.toNumber();
		const orderList = this.getListForOrder(order);
		if (this.openOrders.has(orderId) && !orderList.has(orderId)) {
			orderList.insert(order, userAccount, userOrders);

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

		if (isVariant(order.orderType, 'limit') && order.postOnly) {
			return isVariant(order.direction, 'long')
				? orderLists.mark.desc
				: orderLists.mark.asc;
		}

		if (isVariant(order.orderType, 'triggerLimit')) {
			if (
				isVariant(order.triggerCondition, 'below') &&
				isVariant(order.direction, 'long')
			) {
				return order.price.lt(order.triggerPrice)
					? orderLists.bid.desc
					: orderLists.mark.desc;
			}

			if (
				isVariant(order.triggerCondition, 'above') &&
				isVariant(order.direction, 'short')
			) {
				return order.price.gt(order.triggerPrice)
					? orderLists.ask.asc
					: orderLists.mark.asc;
			}

			return isVariant(order.triggerCondition, 'below')
				? orderLists.mark.desc
				: orderLists.mark.asc;
		}

		if (isVariant(order.orderType, 'triggerMarket')) {
			return isVariant(order.triggerCondition, 'below')
				? orderLists.mark.desc
				: orderLists.mark.asc;
		}

		return isVariant(order.direction, 'long')
			? orderLists.bid.desc
			: orderLists.ask.asc;
	}

	public getOrderLists(marketIndex: number, type: NodeType): OrderLists {
		return this.orderLists.get(marketIndex)[type];
	}
}

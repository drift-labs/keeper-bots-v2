import { OrderList, sortDirectionForOrder } from './OrderList';
import { isVariant, Markets, Order, ZERO } from '@drift-labs/sdk';
import { PublicKey } from '@solana/web3.js';

type OrderLists = {
	desc: OrderList;
	asc: OrderList;
};

export type DLOBOrderLists = { fixed: OrderLists; floating: OrderLists };

type OrderBookCallback = () => void;

export type ListType = 'fixed' | 'floating';

export function listTypeForOrder(order: Order): ListType {
	return order.oraclePriceOffset.eq(ZERO) ? 'fixed' : 'floating';
}

export class DLOB {
	openOrders = new Set<number>();
	orderLists = new Map<number, DLOBOrderLists>();

	public constructor() {
		for (const market of Markets) {
			this.orderLists.set(market.marketIndex.toNumber(), {
				fixed: {
					asc: new OrderList(market.marketIndex, 'asc'),
					desc: new OrderList(market.marketIndex, 'desc'),
				},
				floating: {
					asc: new OrderList(market.marketIndex, 'asc'),
					desc: new OrderList(market.marketIndex, 'desc'),
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

		const listType = listTypeForOrder(order);
		const sortDirection = sortDirectionForOrder(order);

		const orderId = order.orderId.toNumber();
		if (isVariant(order.status, 'open')) {
			this.openOrders.add(orderId);
		}
		this.orderLists
			.get(order.marketIndex.toNumber())[listType][sortDirection].insert(order, userAccount, userOrders);

		if (onInsert) {
			onInsert();
		}
	}

	public remove(order: Order, onRemove?: OrderBookCallback): void {
		const listType = listTypeForOrder(order);
		const sortDirection = sortDirectionForOrder(order);

		const orderId = order.orderId.toNumber();
		this.openOrders.delete(orderId);
		this.orderLists
			.get(order.marketIndex.toNumber())[listType][sortDirection].remove(orderId);

		if (onRemove) {
			onRemove();
		}
	}

	public update(order: Order, onUpdate?: OrderBookCallback): void {
		const listType = listTypeForOrder(order);
		const sortDirection = sortDirectionForOrder(order);

		this.orderLists
			.get(order.marketIndex.toNumber())[listType][sortDirection].update(order);
		if (onUpdate) {
			onUpdate();
		}
	}

	public disable(order: Order, onDisable?: OrderBookCallback): void {
		const listType = listTypeForOrder(order);
		const sortDirection = sortDirectionForOrder(order);

		const orderId = order.orderId.toNumber();
		const orderList = this.orderLists.get(order.marketIndex.toNumber())[
			listType
		][sortDirection];
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
		const listType = listTypeForOrder(order);
		const sortDirection = sortDirectionForOrder(order);

		const orderId = order.orderId.toNumber();
		const orderList = this.orderLists.get(order.marketIndex.toNumber())[
			listType
		][sortDirection];
		if (this.openOrders.has(orderId) && !orderList.has(orderId)) {
			orderList.insert(order, userAccount, userOrders);

			if (onEnable) {
				onEnable();
			}
		}
	}

	public getOrderLists(marketIndex: number, type: ListType): OrderLists {
		return this.orderLists.get(marketIndex)[type];
	}
}

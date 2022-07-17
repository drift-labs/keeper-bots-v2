import {
	AMM_RESERVE_PRECISION,
	BN,
	convertToNumber,
	isVariant,
	MARK_PRICE_PRECISION,
	Order,
	ZERO,
} from '@drift-labs/sdk';
import { PublicKey } from '@solana/web3.js';

export type SortDirection = 'asc' | 'desc';

export class Node {
	order: Order;
	userAccount: PublicKey;
	sortPrice: BN;
	next?: Node;
	previous?: Node;
	haveFilled = false;
	sortDirection: SortDirection;

	constructor(
		order: Order,
		userAccount: PublicKey,
		sortDirection: SortDirection
	) {
		this.order = order;
		this.userAccount = userAccount;
		this.sortPrice = this.getSortingPrice(order);
		this.sortDirection = sortDirection;
	}

	getSortingPrice(order: Order): BN {
		return order.price;
	}

	public getLabel(): string {
		let msg = `Order ${getOrderId(this.order, this.userAccount)}`;
		msg += ` ${isVariant(this.order.direction, 'long') ? 'LONG' : 'SHORT'} `;
		msg += `${convertToNumber(
			this.order.baseAssetAmount,
			AMM_RESERVE_PRECISION
		).toFixed(3)}`;
		if (this.order.price.gt(ZERO)) {
			msg += ` @ ${convertToNumber(
				this.order.price,
				MARK_PRICE_PRECISION
			).toFixed(3)}`;
		}
		if (this.order.triggerPrice.gt(ZERO)) {
			msg += ` ${
				isVariant(this.order.triggerCondition, 'below') ? 'BELOW' : 'ABOVE'
			}`;
			msg += ` ${convertToNumber(
				this.order.triggerPrice,
				MARK_PRICE_PRECISION
			).toFixed(3)}`;
		}
		return msg;
	}
}

export class FloatingNode extends Node {
	getSortingPrice(order: Order): BN {
		return order.oraclePriceOffset;
	}

	public getLabel(): string {
		let msg = `Order ${getOrderId(this.order, this.userAccount)}`;
		msg += ` ${isVariant(this.order.direction, 'long') ? 'LONG' : 'SHORT'} `;
		msg += `${convertToNumber(
			this.order.baseAssetAmount,
			AMM_RESERVE_PRECISION
		).toFixed(3)}`;
		msg += ` @ Oracle ${
			this.order.oraclePriceOffset.gte(ZERO) ? '+' : '-'
		} ${convertToNumber(
			this.order.oraclePriceOffset.abs(),
			MARK_PRICE_PRECISION
		).toFixed(3)}`;
		return msg;
	}
}

export type NodeType = 'fixed' | 'floating';

export function nodeTypeForOrder(order: Order): NodeType {
	return order.oraclePriceOffset.eq(ZERO) ? 'fixed' : 'floating';
}

export function getOrderId(order: Order, userAccount: PublicKey): string {
	return `${userAccount.toString()}-${order.orderId.toString()}`;
}

export class OrderList {
	marketIndex: BN;
	head?: Node;
	length = 0;
	sortDirection: SortDirection;
	nodeMap = new Map<string, Node>();

	constructor(marketIndex: BN, sortDirection: SortDirection) {
		this.marketIndex = marketIndex;
		this.sortDirection = sortDirection;
	}

	public insert(order: Order, userAccount: PublicKey): void {
		if (isVariant(order.status, 'init')) {
			return;
		}

		const newNode =
			nodeTypeForOrder(order) === 'fixed'
				? new Node(order, userAccount, this.sortDirection)
				: new FloatingNode(order, userAccount, this.sortDirection);

		const orderId = getOrderId(order, userAccount);
		if (this.nodeMap.has(orderId)) {
			return;
		}
		this.nodeMap.set(orderId, newNode);

		this.length += 1;

		if (this.head === undefined) {
			this.head = newNode;
			return;
		}

		if (this.prependNode(this.head, newNode)) {
			this.head.previous = newNode;
			newNode.next = this.head;
			this.head = newNode;
			return;
		}

		let currentNode = this.head;
		while (
			currentNode.next !== undefined &&
			!this.prependNode(currentNode.next, newNode)
		) {
			currentNode = currentNode.next;
		}

		newNode.next = currentNode.next;
		if (currentNode.next !== undefined) {
			newNode.next.previous = newNode;
		}
		currentNode.next = newNode;
		newNode.previous = currentNode;
	}

	prependNode(currentNode: Node, newNode: Node): boolean {
		const currentOrder = currentNode.order;
		const newOrder = newNode.order;

		const currentOrderSortPrice = currentNode.sortPrice;
		const newOrderSortPrice = newNode.sortPrice;

		if (newOrderSortPrice.eq(currentOrderSortPrice)) {
			return newOrder.ts.lt(currentOrder.ts);
		}

		if (this.sortDirection === 'asc') {
			return newOrderSortPrice.lt(currentOrderSortPrice);
		} else {
			return newOrderSortPrice.gt(currentOrderSortPrice);
		}
	}

	public update(order: Order, userAccount: PublicKey): void {
		const orderId = getOrderId(order, userAccount);
		if (this.nodeMap.has(orderId)) {
			const node = this.nodeMap.get(orderId);
			Object.assign(node.order, order);
			node.haveFilled = false;
		}
	}

	public remove(order: Order, userAccount: PublicKey): void {
		const orderId = getOrderId(order, userAccount);
		if (this.nodeMap.has(orderId)) {
			const node = this.nodeMap.get(orderId);
			if (node.next) {
				node.next.previous = node.previous;
			}
			if (node.previous) {
				node.previous.next = node.next;
			}

			if (node.order.orderId.eq(this.head.order.orderId)) {
				this.head = node.next;
			}

			node.previous = undefined;
			node.next = undefined;

			this.nodeMap.delete(orderId);

			this.length--;
		}
	}

	public has(order: Order, userAccount: PublicKey): boolean {
		return this.nodeMap.has(getOrderId(order, userAccount));
	}

	public print(): void {
		let currentNode = this.head;
		while (currentNode !== undefined) {
			console.log(currentNode.getLabel());
			currentNode = currentNode.next;
		}
	}

	public printTop(): void {
		if (this.head) {
			console.log(this.sortDirection.toUpperCase(), this.head.getLabel());
		} else {
			console.log('---');
		}
	}
}

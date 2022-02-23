import {
	AMM_RESERVE_PRECISION,
	convertToNumber,
	isVariant,
	MARK_PRICE_PRECISION,
	Order,
	ZERO,
} from '@drift-labs/sdk';
import { BN } from '@project-serum/anchor';
import { PublicKey } from '@solana/web3.js';

export class Node {
	order: Order;
	userAccount: PublicKey;
	userOrdersAccount: PublicKey;
	sortPrice: BN;
	next?: Node;
	previous?: Node;
	haveFilled = false;
	sortDirection: SortDirection;

	constructor(
		order: Order,
		userAccount: PublicKey,
		orderAccount: PublicKey,
		sortDirection: SortDirection
	) {
		this.order = order;
		this.userAccount = userAccount;
		this.userOrdersAccount = orderAccount;
		this.sortPrice = this.getSortingPrice(order);
		this.sortDirection = sortDirection;
	}

	getSortingPrice(order: Order): BN {
		if (isVariant(order.orderType, 'limit')) {
			return order.price;
		}

		if (isVariant(order.orderType, 'triggerLimit')) {
			if (isVariant(order.triggerCondition, 'below')) {
				if (isVariant(order.direction, 'long')) {
					return order.price.lt(order.triggerPrice)
						? order.price
						: order.triggerPrice;
				}
				return order.triggerPrice;
			} else {
				if (isVariant(order.triggerCondition, 'above')) {
					if (isVariant(order.direction, 'short')) {
						return order.price.gt(order.triggerPrice)
							? order.price
							: order.triggerPrice;
					}
					return order.triggerPrice;
				}
			}
		}

		return order.triggerPrice;
	}

	public pricesCross(markPrice: BN): boolean {
		return this.sortDirection === 'desc'
			? markPrice.lt(this.sortPrice)
			: markPrice.gt(this.sortPrice);
	}

	public getLabel(): string {
		let msg = `Order ${this.order.orderId.toString()}`;
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

export type SortDirection = 'asc' | 'desc';

export class OrderList {
	marketIndex: BN;
	head?: Node;
	length = 0;
	sortDirection: SortDirection;
	nodeMap = new Map<number, Node>();

	constructor(marketIndex: BN, sortDirection: SortDirection) {
		this.marketIndex = marketIndex;
		this.sortDirection = sortDirection;
	}

	public insert(
		order: Order,
		userAccount: PublicKey,
		orderAccount: PublicKey
	): void {
		if (isVariant(order.status, 'init')) {
			return;
		}

		const newNode = new Node(
			order,
			userAccount,
			orderAccount,
			this.sortDirection
		);
		if (this.nodeMap.has(order.orderId.toNumber())) {
			return;
		}
		this.nodeMap.set(order.orderId.toNumber(), newNode);

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

	public update(order: Order): void {
		if (this.nodeMap.has(order.orderId.toNumber())) {
			const node = this.nodeMap.get(order.orderId.toNumber());
			Object.assign(node.order, order);
			node.haveFilled = false;
		}
	}

	public remove(orderId: number): void {
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

	public has(orderId: number): boolean {
		return this.nodeMap.has(orderId);
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

export function sortDirectionForOrder(order: Order): SortDirection {
	if (
		isVariant(order.orderType, 'triggerMarket') ||
		isVariant(order.orderType, 'triggerLimit')
	) {
		return isVariant(order.triggerCondition, 'below') ? 'desc' : 'asc';
	}

	return isVariant(order.direction, 'long') ? 'desc' : 'asc';
}

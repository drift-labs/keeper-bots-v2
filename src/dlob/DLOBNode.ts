import {
	AMM_RESERVE_PRECISION,
	BN,
	convertToNumber,
	getLimitPrice,
	isVariant,
	MARK_PRICE_PRECISION,
	OraclePriceData,
	Order,
	ZERO,
} from '@drift-labs/sdk';
import { PublicKey } from '@solana/web3.js';
import { getOrderId } from './NodeList';

export interface DLOBNode {
	getPrice(oraclePriceData: OraclePriceData, slot: number): BN;
	isVammNode(): boolean;
	order: Order | undefined;
	haveFilled: boolean;
	userAccount: PublicKey | undefined;
}

export abstract class OrderNode implements DLOBNode {
	order: Order;
	userAccount: PublicKey;
	sortValue: BN;
	haveFilled = false;

	constructor(order: Order, userAccount: PublicKey) {
		this.order = order;
		this.userAccount = userAccount;
		this.sortValue = this.getSortValue(order);
	}

	abstract getSortValue(order: Order): BN;

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

	getPrice(oraclePriceData: OraclePriceData, slot: number): BN {
		return getLimitPrice(this.order, oraclePriceData, slot);
	}

	isVammNode(): boolean {
		return false;
	}

	getOrder(): Order {
		return this.order;
	}
}

export class LimitOrderNode extends OrderNode {
	next?: LimitOrderNode;
	previous?: LimitOrderNode;

	getSortValue(order: Order): BN {
		return order.price;
	}
}

export class FloatingLimitOrderNode extends OrderNode {
	next?: FloatingLimitOrderNode;
	previous?: FloatingLimitOrderNode;

	getSortValue(order: Order): BN {
		return order.oraclePriceOffset;
	}
}

export class MarketOrderNode extends OrderNode {
	next?: MarketOrderNode;
	previous?: MarketOrderNode;

	getSortValue(order: Order): BN {
		return order.slot;
	}
}

export type DLOBNodeMap = {
	limit: LimitOrderNode;
	floatingLimit: FloatingLimitOrderNode;
	market: MarketOrderNode;
};

export type DLOBNodeType =
	| 'limit'
	| 'floatingLimit'
	| ('market' & keyof DLOBNodeMap);

export function createNode<T extends keyof DLOBNodeMap>(
	nodeType: T,
	order: Order,
	userAccount: PublicKey
): DLOBNodeMap[T] {
	switch (nodeType) {
		case 'floatingLimit':
			return new FloatingLimitOrderNode(order, userAccount);
		case 'limit':
			return new LimitOrderNode(order, userAccount);
		case 'market':
			return new MarketOrderNode(order, userAccount);
		default:
			throw Error(`Unknown DLOBNode type ${nodeType}`);
	}
}

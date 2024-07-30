import {
	OrderStatus,
	OrderType,
	MarketType,
	PositionDirection,
	OrderTriggerCondition,
	SpotBalanceType,
	NodeToFill,
	DLOBNode,
} from '@drift-labs/sdk';

export type SerializedUserAccount = {
	authority: string;
	delegate: string;
	name: number[];
	subAccountId: number;
	spotPositions: SerializedSpotPosition[];
	perpPositions: SerializedPerpPosition[];
	orders: SerializedOrder[];
	status: number;
	nextLiquidationId: number;
	nextOrderId: number;
	maxMarginRatio: number;
	lastAddPerpLpSharesTs: string;
	settledPerpPnl: string;
	totalDeposits: string;
	totalWithdraws: string;
	totalSocialLoss: string;
	cumulativePerpFunding: string;
	cumulativeSpotFees: string;
	liquidationMarginFreed: string;
	lastActiveSlot: string;
	isMarginTradingEnabled: boolean;
	idle: boolean;
	openOrders: number;
	hasOpenOrder: boolean;
	openAuctions: number;
	hasOpenAuction: boolean;
};

export type SerializedOrder = {
	status: OrderStatus;
	orderType: OrderType;
	marketType: MarketType;
	slot: string;
	orderId: number;
	userOrderId: number;
	marketIndex: number;
	price: string;
	baseAssetAmount: string;
	quoteAssetAmount: string;
	baseAssetAmountFilled: string;
	quoteAssetAmountFilled: string;
	direction: PositionDirection;
	reduceOnly: boolean;
	triggerPrice: string;
	triggerCondition: OrderTriggerCondition;
	existingPositionDirection: PositionDirection;
	postOnly: boolean;
	immediateOrCancel: boolean;
	oraclePriceOffset: number;
	auctionDuration: number;
	auctionStartPrice: string;
	auctionEndPrice: string;
	maxTs: string;
};

export type SerializedSpotPosition = {
	marketIndex: number;
	balanceType: SpotBalanceType;
	scaledBalance: string;
	openOrders: number;
	openBids: string;
	openAsks: string;
	cumulativeDeposits: string;
};

export type SerializedPerpPosition = {
	baseAssetAmount: string;
	lastCumulativeFundingRate: string;
	marketIndex: number;
	quoteAssetAmount: string;
	quoteEntryAmount: string;
	quoteBreakEvenAmount: string;
	openOrders: number;
	openBids: string;
	openAsks: string;
	settledPnl: string;
	lpShares: string;
	remainderBaseAssetAmount: number;
	lastBaseAssetAmountPerLp: string;
	lastQuoteAssetAmountPerLp: string;
	perLpBase: number;
};

export type SerializedNodeToTrigger = {
	node: SerializedTriggerOrderNode;
};

export type SerializedTriggerOrderNode = {
	order: SerializedOrder;
	userAccountData: Buffer;
	userAccount: string;
	sortValue: string;
	haveFilled: boolean;
	haveTrigger: boolean;
};

export type SerializedNodeToFill = {
	fallbackAskSource?: FallbackLiquiditySource;
	fallbackBidSource?: FallbackLiquiditySource;
	node: SerializedDLOBNode;
	makerNodes: SerializedDLOBNode[];
};

export type SerializedDLOBNode = {
	type: string;
	order: SerializedOrder;
	userAccountData: Buffer;
	userAccount: string;
	sortValue: string;
	haveFilled: boolean;
	haveTrigger?: boolean;
	fallbackAskSource?: FallbackLiquiditySource;
	fallbackBidSource?: FallbackLiquiditySource;
};

export type FallbackLiquiditySource = 'serum' | 'phoenix' | 'openbook';
export type NodeToFillWithContext = NodeToFill & {
	fallbackAskSource?: FallbackLiquiditySource;
	fallbackBidSource?: FallbackLiquiditySource;
};

export type NodeToFillWithBuffer = {
	userAccountData: Buffer;
	makerAccountData: string;
	node: DLOBNode;
	fallbackAskSource?: FallbackLiquiditySource;
	fallbackBidSource?: FallbackLiquiditySource;
	makerNodes: DLOBNode[];
};

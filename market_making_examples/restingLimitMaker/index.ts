/**
 * This example makes a single perp market by placing resting limit orders on the DLOB. It constantly updates its orders around
 * the dlob mid price, widening the risk increasing side to prevent reaching max leverage.
 *
 */
import { Connection, PublicKey, TransactionInstruction, ComputeBudgetProgram } from '@solana/web3.js';
import {
	DriftClient,
	initialize,
	SlotSubscriber,
	convertToNumber,
	QUOTE_PRECISION,
	BN,
	DriftEnv,
	UserMap,
	DLOBSubscriber,
	DLOB,
	MarketType,
	PRICE_PRECISION,
	DLOBNode,
	BASE_PRECISION,
	isVariant,
	Order,
	OrderType,
	PositionDirection,
	PostOnlyParams,
	calculateAskPrice,
	calculateBidPrice,
} from '@drift-labs/sdk';
import { logger, setLogLevel } from '../../src/logger';
import { getWallet } from '../../src/utils';

require('dotenv').config();

const intEnvVarWithDefault = (name: string, defaultValue: number): number => {
	const value = process.env[name];
	if (value === undefined) {
		return defaultValue;
	}
	return parseInt(value);
};

const boolEnvVarWithDefault = (name: string, defaultValue: boolean): boolean => {
	const value = process.env[name];
	if (value === undefined) {
		return defaultValue;
	}
	return (value as string).toLowerCase() === "true";
};

/// dry run set to 'true' will not actually place orders
const dryRun = boolEnvVarWithDefault("DRY_RUN", false);
/// enable debug logging
const debug = boolEnvVarWithDefault("DEBUG", false);
if (debug) {
	setLogLevel('debug');
} else {
	setLogLevel('info');
}
/// RPC endpoint
const endpoint = process.env.ENDPOINT;
if (!endpoint) {
	throw new Error('Must set ENDPOINT environment variable');
}
/// drift env (devnet or mainnet-beta)
const driftEnv = (process.env.DRIFT_ENV ?? 'mainnet-beta') as DriftEnv;
/// optional WS endpoint if using a separate server for websockets
const wsEndpoint = process.env.WS_ENDPOINT;
/// the min spread to quote in basis points (3 for 0.03%)
const minSpreadBps = intEnvVarWithDefault("MIN_SPREAD_BPS", 10);
if (!minSpreadBps || minSpreadBps <= 0) {
	throw new Error('Must set MIN_SPREAD_BPS environment variable to be > 0');
}
/// the max spread to quote in basis points (10 for 0.1%)
const maxSpreadBps = intEnvVarWithDefault("MAX_SPREAD_BPS", 20);
if (!maxSpreadBps || maxSpreadBps <= 0) {
	throw new Error('Must set MAX_SPREAD_BPS environment variable to be > 0');
}
/// size of each order on the book (1 SOL-PERP)
const orderSizePerSide = intEnvVarWithDefault("ORDER_SIZE", 1);
const orderSizePerSideBN = new BN(orderSizePerSide).mul(BASE_PRECISION);
/// number of orders to place on each side of the book (careful if too many orders might go over tx size or CU limits)
const ordersPerSide = intEnvVarWithDefault("ORDERS_PER_SIDE", 5);
/// number of ticks between each order on the book
const ticksBetweenOrders = intEnvVarWithDefault("TICKS_BETWEEN_ORDERS", 300);
/// target leverage for this account
const targetLeverage = intEnvVarWithDefault("TARGET_LEVERAGE", 1);
if (!targetLeverage || targetLeverage <= 0) {
	throw new Error('Must set TARGET_LEVERAGE environment variable to be > 0');
}
/// perp market index to market make on
const perpMarketIndex = intEnvVarWithDefault("PERP_MARKET_INDEX", 0);
/// subaccount id to use
const subaccountId = intEnvVarWithDefault("SUBACCOUNT_ID", 0);
/// authority private key or path to key.json (numbers array or b58 string)
const privateKeyOrFilepath = process.env.KEEPER_PRIVATE_KEY;
if (!privateKeyOrFilepath) {
	throw new Error(
		'Must set environment variable KEEPER_PRIVATE_KEY with the path to a id.json, list of commma separated numbers, or b58 encoded private key'
	);
}
const [_, wallet] = getWallet(privateKeyOrFilepath);

console.log("Config:");
console.log(`  endpoint:        ${endpoint}`);
console.log(`  wsEndpoint:      ${wsEndpoint}`);
console.log(`  ordersPerSide:   ${ordersPerSide}`);
console.log(`  targetLeverage:  ${targetLeverage}`);
console.log(`  perpMarketIndex: ${perpMarketIndex}`);
console.log(`  subaccountId:    ${subaccountId}`);
console.log(`  authority:       ${wallet.publicKey.toBase58()}`);
console.log("");

const stateCommitment = "confirmed";

// @ts-ignore
const sdkConfig = initialize({ env: driftEnv });

const loadUserPortfolio = (driftClient: DriftClient): {
	currLeverage: number,
	accountUsdValue: number,
	currPositionbase: number,
} => {

	const driftUser = driftClient.getUser();

	const netSpotValue = convertToNumber(
		driftUser.getNetSpotMarketValue(),
		QUOTE_PRECISION
	);
	const unrealizedPnl = convertToNumber(
		driftUser.getUnrealizedPNL(true, undefined, undefined),
		QUOTE_PRECISION
	);
	const accountUsdValue = netSpotValue + unrealizedPnl;
	const currLeverage = convertToNumber(
		driftUser.getLeverage(),
		new BN(10000)
	);

	let currPositionbase = 0;
	const currentPerpPosition = driftUser.getPerpPosition(perpMarketIndex);
	if (currentPerpPosition) {
		currPositionbase = convertToNumber(currentPerpPosition.baseAssetAmount, BASE_PRECISION);
	}

	return {
		currLeverage,
		accountUsdValue,
		currPositionbase,
	};
};

const calculateBaseBidAsk = (dlobMidPrice: number, currLeverage: number, currentBasePosition: number): {
	baseBidPrice: number,
	baseAskPrice: number,
} => {

	// the spread is at its minimum when the current_leverage is 0 and increases linearly
	// with the leverage until it reaches max_spread when current_leverage equals target_leverage.
	const spreadDiffBps = minSpreadBps + (maxSpreadBps - minSpreadBps) * (currLeverage / targetLeverage);

	if (currentBasePosition == 0) {
		// no position, quote tightest
		return {
			baseBidPrice: dlobMidPrice * (1 - spreadDiffBps / 10000.0),
			baseAskPrice: dlobMidPrice * (1 + spreadDiffBps / 10000.0),
		};
	} else if (currentBasePosition > 0) {
		// currently long quote wider on the bid
		return {
			baseBidPrice: dlobMidPrice * (1 - spreadDiffBps / 10000.0),
			baseAskPrice: dlobMidPrice * (1 + minSpreadBps / 10000.0),
		};
	} else if (currentBasePosition < 0) {
		// currently short quote wider on the ask
		return {
			baseBidPrice: dlobMidPrice * (1 - minSpreadBps / 10000.0),
			baseAskPrice: dlobMidPrice * (1 + spreadDiffBps / 10000.0),
		};
	} else {
		throw new Error("Invalid position");
	}
};


const needUpdateOrders = (expectedOpenSize: BN, openBids: BN, openAsks: BN, dlobMidPrice: number, myBaseBid: number, myBaseAsk: number, vBid: number, vAsk: number): boolean => {
	// update orders if any orders have been completely filled
	if (!openBids.abs().eq(expectedOpenSize)) {
		logger.info("Requoting due to filled bids");
		return true;
	}
	if (!openAsks.abs().eq(expectedOpenSize)) {
		logger.info("Requoting due to filled ask order");
		return true;
	}

	// update orders if the dlob midprice is less than min spread from our base bid/ask
	const bidThreshold = myBaseBid * (1 + minSpreadBps / 2 / 10000.0);
	if (dlobMidPrice < bidThreshold) {
		logger.info("Requoting due to dlobMidPrice approaching bid");
		return true;
	}
	const askThreshold = myBaseAsk * (1 - minSpreadBps / 2 / 10000.0);
	if (dlobMidPrice > askThreshold) {
		logger.info("Requoting due to dlobMidPrice approaching ask");
		return true;
	}

	// dont want vamm to cross us
	if (vBid > askThreshold) {
		logger.info(`Requoting due to vBid (${vBid}) approaching askThres (${askThreshold})`);
		return true;
	}
	if (vAsk < bidThreshold) {
		logger.info(`Requoting due to vAsk (${vAsk}) approaching bidThres (${bidThreshold})`);
		return true;
	}


	return false;
};

/**
 * Places orders on both sides of the book, starting at base*price, and increasing by ticksBetweenOrders
 * until ordersPerSide are placed.
 * @param driftClient
 * @param baseBidPrice
 * @param baseAskPrice
 */
const updateOrders = async (driftClient: DriftClient, baseBidPrice: number, baseAskPrice: number) => {
	// load open bids and asks from chain state
	const openBids: Array<Order> = [];
	const openAsks: Array<Order> = [];
	for (const order of driftClient.getUserAccount()!.orders) {
		if (isVariant(order.status, "init")) {
			continue;
		}

		if (isVariant(order.direction, "long")) {
			openBids.push(order);
		} else {
			openAsks.push(order);
		}
	}

	// reduce, resuse, recycle open order slots
	const orderExpireTs = new BN((Date.now()) / 1000 + 30); // new order will expire in 30s
	const marketTick = convertToNumber(driftClient.getPerpMarketAccount(perpMarketIndex)!.amm.orderTickSize, PRICE_PRECISION);
	const ixs: Array<TransactionInstruction> = [];
	ixs.push(
		ComputeBudgetProgram.setComputeUnitLimit({
			units: 2_000_010,
		})
	);
	let reusedOrderSlots = 0;
	let newOrders = 0;
	const bidsPlaced: Array<string> = [];
	const asksPlaced: Array<string> = [];
	for (let i = 0; i < ordersPerSide; i++) {
		const priceDelta = i * ticksBetweenOrders * marketTick;

		// update bids
		const bidlevelPrice = baseBidPrice - priceDelta;
		const bidPriceBN = new BN(bidlevelPrice * PRICE_PRECISION);
		bidsPlaced.push(bidlevelPrice.toFixed(4));
		if (i + 1 <= openBids.length) {
			// reuse existing bid
			const openBid = openBids[i];
			const ops = {
				price: bidPriceBN,
				baseAssetAmount: orderSizePerSideBN,
				maxTs: orderExpireTs,
				direction: null,
				oraclePriceOffset: null,
				triggerPrice: null,
				triggerCondition: null,
				auctionDuration: null,
				auctionStartPrice: null,
				auctionEndPrice: null,
				reduceOnly: null,
				postOnly: null,
				immediateOrCancel: null,
			};
			ixs.push(await driftClient.getModifyOrderIx(openBid.orderId, ops));
			reusedOrderSlots++;
		} else {
			// place a new bid
			ixs.push(await driftClient.getPlacePerpOrderIx({
				orderType: OrderType.LIMIT,
				marketIndex: perpMarketIndex,
				baseAssetAmount: orderSizePerSideBN,
				price: bidPriceBN,
				direction: PositionDirection.LONG,
				postOnly: PostOnlyParams.MUST_POST_ONLY,
				maxTs: orderExpireTs,
			}));
			newOrders++;
		}

		// update asks
		const asklevelPrice = baseAskPrice + priceDelta;
		const askPriceBN = new BN(asklevelPrice * PRICE_PRECISION);
		asksPlaced.push(asklevelPrice.toFixed(4));
		if (i + 1 <= openAsks.length) {
			// reuse existing ask
			const openAsk = openAsks[i];
			const ops = {
				price: askPriceBN,
				baseAssetAmount: orderSizePerSideBN,
				maxTs: orderExpireTs,
				direction: null,
				oraclePriceOffset: null,
				triggerPrice: null,
				triggerCondition: null,
				auctionDuration: null,
				auctionStartPrice: null,
				auctionEndPrice: null,
				reduceOnly: null,
				postOnly: null,
				immediateOrCancel: null,
			};
			ixs.push(await driftClient.getModifyOrderIx(openAsk.orderId, ops));
			reusedOrderSlots++;
		} else {
			// place a new ask
			ixs.push(await driftClient.getPlacePerpOrderIx({
				orderType: OrderType.LIMIT,
				marketIndex: perpMarketIndex,
				baseAssetAmount: orderSizePerSideBN,
				price: askPriceBN,
				direction: PositionDirection.SHORT,
				postOnly: PostOnlyParams.MUST_POST_ONLY,
				maxTs: orderExpireTs,
			}));
			newOrders++;
		}
	}

	logger.info(`Update orders ixs: ${ixs.length}, reusedOrderSlots: ${reusedOrderSlots}, newOrders: ${newOrders}`);
	logger.info(`BidsPlaced: ${bidsPlaced}`);
	logger.info(`AsksPlaced: ${asksPlaced}`);

	if (!dryRun) {
		try {
			const lookupTableAccount = await driftClient.fetchMarketLookupTableAccount();
			const vTx = await driftClient.txSender.getVersionedTransaction(ixs, [lookupTableAccount], [], driftClient.opts);
			const tx = await driftClient.txSender.sendVersionedTransaction(vTx, [], driftClient.opts);
			logger.info(`Update orders tx (slot: ${tx.slot}): ${tx.txSig}`);
		} catch (e) {
			console.error(e);
		}
	}
};

const main = async () => {

	const connection = new Connection(endpoint, {
		wsEndpoint: wsEndpoint,
		commitment: stateCommitment,
	});

	const driftClient = new DriftClient({
		connection,
		wallet,
		programID: new PublicKey(sdkConfig.DRIFT_PROGRAM_ID),
		opts: {
			commitment: stateCommitment,
			skipPreflight: false,
			preflightCommitment: stateCommitment,
		},
		accountSubscription: {
			type: 'websocket',
		},
		env: driftEnv,
		txSenderConfig: {
			type: 'retry',
			timeout: 35000,
		},
		activeSubAccountId: subaccountId,
		subAccountIds: [subaccountId],
	});
	await driftClient.subscribe();
	await driftClient.fetchMarketLookupTableAccount();

	const slotSubscriber = new SlotSubscriber(connection, {});
	await slotSubscriber.subscribe();

	const userMap = new UserMap(
		driftClient,
		driftClient.userAccountSubscriptionConfig,
		false
	);
	await userMap.subscribe();

	const dlobSubscriber = new DLOBSubscriber({
		driftClient,
		dlobSource: userMap,
		slotSource: slotSubscriber,
		updateFrequency: 1000,
	});
	await dlobSubscriber.subscribe();


	const { currLeverage, accountUsdValue } = loadUserPortfolio(driftClient);
	logger.info(`User account value: ${accountUsdValue}, current leverage: ${currLeverage}`);

	// run the market making loop on each update
	let updatInProgress = false;
	dlobSubscriber.eventEmitter.on('update', async (dlob: DLOB) => {
		if (updatInProgress) return;
		updatInProgress = true;
		const start = Date.now();
		const slot = slotSubscriber.getSlot();
		const oracleData = driftClient.getOracleDataForPerpMarket(perpMarketIndex);

		const perpMarketAccount = driftClient.getPerpMarketAccount(perpMarketIndex)!;
		const vAsk = calculateAskPrice(perpMarketAccount, oracleData);
		const vBid = calculateBidPrice(perpMarketAccount, oracleData);
		const vAskNum = convertToNumber(vAsk, PRICE_PRECISION);
		const vBidNum = convertToNumber(vBid, PRICE_PRECISION);

		const bestBidNode = (dlob.getMakerLimitBids(perpMarketIndex, slot, MarketType.PERP, oracleData).next().value as DLOBNode);
		const dlobBid = bestBidNode.getPrice(oracleData, slot);
		const dlobBidNum = convertToNumber(dlobBid, PRICE_PRECISION);
		const bestBid = BN.min(vBid, dlobBid);

		const bestAskNode = (dlob.getMakerLimitAsks(perpMarketIndex, slot, MarketType.PERP, oracleData).next().value as DLOBNode);
		const dlobAsk = bestAskNode.getPrice(oracleData, slot);
		const dlobAskNum = convertToNumber(dlobAsk, PRICE_PRECISION);
		const bestAsk = BN.max(vAsk, dlobAsk);

		const midPrice = bestBid.add(bestAsk).div(new BN(2));

		const bidNum = convertToNumber(bestBid, PRICE_PRECISION);
		const askNum = convertToNumber(bestAsk, PRICE_PRECISION);
		const midPriceNum = convertToNumber(midPrice, PRICE_PRECISION);
		const bidPct = (bidNum / midPriceNum - 1.00) * 100.0;
		const askPct = (midPriceNum / askNum - 1.00) * 100.0;
		const spreadPct = (askNum / bidNum - 1.00) * 100.0;

		const { currLeverage, currPositionbase } = loadUserPortfolio(driftClient);

		const { baseBidPrice, baseAskPrice } = calculateBaseBidAsk(
			midPriceNum,
			currLeverage,
			currPositionbase);
		logger.debug(` . dlobPcts bid: ${bidPct.toFixed(4)}% : ask: (${askPct.toFixed(4)}%), spread: ${spreadPct}%`);
		logger.debug(` . dlobBid:      ${dlobBidNum.toFixed(6)}; \t\tdlobAsk:      ${dlobAskNum.toFixed(6)}`);
		logger.debug(` . vBid:         ${vBidNum.toFixed(6)}; \t\tvAsk:         ${vAskNum.toFixed(6)}`);
		logger.debug(` . midPrice: ${midPriceNum}`);
		logger.debug(` . baseBidPrice: ${baseBidPrice.toFixed(6)}; \t\tbaseAskPrice: ${baseAskPrice.toFixed(6)}`);

		const driftUser = driftClient.getUser();
		const openBids = driftUser.getPerpPosition(perpMarketIndex)!.openBids;
		const openAsks = driftUser.getPerpPosition(perpMarketIndex)!.openAsks;
		const expectedOpenSize = new BN(ordersPerSide).mul(orderSizePerSideBN);
		logger.debug(` . openBids: ${convertToNumber(openBids, BASE_PRECISION)}; openAsks: ${convertToNumber(openAsks, BASE_PRECISION)}, expectedOpenSize: ${convertToNumber(expectedOpenSize, BASE_PRECISION)}`);

		if (needUpdateOrders(expectedOpenSize, openBids, openAsks, midPriceNum, baseBidPrice, baseAskPrice, vBidNum, vAskNum)) {
			await updateOrders(driftClient, baseBidPrice, baseAskPrice);
		}


		updatInProgress = false;
		logger.debug('market making loop took: ' + (Date.now() - start) + 'ms');
	});
};

main();
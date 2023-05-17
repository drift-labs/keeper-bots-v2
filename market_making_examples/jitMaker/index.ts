/**
 * This example makes a single perp market by placing resting limit orders on the DLOB. It constantly updates its orders around
 * the dlob mid price, widening the risk increasing side to prevent reaching max leverage.
 *
 */
import { Connection, PublicKey } from '@solana/web3.js';
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
	MarketType,
	BASE_PRECISION,
	getVariant,
	PRICE_PRECISION,
	PositionDirection,
	UserStatsMap,
	OrderType,
	PostOnlyParams,
	calculateAskPrice,
	calculateBidPrice,
	isVariant,
	UserStats,
	Order,
	UserAccount,
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
/// target leverage for this account
const targetLeverage = intEnvVarWithDefault("TARGET_LEVERAGE", 1);
if (targetLeverage === undefined) {
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

const canIncreaseRisk = (currLeverage: number): boolean => {
	return currLeverage < targetLeverage;
};

const jitOrderIncreasesRisk = (currPositionBase: number, orderDirection: PositionDirection): boolean => {
	// test opposite directions since our jit order we will be on the other side
	return (isVariant(orderDirection, 'short') && currPositionBase > 0) || (isVariant(orderDirection, 'long') && currPositionBase < 0);
};

const executeJitOrder = async (
	driftClient: DriftClient,
	order: Order,
	taker: PublicKey,
	takerUserAccount: UserAccount,
	takerStatsInfo: UserStats,
	fillSize: BN,
	fillPrice: BN,
) => {
	const fillDirection = isVariant(order.direction, 'long') ? PositionDirection.SHORT : PositionDirection.LONG;
	logger.info(`Filling jit order: ${getVariant(fillDirection)} ${convertToNumber(fillSize, BASE_PRECISION)} ${convertToNumber(fillPrice, PRICE_PRECISION)}`);

	if (dryRun) {
		logger.info(`DRY RUN: not placing order.`);
		return;
	}

	const tx = await driftClient.placeAndMakePerpOrder({
		orderType: OrderType.LIMIT,
		marketIndex: perpMarketIndex,
		baseAssetAmount: fillSize,
		price: fillPrice,
		direction: fillDirection,
		immediateOrCancel: true,
		postOnly: PostOnlyParams.MUST_POST_ONLY,
	}, {
		taker,
		takerStats: takerStatsInfo.userStatsAccountPublicKey,
		takerUserAccount: takerUserAccount,
		order,
	},
		takerStatsInfo.getReferrerInfo());
	logger.info(tx);
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
	const userStatsMap = new UserStatsMap(
		driftClient,
		driftClient.userAccountSubscriptionConfig
	);
	await userStatsMap.subscribe();

	const dlobSubscriber = new DLOBSubscriber({
		driftClient,
		dlobSource: userMap,
		slotSource: slotSubscriber,
		updateFrequency: 1000,
	});
	await dlobSubscriber.subscribe();

	const perpMarketAccount = driftClient.getPerpMarketAccount(perpMarketIndex);
	if (!perpMarketAccount) {
		throw new Error(`No perp market account found for index ${perpMarketIndex}`);
	}

	const { currLeverage, accountUsdValue, currPositionbase: currPositionBase } = loadUserPortfolio(driftClient);
	logger.info(`User account value: ${accountUsdValue}, current leverage: ${currLeverage}, currBasePosition: ${currPositionBase}`);

	// run the market making loop on each update
	let updatInProgress = false;
	slotSubscriber.eventEmitter.on('newSlot', async (currSlot: number) => {
		if (updatInProgress) return;
		updatInProgress = true;
		const start = Date.now();
		const oracleData = driftClient.getOracleDataForPerpMarket(perpMarketIndex);

		const dlobUpdateStart = Date.now();
		await dlobSubscriber.updateDLOB();
		const dlob = dlobSubscriber.getDLOB();
		const dlobUpdateDur = Date.now() - dlobUpdateStart;

		const jitNodes = dlob.findJitAuctionNodesToFill(perpMarketIndex, currSlot, oracleData, MarketType.PERP);
		logger.info(`Found ${jitNodes.length} jit nodes for slot: ${currSlot}, dlobupdate took : ${dlobUpdateDur}ms`);
		for (let i = 0; i < jitNodes.length; i++) {
			const jitNode = jitNodes[i];
			const taker = jitNode.node.userAccount;
			if (!taker) {
				logger.error(`empty taker: ${JSON.stringify(jitNode)}`);
				continue;
			}
			const order = jitNode.node.order;
			if (!order) {
				logger.error(`empty order: ${JSON.stringify(jitNode)}`);
				continue;
			}

			const vAsk = calculateAskPrice(perpMarketAccount, oracleData);
			const vBid = calculateBidPrice(perpMarketAccount, oracleData);
			const vAskNum = convertToNumber(vAsk, PRICE_PRECISION);
			const vBidNum = convertToNumber(vBid, PRICE_PRECISION);
			const orderPrice = jitNode.node.getPrice(oracleData, currSlot);
			const orderPrice1 = jitNode.node.getPrice(oracleData, currSlot + 1);
			const orderPrice2 = jitNode.node.getPrice(oracleData, currSlot + 2);
			logger.info(` [${i}]: ${taker?.toBase58()}-${order.orderId}; currSlot: ${currSlot}, slotsLeft: ${(order.slot.toNumber() + order.auctionDuration) - currSlot}; ${getVariant(order.orderType)} ${getVariant(order.direction)} ${convertToNumber(order.baseAssetAmount.sub(order.baseAssetAmountFilled), BASE_PRECISION)} @ ${convertToNumber(orderPrice, PRICE_PRECISION)},${convertToNumber(orderPrice1, PRICE_PRECISION)},${convertToNumber(orderPrice2, PRICE_PRECISION)} vBid: ${vBidNum}, vAsk: ${vAskNum}`);

			// just fill the min trade size
			const takerAuthority = userMap.getUserAuthority(taker.toBase58());
			if (!takerAuthority) {
				logger.error(`empty takerAuthority: ${JSON.stringify(jitNode)}`);
				continue;
			}
			const takerUser = userMap.get(taker.toBase58());
			if (!takerUser) {
				logger.error(`empty takerUserAccount: ${JSON.stringify(jitNode)}`);
				continue;
			}
			const fillSize = perpMarketAccount.amm.minOrderSize;
			const takerStatsInfo = await userStatsMap.mustGet(takerAuthority.toBase58());
			if (jitOrderIncreasesRisk(currPositionBase, order.direction)) {
				if (canIncreaseRisk(currLeverage)) {
					await executeJitOrder(driftClient, order, taker, takerUser.getUserAccount(), takerStatsInfo, fillSize, orderPrice);
				} else {
					logger.info('max leverage reached, cannot increase risk anymore');
				}
			} else {
				await executeJitOrder(driftClient, order, taker, takerUser.getUserAccount(), takerStatsInfo, fillSize, orderPrice);
			}
		}

		updatInProgress = false;
		logger.debug('market making loop took: ' + (Date.now() - start) + 'ms');
	});
};

main();
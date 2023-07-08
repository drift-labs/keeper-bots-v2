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
	AuctionSubscriber, getUserStatsAccountPublicKey, getLimitPrice, hasAuctionPrice, isAuctionComplete,
} from '@drift-labs/sdk';
import { logger, setLogLevel } from '../../src/logger';
import { getWallet } from '../../src/utils';
import { JitProxyClient } from '../../../jit-proxy/ts/sdk/lib';

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
if (targetLeverage < 0) {
	throw new Error('Must set TARGET_LEVERAGE environment variable to be > 0');
}
/// perp market index to market make on
const perpMarketIndex = intEnvVarWithDefault("PERP_MARKET_INDEX", 0);
/// subaccount id to use
const subaccountId = intEnvVarWithDefault("SUBACCOUNT_ID", 2);
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
		activeSubAccountId: subaccountId,
		subAccountIds: [subaccountId],
	});
	await driftClient.subscribe();
	await driftClient.fetchMarketLookupTableAccount();

	const jitProxy = new JitProxyClient({
		// @ts-ignore
		driftClient,
		programId: new PublicKey("J1TnP8zvVxbtF5KFp5xRmWuvG9McnhzmBd9XGfCyuxFP"),
	});

	const slotSubscriber = new SlotSubscriber(connection, {});
	await slotSubscriber.subscribe();

	const auctionSubscriber = new AuctionSubscriber({driftClient});
	await auctionSubscriber.subscribe();
	auctionSubscriber.eventEmitter.on('onAccountUpdate', async (taker, takerKey, slot) => {
		const takerStatsKey = getUserStatsAccountPublicKey(driftClient.program.programId, taker.authority);
		for (const order of taker.orders) {
			if (!isVariant(order.status, 'open')) {
				continue;
			}

			if (!hasAuctionPrice(order, slot)) {
				continue;
			}

			if (!isVariant(order.marketType, 'perp')) {
				continue;
			}

			if (order.marketIndex !== 0) {
				continue;
			}

			if (isAuctionComplete(order, slot)) {
				continue;
			}

			const limitPrice = getLimitPrice(order, driftClient.getOracleDataForPerpMarket(order.marketIndex), slot);

			console.log(`Filler order for: ${takerKey.toBase58()} slot ${slot} price ${convertToNumber(limitPrice, PRICE_PRECISION)}`);

			jitProxy.jit({
				takerKey,
				takerStatsKey,
				taker,
				takerOrderId: order.orderId,
				maxPosition: isVariant(order.direction, 'long') ? new BN(-1000).mul(BASE_PRECISION) : new BN(-1000).mul(BASE_PRECISION),
				worstPrice: isVariant(order.direction, 'long') ? new BN(0) : new BN(1000000).mul(PRICE_PRECISION),
				postOnly: null
			}).then(txSig => {
				console.log(txSig);
			}).catch(e => {
				console.error(e);
			});
		}
	});
};

main();
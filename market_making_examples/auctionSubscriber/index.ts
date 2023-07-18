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
import {sleep} from "jito-ts/dist/sdk/rpc/utils";

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

	const jitter = new Jitter({
		auctionSubscriber,
		driftClient,
		jitProxyClient: jitProxy,
	});

	await jitter.subscribe();

	jitter.setUserFilter((userAccount, userKey) => {
		return userKey === '4kojmr5Xbgrfgg5db9bdEuVrDtb1kjBCcNGHsv8S2TdZ';
	});

	while (true) {
		await sleep(5000);
		const solOracle = jitter.driftClient.getOracleDataForPerpMarket(0);
		jitter.updatePerpParams(0, {
			maxPosition: new BN(200).mul(BASE_PRECISION),
			minPosition: new BN(-200).mul(BASE_PRECISION),
			bid: solOracle.price.sub(solOracle.price.divn(500)),
			ask: solOracle.price.add(solOracle.price.divn(500)),
		});
	}
};

type UserFilter = (userAccount: UserAccount, userKey: string) => boolean;
type JitParams = {bid: BN, ask: BN, minPosition: BN, maxPosition};

class Jitter {
	auctionSubscriber: AuctionSubscriber;
	driftClient: DriftClient;
	jitProxyClient: JitProxyClient;

	perpParams = new Map<number, JitParams>();
	spotParams = new Map<number, JitParams>();

	onGoingAuctions = new Map<string, Promise<void>>();

	userFilter : UserFilter;

	constructor({
		auctionSubscriber,
		jitProxyClient,
		driftClient,
				} : {
		driftClient: DriftClient,
		auctionSubscriber: AuctionSubscriber,
		jitProxyClient: JitProxyClient,
	}) {
		this.auctionSubscriber = auctionSubscriber;
		this.driftClient = driftClient;
		this.jitProxyClient = jitProxyClient;
	}

	async subscribe() {
		await this.auctionSubscriber.subscribe();
		this.auctionSubscriber.eventEmitter.on('onAccountUpdate', async (taker, takerKey, slot) => {
			const takerKeyString = takerKey.toBase58();

			if (this.userFilter) {
				if (this.userFilter(taker, takerKeyString)) {
					return;
				}
			}

			const takerStatsKey = getUserStatsAccountPublicKey(this.driftClient.program.programId, taker.authority);
			for (const order of taker.orders) {
				if (!isVariant(order.status, 'open')) {
					continue;
				}

				if (!hasAuctionPrice(order, slot)) {
					continue;
				}


				const orderSignature = this.getOrderSignatures(takerKeyString, order.orderId);
				if (this.onGoingAuctions.has(orderSignature)) {
					continue;
				}


				if (isVariant(order.marketType, 'perp')) {
					if (!this.perpParams.has(order.marketIndex)) {
						return;
					}

					const promise = this.createTryFill(taker, takerKey, takerStatsKey, order, orderSignature).bind(this)();
					this.onGoingAuctions.set(orderSignature, promise);
				} else {
					if (!this.spotParams.has(order.marketIndex)) {
						return;
					}

					const promise = this.createTryFill(taker, takerKey, takerStatsKey, order, orderSignature).bind(this)();
					this.onGoingAuctions.set(orderSignature, promise);
				}
			}
		});
	}

	createTryFill(taker: UserAccount, takerKey: PublicKey, takerStatsKey: PublicKey, order: Order, orderSignature: string) {
		return async () => {
			let i = 0;
			while (i < 10) {
				const params = this.perpParams.get(order.marketIndex);
				if (!params) {
					this.onGoingAuctions.delete(orderSignature);
					return;
				}

				console.log(`Trying to fill ${orderSignature}`);
				try {
					const { txSig } = await this.jitProxyClient.jit({
						takerKey,
						takerStatsKey,
						taker,
						takerOrderId: order.orderId,
						maxPosition: params.maxPosition,
						minPosition: params.minPosition,
						bid: params.bid,
						ask: params.ask,
						postOnly: null
					});

					console.log(`Filled ${orderSignature} txSig ${txSig}`);
					await sleep(10000);
					this.onGoingAuctions.delete(orderSignature);
					return;
				} catch (e) {
					console.error(`Failed to fill ${orderSignature}`);
					if (e.message.includes('0x1770') || e.message.includes('0x1771')) {
						console.log('Order does not cross params yet, retrying');
					} else {
						await sleep(10000);
						this.onGoingAuctions.delete(orderSignature);
						return;
					}
				}
				i++;
			}

			this.onGoingAuctions.delete(orderSignature);
		};
	}

	getOrderSignatures(takerKey: string, orderId: number) {
		return `${takerKey}-${orderId}`;
	}

	public updatePerpParams(marketIndex: number, params: JitParams) {
		this.perpParams.set(marketIndex, params);
	}

	public updateSpotParams(marketIndex: number, params: JitParams) {
		this.spotParams.set(marketIndex, params);
	}

	public setUserFilter(userFilter: UserFilter | undefined) {
		this.userFilter = userFilter;
	}
}

main();
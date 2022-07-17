import { Connection, Keypair, PublicKey } from '@solana/web3.js';

import {
	BN,
	BulkAccountLoader,
	ClearingHouse,
	initialize,
	isVariant,
	Markets,
	OrderRecord,
	convertToNumber,
	MARK_PRICE_PRECISION,
	Wallet,
	calculateBaseAssetAmountMarketCanExecute,
	calculateAskPrice,
	calculateBidPrice,
	isOracleValid,
	DriftEnv,
	MarketAccount,
	UserAccount,
	EventSubscriber,
	Order,
	OrderAction,
	SlotSubscriber,
	ClearingHouseUser,
	bulkPollingUserSubscribe,
} from '@drift-labs/sdk';

import { getErrorCode } from './error';
import { DLOB } from './dlob/DLOB';
import { ProgramAccount } from '@project-serum/anchor';

require('dotenv').config();
const driftEnv = process.env.ENV as DriftEnv;
//@ts-ignore
const sdkConfig = initialize({ env: process.env.ENV });

export function getWallet(): Wallet {
	const privateKey = process.env.FILLER_PRIVATE_KEY;
	const keypair = Keypair.fromSecretKey(
		Uint8Array.from(privateKey.split(',').map((val) => Number(val)))
	);
	return new Wallet(keypair);
}

const endpoint = process.env.ENDPOINT;
const connection = new Connection(endpoint);

const wallet = getWallet();
const clearingHousePublicKey = new PublicKey(
	sdkConfig.CLEARING_HOUSE_PROGRAM_ID
);

const bulkAccountLoader = new BulkAccountLoader(connection, 'confirmed', 500);
const clearingHouse = new ClearingHouse({
	connection,
	wallet,
	programID: clearingHousePublicKey,
	accountSubscription: {
		type: 'polling',
		accountLoader: bulkAccountLoader,
	},
	env: driftEnv,
});

const eventSubscriber = new EventSubscriber(connection, clearingHouse.program, {
	maxTx: 8192,
	maxEventsPerType: 4096,
	orderBy: 'blockchain',
	orderDir: 'desc',
	commitment: 'confirmed',
	logProviderConfig: {
		type: 'websocket',
	},
});

const slotSubscriber = new SlotSubscriber(connection, {});

const intervalIds = [];
const runBot = async (wallet: Wallet, clearingHouse: ClearingHouse) => {
	const lamportsBalance = await connection.getBalance(wallet.publicKey);
	console.log('SOL balance:', lamportsBalance / 10 ** 9);
	await clearingHouse.subscribe();
	clearingHouse.eventEmitter.on('error', (e) => {
		console.log('clearing house error');
		console.error(e);
	});

	eventSubscriber.subscribe();
	await slotSubscriber.subscribe();

	const dlob = new DLOB(
		clearingHouse
			.getMarketAccounts()
			.map((marketAccount) => marketAccount.marketIndex)
	);
	const programAccounts = await clearingHouse.program.account.user.all();
	for (const programAccount of programAccounts) {
		// @ts-ignore
		const userAccount: UserAccount = programAccount.account;
		const userAccountPublicKey = programAccount.publicKey;

		for (const order of userAccount.orders) {
			dlob.insert(order, userAccountPublicKey);
		}
	}
	console.log('initial number of orders', dlob.openOrders.size);

	const printTopOfOrderLists = (marketIndex: BN) => {
		const market = clearingHouse.getMarketAccount(marketIndex);

		const slot = slotSubscriber.getSlot();
		const oraclePriceData = clearingHouse.getOracleDataForMarket(marketIndex);
		const vAsk = calculateAskPrice(market, oraclePriceData);
		const vBid = calculateBidPrice(market, oraclePriceData);
		const vMid = vAsk.add(vBid).div(new BN(2));

		const bestAsk = dlob.getBestAsk(marketIndex, vAsk, slot, oraclePriceData);
		const bestBid = dlob.getBestBid(marketIndex, vBid, slot, oraclePriceData);

		console.log(`Market ${Markets[marketIndex.toNumber()].symbol} Orders`);
		console.log(`vAsk`, convertToNumber(vAsk, MARK_PRICE_PRECISION).toFixed(3));
		console.log(
			`Ask`,
			convertToNumber(bestAsk, MARK_PRICE_PRECISION).toFixed(3)
		);
		console.log(`vMid`, convertToNumber(vMid, MARK_PRICE_PRECISION).toFixed(3));
		console.log(
			`Bid`,
			convertToNumber(bestBid, MARK_PRICE_PRECISION).toFixed(3)
		);
		console.log(`vBid`, convertToNumber(vBid, MARK_PRICE_PRECISION).toFixed(3));
	};

	const printOrderLists = () => {
		for (const marketAccount of clearingHouse.getMarketAccounts()) {
			printTopOfOrderLists(marketAccount.marketIndex);
		}
	};
	printOrderLists();

	const userAccountLoader = new BulkAccountLoader(
		connection,
		'processed',
		5000
	);

	const userMap = new Map<string, ClearingHouseUser>();
	const fetchAllUsers = async () => {
		const programUserAccounts =
			(await clearingHouse.program.account.user.all()) as ProgramAccount<UserAccount>[];
		const userArray: ClearingHouseUser[] = [];
		for (const programUserAccount of programUserAccounts) {
			if (userMap.has(programUserAccount.publicKey.toString())) {
				continue;
			}

			const user = new ClearingHouseUser({
				clearingHouse,
				userAccountPublicKey: programUserAccount.publicKey,
				accountSubscription: {
					type: 'polling',
					accountLoader: userAccountLoader,
				},
			});
			userArray.push(user);
		}

		await bulkPollingUserSubscribe(userArray, userAccountLoader);
		for (const user of userArray) {
			const userAccountPubkey = await user.getUserAccountPublicKey();
			userMap.set(userAccountPubkey.toString(), user);
		}
	};
	await fetchAllUsers();

	const addToUserMap = async (userAccountPublicKey: PublicKey) => {
		const user = new ClearingHouseUser({
			clearingHouse,
			userAccountPublicKey,
			accountSubscription: {
				type: 'polling',
				accountLoader: userAccountLoader,
			},
		});
		await user.subscribe();
		userMap.set(userAccountPublicKey.toString(), user);
	};

	const handleOrderRecord = async (record: OrderRecord) => {
		if (!record.taker.equals(PublicKey.default)) {
			handleOrder(record.takerOrder, record.taker, record.action);
		}

		if (
			!record.taker.equals(PublicKey.default) &&
			!userMap.has(record.taker.toString())
		) {
			await addToUserMap(record.taker);
		}

		if (!record.maker.equals(PublicKey.default)) {
			handleOrder(record.makerOrder, record.maker, record.action);
		}

		if (
			!record.maker.equals(PublicKey.default) &&
			!userMap.has(record.maker.toString())
		) {
			await addToUserMap(record.maker);
		}

		printTopOfOrderLists(record.marketIndex);
	};

	const handleOrder = (
		order: Order,
		userAccount: PublicKey,
		action: OrderAction
	) => {
		if (isVariant(action, 'place')) {
			dlob.insert(order, userAccount, () => {
				console.log(
					`Order ${dlob.getOpenOrderId(
						order,
						userAccount
					)} placed. Added to dlob`
				);
			});
		} else if (isVariant(action, 'cancel')) {
			dlob.remove(order, userAccount, () => {
				console.log(
					`Order ${dlob.getOpenOrderId(
						order,
						userAccount
					)} canceled. Removed from dlob`
				);
			});
		} else if (isVariant(action, 'fill')) {
			if (order.baseAssetAmount.eq(order.baseAssetAmountFilled)) {
				dlob.remove(order, userAccount, () => {
					console.log(
						`Order ${dlob.getOpenOrderId(
							order,
							userAccount
						)} completely filled. Removed from dlob`
					);
				});
			} else {
				dlob.update(order, userAccount, () => {
					console.log(
						`Order ${dlob.getOpenOrderId(
							order,
							userAccount
						)} partially filled. Updated dlob`
					);
				});
			}
		}
	};

	eventSubscriber.eventEmitter.on('newEvent', (event) => {
		if (event.eventType === 'OrderRecord') {
			handleOrderRecord(event as OrderRecord);
		}
	});

	const perMarketMutex: Array<number> = [];
	const tryFillForMarket = async (market: MarketAccount) => {
		const marketIndex = market.marketIndex;
		if (perMarketMutex[marketIndex.toNumber()] === 1) {
			return;
		}
		perMarketMutex[marketIndex.toNumber()] = 1;

		try {
			const oraclePriceData = clearingHouse.getOracleDataForMarket(marketIndex);
			const oracleIsValid = isOracleValid(
				market.amm,
				oraclePriceData,
				clearingHouse.getStateAccount().oracleGuardRails,
				bulkAccountLoader.mostRecentSlot
			);

			const vAsk = calculateAskPrice(market, oraclePriceData);
			const vBid = calculateBidPrice(market, oraclePriceData);

			const matches = dlob.findMatches(
				marketIndex,
				vBid,
				vAsk,
				slotSubscriber.getSlot(),
				oracleIsValid ? oraclePriceData : undefined
			);

			const unfilledMatches = matches.filter((match) => !match.node.haveFilled);

			for (const match of unfilledMatches) {
				if (
					!match.makerNode &&
					(isVariant(match.node.order.orderType, 'limit') ||
						isVariant(match.node.order.orderType, 'triggerLimit'))
				) {
					const baseAssetAmountMarketCanExecute =
						calculateBaseAssetAmountMarketCanExecute(
							market,
							match.node.order,
							oraclePriceData
						);

					if (
						baseAssetAmountMarketCanExecute.lt(
							market.amm.baseAssetAmountStepSize
						)
					) {
						continue;
					}
				}

				match.node.haveFilled = true;

				console.log(
					`trying to fill (account: ${match.node.userAccount.toString()}) order ${match.node.order.orderId.toString()}`
				);

				if (match.makerNode) {
					`including maker: ${match.makerNode.userAccount.toString()}) with order ${match.makerNode.order.orderId.toString()}`;
				}

				let makerInfo;
				if (match.makerNode) {
					makerInfo = {
						maker: match.makerNode.userAccount,
						order: match.makerNode.order,
					};
				}

				const user = userMap.get(match.node.userAccount.toString());
				clearingHouse
					.fillOrder(
						match.node.userAccount,
						user.getUserAccount(),
						match.node.order,
						makerInfo
					)
					.then((txSig) => {
						console.log(
							`Filled user (account: ${match.node.userAccount.toString()}) order: ${match.node.order.orderId.toString()}`
						);
						console.log(`Tx: ${txSig}`);
					})
					.catch((error) => {
						match.node.haveFilled = false;
						console.log(
							`Error filling user (account: ${match.node.userAccount.toString()}) order: ${match.node.order.orderId.toString()}`
						);

						// If we get an error that order does not exist, assume its been filled by somebody else and we
						// have received the history record yet
						// TODO this might not hold if events arrive out of order
						const errorCode = getErrorCode(error);
						if (errorCode === 6043) {
							dlob.remove(match.node.order, match.node.userAccount, () => {
								console.log(
									`Order ${match.node.order.orderId.toString()} not found when trying to fill. Removing from order list`
								);
							});
							printTopOfOrderLists(match.node.order.marketIndex);
						}
					});
			}
		} catch (e) {
			console.log(`Unexpected error for market ${marketIndex.toString()}`);
			console.error(e);
		} finally {
			perMarketMutex[marketIndex.toNumber()] = 0;
		}
	};

	const tryFill = () => {
		for (const marketAccount of clearingHouse.getMarketAccounts()) {
			tryFillForMarket(marketAccount);
		}
	};

	tryFill();
	const handleFillIntervalId = setInterval(tryFill, 500); // every half second
	intervalIds.push(handleFillIntervalId);
};

async function recursiveTryCatch(f: () => void) {
	function sleep(ms) {
		return new Promise((resolve) => setTimeout(resolve, ms));
	}

	try {
		await f();
	} catch (e) {
		console.error(e);
		for (const intervalId of intervalIds) {
			clearInterval(intervalId);
		}
		await sleep(15000);
		await recursiveTryCatch(f);
	}
}

recursiveTryCatch(() => runBot(wallet, clearingHouse));

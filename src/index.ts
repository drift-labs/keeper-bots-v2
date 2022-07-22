import { Connection, Keypair, PublicKey } from '@solana/web3.js';

import {
	BN,
	BulkAccountLoader,
	ClearingHouse,
	initialize,
	isVariant,
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

		const bestAsk = dlob.getBestAsk(marketIndex, vAsk, slot, oraclePriceData);
		const bestBid = dlob.getBestBid(marketIndex, vBid, slot, oraclePriceData);
		const mid = bestAsk.add(bestBid).div(new BN(2));

		console.log(
			`Market ${sdkConfig.MARKETS[marketIndex.toNumber()].symbol} Orders`
		);
		console.log(
			`Ask`,
			convertToNumber(bestAsk, MARK_PRICE_PRECISION).toFixed(3)
		);
		console.log(`Mid`, convertToNumber(mid, MARK_PRICE_PRECISION).toFixed(3));
		console.log(
			`Bid`,
			convertToNumber(bestBid, MARK_PRICE_PRECISION).toFixed(3)
		);
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
		console.log(`Received an order record ${JSON.stringify(record)}`);

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
		} else if (isVariant(action, 'trigger')) {
			dlob.trigger(order, userAccount, () => {
				console.log(
					`Order ${dlob.getOpenOrderId(order, userAccount)} triggered`
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

	const perMarketMutexFills: Array<number> = [];
	const tryFillForMarket = async (market: MarketAccount) => {
		const marketIndex = market.marketIndex;
		if (perMarketMutexFills[marketIndex.toNumber()] === 1) {
			return;
		}
		perMarketMutexFills[marketIndex.toNumber()] = 1;

		try {
			const oraclePriceData = clearingHouse.getOracleDataForMarket(marketIndex);
			const oracleIsValid = isOracleValid(
				market.amm,
				oraclePriceData,
				clearingHouse.getStateAccount().oracleGuardRails,
				slotSubscriber.getSlot()
			);

			const vAsk = calculateAskPrice(market, oraclePriceData);
			const vBid = calculateBidPrice(market, oraclePriceData);

			const nodesToFill = dlob.findNodesToFill(
				marketIndex,
				vBid,
				vAsk,
				slotSubscriber.getSlot(),
				oracleIsValid ? oraclePriceData : undefined
			);

			for (const nodeToFill of nodesToFill) {
				if (nodeToFill.node.haveFilled) {
					continue;
				}

				if (
					!nodeToFill.makerNode &&
					(isVariant(nodeToFill.node.order.orderType, 'limit') ||
						isVariant(nodeToFill.node.order.orderType, 'triggerLimit'))
				) {
					const baseAssetAmountMarketCanExecute =
						calculateBaseAssetAmountMarketCanExecute(
							market,
							nodeToFill.node.order,
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

				nodeToFill.node.haveFilled = true;

				console.log(
					`trying to fill (account: ${nodeToFill.node.userAccount.toString()}) order ${nodeToFill.node.order.orderId.toString()}`
				);

				if (nodeToFill.makerNode) {
					`including maker: ${nodeToFill.makerNode.userAccount.toString()}) with order ${nodeToFill.makerNode.order.orderId.toString()}`;
				}

				let makerInfo;
				if (nodeToFill.makerNode) {
					makerInfo = {
						maker: nodeToFill.makerNode.userAccount,
						order: nodeToFill.makerNode.order,
					};
				}

				const user = userMap.get(nodeToFill.node.userAccount.toString());
				clearingHouse
					.fillOrder(
						nodeToFill.node.userAccount,
						user.getUserAccount(),
						nodeToFill.node.order,
						makerInfo
					)
					.then((txSig) => {
						console.log(
							`Filled user (account: ${nodeToFill.node.userAccount.toString()}) order: ${nodeToFill.node.order.orderId.toString()}`
						);
						console.log(`Tx: ${txSig}`);
					})
					.catch((error) => {
						nodeToFill.node.haveFilled = false;
						console.log(
							`Error filling user (account: ${nodeToFill.node.userAccount.toString()}) order: ${nodeToFill.node.order.orderId.toString()}`
						);
						console.error(error);

						// If we get an error that order does not exist, assume its been filled by somebody else and we
						// have received the history record yet
						// TODO this might not hold if events arrive out of order
						const errorCode = getErrorCode(error);
						if (errorCode === 6043) {
							dlob.remove(
								nodeToFill.node.order,
								nodeToFill.node.userAccount,
								() => {
									console.log(
										`Order ${nodeToFill.node.order.orderId.toString()} not found when trying to fill. Removing from order list`
									);
								}
							);
							printTopOfOrderLists(nodeToFill.node.order.marketIndex);
						}
					});
			}
		} catch (e) {
			console.log(
				`Unexpected error for market ${marketIndex.toString()} during fills`
			);
			console.error(e);
		} finally {
			perMarketMutexFills[marketIndex.toNumber()] = 0;
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

	const perMarketMutexTriggers: Array<number> = [];
	const tryTriggerForMarket = async (market: MarketAccount) => {
		const marketIndex = market.marketIndex;
		if (perMarketMutexTriggers[marketIndex.toNumber()] === 1) {
			return;
		}
		perMarketMutexTriggers[marketIndex.toNumber()] = 1;

		try {
			const oraclePriceData = clearingHouse.getOracleDataForMarket(marketIndex);

			const nodesToTrigger = dlob.findNodesToTrigger(
				marketIndex,
				slotSubscriber.getSlot(),
				oraclePriceData.price
			);

			for (const nodeToTrigger of nodesToTrigger) {
				if (nodeToTrigger.node.haveTrigger) {
					continue;
				}

				nodeToTrigger.node.haveTrigger = true;

				console.log(
					`trying to trigger (account: ${nodeToTrigger.node.userAccount.toString()}) order ${nodeToTrigger.node.order.orderId.toString()}`
				);

				const user = userMap.get(nodeToTrigger.node.userAccount.toString());
				clearingHouse
					.triggerOrder(
						nodeToTrigger.node.userAccount,
						user.getUserAccount(),
						nodeToTrigger.node.order
					)
					.then((txSig) => {
						console.log(
							`Triggered user (account: ${nodeToTrigger.node.userAccount.toString()}) order: ${nodeToTrigger.node.order.orderId.toString()}`
						);
						console.log(`Tx: ${txSig}`);
					})
					.catch((error) => {
						nodeToTrigger.node.haveTrigger = false;
						console.log(
							`Error triggering user (account: ${nodeToTrigger.node.userAccount.toString()}) order: ${nodeToTrigger.node.order.orderId.toString()}`
						);
						console.error(error);
					});
			}
		} catch (e) {
			console.log(
				`Unexpected error for market ${marketIndex.toString()} during triggers`
			);
			console.error(e);
		} finally {
			perMarketMutexTriggers[marketIndex.toNumber()] = 0;
		}
	};

	const tryTrigger = () => {
		for (const marketAccount of clearingHouse.getMarketAccounts()) {
			tryTriggerForMarket(marketAccount);
		}
	};

	tryTrigger();
	const handleTriggerIntervalId = setInterval(tryTrigger, 500); // every half second
	intervalIds.push(handleTriggerIntervalId);
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

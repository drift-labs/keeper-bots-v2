import { BN, Provider } from '@project-serum/anchor';
import { Connection, Keypair, PublicKey } from '@solana/web3.js';

import {
	BulkAccountLoader,
	ClearingHouse,
	initialize,
	ClearingHouseUser,
	isVariant,
	Markets,
	UserOrdersAccount,
	OrderRecord,
	getUserOrdersAccountPublicKey,
	calculateMarkPrice,
	convertToNumber,
	MARK_PRICE_PRECISION,
	TEN_THOUSAND,
	isOrderRiskIncreasing,
	Wallet,
	getClearingHouse,
	getPollingClearingHouseConfig,
	getClearingHouseUser,
	getPollingClearingHouseUserConfig,
	QUOTE_PRECISION,
	ZERO,
} from '@drift-labs/sdk';

import { Node, OrderList, sortDirectionForOrder } from './OrderList';
import { CloudWatchClient } from './cloudWatchClient';
import { bulkPollingUserSubscribe } from '@drift-labs/sdk/lib/accounts/bulkUserSubscription';
import { getErrorCode } from './error';

require('dotenv').config();
//@ts-ignore
const sdkConfig = initialize({ env: process.env.ENV });

const cloudWatchClient = new CloudWatchClient(
	sdkConfig.ENV === 'mainnet-beta' ? 'eu-west-1' : 'us-east-1',
	process.env.ENABLE_CLOUDWATCH === 'true'
);

export function getWallet(): Wallet {
	const privateKey = process.env.FILLER_PRIVATE_KEY;
	const keypair = Keypair.fromSecretKey(
		Uint8Array.from(privateKey.split(',').map((val) => Number(val)))
	);
	return new Wallet(keypair);
}

const endpoint = process.env.ENDPOINT;
const connection = new Connection(endpoint);

const intervalIds = [];
const runBot = async (wallet: Wallet, clearingHouse: ClearingHouse) => {
	const lamportsBalance = await connection.getBalance(wallet.publicKey);
	console.log('SOL balance:', lamportsBalance / 10 ** 9);
	await clearingHouse.subscribe(['orderHistoryAccount']);
	console.log(clearingHouse.program.programId.toString());

	const marketOrderLists = new Map<
		number,
		{ desc: OrderList; asc: OrderList }
	>();
	for (const market of Markets) {
		const longs = new OrderList(market.marketIndex, 'desc');
		const shorts = new OrderList(market.marketIndex, 'asc');

		marketOrderLists.set(market.marketIndex.toNumber(), {
			desc: longs,
			asc: shorts,
		});
	}
	const openOrders = new Set<number>();

	// explicitly grab order index before we initially build order list
	// so we're less likely to have missed records while we fetch order accounts
	let nextOrderHistoryIndex = clearingHouse
		.getOrderHistoryAccount()
		.head.toNumber();

	const programAccounts = await clearingHouse.program.account.userOrders.all();
	for (const programAccount of programAccounts) {
		const userOrderAccountPublicKey = programAccount.publicKey;
		// @ts-ignore
		const userOrdersAccount: UserOrdersAccount = programAccount.account;
		const userAccountPublicKey = userOrdersAccount.user;

		for (const order of userOrdersAccount.orders) {
			if (isVariant(order, 'init') || isVariant(order.orderType, 'market')) {
				continue;
			}

			const ordersList = marketOrderLists.get(order.marketIndex.toNumber());
			const sortDirection = sortDirectionForOrder(order);
			const orderList =
				sortDirection === 'desc' ? ordersList.desc : ordersList.asc;
			orderList.insert(order, userAccountPublicKey, userOrderAccountPublicKey);
			if (isVariant(order.status, 'open')) {
				openOrders.add(order.orderId.toNumber());
			}
		}
	}

	const printTopOfOrdersList = (ascList: OrderList, descList: OrderList) => {
		console.log(`Market ${Markets[descList.marketIndex.toNumber()].symbol}`);
		descList.printTop();
		console.log(
			`Mark`,
			convertToNumber(
				calculateMarkPrice(clearingHouse.getMarket(descList.marketIndex)),
				MARK_PRICE_PRECISION
			).toFixed(3)
		);
		ascList.printTop();
	};

	const printOrderLists = () => {
		for (const [_, ordersList] of marketOrderLists) {
			printTopOfOrdersList(ordersList.asc, ordersList.desc);
		}
	};
	printOrderLists();

	const sleep = (ms: number) => {
		return new Promise((resolve) => {
			setTimeout(resolve, ms);
		});
	};

	const updateUserOrders = (
		user: ClearingHouseUser,
		userAccountPublicKey: PublicKey,
		userOrdersAccountPublicKey: PublicKey
	): BN => {
		const marginRatio = user.getMarginRatio();
		const tooMuchLeverage = marginRatio.lte(
			user.clearingHouse.getStateAccount().marginRatioInitial
		);

		if (user.getUserOrdersAccount()) {
			for (const order of user.getUserOrdersAccount().orders) {
				const ordersLists = marketOrderLists.get(order.marketIndex.toNumber());
				const sortDirection = sortDirectionForOrder(order);
				const orderList =
					sortDirection === 'desc' ? ordersLists.desc : ordersLists.asc;
				const orderIsRiskIncreasing = isOrderRiskIncreasing(user, order);

				const orderId = order.orderId.toNumber();
				if (tooMuchLeverage && orderIsRiskIncreasing) {
					if (openOrders.has(orderId) && orderList.has(orderId)) {
						console.log(
							`User has too much leverage and order is risk increasing. Removing order ${order.orderId.toString()}`
						);
						orderList.remove(orderId);
						printTopOfOrdersList(ordersLists.asc, ordersLists.desc);
					}
				} else if (orderIsRiskIncreasing && order.reduceOnly) {
					if (openOrders.has(orderId) && orderList.has(orderId)) {
						console.log(
							`Order ${order.orderId.toString()} is risk increasing but reduce only. Removing`
						);
						orderList.remove(orderId);
						printTopOfOrdersList(ordersLists.asc, ordersLists.desc);
					}
				} else if (user.getUserAccount().collateral.eq(ZERO)) {
					if (openOrders.has(orderId) && orderList.has(orderId)) {
						console.log(
							`Removing order ${order.orderId.toString()} as authority ${user.authority.toString()} has no collateral`
						);
						orderList.remove(orderId);
						printTopOfOrdersList(ordersLists.asc, ordersLists.desc);
					}
				} else {
					if (openOrders.has(orderId) && !orderList.has(orderId)) {
						console.log(`Order ${order.orderId.toString()} added back`);
						orderList.insert(
							order,
							userAccountPublicKey,
							userOrdersAccountPublicKey
						);
						printTopOfOrdersList(ordersLists.asc, ordersLists.desc);
					}
				}
			}
		}
		return marginRatio;
	};
	const processUser = async (user: ClearingHouseUser) => {
		const userAccountPublicKey = await user.getUserAccountPublicKey();
		const userOrdersAccountPublicKey =
			await user.getUserOrdersAccountPublicKey();

		user.eventEmitter.on('userPositionsData', () => {
			updateUserOrders(user, userAccountPublicKey, userOrdersAccountPublicKey);
			userMap.set(userAccountPublicKey.toString(), { user, upToDate: true });
		});

		// eslint-disable-next-line no-constant-condition
		while (true) {
			const marginRatio = updateUserOrders(
				user,
				userAccountPublicKey,
				userOrdersAccountPublicKey
			);
			const marginRatioNumber = convertToNumber(marginRatio, TEN_THOUSAND);
			const oneMinute = 1000 * 60;
			const sleepTime = Math.min(
				Math.round(marginRatioNumber * 100) ** 2,
				oneMinute
			);
			await sleep(sleepTime);
		}
	};

	const userAccountLoader = new BulkAccountLoader(
		connection,
		'processed',
		5000
	);
	const userMap = new Map<
		string,
		{ user: ClearingHouseUser; upToDate: boolean }
	>();
	const fetchAllUsers = async () => {
		const programUserAccounts = await clearingHouse.program.account.user.all();
		const userArray: ClearingHouseUser[] = [];
		for (const programUserAccount of programUserAccounts) {
			const userAccountPubkey = programUserAccount.publicKey.toString();

			// if the user has less than one dollar in account, disregard initially
			// if an order comes from this user, we can add them then.
			// This makes it so that we don't need to listen to inactive users
			if (programUserAccount.account.collateral.lt(QUOTE_PRECISION)) {
				continue;
			}

			if (userMap.has(userAccountPubkey)) {
				continue;
			}
			const user = getClearingHouseUser(
				getPollingClearingHouseUserConfig(
					clearingHouse,
					programUserAccount.account.authority,
					userAccountLoader
				)
			);
			userArray.push(user);
		}

		await bulkPollingUserSubscribe(userArray, userAccountLoader);
		for (const user of userArray) {
			const userAccountPubkey = await user.getUserAccountPublicKey();
			userMap.set(userAccountPubkey.toString(), { user, upToDate: true });
			processUser(user);
		}
	};
	await fetchAllUsers();

	let updateOrderListMutex = 0;
	const handleOrderRecord = async (record: OrderRecord) => {
		const order = record.order;
		// Disregard market orders
		if (isVariant(order.orderType, 'market')) {
			return;
		}

		const sortDirection = sortDirectionForOrder(order);
		const ordersList = marketOrderLists.get(order.marketIndex.toNumber());
		const orderList =
			sortDirection === 'desc' ? ordersList.desc : ordersList.asc;

		if (isVariant(record.action, 'place')) {
			const userOrdersAccountPublicKey = await getUserOrdersAccountPublicKey(
				clearingHouse.program.programId,
				record.user
			);
			orderList.insert(order, record.user, userOrdersAccountPublicKey);
			openOrders.add(order.orderId.toNumber());
			console.log(
				`Order ${order.orderId.toString()} placed. Added to order list`
			);
		} else if (isVariant(record.action, 'cancel')) {
			orderList.remove(order.orderId.toNumber());
			openOrders.delete(order.orderId.toNumber());
			console.log(
				`Order ${order.orderId.toString()} canceled. Removed from order list`
			);
		} else if (isVariant(record.action, 'fill')) {
			if (order.baseAssetAmount.eq(order.baseAssetAmountFilled)) {
				orderList.remove(order.orderId.toNumber());
				openOrders.delete(order.orderId.toNumber());
				console.log(
					`Order ${order.orderId.toString()} completely filled. Removed from order list`
				);
			} else {
				orderList.update(order);
				console.log(
					`Order ${order.orderId.toString()} partially filled. Updated`
				);
			}
		}
		printTopOfOrdersList(ordersList.asc, ordersList.desc);
	};

	const updateOrderList = async () => {
		if (updateOrderListMutex === 1) {
			return;
		}
		updateOrderListMutex = 1;

		try {
			let head = clearingHouse.getOrderHistoryAccount().head.toNumber();
			const orderHistoryLength =
				clearingHouse.getOrderHistoryAccount().orderRecords.length;
			while (nextOrderHistoryIndex !== head) {
				const nextRecord =
					clearingHouse.getOrderHistoryAccount().orderRecords[
						nextOrderHistoryIndex
					];
				await handleOrderRecord(nextRecord);
				nextOrderHistoryIndex =
					(nextOrderHistoryIndex + 1) % orderHistoryLength;
				head = clearingHouse.getOrderHistoryAccount().head.toNumber();
			}
		} catch (e) {
			console.log(`Unexpected error in update order list`);
			console.error(e);
		} finally {
			updateOrderListMutex = 0;
		}
	};
	clearingHouse.eventEmitter.on('orderHistoryAccountUpdate', updateOrderList);
	await updateOrderList();

	const fetchUserAndAddToUserMap = async (userAccountPublicKey: PublicKey) => {
		const userAccount = await clearingHouse.program.account.user.fetch(
			userAccountPublicKey
		);
		const user = getClearingHouseUser(
			getPollingClearingHouseUserConfig(
				clearingHouse,
				userAccount.authority,
				userAccountLoader
			)
		);
		await user.subscribe();
		processUser(user);
		const mapValue = { user, upToDate: true };
		userMap.set(userAccountPublicKey.toString(), mapValue);
		return mapValue;
	};

	const findNodesToFill = async (
		node: Node,
		markPrice: BN
	): Promise<Node[]> => {
		const nodesToFill = [];
		let currentNode = node;
		while (currentNode !== undefined) {
			if (!currentNode.pricesCross(markPrice)) {
				currentNode = undefined;
				break;
			}

			let mapValue = userMap.get(node.userAccount.toString());
			if (!mapValue) {
				console.log(
					`User not found in user map ${node.userAccount.toString()}. Adding before filling.`
				);
				mapValue = await fetchUserAndAddToUserMap(node.userAccount);
			}
			const { upToDate: userUpToDate } = mapValue;
			if (!currentNode.haveFilled && userUpToDate) {
				nodesToFill.push(currentNode);
			}

			currentNode = currentNode.next;
		}

		return nodesToFill;
	};

	const perMarketMutex = Array(64).fill(0);
	const tryFillForMarket = async (marketIndex: BN) => {
		if (perMarketMutex[marketIndex.toNumber()] === 1) {
			return;
		}
		perMarketMutex[marketIndex.toNumber()] = 1;

		try {
			const market = clearingHouse.getMarket(marketIndex);
			const orderLists = marketOrderLists.get(marketIndex.toNumber());
			const markPrice = calculateMarkPrice(market);

			const nodesToFill: Node[] = [];
			if (orderLists.asc.head && orderLists.asc.head.pricesCross(markPrice)) {
				nodesToFill.push(
					...(await findNodesToFill(orderLists.asc.head, markPrice))
				);
			}

			if (orderLists.desc.head && orderLists.desc.head.pricesCross(markPrice)) {
				nodesToFill.push(
					...(await findNodesToFill(orderLists.desc.head, markPrice))
				);
			}

			const unfilledNodes = nodesToFill.filter(
				(nodeToFill) => !nodeToFill.haveFilled
			);
			for (const nodeToFill of unfilledNodes) {
				let mapValue = userMap.get(nodeToFill.userAccount.toString());
				if (!mapValue) {
					console.log(
						`User not found in user map ${nodeToFill.userAccount.toString()}. Adding`
					);
					mapValue = await fetchUserAndAddToUserMap(nodeToFill.userAccount);
					continue;
				}
				const { user } = mapValue;
				userMap.set(nodeToFill.userAccount.toString(), {
					user,
					upToDate: false,
				});
				nodeToFill.haveFilled = true;

				console.log(
					`trying to fill (account: ${nodeToFill.userAccount.toString()})`
				);
				clearingHouse
					.fillOrder(
						nodeToFill.userAccount,
						nodeToFill.userOrdersAccount,
						nodeToFill.order
					)
					.then((txSig) => {
						console.log(
							`Filled user (account: ${nodeToFill.userAccount.toString()}) order: ${nodeToFill.order.orderId.toString()}`
						);
						console.log(`Tx: ${txSig}`);
						cloudWatchClient.logFill(true);
					})
					.catch((error) => {
						nodeToFill.haveFilled = false;
						userMap.set(nodeToFill.userAccount.toString(), {
							user,
							upToDate: true,
						});
						console.log(
							`Error filling user (account: ${nodeToFill.userAccount.toString()}) order: ${nodeToFill.order.orderId.toString()}`
						);
						cloudWatchClient.logFill(false);

						// If we get an error that order does not exist, assume its been filled by somebody else and we
						// have received the history record yet
						const errorCode = getErrorCode(error);
						if (errorCode === 6043) {
							console.log(
								`Order ${nodeToFill.order.orderId.toString()} not found when trying to fill. Removing from order list`
							);
							orderLists[nodeToFill.sortDirection].remove(
								nodeToFill.order.orderId.toNumber()
							);
							printTopOfOrdersList(orderLists.asc, orderLists.desc);
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
		for (const market of Markets) {
			tryFillForMarket(market.marketIndex);
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

const wallet = getWallet();
const provider = new Provider(connection, wallet, Provider.defaultOptions());
const clearingHousePublicKey = new PublicKey(
	sdkConfig.CLEARING_HOUSE_PROGRAM_ID
);

const clearingHouse = getClearingHouse(
	getPollingClearingHouseConfig(
		connection,
		provider.wallet,
		clearingHousePublicKey,
		new BulkAccountLoader(connection, 'confirmed', 500)
	)
);

recursiveTryCatch(() => runBot(wallet, clearingHouse));

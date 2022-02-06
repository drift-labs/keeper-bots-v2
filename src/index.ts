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
} from '@drift-labs/sdk';

import { Node, OrderList, sortDirectionForOrder } from './OrderList';
import { CloudWatchClient } from './cloudWatchClient';
import { bulkPollingUserSubscribe } from '@drift-labs/sdk/lib/accounts/bulkUserSubscription';

require('dotenv').config();
//@ts-ignore
const sdkConfig = initialize({ env: process.env.ENV });

const cloudWatchClient = new CloudWatchClient(
	sdkConfig.ENV === 'mainnet-beta' ? 'eu-west-1' : 'us-east-1',
	process.env.ENABLE_CLOUDWATCH === 'true'
);

function getWallet(): Wallet {
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

	for (const [_, ordersList] of marketOrderLists) {
		printTopOfOrdersList(ordersList.asc, ordersList.desc);
	}

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
		for (const order of user.getUserOrdersAccount().orders) {
			const ordersLists = marketOrderLists.get(order.marketIndex.toNumber());
			const sortDirection = sortDirectionForOrder(order);
			const orderList =
				sortDirection === 'desc' ? ordersLists.desc : ordersLists.asc;
			const orderIsRiskIncreasing = isOrderRiskIncreasing(user, order);

			if (tooMuchLeverage && orderIsRiskIncreasing) {
				orderList.remove(order.orderId.toNumber());
			} else if (orderIsRiskIncreasing && order.reduceOnly) {
				orderList.remove(order.orderId.toNumber());
			} else {
				orderList.insert(
					order,
					userAccountPublicKey,
					userOrdersAccountPublicKey
				);
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
				record.authority
			);
			orderList.insert(order, record.user, userOrdersAccountPublicKey);
		} else if (isVariant(record.action, 'cancel')) {
			orderList.remove(order.orderId.toNumber());
		} else if (isVariant(record.action, 'fill')) {
			if (order.baseAssetAmount.eq(order.baseAssetAmountFilled)) {
				orderList.remove(order.orderId.toNumber());
			} else {
				orderList.update(order);
			}
		}
		printTopOfOrdersList(ordersList.asc, ordersList.desc);
	};

	const updateOrderList = async () => {
		if (updateOrderListMutex === 1) {
			return;
		}
		updateOrderListMutex = 1;

		let head = clearingHouse.getOrderHistoryAccount().head.toNumber();
		while (nextOrderHistoryIndex !== head) {
			const nextRecord =
				clearingHouse.getOrderHistoryAccount().orderRecords[
					nextOrderHistoryIndex
				];
			await handleOrderRecord(nextRecord);
			nextOrderHistoryIndex += 1;
			head = clearingHouse.getOrderHistoryAccount().head.toNumber();
		}
		updateOrderListMutex = 0;
	};
	clearingHouse.eventEmitter.on('orderHistoryAccountUpdate', updateOrderList);
	await updateOrderList();

	const findNodeToFill = async (
		node: Node,
		markPrice: BN
	): Promise<Node | undefined> => {
		let currentNode = node;
		while (currentNode !== undefined) {
			if (!currentNode.pricesCross(markPrice)) {
				currentNode = undefined;
				break;
			}

			let mapValue = userMap.get(node.userAccount.toString());
			if (!mapValue) {
				const userAccount = await clearingHouse.program.account.user.fetch(
					node.userAccount
				);
				const user = ClearingHouseUser.from(
					clearingHouse,
					userAccount.authority
				);
				await user.subscribe();
				mapValue = { user, upToDate: true };
				userMap.set(node.userAccount.toString(), mapValue);
				processUser(user);
			}
			const { upToDate: userUpToDate } = mapValue;

			if (!currentNode.haveFilled && userUpToDate) {
				break;
			}

			currentNode = currentNode.next;
		}

		return currentNode;
	};

	const perMarketMutex = Array(64).fill(0);
	const tryFillForMarket = async (marketIndex: BN) => {
		if (perMarketMutex[marketIndex.toNumber()] === 1) {
			return;
		}
		perMarketMutex[marketIndex.toNumber()] = 1;

		const market = clearingHouse.getMarket(marketIndex);
		const orderLists = marketOrderLists.get(marketIndex.toNumber());
		const markPrice = calculateMarkPrice(market);

		let nodeToFill: Node | undefined = undefined;
		if (orderLists.asc.head && orderLists.asc.head.pricesCross(markPrice)) {
			nodeToFill = await findNodeToFill(orderLists.asc.head, markPrice);
		} else if (
			orderLists.desc.head &&
			orderLists.desc.head.pricesCross(markPrice)
		) {
			nodeToFill = await findNodeToFill(orderLists.desc.head, markPrice);
		}

		if (nodeToFill !== undefined && !nodeToFill.haveFilled) {
			const { user } = userMap.get(nodeToFill.userAccount.toString());
			userMap.set(nodeToFill.userAccount.toString(), { user, upToDate: false });
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
				.catch(() => {
					nodeToFill.haveFilled = false;
					userMap.set(nodeToFill.userAccount.toString(), {
						user,
						upToDate: true,
					});
					console.log(
						`Error filling user (account: ${nodeToFill.userAccount.toString()}) order: ${nodeToFill.order.orderId.toString()}`
					);
					cloudWatchClient.logFill(false);
				});
		}
		perMarketMutex[marketIndex.toNumber()] = 0;
	};

	const tryFill = () => {
		for (const market of Markets) {
			tryFillForMarket(market.marketIndex);
		}
	};

	tryFill();
	const handleFillIntervalId = setInterval(tryFill, 1000); // every second
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
		new BulkAccountLoader(connection, 'confirmed', 1000)
	)
);

recursiveTryCatch(() => runBot(wallet, clearingHouse));

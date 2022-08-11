import fs from 'fs';
import { program, Option } from 'commander';

import { Connection, Keypair, PublicKey } from '@solana/web3.js';

import {
	Token,
	TOKEN_PROGRAM_ID,
	ASSOCIATED_TOKEN_PROGRAM_ID,
} from '@solana/spl-token';
import {
	BulkAccountLoader,
	ClearingHouse,
	ClearingHouseUser,
	initialize,
	OrderRecord,
	Wallet,
	DriftEnv,
	EventSubscriber,
	SlotSubscriber,
	convertToNumber,
	QUOTE_PRECISION,
	DevnetBanks,
	BN,
	DevnetMarkets,
	BASE_PRECISION,
} from '@drift-labs/sdk';

import { logger } from './logger';
import { constants } from './types';
import { FillerBot } from './bots/filler';
import { TriggerBot } from './bots/trigger';
import { JitMakerBot } from './bots/jitMaker';
import { Bot } from './types';
import { Metrics } from './metrics';

require('dotenv').config();
const driftEnv = process.env.ENV as DriftEnv;
//@ts-ignore
const sdkConfig = initialize({ env: process.env.ENV });

program
	// .option('-d, --dry', 'Dry run, do not send transactions on chain')
	.option(
		'--init-user',
		'calls clearingHouse.initializeUserAccount if no user account exists'
	)
	.option('--filler', 'Enable order filler')
	.option('--trigger', 'Enable trigger order')
	.option('--jit-maker', 'Enable JIT auction maker')
	.option('--print-info', 'Periodically print market and position info')
	.option('--cancel-open-orders', 'Cancel open orders on startup')
	.option(
		'--deposit <number>',
		'Allow deposit this amount of USDC to collateral account'
	)
	.option(
		'--metrics', // TODO: allow custom url and port
		'Enable Prometheus metric scraper'
	)
	.addOption(
		new Option(
			'-p, --private-key <string>',
			'private key, supports path to id.json, or list of comma separate numbers'
		).env('FILLER_PRIVATE_KEY')
	)
	.parse();

const opts = program.opts();

logger.info(
	`Dry run: ${!!opts.dry}, FillerBot enabled: ${!!opts.filler}, TriggerBot enabled: ${!!opts.trigger} JitMakerBot enabled: ${!!opts.jitMaker}`
);

export function getWallet(): Wallet {
	const privateKey = process.env.FILLER_PRIVATE_KEY;
	if (!privateKey) {
		throw new Error(
			'Must set environment variable FILLER_PRIVATE_KEY with the path to a id.json or a list of commma separated numbers'
		);
	}
	// try to load privateKey as a filepath
	let loadedKey: Uint8Array;
	if (fs.existsSync(privateKey)) {
		logger.info(`loading private key from ${privateKey}`);
		loadedKey = new Uint8Array(
			JSON.parse(fs.readFileSync(privateKey).toString())
		);
	} else {
		logger.info(`loading private key as comma separated numbers`);
		loadedKey = Uint8Array.from(
			privateKey.split(',').map((val) => Number(val))
		);
	}

	const keypair = Keypair.fromSecretKey(Uint8Array.from(loadedKey));
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
	maxTx: 4096,
	maxEventsPerType: 4096,
	orderBy: 'blockchain',
	orderDir: 'desc',
	commitment: 'confirmed',
	logProviderConfig: {
		type: 'websocket',
	},
});

const slotSubscriber = new SlotSubscriber(connection, {});

function sleep(ms) {
	return new Promise((resolve) => setTimeout(resolve, ms));
}

function printUserAccountStats(clearingHouseUser: ClearingHouseUser) {
	const totalCollateral = clearingHouseUser.getCollateralValue();
	logger.info(
		`User total collateral: $${convertToNumber(
			totalCollateral,
			QUOTE_PRECISION
		)}:`
	);
	for (let i = 0; i < DevnetBanks.length; i += 1) {
		const bank = DevnetBanks[i];
		const collateral = clearingHouseUser.getCollateralValue(bank.bankIndex);
		logger.info(
			`  Bank Collateral (${bank.bankIndex}: ${bank.symbol}): ${convertToNumber(
				collateral,
				QUOTE_PRECISION
			)}`
		);
	}

	logger.info(
		`CHUser unsettled PnL:          ${convertToNumber(
			clearingHouseUser.getUnsettledPNL(),
			QUOTE_PRECISION
		)}`
	);
	logger.info(
		`CHUser unrealized funding PnL: ${convertToNumber(
			clearingHouseUser.getUnrealizedFundingPNL(),
			QUOTE_PRECISION
		)}`
	);
	logger.info(
		`CHUser unrealized PnL:         ${convertToNumber(
			clearingHouseUser.getUnrealizedPNL(),
			QUOTE_PRECISION
		)}`
	);
}

function printOpenPositions(clearingHouseUser: ClearingHouseUser) {
	logger.info('Open Positions:');
	for (const p of clearingHouseUser.getUserAccount().positions) {
		if (p.baseAssetAmount.isZero()) {
			continue;
		}
		const market = DevnetMarkets[p.marketIndex.toNumber()];
		console.log(`[${market.symbol}]`);
		console.log(
			` . baseAssetAmount:  ${convertToNumber(
				p.baseAssetAmount,
				BASE_PRECISION
			).toString()}`
		);
		console.log(
			` . quoteAssetAmount: ${convertToNumber(
				p.quoteAssetAmount,
				QUOTE_PRECISION
			).toString()}`
		);
		console.log(
			` . quoteEntryAmount: ${convertToNumber(
				p.quoteEntryAmount,
				QUOTE_PRECISION
			).toString()}`
		);
		console.log(
			` . unsettledPnl:     ${convertToNumber(
				p.unsettledPnl,
				QUOTE_PRECISION
			).toString()}`
		);
		// console.log(` . lastCumulativeFundingRate: ${p.lastCumulativeFundingRate}`);
		console.log(
			` . lastCumulativeFundingRate: ${convertToNumber(
				p.lastCumulativeFundingRate,
				new BN(10).pow(new BN(14))
			)}`
		);
		console.log(
			` . openOrders: ${p.openOrders.toString()}, openBids: ${p.openBids.toString()}, openAsks: ${p.openAsks.toString()}`
		);
	}
}

const bots: Bot[] = [];
const runBot = async (wallet: Wallet, clearingHouse: ClearingHouse) => {
	const lamportsBalance = await connection.getBalance(wallet.publicKey);
	logger.info(
		`ClearingHouse ProgramId: ${clearingHouse.program.programId.toBase58()}`
	);
	logger.info(`wallet pubkey: ${wallet.publicKey.toBase58()}`);
	logger.info(`SOL balance: ${lamportsBalance / 10 ** 9}`);
	const tokenAccount = await Token.getAssociatedTokenAddress(
		ASSOCIATED_TOKEN_PROGRAM_ID,
		TOKEN_PROGRAM_ID,
		new PublicKey(constants.devnet.USDCMint),
		wallet.publicKey
	);
	const usdcBalance = await connection.getTokenAccountBalance(tokenAccount);
	logger.info(`USDC balance: ${usdcBalance.value.uiAmount}`);

	await clearingHouse.subscribe();
	clearingHouse.eventEmitter.on('error', (e) => {
		logger.info('clearing house error');
		logger.error(e);
	});

	eventSubscriber.subscribe();
	await slotSubscriber.subscribe();

	if (!(await clearingHouse.getUser().exists())) {
		logger.error(`ClearingHouseUser for ${wallet.publicKey} does not exist`);
		if (opts.initUser) {
			logger.info(`Creating ClearingHouseUser for ${wallet.publicKey}`);
			const [txSig] = await clearingHouse.initializeUserAccount();
			logger.info(`Initialized user account in transaction: ${txSig}`);
		} else {
			throw new Error(
				"Run with '--init-user' flag to initialize a ClearingHouseUser"
			);
		}
	}

	// subscribe will fail if there is no clearing house user
	const clearingHouseUser = clearingHouse.getUser();
	while (
		!clearingHouse.isSubscribed ||
		!clearingHouseUser.isSubscribed ||
		!eventSubscriber.subscribe()
	) {
		logger.info('not subscribed yet');
		await sleep(1000);
		await clearingHouse.subscribe();
		await clearingHouseUser.subscribe();
	}
	logger.info(
		`ClearingHouseUser PublicKey: ${clearingHouseUser
			.getUserAccountPublicKey()
			.toBase58()}`
	);

	let metrics: Metrics | undefined = undefined;
	if (opts.metrics) {
		metrics = new Metrics(clearingHouse);
		await metrics.init();
		metrics.trackObjectSize('clearingHouse', clearingHouse);
		metrics.trackObjectSize('clearingHouseUser', clearingHouseUser);
		metrics.trackObjectSize('eventSubscriber', eventSubscriber);
	}

	printUserAccountStats(clearingHouseUser);

	// check that user has collateral
	const totalCollateral = clearingHouseUser.getCollateralValue();
	if (totalCollateral.isZero() && opts.jitMaker) {
		logger.info(
			`No collateral in account, collateral is required to run JitMakerBot`
		);
		if (!opts.deposit) {
			logger.error(`Run with '--deposit' flag to deposit collateral`);
			throw new Error('Account has no collateral, and no deposit was provided');
		}

		if (opts.deposit < 0) {
			logger.error(`Deposit amount must be greater than 0`);
			throw new Error('Deposit amount must be greater than 0');
		}

		logger.info(
			`Depositing collateral (${new BN(opts.deposit).toString()} USDC)`
		);

		const ata = await Token.getAssociatedTokenAddress(
			ASSOCIATED_TOKEN_PROGRAM_ID,
			TOKEN_PROGRAM_ID,
			DevnetBanks[0].mint, // USDC
			wallet.publicKey
		);

		const tx = await clearingHouse.deposit(
			new BN(opts.deposit).mul(QUOTE_PRECISION),
			new BN(0), // USDC bank
			ata
		);
		logger.info(`Deposit transaction: ${tx}`);
	}

	// print user orders
	logger.info('');
	logger.info('Open orders:');
	const ordersToCancel: Array<BN> = [];
	for (const order of clearingHouseUser.getUserAccount().orders) {
		if (order.baseAssetAmount.isZero()) {
			continue;
		}
		console.log(order);
		ordersToCancel.push(order.orderId);
	}
	if (opts.cancelOpenOrders) {
		for (const order of ordersToCancel) {
			logger.info(`Cancelling open order ${order.toString()}`);
			await clearingHouse.cancelOrder(order);
		}
	}

	printOpenPositions(clearingHouseUser);

	/*
	 * Start bots depending on flags enabled
	 */

	if (opts.filler) {
		bots.push(
			new FillerBot(
				'filler',
				!!opts.dry,
				clearingHouse,
				slotSubscriber,
				connection,
				metrics
			)
		);
	}
	if (opts.trigger) {
		bots.push(
			new TriggerBot(
				'trigger',
				!!opts.dry,
				clearingHouse,
				slotSubscriber,
				connection,
				metrics
			)
		);
	}
	if (opts.jitMaker) {
		bots.push(
			new JitMakerBot(
				'JitMaker',
				!!opts.dry,
				clearingHouse,
				slotSubscriber,
				connection,
				metrics
			)
		);
	}

	for (const bot of bots) {
		await bot.init();
	}

	for (const bot of bots) {
		bot.startIntervalLoop(200);
	}

	const handleOrderRecord = async (record: OrderRecord) => {
		/*
		{"ts":"62e0abf7","slot":150777343,"taker":"3zztWnffdWvgsu9zTVWq1HP5xh29WexTv7tbyC6Uqy8V","maker":"11111111111111111111111111111111","takerOrder":{"status":{"open":{}},"orderType":{"market":{}},"ts":"62e0abf7","slot":"08fcadff","orderId":"0a","userOrderId":0,"marketIndex":"00","price":"00","existingPositionDirection":{"short":{}},"quoteAssetAmount":"00","baseAssetAmount":"5af3107a4000","baseAssetAmountFilled":"00","quoteAssetAmountFilled":"00","fee":"00","direction":{"long":{}},"reduceOnly":true,"postOnly":false,"immediateOrCancel":false,"discountTier":{"none":{}},"triggerPrice":"00","triggerCondition":{"above":{}},"triggered":false,"referrer":"11111111111111111111111111111111","oraclePriceOffset":"00","auctionStartPrice":"4ef1cd29f0","auctionEndPrice":"53fe8975b8","auctionDuration":10,"padding":[0,0,0]},"makerOrder":{"status":{"init":{}},"orderType":{"limit":{}},"ts":"00","slot":"00","orderId":"00","userOrderId":0,"marketIndex":"00","price":"00","existingPositionDirection":{"long":{}},"quoteAssetAmount":"00","baseAssetAmount":"00","baseAssetAmountFilled":"00","quoteAssetAmountFilled":"00","fee":"00","direction":{"long":{}},"reduceOnly":false,"postOnly":false,"immediateOrCancel":false,"discountTier":{"none":{}},"triggerPrice":"00","triggerCondition":{"above":{}},"triggered":false,"referrer":"11111111111111111111111111111111","oraclePriceOffset":"00","auctionStartPrice":"00","auctionEndPrice":"00","auctionDuration":0,"padding":[0,0,0]},"makerUnsettledPnl":"00","takerUnsettledPnl":"00","action":{"place":{}},"actionExplanation":{"none":{}},"filler":"11111111111111111111111111111111","fillRecordId":"00","marketIndex":"00","baseAssetAmountFilled":"00","quoteAssetAmountFilled":"00","makerRebate":"00","takerFee":"00","fillerReward":"00","quoteAssetAmountSurplus":"00","oraclePrice":"53ed435b20","txSig":"3nA44VjeCC8swADRyxuiSRWhvfGu6HpUu2xnueaqNkSH9ii9c1nPorjmPzcmooMhyD3PMCirVdnMRAtiJ1zRb8YG","eventType":"OrderRecord"}
		{"ts":"62e0abfc","slot":150777356,"taker":"3zztWnffdWvgsu9zTVWq1HP5xh29WexTv7tbyC6Uqy8V","maker":"11111111111111111111111111111111","takerOrder":{"status":{"filled":{}},"orderType":{"market":{}},"ts":"62e0abf7","slot":"08fcadff","orderId":"0a","userOrderId":0,"marketIndex":"00","price":"00","existingPositionDirection":{"short":{}},"quoteAssetAmount":"00","baseAssetAmount":"5af3107a4000","baseAssetAmountFilled":"5af3107a4000","quoteAssetAmountFilled":"157916f3","fee":"057f41","direction":{"long":{}},"reduceOnly":true,"postOnly":false,"immediateOrCancel":false,"discountTier":{"none":{}},"triggerPrice":"00","triggerCondition":{"above":{}},"triggered":false,"referrer":"11111111111111111111111111111111","oraclePriceOffset":"00","auctionStartPrice":"4ef1cd29f0","auctionEndPrice":"53fe8975b8","auctionDuration":10,"padding":[0,0,0]},"makerOrder":{"status":{"init":{}},"orderType":{"limit":{}},"ts":"00","slot":"00","orderId":"00","userOrderId":0,"marketIndex":"00","price":"00","existingPositionDirection":{"long":{}},"quoteAssetAmount":"00","baseAssetAmount":"00","baseAssetAmountFilled":"00","quoteAssetAmountFilled":"00","fee":"00","direction":{"long":{}},"reduceOnly":false,"postOnly":false,"immediateOrCancel":false,"discountTier":{"none":{}},"triggerPrice":"00","triggerCondition":{"above":{}},"triggered":false,"referrer":"11111111111111111111111111111111","oraclePriceOffset":"00","auctionStartPrice":"00","auctionEndPrice":"00","auctionDuration":0,"padding":[0,0,0]},"makerUnsettledPnl":"00","takerUnsettledPnl":"02aa6efe","action":{"fill":{}},"actionExplanation":{"none":{}},"filler":"A8GgA3ZREa73hGV9pZQkEzHUy46dLWUkSC6Q1yPghYNQ","fillRecordId":"0268","marketIndex":"00","baseAssetAmountFilled":"5af3107a4000","quoteAssetAmountFilled":"157916f3","makerRebate":"00","takerFee":"057f41","fillerReward":"3a34","quoteAssetAmountSurplus":"030e3d","oraclePrice":"53cf7d9740","txSig":"4LrN1mMPEeoogDtN7dfTcY3gADK4rxWq45Gwegd4GeLtBRXv1E1xRsNGKQ1bc87jR7uhLi1m9TN77ysSXJdZRUjt","eventType":"OrderRecord"
		{"ts":"62e3091b","slot":151185608,"taker":"tktkRu1DM5cioU8JMyBQvXud2ENayz2mbbU1idUqTmn","maker":"DJwD8T2TKev7asmcvPyU9BUhTsjH5yZYEwoKDoHHcicu","takerOrder":{"status":{"open":{}},"orderType":{"market":{}},"ts":"62e3091a","slot":"0902e8c5","orderId":"02","userOrderId":0,"marketIndex":"00","price":"65653a0c80","existingPositionDirection":{"long":{}},"baseAssetAmount":"0c8d10191000","baseAssetAmountFilled":"0646880c8800","quoteAssetAmountFilled":"01c0ae02","fee":"72dc","direction":{"long":{}},"reduceOnly":false,"postOnly":false,"immediateOrCancel":false,"discountTier":{"none":{}},"triggerPrice":"00","triggerCondition":{"above":{}},"triggered":false,"referrer":"11111111111111111111111111111111","oraclePriceOffset":"00","auctionStartPrice":"6338ccb08c","auctionEndPrice":"65653a0c80","auctionDuration":10,"padding":[0,0,0]},"makerOrder":{"status":{"filled":{}},"orderType":{"limit":{}},"ts":"62e3091b","slot":"0902e8c8","orderId":"05","userOrderId":0,"marketIndex":"00","price":"6338ccb08c","existingPositionDirection":{"short":{}},"baseAssetAmount":"0646880c8800","baseAssetAmountFilled":"0646880c8800","quoteAssetAmountFilled":"01c0ae02","fee":"-44ea","direction":{"short":{}},"reduceOnly":false,"postOnly":true,"immediateOrCancel":true,"discountTier":{"none":{}},"triggerPrice":"00","triggerCondition":{"above":{}},"triggered":false,"referrer":"11111111111111111111111111111111","oraclePriceOffset":"00","auctionStartPrice":"00","auctionEndPrice":"00","auctionDuration":10,"padding":[0,0,0]},"makerUnsettledPnl":"44ea","takerUnsettledPnl":"-72dc","action":{"fill":{}},"actionExplanation":{"none":{}},"filler":"DJwD8T2TKev7asmcvPyU9BUhTsjH5yZYEwoKDoHHcicu","fillRecordId":"08","marketIndex":"00","baseAssetAmountFilled":"0646880c8800","quoteAssetAmountFilled":"01c0ae02","makerRebate":"44ea","takerFee":"72dc","fillerReward":"00","quoteAssetAmountSurplus":"00","oraclePrice":"64d112cd80","txSig":"5QyEApbEuZsuZebciVMwVvCEkb4neLQHrqkQ97QKVL1f9cqW7Z28SxMzos8EQ19xsLqK4Nsxk4XhW2i8JR3DU6jY","eventType":"OrderRecord"}
		*/
		logger.info(`Received an order record ${JSON.stringify(record)}`);

		Promise.all(bots.map((bot) => bot.trigger(record)));
	};

	eventSubscriber.eventEmitter.on('newEvent', (event) => {
		if (event.eventType === 'OrderRecord') {
			handleOrderRecord(event as OrderRecord);
		} else {
			logger.info(`order record event type ${event.eventType}`);
		}
	});

	if (opts.printInfo) {
		setInterval(() => {
			for (const m of DevnetMarkets) {
				if (bots.length === 0) {
					break;
				}
				const dlob = bots[0].viewDlob();
				if (dlob) {
					dlob.printTopOfOrderLists(
						sdkConfig,
						clearingHouse,
						slotSubscriber,
						m.marketIndex
					);
				}
			}
			printUserAccountStats(clearingHouseUser);
			printOpenPositions(clearingHouseUser);
		}, 60000);
	}

	// reset the bots every 15 minutes, it looks like it holds onto stale DLOB orders :shrug:
	setInterval(async () => {
		for await (const bot of bots) {
			bot.reset();
			await bot.init();
		}
	}, 15 * 60 * 1000);
};

async function recursiveTryCatch(f: () => void) {
	try {
		await f();
	} catch (e) {
		console.error(e);
		for (const bot of bots) {
			bot.reset();
			await bot.init();
		}
		await sleep(15000);
		await recursiveTryCatch(f);
	}
}

recursiveTryCatch(() => runBot(wallet, clearingHouse));

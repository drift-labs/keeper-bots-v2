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
import { LiquidatorBot } from './bots/liquidator';
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
	.option('--filler', 'Enable filler bot')
	.option('--trigger', 'Enable trigger bot')
	.option('--jit-maker', 'Enable JIT auction maker bot')
	.option('--liquidator', 'Enable liquidator bot')
	.option('--print-info', 'Periodically print market and position info')
	.option('--cancel-open-orders', 'Cancel open orders on startup')
	.option('--close-open-positions', 'close all open positions')
	.option(
		'--force-deposit <number>',
		'Force deposit this amount of USDC to collateral account, the program will end after the deposit transaction is sent'
	)
	.option(
		'--metrics', // TODO: allow custom url and port
		'Enable Prometheus metric scraper'
	)
	.addOption(
		new Option(
			'-p, --private-key <string>',
			'private key, supports path to id.json, or list of comma separate numbers'
		).env('KEEPER_PRIVATE_KEY')
	)
	.parse();

const opts = program.opts();

logger.info(
	`Dry run: ${!!opts.dry}, FillerBot enabled: ${!!opts.filler}, TriggerBot enabled: ${!!opts.trigger} JitMakerBot enabled: ${!!opts.jitMaker}`
);

export function getWallet(): Wallet {
	const privateKey = process.env.KEEPER_PRIVATE_KEY;
	if (!privateKey) {
		throw new Error(
			'Must set environment variable KEEPER_PRIVATE_KEY with the path to a id.json or a list of commma separated numbers'
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

function sleep(ms) {
	return new Promise((resolve) => setTimeout(resolve, ms));
}

function printUserAccountStats(clearingHouseUser: ClearingHouseUser) {
	const freeCollateral = clearingHouseUser.getFreeCollateral();
	logger.info(
		`User free collateral: $${convertToNumber(
			freeCollateral,
			QUOTE_PRECISION
		)}:`
	);
	/*
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
	*/

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
		/*
		console.log(
			` . unsettledPnl:     ${convertToNumber(
				p.unsettledPnl,
				QUOTE_PRECISION
			).toString()}`
		);
		*/
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
const runBot = async () => {
	const wallet = getWallet();
	const clearingHousePublicKey = new PublicKey(
		sdkConfig.CLEARING_HOUSE_PROGRAM_ID
	);

	const connection = new Connection(endpoint, 'finalized');

	const bulkAccountLoader = new BulkAccountLoader(
		connection,
		'confirmed',
		1000
	);
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

	const eventSubscriber = new EventSubscriber(
		connection,
		clearingHouse.program,
		{
			maxTx: 8192,
			maxEventsPerType: 8192,
			orderBy: 'blockchain',
			orderDir: 'desc',
			commitment: 'confirmed',
			logProviderConfig: {
				type: 'polling',
				frequency: 1000,
				// type: 'websocket',
			},
		}
	);

	const slotSubscriber = new SlotSubscriber(connection, {});

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
		!(await clearingHouse.subscribe()) ||
		!(await clearingHouseUser.subscribe()) ||
		!eventSubscriber.subscribe()
	) {
		logger.info('waiting to subscribe to ClearingHouse and ClearingHouseUser');
		await sleep(1000);
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
	if (opts.closeOpenPositions) {
		logger.info(`Closing open positions`);
		for await (const p of clearingHouseUser.getUserAccount().positions) {
			if (p.baseAssetAmount.isZero()) {
				logger.info(`no position on market: ${p.marketIndex.toNumber()}`);
				continue;
			}
			logger.info(`closing position on ${p.marketIndex.toNumber()}`);
			logger.info(` . ${await clearingHouse.closePosition(p.marketIndex)}`);
		}
	}

	// check that user has collateral
	const freeCollateral = clearingHouseUser.getFreeCollateral();
	if (freeCollateral.isZero() && opts.jitMaker) {
		throw new Error(
			`No collateral in account, collateral is required to run JitMakerBot, run with --force-deposit flag to deposit collateral`
		);
	}
	if (opts.forceDeposit) {
		logger.info(
			`Depositing (${new BN(
				opts.forceDeposit
			).toString()} USDC to collateral account)`
		);

		if (opts.forceDeposit < 0) {
			logger.error(`Deposit amount must be greater than 0`);
			throw new Error('Deposit amount must be greater than 0');
		}

		const ata = await Token.getAssociatedTokenAddress(
			ASSOCIATED_TOKEN_PROGRAM_ID,
			TOKEN_PROGRAM_ID,
			DevnetBanks[0].mint, // USDC
			wallet.publicKey
		);

		const tx = await clearingHouse.deposit(
			new BN(opts.forceDeposit).mul(QUOTE_PRECISION),
			new BN(0), // USDC bank
			ata
		);
		logger.info(`Deposit transaction: ${tx}`);
		logger.info(`exiting...run again without --force-deposit flag`);
		return;
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
				metrics
			)
		);
	}
	if (opts.liquidator) {
		bots.push(
			new LiquidatorBot('liquidator', !!opts.dry, clearingHouse, metrics)
		);
	}

	for (const bot of bots) {
		await bot.init();
	}

	for (const bot of bots) {
		bot.startIntervalLoop(bot.defaultIntervalMs);
	}

	eventSubscriber.eventEmitter.on('newEvent', async (event) => {
		Promise.all(bots.map((bot) => bot.trigger(event)));
	});

	if (opts.printInfo) {
		setInterval(() => {
			for (const m of DevnetMarkets) {
				if (bots.length === 0) {
					break;
				}
				const dlob = bots[0].viewDlob();
				if (dlob) {
					metrics.trackObjectSize('dlob', dlob);
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
		for (const bot of bots) {
			bot.startIntervalLoop(bot.defaultIntervalMs);
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

recursiveTryCatch(() => runBot());

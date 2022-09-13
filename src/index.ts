import fs from 'fs';
import { program, Option } from 'commander';
import * as http from 'http';

import { Connection, Commitment, Keypair, PublicKey } from '@solana/web3.js';

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
import { promiseTimeout } from '@drift-labs/sdk/lib/util/promiseTimeout';
import { Mutex } from 'async-mutex';

import { logger, setLogLevel } from './logger';
import { constants } from './types';
import { FillerBot } from './bots/filler';
import { TriggerBot } from './bots/trigger';
import { JitMakerBot } from './bots/jitMaker';
import { LiquidatorBot } from './bots/liquidator';
import { FloatingMakerBot } from './bots/floatingMaker';
import { Bot } from './types';
import { Metrics } from './metrics';
import { PnlSettlerBot } from './bots/pnlSettler';

require('dotenv').config();
const driftEnv = process.env.ENV as DriftEnv;
//@ts-ignore
const sdkConfig = initialize({ env: process.env.ENV });

const stateCommitment: Commitment = 'confirmed';
const healthCheckPort = process.env.HEALTH_CHECK_PORT || 8888;

program
	.option('-d, --dry-run', 'Dry run, do not send transactions on chain')
	.option(
		'--init-user',
		'calls clearingHouse.initializeUserAccount if no user account exists'
	)
	.option('--filler', 'Enable filler bot')
	.option('--trigger', 'Enable trigger bot')
	.option('--jit-maker', 'Enable JIT auction maker bot')
	.option('--floating-maker', 'Enable floating maker bot')
	.option('--liquidator', 'Enable liquidator bot')
	.option('--pnl-settler', 'Enable PnL settler bot')
	.option('--cancel-open-orders', 'Cancel open orders on startup')
	.option('--close-open-positions', 'close all open positions')
	.option('--test-liveness', 'Purposefully fail liveness test after 1 minute')
	.option(
		'--force-deposit <number>',
		'Force deposit this amount of USDC to collateral account, the program will end after the deposit transaction is sent'
	)
	.option('--metrics <number>', 'Enable Prometheus metric scraper')
	.addOption(
		new Option(
			'-p, --private-key <string>',
			'private key, supports path to id.json, or list of comma separate numbers'
		).env('KEEPER_PRIVATE_KEY')
	)
	.option('--debug', 'Enable debug logging')
	.parse();

const opts = program.opts();
setLogLevel(opts.debug ? 'debug' : 'info');

logger.info(
	`Dry run: ${!!opts.dry}, FillerBot enabled: ${!!opts.filler}, TriggerBot enabled: ${!!opts.trigger} JitMakerBot enabled: ${!!opts.jitMaker} PnlSettler enabled: ${!!opts.pnlSettler}`
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
logger.info(`RPC endpoint: ${endpoint}`);

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

	const connection = new Connection(endpoint, stateCommitment);

	const bulkAccountLoader = new BulkAccountLoader(
		connection,
		stateCommitment,
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
		userStats: true,
	});

	const eventSubscriber = new EventSubscriber(
		connection,
		clearingHouse.program,
		{
			maxTx: 8192,
			maxEventsPerType: 8192,
			orderBy: 'blockchain',
			orderDir: 'desc',
			commitment: stateCommitment,
			logProviderConfig: {
				type: 'polling',
				frequency: 1000,
				// type: 'websocket',
			},
		}
	);

	const slotSubscriber = new SlotSubscriber(connection, {});
	const lastSlotReceivedMutex = new Mutex();
	let lastSlotReceived: number;
	let lastHealthCheckSlot = -1;
	const startupTime = Date.now();

	const lamportsBalance = await connection.getBalance(wallet.publicKey);
	logger.info(
		`ClearingHouse ProgramId: ${clearingHouse.program.programId.toBase58()}`
	);
	logger.info(`Wallet pubkey: ${wallet.publicKey.toBase58()}`);
	logger.info(` . SOL balance: ${lamportsBalance / 10 ** 9}`);
	const tokenAccount = await Token.getAssociatedTokenAddress(
		ASSOCIATED_TOKEN_PROGRAM_ID,
		TOKEN_PROGRAM_ID,
		new PublicKey(constants.devnet.USDCMint),
		wallet.publicKey
	);
	const usdcBalance = await connection.getTokenAccountBalance(tokenAccount);
	logger.info(` . USDC balance: ${usdcBalance.value.uiAmount}`);

	await clearingHouse.subscribe();
	clearingHouse.eventEmitter.on('error', (e) => {
		logger.info('clearing house error');
		logger.error(e);
	});

	eventSubscriber.subscribe();
	await slotSubscriber.subscribe();
	slotSubscriber.eventEmitter.on('newSlot', async (slot: number) => {
		await lastSlotReceivedMutex.runExclusive(async () => {
			lastSlotReceived = slot;
		});
	});

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
	await clearingHouse.fetchAccounts();
	await clearingHouse.getUser().fetchAccounts();

	let metrics: Metrics | undefined = undefined;
	if (opts.metrics) {
		metrics = new Metrics(clearingHouse, parseInt(opts?.metrics));
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
	if (freeCollateral.isZero() && opts.jitMaker && !opts.forceDeposit) {
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
	if (opts.floatingMaker) {
		bots.push(
			new FloatingMakerBot(
				'floatingMaker',
				!!opts.dry,
				clearingHouse,
				slotSubscriber,
				metrics
			)
		);
	}

	if (opts.pnlSettler) {
		bots.push(
			new PnlSettlerBot(
				'pnlSettler',
				!!opts.dry,
				clearingHouse,
				// update to current env markets before deploy to mainnet
				DevnetMarkets,
				metrics
			)
		);
	}

	logger.info(`initializing bots`);
	await Promise.all(bots.map((bot) => bot.init()));

	logger.info(`starting bots`);
	await Promise.all(
		bots.map((bot) => bot.startIntervalLoop(bot.defaultIntervalMs))
	);

	eventSubscriber.eventEmitter.on('newEvent', async (event) => {
		Promise.all(bots.map((bot) => bot.trigger(event)));
	});

	// start http server listening to /health endpoint using http package
	http
		.createServer(async (req, res) => {
			if (req.url === '/health') {
				if (opts.testLiveness) {
					if (Date.now() > startupTime + 60 * 1000) {
						res.writeHead(500);
						res.end('Testing liveness test fail');
						return;
					}
				}
				// check if a slot was received recently
				let healthySlot = false;
				await lastSlotReceivedMutex.runExclusive(async () => {
					healthySlot = lastSlotReceived > lastHealthCheckSlot;
					logger.debug(
						`Health check: lastSlotReceived: ${lastSlotReceived}, lastHealthCheckSlot: ${lastHealthCheckSlot}, healthySlot: ${healthySlot}`
					);
					if (healthySlot) {
						lastHealthCheckSlot = lastSlotReceived;
					}
				});
				if (!healthySlot) {
					res.writeHead(500);
					res.end(`SlotSubscriber is not healthy`);
					return;
				}

				// check all bots if they're live
				for (const bot of bots) {
					const healthCheck = await promiseTimeout(bot.healthCheck(), 1000);
					if (!healthCheck) {
						logger.error(`Health check failed for bot ${bot.name}`);
						res.writeHead(500);
						res.end(`Bot ${bot.name} is not healthy`);
						return;
					}
				}

				// liveness check passed
				res.writeHead(200);
				res.end('OK');
			} else {
				res.writeHead(404);
				res.end('Not found');
			}
		})
		.listen(healthCheckPort);
	logger.info(`Health check server listening on port ${healthCheckPort}`);
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

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
	getVariant,
	BulkAccountLoader,
	DriftClient,
	User,
	initialize,
	Wallet,
	DriftEnv,
	EventSubscriber,
	SlotSubscriber,
	convertToNumber,
	QUOTE_PRECISION,
	SPOT_MARKET_BALANCE_PRECISION,
	SpotMarkets,
	PerpMarkets,
	BN,
	BASE_PRECISION,
	getSignedTokenAmount,
	TokenFaucet,
} from '@drift-labs/sdk';
import { promiseTimeout } from '@drift-labs/sdk/lib/util/promiseTimeout';
import { Mutex } from 'async-mutex';

import { logger, setLogLevel } from './logger';
import { constants } from './types';
import { FillerBot } from './bots/filler';
import { SpotFillerBot } from './bots/spotFiller';
import { TriggerBot } from './bots/trigger';
import { JitMakerBot } from './bots/jitMaker';
import { LiquidatorBot } from './bots/liquidator';
import { FloatingPerpMakerBot } from './bots/floatingMaker';
import { Bot } from './types';
import { Metrics } from './metrics';
import { PnlSettlerBot } from './bots/pnlSettler';
import {
	getOrCreateAssociatedTokenAccount,
	TOKEN_FAUCET_PROGRAM_ID,
} from './utils';
import { PrometheusExporter } from '@opentelemetry/exporter-prometheus';
import { bs58 } from '@project-serum/anchor/dist/cjs/utils/bytes';

require('dotenv').config();
const driftEnv = (process.env.ENV || 'devnet') as DriftEnv;
const commitHash = process.env.COMMIT;
//@ts-ignore
const sdkConfig = initialize({ env: process.env.ENV });

const stateCommitment: Commitment = 'confirmed';
const healthCheckPort = process.env.HEALTH_CHECK_PORT || 8888;
const metricsPort =
	process.env.METRICS_PORT ||
	PrometheusExporter.DEFAULT_OPTIONS.port.toString();

program
	.option('-d, --dry-run', 'Dry run, do not send transactions on chain')
	.option(
		'--init-user',
		'calls clearingHouse.initializeUserAccount if no user account exists'
	)
	.option('--filler', 'Enable filler bot')
	.option('--spot-filler', 'Enable spot filler bot')
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

logger.info(`Dry run: ${!!opts.dry},\n\
FillerBot enabled: ${!!opts.filler},\n\
SpotFillerBot enabled: ${!!opts.spotFiller},\n\
TriggerBot enabled: ${!!opts.trigger},\n\
JitMakerBot enabled: ${!!opts.jitMaker},\n\
PnlSettler enabled: ${!!opts.pnlSettler},\n\
`);

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
		if (privateKey.includes(',')) {
			logger.info(`Trying to load private key as comma separated numbers`);
			loadedKey = Uint8Array.from(
				privateKey.split(',').map((val) => Number(val))
			);
		} else {
			logger.info(`Trying to load private key as base58 string`);
			loadedKey = new Uint8Array(bs58.decode(privateKey));
		}
	}

	const keypair = Keypair.fromSecretKey(Uint8Array.from(loadedKey));
	return new Wallet(keypair);
}

const endpoint = process.env.ENDPOINT;
logger.info(`RPC endpoint: ${endpoint}`);
logger.info(`DriftEnv:     ${driftEnv}`);
logger.info(`Commit:       ${commitHash}`);

function sleep(ms) {
	return new Promise((resolve) => setTimeout(resolve, ms));
}

function printUserAccountStats(clearingHouseUser: User) {
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

function printOpenPositions(clearingHouseUser: User) {
	logger.info('Open Perp Positions:');
	for (const p of clearingHouseUser.getUserAccount().perpPositions) {
		if (p.baseAssetAmount.isZero()) {
			continue;
		}
		const market = PerpMarkets[driftEnv][p.marketIndex];
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
			` . openOrders: ${p.openOrders.toString()}, openBids: ${convertToNumber(
				p.openBids,
				BASE_PRECISION
			)}, openAsks: ${convertToNumber(p.openAsks, BASE_PRECISION)}`
		);
	}

	logger.info('Open Spot Positions:');
	for (const p of clearingHouseUser.getUserAccount().spotPositions) {
		if (p.scaledBalance.isZero()) {
			continue;
		}
		const market = SpotMarkets[driftEnv][p.marketIndex];
		console.log(`[${market.symbol}]`);
		console.log(
			` . baseAssetAmount:  ${convertToNumber(
				getSignedTokenAmount(p.scaledBalance, p.balanceType),
				SPOT_MARKET_BALANCE_PRECISION
			).toString()}`
		);
		console.log(` . balanceType: ${getVariant(p.balanceType)}`);
		console.log(
			` . openOrders: ${p.openOrders.toString()}, openBids: ${convertToNumber(
				p.openBids,
				SPOT_MARKET_BALANCE_PRECISION
			)}, openAsks: ${convertToNumber(
				p.openAsks,
				SPOT_MARKET_BALANCE_PRECISION
			)}`
		);
	}
}

const bots: Bot[] = [];
const runBot = async () => {
	const wallet = getWallet();
	const clearingHousePublicKey = new PublicKey(sdkConfig.DRIFT_PROGRAM_ID);

	const connection = new Connection(endpoint, stateCommitment);

	const bulkAccountLoader = new BulkAccountLoader(
		connection,
		stateCommitment,
		1000
	);
	let lastBulkAccountLoaderSlot = bulkAccountLoader.mostRecentSlot;
	const driftClient = new DriftClient({
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

	const eventSubscriber = new EventSubscriber(connection, driftClient.program, {
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
	});

	const slotSubscriber = new SlotSubscriber(connection, {});
	const lastSlotReceivedMutex = new Mutex();
	let lastSlotReceived: number;
	let lastHealthCheckSlot = -1;
	const startupTime = Date.now();

	const lamportsBalance = await connection.getBalance(wallet.publicKey);
	logger.info(
		`DriftClient ProgramId: ${driftClient.program.programId.toBase58()}`
	);
	logger.info(`Wallet pubkey: ${wallet.publicKey.toBase58()}`);
	logger.info(` . SOL balance: ${lamportsBalance / 10 ** 9}`);

	try {
		const tokenAccount = await getOrCreateAssociatedTokenAccount(
			connection,
			new PublicKey(constants[driftEnv].USDCMint),
			wallet
		);
		const usdcBalance = await connection.getTokenAccountBalance(tokenAccount);
		logger.info(` . USDC balance: ${usdcBalance.value.uiAmount}`);
	} catch (e) {
		logger.info(`Failed to load USDC token account: ${e}`);
	}

	await driftClient.subscribe();
	driftClient.eventEmitter.on('error', (e) => {
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

	if (!(await driftClient.getUser().exists())) {
		logger.error(`User for ${wallet.publicKey} does not exist`);
		if (opts.initUser) {
			logger.info(`Creating User for ${wallet.publicKey}`);
			const [txSig] = await driftClient.initializeUserAccount();
			logger.info(`Initialized user account in transaction: ${txSig}`);
		} else {
			throw new Error("Run with '--init-user' flag to initialize a User");
		}
	}

	// subscribe will fail if there is no clearing house user
	const driftUser = driftClient.getUser();
	while (
		!(await driftClient.subscribe()) ||
		!(await driftUser.subscribe()) ||
		!eventSubscriber.subscribe()
	) {
		logger.info('waiting to subscribe to DriftClient and User');
		await sleep(1000);
	}
	logger.info(
		`User PublicKey: ${driftUser.getUserAccountPublicKey().toBase58()}`
	);
	await driftClient.fetchAccounts();
	await driftClient.getUser().fetchAccounts();

	let metrics: Metrics | undefined = undefined;
	if (opts.metrics) {
		metrics = new Metrics(driftClient, parseInt(opts?.metrics));
		await metrics.init();
	}

	printUserAccountStats(driftUser);
	if (opts.closeOpenPositions) {
		logger.info(`Closing open perp positions`);
		let closedPerps = 0;
		for await (const p of driftUser.getUserAccount().perpPositions) {
			if (p.baseAssetAmount.isZero()) {
				logger.info(`no position on market: ${p.marketIndex}`);
				continue;
			}
			logger.info(`closing position on ${p.marketIndex}`);
			logger.info(` . ${await driftClient.closePosition(p.marketIndex)}`);
			closedPerps++;
		}
		console.log(`Closed ${closedPerps} spot positions`);

		let closedSpots = 0;
		for await (const p of driftUser.getUserAccount().spotPositions) {
			if (p.scaledBalance.isZero()) {
				logger.info(`no position on market: ${p.marketIndex}`);
				continue;
			}
			logger.info(`closing position on ${p.marketIndex}`);
			logger.info(` . ${await driftClient.closePosition(p.marketIndex)}`);
			closedSpots++;
		}
		console.log(`Closed ${closedSpots} spot positions`);
	}

	// check that user has collateral
	const freeCollateral = driftUser.getFreeCollateral();
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

		const mint = SpotMarkets[driftEnv][0].mint; // TODO: are index 0 always USDC???, support other collaterals

		const ata = await Token.getAssociatedTokenAddress(
			ASSOCIATED_TOKEN_PROGRAM_ID,
			TOKEN_PROGRAM_ID,
			mint,
			wallet.publicKey
		);

		const amount = new BN(opts.forceDeposit).mul(QUOTE_PRECISION);

		if (driftEnv == 'devnet') {
			const tokenFaucet = new TokenFaucet(
				connection,
				wallet,
				TOKEN_FAUCET_PROGRAM_ID,
				mint,
				opts
			);
			await tokenFaucet.mintToUser(ata, amount);
		}

		const tx = await driftClient.deposit(
			amount,
			0, // USDC bank
			ata
		);
		logger.info(`Deposit transaction: ${tx}`);
		logger.info(`exiting...run again without --force-deposit flag`);
		return;
	}

	// print user orders
	logger.info('');
	const ordersToCancel: Array<number> = [];
	for (const order of driftUser.getUserAccount().orders) {
		if (order.baseAssetAmount.isZero()) {
			continue;
		}
		ordersToCancel.push(order.orderId);
	}
	if (opts.cancelOpenOrders) {
		for (const order of ordersToCancel) {
			logger.info(`Cancelling open order ${order.toString()}`);
			await driftClient.cancelOrder(order);
		}
	}

	printOpenPositions(driftUser);

	/*
	 * Start bots depending on flags enabled
	 */

	if (opts.filler) {
		bots.push(
			new FillerBot(
				'filler',
				!!opts.dry,
				bulkAccountLoader,
				driftClient,
				{
					rpcEndpoint: endpoint,
					commit: commitHash,
					driftEnv: driftEnv,
					driftPid: clearingHousePublicKey.toBase58(),
					walletAuthority: wallet.publicKey.toBase58(),
				},
				parseInt(metricsPort)
			)
		);
	}
	if (opts.spotFiller) {
		bots.push(
			new SpotFillerBot(
				'spotFiller',
				!!opts.dry,
				bulkAccountLoader,
				driftClient,
				{
					rpcEndpoint: endpoint,
					commit: commitHash,
					driftEnv: driftEnv,
					driftPid: clearingHousePublicKey.toBase58(),
					walletAuthority: wallet.publicKey.toBase58(),
				},
				parseInt(metricsPort)
			)
		);
	}
	if (opts.trigger) {
		bots.push(
			new TriggerBot(
				'trigger',
				!!opts.dry,
				bulkAccountLoader,
				driftClient,
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
				driftClient,
				slotSubscriber,
				metrics
			)
		);
	}
	if (opts.liquidator) {
		bots.push(
			new LiquidatorBot(
				'liquidator',
				!!opts.dry,
				bulkAccountLoader,
				driftClient,
				{
					rpcEndpoint: endpoint,
					commit: commitHash,
					driftEnv: driftEnv,
					driftPid: clearingHousePublicKey.toBase58(),
					walletAuthority: wallet.publicKey.toBase58(),
				},
				parseInt(metricsPort.toString())
			)
		);
	}
	if (opts.floatingMaker) {
		bots.push(
			new FloatingPerpMakerBot(
				'floatingMaker',
				!!opts.dry,
				driftClient,
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
				driftClient,
				PerpMarkets[driftEnv],
				SpotMarkets[driftEnv],
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
				let healthySlotSubscriber = false;
				await lastSlotReceivedMutex.runExclusive(async () => {
					healthySlotSubscriber = lastSlotReceived > lastHealthCheckSlot;
					logger.debug(
						`Health check: lastSlotReceived: ${lastSlotReceived}, lastHealthCheckSlot: ${lastHealthCheckSlot}, healthySlot: ${healthySlotSubscriber}`
					);
					if (healthySlotSubscriber) {
						lastHealthCheckSlot = lastSlotReceived;
					}
				});
				if (!healthySlotSubscriber) {
					res.writeHead(500);
					logger.error(`SlotSubscriber is not healthy`);
					res.end(`SlotSubscriber is not healthy`);
					return;
				}

				if (bulkAccountLoader) {
					// we expect health checks to happen at a rate slower than the BulkAccountLoader's polling frequency
					if (bulkAccountLoader.mostRecentSlot === lastBulkAccountLoaderSlot) {
						res.writeHead(500);
						res.end(`bulkAccountLoader.mostRecentSlot is not healthy`);
						logger.error(
							`Health check failed due to stale bulkAccountLoader.mostRecentSlot`
						);
						return;
					}
					lastBulkAccountLoaderSlot = bulkAccountLoader.mostRecentSlot;
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

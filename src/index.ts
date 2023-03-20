import fs from 'fs';
import { program, Option } from 'commander';
import * as http from 'http';

import { Connection, Commitment, Keypair, PublicKey } from '@solana/web3.js';
import {
	SearcherClient,
	searcherClient,
} from 'jito-ts/dist/sdk/block-engine/searcher';

import {
	Token,
	TOKEN_PROGRAM_ID,
	ASSOCIATED_TOKEN_PROGRAM_ID,
} from '@solana/spl-token';
import {
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
	SpotMarkets,
	PerpMarkets,
	BN,
	TokenFaucet,
	DriftClientSubscriptionConfig,
	LogProviderConfig,
} from '@drift-labs/sdk';
import { assert } from '@drift-labs/sdk/lib/assert/assert';
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
import { IFRevenueSettlerBot } from './bots/ifRevenueSettler';
import { UserPnlSettlerBot } from './bots/userPnlSettler';
import {
	getOrCreateAssociatedTokenAccount,
	TOKEN_FAUCET_PROGRAM_ID,
} from './utils';
import { bs58 } from '@project-serum/anchor/dist/cjs/utils/bytes';
import {
	Config,
	configHasBot,
	loadConfigFromFile,
	loadConfigFromOpts,
} from './config';

require('dotenv').config();
const driftEnv = (process.env.ENV || 'devnet') as DriftEnv;
const commitHash = process.env.COMMIT;
//@ts-ignore
const sdkConfig = initialize({ env: process.env.ENV });

const stateCommitment: Commitment = 'confirmed';
const healthCheckPort = process.env.HEALTH_CHECK_PORT || 8888;

program
	.option('-d, --dry-run', 'Dry run, do not send transactions on chain')
	.option(
		'--init-user',
		'calls driftClient.initializeUserAccount if no user account exists'
	)
	.option('--filler', 'Enable filler bot')
	.option('--spot-filler', 'Enable spot filler bot')
	.option('--trigger', 'Enable trigger bot')
	.option('--jit-maker', 'Enable JIT auction maker bot')
	.option('--floating-maker', 'Enable floating maker bot')
	.option('--liquidator', 'Enable liquidator bot')
	.option('--if-revenue-settler', 'Enable Insurance Fund PnL settler bot')
	.option('--user-pnl-settler', 'Enable User PnL settler bot')
	.option('--cancel-open-orders', 'Cancel open orders on startup')
	.option('--close-open-positions', 'close all open positions')
	.option('--test-liveness', 'Purposefully fail liveness test after 1 minute')
	.option(
		'--force-deposit <number>',
		'Force deposit this amount of USDC to collateral account, the program will end after the deposit transaction is sent'
	)
	.option('--metrics <number>', 'Enable Prometheus metric scraper (deprecated)')
	.addOption(
		new Option(
			'-p, --private-key <string>',
			'private key, supports path to id.json, or list of comma separate numbers'
		).env('KEEPER_PRIVATE_KEY')
	)
	.option('--debug', 'Enable debug logging')
	.option(
		'--run-once',
		'Exit after running bot loops once (only for supported bots)'
	)
	.option(
		'--websocket',
		'Use websocket instead of RPC polling for account updates'
	)
	.option(
		'--disable-auto-derisking',
		'Set to disable auto derisking (primarily used for liquidator to close inherited positions)'
	)
	.option(
		'--subaccount <string>',
		'subaccount(s) to use (comma delimited), specify which subaccountsIDs to load',
		'0'
	)
	.option(
		'--perp-markets <string>',
		'comma delimited list of perp market ID(s) to liquidate (willing to inherit risk), omit for all',
		''
	)
	.option(
		'--spot-markets <string>',
		'comma delimited list of spot market ID(s) to liquidate (willing to inherit risk), omit for all',
		''
	)
	.option(
		'--transaction-version <number>',
		'Select transaction version (omit for legacy transaction)',
		''
	)
	.option(
		'--config-file <string>',
		'Config file to load (yaml format), will override any other config options',
		''
	)
	.option(
		'--use-jito',
		'Submit transactions to a Jito relayer if the bot supports it'
	)
	.parse();

const opts = program.opts();
let config: Config = undefined;
if (opts.configFile) {
	logger.info(`Loading config from ${opts.configFile}`);
	config = loadConfigFromFile(opts.configFile);
} else {
	logger.info(`Loading config from command line options`);
	config = loadConfigFromOpts(opts);
}
logger.info(
	`Bot config:\n${JSON.stringify(
		config,
		(k, v) => {
			if (k === 'keeperPrivateKey') {
				return '*'.repeat(v.length);
			}
			return v;
		},
		2
	)}`
);

setLogLevel(config.global.debug ? 'debug' : 'info');

function loadKeypair(privateKey: string): Keypair {
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

	return Keypair.fromSecretKey(Uint8Array.from(loadedKey));
}

export function getWallet(): Wallet {
	const privateKey = config.global.keeperPrivateKey;
	if (!privateKey) {
		throw new Error(
			'Must set environment variable KEEPER_PRIVATE_KEY with the path to a id.json or a list of commma separated numbers'
		);
	}
	const keypair = loadKeypair(privateKey);
	return new Wallet(keypair);
}

const endpoint = config.global.endpoint;
const wsEndpoint = config.global.wsEndpoint;
logger.info(`RPC endpoint: ${endpoint}`);
logger.info(`WS endpoint:  ${wsEndpoint}`);
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

const bots: Bot[] = [];
const runBot = async () => {
	const wallet = getWallet();
	const driftPublicKey = new PublicKey(sdkConfig.DRIFT_PROGRAM_ID);

	const connection = new Connection(endpoint, {
		wsEndpoint: wsEndpoint,
		commitment: stateCommitment,
	});

	let bulkAccountLoader: BulkAccountLoader | undefined;
	let lastBulkAccountLoaderSlot: number | undefined;
	let accountSubscription: DriftClientSubscriptionConfig = {
		type: 'websocket',
	};
	let logProviderConfig: LogProviderConfig = {
		type: 'websocket',
	};

	if (!config.global.websocket) {
		bulkAccountLoader = new BulkAccountLoader(
			connection,
			stateCommitment,
			config.global.bulkAccountLoaderPollingInterval
		);
		lastBulkAccountLoaderSlot = bulkAccountLoader.mostRecentSlot;
		accountSubscription = {
			type: 'polling',
			accountLoader: bulkAccountLoader,
		};

		logProviderConfig = {
			type: 'polling',
			frequency: config.global.eventSubscriberPollingInterval,
		};
	}

	const driftClient = new DriftClient({
		connection,
		wallet,
		programID: driftPublicKey,
		perpMarketIndexes: PerpMarkets[driftEnv].map((mkt) => mkt.marketIndex),
		spotMarketIndexes: SpotMarkets[driftEnv].map((mkt) => mkt.marketIndex),
		oracleInfos: PerpMarkets[driftEnv].map((mkt) => {
			return { publicKey: mkt.oracle, source: mkt.oracleSource };
		}),
		opts: {
			commitment: stateCommitment,
			skipPreflight: false,
			preflightCommitment: stateCommitment,
		},
		accountSubscription,
		env: driftEnv,
		userStats: true,
		txSenderConfig: {
			type: 'retry',
			timeout: 35000,
		},
		activeSubAccountId: config.global.subaccounts[0],
		subAccountIds: config.global.subaccounts,
	});

	const eventSubscriber = new EventSubscriber(connection, driftClient.program, {
		maxTx: 8192,
		maxEventsPerType: 8192,
		orderBy: 'blockchain',
		orderDir: 'desc',
		commitment: stateCommitment,
		logProviderConfig,
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
		if (config.global.initUser) {
			logger.info(`Creating User for ${wallet.publicKey}`);
			const [txSig] = await driftClient.initializeUserAccount();
			logger.info(`Initialized user account in transaction: ${txSig}`);
		} else {
			throw new Error("Run with '--init-user' flag to initialize a User");
		}
	}

	// subscribe will fail if there is no clearing house user
	const driftUser = driftClient.getUser();
	const driftUserStats = driftClient.getUserStats();
	while (
		!(await driftClient.subscribe()) ||
		!(await driftUser.subscribe()) ||
		!(await driftUserStats.subscribe()) ||
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

	printUserAccountStats(driftUser);
	if (config.global.closeOpenPositions) {
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
	if (
		freeCollateral.isZero() &&
		configHasBot(config, 'jitMaker') &&
		!config.global.forceDeposit
	) {
		throw new Error(
			`No collateral in account, collateral is required to run JitMakerBot, run with --force-deposit flag to deposit collateral`
		);
	}
	if (config.global.forceDeposit) {
		logger.info(
			`Depositing (${new BN(
				config.global.forceDeposit
			).toString()} USDC to collateral account)`
		);

		if (config.global.forceDeposit < 0) {
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

		const amount = new BN(config.global.forceDeposit).mul(QUOTE_PRECISION);

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

	let jitoSearcherClient: SearcherClient | undefined;
	let jitoAuthKeypair: Keypair | undefined;
	if (config.global.useJito) {
		const jitoBlockEngineUrl = config.global.jitoBlockEngineUrl;
		const privateKey = config.global.jitoAuthPrivateKey;
		if (!jitoBlockEngineUrl) {
			throw new Error(
				'Must configure or set JITO_BLOCK_ENGINE_URL environment variable '
			);
		}
		if (!privateKey) {
			throw new Error(
				'Must configure or set JITO_AUTH_PRIVATE_KEY environment variable'
			);
		}
		jitoAuthKeypair = loadKeypair(privateKey);
		jitoSearcherClient = searcherClient(jitoBlockEngineUrl, jitoAuthKeypair);
		jitoSearcherClient.onBundleResult(
			(bundle) => {
				logger.info(`JITO bundle result: ${JSON.stringify(bundle)}`);
			},
			(error) => {
				logger.error(`JITO bundle error: ${error}`);
			}
		);
	}

	/*
	 * Start bots depending on flags enabled
	 */

	if (configHasBot(config, 'filler')) {
		bots.push(
			new FillerBot(
				slotSubscriber,
				bulkAccountLoader,
				driftClient,
				{
					rpcEndpoint: endpoint,
					commit: commitHash,
					driftEnv: driftEnv,
					driftPid: driftPublicKey.toBase58(),
					walletAuthority: wallet.publicKey.toBase58(),
				},
				config.botConfigs.filler,
				jitoSearcherClient,
				jitoAuthKeypair
			)
		);
	}
	if (configHasBot(config, 'spotFiller')) {
		bots.push(
			new SpotFillerBot(
				bulkAccountLoader,
				driftClient,
				{
					rpcEndpoint: endpoint,
					commit: commitHash,
					driftEnv: driftEnv,
					driftPid: driftPublicKey.toBase58(),
					walletAuthority: wallet.publicKey.toBase58(),
				},
				config.botConfigs.spotFiller
			)
		);
	}
	if (configHasBot(config, 'trigger')) {
		bots.push(
			new TriggerBot(
				bulkAccountLoader,
				driftClient,
				slotSubscriber,
				{
					rpcEndpoint: endpoint,
					commit: commitHash,
					driftEnv: driftEnv,
					driftPid: driftPublicKey.toBase58(),
					walletAuthority: wallet.publicKey.toBase58(),
				},
				config.botConfigs.trigger
			)
		);
	}
	if (configHasBot(config, 'jitMaker')) {
		bots.push(
			new JitMakerBot(
				driftClient,
				slotSubscriber,
				{
					rpcEndpoint: endpoint,
					commit: commitHash,
					driftEnv: driftEnv,
					driftPid: driftPublicKey.toBase58(),
					walletAuthority: wallet.publicKey.toBase58(),
				},
				config.botConfigs.trigger
			)
		);
	}

	if (configHasBot(config, 'liquidator')) {
		assert(
			config.global.subaccounts.length === 1,
			'Liquidator bot only works with one subaccount specified'
		);

		bots.push(
			new LiquidatorBot(
				bulkAccountLoader,
				driftClient,
				{
					rpcEndpoint: endpoint,
					commit: commitHash,
					driftEnv: driftEnv,
					driftPid: driftPublicKey.toBase58(),
					walletAuthority: wallet.publicKey.toBase58(),
				},
				config.botConfigs.liquidator
			)
		);
	}
	if (configHasBot(config, 'floatingMaker')) {
		bots.push(
			new FloatingPerpMakerBot(
				driftClient,
				slotSubscriber,
				{
					rpcEndpoint: endpoint,
					commit: commitHash,
					driftEnv: driftEnv,
					driftPid: driftPublicKey.toBase58(),
					walletAuthority: wallet.publicKey.toBase58(),
				},
				config.botConfigs.floatingMaker
			)
		);
	}

	if (configHasBot(config, 'userPnlSettler')) {
		bots.push(
			new UserPnlSettlerBot(
				driftClient,
				PerpMarkets[driftEnv],
				SpotMarkets[driftEnv],
				config.botConfigs.userPnlSettler
			)
		);
	}

	if (configHasBot(config, 'ifRevenueSettler')) {
		bots.push(
			new IFRevenueSettlerBot(
				driftClient,
				SpotMarkets[driftEnv],
				config.botConfigs.ifRevenueSettler
			)
		);
	}

	logger.info(`initializing bots`);
	await Promise.all(bots.map((bot) => bot.init()));

	logger.info(`starting bots (runOnce: ${config.global.runOnce})`);
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
				if (config.global.testLiveness) {
					if (Date.now() > startupTime + 60 * 1000) {
						res.writeHead(500);
						res.end('Testing liveness test fail');
						return;
					}
				}

				if (config.global.websocket) {
					/* @ts-ignore */
					if (!driftClient.connection._rpcWebSocketConnected) {
						logger.error(`Connection rpc websocket disconnected`);
						res.writeHead(500);
						res.end(`Connection rpc websocket disconnected`);
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
					res.writeHead(501);
					logger.error(`SlotSubscriber is not healthy`);
					res.end(`SlotSubscriber is not healthy`);
					return;
				}

				if (bulkAccountLoader) {
					// we expect health checks to happen at a rate slower than the BulkAccountLoader's polling frequency
					if (
						lastBulkAccountLoaderSlot &&
						bulkAccountLoader.mostRecentSlot === lastBulkAccountLoaderSlot
					) {
						res.writeHead(502);
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
						res.writeHead(503);
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

	if (config.global.runOnce) {
		process.exit(0);
	}
};

async function recursiveTryCatch(f: () => void) {
	try {
		f();
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

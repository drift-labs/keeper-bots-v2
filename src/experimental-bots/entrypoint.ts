import { program, Option } from 'commander';
import { logger, setLogLevel } from '../logger';
import {
	Config,
	configHasBot,
	loadConfigFromFile,
	loadConfigFromOpts,
} from '../config';
import { getWallet, sleepMs } from '../utils';
import {
	ConfirmationStrategy,
	DriftClient,
	DriftClientSubscriptionConfig,
	FastSingleTxSender,
	getMarketsAndOraclesForSubscription,
	loadKeypair,
	PublicKey,
	RetryTxSender,
	SlotSubscriber,
	initialize,
	WhileValidTxSender,
} from '@drift-labs/sdk';
import {
	Commitment,
	ConfirmOptions,
	Connection,
	Keypair,
	TransactionVersion,
} from '@solana/web3.js';
import { BundleSender } from '../bundleSender';
import { FillerMultithreaded } from './filler/fillerMultithreaded';
import http from 'http';
import { promiseTimeout } from '@drift-labs/sdk';
import { SpotFillerMultithreaded } from './spotFiller/spotFillerMultithreaded';
import { setGlobalDispatcher, Agent } from 'undici';
import { PythPriceFeedSubscriber } from '../pythPriceFeedSubscriber';

setGlobalDispatcher(
	new Agent({
		connections: 200,
	})
);

require('dotenv').config();

const preflightCommitment: Commitment = 'processed';
const stateCommitment: Commitment = 'confirmed';
const healthCheckPort = process.env.HEALTH_CHECK_PORT || 8888;

program
	.option('-d, --dry-run', 'Dry run, do not send transactions on chain')
	.option('--metrics <number>', 'Enable Prometheus metric scraper (deprecated)')
	.addOption(
		new Option(
			'-p, --private-key <string>',
			'private key, supports path to id.json, or list of comma separate numbers'
		).env('KEEPER_PRIVATE_KEY')
	)
	.option('--debug', 'Enable debug logging')
	.option(
		'--additional-send-tx-endpoints <string>',
		'Additional RPC endpoints to send transactions to (comma delimited)'
	)
	.option(
		'--perp-market-indicies <string>',
		'comma delimited list of perp market index(s) for applicable bots, omit for all',
		''
	)
	.option(
		'--spot-markets-indicies <string>',
		'comma delimited list of spot market index(s) for applicable bots, omit for all',
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
	.option(
		'--priority-fee-multiplier <number>',
		'Multiplier for the priority fee',
		'1.0'
	)
	.option('--metrics-port <number>', 'Port for the Prometheus exporter', '9464')
	.option('--disable-metrics', 'Set to disable Prometheus metrics')
	.parse();

const opts = program.opts();

let config: Config;
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

// @ts-ignore
const sdkConfig = initialize({ env: config.global.driftEnv });
setLogLevel(config.global.debug ? 'debug' : 'info');

const endpoint = config.global.endpoint;
const wsEndpoint = config.global.wsEndpoint;
const heliusEndpoint = config.global.heliusEndpoint;
logger.info(`RPC endpoint: ${endpoint}`);
logger.info(`WS endpoint:  ${wsEndpoint}`);
logger.info(`Helius endpoint:  ${heliusEndpoint}`);
logger.info(`DriftEnv:     ${config.global.driftEnv}`);
if (!endpoint) {
	throw new Error('Must set environment variable ENDPOINT');
}

const bots: any = [];
const runBot = async () => {
	logger.info(`Loading wallet keypair`);
	const privateKeyOrFilepath = config.global.keeperPrivateKey;
	if (!privateKeyOrFilepath) {
		throw new Error(
			'Must set environment variable KEEPER_PRIVATE_KEY with the path to a id.json or a list of commma separated numbers'
		);
	}
	const [keypair, wallet] = getWallet(privateKeyOrFilepath);
	const driftPublicKey = new PublicKey(sdkConfig.DRIFT_PROGRAM_ID);
	const connection = new Connection(endpoint, {
		wsEndpoint: wsEndpoint,
		commitment: stateCommitment,
	});

	const opts: ConfirmOptions = {
		commitment: stateCommitment,
		skipPreflight: config.global.txSkipPreflight,
		preflightCommitment,
		maxRetries: config.global.txMaxRetries,
	};
	const sendTxConnection = new Connection(endpoint, {
		wsEndpoint: wsEndpoint,
		commitment: stateCommitment,
		disableRetryOnRateLimit: true,
	});
	const txSenderType = config.global.txSenderType || 'retry';
	let txSender;
	let additionalConnections: Connection[] = [];
	const confirmationStrategy: ConfirmationStrategy =
		config.global.txSenderConfirmationStrategy;
	if (
		config.global.additionalSendTxEndpoints &&
		config.global.additionalSendTxEndpoints.length > 0
	) {
		additionalConnections = config.global.additionalSendTxEndpoints.map(
			(endpoint: string) => new Connection(endpoint)
		);
	}
	if (txSenderType === 'retry') {
		txSender = new RetryTxSender({
			connection: sendTxConnection,
			wallet,
			opts,
			retrySleep: 8000,
			timeout: config.global.txRetryTimeoutMs,
			confirmationStrategy: ConfirmationStrategy.Polling,
			additionalConnections,
			trackTxLandRate: config.global.trackTxLandRate,
		});
	} else if (txSenderType === 'while-valid') {
		txSender = new WhileValidTxSender({
			connection: sendTxConnection,
			wallet,
			opts,
			retrySleep: 2000,
			additionalConnections,
			trackTxLandRate: config.global.trackTxLandRate,
			confirmationStrategy,
		});
	} else {
		const skipConfirmation =
			configHasBot(config, 'fillerMultithreaded') ||
			configHasBot(config, 'spotFillerMultithreaded');
		txSender = new FastSingleTxSender({
			connection: sendTxConnection,
			blockhashRefreshInterval: 500,
			wallet,
			opts,
			skipConfirmation,
			additionalConnections,
			trackTxLandRate: config.global.trackTxLandRate,
			confirmationStrategy,
		});
	}

	const accountSubscription: DriftClientSubscriptionConfig = {
		type: 'websocket',
		resubTimeoutMs: config.global.resubTimeoutMs,
	};

	// Send unsubscribed subscription to the bot
	let pythPriceSubscriber: PythPriceFeedSubscriber | undefined;
	if (config.global.hermesEndpoint) {
		pythPriceSubscriber = new PythPriceFeedSubscriber(
			config.global.hermesEndpoint,
			{
				priceFeedRequestConfig: {
					binary: true,
				},
			}
		);
	}

	const { perpMarketIndexes, spotMarketIndexes, oracleInfos } =
		getMarketsAndOraclesForSubscription(
			config.global.driftEnv || 'mainnet-beta'
		);
	const marketLookupTable = config.global?.lutPubkey
		? new PublicKey(config.global.lutPubkey)
		: undefined;
	const driftClientConfig = {
		connection,
		wallet,
		programID: driftPublicKey,
		opts,
		accountSubscription,
		env: config.global.driftEnv,
		perpMarketIndexes,
		spotMarketIndexes,
		oracleInfos,
		txVersion: 0 as TransactionVersion,
		txSender,
		marketLookupTable,
	};
	const driftClient = new DriftClient(driftClientConfig);
	await driftClient.subscribe();

	const slotSubscriber = new SlotSubscriber(connection, {
		resubTimeoutMs: 10_000,
	});
	await slotSubscriber.subscribe();

	const lamportsBalance = await connection.getBalance(wallet.publicKey);
	logger.info(
		`DriftClient ProgramId: ${driftClient.program.programId.toBase58()}`
	);
	logger.info(`Wallet pubkey: ${wallet.publicKey.toBase58()}`);
	logger.info(` . SOL balance: ${lamportsBalance / 10 ** 9}`);
	let bundleSender: BundleSender | undefined;
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
		logger.info(`Loading jito keypair`);
		jitoAuthKeypair = loadKeypair(privateKey);

		bundleSender = new BundleSender(
			connection,
			jitoBlockEngineUrl,
			jitoAuthKeypair,
			keypair,
			slotSubscriber,
			config.global.jitoStrategy,
			config.global.jitoMinBundleTip,
			config.global.jitoMaxBundleTip,
			config.global.jitoMaxBundleFailCount,
			config.global.jitoTipMultiplier
		);
		await bundleSender.subscribe();
	}

	if (configHasBot(config, 'fillerMultithreaded')) {
		if (!config.botConfigs?.fillerMultithreaded) {
			throw new Error('fillerMultithreaded bot config not found');
		}

		// Ensure that there are no duplicate market indexes in the Array<number[]> marketIndexes config
		const marketIndexes = new Set<number>();
		for (const marketIndexList of config.botConfigs.fillerMultithreaded
			.marketIndexes) {
			for (const marketIndex of marketIndexList) {
				if (marketIndexes.has(marketIndex)) {
					throw new Error(
						`Market index ${marketIndex} is duplicated in the config`
					);
				}
				marketIndexes.add(marketIndex);
			}
		}

		const fillerMultithreaded = new FillerMultithreaded(
			config.global,
			config.botConfigs?.fillerMultithreaded,
			driftClient,
			slotSubscriber,
			{
				rpcEndpoint: endpoint,
				commit: '',
				driftEnv: config.global.driftEnv!,
				driftPid: driftPublicKey.toBase58(),
				walletAuthority: wallet.publicKey.toBase58(),
			},
			bundleSender,
			pythPriceSubscriber,
			[]
		);
		bots.push(fillerMultithreaded);
	}

	if (configHasBot(config, 'spotFillerMultithreaded')) {
		if (!config.botConfigs?.spotFillerMultithreaded) {
			throw new Error('spotFillerMultithreaded bot config not found');
		}

		// Ensure that there are no duplicate market indexes in the Array<number[]> marketIndexes config
		const marketIndexes = new Set<number>();
		for (const marketIndexList of config.botConfigs.spotFillerMultithreaded
			.marketIndexes) {
			for (const marketIndex of marketIndexList) {
				if (marketIndexes.has(marketIndex)) {
					throw new Error(
						`Market index ${marketIndex} is duplicated in the config`
					);
				}
				marketIndexes.add(marketIndex);
			}
		}

		const spotFillerMultithreaded = new SpotFillerMultithreaded(
			driftClient,
			slotSubscriber,
			{
				rpcEndpoint: endpoint,
				commit: '',
				driftEnv: config.global.driftEnv!,
				driftPid: driftPublicKey.toBase58(),
				walletAuthority: wallet.publicKey.toBase58(),
			},
			config.global,
			config.botConfigs?.spotFillerMultithreaded,
			bundleSender,
			pythPriceSubscriber,
			[]
		);
		bots.push(spotFillerMultithreaded);
	}
	// Initialize bots
	logger.info(`initializing bots`);
	await Promise.all(bots.map((bot: any) => bot.init()));

	logger.info(`starting bots`);

	// start http server listening to /health endpoint using http package
	const startupTime = Date.now();
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

				/* @ts-ignore */
				if (!driftClient.connection._rpcWebSocketConnected) {
					logger.error(`Connection rpc websocket disconnected`);
					res.writeHead(500);
					res.end(`Connection rpc websocket disconnected`);
					return;
				}

				// check all bots if they're live
				for (const bot of bots) {
					const healthCheck = await promiseTimeout(bot.healthCheck(), 1000);
					if (!healthCheck) {
						logger.error(`Health check failed for bot`);
						res.writeHead(503);
						res.end(`Bot is not healthy`);
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

recursiveTryCatch(() => runBot());

async function recursiveTryCatch(f: () => void) {
	try {
		f();
	} catch (e) {
		console.error(e);
		await sleepMs(15000);
		await recursiveTryCatch(f);
	}
}

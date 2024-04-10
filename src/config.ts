import * as fs from 'fs';
import YAML from 'yaml';
import {
	loadCommaDelimitToArray,
	loadCommaDelimitToStringArray,
} from './utils';
import { OrderExecutionAlgoType } from './types';
import { DriftEnv } from '@drift-labs/sdk';

export type BaseBotConfig = {
	botId: string;
	dryRun: boolean;
	/// will override {@link GlobalConfig.metricsPort}
	metricsPort?: number;
	runOnce?: boolean;
};

export type JitMakerConfig = BaseBotConfig & {
	subaccounts?: Array<number>;
	marketType: string;
	/// @deprecated, use {@link JitMakerConfig.marketIndexes} and {@link JitMakerConfig.marketType}
	perpMarketIndicies?: Array<number>;
	marketIndexes?: Array<number>;
	targetLeverage?: number;
	aggressivenessBps?: number;
	jitCULimit?: number;
};

export type MarkTwapCrankConfig = BaseBotConfig & {
	crankIntervalToMarketIndicies?: { [key: number]: number[] };
};

export type FillerMultiThreadedConfig = BaseBotConfig & {
	marketType: string;
	marketIndex: number;
	simulateTxForCUEstimate?: boolean;
};

export type FillerConfig = BaseBotConfig & {
	fillerPollingInterval?: number;
	revertOnFailure?: boolean;
	simulateTxForCUEstimate?: boolean;
};

export type SubaccountConfig = {
	[key: number]: Array<number>;
};

export type LiquidatorConfig = BaseBotConfig & {
	disableAutoDerisking: boolean;
	useJupiter: boolean;
	perpMarketIndicies?: Array<number>;
	spotMarketIndicies?: Array<number>;
	perpSubAccountConfig?: SubaccountConfig;
	spotSubAccountConfig?: SubaccountConfig;

	// deprecated: use {@link LiquidatorConfig.maxSlippageBps} (misnamed)
	maxSlippagePct?: number;
	maxSlippageBps?: number;

	deriskAuctionDurationSlots?: number;
	deriskAlgo?: OrderExecutionAlgoType;
	deriskAlgoSpot?: OrderExecutionAlgoType;
	deriskAlgoPerp?: OrderExecutionAlgoType;
	twapDurationSec?: number;
	minDepositToLiq?: Map<number, number>;
	excludedAccounts?: Set<string>;
	maxPositionTakeoverPctOfCollateral?: number;
	notifyOnLiquidation?: boolean;
};

export type BotConfigMap = {
	fillerMultithreaded?: FillerMultiThreadedConfig;
	filler?: FillerConfig;
	fillerLite?: FillerConfig;
	fillerBulk?: FillerConfig;
	spotFiller?: FillerConfig;
	trigger?: BaseBotConfig;
	liquidator?: LiquidatorConfig;
	floatingMaker?: BaseBotConfig;
	jitMaker?: JitMakerConfig;
	ifRevenueSettler?: BaseBotConfig;
	fundingRateUpdater?: BaseBotConfig;
	userPnlSettler?: BaseBotConfig;
	userLpSettler?: BaseBotConfig;
	userIdleFlipper?: BaseBotConfig;
	markTwapCrank?: MarkTwapCrankConfig;
	uncrossArb?: BaseBotConfig;
};

export interface GlobalConfig {
	driftEnv?: DriftEnv;
	endpoint?: string;
	wsEndpoint?: string;
	/// helius endpoint to use helius priority fee strategy
	heliusEndpoint?: string;
	/// additional rpc endpoints to send transactions to
	additionalSendTxEndpoints?: string[];
	/// endpoint to confirm txs on
	txConfirmationEndpoint?: string;
	/// default metrics port to use, will be overridden by {@link BaseBotConfig.metricsPort} if provided
	metricsPort?: number;
	/// disable all metrics
	disableMetrics?: boolean;

	priorityFeeMethod?: string;
	maxPriorityFeeMicroLamports?: number;
	resubTimeoutMs?: number;
	priorityFeeMultiplier?: number;
	keeperPrivateKey?: string;
	initUser?: boolean;
	testLiveness?: boolean;
	cancelOpenOrders?: boolean;
	closeOpenPositions?: boolean;
	forceDeposit?: number | null;
	websocket?: boolean;
	eventSubscriber?: false;
	runOnce?: boolean;
	debug?: boolean;
	subaccounts?: Array<number>;

	eventSubscriberPollingInterval: number;
	bulkAccountLoaderPollingInterval: number;

	useJito?: boolean;
	jitoStrategy?: 'jito-only' | 'non-jito-only' | 'hybrid';
	jitoBlockEngineUrl?: string;
	jitoAuthPrivateKey?: string;
	jitoMinBundleTip?: number;
	jitoMaxBundleTip?: number;
	jitoMaxBundleFailCount?: number;
	jitoTipMultiplier?: number;

	txRetryTimeoutMs?: number;
	txSenderType?: 'fast' | 'retry' | 'while-valid';
	txSkipPreflight?: boolean;
	txMaxRetries?: number;
}

export interface Config {
	global: GlobalConfig;
	enabledBots: Array<keyof BotConfigMap>;
	botConfigs?: BotConfigMap;
}

const defaultConfig: Partial<Config> = {
	global: {
		driftEnv: (process.env.ENV ?? 'devnet') as DriftEnv,
		initUser: false,
		testLiveness: false,
		cancelOpenOrders: false,
		closeOpenPositions: false,
		forceDeposit: null,
		websocket: false,
		eventSubscriber: false,
		runOnce: false,
		debug: false,
		subaccounts: [0],

		eventSubscriberPollingInterval: 5000,
		bulkAccountLoaderPollingInterval: 5000,

		endpoint: process.env.ENDPOINT,
		wsEndpoint: process.env.WS_ENDPOINT,
		heliusEndpoint: process.env.HELIUS_ENDPOINT,
		additionalSendTxEndpoints: [],
		txConfirmationEndpoint: process.env.TX_CONFIRMATION_ENDPOINT,
		priorityFeeMethod: process.env.PRIORITY_FEE_METHOD ?? 'solana',
		maxPriorityFeeMicroLamports: parseInt(
			process.env.MAX_PRIORITY_FEE_MICRO_LAMPORTS ?? '10000'
		),
		priorityFeeMultiplier: 1.0,
		keeperPrivateKey: process.env.KEEPER_PRIVATE_KEY,

		useJito: false,
		jitoStrategy: 'jito-only',
		jitoMinBundleTip: 10_000,
		jitoMaxBundleTip: 100_000,
		jitoMaxBundleFailCount: 200,
		jitoTipMultiplier: 3,
		jitoBlockEngineUrl: process.env.JITO_BLOCK_ENGINE_URL,
		jitoAuthPrivateKey: process.env.JITO_AUTH_PRIVATE_KEY,
		txRetryTimeoutMs: parseInt(process.env.TX_RETRY_TIMEOUT_MS ?? '30000'),
		txSkipPreflight: false,
		txMaxRetries: 0,

		metricsPort: 9464,
		disableMetrics: false,
	},
	enabledBots: [],
	botConfigs: {
		jitMaker: {
			botId: 'jit-maker',
			dryRun: false,
			marketType: 'perp',
			targetLeverage: 1,
			aggressivenessBps: 0,
			jitCULimit: 800000,
		},
	},
};

function mergeDefaults<T>(defaults: T, data: Partial<T>): T {
	const result: T = { ...defaults } as T;

	for (const key in data) {
		const value = data[key];

		if (value === undefined || value === null) {
			continue;
		}
		if (typeof value === 'object' && !Array.isArray(value)) {
			result[key] = mergeDefaults(
				result[key],
				value as Partial<T[Extract<keyof T, string>]>
			);
		} else if (Array.isArray(value)) {
			if (!Array.isArray(result[key])) {
				result[key] = [] as any;
			}

			for (let i = 0; i < value.length; i++) {
				if (typeof value[i] === 'object' && !Array.isArray(value[i])) {
					const existingObj = (result[key] as unknown as any[])[i] || {};
					(result[key] as unknown as any[])[i] = mergeDefaults(
						existingObj,
						value[i]
					);
				} else {
					(result[key] as unknown as any[])[i] = value[i];
				}
			}
		} else {
			result[key] = value as T[Extract<keyof T, string>];
		}
	}

	return result;
}

export function loadConfigFromFile(path: string): Config {
	if (!path.endsWith('.yaml') && !path.endsWith('.yml')) {
		throw new Error('Config file must be a yaml file');
	}

	const configFile = fs.readFileSync(path, 'utf8');
	const config = YAML.parse(configFile) as Partial<Config>;

	return mergeDefaults(defaultConfig, config) as Config;
}

/**
 * For backwards compatibility, we allow the user to specify the config via command line arguments.
 * @param opts from program.opts()
 * @returns
 */
export function loadConfigFromOpts(opts: any): Config {
	const config: Config = {
		global: {
			driftEnv: (process.env.ENV ?? 'devnet') as DriftEnv,
			endpoint: opts.endpoint ?? process.env.ENDPOINT,
			wsEndpoint: opts.wsEndpoint ?? process.env.WS_ENDPOINT,
			heliusEndpoint: opts.heliusEndpoint ?? process.env.HELIUS_ENDPOINT,
			additionalSendTxEndpoints: loadCommaDelimitToStringArray(
				opts.additionalSendTxEndpoints
			),
			txConfirmationEndpoint:
				opts.txConfirmationEndpoint ?? process.env.TX_CONFIRMATION_ENDPOINT,
			priorityFeeMethod:
				opts.priorityFeeMethod ?? process.env.PRIORITY_FEE_METHOD,
			maxPriorityFeeMicroLamports: parseInt(
				opts.maxPriorityFeeMicroLamports ??
					process.env.MAX_PRIORITY_FEE_MICRO_LAMPORTS ??
					'10000'
			),
			priorityFeeMultiplier: parseFloat(opts.priorityFeeMultiplier ?? '1.0'),
			keeperPrivateKey: opts.privateKey ?? process.env.KEEPER_PRIVATE_KEY,
			eventSubscriberPollingInterval: parseInt(
				process.env.BULK_ACCOUNT_LOADER_POLLING_INTERVAL ?? '5000'
			),
			bulkAccountLoaderPollingInterval: parseInt(
				process.env.EVENT_SUBSCRIBER_POLLING_INTERVAL ?? '5000'
			),
			initUser: opts.initUser ?? false,
			testLiveness: opts.testLiveness ?? false,
			cancelOpenOrders: opts.cancelOpenOrders ?? false,
			closeOpenPositions: opts.closeOpenPositions ?? false,
			forceDeposit: opts.forceDeposit ?? null,
			websocket: opts.websocket ?? false,
			eventSubscriber: opts.eventSubscriber ?? false,
			runOnce: opts.runOnce ?? false,
			debug: opts.debug ?? false,
			subaccounts: loadCommaDelimitToArray(opts.subaccount),
			useJito: opts.useJito ?? false,
			jitoStrategy: opts.jitoStrategy ?? 'exclusive',
			jitoMinBundleTip: opts.jitoMinBundleTip ?? 10_000,
			jitoMaxBundleTip: opts.jitoMaxBundleTip ?? 100_000,
			jitoMaxBundleFailCount: opts.jitoMaxBundleFailCount ?? 200,
			jitoTipMultiplier: opts.jitoTipMultiplier ?? 3,
			txRetryTimeoutMs: parseInt(opts.txRetryTimeoutMs ?? '30000'),
			txSenderType: opts.txSenderType ?? 'fast',
			txSkipPreflight: opts.txSkipPreflight
				? opts.txSkipPreflight.toLowerCase() === 'true'
				: false,
			txMaxRetries: parseInt(opts.txMaxRetries ?? '0'),

			metricsPort: opts.metricsPort ?? 9464,
			disableMetrics: opts.disableMetrics ?? false,
		},
		enabledBots: [],
		botConfigs: {},
	};

	if (opts.filler) {
		config.enabledBots.push('filler');
		config.botConfigs!.filler = {
			dryRun: opts.dryRun ?? false,
			botId: process.env.BOT_ID ?? 'filler',
			fillerPollingInterval: 5000,
			metricsPort: 9464,
			runOnce: opts.runOnce ?? false,
			simulateTxForCUEstimate: opts.simulateTxForCUEstimate ?? true,
		};
	}
	if (opts.fillerLite) {
		config.enabledBots.push('fillerLite');
		config.botConfigs!.fillerLite = {
			dryRun: opts.dryRun ?? false,
			botId: process.env.BOT_ID ?? 'fillerLite',
			fillerPollingInterval: 5000,
			metricsPort: 9464,
			runOnce: opts.runOnce ?? false,
			simulateTxForCUEstimate: opts.simulateTxForCUEstimate ?? true,
		};
	}
	if (opts.fillerBulk) {
		config.enabledBots.push('fillerBulk');
		config.botConfigs!.fillerBulk = {
			dryRun: opts.dryRun ?? false,
			botId: process.env.BOT_ID ?? 'fillerBulk',
			fillerPollingInterval: 5000,
			metricsPort: 9464,
			runOnce: opts.runOnce ?? false,
			simulateTxForCUEstimate: opts.simulateTxForCUEstimate ?? true,
		};
	}
	if (opts.spotFiller) {
		config.enabledBots.push('spotFiller');
		config.botConfigs!.spotFiller = {
			dryRun: opts.dryRun ?? false,
			botId: process.env.BOT_ID ?? 'filler',
			fillerPollingInterval: 5000,
			metricsPort: 9464,
			runOnce: opts.runOnce ?? false,
			simulateTxForCUEstimate: opts.simulateTxForCUEstimate ?? true,
		};
	}
	if (opts.liquidator) {
		config.enabledBots.push('liquidator');
		config.botConfigs!.liquidator = {
			dryRun: opts.dryRun ?? false,
			botId: process.env.BOT_ID ?? 'liquidator',
			metricsPort: 9464,

			disableAutoDerisking: opts.disableAutoDerisking ?? false,
			useJupiter: opts.useJupiter ?? true,
			perpMarketIndicies: loadCommaDelimitToArray(opts.perpMarketIndicies),
			spotMarketIndicies: loadCommaDelimitToArray(opts.spotMarketIndicies),
			runOnce: opts.runOnce ?? false,
			// deprecated: use {@link LiquidatorConfig.maxSlippageBps}
			maxSlippagePct: opts.maxSlippagePct ?? 50,
			maxSlippageBps: opts.maxSlippageBps ?? 50,
			deriskAuctionDurationSlots: opts.deriskAuctionDurationSlots ?? 100,
			deriskAlgo: opts.deriskAlgo ?? OrderExecutionAlgoType.Market,
			twapDurationSec: parseInt(opts.twapDurationSec ?? '300'),
			notifyOnLiquidation: opts.notifyOnLiquidation ?? false,
		};
	}
	if (opts.trigger) {
		config.enabledBots.push('trigger');
		config.botConfigs!.trigger = {
			dryRun: opts.dryRun ?? false,
			botId: process.env.BOT_ID ?? 'trigger',
			metricsPort: 9464,
			runOnce: opts.runOnce ?? false,
		};
	}
	if (opts.jitMaker) {
		config.enabledBots.push('jitMaker');
		config.botConfigs!.jitMaker = {
			dryRun: opts.dryRun ?? false,
			botId: process.env.BOT_ID ?? 'jitMaker',
			metricsPort: 9464,
			runOnce: opts.runOnce ?? false,
			perpMarketIndicies: loadCommaDelimitToArray(opts.perpMarketIndicies) ?? [
				0,
			],
			subaccounts: loadCommaDelimitToArray(opts.subaccounts) ?? [0],
			marketType: opts.marketType ?? 'perp',
			targetLeverage: 1,
		};
	}
	if (opts.ifRevenueSettler) {
		config.enabledBots.push('ifRevenueSettler');
		config.botConfigs!.ifRevenueSettler = {
			dryRun: opts.dryRun ?? false,
			botId: process.env.BOT_ID ?? 'ifRevenueSettler',
			metricsPort: 9464,
			runOnce: opts.runOnce ?? false,
		};
	}
	if (opts.userPnlSettler) {
		config.enabledBots.push('userPnlSettler');
		config.botConfigs!.userPnlSettler = {
			dryRun: opts.dryRun ?? false,
			botId: process.env.BOT_ID ?? 'userPnlSettler',
			metricsPort: 9464,
			runOnce: opts.runOnce ?? false,
		};
	}
	if (opts.userLpSettler) {
		config.enabledBots.push('userLpSettler');
		config.botConfigs!.userLpSettler = {
			dryRun: opts.dryRun ?? false,
			botId: process.env.BOT_ID ?? 'userLpSettler',
			metricsPort: 9464,
			runOnce: opts.runOnce ?? false,
		};
	}
	if (opts.userIdleFlipper) {
		config.enabledBots.push('userIdleFlipper');
		config.botConfigs!.userIdleFlipper = {
			dryRun: opts.dryRun ?? false,
			botId: process.env.BOT_ID ?? 'userIdleFlipper',
			metricsPort: 9464,
			runOnce: opts.runOnce ?? false,
		};
	}
	if (opts.fundingRateUpdater) {
		config.enabledBots.push('fundingRateUpdater');
		config.botConfigs!.fundingRateUpdater = {
			dryRun: opts.dryRun ?? false,
			botId: process.env.BOT_ID ?? 'fundingRateUpdater',
			metricsPort: 9464,
			runOnce: opts.runOnce ?? false,
		};
	}

	if (opts.markTwapCrank) {
		config.enabledBots.push('markTwapCrank');
		config.botConfigs!.markTwapCrank = {
			dryRun: opts.dryRun ?? false,
			botId: process.env.BOT_ID ?? 'crank',
			metricsPort: 9464,
			runOnce: opts.runOnce ?? false,
		};
	}

	if (opts.uncrossArb) {
		config.enabledBots.push('uncrossArb');
		config.botConfigs!.uncrossArb = {
			dryRun: opts.dryRun ?? false,
			botId: process.env.BOT_ID ?? 'uncrossArb',
			metricsPort: 9464,
			runOnce: opts.runOnce ?? false,
		};
	}

	return mergeDefaults(defaultConfig, config) as Config;
}

export function configHasBot(
	config: Config,
	botName: keyof BotConfigMap
): boolean {
	const botEnabled = config.enabledBots.includes(botName) ?? false;
	const botConfigExists = config.botConfigs![botName] !== undefined;
	if (botEnabled && !botConfigExists) {
		throw new Error(
			`Bot ${botName} is enabled but no config was found for it.`
		);
	}
	return botEnabled && botConfigExists;
}

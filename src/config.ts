import * as fs from 'fs';
import YAML from 'yaml';
import { loadCommaDelimitToArray } from './utils';
import { DriftEnv } from '@drift-labs/sdk';

export type BaseBotConfig = {
	botId: string;
	dryRun: boolean;
	metricsPort?: number;
	runOnce?: boolean;
};

export type FillerConfig = BaseBotConfig & {
	fillerPollingInterval?: number;
	transactionVersion: number | null;
	revertOnFailure?: boolean;
};

export type SubaccountConfig = {
	[key: number]: Array<number>;
};

export type LiquidatorConfig = BaseBotConfig & {
	disableAutoDerisking: boolean;
	perpMarketIndicies?: Array<number>;
	spotMarketIndicies?: Array<number>;
	perpSubAccountConfig?: SubaccountConfig;
	spotSubAccountConfig?: SubaccountConfig;
};

export type BotConfigMap = {
	filler?: FillerConfig;
	spotFiller?: FillerConfig;
	trigger?: BaseBotConfig;
	liquidator?: LiquidatorConfig;
	floatingMaker?: BaseBotConfig;
	jitMaker?: BaseBotConfig;
	ifRevenueSettler?: BaseBotConfig;
	userPnlSettler?: BaseBotConfig;
};

export interface GlobalConfig {
	driftEnv?: DriftEnv;
	endpoint?: string;
	wsEndpoint?: string;
	keeperPrivateKey?: string;
	initUser?: boolean;
	testLiveness?: boolean;
	cancelOpenOrders?: boolean;
	closeOpenPositions?: boolean;
	forceDeposit?: number | null;
	websocket?: boolean;
	runOnce?: boolean;
	debug?: boolean;
	subaccounts?: Array<number>;

	eventSubscriberPollingInterval: number;
	bulkAccountLoaderPollingInterval: number;

	useJito?: boolean;
	jitoBlockEngineUrl?: string;
	jitoAuthPrivateKey?: string;
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
		runOnce: false,
		debug: false,
		subaccounts: [0],

		eventSubscriberPollingInterval: 5000,
		bulkAccountLoaderPollingInterval: 5000,

		endpoint: process.env.ENDPOINT,
		wsEndpoint: process.env.WS_ENDPOINT,
		keeperPrivateKey: process.env.KEEPER_PRIVATE_KEY,

		useJito: false,
		jitoBlockEngineUrl: process.env.JITO_BLOCK_ENGINE_URL,
		jitoAuthPrivateKey: process.env.JITO_AUTH_PRIVATE_KEY,
	},
	enabledBots: [],
	botConfigs: {},
};

function mergeDefaults<T>(defaults: T, data: Partial<T>): T {
	const result: T = { ...defaults } as T;

	for (const key in data) {
		const value = data[key];

		if (value !== undefined && value !== null) {
			if (typeof value === 'object' && !Array.isArray(value)) {
				result[key] = mergeDefaults(result[key], value);
			} else if (Array.isArray(value)) {
				if (!Array.isArray(result[key])) {
					result[key] = [] as any;
				}

				for (let i = 0; i < value.length; i++) {
					if (typeof value[i] === 'object' && !Array.isArray(value[i])) {
						const existingObj = result[key][i] || {};
						result[key][i] = mergeDefaults(existingObj, value[i]);
					} else {
						result[key][i] = value[i];
					}
				}
			} else {
				result[key] = value;
			}
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
			keeperPrivateKey: opts.privateKey ?? process.env.KEEPER_PRIVATE_KEY,
			eventSubscriberPollingInterval:
				parseInt(process.env.BULK_ACCOUNT_LOADER_POLLING_INTERVAL) ?? 5000,
			bulkAccountLoaderPollingInterval:
				parseInt(process.env.EVENT_SUBSCRIBER_POLLING_INTERVAL) ?? 5000,
			initUser: opts.initUser ?? false,
			testLiveness: opts.testLiveness ?? false,
			cancelOpenOrders: opts.cancelOpenOrders ?? false,
			closeOpenPositions: opts.closeOpenPositions ?? false,
			forceDeposit: opts.forceDeposit ?? null,
			websocket: opts.websocket ?? false,
			runOnce: opts.runOnce ?? false,
			debug: opts.debug ?? false,
			subaccounts: loadCommaDelimitToArray(opts.subaccount),
			useJito: opts.useJito ?? false,
		},
		enabledBots: [],
		botConfigs: {},
	};

	if (opts.filler) {
		config.enabledBots.push('filler');
		config.botConfigs.filler = {
			dryRun: opts.dryRun ?? false,
			botId: process.env.BOT_ID ?? 'filler',
			fillerPollingInterval: 5000,
			metricsPort: 9464,
			transactionVersion: 0,
			runOnce: opts.runOnce ?? false,
		};
	}
	if (opts.spotFiller) {
		config.enabledBots.push('spotFiller');
		config.botConfigs.spotFiller = {
			dryRun: opts.dryRun ?? false,
			botId: process.env.BOT_ID ?? 'filler',
			fillerPollingInterval: 5000,
			metricsPort: 9464,
			transactionVersion: 0,
			runOnce: opts.runOnce ?? false,
		};
	}
	if (opts.liquidator) {
		config.enabledBots.push('liquidator');
		config.botConfigs.liquidator = {
			dryRun: opts.dryRun ?? false,
			botId: process.env.BOT_ID ?? 'liquidator',
			metricsPort: 9464,
			disableAutoDerisking: opts.disableAutoDerisking ?? false,
			perpMarketIndicies: loadCommaDelimitToArray(opts.perpMarketIndicies),
			spotMarketIndicies: loadCommaDelimitToArray(opts.spotMarketIndicies),
			runOnce: opts.runOnce ?? false,
		};
	}
	if (opts.trigger) {
		config.enabledBots.push('trigger');
		config.botConfigs.trigger = {
			dryRun: opts.dryRun ?? false,
			botId: process.env.BOT_ID ?? 'trigger',
			metricsPort: 9464,
			runOnce: opts.runOnce ?? false,
		};
	}
	if (opts.jitMaker) {
		config.enabledBots.push('jitMaker');
		config.botConfigs.jitMaker = {
			dryRun: opts.dryRun ?? false,
			botId: process.env.BOT_ID ?? 'jitMaker',
			metricsPort: 9464,
			runOnce: opts.runOnce ?? false,
		};
	}
	if (opts.ifRevenueSettler) {
		config.enabledBots.push('ifRevenueSettler');
		config.botConfigs.ifRevenueSettler = {
			dryRun: opts.dryRun ?? false,
			botId: process.env.BOT_ID ?? 'ifRevenueSettler',
			metricsPort: 9464,
			runOnce: opts.runOnce ?? false,
		};
	}
	if (opts.userPnlSettler) {
		config.enabledBots.push('userPnlSettler');
		config.botConfigs.userPnlSettler = {
			dryRun: opts.dryRun ?? false,
			botId: process.env.BOT_ID ?? 'userPnlSettler',
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
	const botEnabled = config.enabledBots.includes(botName);
	const botConfigExists = config.botConfigs[botName] !== undefined;
	if (botEnabled && !botConfigExists) {
		throw new Error(
			`Bot ${botName} is enabled but no config was found for it.`
		);
	}
	return botEnabled && botConfigExists;
}

import * as fs from 'fs';
import YAML from 'yaml';
import { loadCommaDelimitToArray } from './utils';

export type BaseBotConfig = {
	botId: string;
	dryRun: boolean;
	metricsPort?: number;
};

export type FillerConfig = BaseBotConfig & {
	fillerPollingInterval?: number;
	transactionVersion?: number | null;
};

export type LiquidatorConfig = BaseBotConfig & {
	perpMarketIndicies: Array<number>;
	spotMarketIndicies: Array<number>;
	disableAutoDerisking: boolean;
};

export type BotConfigMap = {
	filler?: FillerConfig;
	spotFiller?: FillerConfig;
	trigger?: BaseBotConfig;
	liquidator?: LiquidatorConfig;
	floatingMaker?: BaseBotConfig;
	jitMaker?: BaseBotConfig;
	ifRevenueSettler?: BaseBotConfig;
	fundingRateUpdater?: BaseBotConfig;
	userPnlSettler?: BaseBotConfig;
};

export interface GlobalConfig {
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
}

export interface Config {
	global: GlobalConfig;
	enabledBots: Array<keyof BotConfigMap>;
	botConfigs?: BotConfigMap;
}

const defaultConfig: Partial<Config> = {
	global: {
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
		};
	}
	if (opts.spotFiller) {
		config.enabledBots.push('spotFiller');
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
		};
	}
	if (opts.trigger) {
		config.enabledBots.push('trigger');
	}
	if (opts.jitMaker) {
		config.enabledBots.push('jitMaker');
	}
	if (opts.ifRevenueSettler) {
		config.enabledBots.push('ifRevenueSettler');
	}
	if (opts.userPnlSettler) {
		config.enabledBots.push('userPnlSettler');
	}

	return mergeDefaults(defaultConfig, config) as Config;
}

export function configHasBot(
	config: Config,
	botName: keyof BotConfigMap
): boolean {
	return (
		config.enabledBots.includes(botName) &&
		config.botConfigs[botName] !== undefined
	);
}

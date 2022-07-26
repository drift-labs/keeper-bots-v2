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
	initialize,
	OrderRecord,
	Wallet,
	DriftEnv,
	UserAccount,
	EventSubscriber,
	SlotSubscriber,
	convertToNumber,
	QUOTE_PRECISION,
} from '@drift-labs/sdk';

import { logger } from './logger';
import { constants } from './types';
import { DLOB } from './dlob/DLOB';
import { UserMap } from './userMap';
import { FillerBot } from './bots/filler';
import { TriggerBot } from './bots/trigger';
import { Bot } from './types';

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
	.addOption(
		new Option(
			'-p, --private-key <string>',
			'private key, supports path to id.json, or list of comma separate numbers'
		).env('FILLER_PRIVATE_KEY')
	)
	.parse();

const opts = program.opts();

logger.info(
	`Dry run: ${!!opts.dry}, FillerBot enabled: ${!!opts.filler}, TriggerBot enabled: ${!!opts.trigger} JITMakerBot enabled: ${!!opts.jitMaker}`
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
	maxTx: 8192,
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

const bots: Bot[] = [];
const runBot = async (wallet: Wallet, clearingHouse: ClearingHouse) => {
	const lamportsBalance = await connection.getBalance(wallet.publicKey);
	logger.info(
		`ClearingHouse ProgramId: ${clearingHouse.program.programId.toBase58()}`
	);
	logger.info(`default pubkey: ${PublicKey.default}`);
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

	const dlob = new DLOB(clearingHouse.getMarketAccounts());
	const programAccounts = await clearingHouse.program.account.user.all();
	for (const programAccount of programAccounts) {
		// @ts-ignore
		const userAccount: UserAccount = programAccount.account;
		const userAccountPublicKey = programAccount.publicKey;

		for (const order of userAccount.orders) {
			dlob.insert(order, userAccountPublicKey);
		}
	}

	const userMap = new UserMap(connection, clearingHouse);
	await userMap.fetchAllUsers();

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
	while (!clearingHouse.isSubscribed || !clearingHouseUser.isSubscribed) {
		logger.info('not subscribed yet');
		await sleep(1000);
		await clearingHouse.subscribe();
		await clearingHouseUser.subscribe();
	}

	logger.info(
		`User unsettled PnL:          ${convertToNumber(
			clearingHouseUser.getUnsettledPNL(),
			QUOTE_PRECISION
		)}`
	);
	logger.info(
		`User unrealized funding PnL: ${convertToNumber(
			clearingHouseUser.getUnrealizedFundingPNL(),
			QUOTE_PRECISION
		)}`
	);
	logger.info(
		`User unrealized PnL:         ${convertToNumber(
			clearingHouseUser.getUnrealizedPNL(),
			QUOTE_PRECISION
		)}`
	);

	const printOrderLists = () => {
		for (const marketAccount of clearingHouse.getMarketAccounts()) {
			dlob.printTopOfOrderLists(
				sdkConfig,
				clearingHouse,
				slotSubscriber,
				marketAccount.marketIndex
			);
		}
	};
	logger.info('Current market snapshot:');
	printOrderLists();
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
				dlob,
				userMap
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
				dlob,
				userMap
			)
		);
	}

	for (const bot of bots) {
		bot.startIntervalLoop(500);
	}

	const handleOrderRecord = async (record: OrderRecord) => {
		logger.info(`Received an order record ${JSON.stringify(record)}`);

		dlob.applyOrderRecord(record);
		await userMap.updateWithOrder(record);

		dlob.printTopOfOrderLists(
			sdkConfig,
			clearingHouse,
			slotSubscriber,
			record.marketIndex
		);

		Promise.all(bots.map((bot) => bot.trigger()));
	};

	eventSubscriber.eventEmitter.on('newEvent', (event) => {
		if (event.eventType === 'OrderRecord') {
			handleOrderRecord(event as OrderRecord);
		} else {
			logger.info(`order record event type ${event.eventType}`);
		}
	});
};

async function recursiveTryCatch(f: () => void) {
	try {
		await f();
	} catch (e) {
		console.error(e);
		for (const bot of bots) {
			bot.reset();
		}
		await sleep(15000);
		await recursiveTryCatch(f);
	}
}

recursiveTryCatch(() => runBot(wallet, clearingHouse));

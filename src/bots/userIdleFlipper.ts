import {
	BN,
	DriftClient,
	UserAccount,
	PublicKey,
	UserMap,
	TxSigAndSlot,
	DriftClientConfig,
	BulkAccountLoader,
} from '@drift-labs/sdk';
import { Mutex } from 'async-mutex';

import { logger } from '../logger';
import { Bot } from '../types';
import { BaseBotConfig } from '../config';
import {
	AddressLookupTableAccount,
	ComputeBudgetProgram,
} from '@solana/web3.js';
import { sleepMs } from '../utils';

const USER_IDLE_CHUNKS = 9;
const SLEEP_MS = 1000;

export class UserIdleFlipperBot implements Bot {
	public readonly name: string;
	public readonly dryRun: boolean;
	public readonly runOnce: boolean;
	public readonly defaultIntervalMs: number = 600000;

	private driftClient: DriftClient;
	private lookupTableAccount?: AddressLookupTableAccount;
	private intervalIds: Array<NodeJS.Timer> = [];
	private userMap: UserMap;

	private watchdogTimerMutex = new Mutex();
	private watchdogTimerLastPatTime = Date.now();

	constructor(driftClientConfigs: DriftClientConfig, config: BaseBotConfig) {
		this.name = config.botId;
		this.dryRun = config.dryRun;
		this.runOnce = config.runOnce || false;
		const bulkAccountLoader = new BulkAccountLoader(
			driftClientConfigs.connection,
			driftClientConfigs.connection.commitment || 'processed',
			0
		);
		this.driftClient = new DriftClient(
			Object.assign({}, driftClientConfigs, {
				accountSubscription: {
					type: 'polling',
					accountLoader: bulkAccountLoader,
				},
				txSender: null, // force to default to retry tx send
			})
		);
		this.userMap = new UserMap({
			driftClient: this.driftClient,
			subscriptionConfig: {
				type: 'polling',
				frequency: 60_000,
				commitment: this.driftClient.opts?.commitment,
			},
			skipInitialLoad: false,
			includeIdle: false,
		});
	}

	public async init() {
		logger.info(`${this.name} initing`);

		await this.driftClient.subscribe();
		if (!(await this.driftClient.getUser().exists())) {
			throw new Error(
				`User for ${this.driftClient.wallet.publicKey.toString()} does not exist`
			);
		}
		await this.userMap.subscribe();
		this.lookupTableAccount =
			await this.driftClient.fetchMarketLookupTableAccount();
	}

	public async reset() {
		for (const intervalId of this.intervalIds) {
			clearInterval(intervalId as NodeJS.Timeout);
		}
		this.intervalIds = [];

		await this.userMap?.unsubscribe();
	}

	public async startIntervalLoop(intervalMs?: number): Promise<void> {
		logger.info(`${this.name} Bot started!`);
		if (this.runOnce) {
			await this.tryIdleUsers();
		} else {
			const intervalId = setInterval(this.tryIdleUsers.bind(this), intervalMs);
			this.intervalIds.push(intervalId);
		}
	}

	public async healthCheck(): Promise<boolean> {
		let healthy = false;
		await this.watchdogTimerMutex.runExclusive(async () => {
			healthy =
				this.watchdogTimerLastPatTime > Date.now() - 2 * this.defaultIntervalMs;
		});
		return healthy;
	}

	private async tryIdleUsers() {
		try {
			console.log('tryIdleUsers');
			const currentSlot = await this.driftClient.connection.getSlot();
			const usersToIdle: Array<[PublicKey, UserAccount]> = [];
			for (const user of this.userMap.values()) {
				if (user.canMakeIdle(new BN(currentSlot))) {
					usersToIdle.push([
						user.getUserAccountPublicKey(),
						user.getUserAccount(),
					]);
					logger.info(
						`Can idle user ${user.getUserAccount().authority.toBase58()}`
					);
				}
			}
			logger.info(`Found ${usersToIdle.length} users to idle`);

			for (let i = 0; i < usersToIdle.length; i += USER_IDLE_CHUNKS) {
				const usersChunk = usersToIdle.slice(i, i + USER_IDLE_CHUNKS);
				await this.trySendTxforChunk(usersChunk);
			}
		} catch (err) {
			console.error(err);
			if (!(err instanceof Error)) {
				return;
			}
		} finally {
			logger.info('UserIdleSettler finished');
			await this.watchdogTimerMutex.runExclusive(async () => {
				this.watchdogTimerLastPatTime = Date.now();
			});
		}
	}

	private async trySendTxforChunk(
		usersChunk: Array<[PublicKey, UserAccount]>
	): Promise<void> {
		const success = await this.sendTxforChunk(usersChunk);
		if (!success) {
			const slice = usersChunk.length / 2;
			await sleepMs(SLEEP_MS);
			await this.trySendTxforChunk(usersChunk.slice(0, slice));
			await sleepMs(SLEEP_MS);
			await this.trySendTxforChunk(usersChunk.slice(slice));
		}
		await sleepMs(SLEEP_MS);
	}

	private async sendTxforChunk(
		usersChunk: Array<[PublicKey, UserAccount]>
	): Promise<boolean> {
		if (usersChunk.length === 0) {
			return true;
		}

		let success = false;
		try {
			const ixs = [
				ComputeBudgetProgram.setComputeUnitLimit({
					units: 2_000_000,
				}),
			];
			usersChunk.forEach(async ([userAccountPublicKey, userAccount]) => {
				ixs.push(
					await this.driftClient.getUpdateUserIdleIx(
						userAccountPublicKey,
						userAccount
					)
				);
			});
			const txSigAndSlot =
				await this.driftClient.txSender.sendVersionedTransaction(
					await this.driftClient.txSender.getVersionedTransaction(
						ixs,
						[this.lookupTableAccount!],
						[],
						this.driftClient.opts
					),
					[],
					this.driftClient.opts
				);
			this.logTxAndSlotForUsers(txSigAndSlot, usersChunk);
			success = true;
		} catch (e) {
			const userKeys = usersChunk
				.map(([userAccountPublicKey, _]) => userAccountPublicKey.toBase58())
				.join(', ');
			logger.error(`Failed to idle users: ${userKeys}`);
			logger.error(e);
		}
		return success;
	}

	private logTxAndSlotForUsers(
		txSigAndSlot: TxSigAndSlot,
		usersChunk: Array<[PublicKey, UserAccount]>
	) {
		const txSig = txSigAndSlot.txSig;
		for (const [userAccountPublicKey, _] of usersChunk) {
			logger.info(
				`Flipped user ${userAccountPublicKey.toBase58()} https://solscan.io/tx/${txSig}`
			);
		}
	}
}

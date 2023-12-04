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

const USER_IDLE_CHUNKS = 9;

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
			})
		);
		this.userMap = new UserMap(
			this.driftClient,
			{
				type: 'polling',
				accountLoader: bulkAccountLoader,
			},
			false
		);
	}

	public async init() {
		logger.info(`${this.name} initing`);

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

			const userIdlePromises: Array<Promise<TxSigAndSlot>> = [];
			for (let i = 0; i < usersToIdle.length; i += USER_IDLE_CHUNKS) {
				const usersChunk = usersToIdle.slice(i, i + USER_IDLE_CHUNKS);
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
					userIdlePromises.push(
						this.driftClient.txSender.sendVersionedTransaction(
							await this.driftClient.txSender.getVersionedTransaction(
								ixs,
								[this.lookupTableAccount!],
								[],
								this.driftClient.opts
							),
							[],
							this.driftClient.opts
						)
					);
				} catch (err) {
					if (!(err instanceof Error)) {
						return;
					}
					logger.error(`Error idling user: ${err.message}`);
				}
			}
			const txs = await Promise.all(userIdlePromises);
			for (const tx of txs) {
				logger.info(`https://solscan.io/tx/${tx.txSig}`);
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
}

import {
	DriftClient,
	UserAccount,
	PublicKey,
	UserMap,
	ZERO,
	DriftClientConfig,
	BulkAccountLoader,
	RetryTxSender,
	PriorityFeeSubscriber,
	getUserFilter,
	getNonIdleUserFilter,
} from '@drift-labs/sdk';
import { decodeUser } from '@drift-labs/sdk/lib/decode/user';
import { Mutex } from 'async-mutex';

import { getErrorCode } from '../error';
import { logger } from '../logger';
import { Bot } from '../types';
import { webhookMessage } from '../webhook';
import { BaseBotConfig } from '../config';
import { simulateAndGetTxWithCUs, sleepMs } from '../utils';
import {
	AddressLookupTableAccount,
	RpcResponseAndContext,
	SendTransactionError,
	TransactionInstruction,
} from '@solana/web3.js';

const SETTLE_LP_CHUNKS = 4;
const SLEEP_MS = 500;
const PRIORITY_FEE_SUBSCRIBER_FREQ_MS = 1000;
const MAX_COMPUTE_UNIT_PRICE_MICRO_LAMPORTS = 10000; // cap the computeUnitPrice to pay for settlePnl txs

const errorCodesToSuppress = [
	6010, // Error Code: UserHasNoPositionInMarket. Error Number: 6010. Error Message: User Has No Position In Market.
	6035, // Error Code: InvalidOracle. Error Number: 6035. Error Message: InvalidOracle.
	6078, // Error Code: PerpMarketNotFound. Error Number: 6078. Error Message: PerpMarketNotFound.
	6095, // Error Code: InsufficientCollateralForSettlingPNL. Error Number: 6095. Error Message: InsufficientCollateralForSettlingPNL.
];

export class UserLpSettlerBot implements Bot {
	public readonly name: string;
	public readonly dryRun: boolean;
	public readonly runOnce: boolean;
	public readonly defaultIntervalMs: number = 600000;

	private driftClient: DriftClient;
	private lookupTableAccount?: AddressLookupTableAccount;
	private intervalIds: Array<NodeJS.Timer> = [];
	private userMap: UserMap;
	private priorityFeeSubscriber?: PriorityFeeSubscriber;

	private watchdogTimerMutex = new Mutex();
	private watchdogTimerLastPatTime = Date.now();

	constructor(driftClientConfigs: DriftClientConfig, config: BaseBotConfig) {
		this.name = config.botId;
		this.dryRun = config.dryRun;
		this.runOnce = config.runOnce || false;

		const bulkAccountLoader = new BulkAccountLoader(
			driftClientConfigs.connection,
			driftClientConfigs.connection.commitment || 'confirmed',
			0
		);
		this.driftClient = new DriftClient(
			Object.assign({}, driftClientConfigs, {
				accountSubscription: {
					type: 'polling',
					accountLoader: bulkAccountLoader,
				},
				txSender: new RetryTxSender({
					connection: driftClientConfigs.connection,
					wallet: driftClientConfigs.wallet,
					opts: driftClientConfigs.opts,
					timeout: 3000,
				}),
			})
		);
		this.userMap = new UserMap({
			driftClient: this.driftClient,
			subscriptionConfig: {
				type: 'polling',
				frequency: 0,
				commitment: this.driftClient.opts?.commitment,
			},
			skipInitialLoad: false,
			includeIdle: false,
			disableSyncOnTotalAccountsChange: true,
		});
	}

	public async init() {
		const start = Date.now();
		logger.info(`${this.name} initing`);
		await this.driftClient.subscribe();
		if (!(await this.driftClient.getUser().exists())) {
			throw new Error(
				`User for ${this.driftClient.wallet.publicKey.toString()} does not exist`
			);
		}
		this.lookupTableAccount =
			await this.driftClient.fetchMarketLookupTableAccount();

		const perpMarkets = this.driftClient
			.getPerpMarketAccounts()
			.map((m) => m.pubkey);

		logger.info(
			`Lp settler looking at ${perpMarkets.length} perp markets to determine priority fee`
		);

		this.priorityFeeSubscriber = new PriorityFeeSubscriber({
			connection: this.driftClient.connection,
			frequencyMs: PRIORITY_FEE_SUBSCRIBER_FREQ_MS,
			addresses: [...perpMarkets],
		});
		await this.priorityFeeSubscriber.subscribe();
		await sleepMs(PRIORITY_FEE_SUBSCRIBER_FREQ_MS);

		// logger.info(`Initializing UserMap`);
		// const startUserMapSub = Date.now();
		// await this.userMap.subscribe();
		// logger.info(`UserMap init took: ${Date.now() - startUserMapSub} ms`);

		logger.info(`${this.name} init'd! took ${Date.now() - start}`);
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
			await this.trySettleLps();
		} else {
			const intervalId = setInterval(this.trySettleLps.bind(this), intervalMs);
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

	private async trySettleLps() {
		const start = Date.now();
		try {
			const lpsPerMarket: { [key: number]: number } = {};
			const settleLpIxs: Array<TransactionInstruction> = [];

			logger.info(`Loading users that have been LPs...`);
			const fetchLpUsersStart = Date.now();
			const users = await this.fetchLpUsers();
			logger.info(`Fetch LPs took ${Date.now() - fetchLpUsersStart}`);
			let usersDoneCount = 0;

			// logger.info(`Going through ${this.userMap!.size()} users...`);
			// for (const user of this.userMap!.values()) {
			logger.info(`Going through ${users.length} users...`);
			for (const userData of users) {
				const user = userData.userAccount;
				usersDoneCount++;
				if (usersDoneCount % 100 === 0) {
					logger.info(
						`Processed ${usersDoneCount}/${this.userMap.size()} users...`
					);
				}
				// for (const pos of user.getActivePerpPositions()) {
				for (const pos of user.perpPositions) {
					if (pos.lpShares.eq(ZERO)) {
						continue;
					}

					if (lpsPerMarket[pos.marketIndex] === undefined) {
						lpsPerMarket[pos.marketIndex] = 0;
					} else {
						lpsPerMarket[pos.marketIndex] += 1;
					}

					settleLpIxs.push(
						await this.driftClient.settleLPIx(
							userData.publicKey,
							pos.marketIndex
						)
					);
				}
			}

			logger.info(
				`Settling ${
					settleLpIxs.length
				} LP positions. LPs per market: ${JSON.stringify(lpsPerMarket)}`
			);

			for (let i = 0; i < settleLpIxs.length; i += SETTLE_LP_CHUNKS) {
				const chunk = settleLpIxs.slice(i, i + SETTLE_LP_CHUNKS);
				await this.trySendTxForChunk(chunk);
			}
		} catch (err) {
			console.error(err);
			if (!(err instanceof Error)) {
				return;
			}
			if (
				!err.message.includes('Transaction was not confirmed') &&
				!err.message.includes('Blockhash not found')
			) {
				const errorCode = getErrorCode(err);
				if (errorCodesToSuppress.includes(errorCode!)) {
					console.log(`Suppressing error code: ${errorCode}`);
				} else {
					const simError = err as SendTransactionError;
					if (simError) {
						await webhookMessage(
							`[${
								this.name
							}]: :x: Uncaught error: Error code: ${errorCode} while settling LPs:\n${
								simError.logs!
									? (simError.logs as Array<string>).join('\n')
									: ''
							}\n${err.stack ? err.stack : err.message}`
						);
					}
				}
			}
		} finally {
			logger.info(`Settle LPs finished in ${Date.now() - start}ms`);
			await this.watchdogTimerMutex.runExclusive(async () => {
				this.watchdogTimerLastPatTime = Date.now();
			});
		}
	}

	async trySendTxForChunk(ixs: TransactionInstruction[]): Promise<void> {
		const success = await this.sendTxForChunk(ixs);
		if (!success) {
			const slice = ixs.length / 2;
			if (slice < 1) {
				await webhookMessage(
					`[${this.name}]: :x: Failed to settle LPs, reduced until 0 ixs...`
				);
				return;
			}
			await sleepMs(SLEEP_MS);
			await this.sendTxForChunk(ixs.slice(0, slice));
			await sleepMs(SLEEP_MS);
			await this.sendTxForChunk(ixs.slice(slice));
		}
		await sleepMs(SLEEP_MS);
	}

	async sendTxForChunk(ixs: TransactionInstruction[]): Promise<boolean> {
		if (ixs.length == 0) {
			return true;
		}

		let success = false;
		logger.info(
			`Using avgPriorityFee: ${
				this.priorityFeeSubscriber!.lastAvgStrategyResult
			} (clamp to ${MAX_COMPUTE_UNIT_PRICE_MICRO_LAMPORTS})`
		);
		try {
			const simResult = await simulateAndGetTxWithCUs(
				ixs,
				this.driftClient.connection,
				this.driftClient.txSender,
				[this.lookupTableAccount!],
				[],
				undefined,
				1.15,
				true,
				true
			);
			logger.info(
				`Settle LP estimated ${simResult.cuEstimate} CUs for ${ixs.length} settle LPs.`
			);

			if (simResult.simError !== null) {
				logger.error(
					`Sim error: ${JSON.stringify(simResult.simError)}\n${
						simResult.simTxLogs ? simResult.simTxLogs.join('\n') : ''
					}`
				);
				success = false;
			} else {
				const txSig = await this.driftClient.txSender.sendVersionedTransaction(
					simResult.tx,
					[],
					this.driftClient.opts
				);
				logger.info(
					`Settled LPs for ${ixs.length} users in tx: https://solana.fm/tx/${txSig.txSig}`
				);
				success = true;
			}
		} catch (err) {
			console.error(err);
			// const userKeys = users
			// 	.map(({ settleeUserAccountPublicKey }) =>
			// 		settleeUserAccountPublicKey.toBase58()
			// 	)
			// 	.join(', ');
			// logger.error(`Failed to settle pnl for users: ${userKeys}`);
			// logger.error(err);

			// if (err instanceof Error) {
			// 	const errorCode = getErrorCode(err) ?? 0;
			// 	if (!errorCodesToSuppress.includes(errorCode) && users.length === 1) {
			// 		if (err instanceof SendTransactionError) {
			// 			await webhookMessage(
			// 				`[${this.name
			// 				}]: :x: Error code: ${errorCode} while settling pnls for ${marketIndex}:\n${err.logs ? (err.logs as Array<string>).join('\n') : ''
			// 				}\n${err.stack ? err.stack : err.message}`
			// 			);
			// 		}
			// 	}
			// }
		}
		return success;
	}

	private async fetchLpUsers(): Promise<
		Array<{
			publicKey: PublicKey;
			userAccount: UserAccount;
		}>
	> {
		const rpcRequestArgs = [
			this.driftClient.program.programId.toBase58(),
			{
				commitment: 'confirmed',
				filters: [
					getUserFilter(),
					getNonIdleUserFilter(),
					// getUserThatHasBeenLP()], // doesn't work
				],
				encoding: 'base64',
				withContext: true,
			},
		];

		const rpcJSONResponse: any =
			// @ts-ignore
			await this.driftClient.connection._rpcRequest(
				'getProgramAccounts',
				rpcRequestArgs
			);

		const rpcResponseAndContext: RpcResponseAndContext<
			Array<{
				pubkey: PublicKey;
				account: {
					data: [string, string];
				};
			}>
		> = rpcJSONResponse.result;

		console.log(`Users: ${rpcResponseAndContext.value.length}`);
		return rpcResponseAndContext.value.map((programAccount) => {
			const data = programAccount.account.data;
			// @ts-ignore
			const buffer = Buffer.from(data[0], data[1]);
			return {
				publicKey: programAccount.pubkey,
				userAccount: decodeUser(buffer),
			};
		});
	}
}

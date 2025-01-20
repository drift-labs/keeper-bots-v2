import {
	DriftClient,
	UserMap,
	ZERO,
	BN,
	timeRemainingUntilUpdate,
	isOperationPaused,
	PerpOperation,
	decodeName,
	PriorityFeeSubscriberMap,
	DriftMarketInfo,
} from '@drift-labs/sdk';
import { Mutex } from 'async-mutex';

import { getErrorCode } from '../error';
import { logger } from '../logger';
import { Bot } from '../types';
import { webhookMessage } from '../webhook';
import { BaseBotConfig } from '../config';
import {
	getDriftPriorityFeeEndpoint,
	handleSimResultError,
	simulateAndGetTxWithCUs,
	sleepMs,
} from '../utils';
import {
	AddressLookupTableAccount,
	ComputeBudgetProgram,
	PublicKey,
	SendTransactionError,
} from '@solana/web3.js';

const SETTLE_LP_CHUNKS = 4;
const SLEEP_MS = 500;
const CU_EST_MULTIPLIER = 1.25;

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
	public readonly defaultIntervalMs: number = 30 * 60 * 1000;

	private driftClient: DriftClient;
	private lookupTableAccount?: AddressLookupTableAccount;
	private intervalIds: Array<NodeJS.Timer> = [];
	private userMap: UserMap;
	private priorityFeeSubscriberMap?: PriorityFeeSubscriberMap;
	private inProgress = false;

	private watchdogTimerMutex = new Mutex();
	private watchdogTimerLastPatTime = Date.now();

	constructor(driftClient: DriftClient, config: BaseBotConfig) {
		this.name = config.botId;
		this.dryRun = config.dryRun;
		this.runOnce = config.runOnce || false;

		this.driftClient = driftClient;
		this.userMap = new UserMap({
			driftClient: this.driftClient,
			subscriptionConfig: {
				type: 'polling',
				frequency: 0,
				commitment: this.driftClient.opts?.commitment,
			},
			skipInitialLoad: true,
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

		const perpMarkets: PublicKey[] = [];
		const driftMarkets: DriftMarketInfo[] = [];
		for (const perpMarket of this.driftClient.getPerpMarketAccounts()) {
			perpMarkets.push(perpMarket.pubkey);
			driftMarkets.push({
				marketType: 'perp',
				marketIndex: perpMarket.marketIndex,
			});
		}
		this.priorityFeeSubscriberMap = new PriorityFeeSubscriberMap({
			driftPriorityFeeEndpoint: getDriftPriorityFeeEndpoint('mainnet-beta'),
			driftMarkets,
			frequencyMs: 10_000,
		});
		await this.priorityFeeSubscriberMap.subscribe();

		logger.info(
			`Lp settler looking at ${perpMarkets.length} perp markets to determine priority fee`
		);

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
			await this.trySettleLps();
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
		if (this.inProgress) {
			logger.info(`Settle LPs already in progress, skipping...`);
			return;
		}
		const start = Date.now();
		try {
			this.inProgress = true;

			logger.info(`Loading users that have been LPs...`);
			const fetchLpUsersStart = Date.now();
			await this.driftClient.fetchAccounts();
			await this.userMap.sync();
			logger.info(`Fetch LPs took ${Date.now() - fetchLpUsersStart}`);

			const nowTs = new BN(Date.now() / 1000);

			const lpsToSettleInMarketMap = new Map<number, PublicKey[]>();
			logger.info(`Going through ${this.userMap.size()} users...`);
			for (const user of this.userMap.values()) {
				const userAccount = user.getUserAccount();

				const freeCollateral = user.getFreeCollateral('Initial');

				for (const pos of userAccount.perpPositions) {
					if (pos.lpShares.eq(ZERO)) {
						continue;
					}

					let shouldSettleLp = false;
					if (freeCollateral.lte(ZERO)) {
						logger.info(
							`user ${user.getUserAccountPublicKey()} has ${freeCollateral.toString()} free collateral with a position in market ${
								pos.marketIndex
							}`
						);
						shouldSettleLp = true;
					}

					const perpMarketAccount = this.driftClient.getPerpMarketAccount(
						pos.marketIndex
					);

					const timeTillFunding = timeRemainingUntilUpdate(
						nowTs,
						perpMarketAccount!.amm.lastFundingRateTs,
						perpMarketAccount!.amm.fundingPeriod
					);

					// 10 min away from funding
					if (timeTillFunding.lte(new BN(600))) {
						logger.info(
							`user ${user.getUserAccountPublicKey()} funding within 5 min`
						);
						shouldSettleLp = true;
					}

					// check that the position will change with settle_lp call
					const posWithLp = user.getPerpPositionWithLPSettle(
						pos.marketIndex,
						pos
					);
					if (
						shouldSettleLp &&
						(posWithLp[0].baseAssetAmount.eq(pos.baseAssetAmount) ||
							posWithLp[0].quoteAssetAmount.eq(pos.quoteAssetAmount))
					) {
						shouldSettleLp = false;
					}

					if (!shouldSettleLp) {
						continue;
					}

					if (lpsToSettleInMarketMap.get(pos.marketIndex) === undefined) {
						lpsToSettleInMarketMap.set(pos.marketIndex, []);
					}

					lpsToSettleInMarketMap
						.get(pos.marketIndex)!
						.push(user.getUserAccountPublicKey());
				}
			}

			for (const [
				marketIndex,
				usersToSettle,
			] of lpsToSettleInMarketMap.entries()) {
				const perpMarket = this.driftClient.getPerpMarketAccount(marketIndex)!;
				const settlePnlPaused =
					isOperationPaused(
						perpMarket.pausedOperations,
						PerpOperation.SETTLE_PNL
					) ||
					isOperationPaused(
						perpMarket.pausedOperations,
						PerpOperation.SETTLE_PNL_WITH_POSITION
					);
				if (settlePnlPaused) {
					const marketStr = decodeName(perpMarket.name);
					logger.warn(
						`Settle PNL paused for market ${marketStr}, skipping settle PNL`
					);
					continue;
				}

				logger.info(
					`Settling ${usersToSettle.length} LPs in ${
						usersToSettle.length / SETTLE_LP_CHUNKS
					} chunks for market ${marketIndex}`
				);

				const allTxPromises = [];
				for (let i = 0; i < usersToSettle.length; i += SETTLE_LP_CHUNKS) {
					const chunk = usersToSettle.slice(i, i + SETTLE_LP_CHUNKS);
					allTxPromises.push(this.trySendTxForChunk(marketIndex, chunk));
				}

				logger.info(`Waiting for ${allTxPromises.length} txs to settle...`);
				const settleStart = Date.now();
				await Promise.all(allTxPromises);
				logger.info(`Settled in ${Date.now() - settleStart}ms`);
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
			this.inProgress = false;
			logger.info(`Settle LPs finished in ${Date.now() - start}ms`);
			await this.watchdogTimerMutex.runExclusive(async () => {
				this.watchdogTimerLastPatTime = Date.now();
			});
		}
	}

	async trySendTxForChunk(
		marketIndex: number,
		users: PublicKey[]
	): Promise<void> {
		const success = await this.sendTxForChunk(marketIndex, users);
		if (!success) {
			const slice = users.length / 2;
			if (slice < 1) {
				logger.error(
					`[${this.name}]: :x: Failed to settle LPs, reduced until 0 ixs...`
				);
				return;
			}

			const slice0 = users.slice(0, slice);
			const slice1 = users.slice(slice);
			logger.info(
				`Chunk failed: ${users
					.map((u) => u.toBase58())
					.join(' ')}, retrying with:\nslice0: ${slice0
					.map((u) => u.toBase58())
					.join(' ')}\nslice1: ${slice1.map((u) => u.toBase58()).join(' ')}`
			);

			await sleepMs(SLEEP_MS);
			await this.sendTxForChunk(marketIndex, slice0);
			await sleepMs(SLEEP_MS);
			await this.sendTxForChunk(marketIndex, slice1);
		}
		await sleepMs(SLEEP_MS);
	}

	/**
	 * Send a transaction for a chunk of ixs
	 * @param ixs
	 * @returns true if the transaction was successful, false if it failed (and to retry with 1/2 of ixs)
	 */
	async sendTxForChunk(
		marketIndex: number,
		users: PublicKey[]
	): Promise<boolean> {
		if (users.length == 0) {
			return true;
		}

		let success = false;
		try {
			const pfs = this.priorityFeeSubscriberMap!.getPriorityFees(
				'perp',
				marketIndex
			);
			let microLamports = 10_000;
			if (pfs) {
				microLamports = Math.floor(pfs.medium);
			}
			const ixs = [
				ComputeBudgetProgram.setComputeUnitLimit({
					units: 1_400_000, // simulateAndGetTxWithCUs will overwrite
				}),
				ComputeBudgetProgram.setComputeUnitPrice({
					microLamports,
				}),
				...(await Promise.all(
					users.map((u) => this.driftClient.settleLPIx(u, marketIndex))
				)),
			];

			const recentBlockhash =
				await this.driftClient.connection.getLatestBlockhash('confirmed');
			const simResult = await simulateAndGetTxWithCUs({
				ixs,
				connection: this.driftClient.connection,
				payerPublicKey: this.driftClient.wallet.publicKey,
				lookupTableAccounts: [this.lookupTableAccount!],
				cuLimitMultiplier: CU_EST_MULTIPLIER,
				doSimulation: true,
				recentBlockhash: recentBlockhash.blockhash,
			});
			logger.info(
				`Settle LP estimated ${simResult.cuEstimate} CUs for ${ixs.length} settle LPs.`
			);

			if (simResult.simError !== null) {
				logger.error(
					`Sim error: ${JSON.stringify(simResult.simError)}\n${
						simResult.simTxLogs ? simResult.simTxLogs.join('\n') : ''
					}`
				);
				handleSimResultError(simResult, errorCodesToSuppress, `(settleLps)`);
				success = false;
			} else {
				const txSig = await this.driftClient.txSender.sendVersionedTransaction(
					simResult.tx,
					[],
					{
						...this.driftClient.opts,
						maxRetries: 10,
					}
				);
				logger.info(
					`Settled LPs for ${ixs.length} users in tx: https://solana.fm/tx/${txSig.txSig}`
				);
				success = true;
			}
		} catch (e) {
			const err = e as Error;
			if (err.message.includes('Transaction was not confirmed')) {
				logger.error(
					`Transaction was not confirmed, but we'll assume it's fine`
				);
				success = true;
			} else {
				logger.error(
					`Other error while settling LPs: ${err.message}\n${
						err.stack ? err.stack : 'no stack'
					}`
				);
			}
		}
		return success;
	}
}

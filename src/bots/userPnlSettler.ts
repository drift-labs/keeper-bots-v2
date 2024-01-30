import {
	BN,
	DriftClient,
	UserAccount,
	PublicKey,
	PerpMarketAccount,
	SpotMarketAccount,
	OraclePriceData,
	calculateClaimablePnl,
	QUOTE_PRECISION,
	UserMap,
	ZERO,
	calculateNetUserPnlImbalance,
	convertToNumber,
	isOracleValid,
	isVariant,
	TxSigAndSlot,
	timeRemainingUntilUpdate,
	getTokenAmount,
	SpotBalanceType,
	calculateNetUserPnl,
	BASE_PRECISION,
	QUOTE_SPOT_MARKET_INDEX,
	DriftClientConfig,
	BulkAccountLoader,
	RetryTxSender,
	PriorityFeeSubscriber,
	TxSender,
} from '@drift-labs/sdk';
import { Mutex } from 'async-mutex';

import { getErrorCode } from '../error';
import { logger } from '../logger';
import { Bot } from '../types';
import { webhookMessage } from '../webhook';
import { BaseBotConfig } from '../config';
import { decodeName, simulateAndGetTxWithCUs, sleepMs } from '../utils';
import {
	AddressLookupTableAccount,
	ComputeBudgetProgram,
	SendTransactionError,
} from '@solana/web3.js';

type SettlePnlIxParams = {
	users: {
		settleeUserAccountPublicKey: PublicKey;
		settleeUserAccount: UserAccount;
	}[];
	marketIndex: number;
};

const MIN_PNL_TO_SETTLE = new BN(-10).mul(QUOTE_PRECISION);
const SETTLE_USER_CHUNKS = 4;
const SLEEP_MS = 500;

const errorCodesToSuppress = [
	6010, // Error Code: UserHasNoPositionInMarket. Error Number: 6010. Error Message: User Has No Position In Market.
	6035, // Error Code: InvalidOracle. Error Number: 6035. Error Message: InvalidOracle.
	6078, // Error Code: PerpMarketNotFound. Error Number: 6078. Error Message: PerpMarketNotFound.
	6095, // Error Code: InsufficientCollateralForSettlingPNL. Error Number: 6095. Error Message: InsufficientCollateralForSettlingPNL.
];

export class UserPnlSettlerBot implements Bot {
	public readonly name: string;
	public readonly dryRun: boolean;
	public readonly runOnce: boolean;
	public readonly defaultIntervalMs: number = 600000;

	private driftClient: DriftClient;
	private lookupTableAccount?: AddressLookupTableAccount;
	private intervalIds: Array<NodeJS.Timer> = [];
	private bulkAccountLoader: BulkAccountLoader;
	private userMap: UserMap;
	private priorityFeeSubscriber?: PriorityFeeSubscriber;

	private watchdogTimerMutex = new Mutex();
	private watchdogTimerLastPatTime = Date.now();

	constructor(
		driftClientConfigs: DriftClientConfig,
		config: BaseBotConfig,
		priorityFeeSubscriber: PriorityFeeSubscriber,
		txSender: TxSender
	) {
		this.name = config.botId;
		this.dryRun = config.dryRun;
		this.runOnce = config.runOnce || false;

		const bulkAccountLoader = new BulkAccountLoader(
			driftClientConfigs.connection,
			driftClientConfigs.connection.commitment || 'processed',
			0
		);
		this.bulkAccountLoader = bulkAccountLoader;
		this.driftClient = new DriftClient(
			Object.assign({}, driftClientConfigs, {
				accountSubscription: {
					type: 'polling',
					accountLoader: bulkAccountLoader,
				},
				txSender,
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

		const spotMarkets = this.driftClient
			.getSpotMarketAccounts()
			.map((m) => m.pubkey);
		const perpMarkets = this.driftClient
			.getPerpMarketAccounts()
			.map((m) => m.pubkey);
		this.priorityFeeSubscriber = priorityFeeSubscriber;
		this.priorityFeeSubscriber.updateAddresses([
			...spotMarkets,
			...perpMarkets,
		]);

		logger.info(
			`Pnl settler looking at ${spotMarkets.length} spot markets and ${perpMarkets.length} perp markets to determine priority fee`
		);
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

		logger.info(`${this.name} init'd!`);
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
			await this.trySettlePnl();
		} else {
			const intervalId = setInterval(this.trySettlePnl.bind(this), intervalMs);
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

	private async trySettlePnl() {
		const start = Date.now();
		try {
			const perpMarketAndOracleData: {
				[marketIndex: number]: {
					marketAccount: PerpMarketAccount;
					oraclePriceData: OraclePriceData;
				};
			} = {};
			const spotMarketAndOracleData: {
				[marketIndex: number]: {
					marketAccount: SpotMarketAccount;
					oraclePriceData: OraclePriceData;
				};
			} = {};

			for (const perpMarket of this.driftClient.getPerpMarketAccounts()) {
				perpMarketAndOracleData[perpMarket.marketIndex] = {
					marketAccount: perpMarket,
					oraclePriceData: this.driftClient.getOracleDataForPerpMarket(
						perpMarket.marketIndex
					),
				};
			}
			for (const spotMarket of this.driftClient.getSpotMarketAccounts()) {
				spotMarketAndOracleData[spotMarket.marketIndex] = {
					marketAccount: spotMarket,
					oraclePriceData: this.driftClient.getOracleDataForSpotMarket(
						spotMarket.marketIndex
					),
				};
			}

			const slot = (await this.bulkAccountLoader.mostRecentSlot) ?? 0;

			const validOracleMarketMap = new Map<number, boolean>();
			for (const market of this.driftClient.getPerpMarketAccounts()) {
				const oracleValid = isOracleValid(
					perpMarketAndOracleData[market.marketIndex].marketAccount.amm,
					perpMarketAndOracleData[market.marketIndex].oraclePriceData,
					this.driftClient.getStateAccount().oracleGuardRails,
					slot
				);

				if (!oracleValid) {
					logger.warn(`Oracle for market ${market.marketIndex} is not valid`);
				}

				validOracleMarketMap.set(market.marketIndex, oracleValid);
			}

			const usersToSettle: SettlePnlIxParams[] = [];
			const nowTs = Date.now() / 1000;

			for (const user of this.userMap!.values()) {
				const userAccount = user.getUserAccount();
				const isUsdcBorrow =
					userAccount.spotPositions[0] &&
					isVariant(userAccount.spotPositions[0].balanceType, 'borrow');
				const usdcAmount = user.getTokenAmount(QUOTE_SPOT_MARKET_INDEX);

				for (const settleePosition of user.getActivePerpPositions()) {
					// only settle active positions (base amount) or negative quote
					if (
						settleePosition.quoteAssetAmount.gte(ZERO) &&
						settleePosition.baseAssetAmount.eq(ZERO) &&
						settleePosition.lpShares.eq(ZERO)
					) {
						continue;
					}

					const perpMarketIdx = settleePosition.marketIndex;
					const perpMarket =
						perpMarketAndOracleData[perpMarketIdx].marketAccount;

					let settleePositionWithLp = settleePosition;

					if (!settleePosition.lpShares.eq(ZERO)) {
						settleePositionWithLp = user.getPerpPositionWithLPSettle(
							perpMarketIdx,
							settleePosition
						)[0];
					}
					const spotMarketIdx = 0;

					const oracleValid = validOracleMarketMap.get(perpMarketIdx);
					if (!oracleValid) {
						continue;
					}
					if (
						!perpMarketAndOracleData[perpMarketIdx] ||
						!spotMarketAndOracleData[spotMarketIdx]
					) {
						continue;
					}

					const userUnsettledPnl = calculateClaimablePnl(
						perpMarketAndOracleData[perpMarketIdx].marketAccount,
						spotMarketAndOracleData[spotMarketIdx].marketAccount, // always liquidating the USDC spot market
						settleePositionWithLp,
						perpMarketAndOracleData[perpMarketIdx].oraclePriceData
					);

					const twoPctOfOpenInterestBase = BN.min(
						perpMarket.amm.baseAssetAmountLong,
						perpMarket.amm.baseAssetAmountShort.abs()
					).div(new BN(50));
					const fiveHundredNotionalBase = QUOTE_PRECISION.mul(new BN(500))
						.mul(BASE_PRECISION)
						.div(perpMarket.amm.historicalOracleData.lastOraclePriceTwap5Min);

					const largeUnsettledLP =
						perpMarket.amm.baseAssetAmountWithUnsettledLp
							.abs()
							.gt(twoPctOfOpenInterestBase) &&
						perpMarket.amm.baseAssetAmountWithUnsettledLp
							.abs()
							.gt(fiveHundredNotionalBase);

					const shouldSettleLp =
						settleePosition.lpShares.gt(ZERO) &&
						(timeRemainingUntilUpdate(
							new BN(nowTs ?? Date.now() / 1000),
							perpMarketAndOracleData[perpMarketIdx].marketAccount.amm
								.lastFundingRateTs,
							perpMarketAndOracleData[perpMarketIdx].marketAccount.amm
								.fundingPeriod
						).ltn(120) ||
							largeUnsettledLP);

					// only settle for $10 or more negative pnl
					if (
						(userUnsettledPnl.eq(ZERO) &&
							settleePositionWithLp.lpShares.eq(ZERO)) ||
						(userUnsettledPnl.gt(MIN_PNL_TO_SETTLE) &&
							!settleePositionWithLp.baseAssetAmount.eq(ZERO) &&
							!isUsdcBorrow &&
							settleePositionWithLp.lpShares.eq(ZERO))
					) {
						continue;
					}

					// if user has usdc borrow, only settle if magnitude of pnl is material ($10 and 1% of borrow)
					if (
						isUsdcBorrow &&
						(userUnsettledPnl.abs().lt(MIN_PNL_TO_SETTLE.abs()) ||
							userUnsettledPnl.abs().lt(usdcAmount.abs().div(new BN(100))))
					) {
						continue;
					}

					if (settleePositionWithLp.lpShares.gt(ZERO) && !shouldSettleLp) {
						continue;
					}

					const pnlPool =
						perpMarketAndOracleData[perpMarketIdx].marketAccount.pnlPool;
					let pnlToSettleWithUser = ZERO;
					let pnlPoolTookenAmount = ZERO;
					if (userUnsettledPnl.gt(ZERO)) {
						pnlPoolTookenAmount = getTokenAmount(
							pnlPool.scaledBalance,
							spotMarketAndOracleData[pnlPool.marketIndex].marketAccount,
							SpotBalanceType.DEPOSIT
						);
						pnlToSettleWithUser = BN.min(userUnsettledPnl, pnlPoolTookenAmount);
					}

					if (pnlToSettleWithUser.gt(ZERO)) {
						const netUserPnl = calculateNetUserPnl(
							perpMarketAndOracleData[perpMarketIdx].marketAccount,
							perpMarketAndOracleData[perpMarketIdx].oraclePriceData
						);
						let maxPnlPoolExcess = ZERO;
						if (netUserPnl.lt(pnlPoolTookenAmount)) {
							maxPnlPoolExcess = pnlPoolTookenAmount.sub(
								BN.max(netUserPnl, ZERO)
							);
						}

						// we're only allowed to settle positive pnl if pnl pool is in excess
						if (maxPnlPoolExcess.lte(ZERO)) {
							logger.warn(
								`Want to settle positive PnL for user ${user
									.getUserAccountPublicKey()
									.toBase58()} in market ${perpMarketIdx}, but maxPnlPoolExcess is: (${convertToNumber(
									maxPnlPoolExcess,
									QUOTE_PRECISION
								)})`
							);
							continue;
						}

						const netUserPnlImbalance = calculateNetUserPnlImbalance(
							perpMarketAndOracleData[perpMarketIdx].marketAccount,
							spotMarketAndOracleData[spotMarketIdx].marketAccount,
							perpMarketAndOracleData[perpMarketIdx].oraclePriceData
						);
						if (netUserPnlImbalance.gt(ZERO)) {
							logger.warn(
								`Want to settle positive PnL for user ${user
									.getUserAccountPublicKey()
									.toBase58()} in market ${perpMarketIdx}, protocol's AMM lacks excess PnL (${convertToNumber(
									netUserPnlImbalance,
									QUOTE_PRECISION
								)})`
							);
							continue;
						}
					}

					// only settle user pnl if they have enough collateral
					if (user.canBeLiquidated().canBeLiquidated) {
						logger.warn(
							`Want to settle negative PnL for user ${user
								.getUserAccountPublicKey()
								.toBase58()}, but they have insufficient collateral`
						);
						continue;
					}

					const userData = {
						settleeUserAccountPublicKey: user.getUserAccountPublicKey(),
						settleeUserAccount: userAccount,
					};
					if (
						usersToSettle
							.map((item) => item.marketIndex)
							.includes(perpMarketIdx)
					) {
						const foundItem = usersToSettle.find(
							(item) => item.marketIndex == perpMarketIdx
						);
						if (foundItem) {
							foundItem.users.push(userData);
						}
					} else {
						usersToSettle.push({
							users: [userData],
							marketIndex: settleePosition.marketIndex,
						});
					}
				}
			}

			for (const params of usersToSettle) {
				const marketStr = decodeName(
					this.driftClient.getPerpMarketAccount(params.marketIndex)!.name
				);

				logger.info(
					`Trying to settle PNL for ${params.users.length} users on market ${marketStr}`
				);

				if (this.dryRun) {
					throw new Error('Dry run - not sending settle pnl tx');
				}

				for (let i = 0; i < params.users.length; i += SETTLE_USER_CHUNKS) {
					const usersChunk = params.users.slice(i, i + SETTLE_USER_CHUNKS);
					await this.trySendTxForChunk(params.marketIndex, usersChunk);
				}
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
							}]: :x: Uncaught error: Error code: ${errorCode} while settling pnls:\n${
								simError.logs!
									? (simError.logs as Array<string>).join('\n')
									: ''
							}\n${err.stack ? err.stack : err.message}`
						);
					}
				}
			}
		} finally {
			logger.info(`Settle PNLs finished in ${Date.now() - start}ms`);
			await this.watchdogTimerMutex.runExclusive(async () => {
				this.watchdogTimerLastPatTime = Date.now();
			});
		}
	}

	async trySendTxForChunk(
		marketIndex: number,
		users: {
			settleeUserAccountPublicKey: PublicKey;
			settleeUserAccount: UserAccount;
		}[]
	): Promise<void> {
		const success = await this.sendTxForChunk(marketIndex, users);
		if (!success) {
			const slice = users.length / 2;
			if (slice < 1) {
				return;
			}
			await sleepMs(SLEEP_MS);
			await this.sendTxForChunk(marketIndex, users.slice(0, slice));
			await sleepMs(SLEEP_MS);
			await this.sendTxForChunk(marketIndex, users.slice(slice));
		}
		await sleepMs(SLEEP_MS);
	}

	async sendTxForChunk(
		marketIndex: number,
		users: {
			settleeUserAccountPublicKey: PublicKey;
			settleeUserAccount: UserAccount;
		}[]
	): Promise<boolean> {
		if (users.length == 0) {
			return true;
		}

		let success = false;
		try {
			const ixs = [
				ComputeBudgetProgram.setComputeUnitLimit({
					units: 1_400_000, // simulateAndGetTxWithCUs will overwrite
				}),
				ComputeBudgetProgram.setComputeUnitPrice({
					microLamports: Math.floor(
						this.priorityFeeSubscriber!.getCustomStrategyResult()
					),
				}),
			];
			ixs.push(
				...(await this.driftClient.getSettlePNLsIxs(users, [marketIndex]))
			);

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
				`Settle Pnl estimated ${simResult.cuEstimate} CUs for ${ixs.length} ixs, ${users.length} users.`
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
				success = true;

				this.logTxAndSlotForUsers(
					txSig,
					marketIndex,
					users.map(
						({ settleeUserAccountPublicKey }) => settleeUserAccountPublicKey
					)
				);
			}
		} catch (err) {
			const userKeys = users
				.map(({ settleeUserAccountPublicKey }) =>
					settleeUserAccountPublicKey.toBase58()
				)
				.join(', ');
			logger.error(`Failed to settle pnl for users: ${userKeys}`);
			logger.error(err);

			if (err instanceof Error) {
				const errorCode = getErrorCode(err) ?? 0;
				if (!errorCodesToSuppress.includes(errorCode) && users.length === 1) {
					if (err instanceof SendTransactionError) {
						await webhookMessage(
							`[${
								this.name
							}]: :x: Error code: ${errorCode} while settling pnls for ${marketIndex}:\n${
								err.logs ? (err.logs as Array<string>).join('\n') : ''
							}\n${err.stack ? err.stack : err.message}`
						);
					}
				}
			}
		}
		return success;
	}

	private logTxAndSlotForUsers(
		txSigAndSlot: TxSigAndSlot,
		marketIndex: number,
		userAccountPublicKeys: Array<PublicKey>
	) {
		const txSig = txSigAndSlot.txSig;
		for (const userAccountPublicKey of userAccountPublicKeys) {
			logger.info(
				`Settled pnl user ${userAccountPublicKey.toBase58()} in market ${marketIndex} https://solana.fm/tx/${txSig}`
			);
		}
	}
}

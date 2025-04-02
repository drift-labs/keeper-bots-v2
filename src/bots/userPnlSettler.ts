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
	isOperationPaused,
	PerpOperation,
	PriorityFeeSubscriberMap,
	DriftMarketInfo,
	SlotSubscriber,
} from '@drift-labs/sdk';
import { Mutex } from 'async-mutex';

import { getErrorCode } from '../error';
import { logger } from '../logger';
import { Bot } from '../types';
import { webhookMessage } from '../webhook';
import { GlobalConfig, UserPnlSettlerConfig } from '../config';
import {
	decodeName,
	getDriftPriorityFeeEndpoint,
	handleSimResultError,
	simulateAndGetTxWithCUs,
	sleepMs,
} from '../utils';
import {
	AddressLookupTableAccount,
	ComputeBudgetProgram,
	SendTransactionError,
	TransactionExpiredBlockheightExceededError,
} from '@solana/web3.js';

const SETTLE_USER_CHUNKS = 4;
const SLEEP_MS = 500;
const CU_EST_MULTIPLIER = 1.25;

const FILTER_FOR_MARKET = undefined; // undefined;
const EMPTY_USER_SETTLE_INTERVAL_MS = 1 * 60 * 1000; // 1 minutes

const errorCodesToSuppress = [
	6010, // Error Code: UserHasNoPositionInMarket. Error Number: 6010. Error Message: User Has No Position In Market.
	6035, // Error Code: InvalidOracle. Error Number: 6035. Error Message: InvalidOracle.
	6078, // Error Code: PerpMarketNotFound. Error Number: 6078. Error Message: PerpMarketNotFound.
	6095, // Error Code: InsufficientCollateralForSettlingPNL. Error Number: 6095. Error Message: InsufficientCollateralForSettlingPNL.
	6259, // Error Code: NoUnsettledPnl. Error Number: 6259. Error Message: NoUnsettledPnl.
];

export class UserPnlSettlerBot implements Bot {
	public readonly name: string;
	public readonly dryRun: boolean;
	public readonly runOnce: boolean;
	public readonly defaultIntervalMs: number = 5 * 60 * 1000;

	private driftClient: DriftClient;
	private slotSubscriber: SlotSubscriber;
	private globalConfig: GlobalConfig;
	private lookupTableAccounts?: AddressLookupTableAccount[];
	private intervalIds: Array<NodeJS.Timer> = [];
	private userMap: UserMap;
	private priorityFeeSubscriberMap?: PriorityFeeSubscriberMap;
	private inProgress = false;
	private marketIndexes: Array<number>;
	private minPnlToSettle: BN;
	private maxUsersToConsider: number;

	private watchdogTimerMutex = new Mutex();
	private watchdogTimerLastPatTime = Date.now();

	constructor(
		driftClient: DriftClient,
		slotSubscriber: SlotSubscriber,
		config: UserPnlSettlerConfig,
		globalConfig: GlobalConfig
	) {
		this.name = config.botId;
		this.dryRun = config.dryRun;
		this.runOnce = config.runOnce || false;
		this.marketIndexes = config.perpMarketIndicies ?? [];
		this.minPnlToSettle = new BN(
			Math.abs(Number(config.settlePnlThresholdUsdc) ?? 10) * -1
		).mul(QUOTE_PRECISION);
		this.maxUsersToConsider = Number(config.maxUsersToConsider) ?? 50;
		this.globalConfig = globalConfig;

		this.driftClient = driftClient;
		this.slotSubscriber = slotSubscriber;
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

		const driftMarkets: DriftMarketInfo[] = [];
		for (const perpMarket of this.driftClient.getPerpMarketAccounts()) {
			driftMarkets.push({
				marketType: 'perp',
				marketIndex: perpMarket.marketIndex,
			});
		}
		logger.info(
			`Adding ${driftMarkets.length} perp markets to PriorityFeeSubscriberMap`
		);
		this.priorityFeeSubscriberMap = new PriorityFeeSubscriberMap({
			driftPriorityFeeEndpoint: getDriftPriorityFeeEndpoint(
				this.globalConfig.driftEnv!
			),
			driftMarkets,
			frequencyMs: 10_000,
		});
		await this.priorityFeeSubscriberMap!.subscribe();
		await this.driftClient.subscribe();

		await this.userMap.subscribe();
		this.lookupTableAccounts =
			await this.driftClient.fetchAllLookupTableAccounts();

		logger.info(`${this.name} init'd!`);
	}

	public async reset() {
		for (const intervalId of this.intervalIds) {
			clearInterval(intervalId as NodeJS.Timeout);
		}
		this.intervalIds = [];

		await this.priorityFeeSubscriberMap!.unsubscribe();
		await this.userMap?.unsubscribe();
	}

	public async startIntervalLoop(intervalMs?: number): Promise<void> {
		logger.info(`${this.name} Bot started!`);
		if (this.runOnce) {
			await this.trySettlePnl();
			await this.trySettleUsersWithNoPositions();
		} else {
			await this.trySettlePnl();
			await this.trySettleUsersWithNoPositions();
			this.intervalIds.push(
				setInterval(this.trySettlePnl.bind(this), intervalMs)
			);
			this.intervalIds.push(
				setInterval(
					this.trySettleUsersWithNoPositions.bind(this),
					EMPTY_USER_SETTLE_INTERVAL_MS
				)
			);
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
		if (this.inProgress) {
			logger.info(`Settle PnLs already in progress, skipping...`);
			return;
		}
		const start = Date.now();
		try {
			this.inProgress = true;

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

			for (const market of this.driftClient.getPerpMarketAccounts()) {
				const oracleValid = isOracleValid(
					perpMarketAndOracleData[market.marketIndex].marketAccount,
					perpMarketAndOracleData[market.marketIndex].oraclePriceData,
					this.driftClient.getStateAccount().oracleGuardRails,
					this.slotSubscriber.getSlot()
				);

				if (!oracleValid) {
					logger.warn(`Oracle for market ${market.marketIndex} is not valid`);
				}
			}

			const usersToSettleMap: Map<
				number,
				{
					settleeUserAccountPublicKey: PublicKey;
					settleeUserAccount: UserAccount;
					pnl: number;
				}[]
			> = new Map();
			const nowTs = Date.now() / 1000;

			for (const user of this.userMap!.values()) {
				const userAccount = user.getUserAccount();
				if (userAccount.poolId !== 0) {
					continue;
				}
				const userAccKeyStr = user.getUserAccountPublicKey().toBase58();
				const isUsdcBorrow =
					userAccount.spotPositions[0] &&
					isVariant(userAccount.spotPositions[0].balanceType, 'borrow');
				const usdcAmount = user.getTokenAmount(QUOTE_SPOT_MARKET_INDEX);

				for (const settleePosition of user.getActivePerpPositions()) {
					logger.debug(
						`Checking user ${userAccKeyStr} position in market ${settleePosition.marketIndex}`
					);

					if (
						this.marketIndexes.length > 0 &&
						!this.marketIndexes.includes(settleePosition.marketIndex)
					) {
						logger.info(
							`Skipping user ${userAccKeyStr} in market ${
								settleePosition.marketIndex
							} because it's not in the market indexes to settle (${this.marketIndexes.join(
								', '
							)})`
						);
						continue;
					}

					// only settle active positions (base amount) or negative quote
					if (
						settleePosition.quoteAssetAmount.gte(ZERO) &&
						settleePosition.baseAssetAmount.eq(ZERO) &&
						settleePosition.lpShares.eq(ZERO)
					) {
						logger.debug(
							`Skipping user ${userAccKeyStr}-${settleePosition.marketIndex} because no base amount and has positive quote amount`
						);
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

					const settlePnlWithPositionPaused = isOperationPaused(
						perpMarket.pausedOperations,
						PerpOperation.SETTLE_PNL_WITH_POSITION
					);

					if (
						settlePnlWithPositionPaused &&
						!settleePositionWithLp.baseAssetAmount.eq(ZERO)
					) {
						logger.debug(
							`Skipping user ${userAccKeyStr}-${settleePosition.marketIndex} because settling pnl with position blocked`
						);
						continue;
					}

					if (
						!perpMarketAndOracleData[perpMarketIdx] ||
						!spotMarketAndOracleData[spotMarketIdx]
					) {
						logger.error(
							`Skipping user ${userAccKeyStr}-${settleePosition.marketIndex} because no spot market or oracle data`
						);
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

					const timeToFundingUpdate = timeRemainingUntilUpdate(
						new BN(nowTs ?? Date.now() / 1000),
						perpMarketAndOracleData[perpMarketIdx].marketAccount.amm
							.lastFundingRateTs,
						perpMarketAndOracleData[perpMarketIdx].marketAccount.amm
							.fundingPeriod
					);

					const shouldSettleLp =
						settleePosition.lpShares.gt(ZERO) &&
						(timeToFundingUpdate.ltn(15 * 60) || // settle lp positions within 15 min of funding update
							largeUnsettledLP);
					logger.debug(
						`User ${userAccKeyStr}-${settleePosition.marketIndex} shouldSettleLp: ${shouldSettleLp}, largeUnsettledLp: ${largeUnsettledLP}, timeToUpdate: ${timeToFundingUpdate}`
					);

					// skip users that have:
					// (no unsettledPnl AND no lpShares)
					// OR
					// (if they have unsettledPnl > min AND baseAmount != 0 AND no usdcBorrow AND no lpShares)
					if (
						(userUnsettledPnl.eq(ZERO) &&
							settleePositionWithLp.lpShares.eq(ZERO)) ||
						(userUnsettledPnl.gt(this.minPnlToSettle) &&
							!settleePositionWithLp.baseAssetAmount.eq(ZERO) &&
							!isUsdcBorrow &&
							settleePositionWithLp.lpShares.eq(ZERO))
					) {
						logger.debug(
							`Skipping user ${userAccKeyStr}-${settleePosition.marketIndex} with (no unsettledPnl and no lpShares) OR (unsettledPnl > $10 and no usdcBorrow and no lpShares)`
						);
						continue;
					}

					// if user has usdc borrow, only settle if magnitude of pnl is material ($10 and 1% of borrow)
					if (
						isUsdcBorrow &&
						(userUnsettledPnl.abs().lt(this.minPnlToSettle.abs()) ||
							userUnsettledPnl.abs().lt(usdcAmount.abs().div(new BN(100))))
					) {
						logger.debug(
							`Skipping user ${userAccKeyStr}-${settleePosition.marketIndex} with usdcBorrow AND (unsettledPnl < $10 or unsettledPnl < 1% of borrow)`
						);
						continue;
					}

					if (settleePositionWithLp.lpShares.gt(ZERO) && !shouldSettleLp) {
						logger.debug(
							`Skipping user ${userAccKeyStr}-${
								settleePosition.marketIndex
							} with lpShares and shouldSettleLp === false, timeToFundingUpdate: ${timeToFundingUpdate.toNumber()}`
						);
						continue;
					}

					const pnlPool =
						perpMarketAndOracleData[perpMarketIdx].marketAccount.pnlPool;
					let pnlToSettleWithUser = ZERO;
					let pnlPoolTookenAmount = ZERO;
					let settleUser = false;
					if (userUnsettledPnl.gt(ZERO)) {
						pnlPoolTookenAmount = getTokenAmount(
							pnlPool.scaledBalance,
							spotMarketAndOracleData[pnlPool.marketIndex].marketAccount,
							SpotBalanceType.DEPOSIT
						);
						pnlToSettleWithUser = BN.min(userUnsettledPnl, pnlPoolTookenAmount);
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
						settleUser = true;
					} else {
						// only settle negative pnl if unsettled pnl is material
						if (userUnsettledPnl.abs().gte(this.minPnlToSettle.abs())) {
							logger.info(
								`Settling negative pnl for user ${user
									.getUserAccountPublicKey()
									.toBase58()} in market ${
									settleePosition.marketIndex
								} with unsettled pnl: ${convertToNumber(
									userUnsettledPnl,
									QUOTE_PRECISION
								)}`
							);
							settleUser = true;
						}
					}

					if (!settleUser) {
						continue;
					}

					const userData = {
						settleeUserAccountPublicKey: user.getUserAccountPublicKey(),
						settleeUserAccount: userAccount,
						pnl: convertToNumber(userUnsettledPnl, QUOTE_PRECISION),
					};

					if (usersToSettleMap.has(settleePosition.marketIndex)) {
						const existingData = usersToSettleMap.get(
							settleePosition.marketIndex
						)!;
						existingData.push(userData);
					} else {
						usersToSettleMap.set(settleePosition.marketIndex, [userData]);
					}
				}
			}

			for (const [marketIndex, params] of usersToSettleMap) {
				const perpMarket = this.driftClient.getPerpMarketAccount(marketIndex)!;
				const marketStr = decodeName(perpMarket.name);

				if (FILTER_FOR_MARKET !== undefined) {
					if (marketIndex !== FILTER_FOR_MARKET) {
						logger.info(
							`Skipping market ${marketStr} because FILTER_FOR_MARKET is set to ${FILTER_FOR_MARKET}`
						);
						continue;
					}
				}

				const settlePnlPaused = isOperationPaused(
					perpMarket.pausedOperations,
					PerpOperation.SETTLE_PNL
				);
				if (settlePnlPaused) {
					logger.warn(
						`Settle PNL paused for market ${marketStr}, skipping settle PNL`
					);
					continue;
				}

				logger.info(
					`Trying to settle PNL for ${params.length} users on market ${marketStr}`
				);

				const sortedParams = params
					.sort((a, b) => a.pnl - b.pnl)
					.slice(0, this.maxUsersToConsider);

				logger.info(
					`Settling ${sortedParams.length} users in ${
						sortedParams.length / SETTLE_USER_CHUNKS
					} chunks for market ${marketIndex}`
				);

				const allTxPromises = [];
				for (let i = 0; i < sortedParams.length; i += SETTLE_USER_CHUNKS) {
					const usersChunk = sortedParams.slice(i, i + SETTLE_USER_CHUNKS);
					allTxPromises.push(this.trySendTxForChunk(marketIndex, usersChunk));
				}

				logger.info(`Waiting for ${allTxPromises.length} txs to settle...`);
				const settleStart = Date.now();
				await Promise.all(allTxPromises);
				logger.info(
					`Settled ${sortedParams.length} users in market ${marketIndex} in ${
						Date.now() - settleStart
					}ms`
				);
			}
		} catch (err) {
			console.error('Error in trySettlePnl', err);
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
			this.inProgress = false;
			logger.info(`Settle PNLs finished in ${Date.now() - start}ms`);
			await this.watchdogTimerMutex.runExclusive(async () => {
				this.watchdogTimerLastPatTime = Date.now();
			});
		}
	}

	private async trySettleUsersWithNoPositions() {
		try {
			const usersToSettleMap: Map<
				number,
				{
					settleeUserAccountPublicKey: PublicKey;
					settleeUserAccount: UserAccount;
					pnl: number;
				}[]
			> = new Map();
			for (const user of this.userMap!.values()) {
				if (user.getUserAccount().poolId !== 0) {
					continue;
				}
				const perpPositions = user.getActivePerpPositions();
				for (const perpPosition of perpPositions) {
					// this loop only processes empty positions
					if (!perpPosition.baseAssetAmount.eq(ZERO)) {
						continue;
					}

					const perpMarket = this.driftClient.getPerpMarketAccount(
						perpPosition.marketIndex
					)!;
					const settlePnlPaused = isOperationPaused(
						perpMarket.pausedOperations,
						PerpOperation.SETTLE_PNL
					);
					if (settlePnlPaused) {
						logger.warn(
							`Settle PNL paused for market ${perpPosition.marketIndex}, skipping settle PNL`
						);
						continue;
					}

					const pnl = convertToNumber(
						perpPosition.quoteAssetAmount,
						QUOTE_PRECISION
					);
					logger.info(
						`[trySettleUsersWithNoPositions] User ${user
							.getUserAccountPublicKey()
							.toBase58()} has empty perp position in market ${
							perpPosition.marketIndex
						} with unsettled pnl: ${pnl}`
					);

					const userData = {
						settleeUserAccountPublicKey: user.getUserAccountPublicKey(),
						settleeUserAccount: user.getUserAccount(),
						pnl,
					};

					if (usersToSettleMap.has(perpPosition.marketIndex)) {
						const existingData = usersToSettleMap.get(
							perpPosition.marketIndex
						)!;
						existingData.push(userData);
					} else {
						usersToSettleMap.set(perpPosition.marketIndex, [userData]);
					}
				}
			}

			if (usersToSettleMap.size === 0) {
				logger.info('[trySettleUsersWithNoPositions] No users to settle');
				return;
			}

			for (const [marketIndex, params] of usersToSettleMap) {
				const perpMarket = this.driftClient.getPerpMarketAccount(marketIndex)!;
				const marketStr = decodeName(perpMarket.name);

				if (
					FILTER_FOR_MARKET !== undefined &&
					marketIndex !== FILTER_FOR_MARKET
				) {
					logger.info(
						`[trySettleUsersWithNoPositions] Skipping market ${marketStr} because FILTER_FOR_MARKET is set to ${FILTER_FOR_MARKET}`
					);
					continue;
				}

				logger.info(
					`[trySettleUsersWithNoPositions] Trying to settle PNL for ${params.length} users with no positions on market ${marketStr}`
				);

				const sortedParams = params
					.sort((a, b) => a.pnl - b.pnl)
					.slice(0, this.maxUsersToConsider);

				logger.info(
					`[trySettleUsersWithNoPositions] Settling ${
						sortedParams.length
					} users in ${
						sortedParams.length / SETTLE_USER_CHUNKS
					} chunks for market ${marketIndex}`
				);

				const allTxPromises = [];
				for (let i = 0; i < sortedParams.length; i += SETTLE_USER_CHUNKS) {
					const usersChunk = sortedParams.slice(i, i + SETTLE_USER_CHUNKS);
					allTxPromises.push(this.trySendTxForChunk(marketIndex, usersChunk));
				}

				logger.info(
					`[trySettleUsersWithNoPositions] Waiting for ${allTxPromises.length} txs to settle...`
				);
				const settleStart = Date.now();
				await Promise.all(allTxPromises);
				logger.info(
					`[trySettleUsersWithNoPositions] Settled ${
						sortedParams.length
					} users in market ${marketIndex} in ${Date.now() - settleStart}ms`
				);
			}
		} catch (err) {
			console.error('Error in trySettleUsersWithNoPositions', err);
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

			const slice0 = users.slice(0, slice);
			const slice1 = users.slice(slice);
			logger.info(
				`[trySendTxForChunk] Chunk failed: ${users
					.map((u) => u.settleeUserAccountPublicKey.toBase58())
					.join(' ')}, retrying with:\nslice0: ${slice0
					.map((u) => u.settleeUserAccountPublicKey.toBase58())
					.join(' ')}\nslice1: ${slice1
					.map((u) => u.settleeUserAccountPublicKey.toBase58())
					.join(' ')}`
			);

			await sleepMs(SLEEP_MS);
			await this.sendTxForChunk(marketIndex, slice0);

			await sleepMs(SLEEP_MS);
			await this.sendTxForChunk(marketIndex, slice1);
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
			const pfs = this.priorityFeeSubscriberMap!.getPriorityFees(
				'perp',
				marketIndex
			);
			let microLamports = 10_000;
			if (pfs && pfs.medium) {
				microLamports = Math.floor(pfs.medium);
			}
			const ixs = [
				ComputeBudgetProgram.setComputeUnitLimit({
					units: 1_400_000, // simulateAndGetTxWithCUs will overwrite
				}),
				ComputeBudgetProgram.setComputeUnitPrice({
					microLamports,
				}),
			];
			ixs.push(
				...(await this.driftClient.getSettlePNLsIxs(users, [marketIndex]))
			);

			const recentBlockhash =
				await this.driftClient.connection.getLatestBlockhash('confirmed');
			const simResult = await simulateAndGetTxWithCUs({
				ixs,
				connection: this.driftClient.connection,
				payerPublicKey: this.driftClient.wallet.publicKey,
				lookupTableAccounts: this.lookupTableAccounts!,
				cuLimitMultiplier: CU_EST_MULTIPLIER,
				doSimulation: true,
				recentBlockhash: recentBlockhash.blockhash,
			});
			logger.info(
				`[sendTxForChunk] Settle Pnl estimated ${simResult.cuEstimate} CUs for ${ixs.length} ixs, ${users.length} users.`
			);
			if (simResult.simError !== null) {
				logger.error(
					`Sim error: ${JSON.stringify(simResult.simError)}\n${
						simResult.simTxLogs ? simResult.simTxLogs.join('\n') : ''
					}`
				);
				handleSimResultError(simResult, errorCodesToSuppress, `(settlePnL)`);
				success = false;
			} else {
				const sendTxStart = Date.now();
				const txSig = await this.driftClient.txSender.sendVersionedTransaction(
					simResult.tx,
					[],
					this.driftClient.opts
				);
				const sendTxDuration = Date.now() - sendTxStart;
				success = true;

				this.logTxAndSlotForUsers(
					txSig,
					marketIndex,
					users.map(
						({ settleeUserAccountPublicKey }) => settleeUserAccountPublicKey
					),
					sendTxDuration
				);
			}
		} catch (err) {
			const userKeys = users
				.map(({ settleeUserAccountPublicKey }) =>
					settleeUserAccountPublicKey.toBase58()
				)
				.join(', ');
			logger.error(`Failed to settle pnl for users: ${userKeys}`);
			console.error(err);

			if (err instanceof TransactionExpiredBlockheightExceededError) {
				logger.info(
					`Blockheight exceeded error, retrying with same set of users (${users.length} users on market ${marketIndex})`
				);
				success = await this.sendTxForChunk(marketIndex, users);
			} else if (err instanceof Error) {
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
		userAccountPublicKeys: Array<PublicKey>,
		sendTxDuration: number
	) {
		const txSig = txSigAndSlot.txSig;
		let userStr = '';
		let countUsers = 0;
		for (const userAccountPublicKey of userAccountPublicKeys) {
			userStr += `${userAccountPublicKey.toBase58()}, `;
			countUsers++;
		}
		logger.info(
			`Settled pnl in market ${marketIndex}. For ${countUsers} users: ${userStr}. Took: ${sendTxDuration}ms. https://solana.fm/tx/${txSig}`
		);
	}
}

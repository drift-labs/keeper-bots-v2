import {
	BN,
	DriftClient,
	UserAccount,
	PublicKey,
	PerpMarketAccount,
	OraclePriceData,
	calculateClaimablePnl,
	QUOTE_PRECISION,
	UserMap,
	ZERO,
	calculateNetUserPnlImbalance,
	convertToNumber,
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
	User,
	PerpPosition,
	MarketStatus,
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
import { ENUM_UTILS } from '@drift/common';

// =============================================================================
// CONSTANTS
// =============================================================================

const SETTLE_USER_CHUNKS = 4;
const SLEEP_MS = 500;
const CU_EST_MULTIPLIER = 1.25;

const FILTER_FOR_MARKET = undefined; // undefined;
const EMPTY_USER_SETTLE_INTERVAL_MS = 60 * 60 * 1000; // 1 hour
const POSITIVE_PNL_SETTLE_INTERVAL_MS = 60 * 60 * 1000; // 1 hour
const ALL_NEGATIVE_PNL_SETTLE_INTERVAL_MS = 60 * 60 * 1000; // 1 hour
const MIN_MARGIN_RATIO_FOR_POSITIVE_PNL = 0.1; // 10% of account value

const errorCodesToSuppress = [
	6010, // Error Code: UserHasNoPositionInMarket. Error Number: 6010. Error Message: User Has No Position In Market.
	6035, // Error Code: InvalidOracle. Error Number: 6035. Error Message: InvalidOracle.
	6078, // Error Code: PerpMarketNotFound. Error Number: 6078. Error Message: PerpMarketNotFound.
	6095, // Error Code: InsufficientCollateralForSettlingPNL. Error Number: 6095. Error Message: InsufficientCollateralForSettlingPNL.
	6259, // Error Code: NoUnsettledPnl. Error Number: 6259. Error Message: NoUnsettledPnl.
];

// =============================================================================
// TYPES
// =============================================================================

interface UserToSettle {
	settleeUserAccountPublicKey: PublicKey;
	settleeUserAccount: UserAccount;
	pnl: number;
}

// =============================================================================
// MAIN CLASS
// =============================================================================

export class UserPnlSettlerBot implements Bot {
	public readonly name: string;
	public readonly dryRun: boolean;
	public readonly runOnce: boolean;
	public readonly defaultIntervalMs: number = 5 * 60 * 1000;

	// =============================================================================
	// DEPENDENCIES
	// =============================================================================

	private driftClient: DriftClient;
	private slotSubscriber: SlotSubscriber;
	private globalConfig: GlobalConfig;
	private lookupTableAccounts?: AddressLookupTableAccount[];
	private userMap: UserMap;
	private priorityFeeSubscriberMap?: PriorityFeeSubscriberMap;

	// =============================================================================
	// CONFIGURATION
	// =============================================================================

	private marketIndexes: Array<number>;
	private minPnlToSettle: BN;
	private maxUsersToConsider: number;

	// =============================================================================
	// STATE MANAGEMENT
	// =============================================================================

	private intervalIds: Array<NodeJS.Timer> = [];
	private inProgress = false;
	private watchdogTimerMutex = new Mutex();
	private watchdogTimerLastPatTime = Date.now();

	// =============================================================================
	// CONSTRUCTOR & LIFECYCLE
	// =============================================================================

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
		const negativeInterval = intervalMs || this.defaultIntervalMs;

		if (this.runOnce) {
			logger.info('Running once mode - executing all settlement types');
			await this.trySettleNegativePnl();
			await this.trySettleAllNegativePnl();
			await this.trySettleUsersWithNoPositions();
			await this.trySettlePositivePnlForLowMargin();
		} else {
			// Initial settlement on startup - only settle negative PnL
			await this.trySettleNegativePnl();

			// Set up intervals
			// Top 10 largest negative PnL users every 5 minutes
			this.intervalIds.push(
				setInterval(this.trySettleNegativePnl.bind(this), negativeInterval)
			);
			// Comprehensive negative PnL settlement every hour
			this.intervalIds.push(
				setInterval(
					this.trySettleAllNegativePnl.bind(this),
					ALL_NEGATIVE_PNL_SETTLE_INTERVAL_MS
				)
			);
			// Users with no positions every hour
			this.intervalIds.push(
				setInterval(
					this.trySettleUsersWithNoPositions.bind(this),
					EMPTY_USER_SETTLE_INTERVAL_MS
				)
			);
			// Positive PnL settlement for low margin users every hour
			this.intervalIds.push(
				setInterval(
					this.trySettlePositivePnlForLowMargin.bind(this),
					POSITIVE_PNL_SETTLE_INTERVAL_MS
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

	// =============================================================================
	// MAIN SETTLEMENT ORCHESTRATION
	// =============================================================================

	private async trySettleNegativePnl() {
		if (this.inProgress) {
			logger.info(`Settle PnLs already in progress, skipping...`);
			return;
		}
		const start = Date.now();
		try {
			this.inProgress = true;
			logger.info('Starting top 10 negative PnL settlement...');
			await this.settleLargestNegativePnl();
		} catch (err) {
			this.handleSettlementError(err, 'trySettleNegativePnl');
		} finally {
			this.inProgress = false;
			logger.info(
				`Top 10 negative PnL settlement finished in ${Date.now() - start}ms`
			);
			await this.updateWatchdogTimer();
		}
	}

	private async trySettlePositivePnlForLowMargin() {
		if (this.inProgress) {
			logger.info(
				`Settle PnLs already in progress, skipping positive PnL settlement...`
			);
			return;
		}
		const start = Date.now();
		try {
			this.inProgress = true;
			logger.info('Starting positive PnL settlement for low margin users...');
			await this.settlePnl({ positiveOnly: true, requireLowMargin: true });
		} catch (err) {
			this.handleSettlementError(err, 'trySettlePositivePnlForLowMargin');
		} finally {
			this.inProgress = false;
			logger.info(
				`Settle positive PNLs for low margin finished in ${
					Date.now() - start
				}ms`
			);
			await this.updateWatchdogTimer();
		}
	}

	private async trySettleAllNegativePnl() {
		if (this.inProgress) {
			logger.info(
				`Settle PnLs already in progress, skipping comprehensive negative PnL settlement...`
			);
			return;
		}
		const start = Date.now();
		try {
			this.inProgress = true;
			logger.info('Starting comprehensive negative PnL settlement...');
			await this.settlePnl({ positiveOnly: false });
		} catch (err) {
			this.handleSettlementError(err, 'trySettleAllNegativePnl');
		} finally {
			this.inProgress = false;
			logger.info(
				`Settle all negative PNLs finished in ${Date.now() - start}ms`
			);
			await this.updateWatchdogTimer();
		}
	}

	private async trySettleUsersWithNoPositions() {
		try {
			const usersToSettleMap = await this.findUsersWithNoPositions();

			if (usersToSettleMap.size === 0) {
				logger.info('[trySettleUsersWithNoPositions] No users to settle');
				return;
			}

			await this.processUserSettlements(
				usersToSettleMap,
				'[trySettleUsersWithNoPositions]'
			);
		} catch (err) {
			this.handleSettlementError(err, 'trySettleUsersWithNoPositions');
		}
	}

	// =============================================================================
	// PNL SETTLEMENT LOGIC
	// =============================================================================

	private async settlePnl(options: {
		positiveOnly: boolean;
		requireLowMargin?: boolean;
	}) {
		const usersToSettleMap = await this.findUsersToSettle(options);
		await this.processUserSettlements(usersToSettleMap);
	}

	private async settleLargestNegativePnl() {
		const nowTs = Date.now() / 1000;
		const usersToSettleMap = await this.findLargestNegativePnlUsersToSettle(
			nowTs
		);
		await this.processUserSettlements(
			usersToSettleMap,
			'[settleLargestNegativePnl]'
		);
	}

	private async findUsersToSettle(options: {
		positiveOnly: boolean;
		requireLowMargin?: boolean;
	}): Promise<Map<number, UserToSettle[]>> {
		const usersToSettleMap: Map<number, UserToSettle[]> = new Map();
		const nowTs = Date.now() / 1000;

		for (const user of this.userMap!.values()) {
			const userAccount = user.getUserAccount();
			if (userAccount.poolId !== 0) {
				continue;
			}

			// Check margin requirement for positive PnL settlement
			if (options.requireLowMargin && options.positiveOnly) {
				const userFreeMarginCurrent =
					user.getFreeCollateral('Initial').toNumber() /
					QUOTE_PRECISION.toNumber();
				const userAccountValue = user.getNetUsdValue().toNumber() / 1e6;

				// Only settle if user's free margin is less than 10% of account value
				if (
					userFreeMarginCurrent >=
					userAccountValue * MIN_MARGIN_RATIO_FOR_POSITIVE_PNL
				) {
					continue;
				}
			}

			const isUsdcBorrow =
				userAccount.spotPositions[0] &&
				isVariant(userAccount.spotPositions[0].balanceType, 'borrow');
			const usdcAmount = user.getTokenAmount(QUOTE_SPOT_MARKET_INDEX);

			for (const settleePosition of user.getActivePerpPositions()) {
				const settlementDecision = await this.shouldSettleUserPosition(
					user,
					settleePosition,
					nowTs,
					isUsdcBorrow,
					usdcAmount,
					options
				);

				if (settlementDecision.shouldSettle) {
					const userData: UserToSettle = {
						settleeUserAccountPublicKey: user.getUserAccountPublicKey(),
						settleeUserAccount: userAccount,
						pnl: settlementDecision.pnl!,
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
		}

		return usersToSettleMap;
	}

	private async findLargestNegativePnlUsersToSettle(
		nowTs: number
	): Promise<Map<number, UserToSettle[]>> {
		// Collect all potential negative PnL candidates by market
		const candidatesByMarket: Map<number, UserToSettle[]> = new Map();

		const totalUsers = this.userMap!.size();
		let candidatesFound = 0;

		logger.info(
			`[findLargestNegativePnlUsersToSettle] Processing ${totalUsers} users from userMap`
		);

		for (const user of this.userMap!.values()) {
			const userAccount = user.getUserAccount();
			if (userAccount.poolId !== 0) {
				continue;
			}

			const isUsdcBorrow =
				userAccount.spotPositions[0] &&
				isVariant(userAccount.spotPositions[0].balanceType, 'borrow');
			const usdcAmount = user.getTokenAmount(QUOTE_SPOT_MARKET_INDEX);

			const activePerpPositions = user.getActivePerpPositions();

			for (const settleePosition of activePerpPositions) {
				// Early PnL check to avoid processing positive PnL users
				const perpMarketIdx = settleePosition.marketIndex;
				const perpMarket =
					this.driftClient.getPerpMarketAccount(perpMarketIdx)!;
				const spotMarketIdx = QUOTE_SPOT_MARKET_INDEX;
				const spotMarket = this.driftClient.getSpotMarketAccount(spotMarketIdx);
				if (!spotMarket) {
					logger.warn(
						`Spot market ${spotMarketIdx} not found, skipping user ${user
							.getUserAccountPublicKey()
							.toBase58()}`
					);
					continue;
				}
				const oraclePriceData =
					this.driftClient.getOracleDataForPerpMarket(perpMarketIdx);

				const userUnsettledPnl = calculateClaimablePnl(
					perpMarket,
					spotMarket,
					settleePosition,
					oraclePriceData
				);

				// Skip positive PnL users early
				if (userUnsettledPnl.gt(ZERO)) {
					continue;
				}

				const settlementDecision = await this.shouldSettleUserPosition(
					user,
					settleePosition,
					nowTs,
					isUsdcBorrow,
					usdcAmount,
					{ positiveOnly: false }
				);

				if (settlementDecision.shouldSettle) {
					candidatesFound++;
					const userData: UserToSettle = {
						settleeUserAccountPublicKey: user.getUserAccountPublicKey(),
						settleeUserAccount: userAccount,
						pnl: settlementDecision.pnl!,
					};

					if (!candidatesByMarket.has(settleePosition.marketIndex)) {
						candidatesByMarket.set(settleePosition.marketIndex, []);
					}
					candidatesByMarket.get(settleePosition.marketIndex)!.push(userData);
				}
			}
		}

		logger.info(
			`[findLargestNegativePnlUsersToSettle] Found ${candidatesFound} candidates across ${candidatesByMarket.size} markets`
		);

		// Sort and select top 10 most negative PnL users per market
		const usersToSettleMap: Map<number, UserToSettle[]> = new Map();

		for (const [marketIndex, candidates] of candidatesByMarket) {
			// Sort by PnL ascending (most negative first)
			const sortedCandidates = candidates
				.sort((a, b) => a.pnl - b.pnl)
				.slice(0, 10); // Take top 10 most negative

			if (sortedCandidates.length > 0) {
				logger.info(
					`Market ${marketIndex}: Settling top ${
						sortedCandidates.length
					} users with PnLs: [${sortedCandidates
						.map((u) => `$${u.pnl.toFixed(2)}`)
						.join(', ')}]`
				);

				usersToSettleMap.set(marketIndex, sortedCandidates);
			}
		}

		return usersToSettleMap;
	}

	private async shouldSettleUserPosition(
		user: User,
		settleePosition: PerpPosition,
		nowTs: number,
		isUsdcBorrow: boolean,
		usdcAmount: BN,
		options: { positiveOnly: boolean; requireLowMargin?: boolean }
	): Promise<{ shouldSettle: boolean; pnl?: number }> {
		const userAccKeyStr = user.getUserAccountPublicKey().toBase58();

		// Check market filter
		if (
			this.marketIndexes.length > 0 &&
			!this.marketIndexes.includes(settleePosition.marketIndex)
		) {
			return { shouldSettle: false };
		}

		// Check if position has activity
		if (
			settleePosition.quoteAssetAmount.gte(ZERO) &&
			settleePosition.baseAssetAmount.eq(ZERO) &&
			settleePosition.lpShares.eq(ZERO)
		) {
			return { shouldSettle: false };
		}

		const perpMarketIdx = settleePosition.marketIndex;
		const perpMarket = this.driftClient.getPerpMarketAccount(perpMarketIdx)!;
		const spotMarketIdx = QUOTE_SPOT_MARKET_INDEX;

		// Check if settlement is paused
		const settlePnlWithPositionPaused = isOperationPaused(
			perpMarket.pausedOperations,
			PerpOperation.SETTLE_PNL_WITH_POSITION
		);

		if (
			settlePnlWithPositionPaused &&
			!settleePosition.baseAssetAmount.eq(ZERO)
		) {
			return { shouldSettle: false };
		}

		// Get fresh market and oracle data
		const spotMarket = this.driftClient.getSpotMarketAccount(spotMarketIdx);
		if (!spotMarket) {
			logger.warn(
				`Spot market ${spotMarketIdx} not found, cannot settle user ${userAccKeyStr}`
			);
			return { shouldSettle: false };
		}
		const oraclePriceData =
			this.driftClient.getOracleDataForPerpMarket(perpMarketIdx);

		const userUnsettledPnl = calculateClaimablePnl(
			perpMarket,
			spotMarket,
			settleePosition,
			oraclePriceData
		);

		// Apply PnL filtering based on options
		if (options.positiveOnly) {
			if (userUnsettledPnl.lte(ZERO)) {
				return { shouldSettle: false };
			}
			// For positive PnL, check if we can settle it
			const canSettlePositivePnl = await this.canSettlePositivePnl(
				user,
				userUnsettledPnl,
				perpMarketIdx,
				spotMarketIdx
			);
			if (!canSettlePositivePnl) {
				return { shouldSettle: false };
			}
		} else {
			// For negative PnL settlement, apply existing logic
			const hasZeroPnl = userUnsettledPnl.eq(ZERO);

			// Skip users with zero PnL and no LP shares - these don't need settlement
			if (hasZeroPnl) {
				return { shouldSettle: false };
			}

			// if user has usdc borrow, only settle if magnitude of pnl is material ($10 and 1% of borrow)
			if (
				isUsdcBorrow &&
				(userUnsettledPnl.abs().lt(this.minPnlToSettle.abs()) ||
					userUnsettledPnl.abs().lt(usdcAmount.abs().div(new BN(100))))
			) {
				return { shouldSettle: false };
			}

			// For negative PnL, check if user can be settled
			if (userUnsettledPnl.gt(ZERO)) {
				return { shouldSettle: false };
			} else {
				// only settle negative pnl if unsettled pnl is material
				if (!userUnsettledPnl.abs().gte(this.minPnlToSettle.abs())) {
					return { shouldSettle: false };
				}
			}
		}

		return {
			shouldSettle: true,
			pnl: convertToNumber(userUnsettledPnl, QUOTE_PRECISION),
		};
	}

	private async shouldSettleLpPosition(
		settleePosition: any,
		perpMarket: PerpMarketAccount,
		nowTs: number,
		perpMarketData: {
			marketAccount: PerpMarketAccount;
			oraclePriceData: OraclePriceData;
		}
	): Promise<boolean> {
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
			perpMarketData.marketAccount.amm.lastFundingRateTs,
			perpMarketData.marketAccount.amm.fundingPeriod
		);

		return (
			settleePosition.lpShares.gt(ZERO) &&
			(timeToFundingUpdate.ltn(15 * 60) || largeUnsettledLP)
		);
	}

	private async canSettlePositivePnl(
		user: any,
		userUnsettledPnl: BN,
		perpMarketIdx: number,
		spotMarketIdx: number
	): Promise<boolean> {
		const perpMarket = this.driftClient.getPerpMarketAccount(perpMarketIdx)!;
		const oraclePriceData =
			this.driftClient.getOracleDataForPerpMarket(perpMarketIdx);

		const pnlPool = perpMarket.pnlPool;
		const pnlPoolSpotMarket = this.driftClient.getSpotMarketAccount(
			pnlPool.marketIndex
		);
		if (!pnlPoolSpotMarket) {
			logger.warn(
				`Spot market ${pnlPool.marketIndex} not found for PnL pool, skipping positive PnL settlement check`
			);
			return false;
		}
		const pnlPoolTokenAmount = getTokenAmount(
			pnlPool.scaledBalance,
			pnlPoolSpotMarket,
			SpotBalanceType.DEPOSIT
		);

		const pnlToSettleWithUser = BN.min(userUnsettledPnl, pnlPoolTokenAmount);
		if (pnlToSettleWithUser.lte(ZERO)) {
			return false;
		}

		const netUserPnl = calculateNetUserPnl(perpMarket, oraclePriceData);

		let maxPnlPoolExcess = ZERO;
		if (netUserPnl.lt(pnlPoolTokenAmount)) {
			maxPnlPoolExcess = pnlPoolTokenAmount.sub(BN.max(netUserPnl, ZERO));
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
			return false;
		}

		const spotMarket = this.driftClient.getSpotMarketAccount(spotMarketIdx);
		if (!spotMarket) {
			logger.warn(
				`Spot market ${spotMarketIdx} not found for positive PnL settlement check`
			);
			return false;
		}
		const netUserPnlImbalance = calculateNetUserPnlImbalance(
			perpMarket,
			spotMarket,
			oraclePriceData
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
			return false;
		}

		// only settle user pnl if they have enough collateral
		if (user.canBeLiquidated().canBeLiquidated) {
			logger.warn(
				`Want to settle negative PnL for user ${user
					.getUserAccountPublicKey()
					.toBase58()}, but they have insufficient collateral`
			);
			return false;
		}

		return true;
	}

	private async findUsersWithNoPositions(): Promise<
		Map<number, UserToSettle[]>
	> {
		const usersToSettleMap: Map<number, UserToSettle[]> = new Map();

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
				if (pnl !== 0) {
					logger.info(
						`[trySettleUsersWithNoPositions] User ${user
							.getUserAccountPublicKey()
							.toBase58()} has empty perp position in market ${
							perpPosition.marketIndex
						} with unsettled pnl: ${pnl}`
					);

					const userData: UserToSettle = {
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
		}

		return usersToSettleMap;
	}

	private async processUserSettlements(
		usersToSettleMap: Map<number, UserToSettle[]>,
		logPrefix = ''
	) {
		for (const [marketIndex, params] of usersToSettleMap) {
			const perpMarket = this.driftClient.getPerpMarketAccount(marketIndex)!;
			const marketStr = decodeName(perpMarket.name);

			if (
				FILTER_FOR_MARKET !== undefined &&
				marketIndex !== FILTER_FOR_MARKET
			) {
				logger.info(
					`${logPrefix} Skipping market ${marketStr} because FILTER_FOR_MARKET is set to ${FILTER_FOR_MARKET}`
				);
				continue;
			}

			const settlePnlPaused = isOperationPaused(
				perpMarket.pausedOperations,
				PerpOperation.SETTLE_PNL
			);

			// cannot settle pnl in this state
			const marketIsReduceOnly = ENUM_UTILS.match(
				perpMarket.status,
				MarketStatus.REDUCE_ONLY
			);

			if (settlePnlPaused || marketIsReduceOnly) {
				logger.warn(
					`${logPrefix} Settle PNL paused for market ${marketStr}, skipping settle PNL`
				);
				continue;
			}

			logger.info(
				`${logPrefix} Trying to settle PNL for ${params.length} users on market ${marketStr}`
			);

			const sortedParams = params
				.sort((a, b) => a.pnl - b.pnl)
				.slice(0, this.maxUsersToConsider);

			logger.info(
				`${logPrefix} Settling ${sortedParams.length} users in ${Math.ceil(
					sortedParams.length / SETTLE_USER_CHUNKS
				)} chunks for market ${marketIndex} with respective PnLs: ${sortedParams
					.map((p) => p.pnl)
					.join(', ')}`
			);

			await this.executeSettlementForMarket(
				marketIndex,
				sortedParams,
				logPrefix
			);
		}
	}

	private async executeSettlementForMarket(
		marketIndex: number,
		users: UserToSettle[],
		logPrefix = ''
	) {
		const allTxPromises = [];
		for (let i = 0; i < users.length; i += SETTLE_USER_CHUNKS) {
			const usersChunk = users.slice(i, i + SETTLE_USER_CHUNKS);
			allTxPromises.push(this.trySendTxForChunk(marketIndex, usersChunk));
		}

		logger.info(
			`${logPrefix} Waiting for ${allTxPromises.length} txs to settle...`
		);
		const settleStart = Date.now();
		await Promise.all(allTxPromises);
		logger.info(
			`${logPrefix} Settled ${users.length} users in market ${marketIndex} in ${
				Date.now() - settleStart
			}ms`
		);
	}

	// =============================================================================
	// TRANSACTION SENDING
	// =============================================================================

	async trySendTxForChunk(
		marketIndex: number,
		users: UserToSettle[]
	): Promise<void> {
		const success = await this.sendTxForChunk(marketIndex, users);
		if (!success) {
			const slice = Math.floor(users.length / 2);
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
		users: UserToSettle[]
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
			try {
				ixs.push(
					...(await this.driftClient.getSettlePNLsIxs(users, [marketIndex]))
				);
			} catch (error) {
				logger.error(
					`Failed to get SettlePNLsIxs for users in market ${marketIndex}:`
				);
				logger.error(`Error: ${error}`);
				logger.error(
					`Users: ${users
						.map((u) => u.settleeUserAccountPublicKey.toBase58())
						.join(', ')}`
				);
				throw error;
			}

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
			if (simResult.simError !== null) {
				logger.error(
					`Sim error for users: ${users
						.map((u) => u.settleeUserAccountPublicKey.toBase58())
						.join(', ')}: ${JSON.stringify(simResult.simError)}\n${
						simResult.simTxLogs ? simResult.simTxLogs.join('\n') : ''
					}`
				);
				handleSimResultError(simResult, errorCodesToSuppress, `(settlePnL)`);
				success = false;
			} else {
				logger.info(
					`[sendTxForChunk] Settle Pnl estimated ${
						simResult.cuEstimate
					} CUs for ${ixs.length} ixs, ${users.length} users (${users
						.map((u) => u.settleeUserAccountPublicKey.toBase58())
						.join(', ')})`
				);
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

	// =============================================================================
	// UTILITY METHODS
	// =============================================================================

	private async updateWatchdogTimer() {
		await this.watchdogTimerMutex.runExclusive(async () => {
			this.watchdogTimerLastPatTime = Date.now();
		});
	}

	private handleSettlementError(err: any, methodName: string) {
		console.error(`Error in ${methodName}`, err);
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
					webhookMessage(
						`[${
							this.name
						}]: :x: Uncaught error: Error code: ${errorCode} while settling pnls:\n${
							simError.logs! ? (simError.logs as Array<string>).join('\n') : ''
						}\n${err.stack ? err.stack : err.message}`
					);
				}
			}
		}
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

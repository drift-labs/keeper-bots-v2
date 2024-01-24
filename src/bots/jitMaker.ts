/* eslint-disable @typescript-eslint/no-unused-vars */
import {
	DLOB,
	DriftEnv,
	BASE_PRECISION,
	BN,
	DriftClient,
	JupiterClient,
	getSignedTokenAmount,
	getTokenAmount,
	convertToNumber,
	promiseTimeout,
	PositionDirection,
	MarketType,
	ZERO,
	PRICE_PRECISION,
	DLOBSubscriber,
	UserMap,
	OrderType,
	SlotSubscriber,
	QUOTE_PRECISION,
	DLOBNode,
	OraclePriceData,
	SwapMode,
	getVariant,
	isVariant,
	User,
	getLimitOrderParams,
	getOrderParams,
	ONE,
	PostOnlyParams,
	TEN,
	PerpMarketAccount,
} from '@drift-labs/sdk';
import { Mutex, tryAcquire, E_ALREADY_LOCKED } from 'async-mutex';
import { logger } from '../logger';
import { Bot } from '../types';
import {
	calculateBaseAmountToMarketMakePerp,
	calculateBaseAmountToMarketMakeSpot,
	decodeName,
	getBestLimitAskExcludePubKey,
	getBestLimitBidExcludePubKey,
	isMarketVolatile,
	isSpotMarketVolatile,
	sleepMs,
} from '../utils';
import {
	JitterShotgun,
	JitterSniper,
	PriceType,
} from '@drift-labs/jit-proxy/lib';
import { assert } from '@drift-labs/sdk/lib/assert/assert';
import dotenv from 'dotenv';

dotenv.config();
import {
	ComputeBudgetProgram,
	AddressLookupTableAccount,
	VersionedTransaction,
	Connection,
	PublicKey,
	TransactionInstruction,
	Signer,
	ConfirmOptions,
	TransactionSignature,
} from '@solana/web3.js';
import { BaseBotConfig, JitMakerConfig } from '../config';

const TARGET_LEVERAGE_PER_ACCOUNT = 1;
/// jupiter slippage, which is the difference between the quoted price, and the final swap price
const JUPITER_SLIPPAGE_BPS = 10;
/// this is the slippage away from the oracle price that we're willing to tolerate.
/// i.e. we don't want to buy 50 bps above oracle, or sell 50 bps below oracle
const JUPITER_ORACLE_SLIPPAGE_BPS = 50;
const BASE_PCT_DEVIATION_BEFORE_HEDGE = 0.1;

/**
 * This is an example of a bot that implements the Bot interface.
 */
export class JitMaker implements Bot {
	public readonly name: string;
	public readonly dryRun: boolean;
	public readonly defaultIntervalMs: number = 30000;

	private driftEnv: DriftEnv;
	private periodicTaskMutex = new Mutex();

	private jitter: JitterSniper | JitterShotgun;
	private driftClient: DriftClient;

	// private subaccountConfig: SubaccountConfig;
	private subAccountIds: Array<number>;
	private marketIndexes: Array<number>;
	private marketType: MarketType;

	private intervalIds: Array<NodeJS.Timer> = [];

	private watchdogTimerMutex = new Mutex();
	private watchdogTimerLastPatTime = Date.now();

	private dlobSubscriber: DLOBSubscriber;
	private slotSubscriber: SlotSubscriber;
	private userMap: UserMap;

	constructor(
		driftClient: DriftClient, // driftClient needs to have correct number of subaccounts listed
		jitter: JitterSniper | JitterShotgun,
		userMap: UserMap,
		config: JitMakerConfig,
		driftEnv: DriftEnv
	) {
		this.subAccountIds = config.subaccounts ?? [0];
		this.marketIndexes = config.perpMarketIndicies ?? [0];
		this.marketType = config.marketType;

		const initLen = this.subAccountIds.length;
		const dedupLen = new Set(this.subAccountIds).size;
		if (initLen !== dedupLen) {
			throw new Error(
				'You CANNOT make multiple markets with the same sub account id'
			);
		}

		// Check for 1:1 unique sub account id to market index ratio
		const marketLen = this.marketIndexes.length;
		if (dedupLen !== marketLen) {
			throw new Error('You must have 1 sub account id per market to jit');
		}

		this.jitter = jitter;
		this.driftClient = driftClient;
		this.name = config.botId;
		this.dryRun = config.dryRun;
		this.driftEnv = driftEnv;
		this.userMap = userMap;

		this.slotSubscriber = new SlotSubscriber(this.driftClient.connection);
		this.dlobSubscriber = new DLOBSubscriber({
			dlobSource: this.userMap,
			slotSource: this.slotSubscriber,
			updateFrequency: 30000,
			driftClient: this.driftClient,
		});
	}

	/**
	 * Run initialization procedures for the bot.
	 */
	public async init(): Promise<void> {
		logger.info(`${this.name} initing`);

		// do stuff that takes some time
		await this.slotSubscriber.subscribe();
		await this.dlobSubscriber.subscribe();

		logger.info(`${this.name} init done`);
	}

	/**
	 * Reset the bot - usually you will reset any periodic tasks here
	 */
	public async reset(): Promise<void> {
		// reset any periodic tasks
		for (const intervalId of this.intervalIds) {
			clearInterval(intervalId as NodeJS.Timeout);
		}
		this.intervalIds = [];
	}

	public async startIntervalLoop(intervalMs?: number): Promise<void> {
		const intervalId = setInterval(
			this.runPeriodicTasks.bind(this),
			intervalMs
		);
		this.intervalIds.push(intervalId);

		logger.info(`${this.name} Bot started! driftEnv: ${this.driftEnv}`);
	}

	/**
	 * Typically used for monitoring liveness.
	 * @returns true if bot is healthy, else false.
	 */
	public async healthCheck(): Promise<boolean> {
		let healthy = false;
		await this.watchdogTimerMutex.runExclusive(async () => {
			healthy =
				this.watchdogTimerLastPatTime > Date.now() - 2 * this.defaultIntervalMs;
		});
		return healthy;
	}

	/**
	 * Typical bot loop that runs periodically and pats the watchdog timer on completion.
	 *
	 */
	private async runPeriodicTasks() {
		const start = Date.now();
		let ran = false;
		try {
			await tryAcquire(this.periodicTaskMutex).runExclusive(async () => {
				console.log(
					`[${new Date().toISOString()}] Running JIT periodic tasks...`
				);
				for (let i = 0; i < this.marketIndexes.length; i++) {
					if (this.marketType === 'PERP') {
						await this.jitPerp(i);
					} else {
						await this.jitSpot(i);
					}
				}
				await sleepMs(10000); // 10 seconds

				console.log(`done: ${Date.now() - start}ms`);
				ran = true;
			});
		} catch (e) {
			if (e === E_ALREADY_LOCKED) {
				return;
			} else {
				throw e;
			}
		} finally {
			if (ran) {
				const duration = Date.now() - start;
				logger.debug(`${this.name} Bot took ${duration}ms to run`);

				await this.watchdogTimerMutex.runExclusive(async () => {
					this.watchdogTimerLastPatTime = Date.now();
				});
			}
		}
	}

	private async jitPerp(index: number) {
		const perpIdx = this.marketIndexes[index];
		const subId = this.subAccountIds[index];
		this.driftClient.switchActiveUser(subId);

		const driftUser = this.driftClient.getUser(subId);
		const perpMarketAccount = this.driftClient.getPerpMarketAccount(perpIdx)!;
		const oraclePriceData =
			this.driftClient.getOracleDataForPerpMarket(perpIdx);

		const numMarketsForSubaccount = this.subAccountIds.filter(
			(num) => num === subId
		).length;

		const targetLeverage =
			TARGET_LEVERAGE_PER_ACCOUNT / numMarketsForSubaccount;
		const actualLeverage = driftUser.getLeverage().div(new BN(10_000));

		const maxBase: number = calculateBaseAmountToMarketMakePerp(
			perpMarketAccount,
			driftUser.getNetSpotMarketValue(),
			targetLeverage
		);

		let overleveredLong = false;
		let overleveredShort = false;

		if (actualLeverage.toNumber() >= targetLeverage * 0.95) {
			logger.warn(
				`jit maker at or above max leverage actual: ${actualLeverage} target: ${targetLeverage}`
			);
			const overleveredBaseAssetAmount =
				driftUser.getPerpPosition(perpIdx)!.baseAssetAmount;
			if (overleveredBaseAssetAmount.gt(new BN(0))) {
				overleveredLong = true;
			} else if (overleveredBaseAssetAmount.lt(new BN(0))) {
				overleveredShort = true;
			}
		}

		this.jitter.setUserFilter((userAccount, userKey) => {
			let skip = userKey == driftUser.userAccountPublicKey.toBase58();

			if (
				isMarketVolatile(
					perpMarketAccount,
					oraclePriceData,
					0.015 // 150 bps
				)
			) {
				console.log('skipping, market is volatile');
				skip = true;
			}

			if (skip) {
				console.log('skipping user:', userKey);
			}

			return skip;
		});

		const bestDriftBid = getBestLimitBidExcludePubKey(
			this.dlobSubscriber.dlob,
			perpMarketAccount.marketIndex,
			MarketType.PERP,
			oraclePriceData.slot.toNumber(),
			oraclePriceData,
			driftUser.userAccountPublicKey.toString()
		);

		const bestDriftAsk = getBestLimitAskExcludePubKey(
			this.dlobSubscriber.dlob,
			perpMarketAccount.marketIndex,
			MarketType.PERP,
			oraclePriceData.slot.toNumber(),
			oraclePriceData,
			driftUser.userAccountPublicKey.toString()
		);
		if (!bestDriftBid || !bestDriftAsk) {
			logger.warn('skipping, no best bid/ask');
			return;
		}

		const bestBidPrice = bestDriftBid.getPrice(
			oraclePriceData,
			this.dlobSubscriber.slotSource.getSlot()
		);

		const bestAskPrice = bestDriftAsk.getPrice(
			oraclePriceData,
			this.dlobSubscriber.slotSource.getSlot()
		);

		const bidOffset = bestBidPrice.sub(oraclePriceData.price);

		const askOffset = bestAskPrice.sub(oraclePriceData.price);

		let perpMinPosition = new BN(-maxBase * BASE_PRECISION.toNumber());
		let perpMaxPosition = new BN(maxBase * BASE_PRECISION.toNumber());

		if (overleveredLong) {
			perpMaxPosition = new BN(0);
		} else if (overleveredShort) {
			perpMinPosition = new BN(0);
		}

		this.jitter.updatePerpParams(perpIdx, {
			maxPosition: perpMaxPosition,
			minPosition: perpMinPosition,
			bid: bidOffset,
			ask: askOffset,
			priceType: PriceType.ORACLE,
			subAccountId: subId,
		});
	}

	private async jitSpot(index: number) {
		const spotIdx = this.marketIndexes[index];
		const subId = this.subAccountIds[index];
		this.driftClient.switchActiveUser(subId);

		const driftUser = this.driftClient.getUser(subId);
		const spotMarketAccount = this.driftClient.getSpotMarketAccount(spotIdx)!;
		const oraclePriceData =
			this.driftClient.getOracleDataForSpotMarket(spotIdx);

		const numMarketsForSubaccount = this.subAccountIds.filter(
			(num) => num === subId
		).length;

		const targetLeverage =
			TARGET_LEVERAGE_PER_ACCOUNT / numMarketsForSubaccount;
		const actualLeverage = driftUser.getLeverage().div(new BN(10_000));

		const maxBase: number = calculateBaseAmountToMarketMakeSpot(
			spotMarketAccount,
			driftUser.getNetSpotMarketValue(),
			targetLeverage
		);

		let overleveredLong = false;
		let overleveredShort = false;

		if (actualLeverage.toNumber() >= targetLeverage * 0.95) {
			logger.warn(
				`jit maker at or above max leverage actual: ${actualLeverage} target: ${targetLeverage}`
			);
			const overleveredBaseAssetAmount =
				driftUser.getSpotPosition(spotIdx)!.scaledBalance;
			if (overleveredBaseAssetAmount.gt(new BN(0))) {
				overleveredLong = true;
			} else if (overleveredBaseAssetAmount.lt(new BN(0))) {
				overleveredShort = true;
			}
		}

		this.jitter.setUserFilter((userAccount, userKey) => {
			let skip = userKey == driftUser.userAccountPublicKey.toBase58();

			if (
				isSpotMarketVolatile(
					spotMarketAccount,
					oraclePriceData,
					0.015 // 150 bps
				)
			) {
				console.log('skipping, market is volatile');
				skip = true;
			}

			if (skip) {
				console.log('skipping user:', userKey);
			}

			return skip;
		});

		const bestDriftBid = getBestLimitBidExcludePubKey(
			this.dlobSubscriber.dlob,
			spotMarketAccount.marketIndex,
			MarketType.SPOT,
			oraclePriceData.slot.toNumber(),
			oraclePriceData,
			driftUser.userAccountPublicKey.toString()
		);

		const bestDriftAsk = getBestLimitAskExcludePubKey(
			this.dlobSubscriber.dlob,
			spotMarketAccount.marketIndex,
			MarketType.SPOT,
			oraclePriceData.slot.toNumber(),
			oraclePriceData,
			driftUser.userAccountPublicKey.toString()
		);

		if (!bestDriftBid || !bestDriftAsk) {
			logger.warn('skipping, no best bid/ask');
			return;
		}

		const bestBidPrice = bestDriftBid.getPrice(
			oraclePriceData,
			this.dlobSubscriber.slotSource.getSlot()
		);

		const bestAskPrice = bestDriftAsk.getPrice(
			oraclePriceData,
			this.dlobSubscriber.slotSource.getSlot()
		);

		const bidOffset = bestBidPrice.sub(oraclePriceData.price);

		const askOffset = bestAskPrice.sub(oraclePriceData.price);

		const spotMarketPrecision = 10 ** spotMarketAccount.decimals;

		let spotMinPosition = new BN(-maxBase * spotMarketPrecision);
		let spotMaxPosition = new BN(maxBase * spotMarketPrecision);

		if (overleveredLong) {
			spotMaxPosition = new BN(0);
		} else if (overleveredShort) {
			spotMinPosition = new BN(0);
		}

		this.jitter.updateSpotParams(spotIdx, {
			maxPosition: spotMaxPosition,
			minPosition: spotMinPosition,
			bid: bidOffset,
			ask: askOffset,
			priceType: PriceType.ORACLE,
			subAccountId: subId,
		});
	}
}

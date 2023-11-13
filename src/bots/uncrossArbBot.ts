/* eslint-disable @typescript-eslint/no-non-null-assertion */
/* eslint-disable @typescript-eslint/no-unused-vars */
import {
	DriftEnv,
	DriftClient,
	convertToNumber,
	MarketType,
	PRICE_PRECISION,
	DLOBSubscriber,
	UserMap,
	SlotSubscriber,
	UserStatsMap,
	MakerInfo,
	PerpMarkets,
	getUserStatsAccountPublicKey,
} from '@drift-labs/sdk';
import { Mutex, tryAcquire, E_ALREADY_LOCKED } from 'async-mutex';
import { logger } from '../logger';
import { Bot } from '../types';
import {
	getBestLimitAskExcludePubKey,
	getBestLimitBidExcludePubKey,
	sleepMs,
} from '../utils';
import { JitProxyClient } from '@drift-labs/jit-proxy/lib';
import dotenv = require('dotenv');

dotenv.config();
import { BaseBotConfig } from 'src/config';
import {
	AddressLookupTableAccount,
	ComputeBudgetInstruction,
	ComputeBudgetProgram,
} from '@solana/web3.js';

const TARGET_LEVERAGE_PER_ACCOUNT = 1;

/**
 * This is an example of a bot that implements the Bot interface.
 */
export class UncrossArbBot implements Bot {
	public readonly name: string;
	public readonly dryRun: boolean;
	public readonly defaultIntervalMs: number = 1000;

	private driftEnv: DriftEnv;
	private periodicTaskMutex = new Mutex();

	private jitProxyClient: JitProxyClient;
	private driftClient: DriftClient;
	private lookupTableAccount?: AddressLookupTableAccount;
	private intervalIds: Array<NodeJS.Timer> = [];

	private watchdogTimerMutex = new Mutex();
	private watchdogTimerLastPatTime = Date.now();

	private dlobSubscriber: DLOBSubscriber;
	private slotSubscriber: SlotSubscriber;
	private userMap: UserMap;

	constructor(
		driftClient: DriftClient, // driftClient needs to have correct number of subaccounts listed
		jitProxyClient: JitProxyClient,
		slotSubscriber: SlotSubscriber,
		userMap: UserMap,
		config: BaseBotConfig,
		driftEnv: DriftEnv
	) {
		this.jitProxyClient = jitProxyClient;
		this.driftClient = driftClient;
		this.name = config.botId;
		this.dryRun = config.dryRun;
		this.driftEnv = driftEnv;
		this.slotSubscriber = slotSubscriber;
		this.userMap = userMap;

		this.dlobSubscriber = new DLOBSubscriber({
			dlobSource: this.userMap,
			slotSource: this.slotSubscriber,
			updateFrequency: 1000,
			driftClient: this.driftClient,
		});
	}

	/**
	 * Run initialization procedures for the bot.
	 */
	public async init(): Promise<void> {
		logger.info(`${this.name} initing`);

		await this.dlobSubscriber.subscribe();
		this.lookupTableAccount =
			await this.driftClient.fetchMarketLookupTableAccount();

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
				logger.debug(
					`[${new Date().toISOString()}] Running uncross periodic tasks...`
				);
				const perpMarkets = this.driftClient.getPerpMarketAccounts();
				for (let i = 0; i < perpMarkets.length; i++) {
					const perpIdx = perpMarkets[i].marketIndex;
					const driftUser = this.driftClient.getUser();
					const perpMarketAccount =
						this.driftClient.getPerpMarketAccount(perpIdx)!;
					const oraclePriceData =
						this.driftClient.getOracleDataForPerpMarket(perpIdx);

					const bestDriftBid = getBestLimitBidExcludePubKey(
						this.dlobSubscriber.dlob,
						perpMarketAccount.marketIndex,
						MarketType.PERP,
						oraclePriceData.slot.toNumber(),
						oraclePriceData,
						driftUser.userAccountPublicKey
					);

					const bestDriftAsk = getBestLimitAskExcludePubKey(
						this.dlobSubscriber.dlob,
						perpMarketAccount.marketIndex,
						MarketType.PERP,
						oraclePriceData.slot.toNumber(),
						oraclePriceData,
						driftUser.userAccountPublicKey
					);
					if (!bestDriftBid || !bestDriftAsk) {
						continue;
					}

					const currentSlot = this.slotSubscriber.getSlot();
					const bestBidPrice = convertToNumber(
						bestDriftBid.getPrice(oraclePriceData, currentSlot),
						PRICE_PRECISION
					);
					const bestAskPrice = convertToNumber(
						bestDriftAsk.getPrice(oraclePriceData, currentSlot),
						PRICE_PRECISION
					);

					const bidMakerInfo: MakerInfo = {
						makerUserAccount: this.userMap
							.get(bestDriftBid.userAccount!.toBase58())!
							.getUserAccount(),
						order: bestDriftBid.order,
						maker: bestDriftBid.userAccount!,
						makerStats: getUserStatsAccountPublicKey(
							this.driftClient.program.programId,
							this.userMap
								.get(bestDriftBid.userAccount!.toBase58())!
								.getUserAccount().authority
						),
					};

					const askMakerInfo: MakerInfo = {
						makerUserAccount: this.userMap
							.get(bestDriftAsk.userAccount!.toBase58())!
							.getUserAccount(),
						order: bestDriftAsk.order,
						maker: bestDriftAsk.userAccount!,
						makerStats: getUserStatsAccountPublicKey(
							this.driftClient.program.programId,
							this.userMap
								.get(bestDriftAsk.userAccount!.toBase58())!
								.getUserAccount().authority
						),
					};

					const midPrice = (bestBidPrice + bestAskPrice) / 2;
					if (
						(bestBidPrice - bestAskPrice) / midPrice >
						2 * driftUser.getMarketFees(MarketType.PERP, perpIdx).takerFee
					) {
						try {
							this.driftClient.txSender
								.sendVersionedTransaction(
									await this.driftClient.txSender.getVersionedTransaction(
										[
											ComputeBudgetProgram.setComputeUnitLimit({
												units: 1_000_000,
											}),
											await this.jitProxyClient.getArbPerpIx({
												marketIndex: perpIdx,
												makerInfos: [bidMakerInfo, askMakerInfo],
												referrerInfo: this.driftClient
													.getUserStats()
													.getReferrerInfo(),
											}),
										],
										[this.lookupTableAccount!],
										[],
										this.driftClient.opts
									),
									[],
									this.driftClient.opts
								)
								.then((txResult) => {
									logger.info(
										`Potential arb with sig: ${txResult.txSig}. Check the blockchain for confirmation.`
									);
								})
								.catch((e) => {
									if (e.logs && e.logs.length > 0) {
										let noArbOpError = false;
										for (const log of e.logs) {
											if (log.includes('NoArbOpportunity')) {
												noArbOpError = true;
												break;
											}
										}
										console.error(`Not no arb opp error:\n`);
										console.error(e);
									} else {
										console.error(`Caught unknown error:\n`);
										console.error(e);
									}
								});
						} catch (e) {
							if (e instanceof Error) {
								logger.error(
									`Error sending arb tx on market index ${perpIdx} with detected market ${bestBidPrice}@${bestAskPrice}: ${
										e.stack ? e.stack : e.message
									}`
								);
							}
						}
					}
				}

				logger.debug(`done: ${Date.now() - start}ms`);
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
}

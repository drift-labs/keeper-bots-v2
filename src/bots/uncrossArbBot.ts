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
	MakerInfo,
	getUserStatsAccountPublicKey,
	OrderSubscriber,
	PollingDriftClientAccountSubscriber,
	TxSigAndSlot,
} from '@drift-labs/sdk';
import { Mutex, tryAcquire, E_ALREADY_LOCKED } from 'async-mutex';
import { logger } from '../logger';
import { Bot } from '../types';
import {
	getBestLimitAskExcludePubKey,
	getBestLimitBidExcludePubKey,
} from '../utils';
import { JitProxyClient } from '@drift-labs/jit-proxy/lib';
import dotenv = require('dotenv');

dotenv.config();
import { BaseBotConfig } from 'src/config';
import {
	AddressLookupTableAccount,
	ComputeBudgetProgram,
	PublicKey,
} from '@solana/web3.js';
import { getErrorCode } from '../error';

const SETTLE_POSITIVE_PNL_COOLDOWN_MS = 60_000;
const SETTLE_PNL_CHUNKS = 4;
const MAX_POSITIONS_PER_USER = 8;

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
	private orderSubscriber: OrderSubscriber;

	private lastSettlePnl = Date.now() - SETTLE_POSITIVE_PNL_COOLDOWN_MS;
	private throttledNodes: Map<string, number> = new Map();

	constructor(
		driftClient: DriftClient, // driftClient needs to have correct number of subaccounts listed
		jitProxyClient: JitProxyClient,
		slotSubscriber: SlotSubscriber,
		config: BaseBotConfig,
		driftEnv: DriftEnv
	) {
		this.jitProxyClient = jitProxyClient;
		this.driftClient = driftClient;
		this.name = config.botId;
		this.dryRun = config.dryRun;
		this.driftEnv = driftEnv;
		this.slotSubscriber = slotSubscriber;

		let accountSubscription:
			| {
					type: 'polling';
					frequency: number;
					commitment: 'processed';
			  }
			| {
					commitment: 'processed';
					type: 'websocket';
					resubTimeoutMs?: number;
			  };
		if (
			(
				this.driftClient
					.accountSubscriber as PollingDriftClientAccountSubscriber
			).accountLoader
		) {
			accountSubscription = {
				type: 'polling',
				frequency: 1000,
				commitment: 'processed',
			};
		} else {
			accountSubscription = {
				commitment: 'processed',
				type: 'websocket',
				resubTimeoutMs: 30000,
			};
		}

		this.orderSubscriber = new OrderSubscriber({
			driftClient: this.driftClient,
			subscriptionConfig: accountSubscription,
		});

		this.dlobSubscriber = new DLOBSubscriber({
			dlobSource: this.orderSubscriber,
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

		await this.orderSubscriber.subscribe();
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

		this.intervalIds.push(
			setInterval(
				this.settlePnls.bind(this),
				SETTLE_POSITIVE_PNL_COOLDOWN_MS / 2
			)
		);

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
				this.watchdogTimerLastPatTime > Date.now() - 5 * this.defaultIntervalMs;
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
						[driftUser.userAccountPublicKey.toString()]
					);

					const bestDriftAsk = getBestLimitAskExcludePubKey(
						this.dlobSubscriber.dlob,
						perpMarketAccount.marketIndex,
						MarketType.PERP,
						oraclePriceData.slot.toNumber(),
						oraclePriceData,
						[driftUser.userAccountPublicKey.toString()]
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

					let bidMakerInfo: MakerInfo;
					let askMakerInfo: MakerInfo;
					try {
						bidMakerInfo = {
							makerUserAccount: this.orderSubscriber.usersAccounts.get(
								bestDriftBid.userAccount!.toBase58()
							)!.userAccount,
							order: bestDriftBid.order,
							maker: bestDriftBid.userAccount!,
							makerStats: getUserStatsAccountPublicKey(
								this.driftClient.program.programId,
								this.orderSubscriber.usersAccounts.get(
									bestDriftBid.userAccount!.toBase58()
								)!.userAccount.authority
							),
						};

						askMakerInfo = {
							makerUserAccount: this.orderSubscriber.usersAccounts.get(
								bestDriftAsk.userAccount!.toBase58()
							)!.userAccount,
							order: bestDriftAsk.order,
							maker: bestDriftAsk.userAccount!,
							makerStats: getUserStatsAccountPublicKey(
								this.driftClient.program.programId,
								this.orderSubscriber.usersAccounts.get(
									bestDriftAsk.userAccount!.toBase58()
								)!.userAccount.authority
							),
						};
					} catch (e) {
						continue;
					}

					// console.log('best ask', bestDriftAsk.userAccount!.toBase58());
					// console.log('best bid', bestDriftBid.userAccount!.toBase58());

					const midPrice = (bestBidPrice + bestAskPrice) / 2;
					if (
						(bestBidPrice - bestAskPrice) / midPrice >
						2 * driftUser.getMarketFees(MarketType.PERP, perpIdx).takerFee
					) {
						try {
							await this.driftClient.txSender
								.sendVersionedTransaction(
									await this.driftClient.txSender.getVersionedTransaction(
										[
											ComputeBudgetProgram.setComputeUnitLimit({
												units: 1_400_000,
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

	private async settlePnls() {
		const user = this.driftClient.getUser();
		const marketIds = user
			.getActivePerpPositions()
			.map((pos) => pos.marketIndex);
		const now = Date.now();
		if (marketIds.length === MAX_POSITIONS_PER_USER) {
			if (now < this.lastSettlePnl + SETTLE_POSITIVE_PNL_COOLDOWN_MS) {
				logger.info(`Want to settle positive pnl, but in cooldown...`);
			} else {
				const settlePnlPromises: Array<Promise<TxSigAndSlot>> = [];
				for (let i = 0; i < marketIds.length; i += SETTLE_PNL_CHUNKS) {
					const marketIdChunks = marketIds.slice(i, i + SETTLE_PNL_CHUNKS);
					try {
						const ixs = [
							ComputeBudgetProgram.setComputeUnitLimit({
								units: 2_000_000,
							}),
						];
						ixs.push(
							...(await this.driftClient.getSettlePNLsIxs(
								[
									{
										settleeUserAccountPublicKey: user.getUserAccountPublicKey(),
										settleeUserAccount: this.driftClient.getUserAccount()!,
									},
								],
								marketIdChunks
							))
						);
						settlePnlPromises.push(
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
						const errorCode = getErrorCode(err) ?? 0;
						logger.error(
							`Error code: ${errorCode} while settling pnls for markets ${JSON.stringify(
								marketIds
							)}: ${err.message}`
						);
						console.error(err);
					}
				}
				const txs = await Promise.all(settlePnlPromises);
				for (const tx of txs) {
					logger.info(
						`Settle positive PNLs tx: https://solscan/io/tx/${tx.txSig}`
					);
				}
				this.lastSettlePnl = now;
			}
		}
	}
}

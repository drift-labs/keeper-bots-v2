import {
	BN,
	DriftClient,
	UserAccount,
	PublicKey,
	PerpMarketConfig,
	SpotMarketConfig,
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
} from '@drift-labs/sdk';
import { Mutex } from 'async-mutex';

import { getErrorCode } from '../error';
import { logger } from '../logger';
import { Bot } from '../types';
import { webhookMessage } from '../webhook';
import { BaseBotConfig } from '../config';
import { decodeName } from '../utils';
import {
	AddressLookupTableAccount,
	ComputeBudgetProgram,
} from '@solana/web3.js';

type SettlePnlIxParams = {
	users: {
		settleeUserAccountPublicKey: PublicKey;
		settleeUserAccount: UserAccount;
	}[];
	marketIndex: number;
};

const MIN_PNL_TO_SETTLE = new BN(-10).mul(QUOTE_PRECISION);
const SETTLE_USER_CHUNKS = 2;

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
	private lookupTableAccount: AddressLookupTableAccount;
	private intervalIds: Array<NodeJS.Timer> = [];
	private userMap: UserMap;
	private perpMarkets: PerpMarketConfig[];
	private spotMarkets: SpotMarketConfig[];

	private watchdogTimerMutex = new Mutex();
	private watchdogTimerLastPatTime = Date.now();

	constructor(driftClient: DriftClient, config: BaseBotConfig) {
		this.name = config.botId;
		this.dryRun = config.dryRun;
		this.runOnce = config.runOnce || false;
		this.driftClient = driftClient;
	}

	public async init() {
		logger.info(`${this.name} initing`);

		this.lookupTableAccount =
			await this.driftClient.fetchMarketLookupTableAccount();

		// initialize userMap instance
		this.userMap = new UserMap(
			this.driftClient,
			this.driftClient.userAccountSubscriptionConfig,
			false
		);
		await this.userMap.sync();
	}

	public async reset() {
		for (const intervalId of this.intervalIds) {
			clearInterval(intervalId);
		}
		this.intervalIds = [];

		await this.userMap.unsubscribe();
	}

	public async startIntervalLoop(intervalMs: number): Promise<void> {
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

			const slot = await this.driftClient.connection.getSlot();

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

			for (const user of this.userMap.values()) {
				const userAccount = user.getUserAccount();
				const isUsdcBorrow = isVariant(
					userAccount.spotPositions[0].balanceType,
					'borrow'
				);
				for (const settleePosition of userAccount.perpPositions) {
					if (
						settleePosition.quoteAssetAmount.eq(ZERO) &&
						settleePosition.baseAssetAmount.eq(ZERO)
					) {
						continue;
					}

					const perpMarketIdx = settleePosition.marketIndex;
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

					const unsettledPnl = calculateClaimablePnl(
						perpMarketAndOracleData[perpMarketIdx].marketAccount,
						spotMarketAndOracleData[spotMarketIdx].marketAccount, // always liquidating the USDC spot market
						settleePosition,
						perpMarketAndOracleData[perpMarketIdx].oraclePriceData
					);

					// only settle for $10 or more negative pnl
					if (
						unsettledPnl.gt(MIN_PNL_TO_SETTLE) &&
						!settleePosition.baseAssetAmount.eq(ZERO) &&
						!isUsdcBorrow
					) {
						continue;
					}

					if (unsettledPnl.gt(ZERO)) {
						const pnlImbalance = calculateNetUserPnlImbalance(
							perpMarketAndOracleData[perpMarketIdx].marketAccount,
							spotMarketAndOracleData[spotMarketIdx].marketAccount,
							perpMarketAndOracleData[perpMarketIdx].oraclePriceData
						).mul(new BN(-1));

						if (pnlImbalance.lte(ZERO)) {
							logger.warn(
								`Want to settle positive PnL for user ${user
									.getUserAccountPublicKey()
									.toBase58()} in market ${perpMarketIdx}, but there is a pnl imbalance (${convertToNumber(
									pnlImbalance,
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
						usersToSettle
							.find((item) => item.marketIndex == perpMarketIdx)
							.users.push(userData);
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
					this.driftClient.getPerpMarketAccount(params.marketIndex).name
				);

				logger.info(
					`Trying to settle PNL for ${params.users.length} users on market ${marketStr}`
				);

				if (this.dryRun) {
					throw new Error('Dry run - not sending settle pnl tx');
				}

				const settlePnlPromises: Array<Promise<TxSigAndSlot>> = [];
				for (let i = 0; i < params.users.length; i += SETTLE_USER_CHUNKS) {
					const usersChunk = params.users.slice(i, i + SETTLE_USER_CHUNKS);
					try {
						const ixs = [
							ComputeBudgetProgram.setComputeUnitLimit({
								units: 1_000_000,
							}),
						];
						ixs.push(
							...(await this.driftClient.getSettlePNLsIxs(usersChunk, [
								params.marketIndex,
							]))
						);
						settlePnlPromises.push(
							this.driftClient.txSender.sendVersionedTransaction(
								await this.driftClient.txSender.getVersionedTransaction(
									ixs,
									[this.lookupTableAccount],
									[],
									this.driftClient.opts
								),
								[],
								this.driftClient.opts
							)
						);
					} catch (err) {
						const errorCode = getErrorCode(err);
						logger.error(
							`Error code: ${errorCode} while settling pnls for ${marketStr}: ${err.message}`
						);
						console.error(err);
						if (!errorCodesToSuppress.includes(errorCode)) {
							await webhookMessage(
								`[${
									this.name
								}]: :x: Error code: ${errorCode} while settling pnls for ${marketStr}:\n${
									err.logs ? (err.logs as Array<string>).join('\n') : ''
								}\n${err.stack ? err.stack : err.message}`
							);
						}
					}
				}
				const txs = await Promise.all(settlePnlPromises);
				for (const tx of txs) {
					logger.info(`Settle PNL tx: ${JSON.stringify(tx)}`);
				}
			}
		} catch (err) {
			console.error(err);
			if (
				!(err as Error).message.includes('Transaction was not confirmed') &&
				!(err as Error).message.includes('Blockhash not found')
			) {
				const errorCode = getErrorCode(err);
				if (errorCodesToSuppress.includes(errorCode)) {
					console.log(`Suppressing error code: ${errorCode}`);
				} else {
					await webhookMessage(
						`[${
							this.name
						}]: :x: Uncaught error: Error code: ${errorCode} while settling pnls:\n${
							err.logs ? (err.logs as Array<string>).join('\n') : ''
						}\n${err.stack ? err.stack : err.message}`
					);
				}
			}
		} finally {
			logger.info('Settle PNLs finished');
			await this.watchdogTimerMutex.runExclusive(async () => {
				this.watchdogTimerLastPatTime = Date.now();
			});
		}
	}
}

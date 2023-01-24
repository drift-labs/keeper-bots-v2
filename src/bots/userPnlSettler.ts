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
	NewUserRecord,
	OrderRecord,
	UserMap,
	ZERO,
	calculateNetUserPnlImbalance,
	convertToNumber,
	isOracleValid,
} from '@drift-labs/sdk';
import { Mutex } from 'async-mutex';

import { getErrorCode } from '../error';
import { logger } from '../logger';
import { Bot } from '../types';
import { webhookMessage } from '../webhook';

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
];

export class UserPnlSettlerBot implements Bot {
	public readonly name: string;
	public readonly dryRun: boolean;
	public readonly defaultIntervalMs: number = 600000;

	private driftClient: DriftClient;
	private intervalIds: Array<NodeJS.Timer> = [];
	private userMap: UserMap;
	private perpMarkets: PerpMarketConfig[];
	private spotMarkets: SpotMarketConfig[];

	private watchdogTimerMutex = new Mutex();
	private watchdogTimerLastPatTime = Date.now();

	constructor(
		name: string,
		dryRun: boolean,
		driftClient: DriftClient,
		perpMarkets: PerpMarketConfig[],
		spotMarkets: SpotMarketConfig[]
	) {
		this.name = name;
		this.dryRun = dryRun;
		this.driftClient = driftClient;
		this.perpMarkets = perpMarkets;
		this.spotMarkets = spotMarkets;
	}

	public async init() {
		logger.info(`${this.name} initing`);
		// initialize userMap instance
		this.userMap = new UserMap(
			this.driftClient,
			this.driftClient.userAccountSubscriptionConfig
		);
		await this.userMap.fetchAllUsers();
	}

	public async reset() {
		for (const intervalId of this.intervalIds) {
			clearInterval(intervalId);
		}
		this.intervalIds = [];
		for (const user of this.userMap.values()) {
			await user.unsubscribe();
		}
		delete this.userMap;
	}

	public async startIntervalLoop(_intervalMs: number): Promise<void> {
		logger.info(`${this.name} Bot started!`);
		await this.trySettlePnl();
		// we don't want to run this repeatedly
	}

	public async healthCheck(): Promise<boolean> {
		let healthy = false;
		await this.watchdogTimerMutex.runExclusive(async () => {
			healthy =
				this.watchdogTimerLastPatTime > Date.now() - 2 * this.defaultIntervalMs;
		});
		return healthy;
	}

	public async trigger(record: any): Promise<void> {
		if (record.eventType === 'OrderRecord') {
			await this.userMap.updateWithOrderRecord(record as OrderRecord);
		} else if (record.eventType === 'NewUserRecord') {
			await this.userMap.mustGet((record as NewUserRecord).user.toString());
		}
	}

	public viewDlob(): undefined {
		return undefined;
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

			this.perpMarkets.forEach((market) => {
				perpMarketAndOracleData[market.marketIndex] = {
					marketAccount: this.driftClient.getPerpMarketAccount(
						market.marketIndex
					),
					oraclePriceData: this.driftClient.getOracleDataForPerpMarket(
						market.marketIndex
					),
				};
			});
			this.spotMarkets.forEach((market) => {
				spotMarketAndOracleData[market.marketIndex] = {
					marketAccount: this.driftClient.getSpotMarketAccount(
						market.marketIndex
					),
					oraclePriceData: this.driftClient.getOracleDataForSpotMarket(
						market.marketIndex
					),
				};
			});

			const slot = await this.driftClient.connection.getSlot();

			const validOracleMarketMap = new Map<number, boolean>();
			this.perpMarkets.forEach((market) => {
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
			});

			const usersToSettle: SettlePnlIxParams[] = [];

			for (const user of this.userMap.values()) {
				const userAccount = user.getUserAccount();

				for (const settleePosition of userAccount.perpPositions) {
					if (
						settleePosition.quoteAssetAmount.eq(ZERO) &&
						settleePosition.baseAssetAmount.eq(ZERO)
					) {
						continue;
					}

					const marketIndexNum = settleePosition.marketIndex;

					const oracleValid = validOracleMarketMap.get(marketIndexNum);
					if (!oracleValid) {
						continue;
					}

					const unsettledPnl = calculateClaimablePnl(
						perpMarketAndOracleData[marketIndexNum].marketAccount,
						spotMarketAndOracleData[0].marketAccount, // always liquidating the USDC spot market
						settleePosition,
						perpMarketAndOracleData[marketIndexNum].oraclePriceData
					);

					// only settle for $10 or more negative pnl
					if (
						unsettledPnl.gt(MIN_PNL_TO_SETTLE) &&
						!settleePosition.baseAssetAmount.eq(ZERO)
					) {
						continue;
					}

					if (unsettledPnl.gt(ZERO)) {
						const pnlImbalance = calculateNetUserPnlImbalance(
							perpMarketAndOracleData[marketIndexNum].marketAccount,
							spotMarketAndOracleData[0].marketAccount,
							perpMarketAndOracleData[marketIndexNum].oraclePriceData
						).mul(new BN(-1));

						if (pnlImbalance.lte(ZERO)) {
							logger.warn(
								`Want to settle positive PnL for user ${user
									.getUserAccountPublicKey()
									.toBase58()} in market ${marketIndexNum}, but there is a pnl imbalance (${convertToNumber(
									pnlImbalance,
									QUOTE_PRECISION
								)})`
							);
							continue;
						}
					}

					// only settle user pnl if they have enough collateral
					if (
						user.getTotalCollateral().lt(user.getMaintenanceMarginRequirement())
					) {
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
							.includes(marketIndexNum)
					) {
						usersToSettle
							.find((item) => item.marketIndex == marketIndexNum)
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
				const marketStr = this.perpMarkets.find(
					(mkt) => mkt.marketIndex === params.marketIndex
				).symbol;

				logger.info(
					`Trying to settle PNL for ${params.users.length} users on market ${marketStr}`
				);

				if (this.dryRun) {
					throw new Error('Dry run - not sending settle pnl tx');
				}

				const settlePnlPromises = new Array<Promise<string>>();
				for (let i = 0; i < params.users.length; i += SETTLE_USER_CHUNKS) {
					const usersChunk = params.users.slice(i, i + SETTLE_USER_CHUNKS);
					try {
						settlePnlPromises.push(
							this.driftClient.settlePNLs(usersChunk, params.marketIndex)
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
					logger.info(`Settle PNL tx: ${tx}`);
				}
			}
		} catch (e) {
			console.error(e);
			await webhookMessage(
				`[${this.name}]: :x: uncaught error:\n${e.stack ? e.stack : e.messaage}`
			);
		} finally {
			logger.info('Settle PNLs finished');
			await this.watchdogTimerMutex.runExclusive(async () => {
				this.watchdogTimerLastPatTime = Date.now();
			});
		}
	}
}

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
} from '@drift-labs/sdk';
import { Mutex } from 'async-mutex';

import { getErrorCode } from '../error';
import { logger } from '../logger';
import { Bot } from '../types';
import { Metrics } from '../metrics';

type SettlePnlIxParams = {
	users: {
		settleeUserAccountPublicKey: PublicKey;
		settleeUserAccount: UserAccount;
	}[];
	marketIndex: number;
};

const MIN_PNL_TO_SETTLE = new BN(-10).mul(QUOTE_PRECISION);
const SETTLE_USER_CHUNKS = 2;

export class PnlSettlerBot implements Bot {
	public readonly name: string;
	public readonly dryRun: boolean;
	public readonly defaultIntervalMs: number = 600000;

	private driftClient: DriftClient;
	private intervalIds: Array<NodeJS.Timer> = [];
	private userMap: UserMap;
	private perpMarkets: PerpMarketConfig[];
	private spotMarkets: SpotMarketConfig[];
	private metrics: Metrics | undefined;

	private watchdogTimerMutex = new Mutex();
	private watchdogTimerLastPatTime = Date.now();

	constructor(
		name: string,
		dryRun: boolean,
		driftClient: DriftClient,
		perpMarkets: PerpMarketConfig[],
		spotMarkets: SpotMarketConfig[],
		metrics?: Metrics | undefined
	) {
		this.name = name;
		this.dryRun = dryRun;
		this.driftClient = driftClient;
		this.perpMarkets = perpMarkets;
		this.spotMarkets = spotMarkets;
		this.metrics = metrics;
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
		delete this.userMap;
	}

	public async startIntervalLoop(intervalMs: number): Promise<void> {
		logger.info(`${this.name} Bot started!`);
		this.trySettlePnl();
		const intervalId = setInterval(this.trySettlePnl.bind(this), intervalMs);
		this.intervalIds.push(intervalId);
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

			const usersToSettle: SettlePnlIxParams[] = [];

			for (const user of this.userMap.values()) {
				const userAccount = user.getUserAccount();

				for (const settleePosition of userAccount.perpPositions) {
					const marketIndexNum = settleePosition.marketIndex;
					const unsettledPnl = calculateClaimablePnl(
						perpMarketAndOracleData[marketIndexNum].marketAccount,
						spotMarketAndOracleData[marketIndexNum].marketAccount,
						settleePosition,
						perpMarketAndOracleData[marketIndexNum].oraclePriceData
					);

					// only settle for $10 or more negative pnl
					if (unsettledPnl.gt(MIN_PNL_TO_SETTLE)) {
						continue;
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

			usersToSettle.forEach((params) => {
				const marketStr = this.perpMarkets.find(
					(mkt) => mkt.marketIndex === params.marketIndex
				).symbol;

				logger.info(
					`Trying to settle PNL for ${params.users.length} users on market ${marketStr}`
				);

				if (this.dryRun) {
					throw new Error('Dry run - not sending settle pnl tx');
				}

				for (let i = 0; i < params.users.length; i += SETTLE_USER_CHUNKS) {
					const usersChunk = params.users.slice(i, i + SETTLE_USER_CHUNKS);
					this.driftClient
						.settlePNLs(usersChunk, params.marketIndex)
						.then((txSig) => {
							logger.info(
								`PNL settled successfully on ${marketStr}. TxSig: ${txSig}`
							);
							this.metrics?.recordSettlePnl(
								usersChunk.length,
								params.marketIndex,
								this.name
							);
						})
						.catch((err) => {
							const errorCode = getErrorCode(err);
							this.metrics?.recordErrorCode(
								errorCode,
								this.driftClient.provider.wallet.publicKey,
								this.name
							);
							logger.error(
								`Error code: ${errorCode} while settling pnls for ${marketStr}: ${err.message}`
							);
						});
				}
			});
		} catch (e) {
			console.error(e);
		} finally {
			logger.info('Settle PNLs finished');
			await this.watchdogTimerMutex.runExclusive(async () => {
				this.watchdogTimerLastPatTime = Date.now();
			});
		}
	}
}

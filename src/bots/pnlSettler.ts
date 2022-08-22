import {
	BN,
	ClearingHouse,
	UserAccount,
	PublicKey,
	MarketConfig,
	MarketAccount,
	OraclePriceData,
	calculateUnsettledPnl,
	QUOTE_PRECISION,
} from '@drift-labs/sdk';
import { getErrorCode } from '../error';
import { logger } from '../logger';
import { UserMap } from '../userMap';
import { Bot } from '../types';
import { Metrics } from '../metrics';

type SettlePnlIxParams = {
	users: {
		settleeUserAccountPublicKey: PublicKey;
		settleeUserAccount: UserAccount;
	}[];
	marketIndex: BN;
};

const MIN_PNL_TO_SETTLE = new BN(-10).mul(QUOTE_PRECISION);

export class PnlSettlerBot implements Bot {
	public readonly name: string;
	public readonly dryRun: boolean;
	public readonly defaultIntervalMs: number = 60000;

	private clearingHouse: ClearingHouse;
	private intervalIds: Array<NodeJS.Timer> = [];
	private userMap: UserMap;
	private markets: MarketConfig[];
	private metrics: Metrics | undefined;

	constructor(
		name: string,
		dryRun: boolean,
		clearingHouse: ClearingHouse,
		markets: MarketConfig[],
		metrics?: Metrics | undefined
	) {
		this.name = name;
		this.dryRun = dryRun;
		this.clearingHouse = clearingHouse;
		this.markets = markets;
		this.metrics = metrics;
	}

	public async init() {
		// initialize userMap instance
		this.userMap = new UserMap(
			this.clearingHouse,
			this.clearingHouse.userAccountSubscriptionConfig
		);
		await this.userMap.fetchAllUsers();
	}

	public reset(): void {
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

	public async trigger(_record: any): Promise<void> {
		return undefined;
	}

	public viewDlob(): undefined {
		return undefined;
	}

	private async trySettlePnl() {
		try {
			const marketAndOracleData: {
				[marketIndex: number]: {
					marketAccount: MarketAccount;
					oraclePriceData: OraclePriceData;
				};
			} = {};

			this.markets.forEach((market) => {
				marketAndOracleData[market.marketIndex.toNumber()] = {
					marketAccount: this.clearingHouse.getMarketAccount(
						market.marketIndex
					),
					oraclePriceData: this.clearingHouse.getOracleDataForMarket(
						market.marketIndex
					),
				};
			});

			const usersToSettle: SettlePnlIxParams[] = [];

			for (const user of this.userMap.values()) {
				const userAccount = user.getUserAccount();

				this.clearingHouse.fetchAccounts();
				this.clearingHouse.getUser().fetchAccounts();

				for (const settleePosition of userAccount.positions) {
					const marketIndexNum = settleePosition.marketIndex.toNumber();
					const unsettledPnl = calculateUnsettledPnl(
						marketAndOracleData[marketIndexNum].marketAccount,
						settleePosition,
						marketAndOracleData[marketIndexNum].oraclePriceData
					);
					// only settle for $10 or more negative pnl
					if (unsettledPnl.lte(MIN_PNL_TO_SETTLE)) {
						const userData = {
							settleeUserAccountPublicKey: user.getUserAccountPublicKey(),
							settleeUserAccount: userAccount,
						};
						if (
							usersToSettle
								.map((item) => item.marketIndex.toNumber())
								.includes(marketIndexNum)
						) {
							usersToSettle
								.find((item) => item.marketIndex.toNumber() == marketIndexNum)
								.users.push(userData);
						} else {
							usersToSettle.push({
								users: [userData],
								marketIndex: settleePosition.marketIndex,
							});
						}
					}
				}
			}

			usersToSettle.forEach((params) => {
				const marketStr = this.markets.find((mkt) =>
					mkt.marketIndex.eq(params.marketIndex)
				).symbol;

				logger.info(
					`Trying to settle PNL for ${params.users.length} users on market ${marketStr}`
				);
				this.clearingHouse
					.settlePNLs(params.users, params.marketIndex)
					.then((txSig) => {
						logger.info(
							`PNL settled successfully on ${marketStr}. TxSig: ${txSig}`
						);
						params.users.forEach((settledUser) => {
							this.metrics?.recordSettlePnl(
								settledUser.settleeUserAccountPublicKey,
								this.name
							);
						});
					})
					.catch((err) => {
						logger.error(
							`Error code ${getErrorCode(
								err
							)} while settling pnls for ${marketStr}: ${err.message}`
						);
					});
			});
		} catch (e) {
			console.error(e);
		} finally {
			logger.info('Settle PNLs finished');
		}
	}
}

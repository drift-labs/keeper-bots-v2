import {
	BN,
	DriftClient,
	SpotMarketConfig,
	SpotMarketAccount,
	OraclePriceData,
	ZERO,
} from '@drift-labs/sdk';
import { Mutex } from 'async-mutex';

import { getErrorCode } from '../error';
import { logger } from '../logger';
import { Bot } from '../types';
import { webhookMessage } from '../webhook';

export class IFRevenueSettlerBot implements Bot {
	public readonly name: string;
	public readonly dryRun: boolean;
	public readonly defaultIntervalMs: number = 600000;

	private driftClient: DriftClient;
	private intervalIds: Array<NodeJS.Timer> = [];
	private spotMarkets: SpotMarketConfig[];

	private watchdogTimerMutex = new Mutex();
	private watchdogTimerLastPatTime = Date.now();

	constructor(
		name: string,
		dryRun: boolean,
		clearingHouse: DriftClient,
		spotMarkets: SpotMarketConfig[]
	) {
		this.name = name;
		this.dryRun = dryRun;
		this.driftClient = clearingHouse;
		this.spotMarkets = spotMarkets;
	}

	public async init() {
		logger.info(`${this.name} initing`);
	}

	public async reset() {
		for (const intervalId of this.intervalIds) {
			clearInterval(intervalId);
		}
		this.intervalIds = [];
	}

	public async startIntervalLoop(_intervalMs: number): Promise<void> {
		logger.info(`${this.name} Bot started!`);
		await this.trySettleIFRevenue();
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

	public async trigger(_record: any): Promise<void> {}

	public viewDlob(): undefined {
		return undefined;
	}

	private async trySettleIFRevenue() {
		try {
			const spotMarketAndOracleData: {
				[marketIndex: number]: {
					marketAccount: SpotMarketAccount;
					oraclePriceData: OraclePriceData;
				};
			} = {};

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

			for (let i = 0; i < this.spotMarkets.length; i++) {
				const spotIf = spotMarketAndOracleData[i].marketAccount.insuranceFund;
				if (spotIf.revenueSettlePeriod.eq(ZERO)) {
					continue;
				}
				const currentTs = Date.now() / 1000;
				if (
					spotIf.lastRevenueSettleTs
						.add(spotIf.revenueSettlePeriod)
						.lte(new BN(currentTs))
				) {
					logger.info(
						spotIf.lastRevenueSettleTs.toString() +
							' and ' +
							spotIf.revenueSettlePeriod.toString()
					);
					logger.info(
						spotIf.lastRevenueSettleTs
							.add(spotIf.revenueSettlePeriod)
							.toString() +
							' < ' +
							currentTs.toString()
					);

					try {
						const txSig = await this.driftClient.settleRevenueToInsuranceFund(
							i
						);
						logger.info(
							`IF revenue settled successfully on marketIndex=${i}. TxSig: ${txSig}`
						);
					} catch (err) {
						const errorCode = getErrorCode(err);
						logger.error(
							`Error code: ${errorCode} while settling revenue to IF for marketIndex=${i}: ${err.message}`
						);
						await webhookMessage(
							`[${
								this.name
							}]: :x: Error code: ${errorCode} while settling revenue to IF for marketIndex=${i}:\n${
								err.stack ? err.stack : err.message
							}`
						);
					}
				}
			}
		} catch (e) {
			console.error(e);
			await webhookMessage(
				`[${this.name}]: :x: uncaught error:\n${e.stack ? e.stack : e.message}`
			);
		} finally {
			logger.info('Settle IF Revenues finished');
			await this.watchdogTimerMutex.runExclusive(async () => {
				this.watchdogTimerLastPatTime = Date.now();
			});
		}
	}
}

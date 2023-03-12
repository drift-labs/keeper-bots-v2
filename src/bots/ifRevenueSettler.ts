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
import { BaseBotConfig } from '../config';

export class IFRevenueSettlerBot implements Bot {
	public readonly name: string;
	public readonly dryRun: boolean;
	public readonly runOnce: boolean;
	public readonly defaultIntervalMs: number = 600000;

	private driftClient: DriftClient;
	private intervalIds: Array<NodeJS.Timer> = [];
	private spotMarkets: SpotMarketConfig[];

	private watchdogTimerMutex = new Mutex();
	private watchdogTimerLastPatTime = Date.now();

	constructor(
		driftClient: DriftClient,
		spotMarkets: SpotMarketConfig[],
		config: BaseBotConfig
	) {
		this.name = config.botId;
		this.dryRun = config.dryRun;
		this.runOnce = config.runOnce || false;
		this.driftClient = driftClient;
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

	public async startIntervalLoop(intervalMs: number): Promise<void> {
		logger.info(`${this.name} Bot started!`);
		if (this.runOnce) {
			await this.trySettleIFRevenue();
		} else {
			const intervalId = setInterval(
				this.trySettleIFRevenue.bind(this),
				intervalMs
			);
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
						console.error(err);
						await webhookMessage(
							`[${
								this.name
							}]: :x: Error code: ${errorCode} while settling revenue to IF for marketIndex=${i}:\n${
								err.logs ? (err.logs as Array<string>).join('\n') : ''
							}\n${err.stack ? err.stack : err.message}`
						);
					}
				}
			}
		} catch (err) {
			console.error(err);
			if (
				!(err as Error).message.includes('Transaction was not confirmed') &&
				!(err as Error).message.includes('Blockhash not found')
			) {
				const errorCode = getErrorCode(err);
				await webhookMessage(
					`[${
						this.name
					}]: :x: IF Revenue Settler error: Error code: ${errorCode}:\n${
						err.logs ? (err.logs as Array<string>).join('\n') : ''
					}\n${err.stack ? err.stack : err.message}`
				);
			}
		} finally {
			logger.info('Settle IF Revenues finished');
			await this.watchdogTimerMutex.runExclusive(async () => {
				this.watchdogTimerLastPatTime = Date.now();
			});
		}
	}
}

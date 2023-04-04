import {
	DriftClient,
	PerpMarketConfig,
	ZERO,
	PerpMarketAccount,
} from '@drift-labs/sdk';
import { Mutex } from 'async-mutex';

import { getErrorCode } from '../error';
import { logger } from '../logger';
import { Bot } from '../types';
import { webhookMessage } from '../webhook';
import { BaseBotConfig } from '../config';

function onTheHourUpdate(
	now: number,
	lastUpdateTs: number,
	updatePeriod: number
): number | Error {
	const timeSinceLastUpdate = now - lastUpdateTs;

	if (timeSinceLastUpdate < 0) {
		return new Error('Invalid arguments');
	}

	let nextUpdateWait = updatePeriod;
	if (updatePeriod > 1) {
		const lastUpdateDelay = lastUpdateTs % updatePeriod;
		if (lastUpdateDelay !== 0) {
			const maxDelayForNextPeriod = updatePeriod / 3;
			const twoFundingPeriods = updatePeriod * 2;

			if (lastUpdateDelay > maxDelayForNextPeriod) {
				// too late for on the hour next period, delay to following period
				nextUpdateWait = twoFundingPeriods - lastUpdateDelay;
			} else {
				// allow update on the hour
				nextUpdateWait = updatePeriod - lastUpdateDelay;
			}

			if (nextUpdateWait > twoFundingPeriods) {
				nextUpdateWait -= updatePeriod;
			}
		}
	}

	return Math.max(nextUpdateWait - timeSinceLastUpdate, 0);
}

export class FundingRateUpdaterBot implements Bot {
	public readonly name: string;
	public readonly dryRun: boolean;
	public readonly defaultIntervalMs: number = 600000;

	private driftClient: DriftClient;
	private intervalIds: Array<NodeJS.Timer> = [];
	private perpMarkets: PerpMarketConfig[];

	private watchdogTimerMutex = new Mutex();
	private watchdogTimerLastPatTime = Date.now();

	constructor(
		driftClient: DriftClient,
		perpMarkets: PerpMarketConfig[],
		config: BaseBotConfig
	) {
		this.name = config.botId;
		this.dryRun = config.dryRun;
		this.driftClient = driftClient;
		this.perpMarkets = perpMarkets;
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
		await this.tryUpdateFundingRate();
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

	private async tryUpdateFundingRate() {
		try {
			const perpMarketAndOracleData: {
				[marketIndex: number]: {
					marketAccount: PerpMarketAccount;
				};
			} = {};

			this.perpMarkets.forEach((market) => {
				perpMarketAndOracleData[market.marketIndex] = {
					marketAccount: this.driftClient.getPerpMarketAccount(
						market.marketIndex
					),
				};
			});

			for (let i = 0; i < this.perpMarkets.length; i++) {
				const perpMarket = perpMarketAndOracleData[i].marketAccount;
				if (perpMarket.amm.fundingPeriod.eq(ZERO)) {
					continue;
				}
				const currentTs = Date.now() / 1000;

				logger.info(`Checking market: ${i}`);
				const timeRemainingTilUpdate = onTheHourUpdate(
					currentTs,
					perpMarket.amm.lastFundingRateTs.toNumber(),
					perpMarket.amm.fundingPeriod.toNumber()
				);
				logger.info(` timeRemainingTilUpdate=${timeRemainingTilUpdate}`);
				if ((timeRemainingTilUpdate as number) <= 0) {
					logger.info(
						perpMarket.amm.lastFundingRateTs.toString() +
							' and ' +
							perpMarket.amm.fundingPeriod.toString()
					);
					logger.info(
						perpMarket.amm.lastFundingRateTs
							.add(perpMarket.amm.fundingPeriod)
							.toString() +
							' vs ' +
							currentTs.toString()
					);
					logger.info(`timeRemainingTilUpdate=${timeRemainingTilUpdate}`);

					try {
						const txSig = await this.driftClient.updateFundingRate(
							i,
							perpMarket.amm.oracle
						);
						logger.info(
							`funding rate updated successfully on perp marketIndex=${i}. TxSig: ${txSig}`
						);
					} catch (err) {
						const errorCode = getErrorCode(err);
						logger.error(
							`Error code: ${errorCode} while updating funding rates on perp marketIndex=${i}: ${err.message}`
						);
						console.error(err);
						await webhookMessage(
							`[${
								this.name
							}]: :x: Error code: ${errorCode} while updating funding rates on perp marketIndex=${i}:\n${
								err.logs ? (err.logs as Array<string>).join('\n') : ''
							}\n${err.stack ? err.stack : err.message}`
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
			logger.info('Update Funding Rates finished');
			await this.watchdogTimerMutex.runExclusive(async () => {
				this.watchdogTimerLastPatTime = Date.now();
			});
		}
	}
}

import {
	ClearingHouse,
	PerpMarketAccount,
	SpotMarketAccount,
	OrderRecord,
	SlotSubscriber,
	NewUserRecord,
	DLOB,
	NodeToTrigger,
	UserMap,
	MarketType,
} from '@drift-labs/sdk';
import { Mutex, tryAcquire, withTimeout, E_ALREADY_LOCKED } from 'async-mutex';

import { logger } from '../logger';
import { Bot } from '../types';
import { getErrorCode } from '../error';
import { Metrics } from '../metrics';

const dlobMutexError = new Error('dlobMutex timeout');

export class TriggerBot implements Bot {
	public readonly name: string;
	public readonly dryRun: boolean;
	public readonly defaultIntervalMs: number = 1000;

	private clearingHouse: ClearingHouse;
	private slotSubscriber: SlotSubscriber;
	private dlobMutex = withTimeout(
		new Mutex(),
		10 * this.defaultIntervalMs,
		dlobMutexError
	);
	private dlob: DLOB;
	private periodicTaskMutex = new Mutex();
	private intervalIds: Array<NodeJS.Timer> = [];
	private userMap: UserMap;
	private metrics: Metrics | undefined;

	private watchdogTimerMutex = new Mutex();
	private watchdogTimerLastPatTime = Date.now();

	constructor(
		name: string,
		dryRun: boolean,
		clearingHouse: ClearingHouse,
		slotSubscriber: SlotSubscriber,
		metrics?: Metrics | undefined
	) {
		this.name = name;
		this.dryRun = dryRun;
		this.clearingHouse = clearingHouse;
		this.slotSubscriber = slotSubscriber;
		this.metrics = metrics;
	}

	public async init() {
		logger.info(`${this.name} initing`);
		// initialize userMap instance
		this.userMap = new UserMap(
			this.clearingHouse,
			this.clearingHouse.userAccountSubscriptionConfig
		);
		await this.userMap.fetchAllUsers();
	}

	public async reset() {}

	public async startIntervalLoop(intervalMs: number): Promise<void> {
		this.tryTrigger();
		const intervalId = setInterval(this.tryTrigger.bind(this), intervalMs);
		this.intervalIds.push(intervalId);

		logger.info(`${this.name} Bot started!`);
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
			this.tryTrigger();
		} else if (record.eventType === 'NewUserRecord') {
			await this.userMap.mustGet((record as NewUserRecord).user.toString());
		}
	}

	public viewDlob(): DLOB {
		return this.dlob;
	}

	private async tryTriggerForPerpMarket(market: PerpMarketAccount) {
		const marketIndex = market.marketIndex;

		try {
			const oraclePriceData =
				this.clearingHouse.getOracleDataForMarket(marketIndex);

			let nodesToTrigger: Array<NodeToTrigger> = [];
			await this.dlobMutex.runExclusive(async () => {
				nodesToTrigger = this.dlob.findNodesToTrigger(
					marketIndex,
					this.slotSubscriber.getSlot(),
					oraclePriceData.price,
					MarketType.PERP
				);
			});

			for (const nodeToTrigger of nodesToTrigger) {
				if (nodeToTrigger.node.haveTrigger) {
					continue;
				}

				nodeToTrigger.node.haveTrigger = true;

				logger.info(
					`trying to trigger perp order (account: ${nodeToTrigger.node.userAccount.toString()}) perp order ${nodeToTrigger.node.order.orderId.toString()}`
				);

				const user = await this.userMap.mustGet(
					nodeToTrigger.node.userAccount.toString()
				);
				this.clearingHouse
					.triggerOrder(
						nodeToTrigger.node.userAccount,
						user.getUserAccount(),
						nodeToTrigger.node.order
					)
					.then((txSig) => {
						logger.info(
							`Triggered perp user (account: ${nodeToTrigger.node.userAccount.toString()}) perp order: ${nodeToTrigger.node.order.orderId.toString()}`
						);
						logger.info(`Tx: ${txSig}`);
					})
					.catch((error) => {
						const errorCode = getErrorCode(error);
						this?.metrics.recordErrorCode(
							errorCode,
							this.clearingHouse.provider.wallet.publicKey,
							this.name
						);

						nodeToTrigger.node.haveTrigger = false;
						logger.error(
							`Error (${errorCode}) triggering perp user (account: ${nodeToTrigger.node.userAccount.toString()}) perp order: ${nodeToTrigger.node.order.orderId.toString()}`
						);
						logger.error(error);
					});
			}
		} catch (e) {
			logger.error(
				`Unexpected error for market ${marketIndex.toString()} during triggers`
			);
			console.error(e);
		}
	}

	private async tryTriggerForSpotMarket(market: SpotMarketAccount) {
		const marketIndex = market.marketIndex;

		try {
			const oraclePriceData =
				this.clearingHouse.getOracleDataForMarket(marketIndex);

			let nodesToTrigger: Array<NodeToTrigger> = [];
			await this.dlobMutex.runExclusive(async () => {
				nodesToTrigger = this.dlob.findNodesToTrigger(
					marketIndex,
					this.slotSubscriber.getSlot(),
					oraclePriceData.price,
					MarketType.SPOT
				);
			});

			for (const nodeToTrigger of nodesToTrigger) {
				if (nodeToTrigger.node.haveTrigger) {
					continue;
				}

				nodeToTrigger.node.haveTrigger = true;

				logger.info(
					`trying to trigger (account: ${nodeToTrigger.node.userAccount.toString()}) spot order ${nodeToTrigger.node.order.orderId.toString()}`
				);

				const user = await this.userMap.mustGet(
					nodeToTrigger.node.userAccount.toString()
				);
				this.clearingHouse
					.triggerOrder(
						nodeToTrigger.node.userAccount,
						user.getUserAccount(),
						nodeToTrigger.node.order
					)
					.then((txSig) => {
						logger.info(
							`Triggered user (account: ${nodeToTrigger.node.userAccount.toString()}) spot order: ${nodeToTrigger.node.order.orderId.toString()}`
						);
						logger.info(`Tx: ${txSig}`);
					})
					.catch((error) => {
						const errorCode = getErrorCode(error);
						this?.metrics.recordErrorCode(
							errorCode,
							this.clearingHouse.provider.wallet.publicKey,
							this.name
						);

						nodeToTrigger.node.haveTrigger = false;
						logger.error(
							`Error (${errorCode}) triggering spot order for user (account: ${nodeToTrigger.node.userAccount.toString()}) spot order: ${nodeToTrigger.node.order.orderId.toString()}`
						);
						logger.error(error);
					});
			}
		} catch (e) {
			logger.error(
				`Unexpected error for spot market ${marketIndex.toString()} during triggers`
			);
			console.error(e);
		}
	}

	private async tryTrigger() {
		const start = Date.now();
		let ran = false;
		try {
			await tryAcquire(this.periodicTaskMutex).runExclusive(async () => {
				await this.dlobMutex.runExclusive(async () => {
					if (this.dlob) {
						this.dlob.clear();
						delete this.dlob;
					}
					this.dlob = new DLOB(
						this.clearingHouse.getPerpMarketAccounts(),
						this.clearingHouse.getSpotMarketAccounts(),
						true
					);
					this.metrics?.trackObjectSize('filler-dlob', this.dlob);
					await this.dlob.init(this.clearingHouse, this.userMap);
				});

				await Promise.all([
					this.clearingHouse.getPerpMarketAccounts().map((marketAccount) => {
						this.tryTriggerForPerpMarket(marketAccount);
					}),
					this.clearingHouse.getSpotMarketAccounts().map((marketAccount) => {
						this.tryTriggerForSpotMarket(marketAccount);
					}),
				]);
				ran = true;
			});
		} catch (e) {
			if (e === E_ALREADY_LOCKED) {
				this.metrics?.recordMutexBusy(this.name);
			} else if (e === dlobMutexError) {
				logger.error(`${this.name} dlobMutexError timeout`);
			} else {
				throw e;
			}
		} finally {
			if (ran) {
				const duration = Date.now() - start;
				this.metrics?.recordRpcDuration(
					this.clearingHouse.connection.rpcEndpoint,
					'tryTrigger',
					duration,
					false,
					this.name
				);
				logger.debug(`${this.name} Bot took ${Date.now() - start}ms to run`);
				await this.watchdogTimerMutex.runExclusive(async () => {
					this.watchdogTimerLastPatTime = Date.now();
				});
			}
		}
	}
}

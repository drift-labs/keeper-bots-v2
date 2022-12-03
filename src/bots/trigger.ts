import {
	DriftClient,
	PerpMarketAccount,
	SpotMarketAccount,
	OrderRecord,
	SlotSubscriber,
	NewUserRecord,
	DLOB,
	NodeToTrigger,
	UserMap,
	MarketType,
	BulkAccountLoader,
} from '@drift-labs/sdk';
import { Mutex, tryAcquire, withTimeout, E_ALREADY_LOCKED } from 'async-mutex';

import { logger } from '../logger';
import { Bot } from '../types';
import { getErrorCode } from '../error';
import { Metrics } from '../metrics';
import { webhookMessage } from '../webhook';

const dlobMutexError = new Error('dlobMutex timeout');
const USER_MAP_RESYNC_COOLDOWN_SLOTS = 200;

export class TriggerBot implements Bot {
	public readonly name: string;
	public readonly dryRun: boolean;
	public readonly defaultIntervalMs: number = 1000;

	private bulkAccountLoader: BulkAccountLoader | undefined;
	private driftClient: DriftClient;
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

	private lastSlotReyncUserMapsMutex = new Mutex();
	private lastSlotResyncUserMaps = 0;

	constructor(
		name: string,
		dryRun: boolean,
		bulkAccountLoader: BulkAccountLoader | undefined,
		driftClient: DriftClient,
		slotSubscriber: SlotSubscriber,
		metrics?: Metrics | undefined
	) {
		this.name = name;
		this.dryRun = dryRun;
		(this.bulkAccountLoader = bulkAccountLoader),
			(this.driftClient = driftClient);
		this.slotSubscriber = slotSubscriber;
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
		await this.userMap.updateWithEventRecord(record);
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

	/**
	 * Checks that userMap and userStatsMap are up in sync with , if not, signal that we should update them next block.
	 */
	private async resyncUserMapsIfRequired() {
		const stateAccount = this.driftClient.getStateAccount();
		const resyncRequired =
			this.userMap.size() !== stateAccount.numberOfSubAccounts.toNumber();

		if (resyncRequired) {
			await this.lastSlotReyncUserMapsMutex.runExclusive(async () => {
				let doResync = false;
				const start = Date.now();
				if (!this.bulkAccountLoader) {
					logger.info(`Resyncing UserMaps immediately (no BulkAccountLoader)`);
					doResync = true;
				} else {
					const nextResyncSlot =
						this.lastSlotResyncUserMaps + USER_MAP_RESYNC_COOLDOWN_SLOTS;
					if (nextResyncSlot >= this.bulkAccountLoader.mostRecentSlot) {
						const slotsRemaining =
							nextResyncSlot - this.bulkAccountLoader.mostRecentSlot;
						if (slotsRemaining % 10 === 0) {
							logger.info(
								`Resyncing UserMaps in cooldown, ${slotsRemaining} more slots to go`
							);
						}
						return;
					} else {
						logger.info(`Resyncing UserMaps`);
						doResync = true;
						this.lastSlotResyncUserMaps = this.bulkAccountLoader.mostRecentSlot;
					}
				}

				if (doResync) {
					const newUserMap = new UserMap(
						this.driftClient,
						this.driftClient.userAccountSubscriptionConfig
					);
					newUserMap
						.fetchAllUsers()
						.then(() => {
							delete this.userMap;
							this.userMap = newUserMap;
						})
						.finally(() => {
							logger.info(`UserMaps resynced in ${Date.now() - start}ms`);
						});
				}
			});
		}
	}

	private async tryTriggerForPerpMarket(market: PerpMarketAccount) {
		const marketIndex = market.marketIndex;

		try {
			const oraclePriceData =
				this.driftClient.getOracleDataForPerpMarket(marketIndex);

			let nodesToTrigger: Array<NodeToTrigger> = [];
			await this.dlobMutex.runExclusive(async () => {
				nodesToTrigger = this.dlob.findNodesToTrigger(
					marketIndex,
					this.slotSubscriber.getSlot(),
					oraclePriceData.price,
					MarketType.PERP,
					this.driftClient.getStateAccount()
				);
			});

			for (const nodeToTrigger of nodesToTrigger) {
				if (nodeToTrigger.node.haveTrigger) {
					continue;
				}

				nodeToTrigger.node.haveTrigger = true;

				logger.info(
					`trying to trigger perp order on market ${
						nodeToTrigger.node.order.marketIndex
					} (account: ${nodeToTrigger.node.userAccount.toString()}) perp order ${nodeToTrigger.node.order.orderId.toString()}`
				);

				const user = await this.userMap.mustGet(
					nodeToTrigger.node.userAccount.toString()
				);
				this.driftClient
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
						webhookMessage(
							`[${
								this.name
							}]: :gear: Triggered perp user (account: ${nodeToTrigger.node.userAccount.toString()}) perp order: ${nodeToTrigger.node.order.orderId.toString()}, tx: ${txSig}`
						);
					})
					.catch((error) => {
						const errorCode = getErrorCode(error);
						this?.metrics.recordErrorCode(
							errorCode,
							this.driftClient.provider.wallet.publicKey,
							this.name
						);

						nodeToTrigger.node.haveTrigger = false;
						logger.error(
							`Error (${errorCode}) triggering perp user (account: ${nodeToTrigger.node.userAccount.toString()}) perp order: ${nodeToTrigger.node.order.orderId.toString()}`
						);
						logger.error(error);
						webhookMessage(
							`[${
								this.name
							}]: :x: Error (${errorCode}) triggering perp user (account: ${nodeToTrigger.node.userAccount.toString()}) perp order: ${nodeToTrigger.node.order.orderId.toString()}`
						);
					});
			}
		} catch (e) {
			logger.error(
				`Unexpected error for market ${marketIndex.toString()} during triggers`
			);
			console.error(e);
			webhookMessage(`[${this.name}]: :x: Uncaught error:\n${e}\n${e.stack}`);
		}
	}

	private async tryTriggerForSpotMarket(market: SpotMarketAccount) {
		const marketIndex = market.marketIndex;

		try {
			const oraclePriceData =
				this.driftClient.getOracleDataForSpotMarket(marketIndex);

			let nodesToTrigger: Array<NodeToTrigger> = [];
			await this.dlobMutex.runExclusive(async () => {
				nodesToTrigger = this.dlob.findNodesToTrigger(
					marketIndex,
					this.slotSubscriber.getSlot(),
					oraclePriceData.price,
					MarketType.SPOT,
					this.driftClient.getStateAccount()
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
				this.driftClient
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
						webhookMessage(
							`[${
								this.name
							}]: :gear: Triggered user (account: ${nodeToTrigger.node.userAccount.toString()}) spot order: ${nodeToTrigger.node.order.orderId.toString()}, tx: ${txSig}`
						);
					})
					.catch((error) => {
						const errorCode = getErrorCode(error);
						this?.metrics.recordErrorCode(
							errorCode,
							this.driftClient.provider.wallet.publicKey,
							this.name
						);

						nodeToTrigger.node.haveTrigger = false;
						logger.error(
							`Error (${errorCode}) triggering spot order for user (account: ${nodeToTrigger.node.userAccount.toString()}) spot order: ${nodeToTrigger.node.order.orderId.toString()}`
						);
						logger.error(error);
						webhookMessage(
							`[${
								this.name
							}]: :x: Error (${errorCode}) triggering spot order for user (account: ${nodeToTrigger.node.userAccount.toString()}) spot order: ${nodeToTrigger.node.order.orderId.toString()}`
						);
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
					this.dlob = new DLOB();
					await this.dlob.initFromUserMap(this.userMap);
				});

				await this.resyncUserMapsIfRequired();

				await Promise.all([
					this.driftClient.getPerpMarketAccounts().map((marketAccount) => {
						this.tryTriggerForPerpMarket(marketAccount);
					}),
					this.driftClient.getSpotMarketAccounts().map((marketAccount) => {
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
				webhookMessage(
					`[${this.name}]: :x: Uncaught error in main loop:\n${e}\n${e.stack}`
				);
				throw e;
			}
		} finally {
			if (ran) {
				const duration = Date.now() - start;
				this.metrics?.recordRpcDuration(
					this.driftClient.connection.rpcEndpoint,
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

import {
	ClearingHouse,
	MarketAccount,
	OrderRecord,
	SlotSubscriber,
} from '@drift-labs/sdk';

import { logger } from '../logger';
import { DLOB } from '../dlob/DLOB';
import { UserMap } from '../userMap';
import { Bot } from '../types';
import { getErrorCode } from '../error';
import { Metrics } from '../metrics';

export class TriggerBot implements Bot {
	public readonly name: string;
	public readonly dryRun: boolean;
	public readonly defaultIntervalMs: number = 1000;

	private clearingHouse: ClearingHouse;
	private slotSubscriber: SlotSubscriber;
	private dlob: DLOB;
	private perMarketMutexTriggers = new Uint8Array(new SharedArrayBuffer(8));
	private intervalIds: Array<NodeJS.Timer> = [];
	private userMap: UserMap;
	private metrics: Metrics | undefined;

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
		// initialize DLOB instance
		this.dlob = new DLOB(this.clearingHouse.getMarketAccounts(), true);
		await this.dlob.init(this.clearingHouse);

		// initialize userMap instance
		this.userMap = new UserMap(
			this.clearingHouse,
			this.clearingHouse.userAccountSubscriptionConfig
		);
		await this.userMap.fetchAllUsers();
	}

	public async reset() {
		for (const intervalId of this.intervalIds) {
			clearInterval(intervalId);
		}
		this.intervalIds = [];
		delete this.dlob;
		delete this.userMap;
	}

	public async startIntervalLoop(intervalMs: number): Promise<void> {
		this.tryTrigger();
		const intervalId = setInterval(this.tryTrigger.bind(this), intervalMs);
		this.intervalIds.push(intervalId);

		logger.info(`${this.name} Bot started!`);
	}

	public async trigger(record: any): Promise<void> {
		if (record.eventType === 'OrderRecord') {
			this.dlob.applyOrderRecord(record as OrderRecord);
			await this.userMap.updateWithOrder(record as OrderRecord);
			this.tryTrigger();
		}
	}

	public viewDlob(): DLOB {
		return this.dlob;
	}

	private async tryTriggerForMarket(market: MarketAccount) {
		const marketIndex = market.marketIndex;
		if (
			Atomics.compareExchange(
				this.perMarketMutexTriggers,
				marketIndex.toNumber(),
				0,
				1
			) === 1
		) {
			return;
		}

		try {
			const oraclePriceData =
				this.clearingHouse.getOracleDataForMarket(marketIndex);

			const nodesToTrigger = this.dlob.findNodesToTrigger(
				marketIndex,
				this.slotSubscriber.getSlot(),
				oraclePriceData.price
			);

			for (const nodeToTrigger of nodesToTrigger) {
				if (nodeToTrigger.node.haveTrigger) {
					continue;
				}

				nodeToTrigger.node.haveTrigger = true;

				logger.info(
					`trying to trigger (account: ${nodeToTrigger.node.userAccount.toString()}) order ${nodeToTrigger.node.order.orderId.toString()}`
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
							`Triggered user (account: ${nodeToTrigger.node.userAccount.toString()}) order: ${nodeToTrigger.node.order.orderId.toString()}`
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
							`Error (${errorCode}) triggering user (account: ${nodeToTrigger.node.userAccount.toString()}) order: ${nodeToTrigger.node.order.orderId.toString()}`
						);
						logger.error(error);
					});
			}
		} catch (e) {
			logger.error(
				`Unexpected error for market ${marketIndex.toString()} during triggers`
			);
			console.error(e);
		} finally {
			Atomics.store(this.perMarketMutexTriggers, marketIndex.toNumber(), 0);
		}
	}

	private tryTrigger() {
		for (const marketAccount of this.clearingHouse.getMarketAccounts()) {
			this.tryTriggerForMarket(marketAccount);
		}
	}
}

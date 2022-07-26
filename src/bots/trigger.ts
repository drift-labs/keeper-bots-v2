import { ClearingHouse, MarketAccount, SlotSubscriber } from '@drift-labs/sdk';

import { logger } from '../logger';
import { DLOB } from '../dlob/DLOB';
import { UserMap } from '../userMap';
import { Bot } from '../types';

export class TriggerBot implements Bot {
	public readonly name: string;
	private clearingHouse: ClearingHouse;
	private slotSubscriber: SlotSubscriber;
	private dlob: DLOB;
	private perMarketMutexTriggers: Array<number> = []; // TODO: use real mutex
	private intervalIds: Array<NodeJS.Timer> = [];
	private userMap: UserMap;

	constructor(
		name: string,
		clearingHouse: ClearingHouse,
		slotSubscriber: SlotSubscriber,
		dlob: DLOB,
		userMap: UserMap
	) {
		this.name = name;
		this.clearingHouse = clearingHouse;
		this.slotSubscriber = slotSubscriber;
		this.dlob = dlob;
		this.userMap = userMap;
	}

	public reset(): void {
		for (const intervalId of this.intervalIds) {
			clearInterval(intervalId);
		}
		this.intervalIds = [];
	}

	public start(): void {
		this.tryTrigger();
		const intervalId = setInterval(this.tryTrigger.bind(this), 500);
		this.intervalIds.push(intervalId);
	}

	public async trigger(): Promise<void> {
		logger.info('Triggering bot triggered');
		this.tryTrigger();
	}

	private async tryTriggerForMarket(market: MarketAccount) {
		const marketIndex = market.marketIndex;
		if (this.perMarketMutexTriggers[marketIndex.toNumber()] === 1) {
			return;
		}
		this.perMarketMutexTriggers[marketIndex.toNumber()] = 1;

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

				const user = this.userMap.get(
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
						nodeToTrigger.node.haveTrigger = false;
						logger.error(
							`Error triggering user (account: ${nodeToTrigger.node.userAccount.toString()}) order: ${nodeToTrigger.node.order.orderId.toString()}`
						);
						console.error(error);
					});
			}
		} catch (e) {
			logger.error(
				`Unexpected error for market ${marketIndex.toString()} during triggers`
			);
			console.error(e);
		} finally {
			this.perMarketMutexTriggers[marketIndex.toNumber()] = 0;
		}
	}

	private tryTrigger() {
		for (const marketAccount of this.clearingHouse.getMarketAccounts()) {
			this.tryTriggerForMarket(marketAccount);
		}
	}
}

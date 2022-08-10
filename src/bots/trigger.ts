import {
	ClearingHouse,
	MarketAccount,
	OrderRecord,
	SlotSubscriber,
} from '@drift-labs/sdk';

import { Connection } from '@solana/web3.js';

import { logger } from '../logger';
import { DLOB } from '../dlob/DLOB';
import { UserMap } from '../userMap';
import { Bot } from '../types';
import { getErrorCode } from '../error';
import { Metrics } from '../metrics';

export class TriggerBot implements Bot {
	public readonly name: string;
	public readonly dryRun: boolean;
	private clearingHouse: ClearingHouse;
	private slotSubscriber: SlotSubscriber;
	private dlob: DLOB;
	private perMarketMutexTriggers: Array<number> = []; // TODO: use real mutex
	private intervalIds: Array<NodeJS.Timer> = [];
	private userMap: UserMap;
	private connection: Connection;
	private metrics: Metrics | undefined;

	constructor(
		name: string,
		dryRun: boolean,
		clearingHouse: ClearingHouse,
		slotSubscriber: SlotSubscriber,
		connection: Connection,
		metrics?: Metrics | undefined
	) {
		this.name = name;
		this.dryRun = dryRun;
		this.clearingHouse = clearingHouse;
		this.slotSubscriber = slotSubscriber;
		this.connection = connection;
		this.metrics = metrics;
	}

	public async init() {
		// initialize DLOB instance
		this.dlob = new DLOB(this.clearingHouse.getMarketAccounts());
		const programAccounts = await this.clearingHouse.program.account.user.all();
		for (const programAccount of programAccounts) {
			// @ts-ignore
			const userAccount: UserAccount = programAccount.account;
			const userAccountPublicKey = programAccount.publicKey;

			for (const order of userAccount.orders) {
				this.dlob.insert(order, userAccountPublicKey);
			}
		}

		// initialize userMap instance
		this.userMap = new UserMap(this.connection, this.clearingHouse);
		await this.userMap.fetchAllUsers();
	}

	public reset(): void {
		for (const intervalId of this.intervalIds) {
			clearInterval(intervalId);
		}
		this.intervalIds = [];
		delete this.dlob;
		delete this.userMap;
	}

	public startIntervalLoop(intervalMs: number): void {
		this.tryTrigger();
		const intervalId = setInterval(this.tryTrigger.bind(this), intervalMs);
		this.intervalIds.push(intervalId);

		logger.info(`${this.name} Bot started!`);
	}

	public async trigger(record: OrderRecord): Promise<void> {
		this.dlob.applyOrderRecord(record);
		await this.userMap.updateWithOrder(record);
		this.tryTrigger();
	}

	public viewDlob(): DLOB {
		return this.dlob;
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
						const errorCode = getErrorCode(error);
						this?.metrics.recordErrorCode(
							errorCode,
							this.clearingHouse.provider.wallet.publicKey,
							this.name
						);

						nodeToTrigger.node.haveTrigger = false;
						logger.error(
							`Error triggering user (account: ${nodeToTrigger.node.userAccount.toString()}) order: ${nodeToTrigger.node.order.orderId.toString()}`
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
			this.perMarketMutexTriggers[marketIndex.toNumber()] = 0;
		}
	}

	private tryTrigger() {
		for (const marketAccount of this.clearingHouse.getMarketAccounts()) {
			this.tryTriggerForMarket(marketAccount);
		}
	}
}

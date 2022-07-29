import {
	isVariant,
	isOracleValid,
	ClearingHouse,
	MarketAccount,
	OrderRecord,
	SlotSubscriber,
	calculateAskPrice,
	calculateBidPrice,
	calculateBaseAssetAmountMarketCanExecute,
} from '@drift-labs/sdk';

import { Connection } from '@solana/web3.js';

import { getErrorCode } from '../error';
import { logger } from '../logger';
import { DLOB } from '../dlob/DLOB';
import { UserMap } from '../userMap';
import { Bot } from '../types';

export class FillerBot implements Bot {
	public readonly name: string;
	public readonly dryRun: boolean;
	private clearingHouse: ClearingHouse;
	private slotSubscriber: SlotSubscriber;
	private dlob: DLOB;
	private perMarketMutexFills: Array<number> = []; // TODO: use real mutex
	private intervalIds: Array<NodeJS.Timer> = [];
	private userMap: UserMap;
	private connection: Connection;

	constructor(
		name: string,
		dryRun: boolean,
		clearingHouse: ClearingHouse,
		slotSubscriber: SlotSubscriber,
		connection: Connection
	) {
		this.name = name;
		this.dryRun = dryRun;
		this.clearingHouse = clearingHouse;
		this.slotSubscriber = slotSubscriber;
		this.connection = connection;
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
	}

	public startIntervalLoop(intervalMs: number): void {
		this.tryFill();
		const intervalId = setInterval(this.tryFill.bind(this), intervalMs);
		this.intervalIds.push(intervalId);

		logger.info(`${this.name} Bot started!`);
	}

	public async trigger(record: OrderRecord): Promise<void> {
		this.dlob.applyOrderRecord(record);
		await this.userMap.updateWithOrder(record);
		this.tryFill();
	}

	public viewDlob(): DLOB {
		return this.dlob;
	}

	private async tryFillForMarket(market: MarketAccount) {
		const marketIndex = market.marketIndex;
		if (this.perMarketMutexFills[marketIndex.toNumber()] === 1) {
			return;
		}
		this.perMarketMutexFills[marketIndex.toNumber()] = 1;

		try {
			const oraclePriceData =
				this.clearingHouse.getOracleDataForMarket(marketIndex);
			const oracleIsValid = isOracleValid(
				market.amm,
				oraclePriceData,
				this.clearingHouse.getStateAccount().oracleGuardRails,
				this.slotSubscriber.getSlot()
			);

			const vAsk = calculateAskPrice(market, oraclePriceData);
			const vBid = calculateBidPrice(market, oraclePriceData);

			const nodesToFill = this.dlob.findNodesToFill(
				marketIndex,
				vBid,
				vAsk,
				this.slotSubscriber.getSlot(),
				oracleIsValid ? oraclePriceData : undefined
			);

			for (const nodeToFill of nodesToFill) {
				if (nodeToFill.node.haveFilled) {
					continue;
				}

				if (
					!nodeToFill.makerNode &&
					(isVariant(nodeToFill.node.order.orderType, 'limit') ||
						isVariant(nodeToFill.node.order.orderType, 'triggerLimit'))
				) {
					const baseAssetAmountMarketCanExecute =
						calculateBaseAssetAmountMarketCanExecute(
							market,
							nodeToFill.node.order,
							oraclePriceData
						);

					if (
						baseAssetAmountMarketCanExecute.lt(
							market.amm.baseAssetAmountStepSize
						)
					) {
						continue;
					}
				}

				nodeToFill.node.haveFilled = true;

				logger.info(
					`${
						this.name
					} trying to fill (account: ${nodeToFill.node.userAccount.toString()}) order ${nodeToFill.node.order.orderId.toString()}`
				);

				if (nodeToFill.makerNode) {
					`${
						this.name
					} including maker: ${nodeToFill.makerNode.userAccount.toString()}) with order ${nodeToFill.makerNode.order.orderId.toString()}`;
				}

				let makerInfo;
				if (nodeToFill.makerNode) {
					makerInfo = {
						maker: nodeToFill.makerNode.userAccount,
						order: nodeToFill.makerNode.order,
					};
				}

				const user = this.userMap.get(nodeToFill.node.userAccount.toString());
				this.clearingHouse
					.fillOrder(
						nodeToFill.node.userAccount,
						user.getUserAccount(),
						nodeToFill.node.order,
						makerInfo
					)
					.then((txSig) => {
						logger.info(
							`${
								this.name
							} Filled user (account: ${nodeToFill.node.userAccount.toString()}) order: ${nodeToFill.node.order.orderId.toString()}, Tx: ${txSig}`
						);
					})
					.catch((error) => {
						nodeToFill.node.haveFilled = false;
						logger.error(
							`Error filling user (account: ${nodeToFill.node.userAccount.toString()}) order: ${nodeToFill.node.order.orderId.toString()}`
						);

						// If we get an error that order does not exist, assume its been filled by somebody else and we
						// have received the history record yet
						// TODO this might not hold if events arrive out of order
						const errorCode = getErrorCode(error);
						if (errorCode === 6043) {
							this.dlob.remove(
								nodeToFill.node.order,
								nodeToFill.node.userAccount,
								() => {
									logger.error(
										`Order ${nodeToFill.node.order.orderId.toString()} not found when trying to fill. Removing from order list`
									);
								}
							);
							// dlob.printTopOfOrderLists(this.clearingHouse, nodeToFill.node.order.marketIndex);
						}
						logger.error(`Error code: ${errorCode}`);
						console.error(error);
					});
			}
		} catch (e) {
			logger.info(
				`${
					this.name
				} Unexpected error for market ${marketIndex.toString()} during fills`
			);
			console.error(e);
		} finally {
			this.perMarketMutexFills[marketIndex.toNumber()] = 0;
		}
	}

	private tryFill() {
		for (const marketAccount of this.clearingHouse.getMarketAccounts()) {
			this.tryFillForMarket(marketAccount);
		}
	}
}

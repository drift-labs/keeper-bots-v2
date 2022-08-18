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

import { getErrorCode } from '../error';
import { logger } from '../logger';
import { DLOB } from '../dlob/DLOB';
import { UserMap } from '../userMap';
import { Bot } from '../types';
import { Metrics } from '../metrics';

export class FillerBot implements Bot {
	public readonly name: string;
	public readonly dryRun: boolean;
	public readonly defaultIntervalMs: number = 1000;

	private clearingHouse: ClearingHouse;
	private slotSubscriber: SlotSubscriber;
	private dlob: DLOB;
	private perMarketMutexFills = new Uint8Array(new SharedArrayBuffer(8));
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
			this.clearingHouse.connection,
			this.clearingHouse
		);
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

	public async startIntervalLoop(intervalMs: number): Promise<void> {
		this.tryFill();
		const intervalId = setInterval(this.tryFill.bind(this), intervalMs);
		this.intervalIds.push(intervalId);

		logger.info(`${this.name} Bot started!`);
	}

	public async trigger(record: any): Promise<void> {
		if (record.eventType === 'OrderRecord') {
			this.dlob.applyOrderRecord(record as OrderRecord);
			await this.userMap.updateWithOrder(record as OrderRecord);
			this.tryFill();
		}
	}

	public viewDlob(): DLOB {
		return this.dlob;
	}

	private async tryFillForMarket(market: MarketAccount) {
		const marketIndex = market.marketIndex;

		if (
			Atomics.compareExchange(
				this.perMarketMutexFills,
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

				const user = await this.userMap.mustGet(
					nodeToFill.node.userAccount.toString()
				);
				this.clearingHouse
					.fillOrder(
						nodeToFill.node.userAccount,
						user.getUserAccount(),
						nodeToFill.node.order,
						makerInfo
					)
					.then((txSig) => {
						this.metrics?.recordFilledOrder(
							this.clearingHouse.provider.wallet.publicKey,
							this.name
						);
						logger.info(
							`${
								this.name
							} Filled user (account: ${nodeToFill.node.userAccount.toString()}) order: ${nodeToFill.node.order.orderId.toString()}, Tx: ${txSig}`
						);
					})
					.catch((error) => {
						nodeToFill.node.haveFilled = false;

						// If we get an error that order does not exist, assume its been filled by somebody else and we
						// have received the history record yet
						// TODO this might not hold if events arrive out of order
						const errorCode = getErrorCode(error);
						this.metrics?.recordErrorCode(
							errorCode,
							this.clearingHouse.provider.wallet.publicKey,
							this.name
						);

						if (errorCode === 6042) {
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
						logger.error(
							`Error (${errorCode}) filling user (account: ${nodeToFill.node.userAccount.toString()}) order: ${nodeToFill.node.order.orderId.toString()}`
						);
						// console.error(error);
					});
			}
		} catch (e) {
			logger.error(
				`${
					this.name
				} Unexpected error for market ${marketIndex.toString()} during fills: ${JSON.stringify(
					e
				)}`
			);
			console.error(e);
		} finally {
			Atomics.store(this.perMarketMutexFills, marketIndex.toNumber(), 0);
		}
	}

	private tryFill() {
		for (const marketAccount of this.clearingHouse.getMarketAccounts()) {
			this.tryFillForMarket(marketAccount);
		}
	}
}

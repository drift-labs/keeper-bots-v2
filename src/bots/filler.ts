import {
	isOracleValid,
	ClearingHouse,
	MarketAccount,
	OrderRecord,
	SlotSubscriber,
	calculateAskPrice,
	calculateBidPrice,
	MakerInfo,
	isFillableByVAMM,
} from '@drift-labs/sdk';

import { getErrorCode } from '../error';
import { logger } from '../logger';
import { DLOB } from '../dlob/DLOB';
import { UserMap } from '../userMap';
import { UserStatsMap } from '../userStatsMap';
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
	private userStatsMap: UserStatsMap;
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
		const initPromises: Array<Promise<any>> = [];

		this.dlob = new DLOB(this.clearingHouse.getMarketAccounts(), true);
		initPromises.push(this.dlob.init(this.clearingHouse));

		this.userMap = new UserMap(
			this.clearingHouse,
			this.clearingHouse.userAccountSubscriptionConfig
		);
		initPromises.push(this.userMap.fetchAllUsers());

		this.userStatsMap = new UserStatsMap(
			this.clearingHouse,
			this.clearingHouse.userAccountSubscriptionConfig
		);
		initPromises.push(this.userStatsMap.fetchAllUserStats());

		await Promise.all(initPromises);
	}

	public reset(): void {
		for (const intervalId of this.intervalIds) {
			clearInterval(intervalId);
		}
		this.intervalIds = [];
		delete this.dlob;
		delete this.userMap;
		delete this.userStatsMap;
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
			await this.userStatsMap.updateWithOrder(
				record as OrderRecord,
				this.userMap
			);
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
					!isFillableByVAMM(
						nodeToFill.node.order,
						market,
						oraclePriceData,
						this.slotSubscriber.getSlot()
					)
				) {
					continue;
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

				let makerInfo: MakerInfo | undefined;
				if (nodeToFill.makerNode) {
					const makerAuthority = (
						await this.userMap.mustGet(
							nodeToFill.makerNode.userAccount.toString()
						)
					).getUserAccount().authority;
					const makerUserStats = (
						await this.userStatsMap.mustGet(makerAuthority.toString())
					).userStatsAccountPublicKey;
					makerInfo = {
						maker: nodeToFill.makerNode.userAccount,
						order: nodeToFill.makerNode.order,
						makerStats: makerUserStats,
					};
				}

				const user = await this.userMap.mustGet(
					nodeToFill.node.userAccount.toString()
				);

				const referrerInfo = (
					await this.userStatsMap.mustGet(
						user.getUserAccount().authority.toString()
					)
				).getReferrerInfo();

				// logger.info(`filling user ${user.getUserAccount().authority} - ${nodeToFill.node.order.orderId.toNumber()}`);
				// return;
				this.clearingHouse
					.fillOrder(
						nodeToFill.node.userAccount,
						user.getUserAccount(),
						nodeToFill.node.order,
						makerInfo,
						referrerInfo
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

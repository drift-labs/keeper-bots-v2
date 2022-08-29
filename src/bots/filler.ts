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
import { promiseTimeout } from '@drift-labs/sdk/lib/util/promiseTimeout';

import { SendTransactionError } from '@solana/web3.js';

import { getErrorCode, getErrorMessage } from '../error';
import { logger } from '../logger';
import { DLOB, NodeToFill } from '../dlob/DLOB';
import { UserMap } from '../userMap';
import { UserStatsMap } from '../userStatsMap';
import { Bot } from '../types';
import { Metrics } from '../metrics';

const FILL_ORDER_BACKOFF = 5000;

export class FillerBot implements Bot {
	public readonly name: string;
	public readonly dryRun: boolean;
	public readonly defaultIntervalMs: number = 5000;

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
		this.metrics?.trackObjectSize('filler-dlob', this.dlob);
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
		await this.tryFill();
		const intervalId = setInterval(await this.tryFill.bind(this), intervalMs);
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
			// this.tryFill();
		}
	}

	public viewDlob(): DLOB {
		return this.dlob;
	}

	private getFillableNodesForMarket(market: MarketAccount): Array<NodeToFill> {
		const marketIndex = market.marketIndex;
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

		return this.dlob.findNodesToFill(
			marketIndex,
			vBid,
			vAsk,
			this.slotSubscriber.getSlot(),
			oracleIsValid ? oraclePriceData : undefined
		);
	}

	private async tryFillWithTimeout(nodeToFill: NodeToFill) {
		if (nodeToFill.node.haveFilled) {
			return;
		}
		if (
			nodeToFill.node.lastFillAttempt &&
			nodeToFill.node.lastFillAttempt.getTime() + FILL_ORDER_BACKOFF >
				new Date().getTime()
		) {
			logger.error(
				`${
					this.name
				} backingoff for order (account: ${nodeToFill.node.userAccount.toString()}) order ${nodeToFill.node.order.orderId.toString()} on mktIdx: ${nodeToFill.node.market.marketIndex.toString()}`
			);
			return;
		}

		const marketIndex = nodeToFill.node.market.marketIndex;
		const oraclePriceData =
			this.clearingHouse.getOracleDataForMarket(marketIndex);

		if (
			!nodeToFill.makerNode &&
			!isFillableByVAMM(
				nodeToFill.node.order,
				nodeToFill.node.market,
				oraclePriceData,
				this.slotSubscriber.getSlot(),
				this.clearingHouse.getStateAccount().maxAuctionDuration
			)
		) {
			return;
		}

		nodeToFill.node.haveFilled = true;

		// logger.info(
		// 	`${
		// 		this.name
		// 	} trying to fill (account: ${nodeToFill.node.userAccount.toString()}) order ${nodeToFill.node.order.orderId.toString()} on mktIdx: ${marketIndex.toString()}`
		// );

		if (nodeToFill.makerNode) {
			`${
				this.name
			} including maker: ${nodeToFill.makerNode.userAccount.toString()}) with order ${nodeToFill.makerNode.order.orderId.toString()}`;
		}

		let makerInfo: MakerInfo | undefined;
		if (nodeToFill.makerNode) {
			const makerAuthority = (
				await this.userMap.mustGet(nodeToFill.makerNode.userAccount.toString())
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

		if (this.dryRun) {
			logger.info(`${this.name} dry run, not filling`);
			return;
		}

		const reqStart = Date.now();
		this.metrics?.recordRpcRequests('fillOrder', this.name);
		const txSig: any | null = await promiseTimeout(
			this.clearingHouse.fillOrder(
				nodeToFill.node.userAccount,
				user.getUserAccount(),
				nodeToFill.node.order,
				makerInfo,
				referrerInfo
			),
			10 * 1000
		)
			.catch((error) => {
				nodeToFill.node.haveFilled = false;
				nodeToFill.node.lastFillAttempt = new Date();

				const errorCode = getErrorCode(error);
				this.metrics?.recordErrorCode(
					errorCode,
					this.clearingHouse.provider.wallet.publicKey,
					this.name
				);

				const errorMessage = getErrorMessage(error as SendTransactionError);

				if (errorMessage === 'OrderDoesNotExist') {
					this.dlob.remove(
						nodeToFill.node.order,
						nodeToFill.node.userAccount,
						() => {
							logger.error(
								`Order ${nodeToFill.node.order.orderId.toString()} not found when trying to fill. Removing from order list`
							);
						}
					);
				}
				logger.error(
					`Error (${errorCode}) filling user (account: ${nodeToFill.node.userAccount.toString()}) order: ${nodeToFill.node.order.orderId.toString()}, mktIdx: ${marketIndex.toNumber()}`
				);
			})
			.finally(() => {
				const duration = Date.now() - reqStart;
				this.metrics?.recordRpcDuration(
					this.clearingHouse.connection.rpcEndpoint,
					'fillOrder',
					duration,
					false,
					this.name
				);
			});

		if (txSig === null) {
			logger.error(
				`Timeout filling order mktIdx: ${marketIndex.toNumber()} (account: ${nodeToFill.node.userAccount.toString()}) order: ${nodeToFill.node.order.orderId.toString()}`
			);
			return;
		} else if (txSig === undefined) {
			// this is the case when it hits the catch block above
			return;
		} else {
			this.metrics?.recordFilledOrder(
				this.clearingHouse.provider.wallet.publicKey,
				this.name
			);
			logger.info(
				`${
					this.name
				} Filled user (account: ${nodeToFill.node.userAccount.toString()}) order: ${nodeToFill.node.order.orderId.toString()}, Tx: ${txSig}`
			);
		}
	}

	private async tryFill() {
		const startTime = Date.now();
		if (Atomics.compareExchange(this.perMarketMutexFills, 0, 0, 1) === 1) {
			logger.info(`${this.name} tryFill is bizzy`);
			return false;
		}

		// 1) get all fillable nodes
		const markets = this.clearingHouse.getMarketAccounts();
		const fillableNodes: Array<NodeToFill> = [];
		for (const market of markets) {
			fillableNodes.push(...this.getFillableNodesForMarket(market));
		}

		// 2) fill each node
		logger.info(`Fillable nodes: ${fillableNodes.length}`);
		const fillResult = await promiseTimeout(
			Promise.all(
				fillableNodes.map((nodeToFill) => {
					if (nodeToFill.node.isVammNode()) {
						logger.info('vamm node');
						return Promise.resolve();
					}
					logger.info(
						`fillable node (account: ${nodeToFill.node.userAccount.toString()}) order ${nodeToFill.node.order.orderId.toString()} on mktIdx: ${nodeToFill.node.market.marketIndex.toString()}`
					);

					this.tryFillWithTimeout(nodeToFill);
				})
			),
			10000
		);

		if (Atomics.compareExchange(this.perMarketMutexFills, 0, 1, 0) !== 1) {
			logger.error(`${this.name} tryFill had incorrect mutex value`);
			return;
		}
		if (fillResult === null) {
			logger.error(`Timeout tryFill, took ${Date.now() - startTime}ms`);
		} else {
			logger.info(
				`${this.name} finished tryFill market took ${Date.now() - startTime}ms`
			);
		}
	}
}

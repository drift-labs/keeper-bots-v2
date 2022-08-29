import {
	ClearingHouseUser,
	ReferrerInfo,
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
import { Mutex, tryAcquire, withTimeout, E_ALREADY_LOCKED } from 'async-mutex';

import { SendTransactionError } from '@solana/web3.js';

import { getErrorCode, getErrorMessage } from '../error';
import { logger } from '../logger';
import { DLOB, NodeToFill } from '../dlob/DLOB';
import { UserMap } from '../userMap';
import { UserStatsMap } from '../userStatsMap';
import { Bot } from '../types';
import { Metrics } from '../metrics';

const FILL_ORDER_BACKOFF = 5000;
const dlobMutexError = new Error('dlobMutex timeout');
const userMapMutexError = new Error('userMapMutex timeout');
const periodicTaskMutexError = new Error('periodicTaskMutex timeout');

export class FillerBot implements Bot {
	public readonly name: string;
	public readonly dryRun: boolean;
	public readonly defaultIntervalMs: number = 500;

	private clearingHouse: ClearingHouse;
	private slotSubscriber: SlotSubscriber;

	private dlobMutex = withTimeout(
		new Mutex(),
		10 * this.defaultIntervalMs,
		dlobMutexError
	);
	private dlob: DLOB;

	private userMapMutex = withTimeout(
		new Mutex(),
		10 * this.defaultIntervalMs,
		userMapMutexError
	);
	private userMap: UserMap;
	private userStatsMap: UserStatsMap;

	private periodicTaskMutex = withTimeout(
		new Mutex(),
		5 * this.defaultIntervalMs,
		periodicTaskMutexError
	);

	private intervalIds: Array<NodeJS.Timer> = [];
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
		logger.warn('filler initing');

		await Promise.all([
			this.dlobMutex.runExclusive(async () => {
				const initPromises: Array<Promise<any>> = [];

				this.dlob = new DLOB(this.clearingHouse.getMarketAccounts(), true);
				this.metrics?.trackObjectSize('filler-dlob', this.dlob);
				initPromises.push(this.dlob.init(this.clearingHouse));

				await Promise.all(initPromises);
			}),
			this.userMapMutex.runExclusive(async () => {
				const initPromises: Array<Promise<any>> = [];

				this.userMap = new UserMap(
					this.clearingHouse,
					this.clearingHouse.userAccountSubscriptionConfig
				);
				this.metrics?.trackObjectSize('filler-userMap', this.userMap);
				initPromises.push(this.userMap.fetchAllUsers());

				this.userStatsMap = new UserStatsMap(
					this.clearingHouse,
					this.clearingHouse.userAccountSubscriptionConfig
				);
				this.metrics?.trackObjectSize('filler-userStatsMap', this.userStatsMap);
				initPromises.push(this.userStatsMap.fetchAllUserStats());

				await Promise.all(initPromises);
			}),
		]);

		logger.warn('init done');
	}

	public async reset() {
		logger.warn('filler resetting');
		await Promise.all([
			this.periodicTaskMutex.runExclusive(async () => {
				for (const intervalId of this.intervalIds) {
					clearInterval(intervalId);
				}
				this.intervalIds = [];
			}),
			this.dlobMutex.runExclusive(async () => {
				delete this.dlob;
			}),
			this.userMapMutex.runExclusive(async () => {
				delete this.userMap;
				delete this.userStatsMap;
			}),
		]);
		logger.warn('reset done');
	}

	public async startIntervalLoop(intervalMs: number) {
		// await this.tryFill();
		const intervalId = setInterval(this.tryFill.bind(this), intervalMs);
		this.intervalIds.push(intervalId);

		logger.info(`${this.name} Bot started!`);
	}

	public async trigger(record: any) {
		if (record.eventType === 'OrderRecord') {
			await Promise.all([
				this.dlobMutex.runExclusive(async () => {
					this.dlob.applyOrderRecord(record as OrderRecord);
				}),
				this.userMapMutex.runExclusive(async () => {
					await this.userMap.updateWithOrder(record as OrderRecord);
					await this.userStatsMap.updateWithOrder(
						record as OrderRecord,
						this.userMap
					);
				}),
			]);
		}
	}

	public viewDlob(): DLOB {
		return this.dlob;
	}

	private async getFillableNodesForMarket(
		market: MarketAccount
	): Promise<Array<NodeToFill>> {
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

		let nodes: Array<NodeToFill> = [];
		await this.dlobMutex.runExclusive(async () => {
			nodes = this.dlob.findNodesToFill(
				marketIndex,
				vBid,
				vAsk,
				this.slotSubscriber.getSlot(),
				oracleIsValid ? oraclePriceData : undefined
			);
		});

		return nodes;
	}

	private filterFillableNodes(nodeToFill: NodeToFill): boolean {
		if (nodeToFill.node.isVammNode()) {
			return false;
		}

		if (nodeToFill.node.haveFilled) {
			return false;
		}

		if (
			nodeToFill.node.lastFillAttempt &&
			nodeToFill.node.lastFillAttempt.getTime() + FILL_ORDER_BACKOFF >
				new Date().getTime()
		) {
			return false;
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
			return false;
		}

		return true;
	}

	private async tryFillNode(nodeToFill: NodeToFill) {
		if (!nodeToFill) {
			logger.error(`${this.name} nodeToFill is null`);
			return;
		}

		const marketIndex = nodeToFill.node.market.marketIndex;

		logger.info(
			`${
				this.name
			} trying to fill (account: ${nodeToFill.node.userAccount.toString()}) order ${nodeToFill.node.order.orderId.toString()} on mktIdx: ${marketIndex.toString()}`
		);

		if (nodeToFill.makerNode) {
			`${
				this.name
			} including maker: ${nodeToFill.makerNode.userAccount.toString()}) with order ${nodeToFill.makerNode.order.orderId.toString()}`;
		}

		let makerInfo: MakerInfo | undefined;
		if (nodeToFill.makerNode) {
			await this.userMapMutex.runExclusive(async () => {
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
			});
		}

		let user: ClearingHouseUser;
		let referrerInfo: ReferrerInfo;
		await this.userMapMutex.runExclusive(async () => {
			user = await this.userMap.mustGet(nodeToFill.node.userAccount.toString());

			referrerInfo = (
				await this.userStatsMap.mustGet(
					user.getUserAccount().authority.toString()
				)
			).getReferrerInfo();
		});

		if (this.dryRun) {
			logger.info(`${this.name} dry run, not filling`);
			return;
		}

		const reqStart = Date.now();
		try {
			this.metrics?.recordRpcRequests('fillOrder', this.name);
			const txSig = await this.clearingHouse.fillOrder(
				nodeToFill.node.userAccount,
				user.getUserAccount(),
				nodeToFill.node.order,
				makerInfo,
				referrerInfo
			);
			this.metrics?.recordFilledOrder(
				this.clearingHouse.provider.wallet.publicKey,
				this.name
			);
			logger.info(
				`${
					this.name
				} Filled user (account: ${nodeToFill.node.userAccount.toString()}) order: ${nodeToFill.node.order.orderId.toString()}, Tx: ${txSig}`
			);
		} catch (error) {
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
				await this.dlobMutex.runExclusive(async () => {
					this.dlob.remove(
						nodeToFill.node.order,
						nodeToFill.node.userAccount,
						() => {
							logger.error(
								`Order ${nodeToFill.node.order.orderId.toString()} not found when trying to fill. Removing from order list`
							);
						}
					);
				});
			}
			logger.error(
				`Error (${errorCode}) filling user (account: ${nodeToFill.node.userAccount.toString()}) order: ${nodeToFill.node.order.orderId.toString()}, mktIdx: ${marketIndex.toNumber()}`
			);
		} finally {
			const duration = Date.now() - reqStart;
			this.metrics?.recordRpcDuration(
				this.clearingHouse.connection.rpcEndpoint,
				'fillOrder',
				duration,
				false,
				this.name
			);
		}
	}

	private randomIndex(distribution: Array<any>): any {
		const index = Math.floor(distribution.length * Math.random()); // random index
		return distribution[index];
	}

	private async tryFill() {
		try {
			const startTime = Date.now();
			await tryAcquire(this.periodicTaskMutex).runExclusive(async () => {
				// 1) get all fillable nodes
				const markets = this.clearingHouse.getMarketAccounts();
				const fillableNodes: Array<NodeToFill> = [];
				for (const market of markets) {
					fillableNodes.push(...(await this.getFillableNodesForMarket(market)));
				}

				logger.info(`Fillable nodes: ${fillableNodes.length}`);
				const filteredNodes = fillableNodes.filter((node) =>
					this.filterFillableNodes(node)
				);
				logger.info(`Filtered nodes: ${filteredNodes.length}`);

				// 2) fill a random node - respect rpc rate limit via this.defaultIntervalMs
				const fillResult = await promiseTimeout(
					this.tryFillNode(this.randomIndex(filteredNodes)),
					10000
				);

				if (fillResult === null) {
					logger.error(`Timeout tryFill, took ${Date.now() - startTime}ms`);
				} else {
					logger.info(
						`${this.name} finished tryFill market took ${
							Date.now() - startTime
						}ms`
					);
				}
			});
		} catch (e) {
			if (e === E_ALREADY_LOCKED) {
				this.metrics?.recordMutexBusy(this.name);
			} else if (e === dlobMutexError) {
				logger.error(`${this.name} dlobMutexError timeout`);
			} else if (e === userMapMutexError) {
				logger.error(`${this.name} userMapMutexError timeout`);
			} else if (e === periodicTaskMutexError) {
				logger.error(`${this.name} periodicTaskMutexError timeout`);
			}
		}
	}
}

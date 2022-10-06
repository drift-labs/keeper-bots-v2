import {
	DriftEnv,
	NewUserRecord,
	OrderRecord,
	ClearingHouseUser,
	ReferrerInfo,
	ClearingHouse,
	SpotMarketAccount,
	SlotSubscriber,
	MakerInfo,
	isVariant,
	DLOB,
	NodeToFill,
	UserMap,
	UserStatsMap,
	MarketType,
} from '@drift-labs/sdk';
import { promiseTimeout } from '@drift-labs/sdk/lib/util/promiseTimeout';
import { Mutex, tryAcquire, withTimeout, E_ALREADY_LOCKED } from 'async-mutex';

import { TransactionSignature, PublicKey } from '@solana/web3.js';

import { logger } from '../logger';
import { Bot } from '../types';
import { Metrics } from '../metrics';

/**
 * Size of throttled nodes to get to before pruning the map
 */
const THROTTLED_NODE_SIZE_TO_PRUNE = 10;

/**
 * Time to wait before trying a node again
 */
const FILL_ORDER_BACKOFF = 10000;

const dlobMutexError = new Error('dlobMutex timeout');

export class SpotFillerBot implements Bot {
	public readonly name: string;
	public readonly dryRun: boolean;
	public readonly defaultIntervalMs: number = 1000;

	private driftEnv: DriftEnv;
	private clearingHouse: ClearingHouse;
	private slotSubscriber: SlotSubscriber;

	private dlobMutex = withTimeout(
		new Mutex(),
		10 * this.defaultIntervalMs,
		dlobMutexError
	);
	private dlob: DLOB;

	private userMap: UserMap;
	private userStatsMap: UserStatsMap;

	private periodicTaskMutex = new Mutex();

	private watchdogTimerMutex = new Mutex();
	private watchdogTimerLastPatTime = Date.now();

	private intervalIds: Array<NodeJS.Timer> = [];
	private metrics: Metrics | undefined;
	private throttledNodes = new Map<string, number>();

	constructor(
		name: string,
		dryRun: boolean,
		clearingHouse: ClearingHouse,
		slotSubscriber: SlotSubscriber,
		env: DriftEnv,
		metrics?: Metrics | undefined
	) {
		this.name = name;
		this.dryRun = dryRun;
		this.clearingHouse = clearingHouse;
		this.slotSubscriber = slotSubscriber;
		this.metrics = metrics;
		this.driftEnv = env;
	}

	public async init() {
		logger.info(`${this.name} initing`);

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
	}

	public async reset() {}

	public async startIntervalLoop(intervalMs: number) {
		// await this.tryFill();
		const intervalId = setInterval(this.trySpotFill.bind(this), intervalMs);
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

	public async trigger(record: any) {
		if (record.eventType === 'OrderRecord') {
			await this.userMap.updateWithOrderRecord(record as OrderRecord);
			await this.userStatsMap.updateWithOrderRecord(
				record as OrderRecord,
				this.userMap
			);
			await this.trySpotFill();
		} else if (record.eventType === 'NewUserRecord') {
			await this.userMap.mustGet((record as NewUserRecord).user.toString());
			await this.userStatsMap.mustGet(
				(record as NewUserRecord).user.toString()
			);
		}
	}

	public viewDlob(): DLOB {
		return this.dlob;
	}

	private async getSpotFillableNodesForMarket(
		market: SpotMarketAccount
	): Promise<Array<NodeToFill>> {
		let nodes: Array<NodeToFill> = [];

		const oraclePriceData = this.clearingHouse.getOracleDataForSpotMarket(
			market.marketIndex
		);

		// TODO: check oracle validity when ready

		await this.dlobMutex.runExclusive(async () => {
			nodes = this.dlob.findNodesToFill(
				market.marketIndex,
				undefined,
				undefined,
				this.slotSubscriber.getSlot(),
				MarketType.SPOT,
				oraclePriceData
			);
		});

		return nodes;
	}

	private getNodeToFillSignature(node: NodeToFill): string {
		if (!node.node.userAccount) {
			return '~';
		}
		return `${node.node.userAccount.toString()}-${node.node.order.orderId.toString()}`;
	}

	private filterFillableNodes(nodeToFill: NodeToFill): boolean {
		if (nodeToFill.node.isVammNode()) {
			return false;
		}

		if (nodeToFill.node.haveFilled) {
			return false;
		}

		// if (this.throttledNodes.has(this.getNodeToFillSignature(nodeToFill))) {
		// 	const lastFillAttempt = this.throttledNodes.get(
		// 		this.getNodeToFillSignature(nodeToFill)
		// 	);
		// 	if (lastFillAttempt + FILL_ORDER_BACKOFF > Date.now()) {
		// 		return false;
		// 	} else {
		// 		this.throttledNodes.delete(this.getNodeToFillSignature(nodeToFill));
		// 	}
		// }

		return true;
	}

	private async getNodeFillInfo(nodeToFill: NodeToFill): Promise<{
		makerInfo: MakerInfo | undefined;
		chUser: ClearingHouseUser;
		referrerInfo: ReferrerInfo;
		marketType: MarketType;
	}> {
		let makerInfo: MakerInfo | undefined;
		if (nodeToFill.makerNode) {
			const makerUserAccount = (
				await this.userMap.mustGet(nodeToFill.makerNode.userAccount.toString())
			).getUserAccount();
			const makerAuthority = makerUserAccount.authority;
			const makerUserStats = (
				await this.userStatsMap.mustGet(makerAuthority.toString())
			).userStatsAccountPublicKey;
			makerInfo = {
				maker: nodeToFill.makerNode.userAccount,
				makerUserAccount: makerUserAccount,
				order: nodeToFill.makerNode.order,
				makerStats: makerUserStats,
			};
		}

		const chUser = await this.userMap.mustGet(
			nodeToFill.node.userAccount.toString()
		);
		const referrerInfo = (
			await this.userStatsMap.mustGet(
				chUser.getUserAccount().authority.toString()
			)
		).getReferrerInfo();

		return Promise.resolve({
			makerInfo,
			chUser,
			referrerInfo,
			marketType: nodeToFill.node.order.marketType,
		});
	}

	private nodeIsThrottled(nodeSignature: string): boolean {
		if (this.throttledNodes.has(nodeSignature)) {
			const lastFillAttempt = this.throttledNodes.get(nodeSignature);
			if (lastFillAttempt + FILL_ORDER_BACKOFF > Date.now()) {
				return true;
			}
		}
		return false;
	}
	private throttleNode(nodeSignature: string) {
		this.throttledNodes.set(nodeSignature, Date.now());
	}

	private async tryFillSpotNode(
		nodeToFill: NodeToFill
	): Promise<TransactionSignature> {
		const nodeSignature = this.getNodeToFillSignature(nodeToFill);
		if (this.nodeIsThrottled(nodeSignature)) {
			logger.info(`Throttling ${nodeSignature}`);
			return Promise.resolve(undefined);
		}
		this.throttleNode(nodeSignature);

		logger.info(
			`filling spot node: ${nodeToFill.node.userAccount.toString()}, ${
				nodeToFill.node.order.orderId
			}`
		);

		const { makerInfo, chUser, referrerInfo, marketType } =
			await this.getNodeFillInfo(nodeToFill);

		if (!isVariant(marketType, 'spot')) {
			throw new Error('expected spot market type');
		}

		// TODO: use DevnetSpotMarkets in new SDK
		let serumMarketAddress: PublicKey;
		if (this.driftEnv === 'mainnet-beta') {
			throw new Error('need mainnet markets');
		} else if (this.driftEnv === 'devnet') {
			if (nodeToFill.node.order.marketIndex === 1) {
				serumMarketAddress = new PublicKey(
					'8N37SsnTu8RYxtjrV9SStjkkwVhmU8aCWhLvwduAPEKW'
				);
			} else if (nodeToFill.node.order.marketIndex === 2) {
				serumMarketAddress = new PublicKey(
					'AGsmbVu3MS9u68GEYABWosQQCZwmLcBHu4pWEuBYH7Za'
				);
			} else {
				throw new Error('invalid market index');
			}
		}
		const serumConfig = await this.clearingHouse.getSerumV3FulfillmentConfig(
			serumMarketAddress
		);

		this.metrics?.recordFillableOrdersSeen(
			nodeToFill.node.order.marketIndex,
			marketType,
			1
		);

		const tx = await this.clearingHouse.fillSpotOrder(
			chUser.getUserAccountPublicKey(),
			chUser.getUserAccount(),
			nodeToFill.node.order,
			serumConfig,
			makerInfo,
			referrerInfo
		);

		logger.info(`Filled spot order ${nodeSignature}, TX: ${tx}`);

		return tx;
	}

	private async trySpotFill() {
		const startTime = Date.now();
		let ran = false;

		await tryAcquire(this.periodicTaskMutex)
			.runExclusive(async () => {
				await this.dlobMutex.runExclusive(async () => {
					if (this.dlob) {
						this.dlob.clear();
						delete this.dlob;
					}
					this.dlob = new DLOB(
						this.clearingHouse.getPerpMarketAccounts(), // TODO: new sdk - remove this
						this.clearingHouse.getSpotMarketAccounts(),
						true
					);
					this.metrics?.trackObjectSize('spot-filler-dlob', this.dlob);
					await this.dlob.init(this.clearingHouse, this.userMap);
				});

				if (this.throttledNodes.size > THROTTLED_NODE_SIZE_TO_PRUNE) {
					for (const [key, value] of this.throttledNodes.entries()) {
						if (value + 2 * FILL_ORDER_BACKOFF > Date.now()) {
							this.throttledNodes.delete(key);
						}
					}
				}

				// 1) get all fillable nodes
				let fillableNodes: Array<NodeToFill> = [];
				for (const market of this.clearingHouse.getSpotMarketAccounts()) {
					fillableNodes = fillableNodes.concat(
						await this.getSpotFillableNodesForMarket(market)
					);
				}

				await Promise.all(
					fillableNodes.map(async (spotNodeToFill) => {
						const resp = await promiseTimeout(
							this.tryFillSpotNode(spotNodeToFill),
							FILL_ORDER_BACKOFF / 2
						);
						if (resp === null) {
							logger.error(
								`Timeout tryFillSpotNode, took ${Date.now() - startTime}ms`
							);
						}
					})
				);

				ran = true;
			})
			.catch((e) => {
				if (e === E_ALREADY_LOCKED) {
					console.log("ok, you're busy");
					this.metrics?.recordMutexBusy(this.name);
				} else if (e === dlobMutexError) {
					logger.error(`${this.name} dlobMutexError timeout`);
				} else {
					console.log('some other error...');
					console.error(e);
				}
			})
			.finally(async () => {
				if (ran) {
					const duration = Date.now() - startTime;
					this.metrics?.recordRpcDuration(
						this.clearingHouse.connection.rpcEndpoint,
						'tryFill',
						duration,
						false,
						this.name
					);
					logger.debug(`trySpotFill done, took ${duration}ms`);

					await this.watchdogTimerMutex.runExclusive(async () => {
						this.watchdogTimerLastPatTime = Date.now();
					});
				}
			});
	}
}

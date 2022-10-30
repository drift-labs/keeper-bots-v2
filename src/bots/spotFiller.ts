import {
	DriftEnv,
	NewUserRecord,
	OrderRecord,
	User,
	ReferrerInfo,
	DriftClient,
	SpotMarketAccount,
	SlotSubscriber,
	MakerInfo,
	isVariant,
	DLOB,
	NodeToFill,
	UserMap,
	UserStatsMap,
	MarketType,
	initialize,
	SerumSubscriber,
	PollingDriftClientAccountSubscriber,
	SerumFulfillmentConfigMap,
	promiseTimeout,
	SerumV3FulfillmentConfigAccount,
} from '@drift-labs/sdk';
import { Mutex, tryAcquire, withTimeout, E_ALREADY_LOCKED } from 'async-mutex';

import { PublicKey, TransactionSignature } from '@solana/web3.js';

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
	private driftClient: DriftClient;
	private slotSubscriber: SlotSubscriber;

	private dlobMutex = withTimeout(
		new Mutex(),
		10 * this.defaultIntervalMs,
		dlobMutexError
	);
	private dlob: DLOB;

	private userMap: UserMap;
	private userStatsMap: UserStatsMap;

	private serumFulfillmentConfigMap: SerumFulfillmentConfigMap;
	private serumSubscribers: Map<number, SerumSubscriber>;

	private periodicTaskMutex = new Mutex();

	private watchdogTimerMutex = new Mutex();
	private watchdogTimerLastPatTime = Date.now();

	private intervalIds: Array<NodeJS.Timer> = [];
	private metrics: Metrics | undefined;
	private throttledNodes = new Map<string, number>();

	constructor(
		name: string,
		dryRun: boolean,
		driftClient: DriftClient,
		slotSubscriber: SlotSubscriber,
		env: DriftEnv,
		metrics?: Metrics | undefined
	) {
		this.name = name;
		this.dryRun = dryRun;
		this.driftClient = driftClient;
		this.slotSubscriber = slotSubscriber;
		this.metrics = metrics;
		this.driftEnv = env;
		this.serumFulfillmentConfigMap = new SerumFulfillmentConfigMap(driftClient);
		this.serumSubscribers = new Map<number, SerumSubscriber>();
	}

	public async init() {
		logger.info(`${this.name} initing`);

		const initPromises: Array<Promise<any>> = [];

		this.userMap = new UserMap(
			this.driftClient,
			this.driftClient.userAccountSubscriptionConfig
		);
		initPromises.push(this.userMap.fetchAllUsers());

		this.userStatsMap = new UserStatsMap(
			this.driftClient,
			this.driftClient.userAccountSubscriptionConfig
		);
		initPromises.push(this.userStatsMap.fetchAllUserStats());

		const config = initialize({ env: this.driftEnv });
		for (const spotMarketConfig of config.SPOT_MARKETS) {
			if (spotMarketConfig.serumMarket) {
				// set up fulfillment config
				initPromises.push(
					this.serumFulfillmentConfigMap.add(
						spotMarketConfig.marketIndex,
						spotMarketConfig.serumMarket
					)
				);

				// set up serum price subscriber
				const serumSubscriber = new SerumSubscriber({
					connection: this.driftClient.connection,
					programId: new PublicKey(config.SERUM_V3),
					marketAddress: spotMarketConfig.serumMarket,
					accountSubscription: {
						type: 'polling',
						accountLoader: (
							this.driftClient
								.accountSubscriber as PollingDriftClientAccountSubscriber
						).accountLoader,
					},
				});
				initPromises.push(serumSubscriber.subscribe());
				this.serumSubscribers.set(
					spotMarketConfig.marketIndex,
					serumSubscriber
				);
			}
		}

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

		const oraclePriceData = this.driftClient.getOracleDataForSpotMarket(
			market.marketIndex
		);

		// TODO: check oracle validity when ready

		await this.dlobMutex.runExclusive(async () => {
			const serumSubscriber = this.serumSubscribers.get(market.marketIndex);
			const serumBestBid = serumSubscriber?.getBestBid();
			const serumBestAsk = serumSubscriber?.getBestAsk();

			nodes = this.dlob.findNodesToFill(
				market.marketIndex,
				serumBestBid,
				serumBestAsk,
				this.slotSubscriber.getSlot(),
				Date.now() / 1000,
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

	private async getNodeFillInfo(nodeToFill: NodeToFill): Promise<{
		makerInfo: MakerInfo | undefined;
		chUser: User;
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

		let serumFulfillmentConfig: SerumV3FulfillmentConfigAccount = undefined;
		if (makerInfo === undefined) {
			serumFulfillmentConfig = this.serumFulfillmentConfigMap.get(
				nodeToFill.node.order.marketIndex
			);
		}

		this.metrics?.recordFillableOrdersSeen(
			nodeToFill.node.order.marketIndex,
			marketType,
			1
		);

		const tx = await this.driftClient.fillSpotOrder(
			chUser.getUserAccountPublicKey(),
			chUser.getUserAccount(),
			nodeToFill.node.order,
			serumFulfillmentConfig,
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
						this.driftClient.getPerpMarketAccounts(), // TODO: new sdk - remove this
						this.driftClient.getSpotMarketAccounts(),
						this.driftClient.getStateAccount(),
						this.userMap,
						true
					);
					await this.dlob.init();
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
				for (const market of this.driftClient.getSpotMarketAccounts()) {
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
						this.driftClient.connection.rpcEndpoint,
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

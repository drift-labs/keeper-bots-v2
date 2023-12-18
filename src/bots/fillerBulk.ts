import {
	DriftClient,
	UserStatsMap,
	BulkAccountLoader,
	SlotSubscriber,
	OrderSubscriber,
	UserAccount,
	User,
	NodeToFill,
	NodeToTrigger,
	PerpMarketAccount,
	DLOB,
	calculateAskPrice,
	calculateBidPrice,
	MarketType,
	BN,
} from '@drift-labs/sdk';

import { Keypair, PublicKey } from '@solana/web3.js';

import { SearcherClient } from 'jito-ts/dist/sdk/block-engine/searcher';

import { logger } from '../logger';
import { FillerConfig } from '../config';
import { metricAttrFromUserAccount, RuntimeSpec } from '../metrics';
import { webhookMessage } from '../webhook';
import { FillerBot, SETTLE_POSITIVE_PNL_COOLDOWN_MS } from './filler';
import { E_ALREADY_LOCKED, tryAcquire } from 'async-mutex';

export class FillerBulkBot extends FillerBot {
	private orderSubscriber: OrderSubscriber;

	constructor(
		slotSubscriber: SlotSubscriber,
		driftClient: DriftClient,
		runtimeSpec: RuntimeSpec,
		config: FillerConfig,
		jitoSearcherClient?: SearcherClient,
		jitoAuthKeypair?: Keypair,
		tipPayerKeypair?: Keypair
	) {
		super(
			slotSubscriber,
			undefined,
			driftClient,
			undefined,
			undefined,
			runtimeSpec,
			config,
			jitoSearcherClient,
			jitoAuthKeypair,
			tipPayerKeypair
		);

		this.userStatsMapSubscriptionConfig = {
			type: 'polling',
			accountLoader: new BulkAccountLoader(
				this.driftClient.connection,
				'processed', // No polling so value is irrelevant
				0 // no polling, just for using mustGet
			),
		};

		this.orderSubscriber = new OrderSubscriber({
			driftClient: this.driftClient,
			subscriptionConfig: { type: 'websocket', skipInitialLoad: false },
		});
	}

	public async init() {
		logger.info(`${this.name} initing`);

		// Initializing so we can use mustGet for RPC fall back, but don't subscribe
		// so we don't call getProgramAccounts
		this.userStatsMap = new UserStatsMap(this.driftClient);

		await this.orderSubscriber.subscribe();

		this.lookupTableAccount =
			await this.driftClient.fetchMarketLookupTableAccount();

		await webhookMessage(`[${this.name}]: started`);
	}

	public async startIntervalLoop(_intervalMs?: number) {
		this.intervalIds.push(
			setInterval(this.tryFill.bind(this), this.pollingIntervalMs)
		);
		this.intervalIds.push(
			setInterval(
				this.settlePnls.bind(this),
				SETTLE_POSITIVE_PNL_COOLDOWN_MS / 2
			)
		);

		logger.info(`${this.name} Bot started! (websocket: true)`);
	}

	public async reset() {
		for (const intervalId of this.intervalIds) {
			clearInterval(intervalId as NodeJS.Timeout);
		}
		this.intervalIds = [];

		await this.orderSubscriber.unsubscribe();
	}

	protected async getUserAccountFromMap(key: string): Promise<UserAccount> {
		if (!this.orderSubscriber.usersAccounts.has(key)) {
			const user = new User({
				driftClient: this.driftClient,
				userAccountPublicKey: new PublicKey(key),
				accountSubscription: {
					type: 'polling',
					accountLoader: new BulkAccountLoader(
						this.driftClient.connection,
						'processed',
						0
					),
				},
			});
			await user.subscribe();
			const userAccount = user.getUserAccount();
			return userAccount;
		} else {
			return this.orderSubscriber.usersAccounts.get(key)!.userAccount;
		}
	}

	protected async getDLOB() {
		const currentSlot = this.slotSubscriber.getSlot();
		return await this.orderSubscriber.getDLOB(currentSlot);
	}

	protected async tryFill() {
		const startTime = Date.now();
		let ran = false;
		try {
			await tryAcquire(this.periodicTaskMutex).runExclusive(async () => {
				const dlob = await this.getDLOB();

				this.pruneThrottledNode();

				// 1) get all fillable nodes
				let fillableNodes: Array<NodeToFill> = [];
				let triggerableNodes: Array<NodeToTrigger> = [];
				for (const market of this.driftClient.getPerpMarketAccounts()) {
					try {
						const { nodesToFill, nodesToTrigger } = this.getPerpNodesForMarket(
							market,
							dlob
						);
						fillableNodes = fillableNodes.concat(nodesToFill);
						triggerableNodes = triggerableNodes.concat(nodesToTrigger);
					} catch (e) {
						if (e instanceof Error) {
							console.error(e);
							webhookMessage(
								`[${this.name}]: :x: Failed to get fillable nodes for market ${
									market.marketIndex
								}:\n${e.stack ? e.stack : e.message}`
							);
						}
						continue;
					}
				}

				// filter out nodes that we know cannot be filled
				const { filteredFillableNodes, filteredTriggerableNodes } =
					this.filterPerpNodesForMarket(fillableNodes, triggerableNodes);
				logger.debug(
					`filtered fillable nodes from ${fillableNodes.length} to ${filteredFillableNodes.length}, filtered triggerable nodes from ${triggerableNodes.length} to ${filteredTriggerableNodes.length}`
				);

				// fill the perp nodes
				await Promise.all([
					this.executeFillablePerpNodesForMarket(filteredFillableNodes),
					this.executeTriggerablePerpNodesForMarket(filteredTriggerableNodes),
				]);

				// check if should settle positive pnl

				ran = true;
			});
		} catch (e) {
			if (e === E_ALREADY_LOCKED) {
				const user = this.driftClient.getUser();
				this.mutexBusyCounter!.add(
					1,
					metricAttrFromUserAccount(
						user.getUserAccountPublicKey(),
						user.getUserAccount()
					)
				);
			} else {
				if (e instanceof Error) {
					webhookMessage(
						`[${this.name}]: :x: uncaught error:\n${
							e.stack ? e.stack : e.message
						}`
					);
				}
				throw e;
			}
		} finally {
			if (ran) {
				const duration = Date.now() - startTime;
				const user = this.driftClient.getUser();
				if (this.tryFillDurationHistogram) {
					this.tryFillDurationHistogram!.record(
						duration,
						metricAttrFromUserAccount(
							user.getUserAccountPublicKey(),
							user.getUserAccount()
						)
					);
				}
				logger.debug(`tryFill done, took ${duration}ms`);

				await this.watchdogTimerMutex.runExclusive(async () => {
					this.watchdogTimerLastPatTime = Date.now();
				});
			}
		}
	}

	protected getPerpNodesForMarket(
		market: PerpMarketAccount,
		dlob: DLOB
	): {
		nodesToFill: Array<NodeToFill>;
		nodesToTrigger: Array<NodeToTrigger>;
	} {
		const marketIndex = market.marketIndex;

		const oraclePriceData =
			this.driftClient.getOracleDataForPerpMarket(marketIndex);

		const fillSlot = this.orderSubscriber.getSlot();

		return {
			nodesToFill: this.findNodesToFill(market, dlob),
			nodesToTrigger: dlob.findNodesToTrigger(
				marketIndex,
				fillSlot,
				oraclePriceData.price,
				MarketType.PERP,
				this.driftClient.getStateAccount()
			),
		};
	}

	protected findNodesToFill(
		market: PerpMarketAccount,
		dlob: DLOB
	): NodeToFill[] {
		const nodesToFill = [];
		const marketIndex = market.marketIndex;

		const oraclePriceData =
			this.driftClient.getOracleDataForPerpMarket(marketIndex);

		const vAsk = calculateAskPrice(market, oraclePriceData);
		const vBid = calculateBidPrice(market, oraclePriceData);

		const fillSlot = this.orderSubscriber.getSlot();

		const bestDLOBBid = dlob.getBestBid(
			marketIndex,
			fillSlot,
			MarketType.PERP,
			oraclePriceData
		);
		const bestBid = BN.max(bestDLOBBid, vBid);

		const topRestingBids = [
			...dlob.getRestingLimitBids(
				marketIndex,
				fillSlot,
				MarketType.PERP,
				oraclePriceData
			),
		].slice(0, 8);

		const bestDLOBAsk = dlob.getBestAsk(
			marketIndex,
			fillSlot,
			MarketType.PERP,
			oraclePriceData
		);
		const bestAsk = BN.min(bestDLOBAsk, vAsk);

		const topRestingAsks = [
			...dlob.getRestingLimitAsks(
				marketIndex,
				fillSlot,
				MarketType.PERP,
				oraclePriceData
			),
		].slice(0, 8);

		const takingBidsGenerator = dlob.getTakingBids(
			marketIndex,
			MarketType.PERP,
			fillSlot,
			oraclePriceData
		);
		for (const takingBid of takingBidsGenerator) {
			if (takingBid.getPrice(oraclePriceData, fillSlot).gte(bestAsk)) {
				nodesToFill.push({
					node: takingBid,
					makerNodes: topRestingAsks,
				});
			}
		}

		const takingAsksGenerator = dlob.getTakingAsks(
			marketIndex,
			MarketType.PERP,
			fillSlot,
			oraclePriceData
		);
		for (const takingAsk of takingAsksGenerator) {
			if (takingAsk.getPrice(oraclePriceData, fillSlot).lte(bestBid)) {
				nodesToFill.push({
					node: takingAsk,
					makerNodes: topRestingBids,
				});
			}
		}

		return nodesToFill;
	}
}

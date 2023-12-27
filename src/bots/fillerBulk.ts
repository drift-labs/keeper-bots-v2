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
	BN_MAX,
	ZERO,
} from '@drift-labs/sdk';

import { Keypair, PublicKey } from '@solana/web3.js';

import { SearcherClient } from 'jito-ts/dist/sdk/block-engine/searcher';

import { logger } from '../logger';
import { FillerConfig } from '../config';
import { webhookMessage } from '../webhook';
import { FillerBot, SETTLE_POSITIVE_PNL_COOLDOWN_MS } from './filler';
import { RuntimeSpec } from '../metrics';

const MAX_NUM_MAKERS = 6;

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
		const currentSlot = this.orderSubscriber.getSlot();
		return await this.orderSubscriber.getDLOB(currentSlot);
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
		const bestBid = BN.max(bestDLOBBid ?? ZERO, vBid);

		const seenBidMaker = new Set<string>();
		const restingBids = dlob.getRestingLimitBids(
			marketIndex,
			fillSlot,
			MarketType.PERP,
			oraclePriceData
		);
		const topRestingBids = [];
		for (const restingBid of restingBids) {
			topRestingBids.push(restingBid);
			seenBidMaker.add(restingBid.userAccount?.toString() || '');
			if (seenBidMaker.size == MAX_NUM_MAKERS) {
				break;
			}
		}

		const bestDLOBAsk = dlob.getBestAsk(
			marketIndex,
			fillSlot,
			MarketType.PERP,
			oraclePriceData
		);
		const bestAsk = BN.min(bestDLOBAsk ?? BN_MAX, vAsk);

		const seenAskMaker = new Set<string>();
		const restingAsks = dlob.getRestingLimitAsks(
			marketIndex,
			fillSlot,
			MarketType.PERP,
			oraclePriceData
		);
		const topRestingAsks = [];
		for (const restingAsk of restingAsks) {
			topRestingAsks.push(restingAsk);
			seenAskMaker.add(restingAsk.userAccount?.toString() || '');
			if (seenAskMaker.size == MAX_NUM_MAKERS) {
				break;
			}
		}

		const takingBidsGenerator = dlob.getTakingBids(
			marketIndex,
			MarketType.PERP,
			fillSlot,
			oraclePriceData
		);
		for (const takingBid of takingBidsGenerator) {
			const takingBidPrice = takingBid.getPrice(oraclePriceData, fillSlot);
			if (!takingBidPrice || takingBidPrice.gte(bestAsk)) {
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
			const takingAskPrice = takingAsk.getPrice(oraclePriceData, fillSlot);
			if (!takingAsk || takingAskPrice.lte(bestBid)) {
				nodesToFill.push({
					node: takingAsk,
					makerNodes: topRestingBids,
				});
			}
		}

		return nodesToFill;
	}
}

import {
	DriftClient,
	UserStatsMap,
	BulkAccountLoader,
	SlotSubscriber,
	OrderSubscriber,
	UserAccount,
	User,
	PriorityFeeSubscriber,
	DataAndSlot,
	BlockhashSubscriber,
} from '@drift-labs/sdk';

import { PublicKey } from '@solana/web3.js';

import { logger } from '../logger';
import { FillerConfig } from '../config';
import { RuntimeSpec } from '../metrics';
import { webhookMessage } from '../webhook';
import { FillerBot, SETTLE_POSITIVE_PNL_COOLDOWN_MS } from './filler';

import { sleepMs } from '../utils';
import { BundleSender } from 'src/bundleSender';

export class FillerLiteBot extends FillerBot {
	protected orderSubscriber: OrderSubscriber;

	constructor(
		slotSubscriber: SlotSubscriber,
		driftClient: DriftClient,
		runtimeSpec: RuntimeSpec,
		config: FillerConfig,
		priorityFeeSubscriber: PriorityFeeSubscriber,
		blockhashSubscriber: BlockhashSubscriber,
		bundleSender?: BundleSender
	) {
		super(
			slotSubscriber,
			undefined,
			driftClient,
			undefined,
			undefined,
			runtimeSpec,
			config,
			priorityFeeSubscriber,
			blockhashSubscriber,
			bundleSender
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
			subscriptionConfig: {
				type: 'websocket',
				skipInitialLoad: false,
				resyncIntervalMs: 10_000,
			},
		});
	}

	public async init() {
		logger.info(`${this.name} initing`);

		// Initializing so we can use mustGet for RPC fall back, but don't subscribe
		// so we don't call getProgramAccounts
		this.userStatsMap = new UserStatsMap(this.driftClient);

		await this.orderSubscriber.subscribe();
		await sleepMs(1200); // Wait a few slots to build up order book

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

	protected async getUserAccountAndSlotFromMap(
		key: string
	): Promise<DataAndSlot<UserAccount>> {
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
			return user.getUserAccountAndSlot()!;
		} else {
			const userAccount = this.orderSubscriber.usersAccounts.get(key)!;
			return {
				data: userAccount.userAccount,
				slot: userAccount.slot,
			};
		}
	}

	protected async getDLOB() {
		const currentSlot = this.getMaxSlot();
		return await this.orderSubscriber.getDLOB(currentSlot);
	}

	protected getMaxSlot(): number {
		return Math.max(
			this.slotSubscriber.getSlot(),
			this.orderSubscriber!.getSlot()
		);
	}

	protected logSlots() {
		logger.info(
			`slotSubscriber slot: ${this.slotSubscriber.getSlot()}, orderSubscriber slot: ${this.orderSubscriber.getSlot()}`
		);
	}
}

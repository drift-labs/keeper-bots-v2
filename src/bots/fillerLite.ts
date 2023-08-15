import {
	DriftClient,
	UserStatsMap,
	BulkAccountLoader,
	SlotSubscriber,
	OrderSubscriber,
	UserAccount,
	User,
} from '@drift-labs/sdk';

import { Keypair, PublicKey } from '@solana/web3.js';

import { SearcherClient } from 'jito-ts/dist/sdk/block-engine/searcher';

import { logger } from '../logger';
import { FillerConfig } from '../config';
import { RuntimeSpec } from '../metrics';
import { webhookMessage } from '../webhook';
import { FillerBot } from './filler';

export class FillerLiteBot extends FillerBot {
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
			subscriptionConfig: { type: 'websocket', skipInitialLoad: true },
		});
	}

	public async init() {
		logger.info(`${this.name} initing`);

		// Initializing so we can use mustGet for RPC fall back, but don't subscribe
		// so we don't call getProgramAccounts
		this.userStatsMap = new UserStatsMap(
			this.driftClient,
			this.userStatsMapSubscriptionConfig
		);

		await this.orderSubscriber.subscribe();
		await this.sleep(1200); // Wait a few slots to build up order book

		this.lookupTableAccount =
			await this.driftClient.fetchMarketLookupTableAccount();

		await webhookMessage(`[${this.name}]: started`);
	}

	public async startIntervalLoop(_intervalMs: number) {
		const intervalId = setInterval(
			this.tryFill.bind(this),
			this.pollingIntervalMs
		);
		this.intervalIds.push(intervalId);

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
			return this.orderSubscriber.usersAccounts.get(key).userAccount;
		}
	}

	protected async getDLOB() {
		const currentSlot = this.slotSubscriber.getSlot();
		return await this.orderSubscriber.getDLOB(currentSlot);
	}
}

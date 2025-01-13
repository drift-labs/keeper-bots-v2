import { PythLazerClient } from '@pythnetwork/pyth-lazer-sdk';
import { sleepMs } from './utils';

export class PythLazerSubscriber {
	private pythLazerClient?: PythLazerClient;
	feedIdChunkToPriceMessage: Map<string, string> = new Map();
	feedIdHashToFeedIds: Map<string, number[]> = new Map();
	allSubscribedIds: number[] = [];

	timeoutId?: NodeJS.Timeout;
	receivingData = false;
	isUnsubscribing = false;

	constructor(
		private endpoint: string,
		private token: string,
		private priceFeedIdsArrays: number[][],
		private resubTimeoutMs: number = 2000
	) {}

	async subscribe() {
		this.pythLazerClient = new PythLazerClient([this.endpoint], this.token);
		let subscriptionId = 1;
		let totalSlept = 0;
		if (this.pythLazerClient.wsp !== this.pythLazerClient.wsp) {
			await sleepMs(1000);
			totalSlept += 1000;
			if (totalSlept > 5000) {
				console.error(`Failed to connect to pyth lazer client`);
				throw new Error('Failed to connect to pyth lazer client');
			}
		}
		for (const priceFeedIds of this.priceFeedIdsArrays) {
			this.allSubscribedIds.push(...priceFeedIds);
			this.feedIdHashToFeedIds.set(this.hash(priceFeedIds), priceFeedIds);
			this.pythLazerClient.addMessageListener((message) => {
				this.receivingData = true;
				clearTimeout(this.timeoutId);
				switch (message.type) {
					case 'json': {
						if (message.value.type == 'streamUpdated') {
							if (message.value.solana?.data)
								this.feedIdChunkToPriceMessage.set(
									this.hash(priceFeedIds),
									message.value.solana.data
								);
						}
						break;
					}
					default: {
						break;
					}
				}
				this.setTimeout();
			});
			this.pythLazerClient.subscribe({
				type: 'subscribe',
				subscriptionId,
				priceFeedIds,
				properties: ['price'],
				chains: ['solana'],
				deliveryFormat: 'json',
				channel: 'fixed_rate@200ms',
				jsonBinaryEncoding: 'hex',
			});
			subscriptionId++;
		}

		this.receivingData = true;
		this.setTimeout();
	}

	protected setTimeout(): void {
		this.timeoutId = setTimeout(async () => {
			if (this.isUnsubscribing) {
				// If we are in the process of unsubscribing, do not attempt to resubscribe
				return;
			}

			if (this.receivingData) {
				console.log(`No ws data from pyth lazer client resubscribing`);
				await this.unsubscribe();
				this.receivingData = false;
				await this.subscribe();
			}
		}, this.resubTimeoutMs);
	}

	async unsubscribe() {
		this.isUnsubscribing = true;
		this.pythLazerClient?.shutdown();
		this.pythLazerClient = undefined;
		clearTimeout(this.timeoutId);
		this.timeoutId = undefined;
		this.isUnsubscribing = false;
	}

	hash(arr: number[]): string {
		return 'h:' + arr.join('|');
	}

	getLatestPriceMessage(feedIds: number[]): string | undefined {
		return this.feedIdChunkToPriceMessage.get(this.hash(feedIds));
	}

	getPriceFeedIdsFromHash(hash: string): number[] {
		return this.feedIdHashToFeedIds.get(hash) || [];
	}
}

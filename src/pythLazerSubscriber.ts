import { PythLazerClient } from '@pythnetwork/pyth-lazer-sdk';
import { DriftEnv, PerpMarkets } from '@drift-labs/sdk';

export class PythLazerSubscriber {
	private pythLazerClient?: PythLazerClient;
	feedIdChunkToPriceMessage: Map<string, string> = new Map();
	feedIdHashToFeedIds: Map<string, number[]> = new Map();
	subscriptionIdsToFeedIdsHash: Map<number, string> = new Map();
	allSubscribedIds: number[] = [];

	timeoutId?: NodeJS.Timeout;
	receivingData = false;
	isUnsubscribing = false;

	marketIndextoPriceFeedIdChunk: Map<number, number[]> = new Map();

	constructor(
		private endpoint: string,
		private token: string,
		private priceFeedIdsArrays: number[][],
		env: DriftEnv = 'devnet',
		private resubTimeoutMs: number = 2000
	) {
		const markets = PerpMarkets[env].filter(
			(market) => market.pythLazerId !== undefined
		);
		for (const priceFeedIds of priceFeedIdsArrays) {
			const filteredMarkets = markets.filter((market) =>
				priceFeedIds.includes(market.pythLazerId!)
			);
			for (const market of filteredMarkets) {
				this.marketIndextoPriceFeedIdChunk.set(
					market.marketIndex,
					priceFeedIds
				);
			}
		}
	}

	async subscribe() {
		this.pythLazerClient = await PythLazerClient.create(
			[this.endpoint],
			this.token
		);
		let subscriptionId = 1;
		for (const priceFeedIds of this.priceFeedIdsArrays) {
			const feedIdsHash = this.hash(priceFeedIds);
			this.allSubscribedIds.push(...priceFeedIds);
			this.feedIdHashToFeedIds.set(feedIdsHash, priceFeedIds);
			this.subscriptionIdsToFeedIdsHash.set(subscriptionId, feedIdsHash);
			this.pythLazerClient.addMessageListener((message) => {
				this.receivingData = true;
				clearTimeout(this.timeoutId);
				switch (message.type) {
					case 'json': {
						if (message.value.type == 'streamUpdated') {
							if (message.value.solana?.data) {
								this.feedIdChunkToPriceMessage.set(
									this.subscriptionIdsToFeedIdsHash.get(
										message.value.subscriptionId
									)!,
									message.value.solana.data
								);
							}
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
				properties: ['price', 'bestAskPrice', 'bestBidPrice', 'exponent'],
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

	getLatestPriceMessageForMarketIndex(marketIndex: number): string | undefined {
		const feedIds = this.marketIndextoPriceFeedIdChunk.get(marketIndex);
		if (!feedIds) {
			return undefined;
		}
		return this.feedIdChunkToPriceMessage.get(this.hash(feedIds));
	}

	getPriceFeedIdsFromMarketIndex(marketIndex: number): number[] {
		return this.marketIndextoPriceFeedIdChunk.get(marketIndex) || [];
	}

	getPriceFeedIdsFromHash(hash: string): number[] {
		return this.feedIdHashToFeedIds.get(hash) || [];
	}
}

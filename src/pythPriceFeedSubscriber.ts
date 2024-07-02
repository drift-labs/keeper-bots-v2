import {
	PriceFeed,
	PriceServiceConnection,
	PriceServiceConnectionConfig,
} from '@pythnetwork/price-service-client';

export class PythPriceFeedSubscriber extends PriceServiceConnection {
	protected latestPythVaas: Map<string, string> = new Map(); // priceFeedId -> vaa

	constructor(endpoint: string, config: PriceServiceConnectionConfig) {
		super(endpoint, config);
	}

	async subscribe(feedIds: string[]) {
		await super.subscribePriceFeedUpdates(feedIds, (priceFeed: PriceFeed) => {
			if (priceFeed.vaa) {
				const priceFeedId = '0x' + priceFeed.id;
				this.latestPythVaas.set(priceFeedId, priceFeed.vaa);
			}
		});
	}

	getLatestCachedVaa(feedId: string): string | undefined {
		return this.latestPythVaas.get(feedId);
	}
}

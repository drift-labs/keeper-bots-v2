import { trimFeedId } from '@drift-labs/sdk';
import {
	HermesClient,
	HermesClientConfig,
	PriceUpdate,
} from '@pythnetwork/hermes-client';

export class PythPriceFeedSubscriber extends HermesClient {
	protected latestPythVaas: Map<string, string> = new Map(); // priceFeedId -> vaa
	protected streams?: EventSource[];

	constructor(endpoint: string, config: HermesClientConfig) {
		super(endpoint, config);
	}

	async subscribe(feedIds: string[]) {
		for (const feedId of feedIds) {
			const trimmedId = trimFeedId(feedId);
			const stream = await this.getPriceUpdatesStream([feedId], {
				encoding: 'base64',
			});
			stream.onmessage = async (event) => {
				const data = JSON.parse(event.data) as PriceUpdate;
				if (!data.parsed) {
					console.error('Failed to parse VAA for feedId: ', feedId);
					return;
				}
				this.latestPythVaas.set(trimmedId, data.binary.data[0]);
			};
		}
	}

	getLatestCachedVaa(feedId: string): string | undefined {
		return this.latestPythVaas.get(feedId);
	}
}

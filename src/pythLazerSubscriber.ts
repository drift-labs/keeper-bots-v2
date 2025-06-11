import {
	JsonOrBinaryResponse,
	PythLazerClient,
	Channel,
} from '@pythnetwork/pyth-lazer-sdk';
import { DriftEnv, PerpMarkets } from '@drift-labs/sdk';
import { RedisClient } from '@drift/common/clients';
import * as axios from 'axios';

export class PythLazerSubscriber {
	private pythLazerClient?: PythLazerClient;
	feedIdChunkToPriceMessage: Map<string, string> = new Map();
	feedIdToPrice: Map<number, number> = new Map();
	feedIdHashToFeedIds: Map<string, number[]> = new Map();
	subscriptionIdsToFeedIdsHash: Map<number, string> = new Map();
	allSubscribedIds: number[] = [];

	timeoutId?: NodeJS.Timeout;
	receivingData = false;

	marketIndextoPriceFeedIdChunk: Map<number, number[]> = new Map();
	marketIndextoPriceFeedId: Map<number, number> = new Map();
	useHttpRequests: boolean = false;

	constructor(
		private endpoints: string[],
		private token: string,
		private priceFeedIdsArrays: number[][],
		env: DriftEnv = 'devnet',
		private redisClient?: RedisClient,
		private httpEndpoints: string[] = [],
		private resubTimeoutMs: number = 2000,
		private subscribeChannel = 'fixed_rate@200ms'
	) {
		const markets = PerpMarkets[env].filter(
			(market) => market.pythLazerId !== undefined
		);

		this.allSubscribedIds = this.priceFeedIdsArrays.flat();
		if (
			priceFeedIdsArrays[0].length === 1 &&
			this.allSubscribedIds.length > 3 &&
			this.httpEndpoints.length > 0
		) {
			this.useHttpRequests = true;
		}

		for (const priceFeedIds of priceFeedIdsArrays) {
			const filteredMarkets = markets.filter(
				(market) =>
					market.pythLazerId !== undefined &&
					priceFeedIds.includes(market.pythLazerId)
			);
			for (const market of filteredMarkets) {
				this.marketIndextoPriceFeedIdChunk.set(
					market.marketIndex,
					priceFeedIds
				);
				if (market.pythLazerId === undefined) {
					throw new Error(
						`Pyth Lazer ID not found for market index ${market.marketIndex}`
					);
				}
				this.marketIndextoPriceFeedId.set(
					market.marketIndex,
					market.pythLazerId
				);
			}
		}
	}

	async subscribe() {
		// Will use http requests if chunk size is 1 and there are more than 3 ids
		if (this.useHttpRequests) {
			return;
		}

		this.pythLazerClient = await PythLazerClient.create({
			urls: this.endpoints,
			token: this.token,
			numConnections: 3,
		});

		this.pythLazerClient.addAllConnectionsDownListener(() => {
			console.log(`All connections to pyth lazer are down`);
			this.receivingData = false;
		});

		let subscriptionId = 1;
		for (const priceFeedIds of this.priceFeedIdsArrays) {
			const feedIdsHash = this.hash(priceFeedIds);
			this.feedIdHashToFeedIds.set(feedIdsHash, priceFeedIds);
			this.subscriptionIdsToFeedIdsHash.set(subscriptionId, feedIdsHash);
			this.pythLazerClient.addMessageListener(
				(message: JsonOrBinaryResponse) => {
					this.receivingData = true;
					switch (message.type) {
						case 'json': {
							if (message.value.type == 'streamUpdated') {
								if (message.value.solana?.data) {
									const feedIdHash = this.subscriptionIdsToFeedIdsHash.get(
										message.value.subscriptionId
									);
									if (!feedIdHash) {
										throw new Error(
											`feedIdHash not found for subscriptionId ${message.value.subscriptionId}`
										);
									}
									this.feedIdChunkToPriceMessage.set(
										feedIdHash,
										message.value.solana.data
									);
								}
								if (message.value.parsed?.priceFeeds) {
									for (const priceFeed of message.value.parsed.priceFeeds) {
										if (!priceFeed.price || !priceFeed.exponent) {
											throw new Error(
												`price or exponent not found for priceFeed ${priceFeed.priceFeedId}`
											);
										}
										const price =
											Number(priceFeed.price) *
											Math.pow(10, Number(priceFeed.exponent));
										this.feedIdToPrice.set(priceFeed.priceFeedId, price);
									}
								}
							}
							break;
						}
						default: {
							break;
						}
					}
				}
			);
			this.pythLazerClient.subscribe({
				type: 'subscribe',
				subscriptionId,
				priceFeedIds,
				properties: ['price', 'bestAskPrice', 'bestBidPrice', 'exponent'],
				formats: ['solana'],
				deliveryFormat: 'json',
				channel: this.subscribeChannel as Channel,
				jsonBinaryEncoding: 'hex',
			});
			subscriptionId++;
		}

		this.receivingData = true;
	}

	async unsubscribe() {
		this.pythLazerClient?.shutdown();
		this.pythLazerClient = undefined;
	}

	hash(arr: number[]): string {
		return 'h:' + arr.join('|');
	}

	async getLatestPriceMessage(feedIds: number[]): Promise<string | undefined> {
		if (this.useHttpRequests) {
			if (feedIds.length === 1 && this.redisClient) {
				const priceMessage = (await this.redisClient.get(
					`pythLazerData:${feedIds[0]}`
				)) as { data: string; ts: number } | undefined;
				if (priceMessage?.data && Date.now() - priceMessage.ts < 5000) {
					return priceMessage.data;
				}
			}
			for (const url of this.httpEndpoints) {
				const priceMessage = await this.fetchLatestPriceMessage(url, feedIds);
				if (priceMessage) {
					return priceMessage;
				}
			}
			return undefined;
		}
		return this.feedIdChunkToPriceMessage.get(this.hash(feedIds));
	}

	async fetchLatestPriceMessage(
		url: string,
		feedIds: number[]
	): Promise<string | undefined> {
		try {
			const result = await axios.default.post(
				url,
				{
					priceFeedIds: feedIds,
					properties: ['price', 'bestAskPrice', 'bestBidPrice', 'exponent'],
					chains: ['solana'],
					channel: 'real_time',
					jsonBinaryEncoding: 'hex',
				},
				{
					headers: {
						Authorization: `Bearer ${this.token}`,
					},
				}
			);
			if (result.data && result.status == 200) {
				return result.data['solana']['data'];
			}
		} catch (e) {
			console.error(e);
			return undefined;
		}
	}

	async getLatestPriceMessageForMarketIndex(
		marketIndex: number
	): Promise<string | undefined> {
		const feedIds = this.marketIndextoPriceFeedIdChunk.get(marketIndex);
		if (!feedIds) {
			return undefined;
		}
		return await this.getLatestPriceMessage(feedIds);
	}

	getPriceFeedIdsFromMarketIndex(marketIndex: number): number[] {
		return this.marketIndextoPriceFeedIdChunk.get(marketIndex) || [];
	}

	getPriceFeedIdsFromHash(hash: string): number[] {
		return this.feedIdHashToFeedIds.get(hash) || [];
	}

	getPriceFromMarketIndex(marketIndex: number): number | undefined {
		const feedId = this.marketIndextoPriceFeedId.get(marketIndex);
		if (feedId === undefined) {
			return undefined;
		}
		return this.feedIdToPrice.get(feedId);
	}
}

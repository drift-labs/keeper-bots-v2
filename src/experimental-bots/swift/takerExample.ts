import {
	DriftClient,
	getMarketOrderParams,
	isVariant,
	MarketType,
	PositionDirection,
	digestSignature,
	generateSignedMsgUuid,
	BN,
	OrderParams,
} from '@drift-labs/sdk';
import { RuntimeSpec } from 'src/metrics';
import * as axios from 'axios';
import { sleepMs } from '../../utils';

const CONFIRM_TIMEOUT = 30_000;

export class SwiftTaker {
	interval: NodeJS.Timeout | null = null;
	swiftUrl: string;

	constructor(
		private driftClient: DriftClient,
		runtimeSpec: RuntimeSpec,
		private intervalMs: number
	) {
		this.swiftUrl =
			runtimeSpec.driftEnv === 'mainnet-beta'
				? 'https://swift.drift.trade'
				: 'https://master.swift.drift.trade';
	}

	async init() {
		await this.startInterval();
	}

	public async healthCheck() {
		return true;
	}

	async startInterval() {
		const marketIndexes = [0, 1, 2, 3, 5, 6];
		this.interval = setInterval(async () => {
			await sleepMs(Math.random() * 1000); // Randomize for different grafana metrics
			const slot = await this.driftClient.connection.getSlot();
			const direction =
				Math.random() > 0.5 ? PositionDirection.LONG : PositionDirection.SHORT;

			const marketIndex =
				marketIndexes[Math.floor(Math.random() * marketIndexes.length)];

			const oracleInfo =
				this.driftClient.getOracleDataForPerpMarket(marketIndex);
			const highPrice = oracleInfo.price.muln(101).divn(100);
			const lowPrice = oracleInfo.price;

			const marketOrderParams = getMarketOrderParams({
				marketIndex,
				marketType: MarketType.PERP,
				direction,
				baseAssetAmount: this.driftClient
					.getPerpMarketAccount(marketIndex)!
					.amm.minOrderSize.muln(2),
				auctionStartPrice: isVariant(direction, 'long') ? lowPrice : highPrice,
				auctionEndPrice: isVariant(direction, 'long') ? highPrice : lowPrice,
				auctionDuration: 50,
			});

			const orderMessage = {
				signedMsgOrderParams: marketOrderParams as OrderParams,
				subAccountId: this.driftClient.activeSubAccountId,
				slot: new BN(slot),
				uuid: generateSignedMsgUuid(),
				stopLossOrderParams: null,
				takeProfitOrderParams: null,
			};
			const { orderParams: message, signature } =
				this.driftClient.signSignedMsgOrderParamsMessage(orderMessage);

			const hash = digestSignature(Uint8Array.from(signature));
			console.log(
				`Sending order in slot: ${slot}, time: ${Date.now()}, hash: ${hash}`
			);

			const response = await axios.default.post(
				this.swiftUrl + '/orders',
				{
					market_index: marketIndex,
					market_type: 'perp',
					message: message.toString(),
					signature: signature.toString('base64'),
					taker_pubkey: this.driftClient.wallet.publicKey.toBase58(),
				},
				{
					headers: {
						'Content-Type': 'application/json',
					},
				}
			);
			if (response.status !== 200) {
				console.error('Failed to send order', response.data);
				return;
			}

			const expireTime = Date.now() + CONFIRM_TIMEOUT;
			while (Date.now() < expireTime) {
				const response = await axios.default.get(
					this.swiftUrl +
						'/confirmation/hash-status?hash=' +
						encodeURIComponent(hash),
					{
						validateStatus: (_status) => true,
					}
				);
				if (response.status === 200) {
					console.log('Confirmed hash ', hash);
					return;
				} else if (response.status >= 500) {
					break;
				}
				await new Promise((resolve) => setTimeout(resolve, 10000));
			}
			console.error('Failed to confirm hash: ', hash);
		}, this.intervalMs);
	}
}

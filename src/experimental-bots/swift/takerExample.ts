import {
	BASE_PRECISION,
	DriftClient,
	getMarketOrderParams,
	MarketType,
	PositionDirection,
} from '@drift-labs/sdk';
import { RuntimeSpec } from 'src/metrics';
import * as axios from 'axios';

export class SwiftTaker {
	interval: NodeJS.Timeout | null = null;

	constructor(
		private driftClient: DriftClient,
		runtimeSpec: RuntimeSpec,
		private intervalMs: number
	) {
		if (runtimeSpec.driftEnv != 'devnet') {
			throw new Error('SwiftTaker only works on devnet');
		}
	}

	async init() {
		await this.startInterval();
	}

	async startInterval() {
		this.interval = setInterval(async () => {
			const slot = await this.driftClient.connection.getSlot();
			const direction =
				Math.random() > 0.5 ? PositionDirection.LONG : PositionDirection.SHORT;
			console.log('Sending order in slot:', slot, Date.now());
			const oracleInfo = this.driftClient.getOracleDataForPerpMarket(0);
			const orderMessage = this.driftClient.encodeSwiftOrderParamsMessage({
				swiftOrderParams: getMarketOrderParams({
					marketIndex: 0,
					marketType: MarketType.PERP,
					direction,
					baseAssetAmount: BASE_PRECISION,
					price: oracleInfo.price.muln(105).divn(100),
				}),
				subAccountId: 0,
				expectedOrderId: this.driftClient.getUser().getUserAccount()
					.nextOrderId,
				stopLossOrderParams: null,
				takeProfitOrderParams: null,
			});

			const signature = this.driftClient.signMessage(orderMessage);
			const response = await axios.default.post(
				'https://master.swift.drift.trade/orders',
				{
					market_index: 0,
					market_type: 'perp',
					message: orderMessage.toString('base64'),
					signature: signature.toString('base64'),
					taker_pubkey: this.driftClient.wallet.publicKey.toBase58(),
				},
				{
					headers: {
						'Content-Type': 'application/json',
					},
				}
			);
			console.log(response.data);
		}, this.intervalMs);
	}
}

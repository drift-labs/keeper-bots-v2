import {
	DriftClient,
	getLimitOrderParams,
	getUserAccountPublicKey,
	getUserStatsAccountPublicKey,
	isVariant,
	MarketType,
	PositionDirection,
	PostOnlyParams,
	PublicKey,
	SignedMsgOrderParamsMessage,
	UserMap,
} from '@drift-labs/sdk';
import { RuntimeSpec } from 'src/metrics';
import WebSocket from 'ws';
import nacl from 'tweetnacl';
import { decodeUTF8 } from 'tweetnacl-util';
import { simulateAndGetTxWithCUs } from '../../utils';

export class FastlaneMaker {
	interval: NodeJS.Timeout | null = null;
	private ws: WebSocket | null = null;
	private signedMsgUrl: string;
	private heartbeatTimeout: NodeJS.Timeout | null = null;
	private readonly heartbeatIntervalMs = 80_000;
	constructor(
		private driftClient: DriftClient,
		private userMap: UserMap,
		runtimeSpec: RuntimeSpec,
		private dryRun?: boolean
	) {
		this.signedMsgUrl =
			runtimeSpec.driftEnv === 'mainnet-beta'
				? 'wss://fastlane.drift.trade/ws'
				: 'wss://master.fastlane.drift.trade/ws';
	}

	async init() {
		await this.subscribeWs();
	}

	async subscribeWs() {
		const keypair = this.driftClient.wallet.payer!;
		const ws = new WebSocket(
			this.signedMsgUrl + '?pubkey=' + keypair.publicKey.toBase58()
		);

		ws.on('open', async () => {
			console.log('Connected to the server');
			this.startHeartbeatTimer();

			ws.on('message', async (data: WebSocket.Data) => {
				const message = JSON.parse(data.toString());
				this.startHeartbeatTimer();

				if (message['channel'] === 'auth' && message['nonce'] != null) {
					const messageBytes = decodeUTF8(message['nonce']);
					const signature = nacl.sign.detached(messageBytes, keypair.secretKey);
					const signatureBase64 = Buffer.from(signature).toString('base64');
					ws.send(
						JSON.stringify({
							pubkey: keypair.publicKey.toBase58(),
							signature: signatureBase64,
						})
					);
				}

				if (
					message['channel'] === 'auth' &&
					message['message'] === 'Authenticated'
				) {
					ws.send(
						JSON.stringify({
							action: 'subscribe',
							market_type: 'perp',
							market_name: 'SOL-PERP',
						})
					);
					ws.send(
						JSON.stringify({
							action: 'subscribe',
							market_type: 'perp',
							market_name: 'BTC-PERP',
						})
					);
					ws.send(
						JSON.stringify({
							action: 'subscribe',
							market_type: 'perp',
							market_name: 'ETH-PERP',
						})
					);
					ws.send(
						JSON.stringify({
							action: 'subscribe',
							market_type: 'perp',
							market_name: 'APT-PERP',
						})
					);
					ws.send(
						JSON.stringify({
							action: 'subscribe',
							market_type: 'perp',
							market_name: 'POL-PERP',
						})
					);
					ws.send(
						JSON.stringify({
							action: 'subscribe',
							market_type: 'perp',
							market_name: 'ARB-PERP',
						})
					);
				}

				if (message['order'] && this.driftClient.isSubscribed) {
					const order = message['order'];
					console.info(`uuid: ${order['uuid']} at ${Date.now()}`);

					const signedMsgOrderParamsBufHex = Buffer.from(
						order['order_message']
					);
					const signedMsgOrderParamsBuf = Buffer.from(
						order['order_message'],
						'hex'
					);
					const {
						signedMsgOrderParams,
						subAccountId: takerSubaccountId,
					}: SignedMsgOrderParamsMessage =
						this.driftClient.decodeSignedMsgOrderParamsMessage(
							signedMsgOrderParamsBuf
						);

					const signingAuthority = new PublicKey(order['signing_authority']);
					const takerAuthority = new PublicKey(order['taker_authority']);
					const takerUserPubkey = await getUserAccountPublicKey(
						this.driftClient.program.programId,
						takerAuthority,
						takerSubaccountId
					);
					const takerUserAccount = (
						await this.userMap.mustGet(takerUserPubkey.toString())
					).getUserAccount();

					const isOrderLong = isVariant(signedMsgOrderParams.direction, 'long');
					if (!signedMsgOrderParams.price) {
						console.error(
							`order has no price: ${JSON.stringify(signedMsgOrderParams)}`
						);
						return;
					}

					const ixs =
						await this.driftClient.getPlaceAndMakeSignedMsgPerpOrderIxs(
							{
								orderParams: signedMsgOrderParamsBufHex,
								signature: Buffer.from(order['order_signature'], 'base64'),
							},
							decodeUTF8(order['uuid']),
							{
								taker: takerUserPubkey,
								takerUserAccount,
								takerStats: getUserStatsAccountPublicKey(
									this.driftClient.program.programId,
									takerUserAccount.authority
								),
								signingAuthority,
							},
							getLimitOrderParams({
								marketType: MarketType.PERP,
								marketIndex: signedMsgOrderParams.marketIndex,
								direction: isOrderLong
									? PositionDirection.SHORT
									: PositionDirection.LONG,
								baseAssetAmount: signedMsgOrderParams.baseAssetAmount.divn(2),
								price: isOrderLong
									? signedMsgOrderParams.auctionStartPrice!.muln(99).divn(100)
									: signedMsgOrderParams.auctionEndPrice!.muln(101).divn(100),
								postOnly: PostOnlyParams.MUST_POST_ONLY,
								immediateOrCancel: true,
							})
						);

					if (this.dryRun) {
						// console.log('Dry run, not sending transaction');
						return;
					}

					const resp = await simulateAndGetTxWithCUs({
						connection: this.driftClient.connection,
						payerPublicKey: this.driftClient.wallet.payer!.publicKey,
						ixs,
						cuLimitMultiplier: 1.25,
						lookupTableAccounts:
							await this.driftClient.fetchAllLookupTableAccounts(),
					});
					if (resp.simError) {
						console.log(resp.simTxLogs);
						return;
					}

					this.driftClient.txSender
						.sendVersionedTransaction(resp.tx)
						.then((response) => {
							console.log(response);
						})
						.catch((error) => {
							console.log(error);
						});
				}
			});

			ws.on('close', () => {
				console.log('Disconnected from the server');
				this.reconnect();
			});

			ws.on('error', (error: Error) => {
				console.error('WebSocket error:', error);
				this.reconnect();
			});
		});

		this.ws = ws;
	}

	public async healthCheck() {
		return true;
	}

	private startHeartbeatTimer() {
		if (this.heartbeatTimeout) {
			clearTimeout(this.heartbeatTimeout);
		}
		this.heartbeatTimeout = setTimeout(() => {
			console.warn('No heartbeat received within 60 seconds, reconnecting...');
			this.reconnect();
		}, this.heartbeatIntervalMs);
	}

	private reconnect() {
		if (this.ws) {
			this.ws.removeAllListeners();
			this.ws.terminate();
		}

		console.log('Reconnecting to WebSocket...');
		setTimeout(() => {
			this.subscribeWs();
		}, 1000);
	}
}

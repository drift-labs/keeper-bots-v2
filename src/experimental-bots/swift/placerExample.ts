import {
	BASE_PRECISION,
	BN,
	convertToNumber,
	DriftClient,
	DriftEnv,
	getUserAccountPublicKey,
	getUserStatsAccountPublicKey,
	getVariant,
	isVariant,
	MakerInfo,
	MarketType,
	Order,
	OrderParams,
	OrderStatus,
	OrderTriggerCondition,
	PerpMarkets,
	PositionDirection,
	PRICE_PRECISION,
	PriorityFeeSubscriberMap,
	PublicKey,
	ReferrerInfo,
	ReferrerMap,
	SignedMsgOrderParamsDelegateMessage,
	SignedMsgOrderParamsMessage,
	SlotSubscriber,
	UserMap,
	ZERO,
} from '@drift-labs/sdk';
import { RuntimeSpec } from 'src/metrics';
import WebSocket from 'ws';
import nacl from 'tweetnacl';
import { decodeUTF8 } from 'tweetnacl-util';
import {
	getSizeOfTransaction,
	getWallet,
	simulateAndGetTxWithCUs,
	SimulateAndGetTxWithCUsResponse,
	sleepMs,
} from '../../utils';
import {
	ComputeBudgetProgram,
	Keypair,
	PACKET_DATA_SIZE,
	TransactionInstruction,
} from '@solana/web3.js';
import { getPriorityFeeInstruction } from '../filler-common/utils';
import axios from 'axios';
import { logger } from '../../logger';
import { sha256 } from '@noble/hashes/sha256';

const IGNORE_AUTHORITIES = ['CTh4Q6xooiaJMWCwKP5KLQ4j7X3NEJPf3Uq6rX8UsKSi'];

export class SwiftPlacer {
	interval: NodeJS.Timeout | null = null;
	private ws: WebSocket | null = null;
	private signedMsgUrl: string;
	private heartbeatTimeout: NodeJS.Timeout | null = null;
	private priorityFeeSubscriber: PriorityFeeSubscriberMap;
	private referrerMap: ReferrerMap;
	private readonly heartbeatIntervalMs = 80_000;
	constructor(
		private driftClient: DriftClient,
		private slotSubscriber: SlotSubscriber,
		private userMap: UserMap,
		private runtimeSpec: RuntimeSpec
	) {
		this.signedMsgUrl =
			runtimeSpec.driftEnv === 'mainnet-beta'
				? 'wss://swift.drift.trade/ws'
				: 'wss://master.swift.drift.trade/ws';

		const perpMarketsToWatchForFees = [0, 1, 2, 3, 4, 5].map((x) => {
			return { marketType: 'perp', marketIndex: x };
		});

		this.priorityFeeSubscriber = new PriorityFeeSubscriberMap({
			driftMarkets: perpMarketsToWatchForFees,
			driftPriorityFeeEndpoint: 'https://dlob.drift.trade',
		});

		this.referrerMap = new ReferrerMap(driftClient, true);
	}

	async init() {
		await this.subscribeWs();
		await this.slotSubscriber.subscribe();
		await this.referrerMap.subscribe();
		await this.priorityFeeSubscriber.subscribe();
	}

	async subscribeWs() {
		/**
	  Make sure that WS_DELEGATE_KEY referrs to a keypair for an empty wallet, and that it has been added to
	  ws_delegates for an authority. see here:
	  https://github.com/drift-labs/protocol-v2/blob/master/sdk/src/driftClient.ts#L1160-L1194
	*/
		const keypair = process.env.WS_DELEGATE_KEY
			? getWallet(process.env.WS_DELEGATE_KEY)[0]
			: new Keypair();
		const stakePrivateKey = process.env.STAKE_PRIVATE_KEY;
		let stakeKeypair: Keypair | undefined;
		if (stakePrivateKey) {
			stakeKeypair = getWallet(stakePrivateKey)[0];
		}

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
							stake_pubkey: stakeKeypair?.publicKey.toBase58(),
						})
					);
				}

				if (
					message['channel'] === 'auth' &&
					message['message'] === 'Authenticated'
				) {
					for (const perpMarket of PerpMarkets[
						this.runtimeSpec.driftEnv as DriftEnv
					])
						ws.send(
							JSON.stringify({
								action: 'subscribe',
								market_type: 'perp',
								market_name: perpMarket.symbol,
							})
						);
					await sleepMs(50);
				}

				if (message['order'] && this.driftClient.isSubscribed) {
					const order = message['order'];
					const signedMsgOrderParamsBufHex = Buffer.from(
						order['order_message']
					);
					const signedMsgOrderParamsBuf = Buffer.from(
						order['order_message'],
						'hex'
					);
					const isDelegateSigner = signedMsgOrderParamsBuf
						.slice(0, 8)
						.equals(
							Uint8Array.from(
								Buffer.from(
									sha256('global' + ':' + 'SignedMsgOrderParamsDelegateMessage')
								).slice(0, 8)
							)
						);
					const signedMessage:
						| SignedMsgOrderParamsMessage
						| SignedMsgOrderParamsDelegateMessage =
						this.driftClient.decodeSignedMsgOrderParamsMessage(
							signedMsgOrderParamsBuf,
							isDelegateSigner
						);

					const signedMsgOrderParams = signedMessage.signedMsgOrderParams;

					const signingAuthority = new PublicKey(order['signing_authority']);
					const takerAuthority = new PublicKey(order['taker_authority']);
					const takerUserPubkey = isDelegateSigner
						? (signedMessage as SignedMsgOrderParamsDelegateMessage).takerPubkey
						: await getUserAccountPublicKey(
								this.driftClient.program.programId,
								takerAuthority,
								(signedMessage as SignedMsgOrderParamsMessage).subAccountId
						  );

					const logPrefix = `[${takerUserPubkey.toBase58()}|${order['uuid']}|${
						order['signing_authority']
					}]`;

					if (IGNORE_AUTHORITIES.includes(takerAuthority.toString())) {
						return;
					}

					const takerUser = await this.userMap.mustGet(
						takerUserPubkey.toString()
					);
					const takerUserAccount = takerUser.getUserAccount();

					if (!signedMsgOrderParams.price) {
						console.error(
							`order has no price: ${JSON.stringify(signedMsgOrderParams)}`
						);
						return;
					}
					const computeBudgetIxs: Array<TransactionInstruction> = [
						ComputeBudgetProgram.setComputeUnitLimit({
							units: 1_400_000,
						}),
					];
					computeBudgetIxs.push(
						getPriorityFeeInstruction(
							this.priorityFeeSubscriber.getPriorityFees(
								'perp',
								signedMsgOrderParams.marketIndex
							)?.medium ?? 0
						)
					);

					const ixs = await this.driftClient.getPlaceSignedMsgTakerPerpOrderIxs(
						{
							orderParams: signedMsgOrderParamsBufHex,
							signature: Buffer.from(order['order_signature'], 'base64'),
						},
						signedMsgOrderParams.marketIndex,
						{
							taker: takerUserPubkey,
							takerUserAccount,
							takerStats: getUserStatsAccountPublicKey(
								this.driftClient.program.programId,
								takerUserAccount.authority
							),
							signingAuthority,
						},
						computeBudgetIxs
					);

					const isOrderLong = isVariant(signedMsgOrderParams.direction, 'long');
					const result = await axios.get(
						`https://dlob.drift.trade/topMakers?marketType=perp&marketIndex=${
							signedMsgOrderParams.marketIndex
						}&side=${isOrderLong ? 'ask' : 'bid'}&limit=2`
					);
					if (result.status !== 200) {
						console.error('Failed to get top makers');
						return;
					}

					const topMakers = result.data as string[];

					const orderSlot = Math.min(
						signedMessage.slot.toNumber(),
						this.slotSubscriber.getSlot()
					);
					const signedMsgOrder: Order = {
						status: OrderStatus.OPEN,
						orderType: signedMsgOrderParams.orderType,
						orderId: 0,
						slot: new BN(orderSlot),
						marketIndex: signedMsgOrderParams.marketIndex,
						marketType: MarketType.PERP,
						baseAssetAmount: signedMsgOrderParams.baseAssetAmount,
						auctionDuration: signedMsgOrderParams.auctionDuration!,
						auctionStartPrice: signedMsgOrderParams.auctionStartPrice!,
						auctionEndPrice: signedMsgOrderParams.auctionEndPrice!,
						immediateOrCancel: signedMsgOrderParams.immediateOrCancel ?? false,
						direction: signedMsgOrderParams.direction,
						postOnly: false,
						oraclePriceOffset: signedMsgOrderParams.oraclePriceOffset ?? 0,
						maxTs: signedMsgOrderParams.maxTs ?? ZERO,
						reduceOnly: signedMsgOrderParams.reduceOnly ?? false,
						triggerCondition:
							signedMsgOrderParams.triggerCondition ??
							OrderTriggerCondition.ABOVE,
						price: signedMsgOrderParams.price ?? ZERO,
						userOrderId: signedMsgOrderParams.userOrderId ?? 0,
						// Rest are not necessary and set for type conforming
						existingPositionDirection: PositionDirection.LONG,
						triggerPrice: ZERO,
						baseAssetAmountFilled: ZERO,
						quoteAssetAmountFilled: ZERO,
						quoteAssetAmount: ZERO,
						bitFlags: 0,
						postedSlotTail: 0,
					};

					const makerInfos: MakerInfo[] = [];
					for (const makerKey of topMakers) {
						const makerUser = await this.userMap.mustGet(makerKey);
						makerInfos.push({
							maker: new PublicKey(makerKey),
							makerStats: getUserStatsAccountPublicKey(
								this.driftClient.program.programId,
								makerUser.getUserAccount().authority
							),
							makerUserAccount: makerUser.getUserAccount(),
						});
					}

					let referrerInfo: ReferrerInfo | undefined;
					try {
						referrerInfo = await this.referrerMap?.mustGet(
							takerUserAccount.authority.toString()
						);
					} catch (e) {
						logger.warn(`getNodeFillInfo: Failed to get referrer info: ${e}`);
					}

					let fillIx = await this.driftClient.getFillPerpOrderIx(
						takerUserPubkey,
						takerUserAccount,
						signedMsgOrder,
						makerInfos,
						referrerInfo,
						undefined,
						true
					);

					let txSize = getSizeOfTransaction(
						[...computeBudgetIxs, ...ixs, fillIx],
						true,
						await this.driftClient.fetchAllLookupTableAccounts()
					);
					while (txSize > PACKET_DATA_SIZE) {
						if (makerInfos.length === 0) {
							console.log('No more makers to try');
							break;
						}
						makerInfos.pop();
						fillIx = await this.driftClient.getFillPerpOrderIx(
							takerUserPubkey,
							takerUserAccount,
							signedMsgOrder,
							makerInfos,
							referrerInfo,
							undefined,
							true
						);
						txSize = getSizeOfTransaction(
							[...computeBudgetIxs, ...ixs, fillIx],
							true,
							await this.driftClient.fetchAllLookupTableAccounts()
						);
					}

					let resp: SimulateAndGetTxWithCUsResponse | undefined;
					try {
						resp = await simulateAndGetTxWithCUs({
							connection: this.driftClient.connection,
							payerPublicKey: this.driftClient.wallet.payer!.publicKey,
							ixs: [...computeBudgetIxs, ...ixs, fillIx],
							cuLimitMultiplier: 1.5,
							lookupTableAccounts:
								await this.driftClient.fetchAllLookupTableAccounts(),
							doSimulation: true,
						});
					} catch (e) {
						console.error(e);
						return;
					}

					if (resp.simError) {
						logger.info(`${logPrefix}: ${resp.simTxLogs}`);
						return;
					}

					const orderStr = prettyPrintOrderParams(
						signedMessage.signedMsgOrderParams
					);
					logger.info(`${logPrefix}: placing order: ${orderStr}`);

					this.driftClient.txSender
						.sendVersionedTransaction(resp.tx)
						.then((r) => {
							logger.info(`${logPrefix}: placed order: ${r.txSig}`);
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

function prettyPrintOrderParams(orderParams: OrderParams) {
	const orderParamsStr = `marketIndex:${
		orderParams.marketIndex
	}|orderType:${getVariant(
		orderParams.orderType
	)}|baseAssetAmount:${convertToNumber(
		orderParams.baseAssetAmount,
		BASE_PRECISION
	)}|auctionDuration:${
		orderParams.auctionDuration
	}|auctionStartPrice:${convertToNumber(
		orderParams.auctionStartPrice!,
		PRICE_PRECISION
	)}|auctionEndPrice:${convertToNumber(
		orderParams.auctionEndPrice!,
		PRICE_PRECISION
	)}|`;
	return orderParamsStr;
}

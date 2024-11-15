import {
	DevnetPerpMarkets,
	DriftClient,
	DriftEnv,
	loadKeypair,
	MainnetPerpMarkets,
	SwiftOrderParamsMessage,
	Wallet,
} from '@drift-labs/sdk';
import { Connection, Keypair } from '@solana/web3.js';
import nacl from 'tweetnacl';
import { decodeUTF8 } from 'tweetnacl-util';
import WebSocket from 'ws';
import { sleepMs } from '../../utils';
import dotenv from 'dotenv';
import parseArgs from 'minimist';
import { getDriftClientFromArgs } from './utils';

export type SwiftOrderSubscriberConfig = {
	driftEnv: DriftEnv;
	endpoint: string;
	marketIndexes: number[];
	keypair: Keypair;
};

export class SwiftOrderSubscriber {
	public swiftOrderParamsMessageCallbackFn?: (
		swiftOrderParamsMessage: SwiftOrderParamsMessage
	) => void;
	private heartbeatTimeout: NodeJS.Timeout | null = null;
	private readonly heartbeatIntervalMs = 30000;
	private ws: WebSocket | null = null;
	subscribed: boolean = false;

	constructor(
		private driftClient: DriftClient,
		private config: SwiftOrderSubscriberConfig
	) {}

	get_symbol_for_market_index(marketIndex: number) {
		const markets =
			this.config.driftEnv === 'devnet'
				? DevnetPerpMarkets
				: MainnetPerpMarkets;
		return markets[marketIndex].symbol;
	}

	generateChallengeResponse(nonce: string) {
		const messageBytes = decodeUTF8(nonce);
		const signature = nacl.sign.detached(
			messageBytes,
			this.config.keypair.secretKey
		);
		const signatureBase64 = Buffer.from(signature).toString('base64');
		return signatureBase64;
	}

	handleAuthMessage(message: any) {
		if (message['channel'] === 'auth' && message['nonce'] != null) {
			const signatureBase64 = this.generateChallengeResponse(message['nonce']);
			this.ws?.send(
				JSON.stringify({
					pubkey: this.config.keypair.publicKey.toBase58(),
					signature: signatureBase64,
				})
			);
		}

		if (
			message['channel'] === 'auth' &&
			message['message'].toLowerCase() === 'authenticated'
		) {
			this.subscribed = true;
			this.config.marketIndexes.forEach(async (marketIndex) => {
				this.ws?.send(
					JSON.stringify({
						action: 'subscribe',
						marketType: 'perp',
						market: this.get_symbol_for_market_index(marketIndex),
					})
				);
				await sleepMs(100);
			});
		}
	}

	async subscribe(
		swiftOrderParamsMessageCallbackFn?: (
			swiftOrderParamsMessage: SwiftOrderParamsMessage
		) => void
	) {
		if (!swiftOrderParamsMessageCallbackFn) {
			throw new Error('ordersCallbackFn is required');
		}
		this.swiftOrderParamsMessageCallbackFn = swiftOrderParamsMessageCallbackFn;

		const ws = new WebSocket(
			this.config.endpoint +
				'?pubkey=' +
				this.config.keypair.publicKey.toBase58()
		);
		this.ws = ws;
		ws.on('open', async () => {
			console.log('Connected to the server');

			ws.on('message', async (data: WebSocket.Data) => {
				const message = JSON.parse(data.toString());
				this.startHeartbeatTimer();

				if (message['channel'] === 'auth') {
					this.handleAuthMessage(message);
				}

				if (message['order']) {
					const order = JSON.parse(message['order']);
					const swiftOrderParamsBuf = Buffer.from(
						order['order_message'],
						'base64'
					);
					const swiftOrderParamsMessage: SwiftOrderParamsMessage =
						this.driftClient.program.coder.types.decode(
							'SwiftOrderParamsMessage',
							swiftOrderParamsBuf
						);
					swiftOrderParamsMessageCallbackFn(swiftOrderParamsMessage);
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
	}

	private startHeartbeatTimer() {
		if (this.heartbeatTimeout) {
			clearTimeout(this.heartbeatTimeout);
		}
		this.heartbeatTimeout = setTimeout(() => {
			console.warn('No heartbeat received within 30 seconds, reconnecting...');
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
			this.subscribe(this.swiftOrderParamsMessageCallbackFn);
		}, 1000);
	}
}

async function main() {
	process.on('disconnect', () => process.exit());

	dotenv.config();

	const args = parseArgs(process.argv.slice(2));
	const driftEnv = args['drift-env'] ?? 'devnet';
	const marketIndexesStr = String(args['market-indexes']);
	const marketIndexes = marketIndexesStr.split(',').map(Number);

	const endpoint = process.env.ENDPOINT;
	const privateKey = process.env.KEEPER_PRIVATE_KEY;

	if (!endpoint || !privateKey) {
		throw new Error('ENDPOINT and KEEPER_PRIVATE_KEY must be provided');
	}

	if (driftEnv !== 'devnet') {
		throw new Error('Only devnet is supported');
	}

	const keypair = loadKeypair(privateKey);
	const wallet = new Wallet(keypair);
	const connection = new Connection(endpoint, 'processed');

	const driftClient = getDriftClientFromArgs({
		connection,
		wallet,
		marketIndexes,
		marketTypeStr: 'perp',
		env: driftEnv,
	});
	await driftClient.subscribe();

	const swiftOrderSubscriberConfig: SwiftOrderSubscriberConfig = {
		driftEnv,
		endpoint:
			driftEnv === 'devnet'
				? 'wss://master.swift.drift.trade/ws'
				: 'wss://swift.drift.trade/ws',
		marketIndexes,
		keypair,
	};

	const swiftOrderSubscriber = new SwiftOrderSubscriber(
		driftClient,
		swiftOrderSubscriberConfig
	);
	await swiftOrderSubscriber.subscribe((swiftOrderParamsMessage) => {
		if (typeof process.send === 'function') {
			process.send({
				type: 'swiftOrderParamsMessage',
				data: swiftOrderParamsMessage,
			});
		}
	});
}

main();

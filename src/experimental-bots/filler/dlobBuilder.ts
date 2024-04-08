import {
	DLOB,
	SlotSubscriber,
	MarketType,
	DriftClient,
	loadKeypair,
	calculateBidPrice,
	calculateAskPrice,
	UserAccount,
	isVariant,
	decodeUser,
	NodeToFill,
	NodeToTrigger,
	Wallet,
} from '@drift-labs/sdk';
import { Connection } from '@solana/web3.js';
import dotenv from 'dotenv';
import parseArgs from 'minimist';
import { logger } from '../../logger';
import { SerializedNodeToFill, SerializedNodeToTrigger } from './types';
import {
	getDriftClientFromArgs,
	serializeNodeToFill,
	serializeNodeToTrigger,
} from './utils';

const logPrefix = '[DLOBBuilder]';
class DLOBBuilder {
	private userAccountData = new Map<string, UserAccount>();
	private userAccountDataBuffers = new Map<string, Buffer>();
	private dlob: DLOB;
	public readonly slotSubscriber: SlotSubscriber;
	public readonly marketTypeString: string;
	public readonly marketType: MarketType;
	public readonly marketIndex: number;
	public driftClient: DriftClient;
	public initialized: boolean = false;

	constructor(
		driftClient: DriftClient,
		marketType: MarketType,
		marketTypeString: string,
		marketIndex: number
	) {
		this.dlob = new DLOB();
		this.slotSubscriber = new SlotSubscriber(driftClient.connection);
		this.marketType = marketType;
		this.marketTypeString = marketTypeString;
		this.marketIndex = marketIndex;
		this.driftClient = driftClient;
	}

	public async subscribe() {
		await this.slotSubscriber.subscribe();
	}

	public getUserBuffer(pubkey: string) {
		return this.userAccountDataBuffers.get(pubkey);
	}

	public deserializeAndUpdateUserAccountData(
		userAccount: string,
		pubkey: string
	) {
		const userAccountBuffer = Buffer.from(userAccount, 'base64');
		const deserializedUserAccount = decodeUser(userAccountBuffer);
		this.userAccountDataBuffers.set(pubkey, userAccountBuffer);
		this.userAccountData.set(pubkey, deserializedUserAccount);
	}

	public delete(pubkey: string) {
		this.userAccountData.delete(pubkey);
		this.userAccountDataBuffers.delete(pubkey);
	}

	// Private to avoid race conditions
	private build(): DLOB {
		// console.time("buildDLOB");
		logger.info(
			`${logPrefix} Building DLOB with ${this.userAccountData.size} users`
		);
		this.dlob = new DLOB();
		let counter = 0;
		this.userAccountData.forEach((userAccount, pubkey) => {
			userAccount.orders.forEach((order) => {
				if (
					order.marketIndex != this.marketIndex ||
					!isVariant(order.marketType, this.marketTypeString.toLowerCase())
				) {
					return;
				}
				this.dlob.insertOrder(order, pubkey, this.slotSubscriber.getSlot());
				counter++;
			});
		});
		logger.info(`${logPrefix} Built DLOB with ${counter} orders`);
		// console.timeEnd("buildDLOB");
		return this.dlob;
	}

	public getNodesToTriggerAndNodesToFill(): [NodeToFill[], NodeToTrigger[]] {
		const dlob = this.build();
		const perpMarket = this.driftClient.getPerpMarketAccount(this.marketIndex);
		if (!perpMarket) {
			throw new Error('PerpMarket not found');
		}
		const oraclePriceData = this.driftClient.getOracleDataForPerpMarket(
			this.marketIndex
		);
		const vBid = calculateBidPrice(perpMarket, oraclePriceData);
		const vAsk = calculateAskPrice(perpMarket, oraclePriceData);
		const stateAccount = this.driftClient.getStateAccount();
		const slot = this.slotSubscriber.getSlot();
		const nodesToFill = dlob.findNodesToFill(
			this.marketIndex,
			vBid,
			vAsk,
			slot,
			Date.now(),
			this.marketType,
			oraclePriceData,
			stateAccount,
			perpMarket
		);
		const nodesToTrigger = dlob.findNodesToTrigger(
			this.marketIndex,
			slot,
			oraclePriceData.price,
			this.marketType,
			stateAccount
		);

		return [nodesToFill, nodesToTrigger];
	}

	public serializeNodesToFill(
		nodesToFill: NodeToFill[]
	): SerializedNodeToFill[] {
		return nodesToFill
			.map((node) => {
				const buffer = this.getUserBuffer(node.node.userAccount!);
				if (!buffer) {
					return undefined;
				}
				const makerBuffers = new Map<string, Buffer>();
				for (const makerNode of node.makerNodes) {
					// @ts-ignore
					const makerBuffer = this.getUserBuffer(makerNode.userAccount);

					if (!makerBuffer) {
						return undefined;
					}
					// @ts-ignore
					makerBuffers.set(makerNode.userAccount, makerBuffer);
				}
				return serializeNodeToFill(node, buffer, makerBuffers);
			})
			.filter((node): node is SerializedNodeToFill => node !== undefined);
	}

	public serializeNodesToTrigger(
		nodesToTrigger: NodeToTrigger[]
	): SerializedNodeToTrigger[] {
		return nodesToTrigger
			.map((node) => {
				const buffer = this.getUserBuffer(node.node.userAccount);
				if (!buffer) {
					return undefined;
				}
				return serializeNodeToTrigger(node, buffer);
			})
			.filter((node): node is SerializedNodeToTrigger => node !== undefined);
	}

	public trySendNodes(
		serializedNodesToTrigger: SerializedNodeToTrigger[],
		serializedNodesToFill: SerializedNodeToFill[]
	) {
		if (typeof process.send === 'function') {
			if (serializedNodesToTrigger.length > 0) {
				try {
					logger.info('Sending triggerable nodes');
					process.send({
						type: 'triggerableNodes',
						data: serializedNodesToTrigger,
						// }, { swallowErrors: true });
					});
				} catch (e) {
					logger.error(`${logPrefix} Failed to send triggerable nodes: ${e}`);
					// logger.error(JSON.stringify(serializedNodesToTrigger, null, 2));
				}
			}
			if (serializedNodesToFill.length > 0) {
				try {
					logger.info('Sending fillable nodes');
					process.send({
						type: 'fillableNodes',
						data: serializedNodesToFill,
						// }, { swallowErrors: true });
					});
				} catch (e) {
					logger.error(`${logPrefix} Failed to send fillable nodes: ${e}`);
					// logger.error(JSON.stringify(serializedNodesToFill, null, 2));
				}
			}
		}
	}

	sendLivenessCheck() {
		if (typeof process.send === 'function') {
			process.send({
				type: 'livenessCheck',
			});
		}
	}
}

const main = async () => {
	// kill this process if the parent dies
	process.on('disconnect', () => process.exit());

	dotenv.config();
	const endpoint = process.env.ENDPOINT;
	const privateKey = process.env.KEEPER_PRIVATE_KEY;

	const args = parseArgs(process.argv.slice(2));
	const marketTypeStr = args['market-type'];
	const marketIndex = args['market-index'];
	if (marketTypeStr !== 'perp' && marketTypeStr !== 'spot') {
		throw new Error("market-type must be either 'perp' or 'spot'");
	}

	let marketType: MarketType;
	switch (marketTypeStr) {
		case 'perp':
			marketType = MarketType.PERP;
			break;
		case 'spot':
			marketType = MarketType.SPOT;
			break;
		default:
			console.error('Error: Unsupported market type provided.');
			process.exit(1);
	}

	if (!endpoint || !privateKey) {
		throw new Error('ENDPOINT and KEEPER_PRIVATE_KEY must be provided');
	}
	const wallet = new Wallet(loadKeypair(privateKey));

	const connection = new Connection(endpoint, {
		wsEndpoint: process.env.WS_ENDPOINT,
		commitment: 'processed',
	});

	const driftClient = getDriftClientFromArgs({
		connection,
		wallet,
		marketIndex,
		marketTypeStr,
	});
	await driftClient.subscribe();

	const dlobBuilder = new DLOBBuilder(
		driftClient,
		marketType,
		marketTypeStr,
		marketIndex
	);

	await dlobBuilder.subscribe();
	if (typeof process.send === 'function') {
		logger.info('DLOBBuilder started');
		process.send({ type: 'initialized' });
	}

	process.on('message', (msg: any) => {
		if (!msg.data || typeof msg.data.type === 'undefined') {
			logger.warn(`${logPrefix} Received message without data.type field.`);
			return;
		}
		switch (msg.data.type) {
			case 'update':
				dlobBuilder.deserializeAndUpdateUserAccountData(
					msg.data.userAccount,
					msg.data.pubkey
				);
				break;
			case 'delete':
				dlobBuilder.delete(msg.data.pubkey);
				break;
			default:
				logger.warn(
					`${logPrefix} Received unknown message type: ${msg.data.type}`
				);
		}
	});

	setInterval(() => {
		const [nodesToFill, nodesToTrigger] =
			dlobBuilder.getNodesToTriggerAndNodesToFill();

		const serializedNodesToFill = dlobBuilder.serializeNodesToFill(nodesToFill);
		logger.info(
			`${logPrefix} Serialized ${serializedNodesToFill.length} fillable nodes`
		);

		const serializedNodesToTrigger =
			dlobBuilder.serializeNodesToTrigger(nodesToTrigger);
		logger.info(
			`${logPrefix} Serialized ${serializedNodesToTrigger.length} triggerable nodes`
		);

		dlobBuilder.trySendNodes(serializedNodesToTrigger, serializedNodesToFill);
	}, 200);

	setInterval(() => {
		dlobBuilder.sendLivenessCheck();
	}, 30_000);
};

main();

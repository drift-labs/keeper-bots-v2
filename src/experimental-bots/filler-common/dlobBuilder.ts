/* eslint-disable @typescript-eslint/no-non-null-assertion */
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
	NodeToTrigger,
	Wallet,
	SerumSubscriber,
	PhoenixSubscriber,
	BN,
	ClockSubscriber,
	SerumV3FulfillmentConfigAccount,
	PhoenixV1FulfillmentConfigAccount,
	OpenbookV2Subscriber,
	OpenbookV2FulfillmentConfigAccount,
} from '@drift-labs/sdk';
import { Connection } from '@solana/web3.js';
import dotenv from 'dotenv';
import parseArgs from 'minimist';
import { logger } from '../../logger';
import {
	FallbackLiquiditySource,
	SerializedNodeToFill,
	SerializedNodeToTrigger,
	NodeToFillWithContext,
} from './types';
import {
	getDriftClientFromArgs,
	serializeNodeToFill,
	serializeNodeToTrigger,
} from './utils';
import { initializeSpotFulfillmentAccounts, sleepMs } from '../../utils';

const EXPIRE_ORDER_BUFFER_SEC = 30; // add an extra 30 seconds before trying to expire orders (want to avoid 6252 error due to clock drift)

const logPrefix = '[DLOBBuilder]';
class DLOBBuilder {
	private userAccountData = new Map<string, UserAccount>();
	private userAccountDataBuffers = new Map<string, Buffer>();
	private dlob: DLOB;
	public readonly slotSubscriber: SlotSubscriber;
	public readonly marketTypeString: string;
	public readonly marketType: MarketType;
	public readonly marketIndexes: number[];
	public driftClient: DriftClient;
	public initialized: boolean = false;

	// only used for spot filler
	private serumSubscribers?: Map<number, SerumSubscriber>;
	private phoenixSubscribers?: Map<number, PhoenixSubscriber>;
	private openbookSubscribers?: Map<number, OpenbookV2Subscriber>;
	private serumFulfillmentConfigMap?: Map<
		number,
		SerumV3FulfillmentConfigAccount
	>;
	private phoenixFulfillmentConfigMap?: Map<
		number,
		PhoenixV1FulfillmentConfigAccount
	>;
	private openbookFulfillmentConfigMap?: Map<
		number,
		OpenbookV2FulfillmentConfigAccount
	>;

	private clockSubscriber: ClockSubscriber;

	constructor(
		driftClient: DriftClient,
		marketType: MarketType,
		marketTypeString: string,
		marketIndexes: number[]
	) {
		this.dlob = new DLOB();
		this.slotSubscriber = new SlotSubscriber(driftClient.connection);
		this.marketType = marketType;
		this.marketTypeString = marketTypeString;
		this.marketIndexes = marketIndexes;
		this.driftClient = driftClient;

		if (marketTypeString.toLowerCase() === 'spot') {
			this.serumSubscribers = new Map<number, SerumSubscriber>();
			this.phoenixSubscribers = new Map<number, PhoenixSubscriber>();
			this.openbookSubscribers = new Map<number, OpenbookV2Subscriber>();
			this.serumFulfillmentConfigMap = new Map<
				number,
				SerumV3FulfillmentConfigAccount
			>();
			this.phoenixFulfillmentConfigMap = new Map<
				number,
				PhoenixV1FulfillmentConfigAccount
			>();
			this.openbookFulfillmentConfigMap = new Map<
				number,
				OpenbookV2FulfillmentConfigAccount
			>();
		}

		this.clockSubscriber = new ClockSubscriber(driftClient.connection, {
			commitment: 'confirmed',
			resubTimeoutMs: 5_000,
		});
	}

	public async subscribe() {
		await this.slotSubscriber.subscribe();
		await this.clockSubscriber.subscribe();

		if (this.marketTypeString.toLowerCase() === 'spot') {
			await this.initializeSpotMarkets();
		}
	}

	private async initializeSpotMarkets() {
		({
			serumFulfillmentConfigs: this.serumFulfillmentConfigMap,
			phoenixFulfillmentConfigs: this.phoenixFulfillmentConfigMap,
			openbookFulfillmentConfigs: this.openbookFulfillmentConfigMap,
			serumSubscribers: this.serumSubscribers,
			phoenixSubscribers: this.phoenixSubscribers,
			openbookSubscribers: this.openbookSubscribers,
		} = await initializeSpotFulfillmentAccounts(
			this.driftClient,
			true,
			this.marketIndexes
		));

		if (!this.serumSubscribers) {
			throw new Error('serumSubscribers not initialized');
		}
		if (!this.phoenixSubscribers) {
			throw new Error('phoenixSubscribers not initialized');
		}
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
		logger.debug(
			`${logPrefix} Building DLOB with ${this.userAccountData.size} users`
		);
		this.dlob = new DLOB();
		let counter = 0;
		this.userAccountData.forEach((userAccount, pubkey) => {
			userAccount.orders.forEach((order) => {
				if (
					!this.marketIndexes.includes(order.marketIndex) ||
					!isVariant(order.marketType, this.marketTypeString.toLowerCase())
				) {
					return;
				}
				this.dlob.insertOrder(order, pubkey, this.slotSubscriber.getSlot());
				counter++;
			});
		});
		logger.debug(`${logPrefix} Built DLOB with ${counter} orders`);
		return this.dlob;
	}

	public getNodesToTriggerAndNodesToFill(): [
		NodeToFillWithContext[],
		NodeToTrigger[],
	] {
		const dlob = this.build();
		const nodesToFill: NodeToFillWithContext[] = [];
		const nodesToTrigger: NodeToTrigger[] = [];
		for (const marketIndex of this.marketIndexes) {
			let market;
			let oraclePriceData;
			let fallbackAsk: BN | undefined = undefined;
			let fallbackBid: BN | undefined = undefined;
			let fallbackAskSource: FallbackLiquiditySource | undefined = undefined;
			let fallbackBidSource: FallbackLiquiditySource | undefined = undefined;
			if (this.marketTypeString.toLowerCase() === 'perp') {
				market = this.driftClient.getPerpMarketAccount(marketIndex);
				if (!market) {
					throw new Error('PerpMarket not found');
				}
				oraclePriceData =
					this.driftClient.getOracleDataForPerpMarket(marketIndex);
				fallbackBid = calculateBidPrice(market, oraclePriceData);
				fallbackAsk = calculateAskPrice(market, oraclePriceData);
			} else {
				market = this.driftClient.getSpotMarketAccount(marketIndex);
				if (!market) {
					throw new Error('SpotMarket not found');
				}
				oraclePriceData =
					this.driftClient.getOracleDataForSpotMarket(marketIndex);

				const marketSources = [
					{
						source: 'serum',
						subscriber: this.serumSubscribers!.get(marketIndex),
					},
					{
						source: 'phoenix',
						subscriber: this.phoenixSubscribers!.get(marketIndex),
					},
					{
						source: 'openbook',
						subscriber: this.openbookSubscribers!.get(marketIndex),
					},
				];

				const bids = marketSources
					.map((source) => ({
						source: source.source,
						bid: source.subscriber?.getBestBid(),
					}))
					.filter((bid) => bid.bid !== undefined);

				const asks = marketSources
					.map((source) => ({
						source: source.source,
						ask: source.subscriber?.getBestAsk(),
					}))
					.filter((ask) => ask.ask !== undefined);

				const fallbackBidWithSource = bids.reduce(
					(best, current) => (best.bid!.gte(current.bid!) ? best : current),
					bids[0]
				);

				const fallbackAskWithSource = asks.reduce(
					(best, current) => (best.ask!.lte(current.ask!) ? best : current),
					asks[0]
				);

				if (
					fallbackBidWithSource === undefined ||
					fallbackAskWithSource === undefined
				) {
					logger.warn(
						`${logPrefix} No fallback bids or asks found for market ${marketIndex}`
					);
					continue;
				}

				fallbackBidSource =
					fallbackBidWithSource.source as FallbackLiquiditySource;
				fallbackAskSource =
					fallbackAskWithSource.source as FallbackLiquiditySource;
				fallbackBid = fallbackBidWithSource.bid
					? fallbackBidWithSource.bid
					: undefined;
				fallbackAsk = fallbackAskWithSource.ask
					? fallbackAskWithSource.ask
					: undefined;
			}

			const stateAccount = this.driftClient.getStateAccount();
			const slot = this.slotSubscriber.getSlot();
			const nodesToFillForMarket = dlob.findNodesToFill(
				marketIndex,
				fallbackBid,
				fallbackAsk,
				slot,
				this.clockSubscriber.getUnixTs() - EXPIRE_ORDER_BUFFER_SEC,
				this.marketType,
				oraclePriceData,
				stateAccount,
				market
			);
			const nodesToTriggerForMarket = dlob.findNodesToTrigger(
				marketIndex,
				slot,
				oraclePriceData.price,
				this.marketType,
				stateAccount
			);
			nodesToFill.push(
				...nodesToFillForMarket.map((node) => {
					return { ...node, fallbackAskSource, fallbackBidSource };
				})
			);
			nodesToTrigger.push(...nodesToTriggerForMarket);
		}
		return [nodesToFill, nodesToTrigger];
	}

	public serializeNodesToFill(
		nodesToFill: NodeToFillWithContext[]
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
					logger.debug('Sending triggerable nodes');
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
					logger.debug('Sending fillable nodes');
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

	sendLivenessCheck(health: boolean) {
		if (typeof process.send === 'function') {
			process.send({
				type: 'health',
				data: {
					healthy: health,
				},
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
	const driftEnv = args['drift-env'] ?? 'devnet';
	const marketTypeStr = args['market-type'];

	let marketIndexes;
	if (typeof args['market-indexes'] === 'string') {
		marketIndexes = args['market-indexes'].split(',').map(Number);
	} else {
		marketIndexes = [args['market-indexes']];
	}
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
		marketIndexes,
		marketTypeStr,
		env: driftEnv,
	});
	await driftClient.subscribe();

	const dlobBuilder = new DLOBBuilder(
		driftClient,
		marketType,
		marketTypeStr,
		marketIndexes
	);

	await dlobBuilder.subscribe();
	await sleepMs(5000); // Give the dlob some time to get built
	if (typeof process.send === 'function') {
		logger.info('DLOBBuilder started');
		process.send({ type: 'initialized', data: dlobBuilder.marketIndexes });
	}

	process.on('message', (msg: any) => {
		// console.log("received msg");
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
		logger.debug(
			`${logPrefix} Serialized ${serializedNodesToFill.length} fillable nodes`
		);
		const serializedNodesToTrigger =
			dlobBuilder.serializeNodesToTrigger(nodesToTrigger);
		logger.debug(
			`${logPrefix} Serialized ${serializedNodesToTrigger.length} triggerable nodes`
		);
		dlobBuilder.trySendNodes(serializedNodesToTrigger, serializedNodesToFill);
	}, 200);

	dlobBuilder.sendLivenessCheck(true);
	setInterval(() => {
		dlobBuilder.sendLivenessCheck(true);
	}, 10_000);
};

main();

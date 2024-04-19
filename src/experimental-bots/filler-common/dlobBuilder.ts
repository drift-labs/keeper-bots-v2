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
	NodeToFill,
	NodeToTrigger,
	Wallet,
	SerumSubscriber,
	PhoenixSubscriber,
	initialize,
	DriftEnv,
	SerumFulfillmentConfigMap,
	PhoenixFulfillmentConfigMap,
	BulkAccountLoader,
	BN,
} from '@drift-labs/sdk';
import { Connection, PublicKey } from '@solana/web3.js';
import dotenv from 'dotenv';
import parseArgs from 'minimist';
import { logger } from '../../logger';
import { SerializedNodeToFill, SerializedNodeToTrigger } from './types';
import {
	getDriftClientFromArgs,
	serializeNodeToFill,
	serializeNodeToTrigger,
} from './utils';
import { sleepMs } from '../../utils';

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
	// @ts-ignore
	private serumSubscribers: Map<number, SerumSubscriber>;
	// @ts-ignore
	private phoenixSubscribers: Map<number, PhoenixSubscriber>;
	// @ts-ignore
	private serumFulfillmentConfigMap: SerumFulfillmentConfigMap;
	// @ts-ignore
	private phoenixFulfillmentConfigMap: PhoenixFulfillmentConfigMap;

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
			this.serumSubscribers = new Map();
			this.phoenixSubscribers = new Map();
			this.serumFulfillmentConfigMap = new SerumFulfillmentConfigMap(
				driftClient
			);
			this.phoenixFulfillmentConfigMap = new PhoenixFulfillmentConfigMap(
				driftClient
			);
		}
	}

	public async subscribe() {
		await this.slotSubscriber.subscribe();

		if (this.marketTypeString.toLowerCase() === 'spot') {
			await this.initializeSpotMarkets();
		}
	}

	private async initializeSpotMarkets() {
		const config = initialize({ env: 'mainnet-beta' as DriftEnv });

		const marketsOfInterest = config.SPOT_MARKETS.filter((market) => {
			return this.marketIndexes.includes(market.marketIndex);
		});

		const marketSetupPromises = marketsOfInterest.map(
			async (spotMarketConfig) => {
				const subscribePromises = [];

				const accountSubscription:
					| {
							type: 'polling';
							accountLoader: BulkAccountLoader;
					  }
					| {
							type: 'websocket';
					  } = {
					type: 'websocket', // Correctly matching the expected type without any additional properties
				};

				if (spotMarketConfig.serumMarket) {
					// set up fulfillment config
					await this.serumFulfillmentConfigMap.add(
						spotMarketConfig.marketIndex,
						spotMarketConfig.serumMarket
					);

					const serumConfigAccount =
						await this.driftClient.getSerumV3FulfillmentConfig(
							spotMarketConfig.serumMarket
						);

					if (isVariant(serumConfigAccount.status, 'enabled')) {
						// set up serum price subscriber
						const serumSubscriber = new SerumSubscriber({
							connection: this.driftClient.connection,
							programId: new PublicKey(config.SERUM_V3),
							marketAddress: spotMarketConfig.serumMarket,
							accountSubscription: accountSubscription,
						});
						logger.info(
							`${logPrefix} Initializing SerumSubscriber for ${spotMarketConfig.symbol}...`
						);
						subscribePromises.push(
							serumSubscriber.subscribe().then(() => {
								this.serumSubscribers.set(
									spotMarketConfig.marketIndex,
									serumSubscriber
								);
							})
						);
					}
				}

				if (spotMarketConfig.phoenixMarket) {
					// set up fulfillment config
					await this.phoenixFulfillmentConfigMap.add(
						spotMarketConfig.marketIndex,
						spotMarketConfig.phoenixMarket
					);

					const phoenixConfigAccount = this.phoenixFulfillmentConfigMap.get(
						spotMarketConfig.marketIndex
					);
					if (isVariant(phoenixConfigAccount.status, 'enabled')) {
						// set up phoenix price subscriber
						const phoenixSubscriber = new PhoenixSubscriber({
							connection: this.driftClient.connection,
							programId: new PublicKey(config.PHOENIX),
							marketAddress: spotMarketConfig.phoenixMarket,
							accountSubscription: accountSubscription,
						});
						logger.info(
							`${logPrefix} Initializing PhoenixSubscriber for ${spotMarketConfig.symbol}...`
						);
						subscribePromises.push(
							phoenixSubscriber.subscribe().then(() => {
								this.phoenixSubscribers.set(
									spotMarketConfig.marketIndex,
									phoenixSubscriber
								);
							})
						);
					}
				}
				await Promise.all(subscribePromises);
			}
		);
		await Promise.all(marketSetupPromises);
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

	public getNodesToTriggerAndNodesToFill(): [NodeToFill[], NodeToTrigger[]] {
		const dlob = this.build();
		const nodesToFill: NodeToFill[] = [];
		const nodesToTrigger: NodeToTrigger[] = [];
		for (const marketIndex of this.marketIndexes) {
			let market;
			let oraclePriceData;
			let fallbackAsk: BN | undefined = undefined;
			let fallbackBid: BN | undefined = undefined;
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

				const serumSubscriber = this.serumSubscribers.get(marketIndex);
				const serumBid = serumSubscriber?.getBestBid();
				const serumAsk = serumSubscriber?.getBestAsk();

				const phoenixSubscriber = this.phoenixSubscribers.get(marketIndex);
				const phoenixBid = phoenixSubscriber?.getBestBid();
				const phoenixAsk = phoenixSubscriber?.getBestAsk();

				if (serumBid && phoenixBid) {
					if (serumBid!.gte(phoenixBid!)) {
						fallbackBid = serumBid;
					} else {
						fallbackBid = phoenixBid;
					}
				} else if (serumBid) {
					fallbackBid = serumBid;
				} else if (phoenixBid) {
					fallbackBid = phoenixBid;
				}

				if (serumAsk && phoenixAsk) {
					if (serumAsk!.lte(phoenixAsk!)) {
						fallbackAsk = serumAsk;
					} else {
						fallbackAsk = phoenixAsk;
					}
				} else if (serumAsk) {
					fallbackAsk = serumAsk;
				} else if (phoenixAsk) {
					fallbackAsk = phoenixAsk;
				}
			}

			const stateAccount = this.driftClient.getStateAccount();
			const slot = this.slotSubscriber.getSlot();
			const nodesToFillForMarket = dlob.findNodesToFill(
				marketIndex,
				fallbackBid,
				fallbackAsk,
				slot,
				Date.now(),
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
			nodesToFill.push(...nodesToFillForMarket);
			nodesToTrigger.push(...nodesToTriggerForMarket);
		}
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

	dlobBuilder.sendLivenessCheck(true);
	setInterval(() => {
		dlobBuilder.sendLivenessCheck(true);
	}, 10_000);
};

main();

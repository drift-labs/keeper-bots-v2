import { Bot } from '../types';
import { logger } from '../logger';
import {
	GlobalConfig,
	PythCrankerBotConfig,
	PythUpdateConfigs,
} from '../config';
import {
	PriceFeed,
	PriceServiceConnection,
} from '@pythnetwork/price-service-client';
import { PriceUpdateAccount } from '@pythnetwork/pyth-solana-receiver/lib/PythSolanaReceiver';
import {
	BlockhashSubscriber,
	BN,
	convertToNumber,
	DriftClient,
	getOracleClient,
	getPythPullOraclePublicKey,
	isOneOfVariant,
	ONE,
	OracleClient,
	OracleSource,
	PerpMarkets,
	PRICE_PRECISION,
	PriorityFeeSubscriber,
	SpotMarkets,
	TxSigAndSlot,
} from '@drift-labs/sdk';
import { BundleSender } from '../bundleSender';
import {
	AddressLookupTableAccount,
	ComputeBudgetProgram,
	PublicKey,
} from '@solana/web3.js';
import { convertPythPrice } from '@drift-labs/sdk';
import { getFeedIdUint8Array, trimFeedId } from '@drift-labs/sdk';
import { chunks, shuffle, simulateAndGetTxWithCUs, sleepMs } from '../utils';
import { Agent, setGlobalDispatcher } from 'undici';

setGlobalDispatcher(
	new Agent({
		connections: 200,
	})
);

const SIM_CU_ESTIMATE_MULTIPLIER = 1.5;

export const earlyUpdateDefault: PythUpdateConfigs = {
	timeDiffMs: 15_000,
	priceDiffPct: 0.2,
};

export const updateDefault: PythUpdateConfigs = {
	timeDiffMs: 20_000,
	priceDiffPct: 0.35,
};

type FeedIdToCrankInfo = {
	baseSymbol: string;
	feedId: string;
	updateConfig: PythUpdateConfigs;
	earlyUpdateConfig: PythUpdateConfigs;
	accountAddress: PublicKey;
};

export class PythCrankerBot implements Bot {
	private priceServiceConnection: PriceServiceConnection;
	private feedIdsToCrank: FeedIdToCrankInfo[] = [];
	private pythOracleClient: OracleClient;
	readonly decodeFunc: (name: string, data: Buffer) => PriceUpdateAccount;

	public name: string;
	public dryRun: boolean;
	private intervalMs: number;
	private feedIdToPriceFeedMap: Map<string, PriceFeed> = new Map();
	public defaultIntervalMs = 30_000;

	private blockhashSubscriber: BlockhashSubscriber;
	private health: boolean = true;
	private slotStalenessThresholdRestart: number = 300;
	private txSuccessRateThreshold: number = 0.5;

	constructor(
		private globalConfig: GlobalConfig,
		private crankConfigs: PythCrankerBotConfig,
		private driftClient: DriftClient,
		private priorityFeeSubscriber?: PriorityFeeSubscriber,
		private bundleSender?: BundleSender,
		private lookupTableAccounts: AddressLookupTableAccount[] = []
	) {
		this.name = crankConfigs.botId;
		this.dryRun = crankConfigs.dryRun;
		this.intervalMs = crankConfigs.intervalMs;
		if (!globalConfig.hermesEndpoint) {
			throw new Error('Missing hermesEndpoint in global config');
		}
		this.priceServiceConnection = new PriceServiceConnection(
			globalConfig.hermesEndpoint,
			{
				timeout: 10_000,
			}
		);
		this.pythOracleClient = getOracleClient(
			OracleSource.PYTH_PULL,
			driftClient.connection,
			driftClient.program
		);
		this.decodeFunc = this.driftClient
			.getReceiverProgram()
			.account.priceUpdateV2.coder.accounts.decodeUnchecked.bind(
				this.driftClient.getReceiverProgram().account.priceUpdateV2.coder
					.accounts
			);

		this.blockhashSubscriber = new BlockhashSubscriber({
			connection: driftClient.connection,
		});
		this.txSuccessRateThreshold = crankConfigs.txSuccessRateThreshold;
		this.slotStalenessThresholdRestart =
			crankConfigs.slotStalenessThresholdRestart;
	}

	async init(): Promise<void> {
		logger.info(`Initializing ${this.name} bot`);
		await this.blockhashSubscriber.subscribe();
		this.lookupTableAccounts.push(
			await this.driftClient.fetchMarketLookupTableAccount()
		);

		const perpMarketIndexes = this.driftClient
			.getPerpMarketAccounts()
			.map((market) => market.marketIndex);
		const perpMarketConfigs = PerpMarkets[this.globalConfig.driftEnv].filter(
			(market) => {
				const useMarket = this.crankConfigs.onlyCrankUsedOracles
					? isOneOfVariant(market.oracleSource, [
							'pythPull',
							'pythPullStableCoin',
							'pyth1MPull',
							'pyth1KPull',
					  ])
					: true;
				return perpMarketIndexes.includes(market.marketIndex) && useMarket;
			}
		);
		const spotMarketIndexes = this.driftClient
			.getSpotMarketAccounts()
			.map((market) => market.marketIndex);
		const spotMarketConfigs = SpotMarkets[this.globalConfig.driftEnv].filter(
			(market) => {
				const useMarket = this.crankConfigs.onlyCrankUsedOracles
					? isOneOfVariant(market.oracleSource, [
							'pythPull',
							'pythStableCoinPull',
							'pyth1MPull',
							'pyth1KPull',
					  ])
					: true;
				return spotMarketIndexes.includes(market.marketIndex) && useMarket;
			}
		);

		for (const marketConfig of perpMarketConfigs) {
			const feedId = marketConfig.pythFeedId;
			if (!feedId) {
				logger.warn(`No pyth feed id for market ${marketConfig.symbol}`);
				continue;
			}
			const perpMarket = this.driftClient.getPerpMarketAccount(
				marketConfig.marketIndex
			);
			if (!perpMarket) {
				logger.warn(`No perp market for market ${marketConfig.symbol}`);
				continue;
			}

			const updateConfigs = updateDefault;
			const earlyUpdateConfigs = earlyUpdateDefault;
			if (isOneOfVariant(perpMarket.contractTier, ['a', 'b'])) {
				updateConfigs.timeDiffMs = 15_000;
				earlyUpdateConfigs.timeDiffMs = 10_000;
			}
			const pubkey = getPythPullOraclePublicKey(
				this.driftClient.program.programId,
				getFeedIdUint8Array(feedId)
			);

			this.feedIdsToCrank.push({
				baseSymbol: marketConfig.baseAssetSymbol.toUpperCase(),
				feedId,
				updateConfig:
					this.crankConfigs?.updateConfigs?.[feedId]?.update ?? updateConfigs,
				earlyUpdateConfig:
					this.crankConfigs?.updateConfigs?.[feedId]?.earlyUpdate ??
					earlyUpdateConfigs,
				accountAddress: pubkey,
			});
		}

		for (const marketConfig of spotMarketConfigs) {
			if (
				this.feedIdsToCrank.findIndex(
					(feedId) => feedId.baseSymbol === marketConfig.symbol
				) !== -1
			)
				continue;

			const feedId = marketConfig.pythFeedId;
			if (!feedId) {
				logger.warn(`No pyth feed id for market ${marketConfig.symbol}`);
				continue;
			}
			const updateConfigs = updateDefault;
			const earlyUpdateConfigs = earlyUpdateDefault;
			if (
				isOneOfVariant(marketConfig.oracleSource, [
					'pythPullStableCoin',
					'pythStableCoin',
				])
			) {
				updateConfigs.timeDiffMs = 15_000;
				updateConfigs.priceDiffPct = 0.1;
				earlyUpdateConfigs.timeDiffMs = 10_000;
				earlyUpdateConfigs.priceDiffPct = 0.05;
			}
			const pubkey = getPythPullOraclePublicKey(
				this.driftClient.program.programId,
				getFeedIdUint8Array(feedId)
			);
			this.feedIdsToCrank.push({
				baseSymbol: marketConfig.symbol.toUpperCase(),
				feedId,
				updateConfig:
					this.crankConfigs?.updateConfigs?.[feedId]?.update ?? updateConfigs,
				earlyUpdateConfig:
					this.crankConfigs?.updateConfigs?.[feedId]?.earlyUpdate ??
					earlyUpdateConfigs,
				accountAddress: pubkey,
			});
		}

		await this.priceServiceConnection.subscribePriceFeedUpdates(
			this.feedIdsToCrank.map((x) => x.feedId),
			(priceFeed) => {
				this.feedIdToPriceFeedMap.set(priceFeed.id, priceFeed);
			}
		);

		this.priorityFeeSubscriber?.updateAddresses([
			this.driftClient.getReceiverProgram().programId,
		]);
	}

	async reset(): Promise<void> {
		logger.info(`Resetting ${this.name} bot`);
		this.feedIdsToCrank = [];
		this.blockhashSubscriber.unsubscribe();
		await this.driftClient.unsubscribe();
		await this.priceServiceConnection.unsubscribePriceFeedUpdates(
			this.feedIdsToCrank.map((x) => x.feedId)
		);
	}

	async startIntervalLoop(intervalMs = this.intervalMs): Promise<void> {
		logger.info(`Starting ${this.name} bot with interval ${intervalMs} ms`);
		await sleepMs(5000);
		await this.runCrankLoop();

		setInterval(async () => {
			await this.runCrankLoop();
		}, intervalMs);
	}

	async getVaaForPriceFeedIds(feedIds: string[]): Promise<string> {
		const latestVaa = await this.priceServiceConnection.getLatestVaas(feedIds);
		return latestVaa[0];
	}

	async getLatestPriceFeedUpdatesForFeedIds(
		feedIds: string[]
	): Promise<PriceFeed[] | undefined> {
		const latestPrices = await this.priceServiceConnection.getLatestPriceFeeds(
			feedIds
		);
		return latestPrices;
	}

	private async getBlockhashForTx(): Promise<string> {
		const cachedBlockhash = this.blockhashSubscriber.getLatestBlockhash(10);
		if (cachedBlockhash) {
			return cachedBlockhash.blockhash as string;
		}

		const recentBlockhash =
			await this.driftClient.connection.getLatestBlockhash({
				commitment: 'confirmed',
			});

		return recentBlockhash.blockhash;
	}

	async runCrankLoop() {
		const feedIdsToCrankAddressChunks = chunks(
			this.feedIdsToCrank.map((f) => f.accountAddress),
			50
		);
		const onChainDataResults = (
			await Promise.all(
				feedIdsToCrankAddressChunks.map((feedIdsToCrankAddressChunk) => {
					return this.driftClient.connection.getMultipleAccountsInfo(
						feedIdsToCrankAddressChunk
					);
				})
			)
		).flat();

		const latestSlot = await this.driftClient.connection.getSlot();
		let numFeedsSignalingRestart = 0;
		const feedIdsToUpdate: FeedIdToCrankInfo[] = [];
		let considerEarlyUpdate = false;
		shuffle(onChainDataResults).forEach((result) => {
			if (!result) {
				return;
			}
			const onChainPriceFeed = this.decodeFunc('priceUpdateV2', result.data);
			const feedIdCrankInfo = this.feedIdsToCrank.find((f) =>
				f.accountAddress.equals(onChainPriceFeed.writeAuthority)
			);

			if (!feedIdCrankInfo) {
				logger.warn(
					`Missing feed id data for ${onChainPriceFeed.writeAuthority.toString()}`
				);
				return;
			}
			const pythnetPriceFeed = this.feedIdToPriceFeedMap.get(
				trimFeedId(feedIdCrankInfo.feedId)
			);

			if (!pythnetPriceFeed || !onChainPriceFeed) {
				logger.info(`Missing price feed data for ${feedIdCrankInfo.feedId}`);
				return;
			}

			const pythnetPriceData = pythnetPriceFeed.getPriceUnchecked();
			const onChainPriceData =
				this.pythOracleClient.getOraclePriceDataFromBuffer(result.data);
			const onChainSlot = onChainPriceData.slot.toNumber();
			const slotDiff = latestSlot - onChainSlot;

			const isSlotStale = slotDiff > this.slotStalenessThresholdRestart;

			const priceDiffPct =
				Math.abs(
					convertToNumber(
						convertPythPrice(
							new BN(pythnetPriceData.price),
							pythnetPriceData.expo,
							ONE
						),
						PRICE_PRECISION
					) /
						convertToNumber(onChainPriceData.price, PRICE_PRECISION) -
						1
				) * 100;
			const timestampDiff =
				pythnetPriceData.publishTime -
				onChainPriceFeed.priceMessage.publishTime.toNumber();
			if (
				timestampDiff > feedIdCrankInfo.updateConfig.timeDiffMs / 1000 ||
				priceDiffPct > feedIdCrankInfo.updateConfig.priceDiffPct ||
				(considerEarlyUpdate &&
					(timestampDiff >
						feedIdCrankInfo.earlyUpdateConfig.timeDiffMs / 1000 ||
						priceDiffPct > feedIdCrankInfo.earlyUpdateConfig.priceDiffPct))
			) {
				feedIdsToUpdate.push(feedIdCrankInfo);
				considerEarlyUpdate = true;
			} else if (
				isSlotStale &&
				this.driftClient.txSender.getTxLandRate() > this.txSuccessRateThreshold
			) {
				// Landing txs but slot is not getting updated on chain
				numFeedsSignalingRestart++;
			}
		});

		logger.info(
			`Feed ids to update: ${feedIdsToUpdate.map(
				(feedIdToUpdate) => feedIdToUpdate.baseSymbol
			)}`
		);

		// Pair up the feed ids to fetch vaa and update
		const feedIdPairs = chunks(feedIdsToUpdate, 2);
		await Promise.all(
			feedIdPairs.map(async (feedIds) => {
				const vaa = await this.getVaaForPriceFeedIds(
					feedIds.map((f) => f.feedId)
				);
				const ixs = [
					ComputeBudgetProgram.setComputeUnitLimit({
						units: 1_400_000,
					}),
				];
				if (this.globalConfig.useJito) {
					ixs.push(
						...(await this.driftClient.getPostPythPullOracleUpdateAtomicIxs(
							vaa,
							feedIds.map((f) => f.feedId)
						))
					);
					ixs.push(this.bundleSender!.getTipIx());
					const simResult = await simulateAndGetTxWithCUs({
						ixs,
						connection: this.driftClient.connection,
						payerPublicKey: this.driftClient.wallet.publicKey,
						lookupTableAccounts: this.lookupTableAccounts,
						cuLimitMultiplier: SIM_CU_ESTIMATE_MULTIPLIER,
						doSimulation: true,
						recentBlockhash: await this.getBlockhashForTx(),
					});
					simResult.tx.sign([
						// @ts-ignore
						this.driftClient.wallet.payer,
					]);
					this.bundleSender?.sendTransactions(
						[simResult.tx],
						undefined,
						undefined,
						false
					);
				} else {
					const priorityFees = Math.floor(
						(this.priorityFeeSubscriber?.getCustomStrategyResult() || 0) *
							this.driftClient.txSender.getSuggestedPriorityFeeMultiplier()
					);
					logger.info(
						`Priority fees to use: ${priorityFees} with multiplier: ${this.driftClient.txSender.getSuggestedPriorityFeeMultiplier()}`
					);
					ixs.push(
						ComputeBudgetProgram.setComputeUnitPrice({
							microLamports: priorityFees,
						})
					);
					ixs.push(
						...(await this.driftClient.getPostPythPullOracleUpdateAtomicIxs(
							vaa,
							feedIds.map((f) => f.feedId)
						))
					);
					const simResult = await simulateAndGetTxWithCUs({
						ixs,
						connection: this.driftClient.connection,
						payerPublicKey: this.driftClient.wallet.publicKey,
						lookupTableAccounts: this.lookupTableAccounts,
						cuLimitMultiplier: SIM_CU_ESTIMATE_MULTIPLIER,
						doSimulation: true,
						recentBlockhash: await this.getBlockhashForTx(),
					});
					const startTime = Date.now();
					this.driftClient
						.sendTransaction(simResult.tx)
						.then((txSigAndSlot: TxSigAndSlot) => {
							logger.info(
								`Posted multi pyth pull oracle for ${feedIds.map(
									(feedId) => feedId.baseSymbol
								)} update atomic tx: ${txSigAndSlot.txSig}, took ${
									Date.now() - startTime
								}ms`
							);
						})
						.catch((e) => {
							console.log(e);
						});
				}
			})
		);

		/*
			If number of numFeedsWithStaleOnChainDataButNoPriceChange is too high,
			may need to restart pythnet because we aren't actually updating the price on chain
		*/
		logger.info(
			`Number of feeds with stale on chain data but no price change: ${numFeedsSignalingRestart}`
		);
		logger.info(
			`Tx success rate: ${this.driftClient.txSender.getTxLandRate()}`
		);
		if (numFeedsSignalingRestart > 2) {
			logger.info(
				`Number of feeds with stale on chain data but no price change is too high: ${numFeedsSignalingRestart}. Tx land rate: ${this.driftClient.txSender.getTxLandRate()}. Marking unhealthy`
			);
			this.health = false;
		}
	}

	async healthCheck(): Promise<boolean> {
		return this.health;
	}
}

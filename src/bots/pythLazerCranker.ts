import { Bot } from '../types';
import { logger } from '../logger';
import { GlobalConfig, PythLazerCrankerBotConfig } from '../config';
import { PriceUpdateAccount } from '@pythnetwork/pyth-solana-receiver/lib/PythSolanaReceiver';
import {
	BlockhashSubscriber,
	DevnetPerpMarkets,
	DevnetSpotMarkets,
	DriftClient,
	getPythLazerOraclePublicKey,
	getVariant,
	MainnetPerpMarkets,
	MainnetSpotMarkets,
	PriorityFeeSubscriber,
	TxSigAndSlot,
} from '@drift-labs/sdk';
import {
	AddressLookupTableAccount,
	ComputeBudgetProgram,
} from '@solana/web3.js';
import {
	chunks,
	getVersionedTransaction,
	simulateAndGetTxWithCUs,
	sleepMs,
} from '../utils';
import { Agent, setGlobalDispatcher } from 'undici';
import { PythLazerSubscriber } from '../pythLazerSubscriber';

setGlobalDispatcher(
	new Agent({
		connections: 200,
	})
);

const SIM_CU_ESTIMATE_MULTIPLIER = 1.5;
const DEFAULT_INTEVAL_MS = 30000;

export class PythLazerCrankerBot implements Bot {
	private pythLazerSubscribers: PythLazerSubscriber[] = [];
	readonly decodeFunc: (name: string, data: Buffer) => PriceUpdateAccount;

	public name: string;
	public dryRun: boolean;
	public defaultIntervalMs?;

	private blockhashSubscriber: BlockhashSubscriber;
	private health: boolean = true;

	constructor(
		private globalConfig: GlobalConfig,
		private crankConfigs: PythLazerCrankerBotConfig,
		private driftClient: DriftClient,
		private priorityFeeSubscriber?: PriorityFeeSubscriber,
		private lookupTableAccounts: AddressLookupTableAccount[] = []
	) {
		this.name = crankConfigs.botId;
		this.dryRun = crankConfigs.dryRun;
		this.defaultIntervalMs = crankConfigs.intervalMs ?? DEFAULT_INTEVAL_MS;

		if (this.globalConfig.useJito) {
			throw new Error('Jito is not supported for pyth lazer cranker');
		}

		let allFeedIds: number[];
		if (!this.crankConfigs.pythLazerIds) {
			const spotMarkets =
				this.globalConfig.driftEnv === 'mainnet-beta'
					? MainnetSpotMarkets
					: DevnetSpotMarkets;
			const perpMarkets =
				this.globalConfig.driftEnv === 'mainnet-beta'
					? MainnetPerpMarkets
					: DevnetPerpMarkets;

			const feeds = [];
			for (const market of [...spotMarkets, ...perpMarkets]) {
				if (
					(this.crankConfigs.onlyCrankUsedOracles &&
						!getVariant(market.oracleSource).toLowerCase().includes('lazer')) ||
					market.pythLazerId == undefined
				)
					continue;
				if (
					this.crankConfigs.ignorePythLazerIds?.includes(market.pythLazerId)
				) {
					continue;
				}
				feeds.push(market.pythLazerId);
			}
			allFeedIds = Array.from(new Set(feeds));
		} else {
			allFeedIds = Array.from(new Set(this.crankConfigs.pythLazerIds));
		}

		const realTimeFeedIds = new Set(
			this.crankConfigs.pythLazerRealTimeFeedIds ?? []
		);
		const defaultChannelFeeds: number[] = [];
		const realTimeChannelFeeds: number[] = [];

		for (const feedId of allFeedIds) {
			if (realTimeFeedIds.has(feedId)) {
				realTimeChannelFeeds.push(feedId);
			} else {
				defaultChannelFeeds.push(feedId);
			}
		}

		if (!this.globalConfig.lazerEndpoints || !this.globalConfig.lazerToken) {
			throw new Error('Missing lazerEndpoint or lazerToken in global config');
		}

		if (realTimeChannelFeeds.length > 0) {
			const feedIdChunks = chunks(realTimeChannelFeeds, 11);
			logger.info(
				`Creating real_time subscriber for feeds: ${realTimeChannelFeeds.join(
					', '
				)}`
			);
			const subscriber = new PythLazerSubscriber(
				this.globalConfig.lazerEndpoints,
				this.globalConfig.lazerToken,
				feedIdChunks,
				this.globalConfig.driftEnv,
				undefined,
				undefined,
				undefined,
				'real_time'
			);
			this.pythLazerSubscribers.push(subscriber);
		}

		if (defaultChannelFeeds.length > 0) {
			const feedIdChunks = chunks(defaultChannelFeeds, 11);
			const channel = this.crankConfigs.pythLazerChannel ?? 'fixed_rate@200ms';
			logger.info(
				`Creating ${channel} subscriber for feeds: ${defaultChannelFeeds.join(
					', '
				)}`
			);
			const subscriber = new PythLazerSubscriber(
				this.globalConfig.lazerEndpoints,
				this.globalConfig.lazerToken,
				feedIdChunks,
				this.globalConfig.driftEnv,
				undefined,
				undefined,
				undefined,
				channel
			);
			this.pythLazerSubscribers.push(subscriber);
		}

		this.decodeFunc =
			this.driftClient.program.account.pythLazerOracle.coder.accounts.decodeUnchecked.bind(
				this.driftClient.program.account.pythLazerOracle.coder.accounts
			);

		this.blockhashSubscriber = new BlockhashSubscriber({
			connection: driftClient.connection,
		});
	}

	async init(): Promise<void> {
		logger.info(`Initializing ${this.name} bot`);
		await this.blockhashSubscriber.subscribe();
		this.lookupTableAccounts.push(
			...(await this.driftClient.fetchAllLookupTableAccounts())
		);

		for (const subscriber of this.pythLazerSubscribers) {
			await subscriber.subscribe();
			this.priorityFeeSubscriber?.updateAddresses(
				subscriber.allSubscribedIds.map((feedId) =>
					getPythLazerOraclePublicKey(
						this.driftClient.program.programId,
						Number(feedId)
					)
				)
			);
		}
	}

	async reset(): Promise<void> {
		logger.info(`Resetting ${this.name} bot`);
		this.blockhashSubscriber.unsubscribe();
		await this.driftClient.unsubscribe();
		for (const subscriber of this.pythLazerSubscribers) {
			subscriber.unsubscribe();
		}
	}

	async startIntervalLoop(intervalMs = this.defaultIntervalMs): Promise<void> {
		logger.info(`Starting ${this.name} bot with interval ${intervalMs} ms`);
		await sleepMs(5000);
		await this.runCrankLoop();

		setInterval(async () => {
			await this.runCrankLoop();
		}, intervalMs);
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
		for (const subscriber of this.pythLazerSubscribers) {
			for (const [
				feedIdsStr,
				priceMessage,
			] of subscriber.feedIdChunkToPriceMessage.entries()) {
				const feedIds = subscriber.getPriceFeedIdsFromHash(feedIdsStr);
				const ixs = [
					ComputeBudgetProgram.setComputeUnitLimit({
						units: 30_000,
					}),
				];
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
				const pythLazerIxs =
					await this.driftClient.getPostPythLazerOracleUpdateIxs(
						feedIds,
						priceMessage,
						ixs
					);
				ixs.push(...pythLazerIxs);

				if (!this.crankConfigs.skipSimulation) {
					ixs[0] = ComputeBudgetProgram.setComputeUnitLimit({
						units: 1_400_000,
					});
					const simResult = await simulateAndGetTxWithCUs({
						ixs,
						connection: this.driftClient.connection,
						payerPublicKey: this.driftClient.wallet.publicKey,
						lookupTableAccounts: this.lookupTableAccounts,
						cuLimitMultiplier: SIM_CU_ESTIMATE_MULTIPLIER,
						doSimulation: true,
						recentBlockhash: await this.getBlockhashForTx(),
					});
					if (simResult.simError) {
						logger.error(
							`Error simulating pyth lazer oracles for ${feedIds}: ${simResult.simTxLogs}`
						);
						continue;
					}
					const startTime = Date.now();
					this.driftClient
						.sendTransaction(simResult.tx)
						.then((txSigAndSlot: TxSigAndSlot) => {
							logger.info(
								`Posted pyth lazer oracles for ${feedIds} update atomic tx: ${
									txSigAndSlot.txSig
								}, took ${Date.now() - startTime}ms`
							);
						})
						.catch((e) => {
							console.log(e);
						});
				} else {
					const startTime = Date.now();
					const tx = getVersionedTransaction(
						this.driftClient.wallet.publicKey,
						ixs,
						this.lookupTableAccounts,
						await this.getBlockhashForTx()
					);
					this.driftClient
						.sendTransaction(tx)
						.then((txSigAndSlot: TxSigAndSlot) => {
							logger.info(
								`Posted pyth lazer oracles for ${feedIds} update atomic tx: ${
									txSigAndSlot.txSig
								}, took ${Date.now() - startTime}ms`
							);
						})
						.catch((e) => {
							console.log(e);
						});
				}
			}
		}
	}

	async healthCheck(): Promise<boolean> {
		return this.health;
	}
}

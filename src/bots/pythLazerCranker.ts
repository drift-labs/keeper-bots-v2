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
import {
	PythLazerPriceFeedArray,
	PythLazerSubscriber,
} from '../pythLazerSubscriber';
import { Channel } from '@pythnetwork/pyth-lazer-sdk';
import { TxRecorder } from './common/txRecorder';

setGlobalDispatcher(
	new Agent({
		connections: 200,
	})
);

const SIM_CU_ESTIMATE_MULTIPLIER = 1.5;
const DEFAULT_INTEVAL_MS = 30000;

export class PythLazerCrankerBot implements Bot {
	private pythLazerClient: PythLazerSubscriber;
	readonly decodeFunc: (name: string, data: Buffer) => PriceUpdateAccount;

	public name: string;
	public dryRun: boolean;
	public defaultIntervalMs?;

	private blockhashSubscriber: BlockhashSubscriber;
	private health: boolean = true;
	// Metrics
	private txRecorder: TxRecorder;

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

		let feedIdChunks: PythLazerPriceFeedArray[] = [];
		if (
			!this.crankConfigs.pythLazerIds &&
			!this.crankConfigs.pythLazerIdsByChannel
		) {
			const spotMarkets =
				this.globalConfig.driftEnv === 'mainnet-beta'
					? MainnetSpotMarkets
					: DevnetSpotMarkets;
			const perpMarkets =
				this.globalConfig.driftEnv === 'mainnet-beta'
					? MainnetPerpMarkets
					: DevnetPerpMarkets;

			const allFeedIds: number[] = [];
			for (const market of [...spotMarkets, ...perpMarkets]) {
				if (
					(this.crankConfigs.onlyCrankUsedOracles &&
						!getVariant(market.oracleSource).toLowerCase().includes('lazer')) ||
					market.pythLazerId == undefined
				)
					continue;
				if (
					this.crankConfigs.ignorePythLazerIds?.includes(market.pythLazerId!)
				) {
					continue;
				}
				allFeedIds.push(market.pythLazerId!);
			}
			const allFeedIdsSet = new Set(allFeedIds);
			feedIdChunks = chunks(Array.from(allFeedIdsSet), 11).map((ids) => {
				return {
					priceFeedIds: ids,
					channel: 'fixed_rate@200ms',
				};
			});
		} else if (this.crankConfigs.pythLazerIdsByChannel) {
			for (const key of Object.keys(
				this.crankConfigs.pythLazerIdsByChannel
			) as Channel[]) {
				const ids = this.crankConfigs.pythLazerIdsByChannel[key];
				if (!ids || ids.length === 0) {
					continue;
				}
				feedIdChunks.push({
					priceFeedIds: ids,
					channel: key as Channel,
				});
			}
		} else {
			feedIdChunks = chunks(
				Array.from(this.crankConfigs.pythLazerIds!),
				11
			).map((ids) => {
				return {
					priceFeedIds: ids,
					channel: 'real_time',
				};
			});
		}
		console.log(feedIdChunks);

		if (!this.globalConfig.lazerEndpoints || !this.globalConfig.lazerToken) {
			throw new Error('Missing lazerEndpoint or lazerToken in global config');
		}

		console.log(this.crankConfigs.pythLazerChannel);
		this.pythLazerClient = new PythLazerSubscriber(
			this.globalConfig.lazerEndpoints,
			this.globalConfig.lazerToken,
			feedIdChunks,
			this.globalConfig.driftEnv,
			undefined,
			undefined,
			undefined
		);
		this.decodeFunc =
			this.driftClient.program.account.pythLazerOracle.coder.accounts.decodeUnchecked.bind(
				this.driftClient.program.account.pythLazerOracle.coder.accounts
			);

		this.blockhashSubscriber = new BlockhashSubscriber({
			connection: driftClient.connection,
		});

		this.txRecorder = new TxRecorder(
			this.name,
			crankConfigs.metricsPort,
			false,
			20_000
		);
	}

	async init(): Promise<void> {
		logger.info(`Initializing ${this.name} bot`);
		await this.blockhashSubscriber.subscribe();
		this.lookupTableAccounts.push(
			...(await this.driftClient.fetchAllLookupTableAccounts())
		);

		await this.pythLazerClient.subscribe();

		this.priorityFeeSubscriber?.updateAddresses(
			this.pythLazerClient.allSubscribedIds.map((feedId) =>
				getPythLazerOraclePublicKey(
					this.driftClient.program.programId,
					Number(feedId)
				)
			)
		);
	}

	async reset(): Promise<void> {
		logger.info(`Resetting ${this.name} bot`);
		this.blockhashSubscriber.unsubscribe();
		await this.driftClient.unsubscribe();
		this.pythLazerClient.unsubscribe();
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
		for (const [
			feedIdsStr,
			priceMessage,
		] of this.pythLazerClient.feedIdChunkToPriceMessage.entries()) {
			const feedIds = this.pythLazerClient.getPriceFeedIdsFromHash(feedIdsStr);
			const cus = Math.max(0, feedIds.length - 3) * 6_000 + 30_000;
			const ixs = [
				ComputeBudgetProgram.setComputeUnitLimit({
					units: cus,
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
						const duration = Date.now() - startTime;
						this.txRecorder.send(duration);
						logger.info(
							`Posted pyth lazer oracles for ${feedIds} update atomic tx: ${txSigAndSlot.txSig}, took ${duration}ms, skippedSim: false`
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
						const duration = Date.now() - startTime;
						this.txRecorder.send(duration);
						logger.info(
							`Posted pyth lazer oracles for ${feedIds} update atomic tx: ${txSigAndSlot.txSig}, took ${duration}ms, skippedSim: true`
						);
					})
					.catch((e) => {
						console.log(e);
					});
			}
		}
	}

	async healthCheck(): Promise<boolean> {
		const txRecorderHealthy = this.txRecorder.isHealthy();
		if (!txRecorderHealthy) {
			logger.warn(`${this.name} bot tx recorder is unhealthy`);
		}
		this.health = txRecorderHealthy;
		return this.health;
	}
}

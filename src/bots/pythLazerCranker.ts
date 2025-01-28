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
import { BundleSender } from '../bundleSender';
import {
	AddressLookupTableAccount,
	ComputeBudgetProgram,
} from '@solana/web3.js';
import { chunks, simulateAndGetTxWithCUs, sleepMs } from '../utils';
import { Agent, setGlobalDispatcher } from 'undici';
import { PythLazerSubscriber } from '../pythLazerSubscriber';

setGlobalDispatcher(
	new Agent({
		connections: 200,
	})
);

const SIM_CU_ESTIMATE_MULTIPLIER = 1.5;

export class PythLazerCrankerBot implements Bot {
	private pythLazerClient: PythLazerSubscriber;
	readonly decodeFunc: (name: string, data: Buffer) => PriceUpdateAccount;

	public name: string;
	public dryRun: boolean;
	private intervalMs: number;
	public defaultIntervalMs = 30_000;

	private blockhashSubscriber: BlockhashSubscriber;
	private health: boolean = true;

	constructor(
		private globalConfig: GlobalConfig,
		private crankConfigs: PythLazerCrankerBotConfig,
		private driftClient: DriftClient,
		private priorityFeeSubscriber?: PriorityFeeSubscriber,
		private bundleSender?: BundleSender,
		private lookupTableAccounts: AddressLookupTableAccount[] = []
	) {
		this.name = crankConfigs.botId;
		this.dryRun = crankConfigs.dryRun;
		this.intervalMs = crankConfigs.intervalMs;

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
			allFeedIds.push(market.pythLazerId!);
		}
		const allFeedIdsSet = new Set(allFeedIds);
		const feedIdChunks = chunks(Array.from(allFeedIdsSet), 11);
		console.log(feedIdChunks);

		if (!this.globalConfig.lazerEndpoint || !this.globalConfig.lazerToken) {
			throw new Error('Missing lazerEndpoint or lazerToken in global config');
		}
		this.pythLazerClient = new PythLazerSubscriber(
			this.globalConfig.lazerEndpoint,
			this.globalConfig.lazerToken,
			feedIdChunks
		);
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
			await this.driftClient.fetchMarketLookupTableAccount()
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

	async startIntervalLoop(intervalMs = this.intervalMs): Promise<void> {
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
			const ixs = [
				ComputeBudgetProgram.setComputeUnitLimit({
					units: 1_400_000,
				}),
			];
			if (this.globalConfig.useJito) {
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
			}
			const pythLazerIxs =
				await this.driftClient.getPostPythLazerOracleUpdateIxs(
					feedIds,
					priceMessage,
					ixs
				);
			ixs.push(...pythLazerIxs);
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
		}
	}

	async healthCheck(): Promise<boolean> {
		return this.health;
	}
}

import { Bot } from '../types';
import { logger } from '../logger';
import { GlobalConfig, SwitchboardCrankerBotConfig } from '../config';
import {
	BlockhashSubscriber,
	DriftClient,
	PriorityFeeSubscriber,
	TxSigAndSlot,
} from '@drift-labs/sdk';
import { SwitchboardOnDemandClient } from '@drift-labs/sdk/lib/oracles/switchboardOnDemandClient';
import { BundleSender } from '../bundleSender';
import {
	AddressLookupTableAccount,
	ComputeBudgetProgram,
	PublicKey,
} from '@solana/web3.js';
import { simulateAndGetTxWithCUs, sleepMs } from '../utils';
import { Agent, setGlobalDispatcher } from 'undici';

setGlobalDispatcher(
	new Agent({
		connections: 200,
	})
);

const SIM_CU_ESTIMATE_MULTIPLIER = 1.5;

export class SwitchboardCrankerBot implements Bot {
	public name: string;
	public dryRun: boolean;
	private intervalMs: number;
	private oracleClient: SwitchboardOnDemandClient;
	public defaultIntervalMs = 30_000;

	private blockhashSubscriber: BlockhashSubscriber;

	constructor(
		private globalConfig: GlobalConfig,
		private crankConfigs: SwitchboardCrankerBotConfig,
		private driftClient: DriftClient,
		private priorityFeeSubscriber?: PriorityFeeSubscriber,
		private bundleSender?: BundleSender,
		private lookupTableAccounts: AddressLookupTableAccount[] = []
	) {
		this.name = crankConfigs.botId;
		this.dryRun = crankConfigs.dryRun;
		this.intervalMs = crankConfigs.intervalMs;
		this.blockhashSubscriber = new BlockhashSubscriber({
			connection: driftClient.connection,
		});

		this.oracleClient = new SwitchboardOnDemandClient(
			this.driftClient.connection
		);
	}

	async init(): Promise<void> {
		logger.info(`Initializing ${this.name} bot`);
		await this.blockhashSubscriber.subscribe();
		this.lookupTableAccounts.push(
			await this.driftClient.fetchMarketLookupTableAccount()
		);
	}

	async reset(): Promise<void> {
		logger.info(`Resetting ${this.name} bot`);
		this.blockhashSubscriber.unsubscribe();
		await this.driftClient.unsubscribe();
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
		for (const alias in this.crankConfigs.pullFeedConfigs) {
			try {
				console.log(this.crankConfigs.pullFeedConfigs[alias].pubkey);
				const pubkey = new PublicKey(
					this.crankConfigs.pullFeedConfigs[alias].pubkey
				);
				const ixs = [
					ComputeBudgetProgram.setComputeUnitLimit({
						units: 1_400_000,
					}),
				];

				if (!this.globalConfig.useJito) {
					ixs.push(
						ComputeBudgetProgram.setComputeUnitPrice({
							microLamports: Math.floor(
								this.priorityFeeSubscriber?.getCustomStrategyResult() || 0
							),
						})
					);
				}
				const pullIx =
					await this.driftClient.getPostSwitchboardOnDemandUpdateAtomicIx(
						pubkey
					);
				if (!pullIx) {
					logger.error(`No pullIx for ${alias}`);
					continue;
				}
				ixs.push(pullIx);
				const simResult = await simulateAndGetTxWithCUs({
					ixs,
					connection: this.driftClient.connection,
					payerPublicKey: this.driftClient.wallet.publicKey,
					lookupTableAccounts: this.lookupTableAccounts,
					cuLimitMultiplier: SIM_CU_ESTIMATE_MULTIPLIER,
					doSimulation: true,
					recentBlockhash: await this.getBlockhashForTx(),
				});

				if (this.globalConfig.useJito) {
					this.bundleSender?.sendTransactions([simResult.tx]);
				} else {
					this.driftClient
						.sendTransaction(simResult.tx)
						.then((txSigAndSlot: TxSigAndSlot) => {
							logger.info(`Posted update sb atomic tx: ${txSigAndSlot.txSig}`);
						})
						.catch((e) => {
							console.log(e);
						});
				}
			} catch (e) {
				logger.error(`Error processing alias ${alias}: ${e}`);
			}
		}
	}

	async healthCheck(): Promise<boolean> {
		return true;
	}
}

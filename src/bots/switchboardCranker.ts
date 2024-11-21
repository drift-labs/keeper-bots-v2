import { Bot } from '../types';
import { logger } from '../logger';
import { GlobalConfig, SwitchboardCrankerBotConfig } from '../config';
import {
	BlockhashSubscriber,
	DriftClient,
	PriorityFeeSubscriber,
	SlothashSubscriber,
	TxSigAndSlot,
} from '@drift-labs/sdk';
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
	public defaultIntervalMs: number;

	private blockhashSubscriber: BlockhashSubscriber;
	private slothashSubscriber: SlothashSubscriber;

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
		this.defaultIntervalMs = crankConfigs.intervalMs || 10_000;
		this.blockhashSubscriber = new BlockhashSubscriber({
			connection: driftClient.connection,
		});

		this.slothashSubscriber = new SlothashSubscriber(
			this.driftClient.connection,
			{
				commitment: 'confirmed',
			}
		);
	}

	async init(): Promise<void> {
		logger.info(`Initializing ${this.name} bot`);
		await this.blockhashSubscriber.subscribe();
		this.lookupTableAccounts.push(
			await this.driftClient.fetchMarketLookupTableAccount()
		);
		await this.slothashSubscriber.subscribe();

		this.priorityFeeSubscriber?.updateAddresses([
			...Object.entries(this.crankConfigs.pullFeedConfigs).map(
				([_alias, config]) => {
					return new PublicKey(config.pubkey);
				}
			),
			...this.crankConfigs.writableAccounts.map((acc) => new PublicKey(acc)),
		]);
	}

	async reset(): Promise<void> {
		logger.info(`Resetting ${this.name} bot`);
		this.blockhashSubscriber.unsubscribe();
		await this.driftClient.unsubscribe();
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
		for (const alias in this.crankConfigs.pullFeedConfigs) {
			try {
				const pubkey = new PublicKey(
					this.crankConfigs.pullFeedConfigs[alias].pubkey
				);
				const ixs = [
					ComputeBudgetProgram.setComputeUnitLimit({
						units: 1_400_000,
					}),
				];

				if (this.globalConfig.useJito) {
					ixs.push(this.bundleSender!.getTipIx());
				} else {
					const priorityFees =
						this.priorityFeeSubscriber?.getHeliusPriorityFeeLevel() || 0;
					logger.info(`Priority fee for ${alias}: ${priorityFees}`);
					ixs.push(
						ComputeBudgetProgram.setComputeUnitPrice({
							microLamports: Math.floor(priorityFees),
						})
					);
				}
				const pullIx =
					await this.driftClient.getPostSwitchboardOnDemandUpdateAtomicIx(
						pubkey,
						this.slothashSubscriber.currentSlothash
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

				if (simResult.cuEstimate < 100000) {
					logger.info(
						`cuEst: ${simResult.cuEstimate}, logs: ${JSON.stringify(
							simResult.simTxLogs
						)}`
					);
				}

				if (this.globalConfig.useJito) {
					simResult.tx.sign([
						// @ts-ignore;
						this.driftClient.wallet.payer,
					]);
					this.bundleSender?.sendTransactions(
						[simResult.tx],
						undefined,
						undefined,
						false
					);
				} else {
					this.driftClient
						.sendTransaction(simResult.tx)
						.then((txSigAndSlot: TxSigAndSlot) => {
							logger.info(
								`Posted update sb atomic tx for ${alias}: ${txSigAndSlot.txSig}`
							);
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

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
	TransactionInstruction,
} from '@solana/web3.js';
import { chunks, getVersionedTransaction, shuffle, sleepMs } from '../utils';
import { Agent, setGlobalDispatcher } from 'undici';

setGlobalDispatcher(
	new Agent({
		connections: 200,
	})
);

// ref: https://solscan.io/tx/Z5X334CFBmzbzxXHgfa49UVbMdLZf7nJdDCekjaZYinpykVqgTm47VZphazocMjYe1XJtEyeiL6QgrmvLeMesMA
const MIN_CU_LIMIT = 700_000;

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
		const pullFeedAliases = chunks(
			shuffle(Object.keys(this.crankConfigs.pullFeedConfigs)),
			2
		);
		for (const aliasChunk of pullFeedAliases) {
			try {
				const ixs = [
					ComputeBudgetProgram.setComputeUnitLimit({
						units: MIN_CU_LIMIT,
					}),
				];

				if (this.globalConfig.useJito) {
					ixs.push(this.bundleSender!.getTipIx());
				} else {
					const priorityFees =
						this.priorityFeeSubscriber?.getHeliusPriorityFeeLevel() || 0;
					ixs.push(
						ComputeBudgetProgram.setComputeUnitPrice({
							microLamports: Math.floor(priorityFees),
						})
					);
				}

				const pullIxs = (
					await Promise.all(aliasChunk.map((alias) => this.getPullIx(alias)))
				).filter((ix) => ix !== undefined) as TransactionInstruction[];
				ixs.push(...pullIxs);

				const tx = getVersionedTransaction(
					this.driftClient.wallet.publicKey,
					ixs,
					this.lookupTableAccounts,
					await this.getBlockhashForTx()
				);

				if (this.globalConfig.useJito) {
					tx.sign([
						// @ts-ignore;
						this.driftClient.wallet.payer,
					]);
					this.bundleSender?.sendTransactions(
						[tx],
						undefined,
						undefined,
						false
					);
				} else {
					this.driftClient
						.sendTransaction(tx)
						.then((txSigAndSlot: TxSigAndSlot) => {
							logger.info(
								`Posted update sb atomic tx for ${aliasChunk}: ${txSigAndSlot.txSig}`
							);
						})
						.catch((e) => {
							console.log(e);
						});
				}
			} catch (e) {
				logger.error(`Error processing alias ${aliasChunk}: ${e}`);
			}
		}
	}

	async getPullIx(alias: string): Promise<TransactionInstruction | undefined> {
		const pubkey = new PublicKey(
			this.crankConfigs.pullFeedConfigs[alias].pubkey
		);
		const pullIx =
			await this.driftClient.getPostSwitchboardOnDemandUpdateAtomicIx(
				pubkey,
				this.slothashSubscriber.currentSlothash
			);
		if (!pullIx) {
			logger.error(`No pullIx for ${alias}`);
			return;
		}
		return pullIx;
	}

	async healthCheck(): Promise<boolean> {
		return true;
	}
}

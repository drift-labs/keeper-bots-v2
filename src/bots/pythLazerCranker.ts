import { Bot } from '../types';
import { logger } from '../logger';
import { GlobalConfig, PythLazerCrankerBot } from '../config';
import { PriceUpdateAccount } from '@pythnetwork/pyth-solana-receiver/lib/PythSolanaReceiver';
import {
	BlockhashSubscriber,
	BN,
	DriftClient,
	getOracleClient,
	getPythLazerOraclePublicKey,
	OracleClient,
	OracleSource,
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
import { PythLazerClient } from '@pythnetwork/pyth-lazer-sdk';

setGlobalDispatcher(
	new Agent({
		connections: 200,
	})
);

const SIM_CU_ESTIMATE_MULTIPLIER = 1.5;

export class PythLazerCranker implements Bot {
	private wsClient: PythLazerClient;
	private pythOracleClient: OracleClient;
	readonly decodeFunc: (name: string, data: Buffer) => PriceUpdateAccount;

	public name: string;
	public dryRun: boolean;
	private intervalMs: number;
	private feedIdChunkToPriceMessage: Map<number[], string> = new Map();
	public defaultIntervalMs = 30_000;

	private blockhashSubscriber: BlockhashSubscriber;
	private health: boolean = true;
	private slotStalenessThresholdRestart: number = 300;
	private txSuccessRateThreshold: number = 0.5;

	constructor(
		private globalConfig: GlobalConfig,
		private crankConfigs: PythLazerCrankerBot,
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

		if (globalConfig.driftEnv != 'devnet') {
			throw new Error('Only devnet drift env is supported');
		}

		const hermesEndpointParts = globalConfig.hermesEndpoint.split('?token=');
		this.wsClient = new PythLazerClient(
			hermesEndpointParts[0],
			hermesEndpointParts[1]
		);

		this.pythOracleClient = getOracleClient(
			OracleSource.PYTH_LAZER,
			driftClient.connection,
			driftClient.program
		);
		this.decodeFunc =
			this.driftClient.program.account.pythLazerOracle.coder.accounts.decodeUnchecked.bind(
				this.driftClient.program.account.pythLazerOracle.coder.accounts
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

		const updateConfigs = this.crankConfigs.updateConfigs;

		let subscriptionId = 1;
		for (const configChunk of chunks(Object.keys(updateConfigs), 3)) {
			const priceFeedIds: number[] = configChunk.map((alias) => {
				return updateConfigs[alias].feedId;
			});
			this.wsClient.ws.addEventListener('open', () => {
				this.wsClient.send({
					type: 'subscribe',
					subscriptionId,
					priceFeedIds,
					properties: ['price'],
					chains: ['solana'],
					deliveryFormat: 'json',
					channel: 'fixed_rate@200ms',
					jsonBinaryEncoding: 'hex',
				});
			});
			this.wsClient.addMessageListener((message) => {
				switch (message.type) {
					case 'json': {
						if (message.value.type == 'streamUpdated') {
							if (message.value.solana?.data)
								this.feedIdChunkToPriceMessage.set(
									priceFeedIds,
									message.value.solana.data
								);
						}
						break;
					}
					case 'binary': {
						if (
							'solana' in message.value &&
							message.value.solana?.toString('hex')
						) {
							this.feedIdChunkToPriceMessage.set(
								priceFeedIds,
								message.value.solana.toString('hex')
							);
						}
						break;
					}
				}
			});
			subscriptionId++;
		}

		this.priorityFeeSubscriber?.updateAddresses(
			Object.keys(this.feedIdChunkToPriceMessage)
				.flat()
				.map((feedId) =>
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
		this.wsClient.ws.close();
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
			feedIds,
			priceMessage,
		] of this.feedIdChunkToPriceMessage.entries()) {
			const pythLazerIxs = this.driftClient.getPostPythLazerOracleUpdateIxs(
				feedIds,
				priceMessage
			);
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

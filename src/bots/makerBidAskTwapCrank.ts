import {
	DLOB,
	DriftClient,
	UserMap,
	SlotSubscriber,
	MarketType,
	PositionDirection,
	getUserStatsAccountPublicKey,
	promiseTimeout,
	isVariant,
	PriorityFeeSubscriber,
} from '@drift-labs/sdk';
import { Mutex } from 'async-mutex';

import { logger } from '../logger';
import { Bot } from '../types';
import { BaseBotConfig } from '../config';
import {
	TransactionSignature,
	VersionedTransaction,
	AddressLookupTableAccount,
	PublicKey,
	ComputeBudgetProgram,
} from '@solana/web3.js';
import { webhookMessage } from '../webhook';
import { ConfirmOptions, Signer } from '@solana/web3.js';
import { handleSimResultError, simulateAndGetTxWithCUs } from '../utils';

const CRANK_TX_MARKET_CHUNK_SIZE = 1;
const CU_EST_MULTIPLIER = 1.25;

function isCriticalError(e: Error): boolean {
	// retrying on this error is standard
	if (e.message.includes('Blockhash not found')) {
		return false;
	}

	if (e.message.includes('Transaction was not confirmed in')) {
		return false;
	}
	return true;
}

export async function sendVersionedTransaction(
	driftClient: DriftClient,
	tx: VersionedTransaction,
	additionalSigners?: Array<Signer>,
	opts?: ConfirmOptions,
	timeoutMs = 5000
): Promise<TransactionSignature | null> {
	// @ts-ignore
	tx.sign((additionalSigners ?? []).concat(driftClient.provider.wallet.payer));

	if (opts === undefined) {
		opts = driftClient.provider.opts;
	}

	const rawTransaction = tx.serialize();
	let txid: TransactionSignature | null;
	try {
		txid = await promiseTimeout(
			driftClient.provider.connection.sendRawTransaction(rawTransaction, opts),
			timeoutMs
		);
	} catch (e) {
		console.error(e);
		throw e;
	}

	return txid;
}

/// split the list into chunks
function chunkArray<T>(arr: T[], chunkSize: number): T[][] {
	const chunks: T[][] = [];
	for (let i = 0; i < arr.length; i += chunkSize) {
		chunks.push(arr.slice(i, i + chunkSize));
	}
	return chunks;
}

export class MakerBidAskTwapCrank implements Bot {
	public readonly name: string;
	public readonly dryRun: boolean;
	public readonly runOnce: boolean;
	public readonly defaultIntervalMs?: number = undefined;

	private crankIntervalToMarketIds?: { [key: number]: number[] }; // Object from number to array of numbers
	private allCrankIntervalGroups?: number[];
	private maxIntervalGroup?: number; // tracks the max interval group for health checking

	private slotSubscriber: SlotSubscriber;
	private driftClient: DriftClient;
	private intervalIds: Array<NodeJS.Timer> = [];
	private userMap?: UserMap;

	private dlob?: DLOB;
	private latestDlobSlot?: number;
	private lookupTableAccount?: AddressLookupTableAccount;
	private priorityFeeSubscriber: PriorityFeeSubscriber;

	private watchdogTimerMutex = new Mutex();
	private watchdogTimerLastPatTime = Date.now();

	constructor(
		driftClient: DriftClient,
		slotSubscriber: SlotSubscriber,
		userMap: UserMap,
		config: BaseBotConfig,
		runOnce: boolean,
		priorityFeeSubscriber: PriorityFeeSubscriber,
		crankIntervalToMarketIds?: { [key: number]: number[] }
	) {
		this.slotSubscriber = slotSubscriber;
		this.name = config.botId;
		this.dryRun = config.dryRun;
		this.runOnce = runOnce;
		this.driftClient = driftClient;
		this.userMap = userMap;
		this.crankIntervalToMarketIds = crankIntervalToMarketIds;
		if (this.crankIntervalToMarketIds) {
			this.allCrankIntervalGroups = Object.keys(
				this.crankIntervalToMarketIds
			).map((x) => parseInt(x));
			this.maxIntervalGroup = Math.max(...this.allCrankIntervalGroups!);
			this.watchdogTimerLastPatTime = Date.now() - this.maxIntervalGroup;
		}
		this.priorityFeeSubscriber = priorityFeeSubscriber;
	}

	public async init() {
		logger.info(`${this.name} initing, runOnce: ${this.runOnce}`);
		this.lookupTableAccount =
			await this.driftClient.fetchMarketLookupTableAccount();

		const perpMarkets = this.driftClient
			.getPerpMarketAccounts()
			.map((m) => m.pubkey);

		this.priorityFeeSubscriber.updateAddresses([...perpMarkets]);
	}

	public async reset() {
		for (const intervalId of this.intervalIds) {
			clearInterval(intervalId as NodeJS.Timeout);
		}
		this.intervalIds = [];

		await this.userMap?.unsubscribe();
	}

	public async startIntervalLoop(_intervalMs?: number): Promise<void> {
		logger.info(`${this.name} Bot started!`);
		if (this.runOnce) {
			await this.tryTwapCrank(null);
		} else {
			// start an interval for each crank interval
			for (const intervalGroup of this.allCrankIntervalGroups!) {
				const intervalId = setInterval(
					this.tryTwapCrank.bind(this, intervalGroup),
					intervalGroup
				);
				this.intervalIds.push(intervalId);
			}
		}
	}

	public async healthCheck(): Promise<boolean> {
		let healthy = false;
		await this.watchdogTimerMutex.runExclusive(async () => {
			healthy =
				this.watchdogTimerLastPatTime > Date.now() - 5 * this.maxIntervalGroup!;
		});
		return healthy;
	}

	private async initDlob() {
		try {
			this.latestDlobSlot = this.slotSubscriber.currentSlot;
			this.dlob = await this.userMap!.getDLOB(this.slotSubscriber.currentSlot);
		} catch (e) {
			logger.error(`Error loading dlob: ${e}`);
		}
	}

	private getCombinedList(makersArray: PublicKey[]) {
		const combinedList = [];

		for (const maker of makersArray) {
			const uA = this.userMap!.getUserAuthority(maker.toString());
			if (uA !== undefined) {
				const uStats = getUserStatsAccountPublicKey(
					this.driftClient.program.programId,
					uA
				);

				// Combine maker and uStats into a list and add it to the combinedList
				const combinedItem = [maker, uStats];
				combinedList.push(combinedItem);
			} else {
				logger.warn(
					'skipping maker... cannot find authority for userAccount=',
					maker.toString()
				);
			}
		}

		return combinedList;
	}

	private async tryTwapCrank(intervalGroup: number | null) {
		try {
			await this.initDlob();

			const state = this.driftClient.getStateAccount();

			let crankMarkets: number[] = [];
			if (intervalGroup === null) {
				crankMarkets = Array.from(
					{ length: state.numberOfMarkets },
					(_, index) => index
				);
			} else {
				crankMarkets = this.crankIntervalToMarketIds![intervalGroup]!;
			}
			logger.info(`Cranking interval group ${intervalGroup}: ${crankMarkets}`);

			const allChunks: number[][] = chunkArray(
				crankMarkets,
				CRANK_TX_MARKET_CHUNK_SIZE
			);

			for (const chunkOfMarketIndicies of allChunks) {
				const ixs = [
					ComputeBudgetProgram.setComputeUnitLimit({
						units: 1_400_000, // will be overwritten by simulateAndGetTxWithCUs
					}),
					ComputeBudgetProgram.setComputeUnitPrice({
						microLamports: Math.floor(
							this.priorityFeeSubscriber!.getCustomStrategyResult()
						),
					}),
				];
				for (let i = 0; i < chunkOfMarketIndicies.length; i++) {
					const mi = chunkOfMarketIndicies[i];

					const oraclePriceData =
						this.driftClient.getOracleDataForPerpMarket(mi);

					const bidMakers = this.dlob!.getBestMakers({
						marketIndex: mi,
						marketType: MarketType.PERP,
						direction: PositionDirection.LONG,
						slot: this.latestDlobSlot!,
						oraclePriceData,
						numMakers: 5,
					});

					const askMakers = this.dlob!.getBestMakers({
						marketIndex: mi,
						marketType: MarketType.PERP,
						direction: PositionDirection.SHORT,
						slot: this.latestDlobSlot!,
						oraclePriceData,
						numMakers: 5,
					});
					logger.info(
						`loaded makers for market ${mi}: ${bidMakers.length} bids, ${askMakers.length} asks`
					);

					const concatenatedList = [
						...this.getCombinedList(bidMakers),
						...this.getCombinedList(askMakers),
					];

					const ix = await this.driftClient.getUpdatePerpBidAskTwapIx(
						mi,
						concatenatedList as [PublicKey, PublicKey][]
					);

					if (
						isVariant(
							this.driftClient.getPerpMarketAccount(mi)!.amm.oracleSource,
							'prelaunch'
						)
					) {
						const updatePrelaunchOracleIx =
							await this.driftClient.getUpdatePrelaunchOracleIx(mi);
						ixs.push(updatePrelaunchOracleIx);
					}

					ixs.push(ix);
				}

				let success = false;
				try {
					// const txSig = await sendVersionedTransaction(
					// 	this.driftClient,
					// 	await this.driftClient.txSender.getVersionedTransaction(
					// 		ixs,
					// 		[this.lookupTableAccount!],
					// 		[],
					// 		this.driftClient.opts
					// 	),
					// 	[],
					// 	this.driftClient.opts,
					// 	5000
					// );

					const simResult = await simulateAndGetTxWithCUs(
						ixs,
						this.driftClient.connection,
						this.driftClient.txSender,
						[this.lookupTableAccount!],
						[],
						undefined,
						CU_EST_MULTIPLIER,
						true
					);
					logger.info(
						`makerBidAskTwapCrank estimated ${simResult.cuEstimate} CUs for ${ixs.length} ixs, ${chunkOfMarketIndicies.length} chunks.`
					);

					if (simResult.simError !== null) {
						logger.error(
							`Sim error: ${JSON.stringify(simResult.simError)}\n${
								simResult.simTxLogs ? simResult.simTxLogs.join('\n') : ''
							}`
						);
						handleSimResultError(simResult, [], `(makerBidAskTwapCrank)`);
						success = false;
					} else {
						const txSig =
							await this.driftClient.txSender.sendVersionedTransaction(
								simResult.tx,
								[],
								{
									...this.driftClient.opts,
								}
							);
						success = true;
						logger.info(
							`Send tx: https://solscan.io/tx/${txSig} for markets: ${JSON.stringify(
								chunkOfMarketIndicies
							)}`
						);
					}
				} catch (e: any) {
					console.error(e);
					if (isCriticalError(e as Error)) {
						await webhookMessage(
							`[${this.name}] failed to crank funding rate:\n${
								e.logs ? (e.logs as Array<string>).join('\n') : ''
							} \n${e.stack ? e.stack : e.message}`
						);
					}
				}

				if (!success && CRANK_TX_MARKET_CHUNK_SIZE > 1) {
					logger.error(
						`SIM ERROR, YOU SHOULD IMPLEMENT LOGIC TO RETRY WITH A SMALLER CHUNK SIZE NOW.`
					);
				}
			}
		} catch (e) {
			console.error(e);
			if (e instanceof Error) {
				await webhookMessage(
					`[${this.name}]: :x: uncaught error:\n${
						e.stack ? e.stack : e.message
					}`
				);
			}
		} finally {
			logger.info('tryTwapCrank finished');
			await this.watchdogTimerMutex.runExclusive(async () => {
				this.watchdogTimerLastPatTime = Date.now();
			});
		}
	}
}

import {
	DepositRecord,
	DriftClient,
	DriftEnv,
	EventSubscriber,
	SpotMarkets,
	WrappedEvent,
	isVariant,
} from '@drift-labs/sdk';
import { Mutex } from 'async-mutex';
import {
	PublicKey,
	SystemProgram,
	TransactionMessage,
	VersionedTransaction,
} from '@solana/web3.js';

import { FunXyzGasDropperConfig } from '../config';
import { logger } from '../logger';
import { Bot } from '../types';
import { isFunXyzDepositTxSignature } from './common/funXyz';

const DEFAULT_LAMPORTS_PER_DROP = 10_000;
const DEFAULT_MAX_SEEN_TX_SIGS = 10_000;

type DepositRecordEvent = WrappedEvent<'DepositRecord'>;

export class FunXyzGasDropperBot implements Bot {
	public readonly name: string;
	public readonly dryRun: boolean;
	public readonly runOnce: boolean;
	public readonly defaultIntervalMs = 60_000;

	private readonly driftClient: DriftClient;
	private readonly eventSubscriber: EventSubscriber;
	private readonly lamportsPerDrop: number;
	private readonly minAuthorityLamportsToSkip: number;
	private readonly maxSeenTxSigs: number;
	private readonly targetMarketIndexes: Set<number>;

	private readonly seenTxSigs = new Set<string>();
	private readonly processingTxSigs = new Set<string>();
	private readonly intervalIds: Array<NodeJS.Timer> = [];

	private watchdogTimerMutex = new Mutex();
	private watchdogTimerLastPatTime = Date.now();

	private readonly onNewEvent = (event: WrappedEvent<any>) => {
		void this.handleEvent(event);
	};

	constructor(
		driftClient: DriftClient,
		eventSubscriber: EventSubscriber,
		config: FunXyzGasDropperConfig,
		driftEnv: DriftEnv
	) {
		this.name = config.botId;
		this.dryRun = config.dryRun;
		this.runOnce = config.runOnce ?? false;
		this.driftClient = driftClient;
		this.eventSubscriber = eventSubscriber;
		this.lamportsPerDrop = Math.max(
			0,
			Math.floor(config.lamportsPerDrop ?? DEFAULT_LAMPORTS_PER_DROP)
		);
		this.minAuthorityLamportsToSkip = Math.max(
			0,
			Math.floor(config.minAuthorityLamportsToSkip ?? 0)
		);
		this.maxSeenTxSigs = Math.max(
			1,
			Math.floor(config.maxSeenTxSigs ?? DEFAULT_MAX_SEEN_TX_SIGS)
		);
		this.targetMarketIndexes = this.getTargetMarketIndexes(driftEnv);
	}

	public async init() {
		logger.info(
			`[${this.name}] initing with target markets=${JSON.stringify(
				Array.from(this.targetMarketIndexes)
			)}, lamportsPerDrop=${this.lamportsPerDrop}, minAuthorityLamportsToSkip=${
				this.minAuthorityLamportsToSkip
			}`
		);
		this.eventSubscriber.eventEmitter.on('newEvent', this.onNewEvent);
	}

	public async reset() {
		for (const intervalId of this.intervalIds) {
			clearInterval(intervalId as NodeJS.Timeout);
		}
		this.intervalIds.length = 0;
		this.eventSubscriber.eventEmitter.removeListener(
			'newEvent',
			this.onNewEvent
		);
	}

	public async startIntervalLoop(intervalMs?: number): Promise<void> {
		logger.info(`[${this.name}] Bot started! runOnce=${this.runOnce}`);

		if (this.runOnce) {
			return;
		}

		const intervalId = setInterval(() => {
			void this.patWatchdog();
		}, intervalMs ?? this.defaultIntervalMs);
		this.intervalIds.push(intervalId);
	}

	public async healthCheck(): Promise<boolean> {
		let healthy = false;
		await this.watchdogTimerMutex.runExclusive(async () => {
			healthy =
				this.watchdogTimerLastPatTime > Date.now() - 2 * this.defaultIntervalMs;
		});
		return healthy;
	}

	private async handleEvent(event: WrappedEvent<any>) {
		if (event.eventType !== 'DepositRecord') {
			return;
		}

		const depositEvent = event as DepositRecordEvent;
		const depositRecord = depositEvent as DepositRecordEvent & DepositRecord;

		if (!this.targetMarketIndexes.has(depositRecord.marketIndex)) {
			return;
		}
		if (!isVariant(depositRecord.direction, 'deposit')) {
			return;
		}
		if (!depositRecord.txSig || !depositRecord.userAuthority) {
			return;
		}
		if (
			this.seenTxSigs.has(depositRecord.txSig) ||
			this.processingTxSigs.has(depositRecord.txSig)
		) {
			return;
		}

		this.processingTxSigs.add(depositRecord.txSig);
		try {
			const isFunXyz = await isFunXyzDepositTxSignature(
				this.driftClient.connection,
				depositRecord.txSig
			);
			if (!isFunXyz) {
				return;
			}

			const recipient = new PublicKey(depositRecord.userAuthority);
			if (recipient.equals(this.driftClient.wallet.publicKey)) {
				logger.info(
					`[${this.name}] skipping tx=${depositRecord.txSig} because recipient is keeper wallet`
				);
				return;
			}

			if (this.minAuthorityLamportsToSkip > 0) {
				const recipientBalance = await this.driftClient.connection.getBalance(
					recipient,
					'confirmed'
				);
				if (recipientBalance >= this.minAuthorityLamportsToSkip) {
					logger.info(
						`[${this.name}] skipping tx=${
							depositRecord.txSig
						} for ${recipient.toBase58()} because balance=${recipientBalance} >= minAuthorityLamportsToSkip=${
							this.minAuthorityLamportsToSkip
						}`
					);
					return;
				}
			}

			if (this.dryRun) {
				logger.info(
					`[${this.name}] dry run: would send ${
						this.lamportsPerDrop
					} lamports to ${recipient.toBase58()} for tx=${depositRecord.txSig}`
				);
				return;
			}

			const latestBlockhash =
				await this.driftClient.connection.getLatestBlockhash('confirmed');
			const ix = SystemProgram.transfer({
				fromPubkey: this.driftClient.wallet.publicKey,
				toPubkey: recipient,
				lamports: this.lamportsPerDrop,
			});
			const message = new TransactionMessage({
				payerKey: this.driftClient.wallet.publicKey,
				recentBlockhash: latestBlockhash.blockhash,
				instructions: [ix],
			}).compileToV0Message();
			const tx = new VersionedTransaction(message);
			const txSig = await this.driftClient.txSender.sendVersionedTransaction(
				tx,
				[],
				this.driftClient.opts
			);

			logger.info(
				`[${this.name}] sent ${
					this.lamportsPerDrop
				} lamports to ${recipient.toBase58()} for deposit tx=${
					depositRecord.txSig
				}: https://solana.fm/tx/${txSig.txSig}`
			);
		} catch (e) {
			logger.error(
				`[${this.name}] failed handling deposit tx=${depositRecord.txSig}: ${e}`
			);
		} finally {
			this.processingTxSigs.delete(depositRecord.txSig);
			this.rememberTxSig(depositRecord.txSig);
			await this.patWatchdog();
		}
	}

	private getTargetMarketIndexes(driftEnv: DriftEnv): Set<number> {
		const marketIndexes = SpotMarkets[driftEnv]
			.filter((market) => market.symbol === 'USDC' || market.symbol === 'SOL')
			.map((market) => market.marketIndex);
		return new Set(marketIndexes);
	}

	private rememberTxSig(txSig: string) {
		this.seenTxSigs.add(txSig);
		if (this.seenTxSigs.size <= this.maxSeenTxSigs) {
			return;
		}

		const oldest = this.seenTxSigs.values().next().value;
		if (oldest) {
			this.seenTxSigs.delete(oldest);
		}
	}

	private async patWatchdog() {
		await this.watchdogTimerMutex.runExclusive(async () => {
			this.watchdogTimerLastPatTime = Date.now();
		});
	}
}

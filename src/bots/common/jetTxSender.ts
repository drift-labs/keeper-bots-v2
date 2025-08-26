import {
	ConfirmOptions,
	Connection,
	SendTransactionError,
} from '@solana/web3.js';
import { WhileValidTxSender } from '@drift-labs/sdk';

export class JetProxyTxSender extends WhileValidTxSender {
	private submitConnections: Connection[];

	constructor(
		args: ConstructorParameters<typeof WhileValidTxSender>[0] & {
			submitConnections?: Connection[];
		}
	) {
		const { submitConnections, ...baseArgs } = args;

		super(baseArgs);

		if (submitConnections && submitConnections.length > 0) {
			this.submitConnections = submitConnections;
		} else {
			throw new Error(
				'JetProxySender requires submitConnections or submitConnection'
			);
		}
	}

	private async sendToSubmitConnections(
		rawTransaction: Buffer | Uint8Array,
		opts: ConfirmOptions
	): Promise<string[]> {
		const promises = this.submitConnections.map(async (connection) => {
			try {
				return await connection.sendRawTransaction(rawTransaction, opts);
			} catch (error) {
				console.error(
					'Failed to send transaction to submit connection:',
					error
				);
				throw error;
			}
		});

		const results = await Promise.allSettled(promises);
		const txids: string[] = [];

		for (const result of results) {
			if (result.status === 'fulfilled') {
				txids.push(result.value);
			}
		}

		if (txids.length === 0) {
			throw new Error('Failed to send transaction to any submit connection');
		}

		return txids;
	}

	private async retrySubmitConnections(
		rawTransaction: Buffer | Uint8Array,
		opts: ConfirmOptions
	): Promise<void> {
		const promises = this.submitConnections.map(async (connection) => {
			try {
				await connection.sendRawTransaction(rawTransaction, opts);
			} catch (error) {
				console.error('Retry failed for submit connection:', error);
			}
		});

		Promise.allSettled(promises);
	}

	async sendRawTransaction(
		rawTransaction: Buffer | Uint8Array,
		opts: ConfirmOptions
	) {
		const startTime = this.getTimestamp();

		const txids = await this.sendToSubmitConnections(rawTransaction, opts);
		const primaryTxid = txids[0];

		this.txSigCache?.set(primaryTxid, false);
		this.sendToAdditionalConnections(rawTransaction, opts);

		let done = false;
		const resolveReference: { resolve?: () => void } = {};
		const stopWaiting = () => {
			done = true;
			resolveReference.resolve?.();
		};

		(async () => {
			while (!done && this.getTimestamp() - startTime < this.timeout) {
				await new Promise<void>((resolve) => {
					resolveReference.resolve = resolve;
					setTimeout(resolve, this.retrySleep);
				});
				if (!done) {
					try {
						await this.retrySubmitConnections(rawTransaction, opts);
						this.sendToAdditionalConnections(rawTransaction, opts);
					} catch (e) {
						console.error(e);
						stopWaiting();
					}
				}
			}
		})();

		let slot: number;
		try {
			const result = await this.confirmTransaction(
				primaryTxid,
				opts.commitment
			);

			this.txSigCache?.set(primaryTxid, true);
			await this.checkConfirmationResultForError(primaryTxid, result?.value);

			if (result?.value?.err && this.throwOnTransactionError) {
				throw new SendTransactionError(`Transaction Failed`, [primaryTxid]);
			}

			slot = result?.context?.slot;
		} finally {
			stopWaiting();
		}

		return { txSig: primaryTxid, slot };
	}
}

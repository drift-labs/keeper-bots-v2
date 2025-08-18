import {
	ConfirmOptions,
	Connection,
	SendTransactionError,
} from '@solana/web3.js';
import { WhileValidTxSender } from '@drift-labs/sdk';

export class JetProxyTxSender extends WhileValidTxSender {
	private submitConnection: Connection;

	constructor(
		args: ConstructorParameters<typeof WhileValidTxSender>[0] & {
			submitConnection?: Connection;
		}
	) {
		const { submitConnection, ...baseArgs } = args;

		super(baseArgs);

		if (submitConnection) {
			this.submitConnection = submitConnection;
		} else {
			throw new Error('JetProxySender requires submitConnection');
		}
	}

	async sendRawTransaction(
		rawTransaction: Buffer | Uint8Array,
		opts: ConfirmOptions
	) {
		const startTime = this.getTimestamp();

		const txid = await this.submitConnection.sendRawTransaction(
			rawTransaction,
			opts
		);

		this.txSigCache?.set(txid, false);
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
					this.submitConnection
						.sendRawTransaction(rawTransaction, opts)
						.catch((e) => {
							console.error(e);
							stopWaiting();
						});
					this.sendToAdditionalConnections(rawTransaction, opts);
				}
			}
		})();

		let slot: number;
		try {
			const result = await this.confirmTransaction(txid, opts.commitment);

			this.txSigCache?.set(txid, true);
			await this.checkConfirmationResultForError(txid, result?.value);

			if (result?.value?.err && this.throwOnTransactionError) {
				throw new SendTransactionError(`Transaction Failed`, [txid]);
			}

			slot = result?.context?.slot;
		} finally {
			stopWaiting();
		}

		return { txSig: txid, slot };
	}
}

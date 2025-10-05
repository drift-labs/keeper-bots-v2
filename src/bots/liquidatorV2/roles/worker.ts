import {
	AddressLookupTableAccount,
	VersionedTransaction,
	Connection,
} from '@solana/web3.js';
import { logger } from '../../../logger';
import { GROUP_WORKERS, Queue, STREAM_TXS, TxJob } from '../redis/queue';

export class WorkerService {
	private readonly connection: Connection;
	constructor(
		private readonly queue: Queue,
		private readonly endpoint: string
	) {
		this.connection = new Connection(this.endpoint);
	}

	async init() {
		await this.queue.connect();
		await this.queue.ensureStreamAndGroup(STREAM_TXS, GROUP_WORKERS);
		logger.info('Worker initialized');
	}

	async runOnce(consumerId: string) {
		const jobs = await this.queue.readTxJobs(
			STREAM_TXS,
			GROUP_WORKERS,
			consumerId,
			8,
			2000
		);
		if (jobs.length === 0) return;
		for (const j of jobs) {
			try {
				const data: TxJob = j.data;
				const tx = VersionedTransaction.deserialize(
					Uint8Array.from(Buffer.from(data.versionedTxBase64, 'base64'))
				);
				const sim = await this.connection.simulateTransaction(tx, {
					replaceRecentBlockhash: true,
				});
				logger.info(
					`[SIM-ONLY] job=${data.idemKey} user=${data.userPubkey} action=${
						data.action
					} market=${data.marketIndex} cu=${data.cuLimit} sim=${
						sim.value.unitsConsumed
					} simlogs=${JSON.stringify(sim.value.logs)}`
				);

				// const raw = Buffer.from(data.versionedTxBase64, 'base64');
				// const tx = VersionedTransaction.deserialize(raw);
				// logger.info(
				// 	`[SIM-ONLY] job=${data.idemKey} user=${data.userPubkey} action=${data.action} market=${data.marketIndex} cu=${data.cuLimit}`
				// );
				// // We do not send yet; just log size/info
				// logger.info(`tx_size=${tx.message.serialize().length} accounts=${tx.message.staticAccountKeys.length}`);

				await this.queue.ack(STREAM_TXS, GROUP_WORKERS, [j.id]);
			} catch (e) {
				logger.error(`worker error on job ${j.id}: ${(e as Error).message}`);
			}
		}
	}
}

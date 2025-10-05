import { createClient, RedisClientType } from 'redis';

export type StreamRecord<T> = { id: string; data: T };

export type Candidate = {
	userPubkey: string;
	authority: string;
	affectedMarkets: { spot: number[]; perp: number[] };
	reason: 'price-move' | 'periodic-scan';
	slot: number;
	ts: number;
	idemKey: string;
};

export type TxJob = {
	idemKey: string;
	userPubkey: string;
	action: 'perp' | 'perp-pnl' | 'spot';
	marketIndex: number | null;
	// base64 serialized versioned transaction built with placeholder blockhash
	versionedTxBase64: string;
	cuLimit: number;
	cuPriceMicroLamports: number;
	sendOpts?: { skipPreflight?: boolean; maxRetries?: number };
	telemetry: { slot: number; ts: number };
};

export class Queue {
	public readonly client: RedisClientType;
	constructor(url: string) {
		this.client = createClient({ url });
	}

	async connect() {
		if (!this.client.isOpen) await this.client.connect();
	}

	async ensureStreamAndGroup(stream: string, group: string) {
		try {
			await this.client.xGroupCreate(stream, group, '$', { MKSTREAM: true });
		} catch (e: any) {
			if (!String(e?.message || '').includes('BUSYGROUP')) throw e;
		}
	}

	async addCandidate(stream: string, data: Candidate): Promise<string> {
		return await this.client.xAdd(stream, '*', { data: JSON.stringify(data) });
	}

	async readCandidates(
		stream: string,
		group: string,
		consumer: string,
		count = 16,
		blockMs = 2000
	): Promise<StreamRecord<Candidate>[]> {
		const res = await this.client.xReadGroup(
			group,
			consumer,
			[{ key: stream, id: '>' }],
			{ COUNT: count, BLOCK: blockMs }
		);
		if (!res) return [];
		const out: StreamRecord<Candidate>[] = [];
		for (const s of res) {
			for (const m of s.messages) {
				const d = JSON.parse(m.message.data as string);
				out.push({ id: m.id, data: d });
			}
		}
		return out;
	}

	async ack(stream: string, group: string, ids: string[]) {
		if (ids.length === 0) return 0;
		return await this.client.xAck(stream, group, ids);
	}

	async addTxJob(stream: string, data: TxJob): Promise<string> {
		return await this.client.xAdd(stream, '*', { data: JSON.stringify(data) });
	}

	async readTxJobs(
		stream: string,
		group: string,
		consumer: string,
		count = 16,
		blockMs = 2000
	): Promise<StreamRecord<TxJob>[]> {
		const res = await this.client.xReadGroup(
			group,
			consumer,
			[{ key: stream, id: '>' }],
			{ COUNT: count, BLOCK: blockMs }
		);
		if (!res) return [];
		const out: StreamRecord<TxJob>[] = [];
		for (const s of res) {
			for (const m of s.messages) {
				const d = JSON.parse(m.message.data as string);
				out.push({ id: m.id, data: d });
			}
		}
		return out;
	}
}

export const STREAM_CANDIDATES = 'liq:candidates';
export const STREAM_TXS = 'liq:txs';
export const GROUP_COORD = 'coord';
export const GROUP_WORKERS = 'workers';

/* eslint-disable @typescript-eslint/no-non-null-assertion */
import { DriftClient, UserMap, initialize, configs } from '@drift-labs/sdk';
import {
	Connection,
	PublicKey,
	TransactionVersion,
	ConfirmOptions,
} from '@solana/web3.js';
import { getWallet } from '../../utils';
import { logger } from '../../logger';
import { CoordinatorService } from './roles/coordinator';
import { WorkerService } from './roles/worker';
import { Queue } from './redis/queue';

type GlobalCfg = {
	endpoint: string;
	wsEndpoint?: string;
	driftEnv: 'devnet' | 'mainnet-beta';
	keeperPrivateKey: string;
};

export async function startLiquidatorV2(global: GlobalCfg) {
	const role = process.env.ROLE || 'leader';
	const redisUrl = process.env.REDIS_URL || 'redis://127.0.0.1:6379';
	const queue = new Queue(redisUrl);

	if (role === 'worker') {
		const worker = new WorkerService(queue, global.endpoint);
		await worker.init();
		const consumerId = `worker-${Math.random().toString(36).slice(2, 8)}`;
		logger.info(`Worker starting: ${consumerId}`);
		// single pass runner for now
		setInterval(() => worker.runOnce(consumerId), 1500);
		return;
	}

	// coordinator path
	logger.info(`Coordinator starting`);
	const sdkConfig = initialize({ env: global.driftEnv });
	const endpoint = global.endpoint;
	const wsEndpoint = global.wsEndpoint;
	const connection = new Connection(endpoint, {
		wsEndpoint,
		commitment: 'confirmed',
	});
	const [kp, wallet] = getWallet(global.keeperPrivateKey);

	const driftClient = new DriftClient({
		connection,
		programID: new PublicKey(sdkConfig.DRIFT_PROGRAM_ID),
		wallet,
		opts: { commitment: 'confirmed' } as ConfirmOptions,
		env: global.driftEnv,
		txVersion: 0 as TransactionVersion,
		userStats: true,
	});
	await driftClient.subscribe();

	const userMap = new UserMap({
		driftClient,
		connection: new Connection(endpoint),
		subscriptionConfig: { type: 'websocket', resubTimeoutMs: 30000 },
		skipInitialLoad: false,
		includeIdle: false,
	});
	await userMap.subscribe();

	const luts = await driftClient.fetchAllLookupTableAccounts();
	const coord = new CoordinatorService(driftClient, userMap, queue, luts);
	await coord.init();

	// periodic scan using the same cadence as legacy liquidator
	setInterval(() => {
		coord
			.scanAndEnqueueOnce()
			.catch((e) =>
				logger.error(`scan enqueue error: ${(e as Error).message}`)
			);
	}, 5000);
}

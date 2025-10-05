/* eslint-disable @typescript-eslint/no-non-null-assertion */
import { startLiquidatorV2 } from './main';
import { logger } from '../../logger';

require('dotenv').config();

async function main() {
	const driftEnv = (process.env.DRIFT_ENV || 'devnet') as
		| 'devnet'
		| 'mainnet-beta';
	const endpoint =
		process.env.RPC_ENDPOINT ||
		process.env.ENDPOINT ||
		process.env.DRIFT_ENDPOINT;
	const keeperPrivateKey = process.env.KEEPER_PRIVATE_KEY;
	if (!endpoint)
		throw new Error('RPC_ENDPOINT (or ENDPOINT/DRIFT_ENDPOINT) is required');
	if (!keeperPrivateKey) throw new Error('KEEPER_PRIVATE_KEY is required');

	await startLiquidatorV2({
		endpoint,
		wsEndpoint: process.env.WS_ENDPOINT,
		driftEnv,
		keeperPrivateKey,
	});

	logger.info('liquidatorV2 entry initialized');
}

main().catch((e) => {
	console.error(e);
	process.exit(1);
});

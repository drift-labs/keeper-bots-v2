/**
 * CI SDK Compatibility Test
 *
 * Validates SDK compatibility without requiring a funded wallet or Drift user account.
 */

import { Connection, Keypair, PublicKey } from '@solana/web3.js';
import {
	DriftClient,
	initialize,
	Wallet,
	BulkAccountLoader,
	PerpMarkets,
	SpotMarkets,
} from '@drift-labs/sdk';
import { JitProxyClient } from '@drift-labs/jit-proxy/lib';

const DEVNET_RPC = 'https://api.devnet.solana.com';
const TEST_TIMEOUT_MS = 60_000;

async function runTest(): Promise<void> {
	console.log('=== CI SDK Compatibility Test ===\n');

	// 1. SDK initialization
	console.log('1. Testing SDK initialization...');
	const sdkConfig = initialize({ env: 'devnet' });
	console.log(`   ✓ Drift Program ID: ${sdkConfig.DRIFT_PROGRAM_ID}`);
	console.log(`   ✓ JIT Proxy Program ID: ${sdkConfig.JIT_PROXY_PROGRAM_ID}`);

	// 2. Solana connection
	console.log('\n2. Testing Solana connection...');
	const connection = new Connection(DEVNET_RPC, 'confirmed');
	const slot = await connection.getSlot();
	console.log(`   ✓ Connected to devnet at slot ${slot}`);

	// 3. Market configs
	console.log('\n3. Testing market configs...');
	const perpMarkets = PerpMarkets['devnet'];
	const spotMarkets = SpotMarkets['devnet'];
	console.log(`   ✓ ${perpMarkets.length} perp markets, ${spotMarkets.length} spot markets`);

	// 4. DriftClient instantiation
	console.log('\n4. Testing DriftClient...');
	const wallet = new Wallet(Keypair.generate());
	const bulkAccountLoader = new BulkAccountLoader(connection, 'confirmed', 5000);

	const driftClient = new DriftClient({
		connection,
		wallet,
		programID: new PublicKey(sdkConfig.DRIFT_PROGRAM_ID),
		env: 'devnet',
		accountSubscription: { type: 'polling', accountLoader: bulkAccountLoader },
		activeSubAccountId: 0,
		subAccountIds: [],
		userStats: false,
	});
	console.log(`   ✓ DriftClient instantiated`);

	// 5. Subscribe to market data
	console.log('\n5. Testing DriftClient subscription...');
	if (!(await driftClient.subscribe())) {
		throw new Error('DriftClient subscription failed');
	}
	const perpAccounts = driftClient.getPerpMarketAccounts();
	const spotAccounts = driftClient.getSpotMarketAccounts();
	console.log(`   ✓ Loaded ${perpAccounts.length} perp, ${spotAccounts.length} spot market accounts`);

	// 6. JitProxyClient instantiation
	console.log('\n6. Testing JitProxyClient...');
	// @ts-ignore - version mismatch OK for this test
	new JitProxyClient({
		driftClient,
		programId: new PublicKey(sdkConfig.JIT_PROXY_PROGRAM_ID!),
	});
	console.log(`   ✓ JitProxyClient instantiated`);

	await driftClient.unsubscribe();
	console.log('\n=== All tests passed! ===');
}

// Run with timeout
Promise.race([
	runTest(),
	new Promise<never>((_, reject) =>
		setTimeout(() => reject(new Error(`Timed out after ${TEST_TIMEOUT_MS}ms`)), TEST_TIMEOUT_MS)
	),
])
	.then(() => process.exit(0))
	.catch((err) => {
		console.error('\n✗ Test failed:', err.message);
		process.exit(1);
	});

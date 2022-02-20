import {
	Connection,
	Keypair,
	PublicKey,
	TransactionSignature,
} from '@solana/web3.js';
import { Provider } from '@project-serum/anchor';
import {
	BulkAccountLoader,
	getClearingHouse,
	getPollingClearingHouseConfig,
	initialize,
	Wallet,
} from '@drift-labs/sdk';

require('dotenv').config();
//@ts-ignore
const sdkConfig = initialize({ env: process.env.ENV });

export function getWallet(): Wallet {
	const privateKey = process.env.FILLER_PRIVATE_KEY;
	const keypair = Keypair.fromSecretKey(
		Uint8Array.from(privateKey.split(',').map((val) => Number(val)))
	);
	return new Wallet(keypair);
}

const wallet = getWallet();
console.log(`Using address: ${wallet.publicKey.toString()}`);

const endpoint = process.env.ENDPOINT;
const connection = new Connection(endpoint);

const provider = new Provider(connection, wallet, Provider.defaultOptions());
const clearingHousePublicKey = new PublicKey(
	sdkConfig.CLEARING_HOUSE_PROGRAM_ID
);

const clearingHouse = getClearingHouse(
	getPollingClearingHouseConfig(
		connection,
		provider.wallet,
		clearingHousePublicKey,
		new BulkAccountLoader(connection, 'confirmed', 500)
	)
);

async function main(): Promise<TransactionSignature> {
	await clearingHouse.subscribe();
	const [txSig] = await clearingHouse.initializeUserAccount();
	return txSig;
}

main()
	.then((txSig) => {
		console.log(`Successfully initialized user. Tx: ${txSig}`);
	})
	.catch((e) => {
		console.error(e);
	})
	.finally(() => {
		clearingHouse.unsubscribe();
	});

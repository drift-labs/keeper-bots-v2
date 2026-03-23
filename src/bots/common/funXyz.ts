import { Connection, VersionedTransactionResponse } from '@solana/web3.js';

export const FUN_XYZ_SIGNER_WHITELIST = new Set([
	'zApVWDs3nSychNnUXSS2czhY78Ycopa15zELrK2gAdM',
	'4B62MS5gxpRZ2hwkGCNAAayA5f7LYZRW4z1ASSfU3SXo',
]);

export function extractAccountPubkeys(
	res: VersionedTransactionResponse | null
): string[] {
	if (!res?.transaction?.message) {
		return [];
	}

	const { transaction, meta } = res;
	const accountKeys = transaction.message.getAccountKeys(
		meta?.loadedAddresses
			? { accountKeysFromLookups: meta.loadedAddresses }
			: undefined
	);

	return accountKeys
		.keySegments()
		.flat()
		.map((pk) => pk.toBase58());
}

export async function isFunXyzDepositTxSignature(
	connection: Connection,
	txSig: string
): Promise<boolean> {
	const res = await connection.getTransaction(txSig, {
		maxSupportedTransactionVersion: 0,
		commitment: 'confirmed',
	});
	const accountPubkeys = extractAccountPubkeys(res);
	return accountPubkeys.some((pk) => FUN_XYZ_SIGNER_WHITELIST.has(pk));
}

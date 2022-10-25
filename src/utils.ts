import { Wallet } from '@drift-labs/sdk';
import {
	Token,
	TOKEN_PROGRAM_ID,
	ASSOCIATED_TOKEN_PROGRAM_ID,
} from '@solana/spl-token';
import { Connection, PublicKey, Transaction } from '@solana/web3.js';

export async function getOrCreateAssociatedTokenAccount(
	connection: Connection,
	mint: PublicKey,
	wallet: Wallet
): Promise<PublicKey> {
	const associatedTokenAccount = await Token.getAssociatedTokenAddress(
		ASSOCIATED_TOKEN_PROGRAM_ID,
		TOKEN_PROGRAM_ID,
		mint,
		wallet.publicKey
	);

	const accountInfo = await connection.getAccountInfo(associatedTokenAccount);
	if (accountInfo == null) {
		const tx = new Transaction().add(
			Token.createAssociatedTokenAccountInstruction(
				ASSOCIATED_TOKEN_PROGRAM_ID,
				TOKEN_PROGRAM_ID,
				mint,
				associatedTokenAccount,
				wallet.publicKey,
				wallet.publicKey
			)
		);
		const txSig = await connection.sendTransaction(tx, [wallet.payer]);
		await connection.confirmTransaction(txSig, 'confirmed');
	}

	return associatedTokenAccount;
}

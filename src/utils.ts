import { Wallet } from '@drift-labs/sdk';
import {
	Token,
	TOKEN_PROGRAM_ID,
	ASSOCIATED_TOKEN_PROGRAM_ID,
} from '@solana/spl-token';
import {
	Connection,
	PublicKey,
	Transaction,
	GetTransactionConfig,
} from '@solana/web3.js';

// devnet only
export const TOKEN_FAUCET_PROGRAM_ID = new PublicKey(
	'V4v1mQiAdLz4qwckEb45WqHYceYizoib39cDBHSWfaB'
);

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
		const latestBlock = await connection.getLatestBlockhash();
		await connection.confirmTransaction(
			{
				signature: txSig,
				blockhash: latestBlock.blockhash,
				lastValidBlockHeight: latestBlock.lastValidBlockHeight,
			},
			'confirmed'
		);
	}

	return associatedTokenAccount;
}

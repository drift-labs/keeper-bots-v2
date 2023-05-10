import { bs58 } from '@project-serum/anchor/dist/cjs/utils/bytes';
import fs from 'fs';
import { logger } from './logger';
import { Wallet } from '@drift-labs/sdk';
import {
	createAssociatedTokenAccountInstruction,
	getAssociatedTokenAddress,
} from '@solana/spl-token';
import { Connection, Keypair, PublicKey, Transaction } from '@solana/web3.js';

// devnet only
export const TOKEN_FAUCET_PROGRAM_ID = new PublicKey(
	'V4v1mQiAdLz4qwckEb45WqHYceYizoib39cDBHSWfaB'
);

export async function getOrCreateAssociatedTokenAccount(
	connection: Connection,
	mint: PublicKey,
	wallet: Wallet
): Promise<PublicKey> {
	const associatedTokenAccount = await getAssociatedTokenAddress(
		mint,
		wallet.publicKey
	);

	const accountInfo = await connection.getAccountInfo(associatedTokenAccount);
	if (accountInfo == null) {
		const tx = new Transaction().add(
			createAssociatedTokenAccountInstruction(
				wallet.publicKey,
				associatedTokenAccount,
				wallet.publicKey,
				mint
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

export function loadCommaDelimitToArray(str: string): number[] {
	try {
		return str
			.split(',')
			.filter((element) => {
				if (element.trim() === '') {
					return false;
				}

				return !isNaN(Number(element));
			})
			.map((element) => {
				return Number(element);
			});
	} catch (e) {
		return [];
	}
}

export function loadKeypair(privateKey: string): Keypair {
	// try to load privateKey as a filepath
	let loadedKey: Uint8Array;
	if (fs.existsSync(privateKey)) {
		logger.info(`loading private key from ${privateKey}`);
		privateKey = fs.readFileSync(privateKey).toString();
	}

	if (privateKey.includes('[') && privateKey.includes(']')) {
		logger.info(`Trying to load private key as numbers array`);
		loadedKey = Uint8Array.from(JSON.parse(privateKey));
	} else if (privateKey.includes(',')) {
		logger.info(`Trying to load private key as comma separated numbers`);
		loadedKey = Uint8Array.from(
			privateKey.split(',').map((val) => Number(val))
		);
	} else {
		logger.info(`Trying to load private key as base58 string`);
		privateKey = privateKey.replace(/\s/g, '');
		loadedKey = new Uint8Array(bs58.decode(privateKey));
	}

	return Keypair.fromSecretKey(Uint8Array.from(loadedKey));
}

export function getWallet(privateKeyOrFilepath: string): [Keypair, Wallet] {
	const keypair = loadKeypair(privateKeyOrFilepath);
	return [keypair, new Wallet(keypair)];
}

export function sleepMs(ms: number) {
	return new Promise((resolve) => setTimeout(resolve, ms));
}

export function sleepS(s: number) {
	return sleepMs(s * 1000);
}

import { base64, bs58 } from '@project-serum/anchor/dist/cjs/utils/bytes';
import fs from 'fs';
import { logger } from './logger';
import {
	BASE_PRECISION,
	BN,
	DLOB,
	DLOBNode,
	DataAndSlot,
	MakerInfo,
	MarketType,
	NodeToFill,
	NodeToTrigger,
	OraclePriceData,
	PERCENTAGE_PRECISION,
	PRICE_PRECISION,
	PerpMarketAccount,
	QUOTE_PRECISION,
	SpotMarketAccount,
	TxSender,
	User,
	Wallet,
	convertToNumber,
	getOrderSignature,
	getVariant,
} from '@drift-labs/sdk';
import {
	createAssociatedTokenAccountInstruction,
	getAssociatedTokenAddress,
} from '@solana/spl-token';
import {
	AddressLookupTableAccount,
	ComputeBudgetProgram,
	ConfirmOptions,
	Connection,
	Keypair,
	PublicKey,
	Signer,
	Transaction,
	TransactionError,
	TransactionInstruction,
	VersionedTransaction,
} from '@solana/web3.js';
import { webhookMessage } from './webhook';

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

export function loadCommaDelimitToStringArray(str: string): string[] {
	try {
		return str.split(',').filter((element) => {
			return element.trim() !== '';
		});
	} catch (e) {
		return [];
	}
}

export function convertToMarketType(input: string): MarketType {
	switch (input.toUpperCase()) {
		case 'PERP':
			return MarketType.PERP;
		case 'SPOT':
			return MarketType.SPOT;
		default:
			throw new Error(`Invalid market type: ${input}`);
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

export function decodeName(bytes: number[]): string {
	const buffer = Buffer.from(bytes);
	return buffer.toString('utf8').trim();
}

export function getNodeToFillSignature(node: NodeToFill): string {
	if (!node.node.userAccount) {
		return '~';
	}
	return `${node.node.userAccount}-${node.node.order?.orderId.toString()}`;
}

export function getFillSignatureFromUserAccountAndOrderId(
	userAccount: string,
	orderId: string
): string {
	return `${userAccount}-${orderId}`;
}

export function getNodeToTriggerSignature(node: NodeToTrigger): string {
	return getOrderSignature(node.node.order.orderId, node.node.userAccount);
}

export async function waitForAllSubscribesToFinish(
	subscriptionPromises: Promise<boolean>[]
): Promise<boolean> {
	const results = await Promise.all(subscriptionPromises);
	const falsePromises = subscriptionPromises.filter(
		(_, index) => !results[index]
	);
	if (falsePromises.length > 0) {
		logger.info('waiting to subscribe to DriftClient and User');
		await sleepMs(1000);
		return waitForAllSubscribesToFinish(falsePromises);
	} else {
		return true;
	}
}

export function getBestLimitBidExcludePubKey(
	dlob: DLOB,
	marketIndex: number,
	marketType: MarketType,
	slot: number,
	oraclePriceData: OraclePriceData,
	excludedPubKey: string,
	excludedUserAccountsAndOrder?: [string, number][]
): DLOBNode | undefined {
	const bids = dlob.getRestingLimitBids(
		marketIndex,
		slot,
		marketType,
		oraclePriceData
	);

	for (const bid of bids) {
		if (bid.userAccount === excludedPubKey) {
			continue;
		}
		if (
			excludedUserAccountsAndOrder?.some(
				(entry) =>
					entry[0] === (bid.userAccount ?? '') &&
					entry[1] === (bid.order?.orderId ?? -1)
			)
		) {
			continue;
		}
		return bid;
	}

	return undefined;
}

export function getBestLimitAskExcludePubKey(
	dlob: DLOB,
	marketIndex: number,
	marketType: MarketType,
	slot: number,
	oraclePriceData: OraclePriceData,
	excludedPubKey: string,
	excludedUserAccountsAndOrder?: [string, number][]
): DLOBNode | undefined {
	const asks = dlob.getRestingLimitAsks(
		marketIndex,
		slot,
		marketType,
		oraclePriceData
	);
	for (const ask of asks) {
		if (ask.userAccount === excludedPubKey) {
			continue;
		}
		if (
			excludedUserAccountsAndOrder?.some(
				(entry) =>
					entry[0] === (ask.userAccount ?? '') &&
					entry[1] === (ask.order?.orderId || -1)
			)
		) {
			continue;
		}

		return ask;
	}

	return undefined;
}

export function calculateAccountValueUsd(user: User): number {
	const netSpotValue = convertToNumber(
		user.getNetSpotMarketValue(),
		QUOTE_PRECISION
	);
	const unrealizedPnl = convertToNumber(
		user.getUnrealizedPNL(true, undefined, undefined),
		QUOTE_PRECISION
	);
	return netSpotValue + unrealizedPnl;
}

export function calculateBaseAmountToMarketMakePerp(
	perpMarketAccount: PerpMarketAccount,
	user: User,
	targetLeverage = 1
) {
	const basePriceNormed = convertToNumber(
		perpMarketAccount.amm.historicalOracleData.lastOraclePriceTwap
	);

	const accountValueUsd = calculateAccountValueUsd(user);

	targetLeverage *= 0.95;

	const maxBase = (accountValueUsd / basePriceNormed) * targetLeverage;
	const marketSymbol = decodeName(perpMarketAccount.name);

	logger.info(
		`(mkt index: ${marketSymbol}) base to market make (targetLvg=${targetLeverage.toString()}): ${maxBase.toString()} = ${accountValueUsd.toString()} / ${basePriceNormed.toString()} * ${targetLeverage.toString()}`
	);

	return maxBase;
}

export function calculateBaseAmountToMarketMakeSpot(
	spotMarketAccount: SpotMarketAccount,
	user: User,
	targetLeverage = 1
) {
	const basePriceNormalized = convertToNumber(
		spotMarketAccount.historicalOracleData.lastOraclePriceTwap
	);

	const accountValueUsd = calculateAccountValueUsd(user);

	targetLeverage *= 0.95;

	const maxBase = (accountValueUsd / basePriceNormalized) * targetLeverage;
	const marketSymbol = decodeName(spotMarketAccount.name);

	logger.info(
		`(mkt index: ${marketSymbol}) base to market make (targetLvg=${targetLeverage.toString()}): ${maxBase.toString()} = ${accountValueUsd.toString()} / ${basePriceNormalized.toString()} * ${targetLeverage.toString()}`
	);

	return maxBase;
}

export function isMarketVolatile(
	perpMarketAccount: PerpMarketAccount,
	oraclePriceData: OraclePriceData,
	volatileThreshold = 0.005 // 50 bps
) {
	const twapPrice =
		perpMarketAccount.amm.historicalOracleData.lastOraclePriceTwap5Min;
	const lastPrice = perpMarketAccount.amm.historicalOracleData.lastOraclePrice;
	const currentPrice = oraclePriceData.price;
	const minDenom = BN.min(BN.min(currentPrice, lastPrice), twapPrice);
	const cVsL =
		Math.abs(
			currentPrice.sub(lastPrice).mul(PRICE_PRECISION).div(minDenom).toNumber()
		) / PERCENTAGE_PRECISION.toNumber();
	const cVsT =
		Math.abs(
			currentPrice.sub(twapPrice).mul(PRICE_PRECISION).div(minDenom).toNumber()
		) / PERCENTAGE_PRECISION.toNumber();

	const recentStd =
		perpMarketAccount.amm.oracleStd
			.mul(PRICE_PRECISION)
			.div(minDenom)
			.toNumber() / PERCENTAGE_PRECISION.toNumber();

	if (
		recentStd > volatileThreshold ||
		cVsT > volatileThreshold ||
		cVsL > volatileThreshold
	) {
		return true;
	}

	return false;
}

export function isSpotMarketVolatile(
	spotMarketAccount: SpotMarketAccount,
	oraclePriceData: OraclePriceData,
	volatileThreshold = 0.005
) {
	const twapPrice =
		spotMarketAccount.historicalOracleData.lastOraclePriceTwap5Min;
	const lastPrice = spotMarketAccount.historicalOracleData.lastOraclePrice;
	const currentPrice = oraclePriceData.price;
	const minDenom = BN.min(BN.min(currentPrice, lastPrice), twapPrice);
	const cVsL =
		Math.abs(
			currentPrice.sub(lastPrice).mul(PRICE_PRECISION).div(minDenom).toNumber()
		) / PERCENTAGE_PRECISION.toNumber();
	const cVsT =
		Math.abs(
			currentPrice.sub(twapPrice).mul(PRICE_PRECISION).div(minDenom).toNumber()
		) / PERCENTAGE_PRECISION.toNumber();

	if (cVsT > volatileThreshold || cVsL > volatileThreshold) {
		return true;
	}

	return false;
}
export function isSetComputeUnitsIx(ix: TransactionInstruction): boolean {
	// Compute budget program discriminator is first byte
	// 2: set compute unit limit
	// 3: set compute unit price
	if (
		ix.programId.equals(ComputeBudgetProgram.programId) &&
		ix.data.at(0) === 2
	) {
		return true;
	}
	return false;
}

export type SimulateAndGetTxWithCUsResponse = {
	cuEstimate: number;
	simTxLogs: Array<string> | null;
	simError: TransactionError | string | null;
	tx: VersionedTransaction;
};

export async function simulateAndGetTxWithCUs(
	ixs: Array<TransactionInstruction>,
	connection: Connection,
	txSender: TxSender,
	lookupTableAccounts: AddressLookupTableAccount[],
	additionalSigners: Array<Signer>,
	opts?: ConfirmOptions,
	cuLimitMultiplier = 1.0,
	logSimDuration = false,
	doSimulation = true,
	recentBlockhash?: string,
	dumpTx = false
): Promise<SimulateAndGetTxWithCUsResponse> {
	if (ixs.length === 0) {
		throw new Error('cannot simulate empty tx');
	}

	let setCULimitIxIdx = -1;
	for (let idx = 0; idx < ixs.length; idx++) {
		if (isSetComputeUnitsIx(ixs[idx])) {
			setCULimitIxIdx = idx;
			break;
		}
	}

	const tx = await txSender.getVersionedTransaction(
		ixs,
		lookupTableAccounts,
		additionalSigners,
		opts,
		recentBlockhash
	);
	if (!doSimulation) {
		return {
			cuEstimate: -1,
			simTxLogs: null,
			simError: null,
			tx,
		};
	}
	if (dumpTx) {
		console.log(`===== Simulating The following transaction =====`);
		const serializedTx = base64.encode(Buffer.from(tx.serialize()));
		console.log(serializedTx);
		console.log(`================================================`);
	}

	let resp;
	try {
		const start = Date.now();
		resp = await connection.simulateTransaction(tx, {
			sigVerify: false,
			replaceRecentBlockhash: true,
			commitment: 'processed',
		});
		if (logSimDuration) {
			console.log(`Simulated tx took: ${Date.now() - start}ms`);
		}
	} catch (e) {
		console.error(e);
	}
	if (!resp) {
		throw new Error('Failed to simulate transaction');
	}

	if (resp.value.unitsConsumed === undefined) {
		throw new Error(`Failed to get units consumed from simulateTransaction`);
	}

	const simTxLogs = resp.value.logs;
	const cuEstimate = resp.value.unitsConsumed!;
	if (setCULimitIxIdx === -1) {
		ixs.unshift(
			ComputeBudgetProgram.setComputeUnitLimit({
				units: cuEstimate * cuLimitMultiplier,
			})
		);
	} else {
		ixs[setCULimitIxIdx] = ComputeBudgetProgram.setComputeUnitLimit({
			units: cuEstimate * cuLimitMultiplier,
		});
	}

	const txWithCUs = await txSender.getVersionedTransaction(
		ixs,
		lookupTableAccounts,
		additionalSigners,
		opts,
		recentBlockhash
	);

	return {
		cuEstimate,
		simTxLogs,
		simError: resp.value.err,
		tx: txWithCUs,
	};
}

export function handleSimResultError(
	simResult: SimulateAndGetTxWithCUsResponse,
	errorCodesToSuppress: number[],
	msgSuffix: string,
	suppressOutOfCUsMessage = true
) {
	if (
		(simResult.simError as ExtendedTransactionError).InstructionError ===
		undefined
	) {
		return;
	}
	const err = (simResult.simError as ExtendedTransactionError).InstructionError;
	if (!err) {
		return;
	}
	if (err.length < 2) {
		logger.error(
			`${msgSuffix} sim error has no error code. ${JSON.stringify(simResult)}`
		);
		return;
	}
	if (!err[1]) {
		return;
	}

	if (typeof err[1] === 'object' && 'Custom' in err[1]) {
		const customErrorCode = Number((err[1] as CustomError).Custom);
		if (errorCodesToSuppress.includes(customErrorCode)) {
			return;
		} else {
			const msg = `${msgSuffix} sim error with custom error code, simError: ${JSON.stringify(
				simResult.simError
			)}, cuEstimate: ${simResult.cuEstimate}, sim logs:\n${
				simResult.simTxLogs ? simResult.simTxLogs.join('\n') : 'none'
			}`;
			webhookMessage(msg);
			logger.error(msg);
		}
	} else {
		const msg = `${msgSuffix} sim error with no error code, simError: ${JSON.stringify(
			simResult.simError
		)}, cuEstimate: ${simResult.cuEstimate}, sim logs:\n${
			simResult.simTxLogs ? simResult.simTxLogs.join('\n') : 'none'
		}`;
		logger.error(msg);

		// early return if out of CU error.
		if (
			suppressOutOfCUsMessage &&
			simResult.simTxLogs &&
			simResult.simTxLogs[simResult.simTxLogs.length - 1].includes(
				'exceeded CUs meter at BPF instruction'
			)
		) {
			return;
		}

		webhookMessage(msg);
	}
	return;
}

export interface ExtendedTransactionError {
	InstructionError?: [number, string | object];
}

export interface CustomError {
	Custom?: number;
}

export function logMessageForNodeToFill(
	node: NodeToFill,
	takerUser: string,
	takerUserSlot: number,
	makerInfos: Array<DataAndSlot<MakerInfo>>,
	currSlot: number,
	prefix?: string,
	basePrecision: BN = BASE_PRECISION,
	fallbackSource = 'vAMM'
): string {
	const takerNode = node.node;
	const takerOrder = takerNode.order;
	if (!takerOrder) {
		return 'no taker order';
	}

	if (node.makerNodes.length !== makerInfos.length) {
		logger.error(`makerNodes and makerInfos length mismatch`);
	}

	let msg = '';
	if (prefix) {
		msg += `${prefix}\n`;
	}

	msg += `taker on market ${takerOrder.marketIndex}: ${takerUser}-${
		takerOrder.orderId
	} (takerSlot: ${takerUserSlot}, currSlot: ${currSlot}) ${getVariant(
		takerOrder.direction
	)} ${convertToNumber(
		takerOrder.baseAssetAmountFilled,
		basePrecision
	)}/${convertToNumber(
		takerOrder.baseAssetAmount,
		basePrecision
	)} @ ${convertToNumber(
		takerOrder.price,
		PRICE_PRECISION
	)} (orderType: ${getVariant(takerOrder.orderType)})\n`;
	msg += `makers:\n`;
	if (makerInfos.length > 0) {
		for (let i = 0; i < makerInfos.length; i++) {
			const maker = makerInfos[i].data;
			const makerSlot = makerInfos[i].slot;
			const makerOrder = maker.order!;
			msg += `  [${i}] market ${
				makerOrder.marketIndex
			}: ${maker.maker.toBase58()}-${
				makerOrder.orderId
			} (makerSlot: ${makerSlot}) ${getVariant(
				makerOrder.direction
			)} ${convertToNumber(
				makerOrder.baseAssetAmountFilled,
				basePrecision
			)}/${convertToNumber(
				makerOrder.baseAssetAmount,
				basePrecision
			)} @ ${convertToNumber(makerOrder.price, PRICE_PRECISION)} (offset: ${
				makerOrder.oraclePriceOffset / PRICE_PRECISION.toNumber()
			}) (orderType: ${getVariant(makerOrder.orderType)})\n`;
		}
	} else {
		msg += `  ${fallbackSource}`;
	}
	return msg;
}

export function getTransactionAccountMetas(
	tx: VersionedTransaction,
	lutAccounts: Array<AddressLookupTableAccount>
): {
	estTxSize: number;
	accountMetas: any[];
	writeAccs: number;
	txAccounts: number;
} {
	let writeAccs = 0;
	const accountMetas: any[] = [];
	const estTxSize = tx.message.serialize().length;
	const acc = tx.message.getAccountKeys({
		addressLookupTableAccounts: lutAccounts,
	});
	const txAccounts = acc.length;
	for (let i = 0; i < txAccounts; i++) {
		const meta: any = {};
		if (tx.message.isAccountWritable(i)) {
			writeAccs++;
			meta['writeable'] = true;
		}
		if (tx.message.isAccountSigner(i)) {
			meta['signer'] = true;
		}
		meta['address'] = acc.get(i)!.toBase58();
		accountMetas.push(meta);
	}

	return {
		estTxSize,
		accountMetas,
		writeAccs,
		txAccounts,
	};
}

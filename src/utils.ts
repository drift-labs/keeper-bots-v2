import { base64, bs58 } from '@project-serum/anchor/dist/cjs/utils/bytes';
import fs from 'fs';
import { logger } from './logger';
import {
	BASE_PRECISION,
	BN,
	DLOB,
	DLOBNode,
	DataAndSlot,
	DriftClient,
	HeliusPriorityLevel,
	JupiterClient,
	DriftEnv,
	MakerInfo,
	MarketType,
	NodeToFill,
	NodeToTrigger,
	OraclePriceData,
	PERCENTAGE_PRECISION,
	PRICE_PRECISION,
	PerpMarketAccount,
	PerpMarketConfig,
	PriorityFeeMethod,
	PriorityFeeSubscriber,
	QUOTE_PRECISION,
	SpotMarketAccount,
	// TEN,
	TxSender,
	User,
	Wallet,
	convertToNumber,
	getOrderSignature,
	getVariant,
} from '@drift-labs/sdk';
import {
	NATIVE_MINT,
	createAssociatedTokenAccountInstruction,
	createCloseAccountInstruction,
	getAssociatedTokenAddress,
	getAssociatedTokenAddressSync,
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
	TransactionMessage,
	VersionedTransaction,
} from '@solana/web3.js';
import { webhookMessage } from './webhook';
import { DriftPriorityFeeResponse } from '@drift-labs/sdk/lib/priorityFee/driftPriorityFeeMethod';

// devnet only
export const TOKEN_FAUCET_PROGRAM_ID = new PublicKey(
	'V4v1mQiAdLz4qwckEb45WqHYceYizoib39cDBHSWfaB'
);

const JUPITER_SLIPPAGE_BPS = 100;
export const PRIORITY_FEE_SERVER_RATE_LIMIT_PER_MIN = 300;

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

export const getNodeToFillSignature = (
	node: NodeToFill,
	maker?: MakerInfo
): string => {
	if (!node.node.userAccount) {
		return '~';
	}
	if (maker) {
		return `${
			node.node.userAccount
		}-${node.node.order?.orderId.toString()}-${maker.maker.toString()}`;
	} else {
		return `${node.node.userAccount}-${node.node.order?.orderId.toString()}`;
	}
};

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
	simTxDuration: number;
	tx: VersionedTransaction;
};

const PLACEHOLDER_BLOCKHASH = 'Fdum64WVeej6DeL85REV9NvfSxEJNPZ74DBk7A8kTrKP';
function getVersionedTransaction(
	payerKey: PublicKey,
	ixs: Array<TransactionInstruction>,
	lookupTableAccounts: AddressLookupTableAccount[],
	recentBlockhash: string
): VersionedTransaction {
	const message = new TransactionMessage({
		payerKey,
		recentBlockhash,
		instructions: ixs,
	}).compileToV0Message(lookupTableAccounts);

	return new VersionedTransaction(message);
}

export async function simulateAndGetTxWithCUs(
	ixs: Array<TransactionInstruction>,
	connection: Connection,
	txSender: TxSender,
	lookupTableAccounts: AddressLookupTableAccount[],
	additionalSigners: Array<Signer>,
	opts?: ConfirmOptions,
	cuLimitMultiplier = 1.0,
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

	let simTxDuration = 0;
	const tx = getVersionedTransaction(
		txSender.wallet.publicKey,
		ixs,
		lookupTableAccounts,
		recentBlockhash ?? PLACEHOLDER_BLOCKHASH
	);
	if (!doSimulation) {
		return {
			cuEstimate: -1,
			simTxLogs: null,
			simError: null,
			simTxDuration,
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
		simTxDuration = Date.now() - start;
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
		simTxDuration,
		simError: resp.value.err,
		tx: txWithCUs,
	};
}

export function handleSimResultError(
	simResult: SimulateAndGetTxWithCUsResponse,
	errorCodesToSuppress: number[],
	msgSuffix: string,
	suppressOutOfCUsMessage = true
): undefined | number {
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

	let errorCode: number | undefined;

	if (typeof err[1] === 'object' && 'Custom' in err[1]) {
		const customErrorCode = Number((err[1] as CustomError).Custom);
		errorCode = customErrorCode;
		if (errorCodesToSuppress.includes(customErrorCode)) {
			return errorCode;
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
			return errorCode;
		}

		webhookMessage(msg);
	}

	return errorCode;
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
		logger.error(
			`makerNodes and makerInfos length mismatch, makerNodes: ${node.makerNodes.length}, makerInfos: ${makerInfos.length}`
		);
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

export async function swapFillerHardEarnedUSDCForSOL(
	priorityFeeSubscriber: PriorityFeeSubscriber,
	driftClient: DriftClient,
	jupiterClient: JupiterClient,
	blockhash: string
) {
	try {
		const usdc = driftClient.getUser().getTokenAmount(0);
		const sol = driftClient.getUser().getTokenAmount(1);

		console.log(
			`${driftClient.authority.toBase58()} has ${convertToNumber(
				usdc,
				QUOTE_PRECISION
			)} usdc, ${convertToNumber(sol, BASE_PRECISION)} sol`
		);

		const usdcMarket = driftClient.getSpotMarketAccount(0);
		const solMarket = driftClient.getSpotMarketAccount(1);

		if (!usdcMarket || !solMarket) {
			console.log('Market not found, skipping...');
			return;
		}

		const inPrecision = new BN(10).pow(new BN(usdcMarket.decimals));
		const outPrecision = new BN(10).pow(new BN(solMarket.decimals));

		if (usdc.lt(new BN(1).mul(QUOTE_PRECISION))) {
			console.log(
				`${driftClient.authority.toBase58()} not enough USDC to swap (${convertToNumber(
					usdc,
					QUOTE_PRECISION
				)}), skipping...`
			);
			return;
		}

		const start = performance.now();
		const quote = await jupiterClient.getQuote({
			inputMint: usdcMarket.mint,
			outputMint: solMarket.mint,
			amount: usdc.sub(new BN(1)),
			maxAccounts: 10,
			slippageBps: JUPITER_SLIPPAGE_BPS,
			swapMode: 'ExactIn',
		});

		const quoteInNum = convertToNumber(new BN(quote.inAmount), inPrecision);
		const quoteOutNum = convertToNumber(new BN(quote.outAmount), outPrecision);
		const swapPrice = quoteInNum / quoteOutNum;
		const oraclePrice = convertToNumber(
			driftClient.getOracleDataForSpotMarket(1).price,
			PRICE_PRECISION
		);

		if (swapPrice / oraclePrice - 1 > 0.01) {
			console.log(`Swap price is 1% higher than oracle price, skipping...`);
			return;
		}

		console.log(
			`Quoted ${quoteInNum} USDC for ${quoteOutNum} SOL, swapPrice: ${swapPrice}, oraclePrice: ${oraclePrice}`
		);

		const driftLut = await driftClient.fetchMarketLookupTableAccount();

		const transaction = await jupiterClient.getSwap({
			quote,
			userPublicKey: driftClient.provider.wallet.publicKey,
			slippageBps: JUPITER_SLIPPAGE_BPS,
		});

		const { transactionMessage, lookupTables } =
			await jupiterClient.getTransactionMessageAndLookupTables({
				transaction,
			});

		const jupiterInstructions = jupiterClient.getJupiterInstructions({
			transactionMessage,
			inputMint: usdcMarket.mint,
			outputMint: solMarket.mint,
		});

		const preInstructions = [];

		const outAssociatedTokenAccount =
			await driftClient.getAssociatedTokenAccount(1);

		const solAccountInfo = await driftClient.connection.getAccountInfo(
			outAssociatedTokenAccount
		);

		if (!solAccountInfo) {
			preInstructions.push(
				driftClient.createAssociatedTokenAccountIdempotentInstruction(
					outAssociatedTokenAccount,
					driftClient.provider.wallet.publicKey,
					driftClient.provider.wallet.publicKey,
					solMarket.mint
				)
			);
		}

		const inAssociatedTokenAccount =
			await driftClient.getAssociatedTokenAccount(0);

		const usdcAccountInfo = await driftClient.connection.getAccountInfo(
			inAssociatedTokenAccount
		);

		if (!usdcAccountInfo) {
			preInstructions.push(
				driftClient.createAssociatedTokenAccountIdempotentInstruction(
					inAssociatedTokenAccount,
					driftClient.provider.wallet.publicKey,
					driftClient.provider.wallet.publicKey,
					usdcMarket.mint
				)
			);
		}

		const withdrawIx = await driftClient.getWithdrawIx(
			usdc.muln(10), // gross overestimate just to get everything out of the account
			0,
			inAssociatedTokenAccount,
			true
		);

		const withdrawerWrappedSolAta = getAssociatedTokenAddressSync(
			NATIVE_MINT,
			driftClient.authority
		);

		const closeAccountInstruction = createCloseAccountInstruction(
			withdrawerWrappedSolAta,
			driftClient.authority,
			driftClient.authority
		);

		const ixs = [
			...preInstructions,
			withdrawIx,
			...jupiterInstructions,
			closeAccountInstruction,
		];

		const buildTx = async (cu: number): Promise<VersionedTransaction> => {
			return await driftClient.txSender.getVersionedTransaction(
				[
					ComputeBudgetProgram.setComputeUnitLimit({
						units: cu,
					}),
					ComputeBudgetProgram.setComputeUnitPrice({
						microLamports: Math.floor(
							priorityFeeSubscriber.getHeliusPriorityFeeLevel(
								HeliusPriorityLevel.LOW
							) * 1.1
						),
					}),
					...ixs,
				],
				[...lookupTables, driftLut],
				[],
				driftClient.opts
			);
		};

		const tx = await buildTx(1_000_000);
		tx.message.recentBlockhash = blockhash;

		const simTxResult = await driftClient.connection.simulateTransaction(
			await buildTx(1_000_000),
			{
				replaceRecentBlockhash: true,
				commitment: 'confirmed',
			}
		);

		if (simTxResult.value.err) {
			console.log('Sim error:');
			console.error(simTxResult.value.err);
			console.log('Sim logs:');
			console.log(simTxResult.value.logs);
			console.log(`Units consumed: ${simTxResult.value.unitsConsumed}`);
			return;
		}

		console.log(
			`${driftClient.authority.toBase58()} sending swap tx... ${
				performance.now() - start
			}`
		);

		const txSigAndSlot = await driftClient.txSender.sendVersionedTransaction(
			// @ts-ignore
			await buildTx(Math.floor(simTxResult.value.unitsConsumed * 1.2)),
			[],
			driftClient.opts
		);
		console.log(`Swap tx: https://solana.fm/tx/${txSigAndSlot.txSig}`);
	} catch (e) {
		console.error(e);
	}
}
export function getDriftPriorityFeeEndpoint(driftEnv: DriftEnv): string {
	switch (driftEnv) {
		case 'devnet':
			return 'https://dlob.drift.trade';
		case 'mainnet-beta':
			return 'https://dlob.drift.trade';
	}
}

export function getMarketId(
	marketType: MarketType,
	marketIndex: number
): string {
	return `${getVariant(marketType)}-${marketIndex}`;
}

export async function initializePriorityFeeSubscriberMap({
	pfsMap,
	connection,
	driftPriorityFeeEndpoint,
	perpMarkets,
	includeQuoteMarket,
	maxFeeMicroLamports,
	priorityFeeMultiplier,
}: {
	pfsMap: Map<string, PriorityFeeSubscriber>;
	connection: Connection;
	driftPriorityFeeEndpoint: string;
	perpMarkets: PerpMarketConfig[];
	includeQuoteMarket: boolean;
	maxFeeMicroLamports?: number;
	priorityFeeMultiplier?: number;
}): Promise<Map<string, PriorityFeeSubscriber>> {
	const frequencyMs =
		((perpMarkets.length + 1) * 60_000) /
		PRIORITY_FEE_SERVER_RATE_LIMIT_PER_MIN;

	logger.info(
		`Initializing ${perpMarkets.length} PFS subscribers in a staggered fashion to stay below rate limits`
	);

	for (let i = 0; i < perpMarkets.length; i++) {
		const market = perpMarkets[i];
		const marketId = getMarketId(MarketType.PERP, market.marketIndex);
		if (pfsMap.has(marketId)) {
			continue;
		}
		const driftMarkets = [
			{
				marketType: 'perp',
				marketIndex: market.marketIndex,
			},
		];
		if (includeQuoteMarket) {
			driftMarkets.push({
				marketType: 'spot',
				marketIndex: 0,
			});
		}
		const pfs = new PriorityFeeSubscriber({
			connection: connection,
			frequencyMs,
			priorityFeeMethod: PriorityFeeMethod.DRIFT,
			driftPriorityFeeEndpoint,
			driftMarkets,
			customStrategy: {
				calculate: (samples: DriftPriorityFeeResponse) => {
					return Math.max(...samples.map((p) => p[HeliusPriorityLevel.HIGH]));
				},
			},
			maxFeeMicroLamports,
			priorityFeeMultiplier,
		});
		await pfs.subscribe();
		pfsMap.set(marketId, pfs);

		// stagger pfs subscriptions to avoid rate limit issues, not sure what good method is
		await sleepMs(10000 / perpMarkets.length);
		logger.info(
			`Initialized PFS for market ${market.marketIndex}, ${i + 1}/${
				perpMarkets.length
			}`
		);
	}

	return pfsMap;
}

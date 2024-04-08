import {
	Order,
	UserAccount,
	SpotPosition,
	PerpPosition,
	NodeToTrigger,
	TriggerOrderNode,
	NodeToFill,
	DLOBNode,
	OrderNode,
	TakingLimitOrderNode,
	RestingLimitOrderNode,
	FloatingLimitOrderNode,
	MarketOrderNode,
	DriftClient,
	initialize,
	OracleInfo,
	PerpMarketConfig,
	SpotMarketConfig,
	Wallet,
	BASE_PRECISION,
	convertToNumber,
	DataAndSlot,
	getOrderSignature,
	getVariant,
	MakerInfo,
	PRICE_PRECISION,
	TxSender,
	BN,
} from '@drift-labs/sdk';
import {
	AddressLookupTableAccount,
	ComputeBudgetProgram,
	ConfirmOptions,
	Connection,
	PublicKey,
	Signer,
	TransactionError,
	TransactionInstruction,
	TransactionMessage,
	VersionedTransaction,
} from '@solana/web3.js';
import {
	SerializedUserAccount,
	SerializedOrder,
	SerializedSpotPosition,
	SerializedPerpPosition,
	SerializedNodeToTrigger,
	SerializedTriggerOrderNode,
	SerializedNodeToFill,
	SerializedDLOBNode,
	NodeToFillWithBuffer,
} from './types';
import { ChildProcess, fork } from 'child_process';
import { logger } from '../../logger';

export const serializeUserAccount = (
	userAccount: UserAccount
): SerializedUserAccount => {
	return {
		...userAccount,
		authority: userAccount.authority?.toString(),
		delegate: userAccount.delegate?.toString(),
		orders: userAccount.orders.map(serializeOrder),
		spotPositions: userAccount.spotPositions.map(serializeSpotPosition),
		perpPositions: userAccount.perpPositions.map(serializePerpPosition),
		lastAddPerpLpSharesTs: userAccount.lastAddPerpLpSharesTs?.toString('hex'),
		settledPerpPnl: userAccount.settledPerpPnl?.toString('hex'),
		totalDeposits: userAccount.totalDeposits?.toString('hex'),
		totalWithdraws: userAccount.totalWithdraws?.toString('hex'),
		totalSocialLoss: userAccount.totalSocialLoss?.toString('hex'),
		cumulativePerpFunding: userAccount.cumulativePerpFunding?.toString('hex'),
		cumulativeSpotFees: userAccount.cumulativeSpotFees?.toString('hex'),
		liquidationMarginFreed: userAccount.liquidationMarginFreed?.toString('hex'),
		lastActiveSlot: userAccount.lastActiveSlot?.toString('hex'),
	};
};

const serializeOrder = (order: Order): SerializedOrder => {
	return {
		...order,
		slot: order.slot?.toString('hex'),
		price: order.price?.toString('hex'),
		baseAssetAmount: order.baseAssetAmount?.toString('hex'),
		quoteAssetAmount: order.quoteAssetAmount?.toString('hex'),
		baseAssetAmountFilled: order.baseAssetAmountFilled?.toString('hex'),
		quoteAssetAmountFilled: order.quoteAssetAmountFilled?.toString('hex'),
		triggerPrice: order.triggerPrice?.toString('hex'),
		auctionStartPrice: order.auctionStartPrice?.toString('hex'),
		auctionEndPrice: order.auctionEndPrice?.toString('hex'),
		maxTs: order.maxTs?.toString('hex'),
	};
};

const serializeSpotPosition = (
	position: SpotPosition
): SerializedSpotPosition => {
	return {
		...position,
		scaledBalance: position.scaledBalance?.toString('hex'),
		openBids: position.openBids?.toString('hex'),
		openAsks: position.openAsks?.toString('hex'),
		cumulativeDeposits: position.cumulativeDeposits?.toString('hex'),
	};
};

const serializePerpPosition = (
	position: PerpPosition
): SerializedPerpPosition => {
	return {
		...position,
		baseAssetAmount: position.baseAssetAmount?.toString('hex'),
		lastCumulativeFundingRate:
			position.lastCumulativeFundingRate?.toString('hex'),
		quoteAssetAmount: position.quoteAssetAmount?.toString('hex'),
		quoteEntryAmount: position.quoteEntryAmount?.toString('hex'),
		quoteBreakEvenAmount: position.quoteBreakEvenAmount?.toString('hex'),
		openBids: position.openBids?.toString('hex'),
		openAsks: position.openAsks?.toString('hex'),
		settledPnl: position.settledPnl?.toString('hex'),
		lpShares: position.lpShares?.toString('hex'),
		lastBaseAssetAmountPerLp:
			position.lastBaseAssetAmountPerLp?.toString('hex'),
		lastQuoteAssetAmountPerLp:
			position.lastQuoteAssetAmountPerLp?.toString('hex'),
	};
};

export const deserializeUserAccount = (
	serializedUserAccount: SerializedUserAccount
) => {
	return {
		...serializedUserAccount,
		authority: new PublicKey(serializedUserAccount.authority),
		delegate: new PublicKey(serializedUserAccount.delegate),
		orders: serializedUserAccount.orders.map(deserializeOrder),
		spotPositions: serializedUserAccount.spotPositions.map(
			deserializeSpotPosition
		),
		perpPositions: serializedUserAccount.perpPositions.map(
			deserializePerpPosition
		),
		lastAddPerpLpSharesTs: new BN(
			serializedUserAccount.lastAddPerpLpSharesTs,
			'hex'
		),
		settledPerpPnl: new BN(serializedUserAccount.settledPerpPnl, 'hex'),
		totalDeposits: new BN(serializedUserAccount.totalDeposits, 'hex'),
		totalWithdraws: new BN(serializedUserAccount.totalWithdraws, 'hex'),
		totalSocialLoss: new BN(serializedUserAccount.totalSocialLoss, 'hex'),
		cumulativePerpFunding: new BN(
			serializedUserAccount.cumulativePerpFunding,
			'hex'
		),
		cumulativeSpotFees: new BN(serializedUserAccount.cumulativeSpotFees, 'hex'),
		liquidationMarginFreed: new BN(
			serializedUserAccount.liquidationMarginFreed,
			'hex'
		),
		lastActiveSlot: new BN(serializedUserAccount.lastActiveSlot, 'hex'),
	};
};

export const deserializeOrder = (serializedOrder: SerializedOrder) => {
	return {
		...serializedOrder,
		slot: new BN(serializedOrder.slot, 'hex'),
		price: new BN(serializedOrder.price, 'hex'),
		baseAssetAmount: new BN(serializedOrder.baseAssetAmount, 'hex'),
		quoteAssetAmount: new BN(serializedOrder.quoteAssetAmount, 'hex'),
		baseAssetAmountFilled: new BN(serializedOrder.baseAssetAmountFilled, 'hex'),
		quoteAssetAmountFilled: new BN(
			serializedOrder.quoteAssetAmountFilled,
			'hex'
		),
		triggerPrice: new BN(serializedOrder.triggerPrice, 'hex'),
		auctionStartPrice: new BN(serializedOrder.auctionStartPrice, 'hex'),
		auctionEndPrice: new BN(serializedOrder.auctionEndPrice, 'hex'),
		maxTs: new BN(serializedOrder.maxTs, 'hex'),
	};
};

const deserializeSpotPosition = (
	serializedPosition: SerializedSpotPosition
) => {
	return {
		...serializedPosition,
		scaledBalance: new BN(serializedPosition.scaledBalance, 'hex'),
		openBids: new BN(serializedPosition.openBids, 'hex'),
		openAsks: new BN(serializedPosition.openAsks, 'hex'),
		cumulativeDeposits: new BN(serializedPosition.cumulativeDeposits, 'hex'),
	};
};

const deserializePerpPosition = (
	serializedPosition: SerializedPerpPosition
) => {
	return {
		...serializedPosition,
		baseAssetAmount: new BN(serializedPosition.baseAssetAmount, 'hex'),
		lastCumulativeFundingRate: new BN(
			serializedPosition.lastCumulativeFundingRate,
			'hex'
		),
		quoteAssetAmount: new BN(serializedPosition.quoteAssetAmount, 'hex'),
		quoteEntryAmount: new BN(serializedPosition.quoteEntryAmount, 'hex'),
		quoteBreakEvenAmount: new BN(
			serializedPosition.quoteBreakEvenAmount,
			'hex'
		),
		openBids: new BN(serializedPosition.openBids, 'hex'),
		openAsks: new BN(serializedPosition.openAsks, 'hex'),
		settledPnl: new BN(serializedPosition.settledPnl, 'hex'),
		lpShares: new BN(serializedPosition.lpShares, 'hex'),
		lastBaseAssetAmountPerLp: new BN(
			serializedPosition.lastBaseAssetAmountPerLp,
			'hex'
		),
		lastQuoteAssetAmountPerLp: new BN(
			serializedPosition.lastQuoteAssetAmountPerLp,
			'hex'
		),
	};
};

export const serializeNodeToTrigger = (
	node: NodeToTrigger,
	userAccountData: Buffer
): SerializedNodeToTrigger => {
	return {
		node: serializeTriggerOrderNode(node.node, userAccountData),
	};
};

const serializeTriggerOrderNode = (
	node: TriggerOrderNode,
	userAccountData: Buffer
): SerializedTriggerOrderNode => {
	return {
		userAccountData: userAccountData,
		order: serializeOrder(node.order),
		userAccount: node.userAccount.toString(),
		sortValue: node.sortValue.toString('hex'),
		haveFilled: node.haveFilled,
		haveTrigger: node.haveTrigger,
	};
};

export const serializeNodeToFill = (
	node: NodeToFill,
	userAccountData: Buffer,
	makerAccountDatas: Map<string, Buffer>
): SerializedNodeToFill => {
	return {
		node: serializeDLOBNode(node.node, userAccountData),
		makerNodes: node.makerNodes.map((node) => {
			// @ts-ignore
			return serializeDLOBNode(node, makerAccountDatas.get(node.userAccount));
		}),
	};
};

const serializeDLOBNode = (
	node: DLOBNode,
	userAccountData: Buffer
): SerializedDLOBNode => {
	if (node instanceof OrderNode) {
		return {
			type: node.constructor.name,
			userAccountData: userAccountData,
			order: serializeOrder(node.order),
			userAccount: node.userAccount,
			sortValue: node.sortValue.toString('hex'),
			haveFilled: node.haveFilled,
			haveTrigger: 'haveTrigger' in node ? node.haveTrigger : undefined,
		};
	} else {
		throw new Error(
			'Node is not an OrderNode or does not implement DLOBNode interface correctly.'
		);
	}
};

export const deserializeNodeToFill = (
	serializedNode: SerializedNodeToFill
): NodeToFillWithBuffer => {
	const node = {
		userAccountData: serializedNode.node.userAccountData,
		makerAccountData: JSON.stringify(
			Array.from(
				serializedNode.makerNodes
					.reduce((map, node) => {
						map.set(node.userAccount, node.userAccountData);
						return map;
					}, new Map())
					.entries()
			)
		),
		node: deserializeDLOBNode(serializedNode.node),
		makerNodes: serializedNode.makerNodes.map(deserializeDLOBNode),
	};
	return node;
};

const deserializeDLOBNode = (node: SerializedDLOBNode): DLOBNode => {
	const order = deserializeOrder(node.order);
	switch (node.type) {
		case 'TakingLimitOrderNode':
			return new TakingLimitOrderNode(order, node.userAccount);
		case 'RestingLimitOrderNode':
			return new RestingLimitOrderNode(order, node.userAccount);
		case 'FloatingLimitOrderNode':
			return new FloatingLimitOrderNode(order, node.userAccount);
		case 'MarketOrderNode':
			return new MarketOrderNode(order, node.userAccount);
		default:
			throw new Error(`Invalid node type: ${node.type}`);
	}
};

export const getNodeToTriggerSignature = (
	node: SerializedNodeToTrigger
): string => {
	return getOrderSignature(node.node.order.orderId, node.node.userAccount);
};

export const getFillSignatureFromUserAccountAndOrderId = (
	userAccount: string,
	orderId: string
): string => {
	return `${userAccount}-${orderId}`;
};

export const getNodeToFillSignature = (node: NodeToFill): string => {
	if (!node.node.userAccount) {
		return '~';
	}
	return `${node.node.userAccount}-${node.node.order?.orderId.toString()}`;
};

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
	recentBlockhash?: string
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

	// const tx = await txSender.getVersionedTransaction(
	//   ixs,
	//   lookupTableAccounts,
	//   additionalSigners,
	//   opts,
	//   recentBlockhash,
	// );

	const latestBlockhash = await connection.getLatestBlockhash();

	const message = new TransactionMessage({
		payerKey: txSender.wallet.publicKey,
		recentBlockhash: latestBlockhash.blockhash,
		instructions: ixs,
	}).compileToV0Message(lookupTableAccounts);

	const tx = new VersionedTransaction(message);
	// @ts-ignore
	tx.sign([txSender.wallet.payer]);

	if (!doSimulation) {
		return {
			cuEstimate: -1,
			simTxLogs: null,
			simError: null,
			tx,
		};
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
			logger.info(`[Simulator] Simulated tx took: ${Date.now() - start}ms`);
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

export type SimulateAndGetTxWithCUsResponse = {
	cuEstimate: number;
	simTxLogs: Array<string> | null;
	simError: TransactionError | string | null;
	tx: VersionedTransaction;
};

export interface ExtendedTransactionError {
	InstructionError?: [number, string | object];
}

export interface CustomError {
	Custom?: number;
}

export function getErrorCode(error: Error): number | undefined {
	// @ts-ignore
	let errorCode = error.code;

	if (!errorCode) {
		try {
			const matches = error.message.match(
				/custom program error: (0x[0-9,a-f]+)/
			);
			if (!matches) {
				return undefined;
			}

			const code = matches[1];

			if (code) {
				errorCode = parseInt(code, 16);
			}
		} catch (e) {
			// no problem if couldn't match error code
		}
	}
	return errorCode;
}

export function sleepMs(ms: number) {
	return new Promise((resolve) => setTimeout(resolve, ms));
}

export function logMessageForNodeToFill(
	node: NodeToFill,
	takerUser: string,
	takerUserSlot: number,
	makerInfos: Array<DataAndSlot<MakerInfo>>,
	currSlot: number,
	prefix?: string
): string {
	const takerNode = node.node;
	const takerOrder = takerNode.order;
	if (!takerOrder) {
		return 'no taker order';
	}

	if (node.makerNodes.length !== makerInfos.length) {
		logger.error(`[Logger] makerNodes and makerInfos length mismatch`);
	}

	let msg = '';
	if (prefix) {
		msg += `${prefix}\n`;
	}

	msg += `[Logger] taker on market ${takerOrder.marketIndex}: ${takerUser}-${
		takerOrder.orderId
	} (takerSlot: ${takerUserSlot}, currSlot: ${currSlot}) ${getVariant(
		takerOrder.direction
	)} ${convertToNumber(
		takerOrder.baseAssetAmountFilled,
		BASE_PRECISION
	)}/${convertToNumber(
		takerOrder.baseAssetAmount,
		BASE_PRECISION
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
				BASE_PRECISION
			)}/${convertToNumber(
				makerOrder.baseAssetAmount,
				BASE_PRECISION
			)} @ ${convertToNumber(makerOrder.price, PRICE_PRECISION)} (offset: ${
				makerOrder.oraclePriceOffset / PRICE_PRECISION.toNumber()
			}) (orderType: ${getVariant(makerOrder.orderType)})\n`;
		}
	} else {
		msg += `  vAMM`;
	}
	return msg;
}

export const getOracleInfoForMarket = (
	sdkConfig: any,
	marketIndex: number,
	marketTypeStr: 'spot' | 'perp'
): OracleInfo => {
	if (marketTypeStr === 'perp') {
		const perpMarket: PerpMarketConfig = sdkConfig.PERP_MARKETS.find(
			(config: PerpMarketConfig) => config.marketIndex === marketIndex
		);
		return {
			publicKey: perpMarket.oracle,
			source: perpMarket.oracleSource,
		};
	} else {
		const spotMarket: SpotMarketConfig = sdkConfig.SPOT_MARKETS.find(
			(config: SpotMarketConfig) => config.marketIndex === marketIndex
		);
		return {
			publicKey: spotMarket.oracle,
			source: spotMarket.oracleSource,
		};
	}
};

export const getDriftClientFromArgs = ({
	connection,
	wallet,
	marketIndex,
	marketTypeStr,
}: {
	connection: Connection;
	wallet: Wallet;
	marketIndex: number;
	marketTypeStr: 'spot' | 'perp';
}) => {
	let perpMarketIndexes: number[] = [];
	const spotMarketIndexes: number[] = [0];
	if (marketTypeStr.toLowerCase() === 'perp') {
		perpMarketIndexes = [marketIndex];
	} else if (marketTypeStr.toLowerCase() === 'spot') {
		spotMarketIndexes.push(marketIndex);
	} else {
		throw new Error('Invalid market type provided: ' + marketTypeStr);
	}
	const sdkConfig = initialize({ env: 'mainnet-beta' });
	const oracleInfo = getOracleInfoForMarket(
		sdkConfig,
		marketIndex,
		marketTypeStr
	);
	const driftClient = new DriftClient({
		connection,
		wallet: wallet,
		marketLookupTable: new PublicKey(
			'D9cnvzswDikQDf53k4HpQ3KJ9y1Fv3HGGDFYMXnK5T6c'
		),
		perpMarketIndexes,
		spotMarketIndexes,
		oracleInfos: [oracleInfo],
	});
	return driftClient;
};

export const spawnChildWithRetry = (
	scriptPath: string,
	childArgs: string[],
	processName: string,
	onMessage: (msg: any) => void,
	logPrefix = ''
): ChildProcess => {
	const child = fork(scriptPath, childArgs);

	child.on('message', onMessage);

	child.on('exit', (code) => {
		logger.info(
			`${logPrefix} Child process: ${processName} exited with code ${code}`
		);
		logger.info(`${logPrefix} Restarting child process: ${processName}`);
		spawnChildWithRetry(scriptPath, childArgs, processName, onMessage);
	});

	return child;
};

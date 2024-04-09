/* eslint-disable @typescript-eslint/no-non-null-assertion */
import {
	BlockhashSubscriber,
	BulkAccountLoader,
	DataAndSlot,
	decodeUser,
	DLOBNode,
	DriftClient,
	getOrderSignature,
	getUserAccountPublicKey,
	isFillableByVAMM,
	isOneOfVariant,
	isOracleValid,
	isOrderExpired,
	isVariant,
	MakerInfo,
	MarketType,
	PriorityFeeSubscriber,
	ReferrerInfo,
	SlotSubscriber,
	TxSigAndSlot,
	UserAccount,
	UserStatsMap,
} from '@drift-labs/sdk';
import { FillerMultiThreadedConfig, GlobalConfig } from '../../config';
import { BundleSender } from '../../bundleSender';
import {
	AddressLookupTableAccount,
	ComputeBudgetProgram,
	Connection,
	PublicKey,
	SendTransactionError,
	TransactionInstruction,
	VersionedTransaction,
} from '@solana/web3.js';
import { logger } from '../../logger';
import { getErrorCode } from '../../error';
import { selectMakers } from '../../makerSelection';
import {
	NodeToFillWithBuffer,
	SerializedNodeToTrigger,
	SerializedNodeToFill,
} from './types';
import { assert } from 'console';
import {
	getFillSignatureFromUserAccountAndOrderId,
	getNodeToFillSignature,
	logMessageForNodeToFill,
	simulateAndGetTxWithCUs,
	SimulateAndGetTxWithCUsResponse,
} from '../../utils';
import {
	spawnChildWithRetry,
	deserializeNodeToFill,
	deserializeOrder,
} from './utils';

const logPrefix = '[Filler]';

export type MakerNodeMap = Map<string, DLOBNode[]>;

const FILL_ORDER_THROTTLE_BACKOFF = 1000; // the time to wait before trying to fill a throttled (error filling) node again
const THROTTLED_NODE_SIZE_TO_PRUNE = 10; // Size of throttled nodes to get to before pruning the map
const TRIGGER_ORDER_COOLDOWN_MS = 1000; // the time to wait before trying to a node in the triggering map again
export const MAX_MAKERS_PER_FILL = 6; // max number of unique makers to include per fill
const MAX_ACCOUNTS_PER_TX = 64; // solana limit, track https://github.com/solana-labs/solana/issues/27241

const SETTLE_PNL_CHUNKS = 4;
const MAX_POSITIONS_PER_USER = 8;
export const SETTLE_POSITIVE_PNL_COOLDOWN_MS = 60_000;
const SIM_CU_ESTIMATE_MULTIPLIER = 1.15;
const SLOTS_UNTIL_JITO_LEADER_TO_SEND = 4;

const errorCodesToSuppress = [
	6004, // 0x1774 Error Number: 6004. Error Message: SufficientCollateral.
	6010, // 0x177a Error Number: 6010. Error Message: User Has No Position In Market.
	6081, // 0x17c1 Error Number: 6081. Error Message: MarketWrongMutability.
	// 6078, // 0x17BE Error Number: 6078. Error Message: PerpMarketNotFound
	// 6087, // 0x17c7 Error Number: 6087. Error Message: SpotMarketNotFound.
	6239, // 0x185F Error Number: 6239. Error Message: RevertFill.
	6003, // 0x1773 Error Number: 6003. Error Message: Insufficient collateral.
	6023, // 0x1787 Error Number: 6023. Error Message: PriceBandsBreached.

	6111, // Error Message: OrderNotTriggerable.
	6112, // Error Message: OrderDidNotSatisfyTriggerCondition.
];

const getNodeToTriggerSignature = (node: SerializedNodeToTrigger): string => {
	return getOrderSignature(node.node.order.orderId, node.node.userAccount);
};

export class FillerMultithreaded {
	private slotSubscriber: SlotSubscriber;
	private bundleSender?: BundleSender;
	private driftClient: DriftClient;
	private dryRun: boolean;
	private globalConfig: GlobalConfig;
	private config: FillerMultiThreadedConfig;

	private fillTxId: number = 0;
	private userStatsMap: UserStatsMap;
	private throttledNodes = new Map<string, number>();
	private fillingNodes = new Map<string, number>();
	private triggeringNodes = new Map<string, number>();
	private revertOnFailure: boolean = true;
	private lookupTableAccount?: AddressLookupTableAccount;
	private lastSettlePnl = Date.now() - SETTLE_POSITIVE_PNL_COOLDOWN_MS;
	private seenFillableOrders = new Set<string>();
	private seenTriggerableOrders = new Set<string>();
	private blockhashSubscriber: BlockhashSubscriber;
	private priorityFeeSubscriber: PriorityFeeSubscriber;

	private dlobHealthy = true;
	private orderSubscriberHealthy = true;
	private simulateTxForCUEstimate?: boolean;

	constructor(
		globalConfig: GlobalConfig,
		config: FillerMultiThreadedConfig,
		driftClient: DriftClient,
		slotSubscriber: SlotSubscriber,
		priorityFeeSubscriber: PriorityFeeSubscriber,
		bundleSender?: BundleSender
	) {
		this.globalConfig = globalConfig;
		this.config = config;
		this.dryRun = config.dryRun;
		this.slotSubscriber = slotSubscriber;
		this.driftClient = driftClient;
		this.bundleSender = bundleSender;
		this.simulateTxForCUEstimate = config.simulateTxForCUEstimate ?? true;

		this.userStatsMap = new UserStatsMap(
			this.driftClient,
			new BulkAccountLoader(
				new Connection(this.driftClient.connection.rpcEndpoint),
				'confirmed',
				0
			)
		);
		this.blockhashSubscriber = new BlockhashSubscriber({
			connection: driftClient.connection,
		});
		this.priorityFeeSubscriber = priorityFeeSubscriber;
		const marketType =
			this.config.marketType === 'perp' ? MarketType.PERP : MarketType.SPOT;
		this.priorityFeeSubscriber.updateMarketTypeAndIndex(
			marketType,
			this.config.marketIndex
		);
	}

	async init() {
		await this.blockhashSubscriber.subscribe();

		this.lookupTableAccount =
			await this.driftClient.fetchMarketLookupTableAccount();
		assert(this.lookupTableAccount, 'Lookup table account not found');
		this.startProcesses();
	}

	private startProcesses() {
		let dlobBuilderReady = false;
		const childArgs = [
			`--market-type=${this.config.marketType}`,
			`--market-index=${this.config.marketIndex}`,
		];
		const dlobBuilderProcess = spawnChildWithRetry(
			'./src/experimental-bots/filler/dlobBuilder.ts',
			childArgs,
			'dlobBuilder',
			(msg: any) => {
				switch (msg.type) {
					case 'initialized':
						dlobBuilderReady = true;
						logger.info(
							`${logPrefix} dlobBuilderProcess initialized and acknowledged`
						);
						break;
					case 'triggerableNodes':
						if (this.dryRun) {
							logger.info(`Triggerable node received`);
						} else {
							this.triggerNodes(msg.data);
						}
						break;
					case 'fillableNodes':
						if (this.dryRun) {
							logger.info(`Fillable node received`);
						} else {
							this.fillNodes(msg.data);
						}
						break;
					case 'health':
						this.dlobHealthy = msg.data.healthy;
						break;
				}
			},
			'[FillerMultithreaded]'
		);

		const orderSubscriberProcess = spawnChildWithRetry(
			'./src/experimental-bots/filler/orderSubscriberFiltered.ts',
			childArgs,
			'orderSubscriber',
			(msg: any) => {
				switch (msg.type) {
					case 'userAccountUpdate':
						if (dlobBuilderReady) {
							dlobBuilderProcess.send(msg);
						}
						break;
					case 'health':
						this.orderSubscriberHealthy = msg.data.healthy;
						break;
				}
			},
			'[FillerMultithreaded]'
		);

		process.on('SIGINT', () => {
			logger.info(`${logPrefix} Received SIGINT, killing children`);
			dlobBuilderProcess.kill();
			orderSubscriberProcess.kill();
			process.exit(0);
		});

		logger.info(`dlobBuilder spawned with pid: ${dlobBuilderProcess.pid}`);
		logger.info(
			`orderSubscriber spawned with pid: ${orderSubscriberProcess.pid}`
		);
	}

	public healthCheck(): boolean {
		if (!this.dlobHealthy) {
			logger.error(`${logPrefix} DLOB not healthy`);
		}
		if (!this.orderSubscriberHealthy) {
			logger.error(`${logPrefix} Order subscriber not healthy`);
		}
		return this.dlobHealthy && this.orderSubscriberHealthy;
	}

	private async getBlockhashForTx(): Promise<string> {
		const cachedBlockhash = this.blockhashSubscriber.getLatestBlockhash(10);
		if (cachedBlockhash) {
			return cachedBlockhash.blockhash as string;
		}

		const recentBlockhash =
			await this.driftClient.connection.getLatestBlockhash({
				commitment: 'confirmed',
			});

		return recentBlockhash.blockhash;
	}

	protected removeFillingNodes(nodes: Array<NodeToFillWithBuffer>) {
		for (const node of nodes) {
			this.fillingNodes.delete(getNodeToFillSignature(node));
		}
	}

	protected isThrottledNodeStillThrottled(throttleKey: string): boolean {
		const lastFillAttempt = this.throttledNodes.get(throttleKey) || 0;
		if (lastFillAttempt + FILL_ORDER_THROTTLE_BACKOFF > Date.now()) {
			return true;
		} else {
			this.clearThrottledNode(throttleKey);
			return false;
		}
	}

	protected isDLOBNodeThrottled(dlobNode: DLOBNode): boolean {
		if (!dlobNode.userAccount || !dlobNode.order) {
			return false;
		}

		// first check if the userAccount itself is throttled
		const userAccountPubkey = dlobNode.userAccount;
		if (this.throttledNodes.has(userAccountPubkey)) {
			if (this.isThrottledNodeStillThrottled(userAccountPubkey)) {
				return true;
			} else {
				return false;
			}
		}

		// then check if the specific order is throttled
		const orderSignature = getFillSignatureFromUserAccountAndOrderId(
			dlobNode.userAccount,
			dlobNode.order.orderId.toString()
		);
		if (this.throttledNodes.has(orderSignature)) {
			if (this.isThrottledNodeStillThrottled(orderSignature)) {
				return true;
			} else {
				return false;
			}
		}

		return false;
	}

	protected clearThrottledNode(signature: string) {
		this.throttledNodes.delete(signature);
	}

	protected setThrottledNode(signature: string) {
		this.throttledNodes.set(signature, Date.now());
	}

	protected removeTriggeringNodes(node: SerializedNodeToTrigger) {
		this.triggeringNodes.delete(getNodeToTriggerSignature(node));
	}

	protected pruneThrottledNode() {
		if (this.throttledNodes.size > THROTTLED_NODE_SIZE_TO_PRUNE) {
			for (const [key, value] of this.throttledNodes.entries()) {
				if (value + 2 * FILL_ORDER_THROTTLE_BACKOFF > Date.now()) {
					this.throttledNodes.delete(key);
				}
			}
		}
	}

	protected async sendTxThroughJito(
		tx: VersionedTransaction,
		metadata: number | string
	) {
		const blockhash = await this.getBlockhashForTx();
		tx.message.recentBlockhash = blockhash;

		// @ts-ignore;
		tx.sign([this.driftClient.wallet.payer]);

		if (this.bundleSender === undefined) {
			logger.error(
				`${logPrefix} Called sendTxThroughJito without jito properly enabled`
			);
			return;
		}
		const slotsUntilNextLeader = this.bundleSender?.slotsUntilNextLeader();
		if (slotsUntilNextLeader !== undefined) {
			this.bundleSender.sendTransaction(tx, `(fillTxId: ${metadata})`);
		}
	}

	protected slotsUntilJitoLeader(): number | undefined {
		return this.bundleSender?.slotsUntilNextLeader();
	}

	public async triggerNodes(
		serializedNodesToTrigger: SerializedNodeToTrigger[]
	) {
		logger.info(
			`${logPrefix} Triggering ${serializedNodesToTrigger.length} nodes...`
		);
		const seenTriggerableNodes = new Set<string>();
		const filteredTriggerableNodes = serializedNodesToTrigger.filter((node) => {
			const sig = getNodeToTriggerSignature(node);
			if (seenTriggerableNodes.has(sig)) {
				return false;
			}
			seenTriggerableNodes.add(sig);
			return this.filterTriggerableNodes(node);
		});
		logger.info(
			`${logPrefix} Filtered down to ${filteredTriggerableNodes.length} triggerable nodes...`
		);

		const slotsUntilJito = this.slotsUntilJitoLeader();
		const buildForBundle =
			slotsUntilJito !== undefined &&
			slotsUntilJito < SLOTS_UNTIL_JITO_LEADER_TO_SEND;
		try {
			await this.executeTriggerablePerpNodes(
				filteredTriggerableNodes,
				buildForBundle
			);
		} catch (e) {
			if (e instanceof Error) {
				logger.error(
					`${logPrefix} Error triggering nodes: ${
						e.stack ? e.stack : e.message
					}`
				);
			}
		}
	}

	protected filterTriggerableNodes(
		nodeToTrigger: SerializedNodeToTrigger
	): boolean {
		if (nodeToTrigger.node.haveTrigger) {
			return false;
		}

		const now = Date.now();
		const nodeToFillSignature = getNodeToTriggerSignature(nodeToTrigger);
		const timeStartedToTriggerNode =
			this.triggeringNodes.get(nodeToFillSignature);
		if (timeStartedToTriggerNode) {
			if (timeStartedToTriggerNode + TRIGGER_ORDER_COOLDOWN_MS > now) {
				return false;
			}
		}

		return true;
	}

	async executeTriggerablePerpNodes(
		nodesToTrigger: SerializedNodeToTrigger[],
		buildForBundle: boolean
	) {
		for (const nodeToTrigger of nodesToTrigger) {
			nodeToTrigger.node.haveTrigger = true;
			// @ts-ignore
			const buffer = Buffer.from(nodeToTrigger.node.userAccountData.data);
			// @ts-ignore
			const userAccount = decodeUser(buffer);

			logger.info(
				`${logPrefix} trying to trigger (account: ${
					nodeToTrigger.node.userAccount
				}, order ${nodeToTrigger.node.order.orderId.toString()}`
			);

			const nodeSignature = getNodeToTriggerSignature(nodeToTrigger);
			if (this.seenTriggerableOrders.has(nodeSignature)) {
				logger.debug(
					`${logPrefix} already triggered order (account: ${
						nodeToTrigger.node.userAccount
					}, order ${nodeToTrigger.node.order.orderId.toString()}`
				);
				return;
			}
			this.seenTriggerableOrders.add(nodeSignature);
			this.triggeringNodes.set(nodeSignature, Date.now());

			const ixs = [];
			ixs.push(
				await this.driftClient.getTriggerOrderIx(
					new PublicKey(nodeToTrigger.node.userAccount),
					userAccount,
					deserializeOrder(nodeToTrigger.node.order)
				)
			);

			if (this.revertOnFailure) {
				ixs.push(await this.driftClient.getRevertFillIx());
			}

			const simResult = await simulateAndGetTxWithCUs(
				ixs,
				this.driftClient.connection,
				this.driftClient.txSender,
				[this.lookupTableAccount!],
				[],
				this.driftClient.opts,
				SIM_CU_ESTIMATE_MULTIPLIER,
				this.simulateTxForCUEstimate,
				await this.getBlockhashForTx()
			);

			logger.info(
				`executeTriggerablePerpNodesForMarket estimated CUs: ${simResult.cuEstimate}`
			);

			if (simResult.simError) {
				logger.error(
					`executeTriggerablePerpNodesForMarket simError: (simError: ${JSON.stringify(
						simResult.simError
					)})`
				);
			} else {
				if (buildForBundle) {
					this.sendTxThroughJito(simResult.tx, 'triggerOrder');
				} else {
					const blockhash = await this.getBlockhashForTx();
					simResult.tx.message.recentBlockhash = blockhash;
					this.driftClient
						.sendTransaction(simResult.tx)
						.then((txSig) => {
							logger.info(
								`Triggered user (account: ${nodeToTrigger.node.userAccount.toString()}) order: ${nodeToTrigger.node.order.orderId.toString()}`
							);
							logger.info(`${logPrefix} Tx: ${txSig}`);
						})
						.catch((error) => {
							nodeToTrigger.node.haveTrigger = false;

							const errorCode = getErrorCode(error);
							if (
								errorCode &&
								!errorCodesToSuppress.includes(errorCode) &&
								!(error as Error).message.includes(
									'Transaction was not confirmed'
								)
							) {
								logger.error(
									`Error (${errorCode}) triggering order for user (account: ${nodeToTrigger.node.userAccount.toString()}) order: ${nodeToTrigger.node.order.orderId.toString()}`
								);
								logger.error(error);
							}
						})
						.finally(() => {
							this.removeTriggeringNodes(nodeToTrigger);
						});
				}
			}
		}
	}

	public async fillNodes(serializedNodesToFill: SerializedNodeToFill[]) {
		logger.debug(
			`${logPrefix} Filling ${serializedNodesToFill.length} nodes...`
		);
		const deserializedNodesToFill = serializedNodesToFill.map(
			deserializeNodeToFill
		);
		const seenFillableNodes = new Set<string>();
		const filteredFillableNodes = deserializedNodesToFill.filter((node) => {
			const sig = getNodeToFillSignature(node);
			if (seenFillableNodes.has(sig)) {
				return false;
			}
			seenFillableNodes.add(sig);
			return this.filterFillableNodes(node);
		});
		logger.debug(
			`${logPrefix} Filtered down to ${filteredFillableNodes.length} fillable nodes...`
		);

		const slotsUntilJito = this.slotsUntilJitoLeader();
		const buildForBundle =
			slotsUntilJito !== undefined &&
			slotsUntilJito < SLOTS_UNTIL_JITO_LEADER_TO_SEND;

		try {
			await this.executeFillablePerpNodesForMarket(
				filteredFillableNodes,
				buildForBundle
			);
		} catch (e) {
			if (e instanceof Error) {
				logger.error(
					`${logPrefix} Error filling nodes: ${e.stack ? e.stack : e.message}`
				);
			}
		}
	}

	protected filterFillableNodes(nodeToFill: NodeToFillWithBuffer): boolean {
		if (!nodeToFill.node.order) {
			return false;
		}

		if (nodeToFill.node.isVammNode()) {
			logger.warn(
				`filtered out a vAMM node on market ${nodeToFill.node.order.marketIndex} for user ${nodeToFill.node.userAccount}-${nodeToFill.node.order.orderId}`
			);
			return false;
		}

		if (nodeToFill.node.haveFilled) {
			logger.warn(
				`filtered out filled node on market ${nodeToFill.node.order.marketIndex} for user ${nodeToFill.node.userAccount}-${nodeToFill.node.order.orderId}`
			);
			return false;
		}

		const now = Date.now();
		const nodeToFillSignature = getNodeToFillSignature(nodeToFill);
		if (this.fillingNodes.has(nodeToFillSignature)) {
			const timeStartedToFillNode =
				this.fillingNodes.get(nodeToFillSignature) || 0;
			if (timeStartedToFillNode + FILL_ORDER_THROTTLE_BACKOFF > now) {
				// still cooling down on this node, filter it out
				return false;
			}
		}

		// check if taker node is throttled
		if (this.isDLOBNodeThrottled(nodeToFill.node)) {
			return false;
		}

		const marketIndex = nodeToFill.node.order.marketIndex;
		const oraclePriceData =
			this.driftClient.getOracleDataForPerpMarket(marketIndex);

		if (isOrderExpired(nodeToFill.node.order, Date.now() / 1000)) {
			if (isOneOfVariant(nodeToFill.node.order.orderType, ['limit'])) {
				// do not try to fill (expire) limit orders b/c they will auto expire when filled against
				// or the user places a new order
				return false;
			}
			return true;
		}

		if (
			nodeToFill.makerNodes.length === 0 &&
			isVariant(nodeToFill.node.order.marketType, 'perp') &&
			!isFillableByVAMM(
				nodeToFill.node.order,
				this.driftClient.getPerpMarketAccount(
					nodeToFill.node.order.marketIndex
				)!,
				oraclePriceData,
				this.slotSubscriber.getSlot(),
				Date.now() / 1000,
				this.driftClient.getStateAccount().minPerpAuctionDuration
			)
		) {
			return false;
		}

		const perpMarket = this.driftClient.getPerpMarketAccount(
			nodeToFill.node.order.marketIndex
		)!;
		// if making with vAMM, ensure valid oracle
		if (
			nodeToFill.makerNodes.length === 0 &&
			!isVariant(perpMarket.amm.oracleSource, 'prelaunch')
		) {
			const oracleIsValid = isOracleValid(
				perpMarket,
				oraclePriceData,
				this.driftClient.getStateAccount().oracleGuardRails,
				this.slotSubscriber.getSlot()
			);
			if (!oracleIsValid) {
				logger.error(
					`${logPrefix} Oracle is not valid for market ${marketIndex}`
				);
				return false;
			}
		}

		return true;
	}

	async executeFillablePerpNodesForMarket(
		nodesToFill: NodeToFillWithBuffer[],
		buildForBundle: boolean
	) {
		for (const node of nodesToFill) {
			if (this.seenFillableOrders.has(getNodeToFillSignature(node))) {
				logger.debug(
					// @ts-ignore
					`${logPrefix} already filled order (account: ${
						node.node.userAccount
					}, order ${node.node.order?.orderId.toString()}`
				);
				return;
			}
			this.seenFillableOrders.add(getNodeToFillSignature(node));
			if (node.makerNodes.length > 1) {
				this.tryFillMultiMakerPerpNodes(node, buildForBundle);
			} else {
				this.tryFillPerpNode(node, buildForBundle);
			}
		}
	}

	protected async tryFillMultiMakerPerpNodes(
		nodeToFill: NodeToFillWithBuffer,
		buildForBundle: boolean
	) {
		const fillTxId = this.fillTxId++;

		let nodeWithMakerSet = nodeToFill;
		while (
			!(await this.fillMultiMakerPerpNodes(
				fillTxId,
				nodeWithMakerSet,
				buildForBundle
			))
		) {
			const newMakerSet = nodeWithMakerSet.makerNodes
				.sort(() => 0.5 - Math.random())
				.slice(0, Math.ceil(nodeWithMakerSet.makerNodes.length / 2));
			nodeWithMakerSet = {
				userAccountData: nodeWithMakerSet.userAccountData,
				makerAccountData: nodeWithMakerSet.makerAccountData,
				node: nodeWithMakerSet.node,
				makerNodes: newMakerSet,
			};
			if (newMakerSet.length === 0) {
				logger.error(
					`No makers left to use for multi maker perp node (fillTxId: ${fillTxId})`
				);
				return;
			}
		}
	}

	private async fillMultiMakerPerpNodes(
		fillTxId: number,
		nodeToFill: NodeToFillWithBuffer,
		buildForBundle: boolean
	): Promise<boolean> {
		const ixs: Array<TransactionInstruction> = [
			ComputeBudgetProgram.setComputeUnitLimit({
				units: 1_400_000,
			}),
		];
		if (!buildForBundle) {
			ixs.push(
				ComputeBudgetProgram.setComputeUnitPrice({
					microLamports: Math.floor(
						this.priorityFeeSubscriber.getCustomStrategyResult()
					),
				})
			);
		}

		try {
			const {
				makerInfos,
				takerUser,
				takerUserPubKey,
				takerUserSlot,
				referrerInfo,
				marketType,
			} = await this.getNodeFillInfo(nodeToFill);

			logger.info(
				logMessageForNodeToFill(
					nodeToFill,
					takerUserPubKey,
					takerUserSlot,
					makerInfos,
					this.slotSubscriber.getSlot(),
					`${logPrefix} Filling multi maker perp node with ${nodeToFill.makerNodes.length} makers (fillTxId: ${fillTxId})`
				)
			);

			if (!isVariant(marketType, 'perp')) {
				throw new Error('expected perp market type');
			}

			let makerInfosToUse = makerInfos;
			const buildTxWithMakerInfos = async (
				makers: DataAndSlot<MakerInfo>[]
			): Promise<SimulateAndGetTxWithCUsResponse> => {
				ixs.push(
					await this.driftClient.getFillPerpOrderIx(
						await getUserAccountPublicKey(
							this.driftClient.program.programId,
							takerUser.authority,
							takerUser.subAccountId
						),
						takerUser,
						nodeToFill.node.order!,
						makers.map((m) => m.data),
						referrerInfo
					)
				);

				this.fillingNodes.set(getNodeToFillSignature(nodeToFill), Date.now());

				if (this.revertOnFailure) {
					ixs.push(await this.driftClient.getRevertFillIx());
				}
				const simResult = await simulateAndGetTxWithCUs(
					ixs,
					this.driftClient.connection,
					this.driftClient.txSender,
					[this.lookupTableAccount!],
					[],
					this.driftClient.opts,
					SIM_CU_ESTIMATE_MULTIPLIER,
					this.simulateTxForCUEstimate,
					await this.getBlockhashForTx()
				);
				return simResult;
			};

			let simResult = await buildTxWithMakerInfos(makerInfosToUse);
			let txAccounts = simResult.tx.message.getAccountKeys({
				addressLookupTableAccounts: [this.lookupTableAccount!],
			}).length;
			let attempt = 0;
			while (txAccounts > MAX_ACCOUNTS_PER_TX && makerInfosToUse.length > 0) {
				logger.info(
					`${logPrefix} (fillTxId: ${fillTxId} attempt ${attempt++}) Too many accounts, remove 1 and try again (had ${
						makerInfosToUse.length
					} maker and ${txAccounts} accounts)`
				);
				makerInfosToUse = makerInfosToUse.slice(0, makerInfosToUse.length - 1);
				simResult = await buildTxWithMakerInfos(makerInfosToUse);
				txAccounts = simResult.tx.message.getAccountKeys({
					addressLookupTableAccounts: [this.lookupTableAccount!],
				}).length;
			}

			if (makerInfosToUse.length === 0) {
				logger.error(
					`${logPrefix} No makerInfos left to use for multi maker perp node (fillTxId: ${fillTxId})`
				);
				return true;
			}

			logger.info(
				`${logPrefix} tryFillMultiMakerPerpNodes estimated CUs: ${simResult.cuEstimate} (fillTxId: ${fillTxId})`
			);

			if (simResult.simError) {
				logger.error(
					`${logPrefix} Error simulating multi maker perp node (fillTxId: ${fillTxId}): ${JSON.stringify(
						simResult.simError
					)}\nTaker slot: ${takerUserSlot}\nMaker slots: ${makerInfosToUse
						.map((m) => `  ${m.data.maker.toBase58()}: ${m.slot}`)
						.join('\n')}`
				);
			} else {
				this.sendFillTxAndParseLogs(
					fillTxId,
					[nodeToFill],
					simResult.tx,
					buildForBundle
				);
			}
		} catch (e) {
			if (e instanceof Error) {
				logger.error(
					`${logPrefix} Error filling multi maker perp node (fillTxId: ${fillTxId}): ${
						e.stack ? e.stack : e.message
					}`
				);
			}
		}
		return true;
	}

	protected async tryFillPerpNode(
		nodeToFill: NodeToFillWithBuffer,
		buildForBundle: boolean
	) {
		const ixs = [
			ComputeBudgetProgram.setComputeUnitLimit({
				units: 1_400_000,
			}),
		];

		if (!buildForBundle) {
			ixs.push(
				ComputeBudgetProgram.setComputeUnitPrice({
					microLamports: Math.floor(
						this.priorityFeeSubscriber.getCustomStrategyResult()
					),
				})
			);
		}
		const fillTxId = this.fillTxId++;

		const {
			makerInfos,
			takerUser,
			takerUserPubKey,
			takerUserSlot,
			referrerInfo,
			marketType,
		} = await this.getNodeFillInfo(nodeToFill);
		logger.info(
			logMessageForNodeToFill(
				nodeToFill,
				takerUserPubKey,
				takerUserSlot,
				makerInfos,
				this.slotSubscriber.getSlot(),
				`Filling perp node (fillTxId: ${fillTxId})`
			)
		);

		if (!isVariant(marketType, 'perp')) {
			throw new Error('expected perp market type');
		}

		const ix = await this.driftClient.getFillPerpOrderIx(
			await getUserAccountPublicKey(
				this.driftClient.program.programId,
				takerUser.authority,
				takerUser.subAccountId
			),
			takerUser,
			nodeToFill.node.order!,
			makerInfos.map((m) => m.data),
			referrerInfo
		);

		ixs.push(ix);

		if (this.revertOnFailure) {
			ixs.push(await this.driftClient.getRevertFillIx());
		}

		const simResult = await simulateAndGetTxWithCUs(
			ixs,
			this.driftClient.connection,
			this.driftClient.txSender,
			[this.lookupTableAccount!],
			[],
			this.driftClient.opts,
			SIM_CU_ESTIMATE_MULTIPLIER,
			this.simulateTxForCUEstimate
		);
		logger.info(
			`tryFillPerpNode estimated CUs: ${simResult.cuEstimate} (fillTxId: ${fillTxId})`
		);

		if (simResult.simError) {
			logger.error(
				`simError: ${JSON.stringify(
					simResult.simError
				)} (fillTxId: ${fillTxId})`
			);
		} else {
			this.sendFillTxAndParseLogs(
				fillTxId,
				[nodeToFill],
				simResult.tx,
				buildForBundle
			);
		}
	}

	protected async sendFillTxAndParseLogs(
		fillTxId: number,
		nodesSent: Array<NodeToFillWithBuffer>,
		tx: VersionedTransaction,
		buildForBundle: boolean
	) {
		let txResp: Promise<TxSigAndSlot> | undefined = undefined;
		let estTxSize: number | undefined = undefined;
		let txAccounts = 0;
		let writeAccs = 0;
		const accountMetas: any[] = [];
		const txStart = Date.now();
		// tx.message.recentBlockhash = await this.getBlockhashForTx();
		if (buildForBundle) {
			await this.sendTxThroughJito(tx, fillTxId);
			this.removeFillingNodes(nodesSent);
		} else {
			estTxSize = tx.message.serialize().length;
			const acc = tx.message.getAccountKeys({
				addressLookupTableAccounts: [this.lookupTableAccount!],
			});
			txAccounts = acc.length;
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

			txResp = this.driftClient.txSender.sendVersionedTransaction(
				tx,
				[],
				this.driftClient.opts
			);
		}
		if (txResp) {
			txResp
				.then((resp: TxSigAndSlot) => {
					const duration = Date.now() - txStart;
					logger.info(
						`${logPrefix} sent tx: ${resp.txSig}, took: ${duration}ms (fillTxId: ${fillTxId})`
					);
				})
				.catch(async (e) => {
					const simError = e as SendTransactionError;
					logger.error(
						`${logPrefix} Failed to send packed tx txAccountKeys: ${txAccounts} (${writeAccs} writeable) (fillTxId: ${fillTxId}), error: ${simError.message}`
					);

					if (e.message.includes('too large:')) {
						logger.error(
							`${logPrefix}: :boxing_glove: Tx too large, estimated to be ${estTxSize} (fillId: ${fillTxId}). ${
								e.message
							}\n${JSON.stringify(accountMetas)}`
						);
						return;
					}

					if (simError.logs && simError.logs.length > 0) {
						const errorCode = getErrorCode(e);
						logger.error(
							`${logPrefix} Failed to send tx, sim error (fillTxId: ${fillTxId}) error code: ${errorCode}`
						);
					}
				})
				.finally(() => {
					this.removeFillingNodes(nodesSent);
				});
		}
	}

	public async settlePnls() {
		const user = this.driftClient.getUser();
		const marketIds = user
			.getActivePerpPositions()
			.map((pos) => pos.marketIndex);
		const now = Date.now();
		if (marketIds.length === MAX_POSITIONS_PER_USER) {
			if (now < this.lastSettlePnl + SETTLE_POSITIVE_PNL_COOLDOWN_MS) {
				logger.info(
					`${logPrefix} Want to settle positive pnl, but in cooldown...`
				);
			} else {
				const settlePnlPromises: Array<Promise<TxSigAndSlot>> = [];
				for (let i = 0; i < marketIds.length; i += SETTLE_PNL_CHUNKS) {
					const marketIdChunks = marketIds.slice(i, i + SETTLE_PNL_CHUNKS);
					try {
						const ixs = [
							ComputeBudgetProgram.setComputeUnitLimit({
								units: 1_400_000,
							}),
						];
						ixs.push(
							...(await this.driftClient.getSettlePNLsIxs(
								[
									{
										settleeUserAccountPublicKey: user.getUserAccountPublicKey(),
										settleeUserAccount: this.driftClient.getUserAccount()!,
									},
								],
								marketIdChunks
							))
						);

						const simResult = await simulateAndGetTxWithCUs(
							ixs,
							this.driftClient.connection,
							this.driftClient.txSender,
							[this.lookupTableAccount!],
							[],
							this.driftClient.opts,
							SIM_CU_ESTIMATE_MULTIPLIER,
							this.simulateTxForCUEstimate,
							await this.getBlockhashForTx()
						);
						logger.info(
							`${logPrefix} settlePnls estimatedCUs: ${simResult.cuEstimate}`
						);
						if (simResult.simError) {
							logger.info(
								`settlePnls simError: ${JSON.stringify(simResult.simError)}`
							);
						} else {
							const slotsUntilJito = this.slotsUntilJitoLeader();
							const buildForBundle =
								slotsUntilJito !== undefined &&
								slotsUntilJito < SLOTS_UNTIL_JITO_LEADER_TO_SEND;
							if (buildForBundle) {
								this.sendTxThroughJito(simResult.tx, 'settlePnl');
							} else {
								settlePnlPromises.push(
									this.driftClient.txSender.sendVersionedTransaction(
										simResult.tx,
										[],
										this.driftClient.opts
									)
								);
							}
						}
					} catch (err) {
						if (!(err instanceof Error)) {
							return;
						}
						const errorCode = getErrorCode(err) ?? 0;
						logger.error(
							`Error code: ${errorCode} while settling pnls for markets ${JSON.stringify(
								marketIds
							)}: ${err.message}`
						);
						console.error(err);
					}
				}
				try {
					const txs = await Promise.all(settlePnlPromises);
					for (const tx of txs) {
						logger.info(
							`Settle positive PNLs tx: https://solscan/io/tx/${tx.txSig}`
						);
					}
				} catch (e) {
					logger.error(`${logPrefix} Error settling positive pnls: ${e}`);
				}
				this.lastSettlePnl = now;
			}
		}
	}

	protected async getNodeFillInfo(nodeToFill: NodeToFillWithBuffer): Promise<{
		makerInfos: Array<DataAndSlot<MakerInfo>>;
		takerUserPubKey: string;
		takerUser: UserAccount;
		takerUserSlot: number;
		referrerInfo: ReferrerInfo | undefined;
		marketType: MarketType;
	}> {
		const makerInfos: Array<DataAndSlot<MakerInfo>> = [];

		if (nodeToFill.makerNodes.length > 0) {
			let makerNodesMap: MakerNodeMap = new Map<string, DLOBNode[]>();
			for (const makerNode of nodeToFill.makerNodes) {
				if (this.isDLOBNodeThrottled(makerNode)) {
					continue;
				}

				if (!makerNode.userAccount) {
					continue;
				}

				if (makerNodesMap.has(makerNode.userAccount!)) {
					makerNodesMap.get(makerNode.userAccount!)!.push(makerNode);
				} else {
					makerNodesMap.set(makerNode.userAccount!, [makerNode]);
				}
			}

			if (makerNodesMap.size > MAX_MAKERS_PER_FILL) {
				logger.info(`selecting from ${makerNodesMap.size} makers`);
				makerNodesMap = selectMakers(makerNodesMap);
				logger.info(`selected: ${Array.from(makerNodesMap.keys()).join(',')}`);
			}

			const makerInfoMap = new Map(JSON.parse(nodeToFill.makerAccountData));
			for (const [makerAccount, makerNodes] of makerNodesMap) {
				const makerNode = makerNodes[0];
				const makerUserAccount = decodeUser(
					// @ts-ignore
					Buffer.from(makerInfoMap.get(makerAccount)!.data)
				);
				const makerAuthority = makerUserAccount.authority;
				const makerUserStats = (
					await this.userStatsMap!.mustGet(makerAuthority.toString())
				).userStatsAccountPublicKey;
				makerInfos.push({
					slot: this.slotSubscriber.getSlot(),
					data: {
						maker: new PublicKey(makerAccount),
						makerUserAccount: makerUserAccount,
						order: makerNode.order,
						makerStats: makerUserStats,
					},
				});
			}
		}

		const takerUserPubKey = nodeToFill.node.userAccount!.toString();
		const takerUserAccount = decodeUser(
			// @ts-ignore
			Buffer.from(nodeToFill.userAccountData.data)
		);
		const referrerInfo = (
			await this.userStatsMap!.mustGet(takerUserAccount.authority.toString())
		).getReferrerInfo();

		return Promise.resolve({
			makerInfos,
			takerUserPubKey,
			takerUser: takerUserAccount,
			takerUserSlot: this.slotSubscriber.getSlot(),
			referrerInfo,
			marketType: nodeToFill.node.order!.marketType,
		});
	}
}

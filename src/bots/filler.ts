import {
	DriftEnv,
	NewUserRecord,
	OrderRecord,
	User,
	ReferrerInfo,
	isOracleValid,
	DriftClient,
	PerpMarketAccount,
	SlotSubscriber,
	calculateAskPrice,
	calculateBidPrice,
	MakerInfo,
	isFillableByVAMM,
	calculateBaseAssetAmountForAmmToFulfill,
	isVariant,
	DLOB,
	NodeToFill,
	UserMap,
	UserStatsMap,
	MarketType,
	isOrderExpired,
	getVariant,
} from '@drift-labs/sdk';
import { TxSigAndSlot } from '@drift-labs/sdk/lib/tx/types';
import { Mutex, tryAcquire, withTimeout, E_ALREADY_LOCKED } from 'async-mutex';

import {
	SendTransactionError,
	Transaction,
	TransactionResponse,
	TransactionSignature,
	TransactionInstruction,
	ComputeBudgetProgram,
} from '@solana/web3.js';

import { logger } from '../logger';
import { Bot } from '../types';
import { Metrics } from '../metrics';

const MAX_TX_PACK_SIZE = 900; //1232;
const CU_PER_FILL = 200_000; // CU cost for a successful fill
const BURST_CU_PER_FILL = 350_000; // CU cost for a successful fill
const MAX_CU_PER_TX = 1_400_000; // seems like this is all budget program gives us...on devnet
const TX_COUNT_COOLDOWN_ON_BURST = 10; // send this many tx before resetting burst mode
const FILL_ORDER_BACKOFF = 1000;
const dlobMutexError = new Error('dlobMutex timeout');

export class FillerBot implements Bot {
	public readonly name: string;
	public readonly dryRun: boolean;
	public readonly defaultIntervalMs: number = 3000;

	private driftEnv: DriftEnv;
	private clearingHouse: DriftClient;
	private slotSubscriber: SlotSubscriber;

	private dlobMutex = withTimeout(
		new Mutex(),
		10 * this.defaultIntervalMs,
		dlobMutexError
	);
	private dlob: DLOB;

	private userMap: UserMap;
	private userStatsMap: UserStatsMap;

	private periodicTaskMutex = new Mutex();

	private watchdogTimerMutex = new Mutex();
	private watchdogTimerLastPatTime = Date.now();

	private intervalIds: Array<NodeJS.Timer> = [];
	private metrics: Metrics | undefined;
	private throttledNodes = new Map<string, number>();
	private useBurstCULimit = false;
	private fillTxSinceBurstCU = 0;

	constructor(
		name: string,
		dryRun: boolean,
		clearingHouse: DriftClient,
		slotSubscriber: SlotSubscriber,
		env: DriftEnv,
		metrics?: Metrics | undefined
	) {
		this.name = name;
		this.dryRun = dryRun;
		this.clearingHouse = clearingHouse;
		this.slotSubscriber = slotSubscriber;
		this.metrics = metrics;
		this.driftEnv = env;
	}

	public async init() {
		logger.info(`${this.name} initing`);

		const initPromises: Array<Promise<any>> = [];

		this.userMap = new UserMap(
			this.clearingHouse,
			this.clearingHouse.userAccountSubscriptionConfig
		);
		initPromises.push(this.userMap.fetchAllUsers());

		this.userStatsMap = new UserStatsMap(
			this.clearingHouse,
			this.clearingHouse.userAccountSubscriptionConfig
		);
		initPromises.push(this.userStatsMap.fetchAllUserStats());

		await Promise.all(initPromises);
	}

	public async reset() {}

	public async startIntervalLoop(intervalMs: number) {
		// await this.tryFill();
		const intervalId = setInterval(this.tryFill.bind(this), intervalMs);
		this.intervalIds.push(intervalId);

		logger.info(`${this.name} Bot started!`);
	}

	public async healthCheck(): Promise<boolean> {
		let healthy = false;
		await this.watchdogTimerMutex.runExclusive(async () => {
			healthy =
				this.watchdogTimerLastPatTime > Date.now() - 5 * this.defaultIntervalMs;
		});
		return healthy;
	}

	public async trigger(record: any) {
		if (record.eventType === 'OrderRecord') {
			await this.userMap.updateWithOrderRecord(record as OrderRecord);
			await this.userStatsMap.updateWithOrderRecord(
				record as OrderRecord,
				this.userMap
			);
			await this.tryFill();
		} else if (record.eventType === 'NewUserRecord') {
			await this.userMap.mustGet((record as NewUserRecord).user.toString());
			await this.userStatsMap.mustGet(
				(record as NewUserRecord).user.toString()
			);
		}
	}

	public viewDlob(): DLOB {
		return this.dlob;
	}

	private async getPerpFillableNodesForMarket(
		market: PerpMarketAccount
	): Promise<Array<NodeToFill>> {
		const marketIndex = market.marketIndex;

		const oraclePriceData =
			this.clearingHouse.getOracleDataForPerpMarket(marketIndex);

		const vAsk = calculateAskPrice(market, oraclePriceData);
		const vBid = calculateBidPrice(market, oraclePriceData);

		let nodes: Array<NodeToFill> = [];
		await this.dlobMutex.runExclusive(async () => {
			nodes = this.dlob.findNodesToFill(
				marketIndex,
				vBid,
				vAsk,
				this.slotSubscriber.getSlot(),
				Date.now() / 1000,
				MarketType.PERP,
				oraclePriceData
			);
		});

		return nodes;
	}

	private getNodeToFillSignature(node: NodeToFill): string {
		if (!node.node.userAccount) {
			return '~';
		}
		return this.getFillSignatureFromUserAccountAndOrderId(
			node.node.userAccount.toString(),
			node.node.order.orderId.toString()
		);
	}

	private getFillSignatureFromUserAccountAndOrderId(
		userAccount: string,
		orderId: string
	): string {
		return `${userAccount}-${orderId}`;
	}

	private filterFillableNodes(nodeToFill: NodeToFill): boolean {
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

		if (this.throttledNodes.has(this.getNodeToFillSignature(nodeToFill))) {
			const lastFillAttempt = this.throttledNodes.get(
				this.getNodeToFillSignature(nodeToFill)
			);
			const now = Date.now();
			if (lastFillAttempt + FILL_ORDER_BACKOFF > now) {
				logger.warn(
					`skipping node (throttled, retry in ${
						lastFillAttempt + FILL_ORDER_BACKOFF - now
					}ms) on market ${nodeToFill.node.order.marketIndex} for user ${
						nodeToFill.node.userAccount
					}-${nodeToFill.node.order.orderId}`
				);
				return false;
			} else {
				this.throttledNodes.delete(this.getNodeToFillSignature(nodeToFill));
			}
		}

		const marketIndex = nodeToFill.node.market.marketIndex;
		const oraclePriceData =
			this.clearingHouse.getOracleDataForPerpMarket(marketIndex);

		// return early to fill if order is expired
		if (isOrderExpired(nodeToFill.node.order, Date.now() / 1000)) {
			logger.warn(
				`order is expired on market ${nodeToFill.node.order.marketIndex} for user ${nodeToFill.node.userAccount}-${nodeToFill.node.order.orderId}`
			);
			return true;
		}

		if (
			!nodeToFill.makerNode &&
			isVariant(nodeToFill.node.order.marketType, 'perp') &&
			!isFillableByVAMM(
				nodeToFill.node.order,
				nodeToFill.node.market as PerpMarketAccount,
				oraclePriceData,
				this.slotSubscriber.getSlot(),
				Date.now() / 1000
			)
		) {
			logger.warn(
				`filtered out unfillable node on market ${nodeToFill.node.order.marketIndex} for user ${nodeToFill.node.userAccount}-${nodeToFill.node.order.orderId}`
			);
			logger.warn(` . no maker node: ${!nodeToFill.makerNode}`);
			logger.warn(
				` . is perp: ${isVariant(nodeToFill.node.order.marketType, 'perp')}`
			);
			logger.warn(
				` . is not fillable by vamm: ${!isFillableByVAMM(
					nodeToFill.node.order,
					nodeToFill.node.market as PerpMarketAccount,
					oraclePriceData,
					this.slotSubscriber.getSlot(),
					Date.now() / 1000
				)}`
			);
			logger.warn(
				` .     calculateBaseAssetAmountForAmmToFulfill: ${calculateBaseAssetAmountForAmmToFulfill(
					nodeToFill.node.order,
					nodeToFill.node.market as PerpMarketAccount,
					oraclePriceData,
					this.slotSubscriber.getSlot()
				).toString()}`
			);
			return false;
		}

		// if making with vAMM, ensure valid oracle
		if (!nodeToFill.makerNode) {
			const oracleIsValid = isOracleValid(
				(nodeToFill.node.market as PerpMarketAccount).amm,
				oraclePriceData,
				this.clearingHouse.getStateAccount().oracleGuardRails,
				this.slotSubscriber.getSlot()
			);
			if (!oracleIsValid) {
				logger.error(`Oracle is not valid for market ${marketIndex}`);
				return false;
			}
		}

		return true;
	}

	private async getNodeFillInfo(nodeToFill: NodeToFill): Promise<{
		makerInfo: MakerInfo | undefined;
		chUser: User;
		referrerInfo: ReferrerInfo;
		marketType: MarketType;
	}> {
		let makerInfo: MakerInfo | undefined;
		if (nodeToFill.makerNode) {
			const makerUserAccount = (
				await this.userMap.mustGet(nodeToFill.makerNode.userAccount.toString())
			).getUserAccount();
			const makerAuthority = makerUserAccount.authority;
			const makerUserStats = (
				await this.userStatsMap.mustGet(makerAuthority.toString())
			).userStatsAccountPublicKey;
			makerInfo = {
				maker: nodeToFill.makerNode.userAccount,
				makerUserAccount: makerUserAccount,
				order: nodeToFill.makerNode.order,
				makerStats: makerUserStats,
			};
		}

		const chUser = await this.userMap.mustGet(
			nodeToFill.node.userAccount.toString()
		);
		const referrerInfo = (
			await this.userStatsMap.mustGet(
				chUser.getUserAccount().authority.toString()
			)
		).getReferrerInfo();

		return Promise.resolve({
			makerInfo,
			chUser,
			referrerInfo,
			marketType: nodeToFill.node.order.marketType,
		});
	}

	/**
	 * Returns the number of bytes occupied by this array if it were serialized in compact-u16-format.
	 * NOTE: assumes each element of the array is 1 byte (not sure if this holds?)
	 *
	 * https://docs.solana.com/developing/programming-model/transactions#compact-u16-format
	 *
	 * https://stackoverflow.com/a/69951832
	 *  hex     |  compact-u16
	 *  --------+------------
	 *  0x0000  |  [0x00]
	 *  0x0001  |  [0x01]
	 *  0x007f  |  [0x7f]
	 *  0x0080  |  [0x80 0x01]
	 *  0x3fff  |  [0xff 0x7f]
	 *  0x4000  |  [0x80 0x80 0x01]
	 *  0xc000  |  [0x80 0x80 0x03]
	 *  0xffff  |  [0xff 0xff 0x03])
	 */
	private calcCompactU16EncodedSize(array: any[], elemSize = 1): number {
		if (array.length > 0x3fff) {
			return 3 + array.length * elemSize;
		} else if (array.length > 0x7f) {
			return 2 + array.length * elemSize;
		} else {
			return 1 + (array.length * elemSize || 1);
		}
	}

	/**
	 * Instruction are made of 3 parts:
	 * - index of accounts where programId resides (1 byte)
	 * - affected accounts    (compact-u16-format byte array)
	 * - raw instruction data (compact-u16-format byte array)
	 * @param ix The instruction to calculate size for.
	 */
	private calcIxEncodedSize(ix: TransactionInstruction): number {
		return (
			1 +
			this.calcCompactU16EncodedSize(new Array(ix.keys.length), 1) +
			this.calcCompactU16EncodedSize(new Array(ix.data.byteLength), 1)
		);
	}

	private async sleep(ms: number) {
		return new Promise((resolve) => setTimeout(resolve, ms));
	}

	private isLogFillOrder(log: string): boolean {
		return log.includes('Instruction: FillPerpOrder');
	}

	/**
	 * Iterates through a tx's logs and handles it appropriately (e.g. throttling users, updating metrics, etc.)
	 *
	 * @param nodesFilled nodes that we sent a transaction to fill
	 * @param logs logs from tx.meta.logMessages or this.clearingHouse.program._events._eventParser.parseLogs
	 *
	 * @returns number of nodes successfully filled
	 */
	private handleTransactionLogs(
		nodesFilled: Array<NodeToFill>,
		logs: string[]
	): number {
		let nextIsFillRecord = false;
		let ixIdx = -1; // skip ComputeBudgetProgram
		let successCount = 0;
		let burstedCU = false;
		for (const log of logs) {
			if (log === null) {
				logger.error(`log is null`);
				continue;
			}

			if (log.includes('exceeded maximum number of instructions allowed')) {
				// temporary burst CU limit
				logger.warn(`Using bursted CU limit`);
				this.useBurstCULimit = true;
				this.fillTxSinceBurstCU = 0;
				burstedCU = true;
			}

			if (nextIsFillRecord) {
				if (log.includes('Order does not exist')) {
					const filledNode = nodesFilled[ixIdx];
					logger.error(
						`   assoc order: ${filledNode.node.userAccount.toString()}, ${
							filledNode.node.order.orderId
						}`
					);
					logger.error(` ${log}, ixIdx: ${ixIdx}`);
					this.throttledNodes.delete(this.getNodeToFillSignature(filledNode));
					nextIsFillRecord = false;
				} else if (log.includes('data')) {
					// raw event data, this is expected
					successCount++;
					nextIsFillRecord = false;
				} else if (log.includes('Err filling order id')) {
					const match = log.match(
						new RegExp(
							'.*Err filling order id ([0-9]+) for user ([a-zA-Z0-9]+)'
						)
					);
					if (match !== null) {
						const orderId = match[1];
						const userAcc = match[2];
						const extractedSig = this.getFillSignatureFromUserAccountAndOrderId(
							userAcc,
							orderId
						);
						this.throttledNodes.set(extractedSig, Date.now());

						const filledNode = nodesFilled[ixIdx];
						const assocNodeSig = this.getNodeToFillSignature(filledNode);
						logger.warn(
							`Throttling node due to fill error. extractedSig: ${extractedSig}, assocNodeSig: ${assocNodeSig}, assocNodeIdx: ${ixIdx}`
						);
						nextIsFillRecord = false;
					} else {
						logger.error(`Failed to find erroneous fill via regex: ${log}`);
					}
				} else {
					const filledNode = nodesFilled[ixIdx];
					logger.error(`how parse log?: ${log}`);
					logger.error(
						` assoc order: ${filledNode.node.userAccount.toString()}, ${
							filledNode.node.order.orderId
						}`
					);
					nextIsFillRecord = false;
				}

				// nextIsFillRecord = false;
			} else if (this.isLogFillOrder(log)) {
				nextIsFillRecord = true;
				ixIdx++;
			}
		}

		if (!burstedCU) {
			if (this.fillTxSinceBurstCU > TX_COUNT_COOLDOWN_ON_BURST) {
				this.useBurstCULimit = false;
			}
			this.fillTxSinceBurstCU += 1;
		}

		return successCount;
	}

	private async processBulkFillTxLogs(
		nodesFilled: Array<NodeToFill>,
		txSig: TransactionSignature
	) {
		let tx: TransactionResponse | null = null;
		let attempts = 0;
		while (tx === null && attempts < 10) {
			logger.info(`waiting for ${txSig} to be confirmed`);
			tx = await this.clearingHouse.connection.getTransaction(txSig, {
				commitment: 'confirmed',
			});
			attempts++;
			await this.sleep(1000);
		}

		if (tx === null) {
			logger.error(`tx ${txSig} not found`);
			return;
		}

		const successCount = this.handleTransactionLogs(
			nodesFilled,
			tx.meta.logMessages
		);
		this.metrics?.recordFilledOrder(
			this.clearingHouse.provider.wallet.publicKey,
			this.name,
			successCount
		);
	}

	private async tryBulkFillPerpNodes(
		nodesToFill: Array<NodeToFill>
	): Promise<[TransactionSignature, number]> {
		const tx = new Transaction();
		let txSig = '';
		let lastIdxFilled = 0;

		/**
		 * At all times, the running Tx size is:
		 * - signatures (compact-u16 array, 64 bytes per elem)
		 * - message header (3 bytes)
		 * - affected accounts (compact-u16 array, 32 bytes per elem)
		 * - previous block hash (32 bytes)
		 * - message instructions (
		 * 		- progamIdIdx (1 byte)
		 * 		- accountsIdx (compact-u16, 1 byte per elem)
		 *		- instruction data (compact-u16, 1 byte per elem)
		 */
		let runningTxSize = 0;
		let runningCUUsed = 0;

		const uniqueAccounts = new Set<string>();
		uniqueAccounts.add(this.clearingHouse.provider.wallet.publicKey.toString()); // fee payer goes first

		// first ix is compute budget
		const computeBudgetIx = ComputeBudgetProgram.requestUnits({
			units: 10_000_000,
			additionalFee: 0,
		});
		computeBudgetIx.keys.forEach((key) =>
			uniqueAccounts.add(key.pubkey.toString())
		);
		uniqueAccounts.add(computeBudgetIx.programId.toString());
		tx.add(computeBudgetIx);

		// initialize the barebones transaction
		// signatures
		runningTxSize += this.calcCompactU16EncodedSize(new Array(1), 64);
		// message header
		runningTxSize += 3;
		// accounts
		runningTxSize += this.calcCompactU16EncodedSize(
			new Array(uniqueAccounts.size),
			32
		);
		// block hash
		runningTxSize += 32;
		runningTxSize += this.calcIxEncodedSize(computeBudgetIx);

		const txPackerStart = Date.now();
		const nodesSent: Array<NodeToFill> = [];
		let idxUsed = 0;
		for (const [idx, nodeToFill] of nodesToFill.entries()) {
			logger.info(
				`filling perp node ${idx}, marketIdx: ${
					nodeToFill.node.order.marketIndex
				}: ${nodeToFill.node.userAccount.toString()}, ${
					nodeToFill.node.order.orderId
				}, orderType: ${getVariant(nodeToFill.node.order.orderType)}`
			);

			const { makerInfo, chUser, referrerInfo, marketType } =
				await this.getNodeFillInfo(nodeToFill);

			if (!isVariant(marketType, 'perp')) {
				throw new Error('expected perp market type');
			}

			const ix = await this.clearingHouse.getFillPerpOrderIx(
				chUser.getUserAccountPublicKey(),
				chUser.getUserAccount(),
				nodeToFill.node.order,
				makerInfo,
				referrerInfo
			);

			if (!ix) {
				logger.error(`failed to generate an ix`);
				break;
			}

			this.throttledNodes.set(
				this.getNodeToFillSignature(nodeToFill),
				Date.now()
			);

			// first estimate new tx size with this additional ix and new accounts
			const ixKeys = ix.keys.map((key) => key.pubkey);
			const newAccounts = ixKeys
				.concat(ix.programId)
				.filter((key) => !uniqueAccounts.has(key.toString()));
			const newIxCost = this.calcIxEncodedSize(ix);
			const additionalAccountsCost =
				newAccounts.length > 0
					? this.calcCompactU16EncodedSize(newAccounts, 32) - 1
					: 0;

			// We have to use MAX_TX_PACK_SIZE because it appears we cannot send tx with a size of exactly 1232 bytes.
			// Also, some logs may get truncated near the end of the tx, so we need to leave some room for that.
			const cuToUsePerFill = this.useBurstCULimit
				? BURST_CU_PER_FILL
				: CU_PER_FILL;
			if (
				runningTxSize + newIxCost + additionalAccountsCost >=
					MAX_TX_PACK_SIZE ||
				runningCUUsed + cuToUsePerFill >= MAX_CU_PER_TX
			) {
				logger.info(
					`Fully packed fill tx: est. tx size ${
						runningTxSize + newIxCost + additionalAccountsCost
					}, max: ${MAX_TX_PACK_SIZE}, est. CU used: expected ${
						runningCUUsed + cuToUsePerFill
					}, max: ${MAX_CU_PER_TX}`
				);
				break;
			}

			// add to tx
			logger.info(
				`including tx ${chUser
					.getUserAccountPublicKey()
					.toString()}-${nodeToFill.node.order.orderId.toString()}`
			);
			tx.add(ix);
			runningTxSize += newIxCost + additionalAccountsCost;
			runningCUUsed += cuToUsePerFill;
			newAccounts.forEach((key) => uniqueAccounts.add(key.toString()));
			idxUsed++;
			nodesSent.push(nodeToFill);
			lastIdxFilled = idx;

			this.metrics?.recordFillableOrdersSeen(
				nodeToFill.node.order.marketIndex,
				marketType,
				1
			);
		}
		logger.debug(`txPacker took ${Date.now() - txPackerStart}ms`);

		if (nodesSent.length === 0) {
			return [txSig, -1];
		}

		logger.info(
			`sending tx, ${
				uniqueAccounts.size
			} unique accounts, total ix: ${idxUsed}, calcd tx size: ${runningTxSize}, took ${
				Date.now() - txPackerStart
			}ms`
		);

		const txStart = Date.now();
		this.clearingHouse.txSender
			.send(tx, [], this.clearingHouse.opts)
			.then((resp: TxSigAndSlot) => {
				txSig = resp.txSig;
				const duration = Date.now() - txStart;
				logger.info(`sent tx: ${txSig}, took: ${duration}ms`);
				this.metrics?.recordRpcDuration(
					this.clearingHouse.connection.rpcEndpoint,
					'send',
					duration,
					false,
					this.name
				);

				const parseLogsStart = Date.now();
				this.processBulkFillTxLogs(nodesSent, txSig)
					.then(() => {
						const processBulkFillLogsDuration = Date.now() - parseLogsStart;
						logger.info(`parse logs took ${processBulkFillLogsDuration}ms`);
						this.metrics?.recordRpcDuration(
							this.clearingHouse.connection.rpcEndpoint,
							'processLogs',
							processBulkFillLogsDuration,
							false,
							this.name
						);

						this.metrics?.recordFilledOrder(
							this.clearingHouse.provider.wallet.publicKey,
							this.name,
							nodesSent.length
						);
					})
					.catch((e) => {
						console.error(e);
						logger.error(`Failed to process fill tx logs (error above):`);
					});
			})
			.catch((e) => {
				console.error(e);
				logger.error(`Failed to send packed tx (error above):`);
				const simError = e as SendTransactionError;
				if (simError.logs) {
					const start = Date.now();
					this.handleTransactionLogs(nodesSent, simError.logs);
					logger.error(
						`Failed to send tx, sim error tx logs took: ${Date.now() - start}ms`
					);
				}
			});

		return [txSig, lastIdxFilled];
	}

	private async tryFill() {
		const startTime = Date.now();
		let ran = false;
		try {
			await tryAcquire(this.periodicTaskMutex).runExclusive(async () => {
				await this.dlobMutex.runExclusive(async () => {
					if (this.dlob) {
						this.dlob.clear();
						delete this.dlob;
					}
					this.dlob = new DLOB(
						this.clearingHouse.getPerpMarketAccounts(),
						this.clearingHouse.getSpotMarketAccounts(), // TODO: new sdk - remove this
						this.clearingHouse.getStateAccount(),
						this.userMap,
						true
					);
					await this.dlob.init();
				});

				// 1) get all fillable nodes
				let fillableNodes: Array<NodeToFill> = [];
				for (const market of this.clearingHouse.getPerpMarketAccounts()) {
					fillableNodes = fillableNodes.concat(
						await this.getPerpFillableNodesForMarket(market)
					);
					logger.debug(
						`got ${fillableNodes.length} fillable nodes on market ${market.marketIndex}`
					);
				}

				// filter out nodes that we know cannot be filled
				const seenNodes = new Set<string>();
				const filteredNodes = fillableNodes.filter((node) => {
					const sig = this.getNodeToFillSignature(node);
					if (seenNodes.has(sig)) {
						return false;
					}
					seenNodes.add(sig);
					return this.filterFillableNodes(node);
				});
				logger.debug(
					`filtered ${fillableNodes.length} to ${filteredNodes.length}`
				);

				// fill the perp nodes
				let filledNodeCount = 0;
				while (filledNodeCount < filteredNodes.length) {
					const resp = await this.tryBulkFillPerpNodes(
						filteredNodes.slice(filledNodeCount)
					);
					filledNodeCount += resp[1] + 1;
				}

				ran = true;
			});
		} catch (e) {
			if (e === E_ALREADY_LOCKED) {
				this.metrics?.recordMutexBusy(this.name);
			} else if (e === dlobMutexError) {
				logger.error(`${this.name} dlobMutexError timeout`);
			} else {
				throw e;
			}
		} finally {
			if (ran) {
				const duration = Date.now() - startTime;

				// need another histogram size for tryFill
				// this.metrics?.recordRpcDuration(
				// 	this.clearingHouse.connection.rpcEndpoint,
				// 	'tryFill',
				// 	duration,
				// 	false,
				// 	this.name
				// );
				logger.info(`tryFill done, took ${duration}ms`);

				await this.watchdogTimerMutex.runExclusive(async () => {
					this.watchdogTimerLastPatTime = Date.now();
				});
			}
		}
	}
}

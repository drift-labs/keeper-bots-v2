import {
	DriftEnv,
	NewUserRecord,
	OrderRecord,
	ClearingHouseUser,
	ReferrerInfo,
	isOracleValid,
	ClearingHouse,
	PerpMarketAccount,
	SlotSubscriber,
	calculateAskPrice,
	calculateBidPrice,
	MakerInfo,
	isFillableByVAMM,
	isVariant,
	DLOB,
	NodeToFill,
	UserMap,
	UserStatsMap,
	MarketType,
} from '@drift-labs/sdk';
import { promiseTimeout } from '@drift-labs/sdk/lib/util/promiseTimeout';
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
const FILL_ORDER_BACKOFF = 0; //5000;
const dlobMutexError = new Error('dlobMutex timeout');

export class FillerBot implements Bot {
	public readonly name: string;
	public readonly dryRun: boolean;
	public readonly defaultIntervalMs: number = 3000;

	private driftEnv: DriftEnv;
	private clearingHouse: ClearingHouse;
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

	constructor(
		name: string,
		dryRun: boolean,
		clearingHouse: ClearingHouse,
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
		this.metrics?.trackObjectSize('filler-userMap', this.userMap);
		initPromises.push(this.userMap.fetchAllUsers());

		this.userStatsMap = new UserStatsMap(
			this.clearingHouse,
			this.clearingHouse.userAccountSubscriptionConfig
		);
		this.metrics?.trackObjectSize('filler-userStatsMap', this.userStatsMap);
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
				this.watchdogTimerLastPatTime > Date.now() - 2 * this.defaultIntervalMs;
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
			this.clearingHouse.getOracleDataForMarket(marketIndex);
		const oracleIsValid = isOracleValid(
			market.amm,
			oraclePriceData,
			this.clearingHouse.getStateAccount().oracleGuardRails,
			this.slotSubscriber.getSlot()
		);
		if (!oracleIsValid) {
			console.error(`Oracle is not valid for market ${marketIndex}`);
			return [];
		}

		const vAsk = calculateAskPrice(market, oraclePriceData);
		const vBid = calculateBidPrice(market, oraclePriceData);

		let nodes: Array<NodeToFill> = [];
		await this.dlobMutex.runExclusive(async () => {
			nodes = this.dlob.findNodesToFill(
				marketIndex,
				vBid,
				vAsk,
				this.slotSubscriber.getSlot(),
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
		return `${node.node.userAccount.toString()}-${node.node.order.orderId.toString()}`;
	}

	private filterFillableNodes(nodeToFill: NodeToFill): boolean {
		if (nodeToFill.node.isVammNode()) {
			return false;
		}

		if (nodeToFill.node.haveFilled) {
			return false;
		}

		if (this.throttledNodes.has(this.getNodeToFillSignature(nodeToFill))) {
			const lastFillAttempt = this.throttledNodes.get(
				this.getNodeToFillSignature(nodeToFill)
			);
			if (lastFillAttempt + FILL_ORDER_BACKOFF > Date.now()) {
				return false;
			} else {
				this.throttledNodes.delete(this.getNodeToFillSignature(nodeToFill));
			}
		}

		const marketIndex = nodeToFill.node.market.marketIndex;
		const oraclePriceData =
			this.clearingHouse.getOracleDataForMarket(marketIndex);

		if (
			!nodeToFill.makerNode &&
			isVariant(nodeToFill.node.order.marketType, 'perp') &&
			!isFillableByVAMM(
				nodeToFill.node.order,
				nodeToFill.node.market as PerpMarketAccount,
				oraclePriceData,
				this.slotSubscriber.getSlot()
			)
		) {
			return false;
		}

		return true;
	}

	private async getNodeFillInfo(nodeToFill: NodeToFill): Promise<{
		makerInfo: MakerInfo | undefined;
		chUser: ClearingHouseUser;
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
		return log === 'Program log: Instruction: FillOrder';
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

		let nextIsFillRecord = false;
		let ixIdx = -1; // skip ComputeBudgetProgram
		let successCount = 0;
		for (const log of tx.meta.logMessages) {
			if (log === null) {
				logger.error(`null log message on tx: ${txSig}`);
				continue;
			}

			if (nextIsFillRecord) {
				if (log.includes('Order does not exist')) {
					const filledNode = nodesFilled[ixIdx];
					logger.error(` ${log}, ix: ${ixIdx}`);
					logger.error(
						`   assoc order: ${filledNode.node.userAccount.toString()}, ${
							filledNode.node.order.orderId
						}`
					);
				} else if (log.includes('Amm cant fulfill order')) {
					const filledNode = nodesFilled[ixIdx];
					logger.error(` ${log}, ix: ${ixIdx}`);
					logger.error(
						`  assoc order: ${filledNode.node.userAccount.toString()}, ${
							filledNode.node.order.orderId
						}`
					);
					this.throttledNodes.set(
						this.getNodeToFillSignature(filledNode),
						Date.now()
					);
				} else if (log.length > 50) {
					// probably rawe event data...?
					successCount++;
				} else {
					logger.info(` how parse log?: ${log}`);
				}

				nextIsFillRecord = false;
			} else if (this.isLogFillOrder(log)) {
				nextIsFillRecord = true;
				ixIdx++;
			}
		}

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
				`filling perp node ${idx}: ${nodeToFill.node.userAccount.toString()}, ${
					nodeToFill.node.order.orderId
				}`
			);

			const { makerInfo, chUser, referrerInfo, marketType } =
				await this.getNodeFillInfo(nodeToFill);

			if (!isVariant(marketType, 'perp')) {
				throw new Error('expected perp market type');
			}

			const ix = await this.clearingHouse.getFillOrderIx(
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
			if (
				runningTxSize + newIxCost + additionalAccountsCost >=
				MAX_TX_PACK_SIZE
			) {
				logger.error(
					`too much sizee: expected ${
						runningTxSize + newIxCost + additionalAccountsCost
					}, max: ${MAX_TX_PACK_SIZE}`
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
		try {
			const resp = await this.clearingHouse.txSender.send(
				tx,
				[],
				this.clearingHouse.opts
			);
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
			await this.processBulkFillTxLogs(nodesSent, txSig);
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
		} catch (e) {
			logger.error(`failed to send packed tx:`);
			console.error(e);
			const simError = e as SendTransactionError;
			for (const log of simError.logs) {
				logger.error(`${log}`);
			}
		}

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
						true
					);
					this.metrics?.trackObjectSize('filler-dlob', this.dlob);
					await this.dlob.init(this.clearingHouse, this.userMap);
				});

				// 1) get all fillable nodes
				let fillableNodes: Array<NodeToFill> = [];
				for (const market of this.clearingHouse.getPerpMarketAccounts()) {
					fillableNodes = fillableNodes.concat(
						await this.getPerpFillableNodesForMarket(market)
					);
				}

				// TODO TODO TODO: can remove this if no dupes?
				const seenNodes = new Set<string>();
				const filteredNodes = fillableNodes.filter((node) => {
					const sig = this.getNodeToFillSignature(node);
					if (seenNodes.has(sig)) {
						return false;
					}
					seenNodes.add(sig);
					return this.filterFillableNodes(node);
				});

				// fill the perp nodes
				let filledNodeCount = 0;
				while (filledNodeCount < filteredNodes.length) {
					const resp = await promiseTimeout(
						this.tryBulkFillPerpNodes(filteredNodes.slice(filledNodeCount)),
						30000
					);

					if (resp === null) {
						logger.error(`Timeout tryFill, took ${Date.now() - startTime}ms`);
					} else {
						filledNodeCount += resp[1] + 1;
					}
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
				this.metrics?.recordRpcDuration(
					this.clearingHouse.connection.rpcEndpoint,
					'tryFill',
					duration,
					false,
					this.name
				);
				logger.debug(`tryFill done, took ${duration}ms`);

				await this.watchdogTimerMutex.runExclusive(async () => {
					this.watchdogTimerLastPatTime = Date.now();
				});
			}
		}
	}
}

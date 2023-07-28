import {
	ReferrerInfo,
	DriftClient,
	MakerInfo,
	isVariant,
	NodeToFill,
	UserStatsMap,
	MarketType,
	getVariant,
	PRICE_PRECISION,
	convertToNumber,
	BASE_PRECISION,
	QUOTE_PRECISION,
	BulkAccountLoader,
	SlotSubscriber,
	PublicKey,
	EventSubscriber,
	NodeToTrigger,
	OrderSubscriber,
	UserAccount,
	getUserAccountPublicKey,
	OrderActionRecord,
	WrappedEvent,
	decodeName,
} from '@drift-labs/sdk';
import { TxSigAndSlot } from '@drift-labs/sdk/lib/tx/types';
import { tryAcquire, E_ALREADY_LOCKED } from 'async-mutex';

import {
	SendTransactionError,
	Transaction,
	TransactionInstruction,
	ComputeBudgetProgram,
	Keypair,
	VersionedTransaction,
} from '@solana/web3.js';

import { SearcherClient } from 'jito-ts/dist/sdk/block-engine/searcher';
import { Bundle } from 'jito-ts/dist/sdk/block-engine/types';

import { PrometheusExporter } from '@opentelemetry/exporter-prometheus';
import {
	ExplicitBucketHistogramAggregation,
	InstrumentType,
	MeterProvider,
	View,
} from '@opentelemetry/sdk-metrics-base';
import { BatchObservableResult } from '@opentelemetry/api-metrics';

import { logger } from '../logger';
import { FillerConfig } from '../config';
import { RuntimeSpec, metricAttrFromUserAccount } from '../metrics';
import { webhookMessage } from '../webhook';
import {
	isEndIxLog,
	isErrFillingLog,
	isErrStaleOracle,
	isFillIxLog,
	isIxLog,
	isMakerBreachedMaintenanceMarginLog,
	isOrderDoesNotExistLog,
	isTakerBreachedMaintenanceMarginLog,
} from './common/txLogParse';
import { getErrorCode } from '../error';
import {
	getFillSignatureFromUserAccountAndOrderId,
	getNodeToFillSignature,
	getNodeToTriggerSignature,
} from '../utils';
import { FillerBot } from './filler';

const MAX_TX_PACK_SIZE = 1230; //1232;
const CU_PER_FILL = 260_000; // CU cost for a successful fill
const BURST_CU_PER_FILL = 350_000; // CU cost for a successful fill
const MAX_CU_PER_TX = 1_400_000; // seems like this is all budget program gives us...on devnet
const TX_COUNT_COOLDOWN_ON_BURST = 10; // send this many tx before resetting burst mode

const errorCodesToSuppress = [
	6081, // 0x17c1 Error Number: 6081. Error Message: MarketWrongMutability.
	6078, // 0x17BE Error Number: 6078. Error Message: PerpMarketNotFound
	6087, // 0x17c7 Error Number: 6087. Error Message: SpotMarketNotFound.
	6239, // 0x185F Error Number: 6239. Error Message: RevertFill.
	6003, // 0x1773 Error Number: 6003. Error Message: Insufficient collateral.

	6111, // Error Message: OrderNotTriggerable.
	6112, // Error Message: OrderDidNotSatisfyTriggerCondition.
];

enum METRIC_TYPES {
	sdk_call_duration_histogram = 'sdk_call_duration_histogram',
	try_fill_duration_histogram = 'try_fill_duration_histogram',
	runtime_specs = 'runtime_specs',
	total_collateral = 'total_collateral',
	last_try_fill_time = 'last_try_fill_time',
	unrealized_pnl = 'unrealized_pnl',
	mutex_busy = 'mutex_busy',
	attempted_fills = 'attempted_fills',
	attempted_triggers = 'attempted_triggers',
	successful_fills = 'successful_fills',
	observed_fills_count = 'observed_fills_count',
	tx_sim_error_count = 'tx_sim_error_count',
	user_map_user_account_keys = 'user_map_user_account_keys',
	user_stats_map_authority_keys = 'user_stats_map_authority_keys',
}

function logMessageForNodeToFill(node: NodeToFill, prefix?: string): string {
	const takerNode = node.node;
	const takerOrder = takerNode.order;
	let msg = '';
	if (prefix) {
		msg += `${prefix}\n`;
	}
	msg += `taker on market ${
		takerOrder.marketIndex
	}: ${takerNode.userAccount.toBase58()}-${takerOrder.orderId} ${getVariant(
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
	if (node.makerNodes.length > 0) {
		for (let i = 0; i < node.makerNodes.length; i++) {
			const makerNode = node.makerNodes[i];
			const makerOrder = makerNode.order;
			msg += `  [${i}] market ${
				makerOrder.marketIndex
			}: ${makerNode.userAccount.toBase58()}-${makerOrder.orderId} ${getVariant(
				makerOrder.direction
			)} ${convertToNumber(
				makerOrder.baseAssetAmountFilled,
				BASE_PRECISION
			)}/${convertToNumber(
				makerOrder.baseAssetAmount,
				BASE_PRECISION
			)} @ ${convertToNumber(
				makerOrder.price,
				PRICE_PRECISION
			)} (orderType: ${getVariant(makerOrder.orderType)})\n`;
		}
	} else {
		msg += `  vAMM`;
	}
	return msg;
}

export class FillerLiteBot extends FillerBot {
	private orderSubscriber: OrderSubscriber;

	constructor(
		slotSubscriber: SlotSubscriber,
		bulkAccountLoader: BulkAccountLoader | undefined,
		driftClient: DriftClient,
		eventSubscriber: EventSubscriber,
		runtimeSpec: RuntimeSpec,
		config: FillerConfig,
		jitoSearcherClient?: SearcherClient,
		jitoAuthKeypair?: Keypair,
		tipPayerKeypair?: Keypair
	) {
		super(
			slotSubscriber,
			bulkAccountLoader,
			driftClient,
			eventSubscriber,
			runtimeSpec,
			config,
			jitoSearcherClient,
			jitoAuthKeypair,
			tipPayerKeypair
		);

		this.userStatsMapSubscriptionConfig = {
			type: 'polling',
			accountLoader: new BulkAccountLoader(
				this.driftClient.connection,
				'processed', // No polling so value is irrelevant
				0 // no polling, just for using mustGet
			),
		};

		this.orderSubscriber = new OrderSubscriber({
			driftClient: this.driftClient,
			subscriptionConfig: { type: 'websocket', skipInitialLoad: true },
		});
	}

	initializeMetrics() {
		if (this.metricsInitialized) {
			logger.error('Tried to initilaize metrics multiple times');
			return;
		}
		this.metricsInitialized = true;

		const { endpoint: defaultEndpoint } = PrometheusExporter.DEFAULT_OPTIONS;
		this.exporter = new PrometheusExporter(
			{
				port: this.metricsPort,
				endpoint: defaultEndpoint,
			},
			() => {
				logger.info(
					`prometheus scrape endpoint started: http://localhost:${this.metricsPort}${defaultEndpoint}`
				);
			}
		);
		const meterName = this.name;
		const meterProvider = new MeterProvider({
			views: [
				new View({
					instrumentName: METRIC_TYPES.sdk_call_duration_histogram,
					instrumentType: InstrumentType.HISTOGRAM,
					meterName: meterName,
					aggregation: new ExplicitBucketHistogramAggregation(
						Array.from(new Array(20), (_, i) => 0 + i * 100),
						true
					),
				}),
				new View({
					instrumentName: METRIC_TYPES.try_fill_duration_histogram,
					instrumentType: InstrumentType.HISTOGRAM,
					meterName: meterName,
					aggregation: new ExplicitBucketHistogramAggregation(
						Array.from(new Array(20), (_, i) => 0 + i * 5),
						true
					),
				}),
			],
		});

		meterProvider.addMetricReader(this.exporter);
		this.meter = meterProvider.getMeter(meterName);

		this.bootTimeMs = Date.now();

		this.runtimeSpecsGauge = this.meter.createObservableGauge(
			METRIC_TYPES.runtime_specs,
			{
				description: 'Runtime sepcification of this program',
			}
		);
		this.runtimeSpecsGauge.addCallback((obs) => {
			obs.observe(this.bootTimeMs, this.runtimeSpec);
		});
		this.totalCollateralGauge = this.meter.createObservableGauge(
			METRIC_TYPES.total_collateral,
			{
				description: 'Total collateral of the account',
			}
		);
		this.lastTryFillTimeGauge = this.meter.createObservableGauge(
			METRIC_TYPES.last_try_fill_time,
			{
				description: 'Last time that fill was attempted',
			}
		);
		this.unrealizedPnLGauge = this.meter.createObservableGauge(
			METRIC_TYPES.unrealized_pnl,
			{
				description: 'The account unrealized PnL',
			}
		);

		this.mutexBusyCounter = this.meter.createCounter(METRIC_TYPES.mutex_busy, {
			description: 'Count of times the mutex was busy',
		});
		this.successfulFillsCounter = this.meter.createCounter(
			METRIC_TYPES.successful_fills,
			{
				description: 'Count of fills that we successfully landed',
			}
		);
		this.attemptedFillsCounter = this.meter.createCounter(
			METRIC_TYPES.attempted_fills,
			{
				description: 'Count of fills we attempted',
			}
		);
		this.attemptedTriggersCounter = this.meter.createCounter(
			METRIC_TYPES.attempted_triggers,
			{
				description: 'Count of triggers s s s s s s s s we attempted',
			}
		);
		this.observedFillsCountCounter = this.meter.createCounter(
			METRIC_TYPES.observed_fills_count,
			{
				description: 'Count of fills observed in the market',
			}
		);
		this.txSimErrorCounter = this.meter.createCounter(
			METRIC_TYPES.tx_sim_error_count,
			{
				description: 'Count of errors from simulating transactions',
			}
		);
		this.userStatsMapAuthorityKeysGauge = this.meter.createObservableGauge(
			METRIC_TYPES.user_stats_map_authority_keys,
			{
				description: 'number of authority keys in UserStatsMap',
			}
		);
		this.userStatsMapAuthorityKeysGauge.addCallback(async (obs) => {
			obs.observe(this.userStatsMap.size());
		});

		this.sdkCallDurationHistogram = this.meter.createHistogram(
			METRIC_TYPES.sdk_call_duration_histogram,
			{
				description: 'Distribution of sdk method calls',
				unit: 'ms',
			}
		);
		this.tryFillDurationHistogram = this.meter.createHistogram(
			METRIC_TYPES.try_fill_duration_histogram,
			{
				description: 'Distribution of tryFills',
				unit: 'ms',
			}
		);

		this.lastTryFillTimeGauge.addCallback(async (obs) => {
			await this.watchdogTimerMutex.runExclusive(async () => {
				const user = this.driftClient.getUser();
				obs.observe(
					this.watchdogTimerLastPatTime,
					metricAttrFromUserAccount(
						user.userAccountPublicKey,
						user.getUserAccount()
					)
				);
			});
		});

		this.meter.addBatchObservableCallback(
			async (batchObservableResult: BatchObservableResult) => {
				for (const user of this.driftClient.getUsers()) {
					const userAccount = user.getUserAccount();

					batchObservableResult.observe(
						this.totalCollateralGauge,
						convertToNumber(user.getTotalCollateral(), QUOTE_PRECISION),
						metricAttrFromUserAccount(user.userAccountPublicKey, userAccount)
					);

					batchObservableResult.observe(
						this.unrealizedPnLGauge,
						convertToNumber(user.getUnrealizedPNL(), QUOTE_PRECISION),
						metricAttrFromUserAccount(user.userAccountPublicKey, userAccount)
					);
				}
			},
			[this.totalCollateralGauge, this.unrealizedPnLGauge]
		);
	}

	public async init() {
		logger.info(`${this.name} initing`);

		// Initializing so we can use mustGet for RPC fall back, but don't subscribe
		// so we don't call getProgramAccounts
		this.userStatsMap = new UserStatsMap(
			this.driftClient,
			this.userStatsMapSubscriptionConfig
		);

		await this.orderSubscriber.subscribe();
		await this.sleep(1200); // Wait a few slots to build up order book

		this.lookupTableAccount =
			await this.driftClient.fetchMarketLookupTableAccount();

		await webhookMessage(`[${this.name}]: started`);
	}

	public async startIntervalLoop(_intervalMs: number) {
		const intervalId = setInterval(
			this.tryFill.bind(this),
			this.pollingIntervalMs
		);
		this.intervalIds.push(intervalId);

		this.eventSubscriber.eventEmitter.on(
			'newEvent',
			async (record: WrappedEvent<any>) => {
				if (record.eventType === 'OrderActionRecord') {
					const actionRecord = record as OrderActionRecord;

					if (isVariant(actionRecord.action, 'fill')) {
						if (isVariant(actionRecord.marketType, 'perp')) {
							const perpMarket = this.driftClient.getPerpMarketAccount(
								actionRecord.marketIndex
							);
							if (perpMarket) {
								this.observedFillsCountCounter.add(1, {
									market: decodeName(perpMarket.name),
								});
							}
						}
					}
				}
			}
		);

		logger.info(`${this.name} Bot started! (websocket: true)`);
	}

	public async reset() {
		for (const intervalId of this.intervalIds) {
			clearInterval(intervalId);
		}
		this.intervalIds = [];

		this.eventSubscriber.eventEmitter.removeAllListeners('newEvent');

		await this.orderSubscriber.unsubscribe();
		await this.userStatsMap.unsubscribe();
	}

	protected async getNodeToFillInfo(nodeToFill: NodeToFill): Promise<{
		makerInfos: Array<MakerInfo> | undefined;
		takerUser: UserAccount;
		referrerInfo: ReferrerInfo;
		marketType: MarketType;
	}> {
		const makerInfos: Array<MakerInfo> | undefined = [];

		// set to track whether maker account has already been included
		const makersIncluded = new Set<string>();
		if (nodeToFill.makerNodes.length > 0) {
			for (const makerNode of nodeToFill.makerNodes) {
				if (this.isDLOBNodeThrottled(makerNode)) {
					continue;
				}

				const makerAccount = makerNode.userAccount.toBase58();
				if (makersIncluded.has(makerAccount)) {
					continue;
				}

				// Is must get still necessary here? NodeToFill is coming from orderSubscriber,
				// which also caches the userAccounts in a Map. So get SHOULD be sufficient.
				const makerUserAccount =
					this.orderSubscriber.usersAccounts.get(makerAccount).userAccount;
				const makerAuthority = makerUserAccount.authority;

				const makerUserStats = (
					await this.userStatsMap.mustGet(makerAuthority.toString())
				).userStatsAccountPublicKey;
				makerInfos.push({
					maker: makerNode.userAccount,
					makerUserAccount: makerUserAccount,
					order: makerNode.order,
					makerStats: makerUserStats,
				});
				makersIncluded.add(makerAccount);
			}
		}

		const takerUser = this.orderSubscriber.usersAccounts.get(
			nodeToFill.node.userAccount.toString()
		);

		const referrerInfo = (
			await this.userStatsMap.mustGet(
				takerUser.userAccount.authority.toString()
			)
		).getReferrerInfo();

		return Promise.resolve({
			makerInfos,
			takerUser: takerUser.userAccount,
			referrerInfo,
			marketType: nodeToFill.node.order.marketType,
		});
	}

	/**
	 * Iterates through a tx's logs and handles it appropriately 3e.g. throttling users, updating metrics, etc.)
	 *
	 * @param nodesFilled nodes that we sent a transaction to fill
	 * @param logs logs from tx.meta.logMessages or this.clearingHouse.program._events._eventParser.parseLogs
	 *
	 * @returns number of nodes successfully filled
	 */
	protected async handleTransactionLogs(
		nodesFilled: Array<NodeToFill>,
		logs: string[]
	): Promise<number> {
		let inFillIx = false;
		let errorThisFillIx = false;
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
				continue;
			}

			if (isEndIxLog(this.driftClient.program.programId.toBase58(), log)) {
				if (!errorThisFillIx) {
					successCount++;
				}

				inFillIx = false;
				errorThisFillIx = false;
				continue;
			}

			if (isIxLog(log)) {
				if (isFillIxLog(log)) {
					inFillIx = true;
					errorThisFillIx = false;
					ixIdx++;

					// can also print this from parsing the log record in upcoming
					const nodeFilled = nodesFilled[ixIdx];
					logger.info(
						logMessageForNodeToFill(
							nodeFilled,
							`Processing tx log for assoc node ${ixIdx}`
						)
					);
				} else {
					inFillIx = false;
				}
				continue;
			}

			if (!inFillIx) {
				// this is not a log for a fill instruction
				continue;
			}

			// try to handle the log line
			const orderIdDoesNotExist = isOrderDoesNotExistLog(log);
			if (orderIdDoesNotExist) {
				const filledNode = nodesFilled[ixIdx];
				logger.error(
					`assoc node (ixIdx: ${ixIdx}): ${filledNode.node.userAccount.toString()}, ${
						filledNode.node.order.orderId
					}; does not exist (filled by someone else); ${log}`
				);
				this.clearThrottledNode(getNodeToFillSignature(filledNode));
				errorThisFillIx = true;
				continue;
			}

			const makerBreachedMaintenanceMargin =
				isMakerBreachedMaintenanceMarginLog(log);
			if (makerBreachedMaintenanceMargin !== null) {
				logger.error(
					`Throttling maker breached maintenance margin: ${makerBreachedMaintenanceMargin}`
				);
				this.throttledNodes.set(makerBreachedMaintenanceMargin, Date.now());
				const tx = new Transaction();
				tx.add(
					ComputeBudgetProgram.requestUnits({
						units: 1_000_000,
						additionalFee: 0,
					})
				);
				tx.add(
					await this.driftClient.getForceCancelOrdersIx(
						new PublicKey(makerBreachedMaintenanceMargin),
						this.orderSubscriber.usersAccounts.get(
							makerBreachedMaintenanceMargin
						).userAccount
					)
				);
				this.driftClient.txSender
					.send(tx, [], this.driftClient.opts)
					.then((txSig) => {
						logger.info(
							`Force cancelled orders for makers due to breach of maintenance margin. Tx: ${txSig}`
						);
					})
					.catch((e) => {
						// console.error(e);
						logger.error(`Failed to send ForceCancelOrder Ixs (error above):`);
						webhookMessage(
							`[${this.name}]: :x: error processing fill tx logs:\n${
								e.stack ? e.stack : e.message
							}`
						);
					});

				errorThisFillIx = true;
				continue;
			}

			const takerBreachedMaintenanceMargin =
				isTakerBreachedMaintenanceMarginLog(log);
			if (takerBreachedMaintenanceMargin) {
				const filledNode = nodesFilled[ixIdx];
				const takerNodeSignature = filledNode.node.userAccount.toBase58();
				logger.error(
					`taker breach maint. margin, assoc node (ixIdx: ${ixIdx}): ${filledNode.node.userAccount.toString()}, ${
						filledNode.node.order.orderId
					}; (throttling ${takerNodeSignature} and force cancelling orders); ${log}`
				);
				this.throttledNodes.set(takerNodeSignature, Date.now());
				errorThisFillIx = true;

				const tx = new Transaction();
				tx.add(
					ComputeBudgetProgram.requestUnits({
						units: 1_000_000,
						additionalFee: 0,
					})
				);
				tx.add(
					await this.driftClient.getForceCancelOrdersIx(
						filledNode.node.userAccount,
						this.orderSubscriber.usersAccounts.get(
							filledNode.node.userAccount.toString()
						).userAccount
					)
				);

				this.driftClient.txSender
					.send(tx, [], this.driftClient.opts)
					.then((txSig) => {
						logger.info(
							`Force cancelled orders for user ${filledNode.node.userAccount.toBase58()} due to breach of maintenance margin. Tx: ${txSig}`
						);
					})
					.catch((e) => {
						console.error(e);
						logger.error(`Failed to send ForceCancelOrder Ixs (error above):`);
						webhookMessage(
							`[${this.name}]: :x: error processing fill tx logs:\n${
								e.stack ? e.stack : e.message
							}`
						);
					});

				continue;
			}

			const errFillingLog = isErrFillingLog(log);
			if (errFillingLog) {
				const orderId = errFillingLog[0];
				const userAcc = errFillingLog[1];
				const extractedSig = getFillSignatureFromUserAccountAndOrderId(
					userAcc,
					orderId
				);
				this.throttledNodes.set(extractedSig, Date.now());

				const filledNode = nodesFilled[ixIdx];
				const assocNodeSig = getNodeToFillSignature(filledNode);
				logger.warn(
					`Throttling node due to fill error. extractedSig: ${extractedSig}, assocNodeSig: ${assocNodeSig}, assocNodeIdx: ${ixIdx}`
				);
				errorThisFillIx = true;
				continue;
			}

			if (isErrStaleOracle(log)) {
				logger.error(`Stale oracle error: ${log}`);
				errorThisFillIx = true;
				continue;
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

	protected async sendFillTx(
		fillTxId: number,
		nodesSent: Array<NodeToFill>,
		ixs: Array<TransactionInstruction>
	) {
		let txResp: Promise<TxSigAndSlot>;
		const txStart = Date.now();
		if (this.jitoSearcherClient && this.jitoAuthKeypair) {
			if (!isNaN(this.transactionVersion)) {
				logger.warn(
					`${this.name} should use unversioned tx for jito until https://github.com/jito-labs/jito-ts/pull/7`
				);
				return ['', 0];
			}
			const slotsUntilNextLeader =
				this.jitoLeaderNextSlot - this.slotSubscriber.getSlot();
			logger.info(
				`next jito leader is in ${slotsUntilNextLeader} slots (fillTxId: ${fillTxId})`
			);
			if (slotsUntilNextLeader > 2) {
				logger.info(
					`next jito leader is too far away, skipping... (fillTxId: ${fillTxId})`
				);
				return ['', 0];
			}
			const blockHash =
				await this.driftClient.provider.connection.getLatestBlockhash(
					'processed'
				);

			const tx = new Transaction();
			for (const ix of ixs) {
				tx.add(ix);
			}

			tx.feePayer = this.driftClient.provider.wallet.publicKey;
			tx.recentBlockhash = blockHash.blockhash;

			const signedTx = await this.driftClient.provider.wallet.signTransaction(
				tx
			);
			let b: Bundle | Error = new Bundle(
				[new VersionedTransaction(signedTx.compileMessage())],
				2
			);
			b = b.addTipTx(
				this.tipPayerKeypair,
				100_000, // TODO: make this configurable?
				this.jitoTipAccount,
				blockHash.blockhash
			);
			if (b instanceof Error) {
				logger.error(
					`failed to attach tip: ${b.message} (fillTxId: ${fillTxId})`
				);
				return ['', 0];
			}
			this.jitoSearcherClient
				.sendBundle(b)
				.then((uuid) => {
					logger.info(
						`${this.name} sent bundle with uuid ${uuid} (fillTxId: ${fillTxId})`
					);
				})
				.catch((err) => {
					logger.error(
						`failed to send bundle: ${err.message} (fillTxId: ${fillTxId})`
					);
				});
		} else if (isNaN(this.transactionVersion)) {
			const tx = new Transaction();
			for (const ix of ixs) {
				tx.add(ix);
			}
			txResp = this.driftClient.txSender.send(tx, [], this.driftClient.opts);
		} else if (this.transactionVersion === 0) {
			const tx = await this.driftClient.txSender.getVersionedTransaction(
				ixs,
				[this.lookupTableAccount],
				[],
				this.driftClient.opts
			);
			txResp = this.driftClient.txSender.sendVersionedTransaction(
				tx,
				[],
				this.driftClient.opts
			);
		} else {
			throw new Error(
				`unsupported transaction version ${this.transactionVersion}`
			);
		}
		if (txResp) {
			txResp
				.then((resp: TxSigAndSlot) => {
					const duration = Date.now() - txStart;
					logger.info(
						`sent tx: ${resp.txSig}, took: ${duration}ms (fillTxId: ${fillTxId})`
					);

					const user = this.driftClient.getUser();
					this.sdkCallDurationHistogram.record(duration, {
						...metricAttrFromUserAccount(
							user.getUserAccountPublicKey(),
							user.getUserAccount()
						),
						method: 'sendTx',
					});

					const parseLogsStart = Date.now();
					this.processBulkFillTxLogs(nodesSent, resp.txSig)
						.then((successfulFills) => {
							const processBulkFillLogsDuration = Date.now() - parseLogsStart;
							logger.info(
								`parse logs took ${processBulkFillLogsDuration}ms, filled ${successfulFills} (fillTxId: ${fillTxId})`
							);

							// record successful fills
							const user = this.driftClient.getUser();
							this.successfulFillsCounter.add(
								successfulFills,
								metricAttrFromUserAccount(
									user.userAccountPublicKey,
									user.getUserAccount()
								)
							);
						})
						.catch((e) => {
							// console.error(e);
							logger.error(
								`Failed to process fill tx logs (error above) (fillTxId: ${fillTxId}):`
							);
							webhookMessage(
								`[${this.name}]: :x: error processing fill tx logs:\n${
									e.stack ? e.stack : e.message
								}`
							);
						});
				})
				.catch(async (e) => {
					// console.error(e);
					logger.error(
						`Failed to send packed tx (error above) (fillTxId: ${fillTxId}):`
					);
					const simError = e as SendTransactionError;

					if (simError.logs && simError.logs.length > 0) {
						const start = Date.now();
						await this.handleTransactionLogs(nodesSent, simError.logs);
						logger.error(
							`Failed to send tx, sim error tx logs took: ${
								Date.now() - start
							}ms (fillTxId: ${fillTxId})`
						);

						const errorCode = getErrorCode(e);

						if (
							!errorCodesToSuppress.includes(errorCode) &&
							!(e as Error).message.includes('Transaction was not confirmed')
						) {
							if (errorCode) {
								this.txSimErrorCounter.add(1, {
									errorCode: errorCode.toString(),
								});
							}
							webhookMessage(
								`[${this.name}]: :x: error simulating tx:\n${
									simError.logs ? simError.logs.join('\n') : ''
								}\n${e.stack || e}`
							);
						}
					}
				})
				.finally(() => {
					this.removeFillingNodes(nodesSent);
				});
		}
	}

	/**
	 * It's difficult to estimate CU cost of multi maker ix, so we'll just send it in its own transaction
	 * @param node node with multiple makers
	 */
	protected async tryFillMultiMakerPerpNodes(nodeToFill: NodeToFill) {
		const ixs: Array<TransactionInstruction> = [];
		const fillTxId = this.fillTxId++;
		logger.info(
			logMessageForNodeToFill(
				nodeToFill,
				`Filling multi maker perp node with ${nodeToFill.makerNodes.length} makers (fillTxId: ${fillTxId})`
			)
		);

		try {
			// first ix is compute budget
			const computeBudgetIx = ComputeBudgetProgram.requestUnits({
				units: 10_000_000,
				additionalFee: 0,
			});
			ixs.push(computeBudgetIx);

			const { makerInfos, takerUser, referrerInfo, marketType } =
				await this.getNodeToFillInfo(nodeToFill);

			if (!isVariant(marketType, 'perp')) {
				throw new Error('expected perp market type');
			}

			ixs.push(
				await this.driftClient.getFillPerpOrderIx(
					await getUserAccountPublicKey(
						this.driftClient.program.programId,
						takerUser.authority
					),
					takerUser,
					nodeToFill.node.order,
					makerInfos,
					referrerInfo
				)
			);

			this.fillingNodes.set(getNodeToFillSignature(nodeToFill), Date.now());

			if (this.revertOnFailure) {
				ixs.push(await this.driftClient.getRevertFillIx());
			}

			this.sendFillTx(fillTxId, [nodeToFill], ixs);
		} catch (e) {
			logger.error(
				`Error filling multi maker perp node (fillTxId: ${fillTxId}): ${
					e.stack ? e.stack : e.message
				}`
			);
		}
	}

	async tryBulkFillPerpNodes(nodesToFill: Array<NodeToFill>): Promise<number> {
		const ixs: Array<TransactionInstruction> = [];

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
		uniqueAccounts.add(this.driftClient.provider.wallet.publicKey.toString()); // fee payer goes first

		// first ix is compute budget
		const computeBudgetIx = ComputeBudgetProgram.requestUnits({
			units: 10_000_000,
			additionalFee: 0,
		});
		computeBudgetIx.keys.forEach((key) =>
			uniqueAccounts.add(key.pubkey.toString())
		);
		uniqueAccounts.add(computeBudgetIx.programId.toString());
		ixs.push(computeBudgetIx);

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
		const fillTxId = this.fillTxId++;
		for (const [idx, nodeToFill] of nodesToFill.entries()) {
			if (nodeToFill.makerNodes.length > 1) {
				await this.tryFillMultiMakerPerpNodes(nodeToFill);
				nodesSent.push(nodeToFill);
				continue;
			}
			logger.info(
				logMessageForNodeToFill(
					nodeToFill,
					`Filling perp node ${idx} (fillTxId: ${fillTxId})`
				)
			);

			const { makerInfos, takerUser, referrerInfo, marketType } =
				await this.getNodeToFillInfo(nodeToFill);

			if (!isVariant(marketType, 'perp')) {
				throw new Error('expected perp market type');
			}

			const ix = await this.driftClient.getFillPerpOrderIx(
				await getUserAccountPublicKey(
					this.driftClient.program.programId,
					takerUser.authority
				),
				takerUser,
				nodeToFill.node.order,
				makerInfos,
				referrerInfo
			);

			if (!ix) {
				logger.error(`failed to generate an ix`);
				break;
			}

			this.fillingNodes.set(getNodeToFillSignature(nodeToFill), Date.now());

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
				(runningTxSize + newIxCost + additionalAccountsCost >=
					MAX_TX_PACK_SIZE ||
					runningCUUsed + cuToUsePerFill >= MAX_CU_PER_TX) &&
				ixs.length > 1 // ensure at least 1 attempted fill
			) {
				logger.info(
					`Fully packed fill tx (ixs: ${ixs.length}): est. tx size ${
						runningTxSize + newIxCost + additionalAccountsCost
					}, max: ${MAX_TX_PACK_SIZE}, est. CU used: expected ${
						runningCUUsed + cuToUsePerFill
					}, max: ${MAX_CU_PER_TX}, (fillTxId: ${fillTxId})`
				);
				break;
			}

			// add to tx
			logger.info(
				`including taker ${(
					await getUserAccountPublicKey(
						this.driftClient.program.programId,
						takerUser.authority
					)
				).toString()}-${nodeToFill.node.order.orderId.toString()} (fillTxId: ${fillTxId})`
			);
			ixs.push(ix);
			runningTxSize += newIxCost + additionalAccountsCost;
			runningCUUsed += cuToUsePerFill;

			newAccounts.forEach((key) => uniqueAccounts.add(key.toString()));
			idxUsed++;
			nodesSent.push(nodeToFill);
		}

		logger.debug(
			`txPacker took ${Date.now() - txPackerStart}ms (fillTxId: ${fillTxId})`
		);

		if (nodesSent.length === 0) {
			return 0;
		}

		logger.info(
			`sending tx, ${
				uniqueAccounts.size
			} unique accounts, total ix: ${idxUsed}, calcd tx size: ${runningTxSize}, took ${
				Date.now() - txPackerStart
			}ms (fillTxId: ${fillTxId})`
		);

		if (this.revertOnFailure) {
			ixs.push(await this.driftClient.getRevertFillIx());
		}

		this.sendFillTx(fillTxId, nodesSent, ixs);

		return nodesSent.length;
	}

	protected filterPerpNodesForMarket(
		fillableNodes: Array<NodeToFill>,
		triggerableNodes: Array<NodeToTrigger>
	): {
		filteredFillableNodes: Array<NodeToFill>;
		filteredTriggerableNodes: Array<NodeToTrigger>;
	} {
		const seenFillableNodes = new Set<string>();
		const filteredFillableNodes = fillableNodes.filter((node) => {
			const sig = getNodeToFillSignature(node);
			if (seenFillableNodes.has(sig)) {
				return false;
			}
			seenFillableNodes.add(sig);
			return this.filterFillableNodes(node);
		});

		const seenTriggerableNodes = new Set<string>();
		const filteredTriggerableNodes = triggerableNodes.filter((node) => {
			const sig = getNodeToTriggerSignature(node);
			if (seenTriggerableNodes.has(sig)) {
				return false;
			}
			seenTriggerableNodes.add(sig);
			return this.filterTriggerableNodes(node);
		});

		return {
			filteredFillableNodes,
			filteredTriggerableNodes,
		};
	}

	protected async executeTriggerablePerpNodesForMarket(
		triggerableNodes: Array<NodeToTrigger>
	) {
		for (const nodeToTrigger of triggerableNodes) {
			nodeToTrigger.node.haveTrigger = true;
			logger.info(
				`trying to trigger (account: ${nodeToTrigger.node.userAccount.toString()}) order ${nodeToTrigger.node.order.orderId.toString()}`
			);

			const userAccount = this.orderSubscriber.usersAccounts.get(
				nodeToTrigger.node.userAccount.toString()
			).userAccount;

			const nodeSignature = getNodeToTriggerSignature(nodeToTrigger);
			this.triggeringNodes.set(nodeSignature, Date.now());

			this.driftClient
				.triggerOrder(
					nodeToTrigger.node.userAccount,
					userAccount,
					nodeToTrigger.node.order
				)
				.then((txSig) => {
					logger.info(
						`Triggered user (account: ${nodeToTrigger.node.userAccount.toString()}) order: ${nodeToTrigger.node.order.orderId.toString()}`
					);
					logger.info(`Tx: ${txSig}`);
				})
				.catch((error) => {
					nodeToTrigger.node.haveTrigger = false;

					const errorCode = getErrorCode(error);
					if (
						!errorCodesToSuppress.includes(errorCode) &&
						!(error as Error).message.includes('Transaction was not confirmed')
					) {
						logger.error(
							`Error (${errorCode}) triggering order for user (account: ${nodeToTrigger.node.userAccount.toString()}) order: ${nodeToTrigger.node.order.orderId.toString()}`
						);
						logger.error(error);
						webhookMessage(
							`[${
								this.name
							}]: :x: Error (${errorCode}) triggering order for user (account: ${nodeToTrigger.node.userAccount.toString()}) order: ${nodeToTrigger.node.order.orderId.toString()}\n${
								error.stack ? error.stack : error.message
							}`
						);
					}
				})
				.finally(() => {
					this.removeTriggeringNodes(nodeToTrigger);
				});
		}

		const user = this.driftClient.getUser();
		this.attemptedTriggersCounter.add(
			triggerableNodes.length,
			metricAttrFromUserAccount(
				user.userAccountPublicKey,
				user.getUserAccount()
			)
		);
	}

	protected async tryFill() {
		const startTime = Date.now();
		let ran = false;
		try {
			await tryAcquire(this.periodicTaskMutex).runExclusive(async () => {
				const currentSlot = this.slotSubscriber.getSlot();
				const dlob = await this.orderSubscriber.getDLOB(currentSlot);

				this.pruneThrottledNode();

				// 1) get all fillable nodes
				let fillableNodes: Array<NodeToFill> = [];
				let triggerableNodes: Array<NodeToTrigger> = [];
				for (const market of this.driftClient.getPerpMarketAccounts()) {
					try {
						const { nodesToFill, nodesToTrigger } = this.getPerpNodesForMarket(
							market,
							dlob
						);
						fillableNodes = fillableNodes.concat(nodesToFill);
						triggerableNodes = triggerableNodes.concat(nodesToTrigger);
					} catch (e) {
						console.error(e);
						webhookMessage(
							`[${this.name}]: :x: Failed to get fillable nodes for market ${
								market.marketIndex
							}:\n${e.stack ? e.stack : e.message}`
						);
						continue;
					}
				}

				// filter out nodes that we know cannot be filled
				const { filteredFillableNodes, filteredTriggerableNodes } =
					this.filterPerpNodesForMarket(fillableNodes, triggerableNodes);
				logger.debug(
					`filtered fillable nodes from ${fillableNodes.length} to ${filteredFillableNodes.length}, filtered triggerable nodes from ${triggerableNodes.length} to ${filteredTriggerableNodes.length}`
				);

				// fill the perp nodes
				await Promise.all([
					this.executeFillablePerpNodesForMarket(filteredFillableNodes),
					this.executeTriggerablePerpNodesForMarket(filteredTriggerableNodes),
				]);

				ran = true;
			});
		} catch (e) {
			if (e === E_ALREADY_LOCKED) {
				const user = this.driftClient.getUser();
				this.mutexBusyCounter.add(
					1,
					metricAttrFromUserAccount(
						user.getUserAccountPublicKey(),
						user.getUserAccount()
					)
				);
			} else {
				webhookMessage(
					`[${this.name}]: :x: uncaught error:\n${
						e.stack ? e.stack : e.message
					}`
				);
				throw e;
			}
		} finally {
			if (ran) {
				const duration = Date.now() - startTime;
				const user = this.driftClient.getUser();
				this.tryFillDurationHistogram.record(
					duration,
					metricAttrFromUserAccount(
						user.getUserAccountPublicKey(),
						user.getUserAccount()
					)
				);
				logger.debug(`tryFill done, took ${duration}ms`);

				await this.watchdogTimerMutex.runExclusive(async () => {
					this.watchdogTimerLastPatTime = Date.now();
				});
			}
		}
	}
}

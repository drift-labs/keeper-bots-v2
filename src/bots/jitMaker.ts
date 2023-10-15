/* eslint-disable @typescript-eslint/no-unused-vars */
import {
	DLOB,
	DriftEnv,
	BASE_PRECISION,
	BN,
	DriftClient,
	JupiterClient,
	getSignedTokenAmount,
	getTokenAmount,
	convertToNumber,
	promiseTimeout,
	PositionDirection,
	MarketType,
	ZERO,
	PRICE_PRECISION,
	DLOBSubscriber,
	UserMap,
	SlotSubscriber,
	QUOTE_PRECISION,
	DLOBNode,
	OraclePriceData,
	SwapMode,
	getVariant,
	isVariant,
	User,
} from '@drift-labs/sdk';
import { Mutex, tryAcquire, E_ALREADY_LOCKED } from 'async-mutex';
import { logger } from '../logger';
import { Bot } from '../types';
import {
	calculateBaseAmountToMarketMake,
	decodeName,
	getBestLimitAskExcludePubKey,
	getBestLimitBidExcludePubKey,
	isMarketVolatile,
	sleepMs,
} from '../utils';
import {
	JitterShotgun,
	JitterSniper,
	PriceType,
} from '@drift-labs/jit-proxy/lib';
import { assert } from '@drift-labs/sdk/lib/assert/assert';
import dotenv = require('dotenv');

dotenv.config();
import {
	ComputeBudgetProgram,
	AddressLookupTableAccount,
	VersionedTransaction,
	Connection,
	PublicKey,
	TransactionInstruction,
	Signer,
	ConfirmOptions,
	TransactionSignature,
} from '@solana/web3.js';
import { BaseBotConfig, JitMakerConfig } from 'src/config';

const TARGET_LEVERAGE_PER_ACCOUNT = 1;

/**
 * This is an example of a bot that implements the Bot interface.
 */
export class JitMaker implements Bot {
	public readonly name: string;
	public readonly dryRun: boolean;
	public readonly defaultIntervalMs: number = 1000;

	private driftEnv: DriftEnv;
	private periodicTaskMutex = new Mutex();

	private jitter: JitterSniper | JitterShotgun;
	private driftClient: DriftClient;
	private jupiterClient: JupiterClient;
	// private subaccountConfig: SubaccountConfig;
	private subAccountIds: Array<number>;
	private marketIndexes: Array<number>;

	private intervalIds: Array<NodeJS.Timer> = [];

	private watchdogTimerMutex = new Mutex();
	private watchdogTimerLastPatTime = Date.now();

	private dlobSubscriber: DLOBSubscriber;
	private slotSubscriber: SlotSubscriber;
	private userMap: UserMap;

	constructor(
		driftClient: DriftClient, // driftClient needs to have correct number of subaccounts listed
		jitter: JitterSniper | JitterShotgun,
		config: JitMakerConfig,
		driftEnv: DriftEnv
	) {
		this.subAccountIds = config.subaccounts ?? [0];
		this.marketIndexes = config.perpMarketIndicies ?? [0];
		this.jitter = jitter;
		this.driftClient = driftClient;
		this.jupiterClient = new JupiterClient({
			connection: this.driftClient.connection,
		});
		this.name = config.botId;
		this.dryRun = config.dryRun;
		this.driftEnv = driftEnv;

		this.userMap = new UserMap(
			this.driftClient,
			this.driftClient.userAccountSubscriptionConfig,
			false
		);
		this.slotSubscriber = new SlotSubscriber(this.driftClient.connection);
		this.dlobSubscriber = new DLOBSubscriber({
			dlobSource: this.userMap,
			slotSource: this.slotSubscriber,
			updateFrequency: 1000,
			driftClient: this.driftClient,
		});
	}

	/**
	 * Run initialization procedures for the bot.
	 */
	public async init(): Promise<void> {
		logger.info(`${this.name} initing`);

		// do stuff that takes some time
		await this.userMap.subscribe();
		await this.slotSubscriber.subscribe();
		await this.dlobSubscriber.subscribe();

		logger.info(`${this.name} init done`);
	}

	/**
	 * Reset the bot - usually you will reset any periodic tasks here
	 */
	public async reset(): Promise<void> {
		// reset any periodic tasks
		for (const intervalId of this.intervalIds) {
			clearInterval(intervalId as NodeJS.Timeout);
		}
		this.intervalIds = [];
	}

	public async startIntervalLoop(intervalMs: number): Promise<void> {
		const intervalId = setInterval(
			this.runPeriodicTasks.bind(this),
			intervalMs
		);
		this.intervalIds.push(intervalId);

		logger.info(`${this.name} Bot started! driftEnv: ${this.driftEnv}`);
	}

	/**
	 * Typically used for monitoring liveness.
	 * @returns true if bot is healthy, else false.
	 */
	public async healthCheck(): Promise<boolean> {
		let healthy = false;
		await this.watchdogTimerMutex.runExclusive(async () => {
			healthy =
				this.watchdogTimerLastPatTime > Date.now() - 2 * this.defaultIntervalMs;
		});
		return healthy;
	}

	/**
	 * Typical bot loop that runs periodically and pats the watchdog timer on completion.
	 *
	 */
	private async runPeriodicTasks() {
		const start = Date.now();
		let ran = false;
		try {
			await tryAcquire(this.periodicTaskMutex).runExclusive(async () => {
				console.log(
					`[${new Date().toISOString()}] Running JIT periodic tasks...`
				);
				for (let i = 0; i < this.marketIndexes.length; i++) {
					const perpIdx = this.marketIndexes[i];
					const subId = this.subAccountIds[i];
					this.driftClient.switchActiveUser(subId, this.driftClient.authority);
					console.log(perpIdx, subId);

					let spotMarketIndex = 0;
					const driftUser = this.driftClient.getUser(subId);
					const perpMarketAccount =
						this.driftClient.getPerpMarketAccount(perpIdx)!;
					const oraclePriceData =
						this.driftClient.getOracleDataForPerpMarket(perpIdx);

					const numMarketsForSubaccount = this.subAccountIds.filter(
						(num) => num === subId
					).length;
					const maxBase: number = calculateBaseAmountToMarketMake(
						perpMarketAccount,
						driftUser.getNetSpotMarketValue(),
						TARGET_LEVERAGE_PER_ACCOUNT / numMarketsForSubaccount // target leverage split amongst markets w/in a subacct
					);

					// const dollarDepth = 1000; // todo
					// const baseDepth = new BN(dollarDepth * QUOTE_PRECISION.toNumber()).mul(BASE_PRECISION).div(oraclePriceData.price);

					const perpMarketIndex = perpIdx;

					if (perpIdx == 0) {
						spotMarketIndex = 1;
					} else if (perpIdx == 1) {
						spotMarketIndex = 3;
					} else if (perpIdx == 2) {
						spotMarketIndex = 4;
					}

					this.jitter.setUserFilter((userAccount, userKey) => {
						let skip = userKey == driftUser.userAccountPublicKey.toBase58();

						if (
							isMarketVolatile(
								perpMarketAccount,
								oraclePriceData,
								0.01 // 100 bps
							)
						) {
							console.log('skipping, market is volatile');
							skip = true;
						}
						if (skip) {
							console.log('skipping user:', userKey);
						}

						return skip;
					});

					const bestDriftBid = getBestLimitBidExcludePubKey(
						this.dlobSubscriber.dlob,
						perpMarketAccount.marketIndex,
						MarketType.PERP,
						oraclePriceData.slot.toNumber(),
						oraclePriceData,
						driftUser.userAccountPublicKey
					);

					const bestDriftAsk = getBestLimitAskExcludePubKey(
						this.dlobSubscriber.dlob,
						perpMarketAccount.marketIndex,
						MarketType.PERP,
						oraclePriceData.slot.toNumber(),
						oraclePriceData,
						driftUser.userAccountPublicKey
					);
					if (!bestDriftBid || !bestDriftAsk) {
						logger.warn('skipping, no best bid/ask');
						return;
					}

					// const bestDriftBid = this.dlob.estimateFillWithExactBaseAmount(
					// 	{marketIndex: perpMarketAccount.marketIndex,
					// 	marketType: MarketType.SPOT,
					// 	baseAmount: baseDepth,
					// 	orderDirection: PositionDirection.SHORT,
					// 	slot: oraclePriceData.slot.toNumber(),
					// 	oraclePriceData
					// 	}
					// ).mul(BASE_PRECISION).div(baseDepth);

					// const bestDriftAsk = this.dlob.estimateFillWithExactBaseAmount(
					// 	{marketIndex: perpMarketAccount.marketIndex,
					// 	marketType: MarketType.PERP,
					// 	baseAmount: baseDepth,
					// 	orderDirection: PositionDirection.LONG,
					// 	slot: oraclePriceData.slot.toNumber(),
					// 	oraclePriceData
					// 	}
					// ).mul(BASE_PRECISION).div(baseDepth);

					const bidOffset = bestDriftBid
						.getPrice(oraclePriceData, this.dlobSubscriber.slotSource.getSlot())
						.sub(oraclePriceData.price);

					const askOffset = bestDriftAsk
						.getPrice(oraclePriceData, this.dlobSubscriber.slotSource.getSlot())
						.sub(oraclePriceData.price);

					this.jitter.updatePerpParams(perpMarketIndex, {
						maxPosition: new BN(maxBase * BASE_PRECISION.toNumber()),
						minPosition: new BN(-maxBase * BASE_PRECISION.toNumber()),
						bid: bidOffset,
						ask: askOffset,
						// ask: new BN(1),
						priceType: PriceType.ORACLE,
						subAccountId: subId,
					});

					if (spotMarketIndex != 0) {
						this.jitter.updateSpotParams(spotMarketIndex, {
							maxPosition: new BN((maxBase / 7) * BASE_PRECISION.toNumber()),
							minPosition: new BN((-maxBase / 7) * BASE_PRECISION.toNumber()),
							bid: BN.min(bidOffset, new BN(-1)),
							ask: BN.max(askOffset, new BN(1)),
							priceType: PriceType.ORACLE,
							subAccountId: subId,
						});
						console.log(
							'sub account:',
							this.driftClient.activeSubAccountId,
							'vs',
							this.subAccountIds[i]
						);
						if (this.driftClient.activeSubAccountId == this.subAccountIds[i]) {
							let maxSize = 200;
							if (spotMarketIndex == 1) {
								maxSize *= 2;
							}
							await this.doBasisRebalance(
								this.driftClient,
								this.jupiterClient,
								driftUser,
								perpMarketIndex,
								spotMarketIndex,
								maxSize //todo: $200-$400 max rebalance to start
							);
						}
					}
				}
				await sleepMs(5000); // 30 seconds

				console.log(`done: ${Date.now() - start}ms`);
				ran = true;
			});
		} catch (e) {
			if (e === E_ALREADY_LOCKED) {
				return;
			} else {
				throw e;
			}
		} finally {
			if (ran) {
				const duration = Date.now() - start;
				logger.debug(`${this.name} Bot took ${duration}ms to run`);

				await this.watchdogTimerMutex.runExclusive(async () => {
					this.watchdogTimerLastPatTime = Date.now();
				});
			}
		}
	}

	private async doBasisRebalance(
		driftClient: DriftClient,
		jupiterClient: JupiterClient,
		u: User,
		perpIndex: number,
		spotIndex: number,
		maxDollarSize = 0
	) {
		const solPerpMarket = driftClient.getPerpMarketAccount(perpIndex)!;
		const solSpotMarket = driftClient.getSpotMarketAccount(spotIndex)!;
		const uSpotPosition = u.getSpotPosition(spotIndex);
		assert(
			solPerpMarket.amm.oracle.toString() === solSpotMarket.oracle.toString()
		);

		const perpSize =
			u.getPerpPositionWithLPSettle(perpIndex)[0].baseAssetAmount;

		let spotSize = ZERO;
		if (uSpotPosition) {
			spotSize = getSignedTokenAmount(
				getTokenAmount(
					uSpotPosition.scaledBalance,
					solSpotMarket,
					uSpotPosition.balanceType
				),
				uSpotPosition.balanceType
			);
		}
		const spotSizeNum = convertToNumber(
			spotSize,
			new BN(10 ** solSpotMarket.decimals)
		);
		const perpSizeNum = convertToNumber(perpSize, BASE_PRECISION);
		const mismatch = perpSizeNum + spotSizeNum;

		const lastOraclePrice = convertToNumber(
			solPerpMarket.amm.historicalOracleData.lastOraclePrice,
			PRICE_PRECISION
		);

		// only do $10
		if (Math.abs(mismatch * lastOraclePrice) > 10) {
			let tradeSize;

			const direction =
				mismatch < 0 ? PositionDirection.LONG : PositionDirection.SHORT;
			tradeSize = new BN(Math.abs(mismatch) * BASE_PRECISION.toNumber());

			if (maxDollarSize != 0) {
				tradeSize = BN.min(
					new BN(
						(maxDollarSize /
							(solPerpMarket.amm.historicalOracleData.lastOraclePrice.toNumber() /
								1e6)) *
							BASE_PRECISION.toNumber()
					),
					tradeSize
				);
			}

			if (perpIndex != 0) {
				tradeSize = tradeSize.div(new BN(10)); //1e8 decimal
			}

			console.log('direction=', direction, tradeSize.toNumber() / 1e9, 'base');
			try {
				const dd = await this.doSpotHedgeTrades(
					spotIndex,
					driftClient,
					jupiterClient,
					tradeSize,
					new BN(maxDollarSize * 1e6 * 1.001),
					direction
				);
				if (dd) {
					await this.sendBasisTx(driftClient, dd.ixs, dd.lookupTables);
				}
			} catch (e) {
				console.error(e);
			}
		}
	}

	async sendBasisTx(
		driftClient: DriftClient,
		theInstr: TransactionInstruction[],
		lookupTablesToUse: AddressLookupTableAccount[]
	) {
		const cuEstimate = 2_000_000;
		const chunk = [
			ComputeBudgetProgram.setComputeUnitLimit({
				units: cuEstimate,
			}),
			ComputeBudgetProgram.setComputeUnitPrice({
				microLamports: Math.floor(1000 / (cuEstimate * 1e-6)),
			}),
			...theInstr,
		];
		try {
			const chunkedTx: VersionedTransaction | null = await promiseTimeout(
				driftClient.txSender.getVersionedTransaction(
					chunk,
					lookupTablesToUse,
					[],
					driftClient.opts
				),
				1000
			);
			if (chunkedTx === null) {
				logger.error(`Timed out getting versioned Transaction for tx chunk`);
				return;
			}
			const tx = await sendVersionedTransaction(
				driftClient,
				chunkedTx,
				[],
				// { skipPreflight: true },
				driftClient.opts,
				1000
			);
			logger.info(`tx signature: https://solscan.io/tx/${tx}`);
		} catch (e) {
			logger.error(`Failed to send chunked placeOrder tx: ${e}`);
			return;
		}
	}

	async doSpotHedgeTrades(
		spotMarketIndex: number,
		driftClient: DriftClient,
		jupiterClient: JupiterClient,
		tradeSize: BN,
		maxDollarSize: BN,
		direction: PositionDirection
	): Promise<
		| {
				ixs: TransactionInstruction[];
				lookupTables: AddressLookupTableAccount[];
		  }
		| undefined
	> {
		let jupSwapMode: SwapMode;
		let tsize: BN;
		let inMarketIndex: number;
		let outMarketIndex: number;
		// let jupReduceOnly: SwapReduceOnly;
		if (isVariant(direction, 'long')) {
			// sell USDC, buy spotMarketIndex
			inMarketIndex = 0;
			outMarketIndex = spotMarketIndex;
			jupSwapMode = 'ExactIn';
			tsize = maxDollarSize;
			// jupReduceOnly = SwapReduceOnly.In;
		} else {
			// sell spotMarketIndex, buy USDC
			inMarketIndex = spotMarketIndex;
			outMarketIndex = 0;
			jupSwapMode = 'ExactIn';
			tsize = tradeSize;
			// jupReduceOnly = SwapReduceOnly.In;
		}

		logger.info(
			`Jupiter swap: ${getVariant(
				direction
			)}: ${tradeSize.toString()}, inMarket: ${inMarketIndex}, outMarket: ${outMarketIndex},
			jupSwapMode: ${jupSwapMode}`
		);

		const inMarket = driftClient.getSpotMarketAccount(inMarketIndex)!;
		const outMarket = driftClient.getSpotMarketAccount(outMarketIndex)!;
		const routes = await jupiterClient.getRoutes({
			inputMint: inMarket.mint,
			outputMint: outMarket.mint,
			amount: tsize,
			swapMode: jupSwapMode,
		});

		// TODO: check if price of route is not too far from oracle price
		if (routes.length === 0) {
			return undefined;
		}

		return driftClient.getJupiterSwapIx({
			jupiterClient,
			outMarketIndex,
			inMarketIndex,
			amount: tsize,
			swapMode: jupSwapMode,
			route: routes[0],
			// reduceOnly: jupReduceOnly,
			slippageBps: 10,
		});
	}
}

export async function sendVersionedTransaction(
	driftClient: DriftClient,
	tx: VersionedTransaction,
	additionalSigners?: Array<Signer>,
	opts?: ConfirmOptions,
	timeoutMs = 5000
): Promise<TransactionSignature | null> {
	// @ts-ignore
	tx.sign((additionalSigners ?? []).concat(driftClient.provider.wallet.payer));

	if (opts === undefined) {
		opts = driftClient.provider.opts;
	}

	const rawTransaction = tx.serialize();
	let txid: TransactionSignature | null;
	try {
		txid = await promiseTimeout(
			driftClient.provider.connection.sendRawTransaction(rawTransaction, opts),
			timeoutMs
		);
	} catch (e) {
		console.error(e);
		throw e;
	}

	return txid;
}

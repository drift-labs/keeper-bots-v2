/* eslint-disable @typescript-eslint/no-non-null-assertion */
import {
	BN,
	convertToNumber,
	DriftClient,
	User,
	isVariant,
	BASE_PRECISION,
	PRICE_PRECISION,
	QUOTE_PRECISION,
	PerpPosition,
	UserMap,
	ZERO,
	getTokenAmount,
	SpotPosition,
	PerpMarketAccount,
	SpotMarketAccount,
	QUOTE_SPOT_MARKET_INDEX,
	calculateMarketAvailablePNL,
	findDirectionToClose,
	getSignedTokenAmount,
	standardizeBaseAssetAmount,
	TEN_THOUSAND,
	PositionDirection,
	isUserBankrupt,
	JupiterClient,
	MarketType,
	MarketTypeStr,
	getVariant,
	OraclePriceData,
	UserAccount,
	OptionalOrderParams,
	TEN,
	PriorityFeeSubscriber,
	QuoteResponse,
	getLimitOrderParams,
	PERCENTAGE_PRECISION,
	DLOB,
	calculateEstimatedPerpEntryPrice,
	deriveOracleAuctionParams,
	getOrderParams,
	OrderType,
	getTokenValue,
	calculateClaimablePnl,
	isOperationPaused,
	PerpOperation,
	WRAPPED_SOL_MINT,
} from '@drift-labs/sdk';

import { PrometheusExporter } from '@opentelemetry/exporter-prometheus';
import {
	ExplicitBucketHistogramAggregation,
	InstrumentType,
	MeterProvider,
	View,
} from '@opentelemetry/sdk-metrics-base';
import {
	Meter,
	ObservableGauge,
	BatchObservableResult,
	Histogram,
} from '@opentelemetry/api-metrics';

import { logger } from '../logger';
import { Bot, TwapExecutionProgress } from '../types';
import { RuntimeSpec, metricAttrFromUserAccount } from '../metrics';
import { webhookMessage } from '../webhook';
import { LiquidatorConfig } from '../config';
import { getPerpMarketTierNumber, perpTierIsAsSafeAs } from '@drift-labs/sdk';
import {
	ComputeBudgetProgram,
	PublicKey,
	AddressLookupTableAccount,
	TransactionInstruction,
} from '@solana/web3.js';
import {
	createAssociatedTokenAccountInstruction,
	createCloseAccountInstruction,
	getAssociatedTokenAddress,
} from '@solana/spl-token';
import {
	calculateAccountValueUsd,
	checkIfAccountExists,
	handleSimResultError,
	isSolLstToken,
	simulateAndGetTxWithCUs,
	SimulateAndGetTxWithCUsResponse,
} from '../utils';

const errorCodesToSuppress = [
	6004, // Error Number: 6004. Error Message: Sufficient collateral.
	6010, // Error Number: 6010. Error Message: User Has No Position In Market.
];

enum UserBucket {
	CAN_BE_LIQUIDATED = 'canBeLiquidated',
	UNHEALTHY_HIGH = 'unhealthyHigh',
	UNHEALTHY_MEDIUM = 'unhealthyMedium',
	UNHEALTHY_LOW = 'unhealthyLow',
}

const BPS_PRECISION = 10000;
const LIQUIDATE_THROTTLE_BACKOFF = 5000; // the time to wait before trying to liquidate a throttled user again

function calculateSpotTokenAmountToLiquidate(
	driftClient: DriftClient,
	liquidatorUser: User,
	liquidateePosition: SpotPosition,
	maxPositionTakeoverPctOfCollateralNum: BN,
	maxPositionTakeoverPctOfCollateralDenom: BN
): BN {
	const spotMarket = driftClient.getSpotMarketAccount(
		liquidateePosition.marketIndex
	);
	if (!spotMarket) {
		logger.error(`No spot market found for ${liquidateePosition.marketIndex}`);
		return ZERO;
	}

	const tokenPrecision = new BN(10 ** spotMarket.decimals);

	const oraclePrice = driftClient.getOracleDataForSpotMarket(
		liquidateePosition.marketIndex
	).price;
	const collateralToSpend = liquidatorUser
		.getFreeCollateral()
		.mul(PRICE_PRECISION)
		.mul(maxPositionTakeoverPctOfCollateralNum)
		.mul(tokenPrecision);
	const maxSpendTokenAmountToLiquidate = collateralToSpend.div(
		oraclePrice
			.mul(QUOTE_PRECISION)
			.mul(maxPositionTakeoverPctOfCollateralDenom)
	);

	const liquidateeTokenAmount = getTokenAmount(
		liquidateePosition.scaledBalance,
		spotMarket,
		liquidateePosition.balanceType
	);

	if (maxSpendTokenAmountToLiquidate.gt(liquidateeTokenAmount)) {
		return liquidateeTokenAmount;
	} else {
		return maxSpendTokenAmountToLiquidate;
	}
}

enum METRIC_TYPES {
	total_leverage = 'total_leverage',
	total_collateral = 'total_collateral',
	free_collateral = 'free_collateral',
	perp_posiiton_value = 'perp_position_value',
	perp_posiiton_base = 'perp_position_base',
	perp_posiiton_quote = 'perp_position_quote',
	initial_margin_requirement = 'initial_margin_requirement',
	maintenance_margin_requirement = 'maintenance_margin_requirement',
	initial_margin = 'initial_margin',
	maintenance_margin = 'maintenance_margin',
	unrealized_pnl = 'unrealized_pnl',
	unrealized_funding_pnl = 'unrealized_funding_pnl',
	sdk_call_duration_histogram = 'sdk_call_duration_histogram',
	runtime_specs = 'runtime_specs',
	user_map_user_account_keys = 'user_map_user_account_keys',
}

/**
 * LiquidatorBot implements a simple liquidation bot for the Drift V2 Protocol. Liquidations work by taking over
 * a portion of the endangered account's position, so collateral is required in order to run this bot. The bot
 * will spend at most maxPositionTakeoverPctOfCollateral of its free collateral on any endangered account.
 *
 */
export class LiquidatorBot implements Bot {
	public readonly name: string;
	public readonly dryRun: boolean;
	public readonly defaultIntervalMs: number = 5000;

	private metricsInitialized = false;
	private metricsPort?: number;
	private meter?: Meter;
	private exporter?: PrometheusExporter;
	private throttledUsers = new Map<string, number>();
	private disableAutoDerisking: boolean;
	private liquidatorConfig: LiquidatorConfig;

	// metrics
	private runtimeSpecsGauge?: ObservableGauge;
	private totalLeverage?: ObservableGauge;
	private totalCollateral?: ObservableGauge;
	private freeCollateral?: ObservableGauge;
	private perpPositionValue?: ObservableGauge;
	private perpPositionBase?: ObservableGauge;
	private perpPositionQuote?: ObservableGauge;
	private initialMarginRequirement?: ObservableGauge;
	private maintenanceMarginRequirement?: ObservableGauge;
	private unrealizedPnL?: ObservableGauge;
	private unrealizedFundingPnL?: ObservableGauge;
	private sdkCallDurationHistogram?: Histogram;
	private userMapUserAccountKeysGauge?: ObservableGauge;

	private driftClient: DriftClient;
	private serumLookupTableAddress?: PublicKey;
	private driftLookupTables?: AddressLookupTableAccount;
	private driftSpotLookupTables?: AddressLookupTableAccount;

	private perpMarketIndicies: number[] = [];
	private spotMarketIndicies: number[] = [];
	private activeSubAccountId: number;
	private allSubaccounts: Set<number>;
	private perpMarketToSubAccount: Map<number, number>;
	private spotMarketToSubAccount: Map<number, number>;
	private intervalIds: Array<NodeJS.Timer> = [];
	private userMap: UserMap;
	private deriskMutex = new Uint8Array(new SharedArrayBuffer(1));
	private liquidateMutex = new Uint8Array(new SharedArrayBuffer(1));
	private runtimeSpecs: RuntimeSpec;
	private jupiterClient?: JupiterClient;
	private twapExecutionProgresses?: Map<string, TwapExecutionProgress>; // key: this.getTwapProgressKey, value: TwapExecutionProgress
	private excludedAccounts: Set<string>;

	private priorityFeeSubscriber: PriorityFeeSubscriber;

	private usersCanBeLiquidated: Array<{
		user: User;
		userKey: string;
		marginRequirement: BN;
		canBeLiquidated: boolean;
		health: number;
	}> = [];
	private usersUnhealthyHigh: Array<{
		user: User;
		userKey: string;
		marginRequirement: BN;
		canBeLiquidated: boolean;
		health: number;
	}> = [];
	private usersUnhealthyMedium: Array<{
		user: User;
		userKey: string;
		marginRequirement: BN;
		canBeLiquidated: boolean;
		health: number;
	}> = [];
	private usersUnhealthyLow: Array<{
		user: User;
		userKey: string;
		marginRequirement: BN;
		canBeLiquidated: boolean;
		health: number;
	}> = [];
	/**
	 * Max percentage of collateral to spend on liquidating a single position.
	 */
	private maxPositionTakeoverPctOfCollateralNum: BN;
	private maxPositionTakeoverPctOfCollateralDenom = new BN(100);

	private watchdogTimerLastPatTime = Date.now();

	constructor(
		driftClient: DriftClient,
		userMap: UserMap,
		runtimeSpec: RuntimeSpec,
		config: LiquidatorConfig,
		defaultSubaccountId: number,
		priorityFeeSubscriber: PriorityFeeSubscriber,
		SERUM_LOOKUP_TABLE?: PublicKey
	) {
		this.liquidatorConfig = config;
		if (this.liquidatorConfig.maxSlippageBps === undefined) {
			this.liquidatorConfig.maxSlippageBps =
				this.liquidatorConfig.maxSlippagePct!;
		}
		if (this.liquidatorConfig.deriskAuctionDurationSlots === undefined) {
			this.liquidatorConfig.deriskAuctionDurationSlots = 100;
		}

		this.liquidatorConfig.deriskAlgoPerp =
			config.deriskAlgoPerp ?? config.deriskAlgo;
		this.liquidatorConfig.deriskAlgoSpot =
			config.deriskAlgoSpot ?? config.deriskAlgo;

		if (this.liquidatorConfig.spotDustValueThreshold !== undefined) {
			this.liquidatorConfig.spotDustValueThresholdBN = new BN(
				Math.floor(
					this.liquidatorConfig.spotDustValueThreshold *
						QUOTE_PRECISION.toNumber()
				)
			);
		}

		this.name = config.botId;
		this.dryRun = config.dryRun;
		this.driftClient = driftClient;
		this.runtimeSpecs = runtimeSpec;
		this.userMap = userMap;

		this.metricsPort = config.metricsPort;
		if (this.metricsPort) {
			this.initializeMetrics();
		}

		if (!config.disableAutoDerisking) {
			this.disableAutoDerisking = false;
		} else {
			this.disableAutoDerisking = config.disableAutoDerisking;
		}
		logger.info(
			`${this.name} disableAutoDerisking: ${this.disableAutoDerisking}, notifyLiquidations: ${this.liquidatorConfig.notifyOnLiquidation}`
		);

		this.allSubaccounts = new Set<number>();
		this.allSubaccounts.add(defaultSubaccountId);
		this.activeSubAccountId = defaultSubaccountId;
		this.perpMarketToSubAccount = new Map<number, number>();
		this.spotMarketToSubAccount = new Map<number, number>();

		if (
			config.perpSubAccountConfig &&
			Object.keys(config.perpSubAccountConfig).length > 0
		) {
			logger.info('Loading perp markets to watch from perpSubAccountConfig');
			for (const subAccount of Object.keys(config.perpSubAccountConfig)) {
				const marketsForAccount =
					config.perpSubAccountConfig[parseInt(subAccount)] || [];
				for (const market of marketsForAccount) {
					this.perpMarketToSubAccount.set(market, parseInt(subAccount));
					this.allSubaccounts.add(parseInt(subAccount));
				}
			}
			this.perpMarketIndicies = Object.values(
				config.perpSubAccountConfig
			).flat();
		} else {
			// this will be done in `init` since driftClient needs to be subcribed to first
			logger.info(
				`No perpSubAccountconfig provided, will watch all perp markets on subaccount ${this.activeSubAccountId}`
			);
		}

		if (
			config.spotSubAccountConfig &&
			Object.keys(config.spotSubAccountConfig).length > 0
		) {
			logger.info('Loading spot markets to watch from spotSubAccountConfig');
			for (const subAccount of Object.keys(config.spotSubAccountConfig)) {
				const marketsForAccount =
					config.spotSubAccountConfig[parseInt(subAccount)] || [];
				for (const market of marketsForAccount) {
					this.spotMarketToSubAccount.set(market, parseInt(subAccount));
					this.allSubaccounts.add(parseInt(subAccount));
				}
			}
			this.spotMarketIndicies = Object.values(
				config.spotSubAccountConfig
			).flat();
		} else {
			// this will be done in `init` since driftClient needs to be subcribed to first
			logger.info(
				`No spotSubAccountconfig provided, will watch all spot markets on subaccount ${this.activeSubAccountId}`
			);
		}

		// Load a list of accounts that we will *not* bother trying to liquidate
		// For whatever reason, this value is being parsed as an array even though
		// it is declared as a set, so if we merely assign it, then we'd end up
		// with a compile error later saying `has is not a function`.
		this.excludedAccounts = new Set<string>(config.excludedAccounts);
		logger.info(`Liquidator disregarding accounts: ${config.excludedAccounts}`);

		// ensure driftClient has all subaccounts tracked and subscribed
		for (const subaccount of this.allSubaccounts) {
			if (!this.driftClient.hasUser(subaccount)) {
				this.driftClient.addUser(subaccount).then((subscribed) => {
					logger.info(
						`Added subaccount ${subaccount} to driftClient since it was missing (subscribed: ${subscribed}))`
					);
				});
			}
		}

		const nowSec = Math.floor(Date.now() / 1000);
		this.twapExecutionProgresses = new Map<string, TwapExecutionProgress>();
		if (this.useTwap('perp')) {
			for (const marketIndex of this.perpMarketIndicies) {
				const subaccount = this.perpMarketToSubAccount.get(marketIndex);
				if (subaccount === undefined) {
					throw new Error(
						`Misconfiguration: perp market ${marketIndex} is in perpMarketIndicies but not perpMarketToSubaccount`
					);
				}
				this.twapExecutionProgresses.set(
					this.getTwapProgressKey(MarketType.PERP, subaccount, marketIndex),
					new TwapExecutionProgress({
						currentPosition: new BN(0),
						targetPosition: new BN(0), // target positions to close
						overallDurationSec: this.liquidatorConfig.twapDurationSec!,
						startTimeSec: nowSec,
					})
				);
			}
		}

		if (this.useTwap('spot')) {
			for (const marketIndex of this.spotMarketIndicies) {
				const subaccount = this.spotMarketToSubAccount.get(marketIndex);
				if (subaccount === undefined) {
					throw new Error(
						`Misconfiguration: spot market ${marketIndex} is in spotMarketIndicies but not spotMarketToSubaccount`
					);
				}
				this.twapExecutionProgresses.set(
					this.getTwapProgressKey(MarketType.SPOT, subaccount, marketIndex),
					new TwapExecutionProgress({
						currentPosition: new BN(0),
						targetPosition: new BN(0), // target positions to close
						overallDurationSec: this.liquidatorConfig.twapDurationSec!,
						startTimeSec: nowSec,
					})
				);
			}
		}

		// jupiter only works with mainnet
		if (config.useJupiter && this.runtimeSpecs.driftEnv === 'mainnet-beta') {
			this.jupiterClient = new JupiterClient({
				connection: this.driftClient.connection,
			});
		}
		this.priorityFeeSubscriber = priorityFeeSubscriber;
		this.priorityFeeSubscriber.updateAddresses([
			new PublicKey('6gMq3mRCKf8aP3ttTyYhuijVZ2LGi14oDsBbkgubfLB3'), // USDC SPOT
		]);

		if (!config.maxPositionTakeoverPctOfCollateral) {
			// spend 50% of collateral by default
			this.maxPositionTakeoverPctOfCollateralNum = new BN(50);
		} else {
			if (config.maxPositionTakeoverPctOfCollateral > 1.0) {
				logger.warn(
					`liquidator.config.maxPositionTakeoverPctOfCollateral is > 1.00: ${config.maxPositionTakeoverPctOfCollateral}`
				);
			}
			this.maxPositionTakeoverPctOfCollateralNum = new BN(
				config.maxPositionTakeoverPctOfCollateral * 100.0
			);
		}
		this.serumLookupTableAddress = SERUM_LOOKUP_TABLE;
	}

	private getSubAccountIdToLiquidatePerp(
		marketIndex: number
	): number | undefined {
		if (!this.perpMarketIndicies.includes(marketIndex)) {
			return undefined;
		}
		const subAccountId = this.perpMarketToSubAccount.get(marketIndex);
		if (subAccountId === undefined) {
			logger.info(
				`no configured subaccount to liquidate perp market ${marketIndex}`
			);
			return undefined;
		}

		if (!this.hasCollateralToLiquidate(subAccountId)) {
			logger.info(
				`subaccount ${subAccountId} has insufficient collateral to liquidate perp market ${marketIndex}`
			);
			return undefined;
		}
		return subAccountId;
	}

	private getSubAccountIdToLiquidateSpot(
		marketIndex: number
	): number | undefined {
		if (!this.spotMarketIndicies.includes(marketIndex)) {
			return undefined;
		}
		const subAccountId = this.spotMarketToSubAccount.get(marketIndex);
		if (subAccountId === undefined) {
			logger.info(
				`no configured subaccount to liquidate spot market ${marketIndex}`
			);
			return undefined;
		}
		if (!this.hasCollateralToLiquidate(subAccountId)) {
			logger.info(
				`subaccount ${subAccountId} has insufficient collateral to liquidate spot market ${marketIndex}`
			);
			return undefined;
		}
		return subAccountId;
	}

	private getLiquidatorUserForSpotMarket(
		marketIndex: number
	): User | undefined {
		const subAccountId = this.spotMarketToSubAccount.get(marketIndex);
		if (subAccountId === undefined) {
			return undefined;
		}
		return this.driftClient.getUser(subAccountId);
	}

	private getLiquidatorUserForPerpMarket(
		marketIndex: number
	): User | undefined {
		const subAccountId = this.perpMarketToSubAccount.get(marketIndex);
		if (subAccountId === undefined) {
			return undefined;
		}
		return this.driftClient.getUser(subAccountId);
	}

	private async buildVersionedTransactionWithSimulatedCus(
		ixs: Array<TransactionInstruction>,
		luts: Array<AddressLookupTableAccount>,
		cuPriceMicroLamports?: number
	): Promise<SimulateAndGetTxWithCUsResponse> {
		const fullIxs = [
			ComputeBudgetProgram.setComputeUnitLimit({
				units: 1_400_000, // will be overwriten in sim
			}),
		];
		if (cuPriceMicroLamports !== undefined) {
			fullIxs.push(
				ComputeBudgetProgram.setComputeUnitPrice({
					microLamports: cuPriceMicroLamports,
				})
			);
		}
		fullIxs.push(...ixs);

		let resp: SimulateAndGetTxWithCUsResponse;
		try {
			const recentBlockhash =
				await this.driftClient.connection.getLatestBlockhash('confirmed');
			resp = await simulateAndGetTxWithCUs({
				ixs: fullIxs,
				connection: this.driftClient.connection,
				payerPublicKey: this.driftClient.wallet.publicKey,
				lookupTableAccounts: luts,
				cuLimitMultiplier: 1.2,
				doSimulation: true,
				dumpTx: false,
				recentBlockhash: recentBlockhash.blockhash,
			});
		} catch (e) {
			const err = e as Error;
			logger.error(
				`error in buildVersionedTransactionWithSimulatedCus, using max CUs: ${err.message}\n${err.stack}`
			);
			resp = {
				cuEstimate: -1,
				simTxLogs: null,
				simError: err,
				simTxDuration: -1,
				// @ts-ignore
				tx: undefined,
			};
		}
		return resp;
	}

	private useTwap(marketType: MarketTypeStr) {
		const derisk =
			marketType === 'perp'
				? this.liquidatorConfig.deriskAlgoPerp
				: marketType === 'spot'
				? this.liquidatorConfig.deriskAlgoSpot
				: undefined;

		return (
			derisk === 'twap' && this.liquidatorConfig.twapDurationSec !== undefined
		);
	}

	private getTwapProgressKey(
		marketType: MarketType,
		subaccountId: number,
		marketIndex: number
	): string {
		return `${getVariant(marketType)}-${subaccountId}-${marketIndex}`;
	}

	public async init() {
		logger.info(`${this.name} initing`);

		this.driftLookupTables =
			await this.driftClient.fetchMarketLookupTableAccount();

		let serumLut: AddressLookupTableAccount | null = null;
		if (this.serumLookupTableAddress !== undefined) {
			serumLut = (
				await this.driftClient.connection.getAddressLookupTable(
					this.serumLookupTableAddress
				)
			).value;
		}
		if (
			this.runtimeSpecs.driftEnv === 'mainnet-beta' &&
			serumLut === null &&
			this.liquidatorConfig.useJupiter
		) {
			throw new Error(
				`Failed to load LUT for drift spot accounts at ${this.serumLookupTableAddress?.toBase58()}, jupiter swaps will fail`
			);
		} else {
			this.driftSpotLookupTables = serumLut!;
		}

		const subAccount = this.driftClient.activeSubAccountId;
		if (this.perpMarketIndicies.length === 0) {
			this.perpMarketIndicies = this.driftClient
				.getPerpMarketAccounts()
				.map((m) => {
					return m.marketIndex;
				});

			for (const market of this.perpMarketIndicies) {
				this.perpMarketToSubAccount.set(market, subAccount);
				this.allSubaccounts.add(subAccount);
			}
		}

		if (this.spotMarketIndicies.length === 0) {
			this.spotMarketIndicies = this.driftClient
				.getSpotMarketAccounts()
				.map((m) => {
					return m.marketIndex;
				});

			for (const market of this.spotMarketIndicies) {
				this.spotMarketToSubAccount.set(market, subAccount);
				this.allSubaccounts.add(subAccount);
			}
		}
		logger.info(
			`SubAccountID -> perpMarketIndex:\n${JSON.stringify(
				Object.fromEntries(this.perpMarketToSubAccount),
				null,
				2
			)}`
		);
		logger.info(
			`SubAccountID -> spotMarketIndex:\n${JSON.stringify(
				Object.fromEntries(this.spotMarketToSubAccount),
				null,
				2
			)}`
		);

		await webhookMessage(`[${this.name}]: started`);
	}

	public async reset() {
		for (const intervalId of this.intervalIds) {
			clearInterval(intervalId as NodeJS.Timeout);
		}
		this.intervalIds = [];

		await this.userMap!.unsubscribe();
	}

	public async startIntervalLoop(intervalMs?: number): Promise<void> {
		this.intializeBucketUsers();
		this.tryLiquidateStart();
		const intervalId = setInterval(
			this.tryLiquidateStart.bind(this),
			intervalMs
		);
		this.intervalIds.push(intervalId);

		if (!this.disableAutoDerisking) {
			const deRiskIntervalId = setInterval(
				this.derisk.bind(this),
				3.3 * intervalMs!
			); // try to make it not overlap with the normal liquidation loop
			this.intervalIds.push(deRiskIntervalId);
		}

		logger.info(`${this.name} Bot started!`);

		for (const subAccount of this.allSubaccounts) {
			const freeCollateral = this.driftClient
				.getUser(subAccount)
				.getFreeCollateral();
			logger.info(
				`[${
					this.name
				}] Subaccount: ${subAccount}:  free collateral: $${convertToNumber(
					freeCollateral,
					QUOTE_PRECISION
				)}, spending at most ${
					(this.maxPositionTakeoverPctOfCollateralNum.toNumber() /
						this.maxPositionTakeoverPctOfCollateralDenom.toNumber()) *
					100.0
				}% per liquidation`
			);
		}
	}

	public async healthCheck(): Promise<boolean> {
		let healthy = false;

		// check if we've ran the main loop recently
		healthy =
			this.watchdogTimerLastPatTime > Date.now() - 3 * this.defaultIntervalMs;

		if (!healthy) {
			logger.error(
				`Bot ${this.name} is unhealthy, last pat time: ${
					this.watchdogTimerLastPatTime - Date.now()
				}ms ago`
			);
		}

		return healthy;
	}

	/**
	 * Calculates the worse price to execute at (used for auction end when derisking)
	 * @param oracle for asset we trading
	 * @param direction of trade
	 * @returns
	 */
	private calculateOrderLimitPrice(
		price: BN,
		direction: PositionDirection
	): BN {
		const slippageBN = new BN(
			(this.liquidatorConfig.maxSlippageBps! / BPS_PRECISION) *
				PERCENTAGE_PRECISION.toNumber()
		);
		if (isVariant(direction, 'long')) {
			return price
				.mul(PERCENTAGE_PRECISION.add(slippageBN))
				.div(PERCENTAGE_PRECISION);
		} else {
			return price
				.mul(PERCENTAGE_PRECISION.sub(slippageBN))
				.div(PERCENTAGE_PRECISION);
		}
	}

	/**
	 * Calcualtes the auctionStart price when derisking (the best price we want to execute at)
	 * @param oracle for asset we trading
	 * @param direction of trade
	 * @returns
	 */
	private calculateDeriskAuctionStartPrice(
		oracle: OraclePriceData,
		direction: PositionDirection
	): BN {
		let auctionStartPrice: BN;
		if (isVariant(direction, 'long')) {
			auctionStartPrice = oracle.price.sub(oracle.confidence);
		} else {
			auctionStartPrice = oracle.price.add(oracle.confidence);
		}
		return auctionStartPrice;
	}

	private async driftSpotTrade(
		orderDirection: PositionDirection,
		marketIndex: number,
		tokenAmount: BN,
		limitPrice: BN,
		subAccountId: number
	) {
		const position = this.driftClient.getSpotPosition(marketIndex);
		if (!position) {
			return;
		}
		const positionNetOpenOrders = tokenAmount.gt(ZERO)
			? tokenAmount.add(position.openAsks)
			: tokenAmount.add(position.openBids);

		const spotMarket = this.driftClient.getSpotMarketAccount(marketIndex)!;
		const standardizedTokenAmount = standardizeBaseAssetAmount(
			positionNetOpenOrders,
			spotMarket.orderStepSize
		);

		// check if open orders already net out with current position before placing new order
		if (standardizedTokenAmount.eq(ZERO)) {
			logger.info(
				`Skipping drift spot trade, would have traded 0. ${tokenAmount.toString()} -> ${positionNetOpenOrders.toString()}-> ${standardizedTokenAmount.toString()}`
			);
			return;
		}

		const oracle = this.driftClient.getOracleDataForSpotMarket(marketIndex);
		const auctionStartPrice = this.calculateDeriskAuctionStartPrice(
			oracle,
			orderDirection
		);

		const cancelOrdersIx = await this.driftClient.getCancelOrdersIx(
			MarketType.SPOT,
			marketIndex,
			orderDirection,
			subAccountId
		);
		const placeOrderIx = await this.driftClient.getPlaceSpotOrderIx(
			getLimitOrderParams({
				marketIndex: marketIndex,
				direction: orderDirection,
				baseAssetAmount: standardizedTokenAmount,
				reduceOnly: true,
				price: limitPrice,
				auctionDuration: this.liquidatorConfig.deriskAuctionDurationSlots!,
				auctionStartPrice,
				auctionEndPrice: limitPrice,
			}),
			subAccountId
		);

		const simResult = await this.buildVersionedTransactionWithSimulatedCus(
			[cancelOrdersIx, placeOrderIx],
			[this.driftLookupTables!],
			Math.floor(this.priorityFeeSubscriber.getCustomStrategyResult())
		);
		if (simResult.simError !== null) {
			logger.error(
				`Error trying to close spot position for market ${marketIndex}, subaccount start ${subAccountId}, simError: ${JSON.stringify(
					simResult.simError
				)}`
			);
		} else {
			const resp = await this.driftClient.txSender.sendVersionedTransaction(
				simResult.tx,
				undefined,
				this.driftClient.opts
			);
			logger.info(
				`Sent derisk placeSpotOrder tx for on market ${marketIndex} tx: ${resp.txSig} `
			);
		}
	}

	private async cancelOpenOrdersForSpotMarket(
		marketIndex: number,
		subAccountId: number
	) {
		const cancelOrdersIx = await this.driftClient.getCancelOrdersIx(
			MarketType.SPOT,
			marketIndex,
			null,
			subAccountId
		);
		const simResult = await this.buildVersionedTransactionWithSimulatedCus(
			[cancelOrdersIx],
			[this.driftLookupTables!],
			Math.floor(this.priorityFeeSubscriber.getCustomStrategyResult())
		);
		if (simResult.simError !== null) {
			logger.error(
				`Error trying to close spot orders for market ${marketIndex}, subaccount start ${subAccountId}, simError: ${JSON.stringify(
					simResult.simError
				)}`
			);
		} else {
			const resp = await this.driftClient.txSender.sendVersionedTransaction(
				simResult.tx,
				undefined,
				this.driftClient.opts
			);
			logger.info(
				`Sent derisk placeSpotOrder tx for on market ${marketIndex} tx: ${resp.txSig} `
			);
		}
	}

	private async jupiterSpotSwap(
		orderDirection: PositionDirection,
		spotMarketIndex: number,
		quote: QuoteResponse,
		slippageBps: number,
		subAccountId: number
	) {
		let outMarketIndex: number;
		let inMarketIndex: number;
		if (isVariant(orderDirection, 'long')) {
			// sell USDC, buy spotMarketIndex
			inMarketIndex = 0;
			outMarketIndex = spotMarketIndex;
		} else {
			// sell spotMarketIndex, buy USDC
			inMarketIndex = spotMarketIndex;
			outMarketIndex = 0;
		}

		const swapIx = await this.driftClient.getJupiterSwapIxV6({
			jupiterClient: this.jupiterClient!,
			outMarketIndex,
			inMarketIndex,
			amount: new BN(quote.inAmount),
			quote,
			slippageBps,
			userAccountPublicKey: await this.driftClient.getUserAccountPublicKey(
				subAccountId
			),
		});
		swapIx.ixs.unshift(
			ComputeBudgetProgram.setComputeUnitPrice({
				microLamports: Math.floor(
					this.priorityFeeSubscriber!.getCustomStrategyResult()
				),
			})
		);
		swapIx.ixs.unshift(
			ComputeBudgetProgram.setComputeUnitLimit({
				units: 1_400_000,
			})
		);
		const lookupTables = [...swapIx.lookupTables, this.driftLookupTables!];
		if (this.driftSpotLookupTables) {
			lookupTables.push(this.driftSpotLookupTables);
		}

		const simResult = await this.buildVersionedTransactionWithSimulatedCus(
			swapIx.ixs,
			lookupTables,
			Math.floor(this.priorityFeeSubscriber.getCustomStrategyResult())
		);
		if (simResult.simError !== null) {
			logger.error(
				`Error trying to ${getVariant(
					orderDirection
				)} spot position for market ${spotMarketIndex} on jupiter, subaccount start ${subAccountId}, simError: ${JSON.stringify(
					simResult.simError
				)}`
			);
		} else {
			const resp = await this.driftClient.txSender.sendVersionedTransaction(
				simResult.tx,
				undefined,
				this.driftClient.opts
			);

			logger.info(
				`closed spot position for market ${spotMarketIndex.toString()} on subaccount ${subAccountId}: ${
					resp.txSig
				} `
			);
		}
	}

	private getOrderParamsForPerpDerisk(
		subaccountId: number,
		position: PerpPosition,
		dlob: DLOB
	): OptionalOrderParams | undefined {
		const nowSec = Math.floor(Date.now() / 1000);

		let baseAssetAmount: BN;
		if (this.useTwap('perp')) {
			let twapProgress = this.twapExecutionProgresses!.get(
				this.getTwapProgressKey(
					MarketType.PERP,
					subaccountId,
					position.marketIndex
				)
			);
			if (!twapProgress) {
				twapProgress = new TwapExecutionProgress({
					currentPosition: new BN(0),
					targetPosition: new BN(0), // target positions to close
					overallDurationSec: this.liquidatorConfig.twapDurationSec!,
					startTimeSec: nowSec,
				});
				this.twapExecutionProgresses!.set(
					this.getTwapProgressKey(
						MarketType.PERP,
						subaccountId,
						position.marketIndex
					),
					twapProgress
				);
			}
			twapProgress.updateProgress(position.baseAssetAmount, nowSec);
			baseAssetAmount = twapProgress.getExecutionSlice(nowSec);
			twapProgress.updateExecution(nowSec);
			logger.info(
				`twap progress for PERP subaccount ${subaccountId} for market ${
					position.marketIndex
				}, tradeSize: ${baseAssetAmount.toString()}, amountRemaining: ${twapProgress
					.getAmountRemaining()
					.toString()}`
			);
		} else {
			baseAssetAmount = position.baseAssetAmount;
		}

		const positionPlusOpenOrders = baseAssetAmount.gt(ZERO)
			? baseAssetAmount.add(position.openAsks)
			: baseAssetAmount.add(position.openBids);

		// check if open orders already net out with current position before placing new order
		if (baseAssetAmount.gt(ZERO) && positionPlusOpenOrders.lte(ZERO)) {
			logger.info(
				`already have open orders on subaccount ${subaccountId} for market ${position.marketIndex}, skipping closing`
			);
			return undefined;
		}

		if (baseAssetAmount.lt(ZERO) && positionPlusOpenOrders.gte(ZERO)) {
			logger.info(
				`already have open orders on subaccount ${subaccountId} for market ${position.marketIndex}, skipping`
			);
			return undefined;
		}

		baseAssetAmount = standardizeBaseAssetAmount(
			baseAssetAmount,
			this.driftClient.getPerpMarketAccount(position.marketIndex)!.amm
				.orderStepSize
		);

		if (baseAssetAmount.eq(ZERO)) {
			return undefined;
		}

		const oracle = this.driftClient.getOracleDataForPerpMarket(
			position.marketIndex
		);
		const direction = findDirectionToClose(position);
		let entryPrice;
		let bestPrice;
		try {
			({ entryPrice, bestPrice } = calculateEstimatedPerpEntryPrice(
				'base',
				baseAssetAmount.abs(),
				direction,
				this.driftClient.getPerpMarketAccount(position.marketIndex)!,
				oracle,
				dlob,
				this.userMap.getSlot()
			));
		} catch (e) {
			logger.error(
				`Failed to calculate estimated perp entry price on market: ${
					position.marketIndex
				}, amt: ${baseAssetAmount.toString()}, ${getVariant(direction)}`
			);
			throw e;
		}
		const limitPrice = this.calculateOrderLimitPrice(entryPrice, direction);
		const { auctionStartPrice, auctionEndPrice, oraclePriceOffset } =
			deriveOracleAuctionParams({
				direction,
				oraclePrice: oracle.price,
				auctionStartPrice: bestPrice,
				auctionEndPrice: limitPrice,
				limitPrice,
			});

		return getOrderParams({
			orderType: OrderType.ORACLE,
			direction,
			baseAssetAmount,
			reduceOnly: true,
			marketIndex: position.marketIndex,
			auctionDuration: this.liquidatorConfig.deriskAuctionDurationSlots!,
			auctionStartPrice,
			auctionEndPrice,
			oraclePriceOffset,
		});
	}

	private async deriskPerpPositions(userAccount: UserAccount, dlob: DLOB) {
		for (const position of userAccount.perpPositions) {
			const perpMarket = this.driftClient.getPerpMarketAccount(
				position.marketIndex
			)!;
			if (!position.baseAssetAmount.isZero()) {
				if (position.baseAssetAmount.abs().lt(perpMarket.amm.minOrderSize)) {
					continue;
				}

				const orderParams = this.getOrderParamsForPerpDerisk(
					userAccount.subAccountId,
					position,
					dlob
				);
				if (orderParams === undefined) {
					continue;
				}
				const cancelOrdersIx = await this.driftClient.getCancelOrdersIx(
					MarketType.PERP,
					position.marketIndex,
					orderParams.direction,
					userAccount.subAccountId
				);
				const placeOrderIx = await this.driftClient.getPlacePerpOrderIx(
					orderParams,
					userAccount.subAccountId
				);

				const simResult = await this.buildVersionedTransactionWithSimulatedCus(
					[cancelOrdersIx, placeOrderIx],
					[this.driftLookupTables!],
					Math.floor(this.priorityFeeSubscriber.getCustomStrategyResult())
				);

				if (simResult.simError !== null) {
					logger.error(
						`Error error in placePerpOrder in market: ${
							position.marketIndex
						}, simError: ${JSON.stringify(simResult.simError)}`
					);

					const errorCode = handleSimResultError(
						simResult,
						errorCodesToSuppress,
						`${this.name}: deriskPerpPositions`
					);

					if (errorCode && !errorCodesToSuppress.includes(errorCode)) {
						const msg = `[${
							this.name
						}]: :x: error in placePerpOrder in market ${
							position.marketIndex
						}: \n${simResult.simTxLogs ? simResult.simTxLogs.join('\n') : ''}`;
						logger.error(msg);
						webhookMessage(msg);
					}
				} else {
					const resp = await this.driftClient.txSender.sendVersionedTransaction(
						simResult.tx,
						undefined,
						this.driftClient.opts
					);
					logger.info(
						`Sent deriskPerpPosition tx on market ${position.marketIndex}: ${resp.txSig} `
					);
				}
			} else if (position.quoteAssetAmount.lt(ZERO)) {
				const userAccountPubkey =
					await this.driftClient.getUserAccountPublicKey(
						userAccount.subAccountId
					);
				logger.info(
					`Settling negative perp pnl for ${userAccountPubkey.toBase58()} on market ${
						position.marketIndex
					}`
				);
				const ix = await this.driftClient.getSettlePNLsIxs(
					[
						{
							settleeUserAccountPublicKey: userAccountPubkey,
							settleeUserAccount: userAccount,
						},
					],
					[position.marketIndex]
				);

				const simResult = await this.buildVersionedTransactionWithSimulatedCus(
					ix,
					[this.driftLookupTables!],
					Math.floor(this.priorityFeeSubscriber.getCustomStrategyResult())
				);
				if (simResult.simError !== null) {
					logger.error(
						`Error in SettlePnl for userAccount ${userAccountPubkey.toBase58()} on market ${
							position.marketIndex
						}, simError: ${JSON.stringify(simResult.simError)}`
					);
					const errorCode = handleSimResultError(
						simResult,
						errorCodesToSuppress,
						`${this.name}: settleNegativePnl`
					);
					if (errorCode && !errorCodesToSuppress.includes(errorCode)) {
						webhookMessage(
							`[${
								this.name
							}]: :x: error in settlePnl for userAccount ${userAccountPubkey.toBase58()} on market ${
								position.marketIndex
							}: \n${simResult.simTxLogs ? simResult.simTxLogs.join('\n') : ''}`
						);
					}
				} else {
					const resp = await this.driftClient.txSender.sendVersionedTransaction(
						simResult.tx,
						undefined,
						this.driftClient.opts
					);
					logger.info(
						`Sent settlePnl (negative pnl) tx for ${userAccountPubkey.toBase58()} on market ${
							position.marketIndex
						} tx: ${resp.txSig} `
					);
				}
			} else if (position.quoteAssetAmount.gt(ZERO)) {
				const availablePnl = calculateMarketAvailablePNL(
					this.driftClient.getPerpMarketAccount(position.marketIndex)!,
					this.driftClient.getQuoteSpotMarketAccount()
				);

				if (availablePnl.gt(ZERO)) {
					const userAccountPubkey =
						await this.driftClient.getUserAccountPublicKey(
							userAccount.subAccountId
						);
					logger.info(
						`Settling positive perp pnl for ${userAccountPubkey.toBase58()} on market ${
							position.marketIndex
						}`
					);
					const ix = await this.driftClient.getSettlePNLsIxs(
						[
							{
								settleeUserAccountPublicKey: userAccountPubkey,
								settleeUserAccount: userAccount,
							},
						],
						[position.marketIndex]
					);
					const simResult =
						await this.buildVersionedTransactionWithSimulatedCus(
							ix,
							[this.driftLookupTables!],
							Math.floor(this.priorityFeeSubscriber.getCustomStrategyResult())
						);
					if (simResult.simError !== null) {
						logger.error(
							`Error in SettlePnl for userAccount ${userAccountPubkey.toBase58()} on market ${
								position.marketIndex
							}, simError: ${JSON.stringify(simResult.simError)}`
						);
						const errorCode = handleSimResultError(
							simResult,
							errorCodesToSuppress,
							`${this.name}: settlePositivePnl`
						);
						if (errorCode && !errorCodesToSuppress.includes(errorCode)) {
							const msg = `[${
								this.name
							}]: :x: error in settlePnl for userAccount ${userAccountPubkey.toBase58()} on market ${
								position.marketIndex
							}: \n${
								simResult.simTxLogs ? simResult.simTxLogs.join('\n') : ''
							}`;
							logger.error(msg);
							webhookMessage(msg);
						}
					} else {
						const resp =
							await this.driftClient.txSender.sendVersionedTransaction(
								simResult.tx,
								undefined,
								this.driftClient.opts
							);
						logger.info(
							`Sent settlePnl (positive pnl) tx for ${userAccountPubkey.toBase58()} on market ${
								position.marketIndex
							} tx: ${resp.txSig} `
						);
					}
				}
			}
		}
	}

	private async determineBestSpotSwapRoute(
		spotMarketIndex: number,
		orderDirection: PositionDirection,
		baseAmountIn: BN,
		slippageBps: number
	): Promise<QuoteResponse | undefined> {
		if (!this.jupiterClient) {
			return undefined;
		}
		const oraclePriceData =
			this.driftClient.getOracleDataForSpotMarket(spotMarketIndex);
		const dlob = await this.userMap!.getDLOB(oraclePriceData.slot.toNumber());
		if (!dlob) {
			logger.error('failed to load DLOB');
		}

		let dlobFillQuoteAmount: BN | undefined;
		if (dlob) {
			dlobFillQuoteAmount = dlob.estimateFillWithExactBaseAmount({
				marketIndex: spotMarketIndex,
				marketType: MarketType.SPOT,
				baseAmount: baseAmountIn,
				orderDirection,
				slot: oraclePriceData.slot.toNumber(),
				oraclePriceData,
			});
		}

		let outMarket: SpotMarketAccount | undefined;
		let inMarket: SpotMarketAccount | undefined;
		let amountIn: BN | undefined;
		const spotMarketIsSolLst = isSolLstToken(spotMarketIndex);
		if (isVariant(orderDirection, 'long')) {
			if (spotMarketIsSolLst) {
				// sell SOL, buy the LST
				inMarket = this.driftClient.getSpotMarketAccount(1);
				outMarket = this.driftClient.getSpotMarketAccount(spotMarketIndex);
			} else {
				// sell USDC, buy spotMarketIndex
				inMarket = this.driftClient.getSpotMarketAccount(0);
				outMarket = this.driftClient.getSpotMarketAccount(spotMarketIndex);
			}
			if (!inMarket || !outMarket) {
				logger.error('failed to get spot markets');
				return undefined;
			}
			const inPrecision = TEN.pow(new BN(inMarket.decimals));
			const outPrecision = TEN.pow(new BN(outMarket.decimals));
			amountIn = oraclePriceData.price
				.mul(baseAmountIn)
				.mul(inPrecision)
				.div(PRICE_PRECISION.mul(outPrecision));
		} else {
			if (spotMarketIsSolLst) {
				// sell spotMarketIndex, buy SOL
				inMarket = this.driftClient.getSpotMarketAccount(spotMarketIndex);
				outMarket = this.driftClient.getSpotMarketAccount(1);
			} else {
				// sell spotMarketIndex, buy USDC
				inMarket = this.driftClient.getSpotMarketAccount(spotMarketIndex);
				outMarket = this.driftClient.getSpotMarketAccount(0);
			}
			amountIn = baseAmountIn;
		}

		if (!inMarket || !outMarket) {
			logger.error('failed to get spot markets');
			return undefined;
		}

		logger.info(
			`Getting jupiter quote, ${getVariant(
				orderDirection
			)} amount: ${amountIn.toString()}, inMarketIdx: ${
				inMarket.marketIndex
			}, outMarketIdx: ${outMarket.marketIndex}, slippageBps: ${slippageBps}`
		);
		let quote: QuoteResponse | undefined;
		try {
			quote = await this.jupiterClient.getQuote({
				inputMint: inMarket.mint,
				outputMint: outMarket.mint,
				amount: amountIn.abs(),
				slippageBps: slippageBps,
				maxAccounts: 10,
				excludeDexes: ['Raydium CLMM'],
			});
		} catch (e) {
			logger.error(`Error getting Jupiter quote: ${(e as Error).message}`);
			console.error(e);
			return undefined;
		}

		if (!quote) {
			return undefined;
		}

		console.log('Jupiter quote:');
		console.log(JSON.stringify(quote, null, 2));

		if (!quote.routePlan || quote.routePlan.length === 0) {
			logger.info(`Found no jupiter route`);
			return undefined;
		}

		if (isVariant(orderDirection, 'long')) {
			// buying spotMarketIndex, want min in
			const jupAmountIn = new BN(quote.inAmount);

			if (
				dlobFillQuoteAmount?.gt(ZERO) &&
				dlobFillQuoteAmount?.lt(jupAmountIn)
			) {
				logger.info(
					`Want to long spot market ${spotMarketIndex}, dlob fill amount ${dlobFillQuoteAmount} < jup amount in ${jupAmountIn}, dont trade on jup`
				);
				return undefined;
			} else {
				return quote;
			}
		} else {
			// selling spotMarketIndex, want max out
			const jupAmountOut = new BN(quote.outAmount);
			if (dlobFillQuoteAmount?.gt(jupAmountOut)) {
				logger.info(
					`Want to short spot market ${spotMarketIndex}, dlob fill amount ${dlobFillQuoteAmount} > jup amount out ${jupAmountOut}, dont trade on jup`
				);
				return undefined;
			} else {
				return quote;
			}
		}
	}

	private getOrderParamsForSpotDerisk(
		subaccountId: number,
		position: SpotPosition
	):
		| { tokenAmount: BN; limitPrice: BN; direction: PositionDirection }
		| undefined {
		const spotMarket = this.driftClient.getSpotMarketAccount(
			position.marketIndex
		);
		if (!spotMarket) {
			logger.error(
				`failed to get spot market account for market ${position.marketIndex}`
			);
			return undefined;
		}

		const nowSec = Math.floor(Date.now() / 1000);
		let tokenAmount = getSignedTokenAmount(
			getTokenAmount(position.scaledBalance, spotMarket, position.balanceType),
			position.balanceType
		);

		if (tokenAmount.abs().lt(spotMarket.minOrderSize)) {
			return undefined;
		}

		if (this.useTwap('spot')) {
			let twapProgress = this.twapExecutionProgresses!.get(
				this.getTwapProgressKey(
					MarketType.SPOT,
					subaccountId,
					position.marketIndex
				)
			);
			if (!twapProgress) {
				twapProgress = new TwapExecutionProgress({
					currentPosition: new BN(0),
					targetPosition: new BN(0), // target positions to close
					overallDurationSec: this.liquidatorConfig.twapDurationSec!,
					startTimeSec: nowSec,
				});
				this.twapExecutionProgresses!.set(
					this.getTwapProgressKey(
						MarketType.SPOT,
						subaccountId,
						position.marketIndex
					),
					twapProgress
				);
			}
			twapProgress.updateProgress(tokenAmount, nowSec);
			tokenAmount = twapProgress.getExecutionSlice(nowSec);
			twapProgress.updateExecution(nowSec);
			logger.info(
				`twap progress for SPOT subaccount ${subaccountId} for market ${
					position.marketIndex
				}, tradeSize: ${tokenAmount.toString()}, amountRemaining: ${twapProgress
					.getAmountRemaining()
					.toString()}`
			);
		}

		let direction: PositionDirection = PositionDirection.LONG;
		if (isVariant(position.balanceType, 'deposit')) {
			direction = PositionDirection.SHORT;
		}

		const oracle = this.driftClient.getOracleDataForSpotMarket(
			position.marketIndex
		);
		const limitPrice = this.calculateOrderLimitPrice(oracle.price, direction);

		return {
			tokenAmount,
			limitPrice,
			direction,
		};
	}

	/**
	 * Withdraws dust for a spot position
	 *
	 * @param userAccount
	 * @param position
	 * @returns true if tx was sent to withdraw dust, otherwise false
	 */
	private async withdrawDust(
		userAccount: UserAccount,
		position: SpotPosition
	): Promise<boolean> {
		if (this.liquidatorConfig.spotDustValueThresholdBN === undefined) {
			return false;
		}

		// can only withdraw dust for now
		// TODO: deposit to close dust borrows
		if (isVariant(position.balanceType, 'borrow')) {
			return false;
		}
		const spotMarket = this.driftClient.getSpotMarketAccount(
			position.marketIndex
		);
		if (!spotMarket) {
			throw new Error(
				`failed to get spot market account for market ${position.marketIndex}`
			);
		}
		const oracle = this.driftClient.getOracleDataForSpotMarket(
			position.marketIndex
		);
		const tokenAmount = getTokenAmount(
			position.scaledBalance,
			spotMarket,
			position.balanceType
		);
		const spotPositionValue = getTokenValue(
			tokenAmount,
			spotMarket.decimals,
			oracle
		);
		if (spotPositionValue.lte(this.liquidatorConfig.spotDustValueThresholdBN)) {
			let userTokenAccount = await getAssociatedTokenAddress(
				spotMarket.mint,
				userAccount.authority
			);
			const ixs: TransactionInstruction[] = [];
			const isSolWithdraw = spotMarket.mint.equals(WRAPPED_SOL_MINT);
			if (isSolWithdraw) {
				const { ixs: startIxs, pubkey } =
					await this.driftClient.getWrappedSolAccountCreationIxs(
						tokenAmount,
						true
					);
				ixs.push(...startIxs);
				userTokenAccount = pubkey;
			} else {
				const accountExists = await checkIfAccountExists(
					this.driftClient.connection,
					userTokenAccount
				);

				if (!accountExists) {
					ixs.push(
						createAssociatedTokenAccountInstruction(
							this.driftClient.wallet.publicKey,
							userTokenAccount,
							this.driftClient.wallet.publicKey,
							spotMarket.mint,
							this.driftClient.getTokenProgramForSpotMarket(spotMarket)
						)
					);
				}
			}

			ixs.push(
				await this.driftClient.getWithdrawIx(
					tokenAmount,
					position.marketIndex,
					userTokenAccount,
					undefined,
					userAccount.subAccountId
				)
			);
			if (isSolWithdraw) {
				ixs.push(
					createCloseAccountInstruction(
						userTokenAccount,
						userAccount.authority,
						userAccount.authority,
						[]
					)
				);
			}

			const simResult = await this.buildVersionedTransactionWithSimulatedCus(
				ixs,
				[this.driftLookupTables!],
				Math.floor(this.priorityFeeSubscriber.getCustomStrategyResult())
			);
			if (simResult.simError !== null) {
				logger.error(
					`Error trying to withdraw spot dust for market ${
						position.marketIndex
					}, subaccount start ${
						userAccount.subAccountId
					}, simError: ${JSON.stringify(simResult.simError)}`
				);
				return true;
			} else {
				const resp = await this.driftClient.txSender.sendVersionedTransaction(
					simResult.tx,
					undefined,
					this.driftClient.opts
				);
				logger.info(
					`Sent withdraw dust on market ${position.marketIndex} tx: ${resp.txSig} `
				);
				return true;
			}
		}

		return false;
	}

	private async deriskSpotPositions(userAccount: UserAccount) {
		for (const position of userAccount.spotPositions) {
			if (position.scaledBalance.eq(ZERO) || position.marketIndex === 0) {
				if (position.openOrders != 0) {
					await this.cancelOpenOrdersForSpotMarket(
						position.marketIndex,
						userAccount.subAccountId
					);
				}
				continue;
			}

			const dustWithdrawn = await this.withdrawDust(userAccount, position);
			if (dustWithdrawn) {
				continue;
			}

			const orderParams = this.getOrderParamsForSpotDerisk(
				userAccount.subAccountId,
				position
			);
			if (orderParams === undefined) {
				continue;
			}

			const slippageBps = this.liquidatorConfig.maxSlippageBps!;
			const jupQuote = await this.determineBestSpotSwapRoute(
				position.marketIndex,
				orderParams.direction,
				orderParams.tokenAmount,
				slippageBps
			);
			if (!jupQuote) {
				await this.driftSpotTrade(
					orderParams.direction,
					position.marketIndex,
					orderParams.tokenAmount,
					orderParams.limitPrice,
					userAccount.subAccountId
				);
			} else {
				await this.jupiterSpotSwap(
					orderParams.direction,
					position.marketIndex,
					jupQuote,
					slippageBps,
					userAccount.subAccountId
				);
			}
		}
	}

	private async deriskForSubaccount(subaccountId: number, dlob: DLOB) {
		this.driftClient.switchActiveUser(subaccountId, this.driftClient.authority);
		const userAccount = this.driftClient.getUserAccount(subaccountId);
		if (!userAccount) {
			logger.error('failed to get user account');
			return;
		}
		if (userAccount.subAccountId !== subaccountId) {
			logger.error('failed to switch user account');
			return;
		}

		// need to await, otherwise driftClient.activeUserAccount will get rugged on next iter
		await this.deriskPerpPositions(userAccount, dlob);
		await this.deriskSpotPositions(userAccount);
	}

	/**
	 * attempts to acquire the mutex for derisking. If it is already acquired, it returns false
	 */
	private acquireMutex(mutex: Uint8Array): boolean {
		return Atomics.compareExchange(mutex, 0, 0, 1) === 0;
	}

	/**
	 * releases the mutex.
	 * Warning: this should only be called after acquireMutex() returns true
	 */
	private releaseMutex(mutex: Uint8Array) {
		Atomics.store(mutex, 0, 0);
	}

	/**
	 * attempts to close out any open positions on this account. It starts by cancelling any open orders
	 */
	private async derisk() {
		if (!this.acquireMutex(this.deriskMutex)) {
			return;
		}

		const dlob = await this.userMap.getDLOB(this.userMap.getSlot());
		try {
			for (const subAccountId of this.allSubaccounts) {
				await this.deriskForSubaccount(subAccountId, dlob);
			}
		} finally {
			this.releaseMutex(this.deriskMutex);
		}
	}

	private calculateBaseAmountToLiquidate(liquidateePosition: PerpPosition): BN {
		const liquidatorUser = this.getLiquidatorUserForPerpMarket(
			liquidateePosition.marketIndex
		);
		if (!liquidatorUser) {
			return ZERO;
		}

		const oraclePrice = this.driftClient.getOracleDataForPerpMarket(
			liquidateePosition.marketIndex
		).price;
		const collateralToSpend = liquidatorUser
			.getFreeCollateral()
			.mul(PRICE_PRECISION)
			.mul(this.maxPositionTakeoverPctOfCollateralNum)
			.mul(BASE_PRECISION);
		const baseAssetAmountToLiquidate = collateralToSpend.div(
			oraclePrice
				.mul(QUOTE_PRECISION)
				.mul(this.maxPositionTakeoverPctOfCollateralDenom)
		);

		if (
			baseAssetAmountToLiquidate.gt(liquidateePosition.baseAssetAmount.abs())
		) {
			return liquidateePosition.baseAssetAmount.abs();
		} else {
			return baseAssetAmountToLiquidate;
		}
	}

	/**
	 * Find the user perp position with the largest loss, resolve bankruptcy on this market.
	 *
	 * @param chUserToCheck
	 * @returns
	 */
	private findPerpBankruptingMarkets(chUserToCheck: User): Array<number> {
		const bankruptMarketIndices: Array<number> = [];

		for (const market of this.driftClient.getPerpMarketAccounts()) {
			const position = chUserToCheck.getPerpPosition(market.marketIndex);
			if (!position || position.quoteAssetAmount.gte(ZERO)) {
				// invalid position to liquidate
				continue;
			}
			bankruptMarketIndices.push(position.marketIndex);
		}

		return bankruptMarketIndices;
	}

	/**
	 * Find the user spot position with the largest loss, resolve bankruptcy on this market.
	 *
	 * @param chUserToCheck
	 * @returns
	 */
	private findSpotBankruptingMarkets(chUserToCheck: User): Array<number> {
		const bankruptMarketIndices: Array<number> = [];

		for (const market of this.driftClient.getSpotMarketAccounts()) {
			const position = chUserToCheck.getSpotPosition(market.marketIndex);
			if (!position) {
				continue;
			}
			if (!isVariant(position.balanceType, 'borrow')) {
				// not possible to resolve non-borrow markets
				continue;
			}
			if (position.scaledBalance.lte(ZERO)) {
				// invalid borrow
				continue;
			}
			bankruptMarketIndices.push(position.marketIndex);
		}

		return bankruptMarketIndices;
	}

	private async tryResolveBankruptUser(user: User) {
		const userAcc = user.getUserAccount();
		const userKey = user.getUserAccountPublicKey();

		// find out whether the user is perp-bankrupt or spot-bankrupt
		const bankruptPerpMarkets = this.findPerpBankruptingMarkets(user);
		const bankruptSpotMarkets = this.findSpotBankruptingMarkets(user);

		logger.info(
			`User ${userKey.toBase58()} is bankrupt in perpMarkets: ${bankruptPerpMarkets} and spotMarkets: ${bankruptSpotMarkets} `
		);

		// resolve bankrupt markets
		for (const perpIdx of bankruptPerpMarkets) {
			logger.info(
				`Resolving perp market for userAcc: ${userKey.toBase58()}, marketIndex: ${perpIdx} `
			);
			if (this.liquidatorConfig.notifyOnLiquidation) {
				webhookMessage(
					`[${
						this.name
					}]: Resolving perp market for userAcc: ${userKey.toBase58()}, marketIndex: ${perpIdx} `
				);
			}
			const ix = await this.driftClient.getResolvePerpBankruptcyIx(
				userKey,
				userAcc,
				perpIdx
			);
			const simResult = await this.buildVersionedTransactionWithSimulatedCus(
				[ix],
				[this.driftLookupTables!],
				Math.floor(this.priorityFeeSubscriber.getCustomStrategyResult())
			);
			if (simResult.simError !== null) {
				logger.error(
					`Error in resolveBankruptcy for userAccount ${userKey.toBase58()} on market ${perpIdx}, simError: ${JSON.stringify(
						simResult.simError
					)}`
				);

				const errorCode = handleSimResultError(
					simResult,
					errorCodesToSuppress,
					`${this.name}: resolveBankruptcy`
				);
				if (errorCode && !errorCodesToSuppress.includes(errorCode)) {
					const msg = `[${
						this.name
					}]: :x: error in resolveBankruptcy for userAccount ${userKey.toBase58()}: \n${
						simResult.simTxLogs ? simResult.simTxLogs.join('\n') : ''
					}`;
					logger.error(msg);
					webhookMessage(msg);
				}
			} else {
				const resp = await this.driftClient.txSender.sendVersionedTransaction(
					simResult.tx,
					undefined,
					this.driftClient.opts
				);
				logger.info(
					`Sent resolveBankruptcy tx for ${userKey.toBase58()} in perp market ${perpIdx} tx: ${
						resp.txSig
					} `
				);
			}
		}

		for (const spotIdx of bankruptSpotMarkets) {
			logger.info(
				`Resolving spot market for userAcc: ${userKey.toBase58()}, marketIndex: ${spotIdx} `
			);
			if (this.liquidatorConfig.notifyOnLiquidation) {
				webhookMessage(
					`[${
						this.name
					}]: Resolving spot market for userAcc: ${userKey.toBase58()}, marketIndex: ${spotIdx} `
				);
			}

			const ix = await this.driftClient.getResolveSpotBankruptcyIx(
				userKey,
				userAcc,
				spotIdx
			);
			const simResult = await this.buildVersionedTransactionWithSimulatedCus(
				[ix],
				[this.driftLookupTables!],
				Math.floor(this.priorityFeeSubscriber.getCustomStrategyResult())
			);
			if (simResult.simError !== null) {
				logger.error(
					`Error in resolveBankruptcy for userAccount ${userKey.toBase58()} in spot market ${spotIdx}, simError: ${JSON.stringify(
						simResult.simError
					)}`
				);

				const errorCode = handleSimResultError(
					simResult,
					errorCodesToSuppress,
					`${this.name}: resolveBankruptcy`
				);
				if (errorCode && !errorCodesToSuppress.includes(errorCode)) {
					const msg = `[${
						this.name
					}]: :x: error in resolveBankruptcy for userAccount ${userKey.toBase58()}: \n${
						simResult.simTxLogs ? simResult.simTxLogs.join('\n') : ''
					}`;
					logger.error(msg);
					webhookMessage(msg);
				}
			} else {
				const resp = await this.driftClient.txSender.sendVersionedTransaction(
					simResult.tx,
					undefined,
					this.driftClient.opts
				);
				logger.info(
					`Sent resolveBankruptcy tx for ${userKey.toBase58()} in spot market ${spotIdx} tx: ${
						resp.txSig
					} `
				);
			}
		}
	}

	private hasCollateralToLiquidate(subAccountId: number): boolean {
		const currUser = this.driftClient.getUser(subAccountId);
		const freeCollateral = currUser.getFreeCollateral('Initial');
		const subAccountValue = new BN(
			calculateAccountValueUsd(currUser) * QUOTE_PRECISION.toNumber()
		);
		const minFreeCollateral = subAccountValue
			.mul(this.maxPositionTakeoverPctOfCollateralNum)
			.div(this.maxPositionTakeoverPctOfCollateralDenom);
		return freeCollateral.gte(minFreeCollateral);
	}

	private findBestSpotPosition(
		liquidateeUser: User,
		spotPositions: SpotPosition[],
		isBorrow: boolean,
		positionTakerOverPctNumerator: BN,
		positionTakerOverPctDenominator: BN
	): {
		bestIndex: number;
		bestAmount: BN;
		indexWithMaxAssets: number;
		indexWithOpenOrders: number;
	} {
		let bestIndex = -1;
		let bestAmount = ZERO;
		let currentAstWeight = 0;
		let currentLibWeight = Number.MAX_VALUE;
		let indexWithMaxAssets = -1;
		let maxAssets = new BN(-1);
		let indexWithOpenOrders = -1;

		for (const position of spotPositions) {
			const market = this.driftClient.getSpotMarketAccount(
				position.marketIndex
			);
			if (!market) {
				throw new Error('No spot market found, drift client misconfigured');
				continue;
			}

			if (position.scaledBalance.eq(ZERO)) {
				continue;
			}

			if (position.openOrders > 0) {
				indexWithOpenOrders = position.marketIndex;
			}

			const totalAssetValue = liquidateeUser.getSpotMarketAssetValue(
				position.marketIndex,
				'Maintenance',
				true,
				undefined,
				undefined
			);
			if (totalAssetValue.abs().gt(maxAssets)) {
				maxAssets = totalAssetValue.abs();
				indexWithMaxAssets = position.marketIndex;
			}

			if (
				(isBorrow && isVariant(position.balanceType, 'deposit')) ||
				(!isBorrow && isVariant(position.balanceType, 'borrow'))
			) {
				continue;
			}

			const spotMarket = this.driftClient.getSpotMarketAccount(
				position.marketIndex
			);
			if (!spotMarket) {
				logger.error(`No spot market found for ${position.marketIndex}`);
				continue;
			}

			const liquidatorUser = this.getLiquidatorUserForSpotMarket(
				position.marketIndex
			);
			if (!liquidatorUser) {
				continue;
			}

			const tokenAmount = calculateSpotTokenAmountToLiquidate(
				this.driftClient,
				liquidatorUser,
				position,
				positionTakerOverPctNumerator,
				positionTakerOverPctDenominator
			);

			if (isBorrow) {
				if (spotMarket.maintenanceLiabilityWeight < currentLibWeight) {
					bestAmount = tokenAmount;
					bestIndex = position.marketIndex;
					currentAstWeight = spotMarket.maintenanceAssetWeight;
					currentLibWeight = spotMarket.maintenanceLiabilityWeight;
				}
			} else {
				if (spotMarket.maintenanceAssetWeight > currentAstWeight) {
					bestAmount = tokenAmount;
					bestIndex = position.marketIndex;
					currentAstWeight = spotMarket.maintenanceAssetWeight;
					currentLibWeight = spotMarket.maintenanceLiabilityWeight;
				}
			}
		}

		return {
			bestIndex,
			bestAmount,
			indexWithMaxAssets: indexWithMaxAssets,
			indexWithOpenOrders,
		};
	}

	private async liqSpot(
		user: User,
		depositMarketIndexToLiq: number,
		borrowMarketIndexToLiq: number,
		subAccountToLiqSpot: number,
		amountToLiqBN: BN
	): Promise<boolean> {
		let sentTx = false;
		const ix = await this.driftClient.getLiquidateSpotIx(
			user.userAccountPublicKey,
			user.getUserAccount(),
			depositMarketIndexToLiq,
			borrowMarketIndexToLiq,
			amountToLiqBN,
			undefined,
			subAccountToLiqSpot
		);
		const simResult = await this.buildVersionedTransactionWithSimulatedCus(
			[ix],
			[this.driftLookupTables!],
			Math.floor(this.priorityFeeSubscriber.getCustomStrategyResult())
		);
		if (simResult.simError !== null) {
			logger.error(
				`Error in liquidateSpot for userAccount ${user.userAccountPublicKey.toBase58()} in spot market ${borrowMarketIndexToLiq}, simError: ${JSON.stringify(
					simResult.simError
				)}`
			);

			const errorCode = handleSimResultError(
				simResult,
				errorCodesToSuppress,
				`${this.name}: liquidateSpot`
			);
			if (errorCode && !errorCodesToSuppress.includes(errorCode)) {
				const msg = `[${
					this.name
				}]: :x: error in liquidateSpot for userAccount ${user.userAccountPublicKey.toBase58()}: \n${
					simResult.simTxLogs ? simResult.simTxLogs.join('\n') : ''
				}`;
				logger.error(msg);
				webhookMessage(msg);
			}
		} else {
			const resp = await this.driftClient.txSender.sendVersionedTransaction(
				simResult.tx,
				undefined,
				this.driftClient.opts
			);
			sentTx = true;
			logger.info(
				`Sent liquidateSpot tx for ${user.userAccountPublicKey.toBase58()} in spot market ${borrowMarketIndexToLiq} tx: ${
					resp.txSig
				} `
			);

			if (this.liquidatorConfig.notifyOnLiquidation) {
				webhookMessage(
					`[${
						this.name
					}]: liquidateBorrow for userAccount ${user.userAccountPublicKey.toBase58()} in spot market ${borrowMarketIndexToLiq} tx: ${
						resp.txSig
					} `
				);
			}
		}

		return sentTx;
	}

	private async liqBorrow(
		depositMarketIndexToLiq: number,
		borrowMarketIndexToLiq: number,
		borrowAmountToLiq: BN,
		user: User
	): Promise<boolean> {
		const borrowMarket = this.driftClient.getSpotMarketAccount(
			borrowMarketIndexToLiq
		)!;
		const spotPrecision = TEN.pow(new BN(borrowMarket.decimals));
		logger.info(
			`liqBorrow: ${user.userAccountPublicKey.toBase58()} amountToLiq: ${convertToNumber(
				borrowAmountToLiq,
				spotPrecision
			)} on market ${borrowMarketIndexToLiq}`
		);

		const subAccountToLiqSpot = this.getSubAccountIdToLiquidateSpot(
			borrowMarketIndexToLiq
		);
		if (subAccountToLiqSpot === undefined) {
			return false;
		}

		const currUser = this.driftClient.getUser(subAccountToLiqSpot);
		const oracle = this.driftClient.getOracleDataForSpotMarket(
			borrowMarketIndexToLiq
		);
		const borrowValue = getTokenValue(
			borrowAmountToLiq,
			borrowMarket.decimals,
			oracle
		);
		const subAccountValue = calculateAccountValueUsd(currUser);
		const freeCollateral = currUser.getFreeCollateral('Initial');
		const collateralAvailable = freeCollateral
			.mul(this.maxPositionTakeoverPctOfCollateralNum)
			.div(this.maxPositionTakeoverPctOfCollateralDenom);

		let amountToLiqBN = borrowAmountToLiq.muln(2); // double to make sure it clears out extra interest
		if (borrowValue.gt(collateralAvailable)) {
			amountToLiqBN = spotPrecision.mul(collateralAvailable).div(oracle.price);
		}
		logger.info(
			`Subaccount ${subAccountToLiqSpot}: BorrowValue: ${borrowValue}, accountValue: ${subAccountValue}, collateralAvailable: ${convertToNumber(
				collateralAvailable,
				QUOTE_PRECISION
			)}, amountToLiq: ${convertToNumber(amountToLiqBN, spotPrecision)}`
		);

		return await this.liqSpot(
			user,
			depositMarketIndexToLiq,
			borrowMarketIndexToLiq,
			subAccountToLiqSpot,
			amountToLiqBN
		);
	}

	private async liqPerpPnl(
		user: User,
		perpMarketAccount: PerpMarketAccount,
		usdcAccount: SpotMarketAccount,
		liquidateePosition: PerpPosition,
		depositMarketIndextoLiq: number,
		depositAmountToLiq: BN,
		borrowMarketIndextoLiq: number,
		borrowAmountToLiq: BN
	): Promise<boolean> {
		logger.info(
			`liqPerpPnl: ${user.userAccountPublicKey.toBase58()} depositAmountToLiq: ${depositAmountToLiq.toString()} (depMktIndex: ${depositMarketIndextoLiq}). borrowAmountToLiq: ${borrowAmountToLiq.toString()} (brwMktIndex: ${borrowMarketIndextoLiq}). liqeePositionQuoteAmount: ${liquidateePosition.quoteAssetAmount.toNumber()} (perpMktIdx: ${
				perpMarketAccount.marketIndex
			})`
		);
		let sentTx = false;

		if (liquidateePosition.quoteAssetAmount.gt(ZERO)) {
			const claimablePnl = calculateClaimablePnl(
				perpMarketAccount,
				usdcAccount,
				liquidateePosition,
				this.driftClient.getOracleDataForPerpMarket(
					liquidateePosition.marketIndex
				)
			);

			if (claimablePnl.gt(ZERO) && borrowMarketIndextoLiq === -1) {
				const ix = await this.driftClient.getSettlePNLsIxs(
					[
						{
							settleeUserAccountPublicKey: user.userAccountPublicKey,
							settleeUserAccount: user.getUserAccount(),
						},
					],
					[liquidateePosition.marketIndex]
				);
				const simResult = await this.buildVersionedTransactionWithSimulatedCus(
					ix,
					[this.driftLookupTables!],
					Math.floor(this.priorityFeeSubscriber.getCustomStrategyResult())
				);
				if (simResult.simError !== null) {
					logger.error(
						`Error in settlePnl for userAccount ${user.userAccountPublicKey.toBase58()} in perp market ${
							liquidateePosition.marketIndex
						}, simError: ${JSON.stringify(simResult.simError)}`
					);

					const errorCode = handleSimResultError(
						simResult,
						errorCodesToSuppress,
						`${this.name}: settlePnl`
					);
					if (errorCode && !errorCodesToSuppress.includes(errorCode)) {
						const msg = `[${
							this.name
						}]: :x: error in settlePnl for userAccount ${user.userAccountPublicKey.toBase58()}: \n${
							simResult.simTxLogs ? simResult.simTxLogs.join('\n') : ''
						}`;
						logger.error(msg);
						webhookMessage(msg);
					}
				} else {
					const resp = await this.driftClient.txSender.sendVersionedTransaction(
						simResult.tx,
						undefined,
						this.driftClient.opts
					);
					logger.info(
						`Sent settlePnl tx for ${user.userAccountPublicKey.toBase58()} in perp market ${
							liquidateePosition.marketIndex
						} tx: ${resp.txSig} `
					);
				}

				return sentTx;
			}

			let frac = new BN(100000000);
			if (claimablePnl.gt(ZERO)) {
				frac = BN.max(
					liquidateePosition.quoteAssetAmount.div(claimablePnl),
					new BN(1)
				);
			}

			if (frac.lt(new BN(100000000))) {
				const subAccountToLiqBorrow = this.getSubAccountIdToLiquidateSpot(
					borrowMarketIndextoLiq
				);
				if (subAccountToLiqBorrow === undefined) {
					return sentTx;
				}

				const ix = await this.driftClient.getLiquidateBorrowForPerpPnlIx(
					user.userAccountPublicKey,
					user.getUserAccount(),
					liquidateePosition.marketIndex,
					borrowMarketIndextoLiq,
					borrowAmountToLiq.div(frac),
					undefined,
					subAccountToLiqBorrow
				);
				const simResult = await this.buildVersionedTransactionWithSimulatedCus(
					[ix],
					[this.driftLookupTables!],
					Math.floor(this.priorityFeeSubscriber.getCustomStrategyResult())
				);
				if (simResult.simError !== null) {
					logger.error(
						`Error in liquidateBorrowForPerpPnl for userAccount ${user.userAccountPublicKey.toBase58()} in spot market ${borrowMarketIndextoLiq}, simError: ${JSON.stringify(
							simResult.simError
						)}`
					);

					const errorCode = handleSimResultError(
						simResult,
						errorCodesToSuppress,
						`${this.name}: liquidateBorrowForPerpPnl`
					);
					if (errorCode && !errorCodesToSuppress.includes(errorCode)) {
						const msg = `[${
							this.name
						}]: :x: error in liquidateBorrowForPerpPnl for userAccount ${user.userAccountPublicKey.toBase58()}: \n${
							simResult.simTxLogs ? simResult.simTxLogs.join('\n') : ''
						}`;
						logger.error(msg);
						webhookMessage(msg);
					}
				} else {
					const resp = await this.driftClient.txSender.sendVersionedTransaction(
						simResult.tx,
						undefined,
						this.driftClient.opts
					);
					logger.info(
						`Sent liquidateBorrowForPerpPnl tx for ${user.userAccountPublicKey.toBase58()} in spot market ${borrowMarketIndextoLiq} tx: ${
							resp.txSig
						} `
					);

					if (this.liquidatorConfig.notifyOnLiquidation) {
						webhookMessage(
							`[${
								this.name
							}]: liquidateBorrowForPerpPnl for userAccount ${user.userAccountPublicKey.toBase58()} in spot market ${borrowMarketIndextoLiq} tx: ${
								resp.txSig
							} `
						);
					}
				}
			} else {
				logger.info(
					`claimablePnl = ${claimablePnl.toString()} << liquidateePosition.quoteAssetAmount=${liquidateePosition.quoteAssetAmount.toString()} `
				);
				logger.info(`skipping liquidateBorrowForPerpPnl`);
			}
		} else {
			const start = Date.now();

			const { perpTier: safestPerpTier, spotTier: safestSpotTier } =
				user.getSafestTiers();
			const perpTier = getPerpMarketTierNumber(perpMarketAccount);
			if (!perpTierIsAsSafeAs(perpTier, safestPerpTier, safestSpotTier)) {
				return sentTx;
			}

			if (!this.spotMarketIndicies.includes(depositMarketIndextoLiq)) {
				logger.info(
					`skipping liquidatePerpPnlForDeposit of ${user.userAccountPublicKey.toBase58()} on spot market ${depositMarketIndextoLiq} because it is not in spotMarketIndices`
				);
				return sentTx;
			}

			const subAccountToTakeOverPerpPnl = this.getSubAccountIdToLiquidatePerp(
				liquidateePosition.marketIndex
			);
			if (subAccountToTakeOverPerpPnl === undefined) {
				return sentTx;
			}

			const pnlToLiq = this.driftClient
				.getUser(subAccountToTakeOverPerpPnl)
				.getFreeCollateral('Initial');
			try {
				const ix = await this.driftClient.getLiquidatePerpPnlForDepositIx(
					user.userAccountPublicKey,
					user.getUserAccount(),
					liquidateePosition.marketIndex,
					depositMarketIndextoLiq,
					pnlToLiq,
					undefined,
					subAccountToTakeOverPerpPnl
				);
				const simResult = await this.buildVersionedTransactionWithSimulatedCus(
					[ix],
					[this.driftLookupTables!],
					Math.floor(this.priorityFeeSubscriber.getCustomStrategyResult())
				);

				if (simResult.simError !== null) {
					logger.error(
						`Error in liquidatePerpPnlForDeposit for userAccount ${user.userAccountPublicKey.toBase58()} on market ${
							liquidateePosition.marketIndex
						}: simError: ${JSON.stringify(simResult.simError)}`
					);
					const errorCode = handleSimResultError(
						simResult,
						errorCodesToSuppress,
						`${this.name}: liquidatePerpPnlForDeposit`
					);
					if (errorCode && !errorCodesToSuppress.includes(errorCode)) {
						const msg = `[${
							this.name
						}]: :x: error in liquidatePerpPnlForDeposit for userAccount ${user.userAccountPublicKey.toBase58()} on market ${
							liquidateePosition.marketIndex
						}: \n${simResult.simTxLogs ? simResult.simTxLogs.join('\n') : ''}`;
						logger.error(msg);
						webhookMessage(msg);
					} else {
						this.throttledUsers.set(
							user.userAccountPublicKey.toBase58(),
							Date.now()
						);
					}
				} else {
					const resp = await this.driftClient.txSender.sendVersionedTransaction(
						simResult.tx,
						undefined,
						this.driftClient.opts
					);
					sentTx = true;
					logger.info(
						`Sent liquidatePerpPnlForDeposit tx for ${user.userAccountPublicKey.toBase58()} on market ${
							liquidateePosition.marketIndex
						} tx: ${resp.txSig} `
					);
					if (this.liquidatorConfig.notifyOnLiquidation) {
						webhookMessage(
							`[${
								this.name
							}]: liquidatePerpPnlForDeposit for ${user.userAccountPublicKey.toBase58()} on market ${
								liquidateePosition.marketIndex
							} tx: ${resp.txSig} `
						);
					}
				}
			} catch (err) {
				logger.error(
					`Error in liquidatePerpPnlForDeposit for userAccount ${user.userAccountPublicKey.toBase58()} on market ${
						liquidateePosition.marketIndex
					}`
				);
				console.error(err);
			} finally {
				this.recordHistogram(start, 'liquidatePerpPnlForDeposit');
			}
		}

		return sentTx;
	}

	private async liqPerp(
		user: User,
		perpMarketIndex: number,
		subAccountToLiqPerp: number,
		baseAmountToLiquidate: BN
	): Promise<boolean> {
		// TODO: remove this once the markets are settled properly
		if ([37, 49].includes(perpMarketIndex)) {
			return false;
		}

		let txSent = false;
		const ix = await this.driftClient.getLiquidatePerpIx(
			user.userAccountPublicKey,
			user.getUserAccount(),
			perpMarketIndex,
			baseAmountToLiquidate,
			undefined,
			subAccountToLiqPerp
		);
		const simResult = await this.buildVersionedTransactionWithSimulatedCus(
			[ix],
			[this.driftLookupTables!],
			Math.floor(this.priorityFeeSubscriber.getCustomStrategyResult())
		);
		if (simResult.simError !== null) {
			logger.error(
				`Error in liquidatePerp for userAccount ${user.userAccountPublicKey.toBase58()} on market ${perpMarketIndex}, simError: ${JSON.stringify(
					simResult.simError
				)}`
			);
			const errorCode = handleSimResultError(
				simResult,
				errorCodesToSuppress,
				`${this.name}: liquidatePerp`
			);
			if (errorCode && !errorCodesToSuppress.includes(errorCode)) {
				const msg = `[${
					this.name
				}]: :x: error in liquidatePerp for userAccount ${user.userAccountPublicKey.toBase58()} on market ${perpMarketIndex}: \n${
					simResult.simTxLogs ? simResult.simTxLogs.join('\n') : ''
				}`;
				logger.error(msg);
				webhookMessage(msg);
			} else {
				this.throttledUsers.set(
					user.userAccountPublicKey.toBase58(),
					Date.now()
				);
			}
		} else {
			const resp = await this.driftClient.txSender.sendVersionedTransaction(
				simResult.tx,
				undefined,
				this.driftClient.opts
			);
			txSent = true;
			logger.info(
				`Sent liquidatePerp tx for ${user.userAccountPublicKey.toBase58()} on market ${perpMarketIndex} tx: ${
					resp.txSig
				} `
			);
			if (this.liquidatorConfig.notifyOnLiquidation) {
				webhookMessage(
					`[${
						this.name
					}]: liquidatePerp for ${user.userAccountPublicKey.toBase58()} on market ${perpMarketIndex} tx: ${
						resp.txSig
					} `
				);
			}
		}
		return txSent;
	}

	private intializeBucketUsers(): void {
		for (const user of this.userMap!.values()) {
			const { canBeLiquidated, marginRequirement, totalCollateral } =
				user.canBeLiquidated();
			const health = user.getHealth(totalCollateral, marginRequirement);
			const userKey = user.userAccountPublicKey.toBase58();
			if (canBeLiquidated || user.isBeingLiquidated()) {
				if (this.excludedAccounts.has(userKey)) {
					// Debug log precisely because the intent is to avoid noise
					logger.debug(
						`Skipping liquidation for ${userKey} due to configuration`
					);
				} else {
					this.usersCanBeLiquidated.push({
						user,
						userKey,
						marginRequirement,
						canBeLiquidated,
						health,
					});
				}
			} else if (health >= 20) {
				this.usersUnhealthyHigh.push({
					user,
					userKey,
					marginRequirement,
					canBeLiquidated: false,
					health,
				});
			} else if (health >= 5) {
				this.usersUnhealthyMedium.push({
					user,
					userKey,
					marginRequirement,
					canBeLiquidated: false,
					health,
				});
			} else {
				this.usersUnhealthyLow.push({
					user,
					userKey,
					marginRequirement,
					canBeLiquidated: false,
					health,
				});
			}
		}

		// sort the usersCanBeLiquidated by marginRequirement, largest to smallest
		this.usersCanBeLiquidated.sort((a, b) => {
			return b.marginRequirement.gt(a.marginRequirement) ? 1 : -1;
		});
		this.usersUnhealthyHigh.sort((a, b) => {
			return b.marginRequirement.gt(a.marginRequirement) ? 1 : -1;
		});
		this.usersUnhealthyMedium.sort((a, b) => {
			return b.marginRequirement.gt(a.marginRequirement) ? 1 : -1;
		});
		this.usersUnhealthyLow.sort((a, b) => {
			return b.marginRequirement.gt(a.marginRequirement) ? 1 : -1;
		});
	}

	private async updateUserBucket(
		user: {
			user: User;
			userKey: string;
			marginRequirement: BN;
			canBeLiquidated: boolean;
			health: number;
		},
		currentBucket: UserBucket
	): Promise<void> {
		const { canBeLiquidated, marginRequirement, totalCollateral } =
			user.user.canBeLiquidated();
		const newHealth = user.user.getHealth(totalCollateral, marginRequirement);

		// Determine which bucket the user should be in
		let targetBucket: UserBucket;

		if (canBeLiquidated || user.user.isBeingLiquidated()) {
			targetBucket = UserBucket.CAN_BE_LIQUIDATED;
		} else if (newHealth >= 20) {
			targetBucket = UserBucket.UNHEALTHY_HIGH;
		} else if (newHealth >= 5) {
			targetBucket = UserBucket.UNHEALTHY_MEDIUM;
		} else {
			targetBucket = UserBucket.UNHEALTHY_LOW;
		}

		// If bucket hasn't changed, no need to update
		if (targetBucket === currentBucket) {
			return;
		}

		// Remove from current bucket
		let currentArray: Array<typeof user>;
		switch (currentBucket) {
			case UserBucket.CAN_BE_LIQUIDATED:
				currentArray = this.usersCanBeLiquidated;
				break;
			case UserBucket.UNHEALTHY_HIGH:
				currentArray = this.usersUnhealthyHigh;
				break;
			case UserBucket.UNHEALTHY_MEDIUM:
				currentArray = this.usersUnhealthyMedium;
				break;
			case UserBucket.UNHEALTHY_LOW:
				currentArray = this.usersUnhealthyLow;
				break;
		}

		const index = currentArray.findIndex((u) => u.userKey === user.userKey);
		if (index !== -1) {
			currentArray.splice(index, 1);
		}

		const updatedUser = {
			...user,
			marginRequirement,
			canBeLiquidated,
			health: newHealth,
		};

		switch (targetBucket) {
			case UserBucket.CAN_BE_LIQUIDATED:
				this.usersCanBeLiquidated.push(updatedUser);
				break;
			case UserBucket.UNHEALTHY_HIGH:
				this.usersUnhealthyHigh.push(updatedUser);
				break;
			case UserBucket.UNHEALTHY_MEDIUM:
				this.usersUnhealthyMedium.push(updatedUser);
				break;
			case UserBucket.UNHEALTHY_LOW:
				this.usersUnhealthyLow.push(updatedUser);
				break;
		}

		logger.debug(
			`Moved user ${user.userKey} from ${currentBucket} to ${targetBucket} (health: ${newHealth})`
		);
	}

	private async checkBucket(bucketName: UserBucket): Promise<void> {
		const bucket = (() => {
			switch (bucketName) {
				case UserBucket.CAN_BE_LIQUIDATED:
					return this.usersCanBeLiquidated;
				case UserBucket.UNHEALTHY_HIGH:
					return this.usersUnhealthyHigh;
				case UserBucket.UNHEALTHY_MEDIUM:
					return this.usersUnhealthyMedium;
				case UserBucket.UNHEALTHY_LOW:
					return this.usersUnhealthyLow;
			}
		})();

		// Create a copy of the array since we'll be modifying it during iteration
		const usersToCheck = [...bucket];

		for (const user of usersToCheck) {
			await this.updateUserBucket(user, bucketName);
		}

		// we rerun the sorts of every bucket after we update the bucket
		// TODO: we can optimize this by only sorting the bucket that was updated
		this.usersCanBeLiquidated.sort((a, b) => {
			return b.marginRequirement.gt(a.marginRequirement) ? 1 : -1;
		});
		this.usersUnhealthyHigh.sort((a, b) => {
			return b.marginRequirement.gt(a.marginRequirement) ? 1 : -1;
		});
		this.usersUnhealthyMedium.sort((a, b) => {
			return b.marginRequirement.gt(a.marginRequirement) ? 1 : -1;
		});
		this.usersUnhealthyLow.sort((a, b) => {
			return b.marginRequirement.gt(a.marginRequirement) ? 1 : -1;
		});
	}

	private async tryLiquidate(): Promise<{
		checkedUsers: number;
		liquidatableUsers: number;
		ran: boolean;
		skipReason: {
			untrackedPerpMarket: number;
			untrackedSpotMarket: number;
			throttledUser: number;
			liquidatePerpPnlForDepositSent: number;
			liquidatePerpSent: number;
			liquidateSpotSent: number;
		};
	}> {
		// Simulates CPU clock cycles that prioritize unhealthy users:
		// 		80% of time check very unhealthy users,
		// 		8% of time check almost unhealthy users,
		// 		2% of time check healthy users
		// 		10% of time check liquidatable users
		const random = Math.random();
		let bucket: UserBucket;
		if (random < 0.8) {
			bucket = UserBucket.UNHEALTHY_LOW;
		} else if (random < 0.88) {
			bucket = UserBucket.UNHEALTHY_MEDIUM;
		} else if (random < 0.9) {
			bucket = UserBucket.UNHEALTHY_HIGH;
		} else {
			bucket = UserBucket.CAN_BE_LIQUIDATED;
		}

		const start = Date.now();
		await this.checkBucket(bucket);
		const end = Date.now();
		logger.info(`Checked bucket ${bucket} in ${end - start}ms`);

		const checkedUsersCount =
			this.usersCanBeLiquidated.length +
			this.usersUnhealthyHigh.length +
			this.usersUnhealthyMedium.length +
			this.usersUnhealthyLow.length;

		let untrackedPerpMarket = 0;
		let untrackedSpotMarket = 0;
		let throttledUser = 0;
		let liquidatePerpPnlForDepositSent = 0;
		let liquidatePerpSent = 0;
		let liquidateSpotSent = 0;

		for (const {
			user,
			userKey,
			marginRequirement: _marginRequirement,
			canBeLiquidated,
		} of this.usersCanBeLiquidated) {
			const userAcc = user.getUserAccount();
			const auth = userAcc.authority.toBase58();

			if (isUserBankrupt(user) || user.isBankrupt()) {
				await this.tryResolveBankruptUser(user);
			} else if (canBeLiquidated) {
				const lastAttempt = this.throttledUsers.get(userKey);
				if (lastAttempt) {
					const now = Date.now();
					if (lastAttempt + LIQUIDATE_THROTTLE_BACKOFF > now) {
						logger.warn(
							`skipping user(throttled, retry in ${
								lastAttempt + LIQUIDATE_THROTTLE_BACKOFF - now
							}ms) ${auth}: ${userKey} `
						);
						throttledUser++;
						continue;
					} else {
						this.throttledUsers.delete(userKey);
					}
				}

				const liquidateeUserAccount = user.getUserAccount();

				// most attractive spot market liq
				const {
					bestIndex: depositMarketIndextoLiq,
					bestAmount: depositAmountToLiq,
					indexWithMaxAssets,
					indexWithOpenOrders,
				} = this.findBestSpotPosition(
					user,
					liquidateeUserAccount.spotPositions,
					false,
					this.maxPositionTakeoverPctOfCollateralNum,
					this.maxPositionTakeoverPctOfCollateralDenom
				);

				const {
					bestIndex: borrowMarketIndextoLiq,
					bestAmount: borrowAmountToLiq,
				} = this.findBestSpotPosition(
					user,
					liquidateeUserAccount.spotPositions,
					true,
					this.maxPositionTakeoverPctOfCollateralNum,
					this.maxPositionTakeoverPctOfCollateralDenom
				);

				let liquidateeHasSpotPos = false;
				if (borrowMarketIndextoLiq != -1 && depositMarketIndextoLiq != -1) {
					liquidateeHasSpotPos = true;
					const sent = await this.liqBorrow(
						depositMarketIndextoLiq,
						borrowMarketIndextoLiq,
						borrowAmountToLiq,
						user
					);
					if (sent) {
						liquidateSpotSent++;
					}
				}

				const usdcMarket = this.driftClient.getSpotMarketAccount(
					QUOTE_SPOT_MARKET_INDEX
				);
				if (!usdcMarket) {
					throw new Error(
						`USDC spot market not loaded, misconfigured DriftClient`
					);
				}

				// less attractive, perp / perp pnl liquidations
				let liquidateeHasPerpPos = false;
				let liquidateeHasUnsettledPerpPnl = false;
				let liquidateeHasLpPos = false;
				let liquidateePerpIndexWithOpenOrders = -1;

				// shuffle user perp positions to get good position coverage in case some
				// positions put the liquidator into a bad state.
				const userPerpPositions = user.getActivePerpPositions();
				userPerpPositions.sort(() => Math.random() - 0.5);

				for (const liquidateePosition of userPerpPositions) {
					if (liquidateePosition.openOrders > 0) {
						liquidateePerpIndexWithOpenOrders = liquidateePosition.marketIndex;
					}

					const perpMarket = this.driftClient.getPerpMarketAccount(
						liquidateePosition.marketIndex
					);
					if (!perpMarket) {
						throw new Error(
							`perpMarket not loaded for marketIndex ${liquidateePosition.marketIndex}, misconfigured DriftClient`
						);
					}

					// TODO: use enum on new sdk release
					if (
						isOperationPaused(perpMarket.pausedOperations, 32 as PerpOperation)
					) {
						logger.info(
							`Skipping liquidation for ${userKey} on market ${perpMarket.marketIndex}, liquidation paused`
						);
						continue;
					}

					liquidateeHasUnsettledPerpPnl =
						liquidateePosition.baseAssetAmount.isZero() &&
						!liquidateePosition.quoteAssetAmount.isZero();
					liquidateeHasPerpPos =
						!liquidateePosition.baseAssetAmount.isZero() ||
						!liquidateePosition.quoteAssetAmount.isZero();
					liquidateeHasLpPos = !liquidateePosition.lpShares.isZero();

					const tryLiqPerp =
						liquidateeHasUnsettledPerpPnl &&
						// call liquidatePerpPnlForDeposit
						((liquidateePosition.quoteAssetAmount.lt(ZERO) &&
							depositMarketIndextoLiq > -1) ||
							// call liquidateBorrowForPerpPnl or settlePnl
							liquidateePosition.quoteAssetAmount.gt(ZERO));
					if (tryLiqPerp) {
						const sent = await this.liqPerpPnl(
							user,
							perpMarket,
							usdcMarket,
							liquidateePosition,
							depositMarketIndextoLiq,
							depositAmountToLiq,
							borrowMarketIndextoLiq,
							borrowAmountToLiq
						);
						if (sent) {
							liquidatePerpPnlForDepositSent++;
						}
					}

					const baseAmountToLiquidate = this.calculateBaseAmountToLiquidate(
						user.getPerpPositionWithLPSettle(
							liquidateePosition.marketIndex,
							liquidateePosition
						)[0]
					);

					const subAccountToLiqPerp = this.getSubAccountIdToLiquidatePerp(
						liquidateePosition.marketIndex
					);
					if (subAccountToLiqPerp === undefined) {
						untrackedPerpMarket++;
						continue;
					}

					if (baseAmountToLiquidate.gt(ZERO)) {
						if (this.dryRun) {
							logger.warn('--dry run flag enabled - not sending liquidate tx');
							continue;
						}

						const sent = await this.liqPerp(
							user,
							liquidateePosition.marketIndex,
							subAccountToLiqPerp,
							baseAmountToLiquidate
						);
						if (sent) {
							liquidatePerpSent++;
						}
					} else if (liquidateeHasLpPos) {
						logger.info(
							`liquidatePerp ${auth}-${user.userAccountPublicKey.toBase58()} on market ${
								liquidateePosition.marketIndex
							} has lp shares but no perp pos, trying to clear it:`
						);
						const sent = await this.liqPerp(
							user,
							liquidateePosition.marketIndex,
							subAccountToLiqPerp,
							ZERO
						);
						if (sent) {
							liquidatePerpSent++;
						}
					}
				}

				if (!liquidateeHasSpotPos && !liquidateeHasPerpPos) {
					logger.info(
						`${auth}-${user.userAccountPublicKey.toBase58()} can be liquidated but has no positions`
					);
					if (liquidateePerpIndexWithOpenOrders !== -1) {
						logger.info(
							`${auth}-${user.userAccountPublicKey.toBase58()} liquidatePerp with open orders in ${liquidateePerpIndexWithOpenOrders}`
						);
						this.throttledUsers.set(
							user.userAccountPublicKey.toBase58(),
							Date.now()
						);

						const subAccountToLiqPerp = this.getSubAccountIdToLiquidatePerp(
							liquidateePerpIndexWithOpenOrders
						);
						if (subAccountToLiqPerp === undefined) {
							untrackedPerpMarket++;
							continue;
						}

						const sent = await this.liqPerp(
							user,
							liquidateePerpIndexWithOpenOrders,
							subAccountToLiqPerp,
							ZERO
						);
						if (sent) {
							liquidatePerpSent++;
						}
					}
					if (indexWithOpenOrders !== -1) {
						logger.info(
							`${auth}-${user.userAccountPublicKey.toBase58()} liquidateSpot with assets in ${indexWithMaxAssets} and open orders in ${indexWithOpenOrders}`
						);
						this.throttledUsers.set(
							user.userAccountPublicKey.toBase58(),
							Date.now()
						);

						const subAccountToLiqSpot =
							this.getSubAccountIdToLiquidateSpot(indexWithOpenOrders);
						if (subAccountToLiqSpot === undefined) {
							untrackedSpotMarket++;
							continue;
						}
						const sent = await this.liqSpot(
							user,
							indexWithMaxAssets,
							indexWithOpenOrders,
							subAccountToLiqSpot,
							ZERO
						);
						if (sent) {
							liquidateSpotSent++;
						}
					}
				}
			} else if (user.isBeingLiquidated()) {
				// liquidate the user to bring them out of liquidation status, can liquidate any market even
				// if the user doesn't have a position in it
				logger.info(
					`[${
						this.name
					}]: user stuck in beingLiquidated status, need to clear it for ${user.userAccountPublicKey.toBase58()}`
				);

				// can liquidate with any subaccount, no liability transfer
				const sent = await this.liqPerp(
					user,
					0,
					this.driftClient.activeSubAccountId,
					ZERO
				);
				if (sent) {
					liquidatePerpSent++;
				}
			}
		}
		return {
			ran: true,
			checkedUsers: checkedUsersCount,
			liquidatableUsers: this.usersCanBeLiquidated.length,
			skipReason: {
				untrackedPerpMarket,
				untrackedSpotMarket,
				throttledUser,
				liquidatePerpPnlForDepositSent,
				liquidatePerpSent,
				liquidateSpotSent,
			},
		};
	}

	/**
	 * iterates over users in userMap and checks:
	 * 		1. is user bankrupt? if so, resolve bankruptcy
	 * 		2. is user in liquidation? If so, endangered position is liquidated
	 */
	private async tryLiquidateStart() {
		const start = Date.now();
		/*
		  If there is more than one subAccount, then derisk and liquidate may try to
		  change it, so both need to be mutually exclusive.

		  If not, all we care about is avoiding two liquidation calls at the same time.
		 */
		const mutex =
			this.allSubaccounts.size > 1 ? this.deriskMutex : this.liquidateMutex;
		if (!this.acquireMutex(mutex)) {
			logger.info(
				`${this.name} tryLiquidate ran into locked mutex, skipping run.`
			);
			return;
		}
		let ran = false;
		let checkedUsers = 0;
		let liquidatableUsers = 0;
		let skipReason: any;
		try {
			({ ran, checkedUsers, liquidatableUsers, skipReason } =
				await this.tryLiquidate());
		} catch (e) {
			console.error(e);
			if (e instanceof Error) {
				webhookMessage(
					`[${this.name}]: :x: uncaught error: \n${
						e.stack ? e.stack : e.message
					} `
				);
			}
		} finally {
			if (ran) {
				logger.info(
					`${
						this.name
					} Bot checked ${checkedUsers} users, ${liquidatableUsers} liquidateable, skipReason: ${JSON.stringify(
						skipReason
					)}. Took: ${Date.now() - start}ms to run`
				);
				this.watchdogTimerLastPatTime = Date.now();
			}
		}
		this.releaseMutex(mutex);
	}

	private recordHistogram(start: number, method: string) {
		if (this.sdkCallDurationHistogram) {
			this.sdkCallDurationHistogram!.record(Date.now() - start, {
				method: method,
			});
		}
	}

	private initializeMetrics() {
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
			],
		});

		meterProvider.addMetricReader(this.exporter);
		this.meter = meterProvider.getMeter(meterName);

		this.runtimeSpecsGauge = this.meter.createObservableGauge(
			METRIC_TYPES.runtime_specs,
			{
				description: 'Runtime sepcification of this program',
			}
		);
		this.runtimeSpecsGauge.addCallback((obs) => {
			obs.observe(1, this.runtimeSpecs);
		});

		this.totalLeverage = this.meter.createObservableGauge(
			METRIC_TYPES.total_leverage,
			{
				description: 'Total leverage of the account',
			}
		);
		this.totalCollateral = this.meter.createObservableGauge(
			METRIC_TYPES.total_collateral,
			{
				description: 'Total collateral of the account',
			}
		);
		this.freeCollateral = this.meter.createObservableGauge(
			METRIC_TYPES.free_collateral,
			{
				description: 'Free collateral of the account',
			}
		);
		this.perpPositionValue = this.meter.createObservableGauge(
			METRIC_TYPES.perp_posiiton_value,
			{
				description: 'Value of account perp positions',
			}
		);
		this.perpPositionBase = this.meter.createObservableGauge(
			METRIC_TYPES.perp_posiiton_base,
			{
				description: 'Base asset value of account perp positions',
			}
		);
		this.perpPositionQuote = this.meter.createObservableGauge(
			METRIC_TYPES.perp_posiiton_quote,
			{
				description: 'Quote asset value of account perp positions',
			}
		);
		this.initialMarginRequirement = this.meter.createObservableGauge(
			METRIC_TYPES.initial_margin_requirement,
			{
				description: 'The account initial margin requirement',
			}
		);
		this.maintenanceMarginRequirement = this.meter.createObservableGauge(
			METRIC_TYPES.maintenance_margin_requirement,
			{
				description: 'The account maintenance margin requirement',
			}
		);
		this.unrealizedPnL = this.meter.createObservableGauge(
			METRIC_TYPES.unrealized_pnl,
			{
				description: 'The account unrealized PnL',
			}
		);
		this.unrealizedFundingPnL = this.meter.createObservableGauge(
			METRIC_TYPES.unrealized_funding_pnl,
			{
				description: 'The account unrealized funding PnL',
			}
		);

		this.sdkCallDurationHistogram = this.meter.createHistogram(
			METRIC_TYPES.sdk_call_duration_histogram,
			{
				description: 'Distribution of sdk method calls',
				unit: 'ms',
			}
		);

		this.userMapUserAccountKeysGauge = this.meter.createObservableGauge(
			METRIC_TYPES.user_map_user_account_keys,
			{
				description: 'number of user account keys in UserMap',
			}
		);
		this.userMapUserAccountKeysGauge.addCallback(async (obs) => {
			obs.observe(this.userMap!.size());
		});

		this.meter.addBatchObservableCallback(
			async (batchObservableResult: BatchObservableResult) => {
				// each subaccount is responsible for a market
				// record account specific metrics
				for (const [idx, user] of this.driftClient.getUsers().entries()) {
					const accMarketIdx = idx;
					const userAccount = user.getUserAccount();
					const oracle =
						this.driftClient.getOracleDataForPerpMarket(accMarketIdx);

					batchObservableResult.observe(
						this.totalLeverage!,
						convertToNumber(user.getLeverage(), TEN_THOUSAND),
						metricAttrFromUserAccount(user.userAccountPublicKey, userAccount)
					);
					batchObservableResult.observe(
						this.totalCollateral!,
						convertToNumber(user.getTotalCollateral(), QUOTE_PRECISION),
						metricAttrFromUserAccount(user.userAccountPublicKey, userAccount)
					);
					batchObservableResult.observe(
						this.freeCollateral!,
						convertToNumber(user.getFreeCollateral(), QUOTE_PRECISION),
						metricAttrFromUserAccount(user.userAccountPublicKey, userAccount)
					);
					batchObservableResult.observe(
						this.perpPositionValue!,
						convertToNumber(
							user.getPerpPositionValue(accMarketIdx, oracle),
							QUOTE_PRECISION
						),
						metricAttrFromUserAccount(user.userAccountPublicKey, userAccount)
					);

					const perpPosition = user.getPerpPosition(accMarketIdx);
					batchObservableResult.observe(
						this.perpPositionBase!,
						convertToNumber(
							perpPosition!.baseAssetAmount ?? ZERO,
							BASE_PRECISION
						),
						metricAttrFromUserAccount(user.userAccountPublicKey, userAccount)
					);
					batchObservableResult.observe(
						this.perpPositionQuote!,
						convertToNumber(
							perpPosition!.quoteAssetAmount ?? ZERO,
							QUOTE_PRECISION
						),
						metricAttrFromUserAccount(user.userAccountPublicKey, userAccount)
					);

					batchObservableResult.observe(
						this.initialMarginRequirement!,
						convertToNumber(
							user.getInitialMarginRequirement(),
							QUOTE_PRECISION
						),
						metricAttrFromUserAccount(user.userAccountPublicKey, userAccount)
					);
					batchObservableResult.observe(
						this.maintenanceMarginRequirement!,
						convertToNumber(
							user.getMaintenanceMarginRequirement(),
							QUOTE_PRECISION
						),
						metricAttrFromUserAccount(user.userAccountPublicKey, userAccount)
					);
					batchObservableResult.observe(
						this.unrealizedPnL!,
						convertToNumber(user.getUnrealizedPNL(), QUOTE_PRECISION),
						metricAttrFromUserAccount(user.userAccountPublicKey, userAccount)
					);
					batchObservableResult.observe(
						this.unrealizedFundingPnL!,
						convertToNumber(user.getUnrealizedFundingPNL(), QUOTE_PRECISION),
						metricAttrFromUserAccount(user.userAccountPublicKey, userAccount)
					);
				}
			},
			[
				this.totalLeverage,
				this.totalCollateral,
				this.freeCollateral,
				this.perpPositionValue,
				this.perpPositionBase,
				this.perpPositionQuote,
				this.initialMarginRequirement,
				this.maintenanceMarginRequirement,
				this.unrealizedPnL,
				this.unrealizedFundingPnL,
			]
		);
	}
}

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
	SerumFulfillmentConfigMap,
	initialize,
	DriftEnv,
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
	TxParams,
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
import {
	getPerpMarketTierNumber,
	perpTierIsAsSafeAs,
} from '@drift-labs/sdk/lib/math/tiers';
import {
	ComputeBudgetProgram,
	PublicKey,
	AddressLookupTableAccount,
	TransactionInstruction,
} from '@solana/web3.js';
import {
	calculateAccountValueUsd,
	handleSimResultError,
	simulateAndGetTxWithCUs,
	SimulateAndGetTxWithCUsResponse,
} from '../utils';

const errorCodesToSuppress = [
	6004, // Error Number: 6004. Error Message: Sufficient collateral.
	6010, // Error Number: 6010. Error Message: User Has No Position In Market.
];

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

function findBestSpotPosition(
	driftClient: DriftClient,
	liquidateeUser: User,
	liquidatorUser: User,
	spotPositions: SpotPosition[],
	isBorrow: boolean,
	positionTakerOverPctNumerator: BN,
	positionTakerOverPctDenominator: BN,
	minDepositToLiq: Map<number, number>
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
		if (position.scaledBalance.eq(ZERO)) {
			continue;
		}

		// Skip any position that is less than the configured minimum amount
		// for the specific market
		const minAmount = new BN(minDepositToLiq.get(position.marketIndex) ?? 0);
		if (position.scaledBalance.abs().lt(minAmount)) {
			logger.debug(
				`findBestspotPosition: Amount ${position.scaledBalance} below ${minAmount} liquidation threshold`
			);
			continue;
		}

		const market = driftClient.getSpotMarketAccount(position.marketIndex);
		if (!market) {
			logger.error(`No spot market found for ${position.marketIndex}`);
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

		const spotMarket = driftClient.getSpotMarketAccount(position.marketIndex);
		if (!spotMarket) {
			logger.error(`No spot market found for ${position.marketIndex}`);
			continue;
		}
		const tokenAmount = calculateSpotTokenAmountToLiquidate(
			driftClient,
			liquidatorUser,
			position,
			positionTakerOverPctNumerator,
			positionTakerOverPctDenominator
		);

		liquidatorUser.getSpotMarketAssetAndLiabilityValue();

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

	private perpMarketIndicies: number[];
	private spotMarketIndicies: number[];
	private activeSubAccountId: number;
	private allSubaccounts: Set<number>;
	private perpMarketToSubAccount: Map<number, number>;
	private spotMarketToSubAccount: Map<number, number>;
	private intervalIds: Array<NodeJS.Timer> = [];
	private userMap: UserMap;
	private deriskMutex = new Uint8Array(new SharedArrayBuffer(1));
	private liquidateMutex = new Uint8Array(new SharedArrayBuffer(1));
	private runtimeSpecs: RuntimeSpec;
	private serumFulfillmentConfigMap: SerumFulfillmentConfigMap;
	private jupiterClient?: JupiterClient;
	private twapExecutionProgresses?: Map<string, TwapExecutionProgress>; // key: this.getTwapProgressKey, value: TwapExecutionProgress
	private minDepositToLiq: Map<number, number>;
	private excludedAccounts: Set<string>;

	private priorityFeeSubscriber: PriorityFeeSubscriber;

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

		this.name = config.botId;
		this.dryRun = config.dryRun;
		this.driftClient = driftClient;
		this.runtimeSpecs = runtimeSpec;
		this.serumFulfillmentConfigMap = new SerumFulfillmentConfigMap(
			this.driftClient
		);
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

		const allPerpMarkets = this.driftClient.getPerpMarketAccounts().map((m) => {
			return m.marketIndex;
		});
		if (
			config.perpSubAccountConfig &&
			Object.keys(config.perpSubAccountConfig).length != 0
		) {
			logger.info('Loading perp markets to watch from perpSubAccountConfig');
			for (const subAccount of Object.keys(config.perpSubAccountConfig)) {
				const marketsForAccount =
					config.perpSubAccountConfig[parseInt(subAccount)] || allPerpMarkets;
				for (const market of marketsForAccount) {
					this.perpMarketToSubAccount.set(market, parseInt(subAccount));
					this.allSubaccounts.add(parseInt(subAccount));
				}
			}
			this.perpMarketIndicies = Object.values(
				config.perpSubAccountConfig
			).flat();
		} else {
			logger.info('Loading perp markets to watch from perpMarketIndicies');
			this.perpMarketIndicies = config.perpMarketIndicies || [];
			if (!this.perpMarketIndicies || this.perpMarketIndicies.length === 0) {
				this.perpMarketIndicies = allPerpMarkets;
			}
			for (const market of this.perpMarketIndicies) {
				this.perpMarketToSubAccount.set(market, this.activeSubAccountId);
			}
		}
		logger.info(`${this.name} perpMarketIndicies: ${this.perpMarketIndicies}`);
		console.log('this.perpMarketToSubaccount:');
		console.log(this.perpMarketToSubAccount);

		const allSpotMarkets = this.driftClient.getSpotMarketAccounts().map((m) => {
			return m.marketIndex;
		});
		if (config.spotSubAccountConfig) {
			logger.info('Loading spot markets to watch from spotSubAccountConfig');
			for (const subAccount of Object.keys(config.spotSubAccountConfig)) {
				const marketsForAccount =
					config.spotSubAccountConfig[parseInt(subAccount)] || allSpotMarkets;
				for (const market of marketsForAccount) {
					this.spotMarketToSubAccount.set(market, parseInt(subAccount));
					this.allSubaccounts.add(parseInt(subAccount));
				}
			}
			this.spotMarketIndicies = Object.values(
				config.spotSubAccountConfig
			).flat();
		} else {
			logger.info('Loading spot markets to watch from spotMarketIndicies');
			this.spotMarketIndicies = config.spotMarketIndicies || [];
			if (!this.spotMarketIndicies || this.spotMarketIndicies.length === 0) {
				this.spotMarketIndicies = allSpotMarkets;
			}
			for (const market of this.spotMarketIndicies) {
				this.spotMarketToSubAccount.set(market, this.activeSubAccountId);
			}
		}

		logger.info(`${this.name} spotMarketIndicies: ${this.spotMarketIndicies}`);
		console.log('this.spotMarketToSubaccount:');
		console.log(this.spotMarketToSubAccount);

		this.minDepositToLiq = new Map<number, number>();
		if (config.minDepositToLiq != null) {
			// We might get the keys parsed as strings
			for (const [k, v] of Object.entries(config.minDepositToLiq)) {
				this.minDepositToLiq.set(Number.parseInt(k), v);
			}
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
		logger.info(
			`this.allSubaccounts (${this.allSubaccounts.size}): ${JSON.stringify(
				Array.from(this.allSubaccounts)
			)}`
		);
		for (const subAccountId of this.allSubaccounts) {
			logger.info(` * ${subAccountId}`);
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
				throw new Error(
					`liquidator.config.maxPositionTakeoverPctOfCollateral cannot be > 1.00`
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

	private getTxParamsWithPriorityFees(): TxParams {
		return {
			computeUnits: 1_400_000,
			computeUnitsPrice: Math.floor(
				this.priorityFeeSubscriber.getCustomStrategyResult()
			),
		};
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

		return simulateAndGetTxWithCUs(
			fullIxs,
			this.driftClient.connection,
			this.driftClient.txSender,
			luts,
			[],
			this.driftClient.opts,
			1.2,
			true,
			undefined,
			false
		);
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

		const config = initialize({ env: this.runtimeSpecs.driftEnv as DriftEnv });
		for (const spotMarketConfig of config.SPOT_MARKETS) {
			if (spotMarketConfig.serumMarket) {
				// set up fulfillment config
				await this.serumFulfillmentConfigMap.add(
					spotMarketConfig.marketIndex,
					spotMarketConfig.serumMarket
				);
			}
		}

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

		const freeCollateral = this.driftClient.getUser().getFreeCollateral();
		logger.info(
			`${this.name} free collateral: $${convertToNumber(
				freeCollateral,
				QUOTE_PRECISION
			)}, spending at most ${
				(this.maxPositionTakeoverPctOfCollateralNum.toNumber() /
					this.maxPositionTakeoverPctOfCollateralDenom.toNumber()) *
				100.0
			}% per liquidation`
		);
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

		const ix = await this.driftClient.getPlaceSpotOrderIx(
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
			[ix],
			[this.driftLookupTables!],
			Math.floor(this.priorityFeeSubscriber.getCustomStrategyResult())
		);
		if (simResult.simError !== null) {
			logger.error(
				`Error trying to close spot position for market ${marketIndex}, subaccount start ${subAccountId}`
			);
			console.error(simResult.simError);
		} else {
			const resp = await this.driftClient.txSender.sendVersionedTransaction(
				simResult.tx,
				undefined,
				this.driftClient.opts
			);
			logger.info(
				`did derisk placeSpotOrder for on market ${marketIndex} tx: ${resp.txSig} `
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
				)} spot position for market ${spotMarketIndex} on jupiter, subaccount start ${subAccountId}`
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
				const ix = await this.driftClient.getPlacePerpOrderIx(
					orderParams,
					userAccount.subAccountId
				);

				const simResult = await this.buildVersionedTransactionWithSimulatedCus(
					[ix],
					[this.driftLookupTables!],
					Math.floor(this.priorityFeeSubscriber.getCustomStrategyResult())
				);

				if (simResult.simError !== null) {
					logger.error(
						`Error error in placePerpOrder in market: ${position.marketIndex}`
					);

					const errorCode = handleSimResultError(
						simResult,
						errorCodesToSuppress,
						`${this.name}: deriskPerpPositions`
					);

					if (errorCode && !errorCodesToSuppress.includes(errorCode)) {
						webhookMessage(
							`[${this.name}]: :x: error in placePerpOrder in market ${
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
					logger.info(`did deriskPerpPosition, tx: ${resp.txSig} `);
				}
			} else if (position.quoteAssetAmount.lt(ZERO)) {
				const userAccountPubkey =
					await this.driftClient.getUserAccountPublicKey();
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
						}`
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
						`did settle negative pnl for ${userAccountPubkey.toBase58()} on market ${
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
						await this.driftClient.getUserAccountPublicKey();
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
							}`
						);
						const errorCode = handleSimResultError(
							simResult,
							errorCodesToSuppress,
							`${this.name}: settlePositivePnl`
						);
						if (errorCode && !errorCodesToSuppress.includes(errorCode)) {
							webhookMessage(
								`[${
									this.name
								}]: :x: error in settlePnl for userAccount ${userAccountPubkey.toBase58()} on market ${
									position.marketIndex
								}: \n${
									simResult.simTxLogs ? simResult.simTxLogs.join('\n') : ''
								}`
							);
						}
					} else {
						const resp =
							await this.driftClient.txSender.sendVersionedTransaction(
								simResult.tx,
								undefined,
								this.driftClient.opts
							);
						logger.info(
							`did settle positive pnl for ${userAccountPubkey.toBase58()} on market ${
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
		if (isVariant(orderDirection, 'long')) {
			// sell USDC, buy spotMarketIndex
			inMarket = this.driftClient.getSpotMarketAccount(0);
			outMarket = this.driftClient.getSpotMarketAccount(spotMarketIndex);
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
			// sell spotMarketIndex, buy USDC
			inMarket = this.driftClient.getSpotMarketAccount(spotMarketIndex);
			outMarket = this.driftClient.getSpotMarketAccount(0);
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

	private async deriskSpotPositions(userAccount: UserAccount) {
		for (const position of userAccount.spotPositions) {
			if (position.scaledBalance.eq(ZERO) || position.marketIndex === 0) {
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
		const userAccount = this.driftClient.getUserAccount();
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

	private calculateBaseAmountToLiquidate(
		liquidatorUser: User,
		liquidateePosition: PerpPosition
	): BN {
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
					`Error in resolveBankruptcy for userAccount ${userKey.toBase58()} on market ${perpIdx}`
				);

				const errorCode = handleSimResultError(
					simResult,
					errorCodesToSuppress,
					`${this.name}: resolveBankruptcy`
				);
				if (errorCode && !errorCodesToSuppress.includes(errorCode)) {
					webhookMessage(
						`[${
							this.name
						}]: :x: error in resolveBankruptcy for userAccount ${userKey.toBase58()}: \n${
							simResult.simTxLogs ? simResult.simTxLogs.join('\n') : ''
						}`
					);
				}
			} else {
				const resp = await this.driftClient.txSender.sendVersionedTransaction(
					simResult.tx,
					undefined,
					this.driftClient.opts
				);
				logger.info(
					`did resolveBankruptcy for ${userKey.toBase58()} in perp market ${perpIdx} tx: ${
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
					`Error in resolveBankruptcy for userAccount ${userKey.toBase58()} in spot market ${spotIdx}`
				);

				const errorCode = handleSimResultError(
					simResult,
					errorCodesToSuppress,
					`${this.name}: resolveBankruptcy`
				);
				if (errorCode && !errorCodesToSuppress.includes(errorCode)) {
					webhookMessage(
						`[${
							this.name
						}]: :x: error in resolveBankruptcy for userAccount ${userKey.toBase58()}: \n${
							simResult.simTxLogs ? simResult.simTxLogs.join('\n') : ''
						}`
					);
				}
			} else {
				const resp = await this.driftClient.txSender.sendVersionedTransaction(
					simResult.tx,
					undefined,
					this.driftClient.opts
				);
				logger.info(
					`did resolveBankruptcy for ${userKey.toBase58()} in spot market ${spotIdx} tx: ${
						resp.txSig
					} `
				);
			}
		}
	}

	private getCollateralAvailableToLiquidate(subAccountId: number): BN {
		const currUser = this.driftClient.getUser(subAccountId);
		const freeCollateral = currUser.getFreeCollateral('Initial');
		return freeCollateral
			.mul(this.maxPositionTakeoverPctOfCollateralNum)
			.div(this.maxPositionTakeoverPctOfCollateralDenom);
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

	private async liqBorrow(
		depositMarketIndextoLiq: number,
		borrowMarketIndextoLiq: number,
		borrowAmountToLiq: BN,
		user: User
	) {
		const borrowMarket = this.driftClient.getSpotMarketAccount(
			borrowMarketIndextoLiq
		)!;
		const spotPrecision = TEN.pow(new BN(borrowMarket.decimals));
		logger.info(
			`liqBorrow: ${user.userAccountPublicKey.toBase58()} amountToLiq: ${convertToNumber(
				borrowAmountToLiq,
				spotPrecision
			)} on market ${borrowMarketIndextoLiq}`
		);

		const subAccountToLiqSpot = this.getSubAccountIdToLiquidateSpot(
			borrowMarketIndextoLiq
		);
		if (subAccountToLiqSpot === undefined) {
			return;
		}

		const currUser = this.driftClient.getUser(subAccountToLiqSpot);
		const oracle = this.driftClient.getOracleDataForSpotMarket(
			borrowMarketIndextoLiq
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

		let amountToLiqBN = borrowAmountToLiq;
		if (borrowValue.gt(collateralAvailable)) {
			amountToLiqBN = spotPrecision.mul(collateralAvailable).div(oracle.price);
		}
		logger.info(
			`Subaccount ${subAccountToLiqSpot}: BorrowValue: ${borrowValue}, accountValue: ${subAccountValue}, collateralAvailable: ${convertToNumber(
				collateralAvailable,
				QUOTE_PRECISION
			)}, amountToLiq: ${convertToNumber(amountToLiqBN, spotPrecision)}`
		);

		const ix = await this.driftClient.getLiquidateSpotIx(
			user.userAccountPublicKey,
			user.getUserAccount(),
			depositMarketIndextoLiq,
			borrowMarketIndextoLiq,
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
				`Error in liquidateSpot for userAccount ${user.userAccountPublicKey.toBase58()} in spot market ${borrowMarketIndextoLiq}`
			);

			const errorCode = handleSimResultError(
				simResult,
				errorCodesToSuppress,
				`${this.name}: liquidateSpot`
			);
			if (errorCode && !errorCodesToSuppress.includes(errorCode)) {
				webhookMessage(
					`[${
						this.name
					}]: :x: error in liquidateSpot for userAccount ${user.userAccountPublicKey.toBase58()}: \n${
						simResult.simTxLogs ? simResult.simTxLogs.join('\n') : ''
					}`
				);
			}
		} else {
			const resp = await this.driftClient.txSender.sendVersionedTransaction(
				simResult.tx,
				undefined,
				this.driftClient.opts
			);
			logger.info(
				`did liquidateSpot for ${user.userAccountPublicKey.toBase58()} in spot market ${borrowMarketIndextoLiq} tx: ${
					resp.txSig
				} `
			);

			if (this.liquidatorConfig.notifyOnLiquidation) {
				webhookMessage(
					`[${
						this.name
					}]: liquidateBorrow for userAccount ${user.userAccountPublicKey.toBase58()} in spot market ${borrowMarketIndextoLiq} tx: ${
						resp.txSig
					} `
				);
			}
		}
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
	) {
		logger.info(
			`liqPerpPnl: ${user.userAccountPublicKey.toBase58()} depositAmountToLiq: ${depositAmountToLiq.toString()} (depMktIndex: ${depositMarketIndextoLiq}). borrowAmountToLiq: ${borrowAmountToLiq.toString()} (brwMktIndex: ${borrowMarketIndextoLiq}). liqeePositionQuoteAmount: ${liquidateePosition.quoteAssetAmount.toNumber()} (perpMktIdx: ${
				perpMarketAccount.marketIndex
			})`
		);

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
						}`
					);

					const errorCode = handleSimResultError(
						simResult,
						errorCodesToSuppress,
						`${this.name}: settlePnl`
					);
					if (errorCode && !errorCodesToSuppress.includes(errorCode)) {
						webhookMessage(
							`[${
								this.name
							}]: :x: error in settlePnl for userAccount ${user.userAccountPublicKey.toBase58()}: \n${
								simResult.simTxLogs ? simResult.simTxLogs.join('\n') : ''
							}`
						);
					}
				} else {
					const resp = await this.driftClient.txSender.sendVersionedTransaction(
						simResult.tx,
						undefined,
						this.driftClient.opts
					);
					logger.info(
						`did settlePnl for ${user.userAccountPublicKey.toBase58()} in perp market ${
							liquidateePosition.marketIndex
						} tx: ${resp.txSig} `
					);
				}

				return;
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
					return;
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
						`Error in liquidateBorrowForPerpPnl for userAccount ${user.userAccountPublicKey.toBase58()} in spot market ${borrowMarketIndextoLiq}`
					);

					const errorCode = handleSimResultError(
						simResult,
						errorCodesToSuppress,
						`${this.name}: liquidateBorrowForPerpPnl`
					);
					if (errorCode && !errorCodesToSuppress.includes(errorCode)) {
						webhookMessage(
							`[${
								this.name
							}]: :x: error in liquidateBorrowForPerpPnl for userAccount ${user.userAccountPublicKey.toBase58()}: \n${
								simResult.simTxLogs ? simResult.simTxLogs.join('\n') : ''
							}`
						);
					}
				} else {
					const resp = await this.driftClient.txSender.sendVersionedTransaction(
						simResult.tx,
						undefined,
						this.driftClient.opts
					);
					logger.info(
						`did liquidateBorrowForPerpPnl for ${user.userAccountPublicKey.toBase58()} in spot market ${borrowMarketIndextoLiq} tx: ${
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
				// logger.info(
				// 	`skipping liquidatePerpPnlForDeposit of ${user.userAccountPublicKey.toBase58()} on spot market ${depositMarketIndextoLiq} because there is a safer perp/spot tier. perp tier ${perpTier} safestPerpTier ${safestPerpTier} safestSpotTier ${safestSpotTier}`
				// );
				return;
			}

			if (!this.spotMarketIndicies.includes(depositMarketIndextoLiq)) {
				logger.info(
					`skipping liquidatePerpPnlForDeposit of ${user.userAccountPublicKey.toBase58()} on spot market ${depositMarketIndextoLiq} because it is not in spotMarketIndices`
				);
				return;
			}

			const subAccountToTakeOverPerpPnl = this.getSubAccountIdToLiquidatePerp(
				liquidateePosition.marketIndex
			);
			if (subAccountToTakeOverPerpPnl === undefined) {
				return;
			}

			try {
				const ix = await this.driftClient.getLiquidatePerpPnlForDepositIx(
					user.userAccountPublicKey,
					user.getUserAccount(),
					liquidateePosition.marketIndex,
					depositMarketIndextoLiq,
					depositAmountToLiq,
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
						}`
					);
					const errorCode = handleSimResultError(
						simResult,
						errorCodesToSuppress,
						`${this.name}: liquidatePerpPnlForDeposit`
					);
					if (errorCode && !errorCodesToSuppress.includes(errorCode)) {
						webhookMessage(
							`[${
								this.name
							}]: :x: error in liquidatePerpPnlForDeposit for userAccount ${user.userAccountPublicKey.toBase58()} on market ${
								liquidateePosition.marketIndex
							}: \n${simResult.simTxLogs ? simResult.simTxLogs.join('\n') : ''}`
						);
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
					logger.info(
						`did liquidatePerpPnlForDeposit for ${user.userAccountPublicKey.toBase58()} on market ${
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
	}

	private findSortedUsers(): {
		usersCanBeLiquidated: Array<{
			user: User;
			userKey: string;
			marginRequirement: BN;
			canBeLiquidated: boolean;
		}>;
		checkedUsers: number;
		liquidatableUsers: number;
	} {
		const usersCanBeLiquidated = new Array<{
			user: User;
			userKey: string;
			marginRequirement: BN;
			canBeLiquidated: boolean;
		}>();

		let checkedUsers = 0;
		let liquidatableUsers = 0;
		for (const user of this.userMap!.values()) {
			checkedUsers++;
			const { canBeLiquidated, marginRequirement } = user.canBeLiquidated();
			if (canBeLiquidated || user.isBeingLiquidated()) {
				liquidatableUsers++;
				const userKey = user.userAccountPublicKey.toBase58();
				if (this.excludedAccounts.has(userKey)) {
					// Debug log precisely because the intent is to avoid noise
					logger.debug(
						`Skipping liquidation for ${userKey} due to configuration`
					);
				} else {
					usersCanBeLiquidated.push({
						user,
						userKey,
						marginRequirement,
						canBeLiquidated,
					});
				}
			}
		}

		// sort the usersCanBeLiquidated by marginRequirement, largest to smallest
		usersCanBeLiquidated.sort((a, b) => {
			return b.marginRequirement.gt(a.marginRequirement) ? 1 : -1;
		});

		return {
			usersCanBeLiquidated,
			checkedUsers,
			liquidatableUsers,
		};
	}

	private async tryLiquidate(): Promise<{
		checkedUsers: number;
		liquidatableUsers: number;
		ran: boolean;
	}> {
		const { usersCanBeLiquidated, checkedUsers, liquidatableUsers } =
			this.findSortedUsers();

		for (const {
			user,
			userKey,
			marginRequirement: _marginRequirement,
			canBeLiquidated,
		} of usersCanBeLiquidated) {
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
						continue;
					} else {
						this.throttledUsers.delete(userKey);
					}
				}

				logger.info(`liquidating auth: ${auth}, userAccount: ${userKey}...`);
				if (this.liquidatorConfig.notifyOnLiquidation) {
					webhookMessage(
						`[${this.name}]: liquidating auth: ${auth}: userAccount: ${userKey} ...`
					);
				}

				const liquidatorUser = this.driftClient.getUser();
				const liquidateeUserAccount = user.getUserAccount();

				// most attractive spot market liq
				const {
					bestIndex: depositMarketIndextoLiq,
					bestAmount: depositAmountToLiq,
					indexWithMaxAssets,
					indexWithOpenOrders,
				} = findBestSpotPosition(
					this.driftClient,
					user,
					liquidatorUser,
					liquidateeUserAccount.spotPositions,
					false,
					this.maxPositionTakeoverPctOfCollateralNum,
					this.maxPositionTakeoverPctOfCollateralDenom,
					this.minDepositToLiq
				);

				const {
					bestIndex: borrowMarketIndextoLiq,
					bestAmount: borrowAmountToLiq,
				} = findBestSpotPosition(
					this.driftClient,
					user,
					liquidatorUser,
					liquidateeUserAccount.spotPositions,
					true,
					this.maxPositionTakeoverPctOfCollateralNum,
					this.maxPositionTakeoverPctOfCollateralDenom,
					this.minDepositToLiq
				);

				let liquidateeHasSpotPos = false;
				if (borrowMarketIndextoLiq != -1 && depositMarketIndextoLiq != -1) {
					liquidateeHasSpotPos = true;
					await this.liqBorrow(
						depositMarketIndextoLiq,
						borrowMarketIndextoLiq,
						borrowAmountToLiq,
						user
					);
				}

				const usdcMarket = this.driftClient.getSpotMarketAccount(
					QUOTE_SPOT_MARKET_INDEX
				);
				if (!usdcMarket) {
					logger.error(`no usdcMarket for ${QUOTE_SPOT_MARKET_INDEX}`);
					continue;
				}

				// less attractive, perp / perp pnl liquidations
				let liquidateeHasPerpPos = false;
				let liquidateeHasUnsettledPerpPnl = false;
				let liquidateeHasLpPos = false;
				let liquidateePerpHasOpenOrders = false;
				let liquidateePerpIndexWithOpenOrders = -1;

				// shuffle user perp positions to get good position coverage in case some
				// positions put the liquidator into a bad state.
				const userPerpPositions = user.getActivePerpPositions();
				userPerpPositions.sort(() => Math.random() - 0.5);

				for (const liquidateePosition of userPerpPositions) {
					if (liquidateePosition.openOrders > 0) {
						liquidateePerpHasOpenOrders = true;
						liquidateePerpIndexWithOpenOrders = liquidateePosition.marketIndex;
					}

					liquidateeHasUnsettledPerpPnl =
						liquidateePosition.baseAssetAmount.isZero() &&
						!liquidateePosition.quoteAssetAmount.isZero();
					liquidateeHasPerpPos =
						!liquidateePosition.baseAssetAmount.isZero() ||
						!liquidateePosition.quoteAssetAmount.isZero();
					liquidateeHasLpPos = !liquidateePosition.lpShares.isZero();

					if (liquidateeHasUnsettledPerpPnl && depositMarketIndextoLiq > 0) {
						const perpMarket = this.driftClient.getPerpMarketAccount(
							liquidateePosition.marketIndex
						);
						if (!perpMarket) {
							logger.error(
								`no perpMarket for ${liquidateePosition.marketIndex}`
							);
							continue;
						}
						await this.liqPerpPnl(
							user,
							perpMarket,
							usdcMarket,
							liquidateePosition,
							depositMarketIndextoLiq,
							depositAmountToLiq,
							borrowMarketIndextoLiq,
							borrowAmountToLiq
						);
					}

					const baseAmountToLiquidate = this.calculateBaseAmountToLiquidate(
						liquidatorUser,
						user.getPerpPositionWithLPSettle(
							liquidateePosition.marketIndex,
							liquidateePosition
						)[0]
					);

					const subAccountToLiqPerp = this.getSubAccountIdToLiquidatePerp(
						liquidateePosition.marketIndex
					);
					if (subAccountToLiqPerp === undefined) {
						continue;
					}

					if (baseAmountToLiquidate.gt(ZERO)) {
						if (this.dryRun) {
							logger.warn('--dry run flag enabled - not sending liquidate tx');
							continue;
						}

						const ix = await this.driftClient.getLiquidatePerpIx(
							user.userAccountPublicKey,
							user.getUserAccount(),
							liquidateePosition.marketIndex,
							baseAmountToLiquidate,
							undefined,
							subAccountToLiqPerp
						);
						const simResult =
							await this.buildVersionedTransactionWithSimulatedCus(
								[ix],
								[this.driftLookupTables!],
								Math.floor(this.priorityFeeSubscriber.getCustomStrategyResult())
							);
						if (simResult.simError !== null) {
							logger.error(
								`Error in liquidatePerp for userAccount ${user.userAccountPublicKey.toBase58()} on market ${
									liquidateePosition.marketIndex
								}`
							);
							const errorCode = handleSimResultError(
								simResult,
								errorCodesToSuppress,
								`${this.name}: liquidatePerp`
							);
							if (errorCode && !errorCodesToSuppress.includes(errorCode)) {
								webhookMessage(
									`[${
										this.name
									}]: :x: error in liquidatePerp for userAccount ${user.userAccountPublicKey.toBase58()} on market ${
										liquidateePosition.marketIndex
									}: \n${
										simResult.simTxLogs ? simResult.simTxLogs.join('\n') : ''
									}`
								);
							} else {
								this.throttledUsers.set(
									user.userAccountPublicKey.toBase58(),
									Date.now()
								);
							}
						} else {
							const resp =
								await this.driftClient.txSender.sendVersionedTransaction(
									simResult.tx,
									undefined,
									this.driftClient.opts
								);
							logger.info(
								`did liquidatePerp for ${user.userAccountPublicKey.toBase58()} on market ${
									liquidateePosition.marketIndex
								} tx: ${resp.txSig} `
							);
							if (this.liquidatorConfig.notifyOnLiquidation) {
								webhookMessage(
									`[${
										this.name
									}]: liquidatePerp for ${user.userAccountPublicKey.toBase58()} on market ${
										liquidateePosition.marketIndex
									} tx: ${resp.txSig} `
								);
							}
						}
					} else if (liquidateeHasLpPos) {
						logger.info(
							`liquidatePerp ${auth}-${user.userAccountPublicKey.toBase58()} on market ${
								liquidateePosition.marketIndex
							} has lp shares but no perp pos, trying to clear it:`
						);

						const ix = await this.driftClient.getLiquidatePerpIx(
							user.userAccountPublicKey,
							user.getUserAccount(),
							liquidateePosition.marketIndex,
							ZERO,
							undefined,
							subAccountToLiqPerp
						);
						const simResult =
							await this.buildVersionedTransactionWithSimulatedCus(
								[ix],
								[this.driftLookupTables!],
								Math.floor(this.priorityFeeSubscriber.getCustomStrategyResult())
							);
						if (simResult.simError !== null) {
							logger.error(
								`Error in liquidatePerp (with lp shares and no perp pos) for userAccount ${user.userAccountPublicKey.toBase58()} on market ${
									liquidateePosition.marketIndex
								}`
							);
							const errorCode = handleSimResultError(
								simResult,
								errorCodesToSuppress,
								`${this.name}: liquidatePerp`
							);
							if (errorCode && !errorCodesToSuppress.includes(errorCode)) {
								webhookMessage(
									`[${
										this.name
									}]: :x: error in liquidatePerp (with lp shares and no perp pos) for userAccount ${user.userAccountPublicKey.toBase58()} on market ${
										liquidateePosition.marketIndex
									}: \n${
										simResult.simTxLogs ? simResult.simTxLogs.join('\n') : ''
									}`
								);
							} else {
								this.throttledUsers.set(
									user.userAccountPublicKey.toBase58(),
									Date.now()
								);
							}
						} else {
							const resp =
								await this.driftClient.txSender.sendVersionedTransaction(
									simResult.tx,
									undefined,
									this.driftClient.opts
								);
							logger.info(
								`did liquidatePerp (with lp shares and no perp pos) for ${user.userAccountPublicKey.toBase58()} on market ${
									liquidateePosition.marketIndex
								} tx: ${resp.txSig} `
							);
							if (this.liquidatorConfig.notifyOnLiquidation) {
								webhookMessage(
									`[${
										this.name
									}]: liquidatePerp (with lp shares and no perp pos) for ${user.userAccountPublicKey.toBase58()} on market ${
										liquidateePosition.marketIndex
									} tx: ${resp.txSig} `
								);
							}
						}
					}
				}

				if (!liquidateeHasSpotPos && !liquidateeHasPerpPos) {
					logger.info(
						`${auth}-${user.userAccountPublicKey.toBase58()} can be liquidated but has no positions`
					);
					if (liquidateePerpHasOpenOrders) {
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
							continue;
						}

						const ix = await this.driftClient.getLiquidatePerpIx(
							user.userAccountPublicKey,
							user.getUserAccount(),
							liquidateePerpIndexWithOpenOrders,
							ZERO,
							undefined,
							subAccountToLiqPerp
						);
						const simResult =
							await this.buildVersionedTransactionWithSimulatedCus(
								[ix],
								[this.driftLookupTables!],
								Math.floor(this.priorityFeeSubscriber.getCustomStrategyResult())
							);
						if (simResult.simError !== null) {
							logger.error(
								`Error in liquidatePerp (with open orders and no perp pos) for userAccount ${user.userAccountPublicKey.toBase58()} on market ${liquidateePerpIndexWithOpenOrders}`
							);
							const errorCode = handleSimResultError(
								simResult,
								errorCodesToSuppress,
								`${this.name}: liquidatePerp`
							);
							if (errorCode && !errorCodesToSuppress.includes(errorCode)) {
								webhookMessage(
									`[${
										this.name
									}]: :x: error in liquidatePerp (with open orders and no perp pos) for userAccount ${user.userAccountPublicKey.toBase58()} on market ${liquidateePerpIndexWithOpenOrders}: \n${
										simResult.simTxLogs ? simResult.simTxLogs.join('\n') : ''
									}`
								);
							} else {
								this.throttledUsers.set(
									user.userAccountPublicKey.toBase58(),
									Date.now()
								);
							}
						} else {
							const resp =
								await this.driftClient.txSender.sendVersionedTransaction(
									simResult.tx,
									undefined,
									this.driftClient.opts
								);
							logger.info(
								`did liquidatePerp (with open orders and no perp pos) for ${user.userAccountPublicKey.toBase58()} on market ${liquidateePerpIndexWithOpenOrders} tx: ${
									resp.txSig
								} `
							);
							if (this.liquidatorConfig.notifyOnLiquidation) {
								webhookMessage(
									`[${
										this.name
									}]: liquidatePerp (with open orders and no perp pos) for ${user.userAccountPublicKey.toBase58()} on market ${liquidateePerpIndexWithOpenOrders} tx: ${
										resp.txSig
									} `
								);
							}
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
						logger.info(
							`skipping liquidateSpot call for ${user.userAccountPublicKey.toBase58()} on market with open orders ${indexWithOpenOrders}`
						);
						if (subAccountToLiqSpot === undefined) {
							break;
						}
						const ix = await this.driftClient.getLiquidateSpotIx(
							user.userAccountPublicKey,
							user.getUserAccount(),
							indexWithMaxAssets,
							indexWithOpenOrders,
							ZERO,
							undefined,
							subAccountToLiqSpot
						);
						const simResult =
							await this.buildVersionedTransactionWithSimulatedCus(
								[ix],
								[this.driftLookupTables!],
								Math.floor(this.priorityFeeSubscriber.getCustomStrategyResult())
							);
						if (simResult.simError !== null) {
							logger.error(
								`Error in liquidateSpot for userAccount ${user.userAccountPublicKey.toBase58()} on market ${indexWithOpenOrders}
								}`
							);
							const errorCode = handleSimResultError(
								simResult,
								errorCodesToSuppress,
								`${this.name}: liquidatePerp`
							);
							if (errorCode && !errorCodesToSuppress.includes(errorCode)) {
								webhookMessage(
									`[${
										this.name
									}]: :x: error in liquidateSpot for userAccount ${user.userAccountPublicKey.toBase58()} on market ${indexWithOpenOrders}: \n${
										simResult.simTxLogs ? simResult.simTxLogs.join('\n') : ''
									}`
								);
							} else {
								this.throttledUsers.set(
									user.userAccountPublicKey.toBase58(),
									Date.now()
								);
							}
						} else {
							const resp =
								await this.driftClient.txSender.sendVersionedTransaction(
									simResult.tx,
									undefined,
									this.driftClient.opts
								);
							logger.info(
								`did liquidateSpot for ${user.userAccountPublicKey.toBase58()} on market ${indexWithOpenOrders} tx: ${
									resp.txSig
								} `
							);
							if (this.liquidatorConfig.notifyOnLiquidation) {
								webhookMessage(
									`[${
										this.name
									}]: liquidateSpot for ${user.userAccountPublicKey.toBase58()} on market ${indexWithOpenOrders} tx: ${
										resp.txSig
									} `
								);
							}
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
				const ix = await this.driftClient.getLiquidatePerpIx(
					user.userAccountPublicKey,
					user.getUserAccount(),
					0,
					ZERO,
					undefined
				);
				const simResult = await this.buildVersionedTransactionWithSimulatedCus(
					[ix],
					[this.driftLookupTables!],
					Math.floor(this.priorityFeeSubscriber.getCustomStrategyResult())
				);
				if (simResult.simError !== null) {
					logger.error(
						`Error in liquidatePerp for userAccount ${user.userAccountPublicKey.toBase58()} stuck in liq status
						}`
					);
					const errorCode = handleSimResultError(
						simResult,
						errorCodesToSuppress,
						`${this.name}: liquidatePerp`
					);
					if (errorCode && !errorCodesToSuppress.includes(errorCode)) {
						webhookMessage(
							`[${
								this.name
							}]: :x: error in liquidatePerp for userAccount ${user.userAccountPublicKey.toBase58()} stuck in liq status
							}: \n${simResult.simTxLogs ? simResult.simTxLogs.join('\n') : ''}`
						);
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
					logger.info(
						`did liquidatePerp for ${user.userAccountPublicKey.toBase58()} stuck in liq status tx: ${
							resp.txSig
						} `
					);
				}
			}
		}
		return {
			ran: true,
			checkedUsers,
			liquidatableUsers,
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
		try {
			({ ran, checkedUsers, liquidatableUsers } = await this.tryLiquidate());
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
				logger.debug(
					`${
						this.name
					} Bot checked ${checkedUsers} users, ${liquidatableUsers} liquidateable took ${
						Date.now() - start
					}ms to run`
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

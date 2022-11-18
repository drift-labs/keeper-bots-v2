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
	calculateClaimablePnl,
	calculateMarketAvailablePNL,
	SerumFulfillmentConfigMap,
	initialize,
	DriftEnv,
	getMarketOrderParams,
	findDirectionToClose,
	getSignedTokenAmount,
	standardizeBaseAssetAmount,
	TEN_THOUSAND,
	WrappedEvent,
	PositionDirection,
	BulkAccountLoader,
} from '@drift-labs/sdk';
import { Mutex } from 'async-mutex';

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
import { Bot } from '../types';
import { RuntimeSpec, metricAttrFromUserAccount } from '../metrics';

const USER_MAP_RESYNC_COOLDOWN_SLOTS = 10;

function calculateSpotTokenAmountToLiquidate(
	clearingHouse: DriftClient,
	liquidatorUser: User,
	liquidateePosition: SpotPosition,
	MAX_POSITION_TAKEOVER_PCT_OF_COLLATERAL: BN,
	MAX_POSITION_TAKEOVER_PCT_OF_COLLATERAL_DENOM: BN
): BN {
	const spotMarket = clearingHouse.getSpotMarketAccount(
		liquidateePosition.marketIndex
	);

	const tokenPrecision = new BN(10 ** spotMarket.decimals);

	const oraclePrice = clearingHouse.getOracleDataForSpotMarket(
		liquidateePosition.marketIndex
	).price;
	const collateralToSpend = liquidatorUser
		.getFreeCollateral()
		.mul(PRICE_PRECISION)
		.mul(MAX_POSITION_TAKEOVER_PCT_OF_COLLATERAL)
		.mul(tokenPrecision);
	const tokenAmountToLiquidate = collateralToSpend.div(
		oraclePrice
			.mul(QUOTE_PRECISION)
			.mul(MAX_POSITION_TAKEOVER_PCT_OF_COLLATERAL_DENOM)
	);

	const liquidateeTokenAmount = getTokenAmount(
		liquidateePosition.scaledBalance,
		spotMarket,
		liquidateePosition.balanceType
	);

	if (tokenAmountToLiquidate.gt(liquidateeTokenAmount)) {
		return liquidateeTokenAmount;
	} else {
		return tokenAmountToLiquidate;
	}
}

function findBestSpotPosition(
	clearingHouse: DriftClient,
	liquidatorUser: User,
	spotPositions: SpotPosition[],
	isBorrow: boolean,
	positionTakerOverPctNumerator: BN,
	positionTakerOverPctDenominator: BN
): [number, BN] {
	let bestIndex = -1;
	let bestAmount = ZERO;
	let currentAstWeight = 0;
	let currentLibWeight = Number.MAX_VALUE;

	for (const position of spotPositions) {
		if (position.scaledBalance.eq(ZERO)) {
			continue;
		}

		if (
			(isBorrow && isVariant(position.balanceType, 'deposit')) ||
			(!isBorrow && isVariant(position.balanceType, 'borrow'))
		) {
			continue;
		}

		const spotMarket = clearingHouse.getSpotMarketAccount(position.marketIndex);
		const tokenAmount = calculateSpotTokenAmountToLiquidate(
			clearingHouse,
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

	return [bestIndex, bestAmount];
}

async function liqPerpPnl(
	driftClient: DriftClient,
	user: User,
	perpMarketAccount: PerpMarketAccount,
	usdcAccount: SpotMarketAccount,
	sdkCallDurationHistogram: Histogram,
	liquidateePosition: PerpPosition,
	depositMarketIndextoLiq: number,
	depositAmountToLiq: BN,
	borrowMarketIndextoLiq: number,
	borrowAmountToLiq: BN
) {
	if (liquidateePosition.quoteAssetAmount.gt(ZERO)) {
		const claimablePnl = calculateClaimablePnl(
			perpMarketAccount,
			usdcAccount,
			liquidateePosition,
			undefined
		);

		let frac = new BN(100000000);
		if (claimablePnl.gt(ZERO)) {
			frac = BN.max(
				liquidateePosition.quoteAssetAmount.div(claimablePnl),
				new BN(1)
			);
		}

		if (frac.lt(new BN(100000000))) {
			const start = Date.now();
			driftClient
				.liquidateBorrowForPerpPnl(
					user.userAccountPublicKey,
					user.getUserAccount(),
					liquidateePosition.marketIndex,
					borrowMarketIndextoLiq,
					borrowAmountToLiq.div(frac)
				)
				.then((tx) => {
					logger.info(`liquidateBorrowForPerpPnl tx: ${tx}`);
				})
				.catch((e) => {
					logger.error('Error in liquidateBorrowForPerpPnl');
					logger.error(e);
				})
				.finally(() => {
					sdkCallDurationHistogram.record(Date.now() - start, {
						method: 'liquidateBorrowForPerpPnl',
					});
				});
		} else {
			logger.info(
				`claimablePnl=${claimablePnl.toString()} << liquidateePosition.quoteAssetAmount=${liquidateePosition.quoteAssetAmount.toString()} `
			);
			logger.info(`skipping liquidateBorrowForPerpPnl`);
		}
	} else {
		const start = Date.now();
		driftClient
			.liquidatePerpPnlForDeposit(
				user.userAccountPublicKey,
				user.getUserAccount(),
				liquidateePosition.marketIndex,
				depositMarketIndextoLiq,
				depositAmountToLiq
			)
			.then((tx) => {
				logger.info(`did liquidatePerpPnlForDeposit tx: ${tx}`);
			})
			.catch((e) => {
				console.error(e);
				logger.error('Error in liquidatePerpPnlForDeposit');
			})
			.finally(() => {
				sdkCallDurationHistogram.record(Date.now() - start, {
					method: 'liquidatePerpPnlForDeposit',
				});
			});
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
 * will spend at most MAX_POSITION_TAKEOVER_PCT_OF_COLLATERAL of its free collateral on any endangered account.
 *
 * The bot will immediately market sell any of its open positions if SELL_OPEN_POSITIONS is true.
 */
export class LiquidatorBot implements Bot {
	public readonly name: string;
	public readonly dryRun: boolean;
	public readonly defaultIntervalMs: number = 5000;

	private metricsInitialized = false;
	private metricsPort: number | undefined;
	private meter: Meter;
	private exporter: PrometheusExporter;

	// metrics
	private runtimeSpecsGauge: ObservableGauge;
	private totalLeverage: ObservableGauge;
	private totalCollateral: ObservableGauge;
	private freeCollateral: ObservableGauge;
	private perpPositionValue: ObservableGauge;
	private perpPositionBase: ObservableGauge;
	private perpPositionQuote: ObservableGauge;
	private initialMarginRequirement: ObservableGauge;
	private maintenanceMarginRequirement: ObservableGauge;
	private unrealizedPnL: ObservableGauge;
	private unrealizedFundingPnL: ObservableGauge;
	private sdkCallDurationHistogram: Histogram;
	private userMapUserAccountKeysGauge: ObservableGauge;

	private bulkAccountLoader: BulkAccountLoader | undefined;
	private driftClient: DriftClient;
	private intervalIds: Array<NodeJS.Timer> = [];
	private userMap: UserMap;
	private deriskMutex = new Uint8Array(new SharedArrayBuffer(1));
	private runtimeSpecs: RuntimeSpec;
	private serumFulfillmentConfigMap: SerumFulfillmentConfigMap;

	/**
	 * Max percentage of collateral to spend on liquidating a single position.
	 */
	private MAX_POSITION_TAKEOVER_PCT_OF_COLLATERAL = new BN(50);
	private MAX_POSITION_TAKEOVER_PCT_OF_COLLATERAL_DENOM = new BN(100);

	/**
	 * Immediately sell any open positions.
	 */
	private SELL_OPEN_POSITIONS = true;

	private watchdogTimerMutex = new Mutex();
	private watchdogTimerLastPatTime = Date.now();

	private lastSlotReyncUserMapsMutex = new Mutex();
	private lastSlotResyncUserMaps = 0;

	constructor(
		name: string,
		dryRun: boolean,
		bulkAccountLoader: BulkAccountLoader | undefined,
		driftClient: DriftClient,
		runtimeSpec: RuntimeSpec,
		metricsPort?: number | undefined
	) {
		this.name = name;
		this.dryRun = dryRun;
		this.bulkAccountLoader = bulkAccountLoader;
		this.driftClient = driftClient;
		this.runtimeSpecs = runtimeSpec;
		this.serumFulfillmentConfigMap = new SerumFulfillmentConfigMap(
			this.driftClient
		);

		this.metricsPort = metricsPort;
		if (this.metricsPort) {
			this.initializeMetrics();
		}
	}

	public async init() {
		logger.info(`${this.name} initing`);
		// initialize userMap instance
		this.userMap = new UserMap(
			this.driftClient,
			this.driftClient.userAccountSubscriptionConfig
		);

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

		await this.userMap.fetchAllUsers();
	}

	public async reset() {
		for (const intervalId of this.intervalIds) {
			clearInterval(intervalId);
		}
		this.intervalIds = [];
		delete this.userMap;
	}

	public async trigger(record: WrappedEvent<any>) {
		await this.userMap.updateWithEventRecord(record);
	}

	/**
	 * Checks that userMap and userStatsMap are up in sync with , if not, signal that we should update them next block.
	 */
	private async resyncUserMapsIfRequired() {
		const stateAccount = this.driftClient.getStateAccount();
		const resyncRequired =
			this.userMap.size() !== stateAccount.numberOfSubAccounts.toNumber();

		if (resyncRequired) {
			await this.lastSlotReyncUserMapsMutex.runExclusive(async () => {
				let doResync = false;
				const start = Date.now();
				if (!this.bulkAccountLoader) {
					logger.info(`Resyncing UserMaps immediately (no BulkAccountLoader)`);
					doResync = true;
				} else {
					const nextResyncSlot =
						this.lastSlotResyncUserMaps + USER_MAP_RESYNC_COOLDOWN_SLOTS;
					if (nextResyncSlot >= this.bulkAccountLoader.mostRecentSlot) {
						logger.info(
							`Resyncing UserMaps in cooldown, ${
								nextResyncSlot - this.bulkAccountLoader.mostRecentSlot
							} more slots to go`
						);
						return;
					} else {
						logger.info(`Resyncing UserMaps`);
						doResync = true;
						this.lastSlotResyncUserMaps = this.bulkAccountLoader.mostRecentSlot;
					}
				}

				if (doResync) {
					delete this.userMap;
					this.userMap = new UserMap(
						this.driftClient,
						this.driftClient.userAccountSubscriptionConfig
					);
					await this.userMap.fetchAllUsers();
					logger.info(`UserMaps resynced in ${Date.now() - start}ms`);
				}
			});
		}
	}

	public async startIntervalLoop(intervalMs: number): Promise<void> {
		this.tryLiquidate();
		const intervalId = setInterval(this.tryLiquidate.bind(this), intervalMs);
		this.intervalIds.push(intervalId);

		const deRiskIntervalId = setInterval(this.derisk.bind(this), 10000);
		this.intervalIds.push(deRiskIntervalId);

		logger.info(`${this.name} Bot started!`);

		const freeCollateral = this.driftClient.getUser().getFreeCollateral();
		logger.info(
			`${this.name} free collateral: $${convertToNumber(
				freeCollateral,
				QUOTE_PRECISION
			)}, spending at most ${
				(this.MAX_POSITION_TAKEOVER_PCT_OF_COLLATERAL.toNumber() /
					this.MAX_POSITION_TAKEOVER_PCT_OF_COLLATERAL_DENOM.toNumber()) *
				100.0
			}% per liquidation`
		);
	}

	public async healthCheck(): Promise<boolean> {
		let healthy = false;

		// check if we've ran the main loop recently
		await this.watchdogTimerMutex.runExclusive(async () => {
			healthy =
				this.watchdogTimerLastPatTime > Date.now() - 2 * this.defaultIntervalMs;
		});
		return healthy;
	}

	public viewDlob(): undefined {
		return undefined;
	}

	/**
	 * attempts to close out any open positions on this account. It starts by cancelling any open orders
	 */
	private async derisk() {
		if (Atomics.compareExchange(this.deriskMutex, 0, 0, 1) === 1) {
			return;
		}

		if (!this.SELL_OPEN_POSITIONS) {
			return;
		}

		try {
			const userAccount = this.driftClient.getUserAccount();

			// close open positions
			for (const position of userAccount.perpPositions) {
				if (!position.baseAssetAmount.isZero()) {
					const positionPlusOpenOrders = position.baseAssetAmount.gt(ZERO)
						? position.baseAssetAmount.add(position.openAsks)
						: position.baseAssetAmount.add(position.openBids);

					// check if open orders already net out with current position before placing new order
					if (
						position.baseAssetAmount.gt(ZERO) &&
						positionPlusOpenOrders.lte(ZERO)
					) {
						continue;
					}

					if (
						position.baseAssetAmount.lt(ZERO) &&
						positionPlusOpenOrders.gte(ZERO)
					) {
						continue;
					}

					const start = Date.now();
					this.driftClient
						.placePerpOrder(
							getMarketOrderParams({
								direction: findDirectionToClose(position),
								baseAssetAmount: positionPlusOpenOrders,
								reduceOnly: true,
								marketIndex: position.marketIndex,
							})
						)
						.then((tx) => {
							logger.info(
								`placePerpOrder on market ${position.marketIndex.toString()}: ${tx}`
							);
						})
						.catch((e) => {
							logger.error(e);
							logger.error(
								`Error trying to close perp position for market ${position.marketIndex}`
							);
						})
						.then(() => {
							this.sdkCallDurationHistogram.record(Date.now() - start, {
								method: 'placePerpOrder',
							});
						});
				} else if (position.quoteAssetAmount.lt(ZERO)) {
					const start = Date.now();
					this.driftClient
						.settlePNL(
							await this.driftClient.getUserAccountPublicKey(),
							userAccount,
							position.marketIndex
						)
						.then((tx) => {
							logger.info(
								`settling negative perp pnl on market ${position.marketIndex.toString()}: ${tx}`
							);
						})
						.catch((e) => {
							logger.error(e);
							logger.error(
								`Error trying to settle negative perp pnl for market ${position.marketIndex}`
							);
						})
						.finally(() => {
							this.sdkCallDurationHistogram.record(Date.now() - start, {
								method: 'settlePNL',
							});
						});
				} else if (position.quoteAssetAmount.gt(ZERO)) {
					const availablePnl = calculateMarketAvailablePNL(
						this.driftClient.getPerpMarketAccount(position.marketIndex),
						this.driftClient.getQuoteSpotMarketAccount()
					);

					if (availablePnl.gt(ZERO)) {
						const start = Date.now();
						this.driftClient
							.settlePNL(
								await this.driftClient.getUserAccountPublicKey(),
								userAccount,
								position.marketIndex
							)
							.then((tx) => {
								logger.info(
									`settling positive perp pnl on market ${position.marketIndex.toString()}: ${tx}`
								);
							})
							.catch((e) => {
								logger.error(e);
								logger.error(
									`Error trying to settle positive perp pnl for market ${position.marketIndex}`
								);
							})
							.finally(() => {
								this.sdkCallDurationHistogram.record(Date.now() - start, {
									method: "'settlePNL",
								});
							});
					}
				}
			}

			for (const position of userAccount.spotPositions) {
				if (position.scaledBalance.eq(ZERO) || position.marketIndex === 0) {
					continue;
				}

				// need to standardize token amount to check if its closable via market orders
				const standardizedTokenAmount = getSignedTokenAmount(
					standardizeBaseAssetAmount(
						getTokenAmount(
							position.scaledBalance,
							this.driftClient.getSpotMarketAccount(position.marketIndex),
							position.balanceType
						),
						this.driftClient.getSpotMarketAccount(position.marketIndex)
							.orderStepSize
					),
					position.balanceType
				);

				const positionPlusOpenOrders = standardizedTokenAmount.gt(ZERO)
					? standardizedTokenAmount.add(position.openAsks)
					: standardizedTokenAmount.add(position.openBids);

				// check if open orders already net out with current position before placing new order
				if (positionPlusOpenOrders.eq(ZERO)) {
					continue;
				}

				const serumFulfillmentConfig = this.serumFulfillmentConfigMap.map.get(
					position.marketIndex
				);
				if (isVariant(position.balanceType, 'deposit')) {
					const start = Date.now();
					this.driftClient
						.placeAndTakeSpotOrder(
							getMarketOrderParams({
								marketIndex: position.marketIndex,
								direction: PositionDirection.SHORT,
								baseAssetAmount: standardizedTokenAmount,
								reduceOnly: true,
							}),
							serumFulfillmentConfig
						)
						.then((tx) => {
							logger.info(
								`closing spot position for market ${position.marketIndex.toString()}: ${tx}`
							);
						})
						.catch((e) => {
							logger.error(e);
							logger.error(
								`Error trying to close spot position for market ${position.marketIndex}`
							);
						})
						.finally(() => {
							this.sdkCallDurationHistogram.record(Date.now() - start, {
								method: 'placeAndTakeSpotOrder',
							});
						});
				} else {
					const start = Date.now();
					this.driftClient
						.placeAndTakeSpotOrder(
							getMarketOrderParams({
								marketIndex: position.marketIndex,
								direction: PositionDirection.LONG,
								baseAssetAmount: standardizedTokenAmount,
								reduceOnly: true,
							}),
							serumFulfillmentConfig
						)
						.then((tx) => {
							logger.info(
								`closing spot position for market ${position.marketIndex.toString()}: ${tx}`
							);
						})
						.catch((e) => {
							logger.error(e);
							logger.error(
								`Error trying to close spot position for market ${position.marketIndex}`
							);
						})
						.finally(() => {
							this.sdkCallDurationHistogram.record(Date.now() - start, {
								method: 'placeAndTakeSpotOrder',
							});
						});
				}
			}
		} finally {
			Atomics.store(this.deriskMutex, 0, 0);
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
			.mul(this.MAX_POSITION_TAKEOVER_PCT_OF_COLLATERAL)
			.mul(BASE_PRECISION);
		const baseAssetAmountToLiquidate = collateralToSpend.div(
			oraclePrice
				.mul(QUOTE_PRECISION)
				.mul(this.MAX_POSITION_TAKEOVER_PCT_OF_COLLATERAL_DENOM)
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

		// resolve bankrupt markets
		for (const perpIdx of bankruptPerpMarkets) {
			logger.info(
				`Resolving perp market for userAcc: ${userKey.toBase58()}, marketIndex: ${perpIdx}`
			);
			const start = Date.now();
			this.driftClient
				.resolvePerpBankruptcy(userKey, userAcc, perpIdx)
				.then((tx) => {
					logger.info(
						`Resolved perp bankruptcy for userAcc: ${userKey.toBase58()}, marketIndex: ${perpIdx}: ${tx}`
					);
				})
				.catch((e) => {
					logger.error(e);
					logger.error(
						`Error resolvePerpBankruptcy for ${userKey.toBase58()}, auth: ${userAcc.authority.toBase58()}`
					);
				})
				.finally(() => {
					this.sdkCallDurationHistogram.record(Date.now() - start, {
						method: 'resolvePerpBankruptcy',
					});
				});
		}

		for (const spotIdx of bankruptSpotMarkets) {
			logger.info(
				`Resolving spot market for userAcc: ${userKey.toBase58()}, marketIndex: ${spotIdx}`
			);
			const start = Date.now();
			this.driftClient
				.resolveSpotBankruptcy(userKey, userAcc, spotIdx)
				.then((tx) => {
					logger.info(
						`Resolved spot market for userAcc: ${userKey.toBase58()}, marketIndex: ${spotIdx}: ${tx}`
					);
				})
				.catch((e) => {
					logger.error(e);
					logger.error(
						`Error resolveSpotpBankruptcy for ${userKey.toBase58()}, auth: ${userAcc.authority.toBase58()}`
					);
				})
				.finally(() => {
					this.sdkCallDurationHistogram.record(Date.now() - start, {
						method: 'resolveSpotBankruptcy',
					});
				});
		}
	}

	/**
	 * iterates over users in userMap and checks:
	 * 		1. is user bankrupt? if so, resolve bankruptcy
	 * 		2. is user in liquidation? If so, endangered position is liquidated
	 */
	private async tryLiquidate() {
		const start = Date.now();
		let ran = false;
		try {
			await this.resyncUserMapsIfRequired();

			for (const user of this.userMap.values()) {
				const userAcc = user.getUserAccount();
				const auth = userAcc.authority.toBase58();
				const userKey = user.userAccountPublicKey.toBase58();

				if (isVariant(userAcc.status, 'bankrupt')) {
					await this.tryResolveBankruptUser(user);
				} else if (user.canBeLiquidated()) {
					logger.info(`liquidating ${auth}: ${userKey}...`);

					const liquidatorUser = this.driftClient.getUser();
					const liquidateeUserAccount = user.getUserAccount();

					// most attractive spot market liq
					const [depositMarketIndextoLiq, depositAmountToLiq] =
						findBestSpotPosition(
							this.driftClient,
							liquidatorUser,
							liquidateeUserAccount.spotPositions,
							false,
							this.MAX_POSITION_TAKEOVER_PCT_OF_COLLATERAL,
							this.MAX_POSITION_TAKEOVER_PCT_OF_COLLATERAL_DENOM
						);

					const [borrowMarketIndextoLiq, borrowAmountToLiq] =
						findBestSpotPosition(
							this.driftClient,
							liquidatorUser,
							liquidateeUserAccount.spotPositions,
							true,
							this.MAX_POSITION_TAKEOVER_PCT_OF_COLLATERAL,
							this.MAX_POSITION_TAKEOVER_PCT_OF_COLLATERAL_DENOM
						);

					if (borrowMarketIndextoLiq != -1 && depositMarketIndextoLiq != -1) {
						const start = Date.now();
						this.driftClient
							.liquidateSpot(
								user.userAccountPublicKey,
								user.getUserAccount(),
								depositMarketIndextoLiq,
								borrowMarketIndextoLiq,
								borrowAmountToLiq
							)
							.then((tx) => {
								logger.info(
									`liquidateSpot user=${user.userAccountPublicKey.toString()}
								(deposit_index=${depositMarketIndextoLiq} for borrow_index=${borrowMarketIndextoLiq}
								maxBorrowAmount=${borrowAmountToLiq.toString()})
								tx: ${tx}`
								);
							})
							.catch((e) => {
								logger.error('Error in liquidateSpot');
								logger.error(e);
							})
							.finally(() => {
								this.sdkCallDurationHistogram.record(Date.now() - start, {
									method: 'liquidateSpot',
								});
							});
					}

					const usdcMarket = this.driftClient.getSpotMarketAccount(
						QUOTE_SPOT_MARKET_INDEX
					);

					// less attractive, perp / perp pnl liquidations
					for (const liquidateePosition of liquidateeUserAccount.perpPositions) {
						if (liquidateePosition.baseAssetAmount.isZero()) {
							if (!liquidateePosition.quoteAssetAmount.isZero()) {
								const perpMarket = this.driftClient.getPerpMarketAccount(
									liquidateePosition.marketIndex
								);
								await liqPerpPnl(
									this.driftClient,
									user,
									perpMarket,
									usdcMarket,
									this.sdkCallDurationHistogram,
									liquidateePosition,
									depositMarketIndextoLiq,
									depositAmountToLiq,
									borrowMarketIndextoLiq,
									borrowAmountToLiq
								);

								break; // todo: exit loop to reload accounts etc?
							}
							continue;
						}

						const baseAmountToLiquidate = this.calculateBaseAmountToLiquidate(
							liquidatorUser,
							liquidateePosition
						);

						if (baseAmountToLiquidate.gt(ZERO)) {
							if (this.dryRun) {
								logger.warn(
									'--dry run flag enabled - not sending liquidate tx'
								);
							}
							const start = Date.now();
							this.driftClient
								.liquidatePerp(
									user.userAccountPublicKey,
									user.getUserAccount(),
									liquidateePosition.marketIndex,
									baseAmountToLiquidate
								)
								.then((tx) => {
									logger.info(`liquidatePerp tx: ${tx}`);
								})
								.catch((e) => {
									logger.error(
										`Error liquidating auth: ${auth}, user: ${userKey}`
									);
									console.error(e);
								})
								.finally(() => {
									this.sdkCallDurationHistogram.record(Date.now() - start, {
										method: 'liquidatePerp',
									});
								});
						}
					}
				}
			}
			await this.derisk();
			ran = true;
		} catch (e) {
			console.error(e);
		} finally {
			if (ran) {
				logger.debug(`${this.name} Bot took ${Date.now() - start}ms to run`);
				await this.watchdogTimerMutex.runExclusive(async () => {
					this.watchdogTimerLastPatTime = Date.now();
				});
			}
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
			obs.observe(this.userMap.size());
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
						this.totalLeverage,
						convertToNumber(user.getLeverage(), TEN_THOUSAND),
						metricAttrFromUserAccount(user.userAccountPublicKey, userAccount)
					);
					batchObservableResult.observe(
						this.totalCollateral,
						convertToNumber(user.getTotalCollateral(), QUOTE_PRECISION),
						metricAttrFromUserAccount(user.userAccountPublicKey, userAccount)
					);
					batchObservableResult.observe(
						this.freeCollateral,
						convertToNumber(user.getFreeCollateral(), QUOTE_PRECISION),
						metricAttrFromUserAccount(user.userAccountPublicKey, userAccount)
					);
					batchObservableResult.observe(
						this.perpPositionValue,
						convertToNumber(
							user.getPerpPositionValue(accMarketIdx, oracle),
							QUOTE_PRECISION
						),
						metricAttrFromUserAccount(user.userAccountPublicKey, userAccount)
					);

					const perpPosition = user.getPerpPosition(accMarketIdx);
					batchObservableResult.observe(
						this.perpPositionBase,
						convertToNumber(perpPosition.baseAssetAmount, BASE_PRECISION),
						metricAttrFromUserAccount(user.userAccountPublicKey, userAccount)
					);
					batchObservableResult.observe(
						this.perpPositionQuote,
						convertToNumber(perpPosition.quoteAssetAmount, QUOTE_PRECISION),
						metricAttrFromUserAccount(user.userAccountPublicKey, userAccount)
					);

					batchObservableResult.observe(
						this.initialMarginRequirement,
						convertToNumber(
							user.getInitialMarginRequirement(),
							QUOTE_PRECISION
						),
						metricAttrFromUserAccount(user.userAccountPublicKey, userAccount)
					);
					batchObservableResult.observe(
						this.maintenanceMarginRequirement,
						convertToNumber(
							user.getMaintenanceMarginRequirement(),
							QUOTE_PRECISION
						),
						metricAttrFromUserAccount(user.userAccountPublicKey, userAccount)
					);
					batchObservableResult.observe(
						this.unrealizedPnL,
						convertToNumber(user.getUnrealizedPNL(), QUOTE_PRECISION),
						metricAttrFromUserAccount(user.userAccountPublicKey, userAccount)
					);
					batchObservableResult.observe(
						this.unrealizedFundingPnL,
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

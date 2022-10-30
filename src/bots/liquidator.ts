import {
	BN,
	convertToNumber,
	DriftClient,
	User,
	isVariant,
	OrderRecord,
	LiquidationRecord,
	BASE_PRECISION,
	PRICE_PRECISION,
	QUOTE_PRECISION,
	NewUserRecord,
	PerpPosition,
	UserMap,
	ZERO,
} from '@drift-labs/sdk';
import { Mutex } from 'async-mutex';

import { getErrorCode } from '../error';
import { logger } from '../logger';
import { Bot } from '../types';
import { Metrics } from '../metrics';

/**
 * LiquidatorBot implements a simple liquidation bot for the Drift V2 Protocol. Liquidations work by taking over
 * a portion of the endangered account's position, so collateral is required in order to run this bot. The bot
 * will spend at most MAX_POSITION_TAKEOVER_PCT_OF_COLLATERAL of its free collateral on any endangered account.
 *
 * The bot will immediately market sell any of its open positions if SELL_OPEN_POSITIONS is true.
 */
export class PerpLiquidatorBot implements Bot {
	public readonly name: string;
	public readonly dryRun: boolean;
	public readonly defaultIntervalMs: number = 10000;

	private driftClient: DriftClient;
	private intervalIds: Array<NodeJS.Timer> = [];
	private userMap: UserMap;
	private metrics: Metrics | undefined;
	private deriskMutex = new Uint8Array(new SharedArrayBuffer(1));

	/**
	 * Max percentage of collateral to spend on liquidating a single position.
	 */
	private MAX_POSITION_TAKEOVER_PCT_OF_COLLATERAL = new BN(1);
	private MAX_POSITION_TAKEOVER_PCT_OF_COLLATERAL_DENOM = new BN(100);

	/**
	 * Immediately sell any open positions.
	 */
	private SELL_OPEN_POSITIONS = true;

	private watchdogTimerMutex = new Mutex();
	private watchdogTimerLastPatTime = Date.now();

	constructor(
		name: string,
		dryRun: boolean,
		driftClient: DriftClient,
		metrics?: Metrics | undefined
	) {
		this.name = name;
		this.dryRun = dryRun;
		this.driftClient = driftClient;
		this.metrics = metrics;
	}

	public async init() {
		logger.info(`${this.name} initing`);
		// initialize userMap instance
		this.userMap = new UserMap(
			this.driftClient,
			this.driftClient.userAccountSubscriptionConfig
		);
		await this.userMap.fetchAllUsers();
	}

	public async reset() {
		for (const intervalId of this.intervalIds) {
			clearInterval(intervalId);
		}
		this.intervalIds = [];
		delete this.userMap;
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
		await this.watchdogTimerMutex.runExclusive(async () => {
			healthy =
				this.watchdogTimerLastPatTime > Date.now() - 2 * this.defaultIntervalMs;
		});
		return healthy;
	}

	public async trigger(record: any): Promise<void> {
		if (record.eventType === 'OrderRecord') {
			await this.userMap.updateWithOrderRecord(record as OrderRecord);
		} else if (record.eventType === 'NewUserRecord') {
			await this.userMap.mustGet((record as NewUserRecord).user.toString());
		} else if (record.eventType === 'LiquidationRecord') {
			this.metrics?.recordLiquidationEvent(
				record as LiquidationRecord,
				this.name
			);
		}
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
			// cancel open orders
			let canceledOrders = 0;
			for (const order of userAccount.orders) {
				if (!isVariant(order.status, 'open')) {
					continue;
				}
				const tx = await this.driftClient.cancelOrder(order.orderId);
				logger.info(
					`${this.name} canceling open order ${
						order.orderId
					} on market ${order.marketIndex.toString()}: ${tx}`
				);
				canceledOrders++;
			}
			if (canceledOrders > 0) {
				logger.info(
					`${this.name} canceled ${canceledOrders} open orders while derisking`
				);
			}

			// close open orders
			let closedPositions = 0;
			for (const position of userAccount.perpPositions) {
				if (position.baseAssetAmount.isZero()) {
					continue;
				}
				const tx = await this.driftClient.closePosition(position.marketIndex);
				logger.info(
					`${
						this.name
					} closing position on market ${position.marketIndex.toString()}: ${tx}`
				);
				closedPositions++;
			}
			if (closedPositions > 0) {
				logger.info(
					`${this.name} closed ${closedPositions} positions while derisking`
				);
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
			if (position.quoteAssetAmount.gte(ZERO)) {
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
			const tx = await this.driftClient.resolvePerpBankruptcy(
				userKey,
				userAcc,
				perpIdx
			);
			logger.info(
				`Resolved perp market for userAcc: ${userKey.toBase58()}, marketIndex: ${perpIdx}: ${tx}`
			);
		}

		for (const spotIdx of bankruptSpotMarkets) {
			logger.info(
				`Resolving spot market for userAcc: ${userKey.toBase58()}, marketIndex: ${spotIdx}`
			);
			const tx = await this.driftClient.resolveSpotBankruptcy(
				userKey,
				userAcc,
				spotIdx
			);
			logger.info(
				`Resolved spot market for userAcc: ${userKey.toBase58()}, marketIndex: ${spotIdx}: ${tx}`
			);
		}
	}

	/**
	 * iterates over users in userMap and checks:
	 * 		1. is user bankrupt? if so, resolve bankruptcy
	 * 		2. is user in liquidation? If so, endangered position is liquidated
	 */
	private async tryLiquidate() {
		try {
			for (const user of this.userMap.values()) {
				const userAcc = user.getUserAccount();
				const auth = userAcc.authority.toBase58();
				const userKey = user.userAccountPublicKey.toBase58();

				if (userAcc.isBankrupt) {
					await this.tryResolveBankruptUser(user);
				} else if (user.canBeLiquidated()) {
					logger.info(`liquidating ${auth}: ${userKey}...`);

					const liquidatorUser = this.driftClient.getUser();

					for (const liquidateePosition of user.getUserAccount()
						.perpPositions) {
						if (liquidateePosition.baseAssetAmount.isZero()) {
							continue;
						}

						const baseAmountToLiquidate = this.calculateBaseAmountToLiquidate(
							liquidatorUser,
							liquidateePosition
						);

						if (baseAmountToLiquidate.gt(new BN(0))) {
							try {
								if (this.dryRun) {
									logger.warn(
										'--dry run flag enabled - not sending liquidate tx'
									);
								}
								const tx = await this.driftClient.liquidatePerp(
									user.userAccountPublicKey,
									user.getUserAccount(),
									liquidateePosition.marketIndex,
									baseAmountToLiquidate
								);
								logger.info(`liquidatePerp tx: ${tx}`);
								this.metrics?.recordPerpLiquidation(
									liquidatorUser.getUserAccountPublicKey(),
									user.getUserAccountPublicKey(),
									this.name
								);
							} catch (txError) {
								const errorCode = getErrorCode(txError);
								this.metrics?.recordErrorCode(
									errorCode,
									this.driftClient.provider.wallet.publicKey,
									this.name
								);
								logger.error(
									`Error liquidating auth: ${auth}, user: ${userKey}`
								);
								console.error(txError);
							}
						}
					}
				}
			}
			await this.derisk();
		} catch (e) {
			console.error(e);
		} finally {
			await this.watchdogTimerMutex.runExclusive(async () => {
				this.watchdogTimerLastPatTime = Date.now();
			});
		}
	}
}

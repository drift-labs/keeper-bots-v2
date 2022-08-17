import {
	BN,
	convertToNumber,
	ClearingHouse,
	calculateWorstCaseBaseAssetAmount,
	calculateMarketMarginRatio,
	OrderRecord,
	BASE_PRECISION,
	AMM_TO_QUOTE_PRECISION_RATIO,
	MARK_PRICE_PRECISION,
	MARGIN_PRECISION,
	QUOTE_PRECISION,
	UserPosition,
} from '@drift-labs/sdk';

import { getErrorCode } from '../error';
import { logger } from '../logger';
import { UserMap } from '../userMap';
import { Bot } from '../types';
import { Metrics } from '../metrics';

export class LiquidatorBot implements Bot {
	public readonly name: string;
	public readonly dryRun: boolean;
	public readonly defaultIntervalMs: number = 10000;

	private clearingHouse: ClearingHouse;
	private intervalIds: Array<NodeJS.Timer> = [];
	private userMap: UserMap;
	private metrics: Metrics | undefined;

	constructor(
		name: string,
		dryRun: boolean,
		clearingHouse: ClearingHouse,
		metrics?: Metrics | undefined
	) {
		this.name = name;
		this.dryRun = dryRun;
		this.clearingHouse = clearingHouse;
		this.metrics = metrics;
	}

	public async init() {
		// initialize userMap instance
		this.userMap = new UserMap(
			this.clearingHouse.connection,
			this.clearingHouse
		);
		await this.userMap.fetchAllUsers();
	}

	public reset(): void {
		for (const intervalId of this.intervalIds) {
			clearInterval(intervalId);
		}
		this.intervalIds = [];
		delete this.userMap;
	}

	public startIntervalLoop(intervalMs: number): void {
		this.tryLiquidate();
		const intervalId = setInterval(this.tryLiquidate.bind(this), intervalMs);
		this.intervalIds.push(intervalId);

		logger.info(`${this.name} Bot started!`);
	}

	public async trigger(record: OrderRecord): Promise<void> {
		await this.userMap.updateWithOrder(record);
	}

	public viewDlob(): undefined {
		return undefined;
	}

	private async tryLiquidate() {
		try {
			for (const user of this.userMap.values()) {
				const auth = user.getUserAccount().authority.toBase58();
				const userKey = user.userAccountPublicKey.toBase58();
				// console.log(`[${auth}: ${userKey}]`);
				// console.log(`  leverage: ${convertToNumber(user.getLeverage(), TEN_THOUSAND).toString()}`);
				const [canBeLiquidated, _marginRatio] = user.canBeLiquidated();
				// console.log(`  canBeLiquidated: ${canBeLiquidated}, marginRatio: ${convertToNumber(marginRatio, TEN_THOUSAND).toString()}`);
				// const bankLiabilityValue = user.getBankLiabilityValue();
				// console.log(`  bankLiabilityValue: ${convertToNumber(bankLiabilityValue, QUOTE_PRECISION).toString()}`);

				if (canBeLiquidated) {
					logger.info(`liquidating ${auth}: ${userKey}...`);
					this.clearingHouse.fetchAccounts();
					this.clearingHouse.getUser().fetchAccounts();

					const liquidatorUser = this.clearingHouse.getUser();

					for (const liquidateePosition of user.getUserAccount().positions) {
						if (liquidateePosition.baseAssetAmount.isZero()) {
							continue;
						}
						const liquidatorPosition = liquidatorUser.getUserPosition(
							liquidateePosition.marketIndex
						);

						let currentPosBaseAmount = new BN(0);
						if (liquidatorPosition !== undefined) {
							currentPosBaseAmount = liquidatorPosition.baseAssetAmount;
						}

						const market = this.clearingHouse.getMarketAccount(
							liquidateePosition.marketIndex
						);

						logger.info(
							`  liquidating position in market ${liquidateePosition.marketIndex.toString()}, size: ${convertToNumber(
								liquidateePosition.baseAssetAmount,
								BASE_PRECISION
							).toString()}`
						);
						logger.info(
							`    liquidatorPosition0: ${convertToNumber(
								currentPosBaseAmount,
								BASE_PRECISION
							).toString()}`
						);
						const newPosition: UserPosition = {
							baseAssetAmount: currentPosBaseAmount.add(
								liquidateePosition.baseAssetAmount
							),
							lastCumulativeFundingRate:
								liquidatorPosition?.lastCumulativeFundingRate,
							marketIndex: liquidatorPosition?.marketIndex,
							quoteAssetAmount: liquidatorPosition?.quoteAssetAmount,
							quoteEntryAmount: liquidatorPosition?.quoteEntryAmount,
							openOrders: liquidatorPosition
								? liquidatorPosition.openOrders
								: new BN(0),
							openBids: liquidatorPosition
								? liquidatorPosition.openBids
								: new BN(0),
							openAsks: liquidatorPosition
								? liquidatorPosition.openAsks
								: new BN(0),
						};
						logger.info(
							`    liquidatorPosition1: ${convertToNumber(
								newPosition.baseAssetAmount,
								BASE_PRECISION
							).toString()}`
						);

						// calculate margin required to take over position
						const worstCaseBaseAssetAmount =
							calculateWorstCaseBaseAssetAmount(newPosition);
						const worstCaseAssetValue = worstCaseBaseAssetAmount
							.abs()
							.mul(
								this.clearingHouse.getOracleDataForMarket(
									liquidateePosition.marketIndex
								).price
							)
							.div(AMM_TO_QUOTE_PRECISION_RATIO.mul(MARK_PRICE_PRECISION));

						const marketMarginRatio = new BN(
							calculateMarketMarginRatio(
								market,
								worstCaseBaseAssetAmount,
								'Initial'
							)
						);
						const marginRequired = worstCaseAssetValue
							.mul(marketMarginRatio)
							.div(MARGIN_PRECISION);

						const marginAvailable = liquidatorUser.getFreeCollateral();
						logger.info(
							`    marginRequired: ${convertToNumber(
								marginRequired,
								QUOTE_PRECISION
							).toString()}`
						);
						logger.info(
							`    marginAvailable: ${convertToNumber(
								marginAvailable,
								QUOTE_PRECISION
							).toString()}`
						);
						logger.info(
							`      enough collateral to liquidate??: ${marginAvailable.gte(
								marginRequired
							)}`
						);

						if (marginAvailable.gte(marginRequired)) {
							try {
								const tx = await this.clearingHouse.liquidatePerp(
									user.userAccountPublicKey,
									user.getUserAccount(),
									liquidateePosition.marketIndex,
									liquidateePosition.baseAssetAmount
								);
								logger.info(`liquidatePerp tx: ${tx}`);
								this.metrics?.recordPerpLiquidation(
									liquidatorUser.getUserAccountPublicKey(),
									user.getUserAccountPublicKey(),
									this.name
								);
							} catch (txError) {
								const errorCode = getErrorCode(txError);
								if (errorCode === 6003) {
									logger.error(
										`Liquidator has insufficient collateral to take over position.`
									);
								}
								logger.error(`Error liquidating ${auth}: ${userKey}`);
								console.error(txError);
							}
						}
					}
				}
			}
		} catch (e) {
			console.error(e);
		}
	}
}

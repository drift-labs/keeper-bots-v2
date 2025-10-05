/* eslint-disable @typescript-eslint/no-non-null-assertion */
import {
	DriftClient,
	UserMap,
	User,
	PerpMarketAccount,
	MarketType,
} from '@drift-labs/sdk';
import {
	PublicKey,
	AddressLookupTableAccount,
	ComputeBudgetProgram,
	TransactionInstruction,
} from '@solana/web3.js';
import { logger } from '../../../logger';
import {
	Queue,
	STREAM_CANDIDATES,
	STREAM_TXS,
	GROUP_COORD,
	Candidate,
	TxJob,
} from '../redis/queue';
import {
	SimulateAndGetTxWithCUsResponse,
	simulateAndGetTxWithCUs,
} from '../../../utils';
import {
	BN,
	ZERO,
	findDirectionToClose,
	getOrderParams,
	OrderType,
	deriveOracleAuctionParams,
	calculateEstimatedPerpEntryPrice,
	PositionDirection,
	getLimitOrderParams,
	standardizeBaseAssetAmount,
} from '@drift-labs/sdk';

// Minimal planner that demonstrates scanning like liquidator.ts and creating 1 tx job per opportunity.

export class CoordinatorService {
	constructor(
		private readonly driftClient: DriftClient,
		private readonly userMap: UserMap,
		private readonly queue: Queue,
		private readonly lookupTables: AddressLookupTableAccount[]
	) {}

	async init() {
		await this.queue.connect();
		await this.queue.ensureStreamAndGroup(STREAM_CANDIDATES, GROUP_COORD);
		await this.queue.ensureStreamAndGroup(STREAM_TXS, GROUP_COORD);
		logger.info('Coordinator initialized');
	}

	async scanAndEnqueueOnce(): Promise<void> {
		const { usersCanBeLiquidated } = this.findSortedUsers();
		for (const { user } of usersCanBeLiquidated) {
			try {
				const txJob = await this.planSingleUser(user);
				if (txJob) {
					await this.queue.addTxJob(STREAM_TXS, txJob);
					logger.info(
						`Enqueued tx job for ${user.userAccountPublicKey.toBase58()} action=${
							txJob.action
						} market=${txJob.marketIndex}`
					);
				}
			} catch (e) {
				logger.error(
					`plan error for ${user.userAccountPublicKey.toBase58()}: ${
						(e as Error).message
					}`
				);
			}
		}
	}

	private findSortedUsers(): {
		usersCanBeLiquidated: Array<{ user: User; canBeLiquidated: boolean }>;
	} {
		const usersCanBeLiquidated: Array<{
			user: User;
			canBeLiquidated: boolean;
		}> = [];
		for (const user of this.userMap.values()) {
			const { canBeLiquidated } = user.canBeLiquidated();
			if (canBeLiquidated || user.isBeingLiquidated()) {
				usersCanBeLiquidated.push({ user, canBeLiquidated: true });
			}
		}
		return { usersCanBeLiquidated };
	}

	private async planSingleUser(user: User): Promise<TxJob | undefined> {
		// Simple heuristic: prefer perp position close if any active perp position exists; otherwise skip.
		for (const pos of user.getActivePerpPositions()) {
			const perpMarket = this.driftClient.getPerpMarketAccount(
				pos.marketIndex
			)! as PerpMarketAccount;
			if (!pos.baseAssetAmount.isZero()) {
				const direction = findDirectionToClose(pos);
				const baseAmount = standardizeBaseAssetAmount(
					pos.baseAssetAmount.abs(),
					perpMarket.amm.orderStepSize
				);
				if (baseAmount.eq(ZERO)) continue;

				const dlob = await this.userMap.getDLOB(this.userMap.getSlot());
				const oracle = this.driftClient.getMMOracleDataForPerpMarket(
					pos.marketIndex
				);
				const { entryPrice, bestPrice } = calculateEstimatedPerpEntryPrice(
					'base',
					baseAmount,
					direction,
					perpMarket,
					oracle,
					dlob!,
					this.userMap.getSlot()
				);
				const limitPrice = entryPrice; // keep simple for v2 demo
				const { auctionStartPrice, auctionEndPrice, oraclePriceOffset } =
					deriveOracleAuctionParams({
						direction,
						oraclePrice: oracle.price,
						auctionStartPrice: bestPrice,
						auctionEndPrice: limitPrice,
						limitPrice,
					});
				const orderParams = getOrderParams({
					orderType: OrderType.ORACLE,
					direction,
					baseAssetAmount: baseAmount,
					reduceOnly: true,
					marketIndex: pos.marketIndex,
					auctionDuration: 50,
					auctionStartPrice,
					auctionEndPrice,
					oraclePriceOffset,
				});
				const placeIx = await this.driftClient.getPlacePerpOrderIx(
					orderParams,
					this.driftClient.activeSubAccountId
				);
				const cancelIx = await this.driftClient.getCancelOrdersIx(
					MarketType.PERP,
					pos.marketIndex,
					orderParams.direction,
					this.driftClient.activeSubAccountId
				);

				const sim: SimulateAndGetTxWithCUsResponse =
					await simulateAndGetTxWithCUs({
						ixs: [cancelIx, placeIx],
						connection: this.driftClient.connection,
						payerPublicKey: this.driftClient.wallet.publicKey,
						lookupTableAccounts: this.lookupTables,
						doSimulation: true,
						recentBlockhash: undefined,
						cuLimitMultiplier: 1.2,
						removeLastIxPostSim: false,
					});
				const versionedTxBase64 = Buffer.from(sim.tx.serialize()).toString(
					'base64'
				);
				const txJob: TxJob = {
					idemKey: `${user.userAccountPublicKey.toBase58()}-${
						pos.marketIndex
					}-${Date.now()}`,
					userPubkey: user.userAccountPublicKey.toBase58(),
					action: 'perp',
					marketIndex: pos.marketIndex,
					versionedTxBase64,
					cuLimit: sim.cuEstimate,
					cuPriceMicroLamports: 0,
					sendOpts: { skipPreflight: true },
					telemetry: { slot: this.userMap.getSlot(), ts: Date.now() },
				};
				return txJob;
			}
		}
		return undefined;
	}
}

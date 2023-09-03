import { ComputeBudgetProgram, TransactionInstruction } from '@solana/web3.js';
import { logger } from '../../logger';

/**
 * This class determins whether priority fee needs to be included in a transaction based on
 * a recent history of timed out transactions.
 */
export class PriorityFeeCalculator {
	lastTxTimeoutCount: number;
	priorityFeeTriggered: boolean;
	lastTxTimeoutCountTriggered: number;
	priorityFeeLatchDurationMs: number; // how long to stay in triggered state before resetting

	constructor(
		currentTimeMs: number,
		priorityFeeLatchDurationMs: number = 10 * 1000
	) {
		this.lastTxTimeoutCount = 0;
		this.priorityFeeTriggered = false;
		this.lastTxTimeoutCountTriggered = currentTimeMs;
		this.priorityFeeLatchDurationMs = priorityFeeLatchDurationMs;
	}

	/**
	 * Update the priority fee state based on the current time and the current timeout count.
	 * @param currentTimeMs current time in milliseconds
	 * @returns true if priority fee should be included in the next transaction
	 */
	public updatePriorityFee(
		currentTimeMs: number,
		txTimeoutCount: number
	): boolean {
		let triggerPriorityFee = false;

		if (txTimeoutCount > this.lastTxTimeoutCount) {
			this.lastTxTimeoutCount = txTimeoutCount;
			this.lastTxTimeoutCountTriggered = currentTimeMs;
			triggerPriorityFee = true;
		} else {
			if (!this.priorityFeeTriggered) {
				triggerPriorityFee = false;
			} else if (
				currentTimeMs - this.lastTxTimeoutCountTriggered <
				this.priorityFeeLatchDurationMs
			) {
				triggerPriorityFee = true;
			}
		}

		this.priorityFeeTriggered = triggerPriorityFee;

		return triggerPriorityFee;
	}

	public generateComputeBudgetIxs(
		computeUnitLimit: number
	): Array<TransactionInstruction> {
		const ixs = [
			ComputeBudgetProgram.setComputeUnitLimit({
				units: computeUnitLimit,
			}),
		];

		return ixs;
	}

	/**
	 *
	 * @param computeUnitLimit desired CU to use
	 * @param additionalFeeMicroLamports desired additional fee to pay, in micro lamports
	 * @returns the compute unit price to use, in micro lamports
	 */
	public calculateComputeUnitPrice(
		computeUnitLimit: number,
		additionalFeeMicroLamports: number
	): number {
		return additionalFeeMicroLamports / computeUnitLimit;
	}

	public generateComputeBudgetWithPriorityFeeIx(
		computeUnitLimit: number,
		usePriorityFee: boolean,
		additionalFeeMicroLamports: number
	): Array<TransactionInstruction> {
		const ixs = this.generateComputeBudgetIxs(computeUnitLimit);

		if (usePriorityFee) {
			const computeUnitPrice = this.calculateComputeUnitPrice(
				computeUnitLimit,
				additionalFeeMicroLamports
			);
			logger.info(
				`Using additional fee of ${additionalFeeMicroLamports}, computeUnitLimit: ${computeUnitLimit}, computeUnitPrice: ${computeUnitPrice} micro lamports`
			);
			ixs.push(
				ComputeBudgetProgram.setComputeUnitPrice({
					microLamports: computeUnitPrice,
				})
			);
		}

		return ixs;
	}
}

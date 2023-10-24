import { BN, PositionDirection } from '@drift-labs/sdk';

export const constants = {
	devnet: {
		USDCMint: '8zGuJQqwhZafTah7Uc7Z4tXRnguqkn5KLFAP8oV6PHe2',
	},
	'mainnet-beta': {
		USDCMint: 'EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v',
	},
};

export enum OrderExecutionAlgoType {
	Market = 'market',
	Twap = 'twap',
}

export type TwapExecutionConfig = {
	currentPosition: BN;
	targetPosition: BN;
	overallDurationSec: number;
	startTimeSec: number;
};

export class TwapExecutionProgress {
	amountStart: BN;
	currentPosition: BN;
	amountTarget: BN;
	overallDurationSec: number;
	startTimeSec: number;
	lastUpdateSec: number;
	lastExecSec: number;
	firstExecDone = false;

	constructor(config: TwapExecutionConfig) {
		this.amountStart = config.currentPosition;
		this.currentPosition = config.currentPosition;
		this.amountTarget = config.targetPosition;
		this.overallDurationSec = config.overallDurationSec;
		this.startTimeSec = config.startTimeSec;
		this.lastUpdateSec = this.startTimeSec;
		this.lastExecSec = this.startTimeSec;
	}

	/**
	 *
	 * @returns the amount to execute in the current slice (absolute value)
	 */
	getExecutionSlice(nowSec: number): BN {
		// twap based on how much time has elapsed since last execution
		const orderSize = this.amountTarget.sub(this.amountStart);
		const secElapsed = new BN(nowSec - this.lastExecSec);
		const slice = orderSize
			.abs()
			.mul(secElapsed)
			.div(new BN(this.overallDurationSec))
			.abs();
		if (slice.gt(orderSize.abs())) {
			return orderSize.abs();
		}
		const remaining = this.getAmountRemaining();
		if (remaining.lt(slice)) {
			return remaining;
		}
		return slice;
	}

	/**
	 *
	 * @returns the execution direction (LONG or SHORT) to place orders
	 */
	getExecutionDirection(): PositionDirection {
		return this.amountTarget.gt(this.currentPosition)
			? PositionDirection.LONG
			: PositionDirection.SHORT;
	}

	/**
	 *
	 * @returns the amount remaining to be executed (absolute value)
	 */
	getAmountRemaining(): BN {
		return this.amountTarget.sub(this.currentPosition).abs();
	}

	/**
	 *
	 * @param currentPosition the current position base asset amount
	 */
	updateProgress(currentPosition: BN, nowSec: number): void {
		// if the new position is ultimately a larger order, reinit the twap so the slices reflect the new desired order
		const currExecutionSize = this.amountTarget.sub(this.amountStart).abs();
		const newExecutionSize = this.amountTarget.sub(currentPosition).abs();
		if (newExecutionSize.gt(currExecutionSize)) {
			this.amountStart = currentPosition;
			this.startTimeSec = nowSec;
		}

		this.currentPosition = currentPosition;
		this.lastUpdateSec = nowSec;
	}

	updateExecution(nowSec: number): void {
		this.lastExecSec = nowSec;
		this.firstExecDone = true;
	}

	updateTarget(newTarget: BN, nowSec: number): void {
		this.amountTarget = newTarget;
		this.startTimeSec = nowSec;
		this.lastUpdateSec = nowSec;
	}
}

export interface Bot {
	readonly name: string;
	readonly dryRun: boolean;
	readonly defaultIntervalMs?: number;

	/**
	 * Initialize the bot
	 */
	init: () => Promise<void>;

	/**
	 * Reset the bot. This is called to reset the bot to a fresh state (pre-init).
	 */
	reset: () => Promise<void>;

	/**
	 * Start the bot loop. This is generally a polling loop.
	 */
	startIntervalLoop: (intervalMs?: number) => Promise<void>;

	/**
	 * Returns true if bot is healthy, else false. Typically used for monitoring liveness.
	 */
	healthCheck: () => Promise<boolean>;
}

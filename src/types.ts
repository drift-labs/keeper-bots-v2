import { OrderRecord } from '@drift-labs/sdk';

import { DLOB } from './dlob/DLOB';

export const constants = {
	devnet: {
		USDCMint: '8zGuJQqwhZafTah7Uc7Z4tXRnguqkn5KLFAP8oV6PHe2',
	},
};

export interface Bot {
	readonly name: string;
	readonly dryRun: boolean;

	/**
	 * Initialize the bot
	 */
	init: () => Promise<void>;

	/**
	 * Reset the bot. This is called to reset the bot to a fresh state (pre-init).
	 */
	reset: () => void;

	/**
	 * Start the bot loop. This is generally a polling loop.
	 */
	startIntervalLoop: (intervalMs: number) => void;

	/**
	 * Trigger the bot to run a step, used instead of polling
	 */
	trigger: (record: OrderRecord) => Promise<void>;

	/**
	 * Returns the bot's DLOB
	 */
	viewDlob: () => DLOB;
}

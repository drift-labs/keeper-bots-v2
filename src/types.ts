export const constants = {
	devnet: {
		USDCMint: '8zGuJQqwhZafTah7Uc7Z4tXRnguqkn5KLFAP8oV6PHe2',
	},
};

export interface Bot {
	readonly name: string;

	/**
	 * Reset the bot. This is called to reset the bot to its init state.
	 */
	reset: () => void;

	/**
	 * Start the bot loop. This is generally a polling loop.
	 */
	start: () => void;

	/**
	 * Trigger the bot to run a step, used instead of polling
	 */
	trigger: () => Promise<void>;
}

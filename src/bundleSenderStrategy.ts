import { LAMPORTS_PER_SOL } from '@solana/web3.js';
import { TipStream } from './bundleSender';

/**
 * A strategy for determining the amount of tip
 * @param latestTipStream - The latest tip stream.
 * @param failBundleCount - Running count of bundles that have failed to land.
 * @returns The number of bundles to send.
 */
export type BundleSenderStrategy = (
	latestTipStream: TipStream,
	failBundleCount: number
) => number;

const minBundleTip = 10_000; // cant be lower than this
const maxBundleTip = 100_000;
const maxFailBundleCount = 100; // at 100 failed txs, can expect tip to become maxBundleTip
const tipMultiplier = 3; // bigger == more superlinear, delay the ramp up to prevent overpaying too soon
export const exponentialBundleTip: BundleSenderStrategy = (
	latestTipStream: TipStream,
	failBundleCount: number
) => {
	return Math.floor(
		Math.max(
			latestTipStream.landed_tips_25th_percentile ?? 0 * LAMPORTS_PER_SOL,
			minBundleTip,
			Math.min(
				maxBundleTip,
				Math.pow(failBundleCount / maxFailBundleCount, tipMultiplier) *
					maxBundleTip
			)
		)
	);
};

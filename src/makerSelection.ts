import {
	BN,
	convertToNumber,
	divCeil,
	DLOBNode,
	ZERO,
} from '@drift-labs/sdk';
import { MAX_MAKERS_PER_FILL } from './bots/filler';

const PROBABILITY_PRECISION = new BN(1000);

export function selectMakers(makerNodes: DLOBNode[]): DLOBNode[] {
	const selectedMakers = [];

	while (selectedMakers.length < MAX_MAKERS_PER_FILL && makerNodes.length > 0) {
		const makerIndex = selectMakerIndex(makerNodes);
		if (makerIndex === -1) {
			break;
		}
		const maker = makerNodes[makerIndex];
		selectedMakers.push(maker);
		makerNodes.splice(makerIndex, 1);
	}

	return selectedMakers;
}

function selectMakerIndex(makers: DLOBNode[]): number {
	const probabilities = [];
	const totalLiquidity = makers.reduce(
		(sum, maker) =>
			sum.add(
				maker.order!.baseAssetAmount.sub(maker.order!.baseAssetAmountFilled)
			),
		ZERO
	);
	for (const maker of makers) {
		probabilities.push(getProbability(maker, totalLiquidity));
	}

	let makerIndex = -1;
	const random = Math.random();
	let sum = 0;
	for (let i = 0; i < probabilities.length; i++) {
		sum += probabilities[i];
		if (random < sum) {
			makerIndex = i;
			break;
		}
	}

	return makerIndex;
}

function getProbability(maker: DLOBNode, totalLiquidity: BN): number {
	return convertToNumber(
		divCeil(
			maker
				.order!.baseAssetAmount.sub(maker.order!.baseAssetAmountFilled)
				.mul(PROBABILITY_PRECISION),
			totalLiquidity
		),
		PROBABILITY_PRECISION
	);
}

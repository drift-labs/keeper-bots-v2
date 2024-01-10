import { expect } from 'chai';
import { BN, isVariant } from '@drift-labs/sdk';
import { TwapExecutionProgress } from './types';
import { selectMakers } from './makerSelection';
import { MAX_MAKERS_PER_FILL } from './bots/filler';

describe('TwapExecutionProgress', () => {
	const startTs = 1000;
	it('should calculate correct execution direction and progress', () => {
		let twap = new TwapExecutionProgress({
			currentPosition: new BN(0),
			targetPosition: new BN(10),
			overallDurationSec: 10,
			startTimeSec: startTs,
		});
		let remaining = twap.getAmountRemaining();
		expect(remaining.toString()).to.be.equal(new BN(10).toString());
		expect(isVariant(twap.getExecutionDirection(), 'long')).to.be.equal(true);

		twap = new TwapExecutionProgress({
			currentPosition: new BN(10),
			targetPosition: new BN(0),
			overallDurationSec: 10,
			startTimeSec: startTs,
		});
		remaining = twap.getAmountRemaining();
		expect(remaining.toString()).to.be.equal(new BN(10).toString());
		expect(isVariant(twap.getExecutionDirection(), 'short')).to.be.equal(true);

		twap = new TwapExecutionProgress({
			currentPosition: new BN(-10),
			targetPosition: new BN(30),
			overallDurationSec: 10,
			startTimeSec: startTs,
		});
		remaining = twap.getAmountRemaining();
		expect(remaining.toString()).to.be.equal(new BN(40).toString());
		expect(isVariant(twap.getExecutionDirection(), 'long')).to.be.equal(true);

		twap = new TwapExecutionProgress({
			currentPosition: new BN(10),
			targetPosition: new BN(-30),
			overallDurationSec: 10,
			startTimeSec: startTs,
		});
		remaining = twap.getAmountRemaining();
		expect(remaining.toString()).to.be.equal(new BN(40).toString());
		expect(isVariant(twap.getExecutionDirection(), 'short')).to.be.equal(true);
	});

	it('should calculate execution slice correctly', () => {
		let twap = new TwapExecutionProgress({
			currentPosition: new BN(0),
			targetPosition: new BN(10),
			overallDurationSec: 10,
			startTimeSec: startTs,
		});
		let slice = twap.getExecutionSlice(startTs + 1);
		expect(slice.toString()).to.be.equal(new BN(1).toString());

		twap = new TwapExecutionProgress({
			currentPosition: new BN(-5000),
			targetPosition: new BN(10000),
			overallDurationSec: 300,
			startTimeSec: startTs,
		});
		slice = twap.getExecutionSlice(startTs + 1);
		expect(slice.toString()).to.be.equal(new BN(50).toString());
		expect(isVariant(twap.getExecutionDirection(), 'long')).to.be.equal(true);

		twap = new TwapExecutionProgress({
			currentPosition: new BN(5000),
			targetPosition: new BN(-10000),
			overallDurationSec: 300,
			startTimeSec: startTs,
		});
		slice = twap.getExecutionSlice(startTs + 1);
		expect(slice.toString()).to.be.equal(new BN(50).toString());
		expect(isVariant(twap.getExecutionDirection(), 'short')).to.be.equal(true);
	});

	it('should update progress and calculate execution slice correctly', () => {
		// no flip
		let twap = new TwapExecutionProgress({
			currentPosition: new BN(0),
			targetPosition: new BN(10),
			overallDurationSec: 10,
			startTimeSec: startTs,
		});
		let slice = twap.getExecutionSlice(startTs + 1);
		expect(slice.toString()).to.be.equal(new BN(1).toString());
		let remaining = twap.getAmountRemaining();
		expect(remaining.toString()).to.be.equal(new BN(10).toString());
		expect(isVariant(twap.getExecutionDirection(), 'long')).to.be.equal(true);

		twap.updateProgress(new BN(1), startTs);
		slice = twap.getExecutionSlice(startTs + 1);
		twap.updateExecution(startTs + 1);
		expect(slice.toString()).to.be.equal(new BN(1).toString());
		remaining = twap.getAmountRemaining();
		expect(remaining.toString()).to.be.equal(new BN(9).toString());
		expect(isVariant(twap.getExecutionDirection(), 'long')).to.be.equal(true);

		// flip short -> long
		twap = new TwapExecutionProgress({
			currentPosition: new BN(-5000),
			targetPosition: new BN(10000),
			overallDurationSec: 300,
			startTimeSec: startTs,
		});
		slice = twap.getExecutionSlice(startTs + 1);
		expect(slice.toString()).to.be.equal(new BN(50).toString());
		remaining = twap.getAmountRemaining();
		expect(remaining.toString()).to.be.equal(new BN(15000).toString());
		expect(isVariant(twap.getExecutionDirection(), 'long')).to.be.equal(true);

		twap.updateProgress(new BN(-4950), startTs + 1);
		slice = twap.getExecutionSlice(startTs + 1);
		twap.updateExecution(startTs + 2);
		expect(slice.toString()).to.be.equal(new BN(50).toString());
		remaining = twap.getAmountRemaining();
		expect(remaining.toString()).to.be.equal(new BN(14950).toString());
		expect(isVariant(twap.getExecutionDirection(), 'long')).to.be.equal(true);

		// flip long -> short
		twap = new TwapExecutionProgress({
			currentPosition: new BN(5000),
			targetPosition: new BN(-10000),
			overallDurationSec: 300,
			startTimeSec: startTs,
		});
		slice = twap.getExecutionSlice(startTs + 1);
		expect(slice.toString()).to.be.equal(new BN(50).toString());
		remaining = twap.getAmountRemaining();
		expect(remaining.toString()).to.be.equal(new BN(15000).toString());
		expect(isVariant(twap.getExecutionDirection(), 'short')).to.be.equal(true);

		twap.updateProgress(new BN(4950), startTs + 1);
		slice = twap.getExecutionSlice(startTs + 1);
		twap.updateExecution(startTs + 1);
		expect(slice.toString()).to.be.equal(new BN(50).toString());
		remaining = twap.getAmountRemaining();
		expect(remaining.toString()).to.be.equal(new BN(14950).toString());
		expect(isVariant(twap.getExecutionDirection(), 'short')).to.be.equal(true);

		// position increased while closing long
		twap = new TwapExecutionProgress({
			currentPosition: new BN(5000),
			targetPosition: new BN(0),
			overallDurationSec: 300,
			startTimeSec: startTs,
		});
		slice = twap.getExecutionSlice(startTs + 1);
		expect(slice.toString()).to.be.equal(new BN(16).toString());
		remaining = twap.getAmountRemaining();
		expect(remaining.toString()).to.be.equal(new BN(5000).toString());
		expect(isVariant(twap.getExecutionDirection(), 'short')).to.be.equal(true);

		// filled a bit
		twap.updateProgress(new BN(4950), startTs + 1);
		slice = twap.getExecutionSlice(startTs + 1);
		twap.updateExecution(startTs + 1);
		expect(slice.toString()).to.be.equal(new BN(16).toString());
		remaining = twap.getAmountRemaining();
		expect(remaining.toString()).to.be.equal(new BN(4950).toString());
		expect(isVariant(twap.getExecutionDirection(), 'short')).to.be.equal(true);

		// position got bigger
		twap.updateProgress(new BN(5500), startTs + 1);
		slice = twap.getExecutionSlice(startTs);
		twap.updateExecution(startTs + 2);
		expect(slice.toString()).to.be.equal(new BN(18).toString()); // should recalculate slice based on new larger position
		remaining = twap.getAmountRemaining();
		expect(remaining.toString()).to.be.equal(new BN(5500).toString());
		expect(isVariant(twap.getExecutionDirection(), 'short')).to.be.equal(true);
	});

	it('should correctly size slice as time passes', () => {
		// want to trade out of 1000 over 300s (5 min)
		const twap = new TwapExecutionProgress({
			currentPosition: new BN(1000),
			targetPosition: new BN(0),
			overallDurationSec: 300,
			startTimeSec: startTs,
		});
		let slice = twap.getExecutionSlice(startTs + 0);
		expect(slice.toString()).to.be.equal(new BN(0).toString());

		slice = twap.getExecutionSlice(startTs + 100);
		expect(slice.toString()).to.be.equal(new BN(333).toString());

		slice = twap.getExecutionSlice(startTs + 150);
		expect(slice.toString()).to.be.equal(new BN(500).toString());

		slice = twap.getExecutionSlice(startTs + 300);
		expect(slice.toString()).to.be.equal(new BN(1000).toString());

		slice = twap.getExecutionSlice(startTs + 400);
		expect(slice.toString()).to.be.equal(new BN(1000).toString());
	});

	it('should calculate correct execution for first slice of new init', () => {
		const twap = new TwapExecutionProgress({
			currentPosition: new BN(0),
			targetPosition: new BN(0),
			overallDurationSec: 3000, // 6 min
			startTimeSec: startTs,
		});
		const remaining = twap.getAmountRemaining();
		expect(remaining.toString()).to.be.equal(new BN(0).toString());

		const startTs1 = startTs + 1000;
		twap.updateProgress(new BN(300_000), startTs1); // 300_000 over 3000s, is 100 per second
		let slice = twap.getExecutionSlice(startTs1);
		twap.updateExecution(startTs1);
		expect(slice.toString()).to.be.equal(new BN(100 * 1000).toString());

		const startTs2 = startTs + 2000;
		twap.updateProgress(new BN(200_000), startTs2);
		slice = twap.getExecutionSlice(startTs2);
		twap.updateExecution(startTs2);
		expect(slice.toString()).to.be.equal(new BN(100 * 1000).toString());

		const startTs3 = startTs + 3000;
		twap.updateProgress(new BN(100_000), startTs3);
		slice = twap.getExecutionSlice(startTs3);
		twap.updateExecution(startTs3);
		expect(slice.toString()).to.be.equal(new BN(100 * 1000).toString());

		const startTs4 = startTs + 4000;
		twap.updateProgress(new BN(0), startTs4);
		slice = twap.getExecutionSlice(startTs4);
		twap.updateExecution(startTs4);
		expect(slice.toString()).to.be.equal(new BN(0).toString());
	});

	it('should correctly size slice as time passes with fills', () => {
		// want to trade out of 1000 over 300s (5 min)
		const twap = new TwapExecutionProgress({
			currentPosition: new BN(1000),
			targetPosition: new BN(0),
			overallDurationSec: 300,
			startTimeSec: startTs,
		});
		let slice = twap.getExecutionSlice(startTs + 0);
		expect(slice.toString()).to.be.equal(new BN(0).toString());

		slice = twap.getExecutionSlice(startTs + 100);
		expect(slice.toString()).to.be.equal(new BN(333).toString());

		slice = twap.getExecutionSlice(startTs + 150);
		expect(slice.toString()).to.be.equal(new BN(500).toString());

		// fill half
		twap.updateProgress(new BN(500), startTs + 150);
		slice = twap.getExecutionSlice(startTs + 150);
		twap.updateExecution(startTs + 150);
		slice = twap.getExecutionSlice(startTs + 150);
		expect(slice.toString()).to.be.equal(new BN(0).toString());

		slice = twap.getExecutionSlice(startTs + 300);
		expect(slice.toString()).to.be.equal(new BN(500).toString());

		twap.updateProgress(new BN(0), startTs + 300);
		slice = twap.getExecutionSlice(startTs + 400);
		twap.updateExecution(startTs + 400);
		expect(slice.toString()).to.be.equal(new BN(0).toString());
	});
});

describe('selectMakers', () => {
	let originalRandom: { (): number; (): number };

	beforeEach(() => {
		// Mock Math.random
		let seed = 12345;
		originalRandom = Math.random;
		Math.random = () => {
			const x = Math.sin(seed++) * 10000;
			return x - Math.floor(x);
		};
	});

	afterEach(() => {
		// Restore original Math.random
		Math.random = originalRandom;
	});

	it('more than 6', function () {
		// Mock DLOBNode and Order
		const mockOrder = (filledAmount: number, orderId: number) => ({
			orderId,
			baseAssetAmount: new BN(100),
			baseAssetAmountFilled: new BN(filledAmount),
		});

		const mockDLOBNode = (filledAmount: number, orderId: number) => ({
			order: mockOrder(filledAmount, orderId),
			// Include other necessary properties of DLOBNode if needed
		});

		const makerNodeMap = new Map([
			['0', [mockDLOBNode(10, 0)]],
			['1', [mockDLOBNode(20, 1)]],
			['2', [mockDLOBNode(30, 2)]],
			['3', [mockDLOBNode(40, 3)]],
			['4', [mockDLOBNode(50, 4)]],
			['5', [mockDLOBNode(60, 5)]],
			['6', [mockDLOBNode(70, 6)]],
			['7', [mockDLOBNode(80, 7)]],
			['8', [mockDLOBNode(90, 8)]],
		]);

		// @ts-ignore
		const selectedMakers = selectMakers(makerNodeMap);

		expect(selectedMakers).to.not.be.undefined;
		expect(selectedMakers.size).to.be.equal(MAX_MAKERS_PER_FILL);

		expect(selectedMakers.get('0')).to.not.be.undefined;
		expect(selectedMakers.get('1')).to.not.be.undefined;
		expect(selectedMakers.get('2')).to.not.be.undefined;
		expect(selectedMakers.get('3')).to.not.be.undefined;
		expect(selectedMakers.get('4')).to.be.undefined;
		expect(selectedMakers.get('5')).to.not.be.undefined;
		expect(selectedMakers.get('6')).to.not.be.undefined;
		expect(selectedMakers.get('7')).to.be.undefined;
		expect(selectedMakers.get('8')).to.be.undefined;
	});
});

import { logger } from '../../logger';
import { PrometheusExporter } from '@opentelemetry/exporter-prometheus';
import {
	MeterProvider,
	View,
	ExplicitBucketHistogramAggregation,
	InstrumentType,
} from '@opentelemetry/sdk-metrics-base';
import { metrics, type Histogram } from '@opentelemetry/api';
import { getNodeAutoInstrumentations } from '@opentelemetry/auto-instrumentations-node';
import { registerInstrumentations } from '@opentelemetry/instrumentation';

export class TxRecorder {
	private txLatency?: Histogram;
	private slotLatency?: Histogram;
	private attrs: Record<string, string> = {};

	constructor(botName: string, port?: number, disabled?: boolean) {
		if (disabled || !port) {
			logger.info(`metrics disabled for bot ${botName}`);
			return;
		}

		const { endpoint } = PrometheusExporter.DEFAULT_OPTIONS;
		const exporter = new PrometheusExporter({ port, endpoint }, () =>
			logger.info(`prometheus scrape: http://localhost:${port}${endpoint}`)
		);

		const msBuckets = (() => {
			const b: number[] = [];
			for (let i = 0; i <= 1000; i += 50) b.push(i);
			for (let i = 1100; i <= 2000; i += 100) b.push(i);
			for (let i = 2200; i <= 10000; i += 200) b.push(i);
			return b;
		})();
		const slotBuckets = Array.from({ length: 30 }, (_, i) => i + 1);

		const provider = new MeterProvider({
			views: [
				new View({
					instrumentName: 'tx_confirmation_latency',
					instrumentType: InstrumentType.HISTOGRAM,
					aggregation: new ExplicitBucketHistogramAggregation(msBuckets, true),
				}),
				new View({
					instrumentName: 'tx_slot_latency',
					instrumentType: InstrumentType.HISTOGRAM,
					aggregation: new ExplicitBucketHistogramAggregation(
						slotBuckets,
						true
					),
				}),
			],
		});

		provider.addMetricReader(exporter);

		metrics.setGlobalMeterProvider(provider);

		registerInstrumentations({
			meterProvider: provider,
			instrumentations: [getNodeAutoInstrumentations()],
		});

		const meter = metrics.getMeter(botName);

		this.attrs = { bot_name: botName };
		this.txLatency = meter.createHistogram('tx_confirmation_latency', {
			unit: 'ms',
		});
		this.slotLatency = meter.createHistogram('tx_slot_latency', {
			unit: 'slots',
		});
	}

	send(latencyMs: number, slotDelta = 0) {
		this.txLatency?.record(latencyMs, this.attrs);
		this.slotLatency?.record(Math.max(0, slotDelta), this.attrs);
	}
}

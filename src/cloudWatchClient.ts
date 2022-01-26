import AWS from 'aws-sdk';

export class CloudWatchClient {
	cloudWatch: AWS.CloudWatch;
	enabled: boolean;

	public constructor(region: string, enabled: boolean) {
		this.cloudWatch = new AWS.CloudWatch({ region });
		this.enabled = enabled;
	}

	public logFill(success: boolean): void {
		if (!this.enabled) {
			return;
		}

		const params = {
			MetricData: [
				{
					MetricName: 'Fill',
					Unit: 'None',
					Value: success ? 1 : 0,
				},
			],
			Namespace: 'Orders',
		};
		this.cloudWatch.putMetricData(params, (err) => {
			if (err) {
				console.error(err);
			}
		});
	}
}

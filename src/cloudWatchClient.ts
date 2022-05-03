import AWS from 'aws-sdk';

export class CloudWatchClient {
	cloudWatch: AWS.CloudWatch;
	enabled: boolean;

	public constructor(region: string, enabled: boolean) {
		this.cloudWatch = new AWS.CloudWatch({ region });
		this.enabled = enabled;
	}

	public logNoOrderUpdate(): void {
		if (!this.enabled) {
			return;
		}

		const params = {
			MetricData: [
				{
					MetricName: 'NoOrderUpdate',
					Unit: 'None',
					Value: 1,
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

	public logNoUserUpdate(): void {
		if (!this.enabled) {
			return;
		}

		const params = {
			MetricData: [
				{
					MetricName: 'NoUserUpdate',
					Unit: 'None',
					Value: 1,
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

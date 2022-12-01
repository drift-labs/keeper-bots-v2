import axios from 'axios';
require('dotenv').config();
import { logger } from './logger';

export async function webhookMessage(
	message: string,
	webhookUrl?: string,
	prefix?: string
): Promise<void> {
	if (webhookUrl || process.env.WEBHOOK_URL) {
		const webhook = webhookUrl || process.env.WEBHOOK_URL;
		const fullMessage = prefix ? prefix + message : message;
		if (fullMessage && webhook) {
			try {
				const data = { content: fullMessage };
				axios.post(webhook, data);
			} catch (err) {
				logger.info('webhook error');
			}
		}
	}
}

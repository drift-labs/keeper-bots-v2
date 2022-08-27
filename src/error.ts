import { SendTransactionError } from '@solana/web3.js';

export function getErrorCode(error: Error): number | undefined {
	// @ts-ignore
	let errorCode = error.code;

	if (!errorCode) {
		try {
			const code = error.message.match(
				/custom program error: (0x[0-9,a-f]+)/
			)[1];

			if (code) {
				errorCode = parseInt(code, 16);
			}
		} catch (e) {
			// no problem if couldn't match error code
		}
	}
	return errorCode;
}

export function getErrorMessage(error: SendTransactionError): string {
	let errorString = '';
	error.logs.forEach((logMsg) => {
		try {
			const errorCode = logMsg.match(
				/Program log: AnchorError occurred. Error Code: ([0-9,a-z,A-Z]+). Error Number/
			)[1];

			if (errorCode) {
				errorString = errorCode;
			}
		} catch (e) {
			// no problem if couldn't match error code
		}
	});

	return errorString;
}

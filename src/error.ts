import { SendTransactionError } from '@solana/web3.js';

export function getErrorCode(error: Error): number | undefined {
	// @ts-ignore
	let errorCode = error.code;

	if (!errorCode) {
		try {
			const matches = error.message.match(
				/custom program error: (0x[0-9,a-f]+)/
			);
			if (!matches) {
				return undefined;
			}

			const code = matches[1];

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
	error.logs?.forEach((logMsg) => {
		try {
			const matches = logMsg.match(
				/Program log: AnchorError occurred. Error Code: ([0-9,a-z,A-Z]+). Error Number/
			);
			if (!matches) {
				return;
			}
			const errorCode = matches[1];

			if (errorCode) {
				errorString = errorCode;
			}
		} catch (e) {
			// no problem if couldn't match error code
		}
	});

	return errorString;
}

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

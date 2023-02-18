import { PublicKey, UserAccount } from '@drift-labs/sdk';

export type RuntimeSpec = {
	rpcEndpoint: string;
	driftEnv: string;
	commit: string;
	driftPid: string;
	walletAuthority: string;
};

export function metricAttrFromUserAccount(
	userAccountKey: PublicKey,
	ua: UserAccount
): any {
	return {
		subaccount_id: ua.subAccountId,
		public_key: userAccountKey.toBase58(),
		authority: ua.authority.toBase58(),
		delegate: ua.delegate.toBase58(),
	};
}

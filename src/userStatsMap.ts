import {
	ClearingHouse,
	UserAccount,
	getUserStatsAccountPublicKey,
	OrderRecord,
} from '@drift-labs/sdk';
import { ProgramAccount } from '@project-serum/anchor';

import { PublicKey } from '@solana/web3.js';

export class UserStatsMap {
	/**
	 * map from UserAccount pubkey to ClearingHouseUserStats pubkey
	 * note: multiple UserAccount may have the same ClearingHouseUserStats since ClearingHouseUserStats are unique
	 * to the authority, we key by UserAccount to make lookup easier
	 */
	private userStatsMap = new Map<string, PublicKey>();
	private clearingHouse: ClearingHouse;

	constructor(clearingHouse: ClearingHouse) {
		this.clearingHouse = clearingHouse;
	}

	public async fetchAllUsers() {
		const programUserAccounts =
			(await this.clearingHouse.program.account.user.all()) as ProgramAccount<UserAccount>[];

		for (const programUserAccount of programUserAccounts) {
			const user: UserAccount = programUserAccount.account;

			if (this.userStatsMap.has(programUserAccount.publicKey.toString())) {
				continue;
			}

			this.addUser(programUserAccount.publicKey, user.authority);
		}
	}

	private async getUserAccountFromPublicKey(
		userAccountPublicKey: string
	): Promise<ProgramAccount<UserAccount>> {
		const programUserAccounts =
			(await this.clearingHouse.program.account.user.all()) as ProgramAccount<UserAccount>[];

		const userAccountKey = new PublicKey(userAccountPublicKey);
		const programUserAccount = programUserAccounts.find((programUserAccount) =>
			programUserAccount.publicKey.equals(userAccountKey)
		);
		if (!programUserAccount) {
			throw new Error(`UserAccount not found: ${userAccountPublicKey}`);
		}

		return programUserAccount;
	}

	public addUser(userAccount: PublicKey, userAuthority: PublicKey): PublicKey {
		const userStatsAccountKey = getUserStatsAccountPublicKey(
			this.clearingHouse.program.programId,
			userAuthority
		);
		this.userStatsMap.set(userAccount.toString(), userStatsAccountKey);

		return userStatsAccountKey;
	}

	public async updateWithOrder(record: OrderRecord) {
		if (
			!record.taker.equals(PublicKey.default) &&
			!this.has(record.taker.toString())
		) {
			const takerUserAccount = await this.getUserAccountFromPublicKey(
				record.taker.toString()
			);
			this.addUser(
				takerUserAccount.publicKey,
				takerUserAccount.account.authority
			);
		}

		if (
			!record.maker.equals(PublicKey.default) &&
			!this.has(record.maker.toString())
		) {
			const makerUserAccount = await this.getUserAccountFromPublicKey(
				record.maker.toString()
			);
			this.addUser(
				makerUserAccount.publicKey,
				makerUserAccount.account.authority
			);
		}
	}

	public has(userAccountPublicKey: string): boolean {
		return this.userStatsMap.has(userAccountPublicKey);
	}

	public get(userAccountPublicKey: string): PublicKey {
		return new PublicKey(this.userStatsMap.get(userAccountPublicKey));
	}

	public async mustGet(key: string): Promise<PublicKey> {
		if (!this.has(key)) {
			const programUserAccount = await this.getUserAccountFromPublicKey(key);

			return this.addUser(
				programUserAccount.publicKey,
				programUserAccount.account.authority
			);
		}
		return this.get(key);
	}

	public values(): IterableIterator<PublicKey> {
		return this.userStatsMap.values();
	}
}

import {
	ClearingHouseUser,
	ClearingHouse,
	UserAccount,
	BulkAccountLoader,
	bulkPollingUserSubscribe,
	OrderRecord,
} from '@drift-labs/sdk';
import { ProgramAccount } from '@project-serum/anchor';

import { Connection, PublicKey } from '@solana/web3.js';

export class UserMap {
	private userMap = new Map<string, ClearingHouseUser>();
	private clearingHouse: ClearingHouse;
	private userAccountLoader: BulkAccountLoader;

	constructor(connection: Connection, clearingHouse: ClearingHouse) {
		this.clearingHouse = clearingHouse;

		this.userAccountLoader = new BulkAccountLoader(
			connection,
			'processed',
			5000
		);
	}

	public async fetchAllUsers() {
		const programUserAccounts =
			(await this.clearingHouse.program.account.user.all()) as ProgramAccount<UserAccount>[];
		const userArray: ClearingHouseUser[] = [];
		for (const programUserAccount of programUserAccounts) {
			if (this.userMap.has(programUserAccount.publicKey.toString())) {
				continue;
			}

			const user = new ClearingHouseUser({
				clearingHouse: this.clearingHouse,
				userAccountPublicKey: programUserAccount.publicKey,
				accountSubscription: {
					type: 'polling',
					accountLoader: this.userAccountLoader,
				},
			});
			userArray.push(user);
		}

		await bulkPollingUserSubscribe(userArray, this.userAccountLoader);
		for (const user of userArray) {
			const userAccountPubkey = await user.getUserAccountPublicKey();
			this.userMap.set(userAccountPubkey.toString(), user);
		}
	}

	public async addPubkey(userAccountPublicKey: PublicKey) {
		const user = new ClearingHouseUser({
			clearingHouse: this.clearingHouse,
			userAccountPublicKey,
			accountSubscription: {
				type: 'polling',
				accountLoader: this.userAccountLoader,
			},
		});
		await user.subscribe();
		this.userMap.set(userAccountPublicKey.toString(), user);
	}

	public has(key: string): boolean {
		return this.userMap.has(key);
	}

	public get(key: string): ClearingHouseUser {
		return this.userMap.get(key);
	}

	public async updateWithOrder(record: OrderRecord) {
		if (
			!record.taker.equals(PublicKey.default) &&
			!this.has(record.taker.toString())
		) {
			await this.addPubkey(record.taker);
		}

		if (
			!record.maker.equals(PublicKey.default) &&
			!this.has(record.maker.toString())
		) {
			await this.addPubkey(record.maker);
		}
	}
}

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
	private pollingIntervalMs: number;

	constructor(
		connection: Connection,
		clearingHouse: ClearingHouse,
		pollingIntervalMs?: number
	) {
		this.clearingHouse = clearingHouse;

		if (pollingIntervalMs === undefined) {
			this.pollingIntervalMs = 5000;
		} else {
			this.pollingIntervalMs = pollingIntervalMs;
		}

		this.userAccountLoader = new BulkAccountLoader(
			connection,
			'processed',
			this.pollingIntervalMs
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

	/**
	 * gets the ClearingHouseUser for a particular userAccountPublicKey, if no ClearingHouseUser exists, undefined is returned
	 * @param key userAccountPublicKey to get ClearngHouseUserFor
	 * @returns user ClearingHouseUser | undefined
	 */
	public get(key: string): ClearingHouseUser | undefined {
		return this.userMap.get(key);
	}

	/**
	 * gets the ClearingHouseUser for a particular userAccountPublicKey, if no ClearingHouseUser exists, new one is created
	 * @param key userAccountPublicKey to get ClearngHouseUserFor
	 * @returns  ClearingHouseUser
	 */
	public async mustGet(key: string): Promise<ClearingHouseUser> {
		if (!this.has(key)) {
			await this.addPubkey(new PublicKey(key));
		}
		const user = this.userMap.get(key);
		await user.fetchAccounts();
		return user;
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

	public values(): IterableIterator<ClearingHouseUser> {
		return this.userMap.values();
	}
}

//! One-shot genesis seeding for a fresh env.
//!
//! The executor seeds the chain's genesis allocations (balances, nonces,
//! code, and storage words) into a brand-new env before it spawns the
//! [`crate::writer::StateWriter`]. This way, the writer's first published
//! snapshot already reflects genesis, and the first block's transactions
//! have account state to debit.
//!
//! Seeding is gated on the [`KEY_GENESIS_APPLIED`] meta flag, so a restart
//! against an already-seeded env is a no-op. This check is independent of
//! the block cursor, because genesis is "block 0". `last_committed_block`
//! stays 0 until the first real block commits.

use alloy_primitives::{B256, keccak256};
use kardamom_types::{AccountChange, GenesisAlloc, StorageChange};
use signet_libmdbx::WriteFlags;

use crate::env::StateEnv;
use crate::error::StateError;
use crate::meta::{KEY_GENESIS_APPLIED, KEY_GENESIS_DIGEST, encode_u32};
use crate::schema::{TABLE_ACCOUNTS, TABLE_CODE, TABLE_META, TABLE_STORAGE};

/// An order-insensitive digest of a genesis allocation. This is a keccak
/// hash over the sorted account entries (address, nonce, balance,
/// `code_hash`), the sorted code hashes, and then the sorted storage words
/// (address, slot, value). Bytecode is content-addressed, so the hash pins
/// the bytes. An allocation with no storage word hashes exactly the
/// account and code bytes, so its digest does not depend on the storage
/// part.
///
/// The digest is stored at seed time under [`KEY_GENESIS_DIGEST`], and
/// compared on every later [`seed_genesis`] call. This makes startup fail
/// on a changed `--chain` file, or a node pointed at the wrong state
/// directory, instead of silently ignoring the change.
#[must_use]
pub fn genesis_digest(alloc: &GenesisAlloc) -> B256 {
    let mut accs: Vec<&AccountChange> = alloc.accounts.iter().collect();
    accs.sort_by_key(|a| a.address);
    let mut codes: Vec<B256> = alloc.code.iter().map(|c| c.code_hash).collect();
    codes.sort();
    let mut words: Vec<&StorageChange> = alloc.storage.iter().collect();
    words.sort_by_key(|w| (w.address, w.key));

    let mut buf = Vec::with_capacity(accs.len() * 92 + codes.len() * 32 + words.len() * 84);
    for a in &accs {
        buf.extend_from_slice(a.address.as_slice());
        buf.extend_from_slice(&a.nonce.to_be_bytes());
        buf.extend_from_slice(&a.balance.to_be_bytes::<32>());
        buf.extend_from_slice(a.code_hash.as_slice());
    }
    for ch in &codes {
        buf.extend_from_slice(ch.as_slice());
    }
    for w in &words {
        buf.extend_from_slice(w.address.as_slice());
        buf.extend_from_slice(w.key.as_slice());
        buf.extend_from_slice(&w.value.to_be_bytes::<32>());
    }
    keccak256(&buf)
}

/// Returns true if genesis has already been seeded into this env. This
/// checks whether the [`KEY_GENESIS_APPLIED`] flag is present.
///
/// # Errors
///
/// Returns [`StateError`] if the read transaction or the `meta` table
/// read fails.
pub fn genesis_applied(env: &StateEnv) -> Result<bool, StateError> {
    let txn = env.raw().begin_ro_sync()?;
    let meta = txn.open_db(Some(TABLE_META))?;
    Ok(txn
        .get::<Vec<u8>>(meta.dbi(), KEY_GENESIS_APPLIED)?
        .is_some())
}

/// Seed genesis allocations into a fresh env. This function is idempotent.
///
/// It writes every account, code entry and storage word, plus the
/// [`KEY_GENESIS_APPLIED`] flag and the [`KEY_GENESIS_DIGEST`], in a
/// single read-write transaction.
///
/// Returns `Ok(true)` if it seeded the env. Returns `Ok(false)` if genesis
/// was already applied, after checking that the supplied allocations
/// match the digest the env was seeded from. A mismatch, from a changed
/// `--chain` file or the wrong state directory, returns
/// [`StateError::GenesisMismatch`].
///
/// An env seeded before the digest existed gets the digest backfilled
/// from the supplied allocations on the next start.
///
/// `storage_root` is stored as `B256::ZERO`. v0 keeps no per-account MPT
/// roots, to match [`crate::writer::StateWriter`].
///
/// # Errors
///
/// Returns [`StateError::GenesisMismatch`] if the env was already seeded
/// with a different allocation, and [`StateError`] if the transaction or
/// a table write fails.
pub fn seed_genesis(env: &StateEnv, alloc: &GenesisAlloc) -> Result<bool, StateError> {
    let digest = genesis_digest(alloc);
    let txn = env.raw().begin_rw_sync()?;
    let seed = GenesisSeed::open(&txn)?;

    if seed.already_applied()? {
        seed.check_or_backfill_digest(digest)?;
        txn.commit()?;
        return Ok(false);
    }

    seed.write_allocations(alloc)?;

    // Build the initial hashed-state mirror and account trie. This lets a
    // from-genesis validator start at the correct world-state root before
    // block 1. This step is harmless for the executor: its writer runs
    // with `TrieMode::Off` and ignores these tables.
    seed.seed_trie(alloc)?;

    // The flag and digest commit atomically with the allocations, in one
    // read-write transaction. A crash mid-seed aborts everything, so the
    // next start re-seeds cleanly. The put order within the transaction
    // does not matter.
    txn.put(
        seed.meta,
        KEY_GENESIS_DIGEST,
        crate::meta::encode_b256(digest),
        WriteFlags::UPSERT,
    )?;
    txn.put(
        seed.meta,
        KEY_GENESIS_APPLIED,
        encode_u32(1),
        WriteFlags::UPSERT,
    )?;
    txn.commit()?;
    Ok(true)
}

/// The tables genesis seeding touches, opened once per attempt on one
/// read-write transaction.
struct GenesisSeed<'a> {
    txn: &'a signet_libmdbx::tx::aliases::RwTxSync,
    meta: signet_libmdbx::Database,
    accounts_db: signet_libmdbx::Database,
    code_db: signet_libmdbx::Database,
    storage_db: signet_libmdbx::Database,
}

impl<'a> GenesisSeed<'a> {
    fn open(txn: &'a signet_libmdbx::tx::aliases::RwTxSync) -> Result<Self, StateError> {
        Ok(Self {
            meta: txn.open_db(Some(TABLE_META))?,
            accounts_db: txn.open_db(Some(TABLE_ACCOUNTS))?,
            code_db: txn.open_db(Some(TABLE_CODE))?,
            storage_db: txn.open_db(Some(TABLE_STORAGE))?,
            txn,
        })
    }

    /// Whether [`KEY_GENESIS_APPLIED`] is already set on this env.
    fn already_applied(&self) -> Result<bool, StateError> {
        Ok(self
            .txn
            .get::<Vec<u8>>(self.meta.dbi(), KEY_GENESIS_APPLIED)?
            .is_some())
    }

    /// For an env that already carries [`KEY_GENESIS_APPLIED`]: check the
    /// supplied allocation's digest against the stored one, or, if this env
    /// was seeded before the digest existed, backfill it from the supplied
    /// allocation so future restarts can detect drift.
    fn check_or_backfill_digest(&self, digest: B256) -> Result<(), StateError> {
        let Some(stored) = crate::meta::read_meta_b256(self.txn, self.meta, KEY_GENESIS_DIGEST)?
        else {
            self.txn.put(
                self.meta,
                KEY_GENESIS_DIGEST,
                crate::meta::encode_b256(digest),
                WriteFlags::UPSERT,
            )?;
            return Ok(());
        };
        if stored != digest {
            return Err(StateError::GenesisMismatch {
                stored,
                supplied: digest,
            });
        }
        Ok(())
    }

    /// Write every account, code entry and storage word of the allocation.
    fn write_allocations(&self, alloc: &GenesisAlloc) -> Result<(), StateError> {
        crate::schema::write_accounts(self.txn, self.accounts_db, &alloc.accounts)?;
        crate::schema::write_code(self.txn, self.code_db, &alloc.code)?;
        crate::schema::write_storage(self.txn, self.storage_db, &alloc.storage)?;
        Ok(())
    }

    /// Seed the hashed-state mirror and account trie from the allocation, as
    /// one synthetic genesis block, and persist the resulting world-state
    /// root. This lets a from-genesis validator start at the correct root
    /// before block 1; it is harmless for the executor, whose writer runs
    /// with `TrieMode::Off` and ignores these tables.
    fn seed_trie(&self, alloc: &GenesisAlloc) -> Result<B256, StateError> {
        let genesis_delta = kardamom_types::BlockDelta {
            block_number: 0,
            accounts: alloc.accounts.clone(),
            storage: alloc.storage.clone(),
            code: alloc.code.clone(),
            receipts: Vec::new(),
        };
        crate::trie::commit_trie_root(self.txn, &genesis_delta)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::snapshot::StateSnapshot;
    use crate::testing::temp_env;
    use alloy_primitives::{Address, U256, keccak256};
    use bytes::Bytes;
    use kardamom_types::{CodeEntry, StateDatabase};

    /// One account with code and one storage word.
    fn with_storage() -> GenesisAlloc {
        let code = Bytes::from_static(b"\x60\x00");
        let code_hash = keccak256(&code);
        let addr = Address::from([0x11; 20]);
        GenesisAlloc {
            accounts: vec![AccountChange {
                address: addr,
                nonce: 3,
                balance: U256::from(1_000u64),
                code_hash,
            }],
            code: vec![CodeEntry { code_hash, code }],
            storage: vec![StorageChange {
                address: addr,
                key: B256::repeat_byte(0x05),
                value: U256::from(1u64),
            }],
        }
    }

    #[test]
    fn seed_genesis_writes_accounts_code_and_storage() {
        let (_dir, env) = temp_env();
        let alloc = with_storage();
        let account = &alloc.accounts[0];

        assert!(seed_genesis(&env, &alloc).unwrap());

        let snap = StateSnapshot::open(&env).unwrap();
        let (nonce, balance, ch) = snap.basic(account.address).unwrap().unwrap();
        assert_eq!(nonce, 3);
        assert_eq!(balance, U256::from(1_000u64));
        assert_eq!(ch, account.code_hash);
        assert_eq!(snap.code_by_hash(ch).unwrap(), alloc.code[0].code);
        let word = &alloc.storage[0];
        assert_eq!(snap.storage(word.address, word.key).unwrap(), word.value);
    }

    #[test]
    fn the_genesis_root_holds_the_storage_root_of_the_words() {
        let (_dir, env) = temp_env();
        let alloc = with_storage();
        assert!(seed_genesis(&env, &alloc).unwrap());

        let account = &alloc.accounts[0];
        let word = &alloc.storage[0];
        let want = crate::trie::state_root([(
            account.address,
            crate::trie::AccountTrieParts {
                nonce: account.nonce,
                balance: account.balance,
                code_hash: account.code_hash,
                storage_root: crate::trie::storage_root([(word.key, word.value)]),
            },
        )]);
        let snap = StateSnapshot::open(&env).unwrap();
        assert_eq!(snap.state_root().unwrap(), Some(want));
    }

    #[test]
    fn the_digest_without_words_hashes_only_accounts_and_code() {
        // A live chain without storage words stores this digest. A change
        // of it makes every node of that chain refuse its own state and
        // every checkpoint of the chain.
        let alloc = GenesisAlloc {
            storage: Vec::new(),
            ..with_storage()
        };
        let account = &alloc.accounts[0];
        let mut legacy = Vec::new();
        legacy.extend_from_slice(account.address.as_slice());
        legacy.extend_from_slice(&account.nonce.to_be_bytes());
        legacy.extend_from_slice(&account.balance.to_be_bytes::<32>());
        legacy.extend_from_slice(account.code_hash.as_slice());
        legacy.extend_from_slice(alloc.code[0].code_hash.as_slice());
        assert_eq!(genesis_digest(&alloc), keccak256(&legacy));
        assert_ne!(genesis_digest(&with_storage()), genesis_digest(&alloc));
    }

    #[test]
    fn seed_genesis_rejects_changed_words() {
        let (_dir, env) = temp_env();
        assert!(seed_genesis(&env, &with_storage()).unwrap());
        let mut changed = with_storage();
        changed.storage[0].value = U256::from(2u64);
        assert!(matches!(
            seed_genesis(&env, &changed).unwrap_err(),
            StateError::GenesisMismatch { .. }
        ));
    }

    #[test]
    fn seed_genesis_sets_canonical_state_root() {
        // A from-genesis validator must start at the correct world-state root
        // before block 1.
        let (_dir, env) = temp_env();
        let a1 = Address::repeat_byte(0x11);
        let a2 = Address::repeat_byte(0x22);
        let accounts = vec![
            AccountChange {
                address: a1,
                nonce: 1,
                balance: U256::from(100u64),
                code_hash: B256::ZERO,
            },
            AccountChange {
                address: a2,
                nonce: 0,
                balance: U256::from(50u64),
                code_hash: B256::ZERO,
            },
        ];
        assert!(
            seed_genesis(
                &env,
                &kardamom_types::GenesisAlloc::of_accounts(accounts.clone())
            )
            .unwrap()
        );

        let want = crate::trie::state_root(accounts.iter().map(|a| {
            (
                a.address,
                crate::trie::AccountTrieParts {
                    nonce: a.nonce,
                    balance: a.balance,
                    code_hash: a.code_hash,
                    storage_root: B256::ZERO,
                },
            )
        }));
        let snap = StateSnapshot::open(&env).unwrap();
        assert_eq!(snap.state_root().unwrap(), Some(want));
        assert_ne!(want, crate::trie::empty_root());
    }

    #[test]
    fn seed_genesis_is_idempotent() {
        let (_dir, env) = temp_env();
        let addr = Address::from([0x22; 20]);
        let accounts = vec![AccountChange {
            address: addr,
            nonce: 0,
            balance: U256::from(7u64),
            code_hash: B256::ZERO,
        }];

        assert!(!genesis_applied(&env).unwrap());
        assert!(
            seed_genesis(
                &env,
                &kardamom_types::GenesisAlloc::of_accounts(accounts.clone())
            )
            .unwrap()
        );
        assert!(genesis_applied(&env).unwrap());
        // Second call is a no-op.
        assert!(
            !seed_genesis(
                &env,
                &kardamom_types::GenesisAlloc::of_accounts(accounts.clone())
            )
            .unwrap()
        );

        let snap = StateSnapshot::open(&env).unwrap();
        assert_eq!(snap.basic(addr).unwrap().unwrap().1, U256::from(7u64));
    }

    #[test]
    fn seed_genesis_rejects_changed_alloc() {
        // A restart with a different --chain file must fail clearly. It
        // must not silently keep the old genesis state.
        let (_dir, env) = temp_env();
        let accounts = vec![AccountChange {
            address: Address::repeat_byte(0x11),
            nonce: 1,
            balance: U256::from(100u64),
            code_hash: B256::ZERO,
        }];
        assert!(
            seed_genesis(
                &env,
                &kardamom_types::GenesisAlloc::of_accounts(accounts.clone())
            )
            .unwrap()
        );

        let mut changed = accounts.clone();
        changed[0].balance = U256::from(999u64);
        let err = seed_genesis(
            &env,
            &kardamom_types::GenesisAlloc::of_accounts(changed.clone()),
        )
        .unwrap_err();
        assert!(matches!(err, StateError::GenesisMismatch { .. }), "{err}");

        // The same allocation in a different order is still the same genesis.
        let mut two = vec![
            AccountChange {
                address: Address::repeat_byte(0x22),
                nonce: 0,
                balance: U256::from(1u64),
                code_hash: B256::ZERO,
            },
            accounts[0].clone(),
        ];
        let (_dir2, env2) = temp_env();
        assert!(
            seed_genesis(
                &env2,
                &kardamom_types::GenesisAlloc::of_accounts(two.clone())
            )
            .unwrap()
        );
        two.reverse();
        assert!(
            !seed_genesis(
                &env2,
                &kardamom_types::GenesisAlloc::of_accounts(two.clone())
            )
            .unwrap()
        );
    }

    #[test]
    fn seed_genesis_backfills_missing_digest() {
        // An env seeded before the digest existed gets it backfilled on
        // the next start. After that, drift is detected.
        let (_dir, env) = temp_env();
        let accounts = vec![AccountChange {
            address: Address::repeat_byte(0x33),
            nonce: 2,
            balance: U256::from(5u64),
            code_hash: B256::ZERO,
        }];
        assert!(
            seed_genesis(
                &env,
                &kardamom_types::GenesisAlloc::of_accounts(accounts.clone())
            )
            .unwrap()
        );

        // Simulate an env seeded before the digest key existed.
        {
            let txn = env.raw().begin_rw_sync().unwrap();
            let meta = txn.open_db(Some(TABLE_META)).unwrap();
            txn.del(meta, KEY_GENESIS_DIGEST, None).unwrap();
            txn.commit().unwrap();
        }

        // The first restart backfills the digest. Verification is not
        // possible yet.
        assert!(
            !seed_genesis(
                &env,
                &kardamom_types::GenesisAlloc::of_accounts(accounts.clone())
            )
            .unwrap()
        );
        // After that, a changed allocation is rejected.
        let mut changed = accounts.clone();
        changed[0].nonce = 9;
        assert!(matches!(
            seed_genesis(
                &env,
                &kardamom_types::GenesisAlloc::of_accounts(changed.clone())
            )
            .unwrap_err(),
            StateError::GenesisMismatch { .. }
        ));
    }

    #[test]
    fn seed_genesis_empty_alloc_sets_flag() {
        let (_dir, env) = temp_env();
        assert!(seed_genesis(&env, &kardamom_types::GenesisAlloc::default()).unwrap());
        assert!(genesis_applied(&env).unwrap());
        assert!(!seed_genesis(&env, &kardamom_types::GenesisAlloc::default()).unwrap());
    }
}

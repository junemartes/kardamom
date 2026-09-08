//! One-shot genesis seeding for a fresh env.
//!
//! The executor seeds the chain's genesis allocations (balances, nonces,
//! and code) into a brand-new env before it spawns the
//! [`crate::writer::StateWriter`]. This way, the writer's first published
//! snapshot already reflects genesis, and the first block's transactions
//! have account state to debit.
//!
//! Seeding is gated on the [`KEY_GENESIS_APPLIED`] meta flag, so a restart
//! against an already-seeded env is a no-op. This check is independent of
//! the block cursor, because genesis is "block 0". `last_committed_block`
//! stays 0 until the first real block commits.

use alloy_primitives::{B256, keccak256};
use kardamom_types::{AccountChange, CodeEntry};
use signet_libmdbx::WriteFlags;

use crate::env::StateEnv;
use crate::error::StateError;
use crate::meta::{KEY_GENESIS_APPLIED, KEY_GENESIS_DIGEST, encode_u32};
use crate::schema::{TABLE_ACCOUNTS, TABLE_CODE, TABLE_META};

/// An order-insensitive digest of a genesis allocation. This is a keccak
/// hash over the sorted account entries (address, nonce, balance,
/// `code_hash`) and the sorted code hashes. Bytecode is content-addressed,
/// so the hash pins the bytes.
///
/// The digest is stored at seed time under [`KEY_GENESIS_DIGEST`], and
/// compared on every later [`seed_genesis`] call. This makes startup fail
/// on a changed `--chain` file, or a node pointed at the wrong state
/// directory, instead of silently ignoring the change.
pub fn genesis_digest(accounts: &[AccountChange], code: &[CodeEntry]) -> B256 {
    let mut accs: Vec<&AccountChange> = accounts.iter().collect();
    accs.sort_by_key(|a| a.address);
    let mut codes: Vec<B256> = code.iter().map(|c| c.code_hash).collect();
    codes.sort();

    let mut buf = Vec::with_capacity(accs.len() * 92 + codes.len() * 32);
    for a in &accs {
        buf.extend_from_slice(a.address.as_slice());
        buf.extend_from_slice(&a.nonce.to_be_bytes());
        buf.extend_from_slice(&a.balance.to_be_bytes::<32>());
        buf.extend_from_slice(a.code_hash.as_slice());
    }
    for ch in &codes {
        buf.extend_from_slice(ch.as_slice());
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
/// It writes every account and code entry, plus the
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
pub fn seed_genesis(
    env: &StateEnv,
    accounts: &[AccountChange],
    code: &[CodeEntry],
) -> Result<bool, StateError> {
    let digest = genesis_digest(accounts, code);
    let txn = env.raw().begin_rw_sync()?;
    let seed = GenesisSeed::open(&txn)?;

    if seed.already_applied()? {
        seed.check_or_backfill_digest(digest)?;
        txn.commit()?;
        return Ok(false);
    }

    seed.write_allocations(accounts, code)?;

    // Build the initial hashed-state mirror and account trie. This lets a
    // from-genesis validator start at the correct world-state root before
    // block 1. This step is harmless for the executor: its writer runs
    // with `TrieMode::Off` and ignores these tables.
    seed.seed_trie(accounts, code)?;

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
}

impl<'a> GenesisSeed<'a> {
    fn open(txn: &'a signet_libmdbx::tx::aliases::RwTxSync) -> Result<Self, StateError> {
        Ok(Self {
            meta: txn.open_db(Some(TABLE_META))?,
            accounts_db: txn.open_db(Some(TABLE_ACCOUNTS))?,
            code_db: txn.open_db(Some(TABLE_CODE))?,
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

    /// Write every account and code entry of the allocation.
    fn write_allocations(
        &self,
        accounts: &[AccountChange],
        code: &[CodeEntry],
    ) -> Result<(), StateError> {
        crate::schema::write_accounts(self.txn, self.accounts_db, accounts)?;
        crate::schema::write_code(self.txn, self.code_db, code)?;
        Ok(())
    }

    /// Seed the hashed-state mirror and account trie from the allocation, as
    /// one synthetic genesis block, and persist the resulting world-state
    /// root. This lets a from-genesis validator start at the correct root
    /// before block 1; it is harmless for the executor, whose writer runs
    /// with `TrieMode::Off` and ignores these tables.
    fn seed_trie(
        &self,
        accounts: &[AccountChange],
        code: &[CodeEntry],
    ) -> Result<B256, StateError> {
        let genesis_delta = kardamom_types::BlockDelta {
            block_number: 0,
            accounts: accounts.to_vec(),
            storage: Vec::new(),
            code: code.to_vec(),
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
    use kardamom_types::StateDatabase;

    #[test]
    fn seed_genesis_writes_accounts_and_code() {
        let (_dir, env) = temp_env();
        let addr = Address::from([0x11; 20]);
        let code = Bytes::from_static(b"\x60\x00");
        let code_hash = keccak256(&code);
        let accounts = vec![AccountChange {
            address: addr,
            nonce: 3,
            balance: U256::from(1_000u64),
            code_hash,
        }];
        let codes = vec![CodeEntry {
            code_hash,
            code: code.clone(),
        }];

        assert!(seed_genesis(&env, &accounts, &codes).unwrap());

        let snap = StateSnapshot::open(&env).unwrap();
        let (nonce, balance, ch) = snap.basic(addr).unwrap().unwrap();
        assert_eq!(nonce, 3);
        assert_eq!(balance, U256::from(1_000u64));
        assert_eq!(ch, code_hash);
        assert_eq!(snap.code_by_hash(code_hash).unwrap(), code);
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
        assert!(seed_genesis(&env, &accounts, &[]).unwrap());

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
        assert!(seed_genesis(&env, &accounts, &[]).unwrap());
        assert!(genesis_applied(&env).unwrap());
        // Second call is a no-op.
        assert!(!seed_genesis(&env, &accounts, &[]).unwrap());

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
        assert!(seed_genesis(&env, &accounts, &[]).unwrap());

        let mut changed = accounts.clone();
        changed[0].balance = U256::from(999u64);
        let err = seed_genesis(&env, &changed, &[]).unwrap_err();
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
        assert!(seed_genesis(&env2, &two, &[]).unwrap());
        two.reverse();
        assert!(!seed_genesis(&env2, &two, &[]).unwrap());
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
        assert!(seed_genesis(&env, &accounts, &[]).unwrap());

        // Simulate an env seeded before the digest key existed.
        {
            let txn = env.raw().begin_rw_sync().unwrap();
            let meta = txn.open_db(Some(TABLE_META)).unwrap();
            txn.del(meta, KEY_GENESIS_DIGEST, None).unwrap();
            txn.commit().unwrap();
        }

        // The first restart backfills the digest. Verification is not
        // possible yet.
        assert!(!seed_genesis(&env, &accounts, &[]).unwrap());
        // After that, a changed allocation is rejected.
        let mut changed = accounts.clone();
        changed[0].nonce = 9;
        assert!(matches!(
            seed_genesis(&env, &changed, &[]).unwrap_err(),
            StateError::GenesisMismatch { .. }
        ));
    }

    #[test]
    fn seed_genesis_empty_alloc_sets_flag() {
        let (_dir, env) = temp_env();
        assert!(seed_genesis(&env, &[], &[]).unwrap());
        assert!(genesis_applied(&env).unwrap());
        assert!(!seed_genesis(&env, &[], &[]).unwrap());
    }
}

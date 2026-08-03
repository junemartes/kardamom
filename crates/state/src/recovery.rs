//! Cold-start recovery (§5).
//!
//! On startup the writer:
//! 1. opens (or creates) the env (`StateEnvBuilder::open`),
//! 2. reads the meta cursors via [`read_recovery_point`],
//! 3. opens an initial snapshot,
//! 4. emits a [`RecoveryPoint`] that tells the executor where to resume
//!    reading B from.
//!
//! Recovery itself is read-only — no replay logic lives in this crate; the
//! executor consumes B from `recovery_point.last_fsynced_b_position` and
//! re-derives any blocks the writer never got to commit.

use kardamom_types::BPosition;

use crate::env::StateEnv;
use crate::error::StateError;
use crate::meta::{
    KEY_LAST_COMMITTED_BLOCK, KEY_LAST_COMMITTED_END_TX_POSITION, KEY_LAST_FSYNCED_B_POSITION,
    decode_b_position, decode_u64,
};
use crate::schema::{
    HeaderValue, TABLE_HEADERS, TABLE_META, decode_header_value, encode_block_key,
};

/// Cursors read out of the `meta` table at startup. The writer uses this to
/// hand the executor a resume point.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RecoveryPoint {
    pub last_committed_block: u64,
    pub last_committed_end_tx_position: BPosition,
    pub last_fsynced_b_position: BPosition,
    /// The committed block's boundary `l2_timestamp` (from its `headers` row).
    /// The exec thread seeds its block-timestamp state from this on a
    /// resume-from-cursor: block N+1's txs execute with boundary N's timestamp,
    /// and a resumed replica no longer sees boundary N — deriving it from
    /// anything else would diverge from the replicas that never restarted.
    /// 0 when nothing is committed yet (fresh DB / genesis-only).
    pub last_committed_l2_timestamp: u64,
}

pub fn read_recovery_point(env: &StateEnv) -> Result<RecoveryPoint, StateError> {
    let txn = env.raw().begin_ro_sync()?;
    let meta = txn.open_db(Some(TABLE_META))?;

    let last_committed_block = match txn.get::<Vec<u8>>(meta.dbi(), KEY_LAST_COMMITTED_BLOCK)? {
        Some(b) => decode_u64(&b)?,
        None => 0,
    };
    let last_committed_end_tx_position =
        match txn.get::<Vec<u8>>(meta.dbi(), KEY_LAST_COMMITTED_END_TX_POSITION)? {
            Some(b) => decode_b_position(&b)?,
            None => BPosition::ZERO,
        };
    let last_fsynced_b_position =
        match txn.get::<Vec<u8>>(meta.dbi(), KEY_LAST_FSYNCED_B_POSITION)? {
            Some(b) => decode_b_position(&b)?,
            None => BPosition::ZERO,
        };
    // The committed block's header row is written in the same txn as the meta
    // cursors, so present-cursor/absent-header means a corrupt env — surface
    // it rather than defaulting (a wrong timestamp silently diverges state).
    let last_committed_l2_timestamp = if last_committed_block > 0 {
        let headers = txn.open_db(Some(TABLE_HEADERS))?;
        match txn.get::<Vec<u8>>(headers.dbi(), &encode_block_key(last_committed_block))? {
            Some(b) => decode_header_value(&b)?.l2_timestamp,
            None => {
                return Err(StateError::Recovery(format!(
                    "meta cursor says block {last_committed_block} committed but its headers row is missing"
                )));
            }
        }
    } else {
        0
    };

    Ok(RecoveryPoint {
        last_committed_block,
        last_committed_end_tx_position,
        last_fsynced_b_position,
        last_committed_l2_timestamp,
    })
}

/// Whether the env carries a populated trie (hashed mirror + stored nodes).
/// An EXECUTOR checkpoint has neither — its writer runs trie-off — so a
/// validator adopting one must [`bootstrap_trie_from_state`] before spawning
/// its trie-aware writer (issue #143). Probed as "hashed_accounts non-empty
/// OR accounts table empty" (an empty env legitimately has no trie yet;
/// genesis seeding builds it).
pub fn has_trie(env: &StateEnv) -> Result<bool, StateError> {
    let txn = env.raw().begin_ro_sync()?;
    let accounts_db = txn.open_db(Some(crate::schema::TABLE_ACCOUNTS))?;
    let mut cur = txn.cursor(accounts_db)?;
    if cur.first::<Vec<u8>, Vec<u8>>()?.is_none() {
        return Ok(true); // nothing to mirror; genesis seed will build it
    }
    let hashed_db = txn.open_db(Some(crate::schema::TABLE_HASHED_ACCOUNTS))?;
    let mut cur = txn.cursor(hashed_db)?;
    Ok(cur.first::<Vec<u8>, Vec<u8>>()?.is_some())
}

/// Build the hashed-state mirror + account/storage tries from the PLAIN state
/// tables, returning the world-state root — the one-time adoption step for a
/// state image produced by a trie-off writer (an executor checkpoint fetched
/// by the validator's replay-unavailable fallback, issue #143).
///
/// Mechanically this is the genesis seeding path generalized: every account,
/// storage slot and code entry is folded into one synthetic [`BlockDelta`]
/// and pushed through [`crate::trie::update_for_block`] — the SAME function
/// that maintains the trie incrementally, so the resulting root is
/// byte-identical to one grown block-by-block (pinned by the
/// `incremental_equals_full_rebuild` equivalence test). Runs in one RW txn:
/// crash-safe (a torn bootstrap aborts wholesale and reruns on next start),
/// idempotent (rerunning on a populated mirror upserts the same rows).
pub fn bootstrap_trie_from_state(env: &StateEnv) -> Result<alloy_primitives::B256, StateError> {
    use crate::schema::{
        TABLE_ACCOUNTS, TABLE_CODE, TABLE_STORAGE, decode_account_value, decode_storage_value,
    };
    use alloy_primitives::{Address, B256, U256};
    use signet_libmdbx::WriteFlags;

    let txn = env.raw().begin_rw_sync()?;

    let accounts_db = txn.open_db(Some(TABLE_ACCOUNTS))?;
    let mut accounts = Vec::new();
    {
        let mut cur = txn.cursor(accounts_db)?;
        let mut item = cur.first::<Vec<u8>, Vec<u8>>()?;
        while let Some((k, v)) = item {
            if k.len() != 20 {
                return Err(StateError::Recovery(format!(
                    "accounts key of length {} during trie bootstrap",
                    k.len()
                )));
            }
            let a = decode_account_value(&v)?;
            accounts.push(kardamom_types::AccountChange {
                address: Address::from_slice(&k),
                nonce: a.nonce,
                balance: a.balance,
                code_hash: a.code_hash,
            });
            item = cur.next::<Vec<u8>, Vec<u8>>()?;
        }
    }

    let storage_db = txn.open_db(Some(TABLE_STORAGE))?;
    let mut storage = Vec::new();
    {
        let mut cur = txn.cursor(storage_db)?;
        let mut item = cur.first::<Vec<u8>, Vec<u8>>()?;
        while let Some((k, v)) = item {
            if k.len() != 52 {
                return Err(StateError::Recovery(format!(
                    "storage key of length {} during trie bootstrap",
                    k.len()
                )));
            }
            let value: U256 = decode_storage_value(&v)?;
            storage.push(kardamom_types::StorageChange {
                address: Address::from_slice(&k[..20]),
                key: B256::from_slice(&k[20..]),
                value,
            });
            item = cur.next::<Vec<u8>, Vec<u8>>()?;
        }
    }

    let code_db = txn.open_db(Some(TABLE_CODE))?;
    let mut code = Vec::new();
    {
        let mut cur = txn.cursor(code_db)?;
        let mut item = cur.first::<Vec<u8>, Vec<u8>>()?;
        while let Some((k, v)) = item {
            code.push(kardamom_types::CodeEntry {
                code_hash: B256::from_slice(&k),
                code: v.into(),
            });
            item = cur.next::<Vec<u8>, Vec<u8>>()?;
        }
    }

    let delta = kardamom_types::BlockDelta {
        block_number: 0,
        accounts,
        storage,
        code,
        receipts: Vec::new(),
    };
    let trie_tables = crate::trie::TrieTables::open(&txn)?;
    let root = crate::trie::update_for_block(&txn, &trie_tables, &delta)?;
    let meta = txn.open_db(Some(TABLE_META))?;
    txn.put(
        meta,
        crate::meta::KEY_STATE_ROOT,
        crate::meta::encode_b256(root),
        WriteFlags::UPSERT,
    )?;
    txn.commit()?;
    Ok(root)
}

/// Every persisted block header, in block order.
///
/// Read-only scan of `headers`; used by verification tooling and the
/// chain-semantics suite to assert properties that span the whole chain (the
/// L1-origin sequence, boundary alignment) rather than a single block.
/// Nothing serves headers over RPC, so this is the only way to observe them.
pub fn read_all_headers(env: &StateEnv) -> Result<Vec<(u64, HeaderValue)>, StateError> {
    let txn = env.raw().begin_ro_sync()?;
    let headers = txn.open_db(Some(TABLE_HEADERS))?;
    let mut cur = txn.cursor(headers)?;
    let mut out = Vec::new();
    // Keys are block numbers big-endian, so mdbx's byte order IS block order.
    let mut item = cur.first::<Vec<u8>, Vec<u8>>()?;
    while let Some((k, v)) = item {
        if k.len() != 8 {
            return Err(StateError::BadEncoding {
                table: TABLE_HEADERS,
                expected: 8,
                got: k.len(),
            });
        }
        let block_number = u64::from_be_bytes(k[..8].try_into().expect("8 bytes"));
        out.push((block_number, decode_header_value(&v)?));
        item = cur.next::<Vec<u8>, Vec<u8>>()?;
    }
    Ok(out)
}

#[cfg(test)]
mod trie_bootstrap_tests {
    use super::*;
    use crate::env::StateEnvBuilder;
    use crate::schema::{
        AccountValue, TABLE_ACCOUNTS, TABLE_STORAGE, encode_account_key, encode_account_value,
        encode_storage_key, encode_storage_value,
    };
    use alloy_primitives::{Address, B256, U256};
    use signet_libmdbx::WriteFlags;

    /// Adoption drill (#143): a plain trie-off state image (what an executor
    /// checkpoint restores to) bootstraps a mirror + trie whose root matches
    /// the pure full-rebuild oracle, and the probe flips accordingly.
    #[test]
    fn bootstrap_builds_trie_matching_oracle_on_trie_off_state() {
        let dir = tempfile::tempdir().unwrap();
        let env = StateEnvBuilder::new(dir.path()).open().unwrap();

        let addr_a = Address::repeat_byte(0x11);
        let addr_b = Address::repeat_byte(0x22);
        let slot = B256::repeat_byte(0x03);
        let slot_val = U256::from(7u64);

        // Write plain state the way a trie-off writer leaves it: accounts +
        // storage tables only, no hashed mirror, no trie nodes.
        {
            let txn = env.raw().begin_rw_sync().unwrap();
            let accounts_db = txn.open_db(Some(TABLE_ACCOUNTS)).unwrap();
            for (addr, nonce, bal) in [(addr_a, 3u64, 100u64), (addr_b, 1, 55)] {
                let v = AccountValue {
                    nonce,
                    balance: U256::from(bal),
                    code_hash: B256::ZERO,
                    storage_root: B256::ZERO,
                };
                txn.put(
                    accounts_db,
                    encode_account_key(addr),
                    encode_account_value(&v),
                    WriteFlags::UPSERT,
                )
                .unwrap();
            }
            let storage_db = txn.open_db(Some(TABLE_STORAGE)).unwrap();
            txn.put(
                storage_db,
                encode_storage_key(addr_b, slot),
                encode_storage_value(slot_val),
                WriteFlags::UPSERT,
            )
            .unwrap();
            txn.commit().unwrap();
        }
        assert!(!has_trie(&env).unwrap(), "trie-off image must probe absent");

        let root = bootstrap_trie_from_state(&env).unwrap();
        assert!(
            has_trie(&env).unwrap(),
            "bootstrap must populate the mirror"
        );
        assert_ne!(root, crate::trie::empty_root());

        // Oracle: pure full rebuild over the same accounts, with B's storage
        // root computed from its one slot.
        let storage_root = crate::trie::storage_root([(slot, slot_val)]);
        let want = crate::trie::state_root([
            (
                addr_a,
                crate::trie::AccountTrieParts {
                    nonce: 3,
                    balance: U256::from(100u64),
                    code_hash: B256::ZERO,
                    storage_root: B256::ZERO,
                },
            ),
            (
                addr_b,
                crate::trie::AccountTrieParts {
                    nonce: 1,
                    balance: U256::from(55u64),
                    code_hash: B256::ZERO,
                    storage_root,
                },
            ),
        ]);
        assert_eq!(root, want, "bootstrap root must equal the rebuild oracle");

        // Idempotent: re-running lands on the same root.
        assert_eq!(bootstrap_trie_from_state(&env).unwrap(), root);
    }

    /// The REAL adopted-executor-checkpoint shape (validator-join chaos case
    /// finding): every env carries the GENESIS-seeded mirror + trie (built by
    /// seed_genesis, then never updated by a trie-off writer), so an adopted
    /// image has a stale-at-genesis mirror under newer plain state. The
    /// bootstrap must converge that to the oracle root of the CURRENT state —
    /// and a presence probe alone would wrongly skip it (which is why
    /// adoption is marker-signaled, not probed).
    #[test]
    fn bootstrap_corrects_genesis_stale_mirror_under_newer_state() {
        let dir = tempfile::tempdir().unwrap();
        let env = StateEnvBuilder::new(dir.path()).open().unwrap();
        let addr_a = Address::repeat_byte(0x11);
        let addr_b = Address::repeat_byte(0x22);

        // Genesis: A exists with nonce 0 / balance 1000. Seeds mirror + trie.
        let genesis = [kardamom_types::AccountChange {
            address: addr_a,
            nonce: 0,
            balance: U256::from(1000u64),
            code_hash: B256::ZERO,
        }];
        assert!(crate::genesis::seed_genesis(&env, &genesis, &[]).unwrap());
        assert!(has_trie(&env).unwrap(), "genesis seed builds the mirror");

        // Trie-off progress (what an executor does): A changes, B appears —
        // plain tables only, mirror left frozen at genesis.
        {
            let txn = env.raw().begin_rw_sync().unwrap();
            let accounts_db = txn.open_db(Some(TABLE_ACCOUNTS)).unwrap();
            for (addr, nonce, bal) in [(addr_a, 7u64, 400u64), (addr_b, 2, 600)] {
                let v = AccountValue {
                    nonce,
                    balance: U256::from(bal),
                    code_hash: B256::ZERO,
                    storage_root: B256::ZERO,
                };
                txn.put(
                    accounts_db,
                    encode_account_key(addr),
                    encode_account_value(&v),
                    WriteFlags::UPSERT,
                )
                .unwrap();
            }
            txn.commit().unwrap();
        }
        // The trap: the probe still says "trie present" (it is — stale).
        assert!(has_trie(&env).unwrap());

        let root = bootstrap_trie_from_state(&env).unwrap();
        let want = crate::trie::state_root([
            (
                addr_a,
                crate::trie::AccountTrieParts {
                    nonce: 7,
                    balance: U256::from(400u64),
                    code_hash: B256::ZERO,
                    storage_root: B256::ZERO,
                },
            ),
            (
                addr_b,
                crate::trie::AccountTrieParts {
                    nonce: 2,
                    balance: U256::from(600u64),
                    code_hash: B256::ZERO,
                    storage_root: B256::ZERO,
                },
            ),
        ]);
        assert_eq!(
            root, want,
            "bootstrap must converge a stale mirror to the current-state oracle"
        );
    }

    /// The join must not just adopt — VERIFIED execution continues on top of
    /// the bootstrapped trie. After bootstrap, applying the next block
    /// INCREMENTALLY must land on the same root as a pure full rebuild over
    /// the merged state (the property the validator's post-adoption
    /// verification rides on).
    #[test]
    fn incremental_block_on_bootstrapped_trie_matches_oracle() {
        let dir = tempfile::tempdir().unwrap();
        let env = StateEnvBuilder::new(dir.path()).open().unwrap();
        let addr_a = Address::repeat_byte(0x11);
        let addr_c = Address::repeat_byte(0x33);

        {
            let txn = env.raw().begin_rw_sync().unwrap();
            let accounts_db = txn.open_db(Some(TABLE_ACCOUNTS)).unwrap();
            let v = AccountValue {
                nonce: 3,
                balance: U256::from(100u64),
                code_hash: B256::ZERO,
                storage_root: B256::ZERO,
            };
            txn.put(
                accounts_db,
                encode_account_key(addr_a),
                encode_account_value(&v),
                WriteFlags::UPSERT,
            )
            .unwrap();
            txn.commit().unwrap();
        }
        bootstrap_trie_from_state(&env).unwrap();

        // "Next block": A bumps its nonce, C appears.
        let delta = kardamom_types::BlockDelta {
            block_number: 1,
            accounts: vec![
                kardamom_types::AccountChange {
                    address: addr_a,
                    nonce: 4,
                    balance: U256::from(90u64),
                    code_hash: B256::ZERO,
                },
                kardamom_types::AccountChange {
                    address: addr_c,
                    nonce: 1,
                    balance: U256::from(10u64),
                    code_hash: B256::ZERO,
                },
            ],
            storage: Vec::new(),
            code: Vec::new(),
            receipts: Vec::new(),
        };
        let txn = env.raw().begin_rw_sync().unwrap();
        let tables = crate::trie::TrieTables::open(&txn).unwrap();
        let root = crate::trie::update_for_block(&txn, &tables, &delta).unwrap();
        txn.commit().unwrap();

        let want = crate::trie::state_root([
            (
                addr_a,
                crate::trie::AccountTrieParts {
                    nonce: 4,
                    balance: U256::from(90u64),
                    code_hash: B256::ZERO,
                    storage_root: B256::ZERO,
                },
            ),
            (
                addr_c,
                crate::trie::AccountTrieParts {
                    nonce: 1,
                    balance: U256::from(10u64),
                    code_hash: B256::ZERO,
                    storage_root: B256::ZERO,
                },
            ),
        ]);
        assert_eq!(
            root, want,
            "incremental block on a bootstrapped trie must match the oracle"
        );
    }
}

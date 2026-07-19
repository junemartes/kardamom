//! Canonical Ethereum MPT state root over the libmdbx state — **node-incremental**.
//!
//! Reth's state-root model on `alloy_trie` primitives: stored `BranchNodeCompact`
//! intermediate nodes ([`node`] codec, kept in the `account_trie`/`storage_trie`
//! tables) plus a hashed-state mirror (`hashed_accounts`/`hashed_storage`). Per
//! block, [`update_for_block`] updates the mirror and walks only the changed
//! key-prefixes ([`walker`], driven by a [`PrefixSet`] over [`cursor`]s),
//! skipping unchanged subtries via their stored hash. On a large, dense trie
//! this brings the per-block root cost down from O(all accounts) toward
//! O(changed keys) — but not all the way: the skip only fires where a stored
//! node sits at the exact child path with its hash bit set. Extension-shaped
//! children (whose parent hash bit `HashBuilder` clears) and any subtrie whose
//! node is stored deeper than the exact path get re-walked from leaves even
//! when unchanged, so small/sparse tries — including any trie whose top-level
//! node is an extension — repeatedly pay full-subtree rebuilds. `crate::writer`
//! drives it inside the block-commit txn so the root advances atomically with
//! state.
//!
//! The pure `state_root` / `storage_root` rebuild functions below are retained as
//! the **shadow-check oracle** ([`rebuild_root`]) and the equivalence-test
//! reference; the **root value** is identical to a full rebuild — proven by the
//! 50-block `incremental_equals_full_rebuild` test.

pub mod cursor;
pub mod node;
pub mod prefix_set;
pub mod walker;

#[cfg(test)]
mod incremental_tests;

pub use prefix_set::PrefixSet;
pub use walker::TrieUpdates;

use alloy_primitives::{Address, B256, U256};
use alloy_rlp::Encodable;
use alloy_trie::{EMPTY_ROOT_HASH, KECCAK_EMPTY, TrieAccount, root};
use signet_libmdbx::Database;
use signet_libmdbx::tx::aliases::RwTxSync;

use crate::error::StateError;

/// Node-incremental state-root computation over the stored trie tables. The
/// pure `state_root`/`storage_root` rebuild fns below remain the shadow-check
/// oracle.
pub struct StateRoot;

impl StateRoot {
    /// One account's storage-trie root, incrementally. `prefix_set` holds
    /// `keccak(slot)` of the account's changed slots.
    pub fn storage_root_incremental(
        tx: &RwTxSync,
        storage_trie: Database,
        hashed_storage: Database,
        account_hash: B256,
        prefix_set: &PrefixSet,
    ) -> Result<(B256, TrieUpdates), StateError> {
        walker::storage_root(tx, storage_trie, hashed_storage, &account_hash, prefix_set)
    }

    /// The world-state account-trie root, incrementally. `prefix_set` holds
    /// `keccak(addr)` of every changed account (incl. storage-root changes).
    pub fn state_root_incremental(
        tx: &RwTxSync,
        account_trie: Database,
        hashed_accounts: Database,
        prefix_set: &PrefixSet,
    ) -> Result<(B256, TrieUpdates), StateError> {
        let leaf = |p: &AccountTrieParts| {
            let mut buf = Vec::new();
            p.to_trie_account().encode(&mut buf);
            buf
        };
        walker::account_root(tx, account_trie, hashed_accounts, prefix_set, &leaf)
    }
}

/// Persist a walk's [`TrieUpdates`] to a node table: range-delete each cleared
/// subtrie prefix (stale nodes a leaf rebuild may have orphaned under
/// extensions), then upsert each produced branch node, then delete each
/// collapsed path. Clears run **before** upserts so freshly produced nodes
/// inside a cleared region survive. `account_hash` namespaces storage-trie keys
/// (`None` for the account trie). Used by the writer and the test harness.
pub fn apply_trie_updates(
    txn: &RwTxSync,
    db: Database,
    account_hash: Option<&B256>,
    updates: &TrieUpdates,
) -> Result<(), StateError> {
    use signet_libmdbx::WriteFlags;
    let key = |path: &alloy_trie::Nibbles| -> Vec<u8> { cursor::node_key(account_hash, path) };
    for path in &updates.cleared {
        del_prefix(txn, db, &key(path))?;
    }
    for (path, node) in &updates.upserts {
        txn.put(
            db,
            key(path),
            node::encode_branch_node(node),
            WriteFlags::UPSERT,
        )?;
    }
    for path in &updates.removals {
        match txn.del(db, key(path), None) {
            Ok(_) => {}
            Err(signet_libmdbx::MdbxError::NotFound) => {}
            Err(e) => return Err(e.into()),
        }
    }
    Ok(())
}

/// The four trie tables, opened once per block-commit txn.
pub struct TrieTables {
    pub account_trie: Database,
    pub storage_trie: Database,
    pub hashed_accounts: Database,
    pub hashed_storage: Database,
}

impl TrieTables {
    pub fn open(txn: &RwTxSync) -> Result<Self, StateError> {
        use crate::schema::{
            TABLE_ACCOUNT_TRIE, TABLE_HASHED_ACCOUNTS, TABLE_HASHED_STORAGE, TABLE_STORAGE_TRIE,
        };
        Ok(Self {
            account_trie: txn.open_db(Some(TABLE_ACCOUNT_TRIE))?,
            storage_trie: txn.open_db(Some(TABLE_STORAGE_TRIE))?,
            hashed_accounts: txn.open_db(Some(TABLE_HASHED_ACCOUNTS))?,
            hashed_storage: txn.open_db(Some(TABLE_HASHED_STORAGE))?,
        })
    }
}

/// Delete every row in `db` whose key starts with `prefix` (used to drop a
/// deleted account's whole hashed-storage / storage-trie subtree).
fn del_prefix(txn: &RwTxSync, db: Database, prefix: &[u8]) -> Result<(), StateError> {
    let mut keys: Vec<Vec<u8>> = Vec::new();
    {
        let mut cur = txn.cursor(db)?;
        let mut item = cur.set_range::<Vec<u8>, Vec<u8>>(prefix)?;
        while let Some((k, _)) = item {
            if !k.starts_with(prefix) {
                break;
            }
            keys.push(k);
            item = cur.next::<Vec<u8>, Vec<u8>>()?;
        }
    }
    for k in keys {
        match txn.del(db, k, None) {
            Ok(_) => {}
            Err(signet_libmdbx::MdbxError::NotFound) => {}
            Err(e) => return Err(e.into()),
        }
    }
    Ok(())
}

/// Apply one block's `BlockDelta` to the hashed-state mirror and the stored
/// tries, returning the new canonical world-state root. Runs inside the writer's
/// block-commit txn (so the root advances atomically with the state). Mirrors
/// the equivalence-tested harness: storage tries first (stamping each account's
/// `storage_root` into the hashed account), then the account trie.
pub fn update_for_block(
    txn: &RwTxSync,
    t: &TrieTables,
    delta: &kardamom_types::BlockDelta,
) -> Result<B256, StateError> {
    use alloy_primitives::keccak256;
    use signet_libmdbx::WriteFlags;
    use std::collections::{BTreeMap, BTreeSet};

    let mut touched: BTreeSet<Address> = BTreeSet::new();
    let mut new_sroot: BTreeMap<Address, B256> = BTreeMap::new();

    // --- storage tries first ---
    let mut stor_by: BTreeMap<Address, Vec<(B256, U256)>> = BTreeMap::new();
    for s in &delta.storage {
        stor_by.entry(s.address).or_default().push((s.key, s.value));
    }
    for (addr, changes) in &stor_by {
        let ah = keccak256(addr);
        let mut changed = Vec::with_capacity(changes.len());
        for (slot, val) in changes {
            let sh = keccak256(slot);
            changed.push(sh);
            let mut key = ah.as_slice().to_vec();
            key.extend_from_slice(sh.as_slice());
            if val.is_zero() {
                // Absent-key deletes are fine; any other mdbx failure must
                // surface, or the mirror silently diverges from the reference.
                match txn.del(t.hashed_storage, key, None) {
                    Ok(_) | Err(signet_libmdbx::MdbxError::NotFound) => {}
                    Err(e) => return Err(e.into()),
                }
            } else {
                txn.put(
                    t.hashed_storage,
                    key,
                    val.to_be_bytes::<32>(),
                    WriteFlags::UPSERT,
                )?;
            }
        }
        let ps = PrefixSet::from_b256s(changed);
        let (sr, up) =
            StateRoot::storage_root_incremental(txn, t.storage_trie, t.hashed_storage, ah, &ps)?;
        apply_trie_updates(txn, t.storage_trie, Some(&ah), &up)?;
        new_sroot.insert(*addr, sr);
        touched.insert(*addr);
    }

    let mut basics: BTreeMap<Address, (u64, U256, B256)> = BTreeMap::new();
    for a in &delta.accounts {
        basics.insert(a.address, (a.nonce, a.balance, a.code_hash));
        touched.insert(a.address);
    }

    // --- hashed_accounts rows for every touched account ---
    for addr in &touched {
        let ah = keccak256(addr);
        let existing = cursor::get_hashed_account(txn, t.hashed_accounts, &ah)?;
        let (nonce, balance, code_hash) = match basics.get(addr) {
            Some(b) => *b,
            None => existing
                .map(|e| (e.nonce, e.balance, e.code_hash))
                .unwrap_or((0, U256::ZERO, B256::ZERO)),
        };
        let storage_root = new_sroot
            .get(addr)
            .copied()
            .or_else(|| existing.map(|e| e.storage_root))
            .unwrap_or(EMPTY_ROOT_HASH);
        let parts = AccountTrieParts {
            nonce,
            balance,
            code_hash,
            storage_root,
        };
        if parts.is_empty() {
            match txn.del(t.hashed_accounts, ah.as_slice(), None) {
                Ok(_) | Err(signet_libmdbx::MdbxError::NotFound) => {}
                Err(e) => return Err(e.into()),
            }
            del_prefix(txn, t.hashed_storage, ah.as_slice())?;
            del_prefix(txn, t.storage_trie, ah.as_slice())?;
        } else {
            txn.put(
                t.hashed_accounts,
                ah.as_slice(),
                cursor::encode_account_leaf(&parts),
                WriteFlags::UPSERT,
            )?;
        }
    }

    // --- account trie ---
    let ps = PrefixSet::from_b256s(touched.iter().map(keccak256));
    let (root, up) =
        StateRoot::state_root_incremental(txn, t.account_trie, t.hashed_accounts, &ps)?;
    apply_trie_updates(txn, t.account_trie, None, &up)?;
    Ok(root)
}

/// Independent full-rebuild of the world-state root from the hashed mirror, used
/// by the writer's shadow-check. Different code path than the incremental walker
/// (alloy-trie's one-shot pre-hashed root builders), so a walker bug diverges.
pub fn rebuild_root(txn: &RwTxSync, t: &TrieTables) -> Result<B256, StateError> {
    // Per-account storage root recomputed from the hashed storage mirror.
    let storage_root_for = |ah: &B256| -> Result<B256, StateError> {
        let mut cur = txn.cursor(t.hashed_storage)?;
        let mut pairs: Vec<(B256, U256)> = Vec::new();
        let mut item = cur.set_range::<Vec<u8>, Vec<u8>>(ah.as_slice())?;
        while let Some((k, v)) = item {
            if k.len() != 64 || &k[0..32] != ah.as_slice() {
                break;
            }
            pairs.push((B256::from_slice(&k[32..64]), U256::from_be_slice(&v)));
            item = cur.next::<Vec<u8>, Vec<u8>>()?;
        }
        Ok(root::storage_root_unsorted(pairs))
    };

    let mut accts: Vec<(B256, TrieAccount)> = Vec::new();
    {
        let mut cur = txn.cursor(t.hashed_accounts)?;
        let mut item = cur.first::<Vec<u8>, Vec<u8>>()?;
        while let Some((k, v)) = item {
            let ah = B256::from_slice(&k);
            let parts = cursor::decode_account_leaf(&v)?;
            let mut acc = parts.to_trie_account();
            acc.storage_root = storage_root_for(&ah)?;
            accts.push((ah, acc));
            item = cur.next::<Vec<u8>, Vec<u8>>()?;
        }
    }
    Ok(root::state_root_unsorted(accts))
}

/// The basic account fields needed to form an account-trie leaf. (`AccountValue`
/// in [`crate::schema`] stores these plus the persisted `storage_root`.)
#[derive(Debug, Clone, Copy)]
pub struct AccountTrieParts {
    pub nonce: u64,
    pub balance: U256,
    pub code_hash: B256,
    pub storage_root: B256,
}

impl AccountTrieParts {
    /// Canonical code hash: kardamom seeds codeless accounts (and genesis allocs)
    /// with `B256::ZERO`, but an Ethereum trie leaf uses `KECCAK_EMPTY` for empty
    /// code. `ZERO` is never a valid code hash, so the mapping is unambiguous.
    fn canonical_code_hash(&self) -> B256 {
        if self.code_hash.is_zero() {
            KECCAK_EMPTY
        } else {
            self.code_hash
        }
    }

    /// Canonical storage root: `B256::ZERO` is the "no storage trie computed yet"
    /// sentinel (e.g. genesis-seeded accounts). An empty storage trie roots to
    /// `EMPTY_ROOT_HASH`; `ZERO` is never a valid MPT root, so the mapping is
    /// unambiguous.
    fn canonical_storage_root(&self) -> B256 {
        if self.storage_root.is_zero() {
            EMPTY_ROOT_HASH
        } else {
            self.storage_root
        }
    }

    /// EIP-161 emptiness: an account with zero nonce, zero balance, and no code
    /// is not present in the world-state trie. (Storage-bearing accounts have
    /// code in practice, so `storage_root` is not part of the emptiness test.)
    pub(crate) fn is_empty(&self) -> bool {
        self.nonce == 0 && self.balance.is_zero() && self.canonical_code_hash() == KECCAK_EMPTY
    }

    /// The canonical Ethereum account-trie leaf value for this account.
    pub(crate) fn to_trie_account(self) -> TrieAccount {
        TrieAccount {
            nonce: self.nonce,
            balance: self.balance,
            storage_root: self.canonical_storage_root(),
            code_hash: self.canonical_code_hash(),
        }
    }
}

/// The canonical empty world-state root.
pub fn empty_root() -> B256 {
    EMPTY_ROOT_HASH
}

/// Storage-trie root for one account from its `(slot, value)` pairs. Zero-valued
/// slots are omitted (they are absent from an Ethereum storage trie). Keys are
/// raw slots; alloy-trie hashes them (secure trie). An account with no non-zero
/// slots yields [`EMPTY_ROOT_HASH`].
pub fn storage_root(slots: impl IntoIterator<Item = (B256, U256)>) -> B256 {
    root::storage_root_unhashed(slots.into_iter().filter(|(_, v)| !v.is_zero()))
}

/// World-state root over the full account set. Empty accounts (EIP-161) are
/// omitted. Addresses are raw; alloy-trie hashes them (secure trie). An empty
/// account set yields [`EMPTY_ROOT_HASH`].
pub fn state_root(accounts: impl IntoIterator<Item = (Address, AccountTrieParts)>) -> B256 {
    root::state_root_unhashed(accounts.into_iter().filter(|(_, a)| !a.is_empty()).map(
        |(addr, a)| {
            (
                addr,
                TrieAccount {
                    nonce: a.nonce,
                    balance: a.balance,
                    storage_root: a.canonical_storage_root(),
                    code_hash: a.canonical_code_hash(),
                },
            )
        },
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloy_primitives::{address, b256};

    fn parts(nonce: u64, balance: u64, storage_root: B256) -> AccountTrieParts {
        AccountTrieParts {
            nonce,
            balance: U256::from(balance),
            code_hash: KECCAK_EMPTY,
            storage_root,
        }
    }

    #[test]
    fn empty_state_root_is_canonical() {
        assert_eq!(empty_root(), EMPTY_ROOT_HASH);
        assert_eq!(state_root(std::iter::empty()), EMPTY_ROOT_HASH);
        assert_eq!(storage_root(std::iter::empty()), EMPTY_ROOT_HASH);
    }

    #[test]
    fn empty_accounts_excluded_eip161() {
        // An account that is empty (nonce 0, balance 0, no code) must not be in
        // the trie — so a state of only-empty accounts roots to the empty trie.
        let empty = address!("0x00000000000000000000000000000000000000aa");
        assert_eq!(
            state_root([(empty, parts(0, 0, EMPTY_ROOT_HASH))]),
            EMPTY_ROOT_HASH
        );
        // A non-empty account is included (root differs from empty).
        let funded = address!("0x00000000000000000000000000000000000000bb");
        assert_ne!(
            state_root([(funded, parts(0, 1, EMPTY_ROOT_HASH))]),
            EMPTY_ROOT_HASH
        );
    }

    #[test]
    fn storage_root_skips_zero_slots() {
        let slot = b256!("0x0000000000000000000000000000000000000000000000000000000000000001");
        // A single zero-valued slot is the same as no storage.
        assert_eq!(storage_root([(slot, U256::ZERO)]), EMPTY_ROOT_HASH);
        // A non-zero slot changes the root.
        assert_ne!(storage_root([(slot, U256::from(7u64))]), EMPTY_ROOT_HASH);
    }

    #[test]
    fn state_root_is_order_independent() {
        let a = address!("0x0000000000000000000000000000000000000001");
        let b = address!("0x0000000000000000000000000000000000000002");
        let c = address!("0x0000000000000000000000000000000000000003");
        let forward = state_root([
            (a, parts(1, 10, EMPTY_ROOT_HASH)),
            (b, parts(2, 20, EMPTY_ROOT_HASH)),
            (c, parts(3, 30, EMPTY_ROOT_HASH)),
        ]);
        let reverse = state_root([
            (c, parts(3, 30, EMPTY_ROOT_HASH)),
            (b, parts(2, 20, EMPTY_ROOT_HASH)),
            (a, parts(1, 10, EMPTY_ROOT_HASH)),
        ]);
        assert_eq!(forward, reverse);
    }

    #[test]
    fn zero_sentinels_normalize_to_canonical() {
        // A funded account seeded with ZERO code_hash + ZERO storage_root (the
        // kardamom/genesis sentinels) must root identically to one built with
        // the canonical KECCAK_EMPTY + EMPTY_ROOT_HASH.
        let a = address!("0x0000000000000000000000000000000000000001");
        let sentinel = AccountTrieParts {
            nonce: 1,
            balance: U256::from(5u64),
            code_hash: B256::ZERO,
            storage_root: B256::ZERO,
        };
        let canonical = AccountTrieParts {
            nonce: 1,
            balance: U256::from(5u64),
            code_hash: KECCAK_EMPTY,
            storage_root: EMPTY_ROOT_HASH,
        };
        assert_eq!(state_root([(a, sentinel)]), state_root([(a, canonical)]));
        // And a ZERO-everything empty account is still excluded.
        let empty = AccountTrieParts {
            nonce: 0,
            balance: U256::ZERO,
            code_hash: B256::ZERO,
            storage_root: B256::ZERO,
        };
        assert_eq!(state_root([(a, empty)]), EMPTY_ROOT_HASH);
    }

    #[test]
    fn changing_a_field_changes_the_root() {
        let a = address!("0x0000000000000000000000000000000000000001");
        let base = state_root([(a, parts(1, 10, EMPTY_ROOT_HASH))]);
        assert_ne!(base, state_root([(a, parts(2, 10, EMPTY_ROOT_HASH))])); // nonce
        assert_ne!(base, state_root([(a, parts(1, 11, EMPTY_ROOT_HASH))])); // balance
        let sr = storage_root([(B256::from(U256::from(1u64)), U256::from(5u64))]);
        assert_ne!(base, state_root([(a, parts(1, 10, sr))])); // storage_root
    }
}

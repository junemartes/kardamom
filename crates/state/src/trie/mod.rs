//! The canonical Ethereum MPT state root over the libmdbx state. This
//! computation is node-incremental.
//!
//! This follows reth's state-root model, built on `alloy_trie`
//! primitives. It stores `BranchNodeCompact` intermediate nodes (the
//! [`node`] codec, kept in the `account_trie` and `storage_trie` tables),
//! plus a hashed-state mirror (`hashed_accounts` and `hashed_storage`).
//!
//! Per block, [`update_for_block`] updates the mirror and walks only the
//! changed key-prefixes ([`walker`], driven by a [`PrefixSet`] over
//! [`cursor`]s). It skips unchanged subtries using their stored hash.
//! On a large, dense trie, this brings the per-block root cost down from
//! O(all accounts) toward O(changed keys), but not all the way:
//!
//! - The skip only fires where a stored node sits at the exact child
//!   path, with its hash bit set.
//! - Extension-shaped children, whose parent hash bit `HashBuilder`
//!   clears, get re-walked from leaves even when unchanged.
//! - The same is true for any subtrie whose node is stored deeper than
//!   the exact path.
//!
//! So small or sparse tries, including any trie whose top-level node is
//! an extension, repeatedly pay full-subtree rebuilds. `crate::writer`
//! runs this inside the block-commit transaction, so the root advances
//! atomically with the state.
//!
//! The pure `state_root` and `storage_root` rebuild functions below
//! remain in place as the shadow-check oracle ([`rebuild_root`]) and the
//! equivalence-test reference. The resulting root value is identical to
//! a full rebuild. The 50-block `incremental_equals_full_rebuild` test
//! proves this.

pub mod cursor;
pub mod node;
pub mod prefix_set;
pub mod proofs;
pub mod walker;

#[cfg(test)]
mod incremental_tests;

pub use alloy_trie::Nibbles;
pub(crate) use prefix_set::PrefixSet;
pub(crate) use walker::TrieUpdates;

use alloy_primitives::{Address, B256, U256};
use alloy_rlp::Encodable;
use alloy_trie::{EMPTY_ROOT_HASH, KECCAK_EMPTY, TrieAccount, root};
use signet_libmdbx::Database;
use signet_libmdbx::tx::aliases::RwTxSync;
use std::collections::{BTreeMap, BTreeSet};

use crate::error::StateError;

/// Node-incremental state-root computation over the stored trie
/// tables. The pure `state_root` and `storage_root` rebuild functions
/// below remain the shadow-check oracle.
pub(crate) struct StateRoot;

impl StateRoot {
    /// One account's storage-trie root, computed incrementally.
    /// `prefix_set` holds `keccak(slot)` for the account's changed slots.
    ///
    /// # Errors
    ///
    /// Returns [`StateError`] if a table read fails.
    pub(crate) fn storage_root_incremental(
        tx: &RwTxSync,
        storage_trie: Database,
        hashed_storage: Database,
        account_hash: B256,
        prefix_set: &PrefixSet,
    ) -> Result<(B256, TrieUpdates), StateError> {
        walker::storage_root(tx, storage_trie, hashed_storage, &account_hash, prefix_set)
    }

    /// The world-state account-trie root, computed incrementally.
    /// `prefix_set` holds `keccak(addr)` for every changed account,
    /// including accounts with only a storage-root change.
    ///
    /// # Errors
    ///
    /// Returns [`StateError`] if a table read fails.
    pub(crate) fn state_root_incremental(
        tx: &RwTxSync,
        account_trie: Database,
        hashed_accounts: Database,
        prefix_set: &PrefixSet,
    ) -> Result<(B256, TrieUpdates), StateError> {
        walker::account_root(tx, account_trie, hashed_accounts, prefix_set)
    }
}

/// The account-trie leaf value: the RLP-encoded [`TrieAccount`]. The only
/// leaf shape the walker and the proof generator produce, so both call
/// this directly instead of threading a closure through the walk.
pub(crate) fn account_leaf_rlp(p: &AccountTrieParts) -> Vec<u8> {
    let mut buf = Vec::new();
    p.to_trie_account().encode(&mut buf);
    buf
}

/// The four trie tables, opened once per block-commit txn.
pub struct TrieTables {
    pub account_trie: Database,
    pub storage_trie: Database,
    pub hashed_accounts: Database,
    pub hashed_storage: Database,
}

impl TrieTables {
    /// # Errors
    ///
    /// Returns [`StateError`] if any of the four trie tables fails to open.
    pub fn open<K: cursor::ReadKind>(txn: &signet_libmdbx::TxSync<K>) -> Result<Self, StateError> {
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

    /// Persist a walk's [`TrieUpdates`] to a node table.
    ///
    /// This does three things, in order:
    ///
    /// 1. Range-delete each cleared subtrie prefix. These are stale nodes
    ///    that a leaf rebuild may have orphaned under extensions.
    /// 2. Upsert each produced branch node.
    /// 3. Delete each collapsed path.
    ///
    /// Clears must run before upserts, so freshly produced nodes inside a
    /// cleared region survive.
    ///
    /// `account_hash` namespaces storage-trie keys and picks the table:
    /// `Some` writes `storage_trie`, `None` writes `account_trie`. The
    /// writer and the test harness both use this method.
    ///
    /// # Errors
    ///
    /// Returns [`StateError`] if a table write or delete fails.
    fn apply_updates(
        &self,
        txn: &RwTxSync,
        account_hash: Option<&B256>,
        updates: &TrieUpdates,
    ) -> Result<(), StateError> {
        use signet_libmdbx::WriteFlags;
        let db = match account_hash {
            Some(_) => self.storage_trie,
            None => self.account_trie,
        };
        let key = |path: &alloy_trie::Nibbles| -> Vec<u8> { cursor::node_key(account_hash, path) };
        for path in &updates.cleared {
            crate::schema::del_prefix(txn, db, &key(path))?;
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
            crate::schema::del_if_present(txn, db, key(path))?;
        }
        Ok(())
    }

    /// Group one block's storage changes by account.
    fn group_storage_by_account(
        delta: &kardamom_types::BlockDelta,
    ) -> BTreeMap<Address, Vec<(B256, U256)>> {
        let mut stor_by: BTreeMap<Address, Vec<(B256, U256)>> = BTreeMap::new();
        for s in &delta.storage {
            stor_by.entry(s.address).or_default().push((s.key, s.value));
        }
        stor_by
    }

    /// Write every changed storage slot, then advance each changed account's
    /// storage-trie root. Adds every account with a storage change to
    /// `touched`. Returns the new storage root per such account.
    fn update_storage_tries(
        &self,
        txn: &RwTxSync,
        stor_by: &BTreeMap<Address, Vec<(B256, U256)>>,
        touched: &mut BTreeSet<Address>,
    ) -> Result<BTreeMap<Address, B256>, StateError> {
        use alloy_primitives::keccak256;

        let mut new_sroot: BTreeMap<Address, B256> = BTreeMap::new();
        for (addr, changes) in stor_by {
            let ah = keccak256(addr);
            let changed = self.write_account_slots(txn, ah, changes)?;
            let ps = PrefixSet::from_b256s(changed);
            let (sr, up) = StateRoot::storage_root_incremental(
                txn,
                self.storage_trie,
                self.hashed_storage,
                ah,
                &ps,
            )?;
            self.apply_updates(txn, Some(&ah), &up)?;
            new_sroot.insert(*addr, sr);
            touched.insert(*addr);
        }
        Ok(new_sroot)
    }

    /// Write one account's changed storage slots into `hashed_storage`.
    /// Returns `keccak(slot)` for each changed slot, for the caller's
    /// `PrefixSet`.
    fn write_account_slots(
        &self,
        txn: &RwTxSync,
        ah: B256,
        changes: &[(B256, U256)],
    ) -> Result<Vec<B256>, StateError> {
        changes
            .iter()
            .map(|(slot, val)| self.write_one_slot(txn, ah, *slot, *val))
            .collect()
    }

    /// Write (or delete, if zero) one account's changed storage slot into
    /// `hashed_storage`. Returns `keccak(slot)`, for the caller's
    /// `PrefixSet`.
    fn write_one_slot(
        &self,
        txn: &RwTxSync,
        ah: B256,
        slot: B256,
        val: U256,
    ) -> Result<B256, StateError> {
        use alloy_primitives::keccak256;
        use signet_libmdbx::WriteFlags;

        let sh = keccak256(slot);
        let mut key = ah.as_slice().to_vec();
        key.extend_from_slice(sh.as_slice());
        if val.is_zero() {
            // An absent-key delete is fine. Any other mdbx failure must
            // surface, or the mirror silently diverges from the reference.
            crate::schema::del_if_present(txn, self.hashed_storage, key)?;
        } else {
            txn.put(
                self.hashed_storage,
                key,
                val.to_be_bytes::<32>(),
                WriteFlags::UPSERT,
            )?;
        }
        Ok(sh)
    }

    /// Write the `hashed_accounts` row for every touched account: its basic
    /// fields (from `basics`, or the existing row if this block did not
    /// change them) plus its current storage root (from `new_sroot`, or the
    /// existing row's). An EIP-161-empty account deletes the row and its
    /// storage-trie subtree instead.
    fn write_hashed_accounts(
        &self,
        txn: &RwTxSync,
        touched: &BTreeSet<Address>,
        basics: &BTreeMap<Address, BasicFields>,
        new_sroot: &BTreeMap<Address, B256>,
    ) -> Result<(), StateError> {
        touched
            .iter()
            .try_for_each(|addr| self.write_one_hashed_account(txn, *addr, basics, new_sroot))
    }

    /// Write (or delete, if EIP-161-empty) the `hashed_accounts` row for
    /// one touched account. Basic fields come from `basics`, or the
    /// existing row if this block did not change them; the storage root
    /// comes from `new_sroot`, or the existing row's.
    fn write_one_hashed_account(
        &self,
        txn: &RwTxSync,
        addr: Address,
        basics: &BTreeMap<Address, BasicFields>,
        new_sroot: &BTreeMap<Address, B256>,
    ) -> Result<(), StateError> {
        use alloy_primitives::keccak256;
        use signet_libmdbx::WriteFlags;

        let ah = keccak256(addr);
        let existing = cursor::get_hashed_account(txn, self.hashed_accounts, &ah)?;
        let BasicFields {
            nonce,
            balance,
            code_hash,
        } = match basics.get(&addr) {
            Some(b) => *b,
            None => existing.map_or(BasicFields::EMPTY, |e| BasicFields {
                nonce: e.nonce,
                balance: e.balance,
                code_hash: e.code_hash,
            }),
        };
        let storage_root = new_sroot
            .get(&addr)
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
            crate::schema::del_if_present(txn, self.hashed_accounts, ah.as_slice())?;
            crate::schema::del_prefix(txn, self.hashed_storage, ah.as_slice())?;
            crate::schema::del_prefix(txn, self.storage_trie, ah.as_slice())?;
        } else {
            txn.put(
                self.hashed_accounts,
                ah.as_slice(),
                cursor::encode_account_leaf(&parts),
                WriteFlags::UPSERT,
            )?;
        }
        Ok(())
    }

    /// Advance the account trie for every touched account, and persist the
    /// resulting branch-node updates. Returns the new world-state root.
    fn update_account_trie(
        &self,
        txn: &RwTxSync,
        touched: &BTreeSet<Address>,
    ) -> Result<B256, StateError> {
        use alloy_primitives::keccak256;

        let ps = PrefixSet::from_b256s(touched.iter().map(keccak256));
        let (root, up) =
            StateRoot::state_root_incremental(txn, self.account_trie, self.hashed_accounts, &ps)?;
        self.apply_updates(txn, None, &up)?;
        Ok(root)
    }

    /// An independent full rebuild of the world-state root from the hashed
    /// mirror, used by the writer's shadow-check. This takes a different
    /// code path than the incremental walker, using alloy-trie's one-shot
    /// pre-hashed root builders, so a walker bug would show up as a
    /// divergence.
    ///
    /// # Errors
    ///
    /// Returns [`StateError`] if a table read fails.
    pub(crate) fn rebuild_root(&self, txn: &RwTxSync) -> Result<B256, StateError> {
        let mut accts: Vec<(B256, TrieAccount)> = Vec::new();
        crate::schema::for_each_row(txn, self.hashed_accounts, |k, v| {
            let ah = B256::from_slice(&k);
            let parts = cursor::decode_account_leaf(&v)?;
            let mut acc = parts.to_trie_account();
            acc.storage_root = self.storage_root_for(txn, &ah)?;
            accts.push((ah, acc));
            Ok(std::ops::ControlFlow::Continue(()))
        })?;
        Ok(root::state_root_unsorted(accts))
    }

    /// One account's storage root, recomputed from the hashed storage
    /// mirror. [`rebuild_root`](Self::rebuild_root)'s per-account oracle.
    fn storage_root_for(&self, txn: &RwTxSync, ah: &B256) -> Result<B256, StateError> {
        let mut pairs: Vec<(B256, U256)> = Vec::new();
        crate::schema::for_each_prefix(txn, self.hashed_storage, ah.as_slice(), |k, v| {
            pairs.push((B256::from_slice(&k[32..64]), U256::from_be_slice(&v)));
            Ok(std::ops::ControlFlow::Continue(()))
        })?;
        Ok(root::storage_root_unsorted(pairs))
    }
}

impl TrieTables {
    /// Apply one block's `BlockDelta` to the hashed-state mirror and the
    /// stored tries. Returns the new canonical world-state root.
    ///
    /// This runs inside the writer's block-commit transaction, so the root
    /// advances atomically with the state. It mirrors the equivalence-tested
    /// harness: storage tries first, stamping each account's `storage_root`
    /// into the hashed account, then the account trie.
    ///
    /// # Errors
    ///
    /// Returns [`StateError`] if a table read or write fails.
    pub fn update_for_block(
        &self,
        txn: &RwTxSync,
        delta: &kardamom_types::BlockDelta,
    ) -> Result<B256, StateError> {
        let mut touched: BTreeSet<Address> = BTreeSet::new();

        // --- storage tries first ---
        let stor_by = Self::group_storage_by_account(delta);
        let new_sroot = self.update_storage_tries(txn, &stor_by, &mut touched)?;

        let mut basics: BTreeMap<Address, BasicFields> = BTreeMap::new();
        for a in &delta.accounts {
            basics.insert(
                a.address,
                BasicFields {
                    nonce: a.nonce,
                    balance: a.balance,
                    code_hash: a.code_hash,
                },
            );
            touched.insert(a.address);
        }

        self.write_hashed_accounts(txn, &touched, &basics, &new_sroot)?;

        self.update_account_trie(txn, &touched)
    }
}

/// Compatibility wrapper over [`TrieTables::update_for_block`], kept for
/// `crates/validator/tests/witness_anchoring.rs`, an external caller this
/// group cannot change directly. **Phase B**: move it onto the method at
/// merge, then delete this function.
///
/// # Errors
///
/// Returns [`StateError`] if a table read or write fails.
pub fn update_for_block(
    txn: &RwTxSync,
    t: &TrieTables,
    delta: &kardamom_types::BlockDelta,
) -> Result<B256, StateError> {
    t.update_for_block(txn, delta)
}

/// Open [`TrieTables`], run [`update_for_block`] on `delta`, and persist
/// the resulting root under `KEY_STATE_ROOT` in the same transaction.
/// This is the common tail of genesis seeding and trie-bootstrap-from-state,
/// each folding its source into one synthetic `BlockDelta` first.
///
/// The writer's per-block commit path (`writer::apply`) keeps its own
/// version of this tail instead of calling this: it must run the
/// shadow-check oracle between the trie update and the meta put, which
/// this shared tail has no room for.
///
/// # Errors
///
/// Returns [`StateError`] if a table read or write fails.
pub(crate) fn commit_trie_root(
    txn: &RwTxSync,
    delta: &kardamom_types::BlockDelta,
) -> Result<B256, StateError> {
    let tables = TrieTables::open(txn)?;
    let root = tables.update_for_block(txn, delta)?;
    let meta = txn.open_db(Some(crate::schema::TABLE_META))?;
    txn.put(
        meta,
        crate::meta::KEY_STATE_ROOT,
        crate::meta::encode_b256(root),
        signet_libmdbx::WriteFlags::UPSERT,
    )?;
    Ok(root)
}

/// The three basic account fields [`TrieTables::write_one_hashed_account`]
/// needs from its `basics` map: [`AccountTrieParts`] minus `storage_root`,
/// which comes from a separate map (the account and its storage root are
/// computed by different passes over the delta). `pub(crate)`: the
/// writer's tests model the same three fields, instead of an unnamed
/// tuple (R11).
#[derive(Debug, Clone, Copy)]
pub(crate) struct BasicFields {
    pub(crate) nonce: u64,
    pub(crate) balance: U256,
    pub(crate) code_hash: B256,
}

impl BasicFields {
    /// The fields a brand-new account (no existing row, not in this
    /// block's delta) would have.
    const EMPTY: Self = Self {
        nonce: 0,
        balance: U256::ZERO,
        code_hash: B256::ZERO,
    };
}

/// The basic account fields needed to form an account-trie leaf.
/// `AccountValue` in [`crate::schema`] stores these, plus the persisted
/// `storage_root`.
#[derive(Debug, Clone, Copy)]
pub struct AccountTrieParts {
    pub nonce: u64,
    pub balance: U256,
    pub code_hash: B256,
    pub storage_root: B256,
}

impl AccountTrieParts {
    /// The canonical code hash. Kardamom seeds codeless accounts, and
    /// genesis allocations, with `B256::ZERO`. But an Ethereum trie leaf
    /// uses `KECCAK_EMPTY` for empty code. `ZERO` is never a valid code
    /// hash, so this mapping is unambiguous.
    fn canonical_code_hash(&self) -> B256 {
        if self.code_hash.is_zero() {
            KECCAK_EMPTY
        } else {
            self.code_hash
        }
    }

    /// The canonical storage root. `B256::ZERO` is the sentinel for "no
    /// storage trie computed yet", for example on genesis-seeded accounts.
    /// An empty storage trie roots to `EMPTY_ROOT_HASH`. `ZERO` is never a
    /// valid MPT root, so this mapping is unambiguous.
    fn canonical_storage_root(&self) -> B256 {
        if self.storage_root.is_zero() {
            EMPTY_ROOT_HASH
        } else {
            self.storage_root
        }
    }

    /// EIP-161 emptiness. An account with zero nonce, zero balance, and no
    /// code is not present in the world-state trie. Storage-bearing
    /// accounts have code in practice, so `storage_root` is not part of
    /// this test.
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
#[must_use]
pub fn empty_root() -> B256 {
    EMPTY_ROOT_HASH
}

/// The storage-trie root for one account, from its `(slot, value)`
/// pairs. Zero-valued slots are omitted, because they are absent from an
/// Ethereum storage trie. Keys are raw slots; alloy-trie hashes them, as
/// a secure trie. An account with no non-zero slots yields
/// [`EMPTY_ROOT_HASH`].
pub fn storage_root(slots: impl IntoIterator<Item = (B256, U256)>) -> B256 {
    root::storage_root_unhashed(slots.into_iter().filter(|(_, v)| !v.is_zero()))
}

/// The world-state root over the full account set. Empty accounts
/// (EIP-161) are omitted. Addresses are raw; alloy-trie hashes them, as
/// a secure trie. An empty account set yields [`EMPTY_ROOT_HASH`].
pub fn state_root(accounts: impl IntoIterator<Item = (Address, AccountTrieParts)>) -> B256 {
    root::state_root_unhashed(
        accounts
            .into_iter()
            .filter(|(_, a)| !a.is_empty())
            .map(|(addr, a)| (addr, a.to_trie_account())),
    )
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
        // An account that is empty, with nonce 0, balance 0, and no code,
        // must not be in the trie. So a state of only empty accounts
        // roots to the empty trie.
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
        // A funded account seeded with ZERO code_hash and ZERO storage_root,
        // the kardamom and genesis sentinels, must root identically to
        // one built with the canonical KECCAK_EMPTY and EMPTY_ROOT_HASH.
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
        // A ZERO-everything empty account is still excluded.
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

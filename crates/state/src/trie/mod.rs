//! Canonical Ethereum MPT state root over the libmdbx state.
//!
//! Milestone 1 computes the canonical Ethereum secure-trie world-state root via
//! [`alloy_trie`]. It is **storage-incremental**: per block the writer recomputes
//! only the storage tries of accounts touched this block — persisting each
//! account's `storage_root` in the `accounts` table — then derives the
//! account-trie root over the full account set.
//!
//! Fully branch-node-incremental updates (persisting `BranchNodeCompact`s via
//! [`alloy_trie::HashBuilder::with_updates`] + a stored-node cursor, the reth
//! approach) are a documented fast-follow. The **root value** produced here is
//! already the canonical Ethereum root regardless of how it is computed — proven
//! by the `incremental_equals_full_rebuild` test against an independent rebuild.
//!
//! The pure `state_root` / `storage_root` rebuild functions below are retained
//! as the **shadow-check oracle** for the node-incremental path (see
//! [`node`] / `cursor` / `prefix_set` / `walker`, and `StateRoot::*_incremental`).
//! `crate::writer` drives them with mdbx cursors inside the block-commit
//! transaction so the root advances atomically with the state.

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

/// Persist a walk's [`TrieUpdates`] to a node table: upsert each produced branch
/// node and delete each collapsed path. `account_hash` namespaces storage-trie
/// keys (`None` for the account trie). Used by the writer and the test harness.
pub fn apply_trie_updates(
    txn: &RwTxSync,
    db: Database,
    account_hash: Option<&B256>,
    updates: &TrieUpdates,
) -> Result<(), StateError> {
    use signet_libmdbx::WriteFlags;
    let key = |path: &alloy_trie::Nibbles| -> Vec<u8> {
        let nibs = path.to_vec();
        match account_hash {
            Some(a) => {
                let mut k = Vec::with_capacity(32 + nibs.len());
                k.extend_from_slice(a.as_slice());
                k.extend_from_slice(&nibs);
                k
            }
            None => nibs,
        }
    };
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

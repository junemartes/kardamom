//! The incremental trie walker.
//!
//! This recursively rebuilds only the changed regions of a trie, feeding
//! `alloy_trie::HashBuilder`. An unchanged subtrie with a stored hash is
//! skipped, using `add_branch(..., stored_in_database=true)`. A changed
//! subtrie is descended: the walker recurses into stored sub-branches,
//! or emits hashed leaves.
//!
//! When there is no stored branch node at a path, the whole subtrie is
//! rebuilt from its leaves. So a fresh or small trie degrades to a
//! correct full rebuild, and incrementality emerges as `HashBuilder`
//! produces stored branch nodes for next time.
//!
//! ## Deletion tracking
//!
//! Every stored node the walker descends into is recorded. After the
//! build, `removals` is `visited_old_nodes` minus `new_nodes` (from
//! `split()`), which captures subtries that collapsed.
//!
//! That alone is not enough. `tree_mask` bit `i` means "the child
//! subtree under nibble `i` contains stored nodes", not "a node is
//! stored at exactly `path + [i]`". Under an extension, the stored node
//! lives at a deeper path that the exact-path lookup never sees.
//!
//! So, whenever the exact-path lookup misses and a subtrie is rebuilt
//! from its leaves, this walker records the whole nibble-path prefix in
//! [`TrieUpdates::cleared`], and range-deletes it before the new upserts
//! land. This way, a stored node hiding under an extension can never
//! survive as a stale orphan. Otherwise, a later walk could exact-hit
//! that orphan and use it for an `add_branch` skip, causing silent root
//! divergence.

use alloy_primitives::{B256, U256};
use alloy_trie::{BranchNodeCompact, HashBuilder, Nibbles};
use signet_libmdbx::Database;
use signet_libmdbx::TxSync;

use super::cursor::{collect_hashed_accounts_under, collect_hashed_storage_under, get_branch_node};
use super::prefix_set::PrefixSet;
use crate::error::StateError;

/// Branch-node mutations produced by one walk.
#[derive(Debug, Default)]
pub struct TrieUpdates {
    pub upserts: Vec<(Nibbles, BranchNodeCompact)>,
    pub removals: Vec<Nibbles>,
    /// Nibble-path prefixes whose subtries were rebuilt from leaves,
    /// because no stored node existed at the exact path. Every stored
    /// node under such a prefix, for example one hiding at a deeper path
    /// behind an extension, is stale. Range-delete it before applying
    /// `upserts`.
    pub cleared: Vec<Nibbles>,
}

/// A per-walk record. `visited` holds the stored nodes that were
/// exact-hit. `cleared` holds the subtrie prefixes rebuilt from leaves
/// after an exact-path miss.
#[derive(Default)]
struct WalkLog {
    visited: Vec<Nibbles>,
    cleared: Vec<Nibbles>,
}

/// Compute the account-trie root incrementally. `account_trie` and
/// `hashed_accounts` are the table handles. `prefix_set` holds
/// `keccak(addr)` for every changed account. Returns `(root, updates)`.
pub(crate) fn account_root<K: crate::trie::cursor::ReadKind>(
    tx: &TxSync<K>,
    account_trie: Database,
    hashed_accounts: Database,
    prefix_set: &PrefixSet,
    account_leaf: &dyn Fn(&super::AccountTrieParts) -> Vec<u8>,
) -> Result<(B256, TrieUpdates), StateError> {
    let mut hb = HashBuilder::default().with_updates(true);
    let mut log = WalkLog::default();
    walk_account(
        tx,
        account_trie,
        hashed_accounts,
        &Nibbles::new(),
        prefix_set,
        &mut hb,
        &mut log,
        account_leaf,
    )?;
    let root = hb.root();
    let (_, updated) = hb.split();
    finalize(root, updated, log)
}

#[allow(clippy::too_many_arguments)]
fn walk_account<K: crate::trie::cursor::ReadKind>(
    tx: &TxSync<K>,
    account_trie: Database,
    hashed_accounts: Database,
    path: &Nibbles,
    prefix_set: &PrefixSet,
    hb: &mut HashBuilder,
    log: &mut WalkLog,
    account_leaf: &dyn Fn(&super::AccountTrieParts) -> Vec<u8>,
) -> Result<(), StateError> {
    match get_branch_node(tx, account_trie, None, path)? {
        None => {
            // This is a full leaf rebuild of this subtrie. Stored nodes may
            // still exist under this path, behind extensions, where the
            // exact-path get cannot see them. Mark the prefix for range
            // deletion, so none of them survives as a stale orphan.
            log.cleared.push(*path);
            for (k, parts) in collect_hashed_accounts_under(tx, hashed_accounts, path)? {
                if parts.is_empty() {
                    continue;
                }
                hb.add_leaf(Nibbles::unpack(k.as_slice()), &account_leaf(&parts));
            }
        }
        Some(node) => {
            log.visited.push(*path);
            let (tm, hm) = (node.tree_mask.get(), node.hash_mask.get());
            // Iterate all 16 nibbles, not just the stored node's
            // state_mask, which may be stale. A new account under a
            // nibble absent from the old mask must still be surfaced
            // from the hashed state.
            for i in 0..16u8 {
                let mut child = *path;
                child.push(i);
                let changed = prefix_set.contains_prefix(&child);
                if (tm & (1 << i)) != 0 {
                    // A stored branch child. Skip it if unchanged and
                    // hashed; otherwise recurse.
                    if !changed && (hm & (1 << i)) != 0 {
                        hb.add_branch(child, node.hash_for_nibble(i), true);
                    } else {
                        walk_account(
                            tx,
                            account_trie,
                            hashed_accounts,
                            &child,
                            prefix_set,
                            hb,
                            log,
                            account_leaf,
                        )?;
                    }
                } else {
                    // A leaf-or-empty child. Emit any current leaves
                    // under it, to surface newly created accounts.
                    for (k, parts) in collect_hashed_accounts_under(tx, hashed_accounts, &child)? {
                        if parts.is_empty() {
                            continue;
                        }
                        hb.add_leaf(Nibbles::unpack(k.as_slice()), &account_leaf(&parts));
                    }
                }
            }
        }
    }
    Ok(())
}

/// Compute one account's storage-trie root incrementally.
pub(crate) fn storage_root<K: crate::trie::cursor::ReadKind>(
    tx: &TxSync<K>,
    storage_trie: Database,
    hashed_storage: Database,
    account_hash: &B256,
    prefix_set: &PrefixSet,
) -> Result<(B256, TrieUpdates), StateError> {
    let mut hb = HashBuilder::default().with_updates(true);
    let mut log = WalkLog::default();
    walk_storage(
        tx,
        storage_trie,
        hashed_storage,
        account_hash,
        &Nibbles::new(),
        prefix_set,
        &mut hb,
        &mut log,
    )?;
    let root = hb.root();
    let (_, updated) = hb.split();
    finalize(root, updated, log)
}

#[allow(clippy::too_many_arguments)]
fn walk_storage<K: crate::trie::cursor::ReadKind>(
    tx: &TxSync<K>,
    storage_trie: Database,
    hashed_storage: Database,
    account_hash: &B256,
    path: &Nibbles,
    prefix_set: &PrefixSet,
    hb: &mut HashBuilder,
    log: &mut WalkLog,
) -> Result<(), StateError> {
    match get_branch_node(tx, storage_trie, Some(account_hash), path)? {
        None => {
            // See walk_account. Clear the prefix, so stored nodes hiding
            // under extensions cannot outlive this leaf rebuild as stale
            // orphans.
            log.cleared.push(*path);
            for (k, v) in collect_hashed_storage_under(tx, hashed_storage, account_hash, path)? {
                hb.add_leaf(Nibbles::unpack(k.as_slice()), &storage_leaf(v));
            }
        }
        Some(node) => {
            log.visited.push(*path);
            let (tm, hm) = (node.tree_mask.get(), node.hash_mask.get());
            for i in 0..16u8 {
                let mut child = *path;
                child.push(i);
                let changed = prefix_set.contains_prefix(&child);
                if (tm & (1 << i)) != 0 {
                    if !changed && (hm & (1 << i)) != 0 {
                        hb.add_branch(child, node.hash_for_nibble(i), true);
                    } else {
                        walk_storage(
                            tx,
                            storage_trie,
                            hashed_storage,
                            account_hash,
                            &child,
                            prefix_set,
                            hb,
                            log,
                        )?;
                    }
                } else {
                    for (k, v) in
                        collect_hashed_storage_under(tx, hashed_storage, account_hash, &child)?
                    {
                        hb.add_leaf(Nibbles::unpack(k.as_slice()), &storage_leaf(v));
                    }
                }
            }
        }
    }
    Ok(())
}

/// The proof-generation entry point (`trie::proofs`): the account
/// walk from the root, with a caller-owned `HashBuilder` that has a
/// proof retainer attached, and a discarded log. A proof walk mutates
/// nothing and applies nothing.
pub(crate) fn walk_account_for_proofs<K: crate::trie::cursor::ReadKind>(
    tx: &TxSync<K>,
    account_trie: Database,
    hashed_accounts: Database,
    prefix_set: &PrefixSet,
    hb: &mut HashBuilder,
    account_leaf: &dyn Fn(&super::AccountTrieParts) -> Vec<u8>,
) -> Result<(), StateError> {
    let mut log = WalkLog::default();
    walk_account(
        tx,
        account_trie,
        hashed_accounts,
        &Nibbles::new(),
        prefix_set,
        hb,
        &mut log,
        account_leaf,
    )
}

/// The proof-generation entry for one storage trie's walk. Same
/// contract as [`walk_account_for_proofs`].
pub(crate) fn walk_storage_for_proofs<K: crate::trie::cursor::ReadKind>(
    tx: &TxSync<K>,
    storage_trie: Database,
    hashed_storage: Database,
    account_hash: &B256,
    prefix_set: &PrefixSet,
    hb: &mut HashBuilder,
) -> Result<(), StateError> {
    let mut log = WalkLog::default();
    walk_storage(
        tx,
        storage_trie,
        hashed_storage,
        account_hash,
        &Nibbles::new(),
        prefix_set,
        hb,
        &mut log,
    )
}

/// Storage leaf value = RLP of the slot's U256 (matches `alloy_trie::root`).
fn storage_leaf(v: U256) -> Vec<u8> {
    alloy_rlp::encode_fixed_size(&v).to_vec()
}

/// Build `TrieUpdates`.
///
/// - `upserts`: the nodes `HashBuilder` produced.
/// - `removals`: visited old nodes that are no longer present, meaning
///   collapsed subtries.
/// - `cleared`: prefixes rebuilt from leaves. Their stored nodes,
///   including any hiding at deeper paths under extensions that the
///   walk never exact-visits, must be range-deleted before the upserts
///   land.
fn finalize(
    root: B256,
    updated: alloy_trie::HashMap<Nibbles, BranchNodeCompact>,
    log: WalkLog,
) -> Result<(B256, TrieUpdates), StateError> {
    let upserts: Vec<(Nibbles, BranchNodeCompact)> =
        updated.iter().map(|(k, v)| (*k, v.clone())).collect();
    let removals: Vec<Nibbles> = log
        .visited
        .into_iter()
        .filter(|p| !updated.contains_key(p))
        .collect();
    Ok((
        root,
        TrieUpdates {
            upserts,
            removals,
            cleared: log.cleared,
        },
    ))
}

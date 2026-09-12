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
//! The account trie and each account's storage trie run this exact
//! algorithm over two different leaf sources. [`LeafSource`] captures
//! what differs — which table, which key namespace, and how to emit a
//! subtree's current leaves — and its default [`LeafSource::walk`] method
//! is the one shared implementation. [`AccountWalk`] and [`StorageCtx`]
//! are its two leaf sources.
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

use super::cursor::{
    ReadKind, collect_hashed_accounts_under, collect_hashed_storage_under, get_branch_node,
};
use super::prefix_set::PrefixSet;
use crate::error::StateError;

/// Branch-node mutations produced by one walk.
#[derive(Debug, Default)]
pub(crate) struct TrieUpdates {
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

/// Everything one recursive [`LeafSource::walk`] call threads down to its
/// children, grouped so the walk methods stay under the argument-count
/// limit: the read transaction, the changed-prefix set, the in-progress
/// `HashBuilder`, and the deletion-tracking log.
struct WalkCtx<'a, 'b, K: ReadKind> {
    tx: &'a TxSync<K>,
    prefix_set: &'a PrefixSet,
    hb: &'b mut HashBuilder,
    log: &'b mut WalkLog,
}

/// One trie's node table and leaf source: the account trie over the
/// hashed-account mirror ([`AccountWalk`]), or one account's storage trie
/// over its hashed-storage mirror ([`StorageCtx`]). [`LeafSource::walk`]
/// is the one shared recursive-rebuild algorithm; each implementor only
/// says which table it walks and how to emit a subtree's current leaves.
trait LeafSource {
    /// The branch-node table: `account_trie` or `storage_trie`.
    fn trie_db(&self) -> Database;

    /// `None` for the account trie. `Some(account_hash)` namespaces a
    /// storage trie's keys to one account.
    fn namespace(&self) -> Option<&B256>;

    /// Emit every current leaf under `path` into `hb`, for the exact-miss
    /// (full rebuild) and leaf-or-empty-child cases of [`Self::walk`].
    fn emit_under<K: ReadKind>(
        &self,
        tx: &TxSync<K>,
        path: &Nibbles,
        hb: &mut HashBuilder,
    ) -> Result<(), StateError>;

    /// Recursively rebuild the changed regions of this trie into `hb`,
    /// from `path` down. See the module doc for the skip rule and the
    /// deletion-tracking contract `log` records.
    fn walk<K: ReadKind>(
        &self,
        path: &Nibbles,
        ctx: &mut WalkCtx<'_, '_, K>,
    ) -> Result<(), StateError> {
        let Some(node) = get_branch_node(ctx.tx, self.trie_db(), self.namespace(), path)? else {
            // This is a full leaf rebuild of this subtrie. Stored nodes may
            // still exist under this path, behind extensions, where the
            // exact-path get cannot see them. Mark the prefix for range
            // deletion, so none of them survives as a stale orphan.
            ctx.log.cleared.push(*path);
            return self.emit_under(ctx.tx, path, ctx.hb);
        };
        ctx.log.visited.push(*path);
        // Iterate all 16 nibbles, not just the stored node's state_mask,
        // which may be stale. A new leaf under a nibble absent from the
        // old mask must still be surfaced from the hashed state.
        (0..16u8).try_for_each(|i| self.walk_child(path, &node, i, ctx))
    }

    /// One child nibble `i` of the stored branch `node` at `path`, the
    /// body [`Self::walk`]'s loop over all 16 nibbles calls. A stored,
    /// unchanged, hashed child is added directly; a stored changed child
    /// recurses; an absent child's current leaves are emitted, to
    /// surface newly created entries.
    fn walk_child<K: ReadKind>(
        &self,
        path: &Nibbles,
        node: &BranchNodeCompact,
        i: u8,
        ctx: &mut WalkCtx<'_, '_, K>,
    ) -> Result<(), StateError> {
        let mut child = *path;
        child.push(i);
        let (tm, hm) = (node.tree_mask.get(), node.hash_mask.get());
        if (tm & (1 << i)) == 0 {
            // A leaf-or-empty child. Emit any current leaves under it, to
            // surface newly created entries.
            return self.emit_under(ctx.tx, &child, ctx.hb);
        }
        // A stored branch child. Skip it if unchanged and hashed;
        // otherwise recurse.
        if !ctx.prefix_set.contains_prefix(&child) && (hm & (1 << i)) != 0 {
            ctx.hb.add_branch(child, node.hash_for_nibble(i), true);
            return Ok(());
        }
        self.walk(&child, ctx)
    }
}

/// The two table handles the account walk needs on every recursive call.
#[derive(Clone, Copy)]
struct AccountWalk {
    account_trie: Database,
    hashed_accounts: Database,
}

impl LeafSource for AccountWalk {
    fn trie_db(&self) -> Database {
        self.account_trie
    }

    fn namespace(&self) -> Option<&B256> {
        None
    }

    fn emit_under<K: ReadKind>(
        &self,
        tx: &TxSync<K>,
        path: &Nibbles,
        hb: &mut HashBuilder,
    ) -> Result<(), StateError> {
        collect_hashed_accounts_under(tx, self.hashed_accounts, path)?
            .into_iter()
            .filter(|(_, parts)| !parts.is_empty())
            .for_each(|(k, parts)| {
                hb.add_leaf(
                    Nibbles::unpack(k.as_slice()),
                    &super::account_leaf_rlp(&parts),
                );
            });
        Ok(())
    }
}

/// The three table handles the storage walk needs on every recursive
/// call. Grouping them keeps the walker under the argument-count limit.
#[derive(Clone, Copy)]
struct StorageCtx<'a> {
    storage_trie: Database,
    hashed_storage: Database,
    account_hash: &'a B256,
}

impl LeafSource for StorageCtx<'_> {
    fn trie_db(&self) -> Database {
        self.storage_trie
    }

    fn namespace(&self) -> Option<&B256> {
        Some(self.account_hash)
    }

    fn emit_under<K: ReadKind>(
        &self,
        tx: &TxSync<K>,
        path: &Nibbles,
        hb: &mut HashBuilder,
    ) -> Result<(), StateError> {
        for (k, v) in
            collect_hashed_storage_under(tx, self.hashed_storage, self.account_hash, path)?
        {
            hb.add_leaf(Nibbles::unpack(k.as_slice()), &storage_leaf(v));
        }
        Ok(())
    }
}

/// Run one [`LeafSource`]'s walk from the trie root, and fold the result
/// into `(root, TrieUpdates)`. [`account_root`] and [`storage_root`] are
/// this over the two leaf sources.
fn root_with<K: ReadKind, L: LeafSource>(
    tx: &TxSync<K>,
    source: &L,
    prefix_set: &PrefixSet,
) -> Result<(B256, TrieUpdates), StateError> {
    let mut hb = HashBuilder::default().with_updates(true);
    let mut log = WalkLog::default();
    source.walk(
        &Nibbles::new(),
        &mut WalkCtx {
            tx,
            prefix_set,
            hb: &mut hb,
            log: &mut log,
        },
    )?;
    let root = hb.root();
    let (_, updated) = hb.split();
    Ok(finalize(root, &updated, log))
}

/// Compute the account-trie root incrementally. `account_trie` and
/// `hashed_accounts` are the table handles. `prefix_set` holds
/// `keccak(addr)` for every changed account. Returns `(root, updates)`.
pub(crate) fn account_root<K: ReadKind>(
    tx: &TxSync<K>,
    account_trie: Database,
    hashed_accounts: Database,
    prefix_set: &PrefixSet,
) -> Result<(B256, TrieUpdates), StateError> {
    root_with(
        tx,
        &AccountWalk {
            account_trie,
            hashed_accounts,
        },
        prefix_set,
    )
}

/// Compute one account's storage-trie root incrementally.
pub(crate) fn storage_root<K: ReadKind>(
    tx: &TxSync<K>,
    storage_trie: Database,
    hashed_storage: Database,
    account_hash: &B256,
    prefix_set: &PrefixSet,
) -> Result<(B256, TrieUpdates), StateError> {
    root_with(
        tx,
        &StorageCtx {
            storage_trie,
            hashed_storage,
            account_hash,
        },
        prefix_set,
    )
}

/// The proof-generation entry point (`trie::proofs`): the account
/// walk from the root, with a caller-owned `HashBuilder` that has a
/// proof retainer attached, and a discarded log. A proof walk mutates
/// nothing and applies nothing.
pub(crate) fn walk_account_for_proofs<K: ReadKind>(
    tx: &TxSync<K>,
    account_trie: Database,
    hashed_accounts: Database,
    prefix_set: &PrefixSet,
    hb: &mut HashBuilder,
) -> Result<(), StateError> {
    let mut log = WalkLog::default();
    AccountWalk {
        account_trie,
        hashed_accounts,
    }
    .walk(
        &Nibbles::new(),
        &mut WalkCtx {
            tx,
            prefix_set,
            hb,
            log: &mut log,
        },
    )
}

/// The proof-generation entry for one storage trie's walk. Same
/// contract as [`walk_account_for_proofs`].
pub(crate) fn walk_storage_for_proofs<K: ReadKind>(
    tx: &TxSync<K>,
    storage_trie: Database,
    hashed_storage: Database,
    account_hash: &B256,
    prefix_set: &PrefixSet,
    hb: &mut HashBuilder,
) -> Result<(), StateError> {
    let mut log = WalkLog::default();
    StorageCtx {
        storage_trie,
        hashed_storage,
        account_hash,
    }
    .walk(
        &Nibbles::new(),
        &mut WalkCtx {
            tx,
            prefix_set,
            hb,
            log: &mut log,
        },
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
    updated: &alloy_trie::HashMap<Nibbles, BranchNodeCompact>,
    log: WalkLog,
) -> (B256, TrieUpdates) {
    let upserts: Vec<(Nibbles, BranchNodeCompact)> =
        updated.iter().map(|(k, v)| (*k, v.clone())).collect();
    let removals: Vec<Nibbles> = log
        .visited
        .into_iter()
        .filter(|p| !updated.contains_key(p))
        .collect();
    (
        root,
        TrieUpdates {
            upserts,
            removals,
            cleared: log.cleared,
        },
    )
}

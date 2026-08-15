//! Proof-node generation over the stored trie tables (spec:
//! no-std-exec-core, phase 3b — the capture side).
//!
//! NOT a read of the node tables: the stored `BranchNodeCompact` rows are
//! deliberately not a proof set (leaves and extensions are never stored,
//! and extension-child hash bits are cleared). Proofs come from RE-RUNNING
//! the incremental walk with a proof retainer — the same walker, cursors,
//! and hashed-state mirror the per-block root already drives — with the
//! target paths in the `PrefixSet`, which is what forces descent along
//! paths the incremental skip would otherwise hash over.
//!
//! Targets may be FULL keys (`keccak(addr)` / `keccak(slot)` — the witness
//! read set) or PARTIAL node positions (what the anchor layer's
//! `MissingNode` names during the capture fixed point); the retainer keeps
//! every node at or above each target.
//!
//! Runs against any sync transaction — a read txn over the committed
//! snapshot is the intended caller shape (proof generation must never sit
//! inside the writer's commit txn; witness capture is off the commit path).

use alloy_primitives::B256;
use alloy_rlp::Encodable;
use alloy_trie::proof::ProofRetainer;
use alloy_trie::{HashBuilder, Nibbles};
use bytes::Bytes;
use signet_libmdbx::TxSync;

use super::cursor::ReadKind;
use super::prefix_set::PrefixSet;
use super::walker;
use crate::error::StateError;

/// Retained proof nodes (plus the walked root, as a cross-check) for the
/// ACCOUNT trie at the given targets.
pub fn account_proof_nodes<K: ReadKind>(
    tx: &TxSync<K>,
    account_trie: signet_libmdbx::Database,
    hashed_accounts: signet_libmdbx::Database,
    targets: &[Nibbles],
) -> Result<(B256, Vec<Bytes>), StateError> {
    let leaf = |p: &super::AccountTrieParts| {
        let mut buf = Vec::new();
        p.to_trie_account().encode(&mut buf);
        buf
    };
    let ps = PrefixSet::from_nibbles(targets.iter().copied());
    let mut hb = HashBuilder::default().with_proof_retainer(ProofRetainer::new(targets.to_vec()));
    walker::walk_account_for_proofs(tx, account_trie, hashed_accounts, &ps, &mut hb, &leaf)?;
    let root = hb.root();
    Ok((root, collect(hb)))
}

/// Retained proof nodes (plus the walked storage root) for ONE account's
/// storage trie at the given targets.
pub fn storage_proof_nodes<K: ReadKind>(
    tx: &TxSync<K>,
    storage_trie: signet_libmdbx::Database,
    hashed_storage: signet_libmdbx::Database,
    account_hash: B256,
    targets: &[Nibbles],
) -> Result<(B256, Vec<Bytes>), StateError> {
    let ps = PrefixSet::from_nibbles(targets.iter().copied());
    let mut hb = HashBuilder::default().with_proof_retainer(ProofRetainer::new(targets.to_vec()));
    walker::walk_storage_for_proofs(
        tx,
        storage_trie,
        hashed_storage,
        &account_hash,
        &ps,
        &mut hb,
    )?;
    let root = hb.root();
    Ok((root, collect(hb)))
}

/// Nodes < 32 bytes are inline in their parents per MPT and never fetched
/// by hash — carrying them would only bloat the set.
fn collect(mut hb: HashBuilder) -> Vec<Bytes> {
    hb.take_proof_nodes()
        .into_inner()
        .into_values()
        .filter(|n| n.len() >= 32)
        .map(|n| Bytes::from(n.to_vec()))
        .collect()
}

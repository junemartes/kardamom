//! Proof-node generation over the stored trie tables, the capture side.
//!
//! This is not a read of the node tables. The stored `BranchNodeCompact`
//! rows are deliberately not a proof set: leaves and extensions are
//! never stored, and extension-child hash bits are cleared. Proofs come
//! from re-running the incremental walk with a proof retainer. This uses
//! the same walker, cursors, and hashed-state mirror that the per-block
//! root already drives, with the target paths in the `PrefixSet`. This
//! is what forces descent along paths the incremental skip would
//! otherwise hash over.
//!
//! Targets may be full keys, `keccak(addr)` or `keccak(slot)`, from the
//! witness read set, or partial node positions, which is what the
//! anchor layer's `MissingNode` names during the capture fixed point.
//! The retainer keeps every node at or above each target.
//!
//! This runs against any sync transaction. The intended caller shape is
//! a read transaction over the committed snapshot: proof generation
//! must never run inside the writer's commit transaction, because
//! witness capture is off the commit path.

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

/// The retained proof nodes, plus the walked root as a cross-check,
/// for the account trie at the given targets.
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

/// The retained proof nodes, plus the walked storage root, for one
/// account's storage trie at the given targets.
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

/// Nodes under 32 bytes are inline in their parents, under MPT rules,
/// and are never fetched by hash. Carrying them would only bloat the set.
fn collect(mut hb: HashBuilder) -> Vec<Bytes> {
    hb.take_proof_nodes()
        .into_inner()
        .into_values()
        .filter(|n| n.len() >= 32)
        .map(|n| Bytes::from(n.to_vec()))
        .collect()
}

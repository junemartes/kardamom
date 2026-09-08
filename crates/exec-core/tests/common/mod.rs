//! Shared test-only helpers.
//!
//! - `ns_per_op`: the exec-core cost microbenchmarks (`hash_cost.rs`,
//!   `decode_cost.rs`) both warm up, time `reps` passes, and divide by
//!   `reps * n` to report nanoseconds per op.
//! - `retained_nodes`: the anchor gates (`anchor_sparse.rs`,
//!   `anchor_state.rs`) both build an oracle `HashBuilder` over a target
//!   key set and keep its addressable proof nodes.
//! - `slot`: builds a `TxSlot` from plain integers.
//!
//! Each integration-test binary compiles its own copy of this module
//! through its own `mod common;`.
#![allow(
    dead_code,
    reason = "each integration-test binary compiles its own copy of this module through its own `mod common;`, and no single binary uses every helper here"
)]

use std::collections::BTreeMap;

use alloy_primitives::{B256, Bytes};
use alloy_trie::proof::ProofRetainer;
use alloy_trie::{HashBuilder, Nibbles};
use kardamom_exec_core::{TxIndex, TxSlot};
use kardamom_types::BPosition;

/// Build a [`TxSlot`] from plain integers: `tx_idx` and `tx_position`
/// are given separately, since some tests need a `BPosition` that does
/// not equal its `TxIndex`.
pub fn slot(
    tx_idx: u64,
    tx_position: u64,
    tx_index_in_block: u64,
    cumulative_gas_used_before: u64,
) -> TxSlot {
    TxSlot {
        tx_idx: TxIndex(tx_idx),
        tx_position: BPosition::from_index(tx_position),
        tx_index_in_block,
        cumulative_gas_used_before,
    }
}

/// Time `reps` calls to `f`, and return the average nanoseconds per op
/// across `reps * n` total operations (`n` is however many ops one call
/// to `f` performs — a `hash_cost.rs` call hashes `n` write sets per
/// pass, for example).
#[allow(
    clippy::cast_precision_loss,
    reason = "a nanosecond-scale timing report; f64 precision loss here is irrelevant"
)]
pub fn ns_per_op(reps: usize, n: usize, mut f: impl FnMut()) -> f64 {
    let t = std::time::Instant::now();
    for _ in 0..reps {
        f();
    }
    t.elapsed().as_nanos() as f64 / (reps * n) as f64
}

/// Build a `HashBuilder` over `entries` (already keyed by trie path,
/// sorted by construction) with `targets` as proof-retainer targets, add
/// every leaf, and return the resulting root plus every retained proof
/// node of 32 bytes or more. Nodes smaller than that are inline in their
/// parents and never separately fetched, so they are dropped.
pub fn retained_nodes(entries: &BTreeMap<B256, Vec<u8>>, targets: &[B256]) -> (B256, Vec<Bytes>) {
    let target_nibbles: Vec<Nibbles> = targets.iter().map(|k| Nibbles::unpack(k)).collect();
    let mut hb = HashBuilder::default().with_proof_retainer(ProofRetainer::new(target_nibbles));
    for (k, v) in entries {
        hb.add_leaf(Nibbles::unpack(k), v);
    }
    let root = hb.root();
    let nodes = hb
        .take_proof_nodes()
        .into_inner()
        .into_values()
        .filter(|n| n.len() >= 32)
        .collect();
    (root, nodes)
}

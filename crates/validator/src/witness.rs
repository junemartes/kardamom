//! Validator-side witness capture and stateless re-execution.
//!
//! The validator is one of the state DB's three consumers, so witness
//! collection lives here. The batcher stays state-free, and a witness-fed
//! prover downstream needs no state access at all.
//!
//! [`capture_block_witness`] runs the ordinary sequential block re-execution
//! with a [`WitnessRecorder`] placed at the snapshot seam. It returns both
//! the execution output and the pre-state slice it read.
//! [`reexecute_stateless`] replays the same records over nothing but that
//! witness, the zk-guest execution shape. The two outputs must be
//! identical; `tests/stateless_reexec.rs` holds the round-trip contract.
//! Since phase 3, the driver itself lives in the `no_std` exec core
//! (`kardamom_exec_core::stateless`), and the stateless entry also
//! re-derives every tx's identity (keccak tx_hash and k256 sender
//! recovery). These wrappers are the validator-facing seam.

use kardamom_engine::actor::BlockExecOutput;
use kardamom_engine::witness::WitnessRecorder;
use kardamom_engine::{EngineError, ExecEnv, PendingDelta};
use kardamom_types::{ExecutionWitness, StateDatabase};

use kardamom_engine::actor::BufferedRecord;

/// Re-execute a block sequentially. Capture the pre-state witness and the
/// block's raw (granularity-1) access list. Returns the execution output,
/// the canonical witness (keyed by `env.block_number`), and the BAL: the
/// full prover input set for one block.
pub fn capture_block_witness<S: StateDatabase>(
    snapshot: &S,
    parent: Option<&PendingDelta>,
    records: &[BufferedRecord],
    env: ExecEnv,
) -> Result<
    (
        BlockExecOutput,
        ExecutionWitness,
        alloy_eip7928::BlockAccessList,
    ),
    EngineError,
> {
    let recorder = WitnessRecorder::new(snapshot);
    let (out, bal) =
        kardamom_engine::stateless::execute_block_with_bal(&recorder, parent, records, env)?;
    Ok((out, recorder.into_witness(env.block_number), bal))
}

/// Anchor a captured witness to the committed trie. Stamp
/// `pre_state_root`, then build the [`WitnessProofs`] node set by
/// recompute-guided completion: run the guest's own verify and post-root
/// recompute, and resolve each named `MissingNode` by walking the stored
/// trie again with that position as a proof-retainer target. Completeness
/// holds by construction: if this function returns `Ok`, that is the proof
/// that the guest's identical recompute will succeed on the returned set.
///
/// `tx` must be a read view of the committed state the witness was
/// captured against: the root after block N-1, stamped here into the
/// witness. The caller obtains it when the parent's commit settles. Never
/// block capture on an fsync; proving runs asynchronously, aligned to
/// batches.
///
/// Returns the canonical proof set and the recomputed post-state root.
pub fn anchor_block_witness<K: kardamom_state::trie::cursor::ReadKind>(
    tx: &kardamom_state::signet_libmdbx::TxSync<K>,
    tables: &kardamom_state::trie::TrieTables,
    pre_state_root: alloy_primitives::B256,
    witness: &mut ExecutionWitness,
    delta: &PendingDelta,
) -> Result<(kardamom_types::WitnessProofs, alloy_primitives::B256), EngineError> {
    use alloy_primitives::keccak256;
    use kardamom_engine::anchor::{AnchorError, recompute_post_root, verify_witness_anchored};
    use kardamom_engine::error::ExecutorError;
    use kardamom_state::trie::Nibbles;
    use kardamom_state::trie::proofs::{account_proof_nodes, storage_proof_nodes};
    use std::collections::{BTreeMap, BTreeSet};

    witness.pre_state_root = Some(pre_state_root);

    // Initial targets: the read set and the write set, for each trie.
    let mut acct_targets: BTreeSet<Nibbles> = BTreeSet::new();
    let mut slot_targets: BTreeMap<alloy_primitives::Address, BTreeSet<Nibbles>> = BTreeMap::new();
    for a in &witness.accounts {
        acct_targets.insert(Nibbles::unpack(keccak256(a.address)));
    }
    for s in &witness.storage {
        slot_targets
            .entry(s.address)
            .or_default()
            .insert(Nibbles::unpack(keccak256(s.key)));
    }
    for addr in delta.accounts.keys() {
        acct_targets.insert(Nibbles::unpack(keccak256(addr)));
    }
    for (addr, key) in delta.storage.keys() {
        acct_targets.insert(Nibbles::unpack(keccak256(addr)));
        slot_targets
            .entry(*addr)
            .or_default()
            .insert(Nibbles::unpack(keccak256(key)));
    }

    // Each round must add at least one new target. A repeat means the walk
    // cannot supply what the recompute needs: a real incompleteness, not a
    // fixed-point step. Fail rather than loop forever.
    loop {
        let mut nodes: Vec<bytes::Bytes> = Vec::new();
        let targets: Vec<Nibbles> = acct_targets.iter().copied().collect();
        let (walked_root, mut acct_nodes) =
            account_proof_nodes(tx, tables.account_trie, tables.hashed_accounts, &targets)
                .map_err(|e| ExecutorError::State(format!("account proof walk: {e}")))?;
        if walked_root != pre_state_root {
            return Err(ExecutorError::WitnessUnanchored(format!(
                "committed trie root {walked_root} != claimed pre_state_root {pre_state_root} \
                 — the read view is not the state the witness was captured against"
            )));
        }
        nodes.append(&mut acct_nodes);
        for (addr, keys) in &slot_targets {
            let stargets: Vec<Nibbles> = keys.iter().copied().collect();
            let (_, mut snodes) = storage_proof_nodes(
                tx,
                tables.storage_trie,
                tables.hashed_storage,
                keccak256(addr),
                &stargets,
            )
            .map_err(|e| ExecutorError::State(format!("storage proof walk {addr}: {e}")))?;
            nodes.append(&mut snodes);
        }
        // Canonical wire form: sort by keccak hash, and remove duplicates.
        let mut keyed: Vec<(alloy_primitives::B256, bytes::Bytes)> =
            nodes.into_iter().map(|n| (keccak256(&n), n)).collect();
        keyed.sort_by_key(|(h, _)| *h);
        keyed.dedup_by_key(|(h, _)| *h);
        let proofs = kardamom_types::WitnessProofs {
            nodes: keyed.into_iter().map(|(_, n)| n).collect(),
        };

        match verify_witness_anchored(witness, &proofs)
            .and_then(|pre| recompute_post_root(witness, &proofs, &pre, delta))
        {
            Ok(post_root) => return Ok((proofs, post_root)),
            Err(AnchorError::MissingNode {
                path,
                account,
                hash,
            }) => {
                let fresh = match account {
                    None => acct_targets.insert(path),
                    Some(addr) => slot_targets.entry(addr).or_default().insert(path),
                };
                if !fresh {
                    return Err(ExecutorError::WitnessUnanchored(format!(
                        "capture fixed point stalled: node {hash} at {path:?} \
                         (account {account:?}) missing from its own walk"
                    )));
                }
            }
            Err(e) => return Err(ExecutorError::from(e)),
        }
    }
}

/// Replay `records` over nothing but a witness: no state DB, no snapshot.
/// Three checks must all pass (phase 3): every tx record's identity is
/// re-derived from its raw bytes (keccak tx_hash and k256 sender
/// recovery), any read the witness does not cover aborts execution, and
/// the recomputed access list must equal `expected_bal` at the frame's
/// `granularity`.
pub fn reexecute_stateless(
    witness: &ExecutionWitness,
    parent: Option<&PendingDelta>,
    records: &[BufferedRecord],
    env: ExecEnv,
    expected_bal: &alloy_eip7928::BlockAccessList,
    granularity: u16,
) -> Result<BlockExecOutput, EngineError> {
    kardamom_engine::stateless::execute_block_stateless(
        witness,
        parent,
        records,
        env,
        expected_bal,
        granularity,
    )
}

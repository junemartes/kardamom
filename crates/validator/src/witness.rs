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
//! The driver itself lives in the `no_std` exec core
//! (`kardamom_exec_core::stateless`), and the stateless entry also
//! re-derives every tx's identity (keccak `tx_hash` and k256 sender
//! recovery). These wrappers are the validator-facing seam.

use std::collections::{BTreeMap, BTreeSet};

use alloy_primitives::{Address, B256, keccak256};
use kardamom_engine::actor::BlockExecOutput;
use kardamom_engine::anchor::{AnchorError, recompute_post_root, verify_witness_anchored};
use kardamom_engine::witness::WitnessRecorder;
use kardamom_engine::{EngineError, ExecEnv, PendingDelta};
use kardamom_state::trie::Nibbles;
use kardamom_state::trie::proofs::{account_proof_nodes, storage_proof_nodes};
use kardamom_types::{ExecutionWitness, StateDatabase, WitnessProofs};

use kardamom_engine::actor::BufferedRecord;

/// Re-execute a block sequentially. Capture the pre-state witness and the
/// block's raw (granularity-1) access list. Returns the execution output,
/// the canonical witness (keyed by `env.block_number`), and the BAL: the
/// full prover input set for one block.
///
/// # Errors
///
/// Returns an error if the re-execution itself fails.
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
///
/// # Errors
///
/// Returns an error if the trie walk fails, if the walked root does not
/// match `pre_state_root`, or if a `MissingNode` target repeats: the walk
/// cannot supply what the recompute needs.
pub fn anchor_block_witness<K: kardamom_state::trie::cursor::ReadKind>(
    tx: &kardamom_state::signet_libmdbx::TxSync<K>,
    tables: &kardamom_state::trie::TrieTables,
    pre_state_root: B256,
    witness: &mut ExecutionWitness,
    delta: &PendingDelta,
) -> Result<(WitnessProofs, B256), EngineError> {
    witness.pre_state_root = Some(pre_state_root);
    let (mut acct_targets, mut slot_targets) = initial_targets(witness, delta);

    // Each round must add at least one new target. A repeat means the walk
    // cannot supply what the recompute needs: a real incompleteness, not a
    // fixed-point step. Fail rather than loop forever.
    loop {
        let proofs = walk_proofs(tx, tables, &acct_targets, &slot_targets, pre_state_root)?;
        let Some(post_root) = try_anchor_round(
            witness,
            delta,
            &proofs,
            &mut acct_targets,
            &mut slot_targets,
        )?
        else {
            continue;
        };
        return Ok((proofs, post_root));
    }
}

/// One fixed-point round: verify the witness against `proofs` and
/// recompute the post-state root. `Ok(None)` means the walk found a
/// missing node and grew the targets for another round; the `loop` in
/// [`anchor_block_witness`] stays free of a branch.
fn try_anchor_round(
    witness: &mut ExecutionWitness,
    delta: &PendingDelta,
    proofs: &WitnessProofs,
    acct_targets: &mut BTreeSet<Nibbles>,
    slot_targets: &mut BTreeMap<Address, BTreeSet<Nibbles>>,
) -> Result<Option<B256>, EngineError> {
    match verify_witness_anchored(witness, proofs)
        .and_then(|pre| recompute_post_root(proofs, &pre, delta))
    {
        Ok(post_root) => Ok(Some(post_root)),
        Err(AnchorError::MissingNode {
            path,
            account,
            hash,
        }) => {
            if add_missing_target(acct_targets, slot_targets, path, account) {
                Ok(None)
            } else {
                Err(EngineError::WitnessUnanchored(format!(
                    "capture fixed point stalled: node {hash} at {path:?} \
                     (account {account:?}) missing from its own walk"
                )))
            }
        }
        Err(e) => Err(EngineError::from(e)),
    }
}

/// Build the initial account and storage-slot targets: the witness's read
/// set, plus the delta's write set (the recompute also touches every
/// written account and slot).
fn initial_targets(
    witness: &ExecutionWitness,
    delta: &PendingDelta,
) -> (BTreeSet<Nibbles>, BTreeMap<Address, BTreeSet<Nibbles>>) {
    let acct_targets: BTreeSet<Nibbles> = witness
        .accounts
        .iter()
        .map(|a| Nibbles::unpack(keccak256(a.address)))
        .chain(
            delta
                .accounts
                .keys()
                .map(|addr| Nibbles::unpack(keccak256(addr))),
        )
        .chain(
            delta
                .storage
                .keys()
                .map(|(addr, _)| Nibbles::unpack(keccak256(addr))),
        )
        .collect();
    let slot_targets = witness
        .storage
        .iter()
        .map(|s| (s.address, Nibbles::unpack(keccak256(s.key))))
        .chain(
            delta
                .storage
                .keys()
                .map(|(addr, key)| (*addr, Nibbles::unpack(keccak256(key)))),
        )
        .fold(
            BTreeMap::<Address, BTreeSet<Nibbles>>::new(),
            |mut map, (addr, nib)| {
                map.entry(addr).or_default().insert(nib);
                map
            },
        );
    (acct_targets, slot_targets)
}

/// Walk the account and storage tries for `acct_targets` and
/// `slot_targets`, and return the canonical, sorted, deduplicated proof
/// node set.
///
/// # Errors
///
/// Returns an error if the walk fails, or if the walked root does not
/// match `pre_state_root`: proof that the read view is not the state the
/// witness was captured against.
fn walk_proofs<K: kardamom_state::trie::cursor::ReadKind>(
    tx: &kardamom_state::signet_libmdbx::TxSync<K>,
    tables: &kardamom_state::trie::TrieTables,
    acct_targets: &BTreeSet<Nibbles>,
    slot_targets: &BTreeMap<Address, BTreeSet<Nibbles>>,
    pre_state_root: B256,
) -> Result<WitnessProofs, EngineError> {
    let mut nodes: Vec<bytes::Bytes> = Vec::new();
    let targets: Vec<Nibbles> = acct_targets.iter().copied().collect();
    let (walked_root, mut acct_nodes) =
        account_proof_nodes(tx, tables.account_trie, tables.hashed_accounts, &targets)
            .map_err(|e| EngineError::State(format!("account proof walk: {e}")))?;
    if walked_root != pre_state_root {
        return Err(EngineError::WitnessUnanchored(format!(
            "committed trie root {walked_root} != claimed pre_state_root {pre_state_root} \
             — the read view is not the state the witness was captured against"
        )));
    }
    nodes.append(&mut acct_nodes);
    let storage_nodes: Vec<Vec<bytes::Bytes>> = slot_targets
        .iter()
        .map(|(addr, keys)| {
            let stargets: Vec<Nibbles> = keys.iter().copied().collect();
            storage_proof_nodes(
                tx,
                tables.storage_trie,
                tables.hashed_storage,
                keccak256(addr),
                &stargets,
            )
            .map(|(_, snodes)| snodes)
            .map_err(|e| EngineError::State(format!("storage proof walk {addr}: {e}")))
        })
        .collect::<Result<Vec<_>, EngineError>>()?;
    nodes.extend(storage_nodes.into_iter().flatten());
    // Canonical wire form: sort by keccak hash, and remove duplicates.
    let mut keyed: Vec<(B256, bytes::Bytes)> =
        nodes.into_iter().map(|n| (keccak256(&n), n)).collect();
    keyed.sort_by_key(|(h, _)| *h);
    keyed.dedup_by_key(|(h, _)| *h);
    Ok(WitnessProofs {
        nodes: keyed.into_iter().map(|(_, n)| n).collect(),
    })
}

/// Record a missing-node target the guest's replay named. Returns `true`
/// if the target is new, so the caller can tell a real fixed-point step
/// from a stalled repeat.
fn add_missing_target(
    acct_targets: &mut BTreeSet<Nibbles>,
    slot_targets: &mut BTreeMap<Address, BTreeSet<Nibbles>>,
    path: Nibbles,
    account: Option<Address>,
) -> bool {
    match account {
        None => acct_targets.insert(path),
        Some(addr) => slot_targets.entry(addr).or_default().insert(path),
    }
}

/// Replay `records` over nothing but a witness: no state DB, no snapshot.
/// Three checks must all pass: every tx record's identity is re-derived
/// from its raw bytes (keccak `tx_hash` and k256 sender recovery), any read
/// the witness does not cover aborts execution, and the recomputed access
/// list must equal `expected_bal` at the frame's `granularity`.
///
/// # Errors
///
/// Returns an error if any of the three checks fails.
pub fn reexecute_stateless(
    witness: &ExecutionWitness,
    parent: Option<&PendingDelta>,
    records: &[BufferedRecord],
    env: ExecEnv,
    expected_bal: &alloy_eip7928::BlockAccessList,
    granularity: core::num::NonZeroU16,
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

//! Validator-side witness capture + stateless re-execution (spec:
//! no-std-exec-core, phase 2).
//!
//! The validator is one of the state DB's three consumers, so witness
//! collection lives HERE — the batcher stays state-free, and a witness-fed
//! prover downstream needs no state access at all.
//!
//! [`capture_block_witness`] runs the ordinary sequential block re-execution
//! with a [`WitnessRecorder`] interposed at the snapshot seam, returning both
//! the execution output and the pre-state slice it read.
//! [`reexecute_stateless`] replays the same records over nothing but that
//! witness — the zk-guest execution shape. The two outputs must be
//! IDENTICAL; `tests/stateless_reexec.rs` holds the round-trip contract.
//! (The sequential driver itself still lives in this std crate; hoisting a
//! `no_std` block driver into `kardamom-exec-core` is phase-3 work, where
//! the guest program needs it.)

use kardamom_engine::actor::BlockExecOutput;
use kardamom_engine::witness::{WitnessDb, WitnessRecorder};
use kardamom_engine::{EngineError, ExecEnv, PendingDelta};
use kardamom_types::{ExecutionWitness, StateDatabase};

use kardamom_engine::actor::BufferedRecord;

use crate::parallel::execute_block_sequential;

/// Re-execute a block sequentially while capturing the pre-state witness.
/// Returns the execution output plus the canonical witness (keyed by
/// `env.block_number`).
pub fn capture_block_witness<S: StateDatabase>(
    snapshot: &S,
    parent: Option<&PendingDelta>,
    records: &[BufferedRecord],
    env: ExecEnv,
) -> Result<(BlockExecOutput, ExecutionWitness), EngineError> {
    let recorder = WitnessRecorder::new(snapshot);
    let out = execute_block_sequential(&recorder, parent, records, env)?;
    Ok((out, recorder.into_witness(env.block_number)))
}

/// Replay `records` over NOTHING but a witness — no state DB, no snapshot.
/// Fail-closed: any read the witness does not cover aborts the execution
/// (surfaced as an [`EngineError`]).
pub fn reexecute_stateless(
    witness: &ExecutionWitness,
    parent: Option<&PendingDelta>,
    records: &[BufferedRecord],
    env: ExecEnv,
) -> Result<BlockExecOutput, EngineError> {
    let db = WitnessDb::from_witness(witness);
    execute_block_sequential(&db, parent, records, env)
}

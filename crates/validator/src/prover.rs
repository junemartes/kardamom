//! The prover spool (spec: no-std-exec-core, phase 3c): live assembly of
//! anchored prover inputs, one frame per block, behind `--prove-batches`.
//!
//! Proving must never touch chain liveness, so the spool sits ENTIRELY off
//! the hot path: it feeds from the flight ring (records the block-exec
//! strategy already retains) and from the writer's snapshot channel, and
//! it PINS each block's pre-state by holding the [`StateSnapshot`] whose
//! MVCC read txn anchors exactly that state — the "stamp when the parent's
//! commit settles" rule made concrete. A block whose pre-state snapshot
//! was never observed (the settle sweep can jump several blocks under the
//! depth-K pipeline) or whose records aged out of the ring is DROPPED with
//! a counter, never awaited: proving lags, it does not stall, and PR 4's
//! batch cursor tolerates gaps by design.
//!
//! Each spooled frame is `spool_dir/block-N/{prover-input.rkyv,
//! expected-outputs.bin}` — exactly the fixture layout the SP1 host runner
//! (`guest/kardamom-zk-host`) consumes, so the spool IS the prover queue.

use std::path::PathBuf;
use std::sync::Arc;

use alloy_primitives::keccak256;
use kardamom_engine::actor::{BlockExec, BufferedRecord};
use kardamom_engine::block_env::ExecEnv;
use kardamom_engine::delta::PendingDelta;
use kardamom_engine::error::ExecutorError;
use kardamom_state::trie::TrieTables;
use kardamom_state::{SnapshotReceiver, StateSnapshot};
use kardamom_types::{
    BlockBoundaryStart, ExecutionWitness, ProverInput, ProverRecord, PublicOutputs, StateDatabase,
    WitnessProofs,
};

use crate::flight::FlightRing;
use crate::parallel::execute_block_sequential;
use crate::witness::{anchor_block_witness, capture_block_witness};

/// A whole-block strategy for validators that run `--prove-batches` WITHOUT
/// `--parallel-validation`: identical semantics to the engine's streaming
/// path (it delegates to the shared sequential driver), but records flow
/// through the whole-block buffer so the flight ring — the spool's feed —
/// sees every block.
pub fn sequential_block_exec<D: StateDatabase + Sync + 'static>(
    flight: Arc<FlightRing>,
) -> BlockExec<D> {
    Box::new(
        move |snapshot: &D,
              parent: Option<&PendingDelta>,
              records: &[BufferedRecord],
              env: ExecEnv,
              block: u64| {
            flight.push(block, 1, env, records, None);
            execute_block_sequential(snapshot, parent, records, env)
        },
    )
}

/// Convert exec-core records to the prover wire (the guest rebuilds them).
fn wire_records(records: &[BufferedRecord]) -> Vec<ProverRecord> {
    records
        .iter()
        .map(|r| match r {
            BufferedRecord::Tx {
                tx_idx,
                envelope,
                position,
            } => ProverRecord::Tx {
                tx_idx: tx_idx.0,
                envelope: envelope.clone(),
                position: *position,
            },
            BufferedRecord::Deposit {
                tx_idx,
                deposit,
                position,
            } => ProverRecord::Deposit {
                tx_idx: tx_idx.0,
                deposit: deposit.clone(),
                position: *position,
            },
        })
        .collect()
}

/// Capture, anchor, and spool ONE block against its pinned pre-state
/// snapshot. `snap` MUST be anchored at `block - 1`.
pub fn spool_block(
    spool_dir: &std::path::Path,
    chain_id: u64,
    snap: &StateSnapshot,
    block: u64,
    env: ExecEnv,
    records: &[BufferedRecord],
) -> Result<PublicOutputs, ExecutorError> {
    if snap.block_number() != block.saturating_sub(1) {
        return Err(ExecutorError::WitnessUnanchored(format!(
            "pre-state window mismatch: snapshot pinned at block {}, proving block {block}",
            snap.block_number()
        )));
    }
    let (out, mut witness, bal) = capture_block_witness(snap, None, records, env)?;
    let pre_root = snap
        .state_root()
        .map_err(|e| ExecutorError::State(format!("snapshot state_root: {e}")))?
        .ok_or_else(|| {
            ExecutorError::State(
                "no committed trie root at the pre-state snapshot — \
                 --prove-batches requires the trie-aware writer (TrieMode::Incremental)"
                    .into(),
            )
        })?;
    let txn = snap.ro_txn();
    let tables = TrieTables::open(txn)
        .map_err(|e| ExecutorError::State(format!("open trie tables: {e}")))?;
    let (proofs, post_root) =
        anchor_block_witness(txn, &tables, pre_root, &mut witness, &out.delta)?;

    let mut bal_rlp = Vec::new();
    alloy_rlp::Encodable::encode(&bal, &mut bal_rlp);
    let outputs = PublicOutputs {
        pre_state_root: pre_root,
        post_state_root: post_root,
        bal_commitment: keccak256(&bal_rlp),
        block_number: block,
    };
    let input = assemble_prover_input(chain_id, env, witness, proofs, records, bal_rlp, 1);
    let bytes = rkyv::to_bytes::<rkyv::rancor::Error>(&input)
        .map_err(|e| ExecutorError::State(format!("serialize prover input: {e}")))?;

    let dir = spool_dir.join(format!("block-{block}"));
    std::fs::create_dir_all(&dir)
        .and_then(|()| std::fs::write(dir.join("prover-input.rkyv"), &bytes))
        .and_then(|()| std::fs::write(dir.join("expected-outputs.bin"), outputs.encode()))
        .map_err(|e| ExecutorError::State(format!("write spool frame: {e}")))?;
    Ok(outputs)
}

/// Build the wire frame. The boundary carried is the one the block executed
/// under (`env` holds its fields — reconstructed to the exact live shape).
pub fn assemble_prover_input(
    chain_id: u64,
    env: ExecEnv,
    witness: ExecutionWitness,
    proofs: WitnessProofs,
    records: &[BufferedRecord],
    bal_rlp: Vec<u8>,
    granularity: u16,
) -> ProverInput {
    ProverInput {
        chain_id,
        boundary: BlockBoundaryStart {
            block_number: env.block_number,
            end_tx_idx: kardamom_types::BPosition::from_index(0),
            l2_timestamp: env.l2_timestamp,
            l1_origin: 0,
        },
        witness,
        proofs,
        records: wire_records(records),
        bal_rlp: bal_rlp.into(),
        granularity,
    }
}

/// Spawn the spool task. Parks on the writer's snapshot watch; for each
/// published snapshot at block M, tries to prove block M+1 (records from
/// the flight ring) against that PINNED snapshot. Blocks whose window was
/// skipped are counted and dropped — the watch slot is latest-wins, so a
/// slow spool can still land past intermediate snapshots, but the wake is
/// now per publish instead of a 100 ms timer.
pub fn spawn_prover_spool(
    spool_dir: PathBuf,
    chain_id: u64,
    snap_rx: SnapshotReceiver,
    flight: Arc<FlightRing>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut watch = snap_rx.watch();
        // The next block to prove; its pre-state snapshot is `pending - 1`.
        let mut pending: Option<u64> = None;
        let mut held: Option<StateSnapshot> = None;
        loop {
            // Writer gone => the chain is shutting down; exit the task.
            if watch.changed().await.is_err() {
                return;
            }
            let Some(snap) = watch.borrow_and_update().clone() else {
                continue;
            };
            let at = snap.block_number();
            let next = *pending.get_or_insert(at + 1);
            if held.as_ref().is_none_or(|h| h.block_number() != next - 1) {
                if at == next - 1 {
                    held = Some(snap.clone());
                } else if at >= next {
                    // The settle sweep jumped past next-1: those pre-state
                    // views are unreachable now (MVCC has no history API).
                    crate::metrics::counter_prover_skipped((at + 1) - next);
                    tracing::warn!(
                        from = next,
                        through = at,
                        "prover spool: pre-state snapshots skipped; blocks dropped"
                    );
                    pending = Some(at + 1);
                    held = None;
                    continue;
                } else {
                    continue; // snapshot still behind the pending block
                }
            }
            let Some((_, env, records)) = flight.records_for(next) else {
                // Records not executed yet (normal), or aged out of the
                // ring (drop — the ring outpaced us).
                if at >= next + 2 {
                    crate::metrics::counter_prover_skipped(1);
                    tracing::warn!(block = next, "prover spool: records aged out; dropped");
                    pending = Some(next + 1);
                    held = None;
                }
                continue;
            };
            let snap_held = held.take().expect("held checked above");
            match spool_block(&spool_dir, chain_id, &snap_held, next, env, &records) {
                Ok(outputs) => {
                    crate::metrics::counter_prover_spooled();
                    tracing::info!(
                        block = next,
                        post_root = %outputs.post_state_root,
                        "prover spool: frame written"
                    );
                }
                Err(e) => {
                    // An anchoring failure here is a REAL integrity signal
                    // (the same classes the guest fail-stops on) — but the
                    // spool is an observer, not a verifier seam: log loudly
                    // and keep the chain alive; the verification paths own
                    // fail-stop.
                    crate::metrics::counter_prover_failed();
                    tracing::error!(block = next, error = %e, "prover spool: block failed");
                }
            }
            pending = Some(next + 1);
        }
    })
}

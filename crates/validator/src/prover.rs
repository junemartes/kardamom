//! The prover spool: live assembly of
//! anchored prover inputs, one frame per block, behind `--prove-batches`.
//!
//! Proving must never touch chain liveness, so the spool sits entirely off
//! the hot path. It feeds from the flight ring (records the block-exec
//! strategy already retains) and from the writer's snapshot channel. It
//! pins each block's pre-state by holding the [`StateSnapshot`] whose MVCC
//! read txn anchors exactly that state; this makes the "stamp when the
//! parent's commit settles" rule concrete. A block whose pre-state
//! snapshot was never observed (the settle sweep can jump several blocks
//! under the depth-K pipeline), or whose records aged out of the ring, is
//! dropped with a counter, never awaited. Proving lags, but it does not
//! stall, and the batch cursor tolerates gaps by design.
//!
//! Each spooled frame is `spool_dir/block-N/{prover-input.rkyv,
//! expected-outputs.bin}`. This is exactly the fixture layout the SP1 host
//! runner (`guest/kardamom-zk-host`) consumes, so the spool is the prover
//! queue.

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

/// A whole-block strategy for validators that run `--prove-batches`
/// without `--parallel-validation`. It has the same semantics as the
/// engine's streaming path, since it delegates to the shared sequential
/// driver, but records flow through the whole-block buffer, so the flight
/// ring, the spool's feed, sees every block.
pub fn sequential_block_exec<D: StateDatabase + Sync + 'static>(
    flight: Arc<FlightRing>,
) -> BlockExec<D> {
    Box::new(
        move |snapshot: &D,
              parent: Option<&PendingDelta>,
              records: &[BufferedRecord],
              env: ExecEnv,
              block: u64| {
            flight.push(block, std::num::NonZeroU16::MIN, env, records, None);
            execute_block_sequential(snapshot, parent, records, env)
        },
    )
}

/// Convert exec-core records to the prover wire form; the guest rebuilds
/// them.
///
/// Cross-chain (0x7D) deliveries have no `ProverRecord` shape yet — the
/// guest cannot rebuild an `XChainMessage`'s Inbox call, and its identity
/// is a trusted input in the same way a deposit's is (see the module docs
/// on `kardamom_exec_core::stateless`). A block that carries one is not
/// provable yet, so this fails closed instead of silently dropping the
/// record from the digest.
fn wire_records(records: &[BufferedRecord]) -> Result<Vec<ProverRecord>, ExecutorError> {
    records
        .iter()
        .map(|r| match r {
            BufferedRecord::Tx {
                tx_idx,
                envelope,
                position,
            } => Ok(ProverRecord::Tx {
                tx_idx: tx_idx.0,
                envelope: envelope.clone(),
                position: *position,
            }),
            BufferedRecord::Deposit {
                tx_idx,
                deposit,
                position,
            } => Ok(ProverRecord::Deposit {
                tx_idx: tx_idx.0,
                deposit: deposit.clone(),
                position: *position,
            }),
            BufferedRecord::XChain { .. } => Err(ExecutorError::State(
                "prover spool: cross-chain (0x7D) deliveries have no prover-wire shape yet".into(),
            )),
        })
        .collect()
}

/// A pre-state snapshot known to be anchored at `block - 1`, checked
/// once at construction so [`spool_block`] trusts it instead of
/// re-checking it on every call. [`SpoolCursor::pin`] builds one for
/// every block the live spool proves, already knowing the anchor holds;
/// [`PinnedPreState::new`] is the checked constructor for anyone else
/// (tests, mainly) building one from a raw snapshot.
pub struct PinnedPreState {
    snap: StateSnapshot,
    block: u64,
}

impl std::fmt::Debug for PinnedPreState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PinnedPreState")
            .field("pre_state_block", &self.snap.block_number())
            .field("block", &self.block)
            .finish()
    }
}

impl PinnedPreState {
    /// # Errors
    ///
    /// Returns an error if `snap` is not anchored at `block - 1`.
    pub fn new(snap: StateSnapshot, block: u64) -> Result<Self, ExecutorError> {
        if snap.block_number() != block.saturating_sub(1) {
            return Err(ExecutorError::WitnessUnanchored(format!(
                "pre-state window mismatch: snapshot pinned at block {}, proving block {block}",
                snap.block_number()
            )));
        }
        Ok(Self { snap, block })
    }

    /// Build without checking, for [`SpoolCursor::pin`], which already
    /// tracks the anchor itself and calls this only once it holds.
    fn trusted(snap: StateSnapshot, block: u64) -> Self {
        Self { snap, block }
    }

    fn snap(&self) -> &StateSnapshot {
        &self.snap
    }

    #[must_use]
    pub fn block(&self) -> u64 {
        self.block
    }
}

/// Capture, anchor, and spool one block against its pinned pre-state
/// snapshot.
///
/// # Errors
///
/// Returns an error if capture or anchoring fails, or if writing the
/// spooled frame fails.
pub fn spool_block(
    spool_dir: &std::path::Path,
    chain_id: u64,
    pinned: &PinnedPreState,
    env: ExecEnv,
    records: &[BufferedRecord],
) -> Result<PublicOutputs, ExecutorError> {
    let snap = pinned.snap();
    let block = pinned.block();
    let (out, mut witness, bal) = capture_block_witness(snap, None, records, env)?;
    let pre_root = pre_state_root(snap)?;
    let txn = snap.ro_txn();
    let tables = TrieTables::open(txn)
        .map_err(|e| ExecutorError::State(format!("open trie tables: {e}")))?;
    let (proofs, post_root) =
        anchor_block_witness(txn, &tables, pre_root, &mut witness, &out.delta)?;

    let mut bal_rlp = Vec::new();
    alloy_rlp::Encodable::encode(&bal, &mut bal_rlp);
    let mut digest = kardamom_types::BlockRecordsDigest::new(block);
    records
        .iter()
        .filter_map(|r| match r {
            BufferedRecord::Tx { envelope, .. } => Some(envelope),
            _ => None,
        })
        .for_each(|e| digest.add_tx(&e.raw_tx));
    let outputs = PublicOutputs {
        pre_state_root: pre_root,
        post_state_root: post_root,
        block_number: block,
        records_digest: digest.finish(),
        bal_commitment: keccak256(&bal_rlp),
    };
    let input = assemble_prover_input(chain_id, env, witness, proofs, records, bal_rlp, 1)?;
    let bytes = rkyv::to_bytes::<rkyv::rancor::Error>(&input)
        .map_err(|e| ExecutorError::State(format!("serialize prover input: {e}")))?;

    write_frame(&spool_dir.join(format!("block-{block}")), &bytes, &outputs)?;
    Ok(outputs)
}

/// Read the pre-state's committed trie root.
///
/// # Errors
///
/// Returns an error if the snapshot's trie root cannot be read, or if the
/// pre-state snapshot has no committed trie root at all (`--prove-batches`
/// requires the trie-aware writer, `TrieMode::Incremental`).
fn pre_state_root(snap: &StateSnapshot) -> Result<alloy_primitives::B256, ExecutorError> {
    snap.state_root()
        .map_err(|e| ExecutorError::State(format!("snapshot state_root: {e}")))?
        .ok_or_else(|| {
            ExecutorError::State(
                "no committed trie root at the pre-state snapshot — \
                 --prove-batches requires the trie-aware writer (TrieMode::Incremental)"
                    .into(),
            )
        })
}

/// Write one spooled frame's two files under `dir`.
fn write_frame(
    dir: &std::path::Path,
    bytes: &[u8],
    outputs: &PublicOutputs,
) -> Result<(), ExecutorError> {
    std::fs::create_dir_all(dir)
        .and_then(|()| std::fs::write(dir.join("prover-input.rkyv"), bytes))
        .and_then(|()| std::fs::write(dir.join("expected-outputs.bin"), outputs.encode()))
        .map_err(|e| ExecutorError::State(format!("write spool frame: {e}")))
}

/// Build the wire frame. The boundary carried is the one the block ran
/// under; `env` holds its fields, rebuilt to the exact live shape.
fn assemble_prover_input(
    chain_id: u64,
    env: ExecEnv,
    witness: ExecutionWitness,
    proofs: WitnessProofs,
    records: &[BufferedRecord],
    bal_rlp: Vec<u8>,
    granularity: u16,
) -> Result<ProverInput, ExecutorError> {
    Ok(ProverInput {
        chain_id,
        boundary: BlockBoundaryStart {
            block_number: env.block_number,
            end_tx_idx: kardamom_types::BPosition::from_index(0),
            l2_timestamp: env.l2_timestamp,
            l1_origin: 0,
        },
        witness,
        proofs,
        records: wire_records(records)?,
        bal_rlp: bal_rlp.into(),
        granularity,
    })
}

/// Spawn the spool task. It waits on the writer's snapshot watch. For each
/// published snapshot at block M, it tries to prove block M+1, with records
/// from the flight ring, against that pinned snapshot. Blocks whose window
/// was skipped are counted and dropped. The watch slot holds only the
/// latest snapshot. A slow spool can still skip past older ones. The task
/// wakes on each publish, not on a 100 ms timer.
pub fn spawn_prover_spool(
    spool_dir: PathBuf,
    chain_id: u64,
    snap_rx: SnapshotReceiver,
    flight: Arc<FlightRing>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut watch = snap_rx.watch();
        let mut cursor = SpoolCursor::new();
        loop {
            // Writer gone => the chain is shutting down; exit the task.
            if watch.changed().await.is_err() {
                return;
            }
            let Some(snap) = watch.borrow_and_update().clone() else {
                continue;
            };
            let at = snap.block_number();
            let PinOutcome::Ready(pinned) = cursor.pin(snap) else {
                continue;
            };
            let next = pinned.block();
            let Some((_, env, records)) = cursor.take_records(&flight, next, at) else {
                continue;
            };
            match spool_block(&spool_dir, chain_id, &pinned, env, &records) {
                Ok(outputs) => {
                    crate::metrics::counter_prover_spooled();
                    tracing::info!(
                        block = next,
                        post_root = %outputs.post_state_root,
                        "prover spool: frame written"
                    );
                }
                Err(e) => {
                    // An anchoring failure here is a real integrity
                    // signal, one of the same classes the guest stops on.
                    // But the spool is an observer, not a verifier seam:
                    // log it loudly and keep the chain alive. The
                    // verification paths own the stop.
                    crate::metrics::counter_prover_failed();
                    tracing::error!(block = next, error = %e, "prover spool: block failed");
                }
            }
            cursor.advance(next);
        }
    })
}

/// Outcome of one pin-state update: either the next block to prove is
/// pinned and ready, carrying its pre-state snapshot, or the caller
/// should wait for the next snapshot.
enum PinOutcome {
    Ready(PinnedPreState),
    Pending,
}

/// Tracks which block's pre-state snapshot is pinned, so the spool can
/// wait for the exact commit that anchors the next block to prove, and
/// tolerate the settle sweep skipping ahead.
struct SpoolCursor {
    /// The next block to prove; its pre-state snapshot is `pending - 1`.
    pending: Option<u64>,
    held: Option<StateSnapshot>,
}

impl SpoolCursor {
    fn new() -> Self {
        Self {
            pending: None,
            held: None,
        }
    }

    /// Advance the pin state given the newly observed snapshot. `held`
    /// keeps holding the pinned pre-state snapshot across calls (a
    /// cheap `Arc` handle) until [`take_records`](Self::take_records)
    /// confirms the block is ready to spool, or the pin resets.
    fn pin(&mut self, snap: StateSnapshot) -> PinOutcome {
        // Bounded: `at` is this validator's own committed block number,
        // monotone from genesis and nowhere near `u64::MAX` in a real
        // run, so `at + 1` below cannot overflow. `self.pending` is only
        // ever set from `at + 1` or `next + 1` (never from a wire value),
        // so `next >= 1` wherever `next - 1` appears below.
        let at = snap.block_number();
        let next = *self.pending.get_or_insert(at + 1);

        if let Some(pre_state) = self.held.clone()
            && pre_state.block_number() == next - 1
        {
            return PinOutcome::Ready(PinnedPreState::trusted(pre_state, next));
        }
        if at == next - 1 {
            self.held = Some(snap.clone());
            return PinOutcome::Ready(PinnedPreState::trusted(snap, next));
        }
        if at >= next {
            // The settle sweep jumped past next-1. Those pre-state
            // views are unreachable now, since MVCC has no history API.
            // Bounded: this arm is reached only under `at >= next`, so
            // `(at + 1) - next` cannot underflow.
            crate::metrics::counter_prover_skipped((at + 1) - next);
            tracing::warn!(
                from = next,
                through = at,
                "prover spool: pre-state snapshots skipped; blocks dropped"
            );
            self.pending = Some(at + 1);
            self.held = None;
            return PinOutcome::Pending;
        }
        PinOutcome::Pending // The snapshot is still behind the pending block.
    }

    /// Take `next`'s records from the flight ring, if executed and still
    /// retained. `None` means either not executed yet (normal — the
    /// pinned snapshot stays held for the next call), or aged out of the
    /// ring, in which case the pin state resets so the spool catches up.
    /// On success, releases the held snapshot: the caller now owns the
    /// only copy this cursor needs to give out for this block.
    fn take_records(
        &mut self,
        flight: &FlightRing,
        next: u64,
        at: u64,
    ) -> Option<(std::num::NonZeroU16, ExecEnv, Vec<BufferedRecord>)> {
        let found = flight.records_for(next);
        if found.is_some() {
            self.held = None;
        } else if at >= next.saturating_add(2) {
            crate::metrics::counter_prover_skipped(1);
            tracing::warn!(block = next, "prover spool: records aged out; dropped");
            self.pending = Some(next + 1);
            self.held = None;
        }
        found
    }

    /// Move past `next`, whether it was spooled or not: the next call to
    /// [`pin`](Self::pin) looks for `next + 1`'s pre-state.
    fn advance(&mut self, next: u64) {
        self.pending = Some(next + 1);
    }
}

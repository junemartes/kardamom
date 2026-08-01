//! Executor actor: M tx_data reader threads + 1 tx_ordering reader thread +
//! sequential execution thread + commit thread.
//!
//! ## Topology change (S4-arch-update, /)
//!
//! Pre-S4-arch-update there was **one** tx_ordering reader thread that pulled
//! full `TxEnvelope`s off tx_ordering. Post- the inbound demux is split:
//!
//! - **M tx_data reader threads** (one per sequencer partition) each
//!   subscribe to their tx_data and stream full `TxEnvelope`s into a shared
//!   **join buffer** keyed by `(sequencer_id, tx_data_position)`.
//! - **One tx_ordering reader thread** pulls tiny `TxOrderingMessage` records
//!   (`TxRef | BoundaryStart`) in canonical order. For each `TxRef`, it joins
//!   against the buffer and hands `(b_position, TxEnvelope)` to the exec
//!   thread. For each `BoundaryStart`, it forwards verbatim.
//!
//! The exec thread, commit thread, state-snapshot swap protocol, write-set
//! hashing, and tx_receipts emission are **unchanged** — the executor's
//! external contract (consume canonical-ordered txs + boundaries, produce
//! ordered receipts + slim boundaries on tx_receipts) is identical. Only the
//! inbound demux moves.
//!
//! See `reader.rs` for the join-buffer + reader-thread implementation.
//!
//! Wiring:
//! ```text
//!   tx_data[0..M]    tx_ordering
//!        │                │
//!        ▼                ▼
//!   ┌─────────┐     ┌──────────┐
//!   │M readers│──►  │B reader  │──► exec ──► commit ──► tx_receipts
//!   │ (insert │join │(lookup+  │
//!   │ buffer) │buf  │ forward) │
//!   └─────────┘     └──────────┘
//! ```
//!
//! Each Aeron-touching thread (the M+1 reader threads in production) owns
//! its own `rusteron_client::Aeron` (`!Send + !Sync`) on a dedicated OS
//! thread; cross-thread coordination uses the `DashMap` join buffer and
//! crossbeam channels.

use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

use crossbeam_channel::{Receiver, Sender, bounded};
use tracing::{debug, warn};

use kardamom_types::{
    BPosition, BlockBoundary, BlockBoundaryStart, BlockDelta, Receipt, SnapshotSource,
};

use crate::block_env::ExecEnv;
use crate::delta::PendingDelta;
use crate::error::ExecutorError;
use crate::exec_types::{CMessage, TxIndex};
use crate::executor::execute_deposit_tx;
use crate::executor::execute_tx;
use crate::reader::{
    DepositJoinBuffer, DepositSubscription, JoinBuffer, ReaderConfig, ReaderToExec,
    TxDataSubscription, TxOrderingSubscription, spawn_tx_data_reader, spawn_tx_deposits_reader,
    spawn_tx_ordering_reader,
};

/// Publication handle for tx_receipts.
pub trait TxReceiptsPublication: Send {
    fn publish(&mut self, msg: CMessage) -> Result<(), ExecutorError>;

    /// Publish a RUN of receipts, amortizing per-publish overhead where the
    /// transport supports it. Returns `(published, error)`: the first
    /// `published` receipts are handed off; an error applies to the rest, so
    /// the caller's must-deliver retry resumes at the failed suffix. The
    /// default loops singles — the validator's verifying sink keeps its
    /// exact per-receipt divergence semantics — while the live transport
    /// packs the whole slice into ONE `Vec<Receipt>` wire frame: one encode
    /// and one blocking ack instead of one per receipt (the per-receipt ack
    /// round trip was the executor commit thread's dominant cost).
    fn publish_receipts(&mut self, receipts: &[Receipt]) -> (usize, Option<ExecutorError>) {
        for (i, r) in receipts.iter().enumerate() {
            if let Err(e) = self.publish(CMessage::Receipt(r.clone())) {
                return (i, Some(e));
            }
        }
        (receipts.len(), None)
    }
}

/// Signal from the state writer (S6): "block N is durable in mdbx; you may
/// swap to a snapshot >= N."
pub trait StateWriterSignal: Send {
    /// Block until the state writer reports a block number >= `await_at_least`
    /// has been committed. Returns the committed block number.
    fn wait_committed(&mut self, await_at_least: u64) -> Result<u64, ExecutorError>;

    /// Non-blocking probe: the highest durably-committed block right now
    /// (0 if nothing has committed yet). Drives the pipelined commit's
    /// settle sweep — completed commits settle opportunistically at each
    /// boundary without ever parking the exec thread.
    fn committed(&mut self) -> Result<u64, ExecutorError>;
}

/// Hand-off queue from executor → state writer. The state writer (S6)
/// consumes these to apply the block delta to libmdbx.
pub trait StateWriterQueue: Send {
    fn submit(&mut self, block: BlockBoundary, delta: BlockDelta) -> Result<(), ExecutorError>;
}

// Lets a binary pick between role-specific queue wrappers at runtime (e.g. the
// validator's optional attester tee) without monomorphising `Executor::run`
// twice.
impl StateWriterQueue for Box<dyn StateWriterQueue> {
    fn submit(&mut self, block: BlockBoundary, delta: BlockDelta) -> Result<(), ExecutorError> {
        (**self).submit(block, delta)
    }
}

// Same reason on the receipts seam: the validator boxes its sink so the
// optional attester tee can wrap it at runtime.
impl TxReceiptsPublication for Box<dyn TxReceiptsPublication> {
    fn publish(&mut self, msg: CMessage) -> Result<(), ExecutorError> {
        (**self).publish(msg)
    }

    fn publish_receipts(&mut self, receipts: &[Receipt]) -> (usize, Option<ExecutorError>) {
        (**self).publish_receipts(receipts)
    }
}

/// Where a restarted executor resumes from, derived from the persisted state
/// cursor (`kardamom_state::RecoveryPoint`). The canonical stream source (the
/// cluster's `REPLAY_FROM` on connect) delivers **from this cursor onward** —
/// records below it are deduped inside [`crate::reader::cluster`] — so the
/// reader and exec threads seed their counters here instead of replaying from
/// record 0 and skip-counting:
///
/// - `block` — the last durably-committed block (`last_committed_block`). The
///   first boundary delivered after resume is `block + 1`; the post-`block`
///   state snapshot is what execution resumes against.
/// - `record_count` — the cumulative count of canonical records (TxRef +
///   DepositRef) applied through `block` (`last_fsynced_b_position.as_index()`).
///   The reader assigns the first delivered record this index, so the boundary
///   alignment check (absolute counts) holds across the restart. Because
///   `record_count` is exactly the end count of `block`, the resume boundary
///   falls cleanly between blocks — no partial block is ever half-replayed.
/// - `l2_timestamp` — boundary `block`'s timestamp (from the committed header
///   row). Block N+1's txs execute with boundary N's timestamp, and a resumed
///   replica never sees boundary `block` again — without this seed its first
///   post-resume block would execute with ts=0 and silently diverge from the
///   replicas that never restarted.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ResumePoint {
    pub block: u64,
    pub record_count: u64,
    pub l2_timestamp: u64,
}

#[derive(Debug, Clone)]
pub struct ExecutorConfig {
    pub chain_id: u64,
    /// Bound on the receipt queue between exec and commit threads. Larger =
    /// more amortization, more memory.
    pub receipt_queue_depth: usize,
    /// Reader-layer tunables (join buffer timeout, growth warning
    /// threshold). See [`ReaderConfig`].
    pub reader: ReaderConfig,
}

impl Default for ExecutorConfig {
    fn default() -> Self {
        Self {
            chain_id: 1,
            receipt_queue_depth: 1024,
            reader: ReaderConfig::default(),
        }
    }
}

/// Per-block EIP-7928 handoff to the executor's BAL publisher thread:
/// (boundary, receipts-free merged delta for the frame's V1 section, the
/// captured Bal). Sent at each boundary when capture is enabled.
pub type BalHandoff = (
    BlockBoundary,
    kardamom_types::BlockDelta,
    revm::state::bal::Bal,
);

/// One canonical record buffered for whole-block execution (validator
/// parallel path). Mirrors [`crate::reader::ReaderToExec`]'s payload arms.
pub enum BufferedRecord {
    Tx {
        tx_idx: TxIndex,
        envelope: kardamom_types::TxEnvelope,
        position: BPosition,
    },
    Deposit {
        tx_idx: TxIndex,
        deposit: kardamom_types::Deposit,
        position: BPosition,
    },
}

/// What a block-execution strategy returns: the block's receipts in block
/// order (block-cumulative gas already correct) and its merged writes.
pub struct BlockExecOutput {
    pub receipts: Vec<Receipt>,
    pub delta: PendingDelta,
}

/// Optional whole-block execution strategy. `None` (the executor) keeps the
/// per-tx streaming path untouched. `Some` (the validator's parallel
/// verifier) makes the exec thread BUFFER a block's records and execute
/// them together at the boundary — which is what allows batches to run
/// concurrently, seeded from BAL claims.
pub type BlockExec<D> = Box<
    dyn Fn(&D, &[BufferedRecord], ExecEnv, u64) -> Result<BlockExecOutput, ExecutorError> + Send,
>;

/// Internal envelope routed from exec → commit thread.
enum ExecToCommit {
    Receipt(kardamom_types::Receipt),
    Boundary(BlockBoundary),
}

/// Owns the M+4 threads (M tx_data readers, 1 tx_deposits reader, 1
/// tx_ordering reader, 1 exec, 1 commit). `run` blocks until the
/// tx_ordering subscription closes or an error occurs.
pub struct Executor;

impl Executor {
    /// Spawn the readers, exec, commit threads and join them. Returns when
    /// tx_ordering closes cleanly or when any thread propagates a fatal
    /// error.
    ///
    /// `a_subs` holds one subscription per sequencer partition (M total).
    /// They may be supplied in any order — each subscription declares its
    /// own `sequencer_id`.
    ///
    /// `dep_sub` is the tx_deposits subscription. Pass `None` to disable
    /// the deposit path (legacy + most unit tests); when `None`, any
    /// `DepositRef` observed on tx_ordering will trigger a join timeout
    /// since no deposit can land in the buffer. Production wiring always
    /// passes `Some`.
    ///
    /// `recovery` is the archive-backed join-miss refetch factory (see
    /// [`crate::reader::JoinRecovery`]); `None` keeps the plain bounded join.
    #[allow(clippy::too_many_arguments)] // 10 args is the natural shape of the
    // executor's run-once API; see the long-form note in [`execute_tx`]
    // for the same rationale applied to per-tx execution.
    pub fn run<C, S, Q, P>(
        cfg: ExecutorConfig,
        a_subs: Vec<Box<dyn TxDataSubscription>>,
        b_sub: Box<dyn TxOrderingSubscription>,
        dep_sub: Option<Box<dyn DepositSubscription>>,
        c_pub: C,
        snapshots: S,
        sw_signal: Q,
        sw_queue: P,
        initial_block: u64,
        resume: Option<ResumePoint>,
        bal_tx: Option<Sender<BalHandoff>>,
        block_exec: Option<BlockExec<S::Db>>,
        recovery: Option<crate::reader::JoinRecoveryFactory>,
    ) -> Result<(), ExecutorError>
    where
        C: TxReceiptsPublication + 'static,
        S: SnapshotSource + 'static,
        Q: StateWriterSignal + 'static,
        P: StateWriterQueue + 'static,
    {
        let buffer = JoinBuffer::new();
        let deposit_buffer = DepositJoinBuffer::new();
        let (tx_r2e, rx_r2e) = bounded::<ReaderToExec>(cfg.receipt_queue_depth);
        let (tx_e2c, rx_e2c) = bounded::<ExecToCommit>(cfg.receipt_queue_depth);

        // M tx_data reader threads, one per sequencer partition. Each
        // owns its subscription for the duration; we collect the join
        // handles to surface any error.
        let mut a_handles: Vec<JoinHandle<Result<(), ExecutorError>>> =
            Vec::with_capacity(a_subs.len());
        for a in a_subs {
            // The trait object's `next` already advertises sequencer_id.
            a_handles.push(spawn_tx_data_reader(BoxedASub(a), buffer.clone()));
        }

        let dep_handle =
            dep_sub.map(|d| spawn_tx_deposits_reader(BoxedDepSub(d), deposit_buffer.clone()));

        let b_handle = spawn_tx_ordering_reader(
            BoxedBSub(b_sub),
            buffer.clone(),
            deposit_buffer.clone(),
            cfg.reader.clone(),
            tx_r2e,
            // The canonical source delivers from the resume cursor; indices
            // assigned here are checked against ABSOLUTE boundary counts.
            TxIndex(resume.map(|r| r.record_count).unwrap_or(0)),
            recovery,
        );

        let exec = spawn_exec(
            cfg.clone(),
            rx_r2e,
            tx_e2c,
            snapshots,
            sw_signal,
            sw_queue,
            initial_block,
            resume,
            bal_tx,
            block_exec,
        );
        let commit = spawn_commit(c_pub, rx_e2c);

        // Join the critical pipeline first: B reader (closes when tx_ordering
        // is exhausted), then exec, then commit.
        let r_b = b_handle.join().expect("tx_ordering reader panic");
        let r_exec = exec.join().expect("exec panic");
        let r_commit = commit.join().expect("commit panic");

        // If ANY of the pipeline threads errored (e.g. a fatal
        // BoundaryMisaligned in exec), the executor can no longer make
        // progress. Return immediately so the process exits and the
        // orchestrator restarts it. We must NOT fall through to joining the
        // tx_data (A) + deposit readers: those block in their Aeron `next()`
        // until the subscription closes, which only happens on process
        // teardown — so joining them while the process is still up would hang
        // forever and silently mask the pipeline error (the exact "frozen but
        // alive" failure this guards against). On the normal Ok path the
        // subscriptions have already closed (tx_ordering exhausted), so the A
        // + deposit joins return promptly and we drain them for clean
        // shutdown.
        // Surface any pipeline error here, before joining the A / deposit
        // readers below: on an error path tx_ordering may never close, so
        // joining those readers would hang (the "frozen but alive" failure
        // described above). On the Ok path this is a no-op and we fall through.
        r_b.and(r_exec).and(r_commit)?;
        let mut r_a: Result<(), ExecutorError> = Ok(());
        for h in a_handles {
            let res = h.join().expect("tx_data reader panic");
            if r_a.is_ok() {
                r_a = res;
            }
        }
        let r_dep = dep_handle
            .map(|h| h.join().expect("tx_deposits reader panic"))
            .unwrap_or(Ok(()));
        // `pipeline` was already checked above (Ok by here), so the result is
        // determined by the A and deposit reader joins.
        r_a.and(r_dep)
    }
}

// Trait-object adapters so the reader fns (generic on a concrete type) can
// own a `Box<dyn TxDataSubscription>` / `Box<dyn TxOrderingSubscription>`
// without requiring the caller to monomorphise per-M.
struct BoxedASub(Box<dyn TxDataSubscription>);
impl TxDataSubscription for BoxedASub {
    fn sequencer_id(&self) -> u8 {
        self.0.sequencer_id()
    }
    fn next(
        &mut self,
    ) -> Result<(kardamom_types::TxDataLoc, kardamom_types::TxEnvelope), ExecutorError> {
        self.0.next()
    }
}

struct BoxedBSub(Box<dyn TxOrderingSubscription>);
impl TxOrderingSubscription for BoxedBSub {
    fn next(&mut self) -> Result<(BPosition, kardamom_types::TxOrderingMessage), ExecutorError> {
        self.0.next()
    }
}

struct BoxedDepSub(Box<dyn DepositSubscription>);
impl DepositSubscription for BoxedDepSub {
    fn next(&mut self) -> Result<(BPosition, kardamom_types::Deposit), ExecutorError> {
        self.0.next()
    }
}

// 8 args mirrors `Executor::run`'s shape (readers/exec/commit wiring); a config
// struct would just shuffle the same fields behind one name. See the note on
// `Executor::run`.
#[allow(clippy::too_many_arguments)]
fn spawn_exec<S, Q, P>(
    cfg: ExecutorConfig,
    rx: Receiver<ReaderToExec>,
    tx: Sender<ExecToCommit>,
    snapshots: S,
    mut sw_signal: Q,
    mut sw_queue: P,
    initial_block: u64,
    resume: Option<ResumePoint>,
    bal_tx: Option<Sender<BalHandoff>>,
    block_exec: Option<BlockExec<S::Db>>,
) -> JoinHandle<Result<(), ExecutorError>>
where
    S: SnapshotSource + 'static,
    Q: StateWriterSignal + 'static,
    P: StateWriterQueue + 'static,
{
    thread::Builder::new()
        .name("executor-exec".into())
        .spawn(move || -> Result<(), ExecutorError> {
            // Resume-from-cursor: the canonical stream source delivers from the
            // persisted cursor onward (the cluster client's REPLAY_FROM; below-
            // cursor records are deduped in reader::cluster), so the exec
            // thread seeds its absolute counters from [`ResumePoint`] instead
            // of replaying from record 0 and skip-counting. On a fresh start
            // (`resume == None`) everything seeds at genesis values.
            let resume_count = resume.map(|r| r.record_count).unwrap_or(0);

            // The snapshot source hands back owned snapshots keyed by block
            // number: the block *just committed* — `initial_block` (== the
            // persisted `resume.block` on a resume, 0 on a fresh start).
            let mut snapshot = snapshots.snapshot_after(initial_block);
            let mut delta = PendingDelta::new();
            // Pipelined commit (depth K): at each boundary the finalized
            // delta is SUBMITTED to the writer but not awaited — the next
            // block executes against snapshot ∘ merged-unsettled ∘ delta,
            // completed commits settle opportunistically (non-blocking
            // probe) at each boundary, and the exec thread only ever parks
            // when the writer is a FULL K blocks behind. One fsync slower
            // than a block interval therefore no longer touches execution
            // at all (with depth 1 it still did: blocking wait_committed was
            // the receipt tail's dominant source — commit p50 ~25ms even for
            // empty blocks, p99 ~100ms, worst 1-2.5s). Durability semantics
            // unchanged: a BOUNDARY still reaches tx_receipts only after its
            // block is durable, and receipts already streamed pre-durability
            // (AT-LEAST-ONCE, re-published byte-identical on crash replay).
            //
            // `parent` is the MERGED union of every unsettled block's writes
            // (later blocks win) — one layer regardless of depth, so per-tx
            // cache seeding stays O(one map); it is rebuilt from the
            // survivors when commits settle.
            // EIP-7928 capture: per-block Bal, reset at each boundary; only
            // maintained when a publisher is attached (executor role).
            let mut block_bal = revm::state::bal::Bal::new();
            // Whole-block buffer, used only when a block-exec strategy is
            // supplied (validator parallel path).
            let mut buffered: Vec<BufferedRecord> = Vec::new();
            let mut parent: Option<PendingDelta> = None;
            let mut inflight: std::collections::VecDeque<(BlockBoundary, PendingDelta)> =
                std::collections::VecDeque::new();
            // Matches kardamom_state::geometry::HORIZON_BLOCKS — the writer's
            // own bounded queue depth; a deeper exec pipeline would just
            // block in `submit` instead.
            const COMMIT_PIPELINE_DEPTH: usize = 4;
            // Per-block receipts in arrival order, drained into the
            // BlockDelta at each boundary so the writer persists them
            // (receipts + tx_hash_index tables; #109). The clone per tx is
            // the price of feeding both this and the streaming tx_receipts
            // publisher — flagged for saturation validation.
            let mut block_receipts: Vec<kardamom_types::Receipt> = Vec::new();
            // Block-number bookkeeping. We treat blocks 1-indexed (genesis
            // is block 0). The exec thread assumes every block boundary it
            // sees is for the *current* in-flight block; it doesn't try to
            // re-derive block numbers without sealer help.
            let mut current_block = initial_block + 1;
            // Block N's txs execute with boundary N-1's timestamp; on resume
            // that boundary was consumed before the restart, so its persisted
            // value seeds the state (see `ResumePoint::l2_timestamp`).
            let mut current_l2_ts: u64 = resume.map(|r| r.l2_timestamp).unwrap_or(0);
            // Per-block RPC enrichment counters; reset at each BoundaryStart.
            let mut tx_index_in_block: u64 = 0;
            let mut cumulative_gas_used: u64 = 0;
            // Cumulative count of canonical records (TxRef + DepositRef) this
            // exec thread has folded into a receipt. This is the boundary
            // alignment key: `BlockBoundaryStart.end_tx_idx` carries the
            // sealer's cumulative count of republished canonical records
            // (encoded via `BPosition::from_index`), and at each boundary the
            // two counts MUST match. `expected_tx_idx` already tracks exactly
            // this (it advances once per applied Tx/Deposit and never resets
            // across blocks), so we compare against it directly.
            //
            // (Count, not Aeron byte position: positions are per-publication
            // term spaces under the canonical-publisher MDC merge and ambiguous
            // between offer-return and frame-start frames — the old position
            // key broke under load for exactly that reason.)
            //
            // Seeded at the resume cursor: the boundary counts on the wire are
            // ABSOLUTE, and delivery resumes at the cursor, so the counter must
            // too (starting at 0 made every mid-chain resume die on its first
            // boundary with a misalignment equal to the cursor).
            let mut expected_tx_idx = TxIndex(resume_count);
            // Wall time spent executing the block's txs/deposits (excludes
            // channel idle time between txs), recorded when the BoundaryStart
            // closes the block. `None` for empty blocks.
            let mut block_apply_elapsed: Option<Duration> = None;
            // Pre-resolved counter handles: this loop is the executor's
            // hottest path, so skip the per-event registry lookup.
            let tx_applied_ok =
                metrics::counter!(crate::metrics::TX_APPLIED_TOTAL, "outcome" => "ok");
            let tx_applied_error =
                metrics::counter!(crate::metrics::TX_APPLIED_TOTAL, "outcome" => "error");

            loop {
                let msg = match rx.recv() {
                    Ok(m) => m,
                    Err(_) => {
                        // Clean end of stream: settle every in-flight commit
                        // so the final boundaries aren't silently dropped.
                        if let Some((last, _)) = inflight.back() {
                            let last_n = last.block_number;
                            if sw_signal.wait_committed(last_n).is_ok() {
                                for (b, _) in inflight.drain(..) {
                                    let _ = tx.send(ExecToCommit::Boundary(b));
                                }
                            }
                        }
                        return Ok(());
                    }
                };
                match msg {
                    ReaderToExec::Tx {
                        tx_idx,
                        envelope,
                        position,
                    } => {
                        if tx_idx != expected_tx_idx {
                            tracing::error!(block = current_block, ?position, ?tx_idx, ?expected_tx_idx, "exec ERROR: OutOfOrderTx (Tx)");
                            return Err(ExecutorError::OutOfOrderTx {
                                got: tx_idx,
                                expected: expected_tx_idx,
                            });
                        }
                        expected_tx_idx = expected_tx_idx.next();
                        if block_exec.is_some() {
                            // Whole-block strategy: defer to the boundary so
                            // batches can execute concurrently.
                            buffered.push(BufferedRecord::Tx {
                                tx_idx,
                                envelope,
                                position,
                            });
                            continue;
                        }
                        let env = ExecEnv {
                            chain_id: cfg.chain_id,
                            block_number: current_block,
                            l2_timestamp: current_l2_ts,
                        };
                        let apply_start = Instant::now();
                        let result = execute_tx(
                            &snapshot,
                            parent.as_ref(),
                            &delta,
                            env,
                            tx_idx,
                            position,
                            &envelope,
                            tx_index_in_block,
                            cumulative_gas_used,
                            bal_tx
                                .as_ref()
                                .map(|_| (&mut block_bal, tx_index_in_block + 1)),
                        );
                        if result.is_ok() {
                            tx_applied_ok.increment(1);
                        } else {
                            tx_applied_error.increment(1);
                        }
                        if let Err(ref e) = result {
                            tracing::error!(block = current_block, ?position, error = ?e, "exec ERROR: execute_tx failed");
                        }
                        let (receipt, ws) = result?;
                        if bal_tx.is_some() && tx_index_in_block.is_multiple_of(512) {
                            tracing::debug!(
                                block = current_block,
                                tx_index_in_block,
                                bal_accounts = block_bal.accounts.len(),
                                ws_accounts = ws.accounts.len(),
                                "BAL capture progress"
                            );
                        }
                        cumulative_gas_used = receipt.cumulative_gas_used;
                        tx_index_in_block += 1;
                        delta.apply(ws);
                        *block_apply_elapsed.get_or_insert(Duration::ZERO) += apply_start.elapsed();
                        block_receipts.push(receipt.clone());
                        if tx.send(ExecToCommit::Receipt(receipt)).is_err() {
                            return Ok(());
                        }
                    }
                    ReaderToExec::Deposit {
                        tx_idx,
                        deposit,
                        position,
                    } => {
                        if tx_idx != expected_tx_idx {
                            tracing::error!(block = current_block, ?position, ?tx_idx, ?expected_tx_idx, "exec ERROR: OutOfOrderTx (Deposit)");
                            return Err(ExecutorError::OutOfOrderTx {
                                got: tx_idx,
                                expected: expected_tx_idx,
                            });
                        }
                        expected_tx_idx = expected_tx_idx.next();
                        if block_exec.is_some() {
                            buffered.push(BufferedRecord::Deposit {
                                tx_idx,
                                deposit,
                                position,
                            });
                            continue;
                        }
                        let env = ExecEnv {
                            chain_id: cfg.chain_id,
                            block_number: current_block,
                            l2_timestamp: current_l2_ts,
                        };
                        let apply_start = Instant::now();
                        let result = execute_deposit_tx(
                            &snapshot,
                            parent.as_ref(),
                            &delta,
                            env,
                            tx_idx,
                            position,
                            &deposit,
                            tx_index_in_block,
                            cumulative_gas_used,
                            bal_tx
                                .as_ref()
                                .map(|_| (&mut block_bal, tx_index_in_block + 1)),
                        );
                        if result.is_ok() {
                            tx_applied_ok.increment(1);
                        } else {
                            tx_applied_error.increment(1);
                        }
                        if let Err(ref e) = result {
                            tracing::error!(block = current_block, ?position, error = ?e, "exec ERROR: execute_deposit_tx failed");
                        }
                        let (receipt, ws) = result?;
                        cumulative_gas_used = receipt.cumulative_gas_used;
                        tx_index_in_block += 1;
                        delta.apply(ws);
                        *block_apply_elapsed.get_or_insert(Duration::ZERO) += apply_start.elapsed();
                        block_receipts.push(receipt.clone());
                        if tx.send(ExecToCommit::Receipt(receipt)).is_err() {
                            return Ok(());
                        }
                    }
                    ReaderToExec::Boundary(BlockBoundaryStart {
                        block_number,
                        end_tx_idx,
                        l2_timestamp,
                    }) => {
                        // Settle sweep: pop every in-flight commit the writer
                        // has ALREADY durably finished (non-blocking probe).
                        // Only when the pipeline is at full depth K does the
                        // exec thread block for the OLDEST commit — the
                        // recorded histogram measures exactly that residual
                        // stall (the only wait the pipeline ever pays;
                        // sustained non-zero residuals mean the writer is K
                        // full block intervals behind and this back-pressure
                        // is correct).
                        let mut durable = sw_signal.committed()?;
                        if inflight.len() >= COMMIT_PIPELINE_DEPTH
                            && inflight
                                .front()
                                .is_some_and(|(b, _)| b.block_number > durable)
                        {
                            let oldest = inflight.front().map(|(b, _)| b.block_number).unwrap_or(0);
                            let commit_wait = Instant::now();
                            durable = sw_signal.wait_committed(oldest)?;
                            metrics::histogram!(crate::metrics::STATE_COMMIT_DURATION_SECONDS)
                                .record(commit_wait.elapsed().as_secs_f64());
                        }
                        let mut newest_settled = None;
                        while inflight
                            .front()
                            .is_some_and(|(b, _)| b.block_number <= durable)
                        {
                            let (b, _) = inflight.pop_front().expect("front checked");
                            // Boundary forwards only now — downstream never
                            // observes a boundary a crash could un-commit.
                            metrics::gauge!(crate::metrics::BLOCK_NUMBER)
                                .set(b.block_number as f64);
                            newest_settled = Some(b.block_number);
                            if tx.send(ExecToCommit::Boundary(b)).is_err() {
                                return Ok(());
                            }
                        }
                        if let Some(n) = newest_settled {
                            debug!(
                                target: "executor",
                                durable,
                                through_block = n,
                                unsettled = inflight.len(),
                                "pipelined commits settled; snapshot swapped"
                            );
                            snapshot = snapshots.snapshot_after(n);
                            // Rebuild the merged read layer from the still-
                            // unsettled survivors (a merged map cannot be
                            // subtracted from).
                            parent = inflight.iter().fold(None, |acc, (_, d)| match acc {
                                None => Some(d.clone()),
                                Some(mut m) => {
                                    m.merge_from(d);
                                    Some(m)
                                }
                            });
                        }
                        // Alignment: BlockBoundaryStart.end_tx_idx carries the
                        // sealer's cumulative COUNT of canonical records
                        // (TxRef + DepositRef) republished through the end of
                        // this block, encoded via BPosition::from_index. The
                        // executor must have applied exactly that many records
                        // — i.e. `expected_tx_idx` (which advances once per
                        // applied Tx/Deposit and never resets) must equal it.
                        // A mismatch means the executor's view of the canonical
                        // stream diverged from the sealer's (a lost / extra /
                        // reordered record) — fatal: return so the process
                        // crash-loops rather than committing a wrong block.
                        let want = end_tx_idx.as_index();
                        let have = expected_tx_idx.0;
                        if want != have {
                            tracing::error!(
                                block = block_number,
                                want_count = want,
                                have_count = have,
                                "exec ERROR: BoundaryMisaligned (canonical record count)"
                            );
                            return Err(ExecutorError::BoundaryMisaligned {
                                end: end_tx_idx,
                                last_seen: BPosition::from_index(have),
                            });
                        }

                        // Whole-block strategy: execute everything buffered
                        // for this block now (the validator's batches run
                        // concurrently inside), then feed its receipts and
                        // delta into the SAME boundary path the streaming
                        // executor uses — commit ordering, durability gating
                        // and the write-set cross-check are unchanged.
                        if let Some(exec_block) = block_exec.as_ref() {
                            let env = ExecEnv {
                                chain_id: cfg.chain_id,
                                block_number,
                                l2_timestamp: current_l2_ts,
                            };
                            let apply_start = Instant::now();
                            let out = exec_block(&snapshot, &buffered, env, block_number)?;
                            *block_apply_elapsed.get_or_insert(Duration::ZERO) +=
                                apply_start.elapsed();
                            buffered.clear();
                            delta = out.delta;
                            for r in out.receipts {
                                block_receipts.push(r.clone());
                                if tx.send(ExecToCommit::Receipt(r)).is_err() {
                                    return Ok(());
                                }
                            }
                        }

                        // Record the block's accumulated execution time.
                        // Only recorded when the block had at least one tx;
                        // empty blocks are skipped.
                        if let Some(elapsed) = block_apply_elapsed.take() {
                            metrics::histogram!(crate::metrics::BLOCK_APPLY_DURATION_SECONDS)
                                .record(elapsed.as_secs_f64());
                        }

                        // S0: NO state-root computation. The sealed
                        // BlockBoundary on tx_receipts is slim — three
                        // fields, no commitment.
                        let boundary = BlockBoundary {
                            block_number,
                            end_tx_idx,
                            l2_timestamp,
                        };

                        // Drain the delta. We swap it out so the writer owns
                        // it — but RETAIN a clone as the next block's parent
                        // read layer (pipelined commit: the clone of the
                        // block's write maps costs a few ms; the fsync it
                        // takes off the critical path costs 25ms-2.5s). The
                        // block's receipts ride INSIDE the BlockDelta
                        // (arrival order) so the writer persists them durably
                        // (#109); they also streamed out on tx_receipts at
                        // execute time, above, ahead of durability — a crash
                        // in that window re-executes the block on recovery
                        // and re-publishes byte-identical receipts, so
                        // tx_receipts is AT-LEAST-ONCE and every consumer
                        // must dedup on `tx_idx` (ingress does).
                        let pending = std::mem::take(&mut delta);
                        // EIP-7928 handoff: move the block's Bal + a
                        // receipts-free copy of the merged delta to the
                        // publisher thread (encode + reliable delivery live
                        // entirely off this thread). A dropped send means
                        // the publisher is gone mid-shutdown — not fatal.
                        if let Some(btx) = bal_tx.as_ref() {
                            let bal_delta = pending.clone().finalize(block_number, Vec::new());
                            let _ = btx.send((
                                boundary.clone(),
                                bal_delta,
                                std::mem::take(&mut block_bal),
                            ));
                        }
                        match parent.as_mut() {
                            Some(m) => m.merge_from(&pending),
                            None => parent = Some(pending.clone()),
                        }
                        inflight.push_back((boundary.clone(), pending.clone()));
                        let bd: BlockDelta =
                            pending.finalize(block_number, std::mem::take(&mut block_receipts));

                        // Submit WITHOUT waiting: the commit settles at a
                        // later boundary's sweep (or at end of stream).
                        sw_queue.submit(boundary, bd)?;
                        current_block = block_number + 1;
                        // New block opens with empty per-block counters.
                        tx_index_in_block = 0;
                        cumulative_gas_used = 0;
                        // The next block's wall-clock timestamp arrives in
                        // its own BlockBoundaryStart; until then we keep
                        // the previous value as a deterministic
                        // placeholder for any txs that race ahead of the
                        // sealer (in v0 the sealer is single-leader so
                        // this branch is purely defensive).
                        current_l2_ts = l2_timestamp;
                    }
                }
            }
        })
        .expect("spawn exec")
}

/// Max receipts per published batch. Bounds the wire frame (receipts with
/// logs vary in size; 64 keeps worst-case frames well inside the term-buffer
/// message limit) — NOT a latency knob: the batch is whatever accumulated in
/// the exec→commit channel while the previous publish was in flight, so at
/// low rates batches are size 1 and nothing waits.
const RECEIPT_BATCH_MAX: usize = 64;

fn spawn_commit<C>(
    mut c_pub: C,
    rx: Receiver<ExecToCommit>,
) -> JoinHandle<Result<(), ExecutorError>>
where
    C: TxReceiptsPublication + 'static,
{
    thread::Builder::new()
        .name("executor-commit".into())
        .spawn(move || -> Result<(), ExecutorError> {
            loop {
                // Block for the next message, then opportunistically drain
                // whatever else is already queued into ONE receipt batch
                // (adaptive batching: batch size ≈ arrivals during the
                // previous publish — 1 at low rate, larger under load, no
                // added latency in either regime). A boundary flushes the
                // receipts gathered so far first, preserving stream order.
                let mut receipts: Vec<Receipt> = Vec::new();
                let mut boundary = None;
                match rx.recv() {
                    Ok(ExecToCommit::Receipt(r)) => receipts.push(r),
                    Ok(ExecToCommit::Boundary(b)) => boundary = Some(b),
                    Err(_) => return Ok(()),
                }
                let mut closed = false;
                while boundary.is_none() && receipts.len() < RECEIPT_BATCH_MAX {
                    match rx.try_recv() {
                        Ok(ExecToCommit::Receipt(r)) => receipts.push(r),
                        Ok(ExecToCommit::Boundary(b)) => boundary = Some(b),
                        Err(crossbeam_channel::TryRecvError::Empty) => break,
                        Err(crossbeam_channel::TryRecvError::Disconnected) => {
                            closed = true;
                            break;
                        }
                    }
                }

                // tx_receipts is MUST-DELIVER: a transaction's receipt has to make
                // it back to the ingress that is parking the client. A transient
                // publish failure — NOT_CONNECTED while the ingress's subscription
                // is still forming during multi-host bring-up — must NOT drop the
                // receipt or kill this thread. Killing it would close the bounded
                // exec→commit channel, back-pressure the exec thread, stop
                // tx_ordering consumption, and freeze ALL state progress. So retry
                // until it lands, resuming at the unpublished SUFFIX of the batch
                // (re-publishing a delivered prefix is only harmless-duplicate on
                // the wire, but the validator's verifying sink consumes its buffer
                // per receipt — the suffix resume keeps its semantics identical to
                // the old one-publish-per-receipt loop). (Deploy order brings the
                // ingress up first, so this normally succeeds on the first
                // attempt.)
                let mut attempts: u32 = 0;
                let mut from = 0usize;
                while from < receipts.len() {
                    let (published, err) = c_pub.publish_receipts(&receipts[from..]);
                    from += published;
                    match err {
                        None => {}
                        // A PROVEN divergence from the sink (the validator's
                        // receipt cross-check) is fatal, not transient.
                        // Retrying would silently defeat the fail-stop: the
                        // first failing publish already consumed the buffered
                        // receipt, so a retry finds nothing, waits out the
                        // receipt window, lands in the "unverified" arm and
                        // the pipeline keeps committing past a proven
                        // mismatch. Propagate instead so `Executor::run`
                        // returns the error and the process halts.
                        Some(e) if matches!(e, ExecutorError::Divergence(_)) => return Err(e),
                        Some(e) => {
                            attempts += 1;
                            if attempts == 1 || attempts.is_multiple_of(20) {
                                warn!(error = %e, attempts, "tx_receipts publish failed; retrying (must-deliver)");
                            }
                            thread::sleep(Duration::from_millis(50));
                        }
                    }
                }
                if let Some(b) = boundary {
                    let mut attempts: u32 = 0;
                    while let Err(e) = c_pub.publish(CMessage::BlockBoundary(b.clone())) {
                        if matches!(e, ExecutorError::Divergence(_)) {
                            return Err(e);
                        }
                        attempts += 1;
                        if attempts == 1 || attempts.is_multiple_of(20) {
                            warn!(error = %e, attempts, "tx_receipts boundary publish failed; retrying");
                        }
                        thread::sleep(Duration::from_millis(50));
                    }
                }
                if closed {
                    return Ok(());
                }
            }
        })
        .expect("spawn commit")
}

#[cfg(test)]
mod exec_tests {
    use super::*;
    use crate::exec_types::TxIndex;
    use crate::reader::ReaderToExec;
    use crate::state::{MockStateDatabase, StaticSnapshotSource};
    use alloy_consensus::{SignableTransaction, TxLegacy};
    use alloy_eips::eip2718::Encodable2718;
    use alloy_network::TxSignerSync;
    use alloy_primitives::{
        Address, Bytes as AlloyBytes, TxKind as APTxKind, U256, address, keccak256,
    };
    use alloy_signer_local::PrivateKeySigner;
    use bytes::Bytes;
    use kardamom_types::{BPosition, TxEnvelope as KtTxEnvelope};
    use revm::primitives::KECCAK_EMPTY;
    use std::sync::{Arc, Mutex};

    fn pos(off: i32) -> BPosition {
        BPosition {
            term_id: 0,
            term_offset: off,
        }
    }

    fn legacy(signer: &PrivateKeySigner, to: Address, nonce: u64, value: u64) -> KtTxEnvelope {
        let mut tx = TxLegacy {
            chain_id: Some(1),
            nonce,
            gas_price: 0,
            gas_limit: 21_000,
            to: APTxKind::Call(to),
            value: U256::from(value),
            input: AlloyBytes::new(),
        };
        let sig = signer.sign_transaction_sync(&mut tx).unwrap();
        let alloy_env: alloy_consensus::TxEnvelope = tx.into_signed(sig).into();
        let raw_tx = Bytes::from(alloy_env.encoded_2718());
        let tx_hash = keccak256(&raw_tx);
        KtTxEnvelope {
            correlation_id: 0,
            raw_tx,
            sender: signer.address(),
            tx_hash,
        }
    }

    struct ImmediateCommit;
    impl StateWriterSignal for ImmediateCommit {
        fn wait_committed(&mut self, at_least: u64) -> Result<u64, ExecutorError> {
            Ok(at_least)
        }
        fn committed(&mut self) -> Result<u64, ExecutorError> {
            Ok(u64::MAX)
        }
    }

    /// Writer signal whose durable level is test-controlled: `committed()`
    /// reads it; `wait_committed` (the depth-cap block) jumps it to the
    /// target and counts the call.
    struct StagedCommit {
        durable: Arc<std::sync::atomic::AtomicU64>,
        blocking_waits: Arc<std::sync::atomic::AtomicU64>,
    }
    impl StateWriterSignal for StagedCommit {
        fn wait_committed(&mut self, at_least: u64) -> Result<u64, ExecutorError> {
            self.blocking_waits
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            self.durable
                .fetch_max(at_least, std::sync::atomic::Ordering::SeqCst);
            Ok(self.durable.load(std::sync::atomic::Ordering::SeqCst))
        }
        fn committed(&mut self) -> Result<u64, ExecutorError> {
            Ok(self.durable.load(std::sync::atomic::Ordering::SeqCst))
        }
    }
    struct RecordingQueue(Arc<Mutex<Vec<(BlockBoundary, BlockDelta)>>>);
    impl StateWriterQueue for RecordingQueue {
        fn submit(&mut self, b: BlockBoundary, d: BlockDelta) -> Result<(), ExecutorError> {
            self.0.lock().unwrap().push((b, d));
            Ok(())
        }
    }

    #[test]
    fn exec_runs_two_txs_and_emits_slim_boundary() {
        let signer = PrivateKeySigner::random();
        let from = signer.address();
        let to = address!("00000000000000000000000000000000000ABCDE");

        let snap = MockStateDatabase::builder()
            .account(from, U256::from(10u128.pow(18)), 0, KECCAK_EMPTY)
            .build();
        let writer_log = Arc::new(Mutex::new(Vec::new()));

        let (tx_r2e, rx_r2e) = bounded::<ReaderToExec>(8);
        let (tx_e2c, rx_e2c) = bounded::<ExecToCommit>(8);

        tx_r2e
            .send(ReaderToExec::Tx {
                tx_idx: TxIndex(0),
                envelope: legacy(&signer, to, 0, 100),
                position: pos(0),
            })
            .unwrap();
        tx_r2e
            .send(ReaderToExec::Tx {
                tx_idx: TxIndex(1),
                envelope: legacy(&signer, to, 1, 50),
                position: pos(1),
            })
            .unwrap();
        tx_r2e
            // Two canonical records applied ⇒ cumulative count 2. end_tx_idx
            // encodes that count (pos(2) == BPosition::from_index(2)).
            .send(ReaderToExec::Boundary(BlockBoundaryStart {
                block_number: 1,
                end_tx_idx: pos(2),
                l2_timestamp: 1_700_000_000,
            }))
            .unwrap();
        drop(tx_r2e);

        let cfg = ExecutorConfig::default();
        let h = spawn_exec(
            cfg,
            rx_r2e,
            tx_e2c,
            StaticSnapshotSource(snap),
            ImmediateCommit,
            RecordingQueue(writer_log.clone()),
            0,
            None,
            None,
            None,
        );
        h.join().expect("no panic").expect("exec ok");
        drop(rx_e2c);

        let log = writer_log.lock().unwrap();
        assert_eq!(log.len(), 1);
        let (boundary, delta) = &log[0];
        assert_eq!(boundary.block_number, 1);
        assert_eq!(boundary.end_tx_idx, pos(2));
        assert_eq!(boundary.l2_timestamp, 1_700_000_000);
        // The recipient received 150 total across both transfers — verify
        // by iterating the canonical Vec<AccountChange> the wire form holds.
        let to_acc = delta
            .accounts
            .iter()
            .find(|a| a.address == to)
            .expect("recipient");
        assert_eq!(to_acc.balance, U256::from(150u64));
        // #109: the block's receipts ride inside the BlockDelta, in arrival
        // order, so the writer persists them (receipts + tx_hash_index) and
        // eth_getTransactionReceipt answers from durable state post-restart.
        assert_eq!(delta.receipts.len(), 2, "both txs' receipts persisted");
        assert!(delta.receipts.iter().all(|r| r.block_number == 1));
        assert_eq!(delta.receipts[0].nonce, 0);
        assert_eq!(delta.receipts[1].nonce, 1);
        // S0 regression guard: destructure to enforce the 3-field
        // shape of BlockBoundary at compile time.
        let BlockBoundary {
            block_number: _,
            end_tx_idx: _,
            l2_timestamp: _,
        } = boundary;
    }

    /// Pipelined commit: block N+1 executes against snapshot ∘ parent(N)
    /// while N's commit is unsettled, and boundaries still forward in order
    /// after their block is durable. The StaticSnapshotSource NEVER advances
    /// — cross-block visibility can only come through the parent layer, so a
    /// broken layer makes the block-2 spend fail (status 0) and this test
    /// red.
    #[test]
    fn exec_pipelines_commit_and_next_block_reads_parent_layer() {
        let signer_a = PrivateKeySigner::random();
        let signer_b = PrivateKeySigner::random();
        let a = signer_a.address();
        let b = signer_b.address();
        let c = address!("00000000000000000000000000000000000ABCDE");

        // Only A is funded at genesis; B's balance exists ONLY in block 1's
        // delta until that block's commit lands (which the static source
        // never exposes).
        let snap = MockStateDatabase::builder()
            .account(a, U256::from(10u128.pow(18)), 0, KECCAK_EMPTY)
            .build();
        let writer_log = Arc::new(Mutex::new(Vec::new()));

        let (tx_r2e, rx_r2e) = bounded::<ReaderToExec>(8);
        let (tx_e2c, rx_e2c) = bounded::<ExecToCommit>(8);

        // Block 1: A → B, a fat transfer so B can pay gas in block 2.
        tx_r2e
            .send(ReaderToExec::Tx {
                tx_idx: TxIndex(0),
                envelope: legacy(&signer_a, b, 0, 100_000_000_000_000_000),
                position: pos(0),
            })
            .unwrap();
        tx_r2e
            .send(ReaderToExec::Boundary(BlockBoundaryStart {
                block_number: 1,
                end_tx_idx: pos(1),
                l2_timestamp: 1_700_000_000,
            }))
            .unwrap();
        // Block 2: B → C spends funds B only has via block 1's writes.
        tx_r2e
            .send(ReaderToExec::Tx {
                tx_idx: TxIndex(1),
                envelope: legacy(&signer_b, c, 0, 60),
                position: pos(1),
            })
            .unwrap();
        tx_r2e
            .send(ReaderToExec::Boundary(BlockBoundaryStart {
                block_number: 2,
                end_tx_idx: pos(2),
                l2_timestamp: 1_700_000_002,
            }))
            .unwrap();
        drop(tx_r2e);

        let cfg = ExecutorConfig::default();
        let h = spawn_exec(
            cfg,
            rx_r2e,
            tx_e2c,
            StaticSnapshotSource(snap),
            ImmediateCommit,
            RecordingQueue(writer_log.clone()),
            0,
            None,
            None,
            None,
        );
        h.join().expect("no panic").expect("exec ok");

        // e2c ordering proves the pipeline shape: block 2's receipt streams
        // BEFORE boundary 1 forwards (boundary 1 settles at boundary 2's
        // entry; boundary 2 settles at end of stream).
        let mut kinds = Vec::new();
        while let Ok(m) = rx_e2c.try_recv() {
            kinds.push(match m {
                ExecToCommit::Receipt(r) => {
                    assert!(r.status, "every tx must succeed (parent layer visible)");
                    format!("R{}", r.block_number)
                }
                ExecToCommit::Boundary(bd) => format!("B{}", bd.block_number),
            });
        }
        assert_eq!(
            kinds,
            vec!["R1", "R2", "B1", "B2"],
            "receipts stream ahead; boundaries settle in order at the next boundary / stream end"
        );

        // Block 2's delta must show the spend: C received 60.
        let log = writer_log.lock().unwrap();
        assert_eq!(log.len(), 2);
        let (_, d2) = &log[1];
        let c_acc = d2
            .accounts
            .iter()
            .find(|x| x.address == c)
            .expect("C credited in block 2");
        assert_eq!(c_acc.balance, U256::from(60u64));
    }

    /// Depth-K pipelining: under a writer that never advances on its own,
    /// execution runs K=4 blocks ahead without blocking (boundaries
    /// withheld), the 5th boundary blocks for exactly the OLDEST commit,
    /// and settles forward in order. End of stream drains the rest.
    #[test]
    fn exec_pipelines_k_deep_and_blocks_only_at_capacity() {
        let snap = MockStateDatabase::builder().build();
        let writer_log = Arc::new(Mutex::new(Vec::new()));
        let durable = Arc::new(std::sync::atomic::AtomicU64::new(0));
        let blocking_waits = Arc::new(std::sync::atomic::AtomicU64::new(0));

        let (tx_r2e, rx_r2e) = bounded::<ReaderToExec>(16);
        let (tx_e2c, rx_e2c) = bounded::<ExecToCommit>(16);

        for n in 1..=6u64 {
            tx_r2e
                .send(ReaderToExec::Boundary(BlockBoundaryStart {
                    block_number: n,
                    end_tx_idx: pos(0),
                    l2_timestamp: 1_700_000_000 + n,
                }))
                .unwrap();
        }
        drop(tx_r2e);

        let h = spawn_exec(
            ExecutorConfig::default(),
            rx_r2e,
            tx_e2c,
            StaticSnapshotSource(snap),
            StagedCommit {
                durable: durable.clone(),
                blocking_waits: blocking_waits.clone(),
            },
            RecordingQueue(writer_log.clone()),
            0,
            None,
            None,
            None,
        );
        h.join().expect("no panic").expect("exec ok");

        // Boundaries 1-4 pipeline without any blocking wait; boundaries 5
        // and 6 each block once for the oldest commit; end-of-stream drains
        // with one final wait. 2 capacity waits + 1 drain wait.
        assert_eq!(
            blocking_waits.load(std::sync::atomic::Ordering::SeqCst),
            3,
            "exec must block only at depth K and at end-of-stream"
        );
        let boundaries: Vec<u64> = std::iter::from_fn(|| rx_e2c.try_recv().ok())
            .map(|m| match m {
                ExecToCommit::Boundary(b) => b.block_number,
                ExecToCommit::Receipt(_) => panic!("no receipts in this scenario"),
            })
            .collect();
        assert_eq!(
            boundaries,
            vec![1, 2, 3, 4, 5, 6],
            "settled in order, none lost"
        );
        assert_eq!(writer_log.lock().unwrap().len(), 6, "all deltas submitted");
    }

    /// End-to-end through the ACTOR: with a BAL channel attached, the
    /// handoff at each boundary must carry a POPULATED Bal. Live phase-1
    /// measurement produced 1-byte (empty) BALs while deltas were 76KB —
    /// direct `execute_tx` tests passed, so the gap is in this wiring.
    #[test]
    fn exec_handoff_carries_a_populated_bal() {
        let signer = PrivateKeySigner::random();
        let from = signer.address();
        let to = address!("00000000000000000000000000000000000ABCDE");
        let snap = MockStateDatabase::builder()
            .account(from, U256::from(10u128.pow(18)), 0, KECCAK_EMPTY)
            .build();
        let writer_log = Arc::new(Mutex::new(Vec::new()));

        let (tx_r2e, rx_r2e) = bounded::<ReaderToExec>(8);
        let (tx_e2c, _rx_e2c) = bounded::<ExecToCommit>(64);
        let (bal_tx, bal_rx) = bounded::<BalHandoff>(8);

        tx_r2e
            .send(ReaderToExec::Tx {
                tx_idx: TxIndex(0),
                envelope: legacy(&signer, to, 0, 100),
                position: pos(0),
            })
            .unwrap();
        tx_r2e
            .send(ReaderToExec::Boundary(BlockBoundaryStart {
                block_number: 1,
                end_tx_idx: pos(1),
                l2_timestamp: 1_700_000_000,
            }))
            .unwrap();
        drop(tx_r2e);

        let h = spawn_exec(
            ExecutorConfig::default(),
            rx_r2e,
            tx_e2c,
            StaticSnapshotSource(snap),
            ImmediateCommit,
            RecordingQueue(writer_log.clone()),
            0,
            None,
            Some(bal_tx),
            None,
        );
        h.join().expect("no panic").expect("exec ok");

        let (_boundary, delta, bal) = bal_rx.try_recv().expect("a BAL handoff");
        assert!(
            !delta.accounts.is_empty(),
            "delta carries the block's writes"
        );
        let alloy = bal.into_alloy_bal();
        assert!(
            !alloy.is_empty(),
            "handoff Bal is EMPTY while the delta has {} accounts",
            delta.accounts.len()
        );
    }

    #[test]
    fn exec_rejects_misaligned_boundary() {
        let writer_log = Arc::new(Mutex::new(Vec::new()));

        let (tx_r2e, rx_r2e) = bounded::<ReaderToExec>(8);
        let (tx_e2c, _rx_e2c) = bounded::<ExecToCommit>(8);

        let signer = PrivateKeySigner::random();
        tx_r2e
            .send(ReaderToExec::Tx {
                tx_idx: TxIndex(0),
                envelope: legacy(&signer, Address::from([0x22u8; 20]), 0, 0),
                position: pos(0),
            })
            .unwrap();
        // Boundary claims 5 canonical records (pos(5) == from_index(5)) but we
        // only applied 1 ⇒ count mismatch ⇒ BoundaryMisaligned.
        tx_r2e
            .send(ReaderToExec::Boundary(BlockBoundaryStart {
                block_number: 1,
                end_tx_idx: pos(5),
                l2_timestamp: 0,
            }))
            .unwrap();
        drop(tx_r2e);

        // Pre-fund the signer so the tx doesn't fail before we hit the boundary.
        let snap = MockStateDatabase::builder()
            .account(
                signer.address(),
                U256::from(10u128.pow(18)),
                0,
                KECCAK_EMPTY,
            )
            .build();

        let cfg = ExecutorConfig::default();
        let h = spawn_exec(
            cfg,
            rx_r2e,
            tx_e2c,
            StaticSnapshotSource(snap),
            ImmediateCommit,
            RecordingQueue(writer_log),
            0,
            None,
            None,
            None,
        );
        let res = h.join().expect("no panic");
        assert!(matches!(res, Err(ExecutorError::BoundaryMisaligned { .. })));
    }

    // ---- Phase 2 recovery: skip-count replay -----------------------------
    //
    // On restart the exec thread is fed the canonical stream re-played from
    // record 0 and must skip everything already committed (see [`ResumePoint`]).
    // These tests drive the skip path deterministically with synthetic records
    // — no Aeron, no archive — and assert the executor neither re-commits a
    // replayed block nor re-emits a replayed receipt, while still executing
    // everything past the persisted cursor.

    /// Drain a closed `ExecToCommit` receiver, returning the block numbers of
    /// emitted receipts and boundaries (each in order).
    fn drain_commits(rx: Receiver<ExecToCommit>) -> (Vec<u64>, Vec<u64>) {
        let mut receipts = Vec::new();
        let mut boundaries = Vec::new();
        while let Ok(m) = rx.recv() {
            match m {
                ExecToCommit::Receipt(r) => receipts.push(r.block_number),
                ExecToCommit::Boundary(b) => boundaries.push(b.block_number),
            }
        }
        (receipts, boundaries)
    }

    #[test]
    fn resume_executes_from_cursor_with_absolute_counts() {
        // Pre-restart the executor committed block 1 (2 txs, record_count=2).
        // The canonical source (cluster REPLAY_FROM) delivers from the cursor:
        // only block 2's new tx + boundary arrive, with ABSOLUTE indices/counts
        // (tx_idx 2, boundary end count 3). The exec thread must seed its
        // counters from the ResumePoint — starting them at zero made exactly
        // this stream die BoundaryMisaligned on every mid-chain restart.
        let signer = PrivateKeySigner::random();
        let to = address!("00000000000000000000000000000000000ABCDE");
        // Snapshot represents post-block-1 state: signer nonce already at 2.
        let snap = MockStateDatabase::builder()
            .account(
                signer.address(),
                U256::from(10u128.pow(18)),
                2,
                KECCAK_EMPTY,
            )
            .build();
        let writer_log = Arc::new(Mutex::new(Vec::new()));

        let (tx_r2e, rx_r2e) = bounded::<ReaderToExec>(16);
        let (tx_e2c, rx_e2c) = bounded::<ExecToCommit>(16);

        // Post-cursor work only: block 2's tx + boundary, absolute keys.
        tx_r2e
            .send(ReaderToExec::Tx {
                tx_idx: TxIndex(2),
                envelope: legacy(&signer, to, 2, 10),
                position: pos(2),
            })
            .unwrap();
        tx_r2e
            .send(ReaderToExec::Boundary(BlockBoundaryStart {
                block_number: 2,
                end_tx_idx: pos(3),
                l2_timestamp: 1_700_000_001,
            }))
            .unwrap();
        drop(tx_r2e);

        let h = spawn_exec(
            ExecutorConfig::default(),
            rx_r2e,
            tx_e2c,
            StaticSnapshotSource(snap),
            ImmediateCommit,
            RecordingQueue(writer_log.clone()),
            // initial_block == resume.block (the bins pass the same cursor).
            1,
            Some(ResumePoint {
                block: 1,
                record_count: 2,
                l2_timestamp: 1_700_000_000,
            }),
            None,
            None,
        );
        h.join().expect("no panic").expect("exec ok");

        let (receipt_blocks, boundaries) = drain_commits(rx_e2c);
        // Block 2's single tx produced a receipt, attributed to block 2 (the
        // seeded current_block) — not block 1 (a zero-seeded counter's value).
        assert_eq!(
            receipt_blocks,
            vec![2],
            "the post-cursor tx receipts once, in block 2"
        );
        assert_eq!(boundaries, vec![2], "block 2 commits");
        let log = writer_log.lock().unwrap();
        assert_eq!(log.len(), 1, "only block 2 is submitted to the writer");
        assert_eq!(log[0].0.block_number, 2);
    }

    #[test]
    fn resume_after_empty_block_backlog() {
        // The sealer kept emitting empty blocks (1,2,3) while the executor was
        // down; record_count stayed 0. resume={block:3, count:0}: delivery
        // resumes at block 4, whose first real tx (absolute index 0) executes
        // and commits — attributed to block 4, not a restarted-from-1 counter.
        let signer = PrivateKeySigner::random();
        let to = address!("00000000000000000000000000000000000ABCDE");
        let snap = MockStateDatabase::builder()
            .account(
                signer.address(),
                U256::from(10u128.pow(18)),
                0,
                KECCAK_EMPTY,
            )
            .build();
        let writer_log = Arc::new(Mutex::new(Vec::new()));

        let (tx_r2e, rx_r2e) = bounded::<ReaderToExec>(16);
        let (tx_e2c, rx_e2c) = bounded::<ExecToCommit>(16);

        // Block 4: first real tx (count 0 -> 1).
        tx_r2e
            .send(ReaderToExec::Tx {
                tx_idx: TxIndex(0),
                envelope: legacy(&signer, to, 0, 10),
                position: pos(0),
            })
            .unwrap();
        tx_r2e
            .send(ReaderToExec::Boundary(BlockBoundaryStart {
                block_number: 4,
                end_tx_idx: pos(1),
                l2_timestamp: 1_700_000_004,
            }))
            .unwrap();
        drop(tx_r2e);

        let h = spawn_exec(
            ExecutorConfig::default(),
            rx_r2e,
            tx_e2c,
            StaticSnapshotSource(snap),
            ImmediateCommit,
            RecordingQueue(writer_log.clone()),
            3,
            Some(ResumePoint {
                block: 3,
                record_count: 0,
                l2_timestamp: 1_700_000_003,
            }),
            None,
            None,
        );
        h.join().expect("no panic").expect("exec ok");

        let (receipt_blocks, boundaries) = drain_commits(rx_e2c);
        assert_eq!(
            receipt_blocks,
            vec![4],
            "block 4's tx receipts once, in block 4"
        );
        assert_eq!(boundaries, vec![4]);
        let log = writer_log.lock().unwrap();
        assert_eq!(log.len(), 1);
        assert_eq!(log[0].0.block_number, 4);
    }

    #[test]
    fn resume_boundary_alignment_still_checked() {
        // Resume must not bypass the boundary-alignment invariant: a boundary
        // whose ABSOLUTE record count disagrees with the seeded-and-advanced
        // counter is still fatal (here: cursor 5, one tx seen ⇒ have 6, but
        // the boundary claims 10).
        let signer = PrivateKeySigner::random();
        let to = address!("00000000000000000000000000000000000ABCDE");
        let snap = MockStateDatabase::builder()
            .account(
                signer.address(),
                U256::from(10u128.pow(18)),
                0,
                KECCAK_EMPTY,
            )
            .build();
        let (tx_r2e, rx_r2e) = bounded::<ReaderToExec>(8);
        let (tx_e2c, _rx_e2c) = bounded::<ExecToCommit>(8);

        tx_r2e
            .send(ReaderToExec::Tx {
                tx_idx: TxIndex(5),
                envelope: legacy(&signer, to, 0, 10),
                position: pos(5),
            })
            .unwrap();
        tx_r2e
            .send(ReaderToExec::Boundary(BlockBoundaryStart {
                block_number: 2,
                end_tx_idx: pos(10),
                l2_timestamp: 1_700_000_000,
            }))
            .unwrap();
        drop(tx_r2e);

        let h = spawn_exec(
            ExecutorConfig::default(),
            rx_r2e,
            tx_e2c,
            StaticSnapshotSource(snap),
            ImmediateCommit,
            RecordingQueue(Arc::new(Mutex::new(Vec::new()))),
            1,
            Some(ResumePoint {
                block: 1,
                record_count: 5,
                l2_timestamp: 1_700_000_000,
            }),
            None,
            None,
        );
        let res = h.join().expect("no panic");
        assert!(matches!(res, Err(ExecutorError::BoundaryMisaligned { .. })));
    }

    #[test]
    fn no_resume_executes_and_commits_block_one() {
        // resume=None is the fresh-start path: block 1 executes and commits.
        let signer = PrivateKeySigner::random();
        let to = address!("00000000000000000000000000000000000ABCDE");
        let snap = MockStateDatabase::builder()
            .account(
                signer.address(),
                U256::from(10u128.pow(18)),
                0,
                KECCAK_EMPTY,
            )
            .build();
        let writer_log = Arc::new(Mutex::new(Vec::new()));
        let (tx_r2e, rx_r2e) = bounded::<ReaderToExec>(8);
        let (tx_e2c, rx_e2c) = bounded::<ExecToCommit>(8);

        tx_r2e
            .send(ReaderToExec::Tx {
                tx_idx: TxIndex(0),
                envelope: legacy(&signer, to, 0, 10),
                position: pos(0),
            })
            .unwrap();
        tx_r2e
            .send(ReaderToExec::Boundary(BlockBoundaryStart {
                block_number: 1,
                end_tx_idx: pos(1),
                l2_timestamp: 1_700_000_000,
            }))
            .unwrap();
        drop(tx_r2e);

        let h = spawn_exec(
            ExecutorConfig::default(),
            rx_r2e,
            tx_e2c,
            StaticSnapshotSource(snap),
            ImmediateCommit,
            RecordingQueue(writer_log.clone()),
            0,
            None,
            None,
            None,
        );
        h.join().expect("no panic").expect("exec ok");

        let (receipt_blocks, boundaries) = drain_commits(rx_e2c);
        assert_eq!(receipt_blocks, vec![1]);
        assert_eq!(boundaries, vec![1]);
        assert_eq!(writer_log.lock().unwrap().len(), 1);
    }
}

#[cfg(test)]
mod commit_tests {
    use super::*;
    use alloy_primitives::B256;
    use kardamom_types::{BPosition, BlockBoundary, Receipt};
    use std::sync::{Arc, Mutex};

    struct RecordPub(Arc<Mutex<Vec<CMessage>>>);
    impl TxReceiptsPublication for RecordPub {
        fn publish(&mut self, msg: CMessage) -> Result<(), ExecutorError> {
            self.0.lock().unwrap().push(msg);
            Ok(())
        }
    }

    #[test]
    fn commit_thread_preserves_order() {
        let (tx, rx) = bounded::<ExecToCommit>(8);
        let log = Arc::new(Mutex::new(Vec::new()));
        let pos0 = BPosition {
            term_id: 0,
            term_offset: 0,
        };

        tx.send(ExecToCommit::Receipt(Receipt {
            tx_idx: pos0,
            tx_hash: B256::repeat_byte(0xAA),
            status: true,
            gas_used: 21_000,
            logs: Vec::new(),
            write_set_hash: B256::ZERO,
            ..Default::default()
        }))
        .unwrap();
        tx.send(ExecToCommit::Boundary(BlockBoundary {
            block_number: 1,
            end_tx_idx: pos0,
            l2_timestamp: 100,
        }))
        .unwrap();
        drop(tx);

        let h = spawn_commit(RecordPub(log.clone()), rx);
        h.join().expect("no panic").expect("ok");

        let l = log.lock().unwrap();
        assert_eq!(l.len(), 2);
        assert!(matches!(&l[0], CMessage::Receipt(r) if r.tx_idx == pos0));
        assert!(matches!(&l[1], CMessage::BlockBoundary(b) if b.block_number == 1));
    }

    /// Rejects the first `fails_left` publish attempts (a transient
    /// NOT_CONNECTED while the ingress subscription is forming), then records.
    struct FlakyPub {
        fails_left: u32,
        log: Arc<Mutex<Vec<CMessage>>>,
    }
    impl TxReceiptsPublication for FlakyPub {
        fn publish(&mut self, msg: CMessage) -> Result<(), ExecutorError> {
            if self.fails_left > 0 {
                self.fails_left -= 1;
                return Err(ExecutorError::TxReceiptsClosed);
            }
            self.log.lock().unwrap().push(msg);
            Ok(())
        }
    }

    // tx_receipts is must-deliver: a transient publish failure (subscriber not
    // yet connected during multi-host bring-up) must neither drop the receipt
    // nor kill the commit thread — it must retry until the receipt lands.
    #[test]
    fn commit_thread_retries_until_delivered() {
        let (tx, rx) = bounded::<ExecToCommit>(8);
        let log = Arc::new(Mutex::new(Vec::new()));
        let pos0 = BPosition {
            term_id: 0,
            term_offset: 0,
        };
        tx.send(ExecToCommit::Receipt(Receipt {
            tx_idx: pos0,
            tx_hash: B256::repeat_byte(0xAB),
            status: true,
            gas_used: 21_000,
            logs: Vec::new(),
            write_set_hash: B256::ZERO,
            ..Default::default()
        }))
        .unwrap();
        drop(tx);

        // The publisher rejects the first 3 attempts, then accepts.
        let h = spawn_commit(
            FlakyPub {
                fails_left: 3,
                log: log.clone(),
            },
            rx,
        );
        // Must return Ok — the thread survived the transient failures.
        h.join()
            .expect("no panic")
            .expect("commit thread must not die on a transient publish failure");

        let l = log.lock().unwrap();
        assert_eq!(l.len(), 1, "the receipt must be delivered, not dropped");
        assert!(matches!(&l[0], CMessage::Receipt(r) if r.tx_idx == pos0));
    }

    fn receipt(tag: u8, offset: i32) -> Receipt {
        Receipt {
            tx_idx: BPosition {
                term_id: 0,
                term_offset: offset,
            },
            tx_hash: B256::repeat_byte(tag),
            status: true,
            gas_used: 21_000,
            logs: Vec::new(),
            write_set_hash: B256::ZERO,
            ..Default::default()
        }
    }

    /// Records each `publish_receipts` call's batch (the live transport's
    /// one-frame-per-batch shape) plus boundaries via `publish`.
    struct BatchRecordPub {
        batches: Arc<Mutex<Vec<Vec<Receipt>>>>,
        boundaries: Arc<Mutex<Vec<BlockBoundary>>>,
    }
    impl TxReceiptsPublication for BatchRecordPub {
        fn publish(&mut self, msg: CMessage) -> Result<(), ExecutorError> {
            match msg {
                CMessage::Receipt(r) => self.batches.lock().unwrap().push(vec![r]),
                CMessage::BlockBoundary(b) => self.boundaries.lock().unwrap().push(b),
            }
            Ok(())
        }
        fn publish_receipts(&mut self, receipts: &[Receipt]) -> (usize, Option<ExecutorError>) {
            self.batches.lock().unwrap().push(receipts.to_vec());
            (receipts.len(), None)
        }
    }

    // Queued receipts drain into ONE batch publish (adaptive batching), and a
    // boundary flushes the receipts gathered before it — order preserved.
    #[test]
    fn commit_thread_batches_queued_receipts_and_flushes_on_boundary() {
        let (tx, rx) = bounded::<ExecToCommit>(16);
        for i in 0..5 {
            tx.send(ExecToCommit::Receipt(receipt(i as u8, i * 64)))
                .unwrap();
        }
        tx.send(ExecToCommit::Boundary(BlockBoundary {
            block_number: 1,
            end_tx_idx: BPosition {
                term_id: 0,
                term_offset: 4 * 64,
            },
            l2_timestamp: 100,
        }))
        .unwrap();
        drop(tx);

        let batches = Arc::new(Mutex::new(Vec::new()));
        let boundaries = Arc::new(Mutex::new(Vec::new()));
        let h = spawn_commit(
            BatchRecordPub {
                batches: batches.clone(),
                boundaries: boundaries.clone(),
            },
            rx,
        );
        h.join().expect("no panic").expect("ok");

        let b = batches.lock().unwrap();
        assert_eq!(b.len(), 1, "already-queued receipts ride one batch");
        assert_eq!(b[0].len(), 5);
        let hashes: Vec<u8> = b[0].iter().map(|r| r.tx_hash.0[0]).collect();
        assert_eq!(hashes, vec![0, 1, 2, 3, 4], "in-batch order preserved");
        assert_eq!(
            boundaries.lock().unwrap().len(),
            1,
            "boundary after the flush"
        );
    }

    /// Publishes `accept` receipts of each batch then fails transiently once,
    /// recording everything accepted — exercises the suffix resume.
    struct PartialPub {
        accept: usize,
        fail_once: bool,
        delivered: Arc<Mutex<Vec<Receipt>>>,
    }
    impl TxReceiptsPublication for PartialPub {
        fn publish(&mut self, _msg: CMessage) -> Result<(), ExecutorError> {
            Ok(())
        }
        fn publish_receipts(&mut self, receipts: &[Receipt]) -> (usize, Option<ExecutorError>) {
            if self.fail_once {
                self.fail_once = false;
                let n = self.accept.min(receipts.len());
                self.delivered
                    .lock()
                    .unwrap()
                    .extend_from_slice(&receipts[..n]);
                return (n, Some(ExecutorError::TxReceiptsClosed));
            }
            self.delivered.lock().unwrap().extend_from_slice(receipts);
            (receipts.len(), None)
        }
    }

    // A partial batch failure resumes at the unpublished SUFFIX: every receipt
    // is delivered exactly once, in order.
    #[test]
    fn commit_thread_resumes_batch_at_failed_suffix() {
        let (tx, rx) = bounded::<ExecToCommit>(16);
        for i in 0..6 {
            tx.send(ExecToCommit::Receipt(receipt(i as u8, i * 64)))
                .unwrap();
        }
        drop(tx);

        let delivered = Arc::new(Mutex::new(Vec::new()));
        let h = spawn_commit(
            PartialPub {
                accept: 2,
                fail_once: true,
                delivered: delivered.clone(),
            },
            rx,
        );
        h.join().expect("no panic").expect("ok");

        let d = delivered.lock().unwrap();
        let hashes: Vec<u8> = d.iter().map(|r| r.tx_hash.0[0]).collect();
        assert_eq!(
            hashes,
            vec![0, 1, 2, 3, 4, 5],
            "each receipt delivered exactly once, in order, across the resume"
        );
    }

    /// A sink that reports a PROVEN divergence (the validator's receipt
    /// cross-check) on every publish.
    struct DivergingPub;
    impl TxReceiptsPublication for DivergingPub {
        fn publish(&mut self, _msg: CMessage) -> Result<(), ExecutorError> {
            Err(ExecutorError::Divergence("receipt mismatch at tx 0".into()))
        }
    }

    // F10.1 regression: the must-deliver retry must NOT spin on a proven
    // divergence — that would consume the fail-stop (retry → empty buffer →
    // "unverified" → pipeline keeps committing). A Divergence error has to
    // propagate out of the commit thread immediately.
    #[test]
    fn commit_thread_fail_stops_on_divergence() {
        let (tx, rx) = bounded::<ExecToCommit>(8);
        tx.send(ExecToCommit::Receipt(Receipt {
            tx_idx: BPosition {
                term_id: 0,
                term_offset: 0,
            },
            ..Default::default()
        }))
        .unwrap();
        drop(tx);

        let h = spawn_commit(DivergingPub, rx);
        let res = h.join().expect("no panic");
        assert!(
            matches!(res, Err(ExecutorError::Divergence(_))),
            "divergence must propagate, not be retried: {res:?}"
        );
    }
}

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
//!
//! Module layout: this file keeps the actor's wiring ([`Executor::run`]);
//! the pieces live in focused submodules — [`ports`] (outbound trait seams),
//! [`types`] (plain data types), `exec_thread` (the [`ExecState`] loop),
//! `exec_settle` (pipelined-commit settling), `commit_thread` (receipt
//! batching + must-deliver publish).
//!
//! [`ExecState`]: exec_thread::ExecState

use std::thread::JoinHandle;

use crossbeam_channel::{Sender, bounded};

use kardamom_types::SnapshotSource;

use crate::error::ExecutorError;
use crate::exec_types::TxIndex;
use crate::reader::{
    JoinBuffer, ReaderToExec, TxDataSubscription, TxOrderingSubscription, spawn_tx_data_reader,
    spawn_tx_ordering_reader,
};

mod commit_thread;
mod exec_settle;
mod exec_thread;
mod ports;
mod types;

#[cfg(test)]
mod commit_tests;
#[cfg(test)]
mod exec_pipeline_tests;
#[cfg(test)]
mod exec_resume_tests;
#[cfg(test)]
mod exec_tests;
#[cfg(test)]
mod test_support;

pub use ports::{StateWriterQueue, StateWriterSignal, TxReceiptsPublication};
pub use types::{
    BalHandoff, BlockExec, BlockExecOutput, BufferedRecord, ExecutorConfig, ResumePoint,
};

pub(crate) use commit_thread::spawn_commit;
pub(crate) use exec_thread::spawn_exec;
pub(crate) use types::ExecToCommit;

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
    // executor's run-once API; see the long-form note in [`crate::executor::execute_tx`]
    // for the same rationale applied to per-tx execution.
    pub fn run<C, S, Q, P>(
        cfg: ExecutorConfig,
        a_subs: Vec<Box<dyn TxDataSubscription>>,
        b_sub: Box<dyn TxOrderingSubscription>,
        c_pub: C,
        snapshots: S,
        sw_signal: Q,
        sw_queue: P,
        initial_block: u64,
        resume: Option<ResumePoint>,
        bal_tx: Option<Sender<BalHandoff>>,
        block_exec: Option<BlockExec<S::Db>>,
        recovery: Option<crate::reader::JoinRecoveryFactory>,
        // Role-specific epoch check. `None` on the executor (it trusts the
        // ordered stream); the validator passes a verifier that re-derives
        // each epoch from L1.
        epoch_observer: Option<Box<dyn crate::reader::EpochObserver>>,
        // The interop mirror of `epoch_observer`, invoked per RemoteEpoch
        // marker. `None` everywhere until the destination-validator
        // `RemoteEpochVerifier` lands.
        remote_epoch_observer: Option<Box<dyn crate::reader::RemoteEpochObserver>>,
    ) -> Result<(), ExecutorError>
    where
        C: TxReceiptsPublication + 'static,
        S: SnapshotSource + 'static,
        Q: StateWriterSignal + 'static,
        P: StateWriterQueue + 'static,
    {
        let buffer = JoinBuffer::new();
        let (tx_r2e, rx_r2e) = bounded::<ReaderToExec>(cfg.receipt_queue_depth);
        let (tx_e2c, rx_e2c) = bounded::<ExecToCommit>(cfg.receipt_queue_depth);

        // M tx_data reader threads, one per sequencer partition. Each
        // owns its subscription for the duration; we collect the join
        // handles to surface any error.
        let mut a_handles: Vec<JoinHandle<Result<(), ExecutorError>>> =
            Vec::with_capacity(a_subs.len());
        for a in a_subs {
            // The trait object's `next` already advertises sequencer_id.
            a_handles.push(spawn_tx_data_reader(a, buffer.clone()));
        }

        let b_handle = spawn_tx_ordering_reader(
            b_sub,
            buffer.clone(),
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
            epoch_observer,
            remote_epoch_observer,
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
        // `pipeline` was already checked above (Ok by here), so the result is
        // determined by the A reader joins.
        r_a
    }
}

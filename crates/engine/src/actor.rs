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
//! Module layout: this file keeps the actor's assembly ([`Executor::run`]);
//! the pieces live in focused submodules — [`wiring`] (the [`EngineWiring`]
//! port-type bundle + the grouped run inputs), [`ports`] (outbound trait
//! seams), [`types`] (plain data types), `exec_thread` (the [`ExecState`]
//! loop), `exec_settle` (pipelined-commit settling), `commit_thread`
//! (receipt batching + must-deliver publish).
//!
//! [`ExecState`]: exec_thread::ExecState

use std::thread::JoinHandle;

use crossbeam_channel::bounded;

use crate::error::ExecutorError;
use crate::exec_types::TxIndex;
use crate::reader::{JoinBuffer, ReaderToExec, spawn_tx_data_reader, spawn_tx_ordering_reader};

mod commit_thread;
mod exec_settle;
mod exec_thread;
mod ports;
mod types;
mod wiring;

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
pub use wiring::{EngineWiring, Inbound, Outbound, RoleHooks, SnapshotDb, Start};

pub(crate) use commit_thread::spawn_commit;
pub(crate) use exec_thread::spawn_exec;
pub(crate) use types::ExecToCommit;

/// Owns the M+3 threads (M tx_data readers, 1 tx_ordering reader, 1 exec,
/// 1 commit). `run` blocks until the tx_ordering subscription closes or an
/// error occurs.
pub struct Executor;

impl Executor {
    /// Spawn the readers, exec, commit threads and join them. Returns when
    /// tx_ordering closes cleanly or when any thread propagates a fatal
    /// error.
    ///
    /// The inputs arrive grouped by category — [`Inbound`] (what the reader
    /// threads consume), [`Outbound`] (the receipts publication + state-
    /// writer seams), [`Start`] (fresh start or resume cursor), and
    /// [`RoleHooks`] (optional role-specific behavior) — with every port
    /// type named by one [`EngineWiring`] impl. See [`wiring`] for the full
    /// design, including how a caller opts back into runtime dispatch.
    pub fn run<W: EngineWiring>(
        cfg: ExecutorConfig,
        inbound: Inbound<W>,
        outbound: Outbound<W>,
        start: Start,
        hooks: RoleHooks<W>,
    ) -> Result<(), ExecutorError> {
        let Inbound {
            tx_data,
            tx_ordering,
            join_recovery,
        } = inbound;
        let Outbound {
            tx_receipts,
            snapshots,
            writer_signal,
            writer_queue,
        } = outbound;
        let Start {
            initial_block,
            resume,
        } = start;
        let RoleHooks {
            bal_capture,
            block_exec,
            epoch_observer,
        } = hooks;

        let buffer = JoinBuffer::new();
        let (tx_r2e, rx_r2e) = bounded::<ReaderToExec>(cfg.receipt_queue_depth);
        let (tx_e2c, rx_e2c) = bounded::<ExecToCommit>(cfg.receipt_queue_depth);

        // M tx_data reader threads, one per sequencer partition. Each
        // owns its subscription for the duration; we collect the join
        // handles to surface any error.
        let mut tx_data_handles: Vec<JoinHandle<Result<(), ExecutorError>>> =
            Vec::with_capacity(tx_data.len());
        for sub in tx_data {
            // The subscription's `next` already advertises sequencer_id.
            tx_data_handles.push(spawn_tx_data_reader(sub, buffer.clone()));
        }

        let tx_ordering_handle = spawn_tx_ordering_reader(
            tx_ordering,
            buffer.clone(),
            cfg.reader.clone(),
            tx_r2e,
            // The canonical source delivers from the resume cursor; indices
            // assigned here are checked against ABSOLUTE boundary counts.
            TxIndex(resume.map(|r| r.record_count).unwrap_or(0)),
            join_recovery,
        );

        let exec = spawn_exec(
            cfg.clone(),
            rx_r2e,
            tx_e2c,
            snapshots,
            writer_signal,
            writer_queue,
            initial_block,
            resume,
            bal_capture,
            block_exec,
            epoch_observer,
        );
        let commit = spawn_commit(tx_receipts, rx_e2c);

        // Join the critical pipeline first: the tx_ordering reader (closes
        // when tx_ordering is exhausted), then exec, then commit.
        let r_ordering = tx_ordering_handle.join().expect("tx_ordering reader panic");
        let r_exec = exec.join().expect("exec panic");
        let r_commit = commit.join().expect("commit panic");

        // If ANY of the pipeline threads errored (e.g. a fatal
        // BoundaryMisaligned in exec), the executor can no longer make
        // progress. Return immediately so the process exits and the
        // orchestrator restarts it. We must NOT fall through to joining the
        // tx_data readers: those block in their Aeron `next()` until the
        // subscription closes, which only happens on process teardown — so
        // joining them while the process is still up would hang forever and
        // silently mask the pipeline error (the exact "frozen but alive"
        // failure this guards against). On the normal Ok path the
        // subscriptions have already closed (tx_ordering exhausted), so the
        // tx_data joins return promptly and we drain them for clean shutdown.
        r_ordering.and(r_exec).and(r_commit)?;
        let mut r_tx_data: Result<(), ExecutorError> = Ok(());
        for h in tx_data_handles {
            let res = h.join().expect("tx_data reader panic");
            if r_tx_data.is_ok() {
                r_tx_data = res;
            }
        }
        // The pipeline was already checked above (Ok by here), so the result
        // is determined by the tx_data reader joins.
        r_tx_data
    }
}

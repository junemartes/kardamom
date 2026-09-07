//! Executor actor: M tx_data reader threads, one tx_ordering reader thread,
//! one exec thread, and one commit thread.
//!
//! ## Inbound demux
//!
//! The inbound path splits into two parts:
//!
//! - Each of the M tx_data reader threads (one per sequencer partition)
//!   subscribes to its own tx_data stream. It reads full `TxEnvelope`
//!   records and inserts them into a shared join buffer, keyed by
//!   `(sequencer_id, tx_data_position)`.
//! - The one tx_ordering reader thread reads small `TxOrderingMessage`
//!   records (`TxRef | BoundaryStart`) in canonical order. For each
//!   `TxRef`, it looks up the buffer and sends `(b_position, TxEnvelope)`
//!   to the exec thread. For each `BoundaryStart`, it forwards the record
//!   unchanged.
//!
//! The exec thread, the commit thread, the state-snapshot swap protocol,
//! write-set hashing, and tx_receipts emission do not depend on this split.
//! The executor's external contract stays the same: it consumes
//! canonical-ordered transactions and boundaries, and produces ordered
//! receipts and slim boundaries on tx_receipts.
//!
//! See `reader.rs` for the join buffer and reader-thread code.
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
//! In production, each of the M+1 reader threads that talks to Aeron owns
//! its own `rusteron_client::Aeron` handle (`!Send + !Sync`) on a dedicated
//! OS thread. The threads coordinate through the `DashMap` join buffer and
//! crossbeam channels.
//!
//! Module layout: this file holds the actor's assembly ([`Executor::run`]).
//! The parts live in separate modules:
//!
//! - [`wiring`]: the [`EngineWiring`] port-type bundle and the grouped run
//!   inputs.
//! - [`ports`]: outbound trait seams.
//! - [`types`]: plain data types.
//! - `exec_thread`: the [`ExecState`] loop.
//! - `exec_settle`: pipelined-commit settling.
//! - `commit_thread`: receipt batching and must-deliver publish.
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
pub use wiring::{EngineWiring, Inbound, Outbound, RoleHooks, SnapshotDb};

pub(crate) use commit_thread::spawn_commit;
pub(crate) use exec_thread::spawn_exec;
pub(crate) use types::ExecToCommit;

/// Owns the M+3 threads: M tx_data readers, one tx_ordering reader, one exec
/// thread, and one commit thread. `run` blocks until the tx_ordering
/// subscription closes, or until an error occurs.
pub struct Executor;

impl Executor {
    /// Spawn the reader, exec, and commit threads, then join them.
    /// Returns when tx_ordering closes cleanly, or when any thread reports
    /// a fatal error.
    ///
    /// The inputs arrive grouped by category:
    /// - [`Inbound`]: what the reader threads consume.
    /// - [`Outbound`]: the receipts publication and the state-writer seams.
    /// - [`ResumePoint`]: the cursor execution starts from
    ///   ([`ResumePoint::GENESIS`] on a fresh chain).
    /// - [`RoleHooks`]: optional role-specific behavior.
    ///
    /// One [`EngineWiring`] impl names every port type. See [`wiring`] for
    /// the full design, including how a caller can opt back into runtime
    /// dispatch.
    pub fn run<W: EngineWiring>(
        cfg: ExecutorConfig,
        inbound: Inbound<W>,
        outbound: Outbound<W>,
        start: ResumePoint,
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
        let RoleHooks {
            bal_capture,
            footprint_shadow,
            block_exec,
            epoch_observer,
            remote_epoch_observer: _,
        } = hooks;

        let buffer = JoinBuffer::new();
        let (tx_r2e, rx_r2e) = bounded::<ReaderToExec>(cfg.receipt_queue_depth);
        let (tx_e2c, rx_e2c) = bounded::<ExecToCommit>(cfg.receipt_queue_depth);

        // M tx_data reader threads, one per sequencer partition. Each thread
        // owns its subscription for its full life. `next` already reports
        // the sequencer_id. The join handles below surface any error.
        let tx_data_handles: Vec<JoinHandle<Result<(), ExecutorError>>> = tx_data
            .into_iter()
            .map(|sub| spawn_tx_data_reader(sub, buffer.clone()))
            .collect();

        let tx_ordering_handle = spawn_tx_ordering_reader(
            tx_ordering,
            buffer.clone(),
            cfg.reader.clone(),
            tx_r2e,
            // The canonical source delivers records from the start cursor.
            // The reader checks indices assigned here against absolute
            // boundary counts.
            TxIndex(start.record_count),
            join_recovery,
        );

        let exec = spawn_exec(
            cfg.clone(),
            rx_r2e,
            tx_e2c,
            snapshots,
            writer_signal,
            writer_queue,
            start,
            bal_capture,
            footprint_shadow,
            block_exec,
            epoch_observer,
            remote_epoch_observer,
        );
        let commit = spawn_commit(tx_receipts, rx_e2c);

        // Join the critical pipeline first: the tx_ordering reader, then
        // exec, then commit. The reader closes when tx_ordering is
        // exhausted.
        let r_ordering = tx_ordering_handle.join().expect("tx_ordering reader panic");
        let r_exec = exec.join().expect("exec panic");
        let r_commit = commit.join().expect("commit panic");

        // If any pipeline thread reports an error (for example, a fatal
        // BoundaryMisaligned in exec), the executor cannot make more
        // progress. Return now, so the process exits and the orchestrator
        // restarts it.
        //
        // Do not join the tx_data readers here. Each one blocks in its
        // Aeron `next()` call until its subscription closes, and that only
        // happens on process teardown. Joining them while the process is
        // still running would hang forever and hide the pipeline error: the
        // process looks alive but makes no progress. On the normal Ok path,
        // the subscriptions are already closed (tx_ordering is exhausted),
        // so the tx_data joins return right away and drain cleanly.
        r_ordering.and(r_exec).and(r_commit)?;
        // The pipeline was Ok, so the result now depends on the tx_data
        // reader joins. `fold` reads the whole iterator, so every reader is
        // joined with no short-circuit. `and` keeps the first error.
        tx_data_handles
            .into_iter()
            .map(|h| h.join().expect("tx_data reader panic"))
            .fold(Ok(()), Result::and)
    }
}

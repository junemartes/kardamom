//! Executor actor: M `tx_data` reader threads, one `tx_ordering` reader thread,
//! one exec thread, and one commit thread.
//!
//! ## Inbound demux
//!
//! The inbound path splits into two parts:
//!
//! - The feed threads of the transaction source fill a shared buffer. The
//!   executor's source is M `tx_data` reader threads (one per lane). Each
//!   one reads full `TxEnvelope` records and inserts them into the join
//!   buffer, keyed by `(sequencer_id, tx_data_position)`. A consumer
//!   outside the executors reads the executor stream instead, keyed by
//!   canonical index (see `reader::ExecStreamSource`).
//! - The one `tx_ordering` reader thread reads small `TxOrderingMessage`
//!   records (`TxRef | BoundaryStart`) in canonical order. For each
//!   `TxRef`, it looks up the buffer and sends `(b_position, TxEnvelope)`
//!   to the exec thread. For each `BoundaryStart`, it forwards the record
//!   unchanged.
//!
//! The exec thread, the commit thread, the state-snapshot swap protocol,
//! write-set hashing, and `tx_receipts` emission do not depend on this split.
//! The executor's external contract stays the same: it consumes
//! canonical-ordered transactions and boundaries, and produces ordered
//! receipts and slim boundaries on `tx_receipts`.
//!
//! See the `reader` module for the join buffer and reader-thread code.
//!
//! Wiring:
//! ```text
//!   tx_data[0..M]    tx_ordering
//!        │                │
//!        ▼                ▼
//!   ┌─────────┐     ┌─────────────┐
//!   │M readers│──►  │tx_ordering  │──► exec ──► commit ──► tx_receipts
//!   │ (insert │join │reader       │
//!   │ buffer) │buf  │(lookup +    │
//!   │         │     │ forward)    │
//!   └─────────┘     └─────────────┘
//! ```
//!
//! In production, each of the M+1 reader threads that talks to Aeron owns
//! its own `rusteron_client::Aeron` handle (`!Send + !Sync`) on a dedicated
//! OS thread. The threads coordinate through the `DashMap` join buffer and
//! crossbeam channels.
//!
//! Module layout: this file holds the actor's assembly ([`Executor::new`],
//! [`Executor::spawn`], and [`Executor::run`]).
//! The parts live in separate modules:
//!
//! - [`wiring`]: the [`EngineWiring`] port-type bundle and the grouped run
//!   inputs.
//! - [`ports`]: outbound trait seams.
//! - [`types`]: plain data types.
//! - `tx_hook`: the [`TxHook`] seam around each tx record.
//! - `exec_state`: the [`ExecState`] struct and its constructor.
//! - `exec_block`: the per-block state, the execution mode, and the
//!   optional captures.
//! - `exec_thread`: the loop and [`ExecState::spawn`].
//! - `exec_records`, `exec_markers`, `exec_boundary`: the `ReaderToExec`
//!   arms.
//! - `exec_settle`: pipelined-commit settling.
//! - `commit_thread`: [`CommitLoop`], receipt batching and must-deliver
//!   publish.
//!
//! [`ExecState`]: exec_state::ExecState

use std::thread::JoinHandle;

use crossbeam_channel::{Receiver, bounded};

use crate::error::ExecutorError;
use crate::reader::{
    FeedHandle, ReaderToExec, SourceStart, TxOrderingInputs, TxOrderingReader, TxSource,
};

mod commit_thread;
mod exec_block;
mod exec_boundary;
mod exec_markers;
mod exec_records;
mod exec_settle;
mod exec_state;
mod exec_thread;
mod ports;
mod tx_hook;
mod types;
mod wiring;

#[cfg(test)]
mod commit_tests;
#[cfg(test)]
mod exec_pipeline_tests;
#[cfg(test)]
mod exec_refs_tests;
#[cfg(test)]
mod exec_resume_tests;
#[cfg(test)]
mod exec_tests;
/// The channel-backed [`fixtures::ChannelHarness`], and a re-export of
/// the signed-legacy-transaction fixture [`fixtures::LegacyTx`] from
/// `kardamom-test-support`. This crate's own `test_support::legacy`
/// builds on `LegacyTx`, so this module also compiles under plain
/// `cfg(test)`. `test-support` exposes it `pub`, so another crate's
/// integration tests can reach it instead of copying it. Off by
/// default, so a production build never links a signer.
#[cfg(any(test, feature = "test-support"))]
pub mod fixtures;
#[cfg(test)]
mod test_hooks;
#[cfg(test)]
pub(crate) mod test_support;

pub use ports::{Either, StateWriterQueue, StateWriterSignal, TxReceiptsPublication, publish_each};
pub use tx_hook::{NoTxHook, TxContext, TxHook, TxOutcome, VerifyRecordIdentity};
pub use types::{
    BalHandoff, BlockExecOutput, BlockExecStrategy, BufferedRecord, ExecutorConfig, NoBlockExec,
    ResumePoint,
};
pub use wiring::{EngineWiring, ExecPorts, Inbound, Outbound, RoleHooks, SnapshotDb};

pub(crate) use commit_thread::CommitLoop;
pub(crate) use exec_thread::{ExecHooks, ExecInputs, ExecState};
pub(crate) use types::ExecToCommit;

/// The executor actor: the config, the inbound subscriptions, the outbound
/// ports, the resume cursor, and the role hooks. [`Self::spawn`] starts the
/// M+3 threads (M `tx_data` readers, one `tx_ordering` reader, one exec
/// thread, and one commit thread) and returns their handles. [`Self::run`]
/// spawns them and blocks until the `tx_ordering` subscription closes, or
/// until an error occurs.
pub struct Executor<W: EngineWiring> {
    cfg: ExecutorConfig,
    inbound: Inbound<W>,
    outbound: Outbound<W>,
    start: ResumePoint,
    hooks: RoleHooks<W>,
}

impl<W: EngineWiring + 'static> Executor<W> {
    /// Gather the run inputs. They arrive grouped by category:
    /// - [`Inbound`]: what the reader threads consume.
    /// - [`Outbound`]: the receipts publication and the state-writer seams.
    /// - [`ResumePoint`]: the cursor execution starts from
    ///   ([`ResumePoint::GENESIS`] on a fresh chain).
    /// - [`RoleHooks`]: optional role-specific behavior.
    ///
    /// One [`EngineWiring`] impl names every port type. See [`wiring`] for
    /// the full design, including how a caller can opt back into runtime
    /// dispatch.
    pub fn new(
        cfg: ExecutorConfig,
        inbound: Inbound<W>,
        outbound: Outbound<W>,
        start: ResumePoint,
        hooks: RoleHooks<W>,
    ) -> Self {
        Self {
            cfg,
            inbound,
            outbound,
            start,
            hooks,
        }
    }

    /// Spawn the reader, exec, and commit threads, then join them.
    /// Returns when `tx_ordering` closes cleanly, or when any thread reports
    /// a fatal error.
    ///
    /// # Errors
    ///
    /// Returns `Err` when any of the reader, exec, or commit threads
    /// reports a fatal error (for example, a `BoundaryMisaligned` or a
    /// proven receipt divergence).
    pub fn run(self) -> Result<(), ExecutorError> {
        self.spawn().join()
    }

    /// Spawn the reader, exec, and commit threads and return their handles
    /// without joining. [`Threads::join`] is the safe join; a caller that
    /// joins the handles itself must join the pipeline before the `tx_data`
    /// readers (see [`Threads`]).
    pub fn spawn(self) -> Threads {
        let Self {
            cfg,
            inbound,
            outbound,
            start,
            hooks,
        } = self;
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
            remote_epoch_observer,
            tx_hook,
        } = hooks;

        let (feed_handles, tx_ordering_handle, rx_r2e) = inbound.spawn_readers(&cfg);
        let (tx_e2c, rx_e2c) = bounded::<ExecToCommit>(cfg.receipt_queue_depth.get());

        let exec = ExecState::<W>::spawn(ExecInputs {
            cfg,
            rx: rx_r2e,
            tx: tx_e2c,
            snapshots,
            sw_signal: writer_signal,
            sw_queue: writer_queue,
            start,
            hooks: ExecHooks {
                bal_tx: bal_capture,
                shadow_tx: footprint_shadow,
                block_exec,
                epoch_observer,
                remote_epoch_observer,
                tx_hook,
            },
        });
        let commit = CommitLoop::new(tx_receipts, rx_e2c).spawn();

        Threads {
            feeds: feed_handles,
            tx_ordering: tx_ordering_handle,
            exec,
            commit,
        }
    }
}

/// One reader, exec, or commit thread's outcome: `Ok(())` on a clean
/// subscription close, or the first error.
type ThreadResult = Result<(), ExecutorError>;

/// The readers [`Inbound::spawn_readers`] returns: the feed handles of the
/// transaction source, the one `tx_ordering` handle, and the exec thread's
/// inbound channel.
type SpawnedReaders = (
    Vec<FeedHandle>,
    JoinHandle<ThreadResult>,
    Receiver<ReaderToExec>,
);

impl<W: EngineWiring + 'static> Inbound<W> {
    /// Start the transaction source's feed threads and spawn the one
    /// `tx_ordering` reader. Each feed thread owns its subscription for
    /// its full life, so the returned join handles are enough to surface
    /// an error. Returns the feed handles, the `tx_ordering` handle, and
    /// the exec thread's inbound channel.
    fn spawn_readers(self, cfg: &ExecutorConfig) -> SpawnedReaders {
        let Inbound {
            tx_source,
            tx_ordering,
            exec_stream,
        } = self;
        let SourceStart { feeds, seed } = tx_source.start();
        let (tx_r2e, rx_r2e) = bounded::<ReaderToExec>(cfg.receipt_queue_depth.get());
        let tx_ordering_handle = TxOrderingReader::spawn(TxOrderingInputs {
            sub: tx_ordering,
            cfg: cfg.reader.clone(),
            exec_out: tx_r2e,
            exec_stream,
            join: seed,
        });
        (feeds, tx_ordering_handle, rx_r2e)
    }
}

/// The spawned threads: the feed threads of the transaction source, one
/// `tx_ordering` reader, one exec thread, one commit thread.
/// [`Self::join`] enforces the one join order that is safe: the pipeline
/// first, then the feeds. A caller that joins the handles itself must keep
/// that order, because a feed blocks in `next()` until its subscription
/// closes.
pub struct Threads {
    pub(crate) feeds: Vec<FeedHandle>,
    pub(crate) tx_ordering: JoinHandle<ThreadResult>,
    pub(crate) exec: JoinHandle<ThreadResult>,
    pub(crate) commit: JoinHandle<ThreadResult>,
}

impl Threads {
    /// Join the critical pipeline, then the feeds.
    ///
    /// # Errors
    ///
    /// Returns `Err` when any thread reports a fatal error (for example, a
    /// `BoundaryMisaligned` in exec).
    pub fn join(self) -> Result<(), ExecutorError> {
        let Threads {
            feeds,
            tx_ordering,
            exec,
            commit,
        } = self;
        Self::join_pipeline(tx_ordering, exec, commit)?;
        Self::join_feeds(feeds)
    }

    /// Join the critical pipeline: the `tx_ordering` reader, then exec,
    /// then commit. The reader closes when `tx_ordering` is exhausted. If
    /// any pipeline thread reports an error (for example, a fatal
    /// `BoundaryMisaligned` in exec), the executor cannot make more
    /// progress: the error propagates so the process exits and the
    /// orchestrator restarts it.
    fn join_pipeline(
        tx_ordering: JoinHandle<ThreadResult>,
        exec: JoinHandle<ThreadResult>,
        commit: JoinHandle<ThreadResult>,
    ) -> Result<(), ExecutorError> {
        // A panicked thread here means a logic bug, not a runtime failure
        // the caller can recover from. Propagate it as a panic on this
        // thread too, instead of hiding it behind an `Err`.
        let r_ordering = tx_ordering.join().expect("tx_ordering reader panic");
        let r_exec = exec.join().expect("exec panic");
        let r_commit = commit.join().expect("commit panic");
        r_ordering.and(r_exec).and(r_commit)
    }

    /// Join the feed threads, after [`Self::join_pipeline`] confirms Ok.
    ///
    /// Do not join these before the pipeline: each feed thread blocks
    /// in its Aeron `next()` call until its subscription closes, and that
    /// only happens on process teardown. Joining them while the process is
    /// still running would hang forever and hide a pipeline error: the
    /// process looks alive but makes no progress. On the normal Ok path,
    /// the subscriptions are already closed (`tx_ordering` is exhausted),
    /// so these joins return right away and drain cleanly.
    ///
    /// `fold` reads the whole iterator, so every reader is joined with no
    /// short-circuit. `and` keeps the first error.
    fn join_feeds(handles: Vec<FeedHandle>) -> Result<(), ExecutorError> {
        handles
            .into_iter()
            // A panicked reader thread means a logic bug; propagate it as
            // a panic here too, the same as `join_pipeline` above.
            .map(|h| h.join().expect("feed reader panic"))
            .fold(Ok(()), Result::and)
    }
}

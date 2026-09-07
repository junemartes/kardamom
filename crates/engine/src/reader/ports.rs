//! Reader ports: the subscription traits, the join-recovery seam, the
//! epoch-observer hooks, and the exec-sink handoff.

use crossbeam_channel::Sender;

use kardamom_types::xchain::RemoteEpochRecord;
use kardamom_types::{BPosition, Deposit, EpochRecord, TxDataLoc, TxEnvelope, TxOrderingMessage};

use crate::error::ExecutorError;

use super::threads::ReaderToExec;

/// Subscription to one `tx_data`[i].
///
/// One implementation per sequencer partition:
///   - in production: `kardamom_log::TxDataSubscriber`, on a dedicated OS
///     thread.
///   - in tests: `kardamom_log::testing::FakeTxDataSubscription`.
///
/// Contract: `next` blocks until the next `(tx_data_position, envelope)` is
/// available. It returns `Err(ExecutorError::TxDataClosed { sequencer_id })`
/// when the subscription closes cleanly.
pub trait TxDataSubscription: Send {
    /// Sequencer id this subscription is bound to. It keys the join buffer
    /// and appears in diagnostics.
    fn sequencer_id(&self) -> u8;

    /// # Errors
    ///
    /// Returns `Err(ExecutorError::TxDataClosed)` when the subscription
    /// closes cleanly, or another `ExecutorError` on a transport failure.
    fn next(&mut self) -> Result<(TxDataLoc, TxEnvelope), ExecutorError>;
}

/// Subscription to `tx_ordering`, the canonical orderer.
///
/// Yields tiny [`TxOrderingMessage`] records (`TxRef`, `DepositRef`, or
/// `BoundaryStart`), each tagged with its canonical `BPosition`. The
/// `BPosition` is the system's canonical L2 tx ordering (invariant I1).
///
/// In production: `kardamom_log::TxOrderingSubscriber`, on a dedicated OS
/// thread. In tests: see `kardamom_log::testing::FakeTxOrderingSubscription`.
pub trait TxOrderingSubscription: Send {
    /// # Errors
    ///
    /// Returns `Err(ExecutorError::TxOrderingClosed)` when the subscription
    /// closes cleanly, or another `ExecutorError` on a transport failure.
    fn next(&mut self) -> Result<(BPosition, TxOrderingMessage), ExecutorError>;
}

/// Archive-backed envelope recovery for join misses.
///
/// The `tx_data` and `tx_deposits` side streams are lossy multicast. A
/// canonical ref whose envelope never arrives, from an image lapse under
/// load, a publisher racing a subscriber restart, or a node-kill blackout,
/// stalls the join. Implementations of this trait close that gap in-band.
/// They fetch the missing range from a remote durability archive and feed
/// the join buffer, so a transient loss costs one bounded stall instead of
/// a process death. The join timeout stays the final arbiter: if the
/// archives cannot produce the envelope either, the reader still fails
/// loudly.
///
/// Implementations own their transport (endpoints, failover, backoff). The
/// `tx_ordering` reader thread calls them, so one call must stay well under
/// the join-timeout budget.
///
/// This trait is not `Send`. The Aeron archive client types are
/// thread-bound, so a [`JoinRecoveryFactory`] builds the recovery inside
/// the reader thread, and it never crosses threads.
pub trait JoinRecovery {
    /// Fetch `tx_data` envelopes for `shard_id`, recorded at or after `from` on
    /// the publisher session `session_id`. Feed each into `sink`, which
    /// inserts it into the join buffer. Return the number of envelopes
    /// recovered.
    ///
    /// # Errors
    ///
    /// Returns `Err` with a transport or archive-query failure description.
    fn recover_tx_data(
        &mut self,
        shard_id: u8,
        session_id: i32,
        from: BPosition,
        sink: &mut dyn FnMut(TxDataLoc, TxEnvelope),
    ) -> Result<u64, String>;

    /// Fetch `tx_deposits` recorded at or after `from`, from any publisher
    /// session. Feed each into `sink`. Return the number of deposits
    /// recovered.
    ///
    /// # Errors
    ///
    /// Returns `Err` with a transport or archive-query failure description.
    fn recover_deposits(
        &mut self,
        from: BPosition,
        sink: &mut dyn FnMut(BPosition, Deposit),
    ) -> Result<u64, String>;
}

/// Builds the thread-bound [`JoinRecovery`] inside the reader thread.
/// Return `None`, for example when config is absent, to keep the plain
/// bounded join.
pub type JoinRecoveryFactory = Box<dyn FnOnce() -> Option<Box<dyn JoinRecovery>> + Send>;

/// Role-specific hook, called for every [`EpochRecord`] on the canonical
/// stream, in canonical order, before its deposits are applied.
///
/// The executor wires nothing here; it trusts the ordered stream. The
/// validator wires a verifier that re-derives the epoch from L1 and rejects
/// a chain that disagrees. Deriving deposits is only half the guarantee:
/// without a checker, a buggy sequencer could silently build a chain
/// nobody can rebuild.
///
/// Returning `Err` stops the engine, the same fail-stop a receipt or
/// write-set divergence takes. Implementations must be cheap: this runs on
/// the exec thread. Anything with network latency, such as an L1 read,
/// belongs on a background task with a deferred verdict, not inline here.
pub trait EpochObserver: Send {
    /// # Errors
    ///
    /// Returns `Err` when the implementation rejects `epoch` (for example,
    /// the validator's verifier finds it disagrees with L1). This is
    /// fail-stop: the engine halts.
    fn observe(&mut self, epoch: &EpochRecord) -> Result<(), ExecutorError>;
}

/// The no-check [`EpochObserver`], for roles that trust the ordered stream:
/// the executor role, and most tests. Every epoch passes. This exists so an
/// [`EngineWiring`](crate::actor::EngineWiring) that runs no epoch
/// verification still has a concrete type to name.
#[derive(Debug, Clone, Copy, Default)]
pub struct NoEpochCheck;

impl EpochObserver for NoEpochCheck {
    fn observe(&mut self, _epoch: &EpochRecord) -> Result<(), ExecutorError> {
        Ok(())
    }
}

/// Role-specific hook called for every [`RemoteEpochRecord`] on the
/// canonical stream, in canonical order, before its messages execute.
/// This is the interop mirror of [`EpochObserver`].
///
/// The executor wires nothing here. The destination validator wires a
/// verifier instead. The verifier checks the pair's sequence rules
/// inline, and checks the record's content and anchor against L1 in the
/// background. Deriving remote epochs is only half the guarantee, the
/// same as for L1 epochs.
///
/// Same contract as [`EpochObserver`]: an `Err` result fail-stops the
/// engine. Keep implementations cheap. This code runs on the exec
/// thread. Put slow work, such as a network call, on a background task
/// with a deferred verdict.
pub trait RemoteEpochObserver: Send {
    /// # Errors
    ///
    /// Returns `Err` when the implementation rejects `rec`. This is
    /// fail-stop: the engine halts.
    fn observe(&mut self, rec: &RemoteEpochRecord) -> Result<(), ExecutorError>;
}

/// The consumer is gone; the reader thread exits cleanly.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SinkClosed;

/// Where the `tx_ordering` reader thread hands its records. The reader is a
/// std thread, so the send is blocking on every implementation: a crossbeam
/// channel between std threads (executor/validator exec thread), or a tokio
/// bounded channel whose consumer is an async task (batcher feed loop) via
/// `blocking_send`.
pub trait ExecSink: Send + 'static {
    /// # Errors
    ///
    /// Returns `Err(SinkClosed)` when the consumer (the exec thread) is
    /// gone.
    fn send(&self, msg: ReaderToExec) -> Result<(), SinkClosed>;
}

impl ExecSink for Sender<ReaderToExec> {
    fn send(&self, msg: ReaderToExec) -> Result<(), SinkClosed> {
        Sender::send(self, msg).map_err(|_| SinkClosed)
    }
}

impl ExecSink for tokio::sync::mpsc::Sender<ReaderToExec> {
    fn send(&self, msg: ReaderToExec) -> Result<(), SinkClosed> {
        self.blocking_send(msg).map_err(|_| SinkClosed)
    }
}

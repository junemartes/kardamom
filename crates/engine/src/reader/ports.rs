//! Reader ports: the subscription traits, the join-recovery seam, the
//! epoch-observer hooks, and the exec-sink handoff.

use crossbeam_channel::Sender;

use kardamom_types::xchain::RemoteEpochRecord;
use kardamom_types::{
    BPosition, Deposit, EpochRecord, StateDatabase, TxDataLoc, TxEnvelope, TxOrderingMessage,
};

use crate::delta::ParentState;
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
/// stalls the join. This closes that gap in-band: it fetches the missing
/// range from a remote durability archive and feeds the join buffer, so a
/// transient loss costs one bounded stall instead of a process death. The
/// join timeout stays the final arbiter: if the archives cannot produce
/// the envelope either, the reader still fails loudly.
///
/// A struct, not a trait: every role (the executor, the validator, the
/// batcher) recovers through the same durability archives, so there is
/// one implementation to name. The `tx_ordering` reader thread calls its
/// methods, so one call must stay well under the join-timeout budget.
///
/// This type is not `Send`. The Aeron archive client is thread-bound, so
/// a [`JoinRecoveryFactory`] builds it inside the reader thread, and it
/// never crosses threads.
pub struct JoinRecovery {
    refetcher: kardamom_log::refetch::ArchiveRefetcher,
    tx_data_stream_base: i32,
    tx_deposits_stream_id: i32,
}

/// A join-miss archive recovery attempt failed.
#[derive(Debug, thiserror::Error)]
pub enum JoinRecoveryError {
    /// `tx_data_stream_base + shard_id` overflowed `i32`.
    #[error("tx_data stream id overflow: base {base} + shard {shard}")]
    StreamIdOverflow { base: i32, shard: u8 },
    /// The archive fetch itself failed (transport, or no matching
    /// recording).
    #[error(transparent)]
    Archive(#[from] kardamom_log::error::LogError),
}

impl JoinRecovery {
    /// Fetch `tx_data` envelopes for `shard_id`, recorded at or after `from` on
    /// the publisher session `session_id`. Feed each into `sink`, which
    /// inserts it into the join buffer. Return the number of envelopes
    /// recovered.
    ///
    /// # Errors
    ///
    /// Returns `Err` on a transport or archive-query failure, or a shard id
    /// whose stream id overflows `i32`.
    pub fn recover_tx_data(
        &mut self,
        shard_id: u8,
        session_id: i32,
        from: BPosition,
        sink: impl FnMut(TxDataLoc, TxEnvelope),
    ) -> Result<u64, JoinRecoveryError> {
        let stream_id = self
            .tx_data_stream_base
            .checked_add(i32::from(shard_id))
            .ok_or(JoinRecoveryError::StreamIdOverflow {
                base: self.tx_data_stream_base,
                shard: shard_id,
            })?;
        Ok(self
            .refetcher
            .fetch_tx_data(stream_id, session_id, from, sink)?)
    }

    /// Fetch `tx_deposits` recorded at or after `from`, from any publisher
    /// session. Feed each into `sink`. Return the number of deposits
    /// recovered.
    ///
    /// # Errors
    ///
    /// Returns `Err` on a transport or archive-query failure.
    pub fn recover_deposits(
        &mut self,
        from: BPosition,
        sink: impl FnMut(BPosition, Deposit),
    ) -> Result<u64, JoinRecoveryError> {
        Ok(self
            .refetcher
            .fetch_deposits(self.tx_deposits_stream_id, from, sink)?)
    }
}

/// Builds the thread-bound [`JoinRecovery`] inside the reader thread. A
/// value, not a closure: a wiring seam needs to name a concrete type, and
/// a closure has none. Fields are `pub(crate)`: `bin_support::archive_join_recovery`
/// is the only constructor.
pub struct JoinRecoveryFactory {
    pub(crate) cfg: kardamom_log::refetch::RefetchConfig,
    pub(crate) tx_data_stream_base: i32,
    pub(crate) tx_deposits_stream_id: i32,
}

impl JoinRecoveryFactory {
    #[must_use]
    pub fn build(self) -> JoinRecovery {
        JoinRecovery {
            refetcher: kardamom_log::refetch::ArchiveRefetcher::new(self.cfg),
            tx_data_stream_base: self.tx_data_stream_base,
            tx_deposits_stream_id: self.tx_deposits_stream_id,
        }
    }
}

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
///
/// `parent` reads one storage slot from the state the record's block
/// builds on: the live delta, then the unsettled parent blocks, then the
/// committed snapshot. The destination validator seeds its per-origin
/// lane cursor from `Inbox.nextSeq[origin]` through it, so a restart never
/// exempts the first record from the contiguity check.
pub trait RemoteEpochObserver<S: StateDatabase>: Send {
    /// # Errors
    ///
    /// Returns `Err` when the implementation rejects `rec`. This is
    /// fail-stop: the engine halts.
    fn observe(
        &mut self,
        rec: &RemoteEpochRecord,
        parent: &ParentState<'_, S>,
    ) -> Result<(), ExecutorError>;
}

/// The no-check [`RemoteEpochObserver`], for roles that trust the pair's
/// origin sequence as sent: the executor role, and most tests. Every
/// record passes. This exists so an
/// [`EngineWiring`](crate::actor::EngineWiring) that runs no remote-epoch
/// verification still has a concrete type to name.
#[derive(Debug, Clone, Copy, Default)]
pub struct NoRemoteEpochCheck;

impl<S: StateDatabase> RemoteEpochObserver<S> for NoRemoteEpochCheck {
    fn observe(
        &mut self,
        _rec: &RemoteEpochRecord,
        _parent: &ParentState<'_, S>,
    ) -> Result<(), ExecutorError> {
        Ok(())
    }
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

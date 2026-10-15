//! Epoch verification.
//!
//! Deriving deposits is only half the guarantee. Without a checker, a buggy
//! or dishonest sequencer builds a chain nobody can rebuild from L1, and no
//! one notices until they try. This module is that checker. It treats any
//! disagreement as a divergence.
//!
//! There are two classes of check, split by cost:
//!
//! - Sequence rules (1 and 2), synchronous, always on. These check a
//!   monotonic origin and that no L1 block was skipped. They read only
//!   local state, so they run inline on the exec thread and reject before
//!   the epoch's deposits are applied. They need no L1 connection.
//! - Content checks (the epoch's hash and deposits), asynchronous, on only
//!   with an L1 source. For every [`EpochRecord`] on the canonical stream,
//!   they re-derive the epoch from L1 through the same [`derive_epoch`]
//!   function the producer uses, so a bug cannot cancel itself out. They
//!   need an L1 round trip. Running them inline would add RPC latency to
//!   the execution path and let a slow L1 stall the chain. Instead they run
//!   on a background task, which records the verdict. The next epoch reads
//!   the recorded divergence and stops the process.
//!
//! The deferred verdict has a cost: a bad epoch is detected rather than
//! prevented, and one more epoch may apply before the halt.

use std::sync::Arc;

use alloy_primitives::{Address, B256};
use kardamom_da_watcher::L1Header;
use kardamom_engine::{EpochObserver, ExecutorError};
use kardamom_types::EpochRecord;
use kardamom_types::epoch::derive_epoch;
use tokio::sync::mpsc::error::TrySendError;

use crate::Divergence;
use crate::metrics;

/// What the verifier needs from L1. This mirrors the DA watcher's source
/// trait, so both sides read L1 through one shape, and tests can drive a
/// fake.
#[async_trait::async_trait]
pub trait L1EpochSource: Send + Sync + 'static {
    /// The newest finalized L1 block number.
    async fn finalized_block_number(&self) -> anyhow::Result<u64>;
    /// `(hash, parent_hash)` of L1 block `number`, from one round trip.
    async fn block_ids(&self, number: u64) -> anyhow::Result<(B256, B256)>;
    /// The headers of the blocks `from..=to`, in order, from one batch.
    async fn headers(&self, from: u64, to: u64) -> anyhow::Result<Vec<L1Header>>;
    /// Both lockbox event kinds, from one query, the same call the producer
    /// makes. Reading fewer kinds than the watcher writes would report every
    /// upgrade as a fabricated deposit.
    async fn lockbox_logs(
        &self,
        lockbox: Address,
        from_block: u64,
        to_block: u64,
    ) -> anyhow::Result<Vec<kardamom_types::epoch::LockboxLog>>;
}

/// Blanket adapter over the DA watcher's `L1Source`, so the validator reads
/// L1 through the same implementation the producer uses.
#[async_trait::async_trait]
impl<T> L1EpochSource for T
where
    T: kardamom_da_watcher::L1Source,
{
    async fn finalized_block_number(&self) -> anyhow::Result<u64> {
        kardamom_da_watcher::L1Source::finalized_block_number(self)
            .await
            .map_err(|e| anyhow::anyhow!("{e}"))
    }

    async fn block_ids(&self, number: u64) -> anyhow::Result<(B256, B256)> {
        kardamom_da_watcher::L1Source::block_ids(self, number)
            .await
            .map_err(|e| anyhow::anyhow!("{e}"))
    }

    async fn headers(&self, from: u64, to: u64) -> anyhow::Result<Vec<L1Header>> {
        kardamom_da_watcher::L1Source::headers(self, from, to)
            .await
            .map_err(|e| anyhow::anyhow!("{e}"))
    }

    async fn lockbox_logs(
        &self,
        lockbox: Address,
        from_block: u64,
        to_block: u64,
    ) -> anyhow::Result<Vec<kardamom_types::epoch::LockboxLog>> {
        kardamom_da_watcher::L1Source::lockbox_logs(self, lockbox, from_block, to_block)
            .await
            .map_err(|e| anyhow::anyhow!("{e}"))
    }
}

/// Why an epoch failed verification. Every variant is a chain-level fault,
/// not a transient one; a transport error is reported separately and
/// retried.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum EpochFault {
    /// Rule 1: the origin went backwards, or repeated.
    OriginRegressed { previous: u64, got: u64 },
    /// Rule 2: an L1 block was skipped, so its deposits are unaccounted for.
    OriginSkipped { previous: u64, got: u64 },
    /// The epoch names an L1 block whose hash does not match what L1
    /// reports: a different chain, or a fabricated epoch.
    HashMismatch { l1_number: u64 },
    /// The epoch's L1 block does not descend from the previous epoch's:
    /// block N's parent hash is not block N-1's hash. Consecutive origins
    /// must be consecutive blocks, not only consecutive numbers.
    ParentMismatch {
        l1_number: u64,
        expected_parent: B256,
        got_parent: B256,
    },
    /// Rule 4: the epoch names an L1 block that does not exist at or below
    /// finality, and still does not after the retry window. A chain cannot
    /// anchor to an L1 block that has not happened.
    BlockBeyondFinality { l1_number: u64, attempts: u32 },
    /// The deposits do not match what L1 recorded for that block: some are
    /// dropped, added, altered, or reordered.
    DepositsMismatch {
        l1_number: u64,
        expected: usize,
        got: usize,
        detail: String,
    },
}

impl std::fmt::Display for EpochFault {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::OriginRegressed { previous, got } => write!(
                f,
                "l1_origin regressed: {previous} -> {got} (derivation is ambiguous \
                 if two blocks claim different origins for the same L1)"
            ),
            Self::OriginSkipped { previous, got } => write!(
                f,
                "l1_origin skipped {} block(s): {previous} -> {got} — the deposits in \
                 between are unaccounted for",
                got.saturating_sub(*previous).saturating_sub(1)
            ),
            Self::HashMismatch { l1_number } => write!(
                f,
                "epoch for L1 block {l1_number} names a hash L1 does not report"
            ),
            Self::ParentMismatch {
                l1_number,
                expected_parent,
                got_parent,
            } => write!(
                f,
                "epoch for L1 block {l1_number} does not descend from the previous epoch: \
                 its parent is {got_parent}, the previous epoch's block was {expected_parent} \
                 — the origin sequence is numbered consecutively but is not one chain"
            ),
            Self::BlockBeyondFinality {
                l1_number,
                attempts,
            } => write!(
                f,
                "epoch names L1 block {l1_number}, which L1 still does not have after \
                 {attempts} attempts — the chain is anchored to a block that has not happened"
            ),
            Self::DepositsMismatch {
                l1_number,
                expected,
                got,
                detail,
            } => write!(
                f,
                "epoch for L1 block {l1_number} carries {got} deposit(s), L1 says \
                 {expected}: {detail}"
            ),
        }
    }
}

/// Compare an epoch against the truth derived from L1.
///
/// This is a separate function so it is testable without a runtime or a
/// chain. Pass it what L1 said and what the stream carried.
pub(crate) fn compare_against_l1(
    epoch: &EpochRecord,
    l1_hash: alloy_primitives::B256,
    logs: &[kardamom_types::epoch::LockboxLog],
) -> Result<(), EpochFault> {
    if epoch.l1_hash != l1_hash {
        return Err(EpochFault::HashMismatch {
            l1_number: epoch.l1_number,
        });
    }
    // Derive through the producer's own rule. A verifier running a second,
    // slightly different copy of the rule would verify nothing.
    let truth = match derive_epoch(epoch.l1_number, l1_hash, logs) {
        Ok(t) => t,
        Err(e) => {
            return Err(EpochFault::DepositsMismatch {
                l1_number: epoch.l1_number,
                expected: logs.len(),
                got: epoch.deposits.len(),
                detail: format!("L1 logs do not derive: {e}"),
            });
        }
    };
    if truth.deposits != epoch.deposits {
        // Name the first position that differs. A plain count comparison
        // would miss "3 vs 3 but different", which is the reorder attack.
        let detail = truth
            .deposits
            .iter()
            .zip(epoch.deposits.iter())
            .position(|(a, b)| a != b)
            .map_or_else(
                || "differing length".to_string(),
                |i| format!("first difference at deposit index {i}"),
            );
        return Err(EpochFault::DepositsMismatch {
            l1_number: epoch.l1_number,
            expected: truth.deposits.len(),
            got: epoch.deposits.len(),
            detail,
        });
    }
    Ok(())
}

/// Check rules 1 and 2 over consecutive origins. `previous` is `None`
/// before the first epoch.
///
/// The step out of "no origin yet" is exempt from rule 2. The producer
/// seeds its cursor at the finalized tip it first observes, so the
/// chain's first epoch is not L1 block 1. A chain's verifiable history
/// starts at its first epoch, not at L1 block 1.
pub(crate) fn check_sequence(previous: Option<u64>, got: u64) -> Result<(), EpochFault> {
    let Some(previous) = previous else {
        return Ok(());
    };
    if got <= previous {
        return Err(EpochFault::OriginRegressed { previous, got });
    }
    // Bounded: `previous == u64::MAX` would have failed the check above
    // (every `got` is `<= u64::MAX`), so `previous < u64::MAX` here.
    if got != previous + 1 {
        return Err(EpochFault::OriginSkipped { previous, got });
    }
    Ok(())
}

/// The last epoch that verified: the anchor the next one must descend
/// from. Only a verified epoch becomes an anchor: chaining from an
/// unchecked hash would let one accepted lie make every later block look
/// legitimate.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Anchor {
    pub(crate) number: u64,
    pub(crate) hash: B256,
}

/// Engine-side seam: checks rules 1 and 2 inline on every epoch, and
/// queues the L1 content check when one is wired.
pub struct EpochVerifier {
    previous_origin: Option<u64>,
    divergence: Arc<Divergence>,
    /// The queue to the L1 content-check task. `None` means the validator
    /// has no L1 source, and rules 1 and 2 are the whole check.
    content: Option<ContentQueue>,
}

impl EpochVerifier {
    /// A verifier that checks rules 1 and 2 only. These rules read local
    /// state, so this needs no L1 connection and no runtime.
    #[must_use]
    pub fn new(divergence: Arc<Divergence>) -> Self {
        Self {
            previous_origin: None,
            divergence,
            content: None,
        }
    }

    /// Add the L1 content check. Its task runs on `rt`, owns the L1 reads,
    /// and reads L1 through `sources`. The exec thread only queues epochs.
    /// A second task reads the anchor's finalized tip every
    /// [`TIP_EVERY`], the validator's own view of L1 finality.
    #[must_use]
    pub fn with_content_check<S: L1EpochSource>(
        self,
        sources: ContentSources<S>,
        rt: &tokio::runtime::Handle,
    ) -> Self {
        let (tx, rx) = tokio::sync::mpsc::channel::<EpochRecord>(EPOCH_QUEUE_CAP);
        rt.spawn(step::watch_tip(sources.anchor.clone()));
        rt.spawn(Verifier::new(sources, self.divergence.clone(), rx).run());
        Self {
            content: Some(ContentQueue(tx)),
            ..self
        }
    }
}

/// The exec-thread end of the queue to the content-check task.
struct ContentQueue(tokio::sync::mpsc::Sender<EpochRecord>);

impl ContentQueue {
    /// Queue `epoch` for the content check. This uses `try_send`: a full
    /// queue must not block the exec thread. A dropped item only costs
    /// coverage of that epoch.
    fn offer(&self, epoch: &EpochRecord) {
        match self.0.try_send(epoch.clone()) {
            Ok(()) => {}
            Err(TrySendError::Full(_)) => {
                metrics::counter_epoch_unverified(1);
                tracing::warn!(
                    l1_number = epoch.l1_number,
                    "epoch verifier queue full; epoch not content-checked"
                );
            }
            Err(TrySendError::Closed(_)) => {
                tracing::warn!("epoch verifier task is gone; content checks stopped");
            }
        }
    }
}

impl EpochObserver for EpochVerifier {
    fn observe(&mut self, epoch: &EpochRecord) -> Result<(), ExecutorError> {
        // The background task records the content-check verdict; it lands
        // here, on the next epoch.
        if let Some(reason) = self.divergence.halt_reason("validator halted") {
            return Err(ExecutorError::State(reason));
        }
        // Rules 1 and 2 are local, so they reject inline, before this
        // epoch's deposits are applied.
        if let Err(fault) = check_sequence(self.previous_origin, epoch.l1_number) {
            metrics::counter_epoch_fault();
            self.divergence
                .record(format!("epoch verification failed: {fault}"));
            return Err(ExecutorError::State(fault.to_string()));
        }
        self.previous_origin = Some(epoch.l1_number);
        if let Some(queue) = &self.content {
            queue.offer(epoch);
        }
        Ok(())
    }
}

mod step;

pub use step::{ContentSources, TIP_EVERY};
use step::{EPOCH_QUEUE_CAP, Verifier};

#[cfg(test)]
#[path = "epoch_verify_tests.rs"]
mod tests;

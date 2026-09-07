//! Epoch verification against L1.
//!
//! Deriving deposits is only half the guarantee. Without a checker, a buggy
//! or dishonest sequencer builds a chain nobody can rebuild from L1, and no
//! one notices until they try. This module is that checker. For every
//! [`EpochRecord`] on the canonical stream, it re-derives the epoch from
//! L1, through the same [`derive_epoch`] function the producer used, so a
//! bug cannot cancel itself out. It treats any disagreement as a
//! divergence.
//!
//! There are two classes of check, split by cost:
//!
//! - Sequence rules (1 and 2), synchronous. These check a monotonic origin
//!   and that no L1 block was skipped. They read only local state, so they
//!   run inline on the exec thread and reject before the epoch's deposits
//!   are applied.
//! - Content checks (the epoch's hash and deposits), asynchronous. These
//!   need an L1 round trip. Running them inline would add RPC latency to
//!   the execution path and let a slow L1 stall the chain. Instead they run
//!   on a background task, which records the verdict. The next epoch reads
//!   the recorded divergence and stops the process.
//!
//! The deferred verdict has a cost: a bad epoch is detected rather than
//! prevented, and one more epoch may apply before the halt.

use std::sync::Arc;

use alloy_primitives::{Address, B256};
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
    /// `(hash, parent_hash)` of L1 block `number`, from one round trip.
    async fn block_ids(&self, number: u64) -> anyhow::Result<(B256, B256)>;
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
    async fn block_ids(&self, number: u64) -> anyhow::Result<(B256, B256)> {
        kardamom_da_watcher::L1Source::block_ids(self, number)
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

/// Engine-side seam: checks the sequence inline and queues the content check.
pub struct EpochVerifier {
    previous_origin: Option<u64>,
    divergence: Arc<Divergence>,
    tx: tokio::sync::mpsc::Sender<EpochRecord>,
}

impl EpochVerifier {
    /// Wire a verifier onto `rt`, and read L1 through `source`.
    ///
    /// The background task owns the L1 reads. The returned value is what
    /// the engine calls on the exec thread.
    pub fn spawn<S: L1EpochSource>(
        source: Arc<S>,
        lockbox: Address,
        divergence: Arc<Divergence>,
        rt: &tokio::runtime::Handle,
    ) -> Self {
        let (tx, mut rx) = tokio::sync::mpsc::channel::<EpochRecord>(EPOCH_QUEUE_CAP);
        let div = divergence.clone();
        rt.spawn(async move {
            let mut verifier = Verifier::new(source, lockbox);
            while let Some(epoch) = rx.recv().await {
                let verdict = verifier.verify_with_retry(&epoch).await;
                verifier.record_verdict(&div, verdict);
            }
        });
        Self {
            previous_origin: None,
            divergence,
            tx,
        }
    }
}

/// Outcome of a full retry sequence for one epoch's content check.
enum VerifyVerdict {
    /// The epoch verified; carries the new anchor.
    Verified(Anchor),
    /// A chain fault: the epoch is provably wrong.
    Fault(EpochFault),
    /// L1 stayed unreachable through every retry. Not a chain fault: an
    /// RPC outage must not read as a divergence.
    Unverified,
}

/// The content-check driver: reads L1 through `source`, and carries the
/// anchor forward across epochs.
struct Verifier<S: L1EpochSource> {
    source: Arc<S>,
    lockbox: Address,
    anchor: Option<Anchor>,
}

impl<S: L1EpochSource> Verifier<S> {
    fn new(source: Arc<S>, lockbox: Address) -> Self {
        Self {
            source,
            lockbox,
            anchor: None,
        }
    }

    /// Verify `epoch`'s content against L1, retrying a transient L1
    /// failure up to [`VERIFY_ATTEMPTS`] times. Retrying, rather than
    /// giving up after one try, absorbs the normal case where this
    /// validator's L1 view lags the producer's by a block or two, and
    /// avoids silently dropping verification coverage for the epoch,
    /// which is exactly where a forgery would hide.
    async fn verify_with_retry(&self, epoch: &EpochRecord) -> VerifyVerdict {
        let mut attempt = 0u32;
        loop {
            // Bounded: the loop breaks once `attempt >= VERIFY_ATTEMPTS`
            // (a small constant), so this never approaches `u32::MAX`.
            attempt += 1;
            match verify_one(self.source.as_ref(), self.lockbox, epoch, self.anchor).await {
                Ok(()) => {
                    return VerifyVerdict::Verified(Anchor {
                        number: epoch.l1_number,
                        hash: epoch.l1_hash,
                    });
                }
                Err(VerifyOutcome::Fault(fault)) => return VerifyVerdict::Fault(fault),
                Err(VerifyOutcome::Unavailable(e)) if attempt < VERIFY_ATTEMPTS => {
                    tracing::debug!(
                        l1_number = epoch.l1_number,
                        attempt,
                        error = %e,
                        "epoch verification retrying"
                    );
                    tokio::time::sleep(VERIFY_RETRY_DELAY).await;
                }
                Err(VerifyOutcome::Unavailable(e)) => {
                    // Out of retries. If L1 simply does not have this block,
                    // the epoch is anchored to something that never happened:
                    // rule 4, a fault. Any other transport failure stays a
                    // coverage gap.
                    if is_missing_block(&e) {
                        return VerifyVerdict::Fault(EpochFault::BlockBeyondFinality {
                            l1_number: epoch.l1_number,
                            attempts: attempt,
                        });
                    }
                    tracing::warn!(
                        l1_number = epoch.l1_number,
                        attempts = attempt,
                        error = %e,
                        "epoch verification gave up: L1 unavailable"
                    );
                    return VerifyVerdict::Unverified;
                }
            }
        }
    }

    /// Apply one epoch's verdict: bump the metric, record a divergence on
    /// a fault, and advance the anchor on success.
    fn record_verdict(&mut self, divergence: &Divergence, verdict: VerifyVerdict) {
        match verdict {
            VerifyVerdict::Verified(new_anchor) => {
                metrics::counter_epoch_verified();
                self.anchor = Some(new_anchor);
            }
            VerifyVerdict::Fault(fault) => {
                metrics::counter_epoch_fault();
                divergence.record(format!("epoch verification failed: {fault}"));
            }
            VerifyVerdict::Unverified => {
                metrics::counter_epoch_unverified();
            }
        }
    }
}

/// Depth of the queue from the exec thread to the verifier task. Epochs
/// arrive at L1 block cadence, about 1 per 12 s. One content check takes
/// at most `VERIFY_ATTEMPTS * VERIFY_RETRY_DELAY` (16 s). The queue drains
/// faster than it fills, unless L1 is down for a long time. 64 entries hold
/// about 13 minutes of epochs. After that, the exec thread drops the epoch
/// (see [`EpochObserver::observe`]) and does not block on the outage.
const EPOCH_QUEUE_CAP: usize = 64;

/// How many times a content check is retried before a verdict. This spans a
/// few L1 block times, so a normal lag between this validator's L1 view and
/// the producer's resolves well inside it.
const VERIFY_ATTEMPTS: u32 = 8;
const VERIFY_RETRY_DELAY: std::time::Duration = std::time::Duration::from_secs(2);

/// Tell "L1 does not have this block" apart from "L1 did not answer". The
/// first is a statement about the chain; the second is about the network.
fn is_missing_block(e: &anyhow::Error) -> bool {
    e.to_string().contains("not found")
}

/// Outcome of one content check, separating a chain fault from an L1 outage.
enum VerifyOutcome {
    Fault(EpochFault),
    Unavailable(anyhow::Error),
}

async fn verify_one<S: L1EpochSource + ?Sized>(
    source: &S,
    lockbox: Address,
    epoch: &EpochRecord,
    previous: Option<Anchor>,
) -> Result<(), VerifyOutcome> {
    let (hash, parent) = source
        .block_ids(epoch.l1_number)
        .await
        .map_err(VerifyOutcome::Unavailable)?;
    // Chain the origins together. Verifying each block alone would let an
    // L1 endpoint serve any hash it likes for any number. Requiring block N
    // to descend from block N-1 forces it to fabricate a consistent chain
    // instead. This check costs nothing, since the parent hash came back in
    // the same header. It only checks the immediate predecessor, which the
    // sequence rules already require.
    if let Some(Anchor {
        number: prev_number,
        hash: prev_hash,
    }) = previous
        // `prev_number` came from a verified epoch, but it is still an L1
        // wire value, not this process's own counter: check it rather
        // than assume it is not already `u64::MAX`. `None` here (an
        // impossible predecessor number) just skips the immediate-parent
        // check; the sequence rules already reject any actual gap.
        && prev_number.checked_add(1) == Some(epoch.l1_number)
        && parent != prev_hash
    {
        return Err(VerifyOutcome::Fault(EpochFault::ParentMismatch {
            l1_number: epoch.l1_number,
            expected_parent: prev_hash,
            got_parent: parent,
        }));
    }
    let logs = source
        .lockbox_logs(lockbox, epoch.l1_number, epoch.l1_number)
        .await
        .map_err(VerifyOutcome::Unavailable)?;
    compare_against_l1(epoch, hash, &logs).map_err(VerifyOutcome::Fault)
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
        // The content check uses `try_send`. A full queue must not block
        // the exec thread. A dropped item only costs coverage of that
        // epoch.
        match self.tx.try_send(epoch.clone()) {
            Ok(()) => {}
            Err(TrySendError::Full(_)) => {
                metrics::counter_epoch_unverified();
                tracing::warn!(
                    l1_number = epoch.l1_number,
                    "epoch verifier queue full; epoch not content-checked"
                );
            }
            Err(TrySendError::Closed(_)) => {
                tracing::warn!("epoch verifier task is gone; content checks stopped");
            }
        }
        Ok(())
    }
}

#[cfg(test)]
#[path = "epoch_verify_tests.rs"]
mod tests;

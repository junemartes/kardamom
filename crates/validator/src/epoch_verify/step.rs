//! The L1 content check, at the finality-step cadence.
//!
//! The validator reads L1 on its own: it must not read the L1 follower's
//! stream, or a lie that passed the follower would pass the check too.
//! It checks the epochs on the canonical stream one range at a time; the
//! epochs of one finality step arrive together, so a range is about one
//! step.
//!
//! For a range `a..=b`:
//!
//! 1. the anchor (the light client) gives the hash of block `b`;
//! 2. the logs source gives the headers of `a..=b` in one batch; each
//!    header names the one before it as its parent, the first names the
//!    last verified epoch's block, and the last is the anchor's header;
//! 3. each epoch's hash is its header's hash;
//! 4. the logs source gives the lockbox logs of `a..=b`, in chunks of
//!    `max_log_range` blocks, and each epoch must derive from its block's
//!    logs through the producer's own rule.
//!
//! The logs source is not the follower's first source: by default it is
//! the light client itself, which the follower uses only as a tie-breaker.

use std::collections::BTreeMap;
use std::num::NonZeroU64;
use std::ops::ControlFlow;
use std::sync::Arc;
use std::time::Duration;

use alloy_primitives::Address;
use kardamom_da_watcher::L1Header;
use kardamom_types::EpochRecord;
use kardamom_types::epoch::LockboxLog;

use super::{Anchor, EpochFault, L1EpochSource, compare_against_l1};
use crate::Divergence;
use crate::metrics;

/// Depth of the queue from the exec thread to the verifier task. Epochs
/// arrive at L1 block cadence, about 1 per 12 s, in steps of 32. 256
/// entries hold about 50 minutes of epochs; after that the exec thread
/// drops an epoch and does not block on an L1 outage.
pub(super) const EPOCH_QUEUE_CAP: usize = 256;

/// The most epochs one range holds. The light client serves its last
/// 256 heads, and a range's last block must be among them.
const RANGE_CAP: usize = 64;

/// How long the task waits for one more epoch of the range. The epochs
/// of one step arrive within a few sealer ticks of each other.
const GATHER: Duration = Duration::from_secs(4);

/// How many times a range's check is retried before a verdict. It spans
/// a few L1 block times, so a lag of this validator's L1 view behind the
/// producer's resolves inside it.
const VERIFY_ATTEMPTS: u32 = 8;
const VERIFY_RETRY_DELAY: Duration = Duration::from_secs(2);

/// How often the validator reads its own view of the finalized tip.
pub const TIP_EVERY: Duration = Duration::from_mins(1);

/// Where the content check reads L1.
pub struct ContentSources<S> {
    /// The anchor: the light client. A range's last header must be its
    /// header for that number.
    pub anchor: Arc<S>,
    /// The headers and the logs: a source that is not the follower's
    /// first source.
    pub logs: Arc<S>,
    pub lockbox: Address,
    /// The most blocks one log query spans.
    pub max_log_range: NonZeroU64,
}

/// Outcome of a full retry sequence for one range.
pub(super) enum VerifyVerdict {
    /// Every epoch verified; carries the new anchor.
    Verified(Anchor),
    /// A chain fault: an epoch is provably wrong.
    Fault(EpochFault),
    /// L1 stayed unreachable through every retry. Not a chain fault: an
    /// RPC outage must not read as a divergence.
    Unverified,
}

/// Outcome of one check of a range, separating a chain fault from an L1
/// outage.
pub(super) enum VerifyOutcome {
    Fault(EpochFault),
    Unavailable(anyhow::Error),
}

/// The content-check task's state.
pub(super) struct Verifier<S: L1EpochSource> {
    pub(super) rx: tokio::sync::mpsc::Receiver<EpochRecord>,
    pub(super) sources: ContentSources<S>,
    pub(super) divergence: Arc<Divergence>,
    pub(super) anchor: Option<Anchor>,
}

/// Read the anchor's finalized tip every [`TIP_EVERY`], forever, and
/// export it. A failed read keeps the last value.
pub(super) async fn watch_tip<S: L1EpochSource>(anchor: Arc<S>) {
    let mut interval = tokio::time::interval(TIP_EVERY);
    loop {
        interval.tick().await;
        match anchor.finalized_block_number().await {
            Ok(tip) => metrics::gauge_l1_finalized(tip),
            Err(error) => tracing::debug!(%error, "the L1 finalized tip read failed"),
        }
    }
}

impl<S: L1EpochSource> Verifier<S> {
    pub(super) fn new(
        sources: ContentSources<S>,
        divergence: Arc<Divergence>,
        rx: tokio::sync::mpsc::Receiver<EpochRecord>,
    ) -> Self {
        Self {
            rx,
            sources,
            divergence,
            anchor: None,
        }
    }

    /// Check each range of queued epochs until the exec side drops its
    /// sender.
    pub(super) async fn run(mut self) {
        while let Some(first) = self.rx.recv().await {
            let gathered = self.gather(first).await;
            for range in Self::runs(gathered) {
                let verdict = self.verify_with_retry(&range).await;
                self.record_verdict(&range, verdict);
            }
        }
    }

    /// The epochs that arrive within [`GATHER`] of each other, up to
    /// [`RANGE_CAP`], from `first` on.
    async fn gather(&mut self, first: EpochRecord) -> Vec<EpochRecord> {
        let mut range = vec![first];
        while range.len() < RANGE_CAP {
            let Ok(Some(next)) = tokio::time::timeout(GATHER, self.rx.recv()).await else {
                break;
            };
            range.push(next);
        }
        range
    }

    /// Split `epochs` into runs of consecutive block numbers. A dropped
    /// epoch (a full queue) leaves a gap; each run is checked alone.
    pub(super) fn runs(epochs: Vec<EpochRecord>) -> Vec<Vec<EpochRecord>> {
        epochs.into_iter().fold(Vec::new(), |mut runs, epoch| {
            let joins = runs
                .last()
                .and_then(|run: &Vec<EpochRecord>| run.last())
                .and_then(|last| last.l1_number.checked_add(1))
                == Some(epoch.l1_number);
            match runs.last_mut() {
                Some(run) if joins => run.push(epoch),
                _ => runs.push(vec![epoch]),
            }
            runs
        })
    }

    /// Check a range, retrying a transient L1 failure up to
    /// [`VERIFY_ATTEMPTS`] times.
    async fn verify_with_retry(&self, range: &[EpochRecord]) -> VerifyVerdict {
        let mut attempt = 0u32;
        loop {
            // Bounded: the loop ends at `VERIFY_ATTEMPTS`, a small constant.
            attempt += 1;
            if let ControlFlow::Break(verdict) = self.verify_attempt(range, attempt).await {
                return verdict;
            }
        }
    }

    /// One attempt: `Break` carries the verdict, `Continue` retries after
    /// the delay.
    async fn verify_attempt(
        &self,
        range: &[EpochRecord],
        attempt: u32,
    ) -> ControlFlow<VerifyVerdict> {
        match self.verify_range(range).await {
            Ok(anchor) => ControlFlow::Break(VerifyVerdict::Verified(anchor)),
            Err(VerifyOutcome::Fault(fault)) => ControlFlow::Break(VerifyVerdict::Fault(fault)),
            Err(VerifyOutcome::Unavailable(e)) if attempt < VERIFY_ATTEMPTS => {
                tracing::debug!(attempt, error = %e, "epoch range check retrying");
                tokio::time::sleep(VERIFY_RETRY_DELAY).await;
                ControlFlow::Continue(())
            }
            Err(VerifyOutcome::Unavailable(e)) => {
                ControlFlow::Break(Self::give_up(range, attempt, &e))
            }
        }
    }

    /// Out of retries. If L1 does not have the range's last block, the
    /// epochs are anchored to something that never happened: rule 4, a
    /// fault. Any other failure is a coverage gap.
    pub(super) fn give_up(range: &[EpochRecord], attempt: u32, e: &anyhow::Error) -> VerifyVerdict {
        let last = range.last().map_or(0, |epoch| epoch.l1_number);
        if e.to_string().contains("not found") {
            return VerifyVerdict::Fault(EpochFault::BlockBeyondFinality {
                l1_number: last,
                attempts: attempt,
            });
        }
        tracing::warn!(
            last,
            epochs = range.len(),
            attempts = attempt,
            error = %e,
            "epoch range check gave up: L1 unavailable"
        );
        VerifyVerdict::Unverified
    }

    /// Apply a range's verdict: the counters, a divergence on a fault,
    /// and the new anchor on success.
    fn record_verdict(&mut self, range: &[EpochRecord], verdict: VerifyVerdict) {
        let n = u64::try_from(range.len()).unwrap_or(u64::MAX);
        match verdict {
            VerifyVerdict::Verified(anchor) => {
                metrics::counter_epoch_verified(n);
                self.anchor = Some(anchor);
            }
            VerifyVerdict::Fault(fault) => {
                metrics::counter_epoch_fault();
                self.divergence
                    .record(format!("epoch verification failed: {fault}"));
            }
            VerifyVerdict::Unverified => metrics::counter_epoch_unverified(n),
        }
    }

    /// One check of a consecutive range. Returns the anchor its last
    /// epoch makes.
    pub(super) async fn verify_range(
        &self,
        range: &[EpochRecord],
    ) -> Result<Anchor, VerifyOutcome> {
        let (Some(first), Some(last)) = (range.first(), range.last()) else {
            return Err(VerifyOutcome::Unavailable(anyhow::anyhow!(
                "an empty range"
            )));
        };
        let (from, to) = (first.l1_number, last.l1_number);
        let (anchored, _) = self
            .sources
            .anchor
            .block_ids(to)
            .await
            .map_err(VerifyOutcome::Unavailable)?;
        let headers = self
            .sources
            .logs
            .headers(from, to)
            .await
            .map_err(VerifyOutcome::Unavailable)?;
        self.check_chain(range, &headers)?;
        if last.l1_hash != anchored {
            return Err(VerifyOutcome::Fault(EpochFault::HashMismatch {
                l1_number: to,
            }));
        }
        let mut logs = self.logs(from, to).await?;
        range.iter().zip(&headers).try_for_each(|(epoch, header)| {
            let block_logs = logs.remove(&epoch.l1_number).unwrap_or_default();
            compare_against_l1(epoch, header.hash, &block_logs).map_err(VerifyOutcome::Fault)
        })?;
        Ok(Anchor {
            number: to,
            hash: last.l1_hash,
        })
    }

    /// Each epoch's hash is its header's; each header names the one
    /// before it, and the first the last verified epoch's block.
    fn check_chain(
        &self,
        range: &[EpochRecord],
        headers: &[L1Header],
    ) -> Result<(), VerifyOutcome> {
        if headers.len() != range.len() {
            return Err(VerifyOutcome::Unavailable(anyhow::anyhow!(
                "{} headers for {} epochs",
                headers.len(),
                range.len()
            )));
        }
        let first = range.first().map_or(0, |e| e.l1_number);
        let before = self
            .anchor
            .filter(|a| a.number.checked_add(1) == Some(first))
            .map(|a| a.hash);
        let parents = before.into_iter().chain(headers.iter().map(|h| h.hash));
        let skip = usize::from(before.is_none());
        range
            .iter()
            .zip(headers)
            .try_for_each(|(epoch, header)| {
                if epoch.l1_hash == header.hash {
                    Ok(())
                } else {
                    Err(EpochFault::HashMismatch {
                        l1_number: epoch.l1_number,
                    })
                }
            })
            .and_then(|()| {
                parents
                    .zip(headers.iter().skip(skip))
                    .find(|(parent, header)| header.parent_hash != *parent)
                    .map_or(Ok(()), |(expected, header)| {
                        Err(EpochFault::ParentMismatch {
                            l1_number: header.number,
                            expected_parent: expected,
                            got_parent: header.parent_hash,
                        })
                    })
            })
            .map_err(VerifyOutcome::Fault)
    }

    /// The lockbox logs of `from..=to`, by block, in chunks of
    /// `max_log_range` blocks.
    async fn logs(
        &self,
        from: u64,
        to: u64,
    ) -> Result<BTreeMap<u64, Vec<LockboxLog>>, VerifyOutcome> {
        let step = self.sources.max_log_range.get();
        let starts = (from..=to).step_by(usize::try_from(step).unwrap_or(usize::MAX));
        let mut by_block: BTreeMap<u64, Vec<LockboxLog>> = BTreeMap::new();
        for start in starts {
            self.logs_chunk(start, to.min(start.saturating_add(step - 1)), &mut by_block)
                .await?;
        }
        Ok(by_block)
    }

    async fn logs_chunk(
        &self,
        from: u64,
        to: u64,
        by_block: &mut BTreeMap<u64, Vec<LockboxLog>>,
    ) -> Result<(), VerifyOutcome> {
        let logs = self
            .sources
            .logs
            .lockbox_logs(self.sources.lockbox, from, to)
            .await
            .map_err(VerifyOutcome::Unavailable)?;
        for log in logs {
            by_block.entry(log.block_number()).or_default().push(log);
        }
        Ok(())
    }
}

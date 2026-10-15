//! The L1 watcher follows the sealer's commit, not its own publish.
//!
//! The sealer's boundaries carry its L1 origin C. C confirms every epoch
//! up to it. The watcher keeps the epochs it published after C in a
//! [`crate::window::Window`], and the cursor file holds C. So:
//!
//! - a restart resumes after the sealer's origin, read from the first
//!   boundary. An epoch published but never committed is published again.
//!   A copy of a committed epoch is harmless: the sealer drops it by its
//!   canonical id, or as an origin at or below its own;
//! - when C does not move for [`REPUBLISH_AFTER`] while epochs wait, the
//!   watcher publishes them again, in order. This heals a sequencer that
//!   lost its queue, a leader kill, and a dropped session;
//! - when C leaves the published range, the watcher anchors at C: below
//!   the base (a sealer fleet seeded at an older origin), or past the head
//!   (another watcher published the epochs). It takes C's hash from the
//!   `l1_blocks` record of C, and the next block must descend from it.

use std::num::NonZeroU64;
use std::ops::ControlFlow;
use std::time::Duration;

use kardamom_types::EpochRecord;
use kardamom_types::epoch_delivery::REPUBLISH_AFTER;
use tokio::sync::{oneshot, watch};
use tokio::time::Instant;
use tracing::{info, warn};

use super::{L1Watcher, MonitorError, Position};
use crate::feed::BlockFeed;
use crate::metrics;
use crate::publisher::{EpochPublisher, PublishError};
use crate::window::Confirm;

/// How long a start waits for the first boundary before it resumes from
/// the cursor file. The sealer emits a boundary every tick (2 s in the
/// deploy), so the wait holds a session connect and several ticks.
pub const START_WAIT: Duration = Duration::from_secs(20);

/// What confirms a published epoch.
pub(super) enum Confirmation {
    /// No sealer feed: a publish confirms its epoch. The cursor file holds
    /// the last published block.
    Publish,
    /// The sealer's boundaries: the L1 origin of the last boundary, `None`
    /// before the first.
    Sealer(watch::Receiver<Option<u64>>),
}

impl Confirmation {
    /// The next origin a boundary carries. It never resolves without a
    /// sealer feed, or after the feed closed.
    pub(super) async fn changed(&mut self) -> u64 {
        let Self::Sealer(origins) = self else {
            return std::future::pending().await;
        };
        if origins.changed().await.is_err() {
            return std::future::pending().await;
        }
        origins.borrow_and_update().unwrap_or(0)
    }

    /// The origin of the first boundary, or `None` when none arrives
    /// within [`START_WAIT`].
    async fn first(&mut self) -> Option<u64> {
        let Self::Sealer(origins) = self else {
            return None;
        };
        let first = tokio::time::timeout(START_WAIT, origins.wait_for(Option::is_some)).await;
        first.ok()?.ok().and_then(|origin| *origin)
    }
}

impl<F: BlockFeed, P: EpochPublisher> L1Watcher<F, P> {
    /// Follow the sealer's boundaries. `origins` holds the L1 origin of the
    /// last boundary, `None` before the first.
    #[must_use]
    pub fn following(self, origins: watch::Receiver<Option<u64>>) -> Self {
        Self {
            confirmation: Confirmation::Sealer(origins),
            ..self
        }
    }

    /// Wait for the first boundary, then resume after its origin (see
    /// [`Self::start_at`]). An explicit resume block wins, so the watcher
    /// does not wait for it. `Break` means shutdown came first.
    pub(super) async fn follow_start(
        &mut self,
        shutdown: &mut oneshot::Receiver<()>,
    ) -> ControlFlow<()> {
        if matches!(self.position, Position::After(_))
            || matches!(self.confirmation, Confirmation::Publish)
        {
            return ControlFlow::Continue(());
        }
        let first = tokio::select! {
            biased;
            _ = shutdown => return ControlFlow::Break(()),
            first = self.confirmation.first() => first,
        };
        self.start_at(first);
        ControlFlow::Continue(())
    }

    /// Resume after the sealer's origin S, the `first` boundary's:
    ///
    /// - S > 0: resume after S, whatever the cursor file holds. A file
    ///   ahead of S (a sealer fleet seeded at an older origin) would skip
    ///   the epochs after S; a file behind S publishes copies the sealer
    ///   drops. The watcher takes S's hash from the `l1_blocks` record of
    ///   S, and replays the stream from S + 1; the next block must
    ///   descend from S. This also replaces a wrong hash in the file.
    ///   With no record of S, the watcher waits for it: it never
    ///   publishes without the parent check.
    /// - S = 0: the sealer holds no epoch, and accepts any first one. The
    ///   watcher resumes from the file, or at the first record.
    /// - No boundary within [`START_WAIT`]: resume from the file. The
    ///   file holds an origin the sealer confirmed, so it is at or behind
    ///   the sealer's origin, except after a seed at an older origin. The
    ///   watcher follows the first boundary that arrives, so either case
    ///   heals: copies are dropped, and an origin below the file anchors
    ///   the watcher again.
    pub fn start_at(&mut self, first: Option<u64>) {
        let stored = self.confirmed().map(|c| c.number);
        match first.map(NonZeroU64::new) {
            Some(Some(origin)) => {
                info!(
                    target: "da_watcher",
                    sealer_origin = origin.get(),
                    ?stored,
                    "resuming after the sealer's L1 origin"
                );
                self.resume_after(origin.into());
            }
            Some(None) => info!(
                target: "da_watcher",
                ?stored,
                "the sealer holds no epoch yet; resuming from the cursor file, or at the \
                 first record"
            ),
            None => warn!(
                target: "da_watcher",
                ?stored,
                wait_secs = START_WAIT.as_secs(),
                "no boundary from the sealer within the start wait; resuming from the cursor \
                 file, and following the sealer's L1 origin once a boundary arrives"
            ),
        }
    }

    /// Apply the L1 origin a boundary carries. An origin in the published
    /// range confirms the epochs up to it. An origin outside it anchors
    /// the watcher at the origin: its record gives its hash. Origin 0
    /// confirms nothing: the sealer holds no epoch yet.
    pub fn on_sealer_origin(&mut self, origin: u64) {
        let Some(block) = NonZeroU64::new(origin) else {
            return;
        };
        let followed = match &mut self.position {
            Position::Anchored(window, _) => window.confirm(origin) == Confirm::Inside,
            Position::After(after) => after.block() == origin,
            Position::Tip => false,
        };
        if !followed {
            info!(
                target: "da_watcher",
                sealer_origin = origin,
                published = ?self.cursor(),
                "the sealer's L1 origin is outside the published range; anchoring at it"
            );
            self.resume_after(block.into());
        }
        self.record_window();
    }

    /// Sleep until `due`; never wake when there is nothing due.
    pub(super) async fn sleep_until(due: Option<Instant>) {
        match due {
            Some(due) => tokio::time::sleep_until(due).await,
            None => std::future::pending().await,
        }
    }

    /// When the next re-publish is due: `None` while no epoch waits.
    pub(super) fn republish_at(&self) -> Option<Instant> {
        match &self.position {
            Position::Anchored(window, _) => window.republish_at(),
            Position::Tip | Position::After(_) => None,
        }
    }

    /// Publish the unconfirmed epochs again, in L1 order, when no boundary
    /// confirmed one for [`REPUBLISH_AFTER`]. The pass stops at the first
    /// epoch that does not publish, and runs again one period later.
    /// Returns how many epochs it published.
    ///
    /// # Errors
    /// [`MonitorError::PublisherClosed`] if the publisher transport is
    /// shut.
    pub fn republish_due(&mut self) -> Result<usize, MonitorError> {
        let now = Instant::now();
        let Position::Anchored(window, _) = &mut self.position else {
            return Ok(0);
        };
        if window.republish_at().is_none_or(|due| due > now) {
            return Ok(0);
        }
        // A log field. The window holds an epoch past the base, so the base
        // is below `u64::MAX`.
        let from = window.base().number.saturating_add(1);
        let sent = window.republish().try_fold(0usize, |sent, epoch| {
            Self::publish_again(&self.publisher, epoch, sent)
        });
        let sent = match sent {
            Ok(sent) => sent,
            Err((_, PublishError::Closed)) => return Err(MonitorError::PublisherClosed),
            Err((sent, e)) => {
                warn!(target: "da_watcher", error = %e, "re-publish stopped; it runs again later");
                sent
            }
        };
        ::metrics::counter!(metrics::EPOCHS_REPUBLISHED_TOTAL).increment(sent as u64);
        warn!(
            target: "da_watcher",
            from,
            republished = sent,
            timeout_secs = REPUBLISH_AFTER.as_secs(),
            "no boundary confirmed the published epochs; published them again"
        );
        Ok(sent)
    }

    /// Publish one epoch again. `sent` counts the epochs before it, at
    /// most one window, so the `saturating_add` never saturates.
    fn publish_again(
        publisher: &P,
        epoch: &EpochRecord,
        sent: usize,
    ) -> Result<usize, (usize, PublishError)> {
        publisher
            .publish(epoch)
            .map(|_| sent.saturating_add(1))
            .map_err(|e| (sent, e))
    }

    /// Only a closed publisher stops the loop after a re-publish.
    pub(super) fn report_republish(outcome: &Result<usize, MonitorError>) -> ControlFlow<()> {
        match outcome {
            Err(MonitorError::PublisherClosed) => {
                warn!(target: "da_watcher", "publisher closed; exiting");
                ControlFlow::Break(())
            }
            _ => ControlFlow::Continue(()),
        }
    }

    /// Keep a published epoch in the window. Without a sealer feed, the
    /// publish confirms it at once.
    pub(super) fn keep_published(&mut self, epoch: EpochRecord) {
        let Position::Anchored(window, _) = &mut self.position else {
            return;
        };
        let number = epoch.l1_number;
        window.push(epoch);
        if matches!(self.confirmation, Confirmation::Publish) {
            window.confirm(number);
        }
    }

    /// Export the confirmed origin and the unconfirmed epoch count.
    #[allow(
        clippy::cast_precision_loss,
        reason = "metric values; an L1 block number and a window length never near 2^52"
    )]
    pub(super) fn record_window(&self) {
        let Position::Anchored(window, _) = &self.position else {
            return;
        };
        ::metrics::gauge!(metrics::L1_CONFIRMED_ORIGIN).set(window.base().number as f64);
        ::metrics::gauge!(metrics::EPOCHS_UNCONFIRMED).set(window.len() as f64);
    }
}

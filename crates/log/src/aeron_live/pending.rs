//! The parked publishes of the Aeron thread. The retry rule (one offer
//! for each frame and pass, FIFO for each publication, a deadline) is
//! [`ParkedQueue`]. This module adapts a publish of the Aeron thread to it:
//! its offer on the publication table, and its acknowledgement.

use std::time::Instant;

use crossbeam_channel::Sender as CbSender;
use rkyv::util::AlignedVec;
use tracing::warn;

use super::table_pub::TablePub;
use crate::error::LogError;
use crate::parked::{Offered, Parked, ParkedQueue};
use crate::term_layout::TermLayout;
use kardamom_types::BPosition;

/// One row in the Aeron thread's publication table: the publication plus
/// its term layout, read once when the publication was opened (see
/// [`TermLayout::from_publication`]), so an offer decode never re-derives
/// it.
pub(super) struct PubEntry {
    pub(super) publication: TablePub,
    pub(super) layout: TermLayout,
    /// The Aeron stream id, the label of a dropped best-effort frame.
    pub(super) stream_id: i32,
}

impl PubEntry {
    /// One offer of `bytes`.
    pub(super) fn offer(&self, bytes: &[u8]) -> Offered {
        self.layout.offered(self.publication.offer(bytes))
    }
}

/// Best-effort frames the Aeron thread gave up on, by stream id. A
/// best-effort publisher never learns of a drop: this counter is the
/// only trace of one, so a dashboard reads it per stream.
pub const BEST_EFFORT_DROPPED_TOTAL: &str = "kardamom_log_best_effort_dropped_total";

/// Where the Aeron thread sends the outcome of an acknowledged publish:
/// to a thread that blocks on it, or to a task that awaits it. The task
/// form costs no blocking-pool hop on the caller side.
pub(super) enum PublishAck {
    Blocking(CbSender<Result<BPosition, LogError>>),
    Async(tokio::sync::oneshot::Sender<Result<BPosition, LogError>>),
}

impl PublishAck {
    /// Send the outcome. A caller that gave up has dropped its receiver,
    /// and the outcome then has no reader.
    pub(super) fn send(self, outcome: Result<BPosition, LogError>) {
        match self {
            Self::Blocking(ack) => {
                let _ = ack.send(outcome);
            }
            Self::Async(ack) => {
                let _ = ack.send(outcome);
            }
        }
    }
}

/// A publish awaiting delivery on the Aeron thread.
///
/// Why this queue exists: the Aeron thread is single-threaded and shared
/// by every publication and subscription in a process. A blocking offer
/// (spinning or sleeping in place, up to
/// [`OFFER_TIMEOUT`](crate::offer_retry::OFFER_TIMEOUT)) would stop that
/// thread from polling its subscriptions for the whole back-pressure
/// window. In the cluster that would starve the executor's `tx_ordering`
/// subscription long enough (more than Aeron's minimum flow-control
/// receiver timeout, about 2 s) that the sealer drops it from flow control
/// and advances. The subscription's image would then develop an
/// unfillable gap and go end-of-stream: a permanent freeze, since the
/// executor uses `no_unavailable_image_handler` and never re-subscribes.
/// A must-deliver publish must never starve a must-deliver subscribe.
///
/// So a back-pressured offer is parked here and retried one attempt per
/// loop iteration instead of blocking. The poll loop keeps draining
/// subscriptions between attempts. Per-publication FIFO order is
/// preserved: a publication with an older pending frame is not skipped
/// ahead.
pub(super) struct PendingPublish {
    pub(super) pub_id: u32,
    pub(super) bytes: AlignedVec,
    /// `Some` for an acknowledged publish (`publish_bytes` or
    /// `publish_bytes_async`); `None` for best effort.
    pub(super) ack: Option<PublishAck>,
    /// Give up (ack an error, or log) once this instant passes. Bounds a
    /// publish to a never-connecting subscriber.
    pub(super) deadline: Instant,
    /// The stream of `pub_id`, read when the frame was queued. `None` for
    /// an unknown publication id, which fails on its first offer.
    pub(super) stream_id: Option<i32>,
}

/// One pass over the parked publishes: one offer for each, on its entry in
/// `pubs`. Returns the offers that Aeron refused, which stay queued. The
/// pass never spins or sleeps, so the poll loop of the Aeron thread goes
/// straight back to its subscriptions (see [`PendingPublish`]).
pub(super) fn drain_pending(
    pubs: &[Option<PubEntry>],
    pending: &mut ParkedQueue<PendingPublish>,
) -> usize {
    pending.offer_each(Instant::now(), |item| item.offer_on(pubs))
}

impl PendingPublish {
    /// Attempt one offer against this item's entry in `pubs`. Reports
    /// [`Offered::UnknownPublication`] when `pub_id` has no entry: it is
    /// closed, or lent to a [`super::LentPublication`].
    fn offer_on(&self, pubs: &[Option<PubEntry>]) -> Offered {
        pubs.get(self.pub_id as usize)
            .and_then(Option::as_ref)
            .map_or(Offered::UnknownPublication, |entry| {
                entry.offer(self.bytes.as_slice())
            })
    }
}

impl Parked for PendingPublish {
    fn publication(&self) -> u32 {
        self.pub_id
    }

    fn deadline(&self) -> Instant {
        self.deadline
    }

    /// Ack the position. A best-effort publish has no ack.
    fn delivered(&mut self, position: Result<BPosition, LogError>) {
        if let Some(ack) = self.ack.take() {
            ack.send(position);
        }
    }

    /// Ack the error to a must-deliver caller, so it can decide to publish
    /// again. A best-effort publish has no caller to tell: it logs and
    /// counts the drop for its stream.
    fn failed(&mut self, reason: String) {
        if let Some(ack) = self.ack.take() {
            ack.send(Err(LogError::Aeron(reason)));
            return;
        }
        warn!(pub_id = self.pub_id, "best-effort publish failed: {reason}");
        if let Some(stream_id) = self.stream_id {
            metrics::counter!(BEST_EFFORT_DROPPED_TOTAL, "stream_id" => stream_id.to_string())
                .increment(1);
        }
    }
}

#[cfg(test)]
#[path = "pending_tests.rs"]
mod tests;

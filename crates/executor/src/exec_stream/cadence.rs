//! The cursor cadence: when the publisher thread sends the recorded cursor
//! to the sealer.
//!
//! The sealer keeps the best recorded cursor over the executors as the
//! floor of its record-lag guard. One cursor frame per executor every
//! [`SEND_EVERY`] keeps that floor close to the recording position at a
//! small cost to the replicated log. A cursor that moves by
//! [`SEND_AFTER_RECORDS`] records goes out at once, so a fast stream does
//! not wait for the timer.
//!
//! A sent cursor never moves down, and it never passes the recording
//! position: the publisher sends only [`RecordedCursor::through`] values.
//!
//! [`RecordedCursor::through`]: super::cursor::RecordedCursor::through

use std::time::{Duration, Instant};

use crossbeam_channel::{Receiver, Sender, bounded};
use kardamom_cluster_adapter::gateway::{ClusterIngress, OfferOutcome};
use kardamom_engine::reader::cluster::RecordedCursorPublisher;
use tracing::{info, warn};

/// The time between two sends of a cursor that moves slowly. A refused
/// send also waits this long before the next try.
pub(crate) const SEND_EVERY: Duration = Duration::from_millis(100);

/// A cursor that moved this many records past the last sent cursor goes
/// out at once.
pub(crate) const SEND_AFTER_RECORDS: u64 = 1024;

/// When the next cursor is due.
#[derive(Debug)]
pub(crate) struct CursorCadence {
    /// The last cursor that the cluster session took. `None` before the
    /// first one.
    sent: Option<u64>,
    /// When the last send was tried. `None` before the first try.
    tried: Option<Instant>,
}

impl CursorCadence {
    pub(crate) fn new() -> Self {
        Self {
            sent: None,
            tried: None,
        }
    }

    /// The cursor to send at `now`, if one is due. A cursor is due when it
    /// is above the last sent cursor, and [`SEND_EVERY`] passed since the
    /// last try or the cursor moved by [`SEND_AFTER_RECORDS`] or more. The
    /// first cursor is due at the first try.
    pub(crate) fn due(&self, through: Option<u64>, now: Instant) -> Option<u64> {
        let through = through?;
        if self.sent.is_some_and(|sent| through <= sent) {
            return None;
        }
        let waited = self
            .tried
            .is_none_or(|at| now.saturating_duration_since(at) >= SEND_EVERY);
        let far = self.sent.is_some_and(|sent| {
            through
                .checked_sub(sent)
                .is_some_and(|moved| moved >= SEND_AFTER_RECORDS)
        });
        (waited || far).then_some(through)
    }

    /// One send of `through` at `now` ended. `taken` tells whether the
    /// cluster session took the frame.
    pub(crate) fn tried(&mut self, through: u64, taken: bool, now: Instant) {
        self.tried = Some(now);
        if taken {
            self.sent = Some(through);
        }
    }
}

/// The hand-off of the recorded-cursor publisher to the publisher thread.
///
/// The publisher thread starts before the executor's cluster session is
/// up, so the publisher arrives later through this hand-off. A hand-off
/// that ends with no publisher leaves the cursor off: the thread then
/// sends nothing.
pub struct CursorHandoff<I: ClusterIngress> {
    tx: Sender<RecordedCursorPublisher<I>>,
}

impl<I: ClusterIngress> CursorHandoff<I> {
    /// A hand-off, and the cursor sender of the publisher thread that it
    /// feeds.
    pub(crate) fn new() -> (Self, CursorSender<I>) {
        let (tx, rx) = bounded(1);
        (Self { tx }, CursorSender::new(rx))
    }

    /// Give the publisher thread `publisher`, or end the hand-off with
    /// none. With no publisher the executor sends no recorded cursor.
    pub fn start(self, publisher: Option<RecordedCursorPublisher<I>>) {
        let Some(publisher) = publisher else {
            info!("exec stream: the recorded cursor is off; this executor sends no cursor");
            return;
        };
        if self.tx.send(publisher).is_err() {
            warn!("exec stream: the publisher thread ended before the recorded cursor started");
        }
    }
}

/// The publisher thread's side of the recorded cursor: the publisher,
/// once it arrives, and the cadence.
pub(crate) struct CursorSender<I: ClusterIngress> {
    handoff: Receiver<RecordedCursorPublisher<I>>,
    publisher: Option<RecordedCursorPublisher<I>>,
    cadence: CursorCadence,
    /// Whether the last send was refused. A refusal logs once, and the
    /// next taken send logs the end of it.
    refused: bool,
}

impl<I: ClusterIngress> CursorSender<I> {
    fn new(handoff: Receiver<RecordedCursorPublisher<I>>) -> Self {
        Self {
            handoff,
            publisher: None,
            cadence: CursorCadence::new(),
            refused: false,
        }
    }

    /// Send `through` when the cadence says it is due. Before the
    /// publisher arrives, and after a hand-off with none, this sends
    /// nothing.
    pub(crate) fn tick(&mut self, through: Option<u64>, now: Instant) {
        let Some(due) = self.cadence.due(through, now) else {
            return;
        };
        let Some(publisher) = self.publisher() else {
            return;
        };
        let outcome = publisher.publish(due);
        self.cadence
            .tried(due, outcome == OfferOutcome::Accepted, now);
        self.log(due, outcome);
    }

    /// The publisher, after it arrived on the hand-off.
    fn publisher(&mut self) -> Option<&mut RecordedCursorPublisher<I>> {
        if self.publisher.is_none() {
            self.publisher = self.handoff.try_recv().ok();
        }
        self.publisher.as_mut()
    }

    /// Log the first refusal, and the first taken send after it.
    fn log(&mut self, through: u64, outcome: OfferOutcome) {
        let refused = outcome != OfferOutcome::Accepted;
        if refused && !self.refused {
            warn!(
                through,
                ?outcome,
                "exec stream: the cluster session refuses the recorded cursor; the next tick retries"
            );
        }
        if !refused && self.refused {
            info!(
                through,
                "exec stream: the cluster session takes the recorded cursor again"
            );
        }
        self.refused = refused;
    }
}

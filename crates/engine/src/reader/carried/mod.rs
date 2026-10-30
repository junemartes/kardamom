//! [`CarriedSubscription`]: the canonical stream with the bytes of each
//! transaction, read from the member feed or the serving service.
//!
//! The reader takes each frame through the frame rules of one request
//! ([`answer`]), then through the pending set that the cluster egress
//! reader uses too ([`Reorder`]). So a record waits for the boundary of its
//! block until the replay of the newest request reaches the head, also
//! across a reconnect or a switch of the source. A record of a lossy
//! source, such as the egress, always waits for its boundary.
//!
//! Before it delivers a frame, a reader with a committed history compares
//! the stream with that history ([`compare`]). A difference ends the run
//! with the divergence point. A new connection after the comparison ends
//! the run too: the stream of the new connection can differ from the
//! entries in flight, so the run starts again from the state DB, and the
//! comparison at its start also checks the receipts that the run published
//! above the committed block.
//!
//! A reader that asks again, and still does not move its cursor for the
//! stall window, starts the run again from the state DB too. A source can
//! answer each request and keep the reader busy, so no fault of the source
//! fires.

mod answer;
mod compare;
mod egress;
mod feed;
#[cfg(all(test, feature = "docker-e2e"))]
mod feed_tests;
#[cfg(test)]
mod memory;
mod switch;
#[cfg(test)]
pub(super) mod tests;
#[cfg(test)]
mod tests_compare;
#[cfg(test)]
mod tests_late;
#[cfg(test)]
mod tests_retention;
#[cfg(test)]
mod tests_switch;
mod transport;

use std::time::{Duration, Instant, SystemTime};

use kardamom_types::{BPosition, BlockBoundaryStart};

pub use compare::{LocalHistory, PublishedTail};
pub use egress::EgressTransport;
pub use feed::AeronFeed;
pub use transport::{NoSource, Poll, Request, Transport};

use answer::{Fault, Step, Timing, Wake};
use compare::{Compare, Progress};
#[cfg(test)]
use memory::{MemoryRemote, MemoryTransport};
#[cfg(test)]
use switch::Source;
use switch::{Switch, Verdict};

use super::cluster::{BoundaryMetrics, Reorder, ReplayCursor};
use super::tee::Forward;
use super::{FrameTee, Ordered, TxOrderingSubscription};
use crate::ExecutorError;

/// The canonical stream with its bytes, from the feed `F` or the service
/// `S`. The feed comes first. See [`Switch`] for the order of the sources.
pub struct CarriedSubscription<F, S> {
    reorder: Reorder,
    /// The comparison with the committed history, until it ends. While it
    /// runs, every frame goes to it, and the reader delivers nothing.
    compare: Option<Compare>,
    /// The cursor of a reader with no committed history to compare, as it
    /// started. While the delivery cursor is still there, nothing is in
    /// flight, so a new connection needs no restart.
    idle_start: Option<(u64, u64)>,
    switch: Switch<F, S>,
    timing: Timing,
    woke_at: Instant,
    /// The frames for the egress, when this reader forwards them.
    tee: Forward,
    /// The gauge of the canonical index of the newest record that the
    /// reader delivered, when the binary names one.
    cursor_gauge: Option<metrics::Gauge>,
    watch: Watch,
}

/// The progress of the cursor, for the stall window.
struct Watch {
    /// The cursor at the start of the window.
    cursor: (u64, u64),
    since: Instant,
    /// The requests since the start of the window.
    asks: u32,
    /// The source sent a frame since the start of the window.
    heard: bool,
}

impl Watch {
    fn new(cursor: (u64, u64), now: Instant) -> Self {
        Self {
            cursor,
            since: now,
            asks: 0,
            heard: false,
        }
    }

    /// Start the window again when the cursor moved.
    fn see(&mut self, cursor: (u64, u64), now: Instant) {
        if cursor != self.cursor {
            *self = Self::new(cursor, now);
        }
    }

    fn asked(&mut self) {
        self.asks = self.asks.saturating_add(1);
    }

    /// Whether the window is over, the source sent frames in it, and the
    /// reader asked again. A source that sends nothing has the silence
    /// window instead. The first request of a window can find a source
    /// with nothing new, such as a chain at rest after a new connection.
    fn stuck(&self, now: Instant, stall: Duration) -> bool {
        self.heard && self.asks >= 2 && now.duration_since(self.since) >= stall
    }
}

impl<F: Transport, S: Transport> CarriedSubscription<F, S> {
    /// A reader of `feed`, then `service`, from `cursor`, with the default
    /// waits.
    pub fn new(feed: F, service: S, cursor: ReplayCursor) -> Self {
        Self::with_timing(feed, service, cursor, Timing::default())
    }

    /// A reader of `feed`, then `service`, that first compares the stream
    /// with `history`, and then delivers from the end of the history.
    pub fn compared(feed: F, service: S, history: LocalHistory) -> Self {
        Self::compared_with_timing(feed, service, history, Timing::default())
    }

    /// [`Self::compared`] with the waits of `timing`.
    pub(crate) fn compared_with_timing(
        feed: F,
        service: S,
        history: LocalHistory,
        timing: Timing,
    ) -> Self {
        let (index, block) = history.cursor();
        let mut reader = Self::with_timing(feed, service, ReplayCursor::new(index, block), timing);
        let compare = Compare::new(history);
        tracing::info!(
            from_index = compare.cursor().0,
            from_block = compare.cursor().1,
            to_index = index,
            to_block = block,
            "comparing the stream with the committed history"
        );
        reader.compare = Some(compare);
        reader.idle_start = None;
        reader
    }

    /// A reader with the waits of `timing`. The request ids start at the
    /// clock, so an answer to a request of an earlier run does not name a
    /// request of this run.
    pub(crate) fn with_timing(feed: F, service: S, cursor: ReplayCursor, timing: Timing) -> Self {
        let first_id = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .map_or(0, |since| {
                u64::try_from(since.as_nanos() & u128::from(u64::MAX)).unwrap_or(0)
            });
        let mut reorder = Reorder::new(cursor);
        reorder.restart();
        let now = Instant::now();
        Self {
            watch: Watch::new(reorder.next(), now),
            idle_start: Some(reorder.next()),
            reorder,
            compare: None,
            switch: Switch::new(feed, service, timing, first_id),
            timing,
            woke_at: now,
            tee: Forward::default(),
            cursor_gauge: None,
        }
    }

    /// Set the gauge `name` to the canonical index of each record that the
    /// reader delivers. A boundary does not move it.
    #[must_use]
    pub fn with_cursor_gauge(mut self, name: &'static str) -> Self {
        self.cursor_gauge = Some(metrics::gauge!(name));
        self
    }

    /// Export the head under the validator's name, not as the
    /// `kardamom_sealer_*` series: only the executor emits that series.
    #[must_use]
    pub fn suppress_sealer_metrics(mut self) -> Self {
        self.reorder.metrics = BoundaryMetrics::Validator;
        self
    }

    /// Forward each delivered frame to the egress through `tee`.
    #[must_use]
    pub fn with_tee(mut self, tee: FrameTee) -> Self {
        self.tee = Forward::on(tee);
        self
    }

    /// The source that the reader reads now.
    #[cfg(test)]
    pub(crate) fn source(&self) -> Source {
        self.switch.active()
    }

    /// One frame of the active source, or the timers when none comes.
    fn step(&mut self) -> Result<(), ExecutorError> {
        match self.switch.poll(self.timing.poll) {
            Poll::Frame(bytes) => {
                self.watch.heard = true;
                self.tee.hold(&bytes);
                let step = self.switch.frame(&bytes, Instant::now());
                self.apply(step)?;
            }
            Poll::Idle => {}
            Poll::Closed => return Err(ExecutorError::TxOrderingClosed),
        }
        self.wake(Instant::now())?;
        self.renewal(Instant::now())?;
        self.settle()?;
        self.watchdog(Instant::now())
    }

    /// Apply the step of one frame.
    fn apply(&mut self, step: Step) -> Result<(), ExecutorError> {
        match step {
            Step::Record(index, item) => self.active().ingest_record(index, item),
            Step::Boundary(b) => self.boundary(b),
            Step::Live {
                up_to_index,
                up_to_block,
            } => {
                self.end_replay(up_to_index, up_to_block)?;
                self.switch.live();
                Ok(())
            }
            Step::Drop => Ok(()),
            Step::Ask => {
                self.ask(Instant::now());
                Ok(())
            }
            Step::Fault(fault) => self.fault(fault, Instant::now()),
        }
    }

    /// Buffer a boundary. A boundary that seals below the cursor came late:
    /// the reader delivered records past its end. No answer from the cursor
    /// can put it back in order, so the run starts again from the state DB.
    fn boundary(&mut self, b: BlockBoundaryStart) -> Result<(), ExecutorError> {
        let restarted = self.restarted();
        let block = b.block_number;
        match self.active().ingest_boundary(b) {
            Err(ExecutorError::BoundaryMisaligned { .. }) => {
                tracing::warn!(
                    block,
                    "a late boundary seals below the cursor; restarting from the state DB"
                );
                Err(restarted)
            }
            other => other,
        }
    }

    /// The pending set that takes the frames now: the comparison while it
    /// runs, else the delivery.
    fn active(&mut self) -> &mut Reorder {
        match self.compare.as_mut() {
            Some(compare) => compare.reorder(),
            None => &mut self.reorder,
        }
    }

    /// The cursor of the next request.
    fn cursor(&self) -> (u64, u64) {
        self.compare
            .as_ref()
            .map_or_else(|| self.reorder.next(), Compare::cursor)
    }

    /// The error that starts the run again from the state DB.
    fn restarted(&self) -> ExecutorError {
        let (next_index, next_block) = self.reorder.next();
        ExecutorError::SourceRestarted {
            next_index,
            next_block,
        }
    }

    /// The replay of the newest request reached the head of the source. A
    /// head below the delivery cursor means the source lost entries that
    /// this reader delivered: the run starts again from the state DB, and
    /// the comparison finds the divergence. The comparison reads on past
    /// the head, as the source closes new blocks.
    ///
    /// The live frames of a lossy source can lose a boundary with no hole
    /// in the indexes. So for such a source, the pending set stays in the
    /// catch-up: a record waits for the boundary of its block.
    fn end_replay(&mut self, up_to_index: u64, up_to_block: u64) -> Result<(), ExecutorError> {
        let restarted = self.restarted();
        match self.compare.as_mut() {
            Some(compare) => compare.reorder().end_replay(up_to_index, up_to_block),
            None => self
                .reorder
                .end_replay(up_to_index, up_to_block)
                .map_err(|_| restarted),
        }?;
        if self.switch.lossy() {
            self.active().catch_up();
        }
        Ok(())
    }

    /// A request went out on a new connection. The comparison starts again
    /// from its floor. After the comparison, the run ends, unless the
    /// reader has no history and delivered nothing yet.
    fn renewal(&mut self, now: Instant) -> Result<(), ExecutorError> {
        if !self.switch.take_renewed() || self.idle_start == Some(self.reorder.next()) {
            return Ok(());
        }
        let Some(compare) = self.compare.as_mut() else {
            tracing::warn!("the stream source has a new connection; restarting from the state DB");
            return Err(self.restarted());
        };
        tracing::info!("the stream source has a new connection; comparing again from the floor");
        compare.rewind();
        self.ask(now);
        self.switch.take_renewed();
        Ok(())
    }

    /// Compare the frames that the comparison has, and end it when it
    /// reaches the delivery cursor.
    fn settle(&mut self) -> Result<(), ExecutorError> {
        let Some(compare) = self.compare.as_mut() else {
            return Ok(());
        };
        let progress = compare.drain(&mut self.tee)?;
        self.close_compare(progress)
    }

    /// Act on the progress of the comparison.
    fn close_compare(&mut self, progress: Progress) -> Result<(), ExecutorError> {
        let settled = progress.settle().inspect_err(|err| {
            tracing::error!(error = %err, "the stream diverges from the committed history");
        })?;
        let Some(settled) = settled else {
            return Ok(());
        };
        settled.log();
        if let Some(compare) = self.compare.take().filter(|_| progress == Progress::Equal) {
            compare.forward(&mut self.tee);
        }
        self.ask(Instant::now());
        Ok(())
    }

    /// The timers, at most once in each poll wait: the request of a new
    /// connection, the resend, the silence, and the return to the feed.
    fn wake(&mut self, now: Instant) -> Result<(), ExecutorError> {
        if now.duration_since(self.woke_at) < self.timing.poll {
            return Ok(());
        }
        self.woke_at = now;
        match self.switch.wake(now) {
            Wake::Wait => {}
            Wake::Ask => self.ask(now),
            Wake::Fault(fault) => self.fault(fault, now)?,
        }
        if self.switch.check_return(self.cursor(), now) {
            self.active().restart();
        }
        Ok(())
    }

    /// Ask the active source again from the cursor. The answer carries
    /// every frame above the cursor, so the pending set starts again.
    fn ask(&mut self, now: Instant) {
        self.active().restart();
        self.switch.ask(self.cursor(), now);
        self.watch.asked();
    }

    /// Start the run again from the state DB when the reader asked again
    /// and the cursor stayed for the stall window. Such a source answers,
    /// so no fault of it fires, but the run from the state DB is the only
    /// way past it. The comparison has its own waits for the head.
    fn watchdog(&mut self, now: Instant) -> Result<(), ExecutorError> {
        self.watch.see(self.cursor(), now);
        if self.compare.is_some() || !self.watch.stuck(now, self.timing.stall) {
            return Ok(());
        }
        let (next_index, next_block) = self.reorder.next();
        tracing::warn!(
            next_index,
            next_block,
            "the stream does not move past the cursor; restarting from the state DB"
        );
        Err(self.restarted())
    }

    /// Leave the active source. While the comparison runs, a head below
    /// its cursor and a refused cursor do not leave the source.
    fn fault(&mut self, fault: Fault, now: Instant) -> Result<(), ExecutorError> {
        match (fault, self.compare.as_mut()) {
            (Fault::Ahead { head_block, .. }, Some(compare)) => {
                // The resend timer asks again: the source closes a block on
                // each tick, so the head passes the cursor.
                tracing::info!(
                    head_block,
                    from_block = compare.cursor().1,
                    "the head is below the comparison cursor; asking again"
                );
                Ok(())
            }
            (
                Fault::Unavailable {
                    oldest_index,
                    oldest_block,
                },
                Some(compare),
            ) => {
                let progress = compare.refused(oldest_index, oldest_block);
                if progress == Progress::Pending {
                    self.ask(now);
                }
                self.close_compare(progress)
            }
            _ => self.leave(fault, now),
        }
    }

    /// Leave the active source for the other one.
    fn leave(&mut self, fault: Fault, now: Instant) -> Result<(), ExecutorError> {
        self.active().restart();
        match self.switch.fault(fault, self.cursor(), now) {
            Verdict::Switched => Ok(()),
            Verdict::Unavailable {
                oldest_index,
                oldest_block,
            } => Err(self.reorder.unavailable(oldest_index, oldest_block)),
            Verdict::Behind { .. } => Err(self.restarted()),
            Verdict::Unjoinable { index } => {
                tracing::error!(
                    index,
                    "no source holds the bytes of the transaction reference; stopping"
                );
                Err(ExecutorError::UnjoinableReference { index })
            }
            Verdict::Mismatch { index } => {
                tracing::error!(
                    index,
                    "no source holds the right bytes of the entry; stopping"
                );
                Err(ExecutorError::HashMismatch { index })
            }
        }
    }
}

impl<F: Transport, S: Transport> CarriedSubscription<F, S> {
    /// The next item to deliver, after the comparison ends.
    fn deliver(&mut self) -> Result<Option<(BPosition, Ordered)>, ExecutorError> {
        if self.compare.is_some() {
            return Ok(None);
        }
        self.reorder.try_deliver()
    }

    /// Set the cursor gauge to the index of a delivered record.
    fn show_cursor(&self, (position, item): &(BPosition, Ordered)) {
        if matches!(
            item,
            Ordered::Message(kardamom_types::TxOrderingMessage::BoundaryStart(_))
        ) {
            return;
        }
        let Some(gauge) = &self.cursor_gauge else {
            return;
        };
        #[allow(
            clippy::cast_precision_loss,
            reason = "canonical indexes stay far below 2^52; f64 represents them exactly"
        )]
        gauge.set(position.as_index() as f64);
    }
}

impl<F: Transport, S: Transport> TxOrderingSubscription for CarriedSubscription<F, S> {
    /// The stall window starts at each call: the wait of the caller
    /// between two calls is no stall of the stream.
    fn next(&mut self) -> Result<(BPosition, Ordered), ExecutorError> {
        self.watch = Watch::new(self.cursor(), Instant::now());
        loop {
            let Some(out) = self.deliver()? else {
                self.step()?;
                continue;
            };
            let out = self.tee.release(out);
            self.show_cursor(&out);
            return Ok(out);
        }
    }
}

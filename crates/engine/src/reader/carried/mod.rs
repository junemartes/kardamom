//! [`CarriedSubscription`]: the canonical stream with the bytes of each
//! transaction, read from the member feed or the serving service.
//!
//! The reader takes each frame through the frame rules of one request
//! ([`answer`]), then through the pending set that the cluster egress
//! reader uses too ([`Reorder`]). So a record waits for the boundary of its
//! block until the replay of the newest request reaches the head, also
//! across a reconnect or a switch of the source.

mod answer;
mod feed;
#[cfg(all(test, feature = "docker-e2e"))]
mod feed_tests;
#[cfg(test)]
mod memory;
mod switch;
#[cfg(test)]
mod tests;
#[cfg(test)]
mod tests_switch;
mod transport;

use std::time::{Instant, SystemTime};

use kardamom_types::BPosition;

pub use feed::AeronFeed;
pub use transport::{NoSource, Poll, Request, Transport};

use answer::{Fault, Step, Timing, Wake};
#[cfg(test)]
use memory::{MemoryRemote, MemoryTransport};
#[cfg(test)]
use switch::Source;
use switch::{Switch, Verdict};

use super::cluster::{Reorder, ReplayCursor};
use super::{Ordered, TxOrderingSubscription};
use crate::ExecutorError;

/// The canonical stream with its bytes, from the feed `F` or the service
/// `S`. The feed comes first. See [`Switch`] for the order of the sources.
pub struct CarriedSubscription<F, S> {
    reorder: Reorder,
    switch: Switch<F, S>,
    timing: Timing,
    woke_at: Instant,
}

impl<F: Transport, S: Transport> CarriedSubscription<F, S> {
    /// A reader of `feed`, then `service`, from `cursor`, with the default
    /// waits.
    pub fn new(feed: F, service: S, cursor: ReplayCursor) -> Self {
        Self::with_timing(feed, service, cursor, Timing::default())
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
        Self {
            reorder,
            switch: Switch::new(feed, service, timing, first_id),
            timing,
            woke_at: Instant::now(),
        }
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
                let step = self.switch.frame(&bytes, Instant::now());
                self.apply(step)?;
            }
            Poll::Idle => {}
            Poll::Closed => return Err(ExecutorError::TxOrderingClosed),
        }
        self.wake(Instant::now())
    }

    /// Apply the step of one frame.
    fn apply(&mut self, step: Step) -> Result<(), ExecutorError> {
        match step {
            Step::Record(index, item) => self.reorder.ingest_record(index, item),
            Step::Boundary(b) => self.reorder.ingest_boundary(b),
            Step::Live {
                up_to_index,
                up_to_block,
            } => {
                self.reorder.end_replay(up_to_index, up_to_block)?;
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
        if self.switch.check_return(self.reorder.next(), now) {
            self.reorder.restart();
        }
        Ok(())
    }

    /// Ask the active source again from the delivery cursor. The answer
    /// carries every frame above the cursor, so the pending set starts
    /// again.
    fn ask(&mut self, now: Instant) {
        self.reorder.restart();
        self.switch.ask(self.reorder.next(), now);
    }

    /// Leave the active source.
    fn fault(&mut self, fault: Fault, now: Instant) -> Result<(), ExecutorError> {
        self.reorder.restart();
        match self.switch.fault(fault, self.reorder.next(), now) {
            Verdict::Switched => Ok(()),
            Verdict::Unavailable {
                oldest_index,
                oldest_block,
            } => Err(self.reorder.unavailable(oldest_index, oldest_block)),
            Verdict::Behind {
                head_index,
                head_block,
            } => Err(self.reorder.behind_cursor(head_index, head_block)),
        }
    }
}

impl<F: Transport, S: Transport> TxOrderingSubscription for CarriedSubscription<F, S> {
    fn next(&mut self) -> Result<(BPosition, Ordered), ExecutorError> {
        loop {
            let Some(out) = self.reorder.try_deliver()? else {
                self.step()?;
                continue;
            };
            return Ok(out);
        }
    }
}

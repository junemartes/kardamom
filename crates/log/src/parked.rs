//! A queue of frames that wait for their publication.
//!
//! The rule: each pass makes at most one offer for each frame, oldest
//! first. Once a publication refuses an offer in a pass, its later frames
//! wait for the next pass, so a stream never reorders. A frame fails at
//! its deadline, also while it waits behind a refused frame.
//!
//! The Aeron thread keeps its parked publishes in this queue. A thread that
//! offers on a publication of its own uses the same queue, so the two
//! paths cannot drift apart. The offer itself is a closure of the caller,
//! so the rule is testable with no media driver.

use std::collections::{HashSet, VecDeque};
use std::time::Instant;

use kardamom_types::BPosition;

use crate::error::LogError;

/// A frame in a [`ParkedQueue`].
pub trait Parked {
    /// The publication of the frame. The frames of one publication keep
    /// their order.
    fn publication(&self) -> u32;

    /// The instant at which the frame fails if no offer took it.
    fn deadline(&self) -> Instant;

    /// The publication took the frame: the stream position after it, or
    /// the error of a position that does not decode. The frame was
    /// delivered in both cases, so a retry would publish it twice.
    fn delivered(&mut self, position: Result<BPosition, LogError>);

    /// The frame failed, for `reason`. The queue drops it after this call.
    fn failed(&mut self, reason: String);
}

/// The outcome of one offer of a [`Parked`] frame.
#[derive(Debug)]
pub enum Offered {
    /// The publication of the frame does not exist: a programming error or
    /// a use after a close. The frame fails at once.
    UnknownPublication,
    /// The publication took the frame. See [`Parked::delivered`].
    Delivered(Result<BPosition, LogError>),
    /// The publication refused the frame with this negative Aeron status
    /// (`BACK_PRESSURED`, `NOT_CONNECTED`, and so on). The frame waits
    /// for the next pass, or fails at its deadline.
    Refused(i64),
}

/// The frames that wait for their publication, oldest first.
#[derive(Debug)]
pub struct ParkedQueue<T> {
    frames: VecDeque<T>,
}

impl<T> Default for ParkedQueue<T> {
    fn default() -> Self {
        Self {
            frames: VecDeque::new(),
        }
    }
}

impl<T> FromIterator<T> for ParkedQueue<T> {
    fn from_iter<I: IntoIterator<Item = T>>(frames: I) -> Self {
        Self {
            frames: frames.into_iter().collect(),
        }
    }
}

impl<T: Parked> ParkedQueue<T> {
    /// An empty queue.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Put `frame` at the end of the queue.
    pub fn push(&mut self, frame: T) {
        self.frames.push_back(frame);
    }

    /// The frames in the queue.
    #[must_use]
    pub fn len(&self) -> usize {
        self.frames.len()
    }

    /// Whether the queue holds no frame.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.frames.is_empty()
    }

    /// The frames, oldest first.
    pub fn iter(&self) -> impl Iterator<Item = &T> {
        self.frames.iter()
    }

    /// One pass at `now`: offer each frame once through `offer`, by the
    /// rule of the module. A delivered or a failed frame leaves the queue.
    /// Returns the offers that a publication refused, at most one for each
    /// publication. Those frames stay for the next pass.
    ///
    /// The pass never waits. The caller polls its other work between two
    /// passes.
    pub fn offer_each<F>(&mut self, now: Instant, offer: F) -> usize
    where
        F: FnMut(&T) -> Offered,
    {
        if self.frames.is_empty() {
            return 0;
        }
        let mut pass = Pass::new(now, offer);
        // `retain_mut` visits the frames front to back once, and lets each
        // call take what it needs from a frame before it keeps or drops it.
        self.frames.retain_mut(|frame| pass.keep(frame));
        pass.refused
    }
}

/// The state of one [`ParkedQueue::offer_each`] pass: the offer of the
/// caller, and the publications that refused an offer in this pass.
struct Pass<F> {
    now: Instant,
    offer: F,
    blocked: HashSet<u32>,
    /// The offers of this pass that a publication refused.
    refused: usize,
}

impl<F> Pass<F> {
    fn new(now: Instant, offer: F) -> Self {
        Self {
            now,
            offer,
            blocked: HashSet::new(),
            refused: 0,
        }
    }

    /// Whether `frame` stays in the queue.
    fn keep<T>(&mut self, frame: &mut T) -> bool
    where
        T: Parked,
        F: FnMut(&T) -> Offered,
    {
        if self.blocked.contains(&frame.publication()) {
            return self.wait(frame);
        }
        self.try_offer(frame)
    }

    /// A frame behind a refused frame of its publication: it stays, unless
    /// its own deadline passed. Each pass checks the deadline of each
    /// frame, not only of the head. Otherwise the frames behind a blocked
    /// head would fail one after the other, each one deadline late, and a
    /// caller that already gave up on its frame could see it delivered.
    fn wait<T: Parked>(&self, frame: &mut T) -> bool {
        if self.now < frame.deadline() {
            return true;
        }
        frame.failed(
            "aeron offer failed: expired while queued behind a blocked publication".to_string(),
        );
        false
    }

    /// A frame whose publication refused nothing in this pass: one offer.
    fn try_offer<T>(&mut self, frame: &mut T) -> bool
    where
        T: Parked,
        F: FnMut(&T) -> Offered,
    {
        match (self.offer)(frame) {
            Offered::UnknownPublication => {
                frame.failed(format!("publish: unknown pub_id {}", frame.publication()));
                false
            }
            Offered::Delivered(position) => {
                // A later frame of the same publication can go in this pass.
                frame.delivered(position);
                false
            }
            Offered::Refused(code) if self.now >= frame.deadline() => {
                frame.failed(format!(
                    "aeron offer failed: {} ({code})",
                    crate::offer_retry::offer_code_str(code)
                ));
                false
            }
            Offered::Refused(_) => {
                // Hold this frame and each later frame of its publication
                // until the next pass.
                self.blocked.insert(frame.publication());
                self.refused += 1;
                true
            }
        }
    }
}

#[cfg(test)]
#[path = "parked_tests.rs"]
mod tests;

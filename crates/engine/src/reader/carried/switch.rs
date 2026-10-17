//! [`Switch`]: the order of the sources and the switch between them.
//!
//! The reader reads one source at a time. It reads the member feed first,
//! then the serving service. It leaves a source at a frame boundary: it
//! closes the old source and asks the new one for a replay from its
//! cursor. The frames at each index are equal on both sources, so the
//! cursor names the same point on both. When both sources refuse the
//! cursor, a peer checkpoint is the last source.

use std::time::{Duration, Instant};

use super::answer::{Answer, Fault, Step, Timing, Wake};
use super::transport::{Poll, Transport};

/// The frames that one return check reads from the feed, at most. The
/// check runs between two frames of the service, so it must not starve the
/// service.
const PROBE_BATCH: usize = 256;

/// A source of the reader.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Source {
    /// The feed of the paired member.
    Feed,
    /// The serving service.
    Service,
}

impl Source {
    /// The `source` label of the metrics.
    pub(crate) fn label(self) -> &'static str {
        match self {
            Self::Feed => "feed",
            Self::Service => "service",
        }
    }

    fn other(self) -> Self {
        match self {
            Self::Feed => Self::Service,
            Self::Service => Self::Feed,
        }
    }
}

/// The result of a fault of the active source.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Verdict {
    /// The reader reads the other source now.
    Switched,
    /// Both sources failed, and one refused the cursor below its floor:
    /// only a peer checkpoint at or above the floor can serve.
    Unavailable {
        oldest_index: u64,
        oldest_block: u64,
    },
    /// Both sources have a head below the cursor.
    Behind { head_index: u64, head_block: u64 },
}

/// The last fault of each source since it last reached its head.
#[derive(Debug, Default)]
struct Marks {
    feed: Option<Fault>,
    service: Option<Fault>,
}

impl Marks {
    fn slot(&mut self, source: Source) -> &mut Option<Fault> {
        match source {
            Source::Feed => &mut self.feed,
            Source::Service => &mut self.service,
        }
    }

    /// The verdict of two faults, one of each source. A source that cannot
    /// serve because of its floor wins: a peer checkpoint repairs it. Two
    /// heads below the cursor stop the reader. Any other pair is a fault
    /// that can pass, so the reader tries the other source again.
    fn verdict(&self) -> Verdict {
        let pair = [self.feed, self.service];
        let floor = pair
            .iter()
            .filter_map(|fault| match fault {
                Some(Fault::Unavailable {
                    oldest_index,
                    oldest_block,
                }) => Some((*oldest_block, *oldest_index)),
                _ => None,
            })
            .min();
        match (floor, pair) {
            (Some((oldest_block, oldest_index)), _) => Verdict::Unavailable {
                oldest_index,
                oldest_block,
            },
            (
                None,
                [
                    Some(Fault::Ahead {
                        head_index,
                        head_block,
                    }),
                    Some(Fault::Ahead { .. }),
                ],
            ) => Verdict::Behind {
                head_index,
                head_block,
            },
            _ => Verdict::Switched,
        }
    }
}

/// The check for the return to the feed, while the reader reads the
/// service.
#[derive(Default)]
struct Return {
    /// Since when the member is ready without a break.
    ready_since: Option<Instant>,
    /// The request on the feed that checks that its replay reaches the
    /// head. The reader drops the frames of its answer.
    probe: Option<Answer>,
}

/// The sources of one reader and the source that it reads.
pub(crate) struct Switch<F, S> {
    feed: F,
    service: S,
    timing: Timing,
    active: Source,
    answer: Answer,
    marks: Marks,
    back: Return,
    /// The id of the next request on either source. Ids are labels, so
    /// they wrap.
    next_id: u64,
}

impl<F: Transport, S: Transport> Switch<F, S> {
    pub(crate) fn new(feed: F, service: S, timing: Timing, first_id: u64) -> Self {
        let now = Instant::now();
        let switch = Self {
            feed,
            service,
            timing,
            active: Source::Feed,
            answer: Answer::new(timing, now),
            marks: Marks::default(),
            back: Return::default(),
            next_id: first_id,
        };
        switch.show();
        switch
    }

    #[cfg(test)]
    pub(crate) fn active(&self) -> Source {
        self.active
    }

    pub(crate) fn poll(&mut self, wait: Duration) -> Poll {
        match self.active {
            Source::Feed => self.feed.poll(wait),
            Source::Service => self.service.poll(wait),
        }
    }

    pub(crate) fn frame(&mut self, bytes: &[u8], now: Instant) -> Step {
        self.answer.frame(bytes, now)
    }

    /// The timers of the active source.
    pub(crate) fn wake(&mut self, now: Instant) -> Wake {
        let connected = match self.active {
            Source::Feed => self.feed.connected(),
            Source::Service => self.service.connected(),
        };
        self.answer.wake(connected, now)
    }

    fn take_id(&mut self) -> u64 {
        let id = self.next_id;
        self.next_id = self.next_id.wrapping_add(1);
        id
    }

    /// Send a new request from `cursor` to the active source.
    pub(crate) fn ask(&mut self, cursor: (u64, u64), now: Instant) {
        let id = self.take_id();
        let request = self.answer.ask(cursor, id, now);
        tracing::debug!(
            source = self.active.label(),
            from_index = request.from_index,
            from_block = request.from_block,
            request_id = request.request_id,
            "replay request"
        );
        match self.active {
            Source::Feed => self.feed.request(request),
            Source::Service => self.service.request(request),
        }
    }

    /// The replay of the active source reached its head.
    pub(crate) fn live(&mut self) {
        *self.marks.slot(self.active) = None;
    }

    /// The active source failed. Count it, then switch to the other
    /// source, unless both failed in a way that no source can repair.
    pub(crate) fn fault(&mut self, fault: Fault, cursor: (u64, u64), now: Instant) -> Verdict {
        tracing::warn!(source = self.active.label(), ?fault, "leaving the source");
        if let Fault::Mismatch { index } = fault {
            tracing::error!(
                source = self.active.label(),
                index,
                "the raw transaction does not hash to its canonical id; replaying from the other source"
            );
            metrics::counter!(
                crate::metrics::SOURCE_MISMATCH_TOTAL,
                "source" => self.active.label()
            )
            .increment(1);
        }
        *self.marks.slot(self.active) = Some(fault);
        let verdict = match self.marks.slot(self.active.other()) {
            Some(_) => self.marks.verdict(),
            None => Verdict::Switched,
        };
        if verdict == Verdict::Switched {
            self.switch_to(self.active.other(), cursor, now);
        }
        verdict
    }

    /// Close the active source, and read `to` from `cursor`.
    fn switch_to(&mut self, to: Source, cursor: (u64, u64), now: Instant) {
        match self.active {
            Source::Feed => self.feed.close(),
            Source::Service => self.service.close(),
        }
        tracing::info!(
            from = self.active.label(),
            to = to.label(),
            next_index = cursor.0,
            next_block = cursor.1,
            "switching the input source"
        );
        self.active = to;
        self.answer.reset(now);
        self.back = Return::default();
        self.show();
        if let Wake::Ask = self.wake(now) {
            self.ask(cursor, now);
        }
    }

    /// Set the input-source gauge: 1 for the active source, 0 for the
    /// other.
    fn show(&self) {
        let set = |source: Source| {
            metrics::gauge!(crate::metrics::INPUT_SOURCE, "source" => source.label())
                .set(f64::from(u8::from(source == self.active)));
        };
        set(Source::Feed);
        set(Source::Service);
    }

    /// Check the return to the feed while the reader reads the service.
    /// The reader goes back when the member stays ready for the ready
    /// window, and a replay of the feed from the cursor then reaches the
    /// head. One break of the readiness, or a failed replay, starts the
    /// window again. Returns whether the reader switched to the feed.
    pub(crate) fn check_return(&mut self, cursor: (u64, u64), now: Instant) -> bool {
        if self.active != Source::Service || !self.feed.connected() {
            self.back = Return::default();
            return false;
        }
        let since = *self.back.ready_since.get_or_insert(now);
        if now.duration_since(since) < self.timing.ready {
            return false;
        }
        match self.probe(cursor, now) {
            Some(Step::Live { .. }) => {
                self.switch_to(Source::Feed, cursor, now);
                true
            }
            Some(_) => {
                self.back = Return::default();
                false
            }
            None => false,
        }
    }

    /// One step of the probe on the feed: its timers, then the frames that
    /// the feed already sent. Returns the step that ends the probe, if any.
    fn probe(&mut self, cursor: (u64, u64), now: Instant) -> Option<Step> {
        let id = self.next_id;
        let Self { feed, back, .. } = self;
        let probe = back
            .probe
            .get_or_insert_with(|| Answer::new(self.timing, now));
        match probe.wake(true, now) {
            Wake::Ask => {
                self.next_id = self.next_id.wrapping_add(1);
                feed.request(probe.ask(cursor, id, now));
                None
            }
            Wake::Fault(fault) => Some(Step::Fault(fault)),
            Wake::Wait => (0..PROBE_BATCH)
                .map_while(|_| match feed.poll(Duration::ZERO) {
                    Poll::Frame(bytes) => Some(bytes),
                    Poll::Idle | Poll::Closed => None,
                })
                .map(|bytes| probe.frame(&bytes, now))
                .find(|step| matches!(step, Step::Live { .. } | Step::Ask | Step::Fault(_))),
        }
    }
}

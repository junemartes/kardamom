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

    /// The index of this source in a `[feed, service]` pair.
    fn slot(self) -> usize {
        match self {
            Self::Feed => 0,
            Self::Service => 1,
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

/// The fault of the latest answer of each source. A new request to a
/// source clears its mark, and so does a replay that reaches the head.
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

    /// The verdict of two faults, one of each source.
    ///
    /// - A peer checkpoint only when every source refused the cursor below
    ///   its floor, or never connected in this run. The checkpoint is at or
    ///   above the lowest floor.
    /// - A stop when every source has a head below the cursor, or never
    ///   connected. The stop names the highest head.
    /// - Else the reader tries the other source again: a silent, lost or
    ///   bad source can serve again later, for example a member that
    ///   restarts.
    fn verdict(&self) -> Verdict {
        let pair = [self.feed, self.service];
        let every = |accept: fn(&Fault) -> bool| {
            pair.iter()
                .all(|mark| mark.is_some_and(|f| f == Fault::Absent || accept(&f)))
        };
        let floor = pair
            .iter()
            .flatten()
            .filter_map(|fault| match fault {
                Fault::Unavailable {
                    oldest_index,
                    oldest_block,
                } => Some((*oldest_block, *oldest_index)),
                _ => None,
            })
            .min();
        let head = pair
            .iter()
            .flatten()
            .filter_map(|fault| match fault {
                Fault::Ahead {
                    head_index,
                    head_block,
                } => Some((*head_index, *head_block)),
                _ => None,
            })
            .max();
        match (floor, head) {
            (Some((oldest_block, oldest_index)), _)
                if every(|f| matches!(f, Fault::Unavailable { .. })) =>
            {
                Verdict::Unavailable {
                    oldest_index,
                    oldest_block,
                }
            }
            (_, Some((head_index, head_block))) if every(|f| matches!(f, Fault::Ahead { .. })) => {
                Verdict::Behind {
                    head_index,
                    head_block,
                }
            }
            _ => Verdict::Switched,
        }
    }
}

/// The check for the return to the feed, while the reader reads the
/// service.
#[derive(Default)]
struct Return {
    /// The image of the feed, and since when the feed has that image
    /// without a break. A new image starts the window again.
    ready_since: Option<(u64, Instant)>,
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
    /// Whether each source had a connection in this run, as `[feed,
    /// service]`.
    seen: [bool; 2],
    /// The connection of the latest request to each source, as `[feed,
    /// service]`.
    asked_on: [Option<u64>; 2],
    /// A request went out on a connection of a source other than the
    /// connection of its request before. The stream of a new connection
    /// can differ from the one before, for example after a member lost
    /// the tail of its log.
    renewed: bool,
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
            seen: [false; 2],
            asked_on: [None; 2],
            renewed: false,
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

    /// The connection of the active source now. A source that has one
    /// counts as seen for the rest of the run.
    fn connection(&mut self) -> Option<u64> {
        let connection = match self.active {
            Source::Feed => self.feed.connection(),
            Source::Service => self.service.connection(),
        };
        self.seen[self.active.slot()] |= connection.is_some();
        connection
    }

    /// The timers of the active source.
    pub(crate) fn wake(&mut self, now: Instant) -> Wake {
        let connection = self.connection();
        self.answer.wake(connection, now)
    }

    fn take_id(&mut self) -> u64 {
        let id = self.next_id;
        self.next_id = self.next_id.wrapping_add(1);
        id
    }

    /// Whether a request went out on a new connection of its source since
    /// the last call. The flag clears.
    pub(crate) fn take_renewed(&mut self) -> bool {
        std::mem::take(&mut self.renewed)
    }

    /// Send a new request from `cursor` to the active source.
    pub(crate) fn ask(&mut self, cursor: (u64, u64), now: Instant) {
        let id = self.take_id();
        let connection = self.connection();
        if let Some(current) = connection {
            let before = self.asked_on[self.active.slot()].replace(current);
            self.renewed |= before.is_some_and(|before| before != current);
        }
        *self.marks.slot(self.active) = None;
        let request = self.answer.ask(cursor, id, connection, now);
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
        let mark = if self.seen[self.active.slot()] {
            fault
        } else {
            Fault::Absent
        };
        *self.marks.slot(self.active) = Some(mark);
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
    ///
    /// Before the probe, the reader discards the frames of the feed at each
    /// check. A member that stays live writes on, and a backlog would delay
    /// the answer of the probe past its silence window. While the reader
    /// reads the feed, it discards the frames of the service at each check
    /// in the same way: a service with a live stream writes on too.
    pub(crate) fn check_return(&mut self, cursor: (u64, u64), now: Instant) -> bool {
        if self.active != Source::Service {
            self.service.close();
            self.back = Return::default();
            return false;
        }
        if self.back.probe.is_none() {
            self.feed.close();
        }
        let Some(image) = self.feed.connection() else {
            self.back = Return::default();
            return false;
        };
        if self.back.ready_since.is_some_and(|(seen, _)| seen != image) {
            self.back = Return::default();
        }
        let (_, since) = *self.back.ready_since.get_or_insert((image, now));
        if now.duration_since(since) < self.timing.ready {
            return false;
        }
        match self.probe(cursor, image, now) {
            Some(Step::Live { .. }) => {
                self.switch_to(Source::Feed, cursor, now);
                true
            }
            Some(Step::Fault(fault)) => {
                *self.marks.slot(Source::Feed) = Some(fault);
                self.back = Return::default();
                false
            }
            _ => false,
        }
    }

    /// One step of the probe on the feed: its timers, then the frames that
    /// the feed already sent. A hole or a gap asks again. Returns the step
    /// that ends the probe: the replay end, or a fault.
    fn probe(&mut self, cursor: (u64, u64), image: u64, now: Instant) -> Option<Step> {
        let timing = self.timing;
        let wake = self
            .back
            .probe
            .get_or_insert_with(|| Answer::new(timing, now))
            .wake(Some(image), now);
        match wake {
            Wake::Ask => {
                self.probe_ask(cursor, image, now);
                None
            }
            Wake::Fault(fault) => Some(Step::Fault(fault)),
            Wake::Wait => {
                let Self { feed, back, .. } = self;
                let probe = back.probe.as_mut()?;
                let end = (0..PROBE_BATCH)
                    .map_while(|_| match feed.poll(Duration::ZERO) {
                        Poll::Frame(bytes) => Some(bytes),
                        Poll::Idle | Poll::Closed => None,
                    })
                    .map(|bytes| probe.frame(&bytes, now))
                    .find(|step| matches!(step, Step::Live { .. } | Step::Ask | Step::Fault(_)));
                match end {
                    Some(Step::Ask) => {
                        self.probe_ask(cursor, image, now);
                        None
                    }
                    other => other,
                }
            }
        }
    }

    /// Send a probe request from `cursor` on the feed image `image`. It is a
    /// new request to the feed, so the old mark of the feed goes.
    fn probe_ask(&mut self, cursor: (u64, u64), image: u64, now: Instant) {
        let id = self.take_id();
        *self.marks.slot(Source::Feed) = None;
        if let Some(probe) = self.back.probe.as_mut() {
            self.feed.request(probe.ask(cursor, id, Some(image), now));
        }
    }
}

//! The pipeline metrics of the `tx_ordering` reader and its joins.

use std::time::Instant;

use kardamom_obs::pipeline::{LoopMeter, Queue, Step};
use metrics::Counter;

use crate::metrics::{
    ARCHIVE_REFETCH_RECOVERED_TOTAL, ARCHIVE_REFETCH_TOTAL, JOIN_MISSES_TOTAL, VOID_VOTES_TOTAL,
};

/// The metrics of the join waits. The `tx_ordering` reader owns them and
/// lends them to each [`super::join::JoinWait`].
///
/// A join that finds its envelope at the first lookup records nothing. A
/// join that must wait records [`Self::wait`] once. That join sleeps one
/// poll interval or more, so the record costs little against the wait.
pub(super) struct JoinMeters {
    /// The time from the ordered entry to the end of a join that waited.
    wait: Step,
    /// The time of one archive refetch.
    refetch: Step,
    misses: Counter,
    refetch_ok: Counter,
    refetch_absent: Counter,
    refetch_error: Counter,
    recovered: Counter,
}

impl JoinMeters {
    pub(super) fn new() -> Self {
        let refetch =
            |outcome: &'static str| metrics::counter!(ARCHIVE_REFETCH_TOTAL, "outcome" => outcome);
        Self {
            wait: Step::new("executor.join_wait"),
            refetch: Step::new("executor.archive_refetch"),
            misses: metrics::counter!(JOIN_MISSES_TOTAL),
            refetch_ok: refetch("ok"),
            refetch_absent: refetch("absent"),
            refetch_error: refetch("error"),
            recovered: metrics::counter!(ARCHIVE_REFETCH_RECOVERED_TOTAL),
        }
    }

    /// A join that waited ended. `started` is the time the join began.
    pub(super) fn waited(&self, started: Instant) {
        self.wait.since(started);
    }

    /// The first in-band wait of a join ended without the envelope.
    pub(super) fn missed(&self) {
        self.misses.increment(1);
    }

    /// An archive refetch that began at `started` ended with `outcome`.
    pub(super) fn refetched(&self, started: Instant, outcome: Refetch) {
        self.refetch.since(started);
        match outcome {
            Refetch::Recovered(envelopes) => {
                self.refetch_ok.increment(1);
                self.recovered.increment(envelopes);
            }
            Refetch::Absent => self.refetch_absent.increment(1),
            Refetch::Failed => self.refetch_error.increment(1),
        }
    }
}

/// How one archive refetch ended.
#[derive(Clone, Copy)]
pub(super) enum Refetch {
    /// The archive answered with this many envelopes.
    Recovered(u64),
    /// The archive does not hold the range.
    Absent,
    /// The archive gave no definite answer.
    Failed,
}

/// The metrics of the `tx_ordering` reader thread.
///
/// The loop meter charges the wait for the next message to idle, and the
/// dispatch to busy. A send to a full sink (the exec channel or the
/// executor stream) is blocked. A join wait is part of the dispatch: it is
/// busy, and [`JoinMeters`] shows it apart.
pub(super) struct OrderingMeters {
    pub(super) meter: LoopMeter,
    pub(super) join: JoinMeters,
    /// The envelopes in the join buffer.
    pub(super) buffer: Queue,
    pub(super) void_votes: Counter,
}

impl OrderingMeters {
    pub(super) fn new() -> Self {
        Self {
            meter: LoopMeter::new("executor.ordering_reader"),
            join: JoinMeters::new(),
            buffer: Queue::new("executor.join_buffer"),
            void_votes: metrics::counter!(VOID_VOTES_TOTAL),
        }
    }
}

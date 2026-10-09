//! The escalation of a must-deliver publication that stays unconnected.
//!
//! A must-deliver publish retries until it lands, so one dead publication
//! holds the whole pipeline of this replica. The escalation bounds that
//! hold in two steps. After one stall budget without a connected
//! subscriber, the publisher opens the publication again: a new session
//! on a new control port, which every subscriber attaches afresh. After
//! five stall budgets in total, the process exits with
//! [`PUBLICATION_DEAD_EXIT_CODE`], so the supervisor restarts it.
//!
//! The clock runs only while a subscriber of the publication is known. A
//! stream without a subscriber (a cluster bootstrap where the executor
//! comes up first, an ingress pair down) has nothing to reopen for, and
//! an exit would only restart the publisher against the same empty
//! stream. The publisher reports that knowledge with each failure.
//!
//! The stall budget is the driver timeout of the Aeron client plus a
//! margin (`AeronRuntime::stall_budget`): the wait after which a silent
//! Aeron party counts as gone. A subscriber that is silent for less than
//! the budget can be a stalled party that every Aeron party survives, so
//! no step runs before it. The same clock serves the recorded `exec_txs`
//! publication, whose recording can fail to start after a driver crash.

use std::time::{Duration, Instant};

use crate::metrics::{PUBLICATION_CONNECTED, PUBLICATION_NOT_CONNECTED_SECONDS};

/// The process exit code of a publication that stayed unconnected past
/// the exit threshold. The Aeron client error handler exits with 1, so a
/// supervisor can tell the two apart.
pub const PUBLICATION_DEAD_EXIT_CODE: u8 = 3;

/// How many stall budgets an unconnected publication gets in total
/// before the process exits: one until the reopen, and four more for
/// the new publication to connect. At the production default (a 15 s
/// budget) the exit comes after 75 s, above the 60 s restart SLO of a
/// subscriber, so a restarting subscriber never exits its publishers.
const EXIT_BUDGETS: u32 = 5;

/// What the publisher does after one more unconnected attempt.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Step {
    /// Retry the same publication.
    Wait,
    /// Open the publication again, then retry. Given once per
    /// unconnected period.
    Reopen,
    /// Stop: the publication stayed unconnected for the whole budget.
    /// Carries the unconnected time so far.
    Exit { unconnected: Duration },
}

/// The escalation clock of one publication. Every connected publish
/// resets it. Every unconnected attempt advances it and names the next
/// step.
#[derive(Debug, Clone, Copy)]
pub struct Escalation {
    reopen_after: Duration,
    exit_after: Duration,
    /// The start of the current unconnected period, while one runs.
    since: Option<Instant>,
    reopened: bool,
}

impl Escalation {
    /// The escalation for a client with the stall budget `budget`: the
    /// reopen after one budget, the exit after [`EXIT_BUDGETS`] budgets.
    #[must_use]
    pub fn from_stall_budget(budget: Duration) -> Self {
        Self {
            reopen_after: budget,
            exit_after: budget.saturating_mul(EXIT_BUDGETS),
            since: None,
            reopened: false,
        }
    }

    /// The wait before the reopen.
    #[must_use]
    pub fn reopen_after(&self) -> Duration {
        self.reopen_after
    }

    /// The total unconnected wait before the exit.
    #[must_use]
    pub fn exit_after(&self) -> Duration {
        self.exit_after
    }

    /// A publish landed, or a subscriber answered: the clock resets.
    pub fn connected(&mut self) {
        self.since = None;
        self.reopened = false;
    }

    /// Start the unconnected period at `now` when none runs. A user whose
    /// first attempt is itself the start of the period calls this before
    /// the attempt, so the attempt's own duration counts.
    pub fn start(&mut self, now: Instant) {
        self.since.get_or_insert(now);
    }

    /// One more unconnected attempt at `now`. The first one starts the
    /// clock.
    pub fn unconnected(&mut self, now: Instant) -> Step {
        let since = *self.since.get_or_insert(now);
        let unconnected = now.saturating_duration_since(since);
        if unconnected >= self.exit_after {
            return Step::Exit { unconnected };
        }
        if unconnected >= self.reopen_after && !self.reopened {
            self.reopened = true;
            return Step::Reopen;
        }
        Step::Wait
    }

    /// How long the current unconnected period has lasted at `now`.
    /// Zero while the publication is connected.
    #[must_use]
    pub fn unconnected_for(&self, now: Instant) -> Duration {
        self.since
            .map_or(Duration::ZERO, |since| now.saturating_duration_since(since))
    }
}

/// The two gauges of one publication: whether it is connected, and how
/// long the escalation clock has counted in the current unconnected
/// period. A report writes the gauges on a change of the connected state
/// and on every unconnected report, so a connected hot path pays no gauge
/// write per batch.
#[derive(Debug)]
pub struct PublicationHealth {
    topic: &'static str,
    /// The last reported state. `None` before the first report.
    connected: Option<bool>,
}

impl PublicationHealth {
    #[must_use]
    pub fn new(topic: &'static str) -> Self {
        Self {
            topic,
            connected: None,
        }
    }

    /// Report the state: whether the last publish found a subscriber, and
    /// how long the escalation clock has counted (zero while connected,
    /// and zero while no subscriber is known).
    pub fn report(&mut self, connected: bool, counted: Duration) {
        if connected && self.connected == Some(true) {
            return;
        }
        self.connected = Some(connected);
        metrics::gauge!(PUBLICATION_CONNECTED, "topic" => self.topic)
            .set(f64::from(u8::from(connected)));
        metrics::gauge!(PUBLICATION_NOT_CONNECTED_SECONDS, "topic" => self.topic)
            .set(counted.as_secs_f64());
    }
}

#[cfg(test)]
#[path = "escalation_tests.rs"]
mod tests;

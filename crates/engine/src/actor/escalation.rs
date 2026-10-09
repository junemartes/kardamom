//! The escalation of a must-deliver publication that stays unconnected.
//!
//! A must-deliver publish retries until it lands, so one dead publication
//! holds the whole pipeline of this replica. The escalation bounds that
//! hold in two steps. After one stall budget without a connected
//! subscriber, the publisher opens the publication again: a new session
//! on a new control port, which every subscriber attaches afresh. After
//! four stall budgets in total, the process exits with
//! [`PUBLICATION_DEAD_EXIT_CODE`], so the supervisor restarts it.
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
/// before the process exits: one until the reopen, and three more for
/// the new publication to connect.
const EXIT_BUDGETS: u32 = 4;

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
    /// reopen after one budget, the exit after [`EXIT_BUDGETS`].
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
/// long the current unconnected period has lasted. A report writes the
/// gauges on a change of the connected state and on every unconnected
/// report, so a connected hot path pays no gauge write per batch.
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

    /// Report the length of the current unconnected period. Zero means
    /// connected.
    pub fn report(&mut self, unconnected: Duration) {
        let connected = unconnected.is_zero();
        if connected && self.connected == Some(true) {
            return;
        }
        self.connected = Some(connected);
        metrics::gauge!(PUBLICATION_CONNECTED, "topic" => self.topic)
            .set(f64::from(u8::from(connected)));
        metrics::gauge!(PUBLICATION_NOT_CONNECTED_SECONDS, "topic" => self.topic)
            .set(unconnected.as_secs_f64());
    }
}

#[cfg(test)]
#[path = "escalation_tests.rs"]
mod tests;

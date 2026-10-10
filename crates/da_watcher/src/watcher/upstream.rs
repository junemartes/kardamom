//! The housekeeping tick: the readiness mark, the record the watcher
//! waits for, and the pause on the L1 follower.
//!
//! The watcher waits when the archives do not give the record it needs:
//! the follower is down, or its archive does not hold the block yet. It
//! logs once a minute, sets `kardamom_da_watcher_waiting_for_l1_block`
//! for the block, and is paused with the follower as its root. It never
//! publishes without the parent check, and it has no other source.

use std::time::Duration;

use kardamom_obs::follower::FollowerWatch;
use tokio::sync::watch;
use tokio::time::Instant;
use tracing::warn;

use super::{L1Watcher, Wanted};
use crate::feed::BlockFeed;
use crate::metrics;
use crate::publisher::EpochPublisher;

/// How often the waiting log repeats.
const WAITING_LOG_EVERY: Duration = Duration::from_mins(1);

/// The watcher's view of the follower, and what it reported last.
#[derive(Debug)]
pub(super) struct Upstream {
    watch: FollowerWatch,
    /// The block the waiting gauge names, while it is set.
    gauge: Option<u64>,
    /// When the waiting log last ran.
    logged: Option<Instant>,
}

impl Upstream {
    pub(super) fn new(silence: Duration) -> Self {
        Self {
            watch: FollowerWatch::new(silence),
            gauge: None,
            logged: None,
        }
    }

    pub(super) fn saw_record(&mut self) {
        self.watch.saw_record();
    }

    /// Set the waiting gauge for `number`, or clear it.
    fn set_gauge(&mut self, number: Option<u64>) {
        if self.gauge == number {
            return;
        }
        let labels = |n: u64| [("number", n.to_string())];
        if let Some(old) = self.gauge {
            ::metrics::gauge!(metrics::WAITING_FOR_L1_BLOCK, &labels(old)).set(0.0);
        }
        if let Some(new) = number {
            ::metrics::gauge!(metrics::WAITING_FOR_L1_BLOCK, &labels(new)).set(1.0);
        }
        self.gauge = number;
    }

    /// Log the wait for `wanted` once a minute.
    fn log_waiting(&mut self, wanted: Wanted, now: Instant) {
        if self
            .logged
            .is_some_and(|at| now.saturating_duration_since(at) < WAITING_LOG_EVERY)
        {
            return;
        }
        self.logged = Some(now);
        warn!(
            target: "da_watcher",
            l1_number = wanted.number,
            waiting_secs = now.saturating_duration_since(wanted.since).as_secs(),
            "waiting for the l1_blocks record of the block; no archive holds it, and it is not \
             on the stream (is the L1 follower down?); no epoch is published without it"
        );
    }
}

impl<F: BlockFeed, P: EpochPublisher> L1Watcher<F, P> {
    /// Read the follower's halts from the `events` board: every live
    /// follower instance halted pauses the watcher with that halt as its
    /// root.
    #[must_use]
    pub fn with_board(mut self, board: watch::Receiver<kardamom_obs::events::BoardView>) -> Self {
        let Upstream {
            watch,
            gauge,
            logged,
        } = self.upstream;
        self.upstream = Upstream {
            watch: watch.with_board(board),
            gauge,
            logged,
        };
        self
    }

    /// One housekeeping tick. A wanted record that one tick did not bring
    /// is a wait: logged, exported, and a pause on the follower.
    pub(super) fn housekeep(&mut self) {
        kardamom_obs::ready::mark_now(metrics::LAST_TICK_UNIX_SECONDS);
        let now = Instant::now();
        let waiting = self
            .wanted
            .filter(|w| now.saturating_duration_since(w.since) >= self.tick);
        self.upstream.set_gauge(waiting.map(|w| w.number));
        if let Some(wanted) = waiting {
            self.upstream.log_waiting(wanted, now);
        }
        self.upstream.watch.follow(now, waiting.is_some());
    }
}

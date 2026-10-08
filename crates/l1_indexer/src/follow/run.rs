//! The follower's loop: one tick, then a sleep that the poll plan sets.
//!
//! The plan follows the finality schedule. Once a range reaches the
//! finalized tip, the follower sleeps until the next epoch boundary, the
//! next time the tip can move, then reads the tip every slot until it
//! moves. A range behind the tip reads the next one at once. Three rules
//! keep the sleep from hiding anything:
//!
//! - a failed tick, a halt included, reads again after one slot, so a
//!   halt clears within one slot of its cause;
//! - before each sleep the follower exports the planned wake time, and it
//!   is ready while now is before that time plus one slot;
//! - a chain without a schedule reads every slot.
//!
//! A halt that an operator clears (the light client anchor) holds the
//! follower until the clear. It still exports a wake time every slot, so
//! the wake alert stays quiet and the halt alert pages alone.

use std::ops::ControlFlow;
use std::time::{Duration, SystemTime};

use kardamom_da_watcher::L1Source;
use kardamom_obs::halt::{self, Clears};
use metrics::{counter, gauge};

use super::{Follower, Tick};
use crate::IndexerError;
use crate::metrics::{LAST_TICK_UNIX_SECONDS, NEXT_WAKE, TICK_TOTAL};
use crate::sink::BlockSink;

impl<S: L1Source, K: BlockSink> Follower<S, K> {
    /// Tick forever on the poll plan. An error is logged and counted, and
    /// an error with a halt raises it; a good tick clears the halt.
    pub async fn run(mut self) {
        loop {
            self.step().await;
        }
    }

    async fn step(&mut self) {
        let outcome = self.tick().await;
        kardamom_obs::ready::mark_now(LAST_TICK_UNIX_SECONDS);
        let wait = self.report(outcome).await;
        self.sleep(wait).await;
    }

    /// Count and log one tick, and return how long to sleep after it.
    async fn report(&mut self, outcome: Result<Tick, IndexerError>) -> Duration {
        let now = Self::unix_now();
        match outcome {
            Ok(tick) => {
                Self::count(&tick);
                halt::clear();
                self.plan(&tick, now)
            }
            Err(error) => self.failed(&error).await,
        }
    }

    fn count(tick: &Tick) {
        match tick {
            Tick::Idle => counter!(TICK_TOTAL, "outcome" => "idle").increment(1),
            Tick::Advanced { to, batches, .. } => {
                tracing::info!(to, batches, "indexed");
                counter!(TICK_TOTAL, "outcome" => "advanced").increment(1);
            }
        }
    }

    /// A failed tick: raise its halt, and read again after one slot. An
    /// operator's halt waits for the clear first.
    async fn failed(&self, error: &IndexerError) -> Duration {
        tracing::error!(%error, "tick failed");
        counter!(TICK_TOTAL, "outcome" => "error").increment(1);
        let Some(halt) = error.halt() else {
            return self.cfg.poll_interval;
        };
        let clears = halt.clears;
        halt::raise(halt);
        if clears == Clears::Operator {
            self.await_clear().await;
            return Duration::ZERO;
        }
        self.cfg.poll_interval
    }

    /// Hold until an operator clears the halt.
    async fn await_clear(&self) {
        while self.operator_slot().await.is_continue() {}
    }

    /// One slot of the wait for an operator's clear. `Break` once the
    /// halt is cleared.
    async fn operator_slot(&self) -> ControlFlow<()> {
        Self::mark_wake(self.cfg.poll_interval);
        tokio::select! {
            () = halt::cleared() => ControlFlow::Break(()),
            () = tokio::time::sleep(self.cfg.poll_interval) => ControlFlow::Continue(()),
        }
    }

    /// How long to sleep after a good tick. Without a schedule, one slot.
    /// With one: nothing while the range is behind the tip; until the next
    /// epoch boundary once it reached the tip; and while the tip does not
    /// move, until the awaited boundary, then one slot at a time.
    pub(super) fn plan(&mut self, tick: &Tick, now: u64) -> Duration {
        let slot = self.cfg.poll_interval;
        let Some(schedule) = self.cfg.schedule else {
            return slot;
        };
        match tick {
            Tick::Advanced {
                caught_up: false, ..
            } => Duration::ZERO,
            Tick::Advanced {
                caught_up: true, ..
            } => {
                let step = schedule.next_step_after(now);
                self.next_step = Some(step);
                Duration::from_secs(step.saturating_sub(now))
            }
            Tick::Idle => match self.next_step {
                Some(step) if step > now => Duration::from_secs(step - now),
                _ => slot,
            },
        }
    }

    /// Export the wake time, then sleep until it.
    async fn sleep(&self, wait: Duration) {
        Self::mark_wake(wait);
        tokio::time::sleep(wait).await;
    }

    /// Export now plus `wait` as the planned wake time.
    fn mark_wake(wait: Duration) {
        let at = SystemTime::now()
            .checked_add(wait)
            .unwrap_or(SystemTime::now());
        gauge!(NEXT_WAKE).set(kardamom_obs::ready::unix_seconds(at));
    }

    /// Seconds since the Unix epoch; 0 for a clock before it.
    fn unix_now() -> u64 {
        SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .map_or(0, |d| d.as_secs())
    }
}

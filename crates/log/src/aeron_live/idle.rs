//! The idle wait of a hot-path poll loop: [`IdleCap`], the longest wait,
//! and [`IdleBackoff`], the wait that grows from a base to the cap while
//! the loop finds no work.

use std::num::NonZeroU32;
use std::time::Duration;

use crate::error::LogError;

/// The longest idle wait of a hot-path poll loop.
///
/// A loop that polls a source with no wake-up, for example an Aeron
/// subscription, sees new data only when its wait ends. So the first
/// message after a quiet spell waits up to this cap at each such loop. A
/// lower cap gives a lower latency. It also gives more wake-ups: an idle
/// loop at the cap wakes about once per cap.
///
/// An [`AeronRuntime`](super::AeronRuntime) reads the cap from
/// `KARDAMOM_IDLE_CAP_US`, in whole microseconds, when it spawns. Unset or
/// empty gives the default, 50 µs.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct IdleCap(NonZeroU32);

impl IdleCap {
    /// The environment variable of the cap, in whole microseconds.
    const ENV: &str = "KARDAMOM_IDLE_CAP_US";

    /// The cap from `KARDAMOM_IDLE_CAP_US`.
    ///
    /// # Errors
    ///
    /// Returns an error when the value is not a whole number of
    /// microseconds above 0.
    pub(crate) fn from_env() -> Result<Self, LogError> {
        Self::parse(std::env::var(Self::ENV).ok().as_deref())
    }

    /// The cap from the raw value of `KARDAMOM_IDLE_CAP_US`. `None` or a
    /// blank value gives the default.
    fn parse(raw: Option<&str>) -> Result<Self, LogError> {
        let Some(raw) = raw.map(str::trim).filter(|s| !s.is_empty()) else {
            return Ok(Self::default());
        };
        raw.parse::<NonZeroU32>().map(Self).map_err(|e| {
            LogError::Config(format!(
                "{} must be a whole number of microseconds above 0, got {raw:?}: {e}",
                Self::ENV
            ))
        })
    }

    /// The cap as a duration.
    #[must_use]
    pub fn duration(self) -> Duration {
        Duration::from_micros(u64::from(self.0.get()))
    }

    /// An idle backoff from `base` up to this cap, for the calling
    /// thread. Call it on the thread that waits: it also sets the timer
    /// slack of that thread to 1 µs. A `base` above the cap starts at the
    /// cap.
    #[must_use]
    pub fn thread_backoff(self, base: Duration, grace: u32) -> IdleBackoff {
        Self::tighten_timer_slack();
        let cap = self.duration();
        IdleBackoff::new(base.min(cap), cap, grace)
    }

    /// Set the timer slack of the calling thread to 1 µs. Linux ends a
    /// timed wait up to the timer slack of the thread after its deadline.
    /// The default slack is 50 µs, so a 50 µs wait can take about 100 µs.
    /// At 1 µs, the wait ends within about 1 µs of its deadline. Other
    /// systems have no timer slack to set.
    fn tighten_timer_slack() {
        #[cfg(target_os = "linux")]
        {
            const TIMER_SLACK_NS: libc::c_ulong = 1_000;
            // SAFETY: PR_SET_TIMERSLACK reads one integer argument and
            // changes only the timer slack of the calling thread.
            let rc = unsafe { libc::prctl(libc::PR_SET_TIMERSLACK, TIMER_SLACK_NS) };
            if rc != 0 {
                tracing::warn!(
                    error = %std::io::Error::last_os_error(),
                    "could not set the timer slack of a hot-path thread"
                );
            }
        }
    }
}

impl Default for IdleCap {
    /// 50 µs.
    fn default() -> Self {
        Self(NonZeroU32::new(50).expect("50 is nonzero"))
    }
}

/// Escalating idle wait. This is the Rust analogue of Aeron's
/// `BackoffIdleStrategy`.
///
/// A poll loop waits between two polls. While the loop finds work, it
/// waits `base`. After `grace` consecutive empty iterations, the wait
/// doubles per iteration up to `cap`. Any work snaps it back to `base`.
///
/// A channel wait wakes at once when a sender sends, whatever the wait.
/// Only data from a source with no wake-up, for example an Aeron
/// subscription, can wait up to `cap` after a quiet spell. Under steady
/// traffic, every poll finds work, and the wait stays at `base`.
pub struct IdleBackoff {
    base: Duration,
    cap: Duration,
    grace: u32,
    streak: u32,
}

impl IdleBackoff {
    #[must_use]
    pub fn new(base: Duration, cap: Duration, grace: u32) -> Self {
        Self {
            base,
            cap,
            grace,
            streak: 0,
        }
    }

    /// The loop did work this iteration. Snap back to the base cadence.
    pub fn reset(&mut self) {
        self.streak = 0;
    }

    /// Record an empty iteration and return the wait to use for it.
    pub fn idle_wait(&mut self) -> Duration {
        self.streak = self.streak.saturating_add(1);
        if self.streak <= self.grace {
            return self.base;
        }
        let doublings = (self.streak - self.grace).min(10);
        self.base.saturating_mul(1u32 << doublings).min(self.cap)
    }

    /// The wait after an iteration: the base after work, which also
    /// resets the backoff, else the next [`Self::idle_wait`].
    pub fn next_wait(&mut self, worked: bool) -> Duration {
        if worked {
            self.reset();
            return self.base;
        }
        self.idle_wait()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn idle_backoff_escalates_after_grace_and_resets_on_work() {
        let base = Duration::from_micros(100);
        let cap = Duration::from_millis(1);
        let mut b = IdleBackoff::new(base, cap, 3);
        // Within grace: base cadence.
        assert_eq!(b.idle_wait(), base);
        assert_eq!(b.idle_wait(), base);
        assert_eq!(b.idle_wait(), base);
        // Past grace: doubles per idle iteration,
        assert_eq!(b.idle_wait(), Duration::from_micros(200));
        assert_eq!(b.idle_wait(), Duration::from_micros(400));
        assert_eq!(b.idle_wait(), Duration::from_micros(800));
        // and clamps at the cap, staying there.
        assert_eq!(b.idle_wait(), cap);
        for _ in 0..100 {
            assert_eq!(b.idle_wait(), cap);
        }
        // Any work snaps back to base for a full new grace window.
        b.reset();
        assert_eq!(b.idle_wait(), base);
        assert_eq!(b.idle_wait(), base);
    }

    #[test]
    fn next_wait_gives_the_base_after_work_and_escalates_when_idle() {
        let base = Duration::from_micros(10);
        let mut b = IdleBackoff::new(base, Duration::from_micros(50), 1);
        assert_eq!(b.next_wait(false), base);
        assert_eq!(b.next_wait(false), Duration::from_micros(20));
        assert_eq!(b.next_wait(false), Duration::from_micros(40));
        assert_eq!(b.next_wait(false), Duration::from_micros(50));
        assert_eq!(b.next_wait(true), base);
        assert_eq!(b.next_wait(false), base);
    }

    #[test]
    fn the_cap_defaults_to_50_us_and_parses_whole_microseconds() {
        assert_eq!(
            IdleCap::parse(None).unwrap().duration(),
            Duration::from_micros(50)
        );
        assert_eq!(IdleCap::parse(Some(" ")).unwrap(), IdleCap::default());
        assert_eq!(
            IdleCap::parse(Some(" 200 ")).unwrap().duration(),
            Duration::from_micros(200)
        );
    }

    #[test]
    fn the_cap_refuses_zero_and_garbage() {
        for raw in ["0", "-5", "1.5", "50us"] {
            assert!(
                IdleCap::parse(Some(raw))
                    .is_err_and(|e| e.to_string().contains("KARDAMOM_IDLE_CAP_US")),
                "{raw}"
            );
        }
    }

    #[test]
    fn a_base_above_the_cap_starts_at_the_cap() {
        let cap = IdleCap::parse(Some("5")).unwrap();
        let mut b = cap.thread_backoff(Duration::from_micros(10), 1);
        assert_eq!(b.idle_wait(), Duration::from_micros(5));
        assert_eq!(b.idle_wait(), Duration::from_micros(5));
    }
}

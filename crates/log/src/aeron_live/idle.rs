//! The idle wait of a poll loop: [`IdleBackoff`], the wait that grows
//! from a base to a cap while the loop finds no work, and [`IdleCap`], the
//! deploy setting that lowers the cap of the hot-path loops.

use std::num::NonZeroU32;
use std::time::Duration;

use crate::error::LogError;

/// The longest idle wait of a hot-path poll loop, when the deploy sets
/// one.
///
/// A loop that polls a source with no wake-up, for example an Aeron
/// subscription, sees new data only when its wait ends. So the first
/// message after a quiet spell waits up to the cap at each such loop. A
/// lower cap gives a lower latency. It also gives more wake-ups: an idle
/// loop at the cap wakes about once per cap.
///
/// An [`AeronRuntime`](super::AeronRuntime) reads the cap from
/// `KARDAMOM_IDLE_CAP_US`, in whole microseconds from 1 to 1,000,000, when
/// it spawns. Unset or blank, each loop keeps its own waits (see
/// [`IdleBackoff::under`]). The Java parser of the same variable takes the
/// same values (`crates/log/docker/aeron/idle-cap-cases.txt`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct IdleCap(Option<NonZeroU32>);

impl IdleCap {
    /// The environment variable of the cap, in whole microseconds.
    const ENV: &str = "KARDAMOM_IDLE_CAP_US";

    /// The largest cap, in microseconds: 1 s.
    const MAX_US: u32 = 1_000_000;

    /// The cap from `KARDAMOM_IDLE_CAP_US`.
    ///
    /// # Errors
    ///
    /// Returns an error when the value is not a whole number of
    /// microseconds from 1 to 1,000,000.
    pub(crate) fn from_env() -> Result<Self, LogError> {
        Self::parse(std::env::var(Self::ENV).ok().as_deref())
    }

    /// The cap from the raw value of `KARDAMOM_IDLE_CAP_US`. `None` or a
    /// blank value gives no cap.
    fn parse(raw: Option<&str>) -> Result<Self, LogError> {
        let Some(raw) = raw.map(str::trim).filter(|s| !s.is_empty()) else {
            return Ok(Self(None));
        };
        raw.parse::<NonZeroU32>()
            .ok()
            .filter(|us| us.get() <= Self::MAX_US)
            .map(|us| Self(Some(us)))
            .ok_or_else(|| {
                LogError::Config(format!(
                    "{} must be a whole number of microseconds from 1 to {}, got {raw:?}",
                    Self::ENV,
                    Self::MAX_US
                ))
            })
    }

    /// The cap as a duration, or `None` when the deploy sets no cap.
    #[must_use]
    pub fn duration(self) -> Option<Duration> {
        self.0.map(|us| Duration::from_micros(u64::from(us.get())))
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

/// Escalating idle wait. This is the Rust analogue of Aeron's
/// `BackoffIdleStrategy`.
///
/// A poll loop waits between two polls. While the loop finds work, it
/// waits `base`. After `grace` consecutive empty iterations, the wait
/// doubles per iteration until it reaches `cap`. Any work snaps it back
/// to `base`.
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

    /// This backoff under the deploy cap `cap`: the base `capped_base` and
    /// the cap of `cap`, with the same grace. A `capped_base` above the cap
    /// starts at the cap. With no deploy cap, the backoff stays as it is.
    ///
    /// Call it on the thread that waits: with a cap, it also sets the
    /// timer slack of that thread to 1 µs.
    #[must_use]
    pub fn under(self, cap: IdleCap, capped_base: Duration) -> Self {
        let Some(cap) = cap.duration() else {
            return self;
        };
        IdleCap::tighten_timer_slack();
        Self {
            base: capped_base.min(cap),
            cap,
            ..self
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
        // 31 doublings reach any cap up to 2^31 times the base, so every
        // cap that `IdleCap` takes is reached from a base of 1 ns or more.
        let doublings = (self.streak - self.grace).min(u32::BITS - 1);
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

    /// The cases that every parser of `KARDAMOM_IDLE_CAP_US` shares. The
    /// Java test reads the same file.
    const CASES: &str = include_str!("../../docker/aeron/idle-cap-cases.txt");

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
    fn the_backoff_reaches_a_cap_far_above_the_base() {
        let cap = Duration::from_secs(1);
        let mut b = IdleBackoff::new(Duration::from_micros(1), cap, 1);
        assert_eq!((0..40).map(|_| b.idle_wait()).last(), Some(cap));
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

    /// Check one line `raw|expected` of [`CASES`].
    fn check(line: &str) {
        let (raw, expected) = line.rsplit_once('|').expect("a case has a |");
        let raw = (raw != "<unset>").then_some(raw);
        let parsed = IdleCap::parse(raw);
        match expected {
            "refuse" => assert!(
                parsed.is_err_and(|e| e.to_string().contains("KARDAMOM_IDLE_CAP_US")),
                "{line}"
            ),
            "unset" => assert_eq!(parsed.unwrap().duration(), None, "{line}"),
            us => assert_eq!(
                parsed.unwrap().duration(),
                Some(Duration::from_micros(us.parse().unwrap())),
                "{line}"
            ),
        }
    }

    #[test]
    fn the_cap_takes_and_refuses_the_shared_cases() {
        let cases: Vec<&str> = CASES
            .lines()
            .filter(|l| !l.starts_with('#') && l.contains('|'))
            .collect();
        assert!(cases.len() > 10, "{cases:?}");
        cases.into_iter().for_each(check);
    }

    #[test]
    fn no_cap_keeps_the_backoff_and_a_cap_replaces_base_and_cap() {
        let old = || IdleBackoff::new(Duration::from_micros(100), Duration::from_millis(1), 1);
        let mut kept = old().under(IdleCap::default(), Duration::from_micros(10));
        assert_eq!(kept.idle_wait(), Duration::from_micros(100));
        assert_eq!(kept.idle_wait(), Duration::from_micros(200));

        let cap = IdleCap::parse(Some("50")).unwrap();
        let mut capped = old().under(cap, Duration::from_micros(10));
        assert_eq!(capped.idle_wait(), Duration::from_micros(10));
        assert_eq!(
            (0..10).map(|_| capped.idle_wait()).last(),
            Some(Duration::from_micros(50))
        );

        let mut low = old().under(
            IdleCap::parse(Some("5")).unwrap(),
            Duration::from_micros(10),
        );
        assert_eq!(low.idle_wait(), Duration::from_micros(5));
    }
}

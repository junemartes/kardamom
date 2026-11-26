//! The audit clock: when the next audit rebuild is due. The interval
//! counts from the last completed rebuild of any reason, because each
//! rebuild rewrites every account. The mirror persists the wall-clock time
//! of that rebuild, so a restart does not start the interval again.

use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

/// The age of the last rebuild, and the audit interval. In one process the
/// age runs on the monotonic clock, so a clock step does not move it.
pub(crate) struct AuditClock {
    /// The audit interval. `None` when the audit is off.
    every: Option<Duration>,
    /// The start of the monotonic count: the last rebuild of this
    /// process, or the start.
    since: Instant,
    /// The age of the last rebuild at `since`.
    before: Duration,
}

impl AuditClock {
    /// The clock at a start. `persisted` is the UNIX time in seconds of the
    /// last completed rebuild. No time means that no rebuild is on record,
    /// so the audit is due at once. A time after now counts as now, so a
    /// clock step cannot hold the audit back for longer than one interval.
    pub(crate) fn start(every: Option<Duration>, persisted: Option<u64>) -> Self {
        let now = Self::unix_now();
        let at = persisted.map(|at| at.min(now));
        crate::metrics::set_last_rebuild(at.unwrap_or(0));
        Self {
            every,
            since: Instant::now(),
            before: at.map_or(Duration::MAX, |at| {
                Duration::from_secs(now.saturating_sub(at))
            }),
        }
    }

    /// A rebuild completed now. Returns its UNIX time in seconds, which
    /// the mirror persists.
    pub(crate) fn rebuilt(&mut self) -> u64 {
        let now = Self::unix_now();
        self.since = Instant::now();
        self.before = Duration::ZERO;
        crate::metrics::set_last_rebuild(now);
        now
    }

    /// Whether the audit is on and its interval has passed since the last
    /// rebuild.
    pub(crate) fn audit_due(&self) -> bool {
        self.every.is_some_and(|every| self.age() >= every)
    }

    fn age(&self) -> Duration {
        self.before.saturating_add(self.since.elapsed())
    }

    /// The wall clock in UNIX seconds. A clock before 1970 reads as 0.
    pub(crate) fn unix_now() -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_or(0, |since| since.as_secs())
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::AuditClock;

    const HOUR: Duration = Duration::from_secs(3_600);

    fn hours_ago(hours: u64) -> Option<u64> {
        AuditClock::unix_now().checked_sub(hours.checked_mul(3_600).unwrap())
    }

    #[test]
    fn no_rebuild_on_record_makes_the_audit_due_at_once() {
        let clock = AuditClock::start(Some(HOUR), None);
        assert_eq!(clock.before, Duration::MAX);
        assert!(clock.audit_due());
    }

    #[test]
    fn the_interval_counts_from_the_persisted_rebuild() {
        let recent = AuditClock::start(Some(2 * HOUR), hours_ago(1));
        assert!(!recent.audit_due());
        assert!(recent.age() >= HOUR);
        let old = AuditClock::start(Some(2 * HOUR), hours_ago(3));
        assert!(old.audit_due());
    }

    #[test]
    fn a_rebuild_time_in_the_future_counts_as_now() {
        let future = AuditClock::unix_now().checked_add(10 * 24 * 3_600);
        let clock = AuditClock::start(Some(HOUR), future);
        assert_eq!(clock.before, Duration::ZERO);
        assert!(!clock.audit_due());
        let corrupt = AuditClock::start(Some(HOUR), Some(u64::MAX));
        assert_eq!(corrupt.before, Duration::ZERO);
    }

    #[test]
    fn a_rebuild_restarts_the_interval_and_the_audit_off_is_never_due() {
        let mut clock = AuditClock::start(Some(HOUR), hours_ago(5));
        assert!(clock.audit_due());
        let at = clock.rebuilt();
        assert!(at >= hours_ago(0).unwrap());
        assert!(!clock.audit_due());
        assert!(!AuditClock::start(None, None).audit_due());
    }
}

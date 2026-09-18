//! Independent bounds for park rejection and transport recovery.

use std::time::Duration;

use super::Retry;

const PARK_ATTEMPTS: u32 = 40;
const TRANSPORT_BUDGET: Duration = Duration::from_secs(90);

#[derive(Default)]
pub(super) struct RetryBudget {
    pub(super) timeouts: u32,
    pub(super) transports: u64,
    transport_elapsed: Duration,
}

impl RetryBudget {
    pub(super) fn can_retry(&self) -> bool {
        self.timeouts < PARK_ATTEMPTS && self.transport_elapsed < TRANSPORT_BUDGET
    }

    /// Only a failed transport attempt, including its backoff, consumes
    /// recovery time. A park timeout uses its separate attempt allowance.
    pub(super) fn record(&mut self, reason: Retry, elapsed: Duration) {
        match reason {
            Retry::Timeout => self.timeouts = self.timeouts.saturating_add(1),
            Retry::Transport => {
                self.transports = self.transports.saturating_add(1);
                self.transport_elapsed = self.transport_elapsed.saturating_add(elapsed);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parks_keep_their_full_allowance_even_after_ninety_seconds() {
        let mut budget = RetryBudget::default();
        for _ in 0..PARK_ATTEMPTS - 1 {
            budget.record(Retry::Timeout, Duration::from_secs(30));
            assert!(budget.can_retry());
        }
        budget.record(Retry::Timeout, Duration::from_secs(30));
        assert!(!budget.can_retry());
        assert_eq!(budget.transports, 0);
    }

    #[test]
    fn transport_recovery_is_bounded_by_elapsed_time() {
        let mut budget = RetryBudget::default();
        budget.record(Retry::Transport, Duration::from_secs(89));
        assert!(budget.can_retry());
        budget.record(Retry::Transport, Duration::from_secs(1));
        assert!(!budget.can_retry());
        assert_eq!(budget.timeouts, 0);
    }

    #[test]
    fn mixed_retries_preserve_the_remaining_recovery_opportunity() {
        let mut budget = RetryBudget::default();
        budget.record(Retry::Timeout, Duration::from_secs(60));
        budget.record(Retry::Transport, Duration::from_secs(30));
        budget.record(Retry::Timeout, Duration::from_secs(60));
        budget.record(Retry::Transport, Duration::from_secs(59));
        assert!(budget.can_retry(), "the next attempt can still succeed");
        assert_eq!(budget.timeouts, 2);
        assert_eq!(budget.transports, 2);
        budget.record(Retry::Transport, Duration::from_secs(1));
        assert!(!budget.can_retry());
    }
}

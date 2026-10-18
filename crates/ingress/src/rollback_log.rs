//! [`RollbackLog`]: the recent rollbacks of the executors, in a `watch`
//! channel next to the ordered receipt bus.
//!
//! The receipt bus is a broadcast channel. A reader that falls behind it
//! loses items, a rollback among them. The log keeps the recent rollbacks
//! in a value that a reader always sees, so a reader that lagged on the bus
//! still finds each rollback that it missed. Each rollback carries the
//! identity of its event, and the log takes each identity once. So a late
//! copy of one rollback from another executor changes nothing.

use alloy_primitives::B256;
use kardamom_cache::AppliedRollbacks;
use kardamom_types::ReceiptRollback;

/// The recent rollbacks, oldest first, and a count of every rollback that
/// the log took. A reader that lags by more rollbacks than the log keeps
/// loses the oldest ones. Rollbacks are rare, so the bound is far above any
/// real burst.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RollbackLog {
    /// The count of rollbacks that the log took, for all time.
    generation: u64,
    recent: AppliedRollbacks,
}

impl RollbackLog {
    /// The count of rollbacks that the log took. A reader keeps the value
    /// that it saw, and reads the rollbacks after it with [`Self::since`].
    #[must_use]
    pub fn generation(&self) -> u64 {
        self.generation
    }

    /// Whether the log holds a rollback of the event `id`.
    #[must_use]
    pub fn contains(&self, id: B256) -> bool {
        self.recent.contains(id)
    }

    /// Take `rollback`, unless the log holds its event already. Returns
    /// whether the log took it.
    pub fn push(&mut self, rollback: ReceiptRollback) -> bool {
        let new = self.recent.first_time(&rollback);
        // No log takes 2^64 rollbacks: the count saturates there.
        self.generation = self.generation.saturating_add(u64::from(new));
        new
    }

    /// The rollbacks that the log took after generation `since`, oldest
    /// first, as far as the log still holds them.
    pub fn since(&self, since: u64) -> impl Iterator<Item = &ReceiptRollback> {
        self.recent.newest(self.generation.saturating_sub(since))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn rollback(to_block: u64, id: u8) -> ReceiptRollback {
        ReceiptRollback {
            to_block,
            id: B256::repeat_byte(id),
        }
    }

    #[test]
    fn a_repeated_event_is_taken_once() {
        let mut log = RollbackLog::default();
        assert!(log.push(rollback(4, 1)));
        assert!(!log.push(rollback(4, 1)), "a late copy changes nothing");
        assert!(log.push(rollback(4, 2)), "another event at the same block");
        assert_eq!(log.generation(), 2);
    }

    #[test]
    fn since_gives_the_rollbacks_after_a_generation() {
        let mut log = RollbackLog::default();
        log.push(rollback(4, 1));
        let seen = log.generation();
        log.push(rollback(3, 2));
        log.push(rollback(5, 3));
        let after: Vec<u64> = log.since(seen).map(|r| r.to_block).collect();
        assert_eq!(after, [3, 5]);
        assert_eq!(log.since(log.generation()).count(), 0);
    }

    #[test]
    fn the_log_keeps_the_newest_events() {
        let mut log = RollbackLog::default();
        (0..=AppliedRollbacks::CAP).for_each(|i| {
            log.push(rollback(1, u8::try_from(i).unwrap()));
        });
        assert!(!log.contains(B256::repeat_byte(0)), "the oldest leaves");
        assert_eq!(log.since(0).count(), AppliedRollbacks::CAP);
    }
}

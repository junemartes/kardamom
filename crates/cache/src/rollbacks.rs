//! [`AppliedRollbacks`]: the rollback events that a consumer of
//! `tx_receipts` already applied.
//!
//! Every executor that finds one divergence publishes a rollback record
//! with the same id, and an executor publishes a record again with its id
//! after a restart. The first copy voids the receipts above its block. A
//! later copy comes after the receipts of the new stream, so a second
//! reaction would void valid state. A consumer acts on each id once.

use std::collections::VecDeque;

use alloy_primitives::B256;
use kardamom_types::ReceiptRollback;

/// The newest rollback events that a consumer applied, oldest first.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct AppliedRollbacks {
    recent: VecDeque<ReceiptRollback>,
}

impl AppliedRollbacks {
    /// The events kept. Rollbacks are rare, and the copies of one event
    /// come within the restart time of the executors, so this many is far
    /// more than the copies in flight.
    pub const CAP: usize = 64;

    /// Record `rollback`. Returns whether its id is new: the consumer acts
    /// only on a new one.
    pub fn first_time(&mut self, rollback: &ReceiptRollback) -> bool {
        if self.contains(rollback.id) {
            return false;
        }
        if self.recent.len() == Self::CAP {
            self.recent.pop_front();
        }
        self.recent.push_back(*rollback);
        true
    }

    /// Whether the consumer applied the event `id`.
    #[must_use]
    pub fn contains(&self, id: B256) -> bool {
        self.recent.iter().any(|r| r.id == id)
    }

    /// The `n` newest events, oldest first, as far as they are kept.
    pub fn newest(&self, n: u64) -> impl Iterator<Item = &ReceiptRollback> {
        let held = u64::try_from(self.recent.len()).unwrap_or(u64::MAX);
        let skip = usize::try_from(held.saturating_sub(n)).unwrap_or(usize::MAX);
        self.recent.iter().skip(skip)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn rollback(id: u8) -> ReceiptRollback {
        ReceiptRollback {
            to_block: 4,
            id: B256::repeat_byte(id),
        }
    }

    #[test]
    fn an_id_counts_once_and_the_oldest_id_goes_first() {
        let mut applied = AppliedRollbacks::default();
        assert!(applied.first_time(&rollback(1)));
        assert!(!applied.first_time(&rollback(1)), "a copy is no news");
        (2..=64).for_each(|id| assert!(applied.first_time(&rollback(id))));
        assert!(!applied.first_time(&rollback(1)), "64 ids stay");
        assert!(applied.first_time(&rollback(65)));
        assert!(applied.first_time(&rollback(1)), "the oldest id went");
    }
}

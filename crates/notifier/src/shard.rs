//! Webhook sharding across the notifier instances: rendezvous hashing of
//! the subscription id. Every instance computes the same owner from the
//! same instance count, with no coordination. A count change moves only
//! the subscriptions whose highest weight changed, which is what makes
//! the hashing consistent.

use std::num::NonZeroU32;

use alloy_primitives::{B256, keccak256};
use thiserror::Error;

/// This instance's place in the set: its index and the set's size.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct InstanceSet {
    index: u32,
    count: NonZeroU32,
}

#[derive(Debug, Error)]
#[error("instance index {index} is not below the instance count {count}")]
pub struct IndexOutOfRange {
    index: u32,
    count: NonZeroU32,
}

impl InstanceSet {
    /// # Errors
    ///
    /// Returns an error if `index` is not below `count`.
    pub fn new(index: u32, count: NonZeroU32) -> Result<Self, IndexOutOfRange> {
        if index >= count.get() {
            return Err(IndexOutOfRange { index, count });
        }
        Ok(Self { index, count })
    }

    #[must_use]
    pub fn index(&self) -> u32 {
        self.index
    }

    #[must_use]
    pub fn count(&self) -> NonZeroU32 {
        self.count
    }

    /// The instance that delivers subscription `id`: the one with the
    /// highest weight for it.
    #[must_use]
    pub fn owner(&self, id: B256) -> u32 {
        (1..self.count.get()).fold(0, |best, i| {
            if weight(i, id) > weight(best, id) {
                i
            } else {
                best
            }
        })
    }

    /// Whether this instance delivers subscription `id`.
    #[must_use]
    pub fn owns(&self, id: B256) -> bool {
        self.owner(id) == self.index
    }
}

fn weight(instance: u32, id: B256) -> B256 {
    let mut bytes = [0u8; 36];
    bytes[..4].copy_from_slice(&instance.to_be_bytes());
    bytes[4..].copy_from_slice(id.as_slice());
    keccak256(bytes)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn set(index: u32, count: u32) -> InstanceSet {
        InstanceSet::new(index, NonZeroU32::new(count).unwrap()).unwrap()
    }

    #[test]
    fn one_instance_owns_everything() {
        let s = set(0, 1);
        assert!((0..50u8).all(|b| s.owns(B256::repeat_byte(b))));
    }

    #[test]
    fn two_instances_split_the_ids_and_agree() {
        let a = set(0, 2);
        let b = set(1, 2);
        let ids: Vec<B256> = (0..200u8).map(B256::repeat_byte).collect();
        let owned_by_a = ids.iter().filter(|id| a.owns(**id)).count();
        assert!(owned_by_a > 50 && owned_by_a < 150, "{owned_by_a}");
        assert!(ids.iter().all(|id| a.owns(*id) != b.owns(*id)));
        assert!(ids.iter().all(|id| a.owner(*id) == b.owner(*id)));
    }

    #[test]
    fn a_third_instance_moves_only_the_ids_it_wins() {
        let two = set(0, 2);
        let three = set(0, 3);
        let ids: Vec<B256> = (0..200u8).map(B256::repeat_byte).collect();
        let moved_elsewhere = ids
            .iter()
            .filter(|id| two.owner(**id) != three.owner(**id) && three.owner(**id) != 2)
            .count();
        assert_eq!(moved_elsewhere, 0);
    }

    #[test]
    fn an_index_past_the_count_is_refused() {
        assert!(InstanceSet::new(2, NonZeroU32::new(2).unwrap()).is_err());
    }
}

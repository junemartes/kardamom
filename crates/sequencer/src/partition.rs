//! Sender-to-partition routing.
//!
//! The rule lives in `kardamom_types::shard_map`. The ingress uses the
//! same module (`kardamom_ingress::routing::partition_for`), so the two
//! sides agree byte for byte by construction. The rule has two levels. The
//! fixed level is `vslot = keccak256(sender)[..8] % 256`. The dynamic
//! level is a map from vslot to lane. Today the map is the identity
//! `lane = vslot % M`, which equals the legacy rule
//! `keccak256(sender)[..8] % M`. See `docs/specs/dynamic-sequencer-sizing.md`.

use std::num::NonZeroU32;

use alloy_primitives::Address;
use kardamom_types::shard_map::{ShardMap, ShardMapError, partition_for, validate_shard_count};
use serde::{Deserialize, Serialize};

/// The total number of sequencer partitions (M). Never zero: the only
/// constructor takes a `NonZeroU32`, so [`Self::index_of`] never divides
/// by zero. A count outside the lane plane (not 1, 2, 4, or 8) is legal
/// only with an explicit vslot set; [`Self::identity_map`] reports it.
/// `#[serde(transparent)]` keeps the TOML/CLI representation a plain
/// integer.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize)]
#[serde(transparent)]
pub struct PartitionCount(NonZeroU32);

impl PartitionCount {
    #[must_use]
    pub fn new(m: NonZeroU32) -> Self {
        Self(m)
    }

    #[must_use]
    pub fn get(&self) -> u32 {
        self.0.get()
    }

    /// Compute the partition index for a sender address: the legacy rule
    /// `keccak256(sender)[..8] % M`.
    #[must_use]
    pub fn index_of(&self, sender: Address) -> u32 {
        partition_for(sender, self.0)
    }

    /// The identity map `lane = vslot % M` over this count.
    ///
    /// # Errors
    ///
    /// Returns [`ShardMapError`] when the count is above the lane plane
    /// or does not divide the virtual slot count.
    pub fn identity_map(&self) -> Result<ShardMap, ShardMapError> {
        let lanes = validate_shard_count(self.get())?;
        ShardMap::identity(u32::from(lanes))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloy_primitives::address;

    fn m(n: u32) -> PartitionCount {
        PartitionCount::new(NonZeroU32::new(n).unwrap())
    }

    #[test]
    fn matches_proxy_routing_byte_for_byte() {
        // The proxy uses keccak256(sender)[..8] as a big-endian u64, modulo m.
        // This test reproduces one concrete vector to lock in that behavior.
        let a = address!("00000000000000000000000000000000DeadBeef");
        let h = alloy_primitives::keccak256(a.as_slice());
        let expected = u64::from_be_bytes(h[..8].try_into().unwrap()) % 8;
        assert_eq!(u64::from(m(8).index_of(a)), expected);
    }

    #[test]
    fn stable_per_address() {
        let a = address!("00000000000000000000000000000000DeadBeef");
        assert_eq!(m(8).index_of(a), m(8).index_of(a));
    }

    #[test]
    fn identity_map_accepts_the_lane_plane_divisors() {
        for n in [1, 2, 4, 8] {
            assert!(m(n).identity_map().is_ok());
        }
    }

    #[test]
    fn identity_map_rejects_counts_outside_the_lane_plane() {
        for n in [3, 64] {
            assert!(m(n).identity_map().is_err());
        }
    }
}

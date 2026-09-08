//! Sender-to-partition routing.
//!
//! The rule lives in `kardamom_types::shard_map`. The ingress uses the
//! same module (`kardamom_ingress::routing::partition_for`). So the two
//! sides agree byte for byte by construction. The rule has two levels. The
//! fixed level is `vslot = keccak256(sender)[..8] % 256`. The dynamic
//! level is a map from vslot to lane. Today the map is the identity
//! `lane = vslot % M`, which equals the legacy rule
//! `keccak256(sender)[..8] % M`. See `docs/specs/dynamic-sequencer-sizing.md`.

use alloy_primitives::Address;

/// Compute the partition index for a sender address.
///
/// `m` is the total number of sequencer partitions (must be `>= 1`).
#[inline]
pub fn partition_for(sender: Address, m: u32) -> u32 {
    kardamom_types::shard_map::partition_for(sender, m)
}

pub use kardamom_types::shard_map::ShardMapError as PartitionConfigError;

/// Validate the partition count against the lane plane. `m` must be 1,
/// 2, 4, or 8. See `kardamom_types::shard_map::validate_shard_count`.
pub fn validate_partition_count(m: u32) -> Result<(), PartitionConfigError> {
    kardamom_types::shard_map::validate_shard_count(m).map(|_| ())
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloy_primitives::address;

    #[test]
    fn matches_proxy_routing_byte_for_byte() {
        // The proxy uses keccak256(sender)[..8] as a big-endian u64, modulo m.
        // This test reproduces one concrete vector to lock in that behavior.
        let a = address!("00000000000000000000000000000000DeadBeef");
        let h = alloy_primitives::keccak256(a.as_slice());
        let expected = u64::from_be_bytes(h[..8].try_into().unwrap()) % 8;
        assert_eq!(partition_for(a, 8) as u64, expected);
    }

    #[test]
    fn stable_per_address() {
        let a = address!("00000000000000000000000000000000DeadBeef");
        assert_eq!(partition_for(a, 8), partition_for(a, 8));
    }

    #[test]
    fn validate_rejects_zero() {
        assert!(validate_partition_count(0).is_err());
    }

    #[test]
    fn validate_accepts_the_lane_plane_divisors() {
        assert!(validate_partition_count(1).is_ok());
        assert!(validate_partition_count(2).is_ok());
        assert!(validate_partition_count(8).is_ok());
    }

    #[test]
    fn validate_rejects_counts_outside_the_lane_plane() {
        assert!(validate_partition_count(3).is_err());
        assert!(validate_partition_count(64).is_err());
    }
}

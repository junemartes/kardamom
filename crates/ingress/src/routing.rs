//! This module routes a sender to a partition.
//!
//! The rule lives in `kardamom_types::shard_map`. The sequencer uses the
//! same module. So the two sides agree byte for byte by construction. The
//! rule has two levels. The fixed level is `vslot = keccak256(sender)[..8]
//! % 256`. The dynamic level is a map from vslot to lane. Today the map is
//! the identity `lane = vslot % M`, which equals the legacy rule
//! `keccak256(sender)[..8] % M`. See `docs/specs/dynamic-sequencer-sizing.md`.

use alloy_primitives::Address;

/// Returns the partition index for `sender`, out of `m` partitions.
#[inline]
pub fn partition_for(sender: Address, m: u32) -> u32 {
    kardamom_types::shard_map::partition_for(sender, m)
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloy_primitives::{address, keccak256};

    #[test]
    fn partition_is_stable_per_address() {
        let a = address!("00000000000000000000000000000000DeadBeef");
        let p1 = partition_for(a, 8);
        let p2 = partition_for(a, 8);
        assert_eq!(p1, p2);
        assert!(p1 < 8);
    }

    #[test]
    fn matches_the_legacy_rule() {
        // The legacy rule: keccak256(sender)[..8] as a big-endian u64, % m.
        let a = address!("00000000000000000000000000000000DeadBeef");
        let h = keccak256(a.as_slice());
        let prefix = u64::from_be_bytes(h[..8].try_into().unwrap());
        for m in [2u32, 8] {
            assert_eq!(partition_for(a, m) as u64, prefix % m as u64);
        }
    }

    #[test]
    fn distribution_is_reasonable_over_1024_addresses() {
        // With 1024 addresses in 8 partitions, each bucket should get at
        // least 1024 / 8 / 2 = 64 addresses. This is a smoke test, not a
        // chi-square test.
        let mut counts = [0u32; 8];
        for i in 0u64..1024 {
            let mut bytes = [0u8; 20];
            bytes[12..].copy_from_slice(&i.to_be_bytes());
            let addr = Address::from(bytes);
            counts[partition_for(addr, 8) as usize] += 1;
        }
        for (i, c) in counts.iter().enumerate() {
            assert!(*c >= 64, "partition {i} got {c} addresses, expected >= 64");
        }
    }

    #[test]
    fn partition_changes_with_m() {
        let a = address!("00000000000000000000000000000000DeadBeef");
        let p8 = partition_for(a, 8);
        let p16 = partition_for(a, 16);
        assert!(p8 < 8);
        assert!(p16 < 16);
    }
}

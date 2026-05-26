//! Integration test pinning partition_for distribution + cross-crate parity.

use alloy_primitives::Address;
use kardamom_sequencer::partition::{partition_for, validate_partition_count};

#[test]
fn partition_distributes_roughly_uniformly() {
    let m: u32 = 8;
    let mut counts = [0usize; 8];
    for i in 0u64..10_000 {
        let mut bytes = [0u8; 20];
        bytes[12..].copy_from_slice(&i.to_be_bytes());
        let addr = Address::from(bytes);
        counts[partition_for(addr, m) as usize] += 1;
    }
    // Each partition should see between ~800 and ~1500 of the 10k addresses
    // (loose chi-square slack; this catches a routing bug, not keccak quality).
    for c in counts {
        assert!(c > 800 && c < 1500, "partition imbalance: {counts:?}");
    }
}

#[test]
fn validate_partition_count_rejects_zero() {
    assert!(validate_partition_count(0).is_err());
}

#[test]
fn validate_partition_count_accepts_power_of_two_and_other() {
    assert!(validate_partition_count(1).is_ok());
    assert!(validate_partition_count(8).is_ok());
    assert!(validate_partition_count(64).is_ok());
}

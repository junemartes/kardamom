//! Integration test for `PartitionCount::index_of` distribution and
//! cross-crate parity.

use std::num::NonZeroU32;

use alloy_primitives::Address;
use kardamom_sequencer::partition::PartitionCount;

#[test]
fn partition_distributes_roughly_uniformly() {
    let m = PartitionCount::new(NonZeroU32::new(8).unwrap());
    let mut counts = [0usize; 8];
    for i in 0u64..10_000 {
        let mut bytes = [0u8; 20];
        bytes[12..].copy_from_slice(&i.to_be_bytes());
        let addr = Address::from(bytes);
        counts[m.index_of(addr) as usize] += 1;
    }
    // Each partition gets between ~800 and ~1500 of the 10,000 addresses.
    // This loose margin catches a routing bug. It does not test keccak quality.
    for c in counts {
        assert!(c > 800 && c < 1500, "partition imbalance: {counts:?}");
    }
}

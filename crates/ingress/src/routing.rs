//! Sender-to-partition routing. `partition = keccak256(sender)[..8] % M`.

use alloy_primitives::{Address, keccak256};

/// Returns the partition index for `sender` given `m` partitions.
///
/// Implementation: take the first 8 bytes of `keccak256(sender)` as a
/// big-endian `u64`, then `% m`. This matches the algorithm described in
///
#[inline]
pub fn partition_for(sender: Address, m: u32) -> u32 {
    debug_assert!(m > 0, "partition count must be positive");
    let h = keccak256(sender.as_slice());
    let leading = u64::from_be_bytes(h[..8].try_into().expect("8 bytes"));
    (leading % m as u64) as u32
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloy_primitives::address;

    #[test]
    fn partition_is_stable_per_address() {
        let a = address!("00000000000000000000000000000000DeadBeef");
        let p1 = partition_for(a, 8);
        let p2 = partition_for(a, 8);
        assert_eq!(p1, p2);
        assert!(p1 < 8);
    }

    #[test]
    fn distribution_is_reasonable_over_1024_addresses() {
        // For 1024 random addresses into 8 partitions each bucket should hold
        // at least 1024/8 / 2 = 64. (Smoke test; not a chi-square.)
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

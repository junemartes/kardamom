//! This module routes a sender to a partition. `partition = keccak256(sender)[..8] % M`.

use std::num::NonZeroU32;

use alloy_primitives::{Address, keccak256};

/// Returns the partition index for `sender`, out of `m` partitions.
///
/// Take the first 8 bytes of `keccak256(sender)` as a big-endian `u64`.
/// Then compute `% m`.
///
/// # Panics
///
/// Never panics in practice: `keccak256` always returns 32 bytes, so the
/// leading 8-byte slice always converts to a `[u8; 8]`.
#[inline]
#[must_use]
pub fn partition_for(sender: Address, m: NonZeroU32) -> u32 {
    let h = keccak256(sender.as_slice());
    let leading = u64::from_be_bytes(h[..8].try_into().expect("8 bytes"));
    // The result is `leading % m`, which is always < m <= u32::MAX, so
    // the narrowing cast back to u32 never truncates.
    #[allow(clippy::cast_possible_truncation)]
    let partition = (leading % u64::from(m.get())) as u32;
    partition
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloy_primitives::address;

    fn nz(m: u32) -> NonZeroU32 {
        NonZeroU32::new(m).expect("test m is non-zero")
    }

    #[test]
    fn partition_is_stable_per_address() {
        let a = address!("00000000000000000000000000000000DeadBeef");
        let p1 = partition_for(a, nz(8));
        let p2 = partition_for(a, nz(8));
        assert_eq!(p1, p2);
        assert!(p1 < 8);
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
            counts[partition_for(addr, nz(8)) as usize] += 1;
        }
        for (i, c) in counts.iter().enumerate() {
            assert!(*c >= 64, "partition {i} got {c} addresses, expected >= 64");
        }
    }

    #[test]
    fn partition_changes_with_m() {
        let a = address!("00000000000000000000000000000000DeadBeef");
        let p8 = partition_for(a, nz(8));
        let p16 = partition_for(a, nz(16));
        assert!(p8 < 8);
        assert!(p16 < 16);
    }
}

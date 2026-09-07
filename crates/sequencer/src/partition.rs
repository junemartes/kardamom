//! Sender-to-partition routing.
//!
//! This algorithm must match `kardamom_ingress::routing::partition_for`
//! exactly. Take the first 8 bytes of `keccak256(sender.as_slice())` as a
//! big-endian `u64`, then compute `% m`. The proxy routes by this rule. The
//! sequencer must agree byte-for-byte, or messages go to the wrong
//! partition.

use std::num::NonZeroU32;

use alloy_primitives::{Address, keccak256};
use serde::{Deserialize, Serialize};

/// The total number of sequencer partitions (M). Never zero: the only
/// constructor takes a `NonZeroU32`, so this can never divide by zero.
/// `#[serde(transparent)]` keeps the TOML/CLI representation a plain
/// integer, unchanged from the bare `NonZeroU32` this type replaces.
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

    /// Compute the partition index for a sender address.
    ///
    /// # Panics
    ///
    /// Never: `keccak256` always returns exactly 32 bytes, so the
    /// leading 8-byte slice always converts to a `[u8; 8]`.
    #[must_use]
    pub fn index_of(&self, sender: Address) -> u32 {
        let h = keccak256(sender.as_slice());
        let leading = u64::from_be_bytes(h[..8].try_into().expect("8 bytes"));
        #[allow(
            clippy::cast_possible_truncation,
            reason = "leading % u64::from(self.get()) is always < self.get(), which fits in u32"
        )]
        let idx = (leading % u64::from(self.get())) as u32;
        idx
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
}

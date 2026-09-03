//! Sender routing: the fixed vslot level and the versioned shard map.
//!
//! Routing has two levels. See `docs/specs/dynamic-sequencer-sizing.md`.
//!
//! 1. Fixed: `vslot = keccak256(sender)[..8] % 256`. This rule never
//!    changes. The ingress and the sequencer share it through this module.
//! 2. Dynamic: a versioned table `vslot -> lane`. Map v0 is the identity
//!    `lane = vslot % M`. When `M` divides 256, v0 assigns every sender
//!    exactly as the legacy rule `keccak256(sender)[..8] % M` did. The
//!    test `identity_matches_legacy_rule` pins this.
//!
//! A lane is a tx_data stream. The lane index is a `u8` on the wire
//! (`TxRef::shard_id`).

use alloy_primitives::{Address, keccak256};

/// The number of virtual slots. The fixed level maps each sender to one.
pub const VSLOT_COUNT: usize = 256;

/// The maximum number of lanes. A lane index is a `u8`.
pub const LANE_CAP: u32 = 256;

/// The first 8 bytes of `keccak256(sender)` as a big-endian `u64`.
#[inline]
fn sender_hash_prefix(sender: Address) -> u64 {
    let h = keccak256(sender.as_slice());
    u64::from_be_bytes(h[..8].try_into().expect("8 bytes"))
}

/// The fixed level. Returns the virtual slot of `sender`.
#[inline]
pub fn vslot_for(sender: Address) -> u8 {
    (sender_hash_prefix(sender) % VSLOT_COUNT as u64) as u8
}

/// The legacy rule: `keccak256(sender)[..8] % m`.
///
/// When `m` divides 256, this is the identity map v0 applied to
/// `vslot_for(sender)`. For any other `m`, the map layer does not apply,
/// and this function computes the legacy rule directly.
#[inline]
pub fn partition_for(sender: Address, m: u32) -> u32 {
    debug_assert!(m > 0, "partition count must be positive");
    if m > 0 && (VSLOT_COUNT as u32).is_multiple_of(m) {
        // Map v0: lane = vslot % m.
        vslot_for(sender) as u32 % m
    } else {
        (sender_hash_prefix(sender) % m as u64) as u32
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, thiserror::Error)]
pub enum ShardMapError {
    #[error("lane count must be between 1 and 256, got {0}")]
    LaneCount(u32),
    #[error("the identity map needs a lane count that divides 256, got {0}")]
    NotADivisor(u32),
}

/// A versioned table from virtual slot to lane.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ShardMap {
    version: u32,
    table: [u8; VSLOT_COUNT],
}

impl ShardMap {
    /// Map v0: `lane = vslot % lanes`. `lanes` must divide 256.
    pub fn identity(lanes: u32) -> Result<Self, ShardMapError> {
        if lanes == 0 || lanes > LANE_CAP {
            return Err(ShardMapError::LaneCount(lanes));
        }
        if !(VSLOT_COUNT as u32).is_multiple_of(lanes) {
            return Err(ShardMapError::NotADivisor(lanes));
        }
        let mut table = [0u8; VSLOT_COUNT];
        for (vslot, lane) in table.iter_mut().enumerate() {
            *lane = (vslot as u32 % lanes) as u8;
        }
        Ok(Self { version: 0, table })
    }

    /// A map from an explicit table.
    pub const fn from_table(version: u32, table: [u8; VSLOT_COUNT]) -> Self {
        Self { version, table }
    }

    pub const fn version(&self) -> u32 {
        self.version
    }

    pub const fn table(&self) -> &[u8; VSLOT_COUNT] {
        &self.table
    }

    /// The lane of one virtual slot.
    #[inline]
    pub fn lane_of_vslot(&self, vslot: u8) -> u8 {
        self.table[vslot as usize]
    }

    /// The lane of `sender`: both levels applied.
    #[inline]
    pub fn lane_for(&self, sender: Address) -> u8 {
        self.lane_of_vslot(vslot_for(sender))
    }

    /// The virtual slots that map to `lane`, in ascending order.
    pub fn vslots_of_lane(&self, lane: u8) -> impl Iterator<Item = u8> + '_ {
        self.table
            .iter()
            .enumerate()
            .filter(move |(_, l)| **l == lane)
            .map(|(vslot, _)| vslot as u8)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloy_primitives::address;

    fn addresses() -> impl Iterator<Item = Address> {
        (0u64..4096).map(|i| {
            let mut bytes = [0u8; 20];
            bytes[..8].copy_from_slice(&i.wrapping_mul(0x9E37_79B9_7F4A_7C15).to_be_bytes());
            bytes[12..].copy_from_slice(&i.to_be_bytes());
            Address::from(bytes)
        })
    }

    fn legacy_rule(sender: Address, m: u32) -> u32 {
        let h = keccak256(sender.as_slice());
        (u64::from_be_bytes(h[..8].try_into().unwrap()) % m as u64) as u32
    }

    #[test]
    fn identity_matches_legacy_rule() {
        // The parity test from the spec, milestone 1. For M in {2, 8}, the
        // identity map assigns every sender exactly as the legacy rule.
        for m in [2u32, 8] {
            let map = ShardMap::identity(m).unwrap();
            assert_eq!(map.version(), 0);
            for a in addresses() {
                let legacy = legacy_rule(a, m);
                assert_eq!(map.lane_for(a) as u32, legacy, "sender {a} m {m}");
                assert_eq!(partition_for(a, m), legacy, "sender {a} m {m}");
            }
        }
    }

    #[test]
    fn identity_holds_for_every_divisor_of_256() {
        for m in [1u32, 2, 4, 16, 32, 64, 128, 256] {
            let map = ShardMap::identity(m).unwrap();
            for a in addresses().take(512) {
                assert_eq!(map.lane_for(a) as u32, legacy_rule(a, m));
            }
        }
    }

    #[test]
    fn legacy_rule_still_applies_to_a_non_divisor() {
        for a in addresses().take(512) {
            assert_eq!(partition_for(a, 3), legacy_rule(a, 3));
            assert_eq!(partition_for(a, 7), legacy_rule(a, 7));
        }
    }

    #[test]
    fn vslot_is_the_low_byte_of_the_hash_prefix() {
        for a in addresses().take(512) {
            let h = keccak256(a.as_slice());
            assert_eq!(vslot_for(a), h[7]);
        }
    }

    #[test]
    fn identity_rejects_bad_lane_counts() {
        assert_eq!(ShardMap::identity(0), Err(ShardMapError::LaneCount(0)));
        assert_eq!(ShardMap::identity(257), Err(ShardMapError::LaneCount(257)));
        assert_eq!(ShardMap::identity(3), Err(ShardMapError::NotADivisor(3)));
    }

    #[test]
    fn identity_table_partitions_the_vslots() {
        let map = ShardMap::identity(8).unwrap();
        let mut seen = [false; VSLOT_COUNT];
        for lane in 0u8..8 {
            let slots: Vec<u8> = map.vslots_of_lane(lane).collect();
            assert_eq!(slots.len(), 32);
            for s in slots {
                assert_eq!(s % 8, lane);
                assert!(!seen[s as usize]);
                seen[s as usize] = true;
            }
        }
        assert!(seen.iter().all(|s| *s));
        assert_eq!(map.vslots_of_lane(8).count(), 0);
    }

    #[test]
    fn from_table_round_trips() {
        let mut table = [0u8; VSLOT_COUNT];
        table[5] = 2;
        let map = ShardMap::from_table(7, table);
        assert_eq!(map.version(), 7);
        assert_eq!(map.lane_of_vslot(5), 2);
        assert_eq!(map.lane_of_vslot(6), 0);
        assert_eq!(map.table(), &table);
    }

    #[test]
    fn known_vector_is_stable() {
        // keccak256(0x00000000000000000000000000000000DeadBeef) =
        // 0xbd174f45fb00f790_5ce254c0ef491691c955a15fdf10c5665b4493a591627fbe.
        // The vslot is the low byte of the first 8 bytes: 0x90 = 144.
        let a = address!("00000000000000000000000000000000DeadBeef");
        assert_eq!(vslot_for(a), 0x90);
        assert_eq!(partition_for(a, 8), 0);
        assert_eq!(partition_for(a, 2), 0);
        assert_eq!(partition_for(a, 256), 0x90);
        assert_eq!(ShardMap::identity(16).unwrap().lane_for(a), 0);
    }
}

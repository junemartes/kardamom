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

use alloc::string::String;
use alloc::vec::Vec;
use core::fmt;

use alloy_primitives::{Address, keccak256};

/// The number of virtual slots. The fixed level maps each sender to one.
pub const VSLOT_COUNT: usize = 256;

/// The maximum number of lanes. A lane index is a `u8`.
pub const LANE_CAP: u32 = 256;

/// The number of physical lanes in the lane plane. Every process opens
/// all of them at startup. The active map uses the first `M` lanes,
/// where `M` is the shard count. An idle lane costs one handle and one
/// idle stream. See `docs/specs/dynamic-sequencer-sizing.md`, section 3.1.
pub const LANE_COUNT: u8 = 8;

/// Validate a shard count `m` against the lane plane and the identity map.
/// `m` must be between 1 and [`LANE_COUNT`], and must divide 256. So the
/// valid values are 1, 2, 4, and 8. Returns `m` as a lane count.
pub fn validate_shard_count(m: u32) -> Result<u8, ShardMapError> {
    if m == 0 {
        return Err(ShardMapError::LaneCount(m));
    }
    if m > LANE_COUNT as u32 {
        return Err(ShardMapError::AboveLanePlane(m));
    }
    if !(VSLOT_COUNT as u32).is_multiple_of(m) {
        return Err(ShardMapError::NotADivisor(m));
    }
    Ok(m as u8)
}

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
    #[error("shard map table must have {VSLOT_COUNT} entries, got {0}")]
    TableLength(usize),
    #[error("shard map lane {0} is above the lane plane of {LANE_COUNT} lanes")]
    LaneAbovePlane(u8),
    #[error("lane count must be between 1 and 256, got {0}")]
    LaneCount(u32),
    #[error("the identity map needs a lane count that divides 256, got {0}")]
    NotADivisor(u32),
    #[error("shard count {0} is above the lane plane of {LANE_COUNT} lanes")]
    AboveLanePlane(u32),
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

    /// Check that every lane fits the lane plane.
    pub fn validate(&self) -> Result<(), ShardMapError> {
        match self.table.iter().find(|l| **l >= LANE_COUNT) {
            Some(l) => Err(ShardMapError::LaneAbovePlane(*l)),
            None => Ok(()),
        }
    }

    /// The number of active lanes: the highest lane in the table plus one.
    pub fn active_lanes(&self) -> u8 {
        self.table.iter().copied().max().map_or(0, |l| l + 1)
    }

    /// The virtual slots of `lane`, as a set.
    pub fn vslot_set(&self, lane: u8) -> VslotSet {
        let mut set = VslotSet::EMPTY;
        for v in self.vslots_of_lane(lane) {
            set.insert(v);
        }
        set
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

/// The serde shape of a [`ShardMap`]: `{ version, table: [u8; 256] }`.
/// The ingress reads its map from a TOML file in this shape.
#[derive(serde::Serialize, serde::Deserialize)]
struct ShardMapWire {
    version: u32,
    table: Vec<u8>,
}

impl serde::Serialize for ShardMap {
    fn serialize<S: serde::Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        ShardMapWire {
            version: self.version,
            table: self.table.to_vec(),
        }
        .serialize(s)
    }
}

impl<'de> serde::Deserialize<'de> for ShardMap {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        let wire = ShardMapWire::deserialize(d)?;
        let table: [u8; VSLOT_COUNT] =
            wire.table.as_slice().try_into().map_err(|_| {
                serde::de::Error::custom(ShardMapError::TableLength(wire.table.len()))
            })?;
        let map = Self::from_table(wire.version, table);
        map.validate().map_err(serde::de::Error::custom)?;
        Ok(map)
    }
}

/// A set of virtual slots. 256 bits.
///
/// The text form is a list of ranges, `"0-7,16,32-47"`. The empty set is
/// `""`. Config files and CLI flags use the text form.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Default)]
pub struct VslotSet {
    words: [u64; 4],
}

#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
pub enum VslotSetParseError {
    #[error("bad vslot range {0:?}: expected `a`, or `a-b` with a <= b, in 0..=255")]
    BadRange(String),
}

impl VslotSet {
    pub const EMPTY: Self = Self { words: [0; 4] };

    /// Every virtual slot.
    pub const fn full() -> Self {
        Self {
            words: [u64::MAX; 4],
        }
    }

    pub fn insert(&mut self, vslot: u8) {
        self.words[(vslot >> 6) as usize] |= 1u64 << (vslot & 63);
    }

    pub fn remove(&mut self, vslot: u8) {
        self.words[(vslot >> 6) as usize] &= !(1u64 << (vslot & 63));
    }

    #[inline]
    pub fn contains(&self, vslot: u8) -> bool {
        self.words[(vslot >> 6) as usize] & (1u64 << (vslot & 63)) != 0
    }

    pub fn len(&self) -> usize {
        self.words.iter().map(|w| w.count_ones() as usize).sum()
    }

    pub fn is_empty(&self) -> bool {
        self.words.iter().all(|w| *w == 0)
    }

    pub fn union(&self, other: &Self) -> Self {
        let mut out = *self;
        for (a, b) in out.words.iter_mut().zip(other.words.iter()) {
            *a |= *b;
        }
        out
    }

    pub fn difference(&self, other: &Self) -> Self {
        let mut out = *self;
        for (a, b) in out.words.iter_mut().zip(other.words.iter()) {
            *a &= !*b;
        }
        out
    }

    pub fn is_subset_of(&self, other: &Self) -> bool {
        self.words
            .iter()
            .zip(other.words.iter())
            .all(|(a, b)| *a & !*b == 0)
    }

    /// The slots, ascending.
    pub fn iter(&self) -> impl Iterator<Item = u8> + '_ {
        (0..=255u8).filter(move |v| self.contains(*v))
    }

    /// Parse the text form: comma-separated `a` or `a-b` ranges. Spaces
    /// are ignored. The empty string is the empty set.
    pub fn parse(text: &str) -> Result<Self, VslotSetParseError> {
        let mut set = Self::EMPTY;
        for part in text.split(',') {
            let part = part.trim();
            if part.is_empty() {
                continue;
            }
            let (lo, hi) = match part.split_once('-') {
                Some((a, b)) => (a.trim().parse::<u8>(), b.trim().parse::<u8>()),
                None => (part.parse::<u8>(), part.parse::<u8>()),
            };
            match (lo, hi) {
                (Ok(lo), Ok(hi)) if lo <= hi => {
                    for v in lo..=hi {
                        set.insert(v);
                    }
                }
                _ => return Err(VslotSetParseError::BadRange(String::from(part))),
            }
        }
        Ok(set)
    }
}

impl fmt::Display for VslotSet {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut first = true;
        let mut run: Option<(u8, u8)> = None;
        let flush = |f: &mut fmt::Formatter<'_>, run: (u8, u8), first: &mut bool| {
            if !*first {
                f.write_str(",")?;
            }
            *first = false;
            if run.0 == run.1 {
                write!(f, "{}", run.0)
            } else {
                write!(f, "{}-{}", run.0, run.1)
            }
        };
        for v in self.iter() {
            run = match run {
                Some((lo, hi)) if hi + 1 == v => Some((lo, v)),
                Some(done) => {
                    flush(f, done, &mut first)?;
                    Some((v, v))
                }
                None => Some((v, v)),
            };
        }
        if let Some(done) = run {
            flush(f, done, &mut first)?;
        }
        Ok(())
    }
}

impl fmt::Debug for VslotSet {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "VslotSet({self})")
    }
}

impl core::str::FromStr for VslotSet {
    type Err = VslotSetParseError;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Self::parse(s)
    }
}

impl serde::Serialize for VslotSet {
    fn serialize<S: serde::Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        s.collect_str(self)
    }
}

impl<'de> serde::Deserialize<'de> for VslotSet {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        let text = String::deserialize(d)?;
        Self::parse(&text).map_err(serde::de::Error::custom)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloy_primitives::address;

    #[test]
    fn vslot_set_round_trips_its_text_form() {
        let set = VslotSet::parse("0-7, 16,32-47,255").unwrap();
        assert_eq!(set.len(), 8 + 1 + 16 + 1);
        assert!(set.contains(0) && set.contains(7) && set.contains(16));
        assert!(set.contains(32) && set.contains(47) && set.contains(255));
        assert!(!set.contains(8) && !set.contains(48));
        assert_eq!(set.to_string(), "0-7,16,32-47,255");
        assert_eq!(VslotSet::parse(&set.to_string()).unwrap(), set);
        assert_eq!(VslotSet::parse("").unwrap(), VslotSet::EMPTY);
        assert_eq!(VslotSet::EMPTY.to_string(), "");
        assert_eq!(VslotSet::full().len(), 256);
        assert_eq!(VslotSet::full().to_string(), "0-255");
        assert!(VslotSet::parse("7-3").is_err());
        assert!(VslotSet::parse("256").is_err());
        assert!(VslotSet::parse("x").is_err());
    }

    #[test]
    fn vslot_set_algebra() {
        let a = VslotSet::parse("0-3").unwrap();
        let b = VslotSet::parse("2-5").unwrap();
        assert_eq!(a.union(&b).to_string(), "0-5");
        assert_eq!(a.difference(&b).to_string(), "0-1");
        assert!(VslotSet::parse("1-2").unwrap().is_subset_of(&a));
        assert!(!b.is_subset_of(&a));
        let mut c = a;
        c.remove(0);
        assert_eq!(c.to_string(), "1-3");
    }

    #[test]
    fn map_vslot_set_matches_the_iterator() {
        let map = ShardMap::identity(2).unwrap();
        let set = map.vslot_set(1);
        assert_eq!(set.len(), 128);
        assert!(map.vslots_of_lane(1).all(|v| set.contains(v)));
        assert_eq!(map.active_lanes(), 2);
        assert!(map.validate().is_ok());
    }

    #[test]
    fn map_serde_round_trips_and_validates() {
        let map = ShardMap::identity(4).unwrap();
        let json = serde_json::to_string(&map).unwrap();
        let back: ShardMap = serde_json::from_str(&json).unwrap();
        assert_eq!(back, map);
        let short = r#"{"version":1,"table":[0,1,2]}"#;
        assert!(serde_json::from_str::<ShardMap>(short).is_err());
        let mut table = [0u8; VSLOT_COUNT];
        table[9] = 8;
        let above = serde_json::to_string(&ShardMap::from_table(2, table)).unwrap();
        assert!(serde_json::from_str::<ShardMap>(&above).is_err());
    }

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
    fn shard_count_must_fit_the_lane_plane() {
        for m in [1u32, 2, 4, 8] {
            assert_eq!(validate_shard_count(m), Ok(m as u8));
        }
        assert_eq!(validate_shard_count(0), Err(ShardMapError::LaneCount(0)));
        assert_eq!(validate_shard_count(3), Err(ShardMapError::NotADivisor(3)));
        assert_eq!(validate_shard_count(6), Err(ShardMapError::NotADivisor(6)));
        assert_eq!(
            validate_shard_count(16),
            Err(ShardMapError::AboveLanePlane(16))
        );
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

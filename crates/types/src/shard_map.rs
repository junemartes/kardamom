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
//! A lane is a `tx_data` stream. The lane index is a `u8` on the wire
//! (`TxRef::shard_id`).

use alloc::string::String;
use alloc::vec::Vec;
use core::fmt;
use core::num::NonZeroU32;

use alloy_primitives::{Address, keccak256};

use crate::num::u32_to_usize;

/// The number of virtual slots. The fixed level maps each sender to one.
pub const VSLOT_COUNT: usize = 256;

/// [`VSLOT_COUNT`] as the `u32` the lane arithmetic divides.
const VSLOT_COUNT_U32: u32 = 256;

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
///
/// # Errors
///
/// Returns [`ShardMapError::LaneCount`] for zero,
/// [`ShardMapError::AboveLanePlane`] past [`LANE_COUNT`], and
/// [`ShardMapError::NotADivisor`] when `m` does not divide 256.
pub fn validate_shard_count(m: u32) -> Result<u8, ShardMapError> {
    if m == 0 {
        return Err(ShardMapError::LaneCount(m));
    }
    let lanes = u8::try_from(m)
        .ok()
        .filter(|lanes| *lanes <= LANE_COUNT)
        .ok_or(ShardMapError::AboveLanePlane(m))?;
    if !VSLOT_COUNT_U32.is_multiple_of(m) {
        return Err(ShardMapError::NotADivisor(m));
    }
    Ok(lanes)
}

/// The first 8 bytes of `keccak256(sender)` as a big-endian `u64`.
#[inline]
fn sender_hash_prefix(sender: Address) -> u64 {
    let h = keccak256(sender.as_slice());
    u64::from_be_bytes(h[..8].try_into().expect("8 bytes"))
}

/// The fixed level. Returns the virtual slot of `sender`: the low byte of
/// the hash prefix, which is the prefix modulo [`VSLOT_COUNT`].
#[inline]
#[must_use]
pub fn vslot_for(sender: Address) -> u8 {
    sender_hash_prefix(sender).to_be_bytes()[7]
}

/// The legacy rule: `keccak256(sender)[..8] % m`.
///
/// When `m` divides 256, this is the identity map v0 applied to
/// `vslot_for(sender)`. For any other `m`, the map layer does not apply,
/// and this function computes the legacy rule directly.
#[inline]
#[must_use]
pub fn partition_for(sender: Address, m: NonZeroU32) -> u32 {
    if VSLOT_COUNT_U32.is_multiple_of(m.get()) {
        // Map v0: lane = vslot % m.
        return u32::from(vslot_for(sender)) % m.get();
    }
    #[allow(
        clippy::cast_possible_truncation,
        reason = "prefix % u64::from(m) is below m, which is a u32"
    )]
    let lane = (sender_hash_prefix(sender) % u64::from(m.get())) as u32;
    lane
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
    ///
    /// # Errors
    ///
    /// Returns [`ShardMapError::LaneCount`] for zero or more than
    /// [`LANE_CAP`] lanes, and [`ShardMapError::NotADivisor`] when
    /// `lanes` does not divide 256.
    pub fn identity(lanes: u32) -> Result<Self, ShardMapError> {
        if lanes == 0 || lanes > LANE_CAP {
            return Err(ShardMapError::LaneCount(lanes));
        }
        if !VSLOT_COUNT_U32.is_multiple_of(lanes) {
            return Err(ShardMapError::NotADivisor(lanes));
        }
        let lanes = u32_to_usize(lanes);
        #[allow(
            clippy::cast_possible_truncation,
            reason = "vslot % lanes is below lanes, which is at most 256, and below 256 when lanes is 256"
        )]
        let table = core::array::from_fn(|vslot| (vslot % lanes) as u8);
        Ok(Self { version: 0, table })
    }

    /// A map from an explicit table.
    #[must_use]
    pub const fn from_table(version: u32, table: [u8; VSLOT_COUNT]) -> Self {
        Self { version, table }
    }

    /// Check that every lane fits the lane plane.
    ///
    /// # Errors
    ///
    /// Returns [`ShardMapError::LaneAbovePlane`] with the first lane at or
    /// past [`LANE_COUNT`].
    pub fn validate(&self) -> Result<(), ShardMapError> {
        match self.table.iter().find(|l| **l >= LANE_COUNT) {
            Some(l) => Err(ShardMapError::LaneAbovePlane(*l)),
            None => Ok(()),
        }
    }

    /// The number of active lanes: the highest lane in the table plus one.
    /// Saturates at `u8::MAX` for a table that uses lane 255.
    #[must_use]
    pub fn active_lanes(&self) -> u8 {
        self.table
            .iter()
            .copied()
            .max()
            .map_or(0, |l| l.saturating_add(1))
    }

    /// The virtual slots of `lane`, as a set.
    #[must_use]
    pub fn vslot_set(&self, lane: u8) -> VslotSet {
        self.vslots_of_lane(lane)
            .fold(VslotSet::EMPTY, VslotSet::with)
    }

    #[must_use]
    pub const fn version(&self) -> u32 {
        self.version
    }

    #[must_use]
    pub const fn table(&self) -> &[u8; VSLOT_COUNT] {
        &self.table
    }

    /// The lane of one virtual slot.
    #[inline]
    #[must_use]
    pub fn lane_of_vslot(&self, vslot: u8) -> u8 {
        self.table[usize::from(vslot)]
    }

    /// The lane of `sender`: both levels applied.
    #[inline]
    #[must_use]
    pub fn lane_for(&self, sender: Address) -> u8 {
        self.lane_of_vslot(vslot_for(sender))
    }

    /// The virtual slots that map to `lane`, in ascending order.
    pub fn vslots_of_lane(&self, lane: u8) -> impl Iterator<Item = u8> + '_ {
        (0..=u8::MAX)
            .zip(self.table.iter())
            .filter(move |(_, l)| **l == lane)
            .map(|(vslot, _)| vslot)
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

/// One `a` or `a-b` range of the text form, inclusive.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct VslotRange {
    lo: u8,
    hi: u8,
}

impl VslotRange {
    /// Parse one comma-separated part of the text form.
    fn parse(part: &str) -> Result<Self, VslotSetParseError> {
        let (a, b) = part.split_once('-').unwrap_or((part, part));
        match (a.trim().parse::<u8>(), b.trim().parse::<u8>()) {
            (Ok(lo), Ok(hi)) if lo <= hi => Ok(Self { lo, hi }),
            _ => Err(VslotSetParseError::BadRange(String::from(part))),
        }
    }

    /// The text form: `a` for a single slot, `a-b` otherwise.
    fn write(self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.lo == self.hi {
            write!(f, "{}", self.lo)
        } else {
            write!(f, "{}-{}", self.lo, self.hi)
        }
    }

    /// Extend the range by `v` if `v` is the next slot, else start a new
    /// range at `v` and hand back the finished one.
    fn push(self, v: u8) -> (Self, Option<Self>) {
        if self.hi.checked_add(1) == Some(v) {
            (Self { lo: self.lo, hi: v }, None)
        } else {
            (Self { lo: v, hi: v }, Some(self))
        }
    }
}

impl VslotSet {
    pub const EMPTY: Self = Self { words: [0; 4] };

    /// Every virtual slot.
    #[must_use]
    pub const fn full() -> Self {
        Self {
            words: [u64::MAX; 4],
        }
    }

    /// The word and the bit of `vslot`.
    #[inline]
    const fn slot(vslot: u8) -> (usize, u64) {
        ((vslot >> 6) as usize, 1u64 << (vslot & 63))
    }

    pub fn insert(&mut self, vslot: u8) {
        let (word, bit) = Self::slot(vslot);
        self.words[word] |= bit;
    }

    /// `self` with `vslot` inserted. The by-value form of
    /// [`Self::insert`], for folds.
    #[must_use]
    pub fn with(mut self, vslot: u8) -> Self {
        self.insert(vslot);
        self
    }

    pub fn remove(&mut self, vslot: u8) {
        let (word, bit) = Self::slot(vslot);
        self.words[word] &= !bit;
    }

    #[inline]
    #[must_use]
    pub fn contains(&self, vslot: u8) -> bool {
        let (word, bit) = Self::slot(vslot);
        self.words[word] & bit != 0
    }

    #[must_use]
    pub fn len(&self) -> usize {
        u32_to_usize(self.words.iter().map(|w| w.count_ones()).sum())
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.words.iter().all(|w| *w == 0)
    }

    #[must_use]
    pub fn union(&self, other: &Self) -> Self {
        Self {
            words: core::array::from_fn(|i| self.words[i] | other.words[i]),
        }
    }

    #[must_use]
    pub fn difference(&self, other: &Self) -> Self {
        Self {
            words: core::array::from_fn(|i| self.words[i] & !other.words[i]),
        }
    }

    #[must_use]
    pub fn is_subset_of(&self, other: &Self) -> bool {
        self.words
            .iter()
            .zip(other.words.iter())
            .all(|(a, b)| *a & !*b == 0)
    }

    /// The slots, ascending.
    pub fn iter(&self) -> impl Iterator<Item = u8> + '_ {
        (0..=u8::MAX).filter(move |v| self.contains(*v))
    }

    /// Parse the text form: comma-separated `a` or `a-b` ranges. Spaces
    /// are ignored. The empty string is the empty set.
    ///
    /// # Errors
    ///
    /// Returns [`VslotSetParseError::BadRange`] for a part that is not
    /// `a` or `a-b` with `a <= b` in `0..=255`.
    pub fn parse(text: &str) -> Result<Self, VslotSetParseError> {
        text.split(',')
            .map(str::trim)
            .filter(|part| !part.is_empty())
            .map(VslotRange::parse)
            .try_fold(Self::EMPTY, |set, range| {
                range.map(|r| (r.lo..=r.hi).fold(set, Self::with))
            })
    }

    /// The ranges of the text form, ascending: maximal runs of adjacent
    /// slots.
    fn ranges(&self) -> Vec<VslotRange> {
        let (mut out, open) = self.iter().fold((Vec::new(), None), |(out, open), v| {
            Self::extend_runs(out, open, v)
        });
        out.extend(open);
        out
    }

    /// One [`Self::ranges`] step: add `v` to the open run, or close the
    /// run into `out` and open a new one at `v`.
    fn extend_runs(
        mut out: Vec<VslotRange>,
        open: Option<VslotRange>,
        v: u8,
    ) -> (Vec<VslotRange>, Option<VslotRange>) {
        let Some(open) = open else {
            return (out, Some(VslotRange { lo: v, hi: v }));
        };
        let (open, done) = open.push(v);
        out.extend(done);
        (out, Some(open))
    }
}

impl fmt::Display for VslotSet {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.ranges()
            .into_iter()
            .enumerate()
            .try_for_each(|(i, range)| Self::write_range(f, i, range))
    }
}

impl VslotSet {
    /// One range of the text form, with its separator after the first.
    fn write_range(f: &mut fmt::Formatter<'_>, i: usize, range: VslotRange) -> fmt::Result {
        if i > 0 {
            f.write_str(",")?;
        }
        range.write(f)
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

    fn nz(m: u32) -> NonZeroU32 {
        NonZeroU32::new(m).unwrap()
    }

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

    fn legacy_rule(sender: Address, m: u32) -> u64 {
        let h = keccak256(sender.as_slice());
        u64::from_be_bytes(h[..8].try_into().unwrap()) % u64::from(m)
    }

    #[test]
    fn identity_matches_legacy_rule() {
        // The parity test from the spec, milestone 1. For M in {2, 8}, the
        // identity map assigns every sender exactly as the legacy rule.
        let cases = [2u32, 8]
            .into_iter()
            .flat_map(|m| addresses().map(move |a| (m, a)));
        for (m, a) in cases {
            let map = ShardMap::identity(m).unwrap();
            assert_eq!(map.version(), 0);
            let legacy = legacy_rule(a, m);
            assert_eq!(u64::from(map.lane_for(a)), legacy, "sender {a} m {m}");
            assert_eq!(
                u64::from(partition_for(a, nz(m))),
                legacy,
                "sender {a} m {m}"
            );
        }
    }

    #[test]
    fn identity_holds_for_every_divisor_of_256() {
        let cases = [1u32, 2, 4, 16, 32, 64, 128, 256]
            .into_iter()
            .flat_map(|m| addresses().take(512).map(move |a| (m, a)));
        for (m, a) in cases {
            let map = ShardMap::identity(m).unwrap();
            assert_eq!(u64::from(map.lane_for(a)), legacy_rule(a, m));
        }
    }

    #[test]
    fn legacy_rule_still_applies_to_a_non_divisor() {
        for a in addresses().take(512) {
            assert_eq!(u64::from(partition_for(a, nz(3))), legacy_rule(a, 3));
            assert_eq!(u64::from(partition_for(a, nz(7))), legacy_rule(a, 7));
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
        for m in [1u8, 2, 4, 8] {
            assert_eq!(validate_shard_count(u32::from(m)), Ok(m));
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
        let slots: Vec<(u8, u8)> = (0u8..8)
            .flat_map(|lane| map.vslots_of_lane(lane).map(move |s| (lane, s)))
            .collect();
        assert_eq!(slots.len(), VSLOT_COUNT);
        for (lane, s) in slots {
            assert_eq!(s % 8, lane);
            assert!(!seen[usize::from(s)]);
            seen[usize::from(s)] = true;
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
        assert_eq!(map.lane_of_vslot(4), 0);
        assert_eq!(map.table()[5], 2);
        assert_eq!(map.active_lanes(), 3);
    }

    #[test]
    fn known_vector_is_stable() {
        // Pins the fixed level: a change here changes every sender's lane.
        let a = address!("00000000000000000000000000000000DeadBeef");
        assert_eq!(vslot_for(a), 0x90);
        assert_eq!(partition_for(a, nz(8)), 0);
        assert_eq!(partition_for(a, nz(2)), 0);
        assert_eq!(partition_for(a, nz(256)), 0x90);
        assert_eq!(ShardMap::identity(16).unwrap().lane_for(a), 0);
    }
}

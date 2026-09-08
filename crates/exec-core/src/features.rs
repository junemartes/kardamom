//! Feature flags, and the block-close protocol actions they gate.
//!
//! A feature flag lives in the `KardamomChainState` predeploy
//! ([`kardamom_types::upgrades::CHAIN_STATE`]), as
//! `featureId => activation timestamp (ms)`. Only a system deposit, derived
//! from an L1 upgrade transaction, writes it. The protocol reads the flag at
//! every block boundary, and acts on it.
//!
//! Every function here is pure. Given the same stored activation word and
//! the same block header fields, every role computes the same actions. This
//! makes an upgrade activate at the same block on every node, instead of
//! whenever each operator restarts a binary.
//!
//! ## Why this lives in `exec-core`
//!
//! This workspace has more than one block driver: the live engine's exec
//! thread, the validator's parallel path (which funnels back through the
//! engine's boundary), the offline replay driver, and the stateless/zk guest
//! shape. A block-close action implemented in only some of them causes a
//! consensus divergence. [`apply_block_close_actions`] is the single
//! implementation for all drivers. It takes only a state-read function as a
//! parameter, so a driver adopts it in one line and cannot get the
//! semantics subtly wrong.

use alloy_primitives::{Address, B256, U256, keccak256};
use kardamom_types::upgrades::CHAIN_STATE;

use crate::delta::PendingDelta;

/// Health check. This is the first feature, and it exercises the upgrade
/// path itself. It must match `KardamomChainState.FEATURE_HEALTH_CHECK`.
///
/// While active, every block records a [health beacon](HEALTH_BEACON_SLOT).
pub const FEATURE_HEALTH_CHECK: u64 = 1;

/// Storage slot of `KardamomChainState.healthBeacon`. Declaration order puts
/// the `_activation` mapping at slot 0 and this field at slot 1. The
/// contract's own test pins this against the compiled storage layout.
pub const HEALTH_BEACON_SLOT: B256 = B256::new([
    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1,
]);

/// Storage slot that holds `featureId`'s activation timestamp. This follows
/// the Solidity mapping rule `keccak256(pad32(key) ++ pad32(slot))`, with the
/// mapping at slot 0.
#[must_use]
pub fn activation_slot(feature_id: u64) -> B256 {
    let mut buf = [0u8; 64];
    buf[..32].copy_from_slice(&U256::from(feature_id).to_be_bytes::<32>());
    // buf[32..64] stays zero. This is the mapping's declaration slot.
    keccak256(buf)
}

/// Check if a feature is active for a block, given its header timestamp
/// `header_ts_ms`.
///
/// `stored == 0` means the feature is never scheduled. Activation is
/// inclusive (`<=`), matching `KardamomChainState.isActive`.
///
/// This compares against the header timestamp of the block being closed,
/// not the timestamp txs executed with (block N executes with boundary
/// N-1's timestamp). Both values come from the canonical stream, so the
/// check gives the same result on every replica. Using the header's own
/// timestamp is what makes "active from the first block at or after T" a
/// statement about the chain, not about execution plumbing.
#[must_use]
pub fn is_active(stored_activation: U256, header_ts_ms: u64) -> bool {
    !stored_activation.is_zero() && stored_activation <= U256::from(header_ts_ms)
}

/// A health beacon: a heartbeat counter, the block it was last recorded
/// at, and that block's header timestamp. Packed into one storage word
/// as `count | block << 64 | timestamp << 128`, mirroring
/// `KardamomChainState.health()`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Beacon {
    pub count: u64,
    pub block_number: u64,
    pub timestamp_ms: u64,
}

impl Beacon {
    /// Pack into one word. Each field is already a native `u64`, so it
    /// fits its 64-bit lane exactly and cannot bleed into a neighbor:
    /// the type is what bounds them, not a runtime saturate or wrap.
    /// `64 + 128 < 256`, so all three lanes fit the `U256` word with
    /// room to spare.
    #[must_use]
    pub fn pack(self) -> U256 {
        U256::from(self.count)
            | (U256::from(self.block_number) << 64)
            | (U256::from(self.timestamp_ms) << 128)
    }

    /// Inverse of [`Beacon::pack`].
    #[must_use]
    pub fn unpack(word: U256) -> Self {
        let mask = U256::from(u64::MAX);
        let field = |shift: usize| -> u64 { ((word >> shift) & mask).to::<u64>() };
        Self {
            count: field(0),
            block_number: field(64),
            timestamp_ms: field(128),
        }
    }
}

/// What the block-close pass did, for logging and metrics. This is empty
/// when no feature is active, which is the common case.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct BlockCloseOutcome {
    /// `Some(beat)` if the health beacon was recorded for this block.
    pub health_beat: Option<u64>,
}

/// Apply every block-close protocol action into `delta`.
///
/// Call this at block close, after the block's records are folded into
/// `delta` and before the delta goes to the writer. The actions must see the
/// block's own writes (an upgrade deposit landing in this block activates a
/// feature for this block), and their writes must go into the same delta.
///
/// `read_slot` supplies the state layers this function cannot see: the
/// caller's unsettled-parent layer, then its state snapshot, in that order.
/// This function also reads the `delta` layer, so the full read order is
/// `delta -> parent -> snapshot`, the same order the EVM uses. Reading the
/// snapshot alone would be wrong for two reasons: it lags by up to K
/// unsettled blocks, and it cannot hold the current block's own writes.
///
/// Writes go into `delta` only, not into the block's EIP-7928 BAL. The BAL
/// attributes accesses to transaction indices, and the validator verifies
/// claims per tx-index range. A block-close action belongs to no
/// transaction, so attributing it to one would misdescribe the block and
/// cause a mismatch with a validator that recomputes claims from
/// transactions. The write is still cross-checked between roles, because
/// the validator compares the whole `BlockDelta`.
///
/// # Errors
///
/// Returns `E` when `read_slot` fails to read a state layer.
pub fn apply_block_close_actions<E, F>(
    delta: &mut PendingDelta,
    block_number: u64,
    header_ts_ms: u64,
    mut read_slot: F,
) -> Result<BlockCloseOutcome, E>
where
    F: FnMut(Address, B256) -> Result<U256, E>,
{
    let mut out = BlockCloseOutcome::default();

    // Read through delta first, so a feature scheduled by a deposit in
    // this block is visible to this block's close.
    let mut read = |addr: Address, slot: B256, delta: &PendingDelta| -> Result<U256, E> {
        match delta.storage.get(&(addr, slot)) {
            Some(v) => Ok(*v),
            None => read_slot(addr, slot),
        }
    };

    let activation = read(CHAIN_STATE, activation_slot(FEATURE_HEALTH_CHECK), delta)?;
    if is_active(activation, header_ts_ms) {
        let prior = Beacon::unpack(read(CHAIN_STATE, HEALTH_BEACON_SLOT, delta)?);
        let beat = prior.count.saturating_add(1);
        delta.storage.insert(
            (CHAIN_STATE, HEALTH_BEACON_SLOT),
            Beacon {
                count: beat,
                block_number,
                timestamp_ms: header_ts_ms,
            }
            .pack(),
        );
        out.health_beat = Some(beat);
    }

    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use core::convert::Infallible;

    /// A state with nothing in it. Every read misses.
    #[allow(
        clippy::unnecessary_wraps,
        reason = "must match apply_block_close_actions's read_slot: F bound; this fixture never fails"
    )]
    fn empty(_a: Address, _s: B256) -> Result<U256, Infallible> {
        Ok(U256::ZERO)
    }

    #[test]
    fn activation_slot_matches_the_solidity_mapping_rule() {
        // keccak256(pad32(key) ++ pad32(0)). Cross-checked against the
        // contract by KardamomChainState.t.sol.
        let mut expected_input = [0u8; 64];
        expected_input[31] = 1;
        assert_eq!(activation_slot(1), keccak256(expected_input));
    }

    #[test]
    fn activation_slots_are_distinct_per_feature() {
        assert_ne!(activation_slot(1), activation_slot(2));
    }

    #[test]
    fn beacon_slot_is_slot_one() {
        assert_eq!(HEALTH_BEACON_SLOT, B256::from(U256::from(1u64)));
    }

    #[test]
    fn is_active_boundary_cases() {
        assert!(!is_active(U256::ZERO, 0), "unscheduled is never active");
        assert!(
            !is_active(U256::ZERO, u64::MAX),
            "unscheduled stays inactive at any time"
        );
        assert!(!is_active(U256::from(100u64), 99), "before T");
        assert!(is_active(U256::from(100u64), 100), "exactly at T");
        assert!(is_active(U256::from(100u64), 101), "after T");
    }

    #[test]
    fn beacon_round_trips() {
        let beacon = Beacon {
            count: 42,
            block_number: 1234,
            timestamp_ms: 1_700_000_000_250,
        };
        assert_eq!(Beacon::unpack(beacon.pack()), beacon);
    }

    #[test]
    fn beacon_fields_do_not_bleed_at_maxima() {
        let beacon = Beacon {
            count: u64::MAX,
            block_number: u64::MAX,
            timestamp_ms: u64::MAX,
        };
        assert_eq!(Beacon::unpack(beacon.pack()), beacon);
    }

    #[test]
    fn dormant_feature_writes_nothing() {
        let mut delta = PendingDelta::new();
        let out = apply_block_close_actions(&mut delta, 5, 1_000, empty).unwrap();
        assert_eq!(out.health_beat, None);
        assert!(
            delta.storage.is_empty(),
            "a dormant flag must not touch state"
        );
    }

    #[test]
    fn scheduled_but_not_yet_reached_writes_nothing() {
        let mut delta = PendingDelta::new();
        let read = |_a: Address, s: B256| -> Result<U256, Infallible> {
            if s == activation_slot(FEATURE_HEALTH_CHECK) {
                Ok(U256::from(2_000u64))
            } else {
                Ok(U256::ZERO)
            }
        };
        let out = apply_block_close_actions(&mut delta, 5, 1_999, read).unwrap();
        assert_eq!(out.health_beat, None);
        assert!(delta.storage.is_empty());
    }

    #[test]
    fn active_feature_records_the_beacon() {
        let mut delta = PendingDelta::new();
        let read = |_a: Address, s: B256| -> Result<U256, Infallible> {
            if s == activation_slot(FEATURE_HEALTH_CHECK) {
                Ok(U256::from(1_000u64))
            } else {
                Ok(U256::ZERO)
            }
        };
        let out = apply_block_close_actions(&mut delta, 7, 1_500, read).unwrap();
        assert_eq!(out.health_beat, Some(1));
        let w = delta.storage[&(CHAIN_STATE, HEALTH_BEACON_SLOT)];
        assert_eq!(
            Beacon::unpack(w),
            Beacon {
                count: 1,
                block_number: 7,
                timestamp_ms: 1_500
            }
        );
    }

    /// The layering that lets an immediate upgrade activate in its own
    /// block. The `setFeature` write exists only in this block's delta,
    /// nowhere else.
    #[test]
    fn activation_written_in_this_block_is_visible_to_this_block() {
        let mut delta = PendingDelta::new();
        delta.storage.insert(
            (CHAIN_STATE, activation_slot(FEATURE_HEALTH_CHECK)),
            U256::from(900u64),
        );

        let out = apply_block_close_actions(&mut delta, 3, 1_000, empty).unwrap();
        assert_eq!(
            out.health_beat,
            Some(1),
            "a feature activated by this block's own deposit must fire here"
        );
    }

    #[test]
    fn beat_count_continues_from_prior_state() {
        let mut delta = PendingDelta::new();
        let read = |_a: Address, s: B256| -> Result<U256, Infallible> {
            if s == activation_slot(FEATURE_HEALTH_CHECK) {
                Ok(U256::from(1u64))
            } else {
                Ok(Beacon {
                    count: 9,
                    block_number: 100,
                    timestamp_ms: 500,
                }
                .pack())
            }
        };
        let out = apply_block_close_actions(&mut delta, 101, 750, read).unwrap();
        assert_eq!(out.health_beat, Some(10));
        assert_eq!(
            Beacon::unpack(delta.storage[&(CHAIN_STATE, HEALTH_BEACON_SLOT)]),
            Beacon {
                count: 10,
                block_number: 101,
                timestamp_ms: 750
            }
        );
    }

    /// A beacon already written into this block's delta. This case is not
    /// possible today, but the read precedence must still be delta-first,
    /// and this is what the increment builds on.
    #[test]
    fn delta_takes_precedence_over_the_backing_read() {
        let mut delta = PendingDelta::new();
        delta.storage.insert(
            (CHAIN_STATE, activation_slot(FEATURE_HEALTH_CHECK)),
            U256::from(1u64),
        );
        delta.storage.insert(
            (CHAIN_STATE, HEALTH_BEACON_SLOT),
            Beacon {
                count: 4,
                block_number: 1,
                timestamp_ms: 1,
            }
            .pack(),
        );
        let read = |_a: Address, _s: B256| -> Result<U256, Infallible> {
            // Stale backing value that must not win.
            Ok(Beacon {
                count: 99,
                block_number: 99,
                timestamp_ms: 99,
            }
            .pack())
        };
        let out = apply_block_close_actions(&mut delta, 2, 2, read).unwrap();
        assert_eq!(out.health_beat, Some(5));
    }

    #[test]
    fn read_errors_propagate() {
        let mut delta = PendingDelta::new();
        let read = |_a: Address, _s: B256| -> Result<U256, &'static str> { Err("db down") };
        let err = apply_block_close_actions(&mut delta, 1, 1, read).unwrap_err();
        assert_eq!(err, "db down");
    }
}

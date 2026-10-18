//! Tests for `features`: the frozen flags of a block, and the block-close
//! actions.

use super::*;
use crate::state::MockStateDatabase;
use alloy_primitives::Address;

/// A snapshot that errors on every storage read, to exercise the error
/// paths. `basic`, `code_by_hash`, and the receipt lookups are never
/// called here; each returns its own error too, so a future caller cannot
/// get a silent default instead.
#[derive(Debug, thiserror::Error)]
#[error("db down")]
struct DbDownError;

impl kardamom_types::StateError for DbDownError {}

struct FailingDb;

impl StateDatabase for FailingDb {
    type Error = DbDownError;

    fn basic(&self, _address: Address) -> Result<Option<(u64, U256, B256)>, Self::Error> {
        Err(DbDownError)
    }

    fn storage(&self, _address: Address, _key: B256) -> Result<U256, Self::Error> {
        Err(DbDownError)
    }

    fn code_by_hash(&self, _code_hash: B256) -> Result<bytes::Bytes, Self::Error> {
        Err(DbDownError)
    }

    fn get_receipt(
        &self,
        _pos: kardamom_types::BPosition,
    ) -> Result<Option<kardamom_types::Receipt>, Self::Error> {
        Err(DbDownError)
    }

    fn get_tx_position(
        &self,
        _tx_hash: B256,
    ) -> Result<Option<kardamom_types::BPosition>, Self::Error> {
        Err(DbDownError)
    }
}

/// A snapshot with the health check scheduled at `activation_ms`.
fn scheduled(activation_ms: u64) -> MockStateDatabase {
    MockStateDatabase::builder()
        .storage(
            CHAIN_STATE,
            activation_slot(FEATURE_HEALTH_CHECK),
            U256::from(activation_ms),
        )
        .build()
}

const ACTIVE: BlockFeatures = BlockFeatures {
    health_check: true,
    sealer_v3: false,
};

fn close(block_number: u64, header_ts_ms: u64, features: BlockFeatures) -> BlockClose {
    BlockClose {
        block_number,
        header_ts_ms,
        features,
    }
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

/// Activation is inclusive, and a word of 0 is never active.
#[test]
fn a_flag_is_active_from_its_activation_time() {
    let health = |activation_ms, exec_ts_ms| {
        BlockFeatures::read(&scheduled(activation_ms), None, exec_ts_ms)
            .unwrap()
            .health_check
    };
    assert!(!health(0, 0), "unscheduled is never active");
    assert!(
        !health(0, u64::MAX),
        "unscheduled stays inactive at any time"
    );
    assert!(!health(100, 99), "before T");
    assert!(health(100, 100), "exactly at T");
    assert!(health(100, 101), "after T");
}

/// Each flag reads its own slot.
#[test]
fn every_flag_reads_its_own_word() {
    let snap = MockStateDatabase::builder()
        .storage(
            CHAIN_STATE,
            activation_slot(FEATURE_SEALER_V3),
            U256::from(5u64),
        )
        .build();
    assert_eq!(
        BlockFeatures::read(&snap, None, 5).unwrap(),
        BlockFeatures {
            health_check: false,
            sealer_v3: true,
        }
    );
    assert_eq!(
        BlockFeatures::read(&MockStateDatabase::builder().build(), None, 5).unwrap(),
        BlockFeatures::NONE
    );
}

/// The unsettled parent layer holds the newest blocks, so its word wins
/// over the snapshot: an off switch (a word in the future) in the parent
/// turns the flag off.
#[test]
fn the_parent_layer_wins_over_the_snapshot() {
    let mut parent = PendingDelta::new();
    parent.storage.insert(
        (CHAIN_STATE, activation_slot(FEATURE_HEALTH_CHECK)),
        U256::from(9_000u64),
    );
    let snap = scheduled(1);
    assert!(
        BlockFeatures::read(&snap, None, 2_000)
            .unwrap()
            .health_check
    );
    assert!(
        !BlockFeatures::read(&snap, Some(&parent), 2_000)
            .unwrap()
            .health_check
    );
}

#[test]
fn a_flag_read_error_propagates() {
    let err = BlockFeatures::read(&FailingDb, None, 1).unwrap_err();
    assert!(matches!(err, ExecutorError::State(_)), "got {err:?}");
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
    let out = close(5, 1_000, BlockFeatures::NONE)
        .apply(&mut delta, None, &scheduled(1))
        .unwrap();
    assert_eq!(out.health_beat, None);
    assert!(
        delta.storage.is_empty(),
        "a dormant flag must not touch state"
    );
}

#[test]
fn active_feature_records_the_beacon() {
    let mut delta = PendingDelta::new();
    let snap = MockStateDatabase::builder().build();
    let out = close(7, 1_500, ACTIVE)
        .apply(&mut delta, None, &snap)
        .unwrap();
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

/// The frozen set decides, not the word that the block itself wrote: a
/// `setFeature` deposit in this block changes the flags of the next block.
#[test]
fn an_activation_written_in_this_block_waits_for_the_next_block() {
    let mut delta = PendingDelta::new();
    delta.storage.insert(
        (CHAIN_STATE, activation_slot(FEATURE_HEALTH_CHECK)),
        U256::from(900u64),
    );
    let snap = MockStateDatabase::builder().build();
    let frozen = BlockFeatures::read(&snap, None, 1_000).unwrap();
    let out = close(3, 1_000, frozen)
        .apply(&mut delta, None, &snap)
        .unwrap();
    assert_eq!(out.health_beat, None, "this block froze the flag off");
    let next = BlockFeatures::read(&snap, Some(&delta), 1_000).unwrap();
    assert_eq!(next, ACTIVE, "the next block reads the new word");
}

#[test]
fn beat_count_continues_from_prior_state() {
    let mut delta = PendingDelta::new();
    let snap = MockStateDatabase::builder()
        .storage(
            CHAIN_STATE,
            HEALTH_BEACON_SLOT,
            Beacon {
                count: 9,
                block_number: 100,
                timestamp_ms: 500,
            }
            .pack(),
        )
        .build();
    let out = close(101, 750, ACTIVE)
        .apply(&mut delta, None, &snap)
        .unwrap();
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
        (CHAIN_STATE, HEALTH_BEACON_SLOT),
        Beacon {
            count: 4,
            block_number: 1,
            timestamp_ms: 1,
        }
        .pack(),
    );
    // Stale backing value that must not win.
    let snap = MockStateDatabase::builder()
        .storage(
            CHAIN_STATE,
            HEALTH_BEACON_SLOT,
            Beacon {
                count: 99,
                block_number: 99,
                timestamp_ms: 99,
            }
            .pack(),
        )
        .build();
    let out = close(2, 2, ACTIVE).apply(&mut delta, None, &snap).unwrap();
    assert_eq!(out.health_beat, Some(5));
}

#[test]
fn read_errors_propagate() {
    let mut delta = PendingDelta::new();
    let err = close(1, 1, ACTIVE)
        .apply(&mut delta, None, &FailingDb)
        .unwrap_err();
    assert!(matches!(err, ExecutorError::State(_)), "got {err:?}");
}

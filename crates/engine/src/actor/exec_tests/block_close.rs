//! Block-close protocol actions (L1-governed feature flags).

use std::sync::{Arc, Mutex};

use alloy_primitives::U256;
use kardamom_exec_core::features::{
    FEATURE_HEALTH_CHECK, HEALTH_BEACON_SLOT, activation_slot, unpack_beacon,
};
use kardamom_types::upgrades::CHAIN_STATE;

use crate::state::{MockStateDatabase, MutatingSnapshotSource, StaticSnapshotSource};

use crate::actor::test_support::{
    ApplyingRecordingQueue, ExecRig, ImmediateCommit, boundary_msg, feed,
};

/// Read the beacon out of a submitted block delta.
fn beacon_in(delta: &kardamom_types::BlockDelta) -> Option<(u64, u64, u64)> {
    delta
        .storage
        .iter()
        .find(|s| s.address == CHAIN_STATE && s.key == HEALTH_BEACON_SLOT)
        .map(|s| unpack_beacon(s.value))
}

/// Two empty blocks, with the flag never scheduled. The chain must be
/// byte-identical to one built without the feature. This property lets
/// the code ship dormant on a live chain.
#[test]
fn a_dormant_feature_writes_nothing_at_block_close() {
    let rx_r2e = feed(
        (1..=2u64)
            .map(|block_number| {
                boundary_msg(block_number, 0, 1_700_000_000_000 + block_number * 250)
            })
            .collect(),
    );

    let (rig, writer_log) = ExecRig::recording(
        StaticSnapshotSource(MockStateDatabase::builder().build()),
        ImmediateCommit,
    );
    let (h, _rx_e2c) = rig.spawn(rx_r2e);
    h.join().expect("no panic").expect("exec ok");

    let log = writer_log.lock().unwrap();
    assert_eq!(log.len(), 2);
    for (_, delta) in log.iter() {
        assert!(
            delta.storage.is_empty(),
            "a dormant flag must not touch state"
        );
    }
}

/// With the flag active, every block records a beacon. The beat counter
/// increments across blocks. Each beacon carries its own block's number
/// and header timestamp.
#[test]
fn an_active_feature_beats_once_per_block() {
    let writer_log = Arc::new(Mutex::new(Vec::new()));

    // Activated at a time already in the past.
    let db = MockStateDatabase::builder()
        .storage(
            CHAIN_STATE,
            activation_slot(FEATURE_HEALTH_CHECK),
            U256::from(1_000u64),
        )
        .build();

    let rx_r2e = feed(
        (1..=3u64)
            .map(|block_number| boundary_msg(block_number, 0, 2_000 + block_number))
            .collect(),
    );

    let (h, _rx_e2c) = ExecRig::new(
        MutatingSnapshotSource(db.clone()),
        ImmediateCommit,
        ApplyingRecordingQueue {
            db,
            log: writer_log.clone(),
        },
    )
    .spawn(rx_r2e);
    h.join().expect("no panic").expect("exec ok");

    let log = writer_log.lock().unwrap();
    assert_eq!(log.len(), 3);
    for (i, (boundary, delta)) in log.iter().enumerate() {
        let beat = i as u64 + 1;
        assert_eq!(
            beacon_in(delta),
            Some((beat, boundary.block_number, boundary.l2_timestamp)),
            "block {} must carry beat {beat} with its own header fields",
            boundary.block_number
        );
    }
}

/// The beacon must use the block's own header timestamp, not the previous
/// boundary's timestamp that executes its transactions. So a feature
/// scheduled between two boundaries fires in the first block whose header
/// reaches it.
#[test]
fn activation_is_judged_against_the_blocks_own_header_timestamp() {
    let writer_log = Arc::new(Mutex::new(Vec::new()));

    let activation = 5_000u64;
    let snap = MockStateDatabase::builder()
        .storage(
            CHAIN_STATE,
            activation_slot(FEATURE_HEALTH_CHECK),
            U256::from(activation),
        )
        .build();

    // Headers straddle the activation time: 4_999 (before), 5_000 (at
    // activation, inclusive, so it fires), and 5_001 (after).
    let rx_r2e = feed(
        [(1u64, 4_999u64), (2, 5_000), (3, 5_001)]
            .into_iter()
            .map(|(block_number, ts)| boundary_msg(block_number, 0, ts))
            .collect(),
    );

    let (h, _rx_e2c) = ExecRig::new(
        MutatingSnapshotSource(snap.clone()),
        ImmediateCommit,
        ApplyingRecordingQueue {
            db: snap,
            log: writer_log.clone(),
        },
    )
    .spawn(rx_r2e);
    h.join().expect("no panic").expect("exec ok");

    let log = writer_log.lock().unwrap();
    assert_eq!(beacon_in(&log[0].1), None, "block 1 is before activation");
    assert_eq!(
        beacon_in(&log[1].1),
        Some((1, 2, 5_000)),
        "activation is inclusive: the block AT T beats"
    );
    assert_eq!(beacon_in(&log[2].1), Some((2, 3, 5_001)));
}

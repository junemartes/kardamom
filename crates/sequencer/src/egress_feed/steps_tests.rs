use std::time::Duration;

use crate::outbound::fakes::InMemorySidePublisher;
use crate::resync::{SealerRefusal, SharedWatermark};
use crate::sealer_rules::{SealerRules, SealerVersion};
use alloy_primitives::Address;
use alloy_primitives::B256;
use kardamom_cluster_adapter::wire::{
    RefRefused, SEALER_V5_DECISION_VERSION, encode_egress_boundary, encode_record_lag_reject,
    encode_status,
};
use kardamom_obs::lifecycle::Lifecycle;
use kardamom_types::cluster_status::RecordLagStatus;
use kardamom_types::{ClusterStatus, DropReason, TxDrop, TxErrorReason};

use super::EgressWatermarkFeed;

/// Rules that keep `version` from 0, with drops that the test reads.
fn rules(
    version: &SealerVersion,
    drops: &InMemorySidePublisher,
) -> SealerRules<InMemorySidePublisher> {
    SealerRules::new(0, version.clone(), drops.clone())
}

/// A record-lag reject reaches the terminal-refusal channel with its
/// values, and never the rewind channel.
#[test]
fn a_record_lag_reject_is_a_terminal_refusal() {
    let lifecycle = Lifecycle::new(None);
    let (deadline_tx, deadline_rx) = crossbeam_channel::bounded(4);
    let (answer_tx, answer_rx) = crossbeam_channel::bounded(4);
    let (origins, _origins_rx) = crossbeam_channel::unbounded();
    let mut feed = EgressWatermarkFeed::new(
        1_000,
        0,
        SharedWatermark::new(),
        answer_tx,
        deadline_tx,
        origins,
        rules(&SealerVersion::default(), &InMemorySidePublisher::default()),
    );
    let sender = Address::repeat_byte(0x13);
    let lag = RecordLagStatus {
        best_recorded: Some(3_000),
        budget: 16_384,
        halted: true,
    };

    feed.on_frame(
        &encode_record_lag_reject(sender, 7, 20_000, &lag),
        &lifecycle,
    );

    assert_eq!(
        deadline_rx.try_recv().unwrap(),
        SealerRefusal {
            sender,
            nonce: 7,
            reason: TxErrorReason::RecordLag {
                sealed_index: 20_000,
                recorded_index: 3_000,
                budget: 16_384,
            },
        }
    );
    assert!(answer_rx.try_recv().is_err(), "a refusal is not a rewind");
}

/// A status with the snapshot notice holds the lag flag through the
/// snapshot stall, the boundary after the stall included. The next
/// boundary gap flags again.
#[test]
fn a_snapshot_notice_holds_the_lag_flag_until_the_next_boundary() {
    let lifecycle = Lifecycle::new(None);
    let (deadline_tx, _deadline_rx) = crossbeam_channel::bounded(4);
    let (reject_tx, _reject_rx) = crossbeam_channel::bounded(4);
    let (origins, _origins_rx) = crossbeam_channel::unbounded();
    let watermark = SharedWatermark::new();
    let mut feed = EgressWatermarkFeed::new(
        1,
        0,
        watermark.clone(),
        reject_tx,
        deadline_tx,
        origins,
        rules(&SealerVersion::default(), &InMemorySidePublisher::default()),
    );
    let notice = encode_status(&ClusterStatus {
        snapshot_follows: true,
        ..ClusterStatus::default()
    });

    feed.on_frame(&notice, &lifecycle);
    std::thread::sleep(Duration::from_millis(5));
    feed.on_idle(&lifecycle);
    feed.on_frame(&encode_egress_boundary(9, 100, 0, 0), &lifecycle);
    assert_eq!(
        watermark.take_lag(),
        None,
        "a stall that the notice announced is no lag"
    );

    std::thread::sleep(Duration::from_millis(5));
    feed.on_idle(&lifecycle);
    assert!(
        watermark.take_lag().is_some(),
        "the boundary after the snapshot ends the stall"
    );
}

/// The refusal of a reference (egress kind 18) reaches no channel of the
/// publish loop. It raises the kept version to the version of the frame,
/// and publishes one drop for the hash, with the reason of the refusal.
#[test]
fn a_reference_refusal_raises_the_version_and_drops_the_transaction() {
    let lifecycle = Lifecycle::new(None);
    let (deadline_tx, deadline_rx) = crossbeam_channel::bounded(4);
    let (answer_tx, answer_rx) = crossbeam_channel::bounded(4);
    let (origins, origins_rx) = crossbeam_channel::unbounded();
    let version = SealerVersion::default();
    let drops = InMemorySidePublisher::default();
    let mut feed = EgressWatermarkFeed::new(
        1_000,
        0,
        SharedWatermark::new(),
        answer_tx,
        deadline_tx,
        origins,
        rules(&version, &drops),
    );

    feed.on_frame(
        &RefRefused {
            canonical_id: B256::repeat_byte(0xAB),
            sender: Address::repeat_byte(0x13),
            nonce: 7,
            decision_version: SEALER_V5_DECISION_VERSION,
        }
        .encode(),
        &lifecycle,
    );

    assert!(deadline_rx.try_recv().is_err());
    assert!(answer_rx.try_recv().is_err());
    assert!(origins_rx.try_recv().is_err());
    assert!(version.carries_bytes());
    assert_eq!(
        *drops.drops.lock().unwrap(),
        [TxDrop {
            tx_hash: B256::repeat_byte(0xAB),
            sender: Address::repeat_byte(0x13),
            nonce: 7,
            reason: DropReason::SealerRefusedReference,
        }]
    );
    assert_eq!(drops.statuses.lock().unwrap().len(), 0, "no status");
    assert_eq!(drops.errors.lock().unwrap().len(), 0, "no error");
}

/// A reference refusal that names a version below the kept version does
/// not lower it.
#[test]
fn a_reference_refusal_below_the_kept_version_does_not_lower_it() {
    let lifecycle = Lifecycle::new(None);
    let (deadline_tx, _deadline_rx) = crossbeam_channel::bounded(4);
    let (answer_tx, _answer_rx) = crossbeam_channel::bounded(4);
    let (origins, _origins_rx) = crossbeam_channel::unbounded();
    let version = SealerVersion::starting_at(SEALER_V5_DECISION_VERSION);
    let drops = InMemorySidePublisher::default();
    let mut feed = EgressWatermarkFeed::new(
        1_000,
        0,
        SharedWatermark::new(),
        answer_tx,
        deadline_tx,
        origins,
        rules(&version, &drops),
    );

    feed.on_frame(
        &RefRefused {
            canonical_id: B256::repeat_byte(0xAB),
            sender: Address::repeat_byte(0x13),
            nonce: 7,
            decision_version: 4,
        }
        .encode(),
        &lifecycle,
    );

    assert!(version.carries_bytes(), "the version stays 5");
    assert_eq!(drops.drops.lock().unwrap().len(), 1);
}

#[test]
fn all_terminal_frames_reach_v2_without_filling_the_unconsumed_v1_channel() {
    use crate::v2::AccountEvent;
    use kardamom_cluster_adapter::wire::{encode_da_lag_reject, encode_past_deadline};
    let (deadline_tx, deadline_rx) = crossbeam_channel::bounded(1);
    let (answer_tx, _) = crossbeam_channel::unbounded();
    let (origins, _) = crossbeam_channel::unbounded();
    let (accounts, rx) = crossbeam_channel::bounded(4);
    let mut feed = EgressWatermarkFeed::new(
        1000,
        0,
        SharedWatermark::new(),
        answer_tx,
        deadline_tx,
        origins,
        rules(&SealerVersion::default(), &InMemorySidePublisher::default()).telling(accounts),
    );
    let life = Lifecycle::new(None);
    let sender = Address::repeat_byte(3);
    let lag = RecordLagStatus {
        halted: true,
        best_recorded: Some(100),
        budget: 16,
    };
    let status = ClusterStatus {
        halted: true,
        sealed_head: 30,
        posted_head: 10,
        budget_blocks: 8,
        ..ClusterStatus::default()
    };
    for frame in [
        encode_past_deadline(sender, 7, 100, 101),
        encode_da_lag_reject(sender, 8, &status),
        encode_record_lag_reject(sender, 9, 200, &lag),
    ] {
        feed.on_frame(&frame, &life);
    }
    let actual: Vec<_> = rx
        .try_iter()
        .map(|event| match event {
            AccountEvent::Refusal(refusal) => *refusal,
            other => panic!("unexpected event {other:?}"),
        })
        .collect();
    assert_eq!(
        actual,
        vec![
            SealerRefusal {
                sender,
                nonce: 7,
                reason: TxErrorReason::PastDeadline {
                    max_inclusion_block: 100,
                    at_block: 101
                }
            },
            SealerRefusal {
                sender,
                nonce: 8,
                reason: TxErrorReason::DaLag {
                    sealed_head: 30,
                    posted_head: 10,
                    budget_blocks: 8
                }
            },
            SealerRefusal {
                sender,
                nonce: 9,
                reason: TxErrorReason::RecordLag {
                    sealed_index: 200,
                    recorded_index: 100,
                    budget: 16
                }
            },
        ]
    );
    assert!(deadline_rx.is_empty());
}

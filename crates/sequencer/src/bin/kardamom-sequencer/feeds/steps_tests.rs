use std::time::Duration;

use alloy_primitives::Address;
use kardamom_cluster_adapter::wire::{
    encode_egress_boundary, encode_record_lag_reject, encode_status,
};
use kardamom_sequencer::resync::{SealerRefusal, SharedWatermark};
use kardamom_types::cluster_status::RecordLagStatus;
use kardamom_types::{ClusterStatus, TxErrorReason};

use super::EgressWatermarkFeed;

/// A record-lag reject reaches the terminal-refusal channel with its
/// values, and never the rewind channel.
#[test]
fn a_record_lag_reject_is_a_terminal_refusal() {
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
    );
    let sender = Address::repeat_byte(0x13);
    let lag = RecordLagStatus {
        best_recorded: Some(3_000),
        budget: 16_384,
        halted: true,
    };

    feed.on_frame(&encode_record_lag_reject(sender, 7, 20_000, &lag));

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
    let (deadline_tx, _deadline_rx) = crossbeam_channel::bounded(4);
    let (reject_tx, _reject_rx) = crossbeam_channel::bounded(4);
    let (origins, _origins_rx) = crossbeam_channel::unbounded();
    let watermark = SharedWatermark::new();
    let mut feed =
        EgressWatermarkFeed::new(1, 0, watermark.clone(), reject_tx, deadline_tx, origins);
    let notice = encode_status(&ClusterStatus {
        snapshot_follows: true,
        ..ClusterStatus::default()
    });

    feed.on_frame(&notice);
    std::thread::sleep(Duration::from_millis(5));
    feed.on_idle();
    feed.on_frame(&encode_egress_boundary(9, 100, 0, 0));
    assert_eq!(
        watermark.take_lag(),
        None,
        "a stall that the notice announced is no lag"
    );

    std::thread::sleep(Duration::from_millis(5));
    feed.on_idle();
    assert!(
        watermark.take_lag().is_some(),
        "the boundary after the snapshot ends the stall"
    );
}

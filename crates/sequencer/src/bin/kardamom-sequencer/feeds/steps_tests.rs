use alloy_primitives::Address;
use kardamom_cluster_adapter::wire::encode_record_lag_reject;
use kardamom_sequencer::resync::{SealerRefusal, SharedWatermark};
use kardamom_types::TxErrorReason;
use kardamom_types::cluster_status::RecordLagStatus;

use super::EgressWatermarkFeed;

/// A record-lag reject reaches the terminal-refusal channel with its
/// values, and never the rewind channel.
#[test]
fn a_record_lag_reject_is_a_terminal_refusal() {
    let (deadline_tx, deadline_rx) = crossbeam_channel::bounded(4);
    let (reject_tx, reject_rx) = crossbeam_channel::bounded(4);
    let (origins, _origins_rx) = crossbeam_channel::unbounded();
    let mut feed = EgressWatermarkFeed::new(
        1_000,
        0,
        SharedWatermark::new(),
        reject_tx,
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
    assert!(reject_rx.try_recv().is_err(), "a refusal is not a rewind");
}

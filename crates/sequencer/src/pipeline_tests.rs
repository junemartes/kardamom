use std::time::Duration;

use alloy_primitives::B256;
use kardamom_cluster_adapter::wire::GuardHeader;
use kardamom_obs::pipeline::LoopState;
use kardamom_types::{BPosition, TxDataLoc, TxRef};
use metrics_exporter_prometheus::PrometheusBuilder;

use super::{DEPTH_EVERY, PublishMeters};
use crate::error::SequencerError;
use crate::inbound::fakes::ScriptedTxData;
use crate::outbound::RefOffer;
use crate::outbound::fakes::InMemoryTxOrderingRefPublisher;
use crate::testkit::{signed_envelope, signer};

/// The text that `record` gives on a fresh recorder.
fn render(record: impl FnOnce()) -> String {
    let recorder = PrometheusBuilder::new().build_recorder();
    metrics::with_local_recorder(&recorder, record);
    recorder.handle().render()
}

/// The value of the one series that starts with `series`, or `None`.
fn value(text: &str, series: &str) -> Option<f64> {
    text.lines()
        .find(|line| line.starts_with(series))
        .and_then(|line| line.rsplit(' ').next())
        .map(|v| v.parse().unwrap())
}

fn offer(nonce: u64) -> RefOffer {
    RefOffer {
        envelope: None,
        tx_ref: TxRef::new(B256::ZERO, 0, BPosition::default(), 0),
        guard: GuardHeader {
            nonce,
            ..GuardHeader::EXEMPT
        },
    }
}

#[test]
fn the_outcome_of_a_pass_names_its_state() {
    assert_eq!(PublishMeters::state_of(&Ok(true)), LoopState::Busy);
    assert_eq!(PublishMeters::state_of(&Ok(false)), LoopState::Idle);
    assert_eq!(
        PublishMeters::state_of(&Err(SequencerError::Backpressure)),
        LoopState::Blocked
    );
    assert_eq!(
        PublishMeters::state_of(&Err(SequencerError::IngressDisconnected)),
        LoopState::Busy
    );
}

#[test]
fn an_offer_records_its_round_trip_and_the_refs_it_took() {
    let text = render(|| {
        let meters = PublishMeters::new();
        let mut refs = InMemoryTxOrderingRefPublisher::default();
        let (published, err) = meters.offer(&mut refs, &[offer(0), offer(1), offer(2)]);
        assert_eq!(published, 3);
        assert!(err.is_none());
    });
    assert_eq!(
        value(
            &text,
            r#"kardamom_step_seconds_count{step="sequencer.offer_round_trip"}"#
        ),
        Some(1.0)
    );
    assert_eq!(
        value(
            &text,
            r#"kardamom_batch_items_sum{batch="sequencer.offer_refs"}"#
        ),
        Some(3.0)
    );
}

#[test]
fn a_refused_offer_records_its_round_trip_and_no_batch() {
    let text = render(|| {
        let meters = PublishMeters::new();
        let mut refs = InMemoryTxOrderingRefPublisher::default();
        *refs.fail_with_backpressure.lock().unwrap() = true;
        let (published, err) = meters.offer(&mut refs, &[offer(0), offer(1)]);
        assert_eq!(published, 0);
        assert!(matches!(err, Some(SequencerError::Backpressure)));
    });
    assert_eq!(
        value(
            &text,
            r#"kardamom_step_seconds_count{step="sequencer.offer_round_trip"}"#
        ),
        Some(1.0)
    );
    let batches = value(
        &text,
        r#"kardamom_batch_items_count{batch="sequencer.offer_refs"}"#,
    );
    assert_eq!(batches.unwrap_or(0.0), 0.0);
}

#[test]
fn the_tx_data_depth_moves_once_in_its_interval_of_passes() {
    let text = render(|| {
        let mut meters = PublishMeters::new();
        let mut input = ScriptedTxData::default();
        let s = signer(1);
        (0..5u64).for_each(|n| {
            input
                .queue
                .push_back((TxDataLoc::default(), signed_envelope(&s, n, 1)));
        });
        let due: Vec<bool> = (0..DEPTH_EVERY)
            .map(|_| meters.pass(LoopState::Idle, &input))
            .collect();
        assert_eq!(due.iter().filter(|d| **d).count(), 1);
        assert_eq!(due.last(), Some(&true));
    });
    assert_eq!(
        value(&text, r#"kardamom_queue_depth{queue="sequencer.tx_data"}"#),
        Some(5.0)
    );
}

#[test]
fn a_back_pressured_pass_counts_as_blocked() {
    let text = render(|| {
        let mut meters = PublishMeters::new();
        let input = ScriptedTxData::default();
        // The meter adds its time to the counters at most every 100 ms.
        std::thread::sleep(Duration::from_millis(110));
        meters.pass(LoopState::Blocked, &input);
    });
    let blocked = value(
        &text,
        r#"kardamom_loop_nanoseconds_total{loop="sequencer.main",state="blocked"}"#,
    )
    .unwrap();
    assert!(blocked >= 100_000_000.0, "blocked {blocked} ns");
    assert_eq!(
        value(
            &text,
            r#"kardamom_loop_nanoseconds_total{loop="sequencer.main",state="busy"}"#
        ),
        Some(0.0)
    );
}

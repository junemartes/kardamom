//! The adapter of a parked publish of the Aeron thread: each outcome of
//! the queue reaches its acknowledgement. The rule of the queue itself is
//! tested in `crate::parked`.

use std::time::Duration;

use crossbeam_channel::Receiver as CbReceiver;
use rkyv::util::AlignedVec;

use super::*;

fn bytes() -> AlignedVec {
    let mut bytes = AlignedVec::new();
    bytes.extend_from_slice(&[0xAA]);
    bytes
}

/// A publish on `pub_id` with a blocking ack, due at `deadline`.
fn blocking(
    pub_id: u32,
    deadline: Instant,
) -> (PendingPublish, CbReceiver<Result<BPosition, LogError>>) {
    let (tx, rx) = crossbeam_channel::bounded(1);
    let publish = PendingPublish {
        stream_id: Some(1),
        pub_id,
        bytes: bytes(),
        ack: Some(PublishAck::Blocking(tx)),
        deadline,
    };
    (publish, rx)
}

/// A publish with an async ack, due at `deadline`.
fn awaited(
    deadline: Instant,
) -> (
    PendingPublish,
    tokio::sync::oneshot::Receiver<Result<BPosition, LogError>>,
) {
    let (tx, rx) = tokio::sync::oneshot::channel();
    let publish = PendingPublish {
        stream_id: Some(1),
        pub_id: 0,
        bytes: bytes(),
        ack: Some(PublishAck::Async(tx)),
        deadline,
    };
    (publish, rx)
}

fn delivered() -> Offered {
    Offered::Delivered(Ok(BPosition {
        term_id: 0,
        term_offset: 64,
    }))
}

#[test]
fn a_blocking_ack_gets_the_position_of_a_delivered_publish() {
    let now = Instant::now();
    let (publish, rx) = blocking(0, now + Duration::from_secs(5));
    let mut queue = ParkedQueue::from_iter([publish]);
    queue.offer_each(now, |_| delivered());
    assert!(queue.is_empty());
    assert!(matches!(rx.try_recv(), Ok(Ok(_))));
}

/// An async ack gets the position of a delivered offer, and the error of
/// an offer that passed its deadline, as a blocking ack does.
#[test]
fn an_async_ack_gets_the_outcome_of_the_offer() {
    let now = Instant::now();
    let (publish, mut ack) = awaited(now + Duration::from_secs(5));
    let mut queue = ParkedQueue::from_iter([publish]);
    queue.offer_each(now, |_| delivered());
    assert!(matches!(ack.try_recv(), Ok(Ok(_))));

    let (publish, mut ack) = awaited(now);
    let mut queue = ParkedQueue::from_iter([publish]);
    queue.offer_each(now, |_| Offered::Refused(-1 /* NOT_CONNECTED */));
    assert!(queue.is_empty());
    assert!(matches!(ack.try_recv(), Ok(Err(LogError::Aeron(_)))));
}

/// A publish on a publication that the table does not hold fails at once,
/// and its ack names the publication id.
#[test]
fn a_publish_on_an_unknown_publication_acks_an_error() {
    let now = Instant::now();
    let (publish, rx) = blocking(9, now + Duration::from_secs(5));
    let mut queue = ParkedQueue::from_iter([publish]);
    queue.offer_each(now, |item| item.offer_on(&[]));
    assert!(queue.is_empty());
    match rx.try_recv() {
        Ok(Err(LogError::Aeron(m))) => assert!(m.contains("unknown pub_id 9"), "{m}"),
        other => panic!("expected an unknown publication error, got {other:?}"),
    }
}

/// A best-effort publish has no ack. Its failure counts the drop on its
/// stream.
#[test]
fn a_failed_best_effort_publish_counts_the_drop_of_its_stream() {
    let recorder = metrics_exporter_prometheus::PrometheusBuilder::new().build_recorder();
    metrics::with_local_recorder(&recorder, || {
        let now = Instant::now();
        let publish = PendingPublish {
            stream_id: Some(1007),
            pub_id: 0,
            bytes: bytes(),
            ack: None,
            deadline: now,
        };
        let mut queue = ParkedQueue::from_iter([publish]);
        queue.offer_each(now, |_| Offered::Refused(-2 /* BACK_PRESSURED */));
        assert!(queue.is_empty());
    });
    let text = recorder.handle().render();
    assert!(
        text.lines()
            .any(|l| l == r#"kardamom_log_best_effort_dropped_total{stream_id="1007"} 1"#),
        "{text}"
    );
}

use std::time::{Duration, Instant};

use kardamom_cluster_adapter::gateway::OfferOutcome;
use kardamom_cluster_adapter::gateway::fakes::{FakeEgress, FakeIngress};
use kardamom_engine::reader::cluster::{ClusterTxOrderingSubscription, RecordedCursorPublisher};

use super::cadence::{CursorCadence, CursorHandoff, SEND_AFTER_RECORDS, SEND_EVERY};

const EXECUTOR: u8 = 2;

fn ms(n: u64) -> Duration {
    Duration::from_millis(n)
}

/// A recorded-cursor publisher of executor [`EXECUTOR`] over `ingress`.
fn publisher(ingress: &FakeIngress) -> RecordedCursorPublisher<FakeIngress> {
    ClusterTxOrderingSubscription::new(FakeEgress::new())
        .with_ingress(ingress.clone())
        .recorded_cursor_publisher(EXECUTOR)
}

/// The cursors that reached the ingress, decoded from the kind-9 frames.
fn sent(ingress: &FakeIngress) -> Vec<u64> {
    ingress
        .accepted()
        .iter()
        .map(|frame| {
            assert_eq!(frame[..2], [9, EXECUTOR], "kind 9 of this executor");
            u64::from_le_bytes(frame[2..10].try_into().expect("8 bytes"))
        })
        .collect()
}

#[test]
fn the_first_cursor_is_due_at_once() {
    let t0 = Instant::now();
    let cadence = CursorCadence::new();
    assert_eq!(cadence.due(None, t0), None, "no cursor yet");
    assert_eq!(cadence.due(Some(0), t0), Some(0));
}

#[test]
fn a_slow_cursor_waits_for_the_time_cadence() {
    let t0 = Instant::now();
    let mut cadence = CursorCadence::new();
    cadence.tried(5, true, t0);
    assert_eq!(
        cadence.due(Some(10), t0 + ms(50)),
        None,
        "50 ms is too early"
    );
    assert_eq!(cadence.due(Some(10), t0 + SEND_EVERY), Some(10));
}

#[test]
fn a_fast_cursor_goes_out_on_the_record_count() {
    let t0 = Instant::now();
    let mut cadence = CursorCadence::new();
    cadence.tried(5, true, t0);
    let near = 5 + SEND_AFTER_RECORDS - 1;
    let far = 5 + SEND_AFTER_RECORDS;
    assert_eq!(cadence.due(Some(near), t0 + ms(1)), None);
    assert_eq!(cadence.due(Some(far), t0 + ms(1)), Some(far));
}

#[test]
fn a_cursor_never_moves_down() {
    let t0 = Instant::now();
    let mut cadence = CursorCadence::new();
    cadence.tried(10, true, t0);
    let later = t0 + SEND_EVERY * 10;
    assert_eq!(cadence.due(Some(10), later), None, "the same cursor");
    assert_eq!(cadence.due(Some(9), later), None, "a lower cursor");
    assert_eq!(cadence.due(Some(11), later), Some(11));
}

#[test]
fn a_refused_cursor_retries_on_the_time_cadence() {
    let t0 = Instant::now();
    let mut cadence = CursorCadence::new();
    cadence.tried(3, false, t0);
    assert_eq!(
        cadence.due(Some(3), t0 + ms(20)),
        None,
        "no retry within 100 ms"
    );
    assert_eq!(cadence.due(Some(3), t0 + SEND_EVERY), Some(3));
}

#[test]
fn the_sender_follows_the_cadence_and_never_sends_a_lower_cursor() {
    let ingress = FakeIngress::new();
    let (handoff, mut sender) = CursorHandoff::new();
    handoff.start(Some(publisher(&ingress)));
    let t0 = Instant::now();
    sender.tick(Some(4), t0);
    sender.tick(Some(6), t0 + ms(20));
    sender.tick(Some(3), t0 + ms(500));
    sender.tick(Some(6 + SEND_AFTER_RECORDS), t0 + ms(510));
    sender.tick(Some(7 + SEND_AFTER_RECORDS), t0 + ms(530));
    sender.tick(Some(8 + SEND_AFTER_RECORDS), t0 + ms(610));
    assert_eq!(
        sent(&ingress),
        [4, 6 + SEND_AFTER_RECORDS, 8 + SEND_AFTER_RECORDS],
        "the first at once, then by count, then by time"
    );
}

#[test]
fn a_refused_send_goes_out_again_when_the_session_takes_it() {
    let ingress = FakeIngress::new();
    let (handoff, mut sender) = CursorHandoff::new();
    handoff.start(Some(publisher(&ingress)));
    let t0 = Instant::now();
    ingress.set_outcome(OfferOutcome::NotConnected);
    sender.tick(Some(4), t0);
    ingress.set_outcome(OfferOutcome::Accepted);
    sender.tick(Some(4), t0 + ms(50));
    sender.tick(Some(4), t0 + SEND_EVERY);
    assert_eq!(sent(&ingress), [4]);
}

#[test]
fn a_handoff_with_no_publisher_sends_nothing() {
    let ingress = FakeIngress::new();
    let (handoff, mut sender) = CursorHandoff::<FakeIngress>::new();
    handoff.start(None);
    let t0 = Instant::now();
    (0..20u64).for_each(|i| sender.tick(Some(i * SEND_AFTER_RECORDS), t0 + SEND_EVERY));
    assert_eq!(sent(&ingress), [] as [u64; 0]);
}

#[test]
fn the_sender_waits_for_the_publisher() {
    let ingress = FakeIngress::new();
    let (handoff, mut sender) = CursorHandoff::new();
    let t0 = Instant::now();
    sender.tick(Some(1), t0);
    assert_eq!(sent(&ingress), [] as [u64; 0], "no publisher yet");
    handoff.start(Some(publisher(&ingress)));
    sender.tick(Some(2), t0 + ms(1));
    assert_eq!(sent(&ingress), [2]);
}

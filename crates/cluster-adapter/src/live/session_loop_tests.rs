use super::*;
use crate::wire::EGRESS_KIND_RELAYED;

/// The instant `ms` after `t0`, and a span of `ms`.
fn at(t0: Instant, ms: u64) -> Instant {
    t0 + Duration::from_millis(ms)
}

fn span(ms: u64) -> Duration {
    Duration::from_millis(ms)
}

#[test]
fn egress_watch_fires_after_silence_and_backs_off() {
    let t0 = Instant::now();
    let mut watch = EgressWatch::new(t0);
    assert_eq!(watch.expired(at(t0, EGRESS_SILENCE_RESET_MS - 1)), None);
    assert_eq!(
        watch.expired(at(t0, EGRESS_SILENCE_RESET_MS)),
        Some(span(EGRESS_SILENCE_RESET_MS)),
        "a silent session resets after one window"
    );
    let t = EGRESS_SILENCE_RESET_MS;
    assert_eq!(
        watch.expired(at(t0, t + EGRESS_SILENCE_RESET_MS)),
        None,
        "a fruitless reset doubles the next window"
    );
    assert_eq!(
        watch.expired(at(t0, t + 2 * EGRESS_SILENCE_RESET_MS)),
        Some(span(2 * EGRESS_SILENCE_RESET_MS))
    );
    let t = at(t0, t + 2 * EGRESS_SILENCE_RESET_MS);
    (0..4).fold(t, |t, _| {
        let next = t + watch.window;
        assert!(watch.expired(next).is_some());
        next
    });
    assert_eq!(watch.window, EgressWatch::RESET_MAX, "the window caps");
}

/// A status with the snapshot notice holds the watchdog for the
/// snapshot budget. A relayed record after the notice keeps the hold;
/// the next boundary or plain status ends it.
#[test]
fn egress_watch_waits_out_a_snapshot_after_its_notice() {
    let notice = crate::wire::encode_status(&kardamom_types::ClusterStatus {
        snapshot_follows: true,
        ..Default::default()
    });
    let budget_ms = 60_000;
    let t0 = Instant::now();
    let mut watch = EgressWatch::new(t0);
    watch.on_frame(at(t0, 100));
    watch.on_payload(&notice, at(t0, 100));
    watch.on_frame(at(t0, 101));
    watch.on_payload(&[EGRESS_KIND_RELAYED], at(t0, 101));
    assert_eq!(
        watch.expired(at(t0, 100 + 3 * EGRESS_SILENCE_RESET_MS)),
        None,
        "a snapshot stall longer than the normal window does not reset the session"
    );
    assert_eq!(
        watch.expired(at(t0, 100 + budget_ms)),
        Some(span(budget_ms - 1)),
        "a stall past the snapshot budget resets the session"
    );

    let mut watch = EgressWatch::new(t0);
    watch.on_payload(&notice, at(t0, 100));
    watch.on_frame(at(t0, 200));
    watch.on_payload(&[EGRESS_KIND_BOUNDARY], at(t0, 200));
    assert_eq!(
        watch.expired(at(t0, 200 + EGRESS_SILENCE_RESET_MS)),
        Some(span(EGRESS_SILENCE_RESET_MS)),
        "the boundary after the snapshot sets the normal window back"
    );
}

#[test]
fn egress_watch_restarts_on_frames_and_connects() {
    let t0 = Instant::now();
    let mut watch = EgressWatch::new(t0);
    assert!(watch.expired(at(t0, EGRESS_SILENCE_RESET_MS)).is_some());
    let t = EGRESS_SILENCE_RESET_MS + 5;
    watch.on_frame(at(t0, t));
    assert_eq!(
        watch.window,
        EgressWatch::RESET,
        "a real frame snaps the window back"
    );
    assert_eq!(watch.expired(at(t0, t + EGRESS_SILENCE_RESET_MS - 1)), None);
    let t = t + EGRESS_SILENCE_RESET_MS - 1;
    watch.on_connect(at(t0, t));
    assert_eq!(
        watch.expired(at(t0, t + EGRESS_SILENCE_RESET_MS - 1)),
        None,
        "a new session gets a full window for its first frame"
    );
}

#[test]
fn replay_ask_resends_until_the_leader_answers() {
    let mut ask = ReplayAsk::new();
    assert!(ask.due(1_000), "a fresh session asks at once");
    assert!(!ask.due(1_000 + REPLAY_RESEND_MS - 1));
    ask.on_payload(EGRESS_KIND_RELAYED);
    ask.on_payload(EGRESS_KIND_BOUNDARY);
    assert!(
        ask.due(1_000 + REPLAY_RESEND_MS),
        "live frames are not an answer: a lost request is resent"
    );
    ask.on_payload(EGRESS_KIND_REPLAY_DONE);
    assert!(
        !ask.due(1_000 + 10 * REPLAY_RESEND_MS),
        "an answered ask never resends"
    );
    ask.rearm();
    assert!(
        ask.due(1_000 + 10 * REPLAY_RESEND_MS),
        "a new session asks again"
    );
    ask.on_payload(EGRESS_KIND_REPLAY_UNAVAILABLE);
    assert!(!ask.due(1_000 + 20 * REPLAY_RESEND_MS));
    ask.rearm();
    ask.on_payload(EGRESS_KIND_REPLAY_AHEAD);
    assert!(!ask.due(1_000 + 30 * REPLAY_RESEND_MS));
}

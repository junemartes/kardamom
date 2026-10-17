use super::*;
use crate::wire::{EGRESS_KIND_BOUNDARY, EGRESS_KIND_RELAYED};

#[test]
fn egress_watch_fires_after_silence_and_backs_off() {
    let mut watch = EgressWatch::new(0);
    assert_eq!(watch.expired(EGRESS_SILENCE_RESET_MS - 1), None);
    assert_eq!(
        watch.expired(EGRESS_SILENCE_RESET_MS),
        Some(EGRESS_SILENCE_RESET_MS),
        "a silent session resets after one window"
    );
    let t = EGRESS_SILENCE_RESET_MS;
    assert_eq!(
        watch.expired(t + EGRESS_SILENCE_RESET_MS),
        None,
        "a fruitless reset doubles the next window"
    );
    assert_eq!(
        watch.expired(t + 2 * EGRESS_SILENCE_RESET_MS),
        Some(2 * EGRESS_SILENCE_RESET_MS)
    );
    let t = t + 2 * EGRESS_SILENCE_RESET_MS;
    (0..4).fold(t, |t, _| {
        let next = t + watch.window_ms;
        assert!(watch.expired(next).is_some());
        next
    });
    assert_eq!(
        watch.window_ms, EGRESS_SILENCE_RESET_MAX_MS,
        "the window caps"
    );
}

#[test]
fn egress_watch_restarts_on_frames_and_connects() {
    let mut watch = EgressWatch::new(0);
    assert!(watch.expired(EGRESS_SILENCE_RESET_MS).is_some());
    let t = EGRESS_SILENCE_RESET_MS + 5;
    watch.on_frame(t);
    assert_eq!(
        watch.window_ms, EGRESS_SILENCE_RESET_MS,
        "a real frame snaps the window back"
    );
    assert_eq!(watch.expired(t + EGRESS_SILENCE_RESET_MS - 1), None);
    let t = t + EGRESS_SILENCE_RESET_MS - 1;
    watch.on_connect(t);
    assert_eq!(
        watch.expired(t + EGRESS_SILENCE_RESET_MS - 1),
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

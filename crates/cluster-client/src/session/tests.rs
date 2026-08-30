use super::*;
use crate::protocol::{
    MessageHeader, NewLeaderEvent, SessionEvent, TEMPLATE_SESSION_CONNECT_REQUEST,
    decode_session_connect_request, encode_new_leader_event, encode_session_event,
};

fn ok_event(correlation_id: i64, session: i64, term: i64, leader: i32) -> Vec<u8> {
    encode_session_event(&SessionEvent {
        cluster_session_id: session,
        correlation_id,
        leadership_term_id: term,
        leader_member_id: leader,
        code: EventCode::Ok,
        detail: String::new(),
    })
}

fn event(code: EventCode, correlation_id: i64, session: i64, detail: &str) -> Vec<u8> {
    encode_session_event(&SessionEvent {
        cluster_session_id: session,
        correlation_id,
        leadership_term_id: 3,
        leader_member_id: 1,
        code,
        detail: detail.to_string(),
    })
}

/// Drive a fresh driver to the Connected state, using the given session id.
/// Return the driver.
fn connected(session: i64) -> SessionDriver {
    let mut d = SessionDriver::new("ch", 1, 1_000);
    let corr = decode_session_connect_request(&d.poll_outbound(0)[0])
        .unwrap()
        .correlation_id;
    d.on_egress(&ok_event(corr, session, 3, 1));
    assert!(d.is_connected());
    d
}

// Regression tests: foreign-session events must be ignored.
//
// The egress endpoint is per-node static config, so the subscription also
// receives events addressed to other sessions. The worst case is a killed
// predecessor process's session corpse timing out after the kill. Acting
// on those events tore down a healthy session and caused a perpetual
// open, timeout, reconnect cycle.

#[test]
fn foreign_closed_event_ignored_while_connected() {
    let mut d = connected(77);
    // The predecessor's session (id 42) times out. Its corpse event
    // arrives on our shared egress channel.
    let evs = d.on_egress(&event(EventCode::Closed, 999, 42, "TIMEOUT"));
    assert!(evs.is_empty(), "foreign Closed must produce no events");
    assert!(d.is_connected(), "our session must survive");
    // Keep-alives keep flowing for our session.
    let frames = d.poll_outbound(2_000);
    assert!(!frames.is_empty(), "keep-alive cadence unaffected");
}

#[test]
fn own_closed_event_still_fails_session() {
    let mut d = connected(77);
    let evs = d.on_egress(&event(EventCode::Closed, 999, 77, "TIMEOUT"));
    assert_eq!(evs, vec![DriverEvent::Failed("TIMEOUT".into())]);
    assert!(!d.is_connected());
}

#[test]
fn foreign_ok_does_not_hijack_connected_session() {
    let mut d = connected(77);
    let evs = d.on_egress(&ok_event(12345, 42, 9, 2));
    assert!(evs.is_empty(), "foreign OK must not re-connect us");
    match d.state() {
        SessionState::Connected {
            cluster_session_id, ..
        } => assert_eq!(*cluster_session_id, 77, "session id must not change"),
        other => panic!("expected Connected, got {other:?}"),
    }
}

#[test]
fn redirect_ignored_while_connected() {
    let mut d = connected(77);
    let evs = d.on_egress(&event(EventCode::Redirect, 999, 0, "1=10.0.0.2:9000"));
    assert!(evs.is_empty(), "a redirect is a connect-time response");
    assert!(d.is_connected());
}

#[test]
fn stale_correlation_rejection_ignored_while_connecting() {
    let mut d = SessionDriver::new("ch", 1, 1_000);
    let corr = decode_session_connect_request(&d.poll_outbound(0)[0])
        .unwrap()
        .correlation_id;
    // A rejection for some other correlation, from a previous process's attempt.
    let evs = d.on_egress(&event(EventCode::Error, corr + 555, 0, "nope"));
    assert!(evs.is_empty(), "stale rejection must not fail our connect");
    // Our own rejection still lands.
    let evs = d.on_egress(&event(EventCode::Error, corr, 0, "nope"));
    assert_eq!(evs, vec![DriverEvent::Failed("nope".into())]);
}

#[test]
fn first_poll_emits_connect_request() {
    let mut d = SessionDriver::new("aeron:udp?endpoint=10.0.0.1:0", 101, 1_000);
    let out = d.poll_outbound(0);
    assert_eq!(out.len(), 1);
    let h = MessageHeader::decode(&out[0]).unwrap();
    assert_eq!(h.template_id, TEMPLATE_SESSION_CONNECT_REQUEST);
    let req = decode_session_connect_request(&out[0]).unwrap();
    assert_eq!(req.response_stream_id, 101);
    assert_eq!(req.response_channel, "aeron:udp?endpoint=10.0.0.1:0");
    // The connect is not re-sent on a subsequent poll.
    assert!(d.poll_outbound(1).is_empty());
}

#[test]
fn ok_session_event_connects() {
    let mut d = SessionDriver::new("ch", 1, 1_000);
    let corr = decode_session_connect_request(&d.poll_outbound(0)[0])
        .unwrap()
        .correlation_id;
    let evs = d.on_egress(&ok_event(corr, 77, 3, 1));
    assert_eq!(
        evs,
        vec![DriverEvent::Connected {
            cluster_session_id: 77
        }]
    );
    assert!(d.is_connected());
}

#[test]
fn wrap_app_only_after_connected() {
    let mut d = SessionDriver::new("ch", 1, 1_000);
    assert!(d.wrap_app(b"x", 0).is_none());
    let corr = decode_session_connect_request(&d.poll_outbound(0)[0])
        .unwrap()
        .correlation_id;
    d.on_egress(&ok_event(corr, 5, 9, 0));
    let framed = d.wrap_app(b"payload", 42).expect("connected");
    match decode_egress(&framed).unwrap() {
        Egress::SessionMessage(m) => {
            assert_eq!(m.cluster_session_id, 5);
            assert_eq!(m.leadership_term_id, 9);
            assert_eq!(m.payload, b"payload");
        }
        other => panic!("expected SessionMessage, got {other:?}"),
    }
}

#[test]
fn redirect_repoints_and_reconnects() {
    let mut d = SessionDriver::new("ch", 1, 1_000);
    let corr = decode_session_connect_request(&d.poll_outbound(0)[0])
        .unwrap()
        .correlation_id;
    let redirect = encode_session_event(&SessionEvent {
        cluster_session_id: -1,
        correlation_id: corr,
        leadership_term_id: 0,
        leader_member_id: 2,
        code: EventCode::Redirect,
        detail: "0=h0:9,1=h1:9,2=h2:9".into(),
    });
    let evs = d.on_egress(&redirect);
    assert_eq!(
        evs,
        vec![DriverEvent::Reconnect {
            leader_member_id: 2,
            ingress_endpoints: "0=h0:9,1=h1:9,2=h2:9".into()
        }]
    );
    // A fresh connect (new correlation id) is queued for the leader.
    let out = d.poll_outbound(1);
    assert_eq!(out.len(), 1);
    let corr2 = decode_session_connect_request(&out[0])
        .unwrap()
        .correlation_id;
    assert_ne!(corr2, corr);
}

#[test]
fn new_leader_event_updates_term_keeps_session() {
    let mut d = SessionDriver::new("ch", 1, 1_000);
    let corr = decode_session_connect_request(&d.poll_outbound(0)[0])
        .unwrap()
        .correlation_id;
    d.on_egress(&ok_event(corr, 5, 9, 0));
    let nl = encode_new_leader_event(&NewLeaderEvent {
        leadership_term_id: 10,
        cluster_session_id: 5,
        leader_member_id: 2,
        ingress_endpoints: "0=h0:9,1=h1:9,2=h2:9".into(),
    });
    let evs = d.on_egress(&nl);
    assert_eq!(
        evs,
        vec![DriverEvent::Reconnect {
            leader_member_id: 2,
            ingress_endpoints: "0=h0:9,1=h1:9,2=h2:9".into()
        }]
    );
    // Still connected, with the same session, and now offers under the new term.
    assert!(d.is_connected());
    let framed = d.wrap_app(b"y", 0).unwrap();
    match decode_egress(&framed).unwrap() {
        Egress::SessionMessage(m) => assert_eq!(m.leadership_term_id, 10),
        other => panic!("expected SessionMessage, got {other:?}"),
    }
    // No reconnect is queued. The session is preserved across the term.
    assert!(d.poll_outbound(1).is_empty());
}

#[test]
fn keep_alive_emitted_after_interval() {
    let mut d = SessionDriver::new("ch", 1, 1_000);
    let corr = decode_session_connect_request(&d.poll_outbound(0)[0])
        .unwrap()
        .correlation_id;
    d.on_egress(&ok_event(corr, 5, 9, 0));
    // Before the interval: nothing.
    assert!(d.poll_outbound(500).is_empty());
    // After the interval: one keep-alive.
    let out = d.poll_outbound(1_100);
    assert_eq!(out.len(), 1);
    assert_eq!(
        super::super::protocol::decode_two_i64(
            &out[0],
            super::super::protocol::TEMPLATE_SESSION_KEEP_ALIVE
        )
        .unwrap(),
        (9, 5)
    );
}

#[test]
fn session_message_for_other_session_ignored() {
    let mut d = SessionDriver::new("ch", 1, 1_000);
    let corr = decode_session_connect_request(&d.poll_outbound(0)[0])
        .unwrap()
        .correlation_id;
    d.on_egress(&ok_event(corr, 5, 9, 0));
    let other = wrap_session_message(9, 999, 0, b"not-ours");
    assert!(d.on_egress(&other).is_empty());
    let ours = wrap_session_message(9, 5, 0, b"ours");
    assert_eq!(
        d.on_egress(&ours),
        vec![DriverEvent::AppMessage(b"ours".to_vec())]
    );
}

#[test]
fn failed_session_reconnects_after_backoff() {
    let mut d = SessionDriver::new("ch", 1, 1_000);
    let corr = decode_session_connect_request(&d.poll_outbound(0)[0])
        .unwrap()
        .correlation_id;
    d.on_egress(&ok_event(corr, 5, 9, 0));
    // The cluster closes the session, for example on a timeout during a quorum outage.
    let closed = encode_session_event(&SessionEvent {
        cluster_session_id: 5,
        correlation_id: 0,
        leadership_term_id: 9,
        leader_member_id: 0,
        code: EventCode::Closed,
        detail: "TIMEOUT".into(),
    });
    assert_eq!(
        d.on_egress(&closed),
        vec![DriverEvent::Failed("TIMEOUT".into())]
    );
    // The backoff is relative to the last connect at t=0, and t=10_000 is
    // long past it, so a fresh connect is queued.
    let out = d.poll_outbound(10_000);
    assert_eq!(out.len(), 1);
    assert_eq!(
        MessageHeader::decode(&out[0]).unwrap().template_id,
        TEMPLATE_SESSION_CONNECT_REQUEST
    );
    assert!(d.take_rotate_hint(), "self-heal retry must hint a rotation");
    assert!(matches!(d.state(), SessionState::Connecting));
    // And the new connect can complete with a fresh correlation id.
    let corr2 = decode_session_connect_request(&out[0])
        .unwrap()
        .correlation_id;
    assert_ne!(corr2, corr);
    d.on_egress(&ok_event(corr2, 6, 10, 1));
    assert!(d.is_connected());
}

#[test]
fn unanswered_connect_reemits_after_timeout_with_rotate_hint() {
    let mut d = SessionDriver::new("ch", 1, 1_000);
    assert_eq!(d.poll_outbound(0).len(), 1);
    assert!(!d.take_rotate_hint(), "first connect is not a retry");
    // Before the connect timeout: nothing.
    assert!(d.poll_outbound(CONNECT_TIMEOUT_MS - 1).is_empty());
    // After: the connect is re-emitted and the transport is told to rotate.
    let out = d.poll_outbound(CONNECT_TIMEOUT_MS);
    assert_eq!(out.len(), 1);
    assert!(d.take_rotate_hint());
    assert_eq!(d.connect_attempts(), 2);
}

#[test]
fn redirect_connect_does_not_hint_rotation() {
    let mut d = SessionDriver::new("ch", 1, 1_000);
    let corr = decode_session_connect_request(&d.poll_outbound(0)[0])
        .unwrap()
        .correlation_id;
    let redirect = encode_session_event(&SessionEvent {
        cluster_session_id: -1,
        correlation_id: corr,
        leadership_term_id: 0,
        leader_member_id: 2,
        code: EventCode::Redirect,
        detail: "0=h0:9,1=h1:9,2=h2:9".into(),
    });
    d.on_egress(&redirect);
    // The redirect-driven connect must go to the member the cluster named,
    // not be rotated away from it.
    assert_eq!(d.poll_outbound(1).len(), 1);
    assert!(!d.take_rotate_hint());
}

// Regression test: a consumer whose session egress goes silent must be
// able to force a re-establishment. This closes the old session, then runs
// the normal Failed, backoff, connect self-heal. Without it, re-requesting
// replay on a session with a dead egress image would loop forever, since
// the cluster serves frames and REPLAY_DONE into an image that no longer
// delivers.
#[test]
fn force_reconnect_closes_old_session_and_reestablishes() {
    let mut d = SessionDriver::new("ch", 1, 1_000);
    let corr = decode_session_connect_request(&d.poll_outbound(0)[0])
        .unwrap()
        .correlation_id;
    d.on_egress(&ok_event(corr, 5, 9, 0));
    assert!(d.is_connected());

    let close = d.force_reconnect("egress silent").expect("was connected");
    // The frame closes exactly the old session under its current term.
    assert_eq!(
        super::super::protocol::decode_two_i64(
            &close,
            super::super::protocol::TEMPLATE_SESSION_CLOSE_REQUEST
        )
        .unwrap(),
        (9, 5)
    );
    // The old session is no longer usable for app messages.
    assert!(matches!(d.state(), SessionState::Failed(_)));
    assert!(d.wrap_app(b"x", 0).is_none());

    // The self-heal path re-emits a connect, with a rotate hint, and the
    // new session connects with a fresh correlation id and session id.
    let out = d.poll_outbound(RECONNECT_BACKOFF_MS + 1);
    assert_eq!(out.len(), 1);
    assert!(d.take_rotate_hint(), "forced reconnect rotates the target");
    let corr2 = decode_session_connect_request(&out[0])
        .unwrap()
        .correlation_id;
    assert_ne!(corr2, corr);
    d.on_egress(&ok_event(corr2, 6, 10, 1));
    assert!(d.is_connected());
    assert!(d.wrap_app(b"y", 0).is_some());
}

#[test]
fn force_reconnect_is_noop_unless_connected() {
    let mut d = SessionDriver::new("ch", 1, 1_000);
    // Connecting: the connect-timeout machinery owns recovery.
    assert!(d.force_reconnect("egress silent").is_none());
    assert!(matches!(d.state(), SessionState::Connecting));
}

#[test]
fn auth_rejected_fails_session() {
    let mut d = SessionDriver::new("ch", 1, 1_000);
    let corr = decode_session_connect_request(&d.poll_outbound(0)[0])
        .unwrap()
        .correlation_id;
    let rej = encode_session_event(&SessionEvent {
        cluster_session_id: -1,
        correlation_id: corr,
        leadership_term_id: 0,
        leader_member_id: 0,
        code: EventCode::AuthenticationRejected,
        detail: "bad creds".into(),
    });
    assert_eq!(
        d.on_egress(&rej),
        vec![DriverEvent::Failed("bad creds".into())]
    );
    assert!(matches!(d.state(), SessionState::Failed(_)));
}

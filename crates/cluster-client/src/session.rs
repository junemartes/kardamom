//! Sans-IO Aeron Cluster client **session driver**.
//!
//! This is the protocol state machine with **no transport**: you feed it egress
//! frames (`on_egress`) and a clock (`poll_outbound`), and it tells you what
//! ingress frames to send and what session events occurred. The live runner
//! (behind the `aeron-live` feature) is a thin loop that offers the outbound
//! frames on an Aeron ingress publication and polls an egress subscription —
//! all the protocol logic lives here so it is deterministically unit-testable
//! without a media driver or a cluster (mirrors the codebase's "logic behind a
//! trait, fakes in tests" convention).
//!
//! Session protocol (from `aeron-cluster-codecs.xml`):
//! ```text
//!   -> SessionConnectRequest
//!   <- SessionEvent(OK)  → connected (clusterSessionId, leadershipTermId)
//!   <- SessionEvent(REDIRECT) → re-connect to the leader in `detail`
//!   -> [SessionMessageHeader|payload | SessionKeepAlive]*
//!   <- [SessionMessageHeader|payload | NewLeaderEvent]*
//! ```

use crate::protocol::{
    Egress, EventCode, decode_egress, encode_session_connect_request, encode_session_keep_alive,
    wrap_session_message,
};

/// App semantic version sent in `SessionConnectRequest.version`. This is the Aeron
/// Cluster *appVersion* the ConsensusModule validates — NOT the SBE schema version
/// (a previous value of 5.4.0 conflated the two). It must be MAJOR 0: Aeron checks
/// it two ways and both require major 0 here — at session connect the client major
/// must equal the cluster's appVersion major, and internally a fresh cluster's
/// leadership-term log version is 0.0.0, so a non-zero major triggers "incompatible
/// version" and the members self-terminate. Pinned to 0.3.0 to match the Java
/// cluster (ClusterNode.APP_VERSION).
// `0 << 16` (the MAJOR lane) is kept explicit to mirror the major.minor.patch
// packing even though major is 0; allow clippy's identity_op for clarity.
#[allow(clippy::identity_op)]
pub const APP_SEMANTIC_VERSION: i32 = (0 << 16) | (3 << 8);

/// Session state exposed to the transport.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SessionState {
    Connecting,
    Connected {
        cluster_session_id: i64,
        leadership_term_id: i64,
        leader_member_id: i32,
    },
    Failed(String),
}

/// Something the transport must act on after feeding an egress frame.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DriverEvent {
    /// Session is open. Begin offering app messages.
    Connected { cluster_session_id: i64 },
    /// Re-point the ingress publication at this leader. `ingress_endpoints` is
    /// the cluster's `memberId=endpoint,…` list (from REDIRECT detail or a
    /// `NewLeaderEvent`). The driver will (re)emit a connect on the next
    /// `poll_outbound` when this came from a REDIRECT.
    Reconnect {
        leader_member_id: i32,
        ingress_endpoints: String,
    },
    /// An application payload arrived on egress (the inner bytes of a
    /// `SessionMessageHeader`).
    AppMessage(Vec<u8>),
    /// The session failed (auth rejected / error / closed).
    Failed(String),
}

/// Sans-IO cluster session driver.
pub struct SessionDriver {
    state: SessionState,
    response_channel: String,
    response_stream_id: i32,
    keep_alive_interval_ms: u64,
    /// Correlation id assigned to the in-flight connect request.
    in_flight_correlation_id: i64,
    /// Next correlation id to hand out.
    next_correlation_id: i64,
    /// Set when a connect request is queued for the next `poll_outbound`.
    pending_connect: bool,
    /// Wall-clock (ms) of the last ingress frame we emitted; gates keep-alives.
    last_emit_ms: u64,
}

impl SessionDriver {
    /// `keep_alive_interval_ms` should be < the cluster's session timeout
    /// (typically a few seconds); a connect is queued immediately.
    pub fn new(response_channel: impl Into<String>, response_stream_id: i32, keep_alive_interval_ms: u64) -> Self {
        Self {
            state: SessionState::Connecting,
            response_channel: response_channel.into(),
            response_stream_id,
            keep_alive_interval_ms,
            in_flight_correlation_id: 0,
            next_correlation_id: 1,
            pending_connect: true,
            last_emit_ms: 0,
        }
    }

    pub fn state(&self) -> &SessionState {
        &self.state
    }

    pub fn is_connected(&self) -> bool {
        matches!(self.state, SessionState::Connected { .. })
    }

    /// Frames the transport should offer on the ingress publication now. Emits
    /// a queued connect request, and a keep-alive when the interval has elapsed
    /// while connected.
    pub fn poll_outbound(&mut self, now_ms: u64) -> Vec<Vec<u8>> {
        let mut out = Vec::new();
        if self.pending_connect {
            self.in_flight_correlation_id = self.next_correlation_id;
            self.next_correlation_id += 1;
            out.push(encode_session_connect_request(
                self.in_flight_correlation_id,
                self.response_stream_id,
                APP_SEMANTIC_VERSION,
                &self.response_channel,
                &[],
                "",
            ));
            self.pending_connect = false;
            self.last_emit_ms = now_ms;
        }
        if let SessionState::Connected {
            cluster_session_id,
            leadership_term_id,
            ..
        } = self.state
            && now_ms.saturating_sub(self.last_emit_ms) >= self.keep_alive_interval_ms
        {
            out.push(encode_session_keep_alive(leadership_term_id, cluster_session_id));
            self.last_emit_ms = now_ms;
        }
        out
    }

    /// Feed one egress frame; returns the resulting session events.
    pub fn on_egress(&mut self, frame: &[u8]) -> Vec<DriverEvent> {
        match decode_egress(frame) {
            Ok(Egress::SessionEvent(ev)) => self.on_session_event(ev),
            Ok(Egress::NewLeader(nl)) => {
                // Same session continues under a new leadership term; re-point
                // ingress but do NOT re-connect (the session id is preserved).
                if let SessionState::Connected {
                    leadership_term_id,
                    leader_member_id,
                    ..
                } = &mut self.state
                {
                    *leadership_term_id = nl.leadership_term_id;
                    *leader_member_id = nl.leader_member_id;
                }
                vec![DriverEvent::Reconnect {
                    leader_member_id: nl.leader_member_id,
                    ingress_endpoints: nl.ingress_endpoints,
                }]
            }
            Ok(Egress::SessionMessage(m)) => {
                // Only surface messages for our session.
                match self.state {
                    SessionState::Connected { cluster_session_id, .. }
                        if cluster_session_id == m.cluster_session_id =>
                    {
                        vec![DriverEvent::AppMessage(m.payload.to_vec())]
                    }
                    _ => Vec::new(),
                }
            }
            Ok(Egress::Other { .. }) | Err(_) => Vec::new(),
        }
    }

    fn on_session_event(&mut self, ev: crate::protocol::SessionEvent) -> Vec<DriverEvent> {
        // Ignore events that do not correspond to our in-flight connect (a
        // stale response from a previous attempt), except async OK/Close for an
        // already-open session.
        match ev.code {
            EventCode::Ok => {
                if ev.correlation_id != self.in_flight_correlation_id && !self.is_connected() {
                    return Vec::new();
                }
                self.state = SessionState::Connected {
                    cluster_session_id: ev.cluster_session_id,
                    leadership_term_id: ev.leadership_term_id,
                    leader_member_id: ev.leader_member_id,
                };
                vec![DriverEvent::Connected {
                    cluster_session_id: ev.cluster_session_id,
                }]
            }
            EventCode::Redirect => {
                // A follower told us who the leader is; re-point ingress and
                // queue a fresh connect to the leader.
                self.pending_connect = true;
                vec![DriverEvent::Reconnect {
                    leader_member_id: ev.leader_member_id,
                    ingress_endpoints: ev.detail,
                }]
            }
            EventCode::Error | EventCode::AuthenticationRejected | EventCode::Closed => {
                let reason = if ev.detail.is_empty() {
                    format!("{:?}", ev.code)
                } else {
                    ev.detail.clone()
                };
                self.state = SessionState::Failed(reason.clone());
                vec![DriverEvent::Failed(reason)]
            }
            EventCode::Unknown(_) => Vec::new(),
        }
    }

    /// Frame an application `payload` for ingress (a `SessionMessageHeader`
    /// wrapping `payload`). `None` until the session is open.
    pub fn wrap_app(&self, payload: &[u8], timestamp: i64) -> Option<Vec<u8>> {
        match self.state {
            SessionState::Connected {
                cluster_session_id,
                leadership_term_id,
                ..
            } => Some(wrap_session_message(
                leadership_term_id,
                cluster_session_id,
                timestamp,
                payload,
            )),
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
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
        let corr = decode_session_connect_request(&d.poll_outbound(0)[0]).unwrap().correlation_id;
        let evs = d.on_egress(&ok_event(corr, 77, 3, 1));
        assert_eq!(evs, vec![DriverEvent::Connected { cluster_session_id: 77 }]);
        assert!(d.is_connected());
    }

    #[test]
    fn wrap_app_only_after_connected() {
        let mut d = SessionDriver::new("ch", 1, 1_000);
        assert!(d.wrap_app(b"x", 0).is_none());
        let corr = decode_session_connect_request(&d.poll_outbound(0)[0]).unwrap().correlation_id;
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
        let corr = decode_session_connect_request(&d.poll_outbound(0)[0]).unwrap().correlation_id;
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
        let corr2 = decode_session_connect_request(&out[0]).unwrap().correlation_id;
        assert_ne!(corr2, corr);
    }

    #[test]
    fn new_leader_event_updates_term_keeps_session() {
        let mut d = SessionDriver::new("ch", 1, 1_000);
        let corr = decode_session_connect_request(&d.poll_outbound(0)[0]).unwrap().correlation_id;
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
        // Still connected (same session), and now offers under the new term.
        assert!(d.is_connected());
        let framed = d.wrap_app(b"y", 0).unwrap();
        match decode_egress(&framed).unwrap() {
            Egress::SessionMessage(m) => assert_eq!(m.leadership_term_id, 10),
            other => panic!("expected SessionMessage, got {other:?}"),
        }
        // No re-connect is queued (the session is preserved across the term).
        assert!(d.poll_outbound(1).is_empty());
    }

    #[test]
    fn keep_alive_emitted_after_interval() {
        let mut d = SessionDriver::new("ch", 1, 1_000);
        let corr = decode_session_connect_request(&d.poll_outbound(0)[0]).unwrap().correlation_id;
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
        let corr = decode_session_connect_request(&d.poll_outbound(0)[0]).unwrap().correlation_id;
        d.on_egress(&ok_event(corr, 5, 9, 0));
        let other = wrap_session_message(9, 999, 0, b"not-ours");
        assert!(d.on_egress(&other).is_empty());
        let ours = wrap_session_message(9, 5, 0, b"ours");
        assert_eq!(d.on_egress(&ours), vec![DriverEvent::AppMessage(b"ours".to_vec())]);
    }

    #[test]
    fn auth_rejected_fails_session() {
        let mut d = SessionDriver::new("ch", 1, 1_000);
        let corr = decode_session_connect_request(&d.poll_outbound(0)[0]).unwrap().correlation_id;
        let rej = encode_session_event(&SessionEvent {
            cluster_session_id: -1,
            correlation_id: corr,
            leadership_term_id: 0,
            leader_member_id: 0,
            code: EventCode::AuthenticationRejected,
            detail: "bad creds".into(),
        });
        assert_eq!(d.on_egress(&rej), vec![DriverEvent::Failed("bad creds".into())]);
        assert!(matches!(d.state(), SessionState::Failed(_)));
    }
}

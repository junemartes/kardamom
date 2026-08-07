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
    Egress, EventCode, decode_egress, encode_session_close_request, encode_session_connect_request,
    encode_session_keep_alive, wrap_session_message,
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

/// How long a connect request may stay unanswered before it is re-emitted
/// (the transport is told to rotate to the next member first — the target may
/// be a dead node).
pub const CONNECT_TIMEOUT_MS: u64 = 3_000;

/// Backoff before a FAILED session (closed/rejected by the cluster, e.g. a
/// session timeout during a quorum outage) queues a fresh connect. A failed
/// session is NOT terminal: the cluster closing our session is an event to
/// recover from, not a reason to go dark (a client that never reconnects
/// silently starves its consumer forever — the executor/validator block on an
/// egress that will never speak again).
pub const RECONNECT_BACKOFF_MS: u64 = 1_000;

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
    /// Wall-clock (ms) of the last connect request emission; gates the
    /// connect-timeout re-emit and the failed-state reconnect backoff.
    last_connect_ms: u64,
    /// Total connect requests emitted. The transport watches this to rotate
    /// its ingress target (round-robin over the member list) whenever a NEW
    /// attempt is emitted after the first — the member we were pointed at may
    /// be gone, and any live member will REDIRECT us to the leader.
    connect_attempts: u64,
    /// Set when the next queued connect is a SELF-HEAL retry (connect timeout
    /// or failed-session backoff) — the transport should rotate its ingress
    /// target before publishing it. NOT set for redirect-driven connects (those
    /// must go to the member the cluster just named).
    rotate_hint: bool,
}

impl SessionDriver {
    /// `keep_alive_interval_ms` should be < the cluster's session timeout
    /// (typically a few seconds); a connect is queued immediately.
    pub fn new(
        response_channel: impl Into<String>,
        response_stream_id: i32,
        keep_alive_interval_ms: u64,
    ) -> Self {
        Self {
            state: SessionState::Connecting,
            response_channel: response_channel.into(),
            response_stream_id,
            keep_alive_interval_ms,
            in_flight_correlation_id: 0,
            next_correlation_id: 1,
            pending_connect: true,
            last_emit_ms: 0,
            last_connect_ms: 0,
            connect_attempts: 0,
            rotate_hint: false,
        }
    }

    /// Total connect requests emitted so far.
    pub fn connect_attempts(&self) -> u64 {
        self.connect_attempts
    }

    /// True (once) when the connect just emitted was a self-heal retry — the
    /// transport should rotate its ingress target round-robin. Redirect-driven
    /// connects never set this.
    pub fn take_rotate_hint(&mut self) -> bool {
        std::mem::take(&mut self.rotate_hint)
    }

    pub fn state(&self) -> &SessionState {
        &self.state
    }

    pub fn is_connected(&self) -> bool {
        matches!(self.state, SessionState::Connected { .. })
    }

    /// Frames the transport should offer on the ingress publication now. Emits
    /// a queued connect request, and a keep-alive when the interval has elapsed
    /// while connected. Self-healing: a FAILED session queues a fresh connect
    /// after [`RECONNECT_BACKOFF_MS`], and an unanswered connect re-emits after
    /// [`CONNECT_TIMEOUT_MS`] (the transport rotates the target member).
    pub fn poll_outbound(&mut self, now_ms: u64) -> Vec<Vec<u8>> {
        match &self.state {
            SessionState::Failed(_)
                if now_ms.saturating_sub(self.last_connect_ms) >= RECONNECT_BACKOFF_MS =>
            {
                self.state = SessionState::Connecting;
                self.pending_connect = true;
                self.rotate_hint = true;
            }
            SessionState::Connecting
                if !self.pending_connect
                    && self.connect_attempts > 0
                    && now_ms.saturating_sub(self.last_connect_ms) >= CONNECT_TIMEOUT_MS =>
            {
                self.pending_connect = true;
                self.rotate_hint = true;
            }
            _ => {}
        }
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
            self.last_connect_ms = now_ms;
            self.connect_attempts += 1;
        }
        if let SessionState::Connected {
            cluster_session_id,
            leadership_term_id,
            ..
        } = self.state
            && now_ms.saturating_sub(self.last_emit_ms) >= self.keep_alive_interval_ms
        {
            out.push(encode_session_keep_alive(
                leadership_term_id,
                cluster_session_id,
            ));
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
                // Foreign-session filter (#99): the shared egress channel also
                // carries other sessions' NewLeaderEvents — only OURS may
                // mutate our term or re-point our ingress.
                if let SessionState::Connected {
                    cluster_session_id,
                    leadership_term_id,
                    leader_member_id,
                    ..
                } = &mut self.state
                {
                    if nl.cluster_session_id != *cluster_session_id {
                        return Vec::new();
                    }
                    *leadership_term_id = nl.leadership_term_id;
                    *leader_member_id = nl.leader_member_id;
                    vec![DriverEvent::Reconnect {
                        leader_member_id: nl.leader_member_id,
                        ingress_endpoints: nl.ingress_endpoints,
                    }]
                } else {
                    Vec::new()
                }
            }
            Ok(Egress::SessionMessage(m)) => {
                // Only surface messages for our session.
                match self.state {
                    SessionState::Connected {
                        cluster_session_id, ..
                    } if cluster_session_id == m.cluster_session_id => {
                        vec![DriverEvent::AppMessage(m.payload.to_vec())]
                    }
                    _ => Vec::new(),
                }
            }
            Ok(Egress::Other { .. }) | Err(_) => Vec::new(),
        }
    }

    fn on_session_event(&mut self, ev: crate::protocol::SessionEvent) -> Vec<DriverEvent> {
        // FOREIGN-SESSION FILTER (#99). The egress endpoint is per-node static
        // config, so this subscription also receives events addressed to OTHER
        // sessions on the same channel — most damagingly a hard-killed
        // predecessor process's session corpse: the cluster times it out
        // ~sessionTimeout after the kill and sends its Closed(TIMEOUT) event
        // here. Acting on that event failed OUR healthy session, reconnected,
        // and ABANDONED it — whose own timeout then killed the replacement 90s
        // later, a perpetual death cycle (observed live: a restarted sequencer
        // replica cycling open→TIMEOUT every ~90s forever, silently degrading
        // its shard to P=1). Every arm below must prove the event is OURS —
        // by cluster_session_id when we hold a session, by connect correlation
        // id when we are establishing one — and ignore everything else.
        match ev.code {
            EventCode::Ok => {
                match self.state {
                    SessionState::Connected {
                        cluster_session_id, ..
                    } => {
                        // Already connected: accept only an idempotent re-OK
                        // for OUR session (refreshing term/leader); a foreign
                        // OK must not overwrite our session state.
                        if ev.cluster_session_id != cluster_session_id {
                            return Vec::new();
                        }
                    }
                    _ => {
                        // Establishing: the OK must answer OUR in-flight
                        // connect attempt.
                        if ev.correlation_id != self.in_flight_correlation_id {
                            return Vec::new();
                        }
                    }
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
                // A follower answering OUR connect attempt with the leader's
                // endpoints. Redirects are connect-time responses: while
                // connected, or for a stale correlation id, a redirect is not
                // ours to act on (a foreign one would force a spurious
                // reconnect and leak our healthy session).
                if self.is_connected() || ev.correlation_id != self.in_flight_correlation_id {
                    return Vec::new();
                }
                self.pending_connect = true;
                vec![DriverEvent::Reconnect {
                    leader_member_id: ev.leader_member_id,
                    ingress_endpoints: ev.detail,
                }]
            }
            EventCode::Error | EventCode::AuthenticationRejected | EventCode::Closed => {
                let ours = match self.state {
                    SessionState::Connected {
                        cluster_session_id, ..
                    } => ev.cluster_session_id == cluster_session_id,
                    // Establishing: connect rejections answer our correlation.
                    _ => ev.correlation_id == self.in_flight_correlation_id,
                };
                if !ours {
                    return Vec::new();
                }
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

    /// Force the CURRENT session to be abandoned and a fresh one established
    /// via the existing self-heal path (Failed → backoff → connect with a
    /// rotate hint). Used by the transport when it has proof the session's
    /// EGRESS path is dead while ingress still works — e.g. a canonical-stream
    /// consumer whose egress image went silent: the cluster keeps serving
    /// replay/live frames into a publication whose subscriber-side image no
    /// longer delivers, the client's cursor never advances, and WITHOUT this
    /// reset the client re-requests replay on the same dead session forever (a
    /// permanent livelock). A NEW session makes the cluster open a NEW egress
    /// publication (a fresh image end-to-end), and the consumer's
    /// replay-on-connect closes the gap from its cursor.
    ///
    /// Returns a `SessionCloseRequest` frame for the old session (send it
    /// best-effort on ingress so the cluster reaps the zombie instead of
    /// keeping it alive on our keep-alives) when we were connected; `None`
    /// (and no state change) otherwise — Connecting/Failed states already have
    /// their own retry machinery.
    pub fn force_reconnect(&mut self, reason: &str) -> Option<Vec<u8>> {
        match self.state {
            SessionState::Connected {
                cluster_session_id,
                leadership_term_id,
                ..
            } => {
                let close = encode_session_close_request(leadership_term_id, cluster_session_id);
                self.state = SessionState::Failed(reason.to_string());
                Some(close)
            }
            _ => None,
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
mod tests;

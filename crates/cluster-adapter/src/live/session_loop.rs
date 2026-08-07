//! The session thread's duty cycle: [`run_session`] builds a [`SessionLoop`]
//! and drives its numbered duty methods until `stop`. The struct's fields are
//! the loop-carried state that used to be `run_session` locals; the methods
//! are the numbered duty sections of the old loop body, comments intact.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread;
use std::time::Duration;

use crossbeam_channel::{Receiver, Sender, TryRecvError};
use kardamom_cluster_client::session::{DriverEvent, SessionDriver};
use kardamom_log::aeron_live::{AeronRuntime, IdleBackoff, PubHandle};

use super::endpoints::{now_ms, open_leader_pub, open_next_member_pub, to_aligned};
use super::{LiveClusterConfig, OfferReq, ReplayOnConnect};
use crate::gateway::OfferOutcome;

// Replay-request resend state: the request is publish()ed on the cluster
// ingress, which right after a (re)connect is often still NOT_CONNECTED —
// a single best-effort send is silently lost and the consumer then waits
// forever for a replay nobody asked for. Resend every REPLAY_RESEND_MS
// until the consumer's cursor ADVANCES (progress = frames flowing again).
const REPLAY_RESEND_MS: u64 = 3_000;
// Egress-liveness watchdog (canonical-stream consumers only): if the
// session is Connected but NO egress frame has arrived for this long, the
// session's egress path is dead — the sealer broadcasts a boundary every
// tick (≤2s) to every session, so a connected consumer can never
// legitimately see 10s of egress silence. This happens when the egress
// subscription's image dies under it (e.g. an unfillable gap after a >2s
// poll stall — the image goes end-of-stream and, with
// no_unavailable_image_handler, is never replaced) while the leader's
// offers to the session still SUCCEED (driver-level flow control keeps
// acking). Without the watchdog the client livelocks forever: cursor
// frozen, replay re-requested every 3s over the healthy ingress path, the
// sealer serving frames + REPLAY_DONE into an image that no longer
// delivers — observed as all cluster-e2e CI shards failing with every
// executor pinned at the same cursor. Forcing a session re-establishment
// makes the cluster open a NEW egress publication (fresh image
// end-to-end) and the consumer's replay-on-connect closes the gap from
// its cursor — the exact recovery REPLAY_FROM exists to make gapless.
// Publisher-only clients (no `replay`) legitimately receive ~no egress,
// so the watchdog is gated on being a consumer.
const EGRESS_SILENCE_RESET_MS: u64 = 10_000;
// Backoff cap for consecutive fruitless resets. A forced re-establishment
// leaks its predecessor session server-side for up to the cluster's 90s
// session timeout, so hammering reconnects at a fixed 10s cadence is a
// zombie-session factory: with several consumers it exhausts the module's
// concurrent-session slots and locks OUT even healthy publishers
// (observed live during a dead-boundary-clock incident: ~400 churned
// sessions, every connect rejected "concurrent session limit"). Doubling
// the window per fruitless reset (up to this cap) keeps the first retry
// fast while capping steady-state churn well below the reap rate; any
// real egress frame resets the backoff.
const EGRESS_SILENCE_RESET_MAX_MS: u64 = 60_000;
// Egress-subscribe announcement resend state: like the replay request,
// the announcement rides the ingress publication which is often not yet
// connected right after (re)connect — resend every SUBSCRIBE_RESEND_MS
// until the first app egress frame proves the service has us in the
// fan-out (a boundary arrives within one tick, ≤2s).
const SUBSCRIBE_RESEND_MS: u64 = 3_000;

/// Shared timing gate for the subscribe-announcement and replay-request
/// resends: both ride the ingress publication (often not yet connected right
/// after a (re)connect, so any single send can be silently lost) and both
/// re-send on a fixed interval until confirmed by egress. `due` fires
/// immediately after [`Resend::rearm`] (session establishment) and every
/// `interval_ms` thereafter, recording the send time when it fires.
struct Resend {
    last_ms: u64,
    interval_ms: u64,
}

impl Resend {
    fn new(interval_ms: u64) -> Self {
        Self {
            last_ms: 0,
            interval_ms,
        }
    }

    /// Force the next [`Resend::due`] to fire immediately.
    fn rearm(&mut self) {
        self.last_ms = 0;
    }

    /// True when a (re)send is due — never sent (or re-armed), or the
    /// interval has elapsed since the last send.
    fn due(&mut self, now: u64) -> bool {
        if self.last_ms == 0 || now.saturating_sub(self.last_ms) >= self.interval_ms {
            self.last_ms = now;
            true
        } else {
            false
        }
    }
}

/// The four channel/stop seams connecting the session thread to its owner:
/// egress frames in from the Aeron subscription, offer requests in from
/// [`LiveIngress`](super::LiveIngress) clones, application payloads out to
/// the [`LiveEgress`](super::LiveEgress), and the stop flag set when the
/// owning [`LiveCluster`](super::LiveCluster) is dropped.
pub(super) struct SessionSeams {
    pub(super) frame_rx: Receiver<Vec<u8>>,
    pub(super) req_rx: Receiver<OfferReq>,
    pub(super) out_tx: Sender<Vec<u8>>,
    pub(super) stop: Arc<AtomicBool>,
}

/// The session thread's state: the sans-IO [`SessionDriver`], the live
/// ingress publication, the owner seams, and the loop-carried duty state.
struct SessionLoop {
    rt: AeronRuntime,
    cfg: LiveClusterConfig,
    driver: SessionDriver,
    // Current ingress target + the member list to rotate through when connect
    // attempts go unanswered (the target may be a dead node; any LIVE member
    // answers a connect — the leader with OK, a follower with a REDIRECT to
    // the leader — so round-robin always converges on the leader). The
    // INITIAL publication is opened by `connect_inner` so its failure fails
    // startup rather than silently killing this thread.
    endpoints: String,
    target_member: i32,
    ingress: PubHandle,
    replay: Option<ReplayOnConnect>,
    subscribe: bool,
    egress_kind_filter: Option<Vec<u8>>,
    frame_rx: Receiver<Vec<u8>>,
    req_rx: Receiver<OfferReq>,
    out_tx: Sender<Vec<u8>>,
    stop: Arc<AtomicBool>,
    // Last time an egress frame arrived OR a session was (re)established —
    // the silence window must restart on connect, or a fresh session would be
    // reset before its first frame had a chance to arrive.
    egress_alive_at_ms: u64,
    // Current watchdog window; doubles per fruitless reset up to
    // EGRESS_SILENCE_RESET_MAX_MS, snaps back on any real egress frame.
    egress_silence_reset_ms: u64,
    replay_resend: Resend,
    replay_cursor_at_send: (u64, u64),
    subscribe_resend: Resend,
    subscribe_confirmed: bool,
    // Whether an egress consumer (a `LiveEgress`) is still attached. A
    // publisher-only client (the sequencer) drops its `LiveEgress`, after which
    // we stop routing application payloads (and never accumulate them) but keep
    // the session — and its keep-alives — alive. The sole terminator is `stop`
    // (set when the owning `LiveCluster` is dropped).
    egress_alive: bool,
    // Escalating idle wait: base 1ms (the established duty-cycle cadence),
    // cap 5ms, grace 5 (~5ms of consecutive emptiness before escalating).
    // Egress frames and offer requests wake the Select immediately whatever
    // the timeout, so only the TIME-based duties (keep-alive emission,
    // reconnect backoff, the egress-silence watchdog) see the coarser tick —
    // all of them run on 100ms+ scales. Profiling showed the fixed 1ms wake
    // costing ~8% of the sequencer's CPU while quiet.
    backoff: IdleBackoff,
}

/// Run the session duty cycle until `stop`: the thin entry point the
/// `connect_inner` spawn calls — it builds the [`SessionLoop`] and loops over
/// its numbered duty methods.
pub(super) fn run_session(
    rt: AeronRuntime,
    cfg: LiveClusterConfig,
    initial: (i32, PubHandle),
    replay: Option<ReplayOnConnect>,
    subscribe: bool,
    egress_kind_filter: Option<Vec<u8>>,
    seams: SessionSeams,
) {
    let mut s = SessionLoop::new(rt, cfg, initial, replay, subscribe, egress_kind_filter, seams);
    while !s.stop.load(Ordering::SeqCst) {
        // Whether THIS iteration moved anything (egress frames, offers, or
        // driver outbound frames) — drives the idle backoff.
        let mut worked = s.drain_egress();
        s.watchdog();
        s.send_subscribe();
        s.send_replay();
        let now = now_ms();
        worked |= s.pump_outbound(now);
        worked |= s.handle_offers(now);
        s.idle_wait(worked);
    }
    s.close_on_shutdown();
}

impl SessionLoop {
    fn new(
        rt: AeronRuntime,
        cfg: LiveClusterConfig,
        initial: (i32, PubHandle),
        replay: Option<ReplayOnConnect>,
        subscribe: bool,
        egress_kind_filter: Option<Vec<u8>>,
        seams: SessionSeams,
    ) -> Self {
        let driver = SessionDriver::new(
            cfg.egress_channel.clone(),
            cfg.egress_stream_id,
            cfg.keep_alive_interval_ms,
        );
        let endpoints = cfg.ingress_endpoints.clone();
        let (target_member, ingress) = initial;
        Self {
            rt,
            cfg,
            driver,
            endpoints,
            target_member,
            ingress,
            replay,
            subscribe,
            egress_kind_filter,
            frame_rx: seams.frame_rx,
            req_rx: seams.req_rx,
            out_tx: seams.out_tx,
            stop: seams.stop,
            egress_alive_at_ms: now_ms(),
            egress_silence_reset_ms: EGRESS_SILENCE_RESET_MS,
            replay_resend: Resend::new(REPLAY_RESEND_MS),
            replay_cursor_at_send: (u64::MAX, u64::MAX),
            subscribe_resend: Resend::new(SUBSCRIBE_RESEND_MS),
            subscribe_confirmed: false,
            egress_alive: true,
            backoff: IdleBackoff::new(Duration::from_millis(1), Duration::from_millis(5), 5),
        }
    }

    /// Duty 1: drain egress fragments through the driver. Returns whether
    /// any frame arrived (feeds `worked`).
    fn drain_egress(&mut self) -> bool {
        let mut worked = false;
        loop {
            match self.frame_rx.try_recv() {
                Ok(frame) => {
                    worked = true;
                    let events = self.driver.on_egress(&frame);
                    // Liveness = frames that SURVIVE the session filter. A
                    // frame for a FOREIGN session (the pre-restart zombie's
                    // boundary broadcasts land on this same static endpoint
                    // until the cluster reaps it at the 90s session timeout)
                    // returns no events and must NOT feed the watchdog —
                    // counting raw channel bytes kept the only escape hatch
                    // (force_reconnect below) disarmed for exactly as long as
                    // a zombie was being served, wedging a restarted
                    // validator in a 3s replay-request loop with a session
                    // the cluster may have silently closed.
                    if !events.is_empty() {
                        self.egress_alive_at_ms = now_ms();
                        // Real egress: the path works again — reset the
                        // watchdog backoff so a FUTURE outage gets the fast
                        // first retry.
                        self.egress_silence_reset_ms = EGRESS_SILENCE_RESET_MS;
                    }
                    for ev in events {
                        self.on_driver_event(ev);
                    }
                }
                Err(TryRecvError::Empty) => break,
                // A dropped LiveIngress/LiveEgress must NOT kill the session
                // (the other half may still be in use); only `stop` terminates.
                Err(TryRecvError::Disconnected) => break,
            }
        }
        worked
    }

    fn on_driver_event(&mut self, ev: DriverEvent) {
        match ev {
            DriverEvent::AppMessage(payload) => {
                self.subscribe_confirmed = true;
                let wanted = self
                    .egress_kind_filter
                    .as_ref()
                    .is_none_or(|ks| payload.first().is_some_and(|k| ks.contains(k)));
                if wanted && self.egress_alive && self.out_tx.send(payload).is_err() {
                    self.egress_alive = false; // consumer dropped
                }
            }
            DriverEvent::Reconnect {
                leader_member_id,
                ingress_endpoints,
            } => {
                if let Some(p) = open_leader_pub(
                    &self.rt,
                    &ingress_endpoints,
                    leader_member_id,
                    self.cfg.ingress_stream_id,
                ) {
                    self.ingress = p;
                    self.endpoints = ingress_endpoints;
                    self.target_member = leader_member_id;
                }
            }
            DriverEvent::Connected { cluster_session_id } => {
                tracing::info!(cluster_session_id, "cluster session opened");
                // Canonical-stream consumers request replay from
                // their delivery cursor on EVERY establishment;
                // force an immediate (re)send below.
                self.replay_resend.rearm();
                self.subscribe_resend.rearm();
                self.subscribe_confirmed = false;
                self.egress_alive_at_ms = now_ms();
            }
            DriverEvent::Failed(reason) => {
                tracing::error!(%reason, "cluster session failed");
            }
        }
    }

    /// Duty 1a: egress-liveness watchdog (consumers only, see
    /// [`EGRESS_SILENCE_RESET_MS`]): a Connected session whose egress has been
    /// silent past the window is dead in the egress direction — force a
    /// re-establishment. The close for the OLD session goes best-effort on
    /// ingress (that direction still works — it kept delivering our replay
    /// requests) so the cluster reaps the zombie instead of keeping it
    /// alive on our keep-alives; the driver then reconnects via its normal
    /// Failed→backoff→connect path and the replay-on-connect below closes
    /// the canonical-stream gap from the cursor.
    fn watchdog(&mut self) {
        if self.replay.is_none() || !self.driver.is_connected() {
            return;
        }
        let now = now_ms();
        let silent_ms = now.saturating_sub(self.egress_alive_at_ms);
        if silent_ms < self.egress_silence_reset_ms {
            return;
        }
        // Fruitless-reset backoff (reset on the next real frame).
        let next_window_ms = (self.egress_silence_reset_ms * 2).min(EGRESS_SILENCE_RESET_MAX_MS);
        tracing::warn!(
            silent_ms,
            next_window_ms,
            "cluster egress silent while connected — forcing session \
             re-establishment (replay-on-connect will close the gap)"
        );
        if let Some(close_frame) = self.driver.force_reconnect("egress silent") {
            self.ingress.publish_best_effort(to_aligned(&close_frame));
        }
        self.egress_alive_at_ms = now;
        self.egress_silence_reset_ms = next_window_ms;
    }

    /// Duty 1a': egress-subscribe announcement (canonical-stream consumers):
    /// send on session establishment and RE-SEND until the first app
    /// egress frame arrives (see [`SUBSCRIBE_RESEND_MS`]).
    fn send_subscribe(&mut self) {
        if !self.subscribe || !self.driver.is_connected() || self.subscribe_confirmed {
            return;
        }
        let now = now_ms();
        if !self.subscribe_resend.due(now) {
            return;
        }
        let req = crate::wire::encode_subscribe();
        if let Some(framed) = self.driver.wrap_app(&req, now as i64) {
            if let Err(e) = self.ingress.publish_bytes(to_aligned(&framed)) {
                tracing::warn!(
                    error = %e,
                    "cluster egress-subscribe publish failed (will resend)"
                );
            } else {
                tracing::info!("cluster egress-subscribe announced");
            }
        }
    }

    /// Duty 1b: replay request (canonical-stream consumers): send on session
    /// establishment and RE-SEND periodically until the consumer's cursor
    /// advances — the ingress publication is often not yet connected right
    /// after a (re)connect, so any single send can be silently lost.
    fn send_replay(&mut self) {
        let Some(r) = &self.replay else { return };
        if !self.driver.is_connected() {
            return;
        }
        let cursor = (
            r.next_index.load(std::sync::atomic::Ordering::Relaxed),
            r.next_block.load(std::sync::atomic::Ordering::Relaxed),
        );
        let now = now_ms();
        let progressed = cursor != self.replay_cursor_at_send;
        if progressed && self.replay_resend.last_ms != 0 {
            // Frames are flowing — move the checkpoint so a FUTURE stall is
            // measured from the most recent progress, not the last send.
            self.replay_cursor_at_send = cursor;
            self.replay_resend.last_ms = now;
        } else if self.replay_resend.due(now) {
            let req = crate::wire::encode_replay_request(cursor.0, cursor.1);
            if let Some(framed) = self.driver.wrap_app(&req, now as i64) {
                // RETRYING publish, not best-effort: this rare, critical
                // message is sent exactly when the ingress publication is
                // at its busiest (mass reconnects under churn) — a
                // best-effort deadline would drop it every 3s in lockstep
                // with the backpressure that caused the stall. Published
                // INLINE on this loop, exactly as on main (validated
                // green in cluster e2e CI): the F05.3 helper-thread
                // offload changed the only client-side ordering in the
                // replay path and is reverted with the F07.3 server-side
                // redesign while the CI freeze is being pinned down; the
                // ack wait is bounded (10s) and the 90s cluster session
                // timeout tolerates it. The Result is still surfaced
                // (F05.3's real defect was DISCARDING it).
                if let Err(e) = self.ingress.publish_bytes(to_aligned(&framed)) {
                    tracing::warn!(
                        error = %e,
                        "cluster replay request publish failed (will resend)"
                    );
                }
                tracing::info!(
                    next_index = cursor.0,
                    next_block = cursor.1,
                    "cluster replay requested"
                );
                self.replay_cursor_at_send = cursor;
            }
        }
    }

    /// Duty 2: connect / keep-alive frames. The driver self-heals (re-emits a
    /// connect on connect-timeout and after a session-failure backoff); when
    /// it emits a RETRY attempt, rotate the ingress target to the next
    /// member id BEFORE publishing it — the member we were pointed at may be
    /// gone, and any live member redirects us to the leader. Returns whether
    /// any frame was emitted (feeds `worked`).
    fn pump_outbound(&mut self, now: u64) -> bool {
        let frames = self.driver.poll_outbound(now);
        let worked = !frames.is_empty();
        if self.driver.take_rotate_hint()
            && let Some((next_member, p)) = open_next_member_pub(
                &self.rt,
                &self.endpoints,
                self.target_member,
                self.cfg.ingress_stream_id,
            )
        {
            self.target_member = next_member;
            self.ingress = p;
            tracing::info!(
                member = self.target_member,
                "cluster session: rotating ingress target for reconnect"
            );
        }
        for frame in frames {
            self.ingress.publish_best_effort(to_aligned(&frame));
        }
        worked
    }

    /// Duty 3: application offers from the ref publisher. Returns whether
    /// any offer was serviced (feeds `worked`).
    fn handle_offers(&mut self, now: u64) -> bool {
        let mut worked = false;
        loop {
            match self.req_rx.try_recv() {
                Ok(OfferReq { payload, reply }) => {
                    worked = true;
                    let outcome = match self.driver.wrap_app(&payload, now as i64) {
                        Some(framed) => match self.ingress.publish_bytes(to_aligned(&framed)) {
                            Ok(_) => OfferOutcome::Accepted,
                            Err(_) => OfferOutcome::BackPressured,
                        },
                        None => OfferOutcome::NotConnected,
                    };
                    let _ = reply.send(outcome);
                }
                Err(TryRecvError::Empty) => break,
                // A dropped LiveIngress/LiveEgress must NOT kill the session
                // (the other half may still be in use); only `stop` terminates.
                Err(TryRecvError::Disconnected) => break,
            }
        }
        worked
    }

    /// Wait for work instead of sleeping through it: block until an egress
    /// frame or an offer request is READY (not consumed — the drain loops
    /// at the top of the iteration consume), capped so the
    /// keep-alive/replay/subscribe duties keep their cadence. The
    /// unconditional 1ms sleep here put up to 1ms of latency under EVERY
    /// sequencer offer (two of these hand-offs per tx), a hard ~1-2k tx/s
    /// cap per shard on the offer path. The cap ESCALATES 1ms → 5ms while
    /// consecutive iterations find nothing (IdleBackoff — the fixed 1ms
    /// wake was ~8% of sequencer CPU); any activity snaps it back.
    fn idle_wait(&mut self, worked: bool) {
        if worked {
            self.backoff.reset();
        }
        let wait = if worked {
            Duration::from_millis(1)
        } else {
            self.backoff.idle_wait()
        };
        let mut sel = crossbeam_channel::Select::new();
        sel.recv(&self.frame_rx);
        sel.recv(&self.req_rx);
        // A DISCONNECTED channel counts as "ready" to crossbeam's Select —
        // and consumers legitimately drop their unused LiveIngress seam
        // (`let (cluster, _ingress, egress) = connect_with_replay(...)`),
        // which disconnects req_rx forever. Readiness with NOTHING actually
        // queued is that artifact, and looping on it busy-spins the session
        // thread at 100% of a core (observed: idle executors/validator at
        // ~157% container CPU; the compact CI stack starved outright —
        // chain-semantics S8 timeout). Fall back to the plain duty-cycle
        // sleep in that case; a frame racing in behind the emptiness checks
        // waits at most `wait`.
        if sel.ready_timeout(wait).is_ok() && self.frame_rx.is_empty() && self.req_rx.is_empty() {
            thread::sleep(wait);
        }
    }

    /// Graceful shutdown: tell the cluster to close our session instead of
    /// leaking it until the 90s session timeout. The zombie is not just
    /// hygiene — the sealer keeps unicasting boundary broadcasts to a dead
    /// session's egress endpoint, and for a restarting consumer on the SAME
    /// static endpoint those foreign frames used to disarm the egress-liveness
    /// watchdog for the whole zombie lifetime (issue #141). Best-effort: on a
    /// crash there is no close either, which is exactly what the watchdog +
    /// session-filtered liveness now cover.
    fn close_on_shutdown(mut self) {
        if let Some(close_frame) = self.driver.force_reconnect("shutdown") {
            self.ingress.publish_best_effort(to_aligned(&close_frame));
        }
    }
}

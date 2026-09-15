//! The session thread's duty cycle. [`run_session`] builds a
//! [`SessionLoop`] and runs its numbered duty methods until `stop`.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread;
use std::time::Duration;

use crossbeam_channel::{Receiver, Sender, TryRecvError};
use kardamom_cluster_client::session::{DriverEvent, SessionDriver};
use kardamom_log::aeron_live::{AeronRuntime, IdleBackoff, PubHandle, RawFrame};

use super::endpoints::{now_ms, now_ms_i64, open_leader_pub, open_next_member_pub, to_aligned};
use super::{LiveClusterConfig, OfferReq, ReplayOnConnect};
use crate::gateway::OfferOutcome;
use crate::wire::{EGRESS_KIND_REPLAY_DONE, EGRESS_KIND_REPLAY_UNAVAILABLE};

// Replay-request resend state. The request is published on the cluster
// ingress, which is often still not connected right after a (re)connect.
// A single best-effort send can be silently lost, and the consumer then
// waits forever for a replay nobody asked for. Resend every
// REPLAY_RESEND_MS until the leader's answer arrives (see `ReplayAsk`).
const REPLAY_RESEND_MS: u64 = 3_000;
// Egress-liveness watchdog (every session). If the session is connected
// but no egress frame has arrived for this long, the session is dead.
// The sealer broadcasts a boundary on every tick (2s or less) to every
// session, publishers included, so a connected client should never
// legitimately see 10s of egress silence. This happens when
// the egress subscription's image dies under it (for example, an
// unfillable gap after a poll stall over 2s: the image reaches
// end-of-stream and, with no_unavailable_image_handler, is never
// replaced), while the leader's offers to the session still succeed
// (driver-level flow control keeps acking). Without the watchdog, the
// client livelocks forever: the cursor freezes, the client re-requests
// replay every 3s over the healthy ingress path, and the sealer serves
// frames and REPLAY_DONE into an image that no longer delivers. Forcing
// a session re-establishment makes the cluster open a new egress
// publication (a fresh image end-to-end), and the consumer's
// replay-on-connect closes the gap from its cursor. This is the exact
// recovery that REPLAY_FROM exists to make gapless.
//
// A publisher-only client (the sequencer) needs the watchdog too. After
// a quorum loss, a restarted leader can stop serving a surviving
// session: no boundary reaches it again, and the consensus module drops
// its offers without a reply, while the local publication still accepts
// them. The client then offers into a void forever (the chaos-cluster
// cpu-squeeze zero-accept, with the sealer's canonical count frozen).
// A new session is the only way out, and a publisher needs no replay.
const EGRESS_SILENCE_RESET_MS: u64 = 10_000;
// Backoff cap for consecutive fruitless resets. A forced
// re-establishment leaks its predecessor session on the server for up
// to the cluster's 90s session timeout. So hammering reconnects at a
// fixed 10s cadence creates zombie sessions: with several consumers,
// this exhausts the module's concurrent-session slots and locks out
// even healthy publishers. Doubling the window on each fruitless reset,
// up to this cap, keeps the first retry fast while capping steady-state
// churn well below the reap rate. Any real egress frame resets the
// backoff.
const EGRESS_SILENCE_RESET_MAX_MS: u64 = 60_000;
// Egress-subscribe announcement resend state. Like the replay request,
// the announcement rides the ingress publication, which is often not
// yet connected right after a (re)connect. Resend every
// SUBSCRIBE_RESEND_MS until the first app egress frame proves the
// service has us in the fan-out (a boundary arrives within one tick, 2s
// or less).
const SUBSCRIBE_RESEND_MS: u64 = 3_000;

/// Shared timing gate for the subscribe-announcement and
/// replay-request resends. Both ride the ingress publication, which is
/// often not yet connected right after a (re)connect, so a single send
/// can be silently lost. Both resend on a fixed interval until egress
/// confirms them. `due` fires immediately after [`Resend::rearm`]
/// (session establishment), and every `interval_ms` after that. It
/// records the send time when it fires.
struct Resend {
    /// `None` means "never sent" (or just [`Resend::rearm`]ed), distinct
    /// from a real send timestamp of 0 ms since epoch.
    last_ms: Option<u64>,
    interval_ms: u64,
}

impl Resend {
    fn new(interval_ms: u64) -> Self {
        Self {
            last_ms: None,
            interval_ms,
        }
    }

    /// Force the next [`Resend::due`] to fire immediately.
    fn rearm(&mut self) {
        self.last_ms = None;
    }

    /// True when a (re)send is due: either it was never sent (or was
    /// re-armed), or the interval has passed since the last send.
    fn due(&mut self, now: u64) -> bool {
        let due = match self.last_ms {
            None => true,
            Some(last) => now.saturating_sub(last) >= self.interval_ms,
        };
        if due {
            self.last_ms = Some(now);
        }
        due
    }
}

/// The replay request's resend gate. The leader answers every request it
/// receives: `REPLAY_DONE` after the retained frames, or
/// `REPLAY_UNAVAILABLE`. So the request is resent on the
/// [`REPLAY_RESEND_MS`] cadence until one of those markers arrives on
/// this session's egress, and never after. The consumer's delivery
/// cursor is not the signal: it moves at the consumer's own pace, and an
/// executor that waits on a join miss holds it still for ten seconds
/// while the replay is already in flight. Resending on a still cursor
/// made the leader serve the same thousands of retained frames up to
/// three times per request, which is the egress load behind the
/// back-pressure closes of issue #292.
struct ReplayAsk {
    resend: Resend,
    answered: bool,
}

impl ReplayAsk {
    fn new() -> Self {
        Self {
            resend: Resend::new(REPLAY_RESEND_MS),
            answered: false,
        }
    }

    /// A new session: the next [`ReplayAsk::due`] fires at once, and an
    /// answer to the previous session's request no longer counts.
    fn rearm(&mut self) {
        self.resend.rearm();
        self.answered = false;
    }

    /// Note the kind byte of one egress payload for this session. The
    /// two replay markers close the ask; live frames do not.
    fn on_payload(&mut self, kind: u8) {
        if kind == EGRESS_KIND_REPLAY_DONE || kind == EGRESS_KIND_REPLAY_UNAVAILABLE {
            self.answered = true;
        }
    }

    /// Whether to (re)send the request now. Records the send time when
    /// it fires.
    fn due(&mut self, now: u64) -> bool {
        !self.answered && self.resend.due(now)
    }
}

/// The egress-silence window of one session (see
/// [`EGRESS_SILENCE_RESET_MS`]). A frame for this session, or a new
/// session, restarts the window. A fruitless reset doubles the window, up
/// to [`EGRESS_SILENCE_RESET_MAX_MS`], and a real frame snaps it back.
struct EgressWatch {
    alive_at_ms: u64,
    window_ms: u64,
}

impl EgressWatch {
    fn new(now: u64) -> Self {
        Self {
            alive_at_ms: now,
            window_ms: EGRESS_SILENCE_RESET_MS,
        }
    }

    /// A frame for this session arrived: the path works.
    fn on_frame(&mut self, now: u64) {
        self.alive_at_ms = now;
        self.window_ms = EGRESS_SILENCE_RESET_MS;
    }

    /// A session was (re)established. The window restarts, so a fresh
    /// session is not reset before its first frame can arrive.
    fn on_connect(&mut self, now: u64) {
        self.alive_at_ms = now;
    }

    /// The silence to report when the window has passed, or `None`.
    /// When it fires, the next window doubles and starts now.
    fn expired(&mut self, now: u64) -> Option<u64> {
        let silent_ms = now.saturating_sub(self.alive_at_ms);
        if silent_ms < self.window_ms {
            return None;
        }
        self.alive_at_ms = now;
        self.window_ms = (self.window_ms * 2).min(EGRESS_SILENCE_RESET_MAX_MS);
        Some(silent_ms)
    }
}

/// The four channel and stop seams that connect the session thread to
/// its owner: egress frames in from the Aeron subscription, offer
/// requests in from [`LiveIngress`](super::LiveIngress) clones,
/// application payloads out to the [`LiveEgress`](super::LiveEgress), and
/// the stop flag set when the owning [`LiveCluster`](super::LiveCluster)
/// is dropped.
pub(super) struct SessionSeams {
    pub(super) frame_rx: Receiver<RawFrame>,
    pub(super) req_rx: Receiver<OfferReq>,
    pub(super) out_tx: Sender<Vec<u8>>,
    pub(super) stop: Arc<AtomicBool>,
}

/// Pop one item off `rx`, or `None` on `Empty`. On `Disconnected`, latches
/// `*dead` and also returns `None`. Allocation-free: unlike a `drain`-style
/// helper that collects into a `Vec` first, this borrows `rx`/`dead` only
/// for the call itself, so the caller's `while let` loop body is free to
/// borrow `self` again (for example, `self.on_driver_event(ev)`) on every
/// iteration — required on this thread's offer path, which the module doc
/// calls latency-critical.
fn next_item<T>(rx: &Receiver<T>, dead: &mut bool) -> Option<T> {
    match rx.try_recv() {
        Ok(item) => Some(item),
        Err(TryRecvError::Empty) => None,
        Err(TryRecvError::Disconnected) => {
            *dead = true;
            None
        }
    }
}

/// The session thread's state. It holds the sans-IO [`SessionDriver`],
/// the live ingress publication, the owner seams, and the loop-carried
/// duty state.
#[allow(
    clippy::struct_excessive_bools,
    reason = "independent flags on unrelated axes (an egress consumer detached, a receiver died, a subscribe was confirmed), not a state machine to collapse into one enum"
)]
struct SessionLoop {
    rt: AeronRuntime,
    cfg: LiveClusterConfig,
    driver: SessionDriver,
    // Current ingress target, plus the member list to rotate through when
    // connect attempts go unanswered. The target may be a dead node; any
    // live member answers a connect (the leader with OK, a follower with a
    // REDIRECT to the leader), so round-robin always converges on the
    // leader. `connect_inner` opens the initial publication, so its
    // failure fails startup instead of silently killing this thread.
    endpoints: String,
    target_member: i32,
    ingress: PubHandle,
    replay: Option<ReplayOnConnect>,
    subscribe: bool,
    egress_kind_filter: Option<Vec<u8>>,
    frame_rx: Receiver<RawFrame>,
    req_rx: Receiver<OfferReq>,
    /// Set when a drain sees `Disconnected` on the receiver. A dead
    /// receiver stays out of the idle Select (see `idle_wait`).
    frame_rx_dead: bool,
    req_rx_dead: bool,
    out_tx: Sender<Vec<u8>>,
    stop: Arc<AtomicBool>,
    egress_watch: EgressWatch,
    replay_ask: ReplayAsk,
    subscribe_resend: Resend,
    subscribe_confirmed: bool,
    // Whether an egress consumer (a `LiveEgress`) is still attached. A
    // publisher-only client (the sequencer) drops its `LiveEgress`. After
    // that, the loop stops routing application payloads (and never
    // accumulates them), but keeps the session, and its keep-alives,
    // alive. The only thing that stops the session is `stop` (set when
    // the owning `LiveCluster` is dropped).
    egress_alive: bool,
    // Escalating idle wait. It starts at 1ms (the established duty-cycle
    // cadence), caps at 5ms, with a grace of 5 (about 5ms of consecutive
    // emptiness before it escalates). Egress frames and offer requests
    // wake the Select immediately, whatever the timeout. So only the
    // time-based duties (keep-alive emission, reconnect backoff, the
    // egress-silence watchdog) see the coarser tick, and all of those run
    // on 100ms-plus scales. Profiling showed the fixed 1ms wake cost
    // about 8% of the sequencer's CPU while quiet.
    backoff: IdleBackoff,
}

/// Run the session duty cycle until `stop`. This is the thin entry
/// point that the `connect_inner` spawn calls. It builds the
/// [`SessionLoop`] and loops over its numbered duty methods.
pub(super) fn run_session(
    rt: AeronRuntime,
    cfg: LiveClusterConfig,
    initial: (i32, PubHandle),
    replay: Option<ReplayOnConnect>,
    subscribe: bool,
    egress_kind_filter: Option<Vec<u8>>,
    seams: SessionSeams,
) {
    let mut s = SessionLoop::new(
        rt,
        cfg,
        initial,
        replay,
        subscribe,
        egress_kind_filter,
        seams,
    );
    while !s.stop.load(Ordering::SeqCst) {
        // Whether this iteration moved anything (egress frames, offers, or
        // driver outbound frames). This drives the idle backoff.
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
            frame_rx_dead: false,
            req_rx_dead: false,
            out_tx: seams.out_tx,
            stop: seams.stop,
            egress_watch: EgressWatch::new(now_ms()),
            replay_ask: ReplayAsk::new(),
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
        while let Some(frame) = next_item(&self.frame_rx, &mut self.frame_rx_dead) {
            worked = true;
            self.on_egress_frame(&frame);
        }
        worked
    }

    /// Handle one egress fragment: feed the watchdog when at least one
    /// event survives the session filter, then fan every event out to
    /// [`Self::on_driver_event`].
    fn on_egress_frame(&mut self, frame: &RawFrame) {
        let events = self.driver.on_egress(&frame.bytes);
        // Liveness means frames that survive the session filter. A frame
        // for a foreign session (the pre-restart zombie's boundary
        // broadcasts land on this same static endpoint until the cluster
        // reaps it at the 90s session timeout) returns no events, and
        // must not feed the watchdog: only session-filtered events do.
        if !events.is_empty() {
            self.egress_watch.on_frame(now_ms());
        }
        for ev in events {
            self.on_driver_event(ev);
        }
    }

    fn on_driver_event(&mut self, ev: DriverEvent) {
        match ev {
            DriverEvent::AppMessage(payload) => self.on_app_message(payload),
            DriverEvent::Reconnect {
                leader_member_id,
                ingress_endpoints,
            } => self.on_reconnect(leader_member_id, ingress_endpoints),
            DriverEvent::Connected { cluster_session_id } => {
                tracing::info!(cluster_session_id, "cluster session opened");
                // Canonical-stream consumers request replay from their
                // delivery cursor on every establishment. Force an
                // immediate (re)send below.
                self.replay_ask.rearm();
                self.subscribe_resend.rearm();
                self.subscribe_confirmed = false;
                self.egress_watch.on_connect(now_ms());
            }
            DriverEvent::Failed(reason) => {
                tracing::error!(%reason, "cluster session failed");
            }
        }
    }

    /// One app payload from egress. The session filter already passed, so
    /// this confirms the subscribe. The kind filter then decides whether the
    /// consumer sees the payload. A closed consumer channel ends the egress
    /// direction.
    fn on_app_message(&mut self, payload: Vec<u8>) {
        self.subscribe_confirmed = true;
        // The replay gate reads the kind byte before the egress filter:
        // a REPLAY_DONE or REPLAY_UNAVAILABLE marker ends the resends
        // even when the consumer does not want the frame.
        if let Some(&kind) = payload.first() {
            self.replay_ask.on_payload(kind);
        }
        let wanted = self
            .egress_kind_filter
            .as_ref()
            .is_none_or(|ks| payload.first().is_some_and(|k| ks.contains(k)));
        if wanted && self.egress_alive && self.out_tx.send(payload).is_err() {
            self.egress_alive = false;
        }
    }

    /// Re-point ingress at a new leader. The endpoints and the target member
    /// move together with the publication, so a failed open keeps all three
    /// on the old leader.
    fn on_reconnect(&mut self, leader_member_id: i32, ingress_endpoints: String) {
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

    /// Duty 1a: egress-liveness watchdog (every session, see
    /// [`EGRESS_SILENCE_RESET_MS`]). A connected session whose egress has
    /// been silent past the window is dead, so this forces a
    /// re-establishment. The close for the old session
    /// goes best-effort on ingress (that direction still works: it kept
    /// delivering our replay requests), so the cluster reaps the zombie
    /// instead of keeping it alive on our keep-alives. The driver then
    /// reconnects through its normal Failed-backoff-connect path, and
    /// the replay-on-connect below closes the canonical-stream gap from
    /// the cursor.
    fn watchdog(&mut self) {
        if !self.driver.is_connected() {
            return;
        }
        let Some(silent_ms) = self.egress_watch.expired(now_ms()) else {
            return;
        };
        tracing::warn!(
            silent_ms,
            next_window_ms = self.egress_watch.window_ms,
            "cluster egress silent while connected — forcing session \
             re-establishment (replay-on-connect will close the gap)"
        );
        if let Some(close_frame) = self.driver.force_reconnect("egress silent") {
            self.ingress.publish_best_effort(to_aligned(&close_frame));
        }
    }

    /// Duty 1a': egress-subscribe announcement (canonical-stream
    /// consumers). Send on session establishment, and resend until the
    /// first app egress frame arrives (see [`SUBSCRIBE_RESEND_MS`]).
    fn send_subscribe(&mut self) {
        if !self.subscribe || !self.driver.is_connected() || self.subscribe_confirmed {
            return;
        }
        let now = now_ms();
        if !self.subscribe_resend.due(now) {
            return;
        }
        let req = crate::wire::encode_subscribe();
        if let Some(framed) = self.driver.wrap_app(&req, now_ms_i64(now)) {
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

    /// Duty 1b: replay request (canonical-stream consumers). Send on
    /// session establishment, and resend periodically until the
    /// consumer's cursor advances. The ingress publication is often not
    /// yet connected right after a (re)connect, so a single send can be
    /// silently lost.
    fn send_replay(&mut self) {
        let Some(r) = &self.replay else { return };
        if !self.driver.is_connected() || !self.replay_ask.due(now_ms()) {
            return;
        }
        let cursor = (
            r.next_index.load(std::sync::atomic::Ordering::Relaxed),
            r.next_block.load(std::sync::atomic::Ordering::Relaxed),
        );
        self.publish_replay_request(cursor, now_ms());
    }

    /// Send one replay request for `cursor`. The driver drops the request
    /// while the session is not established, and the gate then resends it.
    fn publish_replay_request(&mut self, cursor: (u64, u64), now: u64) {
        let req = crate::wire::encode_replay_request(cursor.0, cursor.1);
        let Some(framed) = self.driver.wrap_app(&req, now_ms_i64(now)) else {
            return;
        };
        // This is a retrying publish, not best-effort. This rare, critical
        // message is sent exactly when the ingress publication is at its
        // busiest (mass reconnects under churn). A best-effort deadline
        // would drop it every 3s, in lockstep with the backpressure that
        // caused the stall. This call runs inline on this loop, not on a
        // helper thread, to keep client-side ordering in the replay path
        // correct. The ack wait is bounded (10s), and the 90s cluster
        // session timeout tolerates it. The `Result` is still checked, not
        // discarded.
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
    }

    /// Duty 2: connect and keep-alive frames. The driver self-heals: it
    /// re-emits a connect on connect-timeout and after a session-failure
    /// backoff. When it emits a retry attempt, rotate the ingress target
    /// to the next member ID before publishing it. The member we were
    /// pointed at may be gone, and any live member redirects us to the
    /// leader. Returns whether any frame was emitted (feeds `worked`).
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
        while let Some(req) = next_item(&self.req_rx, &mut self.req_rx_dead) {
            worked = true;
            self.handle_one_offer(req, now);
        }
        worked
    }

    /// One offer request, for [`Self::handle_offers`]'s loop: wrap and
    /// publish the payload, then reply with the outcome.
    fn handle_one_offer(&mut self, req: OfferReq, now: u64) {
        let OfferReq { payload, reply } = req;
        let outcome = match self.driver.wrap_app(&payload, now_ms_i64(now)) {
            Some(framed) => match self.ingress.publish_bytes(to_aligned(&framed)) {
                Ok(_) => OfferOutcome::Accepted,
                Err(_) => OfferOutcome::BackPressured,
            },
            None => OfferOutcome::NotConnected,
        };
        let _ = reply.send(outcome);
    }

    /// Wait for work instead of sleeping through it. Block until an
    /// egress frame or an offer request is ready (not consumed; the
    /// drain loops at the top of the iteration consume it), capped so
    /// the keep-alive, replay, and subscribe duties keep their cadence.
    /// The cap climbs from 1ms to 5ms while consecutive iterations find
    /// nothing (`IdleBackoff`). Any activity snaps it back.
    fn idle_wait(&mut self, worked: bool) {
        if worked {
            self.backoff.reset();
        }
        let wait = if worked {
            Duration::from_millis(1)
        } else {
            self.backoff.idle_wait()
        };
        // A disconnected channel counts as "ready" to crossbeam's Select.
        // Consumers legitimately drop their unused `LiveIngress` seam (for
        // example, `let (cluster, _ingress, egress) =
        // connect_with_replay(...)`), which disconnects `req_rx` forever.
        // Build the Select from live receivers only, so readiness means
        // something is queued, and `ready_timeout` alone parks correctly.
        // A dead and empty channel stays dead: nothing can arrive on it
        // again.
        let mut sel = crossbeam_channel::Select::new();
        let mut live = 0;
        if !self.frame_rx_dead {
            sel.recv(&self.frame_rx);
            live += 1;
        }
        if !self.req_rx_dead {
            sel.recv(&self.req_rx);
            live += 1;
        }
        if live == 0 {
            // Every producer is gone: nothing can arrive; plain duty-cycle
            // sleep keeps the keep-alive/replay cadence.
            thread::sleep(wait);
            return;
        }
        let _ = sel.ready_timeout(wait);
    }

    /// Graceful shutdown: tell the cluster to close our session, instead
    /// of leaking it until the 90s session timeout. The zombie is not
    /// just hygiene. The sealer keeps unicasting boundary broadcasts to a
    /// dead session's egress endpoint, and a restarting consumer on the
    /// same static endpoint would otherwise receive those foreign frames.
    /// Session-filtered liveness (`drain_egress`) ignores them, and the
    /// egress-silence watchdog covers the case where this close is
    /// skipped (for example, a crash).
    fn close_on_shutdown(mut self) {
        if let Some(close_frame) = self.driver.force_reconnect("shutdown") {
            self.ingress.publish_best_effort(to_aligned(&close_frame));
        }
    }
}

#[cfg(test)]
mod tests {
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
    }
}

//! Live cluster gateway: drives the sans-IO [`SessionDriver`] over a real Aeron
//! ingress publication + egress subscription (via `kardamom_log`'s
//! [`AeronRuntime`]).
//!
//! A dedicated session thread owns the driver and runs the cluster duty cycle:
//! it offers connect/keep-alive/app frames on the ingress publication, feeds
//! egress fragments into the driver, surfaces application payloads to
//! [`ClusterEgress::recv`], and re-points the ingress publication to the new
//! leader on a `NewLeaderEvent`/REDIRECT. The [`ClusterIngress`] /
//! [`ClusterEgress`] seams it exposes are what the trait adapters consume — so
//! the sequencer/executor wiring is identical whether backed by this live
//! gateway or the in-memory fakes.
//!
//! This module is the IO half of the Rust-native cluster client: its protocol
//! correctness is covered by `kardamom-cluster-client`'s deterministic
//! `SessionDriver` tests; end-to-end behaviour against a real cluster is
//! exercised by the (gated) docker e2e (see `tests/`).

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread::{self, JoinHandle};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use crossbeam_channel::{Receiver, Sender, TryRecvError, bounded, unbounded};
use kardamom_cluster_client::session::{DriverEvent, SessionDriver};
use kardamom_log::aeron_live::{AeronRuntime, DeliverFn, PubHandle};
use rkyv::util::AlignedVec;
use thiserror::Error;

use crate::gateway::{ClusterEgress, ClusterIngress, OfferOutcome};

#[derive(Debug, Error)]
#[error("live cluster gateway: {0}")]
pub struct LiveError(String);

/// Configuration for a live cluster connection.
pub struct LiveClusterConfig {
    /// Cluster member ingress endpoints as `memberId=host:port,…` (the same
    /// form the cluster sends in `NewLeaderEvent`/REDIRECT).
    pub ingress_endpoints: String,
    /// Member id to connect to first (the presumed leader).
    pub initial_leader_member_id: i32,
    /// Aeron stream id for cluster ingress.
    pub ingress_stream_id: i32,
    /// This client's egress (response) channel URI.
    pub egress_channel: String,
    /// Aeron stream id for cluster egress.
    pub egress_stream_id: i32,
    /// Keep-alive cadence (ms); must be < the cluster's session timeout.
    pub keep_alive_interval_ms: u64,
}

struct OfferReq {
    payload: Vec<u8>,
    reply: Sender<OfferOutcome>,
}

/// Owns the session thread; dropping it stops the session cleanly.
pub struct LiveCluster {
    stop: Arc<AtomicBool>,
    join: Option<JoinHandle<()>>,
}

impl Drop for LiveCluster {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::SeqCst);
        if let Some(j) = self.join.take() {
            let _ = j.join();
        }
    }
}

/// `ClusterIngress` over the live session thread.
///
/// `Clone` shares the single session thread: every clone offers through the
/// same `req_tx`, and the session thread serialises those offers — the correct
/// single-writer behaviour when two producer threads (e.g. the sequencer's main
/// loop + deposit pump) publish through one cluster session.
#[derive(Clone)]
pub struct LiveIngress {
    req_tx: Sender<OfferReq>,
}

impl ClusterIngress for LiveIngress {
    fn offer(&mut self, payload: &[u8]) -> OfferOutcome {
        let (reply_tx, reply_rx) = bounded(1);
        if self
            .req_tx
            .send(OfferReq {
                payload: payload.to_vec(),
                reply: reply_tx,
            })
            .is_err()
        {
            return OfferOutcome::NotConnected; // session thread gone
        }
        reply_rx.recv().unwrap_or(OfferOutcome::NotConnected)
    }
}

/// `ClusterEgress` over the live session thread.
pub struct LiveEgress {
    out_rx: Receiver<Vec<u8>>,
}

impl ClusterEgress for LiveEgress {
    fn recv(&mut self) -> Option<Vec<u8>> {
        self.out_rx.recv().ok()
    }
}

/// What the session thread sends on every session establishment when the
/// client is a canonical-stream CONSUMER: a `REPLAY_FROM(next_index,
/// next_block)` request composed from the consumer's live delivery cursor
/// (shared atomics, written by the subscription on every delivery). This is
/// what makes the canonical stream gapless across session loss — without it,
/// frames committed between session death and re-connect are missed forever.
#[derive(Clone)]
pub struct ReplayOnConnect {
    pub next_index: std::sync::Arc<std::sync::atomic::AtomicU64>,
    pub next_block: std::sync::Arc<std::sync::atomic::AtomicU64>,
}

/// Connect to the cluster, spawning the session thread. Returns the lifetime
/// guard plus the ingress/egress seams for the trait adapters.
pub fn connect(
    rt: AeronRuntime,
    cfg: LiveClusterConfig,
) -> Result<(LiveCluster, LiveIngress, LiveEgress), LiveError> {
    connect_inner(rt, cfg, None)
}

/// [`connect`], plus a replay request on every session establishment.
pub fn connect_with_replay(
    rt: AeronRuntime,
    cfg: LiveClusterConfig,
    replay: ReplayOnConnect,
) -> Result<(LiveCluster, LiveIngress, LiveEgress), LiveError> {
    connect_inner(rt, cfg, Some(replay))
}

/// Append a small term-length to a cluster control channel unless the URI
/// already pins one. Cluster ingress/egress carry KB/s of control + canonical
/// frames, but Aeron's default 16MB terms allocate a ~50MB log PER publication
/// image — on both the client's AND every member's aeron tmpfs. Session churn
/// (chaos failovers, zombie-close reconnects) at 50MB a pop exhausts the 1GB
/// tmpfs, and the clustered SERVICE then dies with "insufficient usable
/// storage" while Raft itself stays healthy — observed live as the
/// post-failover pipeline freeze. 1MB terms cut every such log 16x.
fn with_control_term_length(uri: &str) -> String {
    if uri.contains("term-length") {
        uri.to_string()
    } else {
        format!("{uri}|term-length=1m")
    }
}

fn connect_inner(
    rt: AeronRuntime,
    mut cfg: LiveClusterConfig,
    replay: Option<ReplayOnConnect>,
) -> Result<(LiveCluster, LiveIngress, LiveEgress), LiveError> {
    cfg.egress_channel = with_control_term_length(&cfg.egress_channel);
    // Egress subscription: the deliver closure ships each raw frame to the
    // session thread.
    let (frame_tx, frame_rx) = unbounded::<Vec<u8>>();
    // Egress frames are relayed verbatim; the cluster assigns the canonical
    // index, so the Aeron position/session of the egress image are irrelevant.
    let deliver: DeliverFn = Box::new(move |bytes: &[u8], _pos, _session| {
        let _ = frame_tx.send(bytes.to_vec());
    });
    rt.open_subscription_with_deliver(&cfg.egress_channel, cfg.egress_stream_id, deliver)
        .map_err(|e| LiveError(format!("open egress subscription: {e}")))?;

    let (req_tx, req_rx) = unbounded::<OfferReq>();
    let (out_tx, out_rx) = unbounded::<Vec<u8>>();
    let stop = Arc::new(AtomicBool::new(false));
    let stop_thread = stop.clone();

    let join = thread::Builder::new()
        .name("cluster-session".into())
        .spawn(move || run_session(rt, cfg, replay, frame_rx, req_rx, out_tx, stop_thread))
        .map_err(|e| LiveError(format!("spawn session thread: {e}")))?;

    Ok((
        LiveCluster {
            stop,
            join: Some(join),
        },
        LiveIngress { req_tx },
        LiveEgress { out_rx },
    ))
}

fn run_session(
    rt: AeronRuntime,
    cfg: LiveClusterConfig,
    replay: Option<ReplayOnConnect>,
    frame_rx: Receiver<Vec<u8>>,
    req_rx: Receiver<OfferReq>,
    out_tx: Sender<Vec<u8>>,
    stop: Arc<AtomicBool>,
) {
    let mut driver = SessionDriver::new(
        cfg.egress_channel.clone(),
        cfg.egress_stream_id,
        cfg.keep_alive_interval_ms,
    );
    // Current ingress target + the member list to rotate through when connect
    // attempts go unanswered (the target may be a dead node; any LIVE member
    // answers a connect — the leader with OK, a follower with a REDIRECT to
    // the leader — so round-robin always converges on the leader).
    let mut endpoints = cfg.ingress_endpoints.clone();
    let mut target_member = cfg.initial_leader_member_id;
    let mut ingress = match open_leader_pub(&rt, &endpoints, target_member, cfg.ingress_stream_id) {
        Some(p) => p,
        None => {
            tracing::error!(
                endpoints = %endpoints,
                "cluster session: no usable initial ingress endpoint"
            );
            return;
        }
    };

    // Replay-request resend state: the request is publish()ed on the cluster
    // ingress, which right after a (re)connect is often still NOT_CONNECTED —
    // a single best-effort send is silently lost and the consumer then waits
    // forever for a replay nobody asked for. Resend every REPLAY_RESEND_MS
    // until the consumer's cursor ADVANCES (progress = frames flowing again).
    const REPLAY_RESEND_MS: u64 = 3_000;
    let mut replay_last_sent_ms: u64 = 0;
    let mut replay_cursor_at_send: (u64, u64) = (u64::MAX, u64::MAX);

    // Whether an egress consumer (a `LiveEgress`) is still attached. A
    // publisher-only client (the sequencer) drops its `LiveEgress`, after which
    // we stop routing application payloads (and never accumulate them) but keep
    // the session — and its keep-alives — alive. The sole terminator is `stop`
    // (set when the owning `LiveCluster` is dropped).
    let mut egress_alive = true;

    while !stop.load(Ordering::SeqCst) {
        // 1. Drain egress fragments through the driver.
        loop {
            match frame_rx.try_recv() {
                Ok(frame) => {
                    for ev in driver.on_egress(&frame) {
                        match ev {
                            DriverEvent::AppMessage(payload) => {
                                if egress_alive && out_tx.send(payload).is_err() {
                                    egress_alive = false; // consumer dropped
                                }
                            }
                            DriverEvent::Reconnect {
                                leader_member_id,
                                ingress_endpoints,
                            } => {
                                if let Some(p) = open_leader_pub(
                                    &rt,
                                    &ingress_endpoints,
                                    leader_member_id,
                                    cfg.ingress_stream_id,
                                ) {
                                    ingress = p;
                                    endpoints = ingress_endpoints;
                                    target_member = leader_member_id;
                                }
                            }
                            DriverEvent::Connected { cluster_session_id } => {
                                tracing::info!(cluster_session_id, "cluster session opened");
                                // Canonical-stream consumers request replay from
                                // their delivery cursor on EVERY establishment;
                                // force an immediate (re)send below.
                                replay_last_sent_ms = 0;
                            }
                            DriverEvent::Failed(reason) => {
                                tracing::error!(%reason, "cluster session failed");
                            }
                        }
                    }
                }
                Err(TryRecvError::Empty) => break,
                // A dropped LiveIngress/LiveEgress must NOT kill the session
                // (the other half may still be in use); only `stop` terminates.
                Err(TryRecvError::Disconnected) => break,
            }
        }

        // 1b. Replay request (canonical-stream consumers): send on session
        // establishment and RE-SEND periodically until the consumer's cursor
        // advances — the ingress publication is often not yet connected right
        // after a (re)connect, so any single send can be silently lost.
        if let Some(r) = &replay
            && driver.is_connected()
        {
            let cursor = (
                r.next_index.load(std::sync::atomic::Ordering::Relaxed),
                r.next_block.load(std::sync::atomic::Ordering::Relaxed),
            );
            let now = now_ms();
            let progressed = cursor != replay_cursor_at_send;
            if progressed && replay_last_sent_ms != 0 {
                // Frames are flowing — move the checkpoint so a FUTURE stall is
                // measured from the most recent progress, not the last send.
                replay_cursor_at_send = cursor;
                replay_last_sent_ms = now;
            } else if replay_last_sent_ms == 0
                || now.saturating_sub(replay_last_sent_ms) >= REPLAY_RESEND_MS
            {
                let req = crate::wire::encode_replay_request(cursor.0, cursor.1);
                if let Some(framed) = driver.wrap_app(&req, now as i64) {
                    ingress.publish_best_effort(to_aligned(&framed));
                    tracing::info!(
                        next_index = cursor.0,
                        next_block = cursor.1,
                        "cluster replay requested"
                    );
                    replay_last_sent_ms = now;
                    replay_cursor_at_send = cursor;
                }
            }
        }

        // 2. Connect / keep-alive frames. The driver self-heals (re-emits a
        // connect on connect-timeout and after a session-failure backoff); when
        // it emits a RETRY attempt, rotate the ingress target to the next
        // member id BEFORE publishing it — the member we were pointed at may be
        // gone, and any live member redirects us to the leader.
        let now = now_ms();
        let frames = driver.poll_outbound(now);
        if driver.take_rotate_hint()
            && let Some((next_member, p)) =
                open_next_member_pub(&rt, &endpoints, target_member, cfg.ingress_stream_id)
        {
            target_member = next_member;
            ingress = p;
            tracing::info!(
                member = target_member,
                "cluster session: rotating ingress target for reconnect"
            );
        }
        for frame in frames {
            ingress.publish_best_effort(to_aligned(&frame));
        }

        // 3. Application offers from the ref publisher.
        loop {
            match req_rx.try_recv() {
                Ok(OfferReq { payload, reply }) => {
                    let outcome = match driver.wrap_app(&payload, now as i64) {
                        Some(framed) => match ingress.publish_bytes(to_aligned(&framed)) {
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

        thread::sleep(Duration::from_millis(1));
    }
}

/// Parse a `memberId=host:port,…` list and open an ingress publication to the
/// given member.
fn open_leader_pub(
    rt: &AeronRuntime,
    endpoints: &str,
    member_id: i32,
    stream_id: i32,
) -> Option<PubHandle> {
    let endpoint = endpoint_for_member(endpoints, member_id)?;
    // Small terms: this publication's log allocates at ITS term length on the
    // client AND as the matching image on EVERY cluster member's tmpfs.
    let uri = format!("aeron:udp?endpoint={endpoint}|term-length=1m");
    rt.open_publication(&uri, stream_id).ok()
}

/// Extract `host:port` for `member_id` from a `memberId=host:port,…` list.
fn endpoint_for_member(endpoints: &str, member_id: i32) -> Option<String> {
    let want = member_id.to_string();
    endpoints.split(',').find_map(|entry| {
        let (id, ep) = entry.split_once('=')?;
        (id.trim() == want).then(|| ep.trim().to_string())
    })
}

/// All member ids present in a `memberId=host:port,…` list, sorted.
fn member_ids(endpoints: &str) -> Vec<i32> {
    let mut ids: Vec<i32> = endpoints
        .split(',')
        .filter_map(|entry| entry.split_once('=')?.0.trim().parse().ok())
        .collect();
    ids.sort_unstable();
    ids
}

/// Open a publication to the member AFTER `current` in the list (wrapping) —
/// the round-robin step for self-heal reconnects. Falls through dead ids by
/// returning the first member whose publication opens.
fn open_next_member_pub(
    rt: &AeronRuntime,
    endpoints: &str,
    current: i32,
    stream_id: i32,
) -> Option<(i32, PubHandle)> {
    let ids = member_ids(endpoints);
    if ids.is_empty() {
        return None;
    }
    let start = ids.iter().position(|&id| id == current).unwrap_or(0);
    // Try every member once, starting from the one after `current`.
    for step in 1..=ids.len() {
        let id = ids[(start + step) % ids.len()];
        if let Some(p) = open_leader_pub(rt, endpoints, id, stream_id) {
            return Some((id, p));
        }
    }
    None
}

fn to_aligned(bytes: &[u8]) -> AlignedVec {
    let mut av = AlignedVec::new();
    av.extend_from_slice(bytes);
    av
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn endpoint_for_member_parses_list() {
        let eps = "0=h0:9000,1=h1:9001,2=h2:9002";
        assert_eq!(endpoint_for_member(eps, 0).as_deref(), Some("h0:9000"));
        assert_eq!(endpoint_for_member(eps, 2).as_deref(), Some("h2:9002"));
        assert_eq!(endpoint_for_member(eps, 5), None);
    }

    #[test]
    fn endpoint_for_member_tolerates_spaces() {
        assert_eq!(
            endpoint_for_member("0 = h0:9000 , 1 = h1:9001", 1).as_deref(),
            Some("h1:9001")
        );
    }
}

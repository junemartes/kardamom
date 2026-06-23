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
            .send(OfferReq { payload: payload.to_vec(), reply: reply_tx })
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

/// Connect to the cluster, spawning the session thread. Returns the lifetime
/// guard plus the ingress/egress seams for the trait adapters.
pub fn connect(
    rt: AeronRuntime,
    cfg: LiveClusterConfig,
) -> Result<(LiveCluster, LiveIngress, LiveEgress), LiveError> {
    // Egress subscription: the deliver closure ships each raw frame to the
    // session thread.
    let (frame_tx, frame_rx) = unbounded::<Vec<u8>>();
    let deliver: DeliverFn = Box::new(move |bytes: &[u8], _pos| {
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
        .spawn(move || run_session(rt, cfg, frame_rx, req_rx, out_tx, stop_thread))
        .map_err(|e| LiveError(format!("spawn session thread: {e}")))?;

    Ok((
        LiveCluster { stop, join: Some(join) },
        LiveIngress { req_tx },
        LiveEgress { out_rx },
    ))
}

fn run_session(
    rt: AeronRuntime,
    cfg: LiveClusterConfig,
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
    let mut ingress = match open_leader_pub(
        &rt,
        &cfg.ingress_endpoints,
        cfg.initial_leader_member_id,
        cfg.ingress_stream_id,
    ) {
        Some(p) => p,
        None => {
            tracing::error!(
                endpoints = %cfg.ingress_endpoints,
                "cluster session: no usable initial ingress endpoint"
            );
            return;
        }
    };

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
                            DriverEvent::Reconnect { leader_member_id, ingress_endpoints } => {
                                if let Some(p) = open_leader_pub(
                                    &rt,
                                    &ingress_endpoints,
                                    leader_member_id,
                                    cfg.ingress_stream_id,
                                ) {
                                    ingress = p;
                                }
                            }
                            DriverEvent::Connected { cluster_session_id } => {
                                tracing::info!(cluster_session_id, "cluster session opened");
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

        // 2. Connect / keep-alive frames.
        let now = now_ms();
        for frame in driver.poll_outbound(now) {
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
    let uri = format!("aeron:udp?endpoint={endpoint}");
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

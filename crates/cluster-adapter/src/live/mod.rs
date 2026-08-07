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
//!
//! Layout: this file holds the public types/seams and the `connect*` entry
//! points; [`session_loop`] is the session thread's duty cycle
//! (`SessionLoop`); [`endpoints`] parses member lists and opens ingress
//! publications.
//!
//! [`SessionDriver`]: kardamom_cluster_client::session::SessionDriver

mod endpoints;
mod session_loop;

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread::{self, JoinHandle};

use crossbeam_channel::{Receiver, Sender, bounded, unbounded};
use kardamom_log::aeron_live::{AeronRuntime, DeliverFn};
use thiserror::Error;

use crate::gateway::{ClusterEgress, ClusterIngress, OfferOutcome};
use endpoints::{member_ids, open_leader_pub, open_next_member_pub};
use session_loop::SessionSeams;

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

/// Result of a bounded-wait egress poll ([`LiveEgress::recv_timeout`]).
#[derive(Debug)]
pub enum EgressPoll {
    /// A frame arrived.
    Frame(Vec<u8>),
    /// Nothing arrived within the timeout; the session thread is still alive.
    Idle,
    /// The session thread is gone (cluster guard dropped) — stop polling.
    Closed,
}

impl LiveEgress {
    /// Bounded-wait receive, for consumers that must keep OBSERVING while
    /// egress is silent (the sequencer's lag-detection feed measures
    /// inter-arrival gaps — a blocking `recv` cannot notice silence).
    pub fn recv_timeout(&mut self, timeout: std::time::Duration) -> EgressPoll {
        match self.out_rx.recv_timeout(timeout) {
            Ok(frame) => EgressPoll::Frame(frame),
            Err(crossbeam_channel::RecvTimeoutError::Timeout) => EgressPoll::Idle,
            Err(crossbeam_channel::RecvTimeoutError::Disconnected) => EgressPoll::Closed,
        }
    }
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
    connect_inner(rt, cfg, None, false, None)
}

/// [`connect`], plus an egress-subscribe announcement on every session
/// establishment: for canonical-stream consumers that need no replay (e.g.
/// the ingress watermark observer, which derives watermarks from live
/// egress only). Without the announcement the service excludes the session
/// from the per-record egress fan-out.
pub fn connect_subscribed(
    rt: AeronRuntime,
    cfg: LiveClusterConfig,
) -> Result<(LiveCluster, LiveIngress, LiveEgress), LiveError> {
    connect_inner(rt, cfg, None, true, None)
}

/// [`connect`], plus a replay request on every session establishment
/// (implies the egress-subscribe announcement).
pub fn connect_with_replay(
    rt: AeronRuntime,
    cfg: LiveClusterConfig,
    replay: ReplayOnConnect,
) -> Result<(LiveCluster, LiveIngress, LiveEgress), LiveError> {
    connect_inner(rt, cfg, Some(replay), true, None)
}

/// [`connect`], but the session thread forwards ONLY egress app frames whose
/// leading kind byte is in `kinds` to the [`LiveEgress`] — everything else is
/// dropped at the source. For publisher-side consumers that want boundaries
/// (+ the odd control frame) only, like the sequencer's lag-detection feed:
/// relayed records arrive at full line rate, and allocating + channelling
/// each one to a receiver that discards it measurably taxes the session
/// thread, which also services the publish offers (observed as a collapsed
/// load ceiling, CI run 30164871699).
pub fn connect_with_egress_kind_filter(
    rt: AeronRuntime,
    cfg: LiveClusterConfig,
    kinds: &[u8],
) -> Result<(LiveCluster, LiveIngress, LiveEgress), LiveError> {
    // No SUBSCRIBE announcement: boundaries are broadcast to every session
    // (see SealerClusteredService.offerBoundary) and contiguity rejects are
    // offered directly to the offering session, so this feed needs no
    // consumer registration and stays out of the per-record fan-out.
    connect_inner(rt, cfg, None, false, Some(kinds.to_vec()))
}

/// Append a small term-length to a cluster control channel unless the URI
/// already pins one — used for BOTH the egress channel ([`connect_inner`])
/// and every ingress publication ([`endpoints::open_leader_pub`]). Small
/// terms matter on both sides: a publication's log allocates at ITS term
/// length on the client AND as the matching image on EVERY cluster member's
/// tmpfs. Cluster ingress/egress carry KB/s of control + canonical frames,
/// but Aeron's default 16MB terms allocate a ~50MB log PER publication image.
/// Session churn (chaos failovers, zombie-close reconnects) at 50MB a pop
/// exhausts the 1GB tmpfs, and the clustered SERVICE then dies with
/// "insufficient usable storage" while Raft itself stays healthy — observed
/// live as the post-failover pipeline freeze. 1MB terms cut every such log
/// 16x.
fn with_control_term_length(uri: &str) -> String {
    if uri.contains("term-length") {
        uri.to_string()
    } else {
        format!("{uri}|term-length=1m")
    }
}

/// Startup config validation: both fields come from the operator's `[cluster]`
/// TOML section (`egress_channel` usually via `--cluster-egress-endpoint`). An
/// empty/missing section used to surface only as a silently dead session
/// thread at runtime; [`connect`] now fails startup with a config error.
fn validate_config(cfg: &LiveClusterConfig) -> Result<(), LiveError> {
    if cfg.egress_channel.is_empty() {
        return Err(LiveError(
            "cluster config: egress_channel is empty — set [cluster] egress_channel \
             or pass --cluster-egress-endpoint"
                .into(),
        ));
    }
    if member_ids(&cfg.ingress_endpoints).is_empty() {
        return Err(LiveError(format!(
            "cluster config: ingress_endpoints has no memberId=host:port entries \
             (got {:?}) — set [cluster] ingress_endpoints",
            cfg.ingress_endpoints
        )));
    }
    Ok(())
}

fn connect_inner(
    rt: AeronRuntime,
    mut cfg: LiveClusterConfig,
    replay: Option<ReplayOnConnect>,
    subscribe: bool,
    egress_kind_filter: Option<Vec<u8>>,
) -> Result<(LiveCluster, LiveIngress, LiveEgress), LiveError> {
    validate_config(&cfg)?;
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

    // Initial ingress publication: opened HERE (not on the session thread) so
    // a failure surfaces to the caller and fails startup, instead of leaving
    // the owning binary alive with a silently dead session thread. Fall
    // through dead member ids like the reconnect path does — any live member
    // answers a connect (the leader with OK, a follower with a REDIRECT).
    let initial = open_leader_pub(
        &rt,
        &cfg.ingress_endpoints,
        cfg.initial_leader_member_id,
        cfg.ingress_stream_id,
    )
    .map(|p| (cfg.initial_leader_member_id, p))
    .or_else(|| {
        open_next_member_pub(
            &rt,
            &cfg.ingress_endpoints,
            cfg.initial_leader_member_id,
            cfg.ingress_stream_id,
        )
    })
    .ok_or_else(|| {
        LiveError(format!(
            "no usable initial cluster ingress endpoint in {:?}",
            cfg.ingress_endpoints
        ))
    })?;

    let (req_tx, req_rx) = unbounded::<OfferReq>();
    let (out_tx, out_rx) = unbounded::<Vec<u8>>();
    let stop = Arc::new(AtomicBool::new(false));
    let stop_thread = stop.clone();

    let join = thread::Builder::new()
        .name("cluster-session".into())
        .spawn(move || {
            session_loop::run_session(
                rt,
                cfg,
                initial,
                replay,
                subscribe,
                egress_kind_filter,
                SessionSeams {
                    frame_rx,
                    req_rx,
                    out_tx,
                    stop: stop_thread,
                },
            )
        })
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

#[cfg(test)]
mod tests {
    use super::*;

    fn valid_cfg() -> LiveClusterConfig {
        LiveClusterConfig {
            ingress_endpoints: "0=h0:9000,1=h1:9001".into(),
            initial_leader_member_id: 0,
            ingress_stream_id: 101,
            egress_channel: "aeron:udp?endpoint=127.0.0.1:9050".into(),
            egress_stream_id: 102,
            keep_alive_interval_ms: 1000,
        }
    }

    #[test]
    fn validate_accepts_populated_config() {
        assert!(validate_config(&valid_cfg()).is_ok());
    }

    #[test]
    fn validate_rejects_empty_egress_channel() {
        // An empty [cluster] section used to leave the owning binary alive
        // with a silently dead session thread; connect must fail instead.
        let cfg = LiveClusterConfig {
            egress_channel: String::new(),
            ..valid_cfg()
        };
        let err = validate_config(&cfg).unwrap_err();
        assert!(err.to_string().contains("egress_channel"), "got {err}");
    }

    #[test]
    fn validate_rejects_empty_or_unparseable_ingress_endpoints() {
        for eps in ["", "not-an-endpoint-list"] {
            let cfg = LiveClusterConfig {
                ingress_endpoints: eps.into(),
                ..valid_cfg()
            };
            let err = validate_config(&cfg).unwrap_err();
            assert!(err.to_string().contains("ingress_endpoints"), "got {err}");
        }
    }
}

//! Live cluster gateway: drives the sans-IO [`SessionDriver`] over a real
//! Aeron ingress publication and egress subscription (through
//! `kardamom_log`'s [`AeronRuntime`]).
//!
//! A dedicated session thread owns the driver and runs the cluster duty
//! cycle. It offers connect, keep-alive, and app frames on the ingress
//! publication, feeds egress fragments into the driver, surfaces
//! application payloads to [`ClusterEgress::recv`], and re-points the
//! ingress publication to the new leader on a `NewLeaderEvent` or a
//! REDIRECT. The [`ClusterIngress`] and [`ClusterEgress`] seams it exposes
//! are what the trait adapters consume. So the sequencer and executor
//! wiring is the same, whether it is backed by this live gateway or the
//! in-memory fakes.
//!
//! This module is the IO half of the Rust-native cluster client. Its
//! protocol correctness is covered by `kardamom-cluster-client`'s
//! deterministic `SessionDriver` tests. End-to-end behavior against a
//! real cluster is checked by the (gated) docker e2e test (see `tests/`).
//!
//! Layout: this file holds the public types and seams, and the
//! `connect*` entry points. [`session_loop`] is the session thread's
//! duty cycle (`SessionLoop`). [`endpoints`] parses member lists and
//! opens ingress publications.
//!
//! [`SessionDriver`]: kardamom_cluster_client::session::SessionDriver

mod endpoints;
#[cfg(all(test, feature = "docker-e2e"))]
mod fragmented_tests;
mod meters;
mod session_loop;
mod term;

use std::num::NonZeroU16;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread::{self, JoinHandle};

use crossbeam_channel::{Receiver, Sender, bounded, unbounded};
use kardamom_log::aeron_live::{AeronRuntime, PubHandle, RawFrame};
use thiserror::Error;

use crate::gateway::{ClusterEgress, ClusterIngress, OfferOutcome};
use endpoints::{IngressStream, member_ids, open_leader_pub, open_next_member_pub};
use session_loop::SessionSeams;
use term::AppPayload;
pub use term::{PayloadTooLarge, SessionTerm};

#[derive(Debug, Error)]
#[error("live cluster gateway: {0}")]
pub struct LiveError(String);

/// Configuration for a live cluster connection.
pub struct LiveClusterConfig {
    /// Cluster member ingress endpoints as `memberId=host:port,…`. This is
    /// the same form the cluster sends in a `NewLeaderEvent` or REDIRECT.
    pub ingress_endpoints: String,
    /// Member id to connect to first (the presumed leader).
    pub initial_leader_member_id: i32,
    /// Aeron stream id for cluster ingress.
    pub ingress_stream_id: i32,
    /// This client's egress (response) channel URI.
    pub egress_channel: String,
    /// Aeron stream id for cluster egress.
    pub egress_stream_id: i32,
    /// Keep-alive cadence, in ms. It must be less than the cluster's
    /// session timeout.
    pub keep_alive_interval_ms: u64,
    /// The term of the ingress publication. It bounds the payload that
    /// [`LiveIngress`] offers.
    pub ingress_term: SessionTerm,
    /// The term of the egress channel. It bounds the frames the cluster
    /// can send to this session.
    pub egress_term: SessionTerm,
}

impl LiveClusterConfig {
    /// The stream and term of every ingress publication.
    fn ingress_stream(&self) -> IngressStream {
        IngressStream {
            stream_id: self.ingress_stream_id,
            term: self.ingress_term,
        }
    }
}

/// The mark of one session of the session thread: the count of the
/// sessions that it opened, from 1, and back to 1 after `u16::MAX`. A fenced
/// ingress compares two marks for equality only, so the wrap matters only
/// when exactly 65,536 sessions open between two offers. The mark is two
/// bytes, so a fenced [`LiveIngress`] is no larger than a plain one.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct SessionMark(NonZeroU16);

impl SessionMark {
    /// The mark of the session that opens after the session `prev`.
    fn after(prev: Option<Self>) -> Self {
        let next = prev.map_or(0, |mark| mark.0.get()).wrapping_add(1);
        Self(NonZeroU16::new(next).unwrap_or(NonZeroU16::MIN))
    }
}

/// One offer request to the session thread.
struct OfferReq {
    payload: AppPayload,
    /// The session of the last offer of a fenced ingress: the session
    /// thread refuses the offer on any other session. `None` takes any
    /// session.
    expect: Option<SessionMark>,
    reply: Sender<OfferReply>,
}

impl OfferReq {
    /// Whether the offer must not go out on `session`, the open session:
    /// the ingress is fenced to another one.
    fn fenced_off(&self, session: Option<SessionMark>) -> bool {
        self.expect
            .is_some_and(|expect| session.is_some_and(|open| open != expect))
    }
}

/// The answer of the session thread to one offer.
#[derive(Clone, Copy)]
struct OfferReply {
    outcome: OfferOutcome,
    /// The mark of the session that opened last, `None` before the first
    /// one.
    session: Option<SessionMark>,
}

/// Whether an ingress offers only on the session of its last offer.
#[derive(Clone, Copy)]
enum Fence {
    Off,
    /// The session of the last offer that reached a session, `None` before
    /// the first one.
    On(Option<SessionMark>),
}

impl Fence {
    /// The session that the next offer must go to.
    fn expect(self) -> Option<SessionMark> {
        match self {
            Self::Off => None,
            Self::On(session) => session,
        }
    }

    /// Follow the session of `reply`: an accepted offer and a refused one
    /// both name the open session.
    fn follow(&mut self, reply: OfferReply) {
        if let Self::On(held) = self
            && matches!(
                reply.outcome,
                OfferOutcome::Accepted | OfferOutcome::SessionChanged
            )
        {
            *held = reply.session;
        }
    }
}

/// Owns the session thread. Dropping it stops the session cleanly.
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
/// `Clone` shares the single session thread. Every clone offers through
/// the same `req_tx`, and the session thread serializes those offers.
/// This gives the correct single-writer behavior when two producer
/// threads (for example, the sequencer's main loop and the deposit pump)
/// publish through one cluster session.
#[derive(Clone)]
pub struct LiveIngress {
    req_tx: Sender<OfferReq>,
    term: SessionTerm,
    fence: Fence,
}

impl LiveIngress {
    /// A clone that offers only on the session of its last offer. The first
    /// offer after the session changes offers nothing and returns
    /// [`OfferOutcome::SessionChanged`]: the old session can have lost the
    /// payloads that it took, and the caller offers them again before any
    /// new payload. The offers after it go to the new session.
    #[must_use]
    pub fn fenced(&self) -> Self {
        Self {
            fence: Fence::On(None),
            ..self.clone()
        }
    }
}

impl ClusterIngress for LiveIngress {
    /// Refuse a payload above the bound of the ingress term here, before
    /// it reaches the session thread: Aeron refuses an offer above its
    /// maximum message length, and a retry cannot succeed.
    fn offer(&mut self, payload: &[u8]) -> OfferOutcome {
        let payload: AppPayload = match self.term.payload(payload) {
            Ok(payload) => payload,
            Err(PayloadTooLarge { len, .. }) => return OfferOutcome::TooLarge { len },
        };
        let (reply_tx, reply_rx) = bounded(1);
        if self
            .req_tx
            .send(OfferReq {
                payload,
                expect: self.fence.expect(),
                reply: reply_tx,
            })
            .is_err()
        {
            return OfferOutcome::NotConnected; // session thread gone
        }
        // `RecvError` means the session thread dropped `reply_tx` without
        // answering, which only happens when it is gone: the same
        // condition `OfferOutcome::NotConnected` reports above.
        let reply = reply_rx.recv().unwrap_or(OfferReply {
            outcome: OfferOutcome::NotConnected,
            session: None,
        });
        self.fence.follow(reply);
        reply.outcome
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
    /// Nothing arrived within the timeout. The session thread is still
    /// alive.
    Idle,
    /// The session thread is gone (the cluster guard dropped). Stop
    /// polling.
    Closed,
}

impl LiveEgress {
    /// Bounded-wait receive, for consumers that must keep watching while
    /// egress is silent. For example, the sequencer's lag-detection feed
    /// measures gaps between arrivals; a blocking `recv` cannot notice
    /// silence.
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

/// What the session thread sends whenever it establishes a session, when
/// the client is a canonical-stream consumer: a `REPLAY_FROM(next_index,
/// next_block)` request built from the consumer's live delivery cursor
/// (shared atomics, written by the subscription on every delivery). This
/// is what keeps the canonical stream gapless across a lost session.
/// Without it, frames committed between session death and reconnect are
/// lost forever.
///
/// A replay consumer also gets a session-start marker
/// ([`EgressItem::SessionStart`](crate::wire::EgressItem::SessionStart))
/// on its channel when each session opens, before any frame of that
/// session.
#[derive(Clone)]
pub struct ReplayOnConnect {
    pub next_index: std::sync::Arc<std::sync::atomic::AtomicU64>,
    pub next_block: std::sync::Arc<std::sync::atomic::AtomicU64>,
}

/// What a session announces or replays on establishment: [`connect_with`]'s
/// option group. [`connect`] is the no-announcement, no-replay,
/// no-filter default. One struct plus one function covers every connect
/// variant, instead of one thin wrapper per flag.
#[derive(Default)]
pub struct ConnectOptions {
    /// Send a replay request whenever a session is established. Implies
    /// `subscribe` in practice — every replay consumer today is also a
    /// canonical-stream consumer — but callers set both explicitly.
    pub replay: Option<ReplayOnConnect>,
    /// Send the egress-subscribe announcement whenever a session is
    /// established. Use this for canonical-stream consumers (for example,
    /// the ingress watermark observer, which derives watermarks only from
    /// live egress). Without the announcement, the service excludes the
    /// session from the per-record egress fan-out.
    pub subscribe: bool,
    /// Forward to the [`LiveEgress`] only egress app frames whose leading
    /// kind byte is in this list. Everything else is dropped at the
    /// source. Use this for publisher-side consumers that want only
    /// boundaries (and the odd control frame), like the sequencer's
    /// lag-detection feed. Relayed records arrive at full line rate, and
    /// allocating and channelling each one to a receiver that discards it
    /// measurably taxes the session thread, which also services the
    /// publish offers.
    pub egress_kind_filter: Option<Vec<u8>>,
}

/// Connect to the cluster with `opts`, and spawn the session thread.
/// Returns the lifetime guard, plus the ingress and egress seams for the
/// trait adapters.
///
/// # Errors
///
/// Returns an error if `cfg` is invalid, or the initial ingress
/// publication or egress subscription fails to open.
pub fn connect_with(
    rt: AeronRuntime,
    cfg: LiveClusterConfig,
    opts: ConnectOptions,
) -> Result<(LiveCluster, LiveIngress, LiveEgress), LiveError> {
    crate::metrics::describe();
    SessionConnect::new(rt, cfg, opts)?
        .open_egress()?
        .open_ingress()?
        .spawn_thread()
}

/// Startup config validation. Both fields come from the operator's
/// `[cluster]` TOML section (`egress_channel` usually through
/// `--cluster-egress-endpoint`). [`connect`] fails startup with a config
/// error when either is empty, instead of leaving a silently dead session
/// thread.
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

/// Staged construction of a live cluster connection, so each stage's
/// locals live on `self` instead of as one long function's local
/// variables: [`Self::open_egress`], then [`Self::open_ingress`], then
/// [`Self::spawn_thread`]. [`connect_with`] is the one caller.
struct SessionConnect {
    rt: AeronRuntime,
    cfg: LiveClusterConfig,
    opts: ConnectOptions,
}

impl SessionConnect {
    fn new(
        rt: AeronRuntime,
        mut cfg: LiveClusterConfig,
        opts: ConnectOptions,
    ) -> Result<Self, LiveError> {
        validate_config(&cfg)?;
        cfg.egress_channel = cfg.egress_term.channel(&cfg.egress_channel);
        Ok(Self { rt, cfg, opts })
    }

    /// Open the egress subscription. Raw frames ship to the session
    /// thread verbatim — the cluster assigns the canonical index, so the
    /// Aeron position and session of the egress image do not matter. The
    /// session thread is a plain OS thread that waits on this receiver
    /// alongside another crossbeam channel via `crossbeam_channel::Select`
    /// (see [`SessionLoop::idle_wait`](super::session_loop::SessionLoop)),
    /// so this needs the crossbeam-backed subscription, not the tokio one
    /// every async consumer of `AeronRuntime` uses.
    fn open_egress(self) -> Result<EgressOpened, LiveError> {
        let (_sub_id, frame_rx) = self
            .rt
            .open_subscription_raw_crossbeam(&self.cfg.egress_channel, self.cfg.egress_stream_id)
            .map_err(|e| LiveError(format!("open egress subscription: {e}")))?;
        Ok(EgressOpened {
            base: self,
            frame_rx,
        })
    }
}

/// [`SessionConnect`] with its egress subscription open, holding the
/// receiver [`SessionConnect::open_egress`] created.
struct EgressOpened {
    base: SessionConnect,
    frame_rx: Receiver<RawFrame>,
}

impl EgressOpened {
    /// Open the initial ingress publication here, not on the session
    /// thread. This way a failure reaches the caller and fails startup,
    /// instead of leaving the owning binary alive with a silently dead
    /// session thread. Skip dead member IDs, like the reconnect path
    /// does: any live member answers a connect (the leader with OK, a
    /// follower with a REDIRECT).
    fn open_ingress(self) -> Result<IngressOpened, LiveError> {
        let cfg = &self.base.cfg;
        let initial = open_leader_pub(
            &self.base.rt,
            &cfg.ingress_endpoints,
            cfg.initial_leader_member_id,
            cfg.ingress_stream(),
        )
        .map(|p| (cfg.initial_leader_member_id, p))
        .or_else(|| {
            open_next_member_pub(
                &self.base.rt,
                &cfg.ingress_endpoints,
                cfg.initial_leader_member_id,
                cfg.ingress_stream(),
            )
        })
        .ok_or_else(|| {
            LiveError(format!(
                "no usable initial cluster ingress endpoint in {:?}",
                cfg.ingress_endpoints
            ))
        })?;
        Ok(IngressOpened {
            egress: self,
            initial,
        })
    }
}

/// [`EgressOpened`] with its initial ingress publication open, holding
/// the `(member_id, publication)` pair [`EgressOpened::open_ingress`]
/// found.
struct IngressOpened {
    egress: EgressOpened,
    initial: (i32, PubHandle),
}

impl IngressOpened {
    /// Spawn the session thread, consuming every stage's state.
    fn spawn_thread(self) -> Result<(LiveCluster, LiveIngress, LiveEgress), LiveError> {
        let Self {
            egress:
                EgressOpened {
                    base: SessionConnect { rt, cfg, opts },
                    frame_rx,
                },
            initial,
        } = self;
        let (req_tx, req_rx) = unbounded::<OfferReq>();
        let (out_tx, out_rx) = unbounded::<Vec<u8>>();
        let stop = Arc::new(AtomicBool::new(false));
        let stop_thread = stop.clone();
        let ingress_term = cfg.ingress_term;

        let join = thread::Builder::new()
            .name("cluster-session".into())
            .spawn(move || {
                session_loop::run_session(
                    rt,
                    cfg,
                    initial,
                    opts.replay,
                    opts.subscribe,
                    opts.egress_kind_filter,
                    SessionSeams {
                        frame_rx,
                        req_rx,
                        out_tx,
                        stop: stop_thread,
                    },
                );
            })
            .map_err(|e| LiveError(format!("spawn session thread: {e}")))?;

        Ok((
            LiveCluster {
                stop,
                join: Some(join),
            },
            LiveIngress {
                req_tx,
                term: ingress_term,
                fence: Fence::Off,
            },
            LiveEgress { out_rx },
        ))
    }
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
            ingress_term: SessionTerm::Control,
            egress_term: SessionTerm::Control,
        }
    }

    #[test]
    fn an_offer_above_the_bound_is_refused_before_the_session_thread() {
        let (req_tx, req_rx) = unbounded();
        let mut ingress = LiveIngress {
            req_tx,
            term: SessionTerm::Control,
            fence: Fence::Off,
        };
        let len = SessionTerm::Control.max_payload_len() + 1;
        assert_eq!(ingress.offer(&vec![0; len]), OfferOutcome::TooLarge { len });
        assert!(req_rx.is_empty(), "no request reaches the session thread");
    }

    /// Answer one request as the session thread does while the session
    /// `open` is open, with the fence rule of the real session thread.
    fn answer(req_rx: &crossbeam_channel::Receiver<OfferReq>, open: SessionMark) {
        let req = req_rx.recv().unwrap();
        let session = Some(open);
        let outcome = if req.fenced_off(session) {
            OfferOutcome::SessionChanged
        } else {
            OfferOutcome::Accepted
        };
        req.reply.send(OfferReply { outcome, session }).unwrap();
    }

    /// A fenced ingress offers nothing on a new session at first: it
    /// reports the change once, and its next offer goes to the new session.
    /// An ingress that is not fenced offers on any session.
    #[test]
    fn a_fenced_ingress_reports_a_new_session_once() {
        let (req_tx, req_rx) = unbounded();
        let plain = LiveIngress {
            req_tx,
            term: SessionTerm::Control,
            fence: Fence::Off,
        };
        let mut fenced = plain.fenced();
        let mut plain = plain;
        let session_thread = std::thread::spawn(move || {
            let first = SessionMark::after(None);
            let second = SessionMark::after(Some(first));
            for open in [first, first, second, second, second] {
                answer(&req_rx, open);
            }
        });
        let outcomes = [
            fenced.offer(b"a"),
            fenced.offer(b"b"),
            plain.offer(b"c"),
            fenced.offer(b"d"),
            fenced.offer(b"e"),
        ];
        session_thread.join().unwrap();
        assert_eq!(
            outcomes,
            [
                OfferOutcome::Accepted,
                OfferOutcome::Accepted,
                OfferOutcome::Accepted,
                OfferOutcome::SessionChanged,
                OfferOutcome::Accepted,
            ]
        );
    }

    /// The mark counts from 1, and wraps back to 1, never to 0.
    #[test]
    fn a_session_mark_counts_from_one_and_wraps_to_one() {
        let first = SessionMark::after(None);
        assert_eq!(first.0.get(), 1);
        assert_eq!(SessionMark::after(Some(first)).0.get(), 2);
        let last = SessionMark(NonZeroU16::MAX);
        assert_eq!(SessionMark::after(Some(last)), first);
    }

    #[test]
    fn validate_accepts_populated_config() {
        assert!(validate_config(&valid_cfg()).is_ok());
    }

    #[test]
    fn validate_rejects_empty_egress_channel() {
        // An empty [cluster] section must fail connect, not leave the
        // owning binary alive with a silently dead session thread.
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

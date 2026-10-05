//! The hub: the one task that owns the ring. It takes every observed
//! record from the taps, stores it, and fans the stored event out on
//! one broadcast channel. Subscribers replay the ring through a query
//! channel, then follow the broadcast.
//!
//! The broadcast channel is bounded. A subscriber that falls behind by
//! more than its capacity sees a `Lagged` error and continues from the
//! present; the hub never waits for a subscriber.

use std::num::NonZeroUsize;
use std::time::{Duration, Instant};

use kardamom_types::{TxError, TxStatus};
use tokio::sync::{broadcast, mpsc, oneshot};
use tokio_util::sync::CancellationToken;

use crate::dto::StatusFilter;
use crate::metrics as m;
use crate::ring::{Insert, Ring, RingConfig, Stamped};

/// One record a tap observed.
#[derive(Clone, Debug)]
pub enum Observed {
    Status(TxStatus),
    /// A `tx_errors` record. It names the transaction by sender and
    /// nonce; the ring resolves the hash.
    Error(TxError),
}

/// A replay request: the events `filter` selects after `after_seq`, at
/// most `limit`, oldest first.
pub struct Replay {
    pub filter: StatusFilter,
    pub after_seq: u64,
    pub limit: usize,
    pub reply: oneshot::Sender<Vec<Stamped>>,
}

/// The hub's bounds: the ring's, the live feed's buffer, and the ingest
/// queue between the taps and the hub.
#[derive(Clone, Copy, Debug)]
pub struct HubConfig {
    pub ring: RingConfig,
    pub feed_buffer: NonZeroUsize,
    pub ingest_buffer: NonZeroUsize,
}

/// The eviction cadence. Eviction is cheap, so a short period keeps the
/// ring close to its age bound.
const EVICT_EVERY: Duration = Duration::from_secs(1);

/// The hub's client side: ingest, replay, and the live feed.
#[derive(Clone)]
pub struct HubHandle {
    ingest: mpsc::Sender<Observed>,
    queries: mpsc::Sender<Replay>,
    live: broadcast::Sender<Stamped>,
}

impl HubHandle {
    /// The queue the taps push into.
    #[must_use]
    pub fn ingest(&self) -> mpsc::Sender<Observed> {
        self.ingest.clone()
    }

    /// A live feed receiver. Subscribe before the replay, so nothing
    /// stored in between is missed.
    #[must_use]
    pub fn subscribe(&self) -> broadcast::Receiver<Stamped> {
        self.live.subscribe()
    }

    /// One replay page. An empty page means the hub is gone or nothing
    /// matched.
    pub async fn replay(&self, filter: StatusFilter, after_seq: u64, limit: usize) -> Vec<Stamped> {
        let (reply, rx) = oneshot::channel();
        let query = Replay {
            filter,
            after_seq,
            limit,
            reply,
        };
        if self.queries.send(query).await.is_err() {
            return Vec::new();
        }
        rx.await.unwrap_or_default()
    }
}

pub struct Hub {
    ring: Ring,
    ingest: mpsc::Receiver<Observed>,
    queries: mpsc::Receiver<Replay>,
    live: broadcast::Sender<Stamped>,
    shutdown: CancellationToken,
}

impl Hub {
    /// Build the hub and its handle. [`Self::run`] drives it.
    #[must_use]
    pub fn new(cfg: HubConfig, shutdown: CancellationToken) -> (Self, HubHandle) {
        let (ingest_tx, ingest) = mpsc::channel(cfg.ingest_buffer.get());
        let (queries_tx, queries) = mpsc::channel(64);
        let (live, _) = broadcast::channel(cfg.feed_buffer.get());
        let hub = Self {
            ring: Ring::new(cfg.ring),
            ingest,
            queries,
            live: live.clone(),
            shutdown,
        };
        let handle = HubHandle {
            ingest: ingest_tx,
            queries: queries_tx,
            live,
        };
        (hub, handle)
    }

    /// Serve ingest, replays and eviction until the shutdown token
    /// fires or every tap has closed.
    pub async fn run(mut self) {
        let mut evict = tokio::time::interval(EVICT_EVERY);
        loop {
            let next = tokio::select! {
                () = self.shutdown.cancelled() => return,
                observed = self.ingest.recv() => observed.map(Step::Observed),
                query = self.queries.recv() => query.map(Step::Query),
                _ = evict.tick() => Some(Step::Evict),
            };
            match next {
                Some(Step::Observed(o)) => self.on_observed(o),
                Some(Step::Query(q)) => self.on_query(q),
                Some(Step::Evict) => self.on_evict(),
                None => return,
            }
        }
    }

    fn on_observed(&mut self, observed: Observed) {
        let status = match observed {
            Observed::Status(s) => s,
            Observed::Error(e) => {
                let Some(hash) = self.ring.resolve(e.sender, e.nonce) else {
                    metrics::counter!(m::UNRESOLVED_ERRORS_TOTAL).increment(1);
                    return;
                };
                TxStatus::rejected(hash, &e)
            }
        };
        let kind = status.kind();
        match self.ring.insert(&status, Instant::now()) {
            Insert::Stored(stamped) => {
                metrics::counter!(m::EVENTS_TOTAL, "stage" => kind.as_str()).increment(1);
                // No receiver is not an error: the ring still serves
                // the replay of a later subscriber.
                let _ = self.live.send(stamped);
            }
            Insert::Duplicate => metrics::counter!(m::DUPLICATES_TOTAL).increment(1),
        }
    }

    fn on_query(&self, query: Replay) {
        let page = self
            .ring
            .replay(&query.filter, query.after_seq, query.limit);
        // A subscriber that went away while its page was built needs no
        // answer.
        let _ = query.reply.send(page);
    }

    fn on_evict(&mut self) {
        let evicted = self.ring.evict(Instant::now());
        metrics::counter!(m::EVICTED_TOTAL).increment(as_u64(evicted));
        metrics::gauge!(m::RING_EVENTS).set(m::count(self.ring.len()));
        metrics::gauge!(m::RING_TRANSACTIONS).set(m::count(self.ring.transactions()));
    }
}

enum Step {
    Observed(Observed),
    Query(Replay),
    Evict,
}

fn as_u64(n: usize) -> u64 {
    u64::try_from(n).unwrap_or(u64::MAX)
}

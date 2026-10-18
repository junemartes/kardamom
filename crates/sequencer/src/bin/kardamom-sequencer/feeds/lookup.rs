//! The nonce lookup task of the publish loop: the core's requests in, the
//! committed nonce of each sender out, as floor evidence with the rollback
//! generation of its request.

use std::collections::{HashMap, HashSet};
use std::ops::ControlFlow;
use std::sync::Arc;
use std::time::{Duration, Instant};

use alloy_primitives::Address;
use kardamom_cache::{CacheReader, ExecutorQuery, ExecutorQueryConfig, LiveAccounts, QueryError};
use kardamom_sequencer::lookup::{LookupConfig, LookupRequest};
use kardamom_sequencer::metrics as seq_metrics;
use kardamom_sequencer::resync::{Evidence, FloorEvent, FloorUpdate};
use kardamom_sequencer::sequencer::Shutdown;

/// One lookup result, from a query task back to the drain.
struct LookupDone {
    request: LookupRequest,
    result: Result<u64, QueryError>,
}

/// The nonce lookup task. It drains the core's requests, answers a
/// resident sender from the local account layer, dedups the senders in
/// flight, bounds the concurrency and the per-sender retry rate, asks
/// Redis when `[cache]` is on, then the executors, and delivers each
/// answer as a `FloorUpdate`. See `kardamom_sequencer::lookup`.
///
/// One query runs on its own task, so a slow executor never blocks the
/// drain. The endpoints rotate per query, and a query walks the list until
/// one endpoint answers within the timeout. A Redis nonce needs no
/// freshness gate: like the executor's answer, it is a lower bound.
pub(crate) struct NonceLookupFeed {
    cfg: LookupConfig,
    partition: u32,
    /// The core's lookup requests.
    rx: tokio::sync::mpsc::UnboundedReceiver<LookupRequest>,
    shutdown: Shutdown,
    floor_tx: crossbeam_channel::Sender<FloorEvent>,
    /// The local account layer. A resident sender is answered from it.
    live: Arc<LiveAccounts>,
    /// The Redis layer. `None` with `[cache]` off.
    redis: Option<Arc<CacheReader>>,
    query: ExecutorQuery,
    done_tx: tokio::sync::mpsc::UnboundedSender<LookupDone>,
    done_rx: tokio::sync::mpsc::UnboundedReceiver<LookupDone>,
    in_flight: HashSet<Address>,
    /// The last request per sender. It bounds the retry rate to one
    /// lookup per timeout per sender and rollback generation.
    recent: HashMap<Address, Asked>,
}

/// The time and the rollback generation of the last request of a sender.
#[derive(Clone, Copy)]
struct Asked {
    at: Instant,
    generation: u64,
}

impl Asked {
    /// Whether this request makes `request` at `now` a repeat. A request of
    /// a newer generation is not a repeat: a rollback voids the answer to
    /// this one, and the core asks again for the sender.
    fn covers(self, request: LookupRequest, now: Instant, timeout: Duration) -> bool {
        self.generation >= request.generation && now.duration_since(self.at) < timeout
    }
}

impl NonceLookupFeed {
    /// The `recent` map is pruned once it holds this many senders.
    const RECENT_CAP: usize = 4096;

    /// Build the feed over the core's request channel. `None` when the
    /// config names no executor endpoint.
    pub(crate) fn new(
        cfg: LookupConfig,
        partition: u32,
        rx: tokio::sync::mpsc::UnboundedReceiver<LookupRequest>,
        shutdown: Shutdown,
        floor_tx: crossbeam_channel::Sender<FloorEvent>,
        live: Arc<LiveAccounts>,
        redis: Option<Arc<CacheReader>>,
    ) -> Option<Self> {
        let query = ExecutorQuery::new(&ExecutorQueryConfig {
            endpoints: cfg.executor_endpoints.clone(),
            timeout_ms: cfg.timeout_ms,
            max_in_flight: cfg.max_in_flight,
        })?;
        let (done_tx, done_rx) = tokio::sync::mpsc::unbounded_channel();
        Some(Self {
            cfg,
            partition,
            rx,
            shutdown,
            floor_tx,
            live,
            redis,
            query,
            done_tx,
            done_rx,
            in_flight: HashSet::new(),
            recent: HashMap::new(),
        })
    }

    /// Spawn the drain as a plain async task over the core's requests.
    pub(crate) fn spawn(self) -> tokio::task::JoinHandle<()> {
        tokio::spawn(self.run())
    }

    async fn run(mut self) {
        while self.tick().await.is_continue() {}
    }

    /// One [`Self::run`] pass: wait for a finished query, a new request,
    /// or shutdown. `Break` means the task should stop: shutdown, a
    /// closed channel, or the publish loop is gone.
    async fn tick(&mut self) -> ControlFlow<()> {
        tokio::select! {
            biased;
            () = self.shutdown.cancelled() => ControlFlow::Break(()),
            done = self.done_rx.recv() => match done {
                Some(done) => self.on_done(done),
                None => ControlFlow::Break(()),
            },
            req = self.rx.recv() => match req {
                Some(request) => self.on_lookup_request(request),
                None => ControlFlow::Break(()),
            },
        }
    }

    /// One finished query: count it, and forward an answer above nonce 0
    /// as floor evidence. `Break` means the publish loop is gone.
    fn on_done(&mut self, done: LookupDone) -> ControlFlow<()> {
        let sender = done.request.sender;
        self.in_flight.remove(&sender);
        seq_metrics::record_nonce_lookups_in_flight(self.partition, self.in_flight.len());
        let nonce = match done.result {
            Ok(nonce) => nonce,
            Err(e) => return self.record_lookup_error(sender, &e),
        };
        seq_metrics::record_nonce_lookup(self.partition, "ok");
        tracing::debug!(sender = ?sender, nonce, "nonce lookup answered");
        self.floor_from_committed(done.request, nonce)
    }

    /// A committed nonce `c` proves every nonce below it executed, so it
    /// is floor evidence of the same kind as a receipt. Nonce 0 proves
    /// nothing. The update carries the rollback generation of the
    /// request. `Break` means the publish loop is gone.
    fn floor_from_committed(&self, request: LookupRequest, nonce: u64) -> ControlFlow<()> {
        let Some(executed_nonce) = nonce.checked_sub(1) else {
            return ControlFlow::Continue(());
        };
        let evidence = Evidence::Lookup {
            generation: request.generation,
        };
        let update = FloorUpdate::executed(request.sender, executed_nonce).with_evidence(evidence);
        match self.floor_tx.send(update.into()) {
            Ok(()) => ControlFlow::Continue(()),
            Err(_) => ControlFlow::Break(()),
        }
    }

    /// One request from the core: answer it from the local layer when
    /// the sender is resident, else drop it if the sender is in flight
    /// or asked within the last timeout at the same generation, shed it at the concurrency
    /// bound, else start a query task.
    fn on_request(&mut self, request: LookupRequest) {
        let sender = request.sender;
        if let Some(view) = self.live.get(sender) {
            seq_metrics::record_nonce_lookup(self.partition, "local");
            // A gone publish loop ends the drain on its next tick.
            let _ = self.floor_from_committed(request, view.nonce);
            return;
        }
        let now = Instant::now();
        let timeout = self.cfg.timeout();
        let asked_recently = self
            .recent
            .get(&sender)
            .is_some_and(|asked| asked.covers(request, now, timeout));
        if self.in_flight.contains(&sender) || asked_recently {
            return;
        }
        if self.in_flight.len() >= self.cfg.max_in_flight.get() {
            seq_metrics::record_nonce_lookup(self.partition, "shed");
            return;
        }
        if self.recent.len() >= Self::RECENT_CAP {
            self.recent
                .retain(|_, asked| now.duration_since(asked.at) < timeout);
        }
        self.recent.insert(
            sender,
            Asked {
                at: now,
                generation: request.generation,
            },
        );
        self.in_flight.insert(sender);
        seq_metrics::record_nonce_lookups_in_flight(self.partition, self.in_flight.len());
        self.spawn_query(request);
    }

    /// Start one query task for the sender of `request`: Redis first when
    /// it is on, then the executors.
    fn spawn_query(&self, request: LookupRequest) {
        let sender = request.sender;
        let query = self.query.clone();
        let redis = self.redis.clone();
        let done_tx = self.done_tx.clone();
        let partition = self.partition;
        tokio::spawn(async move {
            let result = match redis_nonce(redis.as_deref(), sender).await {
                Some(nonce) => {
                    seq_metrics::record_nonce_lookup(partition, "redis");
                    Ok(nonce)
                }
                None => query.nonce(sender).await.and_then(|answer| {
                    u64::try_from(answer.value)
                        .map_err(|e| QueryError::new(format!("nonce out of range: {e}"), false))
                }),
            };
            let _ = done_tx.send(LookupDone { request, result });
        });
    }
}

/// The sender's nonce from the Redis projection. `None` with the layer
/// off, on a miss, and on every degraded read, which the reader counts.
async fn redis_nonce(redis: Option<&CacheReader>, sender: Address) -> Option<u64> {
    redis?.account(sender).await.map(|view| view.nonce)
}

impl NonceLookupFeed {
    /// Handle one lookup request from the core, for [`Self::tick`]'s
    /// `select!`. Always continues: a request never ends the drain.
    fn on_lookup_request(&mut self, request: LookupRequest) -> ControlFlow<()> {
        self.on_request(request);
        ControlFlow::Continue(())
    }

    /// Record and log a failed lookup, for [`Self::on_done`]'s error arm.
    /// Always continues: a failed lookup does not end the drain.
    fn record_lookup_error(&self, sender: Address, e: &QueryError) -> ControlFlow<()> {
        let outcome = if e.is_timeout() { "timeout" } else { "error" };
        seq_metrics::record_nonce_lookup(self.partition, outcome);
        tracing::warn!(sender = ?sender, error = %e, "nonce lookup failed");
        ControlFlow::Continue(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A repeat at the same generation within the timeout is dropped. A
    /// request after a rollback passes at once: the answer to the first
    /// request is void.
    #[test]
    fn a_request_of_a_newer_generation_is_not_a_repeat() {
        let at = Instant::now();
        let timeout = Duration::from_secs(2);
        let asked = Asked { at, generation: 1 };
        let request = |generation| LookupRequest {
            sender: Address::repeat_byte(0x11),
            generation,
        };
        assert!(asked.covers(request(1), at, timeout));
        assert!(!asked.covers(request(2), at, timeout));
        assert!(!asked.covers(request(1), at + timeout, timeout));
    }
}

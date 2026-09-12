//! The sequencer's background feeds, and the publish-loop spawner.
//!
//! Three loops feed, or drain, the publish path. The cluster-egress
//! watermark task and the `tx_receipts` floor task supply the
//! `ResyncController`: a replica that falls behind its twin past the
//! cluster's dedup window can double-order a transaction, so these feeds
//! detect the lag and drive the sequencer into a receipt-proof-only
//! publish mode until it clears. The nonce lookup task answers the
//! core's requests with an executor's committed nonce, as floor evidence
//! of the same kind as a receipt. `PublishLoops::spawn` runs the
//! canonical `TxRef` loop and the `DepositRef` epoch pump on blocking
//! tasks.
//!
//! Seam rule: the async shell is tokio. Anything that blocks, such as
//! Aeron polls, the crossbeam egress receiver, or the microsecond-backoff
//! publish loop, runs on `spawn_blocking`. It polls `Shutdown::is_signaled`.
//! Async-capable work, such as the receipts fan-in on an existing tokio
//! channel, is a plain task. It uses `select!` on `Shutdown::cancelled`.

mod steps;

use std::collections::{HashMap, HashSet};
use std::ops::ControlFlow;
use std::time::{Duration, Instant};

use alloy_primitives::Address;
use kardamom_cluster_adapter::LiveEgress;
use kardamom_cluster_adapter::live::EgressPoll;
use kardamom_cluster_adapter::wire::{self, EgressItem};
use kardamom_log::aeron_live::{
    IdleBackoff, TxDataSubscriberHandle, TxDepositsSubscriberHandle, TxErrorsPublisherHandle,
    TxReceiptsSubscriberHandle, TxRemoteEpochsSubscriberHandle,
};
use kardamom_sequencer::config::SequencerConfig;
use kardamom_sequencer::error::SequencerError;
use kardamom_sequencer::inbound::{Inbound, TxDataSubscriber};
use kardamom_sequencer::lookup::{self, LookupConfig, LookupRequester};
use kardamom_sequencer::metrics as seq_metrics;
use kardamom_sequencer::outbound::TxOrderingRefPublisher;
use kardamom_sequencer::pump::{OriginLane, Pump};
use kardamom_sequencer::resync::{
    FloorUpdate, ResyncController, SharedWatermark, elapsed_ms_saturating,
};
use kardamom_sequencer::sequencer::{Ports, Sequencer, Shutdown};
use kardamom_types::shard_map::{VslotSet, vslot_for};

/// One `tx_data` lane subscription: the lane index the sequencer stamps
/// into every ref off it, and the handle that reads it.
pub(crate) struct LaneSub {
    pub(crate) lane: u8,
    pub(crate) handle: TxDataSubscriberHandle,
}

/// The live `tx_data` subscription set: one handle per lane the sequencer
/// reads. The own lane comes first. A resize adds the old lanes. The poll
/// walks the lanes in rotation, so a busy old lane cannot starve the own
/// lane. Never empty: the constructor takes the own lane apart from the
/// old ones.
pub(crate) struct LaneSubscriptions {
    lanes: Vec<LaneSub>,
    /// The lane the next poll tries first.
    next: usize,
}

impl LaneSubscriptions {
    pub(crate) fn new(own: LaneSub, old: Vec<LaneSub>) -> Self {
        let lanes = std::iter::once(own).chain(old).collect();
        Self { lanes, next: 0 }
    }
}

impl TxDataSubscriber for LaneSubscriptions {
    fn poll(&mut self) -> Result<Option<Inbound>, SequencerError> {
        // try_recv is non-blocking. The Sequencer's run loop handles
        // backoff when poll returns None.
        let n = self.lanes.len();
        let lanes = &mut self.lanes;
        let found = (0..n)
            .map(|i| self.next.saturating_add(i) % n)
            .find_map(|idx| {
                let sub = &mut lanes[idx];
                sub.handle.try_recv().map(|(loc, envelope)| {
                    let inbound = Inbound {
                        lane: sub.lane,
                        loc,
                        envelope,
                    };
                    (idx, inbound)
                })
            });
        Ok(found.map(|(idx, inbound)| {
            self.next = idx.saturating_add(1) % n;
            inbound
        }))
    }
}

/// The egress-watermark feed: the silence authority. It measures
/// boundary-arrival gaps. Idle traffic still emits a boundary every
/// cluster tick, so arrivals, not count changes, are the liveness signal.
/// It raises the sticky lag flag and a starvation-proof metric.
///
/// This feed must never block without a bound (it uses `recv_timeout`).
/// The publish loop can block: a session offer waits on the session
/// thread, which may be mid-reconnect after a process freeze. A detector
/// that only runs when the publish loop runs would miss the freeze
/// entirely.
pub(crate) struct EgressWatermarkFeed {
    silence_ms: u64,
    partition: u32,
    watermark: SharedWatermark,
    reject_tx: crossbeam_channel::Sender<(Address, u64, u64)>,
    /// Anchored at feed start, not `None`. The cluster emits a boundary
    /// every tick, so "never seen a boundary" past the silence window is
    /// itself the lag state. A restarted replica whose session never
    /// re-establishes must flag, not stay silent forever. While the
    /// condition persists, `flag`'s re-arm repeats the flag once per
    /// silence window: a bounded, genuinely alarming heartbeat.
    last_boundary_at: Option<Instant>,
}

impl EgressWatermarkFeed {
    pub(crate) fn new(
        silence_ms: u64,
        partition: u32,
        watermark: SharedWatermark,
        reject_tx: crossbeam_channel::Sender<(Address, u64, u64)>,
    ) -> Self {
        Self {
            silence_ms,
            partition,
            watermark,
            reject_tx,
            last_boundary_at: Some(Instant::now()),
        }
    }

    /// Spawn on a blocking thread. `LiveEgress` holds nothing `!Send`,
    /// but its only wait primitive is a blocking crossbeam
    /// `recv_timeout`. `reject_tx` stays a std channel, because its
    /// consumer is the sync `ResyncController` in the publish loop.
    pub(crate) fn spawn(
        self,
        egress: LiveEgress,
        shutdown: Shutdown,
    ) -> tokio::task::JoinHandle<()> {
        tokio::task::spawn_blocking(move || self.run(egress, &shutdown))
    }

    /// Run until `shutdown` fires or the egress source closes.
    fn run(mut self, mut egress: LiveEgress, shutdown: &Shutdown) {
        while !shutdown.is_signaled() {
            match egress.recv_timeout(Duration::from_millis(500)) {
                EgressPoll::Frame(frame) => self.on_frame(&frame),
                EgressPoll::Idle => self.on_idle(),
                EgressPoll::Closed => return,
            }
        }
    }

    /// Egress is silent while this thread is demonstrably alive. This
    /// could be a partition from egress, or a dead cluster boundary
    /// clock. Either way, the response is the same.
    fn on_idle(&mut self) {
        self.flag(Instant::now());
    }

    fn on_frame(&mut self, frame: &[u8]) {
        if self.on_reject_frame(frame) {
            return;
        }
        if self.on_remote_origin_reject_frame(frame) {
            return;
        }
        // Check the cheap kind byte first. Relayed records arrive at
        // full line rate on every replica, and fully decoding them
        // here, just to discard them, costs measurable CPU.
        if frame.first() != Some(&wire::EGRESS_KIND_BOUNDARY) {
            return;
        }
        self.on_boundary_frame(frame);
    }

    /// Handle one contiguity-reject frame. Returns `true` when `frame`
    /// was a contiguity reject (already forwarded to `reject_tx`), so
    /// the caller does not also check it for a boundary.
    ///
    /// The sealer rejected one of this sequencer's refs because a known
    /// sender's nonce was not the expected next one. Forward
    /// `(sender, nonce, expected)` to the publish loop, which rewinds
    /// the unconfirmed ledger and republishes right away, instead of
    /// waiting out the confirm timeout.
    fn on_reject_frame(&mut self, frame: &[u8]) -> bool {
        if frame.first() != Some(&wire::EGRESS_KIND_CONTIGUITY_REJECT) {
            return false;
        }
        let Ok(EgressItem::ContiguityReject {
            sender,
            nonce,
            expected,
        }) = EgressItem::decode(frame)
        else {
            return true;
        };
        tracing::warn!(
            partition = self.partition,
            ?sender,
            nonce,
            expected,
            "sealer contiguity reject received"
        );
        self.forward_contiguity_reject(sender, nonce, expected);
        true
    }

    /// Handle one remote-origin-reject frame. Returns `true` when
    /// `frame` was a remote-origin reject, so the caller does not also
    /// check it for a boundary.
    ///
    /// The sealer rejected a remote-origin record this sequencer relayed.
    /// The record is the watcher's, and the watcher reconciles its cursor
    /// with the destination at startup, so this is informational here: log
    /// loudly and count it.
    fn on_remote_origin_reject_frame(&mut self, frame: &[u8]) -> bool {
        if frame.first() != Some(&wire::EGRESS_KIND_REMOTE_ORIGIN_REJECT) {
            return false;
        }
        if let Ok(EgressItem::RemoteOriginReject {
            origin_chain_id,
            first_seq,
            expected_next_seq,
            reason,
        }) = EgressItem::decode(frame)
        {
            let reason = reason.as_str();
            tracing::error!(
                partition = self.partition,
                origin = origin_chain_id,
                first_seq,
                expected_next_seq,
                reason,
                "sealer REMOTE-ORIGIN-REJECT: the record was not sealed; \
                 the lane cursor on the sealer is expected_next_seq"
            );
            seq_metrics::record_remote_origin_reject(origin_chain_id, reason);
        }
        true
    }

    /// Store the canonical count from one boundary frame, and re-arm the
    /// silence timer. A 30 second freeze shows up here as one long
    /// inter-arrival gap. The backlog drains instantly on resume, but
    /// the gap between the last pre-freeze arrival and this one is real
    /// wall-clock time.
    fn on_boundary_frame(&mut self, frame: &[u8]) {
        if let Ok(EgressItem::Boundary(b)) = EgressItem::decode(frame) {
            let now = Instant::now();
            self.flag(now);
            self.last_boundary_at = Some(now);
            self.watermark.store(b.end_tx_idx.as_index());
        }
    }

    /// Raise the sticky lag flag, and the starvation-proof metric, once
    /// per silence window: when the gap since the last boundary reaches
    /// `silence_ms`, re-arm from `now` instead of flagging on every
    /// poll.
    fn flag(&mut self, now: Instant) {
        let Some(prev) = self.last_boundary_at else {
            return;
        };
        let gap = elapsed_ms_saturating(now, prev);
        if gap >= self.silence_ms {
            self.watermark.flag_lag(gap);
            seq_metrics::record_lag_suspected(self.partition);
            tracing::info!(
                partition = self.partition,
                gap_ms = gap,
                "sequencer LAG suspected (boundary-arrival gap)"
            );
            self.last_boundary_at = Some(now);
        }
    }
}

/// The `tx_receipts`-to-per-sender executed-truth floor feed. Only the
/// senders of this replica's vslots reach the floor channel. The set is
/// the replica's whole set, shadow slots included: floors must advance
/// for the incoming senders during the warm-up too.
///
/// The receipts handle already fans in over a tokio unbounded channel. The
/// Aeron poll thread is the producer. So this is a plain async task. It
/// awaits `recv()` and `Shutdown::cancelled`, with no idle-sleep polling.
/// `floor_tx` stays a std channel, because its consumer is the sync
/// `ResyncController` in the publish loop.
pub(crate) struct ReceiptFloorFeed {
    vslots: VslotSet,
    floor_tx: crossbeam_channel::Sender<FloorUpdate>,
}

impl ReceiptFloorFeed {
    pub(crate) fn new(vslots: VslotSet, floor_tx: crossbeam_channel::Sender<FloorUpdate>) -> Self {
        Self { vslots, floor_tx }
    }

    /// Spawn as a plain async task. The handle reduces to its receiver
    /// (`into_receiver`), so the task holds no `AeronRuntime` clone.
    /// `floor_tx` is bounded by `resync::ResyncChannel::open` to the
    /// cluster's dedup capacity, the resync window this feed exists to
    /// cover.
    pub(crate) fn spawn(
        self,
        sub: TxReceiptsSubscriberHandle,
        shutdown: Shutdown,
    ) -> tokio::task::JoinHandle<()> {
        let rx = sub.into_receiver();
        tokio::spawn(self.run(rx, shutdown))
    }

    async fn run(self, mut rx: kardamom_log::aeron_live::TxReceiptsReceiver, shutdown: Shutdown) {
        loop {
            match self.tick(&mut rx, &shutdown).await {
                ControlFlow::Break(()) => return,
                ControlFlow::Continue(()) => (),
            }
        }
    }

    /// One [`Self::run`] pass: wait for the next receipt or shutdown,
    /// then forward it. `Break` means the task should stop: shutdown,
    /// the subscription closed, or the publish loop is gone.
    async fn tick(
        &self,
        rx: &mut kardamom_log::aeron_live::TxReceiptsReceiver,
        shutdown: &Shutdown,
    ) -> ControlFlow<()> {
        let receipt = tokio::select! {
            biased;
            () = shutdown.cancelled() => return ControlFlow::Break(()),
            msg = rx.recv() => match msg {
                Some((_pos, receipt)) => receipt,
                // The subscription closed. The runtime shut down.
                // Nothing more to feed.
                None => return ControlFlow::Break(()),
            },
        };
        self.forward_one_receipt(&receipt)
    }

    /// Filter, build, and forward one receipt, for [`Self::tick`].
    /// `Break` means the publish loop is gone and the task should stop.
    fn forward_one_receipt(&self, receipt: &kardamom_types::Receipt) -> ControlFlow<()> {
        // Forward every receipt of this replica's vslots, nonce 0
        // included. A deposit receipt carries `Receipt::tx_type ==
        // TX_TYPE_DEPOSIT`, so the controller tells it apart from a
        // genuine nonce-0 transaction receipt. It excludes deposits and
        // skip receipts from floor evidence (they consume no L2 nonce),
        // and counts skip receipts as publish confirmations (ordering is
        // the claim). See `ResyncController::drain_floor_updates`.
        //
        // Only this replica's senders can appear in its publish stream,
        // so the floor map stays bounded to them.
        if !self.vslots.contains(vslot_for(receipt.from)) {
            return ControlFlow::Continue(());
        }
        let update = FloorUpdate {
            sender: receipt.from,
            executed_nonce: receipt.nonce,
            skip_reason: receipt.skip_reason,
            deposit: receipt.is_deposit(),
        };
        match self.floor_tx.try_send(update) {
            Ok(()) => ControlFlow::Continue(()),
            // The publish loop is stalled past the resync window already.
            // Dropping is safe: an unproven skip just falls through to
            // publish, the side every degraded mode already degrades
            // toward (see the module doc on `crate::resync`).
            Err(crossbeam_channel::TrySendError::Full(_)) => {
                tracing::warn!("receipt-floor channel full; dropping");
                ControlFlow::Continue(())
            }
            // The publish loop is gone. Exit.
            Err(crossbeam_channel::TrySendError::Disconnected(_)) => ControlFlow::Break(()),
        }
    }
}

/// One lookup result, from a query task back to the drain.
struct LookupDone {
    sender: Address,
    result: Result<u64, String>,
}

/// The nonce lookup task. It drains the core's requests, dedups the
/// senders in flight, bounds the concurrency and the per-sender retry
/// rate, queries the executors, and delivers each answer as a
/// `FloorUpdate`. See `kardamom_sequencer::lookup`.
///
/// One query runs on its own task, so a slow executor never blocks the
/// drain. The endpoints rotate per query, and a query walks the list until
/// one endpoint answers within the timeout.
pub(crate) struct NonceLookupFeed {
    cfg: LookupConfig,
    partition: u32,
    /// The core's lookup requests.
    rx: tokio::sync::mpsc::UnboundedReceiver<Address>,
    shutdown: Shutdown,
    floor_tx: crossbeam_channel::Sender<FloorUpdate>,
    client: reqwest::Client,
    endpoints: std::sync::Arc<[String]>,
    done_tx: tokio::sync::mpsc::UnboundedSender<LookupDone>,
    done_rx: tokio::sync::mpsc::UnboundedReceiver<LookupDone>,
    in_flight: HashSet<Address>,
    /// The last request time per sender. It bounds the retry rate to one
    /// lookup per timeout per sender.
    recent: HashMap<Address, Instant>,
    next_endpoint: usize,
}

impl NonceLookupFeed {
    /// The `recent` map is pruned once it holds this many senders.
    const RECENT_CAP: usize = 4096;

    /// Build the feed over the core's request channel. Fails only when
    /// the HTTP client cannot be built.
    pub(crate) fn new(
        cfg: LookupConfig,
        partition: u32,
        rx: tokio::sync::mpsc::UnboundedReceiver<Address>,
        shutdown: Shutdown,
        floor_tx: crossbeam_channel::Sender<FloorUpdate>,
    ) -> Result<Self, reqwest::Error> {
        let client = reqwest::Client::builder().timeout(cfg.timeout()).build()?;
        let endpoints: std::sync::Arc<[String]> = cfg.executor_endpoints.clone().into();
        let (done_tx, done_rx) = tokio::sync::mpsc::unbounded_channel();
        Ok(Self {
            cfg,
            partition,
            rx,
            shutdown,
            floor_tx,
            client,
            endpoints,
            done_tx,
            done_rx,
            in_flight: HashSet::new(),
            recent: HashMap::new(),
            next_endpoint: 0,
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
                Some(sender) => self.on_lookup_request(sender),
                None => ControlFlow::Break(()),
            },
        }
    }

    /// One finished query: count it, and forward an answer above nonce 0
    /// as floor evidence. `Break` means the publish loop is gone.
    fn on_done(&mut self, done: LookupDone) -> ControlFlow<()> {
        self.in_flight.remove(&done.sender);
        seq_metrics::record_nonce_lookups_in_flight(self.partition, self.in_flight.len());
        let nonce = match done.result {
            Ok(nonce) => nonce,
            Err(e) => return self.record_lookup_error(done.sender, &e),
        };
        seq_metrics::record_nonce_lookup(self.partition, "ok");
        tracing::debug!(sender = ?done.sender, nonce, "nonce lookup answered");
        // A committed nonce `c` proves every nonce below it executed.
        // Nonce 0 proves nothing.
        let Some(executed_nonce) = nonce.checked_sub(1) else {
            return ControlFlow::Continue(());
        };
        let update = FloorUpdate {
            sender: done.sender,
            executed_nonce,
            skip_reason: None,
            deposit: false,
        };
        match self.floor_tx.send(update) {
            Ok(()) => ControlFlow::Continue(()),
            Err(_) => ControlFlow::Break(()),
        }
    }

    /// One request from the core: drop it if the sender is in flight or
    /// asked within the last timeout, shed it at the concurrency bound,
    /// else start a query task.
    fn on_request(&mut self, sender: Address) {
        let now = Instant::now();
        let timeout = self.cfg.timeout();
        let asked_recently = self
            .recent
            .get(&sender)
            .is_some_and(|t| now.duration_since(*t) < timeout);
        if self.in_flight.contains(&sender) || asked_recently {
            return;
        }
        if self.in_flight.len() >= self.cfg.max_in_flight.get() {
            seq_metrics::record_nonce_lookup(self.partition, "shed");
            return;
        }
        if self.recent.len() >= Self::RECENT_CAP {
            self.recent.retain(|_, t| now.duration_since(*t) < timeout);
        }
        self.recent.insert(sender, now);
        self.in_flight.insert(sender);
        seq_metrics::record_nonce_lookups_in_flight(self.partition, self.in_flight.len());
        self.spawn_query(sender);
    }

    /// Start one query task for `sender`, rotating the first endpoint.
    fn spawn_query(&mut self, sender: Address) {
        let first = self.next_endpoint % self.endpoints.len();
        self.next_endpoint = self.next_endpoint.wrapping_add(1);
        let query = ExecutorQuery::new(self.client.clone(), self.endpoints.clone(), first, sender);
        let done_tx = self.done_tx.clone();
        tokio::spawn(async move {
            let result = query.run().await;
            let _ = done_tx.send(LookupDone { sender, result });
        });
    }
}

/// One nonce query: the HTTP client, the executor endpoints, the
/// rotation start, and the sender asked about. It walks the endpoints
/// from `first` until one answers within the timeout.
struct ExecutorQuery {
    client: reqwest::Client,
    endpoints: std::sync::Arc<[String]>,
    first: usize,
    sender: Address,
}

impl ExecutorQuery {
    fn new(
        client: reqwest::Client,
        endpoints: std::sync::Arc<[String]>,
        first: usize,
        sender: Address,
    ) -> Self {
        Self {
            client,
            endpoints,
            first,
            sender,
        }
    }

    /// Query the endpoints from `first` in rotation. The first answer
    /// wins.
    async fn run(self) -> Result<u64, String> {
        let n = self.endpoints.len();
        let mut last_err = String::from("no executor endpoints");
        for endpoint in (0..n).map(|i| &self.endpoints[self.first.saturating_add(i) % n]) {
            if let ControlFlow::Break(nonce) = self.query_endpoint(endpoint, &mut last_err).await {
                return Ok(nonce);
            }
        }
        Err(last_err)
    }

    /// One endpoint of [`Self::run`]'s rotation: `Break` carries the
    /// answer; on failure the error lands in `last_err`.
    async fn query_endpoint(&self, endpoint: &str, last_err: &mut String) -> ControlFlow<u64> {
        match self.query_one(endpoint).await {
            Ok(nonce) => ControlFlow::Break(nonce),
            Err(e) => {
                *last_err = format!("{endpoint}: {e}");
                ControlFlow::Continue(())
            }
        }
    }

    async fn query_one(&self, endpoint: &str) -> Result<u64, String> {
        let resp = self
            .client
            .post(endpoint)
            .header("content-type", "application/json")
            .body(lookup::request_body(self.sender))
            .send()
            .await
            .map_err(|e| {
                if e.is_timeout() {
                    "timed out".to_string()
                } else {
                    e.to_string()
                }
            })?;
        let body = resp.text().await.map_err(|e| e.to_string())?;
        lookup::parse_answer(&body)
    }
}

pub(crate) type LoopHandle = tokio::task::JoinHandle<Result<(), SequencerError>>;

/// Argument group for [`PublishLoops::spawn`]: the config, the `tx_data`
/// lane subscriptions, the three `tx_ordering` publishers, the `tx_errors`
/// publisher, the two origin subscriptions, the resync controller, the
/// nonce lookup requester, and one shutdown token. Each spawned loop
/// clones the token itself.
pub(crate) struct PublishLoops<P> {
    pub(crate) cfg: SequencerConfig,
    pub(crate) tx_data: LaneSubscriptions,
    /// The canonical `TxRef` loop's publisher.
    pub(crate) main_pub: P,
    /// The L1 epoch pump's publisher.
    pub(crate) epoch_pub: P,
    /// The interop remote-epoch pump's publisher.
    pub(crate) remote_epoch_pub: P,
    pub(crate) tx_errors: TxErrorsPublisherHandle,
    pub(crate) epochs: TxDepositsSubscriberHandle,
    pub(crate) remote_epochs: TxRemoteEpochsSubscriberHandle,
    pub(crate) resync: Option<ResyncController>,
    /// `None` when the binary has no executor endpoints.
    pub(crate) lookup: Option<LookupRequester>,
    pub(crate) shutdown: Shutdown,
}

impl<P> PublishLoops<P>
where
    P: TxOrderingRefPublisher + Send + 'static,
{
    /// Spawn the main sequencer loop and the two origin pumps, over three
    /// `TxOrderingRefPublisher`s. This is generic over the publisher
    /// type, so the Aeron and cluster branches share one implementation.
    /// Both branches supply concrete publishers that implement the
    /// trait.
    ///
    /// The two origin pumps run as separate loops, not one merged poll,
    /// because the origins are independent: a peer whose feed has
    /// stalled must not delay L1 deposits, and an L1 RPC outage must not
    /// stall cross-chain delivery.
    pub(crate) fn spawn(self) -> (LoopHandle, LoopHandle, LoopHandle) {
        let Self {
            cfg,
            mut tx_data,
            mut main_pub,
            epoch_pub,
            remote_epoch_pub,
            mut tx_errors,
            epochs: epoch_subscription,
            remote_epochs: remote_epoch_subscription,
            resync,
            lookup,
            shutdown,
        } = self;

        // The sequencer main loop is sync (it uses std::thread and
        // std::thread::sleep for backoff). Hand it to spawn_blocking, so
        // the async runtime stays responsive for shutdown handling.
        let shutdown_for_main = shutdown.clone();
        let join_main = tokio::task::spawn_blocking(move || -> Result<(), SequencerError> {
            let mut sequencer = Sequencer::new(cfg)?;
            if let Some(controller) = resync {
                sequencer.enable_resync(controller);
            }
            if let Some(requester) = lookup {
                sequencer.enable_nonce_lookup(requester);
            }
            let mut ports = Ports {
                tx_data: &mut tx_data,
                refs: &mut main_pub,
                errors: &mut tx_errors,
            };
            sequencer.run(&mut ports, &shutdown_for_main)
        });

        // Independent pump for tx_deposits to epoch on tx_ordering. The
        // epoch path is not nonce-gated. It is a simple poll-and-publish
        // loop that runs alongside the canonical TxData-to-TxRef path. It
        // stays on spawn_blocking. The epoch lane does a sync Aeron poll
        // and a sync cluster offer. So the loop polls `is_signaled`
        // between backoff sleeps. `OriginPump`'s one-slot `pending` holds
        // a popped epoch across a backpressured offer, and the next tick
        // retries it before it polls again, so a backpressured epoch is
        // never dropped.
        let shutdown_for_deposits = shutdown.clone();
        let join_deposits = tokio::task::spawn_blocking(move || {
            OriginPump::<_, _, Pump<kardamom_types::EpochRecord>>::new(
                shutdown_for_deposits,
                epoch_subscription,
                epoch_pub,
            )
            .run()
        });

        // Independent pump for tx_remote_epochs to a remote-origin record
        // on tx_ordering, on the same terms as the deposit pump above,
        // with the same one-slot retry.
        let shutdown_for_remote_epochs = shutdown.clone();
        let join_remote_epochs = tokio::task::spawn_blocking(move || {
            OriginPump::<_, _, Pump<kardamom_types::xchain::RemoteEpochRecord>>::new(
                shutdown_for_remote_epochs,
                remote_epoch_subscription,
                remote_epoch_pub,
            )
            .run()
        });

        (join_main, join_deposits, join_remote_epochs)
    }
}

/// One origin-advancing pump: poll `sub`, publish through `publ`, and
/// idle-backoff, until `shutdown` fires or the source disconnects.
///
/// `Pending` is `kardamom_sequencer::pump::Pump<EpochRecord>` or
/// `Pump<RemoteEpochRecord>` — the one-slot retry state that holds a
/// popped record across a `Backpressure` result and retries it before it
/// polls again, so a backpressured record is never dropped.
struct OriginPump<S, P, Pending> {
    shutdown: Shutdown,
    sub: S,
    publ: P,
    pending: Pending,
}

impl<S, P, Pending> OriginPump<S, P, Pending>
where
    Pending: OriginLane<S, P> + Default,
{
    fn new(shutdown: Shutdown, sub: S, publ: P) -> Self {
        Self {
            shutdown,
            sub,
            publ,
            pending: Pending::default(),
        }
    }

    /// Run until `shutdown` fires or the source disconnects.
    fn run(mut self) -> Result<(), SequencerError> {
        let mut idle = IdleBackoff::new(Duration::from_micros(1), Duration::from_micros(100), 1);
        while !self.shutdown.is_signaled() && self.tick(&mut idle)? {}
        Ok(())
    }

    /// One [`Self::run`] iteration: dispatch on the lane's relay outcome.
    /// Returns whether the loop should keep going; `false` only on a
    /// clean `IngressDisconnected` exit.
    fn tick(&mut self, idle: &mut IdleBackoff) -> Result<bool, SequencerError> {
        match self.pending.relay(&mut self.sub, &mut self.publ) {
            Ok(true) => {
                idle.reset();
                Ok(true)
            }
            Ok(false) => {
                std::thread::sleep(idle.idle_wait());
                Ok(true)
            }
            Err(SequencerError::Backpressure) => {
                std::thread::sleep(Duration::from_micros(10));
                Ok(true)
            }
            Err(SequencerError::IngressDisconnected) => Ok(false),
            Err(e) => Err(e),
        }
    }
}

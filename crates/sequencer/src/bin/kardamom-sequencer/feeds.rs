//! The sequencer's background feeds, and the publish-loop spawner.
//!
//! Three loops feed, or drain, the publish path. The cluster-egress
//! watermark task and the `tx_receipts` floor task supply the
//! `ResyncController`: a replica that falls behind its twin past the
//! cluster's dedup window can double-order a transaction, so these feeds
//! detect the lag and drive the sequencer into a receipt-proof-only
//! publish mode until it clears. `PublishLoops::spawn` runs the
//! canonical `TxRef` loop and the `DepositRef` epoch pump on blocking
//! tasks.
//!
//! Seam rule: the async shell is tokio. Anything that blocks, such as
//! Aeron polls, the crossbeam egress receiver, or the microsecond-backoff
//! publish loop, runs on `spawn_blocking`. It polls `Shutdown::is_signaled`.
//! Async-capable work, such as the receipts fan-in on an existing tokio
//! channel, is a plain task. It uses `select!` on `Shutdown::cancelled`.

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
use kardamom_sequencer::epoch::process_epoch;
use kardamom_sequencer::error::SequencerError;
use kardamom_sequencer::metrics as seq_metrics;
use kardamom_sequencer::outbound::TxOrderingRefPublisher;
use kardamom_sequencer::partition::PartitionCount;
use kardamom_sequencer::remote_epoch::process_remote_epoch;
use kardamom_sequencer::resync::{FloorUpdate, ResyncController, SharedWatermark};
use kardamom_sequencer::sequencer::{Ports, Sequencer, Shutdown};
use kardamom_types::{BPosition, Receipt};

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
        if let Ok(EgressItem::ContiguityReject {
            sender,
            nonce,
            expected,
        }) = EgressItem::decode(frame)
        {
            tracing::warn!(
                partition = self.partition,
                ?sender,
                nonce,
                expected,
                "sealer contiguity reject received"
            );
            match self.reject_tx.try_send((sender, nonce, expected)) {
                Ok(()) | Err(crossbeam_channel::TrySendError::Disconnected(_)) => {}
                Err(crossbeam_channel::TrySendError::Full(_)) => {
                    // The publish loop is stalled past the resync window
                    // already. Dropping is safe: the confirm-timeout sweep
                    // still rewinds and republishes the affected ref, only
                    // later.
                    tracing::warn!(
                        partition = self.partition,
                        "contiguity-reject channel full; dropping"
                    );
                }
            }
        }
        true
    }

    /// Handle one remote-origin-reject frame. Returns `true` when
    /// `frame` was a remote-origin reject, so the caller does not also
    /// check it for a boundary.
    ///
    /// The sealer rejected a remote-origin record this sequencer relayed
    /// (audit H2/H9). The record is the watcher's, and the watcher
    /// reconciles its cursor with the destination at startup, so this is
    /// informational here: log loudly and count it.
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
            let reason = wire::remote_origin_reject_reason(reason);
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
        let gap = u64::try_from(now.duration_since(prev).as_millis()).unwrap_or(u64::MAX);
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

/// The `tx_receipts`-to-per-sender executed-truth floor feed. Only this
/// shard's senders reach the floor channel.
///
/// The receipts handle already fans in over a tokio unbounded channel. The
/// Aeron poll thread is the producer. So this is a plain async task. It
/// awaits `recv()` and `Shutdown::cancelled`, with no idle-sleep polling.
/// `floor_tx` stays a std channel, because its consumer is the sync
/// `ResyncController` in the publish loop.
pub(crate) struct ReceiptFloorFeed {
    partition_count: PartitionCount,
    partition_index: u32,
    floor_tx: crossbeam_channel::Sender<FloorUpdate>,
}

impl ReceiptFloorFeed {
    pub(crate) fn new(
        partition_count: PartitionCount,
        partition_index: u32,
        floor_tx: crossbeam_channel::Sender<FloorUpdate>,
    ) -> Self {
        Self {
            partition_count,
            partition_index,
            floor_tx,
        }
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

    async fn run(
        self,
        mut rx: tokio::sync::mpsc::UnboundedReceiver<(BPosition, Receipt)>,
        shutdown: Shutdown,
    ) {
        loop {
            let receipt = tokio::select! {
                biased;
                () = shutdown.cancelled() => return,
                msg = rx.recv() => match msg {
                    Some((_pos, receipt)) => receipt,
                    // The subscription closed. The runtime shut down.
                    // Nothing more to feed.
                    None => return,
                },
            };
            // Forward every partition-matched receipt. The controller splits
            // floor evidence (skip and deposit receipts excluded, since they
            // consume no L2 nonce) from publish confirmations (skip receipts
            // count as confirmations: ordering is the claim). See
            // `ResyncController::drain_floor_updates` for the split.
            //
            // Only this shard's senders can appear in this replica's publish
            // stream, so the floor map stays bounded to them.
            if self.partition_count.index_of(receipt.from) != self.partition_index {
                continue;
            }
            let update = FloorUpdate {
                sender: receipt.from,
                executed_nonce: receipt.nonce,
                skip_reason: receipt.skip_reason,
                deposit: receipt.is_deposit(),
            };
            match self.floor_tx.try_send(update) {
                Ok(()) => {}
                // The publish loop is stalled past the resync window already.
                // Dropping is safe: an unproven skip just falls through to
                // publish, the side every degraded mode already degrades
                // toward (see the module doc on `crate::resync`).
                Err(crossbeam_channel::TrySendError::Full(_)) => {
                    tracing::warn!("receipt-floor channel full; dropping");
                }
                // The publish loop is gone. Exit.
                Err(crossbeam_channel::TrySendError::Disconnected(_)) => return,
            }
        }
    }
}

pub(crate) type LoopHandle = tokio::task::JoinHandle<Result<(), SequencerError>>;

/// Argument group for [`spawn_publish_loops`]: the config, the `tx_data`
/// subscription, the three `tx_ordering` publishers, the `tx_errors`
/// publisher, the two origin subscriptions, the resync controller, and
/// one shutdown token. Each spawned loop clones the token itself.
pub(crate) struct PublishLoops<P> {
    pub(crate) cfg: SequencerConfig,
    pub(crate) tx_data: TxDataSubscriberHandle,
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
        // stays on spawn_blocking. `process_epoch` does a sync Aeron poll
        // and a sync cluster offer. So the loop polls `is_signaled`
        // between backoff sleeps. `OriginPump`'s one-slot `pending` holds
        // a popped epoch across a backpressured offer, and the next tick
        // retries it before it polls again, so a backpressured epoch is
        // never dropped (audit H2).
        let shutdown_for_deposits = shutdown.clone();
        let join_deposits = tokio::task::spawn_blocking(move || {
            OriginPump::new(shutdown_for_deposits, epoch_subscription, epoch_pub).run(process_epoch)
        });

        // Independent pump for tx_remote_epochs to a remote-origin record
        // on tx_ordering, on the same terms as the deposit pump above,
        // with the same one-slot retry.
        let shutdown_for_remote_epochs = shutdown.clone();
        let join_remote_epochs = tokio::task::spawn_blocking(move || {
            OriginPump::new(
                shutdown_for_remote_epochs,
                remote_epoch_subscription,
                remote_epoch_pub,
            )
            .run(process_remote_epoch)
        });

        (join_main, join_deposits, join_remote_epochs)
    }
}

/// One origin-advancing pump: poll `sub`, publish through `publ`, and
/// idle-backoff, until `shutdown` fires or the source disconnects.
///
/// `Pending` is the one-slot retry state (`epoch::PendingEpoch` or
/// `remote_epoch::PendingRemoteEpoch`). `step` holds a popped record
/// here across a `Backpressure` result and retries it before it polls
/// again, so a backpressured record is never dropped (audit H2).
struct OriginPump<S, P, Pending> {
    shutdown: Shutdown,
    sub: S,
    publ: P,
    pending: Pending,
}

impl<S, P, Pending: Default> OriginPump<S, P, Pending> {
    fn new(shutdown: Shutdown, sub: S, publ: P) -> Self {
        Self {
            shutdown,
            sub,
            publ,
            pending: Pending::default(),
        }
    }

    /// Run until `shutdown` fires or the source disconnects. `step` is
    /// [`process_epoch`] or [`process_remote_epoch`].
    fn run(
        mut self,
        mut step: impl FnMut(&mut S, &mut P, &mut Pending) -> Result<bool, SequencerError>,
    ) -> Result<(), SequencerError> {
        let mut idle = IdleBackoff::new(Duration::from_micros(1), Duration::from_micros(100), 1);
        loop {
            if self.shutdown.is_signaled() {
                return Ok(());
            }
            match step(&mut self.sub, &mut self.publ, &mut self.pending) {
                Ok(true) => idle.reset(),
                Ok(false) => std::thread::sleep(idle.idle_wait()),
                Err(SequencerError::Backpressure) => {
                    std::thread::sleep(Duration::from_micros(10));
                }
                Err(SequencerError::IngressDisconnected) => return Ok(()),
                Err(e) => return Err(e),
            }
        }
    }
}

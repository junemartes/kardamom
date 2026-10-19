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

mod lookup;
mod origin_pump;
mod receipts;
mod steps;

use std::sync::Arc;
use std::time::{Duration, Instant};

use kardamom_cluster_adapter::LiveEgress;
use kardamom_cluster_adapter::live::EgressPoll;
use kardamom_cluster_adapter::wire::{self, EgressItem};
use kardamom_log::aeron_live::{
    IdleCap, TxDataSubscriberHandle, TxDepositsSubscriberHandle,
    TxReceiptsBoundarySubscriberHandle, TxRemoteEpochsSubscriberHandle,
};
use kardamom_obs::halt::{HaltCause, HaltRef};
use kardamom_obs::lifecycle::process;
use kardamom_sequencer::config::SequencerConfig;
use kardamom_sequencer::epoch::{EpochPump, OriginSignalTx};
use kardamom_sequencer::error::SequencerError;
use kardamom_sequencer::fees::{FeeGate, LatestBaseFee};
use kardamom_sequencer::inbound::{Inbound, TxDataSubscriber};
use kardamom_sequencer::lookup::LookupRequester;
use kardamom_sequencer::metrics as seq_metrics;
use kardamom_sequencer::outbound::{SideChannels, TxOrderingRefPublisher};
use kardamom_sequencer::pump::Pump;
use kardamom_sequencer::resync::{
    ResyncController, SealerAnswer, SealerRefusal, SharedWatermark, elapsed_ms_saturating,
};
use kardamom_sequencer::sealer_rules::SealerRules;
use kardamom_sequencer::sequencer::{Ports, Sequencer, Shutdown};
use kardamom_types::cluster_status::SilenceDeadline;
pub(crate) use lookup::NonceLookupFeed;
use origin_pump::OriginPump;
pub(crate) use receipts::ReceiptFloorFeed;

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
    /// The refs the sealer refused for good: late ones, and ones the
    /// DA-lag guard or the record-lag guard refused. The publish loop
    /// drops each one and tells the client.
    deadline_tx: crossbeam_channel::Sender<SealerRefusal>,
    answer_tx: crossbeam_channel::Sender<SealerAnswer>,
    /// The epoch pump's signals: each growth of the boundaries' L1
    /// origin, and each origin-gap reject.
    origins: OriginSignalTx,
    /// The highest L1 origin a boundary carried. Zero is no origin.
    last_origin: u64,
    /// Anchored at feed start, not `None`. The cluster emits a boundary
    /// every tick, so "never seen a boundary" past the silence window is
    /// itself the lag state. A restarted replica whose session never
    /// re-establishes must flag, not stay silent forever. While the
    /// condition persists, `flag`'s re-arm repeats the flag once per
    /// silence window: a bounded, genuinely alarming heartbeat.
    last_boundary_at: Option<Instant>,
    /// The silence since the last boundary, never re-armed. Past its
    /// budget the replica pauses on the sealer's lost quorum; the next
    /// boundary resumes it. A status with the snapshot notice gives the
    /// snapshot budget: the leader sends no boundary until the snapshot
    /// is complete, so that gap is neither a lag nor a lost quorum.
    silence: SilenceDeadline,
    /// The sealer's decision version for the current session.
    rules: SealerRules,
}

impl EgressWatermarkFeed {
    pub(crate) fn new(
        silence_ms: u64,
        partition: u32,
        watermark: SharedWatermark,
        answer_tx: crossbeam_channel::Sender<SealerAnswer>,
        deadline_tx: crossbeam_channel::Sender<SealerRefusal>,
        origins: OriginSignalTx,
    ) -> Self {
        let rules = SealerRules::new(partition);
        Self {
            rules,
            silence_ms,
            partition,
            watermark,
            deadline_tx,
            answer_tx,
            origins,
            last_origin: 0,
            last_boundary_at: Some(Instant::now()),
            silence: SilenceDeadline::new(Instant::now()),
        }
    }

    /// Spawn on a blocking thread. `LiveEgress` holds nothing `!Send`,
    /// but its only wait primitive is a blocking crossbeam
    /// `recv_timeout`. `answer_tx` stays a std channel, because its
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
        let now = Instant::now();
        if self.silence.in_snapshot(now) {
            return;
        }
        self.flag(now);
        self.pause_on_silence(now);
    }

    /// Pause the replica on the sealer's lost quorum once no boundary
    /// arrived for the silence budget. A frame that is not a boundary
    /// does not end the silence.
    fn pause_on_silence(&self, now: Instant) {
        if self.silence.expired(now).is_some() {
            process().follow(Some(HaltRef::sealer(HaltCause::SealerNoQuorum)));
        }
    }

    fn on_frame(&mut self, frame: &[u8]) {
        if self.on_rules_frame(frame) {
            return;
        }
        if self.on_reject_frame(frame) {
            return;
        }
        if self.on_remote_origin_reject_frame(frame) {
            return;
        }
        if self.on_deadline_frame(frame) {
            return;
        }
        if self.on_da_lag_frame(frame) {
            return;
        }
        if self.on_record_lag_frame(frame) {
            return;
        }
        if self.on_origin_gap_frame(frame) {
            return;
        }
        if self.on_status_frame(frame) {
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

    /// Handle the sealer's rules and the start of a session. Returns `true`
    /// when `frame` was either. A sealer under version 2 sends no report.
    fn on_rules_frame(&mut self, frame: &[u8]) -> bool {
        if !matches!(
            frame.first(),
            Some(&(wire::EGRESS_KIND_SESSION_START | wire::EGRESS_KIND_SEALER_INFO))
        ) {
            return false;
        }
        match EgressItem::decode(frame) {
            Ok(EgressItem::SessionStart { session_id }) => {
                self.rules.on_session_start(session_id);
            }
            Ok(EgressItem::SealerInfo {
                decision_version,
                session_id,
            }) => {
                self.rules.on_sealer_info(session_id, decision_version);
            }
            _ => {}
        }
        true
    }

    /// Handle one contiguity-reject frame. Returns `true` when `frame`
    /// was a contiguity reject (already forwarded to `answer_tx`), so
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
        self.forward_answer(SealerAnswer::ContiguityReject {
            sender,
            nonce,
            expected,
        });
        true
    }

    /// Handle one past-deadline or window-full frame. Returns `true` when
    /// `frame` was either, so the caller does not also check it for a
    /// boundary.
    ///
    /// The two go to different places, because their remedies differ.
    ///
    /// - A window-full reject asks for the same ref again once the window
    ///   prunes. It takes the rewind channel. The publish loop
    ///   republishes the ref if it still holds it. It never frees the
    ///   nonce, because the answer names no expected nonce.
    /// - A past-deadline reject can never be accepted, whoever offers it.
    ///   It takes its own channel, so the publish loop drops the ledger
    ///   entry and tells the client to sign again.
    fn on_deadline_frame(&mut self, frame: &[u8]) -> bool {
        match frame.first() {
            Some(&wire::EGRESS_KIND_PAST_DEADLINE) => {
                if let Ok(EgressItem::PastDeadline {
                    sender,
                    nonce,
                    max_inclusion_block,
                    at_block,
                }) = EgressItem::decode(frame)
                {
                    tracing::warn!(
                        partition = self.partition,
                        ?sender,
                        nonce,
                        max_inclusion_block,
                        at_block,
                        "sealer past-deadline reject received; dropping the ref"
                    );
                    self.forward_refusal(SealerRefusal {
                        sender,
                        nonce,
                        reason: kardamom_types::TxErrorReason::PastDeadline {
                            max_inclusion_block,
                            at_block,
                        },
                    });
                }
                true
            }
            Some(&wire::EGRESS_KIND_WINDOW_FULL) => {
                if let Ok(EgressItem::WindowFull { sender, nonce }) = EgressItem::decode(frame) {
                    tracing::warn!(
                        partition = self.partition,
                        ?sender,
                        nonce,
                        "sealer window-full reject received; republishing the ref"
                    );
                    self.forward_answer(SealerAnswer::WindowFull { sender, nonce });
                }
                true
            }
            _ => false,
        }
    }

    /// Handle one DA-lag reject frame. Returns `true` when `frame` was
    /// one, so the caller does not also check it for a boundary.
    ///
    /// The sealer refused the ref because the chain is halted on a DA
    /// lag. No republish can order it until the batcher posts again, so
    /// it takes the terminal-refusal channel: the publish loop drops the
    /// ledger entry and tells the client to resubmit later.
    fn on_da_lag_frame(&mut self, frame: &[u8]) -> bool {
        if frame.first() != Some(&wire::EGRESS_KIND_DA_LAG_REJECT) {
            return false;
        }
        if let Ok(EgressItem::DaLagReject {
            sender,
            nonce,
            sealed_head,
            posted_head,
            budget_blocks,
        }) = EgressItem::decode(frame)
        {
            tracing::warn!(
                partition = self.partition,
                ?sender,
                nonce,
                sealed_head,
                posted_head,
                budget_blocks,
                "sealer DA-lag reject received; dropping the ref"
            );
            self.forward_refusal(SealerRefusal {
                sender,
                nonce,
                reason: kardamom_types::TxErrorReason::DaLag {
                    sealed_head,
                    posted_head,
                    budget_blocks,
                },
            });
        }
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

    /// Handle one status frame. Returns `true` when `frame` was a status.
    /// A status with the snapshot notice starts the snapshot stall: until
    /// its budget ends or the next boundary arrives, the boundary gap
    /// neither flags a lag nor pauses the replica. A plain status does
    /// not prove the boundary clock, so it changes nothing here.
    fn on_status_frame(&mut self, frame: &[u8]) -> bool {
        if frame.first() != Some(&wire::EGRESS_KIND_STATUS) {
            return false;
        }
        if let Ok(EgressItem::Status(status)) = EgressItem::decode(frame)
            && status.snapshot_follows
        {
            self.silence.on_status(&status, Instant::now());
            tracing::info!(
                partition = self.partition,
                "sequencer: the sealer takes a snapshot; the boundaries stop until it is complete"
            );
        }
        true
    }

    /// Store the canonical count from one boundary frame, and re-arm the
    /// silence timer. A 30 second freeze shows up here as one long
    /// inter-arrival gap. The backlog drains instantly on resume, but
    /// the gap between the last pre-freeze arrival and this one is real
    /// wall-clock time. A gap that a snapshot notice announced is no lag.
    fn on_boundary_frame(&mut self, frame: &[u8]) {
        if let Ok(EgressItem::Boundary(b)) = EgressItem::decode(frame) {
            let now = Instant::now();
            if !self.silence.in_snapshot(now) {
                self.flag(now);
            }
            self.last_boundary_at = Some(now);
            self.silence.on_boundary(now);
            process().follow(None);
            kardamom_obs::ready::mark_now(seq_metrics::LAST_BOUNDARY_UNIX_SECONDS);
            self.watermark.store(b.end_tx_idx.as_index());
            self.confirm_origin(b.l1_origin);
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

/// The executor-boundary feed into the base fee view: every closed
/// block's base fee and gas used imply the next block's base fee, which
/// the fee gate checks caps against. A plain async task, like the
/// receipts feed: the handle fans in over a tokio channel.
pub(crate) struct BaseFeeFeed {
    view: Arc<LatestBaseFee>,
}

impl BaseFeeFeed {
    pub(crate) fn new(view: Arc<LatestBaseFee>) -> Self {
        Self { view }
    }

    pub(crate) fn spawn(
        self,
        mut sub: TxReceiptsBoundarySubscriberHandle,
        shutdown: Shutdown,
    ) -> tokio::task::JoinHandle<()> {
        tokio::spawn(async move {
            loop {
                let boundary = tokio::select! {
                    biased;
                    () = shutdown.cancelled() => return,
                    msg = sub.recv() => match msg {
                        Some((_pos, boundary)) => boundary,
                        None => return,
                    },
                };
                self.view.on_boundary(&boundary);
            }
        })
    }
}

pub(crate) type LoopHandle = tokio::task::JoinHandle<Result<(), SequencerError>>;

/// Argument group for [`PublishLoops::spawn`]: the config, the `tx_data`
/// lane subscriptions, the three `tx_ordering` publishers, the side
/// channels, the two origin subscriptions, the resync controller, the
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
    pub(crate) side: SideChannels,
    /// The fee admission gate for the canonical loop.
    pub(crate) fees: FeeGate,
    pub(crate) epochs: TxDepositsSubscriberHandle,
    /// The epoch lane, whose signal sender the egress feed holds.
    pub(crate) epoch_lane: EpochPump,
    pub(crate) remote_epochs: TxRemoteEpochsSubscriberHandle,
    pub(crate) resync: Option<ResyncController>,
    /// `None` when the binary has no executor endpoints.
    pub(crate) lookup: Option<LookupRequester>,
    pub(crate) shutdown: Shutdown,
    /// The idle cap of the process, for the main sequencer loop.
    pub(crate) idle_cap: IdleCap,
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
            mut side,
            fees,
            epochs: epoch_subscription,
            epoch_lane,
            remote_epochs: remote_epoch_subscription,
            resync,
            lookup,
            shutdown,
            idle_cap,
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
            sequencer.enable_pause(process().subscribe());
            sequencer.enable_fees(fees);
            let mut ports = Ports {
                tx_data: &mut tx_data,
                refs: &mut main_pub,
                errors: &mut side,
            };
            sequencer.run(&mut ports, &shutdown_for_main, idle_cap)
        });

        // Independent pump for tx_deposits to epoch on tx_ordering. The
        // epoch path is not nonce-gated. It is a simple poll-and-publish
        // loop that runs alongside the canonical TxData-to-TxRef path. It
        // stays on spawn_blocking. The epoch lane does a sync Aeron poll
        // and a sync cluster offer. So the loop polls `is_signaled`
        // between backoff sleeps. The lane keeps every epoch until a
        // boundary confirms it, and offers the epochs again on an
        // origin-gap reject, so an epoch the cluster lost is never
        // dropped.
        let shutdown_for_deposits = shutdown.clone();
        let join_deposits = tokio::task::spawn_blocking(move || {
            OriginPump::new(
                shutdown_for_deposits,
                epoch_subscription,
                epoch_pub,
                epoch_lane,
            )
            .run()
        });

        // Independent pump for tx_remote_epochs to a remote-origin record
        // on tx_ordering, on the same terms as the deposit pump above,
        // with a one-slot retry.
        let shutdown_for_remote_epochs = shutdown.clone();
        let join_remote_epochs = tokio::task::spawn_blocking(move || {
            OriginPump::new(
                shutdown_for_remote_epochs,
                remote_epoch_subscription,
                remote_epoch_pub,
                Pump::<kardamom_types::xchain::RemoteEpochRecord>::default(),
            )
            .run()
        });

        (join_main, join_deposits, join_remote_epochs)
    }
}

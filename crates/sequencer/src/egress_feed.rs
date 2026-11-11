//! The egress-watermark feed of one sequencer replica: the cluster egress
//! of its session, into the sealer rules, the resync watermark, the lag
//! flag and the boundary-freshness metric.

mod guards;
mod steps;

use std::ops::ControlFlow;
use std::time::{Duration, Instant};

use guards::GuardPauses;
use kardamom_cluster_adapter::LiveEgress;
use kardamom_cluster_adapter::live::EgressPoll;
use kardamom_cluster_adapter::wire::{self, EgressItem};
use kardamom_obs::lifecycle::{Lifecycle, process};
use kardamom_obs::pipeline::{LoopMeter, LoopState};
use kardamom_types::cluster_status::SilenceDeadline;

use crate::epoch::OriginSignalTx;
use crate::metrics as seq_metrics;
use crate::outbound::DropPublisher;
use crate::resync::{SealerAnswer, SealerRefusal, SharedWatermark, elapsed_ms_saturating};
use crate::sealer_rules::SealerRules;
use crate::sequencer::Shutdown;

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
pub struct EgressWatermarkFeed<D> {
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
    pauses: GuardPauses,
    /// The kept decision version of the sealer, and the drops of the
    /// references that it refused.
    rules: SealerRules<D>,
    /// Idle while the receive waits, busy while a frame is handled.
    meter: LoopMeter,
}

impl<D: DropPublisher + 'static> EgressWatermarkFeed<D> {
    #[must_use]
    pub fn new(
        silence_ms: u64,
        partition: u32,
        watermark: SharedWatermark,
        answer_tx: crossbeam_channel::Sender<SealerAnswer>,
        deadline_tx: crossbeam_channel::Sender<SealerRefusal>,
        origins: OriginSignalTx,
        rules: SealerRules<D>,
    ) -> Self {
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
            pauses: GuardPauses::default(),
            meter: LoopMeter::new("sequencer.egress_feed"),
        }
    }

    /// Spawn on a blocking thread. `LiveEgress` holds nothing `!Send`,
    /// but its only wait primitive is a blocking crossbeam
    /// `recv_timeout`. `answer_tx` stays a std channel, because its
    /// consumer is the sync `ResyncController` in the publish loop.
    #[must_use]
    pub fn spawn(self, egress: LiveEgress, shutdown: Shutdown) -> tokio::task::JoinHandle<()> {
        tokio::task::spawn_blocking(move || self.run(egress, &shutdown))
    }

    /// Run until `shutdown` fires or the egress source closes.
    fn run(mut self, mut egress: LiveEgress, shutdown: &Shutdown) {
        while !shutdown.is_signaled() && self.step(&mut egress).is_continue() {}
    }

    /// One wait on `egress` and the work on what it gave. `Break` means
    /// that the egress source closed.
    fn step(&mut self, egress: &mut LiveEgress) -> ControlFlow<()> {
        let poll = egress.recv_timeout(Duration::from_millis(500));
        self.meter.mark(LoopState::Idle);
        match poll {
            EgressPoll::Frame(frame) => self.on_frame(&frame, process()),
            EgressPoll::Idle => self.on_idle(process()),
            EgressPoll::Closed => return ControlFlow::Break(()),
        }
        self.meter.mark(LoopState::Busy);
        ControlFlow::Continue(())
    }

    /// Egress is silent while this thread is demonstrably alive. This
    /// could be a partition from egress, or a dead cluster boundary
    /// clock. Either way, the response is the same.
    fn on_idle(&mut self, lifecycle: &Lifecycle) {
        let now = Instant::now();
        if self.silence.in_snapshot(now) {
            return;
        }
        self.flag(now);
        self.pause_on_silence(now, lifecycle);
    }

    fn on_frame(&mut self, frame: &[u8], lifecycle: &Lifecycle) {
        if self.on_rules_frame(frame) {
            return;
        }
        if self.rules.on_canonical_frame(frame) {
            return;
        }
        if self.on_ref_refused_frame(frame) {
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
        if self.on_da_lag_frame(frame, lifecycle) {
            return;
        }
        if self.on_record_lag_frame(frame, lifecycle) {
            return;
        }
        if self.on_origin_gap_frame(frame) {
            return;
        }
        if self.on_status_frame(frame, lifecycle) {
            return;
        }
        // Check the cheap kind byte first. Relayed records arrive at
        // full line rate on every replica, and fully decoding them
        // here, just to discard them, costs measurable CPU.
        if frame.first() != Some(&wire::EGRESS_KIND_BOUNDARY) {
            return;
        }
        self.on_boundary_frame(frame, lifecycle);
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

    /// Handle one reference refusal (egress kind 18). Returns `true` when
    /// `frame` was one. The sealer takes only a reference with the bytes of
    /// its transaction: the rules raise the version and drop the
    /// transaction.
    fn on_ref_refused_frame(&mut self, frame: &[u8]) -> bool {
        if frame.first() != Some(&wire::EGRESS_KIND_REF_REFUSED) {
            return false;
        }
        match EgressItem::decode(frame) {
            Ok(EgressItem::RefRefused(refused)) => self.rules.on_ref_refused(&refused),
            Ok(_) => {}
            Err(e) => tracing::warn!(
                partition = self.partition,
                error = %e,
                "a reference refusal of the sealer does not decode"
            ),
        }
        true
    }

    /// Handle one contiguity-reject frame. Returns `true` when `frame`
    /// was a contiguity reject (already forwarded to `answer_tx`), so
    /// the caller does not also check it for a boundary.
    ///
    /// The sealer rejected one of this sequencer's refs because a known
    /// sender's nonce was not the expected next one. Forward
    /// `(sender, nonce, expected)` to the publish loop. The v1 loop rewinds
    /// the unconfirmed ledger and republishes right away, instead of
    /// waiting out the confirm timeout. The rules tell the v2 loop, which
    /// offers the run again from the expected nonce.
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
        if !self.rules.on_contiguity_reject(sender, nonce, expected) {
            self.forward_answer(SealerAnswer::ContiguityReject {
                sender,
                nonce,
                expected,
            });
        }
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
                    if !self.rules.on_window_full(sender, nonce) {
                        self.forward_answer(SealerAnswer::WindowFull { sender, nonce });
                    }
                }
                true
            }
            _ => false,
        }
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

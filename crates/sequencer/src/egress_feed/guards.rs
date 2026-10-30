//! Guard status and boundary silence have separate clearing authorities.

use std::time::Instant;

use crate::metrics as seq_metrics;
use crate::outbound::DropPublisher;
use kardamom_cluster_adapter::wire::{self, EgressItem};
use kardamom_obs::halt::{HaltCause, HaltRef};
use kardamom_obs::lifecycle::Lifecycle;
use kardamom_types::ClusterStatus;

use super::EgressWatermarkFeed;

#[derive(Default)]
pub(super) struct GuardPauses {
    guard: Option<HaltCause>,
    silent: bool,
}

impl GuardPauses {
    fn on_status(&mut self, status: &ClusterStatus, lifecycle: &Lifecycle) {
        self.guard = if status.record_lag.halted {
            Some(HaltCause::RecordLag)
        } else {
            status.halted.then_some(HaltCause::DaLag)
        };
        self.follow(lifecycle);
    }

    pub(super) fn on_guard(&mut self, cause: HaltCause, lifecycle: &Lifecycle) {
        if self.guard != Some(HaltCause::RecordLag) {
            self.guard = Some(cause);
        }
        self.follow(lifecycle);
    }

    fn follow(&self, lifecycle: &Lifecycle) {
        let cause = if self.silent {
            Some(HaltCause::SealerNoQuorum)
        } else {
            self.guard
        };
        lifecycle.follow(cause.map(HaltRef::sealer));
    }
}

impl<D: DropPublisher + 'static> EgressWatermarkFeed<D> {
    /// Handle one DA-lag reject frame. Returns `true` when `frame` was
    /// one, so the caller does not also check it for a boundary.
    ///
    /// The sealer refused the ref because the chain is halted on a DA
    /// lag. V1 reports the refusal to the client. V2 retains its run for
    /// replay after a healthy status proves that the guard clears.
    pub(super) fn on_da_lag_frame(&mut self, frame: &[u8], lifecycle: &Lifecycle) -> bool {
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
            self.pauses.on_guard(HaltCause::DaLag, lifecycle);
            tracing::warn!(
                partition = self.partition,
                ?sender,
                nonce,
                sealed_head,
                posted_head,
                budget_blocks,
                "sealer DA-lag reject received"
            );
            self.forward_refusal(crate::resync::SealerRefusal {
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

    /// Only a boundary clears a silence pause. A status can clear a guard
    /// pause, but cannot prove that the boundary clock advances.
    pub(super) fn pause_on_silence(&mut self, now: Instant, lifecycle: &Lifecycle) {
        if self.silence.expired(now).is_some() {
            self.pauses.silent = true;
            self.pauses.follow(lifecycle);
        }
    }

    pub(super) fn on_status_frame(&mut self, frame: &[u8], lifecycle: &Lifecycle) -> bool {
        if frame.first() != Some(&wire::EGRESS_KIND_STATUS) {
            return false;
        }
        if let Ok(EgressItem::Status(status)) = EgressItem::decode(frame) {
            self.pauses.on_status(&status, lifecycle);
            if status.snapshot_follows {
                self.silence.on_status(&status, Instant::now());
                tracing::info!(
                    partition = self.partition,
                    "sequencer: the sealer takes a snapshot; the boundaries stop until it is complete"
                );
            }
        }
        true
    }

    /// A boundary ends an announced snapshot stall and a silence pause.
    /// It preserves any guard pause from the latest status or refusal.
    pub(super) fn on_boundary_frame(&mut self, frame: &[u8], lifecycle: &Lifecycle) {
        if let Ok(EgressItem::Boundary(b)) = EgressItem::decode(frame) {
            let now = Instant::now();
            if !self.silence.in_snapshot(now) {
                self.flag(now);
            }
            self.last_boundary_at = Some(now);
            self.silence.on_boundary(now);
            self.pauses.silent = false;
            self.pauses.follow(lifecycle);
            kardamom_obs::ready::mark_now(seq_metrics::LAST_BOUNDARY_UNIX_SECONDS);
            self.watermark.store(b.end_tx_idx.as_index());
            self.confirm_origin(b.l1_origin);
        }
    }
}

#[cfg(test)]
#[path = "guards_tests.rs"]
mod tests;

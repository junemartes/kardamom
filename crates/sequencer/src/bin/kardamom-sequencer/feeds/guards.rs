//! Guard status and boundary silence have separate clearing authorities.

use std::time::Instant;

use kardamom_cluster_adapter::wire::{self, EgressItem};
use kardamom_obs::halt::{HaltCause, HaltRef};
use kardamom_obs::lifecycle::Lifecycle;
use kardamom_sequencer::metrics as seq_metrics;
use kardamom_sequencer::outbound::DropPublisher;
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
        self.guard = Some(cause);
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

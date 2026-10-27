//! A lag guard holds intake through live boundaries until a status clears it.

use std::time::Instant;

use kardamom_cluster_adapter::wire::{self, EgressItem};
use kardamom_sequencer::outbound::DropPublisher;
use kardamom_sequencer::resync::SealerRefusal;
use kardamom_types::TxErrorReason;

use super::{EgressWatermarkFeed, HaltCause, HaltRef, process};

impl<D: DropPublisher + 'static> EgressWatermarkFeed<D> {
    pub(super) fn on_status_frame(&mut self, frame: &[u8]) -> bool {
        if frame.first() != Some(&wire::EGRESS_KIND_STATUS) {
            return false;
        }
        if let Ok(EgressItem::Status(status)) = EgressItem::decode(frame) {
            self.guard = if status.halted {
                Some(HaltRef::sealer(HaltCause::DaLag))
            } else if status.record_lag.halted {
                Some(HaltRef::sealer(HaltCause::RecordLag))
            } else {
                None
            };
            if self.guard.is_some() {
                process().follow(self.guard.clone());
            }
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

    pub(super) fn pause_for_refusal(&mut self, refusal: &SealerRefusal) {
        let cause = match refusal.reason {
            TxErrorReason::DaLag { .. } => HaltCause::DaLag,
            TxErrorReason::RecordLag { .. } => HaltCause::RecordLag,
            _ => return,
        };
        self.guard = Some(HaltRef::sealer(cause));
        process().follow(self.guard.clone());
    }
}

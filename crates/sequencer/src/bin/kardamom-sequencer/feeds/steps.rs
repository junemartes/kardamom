//! Step helpers for the feed loops in `feeds.rs`.
//!
//! Each helper is one step of a loop, or one `select!` branch, on the
//! struct that owns it.

use alloy_primitives::Address;
use std::ops::ControlFlow;

use kardamom_cache::QueryError;
use kardamom_cluster_adapter::wire::{self, EgressItem};
use kardamom_sequencer::epoch::OriginSignal;
use kardamom_sequencer::metrics as seq_metrics;
use kardamom_sequencer::resync::SealerRefusal;

use super::{EgressWatermarkFeed, NonceLookupFeed};

impl EgressWatermarkFeed {
    /// Handle one origin-gap frame. Returns `true` when `frame` was one,
    /// so the caller does not also check it for a boundary.
    ///
    /// The sealer refused one of this replica's epochs, because an earlier
    /// epoch is missing. The epoch pump offers its unconfirmed epochs
    /// again from the expected origin. This is normal progress after a
    /// leader change, so it is a warning and a count, not a halt.
    pub(super) fn on_origin_gap_frame(&mut self, frame: &[u8]) -> bool {
        if frame.first() != Some(&wire::EGRESS_KIND_ORIGIN_GAP) {
            return false;
        }
        if let Ok(EgressItem::OriginGap {
            offered_origin,
            expected_origin,
        }) = EgressItem::decode(frame)
        {
            tracing::warn!(
                partition = self.partition,
                offered_origin,
                expected_origin,
                "sealer origin-gap reject received; offering the epochs again"
            );
            seq_metrics::record_origin_gap();
            self.signal_origin(OriginSignal::Gap {
                expected: expected_origin,
            });
        }
        true
    }

    /// Handle one record-lag reject frame. Returns `true` when `frame` was
    /// one, so the caller does not also check it for a boundary.
    ///
    /// The sealer refused the ref because no executor recorded the chain
    /// within the record-lag budget. No republish can order it until an
    /// executor records again, so it takes the terminal-refusal channel:
    /// the publish loop drops the ledger entry and tells the client to
    /// resubmit later.
    pub(super) fn on_record_lag_frame(&mut self, frame: &[u8]) -> bool {
        if frame.first() != Some(&wire::EGRESS_KIND_RECORD_LAG_REJECT) {
            return false;
        }
        if let Ok(EgressItem::RecordLagReject {
            sender,
            nonce,
            sealed_index,
            recorded_index,
            budget,
        }) = EgressItem::decode(frame)
        {
            tracing::warn!(
                partition = self.partition,
                ?sender,
                nonce,
                sealed_index,
                recorded_index,
                budget,
                "sealer record-lag reject received; dropping the ref"
            );
            self.forward_refusal(SealerRefusal {
                sender,
                nonce,
                reason: kardamom_types::TxErrorReason::RecordLag {
                    sealed_index,
                    recorded_index,
                    budget,
                },
            });
        }
        true
    }

    /// Tell the epoch pump, and the `l1_origin` gauge, that the boundaries
    /// reached `l1_origin`, once per growth: a boundary arrives every
    /// tick, and the origin moves once per L1 block.
    pub(super) fn confirm_origin(&mut self, l1_origin: u64) {
        if l1_origin <= self.last_origin {
            return;
        }
        self.last_origin = l1_origin;
        seq_metrics::record_l1_origin(l1_origin);
        self.signal_origin(OriginSignal::Confirmed(l1_origin));
    }

    /// Send one signal to the epoch pump. The channel is unbounded, and the
    /// rate is at most one signal per epoch offer and one per L1 block.
    /// A send fails only after the pump stopped, at shutdown.
    fn signal_origin(&self, signal: OriginSignal) {
        let _ = self.origins.send(signal);
    }

    /// Forward one contiguity reject to the publish loop's reject
    /// channel. Drops it, with a warning, only when the channel is full:
    /// the confirm-timeout sweep still recovers a dropped reject, later.
    /// Forward one terminal refusal to the publish loop. A full channel
    /// means the loop has stalled far past the horizon already, and
    /// every ref in flight is refused: dropping is safe, because the
    /// confirm-timeout sweep still clears the ledger.
    pub(super) fn forward_refusal(&self, refusal: SealerRefusal) {
        match self.deadline_tx.try_send(refusal) {
            Ok(()) | Err(crossbeam_channel::TrySendError::Disconnected(_)) => {}
            Err(crossbeam_channel::TrySendError::Full(_)) => {
                tracing::warn!(
                    partition = self.partition,
                    "past-deadline channel full; dropping"
                );
            }
        }
    }

    pub(super) fn forward_contiguity_reject(&self, sender: Address, nonce: u64, expected: u64) {
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
}

impl NonceLookupFeed {
    /// Handle one lookup request from the core, for [`Self::tick`]'s
    /// `select!`. Always continues: a request never ends the drain.
    pub(super) fn on_lookup_request(&mut self, sender: Address) -> ControlFlow<()> {
        self.on_request(sender);
        ControlFlow::Continue(())
    }

    /// Record and log a failed lookup, for [`Self::on_done`]'s error arm.
    /// Always continues: a failed lookup does not end the drain.
    pub(super) fn record_lookup_error(&self, sender: Address, e: &QueryError) -> ControlFlow<()> {
        let outcome = if e.is_timeout() { "timeout" } else { "error" };
        seq_metrics::record_nonce_lookup(self.partition, outcome);
        tracing::warn!(sender = ?sender, error = %e, "nonce lookup failed");
        ControlFlow::Continue(())
    }
}

#[cfg(test)]
#[path = "steps_tests.rs"]
mod tests;

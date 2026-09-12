//! Step helpers for the feed loops in `feeds.rs`.
//!
//! Each helper is one step of a loop, or one `select!` branch, on the
//! struct that owns it.

use alloy_primitives::Address;
use std::ops::ControlFlow;

use kardamom_sequencer::metrics as seq_metrics;

use super::{EgressWatermarkFeed, NonceLookupFeed};

impl EgressWatermarkFeed {
    /// Forward one contiguity reject to the publish loop's reject
    /// channel. Drops it, with a warning, only when the channel is full:
    /// the confirm-timeout sweep still recovers a dropped reject, later.
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
    pub(super) fn record_lookup_error(&self, sender: Address, e: &str) -> ControlFlow<()> {
        let outcome = if e.contains("timed out") {
            "timeout"
        } else {
            "error"
        };
        seq_metrics::record_nonce_lookup(self.partition, outcome);
        tracing::warn!(sender = ?sender, error = %e, "nonce lookup failed");
        ControlFlow::Continue(())
    }
}

//! The receipts of this shard's senders, as the publish loop drains them:
//! the publish confirmations, the skip releases, and the executed-truth
//! floors. Each one moves the publish ledger or the nonce floor of one
//! sender. Each skip release also queues a nonce release for the sealer's
//! guard.

use std::time::Instant;

use alloy_primitives::Address;
use kardamom_cluster_adapter::wire::NonceRelease;
use kardamom_types::num::usize_to_u64;
use tracing::{info, trace};

use super::Sequencer;
use crate::metrics;
use crate::resync::ReceiptDrain;

impl Sequencer {
    /// Drain receipt floors, and move the nonce state machine to the
    /// chain. The releases go before the raises: each release is at or
    /// above the receipt floor at its skip, so a raise that comes later in
    /// the stream wins. Proven-duplicate buffered entries drop, and newly
    /// contiguous runs surface through `drain_pending` in `run_once`.
    pub(super) fn apply_receipt_drain(&mut self, r: &mut crate::resync::ResyncController) {
        let ReceiptDrain {
            raised,
            confirmations,
            released,
        } = r.drain_floor_updates();
        // A receipt proves that every one of this sequencer's published
        // refs for that sender, at or below its nonce, committed. Drop
        // them from the unconfirmed ledger.
        for (sender, confirmed) in confirmations {
            self.unconfirmed.confirm_through(sender, confirmed);
        }
        for release in released {
            self.apply_one_release(release);
        }
        for (sender, floor) in raised {
            self.apply_one_floor_update(sender, floor);
        }
        r.observe(Instant::now());
    }

    /// One skip release, for [`Self::apply_receipt_drain`]'s loop: the
    /// chain expects `release.nonce` of `release.sender` next, so the nonce
    /// floor goes back to it. Without this, the floor stays above the
    /// chain: every later nonce publishes into the gap and skips, and the
    /// resubmit at the chain nonce is a past nonce. The sealer's guard
    /// counts the skipped entry too, so the release also goes to the
    /// sealer, whether or not the floor of this replica moved.
    fn apply_one_release(&mut self, release: NonceRelease) {
        self.releases.push(release);
        let NonceRelease {
            sender,
            nonce: chain,
            ..
        } = release;
        let Some(from) = self.state.release_floor(sender, chain) else {
            return;
        };
        metrics::record_skip_release(self.cfg.partition_index);
        info!(
            sender = ?sender,
            from,
            chain,
            "skip receipt: the nonce floor goes back to the chain nonce"
        );
    }

    /// One receipt-proven floor update, for [`Self::apply_receipt_drain`]'s
    /// loop: advance the state machine's floor, and record the drops it
    /// proves, if the floor actually moved.
    fn apply_one_floor_update(&mut self, sender: Address, floor: u64) {
        self.nonce_ahead.forget(sender);
        let Some((from, dropped)) = self.state.advance_floor(sender, floor) else {
            return;
        };
        metrics::record_floor_advance(self.cfg.partition_index);
        // Every dropped buffered entry is a receipt-proven duplicate,
        // skipped without relying on any dedup window. This is the
        // spec's `resync_skipped_executed_total`. There is no separate
        // flush-time filter: floors drain before any publish action is
        // computed, so the state machine's floor is always current when
        // `process` runs. A proven-stale incoming envelope takes the
        // ordinary `Past`/DuplicatedTx path below, and is counted there.
        metrics::record_resync_skip(self.cfg.partition_index, usize_to_u64(dropped));
        trace!(
            sender = ?sender,
            from,
            floor,
            dropped,
            "resync: receipt floor advanced nonce state"
        );
    }
}

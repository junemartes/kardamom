//! The receipts of this shard's senders, as the publish loop drains them:
//! the publish confirmations, the skip releases, and the executed-truth
//! floors, and the floors that a rollback voids. Each one moves the
//! publish ledger or the nonce floor of one sender. A skip that proves the
//! chain nonce also queues a nonce release for the sealer's guard.

use std::time::Instant;

use alloy_primitives::Address;
use kardamom_types::num::usize_to_u64;
use tracing::{info, trace};

use super::Sequencer;
use crate::metrics;
use crate::resync::ReceiptDrain;

impl Sequencer {
    /// Drain receipt floors, and move the nonce state machine to the
    /// chain. The releases go before the raises: each release is at or
    /// above the receipt floor at its skip, so a raise that comes later in
    /// the stream wins. The forgotten floors go last: the drain ends at the
    /// rollback that voids them. Proven-duplicate buffered entries drop,
    /// and newly contiguous runs surface through `drain_pending` in
    /// `run_once`.
    ///
    /// Returns the senders whose floor a new lookup must learn: each
    /// forgotten floor, and each sender whose lookup answer a rollback
    /// voided. A request carries the rollback generation of `r`, so the
    /// caller sends the requests after `r` is back in `self.resync`.
    pub(super) fn apply_receipt_drain(
        &mut self,
        r: &mut crate::resync::ResyncController,
    ) -> Vec<Address> {
        let ReceiptDrain {
            raised,
            confirmations,
            released,
            forgotten,
            stale_lookups,
            sealer,
        } = r.drain_floor_updates();
        // A receipt proves that every one of this sequencer's published
        // refs for that sender, at or below its nonce, committed. Drop
        // them from the unconfirmed ledger.
        for (sender, confirmed) in confirmations {
            self.unconfirmed.confirm_through(sender, confirmed);
        }
        for (sender, chain) in released {
            self.apply_one_release(sender, chain);
        }
        for release in sealer {
            self.releases.push(release);
        }
        for (sender, floor) in raised {
            self.apply_one_floor_update(sender, floor);
        }
        for &sender in &forgotten {
            self.forget_one_floor(sender);
        }
        r.observe(Instant::now());
        forgotten.into_iter().chain(stale_lookups).collect()
    }

    /// One voided floor, for [`Self::apply_receipt_drain`]'s loop: the
    /// nonce state forgets the sender. A lookup learns its floor again
    /// from the committed state.
    fn forget_one_floor(&mut self, sender: Address) {
        self.nonce_ahead.forget(sender);
        let forgotten = self.state.forget_floor(sender);
        metrics::record_floor_forgotten(self.cfg.partition_index);
        info!(
            sender = ?sender,
            forgotten,
            "rollback: the nonce floor is unknown again; asking for the committed nonce"
        );
    }

    /// One skip release, for [`Self::apply_receipt_drain`]'s loop: the
    /// chain expects `chain` of `sender` next, so the nonce floor goes
    /// back to it. Without this, the floor stays above the chain: every
    /// later nonce publishes into the gap and skips, and the resubmit at
    /// `chain` is a past nonce.
    fn apply_one_release(&mut self, sender: Address, chain: u64) {
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

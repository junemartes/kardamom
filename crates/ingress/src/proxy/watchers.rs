//! Background stream watchers that [`IngressProxy::new`] spawns:
//! `tx_receipts`, `tx_errors`, the two watermark streams, and block
//! boundaries.

use std::sync::atomic::Ordering;

use kardamom_types::{BlockBoundary, Receipt, TxError};

use crate::channels::{IngressPublication, IngressSubscription};
use crate::seen_receipts::SeenReceipts;

use super::{BroadcastWatcher, IngressProxy};

impl<P, S> IngressProxy<P, S>
where
    P: IngressPublication + Clone + 'static,
    S: IngressSubscription + Clone + 'static,
{
    pub(super) fn spawn_block_boundary_watcher(&self) {
        let rx = self.subscription.subscribe_block_boundaries();
        let latest = self.latest_block_number.clone();
        // `fetch_max` keeps the counter increasing without a lock.
        tokio::spawn(
            BroadcastWatcher::new(rx).run(async move |b: BlockBoundary| {
                latest.fetch_max(b.block_number, Ordering::AcqRel);
            }),
        );
    }

    pub(super) fn spawn_tx_errors_watcher(&self) {
        // The sequencer emits a `TxError` on the tx_errors channel when it
        // rejects an inbound tx, today for a duplicate or a past nonce.
        // This code matches the error against parked submissions by
        // `(sender, nonce)`, and releases the client with a JSON-RPC error.
        // This is better than letting the client wait for a receipt that
        // will never arrive.
        //
        // Racing-replica dedup: P sequencer replicas race per
        // shard, so both can emit the same per-tx rejection, up to P
        // copies here. A rejection from one replica can also race a
        // success from its twin. `tx_error_dedup` drops duplicate copies
        // by `{sender, nonce, reason class}`, and drops a rejection
        // already overridden by an observed receipt. `pending.on_tx_error`
        // also holds the release for a short grace period, so a success
        // that arrives just after the rejection still wins.
        let rx = self.subscription.subscribe_tx_errors();
        let pending = self.pending.clone();
        let dedup = self.tx_error_dedup.clone();
        let feed = self.tx_error_feed.clone();
        tokio::spawn(BroadcastWatcher::new(rx).run(async move |err: TxError| {
            if !dedup.observe_error(err.sender, err.nonce, &err.reason) {
                metrics::counter!(crate::metrics::TX_ERROR_DUPLICATE_TOTAL).increment(1);
                return;
            }
            // This re-broadcasts the deduped event to subscription-mode
            // clients. `send` errors only when no subscriber exists,
            // which is fine.
            let _ = feed.send(err.clone());
            pending.on_tx_error(err.sender, err.nonce, err.reason).await;
        }));
    }

    pub(super) fn spawn_tx_receipts_watcher(&self) {
        // The enriched `Receipt` carries `from`, `nonce`, and `tx_hash`
        // directly. So a single tx_receipts subscription can populate both
        // the (sender, nonce) retry-dedup index and the tx_hash-to-Receipt
        // index that `eth_getTransactionReceipt` uses. It also drives
        // client release through `pending.on_receipt`.
        //
        // MDS fan-in dedup, first-wins by tx hash: with the
        // multi-destination subscription, all N executor replicas replay
        // the same canonical order and emit identical receipts. So each
        // receipt arrives up to N times on this stream. This code dedups
        // by `tx_hash`, so a tx's must-deliver ack fires exactly once: the
        // first receipt for a hash is processed, and every later copy is
        // dropped before it reaches `pending` or `cache`. Both
        // `pending.on_receipt`, keyed by (sender, nonce) and removed on
        // resolve, and `cache.insert` are already idempotent and
        // panic-free on their own. So this set is a cheap, explicit first
        // line that also avoids redundant work. It is a no-op on the
        // single-executor IPC path.
        let rx = self.subscription.subscribe_receipts();
        let pending = self.pending.clone();
        let cache = self.cache.clone();
        // Only this task ever touches `seen`, so it owns the set by
        // value, with no `Arc` or lock.
        let mut seen = SeenReceipts::default();
        let error_dedup = self.tx_error_dedup.clone();
        let feed = self.receipt_feed.clone();
        tokio::spawn(
            BroadcastWatcher::new(rx).run(async move |receipt: Receipt| {
                // First-wins: `insert` returns false if the hash was already
                // present. That means this is a duplicate replica copy, so
                // drop it.
                if !seen.insert(receipt.tx_hash) {
                    metrics::counter!(crate::metrics::RECEIPT_DUPLICATE_TOTAL).increment(1);
                    return;
                }
                let sender = receipt.from;
                let nonce = receipt.nonce;
                // Success overrides rejection. This marks the
                // outcome, so a racing replica's late DuplicatedTx for
                // this tx gets dropped.
                error_dedup.record_success(sender, nonce);
                cache.insert(receipt.clone());
                // This re-broadcasts the deduped event to subscription-mode
                // clients. `send` errors only when no subscriber exists,
                // which is fine.
                let _ = feed.send(receipt.clone());
                pending.on_receipt(sender, nonce, receipt).await;
            }),
        );
    }

    pub(super) fn spawn_quorum_watermark_watcher(&self) {
        let rx = self.subscription.subscribe_watermark();
        let pending = self.pending.clone();
        tokio::spawn(BroadcastWatcher::new(rx).run(async move |w| {
            pending.update_quorum_watermark(w).await;
        }));
    }

    pub(super) fn spawn_local_fsync_watermark_watcher(&self) {
        let rx = self.subscription.subscribe_local_fsync_watermark();
        let pending = self.pending.clone();
        tokio::spawn(BroadcastWatcher::new(rx).run(async move |w| {
            pending.update_local_watermark(w).await;
        }));
    }
}

//! Background stream watchers that [`IngressProxy::new`] spawns:
//! `tx_receipts`, `tx_errors`, the two watermark streams, and block
//! boundaries. Each watcher is a struct that holds the proxy state it
//! folds items into; [`BroadcastWatcher`] drains the stream into it.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use tokio::sync::{broadcast, watch};

use kardamom_types::{
    BlockBoundary, FsyncWatermark, QuorumWatermark, Receipt, ReceiptRollback, TxError,
};

use crate::channels::{IngressPublication, IngressSubscription, ReceiptItem};
use crate::cold_accounts::ColdAccounts;
use crate::fee_history::FeeHistory;
use crate::pending::PendingReceipts;
use crate::receipt_cache::ReceiptCache;
use crate::rollback_log::RollbackLog;
use crate::seen_receipts::SeenReceipts;
use crate::tx_error_dedup::TxErrorDedup;

use super::{BroadcastWatcher, IngressProxy, Watch};

impl<P, S> IngressProxy<P, S>
where
    P: IngressPublication + Clone + 'static,
    S: IngressSubscription + Clone + 'static,
{
    pub(super) fn spawn_block_boundary_watcher(&self) {
        BroadcastWatcher::new(
            self.subscription.subscribe_block_boundaries(),
            BlockBoundaryWatcher::new(self.latest_block_number.clone(), self.fee_history.clone()),
        )
        .spawn();
    }

    pub(super) fn spawn_tx_errors_watcher(&self) {
        BroadcastWatcher::new(
            self.subscription.subscribe_tx_errors(),
            TxErrorsWatcher::new(
                self.pending.clone(),
                self.tx_error_dedup.clone(),
                self.tx_error_feed.clone(),
            ),
        )
        .spawn();
    }

    /// Spawn the receipt watcher. It owns `applied`, the log of the
    /// rollbacks that it applied.
    pub(super) fn spawn_tx_receipts_watcher(&self, applied: watch::Sender<RollbackLog>) {
        BroadcastWatcher::new(
            self.subscription.subscribe_receipts(),
            TxReceiptsWatcher {
                pending: self.pending.clone(),
                cache: self.cache.clone(),
                seen: SeenReceipts::default(),
                error_dedup: self.tx_error_dedup.clone(),
                feed: self.receipt_feed.clone(),
                fee_history: self.fee_history.clone(),
                latest: self.latest_block_number.clone(),
                cold: self.cold.clone(),
                rollbacks: self.subscription.rollbacks(),
                applied,
            },
        )
        .spawn();
    }

    pub(super) fn spawn_quorum_watermark_watcher(&self) {
        BroadcastWatcher::new(
            self.subscription.subscribe_watermark(),
            WatermarkWatcher::new(self.pending.clone()),
        )
        .spawn();
    }

    pub(super) fn spawn_local_fsync_watermark_watcher(&self) {
        BroadcastWatcher::new(
            self.subscription.subscribe_local_fsync_watermark(),
            WatermarkWatcher::new(self.pending.clone()),
        )
        .spawn();
    }

    pub(super) fn spawn_chain_watch(&self) {
        crate::chain::ChainWatch::new(
            self.cluster_status.clone(),
            self.subscription.service_board(),
            self.sealer.clone(),
        )
        .spawn();
    }
}

/// Folds block boundaries into the proxy's latest-block counter and the
/// fee history.
struct BlockBoundaryWatcher {
    latest: Arc<AtomicU64>,
    fee_history: Arc<FeeHistory>,
}

impl BlockBoundaryWatcher {
    fn new(latest: Arc<AtomicU64>, fee_history: Arc<FeeHistory>) -> Self {
        Self {
            latest,
            fee_history,
        }
    }
}

impl Watch<BlockBoundary> for BlockBoundaryWatcher {
    /// `fetch_max` keeps the counter increasing without a lock.
    async fn on_item(&mut self, b: BlockBoundary) {
        self.latest.fetch_max(b.block_number, Ordering::AcqRel);
        self.fee_history.on_boundary(&b);
    }
}

/// The sequencer emits a `TxError` on the `tx_errors` channel when it
/// rejects an inbound tx, today for a duplicate or a past nonce. This
/// watcher matches the error against parked submissions by
/// `(sender, nonce)`, and releases the client with a JSON-RPC error.
/// This is better than letting the client wait for a receipt that will
/// never arrive.
///
/// Racing-replica dedup: P sequencer replicas race per shard, so both
/// can emit the same per-tx rejection, up to P copies here. A rejection
/// from one replica can also race a success from its twin. `dedup` drops
/// duplicate copies by `{sender, nonce, reason class}`, and drops a
/// rejection already overridden by an observed receipt.
/// `pending.on_tx_error` also holds the release for a short grace period,
/// so a success that arrives just after the rejection still wins.
struct TxErrorsWatcher {
    pending: Arc<PendingReceipts>,
    dedup: Arc<TxErrorDedup>,
    feed: broadcast::Sender<TxError>,
}

impl TxErrorsWatcher {
    fn new(
        pending: Arc<PendingReceipts>,
        dedup: Arc<TxErrorDedup>,
        feed: broadcast::Sender<TxError>,
    ) -> Self {
        Self {
            pending,
            dedup,
            feed,
        }
    }
}

impl Watch<TxError> for TxErrorsWatcher {
    async fn on_item(&mut self, err: TxError) {
        if !self.dedup.observe_error(err.sender, err.nonce, &err.reason) {
            metrics::counter!(crate::metrics::TX_ERROR_DUPLICATE_TOTAL).increment(1);
            return;
        }
        // This re-broadcasts the deduped event to subscription-mode
        // clients. `send` errors only when no subscriber exists, which is
        // fine.
        let _ = self.feed.send(err.clone());
        self.pending
            .on_tx_error(err.sender, err.nonce, err.reason)
            .await;
    }
}

/// The enriched `Receipt` carries `from`, `nonce`, and `tx_hash`
/// directly. So a single `tx_receipts` subscription can populate both the
/// (sender, nonce) retry-dedup index and the tx_hash-to-Receipt index
/// that `eth_getTransactionReceipt` uses. It also drives client release
/// through `pending.on_receipt`.
///
/// MDS fan-in dedup, first-wins by tx hash: with the multi-destination
/// subscription, all N executor replicas replay the same canonical order
/// and emit identical receipts. So each receipt arrives up to N times on
/// this stream. This watcher dedups by `tx_hash`, so a tx's must-deliver
/// ack fires exactly once: the first receipt for a hash is processed, and
/// every later copy is dropped before it reaches `pending` or `cache`.
/// Both `pending.on_receipt`, keyed by (sender, nonce) and removed on
/// resolve, and `cache.insert` are already idempotent and panic-free on
/// their own. So `seen` is a cheap, explicit first line that also avoids
/// redundant work. It is a no-op on the single-executor IPC path. Only
/// this task ever touches `seen`, so it owns the set by value, with no
/// `Arc` or lock.
///
/// A rollback applies once for each event. It comes on the bus in stream
/// order. When the bus drops items, the watcher reads the rollbacks that it
/// missed from the rollback log of the subscription.
struct TxReceiptsWatcher {
    pending: Arc<PendingReceipts>,
    cache: Arc<ReceiptCache>,
    seen: SeenReceipts,
    error_dedup: Arc<TxErrorDedup>,
    feed: broadcast::Sender<ReceiptItem>,
    fee_history: Arc<FeeHistory>,
    /// The head that `eth_blockNumber` reads. A rollback lowers it.
    latest: Arc<AtomicU64>,
    /// The executor answers of the cold senders. A rollback empties them.
    cold: Arc<ColdAccounts>,
    /// The recent rollbacks of the subscription, for a gap on the bus.
    rollbacks: watch::Receiver<RollbackLog>,
    /// The rollbacks that this watcher applied. The receipt subscriptions
    /// read it.
    applied: watch::Sender<RollbackLog>,
}

impl TxReceiptsWatcher {
    async fn on_receipt(&mut self, receipt: Receipt) {
        // First-wins: `insert` returns false if the hash was already
        // present. That means this is a duplicate replica copy, so drop
        // it.
        if !self.seen.insert(receipt.tx_hash, receipt.block_number) {
            metrics::counter!(crate::metrics::RECEIPT_DUPLICATE_TOTAL).increment(1);
            return;
        }
        let sender = receipt.from;
        let nonce = receipt.nonce;
        // Success overrides rejection. This marks the outcome, so a
        // racing replica's late DuplicatedTx for this tx gets dropped.
        self.error_dedup.record_success(sender, nonce);
        self.fee_history
            .on_receipt(receipt.block_number, receipt.priority_fee_per_gas);
        self.cache.insert(receipt.clone());
        // This re-broadcasts the deduped event to subscription-mode
        // clients. `send` errors only when no subscriber exists, which is
        // fine.
        let _ = self.feed.send(receipt.clone().into());
        self.pending.on_receipt(sender, nonce, receipt).await;
    }

    /// An executor voided the receipts above `to_block`. Drop them from
    /// the cache, the fee history and the head, and forget their seen
    /// hashes, so a transaction that comes again with its old hash passes.
    /// Forget the cold answers too: one can hold a lost state. The feed
    /// carries the rollback on, and each receipt subscription ends on it.
    /// A rollback of an event that this watcher applied already is a late
    /// copy: it changes nothing, so the receipts of the new stream stay.
    fn on_rollback(&mut self, rollback: ReceiptRollback) {
        if !self.applied.send_if_modified(|log| log.push(rollback)) {
            tracing::debug!(to_block = rollback.to_block, id = %rollback.id, "a rollback applied already");
            return;
        }
        let to_block = rollback.to_block;
        tracing::warn!(
            to_block,
            id = %rollback.id,
            "an executor rolled back: dropping the receipts above the block"
        );
        self.cache.roll_back(to_block);
        self.seen.forget_above(to_block);
        self.fee_history.roll_back(to_block);
        self.latest.fetch_min(to_block, Ordering::AcqRel);
        self.cold.clear();
        let _ = self.feed.send(ReceiptItem::Rollback(rollback));
    }
}

impl Watch<ReceiptItem> for TxReceiptsWatcher {
    async fn on_item(&mut self, item: ReceiptItem) {
        match item {
            ReceiptItem::Receipt(receipt) => self.on_receipt(receipt).await,
            ReceiptItem::Rollback(rollback) => self.on_rollback(rollback),
        }
    }

    /// The bus dropped items, and a rollback can be among them. Apply each
    /// recent rollback of the log: the ones that this watcher applied
    /// already change nothing.
    fn on_lagged(&mut self) {
        let recent: Vec<ReceiptRollback> = self
            .rollbacks
            .borrow_and_update()
            .since(0)
            .copied()
            .collect();
        for rollback in recent {
            self.on_rollback(rollback);
        }
    }
}

/// Feeds the quorum and local-fsync watermarks into the parked
/// submissions. One struct serves both streams: they differ only in the
/// `PendingReceipts` method each item lands on.
struct WatermarkWatcher {
    pending: Arc<PendingReceipts>,
}

impl WatermarkWatcher {
    fn new(pending: Arc<PendingReceipts>) -> Self {
        Self { pending }
    }
}

impl Watch<QuorumWatermark> for WatermarkWatcher {
    async fn on_item(&mut self, w: QuorumWatermark) {
        self.pending.update_quorum_watermark(w).await;
    }
}

impl Watch<FsyncWatermark> for WatermarkWatcher {
    async fn on_item(&mut self, w: FsyncWatermark) {
        self.pending.update_local_watermark(w).await;
    }
}

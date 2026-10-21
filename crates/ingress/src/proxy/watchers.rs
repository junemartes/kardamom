//! Background stream watchers that [`IngressProxy::new`] spawns:
//! `tx_receipts`, `tx_errors`, the two watermark streams, and block
//! boundaries. Each watcher is a struct that holds the proxy state it
//! folds items into; [`BroadcastWatcher`] drains the stream into it.

use std::ops::ControlFlow;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use tokio::sync::broadcast::error::TryRecvError;
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

/// The `bus` labels of [`crate::metrics::BUS_LAGGED_TOTAL`].
const BOUNDARIES_BUS: &str = "block_boundaries";
const TX_ERRORS_BUS: &str = "tx_errors";
const RECEIPTS_BUS: &str = "tx_receipts";
const QUORUM_WATERMARK_BUS: &str = "quorum_watermark";
const LOCAL_FSYNC_BUS: &str = "local_fsync_watermark";

impl<P, S> IngressProxy<P, S>
where
    P: IngressPublication + Clone + 'static,
    S: IngressSubscription + Clone + 'static,
{
    pub(super) fn spawn_block_boundary_watcher(&self) {
        BroadcastWatcher::new(
            BOUNDARIES_BUS,
            self.subscription.subscribe_block_boundaries(),
            BlockBoundaryWatcher::new(self.latest_block_number.clone(), self.fee_history.clone()),
        )
        .spawn();
    }

    pub(super) fn spawn_tx_errors_watcher(&self) {
        BroadcastWatcher::new(
            TX_ERRORS_BUS,
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
            RECEIPTS_BUS,
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
            QUORUM_WATERMARK_BUS,
            self.subscription.subscribe_watermark(),
            WatermarkWatcher::new(self.pending.clone()),
        )
        .spawn();
    }

    pub(super) fn spawn_local_fsync_watermark_watcher(&self) {
        BroadcastWatcher::new(
            LOCAL_FSYNC_BUS,
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
/// A rollback applies once for each event, in the order of the bus. When
/// the bus drops items, the watcher reads the rollbacks that it missed
/// from the rollback log of the subscription. See [`Self::catch_up`].
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

    /// The bus dropped items, and a rollback can be among them. Read on
    /// until a read of the queue sees no new gap.
    async fn on_lagged(&mut self, rx: &mut broadcast::Receiver<ReceiptItem>) {
        while self.catch_up(rx).await.is_continue() {}
    }
}

impl TxReceiptsWatcher {
    /// One read of the queue after a gap. The pump puts a rollback on the
    /// bus before it puts it in the log. So each rollback of the log that
    /// this watcher did not apply is in the queue, or the bus dropped it.
    ///
    /// - A rollback in the queue applies in bus order. The receipts before
    ///   it on the bus can be above its block, and it must drop them.
    /// - A rollback that the bus dropped came before the whole queue. It
    ///   applies first.
    ///
    /// When the bus drops items again during the read, a missing rollback
    /// can still come after the new gap. None then applies from the log,
    /// and the next read decides. A late rollback only drops valid
    /// receipts, which the receipt lookup serves again. An early one keeps
    /// void receipts. `Continue` means a new gap.
    async fn catch_up(&mut self, rx: &mut broadcast::Receiver<ReceiptItem>) -> ControlFlow<()> {
        let missed = self.unapplied();
        let queue = LagQueue::read(rx);
        queue
            .dropped(missed)
            .into_iter()
            .for_each(|rollback| self.on_rollback(rollback));
        let next = queue.next();
        for item in queue.items {
            self.on_item(item).await;
        }
        next
    }

    /// The rollbacks of the subscription log that this watcher did not
    /// apply, oldest first.
    fn unapplied(&mut self) -> Vec<ReceiptRollback> {
        let applied = self.applied.borrow();
        self.rollbacks
            .borrow_and_update()
            .since(0)
            .filter(|rollback| !applied.contains(rollback.id))
            .copied()
            .collect()
    }
}

/// The items that the receipt bus holds after a gap, oldest first.
struct LagQueue {
    items: Vec<ReceiptItem>,
    /// False when the bus dropped items again during the read.
    complete: bool,
}

impl LagQueue {
    /// Read the items that `rx` holds now. The read stops at the count of
    /// its start, so a fast pump does not keep it going.
    fn read(rx: &mut broadcast::Receiver<ReceiptItem>) -> Self {
        let held = rx.len();
        let mut queue = Self {
            items: Vec::with_capacity(held),
            complete: true,
        };
        while queue.items.len() < held && queue.take(rx).is_continue() {}
        queue
    }

    /// Take one item of `rx`. `Break` when the bus is empty, or when it
    /// dropped items again.
    fn take(&mut self, rx: &mut broadcast::Receiver<ReceiptItem>) -> ControlFlow<()> {
        match rx.try_recv() {
            Ok(item) => {
                self.items.push(item);
                ControlFlow::Continue(())
            }
            Err(TryRecvError::Lagged(dropped)) => {
                crate::metrics::count_lagged(RECEIPTS_BUS, dropped);
                self.complete = false;
                ControlFlow::Break(())
            }
            Err(TryRecvError::Empty | TryRecvError::Closed) => ControlFlow::Break(()),
        }
    }

    /// The rollbacks of `missed` that the bus dropped: after a complete
    /// read, the ones that are not in the queue.
    fn dropped(&self, missed: Vec<ReceiptRollback>) -> Vec<ReceiptRollback> {
        missed
            .into_iter()
            .filter(|rollback| self.complete && !self.holds(rollback))
            .collect()
    }

    /// Whether the queue holds a rollback of the event of `rollback`.
    fn holds(&self, rollback: &ReceiptRollback) -> bool {
        self.items
            .iter()
            .any(|item| matches!(item, ReceiptItem::Rollback(r) if r.id == rollback.id))
    }

    /// `Continue` when the bus dropped items again during the read.
    fn next(&self) -> ControlFlow<()> {
        if self.complete {
            ControlFlow::Break(())
        } else {
            ControlFlow::Continue(())
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

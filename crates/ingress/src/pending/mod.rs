//! Pending-receipts map. It parks a client `oneshot` until both:
//! (a) a `Receipt` for the matching `(sender, nonce)` arrives on the
//!     `tx_receipts` stream. The executor's enriched receipt carries
//!     `from`, `nonce`, and `tx_hash` directly. And
//! (b) the durability gate that [`AckPolicy`] selects has reached
//!     `receipt.tx_idx`.
//!
//! Invariant I2 requires both conditions.
//!
//! The durability gate is configurable. See [`AckPolicy`] for the four
//! modes. `OnQuorum`, the default, keeps the original behavior: wait for
//! the shared quorum watermark. `OnOffer` skips the watermark wait
//! entirely. `OnLocalFsync` waits on this node's per-recorder fsync
//! stream. `OnLocalFsyncAndQuorum` requires both to have passed the
//! position.
//!
//! ## Ownership topology, leak-proof by construction
//!
//! The waiter owns each entry: [`PendingWait`] holds the only long-lived
//! strong `Arc`, and the map indexes entries through a `Weak`. The
//! watcher paths, `on_receipt`, `on_tx_error`, and `release_satisfied`,
//! are pure readers. They call `upgrade()` and treat a dead `Weak` as "no
//! client parked." They never remove anything. The wait's `Drop` is the
//! one removal site: it reaps the slot, guarded by identity, before the
//! entry `Arc` dies. So however the wait ends, whether by receipt,
//! rejection, timeout, or the RPC handler future being dropped on client
//! disconnect, the slot and the entry go together, and a dead `Weak` is
//! never observable in the map. No removal call needs to be tracked
//! anywhere else: `Drop` is the one removal site.

use std::sync::{Arc, Weak};
use std::time::Duration;

use alloy_primitives::Address;
use dashmap::DashMap;
use tokio::sync::{Mutex, oneshot};

use kardamom_types::{
    AckPolicy, BPosition, FsyncWatermark, QuorumWatermark, Receipt, TxErrorReason,
};

use crate::error::IngressError;
use crate::sync_util::LockIgnorePoison;

#[derive(Debug, Clone)]
pub struct ReceiptResponse {
    pub receipt: Receipt,
}

/// Internal entry: a parked oneshot sender, plus the receipt, once it
/// has arrived.
struct Entry {
    responder: Option<oneshot::Sender<Result<ReceiptResponse, IngressError>>>,
    receipt: Option<Receipt>,
}

/// The registry's index. Values are `Weak`: the map can find an entry,
/// but never keeps one alive. The strong ref lives in the `PendingWait`
/// that the submitting handler holds. See the module docs' ownership
/// topology.
type Index = DashMap<(Address, u64), Weak<Mutex<Entry>>>;

#[derive(Clone)]
struct PendingMap(Arc<Index>);

impl PendingMap {
    fn new() -> Self {
        Self(Arc::new(DashMap::new()))
    }

    fn insert(&self, key: (Address, u64), weak: Weak<Mutex<Entry>>) {
        self.0.insert(key, weak);
    }

    /// Looks up the live entry for `key`. A dead `Weak`, unobservable in
    /// practice, see the module docs, looks the same as an absent key:
    /// the parked client is gone either way.
    fn lookup(&self, key: &(Address, u64)) -> Option<Arc<Mutex<Entry>>> {
        self.0.get(key).and_then(|r| r.value().upgrade())
    }

    /// Looks up the `Weak` for `key`, without upgrading it to a strong
    /// `Arc`. A deferred-release path that only needs to check the entry
    /// later, not hold it alive now, uses this instead of [`Self::lookup`].
    fn lookup_weak(&self, key: &(Address, u64)) -> Option<Weak<Mutex<Entry>>> {
        self.0.get(key).map(|r| r.value().clone())
    }

    /// The one removal path, called from `PendingWait::drop`. Removes
    /// `key`'s slot only if it still indexes `entry`, checked by pointer
    /// identity, then refreshes the depth gauge. The identity check
    /// matters because a re-`register` of the same (sender, nonce)
    /// replaces the slot with a new entry, and the old wait's later
    /// `Drop` must never evict the new registration.
    fn remove_slot(&self, key: &(Address, u64), entry: &Arc<Mutex<Entry>>) {
        self.0
            .remove_if(key, |_, w| std::ptr::eq(w.as_ptr(), Arc::as_ptr(entry)));
        self.set_queue_depth();
    }

    /// The registry owns the queue-depth gauge. The depth changes exactly
    /// when an entry is inserted or removed, including a removal on a
    /// path no proxy code runs directly, such as a cancelled handler's
    /// `PendingWait::drop`.
    fn set_queue_depth(&self) {
        // A gauge only needs an approximate value; `f64`'s 52-bit
        // mantissa covers every depth this registry ever reaches.
        #[allow(clippy::cast_precision_loss)]
        let depth = self.0.len() as f64;
        metrics::gauge!(crate::metrics::QUEUE_DEPTH).set(depth);
    }

    fn len(&self) -> usize {
        self.0.len()
    }
}

/// Tracked watermarks. A field the policy does not need stays `None`
/// forever, and the gate skips it.
#[derive(Default, Clone, Copy)]
struct Watermarks {
    quorum: Option<BPosition>,
    local: Option<BPosition>,
}

/// How long a sequencer rejection is held before it releases the parked
/// client with an error, giving a racing success a chance to win. With P
/// racing sequencer replicas, a replica with a briefly stale nonce floor
/// can reject a tx that its twin accepted and ordered. The rejection is
/// emitted at ordering time, while the receipt lands only after
/// execution, so the error usually arrives first. The grace period must
/// exceed the ordering-to-execution-to-receipt latency, tens of
/// milliseconds in the cluster. A genuine rejection, where both replicas
/// reject, is only delayed by this long, which a client that submits a
/// duplicate can easily afford.
pub(crate) const DEFAULT_TX_ERROR_GRACE: Duration = Duration::from_millis(500);

pub(crate) struct PendingReceipts {
    policy: AckPolicy,
    map: PendingMap,
    /// Latest watermarks observed: one `watch` slot per kind. A `watch`
    /// channel gives the cache-latest-value behavior, with no
    /// one-receiver-per-await fanout, and lock-free reads. Writers call
    /// `send_replace`. Readers call `borrow`, with no lock and no await.
    quorum: tokio::sync::watch::Sender<Option<BPosition>>,
    local: tokio::sync::watch::Sender<Option<BPosition>>,
    /// See [`DEFAULT_TX_ERROR_GRACE`]. Tests can override this.
    error_grace: Duration,
    /// Watermark-ordered index of parked entries: `(tx_idx, seq)` to
    /// `Weak`. Draining the satisfied prefix of this index makes a
    /// watermark tick cost O(released + log parked), with no allocation
    /// at steady state. An entry is inserted only when a receipt arrives
    /// still gated. A dropped waiter's `Weak` simply fails to upgrade at
    /// drain time.
    parked: std::sync::Mutex<ParkedIndex>,
    park_seq: std::sync::atomic::AtomicU64,
}

/// `(tx_idx, insertion seq)` to parked entry, ordered by watermark.
type ParkedIndex = std::collections::BTreeMap<(BPosition, u64), Weak<Mutex<Entry>>>;

impl Default for PendingReceipts {
    fn default() -> Self {
        Self::new(AckPolicy::default())
    }
}

impl PendingReceipts {
    pub(crate) fn new(policy: AckPolicy) -> Self {
        Self::with_error_grace(policy, DEFAULT_TX_ERROR_GRACE)
    }

    /// Like [`new`](Self::new), with an explicit rejection-release
    /// grace. `Duration::ZERO` releases errors inline, with no
    /// success-override window.
    pub(crate) fn with_error_grace(policy: AckPolicy, error_grace: Duration) -> Self {
        Self {
            policy,
            map: PendingMap::new(),
            quorum: tokio::sync::watch::Sender::new(None),
            local: tokio::sync::watch::Sender::new(None),
            error_grace,
            parked: std::sync::Mutex::new(std::collections::BTreeMap::new()),
            park_seq: std::sync::atomic::AtomicU64::new(0),
        }
    }

    /// Two-phase register. Returns a `PendingWait` for the caller to
    /// await with a timeout. Calling code must `register` before it
    /// publishes the tx, so the receipt cannot arrive before the
    /// registration.
    ///
    /// The returned `PendingWait` holds the entry's only strong ref; the
    /// map gets a `Weak`. However the wait ends, whether by receipt,
    /// rejection, timeout, or the caller's future being dropped on
    /// client disconnect, which cancels the RPC handler, the wait owns
    /// the only strong `Arc`, so every end path frees the entry and
    /// reaps its slot.
    pub(crate) fn register(&self, sender: Address, nonce: u64) -> PendingWait {
        let (tx, rx) = oneshot::channel();
        let entry = Arc::new(Mutex::new(Entry {
            responder: Some(tx),
            receipt: None,
        }));
        self.map.insert((sender, nonce), Arc::downgrade(&entry));
        self.map.set_queue_depth();
        PendingWait {
            rx,
            key: (sender, nonce),
            map: self.map.clone(),
            entry,
        }
    }

    /// Called by the `tx_receipts` watcher when a `Receipt` arrives for a
    /// parked (sender, nonce). If the configured durability gate has
    /// already passed the receipt's B-position, this releases the client
    /// right away. Otherwise it stores the receipt and waits for the
    /// next watermark update.
    pub(crate) async fn on_receipt(&self, sender: Address, nonce: u64, receipt: Receipt) {
        let key = (sender, nonce);
        let Some(entry) = self.map.lookup(&key) else {
            return;
        };
        let mut e = entry.lock().await;
        e.receipt = Some(receipt.clone());
        let latest = self.watermarks();
        if self.gate_satisfied(&latest, receipt.tx_idx) {
            if let Some(resp) = e.responder.take() {
                // This only releases the waiter. The woken waiter's Drop
                // removes the slot.
                let _ = resp.send(Ok(ReceiptResponse { receipt }));
            }
        } else {
            let tx_idx = receipt.tx_idx;
            self.park(e, Arc::downgrade(&entry), tx_idx);
            // This is a check-park-recheck sequence. A watermark tick
            // between the gate check and the insert would have drained a
            // prefix that this entry now belongs to. If that was the
            // burst's final tick, the waiter would hang until client
            // timeout, holding its connection open, which amplifies file
            // descriptor use at the tail of a burst. Re-draining after
            // the park is idempotent, and it closes this window. The
            // re-read is a lock-free `borrow`.
            let latest2 = self.watermarks();
            if self.gate_satisfied(&latest2, tx_idx) {
                self.release_satisfied().await;
            }
        }
    }

    /// Indexes `weak` by `tx_idx`, still gated. Takes the entry's mutex
    /// guard by value, so the guard's scope is this function body: it
    /// ends here, before the caller re-reads the watermark, with no
    /// explicit `drop`.
    fn park(
        &self,
        _entry_guard: tokio::sync::MutexGuard<'_, Entry>,
        weak: Weak<Mutex<Entry>>,
        tx_idx: BPosition,
    ) {
        let seq = self
            .park_seq
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        self.parked.lock_ignore_poison().insert((tx_idx, seq), weak);
    }

    /// Called by the `tx_errors` watcher when a sequencer rejects an inbound
    /// `(sender, nonce)`. Releases the parked client with a JSON-RPC
    /// error mapped from `reason`, but only after the configured grace
    /// period, and only if no receipt has won by then. With racing
    /// sequencer replicas, a rejection from one replica can race a
    /// success from its twin, and the success must override the
    /// rejection; see [`DEFAULT_TX_ERROR_GRACE`]. A receipt that already
    /// arrived, even one still gated on a durability watermark,
    /// suppresses the error outright. Returns silently if no client is
    /// parked for that key, since the error is best-effort.
    pub(crate) async fn on_tx_error(&self, sender: Address, nonce: u64, reason: TxErrorReason) {
        let key = (sender, nonce);
        // The deferred release holds only a Weak across the grace sleep.
        // So a client that disconnects mid-grace lets its entry die
        // right away, instead of a pending-error task keeping it alive.
        let Some(weak) = self.map.lookup_weak(&key) else {
            return;
        };
        let grace = self.error_grace;
        let release = async move {
            if !grace.is_zero() {
                tokio::time::sleep(grace).await;
            }
            let Some(entry) = weak.upgrade() else {
                return; // The client is gone, so there is nothing to release.
            };
            let mut e = entry.lock().await;
            // Success overrides rejection. A stored receipt, whether
            // released or still watermark-gated, means the tx landed on
            // the twin, so this drops the error.
            if e.receipt.is_some() {
                return;
            }
            if let Some(resp) = e.responder.take() {
                let err = match reason {
                    TxErrorReason::DuplicatedTx { .. } => IngressError::Duplicate((sender, nonce)),
                    TxErrorReason::Evicted { .. } => IngressError::Evicted((sender, nonce)),
                };
                // This only releases the waiter. The woken waiter's Drop
                // removes the slot.
                let _ = resp.send(Err(err));
            }
        };
        if grace.is_zero() {
            release.await;
        } else {
            // This defers off this watcher task, so a burst of
            // rejections does not serialize behind each other's grace
            // sleeps.
            tokio::spawn(release);
        }
    }

    /// Called when a new quorum-watermark snapshot arrives. The drain
    /// runs inline, so the release happens on the same tick, with no
    /// scheduler hop.
    pub(crate) async fn update_quorum_watermark(&self, wm: QuorumWatermark) {
        self.quorum.send_replace(Some(wm.position));
        self.release_satisfied().await;
    }

    /// Called when a new local-fsync watermark snapshot arrives, from
    /// the per-recorder stream for the local host.
    pub(crate) async fn update_local_watermark(&self, wm: FsyncWatermark) {
        self.local.send_replace(Some(wm.position));
        self.release_satisfied().await;
    }

    /// The latest watermark pair, read lock-free from the watch slots.
    fn watermarks(&self) -> Watermarks {
        Watermarks {
            quorum: *self.quorum.borrow(),
            local: *self.local.borrow(),
        }
    }

    /// Walks every parked entry and releases the ones whose stored
    /// receipt's B-position is now covered by the configured durability
    /// gate. Like every watcher path, this is a pure reader: it releases
    /// through the oneshot, and leaves slot removal to the woken
    /// waiter's Drop.
    async fn release_satisfied(&self) {
        let latest = self.watermarks();
        // The effective release watermark is the minimum over the
        // watermark kinds the policy requires. If a required watermark
        // is absent, nothing releases.
        let mut effective: Option<BPosition> = None;
        if self.policy.requires_local_fsync() {
            match latest.local {
                Some(p) => effective = Some(p),
                None => return,
            }
        }
        if self.policy.requires_quorum() {
            match latest.quorum {
                Some(p) => {
                    effective = Some(match effective {
                        Some(e) if e <= p => e,
                        _ => p,
                    });
                }
                None => return,
            }
        }
        let Some(eff) = effective else {
            return; // OnOffer never parks.
        };
        // This drains the satisfied prefix: keys with tx_idx <= eff. The
        // seq value never reaches u64::MAX, so this bound is exact.
        let drained: Vec<Weak<Mutex<Entry>>> = {
            let mut parked = self.parked.lock_ignore_poison();
            let suffix = parked.split_off(&(eff, u64::MAX));
            let prefix = std::mem::replace(&mut *parked, suffix);
            prefix.into_values().collect()
        };
        for weak in drained {
            release_one(weak).await;
        }
    }

    /// Returns whether the configured policy is satisfied for `target`,
    /// given the currently observed watermarks. An `OnOffer` policy is
    /// always satisfied.
    fn gate_satisfied(&self, latest: &Watermarks, target: BPosition) -> bool {
        let local_ok =
            !self.policy.requires_local_fsync() || latest.local.is_some_and(|p| p >= target);
        let quorum_ok =
            !self.policy.requires_quorum() || latest.quorum.is_some_and(|p| p >= target);
        local_ok && quorum_ok
    }

    /// Number of registered slots. In practice, this equals the number
    /// of live parks: every drop path reaps its own slot before the
    /// entry dies, so a dead `Weak` never outlives a reader's
    /// opportunistic reap.
    pub(crate) fn len(&self) -> usize {
        self.map.len()
    }
}

/// Resolve one drained-satisfied entry, for [`PendingReceipts::release_satisfied`]'s
/// loop: if the waiter is still there and holds both a receipt and a
/// responder, send the receipt. A waiter that dropped (`upgrade` fails),
/// or already sent (no responder left), is a no-op.
async fn release_one(weak: Weak<Mutex<Entry>>) {
    let Some(entry) = weak.upgrade() else {
        return; // The waiter is gone.
    };
    let mut e = entry.lock().await;
    if let (Some(receipt), Some(resp)) = (e.receipt.clone(), e.responder.take()) {
        let _ = resp.send(Ok(ReceiptResponse { receipt }));
    }
}

/// Handle returned by [`PendingReceipts::register`]. Await it, with a
/// timeout, to receive the published receipt once both the receipt-cache
/// stream and the watermark stream have caught up.
///
/// This handle holds the entry's only long-lived strong `Arc`; the map
/// indexes it through a `Weak`. So dropping the handle kills the entry
/// on every path, and no watcher can mistake it for a parked client
/// afterward, since `upgrade` fails. This is what bounds the registry
/// under client disconnects: jsonrpsee drops the RPC handler future when
/// the connection dies, the future's `PendingWait` drops with it, and the
/// entry dies right there. `Drop` also reaps the map slot, guarded by
/// identity so a replacement registration is untouched, and refreshes
/// the queue-depth gauge.
pub(crate) struct PendingWait {
    rx: oneshot::Receiver<Result<ReceiptResponse, IngressError>>,
    key: (Address, u64),
    map: PendingMap,
    entry: Arc<Mutex<Entry>>,
}

impl PendingWait {
    pub(crate) async fn await_with_timeout(
        mut self,
        timeout: Duration,
    ) -> Result<ReceiptResponse, IngressError> {
        // This uses `&mut self.rx` (oneshot::Receiver is Unpin) instead
        // of consuming the field. `self` must stay whole, so its Drop,
        // the one cleanup path for both timeout and cancellation, runs
        // when this future completes or is dropped mid-await.
        match tokio::time::timeout(timeout, &mut self.rx).await {
            Ok(Ok(res)) => res,
            Ok(Err(_)) => Err(IngressError::Internal("oneshot dropped".into())),
            Err(_) => Err(IngressError::Timeout),
        }
    }
}

impl Drop for PendingWait {
    fn drop(&mut self) {
        // This is the one removal site for map slots. Watchers are pure
        // readers that release only through the oneshot. This runs
        // however the wait ends: by receipt, rejection, timeout, or the
        // RPC handler future being dropped on client disconnect. It is
        // also the only trigger that fires without any further traffic
        // on this key: in the deployed on-offer ack mode, the map is
        // never walked, and an abandoned nonce-gap key never sees
        // another receipt or rejection. The slot is removed before
        // `self.entry`, the only long-lived strong ref, drops right
        // after this body, so a dead `Weak` is never observable in the
        // map. This is guarded by identity, so an old wait's Drop
        // spares a newer registration under the same key. It is
        // synchronous and lock-free apart from the dashmap shard, so it
        // is safe inside an async Drop.
        self.map.remove_slot(&self.key, &self.entry);
    }
}

#[cfg(test)]
mod tests;

//! Pending-receipts map: parks a client `oneshot` until both
//! (a) a `Receipt` for the matching `(sender, nonce)` arrives on the
//!     tx_receipts stream (the executor's enriched receipt carries
//!     `from`+`nonce`+`tx_hash` directly), AND
//! (b) the durability gate selected by [`AckPolicy`] has reached
//!     `receipt.tx_idx`.
//!
//! Both conditions are required by invariant I2.
//!
//! The durability gate is configurable: see [`AckPolicy`] for the four modes.
//! `OnQuorum` (the default) preserves the original behavior — wait for the
//! shared quorum watermark. `OnOffer` skips the watermark wait entirely.
//! `OnLocalFsync` waits on this node's per-recorder fsync stream.
//! `OnLocalFsyncAndQuorum` requires both to have advanced past the position.
//!
//! ## Ownership topology (leak-proof by construction)
//!
//! The WAITER owns each entry: [`PendingWait`] holds the only long-lived
//! strong `Arc`; the map indexes entries through `Weak`. The watcher paths
//! (`on_receipt` / `on_tx_error` / `release_satisfied`) are PURE READERS —
//! they `upgrade()` and treat a dead `Weak` as "no client parked"; they
//! never remove anything. The wait's `Drop` is the SINGLE removal site: it
//! reaps the slot (identity-guarded) before the entry Arc dies, so however
//! the wait ends — receipt, rejection, timeout, or the RPC handler future
//! being dropped on client disconnect — slot and entry go together, and a
//! dead `Weak` is never observable in the map. No removal call has to be
//! remembered anywhere else (the #81 "pending-registry cleanup on cancelled
//! RPC futures" follow-up leaked exactly because cleanup was a discipline
//! spread across paths).

use std::sync::{Arc, Weak};
use std::time::Duration;

use alloy_primitives::Address;
use dashmap::DashMap;
use tokio::sync::{Mutex, oneshot};

use kardamom_types::{
    AckPolicy, BPosition, FsyncWatermark, QuorumWatermark, Receipt, TxErrorReason,
};

use crate::error::IngressError;

#[derive(Debug, Clone)]
pub struct ReceiptResponse {
    pub receipt: Receipt,
}

/// Internal entry: a parked oneshot sender, plus the receipt once it has
/// arrived.
struct Entry {
    responder: Option<oneshot::Sender<Result<ReceiptResponse, IngressError>>>,
    receipt: Option<Receipt>,
}

/// The registry's index. Values are `Weak`: the map can find an entry but
/// never keeps one alive — the strong ref lives in the `PendingWait` the
/// submitting handler holds (see the module docs' ownership topology).
type PendingMap = Arc<DashMap<(Address, u64), Weak<Mutex<Entry>>>>;

/// Look up the live entry for `key`. A dead `Weak` (unobservable in
/// practice — see the module docs) is indistinguishable from an absent key:
/// the parked client is gone either way.
fn lookup(map: &PendingMap, key: &(Address, u64)) -> Option<Arc<Mutex<Entry>>> {
    map.get(key).and_then(|r| r.value().upgrade())
}

/// The single removal path, called from `PendingWait::drop`: remove `key`'s
/// slot ONLY if it still indexes `entry` (pointer identity), then refresh
/// the depth gauge. The guard matters because a re-`register` of the same
/// (sender, nonce) replaces the slot with a NEW entry, and the OLD wait's
/// later Drop must never evict the new registration.
fn remove_slot(map: &PendingMap, key: &(Address, u64), entry: &Arc<Mutex<Entry>>) {
    map.remove_if(key, |_, w| std::ptr::eq(w.as_ptr(), Arc::as_ptr(entry)));
    set_queue_depth(map);
}

/// The registry owns the queue-depth gauge: depth changes exactly when an
/// entry is inserted or removed, including removals on paths no proxy code
/// runs (a cancelled handler's `PendingWait::drop`).
fn set_queue_depth(map: &PendingMap) {
    metrics::gauge!(crate::metrics::QUEUE_DEPTH).set(map.len() as f64);
}

/// Tracked watermarks. Whichever fields the policy doesn't need remain `None`
/// forever and the gate skips them.
#[derive(Default, Clone, Copy)]
struct Watermarks {
    quorum: Option<BPosition>,
    local: Option<BPosition>,
}

/// How long a sequencer rejection is held before releasing the parked client
/// with an error, giving a racing SUCCESS the chance to win. With P racing
/// sequencer replicas, a replica with a momentarily stale nonce floor can
/// reject a tx its twin accepted and ordered; the rejection is emitted at
/// ordering time while the receipt only lands after execution, so the error
/// usually arrives FIRST. The grace must exceed the ordering→execution→receipt
/// latency (tens of ms in the cluster); genuine rejections (both replicas
/// reject) are merely delayed by this long, which a client submitting a
/// duplicate can easily afford.
pub const DEFAULT_TX_ERROR_GRACE: Duration = Duration::from_millis(500);

pub struct PendingReceipts {
    policy: AckPolicy,
    map: PendingMap,
    /// Latest watermarks observed. Cached to avoid one-receiver-per-await
    /// fanout.
    latest: Arc<Mutex<Watermarks>>,
    /// See [`DEFAULT_TX_ERROR_GRACE`]; overridable for tests.
    error_grace: Duration,
    /// Watermark-ORDERED index of parked entries: `(tx_idx, seq) → Weak`.
    /// A watermark tick used to snapshot and walk the ENTIRE registry
    /// (O(parked) allocations per tick — the ingress profile's top
    /// allocation site); draining the satisfied PREFIX of this index makes
    /// a tick O(released + log parked) with zero steady-state allocation.
    /// Entries are inserted only when a receipt arrives still-gated; a
    /// dropped waiter's Weak simply fails to upgrade at drain.
    parked: std::sync::Mutex<ParkedIndex>,
    park_seq: std::sync::atomic::AtomicU64,
}

/// `(tx_idx, insertion seq) → parked entry`, ordered by watermark.
type ParkedIndex = std::collections::BTreeMap<(BPosition, u64), Weak<Mutex<Entry>>>;

impl Default for PendingReceipts {
    fn default() -> Self {
        Self::new(AckPolicy::default())
    }
}

impl PendingReceipts {
    pub fn new(policy: AckPolicy) -> Self {
        Self::with_error_grace(policy, DEFAULT_TX_ERROR_GRACE)
    }

    /// Like [`new`](Self::new) with an explicit rejection-release grace
    /// (`Duration::ZERO` releases errors inline, with no success-override
    /// window).
    pub fn with_error_grace(policy: AckPolicy, error_grace: Duration) -> Self {
        Self {
            policy,
            map: Arc::new(DashMap::new()),
            latest: Arc::new(Mutex::new(Watermarks::default())),
            error_grace,
            parked: std::sync::Mutex::new(std::collections::BTreeMap::new()),
            park_seq: std::sync::atomic::AtomicU64::new(0),
        }
    }

    /// Two-phase register: returns a `PendingWait` the caller awaits with a
    /// timeout. Calling code must `register` *before* publishing the tx so
    /// the receipt cannot beat the registration.
    ///
    /// The returned `PendingWait` holds the entry's ONLY strong ref (the map
    /// gets a `Weak`): however the wait ends — receipt, rejection, timeout,
    /// or the CALLER'S FUTURE BEING DROPPED (client disconnect cancelling
    /// the RPC handler) — the entry dies with it and its slot is reaped.
    /// The cancelled-future path used to leak the entry forever (the #81
    /// "pending-registry cleanup on cancelled RPC futures" follow-up).
    pub fn register(&self, sender: Address, nonce: u64) -> PendingWait {
        let (tx, rx) = oneshot::channel();
        let entry = Arc::new(Mutex::new(Entry {
            responder: Some(tx),
            receipt: None,
        }));
        self.map.insert((sender, nonce), Arc::downgrade(&entry));
        set_queue_depth(&self.map);
        PendingWait {
            rx,
            key: (sender, nonce),
            map: self.map.clone(),
            entry,
        }
    }

    /// Called by the tx_receipts watcher when a `Receipt` arrives for a
    /// parked (sender, nonce). If the configured durability gate has already
    /// advanced past the receipt's B-position, releases the client
    /// immediately; otherwise stores the receipt and waits for the next
    /// watermark update.
    pub async fn on_receipt(&self, sender: Address, nonce: u64, receipt: Receipt) {
        let key = (sender, nonce);
        let Some(entry) = lookup(&self.map, &key) else {
            return;
        };
        let mut e = entry.lock().await;
        e.receipt = Some(receipt.clone());
        let latest = *self.latest.lock().await;
        if self.gate_satisfied(&latest, receipt.tx_idx) {
            if let Some(resp) = e.responder.take() {
                // Release only; the woken waiter's Drop removes the slot.
                let _ = resp.send(Ok(ReceiptResponse { receipt }));
            }
        } else {
            // Still gated: index by position so the watermark tick drains
            // exactly the satisfied prefix.
            let seq = self
                .park_seq
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            let tx_idx = receipt.tx_idx;
            self.parked
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .insert((tx_idx, seq), Arc::downgrade(&entry));
            drop(e);
            // CHECK-PARK-RECHECK: a watermark tick between the gate check
            // and the insert would have drained a prefix this entry now
            // belongs to — and if that was the burst's FINAL tick, the
            // waiter would hang until client timeout, holding its
            // connection (an fd amplifier at burst tails). Re-draining
            // after the park is idempotent and closes the window.
            let latest2 = *self.latest.lock().await;
            if self.gate_satisfied(&latest2, tx_idx) {
                self.release_satisfied().await;
            }
        }
    }

    /// Called by the tx_errors watcher when a sequencer rejected an inbound
    /// `(sender, nonce)`. Releases the parked client with a JSON-RPC error
    /// mapped from `reason` — but only after the configured grace, and only if
    /// no receipt has won by then: with racing sequencer replicas a rejection
    /// from one replica can race a SUCCESS from its twin, and the success must
    /// override the rejection (see [`DEFAULT_TX_ERROR_GRACE`]). A receipt that
    /// already arrived (even one still gated on a durability watermark)
    /// suppresses the error outright. Returns silently if no client is parked
    /// for that key (the error is best-effort).
    pub async fn on_tx_error(&self, sender: Address, nonce: u64, reason: TxErrorReason) {
        let key = (sender, nonce);
        let Some(entry) = lookup(&self.map, &key) else {
            return;
        };
        // The deferred release holds only a Weak across the grace sleep: a
        // client that disconnects mid-grace lets its entry die immediately
        // instead of being kept alive by a pending-error task.
        let weak = Arc::downgrade(&entry);
        drop(entry);
        let grace = self.error_grace;
        let release = async move {
            if !grace.is_zero() {
                tokio::time::sleep(grace).await;
            }
            let Some(entry) = weak.upgrade() else {
                return; // client gone; nothing to release
            };
            let mut e = entry.lock().await;
            // Success overrides rejection: a stored receipt (released or still
            // watermark-gated) means the tx landed on the twin — drop the error.
            if e.receipt.is_some() {
                return;
            }
            if let Some(resp) = e.responder.take() {
                let err = match reason {
                    TxErrorReason::DuplicatedTx { .. } => IngressError::Duplicate((sender, nonce)),
                    TxErrorReason::Evicted { .. } => IngressError::Evicted((sender, nonce)),
                };
                // Release only; the woken waiter's Drop removes the slot.
                let _ = resp.send(Err(err));
            }
        };
        if grace.is_zero() {
            release.await;
        } else {
            // Defer off this watcher task so a burst of rejections doesn't
            // serialize behind each other's grace sleeps.
            tokio::spawn(release);
        }
    }

    /// Called when a new quorum-watermark snapshot is observed.
    pub async fn update_quorum_watermark(&self, wm: QuorumWatermark) {
        self.latest.lock().await.quorum = Some(wm.position);
        self.release_satisfied().await;
    }

    /// Called when a new local-fsync watermark snapshot is observed (from
    /// the per-recorder stream for the local host).
    pub async fn update_local_watermark(&self, wm: FsyncWatermark) {
        self.latest.lock().await.local = Some(wm.position);
        self.release_satisfied().await;
    }

    /// Walk every parked entry and release the ones whose stored receipt's
    /// B-position is now covered by the configured durability gate. A pure
    /// reader like every watcher path: it releases through the oneshot and
    /// leaves slot removal to the woken waiter's Drop.
    async fn release_satisfied(&self) {
        let latest = *self.latest.lock().await;
        // Effective release watermark: the MIN over the watermark kinds the
        // policy requires. Absent required watermark ⇒ nothing releases.
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
                    })
                }
                None => return,
            }
        }
        let Some(eff) = effective else {
            return; // OnOffer never parks
        };
        // Drain the satisfied prefix: keys with tx_idx <= eff (seq never
        // reaches u64::MAX, so this bound is exact).
        let drained: Vec<Weak<Mutex<Entry>>> = {
            let mut parked = self
                .parked
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            let suffix = parked.split_off(&(eff, u64::MAX));
            let prefix = std::mem::replace(&mut *parked, suffix);
            prefix.into_values().collect()
        };
        for weak in drained {
            let Some(entry) = weak.upgrade() else {
                continue; // waiter gone
            };
            let mut e = entry.lock().await;
            if let (Some(receipt), Some(resp)) = (e.receipt.clone(), e.responder.take()) {
                let _ = resp.send(Ok(ReceiptResponse { receipt }));
            }
        }
    }

    /// Whether the configured policy is satisfied for `target` given the
    /// currently observed watermarks. An `OnOffer` policy is always satisfied.
    fn gate_satisfied(&self, latest: &Watermarks, target: BPosition) -> bool {
        let local_ok =
            !self.policy.requires_local_fsync() || latest.local.is_some_and(|p| p >= target);
        let quorum_ok =
            !self.policy.requires_quorum() || latest.quorum.is_some_and(|p| p >= target);
        local_ok && quorum_ok
    }

    /// Number of registered slots. Equals the number of live parks in
    /// practice: every drop path reaps its own slot before the entry dies,
    /// so dead `Weak`s never persist beyond a reader's opportunistic reap.
    pub fn len(&self) -> usize {
        self.map.len()
    }

    pub fn is_empty(&self) -> bool {
        self.map.is_empty()
    }
}

/// Handle returned by [`PendingReceipts::register`]. Await it (with a
/// timeout) to receive the published receipt once both the receipt-cache
/// stream and the watermark stream have caught up.
///
/// This handle holds the entry's ONLY long-lived strong `Arc` — the map
/// indexes it through a `Weak`. Dropping the handle therefore kills the
/// entry on every path, and no watcher can mistake it for a parked client
/// afterwards (upgrade fails). This is what bounds the registry under client
/// disconnects: jsonrpsee drops the RPC handler future when the connection
/// dies, the future's `PendingWait` drops with it, and the entry dies right
/// there instead of leaking until process restart (#81 follow-up). `Drop`
/// also reaps the map slot (identity-guarded, so a replacement registration
/// is untouched) and refreshes the queue-depth gauge.
pub struct PendingWait {
    rx: oneshot::Receiver<Result<ReceiptResponse, IngressError>>,
    key: (Address, u64),
    map: PendingMap,
    entry: Arc<Mutex<Entry>>,
}

impl PendingWait {
    pub async fn await_with_timeout(
        mut self,
        timeout: Duration,
    ) -> Result<ReceiptResponse, IngressError> {
        // `&mut self.rx` (oneshot::Receiver is Unpin) rather than consuming
        // the field: `self` must stay whole so its Drop — the single cleanup
        // path for timeout AND cancellation — runs when this future
        // completes or is dropped mid-await.
        match tokio::time::timeout(timeout, &mut self.rx).await {
            Ok(Ok(res)) => res,
            Ok(Err(_)) => Err(IngressError::Internal("oneshot dropped".into())),
            Err(_) => Err(IngressError::Timeout),
        }
    }
}

impl Drop for PendingWait {
    fn drop(&mut self) {
        // THE single removal site for map slots (watchers are pure readers
        // that release through the oneshot only). Runs however the wait ends
        // — receipt, rejection, timeout, or the RPC handler future being
        // dropped on client disconnect — and, crucially, it is the only
        // trigger that fires WITHOUT any further traffic on this key: in the
        // deployed on-offer ack mode the map is never walked, and an
        // abandoned nonce-gap key never sees another receipt or rejection.
        // The slot is removed BEFORE `self.entry` (the only long-lived
        // strong ref) drops right after this body, so a dead Weak is never
        // observable in the map. Identity-guarded, so an old wait's Drop
        // spares a newer registration under the same key. Sync + lock-free
        // apart from the dashmap shard, so it is safe in an async Drop.
        remove_slot(&self.map, &self.key, &self.entry);
    }
}

#[cfg(test)]
mod tests;

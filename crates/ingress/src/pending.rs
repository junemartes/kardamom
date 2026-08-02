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
    parked: std::sync::Mutex<std::collections::BTreeMap<(BPosition, u64), Weak<Mutex<Entry>>>>,
    park_seq: std::sync::atomic::AtomicU64,
}

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
mod tests {
    use super::*;
    use alloy_primitives::B256;

    fn dummy_receipt(pos: BPosition) -> Receipt {
        Receipt {
            tx_idx: pos,
            tx_hash: B256::ZERO,
            status: true,
            gas_used: 21_000,
            logs: Vec::new(),
            write_set_hash: B256::ZERO,
            ..Default::default()
        }
    }

    fn pos(offset: i32) -> BPosition {
        BPosition {
            term_id: 0,
            term_offset: offset,
        }
    }

    // --- OnQuorum (default, the original behavior) -----------------------

    #[tokio::test]
    async fn quorum_parks_until_receipt_and_watermark_both_arrive() {
        let p = Arc::new(PendingReceipts::new(AckPolicy::OnQuorum));
        let sender = Address::repeat_byte(0x11);
        let nonce = 7u64;
        let position = pos(100);

        let wait = p.register(sender, nonce);
        let waiter =
            tokio::spawn(async move { wait.await_with_timeout(Duration::from_secs(5)).await });
        tokio::time::sleep(Duration::from_millis(10)).await;

        p.on_receipt(sender, nonce, dummy_receipt(position)).await;
        tokio::time::sleep(Duration::from_millis(10)).await;
        assert_eq!(p.len(), 1, "must not release before quorum catches up");

        p.update_quorum_watermark(QuorumWatermark { position })
            .await;
        let res = waiter.await.unwrap().unwrap();
        assert_eq!(res.receipt.tx_idx, position);
        assert_eq!(p.len(), 0);
    }

    #[tokio::test]
    async fn tx_error_releases_parked_client_with_duplicate() {
        let p = Arc::new(PendingReceipts::new(AckPolicy::OnQuorum));
        let sender = Address::repeat_byte(0x55);
        let nonce = 3u64;

        let wait = p.register(sender, nonce);
        let waiter =
            tokio::spawn(async move { wait.await_with_timeout(Duration::from_secs(5)).await });
        tokio::time::sleep(Duration::from_millis(10)).await;

        p.on_tx_error(
            sender,
            nonce,
            TxErrorReason::DuplicatedTx { expected_nonce: 9 },
        )
        .await;

        let err = waiter
            .await
            .expect("join")
            .expect_err("on_tx_error must release with Err");
        assert!(
            matches!(err, IngressError::Duplicate((s, n)) if s == sender && n == nonce),
            "got {err:?}"
        );
        assert_eq!(p.len(), 0, "entry removed on release");
    }

    #[tokio::test]
    async fn eviction_releases_the_parked_wait_with_an_evicted_error() {
        // The wedge fix: a sequencer overload-shed (Evicted) must error the
        // parked submit instead of leaving it waiting for a receipt that can
        // never arrive (the pre-fix silent evict permanently gapped the
        // sender and pinned the connection until timeout).
        let p = Arc::new(PendingReceipts::new(AckPolicy::OnOffer));
        let sender = Address::repeat_byte(0x77);
        let nonce = 21u64;

        let wait = p.register(sender, nonce);
        let waiter =
            tokio::spawn(async move { wait.await_with_timeout(Duration::from_secs(5)).await });
        tokio::time::sleep(Duration::from_millis(10)).await;

        p.on_tx_error(sender, nonce, TxErrorReason::Evicted { expected_nonce: 18 })
            .await;

        let res = waiter.await.expect("join");
        match res {
            Err(IngressError::Evicted((s, n))) => {
                assert_eq!((s, n), (sender, nonce));
            }
            other => panic!("expected Evicted release, got {other:?}"),
        }
        assert_eq!(p.len(), 0, "entry removed on release");
    }

    #[tokio::test]
    async fn success_arriving_within_the_grace_overrides_a_rejection() {
        // F02.6: with racing sequencer replicas, replica A's DuplicatedTx can
        // arrive BEFORE replica B's receipt for the same tx. The rejection is
        // held for the grace window; the receipt must win.
        let p = Arc::new(PendingReceipts::with_error_grace(
            AckPolicy::OnOffer,
            Duration::from_millis(200),
        ));
        let sender = Address::repeat_byte(0x88);
        let nonce = 5u64;
        let position = pos(77);

        let wait = p.register(sender, nonce);
        let waiter =
            tokio::spawn(async move { wait.await_with_timeout(Duration::from_secs(5)).await });
        tokio::time::sleep(Duration::from_millis(10)).await;

        // Rejection first (replica A, stale floor)...
        p.on_tx_error(
            sender,
            nonce,
            TxErrorReason::DuplicatedTx { expected_nonce: 6 },
        )
        .await;
        // ...then the twin's success, well inside the grace window.
        tokio::time::sleep(Duration::from_millis(20)).await;
        p.on_receipt(sender, nonce, dummy_receipt(position)).await;

        let res = waiter
            .await
            .expect("join")
            .expect("success must override the racing rejection");
        assert_eq!(res.receipt.tx_idx, position);
        assert_eq!(p.len(), 0);
    }

    #[tokio::test]
    async fn rejection_releases_after_the_grace_when_no_success_arrives() {
        // A genuine duplicate (both replicas reject, no receipt ever) still
        // reaches the client — just delayed by the grace window.
        let p = Arc::new(PendingReceipts::with_error_grace(
            AckPolicy::OnOffer,
            Duration::from_millis(50),
        ));
        let sender = Address::repeat_byte(0x99);
        let nonce = 2u64;

        let wait = p.register(sender, nonce);
        let waiter =
            tokio::spawn(async move { wait.await_with_timeout(Duration::from_secs(5)).await });
        tokio::time::sleep(Duration::from_millis(10)).await;

        p.on_tx_error(
            sender,
            nonce,
            TxErrorReason::DuplicatedTx { expected_nonce: 9 },
        )
        .await;
        let err = waiter
            .await
            .expect("join")
            .expect_err("no success arrived — the rejection must be released");
        assert!(
            matches!(err, IngressError::Duplicate((s, n)) if s == sender && n == nonce),
            "got {err:?}"
        );
        assert_eq!(p.len(), 0);
    }

    #[tokio::test]
    async fn watermark_gated_receipt_suppresses_a_rejection() {
        // A receipt that arrived but is still parked on the durability gate
        // already proves the tx landed — a late rejection must not evict it.
        let p = Arc::new(PendingReceipts::with_error_grace(
            AckPolicy::OnQuorum,
            Duration::ZERO, // inline release path; the stored receipt must gate it
        ));
        let sender = Address::repeat_byte(0xAA);
        let nonce = 1u64;
        let position = pos(9);

        let wait = p.register(sender, nonce);
        let waiter =
            tokio::spawn(async move { wait.await_with_timeout(Duration::from_secs(5)).await });
        tokio::time::sleep(Duration::from_millis(10)).await;

        // Receipt arrives but quorum hasn't caught up — stored, not released.
        p.on_receipt(sender, nonce, dummy_receipt(position)).await;
        // Twin's rejection must be suppressed by the stored receipt.
        p.on_tx_error(
            sender,
            nonce,
            TxErrorReason::DuplicatedTx { expected_nonce: 2 },
        )
        .await;
        tokio::time::sleep(Duration::from_millis(10)).await;
        assert_eq!(p.len(), 1, "entry must survive the rejection");

        // Quorum catches up → the success is delivered.
        p.update_quorum_watermark(QuorumWatermark { position })
            .await;
        let res = waiter.await.expect("join").expect("receipt must win");
        assert_eq!(res.receipt.tx_idx, position);
    }

    #[tokio::test]
    async fn tx_error_for_unparked_key_is_noop() {
        // No client parked for this (sender, nonce) → on_tx_error returns
        // silently without panicking.
        let p = Arc::new(PendingReceipts::new(AckPolicy::OnQuorum));
        p.on_tx_error(
            Address::repeat_byte(0x77),
            42,
            TxErrorReason::DuplicatedTx {
                expected_nonce: 100,
            },
        )
        .await;
        assert_eq!(p.len(), 0);
    }

    #[tokio::test]
    async fn quorum_releases_immediately_when_watermark_already_past() {
        let p = Arc::new(PendingReceipts::new(AckPolicy::OnQuorum));
        let sender = Address::repeat_byte(0x22);
        let position = pos(5);
        p.update_quorum_watermark(QuorumWatermark {
            position: pos(1000),
        })
        .await;

        let wait = p.register(sender, 1);
        let waiter =
            tokio::spawn(async move { wait.await_with_timeout(Duration::from_secs(5)).await });
        tokio::time::sleep(Duration::from_millis(10)).await;
        p.on_receipt(sender, 1, dummy_receipt(position)).await;
        let res = waiter.await.unwrap().unwrap();
        assert_eq!(res.receipt.tx_idx, position);
    }

    #[tokio::test]
    async fn quorum_does_not_release_on_local_watermark_alone() {
        let p = Arc::new(PendingReceipts::new(AckPolicy::OnQuorum));
        let sender = Address::repeat_byte(0x33);
        let position = pos(50);

        let wait = p.register(sender, 0);
        let waiter =
            tokio::spawn(async move { wait.await_with_timeout(Duration::from_millis(50)).await });
        tokio::time::sleep(Duration::from_millis(5)).await;

        p.on_receipt(sender, 0, dummy_receipt(position)).await;
        p.update_local_watermark(FsyncWatermark {
            recorder_id: 0,
            position,
        })
        .await;

        // Local advanced but quorum didn't — must time out.
        let err = waiter.await.unwrap().unwrap_err();
        assert!(matches!(err, IngressError::Timeout));
    }

    // --- OnOffer (no durability gate) ------------------------------------

    #[tokio::test]
    async fn on_offer_releases_as_soon_as_receipt_arrives() {
        let p = Arc::new(PendingReceipts::new(AckPolicy::OnOffer));
        let sender = Address::repeat_byte(0x44);
        let position = pos(1);

        let wait = p.register(sender, 0);
        let waiter =
            tokio::spawn(async move { wait.await_with_timeout(Duration::from_secs(5)).await });
        tokio::time::sleep(Duration::from_millis(10)).await;

        // No watermark updates at all — must still release on receipt.
        p.on_receipt(sender, 0, dummy_receipt(position)).await;
        let res = waiter.await.unwrap().unwrap();
        assert_eq!(res.receipt.tx_idx, position);
    }

    // --- OnLocalFsync (local-only, ignores quorum) -----------------------

    #[tokio::test]
    async fn local_releases_on_local_watermark_only() {
        let p = Arc::new(PendingReceipts::new(AckPolicy::OnLocalFsync));
        let sender = Address::repeat_byte(0x55);
        let position = pos(20);

        let wait = p.register(sender, 0);
        let waiter =
            tokio::spawn(async move { wait.await_with_timeout(Duration::from_secs(5)).await });
        tokio::time::sleep(Duration::from_millis(10)).await;

        p.on_receipt(sender, 0, dummy_receipt(position)).await;
        // Quorum advances but policy ignores it — still parked.
        p.update_quorum_watermark(QuorumWatermark { position })
            .await;
        tokio::time::sleep(Duration::from_millis(10)).await;
        assert_eq!(p.len(), 1);

        // Local advances → releases.
        p.update_local_watermark(FsyncWatermark {
            recorder_id: 0,
            position,
        })
        .await;
        let res = waiter.await.unwrap().unwrap();
        assert_eq!(res.receipt.tx_idx, position);
    }

    // --- OnLocalFsyncAndQuorum (both required) ---------------------------

    #[tokio::test]
    async fn both_requires_local_and_quorum_to_advance() {
        let p = Arc::new(PendingReceipts::new(AckPolicy::OnLocalFsyncAndQuorum));
        let sender = Address::repeat_byte(0x66);
        let position = pos(33);

        let wait = p.register(sender, 0);
        let waiter =
            tokio::spawn(async move { wait.await_with_timeout(Duration::from_secs(5)).await });
        tokio::time::sleep(Duration::from_millis(10)).await;

        p.on_receipt(sender, 0, dummy_receipt(position)).await;
        // Only local — still parked.
        p.update_local_watermark(FsyncWatermark {
            recorder_id: 0,
            position,
        })
        .await;
        tokio::time::sleep(Duration::from_millis(10)).await;
        assert_eq!(p.len(), 1);

        // Now quorum too → releases.
        p.update_quorum_watermark(QuorumWatermark { position })
            .await;
        let res = waiter.await.unwrap().unwrap();
        assert_eq!(res.receipt.tx_idx, position);
    }

    // --- Timeout still works regardless of policy ------------------------

    #[tokio::test]
    async fn times_out_when_neither_event_arrives() {
        let p = PendingReceipts::default();
        let wait = p.register(Address::ZERO, 0);
        let err = wait
            .await_with_timeout(Duration::from_millis(20))
            .await
            .unwrap_err();
        assert!(matches!(err, IngressError::Timeout));
        assert_eq!(p.len(), 0);
    }

    // --- Cancelled-future cleanup (the #81 follow-up) ---------------------

    #[tokio::test]
    async fn dropping_an_unresolved_wait_removes_its_entry() {
        // The core leak: a client disconnect drops the RPC handler future,
        // which drops the PendingWait without any of the release paths
        // running. The entry must leave the map right there.
        let p = PendingReceipts::new(AckPolicy::OnOffer);
        let wait = p.register(Address::repeat_byte(0xB1), 4);
        assert_eq!(p.len(), 1);
        drop(wait);
        assert_eq!(p.len(), 0, "dropped wait must unregister its entry");
    }

    #[tokio::test]
    async fn aborting_a_parked_await_cleans_up() {
        // Same, through the real shape: the wait is parked inside
        // await_with_timeout when the owning task is aborted (the future is
        // dropped mid-await, exactly what jsonrpsee does on disconnect).
        let p = Arc::new(PendingReceipts::new(AckPolicy::OnOffer));
        let wait = p.register(Address::repeat_byte(0xB2), 7);
        let task =
            tokio::spawn(async move { wait.await_with_timeout(Duration::from_secs(60)).await });
        tokio::time::sleep(Duration::from_millis(20)).await;
        assert_eq!(p.len(), 1, "parked");
        task.abort();
        let _ = task.await;
        assert_eq!(p.len(), 0, "abort must unregister the parked entry");
    }

    #[tokio::test]
    async fn stale_wait_drop_spares_a_replacement_registration() {
        // (sender, nonce) re-registered while an old wait still exists: the
        // old wait's Drop is identity-guarded and must NOT evict the new
        // registration.
        let p = PendingReceipts::new(AckPolicy::OnOffer);
        let sender = Address::repeat_byte(0xB3);
        let stale = p.register(sender, 1);
        let fresh = p.register(sender, 1); // replaces the slot
        assert_eq!(p.len(), 1);
        drop(stale);
        assert_eq!(p.len(), 1, "stale drop must spare the replacement");
        drop(fresh);
        assert_eq!(p.len(), 0);
    }

    #[tokio::test]
    async fn a_dead_slot_reads_as_absent() {
        // Structurally unreachable through the public API (Drop removes the
        // slot BEFORE its entry dies) — simulated directly to pin the reader
        // contract anyway: a dead Weak means "no client parked", never a
        // panic or a release.
        let p = PendingReceipts::new(AckPolicy::OnOffer);
        let sender = Address::repeat_byte(0xB5);
        p.map.insert((sender, 3), Weak::new());
        p.on_receipt(sender, 3, dummy_receipt(pos(1))).await;
        p.on_tx_error(sender, 3, TxErrorReason::DuplicatedTx { expected_nonce: 9 })
            .await;
    }

    #[tokio::test]
    async fn grace_task_does_not_keep_a_disconnected_entry_alive() {
        // The deferred rejection-release holds only a Weak across its grace
        // sleep: a client that disconnects mid-grace must free its entry
        // immediately, not after the grace fires.
        let p = Arc::new(PendingReceipts::with_error_grace(
            AckPolicy::OnOffer,
            Duration::from_millis(100),
        ));
        let sender = Address::repeat_byte(0xB6);
        let wait = p.register(sender, 6);
        let weak = Arc::downgrade(&wait.entry);
        p.on_tx_error(sender, 6, TxErrorReason::DuplicatedTx { expected_nonce: 9 })
            .await;
        drop(wait); // disconnect mid-grace
        assert_eq!(
            weak.strong_count(),
            0,
            "the pending grace task must not keep the entry alive"
        );
        assert_eq!(p.len(), 0);
        // Let the grace fire against the dead weak: must be a clean no-op.
        tokio::time::sleep(Duration::from_millis(150)).await;
        assert_eq!(p.len(), 0);
    }

    #[tokio::test]
    async fn replacement_registration_still_releases_on_receipt() {
        // End-to-end on the replacement: after a stale wait is dropped, the
        // fresh registration still parks and releases normally.
        let p = Arc::new(PendingReceipts::new(AckPolicy::OnOffer));
        let sender = Address::repeat_byte(0xB4);
        let position = pos(12);
        let stale = p.register(sender, 0);
        let fresh = p.register(sender, 0);
        drop(stale);
        let waiter =
            tokio::spawn(async move { fresh.await_with_timeout(Duration::from_secs(5)).await });
        tokio::time::sleep(Duration::from_millis(10)).await;
        p.on_receipt(sender, 0, dummy_receipt(position)).await;
        let res = waiter.await.unwrap().expect("fresh wait must release");
        assert_eq!(res.receipt.tx_idx, position);
        assert_eq!(p.len(), 0);
    }
}

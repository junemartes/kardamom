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

use std::sync::Arc;
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

/// Map shard used by both `PendingReceipts` and the `PendingWait` it hands
/// out. Aliased to keep clippy's type-complexity lint happy and to make the
/// "shared between caller and consumer" property obvious.
type PendingMap = Arc<DashMap<(Address, u64), Arc<Mutex<Entry>>>>;

/// Tracked watermarks. Whichever fields the policy doesn't need remain `None`
/// forever and the gate skips them.
#[derive(Default, Clone, Copy)]
struct Watermarks {
    quorum: Option<BPosition>,
    local: Option<BPosition>,
}

pub struct PendingReceipts {
    policy: AckPolicy,
    map: PendingMap,
    /// Latest watermarks observed. Cached to avoid one-receiver-per-await
    /// fanout.
    latest: Arc<Mutex<Watermarks>>,
}

impl Default for PendingReceipts {
    fn default() -> Self {
        Self::new(AckPolicy::default())
    }
}

impl PendingReceipts {
    pub fn new(policy: AckPolicy) -> Self {
        Self {
            policy,
            map: Arc::new(DashMap::new()),
            latest: Arc::new(Mutex::new(Watermarks::default())),
        }
    }

    /// Two-phase register: returns a `PendingWait` the caller awaits with a
    /// timeout. Calling code must `register` *before* publishing the tx so
    /// the receipt cannot beat the registration.
    pub fn register(&self, sender: Address, nonce: u64) -> PendingWait {
        let (tx, rx) = oneshot::channel();
        self.map.insert(
            (sender, nonce),
            Arc::new(Mutex::new(Entry {
                responder: Some(tx),
                receipt: None,
            })),
        );
        PendingWait {
            rx,
            key: (sender, nonce),
            map: self.map.clone(),
        }
    }

    /// Called by the tx_receipts watcher when a `Receipt` arrives for a
    /// parked (sender, nonce). If the configured durability gate has already
    /// advanced past the receipt's B-position, releases the client
    /// immediately; otherwise stores the receipt and waits for the next
    /// watermark update.
    pub async fn on_receipt(&self, sender: Address, nonce: u64, receipt: Receipt) {
        let key = (sender, nonce);
        let entry = {
            let Some(r) = self.map.get(&key) else {
                return;
            };
            r.value().clone()
        };
        let mut e = entry.lock().await;
        e.receipt = Some(receipt.clone());
        let latest = *self.latest.lock().await;
        if self.gate_satisfied(&latest, receipt.tx_idx)
            && let Some(resp) = e.responder.take()
        {
            let _ = resp.send(Ok(ReceiptResponse { receipt }));
            drop(e);
            self.map.remove(&key);
        }
    }

    /// Called by the tx_errors watcher when the sequencer rejected an
    /// inbound `(sender, nonce)`. Releases the parked client immediately
    /// with a JSON-RPC error mapped from `reason`. Returns silently if no
    /// client is parked for that key (the error is best-effort).
    pub async fn on_tx_error(&self, sender: Address, nonce: u64, reason: TxErrorReason) {
        let key = (sender, nonce);
        let entry = {
            let Some(r) = self.map.get(&key) else {
                return;
            };
            r.value().clone()
        };
        let mut e = entry.lock().await;
        if let Some(resp) = e.responder.take() {
            let err = match reason {
                TxErrorReason::DuplicatedTx { .. } => IngressError::Duplicate((sender, nonce)),
            };
            let _ = resp.send(Err(err));
            drop(e);
            self.map.remove(&key);
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
    /// B-position is now covered by the configured durability gate.
    async fn release_satisfied(&self) {
        let latest = *self.latest.lock().await;
        type Snap = Vec<((Address, u64), Arc<Mutex<Entry>>)>;
        let snapshot: Snap = self
            .map
            .iter()
            .map(|r| (*r.key(), r.value().clone()))
            .collect();
        let mut to_release: Vec<(Address, u64)> = Vec::new();
        for (key, entry) in snapshot {
            let mut e = entry.lock().await;
            let release = e
                .receipt
                .as_ref()
                .map(|r| self.gate_satisfied(&latest, r.tx_idx))
                .unwrap_or(false);
            if release && let Some(resp) = e.responder.take() {
                let receipt = e.receipt.clone().expect("checked is_some above");
                let _ = resp.send(Ok(ReceiptResponse { receipt }));
                to_release.push(key);
            }
        }
        for k in to_release {
            self.map.remove(&k);
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
pub struct PendingWait {
    rx: oneshot::Receiver<Result<ReceiptResponse, IngressError>>,
    key: (Address, u64),
    map: PendingMap,
}

impl PendingWait {
    pub async fn await_with_timeout(
        self,
        timeout: Duration,
    ) -> Result<ReceiptResponse, IngressError> {
        match tokio::time::timeout(timeout, self.rx).await {
            Ok(Ok(res)) => res,
            Ok(Err(_)) => Err(IngressError::Internal("oneshot dropped".into())),
            Err(_) => {
                self.map.remove(&self.key);
                Err(IngressError::Timeout)
            }
        }
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
}

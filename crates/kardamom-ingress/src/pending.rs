//! Pending-receipts map: parks a client `oneshot` until both
//! (a) a `CachedReceipt` for `(sender, nonce)` arrives on the receipt-cache
//!     channel (the executor's authoritative `(sender, nonce, receipt)`
//!     binding), AND
//! (b) the quorum fsync watermark on B has reached `receipt.tx_idx`.
//!
//! Both conditions are required by invariant I2 (spec §1).

use std::sync::Arc;
use std::time::Duration;

use alloy_primitives::Address;
use dashmap::DashMap;
use tokio::sync::{Mutex, oneshot};

use kardamom_types::{BPosition, QuorumWatermark, Receipt};

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

pub struct PendingReceipts {
    map: PendingMap,
    /// Latest watermark observed. Cached to avoid one-receiver-per-await
    /// fanout.
    latest_watermark: Arc<Mutex<Option<BPosition>>>,
}

impl Default for PendingReceipts {
    fn default() -> Self {
        Self::new()
    }
}

impl PendingReceipts {
    pub fn new() -> Self {
        Self {
            map: Arc::new(DashMap::new()),
            latest_watermark: Arc::new(Mutex::new(None)),
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

    /// Called by the receipt watcher when a `CachedReceipt` arrives. If the
    /// watermark has already advanced past the receipt's B-position, releases
    /// the client immediately; otherwise stores the receipt and waits for
    /// `update_watermark`.
    pub async fn on_receipt(&self, sender: Address, nonce: u64, receipt: Receipt) {
        let key = (sender, nonce);
        // Clone the entry Arc and immediately drop the dashmap ref so we
        // don't hold the shard lock while awaiting `entry.lock()`.
        let entry = {
            let Some(r) = self.map.get(&key) else {
                return;
            };
            r.value().clone()
        };
        let mut e = entry.lock().await;
        e.receipt = Some(receipt.clone());
        let latest = *self.latest_watermark.lock().await;
        if Self::watermark_past(&latest, receipt.tx_idx)
            && let Some(resp) = e.responder.take()
        {
            let _ = resp.send(Ok(ReceiptResponse { receipt }));
            drop(e);
            self.map.remove(&key);
        }
    }

    /// Called when a new watermark snapshot is observed. Releases every
    /// parked entry whose stored receipt's B-position is now covered.
    pub async fn update_watermark(&self, wm: QuorumWatermark) {
        *self.latest_watermark.lock().await = Some(wm.position);
        // Snapshot the (key, Arc) pairs so we never hold a dashmap shard
        // lock while awaiting per-entry locks below.
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
                .map(|r| Self::watermark_past(&Some(wm.position), r.tx_idx))
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

    /// `latest >= target` in lexicographic `(term_id, term_offset)` order.
    fn watermark_past(latest: &Option<BPosition>, target: BPosition) -> bool {
        match latest {
            None => false,
            Some(p) => *p >= target,
        }
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
        }
    }

    #[tokio::test]
    async fn parks_until_receipt_and_watermark_both_arrive() {
        let p = Arc::new(PendingReceipts::new());
        let sender = Address::repeat_byte(0x11);
        let nonce = 7u64;
        let pos = BPosition {
            term_id: 0,
            term_offset: 100,
        };

        let wait = p.register(sender, nonce);
        let waiter =
            tokio::spawn(async move { wait.await_with_timeout(Duration::from_secs(5)).await });
        // Give the spawn time to register.
        tokio::time::sleep(Duration::from_millis(10)).await;

        // Receipt arrives but watermark hasn't caught up — must NOT release.
        p.on_receipt(sender, nonce, dummy_receipt(pos)).await;
        tokio::time::sleep(Duration::from_millis(10)).await;
        assert_eq!(p.len(), 1);

        // Watermark advances → releases.
        p.update_watermark(QuorumWatermark { position: pos }).await;
        let res = waiter.await.unwrap().unwrap();
        assert_eq!(res.receipt.tx_idx, pos);
        assert_eq!(p.len(), 0);
    }

    #[tokio::test]
    async fn releases_immediately_when_watermark_already_past() {
        let p = Arc::new(PendingReceipts::new());
        let sender = Address::repeat_byte(0x22);
        let pos = BPosition {
            term_id: 0,
            term_offset: 5,
        };
        // Watermark advances first.
        p.update_watermark(QuorumWatermark {
            position: BPosition {
                term_id: 0,
                term_offset: 1000,
            },
        })
        .await;

        let wait = p.register(sender, 1);
        let waiter =
            tokio::spawn(async move { wait.await_with_timeout(Duration::from_secs(5)).await });
        tokio::time::sleep(Duration::from_millis(10)).await;
        p.on_receipt(sender, 1, dummy_receipt(pos)).await;
        let res = waiter.await.unwrap().unwrap();
        assert_eq!(res.receipt.tx_idx, pos);
    }

    #[tokio::test]
    async fn times_out_when_neither_event_arrives() {
        let p = PendingReceipts::new();
        let wait = p.register(Address::ZERO, 0);
        let err = wait
            .await_with_timeout(Duration::from_millis(20))
            .await
            .unwrap_err();
        assert!(matches!(err, IngressError::Timeout));
        assert_eq!(p.len(), 0);
    }
}

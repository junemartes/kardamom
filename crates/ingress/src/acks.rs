//! The ack at admission: the waiters of `eth_sendRawTransaction` for the
//! verdict of the sequencer.
//!
//! An Ethereum node answers `eth_sendRawTransaction` when its mempool takes
//! the transaction. Here the mempool is the admission filter of the
//! sequencer that owns the sender. Each twin of the lane sends one verdict
//! for each transaction, on the `tx_verdicts` stream of the ingress that
//! published it:
//! - `Accepted` answers the call with the hash at once;
//! - `Rejected` answers with the error when every twin rejected, or when
//!   the reject grace passed with no accept: a twin with stale rows can
//!   reject a transaction that the other twin took;
//! - no verdict in the ack timeout answers `transaction not acknowledged`.
//!
//! The map holds one slot for each waiting hash. The wait owns its slot: its
//! `Drop` removes it, guarded by the slot id, however the wait ends. A
//! second submission of a hash that waits gets `already known`, as a copy
//! of a transaction in the mempool of an Ethereum node does.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use alloy_primitives::{Address, B256};
use dashmap::DashMap;
use dashmap::mapref::entry::Entry;
use kardamom_types::{TxErrorReason, TxStage, TxStatus};
use tokio::sync::mpsc;
use tokio::time::Instant;

use crate::config::AdmissionAck;
use crate::error::IngressError;
use crate::pending::reason_to_error;

/// The verdicts that can wait for one waiter: one from each twin, and room
/// for a copy.
const SLOT_DEPTH: usize = 4;

/// The waiting slot of one hash.
struct Slot {
    id: u64,
    verdicts: mpsc::Sender<TxStatus>,
}

/// The waiters of the ack at admission, by transaction hash.
pub(crate) struct AckWaiters {
    slots: DashMap<B256, Slot>,
    next_id: AtomicU64,
    cfg: AdmissionAck,
}

impl AckWaiters {
    pub(crate) fn new(cfg: AdmissionAck) -> Arc<Self> {
        Arc::new(Self {
            slots: DashMap::new(),
            next_id: AtomicU64::new(0),
            cfg,
        })
    }

    /// The submissions that wait for a verdict.
    pub(crate) fn len(&self) -> usize {
        self.slots.len()
    }

    /// Wait for the verdict of `tx_hash`. Register before the publication,
    /// so a fast verdict finds the slot.
    ///
    /// # Errors
    ///
    /// Returns [`IngressError::AlreadyKnown`] when a submission of the same
    /// hash waits already.
    pub(crate) fn register(self: &Arc<Self>, tx_hash: B256) -> Result<AckWait, IngressError> {
        let (tx, rx) = mpsc::channel(SLOT_DEPTH);
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        match self.slots.entry(tx_hash) {
            Entry::Occupied(_) => return Err(IngressError::AlreadyKnown),
            Entry::Vacant(v) => v.insert(Slot { id, verdicts: tx }),
        };
        Ok(AckWait {
            waiters: Arc::clone(self),
            tx_hash,
            id,
            verdicts: rx,
        })
    }

    /// Give one verdict to the waiter of its hash. A verdict with no waiter
    /// is the verdict of another twin after the answer, or of a client that
    /// left: it goes nowhere. A full slot drops the copy.
    pub(crate) fn on_verdict(&self, status: TxStatus) {
        if let Some(slot) = self.slots.get(&status.tx_hash) {
            let _ = slot.verdicts.try_send(status);
        }
    }

    fn remove(&self, tx_hash: &B256, id: u64) {
        self.slots.remove_if(tx_hash, |_, slot| slot.id == id);
    }
}

/// One submission that waits for its verdict.
pub(crate) struct AckWait {
    waiters: Arc<AckWaiters>,
    tx_hash: B256,
    id: u64,
    verdicts: mpsc::Receiver<TxStatus>,
}

impl AckWait {
    /// Wait for the answer: the hash on an accept, or the error.
    ///
    /// # Errors
    ///
    /// Returns the error of the rejection, or
    /// [`IngressError::NotAcknowledged`] when no verdict came in time.
    pub(crate) async fn answer(mut self) -> Result<B256, IngressError> {
        let cfg = self.waiters.cfg;
        let mut deadline = Instant::now() + cfg.timeout;
        let mut rejected = Rejections::default();
        loop {
            let Ok(Some(status)) = tokio::time::timeout_at(deadline, self.verdicts.recv()).await
            else {
                return Err(rejected.into_error(cfg.timeout));
            };
            match status.stage {
                TxStage::Accepted { .. } => return Ok(self.tx_hash),
                TxStage::Rejected {
                    sender,
                    nonce,
                    reason,
                } => {
                    if rejected.add(verdict_error(sender, nonce, &reason), cfg.twins.get()) {
                        return Err(rejected.into_error(cfg.timeout));
                    }
                    deadline = deadline.min(Instant::now() + cfg.reject_grace);
                }
                TxStage::Offered { .. } | TxStage::Sealed | TxStage::Executed { .. } => {}
            }
        }
    }
}

impl Drop for AckWait {
    fn drop(&mut self) {
        self.waiters.remove(&self.tx_hash, self.id);
    }
}

/// The rejections of one submission so far.
#[derive(Default)]
struct Rejections {
    count: u8,
    /// The first rejection: the answer when no twin accepts.
    first: Option<IngressError>,
}

impl Rejections {
    /// Count one rejection. Returns whether every twin rejected.
    fn add(&mut self, error: IngressError, twins: u8) -> bool {
        self.count += 1;
        self.first.get_or_insert(error);
        self.count >= twins
    }

    /// The answer of a wait that no accept ended: the first rejection, or
    /// no ack within `timeout`.
    fn into_error(self, timeout: std::time::Duration) -> IngressError {
        self.first.unwrap_or(IngressError::NotAcknowledged(timeout))
    }
}

/// The client error of a rejection at admission, with the error texts of
/// geth where geth has one.
fn verdict_error(sender: Address, nonce: u64, reason: &TxErrorReason) -> IngressError {
    match reason {
        TxErrorReason::DuplicatedTx { expected_nonce } => IngressError::NonceTooLow {
            address: sender,
            tx_nonce: nonce,
            state_nonce: *expected_nonce,
        },
        TxErrorReason::Evicted { .. } => IngressError::PoolFull,
        other => reason_to_error(sender, nonce, other),
    }
}

#[cfg(test)]
mod tests;

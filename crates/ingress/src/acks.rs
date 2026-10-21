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

use std::ops::ControlFlow;

use alloy_primitives::B256;
use dashmap::DashMap;
use dashmap::mapref::entry::Entry;
use kardamom_types::{TxStage, TxStatus};
use tokio::sync::mpsc;
use tokio::time::Instant;

use crate::config::AdmissionAck;
use crate::error::IngressError;

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
        let mut answer = Answer::new(self.tx_hash, self.waiters.cfg);
        loop {
            let verdict = tokio::time::timeout_at(answer.deadline, self.verdicts.recv()).await;
            if let ControlFlow::Break(result) = answer.on(verdict.ok().flatten()) {
                return result;
            }
        }
    }
}

impl Drop for AckWait {
    fn drop(&mut self) {
        self.waiters.remove(&self.tx_hash, self.id);
    }
}

/// The state of one wait for its answer.
struct Answer {
    tx_hash: B256,
    cfg: AdmissionAck,
    /// The end of the wait: the ack timeout, or the reject grace after the
    /// first rejection.
    deadline: Instant,
    /// The rejections so far.
    count: u8,
    /// The first rejection: the answer when no twin accepts.
    first: Option<IngressError>,
}

impl Answer {
    fn new(tx_hash: B256, cfg: AdmissionAck) -> Self {
        Self {
            tx_hash,
            cfg,
            deadline: Instant::now() + cfg.timeout,
            count: 0,
            first: None,
        }
    }

    /// Take one verdict, or `None` at the deadline. `Break` carries the
    /// answer.
    fn on(&mut self, verdict: Option<TxStatus>) -> ControlFlow<Result<B256, IngressError>> {
        let Some(status) = verdict else {
            return ControlFlow::Break(Err(self.error()));
        };
        match status.stage {
            TxStage::Accepted { .. } => ControlFlow::Break(Ok(self.tx_hash)),
            TxStage::Rejected {
                sender,
                nonce,
                reason,
            } => self.reject(IngressError::of_verdict(sender, nonce, &reason)),
            TxStage::Offered { .. } | TxStage::Sealed | TxStage::Executed { .. } => {
                ControlFlow::Continue(())
            }
        }
    }

    /// Count one rejection. Every twin rejected: answer it. Otherwise wait
    /// the reject grace for an accept of another twin.
    fn reject(&mut self, error: IngressError) -> ControlFlow<Result<B256, IngressError>> {
        // The wait ends at `twins`, so the count stays far below the bound.
        self.count = self.count.saturating_add(1);
        self.first.get_or_insert(error);
        if self.count >= self.cfg.twins.get() {
            return ControlFlow::Break(Err(self.error()));
        }
        self.deadline = self.deadline.min(Instant::now() + self.cfg.reject_grace);
        ControlFlow::Continue(())
    }

    /// The answer of a wait that no accept ended: the first rejection, or
    /// no ack in the timeout.
    fn error(&mut self) -> IngressError {
        self.first
            .take()
            .unwrap_or(IngressError::NotAcknowledged(self.cfg.timeout))
    }
}

#[cfg(test)]
mod tests;

//! `TxVerdicts`: the receipts of the transactions of one ingress, from the
//! sequencer that owns their senders.
//!
//! A verdict frame has the format of a `tx_receipts` frame: a
//! [`ReceiptBatch`] with no account rows, or a [`ReceiptRollback`]. So the
//! ingress reads its verdict stream with [`super::tx_receipts::TxReceiptsSubscriberHandle`],
//! and its receipt pump takes either stream.
//!
//! The publish is best effort: the Aeron thread holds a refused frame, and
//! every later frame of the same publication, until the offer deadline,
//! then drops it. So the first verdicts wait for the image of a new
//! ingress to connect. A client whose verdict drops waits until the
//! timeout of its ingress, and a sequencer never waits for an ingress.

use super::super::PubHandle;
use crate::codec;
use crate::error::LogError;
use kardamom_types::{ReceiptBatch, ReceiptRollback};

/// The publication of the verdicts for one target ingress.
#[derive(Clone)]
pub struct TxVerdictsPublisherHandle {
    inner: PubHandle,
}

impl TxVerdictsPublisherHandle {
    /// Wrap a publication that the stream plane opened.
    #[must_use]
    pub fn from_publication(inner: PubHandle) -> Self {
        Self { inner }
    }

    /// Offer the receipts of `batch`, best effort.
    ///
    /// # Errors
    ///
    /// Returns an error if `batch` fails to encode. The offer itself never
    /// errors.
    pub fn publish_receipts(&self, batch: &ReceiptBatch) -> Result<(), LogError> {
        self.inner.publish_best_effort(codec::encode(batch)?);
        Ok(())
    }

    /// Offer `rollback`, best effort.
    ///
    /// # Errors
    ///
    /// Returns an error if `rollback` fails to encode.
    pub fn publish_rollback(&self, rollback: &ReceiptRollback) -> Result<(), LogError> {
        self.inner.publish_best_effort(codec::encode(rollback)?);
        Ok(())
    }
}

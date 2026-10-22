//! `TxVerdicts`: the answer of the sequencer to each transaction of one
//! ingress.
//!
//! A verdict frame is one [`TxStatus`] record: `Accepted` when the
//! sequencer took the transaction into its mempool, or `Rejected` with the
//! reason. The ingress answers `eth_sendRawTransaction` on it. The ingress
//! reads its verdict stream with
//! [`super::simple::TxStatusSubscriberHandle`].
//!
//! The publish is best effort: the Aeron thread holds a refused frame, and
//! every later frame of the same publication, until the offer deadline,
//! then drops it. So the first verdicts wait for the image of a new
//! ingress to connect. A client whose verdict drops gets the ack timeout
//! of its ingress, and a sequencer never waits for an ingress.

use super::super::PubHandle;
use crate::codec;
use crate::error::LogError;
use kardamom_types::TxStatus;

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

    /// Offer the verdict `status`, best effort.
    ///
    /// # Errors
    ///
    /// Returns an error if `status` fails to encode. The offer itself never
    /// errors.
    pub fn publish_status(&self, status: &TxStatus) -> Result<(), LogError> {
        self.inner.publish_best_effort(codec::encode(status)?);
        Ok(())
    }
}

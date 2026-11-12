//! Publish ready sender prefixes and retain suffixes until confirmation.

use kardamom_types::TxStatus;

use super::AdmissionLoop;
use crate::SequencerError;
use crate::outbound::{RefOffer, SideChannelPublisher, TxOrderingRefPublisher};

impl AdmissionLoop {
    pub(super) fn flush<B: TxOrderingRefPublisher, R: SideChannelPublisher>(
        &mut self,
        refs: &mut B,
        side: &mut R,
    ) -> Result<(), SequencerError> {
        if self.outbox.is_empty() {
            return Ok(());
        }
        let error = if self.sealer_version.orders_by_position() {
            self.flush_positioned(refs, side)
        } else {
            // A later switch to positioned offers scans this outbox in full.
            self.canonical.changed();
            let (published, error) = self.pipeline.offer(refs, self.outbox.make_contiguous());
            self.hot.publish.increment(published as u64);
            self.outbox
                .drain(..published)
                .for_each(|offer| offer.note_sent(side));
            error
        };
        self.publish_result(error)
    }

    fn flush_positioned<B: TxOrderingRefPublisher, R: SideChannelPublisher>(
        &mut self,
        refs: &mut B,
        side: &mut R,
    ) -> Option<SequencerError> {
        let mut ready = self
            .canonical
            .prepare(&mut self.outbox, &mut self.admission);
        if ready.is_empty() {
            self.canonical.recycle(ready);
            return None;
        }
        let (published, error) = self.pipeline.offer(refs, &ready);
        self.hot.publish.increment(published as u64);
        ready[..published]
            .iter()
            .for_each(|offer| self.canonical.published(offer));
        ready
            .drain(..published)
            .for_each(|offer| offer.note_sent(side));
        if !ready.is_empty() {
            self.canonical.changed();
        }
        ready
            .drain(..)
            .rev()
            .for_each(|offer| self.outbox.push_front(offer));
        self.canonical.recycle(ready);
        error
    }

    fn publish_result(&mut self, error: Option<SequencerError>) -> Result<(), SequencerError> {
        match error {
            Some(SequencerError::SessionChanged) => {
                self.offer_again_unconfirmed("sealer session changed");
                Ok(())
            }
            Some(SequencerError::Backpressure) => {
                self.hot.backpressure.increment(1);
                Err(SequencerError::Backpressure)
            }
            other => other.map_or(Ok(()), Err),
        }
    }
}

impl RefOffer {
    fn note_sent<R: SideChannelPublisher>(&self, side: &mut R) {
        side.publish_status(TxStatus::offered(
            self.tx_ref.tx_hash,
            self.guard.sender,
            self.guard.nonce,
        ));
        side.note_sent(&self.tx_ref.tx_hash);
    }
}

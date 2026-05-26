//! Outbound channel abstractions.
//!
//! Under the MDS topology (D-Sh12 v2) the sequencer **does not** publish
//! to tx_data — that's the proxy's job. The sequencer is purely a
//! reader-of-A + publisher-of-B-refs: for each envelope observed on its
//! shard's tx_data (via [`crate::inbound::TxDataSubscriber`]), if the
//! nonce gate matches, it publishes a tiny [`kardamom_types::TxRef`] onto
//! the canonical orderer tx_ordering
//! ([`TxOrderingRefPublisher`]) — Aeron *concurrent* multi-publisher, ordered
//! with refs from the other P-1 sequencers in this shard's group and
//! sealer-emitted `BlockBoundaryStart` markers.
//!
//! Pending-buffer state is advanced once the B publish succeeds. If B
//! back-pressures, the state machine is rewound via
//! [`crate::state::PartitionState::reinsert_for_retry`] and the error
//! bubbles up.
//!
//! In addition the sequencer publishes [`DuplicateNotification`]s for
//! past-nonce txs on the **receipt-cache** channel
//! ([`ReceiptCachePublisher`]).
//!
//! All surfaces are traits so unit tests can use the in-memory fakes
//! (no Aeron media driver required); production wiring binds them to the
//! real `kardamom_log::publisher` types.

use kardamom_types::TxRef;

use crate::duplicate::DuplicateNotification;
use crate::error::SequencerError;

/// TxOrdering publisher contract — the canonical orderer. Publishes tiny
/// [`TxRef`] records (~41 B) into Aeron's concurrent multi-publisher
/// stream.
///
/// A blocked transport must surface as `Err(SequencerError::Backpressure)`
/// so the state machine can rewind.
pub trait TxOrderingRefPublisher: Send {
    fn try_publish_ref(&mut self, r: &TxRef) -> Result<(), SequencerError>;
}

/// Receipt-cache channel publisher. Best-effort: errors are logged by the
/// caller and not propagated, because the canonical state has already
/// advanced.
pub trait ReceiptCachePublisher: Send {
    fn publish_duplicate(&mut self, notification: DuplicateNotification);
}

// ===========================================================================
// In-memory fakes for unit / integration tests.
// ===========================================================================

#[cfg(any(test, feature = "testing"))]
pub mod fakes {
    use std::sync::{Arc, Mutex};

    use kardamom_types::TxRef;

    use super::*;

    /// In-memory tx_ordering `TxRef` publisher. Records every published ref
    /// in arrival order so tests can assert the canonical sequence.
    #[derive(Default, Clone)]
    pub struct InMemoryTxOrderingRefPublisher {
        pub refs: Arc<Mutex<Vec<TxRef>>>,
        pub fail_with_backpressure: Arc<Mutex<bool>>,
    }

    impl TxOrderingRefPublisher for InMemoryTxOrderingRefPublisher {
        fn try_publish_ref(&mut self, r: &TxRef) -> Result<(), SequencerError> {
            if *self.fail_with_backpressure.lock().unwrap() {
                return Err(SequencerError::Backpressure);
            }
            self.refs.lock().unwrap().push(*r);
            Ok(())
        }
    }

    #[derive(Default, Clone)]
    pub struct InMemoryReceiptCachePublisher {
        pub duplicates: Arc<Mutex<Vec<DuplicateNotification>>>,
    }

    impl ReceiptCachePublisher for InMemoryReceiptCachePublisher {
        fn publish_duplicate(&mut self, notification: DuplicateNotification) {
            self.duplicates.lock().unwrap().push(notification);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::fakes::*;
    use super::*;
    use alloy_primitives::{Address, B256};
    use kardamom_types::{BPosition, TxRef};

    #[test]
    fn fake_b_records_refs() {
        let mut p = InMemoryTxOrderingRefPublisher::default();
        p.try_publish_ref(&TxRef::new(B256::ZERO, 0, BPosition::default()))
            .unwrap();
        p.try_publish_ref(&TxRef::new(B256::ZERO, 1, BPosition::default()))
            .unwrap();
        assert_eq!(p.refs.lock().unwrap().len(), 2);
    }

    #[test]
    fn fake_b_can_simulate_backpressure() {
        let mut p = InMemoryTxOrderingRefPublisher::default();
        *p.fail_with_backpressure.lock().unwrap() = true;
        assert!(matches!(
            p.try_publish_ref(&TxRef::new(B256::ZERO, 0, BPosition::default())),
            Err(SequencerError::Backpressure)
        ));
    }

    #[test]
    fn fake_receipt_cache_records_duplicates() {
        let mut p = InMemoryReceiptCachePublisher::default();
        p.publish_duplicate(DuplicateNotification {
            correlation_id: 42,
            sender: Address::repeat_byte(0xAB),
            nonce: 7,
        });
        assert_eq!(p.duplicates.lock().unwrap().len(), 1);
    }
}

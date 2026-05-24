//! Outbound channel abstractions.
//!
//! The sequencer publishes to two Aeron streams:
//!  - **Channel B** (canonical tx log): the source of truth. Carries
//!    `kardamom_types::TxEnvelope` payloads rkyv-archived via
//!    `kardamom_log::codec::encode`.
//!  - **Receipt-cache channel**: receives `DuplicateNotification` for past
//!    nonces so the proxy can release any waiting client with the cached
//!    receipt.
//!
//! Both surfaces are traits so tests use the in-memory fakes (no Aeron media
//! driver required).

use crate::duplicate::DuplicateNotification;
use crate::error::SequencerError;

/// Channel-B publisher contract.
///
/// `try_publish` must return `Err(SequencerError::Backpressure)` if the
/// underlying transport is blocked; the primary sequencer relies on that
/// signal to rewind its state machine and retry without skipping nonces.
pub trait BPublisher: Send {
    fn try_publish(&mut self, frame_bytes: &[u8]) -> Result<(), SequencerError>;
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

    use super::*;

    #[derive(Default, Clone)]
    pub struct InMemoryBPublisher {
        pub published: Arc<Mutex<Vec<Vec<u8>>>>,
        pub fail_with_backpressure: Arc<Mutex<bool>>,
    }

    impl BPublisher for InMemoryBPublisher {
        fn try_publish(&mut self, frame_bytes: &[u8]) -> Result<(), SequencerError> {
            if *self.fail_with_backpressure.lock().unwrap() {
                return Err(SequencerError::Backpressure);
            }
            self.published.lock().unwrap().push(frame_bytes.to_vec());
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
    use alloy_primitives::Address;

    #[test]
    fn fake_b_records_frames() {
        let mut p = InMemoryBPublisher::default();
        p.try_publish(&[1, 2, 3]).unwrap();
        p.try_publish(&[4]).unwrap();
        assert_eq!(p.published.lock().unwrap().len(), 2);
    }

    #[test]
    fn fake_b_can_simulate_backpressure() {
        let mut p = InMemoryBPublisher::default();
        *p.fail_with_backpressure.lock().unwrap() = true;
        assert!(matches!(
            p.try_publish(&[0]),
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

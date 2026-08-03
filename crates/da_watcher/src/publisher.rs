//! Outbound sink: the [`EpochPublisher`] trait the DA watcher writes
//! [`kardamom_types::EpochRecord`]s to. Production binds this to a
//! `kardamom_log::aeron_live::TxDepositsPublisherHandle`; tests use the
//! in-memory fake in [`fakes`].
//!
//! The unit is an EPOCH, not a deposit: one record per finalized L1 block,
//! carrying that block's deposits by value and emitted even when there are
//! none. See `docs/agents/l1-origin-deposit-derivation-spec.md`.

use kardamom_types::{BPosition, EpochRecord};

/// Errors a publish can surface.
#[derive(Debug, thiserror::Error)]
pub enum PublishError {
    /// The transport is backpressured. The watcher's tick caller logs and
    /// drops; the next tick will retry the same `(cursor, tip]` range.
    #[error("publisher backpressured")]
    Backpressure,
    /// The transport is closed (subscription torn down, daemon stopped).
    /// Fatal at the watcher level — the spawn loop exits.
    #[error("publisher closed")]
    Closed,
    /// Other transport/encoding failure.
    #[error("publish failed: {0}")]
    Transport(String),
}

/// Sink for epochs the watcher emits. Implementors must be `Send + Sync`
/// because the watcher's tokio task moves them between executions.
pub trait EpochPublisher: Send + Sync + 'static {
    /// Publish one epoch. Returns the assigned wire position (analogous to
    /// Aeron's offer-position) so callers can correlate.
    fn publish(&self, epoch: &EpochRecord) -> Result<BPosition, PublishError>;
}

#[cfg(any(test, feature = "testing"))]
pub mod fakes {
    use std::sync::{Arc, Mutex};

    use super::*;

    /// In-memory [`EpochPublisher`] that records every published epoch in
    /// order. The synthesised `BPosition` advances by `64` per record so tests
    /// can assert positions without coupling to Aeron framing.
    #[derive(Default, Clone)]
    pub struct InMemoryEpochPublisher {
        pub published: Arc<Mutex<Vec<EpochRecord>>>,
        pub fail_with_backpressure: Arc<Mutex<bool>>,
    }

    impl EpochPublisher for InMemoryEpochPublisher {
        fn publish(&self, epoch: &EpochRecord) -> Result<BPosition, PublishError> {
            if *self.fail_with_backpressure.lock().unwrap() {
                return Err(PublishError::Backpressure);
            }
            let mut v = self.published.lock().unwrap();
            v.push(epoch.clone());
            Ok(BPosition {
                term_id: 0,
                term_offset: (v.len() as i32) * 64,
            })
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloy_primitives::B256;
    use fakes::InMemoryEpochPublisher;

    fn epoch(n: u64) -> EpochRecord {
        EpochRecord {
            l1_number: n,
            l1_hash: B256::repeat_byte(n as u8),
            deposits: Vec::new(),
        }
    }

    #[test]
    fn in_memory_publisher_records_in_order() {
        let p = InMemoryEpochPublisher::default();
        p.publish(&epoch(1)).unwrap();
        p.publish(&epoch(2)).unwrap();
        let got = p.published.lock().unwrap();
        assert_eq!(got.len(), 2);
        assert_eq!(got[0].l1_number, 1);
        assert_eq!(got[1].l1_number, 2);
    }

    #[test]
    fn in_memory_publisher_can_simulate_backpressure() {
        let p = InMemoryEpochPublisher::default();
        *p.fail_with_backpressure.lock().unwrap() = true;
        assert!(matches!(
            p.publish(&epoch(1)),
            Err(PublishError::Backpressure)
        ));
        assert!(p.published.lock().unwrap().is_empty());
    }
}

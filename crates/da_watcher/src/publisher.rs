//! Outbound sink: the [`DepositPublisher`] trait the DA watcher writes
//! [`kardamom_types::Deposit`] envelopes to. Production binds this to a
//! `kardamom_log::TxDepositsPublisher`; tests use the in-memory fake in [`fakes`].

use kardamom_types::{BPosition, Deposit};

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

/// Sink for deposits the watcher emits. Implementors must be `Send + Sync`
/// because the watcher's tokio task moves them between executions.
pub trait DepositPublisher: Send + Sync + 'static {
    /// Publish a single deposit. Returns the assigned wire position
    /// (analogous to Aeron's offer-position) so callers can correlate.
    fn publish(&self, deposit: &Deposit) -> Result<BPosition, PublishError>;
}

#[cfg(any(test, feature = "testing"))]
pub mod fakes {
    use std::sync::{Arc, Mutex};

    use super::*;

    /// In-memory `DepositPublisher` that records every published deposit
    /// in order. The synthesised `BPosition` advances by `64` per record so
    /// tests can assert positions without coupling to Aeron framing.
    #[derive(Default, Clone)]
    pub struct InMemoryDepositPublisher {
        pub published: Arc<Mutex<Vec<Deposit>>>,
        pub fail_with_backpressure: Arc<Mutex<bool>>,
    }

    impl DepositPublisher for InMemoryDepositPublisher {
        fn publish(&self, deposit: &Deposit) -> Result<BPosition, PublishError> {
            if *self.fail_with_backpressure.lock().unwrap() {
                return Err(PublishError::Backpressure);
            }
            let mut v = self.published.lock().unwrap();
            v.push(deposit.clone());
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
    use alloy_primitives::{Address, B256, U256};
    use bytes::Bytes;
    use fakes::InMemoryDepositPublisher;

    fn deposit() -> Deposit {
        Deposit {
            source_hash: B256::repeat_byte(0x42),
            from: Address::repeat_byte(0x11),
            to: Some(Address::repeat_byte(0x22)),
            mint: 1_000,
            value: U256::from(1_000u64),
            gas_limit: 100_000,
            is_system_transaction: false,
            input: Bytes::new(),
        }
    }

    #[test]
    fn in_memory_publisher_records_in_order() {
        let p = InMemoryDepositPublisher::default();
        let pos1 = p.publish(&deposit()).unwrap();
        let pos2 = p.publish(&deposit()).unwrap();
        assert!(pos2.term_offset > pos1.term_offset);
        assert_eq!(p.published.lock().unwrap().len(), 2);
    }

    #[test]
    fn in_memory_publisher_surfaces_backpressure() {
        let p = InMemoryDepositPublisher::default();
        *p.fail_with_backpressure.lock().unwrap() = true;
        assert!(matches!(
            p.publish(&deposit()),
            Err(PublishError::Backpressure)
        ));
        assert!(p.published.lock().unwrap().is_empty());
    }
}

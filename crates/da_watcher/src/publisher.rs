//! Outbound sink: the [`EpochPublisher`] trait the DA watcher writes
//! [`kardamom_types::EpochRecord`]s to. Production binds this to a
//! `kardamom_log::aeron_live::TxDepositsPublisherHandle`. Tests use the
//! in-memory fake in [`fakes`].
//!
//! The unit is an epoch, not a deposit. One record covers one finalized L1
//! block. It carries that block's deposits by value, and the watcher emits
//! it even when there are no deposits.

use kardamom_types::{BPosition, EpochRecord};

/// Errors a publish can surface.
#[derive(Debug, thiserror::Error)]
pub enum PublishError {
    /// The transport is backpressured. The watcher's tick caller logs this
    /// and drops the publish. The next tick retries the same
    /// `(cursor, tip]` range.
    #[error("publisher backpressured")]
    Backpressure,
    /// The transport is closed: the subscription is torn down, or the
    /// daemon stopped. This is fatal at the watcher level, and the spawn
    /// loop exits.
    #[error("publisher closed")]
    Closed,
    /// Other transport/encoding failure.
    #[error("publish failed: {0}")]
    Transport(String),
}

/// Sink for epochs the watcher emits. An implementation must be
/// `Send + Sync`, because the watcher's tokio task moves it between runs.
pub trait EpochPublisher: Send + Sync + 'static {
    /// Publish one epoch. Return the assigned wire position, similar to
    /// Aeron's offer position, so a caller can correlate it.
    ///
    /// # Errors
    /// Returns [`PublishError`] when the transport is backpressured, closed,
    /// or fails.
    fn publish(&self, epoch: &EpochRecord) -> Result<BPosition, PublishError>;
}

#[cfg(any(test, feature = "testing"))]
pub mod fakes {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, AtomicI32, Ordering};
    use std::sync::mpsc::{Receiver, Sender, channel};

    use super::{BPosition, EpochPublisher, EpochRecord, PublishError};

    /// In-memory [`EpochPublisher`]. It sends every published epoch, in
    /// order, to the [`PublisherTap`] that [`Self::new`] returns. Its
    /// synthetic `BPosition` advances by `64` per record, so a test can
    /// check positions without depending on Aeron framing.
    ///
    /// A clone sends to the same tap and shares the position counter, so a
    /// test can hand one stream of epochs to several watcher lifetimes.
    #[derive(Clone)]
    pub struct InMemoryEpochPublisher {
        published: Sender<EpochRecord>,
        count: Arc<AtomicI32>,
        backpressure: Arc<AtomicBool>,
    }

    /// The test's side of an [`InMemoryEpochPublisher`]: the published
    /// epochs, and the switch that jams the transport. The switch is
    /// shared, because a test can jam the transport from another thread
    /// while the watcher publishes.
    pub struct PublisherTap {
        published: Receiver<EpochRecord>,
        backpressure: Arc<AtomicBool>,
    }

    impl InMemoryEpochPublisher {
        #[must_use]
        pub fn new() -> (Self, PublisherTap) {
            let (tx, rx) = channel();
            let backpressure = Arc::new(AtomicBool::new(false));
            let publisher = Self {
                published: tx,
                count: Arc::new(AtomicI32::new(0)),
                backpressure: Arc::clone(&backpressure),
            };
            let tap = PublisherTap {
                published: rx,
                backpressure,
            };
            (publisher, tap)
        }
    }

    impl EpochPublisher for InMemoryEpochPublisher {
        fn publish(&self, epoch: &EpochRecord) -> Result<BPosition, PublishError> {
            if self.backpressure.load(Ordering::Relaxed) {
                return Err(PublishError::Backpressure);
            }
            // The tap may be gone once the test stops reading; the publish
            // still succeeds.
            let _ = self.published.send(epoch.clone());
            let n = self.count.fetch_add(1, Ordering::Relaxed) + 1;
            Ok(BPosition {
                term_id: 0,
                // A fake position; test record counts never approach i32::MAX.
                term_offset: n * 64,
            })
        }
    }

    impl PublisherTap {
        /// Every epoch published since the last call, in order.
        #[must_use]
        pub fn epochs(&self) -> Vec<EpochRecord> {
            self.published.try_iter().collect()
        }

        /// Jam the transport (`true`): every publish then fails with
        /// [`PublishError::Backpressure`]. `false` clears it.
        pub fn set_backpressure(&self, on: bool) {
            self.backpressure.store(on, Ordering::Relaxed);
        }

        /// Block until `n` epochs publish, then jam the transport. If the
        /// publisher drops first, return without jamming.
        pub fn jam_after(self, n: usize) {
            if self.published.iter().take(n).count() == n {
                self.set_backpressure(true);
            }
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
            l1_hash: B256::repeat_byte(u8::try_from(n).unwrap()),
            deposits: Vec::new(),
        }
    }

    #[test]
    fn in_memory_publisher_records_in_order() {
        let (p, tap) = InMemoryEpochPublisher::new();
        p.publish(&epoch(1)).unwrap();
        let second = p.publish(&epoch(2)).unwrap();
        let got = tap.epochs();
        assert_eq!(got.len(), 2);
        assert_eq!(got[0].l1_number, 1);
        assert_eq!(got[1].l1_number, 2);
        assert_eq!(second.term_offset, 128);
    }

    #[test]
    fn in_memory_publisher_can_simulate_backpressure() {
        let (p, tap) = InMemoryEpochPublisher::new();
        tap.set_backpressure(true);
        assert!(matches!(
            p.publish(&epoch(1)),
            Err(PublishError::Backpressure)
        ));
        assert!(tap.epochs().is_empty());
    }
}

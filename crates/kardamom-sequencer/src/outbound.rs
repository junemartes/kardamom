//! Outbound channel abstractions for the **D-Sh12 split architecture**.
//!
//! The sequencer now performs a **dual write** per accepted transaction
//! (spec §2.2 / §2.3 / D11):
//!
//!  1. The full `TxEnvelope` bytes go to the sequencer's *own* per-sequencer
//!     **channel A** ([`ChannelAPublisher`]) — Aeron *exclusive* publication,
//!     so there is no CAS-cursor contention on the bulk-data path. The
//!     publish returns the fragment's `BPosition` on the A archive.
//!  2. A tiny [`kardamom_types::TxRef`] `{ sequencer_id, position_a }` goes
//!     to the canonical orderer **channel B**
//!     ([`ChannelBRefPublisher`]) — Aeron *concurrent* multi-publisher,
//!     ordered with refs from the other M-1 sequencers and sealer-emitted
//!     `BlockBoundaryStart` markers.
//!
//! The pending-buffer state is **only** advanced once *both* publications
//! have succeeded. If either back-pressures, the state machine is rewound
//! via [`crate::state::PartitionState::reinsert_for_retry`] and the error
//! bubbles up. (Note: on a B-backpressure retry the previously-A-published
//! envelope becomes an orphan — no `TxRef` on B will point at it. Downstream
//! consumers MUST tolerate unreferenced A entries; this is the agreed cost
//! of dual writes in the split architecture.)
//!
//! In addition the sequencer publishes [`DuplicateNotification`]s for
//! past-nonce txs on the **receipt-cache** channel
//! ([`ReceiptCachePublisher`]).
//!
//! All three surfaces are traits so unit tests can use the in-memory fakes
//! (no Aeron media driver required); production wiring binds them to the
//! real `kardamom_log::publisher::{ChannelAPublisher, ChannelBPublisher,
//! ReceiptCachePublisher}` types.

use kardamom_types::{BPosition, TxRef};

use crate::duplicate::DuplicateNotification;
use crate::error::SequencerError;

/// Channel A[i] publisher contract — the sequencer's *own* per-sequencer
/// archive of full `TxEnvelope` bytes.
///
/// `try_publish` must return `Err(SequencerError::Backpressure)` if the
/// underlying transport is blocked; the primary sequencer relies on that
/// signal to rewind its state machine and retry without skipping nonces.
/// On success it returns the fragment's start `BPosition` on the A-stream,
/// which the caller then embeds in a [`TxRef`] for the channel-B publish.
pub trait ChannelAPublisher: Send {
    fn try_publish(&mut self, envelope_bytes: &[u8]) -> Result<BPosition, SequencerError>;
}

/// Channel B publisher contract — the canonical orderer. Publishes tiny
/// [`TxRef`] records (~16 B) into Aeron's concurrent multi-publisher
/// stream.
///
/// As with [`ChannelAPublisher`], a blocked transport must surface as
/// `Err(SequencerError::Backpressure)` so the state machine can rewind.
pub trait ChannelBRefPublisher: Send {
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

    use kardamom_types::{BPosition, TxRef};

    use super::*;

    /// Aeron term-buffer length the in-memory channel-A fake mimics when
    /// packing `(byte_offset, len)` writes into `(term_id, term_offset)`
    /// fragment positions. 16 MiB matches the Media Driver default
    /// (`AERON_TERM_BUFFER_LENGTH_DEFAULT`).
    const TERM_LEN: i64 = 16 * 1024 * 1024;

    /// In-memory channel-A publisher. Records every envelope-bytes write
    /// and returns a monotonically-increasing `BPosition` so the
    /// sequencer's per-A cursor advances exactly as it would on a real
    /// Aeron channel. The `sequencer_id` is carried so `TxRef`s built
    /// against this publisher are self-consistent in tests.
    #[derive(Clone)]
    pub struct InMemoryChannelAPublisher {
        pub sequencer_id: u8,
        pub published: Arc<Mutex<Vec<Vec<u8>>>>,
        pub fail_with_backpressure: Arc<Mutex<bool>>,
        next_offset: Arc<Mutex<i64>>,
    }

    impl InMemoryChannelAPublisher {
        pub fn new(sequencer_id: u8) -> Self {
            Self {
                sequencer_id,
                published: Arc::new(Mutex::new(Vec::new())),
                fail_with_backpressure: Arc::new(Mutex::new(false)),
                next_offset: Arc::new(Mutex::new(0)),
            }
        }

        /// Read back the envelope bytes published at a particular `BPosition`
        /// (the same value the sequencer handed to the B-publisher as
        /// `TxRef.position_a`). Returns `None` if the position does not
        /// match any recorded write. Used by integration tests that mirror
        /// the executor's A-lookup path.
        pub fn fetch(&self, position_a: BPosition) -> Option<Vec<u8>> {
            let target = (position_a.term_id as i64) * TERM_LEN + position_a.term_offset as i64;
            let mut cur: i64 = 0;
            for bytes in self.published.lock().unwrap().iter() {
                if cur == target {
                    return Some(bytes.clone());
                }
                cur += bytes.len() as i64;
            }
            None
        }
    }

    impl ChannelAPublisher for InMemoryChannelAPublisher {
        fn try_publish(&mut self, envelope_bytes: &[u8]) -> Result<BPosition, SequencerError> {
            if *self.fail_with_backpressure.lock().unwrap() {
                return Err(SequencerError::Backpressure);
            }
            let mut off = self.next_offset.lock().unwrap();
            let start = *off;
            self.published.lock().unwrap().push(envelope_bytes.to_vec());
            *off += envelope_bytes.len() as i64;
            Ok(BPosition {
                term_id: (start / TERM_LEN) as i32,
                term_offset: (start % TERM_LEN) as i32,
            })
        }
    }

    /// In-memory channel-B `TxRef` publisher. Records every published ref
    /// in arrival order so tests can assert the canonical sequence.
    #[derive(Default, Clone)]
    pub struct InMemoryChannelBRefPublisher {
        pub refs: Arc<Mutex<Vec<TxRef>>>,
        pub fail_with_backpressure: Arc<Mutex<bool>>,
    }

    impl ChannelBRefPublisher for InMemoryChannelBRefPublisher {
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
    use alloy_primitives::Address;
    use kardamom_types::TxRef;

    #[test]
    fn fake_a_records_bytes_and_returns_distinct_positions() {
        let mut p = InMemoryChannelAPublisher::new(0);
        let pos1 = p.try_publish(&[1, 2, 3]).unwrap();
        let pos2 = p.try_publish(&[4, 5]).unwrap();
        assert_ne!(pos1, pos2);
        assert_eq!(p.published.lock().unwrap().len(), 2);
        // Roundtrip lookup by position.
        assert_eq!(p.fetch(pos1).as_deref(), Some(&[1u8, 2, 3][..]));
        assert_eq!(p.fetch(pos2).as_deref(), Some(&[4u8, 5][..]));
    }

    #[test]
    fn fake_a_can_simulate_backpressure() {
        let mut p = InMemoryChannelAPublisher::new(0);
        *p.fail_with_backpressure.lock().unwrap() = true;
        assert!(matches!(
            p.try_publish(&[0]),
            Err(SequencerError::Backpressure)
        ));
    }

    #[test]
    fn fake_b_records_refs() {
        let mut p = InMemoryChannelBRefPublisher::default();
        p.try_publish_ref(&TxRef::new(0, BPosition::default()))
            .unwrap();
        p.try_publish_ref(&TxRef::new(1, BPosition::default()))
            .unwrap();
        assert_eq!(p.refs.lock().unwrap().len(), 2);
    }

    #[test]
    fn fake_b_can_simulate_backpressure() {
        let mut p = InMemoryChannelBRefPublisher::default();
        *p.fail_with_backpressure.lock().unwrap() = true;
        assert!(matches!(
            p.try_publish_ref(&TxRef::new(0, BPosition::default())),
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

//! Outbound channel abstractions.
//!
//! Under the MDS topology, the sequencer does not publish to tx_data.
//! That is the proxy's job. The sequencer only reads A and publishes
//! B-refs: for each envelope it sees on its shard's tx_data (through
//! [`crate::inbound::TxDataSubscriber`]), if the nonce gate matches, it
//! publishes a tiny [`kardamom_types::TxRef`] onto the canonical orderer
//! tx_ordering ([`TxOrderingRefPublisher`]). This is an Aeron concurrent
//! multi-publisher stream, ordered together with refs from the other P-1
//! sequencers in this shard's group and sealer-emitted
//! `BlockBoundaryStart` markers.
//!
//! Pending-buffer state advances once the B publish succeeds. If B applies
//! backpressure, the state machine rewinds through
//! [`crate::state::PartitionState::reinsert_for_retry`], and the error
//! bubbles up.
//!
//! The sequencer also emits a [`types::TxError`] on the dedicated
//! `tx_errors` channel ([`TxErrorPublisher`]) when an inbound transaction
//! fails the nonce gate (today: past-nonce or duplicate; more variants may
//! come later). Ingress reads that channel and releases the parked client
//! right away with a JSON-RPC error.
//!
//! All surfaces are traits, so unit tests can use the in-memory fakes (no
//! Aeron media driver needed). Production wiring binds them to the real
//! `kardamom_log::publisher` types.

pub mod cluster;

use alloy_primitives::Address;
use kardamom_types::xchain::RemoteEpochRecord;
use kardamom_types::{EpochRecord, TxError, TxRef};

use crate::error::SequencerError;

/// TxOrdering publisher contract, the canonical orderer. Publishes tiny
/// [`TxRef`]s for L2 transactions (about 41 bytes) into Aeron's concurrent
/// multi-publisher stream, plus whole [`EpochRecord`]s for L1 epochs and
/// [`RemoteEpochRecord`]s for peer chains. Every lane shares the
/// tx_ordering channel, so all three interleave in one canonical order.
///
/// A blocked transport must surface as `Err(SequencerError::Backpressure)`,
/// so the state machine can rewind.
pub trait TxOrderingRefPublisher: Send {
    /// `sender` and `nonce` ride the ingress frame's guard header, for
    /// the sealer's per-sender contiguity guard. They are not part of the
    /// relayed record.
    fn try_publish_ref(
        &mut self,
        r: &TxRef,
        sender: Address,
        nonce: u64,
    ) -> Result<(), SequencerError>;

    /// Publish a run of refs. This amortizes per-offer overhead where the
    /// transport supports it. Returns `(published, error)`: the first
    /// `published` refs are durably offered, and an error applies to the
    /// rest. The default loops over single refs (fakes and non-batching
    /// transports keep exact semantics). The cluster transport packs the
    /// whole slice into one `KIND_BATCH` app message (all-or-nothing per
    /// call).
    fn try_publish_ref_batch(
        &mut self,
        refs: &[(TxRef, Address, u64)],
    ) -> (usize, Option<SequencerError>) {
        for (i, (r, sender, nonce)) in refs.iter().enumerate() {
            if let Err(e) = self.try_publish_ref(r, *sender, *nonce) {
                return (i, Some(e));
            }
        }
        (refs.len(), None)
    }

    /// Publish an [`EpochRecord`] observed on `tx_deposits` as an
    /// origin-advancing record. The sealer closes the open block, adopts
    /// the epoch's L1 number, then relays it, so the epoch's deposits lead
    /// a block. This has the same backpressure semantics as
    /// `try_publish_ref`. Epochs are not nonce-gated and have no pending
    /// state to rewind, so on `Backpressure` the caller retries the same
    /// epoch on the next tick.
    fn try_publish_epoch(&mut self, e: &EpochRecord) -> Result<(), SequencerError>;

    /// Publish a [`RemoteEpochRecord`] observed on `tx_remote_epochs` as a
    /// REMOTE-ORIGIN-ADVANCING record: [`Self::try_publish_epoch`] for a peer
    /// Kardamom chain rather than L1, so the sealer tracks the adopted
    /// position per origin chain instead of stamping it into boundaries.
    /// Same backpressure semantics — nothing is nonce-gated and there is no
    /// pending state to rewind, so on `Backpressure` the caller retries the
    /// same record next tick.
    fn try_publish_remote_epoch(&mut self, r: &RemoteEpochRecord) -> Result<(), SequencerError>;
}

/// TxErrors channel publisher. This is best-effort: the caller logs
/// errors and does not propagate them. The canonical state has already
/// advanced, or the inbound transaction was rejected, so there is nothing
/// to roll back.
pub trait TxErrorPublisher: Send {
    fn publish_error(&mut self, e: TxError);
}

// ===========================================================================
// In-memory fakes for unit / integration tests.
// ===========================================================================

#[cfg(any(test, feature = "testing"))]
pub mod fakes {
    use std::sync::{Arc, Mutex};

    use kardamom_types::xchain::RemoteEpochRecord;
    use kardamom_types::{EpochRecord, TxRef};

    use super::*;

    /// In-memory tx_ordering publisher. Records every published `TxRef`,
    /// `EpochRecord`, and `RemoteEpochRecord` in arrival order, so tests
    /// can check the canonical sequence.
    #[derive(Default, Clone)]
    pub struct InMemoryTxOrderingRefPublisher {
        pub refs: Arc<Mutex<Vec<TxRef>>>,
        pub epochs: Arc<Mutex<Vec<EpochRecord>>>,
        pub remote_epochs: Arc<Mutex<Vec<RemoteEpochRecord>>>,
        pub fail_with_backpressure: Arc<Mutex<bool>>,
    }

    impl TxOrderingRefPublisher for InMemoryTxOrderingRefPublisher {
        fn try_publish_ref(
            &mut self,
            r: &TxRef,
            _sender: alloy_primitives::Address,
            _nonce: u64,
        ) -> Result<(), SequencerError> {
            if *self.fail_with_backpressure.lock().unwrap() {
                return Err(SequencerError::Backpressure);
            }
            self.refs.lock().unwrap().push(*r);
            Ok(())
        }

        fn try_publish_epoch(&mut self, e: &EpochRecord) -> Result<(), SequencerError> {
            if *self.fail_with_backpressure.lock().unwrap() {
                return Err(SequencerError::Backpressure);
            }
            self.epochs.lock().unwrap().push(e.clone());
            Ok(())
        }

        fn try_publish_remote_epoch(
            &mut self,
            r: &RemoteEpochRecord,
        ) -> Result<(), SequencerError> {
            if *self.fail_with_backpressure.lock().unwrap() {
                return Err(SequencerError::Backpressure);
            }
            self.remote_epochs.lock().unwrap().push(r.clone());
            Ok(())
        }
    }

    #[derive(Default, Clone)]
    pub struct InMemoryTxErrorPublisher {
        pub errors: Arc<Mutex<Vec<TxError>>>,
    }

    impl TxErrorPublisher for InMemoryTxErrorPublisher {
        fn publish_error(&mut self, e: TxError) {
            self.errors.lock().unwrap().push(e);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::fakes::*;
    use super::*;
    use alloy_primitives::{Address, B256};
    use kardamom_types::{BPosition, TxError, TxErrorReason, TxRef};

    #[test]
    fn fake_b_records_refs() {
        let mut p = InMemoryTxOrderingRefPublisher::default();
        p.try_publish_ref(
            &TxRef::new(B256::ZERO, 0, BPosition::default(), 0),
            Address::ZERO,
            0,
        )
        .unwrap();
        p.try_publish_ref(
            &TxRef::new(B256::ZERO, 1, BPosition::default(), 0),
            Address::ZERO,
            1,
        )
        .unwrap();
        assert_eq!(p.refs.lock().unwrap().len(), 2);
    }

    #[test]
    fn fake_b_can_simulate_backpressure() {
        let mut p = InMemoryTxOrderingRefPublisher::default();
        *p.fail_with_backpressure.lock().unwrap() = true;
        assert!(matches!(
            p.try_publish_ref(
                &TxRef::new(B256::ZERO, 0, BPosition::default(), 0),
                Address::ZERO,
                0
            ),
            Err(SequencerError::Backpressure)
        ));
    }

    #[test]
    fn fake_tx_error_records_emissions() {
        let mut p = InMemoryTxErrorPublisher::default();
        p.publish_error(TxError {
            sender: Address::repeat_byte(0xAB),
            nonce: 7,
            reason: TxErrorReason::DuplicatedTx { expected_nonce: 10 },
        });
        assert_eq!(p.errors.lock().unwrap().len(), 1);
    }

    fn test_epoch(n: u64) -> EpochRecord {
        EpochRecord {
            l1_number: n,
            l1_hash: B256::repeat_byte(n as u8),
            deposits: Vec::new(),
        }
    }

    #[test]
    fn fake_b_records_epochs() {
        let mut p = InMemoryTxOrderingRefPublisher::default();
        p.try_publish_epoch(&test_epoch(7)).unwrap();
        assert_eq!(p.epochs.lock().unwrap().len(), 1);
    }

    #[test]
    fn fake_b_epochs_back_off_under_pressure() {
        let mut p = InMemoryTxOrderingRefPublisher::default();
        *p.fail_with_backpressure.lock().unwrap() = true;
        assert!(matches!(
            p.try_publish_epoch(&test_epoch(1)),
            Err(SequencerError::Backpressure)
        ));
    }
}

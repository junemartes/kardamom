//! Outbound channel abstractions.
//!
//! Under the MDS topology the sequencer **does not** publish
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
//! In addition the sequencer emits a [`types::TxError`] on the dedicated
//! `tx_errors` channel ([`TxErrorPublisher`]) when an inbound tx fails the
//! nonce gate (past-nonce / duplicate today; more variants in the future).
//! Ingress consumes that channel and releases the parked client immediately
//! with a JSON-RPC error.
//!
//! All surfaces are traits so unit tests can use the in-memory fakes
//! (no Aeron media driver required); production wiring binds them to the
//! real `kardamom_log::publisher` types.

use kardamom_types::{DepositRef, TxError, TxRef};

use crate::error::SequencerError;

/// TxOrdering publisher contract — the canonical orderer. Publishes tiny
/// reference records into Aeron's concurrent multi-publisher stream: regular
/// [`TxRef`]s for L2 txs (~41 B) and [`DepositRef`]s for L1 deposits
/// (~36 B). Both lanes share the same tx_ordering channel so deposits and
/// regular txs interleave in canonical order.
///
/// A blocked transport must surface as `Err(SequencerError::Backpressure)`
/// so the state machine can rewind.
pub trait TxOrderingRefPublisher: Send {
    fn try_publish_ref(&mut self, r: &TxRef) -> Result<(), SequencerError>;

    /// Publish a [`DepositRef`] for a deposit observed on `tx_deposits`.
    /// Same backpressure semantics as `try_publish_ref` — deposits aren't
    /// nonce-gated and have no pending state to rewind, so on `Backpressure`
    /// the caller just retries the same ref next tick.
    fn try_publish_deposit_ref(&mut self, r: &DepositRef) -> Result<(), SequencerError>;
}

/// TxErrors channel publisher. Best-effort: errors are logged by the
/// caller and not propagated, because the canonical state has already
/// advanced (or the inbound tx was rejected and there's nothing to roll
/// back).
pub trait TxErrorPublisher: Send {
    fn publish_error(&mut self, e: TxError);
}

// ===========================================================================
// In-memory fakes for unit / integration tests.
// ===========================================================================

#[cfg(any(test, feature = "testing"))]
pub mod fakes {
    use std::sync::{Arc, Mutex};

    use kardamom_types::{DepositRef, TxRef};

    use super::*;

    /// In-memory tx_ordering publisher. Records every published `TxRef` and
    /// `DepositRef` in arrival order so tests can assert the canonical
    /// sequence.
    #[derive(Default, Clone)]
    pub struct InMemoryTxOrderingRefPublisher {
        pub refs: Arc<Mutex<Vec<TxRef>>>,
        pub deposit_refs: Arc<Mutex<Vec<DepositRef>>>,
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

        fn try_publish_deposit_ref(&mut self, r: &DepositRef) -> Result<(), SequencerError> {
            if *self.fail_with_backpressure.lock().unwrap() {
                return Err(SequencerError::Backpressure);
            }
            self.deposit_refs.lock().unwrap().push(*r);
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
    use kardamom_types::{BPosition, DepositRef, TxError, TxErrorReason, TxRef};

    #[test]
    fn fake_b_records_refs() {
        let mut p = InMemoryTxOrderingRefPublisher::default();
        p.try_publish_ref(&TxRef::new(B256::ZERO, 0, BPosition::default(), 0))
            .unwrap();
        p.try_publish_ref(&TxRef::new(B256::ZERO, 1, BPosition::default(), 0))
            .unwrap();
        assert_eq!(p.refs.lock().unwrap().len(), 2);
    }

    #[test]
    fn fake_b_can_simulate_backpressure() {
        let mut p = InMemoryTxOrderingRefPublisher::default();
        *p.fail_with_backpressure.lock().unwrap() = true;
        assert!(matches!(
            p.try_publish_ref(&TxRef::new(B256::ZERO, 0, BPosition::default(), 0)),
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

    #[test]
    fn fake_b_records_deposit_refs() {
        let mut p = InMemoryTxOrderingRefPublisher::default();
        p.try_publish_deposit_ref(&DepositRef::new(
            B256::repeat_byte(0x77),
            BPosition::default(),
        ))
        .unwrap();
        assert_eq!(p.deposit_refs.lock().unwrap().len(), 1);
    }

    #[test]
    fn fake_b_deposit_refs_back_off_under_pressure() {
        let mut p = InMemoryTxOrderingRefPublisher::default();
        *p.fail_with_backpressure.lock().unwrap() = true;
        assert!(matches!(
            p.try_publish_deposit_ref(&DepositRef::new(B256::ZERO, BPosition::default())),
            Err(SequencerError::Backpressure)
        ));
    }
}

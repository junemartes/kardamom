//! Epoch-side of the sequencer.
//!
//! The DA watcher publishes one [`types::EpochRecord`] per finalized L1
//! block onto the dedicated `tx_deposits` Aeron channel. This includes
//! blocks with no deposits. Each of the M sequencers subscribes to that
//! channel and forwards the epoch verbatim onto the canonical orderer
//! `tx_ordering`, as an origin-advancing record.
//!
//! All M sequencers race on the multi-publisher `tx_ordering` stream, so
//! each epoch is offered M times. The cluster's first-seen dedup collapses
//! them on `canonical_id = keccak(l1_hash)`. Racing producers derive this
//! id identically from the same L1 block.
//!
//! The deposits travel inside the record itself, so there is no
//! ref-to-envelope join for a consumer to time out on: a lost
//! `tx_deposits` fragment cannot strand a deposit.
//!
//! Epochs are not nonce-gated. They carry OP `source_hash` values and have
//! no state-machine interaction in the sequencer. The code path is a
//! simple poll-and-publish pump. It runs independently of the nonce-gated
//! tx_data-to-TxRef path in [`crate::sequencer`].

use kardamom_log::aeron_live::TxDepositsSubscriberHandle;
use kardamom_types::{BPosition, EpochRecord};

use crate::error::SequencerError;
use crate::outbound::TxOrderingRefPublisher;
use crate::pump::{OriginLane, Pump};

/// Subscription surface that the epoch pump reads from. Production wiring
/// binds this to the real `log::TxDepositsSubscriber`. Tests use the
/// in-memory fake in [`fakes`].
pub trait EpochSubscriber: Send {
    /// Poll for one epoch. Returns:
    /// * `Ok(Some((pos, epoch)))`: an epoch was available.
    /// * `Ok(None)`: no fragment is ready now. The caller should back off.
    /// * `Err(SequencerError::IngressDisconnected)`: the subscription is closed.
    ///
    /// # Errors
    ///
    /// Returns [`SequencerError::IngressDisconnected`] when the
    /// subscription is closed.
    fn poll(&mut self) -> Result<Option<(BPosition, EpochRecord)>, SequencerError>;
}

/// The live adapter: a miss is not an error, the pump backs off.
impl EpochSubscriber for TxDepositsSubscriberHandle {
    fn poll(&mut self) -> Result<Option<(BPosition, EpochRecord)>, SequencerError> {
        Ok(self.try_recv())
    }
}

/// The epoch lane. Takes the held epoch if there is one, else pulls one
/// epoch off the subscription, and forwards it on `tx_ordering`.
///
/// On `SequencerError::Backpressure` the epoch goes into the held slot,
/// and the next call retries the SAME epoch before it polls for a new
/// one. The poll is destructive (`try_recv`), so without this slot a
/// backpressured epoch would be lost, and the L1 origin sequence would
/// have a permanent hole. `Backpressure` includes "not connected", so a
/// leader election would otherwise drain every sequencer's backlog at
/// once.
///
/// Epochs carry no metric to bump on relay (unlike remote epochs, see
/// [`crate::remote_epoch`]), so this is [`Pump::step`] with a no-op
/// `on_relayed` hook.
impl<S, P> OriginLane<S, P> for Pump<EpochRecord>
where
    S: EpochSubscriber,
    P: TxOrderingRefPublisher,
{
    fn relay(&mut self, sub: &mut S, publ: &mut P) -> Result<bool, SequencerError> {
        self.step(|| sub.poll(), |epoch| publ.try_publish_epoch(epoch), |_| {})
    }
}

#[cfg(any(test, feature = "testing"))]
pub mod fakes {
    use super::{BPosition, EpochRecord, EpochSubscriber, SequencerError};
    use crate::fakes::ScriptedQueue;

    /// In-memory [`EpochSubscriber`] driven by a scripted queue. Push test
    /// inputs with `ScriptedEpochs::push`. The sequencer drains them in
    /// first-in-first-out order.
    pub type ScriptedEpochs = ScriptedQueue<EpochRecord>;

    impl EpochSubscriber for ScriptedEpochs {
        fn poll(&mut self) -> Result<Option<(BPosition, EpochRecord)>, SequencerError> {
            self.poll_next()
        }
    }
}

#[cfg(test)]
mod tests {
    use alloy_primitives::B256;

    use super::fakes::ScriptedEpochs;
    use super::*;
    use crate::outbound::fakes::InMemoryTxOrderingRefPublisher;

    fn epoch(n: u64, deposits: usize) -> EpochRecord {
        EpochRecord {
            l1_number: n,
            l1_hash: B256::repeat_byte(u8::try_from(n).unwrap()),
            deposits: (0..deposits)
                .map(|i| kardamom_types::Deposit {
                    source_hash: B256::repeat_byte(0xD0 + u8::try_from(i).unwrap()),
                    mint: 100 + u128::try_from(i).unwrap(),
                    ..Default::default()
                })
                .collect(),
        }
    }

    #[test]
    fn forwards_the_epoch_verbatim() {
        // The sequencer must not re-derive or reorder anything. It orders
        // exactly what the watcher derived from L1. Otherwise the producer
        // and verifier could disagree.
        let mut sub = ScriptedEpochs::default();
        let mut pubr = InMemoryTxOrderingRefPublisher::default();
        let e = epoch(100, 3);
        sub.push(BPosition::default(), e.clone());

        assert!(Pump::default().relay(&mut sub, &mut pubr).unwrap());

        let got = pubr.epochs.lock().unwrap();
        assert_eq!(got.len(), 1);
        assert_eq!(got[0], e);
    }

    #[test]
    fn empty_epochs_are_forwarded_too() {
        // A depositless epoch is the one most tempting to drop. Without
        // testing it, the no-skipping rule is not enforced.
        let mut sub = ScriptedEpochs::default();
        let mut pubr = InMemoryTxOrderingRefPublisher::default();
        sub.push(BPosition::default(), epoch(101, 0));

        assert!(Pump::default().relay(&mut sub, &mut pubr).unwrap());
        assert_eq!(pubr.epochs.lock().unwrap().len(), 1);
    }

    /// Idle, closed, backpressure-holds-the-record, retry-does-not-poll-
    /// past-the-held-record, and relay-after-backpressure-clears: the
    /// contract every `ScriptedQueue<T>`-backed pump shares.
    #[test]
    fn epoch_pump_honors_the_shared_contract() {
        crate::fakes::pump_contract::run(&epoch(102, 1), &epoch(103, 0));
    }
}

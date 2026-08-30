//! Epoch-side of the sequencer.
//!
//! The DA watcher publishes one [`types::EpochRecord`] per finalized L1
//! block onto the dedicated `tx_deposits` Aeron channel. This includes
//! blocks with no deposits. Each of the M sequencers subscribes to that
//! channel and forwards the epoch verbatim onto the canonical orderer
//! `tx_ordering`, as an origin-advancing record.
//!
//! All M sequencers race on the multi-publisher tx_ordering stream, so
//! each epoch is offered M times. The cluster's first-seen dedup collapses
//! them on `canonical_id = keccak(l1_hash)`. Racing producers derive this
//! id identically from the same L1 block.
//!
//! Unlike the `DepositRef` scheme this replaces, the deposits travel
//! inside the record. So there is no ref-to-envelope join for a consumer
//! to time out on: a lost `tx_deposits` fragment can no longer strand a
//! deposit.
//!
//! Epochs are not nonce-gated. They carry OP `source_hash` values and have
//! no state-machine interaction in the sequencer. The code path is a
//! simple poll-and-publish pump. It runs independently of the nonce-gated
//! tx_data-to-TxRef path in [`crate::sequencer`].

use kardamom_types::{BPosition, EpochRecord};

use crate::error::SequencerError;
use crate::outbound::TxOrderingRefPublisher;

/// Subscription surface that the epoch pump reads from. Production wiring
/// binds this to the real `log::TxDepositsSubscriber`. Tests use the
/// in-memory fake in [`fakes`].
pub trait EpochSubscriber: Send {
    /// Poll for one epoch. Returns:
    /// * `Ok(Some((pos, epoch)))`: an epoch was available.
    /// * `Ok(None)`: no fragment is ready now. The caller should back off.
    /// * `Err(SequencerError::IngressDisconnected)`: the subscription is closed.
    fn poll(&mut self) -> Result<Option<(BPosition, EpochRecord)>, SequencerError>;
}

/// Single-step epoch pump. Pulls one epoch off the subscription and
/// forwards it on `tx_ordering`. Returns `Ok(true)` if it processed an
/// epoch (the caller should keep going), or `Ok(false)` if the
/// subscription is idle.
///
/// On `SequencerError::Backpressure`, the caller retries the same epoch on
/// the next tick. The epoch is durable on the deposits stream, so there is
/// no rewind state to manage.
pub fn process_epoch<S, P>(sub: &mut S, b: &mut P) -> Result<bool, SequencerError>
where
    S: EpochSubscriber,
    P: TxOrderingRefPublisher,
{
    let Some((_pos, epoch)) = sub.poll()? else {
        return Ok(false);
    };
    b.try_publish_epoch(&epoch)?;
    Ok(true)
}

#[cfg(any(test, feature = "testing"))]
pub mod fakes {
    use std::collections::VecDeque;
    use std::sync::{Arc, Mutex};

    use super::*;

    /// In-memory [`EpochSubscriber`] driven by a scripted queue. Push test
    /// inputs with [`ScriptedEpochs::push`]. The sequencer drains them in
    /// first-in-first-out order.
    #[derive(Default, Clone)]
    pub struct ScriptedEpochs {
        pub queue: Arc<Mutex<VecDeque<(BPosition, EpochRecord)>>>,
        pub closed: Arc<Mutex<bool>>,
    }

    impl ScriptedEpochs {
        pub fn push(&self, pos: BPosition, epoch: EpochRecord) {
            self.queue.lock().unwrap().push_back((pos, epoch));
        }

        pub fn close(&self) {
            *self.closed.lock().unwrap() = true;
        }
    }

    impl EpochSubscriber for ScriptedEpochs {
        fn poll(&mut self) -> Result<Option<(BPosition, EpochRecord)>, SequencerError> {
            if let Some(item) = self.queue.lock().unwrap().pop_front() {
                return Ok(Some(item));
            }
            if *self.closed.lock().unwrap() {
                return Err(SequencerError::IngressDisconnected);
            }
            Ok(None)
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
            l1_hash: B256::repeat_byte(n as u8),
            deposits: (0..deposits)
                .map(|i| kardamom_types::Deposit {
                    source_hash: B256::repeat_byte(0xD0 + i as u8),
                    mint: 100 + i as u128,
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

        assert!(process_epoch(&mut sub, &mut pubr).unwrap());

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

        assert!(process_epoch(&mut sub, &mut pubr).unwrap());
        assert_eq!(pubr.epochs.lock().unwrap().len(), 1);
    }

    #[test]
    fn idle_subscription_reports_no_work() {
        let mut sub = ScriptedEpochs::default();
        let mut pubr = InMemoryTxOrderingRefPublisher::default();
        assert!(!process_epoch(&mut sub, &mut pubr).unwrap());
    }

    #[test]
    fn closed_subscription_surfaces_disconnect() {
        let mut sub = ScriptedEpochs::default();
        sub.close();
        let mut pubr = InMemoryTxOrderingRefPublisher::default();
        assert!(matches!(
            process_epoch(&mut sub, &mut pubr),
            Err(SequencerError::IngressDisconnected)
        ));
    }

    #[test]
    fn backpressure_propagates_so_the_caller_retries() {
        let mut sub = ScriptedEpochs::default();
        let mut pubr = InMemoryTxOrderingRefPublisher::default();
        *pubr.fail_with_backpressure.lock().unwrap() = true;
        sub.push(BPosition::default(), epoch(102, 1));

        assert!(matches!(
            process_epoch(&mut sub, &mut pubr),
            Err(SequencerError::Backpressure)
        ));
        assert!(pubr.epochs.lock().unwrap().is_empty());
    }
}

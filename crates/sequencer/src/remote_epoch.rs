//! Remote-epoch side of the sequencer — [`crate::epoch`] with a peer Kardamom
//! chain as the origin instead of L1.
//!
//! The interop watcher publishes one [`RemoteEpochRecord`] per peer-origin
//! block that carried cross-chain messages onto the dedicated
//! `tx_remote_epochs` Aeron channel. Each of the M sequencers subscribes and
//! forwards the record VERBATIM onto `tx_ordering` as a
//! remote-origin-advancing record.
//!
//! All M sequencers race on the same stream, so each record is offered M
//! times; the cluster's first-seen dedup collapses them on
//! `RemoteEpochRecord::canonical_id`, which racing producers derive
//! identically from the same feed prefix.
//!
//! Two differences from the L1 epoch pump, both inherited from the derivation
//! rule rather than from this code:
//!
//! * there is no empty record — a remote origin advances only when it has
//!   messages, so the no-skip rule is enforced on the pair's dense `seq`
//!   rather than on origin blocks;
//! * the origin is a PAIR (chain id + anchor), so records from different
//!   peers are independent and interleave freely here.
//!
//! Remote epochs are not nonce-gated and have no state-machine interaction, so
//! this is the same poll → publish pump the epoch path is, running independent
//! of both it and the `tx_data` → `TxRef` path.

use kardamom_log::aeron_live::TxRemoteEpochsSubscriberHandle;
use kardamom_types::BPosition;
use kardamom_types::xchain::RemoteEpochRecord;

use crate::error::SequencerError;
use crate::outbound::TxOrderingRefPublisher;
use crate::pump::Pump;

/// Subscription surface the remote-epoch pump reads from. Production wiring
/// binds this to `kardamom_log`'s `TxRemoteEpochsSubscriberHandle`; tests use
/// the in-memory fake in [`fakes`].
pub trait RemoteEpochSubscriber: Send {
    /// Poll for one record. Returns:
    /// * `Ok(Some((pos, record)))` — a record was available and is yielded.
    /// * `Ok(None)` — no fragment available right now (caller should back off).
    /// * `Err(SequencerError::IngressDisconnected)` — subscription closed.
    ///
    /// # Errors
    ///
    /// Returns [`SequencerError::IngressDisconnected`] when the
    /// subscription is closed.
    fn poll(&mut self) -> Result<Option<(BPosition, RemoteEpochRecord)>, SequencerError>;
}

/// The live adapter: a miss is not an error, the pump backs off.
impl RemoteEpochSubscriber for TxRemoteEpochsSubscriberHandle {
    fn poll(&mut self) -> Result<Option<(BPosition, RemoteEpochRecord)>, SequencerError> {
        Ok(self.try_recv())
    }
}

/// Single-step remote-epoch pump: take the held record if there is one,
/// else pull one record off the subscription, and forward it on
/// `tx_ordering`. Returns `Ok(true)` if a record was processed (caller
/// should keep going), `Ok(false)` if the subscription is idle.
///
/// On `SequencerError::Backpressure` the record goes into `pump`'s held
/// slot, and the next call retries the SAME record before it polls for a
/// new one. The poll is destructive (`try_recv`), so without this slot a
/// backpressured record would be lost. The watcher persists its cursor
/// after the media-driver ack, so it would never re-publish the record,
/// and the pair's lane would have a permanent hole.
/// `Backpressure` includes "not connected", so a leader election would
/// otherwise drain every sequencer's backlog at once.
///
/// This is [`Pump::step`] with an `on_relayed` hook that bumps the
/// relayed-record metric (unlike epochs, see
/// [`crate::epoch::process_epoch`], which have no such metric).
///
/// # Errors
///
/// Returns the subscription's error if the poll fails, or the
/// publisher's error (including [`SequencerError::Backpressure`]) if the
/// publish fails.
pub fn process_remote_epoch<S, P>(
    sub: &mut S,
    b: &mut P,
    pump: &mut Pump<RemoteEpochRecord>,
) -> Result<bool, SequencerError>
where
    S: RemoteEpochSubscriber,
    P: TxOrderingRefPublisher,
{
    pump.step(
        || sub.poll(),
        |record| b.try_publish_remote_epoch(record),
        |record| {
            crate::metrics::record_remote_epoch_relayed(
                record.origin_chain_id,
                record.messages.len().get(),
            );
        },
    )
}

#[cfg(any(test, feature = "testing"))]
pub mod fakes {
    use super::{BPosition, RemoteEpochRecord, RemoteEpochSubscriber, SequencerError};
    use crate::fakes::ScriptedQueue;

    /// In-memory [`RemoteEpochSubscriber`] driven by a scripted queue. Push
    /// test inputs via `ScriptedRemoteEpochs::push`; the sequencer drains
    /// them FIFO.
    pub type ScriptedRemoteEpochs = ScriptedQueue<RemoteEpochRecord>;

    impl RemoteEpochSubscriber for ScriptedRemoteEpochs {
        fn poll(&mut self) -> Result<Option<(BPosition, RemoteEpochRecord)>, SequencerError> {
            self.poll_next()
        }
    }
}

#[cfg(test)]
mod tests {
    use alloy_primitives::B256;
    use kardamom_types::xchain::XChainMessage;

    use super::fakes::ScriptedRemoteEpochs;
    use super::*;
    use crate::outbound::fakes::InMemoryTxOrderingRefPublisher;

    /// `message_count` must be at least 1: `RemoteEpochRecord::messages`
    /// is a [`kardamom_types::xchain::NonEmptyVec`].
    fn record(origin: u64, first_seq: u64, message_count: usize) -> RemoteEpochRecord {
        let message = |i: usize| {
            let i = u64::try_from(i).unwrap();
            XChainMessage {
                source_hash: B256::repeat_byte(u8::try_from(0xE0 + i).unwrap()),
                seq: first_seq + i,
                gas_limit: 100_000,
                ..Default::default()
            }
        };
        let rest = (1..message_count).map(message).collect();
        RemoteEpochRecord {
            origin_chain_id: origin,
            anchor_number: 100 + first_seq,
            anchor_hash: B256::repeat_byte(u8::try_from(first_seq).unwrap()),
            first_seq,
            messages: kardamom_types::xchain::NonEmptyVec::new(message(0), rest),
        }
    }

    #[test]
    fn forwards_the_record_verbatim() {
        // Re-deriving here instead of forwarding would break the one property
        // dedup rests on: racing sequencers must offer byte-identical records,
        // and the destination's verifier re-runs the SAME rule the watcher did.
        let mut sub = ScriptedRemoteEpochs::default();
        let mut pubr = InMemoryTxOrderingRefPublisher::default();
        let r = record(412_346, 7, 3);
        sub.push(BPosition::default(), r.clone());

        assert!(process_remote_epoch(&mut sub, &mut pubr, &mut Pump::default()).unwrap());

        let got = pubr.remote_epochs.lock().unwrap();
        assert_eq!(got.len(), 1);
        assert_eq!(got[0], r);
        assert_eq!(got[0].canonical_id(), r.canonical_id());
    }

    #[test]
    fn records_from_different_origins_interleave() {
        // Peers are independent fault domains; the pump must not serialise or
        // order them relative to each other.
        let mut sub = ScriptedRemoteEpochs::default();
        let mut pubr = InMemoryTxOrderingRefPublisher::default();
        sub.push(BPosition::default(), record(412_346, 0, 1));
        sub.push(BPosition::default(), record(412_999, 0, 1));
        sub.push(BPosition::default(), record(412_346, 1, 1));

        for _ in 0..3 {
            assert!(process_remote_epoch(&mut sub, &mut pubr, &mut Pump::default()).unwrap());
        }
        let got = pubr.remote_epochs.lock().unwrap();
        assert_eq!(
            got.iter().map(|r| r.origin_chain_id).collect::<Vec<_>>(),
            vec![412_346, 412_999, 412_346]
        );
    }

    /// Idle, closed, backpressure-holds-the-record, retry-does-not-poll-
    /// past-the-held-record, and relay-after-backpressure-clears: the
    /// contract every `ScriptedQueue<T>`-backed pump shares.
    #[test]
    fn remote_epoch_pump_honors_the_shared_contract() {
        crate::fakes::pump_contract::run(
            &record(412_346, 2, 1),
            &record(412_346, 3, 2),
            process_remote_epoch,
        );
    }
}

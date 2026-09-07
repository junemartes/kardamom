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
//! of both it and the tx_data → TxRef path.

use kardamom_types::BPosition;
use kardamom_types::xchain::RemoteEpochRecord;

use crate::error::SequencerError;
use crate::outbound::TxOrderingRefPublisher;

/// Subscription surface the remote-epoch pump reads from. Production wiring
/// binds this to `kardamom_log`'s `TxRemoteEpochsSubscriberHandle`; tests use
/// the in-memory fake in [`fakes`].
pub trait RemoteEpochSubscriber: Send {
    /// Poll for one record. Returns:
    /// * `Ok(Some((pos, record)))` — a record was available and is yielded.
    /// * `Ok(None)` — no fragment available right now (caller should back off).
    /// * `Err(SequencerError::IngressDisconnected)` — subscription closed.
    fn poll(&mut self) -> Result<Option<(BPosition, RemoteEpochRecord)>, SequencerError>;
}

/// One record popped from the subscription but not yet accepted by the
/// cluster. The pump holds it here across a `Backpressure` result and
/// retries it before it polls again.
pub type PendingRemoteEpoch = Option<(BPosition, RemoteEpochRecord)>;

/// Single-step remote-epoch pump: take the pending record if there is one,
/// else pull one record off the subscription, and forward it on
/// `tx_ordering`. Returns `Ok(true)` if a record was processed (caller
/// should keep going), `Ok(false)` if the subscription is idle.
///
/// On `SequencerError::Backpressure` the record goes into `pending`, and
/// the next call retries the SAME record before it polls for a new one.
/// The poll is destructive (`try_recv`), so without this slot a
/// backpressured record would be lost. The watcher persists its cursor
/// after the media-driver ack, so it would never re-publish the record,
/// and the pair's lane would have a permanent hole (audit H2).
/// `Backpressure` includes "not connected", so a leader election would
/// otherwise drain every sequencer's backlog at once.
pub fn process_remote_epoch<S, P>(
    sub: &mut S,
    b: &mut P,
    pending: &mut PendingRemoteEpoch,
) -> Result<bool, SequencerError>
where
    S: RemoteEpochSubscriber,
    P: TxOrderingRefPublisher,
{
    let (pos, record) = match pending.take() {
        Some(held) => held,
        None => match sub.poll()? {
            Some(next) => next,
            None => return Ok(false),
        },
    };
    if let Err(e) = b.try_publish_remote_epoch(&record) {
        if matches!(e, SequencerError::Backpressure) {
            *pending = Some((pos, record));
        }
        return Err(e);
    }
    crate::metrics::record_remote_epoch_relayed(record.origin_chain_id, record.messages.len());
    Ok(true)
}

#[cfg(any(test, feature = "testing"))]
pub mod fakes {
    use std::collections::VecDeque;
    use std::sync::{Arc, Mutex};

    use super::*;

    /// In-memory [`RemoteEpochSubscriber`] driven by a scripted queue. Push
    /// test inputs via [`ScriptedRemoteEpochs::push`]; the sequencer drains
    /// them FIFO.
    #[derive(Default, Clone)]
    pub struct ScriptedRemoteEpochs {
        pub queue: Arc<Mutex<VecDeque<(BPosition, RemoteEpochRecord)>>>,
        pub closed: Arc<Mutex<bool>>,
    }

    impl ScriptedRemoteEpochs {
        pub fn push(&self, pos: BPosition, record: RemoteEpochRecord) {
            self.queue.lock().unwrap().push_back((pos, record));
        }

        pub fn close(&self) {
            *self.closed.lock().unwrap() = true;
        }
    }

    impl RemoteEpochSubscriber for ScriptedRemoteEpochs {
        fn poll(&mut self) -> Result<Option<(BPosition, RemoteEpochRecord)>, SequencerError> {
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
    use kardamom_types::xchain::XChainMessage;

    use super::fakes::ScriptedRemoteEpochs;
    use super::*;
    use crate::outbound::fakes::InMemoryTxOrderingRefPublisher;

    fn record(origin: u64, first_seq: u64, messages: usize) -> RemoteEpochRecord {
        RemoteEpochRecord {
            origin_chain_id: origin,
            anchor_number: 100 + first_seq,
            anchor_hash: B256::repeat_byte(first_seq as u8),
            first_seq,
            messages: (0..messages)
                .map(|i| XChainMessage {
                    source_hash: B256::repeat_byte(0xE0 + i as u8),
                    seq: first_seq + i as u64,
                    gas_limit: 100_000,
                    hops: 0,
                    ..Default::default()
                })
                .collect(),
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

        assert!(process_remote_epoch(&mut sub, &mut pubr, &mut None).unwrap());

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
            assert!(process_remote_epoch(&mut sub, &mut pubr, &mut None).unwrap());
        }
        let got = pubr.remote_epochs.lock().unwrap();
        assert_eq!(
            got.iter().map(|r| r.origin_chain_id).collect::<Vec<_>>(),
            vec![412_346, 412_999, 412_346]
        );
    }

    #[test]
    fn idle_subscription_reports_no_work() {
        let mut sub = ScriptedRemoteEpochs::default();
        let mut pubr = InMemoryTxOrderingRefPublisher::default();
        assert!(!process_remote_epoch(&mut sub, &mut pubr, &mut None).unwrap());
    }

    #[test]
    fn closed_subscription_surfaces_disconnect() {
        let mut sub = ScriptedRemoteEpochs::default();
        sub.close();
        let mut pubr = InMemoryTxOrderingRefPublisher::default();
        assert!(matches!(
            process_remote_epoch(&mut sub, &mut pubr, &mut None),
            Err(SequencerError::IngressDisconnected)
        ));
    }

    #[test]
    fn backpressure_propagates_so_the_caller_retries() {
        let mut sub = ScriptedRemoteEpochs::default();
        let mut pubr = InMemoryTxOrderingRefPublisher::default();
        *pubr.fail_with_backpressure.lock().unwrap() = true;
        sub.push(BPosition::default(), record(412_346, 2, 1));
        let mut pending = None;

        assert!(matches!(
            process_remote_epoch(&mut sub, &mut pubr, &mut pending),
            Err(SequencerError::Backpressure)
        ));
        assert!(pubr.remote_epochs.lock().unwrap().is_empty());
        assert!(pending.is_some(), "the popped record is held, not dropped");
    }

    /// Audit H2: a backpressured publish followed by a successful one relays
    /// the same record exactly once, and the record behind it is not
    /// skipped. This is the lane-hole fix: the sequencer never drops.
    #[test]
    fn a_backpressured_record_is_retried_and_relayed_exactly_once() {
        let mut sub = ScriptedRemoteEpochs::default();
        let mut pubr = InMemoryTxOrderingRefPublisher::default();
        let first = record(412_346, 2, 1);
        let second = record(412_346, 3, 2);
        sub.push(BPosition::default(), first.clone());
        sub.push(BPosition::default(), second.clone());
        let mut pending = None;

        *pubr.fail_with_backpressure.lock().unwrap() = true;
        assert!(matches!(
            process_remote_epoch(&mut sub, &mut pubr, &mut pending),
            Err(SequencerError::Backpressure)
        ));
        assert!(matches!(
            process_remote_epoch(&mut sub, &mut pubr, &mut pending),
            Err(SequencerError::Backpressure)
        ));
        assert_eq!(
            sub.queue.lock().unwrap().len(),
            1,
            "the retry does not poll past the held record"
        );

        *pubr.fail_with_backpressure.lock().unwrap() = false;
        assert!(process_remote_epoch(&mut sub, &mut pubr, &mut pending).unwrap());
        assert!(pending.is_none(), "the slot empties on success");
        assert!(process_remote_epoch(&mut sub, &mut pubr, &mut pending).unwrap());
        assert!(!process_remote_epoch(&mut sub, &mut pubr, &mut pending).unwrap());

        let got = pubr.remote_epochs.lock().unwrap();
        assert_eq!(*got, vec![first, second], "each record once, in order");
    }
}

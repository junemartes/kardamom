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

/// Single-step remote-epoch pump: pull one record off the subscription and
/// forward it on `tx_ordering`. Returns `Ok(true)` if a record was processed
/// (caller should keep going), `Ok(false)` if the subscription is idle.
///
/// On `SequencerError::Backpressure` the caller retries the same record next
/// tick — the record is durable on `tx_remote_epochs`, so there is no rewind
/// state to manage.
///
/// # Errors
///
/// Returns an error if the subscription disconnects, or the publish
/// backs off or fails.
pub fn process_remote_epoch<S, P>(sub: &mut S, b: &mut P) -> Result<bool, SequencerError>
where
    S: RemoteEpochSubscriber,
    P: TxOrderingRefPublisher,
{
    let Some((_pos, record)) = sub.poll()? else {
        return Ok(false);
    };
    b.try_publish_remote_epoch(&record)?;
    crate::metrics::record_remote_epoch_relayed(record.origin_chain_id, record.messages.len());
    Ok(true)
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

    fn record(origin: u64, first_seq: u64, messages: usize) -> RemoteEpochRecord {
        RemoteEpochRecord {
            origin_chain_id: origin,
            anchor_number: 100 + first_seq,
            anchor_hash: B256::repeat_byte(u8::try_from(first_seq).unwrap()),
            first_seq,
            messages: (0..messages)
                .map(|i| {
                    let i = u64::try_from(i).unwrap();
                    XChainMessage {
                        source_hash: B256::repeat_byte(u8::try_from(0xE0 + i).unwrap()),
                        seq: first_seq + i,
                        gas_limit: 100_000,
                        ..Default::default()
                    }
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

        assert!(process_remote_epoch(&mut sub, &mut pubr).unwrap());

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
            assert!(process_remote_epoch(&mut sub, &mut pubr).unwrap());
        }
        let got = pubr.remote_epochs.lock().unwrap();
        assert_eq!(
            got.iter().map(|r| r.origin_chain_id).collect::<Vec<_>>(),
            vec![412_346, 412_999, 412_346]
        );
    }

    #[test]
    fn shares_the_idle_closed_backpressure_pump_contract() {
        // Idle-report, closed-disconnect and backpressure-propagation are
        // not remote-epoch-specific: crate::fakes::pump_contract::run
        // exercises them once, generically, for every ScriptedQueue-backed
        // pump.
        crate::fakes::pump_contract::run(&record(412_346, 2, 1), process_remote_epoch);
    }
}

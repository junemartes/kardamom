//! Inbound tx_data subscription.
//!
//! Under the MDS topology the sequencer subscribes to ONE tx_data
//! (the one for its address shard) and observes every `TxEnvelope` that
//! any proxy published there, paired with the Aeron `BPosition` of that
//! fragment. The sequencer's job is to reorder by per-sender nonce and
//! republish a `TxRef { tx_hash, shard_id, tx_data_position }` onto tx_ordering.
//!
//! Per / the inbound `TxEnvelope` already has `sender` and
//! `tx_hash` populated by the proxy — no recovery or hashing happens here.

use crate::error::SequencerError;
use kardamom_types::{BPosition, TxEnvelope};

/// Subscription to one tx_data stream. Yields `(tx_data_position, envelope)`
/// per Aeron fragment. Production implementations wrap a
/// `log` tx_data subscriber; tests use [`fakes::ScriptedTxData`].
///
/// Same shape as the executor's `TxDataSubscription` trait; the
/// difference is the sequencer is one of P concurrent subscribers per
/// shard, while the executor is the sole consumer per shard for the
/// envelope→ref join.
pub trait TxDataSubscriber: Send {
    /// Poll for at most one message. Returns:
    ///  - `Ok(Some((tx_data_position, env)))` on the next available fragment.
    ///  - `Ok(None)` when no message is ready (caller backs off).
    ///  - `Err(IngressDisconnected)` when the subscription is permanently
    ///    closed.
    fn poll(&mut self) -> Result<Option<(BPosition, TxEnvelope)>, SequencerError>;
}

// ===========================================================================
// In-memory fakes for unit / integration tests.
// ===========================================================================

#[cfg(any(test, feature = "testing"))]
pub mod fakes {
    use std::collections::VecDeque;

    use super::*;

    /// In-memory tx_data subscription scripted with `(position, envelope)`
    /// pairs in arrival order. Tests typically prepare a vector of envelopes
    /// and synthesize monotonically increasing positions before driving
    /// `Sequencer::run_once`.
    #[derive(Default)]
    pub struct ScriptedTxData {
        pub queue: VecDeque<(BPosition, TxEnvelope)>,
        pub disconnected: bool,
    }

    impl TxDataSubscriber for ScriptedTxData {
        fn poll(&mut self) -> Result<Option<(BPosition, TxEnvelope)>, SequencerError> {
            if self.disconnected {
                return Err(SequencerError::IngressDisconnected);
            }
            Ok(self.queue.pop_front())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::fakes::*;
    use super::*;

    #[test]
    fn scripted_channel_a_empty_then_disconnect() {
        let mut s = ScriptedTxData::default();
        assert!(matches!(s.poll(), Ok(None)));
        s.disconnected = true;
        assert!(matches!(s.poll(), Err(SequencerError::IngressDisconnected)));
    }
}

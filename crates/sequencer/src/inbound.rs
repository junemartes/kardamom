//! Inbound tx_data subscription.
//!
//! Under the MDS topology, the sequencer subscribes to one tx_data stream
//! (the one for its address shard). It sees every `TxEnvelope` that any
//! proxy published there, paired with the Aeron `BPosition` of that
//! fragment. The sequencer reorders envelopes by per-sender nonce, then
//! republishes a `TxRef { tx_hash, shard_id, tx_data_position }` onto
//! tx_ordering.
//!
//! The inbound `TxEnvelope` already has `sender` and `tx_hash` set by
//! the proxy. No recovery or hashing happens here.

use crate::error::SequencerError;
use kardamom_types::{TxDataLoc, TxEnvelope};

/// Subscription to one tx_data stream.
/// Yields `(TxDataLoc, envelope)` for each Aeron fragment: the envelope
/// paired with its publisher `session_id` and `BPosition`. Production code
/// wraps a `log` tx_data subscriber. Tests use [`fakes::ScriptedTxData`].
///
/// This has the same shape as the executor's `TxDataSubscription` trait.
/// The difference: the sequencer is one of P concurrent subscribers per
/// shard, but the executor is the sole consumer per shard for the
/// envelope-to-ref join. The session id in `TxDataLoc` tells apart
/// concurrent, active-active ingress publishers. This lets the stamped
/// `TxRef.tx_data_session_id` give the executor a unique join key.
pub trait TxDataSubscriber: Send {
    /// Poll for at most one message. Returns:
    ///  - `Ok(Some((loc, env)))` on the next available fragment.
    ///  - `Ok(None)` when no message is ready (caller backs off).
    ///  - `Err(IngressDisconnected)` when the subscription is permanently
    ///    closed.
    fn poll(&mut self) -> Result<Option<(TxDataLoc, TxEnvelope)>, SequencerError>;

    /// The tx_data lane this subscription reads. Every envelope from
    /// `poll` lives on this lane. The sequencer stamps it into
    /// `TxRef::shard_id`, so the executor joins the ref against the
    /// archive that holds the envelope.
    fn lane(&self) -> u8;
}

// ===========================================================================
// In-memory fakes for unit / integration tests.
// ===========================================================================

#[cfg(any(test, feature = "testing"))]
pub mod fakes {
    use std::collections::VecDeque;

    use super::*;

    /// In-memory tx_data subscription. It is scripted with `(loc, envelope)`
    /// pairs in arrival order. Tests usually build a vector of envelopes and
    /// make increasing [`TxDataLoc`] values (session and position) before
    /// they run `Sequencer::run_once`.
    #[derive(Default)]
    pub struct ScriptedTxData {
        pub queue: VecDeque<(TxDataLoc, TxEnvelope)>,
        pub disconnected: bool,
        /// The lane the fake reads. Defaults to 0.
        pub lane: u8,
    }

    impl TxDataSubscriber for ScriptedTxData {
        fn poll(&mut self) -> Result<Option<(TxDataLoc, TxEnvelope)>, SequencerError> {
            if self.disconnected {
                return Err(SequencerError::IngressDisconnected);
            }
            Ok(self.queue.pop_front())
        }

        fn lane(&self) -> u8 {
            self.lane
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

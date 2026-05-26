//! Inbound channel-A subscription.
//!
//! Under the MDS topology the sequencer subscribes to ONE channel A
//! (the one for its address shard) and observes every `TxEnvelope` that
//! any proxy published there, paired with the Aeron `BPosition` of that
//! fragment. The sequencer's job is to reorder by per-sender nonce and
//! republish a `TxRef { tx_hash, shard_id, position_a }` onto channel B.
//!
//! Per D-Sh3 / D-Sh4 the inbound `TxEnvelope` already has `sender` and
//! `tx_hash` populated by the proxy — no recovery or hashing happens here.

use crate::error::SequencerError;
use kardamom_types::{BPosition, TxEnvelope};

/// Subscription to one channel A stream. Yields `(position_a, envelope)`
/// per Aeron fragment. Production implementations wrap a
/// `kardamom_log` channel-A subscriber; tests use [`fakes::ScriptedChannelA`].
///
/// Same shape as the executor's `ChannelASubscription` trait; the
/// difference is the sequencer is one of P concurrent subscribers per
/// shard, while the executor is the sole consumer per shard for the
/// envelope→ref join.
pub trait ChannelASubscriber: Send {
    /// Poll for at most one message. Returns:
    ///  - `Ok(Some((position_a, env)))` on the next available fragment.
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

    /// In-memory channel-A subscription scripted with `(position, envelope)`
    /// pairs in arrival order. Tests typically prepare a vector of envelopes
    /// and synthesize monotonically increasing positions before driving
    /// `Sequencer::run_once`.
    #[derive(Default)]
    pub struct ScriptedChannelA {
        pub queue: VecDeque<(BPosition, TxEnvelope)>,
        pub disconnected: bool,
    }

    impl ChannelASubscriber for ScriptedChannelA {
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
        let mut s = ScriptedChannelA::default();
        assert!(matches!(s.poll(), Ok(None)));
        s.disconnected = true;
        assert!(matches!(s.poll(), Err(SequencerError::IngressDisconnected)));
    }
}

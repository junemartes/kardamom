//! Inbound channel abstractions.
//!
//! The sequencer reads `TxEnvelope` messages from its `ingress[i]` partition;
//! the hot-standby reads `BMessage` items (the union of canonical-B txs and
//! sealer block-boundary markers) from channel B.
//!
//! Per D-Sh3 / D-Sh4 the inbound `TxEnvelope` already has `sender` and
//! `tx_hash` populated by the proxy — no recovery or hashing happens here.

use alloy_primitives::Address;

use crate::error::SequencerError;
use kardamom_types::TxEnvelope;

/// Source of one partition's ingress stream. Production implementation is
/// either an Aeron subscription handle (S3 `aeron-live` feature) or a
/// `tokio::sync::mpsc::UnboundedReceiver<TxEnvelope>` adapter on top of
/// `kardamom_ingress::channels::MockChannels`.
pub trait IngressSource: Send {
    /// Poll for at most one message. Returns:
    ///  - `Ok(Some(env))` on a decoded ingress envelope.
    ///  - `Ok(None)` when no message is ready (caller backs off).
    ///  - `Err(IngressDisconnected)` when the subscription is permanently
    ///    closed.
    fn poll(&mut self) -> Result<Option<TxEnvelope>, SequencerError>;
}

/// One observation off channel B from the standby's perspective.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BMessage {
    Tx {
        sender: Address,
        nonce: u64,
    },
    /// Sealer's `BlockBoundaryStart` marker. Standby decodes and skips.
    BlockBoundary,
}

/// Read side of channel B used by the hot-standby tailer.
pub trait BReplaySource: Send {
    fn poll(&mut self) -> Result<Option<BMessage>, SequencerError>;
}

// ===========================================================================
// In-memory fakes for unit / integration tests.
// ===========================================================================

#[cfg(any(test, feature = "testing"))]
pub mod fakes {
    use std::collections::VecDeque;

    use super::*;

    #[derive(Default)]
    pub struct ScriptedIngress {
        pub queue: VecDeque<TxEnvelope>,
        pub disconnected: bool,
    }

    impl IngressSource for ScriptedIngress {
        fn poll(&mut self) -> Result<Option<TxEnvelope>, SequencerError> {
            if self.disconnected {
                return Err(SequencerError::IngressDisconnected);
            }
            Ok(self.queue.pop_front())
        }
    }

    #[derive(Default)]
    pub struct ScriptedB {
        pub queue: VecDeque<BMessage>,
    }

    impl BReplaySource for ScriptedB {
        fn poll(&mut self) -> Result<Option<BMessage>, SequencerError> {
            Ok(self.queue.pop_front())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::fakes::*;
    use super::*;

    #[test]
    fn scripted_ingress_empty_then_disconnect() {
        let mut s = ScriptedIngress::default();
        assert!(matches!(s.poll(), Ok(None)));
        s.disconnected = true;
        assert!(matches!(s.poll(), Err(SequencerError::IngressDisconnected)));
    }

    #[test]
    fn scripted_b_yields_block_boundary() {
        let mut s = ScriptedB::default();
        s.queue.push_back(BMessage::BlockBoundary);
        match s.poll().unwrap().unwrap() {
            BMessage::BlockBoundary => {}
            BMessage::Tx { .. } => panic!("wrong variant"),
        }
    }
}

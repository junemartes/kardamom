//! Errors surfaced by the sequencer subsystem.

use thiserror::Error;

#[derive(Debug, Error)]
pub enum SequencerError {
    #[error("backpressure: tx_ordering publication blocked")]
    Backpressure,

    #[error("ingress source disconnected")]
    IngressDisconnected,

    #[error("malformed tx frame: {0}")]
    MalformedFrame(String),

    /// An outbound record could not be encoded. Only reachable on an rkyv
    /// failure or an epoch with more deposits than a u32 can count, so it is
    /// a bug rather than a transport condition — but it must not panic the
    /// sequencer's pump.
    #[error("encode failed: {0}")]
    EncodeFailed(String),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn display_strings_are_stable() {
        assert_eq!(
            SequencerError::Backpressure.to_string(),
            "backpressure: tx_ordering publication blocked"
        );
        assert_eq!(
            SequencerError::IngressDisconnected.to_string(),
            "ingress source disconnected"
        );
    }
}

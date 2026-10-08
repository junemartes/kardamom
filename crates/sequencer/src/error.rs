//! Errors surfaced by the sequencer subsystem.

use kardamom_obs::halt::{Halt, HaltCause};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum SequencerError {
    #[error("backpressure: tx_ordering publication blocked")]
    Backpressure,

    #[error("ingress source disconnected")]
    IngressDisconnected,

    #[error("malformed tx frame: {0}")]
    MalformedFrame(String),

    /// An outbound record could not be encoded. This can happen only after
    /// an rkyv failure, or when an epoch has more deposits than a u32 can
    /// hold. It is a bug, not a transport error, but it must not panic the
    /// sequencer's pump.
    #[error("encode failed: {0}")]
    EncodeFailed(String),

    /// The config `Sequencer::new` was built from failed validation.
    #[error("config error: {0}")]
    Config(#[from] crate::config::ConfigError),

    /// The sealer refuses the next epoch until it gets the epoch of L1
    /// block `expected`, and this replica does not hold that epoch. A twin
    /// replica that holds it, or a da-watcher that publishes it again,
    /// fills the gap.
    #[error(
        "origin gap: the sealer expects the epoch of L1 block {expected}, and this replica does not hold it"
    )]
    OriginGapUnfilled { expected: u64 },

    /// The epoch pump holds the most relayed epochs that it may keep
    /// without a boundary that confirms them.
    #[error(
        "epoch queue full: {held} relayed epochs wait for a boundary, the oldest at L1 block {oldest}"
    )]
    EpochQueueFull { held: usize, oldest: u64 },
}

impl SequencerError {
    /// The halt this error raises, if it is a halt. The epoch pump stays up
    /// on these errors and retries; the halt clears when a boundary
    /// confirms the epochs.
    #[must_use]
    pub fn halt(&self) -> Option<Halt> {
        match self {
            Self::OriginGapUnfilled { .. } | Self::EpochQueueFull { .. } => {
                Some(Halt::new(HaltCause::OriginGap, self.to_string()))
            }
            Self::Backpressure
            | Self::IngressDisconnected
            | Self::MalformedFrame(_)
            | Self::EncodeFailed(_)
            | Self::Config(_) => None,
        }
    }
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

    #[test]
    fn an_epoch_stall_is_an_origin_gap_halt_that_clears_by_itself() {
        let halt = SequencerError::OriginGapUnfilled { expected: 101 }
            .halt()
            .unwrap();
        assert_eq!(halt.cause, HaltCause::OriginGap);
        assert_eq!(halt.clears, kardamom_obs::halt::Clears::Auto);
        assert!(halt.detail.contains("L1 block 101"), "{}", halt.detail);
        let full = SequencerError::EpochQueueFull {
            held: 4096,
            oldest: 7,
        };
        assert_eq!(full.halt().unwrap().cause, HaltCause::OriginGap);
        assert!(SequencerError::Backpressure.halt().is_none());
    }
}

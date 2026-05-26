//! Errors surfaced by the sequencer subsystem.

use thiserror::Error;

#[derive(Debug, Error)]
pub enum SequencerError {
    #[error("backpressure: channel B publication blocked")]
    Backpressure,

    #[error("ingress source disconnected")]
    IngressDisconnected,

    #[error("malformed tx frame: {0}")]
    MalformedFrame(String),

    #[error("kardamom-log error: {0}")]
    Log(String),
}

impl From<kardamom_log::LogError> for SequencerError {
    fn from(value: kardamom_log::LogError) -> Self {
        SequencerError::Log(value.to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn display_strings_are_stable() {
        assert_eq!(
            SequencerError::Backpressure.to_string(),
            "backpressure: channel B publication blocked"
        );
        assert_eq!(
            SequencerError::IngressDisconnected.to_string(),
            "ingress source disconnected"
        );
    }
}

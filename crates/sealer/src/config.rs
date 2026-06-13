//! Sealer process configuration.
//!
//! Loaded from TOML at startup. All knobs are explicit; unknown keys are
//! rejected so misconfigured deployments fail fast.

use serde::{Deserialize, Serialize};
use thiserror::Error;

/// All knobs for a single sealer process.
///
/// All fields are required except `tick_interval_ms`, which defaults to 250.
/// Unknown keys are rejected (`deny_unknown_fields`).
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SealerConfig {
    /// Identifier for this sealer process, used as the `host_id` metric
    /// label unless overridden via `--host-id`/`KARDAMOM_HOST_ID`.
    pub host_id: u8,
    /// Aeron channel URI for tx_ordering (publish + subscribe on the same channel).
    pub channel_b_uri: String,
    /// Aeron stream id carrying `TxEnvelope`s on tx_ordering.
    pub channel_b_tx_stream_id: i32,
    /// Aeron stream id carrying `BlockBoundaryStart`s on tx_ordering. Must differ
    /// from `channel_b_tx_stream_id` so subscribers can demultiplex by type
    /// without an in-band tag.
    pub channel_b_boundary_stream_id: i32,
    /// Wall-clock tick interval. Default 250 ms; values other than 250 are for
    /// tests. Must be > 0.
    #[serde(default = "default_tick_ms")]
    pub tick_interval_ms: u64,
}

fn default_tick_ms() -> u64 {
    250
}

/// Errors surfaced by [`SealerConfig::validate`].
#[derive(Debug, Error, PartialEq, Eq)]
pub enum ConfigError {
    #[error("tick_interval_ms must be > 0")]
    BadTick,
    #[error("channel_b_tx_stream_id and channel_b_boundary_stream_id must differ")]
    SharedStreamId,
}

impl SealerConfig {
    /// Reject configurations that would surely misbehave at runtime.
    pub fn validate(&self) -> Result<(), ConfigError> {
        if self.tick_interval_ms == 0 {
            return Err(ConfigError::BadTick);
        }
        if self.channel_b_tx_stream_id == self.channel_b_boundary_stream_id {
            return Err(ConfigError::SharedStreamId);
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn good() -> SealerConfig {
        SealerConfig {
            host_id: 7,
            channel_b_uri: "x".into(),
            channel_b_tx_stream_id: 1,
            channel_b_boundary_stream_id: 2,
            tick_interval_ms: 250,
        }
    }

    #[test]
    fn parses_minimal_toml() {
        let toml = r#"
            host_id = 7
            channel_b_uri = "aeron:udp?endpoint=224.0.0.1:40123"
            channel_b_tx_stream_id = 1001
            channel_b_boundary_stream_id = 1002
        "#;
        let cfg: SealerConfig = toml::from_str(toml).unwrap();
        assert_eq!(cfg.host_id, 7);
        assert_eq!(cfg.tick_interval_ms, 250);
        cfg.validate().unwrap();
    }

    #[test]
    fn rejects_unknown_keys() {
        let toml = r#"
            host_id = 1
            channel_b_uri = "x"
            channel_b_tx_stream_id = 1
            channel_b_boundary_stream_id = 2
            bogus = "field"
        "#;
        assert!(toml::from_str::<SealerConfig>(toml).is_err());
    }

    #[test]
    fn boundary_and_tx_stream_ids_must_differ() {
        let cfg = SealerConfig {
            channel_b_tx_stream_id: 5,
            channel_b_boundary_stream_id: 5,
            ..good()
        };
        assert_eq!(cfg.validate(), Err(ConfigError::SharedStreamId));
    }

    #[test]
    fn tick_interval_must_be_positive() {
        let cfg = SealerConfig {
            tick_interval_ms: 0,
            ..good()
        };
        assert_eq!(cfg.validate(), Err(ConfigError::BadTick));
    }
}

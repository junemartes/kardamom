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
    /// Aeron channel URI for tx_ordering (publish + subscribe on the same
    /// channel). Used as the shared single-host IPC / legacy-multicast
    /// channel. When `channel_b_mdc_control` is set AND the resolved
    /// `LogConfig` enables tx_ordering MDC, this URI is superseded for the
    /// sealer's own publication by its MDC control endpoint (it is still used
    /// for the bootstrap / tail-tracker subscription via the channels config's
    /// MDC subscriber URIs).
    pub channel_b_uri: String,
    /// This sealer's tx_ordering MDC control endpoint (`ip:port`). When set
    /// and the resolved `LogConfig` enables MDC, the sealer publishes its
    /// boundary markers via this MDC `control-mode=dynamic` endpoint instead
    /// of the shared `channel_b_uri`. Must match one of the
    /// `tx_ordering_mdc_publishers` entries in the channels config. `None`
    /// keeps the legacy shared-channel behaviour.
    #[serde(default)]
    pub channel_b_mdc_control: Option<String>,
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
            channel_b_mdc_control: None,
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
    fn channel_b_mdc_control_defaults_none_and_parses() {
        // Omitted ⇒ None (legacy shared-channel path).
        let toml = r#"
            host_id = 7
            channel_b_uri = "aeron:udp?endpoint=224.0.0.1:40123"
            channel_b_tx_stream_id = 1001
            channel_b_boundary_stream_id = 1002
        "#;
        let cfg: SealerConfig = toml::from_str(toml).unwrap();
        assert_eq!(cfg.channel_b_mdc_control, None);

        // Present ⇒ Some(endpoint) (MDC path).
        let toml = r#"
            host_id = 7
            channel_b_uri = "aeron:ipc?alias=tx-ordering"
            channel_b_mdc_control = "192.168.56.22:40110"
            channel_b_tx_stream_id = 1001
            channel_b_boundary_stream_id = 1002
        "#;
        let cfg: SealerConfig = toml::from_str(toml).unwrap();
        assert_eq!(
            cfg.channel_b_mdc_control.as_deref(),
            Some("192.168.56.22:40110")
        );
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

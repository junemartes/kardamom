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
    /// This process's recorder id. Must appear in `recorder_ids`.
    ///
    /// Typed `u8` to match `kardamom_types::FsyncWatermark::recorder_id`; the
    /// `kardamom-leases::Lease` primitive also keys recorders by `u8`.
    pub host_id: u8,
    /// Aeron channel URI for tx_ordering (publish + subscribe on the same channel).
    pub channel_b_uri: String,
    /// Aeron stream id carrying `TxEnvelope`s on tx_ordering.
    pub channel_b_tx_stream_id: i32,
    /// Aeron stream id carrying `BlockBoundaryStart`s on tx_ordering. Must differ
    /// from `channel_b_tx_stream_id` so subscribers can demultiplex by type
    /// without an in-band tag. The two streams share the same underlying
    /// channel — the sealer is "just another publisher on tx_ordering" per spec
    /// §2.6, but with its own stream id so consumers can subscribe selectively.
    pub channel_b_boundary_stream_id: i32,
    /// Aeron channel URI carrying the per-recorder watermark streams.
    pub watermark_channel_uri: String,
    /// Stream id of recorder `host_id` is `watermark_stream_id_base + host_id as i32`.
    pub watermark_stream_id_base: i32,
    /// All recorder ids in the cluster (sealer election pool).
    pub recorder_ids: Vec<u8>,
    /// "Caught up" means `|current_B_position - recorder.fsynced| <= caught_up_lag_bytes`.
    /// Forwarded to `kardamom_leases::LeaseConfig::caught_up_window`.
    pub caught_up_lag_bytes: u64,
    /// A watermark not observed within this many ms is treated as stale; the
    /// recorder is not eligible to lead until it refreshes.
    pub caught_up_stale_ms: u64,
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
    #[error("host_id {host_id} is not in recorder_ids {recorders:?}")]
    HostIdNotRecorder { host_id: u8, recorders: Vec<u8> },
    #[error("recorder_ids must be non-empty")]
    EmptyRecorderSet,
    #[error("tick_interval_ms must be > 0")]
    BadTick,
    #[error("caught_up_lag_bytes does not fit in i64")]
    LagOverflow,
    #[error("channel_b_tx_stream_id and channel_b_boundary_stream_id must differ")]
    SharedStreamId,
}

impl SealerConfig {
    /// Reject configurations that would surely misbehave at runtime.
    pub fn validate(&self) -> Result<(), ConfigError> {
        if self.recorder_ids.is_empty() {
            return Err(ConfigError::EmptyRecorderSet);
        }
        if !self.recorder_ids.contains(&self.host_id) {
            return Err(ConfigError::HostIdNotRecorder {
                host_id: self.host_id,
                recorders: self.recorder_ids.clone(),
            });
        }
        if self.tick_interval_ms == 0 {
            return Err(ConfigError::BadTick);
        }
        if i64::try_from(self.caught_up_lag_bytes).is_err() {
            return Err(ConfigError::LagOverflow);
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
            watermark_channel_uri: "x".into(),
            watermark_stream_id_base: 1,
            recorder_ids: vec![1, 2, 7],
            caught_up_lag_bytes: 65_536,
            caught_up_stale_ms: 500,
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
            watermark_channel_uri = "aeron:udp?endpoint=224.0.0.1:40124"
            watermark_stream_id_base = 2000
            recorder_ids = [1, 2, 7]
            caught_up_lag_bytes = 65536
            caught_up_stale_ms = 500
        "#;
        let cfg: SealerConfig = toml::from_str(toml).unwrap();
        assert_eq!(cfg.host_id, 7);
        assert_eq!(cfg.recorder_ids, vec![1, 2, 7]);
        // `tick_interval_ms` defaults when omitted.
        assert_eq!(cfg.tick_interval_ms, 250);
        assert_eq!(cfg.caught_up_lag_bytes, 65_536);
        assert_eq!(cfg.caught_up_stale_ms, 500);
        cfg.validate().unwrap();
    }

    #[test]
    fn rejects_unknown_keys() {
        let toml = r#"
            host_id = 1
            channel_b_uri = "x"
            channel_b_tx_stream_id = 1
            channel_b_boundary_stream_id = 2
            watermark_channel_uri = "x"
            watermark_stream_id_base = 1
            recorder_ids = [1]
            caught_up_lag_bytes = 1
            caught_up_stale_ms = 1
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
    fn host_id_must_be_in_recorder_set() {
        let cfg = SealerConfig {
            host_id: 99,
            ..good()
        };
        assert_eq!(
            cfg.validate(),
            Err(ConfigError::HostIdNotRecorder {
                host_id: 99,
                recorders: vec![1, 2, 7],
            })
        );
    }

    #[test]
    fn recorder_set_must_be_non_empty() {
        let cfg = SealerConfig {
            recorder_ids: vec![],
            ..good()
        };
        assert_eq!(cfg.validate(), Err(ConfigError::EmptyRecorderSet));
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

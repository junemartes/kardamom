//! Runtime configuration for a single sequencer process.

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct SequencerConfig {
    /// Total partitions in the cluster (M). Default 8.
    pub partition_count: u32,
    /// This process's partition index (`0..partition_count`).
    pub partition_index: u32,
    /// Per-sender future-nonce buffer capacity. Default 16.
    pub max_pending_per_sender: usize,
    /// Optional CPU core to pin this process to. `None` = no pin.
    pub core_id: Option<usize>,
    /// Backpressure behaviour when channel B blocks.
    pub backpressure_policy: BackpressurePolicy,
    /// Role: primary owns the slice; standby tails B and waits for lease
    /// takeover.
    pub role: SequencerRole,
}

#[derive(Debug, Clone, Copy, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum BackpressurePolicy {
    /// Return `Err(Backpressure)` immediately.
    ReturnImmediately,
    /// Spin-retry up to `max_retries` times before returning `Err(Backpressure)`.
    SpinRetry { max_retries: u32 },
}

#[derive(Debug, Clone, Copy, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum SequencerRole {
    Primary,
    Standby,
}

impl Default for SequencerConfig {
    fn default() -> Self {
        Self {
            partition_count: 8,
            partition_index: 0,
            max_pending_per_sender: 16,
            core_id: None,
            backpressure_policy: BackpressurePolicy::ReturnImmediately,
            role: SequencerRole::Primary,
        }
    }
}

impl SequencerConfig {
    pub fn validate(&self) -> Result<(), ConfigError> {
        if self.partition_count == 0 {
            return Err(ConfigError::ZeroPartitions);
        }
        if self.partition_index >= self.partition_count {
            return Err(ConfigError::IndexOutOfRange {
                index: self.partition_index,
                count: self.partition_count,
            });
        }
        Ok(())
    }
}

#[derive(Debug, thiserror::Error)]
pub enum ConfigError {
    #[error("partition_count must be >= 1")]
    ZeroPartitions,
    #[error("partition_index {index} >= partition_count {count}")]
    IndexOutOfRange { index: u32, count: u32 },
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_is_valid() {
        SequencerConfig::default().validate().unwrap();
    }

    #[test]
    fn index_out_of_range_rejected() {
        let cfg = SequencerConfig {
            partition_index: 8,
            ..Default::default()
        };
        assert!(matches!(
            cfg.validate(),
            Err(ConfigError::IndexOutOfRange { .. })
        ));
    }

    #[test]
    fn zero_partitions_rejected() {
        let cfg = SequencerConfig {
            partition_count: 0,
            ..Default::default()
        };
        assert!(matches!(cfg.validate(), Err(ConfigError::ZeroPartitions)));
    }

    #[test]
    fn toml_round_trip() {
        let cfg = SequencerConfig::default();
        let s = toml::to_string(&cfg).unwrap();
        let back: SequencerConfig = toml::from_str(&s).unwrap();
        assert_eq!(cfg, back);
    }
}

//! Runtime configuration for a single sequencer process.

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct SequencerConfig {
    /// Total partitions in the cluster (M). Default 8.
    pub partition_count: u32,
    /// This process's partition index (`0..partition_count`).
    pub partition_index: u32,
    /// Stable identifier for this sequencer process. Embedded in every
    /// [`types::TxRef`] this sequencer writes onto tx_ordering so
    /// downstream consumers can route the ref back to the correct
    /// per-sequencer tx_data archive.
    ///
    /// **Invariant:** `sequencer_id` matches `partition_index` for the
    /// default M=8 deployment (one sequencer per partition). The field is
    /// kept separate so a future asymmetric layout (e.g. multiple
    /// sequencers per partition for hot-standby pre-allocation) can change
    /// it without affecting the partition router. The CLI/TOML may omit
    /// it; if absent, it defaults to `partition_index as u8`.
    pub sequencer_id: u8,
    /// Per-sender future-nonce buffer capacity. Default 16.
    pub max_pending_per_sender: usize,
    /// Optional CPU core to pin this process to. `None` = no pin.
    pub core_id: Option<usize>,
    /// Backpressure behaviour when tx_ordering blocks.
    pub backpressure_policy: BackpressurePolicy,
}

#[derive(Debug, Clone, Copy, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum BackpressurePolicy {
    /// Return `Err(Backpressure)` immediately.
    ReturnImmediately,
    /// Spin-retry up to `max_retries` times before returning `Err(Backpressure)`.
    SpinRetry { max_retries: u32 },
}

impl Default for SequencerConfig {
    fn default() -> Self {
        Self {
            partition_count: 8,
            partition_index: 0,
            sequencer_id: 0,
            max_pending_per_sender: 16,
            core_id: None,
            backpressure_policy: BackpressurePolicy::ReturnImmediately,
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

    /// Derive a default `sequencer_id` from `partition_index` when the
    /// caller wants the conventional "one sequencer per partition"
    /// layout. Helpful for CLI overrides and tests.
    pub fn with_sequencer_id_from_partition(mut self) -> Self {
        self.sequencer_id = self.partition_index as u8;
        self
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
    fn with_sequencer_id_from_partition_overrides() {
        let cfg = SequencerConfig {
            partition_index: 3,
            sequencer_id: 0,
            ..Default::default()
        }
        .with_sequencer_id_from_partition();
        assert_eq!(cfg.sequencer_id, 3);
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

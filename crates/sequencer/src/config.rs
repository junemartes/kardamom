//! Runtime configuration for a single sequencer process.

use std::num::NonZeroU32;

use serde::{Deserialize, Serialize};

use crate::partition::PartitionCount;

/// [`SequencerConfig::default`]'s partition count.
const DEFAULT_PARTITION_COUNT: NonZeroU32 = NonZeroU32::new(8).unwrap();

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct SequencerConfig {
    /// Total partitions in the cluster (M). Default 8. Never zero: serde
    /// rejects a `0` at TOML-parse time, so [`PartitionCount::index_of`]'s
    /// `%` never divides by zero.
    pub partition_count: PartitionCount,
    /// This process's partition index (`0..partition_count`).
    pub partition_index: u32,
    /// Stable identifier for this sequencer process. This sequencer embeds
    /// the id in every [`kardamom_types::TxRef`] it writes onto `tx_ordering`.
    /// This lets downstream consumers route the ref back to the correct
    /// per-sequencer `tx_data` archive.
    ///
    /// Invariant: `sequencer_id` matches `partition_index` in the default
    /// M=8 deployment (one sequencer per partition). The field stays
    /// separate so a future asymmetric layout (for example, multiple
    /// sequencers per partition for hot-standby pre-allocation) can change
    /// it without affecting the partition router. The CLI or TOML config
    /// may omit this field. If it is absent, it defaults to
    /// `partition_index as u8`.
    pub sequencer_id: u8,
    /// Per-sender future-nonce buffer capacity. Default 16.
    pub max_pending_per_sender: usize,
    /// Unused. The field stays only so a deployed TOML file that still
    /// carries the key keeps loading.
    #[serde(default)]
    pub nonce_floor_lag_ms: Option<u64>,
    /// Optional CPU core to pin this process to. `None` means no pin.
    pub core_id: Option<usize>,
    /// Backpressure behaviour when `tx_ordering` blocks.
    pub backpressure_policy: BackpressurePolicy,
    /// Aeron Cluster (Raft) sealer client config. `tx_ordering` always goes
    /// to the cluster ingress. There is no non-cluster path.
    #[serde(default)]
    pub cluster: ClusterConfig,
    /// Lag detection and receipt-floor resync settings.
    /// `resync.dedup_capacity` must equal the cluster's
    /// `-Dkardamom.cluster.dedupCapacity`.
    #[serde(default)]
    pub resync: crate::resync::ResyncConfig,
}

// The `[cluster]` TOML section has one definition. Every cluster client
// shares it, re-exported from `kardamom-cluster-adapter`.
pub use kardamom_cluster_adapter::ClusterConfig;

#[derive(Debug, Clone, Copy, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum BackpressurePolicy {
    /// Return `Err(Backpressure)` immediately.
    ReturnImmediately,
}

impl Default for SequencerConfig {
    fn default() -> Self {
        Self {
            partition_count: PartitionCount::new(DEFAULT_PARTITION_COUNT),
            partition_index: 0,
            sequencer_id: 0,
            max_pending_per_sender: 16,
            nonce_floor_lag_ms: None,
            core_id: None,
            backpressure_policy: BackpressurePolicy::ReturnImmediately,
            cluster: ClusterConfig::default(),
            resync: crate::resync::ResyncConfig::default(),
        }
    }
}

impl SequencerConfig {
    /// # Errors
    ///
    /// Returns [`ConfigError::IndexOutOfRange`] if `partition_index` is
    /// not below `partition_count`. (`partition_count` itself is never
    /// zero: that invariant is carried by its `NonZeroU32` type, checked
    /// once at the TOML/CLI parse boundary.) Returns
    /// [`ConfigError::Resync`] if `[resync]` fails its own validation
    /// (see [`crate::resync::ResyncConfig::validate`]).
    pub fn validate(&self) -> Result<(), ConfigError> {
        if self.partition_index >= self.partition_count.get() {
            return Err(ConfigError::IndexOutOfRange {
                index: self.partition_index,
                count: self.partition_count.get(),
            });
        }
        self.resync.validate()?;
        Ok(())
    }

    /// Rotate the shard assignment for a racing-replica group:
    /// `partition_index = (partition_index + offset) % partition_count`.
    /// `sequencer_id` always follows the rotated partition.
    ///
    /// A second replica group passes `offset = 1`. Then each node serves a
    /// different shard per group. This guarantees that the two replicas of
    /// any shard land on distinct nodes.
    ///
    /// This function always re-derives `sequencer_id`. The `tx_data`
    /// subscription is keyed on `sequencer_id`, but the wrong-shard guard
    /// filters on `partition_index`. So a rotated replica with a different
    /// explicit id would subscribe to one shard's stream, and drop every
    /// envelope as wrong-shard. Its twin would also stamp a different
    /// `TxRef.shard_id`, which breaks the byte-identical-replica dedup
    /// design. For the same reason, the binary rejects `--sequencer-id`
    /// combined with `--partition-offset`.
    ///
    /// # Errors
    ///
    /// Returns [`ConfigError::PartitionOffsetOverflow`] if
    /// `partition_index + offset` overflows `u32`, or
    /// [`ConfigError::PartitionIndexNotU8`] if the rotated index does not
    /// fit in a `u8` (`sequencer_id` is a wire byte).
    pub fn rotate_partition(&mut self, offset: u32) -> Result<(), ConfigError> {
        let sum = self.partition_index.checked_add(offset).ok_or(
            ConfigError::PartitionOffsetOverflow {
                index: self.partition_index,
                offset,
            },
        )?;
        self.partition_index = sum % self.partition_count.get();
        self.sequencer_id =
            u8::try_from(self.partition_index).map_err(|_| ConfigError::PartitionIndexNotU8 {
                index: self.partition_index,
            })?;
        Ok(())
    }
}

#[derive(Debug, thiserror::Error)]
pub enum ConfigError {
    #[error("partition_index {index} >= partition_count {count}")]
    IndexOutOfRange { index: u32, count: u32 },
    #[error("partition_index {index} + partition_offset {offset} overflows u32")]
    PartitionOffsetOverflow { index: u32, offset: u32 },
    #[error("partition_index {index} does not fit in a u8 (sequencer_id is a wire byte)")]
    PartitionIndexNotU8 { index: u32 },
    #[error(transparent)]
    Resync(#[from] crate::resync::ResyncConfigError),
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
    fn zero_partition_count_rejected_at_parse() {
        // The NonZeroU32 field is the parse-once boundary: a zero count
        // never becomes a `SequencerConfig` at all.
        let toml = r#"
            partition_count = 0
            partition_index = 0
            sequencer_id = 0
            max_pending_per_sender = 16
            backpressure_policy = "return_immediately"
        "#;
        assert!(toml::from_str::<SequencerConfig>(toml).is_err());
    }

    #[test]
    fn rotate_partition_wraps_and_updates_sequencer_id() {
        let mut cfg = SequencerConfig {
            partition_count: PartitionCount::new(NonZeroU32::new(2).unwrap()),
            partition_index: 1,
            sequencer_id: 1,
            ..Default::default()
        };
        cfg.rotate_partition(1).unwrap();
        assert_eq!(cfg.partition_index, 0);
        assert_eq!(cfg.sequencer_id, 0);
        cfg.validate().unwrap();

        // Rotating the peer node's raw index 0 lands on the other shard.
        // So node-0 serves {a: shard 0, b: shard 1}, and node-1 the reverse.
        let mut peer = SequencerConfig {
            partition_count: PartitionCount::new(NonZeroU32::new(2).unwrap()),
            partition_index: 0,
            sequencer_id: 0,
            ..Default::default()
        };
        peer.rotate_partition(1).unwrap();
        assert_eq!(peer.partition_index, 1);
        assert_eq!(peer.sequencer_id, 1);
    }

    #[test]
    fn rotate_partition_overrides_explicit_sequencer_id() {
        // A different explicit id would subscribe to tx_data stream 7,
        // while the wrong-shard guard filters on partition 1. This drops
        // everything. So rotation always re-derives sequencer_id.
        let mut cfg = SequencerConfig {
            partition_count: PartitionCount::new(NonZeroU32::new(2).unwrap()),
            partition_index: 0,
            sequencer_id: 7,
            ..Default::default()
        };
        cfg.rotate_partition(1).unwrap();
        assert_eq!(cfg.partition_index, 1);
        assert_eq!(cfg.sequencer_id, 1);
    }

    #[test]
    fn rotate_partition_overflow_is_an_error() {
        let mut cfg = SequencerConfig {
            partition_count: PartitionCount::new(NonZeroU32::new(2).unwrap()),
            partition_index: u32::MAX,
            ..Default::default()
        };
        assert!(matches!(
            cfg.rotate_partition(1),
            Err(ConfigError::PartitionOffsetOverflow { .. })
        ));
    }

    #[test]
    fn toml_round_trip() {
        let cfg = SequencerConfig::default();
        let s = toml::to_string(&cfg).unwrap();
        let back: SequencerConfig = toml::from_str(&s).unwrap();
        assert_eq!(cfg, back);
    }

    #[test]
    fn invalid_resync_section_rejected_at_validate() {
        // `[resync]` is checked at the same parse-once boundary as
        // `partition_count`/`partition_index`, not only later when
        // `ResyncChannel::open` happens to be built from it.
        let cfg = SequencerConfig {
            resync: crate::resync::ResyncConfig {
                dedup_capacity: std::num::NonZeroU64::new(2).unwrap(),
                enter_percent: std::num::NonZeroU64::new(25).unwrap(),
                ..Default::default()
            },
            ..Default::default()
        };
        assert!(matches!(cfg.validate(), Err(ConfigError::Resync(_))));
    }
}

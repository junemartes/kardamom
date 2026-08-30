//! Runtime configuration for a single sequencer process.

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct SequencerConfig {
    /// Total partitions in the cluster (M). Default 8.
    pub partition_count: u32,
    /// This process's partition index (`0..partition_count`).
    pub partition_index: u32,
    /// Stable identifier for this sequencer process. This sequencer embeds
    /// the id in every [`kardamom_types::TxRef`] it writes onto tx_ordering.
    /// This lets downstream consumers route the ref back to the correct
    /// per-sequencer tx_data archive.
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
    /// This field is unused. It is accepted only for config compatibility.
    /// It used to bound the stream-adaptive nonce-floor fast-forward. That
    /// feature was removed: it adopted client-abandoned nonce gaps into the
    /// canonical stream, and every executor fail-stops on that (see the
    /// note on `PartitionState`). The key still parses, so deployed TOML
    /// files that carry it keep loading.
    #[serde(default = "default_nonce_floor_lag_ms")]
    pub nonce_floor_lag_ms: u64,
    /// Optional CPU core to pin this process to. `None` means no pin.
    pub core_id: Option<usize>,
    /// Backpressure behaviour when tx_ordering blocks.
    pub backpressure_policy: BackpressurePolicy,
    /// Aeron Cluster (Raft) sealer client config. tx_ordering always goes
    /// to the cluster ingress. There is no non-cluster path.
    #[serde(default)]
    pub cluster: ClusterConfig,
    /// Lag detection and receipt-floor resync settings. See
    /// docs/agents/sequencer-lag-resync-spec.md. `resync.dedup_capacity`
    /// must equal the cluster's `-Dkardamom.cluster.dedupCapacity`.
    #[serde(default)]
    pub resync: crate::resync::ResyncConfig,
}

fn default_nonce_floor_lag_ms() -> u64 {
    5_000
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
            partition_count: 8,
            partition_index: 0,
            sequencer_id: 0,
            max_pending_per_sender: 16,
            nonce_floor_lag_ms: default_nonce_floor_lag_ms(),
            core_id: None,
            backpressure_policy: BackpressurePolicy::ReturnImmediately,
            cluster: ClusterConfig::default(),
            resync: crate::resync::ResyncConfig::default(),
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

    /// Rotate the shard assignment for a racing-replica group:
    /// `partition_index = (partition_index + offset) % partition_count`.
    /// `sequencer_id` always follows the rotated partition.
    ///
    /// A second replica group passes `offset = 1`. Then each node serves a
    /// different shard per group. This guarantees that the two replicas of
    /// any shard land on distinct nodes.
    ///
    /// This function always re-derives `sequencer_id`. The tx_data
    /// subscription is keyed on `sequencer_id`, but the wrong-shard guard
    /// filters on `partition_index`. So a rotated replica with a different
    /// explicit id would subscribe to one shard's stream, and drop every
    /// envelope as wrong-shard. Its twin would also stamp a different
    /// `TxRef.shard_id`, which breaks the byte-identical-replica dedup
    /// design. For the same reason, the binary rejects `--sequencer-id`
    /// combined with `--partition-offset`.
    pub fn rotate_partition(&mut self, offset: u32) {
        let m = self.partition_count.max(1);
        self.partition_index = (self.partition_index + offset) % m;
        self.sequencer_id = self.partition_index as u8;
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
    fn rotate_partition_wraps_and_updates_sequencer_id() {
        let mut cfg = SequencerConfig {
            partition_count: 2,
            partition_index: 1,
            sequencer_id: 1,
            ..Default::default()
        };
        cfg.rotate_partition(1);
        assert_eq!(cfg.partition_index, 0);
        assert_eq!(cfg.sequencer_id, 0);
        cfg.validate().unwrap();

        // Rotating the peer node's raw index 0 lands on the other shard.
        // So node-0 serves {a: shard 0, b: shard 1}, and node-1 the reverse.
        let mut peer = SequencerConfig {
            partition_count: 2,
            partition_index: 0,
            sequencer_id: 0,
            ..Default::default()
        };
        peer.rotate_partition(1);
        assert_eq!(peer.partition_index, 1);
        assert_eq!(peer.sequencer_id, 1);
    }

    #[test]
    fn rotate_partition_overrides_explicit_sequencer_id() {
        // A different explicit id would subscribe to tx_data stream 7,
        // while the wrong-shard guard filters on partition 1. This drops
        // everything. So rotation always re-derives sequencer_id.
        let mut cfg = SequencerConfig {
            partition_count: 2,
            partition_index: 0,
            sequencer_id: 7,
            ..Default::default()
        };
        cfg.rotate_partition(1);
        assert_eq!(cfg.partition_index, 1);
        assert_eq!(cfg.sequencer_id, 1);
    }

    #[test]
    fn toml_round_trip() {
        let cfg = SequencerConfig::default();
        let s = toml::to_string(&cfg).unwrap();
        let back: SequencerConfig = toml::from_str(&s).unwrap();
        assert_eq!(cfg, back);
    }
}

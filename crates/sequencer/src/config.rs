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
    /// [`kardamom_types::TxRef`] this sequencer writes onto tx_ordering so
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
    /// Aeron Cluster (Raft) sealer client config. tx_ordering ALWAYS goes to the
    /// cluster ingress — there is no longer a non-cluster path.
    #[serde(default)]
    pub cluster: ClusterConfig,
}

/// Aeron Cluster (Raft) sealer client config.
#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq, Default)]
#[serde(default)]
pub struct ClusterConfig {
    /// Retained for config-file backward compatibility; IGNORED — cluster mode
    /// is the only mode, so the sequencer always publishes to the cluster.
    pub enabled: bool,
    /// "memberId=host:port,…" cluster ingress endpoints.
    pub ingress_endpoints: String,
    pub initial_leader_member_id: i32,
    pub ingress_stream_id: i32,
    /// This client's egress (response) channel URI, e.g. "aeron:udp?endpoint=<ip>:<port>".
    pub egress_channel: String,
    pub egress_stream_id: i32,
    pub keep_alive_interval_ms: u64,
}

impl ClusterConfig {
    /// Sane stream-id / keepalive defaults when the TOML omits them.
    pub fn defaults_applied(mut self) -> Self {
        if self.ingress_stream_id == 0 {
            self.ingress_stream_id = 101;
        }
        if self.egress_stream_id == 0 {
            self.egress_stream_id = 102;
        }
        if self.keep_alive_interval_ms == 0 {
            self.keep_alive_interval_ms = 1000;
        }
        self
    }

    pub fn to_live(&self) -> kardamom_cluster_adapter::LiveClusterConfig {
        let c = self.clone().defaults_applied();
        kardamom_cluster_adapter::LiveClusterConfig {
            ingress_endpoints: c.ingress_endpoints,
            initial_leader_member_id: c.initial_leader_member_id,
            ingress_stream_id: c.ingress_stream_id,
            egress_channel: c.egress_channel,
            egress_stream_id: c.egress_stream_id,
            keep_alive_interval_ms: c.keep_alive_interval_ms,
        }
    }
}

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
            core_id: None,
            backpressure_policy: BackpressurePolicy::ReturnImmediately,
            cluster: ClusterConfig::default(),
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
    /// `partition_index ← (partition_index + offset) % partition_count`,
    /// with `sequencer_id` following the rotated partition (the
    /// `sequencer_id == partition_index` invariant) unless
    /// `keep_sequencer_id` is set (an explicit operator override).
    ///
    /// A second replica group passes `offset = 1` so each node serves a
    /// different shard per group, guaranteeing the two replicas of any
    /// shard land on distinct nodes.
    pub fn rotate_partition(&mut self, offset: u32, keep_sequencer_id: bool) {
        let m = self.partition_count.max(1);
        self.partition_index = (self.partition_index + offset) % m;
        if !keep_sequencer_id {
            self.sequencer_id = self.partition_index as u8;
        }
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
        cfg.rotate_partition(1, false);
        assert_eq!(cfg.partition_index, 0);
        assert_eq!(cfg.sequencer_id, 0);
        cfg.validate().unwrap();

        // Rotating the peer node's raw index 0 lands on the other shard, so
        // node-0 serves {a: shard 0, b: shard 1} and node-1 the reverse.
        let mut peer = SequencerConfig {
            partition_count: 2,
            partition_index: 0,
            sequencer_id: 0,
            ..Default::default()
        };
        peer.rotate_partition(1, false);
        assert_eq!(peer.partition_index, 1);
        assert_eq!(peer.sequencer_id, 1);
    }

    #[test]
    fn rotate_partition_keeps_explicit_sequencer_id() {
        let mut cfg = SequencerConfig {
            partition_count: 2,
            partition_index: 0,
            sequencer_id: 7,
            ..Default::default()
        };
        cfg.rotate_partition(1, true);
        assert_eq!(cfg.partition_index, 1);
        assert_eq!(cfg.sequencer_id, 7);
    }

    #[test]
    fn toml_round_trip() {
        let cfg = SequencerConfig::default();
        let s = toml::to_string(&cfg).unwrap();
        let back: SequencerConfig = toml::from_str(&s).unwrap();
        assert_eq!(cfg, back);
    }
}

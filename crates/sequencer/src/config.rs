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
    /// Stream-adaptive nonce-floor fast-forward lag (milliseconds).
    ///
    /// A (re)starting replica live-joins its shard's tx_data mid-stream, so
    /// an established sender's next tx arrives at some nonce `k` strictly
    /// above the locally hydrated floor, and the missing nonces `floor..k`
    /// will never reappear (live-join, no replay — the twin already ordered
    /// them). When a sender's pending buffer has held a run strictly above
    /// the floor for longer than this lag (i.e. comfortably past the
    /// ordering/commit latency bound, so the gap provably isn't in flight),
    /// the floor fast-forwards to the lowest buffered nonce and the run is
    /// published. Safe under racing replicas: refs the twin already offered
    /// are absorbed by the cluster's first-seen dedup, and a fast-forward
    /// only skips forward — per-publisher nonce order is preserved.
    ///
    /// `0` fast-forwards immediately (test-only). Default 5000 ms.
    #[serde(default = "default_nonce_floor_lag_ms")]
    pub nonce_floor_lag_ms: u64,
    /// Optional CPU core to pin this process to. `None` = no pin.
    pub core_id: Option<usize>,
    /// Backpressure behaviour when tx_ordering blocks.
    pub backpressure_policy: BackpressurePolicy,
    /// Aeron Cluster (Raft) sealer client config. tx_ordering ALWAYS goes to the
    /// cluster ingress — there is no longer a non-cluster path.
    #[serde(default)]
    pub cluster: ClusterConfig,
}

fn default_nonce_floor_lag_ms() -> u64 {
    5_000
}

// The `[cluster]` TOML section has ONE definition, shared by every cluster
// client and re-exported from `kardamom-cluster-adapter`.
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
    /// with `sequencer_id` always following the rotated partition.
    ///
    /// A second replica group passes `offset = 1` so each node serves a
    /// different shard per group, guaranteeing the two replicas of any
    /// shard land on distinct nodes.
    ///
    /// `sequencer_id` is unconditionally re-derived: the tx_data
    /// subscription is keyed on `sequencer_id` while the wrong-shard guard
    /// filters on `partition_index`, so a rotated replica with a diverging
    /// explicit id would subscribe to one shard's stream and drop every
    /// envelope as wrong-shard — and its twin would stamp a different
    /// `TxRef.shard_id`, breaking the byte-identical-replica dedup
    /// argument. The binary rejects `--sequencer-id` combined with
    /// `--partition-offset` for the same reason.
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

        // Rotating the peer node's raw index 0 lands on the other shard, so
        // node-0 serves {a: shard 0, b: shard 1} and node-1 the reverse.
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
        // A diverging explicit id would subscribe to tx_data stream 7 while
        // the wrong-shard guard filters on partition 1 — dropping everything.
        // Rotation therefore always re-derives sequencer_id.
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

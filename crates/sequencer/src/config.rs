//! Runtime configuration for a single sequencer process.

use kardamom_types::shard_map::{LANE_COUNT, ShardMap, VslotSet};
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
    /// The lifetime of a transaction that waits on a nonce gap, in
    /// milliseconds. A parked entry older than this expires with an
    /// explicit `Expired` error on the tx_errors channel. The deploy
    /// drives this value and the ingress `pending_receipt_timeout` from
    /// one `group_vars` value. Default 30 000. See
    /// `docs/specs/dynamic-sequencer-sizing.md`, section 3.3.
    #[serde(default = "default_tx_ttl_ms")]
    pub tx_ttl_ms: u64,
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
    /// The nonce lookup from an executor. Off when the endpoint list is
    /// empty. See `crate::lookup`.
    #[serde(default)]
    pub lookup: crate::lookup::LookupConfig,
    /// The own tx_data lane. `None` means `sequencer_id`. A ref for an
    /// envelope on this lane carries this lane. See
    /// `docs/specs/dynamic-sequencer-sizing.md`, section 3.2.
    #[serde(default)]
    pub lane: Option<u8>,
    /// The virtual slots this replica serves, in the text form
    /// `"0-7,16,32-47"`. `None` means the identity map: the slots of
    /// `partition_index` under `lane = vslot % partition_count`. The
    /// wrong-shard guard drops an envelope whose slot is not in the set.
    #[serde(default)]
    pub vslots: Option<VslotSet>,
    /// The old lanes to read during a resize, in addition to `lane`. An
    /// envelope on an old lane keeps that lane in its ref.
    #[serde(default)]
    pub extra_lanes: Vec<u8>,
    /// The incoming slots that start in shadow mode: the state machine
    /// runs, but the ref publisher and the tx_errors publisher stay
    /// suppressed until the warm-up passes. Must be a subset of `vslots`.
    #[serde(default)]
    pub shadow_vslots: VslotSet,
    /// The warm-up, in ms. Shadow mode ends this long after the
    /// subscriptions open. `None` means `tx_ttl_ms + 5000`. Must be at
    /// least `tx_ttl_ms`.
    #[serde(default)]
    pub shadow_warm_ms: Option<u64>,
}

fn default_nonce_floor_lag_ms() -> u64 {
    5_000
}

fn default_tx_ttl_ms() -> u64 {
    30_000
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
            tx_ttl_ms: default_tx_ttl_ms(),
            nonce_floor_lag_ms: default_nonce_floor_lag_ms(),
            core_id: None,
            backpressure_policy: BackpressurePolicy::ReturnImmediately,
            cluster: ClusterConfig::default(),
            resync: crate::resync::ResyncConfig::default(),
            lookup: crate::lookup::LookupConfig::default(),
            lane: None,
            vslots: None,
            extra_lanes: Vec::new(),
            shadow_vslots: VslotSet::EMPTY,
            shadow_warm_ms: None,
        }
    }
}

impl SequencerConfig {
    /// The own tx_data lane.
    pub fn lane(&self) -> u8 {
        self.lane.unwrap_or(self.sequencer_id)
    }

    /// Every lane this replica reads: the own lane first, then the old
    /// lanes of a resize.
    pub fn lanes(&self) -> Vec<u8> {
        std::iter::once(self.lane())
            .chain(self.extra_lanes.iter().copied())
            .collect()
    }

    /// The virtual slots this replica serves. An explicit set wins. The
    /// default is the identity map over `partition_count`.
    pub fn vslot_set(&self) -> Result<VslotSet, ConfigError> {
        match self.vslots {
            Some(set) => Ok(set),
            None => {
                let map =
                    ShardMap::identity(self.partition_count).map_err(ConfigError::LanePlane)?;
                Ok(map.vslot_set(self.partition_index as u8))
            }
        }
    }

    /// The shadow warm-up.
    pub fn shadow_warm(&self) -> std::time::Duration {
        std::time::Duration::from_millis(self.shadow_warm_ms.unwrap_or(self.tx_ttl_ms + 5_000))
    }

    pub fn validate(&self) -> Result<(), ConfigError> {
        if self.tx_ttl_ms == 0 {
            return Err(ConfigError::ZeroTtl);
        }
        if self.vslots.is_none() {
            // The identity map needs a count that fits the lane plane and
            // an index inside it.
            if self.partition_count == 0 {
                return Err(ConfigError::ZeroPartitions);
            }
            crate::partition::validate_partition_count(self.partition_count)
                .map_err(ConfigError::LanePlane)?;
            if self.partition_index >= self.partition_count {
                return Err(ConfigError::IndexOutOfRange {
                    index: self.partition_index,
                    count: self.partition_count,
                });
            }
        }
        let lane = self.lane();
        if lane >= LANE_COUNT {
            return Err(ConfigError::LaneOutOfPlane(lane));
        }
        if let Some(bad) = self.extra_lanes.iter().find(|l| **l >= LANE_COUNT) {
            return Err(ConfigError::LaneOutOfPlane(*bad));
        }
        if self.extra_lanes.contains(&lane) {
            return Err(ConfigError::ExtraLaneIsOwn(lane));
        }
        let vslots = self.vslot_set()?;
        if !self.shadow_vslots.is_subset_of(&vslots) {
            return Err(ConfigError::ShadowNotSubset);
        }
        if !self.shadow_vslots.is_empty() && self.extra_lanes.is_empty() {
            return Err(ConfigError::ShadowWithoutOldLane);
        }
        if let Some(warm) = self.shadow_warm_ms
            && warm < self.tx_ttl_ms
        {
            return Err(ConfigError::ShadowWarmBelowTtl {
                warm_ms: warm,
                ttl_ms: self.tx_ttl_ms,
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
    #[error("tx_ttl_ms must be >= 1")]
    ZeroTtl,
    #[error("lane {0} is outside the lane plane of {LANE_COUNT} lanes")]
    LaneOutOfPlane(u8),
    #[error("extra_lanes contains the own lane {0}")]
    ExtraLaneIsOwn(u8),
    #[error("shadow_vslots is not a subset of vslots")]
    ShadowNotSubset,
    #[error("shadow_vslots needs at least one old lane in extra_lanes")]
    ShadowWithoutOldLane,
    #[error("shadow_warm_ms {warm_ms} is below tx_ttl_ms {ttl_ms}")]
    ShadowWarmBelowTtl { warm_ms: u64, ttl_ms: u64 },
    #[error("partition_count does not fit the lane plane: {0}")]
    LanePlane(#[from] crate::partition::PartitionConfigError),
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
    fn partition_count_outside_the_lane_plane_rejected() {
        for count in [3u32, 16] {
            let cfg = SequencerConfig {
                partition_count: count,
                ..Default::default()
            };
            assert!(matches!(cfg.validate(), Err(ConfigError::LanePlane(_))));
        }
    }

    #[test]
    fn explicit_vslots_skip_the_identity_checks() {
        // A new shard on lane 2 under a 3-lane map. No identity map fits.
        let cfg = SequencerConfig {
            partition_count: 3,
            partition_index: 2,
            sequencer_id: 2,
            lane: Some(2),
            vslots: Some(VslotSet::parse("2,5,8").unwrap()),
            extra_lanes: vec![0, 1],
            shadow_vslots: VslotSet::parse("2,5,8").unwrap(),
            ..Default::default()
        };
        cfg.validate().unwrap();
        assert_eq!(cfg.lane(), 2);
        assert_eq!(cfg.lanes(), vec![2, 0, 1]);
        assert_eq!(cfg.vslot_set().unwrap().len(), 3);
        assert_eq!(cfg.shadow_warm(), std::time::Duration::from_millis(35_000));
    }

    #[test]
    fn default_vslots_follow_the_identity_map() {
        let cfg = SequencerConfig {
            partition_count: 2,
            partition_index: 1,
            sequencer_id: 1,
            ..Default::default()
        };
        let set = cfg.vslot_set().unwrap();
        assert_eq!(set.len(), 128);
        assert!(set.contains(1) && !set.contains(0));
        assert_eq!(cfg.lane(), 1);
    }

    #[test]
    fn resize_config_is_checked() {
        let base = SequencerConfig {
            lane: Some(2),
            vslots: Some(VslotSet::parse("2,5").unwrap()),
            ..Default::default()
        };
        let shadow_not_subset = SequencerConfig {
            shadow_vslots: VslotSet::parse("9").unwrap(),
            extra_lanes: vec![0],
            ..base.clone()
        };
        assert!(matches!(
            shadow_not_subset.validate(),
            Err(ConfigError::ShadowNotSubset)
        ));
        let shadow_without_lane = SequencerConfig {
            shadow_vslots: VslotSet::parse("2").unwrap(),
            ..base.clone()
        };
        assert!(matches!(
            shadow_without_lane.validate(),
            Err(ConfigError::ShadowWithoutOldLane)
        ));
        let own_in_extra = SequencerConfig {
            extra_lanes: vec![2],
            ..base.clone()
        };
        assert!(matches!(
            own_in_extra.validate(),
            Err(ConfigError::ExtraLaneIsOwn(2))
        ));
        let lane_out = SequencerConfig {
            lane: Some(8),
            ..base.clone()
        };
        assert!(matches!(
            lane_out.validate(),
            Err(ConfigError::LaneOutOfPlane(8))
        ));
        let warm_short = SequencerConfig {
            shadow_warm_ms: Some(1_000),
            ..base.clone()
        };
        assert!(matches!(
            warm_short.validate(),
            Err(ConfigError::ShadowWarmBelowTtl { .. })
        ));
        let toml_form: SequencerConfig = toml::from_str(
            "partition_count = 2\npartition_index = 0\nsequencer_id = 0\n\
             max_pending_per_sender = 16\nbackpressure_policy = \"return_immediately\"\n\
             lane = 2\nvslots = \"2,5,8\"\nextra_lanes = [0, 1]\nshadow_vslots = \"5\"\n",
        )
        .unwrap();
        assert_eq!(toml_form.vslots.unwrap().to_string(), "2,5,8");
        assert_eq!(toml_form.shadow_vslots.to_string(), "5");
    }

    #[test]
    fn zero_ttl_rejected() {
        let cfg = SequencerConfig {
            tx_ttl_ms: 0,
            ..Default::default()
        };
        assert!(matches!(cfg.validate(), Err(ConfigError::ZeroTtl)));
    }

    #[test]
    fn tx_ttl_defaults_when_the_toml_omits_it() {
        let cfg: SequencerConfig = toml::from_str(
            "partition_count = 2\npartition_index = 0\nsequencer_id = 0\n\
             max_pending_per_sender = 16\nbackpressure_policy = \"return_immediately\"\n",
        )
        .unwrap();
        assert_eq!(cfg.tx_ttl_ms, 30_000);
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

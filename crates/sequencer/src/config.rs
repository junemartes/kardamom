//! Runtime configuration for a single sequencer process.

use std::num::{NonZeroU32, NonZeroU64};
use std::time::Duration;

use kardamom_types::shard_map::{LANE_COUNT, VslotSet};
use serde::{Deserialize, Serialize};

use crate::partition::PartitionCount;

/// [`SequencerConfig::default`]'s partition count.
const DEFAULT_PARTITION_COUNT: NonZeroU32 = NonZeroU32::new(8).unwrap();

/// [`SequencerConfig::default`]'s `tx_ttl_ms`.
const DEFAULT_TX_TTL_MS: NonZeroU64 = NonZeroU64::new(30_000).unwrap();

/// The shadow warm-up adds this many milliseconds to `tx_ttl_ms` when
/// `shadow_warm_ms` is absent.
const SHADOW_WARM_MARGIN_MS: u64 = 5_000;

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct SequencerConfig {
    /// Total partitions in the cluster (M). Default 8. Never zero: serde
    /// rejects a `0` at TOML-parse time, so [`PartitionCount::index_of`]'s
    /// `%` never divides by zero. A count outside the lane plane needs an
    /// explicit `vslots` set; [`Self::validate`] checks that.
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
    /// The lifetime of a transaction that waits on a nonce gap, in
    /// milliseconds. A parked entry older than this expires with an
    /// explicit `Expired` error on the `tx_errors` channel. The deploy
    /// drives this value and the ingress `pending_receipt_timeout` from
    /// one `group_vars` value. Default 30 000. Never zero: serde rejects
    /// a `0` at parse time. See `docs/specs/dynamic-sequencer-sizing.md`,
    /// section 3.3.
    #[serde(default = "default_tx_ttl_ms")]
    pub tx_ttl_ms: NonZeroU64,
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
    /// The nonce lookup from an executor. Off when the endpoint list is
    /// empty. See `crate::lookup`.
    #[serde(default)]
    pub lookup: crate::lookup::LookupConfig,
    /// The own `tx_data` lane. `None` means `sequencer_id`. A ref for an
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
    /// runs, but the ref publisher and the `tx_errors` publisher stay
    /// suppressed until the warm-up passes. Must be a subset of `vslots`.
    #[serde(default)]
    pub shadow_vslots: VslotSet,
    /// The warm-up, in ms. Shadow mode ends this long after the
    /// subscriptions open. `None` means `tx_ttl_ms + 5000`. Must be at
    /// least `tx_ttl_ms`.
    #[serde(default)]
    pub shadow_warm_ms: Option<u64>,
}

fn default_tx_ttl_ms() -> NonZeroU64 {
    DEFAULT_TX_TTL_MS
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
            tx_ttl_ms: DEFAULT_TX_TTL_MS,
            nonce_floor_lag_ms: None,
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
    /// The own `tx_data` lane.
    #[must_use]
    pub fn lane(&self) -> u8 {
        self.lane.unwrap_or(self.sequencer_id)
    }

    /// Every lane this replica reads: the own lane first, then the old
    /// lanes of a resize.
    #[must_use]
    pub fn lanes(&self) -> Vec<u8> {
        std::iter::once(self.lane())
            .chain(self.extra_lanes.iter().copied())
            .collect()
    }

    /// The transaction lifetime, [`Self::tx_ttl_ms`] as a `Duration`.
    #[must_use]
    pub fn tx_ttl(&self) -> Duration {
        Duration::from_millis(self.tx_ttl_ms.get())
    }

    /// The virtual slots this replica serves. An explicit set wins. The
    /// default is the identity map over `partition_count`.
    ///
    /// # Errors
    ///
    /// Returns [`ConfigError::LanePlane`] if the identity map needs a
    /// count outside the lane plane, or
    /// [`ConfigError::PartitionIndexNotU8`] if `partition_index` is not a
    /// lane index.
    pub fn vslot_set(&self) -> Result<VslotSet, ConfigError> {
        if let Some(set) = self.vslots {
            return Ok(set);
        }
        let map = self.partition_count.identity_map()?;
        let lane =
            u8::try_from(self.partition_index).map_err(|_| ConfigError::PartitionIndexNotU8 {
                index: self.partition_index,
            })?;
        Ok(map.vslot_set(lane))
    }

    /// The shadow warm-up.
    #[must_use]
    pub fn shadow_warm(&self) -> Duration {
        Duration::from_millis(
            self.shadow_warm_ms
                .unwrap_or_else(|| self.tx_ttl_ms.get().saturating_add(SHADOW_WARM_MARGIN_MS)),
        )
    }

    /// # Errors
    ///
    /// Returns [`ConfigError::IndexOutOfRange`] if `partition_index` is
    /// not below `partition_count` under the identity map, or
    /// [`ConfigError::LanePlane`] if the identity map needs a count outside
    /// the lane plane. (`partition_count` and `tx_ttl_ms` are never zero:
    /// their `NonZero` types are checked once at the TOML/CLI parse
    /// boundary.) Returns the lane and shadow errors below for a resize
    /// layout that does not fit the lane plane, and
    /// [`ConfigError::Resync`] if `[resync]` fails its own validation
    /// (see [`crate::resync::ResyncConfig::validate`]).
    pub fn validate(&self) -> Result<(), ConfigError> {
        self.validate_identity_layout()?;
        self.validate_lanes()?;
        self.validate_shadow()?;
        self.resync.validate()?;
        Ok(())
    }

    /// Under the identity map, the count must fit the lane plane and the
    /// index must be inside it. An explicit vslot set skips both checks.
    fn validate_identity_layout(&self) -> Result<(), ConfigError> {
        if self.vslots.is_some() {
            return Ok(());
        }
        self.partition_count.identity_map()?;
        if self.partition_index >= self.partition_count.get() {
            return Err(ConfigError::IndexOutOfRange {
                index: self.partition_index,
                count: self.partition_count.get(),
            });
        }
        Ok(())
    }

    /// Every lane this replica reads is inside the lane plane, and the
    /// own lane is not listed again as an old lane.
    fn validate_lanes(&self) -> Result<(), ConfigError> {
        let lane = self.lane();
        let out_of_plane = self.lanes().into_iter().find(|l| *l >= LANE_COUNT);
        if let Some(bad) = out_of_plane {
            return Err(ConfigError::LaneOutOfPlane(bad));
        }
        if self.extra_lanes.contains(&lane) {
            return Err(ConfigError::ExtraLaneIsOwn(lane));
        }
        Ok(())
    }

    /// The shadow set is a subset of the served set, has an old lane to
    /// read from, and its warm-up covers one transaction lifetime.
    fn validate_shadow(&self) -> Result<(), ConfigError> {
        let vslots = self.vslot_set()?;
        if !self.shadow_vslots.is_subset_of(&vslots) {
            return Err(ConfigError::ShadowNotSubset);
        }
        if !self.shadow_vslots.is_empty() && self.extra_lanes.is_empty() {
            return Err(ConfigError::ShadowWithoutOldLane);
        }
        let ttl_ms = self.tx_ttl_ms.get();
        match self.shadow_warm_ms {
            Some(warm_ms) if warm_ms < ttl_ms => {
                Err(ConfigError::ShadowWarmBelowTtl { warm_ms, ttl_ms })
            }
            _ => Ok(()),
        }
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
    LanePlane(#[from] kardamom_types::shard_map::ShardMapError),
    #[error(transparent)]
    Resync(#[from] crate::resync::ResyncConfigError),
}

#[cfg(test)]
mod tests {
    use super::*;

    fn m(n: u32) -> PartitionCount {
        PartitionCount::new(NonZeroU32::new(n).unwrap())
    }

    /// The five keys every TOML form in these tests needs.
    const BASE_TOML: &str = "partition_count = 2\npartition_index = 0\nsequencer_id = 0\n\
         max_pending_per_sender = 16\nbackpressure_policy = \"return_immediately\"\n";

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
        let toml = BASE_TOML.replace("partition_count = 2", "partition_count = 0");
        assert!(toml::from_str::<SequencerConfig>(&toml).is_err());
    }

    #[test]
    fn partition_count_outside_the_lane_plane_rejected() {
        for count in [3u32, 16] {
            let cfg = SequencerConfig {
                partition_count: m(count),
                ..Default::default()
            };
            assert!(matches!(cfg.validate(), Err(ConfigError::LanePlane(_))));
        }
    }

    #[test]
    fn explicit_vslots_skip_the_identity_checks() {
        // A new shard on lane 2 under a 3-lane map. No identity map fits.
        let cfg = SequencerConfig {
            partition_count: m(3),
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
        assert_eq!(cfg.shadow_warm(), Duration::from_secs(35));
    }

    #[test]
    fn default_vslots_follow_the_identity_map() {
        let cfg = SequencerConfig {
            partition_count: m(2),
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
        let toml_form: SequencerConfig = toml::from_str(&format!(
            "{BASE_TOML}lane = 2\nvslots = \"2,5,8\"\nextra_lanes = [0, 1]\nshadow_vslots = \"5\"\n"
        ))
        .unwrap();
        assert_eq!(toml_form.vslots.unwrap().to_string(), "2,5,8");
        assert_eq!(toml_form.shadow_vslots.to_string(), "5");
    }

    #[test]
    fn zero_ttl_rejected_at_parse() {
        let toml = format!("{BASE_TOML}tx_ttl_ms = 0\n");
        assert!(toml::from_str::<SequencerConfig>(&toml).is_err());
    }

    #[test]
    fn tx_ttl_defaults_when_the_toml_omits_it() {
        let cfg: SequencerConfig = toml::from_str(BASE_TOML).unwrap();
        assert_eq!(cfg.tx_ttl_ms.get(), 30_000);
        assert_eq!(cfg.tx_ttl(), Duration::from_secs(30));
    }

    #[test]
    fn rotate_partition_wraps_and_updates_sequencer_id() {
        let mut cfg = SequencerConfig {
            partition_count: m(2),
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
            partition_count: m(2),
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
            partition_count: m(2),
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
            partition_count: m(2),
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

    #[test]
    fn the_deployed_sequencer_template_loads_and_validates() {
        // `deploy/cluster/config/sequencer.toml.tpl` is what the container
        // cluster's sequencers read. It must parse with this crate's types
        // and pass `validate`.
        let path = concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../../deploy/cluster/config/sequencer.toml.tpl"
        );
        let raw = std::fs::read_to_string(path).expect("read the deployed template");
        let cfg: SequencerConfig =
            toml::from_str(&raw).expect("deployed sequencer.toml.tpl parses");
        cfg.validate()
            .expect("deployed sequencer.toml.tpl validates");
    }
}

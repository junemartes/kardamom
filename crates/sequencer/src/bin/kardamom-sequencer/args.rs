//! The flags of `kardamom-sequencer`, and how they override the TOML
//! config.

use std::num::{NonZeroU32, NonZeroU64};
use std::path::PathBuf;

use anyhow::{Context, Result};
use clap::Parser;
use kardamom_sequencer::config::SequencerConfig;
use kardamom_types::shard_map::VslotSet;

#[derive(Debug, Parser)]
#[command(
    name = "kardamom-sequencer",
    version,
    about = "kardamom sequencer process"
)]
pub(crate) struct Args {
    /// Path to a TOML config file (schema: `SequencerConfig`).
    #[arg(long)]
    pub(crate) config: PathBuf,
    /// Optional `LogConfig` TOML that supplies the Aeron `[channels]`
    /// config. If unset, it uses built-in single-host IPC defaults (this
    /// keeps local and e2e behavior). Multi-host deployments point this at
    /// the rendered UDP channels config.
    #[arg(long, env = "KARDAMOM_LOG_CONFIG")]
    pub(crate) log_config: Option<PathBuf>,
    /// Aeron Media Driver directory (`aeron.dir`).
    #[arg(long)]
    pub(crate) aeron_dir: Option<PathBuf>,
    /// Override the partition index from the config.
    #[arg(long)]
    partition_index: Option<u32>,
    /// Override the partition count (M).
    #[arg(long)]
    partition_count: Option<NonZeroU32>,
    /// Replica-group shard rotation. The effective partition becomes
    /// `(partition_index + partition_offset) % partition_count`, and
    /// `sequencer_id` follows it, unless explicitly overridden. This lets
    /// a second Nomad group of racing replicas reuse the same
    /// node-derived `--partition-index` while it serves a rotated shard.
    /// So the two replicas of any shard land on different nodes,
    /// deterministically. This is incompatible with an explicit
    /// `--sequencer-id`: the `tx_data` subscription and `TxRef.shard_id`
    /// must both follow the rotated shard.
    ///
    /// Racing replicas are safe by construction. Refs encode
    /// deterministically from the shared per-shard `tx_data` stream, and
    /// the Aeron Cluster dedups records by `canonical_id` first-seen. This
    /// is the same mechanism that already absorbs the M duplicate
    /// `DepositRefs`.
    #[arg(long, default_value_t = 0)]
    partition_offset: u32,
    /// Override the sequencer id embedded in every `tx_ordering` `TxRef`.
    /// If omitted and the TOML did not set it, falls back to
    /// `partition_index as u8`.
    #[arg(long)]
    sequencer_id: Option<u8>,
    /// The own `tx_data` lane (`lane`). Defaults to the sequencer id.
    #[arg(long, env = "KARDAMOM_LANE")]
    lane: Option<u8>,
    /// The vslots this replica serves (`vslots`), as ranges: `0-7,16`.
    /// Defaults to the identity map over the partition count.
    #[arg(long, env = "KARDAMOM_VSLOTS")]
    vslots: Option<VslotSet>,
    /// The old lanes to read during a resize (`extra_lanes`).
    #[arg(long, env = "KARDAMOM_EXTRA_LANES", value_delimiter = ',')]
    extra_lanes: Vec<u8>,
    /// The incoming vslots that start in shadow mode (`shadow_vslots`).
    #[arg(long, env = "KARDAMOM_SHADOW_VSLOTS")]
    shadow_vslots: Option<VslotSet>,
    /// The shadow warm-up in ms (`shadow_warm_ms`). Defaults to
    /// `tx_ttl_ms + 5000`.
    #[arg(long, env = "KARDAMOM_SHADOW_WARM_MS")]
    shadow_warm_ms: Option<u64>,
    /// The lifetime of a transaction that waits on a nonce gap, in ms
    /// (`tx_ttl_ms`). The deploy passes the same value to the ingress as
    /// `--pending-receipt-timeout-ms`. Zero is rejected.
    #[arg(long, env = "KARDAMOM_TX_TTL_MS")]
    tx_ttl_ms: Option<NonZeroU64>,
    /// Override the CPU core to pin to.
    #[arg(long)]
    core_id: Option<usize>,
    /// This node's cluster-egress endpoint `ip:port` (cluster mode). Sets
    /// or overrides the `[cluster] egress_channel` as
    /// `aeron:udp?endpoint=<ip:port>`. The Nomad job injects this per node
    /// as `${meta.node_ip}:${NOMAD_HOST_PORT_egress}`.
    #[arg(long, env = "KARDAMOM_CLUSTER_EGRESS_ENDPOINT")]
    cluster_egress_endpoint: Option<String>,
    /// Address for the Prometheus /metrics HTTP listener.
    #[arg(long, env = "KARDAMOM_METRICS_ADDR", default_value = "127.0.0.1:9001")]
    pub(crate) metrics_addr: std::net::SocketAddr,
    /// Host identifier; stamped on every metric.
    #[arg(long, env = "KARDAMOM_HOST_ID", default_value = "local")]
    pub(crate) host_id: kardamom_obs::HostId,
    /// The cluster's first-seen dedup window (`[resync] dedup_capacity`).
    /// Must equal the JVM's `-Dkardamom.cluster.dedupCapacity`: the lag
    /// horizon the resync mechanism protects.
    #[arg(long, env = "KARDAMOM_CLUSTER_DEDUP_CAPACITY")]
    cluster_dedup_capacity: Option<NonZeroU64>,
    /// Watermark-jump enter threshold as a percent of the dedup capacity
    /// (`[resync] enter_percent`).
    #[arg(long, env = "KARDAMOM_RESYNC_ENTER_PERCENT")]
    resync_enter_percent: Option<NonZeroU64>,
    /// Boundary-silence resync trigger, ms (`[resync] boundary_silence_ms`).
    #[arg(long, env = "KARDAMOM_RESYNC_BOUNDARY_SILENCE_MS")]
    resync_boundary_silence_ms: Option<u64>,
    /// The executor nonce query endpoints, as a comma-separated list of
    /// `http://host:port` (`[lookup] executor_endpoints`). Empty means no
    /// lookup. See `docs/specs/dynamic-sequencer-sizing.md`, section 3.4.
    #[arg(long, env = "KARDAMOM_EXECUTOR_QUERY_ENDPOINTS", value_delimiter = ',')]
    executor_query_endpoints: Vec<String>,
    /// Executor replica count for the `tx_receipts` MDS fan-in (parity with
    /// the validator). Falls back to `channels.tx_receipts_executor_count`.
    /// Not relevant when receipts ride multicast (the cluster deploy).
    #[arg(long)]
    pub(crate) executor_count: Option<NonZeroU32>,
    /// Priority fees on or off (`[fees] priority`). Must match the
    /// sealer's `-Dkardamom.cluster.orderingWindow` (20 for on, 0 for
    /// off): the deploy sets both from one value.
    #[arg(long, env = "KARDAMOM_PRIORITY_FEES")]
    priority_fees: Option<bool>,
    /// The decision version of the sealer that a new process keeps until
    /// the sealer reports its version (egress kind 17), from 0 to 5.
    /// Default 0. The deploy sets the lowest decision version of the
    /// members. With 5, a new process offers each transaction with its
    /// bytes from the first offer. A report of the current session that
    /// says a lower version lowers the kept version, so a wrong floor lasts
    /// until the first report. Set 0 before a fleet rebuild from a seed.
    #[arg(
        long,
        env = "KARDAMOM_SEALER_VERSION_FLOOR",
        default_value_t = 0,
        value_parser = clap::value_parser!(u8)
            .range(0..=i64::from(kardamom_cluster_adapter::wire::SEALER_V5_DECISION_VERSION))
    )]
    pub(crate) sealer_version_floor: u8,
    /// Publish the `sent` records of `tx_events`
    /// (`docs/specs/2026-10-25-client-events.md`). Best effort: a refused
    /// offer drops the frame, and the loop never waits for it.
    #[arg(long, env = "KARDAMOM_TX_EVENTS", default_value_t = false)]
    pub(crate) tx_events: bool,
}

impl Args {
    /// The origin of the `tx_events` frames of this replica: the replica
    /// group in the high byte and the rotated partition in the low byte,
    /// so the two twins of a lane differ.
    pub(crate) fn tx_events_origin(&self, cfg: &SequencerConfig) -> u16 {
        let group = u16::try_from(self.partition_offset).unwrap_or(u16::MAX) & 0xFF;
        let partition = u16::try_from(cfg.partition_index).unwrap_or(u16::MAX) & 0xFF;
        (group << 8) | partition
    }

    /// Fold the CLI and env overrides into the TOML-loaded config: the
    /// plain overrides first, then the replica-group shard rotation and the
    /// sequencer id fallback, which read the overridden partition index.
    pub(crate) fn apply_overrides(&self, cfg: &mut SequencerConfig) -> Result<()> {
        self.apply_plain_overrides(cfg);
        if self.partition_offset != 0 {
            // An explicit --sequencer-id combined with rotation would
            // subscribe to tx_data stream `sequencer_id`, while the
            // wrong-shard guard filters on the rotated `partition_index`. The
            // replica would silently drop every envelope, and its TxRefs
            // would stamp a shard_id its twin does not, breaking
            // byte-identical dedup.
            anyhow::ensure!(
                self.sequencer_id.is_none(),
                "--sequencer-id cannot be combined with --partition-offset: \
             the sequencer id must follow the rotated shard \
             (sequencer_id == partition_index)"
            );
            let raw_index = cfg.partition_index;
            cfg.rotate_partition(self.partition_offset)
                .context("rotate partition")?;
            tracing::info!(
                raw_index,
                offset = self.partition_offset,
                rotated = cfg.partition_index,
                "partition-offset: rotated shard assignment (racing replica group)"
            );
        }
        if let Some(id) = self.sequencer_id {
            cfg.sequencer_id = id;
        } else if cfg.sequencer_id == 0 && cfg.partition_index != 0 {
            // A count above 256 would otherwise truncate silently:
            // sequencer_id is a u8 on the wire (TxRef.shard_id).
            cfg.sequencer_id = u8::try_from(cfg.partition_index)
                .context("partition_index does not fit in sequencer_id (u8)")?;
        }
        Ok(())
    }

    /// The overrides that copy a set flag over the TOML value. A flag left
    /// unset, or an empty list, keeps the TOML value.
    fn apply_plain_overrides(&self, cfg: &mut SequencerConfig) {
        cfg.partition_index = self.partition_index.unwrap_or(cfg.partition_index);
        cfg.partition_count = self.partition_count.map_or(
            cfg.partition_count,
            kardamom_sequencer::partition::PartitionCount::new,
        );
        cfg.tx_ttl_ms = self.tx_ttl_ms.unwrap_or(cfg.tx_ttl_ms);
        if !self.executor_query_endpoints.is_empty() {
            cfg.lookup
                .executor_endpoints
                .clone_from(&self.executor_query_endpoints);
        }
        cfg.lane = self.lane.or(cfg.lane);
        cfg.vslots = self.vslots.or(cfg.vslots);
        if !self.extra_lanes.is_empty() {
            cfg.extra_lanes.clone_from(&self.extra_lanes);
        }
        cfg.shadow_vslots = self.shadow_vslots.unwrap_or(cfg.shadow_vslots);
        cfg.shadow_warm_ms = self.shadow_warm_ms.or(cfg.shadow_warm_ms);
        cfg.core_id = self.core_id.or(cfg.core_id);
        cfg.cluster
            .set_egress_endpoint(self.cluster_egress_endpoint.as_deref());
        let resync = &mut cfg.resync;
        resync.dedup_capacity = self.cluster_dedup_capacity.unwrap_or(resync.dedup_capacity);
        resync.enter_percent = self.resync_enter_percent.unwrap_or(resync.enter_percent);
        resync.boundary_silence_ms = self
            .resync_boundary_silence_ms
            .unwrap_or(resync.boundary_silence_ms);
        cfg.fees.priority = self.priority_fees.unwrap_or(cfg.fees.priority);
    }
}

#[cfg(test)]
#[path = "args_tests.rs"]
mod tests;

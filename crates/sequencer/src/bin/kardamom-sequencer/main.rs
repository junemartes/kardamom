//! `kardamom-sequencer`: per-partition sequencer process.
//!
//! Parses a TOML [`SequencerConfig`], opens one `tx_data` subscriber per
//! lane it reads, the Aeron Cluster (Raft) ref publisher (`tx_ordering`),
//! and a `tx_errors` publisher for rejection signals. Runs the sequencer
//! main loop on a dedicated blocking thread until SIGTERM or Ctrl-C.
//!
//! The lag-detection, receipt-floor, and nonce-lookup feed tasks live in
//! [`feeds`]. The `aeron_live` handles implement the sequencer's
//! subscriber/publisher traits directly (`kardamom_sequencer::{epoch,
//! remote_epoch, outbound}`); the lane set is [`feeds::LaneSubscriptions`].

mod feeds;

use std::num::{NonZeroU32, NonZeroU64};
use std::path::PathBuf;

use anyhow::{Context, Result};
use clap::Parser;
use kardamom_cluster_adapter::LiveCluster;
use kardamom_log::aeron_live::{
    AeronRuntime, TxDepositsSubscriberHandle, TxErrorsPublisherHandle,
    TxRemoteEpochsSubscriberHandle,
};
use kardamom_log::config::LogConfig;
use kardamom_log::discovery::StreamPlane;
use kardamom_obs::bin::wait_for_shutdown;
use kardamom_sequencer::config::SequencerConfig;
use kardamom_sequencer::lookup::LookupRequester;
use kardamom_sequencer::sequencer::Shutdown;
use kardamom_types::shard_map::VslotSet;

use feeds::{LaneSub, LaneSubscriptions, PublishLoops};

#[derive(Debug, Parser)]
#[command(
    name = "kardamom-sequencer",
    version,
    about = "kardamom sequencer process"
)]
struct Args {
    /// Path to a TOML config file (schema: `SequencerConfig`).
    #[arg(long)]
    config: PathBuf,
    /// Optional `LogConfig` TOML that supplies the Aeron `[channels]`
    /// config. If unset, it uses built-in single-host IPC defaults (this
    /// keeps local and e2e behavior). Multi-host deployments point this at
    /// the rendered UDP channels config.
    #[arg(long, env = "KARDAMOM_LOG_CONFIG")]
    log_config: Option<PathBuf>,
    /// Aeron Media Driver directory (`aeron.dir`).
    #[arg(long)]
    aeron_dir: Option<PathBuf>,
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
    /// as `${meta.node_ip}:<cluster_egress_port>`.
    #[arg(long, env = "KARDAMOM_CLUSTER_EGRESS_ENDPOINT")]
    cluster_egress_endpoint: Option<String>,
    /// Address for the Prometheus /metrics HTTP listener.
    #[arg(long, env = "KARDAMOM_METRICS_ADDR", default_value = "127.0.0.1:9001")]
    metrics_addr: std::net::SocketAddr,
    /// Host identifier; stamped on every metric.
    #[arg(long, env = "KARDAMOM_HOST_ID", default_value = "local")]
    host_id: kardamom_obs::HostId,
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
    executor_count: Option<NonZeroU32>,
}

/// Fold the CLI and env overrides into the TOML-loaded config:
/// partition index and count, the lane and vslot layout, the transaction
/// lifetime, the lookup endpoints, the replica-group shard rotation,
/// sequencer id fallback, core pin, per-node cluster egress endpoint, and
/// the resync contract settings.
fn apply_cli_overrides(args: &Args, cfg: &mut SequencerConfig) -> Result<()> {
    if let Some(i) = args.partition_index {
        cfg.partition_index = i;
    }
    if let Some(m) = args.partition_count {
        cfg.partition_count = kardamom_sequencer::partition::PartitionCount::new(m);
    }
    apply_layout_overrides(args, cfg);
    if args.partition_offset != 0 {
        // An explicit --sequencer-id combined with rotation would
        // subscribe to tx_data stream `sequencer_id`, while the
        // wrong-shard guard filters on the rotated `partition_index`. The
        // replica would silently drop every envelope, and its TxRefs
        // would stamp a shard_id its twin does not, breaking
        // byte-identical dedup.
        anyhow::ensure!(
            args.sequencer_id.is_none(),
            "--sequencer-id cannot be combined with --partition-offset: \
             the sequencer id must follow the rotated shard \
             (sequencer_id == partition_index)"
        );
        let raw_index = cfg.partition_index;
        cfg.rotate_partition(args.partition_offset)
            .context("rotate partition")?;
        tracing::info!(
            raw_index,
            offset = args.partition_offset,
            rotated = cfg.partition_index,
            "partition-offset: rotated shard assignment (racing replica group)"
        );
    }
    if let Some(id) = args.sequencer_id {
        cfg.sequencer_id = id;
    } else if cfg.sequencer_id == 0 && cfg.partition_index != 0 {
        // A count above 256 would otherwise truncate silently:
        // sequencer_id is a u8 on the wire (TxRef.shard_id).
        cfg.sequencer_id = u8::try_from(cfg.partition_index)
            .context("partition_index does not fit in sequencer_id (u8)")?;
    }
    if let Some(c) = args.core_id {
        cfg.core_id = Some(c);
    }
    // Per-node cluster egress endpoint. The cluster client's
    // egress_channel is this node's reachable address (the node IP
    // differs per replica). So the Nomad job injects it, instead of
    // baking it into the static config template.
    if let Some(ep) = args.cluster_egress_endpoint.as_deref() {
        cfg.cluster.egress_channel = format!("aeron:udp?endpoint={ep}");
    }
    if let Some(cap) = args.cluster_dedup_capacity {
        cfg.resync.dedup_capacity = cap;
    }
    if let Some(p) = args.resync_enter_percent {
        cfg.resync.enter_percent = p;
    }
    if let Some(ms) = args.resync_boundary_silence_ms {
        cfg.resync.boundary_silence_ms = ms;
    }
    Ok(())
}

/// The lane, vslot, shadow, lifetime, and lookup overrides of
/// [`apply_cli_overrides`]. A flag left unset keeps the TOML value.
fn apply_layout_overrides(args: &Args, cfg: &mut SequencerConfig) {
    if let Some(ttl) = args.tx_ttl_ms {
        cfg.tx_ttl_ms = ttl;
    }
    if !args.executor_query_endpoints.is_empty() {
        cfg.lookup
            .executor_endpoints
            .clone_from(&args.executor_query_endpoints);
    }
    if let Some(lane) = args.lane {
        cfg.lane = Some(lane);
    }
    if let Some(set) = args.vslots {
        cfg.vslots = Some(set);
    }
    if !args.extra_lanes.is_empty() {
        cfg.extra_lanes.clone_from(&args.extra_lanes);
    }
    if let Some(set) = args.shadow_vslots {
        cfg.shadow_vslots = set;
    }
    if let Some(warm) = args.shadow_warm_ms {
        cfg.shadow_warm_ms = Some(warm);
    }
}

/// The Aeron subscriptions and publisher this sequencer needs, all opened
/// on the same runtime `rt`.
struct Handles {
    /// One subscription per lane: the own lane, then the old lanes of a
    /// resize. Every lane shares the multicast group; a lane is a stream
    /// id. The shadow warm-up starts when the sequencer is constructed,
    /// after these open.
    data_subs: LaneSubscriptions,
    deposits_sub: TxDepositsSubscriberHandle,
    remote_epochs_sub: TxRemoteEpochsSubscriberHandle,
    errors_pub: TxErrorsPublisherHandle,
}

impl Handles {
    /// Open every handle this sequencer needs, for the lanes of `cfg`.
    /// `tx_errors` follows the plane's transport; the rest still open on
    /// their static channels.
    async fn open(
        rt: &AeronRuntime,
        plane: &mut StreamPlane,
        cfg: &SequencerConfig,
    ) -> Result<Self> {
        let channels = plane.channels().clone();
        let channels = &channels;
        let own = Self::open_lane(rt, plane, cfg.lane())?;
        let old = cfg
            .extra_lanes
            .iter()
            .map(|lane| Self::open_lane(rt, plane, *lane))
            .collect::<Result<Vec<_>>>()?;
        Ok(Self {
            data_subs: LaneSubscriptions::new(own, old),
            deposits_sub: TxDepositsSubscriberHandle::open(rt, channels)
                .context("open TxDepositsSubscriberHandle")?,
            remote_epochs_sub: TxRemoteEpochsSubscriberHandle::open(rt, channels)
                .context("open TxRemoteEpochsSubscriberHandle")?,
            errors_pub: plane
                .publisher::<TxErrorsPublisherHandle>(rt)
                .await
                .context("open TxErrorsPublisherHandle")?,
        })
    }

    /// Open the `tx_data` subscription of one lane through the plane.
    fn open_lane(rt: &AeronRuntime, plane: &mut StreamPlane, lane: u8) -> Result<LaneSub> {
        let handle = plane
            .tx_data_subscriber(rt, lane)
            .with_context(|| format!("open TxDataSubscriberHandle lane={lane}"))?;
        Ok(LaneSub { lane, handle })
    }
}

/// Lag detection, receipt-floor resync, and the nonce lookup: the
/// egress-watermark task, the receipts-floor task, the lookup task, the
/// `ResyncController` and the `LookupRequester` handed to the publish
/// loops. The feed tasks sit in their own field, so the caller can move
/// `controller` and `lookup` into the publish loops and still join the
/// tasks afterward through `feeds`.
struct ResyncWiring {
    controller: kardamom_sequencer::resync::ResyncController,
    /// `None` when the config has no executor query endpoints.
    lookup: Option<LookupRequester>,
    feeds: ResyncFeeds,
}

/// The egress-watermark, receipts-floor, and nonce-lookup tasks, plus the
/// `tx_data` runtime.
struct ResyncFeeds {
    watermark_task: tokio::task::JoinHandle<()>,
    receipts_task: tokio::task::JoinHandle<()>,
    /// `None` when the nonce lookup is off.
    lookup_task: Option<tokio::task::JoinHandle<()>>,
    #[allow(
        dead_code,
        reason = "held only so main_rt (the tx_data runtime) drops after the two feed tasks above are joined (see ResyncFeeds::join), and before the caller's receipts_rt goes out of scope — matching the resource teardown order the two runtimes' isolation comments assume; never read, its value is its Drop impl"
    )]
    main_rt: AeronRuntime,
}

impl ResyncFeeds {
    /// Await both feed tasks, then let `main_rt` drop.
    async fn join(self) {
        if let Err(e) = self.watermark_task.await {
            tracing::warn!(?e, "egress-watermark task panicked");
        }
        if let Err(e) = self.receipts_task.await {
            tracing::warn!(?e, "receipts-floors task panicked");
        }
        // The lookup task exits on the token, or on the closed request
        // channel after the main loop ends.
        if let Some(task) = self.lookup_task
            && let Err(e) = task.await
        {
            tracing::warn!(?e, "nonce-lookup task panicked");
        }
    }
}

impl ResyncWiring {
    fn spawn(
        cfg: &SequencerConfig,
        main_rt: AeronRuntime,
        cluster_egress: kardamom_cluster_adapter::LiveEgress,
        receipts_rt: &AeronRuntime,
        plane: &mut StreamPlane,
        executor_count: Option<NonZeroU32>,
        shutdown: &Shutdown,
    ) -> Result<Self> {
        // Three feeds go into the publish loop's ResyncController:
        //  1. The egress-watermark thread. The cluster broadcasts every
        //     boundary to this publisher session. Decode `end_tx_idx` (the
        //     global canonical count) into the shared watermark, and discard
        //     the records.
        //  2. The receipts thread: tx_receipts to per-sender executed-truth
        //     floors (only this shard's senders).
        //  3. The controller itself, handed to the Sequencer through
        //     enable_resync.
        let kardamom_sequencer::resync::ResyncChannel {
            controller,
            floor_tx,
            reject_tx,
            watermark,
        } = kardamom_sequencer::resync::ResyncChannel::open(
            cfg.resync.clone(),
            cfg.partition_index,
        )
        .context("build resync channel")?;

        let watermark_task = feeds::EgressWatermarkFeed::new(
            cfg.resync.boundary_silence_ms,
            cfg.partition_index,
            watermark,
            reject_tx,
        )
        .spawn(cluster_egress, shutdown.clone());

        // The receipts subscription follows the plane's transport. A
        // discovered subscription receives on OS-chosen ports, so
        // co-located replicas never collide.
        let executor_count = executor_count.or(plane.channels().tx_receipts_executor_count);
        let receipts_sub = plane
            .tx_receipts_subscriber(receipts_rt, executor_count)
            .context("open tx_receipts")?;
        let vslots = cfg.vslot_set().context("vslots")?;
        let receipts_task = feeds::ReceiptFloorFeed::new(vslots, floor_tx.clone())
            .spawn(receipts_sub, shutdown.clone());

        // The nonce lookup task. It shares the floor channel with the
        // receipts feed: an executor's committed nonce is floor evidence
        // of the same kind as a receipt.
        let (lookup, lookup_task) = Self::spawn_lookup(cfg, floor_tx, shutdown)?;

        Ok(Self {
            controller,
            lookup,
            feeds: ResyncFeeds {
                watermark_task,
                receipts_task,
                lookup_task,
                main_rt,
            },
        })
    }

    /// Spawn the nonce lookup task when the config names executor
    /// endpoints. Returns the requester for the publish loop and the task
    /// handle, or `None` for both when the lookup is off.
    #[allow(
        clippy::type_complexity,
        reason = "one optional pair, read once by the caller"
    )]
    fn spawn_lookup(
        cfg: &SequencerConfig,
        floor_tx: crossbeam_channel::Sender<kardamom_sequencer::resync::FloorUpdate>,
        shutdown: &Shutdown,
    ) -> Result<(Option<LookupRequester>, Option<tokio::task::JoinHandle<()>>)> {
        if !cfg.lookup.enabled() {
            return Ok((None, None));
        }
        let (requester, rx) = LookupRequester::channel();
        let task = feeds::NonceLookupFeed::new(cfg.lookup.clone(), cfg.partition_index, floor_tx)
            .context("nonce lookup: http client build failed")?
            .spawn(rx, shutdown.clone());
        Ok((Some(requester), Some(task)))
    }
}

/// The three publish-loop handles, plus the cluster session guard they
/// depend on.
struct SpawnedLoops {
    #[allow(
        dead_code,
        reason = "never read; kept alive until join_all returns for its Drop impl (see SpawnedLoops::join_all)"
    )]
    cluster_guard: LiveCluster,
    main: feeds::LoopHandle,
    deposits: feeds::LoopHandle,
    remote_epochs: feeds::LoopHandle,
}

impl SpawnedLoops {
    /// Await one handle and log its outcome. `label` names the loop in
    /// the "returned cleanly" / "returned an error" lines, `panic_label`
    /// names it in the "panicked" line.
    async fn join_one(handle: feeds::LoopHandle, label: &str, panic_label: &str) {
        match handle.await {
            Ok(Ok(())) => tracing::info!("sequencer {label} returned cleanly"),
            Ok(Err(e)) => tracing::error!(error = %e, "sequencer {label} returned an error"),
            Err(e) => tracing::error!(error = %e, "sequencer {panic_label} panicked"),
        }
    }

    /// Await every publish loop, in order, then let `cluster_guard` fall
    /// out of scope. This also closes the egress channel, which unblocks
    /// the watermark feed. The feed also checks the shutdown token on
    /// each tick. The receipts task exits on the token, or on the closed
    /// floor channel after the main loop ends.
    async fn join_all(self) {
        Self::join_one(self.main, "main loop", "task").await;
        Self::join_one(self.deposits, "epoch pump", "epoch task").await;
        Self::join_one(self.remote_epochs, "remote-epoch pump", "remote-epoch task").await;
    }
}

/// Say where committed nonce floors come from: the `tx_receipts` stream,
/// and the executor nonce lookup when the config names endpoints.
fn log_nonce_floor_sources(cfg: &SequencerConfig) {
    if cfg.lookup.enabled() {
        tracing::info!(
            endpoints = ?cfg.lookup.executor_endpoints,
            timeout_ms = cfg.lookup.timeout_ms.get(),
            max_in_flight = cfg.lookup.max_in_flight.get(),
            "nonce floors: cold senders seed at 0; committed floors come from the \
             tx_receipts stream and from the executor nonce lookup, so a restarted \
             replica regains an established sender on its first park (F02.1 closed)"
        );
    } else {
        tracing::warn!(
            "nonce floors: no executor query endpoints; cold senders seed at 0 and \
             committed floors come from the tx_receipts stream only. A restarted \
             replica does NOT regain coverage of an established sender until a \
             receipt arrives (F02.1). Set --executor-query-endpoints."
        );
    }
}

#[tokio::main(flavor = "multi_thread", worker_threads = 2)]
async fn main() -> anyhow::Result<()> {
    kardamom_obs::bin::init_tracing();
    let args = Args::parse();
    kardamom_obs::init_service!("sequencer", args.metrics_addr, args.host_id.as_ref()).await?;
    let raw = std::fs::read_to_string(&args.config).context("read config")?;
    let mut cfg: SequencerConfig = toml::from_str(&raw).context("parse config")?;
    apply_cli_overrides(&args, &mut cfg)?;
    cfg.validate().context("validate config")?;
    // Contract line for the CI drift check: this must match the cluster
    // JVM's -Dkardamom.cluster.dedupCapacity (see cluster.nomad.hcl).
    kardamom_sequencer::metrics::record_start_time();
    tracing::info!(
        dedup_capacity = cfg.resync.dedup_capacity.get(),
        enter_percent = cfg.resync.enter_percent.get(),
        boundary_silence_ms = cfg.resync.boundary_silence_ms,
        "resync contract: dedup_capacity must equal the cluster's -Dkardamom.cluster.dedupCapacity"
    );

    tracing::info!(
        partition_index = cfg.partition_index,
        sequencer_id = cfg.sequencer_id,
        tx_ttl_ms = cfg.tx_ttl_ms.get(),
        lane = cfg.lane(),
        extra_lanes = ?cfg.extra_lanes,
        vslots = %cfg.vslot_set().context("vslots")?,
        shadow_vslots = %cfg.shadow_vslots,
        shadow_warm_ms = kardamom_types::time::duration_to_ms_saturating(cfg.shadow_warm()),
        "kardamom-sequencer starting"
    );

    let log_cfg = LogConfig::resolve(args.log_config.as_deref()).context("resolve log config")?;
    let rt = AeronRuntime::spawn(args.aeron_dir.as_deref()).context("spawn AeronRuntime")?;
    let mut plane = StreamPlane::from_config(&log_cfg, &format!("sequencer-{}", cfg.lane()))
        .context("build the stream plane")?;

    let handles = Handles::open(&rt, &mut plane, &cfg).await?;

    let shutdown = Shutdown::new();

    log_nonce_floor_sources(&cfg);

    // tx_ordering always publishes to the Aeron Cluster (Raft) ingress. The
    // cluster-session guard (`LiveCluster`) and its dedicated Aeron runtime
    // must outlive both publish loops. So bind the guard in the outer
    // scope; it is dropped only after the publish loops are joined.
    //
    // This is a dedicated cluster runtime (its own Aeron thread, same
    // aeron dir), so the cluster session never contends with the tx_data
    // subscription on the main `rt`.
    let cluster_rt =
        AeronRuntime::spawn(args.aeron_dir.as_deref()).context("spawn cluster AeronRuntime")?;
    let (cluster_guard, cluster_pub, cluster_egress) =
        kardamom_sequencer::outbound::cluster::cluster_ref_publisher_with_egress(
            cluster_rt,
            cfg.cluster.to_live(),
        )
        .context("connect cluster ref publisher")?;
    tracing::info!("kardamom-sequencer: tx_ordering via Aeron Cluster");

    // A dedicated receipts runtime. Receipt decode runs at full line
    // rate, and the main `rt`'s polling thread must stay dedicated to the
    // tx_data subscription (the same isolation reason as `cluster_rt`
    // above). Sharing was observed to collapse the sequencer's
    // sustainable ingest rate.
    let receipts_rt =
        AeronRuntime::spawn(args.aeron_dir.as_deref()).context("spawn receipts AeronRuntime")?;
    let resync = ResyncWiring::spawn(
        &cfg,
        rt,
        cluster_egress,
        &receipts_rt,
        &mut plane,
        args.executor_count,
        &shutdown,
    )?;

    // Cloning shares the single session thread, and offers serialize
    // through it. All three loops use `cluster_pub` (it implements
    // `TxOrderingRefPublisher`): the canonical `TxRef` loop and the two
    // origin pumps.
    let (join_main, join_deposits, join_remote_epochs) = PublishLoops {
        cfg: cfg.clone(),
        tx_data: handles.data_subs,
        main_pub: cluster_pub.clone(),
        epoch_pub: cluster_pub.clone(),
        remote_epoch_pub: cluster_pub,
        tx_errors: handles.errors_pub,
        epochs: handles.deposits_sub,
        remote_epochs: handles.remote_epochs_sub,
        resync: Some(resync.controller),
        lookup: resync.lookup,
        shutdown: shutdown.clone(),
    }
    .spawn();
    let loops = SpawnedLoops {
        cluster_guard,
        main: join_main,
        deposits: join_deposits,
        remote_epochs: join_remote_epochs,
    };

    wait_for_shutdown().await;
    tracing::info!("kardamom-sequencer: shutdown signal received");
    shutdown.signal();
    // The cluster session (`cluster_guard`) stays alive until every
    // publish loop has stopped; `join_all` drops it as soon as it
    // returns. `resync.feeds.join()` then awaits the two resync feed
    // tasks and, when it returns, drops `rt` — before `receipts_rt`,
    // still a local here, drops at the end of `main`.
    loops.join_all().await;
    // The plane's registrations and discovery tasks end before the
    // runtime drops inside `resync.feeds.join()`.
    plane.shutdown().await;
    resync.feeds.join().await;
    Ok(())
}

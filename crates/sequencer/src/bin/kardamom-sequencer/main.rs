//! `kardamom-sequencer`: per-partition sequencer process.
//!
//! Parses a TOML [`SequencerConfig`], opens one `tx_data` subscriber per
//! lane it reads, the Aeron Cluster (Raft) ref publisher (`tx_ordering`),
//! and the `tx_errors` and `tx_status` publishers. Runs the sequencer
//! main loop on a dedicated blocking thread until SIGTERM or Ctrl-C.
//!
//! The lag-detection, receipt-floor, and nonce-lookup feed tasks live in
//! [`feeds`]. The `aeron_live` handles implement the sequencer's
//! subscriber/publisher traits directly (`kardamom_sequencer::{epoch,
//! remote_epoch, outbound}`); the lane set is [`feeds::LaneSubscriptions`].
//!
//! The flags live in [`args`].

mod args;
mod feeds;

use std::num::NonZeroU32;

use anyhow::{Context, Result};
use args::Args;
use clap::Parser;
use feeds::{LaneSub, LaneSubscriptions, PublishLoops};
use kardamom_cluster_adapter::LiveCluster;
use kardamom_log::aeron_live::{
    AeronRuntime, ServiceEventsPublisherHandle, TxDepositsSubscriberHandle,
    TxErrorsPublisherHandle, TxRemoteEpochsSubscriberHandle, TxStatusPublisherHandle,
};
use kardamom_log::config::LogConfig;
use kardamom_log::discovery::StreamPlane;
use kardamom_obs::bin::wait_for_shutdown;
use kardamom_sequencer::config::SequencerConfig;
use kardamom_sequencer::fees::FeeGate;
use kardamom_sequencer::lookup::LookupRequester;
use kardamom_sequencer::sequencer::Shutdown;

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
    status_pub: TxStatusPublisherHandle,
}

impl Handles {
    /// Open every handle this sequencer needs, for the lanes of `cfg`.
    /// `tx_errors` and `tx_status` follow the plane's transport; the rest
    /// still open on their static channels.
    async fn open(
        rt: &AeronRuntime,
        plane: &mut StreamPlane,
        cfg: &SequencerConfig,
    ) -> Result<Self> {
        let own = Self::open_lane(rt, plane, cfg.lane())?;
        let old = cfg
            .extra_lanes
            .iter()
            .map(|lane| Self::open_lane(rt, plane, *lane))
            .collect::<Result<Vec<_>>>()?;
        Ok(Self {
            data_subs: LaneSubscriptions::new(own, old),
            deposits_sub: plane
                .subscriber::<TxDepositsSubscriberHandle>(rt)
                .context("open TxDepositsSubscriberHandle")?,
            remote_epochs_sub: plane
                .subscriber::<TxRemoteEpochsSubscriberHandle>(rt)
                .context("open TxRemoteEpochsSubscriberHandle")?,
            errors_pub: plane
                .publisher::<TxErrorsPublisherHandle>(rt)
                .await
                .context("open TxErrorsPublisherHandle")?,
            status_pub: plane
                .publisher::<TxStatusPublisherHandle>(rt)
                .await
                .context("open TxStatusPublisherHandle")?,
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

/// The account events that may wait for the v2 loop: about one for each
/// receipt batch. A full queue holds the receipts feed back.
const V2_ACCOUNT_EVENTS: usize = 65_536;

/// The senders that may wait for a lookup of the v2 loop. A full queue
/// drops a request; the sender's transactions then wait for its rows.
const V2_LOOKUP_QUEUE: usize = 4_096;

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
    /// The fee admission gate, off unless `[fees] priority` is on.
    fees: FeeGate,
    /// The epoch lane, whose signal sender the egress-watermark feed holds.
    epoch_lane: kardamom_sequencer::epoch::EpochPump,
    /// The decision version that the egress-watermark feed reads off the
    /// sealer, for the publish loop.
    sealer_version: kardamom_sequencer::sealer_rules::SealerVersion,
    /// The inputs of the v2 loop, when `[admission]` turns it on.
    v2: Option<feeds::V2Wiring>,
    feeds: ResyncFeeds,
}

/// The egress-watermark, receipts-floor, and nonce-lookup tasks, plus the
/// `tx_data` runtime.
struct ResyncFeeds {
    watermark_task: tokio::task::JoinHandle<()>,
    receipts_task: tokio::task::JoinHandle<()>,
    /// `None` when priority fees are off.
    base_fee_task: Option<tokio::task::JoinHandle<()>>,
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
        if let Some(task) = self.base_fee_task
            && let Err(e) = task.await
        {
            tracing::warn!(?e, "base-fee task panicked");
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
            answer_tx,
            deadline_tx,
            watermark,
        } = kardamom_sequencer::resync::ResyncChannel::open(
            cfg.resync.clone(),
            cfg.partition_index,
        )
        .context("build resync channel")?;

        // The epoch lane reads the same egress: the boundaries confirm its
        // epochs, and an origin-gap reject makes it offer them again.
        let (origins, epoch_lane) = kardamom_sequencer::epoch::EpochPump::new();
        let sealer_version = kardamom_sequencer::sealer_rules::SealerVersion::default();
        let watermark_task = feeds::EgressWatermarkFeed::new(
            cfg.resync.boundary_silence_ms,
            cfg.partition_index,
            watermark,
            answer_tx,
            deadline_tx,
            origins,
            sealer_version.clone(),
        )
        .spawn(cluster_egress, shutdown.clone());

        // The receipts subscription follows the plane's transport. A
        // discovered subscription receives on OS-chosen ports, so
        // co-located replicas never collide.
        let executor_count = executor_count.or(plane.channels().tx_receipts_executor_count);
        let receipts_sub = if cfg.receipts_from_egress {
            tracing::info!("sequencer: the receipts and their rows come from the egress");
            plane
                .egress_receipts_subscriber(receipts_rt)
                .context("open egress_receipts")?
        } else {
            plane
                .tx_receipts_subscriber(receipts_rt, executor_count)
                .context("open tx_receipts")?
        };
        let vslots = cfg.vslot_set().context("vslots")?;
        // The local account layer: the receipts feed writes the rows of
        // this replica's vslots, the lookup task reads them.
        let (live, live_writer) = kardamom_cache::LiveAccounts::new(&cfg.live_accounts);
        // The Redis layer, when `[cache]` names an address. The mirror ids
        // are the executor indexes; one when no count is known.
        let redis = cfg.cache.enabled().then(|| {
            let mirrors = executor_count.unwrap_or(NonZeroU32::MIN);
            std::sync::Arc::new(kardamom_cache::CacheReader::spawn(
                &cfg.cache,
                mirrors,
                live.clone(),
            ))
        });
        // The v2 loop takes the rows of each batch and the lookup answers
        // as account events. The v1 loop takes the receipts as floors.
        let (accounts_tx, accounts_rx) = crossbeam_channel::bounded(V2_ACCOUNT_EVENTS);
        let sink = if cfg.admission.enabled {
            feeds::ReceiptSink::Accounts(accounts_tx.clone())
        } else {
            feeds::ReceiptSink::Floors(floor_tx.clone())
        };
        let receipts_task = feeds::ReceiptFloorFeed::new(vslots, sink, live_writer)
            .spawn(receipts_sub, shutdown.clone());

        // The fee gate: the base fee view follows the executors' boundary
        // side stream, over the same publisher set as the receipts. Off,
        // no boundary task runs.
        let (fees, base_fee_task) = if cfg.fees.priority {
            let view = kardamom_sequencer::fees::LatestBaseFee::new();
            let boundaries = plane
                .tx_receipt_boundaries_subscriber(receipts_rt, executor_count)
                .context("open tx_receipts boundaries")?;
            let task = feeds::BaseFeeFeed::new(view.clone()).spawn(boundaries, shutdown.clone());
            // The v2 filter checks the balance itself, with the forwarded
            // run counted, so its gate holds no account layer.
            let balances = (!cfg.admission.enabled).then(|| live.clone());
            (FeeGate::on(view, balances), Some(task))
        } else {
            (FeeGate::off(), None)
        };

        // The lookup task: the account lookup of the v2 loop, or the nonce
        // lookup of the v1 loop. The nonce lookup shares the floor channel
        // with the receipts feed: an executor's committed nonce is floor
        // evidence of the same kind as a receipt.
        let (lookup, lookup_task, v2) = if cfg.admission.enabled {
            let (v2, task) =
                Self::spawn_account_lookup(cfg, accounts_tx, accounts_rx, shutdown, live, redis);
            (None, Some(task), Some(v2))
        } else {
            let (lookup, task) = Self::spawn_lookup(cfg, floor_tx, shutdown, live, redis);
            (lookup, task, None)
        };

        Ok(Self {
            controller,
            lookup,
            fees,
            epoch_lane,
            sealer_version,
            v2,
            feeds: ResyncFeeds {
                watermark_task,
                receipts_task,
                base_fee_task,
                lookup_task,
                main_rt,
            },
        })
    }

    /// Spawn the account lookup task of the v2 loop. Returns the inputs of
    /// the loop and the task handle.
    fn spawn_account_lookup(
        cfg: &SequencerConfig,
        accounts_tx: crossbeam_channel::Sender<kardamom_sequencer::v2::AccountEvent>,
        accounts_rx: crossbeam_channel::Receiver<kardamom_sequencer::v2::AccountEvent>,
        shutdown: &Shutdown,
        live: std::sync::Arc<kardamom_cache::LiveAccounts>,
        redis: Option<std::sync::Arc<kardamom_cache::CacheReader>>,
    ) -> (feeds::V2Wiring, tokio::task::JoinHandle<()>) {
        let (lookups, rx) = tokio::sync::mpsc::channel(V2_LOOKUP_QUEUE);
        let query = kardamom_cache::ExecutorQuery::new(&kardamom_cache::ExecutorQueryConfig {
            endpoints: cfg.lookup.executor_endpoints.clone(),
            timeout_ms: cfg.lookup.timeout_ms,
            max_in_flight: cfg.lookup.max_in_flight,
        });
        let task = feeds::AccountLookupFeed::new(
            rx,
            accounts_tx,
            live,
            redis,
            query,
            shutdown.clone(),
            cfg.partition_index,
        )
        .spawn();
        (
            feeds::V2Wiring {
                accounts: accounts_rx,
                lookups,
                verdicts: None,
            },
            task,
        )
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
        floor_tx: crossbeam_channel::Sender<kardamom_sequencer::resync::FloorEvent>,
        shutdown: &Shutdown,
        live: std::sync::Arc<kardamom_cache::LiveAccounts>,
        redis: Option<std::sync::Arc<kardamom_cache::CacheReader>>,
    ) -> (Option<LookupRequester>, Option<tokio::task::JoinHandle<()>>) {
        let (requester, rx) = LookupRequester::channel();
        let Some(feed) = feeds::NonceLookupFeed::new(
            cfg.lookup.clone(),
            cfg.partition_index,
            rx,
            shutdown.clone(),
            floor_tx,
            live,
            redis,
        ) else {
            return (None, None);
        };
        (Some(requester), Some(feed.spawn()))
    }
}

/// The three publish-loop handles, the guard that signals shutdown to
/// them, and the cluster session guard they depend on.
struct SpawnedLoops {
    /// Dropping this signals every loop to stop; see
    /// [`Self::join_all`].
    stop: tokio_util::sync::DropGuard,
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

    /// Signal shutdown, then await every publish loop, in order, then let
    /// `cluster_guard` fall out of scope. This also closes the egress
    /// channel, which unblocks the watermark feed. The feed also checks
    /// the shutdown token on each tick. The receipts task exits on the
    /// token, or on the closed floor channel after the main loop ends.
    async fn join_all(self) {
        // The block scope ends the guard, which signals every loop.
        {
            let _signal = self.stop;
        }
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

/// Publish this replica's lifecycle on the `events` stream: it pauses
/// while the sealer emits no boundary.
async fn spawn_events_beacon(plane: &mut StreamPlane, rt: &AeronRuntime) -> Result<()> {
    plane
        .publisher::<ServiceEventsPublisherHandle>(rt)
        .await
        .context("open events")?
        .spawn_process_beacon();
    Ok(())
}

/// The start lines: the contract line for the CI drift check, which must
/// match the cluster JVM's -Dkardamom.cluster.dedupCapacity (see
/// cluster.nomad.hcl), and the settings of this replica.
fn log_start(cfg: &SequencerConfig) -> anyhow::Result<()> {
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
    Ok(())
}

/// Start the verdict task when the v2 loop runs with `[admission]
/// verdicts`, and give its queue to the loop.
fn start_verdicts(
    resync: &mut ResyncWiring,
    cfg: &SequencerConfig,
    log_cfg: &LogConfig,
    args: &Args,
    shutdown: &Shutdown,
) -> anyhow::Result<Option<tokio::task::JoinHandle<()>>> {
    let Some(v2) = resync.v2.as_mut().filter(|_| cfg.admission.verdicts) else {
        return Ok(None);
    };
    let (frames, task) = feeds::VerdictFeed::start(
        log_cfg,
        &format!("sequencer-{}-verdicts", cfg.lane()),
        args.aeron_dir.as_deref(),
        shutdown,
    )?;
    v2.verdicts = Some(frames);
    Ok(Some(task))
}

/// Join the verdict task. The loop dropped its end of the verdict queue,
/// so the task ends its plane and drops its runtime.
async fn join_verdicts(task: Option<tokio::task::JoinHandle<()>>) {
    if let Some(task) = task
        && let Err(e) = task.await
    {
        tracing::warn!(?e, "verdict task panicked");
    }
}

#[tokio::main(flavor = "multi_thread", worker_threads = 2)]
async fn main() -> anyhow::Result<()> {
    kardamom_obs::bin::init_tracing();
    let args = Args::parse();
    let raw = std::fs::read_to_string(&args.config).context("read config")?;
    let mut cfg: SequencerConfig = toml::from_str(&raw).context("parse config")?;
    args.apply_overrides(&mut cfg)?;
    cfg.validate().context("validate config")?;
    // Ready while the cluster egress delivered a boundary within the
    // boundary-silence window: the session is open and the twin's dedup
    // window is attached. The sealer ticks a boundary on an idle chain
    // too, so this holds before the first transaction.
    kardamom_obs::init_service!(
        "sequencer",
        args.metrics_addr,
        args.host_id.as_ref(),
        kardamom_obs::Readiness::up().fresh(
            kardamom_sequencer::metrics::LAST_BOUNDARY_UNIX_SECONDS,
            std::time::Duration::from_millis(cfg.resync.boundary_silence_ms),
        )
    )
    .await?;
    log_start(&cfg)?;

    let log_cfg = LogConfig::resolve(args.log_config.as_deref()).context("resolve log config")?;
    let rt = AeronRuntime::spawn(args.aeron_dir.as_deref()).context("spawn AeronRuntime")?;
    let idle_cap = rt.idle_cap();
    let mut plane = StreamPlane::from_config(&log_cfg, &format!("sequencer-{}", cfg.lane()))
        .context("build the stream plane")?;

    let handles = Handles::open(&rt, &mut plane, &cfg).await?;
    spawn_events_beacon(&mut plane, &rt).await?;

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
    let mut cluster_cfg = cfg.cluster.to_live();
    if let Some(endpoints) = plane.cluster_ingress_endpoints().await? {
        cluster_cfg.ingress_endpoints = endpoints;
    }
    let (cluster_guard, cluster_pub, cluster_egress) =
        kardamom_sequencer::outbound::cluster::cluster_ref_publisher_with_egress(
            cluster_rt,
            cluster_cfg,
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
    let mut resync = ResyncWiring::spawn(
        &cfg,
        rt,
        cluster_egress,
        &receipts_rt,
        &mut plane,
        args.executor_count,
        &shutdown,
    )?;
    let verdict_task = start_verdicts(&mut resync, &cfg, &log_cfg, &args, &shutdown)?;

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
        side: kardamom_sequencer::outbound::SideChannels {
            errors: handles.errors_pub,
            status: handles.status_pub,
        },
        epochs: handles.deposits_sub,
        epoch_lane: resync.epoch_lane,
        remote_epochs: handles.remote_epochs_sub,
        resync: Some(resync.controller),
        lookup: resync.lookup,
        fees: resync.fees,
        sealer_version: resync.sealer_version,
        v2: resync.v2,
        shutdown: shutdown.clone(),
        idle_cap,
    }
    .spawn();
    let loops = SpawnedLoops {
        stop: shutdown.guard(),
        cluster_guard,
        main: join_main,
        deposits: join_deposits,
        remote_epochs: join_remote_epochs,
    };

    wait_for_shutdown().await;
    tracing::info!("kardamom-sequencer: shutdown signal received");
    // `join_all` signals the loops, then joins them. The cluster session
    // (`cluster_guard`) stays alive until every publish loop has
    // stopped; `join_all` drops it as soon as it returns.
    // `resync.feeds.join()` then awaits the two resync feed tasks and,
    // when it returns, drops `rt` — before `receipts_rt`, still a local
    // here, drops at the end of `main`.
    loops.join_all().await;
    join_verdicts(verdict_task).await;
    // The plane's registrations and discovery tasks end before the
    // runtime drops inside `resync.feeds.join()`.
    plane.shutdown().await;
    resync.feeds.join().await;
    Ok(())
}

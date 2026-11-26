//! `kardamom-state-mirror`: the Redis projection writer.
//!
//! One mirror runs next to each executor. It subscribes to `tx_receipts`
//! and writes every batch's account rows and receipts to Redis through
//! the monotone write rule, then publishes its head. Three mirrors write
//! the same values; the rule makes the order irrelevant.
//!
//! On start the mirror decides whether Redis can be resumed or must be
//! rebuilt. A rebuild scans the newest checkpoint of the co-located
//! executor, or of a checkpoint peer with `--checkpoint-peers`, and never
//! replays history: the mirror subscribes first, then waits for a
//! checkpoint at or beyond the first live position, then scans. Live rows
//! after the checkpoint win by position.
//!
//! Structure: the CLI in this file, the actor in [`mirror`], the rebuild
//! in [`rebuild`], the local head file in [`head`], the audit schedule in
//! [`audit`].

mod audit;
mod head;
mod metrics;
mod mirror;
mod rebuild;

use std::net::SocketAddr;
use std::num::NonZeroU32;
use std::path::PathBuf;
use std::time::Duration;

use anyhow::{Context, Result, bail};
use clap::Parser;
use kardamom_cache::{AccountCache, CacheConfig};
use kardamom_log::aeron_live::{AeronRuntime, ServiceEventsPublisherHandle};
use kardamom_log::config::LogConfig;
use kardamom_log::discovery::StreamPlane;
use kardamom_obs::bin::wait_for_shutdown;

use head::HeadFile;
use mirror::{Mirror, MirrorInputs};
use rebuild::{PeerFetch, Rebuild};

/// How long the mirror waits between connection attempts at start. The
/// sentinels can name no primary for minutes after a failover, and an
/// exit spends the Nomad restart budget: three exits inside one minute
/// stop the task for 40 s.
const CACHE_CONNECT_RETRY: Duration = Duration::from_secs(2);

/// The TOML the binary reads through `--config`: the `[cache]` section.
#[derive(Debug, Default, serde::Deserialize)]
#[serde(default, deny_unknown_fields)]
struct FileConfig {
    cache: CacheConfig,
}

#[derive(Debug, Parser)]
#[command(
    name = "kardamom-state-mirror",
    version,
    about = "kardamom state mirror: the Redis projection writer"
)]
struct Args {
    /// The TOML config with the `[cache]` section.
    #[arg(long)]
    config: PathBuf,
    /// Optional `LogConfig` TOML supplying the Aeron `[channels]` config.
    #[arg(long, env = "KARDAMOM_LOG_CONFIG")]
    log_config: Option<PathBuf>,
    /// Aeron Media Driver directory (`aeron.dir`).
    #[arg(long)]
    aeron_dir: Option<PathBuf>,
    /// Number of executor replicas whose `tx_receipts` endpoints to
    /// attach under MDS, and the number of mirror heads to read. Falls
    /// back to `channels.tx_receipts_executor_count`.
    #[arg(long, env = "KARDAMOM_EXECUTOR_COUNT")]
    executor_count: Option<u32>,
    /// This mirror's id: the co-located executor's index.
    #[arg(long, env = "KARDAMOM_MIRROR_ID", default_value_t = 0)]
    mirror_id: u32,
    /// The co-located executor's checkpoint directory, read for a rebuild.
    #[arg(
        long,
        env = "KARDAMOM_CHECKPOINT_DIR",
        default_value = "/opt/kardamom/checkpoints"
    )]
    checkpoints_dir: PathBuf,
    /// This mirror's own directory: the local head file, the time of the
    /// last completed rebuild (`last-rebuild`), the rebuild scratch copy of
    /// a checkpoint, and the checkpoints fetched from the peers
    /// (`peer-checkpoints`).
    #[arg(
        long,
        env = "KARDAMOM_MIRROR_DIR",
        default_value = "/opt/kardamom/mirror"
    )]
    mirror_dir: PathBuf,
    /// Comma-separated executor checkpoint servers (`host:port`). A
    /// rebuild fetches the newest checkpoint of these peers into the
    /// mirror directory. It then restores the newest of that copy and the
    /// checkpoints of `--checkpoints-dir`. Empty: the rebuild reads
    /// `--checkpoints-dir` only. Requires `--chain`.
    #[arg(
        long,
        env = "KARDAMOM_CHECKPOINT_PEERS",
        value_delimiter = ',',
        requires = "chain"
    )]
    checkpoint_peers: Vec<String>,
    /// The genesis TOML of the chain (schema: `kardamom_types::Genesis`).
    /// With `--checkpoint-peers`, a rebuild refuses a checkpoint of
    /// another genesis. Ignored without `--checkpoint-peers`.
    #[arg(long, env = "KARDAMOM_CHAIN")]
    chain: Option<PathBuf>,
    /// Force a rebuild on this start.
    #[arg(long, env = "KARDAMOM_MIRROR_REBUILD", default_value_t = false)]
    rebuild: bool,
    /// Hours from the last completed rebuild to the next audit rebuild.
    /// The interval counts across restarts: the mirror keeps the time of
    /// the last rebuild in `--mirror-dir`. `0` disables the audit.
    #[arg(long, env = "KARDAMOM_MIRROR_AUDIT_HOURS", default_value_t = 24)]
    audit_hours: u64,
    /// Address for the Prometheus /metrics HTTP listener.
    #[arg(long, env = "KARDAMOM_METRICS_ADDR", default_value = "127.0.0.1:9007")]
    metrics_addr: SocketAddr,
    /// Host identifier. Stamped on every metric.
    #[arg(long, env = "KARDAMOM_HOST_ID", default_value = "local")]
    host_id: String,
}

impl Args {
    /// The checkpoint peers of a rebuild, when the flags name any.
    fn peer_fetch(&self) -> Result<Option<PeerFetch>> {
        self.chain
            .as_deref()
            .filter(|_| !self.checkpoint_peers.is_empty())
            .map(|chain| {
                PeerFetch::new(
                    self.checkpoint_peers.clone(),
                    chain,
                    self.mirror_dir.join("peer-checkpoints"),
                )
            })
            .transpose()
    }
}

#[tokio::main(flavor = "multi_thread", worker_threads = 2)]
async fn main() -> Result<()> {
    kardamom_obs::bin::init_tracing();
    let args = Args::parse();
    kardamom_obs::init_service!(
        "state-mirror",
        args.metrics_addr,
        args.host_id.as_str(),
        kardamom_obs::Readiness::up().equals(metrics::SERVING, 1.0)
    )
    .await?;
    kardamom_cache::metrics::describe();
    metrics::describe();

    let raw = std::fs::read_to_string(&args.config).context("read mirror config")?;
    let file_cfg: FileConfig = toml::from_str(&raw).context("parse mirror config")?;
    if !file_cfg.cache.enabled() {
        bail!("[cache] names no Redis address; the mirror has nothing to write to");
    }
    let cache = AccountCache::connect_waiting(&file_cfg.cache, CACHE_CONNECT_RETRY)
        .await
        .context("connect to Redis")?;

    let log_cfg = LogConfig::resolve(args.log_config.as_deref()).context("resolve log config")?;
    let mut plane =
        StreamPlane::from_config(&log_cfg, "state-mirror").context("build the stream plane")?;
    let rt = AeronRuntime::spawn(args.aeron_dir.as_deref()).context("spawn AeronRuntime")?;
    plane
        .publisher::<ServiceEventsPublisherHandle>(&rt)
        .await
        .context("open events")?
        .spawn_process_beacon();
    let executor_count = args
        .executor_count
        .and_then(NonZeroU32::new)
        .or(plane.channels().tx_receipts_executor_count);
    // The receiver alone moves into the actor. The runtime stays here, so
    // its last clone drops in `main` and the subscription closes on exit.
    let receipts = plane
        .tx_receipts_subscriber(&rt, executor_count)
        .context("open tx_receipts")?
        .into_receiver();
    let shutdown = plane.cancellation().child_token();
    let peers = args.peer_fetch()?;

    let mirror = Mirror::new(MirrorInputs {
        cache,
        receipts,
        id: args.mirror_id,
        head_count: executor_count.map_or(1, NonZeroU32::get),
        head: HeadFile::new(args.mirror_dir.join("head")),
        rebuild: Rebuild::new(args.checkpoints_dir, args.mirror_dir.join("rebuild"))
            .with_peers(peers),
        force_rebuild: args.rebuild,
        audit_hours: args.audit_hours,
        shutdown: shutdown.clone(),
    });
    tracing::info!(
        id = args.mirror_id,
        checkpoint_peers = args.checkpoint_peers.len(),
        "kardamom-state-mirror starting"
    );
    let task = tokio::spawn(mirror.run());

    wait_for_shutdown().await;
    tracing::info!("kardamom-state-mirror: shutdown signal received");
    shutdown.cancel();
    task.await.context("join the mirror")??;
    plane.shutdown().await;
    drop(rt);
    Ok(())
}

#[cfg(test)]
mod tests {
    use clap::Parser;

    use super::Args;

    /// A peer list without the genesis of the chain cannot verify an
    /// image, so the CLI refuses it.
    #[test]
    fn checkpoint_peers_require_the_chain() {
        let peers = [
            "kardamom-state-mirror",
            "--config",
            "mirror.toml",
            "--checkpoint-peers",
            "a:9014,b:9014",
        ];
        assert!(Args::try_parse_from(peers).is_err());
        let args =
            Args::try_parse_from(peers.into_iter().chain(["--chain", "genesis.toml"])).unwrap();
        assert_eq!(args.checkpoint_peers, ["a:9014", "b:9014"]);
    }
}

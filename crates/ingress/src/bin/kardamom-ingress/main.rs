//! `kardamom-ingress`: the standalone proxy (ingress) service process.
//!
//! This parses the flags, starts the metrics exporter, and runs one
//! [`IngressService`]. The service opens one `tx_data` publisher per lane
//! and the subscribers it needs, wires them into the proxy, and starts its
//! JSON-RPC server. The process idles until SIGTERM or Ctrl-C, then drains
//! the service and stops it.

use std::net::SocketAddr;
use std::path::PathBuf;

use anyhow::{Context, Result};
use clap::Parser;
use kardamom_ingress::config::IngressFileConfig;
use kardamom_ingress::service::{IngressService, IngressSettings};
use kardamom_log::config::LogConfig;
use kardamom_obs::bin::wait_for_shutdown;

#[derive(Debug, Parser)]
#[command(
    name = "kardamom-ingress",
    version,
    about = "kardamom ingress (proxy) process"
)]
struct Args {
    /// Path to the TOML config file (schema: `IngressConfig`).
    #[arg(long)]
    config: PathBuf,
    /// Optional path to a `LogConfig` TOML file. It supplies the Aeron
    /// `[channels]` config, and the `[aeron]` and `[quorum]` config. When
    /// unset, the built-in single-host IPC defaults apply, which keeps
    /// local and e2e behavior the same. A multi-host deployment points
    /// this at the rendered UDP channels config.
    #[arg(long, env = "KARDAMOM_LOG_CONFIG")]
    log_config: Option<PathBuf>,
    /// Aeron Media Driver directory (`aeron.dir`).
    #[arg(long)]
    aeron_dir: Option<PathBuf>,
    /// Address for the Prometheus /metrics HTTP listener.
    #[arg(long, env = "KARDAMOM_METRICS_ADDR", default_value = "127.0.0.1:9006")]
    metrics_addr: SocketAddr,
    /// Host identifier. Stamped on every metric.
    #[arg(long, env = "KARDAMOM_HOST_ID", default_value = "local")]
    host_id: kardamom_obs::HostId,
    #[command(flatten)]
    settings: IngressSettings,
}

#[tokio::main(flavor = "multi_thread", worker_threads = 4)]
async fn main() -> Result<()> {
    kardamom_obs::bin::init_tracing();
    let args = Args::parse();
    kardamom_obs::init_service!(
        "ingress",
        args.metrics_addr,
        args.host_id.as_ref(),
        kardamom_obs::Readiness::up().equals(kardamom_ingress::metrics::DRAINING, 0.0)
    )
    .await?;
    kardamom_ingress::metrics::describe();
    kardamom_cache::metrics::describe();

    // Runtime tunables come from defaults and CLI flags. The TOML file
    // supplies only the optional `[cluster]` section, the Aeron Cluster
    // client connection that the on-quorum watermark observer uses.
    let raw = std::fs::read_to_string(&args.config).context("read ingress config")?;
    let file_cfg: IngressFileConfig = toml::from_str(&raw).context("parse ingress config")?;
    let log_cfg = LogConfig::resolve(args.log_config.as_deref()).context("resolve log config")?;

    let running = IngressService {
        settings: args.settings,
        aeron_dir: args.aeron_dir,
        log_cfg,
        file_cfg,
    }
    .run()
    .await?;
    wait_for_shutdown().await;
    tracing::info!("kardamom-ingress: shutdown signal received");
    running.stop().await;
    Ok(())
}

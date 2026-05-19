//! Single-process kardamom bench harness with `tracing-flame` recording
//! scoped to the measurement window.
//!
//! Running the standalone node + bench together for a long session
//! pushes the resulting flamegraph 99%+ into the bare
//! `ThreadId(N)-tokio-rt-worker` root frame — every nanosecond a worker
//! is alive but not inside an entered span is attributed there. Embedding
//! the node in the bench process and only flushing flame samples during
//! the measurement window keeps the recording tight around the work we
//! actually care about.

use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use jsonrpsee::http_client::HttpClientBuilder;
use jsonrpsee::server::Server;
use tracing_flame::FlameLayer;
use tracing_subscriber::Layer;
use tracing_subscriber::filter::FilterFn;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::{EnvFilter, fmt};

use kardamom_node::Node;
use kardamom_node::rpc::{EthApiServer, EthHandlers};

use crate::config::{Config, FileConfig};
use crate::report;

pub struct HarnessArgs {
    pub chain_id: u64,
    pub file_config: FileConfig,
    pub config: Config,
    pub flame_out: PathBuf,
    pub report_json: Option<PathBuf>,
}

/// Build the node + RPC server, run warmup with the flame layer gated off,
/// then run the measurement with the flame layer gated on, then flush.
///
/// `generator::run` drains all in-flight requests via the semaphore before
/// returning, so no spans cross the warmup→measurement boundary — the
/// gate flip never sees a partially-recorded span.
pub async fn run_harness(args: HarnessArgs) -> anyhow::Result<()> {
    let active = Arc::new(AtomicBool::new(false));
    let active_for_filter = Arc::clone(&active);

    let env_filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new("kardamom=info,kardamom_bench=info"));
    let fmt_layer = fmt::layer();

    let (flame, flame_guard) = FlameLayer::with_file(&args.flame_out)?;
    let gated_flame = flame.with_filter(FilterFn::new(move |_meta| {
        active_for_filter.load(Ordering::Relaxed)
    }));

    tracing_subscriber::registry()
        .with(env_filter)
        .with(fmt_layer)
        .with(gated_flame)
        .try_init()
        .map_err(|e| anyhow::anyhow!("tracing init failed: {e}"))?;

    let genesis = crate::genesis::from_config(&args.file_config, args.chain_id)?;
    let node = Node::new(&genesis);
    let bind: SocketAddr = "127.0.0.1:0".parse().unwrap();
    let server = Server::builder().build(bind).await?;
    let bound = server.local_addr()?;

    let module = EthHandlers::new(node).into_rpc();
    let server_handle = server.start(module);

    let url = format!("http://{}", bound);
    let client = HttpClientBuilder::default()
        .request_timeout(Duration::from_secs(30))
        .max_concurrent_requests((args.config.concurrency as usize) * 4 + 16)
        .build(&url)?;

    let warmup_dur = args.config.warmup;
    if !warmup_dur.is_zero() {
        let mut warmup_cfg = args.config.clone();
        warmup_cfg.warmup = Duration::ZERO;
        warmup_cfg.duration = warmup_dur;
        tracing::info!(duration = ?warmup_dur, "harness: warmup (flame off)");
        let _ = crate::generator::run(client.clone(), warmup_cfg).await?;
    }

    active.store(true, Ordering::Relaxed);
    tracing::info!(
        duration = ?args.config.duration,
        flame_out = %args.flame_out.display(),
        "harness: measurement (flame on)"
    );
    let mut measure_cfg = args.config.clone();
    measure_cfg.warmup = Duration::ZERO;
    let outputs = crate::generator::run(client.clone(), measure_cfg).await?;
    active.store(false, Ordering::Relaxed);
    drop(flame_guard);

    let bench_report = report::build_report(&args.config, &outputs.counters, outputs.histograms);
    report::print_terminal(&bench_report);
    if let Some(path) = &args.report_json {
        report::write_json(path, &bench_report)?;
    }

    let _ = server_handle.stop();
    server_handle.stopped().await;
    Ok(())
}

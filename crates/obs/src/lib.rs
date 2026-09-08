//! Shared Prometheus exporter init for every kardamom service binary.
//!
//! Every service binary calls [`init`] (or the [`init_service!`] macro)
//! once, inside its Tokio runtime, to install a Prometheus exporter with a
//! shared histogram bucket layout, a build-info gauge, and a liveness
//! gauge. See `docs/specs/2026-05-29-prometheus-grafana-design.md` for the
//! dashboard layout that reads these metrics.

use std::net::SocketAddr;
use std::time::Duration;

use anyhow::{Context, Result, anyhow};
use metrics_exporter_prometheus::{ExporterFuture, PrometheusBuilder, PrometheusRecorder};

pub mod bin;

/// [`init`] with the version and git-sha values filled in at the call
/// site, so each binary stamps its own crate version. A plain helper
/// function would bake in kardamom-obs's version instead.
///
/// ```ignore
/// kardamom_obs::init_service!("ingress", args.metrics_addr, &args.host_id).await?;
/// ```
#[macro_export]
macro_rules! init_service {
    ($service:expr, $metrics_addr:expr, $host_id:expr $(,)?) => {
        $crate::init(
            $service,
            $metrics_addr,
            $host_id,
            env!("CARGO_PKG_VERSION"),
            option_env!("KARDAMOM_GIT_SHA").unwrap_or("unknown"),
        )
    };
}

/// Check whether an exporter build failure is a TCP bind `AddrInUse` error,
/// the only retryable class. This checks the error chain structurally,
/// with a string fallback in case the exporter crate stringifies the io
/// error instead of sourcing it.
fn is_addr_in_use(e: &anyhow::Error) -> bool {
    e.chain().any(|c| {
        c.downcast_ref::<std::io::Error>()
            .is_some_and(|io| io.kind() == std::io::ErrorKind::AddrInUse)
    }) || format!("{e:#}").contains("Address already in use")
}

/// Shared histogram buckets (seconds). 100 µs to 5 s, exponential. Every
/// service histogram uses these boundaries, so dashboards can compare
/// latency panels across services.
const DURATION_BUCKETS: &[f64] = &[
    0.0001, 0.00025, 0.0005, 0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0,
];

/// Build-info gauge name. The overview dashboard reads this exact name.
const BUILD_INFO: &str = "kardamom_build_info";

/// Heartbeat gauge: set to 1 once init succeeds. Used by the overview
/// dashboard's "services up" panel.
const SERVICE_UP: &str = "kardamom_service_up";

/// This service's Prometheus exporter, before it is installed. Building it
/// (binding the TCP listener, setting bucket layout and global labels) is
/// separate from installing it (making it the global recorder, spawning
/// its future), so [`Exporter::build_with_retry`] can retry the bind alone.
struct Exporter {
    service: &'static str,
    metrics_addr: SocketAddr,
    host_id: String,
}

impl Exporter {
    fn new(service: &'static str, metrics_addr: SocketAddr, host_id: &str) -> Self {
        Self {
            service,
            metrics_addr,
            host_id: host_id.to_string(),
        }
    }

    /// Bind-retry attempt count and delay, read from env so tests can
    /// shrink them. Defaults: 24 attempts x 5 s (2 min), which outlives a
    /// supervisor's reap cycle for a wedged predecessor still holding the
    /// port.
    fn retry_settings() -> (u32, Duration) {
        let attempts: u32 = std::env::var("KARDAMOM_OBS_BIND_RETRIES")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(24);
        let delay = Duration::from_millis(
            std::env::var("KARDAMOM_OBS_BIND_RETRY_DELAY_MS")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(5_000),
        );
        (attempts, delay)
    }

    /// Build the exporter once. `metrics-exporter-prometheus` (0.18) binds
    /// the TCP listener synchronously inside `build()`, so a port
    /// collision surfaces as an init error here, rather than as a
    /// healthy-looking service with no `/metrics`. The `init_port_in_use`
    /// integration test pins this eager bind: if a dependency upgrade
    /// moves the bind into the exporter future's first poll, that test
    /// fails, and the bind must be made eager again (for example, by
    /// pre-binding a std `TcpListener`).
    fn build(&self) -> Result<(PrometheusRecorder, ExporterFuture)> {
        PrometheusBuilder::new()
            .with_http_listener(self.metrics_addr)
            .set_buckets(DURATION_BUCKETS)
            .context("set_buckets")
            .and_then(|b| {
                b.add_global_label("service", self.service)
                    .add_global_label("host_id", self.host_id.as_str())
                    .build()
                    .context("PrometheusBuilder::build")
            })
    }

    /// Retry [`Exporter::build`] on `AddrInUse`, up to the env-configured
    /// budget. A port squatter is usually a wedged or frozen predecessor
    /// seconds away from being reaped by its supervisor. Failing
    /// instantly would burn one restart attempt per squat, and under a
    /// `mode = "fail"` restart policy (the validator allows 5 attempts,
    /// then stays down), that would turn a transient squat into a
    /// permanent outage. Every other bind or build error stays fail-fast
    /// (no retry).
    async fn build_with_retry(&self) -> Result<(PrometheusRecorder, ExporterFuture)> {
        let (bind_retries, bind_retry_delay) = Self::retry_settings();
        let mut built = self.build();
        for attempt in 1..=bind_retries {
            if !built.as_ref().err().is_some_and(is_addr_in_use) {
                break;
            }
            tracing::warn!(
                metrics_addr = %self.metrics_addr,
                attempt,
                max = bind_retries,
                "metrics port in use (squatter not yet reaped?); retrying bind"
            );
            tokio::time::sleep(bind_retry_delay).await;
            built = self.build();
        }
        built
    }

    /// Register and set the build-info and liveness gauges.
    fn register_build_gauges(version: &'static str, git_sha: &'static str) {
        metrics::describe_gauge!(BUILD_INFO, "Build info; value is always 1.");
        metrics::gauge!(
            BUILD_INFO,
            "version" => version,
            "sha" => git_sha,
        )
        .set(1.0);

        metrics::describe_gauge!(SERVICE_UP, "1 while the service's exporter is live.");
        metrics::gauge!(SERVICE_UP).set(1.0);
    }
}

/// Install the Prometheus exporter for this service.
///
/// Must run inside a Tokio runtime: the exporter future is spawned onto the
/// ambient runtime (`tokio::spawn`). Every service binary is a
/// `#[tokio::main]`, so the call site is `init(...).await?` (or the
/// [`init_service!`] macro followed by `.await?`). The exporter runs as a
/// task on that same runtime; this crate does not start a dedicated thread
/// or a private runtime for it.
///
/// # Errors
///
/// Returns an error if `host_id` is empty, if no Tokio runtime is running
/// on the calling thread, or if the exporter fails to bind or build (after
/// exhausting the `AddrInUse` retry budget).
pub async fn init(
    service: &'static str,
    metrics_addr: SocketAddr,
    host_id: &str,
    version: &'static str,
    git_sha: &'static str,
) -> Result<()> {
    if host_id.is_empty() {
        return Err(anyhow!("host_id must be non-empty"));
    }
    // Fail with a clear error (not the exporter's "no reactor running"
    // panic) when a caller forgets the runtime.
    tokio::runtime::Handle::try_current()
        .context("kardamom_obs::init requires an ambient tokio runtime")?;

    let exporter = Exporter::new(service, metrics_addr, host_id);
    let (recorder, fut) = exporter.build_with_retry().await?;
    metrics::set_global_recorder(recorder).map_err(|e| anyhow!("set_global_recorder: {e}"))?;
    tokio::spawn(async move {
        if let Err(e) = fut.await {
            tracing::error!(error = ?e, "obs-exporter: exporter terminated");
        }
    });

    Exporter::register_build_gauges(version, git_sha);

    tracing::info!(
        service = service,
        host_id = host_id,
        addr = %metrics_addr,
        "kardamom_obs: prometheus exporter installed"
    );
    Ok(())
}

//! Shared Prometheus exporter init for every kardamom service binary.
//!
//! See `docs/specs/2026-05-29-prometheus-grafana-design.md`.

use std::net::SocketAddr;

use anyhow::{Context, Result, anyhow};
use metrics_exporter_prometheus::PrometheusBuilder;

/// Shared histogram buckets (seconds). 100 µs → 5 s, exponential.
/// Promoted out of `crates/node/src/metrics.rs` so every service histogram
/// uses the same boundaries.
pub const DURATION_BUCKETS: &[f64] = &[
    0.0001, 0.00025, 0.0005, 0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0,
];

/// Build-info gauge name. Stays `kardamom_build_info` for compatibility with
/// the existing RPC dashboard.
pub const BUILD_INFO: &str = "kardamom_build_info";

/// Heartbeat gauge: set to 1 once init succeeds. Used by the overview
/// dashboard's "services up" panel.
pub const SERVICE_UP: &str = "kardamom_service_up";

/// Install the Prometheus exporter for this service.
pub fn init(
    service: &'static str,
    metrics_addr: SocketAddr,
    host_id: &str,
    version: &'static str,
    git_sha: &'static str,
) -> Result<()> {
    if host_id.is_empty() {
        return Err(anyhow!("host_id must be non-empty"));
    }

    // Run the exporter's HTTP listener on a DEDICATED thread with its own
    // single-threaded runtime, never on the service's tokio runtime.
    // `PrometheusBuilder::install()` spawns onto the ambient runtime when one
    // exists, which couples scrape liveness to service-runtime health: a
    // wedged or starved service runtime silently takes /metrics down with it,
    // and every probe reads "service gone" when the truth is "service wedged"
    // (see issue #76 — the node-failure-executor blackout). A dedicated
    // thread keeps /metrics answering regardless, so wedged-but-alive is
    // observable as `kardamom_service_up == 1` with stale gauges.
    let (recorder, exporter) = PrometheusBuilder::new()
        .with_http_listener(metrics_addr)
        .set_buckets(DURATION_BUCKETS)
        .context("set_buckets")?
        .add_global_label("service", service)
        .add_global_label("host_id", host_id)
        .build()
        .context("PrometheusBuilder::build")?;
    metrics::set_global_recorder(recorder).map_err(|e| anyhow!("set_global_recorder: {e}"))?;
    std::thread::Builder::new()
        .name("obs-exporter".into())
        .spawn(move || {
            let rt = match tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
            {
                Ok(rt) => rt,
                Err(e) => {
                    tracing::error!(error = %e, "obs-exporter: runtime build failed");
                    return;
                }
            };
            if let Err(e) = rt.block_on(exporter) {
                tracing::error!(error = ?e, "obs-exporter: exporter terminated");
            }
        })
        .context("spawn obs-exporter thread")?;

    metrics::describe_gauge!(BUILD_INFO, "Build info; value is always 1.");
    metrics::gauge!(
        BUILD_INFO,
        "version" => version,
        "sha" => git_sha,
    )
    .set(1.0);

    metrics::describe_gauge!(SERVICE_UP, "1 while the service's exporter is live.");
    metrics::gauge!(SERVICE_UP).set(1.0);

    tracing::info!(
        service = service,
        host_id = host_id,
        addr = %metrics_addr,
        "kardamom_obs: prometheus exporter installed"
    );
    Ok(())
}

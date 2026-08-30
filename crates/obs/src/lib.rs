//! Shared Prometheus exporter init for every kardamom service binary.
//!
//! See `docs/specs/2026-05-29-prometheus-grafana-design.md`.

use std::net::SocketAddr;

use anyhow::{Context, Result, anyhow};
use metrics_exporter_prometheus::PrometheusBuilder;

pub mod bin;

/// [`init`] with the version/git-sha incantation filled in at the CALL
/// site, so each binary stamps its own crate version (a plain helper fn
/// would bake in kardamom-obs's).
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

/// Whether an exporter build failure is a TCP bind `AddrInUse` (the ONLY
/// retryable class — #122). Checked structurally through the error chain,
/// with a string fallback in case the exporter crate stringifies the io
/// error instead of sourcing it.
fn is_addr_in_use(e: &anyhow::Error) -> bool {
    e.chain().any(|c| {
        c.downcast_ref::<std::io::Error>()
            .is_some_and(|io| io.kind() == std::io::ErrorKind::AddrInUse)
    }) || format!("{e:#}").contains("Address already in use")
}

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
///
/// Must run inside a Tokio runtime: the exporter future is spawned onto the
/// ambient runtime (`tokio::spawn`). Every service binary is a
/// `#[tokio::main]`, so the call site is `init(...).await?` (or the
/// [`init_service!`] macro followed by `.await?`).
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

    // The exporter runs as a task on the service's runtime (the one tokio
    // runtime in the process). A dedicated thread + private runtime was the
    // earlier design (#76: keep /metrics answering when the service runtime
    // wedges); it was removed to keep one runtime for the async shell.
    //
    // Fail-fast includes the HTTP bind: metrics-exporter-prometheus (0.18)
    // binds the TCP listener synchronously inside `build()`, so a port
    // collision surfaces as an init error rather than a healthy-looking
    // service with no /metrics. Pinned by the init_port_in_use integration
    // test — if a dependency upgrade moves the bind into the exporter
    // future's first poll, that test fails and the bind must be made eager
    // here (e.g. pre-bind a std TcpListener).
    // #122: EADDRINUSE gets a BOUNDED retry; every other bind/build error
    // stays fail-fast. A port squatter is usually a wedged or frozen
    // predecessor seconds away from being reaped by its supervisor — dying
    // instantly burns one restart attempt per squat, and under a
    // `mode = "fail"` restart policy (the validator: 5 attempts, then stay
    // down) that converts a transient squat into a PERMANENT outage
    // (reproduced end-to-end in #122: frozen validator holds :9006, every
    // replacement dies at bind, alloc stranded). The default budget
    // (24 × 5 s = 2 min) outlives a supervisor reap cycle; tests shrink it
    // via the env knobs.
    let bind_retries: u32 = std::env::var("KARDAMOM_OBS_BIND_RETRIES")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(24);
    let bind_retry_delay = std::time::Duration::from_millis(
        std::env::var("KARDAMOM_OBS_BIND_RETRY_DELAY_MS")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(5_000),
    );
    let build_once = || {
        PrometheusBuilder::new()
            .with_http_listener(metrics_addr)
            .set_buckets(DURATION_BUCKETS)
            .context("set_buckets")
            .and_then(|b| {
                b.add_global_label("service", service)
                    .add_global_label("host_id", host_id)
                    .build()
                    .context("PrometheusBuilder::build")
            })
    };
    let mut built = build_once();
    let mut attempt: u32 = 0;
    while attempt < bind_retries && built.as_ref().err().is_some_and(is_addr_in_use) {
        attempt += 1;
        tracing::warn!(
            %metrics_addr,
            attempt,
            max = bind_retries,
            "metrics port in use (squatter not yet reaped?); retrying bind (#122)"
        );
        tokio::time::sleep(bind_retry_delay).await;
        built = build_once();
    }
    let (recorder, exporter) = built?;
    metrics::set_global_recorder(recorder).map_err(|e| anyhow!("set_global_recorder: {e}"))?;
    tokio::spawn(async move {
        if let Err(e) = exporter.await {
            tracing::error!(error = ?e, "obs-exporter: exporter terminated");
        }
    });

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

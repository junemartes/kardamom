//! Process scaffolding shared by every kardamom service binary: tracing
//! init and shutdown-signal waiting.
//!
//! Every service binary already links `kardamom-obs` for the Prometheus
//! exporter, so this is a light enough home for both helpers: a tracing
//! subscriber that honors `RUST_LOG`, and a shutdown wait that resolves on
//! SIGTERM or Ctrl-C.

/// Install the fmt tracing subscriber: `RUST_LOG` if set, "info" otherwise.
pub fn init_tracing() {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();
}

/// Resolve on SIGTERM (how the orchestrator stops a job) or Ctrl-C.
pub async fn wait_for_shutdown() {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{SignalKind, signal};
        let mut sigterm = match signal(SignalKind::terminate()) {
            Ok(s) => s,
            Err(e) => {
                tracing::error!(error = %e, "failed to install SIGTERM handler; falling back to Ctrl-C only");
                let _ = tokio::signal::ctrl_c().await;
                return;
            }
        };
        tokio::select! {
            _ = sigterm.recv() => tracing::info!("SIGTERM received"),
            _ = tokio::signal::ctrl_c() => tracing::info!("Ctrl-C received"),
        }
    }
    #[cfg(not(unix))]
    {
        let _ = tokio::signal::ctrl_c().await;
        tracing::info!("Ctrl-C received");
    }
}

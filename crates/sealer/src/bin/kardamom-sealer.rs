//! kardamom-sealer: block sealer process.
//!
//! Loads a TOML `SealerConfig`, validates it, and idles until SIGTERM /
//! Ctrl-C. The real tx_ordering publisher wrapper + the `Sealer::run_forever`
//! main loop land in a follow-up; today this binary parses + validates the
//! config and keeps the process alive so ansible / nomad can manage it as
//! a long-running service.

use std::path::PathBuf;

use anyhow::Result;
use clap::Parser;
use kardamom_sealer::SealerConfig;

#[derive(Debug, Parser)]
#[command(
    name = "kardamom-sealer",
    version,
    about = "kardamom block sealer process"
)]
struct Args {
    /// Path to a TOML config file (schema: `SealerConfig`).
    #[arg(long)]
    config: PathBuf,
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    let args = Args::parse();
    let raw = std::fs::read_to_string(&args.config)?;
    let cfg: SealerConfig = toml::from_str(&raw)?;
    cfg.validate()?;

    tracing::info!(
        host_id = cfg.host_id,
        tick_interval_ms = cfg.tick_interval_ms,
        "kardamom-sealer: config parsed; main loop wiring TBD; idling until shutdown signal"
    );

    wait_for_shutdown().await;
    tracing::info!("kardamom-sealer: shutdown signal received; exiting cleanly");
    Ok(())
}

/// Wait for SIGTERM or Ctrl-C, whichever arrives first.
async fn wait_for_shutdown() {
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

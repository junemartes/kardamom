//! `kardamom-executor`: standalone executor service process.
//!
//! Loads a TOML config, validates it, and idles until SIGTERM / Ctrl-C so
//! the binary is manageable by ansible / nomad / systemd as a long-running
//! process. The full main loop (M tx_data reader threads + 1 tx_ordering
//! reader + revm exec thread + state-writer commit thread, all hung off
//! `log::aeron_live::AeronRuntime`) lands in a follow-up; today this
//! binary serves three roles:
//!
//!   1. Exists with the canonical service-binary name so deployment
//!      tooling can target `kardamom-executor --config /etc/kardamom/executor.toml`.
//!   2. Surfaces config-file errors at startup (fails fast rather than mid-run).
//!   3. Stays running until a shutdown signal arrives, so process
//!      managers can launch / restart / monitor it normally.

use std::path::PathBuf;

use anyhow::Result;
use clap::Parser;

#[derive(Debug, Parser)]
#[command(
    name = "kardamom-executor",
    version,
    about = "kardamom executor process"
)]
struct Args {
    /// Path to the TOML config file (schema: `ExecutorConfig`).
    #[arg(long)]
    config: PathBuf,
}

#[tokio::main]
async fn main() -> Result<()> {
    init_tracing();
    let args = Args::parse();
    let raw = std::fs::read_to_string(&args.config)?;
    tracing::info!(
        config_path = %args.config.display(),
        config_bytes = raw.len(),
        "kardamom-executor: config loaded; main loop wiring TBD; idling until shutdown signal"
    );
    wait_for_shutdown().await;
    tracing::info!("kardamom-executor: shutdown signal received; exiting cleanly");
    Ok(())
}

fn init_tracing() {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();
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

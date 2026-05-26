//! kardamom-sequencer: per-partition sequencer process.
//!
//! Parses a TOML `SequencerConfig`, validates it, and idles until SIGTERM /
//! Ctrl-C. The tx_data subscriber + tx_ordering publisher + sequencer main
//! loop are wired in a follow-up; today this binary parses + validates the
//! config and keeps the process alive so ansible / nomad can manage it as
//! a long-running service.

use std::path::PathBuf;

use clap::Parser;
use kardamom_sequencer::config::SequencerConfig;

#[derive(Debug, Parser)]
#[command(name = "kardamom-sequencer", version, about = "S2 sequencer process")]
struct Args {
    /// Path to a TOML config file (schema: `SequencerConfig`).
    #[arg(long)]
    config: PathBuf,
    /// Override the partition index from the config.
    #[arg(long)]
    partition_index: Option<u32>,
    /// Override the partition count (M).
    #[arg(long)]
    partition_count: Option<u32>,
    /// Override the sequencer id embedded in every tx_ordering `TxRef`
    ///. If omitted and the TOML did not set it, falls back to
    /// `partition_index as u8` so the default M=8 deployment "just
    /// works".
    #[arg(long)]
    sequencer_id: Option<u8>,
    /// Override the CPU core to pin to.
    #[arg(long)]
    core_id: Option<usize>,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    let args = Args::parse();
    let raw = std::fs::read_to_string(&args.config)?;
    let mut cfg: SequencerConfig = toml::from_str(&raw)?;

    if let Some(i) = args.partition_index {
        cfg.partition_index = i;
    }
    if let Some(m) = args.partition_count {
        cfg.partition_count = m;
    }
    if let Some(id) = args.sequencer_id {
        cfg.sequencer_id = id;
    } else if cfg.sequencer_id == 0 && cfg.partition_index != 0 {
        // Convenience: if the operator only specified --partition-index N
        // (without an explicit --sequencer-id or a TOML override) and the
        // TOML left the field at its default of 0, derive the conventional
        // "one sequencer per partition" id automatically. This avoids the
        // failure mode where every process writes tx_ordering refs with
        // sequencer_id=0 and downstream consumers can't tell them apart.
        cfg.sequencer_id = cfg.partition_index as u8;
        tracing::info!(
            partition_index = cfg.partition_index,
            sequencer_id = cfg.sequencer_id,
            "sequencer_id defaulted from partition_index"
        );
    }
    if let Some(c) = args.core_id {
        cfg.core_id = Some(c);
    }
    cfg.validate()?;

    tracing::info!(
        ?cfg,
        "kardamom-sequencer: config parsed; main loop wiring TBD; idling until shutdown signal"
    );

    wait_for_shutdown().await;
    tracing::info!("kardamom-sequencer: shutdown signal received; exiting cleanly");
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

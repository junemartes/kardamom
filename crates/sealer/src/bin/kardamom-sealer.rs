//! kardamom-sealer: block sealer CLI.
//!
//! Loads a TOML `SealerConfig`, validates it, and (eventually) opens the
//! tx_ordering publisher via `log` to run the sealer's `run_forever` loop.
//! For now this binary is a CLI smoke runner — it parses + validates the
//! config and exits 0; the live wrapper lands with the cross-component
//! real-Aeron e2e.

use std::path::PathBuf;

use anyhow::Result;
use clap::Parser;
use sealer::SealerConfig;

#[derive(Debug, Parser)]
#[command(name = "kardamom-sealer", version, about = "S5 block sealer")]
struct Args {
    /// Path to a TOML config file (schema: `SealerConfig`).
    #[arg(long)]
    config: PathBuf,
}

fn main() -> Result<()> {
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
        "kardamom-sealer config parsed"
    );

    eprintln!(
        "kardamom-sealer: config validated. The real tx_ordering wrapper is \
         still landing as part of the cross-component real-Aeron e2e; this \
         binary is currently a CLI smoke runner. Exiting 0."
    );
    Ok(())
}

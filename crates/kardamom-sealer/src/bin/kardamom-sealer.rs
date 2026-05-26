//! kardamom-sealer: S5 block sealer CLI.
//!
//! Loads a TOML `SealerConfig`, validates it, and either:
//!
//!   - With `aeron-live` enabled: opens the tx_ordering publisher + per-recorder
//!     watermark subscribers via `kardamom-log` and runs the sealer's
//!     `run_forever` loop. (Wiring lands alongside the cross-component
//!     real-Aeron e2e work; see the spec §"Implementation order".)
//!
//!   - Without `aeron-live`: parses + validates the config, prints a clear
//!     "aeron-live build required" message, and exits with status 0 so smoke
//!     tests can exercise the CLI surface without an Aeron environment.
//!
//! This mirrors the convention already established by the S2
//! `kardamom-sequencer` binary: the CLI smoke-tests the config in any
//! environment; production wiring is gated on the `aeron-live` feature.

use std::path::PathBuf;

use anyhow::Result;
use clap::Parser;
use kardamom_sealer::SealerConfig;

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
        recorder_ids = ?cfg.recorder_ids,
        tick_interval_ms = cfg.tick_interval_ms,
        "kardamom-sealer config parsed"
    );

    #[cfg(not(feature = "aeron-live"))]
    {
        eprintln!(
            "kardamom-sealer: built without the aeron-live feature; the live \
             tx_ordering publisher and per-recorder watermark subscribers \
             require it. Re-build with `--features aeron-live` to run \
             against real Aeron. Exiting 0 (config validated)."
        );
        Ok(())
    }

    #[cfg(feature = "aeron-live")]
    {
        eprintln!(
            "kardamom-sealer: aeron-live feature is enabled but the real \
             tx_ordering wrapper is still landing as part of the cross-component \
             real-Aeron e2e. Config validated; exiting 0."
        );
        Ok(())
    }
}

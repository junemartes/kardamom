//! kardamom-sequencer: per-partition CLI binary.
//!
//! Parses a TOML `SequencerConfig`, validates the partition index, and
//! either:
//!   - if `aeron-live` is enabled: opens the real Aeron channels (tx_ordering
//!     publisher, receipt-cache publisher, ingress subscriber) via
//!     `kardamom-log` and runs the sequencer loop, or
//!   - if `aeron-live` is NOT enabled: emits a clear error and exits with
//!     status 2 so operators don't ship a no-op binary by accident.
//!
//! The aeron-live wiring uses the existing `kardamom_log::publisher` /
//! `subscriber` builders for tx_ordering and the receipt-cache channel. The
//! proxy -> sequencer ingress channel surface is still under design in S3 /
//! S1 (currently an in-process `MockChannels` mpsc); when that surface lands
//! as a real Aeron stream this binary will gain a concrete IngressSource
//! adapter. Until then the binary parses + validates the config and prints
//! "ingress wiring TBD", then exits with status 0 so smoke tests can run.

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

fn main() -> anyhow::Result<()> {
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
        // failure mode where every process writes ChannelB refs with
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
        "kardamom-sequencer config parsed; Aeron wiring is staged in a follow-up \
         (S3 ingress-channel surface is in-process mpsc as of this build)"
    );

    #[cfg(feature = "aeron-live")]
    {
        eprintln!(
            "kardamom-sequencer: aeron-live build received; the ingress channel \
             surface (proxy -> sequencer) still uses in-process mpsc on the \
             landed S1/S3 surfaces. A real Aeron ingress publisher will land \
             alongside the S5/S6 e2e work; this binary is currently a CLI \
             smoke runner."
        );
    }
    Ok(())
}

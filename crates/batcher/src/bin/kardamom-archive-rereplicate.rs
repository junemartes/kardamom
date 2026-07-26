//! `kardamom-archive-rereplicate` — restore or repair an Aeron Archive from a peer.
//!
//! `tx_data` is recorded on both ingress replicas (two node-independent mirror
//! copies). If one node's archive volume is lost, the default mode restores it
//! from the surviving peer's copy, returning the cluster to full 2-copy
//! redundancy. `--heal` repairs *corruption* instead: copy only the named (or
//! auto-detected diverging) segments, leaving the rest of the archive alone.
//!
//! It is a file-level segment mirror (rusteron-archive doesn't expose Aeron's
//! network `replicate()`): copy `.rec` segments + `archive.catalog` — never
//! `archive-mark.dat` (a live source heartbeats its mark; a transplanted copy
//! makes the destination's Archive crash-loop on 'active Mark file detected').
//! The **destination archive daemon must be stopped** during
//! any copy. Cross-node transport (docker cp / tar / scp) is the operator's job;
//! this operates on two locally-visible archive `dir/` paths and verifies the
//! result. Mirror inequality proves one side is corrupt, not which: the arbiter
//! is a CRC-armed `aeron-archive verify` (record-time CRC32 is enabled in the
//! driver), run on the suspect node before its daemon rejoins.

use std::path::PathBuf;

use anyhow::Context;
use clap::Parser;
use kardamom_batcher::rereplicate::{diff_mirror, heal_from_mirror, mirror_archive, verify_mirror};
use tracing::info;

#[derive(Parser, Debug)]
#[command(name = "kardamom-archive-rereplicate", version)]
struct Cli {
    /// Source archive `dir/` (a surviving peer's `.rec` segments + catalog).
    #[arg(long)]
    source_dir: PathBuf,

    /// Destination archive `dir/` (the wiped or corrupt node). Its archive
    /// daemon must be stopped. Created if absent.
    #[arg(long)]
    dest_dir: PathBuf,

    /// Skip the post-copy content verification pass.
    #[arg(long, default_value_t = false)]
    no_verify: bool,

    /// Heal mode: copy only diverging segments instead of the whole archive.
    /// Segments come from `--segments`, or are auto-detected by a content
    /// diff against the source mirror when the flag is omitted.
    #[arg(long, default_value_t = false)]
    heal: bool,

    /// Comma-separated bare `.rec` segment names to heal (with `--heal`).
    #[arg(long, value_delimiter = ',')]
    segments: Vec<String>,

    /// Report diverging segments (one per line, exit 3 if any) and change
    /// nothing. Composable: `--diff` then `--heal --segments <names>`.
    #[arg(long, default_value_t = false)]
    diff: bool,
}

fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt().with_env_filter("info").init();
    let cli = Cli::parse();

    if cli.diff {
        let diverged = diff_mirror(&cli.source_dir, &cli.dest_dir).context("diff mirror")?;
        for name in &diverged {
            println!("{name}");
        }
        println!("diverged segments={}", diverged.len());
        if !diverged.is_empty() {
            std::process::exit(3);
        }
        return Ok(());
    }

    if cli.heal {
        let segments = if cli.segments.is_empty() {
            diff_mirror(&cli.source_dir, &cli.dest_dir).context("detect diverging segments")?
        } else {
            cli.segments.clone()
        };
        let report = heal_from_mirror(&cli.source_dir, &cli.dest_dir, &segments)
            .with_context(|| format!("heal {} segment(s)", segments.len()))?;
        info!(
            segments = report.segments_healed,
            bytes = report.bytes_copied,
            "healed diverging archive segments from mirror"
        );
        if !cli.no_verify {
            let verified =
                verify_mirror(&cli.source_dir, &cli.dest_dir).context("verify mirror")?;
            info!(verified, "segment contents verified against source");
        }
        // Machine-readable line for chaos/runbook assertions.
        println!(
            "healed segments={} bytes={}",
            report.segments_healed, report.bytes_copied
        );
        return Ok(());
    }

    let report = mirror_archive(&cli.source_dir, &cli.dest_dir).with_context(|| {
        format!(
            "mirror {} -> {}",
            cli.source_dir.display(),
            cli.dest_dir.display()
        )
    })?;
    info!(
        segments = report.segments_copied,
        bytes = report.bytes_copied,
        catalog = report.catalog_copied,
        "re-replicated archive segments"
    );

    if !cli.no_verify {
        let verified = verify_mirror(&cli.source_dir, &cli.dest_dir).context("verify mirror")?;
        info!(verified, "segment contents verified against source");
    }

    // Machine-readable line for chaos/runbook assertions.
    println!(
        "rereplicated segments={} bytes={} catalog={}",
        report.segments_copied, report.bytes_copied, report.catalog_copied
    );
    Ok(())
}

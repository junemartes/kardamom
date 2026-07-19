//! `kardamom-archive-rereplicate` — restore a wiped Aeron Archive from a peer.
//!
//! `tx_data` is recorded on both ingress replicas (two node-independent mirror
//! copies). If one node's archive volume is lost, this restores it from the
//! surviving peer's copy, returning the cluster to full 2-copy redundancy.
//!
//! It is a file-level segment mirror (rusteron-archive doesn't expose Aeron's
//! network `replicate()`): copy every `.rec` segment + `archive.catalog` +
//! `archive-mark.dat`. The **destination archive daemon must be stopped** during
//! the copy. Cross-node transport (docker cp / tar / scp) is the operator's job;
//! this operates on two locally-visible archive `dir/` paths and verifies the
//! result. Run `aeron-archive verify` on the restored node before the daemon
//! rejoins for a frame-level check.

use std::path::PathBuf;

use anyhow::Context;
use clap::Parser;
use kardamom_batcher::rereplicate::{mirror_archive, verify_mirror};
use tracing::info;

#[derive(Parser, Debug)]
#[command(name = "kardamom-archive-rereplicate", version)]
struct Cli {
    /// Source archive `dir/` (a surviving peer's `.rec` segments + catalog).
    #[arg(long)]
    source_dir: PathBuf,

    /// Destination archive `dir/` (the wiped node). Its archive daemon must be
    /// stopped. Created if absent.
    #[arg(long)]
    dest_dir: PathBuf,

    /// Skip the post-copy size/count verification pass.
    #[arg(long, default_value_t = false)]
    no_verify: bool,
}

fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt().with_env_filter("info").init();
    let cli = Cli::parse();

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
        mark = report.mark_copied,
        "re-replicated archive segments"
    );

    if !cli.no_verify {
        let verified = verify_mirror(&cli.source_dir, &cli.dest_dir).context("verify mirror")?;
        info!(verified, "segment sizes verified against source");
    }

    // Machine-readable line for chaos/runbook assertions.
    println!(
        "rereplicated segments={} bytes={} catalog={} mark={}",
        report.segments_copied, report.bytes_copied, report.catalog_copied, report.mark_copied
    );
    Ok(())
}

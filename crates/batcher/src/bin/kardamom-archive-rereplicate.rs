//! `kardamom-archive-rereplicate`: restore or repair an Aeron Archive from
//! a peer.
//!
//! `tx_data` is recorded on both ingress replicas, giving two
//! node-independent mirror copies. If one node's archive volume is lost,
//! the default mode restores it from the surviving peer's copy, and
//! returns the cluster to full 2-copy redundancy. `--heal` repairs
//! corruption instead: it copies only the named (or auto-detected
//! diverging) segments, leaving the rest of the archive alone.
//!
//! This is a file-level segment mirror, because rusteron-archive does not
//! expose Aeron's network `replicate()`. It copies `.rec` segments and
//! `archive.catalog`, but never `archive-mark.dat`. A live source
//! heartbeats its mark, so a transplanted copy would make the
//! destination's Archive crash-loop on 'active Mark file detected'. The
//! destination archive daemon must be stopped during any copy.
//! Cross-node transport (docker cp, tar, or scp) is the operator's job;
//! this tool operates on two locally visible archive `dir/` paths and
//! verifies the result. A mirror mismatch proves one side is corrupt, but
//! not which one. The arbiter is a CRC-armed `aeron-archive verify`
//! (record-time CRC32 is enabled in the driver), run on the suspect node
//! before its daemon rejoins.

use std::path::PathBuf;

use anyhow::Context;
use clap::Parser;
use kardamom_batcher::rereplicate::{diff_mirror, heal_from_mirror, mirror_archive, verify_mirror};
use tracing::info;

#[derive(Parser, Debug)]
#[command(name = "kardamom-archive-rereplicate", version)]
struct Cli {
    /// The source archive `dir/`: a surviving peer's `.rec` segments and
    /// catalog.
    #[arg(long)]
    source_dir: PathBuf,

    /// The destination archive `dir/`: the wiped or corrupt node. Its
    /// archive daemon must be stopped. This creates the directory if it is
    /// absent.
    #[arg(long)]
    dest_dir: PathBuf,

    /// Skip the post-copy content verification pass.
    #[arg(long, default_value_t = false)]
    no_verify: bool,

    /// Heal mode: copy only diverging segments instead of the whole archive.
    /// Segments come from `--segments`. If that flag is omitted, this tool
    /// auto-detects them with a content diff against the source mirror.
    #[arg(long, default_value_t = false)]
    heal: bool,

    /// Comma-separated bare `.rec` segment names to heal (with `--heal`).
    #[arg(long, value_delimiter = ',')]
    segments: Vec<String>,

    /// Report diverging segments, one per line, and exit with code 3 if any
    /// exist. Change nothing. Compose it as `--diff`, then
    /// `--heal --segments <names>`.
    #[arg(long, default_value_t = false)]
    diff: bool,
}

fn main() -> anyhow::Result<()> {
    kardamom_obs::bin::init_tracing();
    let cli = Cli::parse();

    if cli.diff {
        let diff = diff_mirror(&cli.source_dir, &cli.dest_dir).context("diff mirror")?;
        for name in &diff.diverged {
            println!("{name}");
        }
        // Dest-only segments are recording ids the mirror never opened, from
        // a daemon restart or a post-restore session. They are a divergence
        // the mirror cannot vouch for, and --heal cannot repair them from
        // this source. Tag them so scripted callers can tell the two
        // classes apart.
        for name in &diff.dest_only {
            println!("{name} dest-only (no source counterpart; unhealable from this mirror)");
        }
        println!(
            "diverged segments={} dest-only={}",
            diff.diverged.len(),
            diff.dest_only.len()
        );
        if !diff.is_clean() {
            std::process::exit(3);
        }
        return Ok(());
    }

    if cli.heal {
        let segments = if cli.segments.is_empty() {
            // Auto-detect heals only what the mirror can vouch for.
            // Dest-only segments have no source bytes to copy, and --diff
            // reports them instead.
            let diff =
                diff_mirror(&cli.source_dir, &cli.dest_dir).context("detect diverging segments")?;
            if !diff.dest_only.is_empty() {
                tracing::warn!(
                    dest_only = diff.dest_only.len(),
                    "destination has segments with no mirror counterpart; not healable from this source (see --diff)"
                );
            }
            diff.diverged
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

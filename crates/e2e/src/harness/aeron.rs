//! Host-native `ArchivingMediaDriver` for the Rust services.
//!
//! Mirrors `just aeron-driver-up` (same jar, same JVM opens, same 4 MiB term
//! buffers) but on per-stack temp dirs and OS-assigned ports so concurrent
//! stacks never collide. The Java `ClusterNode` runs its OWN embedded driver
//! (see `sealer.rs`); this one is only for `kardamom-{ingress,sequencer,
//! executor}` attaching via `--aeron-dir`.

use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::Duration;

use anyhow::{Context, Result};

use super::proc::{Proc, free_udp_port, wait_for_file};

/// Locate the aeron-all jar: `KARDAMOM_AERON_ALL_JAR`, else the
/// `just aeron-driver-up` cache path.
pub fn aeron_all_jar() -> Result<PathBuf> {
    if let Ok(p) = std::env::var("KARDAMOM_AERON_ALL_JAR") {
        let p = PathBuf::from(p);
        anyhow::ensure!(
            p.is_file(),
            "KARDAMOM_AERON_ALL_JAR={} not found",
            p.display()
        );
        return Ok(p);
    }
    let cached = PathBuf::from("/tmp/kardamom-aeron-local/aeron-all-1.45.0.jar");
    anyhow::ensure!(
        cached.is_file(),
        "aeron-all jar not found at {} — run `just aeron-driver-up` once (it downloads the \
         jar) or set KARDAMOM_AERON_ALL_JAR",
        cached.display()
    );
    Ok(cached)
}

pub struct MediaDriver {
    pub proc: Proc,
    pub aeron_dir: PathBuf,
    pub archive_dir: PathBuf,
}

impl MediaDriver {
    pub fn launch(root: &Path) -> Result<Self> {
        let jar = aeron_all_jar()?;
        let aeron_dir = root.join("md-aeron");
        let archive_dir = root.join("md-archive");
        std::fs::create_dir_all(&aeron_dir)?;
        std::fs::create_dir_all(&archive_dir)?;
        let (ctrl, ctrl_rsp, repl) = (free_udp_port()?, free_udp_port()?, free_udp_port()?);

        let mut cmd = Command::new("java");
        cmd.args([
            "--add-opens",
            "java.base/sun.nio.ch=ALL-UNNAMED",
            "--add-opens",
            "java.base/java.util.zip=ALL-UNNAMED",
            "--add-opens",
            "java.base/jdk.internal.misc=ALL-UNNAMED",
        ])
        .arg(format!("-Daeron.dir={}", aeron_dir.display()))
        .arg(format!("-Daeron.archive.dir={}", archive_dir.display()))
        .arg("-Daeron.term.buffer.length=4194304")
        .arg("-Daeron.ipc.term.buffer.length=4194304")
        .arg(format!(
            "-Daeron.archive.control.channel=aeron:udp?endpoint=127.0.0.1:{ctrl}"
        ))
        .arg(format!(
            "-Daeron.archive.control.response.channel=aeron:udp?endpoint=127.0.0.1:{ctrl_rsp}"
        ))
        .arg(format!(
            "-Daeron.archive.replication.channel=aeron:udp?endpoint=127.0.0.1:{repl}"
        ))
        .args([
            "-cp",
            &jar.display().to_string(),
            "io.aeron.archive.ArchivingMediaDriver",
        ]);

        let mut proc = Proc::spawn("media-driver", cmd, root.join("media-driver.log"))?;
        wait_for_file(
            &mut proc,
            &aeron_dir.join("cnc.dat"),
            Duration::from_secs(30),
        )
        .context("media driver cnc.dat")?;
        wait_for_file(
            &mut proc,
            &archive_dir.join("archive.catalog"),
            Duration::from_secs(30),
        )
        .context("media driver archive.catalog")?;
        Ok(Self {
            proc,
            aeron_dir,
            archive_dir,
        })
    }
}

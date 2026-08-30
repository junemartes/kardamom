//! Host-native `ArchivingMediaDriver` for the Rust services.
//!
//! This matches `just aeron-driver-up` (same jar, same JVM opens, same
//! 4 MiB term buffers), but it uses per-stack temp directories and
//! OS-assigned ports, so concurrent stacks never collide. The Java
//! `ClusterNode` runs its own embedded driver (see `sealer.rs`). This
//! driver is only for `kardamom-{ingress,sequencer,executor}`, which
//! attach through `--aeron-dir`.

use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::Duration;

use anyhow::{Context, Result};

use super::proc::{Proc, free_udp_port, wait_for_file};

/// Find the aeron-all jar. Use `KARDAMOM_AERON_ALL_JAR` if set, else the
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
    /// The archive's UDP control endpoint. Refetch clients, such as the
    /// executor's join-miss recovery, address the archive at this
    /// endpoint.
    pub archive_control_endpoint: String,
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
        // This is a GC and safepoint log. The driver-stall flake family
        // (client aborts with a "MediaDriver keepalive: age>10000ms"
        // message) needs to tell a JVM pause apart from host-level CPU or
        // disk starvation. Compare this log with the stack's
        // host-load.log timeline.
        .arg(format!(
            "-Xlog:gc*,safepoint=info:file={}/md-jvm.log:time,uptime,level,tags",
            root.display()
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
            archive_control_endpoint: format!("127.0.0.1:{ctrl}"),
        })
    }
}

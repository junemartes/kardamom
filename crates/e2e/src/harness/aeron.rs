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
use kardamom_obs::testkit::free_udp_port;

use super::proc::{ExistingFile, Proc, resolve_artifact, wait_for_file};

/// Find the aeron-all jar. Use `KARDAMOM_AERON_ALL_JAR` if set, else the
/// `just aeron-driver-up` cache path.
///
/// # Errors
/// Returns an error when neither location holds a file.
fn aeron_all_jar() -> Result<ExistingFile> {
    resolve_artifact(
        "KARDAMOM_AERON_ALL_JAR",
        PathBuf::from("/tmp/kardamom-aeron-local/aeron-all-1.45.0.jar"),
        "run `just aeron-driver-up` once (it downloads the jar) or set KARDAMOM_AERON_ALL_JAR",
    )
}

pub struct MediaDriver {
    pub proc: Proc,
    pub aeron_dir: PathBuf,
    pub(crate) archive_dir: PathBuf,
    /// The archive's UDP control endpoint. Refetch clients, such as the
    /// executor's join-miss recovery, address the archive at this
    /// endpoint.
    pub(crate) archive_control_endpoint: String,
    /// The launch settings, kept so that the driver can start again on
    /// the same directories and ports.
    launch: DriverLaunch,
}

/// What one driver process needs: the jar, the stack root, and the three
/// archive UDP ports. Every start of the driver uses the same values, so
/// the clients keep the archive endpoint they hold.
struct DriverLaunch {
    jar: ExistingFile,
    root: PathBuf,
    aeron_dir: PathBuf,
    archive_dir: PathBuf,
    ports: [u16; 3],
}

impl DriverLaunch {
    /// Spawn the driver and wait until its `cnc.dat` and its archive
    /// catalog exist. `log_name` names the log file in the stack root.
    fn spawn(&self, log_name: &str) -> Result<Proc> {
        let [ctrl, ctrl_rsp, repl] = self.ports;
        let mut cmd = Command::new("java");
        cmd.args([
            "--add-opens",
            "java.base/sun.nio.ch=ALL-UNNAMED",
            "--add-opens",
            "java.base/java.util.zip=ALL-UNNAMED",
            "--add-opens",
            "java.base/jdk.internal.misc=ALL-UNNAMED",
        ])
        .arg(format!("-Daeron.dir={}", self.aeron_dir.display()))
        .arg(format!(
            "-Daeron.archive.dir={}",
            self.archive_dir.display()
        ))
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
            "-Xlog:gc*,safepoint=info:file={}/{log_name}-jvm.log:time,uptime,level,tags",
            self.root.display()
        ))
        .args([
            "-cp",
            &self.jar.to_string(),
            "io.aeron.archive.ArchivingMediaDriver",
        ]);

        let mut proc = Proc::spawn(
            "media-driver",
            cmd,
            self.root.join(format!("{log_name}.log")),
        )?;
        wait_for_file(
            &mut proc,
            &self.aeron_dir.join("cnc.dat"),
            Duration::from_secs(30),
        )
        .context("media driver cnc.dat")?;
        wait_for_file(
            &mut proc,
            &self.archive_dir.join("archive.catalog"),
            Duration::from_secs(30),
        )
        .context("media driver archive.catalog")?;
        Ok(proc)
    }
}

impl DriverLaunch {
    /// Spawn the driver and require it to run three seconds later: an
    /// archive that refuses its mark file exits within two seconds of the
    /// JVM start.
    /// `None` when the process exited, also before its readiness files.
    fn spawn_settled(&self, log_name: &str) -> Option<Proc> {
        let mut proc = self.spawn(log_name).ok()?;
        std::thread::sleep(Duration::from_secs(3));
        proc.is_alive().then_some(proc)
    }
}

impl MediaDriver {
    /// # Errors
    /// Returns an error when the aeron-all jar is missing, when the temp
    /// directories cannot be created, when the driver process fails to
    /// spawn, or when its readiness files do not appear within 30s.
    pub fn launch(root: &Path) -> Result<Self> {
        let launch = DriverLaunch {
            jar: aeron_all_jar()?,
            root: root.to_path_buf(),
            aeron_dir: root.join("md-aeron"),
            archive_dir: root.join("md-archive"),
            ports: std::array::from_fn(|_| free_udp_port().port()),
        };
        std::fs::create_dir_all(&launch.aeron_dir)?;
        std::fs::create_dir_all(&launch.archive_dir)?;
        let proc = launch.spawn("media-driver")?;
        Ok(Self {
            proc,
            aeron_dir: launch.aeron_dir.clone(),
            archive_dir: launch.archive_dir.clone(),
            archive_control_endpoint: format!("127.0.0.1:{}", launch.ports[0]),
            launch,
        })
    }

    /// Start the driver again on the same directories and ports, after a
    /// kill. The archive keeps its catalog and recordings. The call
    /// returns once the driver and its archive are up.
    ///
    /// # Errors
    /// Returns an error when the driver fails to spawn, or when its
    /// readiness files do not appear within 30s.
    pub fn restart(&mut self) -> Result<()> {
        self.proc.kill();
        // The readiness check waits for a new `cnc.dat`. The killed driver
        // left its own one behind, and no client may use it.
        std::fs::remove_dir_all(&self.aeron_dir).context("remove the old aeron.dir")?;
        // The killed archive left its mark file, which reads as active
        // until its heartbeat ages out. The new archive refuses to start
        // until then, as a scheduler's restarts of the driver task see.
        self.proc = super::metrics::poll_sync(
            "the returned driver keeps its archive up",
            Duration::from_secs(60),
            Duration::from_millis(500),
            || Ok(self.launch.spawn_settled("media-driver-returned")),
        )?;
        Ok(())
    }
}

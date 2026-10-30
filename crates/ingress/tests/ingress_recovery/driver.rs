//! A host-native archiving media driver: one per simulated node. Two
//! drivers on one host talk over loopback UDP, as two nodes do, so a
//! driver loss takes only the clients of its own node.

use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

/// The aeron-all jar: `KARDAMOM_AERON_ALL_JAR`, else the cache path of
/// `just aeron-driver-up`.
fn jar() -> PathBuf {
    let jar = std::env::var("KARDAMOM_AERON_ALL_JAR").map_or_else(
        |_| PathBuf::from("/tmp/kardamom-aeron-local/aeron-all-1.45.0.jar"),
        PathBuf::from,
    );
    assert!(jar.is_file(), "no aeron-all jar at {}", jar.display());
    jar
}

/// One driver process and its directories. The process is killed on drop.
pub(crate) struct Driver {
    root: PathBuf,
    name: String,
    child: Option<Child>,
    archive_port: u16,
    ports: [u16; 2],
}

impl Driver {
    /// Start a driver named `name` under `root`.
    pub(crate) fn start(root: &Path, name: &str) -> Self {
        let mut driver = Self {
            root: root.to_path_buf(),
            name: name.to_string(),
            child: None,
            archive_port: super::free_udp_port(),
            ports: [super::free_udp_port(), super::free_udp_port()],
        };
        std::fs::create_dir_all(driver.archive_dir()).unwrap();
        driver.child = Some(
            driver
                .spawn(Duration::from_secs(30))
                .unwrap_or_else(|| panic!("driver {name} did not start")),
        );
        driver
    }

    pub(crate) fn aeron_dir(&self) -> PathBuf {
        self.root.join(format!("{}-aeron", self.name))
    }

    pub(crate) fn archive_dir(&self) -> PathBuf {
        self.root.join(format!("{}-archive", self.name))
    }

    /// The archive control endpoint, `host:port`.
    pub(crate) fn archive_endpoint(&self) -> String {
        format!("127.0.0.1:{}", self.archive_port)
    }

    /// Spawn the process and wait until its `cnc.dat` and archive catalog
    /// exist. `None` when the process exits first or `bound` passes.
    fn spawn(&self, bound: Duration) -> Option<Child> {
        let [response, replication] = self.ports;
        let log = std::fs::File::create(self.root.join(format!("{}.log", self.name))).unwrap();
        let mut child = Command::new("java")
            .args([
                "--add-opens",
                "java.base/sun.nio.ch=ALL-UNNAMED",
                "--add-opens",
                "java.base/jdk.internal.misc=ALL-UNNAMED",
            ])
            .arg(format!("-Daeron.dir={}", self.aeron_dir().display()))
            .arg(format!(
                "-Daeron.archive.dir={}",
                self.archive_dir().display()
            ))
            .arg("-Daeron.term.buffer.length=4194304")
            .arg(format!(
                "-Daeron.archive.control.channel=aeron:udp?endpoint={}",
                self.archive_endpoint()
            ))
            .arg(format!(
                "-Daeron.archive.control.response.channel=aeron:udp?endpoint=127.0.0.1:{response}"
            ))
            .arg(format!(
                "-Daeron.archive.replication.channel=aeron:udp?endpoint=127.0.0.1:{replication}"
            ))
            .args([
                "-cp",
                &jar().to_string_lossy(),
                "io.aeron.archive.ArchivingMediaDriver",
            ])
            .stdin(Stdio::null())
            .stdout(log.try_clone().unwrap())
            .stderr(log)
            .spawn()
            .unwrap();
        let end = Instant::now() + bound;
        let ready = std::iter::repeat_with(|| std::thread::sleep(Duration::from_millis(50)))
            .map(|()| {
                (
                    self.files_ready(),
                    child.try_wait().unwrap().is_none(),
                    Instant::now() < end,
                )
            })
            .find(|(ready, alive, in_time)| *ready || !*alive || !*in_time)
            .is_some_and(|(ready, alive, _)| ready && alive);
        if ready {
            return Some(child);
        }
        let _ = child.kill();
        let _ = child.wait();
        None
    }

    fn files_ready(&self) -> bool {
        self.aeron_dir().join("cnc.dat").exists()
            && self.archive_dir().join("archive.catalog").exists()
    }

    /// SIGKILL the driver, as a crash of its task does.
    pub(crate) fn kill(&mut self) {
        if let Some(mut child) = self.child.take() {
            let _ = child.kill();
            let _ = child.wait();
        }
    }

    /// Start the driver again on the same directories and ports. The
    /// killed archive leaves an active mark file; the new archive exits
    /// until the mark file ages out, as the restarts of a driver task see.
    pub(crate) fn restart(&mut self, bound: Duration) -> Duration {
        let started = Instant::now();
        std::fs::remove_dir_all(self.aeron_dir()).unwrap();
        self.child = Some(Self::retry(started + bound, || self.try_settled_start()));
        started.elapsed()
    }

    fn retry(end: Instant, mut start: impl FnMut() -> Option<Child>) -> Child {
        std::iter::repeat_with(|| (start(), Instant::now()))
            .find_map(|(child, now)| {
                assert!(now < end, "the driver did not return");
                child
            })
            .unwrap()
    }

    /// One start: the child when it still runs six seconds later.
    fn try_settled_start(&self) -> Option<Child> {
        let mut child = self.spawn(Duration::from_secs(10))?;
        std::thread::sleep(Duration::from_secs(6));
        child.try_wait().unwrap().is_none().then_some(child)
    }
}

impl Drop for Driver {
    fn drop(&mut self) {
        self.kill();
    }
}

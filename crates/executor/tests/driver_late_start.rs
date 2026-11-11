//! The executor process starts before the media driver of its node, as a
//! service on a returned node does. It must wait for the driver, then open
//! its Aeron publications and its local recording, with no exit.
//!
//! Gated on the `docker-e2e` feature and on Docker availability.

#![cfg(feature = "docker-e2e")]

use std::os::unix::process::CommandExt;
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

use kardamom_log::testing::PendingNode;

/// How long the driver stays away after the executor starts: past the
/// 10 s driver timeout that the C client waits on its own.
const DRIVER_LATE: Duration = Duration::from_secs(12);
/// The line of an executor whose local recording of its stream is active:
/// it opened its publications on the driver.
const RECORDING: &str = "exec stream: the local archive records the recorded publication";
/// The bound for the executor to reach [`RECORDING`] after the driver.
const AFTER_DRIVER: Duration = Duration::from_secs(45);

/// The executor process, killed when the test ends.
struct Executor {
    child: Child,
    log: std::path::PathBuf,
}

impl Drop for Executor {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

impl Executor {
    fn start(root: &std::path::Path, aeron_dir: &std::path::Path) -> Self {
        let config = root.join("executor.toml");
        std::fs::write(
            &config,
            "[cluster]\ningress_endpoints = \"0=127.0.0.1:20999\"\negress_channel = \"aeron:udp?endpoint=127.0.0.1:0\"\n",
        )
        .unwrap();
        let metrics = std::net::TcpListener::bind("127.0.0.1:0")
            .unwrap()
            .local_addr()
            .unwrap();
        let log = root.join("executor.log");
        let out = std::fs::File::create(&log).unwrap();
        let mut command = Command::new(Self::binary());
        command
            .arg("--config")
            .arg(&config)
            .arg("--aeron-dir")
            .arg(aeron_dir)
            .arg("--state-dir")
            .arg(root.join("state"))
            .arg("--metrics-addr")
            .arg(metrics.to_string())
            .env("AERON_DRIVER_TIMEOUT", "10000")
            .env("NO_COLOR", "1")
            .env("RUST_LOG", "info")
            .stdin(Stdio::null())
            .stdout(out.try_clone().unwrap())
            .stderr(out);
        // SAFETY: the hook only calls `prctl`, which is async-signal-safe.
        unsafe {
            command.pre_exec(|| {
                // The executor ends with the test, also when CI kills it.
                if libc::prctl(libc::PR_SET_PDEATHSIG, libc::SIGKILL) == 0 {
                    Ok(())
                } else {
                    Err(std::io::Error::last_os_error())
                }
            });
        }
        Self {
            child: command.spawn().unwrap(),
            log,
        }
    }

    /// The executor binary in the parent of the directory of this test.
    /// Cargo puts a test in `target/<profile>/deps` and the binary in
    /// `target/<profile>`, and the CI bundle of a stack suite keeps that
    /// layout: `stack/deps/<test>` and `stack/kardamom-executor`. The path
    /// that Cargo compiles in (`CARGO_BIN_EXE_*`) names the build host, so
    /// a CI shard on another host does not find it.
    fn binary() -> std::path::PathBuf {
        let test = std::env::current_exe().unwrap();
        let binary = test
            .parent()
            .and_then(std::path::Path::parent)
            .unwrap()
            .join("kardamom-executor");
        assert!(
            binary.is_file(),
            "no executor binary at {}: build it with `cargo build --bins -p kardamom-executor`",
            binary.display()
        );
        binary
    }

    fn log(&self) -> String {
        std::fs::read_to_string(&self.log).unwrap_or_default()
    }

    /// The process still runs.
    fn assert_alive(&mut self) {
        let status = self.child.try_wait().unwrap();
        assert!(
            status.is_none(),
            "the executor exited ({status:?}):\n{}",
            self.log()
        );
    }

    /// Wait until the log shows `line`, while the process runs.
    fn wait_for(&mut self, line: &str, within: Duration) {
        let started = Instant::now();
        while !self.log().contains(line) {
            self.check(started, within, line);
        }
    }

    fn check(&mut self, started: Instant, within: Duration, line: &str) {
        self.assert_alive();
        assert!(
            started.elapsed() < within,
            "no {line:?} within {within:?}:\n{}",
            self.log()
        );
        std::thread::sleep(Duration::from_millis(100));
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires Docker: a media driver and archive that start late"]
async fn an_executor_started_before_its_driver_waits_and_opens_its_streams() {
    kardamom_log::testing::require_docker().await;
    let pending = PendingNode::reserve().expect("a node directory");
    let root = tempfile::tempdir().unwrap();
    let mut executor = Executor::start(root.path(), &pending.aeron_dir());
    tokio::time::sleep(DRIVER_LATE).await;
    executor.assert_alive();
    let log = executor.log();
    assert!(
        log.contains("waiting for the media driver to start"),
        "no wait logged:\n{log}"
    );
    let _driver = pending.start().await.expect("the media driver starts");
    executor.wait_for(RECORDING, AFTER_DRIVER);
    executor.assert_alive();
    let log = executor.log();
    assert!(!log.contains("CnC file not created"), "{log}");
    assert!(!log.contains("TimedOut"), "{log}");
}

//! One ingress replica: the real `kardamom-ingress` binary as a child
//! process on its own media driver. It records `tx_data` into the
//! driver's archive, publishes over UDP to the pipeline, and reads the
//! receipts that the pipeline sends to its own endpoint.

use std::path::{Path, PathBuf};
use std::process::{Child, Command, ExitStatus, Stdio};
use std::time::{Duration, Instant};

use super::Channels;
use super::driver::Driver;

/// The binary next to the test executable (`target/<profile>/deps/<test>`
/// leads to `target/<profile>/`), as the stack bundle lays it out, else
/// the path that Cargo built.
fn binary() -> PathBuf {
    let exe = std::env::current_exe().unwrap();
    let next = exe
        .parent()
        .and_then(Path::parent)
        .map(|dir| dir.join("kardamom-ingress"));
    next.filter(|p| p.is_file())
        .unwrap_or_else(|| PathBuf::from(env!("CARGO_BIN_EXE_kardamom-ingress")))
}

/// How a replica stops.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Stop {
    /// SIGTERM: the replica drains its parked submits and exits.
    Graceful,
    /// SIGKILL: no drain.
    Hard,
}

/// The settings of one replica that a restart keeps.
pub(crate) struct Replica {
    pub(crate) id: u16,
    pub(crate) channels: Channels,
    pub(crate) rpc_port: u16,
    metrics_port: u16,
    root: PathBuf,
    query: String,
    child: Option<Child>,
    starts: u32,
}

impl Replica {
    /// A replica with its ports; not yet running.
    pub(crate) fn new(id: u16, root: &Path, channels: Channels, query: &str) -> Self {
        Self {
            id,
            channels,
            rpc_port: super::free_tcp_port(),
            metrics_port: super::free_tcp_port(),
            root: root.to_path_buf(),
            query: query.to_string(),
            child: None,
            starts: 0,
        }
    }

    pub(crate) fn url(&self) -> String {
        format!("http://127.0.0.1:{}", self.rpc_port)
    }

    /// The log config of this replica: its UDP channels, and the archive
    /// of its own driver for the `tx_data` recorders.
    fn log_config(&self, driver: &Driver) -> PathBuf {
        let path = self.root.join(format!("ingress-{}-channels.toml", self.id));
        std::fs::write(
            &path,
            format!(
                "[aeron]\narchive_dir = \"{}\"\ntx_data_archive_endpoints = [\"{}\"]\n\n\
                 [channels]\ntx_data_channel_template = \"{}\"\ntx_receipts_channel = \"{}\"\n",
                driver.archive_dir().display(),
                driver.archive_endpoint(),
                self.channels.tx_data_template,
                self.channels.receipts_channel(),
            ),
        )
        .unwrap();
        path
    }

    /// Start the replica on `driver`, on its ports.
    pub(crate) fn start(&mut self, driver: &Driver) {
        let config = self.root.join("ingress.toml");
        std::fs::write(&config, "").unwrap();
        self.starts += 1;
        let log = std::fs::File::create(
            self.root
                .join(format!("ingress-{}-{}.log", self.id, self.starts)),
        )
        .unwrap();
        let child = Command::new(binary())
            .arg("--config")
            .arg(&config)
            .arg("--log-config")
            .arg(self.log_config(driver))
            .arg("--aeron-dir")
            .arg(driver.aeron_dir())
            .args(["--jsonrpc-bind", &format!("127.0.0.1:{}", self.rpc_port)])
            .args([
                "--metrics-addr",
                &format!("127.0.0.1:{}", self.metrics_port),
            ])
            .args(["--ingress-id", &self.id.to_string()])
            .args(["--host-id", &format!("ingress-{}", self.id)])
            .args(["--shards", "1"])
            .args(["--ack-policy", "on-offer"])
            .args(["--chain-id", &super::CHAIN_ID.to_string()])
            .args(["--receipt-query-endpoints", &self.query])
            .args(["--executor-query-endpoints", &self.query])
            .arg("--archive-durability")
            .env("RUST_LOG", "info")
            .stdin(Stdio::null())
            .stdout(log.try_clone().unwrap())
            .stderr(log)
            .spawn()
            .unwrap();
        self.child = Some(child);
    }

    /// Stop the replica. A graceful stop must end within the park bound
    /// plus ten seconds, as within the job's kill timeout; returns the
    /// exit status.
    pub(crate) fn stop(&mut self, stop: Stop) -> ExitStatus {
        let mut child = self.child.take().expect("the replica runs");
        match stop {
            Stop::Hard => {
                child.kill().unwrap();
                child.wait().unwrap()
            }
            Stop::Graceful => {
                let pid = i32::try_from(child.id()).unwrap();
                // SAFETY: `kill` sends one signal to the child's pid.
                unsafe { libc::kill(pid, libc::SIGTERM) };
                Self::wait(&mut child, Duration::from_secs(40))
                    .expect("the replica exits within its drain bound")
            }
        }
    }

    /// Wait for the replica to exit on its own within `bound`, as after a
    /// driver loss. Returns its exit status.
    pub(crate) fn await_exit(&mut self, bound: Duration) -> Option<ExitStatus> {
        let status = Self::wait(self.child.as_mut()?, bound);
        if status.is_some() {
            self.child = None;
        }
        status
    }

    fn wait(child: &mut Child, bound: Duration) -> Option<ExitStatus> {
        let end = Instant::now() + bound;
        std::iter::repeat_with(|| std::thread::sleep(Duration::from_millis(50)))
            .map(|()| child.try_wait().unwrap())
            .find(|status| status.is_some() || Instant::now() >= end)
            .flatten()
    }
}

impl Drop for Replica {
    fn drop(&mut self) {
        if let Some(mut child) = self.child.take() {
            let _ = child.kill();
            let _ = child.wait();
        }
    }
}

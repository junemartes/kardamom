//! The validator under test: the production binary as a child process,
//! with its state directory, its metrics endpoint and its log file. The
//! test plays the supervisor: it freezes, continues, stops and starts the
//! process, and reads the verdict from the exported metrics.

use std::io::{Read, Write};
use std::net::{SocketAddr, TcpStream};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, ExitStatus, Stdio};
use std::time::{Duration, Instant};

/// The validator metrics that the chaos case reads.
pub(crate) const VERIFIED: &str = "validator_blocks_verified_total";
pub(crate) const COMMITTED: &str = "validator_committed_block";
pub(crate) const DIVERGENCE: &str = "validator_divergence_total";
pub(crate) const BAL_MISSING: &str = "validator_bal_missing_total";
pub(crate) const ACTIVE: &str = "validator_pipeline_active";
pub(crate) const ROOT_BLOCK: &str = "validator_state_root_block";

/// One sample of the exported metrics.
#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct Sample {
    pub(crate) verified: u64,
    pub(crate) committed: u64,
    pub(crate) divergence: u64,
    pub(crate) bal_missing: u64,
    pub(crate) active: bool,
    pub(crate) root_block: u64,
}

impl Sample {
    fn parse(body: &str) -> Self {
        let read = |name: &str| {
            body.lines()
                .filter(|line| !line.starts_with('#'))
                .find_map(|line| {
                    let (key, value) = line.split_once(' ')?;
                    (key == name || key.starts_with(&format!("{name}{{")))
                        .then(|| value.trim().parse::<f64>().ok())
                        .flatten()
                })
                .map_or(0, |v| {
                    // Counters and gauges of block numbers are integers.
                    #[allow(
                        clippy::cast_possible_truncation,
                        clippy::cast_sign_loss,
                        reason = "the metric values are non-negative integers"
                    )]
                    let v = v as u64;
                    v
                })
        };
        Self {
            verified: read(VERIFIED),
            committed: read(COMMITTED),
            divergence: read(DIVERGENCE),
            bal_missing: read(BAL_MISSING),
            active: read(ACTIVE) == 1,
            root_block: read(ROOT_BLOCK),
        }
    }

    /// The validator verifies live: the pipeline runs, the verified
    /// counter passed `floor`, and the committed block is within `lag`
    /// blocks of the executor head `head`.
    pub(crate) fn verifies_after(self, floor: u64, head: u64, lag: u64) -> bool {
        self.active
            && self.verified > floor
            && self.committed > 0
            && head.saturating_sub(self.committed) <= lag
    }
}

/// How the validator starts.
pub(crate) struct Settings {
    pub(crate) state_dir: PathBuf,
    pub(crate) aeron_dir: PathBuf,
    pub(crate) genesis: PathBuf,
    pub(crate) egress: String,
    /// The local checkpoint staging directory and the peers, for a join.
    pub(crate) checkpoints: Option<(PathBuf, String)>,
    pub(crate) chain_id: u64,
}

/// One run of the validator process.
pub(crate) struct Node {
    child: Child,
    metrics: SocketAddr,
    pub(crate) log: PathBuf,
}

impl Node {
    /// Start the binary with `settings`. Its output goes to `log`.
    pub(crate) fn start(settings: &Settings, log: &Path) -> Self {
        let config = settings.state_dir.with_extension("toml");
        std::fs::write(&config, "").unwrap();
        let metrics = kardamom_obs::testkit::free_port();
        let mut cmd = Command::new(env!("CARGO_BIN_EXE_kardamom-validator"));
        cmd.arg("--config")
            .arg(&config)
            .arg("--aeron-dir")
            .arg(&settings.aeron_dir)
            .args(["--chain-id", &settings.chain_id.to_string()])
            .arg("--chain")
            .arg(&settings.genesis)
            .arg("--state-dir")
            .arg(&settings.state_dir)
            .args(["--egress", &settings.egress])
            .args(["--metrics-addr", &metrics.to_string()])
            .args(["--host-id", "component-validator"])
            .env("RUST_LOG", "info");
        if let Some((dir, peers)) = &settings.checkpoints {
            cmd.arg("--checkpoint-dir")
                .arg(dir)
                .args(["--checkpoint-peers", peers]);
        }
        let out = std::fs::File::create(log).unwrap();
        let child = cmd
            .stdout(Stdio::from(out.try_clone().unwrap()))
            .stderr(Stdio::from(out))
            .spawn()
            .unwrap();
        Self {
            child,
            metrics,
            log: log.to_path_buf(),
        }
    }

    fn signal(&self, signal: libc::c_int) {
        let pid = libc::pid_t::try_from(self.child.id()).unwrap();
        // SAFETY: the pid names this test's own live child process.
        assert_eq!(unsafe { libc::kill(pid, signal) }, 0);
    }

    /// Stop every thread of the process, as `docker pause` and `SIGSTOP`
    /// do to the deployed task.
    pub(crate) fn freeze(&self) {
        self.signal(libc::SIGSTOP);
    }

    pub(crate) fn thaw(&self) {
        self.signal(libc::SIGCONT);
    }

    /// The exit status, once the process ended within `within`.
    pub(crate) fn exited(&mut self, within: Duration) -> Option<ExitStatus> {
        let end = Instant::now() + within;
        std::iter::repeat_with(|| self.child.try_wait().unwrap())
            .take_while(|_| Instant::now() < end)
            .find_map(|status| {
                if status.is_none() {
                    std::thread::sleep(Duration::from_millis(20));
                }
                status
            })
    }

    /// Kill the process and reap it.
    pub(crate) fn kill(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }

    /// Ask the process to stop and wait for its clean exit.
    pub(crate) fn terminate(&mut self) -> ExitStatus {
        self.signal(libc::SIGTERM);
        self.exited(Duration::from_secs(20))
            .expect("the validator stops on SIGTERM")
    }

    /// One scrape of the metrics endpoint, or `None` while it is dark.
    pub(crate) fn sample(&self) -> Option<Sample> {
        let mut stream = TcpStream::connect_timeout(&self.metrics, Duration::from_secs(1)).ok()?;
        stream.set_read_timeout(Some(Duration::from_secs(2))).ok()?;
        write!(
            stream,
            "GET /metrics HTTP/1.1\r\nHost: {}\r\nConnection: close\r\n\r\n",
            self.metrics
        )
        .ok()?;
        let mut body = String::new();
        stream.read_to_string(&mut body).ok()?;
        body.starts_with("HTTP/1.1 200")
            .then(|| Sample::parse(&body))
    }

    /// Sample until `done` holds, or fail after `budget` with the last
    /// sample and the log tail.
    pub(crate) fn await_sample(
        &self,
        what: &str,
        budget: Duration,
        mut done: impl FnMut(Sample) -> bool,
    ) -> Sample {
        let end = Instant::now() + budget;
        let mut last = None;
        let found = std::iter::repeat_with(|| self.sample())
            .take_while(|_| Instant::now() < end)
            .find_map(|sample| {
                last = sample.or(last);
                let ready = sample.filter(|s| done(*s));
                if ready.is_none() {
                    std::thread::sleep(Duration::from_millis(50));
                }
                ready
            });
        found.unwrap_or_else(|| {
            panic!(
                "{what}: not reached within {}s (last {last:?})\n{}",
                budget.as_secs(),
                self.log_tail()
            )
        })
    }

    /// The whole log of this run, without terminal color codes.
    pub(crate) fn log_text(&self) -> String {
        let raw = std::fs::read_to_string(&self.log).unwrap_or_default();
        raw.split('\x1b')
            .enumerate()
            .map(|(i, part)| {
                if i == 0 {
                    return part;
                }
                part.find('m').map_or(part, |end| &part[end + 1..])
            })
            .collect()
    }

    fn log_tail(&self) -> String {
        let text = self.log_text();
        let lines: Vec<&str> = text.lines().collect();
        lines[lines.len().saturating_sub(40)..].join("\n")
    }
}

impl Drop for Node {
    fn drop(&mut self) {
        // A frozen child must continue before it can die; then it is reaped.
        if self.child.try_wait().ok().flatten().is_none() {
            self.signal(libc::SIGCONT);
        }
        self.kill();
    }
}

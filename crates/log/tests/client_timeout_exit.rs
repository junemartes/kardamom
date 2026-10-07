//! A client timeout ends the process in every Aeron context that this
//! crate builds. A child process holds a client against a media driver
//! with a 1 s client liveness timeout. The test stops the child with
//! `SIGSTOP` for longer than that timeout and then continues it. The
//! service interval check of the client then fails. The child must log
//! the line of the crate's error handler on stdout and exit with status
//! 1, so that the supervisor restarts it. One child holds both clients,
//! so both conductor threads call the handler at the same time.
//!
//! Gated on the `docker-e2e` feature and on Docker availability.

#![cfg(feature = "docker-e2e")]

use std::io::{BufRead, BufReader};
use std::path::Path;
use std::process::{Child, ChildStdout, Command, Output, Stdio};
use std::sync::mpsc;
use std::thread::JoinHandle;
use std::time::Duration;

use kardamom_log::aeron_live::AeronRuntime;
use kardamom_log::config::AeronConfig;
use kardamom_log::recorder::{ArchiveSession, connect_archive};
use kardamom_log::testing::{AeronTestCluster, require_docker};

/// The env var that names the context a child process holds.
const CONTEXT_ENV: &str = "KARDAMOM_CLIENT_TIMEOUT_CONTEXT";
/// The env var that gives the child the `aeron.dir` of the driver.
const DIR_ENV: &str = "KARDAMOM_CLIENT_TIMEOUT_AERON_DIR";
/// The line a child prints when its client is up.
const READY_LINE: &str = "client-timeout child ready";
/// The line of the crate's error handler.
const HANDLER_LINE: &str = "aeron client error; exiting";
/// The client liveness timeout of the driver.
const LIVENESS: Duration = Duration::from_secs(1);
/// How long the child stays stopped: three times the liveness timeout.
const STOPPED_FOR: Duration = Duration::from_secs(3);
/// How long the child may take to start its client.
const READY_BUDGET: Duration = Duration::from_secs(60);
/// How long the child may take to exit after it continues.
const EXIT_BUDGET: Duration = Duration::from_secs(30);
/// The name of the test function that runs as the child.
const CHILD_TEST: &str = "child_holds_one_client";

/// The Aeron contexts of this crate.
#[derive(Clone, Copy, Debug)]
enum Context {
    /// The client of `AeronRuntime`.
    Runtime,
    /// The client of an archive control session.
    Archive,
    /// Both clients in one process. Both time out at once, so both
    /// conductor threads call the handler at the same time.
    Both,
}

impl Context {
    fn name(self) -> &'static str {
        match self {
            Self::Runtime => "runtime",
            Self::Archive => "archive",
            Self::Both => "both",
        }
    }

    fn parse(name: &str) -> Self {
        match name {
            "runtime" => Self::Runtime,
            "archive" => Self::Archive,
            "both" => Self::Both,
            other => panic!("unknown context {other:?}"),
        }
    }

    fn runtime(dir: &Path) -> AeronRuntime {
        AeronRuntime::spawn_with_dir(dir).expect("runtime")
    }

    fn archive(dir: &Path) -> ArchiveSession {
        connect_archive(Some(dir), &AeronConfig::default()).expect("archive session")
    }

    /// Start the client of this context against `dir`, report it ready,
    /// and hold it until the process ends.
    fn hold(self, dir: &Path) -> ! {
        match self {
            Self::Runtime => Held::forever(Self::runtime(dir)),
            Self::Archive => Held::forever(Self::archive(dir)),
            Self::Both => Held::forever((Self::runtime(dir), Self::archive(dir))),
        }
    }

    /// Run a child that holds this context, stop it past the liveness
    /// timeout, continue it, and check that it exits through the
    /// handler of the crate.
    async fn assert_timeout_exits(self) {
        require_docker().await;
        let cluster = AeronTestCluster::single_node_with_client_liveness(LIVENESS)
            .await
            .expect("aeron container started");
        let child = ChildClient::spawn(self, cluster.aeron_dir_host(0));
        child.signal("STOP");
        std::thread::sleep(STOPPED_FOR);
        child.signal("CONT");
        let (out, stdout) = child.await_exit().unwrap_or_else(|| {
            panic!(
                "{self:?}: the child kept running {}s after the client timeout",
                EXIT_BUDGET.as_secs()
            )
        });
        let stderr = String::from_utf8_lossy(&out.stderr);
        assert_eq!(
            out.status.code(),
            Some(1),
            "{self:?}: stdout: {stdout} stderr: {stderr}"
        );
        assert!(
            stdout.contains(HANDLER_LINE),
            "{self:?}: no handler line in stdout: {stdout} stderr: {stderr}"
        );
    }
}

/// A client that a child process holds.
struct Held;

impl Held {
    /// Print the ready line and park the thread until the process ends.
    /// The client stays alive on this stack.
    fn forever<T>(_client: T) -> ! {
        println!("{READY_LINE}");
        loop {
            std::thread::park();
        }
    }
}

/// A child process of this test binary that runs [`CHILD_TEST`].
struct ChildClient {
    child: Child,
    context: Context,
    /// The stdout lines after the ready line, once the child ends.
    rest: JoinHandle<String>,
}

impl ChildClient {
    /// Start the child and wait for its ready line. A child that does
    /// not report ready is killed, and its stderr is in the panic.
    fn spawn(context: Context, dir: &Path) -> Self {
        let exe = std::env::current_exe().expect("test binary path");
        let mut child = Command::new(exe)
            .args([CHILD_TEST, "--exact", "--ignored", "--nocapture"])
            .env(CONTEXT_ENV, context.name())
            .env(DIR_ENV, dir)
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .expect("spawn the child");
        let stdout = child.stdout.take().expect("piped stdout");
        if let Some(rest) = Self::await_ready(stdout) {
            return Self {
                child,
                context,
                rest,
            };
        }
        let _ = child.kill();
        let out = child.wait_with_output().expect("wait for the child");
        panic!(
            "{context:?}: the child never reported its client ({}); stderr: {}",
            out.status,
            String::from_utf8_lossy(&out.stderr)
        );
    }

    /// Wait for the ready line on `stdout`. A reader thread keeps the
    /// pipe drained until the child ends, and its handle gives the lines
    /// after the ready line. `None` when no ready line comes.
    fn await_ready(stdout: ChildStdout) -> Option<JoinHandle<String>> {
        let (tx, rx) = mpsc::channel();
        let rest = std::thread::spawn(move || {
            let mut lines = BufReader::new(stdout).lines().map_while(Result::ok);
            let ready = lines.any(|line| line.contains(READY_LINE));
            let _ = tx.send(ready);
            lines.collect::<Vec<_>>().join("\n")
        });
        rx.recv_timeout(READY_BUDGET)
            .unwrap_or(false)
            .then_some(rest)
    }

    /// Send `SIGNAL` to the child with `kill`.
    fn signal(&self, signal: &str) {
        let status = Command::new("kill")
            .arg(format!("-{signal}"))
            .arg(self.child.id().to_string())
            .status()
            .expect("run kill");
        assert!(
            status.success(),
            "{:?}: kill -{signal} failed",
            self.context
        );
    }

    /// The output of the child once it exits, with its stdout after the
    /// ready line, or `None` when it still runs after [`EXIT_BUDGET`]. A
    /// child that still runs is killed.
    fn await_exit(self) -> Option<(Output, String)> {
        let pid = self.child.id().to_string();
        let (tx, rx) = mpsc::channel();
        std::thread::spawn(move || tx.send(self.child.wait_with_output()));
        let Ok(out) = rx.recv_timeout(EXIT_BUDGET) else {
            let _ = Command::new("kill").args(["-KILL", &pid]).status();
            return None;
        };
        let rest = self.rest.join().expect("stdout reader");
        Some((out.expect("wait for the child"), rest))
    }
}

/// The child side: hold the context that [`CONTEXT_ENV`] names. Without
/// that env var, the function is a no-op, so a plain `--ignored` run
/// passes it.
#[test]
#[ignore = "runs as the child of the client-timeout tests"]
fn child_holds_one_client() {
    let Ok(name) = std::env::var(CONTEXT_ENV) else {
        return;
    };
    let dir = std::env::var(DIR_ENV).expect("aeron dir env");
    // The same subscriber as every service binary: `fmt` on stdout.
    tracing_subscriber::fmt().init();
    Context::parse(&name).hold(Path::new(&dir));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires Docker; run with `cargo test -p kardamom-log --features docker-e2e --test client_timeout_exit -- --ignored`"]
async fn a_client_timeout_in_the_runtime_context_exits() {
    Context::Runtime.assert_timeout_exits().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires Docker; run with `cargo test -p kardamom-log --features docker-e2e --test client_timeout_exit -- --ignored`"]
async fn a_client_timeout_in_the_archive_context_exits() {
    Context::Archive.assert_timeout_exits().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires Docker; run with `cargo test -p kardamom-log --features docker-e2e --test client_timeout_exit -- --ignored`"]
async fn a_client_timeout_in_both_contexts_at_once_exits() {
    Context::Both.assert_timeout_exits().await;
}

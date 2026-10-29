//! Test helpers for a service's `/metrics` smoke test, and for reserving
//! ports for other test-only listeners.
//!
//! Feature `test-support`. A service crate's `tests/metrics_endpoint.rs`
//! calls [`free_port`] for an address to pass to [`crate::init`], then
//! [`scrape`] to poll that address until the Prometheus exporter answers.

use std::fs::{File, TryLockError};
use std::net::{SocketAddr, TcpListener, UdpSocket};
use std::ops::ControlFlow;
use std::path::PathBuf;
use std::sync::OnceLock;
use std::sync::atomic::{AtomicU32, Ordering};

/// A free loopback TCP port, bound and released, so the caller can pass the
/// address to [`crate::init`] or to a child process before anything listens
/// on it. See [`TestPorts`] for why the port is not an ephemeral one.
///
/// # Panics
///
/// Panics if no port of the test range binds.
#[must_use]
pub fn free_port() -> SocketAddr {
    TestPorts::shared().claim(|port| TcpListener::bind(("127.0.0.1", port))?.local_addr())
}

/// A free loopback UDP port, bound and released. See [`free_port`].
///
/// # Panics
///
/// Panics if no port of the test range binds.
#[must_use]
pub fn free_udp_port() -> SocketAddr {
    TestPorts::shared().claim(|port| UdpSocket::bind(("127.0.0.1", port))?.local_addr())
}

/// The ports the test helpers hand out: 20000 to 31999, below Linux's
/// default ephemeral range (32768 to 60999).
///
/// A child binds its port after the helper returns. An ephemeral port
/// can be taken in that window by an outgoing connection or port-0 socket.
/// A port in this range is taken only by an explicit bind. Each process holds an
/// exclusive file lock for every 250-port range it uses. Other test
/// processes cannot select those ports while a child is starting. The OS
/// releases the locks when the test process exits, including after a crash.
struct TestPorts {
    next: AtomicU32,
    directory: PathBuf,
    leases: [OnceLock<Option<File>>; 48],
}

impl TestPorts {
    const FIRST: u32 = 20_000;
    const COUNT: u32 = 12_000;
    const RANGE_SIZE: u32 = 250;

    fn new(seed: u32, directory: PathBuf) -> Self {
        std::fs::create_dir_all(&directory).expect("create the shared test-port lock directory");
        Self {
            next: AtomicU32::new(seed % Self::COUNT),
            directory,
            leases: std::array::from_fn(|_| OnceLock::new()),
        }
    }

    fn shared() -> &'static Self {
        static PORTS: OnceLock<TestPorts> = OnceLock::new();
        PORTS.get_or_init(|| {
            Self::new(
                std::process::id(),
                std::env::temp_dir().join("kardamom-test-port-locks"),
            )
        })
    }

    fn reserve_range(&self, range: usize) -> Option<File> {
        // Lock files stay in place so every process locks the same inode.
        let file = File::options()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(self.directory.join(format!("{range}.lock")))
            .expect("open a test-port range lock");
        match file.try_lock() {
            Ok(()) => Some(file),
            Err(TryLockError::WouldBlock) => None,
            Err(TryLockError::Error(error)) => panic!("lock a test-port range: {error}"),
        }
    }

    fn owns_range(&self, port: u16) -> bool {
        let offset = u32::from(port)
            .checked_sub(Self::FIRST)
            .expect("the cursor yields a test port");
        let range = usize::try_from(offset / Self::RANGE_SIZE).expect("range index fits usize");
        self.leases[range]
            .get_or_init(|| self.reserve_range(range))
            .is_some()
    }

    /// The next port of the range, in cursor order, wrapping at the end.
    fn next_port(&self) -> u16 {
        let offset = self.next.fetch_add(1, Ordering::Relaxed) % Self::COUNT;
        u16::try_from(Self::FIRST + offset).expect("the test range ends below u16::MAX")
    }

    /// The first port, from the cursor on, that `bind` accepts.
    fn claim(&self, bind: impl Fn(u16) -> std::io::Result<SocketAddr>) -> SocketAddr {
        (0..Self::COUNT)
            .map(|_| self.next_port())
            .filter(|port| self.owns_range(*port))
            .find_map(|port| bind(port).ok())
            .expect("a free loopback port in 20000-31999")
    }
}

/// Poll `url` until it answers with a success status, then return the
/// response body.
///
/// The exporter's listener binds synchronously, but the exporter starts
/// serving only once its future is first polled. The retry absorbs that
/// startup race.
///
/// # Panics
///
/// Panics if `url` does not answer with a success status within the retry
/// budget (40 attempts, 50 ms apart).
pub async fn scrape(url: &str) -> String {
    for _ in 1..40 {
        if let ControlFlow::Break(body) = scrape_attempt(url).await {
            return body;
        }
    }
    match try_scrape(url).await {
        ControlFlow::Break(body) => body,
        ControlFlow::Continue(()) => panic!("exporter not ready at {url}"),
    }
}

/// One [`scrape`] retry attempt: try once, then (if not ready yet) sleep
/// 50 ms before the caller retries.
async fn scrape_attempt(url: &str) -> ControlFlow<String> {
    let outcome = try_scrape(url).await;
    if outcome.is_continue() {
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
    outcome
}

/// One scrape attempt: a successful response breaks out with its body, a
/// connection failure or a non-success status asks the caller to retry.
async fn try_scrape(url: &str) -> ControlFlow<String> {
    match reqwest::get(url).await {
        Ok(r) if r.status().is_success() => {
            ControlFlow::Break(r.text().await.expect("read the response body"))
        }
        _ => ControlFlow::Continue(()),
    }
}

/// Poll `f` every `interval` until it returns `Some(v)`, or until
/// `timeout` passes. On timeout, fails with `what` naming the wait.
///
/// # Errors
///
/// Returns an error if `f` itself errors, or if `timeout` passes before
/// `f` returns `Some(v)`.
pub async fn poll_until<T>(
    what: &str,
    timeout: std::time::Duration,
    interval: std::time::Duration,
    mut f: impl AsyncFnMut() -> anyhow::Result<Option<T>>,
) -> anyhow::Result<T> {
    let deadline = std::time::Instant::now() + timeout;
    loop {
        if let ControlFlow::Break(v) =
            poll_until_step(f().await?, deadline, timeout, what, interval).await?
        {
            return Ok(v);
        }
    }
}

/// One [`poll_until`] step: apply [`poll_outcome`], then (if it says
/// retry) sleep `interval` before the caller polls again.
async fn poll_until_step<T>(
    item: Option<T>,
    deadline: std::time::Instant,
    timeout: std::time::Duration,
    what: &str,
    interval: std::time::Duration,
) -> anyhow::Result<ControlFlow<T>> {
    let outcome = poll_outcome(item, deadline, timeout, what)?;
    if outcome.is_continue() {
        tokio::time::sleep(interval).await;
    }
    Ok(outcome)
}

/// Blocking twin of [`poll_until`], for code that runs off the tokio
/// runtime (a spawned OS thread, or a sync bring-up path).
///
/// # Errors
///
/// Returns an error if `f` itself errors, or if `timeout` passes before
/// `f` returns `Some(v)`.
pub fn poll_sync<T>(
    what: &str,
    timeout: std::time::Duration,
    interval: std::time::Duration,
    mut f: impl FnMut() -> anyhow::Result<Option<T>>,
) -> anyhow::Result<T> {
    let deadline = std::time::Instant::now() + timeout;
    loop {
        if let ControlFlow::Break(v) = poll_sync_step(f()?, deadline, timeout, what, interval)? {
            return Ok(v);
        }
    }
}

/// One [`poll_sync`] step: apply [`poll_outcome`], then (if it says
/// retry) sleep `interval` before the caller polls again.
fn poll_sync_step<T>(
    item: Option<T>,
    deadline: std::time::Instant,
    timeout: std::time::Duration,
    what: &str,
    interval: std::time::Duration,
) -> anyhow::Result<ControlFlow<T>> {
    let outcome = poll_outcome(item, deadline, timeout, what)?;
    if outcome.is_continue() {
        std::thread::sleep(interval);
    }
    Ok(outcome)
}

/// Shared dispatch behind [`poll_until`] and [`poll_sync`]: `Some(v)`
/// breaks with it, `None` past `deadline` fails naming `what` and
/// `timeout`, and `None` before `deadline` asks the caller to retry.
fn poll_outcome<T>(
    item: Option<T>,
    deadline: std::time::Instant,
    timeout: std::time::Duration,
    what: &str,
) -> anyhow::Result<ControlFlow<T>> {
    match item {
        Some(v) => Ok(ControlFlow::Break(v)),
        None if std::time::Instant::now() >= deadline => {
            anyhow::bail!("timed out ({timeout:?}) waiting for {what}")
        }
        None => Ok(ControlFlow::Continue(())),
    }
}

#[cfg(test)]
#[path = "testkit_port_tests.rs"]
mod port_tests;

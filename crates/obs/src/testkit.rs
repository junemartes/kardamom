//! Test helpers for a service's `/metrics` smoke test, and for reserving
//! ports for other test-only listeners.
//!
//! Feature `test-support`. A service crate's `tests/metrics_endpoint.rs`
//! calls [`free_port`] for an address to pass to [`crate::init`], then
//! [`scrape`] to poll that address until the Prometheus exporter answers.

use std::net::{SocketAddr, TcpListener, UdpSocket};
use std::ops::ControlFlow;
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
/// A port a helper releases is bound again later by a child process. An
/// ephemeral port could be taken in that window by any outgoing connection
/// or port-0 socket on the host, and the e2e stack makes many: an executor
/// failed with "bind nonce query address: Address already in use". A port
/// in this range is taken only by an explicit bind. One cursor per process
/// also keeps two calls from handing out the same port; the start depends
/// on the process id, so two test binaries at once start apart.
struct TestPorts {
    next: AtomicU32,
}

impl TestPorts {
    const FIRST: u32 = 20_000;
    const COUNT: u32 = 12_000;

    fn shared() -> &'static Self {
        static PORTS: OnceLock<TestPorts> = OnceLock::new();
        PORTS.get_or_init(|| Self {
            next: AtomicU32::new(std::process::id() % Self::COUNT),
        })
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
mod port_tests {
    use super::{TestPorts, free_port, free_udp_port};

    #[test]
    fn ports_come_from_below_the_ephemeral_range_and_do_not_repeat() {
        let ports: Vec<u16> = (0..64).map(|_| free_port().port()).collect();
        assert!(ports.iter().all(|p| (20_000..32_000).contains(p)));
        let mut unique = ports.clone();
        unique.sort_unstable();
        unique.dedup();
        assert_eq!(unique.len(), ports.len());
        assert!((20_000..32_000).contains(&free_udp_port().port()));
    }

    #[test]
    fn a_port_that_does_not_bind_is_skipped() {
        let tried = std::cell::RefCell::new(Vec::new());
        let got = TestPorts::shared().claim(|port| {
            tried.borrow_mut().push(port);
            if tried.borrow().len() < 3 {
                return Err(std::io::Error::from(std::io::ErrorKind::AddrInUse));
            }
            Ok(std::net::SocketAddr::from(([127, 0, 0, 1], port)))
        });
        let tried = tried.into_inner();
        assert_eq!(tried.len(), 3);
        assert_eq!(got.port(), tried[2]);
        assert!(tried[..2].iter().all(|p| *p != got.port()));
    }
}

//! Test helpers for a service's `/metrics` smoke test, and for reserving
//! ephemeral ports for other test-only listeners.
//!
//! Feature `test-support`. A service crate's `tests/metrics_endpoint.rs`
//! calls [`free_port`] for an address to pass to [`crate::init`], then
//! [`scrape`] to poll that address until the Prometheus exporter answers.

use std::net::{SocketAddr, TcpListener, UdpSocket};
use std::ops::ControlFlow;

/// Bind an ephemeral TCP port, then release it, so the caller can pass the
/// address to [`crate::init`] before anything listens on it.
///
/// # Panics
///
/// Panics if the OS refuses an ephemeral bind.
#[must_use]
pub fn free_port() -> SocketAddr {
    TcpListener::bind("127.0.0.1:0")
        .expect("bind an ephemeral port")
        .local_addr()
        .expect("read the bound local address")
}

/// Bind an ephemeral UDP port, then release it. Racy (another process can
/// grab it before the caller rebinds), so a caller that needs the port to
/// stay free retries on a bind failure.
///
/// # Panics
///
/// Panics if the OS refuses an ephemeral bind.
#[must_use]
pub fn free_udp_port() -> SocketAddr {
    UdpSocket::bind("127.0.0.1:0")
        .expect("bind an ephemeral UDP port")
        .local_addr()
        .expect("read the bound local address")
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

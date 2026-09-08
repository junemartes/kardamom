//! Prometheus text-format scraping, plus bounded polling.
//!
//! The scenario drivers observe the pipeline only through each service's
//! `/metrics` endpoint (and the ingress JSON-RPC). Polls always have a
//! deadline. This code never uses a fixed sleep, following the repo's
//! test conventions.

use std::io::{Read, Write};
use std::net::SocketAddr;
use std::time::{Duration, Instant};

use anyhow::{Context, Result};

/// One scraped /metrics body.
pub struct Scrape(String);

impl Scrape {
    /// Sum every sample of `name` across label sets. Returns `None` when
    /// the metric is absent, which differs from a genuine 0 sample.
    #[must_use]
    pub fn value(&self, name: &str) -> Option<f64> {
        self.0
            .lines()
            .filter(|line| !line.starts_with('#') && line.starts_with(name))
            .filter_map(|line| {
                // This is an exact-name match. The next character must end
                // the metric name (a label block or a sample separator), so
                // `foo` never matches `foo_total`.
                let rest = &line[name.len()..];
                if !(rest.starts_with('{') || rest.starts_with(' ')) {
                    return None;
                }
                line.rsplit(' ').next().and_then(|v| v.parse::<f64>().ok())
            })
            .reduce(|a, b| a + b)
    }
}

/// Scrape `http://addr/metrics` with a plain HTTP/1.0 GET. This needs no
/// client library: the exporter answers with a non-chunked HTTP/1.0 body
/// and closes the connection.
///
/// # Errors
/// Returns an error when the connection fails, when a read or write times
/// out, or when the response is not valid UTF-8.
fn scrape_blocking(addr: SocketAddr, timeout: Duration) -> Result<Scrape> {
    let mut stream = std::net::TcpStream::connect_timeout(&addr, timeout)
        .with_context(|| format!("connect {addr}"))?;
    stream.set_read_timeout(Some(timeout))?;
    stream.set_write_timeout(Some(timeout))?;
    stream
        .write_all(format!("GET /metrics HTTP/1.0\r\nHost: {addr}\r\n\r\n").as_bytes())
        .context("send scrape request")?;
    let mut buf = String::new();
    stream
        .read_to_string(&mut buf)
        .context("read scrape response")?;
    let body = buf
        .split_once("\r\n\r\n")
        .map(|(_, b)| b.to_string())
        .unwrap_or(buf);
    Ok(Scrape(body))
}

/// Async wrapper for [`scrape_blocking`].
///
/// # Errors
/// Returns an error under the same conditions as [`scrape_blocking`], or
/// when the blocking task panics or is cancelled.
pub async fn scrape(addr: SocketAddr) -> Result<Scrape> {
    tokio::task::spawn_blocking(move || scrape_blocking(addr, Duration::from_secs(5)))
        .await
        .context("scrape task join")?
}

/// Poll `f` every `interval` until it returns `Some(v)`, or until
/// `timeout` passes. On timeout, this fails with `what` as the message.
/// Callers should put context in `what`.
///
/// # Errors
/// Returns an error when `f` itself errors, or when `timeout` passes
/// before `f` returns `Some(v)`.
pub async fn poll_until<T>(
    what: &str,
    timeout: Duration,
    interval: Duration,
    mut f: impl AsyncFnMut() -> Result<Option<T>>,
) -> Result<T> {
    let deadline = Instant::now() + timeout;
    loop {
        if let Some(v) = f().await? {
            return Ok(v);
        }
        if Instant::now() >= deadline {
            anyhow::bail!("timed out ({timeout:?}) waiting for {what}");
        }
        tokio::time::sleep(interval).await;
    }
}

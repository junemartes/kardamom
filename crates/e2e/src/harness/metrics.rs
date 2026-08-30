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
pub struct Scrape(pub String);

impl Scrape {
    /// Sum every sample of `name` across label sets. Returns `None` when
    /// the metric is absent, which differs from a genuine 0 sample.
    pub fn value(&self, name: &str) -> Option<f64> {
        let mut found = false;
        let mut sum = 0.0;
        for line in self.0.lines() {
            if line.starts_with('#') || !line.starts_with(name) {
                continue;
            }
            // This is an exact-name match. The next character must end the
            // metric name (a label block or a sample separator), so `foo`
            // never matches `foo_total`.
            let rest = &line[name.len()..];
            if !(rest.starts_with('{') || rest.starts_with(' ')) {
                continue;
            }
            if let Some(v) = line.rsplit(' ').next().and_then(|v| v.parse::<f64>().ok()) {
                found = true;
                sum += v;
            }
        }
        found.then_some(sum)
    }
}

/// Scrape `http://addr/metrics` with a plain HTTP/1.0 GET. This needs no
/// client library: the exporter answers with a non-chunked HTTP/1.0 body
/// and closes the connection.
pub fn scrape_blocking(addr: SocketAddr, timeout: Duration) -> Result<Scrape> {
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
pub async fn scrape(addr: SocketAddr) -> Result<Scrape> {
    tokio::task::spawn_blocking(move || scrape_blocking(addr, Duration::from_secs(5)))
        .await
        .context("scrape task join")?
}

/// Poll `f` every `interval` until it returns `Some(v)`, or until
/// `timeout` passes. On timeout, this fails with `what` as the message.
/// Callers should put context in `what`.
pub async fn poll_until<T, F, Fut>(
    what: &str,
    timeout: Duration,
    interval: Duration,
    mut f: F,
) -> Result<T>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = Result<Option<T>>>,
{
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

//! The parts of a starved Redis group: one Sentinel host name with more
//! than one address, a primary that stalls on a cycle, and the cache
//! metrics of one test.

use std::net::SocketAddr;
use std::time::Duration;

use metrics_exporter_prometheus::{PrometheusHandle, PrometheusRecorder};
use testcontainers::{ContainerAsync, GenericImage};
use tokio::net::{TcpListener, TcpStream};
use tokio::task::JoinHandle;

use super::support::Fixture;

/// A host name that the system resolver expands to `::1` and
/// `127.0.0.1`.
const NAME: &str = "sentinel.localhost";

/// One host name that resolves to more than one address. Each address
/// forwards to a different Sentinel, as a service name with one DNS
/// record for each Sentinel does in a deployment.
pub(super) struct SentinelName {
    port: u16,
    tasks: Vec<JoinHandle<()>>,
}

impl Drop for SentinelName {
    fn drop(&mut self) {
        self.tasks.iter().for_each(JoinHandle::abort);
    }
}

impl SentinelName {
    /// Forward the addresses of [`NAME`], in resolver order, to the
    /// Sentinels in `targets`.
    pub(super) async fn forward(targets: &[&ContainerAsync<GenericImage>]) -> Self {
        let port = {
            let probe = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
            probe.local_addr().unwrap().port()
        };
        let addrs: Vec<SocketAddr> = tokio::net::lookup_host((NAME, port))
            .await
            .unwrap()
            .collect();
        assert!(
            addrs.len() >= targets.len(),
            "{NAME} must resolve to one address for each Sentinel: {addrs:?}"
        );
        let mut tasks = Vec::new();
        for (addr, target) in addrs.into_iter().zip(targets) {
            let listener = TcpListener::bind(addr).await.unwrap();
            let to = target.get_host_port_ipv4(26379).await.unwrap();
            tasks.push(tokio::spawn(Self::serve(listener, to)));
        }
        Self { port, tasks }
    }

    /// The one Sentinel URL that a reader gets.
    pub(super) fn url(&self) -> String {
        format!("redis://{NAME}:{}", self.port)
    }

    async fn serve(listener: TcpListener, to: u16) {
        loop {
            let (inbound, _) = listener.accept().await.unwrap();
            tokio::spawn(Self::pipe(inbound, to));
        }
    }

    async fn pipe(mut inbound: TcpStream, to: u16) {
        let Ok(mut outbound) = TcpStream::connect(("127.0.0.1", to)).await else {
            return;
        };
        let _ = tokio::io::copy_bidirectional(&mut inbound, &mut outbound).await;
    }
}

/// The primary stalls on a cycle until the handle aborts: `paused` in
/// SIGSTOP, then `running`. Each pause stays below the Sentinel
/// down-after bound, so the Sentinels never fail over.
pub(super) struct Stalls {
    task: JoinHandle<()>,
}

impl Stalls {
    pub(super) fn start(primary: String, paused: Duration, running: Duration) -> Self {
        let task = tokio::spawn(async move {
            loop {
                Fixture::send(&primary, "STOP").await;
                tokio::time::sleep(paused).await;
                Fixture::send(&primary, "CONT").await;
                tokio::time::sleep(running).await;
            }
        });
        Self { task }
    }
}

impl Drop for Stalls {
    fn drop(&mut self) {
        self.task.abort();
    }
}

/// The two cache counters that a reconnect loop drives up.
#[derive(Debug, Clone, Copy, PartialEq)]
pub(super) struct Churn {
    pub(super) reconnects: f64,
    pub(super) skipped: f64,
}

impl Churn {
    pub(super) fn of(handle: &PrometheusHandle) -> Self {
        let text = handle.render();
        Self {
            reconnects: Self::value(&text, "kardamom_cache_reconnects_total "),
            skipped: Self::value(&text, "kardamom_cache_degraded_total{reason=\"reconnecting\"} "),
        }
    }

    fn value(text: &str, prefix: &str) -> f64 {
        text.lines()
            .find_map(|line| line.strip_prefix(prefix))
            .map_or(0.0, |value| value.trim().parse().unwrap())
    }

    pub(super) fn since(self, earlier: Self) -> Self {
        Self {
            reconnects: self.reconnects - earlier.reconnects,
            skipped: self.skipped - earlier.skipped,
        }
    }
}

/// A recorder for the metrics of one current-thread test.
pub(super) fn recorder() -> PrometheusRecorder {
    metrics_exporter_prometheus::PrometheusBuilder::new().build_recorder()
}

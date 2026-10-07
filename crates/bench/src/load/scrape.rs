//! This module scrapes cluster metrics for the load harness.
//!
//! Each target names one exporter: the node container, and the URL of
//! its `/metrics`. A direct read of that URL comes first. When it fails,
//! `docker exec <node> curl 127.0.0.1:<port>/metrics` reads the same
//! exporter from inside the node. A snapshot counts each such fallback,
//! because a runner-wide stall of `docker exec` hides every exec-based
//! read at once. With `via_docker`, every read goes through the node and
//! no read counts as a fallback.

use std::collections::BTreeSet;
use std::net::Ipv4Addr;
use std::time::Duration;

use tokio::process::Command;

/// The default metrics port of the executor (`--metrics-addr`).
const EXECUTOR_METRICS_PORT: u16 = 9004;
/// The default metrics port of the ingress.
const INGRESS_METRICS_PORT: u16 = 9006;
/// The default metrics port of the lane-0 sequencer replica.
const SEQUENCER_METRICS_PORT: u16 = 9001;

/// The time budget of one read, direct or through the node.
const READ_TIMEOUT: Duration = Duration::from_secs(5);

// The exact Prometheus metric names each service exposes.
const M_EXECUTOR_BLOCK: &str = "kardamom_executor_block_number";
// The clustered sealer has no Prometheus endpoint of its own. Each executor
// re-exports the sealer's boundary stream as it decodes cluster egress, so
// the code reads the kardamom_sealer_* series from the executor endpoints.
const M_SEALER_BLOCK: &str = "kardamom_sealer_block_number";
const M_SEALER_BOUNDARIES: &str = "kardamom_sealer_boundaries_emitted_total";
const M_INGRESS_RECEIVED: &str = "kardamom_ingress_tx_received_total";
const M_INGRESS_ACCEPTED: &str = "kardamom_ingress_tx_accepted_total";
const M_INGRESS_REJECTED: &str = "kardamom_ingress_tx_rejected_total";
const M_INGRESS_QUEUE: &str = "kardamom_ingress_queue_depth";
const M_SEQ_DROPPED: &str = "kardamom_sequencer_tx_dropped_past_total";
const M_SEQ_EVICTIONS: &str = "kardamom_sequencer_pending_evictions_total";
const M_SEQ_BACKPRESSURE: &str = "kardamom_sequencer_backpressure_total";
const M_SERVICE_UP: &str = "kardamom_service_up";

/// A point-in-time read of the cluster's pipeline metrics.
#[derive(Debug, Default, Clone)]
pub(crate) struct MetricsSnapshot {
    /// `(node, executor_block_number)` for each scraped executor node.
    pub executor_blocks: Vec<(String, Option<u64>)>,
    /// The sealer's last sealed block number. This is the most advanced
    /// executor observation of the cluster's boundary stream.
    pub sealer_block: Option<u64>,
    /// The sealer's block-boundaries counter, for liveness during chaos,
    /// observed the same way.
    pub sealer_boundaries: Option<u64>,
    /// Ingress: the total submissions received.
    pub ingress_received: Option<u64>,
    /// Ingress: the submissions that returned a receipt.
    pub ingress_accepted: Option<u64>,
    /// Ingress: the submissions rejected, summed over all `reason` labels.
    pub ingress_rejected: Option<u64>,
    /// Ingress: the current pending-transaction queue depth.
    pub ingress_queue_depth: Option<u64>,
    /// Sequencer: transactions dropped for a past nonce, summed over
    /// partitions.
    pub seq_dropped_past: Option<u64>,
    /// Sequencer: pending-buffer evictions, summed over partitions.
    pub seq_evictions: Option<u64>,
    /// Sequencer: backpressure events, summed over partitions.
    pub seq_backpressure: Option<u64>,
    /// `(label, up)` for each service a scrape was attempted for.
    /// `Some(1)` means the exporter reports up. `Some(0)` means the
    /// exporter reports down, or the scrape itself failed: an
    /// unreachable service counts as down, so the end-of-run liveness
    /// gate does not pass just because an exporter vanished. `None`
    /// means the scrape succeeded but the metric was absent. A service
    /// missing from this list was never scraped, because it was not in
    /// the scrape set.
    pub service_up: Vec<(String, Option<u64>)>,
    /// The reads of this snapshot whose direct read failed and that fell
    /// back to `docker exec`.
    pub fallbacks: u64,
}

/// One exporter the load reads: the node container, for the
/// `docker exec` fallback, and the URL of its `/metrics`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MetricsTarget {
    /// The node container name.
    pub(crate) name: String,
    /// The URL of the direct read.
    url: reqwest::Url,
    /// The port of the exporter, for the read inside the node.
    port: u16,
}

impl MetricsTarget {
    /// The exporter on `port` of `host`, in the node container `name`.
    ///
    /// # Errors
    ///
    /// Returns an error if `host` and `port` do not form a valid URL.
    fn new(name: &str, host: &str, port: u16) -> anyhow::Result<Self> {
        let url = reqwest::Url::parse(&format!("http://{host}:{port}/metrics"))?;
        Ok(Self {
            name: name.to_string(),
            url,
            port,
        })
    }

    /// The exporter on `port` of the node container `name`, with the
    /// container name as the host of the direct read.
    ///
    /// # Errors
    ///
    /// Returns an error if `name` is not a valid host name.
    fn named(name: &str, port: u16) -> anyhow::Result<Self> {
        Self::new(name, name, port)
    }

    /// The exporter on `port` of the bridge address `ip`, in the node
    /// container `name`.
    ///
    /// # Panics
    ///
    /// Never in practice: an IPv4 address and a port always form a valid
    /// URL.
    #[must_use]
    pub fn at(name: &str, ip: Ipv4Addr, port: u16) -> Self {
        Self::new(name, &ip.to_string(), port).expect("an IPv4 address and a port form a URL")
    }

    /// The URL that the node reads inside its own network: the same
    /// port on loopback.
    fn loopback_url(&self) -> String {
        format!("http://127.0.0.1:{}/metrics", self.port)
    }
}

/// The exporters one load reads.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MetricsTargets {
    /// The executor exporters.
    pub executors: Vec<MetricsTarget>,
    /// The exporter of the ingress the load submits through.
    pub ingress: MetricsTarget,
    /// The lane-0 sequencer exporters.
    pub sequencers: Vec<MetricsTarget>,
}

impl MetricsTargets {
    /// The exporters on the default ports, with the container names as
    /// the hosts of the direct reads.
    ///
    /// # Errors
    ///
    /// Returns an error if a name is not a valid host name.
    pub fn named(
        executors: &[String],
        ingress: &str,
        sequencers: &[String],
    ) -> anyhow::Result<Self> {
        let named = |names: &[String], port| {
            names
                .iter()
                .map(|n| MetricsTarget::named(n, port))
                .collect::<anyhow::Result<Vec<_>>>()
        };
        Ok(Self {
            executors: named(executors, EXECUTOR_METRICS_PORT)?,
            ingress: MetricsTarget::named(ingress, INGRESS_METRICS_PORT)?,
            sequencers: named(sequencers, SEQUENCER_METRICS_PORT)?,
        })
    }
}

/// The result of one read: the body, and whether the read fell back to
/// `docker exec`.
struct Read {
    body: Option<String>,
    fell_back: bool,
}

/// The services to scrape, and the exporter targets of each.
#[derive(Debug, Clone)]
pub(crate) struct Scraper {
    /// When true, read every target through `docker exec` only.
    via_docker: bool,
    /// The lowercased service names to scrape: any of executor, ingress,
    /// or sequencer. The sealer values ride along with the executor
    /// scrape, since the clustered sealer has no endpoint of its own.
    scrape: BTreeSet<String>,
    targets: MetricsTargets,
    http: reqwest::Client,
    /// The program of the fallback read: `docker`. It runs as
    /// `<program> exec <node> curl ... <loopback url>`.
    exec_program: &'static str,
}

/// The inputs of [`Scraper::new`].
pub(crate) struct ScrapeSet {
    pub via_docker: bool,
    pub scrape: BTreeSet<String>,
    pub targets: MetricsTargets,
}

impl Scraper {
    /// A scraper over `set`, with the `docker exec` fallback.
    ///
    /// # Errors
    ///
    /// Returns an error if the HTTP client cannot be built.
    pub(crate) fn new(set: ScrapeSet) -> anyhow::Result<Self> {
        Self::with_exec_program(set, "docker")
    }

    fn with_exec_program(set: ScrapeSet, exec_program: &'static str) -> anyhow::Result<Self> {
        let http = reqwest::Client::builder().timeout(READ_TIMEOUT).build()?;
        Ok(Self {
            via_docker: set.via_docker,
            scrape: set.scrape,
            targets: set.targets,
            http,
            exec_program,
        })
    }

    fn wants(&self, svc: &str) -> bool {
        self.scrape.contains(svc)
    }

    /// Read one target's `/metrics` body into `snap`'s fallback count.
    /// Returns `None` if no read answers.
    async fn fetch(&self, snap: &mut MetricsSnapshot, target: &MetricsTarget) -> Option<String> {
        let read = self.read(target).await;
        snap.fallbacks = snap.fallbacks.saturating_add(u64::from(read.fell_back));
        read.body
    }

    /// Read `target` directly, then through the node when the direct
    /// read fails. With `via_docker`, read through the node only.
    async fn read(&self, target: &MetricsTarget) -> Read {
        if self.via_docker {
            return Read {
                body: self.read_via_node(target).await,
                fell_back: false,
            };
        }
        match self.read_direct(target).await {
            Some(body) => Read {
                body: Some(body),
                fell_back: false,
            },
            None => Read {
                body: self.read_via_node(target).await,
                fell_back: true,
            },
        }
    }

    async fn read_direct(&self, target: &MetricsTarget) -> Option<String> {
        self.http
            .get(target.url.clone())
            .send()
            .await
            .ok()?
            .error_for_status()
            .ok()?
            .text()
            .await
            .ok()
    }

    async fn read_via_node(&self, target: &MetricsTarget) -> Option<String> {
        let url = target.loopback_url();
        let out = Command::new(self.exec_program)
            .args([
                "exec",
                &target.name,
                "curl",
                "-fsS",
                "--max-time",
                "5",
                &url,
            ])
            .output()
            .await
            .ok()?;
        out.status
            .success()
            .then(|| String::from_utf8_lossy(&out.stdout).into_owned())
    }

    /// Take a full snapshot of the configured services.
    pub(crate) async fn snapshot(&self) -> MetricsSnapshot {
        let mut snap = MetricsSnapshot::default();
        if self.wants("executor") {
            self.scrape_executors(&mut snap).await;
        }
        if self.wants("ingress") {
            self.scrape_ingress(&mut snap).await;
        }
        if self.wants("sequencer") {
            self.scrape_sequencers(&mut snap).await;
        }
        snap
    }

    /// Scrape every executor node into `snap`: block number, the
    /// re-exported sealer boundary stream, and liveness.
    async fn scrape_executors(&self, snap: &mut MetricsSnapshot) {
        for target in &self.targets.executors {
            self.scrape_one_executor(snap, target).await;
        }
    }

    /// Scrape one executor node into `snap`: its block number, and the
    /// most advanced sealer boundary observation seen across executors
    /// so far (a single stalled executor should not hide sealer
    /// progress), plus liveness.
    async fn scrape_one_executor(&self, snap: &mut MetricsSnapshot, target: &MetricsTarget) {
        let body = self.fetch(snap, target).await;
        let node = &target.name;
        let g = |m: &str| {
            body.as_deref()
                .and_then(|b| sum_metric(b, m))
                .map(|v| gauge_u64(v, m))
        };
        snap.executor_blocks
            .push((node.clone(), g(M_EXECUTOR_BLOCK)));
        // This is sealer output, re-exported by this executor from
        // cluster egress.
        snap.sealer_block = snap.sealer_block.max(g(M_SEALER_BLOCK));
        snap.sealer_boundaries = snap.sealer_boundaries.max(g(M_SEALER_BOUNDARIES));
        push_up(snap, format!("executor@{node}"), body.as_deref());
    }

    /// Scrape ingress into `snap`: submission counts, queue depth, and
    /// liveness.
    async fn scrape_ingress(&self, snap: &mut MetricsSnapshot) {
        let body = self.fetch(snap, &self.targets.ingress).await;
        // An absent counter on a scraped body means zero. The metrics-rs
        // library emits a counter only after its first increment. `None`
        // means the scrape itself failed. This is the same distinction
        // used for the sequencer block.
        let g = |m: &str| {
            body.as_deref()
                .map(|b| gauge_u64(sum_metric(b, m).unwrap_or(0.0), m))
        };
        snap.ingress_received = g(M_INGRESS_RECEIVED);
        snap.ingress_accepted = g(M_INGRESS_ACCEPTED);
        snap.ingress_rejected = g(M_INGRESS_REJECTED);
        snap.ingress_queue_depth = g(M_INGRESS_QUEUE);
        push_up(
            snap,
            format!("ingress@{}", self.targets.ingress.name),
            body.as_deref(),
        );
    }

    /// Scrape every sequencer node into `snap`, summing per-partition
    /// counters across nodes.
    async fn scrape_sequencers(&self, snap: &mut MetricsSnapshot) {
        let mut totals = SeqTotals::default();
        for target in &self.targets.sequencers {
            let body = self.fetch(snap, target).await;
            totals.fold_node(snap, &target.name, body.as_deref());
        }
        snap.seq_dropped_past = totals.scraped_any.then_some(totals.dropped_past);
        snap.seq_evictions = totals.scraped_any.then_some(totals.evictions);
        snap.seq_backpressure = totals.scraped_any.then_some(totals.backpressure);
    }
}

/// [`Scraper::scrape_sequencers`]'s running per-partition sums, and
/// whether any sequencer node scraped successfully.
#[derive(Default)]
struct SeqTotals {
    dropped_past: u64,
    evictions: u64,
    backpressure: u64,
    scraped_any: bool,
}

impl SeqTotals {
    /// Record `node`'s liveness in `snap`, and, if it scraped, fold its
    /// per-partition counters into the running totals.
    fn fold_node(&mut self, snap: &mut MetricsSnapshot, node: &str, body: Option<&str>) {
        push_up(snap, format!("sequencer@{node}"), body);
        let Some(body) = body else {
            // `None` means no sequencer was scraped.
            return;
        };
        // A successfully scraped body with an absent counter means zero
        // events, not unknown: metrics-rs counters appear in the
        // exposition only after their first increment.
        self.dropped_past = self.dropped_past.saturating_add(gauge_u64(
            sum_metric(body, M_SEQ_DROPPED).unwrap_or(0.0),
            M_SEQ_DROPPED,
        ));
        self.evictions = self.evictions.saturating_add(gauge_u64(
            sum_metric(body, M_SEQ_EVICTIONS).unwrap_or(0.0),
            M_SEQ_EVICTIONS,
        ));
        self.backpressure = self.backpressure.saturating_add(gauge_u64(
            sum_metric(body, M_SEQ_BACKPRESSURE).unwrap_or(0.0),
            M_SEQ_BACKPRESSURE,
        ));
        self.scraped_any = true;
    }
}

/// The value of one Prometheus sample line for `name`, across any label
/// set. A sample line is `name value`, `name{labels} value`, or
/// `name{labels} value timestamp`. Returns `None` if `line` is blank, a
/// comment, a different metric, or has no parseable value field.
fn sample_value(line: &str, name: &str) -> Option<f64> {
    let line = line.trim();
    if line.is_empty() || line.starts_with('#') {
        return None;
    }
    // Match `name` followed by `{` or whitespace.
    let rest = line.strip_prefix(name)?;
    let next = rest.chars().next();
    if !matches!(next, Some('{' | ' ' | '\t')) {
        return None; // For example, `name_suffix ...`. Not our metric.
    }
    // The value is the field after the (optional) `{...}` label block.
    let after_labels = if next == Some('{') {
        let (_, tail) = rest.split_once('}')?;
        tail
    } else {
        rest
    };
    after_labels.split_whitespace().next()?.parse::<f64>().ok()
}

/// Sum the values of every sample of `name` in a Prometheus text body,
/// across every label set. Returns `None` if no sample matches.
#[must_use]
pub(crate) fn sum_metric(body: &str, name: &str) -> Option<f64> {
    body.lines()
        .filter_map(|l| sample_value(l, name))
        .fold(None, |acc, v| Some(acc.unwrap_or(0.0) + v))
}

/// A scraped Prometheus counter or gauge sample, as `u64`. A negative
/// or NaN value is a scrape anomaly, not "zero events": a plain
/// `v as u64` cast would silently saturate either one to 0, which the
/// drop-accounting and liveness gates would then read as "no drops" or
/// "up-to-date." This logs the anomaly instead of hiding it, still
/// returning 0 so the caller does not need a fallible path for a metric
/// that is expected to be well-formed.
#[allow(
    clippy::cast_possible_truncation,
    clippy::cast_sign_loss,
    reason = "the is_finite/>= 0.0 guard above already rules out the negative and NaN cases cast_sign_loss and cast_possible_truncation warn about; a well-formed Prometheus counter sample stays far under u64::MAX"
)]
fn gauge_u64(v: f64, metric: &str) -> u64 {
    if v.is_finite() && v >= 0.0 {
        v as u64
    } else {
        tracing::warn!(
            metric,
            value = v,
            "scraped gauge is negative or NaN; treating as 0"
        );
        0
    }
}

/// Push a `service_up` entry for `label`. A failed scrape (`body` is
/// `None`) records an explicit down (`Some(0)`), not a missing value:
/// an unreachable service must fail the liveness gate, not be silently
/// excluded from it. A successful scrape with no `service_up` sample
/// yet, meaning the process just started, records `None`.
fn push_up(snap: &mut MetricsSnapshot, label: String, body: Option<&str>) {
    let up = match body {
        Some(b) => sum_metric(b, M_SERVICE_UP).map(|v| gauge_u64(v, M_SERVICE_UP)),
        None => Some(0),
    };
    snap.service_up.push((label, up));
}

#[cfg(test)]
mod tests {
    use super::*;

    const SAMPLE: &str = "\
# HELP kardamom_executor_block_number Most recently committed block number
# TYPE kardamom_executor_block_number gauge
kardamom_executor_block_number{service=\"executor\",host_id=\"local\"} 42
# TYPE kardamom_sequencer_tx_dropped_past_total counter
kardamom_sequencer_tx_dropped_past_total{partition=\"0\"} 3
kardamom_sequencer_tx_dropped_past_total{partition=\"1\"} 4
kardamom_ingress_tx_received_total 100
kardamom_ingress_tx_rejected_total{reason=\"timeout\"} 2
kardamom_ingress_tx_rejected_total{reason=\"duplicate\"} 1
kardamom_executor_block_apply_duration_seconds_count 7
";

    #[test]
    fn sum_single_gauge() {
        assert_eq!(
            sum_metric(SAMPLE, "kardamom_executor_block_number"),
            Some(42.0)
        );
    }

    #[test]
    fn sum_across_label_sets() {
        // 3 + 4 for partitions; 2 + 1 for reasons.
        assert_eq!(
            sum_metric(SAMPLE, "kardamom_sequencer_tx_dropped_past_total"),
            Some(7.0)
        );
        assert_eq!(
            sum_metric(SAMPLE, "kardamom_ingress_tx_rejected_total"),
            Some(3.0)
        );
    }

    #[test]
    fn no_label_block() {
        assert_eq!(
            sum_metric(SAMPLE, "kardamom_ingress_tx_received_total"),
            Some(100.0)
        );
    }

    #[test]
    fn prefix_collision_not_matched() {
        // A request for block_number must not match block_apply_duration.
        assert_eq!(sum_metric(SAMPLE, "kardamom_executor_block"), None);
    }

    #[test]
    fn missing_metric_is_none() {
        assert_eq!(sum_metric(SAMPLE, "kardamom_does_not_exist"), None);
    }

    #[test]
    fn push_up_reads_a_scraped_body_with_no_service_up_sample_as_unknown() {
        // A process that answered the scrape, but has not yet emitted
        // `kardamom_service_up` (metrics-rs emits a counter only after
        // its first increment), is unknown, not down.
        let mut snap = MetricsSnapshot::default();
        push_up(&mut snap, "executor@n0".to_string(), Some(SAMPLE));
        assert_eq!(snap.service_up, vec![("executor@n0".to_string(), None)]);
    }

    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    /// A scraper over the one ingress `target`, whose fallback runs
    /// `exec_program` in place of `docker`.
    fn scraper(via_docker: bool, exec_program: &'static str, target: MetricsTarget) -> Scraper {
        let set = ScrapeSet {
            via_docker,
            scrape: BTreeSet::new(),
            targets: MetricsTargets {
                executors: Vec::new(),
                ingress: target,
                sequencers: Vec::new(),
            },
        };
        Scraper::with_exec_program(set, exec_program).unwrap()
    }

    /// Serve `body` as one HTTP response on a loopback port; the port.
    async fn serve_once(body: &'static str) -> u16 {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        tokio::spawn(async move {
            let (mut sock, _) = listener.accept().await.unwrap();
            let mut request = [0_u8; 1024];
            let _ = sock.read(&mut request).await;
            let response = format!(
                "HTTP/1.1 200 OK\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}",
                body.len()
            );
            sock.write_all(response.as_bytes()).await.unwrap();
        });
        port
    }

    /// A loopback port that no listener holds.
    fn closed_port() -> u16 {
        std::net::TcpListener::bind("127.0.0.1:0")
            .unwrap()
            .local_addr()
            .unwrap()
            .port()
    }

    #[test]
    fn a_target_reads_inside_its_node_on_loopback_at_the_same_port() {
        let target = MetricsTarget::at("executor-0", Ipv4Addr::new(10, 0, 0, 5), 9004);
        assert_eq!(target.url.as_str(), "http://10.0.0.5:9004/metrics");
        assert_eq!(target.loopback_url(), "http://127.0.0.1:9004/metrics");
        let named = MetricsTarget::named("kardamom-ingress-0", INGRESS_METRICS_PORT).unwrap();
        assert_eq!(named.url.as_str(), "http://kardamom-ingress-0:9006/metrics");
        assert!(MetricsTarget::named("not a host", 9006).is_err());
    }

    #[tokio::test]
    async fn a_direct_read_needs_no_fallback() {
        let port = serve_once("kardamom_service_up 1\n").await;
        let target = MetricsTarget::at("ingress-0", Ipv4Addr::LOCALHOST, port);
        let scraper = scraper(false, "false", target.clone());
        let mut snap = MetricsSnapshot::default();
        let body = scraper.fetch(&mut snap, &target).await;
        assert_eq!(body.as_deref(), Some("kardamom_service_up 1\n"));
        assert_eq!(snap.fallbacks, 0);
    }

    #[tokio::test]
    async fn a_failed_direct_read_falls_back_through_the_node_and_counts() {
        let port = closed_port();
        let target = MetricsTarget::at("ingress-0", Ipv4Addr::LOCALHOST, port);
        // `echo` prints the fallback command line as its body.
        let echo = scraper(false, "echo", target.clone());
        let mut snap = MetricsSnapshot::default();
        let body = echo.fetch(&mut snap, &target).await.unwrap();
        assert_eq!(
            body.trim(),
            format!("exec ingress-0 curl -fsS --max-time 5 http://127.0.0.1:{port}/metrics")
        );
        assert_eq!(snap.fallbacks, 1);
        let failed = scraper(false, "false", target.clone());
        assert_eq!(failed.fetch(&mut snap, &target).await, None);
        assert_eq!(snap.fallbacks, 2);
    }

    #[tokio::test]
    async fn via_docker_reads_through_the_node_with_no_fallback_count() {
        let port = serve_once("kardamom_service_up 1\n").await;
        let target = MetricsTarget::at("ingress-0", Ipv4Addr::LOCALHOST, port);
        let scraper = scraper(true, "echo", target.clone());
        let mut snap = MetricsSnapshot::default();
        let body = scraper.fetch(&mut snap, &target).await.unwrap();
        assert!(body.starts_with("exec ingress-0 curl"), "{body}");
        assert_eq!(snap.fallbacks, 0);
    }

    #[tokio::test]
    async fn a_snapshot_counts_the_fallbacks_of_every_target() {
        let target = MetricsTarget::at("ingress-0", Ipv4Addr::LOCALHOST, closed_port());
        let mut scraper = scraper(false, "false", target.clone());
        scraper.scrape = ["executor", "ingress"].map(String::from).into();
        scraper.targets.executors = vec![target.clone(), target];
        let snap = scraper.snapshot().await;
        assert_eq!(snap.fallbacks, 3);
        assert_eq!(snap.executor_blocks.len(), 2);
        assert_eq!(snap.ingress_received, None);
    }

    #[test]
    fn push_up_reads_a_failed_scrape_as_down() {
        let mut snap = MetricsSnapshot::default();
        push_up(&mut snap, "executor@n0".to_string(), None);
        assert_eq!(snap.service_up, vec![("executor@n0".to_string(), Some(0))]);
    }
}

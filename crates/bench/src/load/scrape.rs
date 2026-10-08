//! This module scrapes cluster metrics for the load harness.
//!
//! A snapshot reads every target of the scrape set through
//! [`ExporterReader`]: directly first, then through `docker exec` inside
//! the node. A snapshot counts each fallback, because a runner-wide stall
//! of `docker exec` hides every exec-based read at once.

use std::collections::BTreeSet;

use crate::load::exporter::{ExporterReader, MetricsTarget, MetricsTargets};

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

/// The services to scrape, and the exporter targets of each.
#[derive(Debug, Clone)]
pub(crate) struct Scraper {
    /// The lowercased service names to scrape: any of executor, ingress,
    /// or sequencer. The sealer values ride along with the executor
    /// scrape, since the clustered sealer has no endpoint of its own.
    scrape: BTreeSet<String>,
    targets: MetricsTargets,
    reader: ExporterReader,
}

/// The inputs of [`Scraper::new`].
pub(crate) struct ScrapeSet {
    pub scrape: BTreeSet<String>,
    pub targets: MetricsTargets,
}

impl Scraper {
    /// A scraper over `set`.
    ///
    /// # Errors
    ///
    /// Returns an error if the HTTP client cannot be built.
    pub(crate) fn new(set: ScrapeSet) -> anyhow::Result<Self> {
        Ok(Self::with_reader(set, ExporterReader::new()?))
    }

    fn with_reader(set: ScrapeSet, reader: ExporterReader) -> Self {
        Self {
            scrape: set.scrape,
            targets: set.targets,
            reader,
        }
    }

    fn wants(&self, svc: &str) -> bool {
        self.scrape.contains(svc)
    }

    /// Read one target's `/metrics` body into `snap`'s fallback count.
    /// Returns `None` if no read answers.
    async fn fetch(&self, snap: &mut MetricsSnapshot, target: &MetricsTarget) -> Option<String> {
        let read = self.reader.read(target).await;
        snap.fallbacks = snap.fallbacks.saturating_add(u64::from(read.fell_back));
        read.body
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

    #[tokio::test]
    async fn a_snapshot_counts_the_fallbacks_of_every_target() {
        // A bridged target on a closed loopback port: the direct read
        // fails, and `false` fails the read through the node.
        let port = std::net::TcpListener::bind("127.0.0.1:0")
            .unwrap()
            .local_addr()
            .unwrap()
            .port();
        let target = MetricsTarget::bridged(std::net::Ipv4Addr::LOCALHOST, "ingress-0", port);
        let set = ScrapeSet {
            scrape: ["executor", "ingress"].map(String::from).into(),
            targets: MetricsTargets {
                executors: vec![target.clone(), target.clone()],
                ingress: target,
                sequencers: Vec::new(),
            },
        };
        let reader = ExporterReader::with_exec("false", std::time::Duration::from_secs(1)).unwrap();
        let snap = Scraper::with_reader(set, reader).snapshot().await;
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

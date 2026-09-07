//! This module scrapes cluster metrics for the load harness.
//!
//! Each service's Prometheus exporter binds to loopback inside its own
//! container, which runs on host-net inside the `DinD` node container.
//! So the only way to reach an exporter from the orchestrator or host
//! is `docker exec <node> curl 127.0.0.1:<port>/metrics`. A `direct`
//! mode, plain `curl http://<node>:<port>`, is a fallback for a setup
//! that rebinds the exporters to a routable address.

use std::collections::BTreeSet;

use tokio::process::Command;

// The default metrics ports. These match each service's --metrics-addr default.
const PORT_EXECUTOR: u16 = 9004;
const PORT_INGRESS: u16 = 9006;
const PORT_SEQUENCER: u16 = 9001;

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
}

/// The services to scrape, and the node-container names for each.
#[derive(Debug, Clone)]
pub(crate) struct Scraper {
    /// When true, use `docker exec <node> curl 127.0.0.1:<port>`.
    /// When false, use a direct `curl http://<node>:<port>`.
    pub via_docker: bool,
    /// The lowercased service names to scrape: any of executor, ingress,
    /// or sequencer. The sealer values ride along with the executor
    /// scrape, since the clustered sealer has no endpoint of its own.
    pub scrape: BTreeSet<String>,
    /// The executor node-container names.
    pub executor_nodes: Vec<String>,
    /// The ingress node-container name.
    pub ingress_node: String,
    /// The sequencer node-container names.
    pub sequencer_nodes: Vec<String>,
}

impl Scraper {
    fn wants(&self, svc: &str) -> bool {
        self.scrape.contains(svc)
    }

    /// Fetch one node's `/metrics` body. Returns `None` if unreachable.
    async fn fetch(&self, node: &str, port: u16) -> Option<String> {
        let url = format!("http://127.0.0.1:{port}/metrics");
        let out = if self.via_docker {
            Command::new("docker")
                .args(["exec", node, "curl", "-fsS", "--max-time", "5", &url])
                .output()
                .await
        } else {
            let direct = format!("http://{node}:{port}/metrics");
            Command::new("curl")
                .args(["-fsS", "--max-time", "5", &direct])
                .output()
                .await
        };
        match out {
            Ok(o) if o.status.success() => Some(String::from_utf8_lossy(&o.stdout).into_owned()),
            _ => None,
        }
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
        for node in &self.executor_nodes {
            let body = self.fetch(node, PORT_EXECUTOR).await;
            let g = |m: &str| {
                body.as_deref()
                    .and_then(|b| sum_metric(b, m))
                    .map(|v| gauge_u64(v, m))
            };
            snap.executor_blocks
                .push((node.clone(), g(M_EXECUTOR_BLOCK)));
            // This is sealer output, re-exported by this executor from
            // cluster egress. Keep the most advanced observation across
            // nodes, so a single stalled executor does not hide sealer
            // progress.
            snap.sealer_block = snap.sealer_block.max(g(M_SEALER_BLOCK));
            snap.sealer_boundaries = snap.sealer_boundaries.max(g(M_SEALER_BOUNDARIES));
            push_up(snap, format!("executor@{node}"), body.as_deref());
        }
    }

    /// Scrape ingress into `snap`: submission counts, queue depth, and
    /// liveness.
    async fn scrape_ingress(&self, snap: &mut MetricsSnapshot) {
        let body = self.fetch(&self.ingress_node, PORT_INGRESS).await;
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
            format!("ingress@{}", self.ingress_node),
            body.as_deref(),
        );
    }

    /// Scrape every sequencer node into `snap`, summing per-partition
    /// counters across nodes.
    async fn scrape_sequencers(&self, snap: &mut MetricsSnapshot) {
        let mut totals = SeqTotals::default();
        for node in &self.sequencer_nodes {
            let body = self.fetch(node, PORT_SEQUENCER).await;
            totals.fold_node(snap, node, body.as_deref());
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

    #[test]
    fn push_up_reads_a_failed_scrape_as_down() {
        let mut snap = MetricsSnapshot::default();
        push_up(&mut snap, "executor@n0".to_string(), None);
        assert_eq!(snap.service_up, vec![("executor@n0".to_string(), Some(0))]);
    }
}

//! This module scrapes cluster metrics for the load harness.
//!
//! Each service's Prometheus exporter binds to loopback inside its own
//! container, which runs on host-net inside the DinD node container.
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
pub struct MetricsSnapshot {
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
pub struct Scraper {
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
    pub async fn snapshot(&self) -> MetricsSnapshot {
        let mut snap = MetricsSnapshot::default();

        if self.wants("executor") {
            for node in &self.executor_nodes {
                let body = self.fetch(node, PORT_EXECUTOR).await;
                let g = |m: &str| {
                    #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
                    body.as_deref()
                        .and_then(|b| sum_metric(b, m))
                        .map(|v| v as u64)
                };
                snap.executor_blocks
                    .push((node.clone(), g(M_EXECUTOR_BLOCK)));
                // This is sealer output, re-exported by this executor from
                // cluster egress. Keep the most advanced observation across
                // nodes, so a single stalled executor does not hide sealer
                // progress.
                snap.sealer_block = snap.sealer_block.max(g(M_SEALER_BLOCK));
                snap.sealer_boundaries = snap.sealer_boundaries.max(g(M_SEALER_BOUNDARIES));
                // A failed scrape counts as an explicit down, not a missing value.
                let up = if body.is_some() {
                    g(M_SERVICE_UP)
                } else {
                    Some(0)
                };
                snap.service_up.push((format!("executor@{node}"), up));
            }
        }
        if self.wants("ingress") {
            let body = self.fetch(&self.ingress_node, PORT_INGRESS).await;
            // An absent counter on a scraped body means zero. The metrics-rs
            // library emits a counter only after its first increment. `None`
            // means the scrape itself failed. This is the same distinction
            // used for the sequencer block.
            let g = |m: &str| {
                #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
                body.as_deref()
                    .map(|b| sum_metric(b, m).unwrap_or(0.0) as u64)
            };
            snap.ingress_received = g(M_INGRESS_RECEIVED);
            snap.ingress_accepted = g(M_INGRESS_ACCEPTED);
            snap.ingress_rejected = g(M_INGRESS_REJECTED);
            snap.ingress_queue_depth = g(M_INGRESS_QUEUE);
            // A failed scrape counts as an explicit down, not a missing value.
            let up = if body.is_some() {
                g(M_SERVICE_UP)
            } else {
                Some(0)
            };
            snap.service_up
                .push((format!("ingress@{}", self.ingress_node), up));
        }
        if self.wants("sequencer") {
            // Sum the per-partition counters across all sequencer nodes.
            let (mut d, mut e, mut b) = (0u64, 0u64, 0u64);
            let (mut any_d, mut any_e, mut any_b) = (false, false, false);
            for node in &self.sequencer_nodes {
                let body = self.fetch(node, PORT_SEQUENCER).await;
                if body.is_none() {
                    // A failed scrape is an explicit down entry. Before this
                    // change, the node was silently missing from service_up,
                    // so an unreachable sequencer passed the liveness gate.
                    snap.service_up.push((format!("sequencer@{node}"), Some(0)));
                }
                if let Some(body) = body {
                    // A successfully scraped body with an absent counter means
                    // zero events, not unknown: metrics-rs counters appear in
                    // the exposition only after their first increment.
                    // Requiring the sample line made every clean run report
                    // `None`, so the drop-accounting row was never useful.
                    // `None` now means no sequencer was scraped.
                    #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
                    {
                        d += sum_metric(&body, M_SEQ_DROPPED).unwrap_or(0.0) as u64;
                        e += sum_metric(&body, M_SEQ_EVICTIONS).unwrap_or(0.0) as u64;
                        b += sum_metric(&body, M_SEQ_BACKPRESSURE).unwrap_or(0.0) as u64;
                        any_d = true;
                        any_e = true;
                        any_b = true;
                    }
                    let up = sum_metric(&body, M_SERVICE_UP).map(|v| {
                        #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
                        {
                            v as u64
                        }
                    });
                    snap.service_up.push((format!("sequencer@{node}"), up));
                }
            }
            snap.seq_dropped_past = any_d.then_some(d);
            snap.seq_evictions = any_e.then_some(e);
            snap.seq_backpressure = any_b.then_some(b);
        }
        snap
    }
}

/// Sum the values of every sample of `name` in a Prometheus text body,
/// across every label set. Returns `None` if no sample matches.
#[must_use]
pub fn sum_metric(body: &str, name: &str) -> Option<f64> {
    let mut total = 0.0_f64;
    let mut matched = false;
    for line in body.lines() {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        // A sample line is `name value`, `name{labels} value`, or
        // `name{labels} value timestamp`. Match `name` followed by `{` or
        // whitespace.
        let rest = match line.strip_prefix(name) {
            Some(r) => r,
            None => continue,
        };
        let next = rest.chars().next();
        if !matches!(next, Some('{') | Some(' ') | Some('\t')) {
            continue; // For example, `name_suffix ...`. Not our metric.
        }
        // The value is the field after the (optional) `{...}` label block.
        let after_labels = if next == Some('{') {
            match rest.split_once('}') {
                Some((_, tail)) => tail,
                None => continue,
            }
        } else {
            rest
        };
        if let Some(tok) = after_labels.split_whitespace().next()
            && let Ok(v) = tok.parse::<f64>()
        {
            total += v;
            matched = true;
        }
    }
    matched.then_some(total)
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
}

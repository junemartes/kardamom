//! Cluster metric scraping for the load harness.
//!
//! Service Prometheus exporters bind loopback *inside* each service
//! container (which runs host-net inside the DinD node container), so the
//! only way to reach them from the orchestrator/host is
//! `docker exec <node> curl 127.0.0.1:<port>/metrics`. A `direct` mode
//! (plain `curl http://<node>:<port>`) is provided as a fallback for setups
//! that rebind the exporters to a routable address.

use std::collections::BTreeSet;

use tokio::process::Command;

// Default metrics ports (the services' --metrics-addr defaults).
const PORT_EXECUTOR: u16 = 9004;
const PORT_INGRESS: u16 = 9006;
const PORT_SEQUENCER: u16 = 9001;

// Exact Prometheus metric names exposed by each service.
const M_EXECUTOR_BLOCK: &str = "kardamom_executor_block_number";
// The clustered sealer has no Prometheus endpoint; each executor re-exports
// the sealer's boundary stream as it decodes cluster egress, so the
// kardamom_sealer_* series are read off the executor endpoints.
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
    /// `(node, executor_block_number)` per scraped executor node.
    pub executor_blocks: Vec<(String, Option<u64>)>,
    /// Sealer's last sealed block number (most-advanced executor observation
    /// of the cluster's boundary stream).
    pub sealer_block: Option<u64>,
    /// Sealer block-boundaries counter (liveness during chaos), observed the
    /// same way.
    pub sealer_boundaries: Option<u64>,
    /// Ingress: total submissions received.
    pub ingress_received: Option<u64>,
    /// Ingress: submissions that returned a receipt.
    pub ingress_accepted: Option<u64>,
    /// Ingress: submissions rejected (summed over all `reason` labels).
    pub ingress_rejected: Option<u64>,
    /// Ingress: current pending-tx queue depth.
    pub ingress_queue_depth: Option<u64>,
    /// Sequencer: txs dropped for a past nonce (summed over partitions).
    pub seq_dropped_past: Option<u64>,
    /// Sequencer: pending-buffer evictions (summed over partitions).
    pub seq_evictions: Option<u64>,
    /// Sequencer: backpressure events (summed over partitions).
    pub seq_backpressure: Option<u64>,
    /// `(label, up)` for each scraped service (1 = up).
    pub service_up: Vec<(String, Option<u64>)>,
}

/// Which services to scrape and the node-container names for each.
#[derive(Debug, Clone)]
pub struct Scraper {
    /// `docker exec <node> curl 127.0.0.1:<port>` when true; direct
    /// `curl http://<node>:<port>` when false.
    pub via_docker: bool,
    /// Lowercased service names to scrape: any of executor/ingress/sequencer.
    /// (The sealer values ride the executor scrape — the clustered sealer has
    /// no endpoint of its own.)
    pub scrape: BTreeSet<String>,
    /// Executor node-container names.
    pub executor_nodes: Vec<String>,
    /// Ingress node-container name.
    pub ingress_node: String,
    /// Sequencer node-container names.
    pub sequencer_nodes: Vec<String>,
}

impl Scraper {
    fn wants(&self, svc: &str) -> bool {
        self.scrape.contains(svc)
    }

    /// Fetch one node's `/metrics` body, or `None` if unreachable.
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
                // Sealer output as re-exported by this executor from cluster
                // egress; keep the most-advanced observation across nodes so a
                // single stalled executor doesn't mask sealer progress.
                snap.sealer_block = snap.sealer_block.max(g(M_SEALER_BLOCK));
                snap.sealer_boundaries = snap.sealer_boundaries.max(g(M_SEALER_BOUNDARIES));
                snap.service_up
                    .push((format!("executor@{node}"), g(M_SERVICE_UP)));
            }
        }
        if self.wants("ingress") {
            let body = self.fetch(&self.ingress_node, PORT_INGRESS).await;
            let g = |m: &str| {
                #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
                body.as_deref()
                    .and_then(|b| sum_metric(b, m))
                    .map(|v| v as u64)
            };
            snap.ingress_received = g(M_INGRESS_RECEIVED);
            snap.ingress_accepted = g(M_INGRESS_ACCEPTED);
            snap.ingress_rejected = g(M_INGRESS_REJECTED);
            snap.ingress_queue_depth = g(M_INGRESS_QUEUE);
            snap.service_up
                .push((format!("ingress@{}", self.ingress_node), g(M_SERVICE_UP)));
        }
        if self.wants("sequencer") {
            // Sum the per-partition counters across all sequencer nodes.
            let (mut d, mut e, mut b) = (0u64, 0u64, 0u64);
            let (mut any_d, mut any_e, mut any_b) = (false, false, false);
            for node in &self.sequencer_nodes {
                if let Some(body) = self.fetch(node, PORT_SEQUENCER).await {
                    #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
                    {
                        if let Some(v) = sum_metric(&body, M_SEQ_DROPPED) {
                            d += v as u64;
                            any_d = true;
                        }
                        if let Some(v) = sum_metric(&body, M_SEQ_EVICTIONS) {
                            e += v as u64;
                            any_e = true;
                        }
                        if let Some(v) = sum_metric(&body, M_SEQ_BACKPRESSURE) {
                            b += v as u64;
                            any_b = true;
                        }
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

/// Sum the values of all samples of `name` in a Prometheus text body
/// (i.e. across every label set). Returns `None` if no sample matches.
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
            continue; // e.g. `name_suffix ...` — not our metric
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
        // 3 + 4 partitions; 2 + 1 reasons.
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
        // Requesting the block_number must NOT match block_apply_duration.
        assert_eq!(sum_metric(SAMPLE, "kardamom_executor_block"), None);
    }

    #[test]
    fn missing_metric_is_none() {
        assert_eq!(sum_metric(SAMPLE, "kardamom_does_not_exist"), None);
    }
}

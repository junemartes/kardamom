//! Read-only probes over the topology: the executor tier, the ingress
//! pair, the validator, and the lane-0 sequencer replicas. Every probe
//! returns `None` when no exporter answers.

use std::net::Ipv4Addr;

use crate::contract::NodeContract;
use crate::metrics::{self, Scrape, Target};

/// The executor gauge that only goes up: the pipeline-progress signal.
pub const EXECUTOR_BLOCK_METRIC: &str = "kardamom_executor_block_number";
/// The sealer boundary counter the executors re-export from cluster
/// egress; the Java cluster node has no exporter of its own.
pub const SEALER_BOUNDARIES_METRIC: &str = "kardamom_sealer_boundaries_emitted_total";
pub const INGRESS_RECEIVED_METRIC: &str = "kardamom_ingress_tx_received_total";
pub const EXECUTOR_PORT: u16 = 9004;
/// The ingress and the validator share this port on different nodes.
pub const INGRESS_PORT: u16 = 9006;
pub const VALIDATOR_PORT: u16 = 9006;
/// Lane 0's metrics port: `9001 + 10 * lane`.
pub const SEQUENCER_LANE0_PORT: u16 = 9001;
/// The Nomad task name inside the `cluster` job.
pub const CLUSTER_TASK: &str = "cluster";

/// One node the probes address.
#[derive(Debug, Clone)]
pub struct Probed {
    /// The host container name.
    pub container: String,
    pub ip: Ipv4Addr,
}

/// The probe set of one cluster.
#[derive(Debug, Clone)]
pub struct Probes {
    scrape: Scrape,
    /// The executor nodes, by index.
    pub executors: Vec<Probed>,
    /// The ingress nodes, by index. Their exporter binds loopback.
    pub ingresses: Vec<Probed>,
    /// The aux node that runs the validator.
    pub validator: Probed,
    /// The sequencer nodes, by index; lane 0 runs one replica on each.
    pub sequencers: Vec<Probed>,
}

fn probed(contract: &NodeContract, role: &str) -> Vec<Probed> {
    contract
        .of_role(role)
        .into_iter()
        .map(|n| Probed {
            container: n.container.clone(),
            ip: n.ip,
        })
        .collect()
}

impl Probes {
    /// The probes of the cluster the contract describes.
    ///
    /// # Errors
    ///
    /// Returns an error if the contract has no aux node.
    pub fn new(contract: &NodeContract) -> anyhow::Result<Self> {
        let validator = probed(contract, "aux")
            .into_iter()
            .next()
            .ok_or_else(|| anyhow::anyhow!("node contract has no aux node for the validator"))?;
        Ok(Self {
            scrape: Scrape::new(),
            executors: probed(contract, "executor"),
            ingresses: probed(contract, "ingress"),
            validator,
            sequencers: probed(contract, "sequencer"),
        })
    }

    /// The scraper, for probes a case builds itself.
    #[must_use]
    pub fn scrape(&self) -> &Scrape {
        &self.scrape
    }

    /// The executor target of index `i`.
    ///
    /// # Panics
    ///
    /// Panics if `i` is not an executor index of this cluster; the
    /// shards address three executors by construction.
    #[must_use]
    pub fn executor_target(&self, i: usize) -> Target {
        let e = &self.executors[i];
        Target::bridged(e.ip, &e.container, EXECUTOR_PORT)
    }

    /// The validator's loopback target.
    #[must_use]
    pub fn validator_target(&self) -> Target {
        Target::loopback(&self.validator.container, VALIDATOR_PORT)
    }

    /// One ingress node's loopback target.
    #[must_use]
    pub fn ingress_target(&self, node: &Probed) -> Target {
        Target::loopback(&node.container, INGRESS_PORT)
    }

    /// The lane-0 replica target on sequencer node `i`.
    ///
    /// # Panics
    ///
    /// Panics if `i` is not a sequencer index of this cluster.
    #[must_use]
    pub fn sequencer_lane0_target(&self, i: usize) -> Target {
        let s = &self.sequencers[i];
        Target::bridged(s.ip, &s.container, SEQUENCER_LANE0_PORT)
    }

    /// One executor's `/metrics` body.
    pub async fn exec_metrics(&self, i: usize) -> Option<String> {
        self.scrape.fetch(&self.executor_target(i)).await
    }

    /// The first-sample value of `metric` on executor `i`.
    pub async fn exec_metric(&self, i: usize, metric: &str) -> Option<i64> {
        metrics::first(&self.exec_metrics(i).await?, metric)
    }

    /// The maximum of `metric` across every responding executor. A
    /// replica that restarted can fairly report a low value while it
    /// replays; the fleet maximum is the pipeline signal.
    async fn executor_max(&self, metric: &str) -> Option<i64> {
        let mut best = None;
        for i in 0..self.executors.len() {
            best = best.max(self.exec_metric(i, metric).await);
        }
        best
    }

    /// The pipeline-progress probe: the highest committed block any
    /// executor reports.
    pub async fn executor_progress(&self) -> Option<i64> {
        self.executor_max(EXECUTOR_BLOCK_METRIC).await
    }

    /// The sealer boundary counter, which ticks about four times a
    /// second even with no load.
    pub async fn sealer_boundaries(&self) -> Option<i64> {
        self.executor_max(SEALER_BOUNDARIES_METRIC).await
    }

    /// The submit counter summed across the ingress pair: the "is load
    /// flowing" signal. `None` when no ingress answers.
    pub async fn ingress_received(&self) -> Option<i64> {
        let mut total = None;
        for node in &self.ingresses {
            total = add_option(total, self.ingress_sum(node).await);
        }
        total
    }

    async fn ingress_sum(&self, node: &Probed) -> Option<i64> {
        let body = self.scrape.fetch(&self.ingress_target(node)).await?;
        metrics::sum(&body, INGRESS_RECEIVED_METRIC)
    }

    /// One validator metric, first sample. `None` on a failed scrape.
    pub async fn val_metric(&self, metric: &str) -> Option<i64> {
        let body = self.scrape.fetch(&self.validator_target()).await?;
        metrics::first(&body, metric)
    }

    /// A validator metric a verdict needs. The scrape retries, then uses
    /// the always-present committed-block gauge as a canary: canary up
    /// and counter absent means a genuine zero, since a counter exports
    /// only once incremented. A dead exporter is an error, never zero.
    ///
    /// # Errors
    ///
    /// Returns an error if the exporter does not answer five times.
    pub async fn val_metric_required(&self, metric: &str, why: &str) -> anyhow::Result<i64> {
        for _ in 0..5 {
            if let Some(v) = self.val_metric_or_canary_zero(metric).await {
                return Ok(v);
            }
            tokio::time::sleep(std::time::Duration::from_secs(3)).await;
        }
        Err(crate::chaos_fail!(
            "validator exporter unscrapeable after 5 tries — refusing to treat a dead exporter as 0 ({why})"
        ))
    }

    async fn val_metric_or_canary_zero(&self, metric: &str) -> Option<i64> {
        let body = self.scrape.fetch(&self.validator_target()).await?;
        metrics::first(&body, metric)
            .or_else(|| metrics::first(&body, "validator_committed_block").map(|_| 0))
    }

    /// The summed samples of `metric` on lane 0's replica on sequencer
    /// node `i`.
    pub async fn seq_lane0_metric(&self, i: usize, metric: &str) -> Option<i64> {
        let body = self.scrape.fetch(&self.sequencer_lane0_target(i)).await?;
        metrics::sum(&body, metric)
    }

    /// The label-filtered sum of `metric` on lane 0's replica on
    /// sequencer node `i`.
    pub async fn seq_lane0_metric_where(&self, i: usize, metric: &str, label: &str) -> Option<i64> {
        let body = self.scrape.fetch(&self.sequencer_lane0_target(i)).await?;
        metrics::sum_where(&body, metric, label)
    }

    /// The shard map version an ingress replica runs.
    pub async fn ingress_map_version(&self, node: &Probed) -> Option<i64> {
        let body = self.scrape.fetch(&self.ingress_target(node)).await?;
        metrics::first(&body, "kardamom_ingress_shard_map_version")
    }
}

fn add_option(total: Option<i64>, value: Option<i64>) -> Option<i64> {
    match (total, value) {
        (None, v) => v,
        (Some(t), None) => Some(t),
        (Some(t), Some(v)) => Some(t.saturating_add(v)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_sum_over_the_pair_ignores_a_dark_replica_but_not_both() {
        assert_eq!(add_option(None, None), None);
        assert_eq!(add_option(None, Some(3)), Some(3));
        assert_eq!(add_option(Some(3), None), Some(3));
        assert_eq!(add_option(Some(3), Some(4)), Some(7));
    }
}

//! Running workers keep serving clients while metadata access is unavailable.

use std::time::{Duration, Instant};

use anyhow::Context;
use serde::Serialize;

use crate::harness::Harness;
use crate::probes::CLUSTER_TASK;
use crate::rpc::Rpc;

mod network;
use network::{Action, Check, NodeView, Step};

#[cfg(test)]
mod tests;

const CTX: &str = "control-plane-loss-recover";
/// The Nomad heartbeat grace of the local profile, in seconds.
const HEARTBEAT_GRACE_S: u64 = 60;
/// How long the metadata stays isolated: past the Nomad heartbeat grace,
/// so the servers mark the isolated clients down, with a margin for the
/// heartbeat TTL.
const HOLD: Duration = Duration::from_secs(HEARTBEAT_GRACE_S + 15);
/// The client transfers during the hold: at its start, in its middle,
/// and at its end.
const CLIENT_STEPS: [u64; 3] = [0, 40, HOLD.as_secs()];
const CLIENT_BOUND: Duration = Duration::from_secs(30);

struct ControlPlane {
    views: Vec<NodeView>,
    checks: [Check; 4],
    since: Option<Instant>,
    receipts: Vec<ClientReceipt>,
}

#[derive(Serialize)]
struct ClientReceipt {
    at_secs: f64,
    receipt_seconds: f64,
    nonce: u64,
    hash: String,
}

impl ControlPlane {
    fn new(h: &Harness) -> anyhow::Result<Self> {
        Ok(Self {
            views: h
                .contract
                .nodes
                .values()
                .filter(|node| !node.control_plane)
                .map(|node| NodeView {
                    node: node.container.clone(),
                    tasks: String::new(),
                })
                .collect(),
            checks: Check::metadata(&h.contract.nomad_addr(4646)?, h.contract.control()?.ip),
            since: None,
            receipts: Vec::new(),
        })
    }

    async fn run(&mut self, h: &mut Harness) -> anyhow::Result<()> {
        let during = self.isolated(h).await;
        let cleared = self.all(Action::Clear).await;
        let report = self.write_report(&during, &cleared);
        if let Err(error) = &cleared {
            crate::log(format!(
                "{CTX}: clearing the metadata partition fails: {error:#}"
            ));
        }
        if let Err(error) = &report {
            crate::log(format!("{CTX}: preserving fault evidence fails: {error:#}"));
        }
        during?;
        cleared?;
        report?;
        self.all(Action::Available).await?;
        Self::await_workers(h).await?;
        h.assert_progress().await
    }

    async fn await_workers(h: &mut Harness) -> anyhow::Result<()> {
        let budget = h.knobs.reschedule_slo;
        tokio::time::timeout(budget, Self::workers_ready(h, budget))
            .await
            .context("the workers do not return within the reschedule budget")?
    }

    async fn workers_ready(h: &mut Harness, budget: Duration) -> anyhow::Result<()> {
        for (job, count) in [
            (CLUSTER_TASK, 3),
            ("executor", 3),
            ("sequencer", 4),
            ("ingress", 2),
        ] {
            h.assert_count(job, count, budget).await?;
        }
        // Nomad can report running before the ingress connects to the
        // cluster and binds RPC. Both listeners share the reschedule deadline.
        h.assert_ingress_pair_live(CTX).await
    }

    async fn isolated(&mut self, h: &Harness) -> anyhow::Result<()> {
        self.views = self.all(Action::ObserveAndArm).await?;
        self.since = Some(Instant::now());
        self.all(Action::Isolated).await?;
        for offset in CLIENT_STEPS {
            self.client_step(h, Duration::from_secs(offset)).await?;
        }
        self.all(Action::Unchanged).await?;
        Ok(())
    }

    async fn client_step(&mut self, h: &Harness, offset: Duration) -> anyhow::Result<()> {
        let elapsed = self.since.map_or(Duration::ZERO, |since| since.elapsed());
        tokio::time::sleep(offset.saturating_sub(elapsed)).await;
        let rpc = Rpc::new(&h.rpc_url, h.knobs.chain_id)?;
        let started = Instant::now();
        let (nonce, hash) = tokio::time::timeout(CLIENT_BOUND, async {
            let nonce = rpc.nonce_of(h.knobs.gate_account).await?;
            let hash = rpc
                .transfer_hash(h.knobs.gate_account, nonce, CLIENT_BOUND)
                .await?;
            Ok::<_, anyhow::Error>((nonce, hash))
        })
        .await
        .context("the client cannot transact within the bound during metadata isolation")??;
        let receipt_seconds = started.elapsed().as_secs_f64();
        crate::log(format!(
            "{CTX}: client nonce {nonce} receives {hash} in {receipt_seconds:.3}s while metadata is isolated"
        ));
        self.receipts.push(ClientReceipt {
            at_secs: self
                .since
                .map_or(0.0, |since| since.elapsed().as_secs_f64()),
            receipt_seconds,
            nonce,
            hash,
        });
        Ok(())
    }

    async fn all(&self, action: Action) -> anyhow::Result<Vec<NodeView>> {
        let mut tasks = tokio::task::JoinSet::new();
        for view in &self.views {
            tasks.spawn(
                Step {
                    view: view.clone(),
                    checks: self.checks.clone(),
                    action,
                }
                .run(),
            );
        }
        let mut results = Vec::new();
        while let Some(result) = tasks.join_next().await {
            results.push(
                result
                    .context("control-plane fault worker fails")
                    .and_then(std::convert::identity),
            );
        }
        let mut views = results.into_iter().collect::<anyhow::Result<Vec<_>>>()?;
        views.sort_by(|a, b| a.node.cmp(&b.node));
        Ok(views)
    }

    fn write_report(
        &self,
        during: &anyhow::Result<()>,
        cleared: &anyhow::Result<Vec<NodeView>>,
    ) -> anyhow::Result<()> {
        let body = serde_json::json!({
            "case": CTX,
            "scope": "data nodes lose Nomad API/RPC, Consul HTTP, and Consul DNS; clients must keep transacting on existing data connections",
            "planned_hold_secs": HOLD.as_secs(),
            "observed_secs": self.since.map(|since| since.elapsed().as_secs_f64()),
            "workers": self.views,
            "client_receipts": self.receipts,
            "failure": during.as_ref().err().map(|error| format!("{error:#}")),
            "cleanup_failure": cleared.as_ref().err().map(|error| format!("{error:#}")),
        });
        std::fs::write(
            "/tmp/chaos-control-plane-loss-recover-fault.json",
            serde_json::to_vec_pretty(&body)?,
        )
        .context("preserve control-plane fault evidence")
    }
}

pub(crate) async fn loss_recover(h: &mut Harness) -> anyhow::Result<()> {
    ControlPlane::new(h)?.run(h).await
}

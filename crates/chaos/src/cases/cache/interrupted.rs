//! Stop each mirror inside a checkpoint scan, with its Redis writes
//! blocked. Every replacement completes a rebuild before recovery passes.

use anyhow::Context;
use serde::Serialize;
use std::num::NonZeroU64;
use std::time::Duration;

use super::rebuilds::{MirrorRebuilds, RebuildPhase};
use super::write_pause::WritePause;
use super::{CacheRecovery, MIRROR_ALLOCS, MIRROR_WRITE_RETRIES, freeze_and_flush, live_primary};
use crate::harness::Harness;
use crate::inject::Killed;
use crate::nomad::SavedJob;
use crate::rpc::Rpc;

const CTX: &str = "mirror-kill-rebuild";
const TASK: &str = "state-mirror";
/// The ingress-churn reserve is unused by cache-shard workloads.
const FAULT_ACCOUNT: u32 = 16;

#[derive(Serialize)]
struct Target {
    index: usize,
    node: String,
    cid: String,
    started_at: String,
    write_retries: Option<NonZeroU64>,
}

impl Target {
    async fn verify_blocked(&mut self, h: &Harness) -> anyhow::Result<()> {
        let retries = h
            .probes
            .mirror_metric(self.index, MIRROR_WRITE_RETRIES)
            .await
            .with_context(|| format!("{CTX}: no retry metric on {}", self.node))?;
        self.write_retries = Some(NonZeroU64::new(u64::try_from(retries)?).with_context(|| {
            format!(
                "{CTX}: the current mirror on {} has retried no write",
                self.node
            )
        })?);
        anyhow::ensure!(
            h.probes
                .mirror_metric(self.index, "kardamom_state_mirror_serving")
                .await
                == Some(0),
            "{CTX}: the mirror on {} is not rebuilding",
            self.node
        );
        anyhow::ensure!(
            h.nodes.inner_cid(&self.node, TASK).await.as_ref() == Some(&self.cid)
                && h.nodes.started_at(&self.node, &self.cid).await.as_ref()
                    == Some(&self.started_at),
            "{CTX}: mirror process on {} changed during the blocked-scan proof",
            self.node
        );
        Ok(())
    }
}

pub(super) struct InterruptedRebuild<'a> {
    h: &'a mut Harness,
    saved: SavedJob,
    primary: String,
    writes: WritePause,
    baseline: MirrorRebuilds,
    blocked: Option<MirrorRebuilds>,
    targets: Vec<Target>,
    traffic_hash: Option<String>,
}

impl InterruptedRebuild<'_> {
    pub(super) async fn run(h: &mut Harness) -> anyhow::Result<()> {
        super::wait_readers_connected(h, CTX).await?;
        let coverage = CacheRecovery::capture(h).await?;
        let saved = SavedJob::capture(&h.nomad, TASK).await?;
        let primary = live_primary(h, CTX).await?;
        let writes = WritePause::capture(h).await?;
        let baseline = MirrorRebuilds::read(h).await?;
        let mut fault = InterruptedRebuild {
            h,
            saved,
            primary,
            writes,
            baseline,
            blocked: None,
            targets: Vec::new(),
            traffic_hash: None,
        };
        let verdict = tokio::time::timeout(Duration::from_mins(9), fault.exercise())
            .await
            .context("mirror scan interruption exceeds nine minutes")
            .and_then(std::convert::identity);
        let unpaused = fault.writes.unpause(fault.h).await;
        let restored = fault.saved.restore().await;
        let report = fault.report(&verdict, &unpaused, &restored);
        for error in [unpaused.as_ref().err(), restored.as_ref().err()]
            .into_iter()
            .flatten()
        {
            crate::log(format!("{CTX}: fault cleanup fails: {error:#}"));
        }
        verdict?;
        unpaused?;
        restored?;
        report?;
        fault
            .h
            .assert_count(TASK, MIRROR_ALLOCS, fault.h.knobs.restart_slo)
            .await?;
        super::wait_mirror_advances(fault.h, CTX).await?;
        coverage.await_restored(fault.h, CTX).await?;
        super::wait_readers_recovered(fault.h, CTX).await?;
        fault.h.assert_progress().await
    }

    async fn exercise(&mut self) -> anyhow::Result<()> {
        self.saved.stop().await?;
        let flushed = freeze_and_flush(self.h, CTX, &[]).await?;
        anyhow::ensure!(
            flushed == self.primary,
            "{CTX}: primary changed before the write pause"
        );
        self.writes.await_cold(self.h).await?;
        self.writes.pause(self.h).await?;
        self.saved.restore().await?;
        self.capture_targets().await?;
        self.blocked = Some(
            self.baseline
                .await_phase(self.h, CTX, RebuildPhase::BlockedScan)
                .await?,
        );
        self.verify_targets().await?;
        super::wait_readers_degraded(self.h, CTX).await?;
        self.prove_traffic().await?;
        self.verify_targets().await?;
        self.writes.require_cold(self.h).await?;
        crate::log(format!(
            "{CTX}: every mirror is inside an unfinished checkpoint scan with a failed write; killing {} tasks",
            self.targets.len()
        ));
        self.kill_targets().await?;
        self.saved.stop().await
    }

    async fn verify_targets(&mut self) -> anyhow::Result<()> {
        for target in &mut self.targets {
            target.verify_blocked(self.h).await?;
        }
        Ok(())
    }

    async fn prove_traffic(&mut self) -> anyhow::Result<()> {
        let rpc = Rpc::new(&self.h.rpc_url, self.h.knobs.chain_id)?;
        let nonce = rpc.nonce_of(FAULT_ACCOUNT).await?;
        self.traffic_hash = Some(
            rpc.transfer_hash(FAULT_ACCOUNT, nonce, Duration::from_secs(60))
                .await?,
        );
        crate::log(format!(
            "{CTX}: a transfer from the unused cache-shard reserve {FAULT_ACCOUNT} receipts before the final blocked-scan check"
        ));
        Ok(())
    }

    async fn capture_targets(&mut self) -> anyhow::Result<()> {
        for (index, node) in self.h.probes.executors.clone().into_iter().enumerate() {
            self.capture_target(index, node).await?;
        }
        Ok(())
    }

    async fn capture_target(
        &mut self,
        index: usize,
        node: crate::probes::Probed,
    ) -> anyhow::Result<()> {
        let node = node.container;
        let cid = self
            .h
            .nodes
            .inner_cid(&node, TASK)
            .await
            .with_context(|| format!("{CTX}: no mirror container on {node}"))?;
        let started_at = self
            .h
            .nodes
            .started_at(&node, &cid)
            .await
            .with_context(|| format!("{CTX}: no start time for mirror on {node}"))?;
        self.targets.push(Target {
            index,
            node,
            cid,
            started_at,
            write_retries: None,
        });
        Ok(())
    }

    async fn kill_targets(&mut self) -> anyhow::Result<()> {
        let first = self.targets.first().context("no mirror target")?;
        for target in &self.targets {
            self.h.nodes.inner_kill(&target.node, &target.cid).await?;
        }
        self.h.killed = Some(Killed::Inner {
            node: first.node.clone(),
            task: TASK.into(),
            cid: first.cid.clone(),
        });
        Ok(())
    }

    fn report(
        &self,
        verdict: &anyhow::Result<()>,
        unpaused: &anyhow::Result<()>,
        restored: &anyhow::Result<()>,
    ) -> anyhow::Result<()> {
        let report = serde_json::json!({
            "case": CTX,
            "fault": "CLIENT PAUSE WRITE on both data nodes; kill every mirror after its scan reports a failed write and before completion",
            "max_pause_ms": WritePause::MAX_MS,
            "primary": self.primary,
            "paused_data_nodes": self.writes,
            "baseline": self.baseline,
            "blocked": self.blocked,
            "targets": self.targets,
            "fault_account": FAULT_ACCOUNT,
            "traffic_receipt_hash": self.traffic_hash,
            "failure": verdict.as_ref().err().map(|error| format!("{error:#}")),
            "unpause_failure": unpaused.as_ref().err().map(|error| format!("{error:#}")),
            "restore_failure": restored.as_ref().err().map(|error| format!("{error:#}")),
        });
        std::fs::write(
            "/tmp/chaos-mirror-kill-rebuild-fault.json",
            serde_json::to_vec_pretty(&report)?,
        )?;
        Ok(())
    }
}

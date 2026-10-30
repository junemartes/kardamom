//! Stop each mirror inside a checkpoint scan, with its Redis writes
//! blocked. The replacements run the saved job with no forced rebuild,
//! so the mirrors decide on their own. Each mirror died inside its
//! rebuild, after it wrote its rebuild marker, so each one must rebuild
//! again: every mirror completes a rebuild, the projection holds the
//! exact captured accounts, and every mirror serves before recovery
//! passes.

use anyhow::Context;
use serde::Serialize;
use std::num::NonZeroU64;
use std::time::Duration;

use super::rebuilds::{MirrorRebuilds, RebuildPhase};
use super::{
    CacheRecovery, MIRROR_ALLOCS, MIRROR_HEAD_KEYS, MIRROR_SERVING, MIRROR_WRITE_RETRIES,
    every_mirror_serves, flush_primary, live_primary,
};
use crate::harness::Harness;
use crate::inject::Killed;
use crate::nomad::SavedJob;
use crate::poll::{self, Budget};
use crate::rpc::Rpc;

mod writes;
use writes::WriteBlock;

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
        let body = h
            .probes
            .scrape()
            .fetch(&h.probes.mirror_target(self.index))
            .await
            .with_context(|| format!("{CTX}: no mirror scrape on {}", self.node))?;
        self.write_retries = Some(
            Self::blocked_scan(&body)
                .with_context(|| format!("{CTX}: blocked scan on {}", self.node))?,
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

    fn blocked_scan(body: &str) -> anyhow::Result<NonZeroU64> {
        let serving =
            crate::metrics::first(body, MIRROR_SERVING).context("missing serving metric")?;
        anyhow::ensure!(serving == 0, "mirror serves before interruption: {serving}");
        let retries = crate::metrics::first(body, MIRROR_WRITE_RETRIES)
            .context("missing write retry metric")?;
        NonZeroU64::new(u64::try_from(retries)?).context("mirror has retried no write")
    }
}

pub(super) struct InterruptedRebuild<'a> {
    h: &'a mut Harness,
    saved: SavedJob,
    primary: String,
    writes: WriteBlock,
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
        let baseline = MirrorRebuilds::read(h).await?;
        let writes = WriteBlock::capture(h).await?;
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
        let unblocked = fault.writes.restore(fault.h).await;
        let rebuilt = match (&verdict, &unblocked) {
            (Ok(()), Ok(())) => fault.recover(&coverage).await,
            _ => Ok(()),
        };
        let restored = fault.saved.restore().await;
        let report = fault.report(&verdict, &unblocked, &rebuilt, &restored);
        for error in [
            unblocked.as_ref().err(),
            rebuilt.as_ref().err(),
            restored.as_ref().err(),
        ]
        .into_iter()
        .flatten()
        {
            crate::log(format!("{CTX}: fault cleanup fails: {error:#}"));
        }
        verdict?;
        unblocked?;
        rebuilt?;
        restored?;
        report?;
        fault
            .h
            .assert_count(TASK, MIRROR_ALLOCS, fault.h.knobs.restart_slo)
            .await?;
        super::wait_mirror_advances(fault.h, CTX).await?;
        super::wait_readers_recovered(fault.h, CTX).await?;
        fault.h.assert_progress().await
    }

    async fn recover(&self, coverage: &CacheRecovery) -> anyhow::Result<()> {
        self.saved.restore().await?;
        coverage
            .await_restored(self.h, CTX, RebuildPhase::Complete)
            .await?;
        let h = &*self.h;
        poll::until(Budget::secs(60, 3), |_| async move {
            Ok::<_, anyhow::Error>(every_mirror_serves(h).await.then_some(()))
        })
        .await?
        .or_fail(|t| {
            crate::chaos_fail!(
                "{CTX}: a mirror does not serve {}s after the rebuild",
                t.as_secs()
            )
        })?;
        crate::log(format!("{CTX}: every mirror serves after the rebuild"));
        Ok(())
    }

    async fn exercise(&mut self) -> anyhow::Result<()> {
        self.saved.stop().await?;
        let flushed = flush_primary(self.h, CTX).await?;
        anyhow::ensure!(
            flushed == self.primary,
            "{CTX}: primary changed before the write fault"
        );
        self.writes.await_cold_heads(self.h).await?;
        self.writes.arm(self.h).await?;
        self.writes.verify(self.h).await?;
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
        self.writes.verify(self.h).await?;
        self.writes.require_cold_heads(self.h).await?;
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
        unblocked: &anyhow::Result<()>,
        rebuilt: &anyhow::Result<()>,
        restored: &anyhow::Result<()>,
    ) -> anyhow::Result<()> {
        let report = serde_json::json!({
            "case": CTX,
            "fault": "Deny Lua writes on both Redis data nodes; kill every mirror after its scan reports a failed write and before completion",
            "write_fault_lease_seconds": writes::LEASE_SECONDS,
            "write_fault": "EVAL and EVALSHA denied on both data processes",
            "primary": self.primary,
            "faulted_data_nodes": self.writes,
            "baseline": self.baseline,
            "blocked": self.blocked,
            "targets": self.targets,
            "fault_account": FAULT_ACCOUNT,
            "traffic_receipt_hash": self.traffic_hash,
            "failure": verdict.as_ref().err().map(|error| format!("{error:#}")),
            "write_restore_failure": unblocked.as_ref().err().map(|error| format!("{error:#}")),
            "rebuild_failure": rebuilt.as_ref().err().map(|error| format!("{error:#}")),
            "restore_failure": restored.as_ref().err().map(|error| format!("{error:#}")),
        });
        std::fs::write(
            "/tmp/chaos-mirror-kill-rebuild-fault.json",
            serde_json::to_vec_pretty(&report)?,
        )?;
        Ok(())
    }
}

#[cfg(test)]
mod tests;

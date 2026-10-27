//! A damaged local checkpoint must be quarantined before a peer image is adopted.

use anyhow::Context;
use serde_json::Value;
use std::time::Duration;

use crate::harness::Harness;
use crate::nodes::RunningProcess;
use crate::nomad::SavedJob;
use crate::poll::{self, Budget};

const CTX: &str = "checkpoint-corrupt-recover";
const FAULT: &str = include_str!("corrupt_checkpoint/fault.py");

pub(crate) struct CorruptCheckpoint {
    victim: String,
    previous: RunningProcess,
}

impl CorruptCheckpoint {
    pub(crate) async fn run(h: &mut Harness) -> anyhow::Result<()> {
        let victim = h.probes.executors[0].container.clone();
        super::wait_peer_checkpoint(h, &victim, CTX).await?;
        super::wait_peer_checkpoint(h, &h.probes.executors[1].container, CTX).await?;
        let fault = Self {
            previous: h
                .nodes
                .running_process(&victim, "executor")
                .await
                .context("checkpoint fault has no live victim")?,
            victim,
        };
        let saved = SavedJob::capture(&h.nomad, "executor").await?;
        let paused = Self::paused(saved.definition())?;
        let armed = fault.arm(h, &saved, &paused).await;
        let restored = saved.restore().await;
        let recovered = match (&armed, &restored) {
            (Ok(evidence), Ok(())) => {
                fault
                    .recover(
                        h,
                        evidence["checkpoint"]
                            .as_str()
                            .context("fault has no checkpoint name")?,
                    )
                    .await
            }
            _ => Ok(Value::Null),
        };
        fault.report(&armed, &restored, &recovered)?;
        armed?;
        restored?;
        recovered?;
        h.assert_executor_progress(Duration::from_mins(3)).await?;
        h.assert_executors_converged(CTX).await
    }

    fn report(
        &self,
        armed: &anyhow::Result<Value>,
        restored: &anyhow::Result<()>,
        recovered: &anyhow::Result<Value>,
    ) -> anyhow::Result<()> {
        let report = serde_json::json!({
            "case": CTX,
            "victim": self.victim,
            "previous_container": self.previous.cid,
            "previous_start": self.previous.started,
            "fault": armed.as_ref().ok(),
            "arm_error": armed.as_ref().err().map(|e| format!("{e:#}")),
            "restore_error": restored.as_ref().err().map(|e| format!("{e:#}")),
            "recovery": recovered.as_ref().ok(),
            "recovery_error": recovered.as_ref().err().map(|e| format!("{e:#}")),
        });
        std::fs::write(
            "/tmp/chaos-checkpoint-corrupt-recover-fault.json",
            serde_json::to_vec_pretty(&report)?,
        )?;
        Ok(())
    }

    /// The carry deployment pins one executor per group. Only its victim stops.
    fn paused(saved: &Value) -> anyhow::Result<Value> {
        let mut paused = saved.clone();
        let group = paused["TaskGroups"]
            .as_array_mut()
            .and_then(|groups| {
                groups
                    .iter_mut()
                    .find(|group| group["Name"] == "executor-0")
            })
            .context("checkpoint fault needs the pinned executor-0 group")?;
        anyhow::ensure!(
            group["Count"] == 1,
            "checkpoint victim must have one allocation"
        );
        group["Count"] = 0.into();
        Ok(paused)
    }

    async fn arm(&self, h: &Harness, saved: &SavedJob, paused: &Value) -> anyhow::Result<Value> {
        saved.register(paused).await?;
        poll::until(Budget::secs(120, 2), |_| async {
            let allocs = h.nomad.allocations("executor").await?;
            let stopped = allocs
                .iter()
                .filter(|a| a.task_group == "executor-0")
                .all(crate::nomad::Alloc::is_terminal);
            Ok((stopped
                && h.nodes
                    .running_process(&self.victim, "executor")
                    .await
                    .is_none())
            .then_some(()))
        })
        .await?
        .or_fail(|_| anyhow::anyhow!("{CTX}: victim still runs; refusing to change its files"))?;
        let baseline = tokio::time::timeout(Duration::from_secs(120), self.baseline(h))
            .await
            .context("checkpoint baseline verification exceeds 120s")??;
        let image: alloy_primitives::B256 = baseline["original_sha256"]
            .as_str()
            .context("missing baseline image hash")?
            .parse()?;
        let manifest: alloy_primitives::B256 = baseline["manifest_sha256"]
            .as_str()
            .context("missing baseline manifest hash")?
            .parse()?;
        let arm = format!("arm {image:x} {manifest:x}");
        let report = h.nodes.exec(&self.victim, &Self::script(&arm)).await?;
        let mut report: Value = serde_json::from_str(&report)?;
        report["baseline_verified"] = true.into();
        crate::log(format!(
            "{CTX}: corrupted a stopped executor's checkpoint: {report}"
        ));
        Ok(report)
    }

    /// Validate the stopped node's image with the production restore path before changing it.
    async fn baseline(&self, h: &Harness) -> anyhow::Result<Value> {
        let raw = h.nodes.exec(&self.victim, &Self::script("inspect")).await?;
        let baseline: Value = serde_json::from_str(&raw)?;
        let name = baseline["checkpoint"]
            .as_str()
            .context("missing checkpoint name")?;
        let block = super::checkpoint_copy::CheckpointCopy::block(name)?;
        let restored = super::checkpoint_copy::CheckpointCopy {
            node: &self.victim,
            root: "/opt/kardamom/checkpoints",
            name,
            with_log: false,
        }
        .verify(h)
        .await?;
        anyhow::ensure!(
            restored == block,
            "checkpoint baseline does not restore at its declared block"
        );
        Ok(baseline)
    }

    fn script(mode: &str) -> String {
        format!("python3 - /opt/kardamom {mode} <<'PY'\n{FAULT}\nPY")
    }

    async fn recover(&self, h: &Harness, checkpoint: &str) -> anyhow::Result<Value> {
        let (process, elapsed) = poll::until(Budget::secs(120, 2), |_| async {
            let Some(process) = h.nodes.running_process(&self.victim, "executor").await else {
                return Ok(None);
            };
            let logs = h.nodes.process_logs(&self.victim, &process, 100_000).await.context("replacement executor logs are unavailable")?;
            Ok(self.proves_recovery(&process, &logs, checkpoint).then_some(process))
        }).await?.or_fail(|_| anyhow::anyhow!(
            "{CTX}: no new process rejects the corrupt image and restores a peer checkpoint in 120s"
        ))?;
        let proof = h.nodes.exec(&self.victim, &Self::script("verify")).await?;
        crate::log(format!(
            "{CTX}: quarantined image and unchanged manifest verified: {proof}"
        ));
        anyhow::ensure!(
            h.nodes
                .running_process(&self.victim, "executor")
                .await
                .as_ref()
                == Some(&process),
            "{CTX}: executor changed during recovery proof"
        );
        Ok(
            serde_json::json!({"container": process.cid, "started": process.started, "seconds": elapsed.as_secs_f64(), "quarantine": serde_json::from_str::<Value>(&proof)?}),
        )
    }

    fn proves_recovery(&self, process: &RunningProcess, logs: &str, checkpoint: &str) -> bool {
        let rejected = logs.lines().position(|line| {
            line.contains(checkpoint)
                && line.contains("CORRUPT")
                && line.contains("checkpoint failed verification; quarantining")
        });
        let fetched = logs.lines().position(|line| line.contains(super::FETCHED));
        let restored = logs.lines().position(|line| line.contains(super::RESTORED));
        *process != self.previous
            && matches!((rejected, fetched, restored), (Some(a), Some(b), Some(c)) if a < b && b < c)
            && !logs.contains("fresh start will replay from genesis")
    }
}

#[cfg(test)]
mod tests;

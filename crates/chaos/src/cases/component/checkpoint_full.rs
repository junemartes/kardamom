//! A full auxiliary recovery volume must not stop the authoritative state writer.

use std::time::{Duration, SystemTime};

use anyhow::Context;
use serde_json::Value;

use super::checkpoint_copy::CheckpointCopy;
use crate::harness::Harness;
use crate::nodes::RunningProcess;
use crate::nomad::SavedJob;
use crate::poll::{self, Budget};
use crate::rpc::{RecoveryCause, Rpc};

mod proof;
mod volume;
use proof::Progress;
use volume::Volume;

const CTX: &str = "checkpoint-disk-full";

pub(crate) struct CheckpointFull {
    node: String,
    volume: Volume,
}

impl CheckpointFull {
    pub(crate) async fn run(h: &mut Harness) -> anyhow::Result<()> {
        let fault = Self {
            node: h.probes.executors[0].container.clone(),
            volume: Volume {
                token: SystemTime::now()
                    .duration_since(SystemTime::UNIX_EPOCH)?
                    .as_nanos(),
            },
        };
        let saved = SavedJob::capture(&h.nomad, "executor").await?;
        let definition = fault.volume.job(saved.definition())?;
        let outcome = tokio::time::timeout(
            Duration::from_mins(8),
            fault.exercise(h, &saved, &definition),
        )
        .await
        .context("checkpoint ENOSPC proof exceeds eight minutes")
        .and_then(std::convert::identity);
        // Free space before restoring the job, even when the proof fails.
        let cleared = fault.command(h, "clear").await;
        let restored = saved.restore().await;
        let removed = match &restored {
            Ok(()) => fault.remove(h).await,
            Err(_) => Err(anyhow::anyhow!(
                "keep the cleared volume until the saved job runs"
            )),
        };
        let report = serde_json::json!({
            "case": CTX, "node": fault.node, "path": fault.volume.root(),
            "proof": outcome.as_ref().ok(),
            "failure": outcome.as_ref().err().map(|e| format!("{e:#}")),
            "clear_failure": cleared.as_ref().err().map(|e| format!("{e:#}")),
            "restore_failure": restored.as_ref().err().map(|e| format!("{e:#}")),
            "remove_failure": removed.as_ref().err().map(|e| format!("{e:#}")),
        });
        let written = std::fs::write(
            "/tmp/chaos-checkpoint-disk-full-fault.json",
            serde_json::to_vec_pretty(&report)?,
        );
        crate::log(format!("{CTX}: {report}"));
        outcome?;
        cleared?;
        restored?;
        removed?;
        written?;
        h.assert_executor_progress(Duration::from_mins(3)).await?;
        h.assert_executors_converged(CTX).await
    }

    async fn exercise(
        &self,
        h: &Harness,
        saved: &SavedJob,
        definition: &Value,
    ) -> anyhow::Result<Value> {
        self.command(h, "prepare").await?;
        saved.register(definition).await?;
        let process = h
            .nodes
            .running_process(&self.node, "executor")
            .await
            .context("checkpoint victim has no process")?;
        self.volume
            .assert_bind(h, &self.node, &process.cid, true)
            .await?;
        let baseline = self.checkpoint(h, 0).await?;
        self.verify(h, &baseline, false).await?;
        let before = self.progress(h, &process).await?;
        let armed = self.command(h, "arm").await?;
        let failed = poll::until(Budget::secs(90, 2), |_| async {
            let current = self.progress(h, &process).await?;
            Ok(current.failed_after(&before).then_some(current))
        })
        .await?
        .or_fail(|_| anyhow::anyhow!("{CTX}: no executor checkpoint and block-log ENOSPC in 90s"))?
        .0;
        Rpc::new(&h.rpc_url, h.knobs.chain_id)?
            .transfer_within(
                h.knobs.gate_account,
                Duration::from_secs(60),
                RecoveryCause::NoQuorum,
            )
            .await?;
        let during = poll::until(Budget::secs(30, 2), |_| async {
            let current = self.progress(h, &process).await?;
            Ok(current.continued_during(&failed).then_some(current))
        })
        .await?
        .or_fail(|_| {
            anyhow::anyhow!("{CTX}: the same executor stops applying transactions during ENOSPC")
        })?
        .0;
        let held = self.command(h, "inspect").await?;
        anyhow::ensure!(
            held["available"] == 0,
            "{CTX}: fault released before the client proof"
        );
        let cleared = self.command(h, "clear").await?;
        anyhow::ensure!(
            cleared["available"].as_u64().is_some_and(|bytes| bytes > 0),
            "{CTX}: clearing the filler did not free space"
        );
        let recovered = self.checkpoint(h, during.block).await?;
        // The restored block must include log records after the new base.
        tokio::time::sleep(Duration::from_secs(3)).await;
        let restored_block = self.verify(h, &recovered, true).await?;
        let after = self.progress(h, &process).await?;
        anyhow::ensure!(
            after.advanced_after(&during),
            "{CTX}: no same-process progress after release"
        );
        Ok(
            serde_json::json!({"container":process.cid, "started":process.started,
            "baseline":baseline, "armed":armed, "before":before, "failed":failed,
            "during":during, "held":held, "recovered":recovered,
            "restored_block":restored_block, "after":after}),
        )
    }

    async fn progress(&self, h: &Harness, process: &RunningProcess) -> anyhow::Result<Progress> {
        anyhow::ensure!(
            h.nodes
                .running_process(&self.node, "executor")
                .await
                .as_ref()
                == Some(process),
            "{CTX}: victim process changed during the fault"
        );
        let metrics = h
            .probes
            .exec_metrics(0)
            .await
            .context("victim metrics unavailable")?;
        let logs = h
            .nodes
            .process_logs(&self.node, process, 100_000)
            .await
            .context("victim logs unavailable")?;
        Progress::read(&metrics, &logs)
    }

    async fn checkpoint(&self, h: &Harness, after: i64) -> anyhow::Result<Value> {
        let block = u64::try_from(after).context("negative executor block")?;
        Ok(poll::until(Budget::secs(90, 2), |_| async {
            let image = self.command(h, "inspect").await?;
            let newer = image["checkpoint"]
                .as_str()
                .map(CheckpointCopy::block)
                .transpose()?
                .is_some_and(|b| b > block);
            Ok(newer.then_some(image))
        })
        .await?
        .or_fail(|_| anyhow::anyhow!("{CTX}: no completed checkpoint above block {block} in 90s"))?
        .0)
    }

    async fn verify(&self, h: &Harness, image: &Value, with_log: bool) -> anyhow::Result<u64> {
        let name = image["checkpoint"].as_str().context("missing checkpoint")?;
        let root = self.volume.root();
        let copy = CheckpointCopy {
            node: &self.node,
            root: &root,
            name,
            with_log,
        };
        let restored = tokio::time::timeout(Duration::from_secs(120), copy.verify(h))
            .await
            .context("checkpoint copy verification exceeds 120s")??;
        let base = CheckpointCopy::block(name)?;
        anyhow::ensure!(
            if with_log {
                restored > base
            } else {
                restored == base
            },
            "{CTX}: restored block {restored} does not prove image/log recovery from base {base}"
        );
        Ok(restored)
    }

    async fn command(&self, h: &Harness, operation: &str) -> anyhow::Result<Value> {
        let text = h
            .nodes
            .exec(&self.node, &self.volume.script(operation))
            .await?;
        serde_json::from_str(&text).context("invalid checkpoint volume evidence")
    }

    async fn remove(&self, h: &Harness) -> anyhow::Result<Value> {
        let process = h
            .nodes
            .running_process(&self.node, "executor")
            .await
            .context("restored victim has no running process")?;
        self.volume
            .assert_bind(h, &self.node, &process.cid, false)
            .await?;
        poll::until(Budget::secs(120, 2), |_| async {
            let status = self.command(h, "unused").await?;
            Ok((status["unused"] == true).then_some(()))
        })
        .await?
        .or_fail(|_| anyhow::anyhow!("{CTX}: an old task still binds the temporary volume"))?;
        self.command(h, "remove").await
    }
}

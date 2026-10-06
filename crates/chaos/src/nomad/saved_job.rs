//! Preserve the full registered job while temporarily stopping its writers.

use anyhow::Context;
use serde_json::Value;

use super::Nomad;
use crate::poll::{self, Budget};

pub(crate) struct SavedJob {
    nomad: Nomad,
    id: String,
    definition: Value,
}

impl SavedJob {
    pub(crate) async fn capture(nomad: &Nomad, id: &str) -> anyhow::Result<Self> {
        let definition: Value = nomad.read(&format!("/v1/job/{id}")).await?;
        anyhow::ensure!(definition["Stop"] != true, "job {id} is already stopped");
        Ok(Self {
            nomad: nomad.clone(),
            id: id.into(),
            definition,
        })
    }

    pub(crate) async fn stop(&self) -> anyhow::Result<()> {
        self.nomad
            .http
            .delete(self.nomad.url(&format!("/v1/job/{}", self.id)))
            .send()
            .await?
            .error_for_status()
            .context("stop job for state audit")?;
        let outcome = poll::until(Budget::secs(120, 2), |_| async {
            let allocs = self.nomad.allocations(&self.id).await?;
            Ok(allocs
                .iter()
                .all(|a| matches!(a.client_status.as_str(), "complete" | "failed" | "lost"))
                .then_some(()))
        })
        .await?;
        outcome.or_fail(|_| anyhow::anyhow!("job {} still has writers after stop", self.id))?;
        Ok(())
    }

    pub(crate) async fn restore(&self) -> anyhow::Result<()> {
        self.register(&self.definition).await
    }

    /// Register the saved job with `extra` appended to the args of every
    /// task named `task`, and wait for it to run. An operator step runs a
    /// job once with one more flag this way; [`Self::restore`] removes the
    /// flag again.
    pub(crate) async fn restore_with_args(
        &self,
        task: &str,
        extra: &[String],
    ) -> anyhow::Result<()> {
        let mut definition = self.definition.clone();
        let appended = with_task_args(&mut definition, task, extra);
        anyhow::ensure!(appended > 0, "job {} has no task {task} with args", self.id);
        self.register(&definition).await
    }

    async fn register(&self, definition: &Value) -> anyhow::Result<()> {
        self.nomad
            .http
            .post(self.nomad.url("/v1/jobs"))
            .json(&serde_json::json!({"Job": definition}))
            .send()
            .await?
            .error_for_status()
            .context("restore job after state audit")?;
        let desired = self.nomad.job(&self.id).await?;
        let outcome = poll::until(Budget::secs(180, 3), |_| async {
            Ok(desired
                .running(&self.nomad.allocations(&self.id).await?)
                .map(|_| ()))
        })
        .await?;
        outcome
            .or_fail(|_| anyhow::anyhow!("job {} did not recover after state audit", self.id))?;
        Ok(())
    }
}

/// Append `extra` to the docker args of every task named `task` in the
/// job `definition`. Returns the number of tasks changed.
fn with_task_args(definition: &mut Value, task: &str, extra: &[String]) -> usize {
    definition["TaskGroups"]
        .as_array_mut()
        .into_iter()
        .flatten()
        .filter_map(|group| group["Tasks"].as_array_mut())
        .flatten()
        .filter(|t| t["Name"] == task)
        .filter_map(|t| t["Config"]["args"].as_array_mut())
        .map(|args| args.extend(extra.iter().cloned().map(Value::String)))
        .count()
}

#[cfg(test)]
mod tests;

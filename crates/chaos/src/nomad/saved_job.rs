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
            .with_context(|| format!("stop job {}", self.id))?;
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

    /// The definition Nomad held at the capture.
    pub(crate) fn definition(&self) -> &Value {
        &self.definition
    }

    /// Register the saved definition again, and wait until its
    /// allocations run.
    pub(crate) async fn restore(&self) -> anyhow::Result<()> {
        self.register(&self.definition).await
    }

    /// Register `definition` under the saved job's id, and wait until the
    /// allocations of that version run.
    pub(crate) async fn register(&self, definition: &Value) -> anyhow::Result<()> {
        self.nomad
            .http
            .post(self.nomad.url("/v1/jobs"))
            .json(&serde_json::json!({"Job": definition}))
            .send()
            .await?
            .error_for_status()
            .with_context(|| format!("register job {}", self.id))?;
        let desired = self.nomad.job(&self.id).await?;
        let outcome = poll::until(Budget::secs(180, 3), |_| async {
            Ok(desired
                .running(&self.nomad.allocations(&self.id).await?)
                .map(|_| ()))
        })
        .await?;
        outcome.or_fail(|_| {
            anyhow::anyhow!(
                "job {} version {} did not run after its registration",
                self.id,
                desired.version
            )
        })?;
        Ok(())
    }
}

#[cfg(test)]
mod tests;

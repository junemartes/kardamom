//! The job spec that an allocation runs, as `GET /v1/allocation/<id>`
//! returns it. Nomad can move an allocation to a new job version in
//! place, without a restart, when the spec of its group does not change.
//! So the job version of an allocation does not tell which image its task
//! runs. The task config in the spec of the allocation tells it.

use anyhow::Context;
use serde::Deserialize;

/// One allocation and the job spec that it runs.
#[derive(Debug, Deserialize)]
pub(crate) struct AllocSpec {
    #[serde(rename = "ID")]
    id: String,
    #[serde(rename = "TaskGroup")]
    group: String,
    #[serde(rename = "Job")]
    job: SpecJob,
}

#[derive(Debug, Deserialize)]
struct SpecJob {
    #[serde(rename = "TaskGroups")]
    groups: Vec<SpecGroup>,
}

#[derive(Debug, Deserialize)]
struct SpecGroup {
    #[serde(rename = "Name")]
    name: String,
    #[serde(rename = "Tasks")]
    tasks: Vec<SpecTask>,
}

#[derive(Debug, Deserialize)]
struct SpecTask {
    #[serde(rename = "Name")]
    name: String,
    /// The driver config. A docker task names its image here.
    #[serde(rename = "Config", default)]
    config: serde_json::Value,
}

impl AllocSpec {
    /// The image of the docker task `task` in the group of the
    /// allocation.
    ///
    /// # Errors
    ///
    /// Returns an error if the group of the allocation has no task
    /// `task` with an image.
    pub(crate) fn image(&self, task: &str) -> anyhow::Result<&str> {
        self.job
            .groups
            .iter()
            .filter(|g| g.name == self.group)
            .flat_map(|g| &g.tasks)
            .find(|t| t.name == task)
            .and_then(|t| t.config["image"].as_str())
            .with_context(|| {
                format!(
                    "allocation {} has no task {task} with an image in its group {}",
                    self.id, self.group
                )
            })
    }
}

#[cfg(test)]
mod tests;

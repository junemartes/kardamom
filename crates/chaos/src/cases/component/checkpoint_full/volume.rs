use anyhow::Context;
use serde_json::Value;

use crate::harness::Harness;

const SCRIPT: &str = include_str!("volume.py");
const ORIGINAL: &str = "/opt/kardamom/checkpoints";

pub(super) struct Volume {
    pub token: u128,
}

impl Volume {
    pub(super) fn root(&self) -> String {
        format!("/opt/kardamom/chaos-checkpoint-enospc-{}", self.token)
    }

    pub(super) fn script(&self, operation: &str) -> String {
        format!("python3 - {} {operation} <<'PY'\n{SCRIPT}\nPY", self.token)
    }

    pub(super) fn job(&self, saved: &Value) -> anyhow::Result<Value> {
        let mut patched = saved.clone();
        let group = patched["TaskGroups"]
            .as_array_mut()
            .and_then(|groups| {
                groups
                    .iter_mut()
                    .find(|group| group["Name"] == "executor-0")
            })
            .context("disk-full case requires pinned executor-0")?;
        anyhow::ensure!(
            group["Count"] == 1,
            "disk-full victim requires one allocation"
        );
        let task = group["Tasks"]
            .as_array_mut()
            .and_then(|tasks| tasks.iter_mut().find(|task| task["Name"] == "executor"))
            .context("victim has no executor task")?;
        anyhow::ensure!(
            task["Env"]["KARDAMOM_CHECKPOINT_LOG"] == "true",
            "disk-full proof requires the block log"
        );
        let volumes = task["Config"]["volumes"]
            .as_array_mut()
            .context("victim has no volumes")?;
        let original = format!("{ORIGINAL}:{ORIGINAL}");
        anyhow::ensure!(
            volumes
                .iter()
                .filter(|v| v.as_str() == Some(&original))
                .count()
                == 1,
            "victim must have exactly one original checkpoint bind"
        );
        let entry = volumes
            .iter_mut()
            .find(|v| v.as_str() == Some(&original))
            .context("missing checkpoint bind")?;
        *entry = format!("{}:{ORIGINAL}", self.root()).into();
        Ok(patched)
    }

    pub(super) async fn assert_bind(
        &self,
        h: &Harness,
        node: &str,
        cid: &str,
        temporary: bool,
    ) -> anyhow::Result<()> {
        let raw = h.nodes.exec(node, &format!("docker inspect {cid}")).await?;
        let inspect: Value = serde_json::from_str(&raw)?;
        let source = if temporary {
            self.root()
        } else {
            ORIGINAL.into()
        };
        let mounts = inspect[0]["Mounts"]
            .as_array()
            .context("victim has no Docker mounts")?;
        anyhow::ensure!(
            mounts
                .iter()
                .any(|m| m["Source"] == source && m["Destination"] == ORIGINAL && m["RW"] == true),
            "victim does not bind the expected checkpoint source {source}"
        );
        let devices = h
            .nodes
            .exec(
                node,
                &format!("stat -c %d {source}; docker exec {cid} stat -c %d {ORIGINAL}"),
            )
            .await?;
        let values: Vec<_> = devices.split_whitespace().collect();
        anyhow::ensure!(
            values.len() == 2 && values[0] == values[1],
            "victim bind does not reach the source filesystem"
        );
        Ok(())
    }
}

#[cfg(test)]
#[path = "tests.rs"]
mod tests;

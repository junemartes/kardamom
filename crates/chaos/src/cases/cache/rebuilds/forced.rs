//! A scan fixture requests rebuilding on every mirror instead of racing
//! the shared head that an earlier-starting mirror can already publish.

use anyhow::Context;
use serde_json::{Map, Value};

pub(crate) struct ForcedRebuild {
    definition: Value,
}

impl ForcedRebuild {
    pub(crate) fn new(saved: &Value) -> anyhow::Result<Self> {
        let mut definition = saved.clone();
        let groups = definition["TaskGroups"]
            .as_array_mut()
            .context("mirror job has no task groups")?;
        let changed = groups.iter_mut().try_fold(false, |changed, group| {
            Self::force_group(group).map(|forced| forced || changed)
        })?;
        anyhow::ensure!(changed, "mirror job has no state-mirror task");
        Ok(Self { definition })
    }

    pub(crate) fn definition(&self) -> &Value {
        &self.definition
    }

    fn force_group(group: &mut Value) -> anyhow::Result<bool> {
        group["Tasks"]
            .as_array_mut()
            .context("mirror group has no tasks")?
            .iter_mut()
            .try_fold(false, |changed, task| {
                Self::force_task(task).map(|forced| forced || changed)
            })
    }

    fn force_task(task: &mut Value) -> anyhow::Result<bool> {
        if task["Name"] != "state-mirror" {
            return Ok(false);
        }
        if task["Env"].is_null() {
            task["Env"] = Value::Object(Map::new());
        }
        task["Env"]
            .as_object_mut()
            .context("mirror task environment is not an object")?
            .insert(
                "KARDAMOM_MIRROR_REBUILD".into(),
                Value::String("true".into()),
            );
        Ok(true)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn forcing_a_scan_preserves_the_saved_job_and_other_tasks() {
        let saved = json!({"ID":"state-mirror","TaskGroups":[{"Count":3,"Tasks":[
            {"Name":"state-mirror","Env":{"AERON_DRIVER_TIMEOUT":"30000"},"Config":{"image":"pinned"}},
            {"Name":"sidecar","Env":{"keep":"value"}}
        ]}]});
        let mut expected = saved.clone();
        expected["TaskGroups"][0]["Tasks"][0]["Env"]["KARDAMOM_MIRROR_REBUILD"] = json!("true");
        let forced = ForcedRebuild::new(&saved).unwrap();
        assert_eq!(*forced.definition(), expected);
        assert!(
            saved["TaskGroups"][0]["Tasks"][0]["Env"]
                .get("KARDAMOM_MIRROR_REBUILD")
                .is_none()
        );
    }

    #[test]
    fn every_mirror_task_is_forced_including_an_absent_environment() {
        let saved = json!({"TaskGroups":[
            {"Tasks":[{"Name":"state-mirror"}]},
            {"Tasks":[{"Name":"state-mirror","Env":null}]}
        ]});
        let forced = ForcedRebuild::new(&saved).unwrap();
        assert!(
            forced.definition()["TaskGroups"]
                .as_array()
                .unwrap()
                .iter()
                .all(|group| { group["Tasks"][0]["Env"]["KARDAMOM_MIRROR_REBUILD"] == "true" })
        );
    }

    #[test]
    fn a_job_without_a_mirror_task_cannot_arm_the_scan_proof() {
        assert!(ForcedRebuild::new(&json!({"TaskGroups":[{"Tasks":[{"Name":"other"}]}]})).is_err());
        assert!(ForcedRebuild::new(&json!({"TaskGroups":[]})).is_err());
    }

    #[test]
    fn malformed_job_fields_fail_before_registration() {
        assert!(
            [
                json!({}),
                json!({"TaskGroups":[{}]}),
                json!({"TaskGroups":[{"Tasks":[{"Name":"state-mirror","Env":"bad"}]}]})
            ]
            .iter()
            .all(|job| ForcedRebuild::new(job).is_err())
        );
    }
}

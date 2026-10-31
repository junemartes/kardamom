//! A targeted update of the jobs that one carry switch changes. The patch
//! makes the change that the job file renders for the switch, on the job
//! that Nomad holds, so the jobs that the switch does not touch keep
//! running. A patch that finds no task to change fails: the job file and
//! the patch disagree.

use serde_json::Value;

use crate::harness::Harness;
use crate::nomad::SavedJob;

/// The HTTP port of the egress (`ports.egress_http` in
/// `group_vars/all.yml`).
const EGRESS_HTTP_PORT: u16 = 8548;

/// One change of the args of the docker tasks of a job.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum Edit {
    /// Set the value after `flag` in every task that passes the flag.
    Value { flag: &'static str, value: String },
    /// Append `args` to every task that passes `marker`. A task that
    /// passes the first of `args` already keeps its args.
    Append {
        marker: &'static str,
        args: Vec<String>,
    },
}

impl Edit {
    /// Change `args`, and tell whether the task is one that the edit
    /// selects.
    fn apply(&self, args: &mut Vec<Value>) -> bool {
        match self {
            Self::Value { flag, value } => args
                .iter()
                .position(|a| a == flag)
                .and_then(|at| args.get_mut(at.checked_add(1)?))
                .map(|slot| *slot = value.as_str().into())
                .is_some(),
            Self::Append {
                marker,
                args: extra,
            } => {
                let selected = args.iter().any(|a| a == marker);
                let present = extra
                    .first()
                    .is_some_and(|first| args.iter().any(|a| a == first));
                if selected && !present {
                    args.extend(extra.iter().map(|a| Value::from(a.as_str())));
                }
                selected
            }
        }
    }
}

/// The change of one job: the job metas to set, and the arg edits of its
/// tasks, in one group or in all of them.
#[derive(Debug, Clone)]
pub(super) struct JobPatch {
    job: &'static str,
    group: Option<String>,
    metas: Vec<&'static str>,
    edits: Vec<Edit>,
}

impl JobPatch {
    pub(super) fn new(job: &'static str) -> Self {
        Self {
            job,
            group: None,
            metas: Vec::new(),
            edits: Vec::new(),
        }
    }

    /// Change only the tasks of the group `name`.
    pub(super) fn in_group(self, name: String) -> Self {
        Self {
            group: Some(name),
            ..self
        }
    }

    /// Set the job meta `key` to `"true"`.
    pub(super) fn meta(mut self, key: &'static str) -> Self {
        self.metas.push(key);
        self
    }

    pub(super) fn edit(mut self, edit: Edit) -> Self {
        self.edits.push(edit);
        self
    }

    /// The egress URL that the deploy gives a reader of the egress, in
    /// the datacenter of the egress job.
    ///
    /// # Errors
    ///
    /// Returns an error when Nomad does not hold the egress job.
    pub(super) async fn egress_url(h: &Harness) -> anyhow::Result<String> {
        let egress = SavedJob::capture(&h.nomad, "egress").await?;
        let datacenter = egress.definition()["Datacenters"][0]
            .as_str()
            .ok_or_else(|| crate::chaos_fail!("the egress job names no datacenter"))?
            .to_string();
        Ok(format!(
            "http://kardamom-egress.service.{datacenter}.consul:{EGRESS_HTTP_PORT}"
        ))
    }

    /// The args of the docker tasks that the patch may change.
    fn args_of<'a>(&self, definition: &'a mut Value) -> Vec<&'a mut Vec<Value>> {
        let group = self.group.as_deref();
        definition["TaskGroups"]
            .as_array_mut()
            .into_iter()
            .flatten()
            .filter(|g| group.is_none_or(|name| g["Name"] == name))
            .flat_map(|g| g["Tasks"].as_array_mut().into_iter().flatten())
            .filter_map(|t| t["Config"]["args"].as_array_mut())
            .collect()
    }

    /// Make the change on `definition`.
    ///
    /// # Errors
    ///
    /// Returns an error when an edit selects no task.
    pub(super) fn apply_to(&self, definition: &mut Value) -> anyhow::Result<()> {
        self.metas
            .iter()
            .for_each(|key| definition["Meta"][*key] = "true".into());
        let mut tasks = self.args_of(definition);
        self.edits.iter().try_for_each(|edit| {
            let selected = tasks
                .iter_mut()
                .map(|args| edit.apply(args))
                .filter(|selected| *selected)
                .count();
            anyhow::ensure!(
                selected > 0,
                "{}: the job {} has no task that {edit:?} selects",
                crate::FAIL_PREFIX,
                self.job
            );
            Ok(())
        })
    }

    /// Register the changed job, and wait until its allocations of the
    /// new version run.
    ///
    /// # Errors
    ///
    /// Returns an error when an edit selects no task, or when the new
    /// version does not run.
    pub(super) async fn register(&self, h: &Harness, ctx: &str) -> anyhow::Result<()> {
        let saved = SavedJob::capture(&h.nomad, self.job).await?;
        let mut definition = saved.definition().clone();
        self.apply_to(&mut definition)?;
        crate::log(format!(
            "{ctx}: register job {} with metas {:?} and {:?}",
            self.job, self.metas, self.edits
        ));
        saved.register(&definition).await
    }
}

#[cfg(test)]
#[path = "patch_tests.rs"]
mod tests;

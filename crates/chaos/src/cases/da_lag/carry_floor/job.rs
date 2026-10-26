use anyhow::Context;
use serde_json::Value;

pub(super) const SEGMENT_BLOCKS: u64 = 16;

/// A short reader window with the registered L1 safety floors intact.
pub(super) struct WindowJob(Value);

impl WindowJob {
    pub(super) fn new(saved: &Value, blocks: u64) -> anyhow::Result<Self> {
        let mut definition = saved.clone();
        let groups = definition["TaskGroups"]
            .as_array_mut()
            .context("egress job has no task groups")?;
        let changed = groups.iter_mut().try_fold(0usize, |count, group| {
            let tasks = group["Tasks"]
                .as_array_mut()
                .context("group has no tasks")?;
            tasks.iter_mut().try_fold(count, |count, task| {
                count
                    .checked_add(usize::from(Self::set(task, blocks)?))
                    .context("egress task count overflows")
            })
        })?;
        anyhow::ensure!(changed > 0, "egress job has no egress task");
        Ok(Self(definition))
    }

    fn set(task: &mut Value, blocks: u64) -> anyhow::Result<bool> {
        if task["Name"] != "egress" {
            return Ok(false);
        }
        let args = task["Config"]["args"]
            .as_array_mut()
            .context("egress task has no arguments")?;
        Self::set_arg(args, "--keep-blocks", blocks)?;
        Self::set_arg(args, "--segment-blocks", SEGMENT_BLOCKS)?;
        Ok(true)
    }

    fn set_arg(args: &mut [Value], flag: &str, value: u64) -> anyhow::Result<()> {
        let at = args
            .iter()
            .position(|arg| arg == flag)
            .with_context(|| format!("egress task has no {flag}"))?;
        let next = at.checked_add(1).context("argument index overflows")?;
        *args
            .get_mut(next)
            .with_context(|| format!("{flag} has no value"))? = value.to_string().into();
        Ok(())
    }

    pub(super) fn definition(&self) -> &Value {
        &self.0
    }
}

#[cfg(test)]
#[path = "job_tests.rs"]
mod tests;

//! The deploy cases: a rolling update of the executor job under its
//! readiness checks.

use std::time::Duration;

use anyhow::Context;
use serde_json::Value;

use crate::harness::Harness;
use crate::nomad::{Deployment, SavedJob};
use crate::poll::{self, Budget};

/// The manifest of the running cluster, written by the image push.
pub(crate) const MANIFEST: &str = "images.digests";

/// The job and the task that the case rolls.
const JOB: &str = "executor";
const TASK: &str = "executor";

/// The healthy deadline of the rollouts of the case. A real executor
/// passes its readiness check well inside it: the heal rolls the real
/// image under the same deadline and must succeed.
const HEALTHY_DEADLINE: Duration = Duration::from_secs(50);

/// The progress deadline of the rollouts of the case: above the healthy
/// deadline, as Nomad requires.
const PROGRESS_DEADLINE: Duration = Duration::from_secs(55);

/// How long a wait for the end of one rollout may take: its progress
/// deadline, the placement, and a margin for the starved runner.
const ROLLOUT_WAIT: Budget = Budget {
    timeout: Duration::from_secs(90),
    interval: Duration::from_secs(3),
};

/// A rollout of a broken executor image stops at its first replacement,
/// and the old replicas keep the chain advancing. The case takes the
/// executor job as Nomad holds it and changes one group: its executor
/// image becomes the DA stand-in's image, a real image whose entrypoint
/// is not an executor, so the new replica never passes its readiness
/// check. The update stanza of the job replaces one allocation at a time
/// and does not revert, so Nomad fails the deployment at its deadline
/// with the other replicas on the old image. The real spec under the
/// same deadlines then heals the group: the deadline is one that a real
/// executor meets. Last, the job gets back the update stanza it had.
pub(crate) async fn broken_image(h: &mut Harness) -> anyhow::Result<()> {
    let cluster_dir = h.lifecycle.cluster_dir().to_path_buf();
    let manifest = std::fs::read_to_string(cluster_dir.join(MANIFEST))
        .with_context(|| format!("read {MANIFEST}"))?;
    let manifest = Manifest::parse(&manifest);
    let broken_image = manifest.broken_image()?;
    let saved = SavedJob::capture(&h.nomad, JOB).await?;
    let rollout = Rollout::of(saved.definition(), broken_image.0)?;
    crate::log(format!(
        "deploy-broken-image: rolling group {} of the executor job to the DA stand-in image",
        rollout.group
    ));
    let broken = saved.submit(&rollout.broken).await?;
    let failed = rollout
        .await_end(h, broken.version, Deployment::failed)
        .await?;
    let (healthy, _) = failed.health(&rollout.group);
    anyhow::ensure!(
        healthy == 0,
        "{}: deploy-broken-image: {healthy} replica(s) of the broken image passed the readiness check",
        crate::FAIL_PREFIX
    );
    let images = h.nomad.running_images(JOB, TASK).await?;
    let old_running = broken_image.spared(&images);
    anyhow::ensure!(
        old_running >= 2,
        "deploy-broken-image: {old_running} running executor(s) keep the old image; the failed rollout must stop after one"
    );
    h.assert_executor_progress(Duration::from_mins(1)).await?;
    crate::log(format!(
        "deploy-broken-image: the rollout failed ({}); {old_running} executors on the old image keep the chain advancing",
        failed.description
    ));
    let healed = saved.submit(&rollout.healed).await?;
    rollout
        .await_end(h, healed.version, Deployment::successful)
        .await?;
    crate::log("deploy-broken-image: the real image passes under the same deadlines");
    saved.restore().await?;
    h.assert_count(JOB, 3, h.knobs.reschedule_slo).await?;
    h.assert_executor_progress(Duration::from_mins(1)).await
}

/// The two specs of the case: the job with one group on the broken
/// image, and the job with that group healed. Both carry the short
/// deadlines in the rolled group.
struct Rollout {
    group: String,
    broken: Value,
    healed: Value,
}

impl Rollout {
    /// The rollout of `definition`, the executor job as Nomad holds it,
    /// with the first group of the executor task on `image`.
    fn of(definition: &Value, image: &str) -> anyhow::Result<Self> {
        let mut healed = definition.clone();
        let group = Self::first_group(&mut healed)?;
        let name = group["Name"]
            .as_str()
            .context("an executor group has no name")?
            .to_string();
        let deadlines = &mut group["Update"];
        deadlines["HealthyDeadline"] = Self::nanos(HEALTHY_DEADLINE).into();
        deadlines["ProgressDeadline"] = Self::nanos(PROGRESS_DEADLINE).into();
        let mut broken = healed.clone();
        let task = Self::first_group(&mut broken)?["Tasks"]
            .as_array_mut()
            .and_then(|tasks| tasks.iter_mut().find(|t| t["Name"] == TASK))
            .context("the executor group has no executor task")?;
        task["Config"]["image"] = image.into();
        Ok(Self {
            group: name,
            broken,
            healed,
        })
    }

    /// The first task group of `job` that runs the executor task.
    fn first_group(job: &mut Value) -> anyhow::Result<&mut Value> {
        job["TaskGroups"]
            .as_array_mut()
            .and_then(|groups| {
                groups.iter_mut().find(|g| {
                    g["Tasks"]
                        .as_array()
                        .is_some_and(|tasks| tasks.iter().any(|t| t["Name"] == TASK))
                })
            })
            .context("the executor job has no group with the executor task")
    }

    /// A duration in the nanoseconds of the Nomad job API.
    fn nanos(d: Duration) -> u64 {
        u64::try_from(d.as_nanos()).unwrap_or(u64::MAX)
    }

    /// Wait until the deployment of `version` ends, and require the end
    /// that `expected` accepts.
    async fn await_end(
        &self,
        h: &Harness,
        version: u64,
        expected: fn(&Deployment) -> bool,
    ) -> anyhow::Result<Deployment> {
        let outcome = poll::until(ROLLOUT_WAIT, |_| async move {
            let latest = h.nomad.latest_deployment(JOB).await?;
            Ok(latest.filter(|d| d.job_version == version && (d.failed() || d.successful())))
        })
        .await?;
        let (deployment, _) = outcome.or_fail(|t| {
            crate::chaos_fail!(
                "deploy-broken-image: the rollout of executor job version {version} did not end within {}s",
                t.as_secs()
            )
        })?;
        anyhow::ensure!(
            expected(&deployment),
            "{}: deploy-broken-image: the rollout of group {} at version {version} ended {:?}: {}",
            crate::FAIL_PREFIX,
            self.group,
            deployment.health(&self.group),
            deployment.description
        );
        Ok(deployment)
    }
}

/// The image manifest: one `<service> <ref>` record per line.
struct Manifest<'a> {
    lines: Vec<&'a str>,
}

impl<'a> Manifest<'a> {
    fn parse(text: &'a str) -> Self {
        Self {
            lines: text.lines().collect(),
        }
    }

    /// The image reference of `service`.
    fn image(&self, service: &str) -> anyhow::Result<&'a str> {
        self.lines
            .iter()
            .filter_map(|l| l.split_once(' '))
            .find(|(name, _)| *name == service)
            .map(|(_, image)| image.trim())
            .with_context(|| format!("no {service} record in the manifest"))
    }

    /// The executor image of the broken manifest: the DA stand-in's
    /// image.
    fn broken_image(&self) -> anyhow::Result<BrokenImage<'a>> {
        self.image("da-store").map(BrokenImage)
    }
}

/// The image reference that the broken deploy gives the executor.
struct BrokenImage<'a>(&'a str);

impl BrokenImage<'_> {
    /// The number of `images` that are not the broken image: the
    /// executors that keep the old image. The job version does not tell
    /// this. The deploy rolls pinned executor groups one at a time, and
    /// Nomad moves a group that the step does not change to the new job
    /// version in place, on the old image.
    fn spared(&self, images: &[String]) -> usize {
        images.iter().filter(|image| *image != self.0).count()
    }
}

#[cfg(test)]
mod tests {
    use super::{Manifest, Rollout};

    /// A manifest with an executor and a DA stand-in record.
    const TEXT: &str =
        "executor r/kardamom-executor:t@sha256:aa\nda-store r/kardamom-da-store:t@sha256:bb\n";

    #[test]
    fn the_rollout_changes_only_the_image_and_deadlines_of_the_first_executor_group() {
        let job = serde_json::json!({"TaskGroups": [
            {"Name": "executor-0", "Update": {"HealthyDeadline": 300_000_000_000_u64},
             "Tasks": [{"Name": "executor", "Config": {"image": "r/exec@sha256:aa"}},
                       {"Name": "recorder", "Config": {"image": "r/rec@sha256:cc"}}]},
            {"Name": "executor-1", "Update": {},
             "Tasks": [{"Name": "executor", "Config": {"image": "r/exec@sha256:aa"}}]}
        ]});
        let rollout = Rollout::of(&job, "r/da@sha256:bb").unwrap();
        assert_eq!(rollout.group, "executor-0");
        let group = &rollout.broken["TaskGroups"][0];
        assert_eq!(group["Tasks"][0]["Config"]["image"], "r/da@sha256:bb");
        assert_eq!(group["Tasks"][1]["Config"]["image"], "r/rec@sha256:cc");
        assert_eq!(group["Update"]["HealthyDeadline"], 50_000_000_000_u64);
        assert_eq!(group["Update"]["ProgressDeadline"], 55_000_000_000_u64);
        assert_eq!(rollout.broken["TaskGroups"][1], job["TaskGroups"][1]);
        let healed = &rollout.healed["TaskGroups"][0];
        assert_eq!(healed["Tasks"][0]["Config"]["image"], "r/exec@sha256:aa");
        assert_eq!(healed["Update"]["HealthyDeadline"], 50_000_000_000_u64);
        assert!(Rollout::of(&serde_json::json!({"TaskGroups": []}), "x").is_err());
    }

    #[test]
    fn only_the_executors_off_the_broken_image_count_as_old() {
        let broken = Manifest::parse(TEXT).broken_image().unwrap();
        let (old, bad) = (
            "r/kardamom-executor:t@sha256:aa".to_string(),
            "r/kardamom-da-store:t@sha256:bb".to_string(),
        );
        assert_eq!(broken.spared(&[bad.clone(), old.clone(), old.clone()]), 2);
        assert_eq!(broken.spared(&[old.clone(), old]), 2);
        assert_eq!(broken.spared(&[bad]), 0);
    }
}

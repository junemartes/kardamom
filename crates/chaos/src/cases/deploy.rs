//! The deploy cases: the rolling deploy under its readiness checks.

use std::cell::RefCell;
use std::time::Duration;

use anyhow::Context;

use crate::cases::chain_status::ChainView;
use crate::harness::Harness;
use crate::lifecycle::{LastAttempt, NOMAD_HTTP_PORT};
use crate::poll::{self, Budget};

/// The manifest of the running cluster, written by the image push.
pub(crate) const MANIFEST: &str = "images.digests";
/// The broken copy the case deploys, beside the real one.
const BROKEN_MANIFEST: &str = "images.digests.broken";

/// A deploy of a broken executor image stops at the executor job, and
/// the old replicas keep the chain advancing. The executor line of the
/// manifest points at the DA stand-in's image: a real image whose
/// entrypoint is not an executor, so the new replica never passes its
/// readiness check. Nomad fails the deployment at its healthy deadline,
/// the role fails the play there, and the deploy never reaches the jobs
/// after the executor. The deploy of the real manifest then heals the
/// job: a rolling replacement of every executor under the same checks.
/// The release gate refuses a deploy while the chain stands on a halt or
/// a pause, so each deploy waits for an open gate first. The broken
/// deploy must fail at the executor rollout, not at the gate.
pub(crate) async fn broken_image(h: &mut Harness) -> anyhow::Result<()> {
    let nomad_addr = h.contract.nomad_addr(NOMAD_HTTP_PORT)?;
    let cluster_dir = h.lifecycle.cluster_dir().to_path_buf();
    let manifest = std::fs::read_to_string(cluster_dir.join(MANIFEST))
        .with_context(|| format!("read {MANIFEST}"))?;
    let manifest = Manifest::parse(&manifest);
    let broken_image = manifest.broken_image()?;
    let broken = manifest.broken_executor()?;
    std::fs::write(cluster_dir.join(BROKEN_MANIFEST), broken)
        .with_context(|| format!("write {BROKEN_MANIFEST}"))?;
    let version = h.nomad.job("executor").await?.version;

    await_gate_open(h, "the broken deploy").await?;
    crate::log("deploy-broken-image: deploying a manifest whose executor image is the DA stand-in");
    let broken_deploy = h
        .lifecycle
        .deploy(&nomad_addr, BROKEN_MANIFEST, LastAttempt::Settled)
        .await?;
    let _ = std::fs::remove_file(cluster_dir.join(BROKEN_MANIFEST));
    anyhow::ensure!(
        !broken_deploy,
        "deploy-broken-image: the deploy of a broken executor image succeeded"
    );
    let registered = h.nomad.job("executor").await?.version;
    anyhow::ensure!(
        registered > version,
        "{}: deploy-broken-image: the deploy failed before the executor rollout: the executor job stays at version {version}",
        crate::FAIL_PREFIX
    );
    let images = h.nomad.running_images("executor", "executor").await?;
    let old_running = broken_image.spared(&images);
    anyhow::ensure!(
        old_running >= 2,
        "deploy-broken-image: {old_running} running executor(s) keep the old image; the failed deploy must stop after one"
    );
    h.assert_executor_progress(Duration::from_mins(1)).await?;
    crate::log(format!(
        "deploy-broken-image: the deploy stopped at the executor job; {old_running} executors on the old image keep the chain advancing"
    ));

    await_gate_open(h, "the healing deploy").await?;
    crate::log("deploy-broken-image: deploying the real manifest to heal the executor job");
    let healed = h
        .lifecycle
        .deploy(&nomad_addr, MANIFEST, LastAttempt::Dead)
        .await?;
    anyhow::ensure!(
        healed,
        "deploy-broken-image: the deploy of the real manifest failed"
    );
    h.assert_count("executor", 3, h.knobs.reschedule_slo)
        .await?;
    h.assert_executor_progress(Duration::from_mins(1)).await
}

/// Wait until the release gate lets a deploy start: no root and no pause
/// on the chain status. An earlier case can leave a pause that its
/// recovery clears within the reschedule SLO.
async fn await_gate_open(h: &Harness, deploy: &str) -> anyhow::Result<()> {
    let last = RefCell::new(Vec::new());
    let last_ref = &last;
    let outcome = poll::until(
        Budget::new(h.knobs.reschedule_slo, Duration::from_secs(5)),
        |_| async move {
            let refusals = ChainView::read(h).await.map_or_else(
                |e| vec![format!("no chain status ({e:#})")],
                |view| view.gate_refusals(),
            );
            let open = refusals.is_empty();
            *last_ref.borrow_mut() = refusals;
            Ok::<_, anyhow::Error>(open.then_some(()))
        },
    )
    .await?;
    let ((), elapsed) = outcome.or_fail(|t| {
        crate::chaos_fail!(
            "deploy-broken-image: the release gate stayed shut before {deploy} for {}s: {}",
            t.as_secs(),
            last.borrow().join(", ")
        )
    })?;
    crate::log(format!(
        "deploy-broken-image: the release gate is open before {deploy} (after {}s)",
        elapsed.as_secs()
    ));
    Ok(())
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

    /// The manifest with the executor record pointing at the DA
    /// stand-in's image.
    fn broken_executor(&self) -> anyhow::Result<String> {
        let BrokenImage(wrong) = self.broken_image()?;
        let text: String = self
            .lines
            .iter()
            .map(|l| match l.split_once(' ') {
                Some(("executor", _)) => format!("executor {wrong}\n"),
                _ => format!("{l}\n"),
            })
            .collect();
        Ok(text)
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
    use super::Manifest;

    /// A manifest with an executor and a DA stand-in record.
    const TEXT: &str =
        "executor r/kardamom-executor:t@sha256:aa\nda-store r/kardamom-da-store:t@sha256:bb\n";

    #[test]
    fn the_broken_manifest_swaps_only_the_executor_image() {
        let broken = Manifest::parse(TEXT).broken_executor().unwrap();
        assert_eq!(
            broken,
            "executor r/kardamom-da-store:t@sha256:bb\nda-store r/kardamom-da-store:t@sha256:bb\n"
        );
        assert!(
            Manifest::parse("executor r/x@sha256:aa\n")
                .broken_executor()
                .is_err()
        );
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

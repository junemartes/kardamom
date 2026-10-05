//! The deploy cases: the rolling deploy under its readiness checks.

use std::time::Duration;

use anyhow::Context;

use crate::harness::Harness;
use crate::lifecycle::NOMAD_HTTP_PORT;

/// The manifest of the running cluster, written by the image push.
const MANIFEST: &str = "images.digests";
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
pub(crate) async fn broken_image(h: &mut Harness) -> anyhow::Result<()> {
    let nomad_addr = h.contract.nomad_addr(NOMAD_HTTP_PORT)?;
    let cluster_dir = h.lifecycle.cluster_dir().to_path_buf();
    let manifest = std::fs::read_to_string(cluster_dir.join(MANIFEST))
        .with_context(|| format!("read {MANIFEST}"))?;
    let broken = Manifest::parse(&manifest).broken_executor()?;
    std::fs::write(cluster_dir.join(BROKEN_MANIFEST), broken)
        .with_context(|| format!("write {BROKEN_MANIFEST}"))?;
    let version = h.nomad.job("executor").await?.version;

    crate::log("deploy-broken-image: deploying a manifest whose executor image is the DA stand-in");
    let broken_deploy = h.lifecycle.deploy(&nomad_addr, BROKEN_MANIFEST).await?;
    let _ = std::fs::remove_file(cluster_dir.join(BROKEN_MANIFEST));
    anyhow::ensure!(
        !broken_deploy,
        "deploy-broken-image: the deploy of a broken executor image succeeded"
    );
    let old_running = h
        .nomad
        .allocations("executor")
        .await?
        .iter()
        .filter(|a| a.job_version == version && a.client_status == "running")
        .count();
    anyhow::ensure!(
        old_running >= 2,
        "deploy-broken-image: {old_running} executor(s) of the old version still run; the failed deploy must stop after one"
    );
    h.assert_executor_progress(Duration::from_mins(1)).await?;
    crate::log(format!(
        "deploy-broken-image: the deploy stopped at the executor job; {old_running} old executors keep the chain advancing"
    ));

    crate::log("deploy-broken-image: deploying the real manifest to heal the executor job");
    let healed = h.lifecycle.deploy(&nomad_addr, MANIFEST).await?;
    anyhow::ensure!(
        healed,
        "deploy-broken-image: the deploy of the real manifest failed"
    );
    h.assert_count("executor", 3, h.knobs.reschedule_slo)
        .await?;
    h.assert_executor_progress(Duration::from_mins(1)).await
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

    /// The manifest with the executor record pointing at the DA
    /// stand-in's image.
    fn broken_executor(&self) -> anyhow::Result<String> {
        let wrong = self.image("da-store")?;
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

#[cfg(test)]
mod tests {
    use super::Manifest;

    #[test]
    fn the_broken_manifest_swaps_only_the_executor_image() {
        let text =
            "executor r/kardamom-executor:t@sha256:aa\nda-store r/kardamom-da-store:t@sha256:bb\n";
        let broken = Manifest::parse(text).broken_executor().unwrap();
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
}

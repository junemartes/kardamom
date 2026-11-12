//! Diff and heal the selected segment pair, then restore only the victim segment.

use std::path::Path;

use anyhow::Context;

use super::{ARCHIVE_DIR, Segment, staging::SegmentStage, tail};
use crate::harness::Harness;

pub(super) struct Repair<'a> {
    pub(super) harness: &'a Harness,
    pub(super) tool: &'a Path,
    pub(super) victim: &'a str,
    pub(super) mirror: &'a str,
    pub(super) segment: &'a Segment,
}

impl Repair<'_> {
    pub(super) async fn run(&self) -> anyhow::Result<()> {
        let tmp = tempfile::tempdir().context("staging dir")?;
        self.stage(tmp.path()).await?;
        let source = tmp.path().join("mirror/dir");
        let dest = tmp.path().join("victim/dir");
        self.heal(&source, &dest).await?;
        let seg = &self.segment.name;
        let healed = std::fs::read(dest.join(seg)).context("read the healed segment")?;
        self.harness
            .nodes
            .exec_with_stdin(self.victim, &format!("cat > {ARCHIVE_DIR}/{seg}"), healed)
            .await
            .map_err(|e| {
                crate::chaos_fail!("archive-corruption: writing healed segment back failed: {e}")
            })
    }

    async fn stage(&self, into: &Path) -> anyhow::Result<()> {
        for (node, sub, name) in [
            (self.victim, "victim", &self.segment.name),
            (self.mirror, "mirror", &self.segment.mirror_name),
        ] {
            SegmentStage::new(name)
                .fetch(self.harness, node, &into.join(sub))
                .await?;
        }
        // Recording IDs are local to each archive; the repair tool pairs file names.
        if self.segment.mirror_name != self.segment.name {
            let source = into.join("mirror/dir");
            std::fs::copy(
                source.join(&self.segment.mirror_name),
                source.join(&self.segment.name),
            )
            .context("stage the mirror segment under the victim's name")?;
        }
        Ok(())
    }

    async fn heal(&self, source: &Path, dest: &Path) -> anyhow::Result<()> {
        let seg = self.segment.name.as_str();
        let diff = self
            .tool(&[
                "--diff",
                "--source-dir",
                source.to_str().unwrap_or(""),
                "--dest-dir",
                dest.to_str().unwrap_or(""),
            ])
            .await;
        anyhow::ensure!(
            diff.contains(seg),
            "{}: archive-corruption: --diff did not name the corrupted segment {seg}",
            crate::FAIL_PREFIX
        );
        let heal = self
            .tool(&[
                "--heal",
                "--segments",
                seg,
                "--no-verify",
                "--source-dir",
                source.to_str().unwrap_or(""),
                "--dest-dir",
                dest.to_str().unwrap_or(""),
            ])
            .await;
        anyhow::ensure!(
            heal.contains("healed segments=1"),
            "{}: archive-corruption: --heal did not repair the segment\n{}",
            crate::FAIL_PREFIX,
            tail(&heal, 20)
        );
        Ok(())
    }

    async fn tool(&self, args: &[&str]) -> String {
        tokio::process::Command::new(self.tool)
            .args(args)
            .output()
            .await
            .map(|o| {
                format!(
                    "{}{}",
                    String::from_utf8_lossy(&o.stdout),
                    String::from_utf8_lossy(&o.stderr)
                )
            })
            .unwrap_or_default()
    }
}

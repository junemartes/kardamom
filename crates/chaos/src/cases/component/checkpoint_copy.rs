//! A temporary copy passes through the production image and log verifier.

use anyhow::Context;
use std::process::{Output, Stdio};
use std::time::Duration;
use tokio::process::Command;

pub(super) struct CheckpointCopy<'a> {
    pub node: &'a str,
    pub root: &'a str,
    pub name: &'a str,
    pub with_log: bool,
}

impl CheckpointCopy<'_> {
    pub(super) fn block(name: &str) -> anyhow::Result<u64> {
        name.strip_prefix("checkpoint-")
            .context("invalid checkpoint name")?
            .parse()
            .context("invalid checkpoint block")
    }

    pub(super) async fn verify(&self) -> anyhow::Result<u64> {
        tokio::time::timeout(Duration::from_secs(120), self.copy_and_restore())
            .await
            .context("checkpoint copy verification exceeds 120s")?
    }

    async fn copy_and_restore(&self) -> anyhow::Result<u64> {
        Self::block(self.name)?;
        let directory = tempfile::tempdir()?;
        let copies = directory.path().join("checkpoints");
        std::fs::create_dir(&copies)?;
        self.copy(self.name, &copies).await?;
        if self.with_log {
            self.copy("log", &copies).await?;
        }
        tokio::task::spawn_blocking(move || {
            kardamom_state::restore_best_checkpoint(
                &copies,
                &directory.path().join("state"),
                kardamom_state::BlockRange::ANY,
                None,
            )?
            .map(|(block, _)| block)
            .context("checkpoint copy has no verified restore point")
        })
        .await?
    }

    async fn copy(&self, name: &str, destination: &std::path::Path) -> anyhow::Result<()> {
        let archive = tempfile::NamedTempFile::new()?;
        // Exec reads the running container's mount namespace, including the fault tmpfs.
        let copied = Command::new("docker")
            .args([
                "exec", "-e", "LC_ALL=C", self.node, "tar", "-C", self.root, "-cf", "-", "--", name,
            ])
            .stdout(Stdio::from(archive.reopen()?))
            .stderr(Stdio::piped())
            .kill_on_drop(true)
            .spawn()?
            .wait_with_output()
            .await?;
        Self::copied(&copied, name)?;
        let extracted = Command::new("tar")
            .arg("-xf")
            .arg(archive.path())
            .arg("-C")
            .arg(destination)
            .kill_on_drop(true)
            .output()
            .await?;
        anyhow::ensure!(
            extracted.status.success(),
            "checkpoint archive extraction failed: {}",
            String::from_utf8_lossy(&extracted.stderr)
        );
        Ok(())
    }

    fn copied(output: &Output, name: &str) -> anyhow::Result<()> {
        let errors = String::from_utf8_lossy(&output.stderr);
        // A live log can grow after tar reads its size. The captured prefix still passes
        // through the production log verifier. Missing files and every other error fail.
        let grew = name == "log"
            && output.status.code() == Some(1)
            && !errors.is_empty()
            && errors
                .lines()
                .all(|line| line.ends_with(": file changed as we read it"));
        anyhow::ensure!(
            output.status.success() || grew,
            "checkpoint archive read of {name} failed ({}): {errors}",
            output.status
        );
        Ok(())
    }
}

#[cfg(test)]
mod tests;

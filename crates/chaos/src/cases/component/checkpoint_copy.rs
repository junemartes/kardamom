//! A temporary copy passes through the production image and log verifier.

use anyhow::Context;

use crate::harness::Harness;

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

    pub(super) async fn verify(&self, h: &Harness) -> anyhow::Result<u64> {
        Self::block(self.name)?;
        let directory = tempfile::tempdir()?;
        let copies = directory.path().join("checkpoints");
        std::fs::create_dir(&copies)?;
        self.copy(h, self.name, &copies).await?;
        if self.with_log {
            self.copy(h, "log", &copies).await?;
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

    async fn copy(
        &self,
        h: &Harness,
        name: &str,
        destination: &std::path::Path,
    ) -> anyhow::Result<()> {
        h.nodes
            .docker_ok_long(&[
                "cp",
                &format!("{}:{}/{name}", self.node, self.root),
                destination.to_str().context("copy path is not UTF-8")?,
            ])
            .await?;
        Ok(())
    }
}

//! Transfer only the selected segment; CRC verification stays on the live archive.

use std::path::Path;

use anyhow::Context;

use crate::harness::Harness;

pub(super) struct SegmentStage<'a> {
    name: &'a str,
}

impl<'a> SegmentStage<'a> {
    pub(super) fn new(name: &'a str) -> Self {
        Self { name }
    }

    fn command(&self, root: &Path) -> String {
        let root = Self::quoted(&root.to_string_lossy());
        let member = Self::quoted(&format!("dir/{}", self.name));
        format!("tar -C {root} -cf - -- {member}")
    }

    fn quoted(value: &str) -> String {
        format!("'{}'", value.replace('\'', "'\\''"))
    }

    pub(super) async fn fetch(&self, h: &Harness, node: &str, into: &Path) -> anyhow::Result<()> {
        std::fs::create_dir_all(into).context("create staging dir")?;
        let tar = h
            .nodes
            .exec_bytes(node, &self.command(Path::new("/opt/kardamom/archive")), 1)
            .await
            .map_err(|e| {
                crate::chaos_fail!("archive-corruption: staging copy from {node} failed: {e}")
            })?;
        Self::unpack(into, tar).await
    }
    /// Extract a tar stream into a runner directory.
    pub(super) async fn unpack(into: &Path, tar: Vec<u8>) -> anyhow::Result<()> {
        use tokio::io::AsyncWriteExt;
        let mut child = tokio::process::Command::new("tar")
            .args(["-C", into.to_str().unwrap_or(""), "-xf", "-"])
            .stdin(std::process::Stdio::piped())
            .spawn()
            .context("spawn tar on the runner")?;
        let mut stdin = child.stdin.take().context("tar stdin")?;
        stdin.write_all(&tar).await.context("feed tar")?;
        stdin.shutdown().await.context("close tar stdin")?;
        let status = child.wait().await.context("wait for tar")?;
        anyhow::ensure!(
            status.success(),
            "tar extraction into {} failed: {status}",
            into.display()
        );
        Ok(())
    }
}

#[cfg(test)]
mod tests;

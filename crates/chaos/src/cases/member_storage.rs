//! A member's persistent files share a parent with the live executor archive.

use crate::harness::Harness;
use crate::nodes::Pipe;

pub(super) struct MemberStorage;

impl MemberStorage {
    // The shared driver owns archive/dir. It stays live during a member-only fault.
    const WIPE: &str = "mkdir -p seed && find cluster seed -mindepth 1 -delete && find archive -mindepth 1 -maxdepth 1 ! -name dir -exec rm -rf {} +";
    const COPY: &str = "tar --exclude='cluster-mark*' --exclude='archive-mark*' --exclude='archive/dir' -cf - cluster archive";
    const MANIFEST: &str = "find cluster archive -path archive/dir -prune -o -type f ! -name 'cluster-mark*' ! -name 'archive-mark*' -exec sha256sum {} +";

    pub(super) async fn wipe(h: &Harness, node: &str, ctx: &str) -> anyhow::Result<()> {
        h.nodes
            .exec(node, &format!("cd /opt/kardamom && {}", Self::WIPE))
            .await
            .map_err(|e| crate::chaos_fail!("{ctx}: could not wipe member state on {node}: {e}"))?;
        Ok(())
    }

    /// All member tasks stay stopped through the copy. The survivor's
    /// recording log and archive form one immutable recovery image.
    /// Node-local process marks and the shared executor archive stay out.
    pub(super) async fn copy_stopped(
        h: &Harness,
        donor: &str,
        victim: &str,
        ctx: &str,
    ) -> anyhow::Result<()> {
        let manifest = h
            .nodes
            .exec(
                donor,
                &format!(
                    "cd /opt/kardamom && test -s cluster/recording.log && {}",
                    Self::MANIFEST
                ),
            )
            .await?;
        let copied = Self::transfer(h, donor, victim, &manifest).await;
        if copied.is_err() {
            Self::wipe(h, victim, ctx).await?;
        }
        copied
    }

    async fn transfer(
        h: &Harness,
        donor: &str,
        victim: &str,
        manifest: &str,
    ) -> anyhow::Result<()> {
        let read = format!("cd /opt/kardamom && {}", Self::COPY);
        let copied = h
            .nodes
            .stream(&Pipe {
                from: donor,
                read: &read,
                to: victim,
                write: "cd /opt/kardamom && tar -xf -",
                max_read_status: 0,
            })
            .await?;
        h.nodes
            .exec_with_stdin(
                victim,
                "cd /opt/kardamom && sha256sum --quiet -c -",
                manifest.as_bytes().to_vec(),
            )
            .await?;
        crate::log(format!(
            "offline member copy: {donor} -> {victim}, {} bytes; all {} file hashes match",
            copied.bytes,
            manifest.lines().count(),
        ));
        Ok(())
    }
}

#[cfg(test)]
mod tests;

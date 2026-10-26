//! A member's persistent files share a parent with the live executor archive.

use crate::harness::Harness;

pub(super) struct MemberStorage;

impl MemberStorage {
    // The shared driver owns archive/dir. It stays live during a member-only fault.
    const WIPE: &str = "mkdir -p seed && find cluster seed -mindepth 1 -delete && find archive -mindepth 1 -maxdepth 1 ! -name dir -exec rm -rf {} +";

    pub(super) async fn wipe(h: &Harness, node: &str, ctx: &str) -> anyhow::Result<()> {
        h.nodes
            .exec(node, &format!("cd /opt/kardamom && {}", Self::WIPE))
            .await
            .map_err(|e| crate::chaos_fail!("{ctx}: could not wipe member state on {node}: {e}"))?;
        Ok(())
    }
}

#[cfg(test)]
mod tests;

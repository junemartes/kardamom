//! The format gate of a deploy. The deploy role compares the format
//! registry of its target tree with the registry of the accepted release,
//! and refuses a release by the findings. The role holds the policy; this
//! module computes the findings and prints them as JSON.

use std::path::PathBuf;

use anyhow::Context;
use kardamom_formats::{Comparison, Registry};

/// The two registries of a deploy: the accepted release's `formats.toml`
/// as the base, and the target tree's as the head.
pub struct FormatGate {
    /// The `formats.toml` of the accepted release.
    pub base: PathBuf,
    /// The root of the target tree: the directory that holds its
    /// `formats.toml`.
    pub root: PathBuf,
}

impl FormatGate {
    /// Every rule that the target breaks against the accepted release, as
    /// a JSON array. Each finding has `rule` (`rollback`, `rolling`,
    /// `mixed_fleet` or `retired`), `id`, `head` and `base`.
    ///
    /// # Errors
    ///
    /// Returns an error when a registry does not load.
    pub fn findings_json(&self) -> anyhow::Result<String> {
        let base = Registry::read(&self.base)
            .with_context(|| format!("read the accepted registry {}", self.base.display()))?;
        let head_path = self.root.join("formats.toml");
        let head = Registry::read(&head_path)
            .with_context(|| format!("read the target registry {}", head_path.display()))?;
        let findings = Comparison::new(&base, &head).findings();
        Ok(serde_json::to_string(&findings)?)
    }
}

#[cfg(test)]
#[path = "release_tests.rs"]
mod tests;

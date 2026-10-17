//! The format gate of a deploy. The deploy role compares the format
//! registry of its target tree with the registry of the accepted release,
//! and refuses a release by the findings. The role holds the policy; the
//! gate computes the findings.

use std::path::PathBuf;

use crate::{Comparison, Finding, Registry, RegistryError};

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
    /// Every rule that the target breaks against the accepted release.
    /// Each finding serializes with `rule` (`rollback`, `rolling`,
    /// `mixed_fleet` or `retired`), `id`, `head` and `base`.
    ///
    /// # Errors
    ///
    /// Returns an error when a registry does not load.
    pub fn findings(&self) -> Result<Vec<Finding>, RegistryError> {
        let base = Registry::read(&self.base)?;
        let head = Registry::read(&self.root.join("formats.toml"))?;
        Ok(Comparison::new(&base, &head).findings())
    }
}

#[cfg(test)]
#[path = "gate_tests.rs"]
mod tests;

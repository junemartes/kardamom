//! Assertions for the tests of the crates that own a format. Each one
//! reads the workspace registry and panics with a clear message when the
//! code and the registry disagree.

use crate::{Registry, Versions};

impl Registry {
    /// The versions of the format `id` in the workspace registry.
    ///
    /// # Panics
    /// Panics when the registry does not load or does not list `id`.
    #[must_use]
    pub fn workspace_versions(id: &str) -> Versions {
        let registry = Self::workspace().unwrap_or_else(|error| panic!("formats.toml: {error}"));
        registry
            .versions(id)
            .unwrap_or_else(|| panic!("formats.toml has no [format.{id}]"))
    }

    /// Assert that the workspace registry lists the format `id` at
    /// `expected`.
    ///
    /// # Panics
    /// Panics when the versions differ.
    pub fn assert_versions(id: &str, expected: Versions) {
        assert_eq!(
            Self::workspace_versions(id),
            expected,
            "the code and formats.toml disagree on [format.{id}]"
        );
    }

    /// Assert that the workspace registry lists the format `id` at exactly
    /// `version` for writes and reads.
    ///
    /// # Panics
    /// Panics when the versions differ.
    pub fn assert_exact(id: &str, version: u32) {
        Self::assert_versions(id, Versions::exact(version));
    }

    /// Assert that the layout part `part` of the format `id` has the
    /// fingerprint `actual` in the workspace registry.
    ///
    /// # Panics
    /// Panics when the registry does not load, or the fingerprints differ.
    pub fn assert_layout(id: &str, part: &str, actual: &str) {
        let registry = Self::workspace().unwrap_or_else(|error| panic!("formats.toml: {error}"));
        let listed = registry
            .0
            .format
            .get(id)
            .and_then(|format| format.layout.get(part));
        assert_eq!(
            listed.map(String::as_str),
            Some(actual),
            "the code and formats.toml disagree on [format.{id}.layout] {part}"
        );
    }

    /// Assert the fingerprint of the rkyv archived form of `T`: its size
    /// and alignment. The fingerprint covers the fixed part of the archive.
    /// It does not cover the data behind a vector or a string, the order of
    /// fields of one size, or the order of enum variants.
    ///
    /// # Panics
    /// Panics when the fingerprints differ.
    pub fn assert_rkyv_layout<T: rkyv::Archive>(id: &str, part: &str) {
        let actual = format!(
            "rkyv size {} align {}",
            size_of::<rkyv::Archived<T>>(),
            align_of::<rkyv::Archived<T>>()
        );
        Self::assert_layout(id, part, &actual);
    }
}

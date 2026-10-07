//! The format registry and the release check.
//!
//! `formats.toml` at the workspace root lists every stored or wire format
//! that one release writes and another release reads. For each format it
//! holds the version that a release writes and the range of versions that
//! it reads. `docs/formats.md` states the rules.
//!
//! [`Registry::parse`] is the one boundary. In a registry that it returns,
//! each format has `reads_min <= writes <= reads_max`, at least one code
//! location, and an activation version inside its read range. Each waiver
//! names a known format and gives a reason.

mod check;

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use serde::Deserialize;

pub use check::{Comparison, Finding, Rule};

/// The versions of one format that a release writes and reads.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Versions {
    /// The version that the release writes with its default configuration.
    pub writes: u32,
    /// The oldest version that the release reads.
    pub reads_min: u32,
    /// The newest version that the release reads.
    pub reads_max: u32,
}

impl Versions {
    /// A format that a release writes and reads only at `version`.
    #[must_use]
    pub const fn exact(version: u32) -> Self {
        Self {
            writes: version,
            reads_min: version,
            reads_max: version,
        }
    }
}

/// A file that defines a format, relative to the workspace root, and a
/// text in that file. The registry spells it `path#symbol`.
#[derive(Clone, Debug, PartialEq, Eq, Deserialize)]
#[serde(try_from = "String")]
struct Location {
    path: String,
    symbol: String,
}

impl TryFrom<String> for Location {
    type Error = RegistryError;

    fn try_from(text: String) -> Result<Self, Self::Error> {
        let (path, symbol) = text
            .split_once('#')
            .ok_or_else(|| RegistryError::Location(text.clone()))?;
        Ok(Self {
            path: path.to_owned(),
            symbol: symbol.to_owned(),
        })
    }
}

impl Location {
    /// True when the file under `root` exists and holds the symbol.
    fn found(&self, root: &Path) -> bool {
        std::fs::read_to_string(root.join(&self.path)).is_ok_and(|text| text.contains(&self.symbol))
    }
}

/// A writer version that an operator switches on with a flag.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct Activation {
    flag: String,
    writes: u32,
}

/// One `[format.<id>]` entry.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct Format {
    writes: u32,
    reads_min: u32,
    reads_max: u32,
    code: Vec<Location>,
    activation: Option<Activation>,
}

impl Format {
    fn versions(&self) -> Versions {
        Versions {
            writes: self.writes,
            reads_min: self.reads_min,
            reads_max: self.reads_max,
        }
    }

    fn reads(&self, version: u32) -> bool {
        (self.reads_min..=self.reads_max).contains(&version)
    }

    fn validate(&self, id: &str) -> Result<(), RegistryError> {
        if !self.reads(self.writes) {
            return Err(RegistryError::Order {
                id: id.to_owned(),
                versions: self.versions(),
            });
        }
        if self.code.is_empty() {
            return Err(RegistryError::NoCode(id.to_owned()));
        }
        match &self.activation {
            Some(activation) if !self.reads(activation.writes) => Err(RegistryError::Activation {
                id: id.to_owned(),
                flag: activation.flag.clone(),
                writes: activation.writes,
            }),
            _ => Ok(()),
        }
    }
}

/// A release that breaks a rule on purpose, for one version of one
/// format. `version` is the head `writes` under `one_way`, and the head
/// `reads_min` under `coordinated`.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct Waiver {
    version: u32,
    reason: String,
}

/// The file as TOML gives it, before the checks of [`Registry::parse`].
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct Table {
    format: BTreeMap<String, Format>,
    #[serde(default)]
    one_way: BTreeMap<String, Waiver>,
    #[serde(default)]
    coordinated: BTreeMap<String, Waiver>,
}

impl Table {
    fn waivers(&self, rule: Rule) -> &BTreeMap<String, Waiver> {
        match rule {
            Rule::Rollback => &self.one_way,
            Rule::Rolling => &self.coordinated,
        }
    }

    fn validate_waiver(&self, rule: Rule, id: &str, waiver: &Waiver) -> Result<(), RegistryError> {
        if !self.format.contains_key(id) {
            return Err(RegistryError::UnknownWaiver {
                table: rule.table(),
                id: id.to_owned(),
            });
        }
        if waiver.reason.trim().is_empty() {
            return Err(RegistryError::NoReason {
                table: rule.table(),
                id: id.to_owned(),
            });
        }
        Ok(())
    }
}

/// A parsed and checked format registry.
#[derive(Debug)]
pub struct Registry(Table);

impl Registry {
    /// Parse and check the text of a registry.
    ///
    /// # Errors
    /// Returns an error when the text is not a valid registry.
    pub fn parse(text: &str) -> Result<Self, RegistryError> {
        let table: Table = toml::from_str(text)?;
        table
            .format
            .iter()
            .try_for_each(|(id, format)| format.validate(id))?;
        Rule::ALL
            .into_iter()
            .flat_map(|rule| {
                table
                    .waivers(rule)
                    .iter()
                    .map(move |(id, waiver)| (rule, id, waiver))
            })
            .try_for_each(|(rule, id, waiver)| table.validate_waiver(rule, id, waiver))?;
        Ok(Self(table))
    }

    /// Read, parse and check the registry at `path`.
    ///
    /// # Errors
    /// Returns an error when the file cannot be read or is not a valid
    /// registry.
    pub fn read(path: &Path) -> Result<Self, RegistryError> {
        let text = std::fs::read_to_string(path).map_err(|source| RegistryError::Read {
            path: path.to_owned(),
            source,
        })?;
        Self::parse(&text)
    }

    /// The registry of this workspace: `formats.toml` at its root.
    ///
    /// # Errors
    /// Returns an error when the file cannot be read or is not a valid
    /// registry.
    pub fn workspace() -> Result<Self, RegistryError> {
        Self::read(&Self::root().join("formats.toml"))
    }

    /// The versions of the format `id`, if the registry lists it.
    #[must_use]
    pub fn versions(&self, id: &str) -> Option<Versions> {
        self.0.format.get(id).map(Format::versions)
    }

    /// Every code location that is not found under `root`, the directory
    /// of the registry file.
    #[must_use]
    pub fn missing_code(&self, root: &Path) -> Vec<String> {
        self.0
            .format
            .iter()
            .flat_map(|(id, format)| format.code.iter().map(move |location| (id, location)))
            .filter(|(_, location)| !location.found(root))
            .map(|(id, location)| {
                format!(
                    "format {id}: {} does not hold `{}`",
                    location.path, location.symbol
                )
            })
            .collect()
    }

    fn root() -> PathBuf {
        Path::new(env!("CARGO_MANIFEST_DIR")).join("../..")
    }
}

/// Why a registry does not load.
#[derive(Debug, thiserror::Error)]
pub enum RegistryError {
    #[error("read {path}: {source}")]
    Read {
        path: PathBuf,
        source: std::io::Error,
    },
    #[error("parse the registry: {0}")]
    Toml(#[from] toml::de::Error),
    #[error("a code location is `path#symbol`; got `{0}`")]
    Location(String),
    #[error("format {id}: reads_min <= writes <= reads_max is false for {versions:?}")]
    Order { id: String, versions: Versions },
    #[error("format {0}: the entry has no code location")]
    NoCode(String),
    #[error("format {id}: flag {flag} writes version {writes}, outside the read range")]
    Activation {
        id: String,
        flag: String,
        writes: u32,
    },
    #[error("[{table}.{id}] names no format")]
    UnknownWaiver { table: &'static str, id: String },
    #[error("[{table}.{id}] has no reason")]
    NoReason { table: &'static str, id: String },
}

#[cfg(test)]
mod tests;

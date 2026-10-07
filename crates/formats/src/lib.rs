//! The format registry and the release check.
//!
//! `formats.toml` at the workspace root lists every stored or wire format
//! that one release writes and another release reads. For each format it
//! holds the version that a release writes and the range of versions that
//! it reads. `docs/formats.md` states the rules.
//!
//! [`Registry::parse`] is the one boundary. In a registry that it returns,
//! each format has `reads_min <= writes <= reads_max`, at least one code
//! location, and an activation version inside its read range. Each
//! `one_way` and `coordinated` waiver names a format of the registry, each
//! `retired` waiver names a format that is not in it, and each waiver
//! gives a reason.
//!
//! [`Comparison::report`] compares any two registries: a pull request
//! against its base, or a deploy target against the deployed release.

mod check;
mod support;

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use serde::Deserialize;

pub use check::{Comparison, Finding, Report, Rule};

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
/// name in that file. The registry spells it `path#symbol`. It is a
/// pointer for the reader of the registry: the check finds the symbol as a
/// whole word, and nothing more.
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
    /// True when the file under `root` holds the symbol as a whole word:
    /// no identifier character touches either end of it.
    fn found(&self, root: &Path) -> bool {
        std::fs::read_to_string(root.join(&self.path)).is_ok_and(|text| {
            text.match_indices(&self.symbol).any(|(start, symbol)| {
                let before = text[..start].chars().next_back();
                let after = text[start + symbol.len()..].chars().next();
                !before.is_some_and(Self::is_word) && !after.is_some_and(Self::is_word)
            })
        })
    }

    fn is_word(c: char) -> bool {
        c.is_alphanumeric() || c == '_'
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
    /// True when two releases of a mixed fleet read each other's output.
    /// False for a file that only one process writes and reads.
    shared: bool,
    /// True when the data stays for all time, as a batch on L1 does. A
    /// release never stops reading an old version of it.
    #[serde(default)]
    permanent: bool,
    code: Vec<Location>,
    activation: Option<Activation>,
    /// The fingerprint of each part of a frozen layout, by part name. A
    /// test in the owning crate computes each one from the code.
    #[serde(default)]
    layout: BTreeMap<String, String>,
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

/// A waiver table of the registry.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Waivers {
    /// `[one_way.<id>]`: a rollback to the base is not safe.
    OneWay,
    /// `[coordinated.<id>]`: a rolling deploy is not safe.
    Coordinated,
    /// `[retired.<id>]`: the head drops a format of the base.
    Retired,
}

impl Waivers {
    const ALL: [Self; 3] = [Self::OneWay, Self::Coordinated, Self::Retired];

    /// The table name in `formats.toml`.
    #[must_use]
    pub const fn name(self) -> &'static str {
        match self {
            Self::OneWay => "one_way",
            Self::Coordinated => "coordinated",
            Self::Retired => "retired",
        }
    }
}

/// A release that breaks a rule on purpose, for one version of one
/// format. `version` is the version that the finding names: see
/// [`Finding::head`].
#[derive(Debug, PartialEq, Eq, Deserialize)]
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
    #[serde(default)]
    retired: BTreeMap<String, Waiver>,
}

impl Table {
    fn waivers(&self, table: Waivers) -> &BTreeMap<String, Waiver> {
        match table {
            Waivers::OneWay => &self.one_way,
            Waivers::Coordinated => &self.coordinated,
            Waivers::Retired => &self.retired,
        }
    }

    fn validate_waiver(
        &self,
        table: Waivers,
        id: &str,
        waiver: &Waiver,
    ) -> Result<(), RegistryError> {
        let listed = self.format.contains_key(id);
        if listed == (table == Waivers::Retired) {
            return Err(RegistryError::WaiverTarget {
                table: table.name(),
                id: id.to_owned(),
            });
        }
        if waiver.reason.trim().is_empty() {
            return Err(RegistryError::NoReason {
                table: table.name(),
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
        Waivers::ALL
            .into_iter()
            .flat_map(|kind| {
                table
                    .waivers(kind)
                    .iter()
                    .map(move |(id, waiver)| (kind, id, waiver))
            })
            .try_for_each(|(kind, id, waiver)| table.validate_waiver(kind, id, waiver))?;
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
    #[error(
        "[{table}.{id}]: a one_way or coordinated waiver names a format of the registry, and a retired waiver names a format that is not in it"
    )]
    WaiverTarget { table: &'static str, id: String },
    #[error("[{table}.{id}] has no reason")]
    NoReason { table: &'static str, id: String },
}

#[cfg(test)]
mod tests;

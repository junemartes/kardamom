//! The release check: the rules that a head registry breaks against a
//! base registry, and the new waivers in the head that cover them.
//!
//! The check is incremental when the base is the base of a pull request.
//! It is the release gate when the base is the registry of the deployed
//! release and the head is the registry of the deploy target.

use std::fmt;

use crate::{Format, Registry, Waiver, Waivers};

/// A rule between two releases of one format.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Rule {
    /// The base read range contains the version that the head writes.
    /// When it holds, a rollback from the head to the base is safe.
    Rollback,
    /// The head read range contains the version that the base writes.
    /// When it holds, a rolling deploy from the base to the head is safe.
    Rolling,
    /// For a shared format, the base reads what the head writes. When it
    /// does not hold, a base reader in a mixed fleet fails during a roll.
    MixedFleet,
    /// The head lists every format of the base.
    Retired,
}

impl Rule {
    /// The waiver table whose entries waive this rule.
    #[must_use]
    pub const fn waivers(self) -> Waivers {
        match self {
            Self::Rollback => Waivers::OneWay,
            Self::Rolling | Self::MixedFleet => Waivers::Coordinated,
            Self::Retired => Waivers::Retired,
        }
    }
}

/// A rule that the head breaks for one format.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Finding {
    pub rule: Rule,
    pub id: String,
    /// The version that a waiver names: the head `writes` for
    /// [`Rule::Rollback`] and [`Rule::MixedFleet`], the incompatible head read boundary
    /// for [`Rule::Rolling`], and the base `writes` for [`Rule::Retired`].
    pub head: u32,
    /// The base version that the rule compares with.
    pub base: u32,
}

impl fmt::Display for Finding {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let Self { id, head, base, .. } = self;
        match self.rule {
            Rule::Rollback => write!(
                f,
                "{id}: the head writes version {head}, outside the base read boundary {base}. A rollback to the base is not safe."
            ),
            Rule::Rolling => write!(
                f,
                "{id}: the base writes version {base}, outside the head read boundary {head}. A rolling deploy is not safe."
            ),
            Rule::MixedFleet => write!(
                f,
                "{id}: the format is shared, the head writes version {head}, outside the base read boundary {base}. A rolling deploy is not safe."
            ),
            Rule::Retired => write!(
                f,
                "{id}: the base lists the format at version {base}, and the head does not list it."
            ),
        }
    }
}

impl Format {
    /// The end of the read range nearest a version outside it.
    fn read_boundary(&self, version: u32) -> u32 {
        if version < self.reads_min {
            self.reads_min
        } else {
            self.reads_max
        }
    }

    /// The rules that this head entry breaks against the `base` entry.
    fn findings_against(&self, id: &str, base: &Format) -> impl Iterator<Item = Finding> {
        let finding = |rule, head, base| Finding {
            rule,
            id: id.to_owned(),
            head,
            base,
        };
        let one_way = !base.reads(self.writes);
        let base_boundary = base.read_boundary(self.writes);
        let rollback = one_way.then(|| finding(Rule::Rollback, self.writes, base_boundary));
        let mixed = (one_way && (self.shared || base.shared))
            .then(|| finding(Rule::MixedFleet, self.writes, base_boundary));
        let rolling = (!self.reads(base.writes))
            .then(|| finding(Rule::Rolling, self.read_boundary(base.writes), base.writes));
        rollback.into_iter().chain(mixed).chain(rolling)
    }
}

/// The result of a comparison.
#[derive(Debug, Default)]
pub struct Report {
    /// The findings that a new waiver of the head covers, one line each.
    pub waived: Vec<String>,
    /// The lines that make the check fail.
    pub problems: Vec<String>,
    /// The lines for information only.
    pub notes: Vec<String>,
}

impl Report {
    /// True when the head breaks no rule that a new waiver does not cover.
    #[must_use]
    pub fn passed(&self) -> bool {
        self.problems.is_empty()
    }
}

/// A head registry compared with a base registry.
pub struct Comparison<'a> {
    base: &'a Registry,
    head: &'a Registry,
}

impl<'a> Comparison<'a> {
    #[must_use]
    pub fn new(base: &'a Registry, head: &'a Registry) -> Self {
        Self { base, head }
    }

    /// Compare the two registries.
    #[must_use]
    pub fn report(&self) -> Report {
        let findings = self.findings();
        let (waived, unwaived): (Vec<_>, Vec<_>) = findings
            .iter()
            .map(|finding| self.verdict(finding))
            .partition(|(waived, _)| *waived);
        let problems = unwaived
            .into_iter()
            .map(|(_, line)| line)
            .chain(self.unused_waivers(&findings))
            .chain(self.permanent_problems())
            .chain(self.layout_problems())
            .collect();
        Report {
            waived: waived.into_iter().map(|(_, line)| line).collect(),
            problems,
            notes: self.activation_notes().collect(),
        }
    }

    /// Every rule that the head breaks: first the formats that both list,
    /// in id order, then the formats that only the base lists.
    #[must_use]
    pub fn findings(&self) -> Vec<Finding> {
        let retired = self
            .base
            .0
            .format
            .iter()
            .filter(|(id, _)| !self.head.0.format.contains_key(*id))
            .map(|(id, base)| Finding {
                rule: Rule::Retired,
                id: id.clone(),
                head: base.writes,
                base: base.writes,
            });
        self.pairs()
            .flat_map(|(id, head, base)| head.findings_against(id, base))
            .chain(retired)
            .collect()
    }

    /// The reason of the new head waiver that covers `finding`. A waiver
    /// covers a finding when it is in the table of the rule and names the
    /// same format and version.
    #[must_use]
    pub fn waiver(&self, finding: &Finding) -> Option<&'a str> {
        let table = finding.rule.waivers();
        self.head
            .0
            .waivers(table)
            .get(&finding.id)
            .filter(|waiver| {
                waiver.version == finding.head && self.is_new(table, &finding.id, waiver)
            })
            .map(|waiver| waiver.reason.as_str())
    }

    /// Whether `finding` is waived, and its line for the report.
    fn verdict(&self, finding: &Finding) -> (bool, String) {
        let table = finding.rule.waivers().name();
        let Finding { id, head, .. } = finding;
        match self.waiver(finding) {
            Some(reason) => (
                true,
                format!("{finding} Waived by [{table}.{id}]: {reason}"),
            ),
            None => (
                false,
                format!(
                    "{finding} Add [{table}.{id}] with version = {head} and a reason, or change the release. See docs/formats.md."
                ),
            ),
        }
    }

    /// The formats that both registries list: the id, the head entry and
    /// the base entry.
    fn pairs(&self) -> impl Iterator<Item = (&'a String, &'a Format, &'a Format)> {
        let base = self.base;
        self.head
            .0
            .format
            .iter()
            .filter_map(move |(id, head)| base.0.format.get(id).map(|base| (id, head, base)))
    }

    /// A head waiver is new when the base has no waiver in the same table
    /// for the same format and version. Only a new waiver covers a finding.
    fn is_new(&self, table: Waivers, id: &str, waiver: &Waiver) -> bool {
        self.base
            .0
            .waivers(table)
            .get(id)
            .is_none_or(|old| old.version != waiver.version)
    }

    fn unused_waivers<'f>(&'f self, findings: &'f [Finding]) -> impl Iterator<Item = String> + 'f {
        Waivers::ALL
            .into_iter()
            .flat_map(|table| {
                self.head
                    .0
                    .waivers(table)
                    .iter()
                    .map(move |(id, waiver)| (table, id, waiver))
            })
            .filter(|(table, id, waiver)| self.is_new(*table, id, waiver))
            .filter(|(table, id, waiver)| {
                !findings.iter().any(|finding| {
                    finding.rule.waivers() == *table && finding.id == **id && finding.head == waiver.version
                })
            })
            .map(|(table, id, waiver)| {
                format!(
                    "[{}.{id}] with version = {} is new and covers no finding. Remove it, or give it the version that the finding names.",
                    table.name(),
                    waiver.version
                )
            })
    }

    fn permanent_problems(&self) -> impl Iterator<Item = String> + '_ {
        let removed = self
            .base
            .0
            .format
            .iter()
            .filter(|(id, base)| base.permanent && !self.head.0.format.contains_key(*id))
            .map(|(id, _)| {
                format!(
                    "{id}: permanent data still needs a reader. No retirement waiver covers this."
                )
            });
        self.pairs()
            .filter(|(_, head, base)| (head.permanent || base.permanent)
                && (head.reads_min > base.reads_min || head.reads_max < base.reads_max
                    || !head.permanent))
            .map(|(id, _, base)| {
                format!(
                    "{id}: permanent data must retain its reader range {}..={} and permanent designation. No waiver covers this.",
                    base.reads_min, base.reads_max
                )
            }).chain(removed)
    }

    fn layout_problems(&self) -> impl Iterator<Item = String> + '_ {
        self.pairs()
            .filter(|(_, head, base)| head.writes == base.writes && head.layout != base.layout)
            .map(|(id, head, _)| {
                format!(
                    "{id}: the layout changes, and the version stays {}. Give the format a new version, or a new id.",
                    head.writes
                )
            })
    }

    fn activation_notes(&self) -> impl Iterator<Item = String> + '_ {
        self.pairs().filter_map(|(id, head, base)| {
            head.activation
                .as_ref()
                .filter(|activation| !base.reads(activation.writes))
                .map(|activation| {
                    format!(
                        "{id}: flag {} writes version {}, and the base reads only up to version {}. Switch the flag on only after no node runs the base.",
                        activation.flag, activation.writes, base.read_boundary(activation.writes)
                    )
                })
        })
    }
}

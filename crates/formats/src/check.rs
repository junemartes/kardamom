//! The release check: the rules that a head registry breaks against a
//! base registry, and the waivers in the head that cover them.

use std::fmt;

use crate::{Format, Registry};

/// A rule between two releases of one format.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Rule {
    /// The base reads what the head writes: `writes <= base reads_max`.
    /// When it holds, a rollback from the head to the base is safe.
    Rollback,
    /// The head reads what the base writes: `reads_min <= base writes`.
    /// When it holds, a rolling deploy from the base to the head is safe.
    Rolling,
}

impl Rule {
    pub(crate) const ALL: [Self; 2] = [Self::Rollback, Self::Rolling];

    /// The registry table whose entries waive this rule.
    #[must_use]
    pub const fn table(self) -> &'static str {
        match self {
            Self::Rollback => "one_way",
            Self::Rolling => "coordinated",
        }
    }
}

/// A rule that the head breaks for one format.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Finding {
    pub rule: Rule,
    pub id: String,
    /// The head `writes` for [`Rule::Rollback`], the head `reads_min` for
    /// [`Rule::Rolling`]. A waiver names this version.
    pub head: u32,
    /// The base `reads_max` for [`Rule::Rollback`], the base `writes` for
    /// [`Rule::Rolling`].
    pub base: u32,
}

impl fmt::Display for Finding {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let Self { id, head, base, .. } = self;
        match self.rule {
            Rule::Rollback => write!(
                f,
                "{id}: the head writes version {head}, and the base reads only up to version {base}. A rollback to the base is not safe."
            ),
            Rule::Rolling => write!(
                f,
                "{id}: the head reads only from version {head}, and the base writes version {base}. A rolling deploy is not safe."
            ),
        }
    }
}

impl Format {
    /// The rules that this head entry breaks against the `base` entry.
    fn findings_against(&self, id: &str, base: &Format) -> impl Iterator<Item = Finding> {
        let finding = |rule, head, base| Finding {
            rule,
            id: id.to_owned(),
            head,
            base,
        };
        let rollback = (self.writes > base.reads_max)
            .then(|| finding(Rule::Rollback, self.writes, base.reads_max));
        let rolling = (self.reads_min > base.writes)
            .then(|| finding(Rule::Rolling, self.reads_min, base.writes));
        rollback.into_iter().chain(rolling)
    }
}

/// A head registry compared with the registry of its base revision.
pub struct Comparison<'a> {
    base: &'a Registry,
    head: &'a Registry,
}

impl<'a> Comparison<'a> {
    #[must_use]
    pub fn new(base: &'a Registry, head: &'a Registry) -> Self {
        Self { base, head }
    }

    /// Every rule that the head breaks, in format id order. A format that
    /// the base does not list is new and breaks no rule.
    #[must_use]
    pub fn findings(&self) -> Vec<Finding> {
        self.head
            .0
            .format
            .iter()
            .filter_map(|(id, head)| self.base.0.format.get(id).map(|base| (id, head, base)))
            .flat_map(|(id, head, base)| head.findings_against(id, base))
            .collect()
    }

    /// The reason of the head waiver that covers `finding`. A waiver
    /// covers a finding when it names the same format and version.
    #[must_use]
    pub fn waiver(&self, finding: &Finding) -> Option<&'a str> {
        self.head
            .0
            .waivers(finding.rule)
            .get(&finding.id)
            .filter(|waiver| waiver.version == finding.head)
            .map(|waiver| waiver.reason.as_str())
    }

    /// One line for `finding`: the waiver that covers it, or the entry
    /// that the head must add.
    #[must_use]
    pub fn verdict(&self, finding: &Finding) -> String {
        let table = finding.rule.table();
        let Finding { id, head, .. } = finding;
        match self.waiver(finding) {
            Some(reason) => format!("{finding} Waived by [{table}.{id}]: {reason}"),
            None => format!(
                "{finding} Add [{table}.{id}] with version = {head} and a reason, or change the release. See docs/formats.md."
            ),
        }
    }
}

//! Desired service-group counts and the allocation version that must satisfy them.

use serde::Deserialize;

use super::Alloc;

#[derive(Debug, Deserialize)]
pub(crate) struct Job {
    #[serde(rename = "Version")]
    pub(crate) version: u64,
    #[serde(rename = "TaskGroups")]
    groups: Vec<Group>,
}

#[derive(Debug, Deserialize)]
struct Group {
    #[serde(rename = "Name")]
    name: String,
    #[serde(rename = "Count")]
    count: usize,
}

impl Job {
    /// A complete set of current, desired-running allocations. Historical
    /// failed allocations cannot replace a missing replica or block its replacement.
    pub(crate) fn running<'a>(&self, allocs: &'a [Alloc]) -> Option<Vec<&'a Alloc>> {
        let current: Vec<&Alloc> = allocs.iter().filter(|a| self.current(a)).collect();
        let complete = !self.groups.is_empty()
            && self.groups.iter().all(|g| g.satisfied(&current))
            && current.iter().all(|a| self.has_group(&a.task_group));
        complete.then_some(current)
    }

    /// Whether an allocation of this version runs on each node of
    /// `nodes`. An allocation on another node does not count, so a
    /// replica that stops by design on its own node does not hold back
    /// the others.
    pub(crate) fn runs_on(&self, allocs: &[Alloc], nodes: &Nodes) -> bool {
        nodes.iter().all(|node| {
            allocs
                .iter()
                .any(|a| a.node_name == node && self.current(a))
        })
    }

    /// Whether `alloc` belongs to this version and is meant to keep
    /// running.
    fn current(&self, alloc: &Alloc) -> bool {
        alloc.job_version == self.version && alloc.is_desired_running()
    }

    pub(crate) fn has_group(&self, name: &str) -> bool {
        self.groups.iter().any(|g| g.name == name && g.count > 0)
    }
}

/// The Nomad names of one or more nodes. The type holds at least one
/// name, so a wait on the set always waits for a node.
#[derive(Debug)]
pub(crate) struct Nodes {
    first: String,
    rest: Vec<String>,
}

impl Nodes {
    /// The set of `names`, or `None` when `names` is empty.
    pub(crate) fn new(names: impl IntoIterator<Item = String>) -> Option<Self> {
        let mut names = names.into_iter();
        let first = names.next()?;
        Some(Self {
            first,
            rest: names.collect(),
        })
    }

    fn iter(&self) -> impl Iterator<Item = &str> {
        std::iter::once(&self.first)
            .chain(&self.rest)
            .map(String::as_str)
    }
}

impl Group {
    fn satisfied(&self, allocs: &[&Alloc]) -> bool {
        allocs.iter().filter(|a| a.task_group == self.name).count() == self.count
    }
}

#[cfg(test)]
mod tests;

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
        let current: Vec<&Alloc> = allocs
            .iter()
            .filter(|a| a.job_version == self.version && a.is_desired_running())
            .collect();
        let complete = !self.groups.is_empty()
            && self.groups.iter().all(|g| g.satisfied(&current))
            && current.iter().all(|a| self.has_group(&a.task_group));
        complete.then_some(current)
    }

    pub(crate) fn has_group(&self, name: &str) -> bool {
        self.groups.iter().any(|g| g.name == name && g.count > 0)
    }
}

impl Group {
    fn satisfied(&self, allocs: &[&Alloc]) -> bool {
        allocs.iter().filter(|a| a.task_group == self.name).count() == self.count
    }
}

#[cfg(test)]
mod tests;

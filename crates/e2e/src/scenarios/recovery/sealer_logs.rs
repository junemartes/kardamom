//! Election evidence from the logs of the sealer members. Every member
//! prints a `cluster TERM` line for each leadership term, also on replay,
//! a `cluster ELECTION` line on each election state change, and a
//! `cluster role=` line on each role change. A member that ran more than
//! once has one log file for each process.

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};
use std::time::Duration;

use anyhow::{Context, Result};

const TERM_LINE: &str = "cluster TERM ";
const ELECTION_LINE: &str = "cluster ELECTION ";
const ROLE_LINE: &str = "cluster role=";

/// The logs of every member: every process, oldest first.
pub struct SealerLogs {
    /// For each member, the text of its log files, oldest first.
    members: Vec<Vec<String>>,
}

/// A closed election: the leader, and the term that a majority closed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Elected {
    pub leader: u64,
    pub term: u64,
}

impl SealerLogs {
    /// The number after `key` in `line`.
    fn number_after(line: &str, key: &str) -> Option<u64> {
        let rest = &line[line.find(key)? + key.len()..];
        let end = rest
            .find(|c: char| !c.is_ascii_digit())
            .unwrap_or(rest.len());
        rest[..end].parse().ok()
    }

    /// Read the logs of `members` members from the stack root.
    ///
    /// # Errors
    /// Returns an error when the root cannot be listed or a log cannot
    /// be read.
    pub fn read(root: &Path, members: usize) -> Result<Self> {
        let names: Vec<PathBuf> = std::fs::read_dir(root)
            .context("list the stack root")?
            .filter_map(|e| e.ok().map(|e| e.path()))
            .collect();
        let members = (0..members)
            .map(|id| Self::member_files(&names, id))
            .map(|files| {
                files
                    .iter()
                    .map(std::fs::read_to_string)
                    .collect::<std::io::Result<Vec<_>>>()
            })
            .collect::<std::io::Result<Vec<_>>>()
            .context("read a sealer log")?;
        Ok(Self { members })
    }

    /// The log files of member `id`, oldest first. The first process logs
    /// to `sealer-<id>.log`, a returned one to `sealer-<id>-r<k>.log`.
    fn member_files(names: &[PathBuf], id: usize) -> Vec<PathBuf> {
        let first = format!("sealer-{id}.log");
        let returned = format!("sealer-{id}-r");
        let mut files: Vec<(u32, PathBuf)> = names
            .iter()
            .filter_map(|p| {
                let name = p.file_name()?.to_str()?;
                let order = if name == first {
                    Some(0)
                } else {
                    name.strip_prefix(&returned)?
                        .strip_suffix(".log")?
                        .parse()
                        .ok()
                };
                order.map(|k| (k, p.clone()))
            })
            .collect();
        files.sort();
        files.into_iter().map(|(_, p)| p).collect()
    }

    /// The block at which the current processes resume: the block of the
    /// newest term of each current process, the lowest over the members.
    /// A returned member first replays the term lines of its log, then
    /// prints the term of the new election.
    #[must_use]
    pub fn resume_block(&self) -> Option<u64> {
        self.members
            .iter()
            .filter_map(|files| files.last())
            .filter_map(|text| {
                text.lines()
                    .filter(|l| l.contains(TERM_LINE))
                    .filter_map(|l| {
                        Self::number_after(l, "leadershipTermId=")
                            .zip(Self::number_after(l, " block="))
                    })
                    .max()
                    .map(|(_, block)| block)
            })
            .min()
    }

    /// The lines of every process of every member that contain `needle`.
    #[must_use]
    pub fn count(&self, needle: &str) -> usize {
        self.members
            .iter()
            .flatten()
            .map(|text| text.lines().filter(|l| l.contains(needle)).count())
            .sum()
    }

    /// The leaders that the members name for each term, over every
    /// process of every member.
    fn leaders(&self) -> BTreeMap<u64, BTreeSet<u64>> {
        self.members
            .iter()
            .flatten()
            .flat_map(|text| text.lines())
            .filter(|l| l.contains(TERM_LINE))
            .filter_map(|l| {
                Self::number_after(l, "leadershipTermId=")
                    .zip(Self::number_after(l, "leaderMemberId="))
            })
            .fold(BTreeMap::new(), |mut terms, (term, leader)| {
                terms
                    .entry(term)
                    .or_insert_with(BTreeSet::new)
                    .insert(leader);
                terms
            })
    }

    /// At most one member led each term, and every member logged a term.
    ///
    /// # Errors
    /// Returns an error naming a split term or a silent member.
    pub fn assert_one_leader_per_term(&self, ctx: &str) -> Result<()> {
        let silent: Vec<usize> = (0..self.members.len())
            .filter(|&m| !self.members[m].iter().any(|text| text.contains(TERM_LINE)))
            .collect();
        anyhow::ensure!(
            silent.is_empty(),
            "{ctx}: sealer members {silent:?} logged no leadership term"
        );
        let split: Vec<String> = self
            .leaders()
            .iter()
            .filter(|(_, leaders)| leaders.len() > 1)
            .map(|(term, leaders)| format!("term {term}: members {leaders:?}"))
            .collect();
        anyhow::ensure!(
            split.is_empty(),
            "{ctx}: two members led one term: {}",
            split.join("; ")
        );
        Ok(())
    }

    /// The election that the current processes closed: the member whose
    /// last role line says LEADER, in a term that a majority of the
    /// current processes closed.
    #[must_use]
    pub fn elected(&self) -> Option<Elected> {
        let current: Vec<&str> = self
            .members
            .iter()
            .filter_map(|files| files.last().map(String::as_str))
            .collect();
        let closed: Vec<Option<u64>> = current
            .iter()
            .map(|text| {
                text.lines()
                    .rfind(|l| l.contains(ELECTION_LINE))
                    .filter(|l| l.contains(" state=CLOSED "))
                    .and_then(|l| {
                        // The closed line says `unknown` when its read of
                        // the recording log fails; the last term line of
                        // the process then names the term.
                        Self::number_after(l, "logLeadershipTermId=").or_else(|| {
                            text.lines()
                                .rfind(|t| t.contains(TERM_LINE))
                                .and_then(|t| Self::number_after(t, "leadershipTermId="))
                        })
                    })
            })
            .collect();
        let majority = current.len() / 2 + 1;
        current
            .iter()
            .zip(&closed)
            .filter_map(|(text, term)| {
                let role = text.lines().rfind(|l| l.contains(ROLE_LINE))?;
                let leader = role
                    .contains("role=LEADER")
                    .then(|| Self::number_after(role, "memberId="))??;
                Some(Elected {
                    leader,
                    term: (*term)?,
                })
            })
            .find(|e| closed.iter().filter(|t| **t == Some(e.term)).count() >= majority)
    }

    /// Wait until the current processes closed an election.
    ///
    /// # Errors
    /// Returns an error when no election closes within `bound`.
    pub async fn await_elected(root: &Path, members: usize, bound: Duration) -> Result<Elected> {
        crate::harness::metrics::poll_until(
            "the returned sealer members close an election",
            bound,
            Duration::from_millis(200),
            async || Ok(Self::read(root, members)?.elected()),
        )
        .await
    }
}

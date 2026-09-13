//! Evidence from the Nomad allocation logs: line counts that survive
//! task restarts, the Raft leader, and the validator divergence scan.
//! Callers assert on counts increasing, never on mere presence: bring-up
//! and earlier cases log the same lines.

use std::cell::Cell;
use std::time::Duration;

use crate::nomad::{Alloc, Nomad, Streams};
use crate::poll::{self, Budget};
use crate::probes::CLUSTER_TASK;

/// A divergence hit: the allocation whose log holds the line, and the
/// log itself, for the context dump.
#[derive(Debug, Clone)]
pub struct Divergence {
    pub alloc: String,
    pub logs: String,
}

/// The evidence sources of one cluster.
#[derive(Debug, Clone)]
pub struct Evidence {
    nomad: Nomad,
}

/// The inputs of a wait on a log count.
#[derive(Debug, Clone)]
pub struct CountWait<'a> {
    pub job: &'a str,
    pub needle: &'a str,
    pub baseline: usize,
    pub timeout: Duration,
    pub interval: Duration,
    pub streams: Streams,
    /// The failure message when the count never exceeds the baseline.
    pub fail_msg: &'a str,
}

impl Evidence {
    #[must_use]
    pub fn new(nomad: Nomad) -> Self {
        Self { nomad }
    }

    /// The number of log lines of `job` that contain `needle`.
    ///
    /// # Errors
    ///
    /// Returns an error if the logs cannot be read.
    pub async fn count_lines(
        &self,
        job: &str,
        needle: &str,
        streams: Streams,
    ) -> anyhow::Result<usize> {
        let logs = self.nomad.job_logs(job, streams).await?;
        Ok(logs.lines().filter(|l| l.contains(needle)).count())
    }

    /// Poll until the count of `needle` in the job's logs exceeds the
    /// baseline.
    ///
    /// # Errors
    ///
    /// Returns the wait's failure message after the timeout.
    pub async fn wait_count_gt(&self, wait: &CountWait<'_>) -> anyhow::Result<()> {
        let last = Cell::new(0_usize);
        let last_ref = &last;
        let outcome = poll::until(Budget::new(wait.timeout, wait.interval), |_| async move {
            let now = self
                .count_lines(wait.job, wait.needle, wait.streams)
                .await?;
            last_ref.set(now);
            Ok((now > wait.baseline).then_some(now))
        })
        .await?;
        let (now, elapsed) = outcome.or_fail(|t| {
            crate::chaos_fail!(
                "{} (log count {} -> {} over {}s)",
                wait.fail_msg,
                wait.baseline,
                last.get(),
                t.as_secs()
            )
        })?;
        crate::log(format!(
            "alloc-log count for '{}' (job {}) advanced {} -> {now} after {}s",
            wait.needle,
            wait.job,
            wait.baseline,
            elapsed.as_secs()
        ));
        Ok(())
    }

    /// The concatenated stdout of every cluster allocation.
    ///
    /// # Errors
    ///
    /// Returns an error if the logs cannot be read.
    pub async fn cluster_logs(&self) -> anyhow::Result<String> {
        self.nomad.job_logs(CLUSTER_TASK, Streams::StdoutOnly).await
    }

    /// The memberId of the current Raft leader, within `max`. Each
    /// member prints `cluster role=<ROLE> memberId=<N>` to stdout on
    /// every role change, so the member whose last role line says
    /// LEADER is the current leader. A just-killed member's log may
    /// still show a role line; only a last line that is LEADER counts.
    ///
    /// # Errors
    ///
    /// Returns an error if no leader is observed within `max`.
    pub async fn cluster_leader(&self, max: Duration) -> anyhow::Result<u32> {
        let budget = Budget::new(max, Duration::from_secs(3));
        let outcome = poll::until(budget, |_| async move { self.leader_once().await }).await?;
        outcome
            .or_fail(|t| {
                crate::chaos_fail!(
                    "no cluster leader observed within {}s (checked alloc logs for 'role=LEADER memberId=')",
                    t.as_secs()
                )
            })
            .map(|(leader, _)| leader)
    }

    async fn leader_once(&self) -> anyhow::Result<Option<u32>> {
        let allocs = self.nomad.allocations(CLUSTER_TASK).await?;
        let mut leader = None;
        for alloc in &allocs {
            leader = leader.or(self.leader_of(alloc).await?);
        }
        Ok(leader)
    }

    async fn leader_of(&self, alloc: &Alloc) -> anyhow::Result<Option<u32>> {
        let logs = self.nomad.alloc_logs(alloc, Streams::StdoutOnly).await?;
        Ok(leader_from_last_role_line(&logs))
    }

    /// Whether any validator allocation logged `halted on divergence`.
    /// A rescheduled validator gets a new allocation, so every
    /// allocation is scanned, not only the running one.
    ///
    /// # Errors
    ///
    /// Returns an error if the logs cannot be read.
    pub async fn divergence_scan(&self) -> anyhow::Result<Option<Divergence>> {
        let allocs = self.nomad.allocations("validator").await?;
        for alloc in &allocs {
            let logs = self.nomad.alloc_logs(alloc, Streams::Both).await?;
            if logs.contains("halted on divergence") {
                return Ok(Some(Divergence {
                    alloc: alloc.id.clone(),
                    logs,
                }));
            }
        }
        Ok(None)
    }
}

/// The memberId when the last `cluster role=... memberId=...` line of
/// `logs` reports LEADER.
fn leader_from_last_role_line(logs: &str) -> Option<u32> {
    let last = logs
        .lines()
        .rfind(|l| l.contains("cluster role=") && l.contains("memberId="))?;
    if !last.contains("role=LEADER") {
        return None;
    }
    member_id_of(last)
}

/// The `memberId=<N>` value of a log line.
#[must_use]
pub fn member_id_of(line: &str) -> Option<u32> {
    let rest = line.split("memberId=").nth(1)?;
    let digits: String = rest.chars().take_while(char::is_ascii_digit).collect();
    digits.parse().ok()
}

/// Print the divergence context to stderr: the lines around the hit.
pub fn dump_divergence(hit: &Divergence) {
    eprintln!("----- divergence context (alloc {}) -----", hit.alloc);
    let lines: Vec<&str> = hit.logs.lines().collect();
    let context: Vec<&str> = lines
        .iter()
        .enumerate()
        .filter(|(i, _)| {
            lines[i.saturating_sub(10)..=*i]
                .iter()
                .chain(
                    lines
                        .get(*i..(i.saturating_add(4)).min(lines.len()))
                        .unwrap_or(&[]),
                )
                .any(|l| l.contains("halted on divergence"))
        })
        .map(|(_, l)| *l)
        .collect();
    for l in context {
        eprintln!("{l}");
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_last_role_line_decides_the_leader() {
        let logs = "boot\ncluster role=FOLLOWER memberId=1\ncluster role=LEADER memberId=1\n";
        assert_eq!(leader_from_last_role_line(logs), Some(1));
        let demoted = "cluster role=LEADER memberId=2\ncluster role=FOLLOWER memberId=2\n";
        assert_eq!(leader_from_last_role_line(demoted), None);
        assert_eq!(leader_from_last_role_line("no roles here"), None);
        assert_eq!(
            member_id_of("sealer snapshot TAKEN memberId=12 block=30"),
            Some(12)
        );
    }
}

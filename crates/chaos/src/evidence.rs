//! Evidence from the Nomad allocation logs: line counts that survive
//! task restarts, the Raft leader, and the validator divergence scan.
//! Callers assert on counts increasing, never on mere presence: bring-up
//! and earlier cases log the same lines.

use std::cell::{Cell, RefCell};
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
        Ok(Self::count_in(&logs, needle))
    }

    /// The number of lines of `logs` that contain `needle`. The services
    /// color each field name and its `=` with ANSI escapes, so a needle
    /// such as `field=value` matches the line without them.
    fn count_in(logs: &str, needle: &str) -> usize {
        logs.lines()
            .filter(|l| without_ansi(l).contains(needle))
            .count()
    }

    /// Poll until the count of `needle` in the job's logs exceeds the
    /// baseline.
    ///
    /// # Errors
    ///
    /// Returns the wait's failure message after the timeout.
    pub async fn wait_count_gt(&self, wait: &CountWait<'_>) -> anyhow::Result<()> {
        self.wait_count_reaches(wait, wait.baseline + 1).await
    }

    /// Wait until the log count of `wait.needle` reaches `target`.
    ///
    /// # Errors
    ///
    /// Returns `wait.fail_msg` with the counts if the target is not
    /// reached within `wait.timeout`.
    pub async fn wait_count_reaches(
        &self,
        wait: &CountWait<'_>,
        target: usize,
    ) -> anyhow::Result<()> {
        let last = Cell::new(0_usize);
        let last_ref = &last;
        let outcome = poll::until(Budget::new(wait.timeout, wait.interval), |_| async move {
            let now = self
                .count_lines(wait.job, wait.needle, wait.streams)
                .await?;
            last_ref.set(now);
            Ok((now >= target).then_some(now))
        })
        .await?;
        let (now, elapsed) = outcome.or_fail(|t| {
            crate::chaos_fail!(
                "{} (log count {} -> {} over {}s, target {target})",
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
        Ok(self
            .member_logs()
            .await?
            .iter()
            .find_map(|logs| leader_from_last_role_line(logs)))
    }

    /// The stdout of every cluster allocation that Nomad wants running.
    async fn member_logs(&self) -> anyhow::Result<Vec<String>> {
        let allocs: Vec<Alloc> = self
            .nomad
            .allocations_with_logs(CLUSTER_TASK)
            .await?
            .into_iter()
            .filter(Alloc::is_desired_running)
            .collect();
        let mut logs = Vec::with_capacity(allocs.len());
        for alloc in &allocs {
            logs.push(self.nomad.alloc_logs(alloc, Streams::StdoutOnly).await?);
        }
        Ok(logs)
    }

    /// The leader and the term of a closed election, within `max`. A
    /// member logs `role=LEADER` when it wins the vote. Its followers can
    /// still fail to join its log, and the election then runs again. So
    /// the election counts only when a majority of the members closed it
    /// in the term of the leader.
    ///
    /// # Errors
    ///
    /// Returns an error if no election closes within `max`.
    pub async fn cluster_elected(&self, max: Duration) -> anyhow::Result<Elected> {
        let budget = Budget::new(max, Duration::from_secs(3));
        let last = RefCell::new(None);
        // Allocation and log availability can change during node recovery. A failed
        // read provides no election evidence; the next attempt refreshes the listing.
        let outcome = poll::until(budget, |_| async {
            match self.election_once().await {
                Ok(elected) => Ok(elected),
                Err(error) => {
                    let cause = format!("{error:#}");
                    crate::log(format!("election evidence is not ready: {cause}"));
                    last.replace(Some(cause));
                    Ok(None)
                }
            }
        })
        .await?;
        outcome
            .or_fail(|t| {
                let cause = last.take().map_or_else(String::new, |cause| {
                    format!("; last evidence read failed: {cause}")
                });
                crate::chaos_fail!(
                    "no election closed on a majority within {}s (checked the alloc logs for 'role=LEADER' and 'cluster ELECTION ... state=CLOSED' after the last '{START_LINE}'){cause}",
                    t.as_secs()
                )
            })
            .map(|(elected, _)| elected)
    }

    pub(crate) async fn election_once(&self) -> anyhow::Result<Option<Elected>> {
        Ok(Elected::from_member_logs(&self.member_logs().await?))
    }

    /// Whether any validator allocation logged `halted on divergence`.
    /// A rescheduled validator gets a new allocation, so every
    /// allocation is scanned, not only the running one.
    ///
    /// # Errors
    ///
    /// Returns an error if the logs cannot be read.
    pub async fn divergence_scan(&self) -> anyhow::Result<Option<Divergence>> {
        let allocs = self.nomad.allocations_with_logs("validator").await?;
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

/// The members of the sealer cluster.
pub(crate) const SEALER_MEMBERS: u64 = 3;
/// The line that each start of a sealer process prints first.
const START_LINE: &str = "cluster START mode=";
/// The line of an election state change. The join watchdog of each
/// member samples the state once a second.
const ELECTION_LINE: &str = "cluster ELECTION ";
/// The line of a term start. Every member prints it, also on replay.
const TERM_LINE: &str = "cluster TERM ";

/// A closed election: the leader, and the term that a majority of the
/// members closed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Elected {
    pub leader: u32,
    pub term: u64,
}

impl Elected {
    /// The closed election in the logs of the members, one log for each.
    /// The leader is the member whose last role line says LEADER. The
    /// election is closed when a majority of the members, the leader
    /// with them, closed their elections in one term.
    fn from_member_logs(logs: &[String]) -> Option<Self> {
        let views: Vec<MemberView> = logs.iter().map(|l| MemberView::parse(l)).collect();
        let majority = usize::try_from(SEALER_MEMBERS / 2 + 1).ok()?;
        views
            .iter()
            .filter_map(|view| view.leader.zip(view.closed_term))
            .filter(|(_, term)| {
                views
                    .iter()
                    .filter(|view| view.closed_term == Some(*term))
                    .count()
                    >= majority
            })
            .max_by_key(|(_, term)| *term)
            .map(|(leader, term)| Self { leader, term })
    }
}

/// What the current process of one member logged about its election.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct MemberView {
    /// The memberId when the last role line says LEADER.
    leader: Option<u32>,
    /// The term of the election when the last election line says
    /// CLOSED.
    closed_term: Option<u64>,
}

impl MemberView {
    /// Read the log of one member from its last start line on. The lines
    /// of an earlier process in the same allocation are stale: a killed
    /// member's last line can say LEADER or CLOSED. The closed line names
    /// the term of the last term entry of the recording log. When that
    /// read fails, it says `unknown`, and the last term line of the
    /// process names the term.
    fn parse(logs: &str) -> Self {
        let current = logs.rfind(START_LINE).map_or(logs, |at| &logs[at..]);
        let closed = current
            .lines()
            .rfind(|l| l.contains(ELECTION_LINE))
            .filter(|l| l.contains(" state=CLOSED "));
        let closed_term = closed.and_then(|l| {
            number_after(l, "logLeadershipTermId=").or_else(|| {
                current
                    .lines()
                    .rfind(|l| l.contains(TERM_LINE))
                    .and_then(|t| number_after(t, "leadershipTermId="))
            })
        });
        Self {
            leader: leader_from_last_role_line(current),
            closed_term,
        }
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
    u32::try_from(number_after(line, "memberId=")?).ok()
}

/// The number that follows the first `key` of a log line, such as the
/// `7` of `leadershipTermId=7`.
#[must_use]
pub fn number_after(line: &str, key: &str) -> Option<u64> {
    let rest = line.split(key).nth(1)?;
    let digits: String = rest.chars().take_while(char::is_ascii_digit).collect();
    digits.parse().ok()
}

/// `line` without its ANSI escape sequences (`ESC [` up to the final
/// letter).
pub(crate) fn without_ansi(line: &str) -> String {
    line.split('\x1b')
        .enumerate()
        .map(|(i, part)| match i {
            0 => part,
            _ => part
                .strip_prefix('[')
                .and_then(|rest| {
                    rest.find(|c: char| c.is_ascii_alphabetic())
                        .map(|end| &rest[end + 1..])
                })
                .unwrap_or(part),
        })
        .collect()
}

/// A log read by the time stamp of each line. A Nomad log read returns
/// the stdout of each task and then its stderr, so the text order is
/// not the time order. The services stamp each line with an RFC 3339
/// time of fixed width, so the stamps compare as strings.
pub(crate) struct StampedLog<'a>(pub(crate) &'a str);

impl StampedLog<'_> {
    /// Each line that starts with a time stamp, without its ANSI escape
    /// sequences, as the stamp and the line.
    pub(crate) fn lines(&self) -> impl Iterator<Item = (String, String)> + '_ {
        self.0.lines().map(without_ansi).filter_map(|line| {
            let stamp = line.split_whitespace().next()?;
            humantime::parse_rfc3339(stamp).ok()?;
            Some((stamp.to_owned(), line))
        })
    }

    /// The time stamps of the lines that hold `needle` and the word
    /// `word`. An empty `word` matches every line.
    pub(crate) fn stamps<'s>(
        &'s self,
        needle: &'s str,
        word: &'s str,
    ) -> impl Iterator<Item = String> + 's {
        self.lines()
            .filter(move |(_, l)| l.contains(needle))
            .filter(move |(_, l)| word.is_empty() || l.split_whitespace().any(|w| w == word))
            .map(|(stamp, _)| stamp)
    }
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

    /// The logs of three members after a full restart: each member logs
    /// its last process from `cluster START` on.
    fn member(id: u32, current: &str) -> String {
        format!(
            "2026-10-24T04:49:03Z cluster role=LEADER memberId={id}\n\
             2026-10-24T04:49:03Z cluster ELECTION memberId={id} state=CLOSED commitPosition=1 candidateTermId=1 logLeadershipTermId=1\n\
             2026-10-24T04:56:19Z cluster START mode=RESUME memberId={id}\n{current}"
        )
    }

    /// A won vote is not a closed election: the followers can fail to
    /// join the log of the leader. The lines of the killed process do
    /// not count.
    #[test]
    fn an_election_counts_only_when_a_majority_closed_it_in_one_term() {
        let leader = "2026-10-24T05:00:27Z cluster role=LEADER memberId=1\n";
        let awaiting = |id: u32| {
            format!(
                "2026-10-24T05:00:20Z cluster ELECTION memberId={id} state=FOLLOWER_LOG_AWAIT commitPosition=9 candidateTermId=4 logLeadershipTermId=2\n"
            )
        };
        let closed = |id: u32, term: &str| {
            format!(
                "2026-10-24T05:00:28Z cluster ELECTION memberId={id} state=CLOSED commitPosition=9 candidateTermId=0 logLeadershipTermId={term}\n"
            )
        };
        let won = [
            member(0, &awaiting(0)),
            member(1, leader),
            member(2, &awaiting(2)),
        ];
        assert_eq!(Elected::from_member_logs(&won), None);
        let stale = [member(0, ""), member(1, ""), member(2, "")];
        assert_eq!(Elected::from_member_logs(&stale), None);
        let lone = [
            member(0, &awaiting(0)),
            member(1, &format!("{leader}{}", closed(1, "4"))),
            member(2, &awaiting(2)),
        ];
        assert_eq!(Elected::from_member_logs(&lone), None);
        let split = [
            member(0, &closed(0, "3")),
            member(1, &format!("{leader}{}", closed(1, "4"))),
            member(2, &awaiting(2)),
        ];
        assert_eq!(Elected::from_member_logs(&split), None);
        let majority = [
            member(0, &closed(0, "4")),
            member(1, &format!("{leader}{}", closed(1, "4"))),
            member(2, &awaiting(2)),
        ];
        assert_eq!(
            Elected::from_member_logs(&majority),
            Some(Elected { leader: 1, term: 4 })
        );
    }

    #[test]
    fn a_stale_leader_cannot_hide_the_new_majoritys_closed_election() {
        let logs = [
            member(
                0,
                "cluster role=LEADER memberId=0\ncluster ELECTION memberId=0 state=CLOSED commitPosition=1 logLeadershipTermId=3\n",
            ),
            member(
                1,
                "cluster role=LEADER memberId=1\ncluster ELECTION memberId=1 state=CLOSED commitPosition=2 logLeadershipTermId=4\n",
            ),
            member(
                2,
                "cluster role=FOLLOWER memberId=2\ncluster ELECTION memberId=2 state=CLOSED commitPosition=2 logLeadershipTermId=4\n",
            ),
        ];
        assert_eq!(
            Elected::from_member_logs(&logs),
            Some(Elected { leader: 1, term: 4 })
        );
    }

    /// A closed line whose log term reads `unknown` takes the term of the
    /// last term line of the process.
    #[test]
    fn an_unknown_log_term_falls_back_to_the_last_term_line() {
        let logs = member(
            0,
            "2026-10-24T05:00:30Z cluster TERM memberId=0 leadershipTermId=4 leaderMemberId=1 logPosition=9 role=FOLLOWER block=1601\n\
             2026-10-24T05:00:31Z cluster ELECTION memberId=0 state=CLOSED commitPosition=9 candidateTermId=4 logLeadershipTermId=unknown\n",
        );
        assert_eq!(
            MemberView::parse(&logs),
            MemberView {
                leader: None,
                closed_term: Some(4)
            }
        );
    }

    #[test]
    fn a_field_needle_matches_a_colored_line() {
        let logs = "\x1b[32m INFO\x1b[0m parking to ask the peers \x1b[3mindex\x1b[0m\x1b[2m=\x1b[0m7 \x1b[3mevery_archive_refused\x1b[0m\x1b[2m=\x1b[0mtrue\n\
            \x1b[32m INFO\x1b[0m parking to ask the peers \x1b[3mevery_archive_refused\x1b[0m\x1b[2m=\x1b[0mfalse\n\
            plain parking to ask the peers every_archive_refused=true\n";
        assert_eq!(Evidence::count_in(logs, "every_archive_refused=true"), 2);
        assert_eq!(Evidence::count_in(logs, "every_archive_refused=false"), 1);
        assert_eq!(Evidence::count_in(logs, "parking to ask the peers"), 3);
        assert_eq!(Evidence::count_in(logs, "index=7"), 1);
    }
}

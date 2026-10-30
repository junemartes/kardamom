//! The task events of an allocation, and the failed exits among them.
//! The docker driver removes a dead container, so the Nomad events are
//! the record that survives. A Terminated event carries the exit code
//! and the signal. Nomad keeps only the last events of a task, and the
//! restart count covers the rest.

use std::collections::BTreeMap;
use std::time::{Duration, SystemTime};

use serde::Deserialize;

use super::{Alloc, Nomad, Streams};
use crate::evidence::without_ansi;

/// The log lines of a designed recovery exit: the process fails, and
/// its restart is the recovery.
/// - An executor or a validator stops on a join timeout: no archive
///   serves the envelope of a `TxRef`. The restart resumes from the
///   persisted cursor and replays the gap.
/// - A sealer member ends a wedged or stalled election, or a log that a
///   new leader truncated, with an exit of its join watchdog. The
///   relaunch elects again or joins the leader. Members that return
///   together after a stop of their job can take this path. The deferred
///   follower stall line ends no process, so the follower stall counts
///   by the text of its exit.
const RECOVERY_EXIT_LINES: [&str; 6] = [
    "join timeout: TxRef has no envelope on tx_data",
    "cluster JOIN WEDGE",
    "cluster LEADER WEDGE",
    "cluster LEADER STALL",
    "cluster CATCHUP STALL",
    "exiting for a relaunch that keeps the recording log",
];

/// One event of a Nomad task.
#[derive(Debug, Clone, Deserialize)]
pub(crate) struct TaskEvent {
    #[serde(rename = "Type")]
    kind: String,
    /// Unix nanoseconds.
    #[serde(rename = "Time")]
    time: i64,
    #[serde(rename = "DisplayMessage", default)]
    message: String,
    #[serde(rename = "ExitCode", default)]
    exit_code: i64,
    #[serde(rename = "Signal", default)]
    signal: i64,
    #[serde(rename = "Details", default)]
    details: Option<BTreeMap<String, String>>,
}

impl TaskEvent {
    /// The event on one line: time, type, exit code, signal, the OOM
    /// flag, and the message.
    pub(crate) fn line(&self) -> String {
        let oom = self
            .details
            .as_ref()
            .and_then(|d| d.get("oom_killed"))
            .map_or("-", String::as_str);
        format!(
            "    {} {} exit={} signal={} oom={oom} {}",
            self.utc(),
            self.kind,
            self.exit_code,
            self.signal,
            self.message
        )
    }

    /// The event time in RFC 3339, comparable with `docker logs -t`.
    fn utc(&self) -> String {
        self.at().map_or_else(
            || format!("time={}", self.time),
            |t| humantime::format_rfc3339_millis(t).to_string(),
        )
    }

    /// The event time, when it is a valid Unix time.
    fn at(&self) -> Option<SystemTime> {
        u64::try_from(self.time)
            .ok()
            .and_then(|nanos| SystemTime::UNIX_EPOCH.checked_add(Duration::from_nanos(nanos)))
    }

    /// The end of a task process, with any exit code.
    fn is_end(&self) -> bool {
        self.kind == "Terminated"
    }

    /// An event that no process of the task outlives: its start, or the
    /// end of the process before it.
    fn bounds_process(&self) -> bool {
        self.kind == "Started" || self.is_end()
    }
}

/// One process end of a task: the Terminated event, and the time of
/// the event before it that bounds the process. Nomad keeps only the
/// last events, so the oldest listed end can have no bound.
struct ProcessEnd<'a> {
    task: &'a str,
    end: TaskEvent,
    since: Option<SystemTime>,
}

impl ProcessEnd<'_> {
    /// Whether a recovery line stamped `at` belongs to this process.
    fn holds(&self, at: SystemTime) -> bool {
        self.since.is_none_or(|since| since <= at) && self.end.at().is_none_or(|end| at <= end)
    }
}

/// The recovery exit lines of a log: the time stamp of each stamped
/// line, and the count of lines with no stamp. The Rust services stamp
/// each line. The sealer member prints its lines with no stamp.
struct RecoveryLines {
    stamped: Vec<SystemTime>,
    unstamped: usize,
}

impl RecoveryLines {
    fn of(logs: &str) -> Self {
        let (stamped, unstamped): (Vec<_>, Vec<_>) = logs
            .lines()
            .map(without_ansi)
            .filter(|l| RECOVERY_EXIT_LINES.iter().any(|r| l.contains(r)))
            .map(|l| {
                l.split_whitespace()
                    .next()
                    .and_then(|stamp| humantime::parse_rfc3339(stamp).ok())
            })
            .partition(Option::is_some);
        Self {
            stamped: stamped.into_iter().flatten().collect(),
            unstamped: unstamped.len(),
        }
    }

    fn total(&self) -> usize {
        self.stamped.len().saturating_add(self.unstamped)
    }
}

/// The process ends and the restart count of one allocation.
struct Restarts<'a> {
    alloc: &'a Alloc,
    ends: Vec<ProcessEnd<'a>>,
}

impl<'a> Restarts<'a> {
    /// The process ends that Nomad still lists for the tasks of `alloc`.
    ///
    /// # Errors
    ///
    /// Returns an error if the events of a task do not decode.
    fn of(alloc: &'a Alloc) -> anyhow::Result<Self> {
        let ends = alloc
            .task_states
            .iter()
            .map(|(task, state)| {
                let events: Option<Vec<TaskEvent>> =
                    serde_json::from_value(state["Events"].clone())?;
                Ok(Self::ends_of(task, &events.unwrap_or_default()))
            })
            .collect::<anyhow::Result<Vec<_>>>()?;
        Ok(Self {
            alloc,
            ends: ends.into_iter().flatten().collect(),
        })
    }

    /// Each end in `events`, in time order, with the bound before it.
    fn ends_of<'t>(task: &'t str, events: &[TaskEvent]) -> Vec<ProcessEnd<'t>> {
        events
            .iter()
            .enumerate()
            .filter(|(_, event)| event.is_end())
            .map(|(at, event)| ProcessEnd {
                task,
                end: event.clone(),
                since: events[..at]
                    .iter()
                    .rfind(|e| e.bounds_process())
                    .and_then(TaskEvent::at),
            })
            .collect()
    }

    /// Whether the allocation shows no restart and no process end.
    fn is_clean(&self) -> bool {
        self.ends.is_empty() && self.alloc.task_restarts() == 0
    }

    /// One note for each restart that no recovery line explains, else
    /// none. A designed exit logs its line before it ends the process,
    /// so a stamped line explains only the end of its own process. A
    /// line with no stamp explains one end of any process. Each restart
    /// that Nomad counts, also one whose events Nomad no longer lists,
    /// needs one recovery line.
    fn unexplained(&self, logs: &str) -> Vec<String> {
        let lines = RecoveryLines::of(logs);
        let open: Vec<&ProcessEnd<'_>> = self
            .ends
            .iter()
            .filter(|end| !lines.stamped.iter().any(|at| end.holds(*at)))
            .collect();
        let restarts = self.alloc.task_restarts();
        let short = u64::try_from(lines.total()).is_ok_and(|total| restarts > total);
        let notes = open.iter().skip(lines.unstamped).map(|end| {
            format!(
                "{} task {} (no designed recovery line in its process):{}",
                self.alloc.short_id(),
                end.task,
                end.end.line()
            )
        });
        let count = short.then(|| {
            format!(
                "{} restarted {restarts} times with {} designed recovery lines in its log",
                self.alloc.short_id(),
                lines.total()
            )
        });
        notes.chain(count).collect()
    }
}

impl Nomad {
    /// The restarts of `allocs` that no designed recovery explains, one
    /// note each with the Nomad event. A process end with exit code 0
    /// counts too: a task that ends at once and restarts did not wait.
    ///
    /// # Errors
    ///
    /// Returns an error if the events do not decode or a log read fails.
    pub(crate) async fn unexplained_restarts(
        &self,
        allocs: &[Alloc],
    ) -> anyhow::Result<Vec<String>> {
        let mut notes = Vec::new();
        for alloc in allocs {
            notes.extend(self.unexplained_restarts_of(alloc).await?);
        }
        Ok(notes)
    }

    async fn unexplained_restarts_of(&self, alloc: &Alloc) -> anyhow::Result<Vec<String>> {
        let restarts = Restarts::of(alloc)?;
        if restarts.is_clean() {
            return Ok(Vec::new());
        }
        let logs = self.alloc_logs(alloc, Streams::Both).await?;
        Ok(restarts.unexplained(&logs))
    }
}

#[cfg(test)]
mod tests;

//! The task events of an allocation, and the failed exits among them.
//! The docker driver removes a dead container, so the Nomad events are
//! the record that survives. A Terminated event carries the exit code
//! and the signal. Nomad keeps only the last events of a task, and the
//! restart count covers the rest.

use std::collections::BTreeMap;
use std::time::{Duration, SystemTime};

use serde::Deserialize;

use super::{Alloc, Nomad, Streams};

/// The log lines of a designed recovery exit: the process fails, and
/// its restart is the recovery. An executor or a validator stops on a
/// join timeout: no archive serves the envelope of a `TxRef`. The
/// restart resumes from the persisted cursor and replays the gap.
const RECOVERY_EXIT_LINES: [&str; 1] = ["join timeout: TxRef has no envelope on tx_data"];

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
        u64::try_from(self.time)
            .ok()
            .and_then(|nanos| SystemTime::UNIX_EPOCH.checked_add(Duration::from_nanos(nanos)))
            .map_or_else(
                || format!("time={}", self.time),
                |t| humantime::format_rfc3339_millis(t).to_string(),
            )
    }

    /// A death of the task with a nonzero exit code.
    fn is_failed_exit(&self) -> bool {
        self.kind == "Terminated" && self.exit_code != 0
    }
}

/// The failed exits of one allocation: one task name and Terminated
/// event for each.
struct FailedExits<'a> {
    alloc: &'a Alloc,
    exits: Vec<(&'a str, TaskEvent)>,
}

impl<'a> FailedExits<'a> {
    /// The failed exits that Nomad still lists for the tasks of `alloc`.
    ///
    /// # Errors
    ///
    /// Returns an error if the events of a task do not decode.
    fn of(alloc: &'a Alloc) -> anyhow::Result<Self> {
        let events = alloc
            .task_states
            .iter()
            .map(|(task, state)| {
                let events: Option<Vec<TaskEvent>> =
                    serde_json::from_value(state["Events"].clone())?;
                Ok(events
                    .into_iter()
                    .flatten()
                    .filter(TaskEvent::is_failed_exit)
                    .map(move |event| (task.as_str(), event)))
            })
            .collect::<anyhow::Result<Vec<_>>>()?;
        Ok(Self {
            alloc,
            exits: events.into_iter().flatten().collect(),
        })
    }

    /// One note for each failed exit when the log holds fewer recovery
    /// lines than failed exits, else none. Each designed exit logs its
    /// line at least once before it ends the process.
    fn unexplained(&self, logs: &str) -> Vec<String> {
        let designed = logs
            .lines()
            .filter(|l| RECOVERY_EXIT_LINES.iter().any(|r| l.contains(r)))
            .count();
        if designed >= self.exits.len() {
            return Vec::new();
        }
        self.exits
            .iter()
            .map(|(task, event)| {
                format!(
                    "{} task {task} ({designed} designed recovery lines in its log):{}",
                    self.alloc.short_id(),
                    event.line()
                )
            })
            .collect()
    }
}

impl Nomad {
    /// The failed exits of `allocs` that no designed recovery explains,
    /// one note each with the Nomad event. A restart with no failed exit
    /// passes: a node loss, an image pull, or a stop for a move.
    ///
    /// # Errors
    ///
    /// Returns an error if the events do not decode or a log read fails.
    pub(crate) async fn unexplained_exits(&self, allocs: &[Alloc]) -> anyhow::Result<Vec<String>> {
        let mut notes = Vec::new();
        for alloc in allocs {
            notes.extend(self.unexplained_exits_of(alloc).await?);
        }
        Ok(notes)
    }

    async fn unexplained_exits_of(&self, alloc: &Alloc) -> anyhow::Result<Vec<String>> {
        let exits = FailedExits::of(alloc)?;
        if exits.exits.is_empty() {
            return Ok(Vec::new());
        }
        let logs = self.alloc_logs(alloc, Streams::Both).await?;
        Ok(exits.unexplained(&logs))
    }
}

#[cfg(test)]
mod tests;

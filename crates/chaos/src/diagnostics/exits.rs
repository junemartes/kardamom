//! Why a task died: Nomad's task state and events for every allocation,
//! and the Docker state of the inner containers on each node. The docker
//! driver removes a dead container, so the Nomad events are the record
//! that survives; a Terminated event carries the exit code and the signal.
//! Nomad keeps only the last events of a task, and the restart count
//! covers the rest.

use std::collections::BTreeMap;
use std::time::{Duration, SystemTime};

use serde::Deserialize;

use super::{Diagnostics, section};
use crate::contract::Node;

/// A task whose deaths the dump records: the node role that runs it, and
/// its name, which is also its Nomad job and the prefix of its inner
/// container.
struct Watched {
    role: &'static str,
    task: &'static str,
}

impl Watched {
    /// The node script that inspects every inner container of the task,
    /// exited ones included.
    fn inspect_script(&self) -> String {
        format!(
            "docker ps -a --format '{{{{.Names}}}}' | grep '^{}-' | xargs -r docker inspect -f '{INSPECT}'",
            self.task
        )
    }
}

const WATCHED: [Watched; 2] = [
    Watched {
        role: "sealer",
        task: "cluster",
    },
    Watched {
        role: "executor",
        task: "executor",
    },
];

/// The `docker inspect` fields that tell how a container ended.
/// `RestartCount` counts Docker restarts only; Nomad restarts a task in a
/// new container.
const INSPECT: &str = "{{.Name}} status={{.State.Status}} exit={{.State.ExitCode}} \
    oom={{.State.OOMKilled}} restarts={{.RestartCount}} started={{.State.StartedAt}} \
    finished={{.State.FinishedAt}} error={{.State.Error}}";

/// The part of a Nomad task state that records restarts and deaths.
#[derive(Debug, Deserialize)]
struct TaskRecord {
    #[serde(rename = "Restarts", default)]
    restarts: u64,
    #[serde(rename = "Failed", default)]
    failed: bool,
    #[serde(rename = "StartedAt", default)]
    started_at: String,
    #[serde(rename = "FinishedAt", default)]
    finished_at: String,
    /// A task with no event yet lists an explicit `null`.
    #[serde(rename = "Events", default)]
    events: Option<Vec<TaskEvent>>,
}

#[derive(Debug, Deserialize)]
struct TaskEvent {
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

impl TaskRecord {
    /// The state, the restart count, and one line per event of one task.
    fn render(task: &str, state: &serde_json::Value) -> String {
        match Self::deserialize(state) {
            Ok(record) => record.lines(task),
            Err(e) => format!("  task {task}: undecodable task state: {e}"),
        }
    }

    fn lines(&self, task: &str) -> String {
        let header = format!(
            "  task {task}: restarts={} failed={} started={} finished={}",
            self.restarts, self.failed, self.started_at, self.finished_at
        );
        std::iter::once(header)
            .chain(self.events.iter().flatten().map(TaskEvent::line))
            .collect::<Vec<_>>()
            .join("\n")
    }
}

impl TaskEvent {
    fn line(&self) -> String {
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
}

impl Diagnostics {
    /// The task state and events of every allocation of a watched job.
    pub(super) fn task_records(job: &str, alloc: &crate::nomad::Alloc) {
        if !WATCHED.iter().any(|w| w.task == job) {
            return;
        }
        println!("----- {job} alloc {}: task events -----", alloc.short_id());
        let records: Vec<String> = alloc
            .task_states
            .iter()
            .map(|(task, state)| TaskRecord::render(task, state))
            .collect();
        println!("{}", records.join("\n"));
    }

    /// The Docker state of every watched inner container, on every node
    /// of its role.
    pub(super) async fn container_states(&self) {
        let targets = WATCHED.iter().flat_map(|w| {
            self.contract
                .of_role(w.role)
                .into_iter()
                .map(move |node| (node, w))
        });
        for (node, watched) in targets {
            self.container_state(node, watched).await;
        }
    }

    async fn container_state(&self, node: &Node, watched: &Watched) {
        let task = watched.task;
        section(format!("{}: {task} containers (docker inspect)", node.name));
        let out = self
            .nodes
            .exec(&node.container, &watched.inspect_script())
            .await
            .unwrap_or_else(|e| format!("(inspect failed: {e:#})"));
        if out.is_empty() {
            println!("(no {task} container on the node)");
            return;
        }
        println!("{out}");
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_task_record_shows_each_death_with_its_exit_code_and_signal() {
        let state = serde_json::json!({
            "State": "running", "Restarts": 2, "Failed": false,
            "StartedAt": "2024-01-01T00:00:20Z", "FinishedAt": "0001-01-01T00:00:00Z",
            "Events": [
                {"Type": "Terminated", "Time": 1_704_067_200_123_000_000_i64,
                 "DisplayMessage": "Exit Code: 137", "ExitCode": 137, "Signal": 9,
                 "Details": {"oom_killed": "false"}},
                {"Type": "Restarting", "Time": 1_704_067_201_000_000_000_i64,
                 "DisplayMessage": "Task restarting in 15s"}]});
        let rendered = TaskRecord::render("cluster", &state);
        let lines: Vec<&str> = rendered.lines().collect();
        assert_eq!(
            lines[0],
            "  task cluster: restarts=2 failed=false started=2024-01-01T00:00:20Z \
             finished=0001-01-01T00:00:00Z"
        );
        assert_eq!(
            lines[1],
            "    2024-01-01T00:00:00.123Z Terminated exit=137 signal=9 oom=false Exit Code: 137"
        );
        assert_eq!(
            lines[2],
            "    2024-01-01T00:00:01.000Z Restarting exit=0 signal=0 oom=- Task restarting in 15s"
        );
    }

    #[test]
    fn a_task_with_no_events_shows_only_its_state() {
        let state = serde_json::json!({"State": "pending", "Events": null});
        assert_eq!(
            TaskRecord::render("executor", &state),
            "  task executor: restarts=0 failed=false started= finished="
        );
    }

    #[test]
    fn the_inspect_script_covers_exited_containers_of_the_task() {
        let script = WATCHED[0].inspect_script();
        assert!(script.starts_with("docker ps -a --format '{{.Names}}' | grep '^cluster-' |"));
        assert!(script.contains("oom={{.State.OOMKilled}}"));
        assert!(script.contains("exit={{.State.ExitCode}}"));
    }
}

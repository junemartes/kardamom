//! The Nomad HTTP API on the control node: allocations, alloc stop, alloc
//! logs, nodes, and node drain. The Docker host reaches the control
//! node's address directly, so nothing goes through `docker exec`.

use std::collections::BTreeMap;
use std::time::Duration;

use anyhow::Context;
use serde::Deserialize;

use crate::poll::Budget;

mod job;
mod log;
mod saved_job;
pub(crate) use job::Job;
pub(crate) use saved_job::SavedJob;

/// The log streams to read.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Streams {
    /// stdout and stderr, in that order per task.
    Both,
    /// stdout only. The cluster job prints its role and snapshot lines
    /// to stdout; its stderr is JVM noise.
    StdoutOnly,
}

impl Streams {
    fn names(self) -> &'static [&'static str] {
        match self {
            Self::Both => &["stdout", "stderr"],
            Self::StdoutOnly => &["stdout"],
        }
    }
}

/// One allocation of a job, as the listing returns it.
#[derive(Debug, Clone, Deserialize)]
pub struct Alloc {
    #[serde(rename = "JobVersion")]
    pub job_version: u64,
    #[serde(rename = "DesiredStatus")]
    pub desired_status: String,
    #[serde(rename = "ID")]
    pub id: String,
    #[serde(rename = "TaskGroup")]
    pub task_group: String,
    #[serde(rename = "ClientStatus")]
    pub client_status: String,
    #[serde(rename = "NodeName")]
    pub node_name: String,
    #[serde(rename = "NodeID")]
    pub node_id: String,
    /// Keyed by task name. The log endpoint needs the task name. A
    /// pending allocation the client has not started yet lists an
    /// explicit `null` here, which `default` alone does not cover.
    #[serde(rename = "TaskStates", default, deserialize_with = "null_as_empty")]
    pub task_states: BTreeMap<String, serde_json::Value>,
}

/// Decode a map field whose value may be JSON `null` as an empty map.
fn null_as_empty<'de, D>(d: D) -> Result<BTreeMap<String, serde_json::Value>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    Option::<BTreeMap<String, serde_json::Value>>::deserialize(d).map(Option::unwrap_or_default)
}

impl Alloc {
    /// Whether the client reports the allocation running.
    #[must_use]
    pub fn is_running(&self) -> bool {
        self.client_status == "running"
    }

    /// Whether the allocation ended: complete, failed or lost. Its task
    /// runs no more.
    #[must_use]
    pub fn is_terminal(&self) -> bool {
        matches!(self.client_status.as_str(), "complete" | "failed" | "lost")
    }

    /// Running, and meant to keep running: a stopping allocation still
    /// reports `running` for a moment after the job asks it to stop.
    #[must_use]
    pub fn is_desired_running(&self) -> bool {
        self.is_running() && self.desired_status == "run"
    }

    /// The short id CI logs show.
    #[must_use]
    pub fn short_id(&self) -> &str {
        self.id.get(..8).unwrap_or(&self.id)
    }

    /// The restarts of every task of the allocation, as Nomad counts
    /// them. A task that Nomad restarts in place keeps its allocation.
    #[must_use]
    pub fn task_restarts(&self) -> u64 {
        self.task_states
            .values()
            .filter_map(|state| state["Restarts"].as_u64())
            .sum()
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct NomadNode {
    #[serde(rename = "ID")]
    pub id: String,
    #[serde(rename = "Name")]
    pub name: String,
    #[serde(rename = "Status")]
    pub status: String,
}

/// A client of one Nomad HTTP endpoint.
#[derive(Debug, Clone)]
pub struct Nomad {
    http: reqwest::Client,
    base: String,
    /// The retry budget of one task-log read.
    log_budget: Budget,
}

impl Nomad {
    /// A client of `addr`, for example `http://192.168.56.8:4646`.
    ///
    /// # Errors
    ///
    /// Returns an error if the HTTP client fails to build.
    pub fn new(addr: &str) -> anyhow::Result<Self> {
        let http = reqwest::Client::builder()
            .timeout(Duration::from_secs(30))
            .build()
            .context("build the Nomad HTTP client")?;
        Ok(Self {
            http,
            base: addr.trim_end_matches('/').to_string(),
            log_budget: log::LOG_READ_BUDGET,
        })
    }

    fn url(&self, path: &str) -> String {
        format!("{}{path}", self.base)
    }

    /// Write the variable at `path` with `items`, as `nomad var put`
    /// does.
    ///
    /// # Errors
    ///
    /// Returns an error if Nomad refuses the write.
    pub(crate) async fn put_variable(
        &self,
        path: &str,
        items: &serde_json::Value,
    ) -> anyhow::Result<()> {
        let url = self.url(&format!("/v1/var/{path}"));
        self.http
            .put(&url)
            .json(&serde_json::json!({ "Path": path, "Items": items }))
            .send()
            .await
            .and_then(reqwest::Response::error_for_status)
            .with_context(|| format!("PUT {url}"))?;
        Ok(())
    }

    /// Delete the variable at `path`, as `nomad var purge` does. A
    /// variable that does not exist is deleted already.
    ///
    /// # Errors
    ///
    /// Returns an error if Nomad refuses the delete.
    pub(crate) async fn delete_variable(&self, path: &str) -> anyhow::Result<()> {
        let url = self.url(&format!("/v1/var/{path}"));
        self.http
            .delete(&url)
            .send()
            .await
            .and_then(reqwest::Response::error_for_status)
            .with_context(|| format!("DELETE {url}"))?;
        Ok(())
    }

    /// Every allocation of `job`, in any client status.
    ///
    /// # Errors
    ///
    /// Returns an error if the request fails or the body is not the
    /// allocation listing.
    pub async fn allocations(&self, job: &str) -> anyhow::Result<Vec<Alloc>> {
        self.read(&format!("/v1/job/{job}/allocations")).await
    }

    pub(crate) async fn job(&self, id: &str) -> anyhow::Result<Job> {
        self.read(&format!("/v1/job/{id}")).await
    }

    async fn read<T: serde::de::DeserializeOwned>(&self, path: &str) -> anyhow::Result<T> {
        let url = self.url(path);
        self.http
            .get(&url)
            .send()
            .await
            .and_then(reqwest::Response::error_for_status)
            .with_context(|| format!("GET {url}"))?
            .json()
            .await
            .with_context(|| format!("decode {url}"))
    }

    /// The allocations of `job` whose logs Nomad can serve: those on a
    /// ready client node. A node that a replacement removed, or one that
    /// is down, keeps its allocations in the listing, but a log read on
    /// them fails with a server error.
    ///
    /// # Errors
    ///
    /// Returns an error if the node or allocation listing fails.
    pub async fn allocations_with_logs(&self, job: &str) -> anyhow::Result<Vec<Alloc>> {
        let ready: std::collections::HashSet<String> = self
            .nodes()
            .await?
            .into_iter()
            .filter(|n| n.status == "ready")
            .map(|n| n.id)
            .collect();
        Ok(self
            .allocations(job)
            .await?
            .into_iter()
            .filter(|a| ready.contains(&a.node_id))
            .collect())
    }

    /// The running allocations of `job`.
    ///
    /// # Errors
    ///
    /// Returns an error if the listing fails.
    pub async fn running(&self, job: &str) -> anyhow::Result<Vec<Alloc>> {
        Ok(self
            .allocations(job)
            .await?
            .into_iter()
            .filter(Alloc::is_desired_running)
            .collect())
    }

    /// The running allocations of one task group of `job`.
    ///
    /// # Errors
    ///
    /// Returns an error if the listing fails.
    pub async fn running_in_group(&self, job: &str, group: &str) -> anyhow::Result<Vec<Alloc>> {
        Ok(self
            .running(job)
            .await?
            .into_iter()
            .filter(|a| a.task_group == group)
            .collect())
    }

    /// The number of running allocations of `job`.
    ///
    /// # Errors
    ///
    /// Returns an error if the listing fails.
    pub async fn count_running(&self, job: &str) -> anyhow::Result<usize> {
        Ok(self.running(job).await?.len())
    }

    /// Stop an allocation gracefully, as `nomad alloc stop` does. Nomad
    /// reschedules a service allocation under a new id.
    ///
    /// # Errors
    ///
    /// Returns an error if the request fails.
    pub async fn stop_alloc(&self, alloc_id: &str) -> anyhow::Result<()> {
        let url = self.url(&format!("/v1/allocation/{alloc_id}/stop"));
        self.http
            .post(&url)
            .send()
            .await
            .and_then(reqwest::Response::error_for_status)
            .with_context(|| format!("POST {url}"))?;
        Ok(())
    }

    /// The concatenated logs of every allocation of `job` on a ready node,
    /// running or not, every task, in the streams asked for. This is the
    /// evidence source for lines that straddle a task restart.
    ///
    /// # Errors
    ///
    /// Returns an error if the listing or a log read fails.
    pub async fn job_logs(&self, job: &str, streams: Streams) -> anyhow::Result<String> {
        let allocs = self.allocations_with_logs(job).await?;
        let mut out = String::new();
        for alloc in &allocs {
            out.push_str(&self.alloc_logs(alloc, streams).await?);
        }
        Ok(out)
    }

    /// The logs of every task of one allocation.
    ///
    /// # Errors
    ///
    /// Returns an error if a log read fails.
    pub async fn alloc_logs(&self, alloc: &Alloc, streams: Streams) -> anyhow::Result<String> {
        let mut out = String::new();
        let reads = alloc
            .task_states
            .keys()
            .flat_map(|task| streams.names().iter().map(move |s| (task, *s)));
        for (task, stream) in reads {
            self.append_task_log(&mut out, alloc, task, stream).await?;
        }
        Ok(out)
    }

    async fn append_task_log(
        &self,
        out: &mut String,
        alloc: &Alloc,
        task: &str,
        stream: &str,
    ) -> anyhow::Result<()> {
        out.push_str(&self.task_log(alloc, task, stream).await?);
        out.push('\n');
        Ok(())
    }

    /// Every client node.
    ///
    /// # Errors
    ///
    /// Returns an error if the request fails.
    pub async fn nodes(&self) -> anyhow::Result<Vec<NomadNode>> {
        let url = self.url("/v1/nodes");
        self.http
            .get(&url)
            .send()
            .await
            .and_then(reqwest::Response::error_for_status)
            .with_context(|| format!("GET {url}"))?
            .json()
            .await
            .with_context(|| format!("decode {url}"))
    }

    /// The Nomad node id of the node named `name` (the bare instance
    /// name, `<class>-<i>`).
    ///
    /// # Errors
    ///
    /// Returns an error if the request fails or no node has that name.
    pub async fn node_id(&self, name: &str) -> anyhow::Result<String> {
        self.nodes()
            .await?
            .into_iter()
            .find(|n| n.name == name)
            .map(|n| n.id)
            .ok_or_else(|| anyhow::anyhow!("no Nomad node named {name}"))
    }

    /// Enable a drain with a deadline, or disable it. A drain evicts
    /// every allocation of the node, system jobs included, and keeps
    /// them off until it is disabled.
    ///
    /// # Errors
    ///
    /// Returns an error if the request fails.
    pub async fn drain(
        &self,
        node_id: &str,
        enable: bool,
        deadline: Duration,
    ) -> anyhow::Result<()> {
        let spec = drain_request(enable, deadline);
        let url = self.url(&format!("/v1/node/{node_id}/drain"));
        self.http
            .post(&url)
            .json(&spec)
            .send()
            .await
            .and_then(reqwest::Response::error_for_status)
            .with_context(|| format!("POST {url}"))?;
        Ok(())
    }
}

/// The body of a drain update. Disabling the drain also marks the node
/// eligible again, as `nomad node drain -disable` does: a drain leaves
/// the node ineligible, and the API keeps it so unless told otherwise,
/// in which case no job is placed on the node again and a system job
/// never returns to it.
fn drain_request(enable: bool, deadline: Duration) -> serde_json::Value {
    if enable {
        serde_json::json!({ "DrainSpec": { "Deadline": deadline.as_nanos() } })
    } else {
        serde_json::json!({ "DrainSpec": null, "MarkEligible": true })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn disabling_a_drain_marks_the_node_eligible() {
        let on = drain_request(true, Duration::from_mins(2));
        assert_eq!(on["DrainSpec"]["Deadline"], 120_000_000_000_u64);
        assert!(on.get("MarkEligible").is_none());
        let off = drain_request(false, Duration::ZERO);
        assert!(off["DrainSpec"].is_null());
        assert_eq!(off["MarkEligible"], true);
    }

    #[test]
    fn decodes_an_allocation_listing() {
        let body = r#"[{"ID":"788914fb-2aa0","TaskGroup":"executor","ClientStatus":"running",
            "JobVersion":2,"DesiredStatus":"run","NodeName":"executor-2","NodeID":"2d99","TaskStates":{"executor":{"State":"running"}}},
            {"ID":"35f2b819-1111","TaskGroup":"executor","ClientStatus":"complete",
            "JobVersion":2,"DesiredStatus":"run","NodeName":"executor-1","NodeID":"3653","TaskStates":{}},
            {"ID":"0f89a7f1-2222","TaskGroup":"executor","ClientStatus":"pending",
            "JobVersion":2,"DesiredStatus":"run","NodeName":"executor-1","NodeID":"3653","TaskStates":null}]"#;
        let allocs: Vec<Alloc> = serde_json::from_str(body).unwrap();
        assert_eq!(allocs.iter().filter(|a| a.is_running()).count(), 1);
        assert_eq!(allocs[0].short_id(), "788914fb");
        assert_eq!(allocs[0].task_states.keys().next().unwrap(), "executor");
        assert!(
            allocs[2].task_states.is_empty(),
            "a pending alloc has no task states"
        );
        assert_eq!(allocs[0].task_restarts(), 0);
        let restarted: Alloc = serde_json::from_str(
            r#"{"ID":"a","TaskGroup":"ingress","ClientStatus":"running","JobVersion":1,
            "DesiredStatus":"run","NodeName":"ingress-0","NodeID":"n","TaskStates":{
            "ingress":{"State":"running","Restarts":2,"Failed":false},
            "sidecar":{"State":"running","Restarts":1,"Failed":false}}}"#,
        )
        .unwrap();
        assert_eq!(restarted.task_restarts(), 3);
    }
}

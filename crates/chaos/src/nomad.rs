//! The Nomad HTTP API on the control node: allocations, alloc stop, alloc
//! logs, nodes, and node drain. The Docker host reaches the control
//! node's address directly, so nothing goes through `docker exec`.

use std::collections::BTreeMap;
use std::time::Duration;

use anyhow::Context;
use serde::Deserialize;

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
    /// Keyed by task name. The log endpoint needs the task name.
    #[serde(rename = "TaskStates", default)]
    pub task_states: BTreeMap<String, serde_json::Value>,
}

impl Alloc {
    /// Whether the client reports the allocation running.
    #[must_use]
    pub fn is_running(&self) -> bool {
        self.client_status == "running"
    }

    /// The short id CI logs show.
    #[must_use]
    pub fn short_id(&self) -> &str {
        self.id.get(..8).unwrap_or(&self.id)
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
        })
    }

    fn url(&self, path: &str) -> String {
        format!("{}{path}", self.base)
    }

    /// Every allocation of `job`, in any client status.
    ///
    /// # Errors
    ///
    /// Returns an error if the request fails or the body is not the
    /// allocation listing.
    pub async fn allocations(&self, job: &str) -> anyhow::Result<Vec<Alloc>> {
        let url = self.url(&format!("/v1/job/{job}/allocations"));
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
            .filter(Alloc::is_running)
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

    /// One task's log stream of one allocation, from the start. Nomad's
    /// own log files persist across in-place restarts of the same
    /// allocation, so lines from a dead task generation are still here
    /// after the docker driver garbage-collected its container. An
    /// allocation the client has not started yet yields an empty string.
    ///
    /// # Errors
    ///
    /// Returns an error if the request fails for a reason other than a
    /// missing log.
    pub async fn task_log(
        &self,
        alloc_id: &str,
        task: &str,
        stream: &str,
    ) -> anyhow::Result<String> {
        let url = self.url(&format!(
            "/v1/client/fs/logs/{alloc_id}?task={task}&type={stream}&plain=true&origin=start"
        ));
        let response = self
            .http
            .get(&url)
            .send()
            .await
            .with_context(|| format!("GET {url}"))?;
        if response.status() == reqwest::StatusCode::NOT_FOUND {
            return Ok(String::new());
        }
        response
            .error_for_status()
            .with_context(|| format!("GET {url}"))?
            .text()
            .await
            .with_context(|| format!("read {url}"))
    }

    /// The concatenated logs of every allocation of `job`, running or
    /// not, every task, in the streams asked for. This is the evidence
    /// source for lines that straddle a task restart.
    ///
    /// # Errors
    ///
    /// Returns an error if the listing or a log read fails.
    pub async fn job_logs(&self, job: &str, streams: Streams) -> anyhow::Result<String> {
        let allocs = self.allocations(job).await?;
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
            self.append_task_log(&mut out, &alloc.id, task, stream)
                .await?;
        }
        Ok(out)
    }

    async fn append_task_log(
        &self,
        out: &mut String,
        alloc_id: &str,
        task: &str,
        stream: &str,
    ) -> anyhow::Result<()> {
        out.push_str(&self.task_log(alloc_id, task, stream).await?);
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
        let spec = if enable {
            serde_json::json!({ "DrainSpec": { "Deadline": deadline.as_nanos() } })
        } else {
            serde_json::json!({ "DrainSpec": null })
        };
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn decodes_an_allocation_listing() {
        let body = r#"[{"ID":"788914fb-2aa0","TaskGroup":"executor","ClientStatus":"running",
            "NodeName":"executor-2","NodeID":"2d99","TaskStates":{"executor":{"State":"running"}}},
            {"ID":"35f2b819-1111","TaskGroup":"executor","ClientStatus":"complete",
            "NodeName":"executor-1","NodeID":"3653","TaskStates":{}}]"#;
        let allocs: Vec<Alloc> = serde_json::from_str(body).unwrap();
        assert_eq!(allocs.iter().filter(|a| a.is_running()).count(), 1);
        assert_eq!(allocs[0].short_id(), "788914fb");
        assert_eq!(allocs[0].task_states.keys().next().unwrap(), "executor");
    }
}

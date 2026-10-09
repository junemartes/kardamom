//! Task-log reads. The control agent does not hold the log of a remote
//! allocation: it forwards the read to the node's client, over the
//! session that the client holds open to the server. The node's own
//! agent serves an allocation that it runs from its local disk, with no
//! forward. Thus a read that the control agent answers with a 5xx goes
//! to the node's agent next.

use std::cell::RefCell;
use std::time::Duration;

use anyhow::Context;
use serde::Deserialize;

use super::{Alloc, Nomad};
use crate::poll::{self, Budget};

/// How long a task-log read tries again after 5xx answers, and the pause
/// between the attempts. Each attempt asks the control agent, then the
/// node's agent.
pub(super) const LOG_READ_BUDGET: Budget = Budget {
    timeout: Duration::from_secs(30),
    interval: Duration::from_secs(5),
};

/// The cause a client agent names in a 5xx when the directory of an
/// allocation is gone: the client collected the allocation, and the
/// server still lists it. The log is gone for good, as with a 404.
const ALLOC_DIR_GONE: &str = "no such file or directory";

/// One answer of the fs/logs endpoint of one agent.
enum LogRead {
    /// The log text. It is empty for a log that the agent does not have.
    Text(String),
    /// A 5xx answer or a transport failure, with the text that names the
    /// cause.
    Failed(String),
}

/// The HTTP address that a node record advertises for its agent.
#[derive(Deserialize)]
struct NodeHttp {
    #[serde(rename = "HTTPAddr")]
    http_addr: String,
}

/// One log stream of one task of one allocation.
struct LogStream<'a> {
    alloc: &'a Alloc,
    task: &'a str,
    stream: &'a str,
}

impl LogStream<'_> {
    /// The fs/logs path. It is the same on every agent.
    fn path(&self) -> String {
        format!(
            "/v1/client/fs/logs/{}?task={}&type={}&plain=true&origin=start",
            self.alloc.id, self.task, self.stream
        )
    }
}

impl Nomad {
    /// One task's log stream of one allocation, from the start. Nomad's
    /// own log files persist across in-place restarts of the same
    /// allocation, so lines from a dead task generation are still here
    /// after the docker driver garbage-collected its container. A log
    /// that the agent does not have (404) yields an empty string: an
    /// allocation that Nomad collected, or a task that never started.
    ///
    /// # Errors
    ///
    /// Returns an error if the control agent fails the request for a
    /// reason other than a 5xx, or if the control agent and the node's
    /// agent both fail it for the whole retry budget.
    pub(crate) async fn task_log(
        &self,
        alloc: &Alloc,
        task: &str,
        stream: &str,
    ) -> anyhow::Result<String> {
        let read = LogStream {
            alloc,
            task,
            stream,
        };
        let last = RefCell::new(String::new());
        let (read_ref, last_ref) = (&read, &last);
        let outcome = poll::until(self.log_budget, |_| async move {
            self.log_attempt(read_ref, last_ref).await
        })
        .await?;
        outcome
            .or_fail(|t| {
                anyhow::anyhow!(
                    "GET {}: {} after {}s of retries",
                    self.url(&read.path()),
                    last.take(),
                    t.as_secs()
                )
            })
            .map(|(text, _)| text)
    }

    /// One attempt: the control agent first, and the node's agent when
    /// the control agent answers a 5xx. `None` asks for one more
    /// attempt, and `last` keeps both causes for the final error.
    async fn log_attempt(
        &self,
        read: &LogStream<'_>,
        last: &RefCell<String>,
    ) -> anyhow::Result<Option<String>> {
        let url = self.url(&read.path());
        let control = match self.read_log_once(&url).await? {
            LogRead::Text(text) => return Ok(Some(text)),
            LogRead::Failed(cause) => cause,
        };
        let node = &read.alloc.node_name;
        crate::log(format!(
            "log read {url} answered {control}; reading it from the agent on {node}"
        ));
        match self.node_log(read).await {
            LogRead::Text(text) => Ok(Some(text)),
            LogRead::Failed(cause) => {
                crate::log(format!(
                    "log read on the agent on {node} answered {cause}; retrying"
                ));
                last.replace(format!("{control}; the agent on {node} answered {cause}"));
                Ok(None)
            }
        }
    }

    /// The log from the agent of the node that runs the allocation. A
    /// failure to find or reach that agent is a failed read, so the
    /// retry covers it.
    async fn node_log(&self, read: &LogStream<'_>) -> LogRead {
        let attempt = async {
            let base = self.node_base(&read.alloc.node_id).await?;
            self.read_log_once(&format!("{base}{}", read.path())).await
        };
        attempt
            .await
            .unwrap_or_else(|e| LogRead::Failed(format!("{e:#}")))
    }

    /// The base URL of a node's own agent: the scheme of the control
    /// agent and the HTTP address that the node record advertises.
    async fn node_base(&self, node_id: &str) -> anyhow::Result<String> {
        let node: NodeHttp = self.read(&format!("/v1/node/{node_id}")).await?;
        let scheme = self.base.split_once("://").map_or("http", |(s, _)| s);
        Ok(format!("{scheme}://{}", node.http_addr))
    }

    /// One read of a task log from one agent. A 404 reads as empty text,
    /// and so does a 5xx that says the allocation directory is gone. Any
    /// other 5xx is a failed read, with its body: the body names the
    /// cause, and the status alone does not.
    async fn read_log_once(&self, url: &str) -> anyhow::Result<LogRead> {
        let response = self
            .http
            .get(url)
            .send()
            .await
            .with_context(|| format!("GET {url}"))?;
        let status = response.status();
        if status == reqwest::StatusCode::NOT_FOUND {
            return Ok(LogRead::Text(String::new()));
        }
        if status.is_server_error() {
            let body = response.text().await.unwrap_or_default();
            if body.contains(ALLOC_DIR_GONE) {
                return Ok(LogRead::Text(String::new()));
            }
            return Ok(LogRead::Failed(format!("{status}: {}", body.trim())));
        }
        let text = response
            .error_for_status()
            .with_context(|| format!("GET {url}"))?
            .text()
            .await
            .with_context(|| format!("read {url}"))?;
        Ok(LogRead::Text(text))
    }
}

#[cfg(test)]
mod tests;

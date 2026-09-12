//! The Docker host's view of the node containers, and the inner Nomad
//! task containers each node's own dockerd runs. Every call is a
//! `docker` command on the host socket, bounded by a timeout: a hard
//! kill of a privileged sibling can stall the host's dockerd for tens
//! of seconds, and an unbounded exec would hang a probe with it.

use std::process::Output;
use std::time::Duration;

use anyhow::Context;
use tokio::process::Command;

/// The bound on one `docker` command.
const DOCKER_TIMEOUT: Duration = Duration::from_secs(20);

/// A signal `docker kill -s` sends to an inner container.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Signal {
    Stop,
    Cont,
}

impl Signal {
    fn name(self) -> &'static str {
        match self {
            Self::Stop => "STOP",
            Self::Cont => "CONT",
        }
    }
}

/// The host-side Docker client.
#[derive(Debug, Clone, Copy, Default)]
pub struct Nodes;

impl Nodes {
    /// Run `docker <args>` on the host, within the bound. The output is
    /// returned whatever the exit status; callers judge it.
    ///
    /// # Errors
    ///
    /// Returns an error if the command cannot spawn or exceeds the bound.
    pub async fn docker(&self, args: &[&str]) -> anyhow::Result<Output> {
        let run = Command::new("docker").args(args).output();
        tokio::time::timeout(DOCKER_TIMEOUT, run)
            .await
            .with_context(|| format!("docker {} timed out", args.join(" ")))?
            .with_context(|| format!("spawn docker {}", args.join(" ")))
    }

    /// Run `docker <args>` and return its trimmed stdout, or an error on
    /// a non-zero exit.
    ///
    /// # Errors
    ///
    /// Returns an error if the command fails, hangs, or exits non-zero.
    pub async fn docker_ok(&self, args: &[&str]) -> anyhow::Result<String> {
        let out = self.docker(args).await?;
        anyhow::ensure!(
            out.status.success(),
            "docker {} failed ({}): {}",
            args.join(" "),
            out.status,
            String::from_utf8_lossy(&out.stderr).trim()
        );
        Ok(String::from_utf8_lossy(&out.stdout).trim().to_string())
    }

    /// Run a shell script inside a node container, as root.
    ///
    /// # Errors
    ///
    /// Returns an error if the exec fails, hangs, or exits non-zero.
    pub async fn exec(&self, node: &str, script: &str) -> anyhow::Result<String> {
        self.docker_ok(&["exec", node, "bash", "-lc", script]).await
    }

    /// Run a shell script inside a node container, and return whether
    /// it exited zero. A probe uses this where a failure is an answer,
    /// not an error.
    ///
    /// # Errors
    ///
    /// Returns an error only if the exec cannot run at all.
    pub async fn exec_status(&self, node: &str, script: &str) -> anyhow::Result<bool> {
        Ok(self
            .docker(&["exec", node, "bash", "-lc", script])
            .await?
            .status
            .success())
    }

    /// Run a shell script inside a node container with `stdin` fed to
    /// it. This is the transport of a tar stream between two nodes.
    ///
    /// # Errors
    ///
    /// Returns an error if the exec fails, hangs, or exits non-zero.
    pub async fn exec_with_stdin(
        &self,
        node: &str,
        script: &str,
        stdin: Vec<u8>,
    ) -> anyhow::Result<()> {
        use tokio::io::AsyncWriteExt;
        let mut child = Command::new("docker")
            .args(["exec", "-i", node, "bash", "-lc", script])
            .stdin(std::process::Stdio::piped())
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::piped())
            .spawn()
            .with_context(|| format!("spawn docker exec -i {node}"))?;
        let mut pipe = child.stdin.take().context("child stdin")?;
        pipe.write_all(&stdin)
            .await
            .context("write to docker exec stdin")?;
        pipe.shutdown().await.context("close docker exec stdin")?;
        let out = tokio::time::timeout(Duration::from_secs(600), child.wait_with_output())
            .await
            .with_context(|| format!("docker exec -i {node} timed out"))?
            .context("wait for docker exec")?;
        anyhow::ensure!(
            out.status.success(),
            "docker exec -i {node} failed ({}): {}",
            out.status,
            String::from_utf8_lossy(&out.stderr).trim()
        );
        Ok(())
    }

    /// The raw stdout bytes of a shell script inside a node container.
    /// This is how a tar stream leaves a node.
    ///
    /// # Errors
    ///
    /// Returns an error if the exec fails or exits above `max_status`.
    /// tar exits 1 when a file changed as it read it; a caller that
    /// tolerates live-writer drift passes 1.
    pub async fn exec_bytes(
        &self,
        node: &str,
        script: &str,
        max_status: i32,
    ) -> anyhow::Result<Vec<u8>> {
        let run = Command::new("docker")
            .args(["exec", node, "bash", "-lc", script])
            .output();
        let out = tokio::time::timeout(Duration::from_secs(600), run)
            .await
            .with_context(|| format!("docker exec {node} timed out"))?
            .with_context(|| format!("spawn docker exec {node}"))?;
        let code = out.status.code().unwrap_or(-1);
        anyhow::ensure!(
            code >= 0 && code <= max_status,
            "docker exec {node} failed ({}): {}",
            out.status,
            String::from_utf8_lossy(&out.stderr).trim()
        );
        Ok(out.stdout)
    }

    /// The first running inner container on `node` whose name starts
    /// with `prefix`, case-insensitively. Task containers are named
    /// `<task>-<alloc-id>`. `None` when none matches or the node does
    /// not answer.
    pub async fn inner_container(&self, node: &str, prefix: &str) -> Option<String> {
        let script = format!("docker ps --format '{{{{.Names}}}}' | grep -im1 '^{prefix}'");
        self.exec(node, &script)
            .await
            .ok()
            .filter(|s| !s.is_empty())
    }

    /// The id of the first running inner container on `node` whose name
    /// contains `task`. `None` when none runs or the node does not
    /// answer.
    pub async fn inner_cid(&self, node: &str, task: &str) -> Option<String> {
        let script = format!("docker ps --filter name={task} -q | head -1");
        self.exec(node, &script)
            .await
            .ok()
            .filter(|s| !s.is_empty())
    }

    /// The `StartedAt` timestamp of an inner container. A container name
    /// survives an in-place task restart; the timestamp does not, so it
    /// tells a newborn from a survivor. `None` on failure.
    pub async fn started_at(&self, node: &str, inner: &str) -> Option<String> {
        let script = format!("docker inspect -f '{{{{.State.StartedAt}}}}' {inner}");
        self.exec(node, &script)
            .await
            .ok()
            .filter(|s| !s.is_empty())
    }

    /// `docker kill` an inner container on a node.
    ///
    /// # Errors
    ///
    /// Returns an error if the kill fails.
    pub async fn inner_kill(&self, node: &str, inner: &str) -> anyhow::Result<()> {
        self.exec(node, &format!("docker kill {inner} >/dev/null"))
            .await
            .map(|_| ())
    }

    /// Send a signal to an inner container.
    ///
    /// # Errors
    ///
    /// Returns an error if the signal cannot be sent.
    pub async fn inner_signal(
        &self,
        node: &str,
        inner: &str,
        signal: Signal,
    ) -> anyhow::Result<()> {
        self.exec(
            node,
            &format!("docker kill -s {} {inner} >/dev/null", signal.name()),
        )
        .await
        .map(|_| ())
    }

    /// The recent log tail of an inner container, both streams merged.
    pub async fn inner_logs(&self, node: &str, inner: &str, tail: u32) -> Option<String> {
        self.exec(node, &format!("docker logs --tail {tail} {inner} 2>&1"))
            .await
            .ok()
    }

    /// Whether a host container's state is `Running`. `None` when the
    /// container does not exist or Docker does not answer.
    pub async fn running(&self, container: &str) -> Option<bool> {
        let out = self
            .docker_ok(&["inspect", "-f", "{{.State.Running}}", container])
            .await
            .ok()?;
        match out.as_str() {
            "true" => Some(true),
            "false" => Some(false),
            _ => None,
        }
    }

    /// `docker start` a host container.
    ///
    /// # Errors
    ///
    /// Returns an error if the start fails.
    pub async fn start(&self, container: &str) -> anyhow::Result<()> {
        self.docker_ok(&["start", container]).await.map(|_| ())
    }

    /// `docker update --cpus <cpus>` a host container. `"0"` lifts the
    /// limit.
    ///
    /// # Errors
    ///
    /// Returns an error if the update fails.
    pub async fn update_cpus(&self, container: &str, cpus: &str) -> anyhow::Result<()> {
        self.docker_ok(&["update", "--cpus", cpus, container])
            .await
            .map(|_| ())
    }

    /// The `NanoCpus` limit of a host container.
    ///
    /// # Errors
    ///
    /// Returns an error if the inspect fails or the value is not a
    /// number.
    pub async fn nano_cpus(&self, container: &str) -> anyhow::Result<i64> {
        self.docker_ok(&["inspect", "-f", "{{.HostConfig.NanoCpus}}", container])
            .await?
            .parse()
            .context("NanoCpus is not a number")
    }

    /// The host containers named `kardamom-<class>-<i>` of the pipeline
    /// classes, in `docker ps` order. The control node is not one.
    ///
    /// # Errors
    ///
    /// Returns an error if `docker ps` fails.
    pub async fn pipeline_nodes(&self) -> anyhow::Result<Vec<String>> {
        let listing = self.docker_ok(&["ps", "--format", "{{.Names}}"]).await?;
        Ok(listing
            .lines()
            .filter(|n| is_pipeline_node(n))
            .map(str::to_string)
            .collect())
    }
}

/// Whether a container name is `kardamom-<class>-<i>` for a pipeline
/// class: executor, sequencer, ingress, sealer, or aux.
fn is_pipeline_node(name: &str) -> bool {
    let Some(rest) = name.strip_prefix("kardamom-") else {
        return false;
    };
    let Some((class, index)) = rest.rsplit_once('-') else {
        return false;
    };
    matches!(
        class,
        "executor" | "sequencer" | "ingress" | "sealer" | "aux"
    ) && !index.is_empty()
        && index.bytes().all(|b| b.is_ascii_digit())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pipeline_node_names() {
        assert!(is_pipeline_node("kardamom-executor-2"));
        assert!(is_pipeline_node("kardamom-aux-0"));
        assert!(!is_pipeline_node("kardamom-control-0"));
        assert!(!is_pipeline_node("kardamom-executor-"));
        assert!(!is_pipeline_node("probe-consul"));
    }
}

//! The injectors. Each records what it killed, so `assert_count` can
//! require observed replacement, not only a running count: right after
//! a kill, the doomed allocation can still report running on the first
//! poll, and a bare count would pass instantly against it.

use std::time::Duration;

use crate::harness::Harness;
use crate::poll::{self, Budget};

/// What the last injector killed. `assert_count` reads and clears it.
#[derive(Debug, Clone)]
pub enum Killed {
    /// A gracefully stopped allocation; a different running allocation
    /// id must replace it.
    Alloc(String),
    /// A hard-killed inner task container; the task must run again on
    /// the same node under a different container id.
    Inner {
        node: String,
        task: String,
        cid: String,
    },
}

impl Harness {
    /// Stop the first running allocation of `job`, as `nomad alloc stop`
    /// does.
    ///
    /// # Errors
    ///
    /// Returns an error if the job has no running allocation or the
    /// stop fails.
    pub async fn inject_graceful(&mut self, job: &str) -> anyhow::Result<()> {
        let alloc = self
            .nomad
            .running(job)
            .await?
            .into_iter()
            .next()
            .ok_or_else(|| crate::chaos_fail!("no running alloc to stop for job {job}"))?;
        crate::log(format!(
            "graceful: nomad alloc stop {} (job {job})",
            alloc.short_id()
        ));
        self.nomad.stop_alloc(&alloc.id).await?;
        self.killed = Some(Killed::Alloc(alloc.id));
        Ok(())
    }

    /// Stop the first running allocation of one task group of `job`.
    ///
    /// # Errors
    ///
    /// Returns an error if the group has no running allocation or the
    /// stop fails.
    pub async fn inject_graceful_group(&mut self, job: &str, group: &str) -> anyhow::Result<()> {
        let alloc = self
            .nomad
            .running_in_group(job, group)
            .await?
            .into_iter()
            .next()
            .ok_or_else(|| crate::chaos_fail!("no running {group} alloc to stop for job {job}"))?;
        crate::log(format!(
            "graceful: nomad alloc stop {} (job {job}, group {group})",
            alloc.short_id()
        ));
        self.nomad.stop_alloc(&alloc.id).await?;
        self.killed = Some(Killed::Alloc(alloc.id));
        Ok(())
    }

    /// Hard-kill the inner container of `task` on the first of `nodes`
    /// that runs it. Several candidates cover a task whose placement
    /// moves between cases. No candidate running it is a loud failure,
    /// never a vacuous pass.
    ///
    /// # Errors
    ///
    /// Returns an error if no candidate runs the task or the kill
    /// fails.
    pub async fn inject_hard(&mut self, nodes: &[&str], task: &str) -> anyhow::Result<()> {
        let mut found = None;
        for node in nodes {
            found = found.or(self
                .nodes
                .inner_cid(node, task)
                .await
                .map(|cid| (node.to_string(), cid)));
        }
        let (node, cid) = found.ok_or_else(|| {
            crate::chaos_fail!(
                "no running {task} container to hard-kill on any of: {}",
                nodes.join(" ")
            )
        })?;
        crate::log(format!(
            "hard: docker kill inner {task} container {cid} on {node}"
        ));
        self.nodes
            .inner_kill(&node, &cid)
            .await
            .map_err(|e| crate::chaos_fail!("could not hard-kill {task} ({cid}) on {node}: {e}"))?;
        self.killed = Some(Killed::Inner {
            node,
            task: task.to_string(),
            cid,
        });
        Ok(())
    }

    /// `docker kill` whole node containers, judging the kill by the
    /// container state rather than by the exit event. On a thrashed
    /// host, `docker kill` can report a missing exit event after the
    /// SIGKILL already landed; only a node still running after a grace
    /// window is a failure.
    ///
    /// # Errors
    ///
    /// Returns an error if a node is still running 30s after the kill.
    pub async fn kill_nodes(&self, nodes: &[&str]) -> anyhow::Result<()> {
        let mut args = vec!["kill"];
        args.extend_from_slice(nodes);
        if self.nodes.docker(&args).await?.status.success() {
            return Ok(());
        }
        for node in nodes {
            self.await_node_exited(node).await?;
        }
        Ok(())
    }

    async fn await_node_exited(&self, node: &str) -> anyhow::Result<()> {
        let outcome = poll::until(Budget::secs(30, 2), |_| async move {
            Ok((self.nodes.running(node).await == Some(false)).then_some(()))
        })
        .await?;
        let ((), elapsed) = outcome.or_fail(|t| {
            crate::chaos_fail!(
                "could not kill node {node} (still running {}s after docker kill)",
                t.as_secs()
            )
        })?;
        crate::log(format!(
            "kill_node: {node} — exit event was late but the kill took after {}s (state=exited)",
            elapsed.as_secs()
        ));
        Ok(())
    }

    /// Freeze an inner container with SIGSTOP and verify it: a frozen
    /// process cannot answer HTTP, so a metrics endpoint that still
    /// answers means the freeze did not take, and the case fails loudly
    /// instead of asserting against a replica that never lapsed.
    /// `docker pause` is not used: the nested freezer of a privileged
    /// node can silently no-op.
    ///
    /// # Errors
    ///
    /// Returns an error if the signal fails or the endpoint stays up.
    pub async fn freeze_verified(
        &self,
        node: &str,
        inner: &str,
        target: &crate::metrics::Target,
        ctx: &str,
    ) -> anyhow::Result<()> {
        self.nodes
            .inner_signal(node, inner, crate::nodes::Signal::Stop)
            .await
            .map_err(|e| crate::chaos_fail!("{ctx}: SIGSTOP failed: {e}"))?;
        tokio::time::sleep(Duration::from_secs(3)).await;
        if self.probes.scrape().answers(target).await {
            let _ = self.thaw(node, inner).await;
            return Err(crate::chaos_fail!(
                "{ctx}: freeze did NOT take effect (metrics endpoint still answering mid-freeze)"
            ));
        }
        crate::log(format!("{ctx}: freeze verified (metrics endpoint dark)"));
        Ok(())
    }

    /// Send SIGCONT to a frozen inner container. A failure is not a case
    /// error by itself: the task can have been replaced mid-freeze.
    ///
    /// # Errors
    ///
    /// Returns an error if the signal cannot be sent.
    pub async fn thaw(&self, node: &str, inner: &str) -> anyhow::Result<()> {
        self.nodes
            .inner_signal(node, inner, crate::nodes::Signal::Cont)
            .await
    }
}

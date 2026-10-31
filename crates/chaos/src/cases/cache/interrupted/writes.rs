//! A Lua write fault covers both data processes and leaves Sentinel traffic live.

use std::future::Future;
use std::net::Ipv4Addr;
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::Context;
use serde::Serialize;

use super::{CTX, MIRROR_HEAD_KEYS};
use crate::harness::Harness;
use crate::nomad::Alloc;
use crate::poll::{self, Budget};

/// The fault expires after the case's nine-minute deadline, even if its process dies.
pub(super) const LEASE_SECONDS: u64 = 600;

#[derive(Clone, Copy)]
enum ScriptAccess {
    Blocked,
    Allowed,
}

#[derive(Serialize)]
struct DataNode {
    allocation: String,
    node: String,
    ip: Ipv4Addr,
    cid: String,
    started_at: String,
}

impl DataNode {
    async fn capture(h: &Harness, allocation: &Alloc) -> anyhow::Result<Self> {
        let node = h.contract.node(&allocation.node_name)?;
        let process = h
            .nodes
            .running_process(&node.container, "redis-")
            .await
            .with_context(|| format!("{CTX}: no Redis process on {}", node.name))?;
        let target = Self {
            allocation: allocation.id.clone(),
            node: node.container.clone(),
            ip: node.ip,
            cid: process.cid,
            started_at: process.started,
        };
        let check = format!(
            "{}\n{}",
            target.script("ACL DRYRUN default EVAL 'return 1' 0"),
            target.script("ACL DRYRUN default EVALSHA 0000000000000000000000000000000000000000 0"),
        );
        Self::require_script_access(&h.nodes.exec(&target.node, &check).await?)?;
        Ok(target)
    }

    fn require_script_access(answer: &str) -> anyhow::Result<()> {
        anyhow::ensure!(
            answer.lines().collect::<Vec<_>>() == ["OK", "OK"],
            "{CTX}: the Redis default user must allow EVAL and EVALSHA before the fault"
        );
        Ok(())
    }

    fn require_denied_scripts(answer: &str) -> anyhow::Result<()> {
        // redis-cli --raw separates error replies with a blank line.
        let answers = answer
            .lines()
            .filter(|line| !line.is_empty())
            .collect::<Vec<_>>();
        anyhow::ensure!(
            answers.len() == 2 && answers.iter().all(|answer| answer.starts_with("NOPERM")),
            "{CTX}: both Redis Lua entry points must reject writes before the mirror kill: {answer:?}"
        );
        Ok(())
    }

    fn script(&self, command: &str) -> String {
        format!(
            "docker exec {} redis-cli --raw -h {} -p 6379 {command}",
            self.cid, self.ip
        )
    }

    fn access_script(&self, lease: &str, access: ScriptAccess) -> String {
        // Nomad mounts the task directory writable at /local; the image root is read-only.
        let path = format!("/local/kardamom-chaos-lua-{lease}");
        let allow = self.script("ACL SETUSER default +eval +evalsha");
        match access {
            ScriptAccess::Blocked => format!(
                "set -eu\ndocker exec {} touch {path}\ndocker exec -d {} sh -c 'sleep {LEASE_SECONDS}; if [ -e {path} ]; then redis-cli --raw -h {} -p 6379 ACL SETUSER default +eval +evalsha; rm -f {path}; fi'\n{}",
                self.cid,
                self.cid,
                self.ip,
                self.script("ACL SETUSER default -eval -evalsha"),
            ),
            ScriptAccess::Allowed => format!("docker exec {} rm -f {path}\n{allow}", self.cid),
        }
    }

    async fn verify(&self, h: &Harness) -> anyhow::Result<()> {
        let process = h.nodes.running_process(&self.node, "redis-").await;
        anyhow::ensure!(
            process.is_some_and(
                |process| process.cid == self.cid && process.started == self.started_at
            ),
            "{CTX}: faulted Redis process changed on {}",
            self.node
        );
        let check = format!(
            "{}\n{}",
            self.script("EVAL 'return 1' 0"),
            self.script("EVALSHA 0000000000000000000000000000000000000000 0"),
        );
        Self::require_denied_scripts(&h.nodes.exec(&self.node, &check).await?)?;
        Ok(())
    }

    async fn cold_heads(&self, h: &Harness) -> anyhow::Result<bool> {
        let raw = h
            .nodes
            .exec(
                &self.node,
                &self.script(&format!("EXISTS {MIRROR_HEAD_KEYS}")),
            )
            .await?;
        Ok(raw.parse::<usize>()? == 0)
    }
}

#[derive(Serialize)]
pub(super) struct WriteBlock {
    targets: [DataNode; 2],
    lease: String,
}

impl WriteBlock {
    pub(super) async fn capture(h: &Harness) -> anyhow::Result<Self> {
        let allocations = h.nomad.allocations("redis").await?;
        let [primary, replica] = Self::data_nodes(&allocations)?;
        Ok(Self {
            targets: [
                DataNode::capture(h, primary).await?,
                DataNode::capture(h, replica).await?,
            ],
            lease: SystemTime::now()
                .duration_since(UNIX_EPOCH)?
                .as_nanos()
                .to_string(),
        })
    }

    fn data_nodes(allocations: &[Alloc]) -> anyhow::Result<[&Alloc; 2]> {
        let [primary, replica] = [
            Self::one(allocations, "primary")?,
            Self::one(allocations, "replica")?,
        ];
        anyhow::ensure!(
            primary.node_name != replica.node_name,
            "{CTX}: Redis data processes share a node"
        );
        Ok([primary, replica])
    }

    fn one<'a>(allocations: &'a [Alloc], group: &str) -> anyhow::Result<&'a Alloc> {
        let matching = allocations
            .iter()
            .filter(|a| a.task_group == group && a.is_desired_running())
            .collect::<Vec<_>>();
        let [allocation] = matching.as_slice() else {
            anyhow::bail!(
                "{CTX}: expected one live Redis {group}, observed {}",
                matching.len()
            );
        };
        Ok(allocation)
    }

    pub(super) async fn arm(&self, h: &Harness) -> anyhow::Result<()> {
        self.change(h, ScriptAccess::Blocked).await
    }

    pub(super) async fn restore(&self, h: &Harness) -> anyhow::Result<()> {
        self.change(h, ScriptAccess::Allowed).await
    }

    async fn change(&self, h: &Harness, access: ScriptAccess) -> anyhow::Result<()> {
        self.command_with(access, |target, script| async move {
            h.nodes.exec(&target.node, &script).await
        })
        .await
    }

    /// Both destinations run to completion, even when one rejects the command.
    /// Cleanup therefore restores both nodes after a partial arm failure.
    async fn command_with<'a, F, Fut>(
        &'a self,
        access: ScriptAccess,
        execute: F,
    ) -> anyhow::Result<()>
    where
        F: Fn(&'a DataNode, String) -> Fut,
        Fut: Future<Output = anyhow::Result<String>>,
    {
        let (first, second) = tokio::join!(
            execute(
                &self.targets[0],
                self.targets[0].access_script(&self.lease, access)
            ),
            execute(
                &self.targets[1],
                self.targets[1].access_script(&self.lease, access)
            ),
        );
        let answers = [first?, second?];
        anyhow::ensure!(
            answers.iter().all(|answer| answer.trim() == "OK"),
            "{CTX}: Redis rejects the Lua access change: {answers:?}"
        );
        Ok(())
    }

    pub(super) async fn verify(&self, h: &Harness) -> anyhow::Result<()> {
        let allocations = h.nomad.allocations("redis").await?;
        let observed = Self::data_nodes(&allocations)?;
        anyhow::ensure!(
            observed
                .iter()
                .zip(&self.targets)
                .all(|(allocation, target)| allocation.id == target.allocation),
            "{CTX}: faulted Redis allocations changed"
        );
        let (first, second) = tokio::join!(self.targets[0].verify(h), self.targets[1].verify(h));
        first.and(second)
    }

    async fn cold_heads(&self, h: &Harness) -> anyhow::Result<bool> {
        let (first, second) =
            tokio::join!(self.targets[0].cold_heads(h), self.targets[1].cold_heads(h));
        Ok([first?, second?].into_iter().all(std::convert::identity))
    }

    pub(super) async fn await_cold_heads(&self, h: &Harness) -> anyhow::Result<()> {
        poll::until(Budget::secs(30, 1), |_| async {
            Ok(self.cold_heads(h).await?.then_some(()))
        })
        .await?
        .or_fail(|_| anyhow::anyhow!("{CTX}: Redis data nodes retain a head after the flush"))?;
        Ok(())
    }

    pub(super) async fn require_cold_heads(&self, h: &Harness) -> anyhow::Result<()> {
        anyhow::ensure!(
            self.cold_heads(h).await?,
            "{CTX}: a fresh head exists before the rebuild completes"
        );
        Ok(())
    }
}

#[cfg(test)]
mod tests;

#[cfg(test)]
mod lease_tests;

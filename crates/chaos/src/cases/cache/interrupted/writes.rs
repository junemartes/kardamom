//! A write pause covers both data processes, including a promoted replica.

use std::future::Future;
use std::net::Ipv4Addr;

use anyhow::Context;
use serde::Serialize;

use super::{CTX, MIRROR_HEAD_KEYS};
use crate::harness::Harness;
use crate::nomad::Alloc;
use crate::poll::{self, Budget};

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
        Ok(Self {
            allocation: allocation.id.clone(),
            node: node.container.clone(),
            ip: node.ip,
            cid: process.cid,
            started_at: process.started,
        })
    }

    fn script(&self, command: &str) -> String {
        format!(
            "docker exec {} redis-cli --raw -h {} -p 6379 {command}",
            self.cid, self.ip
        )
    }

    async fn verify(&self, h: &Harness) -> anyhow::Result<()> {
        let process = h.nodes.running_process(&self.node, "redis-").await;
        anyhow::ensure!(
            process.is_some_and(
                |process| process.cid == self.cid && process.started == self.started_at
            ),
            "{CTX}: paused Redis process changed on {}",
            self.node
        );
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
pub(super) struct WritePause {
    targets: [DataNode; 2],
}

impl WritePause {
    pub(super) async fn capture(h: &Harness) -> anyhow::Result<Self> {
        let allocations = h.nomad.allocations("redis").await?;
        let [primary, replica] = Self::data_nodes(&allocations)?;
        Ok(Self {
            targets: [
                DataNode::capture(h, primary).await?,
                DataNode::capture(h, replica).await?,
            ],
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

    pub(super) async fn command(&self, h: &Harness, command: &str) -> anyhow::Result<()> {
        self.command_with(command, |target, script| async move {
            h.nodes.exec(&target.node, &script).await
        })
        .await
    }

    /// Both destinations run to completion, even when one rejects the command.
    /// Cleanup therefore attempts every unpause after a partial arm failure.
    async fn command_with<'a, F, Fut>(&'a self, command: &str, execute: F) -> anyhow::Result<()>
    where
        F: Fn(&'a DataNode, String) -> Fut,
        Fut: Future<Output = anyhow::Result<String>>,
    {
        let (first, second) = tokio::join!(
            execute(&self.targets[0], self.targets[0].script(command)),
            execute(&self.targets[1], self.targets[1].script(command)),
        );
        let answers = [first?, second?];
        anyhow::ensure!(
            answers.iter().all(|answer| answer.trim() == "OK"),
            "{CTX}: Redis rejects {command}: {answers:?}"
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
            "{CTX}: paused Redis allocations changed"
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

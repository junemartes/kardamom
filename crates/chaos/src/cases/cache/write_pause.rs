//! A write pause covers every Redis data node through a promotion.

use std::net::Ipv4Addr;

use anyhow::Context;
use serde::Serialize;

use super::{MIRROR_HEAD_KEYS, aux, redis_cli};
use crate::harness::Harness;
use crate::nomad::Alloc;
use crate::poll::{self, Budget};

#[derive(Serialize)]
struct DataNode {
    group: String,
    allocation: String,
    node: String,
    address: Ipv4Addr,
}

impl DataNode {
    async fn capture(h: &Harness, group: &str) -> anyhow::Result<Self> {
        let allocations = h.nomad.running_in_group("redis", group).await?;
        let allocation = Self::single(&allocations, group)?;
        let node = h.contract.node(&allocation.node_name)?;
        Ok(Self {
            group: group.into(),
            allocation: allocation.id.clone(),
            node: node.name.clone(),
            address: node.ip,
        })
    }

    fn single<'a>(allocations: &'a [Alloc], group: &str) -> anyhow::Result<&'a Alloc> {
        match allocations {
            [allocation] => Ok(allocation),
            _ => anyhow::bail!("redis {group}: expected one running data allocation"),
        }
    }

    async fn request(&self, h: &Harness, command: &str) -> anyhow::Result<String> {
        h.nodes
            .exec(
                &aux(h),
                &redis_cli(&format!("--raw -h {} -p 6379 {command}", self.address)),
            )
            .await
            .with_context(|| format!("redis {} on {}: {command}", self.group, self.node))
    }

    async fn command(&self, h: &Harness, command: &str) -> anyhow::Result<()> {
        let answer = self.request(h, command).await?;
        anyhow::ensure!(
            answer.trim() == "OK",
            "redis {} on {} rejects {command}: {answer}",
            self.group,
            self.node
        );
        Ok(())
    }

    async fn cold(&self, h: &Harness) -> anyhow::Result<bool> {
        let heads: usize = self
            .request(h, &format!("EXISTS {MIRROR_HEAD_KEYS}"))
            .await?
            .parse()?;
        Ok(heads == 0)
    }
}

#[derive(Serialize)]
pub(super) struct WritePause {
    nodes: [DataNode; 2],
}

impl WritePause {
    /// The pause expires if the test process dies before cleanup.
    pub(super) const MAX_MS: u64 = 600_000;

    pub(super) async fn capture(h: &Harness) -> anyhow::Result<Self> {
        let primary = DataNode::capture(h, "primary").await?;
        let replica = DataNode::capture(h, "replica").await?;
        Ok(Self {
            nodes: [primary, replica],
        })
    }

    pub(super) async fn pause(&self, h: &Harness) -> anyhow::Result<()> {
        self.command(h, &format!("CLIENT PAUSE {} WRITE", Self::MAX_MS))
            .await
    }

    pub(super) async fn unpause(&self, h: &Harness) -> anyhow::Result<()> {
        self.command(h, "CLIENT UNPAUSE").await
    }

    async fn command(&self, h: &Harness, command: &str) -> anyhow::Result<()> {
        let (primary, replica) = tokio::join!(
            self.nodes[0].command(h, command),
            self.nodes[1].command(h, command),
        );
        // Both requests finish even when one fails, so cleanup reaches both nodes.
        primary.and(replica)
    }

    async fn cold(&self, h: &Harness) -> anyhow::Result<bool> {
        let (primary, replica) = tokio::try_join!(self.nodes[0].cold(h), self.nodes[1].cold(h))?;
        Ok(primary && replica)
    }

    pub(super) async fn await_cold(&self, h: &Harness) -> anyhow::Result<()> {
        let outcome = poll::until(Budget::secs(60, 1), |_| async {
            Ok::<_, anyhow::Error>(self.cold(h).await?.then_some(()))
        })
        .await?;
        outcome.or_fail(|elapsed| {
            crate::chaos_fail!(
                "mirror-kill-rebuild: Redis data nodes retain coverage heads after {}s",
                elapsed.as_secs()
            )
        })?;
        Ok(())
    }

    pub(super) async fn require_cold(&self, h: &Harness) -> anyhow::Result<()> {
        anyhow::ensure!(
            self.cold(h).await?,
            "mirror-kill-rebuild: a Redis data node has a fresh head before scan interruption"
        );
        Ok(())
    }
}

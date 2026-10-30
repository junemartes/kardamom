//! A recovered sequencer orders a used sender while its healthy twin is frozen.

use std::time::Duration;

use crate::harness::Harness;
use crate::metrics::{self, Target};
use crate::rpc::Rpc;

const PUBLISHED: &str = "kardamom_sequencer_tx_published_to_b_total";
const RECEIPT_BUDGET: Duration = Duration::from_secs(60);

#[derive(Clone, Copy)]
struct Replica {
    node: usize,
    twin: usize,
    lane: u8,
    account: u32,
}

impl Replica {
    /// Both lanes and both replicas run on every shard invocation.
    const ALL: [Self; 4] = [
        Self {
            node: 0,
            twin: 1,
            lane: 0,
            account: 3,
        },
        Self {
            node: 1,
            twin: 0,
            lane: 1,
            account: 1,
        },
        Self {
            node: 1,
            twin: 0,
            lane: 0,
            account: 3,
        },
        Self {
            node: 0,
            twin: 1,
            lane: 1,
            account: 1,
        },
    ];

    fn target(self, h: &Harness) -> Target {
        h.probes.sequencer_lane_target(self.node, self.lane)
    }

    fn twin_target(self, h: &Harness) -> Target {
        h.probes.sequencer_lane_target(self.twin, self.lane)
    }

    fn task(self) -> String {
        format!("sequencer-{}", self.lane)
    }

    async fn published(self, h: &Harness) -> Option<i64> {
        let body = h.probes.scrape().fetch(&self.target(h)).await?;
        metrics::first(&body, PUBLISHED)
    }
}

pub(super) struct SequencerRecovery {
    rpc: Rpc,
    next: u64,
    replica: Replica,
}

impl SequencerRecovery {
    pub(super) async fn run(h: &mut Harness) -> anyhow::Result<()> {
        for replica in Replica::ALL {
            Self::exercise(h, replica).await?;
        }
        Ok(())
    }

    async fn exercise(h: &mut Harness, replica: Replica) -> anyhow::Result<()> {
        let recovery = Self::establish(h, replica).await?;
        let node = h.container(&format!("sequencer-{}", replica.node))?;
        crate::log(format!(
            "sequencer-replica-kill: lane {} node {}",
            replica.lane, replica.node
        ));
        h.inject_hard(&[&node], &replica.task()).await?;
        h.assert_progress().await?;
        h.assert_count("sequencer", 4, h.knobs.restart_slo).await?;
        h.assert_replica_healthy(&replica.target(h), Duration::from_secs(90))
            .await?;
        recovery.assert_orders_alone(h).await
    }

    async fn establish(h: &Harness, replica: Replica) -> anyhow::Result<Self> {
        let rpc = Rpc::new(&h.rpc_url, h.knobs.chain_id)?;
        let nonce = rpc.nonce_of(replica.account).await?;
        rpc.transfer_hash(replica.account, nonce, RECEIPT_BUDGET)
            .await?;
        Ok(Self {
            rpc,
            replica,
            next: nonce
                .checked_add(1)
                .ok_or_else(|| crate::chaos_fail!("sequencer recovery sender nonce overflows"))?,
        })
    }

    async fn assert_orders_alone(&self, h: &Harness) -> anyhow::Result<()> {
        let ctx = "sequencer-replica-kill";
        let twin = h.container(&format!("sequencer-{}", self.replica.twin))?;
        let task = self.replica.task();
        let inner = h
            .nodes
            .inner_cid(&twin, &task)
            .await
            .ok_or_else(|| crate::chaos_fail!("{ctx}: no healthy twin to freeze"))?;
        let target = self.replica.twin_target(h);
        h.freeze_verified(&twin, &inner, &target, ctx).await?;
        let proof = self.while_twin_frozen(h, &twin, &inner).await;
        let thawed = h.thaw(&twin, &inner).await;
        proof?;
        thawed?;
        h.assert_replica_healthy(&target, Duration::from_secs(90))
            .await?;
        crate::log(format!(
            "{ctx}: recovered lane {} node {} orders the established sender with its twin disabled",
            self.replica.lane, self.replica.node
        ));
        Ok(())
    }

    async fn while_twin_frozen(&self, h: &Harness, twin: &str, inner: &str) -> anyhow::Result<()> {
        let ctx = "sequencer-replica-kill";
        let published =
            self.replica.published(h).await.ok_or_else(|| {
                crate::chaos_fail!("{ctx}: recovered replica has no publish counter")
            })?;
        self.rpc
            .transfer_hash(self.replica.account, self.next, RECEIPT_BUDGET)
            .await?;
        let now = self.replica.published(h).await;
        anyhow::ensure!(
            now.is_some_and(|now| now > published),
            "{}: {ctx}: recovered replica publishes no transaction while its twin is frozen",
            crate::FAIL_PREFIX
        );
        anyhow::ensure!(
            h.nodes
                .inner_cid(twin, &self.replica.task())
                .await
                .as_deref()
                == Some(inner)
                && !h
                    .probes
                    .scrape()
                    .answers(&self.replica.twin_target(h))
                    .await,
            "{}: {ctx}: the twin is replaced or answers during the isolated recovery proof",
            crate::FAIL_PREFIX
        );
        Ok(())
    }
}

#[cfg(test)]
mod tests;

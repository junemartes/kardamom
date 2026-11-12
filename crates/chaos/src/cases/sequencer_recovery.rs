//! A recovered sequencer orders a used sender while its healthy twin is frozen.

use std::time::Duration;

use crate::accounts::Pin;
use crate::harness::Harness;
use crate::metrics::{self, Target};
use crate::nodes::Signal;
use crate::rpc::Rpc;

const PUBLISHED: &str = "kardamom_sequencer_tx_published_to_b_total";
const RECEIPT_BUDGET: Duration = Duration::from_secs(60);
const CTX: &str = "sequencer-replica-kill";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Replica {
    node: usize,
    twin: usize,
    lane: u8,
    account: u32,
}

/// One drill: the kill of one replica of one lane, as one case. The four
/// drills cover both replicas of both lanes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Drill {
    Lane0Node0,
    Lane1Node1,
    Lane0Node1,
    Lane1Node0,
}

impl Drill {
    /// Every drill.
    pub(crate) const ALL: [Self; 4] = [
        Self::Lane0Node0,
        Self::Lane1Node1,
        Self::Lane0Node1,
        Self::Lane1Node0,
    ];

    /// The CI name.
    pub(crate) fn name(self) -> &'static str {
        match self {
            Self::Lane0Node0 => "sequencer-replica-kill-lane0-node0",
            Self::Lane1Node1 => "sequencer-replica-kill-lane1-node1",
            Self::Lane0Node1 => "sequencer-replica-kill-lane0-node1",
            Self::Lane1Node0 => "sequencer-replica-kill-lane1-node0",
        }
    }

    /// The account of the case load. A lane-0 drill drives the load
    /// through the killed lane. A lane-1 drill proves its lane with the
    /// reserved sender, and its load takes the next account of either
    /// lane, so the shard keeps enough funded accounts.
    pub(crate) fn pin(self) -> Pin {
        match self {
            Self::Lane0Node0 | Self::Lane0Node1 => Pin::Shard0,
            Self::Lane1Node1 | Self::Lane1Node0 => Pin::Any,
        }
    }

    /// The killed replica, its twin, and the reserved sender of its lane.
    fn replica(self) -> Replica {
        let (lane, node, account) = match self {
            Self::Lane0Node0 => (0, 0, 3),
            Self::Lane1Node1 => (1, 1, 1),
            Self::Lane0Node1 => (0, 1, 3),
            Self::Lane1Node0 => (1, 0, 1),
        };
        Replica {
            node,
            twin: 1 - node,
            lane,
            account,
        }
    }

    /// The case body.
    ///
    /// # Errors
    ///
    /// Returns the failure of the drill.
    pub(crate) async fn run(self, h: &mut Harness) -> anyhow::Result<()> {
        SequencerRecovery::exercise(h, self.replica()).await
    }
}

impl Replica {
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

struct SequencerRecovery {
    rpc: Rpc,
    next: u64,
    replica: Replica,
}

/// The frozen twin of the drill: its node and its inner container.
struct Frozen {
    node: String,
    inner: String,
}

impl SequencerRecovery {
    async fn exercise(h: &mut Harness, replica: Replica) -> anyhow::Result<()> {
        let recovery = Self::establish(h, replica).await?;
        let node = h.container(&format!("sequencer-{}", replica.node))?;
        crate::log(format!(
            "{CTX}: lane {} node {}",
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

    /// Freeze the twin, prove that the recovered replica orders the
    /// established sender alone, then thaw the twin. The freeze lasts
    /// only the proof, below the Aeron stall tolerance, so the thawed
    /// twin keeps its session and answers at once.
    async fn assert_orders_alone(&self, h: &Harness) -> anyhow::Result<()> {
        let node = h.container(&format!("sequencer-{}", self.replica.twin))?;
        let inner = h
            .nodes
            .inner_cid(&node, &self.replica.task())
            .await
            .ok_or_else(|| crate::chaos_fail!("{CTX}: no healthy twin to freeze"))?;
        h.nodes
            .inner_signal(&node, &inner, Signal::Stop)
            .await
            .map_err(|e| crate::chaos_fail!("{CTX}: SIGSTOP failed: {e}"))?;
        let frozen = Frozen { node, inner };
        let proof = self.while_twin_frozen(h, &frozen).await;
        let thawed = h.thaw(&frozen.node, &frozen.inner).await;
        proof?;
        thawed?;
        h.assert_replica_healthy(&self.replica.twin_target(h), Duration::from_secs(90))
            .await?;
        crate::log(format!(
            "{CTX}: recovered lane {} node {} orders the established sender with its twin disabled",
            self.replica.lane, self.replica.node
        ));
        Ok(())
    }

    /// The transfer and the dark scrape of the twin run at once: both
    /// observe the same frozen window, and a scrape of a frozen exporter
    /// takes its whole read timeout.
    async fn while_twin_frozen(&self, h: &Harness, frozen: &Frozen) -> anyhow::Result<()> {
        let published =
            self.replica.published(h).await.ok_or_else(|| {
                crate::chaos_fail!("{CTX}: recovered replica has no publish counter")
            })?;
        let twin = self.replica.twin_target(h);
        let (sent, twin_answers) = tokio::join!(
            self.rpc
                .transfer_hash(self.replica.account, self.next, RECEIPT_BUDGET),
            h.probes.scrape().answers(&twin),
        );
        sent?;
        anyhow::ensure!(
            !twin_answers,
            "{}: {CTX}: the freeze does not take: the twin answers during the isolated recovery proof",
            crate::FAIL_PREFIX
        );
        let now = self.replica.published(h).await;
        anyhow::ensure!(
            now.is_some_and(|now| now > published),
            "{}: {CTX}: recovered replica publishes no transaction while its twin is frozen",
            crate::FAIL_PREFIX
        );
        anyhow::ensure!(
            h.nodes
                .inner_cid(&frozen.node, &self.replica.task())
                .await
                .as_deref()
                == Some(frozen.inner.as_str()),
            "{}: {CTX}: the twin is replaced during the isolated recovery proof",
            crate::FAIL_PREFIX
        );
        Ok(())
    }
}

#[cfg(test)]
mod tests;

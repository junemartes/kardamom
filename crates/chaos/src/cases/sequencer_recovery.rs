//! A recovered sequencer orders a used sender while its healthy twin is frozen.

use std::time::Duration;

use crate::harness::Harness;
use crate::rpc::Rpc;

/// A load-reserve sender on lane 0; the case load uses a separate account.
const ACCOUNT: u32 = 3;
const PUBLISHED: &str = "kardamom_sequencer_tx_published_to_b_total";
const RECEIPT_BUDGET: Duration = Duration::from_secs(60);

pub(super) struct SequencerRecovery {
    rpc: Rpc,
    next: u64,
}

impl SequencerRecovery {
    // The load covers both receipt waits, both exporter waits, and the fault checks.
    pub(super) const PROOF_WINDOW: Duration = Duration::from_mins(6);

    pub(super) async fn establish(h: &Harness) -> anyhow::Result<Self> {
        let rpc = Rpc::new(&h.rpc_url, h.knobs.chain_id)?;
        let nonce = rpc.nonce_of(ACCOUNT).await?;
        rpc.transfer_hash(ACCOUNT, nonce, RECEIPT_BUDGET).await?;
        Ok(Self {
            rpc,
            next: nonce
                .checked_add(1)
                .ok_or_else(|| crate::chaos_fail!("sequencer recovery sender nonce overflows"))?,
        })
    }

    pub(super) async fn assert_orders_alone(&self, h: &Harness) -> anyhow::Result<()> {
        let ctx = "sequencer-replica-kill";
        let twin = h.container("sequencer-1")?;
        let inner = h
            .nodes
            .inner_cid(&twin, "sequencer-0")
            .await
            .ok_or_else(|| crate::chaos_fail!("{ctx}: no healthy twin to freeze"))?;
        let target = h.probes.sequencer_lane0_target(1);
        h.freeze_verified(&twin, &inner, &target, ctx).await?;
        let proof = self.while_twin_frozen(h, &twin, &inner).await;
        let thawed = h.thaw(&twin, &inner).await;
        proof?;
        thawed?;
        h.assert_replica_healthy(&target, Duration::from_secs(90))
            .await?;
        crate::log(format!(
            "{ctx}: recovered replica orders the established sender with its twin disabled"
        ));
        Ok(())
    }

    async fn while_twin_frozen(&self, h: &Harness, twin: &str, inner: &str) -> anyhow::Result<()> {
        let ctx = "sequencer-replica-kill";
        let published = h
            .probes
            .seq_lane0_metric(0, PUBLISHED)
            .await
            .ok_or_else(|| crate::chaos_fail!("{ctx}: recovered replica has no publish counter"))?;
        self.rpc
            .transfer_hash(ACCOUNT, self.next, RECEIPT_BUDGET)
            .await?;
        let now = h.probes.seq_lane0_metric(0, PUBLISHED).await;
        anyhow::ensure!(
            now.is_some_and(|now| now > published),
            "{}: {ctx}: recovered replica publishes no transaction while its twin is frozen",
            crate::FAIL_PREFIX
        );
        anyhow::ensure!(
            h.nodes.inner_cid(twin, "sequencer-0").await.as_deref() == Some(inner)
                && !h
                    .probes
                    .scrape()
                    .answers(&h.probes.sequencer_lane0_target(1))
                    .await,
            "{}: {ctx}: the twin is replaced or answers during the isolated recovery proof",
            crate::FAIL_PREFIX
        );
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn the_recovery_sender_belongs_to_the_faulted_lane() {
        assert_eq!(crate::accounts::ACCT_SHARD[super::ACCOUNT as usize], 0);
    }
}

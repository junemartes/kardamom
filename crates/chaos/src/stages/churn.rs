//! The ingress-churn re-smoke: stop the ingress-0 allocation, then send
//! the churn account's transfer through ingress-1. The active/active
//! pair must serve while one replica is replaced.

use std::time::Duration;

use crate::harness::{Harness, INGRESS_RPC_PORT};
use crate::rpc::Rpc;

/// The funded account of the re-smoke; the smoke gate holds #0 and the
/// load reserve holds #1..#15.
pub const CHURN_ACCOUNT: u32 = 16;
const SETTLE: Duration = Duration::from_secs(5);
const SMOKE_BUDGET: Duration = Duration::from_secs(60);

impl Harness {
    /// Stop the ingress-0 allocation and re-smoke through ingress-1.
    ///
    /// # Errors
    ///
    /// Returns an error if the allocation listing fails or the re-smoke
    /// gets no receipt.
    pub async fn ingress_churn(&self) -> anyhow::Result<()> {
        crate::log("ingress-churn: stopping ingress-0 and re-smoking against ingress-1");
        let victim = self
            .nomad
            .running("ingress")
            .await?
            .into_iter()
            .find(|a| a.node_name.ends_with("ingress-0"));
        match victim {
            Some(alloc) => {
                self.nomad.stop_alloc(&alloc.id).await?;
                crate::log(format!("ingress-churn: stopped alloc {}", alloc.short_id()));
            }
            None => crate::log("ingress-churn: no running ingress-0 allocation; re-smoke only"),
        }
        tokio::time::sleep(SETTLE).await;
        let url = self.ingress_url(1)?;
        Rpc::new(&url, self.knobs.chain_id)?
            .transfer_smoke(CHURN_ACCOUNT, SMOKE_BUDGET)
            .await
    }

    /// The JSON-RPC URL of the `i`-th ingress node.
    ///
    /// # Errors
    ///
    /// Returns an error if the contract has no such ingress node.
    pub fn ingress_url(&self, i: usize) -> anyhow::Result<String> {
        let node = self
            .probes
            .ingresses
            .get(i)
            .ok_or_else(|| anyhow::anyhow!("node contract has no ingress-{i}"))?;
        Ok(format!("http://{}:{INGRESS_RPC_PORT}", node.ip))
    }
}

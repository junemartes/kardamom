//! The chain-semantics stage (Target C): the `kardamom-semantics`
//! scenario runner against the live cluster, with the metrics targets
//! from the node contract and the settlement address from
//! `kardamom-deploy`. Both binaries come from the release build the
//! shard staged.

use std::path::PathBuf;
use std::process::Stdio;

use anyhow::Context;
use tokio::process::Command;

use crate::harness::Harness;
use crate::probes::{EXECUTOR_PORT, Probed, SEQUENCER_LANE0_PORT, VALIDATOR_PORT};

/// The in-cluster L1 listens on the control node.
const L1_RPC_PORT: u16 = 8546;
/// The second sequencer lane's exporter; lane `n` is `9001 + 10n`.
const SEQUENCER_LANE1_PORT: u16 = 9011;
/// The dev L1 owner that deployed the settlement contracts.
const L1_OWNER: &str = "0xf39Fd6e51aad88F6F4ce6aB8827279cffFb92266";

impl Harness {
    /// Run the chain-semantics suite.
    ///
    /// # Errors
    ///
    /// Returns an error if a binary is missing, the settlement address
    /// cannot be resolved, or a case fails.
    pub async fn semantics(&self) -> anyhow::Result<()> {
        let bin = self.release_binary("kardamom-semantics")?;
        let settlement = self.settlement_address().await?;
        let values = &self.knobs.stages.semantics;
        crate::log(format!(
            "chain-semantics suite (Target C): {} (settlement {settlement})",
            values.cases
        ));
        let sequencers = format!(
            "{},{}",
            targets(&self.probes.sequencers, SEQUENCER_LANE0_PORT),
            targets(&self.probes.sequencers, SEQUENCER_LANE1_PORT)
        );
        let status = Command::new(&bin)
            .args(["--rpc", &self.rpc_url])
            .args(["--chain-id", &self.knobs.chain_id.to_string()])
            .args([
                "--executor-metrics",
                &targets(&self.probes.executors, EXECUTOR_PORT),
            ])
            .args(["--sequencer-metrics", &sequencers])
            .args([
                "--validator-metrics",
                &targets(std::slice::from_ref(&self.probes.validator), VALIDATOR_PORT),
            ])
            .args([
                "--pending-receipt-timeout-ms",
                &values.park.as_millis().to_string(),
            ])
            .args(["--account-base", &values.account_base.to_string()])
            .args(["--l1-rpc", &self.l1_rpc()?])
            .args(["--settlement", &settlement])
            .args(["--cases", &values.cases])
            .stdin(Stdio::null())
            .status()
            .await
            .with_context(|| format!("spawn {}", bin.display()))?;
        anyhow::ensure!(status.success(), "kardamom-semantics failed with {status}");
        Ok(())
    }

    /// The JSON-RPC URL of the in-cluster L1.
    ///
    /// # Errors
    ///
    /// Returns an error if the contract has no control-plane node.
    pub fn l1_rpc(&self) -> anyhow::Result<String> {
        Ok(format!(
            "http://{}:{L1_RPC_PORT}",
            self.contract.control()?.ip
        ))
    }

    pub(crate) fn release_binary(&self, name: &str) -> anyhow::Result<PathBuf> {
        let path = self
            .lifecycle
            .repo_root()
            .join("target")
            .join("release")
            .join(name);
        anyhow::ensure!(path.is_file(), "{} is not staged", path.display());
        Ok(path)
    }

    pub(crate) async fn settlement_address(&self) -> anyhow::Result<String> {
        if let Some(explicit) = self.knobs.stages.semantics.settlement.clone() {
            return Ok(explicit);
        }
        let bin = self.release_binary("kardamom-deploy")?;
        let out = Command::new(&bin)
            .args(["--rpc-url", &self.l1_rpc()?])
            .args(["--owner", L1_OWNER])
            .arg("addresses")
            .args(["--l2-chain-id", &self.knobs.chain_id.to_string()])
            .stdin(Stdio::null())
            .output()
            .await
            .with_context(|| format!("spawn {}", bin.display()))?;
        anyhow::ensure!(
            out.status.success(),
            "kardamom-deploy addresses failed: {}",
            String::from_utf8_lossy(&out.stderr).trim()
        );
        settlement_in(&String::from_utf8_lossy(&out.stdout))
            .context("could not resolve the settlement address for the l1-batch case")
    }
}

/// `ip:port` for every node, comma-separated.
fn targets(nodes: &[Probed], port: u16) -> String {
    nodes
        .iter()
        .map(|n| format!("{}:{port}", n.ip))
        .collect::<Vec<_>>()
        .join(",")
}

/// The first proxy address in the `addresses` listing.
fn settlement_in(listing: &str) -> Option<String> {
    listing
        .lines()
        .find(|l| l.contains("proxy"))
        .and_then(|l| l.split_whitespace().nth(1))
        .map(str::to_string)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_first_proxy_line_gives_the_settlement() {
        let listing = "settlement v1\n  proxy     0xabc\n  impl      0xdef\n  proxy     0x123\n";
        assert_eq!(settlement_in(listing).as_deref(), Some("0xabc"));
        assert!(settlement_in("nothing").is_none());
    }
}

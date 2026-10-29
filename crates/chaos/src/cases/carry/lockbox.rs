//! Configure the deposit source without changing the chain's feature state.

use super::Run;
use crate::cases::deploy::MANIFEST;
use crate::l1::L1_OWNER;
use crate::lifecycle::{LastAttempt, NOMAD_HTTP_PORT};

impl Run<'_> {
    pub(super) async fn configure_lockbox(&mut self) -> anyhow::Result<()> {
        let lockbox = self.lockbox().await?;
        let mut vars = self.h.lifecycle.vars().clone();
        vars.carry.lockbox = Some(lockbox);
        self.h.lifecycle = self.h.lifecycle.clone().with_vars(vars);
        let nomad_addr = self.h.contract.nomad_addr(NOMAD_HTTP_PORT)?;
        anyhow::ensure!(
            self.h
                .lifecycle
                .deploy(&nomad_addr, MANIFEST, LastAttempt::Settled)
                .await?,
            "{}: {}: the lockbox configuration failed",
            crate::FAIL_PREFIX,
            self.ctx
        );
        Ok(())
    }

    /// The lockbox of the in-cluster L1. The deploy CLI deploys it when
    /// the registry has none. The owner mints the test deposits, and the
    /// oracle is the zero address.
    async fn lockbox(&self) -> anyhow::Result<String> {
        let chain = self.h.knobs.chain_id.to_string();
        let find = [
            "addresses",
            "--l2-chain-id",
            &chain,
            "--contract",
            "ETHLockbox",
            "--json",
        ];
        if let Some(lockbox) = Self::proxy_in(&self.h.deploy_cli(&find).await?)? {
            return Ok(lockbox);
        }
        self.h
            .deploy_cli(&[
                "deploy",
                "--private-key",
                "env:KARDAMOM_DEPLOY_OWNER_KEY",
                "ETHLockbox",
                "--l2-chain-id",
                &chain,
                "--l2-minter",
                L1_OWNER,
            ])
            .await?;
        Self::proxy_in(&self.h.deploy_cli(&find).await?)?.ok_or_else(|| {
            crate::chaos_fail!("{}: the deployed lockbox is not in the registry", self.ctx)
        })
    }

    /// The proxy of the first entry of an `addresses --json` listing.
    pub(super) fn proxy_in(listing: &str) -> anyhow::Result<Option<String>> {
        let entries: Vec<serde_json::Value> = serde_json::from_str(listing.trim())?;
        Ok(entries
            .first()
            .and_then(|e| e["proxy"].as_str())
            .map(str::to_string))
    }
}

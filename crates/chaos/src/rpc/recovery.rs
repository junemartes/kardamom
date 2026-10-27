//! A fault transfer retries only an explicit no-quorum refusal within one deadline.

use std::time::Duration;

use anyhow::Context;

use super::{CHAIN_HALTED_CODE, Rpc, RpcError, Signed};
use crate::poll::{self, Budget};

impl RpcError {
    fn is_no_quorum(&self) -> bool {
        self.code == CHAIN_HALTED_CODE
            && self
                .message
                .starts_with("chain halted: sealer_no_quorum at sealer (")
    }
}

impl Rpc {
    /// Nonce lookup, refused submissions and the receipt share this deadline.
    pub(crate) async fn transfer_within(
        &self,
        account: u32,
        budget: Duration,
    ) -> anyhow::Result<()> {
        tokio::time::timeout(budget, self.recovery_transfer(account, budget))
            .await
            .context("fault blocks the client past its transfer bound")?
    }

    async fn recovery_transfer(&self, account: u32, budget: Duration) -> anyhow::Result<()> {
        let signed = self.sign_transfer(account, self.nonce_of(account).await?)?;
        poll::until(Budget::new(budget, Duration::from_secs(1)), |_| {
            self.recovery_submit(&signed)
        })
        .await?
        .or_fail(|_| anyhow::anyhow!("no-quorum refusal persists through the transfer bound"))?;
        self.await_receipt(&signed.hash, budget).await
    }

    /// A no-quorum refusal admits nothing, so every retry uses the same signed bytes.
    async fn recovery_submit(&self, signed: &Signed) -> anyhow::Result<Option<()>> {
        match self.send_raw(&signed.raw).await? {
            Ok(hash) => {
                anyhow::ensure!(
                    hash == signed.hash,
                    "submit returns {hash}, expected {}",
                    signed.hash
                );
                Ok(Some(()))
            }
            Err(error) if error.is_no_quorum() => {
                crate::log(format!(
                    "fault transfer {}: {}; retrying within its deadline",
                    signed.hash, error.message
                ));
                Ok(None)
            }
            Err(error) => Err(error.into_error("eth_sendRawTransaction")),
        }
    }
}

#[cfg(test)]
#[path = "recovery_tests.rs"]
mod tests;

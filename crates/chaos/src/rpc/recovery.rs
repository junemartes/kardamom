//! A fault transfer retries only its named refusal within one deadline.

use std::time::Duration;

use anyhow::Context;

use super::{CHAIN_HALTED_CODE, Rpc, RpcError, Signed};
use crate::poll::{self, Budget};

/// The sole transient refusal a fault's recovery transfer may retry.
#[derive(Clone, Copy)]
pub(crate) enum RecoveryCause {
    NoQuorum,
    DaLag,
}

impl RecoveryCause {
    fn persistent(self) -> &'static str {
        match self {
            Self::NoQuorum => "no-quorum refusal persists through the transfer bound",
            Self::DaLag => "DA-lag refusal persists through the transfer bound",
        }
    }

    fn accepts(self, error: &RpcError) -> bool {
        let prefix = match self {
            Self::NoQuorum => "chain halted: sealer_no_quorum at sealer (",
            Self::DaLag => "chain halted: da_lag at sealer (",
        };
        error.code == CHAIN_HALTED_CODE && error.message.starts_with(prefix)
    }
}

impl Rpc {
    /// Nonce lookup, refused submissions and the receipt share this deadline.
    pub(crate) async fn transfer_within(
        &self,
        account: u32,
        budget: Duration,
        cause: RecoveryCause,
    ) -> anyhow::Result<()> {
        tokio::time::timeout(budget, self.recovery_transfer(account, budget, cause))
            .await
            .context("fault blocks the client past its transfer bound")?
    }

    async fn recovery_transfer(
        &self,
        account: u32,
        budget: Duration,
        cause: RecoveryCause,
    ) -> anyhow::Result<()> {
        let signed = self.sign_transfer(account, self.nonce_of(account).await?)?;
        poll::until(Budget::new(budget, Duration::from_secs(1)), |_| {
            self.recovery_submit(&signed, cause)
        })
        .await?
        .or_fail(|_| anyhow::anyhow!(cause.persistent()))?;
        self.await_receipt(&signed.hash, budget).await
    }

    /// The selected halt refusal admits nothing; each retry uses the same signed bytes.
    async fn recovery_submit(
        &self,
        signed: &Signed,
        cause: RecoveryCause,
    ) -> anyhow::Result<Option<()>> {
        match self.send_raw(&signed.raw).await? {
            Ok(hash) => {
                anyhow::ensure!(
                    hash == signed.hash,
                    "submit returns {hash}, expected {}",
                    signed.hash
                );
                Ok(Some(()))
            }
            Err(error) if cause.accepts(&error) => {
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

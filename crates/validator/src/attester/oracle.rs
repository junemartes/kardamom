//! The L1 `WithdrawalOutputOracle` binding and the poster that proposes
//! output roots to it.

use alloy_network::Ethereum;
use alloy_primitives::{B256, TxHash, U256};
use alloy_provider::Provider;
use alloy_sol_types::sol;

sol! {
    #[sol(rpc)]
    contract IWithdrawalOutputOracle {
        struct Output {
            bytes32 outputRoot;
            uint64 l2BlockNumber;
            uint64 timestamp;
            bool deleted;
        }
        function proposeOutput(bytes32 outputRoot, uint64 l2BlockNumber) external returns (uint256);
        function outputCount() external view returns (uint256);
        function getOutput(uint256 index) external view returns (Output memory);
    }
}

#[derive(Debug, thiserror::Error)]
pub enum AttesterError {
    #[error("L1 provider error: {0}")]
    Provider(String),
}

impl From<alloy_contract::Error> for AttesterError {
    fn from(e: alloy_contract::Error) -> Self {
        AttesterError::Provider(e.to_string())
    }
}

impl From<alloy_provider::PendingTransactionError> for AttesterError {
    fn from(e: alloy_provider::PendingTransactionError) -> Self {
        AttesterError::Provider(e.to_string())
    }
}

/// Posts output roots to the L1 [`WithdrawalOutputOracle`]. This is generic
/// over the alloy provider, so it works with a wallet-backed HTTP provider
/// in production and an anvil-backed provider in tests.
pub struct OutputPoster<P> {
    provider: P,
    oracle: alloy_primitives::Address,
}

impl<P: Provider<Ethereum> + Clone> OutputPoster<P> {
    pub fn new(provider: P, oracle: alloy_primitives::Address) -> Self {
        Self { provider, oracle }
    }

    /// Propose `output_root`, covering up to L2 block `l2_block_number`.
    /// Sends the tx, waits for inclusion, and returns its hash.
    ///
    /// # Errors
    ///
    /// Returns an error if the L1 provider rejects the transaction or it
    /// never confirms.
    pub async fn propose_output(
        &self,
        output_root: B256,
        l2_block_number: u64,
    ) -> Result<TxHash, AttesterError> {
        let oracle = IWithdrawalOutputOracle::new(self.oracle, self.provider.clone());
        let receipt = oracle
            .proposeOutput(output_root, l2_block_number)
            .send()
            .await?
            .get_receipt()
            .await?;
        Ok(receipt.transaction_hash)
    }

    /// Number of outputs already proposed. Used to resume, or skip, on restart.
    ///
    /// # Errors
    ///
    /// Returns an error if the L1 call fails.
    pub async fn output_count(&self) -> Result<u64, AttesterError> {
        let oracle = IWithdrawalOutputOracle::new(self.oracle, self.provider.clone());
        let n = oracle.outputCount().call().await?;
        u64::try_from(n).map_err(|_| AttesterError::Provider("output count exceeds u64".into()))
    }

    /// Highest L2 block covered by a non-deleted on-chain output, or `None`
    /// if none exists. This is the resume floor. Deleted, challenged
    /// outputs are skipped, so a restarted attester re-attests the
    /// challenged range, with its leaves re-collected from the validator's
    /// replay, instead of leaving it stranded.
    ///
    /// # Errors
    ///
    /// Returns an error if the L1 call fails.
    pub async fn latest_attested_block(&self) -> Result<Option<u64>, AttesterError> {
        let oracle = IWithdrawalOutputOracle::new(self.oracle, self.provider.clone());
        for i in (0..self.output_count().await?).rev() {
            let o = oracle.getOutput(U256::from(i)).call().await?;
            if !o.deleted {
                return Ok(Some(o.l2BlockNumber));
            }
            tracing::warn!(
                output_index = i,
                l2_block = o.l2BlockNumber,
                "on-chain output was deleted by a challenge; resuming attestation below it"
            );
        }
        Ok(None)
    }
}

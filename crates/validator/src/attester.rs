//! Withdrawal attester: the validator's L1-facing seam.
//!
//! After the validator commits a block range and advances its MPT state root,
//! the attester (1) collects the withdrawals initiated in that range from the
//! re-executed block receipts, (2) builds the output root
//! `keccak(VERSION ++ stateRoot ++ withdrawalsRoot)`, and (3) posts it to the L1
//! `WithdrawalOutputOracle`. It is off the hot path and posts with its own L1
//! key — a dishonest attester is caught by the (milestone-1 permissioned,
//! later ZK) challenge.
//!
//! This module is split so the pure parts ([`collect_withdrawal_leaves`],
//! [`build_output`]) are unit-tested in isolation and the [`OutputPoster`] is
//! exercised against anvil by the integration test.

use alloy_network::Ethereum;
use alloy_primitives::{B256, TxHash, U256};
use alloy_provider::Provider;
use alloy_sol_types::sol;
use kardamom_types::BlockDelta;
use kardamom_types::withdrawals;

sol! {
    #[sol(rpc)]
    contract IWithdrawalOutputOracle {
        function proposeOutput(bytes32 outputRoot, uint64 l2BlockNumber) external returns (uint256);
        function outputCount() external view returns (uint256);
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

/// Collect the withdrawal leaves initiated in `delta`'s block range, ordered by
/// withdrawal nonce. Scans every receipt's logs for `MessagePassed` events from
/// the `L2ToL1MessagePasser` predeploy. Returns the leaves in the canonical
/// order the on-chain Merkle proof indexes into.
pub fn collect_withdrawal_leaves(delta: &BlockDelta) -> Vec<B256> {
    let mut found: Vec<(U256, B256)> = Vec::new();
    for receipt in &delta.receipts {
        for log in &receipt.logs {
            if log.address != withdrawals::MESSAGE_PASSER {
                continue;
            }
            if let Some((nonce, leaf)) = withdrawals::decode_message_passed(&log.topics, &log.data) {
                found.push((nonce, leaf));
            }
        }
    }
    found.sort_by_key(|(nonce, _)| *nonce);
    found.into_iter().map(|(_, leaf)| leaf).collect()
}

/// The committed output for a block range: its withdrawals root and the output
/// root posted to L1.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Output {
    pub state_root: B256,
    pub withdrawals_root: B256,
    pub output_root: B256,
}

/// Build the output root from the committed `state_root` and the range's
/// withdrawal `leaves`.
pub fn build_output(state_root: B256, leaves: &[B256]) -> Output {
    let withdrawals_root = withdrawals::withdrawals_root(leaves);
    let output_root = withdrawals::output_root(state_root, withdrawals_root);
    Output {
        state_root,
        withdrawals_root,
        output_root,
    }
}

/// Posts output roots to the L1 [`WithdrawalOutputOracle`]. Generic over the
/// alloy provider so it works with a wallet-backed HTTP provider in production
/// and an anvil-backed provider in tests.
pub struct OutputPoster<P> {
    provider: P,
    oracle: alloy_primitives::Address,
}

impl<P: Provider<Ethereum> + Clone> OutputPoster<P> {
    pub fn new(provider: P, oracle: alloy_primitives::Address) -> Self {
        Self { provider, oracle }
    }

    /// Propose `output_root` covering up to L2 block `l2_block_number`. Sends the
    /// tx, waits for inclusion, returns its hash.
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

    /// Number of outputs already proposed (used to resume / skip on restart).
    pub async fn output_count(&self) -> Result<u64, AttesterError> {
        let oracle = IWithdrawalOutputOracle::new(self.oracle, self.provider.clone());
        let n = oracle.outputCount().call().await?;
        Ok(n.try_into().unwrap_or(u64::MAX))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloy_primitives::{Address, U256};
    use alloy_sol_types::SolValue;
    use kardamom_types::{Receipt, WireLog};

    /// Build a MessagePassed log the way the predeploy emits it.
    fn message_passed_log(nonce: u64, sender: Address, target: Address, value: u64) -> WireLog {
        let leaf =
            withdrawals::withdrawal_leaf(U256::from(nonce), sender, target, U256::from(value));
        let mut data = Vec::new();
        data.extend_from_slice(&U256::from(value).to_be_bytes::<32>());
        data.extend_from_slice(leaf.as_slice());
        WireLog {
            address: withdrawals::MESSAGE_PASSER,
            topics: vec![
                withdrawals::message_passed_topic0(),
                B256::from(U256::from(nonce)),
                B256::from_slice(&(sender,).abi_encode()),
                B256::from_slice(&(target,).abi_encode()),
            ],
            data: data.into(),
        }
    }

    fn delta_with_logs(logs: Vec<WireLog>) -> BlockDelta {
        BlockDelta {
            block_number: 1,
            accounts: vec![],
            storage: vec![],
            code: vec![],
            receipts: vec![Receipt {
                logs,
                ..Default::default()
            }],
        }
    }

    #[test]
    fn collects_and_orders_leaves() {
        let s = Address::from([0x11; 20]);
        let t = Address::from([0x22; 20]);
        // Emit out of order; expect nonce-sorted leaves.
        let delta = delta_with_logs(vec![
            message_passed_log(1, s, t, 200),
            message_passed_log(0, s, t, 100),
        ]);
        let leaves = collect_withdrawal_leaves(&delta);
        assert_eq!(leaves.len(), 2);
        assert_eq!(leaves[0], withdrawals::withdrawal_leaf(U256::ZERO, s, t, U256::from(100u64)));
        assert_eq!(
            leaves[1],
            withdrawals::withdrawal_leaf(U256::from(1u64), s, t, U256::from(200u64))
        );
    }

    #[test]
    fn ignores_non_message_passed_logs() {
        let mut foreign = message_passed_log(0, Address::ZERO, Address::ZERO, 1);
        foreign.address = Address::from([0xff; 20]); // not the predeploy
        let delta = delta_with_logs(vec![foreign]);
        assert!(collect_withdrawal_leaves(&delta).is_empty());
    }

    #[test]
    fn build_output_matches_types_helpers() {
        let sr = B256::from([0xab; 32]);
        let s = Address::from([0x11; 20]);
        let t = Address::from([0x22; 20]);
        let leaves =
            vec![withdrawals::withdrawal_leaf(U256::ZERO, s, t, U256::from(5u64))];
        let out = build_output(sr, &leaves);
        assert_eq!(out.withdrawals_root, withdrawals::withdrawals_root(&leaves));
        assert_eq!(out.output_root, withdrawals::output_root(sr, out.withdrawals_root));
    }
}

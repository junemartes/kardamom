//! `TransfersWorkflow` — saturate the node's write path with signed value
//! transfers.

use alloy_primitives::{B256, Bytes, U256};
use jsonrpsee::core::client::ClientT;
use jsonrpsee::http_client::HttpClient;
use jsonrpsee::rpc_params;
use kardamom_node::AllocEntry;

use crate::mnemonic;
use crate::signers::presign_transfers;
use crate::workflow::{BenchWorkflow, default_signer_balance};
use crate::workflows::{ANVIL_MNEMONIC, TRANSFER_SINK};

const METHOD: &str = "eth_sendRawTransaction";

/// Built-in workflow: presigned-value-transfer load (`eth_sendRawTransaction`).
/// Stresses the node's write path.
#[derive(Debug, Clone)]
pub struct TransfersWorkflow {
    /// BIP-39 phrase the signers are derived from.
    pub mnemonic: String,
    /// Balance each prefunded signer EOA gets in genesis.
    pub signer_balance: U256,
}

impl Default for TransfersWorkflow {
    fn default() -> Self {
        Self {
            mnemonic: ANVIL_MNEMONIC.to_string(),
            signer_balance: default_signer_balance(),
        }
    }
}

impl BenchWorkflow for TransfersWorkflow {
    type Item = Bytes;

    fn name(&self) -> &'static str {
        "transfers"
    }

    fn methods(&self) -> &'static [&'static str] {
        &[METHOD]
    }

    fn genesis_alloc(&self, n_tasks: u32) -> anyhow::Result<Vec<AllocEntry>> {
        prefunded_signer_allocs(&self.mnemonic, n_tasks, self.signer_balance)
    }

    async fn prepare(
        &self,
        client: &HttpClient,
        n_tasks: u32,
        txs_per_task: u32,
    ) -> anyhow::Result<Vec<Vec<Self::Item>>> {
        let chain_id = preflight_chain_id(client).await?;
        let signers = mnemonic::derive_signers(&self.mnemonic, n_tasks)?;
        let mut tasks: Vec<Vec<Bytes>> = Vec::with_capacity(signers.len());
        for s in &signers {
            let presigned = presign_transfers(
                std::slice::from_ref(s),
                chain_id,
                TRANSFER_SINK,
                U256::from(1u64),
                txs_per_task as usize,
                0,
            )?;
            tasks.push(presigned);
        }
        Ok(tasks)
    }

    async fn dispatch(&self, client: &HttpClient, item: Bytes) -> (&'static str, bool) {
        let r: Result<B256, _> = client.request(METHOD, rpc_params![item]).await;
        (METHOD, r.is_ok())
    }
}

/// Build prefunded EOA allocs for `n_tasks` signers derived from `mnemonic`.
/// Shared with `MixedWorkflow` / `CallsWorkflow`.
pub(crate) fn prefunded_signer_allocs(
    mnemonic: &str,
    n_tasks: u32,
    balance: U256,
) -> anyhow::Result<Vec<AllocEntry>> {
    let signers = mnemonic::derive_signers(mnemonic, n_tasks)?;
    Ok(signers
        .into_iter()
        .map(|s| AllocEntry {
            address: s.address,
            balance,
            code: None,
            nonce: None,
        })
        .collect())
}

pub(crate) async fn preflight_chain_id(client: &HttpClient) -> anyhow::Result<u64> {
    let chain_id: U256 = client
        .request("eth_chainId", rpc_params![])
        .await
        .map_err(|e| anyhow::anyhow!("eth_chainId failed (is the node running?): {e}"))?;
    chain_id
        .try_into()
        .map_err(|e| anyhow::anyhow!("chain_id overflow: {e}"))
}

//! `CallsWorkflow` saturates the node's read path with a single
//! deterministic `eth_call`.

use alloy_primitives::{Address, Bytes, TxKind, U256};
use alloy_rpc_types_eth::{BlockNumberOrTag, TransactionRequest};
use jsonrpsee::core::client::ClientT;
use jsonrpsee::http_client::HttpClient;
use jsonrpsee::rpc_params;
use kardamom_types::AllocEntry;

use crate::benchmark::Prepared;
use crate::workflow::{BenchWorkflow, default_signer_balance};
use crate::workflows::transfers::{preflight_chain_id, prefunded_signer_allocs};
use crate::workflows::{
    ANVIL_MNEMONIC, DEFAULT_CALL_CONTRACT, WARMUP_PER_TASK, default_call_bytecode,
};

const METHOD: &str = "eth_call";

/// A built-in workflow with read-only `eth_call` load against a
/// deterministic contract. It stresses the node's read path.
#[derive(Debug, Clone)]
pub struct CallsWorkflow {
    /// The BIP-39 phrase the signers are derived from. `eth_call` does
    /// not strictly need EOAs, but the harness expects every workflow to
    /// declare them, so the same genesis works across runs.
    pub mnemonic: String,
    /// The balance each prefunded signer EOA gets in genesis.
    pub signer_balance: U256,
    /// The address of the contract that is the `to` value of every
    /// `eth_call`.
    pub contract: Address,
    /// The bytecode deployed at `contract` in the in-process genesis.
    pub contract_code: Bytes,
}

impl Default for CallsWorkflow {
    fn default() -> Self {
        Self {
            mnemonic: ANVIL_MNEMONIC.to_string(),
            signer_balance: default_signer_balance(),
            contract: DEFAULT_CALL_CONTRACT,
            contract_code: default_call_bytecode(),
        }
    }
}

impl BenchWorkflow for CallsWorkflow {
    type Item = ();

    fn name(&self) -> &'static str {
        "calls"
    }

    fn methods(&self) -> &'static [&'static str] {
        &[METHOD]
    }

    fn genesis_alloc(&self, n_tasks: u32) -> anyhow::Result<Vec<AllocEntry>> {
        let mut alloc = prefunded_signer_allocs(&self.mnemonic, n_tasks, self.signer_balance)?;
        alloc.push(AllocEntry {
            address: self.contract,
            balance: U256::ZERO,
            code: Some(self.contract_code.clone()),
            nonce: Some(1),
        });
        Ok(alloc)
    }

    async fn prepare(
        &self,
        client: &HttpClient,
        n_tasks: u32,
        txs_per_task: u32,
    ) -> anyhow::Result<Prepared<Self::Item>> {
        let _chain_id = preflight_chain_id(client).await?;
        // Check that the contract is deployed at the expected address.
        let req = TransactionRequest {
            to: Some(TxKind::Call(self.contract)),
            ..Default::default()
        };

        if let probe = client
            .request("eth_call", rpc_params![req, BlockNumberOrTag::Latest])
            .await
            && (probe.is_err() || probe.is_ok_and(|b: Bytes| b.is_empty()))
        {
            anyhow::bail!(
                "contract {addr} is not deployed (empty eth_call output). \
                 Check that the workflow's `genesis_alloc()` was used to \
                 build the node, or override `CallsWorkflow.contract` to \
                 point at a real deployment.",
                addr = self.contract,
            )
        }

        // `eth_call` has no per-item state. Warmup and main are only unit
        // markers. `dispatch` builds the actual request from `self` on
        // each iteration.
        let warmup = vec![(); WARMUP_PER_TASK * n_tasks as usize];
        let main = (0..n_tasks)
            .map(|_| vec![(); txs_per_task as usize])
            .collect();
        Ok(Prepared { warmup, main })
    }

    async fn dispatch(&self, client: &HttpClient, _item: ()) -> (&'static str, bool) {
        let req = TransactionRequest {
            to: Some(TxKind::Call(self.contract)),
            ..Default::default()
        };
        let r: Result<Bytes, _> = client
            .request(METHOD, rpc_params![req, BlockNumberOrTag::Latest])
            .await;
        (METHOD, r.is_ok())
    }
}

//! `CallsWorkflow` saturates the node's read path with a single
//! deterministic `eth_call`.

use alloy_primitives::{Address, Bytes, U256};
use alloy_rpc_types_eth::BlockNumberOrTag;
use jsonrpsee::core::client::ClientT;
use jsonrpsee::http_client::HttpClient;
use jsonrpsee::rpc_params;
use kardamom_types::AllocEntry;

use crate::benchmark::Prepared;
use crate::workflow::{BenchWorkflow, DispatchOutcome, default_signer_balance};
use crate::workflows::transfers::{allocs_with_contract, preflight_chain_id};
use crate::workflows::{
    ANVIL_MNEMONIC, DEFAULT_CALL_CONTRACT, WARMUP_PER_TASK, assert_contract_deployed, call_req,
    default_call_bytecode,
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
        allocs_with_contract(
            &self.mnemonic,
            n_tasks,
            self.signer_balance,
            self.contract,
            self.contract_code.clone(),
        )
    }

    async fn prepare(
        &self,
        client: &HttpClient,
        n_tasks: u32,
        txs_per_task: u32,
    ) -> anyhow::Result<Prepared<Self::Item>> {
        let _chain_id = preflight_chain_id(client).await?;
        assert_contract_deployed(client, self.contract).await?;

        // `eth_call` has no per-item state. Warmup and main are only unit
        // markers. `dispatch` builds the actual request from `self` on
        // each iteration.
        let warmup = vec![(); WARMUP_PER_TASK.saturating_mul(n_tasks as usize)];
        let main = (0..n_tasks)
            .map(|_| vec![(); txs_per_task as usize])
            .collect();
        Ok(Prepared { warmup, main })
    }

    async fn dispatch(&self, client: &HttpClient, _item: ()) -> DispatchOutcome {
        let r: Result<Bytes, _> = client
            .request(
                METHOD,
                rpc_params![call_req(self.contract), BlockNumberOrTag::Latest],
            )
            .await;
        DispatchOutcome {
            method: METHOD,
            success: r.is_ok(),
        }
    }
}

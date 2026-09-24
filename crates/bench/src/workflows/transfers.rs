//! `TransfersWorkflow` saturates the node's write path with signed
//! value transfers.

use alloy_primitives::{Address, B256, Bytes, U256};
use jsonrpsee::core::client::ClientT;
use jsonrpsee::http_client::HttpClient;
use jsonrpsee::rpc_params;
use kardamom_types::AllocEntry;

use crate::benchmark::Prepared;
use crate::mnemonic;
use crate::signers::{DerivedSigner, SignerSet, presign_transfers};
use crate::workflow::{BenchWorkflow, DispatchOutcome, default_signer_balance};
use crate::workflows::{ANVIL_MNEMONIC, TRANSFER_SINK, WARMUP_PER_TASK};

const METHOD: &str = "eth_sendRawTransaction";

/// A built-in workflow with presigned-value-transfer load, through
/// `eth_sendRawTransaction`. It stresses the node's write path.
#[derive(Debug, Clone)]
pub struct TransfersWorkflow {
    /// The BIP-39 phrase the signers are derived from.
    pub mnemonic: String,
    /// The balance each prefunded signer EOA gets in genesis.
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
    ) -> anyhow::Result<Prepared<Self::Item>> {
        let chain_id = preflight_chain_id(client).await?;
        let signers = SignerSet::derive(&self.mnemonic, n_tasks)?;
        // Warmup: one flat queue, rotating across all signers for nonces
        // 0 to WARMUP_PER_TASK. The warmup loop runs in order, so it keeps
        // per-signer nonce order: each signer's k-th transaction is
        // produced before any signer's (k+1)-th transaction.
        let warmup = presign_transfers(
            &signers,
            chain_id,
            TRANSFER_SINK,
            U256::from(1u64),
            WARMUP_PER_TASK.saturating_mul(signers.len()),
            0,
        )?;
        // Main: per-task chunks. Each signer signs its own `txs_per_task`
        // items, starting at nonce WARMUP_PER_TASK, so the sequence
        // continues where warmup left off.
        let main: Vec<Vec<Bytes>> = signers
            .iter()
            .map(|s| main_chunk_for_signer(s, chain_id, txs_per_task))
            .collect::<anyhow::Result<_>>()?;
        Ok(Prepared { warmup, main })
    }

    async fn dispatch(&self, client: &HttpClient, item: Bytes) -> DispatchOutcome {
        let r: Result<B256, _> = client.request(METHOD, rpc_params![item]).await;
        DispatchOutcome {
            method: METHOD,
            success: r.is_ok(),
        }
    }
}

/// One signer's main-phase chunk: its own `txs_per_task` presigned
/// transfers, starting at nonce `WARMUP_PER_TASK` (continuing where its
/// warmup nonces left off).
fn main_chunk_for_signer(
    s: &DerivedSigner,
    chain_id: u64,
    txs_per_task: u32,
) -> anyhow::Result<Vec<Bytes>> {
    let one = SignerSet::new(vec![s.clone()])?;
    presign_transfers(
        &one,
        chain_id,
        TRANSFER_SINK,
        U256::from(1u64),
        txs_per_task as usize,
        WARMUP_PER_TASK as u64,
    )
}

/// Build prefunded EOA allocations for `n_tasks` signers derived from
/// `mnemonic`. `MixedWorkflow` and `CallsWorkflow` share this function.
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

/// [`prefunded_signer_allocs`], plus one more `AllocEntry` for a
/// deployed contract at `contract`, with `code` and nonce 1. Both
/// `CallsWorkflow` and `MixedWorkflow`'s `genesis_alloc` need exactly
/// this: prefunded EOAs, and their fixed `eth_call` target already
/// deployed.
///
/// # Errors
///
/// Forwards errors from [`prefunded_signer_allocs`].
pub(crate) fn allocs_with_contract(
    mnemonic: &str,
    n_tasks: u32,
    balance: U256,
    contract: Address,
    code: Bytes,
) -> anyhow::Result<Vec<AllocEntry>> {
    let mut alloc = prefunded_signer_allocs(mnemonic, n_tasks, balance)?;
    alloc.push(AllocEntry {
        address: contract,
        balance: U256::ZERO,
        code: Some(code),
        nonce: Some(1),
    });
    Ok(alloc)
}

pub(crate) use crate::config::preflight_chain_id;

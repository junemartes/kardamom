use alloy_consensus::{Transaction, TxEnvelope};
use alloy_primitives::{Address, Bytes, U256};
use alloy_rpc_types_eth::TransactionRequest;
use revm::context::result::{ExecutionResult, HaltReason};
use revm::context::{BlockEnv, CfgEnv, TxEnv};
use revm::database::{DatabaseRef, WrapDatabaseRef};
use revm::primitives::TxKind;
use revm::{
    Context, Database, DatabaseCommit, ExecuteCommitEvm, ExecuteEvm, MainBuilder, MainContext,
};

use crate::error::NodeError;

/// Configuration that does not depend on the transaction being executed.
#[derive(Debug, Clone)]
pub struct ExecEnv {
    pub chain_id: u64,
    pub block_number: u64,
}

impl ExecEnv {
    pub fn block_env(&self) -> BlockEnv {
        let mut b = BlockEnv::default();
        b.number = U256::from(self.block_number);
        b
    }

    pub fn cfg_env(&self) -> CfgEnv {
        let mut c = CfgEnv::default();
        c.chain_id = self.chain_id;
        c
    }
}

/// Convert an RPC `TransactionRequest` to a revm `TxEnv` suitable for `eth_call`.
pub fn tx_env_from_request(req: &TransactionRequest) -> TxEnv {
    let mut tx = TxEnv::default();
    tx.caller = req.from.unwrap_or(Address::ZERO);
    // Default below the EIP-7825 cap (2^24 = 16_777_216) enforced by Osaka spec.
    tx.gas_limit = req.gas.unwrap_or(15_000_000);
    tx.gas_price = req.gas_price.unwrap_or(0);
    tx.kind = match req.to {
        Some(alloy_primitives::TxKind::Call(addr)) => TxKind::Call(addr),
        Some(alloy_primitives::TxKind::Create) | None => TxKind::Create,
    };
    tx.value = req.value.unwrap_or(U256::ZERO);
    tx.data = req.input.input().cloned().unwrap_or_default();
    tx.nonce = req.nonce.unwrap_or(0);
    tx.chain_id = req.chain_id;
    tx
}

/// Execute a transaction read-only: state changes are discarded.
pub fn call<DB>(db: &DB, env: ExecEnv, tx: TxEnv) -> Result<Bytes, NodeError>
where
    DB: DatabaseRef,
    DB::Error: std::fmt::Debug,
{
    let mut evm = Context::mainnet()
        .with_db(WrapDatabaseRef(db))
        .with_block(env.block_env())
        .with_cfg(env.cfg_env())
        .build_mainnet();

    let outcome = evm
        .transact(tx)
        .map_err(|e| NodeError::Execution(format!("{e:?}")))?;

    interpret(outcome.result)
}

fn interpret<H: std::fmt::Debug>(result: ExecutionResult<H>) -> Result<Bytes, NodeError> {
    match result {
        ExecutionResult::Success { output, .. } => Ok(output.into_data()),
        ExecutionResult::Revert { output, .. } => Err(NodeError::Execution(format!(
            "reverted: 0x{}",
            alloy_primitives::hex::encode(&output)
        ))),
        ExecutionResult::Halt { reason, .. } => {
            Err(NodeError::Execution(format!("halted: {reason:?}")))
        }
    }
}

/// Output of a committed transaction execution.
pub struct ExecOutput {
    pub result: ExecutionResult<HaltReason>,
    pub output_bytes: Bytes,
}

/// Convert a signed transaction envelope plus its recovered signer to a `TxEnv`.
pub fn tx_env_from_envelope(envelope: &TxEnvelope, signer: Address) -> TxEnv {
    let mut tx = TxEnv::default();
    tx.caller = signer;
    tx.chain_id = envelope.chain_id();
    tx.nonce = envelope.nonce();
    tx.gas_limit = envelope.gas_limit();
    tx.value = envelope.value();
    tx.data = envelope.input().clone();
    tx.kind = match envelope.to() {
        Some(addr) => TxKind::Call(addr),
        None => TxKind::Create,
    };
    // gas_price: prefer the legacy/effective gas price if reported, fall back to
    // the EIP-1559 max fee per gas (which is `u128`, not `Option<u128>`).
    tx.gas_price = envelope
        .gas_price()
        .unwrap_or_else(|| envelope.max_fee_per_gas());
    tx
}

/// Execute a transaction and commit state changes to `db`.
pub fn execute<DB>(db: &mut DB, env: ExecEnv, tx: TxEnv) -> Result<ExecOutput, NodeError>
where
    DB: Database + DatabaseCommit,
    <DB as Database>::Error: std::fmt::Debug,
{
    let mut evm = Context::mainnet()
        .with_db(db)
        .with_block(env.block_env())
        .with_cfg(env.cfg_env())
        .build_mainnet();

    let result = evm
        .transact_commit(tx)
        .map_err(|e| NodeError::Execution(format!("{e:?}")))?;

    let output_bytes = match &result {
        ExecutionResult::Success { output, .. } => output.data().clone(),
        ExecutionResult::Revert { output, .. } => output.clone(),
        ExecutionResult::Halt { .. } => Bytes::new(),
    };

    Ok(ExecOutput { result, output_bytes })
}

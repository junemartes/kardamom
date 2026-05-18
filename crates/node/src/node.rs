use std::collections::HashMap;
use std::sync::Arc;

use alloy_consensus::transaction::SignerRecoverable;
use alloy_consensus::{
    Eip658Value, Receipt, ReceiptEnvelope, ReceiptWithBloom, Transaction as _, TxEnvelope,
};
use alloy_primitives::{Address, B256, Bytes, U256};
use alloy_rlp::Decodable;
use alloy_rpc_types_eth::{Log as RpcLog, TransactionReceipt, TransactionRequest};
use revm::context::result::{ExecutionResult, HaltReason};
use revm::database::{CacheDB, EmptyDB};
use revm::primitives::KECCAK_EMPTY;
use revm::state::{AccountInfo, Bytecode};
use tokio::sync::RwLock;

use crate::error::NodeError;
use crate::executor::{self, ExecEnv};
use crate::genesis::Genesis;
use crate::metrics as kmetrics;
use crate::{stage, stage_await};

#[derive(Clone)]
pub struct Node {
    inner: Arc<RwLock<NodeState>>,
    chain_id: u64,
}

#[allow(dead_code)] // fields populated in later tasks
struct NodeState {
    block_number: u64,
    db: CacheDB<EmptyDB>,
    receipts: HashMap<B256, TransactionReceipt>,
}

impl Node {
    pub fn new(genesis: &Genesis) -> Self {
        let mut db = CacheDB::new(EmptyDB::default());
        for (addr, entry) in &genesis.alloc {
            let (code_hash, code) = match &entry.code {
                Some(bytes) => {
                    let bytecode = Bytecode::new_raw(bytes.clone());
                    (bytecode.hash_slow(), Some(bytecode))
                }
                None => (KECCAK_EMPTY, None),
            };
            db.insert_account_info(
                *addr,
                AccountInfo {
                    balance: entry.balance,
                    nonce: entry.nonce,
                    code_hash,
                    code,
                    ..Default::default()
                },
            );
        }
        Self {
            inner: Arc::new(RwLock::new(NodeState {
                block_number: 0,
                db,
                receipts: HashMap::new(),
            })),
            chain_id: genesis.chain_id,
        }
    }

    pub fn chain_id(&self) -> u64 {
        self.chain_id
    }

    pub async fn block_number(&self) -> u64 {
        self.inner.read().await.block_number
    }

    pub async fn balance(&self, addr: Address) -> U256 {
        let state = self.inner.read().await;
        state
            .db
            .cache
            .accounts
            .get(&addr)
            .map(|a| a.info.balance)
            .unwrap_or(U256::ZERO)
    }

    pub async fn nonce(&self, addr: Address) -> u64 {
        let state = self.inner.read().await;
        state
            .db
            .cache
            .accounts
            .get(&addr)
            .map(|a| a.info.nonce)
            .unwrap_or(0)
    }

    pub async fn receipt(&self, hash: B256) -> Option<TransactionReceipt> {
        self.inner.read().await.receipts.get(&hash).cloned()
    }

    pub async fn call(&self, req: TransactionRequest) -> Result<Bytes, NodeError> {
        const METHOD: &str = "eth_call";
        let state = stage_await!("acquire_read_lock", method = METHOD, self.inner.read());
        let tx = stage!("build_tx_env", method = METHOD, {
            executor::tx_env_from_request(&req)
        });
        let env = ExecEnv {
            chain_id: self.chain_id,
            block_number: state.block_number,
        };
        stage!("execute", method = METHOD, {
            executor::call(&state.db, env, tx)
        })
    }

    /// Decode an EIP-2718-encoded signed transaction, execute it against the
    /// node's mutable state, store a receipt, and bump the block number.
    pub async fn submit_raw_transaction(&self, bytes: Bytes) -> Result<B256, NodeError> {
        const METHOD: &str = "eth_sendRawTransaction";

        let envelope: TxEnvelope = stage!("decode", method = METHOD, {
            TxEnvelope::decode(&mut bytes.as_ref()).map_err(|e| NodeError::Decode(e.to_string()))?
        });
        let signer = stage!("recover_signer", method = METHOD, {
            envelope
                .recover_signer()
                .map_err(|_| NodeError::SignatureRecovery)?
        });
        let tx_hash = *envelope.tx_hash();

        let mut state = stage_await!("acquire_write_lock", method = METHOD, self.inner.write());
        let sealed_block = state.block_number + 1;
        let env = ExecEnv {
            chain_id: self.chain_id,
            block_number: sealed_block,
        };
        let tx = stage!("build_tx_env", method = METHOD, {
            executor::tx_env_from_envelope(&envelope, signer)
        });

        let out = stage!("execute", method = METHOD, {
            executor::execute(&mut state.db, env, tx)?
        });
        let receipt = stage!("build_receipt", method = METHOD, {
            build_receipt(&envelope, &out.result, signer, tx_hash, sealed_block)
        });

        stage!("store_receipt", method = METHOD, {
            state.receipts.insert(tx_hash, receipt);
            state.block_number = sealed_block;
        });

        kmetrics::set_block_number(sealed_block);
        Ok(tx_hash)
    }

}

fn build_receipt(
    envelope: &TxEnvelope,
    result: &ExecutionResult<HaltReason>,
    from: Address,
    tx_hash: B256,
    block_number: u64,
) -> TransactionReceipt {
    let (status, gas_used, logs) = match result {
        ExecutionResult::Success { logs, .. } => {
            let gas_used = result.tx_gas_used();
            let rpc_logs: Vec<RpcLog> = logs
                .iter()
                .enumerate()
                .map(|(i, log)| RpcLog {
                    inner: log.clone(),
                    log_index: Some(i as u64),
                    transaction_hash: Some(tx_hash),
                    block_number: Some(block_number),
                    ..Default::default()
                })
                .collect();
            (true, gas_used, rpc_logs)
        }
        ExecutionResult::Revert { .. } => (false, result.tx_gas_used(), Vec::new()),
        ExecutionResult::Halt { .. } => (false, result.tx_gas_used(), Vec::new()),
    };

    let inner_receipt: Receipt<RpcLog> = Receipt {
        status: Eip658Value::Eip658(status),
        cumulative_gas_used: gas_used,
        logs,
    };
    let with_bloom = ReceiptWithBloom::from(inner_receipt);
    let envelope_receipt = match envelope {
        TxEnvelope::Legacy(_) => ReceiptEnvelope::Legacy(with_bloom),
        TxEnvelope::Eip2930(_) => ReceiptEnvelope::Eip2930(with_bloom),
        TxEnvelope::Eip1559(_) => ReceiptEnvelope::Eip1559(with_bloom),
        TxEnvelope::Eip4844(_) => ReceiptEnvelope::Eip4844(with_bloom),
        TxEnvelope::Eip7702(_) => ReceiptEnvelope::Eip7702(with_bloom),
    };

    TransactionReceipt {
        inner: envelope_receipt,
        transaction_hash: tx_hash,
        transaction_index: Some(0),
        block_hash: None,
        block_number: Some(block_number),
        gas_used,
        effective_gas_price: 0,
        blob_gas_used: None,
        blob_gas_price: None,
        from,
        to: envelope.to(),
        contract_address: None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::genesis::{AllocEntry, Genesis};

    fn genesis_with(chain_id: u64, entries: Vec<(Address, AllocEntry)>) -> Genesis {
        Genesis {
            chain_id,
            alloc: entries.into_iter().collect(),
        }
    }

    fn funded(balance: U256) -> AllocEntry {
        AllocEntry { balance, code: None, nonce: 0 }
    }

    fn contract(code: Bytes) -> AllocEntry {
        AllocEntry { balance: U256::ZERO, code: Some(code), nonce: 1 }
    }

    #[test]
    fn chain_id_returns_configured_value() {
        let node = Node::new(&genesis_with(412346, vec![]));
        assert_eq!(node.chain_id(), 412346);
    }

    #[tokio::test]
    async fn balance_reflects_prefunded_amount() {
        let addr = Address::from([1u8; 20]);
        let node = Node::new(&genesis_with(1, vec![(addr, funded(U256::from(1000u64)))]));
        assert_eq!(node.balance(addr).await, U256::from(1000u64));
    }

    #[tokio::test]
    async fn unfunded_account_has_zero_balance() {
        let node = Node::new(&genesis_with(1, vec![]));
        let addr = Address::from([2u8; 20]);
        assert_eq!(node.balance(addr).await, U256::ZERO);
    }

    #[tokio::test]
    async fn block_number_starts_at_zero() {
        let node = Node::new(&genesis_with(1, vec![]));
        assert_eq!(node.block_number().await, 0);
    }

    #[tokio::test]
    async fn nonce_starts_at_zero_for_new_account() {
        let node = Node::new(&genesis_with(1, vec![]));
        let addr = Address::from([3u8; 20]);
        assert_eq!(node.nonce(addr).await, 0);
    }

    #[tokio::test]
    async fn unknown_receipt_returns_none() {
        let node = Node::new(&genesis_with(1, vec![]));
        assert!(node.receipt(B256::ZERO).await.is_none());
    }

    #[tokio::test]
    async fn call_returns_constant_from_bytecode() {
        use alloy_primitives::{TxKind, address, hex};
        use alloy_rpc_types_eth::TransactionRequest;

        let code = Bytes::from(hex!("604260005260206000f3").to_vec());
        let contract_addr = address!("0000000000000000000000000000000000001234");
        let caller = address!("0000000000000000000000000000000000005678");

        let node = Node::new(&genesis_with(
            1,
            vec![
                (caller, funded(U256::from(1_000_000_000u64))),
                (contract_addr, contract(code)),
            ],
        ));

        let req = TransactionRequest {
            from: Some(caller),
            to: Some(TxKind::Call(contract_addr)),
            ..Default::default()
        };

        let output = node.call(req).await.expect("call ok");
        let mut expected = [0u8; 32];
        expected[31] = 0x42;
        assert_eq!(output.as_ref(), &expected[..]);
    }

    #[tokio::test]
    async fn value_transfer_updates_balances_and_stores_receipt() {
        use alloy_consensus::{SignableTransaction, TxLegacy};
        use alloy_eips::eip2718::Encodable2718;
        use alloy_network::TxSignerSync;
        use alloy_primitives::TxKind as APTxKind;
        use alloy_signer_local::PrivateKeySigner;

        let signer = PrivateKeySigner::random();
        let from = signer.address();
        let to = Address::from([0x22u8; 20]);

        let mut tx = TxLegacy {
            chain_id: Some(1),
            nonce: 0,
            gas_price: 1_000_000_000,
            gas_limit: 21_000,
            to: APTxKind::Call(to),
            value: U256::from(1_000u64),
            input: Bytes::new(),
        };
        let sig = signer.sign_transaction_sync(&mut tx).expect("sign");
        let envelope: TxEnvelope = tx.into_signed(sig).into();
        let mut bytes = Vec::new();
        envelope.encode_2718(&mut bytes);

        let node = Node::new(&genesis_with(
            1,
            vec![(from, funded(U256::from(10u64).pow(U256::from(18u64))))],
        ));
        let hash = node
            .submit_raw_transaction(Bytes::from(bytes))
            .await
            .expect("submit ok");

        assert_eq!(node.balance(to).await, U256::from(1_000u64));
        assert_eq!(node.block_number().await, 1);
        let receipt = node.receipt(hash).await.expect("receipt present");
        assert_eq!(receipt.transaction_hash, hash);
        assert_eq!(receipt.from, from);
        assert_eq!(receipt.to, Some(to));
        assert_eq!(receipt.block_number, Some(1));
    }
}

use std::collections::{HashMap, HashSet};
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

struct NodeState {
    block_number: u64,
    db: CacheDB<EmptyDB>,
    receipts: HashMap<B256, TransactionReceipt>,
    applied_deposits: HashSet<B256>,
}

impl Node {
    pub fn new(genesis: &Genesis) -> Self {
        let mut db = CacheDB::new(EmptyDB::default());
        for entry in &genesis.alloc {
            let (code_hash, code) = match &entry.code {
                Some(bytes) => {
                    let bytecode = Bytecode::new_raw(bytes.clone());
                    (bytecode.hash_slow(), Some(bytecode))
                }
                None => (KECCAK_EMPTY, None),
            };
            let nonce = entry
                .nonce
                .unwrap_or_else(|| if entry.code.is_some() { 1 } else { 0 });
            db.insert_account_info(
                entry.address,
                AccountInfo {
                    balance: entry.balance,
                    nonce,
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
                applied_deposits: HashSet::new(),
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

    /// Decode an OP-style EIP-2718 deposit envelope, run the deposit (mint pre-credit +
    /// EVM call), store the receipt, and bump the block number. Rejects non-deposit
    /// envelopes with `InvalidDepositEnvelope`. The `source_hash` is consumed only after
    /// a successful execution so failed runs (e.g., `MintOverflow`) remain retryable.
    pub async fn submit_deposit_transaction(&self, bytes: Bytes) -> Result<B256, NodeError> {
        use alloy_eips::eip2718::Decodable2718;
        use op_alloy_consensus::OpTxEnvelope;

        let envelope = OpTxEnvelope::decode_2718(&mut bytes.as_ref())
            .map_err(|e| NodeError::Decode(e.to_string()))?;

        let dep: crate::deposit::DepositTx = match envelope {
            OpTxEnvelope::Deposit(sealed) => sealed.into_inner(),
            other => {
                return Err(NodeError::InvalidDepositEnvelope(format!(
                    "expected deposit tx, got {:?}",
                    other.tx_type()
                )));
            }
        };

        let tx_hash = alloy_primitives::keccak256(bytes.as_ref());

        let mut state = self.inner.write().await;
        if state.applied_deposits.contains(&dep.source_hash) {
            return Err(NodeError::DuplicateDeposit);
        }

        let sealed_block = state.block_number + 1;
        let env = ExecEnv {
            chain_id: self.chain_id,
            block_number: sealed_block,
        };

        let out = executor::execute_deposit(&mut state.db, env, &dep)?;

        let to_opt: Option<Address> = match dep.to {
            alloy_primitives::TxKind::Call(addr) => Some(addr),
            alloy_primitives::TxKind::Create => None,
        };
        let receipt = build_deposit_receipt(&out.result, dep.from, to_opt, tx_hash, sealed_block);

        state.applied_deposits.insert(dep.source_hash);
        state.receipts.insert(tx_hash, receipt);
        state.block_number = sealed_block;

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

fn build_deposit_receipt(
    result: &ExecutionResult<HaltReason>,
    from: Address,
    to: Option<Address>,
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
    // Wrap in Eip1559 as a placeholder; a dedicated deposit-receipt variant is future work.
    let envelope_receipt = ReceiptEnvelope::Eip1559(with_bloom);

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
        to,
        contract_address: None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::genesis::{AllocEntry, Genesis};

    fn genesis_with(chain_id: u64, alloc: Vec<AllocEntry>) -> Genesis {
        Genesis { chain_id, alloc }
    }

    fn funded(address: Address, balance: U256) -> AllocEntry {
        AllocEntry {
            address,
            balance,
            code: None,
            nonce: None,
        }
    }

    fn contract(address: Address, code: Bytes) -> AllocEntry {
        AllocEntry {
            address,
            balance: U256::ZERO,
            code: Some(code),
            nonce: None,
        }
    }

    #[test]
    fn chain_id_returns_configured_value() {
        let node = Node::new(&genesis_with(412346, vec![]));
        assert_eq!(node.chain_id(), 412346);
    }

    #[tokio::test]
    async fn balance_reflects_prefunded_amount() {
        let addr = Address::from([1u8; 20]);
        let node = Node::new(&genesis_with(1, vec![funded(addr, U256::from(1000u64))]));
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
                funded(caller, U256::from(1_000_000_000u64)),
                contract(contract_addr, code),
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
            vec![funded(from, U256::from(10u64).pow(U256::from(18u64)))],
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

    fn encode_deposit(dep: op_alloy_consensus::TxDeposit) -> Vec<u8> {
        use alloy_eips::eip2718::Encodable2718;
        let envelope: op_alloy_consensus::OpTxEnvelope = dep.into();
        let mut raw = Vec::new();
        envelope.encode_2718(&mut raw);
        raw
    }

    #[tokio::test]
    async fn deposit_happy_path_credits_and_stores_receipt() {
        use alloy_primitives::TxKind as APTxKind;
        use op_alloy_consensus::TxDeposit;

        let from = Address::from([0xAAu8; 20]);
        let to = Address::from([0xBBu8; 20]);

        let dep = TxDeposit {
            source_hash: B256::repeat_byte(0x01),
            from,
            to: APTxKind::Call(to),
            mint: 1_000u128,
            value: U256::from(400u64),
            gas_limit: 200_000,
            is_system_transaction: false,
            input: Bytes::new(),
        };

        let node = Node::new(&genesis_with(1, vec![]));
        let tx_hash = node
            .submit_deposit_transaction(Bytes::from(encode_deposit(dep)))
            .await
            .expect("submit ok");

        assert_eq!(node.balance(from).await, U256::from(600u64));
        assert_eq!(node.balance(to).await, U256::from(400u64));
        assert_eq!(node.block_number().await, 1);
        let r = node.receipt(tx_hash).await.expect("receipt present");
        assert_eq!(r.transaction_hash, tx_hash);
    }

    #[tokio::test]
    async fn deposit_replay_returns_duplicate_error() {
        use alloy_primitives::TxKind as APTxKind;
        use op_alloy_consensus::TxDeposit;

        let make_dep = || TxDeposit {
            source_hash: B256::repeat_byte(0x02),
            from: Address::from([0xAAu8; 20]),
            to: APTxKind::Call(Address::from([0xBBu8; 20])),
            mint: 10u128,
            value: U256::ZERO,
            gas_limit: 100_000,
            is_system_transaction: false,
            input: Bytes::new(),
        };

        let node = Node::new(&genesis_with(1, vec![]));
        node.submit_deposit_transaction(Bytes::from(encode_deposit(make_dep())))
            .await
            .unwrap();
        let err = node
            .submit_deposit_transaction(Bytes::from(encode_deposit(make_dep())))
            .await
            .unwrap_err();
        assert!(matches!(err, NodeError::DuplicateDeposit));
    }

    #[tokio::test]
    async fn deposit_revert_target_preserves_mint_on_node() {
        use alloy_primitives::TxKind as APTxKind;
        use op_alloy_consensus::TxDeposit;

        let from = Address::from([0xCCu8; 20]);
        let revert_addr = Address::from([0xDDu8; 20]);
        // 60 00 60 00 fd  ; REVERT
        let code = Bytes::from(vec![0x60, 0x00, 0x60, 0x00, 0xfd]);

        let node = Node::new(&genesis_with(1, vec![contract(revert_addr, code)]));

        let dep = TxDeposit {
            source_hash: B256::repeat_byte(0x03),
            from,
            to: APTxKind::Call(revert_addr),
            mint: 500u128,
            value: U256::from(100u64),
            gas_limit: 200_000,
            is_system_transaction: false,
            input: Bytes::new(),
        };
        node.submit_deposit_transaction(Bytes::from(encode_deposit(dep)))
            .await
            .unwrap();
        assert_eq!(node.balance(from).await, U256::from(500u64));
        assert_eq!(node.balance(revert_addr).await, U256::ZERO);
    }

    #[tokio::test]
    async fn deposit_wrong_envelope_type_returns_invalid_envelope() {
        // Encode an EIP-1559 envelope and submit it; expect InvalidDepositEnvelope.
        use alloy_consensus::{SignableTransaction, TxEip1559, TxEnvelope};
        use alloy_eips::eip2718::Encodable2718;
        use alloy_network::TxSignerSync;
        use alloy_primitives::TxKind as APTxKind;
        use alloy_signer_local::PrivateKeySigner;

        let signer = PrivateKeySigner::random();
        let mut tx = TxEip1559 {
            chain_id: 1,
            nonce: 0,
            gas_limit: 21_000,
            max_fee_per_gas: 0,
            max_priority_fee_per_gas: 0,
            to: APTxKind::Call(Address::from([0x22u8; 20])),
            value: U256::from(1u64),
            input: Bytes::new(),
            access_list: Default::default(),
        };
        let sig = signer.sign_transaction_sync(&mut tx).unwrap();
        let env: TxEnvelope = tx.into_signed(sig).into();
        let mut raw = Vec::new();
        env.encode_2718(&mut raw);

        let node = Node::new(&genesis_with(1, vec![]));
        let err = node
            .submit_deposit_transaction(Bytes::from(raw))
            .await
            .unwrap_err();
        assert!(
            matches!(err, NodeError::InvalidDepositEnvelope(_)),
            "got {err:?}"
        );
    }

    #[tokio::test]
    async fn deposit_failed_execution_does_not_consume_source_hash() {
        use alloy_primitives::TxKind as APTxKind;
        use op_alloy_consensus::TxDeposit;

        let from = Address::from([0xEEu8; 20]);
        // Pre-fund `from` to U256::MAX so a u128 mint will overflow.
        let node = Node::new(&genesis_with(1, vec![funded(from, U256::MAX)]));

        let dup_source = B256::repeat_byte(0x77);

        let dep_overflow = TxDeposit {
            source_hash: dup_source,
            from,
            to: APTxKind::Call(Address::from([0xFFu8; 20])),
            mint: 1u128,
            value: U256::ZERO,
            gas_limit: 100_000,
            is_system_transaction: false,
            input: Bytes::new(),
        };

        let err = node
            .submit_deposit_transaction(Bytes::from(encode_deposit(dep_overflow)))
            .await
            .unwrap_err();
        assert!(matches!(err, NodeError::MintOverflow), "got {err:?}");

        // Same source_hash but a non-overflowing deposit (different `from`) must succeed.
        let benign_from = Address::from([0x01u8; 20]);
        let dep_ok = TxDeposit {
            source_hash: dup_source,
            from: benign_from,
            to: APTxKind::Call(Address::from([0xFFu8; 20])),
            mint: 100u128,
            value: U256::ZERO,
            gas_limit: 100_000,
            is_system_transaction: false,
            input: Bytes::new(),
        };
        node.submit_deposit_transaction(Bytes::from(encode_deposit(dep_ok)))
            .await
            .expect("second submission must succeed");
    }
}

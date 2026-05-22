use std::path::Path;
use std::sync::Arc;

use alloy_consensus::transaction::SignerRecoverable;
use alloy_consensus::{
    Eip658Value, Header, Receipt, ReceiptEnvelope, ReceiptWithBloom, Transaction as _, TxEnvelope,
};
use alloy_primitives::{Address, B256, Bytes, U256};
use alloy_rlp::Decodable;
use alloy_rpc_types_eth::{Log as RpcLog, TransactionReceipt, TransactionRequest};
use revm::context::result::{ExecutionResult, HaltReason};
use revm::database::{AccountState, CacheDB};
use revm::primitives::KECCAK_EMPTY;
use tokio::sync::Mutex;

use kardamom_state::codecs::AccountRow;
use kardamom_state::{BlockCommit, MdbxBackend, State};

use crate::error::NodeError;
use crate::executor::{self, ExecEnv};
use crate::genesis::Genesis;
use crate::metrics as kmetrics;
use crate::{stage, stage_await};

/// Persistent sequencer node. Reads go through MDBX-backed `State`; writes
/// take `write_lock` and atomically commit the resulting block.
#[derive(Clone)]
pub struct Node {
    state: State,
    backend: MdbxBackend,
    chain_id: u64,
    write_lock: Arc<Mutex<()>>,
}

impl Node {
    /// Open the state at `db_path`, initialize genesis on first open, and
    /// return a node ready to serve.
    pub fn new(genesis: &Genesis, db_path: &Path) -> Result<Self, NodeError> {
        let state = State::open(db_path, genesis.chain_id)?;
        let state_genesis = kardamom_state::state::Genesis {
            chain_id: genesis.chain_id,
            alloc: genesis
                .alloc
                .iter()
                .map(|e| kardamom_state::state::AllocEntry {
                    address: e.address,
                    balance: e.balance,
                    code: e.code.clone(),
                    nonce: e.nonce,
                })
                .collect(),
        };
        state.initialize_genesis(&state_genesis)?;
        let backend = MdbxBackend::new(state.clone());
        Ok(Self {
            state,
            backend,
            chain_id: genesis.chain_id,
            write_lock: Arc::new(Mutex::new(())),
        })
    }

    pub fn chain_id(&self) -> u64 {
        self.chain_id
    }

    pub async fn block_number(&self) -> u64 {
        self.state
            .latest_block_number()
            .ok()
            .flatten()
            .unwrap_or(0)
    }

    pub async fn balance(&self, addr: Address) -> U256 {
        self.state.balance(addr).unwrap_or(U256::ZERO)
    }

    pub async fn nonce(&self, addr: Address) -> u64 {
        self.state.nonce(addr).unwrap_or(0)
    }

    pub async fn code_at(&self, addr: Address) -> Bytes {
        self.state.code(addr).unwrap_or_default()
    }

    pub async fn receipt(&self, hash: B256) -> Option<TransactionReceipt> {
        self.state.receipt(hash).ok().flatten()
    }

    pub async fn call(&self, req: TransactionRequest) -> Result<Bytes, NodeError> {
        const METHOD: &str = "eth_call";
        let tx = stage!("build_tx_env", method = METHOD, {
            executor::tx_env_from_request(&req)
        });
        let env = ExecEnv {
            chain_id: self.chain_id,
            block_number: self.block_number().await,
        };
        stage!("execute", method = METHOD, {
            executor::call(&self.backend, env, tx)
        })
    }

    /// Decode an EIP-2718-encoded signed transaction, execute it against a
    /// fresh per-block overlay, and atomically commit the resulting block.
    /// Returns only after the MDBX write has fsync'd.
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

        // Bind the guard to a name (NOT `_`) so it lives until function
        // return. Renaming to `_` would drop it immediately and break the
        // read-then-commit atomicity invariant — every writer must observe
        // a consistent `latest_block_number` → `commit_block` window.
        let _guard = stage_await!("acquire_write_lock", method = METHOD, self.write_lock.lock());

        let (parent_number, sealed_block, env) = self.next_block_env()?;
        let tx_env = stage!("build_tx_env", method = METHOD, {
            executor::tx_env_from_envelope(&envelope, signer)
        });

        let mut overlay = CacheDB::new(self.backend.clone());
        let out = stage!("execute", method = METHOD, {
            executor::execute(&mut overlay, env, tx_env)?
        });
        let receipt = stage!("build_receipt", method = METHOD, {
            build_receipt(&envelope, &out.result, signer, tx_hash, sealed_block)
        });
        let header = stage!("build_header", method = METHOD, {
            self.build_header(sealed_block, parent_number, &out.result)?
        });
        let commit = stage!("build_commit", method = METHOD, {
            build_commit(
                sealed_block,
                header,
                &overlay,
                vec![(tx_hash, receipt)],
                vec![],
            )
        });
        stage!("commit_block", method = METHOD, {
            self.state.commit_block(commit)?
        });

        kmetrics::set_block_number(sealed_block);
        Ok(tx_hash)
    }

    /// Decode an OP-style EIP-2718 deposit envelope, run the deposit, and
    /// atomically commit the block. `source_hash` is consumed only after a
    /// successful execution.
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

        // Held across `is_deposit_applied` and `commit_block` so the
        // check + insert is atomic w.r.t. other writers. See the same
        // pattern in `submit_raw_transaction`.
        let _guard = self.write_lock.lock().await;

        if self.state.is_deposit_applied(dep.source_hash)? {
            return Err(NodeError::DuplicateDeposit);
        }

        let (parent_number, sealed_block, env) = self.next_block_env()?;

        let mut overlay = CacheDB::new(self.backend.clone());
        let out = executor::execute_deposit(&mut overlay, env, &dep)?;

        let to_opt: Option<Address> = match dep.to {
            alloy_primitives::TxKind::Call(addr) => Some(addr),
            alloy_primitives::TxKind::Create => None,
        };
        let receipt = build_deposit_receipt(&out.result, dep.from, to_opt, tx_hash, sealed_block);
        let header = self.build_header(sealed_block, parent_number, &out.result)?;

        let commit = build_commit(
            sealed_block,
            header,
            &overlay,
            vec![(tx_hash, receipt)],
            vec![dep.source_hash],
        );
        self.state.commit_block(commit)?;

        kmetrics::set_block_number(sealed_block);
        Ok(tx_hash)
    }

    /// Computes `(parent_number, sealed_block, ExecEnv)`. Caller must hold
    /// `write_lock`.
    fn next_block_env(&self) -> Result<(u64, u64, ExecEnv), NodeError> {
        let parent = self.state.latest_block_number()?.unwrap_or(0);
        let sealed = parent + 1;
        let env = ExecEnv {
            chain_id: self.chain_id,
            block_number: sealed,
        };
        Ok((parent, sealed, env))
    }

    fn build_header(
        &self,
        sealed_block: u64,
        parent_number: u64,
        result: &ExecutionResult<HaltReason>,
    ) -> Result<Header, NodeError> {
        let parent_hash = self.state.block_hash(parent_number)?.unwrap_or(B256::ZERO);
        Ok(Header {
            number: sealed_block,
            parent_hash,
            gas_used: result.tx_gas_used(),
            ..Default::default()
        })
    }
}

/// Walk a CacheDB after execution to collect every dirty account/storage/code
/// entry into a `BlockCommit`. Read-only loads (state `None`) are filtered
/// out. EIP-161 prune: touched-but-empty accounts are written as `None` so
/// `commit_block` deletes the row.
fn build_commit(
    block_number: u64,
    header: Header,
    overlay: &CacheDB<MdbxBackend>,
    receipts: Vec<(B256, TransactionReceipt)>,
    applied_deposits: Vec<B256>,
) -> BlockCommit {
    let mut accounts = Vec::new();
    let mut storage = Vec::new();
    let mut code = Vec::new();

    for (addr, db_account) in overlay.cache.accounts.iter() {
        match db_account.account_state {
            AccountState::None => continue, // read-only load
            AccountState::NotExisting => {
                // Selfdestructed: delete the row.
                accounts.push((*addr, None));
                continue;
            }
            AccountState::Touched | AccountState::StorageCleared => {}
        }
        let info = &db_account.info;
        if info.is_empty() {
            // EIP-161: prune empty touched accounts.
            accounts.push((*addr, None));
        } else {
            accounts.push((
                *addr,
                Some(AccountRow {
                    balance: info.balance,
                    nonce: info.nonce,
                    code_hash: info.code_hash,
                }),
            ));
        }
        for (slot_u256, value) in &db_account.storage {
            storage.push((*addr, B256::from(slot_u256.to_be_bytes()), *value));
        }
    }

    // Newly-deployed code lives in `cache.contracts`. Idempotent upserts make
    // it safe to re-write already-known hashes; we don't bother filtering.
    for (hash, bytecode) in overlay.cache.contracts.iter() {
        if *hash == KECCAK_EMPTY || bytecode.is_empty() {
            continue;
        }
        code.push((*hash, Bytes::from(bytecode.original_bytes().to_vec())));
    }

    BlockCommit {
        block_number,
        header,
        accounts,
        storage,
        code,
        receipts,
        applied_deposits,
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
    use tempfile::TempDir;

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

    /// Build a Node backed by a temp MDBX env. Returns `(dir, node)` —
    /// the `TempDir` must outlive the node, so callers bind it.
    fn boot(genesis: Genesis) -> (TempDir, Node) {
        let dir = TempDir::new().unwrap();
        let node = Node::new(&genesis, dir.path()).expect("node");
        (dir, node)
    }

    #[test]
    fn chain_id_returns_configured_value() {
        let (_dir, node) = boot(genesis_with(412346, vec![]));
        assert_eq!(node.chain_id(), 412346);
    }

    #[tokio::test]
    async fn balance_reflects_prefunded_amount() {
        let addr = Address::from([1u8; 20]);
        let (_dir, node) = boot(genesis_with(1, vec![funded(addr, U256::from(1000u64))]));
        assert_eq!(node.balance(addr).await, U256::from(1000u64));
    }

    #[tokio::test]
    async fn unfunded_account_has_zero_balance() {
        let (_dir, node) = boot(genesis_with(1, vec![]));
        let addr = Address::from([2u8; 20]);
        assert_eq!(node.balance(addr).await, U256::ZERO);
    }

    #[tokio::test]
    async fn block_number_starts_at_zero() {
        let (_dir, node) = boot(genesis_with(1, vec![]));
        assert_eq!(node.block_number().await, 0);
    }

    #[tokio::test]
    async fn nonce_starts_at_zero_for_new_account() {
        let (_dir, node) = boot(genesis_with(1, vec![]));
        let addr = Address::from([3u8; 20]);
        assert_eq!(node.nonce(addr).await, 0);
    }

    #[tokio::test]
    async fn unknown_receipt_returns_none() {
        let (_dir, node) = boot(genesis_with(1, vec![]));
        assert!(node.receipt(B256::ZERO).await.is_none());
    }

    #[tokio::test]
    async fn call_returns_constant_from_bytecode() {
        use alloy_primitives::{TxKind, address, hex};
        use alloy_rpc_types_eth::TransactionRequest;

        let code = Bytes::from(hex!("604260005260206000f3").to_vec());
        let contract_addr = address!("0000000000000000000000000000000000001234");
        let caller = address!("0000000000000000000000000000000000005678");

        let (_dir, node) = boot(genesis_with(
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

        let (_dir, node) = boot(genesis_with(
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

        let (_dir, node) = boot(genesis_with(1, vec![]));
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

        let (_dir, node) = boot(genesis_with(1, vec![]));
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
        let code = Bytes::from(vec![0x60, 0x00, 0x60, 0x00, 0xfd]);

        let (_dir, node) = boot(genesis_with(1, vec![contract(revert_addr, code)]));

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

        let (_dir, node) = boot(genesis_with(1, vec![]));
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
        let (_dir, node) = boot(genesis_with(1, vec![funded(from, U256::MAX)]));

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

    /// Persistence: submit a value-transfer, drop the node, reopen against
    /// the same path — recipient balance and block number are preserved.
    #[tokio::test]
    async fn restart_resumes_at_last_committed_block() {
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

        let dir = TempDir::new().unwrap();
        let g = genesis_with(
            1,
            vec![funded(from, U256::from(10u64).pow(U256::from(18u64)))],
        );
        let tx_hash = {
            let node = Node::new(&g, dir.path()).unwrap();
            let h = node
                .submit_raw_transaction(Bytes::from(bytes))
                .await
                .expect("submit ok");
            assert_eq!(node.balance(to).await, U256::from(1_000u64));
            h
        };

        // Reopen: state should be intact.
        let node = Node::new(&g, dir.path()).unwrap();
        assert_eq!(node.balance(to).await, U256::from(1_000u64));
        assert_eq!(node.block_number().await, 1);
        assert!(node.receipt(tx_hash).await.is_some());
    }

    #[tokio::test]
    async fn restart_preserves_applied_deposits() {
        use alloy_primitives::TxKind as APTxKind;
        use op_alloy_consensus::TxDeposit;

        let dir = TempDir::new().unwrap();
        let g = genesis_with(1, vec![]);

        let make_dep = || TxDeposit {
            source_hash: B256::repeat_byte(0x09),
            from: Address::from([0xAAu8; 20]),
            to: APTxKind::Call(Address::from([0xBBu8; 20])),
            mint: 10u128,
            value: U256::ZERO,
            gas_limit: 100_000,
            is_system_transaction: false,
            input: Bytes::new(),
        };

        {
            let node = Node::new(&g, dir.path()).unwrap();
            node.submit_deposit_transaction(Bytes::from(encode_deposit(make_dep())))
                .await
                .unwrap();
        }
        let node = Node::new(&g, dir.path()).unwrap();
        let err = node
            .submit_deposit_transaction(Bytes::from(encode_deposit(make_dep())))
            .await
            .unwrap_err();
        assert!(matches!(err, NodeError::DuplicateDeposit));
    }
}

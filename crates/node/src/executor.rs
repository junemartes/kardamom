use std::collections::HashMap;

use alloy_consensus::{Transaction, TxEnvelope};
use alloy_primitives::{Address, Bytes, U256};
use alloy_rpc_types_eth::TransactionRequest;
use revm::context::result::{ExecutionResult, HaltReason};
use revm::context::{BlockEnv, CfgEnv, TxEnv};
use revm::database::{DatabaseRef, WrapDatabaseRef};
use revm::primitives::TxKind;
use revm::state::Account;
use revm::{
    Context, Database, DatabaseCommit, ExecuteCommitEvm, ExecuteEvm, MainBuilder, MainContext,
};

use crate::deposit::DepositTx;
use crate::error::NodeError;

/// Configuration that does not depend on the transaction being executed.
#[derive(Debug, Clone)]
pub struct ExecEnv {
    pub chain_id: u64,
    pub block_number: u64,
}

impl ExecEnv {
    pub fn block_env(&self) -> BlockEnv {
        BlockEnv {
            number: U256::from(self.block_number),
            ..Default::default()
        }
    }

    // CfgEnv is #[non_exhaustive], so a struct literal with ..Default::default()
    // is rejected (E0639). Field-by-field assignment is the only option here.
    #[allow(clippy::field_reassign_with_default)]
    pub fn cfg_env(&self) -> CfgEnv {
        let mut c = CfgEnv::default();
        c.chain_id = self.chain_id;
        c
    }

    /// Config for read-only simulation paths (`eth_call`). Simulations skip
    /// the preconditions that only make sense for real transactions: nonce,
    /// caller balance, and base-fee enforcement. The caller pays nothing,
    /// nothing is committed to state, and the result is purely informational
    /// — so these checks would only reject otherwise-valid probes (calls from
    /// unfunded addresses, requests with `gas_price: 0`, stale nonces). This
    /// matches geth/reth `eth_call` semantics.
    pub fn simulation_cfg(&self) -> CfgEnv {
        let mut c = self.cfg_env();
        c.disable_nonce_check = true;
        c.disable_balance_check = true;
        c.disable_base_fee = true;
        c
    }
}

/// Convert an RPC `TransactionRequest` to a revm `TxEnv` suitable for `eth_call`.
///
/// Every value-bearing field must be supplied explicitly by the caller; a missing
/// field produces `NodeError::MissingRequestField` rather than silently
/// defaulting. JSON-RPC-level defaults (e.g. omitted `from`/`gas`/`value` in
/// an `eth_call` request) must be applied by the caller before invoking this
/// function — see `Node::call`.
///
/// The single exception is `to`: a `None` value is Ethereum's canonical
/// encoding for contract creation (equivalent to `Some(TxKind::Create)`), not
/// an unspecified default.
pub fn tx_env_from_request(req: &TransactionRequest) -> Result<TxEnv, NodeError> {
    Ok(TxEnv {
        caller: req.from.ok_or(NodeError::MissingRequestField("from"))?,
        gas_limit: req.gas.ok_or(NodeError::MissingRequestField("gas"))?,
        gas_price: req
            .gas_price
            .ok_or(NodeError::MissingRequestField("gas_price"))?,
        // `to: None` is Ethereum's canonical encoding of contract creation
        // (equivalent to `Some(Create)`), not an unspecified value.
        kind: match req.to {
            Some(alloy_primitives::TxKind::Call(addr)) => TxKind::Call(addr),
            Some(alloy_primitives::TxKind::Create) | None => TxKind::Create,
        },
        value: req.value.ok_or(NodeError::MissingRequestField("value"))?,
        data: req
            .input
            .input()
            .cloned()
            .ok_or(NodeError::MissingRequestField("input"))?,
        nonce: req.nonce.ok_or(NodeError::MissingRequestField("nonce"))?,
        chain_id: req.chain_id,
        ..Default::default()
    })
}

/// Execute a transaction read-only: state changes are discarded.
///
/// This is the **simulation** path: `ExecEnv::simulation_cfg` is used to
/// relax the nonce/balance/base-fee preconditions. See its doc for why.
/// For real transaction execution see `execute` / `execute_deposit`.
pub fn call<DB>(db: &DB, env: ExecEnv, tx: TxEnv) -> Result<Bytes, NodeError>
where
    DB: DatabaseRef,
    DB::Error: std::fmt::Debug,
{
    let mut evm = Context::mainnet()
        .with_db(WrapDatabaseRef(db))
        .with_block(env.block_env())
        .with_cfg(env.simulation_cfg())
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
#[derive(Debug)]
pub struct ExecOutput {
    pub result: ExecutionResult<HaltReason>,
    pub output_bytes: Bytes,
}

/// Convert a signed transaction envelope plus its recovered signer to a `TxEnv`.
pub fn tx_env_from_envelope(envelope: &TxEnvelope, signer: Address) -> TxEnv {
    TxEnv {
        caller: signer,
        chain_id: envelope.chain_id(),
        nonce: envelope.nonce(),
        gas_limit: envelope.gas_limit(),
        value: envelope.value(),
        data: envelope.input().clone(),
        kind: match envelope.to() {
            Some(addr) => TxKind::Call(addr),
            None => TxKind::Create,
        },
        // gas_price: prefer the legacy/effective gas price if reported, fall back to
        // the EIP-1559 max fee per gas (which is `u128`, not `Option<u128>`).
        gas_price: envelope
            .gas_price()
            .unwrap_or_else(|| envelope.max_fee_per_gas()),
        ..Default::default()
    }
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

    Ok(ExecOutput {
        result,
        output_bytes,
    })
}

/// Build a `TxEnv` from a deposit envelope. Deposits never deduct fees (`gas_price = 0`)
/// and do not assert chain-id at the envelope layer; the node enforces single-chain coherence
/// elsewhere. Nonce is left at zero — `execute_deposit` disables the nonce check.
pub fn tx_env_from_deposit(dep: &DepositTx) -> TxEnv {
    TxEnv {
        caller: dep.from,
        kind: match dep.to {
            alloy_primitives::TxKind::Call(addr) => TxKind::Call(addr),
            alloy_primitives::TxKind::Create => TxKind::Create,
        },
        value: dep.value,
        data: dep.input.clone(),
        gas_limit: dep.gas_limit,
        gas_price: 0,
        nonce: 0,
        chain_id: None,
        ..Default::default()
    }
}

/// Apply a deposit:
/// 1. Pre-credit `dep.from` with `dep.mint`. This commit happens BEFORE the EVM call,
///    so a revert inside the inner call does NOT roll it back.
/// 2. Run a normal EVM call `from → to` with `value`/`data`, fee-free, nonce-check off.
pub fn execute_deposit<DB>(
    db: &mut DB,
    env: ExecEnv,
    dep: &DepositTx,
) -> Result<ExecOutput, NodeError>
where
    DB: Database + DatabaseCommit,
    <DB as Database>::Error: std::fmt::Debug,
{
    // 1. Mint pre-credit. `dep.mint` is u128; widen to U256 for balance arithmetic.
    let mut info = db
        .basic(dep.from)
        .map_err(|e| NodeError::Execution(format!("{e:?}")))?
        .unwrap_or_default();
    info.balance = info
        .balance
        .checked_add(U256::from(dep.mint))
        .ok_or(NodeError::MintOverflow)?;

    // Account::from(info) yields status Loaded; mark_touch() forces commit to apply.
    let mut acct = Account::from(info);
    acct.mark_touch();
    let mut changes = HashMap::new();
    changes.insert(dep.from, acct);
    db.commit(changes.into_iter().collect());

    // 2. Inner EVM call. Disable the nonce check — deposits do not carry a nonce.
    let mut cfg = env.cfg_env();
    cfg.disable_nonce_check = true;

    let mut evm = Context::mainnet()
        .with_db(db)
        .with_block(env.block_env())
        .with_cfg(cfg)
        .build_mainnet();

    let result = evm
        .transact_commit(tx_env_from_deposit(dep))
        .map_err(|e| NodeError::Execution(format!("{e:?}")))?;

    let output_bytes = match &result {
        ExecutionResult::Success { output, .. } => output.data().clone(),
        ExecutionResult::Revert { output, .. } => output.clone(),
        ExecutionResult::Halt { .. } => Bytes::new(),
    };

    Ok(ExecOutput {
        result,
        output_bytes,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::deposit::DepositTx;
    use alloy_primitives::{B256, Bytes, TxKind as APTxKind, U256, address, b256};
    use alloy_rpc_types_eth::TransactionRequest;
    use revm::database::{CacheDB, EmptyDB};
    use revm::state::{AccountInfo, Bytecode};

    fn fully_populated_request() -> TransactionRequest {
        TransactionRequest {
            from: Some(Address::ZERO),
            to: Some(alloy_primitives::TxKind::Call(Address::ZERO)),
            gas: Some(21_000),
            gas_price: Some(0),
            value: Some(U256::ZERO),
            input: Bytes::new().into(),
            nonce: Some(0),
            ..Default::default()
        }
    }

    #[test]
    fn tx_env_from_request_succeeds_when_all_fields_present() {
        let req = fully_populated_request();
        let env = tx_env_from_request(&req).expect("ok");
        assert_eq!(env.caller, Address::ZERO);
        assert_eq!(env.gas_limit, 21_000);
    }

    #[test]
    fn tx_env_from_request_treats_to_none_and_explicit_create_as_create() {
        let mut req = fully_populated_request();
        req.to = None;
        assert_eq!(tx_env_from_request(&req).expect("ok").kind, TxKind::Create);

        req.to = Some(alloy_primitives::TxKind::Create);
        assert_eq!(tx_env_from_request(&req).expect("ok").kind, TxKind::Create);
    }

    #[test]
    fn tx_env_from_request_errors_on_missing_from() {
        let mut req = fully_populated_request();
        req.from = None;
        assert!(matches!(
            tx_env_from_request(&req),
            Err(NodeError::MissingRequestField("from"))
        ));
    }

    #[test]
    fn tx_env_from_request_errors_on_missing_gas() {
        let mut req = fully_populated_request();
        req.gas = None;
        assert!(matches!(
            tx_env_from_request(&req),
            Err(NodeError::MissingRequestField("gas"))
        ));
    }

    #[test]
    fn tx_env_from_request_errors_on_missing_gas_price() {
        let mut req = fully_populated_request();
        req.gas_price = None;
        assert!(matches!(
            tx_env_from_request(&req),
            Err(NodeError::MissingRequestField("gas_price"))
        ));
    }

    #[test]
    fn tx_env_from_request_errors_on_missing_value() {
        let mut req = fully_populated_request();
        req.value = None;
        assert!(matches!(
            tx_env_from_request(&req),
            Err(NodeError::MissingRequestField("value"))
        ));
    }

    #[test]
    fn tx_env_from_request_errors_on_missing_input() {
        let mut req = fully_populated_request();
        req.input = alloy_rpc_types_eth::TransactionInput::default();
        assert!(matches!(
            tx_env_from_request(&req),
            Err(NodeError::MissingRequestField("input"))
        ));
    }

    #[test]
    fn tx_env_from_request_errors_on_missing_nonce() {
        let mut req = fully_populated_request();
        req.nonce = None;
        assert!(matches!(
            tx_env_from_request(&req),
            Err(NodeError::MissingRequestField("nonce"))
        ));
    }

    #[test]
    fn simulation_cfg_disables_simulation_only_preconditions() {
        let env = ExecEnv {
            chain_id: 7,
            block_number: 1,
        };
        let cfg = env.simulation_cfg();
        assert_eq!(cfg.chain_id, 7);
        assert!(cfg.disable_nonce_check);
        assert!(cfg.disable_balance_check);
        assert!(cfg.disable_base_fee);
    }

    #[test]
    fn cfg_env_does_not_relax_preconditions_for_real_execution() {
        let env = ExecEnv {
            chain_id: 7,
            block_number: 1,
        };
        let cfg = env.cfg_env();
        assert!(!cfg.disable_nonce_check);
        assert!(!cfg.disable_balance_check);
        assert!(!cfg.disable_base_fee);
    }

    #[test]
    fn call_simulation_succeeds_from_unfunded_caller_with_nonzero_gas_price() {
        // PUSH1 0x42; PUSH1 0x00; MSTORE; PUSH1 0x20; PUSH1 0x00; RETURN
        // Returns the 32-byte value 0x42.
        let code = Bytes::from(vec![
            0x60, 0x42, 0x60, 0x00, 0x52, 0x60, 0x20, 0x60, 0x00, 0xf3,
        ]);
        let bytecode = Bytecode::new_raw(code);
        let code_hash = bytecode.hash_slow();
        let contract = Address::from([0x42u8; 20]);

        let mut db = make_db(&[]);
        db.insert_account_info(
            contract,
            AccountInfo {
                balance: U256::ZERO,
                nonce: 0,
                code_hash,
                code: Some(bytecode),
                ..Default::default()
            },
        );

        let env = ExecEnv {
            chain_id: 1,
            block_number: 1,
        };
        // gas_price > 0 forces revm's balance check unless `disable_balance_check`
        // is in effect — and the caller (Address::ZERO) is unfunded.
        let tx = TxEnv {
            caller: Address::ZERO,
            kind: TxKind::Call(contract),
            gas_limit: 100_000,
            gas_price: 1,
            value: U256::ZERO,
            data: Bytes::new(),
            nonce: 0,
            chain_id: Some(1),
            ..Default::default()
        };

        let output = call(&db, env, tx).expect("simulation must succeed from unfunded caller");
        let mut expected = [0u8; 32];
        expected[31] = 0x42;
        assert_eq!(output.as_ref(), &expected[..]);
    }

    #[test]
    fn tx_env_from_deposit_call_kind() {
        let dep = DepositTx {
            source_hash: b256!("0101010101010101010101010101010101010101010101010101010101010101"),
            from: address!("00000000000000000000000000000000000000aa"),
            to: APTxKind::Call(address!("00000000000000000000000000000000000000bb")),
            mint: 100u128,
            value: U256::from(50u64),
            gas_limit: 200_000,
            is_system_transaction: false,
            input: Bytes::from(vec![0xde, 0xad]),
        };
        let env = tx_env_from_deposit(&dep);
        assert_eq!(env.caller, dep.from);
        assert_eq!(
            env.kind,
            TxKind::Call(address!("00000000000000000000000000000000000000bb"))
        );
        assert_eq!(env.value, U256::from(50u64));
        assert_eq!(env.data.as_ref(), &[0xde, 0xad]);
        assert_eq!(env.gas_limit, 200_000);
        assert_eq!(env.gas_price, 0);
        assert_eq!(env.chain_id, None);
    }

    #[test]
    fn tx_env_from_deposit_create_kind() {
        let dep = DepositTx {
            source_hash: B256::ZERO,
            from: Address::ZERO,
            to: APTxKind::Create,
            mint: 0u128,
            value: U256::ZERO,
            gas_limit: 100_000,
            is_system_transaction: false,
            input: Bytes::new(),
        };
        let env = tx_env_from_deposit(&dep);
        assert_eq!(env.kind, TxKind::Create);
    }

    fn make_db(prefunded: &[(Address, U256)]) -> CacheDB<EmptyDB> {
        let mut db = CacheDB::new(EmptyDB::default());
        for (addr, bal) in prefunded {
            db.insert_account_info(
                *addr,
                AccountInfo {
                    balance: *bal,
                    nonce: 0,
                    ..Default::default()
                },
            );
        }
        db
    }

    fn exec_env() -> ExecEnv {
        ExecEnv {
            chain_id: 1,
            block_number: 1,
        }
    }

    fn dep(from: Address, to: APTxKind, mint: u128, value: u64, data: Bytes) -> DepositTx {
        DepositTx {
            source_hash: B256::ZERO,
            from,
            to,
            mint,
            value: U256::from(value),
            gas_limit: 1_000_000,
            is_system_transaction: false,
            input: data,
        }
    }

    #[test]
    fn execute_deposit_mints_and_forwards_value() {
        let from = Address::from([0x11u8; 20]);
        let to = Address::from([0x22u8; 20]);
        let mut db = make_db(&[]);
        let d = dep(from, APTxKind::Call(to), 1_000u128, 400, Bytes::new());

        execute_deposit(&mut db, exec_env(), &d).expect("ok");

        // from = mint - value = 600; to = value = 400. gas_price=0 so no fee deduction.
        assert_eq!(
            db.cache.accounts.get(&from).unwrap().info.balance,
            U256::from(600u64)
        );
        assert_eq!(
            db.cache.accounts.get(&to).unwrap().info.balance,
            U256::from(400u64)
        );
    }

    #[test]
    fn execute_deposit_mint_survives_inner_revert() {
        // Inline bytecode: PUSH1 0; PUSH1 0; REVERT (60 00 60 00 fd).
        let revert_code = Bytes::from(vec![0x60, 0x00, 0x60, 0x00, 0xfd]);
        let bytecode = Bytecode::new_raw(revert_code);
        let code_hash = bytecode.hash_slow();
        let revert_addr = Address::from([0x33u8; 20]);

        let from = Address::from([0x11u8; 20]);
        let mut db = make_db(&[]);
        db.insert_account_info(
            revert_addr,
            AccountInfo {
                balance: U256::ZERO,
                nonce: 1,
                code_hash,
                code: Some(bytecode),
                ..Default::default()
            },
        );

        let d = dep(
            from,
            APTxKind::Call(revert_addr),
            1_000u128,
            200,
            Bytes::new(),
        );
        execute_deposit(&mut db, exec_env(), &d).expect("ok (revert is OK at the executor layer)");

        // Mint pre-credit is OUTSIDE the EVM call: from keeps the full mint.
        assert_eq!(
            db.cache.accounts.get(&from).unwrap().info.balance,
            U256::from(1_000u64),
            "from must keep full mint after inner revert"
        );
        assert_eq!(
            db.cache.accounts.get(&revert_addr).unwrap().info.balance,
            U256::ZERO,
            "revert target must not retain value"
        );
    }

    #[test]
    fn execute_deposit_overflow_returns_mint_overflow() {
        let from = Address::from([0x11u8; 20]);
        let mut db = make_db(&[(from, U256::MAX)]);
        let d = dep(
            from,
            APTxKind::Call(Address::from([0x22u8; 20])),
            1u128,
            0,
            Bytes::new(),
        );

        let err = execute_deposit(&mut db, exec_env(), &d).unwrap_err();
        assert!(matches!(err, NodeError::MintOverflow), "got {err:?}");
    }
}

//! OP-aligned deposit execution ([`execute_deposit_tx`]): a durable mint
//! pre-credit, a fee-free inner EVM call, and `source_hash` as the
//! canonical id.

use alloy_primitives::U256;
use kardamom_types::{BPosition, Deposit, Receipt, StateDatabase, WireLog};
use revm::context::result::ExecutionResult;
use revm::database::CacheDB;
use revm::primitives::Log;
use revm::state::Account;
use revm::{Context, DatabaseCommit, ExecuteCommitEvm, MainBuilder, MainContext};

use alloc::format;
use alloc::vec::Vec;

use crate::block_env::ExecEnv;
use crate::delta::{PendingDelta, WriteSet};
use crate::error::ExecutorError;
use crate::exec_types::TxIndex;

use super::db::{SnapshotRef, seed_cache_layer};
use super::tx_env::tx_env_from_deposit;
use super::write_set::{record_writeset_into_bal, wire_log, write_set_from_cache};

/// Execute one [`kardamom_types::Deposit`] against a snapshot and the
/// current `PendingDelta`. Returns the receipt, plus a fresh per-tx
/// `WriteSet`.
///
/// Deposit semantics (OP-aligned, ported from the old
/// `crates/node/src/executor.rs`):
///
/// 1. Pre-credit `deposit.from` with `deposit.mint`. This commit happens
///    before the EVM call, so a revert inside the inner call does not
///    roll it back. The mint is durable.
/// 2. Run a normal EVM call from `from` to `to`, with `value` and `data`,
///    fee-free (`gas_price = 0`), with `disable_nonce_check = true`
///    (deposits do not carry a nonce).
/// 3. The receipt's `tx_hash` is the deposit's `source_hash`. Deposits
///    have no 2718-canonical hash on the inbound side, so `source_hash`
///    is the canonical id that ingress queries by.
#[allow(clippy::too_many_arguments)] // matches execute_tx's shape; see the
// matching allow on execute_tx for the reason.
pub fn execute_deposit_tx<S: StateDatabase>(
    snapshot: &S,
    parent: Option<&PendingDelta>,
    delta: &PendingDelta,
    env: ExecEnv,
    tx_idx: TxIndex,
    tx_position: BPosition,
    deposit: &Deposit,
    tx_index_in_block: u64,
    cumulative_gas_used_before: u64,
    // See `execute_tx`. Deposits capture writes only (their WriteSet
    // keys) through constructed accounts. The commit-cache shape loses
    // original values, so read attribution is not available here. The
    // executor and the validator both build deposit claims through this
    // same path, so the claims stay symmetric.
    bal: Option<(&mut revm::state::bal::Bal, u64)>,
) -> Result<(Receipt, WriteSet), ExecutorError> {
    // Layer the running delta on top of the snapshot through CacheDB, so
    // revm sees writes from earlier txs in the same block. This mirrors
    // execute_tx.
    let snap_ref = SnapshotRef { inner: snapshot };
    let mut cache: CacheDB<SnapshotRef<'_, S>> = CacheDB::new(snap_ref);
    // Seed layers in order: the parent (the previous block's writes,
    // while its commit is still fsyncing under pipelined commit) first,
    // then the live delta. Later inserts overwrite earlier ones, so the
    // view equals snapshot, parent, delta composed together.
    for layer in parent.into_iter().chain(core::iter::once(delta)) {
        seed_cache_layer(&mut cache, layer).map_err(|detail| ExecutorError::Execution {
            idx: tx_idx,
            detail,
        })?;
    }

    // (1) Mint pre-credit. `dep.mint` is a u128; widen it to U256 for
    // balance arithmetic. Commit unconditionally: the mint is durable no
    // matter the inner-call outcome.
    let mut info = revm::Database::basic(&mut cache, deposit.from)
        .map_err(|e| ExecutorError::Execution {
            idx: tx_idx,
            detail: format!("basic({:?}): {e:?}", deposit.from),
        })?
        .unwrap_or_default();
    info.balance = info
        .balance
        .checked_add(U256::from(deposit.mint))
        .ok_or_else(|| ExecutorError::Execution {
            idx: tx_idx,
            detail: format!(
                "mint overflow: account {:?} balance + mint {} would exceed U256::MAX",
                deposit.from, deposit.mint
            ),
        })?;
    let mut acct = Account::from(info);
    acct.mark_touch();
    // A single-entry commit, built with `once(...).collect()` instead of
    // a std `HashMap`. The exec core is `no_std` and must not depend on
    // `RandomState`.
    cache.commit(core::iter::once((deposit.from, acct)).collect());

    // (2) Inner EVM call. Disable the nonce check, since deposits do not
    // carry a nonce.
    let mut cfg = env.cfg_env();
    cfg.disable_nonce_check = true;

    let tx_env = tx_env_from_deposit(deposit);
    let mut evm = Context::mainnet()
        .with_db(&mut cache)
        .with_block(env.block_env())
        .with_cfg(cfg)
        .build_mainnet();
    let result = evm
        .transact_commit(tx_env)
        .map_err(|e| ExecutorError::Execution {
            idx: tx_idx,
            detail: format!("{e:?}"),
        })?;

    let gas_used = result.gas().tx_gas_used();
    let (status_success, logs) = match &result {
        ExecutionResult::Success { logs, .. } => (true, logs.clone()),
        ExecutionResult::Revert { .. } => (false, Vec::<Log>::new()),
        ExecutionResult::Halt { .. } => (false, Vec::<Log>::new()),
    };

    // Build the write set from revm's final-state cache. Both the mint
    // pre-credit and any inner-call writes add touched accounts.
    let ws = write_set_from_cache(&cache.cache);
    if let Some((bal, bal_index)) = bal {
        record_writeset_into_bal(bal, bal_index, &ws);
    }

    let write_set_hash = ws.hash();
    let wire_logs: Vec<WireLog> = logs.iter().map(wire_log).collect();
    let cumulative_gas_used = cumulative_gas_used_before + gas_used;

    let receipt = Receipt {
        tx_idx: tx_position,
        // Deposits' canonical id is the OP source_hash, not a 2718 keccak hash.
        tx_hash: deposit.source_hash,
        // L1-originated: this uses no L2 nonce. The filler `nonce: 0`
        // below is not a real nonce. Consumers branch on `tx_type`, never
        // on this value.
        tx_type: kardamom_types::TX_TYPE_DEPOSIT,
        status: status_success,
        gas_used,
        logs: wire_logs,
        write_set_hash,
        nonce: 0,
        from: deposit.from,
        to: deposit.to,
        contract_address: None,
        // Deposits pay no fee.
        effective_gas_price: 0,
        block_number: env.block_number,
        transaction_index: tx_index_in_block,
        cumulative_gas_used,
    };
    Ok((receipt, ws))
}

// -----------------------------------------------------------------
// Deposit-execution tests. These mirror the old `execute_deposit`
// scenarios from `crates/node/src/executor.rs`, ported to the new
// snapshot-and-delta and `kardamom_types::Deposit` shape.
// -----------------------------------------------------------------
#[cfg(test)]
mod tests {
    use super::*;
    use crate::executor::test_support::{boundary, pos};
    use crate::state::MockStateDatabase;
    use alloy_primitives::Bytes as AlloyBytes;
    use alloy_primitives::{Address, B256};
    use bytes::Bytes;
    use revm::primitives::KECCAK_EMPTY;
    use revm::state::Bytecode;

    fn dep(from: Address, to: Option<Address>, mint: u128, value: u64, data: Bytes) -> Deposit {
        Deposit {
            source_hash: B256::repeat_byte(0x42),
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
    fn deposit_mints_and_forwards_value() {
        let from = Address::from([0x11u8; 20]);
        let to = Address::from([0x22u8; 20]);
        let snap = MockStateDatabase::builder().build();
        let delta = PendingDelta::new();
        let env = ExecEnv::new(1, &boundary(1));

        let d = dep(from, Some(to), 1_000, 400, Bytes::new());
        let (receipt, ws) =
            execute_deposit_tx(&snap, None, &delta, env, TxIndex(0), pos(0), &d, 0, 0, None)
                .expect("execute");

        // Mint is 1000, value forwarded is 400, so:
        // - the sender ends at 1000 - 400 = 600
        // - the recipient ends at 400
        // (Gas price is 0, so there is no fee deduction.)
        assert_eq!(ws.account(&from).unwrap().1, U256::from(600u64));
        assert_eq!(ws.account(&to).unwrap().1, U256::from(400u64));

        // Receipt fields specific to deposits.
        assert_eq!(receipt.tx_hash, d.source_hash);
        assert!(receipt.status);
        assert_eq!(receipt.nonce, 0);
        assert_eq!(receipt.from, from);
        assert_eq!(receipt.to, Some(to));
        assert_eq!(receipt.contract_address, None);
        assert_eq!(receipt.effective_gas_price, 0);
        assert_eq!(receipt.block_number, 1);
        assert_eq!(receipt.transaction_index, 0);
        assert_eq!(receipt.cumulative_gas_used, receipt.gas_used);
    }

    #[test]
    fn deposit_mint_survives_inner_revert() {
        // Inline bytecode: PUSH1 0, PUSH1 0, REVERT (60 00 60 00 fd).
        let revert_code = AlloyBytes::from(vec![0x60, 0x00, 0x60, 0x00, 0xfd]);
        let bytecode = Bytecode::new_raw(revert_code);
        let code_hash = bytecode.hash_slow();
        let revert_addr = Address::from([0x33u8; 20]);

        let from = Address::from([0x11u8; 20]);
        let snap = MockStateDatabase::builder()
            .account(revert_addr, U256::ZERO, 1, code_hash)
            .code(
                code_hash,
                Bytes::copy_from_slice(bytecode.original_bytes().as_ref()),
            )
            .build();
        let delta = PendingDelta::new();
        let env = ExecEnv::new(1, &boundary(1));

        let d = dep(from, Some(revert_addr), 1_000, 200, Bytes::new());
        let (receipt, ws) =
            execute_deposit_tx(&snap, None, &delta, env, TxIndex(0), pos(0), &d, 0, 0, None)
                .expect("execute (revert is OK at the executor layer)");

        // Mint pre-credit happens outside the EVM call, so `from` keeps
        // the full mint.
        assert_eq!(
            ws.account(&from).unwrap().1,
            U256::from(1_000u64),
            "from must keep full mint after inner revert"
        );
        // The inner call reverted. The receipt status reflects that.
        assert!(!receipt.status, "inner-revert deposit yields status=false");
        // Recipient observed no transferred value.
        assert!(
            ws.account(&revert_addr).is_none_or(|a| a.1 == U256::ZERO),
            "revert target must not retain value"
        );
    }

    #[test]
    fn deposit_mint_overflow_returns_execution_error() {
        let from = Address::from([0x11u8; 20]);
        // The sender is pre-funded with U256::MAX, so any mint overflows.
        let snap = MockStateDatabase::builder()
            .account(from, U256::MAX, 0, KECCAK_EMPTY)
            .build();
        let delta = PendingDelta::new();
        let env = ExecEnv::new(1, &boundary(1));

        let d = dep(from, Some(Address::from([0x22u8; 20])), 1, 0, Bytes::new());
        let err = execute_deposit_tx(&snap, None, &delta, env, TxIndex(0), pos(0), &d, 0, 0, None)
            .unwrap_err();
        assert!(
            matches!(err, ExecutorError::Execution { ref detail, .. } if detail.contains("mint overflow")),
            "got {err:?}"
        );
    }
}

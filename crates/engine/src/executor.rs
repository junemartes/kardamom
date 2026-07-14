//! Per-tx revm execution. Adapted from `crates/node/src/executor.rs::execute`.
//!
//! Differences from the node executor:
//! - Reads come from a snapshot-backed `StateDatabase` via a `DatabaseRef`
//!   adapter, layered through revm's `CacheDB` so writes from earlier txs in
//!   the same block are observed.
//! - Writes are captured into a per-tx `WriteSet` (built from revm's
//!   `EvmState` output) and merged into the running `PendingDelta` by the
//!   actor.
//!
//!this module does **not** compute `tx_hash`. It copies
//! `tx_hash` (and `sender`) directly from the inbound `kardamom_types::
//! TxEnvelope`, which the proxy (S1) populated at the system boundary.

use alloy_consensus::Transaction;
use alloy_eips::eip2718::Decodable2718;
use alloy_primitives::Bytes as AlloyBytes;
use alloy_primitives::{Address, B256, U256};
use bytes::Bytes;
use kardamom_types::{BPosition, Deposit, Receipt, StateDatabase, TxEnvelope, WireLog};
use revm::context::TxEnv;
use revm::context::result::ExecutionResult;
use revm::database::{CacheDB, DatabaseRef};
use revm::primitives::{KECCAK_EMPTY, Log, TxKind};
use revm::state::{Account, AccountInfo, Bytecode};
use revm::{Context, DatabaseCommit, ExecuteCommitEvm, ExecuteEvm, MainBuilder, MainContext};
use std::collections::HashMap;

use crate::block_env::ExecEnv;
use crate::delta::{PendingDelta, WriteSet};
use crate::error::ExecutorError;
use crate::exec_types::{ReceiptStatus, TxIndex};

/// `revm::DatabaseRef` adapter for a `StateDatabase` snapshot. Reads only —
/// writes go through revm's per-tx state journal returned by `transact`.
pub struct SnapshotRef<'a, S: StateDatabase> {
    pub inner: &'a S,
}

impl<S: StateDatabase> DatabaseRef for SnapshotRef<'_, S> {
    type Error = StateRefError;

    fn basic_ref(&self, address: Address) -> Result<Option<AccountInfo>, Self::Error> {
        let a = self
            .inner
            .basic(address)
            .map_err(|e| StateRefError(e.to_string()))?;
        Ok(a.map(|(nonce, balance, code_hash)| AccountInfo {
            balance,
            nonce,
            code_hash,
            account_id: None,
            code: None,
        }))
    }

    fn code_by_hash_ref(&self, code_hash: B256) -> Result<Bytecode, Self::Error> {
        if code_hash == KECCAK_EMPTY {
            return Ok(Bytecode::default());
        }
        let raw = self
            .inner
            .code_by_hash(code_hash)
            .map_err(|e| StateRefError(e.to_string()))?;
        if raw.is_empty() {
            return Ok(Bytecode::default());
        }
        Ok(Bytecode::new_raw(AlloyBytes::from(raw)))
    }

    fn storage_ref(&self, address: Address, index: U256) -> Result<U256, Self::Error> {
        // kardamom-types' StateDatabase::storage takes a B256 key; revm uses
        // U256. The two are isomorphic — U256 is a 32-byte big-endian integer
        // and B256 is a 32-byte buffer.
        let key = B256::from(index.to_be_bytes::<32>());
        self.inner
            .storage(address, key)
            .map_err(|e| StateRefError(e.to_string()))
    }

    fn block_hash_ref(&self, _number: u64) -> Result<B256, Self::Error> {
        // The shipped kardamom-types StateDatabase trait does not expose
        // block_hash. v0 callers (BLOCKHASH opcode in executed contracts)
        // observe the zero hash — historically what an executor without an
        // ancestor cache returns until S6 wires one in.
        Ok(B256::ZERO)
    }
}

/// Concrete error type for `SnapshotRef`. We collapse the `StateDatabase`
/// associated error into a string so the `revm::Database` blanket impl can
/// see a single concrete type.
#[derive(Debug, thiserror::Error)]
#[error("snapshot ref error: {0}")]
pub struct StateRefError(pub String);

impl revm::database_interface::DBErrorMarker for StateRefError {}

/// Decode an `alloy_consensus::TxEnvelope` from the `raw_tx` bytes carried
/// in a `kardamom_types::TxEnvelope`. The signature is already verified by
/// the proxy (S1, S0); we just need the typed accessors to build a
/// revm `TxEnv`.
pub fn decode_alloy_envelope(
    raw_tx: &Bytes,
    tx_idx: TxIndex,
) -> Result<alloy_consensus::TxEnvelope, ExecutorError> {
    let mut slice: &[u8] = raw_tx.as_ref();
    alloy_consensus::TxEnvelope::decode_2718(&mut slice).map_err(|e| ExecutorError::Execution {
        idx: tx_idx,
        detail: format!("decode raw_tx: {e}"),
    })
}

/// Convert a recovered tx envelope into a `TxEnv`. `signer` is the proxy-
/// populated sender — never recomputed here (S0).
pub fn tx_env_from_alloy(alloy_env: &alloy_consensus::TxEnvelope, signer: Address) -> TxEnv {
    TxEnv {
        caller: signer,
        chain_id: alloy_env.chain_id(),
        nonce: alloy_env.nonce(),
        gas_limit: alloy_env.gas_limit(),
        value: alloy_env.value(),
        data: alloy_env.input().clone(),
        kind: match alloy_env.to() {
            Some(addr) => TxKind::Call(addr),
            None => TxKind::Create,
        },
        gas_price: alloy_env
            .gas_price()
            .unwrap_or_else(|| alloy_env.max_fee_per_gas()),
        ..Default::default()
    }
}

/// Execute one tx against a snapshot + the current PendingDelta. Returns the
/// receipt plus a fresh per-tx WriteSet. The caller folds the WriteSet into
/// the PendingDelta before invoking the next tx so later txs see the writes.
///
/// `inbound_envelope: &TxEnvelope` is `kardamom_types::TxEnvelope`. Its
/// `sender` and `tx_hash` are trusted unconditionally — the proxy (S1)
/// populated them at the system boundary. The executor **never recomputes
/// `tx_hash`** (S0) and **never recovers a sender** (S0); it
/// copies both fields straight through into the outbound `Receipt`.
///
/// `tx_index_in_block` is the zero-based index within the in-flight block
/// (resets at every `BlockBoundaryStart`). `cumulative_gas_used_before` is the
/// running gas sum for txs already executed in the same block; the returned
/// receipt's `cumulative_gas_used` equals this plus the new tx's `gas_used`.
#[allow(clippy::too_many_arguments)] // 8 args is the natural shape of an
// "execute one tx" entry point — packaging them into a struct would shuffle
// the noise around without reducing it.
pub fn execute_tx<S: StateDatabase>(
    snapshot: &S,
    delta: &PendingDelta,
    env: ExecEnv,
    tx_idx: TxIndex,
    tx_position: BPosition,
    inbound_envelope: &TxEnvelope,
    tx_index_in_block: u64,
    cumulative_gas_used_before: u64,
) -> Result<(Receipt, WriteSet), ExecutorError> {
    let alloy_env = decode_alloy_envelope(&inbound_envelope.raw_tx, tx_idx)?;
    let signer = inbound_envelope.sender; // trusted from proxy; no recovery
    let nonce = alloy_env.nonce();
    let to = alloy_env.to();
    // Effective gas price mirrors the value `tx_env_from_alloy` feeds revm:
    // legacy/2930 `gas_price` when present, otherwise the 1559/4844
    // `max_fee_per_gas` cap. v0 has basefee = 0 so the cap is what's paid.
    let effective_gas_price = alloy_env
        .gas_price()
        .unwrap_or_else(|| alloy_env.max_fee_per_gas());

    // Layer the running delta on top of the snapshot via CacheDB so revm
    // sees writes from earlier txs in the same block.
    let snap_ref = SnapshotRef { inner: snapshot };
    let mut cache: CacheDB<SnapshotRef<'_, S>> = CacheDB::new(snap_ref);

    for (addr, (nonce, balance, code_hash)) in &delta.accounts {
        let code = delta
            .code
            .get(code_hash)
            .cloned()
            .filter(|b| !b.is_empty())
            .map(|b| Bytecode::new_raw(AlloyBytes::from(b)));
        cache.insert_account_info(
            *addr,
            AccountInfo {
                balance: *balance,
                nonce: *nonce,
                code_hash: *code_hash,
                account_id: None,
                code,
            },
        );
    }
    for ((addr, key), value) in &delta.storage {
        let u_key = U256::from_be_bytes::<32>(key.0);
        cache
            .insert_account_storage(*addr, u_key, *value)
            .map_err(|e| ExecutorError::Execution {
                idx: tx_idx,
                detail: format!("seed storage: {e:?}"),
            })?;
    }

    let tx_env = tx_env_from_alloy(&alloy_env, signer);
    let mut evm = Context::mainnet()
        .with_db(&mut cache)
        .with_block(env.block_env())
        .with_cfg(env.cfg_env())
        .build_mainnet();

    let outcome = evm.transact(tx_env).map_err(|e| ExecutorError::Execution {
        idx: tx_idx,
        detail: format!("{e:?}"),
    })?;

    let gas_used = outcome.result.gas().tx_gas_used();
    let (status, logs) = match &outcome.result {
        ExecutionResult::Success { logs, .. } => (ReceiptStatus::Success, logs.clone()),
        ExecutionResult::Revert { .. } => (ReceiptStatus::Revert, Vec::new()),
        ExecutionResult::Halt { reason, .. } => (ReceiptStatus::Halt(reason.clone()), Vec::new()),
    };

    // Build the write set from revm's per-tx EvmState. Only touched / changed
    // accounts and slots are emitted, which keeps the per-tx hash stable
    // across replicas (revm's iteration is over an AddressMap; we re-sort
    // into BTreeMap inside WriteSet via insert).
    let ws = write_set_from_evm_state(&outcome.state);

    let write_set_hash = ws.hash();
    let wire_logs = logs.iter().map(wire_log).collect();
    let cumulative_gas_used = cumulative_gas_used_before + gas_used;
    // Contract address is meaningful only for successful CREATE txs.
    // Failed CREATEs and any CALL tx have `contract_address = None`.
    let contract_address = if to.is_none() && status.is_success() {
        Some(signer.create(nonce))
    } else {
        None
    };

    // CRITICAL (S0): copy `tx_hash` straight from the inbound envelope —
    // DO NOT recompute via keccak256(raw_tx). The proxy (S1) is the canonical
    // hash producer.
    let receipt = Receipt {
        tx_idx: tx_position,
        tx_hash: inbound_envelope.tx_hash,
        status: status.is_success(),
        gas_used,
        logs: wire_logs,
        write_set_hash,
        nonce,
        from: signer,
        to,
        contract_address,
        effective_gas_price,
        block_number: env.block_number,
        transaction_index: tx_index_in_block,
        cumulative_gas_used,
    };
    Ok((receipt, ws))
}

/// Execute one [`kardamom_types::Deposit`] against a snapshot + the current
/// `PendingDelta`. Returns the receipt plus a fresh per-tx `WriteSet`.
///
/// Deposit semantics (OP-aligned, ported from old `crates/node/src/executor.rs`):
/// 1. Pre-credit `deposit.from` with `deposit.mint`. This commit happens
///    *before* the EVM call, so a revert inside the inner call does NOT
///    roll it back — the mint is durable.
/// 2. Run a normal EVM call `from → to` with `value`/`data`, fee-free
///    (`gas_price = 0`), with `disable_nonce_check = true` (deposits do
///    not carry a nonce).
/// 3. The receipt's `tx_hash` is the deposit's `source_hash` (deposits have
///    no 2718-canonical hash on the inbound side; `source_hash` is the
///    canonical id ingress will query by).
#[allow(clippy::too_many_arguments)] // matches execute_tx's shape; see the
// equivalent allow on execute_tx for the rationale.
pub fn execute_deposit_tx<S: StateDatabase>(
    snapshot: &S,
    delta: &PendingDelta,
    env: ExecEnv,
    tx_idx: TxIndex,
    tx_position: BPosition,
    deposit: &Deposit,
    tx_index_in_block: u64,
    cumulative_gas_used_before: u64,
) -> Result<(Receipt, WriteSet), ExecutorError> {
    // Layer the running delta on top of the snapshot via CacheDB so revm
    // sees writes from earlier txs in the same block. Mirrors execute_tx.
    let snap_ref = SnapshotRef { inner: snapshot };
    let mut cache: CacheDB<SnapshotRef<'_, S>> = CacheDB::new(snap_ref);
    for (addr, (nonce, balance, code_hash)) in &delta.accounts {
        let code = delta
            .code
            .get(code_hash)
            .cloned()
            .filter(|b| !b.is_empty())
            .map(|b| Bytecode::new_raw(AlloyBytes::from(b)));
        cache.insert_account_info(
            *addr,
            AccountInfo {
                balance: *balance,
                nonce: *nonce,
                code_hash: *code_hash,
                account_id: None,
                code,
            },
        );
    }
    for ((addr, key), value) in &delta.storage {
        let u_key = U256::from_be_bytes::<32>(key.0);
        cache
            .insert_account_storage(*addr, u_key, *value)
            .map_err(|e| ExecutorError::Execution {
                idx: tx_idx,
                detail: format!("seed storage: {e:?}"),
            })?;
    }

    // (1) Mint pre-credit. `dep.mint` is u128; widen to U256 for balance
    // arithmetic. Commit unconditionally — the mint is durable regardless
    // of inner-call outcome.
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
    let mut changes: HashMap<Address, Account> = HashMap::new();
    changes.insert(deposit.from, acct);
    cache.commit(changes.into_iter().collect());

    // (2) Inner EVM call. Disable the nonce check — deposits do not carry
    // a nonce.
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
    // pre-credit and any inner-call writes contribute touched accounts.
    let ws = write_set_from_cache(&cache.cache);

    let write_set_hash = ws.hash();
    let wire_logs: Vec<WireLog> = logs.iter().map(wire_log).collect();
    let cumulative_gas_used = cumulative_gas_used_before + gas_used;

    let receipt = Receipt {
        tx_idx: tx_position,
        // Deposits' canonical id is the OP source_hash, NOT a 2718 keccak.
        tx_hash: deposit.source_hash,
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

/// Build a `TxEnv` from a deposit envelope. Deposits never deduct fees
/// (`gas_price = 0`), do not assert chain-id at the envelope layer, and
/// disable the nonce check at the caller's `cfg.disable_nonce_check = true`.
fn tx_env_from_deposit(dep: &Deposit) -> TxEnv {
    TxEnv {
        caller: dep.from,
        kind: match dep.to {
            Some(addr) => TxKind::Call(addr),
            None => TxKind::Create,
        },
        value: dep.value,
        data: AlloyBytes::copy_from_slice(dep.input.as_ref()),
        gas_limit: dep.gas_limit,
        gas_price: 0,
        nonce: 0,
        chain_id: None,
        ..Default::default()
    }
}

/// Build a `WriteSet` from CacheDB's accumulated cache. Unlike
/// [`write_set_from_evm_state`] (which iterates revm's per-tx
/// `EvmState`), this iterates `CacheDB::cache.accounts` after the deposit's
/// commit cycle is complete, so the resulting WriteSet covers BOTH the
/// mint pre-credit and any inner-call writes. Accounts in state `None`
/// (loaded but unchanged) and `NotExisting` (never observed) are skipped.
fn write_set_from_cache(state: &revm::database::Cache) -> WriteSet {
    let mut ws = WriteSet::default();
    for (addr, account) in state.accounts.iter() {
        match account.account_state {
            revm::database::AccountState::None | revm::database::AccountState::NotExisting => {
                continue;
            }
            revm::database::AccountState::Touched
            | revm::database::AccountState::StorageCleared => {}
        }
        let info = &account.info;
        ws.accounts
            .insert(*addr, (info.nonce, info.balance, info.code_hash));

        // CacheDB stores bytecode separately by code_hash; resolve via the
        // `contracts` map. KECCAK_EMPTY (canonical empty-code) and empty
        // bytecode are not worth shipping in the delta.
        if info.code_hash != KECCAK_EMPTY
            && let Some(code) = state.contracts.get(&info.code_hash)
            && !code.is_empty()
        {
            ws.code.insert(
                info.code_hash,
                Bytes::copy_from_slice(code.original_bytes().as_ref()),
            );
        }

        for (key, value) in &account.storage {
            let b_key = B256::from(key.to_be_bytes::<32>());
            ws.storage.insert((*addr, b_key), *value);
        }
    }
    ws
}

fn write_set_from_evm_state(state: &revm::state::EvmState) -> WriteSet {
    let mut ws = WriteSet::default();
    for (addr, account) in state.iter() {
        // Only emit accounts revm marked as touched. Untouched entries are
        // pure reads.
        if !account.is_touched() {
            continue;
        }
        let info = &account.info;
        ws.accounts
            .insert(*addr, (info.nonce, info.balance, info.code_hash));

        // Code bytes: if revm loaded fresh bytecode this tx, capture it once
        // keyed by code_hash. KECCAK_EMPTY is the canonical empty-code marker
        // and isn't worth shipping in the delta.
        if let Some(code) = info.code.as_ref()
            && info.code_hash != KECCAK_EMPTY
            && !code.is_empty()
        {
            ws.code.insert(
                info.code_hash,
                Bytes::copy_from_slice(code.original_bytes().as_ref()),
            );
        }

        for (key, slot) in account.storage.iter() {
            // Only record slots whose present_value differs from the
            // original_value. revm tracks both on `EvmStorageSlot`.
            if slot.original_value != slot.present_value {
                let b_key = B256::from(key.to_be_bytes::<32>());
                ws.storage.insert((*addr, b_key), slot.present_value);
            }
        }
    }
    ws
}

fn wire_log(log: &Log) -> WireLog {
    WireLog {
        address: log.address,
        topics: log.data.topics().to_vec(),
        data: Bytes::copy_from_slice(log.data.data.as_ref()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::state::MockStateDatabase;
    use alloy_consensus::{SignableTransaction, TxLegacy};
    use alloy_eips::eip2718::Encodable2718;
    use alloy_network::TxSignerSync;
    use alloy_primitives::{TxKind as APTxKind, U256, address, keccak256};
    use alloy_signer_local::PrivateKeySigner;
    use kardamom_types::{BPosition, BlockBoundaryStart, TxEnvelope as KtTxEnvelope};

    fn boundary(block_number: u64) -> BlockBoundaryStart {
        BlockBoundaryStart {
            block_number,
            end_tx_idx: BPosition {
                term_id: 0,
                term_offset: 0,
            },
            l2_timestamp: 0,
        }
    }

    fn pos(off: i32) -> BPosition {
        BPosition {
            term_id: 0,
            term_offset: off,
        }
    }

    fn signed_transfer(
        from: &PrivateKeySigner,
        to: Address,
        value: u64,
        nonce: u64,
    ) -> KtTxEnvelope {
        let mut tx = TxLegacy {
            chain_id: Some(1),
            nonce,
            gas_price: 0,
            gas_limit: 21_000,
            to: APTxKind::Call(to),
            value: U256::from(value),
            input: AlloyBytes::new(),
        };
        let sig = from.sign_transaction_sync(&mut tx).expect("sign");
        let alloy_env: alloy_consensus::TxEnvelope = tx.into_signed(sig).into();
        let raw_tx = Bytes::from(alloy_env.encoded_2718());
        let tx_hash = keccak256(&raw_tx);
        KtTxEnvelope {
            correlation_id: 0,
            raw_tx,
            sender: from.address(),
            tx_hash,
        }
    }

    #[test]
    fn simple_transfer_produces_write_set_and_success_receipt() {
        let signer = PrivateKeySigner::random();
        let from = signer.address();
        let to = address!("0000000000000000000000000000000000001234");

        let snap = MockStateDatabase::builder()
            .account(from, U256::from(10u128.pow(18)), 0, KECCAK_EMPTY)
            .build();
        let delta = PendingDelta::new();
        let env = ExecEnv::new(1, &boundary(1));

        let env_tx = signed_transfer(&signer, to, 1_000, 0);
        let (receipt, ws) =
            execute_tx(&snap, &delta, env, TxIndex(0), pos(0), &env_tx, 0, 0).expect("execute");

        //the receipt's tx_hash MUST equal the inbound envelope's
        // tx_hash byte-for-byte. No recomputation in the executor.
        assert_eq!(receipt.tx_hash, env_tx.tx_hash);
        assert!(receipt.status); // success = bool true (kardamom_types::Receipt)
        assert!(receipt.gas_used >= 21_000);
        // Both accounts touched: sender (balance + nonce) and recipient (balance).
        assert!(ws.accounts.contains_key(&from));
        assert!(ws.accounts.contains_key(&to));
        assert_eq!(ws.accounts[&to].1, U256::from(1_000u64));
        // No storage or code writes for a plain transfer.
        assert!(ws.storage.is_empty());
        assert!(ws.code.is_empty());

        // RPC enrichment populated by execute_tx.
        assert_eq!(receipt.from, from);
        assert_eq!(receipt.to, Some(to));
        assert_eq!(receipt.contract_address, None);
        assert_eq!(receipt.nonce, 0);
        assert_eq!(receipt.effective_gas_price, 0); // tx built with gas_price = 0
        assert_eq!(receipt.block_number, 1);
        assert_eq!(receipt.transaction_index, 0);
        assert_eq!(receipt.cumulative_gas_used, receipt.gas_used);
    }

    #[test]
    fn second_tx_sees_first_tx_balance_via_delta() {
        let signer = PrivateKeySigner::random();
        let from = signer.address();
        let to = address!("00000000000000000000000000000000000ABCDE");

        let snap = MockStateDatabase::builder()
            .account(from, U256::from(10u128.pow(18)), 0, KECCAK_EMPTY)
            .build();
        let env = ExecEnv::new(1, &boundary(1));

        let mut delta = PendingDelta::new();
        // First transfer.
        let tx1 = signed_transfer(&signer, to, 100, 0);
        let (r1, ws1) =
            execute_tx(&snap, &delta, env, TxIndex(0), pos(0), &tx1, 0, 0).expect("execute 1");
        assert!(r1.status);
        assert_eq!(r1.tx_hash, tx1.tx_hash); // copied, not recomputed
        assert_eq!(r1.nonce, 0);
        assert_eq!(r1.transaction_index, 0);
        let gas_after_tx1 = r1.cumulative_gas_used;
        delta.apply(ws1);

        // Second transfer from the same sender; nonce must be 1, sender
        // balance must already be debited 100.
        let tx2 = signed_transfer(&signer, to, 50, 1);
        let (r2, ws2) = execute_tx(
            &snap,
            &delta,
            env,
            TxIndex(1),
            pos(1),
            &tx2,
            1,
            gas_after_tx1,
        )
        .expect("execute 2");
        assert!(r2.status);
        assert_eq!(r2.tx_hash, tx2.tx_hash);
        assert_eq!(ws2.accounts[&to].1, U256::from(150u64));
        assert_eq!(ws2.accounts[&from].0, 2); // nonce
        // RPC enrichment: tx2 sees a higher nonce + transaction_index;
        // cumulative_gas_used accumulates across both txs in the block.
        assert_eq!(r2.nonce, 1);
        assert_eq!(r2.transaction_index, 1);
        assert_eq!(r2.cumulative_gas_used, gas_after_tx1 + r2.gas_used);
    }

    // -----------------------------------------------------------------
    // Deposit-execution tests (mirror of the old `execute_deposit`
    // scenarios from `crates/node/src/executor.rs`, ported to the new
    // snapshot+delta + kardamom_types::Deposit shape).
    // -----------------------------------------------------------------
    use kardamom_types::Deposit;

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
            execute_deposit_tx(&snap, &delta, env, TxIndex(0), pos(0), &d, 0, 0).expect("execute");

        // Mint = 1000, value forwarded = 400, so:
        // - sender ends at 1000 - 400 = 600
        // - recipient ends at 400
        // (Gas price is 0 — no fee deduction.)
        assert_eq!(ws.accounts[&from].1, U256::from(600u64));
        assert_eq!(ws.accounts[&to].1, U256::from(400u64));

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
        // Inline bytecode: PUSH1 0; PUSH1 0; REVERT (60 00 60 00 fd).
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
        let (receipt, ws) = execute_deposit_tx(&snap, &delta, env, TxIndex(0), pos(0), &d, 0, 0)
            .expect("execute (revert is OK at the executor layer)");

        // Mint pre-credit is OUTSIDE the EVM call: from keeps the full mint.
        assert_eq!(
            ws.accounts[&from].1,
            U256::from(1_000u64),
            "from must keep full mint after inner revert"
        );
        // Inner call reverted — receipt status reflects that.
        assert!(!receipt.status, "inner-revert deposit yields status=false");
        // Recipient observed no transferred value.
        assert!(
            ws.accounts
                .get(&revert_addr)
                .is_none_or(|a| a.1 == U256::ZERO),
            "revert target must not retain value"
        );
    }

    #[test]
    fn deposit_mint_overflow_returns_execution_error() {
        let from = Address::from([0x11u8; 20]);
        // Sender pre-funded with U256::MAX — any mint causes overflow.
        let snap = MockStateDatabase::builder()
            .account(from, U256::MAX, 0, KECCAK_EMPTY)
            .build();
        let delta = PendingDelta::new();
        let env = ExecEnv::new(1, &boundary(1));

        let d = dep(from, Some(Address::from([0x22u8; 20])), 1, 0, Bytes::new());
        let err = execute_deposit_tx(&snap, &delta, env, TxIndex(0), pos(0), &d, 0, 0).unwrap_err();
        assert!(
            matches!(err, ExecutorError::Execution { ref detail, .. } if detail.contains("mint overflow")),
            "got {err:?}"
        );
    }
}

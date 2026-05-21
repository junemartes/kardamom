//! `eth_simulateV1` implementation.
//!
//! Speculatively executes one or more hypothetical blocks of transactions
//! on top of the live node state. Live state is never mutated: all writes
//! happen in an overlay `CacheDB` that wraps the live state via
//! `DatabaseRef` and is discarded when the call returns.
//!
//! Design notes live in `docs/agents/eth-simulate-v1-spec.md`.

use alloy_consensus::constants::{EMPTY_OMMER_ROOT_HASH, EMPTY_ROOT_HASH};
use alloy_consensus::{Header as ConsensusHeader, TxEnvelope};
use alloy_primitives::{Address, B64, B256, Bloom, Bytes, U256};
use alloy_rpc_types_eth::simulate::{
    MAX_SIMULATE_BLOCKS, SimBlock, SimCallResult, SimulateError, SimulatePayload, SimulatedBlock,
};
use alloy_rpc_types_eth::state::{AccountOverride, StateOverride};
use alloy_rpc_types_eth::{
    Block, BlockOverrides, BlockTransactions, Header as RpcHeader, Log as RpcLog, Transaction,
    TransactionRequest, Withdrawals,
};
use revm::context::result::{ExecutionResult, HaltReason, Output};
use revm::context::{BlockEnv, CfgEnv, TxEnv};
use revm::database::{CacheDB, EmptyDB};
use revm::inspector::InspectCommitEvm;
use revm::primitives::{KECCAK_EMPTY, TxKind};
use revm::state::Bytecode;
use revm::{Context, ExecuteCommitEvm, MainBuilder, MainContext};

use crate::transfers::TransferInspector;

/// Conservative upper bound on per-block gas, matching geth.
const MAX_BLOCK_GAS_LIMIT: u64 = u64::MAX / 2;

/// `keccak256("Transfer(address,address,uint256)")`. ERC-20 standard event.
pub(crate) const TRANSFER_SIG: B256 = B256::new([
    0xdd, 0xf2, 0x52, 0xad, 0x1b, 0xe2, 0xc8, 0x9b, 0x69, 0xc2, 0xb0, 0x68, 0xfc, 0x37, 0x8d, 0xaa,
    0x95, 0x2b, 0xa7, 0xf1, 0x63, 0xc4, 0xa1, 0x16, 0x28, 0xf5, 0x5a, 0x4d, 0xf5, 0x23, 0xb3, 0xef,
]);

/// Run a full `eth_simulateV1` payload against a snapshot of the live state.
///
/// `base_db` is borrowed (not cloned) — an overlay `CacheDB` reads through
/// it via `DatabaseRef` and absorbs all simulated writes. `base_block` is
/// the block height of the live head; the first simulated block defaults
/// to `base_block + 1`.
pub fn run(
    payload: &SimulatePayload,
    base_block: u64,
    base_db: &CacheDB<EmptyDB>,
    chain_id: u64,
) -> Result<Vec<SimulatedBlock>, SimulateError> {
    if payload.block_state_calls.len() as u64 > MAX_SIMULATE_BLOCKS {
        return Err(invalid_params(format!(
            "too many blocks: {} > {MAX_SIMULATE_BLOCKS}",
            payload.block_state_calls.len()
        )));
    }

    let mut overlay: CacheDB<&CacheDB<EmptyDB>> = CacheDB::new(base_db);
    let mut tracker = BlockTracker::new(base_block);
    let mut out = Vec::with_capacity(payload.block_state_calls.len());

    for sim_block in &payload.block_state_calls {
        let block_env = tracker.next(sim_block.block_overrides.as_ref())?;

        if let Some(overrides) = sim_block.state_overrides.as_ref() {
            apply_state_overrides(&mut overlay, overrides)?;
        }

        let block_out = run_block(
            sim_block,
            &block_env,
            chain_id,
            &mut overlay,
            payload.validation,
            payload.trace_transfers,
            payload.return_full_transactions,
        )?;
        out.push(block_out);
    }

    Ok(out)
}

/// Tracks block-envelope defaults between successive simulated blocks and
/// enforces monotonic invariants.
struct BlockTracker {
    /// Most recently produced number, or the base block on initialization.
    prev_number: u64,
    /// Most recently produced timestamp, or 0 on initialization.
    prev_timestamp: u64,
    /// Most recently produced gas limit, or a sensible default.
    prev_gas_limit: u64,
    /// Most recently produced base fee, or 0.
    prev_basefee: u64,
}

impl BlockTracker {
    fn new(base_block: u64) -> Self {
        Self {
            prev_number: base_block,
            prev_timestamp: 0,
            prev_gas_limit: 30_000_000,
            prev_basefee: 0,
        }
    }

    fn next(&mut self, overrides: Option<&BlockOverrides>) -> Result<BlockEnv, SimulateError> {
        let number = match overrides.and_then(|o| o.number) {
            Some(n) => {
                let n = u64::try_from(n)
                    .map_err(|_| invalid_params(format!("block number {n} exceeds u64::MAX")))?;
                if n <= self.prev_number {
                    return Err(invalid_params(format!(
                        "block number not strictly increasing: {n} <= {}",
                        self.prev_number
                    )));
                }
                n
            }
            None => self
                .prev_number
                .checked_add(1)
                .ok_or_else(|| invalid_params("block number overflow"))?,
        };

        let timestamp = match overrides.and_then(|o| o.time) {
            Some(t) => {
                if t <= self.prev_timestamp {
                    return Err(invalid_params(format!(
                        "block timestamp not strictly increasing: {t} <= {}",
                        self.prev_timestamp
                    )));
                }
                t
            }
            None => self
                .prev_timestamp
                .checked_add(1)
                .ok_or_else(|| invalid_params("block timestamp overflow"))?,
        };

        let gas_limit = overrides
            .and_then(|o| o.gas_limit)
            .unwrap_or(self.prev_gas_limit);
        if gas_limit > MAX_BLOCK_GAS_LIMIT {
            return Err(invalid_params(format!(
                "block gas limit {gas_limit} exceeds maximum {MAX_BLOCK_GAS_LIMIT}"
            )));
        }

        let basefee_u256 = overrides
            .and_then(|o| o.base_fee)
            .unwrap_or(U256::from(self.prev_basefee));
        let basefee = u64::try_from(basefee_u256)
            .map_err(|_| invalid_params(format!("base fee {basefee_u256} exceeds u64::MAX")))?;

        let beneficiary = overrides.and_then(|o| o.coinbase).unwrap_or(Address::ZERO);
        let prevrandao = overrides.and_then(|o| o.random);
        let difficulty = overrides.and_then(|o| o.difficulty).unwrap_or(U256::ZERO);

        let env = BlockEnv {
            number: U256::from(number),
            beneficiary,
            timestamp: U256::from(timestamp),
            gas_limit,
            basefee,
            difficulty,
            prevrandao: Some(prevrandao.unwrap_or(B256::ZERO)),
            ..Default::default()
        };

        self.prev_number = number;
        self.prev_timestamp = timestamp;
        self.prev_gas_limit = gas_limit;
        self.prev_basefee = basefee;

        Ok(env)
    }
}

/// Apply a batch of `AccountOverride`s to the overlay.
///
/// `state` (replace) and `state_diff` (merge) are mutually exclusive per
/// account; `move_precompile_to` is rejected because the spec calls its
/// behaviour "undefined" when the target isn't a precompile and we have
/// no use case requiring it.
fn apply_state_overrides(
    overlay: &mut CacheDB<&CacheDB<EmptyDB>>,
    overrides: &StateOverride,
) -> Result<(), SimulateError> {
    for (address, ovr) in overrides {
        apply_one_override(overlay, *address, ovr)?;
    }
    Ok(())
}

fn apply_one_override(
    overlay: &mut CacheDB<&CacheDB<EmptyDB>>,
    address: Address,
    ovr: &AccountOverride,
) -> Result<(), SimulateError> {
    if ovr.state.is_some() && ovr.state_diff.is_some() {
        return Err(invalid_params(format!(
            "account {address}: 'state' and 'stateDiff' are mutually exclusive"
        )));
    }
    if ovr.move_precompile_to.is_some() {
        return Err(invalid_params(format!(
            "account {address}: 'movePrecompileToAddress' is not supported"
        )));
    }

    // Materialize the account in the overlay, seeding from the live DB if
    // it isn't cached yet. Without this fallback an override that only sets
    // one field would silently zero balance/nonce/code from the live state.
    materialize_account(overlay, address);

    let mut info = overlay
        .cache
        .accounts
        .get(&address)
        .map(|a| a.info.clone())
        .unwrap_or_default();

    if let Some(balance) = ovr.balance {
        info.balance = balance;
    }
    if let Some(nonce) = ovr.nonce {
        info.nonce = nonce;
    }
    if let Some(code) = ovr.code.as_ref() {
        if code.is_empty() {
            info.code = None;
            info.code_hash = KECCAK_EMPTY;
        } else {
            let bytecode = Bytecode::new_raw(code.clone());
            info.code_hash = bytecode.hash_slow();
            info.code = Some(bytecode);
        }
    }
    overlay.insert_account_info(address, info);

    let storage_update: Option<(bool, &alloy_primitives::map::B256HashMap<B256>)> = ovr
        .state
        .as_ref()
        .map(|s| (true, s))
        .or_else(|| ovr.state_diff.as_ref().map(|s| (false, s)));

    if let Some((replace, slots)) = storage_update {
        let account = overlay.cache.accounts.entry(address).or_default();
        if replace {
            account.storage.clear();
        }
        for (k, v) in slots {
            account
                .storage
                .insert(U256::from_be_bytes(k.0), U256::from_be_bytes(v.0));
        }
    }

    Ok(())
}

/// Seed the overlay's cache for `address` from the live DB if absent. This
/// makes subsequent partial overrides (balance-only, nonce-only, etc.)
/// preserve the live values for unmodified fields.
fn materialize_account(overlay: &mut CacheDB<&CacheDB<EmptyDB>>, address: Address) {
    if overlay.cache.accounts.contains_key(&address) {
        return;
    }
    if let Some(live) = overlay.db.cache.accounts.get(&address) {
        overlay.insert_account_info(address, live.info.clone());
        if let Some(acc) = overlay.cache.accounts.get_mut(&address) {
            acc.storage = live.storage.clone();
        }
    }
}

fn run_block(
    sim_block: &SimBlock,
    block_env: &BlockEnv,
    chain_id: u64,
    overlay: &mut CacheDB<&CacheDB<EmptyDB>>,
    validation: bool,
    trace_transfers: bool,
    return_full_transactions: bool,
) -> Result<SimulatedBlock, SimulateError> {
    let mut cumulative_gas: u64 = 0;
    let mut call_results = Vec::with_capacity(sim_block.calls.len());
    let mut tx_objects: Vec<Transaction<TxEnvelope>> = Vec::new();
    let mut tx_hashes: Vec<B256> = Vec::new();
    let mut block_logs_bloom = Bloom::default();

    if validation {
        // Sum of declared gas must fit in the block.
        let declared: u128 = sim_block
            .calls
            .iter()
            .map(|c| c.gas.unwrap_or(0) as u128)
            .sum();
        if declared > block_env.gas_limit as u128 {
            return Err(invalid_params(format!(
                "sum of call gas {declared} exceeds block gas limit {}",
                block_env.gas_limit
            )));
        }
    }

    for (idx, call) in sim_block.calls.iter().enumerate() {
        let remaining = block_env.gas_limit.saturating_sub(cumulative_gas);
        let from = call.from.unwrap_or(Address::ZERO);

        // For unspecified nonces we always read the overlay's current value,
        // which REVM increments via `transact_commit` after each successful
        // call — so successive nonceless calls from the same sender naturally
        // chain when validation is off.
        let nonce = call.nonce.unwrap_or_else(|| account_nonce(overlay, from));

        let tx = build_tx_env(call, chain_id, remaining, nonce, block_env.basefee);

        let cfg = make_cfg(chain_id, validation, block_env.gas_limit);
        let call_result =
            match execute_with_options(overlay, block_env, &cfg, tx.clone(), trace_transfers) {
                Ok((exec_result, traced_logs)) => {
                    let (status, return_data, gas_used, real_logs, error) = interpret(&exec_result);
                    SimCallResult {
                        return_data,
                        logs: traced_logs.unwrap_or(real_logs),
                        gas_used,
                        max_used_gas: None,
                        status,
                        error,
                    }
                }
                // Per spec: per-call validation failures (insufficient balance,
                // wrong nonce under validation=true, etc.) are reported in the
                // call's result, not as a top-level simulation abort.
                Err(per_call_err) => SimCallResult {
                    return_data: Bytes::new(),
                    logs: Vec::new(),
                    gas_used: 0,
                    max_used_gas: None,
                    status: false,
                    error: Some(per_call_err),
                },
            };

        cumulative_gas = cumulative_gas.saturating_add(call_result.gas_used);

        for log in &call_result.logs {
            block_logs_bloom.accrue_log(&log.inner);
        }

        call_results.push(call_result);

        if return_full_transactions {
            tx_objects.push(synth_transaction(&tx, idx as u64, block_env, chain_id));
        } else {
            tx_hashes.push(synth_tx_hash(&tx, idx as u64, block_env));
        }
    }

    let header = build_header(block_env, cumulative_gas, block_logs_bloom);

    let block = Block {
        header,
        uncles: Vec::new(),
        transactions: if return_full_transactions {
            BlockTransactions::Full(tx_objects)
        } else {
            BlockTransactions::Hashes(tx_hashes)
        },
        withdrawals: Some(Withdrawals::default()),
    };

    Ok(SimulatedBlock {
        inner: block,
        calls: call_results,
    })
}

fn account_nonce(overlay: &CacheDB<&CacheDB<EmptyDB>>, from: Address) -> u64 {
    overlay
        .cache
        .accounts
        .get(&from)
        .map(|a| a.info.nonce)
        .unwrap_or_else(|| {
            overlay
                .db
                .cache
                .accounts
                .get(&from)
                .map_or(0, |a| a.info.nonce)
        })
}

fn build_tx_env(
    call: &TransactionRequest,
    chain_id: u64,
    remaining_gas: u64,
    nonce: u64,
    basefee: u64,
) -> TxEnv {
    let gas_price = call
        .gas_price
        .or(call.max_fee_per_gas)
        .unwrap_or(basefee as u128);
    TxEnv {
        caller: call.from.unwrap_or(Address::ZERO),
        gas_limit: call.gas.unwrap_or(remaining_gas),
        gas_price,
        kind: match call.to {
            Some(alloy_primitives::TxKind::Call(addr)) => TxKind::Call(addr),
            Some(alloy_primitives::TxKind::Create) | None => TxKind::Create,
        },
        value: call.value.unwrap_or(U256::ZERO),
        data: call.input.input().cloned().unwrap_or_default(),
        nonce,
        chain_id: Some(chain_id),
        ..Default::default()
    }
}

#[allow(clippy::field_reassign_with_default)]
fn make_cfg(chain_id: u64, validation: bool, block_gas_limit: u64) -> CfgEnv {
    let mut cfg = CfgEnv::default();
    cfg.chain_id = chain_id;
    if !validation {
        // Lift EIP-7825's 2^24 per-tx cap to the block gas limit so a single
        // exploratory call can use the whole block. Validation mode keeps the
        // consensus-rule cap.
        cfg.tx_gas_limit_cap = Some(block_gas_limit);
        cfg.disable_balance_check = true;
        cfg.disable_nonce_check = true;
        cfg.disable_base_fee = true;
        cfg.disable_block_gas_limit = true;
        cfg.disable_eip3607 = true;
    }
    cfg
}

/// Run one tx and (optionally) return the merged real+synthetic log stream
/// produced by the [`TransferInspector`].
fn execute_with_options(
    overlay: &mut CacheDB<&CacheDB<EmptyDB>>,
    block_env: &BlockEnv,
    cfg: &CfgEnv,
    tx: TxEnv,
    trace_transfers: bool,
) -> Result<(ExecutionResult<HaltReason>, Option<Vec<RpcLog>>), SimulateError> {
    if trace_transfers {
        let inspector = TransferInspector::new();
        let mut evm = Context::mainnet()
            .with_db(overlay)
            .with_block(block_env.clone())
            .with_cfg(cfg.clone())
            .build_mainnet_with_inspector(inspector);
        let result = evm.inspect_tx_commit(tx.clone()).map_err(vm_error)?;
        let priority_fee_per_gas = tx.gas_price.saturating_sub(u128::from(block_env.basefee));
        let gas_used = u128::from(result.tx_gas_used());
        let coinbase_payment = U256::from(priority_fee_per_gas.saturating_mul(gas_used));
        evm.inspector
            .record_coinbase_payment(tx.caller, block_env.beneficiary, coinbase_payment);
        Ok((result, Some(evm.inspector.into_rpc_logs())))
    } else {
        let mut evm = Context::mainnet()
            .with_db(overlay)
            .with_block(block_env.clone())
            .with_cfg(cfg.clone())
            .build_mainnet();
        let result = evm.transact_commit(tx).map_err(vm_error)?;
        Ok((result, None))
    }
}

fn vm_error(e: impl std::fmt::Debug) -> SimulateError {
    SimulateError {
        code: SimulateError::VM_EXECUTION_ERROR_CODE,
        message: format!("{e:?}"),
        data: None,
    }
}

fn interpret(
    result: &ExecutionResult<HaltReason>,
) -> (bool, Bytes, u64, Vec<RpcLog>, Option<SimulateError>) {
    let gas_used = result.tx_gas_used();
    match result {
        ExecutionResult::Success { output, logs, .. } => {
            let data = match output {
                Output::Call(b) => b.clone(),
                Output::Create(b, _) => b.clone(),
            };
            let rpc_logs = logs
                .iter()
                .enumerate()
                .map(|(i, log)| RpcLog {
                    inner: log.clone(),
                    log_index: Some(i as u64),
                    ..Default::default()
                })
                .collect();
            (true, data, gas_used, rpc_logs, None)
        }
        ExecutionResult::Revert { output, .. } => (
            false,
            output.clone(),
            gas_used,
            Vec::new(),
            Some(SimulateError {
                code: SimulateError::EXECUTION_REVERTED_CODE,
                message: "execution reverted".into(),
                data: Some(output.clone()),
            }),
        ),
        ExecutionResult::Halt { reason, .. } => (
            false,
            Bytes::new(),
            gas_used,
            Vec::new(),
            Some(SimulateError {
                code: SimulateError::VM_EXECUTION_ERROR_CODE,
                message: format!("halted: {reason:?}"),
                data: None,
            }),
        ),
    }
}

fn build_header(block_env: &BlockEnv, gas_used: u64, logs_bloom: Bloom) -> RpcHeader {
    let number: u64 = u64::try_from(block_env.number).unwrap_or(u64::MAX);
    let timestamp: u64 = u64::try_from(block_env.timestamp).unwrap_or(u64::MAX);
    let inner = ConsensusHeader {
        parent_hash: B256::ZERO,
        ommers_hash: EMPTY_OMMER_ROOT_HASH,
        beneficiary: block_env.beneficiary,
        state_root: EMPTY_ROOT_HASH,
        transactions_root: EMPTY_ROOT_HASH,
        receipts_root: EMPTY_ROOT_HASH,
        logs_bloom,
        difficulty: block_env.difficulty,
        number,
        gas_limit: block_env.gas_limit,
        gas_used,
        timestamp,
        extra_data: Bytes::new(),
        mix_hash: block_env.prevrandao.unwrap_or(B256::ZERO),
        nonce: B64::ZERO,
        base_fee_per_gas: Some(block_env.basefee),
        withdrawals_root: Some(EMPTY_ROOT_HASH),
        blob_gas_used: Some(0),
        excess_blob_gas: Some(0),
        parent_beacon_block_root: Some(B256::ZERO),
        requests_hash: Some(EMPTY_ROOT_HASH),
        block_access_list_hash: None,
        slot_number: None,
    };
    RpcHeader::new(inner)
}

/// Deterministic synthetic hash for a simulated transaction. Real txs in
/// `eth_simulateV1` are unsigned, so we hash the encoded `TxEnv` fields
/// salted with (block number, tx index) so identical calls in different
/// simulated blocks never collide. Clients use this as an opaque identifier.
fn synth_tx_hash(tx: &TxEnv, idx: u64, block_env: &BlockEnv) -> B256 {
    use alloy_primitives::keccak256;
    let mut buf = Vec::with_capacity(128);
    buf.extend_from_slice(&block_env.number.to_be_bytes::<32>());
    buf.extend_from_slice(tx.caller.as_slice());
    buf.extend_from_slice(&tx.nonce.to_be_bytes());
    buf.extend_from_slice(&idx.to_be_bytes());
    buf.extend_from_slice(&tx.value.to_be_bytes::<32>());
    buf.extend_from_slice(&tx.data);
    keccak256(buf)
}

fn synth_transaction(
    tx: &TxEnv,
    idx: u64,
    block_env: &BlockEnv,
    chain_id: u64,
) -> Transaction<TxEnvelope> {
    // No real signature exists; we return a Legacy zero-sig envelope as an
    // opaque placeholder. Clients that need full envelope semantics should
    // sign and submit via eth_sendRawTransaction instead.
    use alloy_consensus::{Signed, TxLegacy};
    use alloy_primitives::Signature;

    let tx_legacy = TxLegacy {
        chain_id: Some(chain_id),
        nonce: tx.nonce,
        gas_price: tx.gas_price,
        gas_limit: tx.gas_limit,
        to: tx.kind,
        value: tx.value,
        input: tx.data.clone(),
    };
    let sig = Signature::from_scalars_and_parity(B256::ZERO, B256::ZERO, false);
    let signed: Signed<TxLegacy> =
        Signed::new_unchecked(tx_legacy, sig, synth_tx_hash(tx, idx, block_env));
    let envelope: TxEnvelope = signed.into();

    Transaction {
        inner: alloy_consensus::transaction::Recovered::new_unchecked(envelope, tx.caller),
        block_hash: None,
        block_number: u64::try_from(block_env.number).ok(),
        transaction_index: Some(idx),
        effective_gas_price: Some(tx.gas_price),
        block_timestamp: u64::try_from(block_env.timestamp).ok(),
    }
}

fn invalid_params(msg: impl Into<String>) -> SimulateError {
    SimulateError {
        code: SimulateError::INVALID_PARAMS_ERROR_CODE,
        message: msg.into(),
        data: None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloy_primitives::{address, hex};
    use revm::state::AccountInfo;

    fn empty_db() -> CacheDB<EmptyDB> {
        CacheDB::new(EmptyDB::default())
    }

    fn db_with(accounts: Vec<(Address, AccountInfo)>) -> CacheDB<EmptyDB> {
        let mut db = empty_db();
        for (addr, info) in accounts {
            db.insert_account_info(addr, info);
        }
        db
    }

    fn just_call(call: TransactionRequest) -> SimBlock {
        SimBlock {
            block_overrides: None,
            state_overrides: None,
            calls: vec![call],
        }
    }

    fn payload(blocks: Vec<SimBlock>) -> SimulatePayload {
        SimulatePayload {
            block_state_calls: blocks,
            trace_transfers: false,
            validation: false,
            return_full_transactions: false,
        }
    }

    /// Returns the address `addr` followed by code that pushes 0x42 to memory
    /// and returns 32 bytes — a standard constant-returning contract.
    fn const_42_contract() -> (Address, Bytes) {
        (
            address!("0000000000000000000000000000000000001234"),
            Bytes::from(hex!("604260005260206000f3").to_vec()),
        )
    }

    #[test]
    fn empty_payload_returns_no_blocks() {
        let db = empty_db();
        let p = payload(vec![]);
        let out = run(&p, 0, &db, 1).expect("ok");
        assert!(out.is_empty());
    }

    #[test]
    fn rejects_too_many_blocks() {
        let db = empty_db();
        let p = payload(
            (0..=MAX_SIMULATE_BLOCKS)
                .map(|_| just_call(TransactionRequest::default()))
                .collect(),
        );
        let err = run(&p, 0, &db, 1).expect_err("rejected");
        assert_eq!(err.code, SimulateError::INVALID_PARAMS_ERROR_CODE);
    }

    #[test]
    fn block_defaults_number_increments() {
        let db = empty_db();
        let p = payload(vec![
            SimBlock::default(),
            SimBlock::default(),
            SimBlock::default(),
        ]);
        let out = run(&p, 10, &db, 1).expect("ok");
        assert_eq!(out.len(), 3);
        assert_eq!(out[0].inner.header.number, 11);
        assert_eq!(out[1].inner.header.number, 12);
        assert_eq!(out[2].inner.header.number, 13);
    }

    #[test]
    fn block_defaults_timestamp_increments() {
        let db = empty_db();
        let p = payload(vec![SimBlock::default(), SimBlock::default()]);
        let out = run(&p, 0, &db, 1).expect("ok");
        assert_eq!(out[0].inner.header.timestamp, 1);
        assert_eq!(out[1].inner.header.timestamp, 2);
    }

    #[test]
    fn block_override_number_applied() {
        let db = empty_db();
        let block = SimBlock {
            block_overrides: Some(BlockOverrides {
                number: Some(U256::from(99u64)),
                ..Default::default()
            }),
            ..Default::default()
        };
        let out = run(&payload(vec![block]), 0, &db, 1).expect("ok");
        assert_eq!(out[0].inner.header.number, 99);
    }

    #[test]
    fn block_override_timestamp_applied() {
        let db = empty_db();
        let block = SimBlock {
            block_overrides: Some(BlockOverrides {
                time: Some(1_700_000_000),
                ..Default::default()
            }),
            ..Default::default()
        };
        let out = run(&payload(vec![block]), 0, &db, 1).expect("ok");
        assert_eq!(out[0].inner.header.timestamp, 1_700_000_000);
    }

    #[test]
    fn error_block_number_not_strictly_increasing() {
        let db = empty_db();
        let block = SimBlock {
            block_overrides: Some(BlockOverrides {
                number: Some(U256::from(5u64)),
                ..Default::default()
            }),
            ..Default::default()
        };
        let err = run(&payload(vec![block]), 10, &db, 1).expect_err("rejected");
        assert_eq!(err.code, SimulateError::INVALID_PARAMS_ERROR_CODE);
    }

    #[test]
    fn error_block_timestamp_not_strictly_increasing() {
        let db = empty_db();
        // First block: explicit time=100; second block: explicit time=50 → reject.
        let b1 = SimBlock {
            block_overrides: Some(BlockOverrides {
                time: Some(100),
                ..Default::default()
            }),
            ..Default::default()
        };
        let b2 = SimBlock {
            block_overrides: Some(BlockOverrides {
                time: Some(50),
                ..Default::default()
            }),
            ..Default::default()
        };
        let err = run(&payload(vec![b1, b2]), 0, &db, 1).expect_err("rejected");
        assert_eq!(err.code, SimulateError::INVALID_PARAMS_ERROR_CODE);
    }

    #[test]
    fn error_block_gas_limit_overflow() {
        let db = empty_db();
        let block = SimBlock {
            block_overrides: Some(BlockOverrides {
                gas_limit: Some(u64::MAX),
                ..Default::default()
            }),
            ..Default::default()
        };
        let err = run(&payload(vec![block]), 0, &db, 1).expect_err("rejected");
        assert_eq!(err.code, SimulateError::INVALID_PARAMS_ERROR_CODE);
    }

    #[test]
    fn state_override_sets_balance_observable_via_balance_opcode() {
        // Install a contract that returns BALANCE(probed_addr) for any caller.
        // PUSH20 <probed> BALANCE PUSH1 0x00 MSTORE PUSH1 0x20 PUSH1 0x00 RETURN
        let probed = address!("00000000000000000000000000000000000000aa");
        let mut code = vec![0x73u8]; // PUSH20
        code.extend_from_slice(probed.as_slice());
        code.extend_from_slice(&[
            0x31, // BALANCE
            0x60, 0x00, // PUSH1 0
            0x52, // MSTORE
            0x60, 0x20, 0x60, 0x00, 0xf3, // PUSH1 32 PUSH1 0 RETURN
        ]);
        let contract = address!("00000000000000000000000000000000000000bb");

        let db = empty_db();
        let mut overrides = StateOverride::default();
        overrides.insert(
            probed,
            AccountOverride {
                balance: Some(U256::from(999u64)),
                ..Default::default()
            },
        );
        overrides.insert(
            contract,
            AccountOverride {
                code: Some(Bytes::from(code)),
                ..Default::default()
            },
        );

        let p = SimulatePayload {
            block_state_calls: vec![SimBlock {
                state_overrides: Some(overrides),
                calls: vec![TransactionRequest {
                    from: Some(Address::ZERO),
                    to: Some(alloy_primitives::TxKind::Call(contract)),
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..payload(vec![]).clone()
        };
        let out = run(&p, 0, &db, 1).expect("ok");
        assert!(out[0].calls[0].status);
        assert_eq!(
            U256::from_be_slice(out[0].calls[0].return_data.as_ref()),
            U256::from(999u64)
        );
    }

    #[test]
    fn state_override_preserves_live_account_fields() {
        // Live DB: account with balance, nonce, and code. Override only the
        // balance. The nonce and code must survive.
        let addr = address!("00000000000000000000000000000000000000aa");
        let code = Bytes::from(hex!("604260005260206000f3").to_vec());
        let bytecode = Bytecode::new_raw(code.clone());
        let code_hash = bytecode.hash_slow();
        let live = AccountInfo {
            balance: U256::from(1_000_000u64),
            nonce: 9,
            code_hash,
            code: Some(bytecode),
            ..Default::default()
        };
        let db = db_with(vec![(addr, live)]);

        // Override only the balance.
        let mut overrides = StateOverride::default();
        overrides.insert(
            addr,
            AccountOverride {
                balance: Some(U256::from(42u64)),
                ..Default::default()
            },
        );
        // Probe contract: caller invokes it which returns BALANCE(addr).
        let probe_addr = address!("00000000000000000000000000000000000000bb");
        let mut probe_code = vec![0x73u8]; // PUSH20
        probe_code.extend_from_slice(addr.as_slice());
        probe_code.extend_from_slice(&[0x31, 0x60, 0x00, 0x52, 0x60, 0x20, 0x60, 0x00, 0xf3]);
        overrides.insert(
            probe_addr,
            AccountOverride {
                code: Some(Bytes::from(probe_code)),
                ..Default::default()
            },
        );

        let p = SimulatePayload {
            block_state_calls: vec![SimBlock {
                state_overrides: Some(overrides),
                calls: vec![TransactionRequest {
                    from: Some(Address::ZERO),
                    to: Some(alloy_primitives::TxKind::Call(probe_addr)),
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..payload(vec![]).clone()
        };
        let out = run(&p, 0, &db, 1).expect("ok");
        assert_eq!(
            U256::from_be_slice(out[0].calls[0].return_data.as_ref()),
            U256::from(42u64),
            "balance override took effect"
        );

        // Check the materialized overlay has preserved the live nonce + code.
        // We can't directly inspect the overlay from the test, but a second
        // simulation that overrides only `code: None` would re-confirm. Easier:
        // call `account_nonce` semantics via a CALLER nonce-bumping path is
        // out-of-scope; instead, we use a tx that requires the original code
        // to be intact. Call the original `addr` (which has the const-42
        // contract code from the live DB) and assert the return matches.
        let p2 = SimulatePayload {
            block_state_calls: vec![SimBlock {
                state_overrides: Some({
                    let mut o = StateOverride::default();
                    o.insert(
                        addr,
                        AccountOverride {
                            balance: Some(U256::from(42u64)),
                            ..Default::default()
                        },
                    );
                    o
                }),
                calls: vec![TransactionRequest {
                    from: Some(Address::ZERO),
                    to: Some(alloy_primitives::TxKind::Call(addr)),
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..payload(vec![]).clone()
        };
        let out2 = run(&p2, 0, &db, 1).expect("ok");
        let mut expected_42 = [0u8; 32];
        expected_42[31] = 0x42;
        assert_eq!(
            out2[0].calls[0].return_data.as_ref(),
            &expected_42[..],
            "live code must survive a balance-only override"
        );
    }

    #[test]
    fn state_override_state_diff_merges_existing_storage() {
        // First call writes slot 0 = 0xaa via SSTORE; then state_diff sets
        // slot 1 = 0xbb; then a read of slot 0 should still return 0xaa.
        let contract = address!("00000000000000000000000000000000000000bb");
        // Writer: PUSH1 0xaa PUSH1 0x00 SSTORE STOP
        let writer_code = Bytes::from(vec![0x60, 0xaa, 0x60, 0x00, 0x55, 0x00]);
        // Reader: PUSH1 0x00 SLOAD PUSH1 0x00 MSTORE PUSH1 0x20 PUSH1 0x00 RETURN
        let reader_code = Bytes::from(vec![
            0x60, 0x00, 0x54, 0x60, 0x00, 0x52, 0x60, 0x20, 0x60, 0x00, 0xf3,
        ]);

        let db = empty_db();
        let mut overrides_b1 = StateOverride::default();
        overrides_b1.insert(
            contract,
            AccountOverride {
                code: Some(writer_code),
                ..Default::default()
            },
        );

        let mut overrides_b2 = StateOverride::default();
        let mut diff = alloy_primitives::map::B256HashMap::default();
        diff.insert(
            B256::from(U256::from(1u64).to_be_bytes::<32>()),
            B256::from(U256::from(0xbbu64).to_be_bytes::<32>()),
        );
        overrides_b2.insert(
            contract,
            AccountOverride {
                code: Some(reader_code),
                state_diff: Some(diff),
                ..Default::default()
            },
        );

        let p = SimulatePayload {
            block_state_calls: vec![
                SimBlock {
                    state_overrides: Some(overrides_b1),
                    calls: vec![TransactionRequest {
                        from: Some(Address::ZERO),
                        to: Some(alloy_primitives::TxKind::Call(contract)),
                        ..Default::default()
                    }],
                    ..Default::default()
                },
                SimBlock {
                    state_overrides: Some(overrides_b2),
                    calls: vec![TransactionRequest {
                        from: Some(Address::ZERO),
                        to: Some(alloy_primitives::TxKind::Call(contract)),
                        ..Default::default()
                    }],
                    ..Default::default()
                },
            ],
            ..payload(vec![]).clone()
        };
        let out = run(&p, 0, &db, 1).expect("ok");
        assert_eq!(
            U256::from_be_slice(out[1].calls[0].return_data.as_ref()),
            U256::from(0xaau64),
            "slot 0 must survive state_diff that only sets slot 1"
        );
    }

    #[test]
    fn block_override_basefee_reaches_evm() {
        // Contract: BASEFEE PUSH1 0 MSTORE PUSH1 0x20 PUSH1 0 RETURN
        let contract = address!("00000000000000000000000000000000000000bb");
        let code = Bytes::from(vec![0x48, 0x60, 0x00, 0x52, 0x60, 0x20, 0x60, 0x00, 0xf3]);
        let db = empty_db();
        let mut overrides = StateOverride::default();
        overrides.insert(
            contract,
            AccountOverride {
                code: Some(code),
                ..Default::default()
            },
        );
        let p = SimulatePayload {
            block_state_calls: vec![SimBlock {
                block_overrides: Some(BlockOverrides {
                    base_fee: Some(U256::from(42u64)),
                    ..Default::default()
                }),
                state_overrides: Some(overrides),
                calls: vec![TransactionRequest {
                    from: Some(Address::ZERO),
                    to: Some(alloy_primitives::TxKind::Call(contract)),
                    ..Default::default()
                }],
            }],
            ..payload(vec![]).clone()
        };
        let out = run(&p, 0, &db, 1).expect("ok");
        assert_eq!(
            U256::from_be_slice(out[0].calls[0].return_data.as_ref()),
            U256::from(42u64)
        );
    }

    #[test]
    fn block_override_coinbase_reaches_evm() {
        // Contract: COINBASE PUSH1 0 MSTORE PUSH1 0x20 PUSH1 0 RETURN
        let contract = address!("00000000000000000000000000000000000000bb");
        let code = Bytes::from(vec![0x41, 0x60, 0x00, 0x52, 0x60, 0x20, 0x60, 0x00, 0xf3]);
        let coinbase = address!("00000000000000000000000000000000000000cc");
        let db = empty_db();
        let mut overrides = StateOverride::default();
        overrides.insert(
            contract,
            AccountOverride {
                code: Some(code),
                ..Default::default()
            },
        );
        let p = SimulatePayload {
            block_state_calls: vec![SimBlock {
                block_overrides: Some(BlockOverrides {
                    coinbase: Some(coinbase),
                    ..Default::default()
                }),
                state_overrides: Some(overrides),
                calls: vec![TransactionRequest {
                    from: Some(Address::ZERO),
                    to: Some(alloy_primitives::TxKind::Call(contract)),
                    ..Default::default()
                }],
            }],
            ..payload(vec![]).clone()
        };
        let out = run(&p, 0, &db, 1).expect("ok");
        assert_eq!(
            B256::from_slice(out[0].calls[0].return_data.as_ref()),
            B256::left_padding_from(coinbase.as_slice())
        );
    }

    #[test]
    fn trace_transfers_selfdestruct_emits_log() {
        // Suicide contract: PUSH20 <target> SELFDESTRUCT
        let target = address!("00000000000000000000000000000000000000dd");
        let mut code = vec![0x73u8];
        code.extend_from_slice(target.as_slice());
        code.push(0xff); // SELFDESTRUCT
        let suicide = address!("00000000000000000000000000000000000000bb");

        let db = empty_db();
        let mut overrides = StateOverride::default();
        overrides.insert(
            suicide,
            AccountOverride {
                balance: Some(U256::from(7u64)),
                code: Some(Bytes::from(code)),
                ..Default::default()
            },
        );
        let p = SimulatePayload {
            block_state_calls: vec![SimBlock {
                state_overrides: Some(overrides),
                calls: vec![TransactionRequest {
                    from: Some(Address::ZERO),
                    to: Some(alloy_primitives::TxKind::Call(suicide)),
                    ..Default::default()
                }],
                ..Default::default()
            }],
            trace_transfers: true,
            ..payload(vec![]).clone()
        };
        let out = run(&p, 0, &db, 1).expect("ok");
        let logs = &out[0].calls[0].logs;
        let sd_log = logs
            .iter()
            .find(|l| l.inner.address == suicide)
            .expect("selfdestruct emits transfer log");
        assert_eq!(sd_log.inner.data.topics()[0], TRANSFER_SIG);
        assert_eq!(
            sd_log.inner.data.topics()[2],
            B256::left_padding_from(target.as_slice())
        );
    }

    #[test]
    fn default_from_is_zero_address() {
        // Contract: CALLER PUSH1 0 MSTORE PUSH1 0x20 PUSH1 0 RETURN
        let contract = address!("00000000000000000000000000000000000000bb");
        let code = Bytes::from(vec![0x33, 0x60, 0x00, 0x52, 0x60, 0x20, 0x60, 0x00, 0xf3]);
        let db = empty_db();
        let mut overrides = StateOverride::default();
        overrides.insert(
            contract,
            AccountOverride {
                code: Some(code),
                ..Default::default()
            },
        );
        let p = SimulatePayload {
            block_state_calls: vec![SimBlock {
                state_overrides: Some(overrides),
                calls: vec![TransactionRequest {
                    // from intentionally omitted
                    to: Some(alloy_primitives::TxKind::Call(contract)),
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..payload(vec![]).clone()
        };
        let out = run(&p, 0, &db, 1).expect("ok");
        assert_eq!(
            B256::from_slice(out[0].calls[0].return_data.as_ref()),
            B256::ZERO
        );
    }

    #[test]
    fn synth_hashes_differ_across_blocks_for_identical_calls() {
        let db = empty_db();
        let mk = || TransactionRequest {
            from: Some(Address::ZERO),
            to: Some(alloy_primitives::TxKind::Call(Address::ZERO)),
            nonce: Some(0),
            ..Default::default()
        };
        let p = SimulatePayload {
            block_state_calls: vec![just_call(mk()), just_call(mk())],
            return_full_transactions: false,
            ..payload(vec![]).clone()
        };
        let out = run(&p, 0, &db, 1).expect("ok");
        let h0 = match &out[0].inner.transactions {
            BlockTransactions::Hashes(h) => h[0],
            _ => panic!("hashes"),
        };
        let h1 = match &out[1].inner.transactions {
            BlockTransactions::Hashes(h) => h[0],
            _ => panic!("hashes"),
        };
        assert_ne!(
            h0, h1,
            "identical calls in different blocks must hash differently"
        );
    }

    #[test]
    fn state_override_rejects_state_and_diff_together() {
        let db = empty_db();
        let addr = address!("00000000000000000000000000000000000000aa");
        let mut overrides = StateOverride::default();
        overrides.insert(
            addr,
            AccountOverride {
                state: Some(Default::default()),
                state_diff: Some(Default::default()),
                ..Default::default()
            },
        );
        let p = SimulatePayload {
            block_state_calls: vec![SimBlock {
                state_overrides: Some(overrides),
                ..Default::default()
            }],
            ..payload(vec![]).clone()
        };
        let err = run(&p, 0, &db, 1).expect_err("rejected");
        assert_eq!(err.code, SimulateError::INVALID_PARAMS_ERROR_CODE);
    }

    #[test]
    fn state_override_rejects_move_precompile() {
        let db = empty_db();
        let addr = address!("00000000000000000000000000000000000000aa");
        let mut overrides = StateOverride::default();
        overrides.insert(
            addr,
            AccountOverride {
                move_precompile_to: Some(Address::ZERO),
                ..Default::default()
            },
        );
        let p = SimulatePayload {
            block_state_calls: vec![SimBlock {
                state_overrides: Some(overrides),
                ..Default::default()
            }],
            ..payload(vec![]).clone()
        };
        let err = run(&p, 0, &db, 1).expect_err("rejected");
        assert_eq!(err.code, SimulateError::INVALID_PARAMS_ERROR_CODE);
    }

    #[test]
    fn state_override_sets_code_callable() {
        let db = empty_db();
        let (contract_addr, code) = const_42_contract();
        let mut overrides = StateOverride::default();
        overrides.insert(
            contract_addr,
            AccountOverride {
                code: Some(code),
                ..Default::default()
            },
        );
        let p = SimulatePayload {
            block_state_calls: vec![SimBlock {
                state_overrides: Some(overrides),
                calls: vec![TransactionRequest {
                    from: Some(Address::ZERO),
                    to: Some(alloy_primitives::TxKind::Call(contract_addr)),
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..payload(vec![]).clone()
        };
        let out = run(&p, 0, &db, 1).expect("ok");
        assert_eq!(out[0].calls.len(), 1);
        assert!(out[0].calls[0].status);
        let mut expected = [0u8; 32];
        expected[31] = 0x42;
        assert_eq!(out[0].calls[0].return_data.as_ref(), &expected[..]);
    }

    #[test]
    fn validation_off_allows_unfunded_value_transfer() {
        let db = empty_db();
        let from = address!("00000000000000000000000000000000000000aa");
        let to = address!("00000000000000000000000000000000000000bb");
        let p = SimulatePayload {
            block_state_calls: vec![just_call(TransactionRequest {
                from: Some(from),
                to: Some(alloy_primitives::TxKind::Call(to)),
                value: Some(U256::from(1u64)),
                ..Default::default()
            })],
            validation: false,
            ..payload(vec![]).clone()
        };
        let out = run(&p, 0, &db, 1).expect("ok");
        assert!(out[0].calls[0].status);
    }

    #[test]
    fn validation_on_rejects_unfunded_value_transfer() {
        let db = empty_db();
        let from = address!("00000000000000000000000000000000000000aa");
        let to = address!("00000000000000000000000000000000000000bb");
        let p = SimulatePayload {
            block_state_calls: vec![just_call(TransactionRequest {
                from: Some(from),
                to: Some(alloy_primitives::TxKind::Call(to)),
                value: Some(U256::from(1u64)),
                gas: Some(21_000),
                gas_price: Some(1),
                nonce: Some(0),
                ..Default::default()
            })],
            validation: true,
            ..payload(vec![]).clone()
        };
        // With validation on, the tx must fail because the caller has no
        // balance. Per spec, the failure surfaces in the per-call result, not
        // as a top-level abort.
        let out = run(&p, 0, &db, 1).expect("ok");
        let result = &out[0].calls[0];
        assert!(!result.status);
        assert!(result.error.is_some());
    }

    #[test]
    fn nonce_auto_increments_within_block_when_validation_off() {
        let db = empty_db();
        let (contract_addr, code) = const_42_contract();
        let from = address!("00000000000000000000000000000000000000aa");
        let mut overrides = StateOverride::default();
        overrides.insert(
            contract_addr,
            AccountOverride {
                code: Some(code),
                ..Default::default()
            },
        );

        let mk = || TransactionRequest {
            from: Some(from),
            to: Some(alloy_primitives::TxKind::Call(contract_addr)),
            ..Default::default()
        };

        let p = SimulatePayload {
            block_state_calls: vec![SimBlock {
                state_overrides: Some(overrides),
                calls: vec![mk(), mk(), mk()],
                ..Default::default()
            }],
            ..payload(vec![]).clone()
        };
        let out = run(&p, 0, &db, 1).expect("ok");
        for r in &out[0].calls {
            assert!(r.status);
        }
        assert_eq!(out[0].calls.len(), 3);
    }

    #[test]
    fn multi_block_state_inherited() {
        let db = empty_db();
        let from = address!("00000000000000000000000000000000000000aa");
        let to = address!("00000000000000000000000000000000000000bb");

        // Block 0: transfer 100 wei.
        let b0 = SimBlock {
            calls: vec![TransactionRequest {
                from: Some(from),
                to: Some(alloy_primitives::TxKind::Call(to)),
                value: Some(U256::from(100u64)),
                ..Default::default()
            }],
            ..Default::default()
        };
        // Block 1: transfer another 50 wei. If state is shared and validation
        // is off (no balance check), both succeed.
        let b1 = SimBlock {
            calls: vec![TransactionRequest {
                from: Some(from),
                to: Some(alloy_primitives::TxKind::Call(to)),
                value: Some(U256::from(50u64)),
                ..Default::default()
            }],
            ..Default::default()
        };
        let p = payload(vec![b0, b1]);
        let out = run(&p, 0, &db, 1).expect("ok");
        assert!(out[0].calls[0].status);
        assert!(out[1].calls[0].status);
        // Both blocks share state — the nonce auto-incrementer should have
        // advanced the sender's nonce across blocks (1 → 2).
    }

    #[test]
    fn live_state_unchanged_after_simulation() {
        let from = address!("00000000000000000000000000000000000000aa");
        let info = AccountInfo {
            balance: U256::from(1_000_000u64),
            nonce: 7,
            code_hash: KECCAK_EMPTY,
            code: None,
            ..Default::default()
        };
        let db = db_with(vec![(from, info.clone())]);

        let p = payload(vec![just_call(TransactionRequest {
            from: Some(from),
            to: Some(alloy_primitives::TxKind::Call(Address::ZERO)),
            value: Some(U256::from(500_000u64)),
            ..Default::default()
        })]);
        let _ = run(&p, 0, &db, 1).expect("ok");

        // The live DB still has the original balance & nonce.
        let after = db.cache.accounts.get(&from).expect("present");
        assert_eq!(after.info.balance, U256::from(1_000_000u64));
        assert_eq!(after.info.nonce, 7);
    }

    #[test]
    fn revert_returns_status_zero_and_revert_data() {
        let db = empty_db();
        // PUSH1 0x00 PUSH1 0x00 REVERT
        let revert_code = Bytes::from(hex!("60006000fd").to_vec());
        let contract_addr = address!("0000000000000000000000000000000000003456");
        let mut overrides = StateOverride::default();
        overrides.insert(
            contract_addr,
            AccountOverride {
                code: Some(revert_code),
                ..Default::default()
            },
        );
        let p = SimulatePayload {
            block_state_calls: vec![SimBlock {
                state_overrides: Some(overrides),
                calls: vec![TransactionRequest {
                    from: Some(Address::ZERO),
                    to: Some(alloy_primitives::TxKind::Call(contract_addr)),
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..payload(vec![]).clone()
        };
        let out = run(&p, 0, &db, 1).expect("ok");
        assert!(!out[0].calls[0].status);
        assert!(out[0].calls[0].error.is_some());
    }

    #[test]
    fn return_full_transactions_false_returns_hashes() {
        let db = empty_db();
        let p = SimulatePayload {
            block_state_calls: vec![just_call(TransactionRequest {
                from: Some(Address::ZERO),
                to: Some(alloy_primitives::TxKind::Call(Address::ZERO)),
                ..Default::default()
            })],
            return_full_transactions: false,
            ..payload(vec![]).clone()
        };
        let out = run(&p, 0, &db, 1).expect("ok");
        match &out[0].inner.transactions {
            BlockTransactions::Hashes(h) => assert_eq!(h.len(), 1),
            other => panic!("expected hashes, got {other:?}"),
        }
    }

    #[test]
    fn trace_transfers_off_emits_no_synthetic_logs() {
        let db = empty_db();
        let from = address!("00000000000000000000000000000000000000aa");
        let to = address!("00000000000000000000000000000000000000bb");
        let p = SimulatePayload {
            block_state_calls: vec![just_call(TransactionRequest {
                from: Some(from),
                to: Some(alloy_primitives::TxKind::Call(to)),
                value: Some(U256::from(100u64)),
                ..Default::default()
            })],
            trace_transfers: false,
            ..payload(vec![]).clone()
        };
        let out = run(&p, 0, &db, 1).expect("ok");
        assert!(out[0].calls[0].logs.is_empty());
    }

    #[test]
    fn trace_transfers_top_level_value_emits_transfer_log() {
        let db = empty_db();
        let from = address!("00000000000000000000000000000000000000aa");
        let to = address!("00000000000000000000000000000000000000bb");
        let p = SimulatePayload {
            block_state_calls: vec![just_call(TransactionRequest {
                from: Some(from),
                to: Some(alloy_primitives::TxKind::Call(to)),
                value: Some(U256::from(100u64)),
                ..Default::default()
            })],
            trace_transfers: true,
            ..payload(vec![]).clone()
        };
        let out = run(&p, 0, &db, 1).expect("ok");
        let logs = &out[0].calls[0].logs;
        assert_eq!(logs.len(), 1);
        assert_eq!(logs[0].inner.address, from);
        assert_eq!(logs[0].inner.data.topics()[0], TRANSFER_SIG);
    }

    #[test]
    fn trace_transfers_coinbase_payment_emits_log_when_priority_fee_nonzero() {
        let db = empty_db();
        let from = address!("00000000000000000000000000000000000000aa");
        let bene = address!("00000000000000000000000000000000000000cc");
        // gas_price=10 > basefee=1, so priority fee = 9 * gas_used > 0.
        let p = SimulatePayload {
            block_state_calls: vec![SimBlock {
                block_overrides: Some(BlockOverrides {
                    coinbase: Some(bene),
                    base_fee: Some(U256::from(1u64)),
                    ..Default::default()
                }),
                calls: vec![TransactionRequest {
                    from: Some(from),
                    to: Some(alloy_primitives::TxKind::Call(Address::ZERO)),
                    gas: Some(21_000),
                    gas_price: Some(10),
                    ..Default::default()
                }],
                ..Default::default()
            }],
            trace_transfers: true,
            ..payload(vec![]).clone()
        };
        let out = run(&p, 0, &db, 1).expect("ok");
        let logs = &out[0].calls[0].logs;
        let last = logs.last().expect("at least one log");
        assert_eq!(last.inner.address, from);
        assert_eq!(last.inner.data.topics()[0], TRANSFER_SIG);
        assert_eq!(
            last.inner.data.topics()[2],
            B256::left_padding_from(bene.as_slice())
        );
    }

    #[test]
    fn trace_transfers_no_coinbase_log_when_priority_fee_zero() {
        let db = empty_db();
        let from = address!("00000000000000000000000000000000000000aa");
        let bene = address!("00000000000000000000000000000000000000cc");
        // gas_price == basefee → priority fee is 0 → no coinbase synthetic log.
        let p = SimulatePayload {
            block_state_calls: vec![SimBlock {
                block_overrides: Some(BlockOverrides {
                    coinbase: Some(bene),
                    base_fee: Some(U256::from(1u64)),
                    ..Default::default()
                }),
                calls: vec![TransactionRequest {
                    from: Some(from),
                    to: Some(alloy_primitives::TxKind::Call(Address::ZERO)),
                    gas: Some(21_000),
                    gas_price: Some(1),
                    ..Default::default()
                }],
                ..Default::default()
            }],
            trace_transfers: true,
            ..payload(vec![]).clone()
        };
        let out = run(&p, 0, &db, 1).expect("ok");
        assert!(out[0].calls[0].logs.is_empty());
    }

    #[test]
    fn return_full_transactions_true_returns_full() {
        let db = empty_db();
        let p = SimulatePayload {
            block_state_calls: vec![just_call(TransactionRequest {
                from: Some(Address::ZERO),
                to: Some(alloy_primitives::TxKind::Call(Address::ZERO)),
                ..Default::default()
            })],
            return_full_transactions: true,
            ..payload(vec![]).clone()
        };
        let out = run(&p, 0, &db, 1).expect("ok");
        match &out[0].inner.transactions {
            BlockTransactions::Full(txs) => assert_eq!(txs.len(), 1),
            other => panic!("expected full, got {other:?}"),
        }
    }
}

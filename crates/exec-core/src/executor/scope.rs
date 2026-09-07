//! [`Executor`]: the per-block EVM and committed cache, plus the free
//! [`execute_tx`] compatibility wrapper and the deterministic invalid-skip
//! path.

use alloy_consensus::Transaction;
use alloy_primitives::{Address, B256};
use kardamom_types::xchain::{self, XChainMessage};
use kardamom_types::{BPosition, Receipt, SkipReason, StateDatabase, TxEnvelope};
use revm::context::result::{EVMError, ExecutionResult};
use revm::database::CacheDB;
use revm::{Context, ExecuteEvm, MainBuilder, MainContext};

use alloc::format;
use alloc::vec::Vec;

use crate::block_env::ExecEnv;
use crate::delta::{PendingDelta, WriteSet};
use crate::error::ExecutorError;
use crate::exec_types::{ReceiptStatus, TxIndex};

use super::db::{SnapshotDb, seed_cache_layer};
use super::deposit::{failed_deposit_receipt, mint_only_write_set};
use super::tx_env::{DecodedTx, tx_env_from_deposit, tx_env_from_xchain};
use super::xchain::{failed_xchain_receipt, xchain_value_rejection};

/// Per-tx read-touch capture for the footprint shadow scheduler. The
/// block-level EIP-7928 BAL cannot
/// attribute reads per tx. A slot read after another tx wrote it leaves no
/// trace at all (revm keeps writer indexes only), and `storage_reads` is
/// block-scoped. So the shadow captures the read side here, at the same
/// point the BAL capture runs, from the same `outcome.state`. Writes need
/// no capture: the returned `WriteSet` already carries them exactly.
///
/// `account_reads` holds accounts revm loaded but never touched: the
/// BALANCE, EXTCODE*, STATICCALL, and DELEGATECALL subject class. Note
/// that plain CALL targets do not appear here. EIP-161 marks even a
/// zero-value CALL's recipient as touched (the state-clearing rule),
/// which revm mirrors, so a call target is visible only through its
/// storage reads or `WriteSet` entry. `slot_reads` holds every accessed
/// slot whose value did not change, on both touched and untouched
/// accounts.
#[derive(Debug, Default, Clone)]
pub struct TouchSet {
    pub account_reads: Vec<Address>,
    pub slot_reads: Vec<(Address, B256)>,
}

/// A per-block execution scope: one `CacheDB` (layered over `parent`
/// composed with `snapshot`) that revm commits into after each tx, and
/// one EVM instance whose tx-env is swapped per transaction.
///
/// This replaces the old per-tx construction, which DHAT measured at
/// about 90% of all execution-path allocation (421KB/tx): eight 32KB
/// interpreter stacks per tx (the EVM rebuilt per call), plus a rehash
/// storm from re-seeding the whole block delta into a fresh cache for
/// every tx. Per-tx `delta` seeding disappears entirely. The committed
/// cache is now the intra-block view. `PendingDelta` remains the
/// boundary and BAL artifact, maintained by the caller exactly as before.
///
/// The free [`execute_tx`] wrapper (one scope per call) keeps the old
/// signature, for replay and tests. Hot paths hold one scope per block
/// (the executor) or per batch (the validator).
pub struct Executor<S: StateDatabase> {
    evm: revm::handler::MainnetEvm<
        revm::context::Context<
            revm::context::BlockEnv,
            revm::context::TxEnv,
            revm::context::CfgEnv,
            CacheDB<SnapshotDb<S>>,
        >,
    >,
    env: ExecEnv,
}

impl<S: StateDatabase> Executor<S> {
    /// Build the block's scope. The cache is seeded with the parent layer
    /// only (fixed for the whole block), and the EVM is constructed once.
    pub fn new(
        snapshot: S,
        parent: Option<&PendingDelta>,
        env: ExecEnv,
    ) -> Result<Self, ExecutorError> {
        let (block, cfg) = (env.block_env(), env.cfg_env());
        Self::new_with_envs(snapshot, parent, env, block, cfg)
    }

    /// Like [`Executor::new`], but with caller-supplied revm envs, instead
    /// of the ones derived from [`ExecEnv`]. This is the seam the EEST
    /// conformance runner uses to execute under a fixture's block env
    /// (coinbase, basefee, difficulty, blob params). It tests the
    /// engine's revm integration, not kardamom's boundary derivation.
    /// Production paths use [`Executor::new`]. Here, `env` only feeds
    /// the metadata on skip receipts (block number).
    pub fn new_with_envs(
        snapshot: S,
        parent: Option<&PendingDelta>,
        env: ExecEnv,
        block: revm::context::BlockEnv,
        cfg: revm::context::CfgEnv,
    ) -> Result<Self, ExecutorError> {
        let cache: CacheDB<SnapshotDb<S>> = CacheDB::new(SnapshotDb { inner: snapshot });
        let evm = Context::mainnet()
            .with_db(cache)
            .with_block(block)
            .with_cfg(cfg)
            .build_mainnet();
        let mut scope = Self { evm, env };
        if let Some(layer) = parent {
            scope.seed_layer(layer)?;
        }
        Ok(scope)
    }

    /// Seed a delta layer into the block cache. Later seeds overwrite
    /// earlier ones. This is used for the parent layer at construction,
    /// and by the compatibility wrapper for a caller-maintained live
    /// delta.
    pub fn seed_layer(&mut self, layer: &PendingDelta) -> Result<(), ExecutorError> {
        let cache = revm::context_interface::ContextTr::db_mut(&mut *self.evm);
        seed_cache_layer(cache, layer).map_err(ExecutorError::State)
    }

    /// Execute a DEPOSIT on this block scope. The historic free function
    /// (`execute_deposit_tx`, kept as the equivalence reference) rebuilt a
    /// fresh `CacheDB` and re-seeded parent + delta for every deposit; on
    /// the scope, the block cache is reused and only the nonce check is
    /// toggled for the inner call (deposits carry no nonce).
    ///
    /// Artifact contract: receipt, `WriteSet` (called-contract code
    /// included, unchanged entries filtered out — see
    /// [`WriteSet::from_evm_state_deposit`]), `write_set_hash`, and the
    /// BAL claims match the historic path's true changes. The
    /// `old_and_new_deposit_paths_agree` test in `deposit.rs` is the gate.
    ///
    /// A deposit that revm rejects at validation (for example an L1
    /// `gasLimit` below the intrinsic cost) becomes a deterministic failed
    /// receipt (`failed_deposit_receipt`): the mint stays, the inner call
    /// never runs. Local failures (database errors) fail-stop the pipeline,
    /// so a mint committed before such a failure cannot leak into a later
    /// tx: nothing later runs.
    pub fn execute_deposit(
        &mut self,
        tx_idx: TxIndex,
        tx_position: BPosition,
        deposit: &kardamom_types::Deposit,
        tx_index_in_block: u64,
        cumulative_gas_used_before: u64,
        bal: Option<(&mut revm::state::bal::Bal, u64)>,
    ) -> Result<(Receipt, WriteSet), ExecutorError> {
        use revm::context::result::ExecutionResult;

        // (1) Mint pre-credit, committed unconditionally — the mint is
        // durable regardless of inner-call outcome.
        let cache = revm::context_interface::ContextTr::db_mut(&mut *self.evm);
        let mut info = revm::Database::basic(cache, deposit.from)
            .map_err(|e| ExecutorError::Execution {
                idx: tx_idx,
                detail: format!("basic({:?}): {e:?}", deposit.from),
            })?
            .unwrap_or_default();
        info.balance = info
            .balance
            .checked_add(alloy_primitives::U256::from(deposit.mint))
            .ok_or_else(|| ExecutorError::Execution {
                idx: tx_idx,
                detail: format!(
                    "mint overflow: account {:?} balance + mint {} would exceed U256::MAX",
                    deposit.from, deposit.mint
                ),
            })?;
        let mut acct = revm::state::Account::from(info);
        acct.mark_touch();
        revm::DatabaseCommit::commit(cache, core::iter::once((deposit.from, acct)).collect());

        // (2) Inner call with the nonce check off. The toggle-restore pair
        // has NO fallible call between toggle and restore: an early return
        // with the toggle still set would run every later tx in the block
        // without nonce validation.
        let tx_env = tx_env_from_deposit(deposit);
        (*self.evm).modify_cfg(|c| c.disable_nonce_check = true);
        let result = self.evm.transact(tx_env);
        (*self.evm).modify_cfg(|c| c.disable_nonce_check = false);
        let outcome = match result {
            Ok(o) => o,
            // Deterministic input invalidity: the mint stays, the inner
            // call never ran. Same artifact as the free function.
            Err(EVMError::Transaction(e)) => {
                let cache = revm::context_interface::ContextTr::db_mut(&mut *self.evm);
                let info = revm::Database::basic(cache, deposit.from)
                    .map_err(|e| ExecutorError::Execution {
                        idx: tx_idx,
                        detail: format!("basic({:?}): {e:?}", deposit.from),
                    })?
                    .unwrap_or_default();
                let ws = mint_only_write_set(deposit, &info);
                return Ok(failed_deposit_receipt(
                    skip_reason_of_tx(&e),
                    &format!("{e:?}"),
                    tx_position,
                    deposit,
                    ws,
                    bal,
                    self.env.block_number,
                    tx_index_in_block,
                    cumulative_gas_used_before,
                ));
            }
            Err(e) => return Err(derived_tx_rejection(e, tx_idx)),
        };

        let gas_used = outcome.result.gas().tx_gas_used();
        let (status_success, logs) = match &outcome.result {
            ExecutionResult::Success { logs, .. } => (true, logs.clone()),
            ExecutionResult::Revert { .. } => (false, alloc::vec::Vec::new()),
            ExecutionResult::Halt { .. } => (false, alloc::vec::Vec::new()),
        };

        // (3) The deposit artifact keeps read slots (see the extractor
        // doc). Capture before the commit consumes the state.
        let mut ws = WriteSet::from_evm_state_deposit(&outcome.state);
        revm::DatabaseCommit::commit(
            revm::context_interface::ContextTr::db_mut(&mut *self.evm),
            outcome.state,
        );
        // The sender always carries the mint in the artifact, even when
        // the inner call never touched it.
        if !ws.accounts.iter().any(|(a, _)| *a == deposit.from) {
            let cache = revm::context_interface::ContextTr::db_mut(&mut *self.evm);
            let info = revm::Database::basic(cache, deposit.from)
                .map_err(|e| ExecutorError::Execution {
                    idx: tx_idx,
                    detail: format!("basic({:?}): {e:?}", deposit.from),
                })?
                .unwrap_or_default();
            ws.accounts
                .push((deposit.from, (info.nonce, info.balance, info.code_hash)));
            ws.finish();
        }
        if let Some((bal, bal_index)) = bal {
            ws.record_into_bal(bal, bal_index);
        }

        let write_set_hash = ws.hash();
        let wire_logs: alloc::vec::Vec<kardamom_types::WireLog> =
            logs.iter().map(kardamom_types::WireLog::from).collect();
        let cumulative_gas_used = cumulative_gas_used_before + gas_used;
        let receipt = Receipt {
            tx_idx: tx_position,
            // Deposits' canonical id is the OP source_hash, NOT a 2718
            // keccak.
            tx_hash: deposit.source_hash,
            // L1-originated: consumes no L2 nonce (the filler `nonce: 0`
            // is NOT a real nonce — consumers branch on the type).
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
            block_number: self.env.block_number,
            transaction_index: tx_index_in_block,
            cumulative_gas_used,
            skip_reason: None,
        };
        Ok((receipt, ws))
    }

    /// Execute one cross-chain delivery (a 0x7D message) on this block
    /// scope. Mirrors [`Self::execute_deposit`]: the block cache is
    /// reused, the call is fee-free with the nonce check off, and the
    /// receipt's canonical id is the message's `source_hash`, not a 2718
    /// keccak.
    ///
    /// Artifact contract: receipt, `WriteSet` (called-contract code
    /// included, unchanged entries filtered out — see
    /// [`WriteSet::from_evm_state_deposit`]), `write_set_hash`, and the
    /// BAL claims match the historic free function (`execute_xchain_tx`).
    /// A gate test like `old_and_new_deposit_paths_agree` applies here
    /// too.
    ///
    /// v1 carries no value. A nonzero `message.value`, or a message revm
    /// rejects at validation, becomes a deterministic failed receipt
    /// (`failed_xchain_receipt`) before it touches the cache. Only local
    /// failures (database errors) are engine errors.
    #[allow(clippy::too_many_arguments)] // matches execute_deposit's shape;
    // see the equivalent allow there for the reason.
    pub fn execute_xchain(
        &mut self,
        tx_idx: TxIndex,
        tx_position: BPosition,
        origin_chain_id: u64,
        message: &XChainMessage,
        tx_index_in_block: u64,
        cumulative_gas_used_before: u64,
        bal: Option<(&mut revm::state::bal::Bal, u64)>,
    ) -> Result<(Receipt, WriteSet), ExecutorError> {
        if let Some((reason, detail)) = xchain_value_rejection(origin_chain_id, message) {
            return Ok(failed_xchain_receipt(
                reason,
                &detail,
                tx_position,
                origin_chain_id,
                message,
                self.env.block_number,
                tx_index_in_block,
                cumulative_gas_used_before,
            ));
        }

        // Inner call with the nonce check off. No fallible call sits
        // between the toggle and its restore — deposits and cross-chain
        // deliveries share this rule; see the note on `execute_deposit`.
        let tx_env = tx_env_from_xchain(origin_chain_id, message);
        (*self.evm).modify_cfg(|c| c.disable_nonce_check = true);
        let result = self.evm.transact(tx_env);
        (*self.evm).modify_cfg(|c| c.disable_nonce_check = false);
        let outcome = match result {
            Ok(o) => o,
            Err(EVMError::Transaction(e)) => {
                return Ok(failed_xchain_receipt(
                    skip_reason_of_tx(&e),
                    &format!("{e:?}"),
                    tx_position,
                    origin_chain_id,
                    message,
                    self.env.block_number,
                    tx_index_in_block,
                    cumulative_gas_used_before,
                ));
            }
            Err(e) => return Err(derived_tx_rejection(e, tx_idx)),
        };

        let gas_used = outcome.result.gas().tx_gas_used();
        // A revert inside deliver (or a bubbled-up inner-call revert)
        // marks the receipt failed, but it is not an engine error. This
        // is the deposit posture.
        let (status_success, logs) = match &outcome.result {
            ExecutionResult::Success { logs, .. } => (true, logs.clone()),
            ExecutionResult::Revert { .. } => (false, Vec::new()),
            ExecutionResult::Halt { .. } => (false, Vec::new()),
        };

        let ws = WriteSet::from_evm_state_deposit(&outcome.state);
        revm::DatabaseCommit::commit(
            revm::context_interface::ContextTr::db_mut(&mut *self.evm),
            outcome.state,
        );
        if let Some((bal, bal_index)) = bal {
            ws.record_into_bal(bal, bal_index);
        }

        let write_set_hash = ws.hash();
        let wire_logs: Vec<kardamom_types::WireLog> =
            logs.iter().map(kardamom_types::WireLog::from).collect();
        let cumulative_gas_used = cumulative_gas_used_before + gas_used;
        let receipt = Receipt {
            tx_idx: tx_position,
            // The canonical id stamped at derivation (source_hash), not a
            // 2718 keccak. Same posture as deposits.
            tx_hash: message.source_hash,
            tx_type: kardamom_types::TX_TYPE_XCHAIN,
            status: status_success,
            gas_used,
            logs: wire_logs,
            write_set_hash,
            // Origin-derived: consumes no local nonce. The filler
            // `nonce: 0` is never a real nonce; consumers branch on
            // `tx_type`, not on it.
            nonce: 0,
            from: xchain::xchain_tx_sender(origin_chain_id),
            to: Some(xchain::INBOX),
            contract_address: None,
            // Destination-side delivery pays no fee (quota-gated instead).
            effective_gas_price: 0,
            block_number: self.env.block_number,
            transaction_index: tx_index_in_block,
            cumulative_gas_used,
            skip_reason: None,
        };
        Ok((receipt, ws))
    }

    #[allow(clippy::too_many_arguments)] // the per-tx entry point's shape;
    // see the equivalent allow on `execute_once`.
    pub fn execute_tx(
        &mut self,
        tx_idx: TxIndex,
        tx_position: BPosition,
        inbound_envelope: &TxEnvelope,
        tx_index_in_block: u64,
        cumulative_gas_used_before: u64,
        // EIP-7928 capture: see the free `execute_tx`.
        bal: Option<(&mut revm::state::bal::Bal, u64)>,
        // Footprint-shadow read capture: see [`TouchSet`]. This is `None`
        // everywhere except the executor's streaming path, with the
        // shadow enabled.
        touches: Option<&mut TouchSet>,
    ) -> Result<(Receipt, WriteSet), ExecutorError> {
        // Derivation must be total. A canonical record that is
        // deterministically invalid must not halt execution. This
        // includes undecodable bytes, and a tx revm rejects at validation
        // (a NonceTooLow duplicate that got past every dedup layer,
        // NonceTooHigh from a sealed gap, insufficient balance, and so
        // on). Every replica, the recovery replay, and the validator all
        // see the same input, and would all halt in lockstep, forever (a
        // poisoned log wedges recovery replay on the same record
        // forever). Instead, this skips the record with a receipt:
        // `status=false, gas_used=0`. Real execution can never produce
        // this pair, since any executed tx (including a revert or a
        // halt) charges at least intrinsic gas. So the pair is the
        // wire-visible skip marker
        // ([`kardamom_types::Receipt::is_invalid_skip`]). The skip is
        // part of the deterministic state transition (empty write set,
        // no state change, counters advance), identical across the live
        // path, replay, and validator re-execution. Non-deterministic
        // failures (database errors) still fail-stop below. A skip is
        // loud by design: any occurrence means an upstream guard failed,
        // and `kardamom_executor_invalid_tx_skipped_total` should alert.
        let alloy_env = match DecodedTx::decode(&inbound_envelope.raw_tx, tx_idx) {
            Ok(env_) => env_,
            Err(e) => {
                return Ok(Self::skip_receipt(
                    SkipReason::Undecodable,
                    &format!("undecodable raw_tx: {e}"),
                    tx_position,
                    inbound_envelope,
                    0,
                    None,
                    self.env.block_number,
                    tx_index_in_block,
                    cumulative_gas_used_before,
                ));
            }
        };
        self.execute_tx_decoded(
            tx_idx,
            tx_position,
            inbound_envelope,
            &alloy_env,
            tx_index_in_block,
            cumulative_gas_used_before,
            bal,
            touches,
        )
    }

    /// [`Self::execute_tx`], with the RLP already decoded.
    ///
    /// Decoding costs about 180ns/tx, and whoever reads the tx stream
    /// naturally does it (the STM engine's `prepare` does exactly this,
    /// off the execution thread). Exposing the pre-decoded entry point
    /// lets the sequential path get the same benefit, and lets the A/B
    /// harness compare the two engines on equal footing, instead of
    /// charging decode to only one side.
    #[allow(clippy::too_many_arguments)]
    pub fn execute_tx_decoded(
        &mut self,
        tx_idx: TxIndex,
        tx_position: BPosition,
        inbound_envelope: &TxEnvelope,
        alloy_env: &DecodedTx,
        tx_index_in_block: u64,
        cumulative_gas_used_before: u64,
        bal: Option<(&mut revm::state::bal::Bal, u64)>,
        touches: Option<&mut TouchSet>,
    ) -> Result<(Receipt, WriteSet), ExecutorError> {
        let signer = inbound_envelope.sender; // trusted from the proxy; no recovery
        let nonce = alloy_env.nonce();
        let to = alloy_env.to();
        // Effective gas price mirrors the value `tx_env_from_alloy` feeds
        // to revm: the legacy or 2930 `gas_price` when present, otherwise
        // the 1559 or 4844 `max_fee_per_gas` cap. Version 0 has
        // basefee = 0, so the cap is what gets paid.
        let effective_gas_price = alloy_env
            .gas_price()
            .unwrap_or_else(|| alloy_env.max_fee_per_gas());

        let tx_env = alloy_env.tx_env(signer);
        let outcome = match self.evm.transact(tx_env) {
            Ok(o) => o,
            // Deterministic input invalidity: every replica computes the
            // same rejection from the same state and tx. Skip, never halt.
            Err(revm::context::result::EVMError::Transaction(e)) => {
                return Ok(Self::skip_receipt(
                    skip_reason_of_tx(&e),
                    &format!("{e:?}"),
                    tx_position,
                    inbound_envelope,
                    nonce,
                    to,
                    self.env.block_number,
                    tx_index_in_block,
                    cumulative_gas_used_before,
                ));
            }
            Err(revm::context::result::EVMError::Header(e)) => {
                return Ok(Self::skip_receipt(
                    SkipReason::Header,
                    &format!("{e:?}"),
                    tx_position,
                    inbound_envelope,
                    nonce,
                    to,
                    self.env.block_number,
                    tx_index_in_block,
                    cumulative_gas_used_before,
                ));
            }
            // Database or custom failures are local, not derivable from
            // the input. Halting here is correct; crash recovery replays
            // cleanly.
            Err(e) => {
                return Err(ExecutorError::Execution {
                    idx: tx_idx,
                    detail: format!("{e:?}"),
                });
            }
        };

        let gas_used = outcome.result.gas().tx_gas_used();
        let (status, logs) = match &outcome.result {
            ExecutionResult::Success { logs, .. } => (ReceiptStatus::Success, logs.clone()),
            ExecutionResult::Revert { .. } => (ReceiptStatus::Revert, Vec::new()),
            ExecutionResult::Halt { reason, .. } => {
                (ReceiptStatus::Halt(reason.clone()), Vec::new())
            }
        };

        // Build the write set from revm's per-tx EvmState. Only touched
        // and changed accounts and slots are emitted, which keeps the
        // per-tx hash stable across replicas. Revm iterates over an
        // AddressMap; `WriteSet::finish` sorts the entries afterward.
        let ws = WriteSet::from_evm_state(&outcome.state);
        if let Some((bal, bal_index)) = bal {
            for (addr, account) in outcome.state.iter() {
                bal.update_account(bal_index, *addr, account);
            }
        }
        if let Some(t) = touches {
            for (addr, account) in outcome.state.iter() {
                if !account.is_touched() {
                    t.account_reads.push(*addr);
                }
                for (key, slot) in account.storage.iter() {
                    if slot.original_value == slot.present_value {
                        t.slot_reads
                            .push((*addr, B256::from(key.to_be_bytes::<32>())));
                    }
                }
            }
        }
        // Fold this tx's writes into the block cache. Later txs read
        // them directly, with no per-tx re-seeding (that used to cause
        // 84% of all allocation).
        revm::DatabaseCommit::commit(
            revm::context_interface::ContextTr::db_mut(&mut *self.evm),
            outcome.state,
        );

        let write_set_hash = ws.hash();
        let wire_logs = logs.iter().map(kardamom_types::WireLog::from).collect();
        let cumulative_gas_used = cumulative_gas_used_before + gas_used;
        // Contract address is meaningful only for a successful CREATE tx.
        // A failed CREATE and any CALL tx have `contract_address = None`.
        let contract_address = if to.is_none() && status.is_success() {
            Some(signer.create(nonce))
        } else {
            None
        };

        // Copy `tx_hash` straight from the inbound envelope. Do not
        // recompute it with keccak256(raw_tx). The proxy is the canonical
        // hash producer.
        let receipt = Receipt {
            tx_idx: tx_position,
            tx_hash: inbound_envelope.tx_hash,
            // EIP-2718 type, read from the raw envelope (legacy is 0x00).
            tx_type: kardamom_types::tx_type_of(&inbound_envelope.raw_tx),
            status: status.is_success(),
            gas_used,
            logs: wire_logs,
            write_set_hash,
            nonce,
            from: signer,
            to,
            contract_address,
            effective_gas_price,
            block_number: self.env.block_number,
            transaction_index: tx_index_in_block,
            cumulative_gas_used,
            skip_reason: None,
        };
        Ok((receipt, ws))
    }
}

/// Execute one tx against a snapshot and the current `PendingDelta`.
/// Returns the receipt, plus a fresh per-tx `WriteSet`. The caller folds
/// the `WriteSet` into the `PendingDelta` before calling this for the
/// next tx, so later txs see the writes.
///
/// `inbound_envelope: &TxEnvelope` is `kardamom_types::TxEnvelope`. Its
/// `sender` and `tx_hash` are trusted unconditionally; the proxy
/// populated them at the system boundary. The executor never recomputes
/// `tx_hash` and never recovers a sender. It copies both fields straight
/// through into the outbound `Receipt`.
///
/// `tx_index_in_block` is the zero-based index within the in-flight
/// block (it resets at every `BlockBoundaryStart`).
/// `cumulative_gas_used_before` is the running gas sum for txs already
/// executed in the same block. The returned receipt's
/// `cumulative_gas_used` equals this plus the new tx's `gas_used`.
#[allow(clippy::too_many_arguments)] // 8 args is the natural shape of an
// "execute one tx" entry point. Packaging them into a struct would just
// move the noise around, not reduce it.
impl<S: StateDatabase> Executor<S> {
    /// One-shot convenience method. It builds a throwaway executor for
    /// one call. It seeds the caller's live delta, then executes one tx.
    /// Use this for replay and tests. A hot path holds one [`Executor`]
    /// per block or batch instead, for the allocation win.
    #[allow(clippy::too_many_arguments)] // the one-tx entry point's shape.
    pub fn execute_once(
        snapshot: &S,
        parent: Option<&PendingDelta>,
        delta: &PendingDelta,
        env: ExecEnv,
        tx_idx: TxIndex,
        tx_position: BPosition,
        inbound_envelope: &TxEnvelope,
        tx_index_in_block: u64,
        cumulative_gas_used_before: u64,
        // EIP-7928 capture. When set, this records every account and
        // slot the tx touched into the block's Bal, under `bal_index`
        // (a 1-based tx position, by revm's convention). Writes go in
        // as (index, value); read-only accesses go into storage_reads.
        // Revm classifies each by comparing original and present
        // values in `outcome.state`.
        bal: Option<(&mut revm::state::bal::Bal, u64)>,
    ) -> Result<(Receipt, WriteSet), ExecutorError> {
        let mut scope = Executor::new(snapshot, parent, env)?;
        scope.seed_layer(delta)?;
        scope.execute_tx(
            tx_idx,
            tx_position,
            inbound_envelope,
            tx_index_in_block,
            cumulative_gas_used_before,
            bal,
            None,
        )
    }
}

/// Classify a revm tx-validation rejection into the wire [`SkipReason`].
/// This is part of the state transition: every replica maps the same
/// rejection to the same reason. This function stands in for
/// `From<&InvalidTransaction>`, which the orphan rule blocks here (both
/// types are foreign to this crate).
pub fn skip_reason_of_tx(err: &revm::context::result::InvalidTransaction) -> SkipReason {
    use revm::context::result::InvalidTransaction as E;
    match err {
        E::NonceTooLow { .. } => SkipReason::NonceTooLow,
        E::NonceTooHigh { .. } => SkipReason::NonceTooHigh,
        E::LackOfFundForMaxFee { .. } | E::OverflowPaymentInTransaction => {
            SkipReason::InsufficientFunds
        }
        E::CallerGasLimitMoreThanBlock
        | E::CallGasCostMoreThanGasLimit { .. }
        | E::GasFloorMoreThanGasLimit { .. }
        | E::TxGasLimitGreaterThanCap { .. } => SkipReason::GasLimit,
        E::PriorityFeeGreaterThanMaxFee
        | E::GasPriceLessThanBasefee
        | E::BlobGasPriceGreaterThanMax { .. } => SkipReason::Fee,
        E::CreateInitCodeSizeLimit => SkipReason::InitCodeSize,
        E::RejectCallerWithCode => SkipReason::SenderHasCode,
        _ => SkipReason::OtherTransaction,
    }
}

/// Map a non-validation revm error on a derived tx (a deposit or a 0x7D
/// delivery) to the engine error. The `EVMError::Transaction` arm is
/// handled by the caller as a deterministic failed receipt. Everything
/// else (database, header, custom) is local or is a block-env fault, not
/// derivable from the record. It stays fatal, so crash recovery replays
/// cleanly.
pub(super) fn derived_tx_rejection<DbErr: core::fmt::Debug>(
    err: EVMError<DbErr>,
    idx: TxIndex,
) -> ExecutorError {
    ExecutorError::Execution {
        idx,
        detail: format!("{err:?}"),
    }
}

/// The deterministic skip receipt, as an associated constructor. It
/// needs no state access. The Block-STM engine builds skip receipts
/// inside its own worker EVM, and both paths must keep one definition
/// of the skip artifact.
impl<S: StateDatabase> Executor<S> {
    /// Build the deterministic skip receipt for a canonical record that is
    /// invalid at execution: `status=false, gas_used=0` (the wire-visible
    /// marker, since real execution always charges intrinsic gas), empty
    /// logs, an empty write set (`WriteSet::default().hash()` on both the
    /// live and re-exec sides), and unchanged gas accounting. This is loud
    /// by design: a log plus a counter. A skip existing at all means an
    /// upstream guard (sequencer nonce fence, cluster dedup, or resync
    /// floor) let an invalid record reach the canonical log.
    #[allow(clippy::too_many_arguments)]
    #[cfg_attr(not(feature = "std"), allow(unused_variables))]
    pub fn skip_receipt(
        reason: SkipReason,
        detail: &str,
        tx_position: BPosition,
        inbound_envelope: &TxEnvelope,
        nonce: u64,
        to: Option<Address>,
        block_number: u64,
        tx_index_in_block: u64,
        cumulative_gas_used_before: u64,
    ) -> (Receipt, WriteSet) {
        // Loudness is a `std`-side concern. The skip receipt itself is
        // the consensus artifact, and guest builds produce it
        // identically. `detail` keeps the full revm rejection next to
        // the coarse enum.
        #[cfg(feature = "std")]
        {
            tracing::error!(
                tx_hash = ?inbound_envelope.tx_hash,
                from = ?inbound_envelope.sender,
                nonce,
                block = block_number,
                reason = reason.as_str(),
                detail,
                "INVALID canonical tx SKIPPED (deterministic; upstream guard failed — investigate)"
            );
            crate::metrics::record_invalid_tx_skipped(reason);
        }
        let ws = WriteSet::default();
        let write_set_hash = ws.hash();
        let receipt = Receipt {
            tx_idx: tx_position,
            tx_hash: inbound_envelope.tx_hash,
            tx_type: kardamom_types::tx_type_of(&inbound_envelope.raw_tx),
            status: false,
            gas_used: 0,
            logs: Vec::new(),
            write_set_hash,
            nonce,
            from: inbound_envelope.sender,
            to,
            contract_address: None,
            effective_gas_price: 0,
            block_number,
            transaction_index: tx_index_in_block,
            cumulative_gas_used: cumulative_gas_used_before,
            skip_reason: Some(reason),
        };
        (receipt, ws)
    }

    /// [`Self::skip_receipt`] with this executor's block number.
    #[allow(clippy::too_many_arguments)]
    pub fn skip(
        &self,
        reason: SkipReason,
        detail: &str,
        tx_position: BPosition,
        inbound_envelope: &TxEnvelope,
        nonce: u64,
        to: Option<Address>,
        tx_index_in_block: u64,
        cumulative_gas_used_before: u64,
    ) -> (Receipt, WriteSet) {
        Self::skip_receipt(
            reason,
            detail,
            tx_position,
            inbound_envelope,
            nonce,
            to,
            self.env.block_number,
            tx_index_in_block,
            cumulative_gas_used_before,
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::executor::test_support::{boundary, pos};
    use crate::state::MockStateDatabase;
    use alloy_consensus::{SignableTransaction, TxLegacy};
    use alloy_eips::eip2718::Encodable2718;
    use alloy_network::TxSignerSync;
    use alloy_primitives::Bytes as AlloyBytes;
    use alloy_primitives::{TxKind as APTxKind, U256, address, keccak256};
    use alloy_signer_local::PrivateKeySigner;
    use bytes::Bytes;
    use kardamom_types::TxEnvelope as KtTxEnvelope;
    use revm::primitives::KECCAK_EMPTY;

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

    // -- Deterministically invalid canonical txs skip, never halt ---------

    #[test]
    fn nonce_too_low_skips_with_marker_receipt_and_chain_continues() {
        let signer = PrivateKeySigner::random();
        let from = signer.address();
        let to = address!("0000000000000000000000000000000000001234");
        // The sender's canonical nonce is 5. A nonce-3 tx (a duplicate
        // past every dedup layer) is deterministically invalid.
        let snap = MockStateDatabase::builder()
            .account(from, U256::from(10u128.pow(18)), 5, KECCAK_EMPTY)
            .build();
        let delta = PendingDelta::new();
        let env = ExecEnv::new(1, &boundary(1));

        let stale = signed_transfer(&signer, to, 1_000, 3);
        let (receipt, ws) = Executor::execute_once(
            &snap,
            None,
            &delta,
            env,
            TxIndex(0),
            pos(0),
            &stale,
            0,
            77,
            None,
        )
        .expect("invalid tx must SKIP, not error");
        assert!(receipt.is_invalid_skip(), "status=false, gas_used=0 marker");
        assert_eq!(
            receipt.skip_reason,
            Some(SkipReason::NonceTooLow),
            "typed cause on the wire (#241)"
        );
        assert_eq!(receipt.tx_hash, stale.tx_hash);
        assert_eq!(receipt.nonce, 3);
        assert_eq!(
            receipt.cumulative_gas_used, 77,
            "gas accounting unchanged by a skip"
        );
        assert_eq!(
            receipt.write_set_hash,
            WriteSet::default().hash(),
            "a skip writes NOTHING (empty-set hash on every re-exec path)"
        );
        assert!(ws.accounts.is_empty() && ws.storage.is_empty() && ws.code.is_empty());

        // The chain continues. The sender's real next tx, nonce 5, executes.
        let env2 = ExecEnv::new(1, &boundary(1));
        let live = signed_transfer(&signer, to, 1_000, 5);
        let (r2, _) = Executor::execute_once(
            &snap,
            None,
            &delta,
            env2,
            TxIndex(1),
            pos(64),
            &live,
            1,
            77,
            None,
        )
        .expect("valid tx after a skip");
        assert!(r2.status);
        assert!(!r2.is_invalid_skip());
        assert_eq!(r2.skip_reason, None, "an executed tx carries no reason");
    }

    #[test]
    fn undecodable_raw_tx_skips_with_marker_receipt() {
        let signer = PrivateKeySigner::random();
        let snap = MockStateDatabase::builder().build();
        let delta = PendingDelta::new();
        let env = ExecEnv::new(1, &boundary(1));
        let garbage = KtTxEnvelope {
            correlation_id: 9,
            raw_tx: Bytes::from(vec![0xde, 0xad, 0xbe, 0xef]),
            sender: signer.address(),
            tx_hash: keccak256([0xde, 0xad, 0xbe, 0xef]),
        };
        let (receipt, ws) = Executor::execute_once(
            &snap,
            None,
            &delta,
            env,
            TxIndex(0),
            pos(0),
            &garbage,
            0,
            0,
            None,
        )
        .expect("undecodable bytes must SKIP, not error");
        assert!(receipt.is_invalid_skip());
        assert_eq!(receipt.skip_reason, Some(SkipReason::Undecodable));
        assert_eq!(receipt.nonce, 0, "nonce unknowable from undecodable bytes");
        assert_eq!(receipt.write_set_hash, WriteSet::default().hash());
        assert!(ws.accounts.is_empty());
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
        let (receipt, ws) = Executor::execute_once(
            &snap,
            None,
            &delta,
            env,
            TxIndex(0),
            pos(0),
            &env_tx,
            0,
            0,
            None,
        )
        .expect("execute");

        // The receipt's tx_hash must equal the inbound envelope's
        // tx_hash, byte for byte. The executor never recomputes it.
        assert_eq!(receipt.tx_hash, env_tx.tx_hash);
        assert!(receipt.status); // success is bool true (kardamom_types::Receipt)
        assert!(receipt.gas_used >= 21_000);
        // Both accounts are touched: the sender (balance and nonce) and
        // the recipient (balance).
        assert!(ws.account(&from).is_some());
        assert!(ws.account(&to).is_some());
        assert_eq!(ws.account(&to).unwrap().1, U256::from(1_000u64));
        // No storage or code writes for a plain transfer.
        assert!(ws.storage.is_empty());
        assert!(ws.code.is_empty());

        // RPC enrichment populated by execute_tx.
        assert_eq!(receipt.from, from);
        assert_eq!(receipt.to, Some(to));
        assert_eq!(receipt.contract_address, None);
        assert_eq!(receipt.nonce, 0);
        assert_eq!(receipt.effective_gas_price, 0); // the tx was built with gas_price = 0
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
        let (r1, ws1) = Executor::execute_once(
            &snap,
            None,
            &delta,
            env,
            TxIndex(0),
            pos(0),
            &tx1,
            0,
            0,
            None,
        )
        .expect("execute 1");
        assert!(r1.status);
        assert_eq!(r1.tx_hash, tx1.tx_hash); // copied, not recomputed
        assert_eq!(r1.nonce, 0);
        assert_eq!(r1.transaction_index, 0);
        let gas_after_tx1 = r1.cumulative_gas_used;
        delta.apply(ws1);

        // Second transfer from the same sender. The nonce must be 1, and
        // the sender balance must already show a debit of 100.
        let tx2 = signed_transfer(&signer, to, 50, 1);
        let (r2, ws2) = Executor::execute_once(
            &snap,
            None,
            &delta,
            env,
            TxIndex(1),
            pos(1),
            &tx2,
            1,
            gas_after_tx1,
            None,
        )
        .expect("execute 2");
        assert!(r2.status);
        assert_eq!(r2.tx_hash, tx2.tx_hash);
        assert_eq!(ws2.account(&to).unwrap().1, U256::from(150u64));
        assert_eq!(ws2.account(&from).unwrap().0, 2); // nonce
        // RPC enrichment: tx2 sees a higher nonce and transaction_index.
        // cumulative_gas_used adds up across both txs in the block.
        assert_eq!(r2.nonce, 1);
        assert_eq!(r2.transaction_index, 1);
        assert_eq!(r2.cumulative_gas_used, gas_after_tx1 + r2.gas_used);
    }

    /// Regression: EIP-7928 capture must actually populate the block BAL
    /// when a BAL handle is supplied to `execute_tx` (spec phase 1). An
    /// empty BAL means the validator has nothing to verify or seed from.
    #[test]
    fn execute_tx_captures_into_the_block_bal() {
        let signer = PrivateKeySigner::random();
        let from = signer.address();
        let to = address!("0000000000000000000000000000000000005678");
        let snap = MockStateDatabase::builder()
            .account(from, U256::from(10u128.pow(18)), 0, KECCAK_EMPTY)
            .build();
        let delta = PendingDelta::new();
        let env = ExecEnv::new(1, &boundary(1));
        let tx = signed_transfer(&signer, to, 1_000, 0);

        let mut bal = revm::state::bal::Bal::new();
        let (_receipt, ws) = Executor::execute_once(
            &snap,
            None,
            &delta,
            env,
            TxIndex(0),
            pos(0),
            &tx,
            0,
            0,
            Some((&mut bal, 1)),
        )
        .expect("execute");
        assert!(!ws.accounts.is_empty(), "the tx wrote accounts");

        let alloy = bal.into_alloy_bal();
        assert!(
            !alloy.is_empty(),
            "capture produced an EMPTY BAL for a tx that wrote {} accounts",
            ws.accounts.len()
        );
        // The sender and recipient must both appear, with balance or
        // nonce claims.
        let has_sender = alloy.iter().any(|a| {
            a.address == from && (!a.balance_changes.is_empty() || !a.nonce_changes.is_empty())
        });
        assert!(
            has_sender,
            "sender's balance/nonce change must be claimed: {alloy:?}"
        );
    }

    /// Production shape: the second tx in a block executes against a
    /// non-empty `delta`, seeded into the CacheDB. Capture must still
    /// record it. An earlier bug produced empty BALs once deltas grew to
    /// about 76KB/block, even though empty-delta tests passed.
    #[test]
    fn execute_tx_captures_with_a_seeded_delta() {
        let signer = PrivateKeySigner::random();
        let from = signer.address();
        let to = address!("0000000000000000000000000000000000009999");
        let snap = MockStateDatabase::builder()
            .account(from, U256::from(10u128.pow(18)), 0, KECCAK_EMPTY)
            .build();
        let mut delta = PendingDelta::new();
        let env = ExecEnv::new(1, &boundary(1));

        // tx1 populates the delta (as in a real block).
        let tx1 = signed_transfer(&signer, to, 1_000, 0);
        let mut bal = revm::state::bal::Bal::new();
        let (_r1, ws1) = Executor::execute_once(
            &snap,
            None,
            &delta,
            env,
            TxIndex(0),
            pos(0),
            &tx1,
            0,
            0,
            Some((&mut bal, 1)),
        )
        .expect("execute 1");
        delta.apply(ws1);
        let after_tx1 = bal.clone().into_alloy_bal().len();

        // tx2 runs with the seeded delta. This is the production path.
        let tx2 = signed_transfer(&signer, to, 500, 1);
        let (_r2, ws2) = Executor::execute_once(
            &snap,
            None,
            &delta,
            env,
            TxIndex(1),
            pos(64),
            &tx2,
            1,
            21_000,
            Some((&mut bal, 2)),
        )
        .expect("execute 2");
        assert!(!ws2.accounts.is_empty(), "tx2 wrote accounts");

        let alloy = bal.into_alloy_bal();
        assert!(after_tx1 > 0, "tx1 must be captured");
        assert!(!alloy.is_empty(), "capture must survive a seeded delta");
        // tx2's claims must be present: some account carries a
        // bal_index 2 change.
        let has_tx2 = alloy.iter().any(|a| {
            a.balance_changes.iter().any(|c| c.block_access_index == 2)
                || a.nonce_changes.iter().any(|c| c.block_access_index == 2)
        });
        assert!(has_tx2, "tx2's claims missing from BAL: {alloy:?}");
    }
}

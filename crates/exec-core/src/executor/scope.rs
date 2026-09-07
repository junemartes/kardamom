//! [`Executor`]: the per-block EVM and committed cache, plus the
//! `execute_once` one-shot constructor. See [`super::skip`] for the
//! deterministic invalid-skip path.

use alloy_consensus::Transaction;
use alloy_primitives::{Address, B256};
use kardamom_types::{Receipt, SkipReason, StateDatabase, TxEnvelope};
use revm::context::result::ExecutionResult;
use revm::database::CacheDB;
use revm::{Context, ExecuteEvm, MainBuilder, MainContext};

use alloc::format;
use alloc::vec::Vec;

use crate::block_env::ExecEnv;
use crate::delta::{PendingDelta, WriteSet};
use crate::error::ExecutorError;
use crate::exec_types::{ReceiptStatus, TxSlot};

use super::db::{SnapshotDb, seed_cache_layer};
use super::skip::skip_reason_of_tx;
use super::tx_env::DecodedTx;

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
/// One EVM and one cache serve the whole block: no per-tx `CacheDB` or
/// EVM construction, and no per-tx re-seeding of the block delta into a
/// fresh cache. Per-tx `delta` seeding disappears entirely. The committed
/// cache is the intra-block view. `PendingDelta` remains the boundary and
/// BAL artifact, maintained by the caller.
///
/// [`Executor::execute_once`] builds a throwaway scope for a single call,
/// for replay and tests. Hot paths hold one scope per block (the
/// executor) or per batch (the validator).
pub struct Executor<S: StateDatabase> {
    pub(super) evm: revm::handler::MainnetEvm<
        revm::context::Context<
            revm::context::BlockEnv,
            revm::context::TxEnv,
            revm::context::CfgEnv,
            CacheDB<SnapshotDb<S>>,
        >,
    >,
    pub(super) env: ExecEnv,
}

impl<S: StateDatabase> Executor<S> {
    /// Build the block's scope. The cache is seeded with the parent layer
    /// only (fixed for the whole block), and the EVM is constructed once.
    /// # Errors
    ///
    /// Returns [`ExecutorError::State`] when seeding the parent layer
    /// fails.
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
    /// # Errors
    ///
    /// Returns [`ExecutorError::State`] when seeding the parent layer
    /// fails.
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
    ///
    /// # Errors
    ///
    /// Returns [`ExecutorError::State`] when a storage insert into the
    /// cache fails.
    pub fn seed_layer(&mut self, layer: &PendingDelta) -> Result<(), ExecutorError> {
        let cache = revm::context_interface::ContextTr::db_mut(&mut *self.evm);
        seed_cache_layer(cache, layer).map_err(ExecutorError::State)
    }

    /// Decode `inbound_envelope` and execute it. See
    /// [`Self::execute_tx_decoded`] for the semantics; this is the
    /// undecoded entry point, used where the caller has not already
    /// parsed the tx.
    ///
    /// # Errors
    ///
    /// Returns [`ExecutorError::Execution`] on a non-deterministic
    /// failure (a database error). A deterministically invalid tx
    /// (undecodable bytes, a validation rejection) is not an error: it
    /// returns `Ok` with a skip receipt.
    pub fn execute_tx(
        &mut self,
        slot: TxSlot,
        inbound_envelope: &TxEnvelope,
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
        let alloy_env = match DecodedTx::decode(&inbound_envelope.raw_tx, slot.tx_idx) {
            Ok(env_) => env_,
            Err(e) => {
                return Ok(Self::skip_receipt(
                    SkipReason::Undecodable,
                    &format!("undecodable raw_tx: {e}"),
                    slot,
                    inbound_envelope,
                    0,
                    None,
                    self.env.block_number,
                ));
            }
        };
        self.execute_tx_decoded(slot, inbound_envelope, &alloy_env, bal, touches)
    }

    /// [`Self::execute_tx`], with the RLP already decoded.
    ///
    /// Decoding costs about 180ns/tx, and whoever reads the tx stream
    /// naturally does it (the STM engine's `prepare` does exactly this,
    /// off the execution thread). Exposing the pre-decoded entry point
    /// lets the sequential path get the same benefit, and lets the A/B
    /// harness compare the two engines on equal footing, instead of
    /// charging decode to only one side.
    ///
    /// # Errors
    ///
    /// Returns [`ExecutorError::Execution`] on a non-deterministic
    /// failure (a database error). A deterministically invalid tx
    /// (a validation rejection) is not an error: it returns `Ok` with a
    /// skip receipt.
    pub fn execute_tx_decoded(
        &mut self,
        slot: TxSlot,
        inbound_envelope: &TxEnvelope,
        alloy_env: &DecodedTx,
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
                    slot,
                    inbound_envelope,
                    nonce,
                    to,
                    self.env.block_number,
                ));
            }
            Err(revm::context::result::EVMError::Header(e)) => {
                return Ok(Self::skip_receipt(
                    SkipReason::Header,
                    &format!("{e:?}"),
                    slot,
                    inbound_envelope,
                    nonce,
                    to,
                    self.env.block_number,
                ));
            }
            // Database or custom failures are local, not derivable from
            // the input. Halting here is correct; crash recovery replays
            // cleanly.
            Err(e) => {
                return Err(ExecutorError::Execution {
                    idx: slot.tx_idx,
                    detail: format!("{e:?}"),
                });
            }
        };

        // Build the write set from revm's per-tx EvmState. Only touched
        // and changed accounts and slots are emitted, which keeps the
        // per-tx hash stable across replicas. Revm iterates over an
        // AddressMap; `WriteSet::finish` sorts the entries afterward.
        let ws = WriteSet::from_evm_state(&outcome.state);
        if let Some((bal, bal_index)) = bal {
            for (addr, account) in &outcome.state {
                bal.update_account(bal_index, *addr, account);
            }
        }
        Self::capture_touches(&outcome.state, touches);
        // Fold this tx's writes into the block cache. Later txs read
        // them directly, with no per-tx re-seeding.
        revm::DatabaseCommit::commit(
            revm::context_interface::ContextTr::db_mut(&mut *self.evm),
            outcome.state,
        );

        let write_set_hash = ws.hash();
        let receipt = self.build_tx_receipt(TxReceiptInputs {
            outcome: &outcome.result,
            slot,
            inbound_envelope,
            nonce,
            to,
            signer,
            effective_gas_price,
            write_set_hash,
        })?;
        Ok((receipt, ws))
    }

    /// Record which touched accounts and slots this outcome only READ, for
    /// the footprint shadow scheduler (see [`TouchSet`]). A written
    /// account or slot is already implied by the `WriteSet`; only
    /// unwritten touches need naming here. A no-op when the caller passed
    /// no `touches` sink.
    fn capture_touches(state: &revm::state::EvmState, touches: Option<&mut TouchSet>) {
        let Some(t) = touches else {
            return;
        };
        for (addr, account) in state {
            Self::record_account_touch(t, *addr, account);
        }
    }

    /// Record one account's untouched-read marker, plus every one of its
    /// slots read but not written. The single loop in
    /// [`Self::capture_touches`] calls this once per account, so that
    /// function stays at one loop level.
    fn record_account_touch(t: &mut TouchSet, addr: Address, account: &revm::state::Account) {
        if !account.is_touched() {
            t.account_reads.push(addr);
        }
        for (key, slot) in &account.storage {
            if slot.original_value == slot.present_value {
                t.slot_reads
                    .push((addr, B256::from(key.to_be_bytes::<32>())));
            }
        }
    }

    /// Assemble one executed tx's receipt: the status/gas/logs the outcome
    /// carries, plus the identity and block-position fields
    /// `execute_tx_decoded` already resolved. `tx_hash` is copied straight
    /// from the inbound envelope, never recomputed with
    /// `keccak256(raw_tx)` — the proxy is the canonical hash producer.
    ///
    /// # Errors
    ///
    /// Returns [`ExecutorError::Execution`] if the running cumulative gas
    /// total would overflow `u64`. Exec-core enforces no cross-tx block
    /// gas cap, so the per-tx EIP-7825 cap times the tx count is the only
    /// bound; a checked add turns a hypothetical wrap into a defined
    /// error instead of a wrong receipt.
    fn build_tx_receipt(&self, inputs: TxReceiptInputs<'_>) -> Result<Receipt, ExecutorError> {
        let gas_used = inputs.outcome.gas().tx_gas_used();
        let (status, logs) = match inputs.outcome {
            ExecutionResult::Success { logs, .. } => (ReceiptStatus::Success, logs.clone()),
            ExecutionResult::Revert { .. } => (ReceiptStatus::Revert, Vec::new()),
            ExecutionResult::Halt { reason, .. } => {
                (ReceiptStatus::Halt(reason.clone()), Vec::new())
            }
        };
        let wire_logs = logs.iter().map(kardamom_types::WireLog::from).collect();
        let cumulative_gas_used = inputs
            .slot
            .cumulative_gas_used_before
            .checked_add(gas_used)
            .ok_or(ExecutorError::Execution {
                idx: inputs.slot.tx_idx,
                detail: alloc::format!(
                    "cumulative gas overflow: {} + {gas_used}",
                    inputs.slot.cumulative_gas_used_before
                ),
            })?;
        // Contract address is meaningful only for a successful CREATE tx.
        // A failed CREATE and any CALL tx have `contract_address = None`.
        let contract_address = if inputs.to.is_none() && status.is_success() {
            Some(inputs.signer.create(inputs.nonce))
        } else {
            None
        };
        Ok(Receipt {
            tx_idx: inputs.slot.tx_position,
            tx_hash: inputs.inbound_envelope.tx_hash,
            // EIP-2718 type, read from the raw envelope (legacy is 0x00).
            tx_type: kardamom_types::tx_type_of(&inputs.inbound_envelope.raw_tx),
            status: status.is_success(),
            gas_used,
            logs: wire_logs,
            write_set_hash: inputs.write_set_hash,
            nonce: inputs.nonce,
            from: inputs.signer,
            to: inputs.to,
            contract_address,
            effective_gas_price: inputs.effective_gas_price,
            block_number: self.env.block_number,
            transaction_index: inputs.slot.tx_index_in_block,
            cumulative_gas_used,
            skip_reason: None,
        })
    }
}

/// Everything [`Executor::build_tx_receipt`] needs beyond `self` and the
/// write-set hash: the outcome, and the identity/position fields
/// `execute_tx_decoded` already resolved before the outcome existed.
/// Every field is a reference or `Copy`, so this is too.
#[derive(Clone, Copy)]
struct TxReceiptInputs<'a> {
    outcome: &'a ExecutionResult,
    slot: TxSlot,
    inbound_envelope: &'a TxEnvelope,
    nonce: u64,
    to: Option<Address>,
    signer: Address,
    effective_gas_price: u128,
    write_set_hash: B256,
}

impl<S: StateDatabase> Executor<S> {
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
    /// `slot.tx_index_in_block` is the zero-based index within the
    /// in-flight block (it resets at every `BlockBoundaryStart`).
    /// `slot.cumulative_gas_used_before` is the running gas sum for txs
    /// already executed in the same block. The returned receipt's
    /// `cumulative_gas_used` equals this plus the new tx's `gas_used`.
    ///
    /// One-shot convenience method. It builds a throwaway executor for
    /// one call. It seeds the caller's live delta, then executes one tx.
    /// Use this for replay and tests. A hot path holds one [`Executor`]
    /// per block or batch instead, for the allocation win.
    ///
    /// # Errors
    ///
    /// Returns [`ExecutorError`] when the scope fails to construct, or
    /// when [`Self::execute_tx`] fails non-deterministically.
    pub fn execute_once(
        snapshot: &S,
        parent: Option<&PendingDelta>,
        delta: &PendingDelta,
        env: ExecEnv,
        slot: TxSlot,
        inbound_envelope: &TxEnvelope,
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
        scope.execute_tx(slot, inbound_envelope, bal, None)
    }
}

#[cfg(test)]
#[path = "scope_tests.rs"]
mod tests;

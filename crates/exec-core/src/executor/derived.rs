//! The shared execution context and receipt shape for both
//! derived-transaction kinds (deposits and cross-chain deliveries), on
//! both entry-point pairs (the fresh-cache reference path and the
//! block-scope path). A deposit and a 0x7D delivery run the same
//! mint/transact/commit/receipt sequence and build byte-identical
//! receipt fields except for identity (`tx_hash`, `tx_type`, `from`,
//! `to`); [`DerivedCall`] and [`DerivedCall::derived_receipt`] name that
//! shared shape once, instead of writing it out four times.

use alloy_primitives::{B256, U256};
use kardamom_types::Receipt;
use kardamom_types::{Deposit, WireLog};
use revm::context::TxEnv;
use revm::context::result::{EVMError, ExecResultAndState, ExecutionResult};
use revm::primitives::Log;
use revm::state::EvmState;
use revm::{Context, ExecuteEvm, MainBuilder, MainContext};

use alloc::format;
use alloc::vec::Vec;

use crate::block_env::ExecEnv;
use crate::error::ExecutorError;
use crate::exec_types::TxSlot;

use super::skip::{DerivedTxIdentity, Rejection, skip_reason_of_tx};

/// A revm transaction outcome, reduced to what a receipt needs. Both
/// derived-tx call shapes build one of these from the `&ExecutionResult`
/// [`DerivedCall::transact`] returns — never an engine error, since a
/// revert or halt on a derived tx is a receipt fact, not a failure. One
/// classification of the EVM result serves every derived path.
pub(super) struct CallOutcome {
    pub(super) status_success: bool,
    pub(super) gas_used: u64,
    pub(super) logs: Vec<Log>,
}

impl From<&ExecutionResult> for CallOutcome {
    fn from(result: &ExecutionResult) -> Self {
        let gas_used = result.gas().tx_gas_used();
        let (status_success, logs) = match result {
            ExecutionResult::Success { logs, .. } => (true, logs.clone()),
            ExecutionResult::Revert { .. } | ExecutionResult::Halt { .. } => (false, Vec::new()),
        };
        Self {
            status_success,
            gas_used,
            logs,
        }
    }
}

/// The result of [`DerivedCall::transact_or_skip`]'s inner call: it ran
/// (the caller inspects the outcome for revert/halt/success through
/// [`CallOutcome`]), or revm rejected it at validation before it ran.
pub(super) enum DerivedOutcome {
    Ran(ExecResultAndState<ExecutionResult, EvmState>),
    Rejected(Rejection),
}

/// The shared execution context for one derived transaction (a deposit
/// or a cross-chain delivery): its tx slot, the block env, and mutable
/// access to the cache it executes against. Both the fresh-cache
/// reference path (`deposit.rs`, `xchain.rs`) and the Executor's
/// block-scope path (`scope_derived.rs`) build one of these per derived
/// tx, so [`DerivedCall::credit_mint`], [`DerivedCall::transact`], and
/// [`DerivedCall::derived_receipt`] read `slot`/`env`/`cache` as state
/// instead of as loose parameters repeated at every call site.
/// Built fresh right before use and dropped after — the free-path
/// callers construct one and hold it for the whole derived-tx sequence;
/// the Executor path constructs a temporary per call, since `Executor`
/// cannot hold a borrow of its own `evm`'s db across other method calls.
pub(super) struct DerivedCall<'a, D> {
    pub(super) cache: &'a mut D,
    pub(super) env: &'a ExecEnv,
    pub(super) slot: TxSlot,
}

impl<'a, D> DerivedCall<'a, D>
where
    D: revm::Database + revm::DatabaseCommit,
    <D as revm::Database>::Error: core::fmt::Debug,
{
    pub(super) fn new(cache: &'a mut D, env: &'a ExecEnv, slot: TxSlot) -> Self {
        Self { cache, env, slot }
    }

    /// Credit `deposit.mint` to `deposit.from`, committed unconditionally.
    /// `deposit.mint` is a u128; this widens it to `U256` for balance
    /// arithmetic. The mint is durable no matter the inner-call outcome,
    /// so it is committed before the inner call runs. Generic over the
    /// cache type, so both the fresh-cache path (`execute_deposit_tx`)
    /// and the block-scope path (`Executor::execute_deposit`) share this
    /// one implementation.
    ///
    /// # Errors
    ///
    /// Returns [`ExecutorError::Execution`] when the account read fails,
    /// or when crediting the mint would overflow `U256`.
    pub(super) fn credit_mint(&mut self, deposit: &Deposit) -> Result<(), ExecutorError> {
        let mut info = revm::Database::basic(self.cache, deposit.from)
            .map_err(|e| ExecutorError::Execution {
                idx: self.slot.tx_idx,
                detail: format!("basic({:?}): {e:?}", deposit.from),
            })?
            .unwrap_or_default();
        info.balance = info
            .balance
            .checked_add(U256::from(deposit.mint))
            .ok_or_else(|| ExecutorError::Execution {
                idx: self.slot.tx_idx,
                detail: format!(
                    "mint overflow: account {:?} balance + mint {} would exceed U256::MAX",
                    deposit.from, deposit.mint
                ),
            })?;
        let mut acct = revm::state::Account::from(info);
        acct.mark_touch();
        // A single-entry commit, built with `once(...).collect()` instead
        // of a std `HashMap`. The exec core is `no_std` and must not
        // depend on `RandomState`.
        self.cache
            .commit(core::iter::once((deposit.from, acct)).collect());
        Ok(())
    }

    /// Run one fee-free, nonce-check-off inner call: build a fresh EVM
    /// over `self.cache` with the nonce check off, and execute. Both
    /// derived-tx call shapes (the free-cache reference path in
    /// `deposit.rs`/`xchain.rs`, and the Executor's block-scope path in
    /// `scope_derived.rs`) share this one hand-built EVM, instead of each
    /// building its own.
    ///
    /// The state is NOT committed here. A deposit's write-set capture
    /// reads the raw per-tx state before it is folded into the caller's
    /// cache, so the caller commits once it has read what it needs from
    /// the returned state.
    ///
    /// # Errors
    ///
    /// Returns the revm [`EVMError`]. [`EVMError::Transaction`] is
    /// deterministic input invalidity — every replica computes the same
    /// rejection from the same state and tx, so the caller turns it into
    /// a failed receipt through `skip_reason_of_tx` rather than an engine
    /// error. Every other variant is a database error or a block-env
    /// fault, not derivable from the record; the caller turns it into an
    /// [`ExecutorError`] through `derived_tx_rejection`. A revert or halt
    /// is not an error either way — it reaches [`CallOutcome`] as a
    /// failed receipt.
    pub(super) fn transact(
        &mut self,
        tx_env: TxEnv,
    ) -> Result<ExecResultAndState<ExecutionResult, EvmState>, EVMError<<D as revm::Database>::Error>>
    {
        let mut cfg = self.env.cfg_env();
        cfg.disable_nonce_check = true;
        let mut evm = Context::mainnet()
            .with_db(&mut *self.cache)
            .with_block(self.env.block_env())
            .with_cfg(cfg)
            .build_mainnet();
        evm.transact(tx_env)
    }

    /// [`Self::transact`], with revm's validation rejection turned into a
    /// [`Rejection`] instead of an error. All four derived-tx call sites
    /// (the free-cache reference path in `deposit.rs`/`xchain.rs`, and the
    /// Executor's block-scope path in `scope_derived.rs`, one each) share
    /// this one dispatch, instead of writing the three-way match out four
    /// times.
    ///
    /// # Errors
    ///
    /// Returns [`ExecutorError::Execution`] for a database or block-env
    /// fault, not derivable from the record. A revm validation rejection
    /// is not an error; it comes back as `Ok(DerivedOutcome::Rejected(_))`.
    pub(super) fn transact_or_skip(
        &mut self,
        tx_env: TxEnv,
    ) -> Result<DerivedOutcome, ExecutorError> {
        match self.transact(tx_env) {
            Ok(o) => Ok(DerivedOutcome::Ran(o)),
            // Deterministic input invalidity: every replica computes the
            // same rejection from the same state and tx.
            Err(EVMError::Transaction(e)) => Ok(DerivedOutcome::Rejected(Rejection {
                reason: skip_reason_of_tx(&e),
                detail: format!("{e:?}"),
            })),
            Err(e) => Err(self.derived_tx_rejection(&e)),
        }
    }

    /// Map a non-validation revm error on this derived tx to the engine
    /// error. The `EVMError::Transaction` arm is handled by
    /// [`Self::transact_or_skip`] as a [`Rejection`]. Everything else
    /// (database, header, custom) is local or is a block-env fault, not
    /// derivable from the record. It stays fatal, so crash recovery
    /// replays cleanly.
    fn derived_tx_rejection<DbErr: core::fmt::Debug>(
        &self,
        err: &EVMError<DbErr>,
    ) -> ExecutorError {
        ExecutorError::Execution {
            idx: self.slot.tx_idx,
            detail: format!("{err:?}"),
        }
    }

    /// Build a derived transaction's receipt. `identity.nonce` is 0 for
    /// both kinds — a deposit and a cross-chain delivery carry no real
    /// nonce, and consumers branch on `tx_type` instead. Both kinds pay
    /// no fee (`effective_gas_price: 0`); a deposit's mint and a
    /// delivery's quota gate are the value transfer, not a fee.
    ///
    /// # Errors
    ///
    /// Returns [`ExecutorError::Execution`] if the running cumulative
    /// gas total would overflow `u64`. Exec-core enforces no cross-tx
    /// block gas cap, so a checked add turns a hypothetical wrap into a
    /// defined error instead of a wrong receipt.
    pub(super) fn derived_receipt(
        &self,
        identity: DerivedTxIdentity,
        outcome: &CallOutcome,
        write_set_hash: B256,
    ) -> Result<Receipt, ExecutorError> {
        let wire_logs: Vec<WireLog> = outcome.logs.iter().map(WireLog::from).collect();
        let cumulative_gas_used = self
            .slot
            .cumulative_gas_used_before
            .checked_add(outcome.gas_used)
            .ok_or(ExecutorError::Execution {
                idx: self.slot.tx_idx,
                detail: format!(
                    "cumulative gas overflow: {} + {}",
                    self.slot.cumulative_gas_used_before, outcome.gas_used
                ),
            })?;
        Ok(Receipt {
            tx_idx: self.slot.tx_position,
            tx_hash: identity.tx_hash,
            tx_type: identity.tx_type,
            status: outcome.status_success,
            gas_used: outcome.gas_used,
            logs: wire_logs,
            write_set_hash,
            nonce: identity.nonce,
            from: identity.from,
            to: identity.to,
            contract_address: None,
            effective_gas_price: 0,
            block_number: self.env.block_number,
            transaction_index: self.slot.tx_index_in_block,
            cumulative_gas_used,
            skip_reason: None,
        })
    }
}

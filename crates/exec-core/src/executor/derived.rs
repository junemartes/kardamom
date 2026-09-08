//! The shared execution context and receipt shape for both
//! derived-transaction kinds (deposits and cross-chain deliveries), on
//! both entry-point pairs (the fresh-cache reference path and the
//! block-scope path). A deposit and a 0x7D delivery run the same
//! mint/transact/commit/receipt sequence and build byte-identical
//! receipt fields except for identity (`tx_hash`, `tx_type`, `from`,
//! `to`); [`DerivedCall`] and [`DerivedCall::derived_receipt`] name that
//! shared shape once, instead of writing it out four times.

use alloy_primitives::{Address, B256, U256};
use kardamom_types::Receipt;
use kardamom_types::{Deposit, WireLog};
use revm::context::TxEnv;
use revm::context::result::ExecutionResult;
use revm::primitives::Log;
use revm::{Context, ExecuteCommitEvm, MainBuilder, MainContext};

use alloc::format;
use alloc::vec::Vec;

use crate::block_env::ExecEnv;
use crate::error::ExecutorError;
use crate::exec_types::TxSlot;

/// A revm transaction outcome, reduced to what a receipt needs. Both
/// derived-tx call shapes (`transact_commit`, and `transact` with a
/// separate commit) produce one of these from their
/// `&ExecutionResult` — never an engine error, since a revert or halt on
/// a derived tx is a receipt fact, not a failure. This is the `match
/// &outcome.result { ... }` block that used to appear four times.
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

/// The one difference between a deposit's and a cross-chain delivery's
/// receipt: the canonical id (`source_hash`, either way — not a 2718
/// keccak) and the caller/callee pair. Both fillers (`nonce: 0`,
/// `effective_gas_price: 0`) are shared, so they live in
/// [`DerivedCall::derived_receipt`] itself, not here. Every field is
/// `Copy`, so this is too.
#[derive(Clone, Copy)]
pub(super) struct DerivedIdentity {
    pub(super) tx_hash: B256,
    pub(super) tx_type: u8,
    pub(super) from: Address,
    pub(super) to: Option<Address>,
}

/// The shared execution context for one derived transaction (a deposit
/// or a cross-chain delivery): its tx slot, the block env, and mutable
/// access to the cache it executes against. Both the fresh-cache
/// reference path (`deposit.rs`, `xchain.rs`) and the Executor's
/// block-scope path (`scope_derived.rs`) build one of these per derived
/// tx, so [`DerivedCall::credit_mint`], [`DerivedCall::transact_commit`],
/// and [`DerivedCall::derived_receipt`] read `slot`/`env`/`cache` as
/// state instead of as loose parameters repeated at every call site.
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

    /// Run one fee-free, nonce-check-off inner call and commit its
    /// state, in one step. A deposit's inner call and a cross-chain
    /// delivery's inner call ([`super::xchain::ValuelessMessage`]) share
    /// this exact shape — build a fresh EVM over `self.cache` with the
    /// nonce check off, `transact_commit`, and reduce the result to a
    /// [`CallOutcome`]. Free-path only (`deposit.rs`, `xchain.rs`): the
    /// Executor's block-scope path
    /// ([`super::scope_derived::Executor::execute_deposit`] and
    /// `execute_xchain`) toggles the block-scope EVM's own config
    /// instead, through `transact_without_nonce_check`, since it must
    /// reuse the block cache rather than build a fresh EVM per tx.
    ///
    /// # Errors
    ///
    /// Returns [`ExecutorError::Execution`] when the inner call fails
    /// non-deterministically or a database read fails. A revert or halt
    /// is not an error — it reaches [`CallOutcome`] as a failed receipt.
    pub(super) fn transact_commit(&mut self, tx_env: TxEnv) -> Result<CallOutcome, ExecutorError> {
        let mut cfg = self.env.cfg_env();
        cfg.disable_nonce_check = true;
        let mut evm = Context::mainnet()
            .with_db(&mut *self.cache)
            .with_block(self.env.block_env())
            .with_cfg(cfg)
            .build_mainnet();
        let result = evm
            .transact_commit(tx_env)
            .map_err(|e| ExecutorError::Execution {
                idx: self.slot.tx_idx,
                detail: format!("{e:?}"),
            })?;
        Ok(CallOutcome::from(&result))
    }

    /// Build a derived transaction's receipt. `nonce: 0` is never a real
    /// nonce for either kind — consumers branch on `tx_type`. Both kinds
    /// pay no fee (`effective_gas_price: 0`); a deposit's mint and a
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
        identity: DerivedIdentity,
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
            nonce: 0,
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

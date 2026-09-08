//! The two derived-transaction paths on `Executor`: deposits and
//! cross-chain deliveries. Both toggle the nonce check, transact,
//! commit, and build a fee-free receipt.

use kardamom_types::xchain;
use kardamom_types::{Receipt, StateDatabase};
use revm::ExecuteEvm;

use alloc::format;

use crate::delta::WriteSet;
use crate::error::ExecutorError;
use crate::exec_types::TxSlot;

use super::derived::{CallOutcome, DerivedCall, DerivedIdentity};
use super::scope::Executor;
use super::tx_env::{tx_env_from_deposit, tx_env_from_xchain};

impl<S: StateDatabase> Executor<S> {
    /// Execute a DEPOSIT on this block scope. Unlike `execute_deposit_tx`
    /// (kept as the equivalence reference, which rebuilds a fresh
    /// `CacheDB` and re-seeds parent + delta for every deposit), the
    /// scope reuses the block cache and only toggles the nonce check for
    /// the inner call (deposits carry no nonce).
    ///
    /// Artifact contract: receipt, `WriteSet` (called-contract code
    /// included, unchanged entries filtered out — see
    /// [`WriteSet::from_evm_state_deposit`]), and `write_set_hash` must
    /// match `execute_deposit_tx`'s output exactly. The
    /// `old_and_new_deposit_paths_agree` test in `deposit.rs` is the gate.
    ///
    /// Error paths fail-stop the pipeline (deposits have no skip
    /// semantics), so a mint committed before a failed inner call cannot
    /// leak into a later tx: nothing later runs.
    ///
    /// # Errors
    ///
    /// Returns [`ExecutorError::Execution`] when the mint overflows, the
    /// inner call fails non-deterministically, or a database read fails.
    pub fn execute_deposit(
        &mut self,
        slot: TxSlot,
        deposit: &kardamom_types::Deposit,
        bal: Option<(&mut revm::state::bal::Bal, u64)>,
    ) -> Result<(Receipt, WriteSet), ExecutorError> {
        // (1) Mint pre-credit, committed unconditionally — the mint is
        // durable regardless of inner-call outcome.
        DerivedCall::new(
            revm::context_interface::ContextTr::db_mut(&mut *self.evm),
            &self.env,
            slot,
        )
        .credit_mint(deposit)?;

        // (2) Inner call with the nonce check off. The toggle-restore pair
        // has NO fallible call between toggle and restore: an early return
        // with the toggle still set would run every later tx in the block
        // without nonce validation.
        let tx_env = tx_env_from_deposit(deposit);
        let outcome = self.transact_without_nonce_check(slot.tx_idx, tx_env)?;
        let call_outcome = CallOutcome::from(&outcome.result);

        // (3) The deposit artifact keeps read slots (see the extractor
        // doc). Capture before the commit consumes the state.
        let mut ws = WriteSet::from_evm_state_deposit(&outcome.state);
        revm::DatabaseCommit::commit(
            revm::context_interface::ContextTr::db_mut(&mut *self.evm),
            outcome.state,
        );
        self.ensure_sender_in_write_set(&mut ws, slot.tx_idx, deposit.from)?;
        if let Some((bal, bal_index)) = bal {
            ws.record_into_bal(bal, bal_index);
        }

        let receipt = DerivedCall::new(
            revm::context_interface::ContextTr::db_mut(&mut *self.evm),
            &self.env,
            slot,
        )
        .derived_receipt(
            DerivedIdentity {
                tx_hash: deposit.source_hash,
                tx_type: kardamom_types::TX_TYPE_DEPOSIT,
                from: deposit.from,
                to: deposit.to,
            },
            &call_outcome,
            ws.hash(),
        )?;
        Ok((receipt, ws))
    }

    /// Toggle the nonce check off, run the inner call, and restore the
    /// check. No fallible call sits between the toggle and its restore:
    /// an early return with the toggle still set would run every later
    /// tx in the block without nonce validation. Deposits and
    /// cross-chain deliveries share this rule.
    fn transact_without_nonce_check(
        &mut self,
        tx_idx: crate::exec_types::TxIndex,
        tx_env: revm::context::TxEnv,
    ) -> Result<
        revm::context::result::ExecResultAndState<
            revm::context::result::ExecutionResult,
            revm::state::EvmState,
        >,
        ExecutorError,
    > {
        (*self.evm).modify_cfg(|c| c.disable_nonce_check = true);
        let result = self.evm.transact(tx_env);
        (*self.evm).modify_cfg(|c| c.disable_nonce_check = false);
        result.map_err(|e| ExecutorError::Execution {
            idx: tx_idx,
            detail: format!("{e:?}"),
        })
    }

    /// The sender always carries the mint in the artifact, even when the
    /// inner call never touched it.
    fn ensure_sender_in_write_set(
        &mut self,
        ws: &mut WriteSet,
        tx_idx: crate::exec_types::TxIndex,
        sender: alloy_primitives::Address,
    ) -> Result<(), ExecutorError> {
        if !ws.accounts.iter().any(|(a, _)| *a == sender) {
            let cache = revm::context_interface::ContextTr::db_mut(&mut *self.evm);
            let info = revm::Database::basic(cache, sender)
                .map_err(|e| ExecutorError::Execution {
                    idx: tx_idx,
                    detail: format!("basic({sender:?}): {e:?}"),
                })?
                .unwrap_or_default();
            ws.accounts
                .push((sender, (info.nonce, info.balance, info.code_hash)));
            ws.finish();
        }
        Ok(())
    }

    /// Execute one cross-chain delivery (a 0x7D message) on this block
    /// scope. Mirrors [`Self::execute_deposit`]: the block cache is
    /// reused, the call is fee-free with the nonce check off, and the
    /// receipt's canonical id is the message's `source_hash`, not a 2718
    /// keccak.
    ///
    /// Artifact contract: receipt, `WriteSet` (called-contract code
    /// included, unchanged entries filtered out — see
    /// [`WriteSet::from_evm_state_deposit`]), and `write_set_hash` must
    /// match `execute_xchain_tx`'s output exactly. A gate test like
    /// `old_and_new_deposit_paths_agree` applies here too.
    ///
    /// v1 carries no value. A nonzero `message.value` is a chain fault,
    /// not a droppable message, so this fails the engine before it
    /// touches the cache.
    ///
    /// # Errors
    ///
    /// Returns [`ExecutorError::Execution`] when `message.value` is
    /// nonzero, the inner call fails non-deterministically, or a
    /// database read fails.
    pub fn execute_xchain(
        &mut self,
        slot: TxSlot,
        delivery: super::xchain::XChainDelivery<'_>,
        bal: Option<(&mut revm::state::bal::Bal, u64)>,
    ) -> Result<(Receipt, WriteSet), ExecutorError> {
        let origin_chain_id = delivery.origin_chain_id;
        let message =
            super::xchain::ValuelessMessage::new(slot.tx_idx, origin_chain_id, delivery.message)?;

        // Inner call with the nonce check off. No fallible call sits
        // between the toggle and its restore — deposits and cross-chain
        // deliveries share this rule; see the note on `execute_deposit`.
        let tx_env = tx_env_from_xchain(origin_chain_id, &message);
        let outcome = self.transact_without_nonce_check(slot.tx_idx, tx_env)?;
        // A revert inside deliver (or a bubbled-up inner-call revert)
        // marks the receipt failed, but it is not an engine error. This
        // is the deposit posture.
        let call_outcome = CallOutcome::from(&outcome.result);

        let ws = WriteSet::from_evm_state_deposit(&outcome.state);
        revm::DatabaseCommit::commit(
            revm::context_interface::ContextTr::db_mut(&mut *self.evm),
            outcome.state,
        );
        if let Some((bal, bal_index)) = bal {
            ws.record_into_bal(bal, bal_index);
        }

        let sender = xchain::xchain_tx_sender(origin_chain_id);
        let receipt = DerivedCall::new(
            revm::context_interface::ContextTr::db_mut(&mut *self.evm),
            &self.env,
            slot,
        )
        .derived_receipt(
            DerivedIdentity {
                tx_hash: message.source_hash,
                tx_type: kardamom_types::TX_TYPE_XCHAIN,
                from: sender,
                to: Some(xchain::INBOX),
            },
            &call_outcome,
            ws.hash(),
        )?;
        Ok((receipt, ws))
    }
}

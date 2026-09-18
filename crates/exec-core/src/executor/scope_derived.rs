//! The two derived-transaction paths on `Executor`: deposits and
//! cross-chain deliveries. Both toggle the nonce check, transact,
//! commit, and build a fee-free receipt.

use kardamom_types::xchain;
use kardamom_types::{Receipt, StateDatabase};

use alloc::format;

use revm::database::CacheDB;

use crate::delta::WriteSet;
use crate::error::ExecutorError;
use crate::exec_types::TxSlot;

use super::db::SnapshotDb;
use super::deposit::DepositFailure;
use super::derived::{CallOutcome, DerivedCall, DerivedOutcome};
use super::scope::Executor;
use super::skip::{DerivedTxIdentity, Rejection, SkipContext};
use super::tx_env::{tx_env_from_deposit, tx_env_from_xchain};

impl<S: StateDatabase> Executor<S> {
    /// Build a [`DerivedCall`] over this scope's block cache, for one
    /// derived tx. Every entry point on this `impl` needs one, and none
    /// can hold it across another `&mut self` call, so this stays a
    /// per-call builder instead of a cached field.
    fn derived_call(&mut self, slot: TxSlot) -> DerivedCall<'_, CacheDB<SnapshotDb<S>>> {
        DerivedCall::new(
            revm::context_interface::ContextTr::db_mut(&mut *self.evm),
            &self.env,
            slot,
        )
    }

    /// Execute a DEPOSIT on this block scope. Unlike `execute_deposit_tx`
    /// (kept as the equivalence reference, which rebuilds a fresh
    /// `CacheDB` and re-seeds parent + delta for every deposit), the
    /// scope reuses the block cache and only builds a throwaway EVM,
    /// through [`DerivedCall::transact`], for the inner call.
    ///
    /// Artifact contract: receipt, `WriteSet` (called-contract code
    /// included, unchanged entries filtered out — see
    /// [`WriteSet::from_evm_state_deposit`]), and `write_set_hash` must
    /// match `execute_deposit_tx`'s output exactly. The
    /// `old_and_new_deposit_paths_agree` test in `deposit.rs` is the gate.
    ///
    /// A deposit that revm rejects at validation (for example an L1
    /// `gasLimit` below the intrinsic cost) becomes a deterministic failed
    /// receipt (`failed_deposit_receipt`): the mint stays, the inner call
    /// never runs. Local failures (database errors) fail-stop the pipeline,
    /// so a mint committed before such a failure cannot leak into a later
    /// tx: nothing later runs.
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
        self.derived_call(slot).credit_mint(deposit)?;

        // (2) Inner call with the nonce check off.
        let tx_env = tx_env_from_deposit(deposit);
        let outcome = match self.derived_call(slot).transact_or_skip(tx_env)? {
            DerivedOutcome::Ran(o) => o,
            // Deterministic input invalidity: the mint stays, the inner
            // call never ran. Same artifact as the free function.
            DerivedOutcome::Rejected(Rejection { reason, detail }) => {
                let cache = revm::context_interface::ContextTr::db_mut(&mut *self.evm);
                return DepositFailure::from_validation::<S, _>(
                    cache,
                    deposit,
                    slot,
                    self.env.block_number,
                    reason,
                    &detail,
                    bal,
                );
            }
        };
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

        let receipt = self.derived_call(slot).derived_receipt(
            DerivedTxIdentity {
                tx_hash: deposit.source_hash,
                tx_type: kardamom_types::TX_TYPE_DEPOSIT,
                from: deposit.from,
                to: deposit.to,
                nonce: 0,
            },
            &call_outcome,
            ws.hash(),
        )?;
        Ok((receipt, ws))
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
            ws.accounts.push((
                sender,
                crate::delta::AccountFields {
                    nonce: info.nonce,
                    balance: info.balance,
                    code_hash: info.code_hash,
                },
            ));
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
    /// v1 carries no value. A nonzero `message.value`, or a message revm
    /// rejects at validation (for example a `gas_limit` above the
    /// EIP-7825 cap), becomes a deterministic failed receipt
    /// (`failed_xchain_receipt`) before it touches the cache. Only local
    /// failures (database errors) are engine errors.
    ///
    /// # Errors
    ///
    /// Returns [`ExecutorError::Execution`] when the inner call fails
    /// non-deterministically or a database read fails.
    pub fn execute_xchain(
        &mut self,
        slot: TxSlot,
        delivery: super::xchain::XChainDelivery<'_>,
        bal: Option<(&mut revm::state::bal::Bal, u64)>,
    ) -> Result<(Receipt, WriteSet), ExecutorError> {
        let origin_chain_id = delivery.origin_chain_id;
        if let Err(Rejection { reason, detail }) =
            Executor::<S>::xchain_value_rejection(origin_chain_id, delivery.message)
        {
            return Ok(Executor::<S>::failed_xchain_receipt(
                reason,
                &detail,
                SkipContext::new(slot, self.env.block_number),
                origin_chain_id,
                delivery.message,
            ));
        }
        let message = super::xchain::ValuelessMessage::new(delivery.message);

        // Inner call with the nonce check off.
        let tx_env = tx_env_from_xchain(origin_chain_id, &message);
        let outcome = match self.derived_call(slot).transact_or_skip(tx_env)? {
            DerivedOutcome::Ran(o) => o,
            DerivedOutcome::Rejected(Rejection { reason, detail }) => {
                return Ok(Executor::<S>::failed_xchain_receipt(
                    reason,
                    &detail,
                    SkipContext::new(slot, self.env.block_number),
                    origin_chain_id,
                    &message,
                ));
            }
        };
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
        let receipt = self.derived_call(slot).derived_receipt(
            DerivedTxIdentity {
                tx_hash: message.source_hash,
                tx_type: kardamom_types::TX_TYPE_XCHAIN,
                from: sender,
                to: Some(xchain::INBOX),
                nonce: 0,
            },
            &call_outcome,
            ws.hash(),
        )?;
        Ok((receipt, ws))
    }
}

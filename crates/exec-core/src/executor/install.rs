//! The install of the `KardamomChainState` code at [`CHAIN_STATE`].
//!
//! A `setFeature` system deposit calls [`CHAIN_STATE`]. On a genesis
//! without the contract, that address has no code, and the call writes no
//! flag word. So before such a deposit calls, the deposit path puts the
//! canonical code ([`CHAIN_STATE_CODE`]) at [`CHAIN_STATE`] when the address
//! has no code. The nonce, the balance and the storage of the account do
//! not change. On a chain that has the code, the install does nothing.
//!
//! The install is a write of its deposit, as the mint is. It goes into the
//! cache before the call, so a revert of the call keeps it. The write set
//! of the deposit holds the account and the code whatever the call does,
//! so the delta, the BAL claims and the witness see the install. Every
//! block driver runs deposits through this path: the live engine, the
//! Block-STM strategy (deposits run there as serial singletons), the
//! validator, replay and the zk guest.

use alloy_primitives::{B256, keccak256};
use kardamom_types::Deposit;
use kardamom_types::upgrades::{CHAIN_STATE, CHAIN_STATE_CODE, FeatureSchedule, SYSTEM_UPGRADER};
use revm::primitives::KECCAK_EMPTY;
use revm::state::{AccountInfo, Bytecode};

use crate::delta::WriteSet;
use crate::error::ExecutorError;

use super::derived::DerivedCall;

/// The install of the canonical code at [`CHAIN_STATE`] that one deposit
/// did before its call.
pub(super) struct ChainStateInstall {
    code_hash: B256,
}

impl ChainStateInstall {
    /// Add the account at [`CHAIN_STATE`] and the canonical code to `ws`
    /// when the writes of the call lack them. The fields of the account
    /// come from the cache of `call`, after the call.
    ///
    /// # Errors
    ///
    /// Returns [`ExecutorError::Execution`] when the account read fails.
    pub(super) fn add_to<D>(
        &self,
        ws: &mut WriteSet,
        call: &mut DerivedCall<'_, D>,
    ) -> Result<(), ExecutorError>
    where
        D: revm::Database + revm::DatabaseCommit,
        <D as revm::Database>::Error: core::fmt::Debug,
    {
        let info = call.account(CHAIN_STATE)?;
        ws.ensure_account(CHAIN_STATE, &info);
        ws.ensure_code(self.code_hash, bytes::Bytes::from_static(CHAIN_STATE_CODE));
        Ok(())
    }

    /// `true` for a `setFeature` system deposit of an upgrade transaction:
    /// from [`SYSTEM_UPGRADER`], to [`CHAIN_STATE`], with `setFeature`
    /// calldata. Only these deposits install the code.
    fn is_upgrade(deposit: &Deposit) -> bool {
        deposit.from == SYSTEM_UPGRADER && FeatureSchedule::of_deposit(deposit).is_some()
    }
}

impl<D> DerivedCall<'_, D>
where
    D: revm::Database + revm::DatabaseCommit,
    <D as revm::Database>::Error: core::fmt::Debug,
{
    /// Install the canonical code at [`CHAIN_STATE`] before `deposit`
    /// calls, when `deposit` is a `setFeature` system deposit and the
    /// address has no code. See the module doc for the rule.
    ///
    /// # Errors
    ///
    /// Returns [`ExecutorError::Execution`] when the account read fails.
    pub(super) fn install_chain_state(
        &mut self,
        deposit: &Deposit,
    ) -> Result<Option<ChainStateInstall>, ExecutorError> {
        if !ChainStateInstall::is_upgrade(deposit) {
            return Ok(None);
        }
        let info = self.account(CHAIN_STATE)?;
        if crate::code_hash::to_revm_code_hash(info.code_hash) != KECCAK_EMPTY {
            return Ok(None);
        }
        let code_hash = keccak256(CHAIN_STATE_CODE);
        self.commit_account(
            CHAIN_STATE,
            AccountInfo {
                code_hash,
                code: Some(Bytecode::new_raw(alloy_primitives::Bytes::from_static(
                    CHAIN_STATE_CODE,
                ))),
                ..info
            },
        );
        Ok(Some(ChainStateInstall { code_hash }))
    }
}

#[cfg(test)]
#[path = "install_tests.rs"]
mod tests;

//! The nonce burn of a skipped transaction.
//!
//! With [`FEATURE_SEALER_V3`] active, a skip of a transaction at nonce `n`
//! of a sender whose chain nonce is `n` sets that nonce to `n + 1`. The
//! skip charges no fee, runs no code, and changes nothing else. So the
//! chain nonce of the sender follows the contiguity guard of the sealer,
//! which counts every ordered entry, skipped or not.
//!
//! The feature check reads the activation word through the same database
//! as the transaction, and compares it with the timestamp that the block
//! executes with. This is what `KardamomChainState.isActive` answers for
//! a transaction at that point of the block. Every block driver reads
//! through its own view (the block cache, the multi-version view of a
//! worker, or a witness), so every driver decides alike.

use alloy_primitives::{Address, U256};
use kardamom_types::SkipReason;
use kardamom_types::upgrades::{CHAIN_STATE, FEATURE_SEALER_V3};
use revm::state::{Account, EvmState};

use alloc::format;

use crate::error::ExecutorError;
use crate::features::{activation_slot, is_active};

/// A skipped transaction that can burn its nonce: a decoded transaction
/// of `sender` at `nonce`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct NonceBurn {
    pub sender: Address,
    pub nonce: u64,
}

impl NonceBurn {
    /// The burn candidate of a skip for `reason`. A nonce skip never
    /// burns: the nonce of the transaction is not the chain nonce.
    /// Undecodable bytes carry no nonce.
    #[must_use]
    pub fn of_skip(reason: SkipReason, sender: Address, nonce: u64) -> Option<Self> {
        match reason {
            SkipReason::Undecodable | SkipReason::NonceTooLow | SkipReason::NonceTooHigh => None,
            _ => Some(Self { sender, nonce }),
        }
    }

    /// The state change of the burn, read through `db` at the block
    /// timestamp `timestamp_ms`: the sender's account with the nonce one
    /// higher, marked touched. `None` when the feature is not active, when
    /// the chain nonce of the sender is not the nonce of the transaction,
    /// or when the nonce is `u64::MAX` (EIP-2681: no next nonce).
    ///
    /// # Errors
    ///
    /// Returns [`ExecutorError::State`] when a read of `db` fails.
    pub fn state<DB: revm::Database>(
        self,
        db: &mut DB,
        timestamp_ms: u64,
    ) -> Result<Option<EvmState>, ExecutorError>
    where
        DB::Error: core::fmt::Debug,
    {
        let slot = U256::from_be_bytes(activation_slot(FEATURE_SEALER_V3).0);
        let activation = db.storage(CHAIN_STATE, slot).map_err(Self::read_error)?;
        if !is_active(activation, timestamp_ms) {
            return Ok(None);
        }
        let info = db
            .basic(self.sender)
            .map_err(Self::read_error)?
            .unwrap_or_default();
        let Some(next) = self
            .nonce
            .checked_add(1)
            .filter(|_| info.nonce == self.nonce)
        else {
            return Ok(None);
        };
        let mut account = Account::from(info);
        account.info.nonce = next;
        account.mark_touch();
        Ok(Some(EvmState::from_iter([(self.sender, account)])))
    }

    fn read_error<E: core::fmt::Debug>(e: E) -> ExecutorError {
        ExecutorError::State(format!("nonce burn read: {e:?}"))
    }
}

#[cfg(test)]
#[path = "burn_tests.rs"]
mod tests;

//! The nonce burn of a skipped transaction.
//!
//! With [`FEATURE_SEALER_V3`] active, a skip of a transaction at nonce `n`
//! of a sender whose chain nonce is `n` sets that nonce to `n + 1`. The
//! skip charges no fee, runs no code, and changes nothing else. So the
//! chain nonce of the sender follows the contiguity guard of the sealer,
//! which counts every ordered entry, skipped or not.
//!
//! [`FEATURE_SEALER_V3`]: kardamom_types::upgrades::FEATURE_SEALER_V3
//!
//! The feature check reads the flags of the block ([`BlockFeatures`]),
//! which the block driver froze before the first record of the block. So
//! every transaction of a block decides alike, and a `setFeature` deposit
//! of the block changes nothing before the next block. The burn reads only
//! the account of the sender, through the view of the transaction (the
//! block cache, the multi-version view of a worker, or a witness).

use alloy_primitives::Address;
use revm::state::{Account, EvmState};

use alloc::format;

use crate::error::ExecutorError;
use crate::features::BlockFeatures;

/// A skipped transaction whose rejection can use its nonce (see
/// [`super::SkippedRecord::rejected`]): a decoded transaction of `sender`
/// at `nonce`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct NonceBurn {
    pub(super) sender: Address,
    pub(super) nonce: u64,
}

impl NonceBurn {
    /// The state change of the burn in a block with the flags `features`,
    /// read through `db`: the sender's account with the nonce one higher,
    /// marked touched. `None` when the feature is not active in the block,
    /// when the chain nonce of the sender is not the nonce of the
    /// transaction, or when the nonce is `u64::MAX` (EIP-2681: no next
    /// nonce).
    ///
    /// # Errors
    ///
    /// Returns [`ExecutorError::State`] when a read of `db` fails.
    pub(super) fn state<DB: revm::Database>(
        self,
        db: &mut DB,
        features: BlockFeatures,
    ) -> Result<Option<EvmState>, ExecutorError>
    where
        DB::Error: core::fmt::Debug,
    {
        if !features.sealer_v3 {
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

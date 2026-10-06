//! The L1 epochs a rebuild puts at the head of its blocks.
//!
//! A deposit is not in the DA payload. L1 fixes the deposits of an epoch
//! and their order, and the block's L1 origin, which the payload carries,
//! fixes the L2 block an epoch leads. When the origin moves from `M` to
//! `N`, the block leads with the epochs `M + 1 ..= N`. Each epoch comes from
//! the lockbox logs of its L1 block through [`derive_epoch`], the rule the
//! producer and the validator use.
//!
//! The first step from origin 0 takes epoch `N` only. The producer starts
//! at the finalized L1 block it first sees, so the chain holds no epoch
//! before that block. A block whose payload has no origin (a version 2
//! payload) leads with no epoch and leaves the origin where it was.

use std::cmp::Ordering;
use std::ops::RangeInclusive;

use alloy_primitives::Address;
use futures::{StreamExt, TryStreamExt, stream};
use kardamom_da_watcher::L1Source;
use kardamom_engine::ReplayBlock;
use kardamom_types::EpochRecord;
use kardamom_types::epoch::derive_epoch;

use crate::ReconstructError;

/// Where a rebuild reads the L1 epochs: an L1 source, and the lockbox
/// whose logs hold the deposits.
pub struct L1Epochs<S> {
    source: S,
    lockbox: Address,
}

impl<S: L1Source> L1Epochs<S> {
    #[must_use]
    pub const fn new(source: S, lockbox: Address) -> Self {
        Self { source, lockbox }
    }

    /// Give each block the L1 epochs it leads with. This costs two L1
    /// reads per epoch: the block hash, and the lockbox logs.
    ///
    /// # Errors
    ///
    /// Returns an error when a block's origin is below the origin before
    /// it, when an L1 read fails, or when an L1 block's logs do not derive
    /// an epoch.
    pub async fn attach(
        &self,
        blocks: Vec<ReplayBlock>,
    ) -> Result<Vec<ReplayBlock>, ReconstructError> {
        let mut origin = OriginSteps::default();
        let steps = blocks
            .iter()
            .map(|block| origin.step(block))
            .collect::<Result<Vec<_>, _>>()?;
        stream::iter(blocks.into_iter().zip(steps))
            .then(|(block, epochs)| self.lead(block, epochs))
            .try_collect()
            .await
    }

    /// `block`, led by the epochs `numbers`, or by none.
    async fn lead(
        &self,
        block: ReplayBlock,
        numbers: Option<RangeInclusive<u64>>,
    ) -> Result<ReplayBlock, ReconstructError> {
        let l1_epochs = stream::iter(numbers.into_iter().flatten())
            .then(|number| self.derive(number))
            .try_collect()
            .await?;
        Ok(ReplayBlock { l1_epochs, ..block })
    }

    /// The epoch of L1 block `number`, derived from its lockbox logs.
    async fn derive(&self, number: u64) -> Result<EpochRecord, ReconstructError> {
        let (hash, _parent) = self
            .source
            .block_ids(number)
            .await
            .map_err(|e| ReconstructError(format!("read L1 block {number}: {e}")))?;
        let logs = self
            .source
            .lockbox_logs(self.lockbox, number, number)
            .await
            .map_err(|e| {
                ReconstructError(format!("read the lockbox logs of L1 block {number}: {e}"))
            })?;
        derive_epoch(number, hash, &logs)
            .map_err(|e| ReconstructError(format!("derive the epoch of L1 block {number}: {e}")))
    }
}

/// The L1 origin of the last block seen, as a rebuild walks its blocks.
#[derive(Default)]
struct OriginSteps {
    origin: u64,
}

impl OriginSteps {
    /// The numbers of the epochs `block` leads with. `None` when its
    /// origin did not move or its payload carries none.
    fn step(
        &mut self,
        block: &ReplayBlock,
    ) -> Result<Option<RangeInclusive<u64>>, ReconstructError> {
        let Some(end) = block.canonical_end else {
            return Ok(None);
        };
        let to = end.l1_origin;
        let numbers = match to.cmp(&self.origin) {
            Ordering::Less => {
                return Err(ReconstructError(format!(
                    "block {}: L1 origin {to} is below the origin {} of the block before it",
                    block.block_number, self.origin
                )));
            }
            Ordering::Equal => None,
            Ordering::Greater if self.origin == 0 => Some(to..=to),
            Ordering::Greater => {
                let first = self.origin.checked_add(1).ok_or_else(|| {
                    ReconstructError(format!("block {}: L1 origin overflows", block.block_number))
                })?;
                Some(first..=to)
            }
        };
        self.origin = to;
        Ok(numbers)
    }
}

#[cfg(test)]
#[path = "epochs_tests.rs"]
mod tests;

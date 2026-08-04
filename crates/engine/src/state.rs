//! Engine-side test fixtures that couple `kardamom-exec-core`'s
//! `MockStateDatabase` to the actor's commit seam.
//!
//! The mock itself (plus `StaticSnapshotSource` / `MutatingSnapshotSource`)
//! lives in `kardamom-exec-core::state` — it only depends on the
//! `kardamom-types` traits. `WriterApplyingQueue` stays HERE because it
//! implements [`StateWriterQueue`], an actor seam that has no place in the
//! `no_std` core.

// Re-exported so pre-split `crate::state::…` / `kardamom_engine::state::…`
// paths keep resolving.
pub use kardamom_exec_core::state::{
    MockStateDatabase, MockStateError, MutatingSnapshotSource, StaticSnapshotSource,
};
use kardamom_types::{BlockBoundary, BlockDelta};

use crate::actor::StateWriterQueue;
use crate::error::ExecutorError;

/// State-writer queue that applies each submitted `BlockDelta` directly to
/// the shared `MockStateDatabase`. Pair with `MutatingSnapshotSource` so the
/// exec thread observes block N's writes when it opens its snapshot for
/// block N+1.
#[derive(Debug, Clone)]
pub struct WriterApplyingQueue {
    db: MockStateDatabase,
}

impl WriterApplyingQueue {
    pub fn new(db: MockStateDatabase) -> Self {
        Self { db }
    }
}

impl StateWriterQueue for WriterApplyingQueue {
    fn submit(&mut self, _block: BlockBoundary, delta: BlockDelta) -> Result<(), ExecutorError> {
        self.db.apply_block_delta(&delta);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloy_primitives::{Address, B256, U256};
    use kardamom_exec_core::state::MutatingSnapshotSource;
    use kardamom_types::{AccountChange, BPosition, SnapshotSource, StateDatabase};

    #[test]
    fn writer_applies_account_changes_and_snapshot_sees_them() {
        let addr = Address::from([1u8; 20]);
        let db = MockStateDatabase::builder()
            .account(addr, U256::from(1u64), 0, B256::ZERO)
            .build();
        let mut q = WriterApplyingQueue::new(db.clone());
        let src = MutatingSnapshotSource(db);

        let delta = BlockDelta {
            block_number: 1,
            accounts: vec![AccountChange {
                address: addr,
                nonce: 5,
                balance: U256::from(999u64),
                code_hash: B256::ZERO,
            }],
            storage: Vec::new(),
            code: Vec::new(),
            receipts: Vec::new(),
        };
        let boundary = BlockBoundary {
            block_number: 1,
            end_tx_idx: BPosition::ZERO,
            l2_timestamp: 0,
            l1_origin: 0,
        };
        q.submit(boundary, delta).unwrap();

        let snap = src.snapshot_after(1);
        let (nonce, balance, _) = snap.basic(addr).unwrap().unwrap();
        assert_eq!(nonce, 5);
        assert_eq!(balance, U256::from(999u64));
    }
}

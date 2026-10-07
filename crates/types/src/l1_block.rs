//! The `l1_blocks` stream: one record per finalized L1 block, as the L1
//! follower read it.
//!
//! The follower is the one service that reads L1 data. A record on the
//! stream is a block that two L1 sources agreed on, that descends from the
//! record before it, and whose finality step ends at the light client's
//! finalized header where a light client runs. The consumers trust the
//! record and check only its parent link against their own cursor.
//!
//! Two follower instances publish the same blocks. A consumer takes the
//! first record of each block number and drops a second record with the
//! same hash. Two records of one number with different hashes mean that
//! one instance read a lie its cross-check did not catch: the consumer
//! halts. [`L1BlockDedup`] holds this rule.

use alloc::collections::BTreeMap;
use alloc::vec::Vec;

use alloy_primitives::{B256, Bytes};
use rkyv::{Archive, Deserialize, Serialize};

use crate::epoch::EpochRecord;
use crate::wire;

/// One posted batch: the fields of the settlement's `BatchPosted` event,
/// and where on L1 it was posted.
#[derive(
    Clone,
    Debug,
    PartialEq,
    Eq,
    Archive,
    Serialize,
    Deserialize,
    serde::Serialize,
    serde::Deserialize,
)]
#[rkyv(derive(Debug))]
pub struct BatchEntry {
    pub index: u64,
    /// The EigenDA certificate of the batch's payload, as posted.
    #[rkyv(with = wire::PrimitiveBytesVec)]
    pub da_cert: Bytes,
    pub l2_block_start: u64,
    pub l2_block_end: u64,
    #[rkyv(with = wire::B256Bytes)]
    pub records_commitment: B256,
    /// The L1 block that carried the `postBatch` transaction.
    pub l1_block: u64,
    #[rkyv(with = wire::B256Bytes)]
    pub l1_tx: B256,
}

/// One finalized L1 block on the `l1_blocks` stream.
#[derive(Clone, Debug, Default, PartialEq, Eq, Archive, Serialize, Deserialize)]
#[rkyv(derive(Debug))]
pub struct L1Block {
    pub number: u64,
    #[rkyv(with = wire::B256Bytes)]
    pub hash: B256,
    #[rkyv(with = wire::B256Bytes)]
    pub parent_hash: B256,
    /// The block's timestamp, in seconds since the Unix epoch.
    pub timestamp: u64,
    /// The block's epoch: the `derive_epoch` output, with its deposits.
    /// The follower's epoch store holds the same bytes.
    pub epoch: EpochRecord,
    /// The `BatchPosted` events of this block, in log order.
    pub batches: Vec<BatchEntry>,
}

/// How many block numbers a consumer keeps the first hash of. Two
/// follower instances run within a few finality steps of each other (one
/// step is 32 blocks), so a second record always arrives inside this
/// horizon.
pub const DEDUP_HORIZON: u64 = 8192;

/// What a consumer does with one record.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Admit {
    /// The next block after the consumer's head, and it descends from the
    /// head: the consumer takes it.
    Next,
    /// A block the consumer already took, with the same hash, or a block
    /// at or below its start: the consumer drops it.
    Duplicate,
    /// A block past the next one: the records between are missing on
    /// this subscription. The consumer reads them from the archive.
    Ahead { expected: u64 },
    /// The next block does not descend from the head. The follower checks
    /// the chain before it publishes, so a record that breaks it comes
    /// from a follower that lies.
    ParentMismatch {
        number: u64,
        parent: B256,
        expected: B256,
    },
    /// Two records of one block number with different hashes: the halt
    /// `l1_follower_disagreement`.
    Disagreement {
        number: u64,
        first: B256,
        second: B256,
    },
}

/// The consumer's side of the two instances: its head and the first hash
/// of every block number inside [`DEDUP_HORIZON`] below the head.
#[derive(Debug, Clone)]
pub struct L1BlockDedup {
    head: u64,
    head_hash: B256,
    seen: BTreeMap<u64, B256>,
}

impl L1BlockDedup {
    /// A dedup whose head is block `number` with `hash`: the consumer's
    /// cursor. The next record it takes is block `number + 1`.
    #[must_use]
    pub fn after(number: u64, hash: B256) -> Self {
        Self {
            head: number,
            head_hash: hash,
            seen: BTreeMap::from([(number, hash)]),
        }
    }

    /// The head: the last block taken.
    #[must_use]
    pub fn head(&self) -> u64 {
        self.head
    }

    /// The hash of the head.
    #[must_use]
    pub fn head_hash(&self) -> B256 {
        self.head_hash
    }

    /// What the consumer does with one record. It moves nothing: the
    /// consumer calls [`Self::take`] once it handled a [`Admit::Next`]
    /// record, so a record it could not handle stays the next one.
    #[must_use]
    pub fn admit(&self, record: &L1Block) -> Admit {
        if let Some(&first) = self.seen.get(&record.number) {
            return if first == record.hash {
                Admit::Duplicate
            } else {
                Admit::Disagreement {
                    number: record.number,
                    first,
                    second: record.hash,
                }
            };
        }
        if record.number <= self.head {
            return Admit::Duplicate;
        }
        // The head is below `record.number`, so it is below `u64::MAX`.
        let expected = self.head.saturating_add(1);
        if record.number > expected {
            return Admit::Ahead { expected };
        }
        if record.parent_hash != self.head_hash {
            return Admit::ParentMismatch {
                number: record.number,
                parent: record.parent_hash,
                expected: self.head_hash,
            };
        }
        Admit::Next
    }

    /// Take a record that [`Self::admit`] called [`Admit::Next`]: it is the
    /// new head.
    pub fn take(&mut self, record: &L1Block) {
        self.head = record.number;
        self.head_hash = record.hash;
        self.seen.insert(record.number, record.hash);
        let floor = self.head.saturating_sub(DEDUP_HORIZON);
        self.seen = self.seen.split_off(&floor);
    }
}

#[cfg(test)]
#[path = "l1_block_tests.rs"]
mod tests;

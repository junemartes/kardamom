//! The void record: the sealer's canonical decision to remove one entry.
//!
//! The sealer orders a [`crate::TxRef`] before the archives make the
//! transaction data durable. A failure can lose the data of an entry that is
//! already in the order. No consumer can execute such an entry. The sealer
//! then appends a void record, and every consumer drops the entry at the same
//! canonical index. A consumer never drops an entry on its own clock: two
//! replicas with different archives would then build different states.

use alloy_primitives::B256;
use rkyv::{Archive, Deserialize, Serialize};

use crate::wire::B256Bytes;

/// Removes the [`crate::TxRef`] at canonical index `index` from the chain.
///
/// The transaction enters no block and has no receipt, and its nonce stays
/// free. The record takes one canonical slot of its own, as an epoch record
/// does.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Hash, Archive, Serialize, Deserialize)]
#[rkyv(derive(Debug), compare(PartialEq))]
pub struct VoidRecord {
    /// The canonical index of the entry that the record removes.
    pub index: u64,
    /// The hash of the removed transaction. A consumer compares it with the
    /// entry it holds at `index`, so a record for the wrong entry is an error
    /// and not a silent drop. A consumer also removes it from its dedup
    /// window, so the sender can submit the same signed bytes again.
    #[rkyv(with = B256Bytes)]
    pub tx_hash: B256,
}

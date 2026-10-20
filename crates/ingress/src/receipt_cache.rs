//! In-memory `tx_receipts` index that the ingress proxy uses.
//!
//! This gives two views over the same receipts. The proxy's `tx_receipts`
//! watcher, from executor to ingress, fills both through
//! [`ReceiptCache::insert`]:
//!
//! - `(Address, u64) -> Receipt` for retry-dedup in `submit_raw`.
//! - `B256 -> Receipt` to answer `eth_getTransactionReceipt(tx_hash)`
//!   without a join against the state DB.
//!
//! The enriched `types::Receipt` carries `from`, `nonce`, and `tx_hash`
//! directly, so one `tx_receipts` subscription feeds both indexes.

use std::num::NonZeroUsize;
use std::sync::Arc;

use alloy_primitives::{Address, B256};
use crossbeam_queue::ArrayQueue;
use dashmap::DashMap;

use kardamom_types::Receipt;

/// Bounded, oldest first. The `order` ring holds one identity per
/// insert. When the ring is full, the insert that fills it displaces
/// the oldest identity, and that receipt leaves both indexes. So the
/// newest `capacity` receipts are always present. An evicted sender
/// resubmits, and the sequencer dedupes the resubmit through the
/// past-nonce path. A lookup on `eth_getTransactionReceipt` for an
/// evicted entry returns `null`. A future v1 fallback can query the
/// state DB instead.
pub(crate) struct ReceiptCache {
    by_sender_nonce: DashMap<(Address, u64), Arc<Receipt>>,
    by_tx_hash: DashMap<B256, Arc<Receipt>>,
    order: ArrayQueue<Identity>,
}

/// One insert's identity in the eviction ring.
#[derive(Clone, Copy)]
struct Identity {
    sender: Address,
    nonce: u64,
    tx_hash: B256,
}

impl Identity {
    fn of(receipt: &Receipt) -> Self {
        Self {
            sender: receipt.from,
            nonce: receipt.nonce,
            tx_hash: receipt.tx_hash,
        }
    }
}

impl ReceiptCache {
    #[must_use]
    pub(crate) fn new(capacity: NonZeroUsize) -> Self {
        Self {
            by_sender_nonce: DashMap::new(),
            by_tx_hash: DashMap::new(),
            order: ArrayQueue::new(capacity.get()),
        }
    }

    /// Inserts one receipt into both indexes, then retires the oldest
    /// identity if the ring is full. A duplicate (sender, nonce) entry
    /// overwrites the older row and takes one more ring slot, so it
    /// shortens only its own horizon. The indexes fill before the ring
    /// does: a concurrent insert that displaces this slot then finds
    /// the rows to remove, and no row outlives its slot.
    ///
    /// A skip that used no nonce, such as a copy of a transaction that the
    /// sealer ordered again, never replaces the receipt of its
    /// (sender, nonce): it is dropped, and takes no slot.
    pub(crate) fn insert(&self, receipt: Receipt) {
        if !receipt.uses_nonce()
            && self
                .by_sender_nonce
                .contains_key(&(receipt.from, receipt.nonce))
        {
            return;
        }
        let identity = Identity::of(&receipt);
        let receipt = Arc::new(receipt);
        self.by_sender_nonce
            .insert((identity.sender, identity.nonce), receipt.clone());
        self.by_tx_hash.insert(identity.tx_hash, receipt);
        if let Some(oldest) = self.order.force_push(identity) {
            self.retire(oldest);
        }
    }

    /// Removes the rows of a displaced identity. Each row goes only if
    /// it still belongs to that identity: a newer receipt of the same
    /// (sender, nonce) keeps its row and its own slot.
    fn retire(&self, oldest: Identity) {
        self.by_sender_nonce
            .remove_if(&(oldest.sender, oldest.nonce), |_, r| {
                r.tx_hash == oldest.tx_hash
            });
        self.by_tx_hash.remove_if(&oldest.tx_hash, |_, r| {
            r.from == oldest.sender && r.nonce == oldest.nonce
        });
    }

    /// Drops every receipt of a block above `to_block` from both
    /// indexes. The ring keeps their identities. A retired identity
    /// removes only a row that still belongs to it, so a stale identity
    /// costs one slot and nothing more.
    pub(crate) fn roll_back(&self, to_block: u64) {
        self.by_sender_nonce
            .retain(|_, r| r.block_number <= to_block);
        self.by_tx_hash.retain(|_, r| r.block_number <= to_block);
    }

    pub(crate) fn lookup(&self, sender: Address, nonce: u64) -> Option<Receipt> {
        self.by_sender_nonce
            .get(&(sender, nonce))
            .map(|r| (**r).clone())
    }

    pub(crate) fn lookup_by_tx_hash(&self, tx_hash: B256) -> Option<Receipt> {
        self.by_tx_hash.get(&tx_hash).map(|r| (**r).clone())
    }

    #[cfg(test)]
    fn len(&self) -> usize {
        self.by_sender_nonce.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::receipt;

    #[tokio::test]
    async fn lookup_returns_inserted() {
        let c = ReceiptCache::new(NonZeroUsize::new(8).unwrap());
        let s = Address::repeat_byte(0x33);
        let h = B256::repeat_byte(0x11);
        c.insert(receipt(s, 1, h, 1));
        assert_eq!(c.lookup(s, 1).unwrap().tx_idx.term_offset, 1);
        assert!(c.lookup(s, 2).is_none());
        // The same entry is also indexed by tx_hash.
        assert_eq!(c.lookup_by_tx_hash(h).unwrap().tx_hash, h);
        assert!(c.lookup_by_tx_hash(B256::repeat_byte(0x22)).is_none());
    }

    // A rollback drops the receipts above its block from both indexes,
    // and keeps the receipts at or below it.
    #[tokio::test]
    async fn a_rollback_drops_the_receipts_above_its_block() {
        let c = ReceiptCache::new(NonZeroUsize::new(8).unwrap());
        let s = Address::repeat_byte(0x33);
        let in_block = |nonce: u64, hash: u8, block_number: u64| Receipt {
            block_number,
            ..receipt(s, nonce, B256::repeat_byte(hash), 0)
        };
        c.insert(in_block(1, 0x11, 4));
        c.insert(in_block(2, 0x22, 5));
        c.roll_back(4);
        assert_eq!(c.lookup(s, 1).unwrap().block_number, 4);
        assert!(c.lookup_by_tx_hash(B256::repeat_byte(0x11)).is_some());
        assert!(c.lookup(s, 2).is_none());
        assert!(c.lookup_by_tx_hash(B256::repeat_byte(0x22)).is_none());
    }

    // Inserting past capacity keeps the cache bounded and usable.
    #[tokio::test(flavor = "current_thread")]
    async fn insert_past_capacity_stays_bounded() {
        let cap = 64usize;
        let c = ReceiptCache::new(NonZeroUsize::new(cap).unwrap());
        // Insert well past capacity, with a distinct (sender, nonce,
        // tx_hash) each time.
        for i in 0..(cap as u64 * 4) {
            let b = (i % 251) as u8;
            c.insert(receipt(
                Address::repeat_byte(b ^ 0x5a),
                i,
                B256::repeat_byte(b ^ 0xa5),
                i32::try_from(i).unwrap(),
            ));
        }
        // Bounded: the cache never exceeds capacity. The hashes repeat
        // every 251 inserts while the nonces do not, so the rows are not
        // one per insert; the bound still holds.
        assert!(
            c.len() <= cap,
            "cache must stay bounded: {} > {cap}",
            c.len()
        );
        // Still usable: the cache can retrieve the most recent insert.
        let last = (cap as u64 * 4) - 1;
        assert!(
            c.lookup(Address::repeat_byte(((last % 251) as u8) ^ 0x5a), last)
                .is_some()
        );
    }

    // The horizon is a lower bound for every entry: the newest
    // `capacity` receipts are always present, and older ones are gone.
    #[tokio::test]
    async fn the_newest_capacity_receipts_always_survive() {
        let cap = 256usize;
        let c = ReceiptCache::new(NonZeroUsize::new(cap).unwrap());
        let ident = |i: u64| {
            let mut a = [0u8; 20];
            a[..8].copy_from_slice(&i.to_be_bytes());
            let mut h = [0u8; 32];
            h[..8].copy_from_slice(&i.to_be_bytes());
            (Address::from(a), B256::from(h))
        };
        let total = cap as u64 * 3;
        for i in 0..total {
            let (s, h) = ident(i);
            c.insert(receipt(s, i, h, 0));
        }
        for i in 0..total {
            let (s, h) = ident(i);
            let kept = i >= total - cap as u64;
            assert_eq!(
                c.lookup_by_tx_hash(h).is_some(),
                kept,
                "hash of receipt {i}"
            );
            assert_eq!(c.lookup(s, i).is_some(), kept, "identity of receipt {i}");
        }
    }

    // A newer receipt of the same (sender, nonce) keeps its row when
    // the older one's slot retires.
    #[tokio::test]
    async fn a_retired_slot_does_not_remove_a_newer_row_of_its_identity() {
        let c = ReceiptCache::new(NonZeroUsize::new(2).unwrap());
        let s = Address::repeat_byte(0x33);
        let old = B256::repeat_byte(0x11);
        let new = B256::repeat_byte(0x22);
        c.insert(receipt(s, 1, old, 1));
        c.insert(receipt(s, 1, new, 2));
        // This insert retires the slot of `old`.
        c.insert(receipt(
            Address::repeat_byte(0x44),
            9,
            B256::repeat_byte(0x99),
            3,
        ));
        assert!(c.lookup_by_tx_hash(old).is_none());
        assert_eq!(c.lookup(s, 1).unwrap().tx_hash, new);
        assert_eq!(c.lookup_by_tx_hash(new).unwrap().tx_idx.term_offset, 2);
    }

    // A copy of a transaction that the sealer ordered again skips with no
    // nonce. It never replaces the receipt that used the nonce, and a later
    // receipt that uses the nonce replaces a skip.
    #[tokio::test]
    async fn a_skip_that_used_no_nonce_never_replaces_the_receipt_of_its_nonce() {
        let c = ReceiptCache::new(NonZeroUsize::new(8).unwrap());
        let s = Address::repeat_byte(0x33);
        let h = B256::repeat_byte(0x11);
        let skip = |at: i32| Receipt {
            status: false,
            gas_used: 0,
            skip_reason: Some(kardamom_types::SkipReason::NonceTooLow),
            ..receipt(s, 1, h, at)
        };
        c.insert(receipt(s, 1, h, 1));
        c.insert(skip(2));
        assert_eq!(c.lookup(s, 1).unwrap().tx_idx.term_offset, 1);
        assert_eq!(c.lookup_by_tx_hash(h).unwrap().tx_idx.term_offset, 1);

        let t = Address::repeat_byte(0x44);
        c.insert(Receipt { from: t, ..skip(3) });
        assert_eq!(c.lookup(t, 1).unwrap().tx_idx.term_offset, 3);
        c.insert(receipt(t, 1, B256::repeat_byte(0x22), 4));
        assert_eq!(c.lookup(t, 1).unwrap().tx_idx.term_offset, 4);
    }
}

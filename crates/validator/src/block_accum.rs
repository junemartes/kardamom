//! A per-block accumulator, flushed at the block boundary. Shared shape
//! behind [`AttestingReceiptSink`](crate::attester::AttestingReceiptSink)
//! and [`ExtractingReceiptSink`](crate::interop::sink::ExtractingReceiptSink):
//! both buffer items per block as they stream in, then drain everything
//! through a boundary in one call, in ascending block order. Whether the
//! boundary block itself must appear even with nothing accumulated is a
//! per-caller policy (see each sink's own `flush_through`), not baked in
//! here: forcing it here would force every caller's per-block follow-up
//! work (a claims lookup, for one) to run for the boundary block too, even
//! when a caller's own policy is to skip that work for an empty block.

use std::collections::BTreeMap;

pub(crate) struct BlockAccumulator<T> {
    pending: BTreeMap<u64, Vec<T>>,
}

impl<T> BlockAccumulator<T> {
    pub(crate) fn new() -> Self {
        Self {
            pending: BTreeMap::new(),
        }
    }

    /// Append one item to a block's entry.
    pub(crate) fn push(&mut self, block: u64, item: T) {
        self.pending.entry(block).or_default().push(item);
    }

    /// Append one block's items, growing its entry.
    pub(crate) fn push_all(&mut self, block: u64, items: impl IntoIterator<Item = T>) {
        self.pending.entry(block).or_default().extend(items);
    }

    /// Drain every block up to and including `block`, ascending. A block
    /// with nothing accumulated is simply absent from the result.
    pub(crate) fn drain_through(&mut self, block: u64) -> BTreeMap<u64, Vec<T>> {
        // A wrap here (`block == u64::MAX`) would drain nothing, instead
        // of clearing the just-flushed range.
        let tail = self.pending.split_off(&block.saturating_add(1));
        std::mem::replace(&mut self.pending, tail)
    }
}

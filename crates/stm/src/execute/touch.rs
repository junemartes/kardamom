/// The feed's last-toucher index: cell-hash to most recent toucher.
///
/// Flat and hash-keyed, not a `HashMap<DomainKey, u32>`. The map held
/// thousands of live entries with large keys, ran past the L2 cache
/// size, and compared those keys on every probe. The upsert pair was
/// the serial feed's largest stage. Here a slot is 16 bytes, the whole
/// table fits in 256KB, a probe is one cache line, and the key
/// comparison is a u64.
///
/// Collisions are safe by construction: a 64-bit collision fabricates a
/// dependency edge between two transactions that do not actually share
/// a cell. The DAG is conservative: a false edge costs a sliver of
/// parallelism and nothing else, while a missed edge (impossible here,
/// since equal cells hash equal) is what validation exists to catch.
///
/// Reset is O(1): a slot belongs to the current block only if its
/// `stamp` matches, so a new block bumps the stamp instead of clearing
/// the whole table.
pub(super) struct TouchSlot {
    pub(super) hash: u64,
    pub(super) idx: u32,
    pub(super) stamp: u32,
}

/// A table capacity that is always a power of two. The `& mask` probe
/// walk in [`TouchTable`] needs this: with a non-power-of-two capacity
/// it would reach only a subset of the slots, and `upsert` would spin
/// forever once that subset fills. Rounding up at construction makes
/// that failure unrepresentable, so `TouchTable::new` cannot fail.
#[derive(Debug, Clone, Copy)]
pub(super) struct Pow2(usize);

impl Pow2 {
    pub(super) fn new(n: usize) -> Self {
        Self(n.next_power_of_two())
    }
}

pub(crate) struct TouchTable {
    slots: Vec<TouchSlot>,
    mask: usize,
    stamp: u32,
}

impl TouchTable {
    pub(super) fn new(capacity: Pow2) -> Self {
        let capacity_pow2 = capacity.0;
        Self {
            slots: (0..capacity_pow2)
                .map(|_| TouchSlot {
                    hash: 0,
                    idx: 0,
                    stamp: 0,
                })
                .collect(),
            mask: capacity_pow2 - 1,
            stamp: 0,
        }
    }

    /// O(1) between-block reset (and the ⊤-barrier clear).
    pub(super) fn clear(&mut self) {
        self.stamp = self.stamp.wrapping_add(1);
        if self.stamp == 0 {
            self.hard_clear();
        }
    }

    /// The stamp wrapped after billions of blocks. Hard-clear so no
    /// stale slot can resurrect, then restart at 1. [`Self::clear`]'s
    /// branch stays free of a loop.
    fn hard_clear(&mut self) {
        for s in &mut self.slots {
            s.stamp = 0;
        }
        self.stamp = 1;
    }

    /// Record `idx` as the latest toucher of `hash`, and return the
    /// previous one, if this block has seen the cell.
    #[inline]
    #[allow(
        clippy::cast_possible_truncation,
        reason = "`& self.mask` keeps only the table's low index bits either way, so truncating hash first changes nothing"
    )]
    pub(super) fn upsert(&mut self, hash: u64, idx: u32) -> Option<u32> {
        let mut i = hash as usize & self.mask;
        loop {
            if let Probe::Done(r) = self.probe_slot(hash, idx, &mut i) {
                return r;
            }
        }
    }

    /// One linear-probe step at index `*i`: claim an empty or stale
    /// slot, update an existing match, or advance `*i` to the next
    /// slot. The `while` loop in [`Self::upsert`] stays free of a
    /// branch.
    fn probe_slot(&mut self, hash: u64, idx: u32, i: &mut usize) -> Probe {
        let slot = &mut self.slots[*i];
        if slot.stamp != self.stamp {
            slot.hash = hash;
            slot.idx = idx;
            slot.stamp = self.stamp;
            return Probe::Done(None);
        }
        if slot.hash == hash {
            let prev = slot.idx;
            slot.idx = idx;
            return Probe::Done(Some(prev));
        }
        *i = (*i + 1) & self.mask;
        Probe::Continue
    }
}

/// One [`TouchTable::probe_slot`] step's outcome.
enum Probe {
    /// The probe finished: `Some(prev)` on an existing match, `None` on
    /// an empty or stale slot claimed for `idx`.
    Done(Option<u32>),
    /// No decision yet; keep probing the next slot.
    Continue,
}

/// Per-shard last-toucher tables for sharded admission.
///
/// SAFETY: table `k` is touched only from the lane executing chunk `k`,
/// and `WorkerPool::run` hands each chunk index to exactly one lane and
/// returns only after every lane has finished. So accesses to a given
/// table are serialized, and the batch boundary orders them against the
/// router's reads.
pub(super) struct ShardTables(Vec<std::cell::UnsafeCell<TouchTable>>);

unsafe impl Sync for ShardTables {}

unsafe impl Send for ShardTables {}

impl ShardTables {
    pub(super) fn new(k: usize, capacity_per_shard: usize) -> Self {
        let capacity = Pow2::new(capacity_per_shard);
        Self(
            (0..k)
                .map(|_| std::cell::UnsafeCell::new(TouchTable::new(capacity)))
                .collect(),
        )
    }
    /// # Safety
    /// Caller must be the sole accessor of shard `k` for the duration
    /// (guaranteed by the one-chunk-per-lane contract).
    #[allow(
        clippy::mut_from_ref,
        reason = "returns a mutable view into shard k under the SAFETY contract above: the caller holds sole access to that shard for the call's duration"
    )]
    pub(super) unsafe fn table(&self, k: usize) -> &mut TouchTable {
        unsafe { &mut *self.0[k].get() }
    }
    pub(super) fn len(&self) -> usize {
        self.0.len()
    }
}

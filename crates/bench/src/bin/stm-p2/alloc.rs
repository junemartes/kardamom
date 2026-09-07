//! Allocation counting for the STM A/B benchmark.
//!
//! A counting allocator: every allocation goes through one pair of
//! relaxed counters. It counts allocation calls, bytes, and a
//! size-class histogram, plus reallocation calls and bytes. This is
//! the only offline way to attribute allocation pressure to a specific
//! run.

struct CountingAlloc;
static ALLOC_CALLS: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
static ALLOC_BYTES: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
/// A size-class histogram: under 64, under 512, under 4K, under 32K,
/// under 256K, and larger.
static ALLOC_BUCKETS: [std::sync::atomic::AtomicU64; 6] = [
    std::sync::atomic::AtomicU64::new(0),
    std::sync::atomic::AtomicU64::new(0),
    std::sync::atomic::AtomicU64::new(0),
    std::sync::atomic::AtomicU64::new(0),
    std::sync::atomic::AtomicU64::new(0),
    std::sync::atomic::AtomicU64::new(0),
];
static BUCKET_BYTES: [std::sync::atomic::AtomicU64; 6] = [
    std::sync::atomic::AtomicU64::new(0),
    std::sync::atomic::AtomicU64::new(0),
    std::sync::atomic::AtomicU64::new(0),
    std::sync::atomic::AtomicU64::new(0),
    std::sync::atomic::AtomicU64::new(0),
    std::sync::atomic::AtomicU64::new(0),
];

fn bucket_of(sz: usize) -> usize {
    match sz {
        0..=63 => 0,
        64..=511 => 1,
        512..=4095 => 2,
        4096..=32767 => 3,
        32_768..=262_143 => 4,
        _ => 5,
    }
}

static REALLOC_CALLS: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
static REALLOC_BYTES: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

impl CountingAlloc {
    /// Record one allocation call for `layout`, in the totals and in
    /// its size-class bucket.
    fn count(layout: std::alloc::Layout) {
        use std::sync::atomic::Ordering::Relaxed;
        ALLOC_CALLS.fetch_add(1, Relaxed);
        ALLOC_BYTES.fetch_add(layout.size() as u64, Relaxed);
        let b = bucket_of(layout.size());
        ALLOC_BUCKETS[b].fetch_add(1, Relaxed);
        BUCKET_BYTES[b].fetch_add(layout.size() as u64, Relaxed);
    }
}

unsafe impl std::alloc::GlobalAlloc for CountingAlloc {
    unsafe fn alloc(&self, layout: std::alloc::Layout) -> *mut u8 {
        Self::count(layout);
        unsafe { std::alloc::System.alloc(layout) }
    }
    unsafe fn dealloc(&self, ptr: *mut u8, layout: std::alloc::Layout) {
        unsafe { std::alloc::System.dealloc(ptr, layout) }
    }
    // This is explicit, so Vec growth is measured as growth. The default
    // implementation routes through alloc() and dealloc(), which makes a
    // 4 to 8 to 16 growth series read as three fresh allocations.
    unsafe fn realloc(&self, ptr: *mut u8, layout: std::alloc::Layout, new_size: usize) -> *mut u8 {
        REALLOC_CALLS.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        REALLOC_BYTES.fetch_add(new_size as u64, std::sync::atomic::Ordering::Relaxed);
        unsafe { std::alloc::System.realloc(ptr, layout, new_size) }
    }
    unsafe fn alloc_zeroed(&self, layout: std::alloc::Layout) -> *mut u8 {
        Self::count(layout);
        unsafe { std::alloc::System.alloc_zeroed(layout) }
    }
}

fn bucket_snap() -> [(u64, u64); 6] {
    std::array::from_fn(|i| {
        (
            ALLOC_BUCKETS[i].load(std::sync::atomic::Ordering::Relaxed),
            BUCKET_BYTES[i].load(std::sync::atomic::Ordering::Relaxed),
        )
    })
}

#[global_allocator]
static COUNTING_ALLOC: CountingAlloc = CountingAlloc;

/// A reading of the counting allocator, or the activity between two
/// readings. Both share this shape: a reading is just the activity
/// since the counters started at zero.
#[derive(Clone, Copy, Default)]
pub(crate) struct AllocDelta {
    pub(crate) calls: u64,
    pub(crate) bytes: u64,
    pub(crate) reallocs: u64,
    pub(crate) rebytes: u64,
    pub(crate) buckets: [(u64, u64); 6],
}

/// Read the counting allocator now.
pub(crate) fn snapshot() -> AllocDelta {
    use std::sync::atomic::Ordering::Relaxed;
    AllocDelta {
        calls: ALLOC_CALLS.load(Relaxed),
        bytes: ALLOC_BYTES.load(Relaxed),
        reallocs: REALLOC_CALLS.load(Relaxed),
        rebytes: REALLOC_BYTES.load(Relaxed),
        buckets: bucket_snap(),
    }
}

impl AllocDelta {
    /// The activity between `before` and `after`.
    pub(crate) fn since(before: &Self, after: &Self) -> Self {
        let mut buckets = [(0u64, 0u64); 6];
        buckets
            .iter_mut()
            .zip(before.buckets.iter().zip(after.buckets.iter()))
            .for_each(|(b, (bef, aft))| {
                *b = (aft.0.saturating_sub(bef.0), aft.1.saturating_sub(bef.1));
            });
        Self {
            calls: after.calls.saturating_sub(before.calls),
            bytes: after.bytes.saturating_sub(before.bytes),
            reallocs: after.reallocs.saturating_sub(before.reallocs),
            rebytes: after.rebytes.saturating_sub(before.rebytes),
            buckets,
        }
    }
}

//! libmdbx geometry and MVCC version-horizon sizing.
//!
//! # Throughput → sizing derivation
//!
//! Spec §5 calls for sustaining the v1 hot path:
//!
//! - 1e6 tx/s × ~100 B average state delta = 100 MB/s of dirty state.
//! - 250 ms virtual-block cadence ⇒ ~25 MB written per RW txn at the cap.
//! - Snapshot horizon = 4 blocks (≈1 s) so the executor can hold an RO txn
//!   across at most 4 commits without page reuse.
//!
//! Therefore the live working set the freelist must keep alive is roughly
//! 4 × 25 MB = 100 MB of dirty pages on top of the resident state.
//!
//! At V0 (sequential executor) the realistic ceiling is ~100k tx/s, so the
//! same numbers leave a 10× safety margin.

/// Max blocks an executor RO snapshot may be held before the writer must stop.
/// The writer pauses (or alerts and halts) if the snapshot horizon is exceeded.
pub const HORIZON_BLOCKS: u32 = 4;

/// mdbx page size — power-of-two between 256B and 64KB.
/// 16 KB matches the OS page on aarch64 and keeps freelist entries compact.
pub const PAGE_SIZE: usize = 16 * 1024;

/// Address-space ceiling. libmdbx supports up to 128 TB; we pick 256 GB so
/// we never need a runtime grow / remap. Actual on-disk size grows on demand.
pub const SIZE_UPPER: usize = 256 * 1024 * 1024 * 1024;

/// Starting on-disk size: 64 MB. Grows by `growth_step` on demand.
pub const SIZE_LOWER: usize = 64 * 1024 * 1024;

/// Grow the file 256 MB at a time so frequent commits do not fragment.
pub const GROWTH_STEP: isize = 256 * 1024 * 1024;

/// Shrink threshold: only release back to OS if 1 GB+ slack accumulates.
pub const SHRINK_STEP: isize = 1024 * 1024 * 1024;

/// Max concurrent readers: 1 executor (the long-lived snapshot) + W block-STM
/// workers (each may briefly open a child read in v1) + the RPC server's
/// state-query pool (default 16) + compaction reader + 4 slots of headroom
/// for chaos tests. Kept conservative because each reader slot costs ~128 B
/// in shared memory.
pub const MAX_READERS: u64 = 64;

/// Number of named DBs we open at env init time. Keep this in sync with
/// [`crate::schema::ALL_TABLES`]; the env builder uses this to size the
/// internal slot table.
pub const MAX_DBS: usize = 16;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn growth_step_is_page_aligned() {
        assert_eq!(GROWTH_STEP as usize % PAGE_SIZE, 0);
        assert_eq!(SHRINK_STEP as usize % PAGE_SIZE, 0);
    }

    #[test]
    fn size_lower_below_upper() {
        // const-block per clippy::assertions_on_constants
        const _: () = assert!(SIZE_LOWER < SIZE_UPPER);
    }

    #[test]
    fn max_dbs_above_table_count() {
        // MAX_DBS is the upper bound used by mdbx_env_set_maxdbs; it must
        // be ≥ the number of tables we open. We tolerate slack so adding a
        // future table doesn't require a code change here.
        assert!(MAX_DBS >= crate::schema::ALL_TABLES.len());
    }
}

//! libmdbx geometry and MVCC version-horizon sizing.
//!
//! # Throughput to sizing derivation
//!
//! Spec section 5 sets the target for the v1 hot path:
//!
//! - 1e6 tx/s at ~100 B average state delta gives 100 MB/s of dirty state.
//! - The block cadence is 250 ms. This gives ~25 MB per write transaction
//!   at the cap.
//! - The snapshot horizon is 4 blocks (about 1 s). The executor can hold a
//!   read-only transaction across at most 4 commits before pages get reused.
//!
//! So the freelist must keep about 4 × 25 MB = 100 MB of dirty pages alive,
//! on top of the resident state.
//!
//! At V0 (the sequential executor), the realistic ceiling is about 100k tx/s.
//! These numbers give a 10× safety margin at that rate.

/// Maximum number of blocks an executor read-only snapshot can stay open
/// before the writer must stop.
///
/// The writer pauses, or alerts and halts, if the snapshot exceeds this limit.
pub const HORIZON_BLOCKS: u32 = 4;

/// The mdbx page size. It must be a power of two between 256 B and 64 KB.
///
/// 16 KB matches the OS page size on aarch64 and keeps freelist entries
/// compact.
pub const PAGE_SIZE: usize = 16 * 1024;

/// The address-space ceiling. libmdbx supports up to 128 TB, but we use
/// 256 GB so the process never needs a runtime grow or remap.
///
/// The actual on-disk size still grows on demand.
pub const SIZE_UPPER: usize = 256 * 1024 * 1024 * 1024;

/// The starting on-disk size, 64 MB. It grows by `growth_step` on demand.
pub const SIZE_LOWER: usize = 64 * 1024 * 1024;

/// Grow the file 256 MB at a time so frequent commits do not fragment it.
pub const GROWTH_STEP: isize = 256 * 1024 * 1024;

/// The shrink threshold. Only release space back to the OS after 1 GB or
/// more of slack accumulates.
pub const SHRINK_STEP: isize = 1024 * 1024 * 1024;

/// The maximum number of concurrent readers. The budget covers:
///
/// - 1 executor (the long-lived snapshot).
/// - W block-STM workers. Each may briefly open a child read in v1.
/// - The RPC server's state-query pool (default 16).
/// - 1 compaction reader.
/// - 4 slots of headroom for chaos tests.
///
/// This value is conservative. Each reader slot costs about 128 B of shared
/// memory.
pub const MAX_READERS: u64 = 64;

/// The number of named DBs the env opens at init time.
///
/// Keep this value in sync with [`crate::schema::ALL_TABLES`]. The env
/// builder uses it to size the internal slot table.
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
        // Use a const block. This satisfies clippy::assertions_on_constants.
        const _: () = assert!(SIZE_LOWER < SIZE_UPPER);
    }

    #[test]
    fn max_dbs_above_table_count() {
        // MAX_DBS is the upper bound for mdbx_env_set_maxdbs. It must be at
        // least the number of tables we open. We allow slack, so adding a
        // table later does not need a code change here.
        assert!(MAX_DBS >= crate::schema::ALL_TABLES.len());
    }
}

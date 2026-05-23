# S6 State Writer Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Ship `crates/kardamom-state` — a libmdbx-backed state DB with a snapshot-swap protocol that lets the S4 executor read state without blocking, plus all schema/recovery/compaction machinery called out in §5 of the sequencer design.

**Architecture:** Single-writer, many-reader libmdbx environment. The writer thread drains the local S4 executor's commit-thread channel (a typed `BlockDelta` message per virtual block boundary) and applies it in one `RW` transaction per block. Reads go through a `StateSnapshot` wrapper around a long-lived `RO` transaction; the executor receives a fresh snapshot through the snapshot-swap protocol after every commit. MVCC horizon is sized so any snapshot the executor may still hold (~4 blocks back) is never page-reused. The trait the executor uses to back `revm::Database` lives in a new shared `crates/kardamom-types` crate so neither `kardamom-executor` nor `kardamom-state` depends on the other.

**Tech Stack:** Rust 2024, `libmdbx = "0.6"` (the `vorot93/libmdbx-rs` binding), `alloy-primitives`, `revm` (consumer only — we expose a `DatabaseRef`-compatible read view), `crossbeam-channel` for the executor↔writer hand-off, `criterion` for benches, `tempfile` for tests.

**Branch:** `claude/s6-state-writer` (branched off `origin/main`). Final PR opens against `main`.

**Reference spec:** `docs/specs/2026-05-23-high-throughput-sequencer-design.md` (§1, §2.4, §2.7, §4.6, §5, V0 scope).

**Assumed interfaces (coordination required):**
- **S3 (`kardamom-log`):** exports `BPosition` (a `(u64 term_id, u32 term_offset)` newtype), an opaque `Receipt` envelope (RLP-encoded body + `tx_idx: u64`), and a `BlockBoundary { block_number: u64, end_tx_idx: u64, l2_timestamp: u64, state_root_commitment: B256 }`. **If S3 does not exist yet, this plan defines all three types locally inside `kardamom-types` with the understanding that S3 will move them out later.** Task 2 below pins those definitions.
- **S4 (`kardamom-executor`):** emits a `BlockDelta` value (defined in this plan, Task 3) on a `crossbeam::channel::Sender<BlockDelta>` provided by `kardamom-state` at startup. Coordination: S4 imports `kardamom_state::BlockDelta` and uses the channel the state writer creates.
- **`StateDatabase` trait:** defined in `kardamom-types` (this plan, Task 2). S4 implements `revm::DatabaseRef` on top of it; S6 provides the concrete impl `StateSnapshot`.

---

## File Structure

New crate `crates/kardamom-state/`:

```
crates/kardamom-state/
├── Cargo.toml
├── src/
│   ├── lib.rs                 # re-exports + crate docs
│   ├── error.rs               # StateError enum
│   ├── schema.rs              # table names + key/value codecs
│   ├── env.rs                 # mdbx Environment open + geometry config
│   ├── snapshot.rs            # StateSnapshot (RO txn wrapper) + StateDatabase impl
│   ├── writer.rs              # StateWriter (single writer thread)
│   ├── delta.rs               # BlockDelta + AccountChanges/StorageChanges/CodeChanges
│   ├── meta.rs                # Meta cursor read/write helpers
│   ├── recovery.rs            # cold-start path (read meta, open snapshot, signal executor)
│   ├── swap.rs                # snapshot-swap protocol (SnapshotHandle, watch channel)
│   ├── compaction.rs          # mdbx_env_copy_compact daemon
│   └── geometry.rs            # constants + sizing math doc-tests
├── tests/
│   ├── schema_codec.rs        # round-trip every table
│   ├── write_replay.rs        # synthetic deltas → assert state matches expected
│   ├── snapshot_mvcc.rs       # pre-N snapshot keeps reading pre-N values across write
│   ├── snapshot_swap.rs       # post-N swap exposes new values
│   ├── recovery_midblock.rs   # kill writer mid-block → restart → no corruption
│   ├── concurrent_readers.rs  # 4 threads at 4 different blocks each see frozen view
│   └── common/mod.rs          # test helpers (genesis fixture, delta builder)
└── benches/
    ├── write_throughput.rs    # criterion: 25MB/block at 4Hz
    └── snapshot_open.rs       # criterion: open RO txn latency
```

New crate `crates/kardamom-types/` (shared types only; no logic):

```
crates/kardamom-types/
├── Cargo.toml
└── src/
    ├── lib.rs                 # re-exports
    ├── state_database.rs      # StateDatabase trait
    ├── block.rs               # BlockNumber, BlockBoundary, BPosition (stub until S3)
    └── receipt.rs             # Receipt envelope (stub until S3)
```

---

## Task 1: Create `kardamom-types` crate skeleton

**Files:**
- Create: `crates/kardamom-types/Cargo.toml`
- Create: `crates/kardamom-types/src/lib.rs`

- [ ] **Step 1: Write `crates/kardamom-types/Cargo.toml`**

```toml
[package]
name = "kardamom-types"
version.workspace = true
edition.workspace = true

[dependencies]
alloy-primitives.workspace = true
revm.workspace = true
serde.workspace = true
thiserror.workspace = true
```

- [ ] **Step 2: Write `crates/kardamom-types/src/lib.rs`**

```rust
//! Shared types used across kardamom subsystems.
//!
//! This crate exists to break dependency cycles: the executor (`kardamom-executor`)
//! and the state writer (`kardamom-state`) both need a common `StateDatabase`
//! trait and shared block/receipt types, but neither should depend on the other.
//!
//! Types and traits here are stable boundaries. Implementation details belong
//! in the owning subsystem crate.

pub mod block;
pub mod receipt;
pub mod state_database;

pub use block::{BPosition, BlockBoundary, BlockNumber};
pub use receipt::Receipt;
pub use state_database::{StateDatabase, StateDatabaseError};
```

- [ ] **Step 3: Verify it builds**

```bash
cd /home/dev/kardamom && cargo build -p kardamom-types
```

Expected: builds. Missing-module errors are expected only after Task 2's modules are written; placeholder empty files are added in Step 4.

- [ ] **Step 4: Create empty module stubs so Step 3 succeeds**

```bash
cd /home/dev/kardamom/crates/kardamom-types/src
echo 'pub struct BlockNumber(pub u64);' > block.rs
echo 'pub struct Receipt;' > receipt.rs
echo 'pub trait StateDatabase {}' > state_database.rs
```

(Real contents land in Task 2; this is just to make the crate compile so the next task can fill it in incrementally.)

Replace `lib.rs` re-exports to match the stubs:

```rust
//! See full docs in Task 2.

pub mod block;
pub mod receipt;
pub mod state_database;
```

- [ ] **Step 5: Verify build**

```bash
cd /home/dev/kardamom && cargo build -p kardamom-types
```

Expected: builds clean (with one warning per stub for unused items).

- [ ] **Step 6: Commit**

```bash
cd /home/dev/kardamom
git add crates/kardamom-types/
git commit -m "types: add kardamom-types crate skeleton"
```

---

## Task 2: Define `StateDatabase` trait and shared types

**Files:**
- Modify: `crates/kardamom-types/src/state_database.rs`
- Modify: `crates/kardamom-types/src/block.rs`
- Modify: `crates/kardamom-types/src/receipt.rs`
- Modify: `crates/kardamom-types/src/lib.rs`

- [ ] **Step 1: Write `state_database.rs`**

```rust
//! Read-only state interface used by the executor to back `revm::DatabaseRef`.
//!
//! Implementors are concrete snapshots of L2 state at some block boundary.
//! The trait is intentionally minimal: only the four reads `revm::DatabaseRef`
//! needs, plus a `block_number()` accessor for diagnostics. All methods are
//! `&self` — concrete impls (e.g. `kardamom_state::StateSnapshot`) wrap a
//! libmdbx RO transaction so reads are MVCC-snapshot-isolated.

use alloy_primitives::{Address, B256, U256};
use revm::primitives::Bytes;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum StateDatabaseError {
    #[error("storage backend error: {0}")]
    Backend(String),
    #[error("decode error in table {table}: {detail}")]
    Decode { table: &'static str, detail: String },
    #[error("snapshot exhausted: writer outran the version horizon")]
    SnapshotExhausted,
}

/// Account record stored in the `accounts` table.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AccountRecord {
    pub nonce: u64,
    pub balance: U256,
    pub code_hash: B256,
    /// `storage_root` is a Merkle commitment recomputed at block boundaries.
    /// Storage *values* live in the flat `storage` table; this is only used
    /// when emitting state-root commitments.
    pub storage_root: B256,
}

pub trait StateDatabase: Send + Sync {
    fn block_number(&self) -> u64;
    fn account(&self, address: Address) -> Result<Option<AccountRecord>, StateDatabaseError>;
    fn storage(&self, address: Address, key: U256) -> Result<U256, StateDatabaseError>;
    fn code_by_hash(&self, code_hash: B256) -> Result<Option<Bytes>, StateDatabaseError>;
}
```

- [ ] **Step 2: Write `block.rs`**

```rust
//! Block and log-position primitives.
//!
//! `BlockBoundary` and `BPosition` are stubs until `kardamom-log` (S3) lands;
//! they are defined here so `kardamom-state` can build standalone. Once S3
//! ships, S3 re-exports these from `kardamom-types` (the canonical home).

use alloy_primitives::B256;
use serde::{Deserialize, Serialize};

pub type BlockNumber = u64;

/// Aeron archive position: `(term_id, term_offset)`. Canonical tx identifier.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct BPosition {
    pub term_id: u64,
    pub term_offset: u32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct BlockBoundary {
    pub block_number: BlockNumber,
    pub end_tx_idx: u64,
    pub l2_timestamp: u64,
    pub state_root_commitment: B256,
}
```

- [ ] **Step 3: Write `receipt.rs`**

```rust
//! Stub receipt envelope. The real shape is owned by `kardamom-log` (S3).
//!
//! Keep this minimal: `kardamom-state` only writes receipts opaquely
//! (`code_hash -> bytes` semantics), so we expose a thin newtype around RLP bytes.

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Receipt {
    pub tx_idx: u64,
    pub rlp: Vec<u8>,
}
```

- [ ] **Step 4: Update `lib.rs`**

```rust
//! Shared types used across kardamom subsystems.

pub mod block;
pub mod receipt;
pub mod state_database;

pub use block::{BPosition, BlockBoundary, BlockNumber};
pub use receipt::Receipt;
pub use state_database::{AccountRecord, StateDatabase, StateDatabaseError};
```

- [ ] **Step 5: Build**

```bash
cd /home/dev/kardamom && cargo build -p kardamom-types
```

Expected: clean.

- [ ] **Step 6: Commit**

```bash
cd /home/dev/kardamom
git add crates/kardamom-types/
git commit -m "types: define StateDatabase trait and shared block/receipt types"
```

---

## Task 3: Create `kardamom-state` crate skeleton

**Files:**
- Create: `crates/kardamom-state/Cargo.toml`
- Create: `crates/kardamom-state/src/lib.rs`
- Create: empty stubs for every src/ file listed in File Structure.

- [ ] **Step 1: Write `Cargo.toml`**

```toml
[package]
name = "kardamom-state"
version.workspace = true
edition.workspace = true

[dependencies]
kardamom-types = { path = "../kardamom-types" }
alloy-primitives.workspace = true
alloy-rlp.workspace = true
revm.workspace = true
serde.workspace = true
thiserror.workspace = true
tracing.workspace = true
anyhow.workspace = true
crossbeam-channel = "0.5"
libmdbx = "0.6"
metrics.workspace = true

[dev-dependencies]
tempfile = "3"
criterion = { version = "0.5", features = ["html_reports"] }

[[bench]]
name = "write_throughput"
harness = false

[[bench]]
name = "snapshot_open"
harness = false
```

- [ ] **Step 2: Write `src/lib.rs`**

```rust
//! libmdbx-backed L2 state DB.
//!
//! See `docs/specs/2026-05-23-high-throughput-sequencer-design.md` §5 for the
//! protocol and crate-level invariants.
//!
//! Public surface:
//! - [`StateEnv`] — owns the mdbx `Environment` and table handles.
//! - [`StateWriter`] — single-writer thread that drains a `BlockDelta` channel
//!   and commits one mdbx RW txn per block boundary.
//! - [`StateSnapshot`] — RO txn wrapper exposed to the executor via the
//!   `kardamom_types::StateDatabase` trait.
//! - [`SnapshotHandle`] — snapshot-swap channel published by the writer; the
//!   executor watches it to pick up post-N snapshots.

pub mod compaction;
pub mod delta;
pub mod env;
pub mod error;
pub mod geometry;
pub mod meta;
pub mod recovery;
pub mod schema;
pub mod snapshot;
pub mod swap;
pub mod writer;

pub use delta::{AccountChanges, BlockDelta, CodeChanges, StorageChanges};
pub use env::{StateEnv, StateEnvBuilder};
pub use error::StateError;
pub use snapshot::StateSnapshot;
pub use swap::{SnapshotHandle, SnapshotReceiver};
pub use writer::{StateWriter, WriterHandle};
```

- [ ] **Step 3: Create empty stub files**

```bash
cd /home/dev/kardamom/crates/kardamom-state
mkdir -p src tests benches
for f in error schema env snapshot writer delta meta recovery swap compaction geometry; do
  printf '//! See module docs in lib.rs.\n' > src/$f.rs
done
mkdir -p tests/common
printf '//! Test helpers.\n' > tests/common/mod.rs
```

- [ ] **Step 4: Build**

```bash
cd /home/dev/kardamom && cargo build -p kardamom-state
```

Expected: build fails — `lib.rs` re-exports types not yet defined. Reduce `lib.rs` to just the module declarations (drop the `pub use` block) until Task 4 fills in `error.rs`, etc.

Replace `lib.rs` with the minimal version:

```rust
//! libmdbx-backed L2 state DB. See spec §5.

pub mod compaction;
pub mod delta;
pub mod env;
pub mod error;
pub mod geometry;
pub mod meta;
pub mod recovery;
pub mod schema;
pub mod snapshot;
pub mod swap;
pub mod writer;
```

- [ ] **Step 5: Build again**

```bash
cd /home/dev/kardamom && cargo build -p kardamom-state
```

Expected: clean (lots of "module file is empty" but no errors).

- [ ] **Step 6: Commit**

```bash
cd /home/dev/kardamom
git add crates/kardamom-state/
git commit -m "state: add kardamom-state crate skeleton"
```

---

## Task 4: `error.rs` — `StateError` enum

**Files:**
- Modify: `crates/kardamom-state/src/error.rs`

- [ ] **Step 1: Write the file**

```rust
//! Error type for all `kardamom-state` operations.

use kardamom_types::StateDatabaseError;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum StateError {
    #[error("mdbx error: {0}")]
    Mdbx(#[from] libmdbx::Error),
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    #[error("rlp decode error: {0}")]
    Rlp(#[from] alloy_rlp::Error),
    #[error("decode error in table {table}: expected {expected} bytes, got {got}")]
    BadEncoding {
        table: &'static str,
        expected: usize,
        got: usize,
    },
    #[error("writer channel closed before block {block} was committed")]
    WriterChannelClosed { block: u64 },
    #[error("recovery failed: {0}")]
    Recovery(String),
    #[error("snapshot exhausted: writer outran the {horizon}-block version horizon")]
    SnapshotExhausted { horizon: u32 },
}

impl From<StateError> for StateDatabaseError {
    fn from(value: StateError) -> Self {
        match value {
            StateError::SnapshotExhausted { .. } => StateDatabaseError::SnapshotExhausted,
            StateError::BadEncoding {
                table,
                expected,
                got,
            } => StateDatabaseError::Decode {
                table,
                detail: format!("expected {expected} bytes, got {got}"),
            },
            other => StateDatabaseError::Backend(other.to_string()),
        }
    }
}
```

- [ ] **Step 2: Build**

```bash
cd /home/dev/kardamom && cargo build -p kardamom-state
```

Expected: clean.

- [ ] **Step 3: Commit**

```bash
git add crates/kardamom-state/src/error.rs
git commit -m "state: add StateError enum"
```

---

## Task 5: `geometry.rs` — MVCC horizon constants + sizing doctests

**Files:**
- Modify: `crates/kardamom-state/src/geometry.rs`

- [ ] **Step 1: Write the file**

```rust
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
//!
//! ```
//! use kardamom_state::geometry::{BYTES_PER_BLOCK, HORIZON_BLOCKS, HORIZON_BYTES};
//! assert_eq!(BYTES_PER_BLOCK, 25 * 1024 * 1024);
//! assert_eq!(HORIZON_BLOCKS, 4);
//! assert_eq!(HORIZON_BYTES, 100 * 1024 * 1024);
//! ```

/// Worst-case state delta per virtual block (250 ms @ 1M tx/s × ~100 B/tx).
pub const BYTES_PER_BLOCK: usize = 25 * 1024 * 1024;

/// Max blocks an executor RO snapshot may be held before the writer must stop.
/// The writer pauses (or alerts and halts) if the snapshot horizon is exceeded.
pub const HORIZON_BLOCKS: u32 = 4;

/// Total dirty-page reservation: `BYTES_PER_BLOCK * HORIZON_BLOCKS`.
pub const HORIZON_BYTES: usize = BYTES_PER_BLOCK * (HORIZON_BLOCKS as usize);

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
pub const MAX_READERS: u32 = 64;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn horizon_math_holds() {
        assert_eq!(HORIZON_BYTES, BYTES_PER_BLOCK * HORIZON_BLOCKS as usize);
    }

    #[test]
    fn growth_step_is_page_aligned() {
        assert_eq!(GROWTH_STEP as usize % PAGE_SIZE, 0);
        assert_eq!(SHRINK_STEP as usize % PAGE_SIZE, 0);
    }

    #[test]
    fn size_lower_below_upper() {
        assert!(SIZE_LOWER < SIZE_UPPER);
    }
}
```

- [ ] **Step 2: Build + run tests**

```bash
cd /home/dev/kardamom && cargo test -p kardamom-state geometry
```

Expected: 3 unit tests pass.

- [ ] **Step 3: Commit**

```bash
git add crates/kardamom-state/src/geometry.rs
git commit -m "state: add MVCC horizon sizing constants with doctest"
```

---

## Task 6: `schema.rs` — table names + key/value codecs

**Files:**
- Modify: `crates/kardamom-state/src/schema.rs`

- [ ] **Step 1: Write the file**

```rust
//! libmdbx schema. Six named tables, each with a fixed key/value encoding.
//!
//! | Table       | Key                              | Value                                            |
//! |-------------|----------------------------------|--------------------------------------------------|
//! | `accounts`  | `Address` (20 B)                 | RLP `(u64 nonce, U256 balance, B256 code_hash, B256 storage_root)` |
//! | `storage`   | `Address ++ B256 key` (52 B)     | `U256 value` (32 B, big-endian)                  |
//! | `code`      | `B256 code_hash` (32 B)          | raw bytecode                                     |
//! | `headers`   | `u64 block_number` (8 B BE)      | RLP `(B256 state_root, u64 end_tx_idx, u64 ts)` |
//! | `receipts`  | `u64 tx_idx` (8 B BE)            | RLP-encoded `Receipt`                            |
//! | `meta`      | `&[u8]` (well-known keys, below) | varies — see `meta.rs`                           |
//!
//! BE encoding keeps `block_number` and `tx_idx` ordered correctly under
//! mdbx's lexicographic cursor; we depend on that for the cold-start scan.

use alloy_primitives::{Address, B256, U256};
use alloy_rlp::{Decodable, Encodable, RlpDecodable, RlpEncodable};

use crate::error::StateError;

pub const TABLE_ACCOUNTS: &str = "accounts";
pub const TABLE_STORAGE: &str = "storage";
pub const TABLE_CODE: &str = "code";
pub const TABLE_HEADERS: &str = "headers";
pub const TABLE_RECEIPTS: &str = "receipts";
pub const TABLE_META: &str = "meta";

pub const ALL_TABLES: &[&str] = &[
    TABLE_ACCOUNTS,
    TABLE_STORAGE,
    TABLE_CODE,
    TABLE_HEADERS,
    TABLE_RECEIPTS,
    TABLE_META,
];

// ---------- accounts ----------

#[derive(Debug, Clone, PartialEq, Eq, RlpEncodable, RlpDecodable)]
pub struct AccountValue {
    pub nonce: u64,
    pub balance: U256,
    pub code_hash: B256,
    pub storage_root: B256,
}

pub fn encode_account_key(addr: Address) -> [u8; 20] {
    addr.into_array()
}

pub fn encode_account_value(v: &AccountValue) -> Vec<u8> {
    let mut buf = Vec::with_capacity(96);
    v.encode(&mut buf);
    buf
}

pub fn decode_account_value(bytes: &[u8]) -> Result<AccountValue, StateError> {
    AccountValue::decode(&mut &bytes[..]).map_err(StateError::from)
}

// ---------- storage ----------

pub fn encode_storage_key(addr: Address, slot: U256) -> [u8; 52] {
    let mut out = [0u8; 52];
    out[..20].copy_from_slice(addr.as_slice());
    out[20..].copy_from_slice(&slot.to_be_bytes::<32>());
    out
}

pub fn encode_storage_value(v: U256) -> [u8; 32] {
    v.to_be_bytes::<32>()
}

pub fn decode_storage_value(bytes: &[u8]) -> Result<U256, StateError> {
    if bytes.len() != 32 {
        return Err(StateError::BadEncoding {
            table: TABLE_STORAGE,
            expected: 32,
            got: bytes.len(),
        });
    }
    let mut arr = [0u8; 32];
    arr.copy_from_slice(bytes);
    Ok(U256::from_be_bytes(arr))
}

// ---------- code ----------

pub fn encode_code_key(hash: B256) -> [u8; 32] {
    hash.into()
}

// code value = raw bytes; no codec needed

// ---------- headers ----------

#[derive(Debug, Clone, PartialEq, Eq, RlpEncodable, RlpDecodable)]
pub struct HeaderValue {
    pub state_root_commitment: B256,
    pub end_tx_idx: u64,
    pub l2_timestamp: u64,
}

pub fn encode_block_key(block_number: u64) -> [u8; 8] {
    block_number.to_be_bytes()
}

pub fn decode_block_key(bytes: &[u8]) -> Result<u64, StateError> {
    if bytes.len() != 8 {
        return Err(StateError::BadEncoding {
            table: TABLE_HEADERS,
            expected: 8,
            got: bytes.len(),
        });
    }
    let mut arr = [0u8; 8];
    arr.copy_from_slice(bytes);
    Ok(u64::from_be_bytes(arr))
}

pub fn encode_header_value(v: &HeaderValue) -> Vec<u8> {
    let mut buf = Vec::with_capacity(64);
    v.encode(&mut buf);
    buf
}

pub fn decode_header_value(bytes: &[u8]) -> Result<HeaderValue, StateError> {
    HeaderValue::decode(&mut &bytes[..]).map_err(StateError::from)
}

// ---------- receipts ----------

pub fn encode_tx_key(tx_idx: u64) -> [u8; 8] {
    tx_idx.to_be_bytes()
}
```

- [ ] **Step 2: Add unit tests at the bottom of the same file**

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use alloy_primitives::{address, b256, U256};

    #[test]
    fn account_value_roundtrip() {
        let v = AccountValue {
            nonce: 42,
            balance: U256::from(1234567890u64),
            code_hash: b256!("11"),
            storage_root: b256!("22"),
        };
        let bytes = encode_account_value(&v);
        let got = decode_account_value(&bytes).unwrap();
        assert_eq!(v, got);
    }

    #[test]
    fn storage_key_layout() {
        let addr = address!("00000000000000000000000000000000000000aa");
        let slot = U256::from(7u64);
        let key = encode_storage_key(addr, slot);
        assert_eq!(&key[..20], addr.as_slice());
        assert_eq!(key[51], 7);
    }

    #[test]
    fn storage_value_roundtrip() {
        let v = U256::from(u128::MAX);
        let bytes = encode_storage_value(v);
        assert_eq!(decode_storage_value(&bytes).unwrap(), v);
    }

    #[test]
    fn storage_value_wrong_length_errors() {
        let err = decode_storage_value(&[0u8; 31]).unwrap_err();
        assert!(matches!(err, StateError::BadEncoding { table, expected: 32, got: 31 } if table == TABLE_STORAGE));
    }

    #[test]
    fn block_key_is_big_endian_ordered() {
        let a = encode_block_key(1);
        let b = encode_block_key(2);
        let c = encode_block_key(256);
        assert!(a < b);
        assert!(b < c);
    }

    #[test]
    fn block_key_roundtrip() {
        for n in [0u64, 1, 250, u64::MAX] {
            assert_eq!(decode_block_key(&encode_block_key(n)).unwrap(), n);
        }
    }

    #[test]
    fn header_value_roundtrip() {
        let v = HeaderValue {
            state_root_commitment: b256!("ab"),
            end_tx_idx: 99,
            l2_timestamp: 1_700_000_000,
        };
        let bytes = encode_header_value(&v);
        assert_eq!(decode_header_value(&bytes).unwrap(), v);
    }
}
```

- [ ] **Step 3: Run tests**

```bash
cd /home/dev/kardamom && cargo test -p kardamom-state schema
```

Expected: 7 unit tests pass.

- [ ] **Step 4: Commit**

```bash
git add crates/kardamom-state/src/schema.rs
git commit -m "state: add libmdbx schema codecs with round-trip tests"
```

---

## Task 7: `meta.rs` — durable cursors

**Files:**
- Modify: `crates/kardamom-state/src/meta.rs`

- [ ] **Step 1: Write the file**

```rust
//! `meta` table: well-known keys for durable cursors.
//!
//! All writes go through the same RW txn as the block delta they correspond
//! to. The atomic boundary is the mdbx commit; cold-start reads the cursors
//! to find the post-recovery snapshot point.
//!
//! | Key                                 | Value                          |
//! |-------------------------------------|--------------------------------|
//! | `last_committed_block`              | `u64 BE`                       |
//! | `last_committed_end_tx_idx`         | `u64 BE`                       |
//! | `last_fsynced_b_position`           | `(u64 term_id, u32 offset)`    |
//! | `schema_version`                    | `u32 BE` (currently 1)         |

use kardamom_types::BPosition;

use crate::error::StateError;

pub const KEY_LAST_COMMITTED_BLOCK: &[u8] = b"last_committed_block";
pub const KEY_LAST_COMMITTED_END_TX_IDX: &[u8] = b"last_committed_end_tx_idx";
pub const KEY_LAST_FSYNCED_B_POSITION: &[u8] = b"last_fsynced_b_position";
pub const KEY_SCHEMA_VERSION: &[u8] = b"schema_version";

pub const SCHEMA_VERSION: u32 = 1;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DurableCursors {
    pub last_committed_block: u64,
    pub last_committed_end_tx_idx: u64,
    pub last_fsynced_b_position: BPosition,
    pub schema_version: u32,
}

impl Default for DurableCursors {
    fn default() -> Self {
        Self {
            last_committed_block: 0,
            last_committed_end_tx_idx: 0,
            last_fsynced_b_position: BPosition {
                term_id: 0,
                term_offset: 0,
            },
            schema_version: SCHEMA_VERSION,
        }
    }
}

pub fn encode_u64(v: u64) -> [u8; 8] {
    v.to_be_bytes()
}

pub fn decode_u64(bytes: &[u8]) -> Result<u64, StateError> {
    if bytes.len() != 8 {
        return Err(StateError::BadEncoding {
            table: "meta",
            expected: 8,
            got: bytes.len(),
        });
    }
    let mut arr = [0u8; 8];
    arr.copy_from_slice(bytes);
    Ok(u64::from_be_bytes(arr))
}

pub fn encode_u32(v: u32) -> [u8; 4] {
    v.to_be_bytes()
}

pub fn decode_u32(bytes: &[u8]) -> Result<u32, StateError> {
    if bytes.len() != 4 {
        return Err(StateError::BadEncoding {
            table: "meta",
            expected: 4,
            got: bytes.len(),
        });
    }
    let mut arr = [0u8; 4];
    arr.copy_from_slice(bytes);
    Ok(u32::from_be_bytes(arr))
}

pub fn encode_b_position(p: BPosition) -> [u8; 12] {
    let mut out = [0u8; 12];
    out[..8].copy_from_slice(&p.term_id.to_be_bytes());
    out[8..].copy_from_slice(&p.term_offset.to_be_bytes());
    out
}

pub fn decode_b_position(bytes: &[u8]) -> Result<BPosition, StateError> {
    if bytes.len() != 12 {
        return Err(StateError::BadEncoding {
            table: "meta",
            expected: 12,
            got: bytes.len(),
        });
    }
    let mut term_id_bytes = [0u8; 8];
    term_id_bytes.copy_from_slice(&bytes[..8]);
    let mut term_offset_bytes = [0u8; 4];
    term_offset_bytes.copy_from_slice(&bytes[8..]);
    Ok(BPosition {
        term_id: u64::from_be_bytes(term_id_bytes),
        term_offset: u32::from_be_bytes(term_offset_bytes),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn u64_roundtrip() {
        for v in [0u64, 1, 250, u64::MAX] {
            assert_eq!(decode_u64(&encode_u64(v)).unwrap(), v);
        }
    }

    #[test]
    fn b_position_roundtrip() {
        let p = BPosition {
            term_id: 7,
            term_offset: 12345,
        };
        assert_eq!(decode_b_position(&encode_b_position(p)).unwrap(), p);
    }

    #[test]
    fn schema_version_codec() {
        assert_eq!(decode_u32(&encode_u32(SCHEMA_VERSION)).unwrap(), SCHEMA_VERSION);
    }
}
```

- [ ] **Step 2: Run tests**

```bash
cd /home/dev/kardamom && cargo test -p kardamom-state meta
```

Expected: 3 tests pass.

- [ ] **Step 3: Commit**

```bash
git add crates/kardamom-state/src/meta.rs
git commit -m "state: add meta-cursor codecs"
```

---

## Task 8: `delta.rs` — `BlockDelta` message format

**Files:**
- Modify: `crates/kardamom-state/src/delta.rs`

- [ ] **Step 1: Write the file**

```rust
//! Block-delta message — the executor → state-writer handoff.
//!
//! The executor's commit thread emits exactly one `BlockDelta` per virtual
//! block boundary on a `crossbeam_channel::Sender<BlockDelta>` handed to it
//! by `StateWriter::new`. The writer drains the receiver and commits one mdbx
//! RW txn per delta.
//!
//! Lifetime: `BlockDelta` is owned data — no borrowed slices, no Arc<…>,
//! because the executor's MV-memory is reset immediately after sending.

use alloy_primitives::{Address, B256, U256};
use kardamom_types::{BPosition, BlockBoundary, Receipt};
use revm::primitives::Bytes;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AccountChange {
    pub address: Address,
    /// `None` ⇒ delete the account record (self-destruct).
    pub new_state: Option<NewAccountState>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NewAccountState {
    pub nonce: u64,
    pub balance: U256,
    pub code_hash: B256,
    pub storage_root: B256,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct AccountChanges(pub Vec<AccountChange>);

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StorageChange {
    pub address: Address,
    pub key: U256,
    /// `None` ⇒ delete the slot (== write zero in EVM semantics, but we
    /// represent it as a tombstone so the flat table shrinks).
    pub value: Option<U256>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct StorageChanges(pub Vec<StorageChange>);

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CodeEntry {
    pub code_hash: B256,
    pub bytecode: Bytes,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct CodeChanges(pub Vec<CodeEntry>);

/// Per-block payload from the executor commit thread.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BlockDelta {
    pub boundary: BlockBoundary,
    /// Position in B (the canonical archive) of the *last* tx covered by
    /// this block. Persisted into `meta.last_fsynced_b_position` at commit.
    pub end_b_position: BPosition,
    pub accounts: AccountChanges,
    pub storage: StorageChanges,
    pub code: CodeChanges,
    pub receipts: Vec<Receipt>,
}

impl BlockDelta {
    /// Worst-case encoded size used by the writer to budget the mdbx txn.
    pub fn approx_size_bytes(&self) -> usize {
        let acct = self.accounts.0.len() * (20 + 96);
        let stor = self.storage.0.len() * (52 + 32);
        let code: usize = self.code.0.iter().map(|c| 32 + c.bytecode.len()).sum();
        let receipts: usize = self.receipts.iter().map(|r| 8 + r.rlp.len()).sum();
        let header = 8 + 64;
        acct + stor + code + receipts + header
    }
}
```

- [ ] **Step 2: Build**

```bash
cd /home/dev/kardamom && cargo build -p kardamom-state
```

Expected: clean.

- [ ] **Step 3: Commit**

```bash
git add crates/kardamom-state/src/delta.rs
git commit -m "state: define BlockDelta message format"
```

---

## Task 9: `env.rs` — open the mdbx environment with geometry

**Files:**
- Modify: `crates/kardamom-state/src/env.rs`

- [ ] **Step 1: Write the file**

```rust
//! mdbx Environment opener and table-handle cache.
//!
//! Every other module in this crate goes through `StateEnv` for the env
//! handle. Geometry is set once at open time per `geometry.rs`.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use libmdbx::{DatabaseFlags, Environment, EnvironmentBuilder, Geometry, Mode, NoWriteMap, PageSize, SyncMode};

use crate::error::StateError;
use crate::geometry::{
    GROWTH_STEP, MAX_READERS, PAGE_SIZE, SHRINK_STEP, SIZE_LOWER, SIZE_UPPER,
};
use crate::schema::ALL_TABLES;

#[derive(Debug, Clone)]
pub struct StateEnvBuilder {
    path: PathBuf,
    durability: Durability,
    max_readers: u32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Durability {
    /// Default: `Durable` mode (fdatasync on commit). Use in production.
    Durable,
    /// `SafeNoSync` — commit returns after page-table flush but skips fdatasync.
    /// Use only in tests; combined with PLP NVMe this is unsafe on real hosts.
    SafeNoSync,
}

impl StateEnvBuilder {
    pub fn new(path: impl Into<PathBuf>) -> Self {
        Self {
            path: path.into(),
            durability: Durability::Durable,
            max_readers: MAX_READERS,
        }
    }

    pub fn durability(mut self, d: Durability) -> Self {
        self.durability = d;
        self
    }

    pub fn max_readers(mut self, n: u32) -> Self {
        self.max_readers = n;
        self
    }

    pub fn open(self) -> Result<StateEnv, StateError> {
        std::fs::create_dir_all(&self.path)?;

        let mut builder: EnvironmentBuilder<NoWriteMap> = Environment::builder();
        builder
            .set_max_dbs(ALL_TABLES.len())
            .set_max_readers(self.max_readers)
            .set_geometry(Geometry {
                size: Some(SIZE_LOWER..SIZE_UPPER),
                growth_step: Some(GROWTH_STEP),
                shrink_threshold: Some(SHRINK_STEP),
                page_size: Some(PageSize::Set(PAGE_SIZE)),
            });

        let sync_mode = match self.durability {
            Durability::Durable => SyncMode::Durable,
            Durability::SafeNoSync => SyncMode::SafeNoSync,
        };
        builder.set_flags(libmdbx::EnvironmentFlags {
            mode: Mode::ReadWrite { sync_mode },
            no_sub_dir: false,
            exclusive: false,
            accede: false,
            no_rdahead: true,
            no_meminit: false,
            coalesce: true,
            liforeclaim: true,
        });

        let env = Arc::new(builder.open(&self.path)?);
        // Open every table once so handles are cached in the environment.
        let txn = env.begin_rw_txn()?;
        for name in ALL_TABLES {
            txn.create_db(Some(name), DatabaseFlags::default())?;
        }
        txn.commit()?;

        Ok(StateEnv {
            env,
            path: self.path,
        })
    }
}

/// Shared handle to an open mdbx environment. Cheap to clone.
#[derive(Debug, Clone)]
pub struct StateEnv {
    pub(crate) env: Arc<Environment<NoWriteMap>>,
    pub(crate) path: PathBuf,
}

impl StateEnv {
    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn raw(&self) -> &Environment<NoWriteMap> {
        &self.env
    }
}
```

- [ ] **Step 2: Build**

```bash
cd /home/dev/kardamom && cargo build -p kardamom-state
```

Expected: clean. If `libmdbx` symbol names differ (the 0.6 API uses `NoWriteMap` vs `WriteMap`; `EnvironmentFlags` has private fields exposed via `set_flags`), adjust to match the actual exports — look at `cargo doc --open -p libmdbx` and reconcile.

- [ ] **Step 3: Add an open-close smoke test in `tests/env_smoke.rs`**

```rust
use kardamom_state::env::{Durability, StateEnvBuilder};
use kardamom_state::schema::ALL_TABLES;

#[test]
fn env_opens_and_closes() {
    let tmp = tempfile::tempdir().unwrap();
    let env = StateEnvBuilder::new(tmp.path())
        .durability(Durability::SafeNoSync)
        .open()
        .unwrap();
    // Verify every table is openable via a read txn.
    let txn = env.raw().begin_ro_txn().unwrap();
    for name in ALL_TABLES {
        txn.open_db(Some(name)).unwrap();
    }
    drop(txn);
}
```

- [ ] **Step 4: Run**

```bash
cd /home/dev/kardamom && cargo test -p kardamom-state --test env_smoke
```

Expected: pass.

- [ ] **Step 5: Commit**

```bash
git add crates/kardamom-state/src/env.rs crates/kardamom-state/tests/env_smoke.rs
git commit -m "state: open mdbx env with v0 geometry; add smoke test"
```

---

## Task 10: `snapshot.rs` — RO snapshot + `StateDatabase` impl

**Files:**
- Modify: `crates/kardamom-state/src/snapshot.rs`

- [ ] **Step 1: Write the file**

```rust
//! Read-only snapshot: long-lived mdbx RO txn that backs `StateDatabase`.
//!
//! The mdbx RO txn is the MVCC anchor. As long as one of these is alive, the
//! mdbx freelist will not reuse the pages reachable from that snapshot — see
//! `geometry::HORIZON_BLOCKS` for the bound the writer enforces.

use std::sync::Arc;

use alloy_primitives::{Address, B256, U256};
use kardamom_types::{AccountRecord, StateDatabase, StateDatabaseError};
use libmdbx::{NoWriteMap, RO, Transaction};
use revm::primitives::Bytes;

use crate::env::StateEnv;
use crate::error::StateError;
use crate::meta::{decode_u64, KEY_LAST_COMMITTED_BLOCK};
use crate::schema::{
    decode_account_value, decode_storage_value, encode_account_key, encode_code_key,
    encode_storage_key, TABLE_ACCOUNTS, TABLE_CODE, TABLE_META, TABLE_STORAGE,
};

/// MVCC snapshot of the state DB at exactly one block boundary.
///
/// Holds the underlying RO txn for its full lifetime. Drop it to release the
/// snapshot — which the writer's horizon check uses to know it can reclaim
/// older pages.
#[derive(Clone)]
pub struct StateSnapshot {
    inner: Arc<SnapshotInner>,
}

struct SnapshotInner {
    txn: Transaction<RO, NoWriteMap>,
    block_number: u64,
}

impl StateSnapshot {
    pub(crate) fn open(env: &StateEnv) -> Result<Self, StateError> {
        let txn = env.raw().begin_ro_txn()?;
        let meta = txn.open_db(Some(TABLE_META))?;
        let block_number = match txn.get(meta.dbi(), KEY_LAST_COMMITTED_BLOCK)? {
            Some(bytes) => decode_u64(&bytes)?,
            None => 0,
        };
        Ok(Self {
            inner: Arc::new(SnapshotInner { txn, block_number }),
        })
    }
}

impl StateDatabase for StateSnapshot {
    fn block_number(&self) -> u64 {
        self.inner.block_number
    }

    fn account(
        &self,
        address: Address,
    ) -> Result<Option<AccountRecord>, StateDatabaseError> {
        let key = encode_account_key(address);
        let db = self
            .inner
            .txn
            .open_db(Some(TABLE_ACCOUNTS))
            .map_err(|e| StateDatabaseError::Backend(e.to_string()))?;
        match self
            .inner
            .txn
            .get(db.dbi(), &key)
            .map_err(|e| StateDatabaseError::Backend(e.to_string()))?
        {
            None => Ok(None),
            Some(bytes) => {
                let v = decode_account_value(&bytes).map_err(StateDatabaseError::from)?;
                Ok(Some(AccountRecord {
                    nonce: v.nonce,
                    balance: v.balance,
                    code_hash: v.code_hash,
                    storage_root: v.storage_root,
                }))
            }
        }
    }

    fn storage(&self, address: Address, key: U256) -> Result<U256, StateDatabaseError> {
        let composite = encode_storage_key(address, key);
        let db = self
            .inner
            .txn
            .open_db(Some(TABLE_STORAGE))
            .map_err(|e| StateDatabaseError::Backend(e.to_string()))?;
        match self
            .inner
            .txn
            .get(db.dbi(), &composite)
            .map_err(|e| StateDatabaseError::Backend(e.to_string()))?
        {
            None => Ok(U256::ZERO),
            Some(bytes) => decode_storage_value(&bytes).map_err(StateDatabaseError::from),
        }
    }

    fn code_by_hash(
        &self,
        code_hash: B256,
    ) -> Result<Option<Bytes>, StateDatabaseError> {
        let key = encode_code_key(code_hash);
        let db = self
            .inner
            .txn
            .open_db(Some(TABLE_CODE))
            .map_err(|e| StateDatabaseError::Backend(e.to_string()))?;
        Ok(self
            .inner
            .txn
            .get(db.dbi(), &key)
            .map_err(|e| StateDatabaseError::Backend(e.to_string()))?
            .map(|b| Bytes::copy_from_slice(&b)))
    }
}
```

- [ ] **Step 2: Build**

```bash
cd /home/dev/kardamom && cargo build -p kardamom-state
```

Expected: clean. If `libmdbx::Transaction::get`'s return type is `Result<Option<Cow<[u8]>>>` rather than `Result<Option<Vec<u8>>>`, adjust `bytes.as_ref()` accordingly — both forms compile against the current API; the `&bytes[..]` indexing in `decode_*` accepts either.

- [ ] **Step 3: Commit**

```bash
git add crates/kardamom-state/src/snapshot.rs
git commit -m "state: implement StateSnapshot + StateDatabase impl"
```

---

## Task 11: `swap.rs` — snapshot-swap channel

**Files:**
- Modify: `crates/kardamom-state/src/swap.rs`

- [ ] **Step 1: Write the file**

```rust
//! Snapshot-swap protocol (§5).
//!
//! The writer publishes a fresh `StateSnapshot` after every successful RW
//! commit. Consumers (the executor) watch the channel and atomically swap
//! their `MV-memory` underlying snapshot to the new one. Old snapshots are
//! dropped, which releases the mdbx RO txn and lets the freelist reclaim
//! the corresponding pages.
//!
//! Implementation: a `tokio::sync::watch` would work but we want zero async
//! on the executor's hot path, so we use `crossbeam_channel::Sender`-of-1 in
//! "replace" mode — keep only the latest snapshot in flight.

use std::sync::{Arc, Mutex};

use crate::snapshot::StateSnapshot;

/// Producer side. The writer calls `publish(snapshot)` after every commit.
#[derive(Clone)]
pub struct SnapshotHandle {
    latest: Arc<Mutex<Option<StateSnapshot>>>,
    notify: crossbeam_channel::Sender<()>,
}

/// Consumer side. The executor calls `recv()` to block on the next snapshot,
/// or `current()` to peek without blocking.
#[derive(Clone)]
pub struct SnapshotReceiver {
    latest: Arc<Mutex<Option<StateSnapshot>>>,
    notify: crossbeam_channel::Receiver<()>,
}

pub fn channel() -> (SnapshotHandle, SnapshotReceiver) {
    let latest = Arc::new(Mutex::new(None));
    let (tx, rx) = crossbeam_channel::bounded(1);
    (
        SnapshotHandle {
            latest: latest.clone(),
            notify: tx,
        },
        SnapshotReceiver { latest, notify: rx },
    )
}

impl SnapshotHandle {
    /// Replace the latest snapshot. Drops any prior unconsumed snapshot,
    /// which releases its mdbx RO txn — exactly the desired behavior since
    /// the consumer only ever needs the freshest one.
    pub fn publish(&self, snapshot: StateSnapshot) {
        *self.latest.lock().expect("snapshot mutex poisoned") = Some(snapshot);
        // try_send: if the slot is full, the receiver has not consumed yet —
        // the latest-pointer update above is sufficient.
        let _ = self.notify.try_send(());
    }
}

impl SnapshotReceiver {
    /// Non-blocking peek at the most recently published snapshot.
    pub fn current(&self) -> Option<StateSnapshot> {
        self.latest.lock().expect("snapshot mutex poisoned").clone()
    }

    /// Blocks until a new snapshot is published, then returns it. Returns
    /// `None` if the writer has been dropped.
    pub fn recv(&self) -> Option<StateSnapshot> {
        self.notify.recv().ok()?;
        self.current()
    }
}

#[cfg(test)]
mod tests {
    // Real swap behavior is tested in tests/snapshot_swap.rs (needs a live env).
    // Here we only test that the channel mechanics don't deadlock.
    use super::*;

    #[test]
    fn drop_writer_closes_recv() {
        let (handle, recv) = channel();
        drop(handle);
        assert!(recv.recv().is_none());
    }
}
```

- [ ] **Step 2: Run tests**

```bash
cd /home/dev/kardamom && cargo test -p kardamom-state swap
```

Expected: 1 unit test passes.

- [ ] **Step 3: Commit**

```bash
git add crates/kardamom-state/src/swap.rs
git commit -m "state: add snapshot-swap channel mechanism"
```

---

## Task 12: `writer.rs` — single-writer thread

**Files:**
- Modify: `crates/kardamom-state/src/writer.rs`

- [ ] **Step 1: Write the file**

```rust
//! Single writer thread: drains BlockDelta channel, commits one mdbx RW txn
//! per block boundary, publishes new snapshots.

use std::sync::Arc;
use std::thread::{self, JoinHandle};

use crossbeam_channel::{Receiver, Sender};
use libmdbx::{NoWriteMap, Transaction, WriteFlags, RW};
use tracing::{debug, error, info, warn};

use crate::delta::BlockDelta;
use crate::env::StateEnv;
use crate::error::StateError;
use crate::meta::{
    encode_b_position, encode_u32, encode_u64, KEY_LAST_COMMITTED_BLOCK,
    KEY_LAST_COMMITTED_END_TX_IDX, KEY_LAST_FSYNCED_B_POSITION, KEY_SCHEMA_VERSION,
    SCHEMA_VERSION,
};
use crate::schema::{
    encode_account_key, encode_account_value, encode_block_key, encode_code_key,
    encode_header_value, encode_storage_key, encode_storage_value, encode_tx_key, AccountValue,
    HeaderValue, TABLE_ACCOUNTS, TABLE_CODE, TABLE_HEADERS, TABLE_META, TABLE_RECEIPTS,
    TABLE_STORAGE,
};
use crate::snapshot::StateSnapshot;
use crate::swap::{channel as swap_channel, SnapshotHandle, SnapshotReceiver};

/// Handle returned by `StateWriter::spawn`. Drop to stop the writer thread
/// (which closes the delta sender and joins on the next loop iteration).
pub struct WriterHandle {
    pub delta_tx: Sender<BlockDelta>,
    pub snapshot_rx: SnapshotReceiver,
    join: Option<JoinHandle<Result<(), StateError>>>,
}

impl WriterHandle {
    /// Stop the writer and wait for its thread to exit. Returns the writer's
    /// final result.
    pub fn shutdown(mut self) -> Result<(), StateError> {
        drop(self.delta_tx);
        match self.join.take() {
            Some(j) => j.join().expect("writer thread panicked"),
            None => Ok(()),
        }
    }
}

pub struct StateWriter {
    env: StateEnv,
    delta_rx: Receiver<BlockDelta>,
    snapshot_handle: SnapshotHandle,
}

impl StateWriter {
    /// Spawn the writer on a dedicated OS thread.
    pub fn spawn(env: StateEnv) -> Result<WriterHandle, StateError> {
        // Bounded channel: HORIZON_BLOCKS deep. If the writer falls behind by
        // more than the version horizon, the executor will block here — at
        // which point the snapshot it holds is about to be invalidated anyway,
        // so blocking is the correct fail-fast.
        let (delta_tx, delta_rx) =
            crossbeam_channel::bounded(crate::geometry::HORIZON_BLOCKS as usize);
        let (snapshot_handle, snapshot_rx) = swap_channel();

        // Write the schema-version meta key on first start (and verify it on
        // subsequent starts).
        ensure_schema_version(&env)?;

        // Publish an initial snapshot at the current cursors.
        let initial = StateSnapshot::open(&env)?;
        snapshot_handle.publish(initial);

        let writer = StateWriter {
            env: env.clone(),
            delta_rx,
            snapshot_handle: snapshot_handle.clone(),
        };

        let join = thread::Builder::new()
            .name("kardamom-state-writer".into())
            .spawn(move || writer.run())?;

        Ok(WriterHandle {
            delta_tx,
            snapshot_rx,
            join: Some(join),
        })
    }

    fn run(self) -> Result<(), StateError> {
        info!(path = %self.env.path().display(), "state writer started");
        loop {
            let delta = match self.delta_rx.recv() {
                Ok(d) => d,
                Err(_) => {
                    info!("delta channel closed; writer shutting down");
                    return Ok(());
                }
            };
            let block = delta.boundary.block_number;
            let size = delta.approx_size_bytes();
            debug!(block, size_bytes = size, "applying block delta");
            if let Err(e) = self.apply(&delta) {
                error!(block, error = %e, "block apply failed; halting writer");
                return Err(e);
            }
            // Publish the post-N snapshot. Old snapshot is dropped inside
            // SnapshotHandle::publish, which releases its RO txn.
            match StateSnapshot::open(&self.env) {
                Ok(snap) => self.snapshot_handle.publish(snap),
                Err(e) => {
                    warn!(block, error = %e, "snapshot open failed after commit");
                    return Err(e);
                }
            }
        }
    }

    fn apply(&self, delta: &BlockDelta) -> Result<(), StateError> {
        let txn: Transaction<RW, NoWriteMap> = self.env.raw().begin_rw_txn()?;

        let accounts = txn.open_db(Some(TABLE_ACCOUNTS))?;
        let storage = txn.open_db(Some(TABLE_STORAGE))?;
        let code = txn.open_db(Some(TABLE_CODE))?;
        let headers = txn.open_db(Some(TABLE_HEADERS))?;
        let receipts = txn.open_db(Some(TABLE_RECEIPTS))?;
        let meta = txn.open_db(Some(TABLE_META))?;

        // --- accounts ---
        for change in &delta.accounts.0 {
            let key = encode_account_key(change.address);
            match &change.new_state {
                Some(s) => {
                    let v = AccountValue {
                        nonce: s.nonce,
                        balance: s.balance,
                        code_hash: s.code_hash,
                        storage_root: s.storage_root,
                    };
                    txn.put(accounts.dbi(), &key, &encode_account_value(&v), WriteFlags::UPSERT)?;
                }
                None => {
                    // delete; ignore NOTFOUND
                    let _ = txn.del(accounts.dbi(), &key, None);
                }
            }
        }

        // --- storage ---
        for change in &delta.storage.0 {
            let key = encode_storage_key(change.address, change.key);
            match change.value {
                Some(v) => {
                    txn.put(storage.dbi(), &key, &encode_storage_value(v), WriteFlags::UPSERT)?;
                }
                None => {
                    let _ = txn.del(storage.dbi(), &key, None);
                }
            }
        }

        // --- code ---
        for entry in &delta.code.0 {
            let key = encode_code_key(entry.code_hash);
            // code is content-addressed; NO_OVERWRITE is safe and saves a write.
            match txn.put(code.dbi(), &key, &entry.bytecode, WriteFlags::NO_OVERWRITE) {
                Ok(()) => {}
                Err(libmdbx::Error::KeyExist) => {} // duplicate code, fine
                Err(e) => return Err(e.into()),
            }
        }

        // --- headers ---
        let header = HeaderValue {
            state_root_commitment: delta.boundary.state_root_commitment,
            end_tx_idx: delta.boundary.end_tx_idx,
            l2_timestamp: delta.boundary.l2_timestamp,
        };
        txn.put(
            headers.dbi(),
            &encode_block_key(delta.boundary.block_number),
            &encode_header_value(&header),
            WriteFlags::UPSERT,
        )?;

        // --- receipts ---
        for r in &delta.receipts {
            txn.put(
                receipts.dbi(),
                &encode_tx_key(r.tx_idx),
                &r.rlp,
                WriteFlags::UPSERT,
            )?;
        }

        // --- meta cursors (last) ---
        txn.put(
            meta.dbi(),
            KEY_LAST_COMMITTED_BLOCK,
            &encode_u64(delta.boundary.block_number),
            WriteFlags::UPSERT,
        )?;
        txn.put(
            meta.dbi(),
            KEY_LAST_COMMITTED_END_TX_IDX,
            &encode_u64(delta.boundary.end_tx_idx),
            WriteFlags::UPSERT,
        )?;
        txn.put(
            meta.dbi(),
            KEY_LAST_FSYNCED_B_POSITION,
            &encode_b_position(delta.end_b_position),
            WriteFlags::UPSERT,
        )?;

        txn.commit()?;
        Ok(())
    }
}

fn ensure_schema_version(env: &StateEnv) -> Result<(), StateError> {
    let txn = env.raw().begin_rw_txn()?;
    let meta = txn.open_db(Some(TABLE_META))?;
    match txn.get(meta.dbi(), KEY_SCHEMA_VERSION)? {
        None => {
            txn.put(
                meta.dbi(),
                KEY_SCHEMA_VERSION,
                &encode_u32(SCHEMA_VERSION),
                WriteFlags::UPSERT,
            )?;
        }
        Some(bytes) => {
            let on_disk = crate::meta::decode_u32(&bytes)?;
            if on_disk != SCHEMA_VERSION {
                txn.abort();
                return Err(StateError::Recovery(format!(
                    "schema version mismatch: on-disk={on_disk}, code={SCHEMA_VERSION}"
                )));
            }
        }
    }
    txn.commit()?;
    Ok(())
}
```

- [ ] **Step 2: Build**

```bash
cd /home/dev/kardamom && cargo build -p kardamom-state
```

Expected: clean. Any `libmdbx::Transaction` method-name mismatches (e.g. `del` vs `delete`, the exact `WriteFlags` enum casing) need a docs check and a one-line fix.

- [ ] **Step 3: Commit**

```bash
git add crates/kardamom-state/src/writer.rs
git commit -m "state: implement StateWriter single-writer thread"
```

---

## Task 13: `recovery.rs` — cold start

**Files:**
- Modify: `crates/kardamom-state/src/recovery.rs`

- [ ] **Step 1: Write the file**

```rust
//! Cold-start recovery (§5).
//!
//! On startup the writer (a) opens (or creates) the env, (b) reads the meta
//! cursors, (c) opens an initial snapshot, (d) emits a `RecoveryPoint` that
//! tells the executor where to resume reading B from.
//!
//! Recovery itself is read-only — no replay logic lives in this crate; the
//! executor consumes B from `recovery_point.last_fsynced_b_position` and
//! re-derives any blocks the writer never got to commit.

use kardamom_types::BPosition;

use crate::env::StateEnv;
use crate::error::StateError;
use crate::meta::{
    decode_b_position, decode_u64, DurableCursors, KEY_LAST_COMMITTED_BLOCK,
    KEY_LAST_COMMITTED_END_TX_IDX, KEY_LAST_FSYNCED_B_POSITION,
};
use crate::schema::TABLE_META;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RecoveryPoint {
    pub last_committed_block: u64,
    pub last_committed_end_tx_idx: u64,
    pub last_fsynced_b_position: BPosition,
}

pub fn read_recovery_point(env: &StateEnv) -> Result<RecoveryPoint, StateError> {
    let txn = env.raw().begin_ro_txn()?;
    let meta = txn.open_db(Some(TABLE_META))?;

    let last_committed_block = match txn.get(meta.dbi(), KEY_LAST_COMMITTED_BLOCK)? {
        Some(b) => decode_u64(&b)?,
        None => 0,
    };
    let last_committed_end_tx_idx = match txn.get(meta.dbi(), KEY_LAST_COMMITTED_END_TX_IDX)? {
        Some(b) => decode_u64(&b)?,
        None => 0,
    };
    let last_fsynced_b_position = match txn.get(meta.dbi(), KEY_LAST_FSYNCED_B_POSITION)? {
        Some(b) => decode_b_position(&b)?,
        None => BPosition {
            term_id: 0,
            term_offset: 0,
        },
    };

    Ok(RecoveryPoint {
        last_committed_block,
        last_committed_end_tx_idx,
        last_fsynced_b_position,
    })
}

impl From<RecoveryPoint> for DurableCursors {
    fn from(p: RecoveryPoint) -> Self {
        DurableCursors {
            last_committed_block: p.last_committed_block,
            last_committed_end_tx_idx: p.last_committed_end_tx_idx,
            last_fsynced_b_position: p.last_fsynced_b_position,
            schema_version: crate::meta::SCHEMA_VERSION,
        }
    }
}
```

- [ ] **Step 2: Build**

```bash
cd /home/dev/kardamom && cargo build -p kardamom-state
```

Expected: clean.

- [ ] **Step 3: Commit**

```bash
git add crates/kardamom-state/src/recovery.rs
git commit -m "state: implement cold-start recovery point read"
```

---

## Task 14: `compaction.rs` — `mdbx_env_copy_compact` daemon

**Files:**
- Modify: `crates/kardamom-state/src/compaction.rs`

- [ ] **Step 1: Write the file**

```rust
//! Scheduled libmdbx compaction (§5).
//!
//! libmdbx is copy-on-write; long-running databases fragment. The plan is:
//!
//! 1. Open a hot mirror directory next to the live env.
//! 2. Call `Environment::copy(...)` with the `WithCompacting` flag — this
//!    walks the live env's pages and writes a compacted copy into the mirror.
//! 3. Atomically swap the mirror into the live path (rename + reopen).
//!
//! For v0 this is exposed as a one-shot `compact()` function; an external
//! scheduler (systemd timer, cron) invokes it once per day. A long-running
//! `CompactionDaemon` is left for v1 — there is no benefit to keeping a
//! tokio task alive 23h/day for one operation.

use std::path::Path;

use libmdbx::CopyFlags;
use tracing::{info, warn};

use crate::env::StateEnv;
use crate::error::StateError;

/// Copy-with-compact the env to `dest`. Caller is responsible for swapping
/// `dest` into place if it wants the compacted copy to become the live env.
///
/// The live env stays online for reads and writes throughout — compaction
/// runs against an RO snapshot of the env.
pub fn compact_to(env: &StateEnv, dest: &Path) -> Result<(), StateError> {
    info!(src = %env.path().display(), dst = %dest.display(), "starting compaction");
    if dest.exists() {
        warn!("destination already exists; refusing to overwrite");
        return Err(StateError::Recovery(format!(
            "compaction destination {} already exists",
            dest.display()
        )));
    }
    std::fs::create_dir_all(dest)?;
    env.raw().copy(dest, CopyFlags::COMPACT)?;
    info!("compaction complete");
    Ok(())
}

/// Atomic swap: rename live → live.old, dest → live, then drop the old env.
/// Caller must have closed all `StateEnv` clones before calling — otherwise
/// readers will hold file handles into the renamed directory and observe
/// stale data.
pub fn swap_compacted(live: &Path, dest: &Path) -> Result<(), StateError> {
    let backup = live.with_extension("old");
    if backup.exists() {
        std::fs::remove_dir_all(&backup)?;
    }
    std::fs::rename(live, &backup)?;
    std::fs::rename(dest, live)?;
    Ok(())
}
```

- [ ] **Step 2: Build**

```bash
cd /home/dev/kardamom && cargo build -p kardamom-state
```

Expected: clean. If `libmdbx::CopyFlags::COMPACT` is spelled `CopyFlags::Compact` or similar in the actual 0.6 API, adjust.

- [ ] **Step 3: Add a small smoke test in `tests/compaction_smoke.rs`**

```rust
use kardamom_state::compaction::compact_to;
use kardamom_state::env::{Durability, StateEnvBuilder};

#[test]
fn compact_emits_a_directory() {
    let src_dir = tempfile::tempdir().unwrap();
    let env = StateEnvBuilder::new(src_dir.path())
        .durability(Durability::SafeNoSync)
        .open()
        .unwrap();

    let dst_dir = tempfile::tempdir().unwrap();
    let dst = dst_dir.path().join("compacted");
    compact_to(&env, &dst).unwrap();
    // mdbx writes a `mdbx.dat` file inside the dest directory.
    assert!(dst.join("mdbx.dat").exists(), "expected mdbx.dat in {}", dst.display());
}
```

- [ ] **Step 4: Run**

```bash
cd /home/dev/kardamom && cargo test -p kardamom-state --test compaction_smoke
```

Expected: pass.

- [ ] **Step 5: Commit**

```bash
git add crates/kardamom-state/src/compaction.rs crates/kardamom-state/tests/compaction_smoke.rs
git commit -m "state: add compact_to + swap_compacted with smoke test"
```

---

## Task 15: Re-enable full lib.rs re-exports

**Files:**
- Modify: `crates/kardamom-state/src/lib.rs`

- [ ] **Step 1: Write the full lib.rs**

```rust
//! libmdbx-backed L2 state DB.
//!
//! See `docs/specs/2026-05-23-high-throughput-sequencer-design.md` §5.

pub mod compaction;
pub mod delta;
pub mod env;
pub mod error;
pub mod geometry;
pub mod meta;
pub mod recovery;
pub mod schema;
pub mod snapshot;
pub mod swap;
pub mod writer;

pub use delta::{AccountChange, AccountChanges, BlockDelta, CodeChanges, CodeEntry, NewAccountState, StorageChange, StorageChanges};
pub use env::{Durability, StateEnv, StateEnvBuilder};
pub use error::StateError;
pub use recovery::{read_recovery_point, RecoveryPoint};
pub use snapshot::StateSnapshot;
pub use swap::{channel as snapshot_channel, SnapshotHandle, SnapshotReceiver};
pub use writer::{StateWriter, WriterHandle};
```

- [ ] **Step 2: Build the whole workspace**

```bash
cd /home/dev/kardamom && cargo build --workspace
```

Expected: clean across all existing crates.

- [ ] **Step 3: Commit**

```bash
git add crates/kardamom-state/src/lib.rs
git commit -m "state: re-export public surface from lib.rs"
```

---

## Task 16: Integration test — `tests/common/mod.rs`

**Files:**
- Modify: `crates/kardamom-state/tests/common/mod.rs`

- [ ] **Step 1: Write the helpers**

```rust
//! Shared test helpers: build deltas, open an env, drive the writer.

use alloy_primitives::{Address, B256, U256};
use kardamom_state::{
    BlockDelta, AccountChange, AccountChanges, CodeChanges, NewAccountState,
    StateEnvBuilder, StateWriter, StorageChange, StorageChanges, WriterHandle,
};
use kardamom_state::env::Durability;
use kardamom_types::{BPosition, BlockBoundary, Receipt};

pub fn open_tmp_writer() -> (tempfile::TempDir, WriterHandle) {
    let dir = tempfile::tempdir().unwrap();
    let env = StateEnvBuilder::new(dir.path())
        .durability(Durability::SafeNoSync)
        .open()
        .unwrap();
    let writer = StateWriter::spawn(env).unwrap();
    (dir, writer)
}

pub fn simple_delta(block: u64, addr: Address, balance: u64, slot: u64, slot_value: u64) -> BlockDelta {
    BlockDelta {
        boundary: BlockBoundary {
            block_number: block,
            end_tx_idx: block * 10,
            l2_timestamp: 1_700_000_000 + block,
            state_root_commitment: B256::ZERO,
        },
        end_b_position: BPosition {
            term_id: 0,
            term_offset: (block * 1024) as u32,
        },
        accounts: AccountChanges(vec![AccountChange {
            address: addr,
            new_state: Some(NewAccountState {
                nonce: block,
                balance: U256::from(balance),
                code_hash: B256::ZERO,
                storage_root: B256::ZERO,
            }),
        }]),
        storage: StorageChanges(vec![StorageChange {
            address: addr,
            key: U256::from(slot),
            value: Some(U256::from(slot_value)),
        }]),
        code: CodeChanges(vec![]),
        receipts: vec![Receipt {
            tx_idx: block * 10,
            rlp: vec![0xab, 0xcd],
        }],
    }
}
```

- [ ] **Step 2: Build**

```bash
cd /home/dev/kardamom && cargo build -p kardamom-state --tests
```

Expected: clean.

- [ ] **Step 3: Commit**

```bash
git add crates/kardamom-state/tests/common/mod.rs
git commit -m "state(test): add test helpers"
```

---

## Task 17: Integration test — `tests/write_replay.rs`

**Files:**
- Create: `crates/kardamom-state/tests/write_replay.rs`

- [ ] **Step 1: Write the test**

```rust
//! Apply a synthetic stream of block deltas and assert the post-replay state
//! matches the expected per-key values.

mod common;

use alloy_primitives::{address, U256};
use kardamom_state::StateSnapshot;
use kardamom_types::StateDatabase;

#[test]
fn writer_applies_deltas_and_state_reflects_them() {
    let (_dir, writer) = common::open_tmp_writer();
    let addr = address!("00000000000000000000000000000000000000aa");

    for block in 1..=5u64 {
        writer
            .delta_tx
            .send(common::simple_delta(block, addr, 1000 + block, 7, block * 100))
            .unwrap();
    }

    // Drain writer: shutdown waits for the thread.
    // But we want the post-block-5 snapshot before shutdown — block on it via
    // the snapshot_rx, which the writer publishes after every commit.
    let mut latest: Option<StateSnapshot> = None;
    for _ in 0..5 {
        latest = writer.snapshot_rx.recv();
    }
    let snap = latest.expect("at least one snapshot");
    assert_eq!(snap.block_number(), 5);

    let acct = snap.account(addr).unwrap().expect("account exists");
    assert_eq!(acct.balance, U256::from(1005u64));
    assert_eq!(acct.nonce, 5);

    let slot = snap.storage(addr, U256::from(7u64)).unwrap();
    assert_eq!(slot, U256::from(500u64));

    writer.shutdown().unwrap();
}
```

- [ ] **Step 2: Run**

```bash
cd /home/dev/kardamom && cargo test -p kardamom-state --test write_replay
```

Expected: pass.

- [ ] **Step 3: Commit**

```bash
git add crates/kardamom-state/tests/write_replay.rs
git commit -m "state(test): write/replay integration test"
```

---

## Task 18: Integration test — `tests/snapshot_mvcc.rs`

**Files:**
- Create: `crates/kardamom-state/tests/snapshot_mvcc.rs`

- [ ] **Step 1: Write the test**

```rust
//! MVCC invariant: a snapshot opened before write N still reads pre-N values
//! after the writer has committed N.

mod common;

use alloy_primitives::{address, U256};
use kardamom_types::StateDatabase;

#[test]
fn pre_n_snapshot_keeps_pre_n_view() {
    let (_dir, writer) = common::open_tmp_writer();
    let addr = address!("00000000000000000000000000000000000000aa");

    // Apply block 1.
    writer
        .delta_tx
        .send(common::simple_delta(1, addr, 100, 7, 999))
        .unwrap();
    // Wait for the post-block-1 snapshot.
    let snap_at_1 = writer.snapshot_rx.recv().unwrap();
    assert_eq!(
        snap_at_1.account(addr).unwrap().unwrap().balance,
        U256::from(101u64)
    );
    assert_eq!(snap_at_1.storage(addr, U256::from(7u64)).unwrap(), U256::from(999u64));

    // Apply block 2 — overwrites the slot.
    writer
        .delta_tx
        .send(common::simple_delta(2, addr, 200, 7, 12345))
        .unwrap();
    let snap_at_2 = writer.snapshot_rx.recv().unwrap();

    // The OLD snapshot must still see the OLD values.
    assert_eq!(
        snap_at_1.storage(addr, U256::from(7u64)).unwrap(),
        U256::from(999u64),
        "pre-N snapshot must still see pre-N storage value"
    );
    assert_eq!(
        snap_at_1.account(addr).unwrap().unwrap().balance,
        U256::from(101u64),
        "pre-N snapshot must still see pre-N account balance"
    );

    // The NEW snapshot sees the NEW values.
    assert_eq!(
        snap_at_2.storage(addr, U256::from(7u64)).unwrap(),
        U256::from(12345u64)
    );
    assert_eq!(
        snap_at_2.account(addr).unwrap().unwrap().balance,
        U256::from(202u64)
    );

    writer.shutdown().unwrap();
}
```

- [ ] **Step 2: Run**

```bash
cd /home/dev/kardamom && cargo test -p kardamom-state --test snapshot_mvcc
```

Expected: pass.

- [ ] **Step 3: Commit**

```bash
git add crates/kardamom-state/tests/snapshot_mvcc.rs
git commit -m "state(test): MVCC pre-N snapshot keeps pre-N view"
```

---

## Task 19: Integration test — `tests/snapshot_swap.rs`

**Files:**
- Create: `crates/kardamom-state/tests/snapshot_swap.rs`

- [ ] **Step 1: Write the test**

```rust
//! Snapshot-swap protocol: each commit publishes exactly one new snapshot
//! that exposes the post-commit view.

mod common;

use alloy_primitives::address;
use kardamom_types::StateDatabase;

#[test]
fn each_commit_publishes_a_post_commit_snapshot() {
    let (_dir, writer) = common::open_tmp_writer();
    let addr = address!("00000000000000000000000000000000000000aa");

    // Initial snapshot at block 0 published on spawn.
    let snap0 = writer.snapshot_rx.recv().unwrap();
    assert_eq!(snap0.block_number(), 0);
    assert!(snap0.account(addr).unwrap().is_none());

    // Apply block 1.
    writer
        .delta_tx
        .send(common::simple_delta(1, addr, 100, 0, 0))
        .unwrap();
    let snap1 = writer.snapshot_rx.recv().unwrap();
    assert_eq!(snap1.block_number(), 1);
    assert!(snap1.account(addr).unwrap().is_some());

    // Apply block 2.
    writer
        .delta_tx
        .send(common::simple_delta(2, addr, 200, 0, 0))
        .unwrap();
    let snap2 = writer.snapshot_rx.recv().unwrap();
    assert_eq!(snap2.block_number(), 2);

    writer.shutdown().unwrap();
}
```

- [ ] **Step 2: Run**

```bash
cd /home/dev/kardamom && cargo test -p kardamom-state --test snapshot_swap
```

Expected: pass.

- [ ] **Step 3: Commit**

```bash
git add crates/kardamom-state/tests/snapshot_swap.rs
git commit -m "state(test): snapshot swap exposes post-N view"
```

---

## Task 20: Integration test — `tests/recovery_midblock.rs`

**Files:**
- Create: `crates/kardamom-state/tests/recovery_midblock.rs`

- [ ] **Step 1: Write the test**

```rust
//! Recovery: drop the writer mid-stream (after some commits); re-open the
//! env; assert the recovery point matches the last committed block and that
//! the post-recovery snapshot still serves the right values.

mod common;

use alloy_primitives::{address, U256};
use kardamom_state::{
    env::{Durability, StateEnvBuilder},
    read_recovery_point, StateSnapshot, StateWriter,
};
use kardamom_types::StateDatabase;

#[test]
fn recovery_point_matches_last_committed_block() {
    let dir = tempfile::tempdir().unwrap();
    let addr = address!("00000000000000000000000000000000000000aa");

    // --- run 1: commit blocks 1..=3, then drop ---
    {
        let env = StateEnvBuilder::new(dir.path())
            .durability(Durability::SafeNoSync)
            .open()
            .unwrap();
        let writer = StateWriter::spawn(env).unwrap();
        for block in 1..=3u64 {
            writer
                .delta_tx
                .send(common::simple_delta(block, addr, 1000 + block, 7, block * 100))
                .unwrap();
        }
        // Drain three snapshots so we know the commits landed.
        for _ in 0..3 {
            writer.snapshot_rx.recv().unwrap();
        }
        // Trigger a "mid-block" crash by sending a 4th delta but not waiting
        // for it: shutdown closes the channel and joins; the writer may or
        // may not have started txn 4 by the time it sees the close. The
        // important invariant is that ANY uncommitted txn is discarded by
        // mdbx — commit() is atomic.
        let _ = writer
            .delta_tx
            .send(common::simple_delta(4, addr, 9999, 7, 9999));
        writer.shutdown().unwrap();
    }

    // --- run 2: re-open, assert recovery point ---
    let env = StateEnvBuilder::new(dir.path())
        .durability(Durability::SafeNoSync)
        .open()
        .unwrap();

    let rp = read_recovery_point(&env).unwrap();
    // The writer may have committed block 4 before shutdown. Both 3 and 4 are
    // acceptable outcomes; the invariant is that whichever block is "last
    // committed", the snapshot is internally consistent for that block.
    assert!(rp.last_committed_block == 3 || rp.last_committed_block == 4);

    let snap = StateSnapshot::open(&env).unwrap();
    let acct = snap.account(addr).unwrap().unwrap();
    // For block 3: balance = 1003, for block 4: 9999.
    assert!(acct.balance == U256::from(1003u64) || acct.balance == U256::from(9999u64));
    // No torn writes: the slot value matches the same block as the balance.
    let slot = snap.storage(addr, U256::from(7u64)).unwrap();
    if acct.balance == U256::from(1003u64) {
        assert_eq!(slot, U256::from(300u64), "block 3 consistency");
    } else {
        assert_eq!(slot, U256::from(9999u64), "block 4 consistency");
    }
}
```

- [ ] **Step 2: Run**

```bash
cd /home/dev/kardamom && cargo test -p kardamom-state --test recovery_midblock
```

Expected: pass.

- [ ] **Step 3: Commit**

```bash
git add crates/kardamom-state/tests/recovery_midblock.rs
git commit -m "state(test): recovery from mid-block drop"
```

---

## Task 21: Integration test — `tests/concurrent_readers.rs`

**Files:**
- Create: `crates/kardamom-state/tests/concurrent_readers.rs`

- [ ] **Step 1: Write the test**

```rust
//! Four reader threads each hold a snapshot at a different block; each
//! continuously reads its frozen view; the writer commits more blocks
//! concurrently. Assert each reader sees only its own view and that no
//! panics or page-reuse-during-read occurs.

mod common;

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread;
use std::time::Duration;

use alloy_primitives::{address, U256};
use kardamom_types::StateDatabase;

#[test]
fn four_readers_with_distinct_snapshots() {
    let (_dir, writer) = common::open_tmp_writer();
    let addr = address!("00000000000000000000000000000000000000aa");

    // Pre-load 4 blocks; capture a snapshot after each.
    let mut snapshots = Vec::new();
    for block in 1..=4u64 {
        writer
            .delta_tx
            .send(common::simple_delta(block, addr, 1000 + block, 7, block * 100))
            .unwrap();
        snapshots.push(writer.snapshot_rx.recv().unwrap());
    }

    let stop = Arc::new(AtomicBool::new(false));
    let mut handles = Vec::new();
    for (i, snap) in snapshots.into_iter().enumerate() {
        let expected_balance = U256::from(1001 + i as u64);
        let expected_slot = U256::from(((i + 1) as u64) * 100);
        let stop = stop.clone();
        let handle = thread::spawn(move || {
            while !stop.load(Ordering::Relaxed) {
                let acct = snap.account(addr).unwrap().unwrap();
                assert_eq!(acct.balance, expected_balance, "reader {i} saw drift");
                let slot = snap.storage(addr, U256::from(7u64)).unwrap();
                assert_eq!(slot, expected_slot, "reader {i} saw drift");
            }
        });
        handles.push(handle);
    }

    // Concurrently apply blocks 5..=12.
    for block in 5..=12u64 {
        writer
            .delta_tx
            .send(common::simple_delta(block, addr, 1000 + block, 7, block * 100))
            .unwrap();
        writer.snapshot_rx.recv().unwrap();
    }

    // Let readers race for a bit longer.
    thread::sleep(Duration::from_millis(50));
    stop.store(true, Ordering::Relaxed);
    for h in handles {
        h.join().unwrap();
    }
    writer.shutdown().unwrap();
}
```

- [ ] **Step 2: Run**

```bash
cd /home/dev/kardamom && cargo test -p kardamom-state --test concurrent_readers
```

Expected: pass. If the test exceeds `MAX_READERS` because the writer's spawn-time snapshot stays in `snapshot_rx.latest`, bump `max_readers` in the test helper.

- [ ] **Step 3: Commit**

```bash
git add crates/kardamom-state/tests/concurrent_readers.rs
git commit -m "state(test): 4 concurrent readers see frozen views"
```

---

## Task 22: Criterion bench — `benches/write_throughput.rs`

**Files:**
- Create: `crates/kardamom-state/benches/write_throughput.rs`

- [ ] **Step 1: Write the bench**

```rust
//! Measure block-delta write throughput at the spec's target size.
//!
//! Target: 25 MB per block at 4 Hz = 100 MB/s sustained. We do not pace at
//! 4 Hz here — we measure raw apply latency per block.

use std::time::Duration;

use alloy_primitives::{Address, B256, U256};
use criterion::{criterion_group, criterion_main, BatchSize, Criterion};
use kardamom_state::{
    env::{Durability, StateEnvBuilder},
    AccountChange, AccountChanges, BlockDelta, CodeChanges, NewAccountState, StateWriter,
    StorageChange, StorageChanges,
};
use kardamom_types::{BPosition, BlockBoundary, Receipt};

fn big_delta(block: u64) -> BlockDelta {
    // ~25 MB target: 100 B per account × 250k accounts. Real workload mixes
    // accounts + storage; we approximate with accounts only for the bench.
    let n = 250_000usize;
    let accounts: Vec<AccountChange> = (0..n)
        .map(|i| {
            let mut bytes = [0u8; 20];
            bytes[..8].copy_from_slice(&(i as u64).to_be_bytes());
            AccountChange {
                address: Address::from(bytes),
                new_state: Some(NewAccountState {
                    nonce: block,
                    balance: U256::from(i as u64),
                    code_hash: B256::ZERO,
                    storage_root: B256::ZERO,
                }),
            }
        })
        .collect();
    BlockDelta {
        boundary: BlockBoundary {
            block_number: block,
            end_tx_idx: block * 1000,
            l2_timestamp: 1_700_000_000 + block,
            state_root_commitment: B256::ZERO,
        },
        end_b_position: BPosition {
            term_id: 0,
            term_offset: (block * 1024) as u32,
        },
        accounts: AccountChanges(accounts),
        storage: StorageChanges(vec![]),
        code: CodeChanges(vec![]),
        receipts: vec![Receipt {
            tx_idx: block * 1000,
            rlp: vec![],
        }],
    }
}

fn bench_apply(c: &mut Criterion) {
    let mut group = c.benchmark_group("state_writer");
    group.measurement_time(Duration::from_secs(10));
    group.sample_size(20);

    group.bench_function("apply_25mb_block", |b| {
        b.iter_batched(
            || {
                let dir = tempfile::tempdir().unwrap();
                let env = StateEnvBuilder::new(dir.path())
                    .durability(Durability::SafeNoSync)
                    .open()
                    .unwrap();
                let writer = StateWriter::spawn(env).unwrap();
                // Drain the initial snapshot.
                writer.snapshot_rx.recv().unwrap();
                (dir, writer)
            },
            |(_dir, writer)| {
                writer.delta_tx.send(big_delta(1)).unwrap();
                writer.snapshot_rx.recv().unwrap();
                writer.shutdown().unwrap();
            },
            BatchSize::LargeInput,
        );
    });

    group.finish();
}

criterion_group!(benches, bench_apply);
criterion_main!(benches);
```

- [ ] **Step 2: Run a smoke iteration (fast mode)**

```bash
cd /home/dev/kardamom && cargo bench -p kardamom-state --bench write_throughput -- --quick
```

Expected: completes; reports a per-iteration time. The per-block latency should be in the tens-of-ms range on a modern NVMe under `SafeNoSync` (durability off for the bench).

- [ ] **Step 3: Commit**

```bash
git add crates/kardamom-state/benches/write_throughput.rs
git commit -m "state(bench): criterion write-throughput bench"
```

---

## Task 23: Criterion bench — `benches/snapshot_open.rs`

**Files:**
- Create: `crates/kardamom-state/benches/snapshot_open.rs`

- [ ] **Step 1: Write the bench**

```rust
//! Measure RO snapshot open latency. Target: <100 µs.

use std::time::Duration;

use criterion::{criterion_group, criterion_main, Criterion};
use kardamom_state::{
    env::{Durability, StateEnvBuilder},
    StateSnapshot,
};

fn bench_snapshot_open(c: &mut Criterion) {
    let dir = tempfile::tempdir().unwrap();
    let env = StateEnvBuilder::new(dir.path())
        .durability(Durability::SafeNoSync)
        .open()
        .unwrap();

    let mut group = c.benchmark_group("state_snapshot");
    group.measurement_time(Duration::from_secs(5));
    group.bench_function("open_ro_txn", |b| {
        b.iter(|| {
            let snap = StateSnapshot::open(&env).unwrap();
            criterion::black_box(snap);
        });
    });
    group.finish();
}

criterion_group!(benches, bench_snapshot_open);
criterion_main!(benches);
```

- [ ] **Step 2: Run**

```bash
cd /home/dev/kardamom && cargo bench -p kardamom-state --bench snapshot_open -- --quick
```

Expected: completes; per-open time should be <100 µs on a quiet host. (If it isn't, the open path is doing more than necessary — investigate before relaxing the target.)

- [ ] **Step 3: Commit**

```bash
git add crates/kardamom-state/benches/snapshot_open.rs
git commit -m "state(bench): criterion snapshot-open latency bench"
```

---

## Task 24: Workspace wiring sanity

**Files:** none modified — verification only.

- [ ] **Step 1: Verify all crates still build**

```bash
cd /home/dev/kardamom && cargo build --workspace --all-targets
```

Expected: clean across the workspace.

- [ ] **Step 2: Run the full workspace test suite**

```bash
cd /home/dev/kardamom && cargo test --workspace
```

Expected: all pre-existing + new tests pass.

- [ ] **Step 3: Run clippy in CI-strict mode**

```bash
cd /home/dev/kardamom && cargo clippy --workspace --all-targets -- -D warnings
```

Expected: clean. Fix any new warnings in the touched crates only.

- [ ] **Step 4: Format check**

```bash
cd /home/dev/kardamom && cargo fmt --all -- --check
```

Expected: clean. If diffs, run `cargo fmt --all` and amend the offending commit.

- [ ] **Step 5: Open the PR (only after the review step)**

Defer to the user — do not auto-open. The plan ends with all changes committed locally on `claude/s6-state-writer`.

---

## Spec coverage cross-check

| Spec item (§5 + §2.7 + §4.6)                                | Task                |
|-------------------------------------------------------------|---------------------|
| `accounts` table                                            | Task 6 (schema) + Task 12 (writer) |
| `storage` flat table                                        | Task 6 + Task 12    |
| `code` content-addressed                                    | Task 6 + Task 12    |
| `headers`                                                   | Task 6 + Task 12    |
| `receipts` (optional)                                       | Task 6 + Task 12    |
| `meta` cursors                                              | Task 7 + Task 12 + Task 13 |
| `StateDatabase` trait (location decided)                    | Task 2 (in `kardamom-types`) |
| RO snapshot impl                                            | Task 10             |
| Snapshot-swap protocol                                      | Task 11 (channel) + Task 12 (writer publishes) |
| Write path consuming local executor channel                 | Task 8 (BlockDelta) + Task 12 (writer) |
| MVCC version horizon sizing                                 | Task 5              |
| Cold-start recovery from meta                               | Task 13             |
| Compaction                                                  | Task 14             |
| Unit: schema codecs                                         | Task 6              |
| Unit: MVCC horizon sizing math                              | Task 5              |
| Unit: meta-cursor read/write                                | Task 7              |
| Integration: synthetic deltas → replay                      | Task 17             |
| Integration: pre-N snapshot reads pre-N values              | Task 18             |
| Integration: post-N swap exposes new values                 | Task 19             |
| Recovery: kill mid-block; restart; no corruption            | Task 20             |
| Concurrent reader test (4 readers)                          | Task 21             |
| Bench: write throughput                                     | Task 22             |
| Bench: snapshot open latency <100 µs                        | Task 23             |
| §4.6 — "fall behind > horizon ⇒ executor sees snapshot exhaustion, halt" | Task 4 (error variant) + Task 5 (horizon doc) + Task 12 (bounded channel == hard backpressure) |

## Open questions for the user / S4 coordinator

1. **`StateDatabase` trait location.** This plan puts it in a brand-new `kardamom-types` crate (Task 1–2) so neither S4 nor S6 depends on the other. If the S4 plan author already picked a different location, collapse Task 1–2 and re-point Task 10's `impl` block accordingly — no other plan steps change.
2. **`BPosition` / `BlockBoundary` / `Receipt` source.** This plan stubs them in `kardamom-types` because S3 is also in-flight. Once S3 lands, S3 must move these stubs into a `kardamom-log` re-export with identical field layouts (or we migrate `kardamom-types` to re-export from `kardamom-log`). Task 2 is the canonical site for the field shapes.
3. **`libmdbx` crate choice.** Plan uses `libmdbx = "0.6"` (vorot93). Alternative is `signet-libmdbx = "0.8"` (init4tech, MIT/Apache-2.0 vs MPL-2.0 — friendlier license, more actively maintained). If the workspace prefers MIT/Apache-only deps for license hygiene, swap the dep and adjust the few `libmdbx::*` import paths in Task 9–14. Behaviour is equivalent.

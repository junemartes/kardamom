# S6 State Writer Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Ship `crates/kardamom-state` — a libmdbx-backed state DB with a snapshot-swap protocol that lets the S4 executor read state without blocking, plus all schema/recovery/compaction machinery called out in §5 of the sequencer design.

**Architecture:** Single-writer, many-reader libmdbx environment. The writer thread drains the local S4 executor's commit-thread channel (a typed `BlockDelta` message per virtual block boundary) and applies it in one `RW` transaction per block. Reads go through a `StateSnapshot` wrapper around a long-lived `RO` transaction; the executor receives a fresh snapshot through the snapshot-swap protocol after every commit. MVCC horizon is sized so any snapshot the executor may still hold (~4 blocks back) is never page-reused. The `StateDatabase` trait that the executor uses to back `revm::Database` is **defined in `crates/kardamom-types`** (per S0 D-Sh1) — this crate only **implements** it for the libmdbx backend.

**Tech Stack:** Rust 2024, `libmdbx = "0.6"` (the `vorot93/libmdbx-rs` binding), `alloy-primitives`, `revm` (consumer only — we expose a `DatabaseRef`-compatible read view), `crossbeam-channel` for the executor↔writer hand-off, `criterion` for benches, `tempfile` for tests.

**Branch:** `claude/s6-state-writer` (branched off `origin/main`). Final PR opens against `main`.

**Reference spec:** `docs/specs/2026-05-23-high-throughput-sequencer-design.md` (§1, §2.4, §2.7, §4.6, §5, V0 scope).

**Assumed interfaces (coordination required):**
- **S0 / `kardamom-types`:** owns `BPosition`, `Receipt`, `BlockBoundary`, and the `StateDatabase` trait (per D-Sh1). **This plan does not redefine any of those types** — every module imports them via `use kardamom_types::{...}`. The `Receipt` shape is `{ tx_idx: BPosition, tx_hash: B256, status: bool, gas_used: u64, logs: Vec<Log>, write_set_hash: B256 }` (D-Sh1). `BlockBoundary` is `{ block_number, end_tx_idx: BPosition, l2_timestamp }` — **no `state_root_commitment` field** (D-Sh11). `BPosition` is `{ term_id: i32, term_offset: i32 }` (D-Sh1).
- **S3 (`kardamom-log`):** provides the Aeron channel implementations that ferry `Receipt` and `BlockBoundary` over tx_receipts. This crate's e2e test (Task 25) drives the real Aeron testcontainer harness exported by `kardamom-log`.
- **S4 (`kardamom-executor`):** emits a `BlockDelta` value (defined in this plan, Task 8) on a `crossbeam::channel::Sender<BlockDelta>` provided by `kardamom-state` at startup. Coordination: S4 imports `kardamom_state::BlockDelta` and uses the channel the state writer creates.
- **`StateDatabase` trait:** **defined in `kardamom-types`** (per D-Sh1, not in this crate). S6 only provides the concrete `impl StateDatabase for StateSnapshot` for the libmdbx backend. S6 also extends the trait surface with `get_tx_position(tx_hash: B256) -> Option<BPosition>` and `get_receipt(position: BPosition) -> Option<Receipt>` (declared in `kardamom-types`, implemented here) so the S1 proxy can serve `eth_getTransactionReceipt(hash)` (D-Sh4).

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

**Note on `crates/kardamom-types/`:** this crate is **owned upstream** (foundation crates landed in the S3+S0 PR per D-Sh7). S6 only **consumes** it. The `StateDatabase` trait and the `BPosition` / `Receipt` / `BlockBoundary` types live there — S6 must not redefine them. The two task-level edits required upstream (so S6 can compile) are: (a) add a `get_tx_position(tx_hash: B256) -> Result<Option<BPosition>, StateDatabaseError>` method to the `StateDatabase` trait, (b) add a `get_receipt(position: BPosition) -> Result<Option<Receipt>, StateDatabaseError>` method to the `StateDatabase` trait. Both are declared in `kardamom-types`; the libmdbx impl below (Task 10) provides the bodies. If those methods are not yet on `main`, this PR includes the trait additions as part of the same change set.

---

## Task 1: Extend the upstream `StateDatabase` trait with receipt-lookup methods

**Files:**
- Modify: `crates/kardamom-types/src/state_database.rs` (upstream — owned by S0/foundation PR)

The `StateDatabase` trait and the `BPosition` / `Receipt` / `BlockBoundary` types are **already defined upstream** in `crates/kardamom-types` (per S0 D-Sh1). This plan must not redefine any of them. We only extend the trait with two methods that the S1 proxy needs for `eth_getTransactionReceipt(hash)` (per S0 D-Sh4):

- [ ] **Step 1: Add `get_tx_position` and `get_receipt` to the trait declaration**

In `crates/kardamom-types/src/state_database.rs`, add to the `StateDatabase` trait:

```rust
use alloy_primitives::B256;
use crate::{BPosition, Receipt};

pub trait StateDatabase: Send + Sync {
    // ... existing methods (block_number, account, storage, code_by_hash) ...

    /// Resolve a tx hash to its canonical `BPosition`. Returns `Ok(None)` if
    /// the tx is not yet committed to state (or never was). Backed by the
    /// libmdbx `tx_hash_index` table (see S6 Task 6 schema).
    fn get_tx_position(
        &self,
        tx_hash: B256,
    ) -> Result<Option<BPosition>, StateDatabaseError>;

    /// Fetch the `Receipt` previously committed at `position`. Returns
    /// `Ok(None)` if no receipt exists at that key (e.g. position was a
    /// system marker, not a tx). Backed by the libmdbx `receipts` table.
    fn get_receipt(
        &self,
        position: BPosition,
    ) -> Result<Option<Receipt>, StateDatabaseError>;
}
```

- [ ] **Step 2: Build the workspace**

```bash
cd /home/dev/kardamom && cargo build -p kardamom-types
```

Expected: clean (existing impls in S4's in-memory `StateDatabase` will need stubs returning `Ok(None)` — that's a one-line addition there; do it as part of this commit).

- [ ] **Step 3: Commit**

```bash
git add crates/kardamom-types/src/state_database.rs
git commit -m "types: extend StateDatabase with get_tx_position/get_receipt for S1 receipt-by-hash lookup"
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
kardamom-log = { path = "../kardamom-log" }            # for the testing harness used by e2e
alloy-primitives.workspace = true
alloy-rlp.workspace = true
revm.workspace = true
thiserror.workspace = true
tracing.workspace = true
anyhow.workspace = true
crossbeam-channel = "0.5"
libmdbx = "0.6"
metrics.workspace = true
# rkyv 0.8: receipts are stored as rkyv archives at rest (D-Sh2 also uses rkyv on the wire).
rkyv = { version = "0.8", features = ["alloc"] }

[dev-dependencies]
tempfile = "3"
criterion = { version = "0.5", features = ["html_reports"] }
# E2E: real Aeron testcontainer harness exported by kardamom-log under `testing`.
kardamom-log = { path = "../kardamom-log", features = ["testing"] }
tokio = { workspace = true, features = ["macros", "rt-multi-thread"] }

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
//! libmdbx schema. Seven named tables, each with a fixed key/value encoding.
//!
//! | Table           | Key                              | Value                                            |
//! |-----------------|----------------------------------|--------------------------------------------------|
//! | `accounts`      | `Address` (20 B)                 | RLP `(u64 nonce, U256 balance, B256 code_hash, B256 storage_root)` |
//! | `storage`       | `Address ++ B256 key` (52 B)     | `U256 value` (32 B, big-endian)                  |
//! | `code`          | `B256 code_hash` (32 B)          | raw bytecode                                     |
//! | `headers`       | `u64 block_number` (8 B BE)      | encoded `(BPosition end_tx_idx, u64 l2_timestamp)` — **no state root** (D-Sh11) |
//! | `receipts`      | `BPosition tx_idx` (8 B)         | encoded `Receipt` (rkyv archive, owned at rest)  |
//! | `tx_hash_index` | `B256 tx_hash` (32 B)            | `BPosition` (8 B, i32 BE term_id ++ i32 BE term_offset) — feeds S1 `eth_getTransactionReceipt(hash)` (D-Sh4) |
//! | `meta`          | `&[u8]` (well-known keys, below) | varies — see `meta.rs`                           |
//!
//! BE encoding on the `headers` key keeps `block_number` ordered under mdbx's
//! lexicographic cursor; we depend on that for the cold-start scan. `BPosition`
//! encoding (term_id i32 BE ++ term_offset i32 BE, 8 bytes) is lexicographically
//! ordered by `(term_id, term_offset)` — same property holds for `receipts`.

use alloy_primitives::{Address, B256, U256};
use alloy_rlp::{Decodable, Encodable, RlpDecodable, RlpEncodable};
use kardamom_types::{BPosition, Receipt};

use crate::error::StateError;

pub const TABLE_ACCOUNTS: &str = "accounts";
pub const TABLE_STORAGE: &str = "storage";
pub const TABLE_CODE: &str = "code";
pub const TABLE_HEADERS: &str = "headers";
pub const TABLE_RECEIPTS: &str = "receipts";
pub const TABLE_TX_HASH_INDEX: &str = "tx_hash_index";
pub const TABLE_META: &str = "meta";

pub const ALL_TABLES: &[&str] = &[
    TABLE_ACCOUNTS,
    TABLE_STORAGE,
    TABLE_CODE,
    TABLE_HEADERS,
    TABLE_RECEIPTS,
    TABLE_TX_HASH_INDEX,
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
//
// Per D-Sh11, headers do NOT carry a state-root commitment. The encoded value
// is `(end_tx_idx: BPosition, l2_timestamp: u64)`. We use a hand-rolled fixed-
// width encoding (12 + 8 = 20 bytes) instead of RLP — the row is fixed-size
// and BPosition is not an RLP-native type.

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct HeaderValue {
    pub end_tx_idx: BPosition,
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

pub fn encode_header_value(v: &HeaderValue) -> [u8; 20] {
    let mut out = [0u8; 20];
    out[..4].copy_from_slice(&v.end_tx_idx.term_id.to_be_bytes());
    out[4..8].copy_from_slice(&v.end_tx_idx.term_offset.to_be_bytes());
    out[8..16].copy_from_slice(&v.l2_timestamp.to_be_bytes());
    // bytes 16..20 reserved (zero-filled) for forward-compat
    out
}

pub fn decode_header_value(bytes: &[u8]) -> Result<HeaderValue, StateError> {
    if bytes.len() < 16 {
        return Err(StateError::BadEncoding {
            table: TABLE_HEADERS,
            expected: 16,
            got: bytes.len(),
        });
    }
    let mut t_id = [0u8; 4];
    t_id.copy_from_slice(&bytes[..4]);
    let mut t_off = [0u8; 4];
    t_off.copy_from_slice(&bytes[4..8]);
    let mut ts = [0u8; 8];
    ts.copy_from_slice(&bytes[8..16]);
    Ok(HeaderValue {
        end_tx_idx: BPosition {
            term_id: i32::from_be_bytes(t_id),
            term_offset: i32::from_be_bytes(t_off),
        },
        l2_timestamp: u64::from_be_bytes(ts),
    })
}

// ---------- receipts ----------
//
// Key: BPosition (8 bytes — i32 BE term_id ++ i32 BE term_offset).
// Value: rkyv-archived `Receipt` (kardamom-types). At rest we materialize to
// owned bytes via `rkyv::to_bytes`; reads go through `rkyv::access::<Receipt>`
// to deserialize zero-copy when called via `get_receipt`.

pub fn encode_b_position_key(p: BPosition) -> [u8; 8] {
    let mut out = [0u8; 8];
    out[..4].copy_from_slice(&p.term_id.to_be_bytes());
    out[4..].copy_from_slice(&p.term_offset.to_be_bytes());
    out
}

pub fn decode_b_position_key(bytes: &[u8]) -> Result<BPosition, StateError> {
    if bytes.len() != 8 {
        return Err(StateError::BadEncoding {
            table: TABLE_RECEIPTS,
            expected: 8,
            got: bytes.len(),
        });
    }
    let mut t_id = [0u8; 4];
    t_id.copy_from_slice(&bytes[..4]);
    let mut t_off = [0u8; 4];
    t_off.copy_from_slice(&bytes[4..]);
    Ok(BPosition {
        term_id: i32::from_be_bytes(t_id),
        term_offset: i32::from_be_bytes(t_off),
    })
}

pub fn encode_receipt_value(r: &Receipt) -> Vec<u8> {
    // `Receipt` derives `rkyv::Archive/Serialize/Deserialize` upstream.
    rkyv::to_bytes::<rkyv::rancor::Error>(r)
        .expect("Receipt rkyv serialize is infallible for owned data")
        .to_vec()
}

pub fn decode_receipt_value(bytes: &[u8]) -> Result<Receipt, StateError> {
    rkyv::from_bytes::<Receipt, rkyv::rancor::Error>(bytes)
        .map_err(|e| StateError::BadEncoding {
            table: TABLE_RECEIPTS,
            expected: bytes.len(),
            got: bytes.len(),
        }
        .into_with_msg(format!("rkyv: {e}")))
}

// ---------- tx_hash_index (D-Sh4) ----------
//
// Key: `B256 tx_hash` (32 B). Value: `BPosition` (8 B, same layout as
// `encode_b_position_key`). Populated during block commit (one entry per
// receipt). Read path: S1 proxy's `eth_getTransactionReceipt(hash)` calls
// `StateDatabase::get_tx_position(hash)` → `StateDatabase::get_receipt(pos)`.

pub fn encode_tx_hash_key(hash: B256) -> [u8; 32] {
    hash.into()
}

pub fn encode_tx_hash_value(pos: BPosition) -> [u8; 8] {
    encode_b_position_key(pos)
}

pub fn decode_tx_hash_value(bytes: &[u8]) -> Result<BPosition, StateError> {
    decode_b_position_key(bytes).map_err(|_| StateError::BadEncoding {
        table: TABLE_TX_HASH_INDEX,
        expected: 8,
        got: bytes.len(),
    })
}
```

> **Note:** `StateError::into_with_msg` is a small helper to attach a decode message — add it as `impl StateError { pub fn into_with_msg(self, _msg: String) -> Self { self } }` in `error.rs` if it doesn't already exist, or simplify the rkyv error branch to `StateError::BadEncoding { table: TABLE_RECEIPTS, expected: 0, got: bytes.len() }`.

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
    fn header_value_roundtrip_no_state_root() {
        // D-Sh11: headers carry NO state_root_commitment.
        let v = HeaderValue {
            end_tx_idx: BPosition { term_id: 3, term_offset: 4096 },
            l2_timestamp: 1_700_000_000,
        };
        let bytes = encode_header_value(&v);
        assert_eq!(decode_header_value(&bytes).unwrap(), v);
    }

    #[test]
    fn tx_hash_index_roundtrip() {
        // D-Sh4: tx_hash → BPosition lookup table.
        let hash = b256!("dead");
        let pos = BPosition { term_id: 7, term_offset: 12345 };
        let k = encode_tx_hash_key(hash);
        let v = encode_tx_hash_value(pos);
        assert_eq!(k.len(), 32);
        assert_eq!(v.len(), 8);
        assert_eq!(decode_tx_hash_value(&v).unwrap(), pos);
    }

    #[test]
    fn b_position_key_lexicographically_ordered() {
        let a = encode_b_position_key(BPosition { term_id: 0, term_offset: 1 });
        let b = encode_b_position_key(BPosition { term_id: 0, term_offset: 2 });
        let c = encode_b_position_key(BPosition { term_id: 1, term_offset: 0 });
        assert!(a < b);
        assert!(b < c);
    }
}
```

- [ ] **Step 3: Run tests**

```bash
cd /home/dev/kardamom && cargo test -p kardamom-state schema
```

Expected: 9 unit tests pass.

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
//! | Key                                  | Value                          |
//! |--------------------------------------|--------------------------------|
//! | `last_committed_block`               | `u64 BE`                       |
//! | `last_committed_end_tx_position`     | `BPosition` (8 B, i32 BE + i32 BE) |
//! | `last_fsynced_b_position`            | `BPosition` (8 B)              |
//! | `schema_version`                     | `u32 BE` (currently 1)         |

use kardamom_types::BPosition;

use crate::error::StateError;

pub const KEY_LAST_COMMITTED_BLOCK: &[u8] = b"last_committed_block";
pub const KEY_LAST_COMMITTED_END_TX_POSITION: &[u8] = b"last_committed_end_tx_position";
pub const KEY_LAST_FSYNCED_B_POSITION: &[u8] = b"last_fsynced_b_position";
pub const KEY_SCHEMA_VERSION: &[u8] = b"schema_version";

pub const SCHEMA_VERSION: u32 = 1;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DurableCursors {
    pub last_committed_block: u64,
    pub last_committed_end_tx_position: BPosition,
    pub last_fsynced_b_position: BPosition,
    pub schema_version: u32,
}

impl Default for DurableCursors {
    fn default() -> Self {
        Self {
            last_committed_block: 0,
            last_committed_end_tx_position: BPosition { term_id: 0, term_offset: 0 },
            last_fsynced_b_position: BPosition { term_id: 0, term_offset: 0 },
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

pub fn encode_b_position(p: BPosition) -> [u8; 8] {
    // BPosition is `(i32 term_id, i32 term_offset)` per D-Sh1. 8 bytes total.
    let mut out = [0u8; 8];
    out[..4].copy_from_slice(&p.term_id.to_be_bytes());
    out[4..].copy_from_slice(&p.term_offset.to_be_bytes());
    out
}

pub fn decode_b_position(bytes: &[u8]) -> Result<BPosition, StateError> {
    if bytes.len() != 8 {
        return Err(StateError::BadEncoding {
            table: "meta",
            expected: 8,
            got: bytes.len(),
        });
    }
    let mut term_id_bytes = [0u8; 4];
    term_id_bytes.copy_from_slice(&bytes[..4]);
    let mut term_offset_bytes = [0u8; 4];
    term_offset_bytes.copy_from_slice(&bytes[4..]);
    Ok(BPosition {
        term_id: i32::from_be_bytes(term_id_bytes),
        term_offset: i32::from_be_bytes(term_offset_bytes),
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
        let bytes = encode_b_position(p);
        assert_eq!(bytes.len(), 8);
        assert_eq!(decode_b_position(&bytes).unwrap(), p);
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
        // Receipts: 8B key + ~256B estimate per archived Receipt (variable due
        // to logs); good enough for sizing.
        let receipts: usize = self.receipts.len() * (8 + 256);
        // tx_hash_index: 32B key + 8B value per receipt.
        let tx_index: usize = self.receipts.len() * (32 + 8);
        let header = 8 + 20;
        acct + stor + code + receipts + tx_index + header
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
use kardamom_types::{AccountRecord, BPosition, Receipt, StateDatabase, StateDatabaseError};
use libmdbx::{NoWriteMap, RO, Transaction};
use revm::primitives::Bytes;

use crate::env::StateEnv;
use crate::error::StateError;
use crate::meta::{decode_u64, KEY_LAST_COMMITTED_BLOCK};
use crate::schema::{
    decode_account_value, decode_receipt_value, decode_storage_value, decode_tx_hash_value,
    encode_account_key, encode_b_position_key, encode_code_key, encode_storage_key,
    encode_tx_hash_key, TABLE_ACCOUNTS, TABLE_CODE, TABLE_META, TABLE_RECEIPTS, TABLE_STORAGE,
    TABLE_TX_HASH_INDEX,
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

    /// D-Sh4: tx_hash → BPosition lookup. Feeds S1 `eth_getTransactionReceipt`.
    fn get_tx_position(
        &self,
        tx_hash: B256,
    ) -> Result<Option<BPosition>, StateDatabaseError> {
        let key = encode_tx_hash_key(tx_hash);
        let db = self
            .inner
            .txn
            .open_db(Some(TABLE_TX_HASH_INDEX))
            .map_err(|e| StateDatabaseError::Backend(e.to_string()))?;
        match self
            .inner
            .txn
            .get(db.dbi(), &key)
            .map_err(|e| StateDatabaseError::Backend(e.to_string()))?
        {
            None => Ok(None),
            Some(bytes) => decode_tx_hash_value(&bytes)
                .map(Some)
                .map_err(StateDatabaseError::from),
        }
    }

    /// D-Sh4: load a Receipt by its canonical BPosition. Returns None if no
    /// receipt was committed at that position (e.g. the position was a system
    /// marker, or the tx has not yet committed).
    fn get_receipt(
        &self,
        position: BPosition,
    ) -> Result<Option<Receipt>, StateDatabaseError> {
        let key = encode_b_position_key(position);
        let db = self
            .inner
            .txn
            .open_db(Some(TABLE_RECEIPTS))
            .map_err(|e| StateDatabaseError::Backend(e.to_string()))?;
        match self
            .inner
            .txn
            .get(db.dbi(), &key)
            .map_err(|e| StateDatabaseError::Backend(e.to_string()))?
        {
            None => Ok(None),
            Some(bytes) => decode_receipt_value(&bytes)
                .map(Some)
                .map_err(StateDatabaseError::from),
        }
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
    KEY_LAST_COMMITTED_END_TX_POSITION, KEY_LAST_FSYNCED_B_POSITION, KEY_SCHEMA_VERSION,
    SCHEMA_VERSION,
};
use crate::schema::{
    encode_account_key, encode_account_value, encode_b_position_key, encode_block_key,
    encode_code_key, encode_header_value, encode_receipt_value, encode_storage_key,
    encode_storage_value, encode_tx_hash_key, encode_tx_hash_value, AccountValue, HeaderValue,
    TABLE_ACCOUNTS, TABLE_CODE, TABLE_HEADERS, TABLE_META, TABLE_RECEIPTS, TABLE_STORAGE,
    TABLE_TX_HASH_INDEX,
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
        let tx_hash_index = txn.open_db(Some(TABLE_TX_HASH_INDEX))?;
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

        // --- headers (D-Sh11: no state_root_commitment) ---
        let header = HeaderValue {
            end_tx_idx: delta.boundary.end_tx_idx,
            l2_timestamp: delta.boundary.l2_timestamp,
        };
        txn.put(
            headers.dbi(),
            &encode_block_key(delta.boundary.block_number),
            &encode_header_value(&header),
            WriteFlags::UPSERT,
        )?;

        // --- receipts + tx_hash_index (D-Sh4) ---
        // For each receipt: write the receipt at its BPosition key, AND
        // populate tx_hash_index[receipt.tx_hash] = receipt.tx_idx so the S1
        // proxy can serve `eth_getTransactionReceipt(hash)` via two reads:
        //   StateDatabase::get_tx_position(hash) → StateDatabase::get_receipt(pos)
        for r in &delta.receipts {
            let pos_key = encode_b_position_key(r.tx_idx);
            txn.put(
                receipts.dbi(),
                &pos_key,
                &encode_receipt_value(r),
                WriteFlags::UPSERT,
            )?;
            txn.put(
                tx_hash_index.dbi(),
                &encode_tx_hash_key(r.tx_hash),
                &encode_tx_hash_value(r.tx_idx),
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
        // end_tx_idx is a BPosition (per D-Sh1 BlockBoundary shape), not u64.
        txn.put(
            meta.dbi(),
            KEY_LAST_COMMITTED_END_TX_POSITION,
            &encode_b_position(delta.boundary.end_tx_idx),
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
    KEY_LAST_COMMITTED_END_TX_POSITION, KEY_LAST_FSYNCED_B_POSITION,
};
use crate::schema::TABLE_META;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RecoveryPoint {
    pub last_committed_block: u64,
    pub last_committed_end_tx_position: BPosition,
    pub last_fsynced_b_position: BPosition,
}

pub fn read_recovery_point(env: &StateEnv) -> Result<RecoveryPoint, StateError> {
    let txn = env.raw().begin_ro_txn()?;
    let meta = txn.open_db(Some(TABLE_META))?;

    let last_committed_block = match txn.get(meta.dbi(), KEY_LAST_COMMITTED_BLOCK)? {
        Some(b) => decode_u64(&b)?,
        None => 0,
    };
    let last_committed_end_tx_position =
        match txn.get(meta.dbi(), KEY_LAST_COMMITTED_END_TX_POSITION)? {
            Some(b) => decode_b_position(&b)?,
            None => BPosition { term_id: 0, term_offset: 0 },
        };
    let last_fsynced_b_position = match txn.get(meta.dbi(), KEY_LAST_FSYNCED_B_POSITION)? {
        Some(b) => decode_b_position(&b)?,
        None => BPosition { term_id: 0, term_offset: 0 },
    };

    Ok(RecoveryPoint {
        last_committed_block,
        last_committed_end_tx_position,
        last_fsynced_b_position,
    })
}

impl From<RecoveryPoint> for DurableCursors {
    fn from(p: RecoveryPoint) -> Self {
        DurableCursors {
            last_committed_block: p.last_committed_block,
            last_committed_end_tx_position: p.last_committed_end_tx_position,
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

pub fn bpos(block: u64) -> BPosition {
    BPosition { term_id: 0, term_offset: (block * 1024) as i32 }
}

pub fn simple_delta(block: u64, addr: Address, balance: u64, slot: u64, slot_value: u64) -> BlockDelta {
    let end_pos = bpos(block);
    // Deterministic per-block tx_hash so tx_hash_index tests can look it up.
    let mut hash_bytes = [0u8; 32];
    hash_bytes[24..].copy_from_slice(&block.to_be_bytes());
    let tx_hash = B256::from(hash_bytes);
    BlockDelta {
        boundary: BlockBoundary {
            block_number: block,
            end_tx_idx: end_pos,           // BPosition per D-Sh1 — NO state_root_commitment (D-Sh11)
            l2_timestamp: 1_700_000_000 + block,
        },
        end_b_position: end_pos,
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
            tx_idx: end_pos,
            tx_hash,
            status: true,
            gas_used: 21_000,
            logs: vec![],
            write_set_hash: B256::ZERO,
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
    let pos = BPosition { term_id: 0, term_offset: (block * 1024) as i32 };
    let mut hash_bytes = [0u8; 32];
    hash_bytes[24..].copy_from_slice(&block.to_be_bytes());
    let tx_hash = B256::from(hash_bytes);
    BlockDelta {
        boundary: BlockBoundary {
            block_number: block,
            end_tx_idx: pos,                  // D-Sh11: no state_root_commitment
            l2_timestamp: 1_700_000_000 + block,
        },
        end_b_position: pos,
        accounts: AccountChanges(accounts),
        storage: StorageChanges(vec![]),
        code: CodeChanges(vec![]),
        receipts: vec![Receipt {
            tx_idx: pos,
            tx_hash,
            status: true,
            gas_used: 21_000,
            logs: vec![],
            write_set_hash: B256::ZERO,
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

Defer to the user — do not auto-open. The plan ends with all changes committed locally on `claude/s6-state-writer` after Task 25.

---

## Task 25: E2E test against real Aeron in Docker

**Files:**
- Create: `crates/kardamom-state/tests/e2e_real_aeron.rs`

Per S0 D-Sh8, every component plan **must** include an e2e test that drives the real Aeron Media Driver + Archive containers (via the `testcontainers`-based harness exported by `kardamom-log` under the `testing` feature). Unit tests with the in-memory fake are fine for logic; e2e MUST be real Aeron — no mocks at this layer.

This test brings up a real Aeron stack, feeds real `Receipt` and `BlockBoundary` messages through tx_receipts, runs a real state-writer process consuming them, and asserts:

1. After all blocks are committed, the libmdbx state matches the expected account / storage / receipt values.
2. The `tx_hash_index` table correctly resolves every tx hash back to its `BPosition` and a follow-up `get_receipt(position)` returns the same receipt that was sent.
3. The committed `headers` row contains the right `(end_tx_idx, l2_timestamp)` and **no state-root field** (D-Sh11).

- [ ] **Step 1: Write the test**

```rust
//! End-to-end: real Aeron (testcontainer) → state writer (real libmdbx) →
//! assertions on the persisted state, tx_hash_index, and headers.

use std::time::Duration;

use alloy_primitives::{address, B256, U256};
use kardamom_log::testing::aeron_docker::AeronStack;       // harness from S3 (D-Sh8)
use kardamom_log::{ChannelC, ChannelCMessage};            // tx_receipts pub/sub on real Aeron
use kardamom_state::{
    env::{Durability, StateEnvBuilder},
    AccountChange, AccountChanges, BlockDelta, CodeChanges, NewAccountState, StateWriter,
    StorageChange, StorageChanges,
};
use kardamom_types::{BPosition, BlockBoundary, Receipt, StateDatabase};

fn bpos(block: u64, offset: u64) -> BPosition {
    BPosition {
        term_id: 0,
        term_offset: ((block * 1000) + offset) as i32,
    }
}

fn deterministic_hash(block: u64, idx: u64) -> B256 {
    let mut b = [0u8; 32];
    b[16..24].copy_from_slice(&block.to_be_bytes());
    b[24..].copy_from_slice(&idx.to_be_bytes());
    B256::from(b)
}

#[tokio::test(flavor = "multi_thread")]
async fn real_aeron_to_state_writer_e2e() {
    // ---- 1. spin up real Aeron Media Driver + Archive in Docker ----
    let aeron = AeronStack::start().await.expect("aeron docker stack");
    let channel_c = ChannelC::connect(aeron.client_config())
        .await
        .expect("tx_receipts pub/sub");

    // ---- 2. spin up the real state writer against a fresh libmdbx env ----
    let dir = tempfile::tempdir().unwrap();
    let env = StateEnvBuilder::new(dir.path())
        .durability(Durability::SafeNoSync)
        .open()
        .unwrap();
    let writer = StateWriter::spawn(env.clone()).unwrap();

    // The "state writer process": a single task that subscribes to tx_receipts,
    // accumulates receipts per virtual block, and forwards a `BlockDelta` to
    // the writer whenever it sees a `BlockBoundary`. Real Aeron in the middle,
    // real BlockDelta channel out to libmdbx.
    let sub = channel_c.subscribe().await.expect("subscribe to C");
    let delta_tx = writer.delta_tx.clone();
    let consumer = tokio::spawn(async move {
        let mut current_block: Option<u64> = None;
        let mut current_receipts: Vec<Receipt> = Vec::new();
        let mut current_end_pos = BPosition { term_id: 0, term_offset: 0 };
        while let Some(msg) = sub.recv().await {
            match msg {
                ChannelCMessage::Receipt(r) => {
                    current_end_pos = r.tx_idx;
                    current_receipts.push(r);
                }
                ChannelCMessage::BlockBoundary(b) => {
                    let block = b.block_number;
                    current_block = Some(block);
                    // Build a delta whose accounts/storage track the receipts.
                    // For the e2e we apply a deterministic per-block account
                    // mutation so we can assert state from the receipts alone.
                    let addr = address!("00000000000000000000000000000000000000aa");
                    let delta = BlockDelta {
                        boundary: b, // contains end_tx_idx: BPosition; NO state root
                        end_b_position: current_end_pos,
                        accounts: AccountChanges(vec![AccountChange {
                            address: addr,
                            new_state: Some(NewAccountState {
                                nonce: block,
                                balance: U256::from(1000 + block),
                                code_hash: B256::ZERO,
                                storage_root: B256::ZERO,
                            }),
                        }]),
                        storage: StorageChanges(vec![StorageChange {
                            address: addr,
                            key: U256::from(7u64),
                            value: Some(U256::from(block * 100)),
                        }]),
                        code: CodeChanges(vec![]),
                        receipts: std::mem::take(&mut current_receipts),
                    };
                    delta_tx.send(delta).expect("forward to writer");
                }
            }
        }
        current_block
    });

    // ---- 3. publish real Receipt+BlockBoundary messages onto Aeron ----
    let pub_handle = channel_c.publication().await.expect("pub handle");
    // Three blocks, two receipts each.
    let mut all_hashes: Vec<(u64, u64, B256, BPosition)> = Vec::new();
    for block in 1u64..=3 {
        for idx in 0u64..2 {
            let pos = bpos(block, idx);
            let hash = deterministic_hash(block, idx);
            let r = Receipt {
                tx_idx: pos,
                tx_hash: hash,
                status: true,
                gas_used: 21_000 + idx,
                logs: vec![],
                write_set_hash: B256::ZERO,
            };
            all_hashes.push((block, idx, hash, pos));
            pub_handle
                .send(ChannelCMessage::Receipt(r))
                .await
                .expect("publish receipt");
        }
        // Boundary closes the block at the position of the last receipt.
        let last_pos = bpos(block, 1);
        pub_handle
            .send(ChannelCMessage::BlockBoundary(BlockBoundary {
                block_number: block,
                end_tx_idx: last_pos,
                l2_timestamp: 1_700_000_000 + block,
            }))
            .await
            .expect("publish boundary");
    }

    // ---- 4. wait for all 3 post-commit snapshots ----
    let mut latest = None;
    for _ in 0..3 {
        latest = tokio::task::spawn_blocking({
            let rx = writer.snapshot_rx.clone();
            move || rx.recv()
        })
        .await
        .unwrap();
    }
    let snap = latest.expect("at least one snapshot");

    // ---- 5. assertions: state matches expectation ----
    assert_eq!(snap.block_number(), 3, "all 3 blocks committed");
    let addr = address!("00000000000000000000000000000000000000aa");
    let acct = snap.account(addr).unwrap().expect("account exists");
    assert_eq!(acct.balance, U256::from(1003u64));
    assert_eq!(acct.nonce, 3);
    let slot = snap.storage(addr, U256::from(7u64)).unwrap();
    assert_eq!(slot, U256::from(300u64), "block 3 slot value");

    // ---- 6. assertions: tx_hash_index resolves every hash correctly ----
    for (block, idx, hash, expected_pos) in &all_hashes {
        let pos = snap
            .get_tx_position(*hash)
            .expect("tx_hash_index lookup")
            .unwrap_or_else(|| panic!("missing tx_hash entry for block {block} idx {idx}"));
        assert_eq!(pos, *expected_pos, "tx_hash_index points at the right BPosition");
        let r = snap
            .get_receipt(pos)
            .expect("receipt fetch")
            .expect("receipt exists at position");
        assert_eq!(r.tx_hash, *hash, "receipt at position matches by hash");
        assert_eq!(r.gas_used, 21_000 + idx, "receipt content round-trips through Aeron + libmdbx");
    }

    // ---- 7. shutdown ----
    drop(pub_handle);
    drop(channel_c);
    let _ = tokio::time::timeout(Duration::from_secs(2), consumer).await;
    writer.shutdown().unwrap();
    aeron.stop().await;
}
```

- [ ] **Step 2: Run (Docker required)**

```bash
cd /home/dev/kardamom && cargo test -p kardamom-state --test e2e_real_aeron -- --nocapture
```

Expected: passes. CI runs this on every PR (see S3 D-Sh8 CI workflow).

- [ ] **Step 3: Commit**

```bash
git add crates/kardamom-state/tests/e2e_real_aeron.rs
git commit -m "state(e2e): real Aeron → state writer → libmdbx + tx_hash_index round-trip"
```

---

## Spec coverage cross-check

| Spec item (§5 + §2.7 + §4.6)                                | Task                |
|-------------------------------------------------------------|---------------------|
| `accounts` table                                            | Task 6 (schema) + Task 12 (writer) |
| `storage` flat table                                        | Task 6 + Task 12    |
| `code` content-addressed                                    | Task 6 + Task 12    |
| `headers` (D-Sh11: no state root)                           | Task 6 + Task 12    |
| `receipts`                                                  | Task 6 + Task 12    |
| `tx_hash_index` (D-Sh4)                                     | Task 6 (schema) + Task 12 (populate) + Task 10 (`get_tx_position`/`get_receipt` impl) |
| `meta` cursors                                              | Task 7 + Task 12 + Task 13 |
| `StateDatabase` trait — **defined upstream** in `kardamom-types` (D-Sh1); implemented here | Task 1 (extend trait) + Task 10 (`impl`) |
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
| **E2E** real Aeron → state writer → libmdbx (D-Sh8)         | **Task 25**         |
| §4.6 — "fall behind > horizon ⇒ executor sees snapshot exhaustion, halt" | Task 4 (error variant) + Task 5 (horizon doc) + Task 12 (bounded channel == hard backpressure) |

## Open questions for the user / S4 coordinator

1. ~~**`StateDatabase` trait location.**~~ Resolved by S0 D-Sh1: defined in `kardamom-types`, **implemented** here. Task 1 only extends the trait with the two receipt-lookup methods (D-Sh4).
2. ~~**`BPosition` / `BlockBoundary` / `Receipt` source.**~~ Resolved by S0 D-Sh1: owned by `kardamom-types`. `BlockBoundary` does **not** carry a state-root commitment (D-Sh11). `Receipt` shape (with `tx_idx: BPosition`, `tx_hash: B256`, etc.) is fixed upstream — this plan does not redefine any of it.
3. **`libmdbx` crate choice.** Plan uses `libmdbx = "0.6"` (vorot93, MPL-2.0). Alternative `signet-libmdbx = "0.8"` (init4tech, MIT/Apache-2.0). See S0 D-Sh9: default is `libmdbx`; if workspace standardizes on MIT/Apache, swap the dep + adjust import paths in Task 9–14. Behaviour equivalent.

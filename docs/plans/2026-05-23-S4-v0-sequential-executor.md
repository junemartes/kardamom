# S4 v0 Sequential Executor Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Ship the v0 executor subsystem (S4) — a single-threaded, deterministic, sequential revm executor that consumes Aeron channel B (txs + block boundaries) and publishes receipts + sealed block boundaries to channel C, with a state-snapshot swap protocol that lets the libmdbx writer (S6) advance underneath it.

**Architecture:** A new crate `crates/kardamom-executor` exposes one long-running `Executor` actor: a **reader thread** demuxes channel B (`TxEnvelope` | `BlockBoundaryStart`); a **single execution thread** runs revm against a `Box<dyn StateDatabase>` snapshot, accumulates writes in a per-block `BlockDelta`, computes a deterministic `write_set_hash` per tx; a **commit thread** drains receipts in tx-index order and publishes to channel C. At each `BlockBoundaryStart` the executor drains in-flight work, publishes a slim `BlockBoundary { block_number, end_tx_idx, l2_timestamp }` on C (no state-root commitment — per S0 D-Sh11, state-root attestation is a validator concern deferred out of v0), hands the delta to the state-writer queue, waits for the state writer's "block N committed" signal, briefly pauses the reader to swap the read snapshot, and resumes. Per-tx determinism is still enforced via `Receipt.write_set_hash`. Block-STM is **explicitly out of scope** for v0 — S4 v1 will replace the single execution thread with worker threads behind the same channel-B-in / channel-C-out interface.

**Tech Stack:** Rust (edition 2024), revm 38, alloy-primitives 1.6, alloy-consensus 2.0, `crossbeam-channel` for in-process queues, `sha3` (via `alloy_primitives::keccak256`) for deterministic hashing, `rkyv` 0.8 zero-copy wire codec (per S0 D-Sh2; executor consumes `Archived<TxEnvelope>` from channel B zero-copy on the hot path and produces owned `Receipt` values for channel C), `criterion` for benches, `tempfile` + an in-crate mock `StateDatabase` for tests.

**Branch:** `claude/s4-v0-sequential-executor` (branched off `claude/work`).

**Reference spec:** `docs/specs/2026-05-23-high-throughput-sequencer-design.md` — see §1 (architecture), §2.4 (executor), §3 (latency budget), §4.4 (executor failure & divergence panic), §5 (state persistence + snapshot swap), and the **V0 scope** section.

**Adapted from:** `crates/node/src/executor.rs` provides the sequential-revm building blocks (`tx_env_from_envelope`, `execute`, `execute_deposit`, `ExecEnv`). The new crate moves that logic into a thread-actor shape; the original module stays in place until S6/S7 wire the new executor in.

**Key dependencies / assumptions:**
- **`crates/kardamom-types`** (per S0 D-Sh1) owns all shared wire/data types: `BPosition`, `TxEnvelope`, `Receipt`, `BlockBoundaryStart`, `BlockBoundary`, `BlockDelta`, the `StateDatabase` trait, and the `SnapshotSource` trait. The executor **imports** these — it does not define them. (S0 explicitly overrides this plan's earlier choice to host `StateDatabase` in `crates/kardamom-executor/src/state.rs`.)
- **S3 (`crates/kardamom-log`)** publishes/consumes Aeron channels B and C. The wire-type definitions live in `kardamom-types`; `kardamom-log` exposes only the channel implementations (`TxOrderingSubscription`, `ChannelCPublication`) and reads `Archived<T>` zero-copy from Aeron buffers using `rkyv` 0.8 (S0 D-Sh2).
- **S6 (`crates/kardamom-state`)** provides a `libmdbx`-backed implementation of the `StateDatabase` trait (the trait itself lives in `kardamom-types`, so S6 depends on `kardamom-types`, not on `kardamom-executor`).
- **S5 (`crates/kardamom-sealer`)** emits `BlockBoundaryStart` *inline* on channel B. The executor never reads a wall clock; `block.timestamp` comes from `BlockBoundaryStart.l2_timestamp` and `block.number` from `BlockBoundaryStart.block_number`.

---

## File structure

New crate `crates/kardamom-executor`:

```
crates/kardamom-executor/
├── Cargo.toml
└── src/
    ├── lib.rs              -- crate root: re-exports public API (most types re-exported from kardamom-types)
    ├── types.rs            -- BMessage / CMessage enums only (these are executor-internal demux wrappers
                              around kardamom-types' TxEnvelope / Receipt / BlockBoundary*); no Receipt /
                              BlockBoundary / TxEnvelope / BPosition definitions — those live in kardamom-types.
    ├── state.rs            -- MockStateDatabase test fixture only (StateDatabase trait lives in kardamom-types
                              per S0 D-Sh1; this module imports it).
    ├── delta.rs            -- per-tx WriteSet accumulator + deterministic write_set_hash. No state-root
                              computation (per S0 D-Sh11).
    ├── block_env.rs        -- ExecEnv builder: chain_id, block_number, l2_timestamp, prevrandao (deterministic)
    ├── executor.rs         -- sequential revm step function (single-tx); adapted from crates/node/src/executor.rs
    ├── actor.rs            -- Executor struct: reader/exec/commit threads, snapshot-swap loop
    ├── error.rs            -- ExecutorError
    └── tests/              -- in-tree integration tests (not a tests/ dir; #[cfg(test)] modules per file)
crates/kardamom-executor/tests/
├── replay_integration.rs   -- mocked channels + MockStateDatabase, synthetic N-tx/K-block stream
├── determinism.rs          -- two replicas, same input, byte-identical CMessage stream
├── diff_reference.rs       -- differential test against naive revm reference path
└── docker_aeron_e2e.rs     -- real-Aeron e2e via S3's testcontainers harness (Task N)
crates/kardamom-executor/benches/
└── sequential_throughput.rs -- criterion: transfers + Uniswap-fixture
```

Files outside the new crate:

```
crates/kardamom-types/      -- (S0 D-Sh1) owns BPosition, TxEnvelope, Receipt, BlockBoundary,
                              BlockBoundaryStart, BlockDelta, StateDatabase trait, SnapshotSource trait.
                              The executor imports from here; it does NOT redefine these types locally.
crates/kardamom-log/        -- (S3) channel implementations only; depends on kardamom-types for messages.
crates/node/src/executor.rs -- UNTOUCHED. Stays as the in-process RPC node's executor until the new
                              executor replaces it in a later integration spec.
```

---

## Self-review checklist (executed after Task 19)

- Every spec V0-scope item maps to a task: reader/exec/commit threads (Tasks 9–11), boundary handling (Task 12), snapshot swap (Task 13), determinism (Tasks 6, 15), divergence panic (referenced in Task 16; lives in `kardamom-log`), real-Aeron e2e (Task N).
- No placeholders. Every code step is complete.
- Type consistency: `BPosition`, `TxEnvelope`, `Receipt`, `BlockBoundary`, `BlockBoundaryStart`, `BlockDelta`, and `StateDatabase` are all imported from `kardamom-types` (S0 D-Sh1) — no local redefinitions in `kardamom-executor`.
- **No state-root computation anywhere in the executor** (S0 D-Sh11). `BlockBoundary` carries only `{ block_number, end_tx_idx, l2_timestamp }`. Per-tx determinism is enforced exclusively via `Receipt.write_set_hash`.
- **`tx_hash` is never computed by the executor** (S0 D-Sh4). The executor copies `tx_hash` directly from `inbound_envelope.tx_hash` into the outbound `Receipt.tx_hash`. The proxy (S1) is the sole producer.

---

## Task 1: Create `crates/kardamom-executor` skeleton

**Files:**
- Create: `crates/kardamom-executor/Cargo.toml`
- Create: `crates/kardamom-executor/src/lib.rs`

- [ ] **Step 1: Verify workspace membership**

The workspace `Cargo.toml` already has `members = ["crates/*"]`, so the new crate joins automatically. Confirm:

```bash
grep -n 'members' /home/dev/kardamom/Cargo.toml
```

Expected: `members = ["crates/*"]`.

- [ ] **Step 2: Write Cargo.toml**

```toml
[package]
name = "kardamom-executor"
version.workspace = true
edition.workspace = true

[dependencies]
# Per S0 D-Sh1: shared types live in kardamom-types.
kardamom-types = { path = "../kardamom-types" }
revm.workspace = true
alloy-primitives.workspace = true
alloy-consensus.workspace = true
alloy-rlp.workspace = true
tracing.workspace = true
thiserror.workspace = true
crossbeam-channel = "0.5"

[dev-dependencies]
# Per S0 D-Sh8: real-Aeron e2e tests use S3's testcontainers harness.
kardamom-log = { path = "../kardamom-log", features = ["testing"] }
alloy-signer-local.workspace = true
alloy-network.workspace = true
tempfile = "3"
criterion = { version = "0.5", features = ["html_reports"] }
rand = "0.8"
rand_chacha = "0.3"
testcontainers = "0.20"
tokio = { version = "1", features = ["rt-multi-thread", "macros", "time"] }

[[bench]]
name = "sequential_throughput"
harness = false
```

- [ ] **Step 3: Write stub lib.rs**

```rust
//! Kardamom S4 v0 sequential executor.
//!
//! Single-threaded revm executor that consumes Aeron channel B (txs + block
//! boundaries from the sealer) and publishes receipts + sealed boundaries to
//! channel C. Block-STM is out of scope for v0; S4 v1 will replace the single
//! execution thread with parallel workers behind the same channel interface.
//!
//! See docs/specs/2026-05-23-high-throughput-sequencer-design.md §2.4 and the
//! V0 scope section. Shared types come from `kardamom-types` (S0 D-Sh1).

pub mod actor;
pub mod block_env;
pub mod delta;
pub mod error;
pub mod executor;
pub mod state;
pub mod types;

pub use actor::Executor;
pub use error::ExecutorError;
// Re-export shared types from kardamom-types so callers can use this crate's
// public API without a separate `kardamom-types` import path.
pub use kardamom_types::{
    AccountChange, BPosition, BlockBoundary, BlockBoundaryStart, BlockDelta, Receipt,
    SnapshotSource, StateDatabase, StateDatabaseError, TxEnvelope,
};
pub use types::{BMessage, CMessage, TxIndex};
```

- [ ] **Step 4: Create empty module files so `cargo check` succeeds**

```bash
cd /home/dev/kardamom
mkdir -p crates/kardamom-executor/src crates/kardamom-executor/tests crates/kardamom-executor/benches
: > crates/kardamom-executor/src/types.rs
: > crates/kardamom-executor/src/state.rs
: > crates/kardamom-executor/src/delta.rs
: > crates/kardamom-executor/src/block_env.rs
: > crates/kardamom-executor/src/executor.rs
: > crates/kardamom-executor/src/actor.rs
: > crates/kardamom-executor/src/error.rs
```

- [ ] **Step 5: Verify it builds**

The lib.rs above references modules that are still empty — Rust will fail with "unresolved module" for the `pub use` lines. Replace lib.rs temporarily with just `//! placeholder` to confirm the crate compiles, then re-add the modules in subsequent tasks. To avoid the dance, write a minimal placeholder body in each file:

```bash
cd /home/dev/kardamom
for f in types state delta block_env executor actor error; do
  printf '//! placeholder; populated in later task.\n' > crates/kardamom-executor/src/$f.rs
done
```

Now make `lib.rs` only declare modules without `pub use`:

```rust
//! Kardamom S4 v0 sequential executor — placeholder; see plan task list.

pub mod actor;
pub mod block_env;
pub mod delta;
pub mod error;
pub mod executor;
pub mod state;
pub mod types;
```

(Subsequent tasks will reinstate the `pub use` block.)

```bash
cargo check -p kardamom-executor
```

Expected: builds cleanly.

- [ ] **Step 6: Commit**

```bash
cd /home/dev/kardamom
git add crates/kardamom-executor
git commit -m "executor: add crate skeleton"
```

---

## Task 2: Define executor-internal `BMessage` / `CMessage` / `TxIndex` in `types.rs`

**Files:**
- Modify: `crates/kardamom-executor/src/types.rs`

**Context:** Per S0 D-Sh1, all shared wire types (`BPosition`, `TxEnvelope`, `Receipt`, `BlockBoundary`, `BlockBoundaryStart`, `BlockDelta`, `AccountChange`) live in `kardamom-types`. This module defines only the executor-internal demux enums (`BMessage`, `CMessage`) that wrap those imported types plus the executor-local `TxIndex` newtype. Do **not** redefine any imported type here.

- [ ] **Step 1: Write the type definitions**

```rust
//! TxOrdering / Channel C executor-side demux wrappers.
//!
//! Shared wire types (`BPosition`, `TxEnvelope`, `Receipt`, `BlockBoundary`,
//! `BlockBoundaryStart`, `BlockDelta`, `AccountChange`) are imported from
//! `kardamom-types` per S0 D-Sh1; we never redefine them here. The executor's
//! `ReceiptStatus` enum is a local presentation of revm's execution outcome —
//! `kardamom-types::Receipt.status` is a single `bool` (success/failure); the
//! executor converts before publishing.

use kardamom_types::{BPosition, BlockBoundary, BlockBoundaryStart, Receipt, TxEnvelope};
use revm::context::result::HaltReason;

/// Monotonically increasing global index of a tx within the canonical channel-B
/// stream. Derived by the executor's reader from the input order, starting at 0
/// for the first tx after genesis.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct TxIndex(pub u64);

impl TxIndex {
    pub const ZERO: TxIndex = TxIndex(0);
    pub fn next(self) -> TxIndex {
        TxIndex(self.0 + 1)
    }
}

/// One canonical-ordered record off channel B. The sealer emits
/// `BlockBoundaryStart` records inline; the sequencer emits `Tx` records.
///
/// `envelope` is `kardamom_types::TxEnvelope`, which already carries the
/// proxy-populated `sender` and `tx_hash` (S0 D-Sh3, D-Sh4). The executor
/// trusts those fields unconditionally — no re-recovery, no re-hash.
#[derive(Debug, Clone)]
pub enum BMessage {
    Tx {
        position: BPosition,
        tx_idx: TxIndex,
        envelope: TxEnvelope,
    },
    BlockBoundaryStart(BlockBoundaryStart),
}

/// One published record on channel C — receipts and sealed boundaries.
#[derive(Debug, Clone)]
pub enum CMessage {
    Receipt(Receipt),
    BlockBoundary(BlockBoundary),
}

/// Executor-local presentation of revm's execution outcome. Folded into a
/// `bool` when materializing `kardamom_types::Receipt.status` (success vs.
/// failure); the richer Halt reason stays internal for diagnostics/logs only.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReceiptStatus {
    Success,
    Revert,
    Halt(HaltReason),
}

impl ReceiptStatus {
    pub fn is_success(self) -> bool {
        matches!(self, ReceiptStatus::Success)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use kardamom_types::BPosition;

    #[test]
    fn tx_index_next_increments() {
        assert_eq!(TxIndex(5).next(), TxIndex(6));
    }

    #[test]
    fn bposition_orders_by_term_then_offset() {
        // BPosition comes from kardamom-types; sanity-check the import works.
        let a = BPosition { term_id: 0, term_offset: 100 };
        let b = BPosition { term_id: 1, term_offset: 0 };
        let c = BPosition { term_id: 0, term_offset: 200 };
        assert!(a < b);
        assert!(a < c);
        assert!(c < b);
    }

    #[test]
    fn receipt_status_is_success_helper() {
        assert!(ReceiptStatus::Success.is_success());
        assert!(!ReceiptStatus::Revert.is_success());
    }
}
```

- [ ] **Step 2: Build and test**

```bash
cd /home/dev/kardamom
cargo test -p kardamom-executor types::tests
```

Expected: 3 tests pass.

- [ ] **Step 3: Commit**

```bash
git add crates/kardamom-executor/src/types.rs
git commit -m "executor: BMessage/CMessage/TxIndex demux types (shared types from kardamom-types)"
```

---

## Task 3: Add `MockStateDatabase` in-memory fixture in `state.rs` (trait imported from `kardamom-types`)

**Files:**
- Modify: `crates/kardamom-executor/src/state.rs`

**Context:** Per S0 D-Sh1, the `StateDatabase` trait, the `SnapshotSource` trait, the `StateDatabaseError` type, and the `AccountState` type **all live in `kardamom-types`**. This module **imports** them — it does not redefine them. The only new code here is `MockStateDatabase`, a `BTreeMap`-backed test fixture that `impl`s the imported trait. S6 (state writer) provides the libmdbx-backed impl in its own crate, also against the `kardamom-types` trait.

- [ ] **Step 1: Write the mock (no trait definition)**

```rust
//! `MockStateDatabase`: in-memory fixture implementing the `StateDatabase`
//! trait defined in `kardamom-types` (per S0 D-Sh1).
//!
//! The trait, `StateDatabaseError`, and `AccountState` are imported from
//! `kardamom-types`. No trait definition lives in this crate.
//!
//! `MockStateDatabase` is a `BTreeMap`-backed implementation used by every
//! integration test and bench in this plan; it supports cheap snapshot cloning
//! (Arc + persistent maps not needed at v0 scale).

use std::collections::BTreeMap;
use std::sync::Arc;

use alloy_primitives::{Address, B256, Bytes, U256};
use kardamom_types::{AccountState, StateDatabase, StateDatabaseError};

/// Test fixture. Cheap to clone (Arc-internal). Construct via
/// `MockStateDatabase::builder()`.
#[derive(Debug, Default, Clone)]
pub struct MockStateDatabase {
    inner: Arc<MockInner>,
}

#[derive(Debug, Default)]
struct MockInner {
    accounts: BTreeMap<Address, AccountState>,
    storage: BTreeMap<(Address, U256), U256>,
    code: BTreeMap<B256, Bytes>,
}

impl MockStateDatabase {
    pub fn builder() -> MockStateDatabaseBuilder {
        MockStateDatabaseBuilder::default()
    }
}

#[derive(Debug, Default)]
pub struct MockStateDatabaseBuilder {
    accounts: BTreeMap<Address, AccountState>,
    storage: BTreeMap<(Address, U256), U256>,
    code: BTreeMap<B256, Bytes>,
}

impl MockStateDatabaseBuilder {
    pub fn account(mut self, addr: Address, balance: U256, nonce: u64, code_hash: B256) -> Self {
        self.accounts.insert(addr, AccountState { balance, nonce, code_hash });
        self
    }

    pub fn storage(mut self, addr: Address, key: U256, value: U256) -> Self {
        self.storage.insert((addr, key), value);
        self
    }

    pub fn code(mut self, code_hash: B256, bytes: Bytes) -> Self {
        self.code.insert(code_hash, bytes);
        self
    }

    pub fn build(self) -> MockStateDatabase {
        MockStateDatabase {
            inner: Arc::new(MockInner {
                accounts: self.accounts,
                storage: self.storage,
                code: self.code,
            }),
        }
    }
}

impl StateDatabase for MockStateDatabase {
    fn basic(&self, address: Address) -> Result<Option<AccountState>, StateDatabaseError> {
        Ok(self.inner.accounts.get(&address).cloned())
    }
    fn code_by_hash(&self, code_hash: B256) -> Result<Bytes, StateDatabaseError> {
        Ok(self.inner.code.get(&code_hash).cloned().unwrap_or_default())
    }
    fn storage(&self, address: Address, key: U256) -> Result<U256, StateDatabaseError> {
        Ok(self.inner.storage.get(&(address, key)).copied().unwrap_or(U256::ZERO))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_account_is_none() {
        let db = MockStateDatabase::default();
        assert_eq!(db.basic(Address::ZERO).unwrap(), None);
    }

    #[test]
    fn inserted_account_round_trips() {
        let addr = Address::from([1u8; 20]);
        let code_hash = B256::repeat_byte(0xAB);
        let db = MockStateDatabase::builder()
            .account(addr, U256::from(100u64), 7, code_hash)
            .build();
        let got = db.basic(addr).unwrap().unwrap();
        assert_eq!(got.balance, U256::from(100u64));
        assert_eq!(got.nonce, 7);
        assert_eq!(got.code_hash, code_hash);
    }

    #[test]
    fn missing_storage_returns_zero() {
        let db = MockStateDatabase::default();
        let v = db.storage(Address::ZERO, U256::from(42u64)).unwrap();
        assert_eq!(v, U256::ZERO);
    }

    #[test]
    fn block_hash_default_is_zero() {
        let db = MockStateDatabase::default();
        assert_eq!(db.block_hash(123).unwrap(), B256::ZERO);
    }
}
```

- [ ] **Step 2: Build and test**

```bash
cargo test -p kardamom-executor state::tests
```

Expected: 4 tests pass.

- [ ] **Step 3: Commit**

```bash
git add crates/kardamom-executor/src/state.rs
git commit -m "executor: MockStateDatabase fixture (StateDatabase trait imported from kardamom-types)"
```

---

## Task 4: Define `ExecutorError` in `error.rs`

**Files:**
- Modify: `crates/kardamom-executor/src/error.rs`

- [ ] **Step 1: Write the error enum**

```rust
//! Errors raised by the executor actor and its helpers.

use crate::types::TxIndex;
use kardamom_types::StateDatabaseError;

#[derive(Debug, thiserror::Error)]
pub enum ExecutorError {
    #[error("state backend error: {0}")]
    State(#[from] StateDatabaseError),

    #[error("revm execution failure at tx {idx:?}: {detail}")]
    Execution { idx: TxIndex, detail: String },

    #[error("out-of-order tx_idx: got {got:?}, expected {expected:?}")]
    OutOfOrderTx { got: TxIndex, expected: TxIndex },

    #[error("block boundary closes before observed end_tx_idx: end={end:?} last_seen={last_seen:?}")]
    BoundaryMisaligned { end: TxIndex, last_seen: TxIndex },

    #[error("channel-B subscription closed")]
    ChannelBClosed,

    #[error("channel-C publication closed")]
    ChannelCClosed,

    #[error("state-writer signal channel closed")]
    StateWriterClosed,
}
```

- [ ] **Step 2: Build**

```bash
cargo check -p kardamom-executor
```

Expected: builds cleanly.

- [ ] **Step 3: Commit**

```bash
git add crates/kardamom-executor/src/error.rs
git commit -m "executor: define ExecutorError"
```

---

## Task 5: Write deterministic per-tx `write_set_hash` in `delta.rs`

**Files:**
- Modify: `crates/kardamom-executor/src/delta.rs`

**Context:** `write_set_hash` is the determinism witness on every `Receipt`. Spec §4.4 requires that any divergence between replicas at the same `tx_idx` triggers chain halt. The hash must:

1. Be independent of insertion order (replicas may discover writes in different microsecond orderings even when sequential, e.g., revm internal map iteration).
2. Cover both account-level updates (balance/nonce/code_hash/destroyed flag) and storage slots.
3. Be cheap (a few hundred ns per tx — this is on the critical latency path).

V0 algorithm: sort every (address, kind, key, value) tuple lexicographically (free via `BTreeMap`), explicit-width / explicit-endian byte-encode, keccak256.

**No state-root / block-delta-root computation lives in this module** (per S0 D-Sh11). The executor does not produce a state-root commitment at any level; per-tx `write_set_hash` is the only determinism witness, and the sealed `BlockBoundary` published on channel C is slim (`{ block_number, end_tx_idx, l2_timestamp }`).

- [ ] **Step 1: Write the per-tx write-set type + hasher**

```rust
//! Per-tx write-set accumulation and deterministic hashing.
//!
//! `WriteSet` is the per-tx unit. `kardamom_types::BlockDelta` is the per-block
//! accumulator the state writer eventually consumes. The crucial invariant is
//! that `WriteSet::hash()` is identical across all executor replicas for any
//! given tx — see spec §4.4 (divergence panic).
//!
//! **No state-root computation here** (S0 D-Sh11). The executor never emits a
//! state-root commitment. Per-tx `write_set_hash` is the sole determinism
//! witness; block-level attestation is a future validator concern.

use std::collections::BTreeMap;

use alloy_primitives::{Address, B256, U256, keccak256};
use kardamom_types::{AccountChange, BlockDelta};

/// One transaction's write effects. `BTreeMap` so iteration is canonical.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct WriteSet {
    pub accounts: BTreeMap<Address, AccountChange>,
    pub storage: BTreeMap<(Address, U256), U256>,
    pub code: BTreeMap<B256, alloy_primitives::Bytes>,
}

impl WriteSet {
    pub fn is_empty(&self) -> bool {
        self.accounts.is_empty() && self.storage.is_empty() && self.code.is_empty()
    }

    /// Deterministic keccak256 hash of the write set.
    ///
    /// Layout (concatenated, then hashed):
    ///   "ACC" || count_be_u32
    ///     || for each (addr asc): addr(20) || balance(32 LE) || nonce(8 LE) || code_hash(32) || destroyed(1)
    ///   "STO" || count_be_u32
    ///     || for each ((addr, key) asc): addr(20) || key(32 BE) || value(32 BE)
    ///   "COD" || count_be_u32
    ///     || for each (code_hash asc): code_hash(32) || bytes_len(8 LE) || bytes
    ///
    /// Stable ordering comes from `BTreeMap`. Numeric encodings are explicit
    /// width + endianness so two replicas on different architectures produce
    /// identical bytes.
    pub fn hash(&self) -> B256 {
        let mut buf: Vec<u8> = Vec::with_capacity(
            3 + 4 + self.accounts.len() * (20 + 32 + 8 + 32 + 1)
                + 3 + 4 + self.storage.len() * (20 + 32 + 32)
                + 3 + 4 + self.code.len() * (32 + 8),
        );

        buf.extend_from_slice(b"ACC");
        buf.extend_from_slice(&(self.accounts.len() as u32).to_be_bytes());
        for (addr, ch) in &self.accounts {
            buf.extend_from_slice(addr.as_slice());
            // U256 has a stable `to_le_bytes::<32>()`; LE chosen because revm internal
            // numeric reps are LE — picking one consistently is the only requirement.
            buf.extend_from_slice(&ch.balance.to_le_bytes::<32>());
            buf.extend_from_slice(&ch.nonce.to_le_bytes());
            buf.extend_from_slice(ch.code_hash.as_slice());
            buf.push(ch.destroyed as u8);
        }

        buf.extend_from_slice(b"STO");
        buf.extend_from_slice(&(self.storage.len() as u32).to_be_bytes());
        for ((addr, key), value) in &self.storage {
            buf.extend_from_slice(addr.as_slice());
            buf.extend_from_slice(&key.to_be_bytes::<32>());
            buf.extend_from_slice(&value.to_be_bytes::<32>());
        }

        buf.extend_from_slice(b"COD");
        buf.extend_from_slice(&(self.code.len() as u32).to_be_bytes());
        for (code_hash, bytes) in &self.code {
            buf.extend_from_slice(code_hash.as_slice());
            buf.extend_from_slice(&(bytes.len() as u64).to_le_bytes());
            buf.extend_from_slice(bytes);
        }

        keccak256(&buf)
    }
}

/// Merge a per-tx WriteSet into the running BlockDelta. Later writes overwrite
/// earlier ones; this matches the sequential execution model — the last tx to
/// touch a slot wins for the block.
pub fn apply_write_set(delta: &mut BlockDelta, ws: WriteSet) {
    for (addr, ch) in ws.accounts {
        delta.accounts.insert(addr, ch);
    }
    for (k, v) in ws.storage {
        delta.storage.insert(k, v);
    }
    for (h, b) in ws.code {
        delta.code.insert(h, b);
    }
}

// NOTE: No `block_delta_root` / state-root function here. Per S0 D-Sh11 the
// executor does not compute or publish any state-root commitment. The sealed
// BlockBoundary on channel C is slim; the state writer flushes the delta to
// libmdbx, and that's the end of the executor's role in block closure.

#[cfg(test)]
mod tests {
    use super::*;
    use alloy_primitives::{Address, Bytes, U256};

    fn sample_account(b: u64, n: u64) -> AccountChange {
        AccountChange {
            balance: U256::from(b),
            nonce: n,
            code_hash: B256::repeat_byte(0xCC),
            destroyed: false,
        }
    }

    #[test]
    fn empty_write_set_has_stable_hash() {
        let h1 = WriteSet::default().hash();
        let h2 = WriteSet::default().hash();
        assert_eq!(h1, h2);
    }

    #[test]
    fn hash_is_independent_of_insertion_order() {
        // BTreeMap canonicalizes order, so even if a buggy revm wrapper inserted
        // in different orders, the hash is the same.
        let a1 = Address::from([0x11u8; 20]);
        let a2 = Address::from([0x22u8; 20]);

        let mut ws_a = WriteSet::default();
        ws_a.accounts.insert(a1, sample_account(10, 1));
        ws_a.accounts.insert(a2, sample_account(20, 2));
        ws_a.storage.insert((a1, U256::from(1u64)), U256::from(100u64));
        ws_a.storage.insert((a2, U256::from(2u64)), U256::from(200u64));

        let mut ws_b = WriteSet::default();
        ws_b.accounts.insert(a2, sample_account(20, 2));
        ws_b.accounts.insert(a1, sample_account(10, 1));
        ws_b.storage.insert((a2, U256::from(2u64)), U256::from(200u64));
        ws_b.storage.insert((a1, U256::from(1u64)), U256::from(100u64));

        assert_eq!(ws_a.hash(), ws_b.hash());
    }

    #[test]
    fn hash_differs_on_value_change() {
        let addr = Address::from([0x11u8; 20]);

        let mut ws_a = WriteSet::default();
        ws_a.storage.insert((addr, U256::from(1u64)), U256::from(100u64));

        let mut ws_b = WriteSet::default();
        ws_b.storage.insert((addr, U256::from(1u64)), U256::from(101u64));

        assert_ne!(ws_a.hash(), ws_b.hash());
    }

    #[test]
    fn hash_differs_on_destroyed_flag() {
        let addr = Address::from([0x11u8; 20]);
        let mut ws_a = WriteSet::default();
        ws_a.accounts.insert(addr, sample_account(0, 0));
        let mut ws_b = WriteSet::default();
        let mut destroyed = sample_account(0, 0);
        destroyed.destroyed = true;
        ws_b.accounts.insert(addr, destroyed);
        assert_ne!(ws_a.hash(), ws_b.hash());
    }

    #[test]
    fn hash_covers_code_bytes() {
        let h = B256::repeat_byte(0xAA);
        let mut ws_a = WriteSet::default();
        ws_a.code.insert(h, Bytes::from_static(&[0x60, 0x00]));
        let mut ws_b = WriteSet::default();
        ws_b.code.insert(h, Bytes::from_static(&[0x60, 0x01]));
        assert_ne!(ws_a.hash(), ws_b.hash());
    }

    #[test]
    fn apply_write_set_merges_and_overwrites() {
        let addr = Address::from([0x11u8; 20]);
        let mut delta = BlockDelta::default();

        let mut ws1 = WriteSet::default();
        ws1.accounts.insert(addr, sample_account(10, 1));
        ws1.storage.insert((addr, U256::from(1u64)), U256::from(100u64));
        apply_write_set(&mut delta, ws1);

        let mut ws2 = WriteSet::default();
        ws2.accounts.insert(addr, sample_account(15, 2));
        ws2.storage.insert((addr, U256::from(1u64)), U256::from(200u64));
        apply_write_set(&mut delta, ws2);

        assert_eq!(delta.accounts[&addr].balance, U256::from(15u64));
        assert_eq!(delta.accounts[&addr].nonce, 2);
        assert_eq!(delta.storage[&(addr, U256::from(1u64))], U256::from(200u64));
    }
}
```

- [ ] **Step 2: Build and test**

```bash
cargo test -p kardamom-executor delta::tests
```

Expected: 6 tests pass. (One less than the prior draft — the `block_delta_root_is_deterministic` test is removed along with the function it tested, per S0 D-Sh11.)

- [ ] **Step 3: Commit**

```bash
git add crates/kardamom-executor/src/delta.rs
git commit -m "executor: per-tx write_set_hash (no state-root commitment per S0 D-Sh11)"
```

---

## Task 6: Hash-invariance property test (permutation-independence)

**Files:**
- Create: `crates/kardamom-executor/tests/hash_invariance.rs`

- [ ] **Step 1: Write the property test**

```rust
//! Permutation-invariance and value-change sensitivity tests for write_set_hash.
//!
//! Property: for any WriteSet `ws`, building `ws'` by inserting the same
//! (addr, kind, key, value) tuples in a randomly shuffled order yields
//! `ws'.hash() == ws.hash()`. Sensitivity: changing any single value flips
//! the hash.

use alloy_primitives::{Address, B256, U256};
use kardamom_executor::delta::WriteSet;
use kardamom_types::AccountChange;
use rand::seq::SliceRandom;
use rand::SeedableRng;
use rand_chacha::ChaCha8Rng;

fn build(
    accounts: &[(Address, AccountChange)],
    storage: &[((Address, U256), U256)],
) -> WriteSet {
    let mut ws = WriteSet::default();
    for (a, c) in accounts {
        ws.accounts.insert(*a, c.clone());
    }
    for (k, v) in storage {
        ws.storage.insert(*k, *v);
    }
    ws
}

fn sample(seed: u64) -> (Vec<(Address, AccountChange)>, Vec<((Address, U256), U256)>) {
    let mut rng = ChaCha8Rng::seed_from_u64(seed);
    use rand::Rng;
    let n_acc = 32;
    let n_sto = 128;
    let accounts: Vec<(Address, AccountChange)> = (0..n_acc)
        .map(|i| {
            let mut a = [0u8; 20];
            rng.fill(&mut a);
            (
                Address::from(a),
                AccountChange {
                    balance: U256::from(i * 7),
                    nonce: i,
                    code_hash: B256::repeat_byte((i % 256) as u8),
                    destroyed: i % 5 == 0,
                },
            )
        })
        .collect();
    let storage: Vec<((Address, U256), U256)> = (0..n_sto)
        .map(|i| {
            let addr = accounts[(i as usize) % n_acc as usize].0;
            ((addr, U256::from(i)), U256::from(i * 13))
        })
        .collect();
    (accounts, storage)
}

#[test]
fn permuting_input_does_not_change_hash() {
    let (accounts, storage) = sample(0xDEADBEEF);
    let base = build(&accounts, &storage).hash();

    let mut rng = ChaCha8Rng::seed_from_u64(0xC0FFEE);
    for _ in 0..16 {
        let mut a = accounts.clone();
        let mut s = storage.clone();
        a.shuffle(&mut rng);
        s.shuffle(&mut rng);
        assert_eq!(build(&a, &s).hash(), base);
    }
}

#[test]
fn flipping_one_storage_value_changes_hash() {
    let (accounts, storage) = sample(42);
    let base = build(&accounts, &storage).hash();
    let mut storage_b = storage.clone();
    storage_b[0].1 = storage_b[0].1 + U256::from(1u64);
    assert_ne!(build(&accounts, &storage_b).hash(), base);
}

#[test]
fn flipping_one_balance_changes_hash() {
    let (accounts, storage) = sample(99);
    let base = build(&accounts, &storage).hash();
    let mut accounts_b = accounts.clone();
    accounts_b[0].1.balance = accounts_b[0].1.balance + U256::from(1u64);
    assert_ne!(build(&accounts_b, &storage).hash(), base);
}
```

- [ ] **Step 2: Run the test**

```bash
cargo test -p kardamom-executor --test hash_invariance
```

Expected: 3 tests pass.

- [ ] **Step 3: Commit**

```bash
git add crates/kardamom-executor/tests/hash_invariance.rs
git commit -m "executor(tests): property tests for write_set_hash invariance"
```

---

## Task 7: Build deterministic `ExecEnv` in `block_env.rs`

**Files:**
- Modify: `crates/kardamom-executor/src/block_env.rs`

**Context:** Determinism (spec invariant I3) requires that every field revm reads from the block context is a pure function of the canonical input (channel B). No wall clocks, no randomness.

- v0 choice for `prevrandao`/`difficulty`: **constant `B256::ZERO`**. A hash-chain over `(prev_root, block_number)` is the v1 plan; for v0 the value just must be deterministic.
- `basefee`: 0. The L2 has no fee market in v0.
- `gas_limit`: 30M (a high but bounded number; matches mainnet block limit).

- [ ] **Step 1: Write the module**

```rust
//! Build a deterministic revm `BlockEnv` / `CfgEnv` for a single executed tx.
//!
//! Spec invariant I3: every field is a pure function of the canonical channel-B
//! input. No wall clocks, no entropy.

use alloy_primitives::U256;
use kardamom_types::BlockBoundaryStart;
use revm::context::{BlockEnv, CfgEnv};

/// Per-block execution context derived from the sealer's BlockBoundaryStart.
/// Stable for every tx in the block; rebuilt at each boundary.
#[derive(Debug, Clone, Copy)]
pub struct ExecEnv {
    pub chain_id: u64,
    pub block_number: u64,
    pub l2_timestamp: u64,
}

impl ExecEnv {
    pub fn new(chain_id: u64, boundary: BlockBoundaryStart) -> Self {
        Self {
            chain_id,
            block_number: boundary.block_number,
            l2_timestamp: boundary.l2_timestamp,
        }
    }

    pub fn block_env(&self) -> BlockEnv {
        BlockEnv {
            number: U256::from(self.block_number),
            timestamp: U256::from(self.l2_timestamp),
            gas_limit: 30_000_000,
            basefee: 0,
            // V0 choice: prevrandao = zero. Deterministic and trivially
            // documented; a per-block hash chain is a v1 follow-up.
            prevrandao: Some(Default::default()),
            ..Default::default()
        }
    }

    /// CfgEnv is `#[non_exhaustive]`; field-by-field assignment is required.
    #[allow(clippy::field_reassign_with_default)]
    pub fn cfg_env(&self) -> CfgEnv {
        let mut c = CfgEnv::default();
        c.chain_id = self.chain_id;
        c
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use kardamom_types::BPosition;

    // Per S0 D-Sh1: BlockBoundaryStart.end_tx_idx is a BPosition (not the
    // executor-local TxIndex). The sealer publishes the position of the last
    // record that belongs to the closing block.
    fn pos(off: i32) -> BPosition { BPosition { term_id: 0, term_offset: off } }

    #[test]
    fn exec_env_carries_boundary_fields() {
        let b = BlockBoundaryStart { block_number: 7, end_tx_idx: pos(42), l2_timestamp: 1_700_000_000 };
        let e = ExecEnv::new(412346, b);
        assert_eq!(e.chain_id, 412346);
        assert_eq!(e.block_number, 7);
        assert_eq!(e.l2_timestamp, 1_700_000_000);
    }

    #[test]
    fn block_env_uses_boundary_timestamp() {
        let b = BlockBoundaryStart { block_number: 1, end_tx_idx: pos(0), l2_timestamp: 12345 };
        let env = ExecEnv::new(1, b).block_env();
        assert_eq!(env.timestamp, U256::from(12345u64));
        assert_eq!(env.number, U256::from(1u64));
    }

    #[test]
    fn cfg_env_carries_chain_id() {
        let b = BlockBoundaryStart { block_number: 1, end_tx_idx: pos(0), l2_timestamp: 0 };
        let cfg = ExecEnv::new(412346, b).cfg_env();
        assert_eq!(cfg.chain_id, 412346);
    }
}
```

- [ ] **Step 2: Build and test**

```bash
cargo test -p kardamom-executor block_env::tests
```

Expected: 3 tests pass.

- [ ] **Step 3: Commit**

```bash
git add crates/kardamom-executor/src/block_env.rs
git commit -m "executor: deterministic ExecEnv from BlockBoundaryStart"
```

---

## Task 8: Per-tx revm execution + write-set capture in `executor.rs`

**Files:**
- Modify: `crates/kardamom-executor/src/executor.rs`

**Context:** The single-tx step function. It bridges (a) the `StateDatabase` trait into revm's `DatabaseRef`, (b) the revm output into a `Receipt`, and (c) the revm state journal into a `WriteSet`. The shape mirrors `crates/node/src/executor.rs::execute` but emits a `WriteSet` instead of mutating a `CacheDB`. The base-state read remains via the `StateDatabase` snapshot; staged writes (from earlier txs in the same block) are layered on top via the `CacheDB` shim we build inside `execute_tx`.

- [ ] **Step 1: Write a `BlockState` wrapper that layers BlockDelta over StateDatabase**

```rust
//! Per-tx revm execution. Adapted from `crates/node/src/executor.rs::execute`.
//!
//! Differences from the node executor:
//! - Reads come from a snapshot-backed `StateDatabase` (Box<dyn>) instead of
//!   an owned `CacheDB`.
//! - Writes are captured into a per-tx `WriteSet` and merged into the running
//!   `BlockDelta` by the caller (the actor in `actor.rs`).
//! - No async, no node-level metrics — those layer above the executor.
//!
//! Per S0 D-Sh4: this module does **not** compute `tx_hash`. It copies
//! `tx_hash` (and `sender`) directly from the inbound `kardamom_types::
//! TxEnvelope`, which the proxy (S1) populated at the system boundary.

use alloy_consensus::Transaction;
use alloy_primitives::{Address, B256, U256};
use kardamom_types::{AccountChange, BlockDelta, Receipt, StateDatabase, StateDatabaseError, TxEnvelope};
use revm::context::result::ExecutionResult;
use revm::context::TxEnv;
use revm::database::{CacheDB, DatabaseRef};
use revm::primitives::{KECCAK_EMPTY, TxKind};
use revm::state::{AccountInfo, Bytecode};
use revm::{Context, ExecuteCommitEvm, MainBuilder, MainContext};

use crate::block_env::ExecEnv;
use crate::delta::WriteSet;
use crate::error::ExecutorError;
use crate::types::{ReceiptStatus, TxIndex};

/// `revm::DatabaseRef` adapter for a `StateDatabase` snapshot. Reads only —
/// writes go to the layered `CacheDB` built per tx in `execute_tx`.
pub struct SnapshotRef<'a> {
    pub inner: &'a dyn StateDatabase,
}

impl<'a> DatabaseRef for SnapshotRef<'a> {
    type Error = StateDatabaseError;

    fn basic_ref(&self, address: Address) -> Result<Option<AccountInfo>, Self::Error> {
        let a = self.inner.basic(address)?;
        Ok(a.map(|s| AccountInfo {
            balance: s.balance,
            nonce: s.nonce,
            code_hash: s.code_hash,
            code: None,
        }))
    }

    fn code_by_hash_ref(&self, code_hash: B256) -> Result<Bytecode, Self::Error> {
        if code_hash == KECCAK_EMPTY {
            return Ok(Bytecode::default());
        }
        let raw = self.inner.code_by_hash(code_hash)?;
        Ok(Bytecode::new_raw(raw))
    }

    fn storage_ref(&self, address: Address, index: U256) -> Result<U256, Self::Error> {
        self.inner.storage(address, index)
    }

    fn block_hash_ref(&self, number: u64) -> Result<B256, Self::Error> {
        self.inner.block_hash(number)
    }
}

/// Decode an `alloy_consensus::TxEnvelope` from the `raw_tx` bytes carried in a
/// `kardamom_types::TxEnvelope`. The signature is already verified by the proxy
/// (S1, S0 D-Sh3); we just need the typed accessors to build a revm `TxEnv`.
fn decode_alloy_envelope(raw_tx: &alloy_primitives::Bytes) -> Result<alloy_consensus::TxEnvelope, ExecutorError> {
    use alloy_eips::eip2718::Decodable2718;
    alloy_consensus::TxEnvelope::decode_2718(&mut raw_tx.as_ref())
        .map_err(|e| ExecutorError::Execution {
            idx: TxIndex::ZERO,
            detail: format!("decode raw_tx: {e}"),
        })
}

/// Convert a recovered tx envelope into a `TxEnv`. `signer` and `tx_hash` come
/// from the inbound `kardamom_types::TxEnvelope` — never recomputed here
/// (S0 D-Sh3 / D-Sh4).
pub fn tx_env_from_alloy(alloy_env: &alloy_consensus::TxEnvelope, signer: Address) -> TxEnv {
    TxEnv {
        caller: signer,
        chain_id: alloy_env.chain_id(),
        nonce: alloy_env.nonce(),
        gas_limit: alloy_env.gas_limit(),
        value: alloy_env.value(),
        data: alloy_env.input().clone(),
        kind: match alloy_env.to() {
            Some(addr) => TxKind::Call(addr),
            None => TxKind::Create,
        },
        gas_price: alloy_env.gas_price().unwrap_or_else(|| alloy_env.max_fee_per_gas()),
        ..Default::default()
    }
}

/// Execute one tx against a snapshot + the current BlockDelta. Returns the
/// receipt plus a fresh per-tx WriteSet. The caller folds the WriteSet into
/// the BlockDelta before invoking the next tx so later txs see the writes.
///
/// `inbound_envelope: &TxEnvelope` is `kardamom_types::TxEnvelope`. Its
/// `sender` and `tx_hash` are trusted unconditionally — the proxy (S1)
/// populated them at the system boundary. The executor **never recomputes
/// `tx_hash`** (S0 D-Sh4) and **never recovers a sender** (S0 D-Sh3); it
/// copies both fields straight through into the outbound `Receipt`.
pub fn execute_tx(
    snapshot: &dyn StateDatabase,
    delta: &BlockDelta,
    env: ExecEnv,
    tx_idx: TxIndex,
    tx_position: kardamom_types::BPosition,
    inbound_envelope: &TxEnvelope,
) -> Result<(Receipt, WriteSet), ExecutorError> {
    let alloy_env = decode_alloy_envelope(&inbound_envelope.raw_tx)?;
    let signer = inbound_envelope.sender; // trusted from proxy; no recovery
    // Layer the running delta on top of the snapshot via CacheDB so revm sees
    // writes from earlier txs in the same block.
    let snap_ref = SnapshotRef { inner: snapshot };
    let mut cache: CacheDB<SnapshotRef<'_>> = CacheDB::new(snap_ref);

    for (addr, ch) in &delta.accounts {
        let code_bytes = delta.code.get(&ch.code_hash).cloned();
        let code = code_bytes.map(Bytecode::new_raw);
        cache.insert_account_info(
            *addr,
            AccountInfo {
                balance: ch.balance,
                nonce: ch.nonce,
                code_hash: ch.code_hash,
                code,
            },
        );
    }
    for ((addr, key), value) in &delta.storage {
        cache
            .insert_account_storage(*addr, *key, *value)
            .map_err(|e| ExecutorError::Execution {
                idx: tx_idx,
                detail: format!("seed storage: {e:?}"),
            })?;
    }

    let tx_env = tx_env_from_alloy(&alloy_env, signer);
    let mut evm = Context::mainnet()
        .with_db(&mut cache)
        .with_block(env.block_env())
        .with_cfg(env.cfg_env())
        .build_mainnet();

    let result = evm
        .transact_commit(tx_env)
        .map_err(|e| ExecutorError::Execution {
            idx: tx_idx,
            detail: format!("{e:?}"),
        })?;

    let (status, gas_used, logs) = match &result {
        ExecutionResult::Success { logs, gas_used, .. } => (ReceiptStatus::Success, *gas_used, logs.clone()),
        ExecutionResult::Revert { gas_used, .. } => (ReceiptStatus::Revert, *gas_used, Vec::new()),
        ExecutionResult::Halt { reason, gas_used } => (ReceiptStatus::Halt(*reason), *gas_used, Vec::new()),
    };

    // Diff the cache against the snapshot/delta to build the WriteSet.
    let ws = diff_cache(&cache, delta);

    let write_set_hash = ws.hash();
    // Build the outbound kardamom_types::Receipt. CRITICAL (S0 D-Sh4): copy
    // `tx_hash` straight from the inbound envelope — DO NOT recompute via
    // keccak256(raw_tx). The proxy (S1) is the canonical hash producer.
    // CRITICAL (S0 D-Sh11): no state_root_commitment field exists on Receipt
    // or anywhere else the executor emits — write_set_hash is the only
    // determinism witness.
    // `kardamom_types::Receipt.tx_idx` is a `BPosition` (S0 D-Sh1). Pass the
    // inbound channel-B position straight through. The executor-local
    // `TxIndex` newtype is kept around for monotonicity bookkeeping and
    // boundary-alignment checks; it never appears on the wire.
    let _ = tx_idx;
    let receipt = Receipt {
        tx_idx: tx_position,
        tx_hash: inbound_envelope.tx_hash,
        status: status.is_success(),
        gas_used,
        logs,
        write_set_hash,
    };
    let _ = signer; // silence unused-binding lint; signer is consumed via tx_env.caller above
    Ok((receipt, ws))
}

/// Build a WriteSet covering only the accounts/storage/code that revm touched
/// in this transaction. The `delta` parameter is the pre-tx running block
/// delta — any cache entry that differs from `delta` (or, if absent in delta,
/// from the snapshot fetched on demand) is a write.
///
/// Implementation note: revm-38's CacheDB exposes `cache.accounts` (account
/// map) and per-account storage. Touch tracking lives on `Account::status` /
/// `status.is_touched()`. We iterate every touched account.
fn diff_cache(
    cache: &CacheDB<SnapshotRef<'_>>,
    pre_delta: &BlockDelta,
) -> WriteSet {
    let mut ws = WriteSet::default();

    for (addr, entry) in cache.cache.accounts.iter() {
        // `account_state.is_touched()` is true iff revm wrote to the account
        // during this tx. (In revm-38 the field is on the AccountInfo-wrapper
        // type; consult the exact accessor when implementing — if the name
        // differs use `entry.account_state` or `entry.info` accordingly.)
        if !entry.account_state.is_touched() {
            continue;
        }

        let info = &entry.info;
        let destroyed = entry.account_state.is_self_destructed();
        let change = AccountChange {
            balance: info.balance,
            nonce: info.nonce,
            code_hash: info.code_hash,
            destroyed,
        };

        // Only record if it differs from the pre-tx delta entry (or is new).
        let pre = pre_delta.accounts.get(addr);
        if pre != Some(&change) {
            ws.accounts.insert(*addr, change);
        }

        // Capture code if revm loaded fresh bytecode this tx and we haven't
        // staged it yet.
        if let Some(code) = info.code.as_ref() {
            let h = info.code_hash;
            if h != KECCAK_EMPTY && !pre_delta.code.contains_key(&h) {
                ws.code.insert(h, alloy_primitives::Bytes::from(code.original_bytes().to_vec()));
            }
        }

        // Storage: revm-38 stores per-account written slots in `entry.storage`.
        for (key, slot) in entry.storage.iter() {
            // Only record present_value != snapshot/pre-delta value.
            let value = slot.present_value();
            let pre_val = pre_delta.storage.get(&(*addr, *key)).copied();
            if pre_val != Some(value) {
                ws.storage.insert((*addr, *key), value);
            }
        }
    }

    ws
}
```

**Implementation note for the engineer:** revm 38's exact field/method names on `CacheDB::cache.accounts` entries may differ slightly from what's shown above (`account_state.is_touched()`, `entry.storage`, `slot.present_value()`). If a name mismatch causes a build error, consult `revm::database::CacheDB` rustdoc for the installed version and adjust; the algorithm is unchanged. If revm-38 does not expose a touched-bit on `CacheDB` entries, replace `diff_cache` with one that compares every cache entry against a fresh `snapshot.basic(addr)` lookup — slower but unambiguous.

- [ ] **Step 2: Write a happy-path unit test**

Append to the same file:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use crate::state::MockStateDatabase;
    use alloy_consensus::{SignableTransaction, TxLegacy};
    use alloy_eips::eip2718::Encodable2718;
    use alloy_network::TxSignerSync;
    use alloy_primitives::{Bytes, TxKind as APTxKind, U256, address, keccak256};
    use alloy_signer_local::PrivateKeySigner;
    use kardamom_types::{BPosition, BlockBoundaryStart, TxEnvelope as KtTxEnvelope};

    fn boundary(block_number: u64) -> BlockBoundaryStart {
        BlockBoundaryStart {
            block_number,
            end_tx_idx: BPosition { term_id: 0, term_offset: 0 },
            l2_timestamp: 0,
        }
    }

    fn pos(off: i32) -> BPosition {
        BPosition { term_id: 0, term_offset: off }
    }

    /// Build a `kardamom_types::TxEnvelope` the way the proxy (S1) would: sign,
    /// 2718-encode to `raw_tx`, populate `sender` from the signer, populate
    /// `tx_hash` as keccak256(raw_tx). The executor under test trusts these
    /// fields blindly — it never recomputes them (S0 D-Sh3 / D-Sh4).
    fn signed_transfer(from: &PrivateKeySigner, to: Address, value: u64, nonce: u64) -> KtTxEnvelope {
        let mut tx = TxLegacy {
            chain_id: Some(1),
            nonce,
            gas_price: 0,
            gas_limit: 21_000,
            to: APTxKind::Call(to),
            value: U256::from(value),
            input: Bytes::new(),
        };
        let sig = from.sign_transaction_sync(&mut tx).expect("sign");
        let alloy_env: alloy_consensus::TxEnvelope = tx.into_signed(sig).into();
        let raw_tx = Bytes::from(alloy_env.encoded_2718());
        let tx_hash = keccak256(&raw_tx);
        KtTxEnvelope {
            correlation_id: 0,
            raw_tx,
            sender: from.address(),
            tx_hash,
        }
    }

    #[test]
    fn simple_transfer_produces_write_set_and_success_receipt() {
        let signer = PrivateKeySigner::random();
        let from = signer.address();
        let to = address!("0000000000000000000000000000000000001234");

        let snap = MockStateDatabase::builder()
            .account(from, U256::from(10u128.pow(18)), 0, KECCAK_EMPTY)
            .build();
        let delta = BlockDelta::default();
        let env = ExecEnv::new(1, boundary(1));

        let env_tx = signed_transfer(&signer, to, 1_000, 0);
        let (receipt, ws) = execute_tx(&snap, &delta, env, TxIndex(0), pos(0), &env_tx).unwrap();

        // Per S0 D-Sh4: the receipt's tx_hash MUST equal the inbound envelope's
        // tx_hash byte-for-byte. No recomputation in the executor.
        assert_eq!(receipt.tx_hash, env_tx.tx_hash);
        assert!(receipt.status); // success = bool true (kardamom_types::Receipt)
        assert!(receipt.gas_used >= 21_000);
        // Both accounts touched: sender (balance + nonce) and recipient (balance).
        assert!(ws.accounts.contains_key(&from));
        assert!(ws.accounts.contains_key(&to));
        assert_eq!(ws.accounts[&to].balance, U256::from(1_000u64));
        // No storage or code writes for a plain transfer.
        assert!(ws.storage.is_empty());
        assert!(ws.code.is_empty());
    }

    #[test]
    fn second_tx_sees_first_tx_balance_via_delta() {
        let signer = PrivateKeySigner::random();
        let from = signer.address();
        let to = address!("00000000000000000000000000000000000ABCDE");

        let snap = MockStateDatabase::builder()
            .account(from, U256::from(10u128.pow(18)), 0, KECCAK_EMPTY)
            .build();
        let env = ExecEnv::new(1, boundary(1));

        let mut delta = BlockDelta::default();
        // First transfer.
        let tx1 = signed_transfer(&signer, to, 100, 0);
        let (r1, ws1) = execute_tx(&snap, &delta, env, TxIndex(0), pos(0), &tx1).unwrap();
        assert!(r1.status);
        assert_eq!(r1.tx_hash, tx1.tx_hash); // copied, not recomputed
        crate::delta::apply_write_set(&mut delta, ws1);

        // Second transfer from the same sender; nonce must be 1, sender balance
        // must already be debited 100.
        let tx2 = signed_transfer(&signer, to, 50, 1);
        let (r2, ws2) = execute_tx(&snap, &delta, env, TxIndex(1), pos(1), &tx2).unwrap();
        assert!(r2.status);
        assert_eq!(r2.tx_hash, tx2.tx_hash);
        assert_eq!(ws2.accounts[&to].balance, U256::from(150u64));
        assert_eq!(ws2.accounts[&from].nonce, 2);
    }
}
```

- [ ] **Step 3: Run the tests**

```bash
cargo test -p kardamom-executor executor::tests
```

Expected: 2 tests pass. If a `diff_cache` field-name mismatch fails the build, fix per the implementation note above before this step passes.

- [ ] **Step 4: Commit**

```bash
git add crates/kardamom-executor/src/executor.rs
git commit -m "executor: per-tx revm step with WriteSet capture"
```

---

## Task 9: Reader-to-exec channel + ExecutorConfig in `actor.rs` (skeleton)

**Files:**
- Modify: `crates/kardamom-executor/src/actor.rs`

**Context:** The actor wires three threads. This task lays in the struct + configuration + the inbound channel-B trait the reader consumes; the next task adds the reader loop, then the exec loop, then the commit loop.

- [ ] **Step 1: Define the channel-B subscription trait**

```rust
//! Executor actor: reader thread + sequential execution thread + commit thread.
//!
//! Threads communicate via crossbeam-channel queues. The actor itself is
//! `Send`; spawning is the caller's responsibility (use `std::thread::spawn`
//! or a `std::thread::Builder` for stack-size / pinning control).

use std::sync::Arc;
use std::thread::{self, JoinHandle};

use crossbeam_channel::{Receiver, Sender, bounded};
use tracing::debug;

use kardamom_types::{
    BPosition, BlockBoundary, BlockBoundaryStart, BlockDelta, SnapshotSource, StateDatabase,
};

use crate::block_env::ExecEnv;
use crate::delta::apply_write_set;
use crate::error::ExecutorError;
use crate::executor::execute_tx;
use crate::types::{BMessage, CMessage, TxIndex};

/// Subscription to channel B. Implementations: real (Aeron) in `kardamom-log`;
/// test mock in `actor.rs::tests`.
pub trait TxOrderingSubscription: Send {
    /// Block until the next record is available or return Err when the
    /// subscription closes.
    fn next(&mut self) -> Result<BMessage, ExecutorError>;
}

/// Publication handle for channel C.
pub trait ChannelCPublication: Send {
    fn publish(&mut self, msg: CMessage) -> Result<(), ExecutorError>;
}

/// Signal from the state writer (S6): "block N is durable in mdbx; you may
/// swap to a snapshot >= N."
pub trait StateWriterSignal: Send {
    /// Block until the state writer reports a block number >= `await_at_least`
    /// has been committed. Returns the committed block number.
    fn wait_committed(&mut self, await_at_least: u64) -> Result<u64, ExecutorError>;
}

// NOTE: `SnapshotSource` is the trait from `kardamom-types` (S0 D-Sh1).
// We do NOT redefine it here. Implementations live in S6 (libmdbx) for
// production and in this crate's tests for hermetic fixtures.

/// Hand-off queue from executor → state writer. The state writer (S6) consumes
/// these to apply the block delta to libmdbx.
pub trait StateWriterQueue: Send {
    fn submit(&mut self, block: BlockBoundary, delta: BlockDelta) -> Result<(), ExecutorError>;
}

#[derive(Debug, Clone)]
pub struct ExecutorConfig {
    pub chain_id: u64,
    /// Bound on the receipt queue between exec and commit threads. Larger =
    /// more amortization, more memory.
    pub receipt_queue_depth: usize,
}

impl Default for ExecutorConfig {
    fn default() -> Self {
        Self { chain_id: 1, receipt_queue_depth: 1024 }
    }
}

/// Owns the three threads. `run` blocks until the channel-B subscription
/// closes or an error occurs.
pub struct Executor;

impl Executor {
    /// Spawn reader, exec, commit threads and join them. Returns when channel B
    /// closes cleanly or when any thread propagates a fatal error.
    pub fn run<B, C, S, Q, P>(
        cfg: ExecutorConfig,
        mut b_sub: B,
        c_pub: C,
        mut snapshots: S,
        mut sw_signal: Q,
        mut sw_queue: P,
        initial_block: u64,
    ) -> Result<(), ExecutorError>
    where
        B: TxOrderingSubscription + 'static,
        C: ChannelCPublication + 'static,
        S: SnapshotSource + 'static,
        Q: StateWriterSignal + 'static,
        P: StateWriterQueue + 'static,
    {
        // Filled in by Task 10–12.
        let _ = (cfg, &mut b_sub, c_pub, &mut snapshots, &mut sw_signal, &mut sw_queue, initial_block);
        Err(ExecutorError::ChannelBClosed)
    }
}
```

- [ ] **Step 2: Build**

```bash
cargo check -p kardamom-executor
```

Expected: builds cleanly (the unused-variable warnings are fine for a skeleton).

- [ ] **Step 3: Commit**

```bash
git add crates/kardamom-executor/src/actor.rs
git commit -m "executor: actor module skeleton (config + channel traits)"
```

---

## Task 10: Implement reader thread

**Files:**
- Modify: `crates/kardamom-executor/src/actor.rs`

**Context:** The reader thread is single-purpose: pull `BMessage` from the channel-B subscription, validate `tx_idx` monotonicity (one off-by-one bug here is a determinism violation), forward to the exec thread.

- [ ] **Step 1: Add the reader loop**

Replace the body of `Executor::run` and add helpers below it:

```rust
/// Internal envelope routed from reader → exec thread.
///
/// `envelope` is `kardamom_types::TxEnvelope` (carries `sender` and `tx_hash`
/// populated by the proxy — S0 D-Sh3 / D-Sh4). No `signer` field separate from
/// the envelope; the executor reads it directly off `envelope.sender`.
enum ReaderToExec {
    Tx { tx_idx: TxIndex, envelope: kardamom_types::TxEnvelope, position: BPosition },
    Boundary(BlockBoundaryStart),
}

/// Internal envelope routed from exec → commit thread.
enum ExecToCommit {
    Receipt(kardamom_types::Receipt),
    Boundary(BlockBoundary),
}

impl Executor {
    pub fn run<B, C, S, Q, P>(
        cfg: ExecutorConfig,
        b_sub: B,
        c_pub: C,
        snapshots: S,
        sw_signal: Q,
        sw_queue: P,
        initial_block: u64,
    ) -> Result<(), ExecutorError>
    where
        B: TxOrderingSubscription + 'static,
        C: ChannelCPublication + 'static,
        S: SnapshotSource + 'static,
        Q: StateWriterSignal + 'static,
        P: StateWriterQueue + 'static,
    {
        let (tx_r2e, rx_r2e) = bounded::<ReaderToExec>(cfg.receipt_queue_depth);
        let (tx_e2c, rx_e2c) = bounded::<ExecToCommit>(cfg.receipt_queue_depth);

        let reader = spawn_reader(b_sub, tx_r2e);
        let exec = spawn_exec(cfg.clone(), rx_r2e, tx_e2c, snapshots, sw_signal, sw_queue, initial_block);
        let commit = spawn_commit(c_pub, rx_e2c);

        // Join in order: reader, exec, commit. The first error wins; remaining
        // joins still complete so threads shut down cleanly.
        let r1 = reader.join().expect("reader panic");
        let r2 = exec.join().expect("exec panic");
        let r3 = commit.join().expect("commit panic");
        r1.and(r2).and(r3)
    }
}

fn spawn_reader<B>(mut b_sub: B, out: Sender<ReaderToExec>) -> JoinHandle<Result<(), ExecutorError>>
where
    B: TxOrderingSubscription + 'static,
{
    thread::Builder::new()
        .name("executor-reader".into())
        .spawn(move || {
            let mut expected: TxIndex = TxIndex::ZERO;
            loop {
                let msg = match b_sub.next() {
                    Ok(m) => m,
                    Err(ExecutorError::ChannelBClosed) => return Ok(()),
                    Err(e) => return Err(e),
                };
                match msg {
                    BMessage::Tx { position, tx_idx, envelope } => {
                        if tx_idx != expected {
                            return Err(ExecutorError::OutOfOrderTx { got: tx_idx, expected });
                        }
                        expected = expected.next();
                        if out.send(ReaderToExec::Tx { tx_idx, envelope, position }).is_err() {
                            return Ok(()); // exec thread shutting down
                        }
                    }
                    BMessage::BlockBoundaryStart(b) => {
                        if out.send(ReaderToExec::Boundary(b)).is_err() {
                            return Ok(());
                        }
                    }
                }
            }
        })
        .expect("spawn reader")
}

fn spawn_exec<S, Q, P>(
    _cfg: ExecutorConfig,
    _rx: Receiver<ReaderToExec>,
    _tx: Sender<ExecToCommit>,
    _snapshots: S,
    _sw_signal: Q,
    _sw_queue: P,
    _initial_block: u64,
) -> JoinHandle<Result<(), ExecutorError>>
where
    S: SnapshotSource + 'static,
    Q: StateWriterSignal + 'static,
    P: StateWriterQueue + 'static,
{
    // Filled in Task 11.
    thread::Builder::new()
        .name("executor-exec".into())
        .spawn(move || Ok(()))
        .expect("spawn exec")
}

fn spawn_commit<C>(_c_pub: C, _rx: Receiver<ExecToCommit>) -> JoinHandle<Result<(), ExecutorError>>
where
    C: ChannelCPublication + 'static,
{
    // Filled in Task 12.
    thread::Builder::new()
        .name("executor-commit".into())
        .spawn(move || Ok(()))
        .expect("spawn commit")
}
```

- [ ] **Step 2: Add a unit test for the reader's out-of-order detection**

Append a test module to `actor.rs`:

```rust
#[cfg(test)]
mod reader_tests {
    use super::*;
    use crate::types::TxIndex;
    use alloy_consensus::{SignableTransaction, TxLegacy};
    use alloy_eips::eip2718::Encodable2718;
    use alloy_network::TxSignerSync;
    use alloy_primitives::{Address, Bytes, TxKind as APTxKind, U256, keccak256};
    use alloy_signer_local::PrivateKeySigner;
    use kardamom_types::TxEnvelope as KtTxEnvelope;
    use std::collections::VecDeque;

    /// Build a proxy-style envelope: sign, encode raw_tx, derive tx_hash.
    fn legacy_envelope(signer: &PrivateKeySigner, nonce: u64) -> KtTxEnvelope {
        let mut tx = TxLegacy {
            chain_id: Some(1),
            nonce,
            gas_price: 0,
            gas_limit: 21_000,
            to: APTxKind::Call(Address::from([0x22u8; 20])),
            value: U256::from(1u64),
            input: Bytes::new(),
        };
        let sig = signer.sign_transaction_sync(&mut tx).unwrap();
        let alloy_env: alloy_consensus::TxEnvelope = tx.into_signed(sig).into();
        let raw_tx = Bytes::from(alloy_env.encoded_2718());
        let tx_hash = keccak256(&raw_tx);
        KtTxEnvelope {
            correlation_id: 0,
            raw_tx,
            sender: signer.address(),
            tx_hash,
        }
    }

    struct VecBSub {
        queue: VecDeque<Result<BMessage, ExecutorError>>,
    }
    impl TxOrderingSubscription for VecBSub {
        fn next(&mut self) -> Result<BMessage, ExecutorError> {
            self.queue
                .pop_front()
                .unwrap_or(Err(ExecutorError::ChannelBClosed))
        }
    }

    #[test]
    fn reader_rejects_out_of_order_tx_idx() {
        let signer = PrivateKeySigner::random();
        let e0 = legacy_envelope(&signer, 0);
        let e2 = legacy_envelope(&signer, 1);
        let pos = |o| BPosition { term_id: 0, term_offset: o };

        let queue = VecDeque::from(vec![
            Ok(BMessage::Tx { position: pos(0), tx_idx: TxIndex(0), envelope: e0 }),
            // Skip 1; emit 2 — must trigger OutOfOrderTx.
            Ok(BMessage::Tx { position: pos(2), tx_idx: TxIndex(2), envelope: e2 }),
        ]);
        let (tx_r2e, _rx_r2e) = bounded::<ReaderToExec>(4);
        let h = spawn_reader(VecBSub { queue }, tx_r2e);
        let res = h.join().expect("no panic");
        assert!(matches!(res, Err(ExecutorError::OutOfOrderTx { got, expected }) if got == TxIndex(2) && expected == TxIndex(1)));
    }

    #[test]
    fn reader_clean_close_returns_ok() {
        let (tx_r2e, _rx_r2e) = bounded::<ReaderToExec>(4);
        let h = spawn_reader(VecBSub { queue: VecDeque::new() }, tx_r2e);
        assert!(h.join().expect("no panic").is_ok());
    }
}
```

- [ ] **Step 3: Build and test**

```bash
cargo test -p kardamom-executor actor::reader_tests
```

Expected: 2 tests pass.

- [ ] **Step 4: Commit**

```bash
git add crates/kardamom-executor/src/actor.rs
git commit -m "executor: reader thread + out-of-order tx_idx detection"
```

---

## Task 11: Implement exec thread (per-tx execution + boundary handling + snapshot swap)

**Files:**
- Modify: `crates/kardamom-executor/src/actor.rs`

**Context:** Exec thread is the heart of the actor. State machine:

```
state = (current_snapshot, current_delta, current_block_number, last_processed_position)
loop:
  msg = rx_r2e.recv()
  match msg:
    Tx{...}:
      (receipt, ws) = execute_tx(snapshot, delta, env, tx_idx, position, envelope)
      apply_write_set(delta, ws)
      tx_e2c.send(ExecToCommit::Receipt(receipt))
      last_processed_position = position
    BlockBoundaryStart{block_number, end_tx_idx, l2_timestamp}:
      assert last_processed_position == end_tx_idx (or BoundaryMisaligned)
      # NO state-root computation (S0 D-Sh11). The BlockBoundary is slim.
      boundary = BlockBoundary{block_number, end_tx_idx, l2_timestamp}
      tx_e2c.send(ExecToCommit::Boundary(boundary))
      sw_queue.submit(boundary, delta_drained)
      sw_signal.wait_committed(block_number)
      snapshot = snapshots.open_at(block_number)
      delta = BlockDelta::default()
      current_block_number = block_number + 1
```

Note `current_block_number` is updated **before** the next tx executes; the next `BlockBoundaryStart` reasserts it. The exec thread tracks both the executor-local `TxIndex` (for monotonicity) and the channel-B `BPosition` (for boundary alignment, since `BlockBoundaryStart.end_tx_idx` is a `BPosition` per S0 D-Sh1).

- [ ] **Step 1: Replace the `spawn_exec` stub**

```rust
fn spawn_exec<S, Q, P>(
    cfg: ExecutorConfig,
    rx: Receiver<ReaderToExec>,
    tx: Sender<ExecToCommit>,
    mut snapshots: S,
    mut sw_signal: Q,
    mut sw_queue: P,
    initial_block: u64,
) -> JoinHandle<Result<(), ExecutorError>>
where
    S: SnapshotSource + 'static,
    Q: StateWriterSignal + 'static,
    P: StateWriterQueue + 'static,
{
    thread::Builder::new()
        .name("executor-exec".into())
        .spawn(move || -> Result<(), ExecutorError> {
            let mut snapshot = snapshots.open_at(initial_block)?;
            let mut delta = BlockDelta::default();
            let mut current_block = initial_block + 1;
            // Set after the first boundary observed for `current_block`; used
            // to validate the end_tx_idx alignment.
            let mut current_l2_ts: u64 = 0;
            // We track both the executor-local TxIndex (monotonic counter for
            // sanity) and the channel-B BPosition (the actual wire identifier
            // BlockBoundaryStart.end_tx_idx compares against, per S0 D-Sh1).
            let mut last_processed_position: Option<BPosition> = None;

            loop {
                let msg = match rx.recv() {
                    Ok(m) => m,
                    Err(_) => return Ok(()),
                };
                match msg {
                    ReaderToExec::Tx { tx_idx, envelope, position } => {
                        let env = ExecEnv {
                            chain_id: cfg.chain_id,
                            block_number: current_block,
                            l2_timestamp: current_l2_ts,
                        };
                        let (receipt, ws) = execute_tx(&*snapshot, &delta, env, tx_idx, position, &envelope)?;
                        apply_write_set(&mut delta, ws);
                        last_processed_position = Some(position);
                        if tx.send(ExecToCommit::Receipt(receipt)).is_err() {
                            return Ok(());
                        }
                    }
                    ReaderToExec::Boundary(BlockBoundaryStart { block_number, end_tx_idx, l2_timestamp }) => {
                        // Alignment: BlockBoundaryStart.end_tx_idx is a BPosition
                        // identifying the LAST channel-B record that belongs to
                        // the closing block. It must match the executor's most
                        // recent processed position.
                        if let Some(lp) = last_processed_position {
                            if lp != end_tx_idx {
                                return Err(ExecutorError::BoundaryMisaligned {
                                    end: end_tx_idx,
                                    last_seen: lp,
                                });
                            }
                        }

                        // S0 D-Sh11: NO state-root computation. The sealed
                        // BlockBoundary on channel C is slim — three fields,
                        // no commitment. State-root attestation is a future
                        // validator concern.
                        let boundary = BlockBoundary {
                            block_number,
                            end_tx_idx,
                            l2_timestamp,
                        };

                        // Drain the delta. We swap it out so the writer owns it.
                        let to_writer = std::mem::take(&mut delta);
                        sw_queue.submit(boundary, to_writer)?;

                        if tx.send(ExecToCommit::Boundary(boundary)).is_err() {
                            return Ok(());
                        }

                        // Wait for the writer to durably commit.
                        let committed = sw_signal.wait_committed(block_number)?;
                        debug!(target: "executor", committed, block_number, "snapshot-swap: writer caught up");

                        // Snapshot swap: open the new snapshot, drop the old.
                        snapshot = snapshots.open_at(block_number)?;
                        current_block = block_number + 1;
                        current_l2_ts = l2_timestamp; // next block uses next boundary's ts; safe placeholder until next boundary.
                    }
                }
            }
        })
        .expect("spawn exec")
}
```

- [ ] **Step 2: Add unit tests for the exec thread**

Append to the `#[cfg(test)] mod reader_tests` (rename to `mod actor_tests` or add a new module). For brevity, append a new module:

```rust
#[cfg(test)]
mod exec_tests {
    use super::*;
    use crate::state::MockStateDatabase;
    use crate::types::TxIndex;
    use alloy_consensus::{SignableTransaction, TxLegacy};
    use alloy_eips::eip2718::Encodable2718;
    use alloy_network::TxSignerSync;
    use alloy_primitives::{Address, Bytes, TxKind as APTxKind, U256, address, keccak256};
    use alloy_signer_local::PrivateKeySigner;
    use kardamom_types::{BPosition, TxEnvelope as KtTxEnvelope};
    use revm::primitives::KECCAK_EMPTY;
    use std::sync::{Arc, Mutex};

    fn pos(off: i32) -> BPosition { BPosition { term_id: 0, term_offset: off } }

    fn legacy(signer: &PrivateKeySigner, to: Address, nonce: u64, value: u64) -> KtTxEnvelope {
        let mut tx = TxLegacy {
            chain_id: Some(1),
            nonce,
            gas_price: 0,
            gas_limit: 21_000,
            to: APTxKind::Call(to),
            value: U256::from(value),
            input: Bytes::new(),
        };
        let sig = signer.sign_transaction_sync(&mut tx).unwrap();
        let alloy_env: alloy_consensus::TxEnvelope = tx.into_signed(sig).into();
        let raw_tx = Bytes::from(alloy_env.encoded_2718());
        let tx_hash = keccak256(&raw_tx);
        KtTxEnvelope {
            correlation_id: 0,
            raw_tx,
            sender: signer.address(),
            tx_hash,
        }
    }

    struct StaticSnap(Arc<MockStateDatabase>);
    impl SnapshotSource for StaticSnap {
        fn open_at(&mut self, _block_number: u64) -> Result<Arc<dyn StateDatabase>, ExecutorError> {
            Ok(self.0.clone() as Arc<dyn StateDatabase>)
        }
    }
    struct ImmediateCommit;
    impl StateWriterSignal for ImmediateCommit {
        fn wait_committed(&mut self, at_least: u64) -> Result<u64, ExecutorError> {
            Ok(at_least)
        }
    }
    struct RecordingQueue(Arc<Mutex<Vec<(BlockBoundary, BlockDelta)>>>);
    impl StateWriterQueue for RecordingQueue {
        fn submit(&mut self, b: BlockBoundary, d: BlockDelta) -> Result<(), ExecutorError> {
            self.0.lock().unwrap().push((b, d));
            Ok(())
        }
    }

    #[test]
    fn exec_runs_two_txs_and_emits_slim_boundary() {
        let signer = PrivateKeySigner::random();
        let from = signer.address();
        let to = address!("00000000000000000000000000000000000ABCDE");

        let snap = Arc::new(
            MockStateDatabase::builder()
                .account(from, U256::from(10u128.pow(18)), 0, KECCAK_EMPTY)
                .build(),
        );
        let writer_log = Arc::new(Mutex::new(Vec::new()));

        let (tx_r2e, rx_r2e) = bounded::<ReaderToExec>(8);
        let (tx_e2c, rx_e2c) = bounded::<ExecToCommit>(8);

        // Pre-load the queue.
        tx_r2e.send(ReaderToExec::Tx {
            tx_idx: TxIndex(0),
            envelope: legacy(&signer, to, 0, 100),
            position: pos(0),
        }).unwrap();
        tx_r2e.send(ReaderToExec::Tx {
            tx_idx: TxIndex(1),
            envelope: legacy(&signer, to, 1, 50),
            position: pos(1),
        }).unwrap();
        tx_r2e.send(ReaderToExec::Boundary(BlockBoundaryStart {
            block_number: 1,
            end_tx_idx: pos(1),
            l2_timestamp: 1_700_000_000,
        })).unwrap();
        drop(tx_r2e);

        let cfg = ExecutorConfig { chain_id: 1, receipt_queue_depth: 8 };
        let h = spawn_exec(
            cfg,
            rx_r2e,
            tx_e2c,
            StaticSnap(snap),
            ImmediateCommit,
            RecordingQueue(writer_log.clone()),
            0,
        );
        h.join().expect("no panic").expect("exec ok");
        drop(rx_e2c);

        let log = writer_log.lock().unwrap();
        assert_eq!(log.len(), 1);
        let (boundary, delta) = &log[0];
        assert_eq!(boundary.block_number, 1);
        assert_eq!(boundary.end_tx_idx, pos(1));
        assert_eq!(boundary.l2_timestamp, 1_700_000_000);
        // Recipient received 150 across both transfers.
        assert_eq!(delta.accounts[&to].balance, U256::from(150u64));
        // The BlockBoundary type has exactly three fields per S0 D-Sh11:
        // block_number, end_tx_idx, l2_timestamp. There is no
        // `state_root_commitment` to assert on; if a future change re-adds one
        // this test should not compile.
    }

    #[test]
    fn exec_rejects_misaligned_boundary() {
        let writer_log = Arc::new(Mutex::new(Vec::new()));

        let (tx_r2e, rx_r2e) = bounded::<ReaderToExec>(8);
        let (tx_e2c, _rx_e2c) = bounded::<ExecToCommit>(8);

        let signer = PrivateKeySigner::random();
        tx_r2e.send(ReaderToExec::Tx {
            tx_idx: TxIndex(0),
            envelope: legacy(&signer, Address::from([0x22u8; 20]), 0, 0),
            position: pos(0),
        }).unwrap();
        // Boundary claims end_tx_idx at offset 5 but we only processed offset 0.
        tx_r2e.send(ReaderToExec::Boundary(BlockBoundaryStart {
            block_number: 1, end_tx_idx: pos(5), l2_timestamp: 0,
        })).unwrap();
        drop(tx_r2e);

        // Pre-fund the signer so the tx doesn't fail before we hit the boundary.
        let snap = Arc::new(
            MockStateDatabase::builder()
                .account(signer.address(), U256::from(10u128.pow(18)), 0, KECCAK_EMPTY)
                .build(),
        );

        let cfg = ExecutorConfig { chain_id: 1, receipt_queue_depth: 8 };
        let h = spawn_exec(cfg, rx_r2e, tx_e2c, StaticSnap(snap), ImmediateCommit, RecordingQueue(writer_log), 0);
        let res = h.join().expect("no panic");
        assert!(matches!(res, Err(ExecutorError::BoundaryMisaligned { .. })));
    }
}
```

- [ ] **Step 3: Run the tests**

```bash
cargo test -p kardamom-executor actor::exec_tests
```

Expected: 2 tests pass.

- [ ] **Step 4: Commit**

```bash
git add crates/kardamom-executor/src/actor.rs
git commit -m "executor: exec thread (per-tx revm + boundary + snapshot swap)"
```

---

## Task 12: Implement commit thread

**Files:**
- Modify: `crates/kardamom-executor/src/actor.rs`

**Context:** The commit thread is intentionally tiny: pull from the receipt queue in arrival order (which is tx_idx order by construction — exec is sequential and queues are FIFO), republish to channel C. Boundaries pass through the same queue inline so consumers of C see receipts and boundaries in canonical order.

- [ ] **Step 1: Replace the `spawn_commit` stub**

```rust
fn spawn_commit<C>(mut c_pub: C, rx: Receiver<ExecToCommit>) -> JoinHandle<Result<(), ExecutorError>>
where
    C: ChannelCPublication + 'static,
{
    thread::Builder::new()
        .name("executor-commit".into())
        .spawn(move || -> Result<(), ExecutorError> {
            loop {
                let msg = match rx.recv() {
                    Ok(m) => m,
                    Err(_) => return Ok(()),
                };
                let c_msg = match msg {
                    ExecToCommit::Receipt(r) => CMessage::Receipt(r),
                    ExecToCommit::Boundary(b) => CMessage::BlockBoundary(b),
                };
                c_pub.publish(c_msg)?;
            }
        })
        .expect("spawn commit")
}
```

- [ ] **Step 2: Add a commit thread unit test**

Append:

```rust
#[cfg(test)]
mod commit_tests {
    use super::*;
    use alloy_primitives::B256;
    use kardamom_types::{BPosition, BlockBoundary, Receipt};
    use std::sync::{Arc, Mutex};

    struct RecordPub(Arc<Mutex<Vec<CMessage>>>);
    impl ChannelCPublication for RecordPub {
        fn publish(&mut self, msg: CMessage) -> Result<(), ExecutorError> {
            self.0.lock().unwrap().push(msg);
            Ok(())
        }
    }

    #[test]
    fn commit_thread_preserves_order() {
        let (tx, rx) = bounded::<ExecToCommit>(8);
        let log = Arc::new(Mutex::new(Vec::new()));
        let pos0 = BPosition { term_id: 0, term_offset: 0 };

        tx.send(ExecToCommit::Receipt(Receipt {
            tx_idx: pos0,
            tx_hash: B256::repeat_byte(0xAA),
            status: true,
            gas_used: 21_000,
            logs: Vec::new(),
            write_set_hash: B256::ZERO,
        })).unwrap();
        // S0 D-Sh11: BlockBoundary has exactly three fields — no state_root_commitment.
        tx.send(ExecToCommit::Boundary(BlockBoundary {
            block_number: 1, end_tx_idx: pos0, l2_timestamp: 100,
        })).unwrap();
        drop(tx);

        let h = spawn_commit(RecordPub(log.clone()), rx);
        h.join().expect("no panic").expect("ok");

        let l = log.lock().unwrap();
        assert_eq!(l.len(), 2);
        assert!(matches!(&l[0], CMessage::Receipt(r) if r.tx_idx == pos0));
        assert!(matches!(&l[1], CMessage::BlockBoundary(b) if b.block_number == 1));
    }
}
```

- [ ] **Step 3: Run tests**

```bash
cargo test -p kardamom-executor actor::commit_tests
```

Expected: 1 test passes.

- [ ] **Step 4: Commit**

```bash
git add crates/kardamom-executor/src/actor.rs
git commit -m "executor: commit thread publishes receipts + boundaries on C"
```

---

## Task 13: Reinstate full re-exports in `lib.rs`

**Files:**
- Modify: `crates/kardamom-executor/src/lib.rs`

- [ ] **Step 1: Update lib.rs**

```rust
//! Kardamom S4 v0 sequential executor.
//!
//! Single-threaded revm executor consuming Aeron channel B (txs +
//! BlockBoundaryStart) and publishing receipts + sealed BlockBoundaries to
//! channel C. Block-STM is explicitly out of scope for v0; S4 v1 will replace
//! the single execution thread with parallel workers behind the same channel
//! interface (`TxOrderingSubscription` / `ChannelCPublication`).
//!
//! See `docs/specs/2026-05-23-high-throughput-sequencer-design.md`
//! §2.4 + V0 scope. Shared types come from `kardamom-types` (S0 D-Sh1).

pub mod actor;
pub mod block_env;
pub mod delta;
pub mod error;
pub mod executor;
pub mod state;
pub mod types;

pub use actor::{
    TxOrderingSubscription, ChannelCPublication, Executor, ExecutorConfig, StateWriterQueue,
    StateWriterSignal,
};
pub use block_env::ExecEnv;
pub use delta::{WriteSet, apply_write_set};
pub use error::ExecutorError;
pub use state::MockStateDatabase;
// Shared types re-exported from kardamom-types so external callers can pull
// them via `kardamom_executor::*` without a separate dependency line.
pub use kardamom_types::{
    AccountChange, AccountState, BPosition, BlockBoundary, BlockBoundaryStart, BlockDelta,
    Receipt, SnapshotSource, StateDatabase, StateDatabaseError, TxEnvelope,
};
pub use types::{BMessage, CMessage, ReceiptStatus, TxIndex};
```

- [ ] **Step 2: Build the whole crate**

```bash
cargo check -p kardamom-executor
cargo test -p kardamom-executor --lib
```

Expected: builds; all in-crate tests pass.

- [ ] **Step 3: Commit**

```bash
git add crates/kardamom-executor/src/lib.rs
git commit -m "executor: re-export public API"
```

---

## Task 14: End-to-end integration test: synthetic N-tx / K-block replay

**Files:**
- Create: `crates/kardamom-executor/tests/replay_integration.rs`

**Context:** Drives the full Executor via mocked channels; asserts the channel-C output matches expectation for a known synthetic stream.

- [ ] **Step 1: Write the test**

```rust
//! Integration test: feed a synthetic stream of (txs + boundaries) into an
//! `Executor` and assert the channel-C output matches expectation.
//!
//! No real Aeron, no real libmdbx — mock channels and `MockStateDatabase`.

use std::sync::Arc;
use std::thread;
use std::time::Duration;

use alloy_consensus::{SignableTransaction, TxLegacy};
use alloy_eips::eip2718::Encodable2718;
use alloy_network::TxSignerSync;
use alloy_primitives::{Address, B256, Bytes, TxKind as APTxKind, U256, address, keccak256};
use alloy_signer_local::PrivateKeySigner;
use crossbeam_channel::{Receiver, Sender, bounded};
use revm::primitives::KECCAK_EMPTY;

use kardamom_executor::{
    BMessage, BPosition, BlockBoundary, BlockBoundaryStart, BlockDelta, CMessage,
    TxOrderingSubscription, ChannelCPublication, Executor, ExecutorConfig, ExecutorError,
    MockStateDatabase, SnapshotSource, StateDatabase, StateWriterQueue, StateWriterSignal,
    TxEnvelope as KtTxEnvelope, TxIndex,
};

struct ChanBSub(Receiver<BMessage>);
impl TxOrderingSubscription for ChanBSub {
    fn next(&mut self) -> Result<BMessage, ExecutorError> {
        self.0.recv().map_err(|_| ExecutorError::ChannelBClosed)
    }
}

struct ChanCPub(Sender<CMessage>);
impl ChannelCPublication for ChanCPub {
    fn publish(&mut self, msg: CMessage) -> Result<(), ExecutorError> {
        self.0.send(msg).map_err(|_| ExecutorError::ChannelCClosed)
    }
}

struct StaticSnap(Arc<MockStateDatabase>);
impl SnapshotSource for StaticSnap {
    fn open_at(&mut self, _: u64) -> Result<Arc<dyn StateDatabase>, ExecutorError> {
        Ok(self.0.clone())
    }
}
struct Imm;
impl StateWriterSignal for Imm {
    fn wait_committed(&mut self, b: u64) -> Result<u64, ExecutorError> { Ok(b) }
}
struct DropQ;
impl StateWriterQueue for DropQ {
    fn submit(&mut self, _: BlockBoundary, _: BlockDelta) -> Result<(), ExecutorError> { Ok(()) }
}

/// Proxy-style envelope builder: sign, encode raw_tx, populate sender + tx_hash.
fn transfer(signer: &PrivateKeySigner, nonce: u64, to: Address, val: u64) -> KtTxEnvelope {
    let mut tx = TxLegacy {
        chain_id: Some(1),
        nonce,
        gas_price: 0,
        gas_limit: 21_000,
        to: APTxKind::Call(to),
        value: U256::from(val),
        input: Bytes::new(),
    };
    let sig = signer.sign_transaction_sync(&mut tx).unwrap();
    let alloy_env: alloy_consensus::TxEnvelope = tx.into_signed(sig).into();
    let raw_tx = Bytes::from(alloy_env.encoded_2718());
    let tx_hash = keccak256(&raw_tx);
    KtTxEnvelope { correlation_id: 0, raw_tx, sender: signer.address(), tx_hash }
}

#[test]
fn replay_10_txs_across_3_blocks_yields_expected_c_stream() {
    let signer = PrivateKeySigner::random();
    let from = signer.address();
    let to = address!("00000000000000000000000000000000000ABCDE");

    let snap = Arc::new(
        MockStateDatabase::builder()
            .account(from, U256::from(10u128.pow(18)), 0, KECCAK_EMPTY)
            .build(),
    );

    let (b_tx, b_rx) = bounded::<BMessage>(64);
    let (c_tx, c_rx) = bounded::<CMessage>(64);

    // 4 txs → boundary block 1 → 3 txs → boundary block 2 → 3 txs → boundary block 3.
    let mut nonce: u64 = 0;
    let mut tx_idx: u64 = 0;
    let mut expected_hashes: Vec<B256> = Vec::new();
    let plan = [(4, 1u64), (3, 2), (3, 3)];
    for (n_txs, blk) in plan {
        for _ in 0..n_txs {
            let env = transfer(&signer, nonce, to, 1);
            expected_hashes.push(env.tx_hash);
            b_tx.send(BMessage::Tx {
                position: BPosition { term_id: 0, term_offset: tx_idx as i32 },
                tx_idx: TxIndex(tx_idx),
                envelope: env,
            }).unwrap();
            nonce += 1;
            tx_idx += 1;
        }
        b_tx.send(BMessage::BlockBoundaryStart(BlockBoundaryStart {
            block_number: blk,
            end_tx_idx: BPosition { term_id: 0, term_offset: (tx_idx as i32) - 1 },
            l2_timestamp: 1_700_000_000 + blk,
        })).unwrap();
    }
    drop(b_tx);

    let cfg = ExecutorConfig { chain_id: 1, receipt_queue_depth: 64 };
    let join = thread::spawn(move || {
        Executor::run(cfg, ChanBSub(b_rx), ChanCPub(c_tx), StaticSnap(snap), Imm, DropQ, 0)
    });

    let mut receipts = 0usize;
    let mut boundaries = 0usize;
    let mut got_hashes: Vec<B256> = Vec::new();
    while let Ok(msg) = c_rx.recv_timeout(Duration::from_secs(5)) {
        match msg {
            CMessage::Receipt(r) => {
                assert!(r.status, "tx {receipts} should succeed");
                assert_ne!(r.write_set_hash, B256::ZERO);
                got_hashes.push(r.tx_hash);
                receipts += 1;
            }
            CMessage::BlockBoundary(b) => {
                // S0 D-Sh11: BlockBoundary has no state_root_commitment field.
                // We assert only the slim three-field shape.
                assert!(b.block_number >= 1 && b.block_number <= 3);
                boundaries += 1;
            }
        }
    }
    assert_eq!(receipts, 10);
    assert_eq!(boundaries, 3);
    // CRITICAL (S0 D-Sh4): every receipt's tx_hash must equal the inbound
    // envelope's tx_hash, byte-for-byte, in the same order. The executor
    // never recomputes — it propagates.
    assert_eq!(got_hashes, expected_hashes);

    join.join().expect("no panic").expect("exec ok");
}
```

- [ ] **Step 2: Run the test**

```bash
cargo test -p kardamom-executor --test replay_integration
```

Expected: pass.

- [ ] **Step 3: Commit**

```bash
git add crates/kardamom-executor/tests/replay_integration.rs
git commit -m "executor(test): end-to-end replay integration"
```

---

## Task 15: Determinism test — two replicas produce byte-identical C streams

**Files:**
- Create: `crates/kardamom-executor/tests/determinism.rs`

**Context:** Spec invariant I3: replicas are byte-identical given the same channel-B input. Drive two `Executor` instances against the same synthetic stream and assert their channel-C output sequences are equal.

- [ ] **Step 1: Write the test**

```rust
//! Determinism conformance: two executor instances driven by the same input
//! must produce byte-identical channel-C output (every `tx_hash` and every
//! `write_set_hash` matches). No state-root assertion: the executor does not
//! emit a state-root commitment (S0 D-Sh11).

use std::sync::Arc;
use std::thread;
use std::time::Duration;

use alloy_consensus::{SignableTransaction, TxLegacy};
use alloy_eips::eip2718::Encodable2718;
use alloy_network::TxSignerSync;
use alloy_primitives::{Bytes, TxKind as APTxKind, U256, address, keccak256};
use alloy_signer_local::PrivateKeySigner;
use crossbeam_channel::{Receiver, Sender, bounded};
use revm::primitives::KECCAK_EMPTY;

use kardamom_executor::{
    BMessage, BPosition, BlockBoundary, BlockBoundaryStart, BlockDelta, CMessage,
    TxOrderingSubscription, ChannelCPublication, Executor, ExecutorConfig, ExecutorError,
    MockStateDatabase, SnapshotSource, StateDatabase, StateWriterQueue, StateWriterSignal,
    TxEnvelope as KtTxEnvelope, TxIndex,
};

struct ChanBSub(Receiver<BMessage>);
impl TxOrderingSubscription for ChanBSub {
    fn next(&mut self) -> Result<BMessage, ExecutorError> {
        self.0.recv().map_err(|_| ExecutorError::ChannelBClosed)
    }
}
struct ChanCPub(Sender<CMessage>);
impl ChannelCPublication for ChanCPub {
    fn publish(&mut self, m: CMessage) -> Result<(), ExecutorError> {
        self.0.send(m).map_err(|_| ExecutorError::ChannelCClosed)
    }
}
struct Snap(Arc<MockStateDatabase>);
impl SnapshotSource for Snap {
    fn open_at(&mut self, _: u64) -> Result<Arc<dyn StateDatabase>, ExecutorError> { Ok(self.0.clone()) }
}
struct Imm;
impl StateWriterSignal for Imm { fn wait_committed(&mut self, b: u64) -> Result<u64, ExecutorError> { Ok(b) } }
struct DropQ;
impl StateWriterQueue for DropQ { fn submit(&mut self, _: BlockBoundary, _: BlockDelta) -> Result<(), ExecutorError> { Ok(()) } }

fn build_stream(_snap: Arc<MockStateDatabase>) -> (Receiver<BMessage>, Sender<CMessage>, Receiver<CMessage>) {
    let (b_tx, b_rx) = bounded::<BMessage>(128);
    let (c_tx, c_rx) = bounded::<CMessage>(128);

    let signer = PrivateKeySigner::from_bytes(&alloy_primitives::B256::repeat_byte(0xCD)).unwrap();
    let from = signer.address();
    let to = address!("00000000000000000000000000000000DEAD0001");

    let mut tx_idx: u64 = 0;
    let mut nonce: u64 = 0;
    for blk in 1..=3u64 {
        for _ in 0..5 {
            let mut tx = TxLegacy {
                chain_id: Some(1),
                nonce,
                gas_price: 0,
                gas_limit: 21_000,
                to: APTxKind::Call(to),
                value: U256::from(1u64),
                input: Bytes::new(),
            };
            let sig = signer.sign_transaction_sync(&mut tx).unwrap();
            let alloy_env: alloy_consensus::TxEnvelope = tx.into_signed(sig).into();
            let raw_tx = Bytes::from(alloy_env.encoded_2718());
            let tx_hash = keccak256(&raw_tx);
            let env = KtTxEnvelope { correlation_id: 0, raw_tx, sender: from, tx_hash };
            b_tx.send(BMessage::Tx {
                position: BPosition { term_id: 0, term_offset: tx_idx as i32 },
                tx_idx: TxIndex(tx_idx),
                envelope: env,
            }).unwrap();
            tx_idx += 1;
            nonce += 1;
        }
        b_tx.send(BMessage::BlockBoundaryStart(BlockBoundaryStart {
            block_number: blk,
            end_tx_idx: BPosition { term_id: 0, term_offset: (tx_idx as i32) - 1 },
            l2_timestamp: 1_700_000_000 + blk,
        })).unwrap();
    }
    drop(b_tx);
    (b_rx, c_tx, c_rx)
}

fn run_one(snap: Arc<MockStateDatabase>) -> Vec<CMessage> {
    let (b_rx, c_tx, c_rx) = build_stream(snap.clone());
    let cfg = ExecutorConfig { chain_id: 1, receipt_queue_depth: 128 };
    let h = thread::spawn(move || {
        Executor::run(cfg, ChanBSub(b_rx), ChanCPub(c_tx), Snap(snap), Imm, DropQ, 0)
    });
    let mut out = Vec::new();
    while let Ok(m) = c_rx.recv_timeout(Duration::from_secs(5)) {
        out.push(m);
    }
    h.join().expect("no panic").expect("ok");
    out
}

#[test]
fn two_replicas_produce_byte_identical_c_stream() {
    let signer = PrivateKeySigner::from_bytes(&alloy_primitives::B256::repeat_byte(0xCD)).unwrap();
    let from = signer.address();
    let snap = Arc::new(
        MockStateDatabase::builder()
            .account(from, U256::from(10u128.pow(18)), 0, KECCAK_EMPTY)
            .build(),
    );

    let a = run_one(snap.clone());
    let b = run_one(snap);

    assert_eq!(a.len(), b.len());
    for (i, (x, y)) in a.iter().zip(b.iter()).enumerate() {
        match (x, y) {
            (CMessage::Receipt(rx), CMessage::Receipt(ry)) => {
                assert_eq!(rx, ry, "receipt mismatch at idx {i}");
            }
            (CMessage::BlockBoundary(bx), CMessage::BlockBoundary(by)) => {
                assert_eq!(bx, by, "boundary mismatch at idx {i}");
            }
            _ => panic!("type mismatch at idx {i}"),
        }
    }
}
```

- [ ] **Step 2: Run**

```bash
cargo test -p kardamom-executor --test determinism
```

Expected: pass.

- [ ] **Step 3: Commit**

```bash
git add crates/kardamom-executor/tests/determinism.rs
git commit -m "executor(test): determinism conformance — two replicas, identical C stream"
```

---

## Task 16: Differential test — receipts match a reference single-threaded path

**Files:**
- Create: `crates/kardamom-executor/tests/diff_reference.rs`

**Context:** We don't have a separate "reference EVM" — revm itself is the reference. The differential value here is structural: replay a corpus of transactions through `Executor` and a hand-rolled "naïve sequential loop that builds a `CacheDB` and calls `transact_commit` directly" (without the actor's queues), then assert receipts (status, gas, logs) match. This pins the actor's WriteSet diff against straight revm execution, catching diff_cache bugs.

Note on the spec's "differential test against historical mainnet-style txs": v0 limits itself to deterministic synthetic corpora (transfers, simple contract calls, a reverting call) because importing a mainnet vector set is itself a sub-project (chain configs, hardfork flags, precompiles). The mainnet differential lives in the **S4 v1** plan; we leave a TODO comment here pointing to that.

- [ ] **Step 1: Write the diff test**

```rust
//! Differential test: actor's receipt for each tx must match a naïve
//! single-threaded `revm` loop's receipt for the same tx.
//!
//! v0 corpus: transfers, a contract `SSTORE`, a revert. Mainnet-vector
//! corpus is a v1 follow-up.

use std::sync::Arc;
use std::thread;
use std::time::Duration;

use alloy_consensus::{SignableTransaction, TxLegacy};
use alloy_eips::eip2718::Encodable2718;
use alloy_network::TxSignerSync;
use alloy_primitives::{Address, Bytes, TxKind as APTxKind, U256, address, keccak256};
use alloy_signer_local::PrivateKeySigner;
use crossbeam_channel::{Receiver, Sender, bounded};
use revm::context::result::ExecutionResult;
use revm::context::{BlockEnv, CfgEnv, TxEnv};
use revm::database::{CacheDB, DatabaseRef};
use revm::primitives::{KECCAK_EMPTY, TxKind};
use revm::state::Bytecode;
use revm::{Context, ExecuteCommitEvm, MainBuilder, MainContext};

use kardamom_executor::executor::SnapshotRef;
use kardamom_executor::{
    BMessage, BPosition, BlockBoundary, BlockBoundaryStart, BlockDelta, CMessage,
    TxOrderingSubscription, ChannelCPublication, Executor, ExecutorConfig, ExecutorError,
    MockStateDatabase, SnapshotSource, StateDatabase, StateWriterQueue, StateWriterSignal,
    TxEnvelope as KtTxEnvelope, TxIndex,
};

// Minimal: PUSH1 0x42; PUSH1 0x00; SSTORE; STOP
const SSTORE_42_AT_0: [u8; 6] = [0x60, 0x42, 0x60, 0x00, 0x55, 0x00];
// PUSH1 0x00; PUSH1 0x00; REVERT
const REVERT_CODE: [u8; 5] = [0x60, 0x00, 0x60, 0x00, 0xfd];

// TxOrderingSubscription / ChannelCPublication mocks (same as Task 14/15).
struct ChanBSub(Receiver<BMessage>);
impl TxOrderingSubscription for ChanBSub {
    fn next(&mut self) -> Result<BMessage, ExecutorError> { self.0.recv().map_err(|_| ExecutorError::ChannelBClosed) }
}
struct ChanCPub(Sender<CMessage>);
impl ChannelCPublication for ChanCPub {
    fn publish(&mut self, m: CMessage) -> Result<(), ExecutorError> { self.0.send(m).map_err(|_| ExecutorError::ChannelCClosed) }
}
struct Snap(Arc<MockStateDatabase>);
impl SnapshotSource for Snap {
    fn open_at(&mut self, _: u64) -> Result<Arc<dyn StateDatabase>, ExecutorError> { Ok(self.0.clone()) }
}
struct Imm;
impl StateWriterSignal for Imm { fn wait_committed(&mut self, b: u64) -> Result<u64, ExecutorError> { Ok(b) } }
struct DropQ;
impl StateWriterQueue for DropQ { fn submit(&mut self, _: BlockBoundary, _: BlockDelta) -> Result<(), ExecutorError> { Ok(()) } }

/// Build a proxy-style `kardamom_types::TxEnvelope` (raw_tx, sender, tx_hash
/// populated). The naïve reference decodes back to alloy for revm.
fn legacy(signer: &PrivateKeySigner, to: APTxKind, nonce: u64, value: u64, data: Bytes, gas: u64) -> KtTxEnvelope {
    let mut tx = TxLegacy {
        chain_id: Some(1),
        nonce,
        gas_price: 0,
        gas_limit: gas,
        to,
        value: U256::from(value),
        input: data,
    };
    let sig = signer.sign_transaction_sync(&mut tx).unwrap();
    let alloy_env: alloy_consensus::TxEnvelope = tx.into_signed(sig).into();
    let raw_tx = Bytes::from(alloy_env.encoded_2718());
    let tx_hash = keccak256(&raw_tx);
    KtTxEnvelope { correlation_id: 0, raw_tx, sender: signer.address(), tx_hash }
}

fn naive_reference(snap: Arc<MockStateDatabase>, txs: &[(KtTxEnvelope, Address)]) -> Vec<(bool, u64)> {
    use alloy_eips::eip2718::Decodable2718;
    let snap_ref = SnapshotRef { inner: &*snap };
    let mut cache: CacheDB<SnapshotRef<'_>> = CacheDB::new(snap_ref);
    let mut out = Vec::new();
    for (kt_env, signer) in txs {
        use alloy_consensus::Transaction;
        let env = alloy_consensus::TxEnvelope::decode_2718(&mut kt_env.raw_tx.as_ref())
            .expect("decode raw_tx");
        let tx_env = TxEnv {
            caller: *signer,
            chain_id: env.chain_id(),
            nonce: env.nonce(),
            gas_limit: env.gas_limit(),
            value: env.value(),
            data: env.input().clone(),
            kind: match env.to() {
                Some(a) => TxKind::Call(a),
                None => TxKind::Create,
            },
            gas_price: env.gas_price().unwrap_or_else(|| env.max_fee_per_gas()),
            ..Default::default()
        };
        #[allow(clippy::field_reassign_with_default)]
        let cfg = { let mut c = CfgEnv::default(); c.chain_id = 1; c };
        let blk = BlockEnv {
            number: U256::from(1u64),
            timestamp: U256::from(1_700_000_000u64),
            gas_limit: 30_000_000,
            basefee: 0,
            prevrandao: Some(Default::default()),
            ..Default::default()
        };
        let mut evm = Context::mainnet()
            .with_db(&mut cache)
            .with_block(blk)
            .with_cfg(cfg)
            .build_mainnet();
        let r = evm.transact_commit(tx_env).expect("commit");
        let (ok, gas_used) = match r {
            ExecutionResult::Success { gas_used, .. } => (true, gas_used),
            ExecutionResult::Revert { gas_used, .. } => (false, gas_used),
            ExecutionResult::Halt { gas_used, .. } => (false, gas_used),
        };
        out.push((ok, gas_used));
    }
    out
}

#[test]
fn actor_receipts_match_naive_reference() {
    let signer = PrivateKeySigner::random();
    let from = signer.address();
    let to = address!("00000000000000000000000000000000000ABCDE");
    let sstore_addr = address!("00000000000000000000000000000000000ABC55");
    let revert_addr = address!("00000000000000000000000000000000000ABCFD");

    let sstore_code = Bytes::from_static(&SSTORE_42_AT_0);
    let revert_code = Bytes::from_static(&REVERT_CODE);
    let sstore_hash = Bytecode::new_raw(sstore_code.clone()).hash_slow();
    let revert_hash = Bytecode::new_raw(revert_code.clone()).hash_slow();

    let snap = Arc::new(
        MockStateDatabase::builder()
            .account(from, U256::from(10u128.pow(18)), 0, KECCAK_EMPTY)
            .account(sstore_addr, U256::ZERO, 1, sstore_hash)
            .account(revert_addr, U256::ZERO, 1, revert_hash)
            .code(sstore_hash, sstore_code)
            .code(revert_hash, revert_code)
            .build(),
    );

    let txs = vec![
        legacy(&signer, APTxKind::Call(to), 0, 10, Bytes::new(), 21_000),
        legacy(&signer, APTxKind::Call(sstore_addr), 1, 0, Bytes::new(), 100_000),
        legacy(&signer, APTxKind::Call(revert_addr), 2, 0, Bytes::new(), 100_000),
    ];
    let signers = vec![from, from, from];
    let pairs: Vec<(KtTxEnvelope, Address)> = txs.iter().cloned().zip(signers).collect();

    let reference = naive_reference(snap.clone(), &pairs);

    // Now drive the actor.
    let (b_tx, b_rx) = bounded::<BMessage>(8);
    let (c_tx, c_rx) = bounded::<CMessage>(8);
    for (i, (env, _sg)) in pairs.iter().enumerate() {
        b_tx.send(BMessage::Tx {
            position: BPosition { term_id: 0, term_offset: i as i32 },
            tx_idx: TxIndex(i as u64),
            envelope: env.clone(),
        }).unwrap();
    }
    b_tx.send(BMessage::BlockBoundaryStart(BlockBoundaryStart {
        block_number: 1,
        end_tx_idx: BPosition { term_id: 0, term_offset: (pairs.len() - 1) as i32 },
        l2_timestamp: 1_700_000_000,
    })).unwrap();
    drop(b_tx);

    let h = thread::spawn(move || {
        Executor::run(
            ExecutorConfig { chain_id: 1, receipt_queue_depth: 8 },
            ChanBSub(b_rx), ChanCPub(c_tx), Snap(snap), Imm, DropQ, 0,
        )
    });

    let mut actor = Vec::new();
    while let Ok(m) = c_rx.recv_timeout(Duration::from_secs(5)) {
        if let CMessage::Receipt(r) = m {
            // kardamom_types::Receipt.status is a plain bool — success/failure.
            actor.push((r.status, r.gas_used));
        }
    }
    h.join().expect("no panic").expect("ok");

    assert_eq!(actor.len(), reference.len());
    for (i, (a, r)) in actor.iter().zip(reference.iter()).enumerate() {
        assert_eq!(a, r, "diff at idx {i}: actor={a:?} reference={r:?}");
    }
}

// TODO(S4 v1): import a mainnet-style tx corpus (historical Uniswap swaps,
// USDC transfers) and re-run this assertion.
```

- [ ] **Step 2: Run**

```bash
cargo test -p kardamom-executor --test diff_reference
```

Expected: pass. (Note: this test imports `kardamom_executor::executor::SnapshotRef`, which means `executor.rs` must mark `SnapshotRef` `pub` — already done in Task 8 — and the `executor` module must be `pub` — already done in Task 13.)

- [ ] **Step 3: Commit**

```bash
git add crates/kardamom-executor/tests/diff_reference.rs
git commit -m "executor(test): actor receipts match naive revm reference"
```

---

## Task 17: Document divergence-panic delegation to `kardamom-log`

**Files:**
- Modify: `crates/kardamom-executor/src/lib.rs` (doc comment only)

**Context:** Per spec §4.4, **divergence detection lives on the consumer side**, not in the executor. Two executor replicas publishing different `write_set_hash` for the same `tx_idx` is what consumers (proxy, state writer) flag — the executor cannot detect divergence by itself. This task adds a doc comment on `lib.rs` pointing readers to the (future) `kardamom-log` consumer-side dedup that performs the panic, and records the test that will live there.

- [ ] **Step 1: Append a "Divergence detection" section to lib.rs**

At the bottom of `crates/kardamom-executor/src/lib.rs`, before the `pub mod` declarations or anywhere convenient, add a `//!` block:

```rust
//! ## Divergence detection
//!
//! Spec §4.4 mandates that two replicas publishing a `Receipt` with the same
//! `tx_idx` but different `write_set_hash` halt the chain. The executor
//! cannot detect this from its own output — it has no visibility into peer
//! replicas. The detection point is the **channel-C consumer** that dedupes
//! by `tx_idx`; that consumer panics on hash mismatch.
//!
//! That consumer lives in the `kardamom-log` crate (S3). The chaos test
//! `cargo test -p kardamom-log --test divergence_halt` (to be added in the
//! S3 plan) injects a "buggy replica" by hand-crafting a `Receipt` with a
//! tweaked `write_set_hash` and asserts the consumer panics. Cross-reference
//! when S3 lands.
```

- [ ] **Step 2: Build**

```bash
cargo check -p kardamom-executor
```

Expected: builds cleanly.

- [ ] **Step 3: Commit**

```bash
git add crates/kardamom-executor/src/lib.rs
git commit -m "executor: document divergence-detection delegation to S3 consumer"
```

---

## Task 18: Criterion benchmark — sequential throughput

**Files:**
- Create: `crates/kardamom-executor/benches/sequential_throughput.rs`

**Context:** Spec target for v0: >50k tx/s on simple transfers, on one core. The bench measures the **execute-per-tx** path (not the actor + channel overhead) plus a separate **actor end-to-end** path. Two scenarios: pure transfers (sender/recipient unique per tx — avoids same-account contention even if it doesn't matter for sequential execution) and a synthetic contract-call scenario (an SSTORE-heavy contract acting as a stand-in for Uniswap-style state writes).

- [ ] **Step 1: Write the bench file**

```rust
//! Criterion: sequential executor throughput.
//!
//! Scenarios:
//!   - `transfer_step`         : just `execute_tx` for plain transfers (per-tx CPU).
//!   - `actor_throughput`      : full actor end-to-end via mock channels.
//!   - `sstore_step`           : `execute_tx` against an SSTORE-heavy contract.

use std::sync::Arc;
use std::thread;
use std::time::Duration;

use alloy_consensus::{SignableTransaction, TxLegacy};
use alloy_eips::eip2718::Encodable2718;
use alloy_network::TxSignerSync;
use alloy_primitives::{Address, Bytes, TxKind as APTxKind, U256, address, keccak256};
use alloy_signer_local::PrivateKeySigner;
use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use crossbeam_channel::{Receiver, Sender, bounded};
use revm::primitives::KECCAK_EMPTY;
use revm::state::Bytecode;

use kardamom_executor::block_env::ExecEnv;
use kardamom_executor::executor::execute_tx;
use kardamom_executor::{
    BMessage, BPosition, BlockBoundary, BlockBoundaryStart, BlockDelta, CMessage,
    TxOrderingSubscription, ChannelCPublication, Executor, ExecutorConfig, ExecutorError,
    MockStateDatabase, SnapshotSource, StateDatabase, StateWriterQueue, StateWriterSignal,
    TxEnvelope as KtTxEnvelope, TxIndex,
};

const SSTORE_42_AT_VAR_KEY: [u8; 8] = [
    0x60, 0x42, // PUSH1 0x42 (value)
    0x60, 0x00, // PUSH1 0x00 (key)
    0x55,       // SSTORE
    0x60, 0x00, // PUSH1 0x00
    0x00,       // STOP
];

fn wrap_envelope(signer: &PrivateKeySigner, alloy_env: alloy_consensus::TxEnvelope) -> KtTxEnvelope {
    let raw_tx = Bytes::from(alloy_env.encoded_2718());
    let tx_hash = keccak256(&raw_tx);
    KtTxEnvelope { correlation_id: 0, raw_tx, sender: signer.address(), tx_hash }
}

fn signed_transfer(signer: &PrivateKeySigner, to: Address, nonce: u64) -> KtTxEnvelope {
    let mut tx = TxLegacy {
        chain_id: Some(1),
        nonce,
        gas_price: 0,
        gas_limit: 21_000,
        to: APTxKind::Call(to),
        value: U256::from(1u64),
        input: Bytes::new(),
    };
    let sig = signer.sign_transaction_sync(&mut tx).unwrap();
    wrap_envelope(signer, tx.into_signed(sig).into())
}

fn signed_sstore_call(signer: &PrivateKeySigner, contract: Address, nonce: u64) -> KtTxEnvelope {
    let mut tx = TxLegacy {
        chain_id: Some(1),
        nonce,
        gas_price: 0,
        gas_limit: 100_000,
        to: APTxKind::Call(contract),
        value: U256::ZERO,
        input: Bytes::new(),
    };
    let sig = signer.sign_transaction_sync(&mut tx).unwrap();
    wrap_envelope(signer, tx.into_signed(sig).into())
}

fn pos(off: i32) -> BPosition { BPosition { term_id: 0, term_offset: off } }

fn bench_transfer_step(c: &mut Criterion) {
    let signer = PrivateKeySigner::random();
    let from = signer.address();
    let to = address!("00000000000000000000000000000000000ABCDE");
    let snap = MockStateDatabase::builder()
        .account(from, U256::MAX, 0, KECCAK_EMPTY)
        .build();
    let env = ExecEnv { chain_id: 1, block_number: 1, l2_timestamp: 0 };

    let mut group = c.benchmark_group("transfer_step");
    group.throughput(Throughput::Elements(1));
    group.bench_function("plain_transfer", |b| {
        let mut nonce: u64 = 0;
        b.iter(|| {
            let delta = BlockDelta::default();
            let env_tx = signed_transfer(&signer, to, nonce);
            let _ = execute_tx(&snap, &delta, env, TxIndex(0), pos(nonce as i32), &env_tx).unwrap();
            nonce += 1;
        })
    });
    group.finish();
}

fn bench_sstore_step(c: &mut Criterion) {
    let signer = PrivateKeySigner::random();
    let from = signer.address();
    let contract = address!("00000000000000000000000000000000000ABC55");
    let code = Bytes::from_static(&SSTORE_42_AT_VAR_KEY);
    let code_hash = Bytecode::new_raw(code.clone()).hash_slow();
    let snap = MockStateDatabase::builder()
        .account(from, U256::MAX, 0, KECCAK_EMPTY)
        .account(contract, U256::ZERO, 1, code_hash)
        .code(code_hash, code)
        .build();
    let env = ExecEnv { chain_id: 1, block_number: 1, l2_timestamp: 0 };

    let mut group = c.benchmark_group("sstore_step");
    group.throughput(Throughput::Elements(1));
    group.bench_function("sstore_one_slot", |b| {
        let mut nonce: u64 = 0;
        b.iter(|| {
            let delta = BlockDelta::default();
            let env_tx = signed_sstore_call(&signer, contract, nonce);
            let (_r, ws) = execute_tx(&snap, &delta, env, TxIndex(0), pos(nonce as i32), &env_tx).unwrap();
            // One storage write expected; the assertion documents the workload.
            assert_eq!(ws.storage.len(), 1);
            nonce += 1;
        })
    });
    group.finish();
}

// Actor end-to-end: 256 txs per iter; reports throughput in tx/s.
struct ChanBSub(Receiver<BMessage>);
impl TxOrderingSubscription for ChanBSub {
    fn next(&mut self) -> Result<BMessage, ExecutorError> { self.0.recv().map_err(|_| ExecutorError::ChannelBClosed) }
}
struct ChanCPub(Sender<CMessage>);
impl ChannelCPublication for ChanCPub {
    fn publish(&mut self, m: CMessage) -> Result<(), ExecutorError> { self.0.send(m).map_err(|_| ExecutorError::ChannelCClosed) }
}
struct Snap(Arc<MockStateDatabase>);
impl SnapshotSource for Snap {
    fn open_at(&mut self, _: u64) -> Result<Arc<dyn StateDatabase>, ExecutorError> { Ok(self.0.clone()) }
}
struct Imm;
impl StateWriterSignal for Imm { fn wait_committed(&mut self, b: u64) -> Result<u64, ExecutorError> { Ok(b) } }
struct DropQ;
impl StateWriterQueue for DropQ { fn submit(&mut self, _: BlockBoundary, _: BlockDelta) -> Result<(), ExecutorError> { Ok(()) } }

fn bench_actor_throughput(c: &mut Criterion) {
    const BATCH: u64 = 256;
    let mut group = c.benchmark_group("actor_throughput");
    group.throughput(Throughput::Elements(BATCH));

    group.bench_function(BenchmarkId::from_parameter("transfers_256"), |b| {
        b.iter(|| {
            let signer = PrivateKeySigner::random();
            let from = signer.address();
            let to = address!("00000000000000000000000000000000DEAD0001");
            let snap = Arc::new(
                MockStateDatabase::builder()
                    .account(from, U256::MAX, 0, KECCAK_EMPTY)
                    .build(),
            );
            let (b_tx, b_rx) = bounded::<BMessage>((BATCH as usize) + 8);
            let (c_tx, c_rx) = bounded::<CMessage>((BATCH as usize) + 8);

            for i in 0..BATCH {
                b_tx.send(BMessage::Tx {
                    position: BPosition { term_id: 0, term_offset: i as i32 },
                    tx_idx: TxIndex(i),
                    envelope: signed_transfer(&signer, to, i),
                }).unwrap();
            }
            b_tx.send(BMessage::BlockBoundaryStart(BlockBoundaryStart {
                block_number: 1,
                end_tx_idx: BPosition { term_id: 0, term_offset: (BATCH as i32) - 1 },
                l2_timestamp: 0,
            })).unwrap();
            drop(b_tx);
            // silence unused-binding warning if `from` not consumed elsewhere
            let _ = from;

            let h = thread::spawn(move || {
                Executor::run(
                    ExecutorConfig { chain_id: 1, receipt_queue_depth: 512 },
                    ChanBSub(b_rx), ChanCPub(c_tx), Snap(snap), Imm, DropQ, 0,
                )
            });

            // Drain.
            let mut got = 0u64;
            while let Ok(m) = c_rx.recv_timeout(Duration::from_secs(10)) {
                if matches!(m, CMessage::Receipt(_)) {
                    got += 1;
                }
            }
            assert_eq!(got, BATCH);
            h.join().expect("no panic").expect("ok");
        });
    });
    group.finish();
}

criterion_group!(benches, bench_transfer_step, bench_sstore_step, bench_actor_throughput);
criterion_main!(benches);
```

- [ ] **Step 2: Smoke-run the bench**

```bash
cargo bench -p kardamom-executor -- --quick
```

Expected: completes; reports times. The exact throughput depends on hardware; the spec floor is 50k tx/s on one core for plain transfers. **Do not assert** a throughput floor in code (flaky in CI); document the observed numbers in the commit message.

- [ ] **Step 3: Commit**

```bash
git add crates/kardamom-executor/benches/sequential_throughput.rs
git commit -m "executor(bench): criterion suite for sequential throughput"
```

---

## Task 19: E2E test against real Aeron in Docker

**Files:**
- Create: `crates/kardamom-executor/tests/docker_aeron_e2e.rs`

**Context:** Per S0 D-Sh8, every component plan must include an e2e task that runs against a real Aeron Media Driver + Archive in Docker, using the `testcontainers` harness shipped by `kardamom-log` (S3) under the `testing` feature. Mocks are not acceptable at the e2e layer — they hide wire-format / back-pressure / fsync-ordering bugs that only surface against real Aeron.

This test:
1. Brings up Aeron containers via the `kardamom-log` testcontainers helper.
2. Publishes a synthetic stream of `TxEnvelope` records and `BlockBoundaryStart` markers onto channel B with rkyv-encoded payloads.
3. Spawns a real `Executor` subscribing to that channel B and publishing to channel C.
4. Drains channel C, asserts the receipts match expectation (one per tx, in order, with the expected `tx_hash` and `write_set_hash`), and asserts the slim `BlockBoundary` (no state-root commitment) closes the block.
5. Tears down the containers.

The test is gated behind `#[cfg(feature = "docker-e2e")]` — actually we leave it on by default but it requires Docker to be available; CI runs it in the Docker-enabled job.

- [ ] **Step 1: Write the test**

```rust
//! E2E: real Aeron Media Driver + Archive in Docker, real `Executor` actor,
//! channel-B input + channel-C output. Per S0 D-Sh8 every component plan
//! ships one of these.
//!
//! Requires Docker. Skipped if `DOCKER_HOST` is unset and the default socket
//! is unavailable — see `harness::docker_available()`.

use std::sync::Arc;
use std::thread;
use std::time::Duration;

use alloy_consensus::{SignableTransaction, TxLegacy};
use alloy_eips::eip2718::Encodable2718;
use alloy_network::TxSignerSync;
use alloy_primitives::{Address, B256, Bytes, TxKind as APTxKind, U256, address, keccak256};
use alloy_signer_local::PrivateKeySigner;
use revm::primitives::KECCAK_EMPTY;

use kardamom_executor::{
    BMessage, BPosition, BlockBoundary, BlockBoundaryStart, BlockDelta, CMessage,
    TxOrderingSubscription, ChannelCPublication, Executor, ExecutorConfig, ExecutorError,
    MockStateDatabase, SnapshotSource, StateDatabase, StateWriterQueue, StateWriterSignal,
    TxEnvelope as KtTxEnvelope, TxIndex,
};
// From kardamom-log's `testing` feature (S3): the Docker harness + the real
// Aeron-backed ChannelB/ChannelC adapters that implement the executor's
// subscription/publication traits.
use kardamom_log::testing::{
    docker_available, AeronTestHarness, RealChannelBPublication,
    RealTxOrderingSubscription, RealChannelCPublication, RealChannelCSubscription,
};

/// Build a proxy-style envelope: sign, 2718-encode, populate sender + tx_hash.
fn build_envelope(signer: &PrivateKeySigner, nonce: u64, to: Address) -> KtTxEnvelope {
    let mut tx = TxLegacy {
        chain_id: Some(1),
        nonce,
        gas_price: 0,
        gas_limit: 21_000,
        to: APTxKind::Call(to),
        value: U256::from(1u64),
        input: Bytes::new(),
    };
    let sig = signer.sign_transaction_sync(&mut tx).unwrap();
    let alloy_env: alloy_consensus::TxEnvelope = tx.into_signed(sig).into();
    let raw_tx = Bytes::from(alloy_env.encoded_2718());
    let tx_hash = keccak256(&raw_tx);
    KtTxEnvelope { correlation_id: nonce, raw_tx, sender: signer.address(), tx_hash }
}

// Adapter from the real Aeron-backed channel-B subscription to the executor's
// trait. `RealTxOrderingSubscription::poll_next` returns archived rkyv views; we
// materialize once here for the executor's BMessage demux.
struct B(RealTxOrderingSubscription);
impl TxOrderingSubscription for B {
    fn next(&mut self) -> Result<BMessage, ExecutorError> {
        loop {
            match self.0.poll_next_blocking(Duration::from_secs(5)) {
                Ok(Some(msg)) => return Ok(msg),
                Ok(None) => return Err(ExecutorError::ChannelBClosed),
                Err(_) => continue, // transient; retry within the e2e timeout
            }
        }
    }
}

struct C(RealChannelCPublication);
impl ChannelCPublication for C {
    fn publish(&mut self, msg: CMessage) -> Result<(), ExecutorError> {
        self.0
            .publish(msg)
            .map_err(|_| ExecutorError::ChannelCClosed)
    }
}

struct StaticSnap(Arc<MockStateDatabase>);
impl SnapshotSource for StaticSnap {
    fn open_at(&mut self, _: u64) -> Result<Arc<dyn StateDatabase>, ExecutorError> {
        Ok(self.0.clone())
    }
}
struct Imm;
impl StateWriterSignal for Imm {
    fn wait_committed(&mut self, b: u64) -> Result<u64, ExecutorError> { Ok(b) }
}
struct DropQ;
impl StateWriterQueue for DropQ {
    fn submit(&mut self, _: BlockBoundary, _: BlockDelta) -> Result<(), ExecutorError> { Ok(()) }
}

#[test]
fn executor_consumes_real_aeron_b_publishes_real_aeron_c() {
    if !docker_available() {
        eprintln!("docker not available; skipping real-aeron e2e test");
        return;
    }

    // 1. Spin up real Aeron Media Driver + Archive in Docker (S3 harness).
    let harness = AeronTestHarness::start();
    let channel_b_uri = harness.channel_b_uri();
    let channel_c_uri = harness.channel_c_uri();

    // 2. Build a publisher onto channel B (the "synthetic sequencer") and a
    // subscriber on channel C (the "test observer that stands in for the proxy
    // / state writer / archiver").
    let mut b_pub: RealChannelBPublication = harness
        .open_channel_b_publication(&channel_b_uri)
        .expect("open b pub");
    let mut c_sub: RealChannelCSubscription = harness
        .open_channel_c_subscription(&channel_c_uri)
        .expect("open c sub");

    // The executor's adapters open *its* subscription on B and publication on C.
    let b_sub_for_exec: RealTxOrderingSubscription = harness
        .open_channel_b_subscription(&channel_b_uri)
        .expect("open b sub for executor");
    let c_pub_for_exec: RealChannelCPublication = harness
        .open_channel_c_publication(&channel_c_uri)
        .expect("open c pub for executor");

    // 3. Pre-fund the synthetic sender so the txs succeed.
    let signer = PrivateKeySigner::random();
    let from = signer.address();
    let to = address!("00000000000000000000000000000000000ABCDE");
    let snap = Arc::new(
        MockStateDatabase::builder()
            .account(from, U256::from(10u128.pow(18)), 0, KECCAK_EMPTY)
            .build(),
    );

    // 4. Publish 5 txs + a BlockBoundaryStart onto real channel B.
    const N: u64 = 5;
    let mut expected_hashes: Vec<B256> = Vec::with_capacity(N as usize);
    for nonce in 0..N {
        let env = build_envelope(&signer, nonce, to);
        expected_hashes.push(env.tx_hash);
        let pos = BPosition { term_id: 0, term_offset: nonce as i32 };
        b_pub
            .publish(BMessage::Tx { position: pos, tx_idx: TxIndex(nonce), envelope: env })
            .expect("publish tx onto real aeron B");
    }
    b_pub
        .publish(BMessage::BlockBoundaryStart(BlockBoundaryStart {
            block_number: 1,
            end_tx_idx: BPosition { term_id: 0, term_offset: (N as i32) - 1 },
            l2_timestamp: 1_700_000_000,
        }))
        .expect("publish boundary onto real aeron B");

    // 5. Spawn the real executor against real Aeron channels.
    let exec_handle = thread::spawn(move || {
        Executor::run(
            ExecutorConfig { chain_id: 1, receipt_queue_depth: 64 },
            B(b_sub_for_exec),
            C(c_pub_for_exec),
            StaticSnap(snap),
            Imm,
            DropQ,
            0,
        )
    });

    // 6. Drain channel C; collect receipts + the slim boundary.
    let mut got_hashes: Vec<B256> = Vec::new();
    let mut got_boundary: Option<BlockBoundary> = None;
    let deadline = std::time::Instant::now() + Duration::from_secs(30);
    while std::time::Instant::now() < deadline {
        match c_sub.poll_next_blocking(Duration::from_millis(500)) {
            Ok(Some(CMessage::Receipt(r))) => {
                assert!(r.status, "tx should succeed on real-aeron e2e");
                assert_ne!(r.write_set_hash, B256::ZERO);
                got_hashes.push(r.tx_hash);
            }
            Ok(Some(CMessage::BlockBoundary(b))) => {
                got_boundary = Some(b);
                break; // boundary is the terminator for this test
            }
            Ok(None) | Err(_) => continue,
        }
    }

    // 7. Assertions.
    assert_eq!(got_hashes, expected_hashes,
        "executor's receipts must carry the proxy-populated tx_hash byte-for-byte (S0 D-Sh4)");
    let b = got_boundary.expect("never observed the BlockBoundary on channel C");
    assert_eq!(b.block_number, 1);
    assert_eq!(b.end_tx_idx, BPosition { term_id: 0, term_offset: (N as i32) - 1 });
    assert_eq!(b.l2_timestamp, 1_700_000_000);
    // S0 D-Sh11: BlockBoundary has exactly three fields (compile-time evidence).
    // The struct destructure below will fail to compile if a state-root field
    // is re-added — keeping a regression guard at the e2e layer too.
    let BlockBoundary { block_number: _, end_tx_idx: _, l2_timestamp: _ } = b;

    // 8. Clean shutdown of the executor (B publisher dropped => B subscription
    // EOF => exec loop returns Ok).
    drop(b_pub);
    exec_handle.join().expect("no exec panic").expect("exec ok");

    // 9. Harness teardown is RAII via Drop on `AeronTestHarness`.
}
```

- [ ] **Step 2: Run the test (requires Docker)**

```bash
docker info > /dev/null 2>&1 && cargo test -p kardamom-executor --test docker_aeron_e2e -- --nocapture
```

Expected: pass when Docker is available. The test self-skips with a printed note when Docker is not available, so it does not break local dev on machines without Docker.

- [ ] **Step 3: Commit**

```bash
git add crates/kardamom-executor/tests/docker_aeron_e2e.rs
git commit -m "executor(e2e): real-Aeron Docker test via S3 testcontainers harness"
```

---

## Task 20: Workspace test sweep + plan close-out

**Files:** none.

- [ ] **Step 1: Build everything**

```bash
cd /home/dev/kardamom
cargo build --workspace
```

Expected: builds cleanly (the new crate is part of the workspace; nothing else depends on it yet so no other crate should regress).

- [ ] **Step 2: Run the full test suite**

```bash
cargo test --workspace
```

Expected: all tests pass — existing node/deployer/bench/kardamom tests, plus the new `kardamom-executor` lib + integration tests.

- [ ] **Step 3: Run formatters**

```bash
cargo fmt --all
```

Expected: no churn (you already formatted as you wrote; this is a no-op check).

- [ ] **Step 4: Commit any fmt fallout**

```bash
git status
# If there are formatting changes:
git add -u
git commit -m "executor: cargo fmt"
```

If `git status` is clean, skip this step.

---

## Open questions (resolve before S4 v1)

1. **State root: validator subsystem.** v0 does **not** compute a state root anywhere (S0 D-Sh11). The future state-root attestation lives in a separate validator subsystem; spec it out as a follow-up.
2. **revm-38 `CacheDB` write extraction API.** Task 8's `diff_cache` relies on `entry.account_state.is_touched()` / `entry.storage.<slot>.present_value()`; the actual revm-38 names may differ. Implementer must consult the rustdoc and adjust. If revm doesn't expose touched-bit information at all, replace with a snapshot-vs-cache diff (slower but unambiguous) — document the choice in the commit.
3. **Snapshot-swap latency budget.** Spec §5 quotes "microseconds" for opening a new mdbx read-txn, but we have not measured it. Once S6 (libmdbx) lands, add a microbenchmark for the swap (open + drop) and confirm it stays inside the 250 ms inter-block window's slack budget.

---

## Plan summary

- **Tasks:** 20 (skeleton → types → state → error → hashing → invariance property → block env → per-tx step → actor skeleton → reader → exec → commit → re-exports → integration replay → determinism → diff vs naive revm → divergence-doc → criterion bench → Docker real-Aeron e2e → workspace sweep).
- **Files created/modified:** 1 new crate, 8 src files, 4 tests/ (including the real-Aeron Docker e2e), 1 bench, 1 Cargo.toml.
- **No code outside `crates/kardamom-executor/` is touched.** `crates/node/src/executor.rs` stays as the in-process RPC node's executor until a later integration spec wires the new executor in.
- **Block-STM remains out of scope for v0.** S4 v1 (separate spec + plan) will replace `spawn_exec`'s single-threaded body with parallel Block-STM workers behind the same `TxOrderingSubscription` / `ChannelCPublication` / `SnapshotSource` traits — no other component should require changes.

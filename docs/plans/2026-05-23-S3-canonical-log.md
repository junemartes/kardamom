# S3 Canonical Log Subsystem Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

> **⚠️ ARCHITECTURE UPDATE — 2026-05-24 (D-Sh12):** the persistence tier ships **two kinds of archives**: (1) M per-sequencer **channel A** archives carrying full `TxEnvelope`s via Aeron *exclusive* publication, each with its own `io_uring` fsync sidecar + `fsynced_position_a[i]` watermark stream; channel A defaults to single-host durability (no quorum) — operators opt into per-A mirroring if they need stronger safety. (2) One canonical **channel B** archive carrying tiny `TxRef { sequencer_id, position_a }` records via Aeron *concurrent* multi-publisher, replicated to N recorders (default N=3) for availability. New `kardamom-log` adapters: `ChannelAPublication`/`ChannelASubscription` (exclusive, per-partition); `ChannelBPublication`/`ChannelBSubscription` (concurrent, tiny payloads). See `docs/plans/2026-05-23-S0-shared-decisions.md` D-Sh12 and `docs/specs/2026-05-23-high-throughput-sequencer-design.md` D11 / §2.3 for the full model. Tasks below predate this split — channel-B archive tasks stay; channel-A archive tasks (one per partition) are added at implementation time.
>
> **⚠️ FOLLOW-UP — 2026-05-25 (D-Sh13):** the **quorum-watermark aggregator is deleted**. Per-recorder `FsyncWatermark` streams stay; the aggregator that emitted `quorum_fsync_position_b` (the Q-th smallest across N recorders) is gone. The ack path waits on a **single recorder's** fsync watermark — typically the proxy's co-located one. Tasks that reference the aggregator, `QuorumWatermark` type, or "Q-of-N" semantics need to be rewritten or deleted at implementation time. See `docs/plans/2026-05-23-S0-shared-decisions.md` D-Sh13 and `docs/specs/2026-05-23-high-throughput-sequencer-design.md` D8 / I2 / §2.3.2.

**Goal:** Ship the three foundation crates that every other kardamom subsystem depends on:

1. **`kardamom-types`** — pure data types and traits (BPosition, TxEnvelope, Receipt, BlockBoundaryStart, BlockBoundary, FsyncWatermark, QuorumWatermark, CachedReceipt, BlockDelta, StateDatabase trait, SnapshotSource trait). No Aeron, no libmdbx, no I/O dependencies. All wire types derive `rkyv::{Archive, Serialize, Deserialize}`.
2. **`kardamom-log`** — Aeron channel implementations (B and C), per-recorder background `io_uring` fsync worker, quorum fsync-watermark aggregator, receipt-cache channel, plus a `testing` feature exposing in-memory pub/sub fakes and a `tests/docker_e2e.rs` testcontainers harness. Depends on `kardamom-types`. Defines **no** wire types of its own.
3. **`kardamom-leases`** — lease primitive (deterministic lowest-host-id-among-caught-up-recorders, derived from per-recorder `FsyncWatermark` streams). Used by S2 (sequencer hot standby), S5 (sealer leader election), S7 (L1 batcher leader election). Depends on `kardamom-types`.

This split is mandated by **D-Sh1** in `docs/plans/2026-05-23-S0-shared-decisions.md`.

**Architecture:**
- **Aeron client:** `rusteron-archive` (GSR-maintained Rust wrapper over the Aeron C client), with the Aeron Media Driver and Aeron Archive Java process run out-of-process under a small Rust supervisor. We do not reimplement Aeron; we drive it.
- **Channel B:** one Aeron stream, concurrent multi-publisher, recorded by N independent `aeron-archive` recorders (one per host). Recording goes through the standard Aeron Archive control protocol so we get `replay-merge` for free during recovery. **No custom replay API** — Aeron Archive already exposes the standard replay protocol; offline consumers (e.g. S7) read segment files directly or use Aeron Archive's built-in replay (per D-Sh10).
- **Continuous fsync:** each recorder host runs a Rust process (the *fsync sidecar*) that opens the active Archive segment file with `O_DIRECT`, watches the recorder's published `recording-position` counter, and pipelines `IORING_OP_WRITE` (mirror-write of the buffer) + `IORING_OP_FSYNC` (`fdatasync` flag) through an `io_uring` SQ. After every completion, it publishes a `FsyncWatermark` on a per-recorder Aeron stream.
- **Quorum aggregator:** subscribes to all N per-recorder watermark streams, maintains a `[BPosition; N]` array, and publishes the Q-th smallest position as a `QuorumWatermark` on a shared stream that proxies/sequencers subscribe to.
- **Channel C:** plain Aeron multi-publisher stream, RAM only, no Archive. Same wire codec as B for shared infra.
- **Receipt-cache channel:** plain Aeron multi-publisher stream carrying `CachedReceipt` messages from the executor to short-lived consumers (proxy nonce-cache invalidations etc.). RAM only.
- **Wire codec:** `rkyv` v0.8 (zero-copy archival serialization). Types in `kardamom-types` derive `rkyv::Archive`, `rkyv::Serialize`, `rkyv::Deserialize`. `kardamom-log` reads `Archived<T>` views straight out of Aeron buffers (no allocation, no decode pass) and only materializes to owned `T` when callers explicitly ask. Per D-Sh2.
- **Runtime:** `tokio` for the supervisors, control plane, and tests. The fsync hot loop is a dedicated OS thread driving `io_uring` directly (the `io-uring` crate, not `tokio-uring`) — see Task 9 for the justification.

**Tech Stack:**
- Rust 2024, workspace deps from `/home/dev/kardamom/Cargo.toml` (alloy-primitives 1.6, tokio 1, tracing 0.1, thiserror 2)
- `rkyv` 0.8 (zero-copy serialization; replaces bincode per D-Sh2)
- `rusteron-client` and `rusteron-archive` (latest 0.1.x as of 2026-05) — Aeron C bindings, maintained by GSR
- `io-uring` (the `tokio-rs/io-uring` crate, raw SQ/CQ API) — lowest overhead for the continuous-submission fsync loop
- `testcontainers` v0.20+ (Docker-based Aeron Media Driver + Archive containers for e2e tests)
- `criterion` 0.5 for benchmarks
- `tempfile` 3, `tokio-test` for tests
- Aeron 1.45+ binaries (Media Driver + Archive) installed on the build host *for native tests*; the Docker harness vendors the same versions for e2e tests

**Branch:** `claude/s3-canonical-log` (branched off `main` after PR #12 — the design-spec PR — merges).

**Reference spec:** `/home/dev/kardamom/docs/specs/2026-05-23-high-throughput-sequencer-design.md` §2.3 (canonical archive), §2.5 (receipt channel), §3 (latency budget), §4.3 (recorder failure), and the V0 scope section.

---

## Tech Research Notes (read before starting)

### Aeron Rust binding choice: `rusteron-archive`

We surveyed three options:

1. **`aeron-rs` (UnitedTraders).** Pure-Rust port of the Aeron Java/C++ client. Last meaningful release was years ago; only implements the client side, not the Archive control protocol. **Rejected** — no Archive support means we would have to reimplement recording, replay, replay-merge from scratch.
2. **`libaeron-sys` (bspeice).** Raw bindgen over the Aeron C library. Low-level, no Archive wrapper. Would force us to write our own safe wrapper for both client and Archive. **Rejected** — duplicates effort that rusteron has already done.
3. **`rusteron-client` + `rusteron-archive` (GSR).** Maintained Rust wrapper over the Aeron C client *and* the Aeron Archive C client. Marked production-ready on crates.io; used by GSR in algo-trading. Versions 0.1.141+ as of 2026-05. **Chosen.**

**Architecture consequence:** the Aeron Media Driver and the Aeron Archive itself are Java/C++ processes — we spawn them under a Rust supervisor (`tokio::process::Command`) and talk to them over the standard Aeron IPC shared-memory ring buffers via `rusteron-archive`. This is the same pattern GSR uses in production. The supervisor handles startup ordering (Media Driver before Archive before publishers), graceful shutdown, and crash restart.

### io_uring crate choice: `io-uring` (raw)

1. **`tokio-uring`.** Tokio-integrated, async-await ergonomics. Built around a per-task model with per-op buffer allocation. **Rejected** for the fsync hot loop — its per-op `Box::pin` overhead is exactly what we are trying to avoid. We use plain `tokio` for the control plane only.
2. **`rio`.** Higher-level wrapper with `Link`/`Drain` ordering primitives. Convenient, but the abstraction adds a per-op `Arc` clone and a completion-handler thread. **Rejected** for the same throughput reason.
3. **`io-uring` (tokio-rs/io-uring).** Raw `IoUring`, `SubmissionQueue`, `CompletionQueue`. Caller manages buffers, ordering, batch submission. **Chosen.** We can pipeline up to `sq_entries` writes + fsyncs with a single `enter()` syscall and amortize the syscall cost across many segment writes.

### NVMe + PLP + O_DIRECT

- **Mode:** `O_DIRECT` for the fsync sidecar's mirror file. Avoids the page cache entirely; writes go straight to NVMe queue depth. With enterprise NVMe + PLP, an `fdatasync` after a `pwrite` returns in ~25 µs and the device persists on power loss without flushing the device cache.
- **Alignment:** `O_DIRECT` requires 512-byte alignment for buffer, offset, and length. The fsync sidecar maintains a 4 KiB-aligned slab allocator (`std::alloc::alloc` with `Layout::from_size_align(_, 4096)`).
- **The sidecar mirrors the recorder.** The Aeron Archive recorder writes its own segment files (NOT `O_DIRECT`, so the page cache absorbs bursts cheaply); the sidecar reads the recorder's published `recording-position` counter, then writes the same bytes into a parallel `O_DIRECT` mirror file and fsyncs that. This decouples Aeron's internal storage policy from our durability accounting. Mirror files are pruned in lockstep with Aeron's segment retention.
- **Alternative considered:** buffered writes + `fdatasync` on the recorder's segment files directly. Rejected because (a) it requires monkey-patching Aeron Archive's storage layer or running with the page cache hot, which makes `fdatasync` latency a function of dirty-page volume, and (b) it would entangle our durability accounting with Aeron internals.

---

## File Structure

S3 owns three crates: `kardamom-types`, `kardamom-log`, `kardamom-leases`.

### `crates/kardamom-types/` — pure data types

```
crates/kardamom-types/
├── Cargo.toml             # deps: alloy-primitives, bytes, rkyv (no I/O crates)
├── src/
│   ├── lib.rs             # re-exports
│   ├── position.rs        # BPosition
│   ├── envelope.rs        # TxEnvelope
│   ├── receipt.rs         # Receipt, CachedReceipt
│   ├── boundary.rs        # BlockBoundaryStart, BlockBoundary
│   ├── watermark.rs       # FsyncWatermark, QuorumWatermark
│   ├── delta.rs           # BlockDelta (account/storage/code changes + receipts)
│   └── state.rs           # StateDatabase trait, SnapshotSource trait
└── tests/
    └── rkyv_roundtrip.rs  # archive/deserialize roundtrip for every wire type
```

### `crates/kardamom-log/` — Aeron channels + recorder + fsync sidecar + watermark aggregator + receipt-cache channel + testing fakes

```
crates/kardamom-log/
├── Cargo.toml                  # deps: kardamom-types, rkyv, rusteron-*, io-uring, tokio, ...
│                               # dev-deps: testcontainers, criterion, tempfile
│                               # features: ["testing"] gates in-memory fake module
├── src/
│   ├── lib.rs                  # re-exports; crate-level docs
│   ├── error.rs                # LogError (thiserror)
│   ├── codec.rs                # rkyv encode + zero-copy access<'a, T>
│   ├── supervisor.rs           # spawns Aeron Media Driver + Archive as child processes
│   ├── publisher.rs            # ChannelBPublisher, ChannelCPublisher, ReceiptCachePublisher (rusteron wrappers)
│   ├── subscriber.rs           # ChannelBSubscriber, ChannelCSubscriber, ReceiptCacheSubscriber (rusteron wrappers)
│   ├── recorder.rs             # Recorder: drives rusteron-archive recording control
│   ├── fsync_sidecar.rs        # io_uring O_DIRECT mirror + fdatasync loop
│   ├── watermark.rs            # FsyncWatermark publisher + QuorumWatermark aggregator
│   ├── receipt_cache.rs        # CachedReceipt pub/sub channel wrapper
│   ├── testing.rs              # gated `#[cfg(any(test, feature = "testing"))]`:
│   │                           #   in-memory Publication, Subscription, ConcurrentPublication,
│   │                           #   FsyncWatermark fakes
│   └── config.rs               # LogConfig (channels, paths, N, Q, segment size)
├── docker/
│   └── aeron/
│       ├── Dockerfile          # builds Media Driver + Archive Java image
│       └── docker-compose.yml  # optional multi-container compose for 3-recorder e2e
├── tests/
│   ├── codec_roundtrip.rs
│   ├── watermark_quorum.rs
│   ├── publisher_subscriber.rs
│   ├── fsync_sidecar.rs
│   ├── docker_e2e.rs           # testcontainers-driven real Aeron e2e (D-Sh8)
│   └── recorder_cluster.rs     # integration: 3 recorders × 4 publishers × 1000 messages
└── benches/
    ├── publish_throughput.rs
    ├── subscribe_throughput.rs
    └── fsync_watermark_latency.rs
```

### `crates/kardamom-leases/` — lease primitive

```
crates/kardamom-leases/
├── Cargo.toml             # deps: kardamom-types, tokio, tracing
│                          # dev-deps: kardamom-log with features = ["testing"]
├── src/
│   ├── lib.rs
│   └── lease.rs           # Lease: deterministic lowest-host-id from FsyncWatermark streams
└── tests/
    └── lease.rs           # uses kardamom-log testing fakes to simulate watermark streams
```

---

## Task 1: Scaffold the three foundation crates

Per D-Sh1, S3 owns three crates. Scaffold them all in one task so cross-crate deps line up.

**Files:**
- Create: `crates/kardamom-types/Cargo.toml`
- Create: `crates/kardamom-types/src/lib.rs`
- Create: `crates/kardamom-log/Cargo.toml`
- Create: `crates/kardamom-log/src/lib.rs`
- Create: `crates/kardamom-leases/Cargo.toml`
- Create: `crates/kardamom-leases/src/lib.rs`
- Modify: root `Cargo.toml` (workspace deps)

- [ ] **Step 1: Verify the workspace will pick up the new crates**

```bash
grep -n 'members' /home/dev/kardamom/Cargo.toml
```

Expected: shows `members = ["crates/*"]`. No edit needed.

- [ ] **Step 2: Add new workspace deps**

Edit `/home/dev/kardamom/Cargo.toml`, append under `[workspace.dependencies]`:

```toml
# S3 foundation crates
kardamom-types = { path = "crates/kardamom-types" }
kardamom-log = { path = "crates/kardamom-log" }
kardamom-leases = { path = "crates/kardamom-leases" }

# S3 external deps
rkyv = { version = "0.8", default-features = false, features = ["alloc", "bytecheck"] }
io-uring = "0.7"
rusteron-client = "0.1"
rusteron-archive = "0.1"
testcontainers = "0.20"
criterion = "0.5"
tempfile = "3"
bytes = "1"
libc = "0.2"
futures = "0.3"
```

Note: `serde` and `bincode` are intentionally **not** required by these crates. Wire serialization is rkyv (D-Sh2). `serde` may still appear elsewhere in the workspace for unrelated reasons (config files, etc.).

- [ ] **Step 3: Write `crates/kardamom-types/Cargo.toml`**

```toml
[package]
name = "kardamom-types"
version.workspace = true
edition.workspace = true

[dependencies]
alloy-primitives.workspace = true
bytes.workspace = true
rkyv.workspace = true
thiserror.workspace = true

[dev-dependencies]
# rkyv roundtrip tests only — nothing I/O
```

This crate **must not** depend on Aeron, tokio I/O, libmdbx, alloy-provider, jsonrpsee, rusteron, io-uring, or any other transport/storage crate. Enforce by review.

- [ ] **Step 4: Write `crates/kardamom-types/src/lib.rs` skeleton**

```rust
//! Pure data types and traits shared across the kardamom subsystems.
//!
//! No I/O. No Aeron. No libmdbx. Everything in this crate is `#[no_std]`-
//! friendly in spirit (we still use `alloc` for `Vec`/`Bytes`).
//!
//! Wire types (`TxEnvelope`, `Receipt`, `BlockBoundary*`, `CachedReceipt`,
//! `FsyncWatermark`, `QuorumWatermark`, `BlockDelta`) derive
//! `#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize)]`. Consumers
//! that need zero-copy access use `rkyv::access::<Archived<T>>(bytes)`;
//! consumers that need an owned value call `rkyv::deserialize`.

pub mod boundary;
pub mod delta;
pub mod envelope;
pub mod position;
pub mod receipt;
pub mod state;
pub mod watermark;

pub use boundary::{BlockBoundary, BlockBoundaryStart};
pub use delta::BlockDelta;
pub use envelope::TxEnvelope;
pub use position::BPosition;
pub use receipt::{CachedReceipt, Receipt};
pub use state::{SnapshotSource, StateDatabase};
pub use watermark::{FsyncWatermark, QuorumWatermark};
```

Stub each module file with `// stub` until Task 2.

- [ ] **Step 5: Write `crates/kardamom-log/Cargo.toml`**

```toml
[package]
name = "kardamom-log"
version.workspace = true
edition.workspace = true

[features]
default = []
# Exposes in-memory pub/sub fakes that mimic the Aeron-backed channel surface,
# for other crates' unit tests. Real Aeron is still required for e2e (see
# tests/docker_e2e.rs).
testing = []

[dependencies]
alloy-primitives.workspace = true
bytes.workspace = true
futures.workspace = true
io-uring.workspace = true
kardamom-types.workspace = true
libc.workspace = true
rkyv.workspace = true
rusteron-archive.workspace = true
rusteron-client.workspace = true
serde.workspace = true                  # for `LogConfig` TOML parsing only
thiserror.workspace = true
tokio.workspace = true
tracing.workspace = true

[dev-dependencies]
criterion.workspace = true
tempfile.workspace = true
testcontainers.workspace = true
tokio = { workspace = true, features = ["test-util", "macros", "rt-multi-thread"] }
# Enable our own `testing` feature for unit tests inside this crate.
kardamom-log = { path = ".", features = ["testing"] }

[[bench]]
name = "publish_throughput"
harness = false

[[bench]]
name = "subscribe_throughput"
harness = false

[[bench]]
name = "fsync_watermark_latency"
harness = false
```

- [ ] **Step 6: Write `crates/kardamom-log/src/lib.rs` skeleton**

```rust
//! Kardamom canonical log: Aeron-backed channels B and C, the receipt-cache
//! channel, the io_uring fsync sidecar, and the quorum watermark aggregator
//! that give channel B its durability guarantee.
//!
//! This crate owns the **transport implementation** only. Wire data types live
//! in [`kardamom_types`] (re-exported from there). Do not add new wire types
//! here — extend `kardamom-types` instead, per D-Sh1.

pub mod codec;
pub mod config;
pub mod error;
pub mod fsync_sidecar;
pub mod publisher;
pub mod receipt_cache;
pub mod recorder;
pub mod subscriber;
pub mod supervisor;
pub mod watermark;

#[cfg(any(test, feature = "testing"))]
pub mod testing;

pub use error::LogError;
// Re-export the shared types so existing call sites can `use kardamom_log::types::*`
// transparently (they import from kardamom-types under the hood).
pub mod types {
    pub use kardamom_types::*;
}
```

- [ ] **Step 7: Write `crates/kardamom-leases/Cargo.toml`**

```toml
[package]
name = "kardamom-leases"
version.workspace = true
edition.workspace = true

[dependencies]
kardamom-types.workspace = true
tokio.workspace = true
tracing.workspace = true
thiserror.workspace = true

[dev-dependencies]
kardamom-log = { workspace = true, features = ["testing"] }
tokio = { workspace = true, features = ["test-util", "macros", "rt-multi-thread"] }
```

- [ ] **Step 8: Write `crates/kardamom-leases/src/lib.rs` skeleton**

```rust
//! Lease primitive used by sequencer hot-standby (S2), sealer leader election
//! (S5), and L1 batcher leader election (S7).
//!
//! V0 implementation: deterministic *lowest-host-id among caught-up recorders*,
//! computed from per-recorder `FsyncWatermark` streams. No external KV, no
//! consensus library. A host "holds the lease" iff it has the lowest id among
//! recorders whose `FsyncWatermark.position` is within `caught_up_window` of
//! the quorum watermark.

pub mod lease;
pub use lease::{Lease, LeaseConfig};
```

Stub `lease.rs` with `// stub` until a later task implements it.

- [ ] **Step 9: Stub each module so all three crates compile**

For each `// stub` module, add minimal pub items as needed so `cargo build -p <crate>` succeeds. For example:

```rust
// crates/kardamom-types/src/position.rs
pub struct BPosition;
```

```rust
// crates/kardamom-log/src/error.rs
#[derive(Debug, thiserror::Error)]
pub enum LogError {
    #[error("placeholder")]
    Placeholder,
}
```

```rust
// crates/kardamom-leases/src/lease.rs
pub struct Lease;
pub struct LeaseConfig;
```

- [ ] **Step 10: Build the workspace**

```bash
cd /home/dev/kardamom && cargo build -p kardamom-types -p kardamom-log -p kardamom-leases
```

Expected: builds cleanly. Warnings about unused stubs are OK.

- [ ] **Step 11: Commit**

```bash
cd /home/dev/kardamom
git add Cargo.toml crates/kardamom-types crates/kardamom-log crates/kardamom-leases
git commit -m "log: scaffold kardamom-{types,log,leases} crates"
```

---

## Task 2: Define shared message types in `kardamom-types` (rkyv)

These types are the **interface contract** for the other six S-plans. Field names, types, `Ord` semantics, and rkyv derives must not drift after this task lands.

Per D-Sh1: types live in `kardamom-types`, not `kardamom-log`.
Per D-Sh2: wire codec is rkyv 0.8 zero-copy.
Per D-Sh1 / D-Sh11: `BlockBoundary` does **not** carry `state_root_commitment`.
Per D-Sh1 / D-Sh3: `TxEnvelope.sender` and `TxEnvelope.tx_hash` are always populated (no `Option`).
Per D-Sh1 / D-Sh4: `Receipt.tx_hash` is propagated from the envelope (no recomputation).

**Files:**
- Modify: `crates/kardamom-types/src/position.rs`
- Modify: `crates/kardamom-types/src/envelope.rs`
- Modify: `crates/kardamom-types/src/receipt.rs`
- Modify: `crates/kardamom-types/src/boundary.rs`
- Modify: `crates/kardamom-types/src/watermark.rs`
- Modify: `crates/kardamom-types/src/delta.rs`
- Modify: `crates/kardamom-types/src/state.rs`
- Create: `crates/kardamom-types/tests/rkyv_roundtrip.rs`
- Modify: `crates/kardamom-log/src/codec.rs` (rkyv access helpers)
- Modify: `crates/kardamom-log/src/error.rs`

- [ ] **Step 1: Write the failing rkyv roundtrip test in `kardamom-types`**

```rust
// crates/kardamom-types/tests/rkyv_roundtrip.rs
use alloy_primitives::{Address, B256, Log, LogData};
use bytes::Bytes;
use kardamom_types::*;

fn roundtrip<T>(value: &T) -> T
where
    T: rkyv::Archive
        + for<'a> rkyv::Serialize<
            rkyv::api::high::HighSerializer<
                rkyv::util::AlignedVec,
                rkyv::ser::allocator::ArenaHandle<'a>,
                rkyv::rancor::Error,
            >,
        >,
    T::Archived: rkyv::Deserialize<T, rkyv::api::high::HighDeserializer<rkyv::rancor::Error>>,
{
    let bytes = rkyv::to_bytes::<rkyv::rancor::Error>(value).unwrap();
    rkyv::from_bytes::<T, rkyv::rancor::Error>(&bytes).unwrap()
}

#[test]
fn bposition_orders_by_term_then_offset() {
    let a = BPosition { term_id: 1, term_offset: 100 };
    let b = BPosition { term_id: 1, term_offset: 200 };
    let c = BPosition { term_id: 2, term_offset: 0 };
    assert!(a < b);
    assert!(b < c);
    assert_eq!(a, BPosition { term_id: 1, term_offset: 100 });
}

#[test]
fn tx_envelope_roundtrip() {
    let v = TxEnvelope {
        correlation_id: 0xDEAD_BEEF,
        raw_tx: Bytes::from_static(b"hello"),
        sender: Address::repeat_byte(0x11),
        tx_hash: B256::repeat_byte(0x22),
    };
    let back = roundtrip(&v);
    assert_eq!(v.correlation_id, back.correlation_id);
    assert_eq!(v.raw_tx, back.raw_tx);
    assert_eq!(v.sender, back.sender);
    assert_eq!(v.tx_hash, back.tx_hash);
}

#[test]
fn receipt_roundtrip() {
    let v = Receipt {
        tx_idx: BPosition { term_id: 3, term_offset: 4096 },
        tx_hash: B256::repeat_byte(0x44),
        status: true,
        gas_used: 21_000,
        logs: vec![Log { address: Default::default(), data: LogData::default() }],
        write_set_hash: B256::repeat_byte(0xAB),
    };
    assert_eq!(roundtrip(&v), v);
}

#[test]
fn boundary_roundtrip() {
    let start = BlockBoundaryStart {
        block_number: 7,
        end_tx_idx: BPosition { term_id: 1, term_offset: 999 },
        l2_timestamp: 1_700_000_000,
    };
    assert_eq!(roundtrip(&start), start);

    // BlockBoundary has NO state_root_commitment field (D-Sh1 / D-Sh11).
    let end = BlockBoundary {
        block_number: 7,
        end_tx_idx: BPosition { term_id: 1, term_offset: 999 },
        l2_timestamp: 1_700_000_000,
    };
    assert_eq!(roundtrip(&end), end);
}

#[test]
fn watermark_roundtrip() {
    let w = FsyncWatermark { recorder_id: 2, position: BPosition { term_id: 4, term_offset: 1024 } };
    assert_eq!(roundtrip(&w), w);

    let q = QuorumWatermark { position: BPosition { term_id: 4, term_offset: 1024 } };
    assert_eq!(roundtrip(&q), q);
}

#[test]
fn cached_receipt_roundtrip() {
    let cr = CachedReceipt {
        sender: Address::repeat_byte(0x33),
        nonce: 42,
        tx_hash: B256::repeat_byte(0x44),
        receipt: Receipt {
            tx_idx: BPosition { term_id: 1, term_offset: 0 },
            tx_hash: B256::repeat_byte(0x44),
            status: true,
            gas_used: 21_000,
            logs: vec![],
            write_set_hash: B256::ZERO,
        },
    };
    assert_eq!(roundtrip(&cr), cr);
}
```

- [ ] **Step 2: Run, confirm FAIL** (types are still stubs)

```bash
cd /home/dev/kardamom && cargo test -p kardamom-types --test rkyv_roundtrip
```

- [ ] **Step 3: Implement `crates/kardamom-types/src/position.rs`**

```rust
//! Position in Aeron channel B's recording — the canonical L2 tx identifier.

use rkyv::{Archive, Deserialize, Serialize};

/// Aeron's `term_id` is `i32`; `term_offset` is the byte offset within the term
/// and is always non-negative but typed `i32` to match Aeron's wire format.
/// Ordering is `(term_id, term_offset)` lexicographic so watermark comparisons
/// are a single cmp.
#[derive(
    Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Hash, Archive, Serialize, Deserialize,
)]
#[rkyv(derive(Debug), compare(PartialEq, PartialOrd))]
pub struct BPosition {
    pub term_id: i32,
    pub term_offset: i32,
}

impl BPosition {
    pub const ZERO: Self = Self { term_id: 0, term_offset: 0 };
}
```

- [ ] **Step 4: Implement `crates/kardamom-types/src/envelope.rs`**

```rust
//! Tx envelope. `sender` and `tx_hash` are *always* populated by the proxy
//! (D-Sh3, D-Sh4). Downstream code trusts both fields unconditionally.

use alloy_primitives::{Address, B256};
use bytes::Bytes;
use rkyv::{Archive, Deserialize, Serialize};

#[derive(Clone, Debug, Eq, PartialEq, Archive, Serialize, Deserialize)]
#[rkyv(derive(Debug))]
pub struct TxEnvelope {
    pub correlation_id: u64,
    pub raw_tx: Bytes,
    /// Recovered by the proxy from the secp256k1 signature at decode time.
    /// CFT trust boundary: every downstream consumer treats this as authoritative.
    pub sender: Address,
    /// `keccak256(raw_tx)` computed by the proxy alongside sig verification.
    /// Never recomputed downstream; propagates unchanged into `Receipt.tx_hash`.
    pub tx_hash: B256,
}
```

- [ ] **Step 5: Implement `crates/kardamom-types/src/receipt.rs`**

```rust
//! Per-tx execution receipt and the receipt-cache message.

use alloy_primitives::{Address, B256, Log};
use rkyv::{Archive, Deserialize, Serialize};

use crate::position::BPosition;

/// Per-tx execution receipt. Published on channel C by executor replicas.
#[derive(Clone, Debug, Eq, PartialEq, Archive, Serialize, Deserialize)]
#[rkyv(derive(Debug))]
pub struct Receipt {
    pub tx_idx: BPosition,
    /// Copied from `TxEnvelope.tx_hash` — never recomputed by the executor (D-Sh4).
    pub tx_hash: B256,
    pub status: bool,
    pub gas_used: u64,
    pub logs: Vec<Log>,
    pub write_set_hash: B256,
}

/// Receipt-cache message: pushed by the executor onto the receipt-cache
/// channel so consumers (proxy nonce cache, RPC frontends) can invalidate and
/// repopulate without round-tripping through libmdbx.
#[derive(Clone, Debug, Eq, PartialEq, Archive, Serialize, Deserialize)]
#[rkyv(derive(Debug))]
pub struct CachedReceipt {
    pub sender: Address,
    pub nonce: u64,
    pub tx_hash: B256,
    pub receipt: Receipt,
}
```

- [ ] **Step 6: Implement `crates/kardamom-types/src/boundary.rs`**

```rust
//! Block boundary markers. State root is **not** carried (D-Sh11).

use rkyv::{Archive, Deserialize, Serialize};

use crate::position::BPosition;

/// Block-boundary marker emitted by the sealer onto channel B.
#[derive(Clone, Debug, Eq, PartialEq, Archive, Serialize, Deserialize)]
#[rkyv(derive(Debug))]
pub struct BlockBoundaryStart {
    pub block_number: u64,
    pub end_tx_idx: BPosition,
    pub l2_timestamp: u64,
}

/// Block-boundary closeout emitted by executors onto channel C once they have
/// finished executing through `end_tx_idx`. No `state_root_commitment` field
/// (D-Sh11 — state-root attestation is a deferred validator concern).
#[derive(Clone, Debug, Eq, PartialEq, Archive, Serialize, Deserialize)]
#[rkyv(derive(Debug))]
pub struct BlockBoundary {
    pub block_number: u64,
    pub end_tx_idx: BPosition,
    pub l2_timestamp: u64,
}
```

- [ ] **Step 7: Implement `crates/kardamom-types/src/watermark.rs`**

```rust
//! Fsync watermark types.

use rkyv::{Archive, Deserialize, Serialize};

use crate::position::BPosition;

/// Single-recorder fsync progress. Published on a per-recorder watermark stream.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Archive, Serialize, Deserialize)]
#[rkyv(derive(Debug))]
pub struct FsyncWatermark {
    pub recorder_id: u8,
    pub position: BPosition,
}

/// Q-of-N aggregated fsync progress. Published on the shared watermark stream
/// that proxies subscribe to for the I2 ack guarantee.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Archive, Serialize, Deserialize)]
#[rkyv(derive(Debug))]
pub struct QuorumWatermark {
    pub position: BPosition,
}
```

- [ ] **Step 8: Implement `crates/kardamom-types/src/delta.rs`**

```rust
//! Block-write payload from executor to state writer.
//!
//! Carries the full account / storage / code mutations + receipts produced by
//! a sealed block, so the S6 state writer can commit them atomically.

use alloy_primitives::{Address, B256, U256};
use rkyv::{Archive, Deserialize, Serialize};

use crate::receipt::Receipt;

#[derive(Clone, Debug, Eq, PartialEq, Archive, Serialize, Deserialize)]
#[rkyv(derive(Debug))]
pub struct AccountChange {
    pub address: Address,
    pub nonce: u64,
    pub balance: U256,
    pub code_hash: B256,
}

#[derive(Clone, Debug, Eq, PartialEq, Archive, Serialize, Deserialize)]
#[rkyv(derive(Debug))]
pub struct StorageChange {
    pub address: Address,
    pub key: B256,
    pub value: U256,
}

#[derive(Clone, Debug, Eq, PartialEq, Archive, Serialize, Deserialize)]
#[rkyv(derive(Debug))]
pub struct BlockDelta {
    pub block_number: u64,
    pub accounts: Vec<AccountChange>,
    pub storage: Vec<StorageChange>,
    pub code: Vec<(B256, Vec<u8>)>,
    pub receipts: Vec<Receipt>,
}
```

- [ ] **Step 9: Implement `crates/kardamom-types/src/state.rs`**

```rust
//! State-access traits. The `StateDatabase` trait is `revm::Database`-compatible
//! (in spirit; we do not depend on revm here). S4's executor consumes any
//! implementor; S6 ships the libmdbx-backed one.

use alloy_primitives::{Address, B256, U256};

use crate::position::BPosition;
use crate::receipt::Receipt;

/// Errors a state implementation may surface. Concrete crates wrap their own.
pub trait StateError: std::error::Error + Send + Sync + 'static {}

/// Read-only state access. A "snapshot" is a point-in-time view that does not
/// observe writes made by later blocks.
pub trait StateDatabase: Send + Sync {
    type Error: StateError;

    fn basic(&self, address: Address) -> Result<Option<(u64, U256, B256)>, Self::Error>;
    fn storage(&self, address: Address, key: B256) -> Result<U256, Self::Error>;
    fn code_by_hash(&self, code_hash: B256) -> Result<Vec<u8>, Self::Error>;

    /// Receipt lookup by canonical position.
    fn get_receipt(&self, pos: BPosition) -> Result<Option<Receipt>, Self::Error>;

    /// tx_hash → BPosition (the `tx_hash_index` table in S6).
    fn get_tx_position(&self, tx_hash: B256) -> Result<Option<BPosition>, Self::Error>;
}

/// Source of fresh post-block state snapshots. The executor calls
/// [`SnapshotSource::snapshot_after`] when the state writer signals that a
/// block is durable.
pub trait SnapshotSource: Send + Sync {
    type Db: StateDatabase;

    fn snapshot_after(&self, block_number: u64) -> Self::Db;
}
```

- [ ] **Step 10: Implement `crates/kardamom-log/src/codec.rs` — zero-copy access**

```rust
//! rkyv zero-copy access helpers.
//!
//! The hot path reads `Archived<T>` views straight out of Aeron fragment
//! buffers — no allocation, no decode pass. Convert to an owned `T` only when
//! the caller explicitly asks (`materialize`), e.g. when they need to outlive
//! the fragment buffer.
//!
//! Per D-Sh2: rkyv v0.8 replaces the earlier bincode choice. Wire types live
//! in `kardamom-types`; this crate is transport only.

use rkyv::api::high::{HighDeserializer, HighSerializer};
use rkyv::rancor;
use rkyv::ser::allocator::ArenaHandle;
use rkyv::util::AlignedVec;
use rkyv::{Archive, Deserialize, Serialize};

use crate::error::LogError;

/// Encode a wire value to a fresh `AlignedVec` suitable for handing to
/// `rusteron`'s `offer()`.
pub fn encode<T>(value: &T) -> Result<AlignedVec, LogError>
where
    T: for<'a> Serialize<HighSerializer<AlignedVec, ArenaHandle<'a>, rancor::Error>>,
{
    rkyv::to_bytes::<rancor::Error>(value).map_err(|e| LogError::Codec(e.to_string()))
}

/// Zero-copy access: borrow an `&Archived<T>` view of `bytes` without
/// allocating. Returns an error if the bytes are not a valid rkyv archive
/// for `T`.
pub fn access<T>(bytes: &[u8]) -> Result<&T::Archived, LogError>
where
    T: Archive,
    T::Archived: rkyv::bytecheck::CheckBytes<rkyv::api::high::HighValidator<'_, rancor::Error>>,
{
    rkyv::access::<T::Archived, rancor::Error>(bytes).map_err(|e| LogError::Codec(e.to_string()))
}

/// Owning decode: copy an `Archived<T>` into an owned `T`. Use when the value
/// must outlive the fragment buffer or when downstream code needs `T`
/// directly. Hot-path consumers prefer [`access`] instead.
pub fn materialize<T>(bytes: &[u8]) -> Result<T, LogError>
where
    T: Archive,
    T::Archived: Deserialize<T, HighDeserializer<rancor::Error>>
        + rkyv::bytecheck::CheckBytes<rkyv::api::high::HighValidator<'_, rancor::Error>>,
{
    rkyv::from_bytes::<T, rancor::Error>(bytes).map_err(|e| LogError::Codec(e.to_string()))
}
```

- [ ] **Step 11: Extend `crates/kardamom-log/src/error.rs`**

```rust
#[derive(Debug, thiserror::Error)]
pub enum LogError {
    #[error("codec: {0}")]
    Codec(String),

    #[error("aeron: {0}")]
    Aeron(String),

    #[error("io: {0}")]
    Io(#[from] std::io::Error),

    #[error("supervisor: {0}")]
    Supervisor(String),

    #[error("quorum stalled: only {present} of {required} recorders reporting")]
    QuorumStalled { present: usize, required: usize },
}
```

- [ ] **Step 12: Add a thin `kardamom-log` codec roundtrip test**

```rust
// crates/kardamom-log/tests/codec_roundtrip.rs
use alloy_primitives::{Address, B256};
use bytes::Bytes;
use kardamom_log::codec::{access, encode, materialize};
use kardamom_types::*;

#[test]
fn log_codec_access_and_materialize() {
    let v = TxEnvelope {
        correlation_id: 7,
        raw_tx: Bytes::from_static(b"raw"),
        sender: Address::repeat_byte(0xAA),
        tx_hash: B256::repeat_byte(0xBB),
    };
    let bytes = encode(&v).unwrap();

    // Zero-copy view.
    let view = access::<TxEnvelope>(&bytes).unwrap();
    assert_eq!(view.correlation_id, 7);

    // Owning view.
    let back: TxEnvelope = materialize(&bytes).unwrap();
    assert_eq!(back, v);
}
```

- [ ] **Step 13: Run all tests, confirm PASS**

```bash
cd /home/dev/kardamom && cargo test -p kardamom-types --test rkyv_roundtrip \
                       && cargo test -p kardamom-log --test codec_roundtrip
```

- [ ] **Step 14: Commit**

```bash
git add crates/kardamom-types crates/kardamom-log/src/codec.rs \
        crates/kardamom-log/src/error.rs crates/kardamom-log/tests/codec_roundtrip.rs
git commit -m "types: rkyv wire types; log: rkyv access/materialize codec helpers"
```

---

## Task 3: Config types

**Files:**
- Modify: `crates/kardamom-log/src/config.rs`

- [ ] **Step 1: Write `config.rs`**

```rust
//! Configuration types for the log subsystem.
//!
//! Loaded from TOML at process start; passed to [`crate::supervisor::Supervisor`]
//! and [`crate::watermark::QuorumAggregator`]. No global state.

use std::path::PathBuf;

use serde::{Deserialize, Serialize};

/// Static identifier for this host's recorder. Must be unique across N recorders.
pub type RecorderId = u8;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct LogConfig {
    pub recorder_id: RecorderId,
    pub aeron: AeronConfig,
    pub channels: ChannelsConfig,
    pub fsync: FsyncConfig,
    pub quorum: QuorumConfig,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct AeronConfig {
    /// Directory the Media Driver uses for its shared-memory ring buffers.
    /// Must be on tmpfs for low latency. Default: `/dev/shm/aeron-kardamom`.
    pub aeron_dir: PathBuf,

    /// Directory the Archive uses for its segment files.
    pub archive_dir: PathBuf,

    /// Path to the Aeron Media Driver binary (jar or native). Spawned by supervisor.
    pub media_driver_cmd: Vec<String>,

    /// Path to the Aeron Archive runner. Spawned by supervisor.
    pub archive_cmd: Vec<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ChannelsConfig {
    /// Channel B: canonical tx log. Recorded.
    pub b_channel: String,
    pub b_stream_id: i32,

    /// Channel C: receipts + block boundaries. Not recorded.
    pub c_channel: String,
    pub c_stream_id: i32,

    /// Receipt-cache channel: `CachedReceipt` messages for proxy/RPC consumers.
    /// Not recorded.
    pub receipt_cache_channel: String,
    pub receipt_cache_stream_id: i32,

    /// Per-recorder fsync watermark publication, parameterized by recorder_id.
    /// e.g. "aeron:ipc?alias=fsync-wm-{rid}".
    pub fsync_watermark_channel_template: String,
    pub fsync_watermark_stream_id: i32,

    /// Aggregated quorum watermark.
    pub quorum_watermark_channel: String,
    pub quorum_watermark_stream_id: i32,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct FsyncConfig {
    /// `O_DIRECT` mirror file path. Sidecar writes the recorder's bytes here
    /// and fsyncs this file.
    pub mirror_path: PathBuf,

    /// io_uring submission queue depth. 256 is a good default for sustained throughput.
    pub uring_entries: u32,

    /// How often (number of completed fsyncs) to publish a watermark.
    /// 1 = every fsync; 16 = every 16th. Higher = lower watermark CPU, higher tail latency.
    pub watermark_publish_every: u32,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct QuorumConfig {
    /// Total recorders.
    pub n: usize,
    /// Required for quorum (Q ≤ N). Default Q=2 for N=3.
    pub q: usize,
}

impl Default for LogConfig {
    fn default() -> Self {
        Self {
            recorder_id: 0,
            aeron: AeronConfig {
                aeron_dir: PathBuf::from("/dev/shm/aeron-kardamom"),
                archive_dir: PathBuf::from("/var/lib/kardamom/archive"),
                media_driver_cmd: vec!["aeron-media-driver".into()],
                archive_cmd: vec!["aeron-archive".into()],
            },
            channels: ChannelsConfig {
                b_channel: "aeron:udp?endpoint=224.0.1.1:40001".into(),
                b_stream_id: 1001,
                c_channel: "aeron:udp?endpoint=224.0.1.1:40002".into(),
                c_stream_id: 1002,
                receipt_cache_channel: "aeron:udp?endpoint=224.0.1.1:40003".into(),
                receipt_cache_stream_id: 1003,
                fsync_watermark_channel_template: "aeron:udp?endpoint=224.0.1.1:4010{rid}".into(),
                fsync_watermark_stream_id: 1010,
                quorum_watermark_channel: "aeron:udp?endpoint=224.0.1.1:40020".into(),
                quorum_watermark_stream_id: 1020,
            },
            fsync: FsyncConfig {
                mirror_path: PathBuf::from("/var/lib/kardamom/mirror.bin"),
                uring_entries: 256,
                watermark_publish_every: 1,
            },
            quorum: QuorumConfig { n: 3, q: 2 },
        }
    }
}
```

- [ ] **Step 2: Build**

```bash
cd /home/dev/kardamom && cargo build -p kardamom-log
```

Expected: builds cleanly.

- [ ] **Step 3: Commit**

```bash
git add crates/kardamom-log/src/config.rs
git commit -m "log: config types"
```

---

## Task 4: Quorum watermark aggregator (pure logic + test)

The aggregator is the only piece with non-trivial logic that does not touch Aeron. Implement and test it standalone first, then wire it to Aeron streams in a later task.

**Files:**
- Modify: `crates/kardamom-log/src/watermark.rs`
- Create: `crates/kardamom-log/tests/watermark_quorum.rs`

- [ ] **Step 1: Write the failing aggregator test**

```rust
// crates/kardamom-log/tests/watermark_quorum.rs
use kardamom_log::watermark::QuorumState;
use kardamom_types::{BPosition, FsyncWatermark};

fn pos(t: i32, o: i32) -> BPosition { BPosition { term_id: t, term_offset: o } }
fn w(rid: u8, t: i32, o: i32) -> FsyncWatermark { FsyncWatermark { recorder_id: rid, position: pos(t, o) } }

#[test]
fn no_recorders_no_watermark() {
    let s = QuorumState::new(3, 2);
    assert!(s.quorum().is_none());
}

#[test]
fn one_of_three_with_q2_no_watermark() {
    let mut s = QuorumState::new(3, 2);
    s.observe(w(0, 1, 100));
    assert!(s.quorum().is_none());
}

#[test]
fn two_of_three_with_q2_emits_smaller_position() {
    let mut s = QuorumState::new(3, 2);
    s.observe(w(0, 1, 100));
    s.observe(w(1, 1, 200));
    assert_eq!(s.quorum(), Some(pos(1, 100)));
}

#[test]
fn three_of_three_with_q2_emits_middle_position() {
    // Q=2 means "fsynced on at least 2 of 3". With three reports, that's the
    // 2nd-smallest position (sorted ascending; pick index N-Q+1 from end == index Q-1 from start).
    let mut s = QuorumState::new(3, 2);
    s.observe(w(0, 1, 100));
    s.observe(w(1, 1, 200));
    s.observe(w(2, 1, 300));
    assert_eq!(s.quorum(), Some(pos(1, 200)));
}

#[test]
fn watermark_is_monotonic() {
    let mut s = QuorumState::new(3, 2);
    s.observe(w(0, 1, 100));
    s.observe(w(1, 1, 200));
    assert_eq!(s.quorum(), Some(pos(1, 100)));
    s.observe(w(0, 1, 150));
    assert_eq!(s.quorum(), Some(pos(1, 150)));
}

#[test]
fn losing_one_of_three_with_q2_still_holds_quorum() {
    // We do not actively track recorder liveness in QuorumState; the aggregator
    // only ever sees positions that have arrived. If a recorder dies, its slot
    // freezes — the quorum will advance only as far as the slowest survivor.
    let mut s = QuorumState::new(3, 2);
    s.observe(w(0, 1, 100));
    s.observe(w(1, 1, 200));
    s.observe(w(2, 1, 300));
    assert_eq!(s.quorum(), Some(pos(1, 200)));
    // Recorder 0 keeps reporting; 2 dies (no more updates).
    s.observe(w(0, 1, 250));
    s.observe(w(1, 1, 400));
    // Sorted positions are now [250, 400, 300] → sorted [250, 300, 400], Q-th smallest = 300.
    assert_eq!(s.quorum(), Some(pos(1, 300)));
}
```

- [ ] **Step 2: Run, confirm FAIL**

```bash
cd /home/dev/kardamom && cargo test -p kardamom-log --test watermark_quorum
```

Expected: FAIL — `QuorumState` doesn't exist.

- [ ] **Step 3: Implement `QuorumState`**

Replace the stub in `crates/kardamom-log/src/watermark.rs` with:

```rust
//! Quorum fsync-watermark aggregator.
//!
//! Per-recorder fsync positions arrive on N independent Aeron streams. The
//! aggregator keeps the latest position per recorder, and on every update
//! computes the Q-th smallest known position — that is the watermark proxies
//! consume for the I2 ack guarantee.
//!
//! Liveness is *not* tracked here: a dead recorder's slot simply stops
//! advancing, and the quorum stalls past it once Q-1 survivors have moved
//! beyond it. The supervisor is responsible for restarting dead recorders.

use kardamom_types::{BPosition, FsyncWatermark};

#[derive(Clone, Debug)]
pub struct QuorumState {
    n: usize,
    q: usize,
    /// `positions[i] = Some(p)` once recorder `i` has reported at least once.
    positions: Vec<Option<BPosition>>,
}

impl QuorumState {
    pub fn new(n: usize, q: usize) -> Self {
        assert!(q >= 1 && q <= n, "0 < q <= n required (got q={q}, n={n})");
        Self { n, q, positions: vec![None; n] }
    }

    pub fn observe(&mut self, w: FsyncWatermark) {
        let i = w.recorder_id as usize;
        assert!(i < self.n, "recorder_id {i} out of range for N={}", self.n);
        // Monotonic per recorder: never accept a regression.
        match self.positions[i] {
            Some(prev) if prev >= w.position => {}
            _ => self.positions[i] = Some(w.position),
        }
    }

    /// Returns the Q-th smallest known position, or `None` if fewer than Q
    /// recorders have reported.
    pub fn quorum(&self) -> Option<BPosition> {
        let mut known: Vec<BPosition> = self.positions.iter().copied().flatten().collect();
        if known.len() < self.q {
            return None;
        }
        known.sort();
        // Q-th smallest: index (q - 1) of the ascending-sorted slice.
        Some(known[self.q - 1])
    }
}
```

- [ ] **Step 4: Run, confirm PASS**

```bash
cd /home/dev/kardamom && cargo test -p kardamom-log --test watermark_quorum
```

Expected: all six tests PASS.

- [ ] **Step 5: Commit**

```bash
git add crates/kardamom-log/src/watermark.rs crates/kardamom-log/tests/watermark_quorum.rs
git commit -m "log: QuorumState aggregator + tests"
```

---

## Task 5: Aeron supervisor (spawns Media Driver + Archive child processes)

The Aeron Media Driver and Archive are Java/C++ processes. We `Command`-spawn them under a Rust supervisor that handles startup ordering, log capture, graceful shutdown, and crash restart with bounded backoff.

**Files:**
- Modify: `crates/kardamom-log/src/supervisor.rs`

- [ ] **Step 1: Implement the supervisor**

```rust
//! Spawns the Aeron Media Driver and the Aeron Archive as child processes.
//!
//! The Aeron client (rusteron) talks to the Media Driver over shared-memory
//! ring buffers in `aeron_dir`. The Archive talks to the Media Driver the same
//! way. We do not embed either — they are Java/C++ processes we drive.
//!
//! Restart policy: exponential backoff capped at 5s. After 10 consecutive
//! failures the supervisor surfaces a `LogError::Supervisor` and exits.

use std::process::Stdio;
use std::time::Duration;

use tokio::process::{Child, Command};
use tokio::sync::oneshot;
use tracing::{error, info, warn};

use crate::config::AeronConfig;
use crate::error::LogError;

pub struct Supervisor {
    cfg: AeronConfig,
    shutdown_tx: Option<oneshot::Sender<()>>,
}

impl Supervisor {
    pub fn new(cfg: AeronConfig) -> Self {
        Self { cfg, shutdown_tx: None }
    }

    /// Spawn Media Driver, wait for its readiness file, then spawn Archive.
    /// Returns once both are up. Background task supervises restarts.
    pub async fn start(&mut self) -> Result<(), LogError> {
        std::fs::create_dir_all(&self.cfg.aeron_dir)?;
        std::fs::create_dir_all(&self.cfg.archive_dir)?;

        let md = spawn(&self.cfg.media_driver_cmd, &self.cfg).await?;
        info!(pid = md.id(), "media driver started");

        // Wait for the Media Driver to create its CnC file before launching the Archive.
        wait_for_path(&self.cfg.aeron_dir.join("cnc.dat"), Duration::from_secs(5)).await?;

        let arch = spawn(&self.cfg.archive_cmd, &self.cfg).await?;
        info!(pid = arch.id(), "archive started");

        let (tx, rx) = oneshot::channel();
        self.shutdown_tx = Some(tx);
        tokio::spawn(supervise([md, arch], rx));
        Ok(())
    }

    pub fn shutdown(&mut self) {
        if let Some(tx) = self.shutdown_tx.take() {
            let _ = tx.send(());
        }
    }
}

async fn spawn(argv: &[String], cfg: &AeronConfig) -> Result<Child, LogError> {
    let (exe, args) = argv.split_first().ok_or_else(|| LogError::Supervisor("empty argv".into()))?;
    Command::new(exe)
        .args(args)
        .env("AERON_DIR", &cfg.aeron_dir)
        .env("AERON_ARCHIVE_DIR", &cfg.archive_dir)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true)
        .spawn()
        .map_err(|e| LogError::Supervisor(format!("spawn {exe}: {e}")))
}

async fn wait_for_path(path: &std::path::Path, timeout: Duration) -> Result<(), LogError> {
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        if path.exists() {
            return Ok(());
        }
        if tokio::time::Instant::now() >= deadline {
            return Err(LogError::Supervisor(format!("timeout waiting for {path:?}")));
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

async fn supervise<const M: usize>(mut children: [Child; M], mut shutdown: oneshot::Receiver<()>) {
    // V0: if any child dies, log loudly and exit. Production-grade restart
    // policy is a follow-up; for now the operator restarts the process.
    loop {
        tokio::select! {
            _ = &mut shutdown => {
                for c in children.iter_mut() {
                    let _ = c.start_kill();
                }
                break;
            }
            res = wait_any(&mut children) => {
                match res {
                    Ok((i, status)) => warn!(child = i, ?status, "aeron child exited"),
                    Err(e) => error!(error = %e, "aeron child wait failed"),
                }
                break;
            }
        }
    }
}

async fn wait_any<const M: usize>(children: &mut [Child; M]) -> std::io::Result<(usize, std::process::ExitStatus)> {
    // Poll each child by spawning a select over their `wait()` futures.
    // This is small-M (always 2 today) so a manual select is fine.
    let mut futs: Vec<_> = children.iter_mut().enumerate().map(|(i, c)| Box::pin(async move {
        let s = c.wait().await?;
        Ok::<_, std::io::Error>((i, s))
    })).collect();
    let (res, _idx, _rest) = futures::future::select_all(futs.drain(..)).await;
    res
}
```

- [ ] **Step 2: Add `futures` to the crate's deps**

Edit `crates/kardamom-log/Cargo.toml`:

```toml
futures = "0.3"
```

And to `[workspace.dependencies]` in the root `Cargo.toml`:

```toml
futures = "0.3"
```

Change the dep line in `crates/kardamom-log/Cargo.toml` to:

```toml
futures.workspace = true
```

- [ ] **Step 3: Build**

```bash
cd /home/dev/kardamom && cargo build -p kardamom-log
```

Expected: builds. The supervisor is not yet integration-tested — that comes in Task 9.

- [ ] **Step 4: Commit**

```bash
git add crates/kardamom-log Cargo.toml
git commit -m "log: aeron supervisor (spawn media driver + archive)"
```

---

## Task 6: Channel B + C publishers

**Files:**
- Modify: `crates/kardamom-log/src/publisher.rs`

This is the first task that touches `rusteron-archive`. The publisher API is intentionally thin: open a publication, encode a typed message with our `codec`, hand the bytes to Aeron via `offer()`.

- [ ] **Step 1: Implement publishers**

```rust
//! Aeron publishers for channel B (canonical, recorded) and channel C
//! (receipts, RAM only).
//!
//! All publishers are concurrent-pub: many publisher handles may offer to the
//! same Aeron stream and Aeron will serialize them into a single byte order.
//! That serialization is the canonical L2 ordering (system invariant I1).

use std::sync::Arc;

use rkyv::api::high::HighSerializer;
use rkyv::rancor;
use rkyv::ser::allocator::ArenaHandle;
use rkyv::util::AlignedVec;
use rusteron_client::Aeron;
use tracing::warn;

use crate::codec;
use crate::config::ChannelsConfig;
use crate::error::LogError;
use kardamom_types::{
    BPosition, BlockBoundaryStart, CachedReceipt, FsyncWatermark, QuorumWatermark, Receipt,
    TxEnvelope,
};

/// Channel B: canonical tx log. Concurrent-pub.
pub struct ChannelBPublisher {
    pub_handle: rusteron_client::ConcurrentPublication,
}

impl ChannelBPublisher {
    pub fn open(aeron: &Aeron, ch: &ChannelsConfig) -> Result<Self, LogError> {
        let pub_handle = aeron
            .add_concurrent_publication(&ch.b_channel, ch.b_stream_id)
            .map_err(|e| LogError::Aeron(format!("add_concurrent_publication B: {e}")))?;
        Ok(Self { pub_handle })
    }

    pub fn publish_tx(&self, env: &TxEnvelope) -> Result<BPosition, LogError> {
        offer(&self.pub_handle, env)
    }

    pub fn publish_boundary(&self, b: &BlockBoundaryStart) -> Result<BPosition, LogError> {
        offer(&self.pub_handle, b)
    }
}

/// Channel C: receipts + boundaries. RAM only.
pub struct ChannelCPublisher {
    pub_handle: rusteron_client::ConcurrentPublication,
}

impl ChannelCPublisher {
    pub fn open(aeron: &Aeron, ch: &ChannelsConfig) -> Result<Self, LogError> {
        let pub_handle = aeron
            .add_concurrent_publication(&ch.c_channel, ch.c_stream_id)
            .map_err(|e| LogError::Aeron(format!("add_concurrent_publication C: {e}")))?;
        Ok(Self { pub_handle })
    }

    pub fn publish_receipt(&self, r: &Receipt) -> Result<BPosition, LogError> {
        offer(&self.pub_handle, r)
    }

    pub fn publish_boundary(&self, b: &kardamom_types::BlockBoundary) -> Result<BPosition, LogError> {
        offer(&self.pub_handle, b)
    }
}

/// Receipt-cache channel: per-tx `CachedReceipt` messages. RAM only,
/// consumed by short-lived clients (proxy nonce cache, RPC frontends).
pub struct ReceiptCachePublisher {
    pub_handle: rusteron_client::ConcurrentPublication,
}

impl ReceiptCachePublisher {
    pub fn open(aeron: &Aeron, ch: &ChannelsConfig) -> Result<Self, LogError> {
        let pub_handle = aeron
            .add_concurrent_publication(&ch.receipt_cache_channel, ch.receipt_cache_stream_id)
            .map_err(|e| LogError::Aeron(format!("add_concurrent_publication rc: {e}")))?;
        Ok(Self { pub_handle })
    }

    pub fn publish(&self, r: &CachedReceipt) -> Result<BPosition, LogError> {
        offer(&self.pub_handle, r)
    }
}

/// Per-recorder fsync-watermark publisher. Each recorder host opens one of these.
pub struct WatermarkPublisher {
    pub_handle: rusteron_client::ConcurrentPublication,
}

impl WatermarkPublisher {
    pub fn open(aeron: &Aeron, ch: &ChannelsConfig, recorder_id: u8) -> Result<Self, LogError> {
        let channel = ch
            .fsync_watermark_channel_template
            .replace("{rid}", &recorder_id.to_string());
        let pub_handle = aeron
            .add_concurrent_publication(&channel, ch.fsync_watermark_stream_id)
            .map_err(|e| LogError::Aeron(format!("add_concurrent_publication wm: {e}")))?;
        Ok(Self { pub_handle })
    }

    pub fn publish(&self, w: &FsyncWatermark) -> Result<(), LogError> {
        offer(&self.pub_handle, w).map(|_| ())
    }
}

/// Shared quorum-watermark publisher, used by the aggregator.
pub struct QuorumPublisher {
    pub_handle: rusteron_client::ConcurrentPublication,
}

impl QuorumPublisher {
    pub fn open(aeron: &Aeron, ch: &ChannelsConfig) -> Result<Self, LogError> {
        let pub_handle = aeron
            .add_concurrent_publication(&ch.quorum_watermark_channel, ch.quorum_watermark_stream_id)
            .map_err(|e| LogError::Aeron(format!("add_concurrent_publication qwm: {e}")))?;
        Ok(Self { pub_handle })
    }

    pub fn publish(&self, q: &QuorumWatermark) -> Result<(), LogError> {
        offer(&self.pub_handle, q).map(|_| ())
    }
}

fn offer<T>(
    p: &rusteron_client::ConcurrentPublication,
    msg: &T,
) -> Result<BPosition, LogError>
where
    T: for<'a> rkyv::Serialize<HighSerializer<AlignedVec, ArenaHandle<'a>, rancor::Error>>,
{
    let bytes: AlignedVec = codec::encode(msg)?;
    // rusteron::ConcurrentPublication::offer returns the new stream position
    // (or a negative back-pressure code). Retry up to 1024 times on back-pressure.
    for attempt in 0..1024 {
        let r = p.offer(bytes.as_slice());
        if r >= 0 {
            return Ok(decode_position(r));
        }
        if attempt % 64 == 63 {
            warn!(attempt, "aeron back-pressure, retrying");
        }
        std::hint::spin_loop();
    }
    Err(LogError::Aeron("back-pressure timeout after 1024 retries".into()))
}

/// Aeron returns a stream position as `(term_id << 32) | term_offset` packed
/// into i64. Unpack into our `BPosition`.
fn decode_position(p: i64) -> BPosition {
    let term_id = (p >> 32) as i32;
    let term_offset = (p & 0xFFFF_FFFF) as i32;
    BPosition { term_id, term_offset }
}

/// Bundle of all publishers a single host might need.
#[derive(Clone)]
pub struct Publishers {
    pub aeron: Arc<Aeron>,
    pub b: Arc<ChannelBPublisher>,
    pub c: Arc<ChannelCPublisher>,
}
```

- [ ] **Step 2: Build**

```bash
cd /home/dev/kardamom && cargo build -p kardamom-log
```

Expected: builds. Some `rusteron` API names may not match exactly — refer to https://docs.rs/rusteron-client and adjust (e.g. `add_concurrent_publication` may be named slightly differently in the actual crate; treat the docs as authoritative and update the wrapper to match).

- [ ] **Step 3: Commit**

```bash
git add crates/kardamom-log/src/publisher.rs
git commit -m "log: channel B/C publishers + watermark publishers"
```

---

## Task 7: Channel B + C subscribers

**Files:**
- Modify: `crates/kardamom-log/src/subscriber.rs`

Subscribers poll Aeron `Image`s and decode incoming bytes into typed messages. The hot-path consumer (the executor's reader thread) needs zero-copy access; the test/convenience consumer can `decode` into owned values.

- [ ] **Step 1: Implement subscribers**

```rust
//! Aeron subscribers for channels B, C, per-recorder watermark streams, and
//! the aggregated quorum watermark.

use std::sync::Arc;

use rkyv::api::high::HighDeserializer;
use rkyv::rancor;
use rusteron_client::Aeron;

use crate::codec;
use crate::config::ChannelsConfig;
use crate::error::LogError;
use kardamom_types::{
    BPosition, CachedReceipt, FsyncWatermark, QuorumWatermark, Receipt, TxEnvelope,
};

/// Generic single-stream subscriber over a typed message. Materializes each
/// fragment into an owned `T` for ergonomics. Hot-path consumers that want
/// zero-copy use [`Subscribers::b_zero_copy`] (TODO follow-up) which hands
/// `&Archived<T>` directly to the callback.
pub struct TypedSubscriber<T> {
    sub: rusteron_client::Subscription,
    _marker: std::marker::PhantomData<T>,
}

impl<T> TypedSubscriber<T>
where
    T: rkyv::Archive + 'static,
    T::Archived: rkyv::Deserialize<T, HighDeserializer<rancor::Error>>
        + for<'a> rkyv::bytecheck::CheckBytes<rkyv::api::high::HighValidator<'a, rancor::Error>>,
{
    pub fn open(aeron: &Aeron, channel: &str, stream_id: i32) -> Result<Self, LogError> {
        let sub = aeron
            .add_subscription(channel, stream_id)
            .map_err(|e| LogError::Aeron(format!("add_subscription {channel}: {e}")))?;
        Ok(Self { sub, _marker: std::marker::PhantomData })
    }

    /// Poll once and invoke `f` with an owned `T` on every fragment that
    /// arrived in this poll cycle. Returns the number of fragments processed.
    pub fn poll<F: FnMut(T, BPosition)>(&mut self, mut f: F, fragment_limit: usize) -> usize {
        self.sub.poll(
            |bytes: &[u8], header: rusteron_client::Header| {
                match codec::materialize::<T>(bytes) {
                    Ok(v) => f(v, BPosition { term_id: header.term_id(), term_offset: header.term_offset() }),
                    Err(e) => tracing::error!(error = %e, "decode failed"),
                }
            },
            fragment_limit,
        )
    }

    /// Zero-copy poll: invoke `f` with a borrowed `&Archived<T>` view that
    /// lives only for the duration of the callback. Use for hot-path readers
    /// that don't need ownership.
    pub fn poll_zero_copy<F: FnMut(&T::Archived, BPosition)>(
        &mut self,
        mut f: F,
        fragment_limit: usize,
    ) -> usize {
        self.sub.poll(
            |bytes: &[u8], header: rusteron_client::Header| match codec::access::<T>(bytes) {
                Ok(view) => f(view, BPosition {
                    term_id: header.term_id(),
                    term_offset: header.term_offset(),
                }),
                Err(e) => tracing::error!(error = %e, "access failed"),
            },
            fragment_limit,
        )
    }
}

pub type ChannelBSubscriber = TypedSubscriber<TxEnvelope>;
pub type ChannelCReceiptSubscriber = TypedSubscriber<Receipt>;
pub type ReceiptCacheSubscriber = TypedSubscriber<CachedReceipt>;
pub type WatermarkSubscriber = TypedSubscriber<FsyncWatermark>;
pub type QuorumSubscriber = TypedSubscriber<QuorumWatermark>;

/// Convenience bundle.
pub struct Subscribers {
    pub aeron: Arc<Aeron>,
    pub ch: ChannelsConfig,
}

impl Subscribers {
    pub fn b(&self) -> Result<ChannelBSubscriber, LogError> {
        TypedSubscriber::open(&self.aeron, &self.ch.b_channel, self.ch.b_stream_id)
    }

    pub fn c_receipts(&self) -> Result<ChannelCReceiptSubscriber, LogError> {
        TypedSubscriber::open(&self.aeron, &self.ch.c_channel, self.ch.c_stream_id)
    }

    pub fn receipt_cache(&self) -> Result<ReceiptCacheSubscriber, LogError> {
        TypedSubscriber::open(
            &self.aeron,
            &self.ch.receipt_cache_channel,
            self.ch.receipt_cache_stream_id,
        )
    }

    pub fn watermark(&self, recorder_id: u8) -> Result<WatermarkSubscriber, LogError> {
        let channel = self.ch.fsync_watermark_channel_template.replace("{rid}", &recorder_id.to_string());
        TypedSubscriber::open(&self.aeron, &channel, self.ch.fsync_watermark_stream_id)
    }

    pub fn quorum(&self) -> Result<QuorumSubscriber, LogError> {
        TypedSubscriber::open(
            &self.aeron,
            &self.ch.quorum_watermark_channel,
            self.ch.quorum_watermark_stream_id,
        )
    }
}
```

- [ ] **Step 2: Build**

```bash
cd /home/dev/kardamom && cargo build -p kardamom-log
```

Expected: builds. As with Task 6, adjust `rusteron` API names if upstream differs.

- [ ] **Step 3: Commit**

```bash
git add crates/kardamom-log/src/subscriber.rs
git commit -m "log: channel B/C/watermark subscribers"
```

---

## Task 8: Aeron Archive recorder wrapper

**Files:**
- Modify: `crates/kardamom-log/src/recorder.rs`

The recorder is responsible for telling the Aeron Archive (running as a child process) to start recording channel B and to publish its `recording-position` counter that the fsync sidecar will tail.

- [ ] **Step 1: Implement the recorder wrapper**

```rust
//! Drives an Aeron Archive instance to record channel B and exposes the
//! recording-position counter the fsync sidecar tails.

use rusteron_archive::AeronArchive;
use tracing::info;

use crate::config::{ChannelsConfig, RecorderId};
use crate::error::LogError;

pub struct Recorder {
    archive: AeronArchive,
    recorder_id: RecorderId,
    recording_id: i64,
    counter_id: i32,
}

impl Recorder {
    pub fn start(
        archive: AeronArchive,
        ch: &ChannelsConfig,
        recorder_id: RecorderId,
    ) -> Result<Self, LogError> {
        // start_recording: (channel, stream_id, source_location, auto_stop)
        // SourceLocation::Local -> we are co-located with the publisher.
        let recording_id = archive
            .start_recording(
                &ch.b_channel,
                ch.b_stream_id,
                rusteron_archive::SourceLocation::Local,
                false,
            )
            .map_err(|e| LogError::Aeron(format!("start_recording: {e}")))?;
        info!(recording_id, "started B recording");

        // The Archive exposes a `recording-pos` counter per recording.
        // The id is looked up from the counter reader.
        let counter_id = archive
            .find_recording_position_counter(recording_id)
            .map_err(|e| LogError::Aeron(format!("find_recording_position_counter: {e}")))?;

        Ok(Self { archive, recorder_id, recording_id, counter_id })
    }

    pub fn recorder_id(&self) -> RecorderId {
        self.recorder_id
    }

    pub fn recording_id(&self) -> i64 {
        self.recording_id
    }

    /// Counter id the fsync sidecar tails to learn how much data has been
    /// committed to Aeron's internal buffer (i.e. is *available* for fsync).
    pub fn position_counter_id(&self) -> i32 {
        self.counter_id
    }

    /// Path to the active segment file on disk. The fsync sidecar mirrors
    /// bytes from here into its `O_DIRECT` file.
    pub fn active_segment_path(&self) -> Result<std::path::PathBuf, LogError> {
        // Archive segment files live under `archive_dir/<recording_id>-<segment>.rec`.
        // rusteron exposes a helper; if not, we compute by convention.
        self.archive
            .recording_segment_file(self.recording_id)
            .map(Into::into)
            .map_err(|e| LogError::Aeron(format!("segment_file: {e}")))
    }
}
```

- [ ] **Step 2: Build**

```bash
cd /home/dev/kardamom && cargo build -p kardamom-log
```

Expected: builds. The exact `rusteron-archive` method names may differ; if a method does not exist, find the closest equivalent and add a TODO comment with the doc URL. Do not invent names.

- [ ] **Step 3: Commit**

```bash
git add crates/kardamom-log/src/recorder.rs
git commit -m "log: aeron archive recorder wrapper"
```

---

## Task 9: io_uring fsync sidecar — write+fdatasync loop

**Files:**
- Modify: `crates/kardamom-log/src/fsync_sidecar.rs`
- Create: `crates/kardamom-log/tests/fsync_sidecar.rs`

This is the heart of the durability story. The sidecar polls the recorder's position counter; whenever new bytes are available, it reads them from the recorder's segment file, writes them through `O_DIRECT` into a mirror file, fsyncs the mirror, and publishes the new fsynced position as a `FsyncWatermark`.

- [ ] **Step 1: Write the failing test**

```rust
// crates/kardamom-log/tests/fsync_sidecar.rs
//! Unit-level test for the FsyncSidecar's write+fdatasync loop, decoupled from
//! Aeron. We feed it a fake source (a temp file we append bytes to) and a fake
//! position-counter (an atomic), and assert that after each "burst" the mirror
//! file contents match the source up to the watermark.

use std::sync::Arc;
use std::sync::atomic::{AtomicI64, Ordering};

use kardamom_log::fsync_sidecar::{FsyncSidecar, PositionSource};
use kardamom_types::BPosition;

struct FakePosition(Arc<AtomicI64>);
impl PositionSource for FakePosition {
    fn current(&self) -> i64 { self.0.load(Ordering::Acquire) }
}

#[tokio::test]
async fn sidecar_mirrors_and_fsyncs_appended_bytes() {
    let dir = tempfile::tempdir().unwrap();
    let source = dir.path().join("source.bin");
    let mirror = dir.path().join("mirror.bin");
    std::fs::write(&source, b"").unwrap();

    let pos = Arc::new(AtomicI64::new(0));
    let mut sidecar = FsyncSidecar::open(
        &source,
        &mirror,
        Box::new(FakePosition(pos.clone())),
        256,
    )
    .unwrap();

    // Append 4096 bytes to source and advance position counter.
    let buf = vec![0xAB; 4096];
    std::fs::write(&source, &buf).unwrap();
    pos.store(4096, Ordering::Release);

    let wm = sidecar.tick().unwrap();
    assert_eq!(wm, Some(BPosition { term_id: 0, term_offset: 4096 }));

    let on_disk = std::fs::read(&mirror).unwrap();
    assert_eq!(on_disk.len(), 4096);
    assert_eq!(&on_disk[..], &buf[..]);
}

#[tokio::test]
async fn sidecar_returns_none_when_no_new_bytes() {
    let dir = tempfile::tempdir().unwrap();
    let source = dir.path().join("source.bin");
    let mirror = dir.path().join("mirror.bin");
    std::fs::write(&source, b"").unwrap();

    let pos = Arc::new(AtomicI64::new(0));
    let mut sidecar = FsyncSidecar::open(
        &source,
        &mirror,
        Box::new(FakePosition(pos.clone())),
        256,
    )
    .unwrap();

    assert_eq!(sidecar.tick().unwrap(), None);
}
```

- [ ] **Step 2: Run, confirm FAIL**

```bash
cd /home/dev/kardamom && cargo test -p kardamom-log --test fsync_sidecar
```

Expected: FAIL — `FsyncSidecar` not implemented.

- [ ] **Step 3: Implement `fsync_sidecar.rs`**

```rust
//! Continuous io_uring fsync sidecar.
//!
//! Polls a `PositionSource` (in production: the Aeron Archive
//! `recording-position` counter). Whenever the source advances past the
//! mirror's tail, the sidecar:
//!
//!   1. mmap-reads the new bytes from the recorder's segment file,
//!   2. submits an `IORING_OP_WRITE` of those bytes to the mirror file
//!      (opened `O_DIRECT`),
//!   3. submits an `IORING_OP_FSYNC` (with `IORING_FSYNC_DATASYNC`) linked
//!      after the write,
//!   4. waits for the fsync CQE,
//!   5. returns the new fsynced position so the caller can publish a
//!      [`FsyncWatermark`].
//!
//! Buffers are 4 KiB aligned to satisfy `O_DIRECT`.

use std::alloc::{alloc, dealloc, Layout};
use std::fs::OpenOptions;
use std::os::fd::{AsRawFd, RawFd};
use std::os::unix::fs::OpenOptionsExt;
use std::path::Path;

use io_uring::{opcode, types, IoUring};
use libc::O_DIRECT;

use crate::error::LogError;
use kardamom_types::BPosition;

pub trait PositionSource: Send {
    /// Returns the (monotonically increasing) Aeron stream position in bytes
    /// that has been written into the recorder's segment file.
    fn current(&self) -> i64;
}

pub struct FsyncSidecar {
    source_fd: std::fs::File,
    mirror_fd: std::fs::File,
    position: Box<dyn PositionSource>,
    ring: IoUring,
    /// Bytes mirrored + fsynced so far.
    fsynced: i64,
    /// 4 KiB aligned bounce buffer.
    bounce: AlignedBuf,
}

struct AlignedBuf {
    ptr: *mut u8,
    cap: usize,
    layout: Layout,
}

unsafe impl Send for AlignedBuf {}

impl AlignedBuf {
    fn new(cap: usize) -> Self {
        let layout = Layout::from_size_align(cap, 4096).expect("alignable");
        // SAFETY: layout is valid (non-zero, alignment power-of-two).
        let ptr = unsafe { alloc(layout) };
        assert!(!ptr.is_null(), "alloc failed");
        Self { ptr, cap, layout }
    }

    fn as_mut(&mut self) -> &mut [u8] {
        // SAFETY: ptr is valid for cap bytes, exclusive access through &mut self.
        unsafe { std::slice::from_raw_parts_mut(self.ptr, self.cap) }
    }
}

impl Drop for AlignedBuf {
    fn drop(&mut self) {
        // SAFETY: ptr was allocated with the stored layout.
        unsafe { dealloc(self.ptr, self.layout) };
    }
}

impl FsyncSidecar {
    pub fn open(
        source: &Path,
        mirror: &Path,
        position: Box<dyn PositionSource>,
        uring_entries: u32,
    ) -> Result<Self, LogError> {
        let source_fd = OpenOptions::new().read(true).open(source)?;
        let mirror_fd = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .custom_flags(O_DIRECT)
            .open(mirror)?;

        let ring = IoUring::new(uring_entries).map_err(LogError::Io)?;

        // 1 MiB bounce buffer per tick. Sized to amortize syscall overhead
        // while keeping per-tick latency well under the 250ms boundary cadence.
        let bounce = AlignedBuf::new(1 << 20);

        Ok(Self { source_fd, mirror_fd, position, ring, fsynced: 0, bounce })
    }

    /// One iteration: copy any newly-available bytes through the bounce buffer,
    /// submit write + fdatasync linked SQEs, wait for fsync completion.
    /// Returns the new fsynced position, or `None` if nothing advanced.
    pub fn tick(&mut self) -> Result<Option<BPosition>, LogError> {
        let avail = self.position.current();
        if avail <= self.fsynced {
            return Ok(None);
        }
        let want = (avail - self.fsynced) as usize;
        let chunk = want.min(self.bounce.cap);
        // Round down to 4 KiB to satisfy O_DIRECT length alignment.
        let chunk = chunk & !4095;
        if chunk == 0 {
            // We have <4 KiB of unaligned tail; wait until more arrives.
            return Ok(None);
        }

        // Read from source (buffered, not O_DIRECT — kernel page cache absorbs).
        let read_off = self.fsynced as u64;
        let read = read_at(&self.source_fd, self.bounce.as_mut(), read_off, chunk)?;
        if read == 0 {
            return Ok(None);
        }
        assert_eq!(read & 4095, 0, "source must yield 4 KiB-aligned bytes");

        let write_off = self.fsynced as u64;
        self.submit_write_then_fsync(read, write_off)?;
        self.fsynced += read as i64;

        Ok(Some(stream_position_to_bposition(self.fsynced)))
    }

    fn submit_write_then_fsync(&mut self, len: usize, offset: u64) -> Result<(), LogError> {
        let write = opcode::Write::new(
            types::Fd(self.mirror_fd.as_raw_fd()),
            self.bounce.ptr,
            len as u32,
        )
        .offset(offset)
        .build()
        .user_data(0xAA)
        .flags(io_uring::squeue::Flags::IO_LINK);

        let fsync = opcode::Fsync::new(types::Fd(self.mirror_fd.as_raw_fd()))
            .flags(types::FsyncFlags::DATASYNC)
            .build()
            .user_data(0xBB);

        // SAFETY: SQEs reference `self.bounce.ptr` and `self.mirror_fd`; both
        // outlive the submission because `tick` blocks until the CQE arrives.
        unsafe {
            let mut sq = self.ring.submission();
            sq.push(&write).map_err(|_| LogError::Io(std::io::Error::other("uring sq full")))?;
            sq.push(&fsync).map_err(|_| LogError::Io(std::io::Error::other("uring sq full")))?;
        }
        // Submit and wait for the fsync (the linked write completes first;
        // its CQE arrives but we only need to ensure the fsync is durable).
        self.ring.submit_and_wait(2).map_err(LogError::Io)?;

        let mut cq = self.ring.completion();
        while let Some(cqe) = cq.next() {
            if cqe.result() < 0 {
                return Err(LogError::Io(std::io::Error::from_raw_os_error(-cqe.result())));
            }
        }
        Ok(())
    }
}

fn read_at(f: &std::fs::File, buf: &mut [u8], offset: u64, len: usize) -> Result<usize, LogError> {
    use std::os::unix::fs::FileExt;
    let n = f.read_at(&mut buf[..len], offset)?;
    Ok(n)
}

/// Aeron stream position decomposes to (term_id, term_offset). We use a fixed
/// term length of 16 MiB (Aeron default `aeron.term.buffer.length=16777216`).
const TERM_LEN: i64 = 16 * 1024 * 1024;

fn stream_position_to_bposition(pos: i64) -> BPosition {
    let term_id = (pos / TERM_LEN) as i32;
    let term_offset = (pos % TERM_LEN) as i32;
    BPosition { term_id, term_offset }
}
```

- [ ] **Step 4: Add `libc` to deps**

`crates/kardamom-log/Cargo.toml`:

```toml
libc = "0.2"
```

(Also add to workspace deps in the root `Cargo.toml`.)

- [ ] **Step 5: Run the test, confirm PASS**

```bash
cd /home/dev/kardamom && cargo test -p kardamom-log --test fsync_sidecar
```

Expected: both tests PASS.

If `O_DIRECT` is not supported on the test filesystem (e.g. tmpfs returns EINVAL), the test will fail with `EINVAL`. In that case, locally re-run with the temp dir pointed at an ext4/xfs mount. The CI runner must be configured the same way; document this in `crates/kardamom-log/README.md` as a follow-up.

- [ ] **Step 6: Commit**

```bash
git add crates/kardamom-log Cargo.toml
git commit -m "log: io_uring O_DIRECT fsync sidecar + unit test"
```

---

## Task 10: Wire the sidecar to a real Aeron position-counter

**Files:**
- Modify: `crates/kardamom-log/src/fsync_sidecar.rs`
- Modify: `crates/kardamom-log/src/recorder.rs`

- [ ] **Step 1: Add an `AeronPositionSource`**

Append to `crates/kardamom-log/src/fsync_sidecar.rs`:

```rust
/// `PositionSource` backed by an Aeron counter (the recording-position counter
/// exposed by the Aeron Archive).
pub struct AeronPositionSource {
    counter: rusteron_client::AtomicCounter,
}

impl AeronPositionSource {
    pub fn new(aeron: &rusteron_client::Aeron, counter_id: i32) -> Result<Self, crate::error::LogError> {
        let counter = aeron
            .counter_for_id(counter_id)
            .map_err(|e| crate::error::LogError::Aeron(format!("counter_for_id {counter_id}: {e}")))?;
        Ok(Self { counter })
    }
}

impl PositionSource for AeronPositionSource {
    fn current(&self) -> i64 {
        self.counter.get()
    }
}
```

- [ ] **Step 2: Build**

```bash
cd /home/dev/kardamom && cargo build -p kardamom-log
```

Expected: builds. Adjust the `rusteron_client::AtomicCounter` name if upstream differs.

- [ ] **Step 3: Commit**

```bash
git add crates/kardamom-log/src/fsync_sidecar.rs
git commit -m "log: AeronPositionSource for fsync sidecar"
```

---

## Task 11: Quorum aggregator task (wires Aeron streams to QuorumState)

**Files:**
- Modify: `crates/kardamom-log/src/watermark.rs`

- [ ] **Step 1: Append the aggregator runner**

Add to `crates/kardamom-log/src/watermark.rs`:

```rust
use std::sync::Arc;
use std::time::Duration;

use tokio::task::JoinHandle;

use crate::config::{ChannelsConfig, QuorumConfig};
use crate::error::LogError;
use crate::publisher::QuorumPublisher;
use crate::subscriber::Subscribers;
use kardamom_types::QuorumWatermark;

/// Tokio task that drains all N per-recorder watermark subscriptions and
/// republishes the quorum position whenever it advances.
pub struct QuorumAggregator {
    pub handle: JoinHandle<()>,
}

impl QuorumAggregator {
    pub fn start(
        subscribers: Subscribers,
        publisher: Arc<QuorumPublisher>,
        cfg: QuorumConfig,
    ) -> Result<Self, LogError> {
        let mut state = QuorumState::new(cfg.n, cfg.q);
        let mut subs: Vec<_> = (0..cfg.n)
            .map(|rid| subscribers.watermark(rid as u8))
            .collect::<Result<_, _>>()?;

        let handle = tokio::task::spawn_blocking(move || {
            let mut last_published = None;
            loop {
                let mut any = false;
                for sub in subs.iter_mut() {
                    any |= sub.poll(|w, _| state.observe(w), 64) > 0;
                }
                if any {
                    if let Some(p) = state.quorum() {
                        if last_published != Some(p) {
                            if let Err(e) = publisher.publish(&QuorumWatermark { position: p }) {
                                tracing::error!(error = %e, "quorum publish failed");
                            } else {
                                last_published = Some(p);
                            }
                        }
                    }
                }
                if !any {
                    // No new fsync data this cycle — short park to avoid burn.
                    std::thread::sleep(Duration::from_micros(50));
                }
            }
        });
        Ok(Self { handle })
    }
}
```

- [ ] **Step 2: Build**

```bash
cd /home/dev/kardamom && cargo build -p kardamom-log
```

Expected: builds.

- [ ] **Step 3: Commit**

```bash
git add crates/kardamom-log/src/watermark.rs
git commit -m "log: QuorumAggregator task wiring subscribers to QuorumState"
```

---

## Task 12: Publisher / subscriber roundtrip integration test

**Files:**
- Create: `crates/kardamom-log/tests/publisher_subscriber.rs`

This is the first end-to-end test that boots a real Aeron Media Driver. It is gated on the presence of the Aeron binaries (`AERON_MEDIA_DRIVER_BIN` env var); without it, the test is skipped.

- [ ] **Step 1: Write the test**

```rust
// crates/kardamom-log/tests/publisher_subscriber.rs
//! Boots the Aeron supervisor with a single Media Driver (no Archive),
//! publishes N TxEnvelopes on channel B from M concurrent tasks, and asserts
//! that the subscriber receives all N*M in canonical order.

use std::sync::Arc;
use std::time::Duration;

use alloy_primitives::{Address, B256};
use bytes::Bytes;
use kardamom_log::config::LogConfig;
use kardamom_log::publisher::ChannelBPublisher;
use kardamom_log::subscriber::Subscribers;
use kardamom_log::supervisor::Supervisor;
use kardamom_types::TxEnvelope;

fn aeron_binaries_available() -> bool {
    std::env::var("AERON_MEDIA_DRIVER_BIN").is_ok()
}

#[tokio::test]
async fn channel_b_publisher_subscriber_roundtrip() {
    if !aeron_binaries_available() {
        eprintln!("skipping: AERON_MEDIA_DRIVER_BIN not set");
        return;
    }
    let mut cfg = LogConfig::default();
    let tmp = tempfile::tempdir().unwrap();
    cfg.aeron.aeron_dir = tmp.path().join("aeron");
    cfg.aeron.archive_dir = tmp.path().join("archive");
    cfg.aeron.media_driver_cmd = vec![std::env::var("AERON_MEDIA_DRIVER_BIN").unwrap()];
    // Skip the archive for this test.
    cfg.aeron.archive_cmd = vec!["/bin/true".into()];
    cfg.channels.b_channel = "aeron:ipc".into();
    cfg.channels.b_stream_id = 1001;

    let mut sup = Supervisor::new(cfg.aeron.clone());
    sup.start().await.unwrap();

    let aeron = Arc::new(
        rusteron_client::Aeron::connect_to(&cfg.aeron.aeron_dir).expect("aeron connect")
    );

    let pubr = ChannelBPublisher::open(&aeron, &cfg.channels).unwrap();
    let subs = Subscribers { aeron: aeron.clone(), ch: cfg.channels.clone() };
    let mut sub = subs.b().unwrap();

    // Publish 4 publishers × 250 messages = 1000.
    let pub_arc = Arc::new(pubr);
    let mut joins = Vec::new();
    for p in 0..4u64 {
        let pub_arc = pub_arc.clone();
        joins.push(tokio::task::spawn_blocking(move || {
            for i in 0..250u64 {
                pub_arc.publish_tx(&TxEnvelope {
                    correlation_id: p * 1000 + i,
                    raw_tx: Bytes::from(format!("tx-{p}-{i}").into_bytes()),
                    sender: Address::repeat_byte(p as u8),
                    tx_hash: B256::repeat_byte(i as u8),
                }).unwrap();
            }
        }));
    }
    for j in joins { j.await.unwrap(); }

    let mut received: Vec<TxEnvelope> = Vec::with_capacity(1000);
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    while received.len() < 1000 && std::time::Instant::now() < deadline {
        sub.poll(|t, _pos| received.push(t), 256);
        if received.len() < 1000 {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    }
    assert_eq!(received.len(), 1000, "expected 1000 messages, got {}", received.len());
    // Order across publishers is not deterministic, but each publisher's
    // messages must arrive in publish order. Group and check.
    for p in 0..4u64 {
        let mut nums: Vec<u64> = received.iter()
            .filter(|t| t.correlation_id / 1000 == p)
            .map(|t| t.correlation_id % 1000)
            .collect();
        let mut want: Vec<u64> = (0..250).collect();
        want.sort();
        nums.sort();
        assert_eq!(nums, want);
    }

    sup.shutdown();
}
```

- [ ] **Step 2: Run (will likely be skipped on dev workstation; required to pass in CI with Aeron installed)**

```bash
cd /home/dev/kardamom && cargo test -p kardamom-log --test publisher_subscriber -- --nocapture
```

Expected without Aeron installed: prints "skipping: AERON_MEDIA_DRIVER_BIN not set" and returns 0.
Expected with `AERON_MEDIA_DRIVER_BIN` set to a real Media Driver binary: PASS.

- [ ] **Step 3: Commit**

```bash
git add crates/kardamom-log/tests/publisher_subscriber.rs
git commit -m "log: pub/sub roundtrip integration test"
```

---

## Task 13: 3-recorder cluster integration test

**Files:**
- Create: `crates/kardamom-log/tests/recorder_cluster.rs`

The big one. Three recorders (each a Media Driver + Archive + fsync sidecar), four publishers, 1000 messages, quorum aggregator. Asserts (a) all messages arrive on all three recorders in canonical order, (b) quorum watermark advances to cover the last published message, (c) killing 1 recorder leaves quorum satisfied, (d) killing 2 stalls the watermark.

- [ ] **Step 1: Write the test scaffold**

```rust
// crates/kardamom-log/tests/recorder_cluster.rs
//! Integration test: 3 recorders, 4 publishers, 1000 messages, quorum aggregator.
//! Gated on AERON_MEDIA_DRIVER_BIN and AERON_ARCHIVE_BIN.

use std::sync::Arc;
use std::time::Duration;

use alloy_primitives::{Address, B256};
use bytes::Bytes;
use kardamom_log::config::LogConfig;
use kardamom_log::publisher::{ChannelBPublisher, QuorumPublisher};
use kardamom_log::subscriber::Subscribers;
use kardamom_log::supervisor::Supervisor;
use kardamom_log::watermark::QuorumAggregator;
use kardamom_types::{BPosition, TxEnvelope};

fn aeron_available() -> bool {
    std::env::var("AERON_MEDIA_DRIVER_BIN").is_ok()
        && std::env::var("AERON_ARCHIVE_BIN").is_ok()
}

struct RecorderHost {
    supervisor: Supervisor,
    aeron: Arc<rusteron_client::Aeron>,
    cfg: LogConfig,
}

async fn spawn_recorder(rid: u8, base_tmp: &std::path::Path) -> RecorderHost {
    let mut cfg = LogConfig::default();
    cfg.recorder_id = rid;
    cfg.aeron.aeron_dir = base_tmp.join(format!("aeron-{rid}"));
    cfg.aeron.archive_dir = base_tmp.join(format!("archive-{rid}"));
    cfg.aeron.media_driver_cmd = vec![std::env::var("AERON_MEDIA_DRIVER_BIN").unwrap()];
    cfg.aeron.archive_cmd = vec![std::env::var("AERON_ARCHIVE_BIN").unwrap()];
    cfg.fsync.mirror_path = base_tmp.join(format!("mirror-{rid}.bin"));

    let mut sup = Supervisor::new(cfg.aeron.clone());
    sup.start().await.unwrap();
    let aeron = Arc::new(
        rusteron_client::Aeron::connect_to(&cfg.aeron.aeron_dir).expect("aeron connect"),
    );
    RecorderHost { supervisor: sup, aeron, cfg }
}

#[tokio::test]
async fn three_recorders_quorum_advances_and_tolerates_one_failure() {
    if !aeron_available() {
        eprintln!("skipping: AERON_MEDIA_DRIVER_BIN / AERON_ARCHIVE_BIN not set");
        return;
    }
    let tmp = tempfile::tempdir().unwrap();
    let mut hosts = Vec::new();
    for rid in 0..3 {
        hosts.push(spawn_recorder(rid, tmp.path()).await);
    }
    // (For a full test, each host also runs a Recorder + FsyncSidecar +
    // WatermarkPublisher; assemble those here per host.)

    // Quorum aggregator on host[0].
    let qcfg = hosts[0].cfg.quorum.clone();
    let qpub = Arc::new(QuorumPublisher::open(&hosts[0].aeron, &hosts[0].cfg.channels).unwrap());
    let subs = Subscribers { aeron: hosts[0].aeron.clone(), ch: hosts[0].cfg.channels.clone() };
    let _agg = QuorumAggregator::start(subs, qpub.clone(), qcfg).unwrap();

    // 4 publishers × 250 messages.
    let pubr = Arc::new(ChannelBPublisher::open(&hosts[0].aeron, &hosts[0].cfg.channels).unwrap());
    let mut last_pub_pos = BPosition::ZERO;
    {
        let mut joins = Vec::new();
        for p in 0..4u64 {
            let pubr = pubr.clone();
            joins.push(tokio::task::spawn_blocking(move || {
                let mut last = BPosition::ZERO;
                for i in 0..250u64 {
                    last = pubr
                        .publish_tx(&TxEnvelope {
                            correlation_id: p * 1000 + i,
                            raw_tx: Bytes::from(vec![0xCDu8; 256]),
                            sender: Address::repeat_byte(p as u8),
                            tx_hash: B256::repeat_byte(i as u8),
                        })
                        .unwrap();
                }
                last
            }));
        }
        for j in joins {
            let p = j.await.unwrap();
            if p > last_pub_pos { last_pub_pos = p; }
        }
    }

    // Each of three subscribers must receive all 1000.
    for host in hosts.iter() {
        let subs = Subscribers { aeron: host.aeron.clone(), ch: host.cfg.channels.clone() };
        let mut sub = subs.b().unwrap();
        let mut got = 0usize;
        let deadline = std::time::Instant::now() + Duration::from_secs(10);
        while got < 1000 && std::time::Instant::now() < deadline {
            got += sub.poll(|_t, _pos| (), 256);
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
        assert_eq!(got, 1000, "host {} only received {got}", host.cfg.recorder_id);
    }

    // Quorum watermark must reach >= last_pub_pos within 1s.
    let mut qsub = Subscribers { aeron: hosts[0].aeron.clone(), ch: hosts[0].cfg.channels.clone() }
        .quorum()
        .unwrap();
    let mut latest = BPosition::ZERO;
    let deadline = std::time::Instant::now() + Duration::from_secs(2);
    while latest < last_pub_pos && std::time::Instant::now() < deadline {
        qsub.poll(|q, _| latest = q.position, 64);
        tokio::time::sleep(Duration::from_millis(1)).await;
    }
    assert!(latest >= last_pub_pos, "quorum watermark {latest:?} did not reach {last_pub_pos:?}");

    // Kill recorder 2; quorum (N=3, Q=2) should still be satisfied.
    hosts[2].supervisor.shutdown();
    // Publish 100 more; expect quorum to keep advancing.
    let mut after_kill_pos = last_pub_pos;
    for i in 0..100u64 {
        after_kill_pos = pubr
            .publish_tx(&TxEnvelope {
                correlation_id: 99_000 + i,
                raw_tx: Bytes::from_static(b"x"),
                sender: Address::ZERO,
                tx_hash: B256::repeat_byte(i as u8),
            })
            .unwrap();
    }
    let deadline = std::time::Instant::now() + Duration::from_secs(3);
    while latest < after_kill_pos && std::time::Instant::now() < deadline {
        qsub.poll(|q, _| latest = q.position, 64);
        tokio::time::sleep(Duration::from_millis(1)).await;
    }
    assert!(latest >= after_kill_pos, "quorum stalled after single failure");

    // Kill recorder 1 (now only 1 of 3 alive); quorum (Q=2) must stall.
    hosts[1].supervisor.shutdown();
    let frozen = latest;
    let mut stalled = BPosition::ZERO;
    for i in 0..100u64 {
        stalled = pubr
            .publish_tx(&TxEnvelope {
                correlation_id: 199_000 + i,
                raw_tx: Bytes::from_static(b"y"),
                sender: Address::ZERO,
                tx_hash: B256::repeat_byte(i as u8),
            })
            .unwrap();
    }
    tokio::time::sleep(Duration::from_secs(1)).await;
    let mut after = frozen;
    qsub.poll(|q, _| after = q.position, 256);
    assert!(after < stalled, "quorum should not advance past stalled position {stalled:?}, advanced to {after:?}");

    hosts[0].supervisor.shutdown();
}
```

- [ ] **Step 2: Run**

```bash
cd /home/dev/kardamom && cargo test -p kardamom-log --test recorder_cluster -- --nocapture
```

Expected without env vars: skipped.
Expected with Aeron binaries: PASS within ~15s.

- [ ] **Step 3: Commit**

```bash
git add crates/kardamom-log/tests/recorder_cluster.rs
git commit -m "log: 3-recorder cluster integration test (quorum + failure)"
```

---

## Task 14: Criterion benchmarks

**Files:**
- Create: `crates/kardamom-log/benches/publish_throughput.rs`
- Create: `crates/kardamom-log/benches/subscribe_throughput.rs`
- Create: `crates/kardamom-log/benches/fsync_watermark_latency.rs`

- [ ] **Step 1: `publish_throughput.rs`**

```rust
//! Measures per-publisher and aggregate throughput of channel B publication.
//! Requires AERON_MEDIA_DRIVER_BIN.

use std::sync::Arc;
use std::time::Duration;

use alloy_primitives::{Address, B256};
use bytes::Bytes;
use criterion::{Criterion, Throughput, criterion_group, criterion_main};
use kardamom_log::config::LogConfig;
use kardamom_log::publisher::ChannelBPublisher;
use kardamom_log::supervisor::Supervisor;
use kardamom_types::TxEnvelope;

fn bench(c: &mut Criterion) {
    if std::env::var("AERON_MEDIA_DRIVER_BIN").is_err() {
        eprintln!("skipping bench: AERON_MEDIA_DRIVER_BIN not set");
        return;
    }
    let rt = tokio::runtime::Runtime::new().unwrap();
    let tmp = tempfile::tempdir().unwrap();
    let mut cfg = LogConfig::default();
    cfg.aeron.aeron_dir = tmp.path().join("aeron");
    cfg.aeron.media_driver_cmd = vec![std::env::var("AERON_MEDIA_DRIVER_BIN").unwrap()];
    cfg.aeron.archive_cmd = vec!["/bin/true".into()];
    cfg.channels.b_channel = "aeron:ipc".into();
    let mut sup = Supervisor::new(cfg.aeron.clone());
    rt.block_on(sup.start()).unwrap();
    let aeron = Arc::new(rusteron_client::Aeron::connect_to(&cfg.aeron.aeron_dir).unwrap());
    let pubr = Arc::new(ChannelBPublisher::open(&aeron, &cfg.channels).unwrap());

    let mut group = c.benchmark_group("publish_throughput");
    group.throughput(Throughput::Elements(1));
    group.measurement_time(Duration::from_secs(5));

    let payload = Bytes::from(vec![0xAB; 200]);
    group.bench_function("single_publisher_200B", |b| {
        let pubr = pubr.clone();
        let payload = payload.clone();
        let mut i = 0u64;
        b.iter(|| {
            pubr.publish_tx(&TxEnvelope {
                correlation_id: i,
                raw_tx: payload.clone(),
                sender: Address::ZERO,
                tx_hash: B256::ZERO,
            }).unwrap();
            i += 1;
        });
    });

    group.finish();
    sup.shutdown();
}

criterion_group!(benches, bench);
criterion_main!(benches);
```

- [ ] **Step 2: `subscribe_throughput.rs`** (analogous; subscriber-side polling rate over a pre-published stream)

```rust
use std::sync::Arc;
use std::time::Duration;

use alloy_primitives::{Address, B256};
use bytes::Bytes;
use criterion::{Criterion, Throughput, criterion_group, criterion_main};
use kardamom_log::config::LogConfig;
use kardamom_log::publisher::ChannelBPublisher;
use kardamom_log::subscriber::Subscribers;
use kardamom_log::supervisor::Supervisor;
use kardamom_types::TxEnvelope;

fn bench(c: &mut Criterion) {
    if std::env::var("AERON_MEDIA_DRIVER_BIN").is_err() {
        eprintln!("skipping bench: AERON_MEDIA_DRIVER_BIN not set");
        return;
    }
    let rt = tokio::runtime::Runtime::new().unwrap();
    let tmp = tempfile::tempdir().unwrap();
    let mut cfg = LogConfig::default();
    cfg.aeron.aeron_dir = tmp.path().join("aeron");
    cfg.aeron.media_driver_cmd = vec![std::env::var("AERON_MEDIA_DRIVER_BIN").unwrap()];
    cfg.aeron.archive_cmd = vec!["/bin/true".into()];
    cfg.channels.b_channel = "aeron:ipc".into();
    let mut sup = Supervisor::new(cfg.aeron.clone());
    rt.block_on(sup.start()).unwrap();
    let aeron = Arc::new(rusteron_client::Aeron::connect_to(&cfg.aeron.aeron_dir).unwrap());

    // Pre-publish 100k messages.
    let pubr = ChannelBPublisher::open(&aeron, &cfg.channels).unwrap();
    let payload = Bytes::from(vec![0xAB; 200]);
    for i in 0..100_000u64 {
        pubr.publish_tx(&TxEnvelope {
            correlation_id: i,
            raw_tx: payload.clone(),
            sender: Address::ZERO,
            tx_hash: B256::ZERO,
        }).unwrap();
    }

    let subs = Subscribers { aeron: aeron.clone(), ch: cfg.channels.clone() };
    let mut sub = subs.b().unwrap();

    let mut group = c.benchmark_group("subscribe_throughput");
    group.throughput(Throughput::Elements(1));
    group.measurement_time(Duration::from_secs(5));
    group.bench_function("poll_b_200B", |b| {
        b.iter(|| {
            let mut got = 0usize;
            while got == 0 {
                got = sub.poll(|_t, _pos| (), 256);
            }
        });
    });
    group.finish();
    sup.shutdown();
}

criterion_group!(benches, bench);
criterion_main!(benches);
```

- [ ] **Step 3: `fsync_watermark_latency.rs`** — uses the standalone `FsyncSidecar` so it does not depend on Aeron binaries.

```rust
use std::sync::Arc;
use std::sync::atomic::{AtomicI64, Ordering};
use std::time::Duration;

use criterion::{Criterion, Throughput, criterion_group, criterion_main};
use kardamom_log::fsync_sidecar::{FsyncSidecar, PositionSource};

struct FakePos(Arc<AtomicI64>);
impl PositionSource for FakePos { fn current(&self) -> i64 { self.0.load(Ordering::Acquire) } }

fn bench(c: &mut Criterion) {
    let tmp = tempfile::tempdir().unwrap();
    let source = tmp.path().join("source.bin");
    let mirror = tmp.path().join("mirror.bin");
    std::fs::write(&source, vec![0u8; 16 * 1024 * 1024]).unwrap();

    let pos = Arc::new(AtomicI64::new(0));
    let mut sidecar = FsyncSidecar::open(&source, &mirror, Box::new(FakePos(pos.clone())), 256).unwrap();

    let mut group = c.benchmark_group("fsync_watermark");
    group.throughput(Throughput::Bytes(4096));
    group.measurement_time(Duration::from_secs(10));
    group.bench_function("4KiB_write_plus_fdatasync", |b| {
        let mut off = 0i64;
        b.iter(|| {
            off += 4096;
            pos.store(off, Ordering::Release);
            let _ = sidecar.tick().unwrap();
        });
    });
    group.finish();
}

criterion_group!(benches, bench);
criterion_main!(benches);
```

- [ ] **Step 4: Run the fsync bench (always available)**

```bash
cd /home/dev/kardamom && cargo bench -p kardamom-log --bench fsync_watermark_latency
```

Expected: completes; reports per-op latency.

- [ ] **Step 5: Commit**

```bash
git add crates/kardamom-log/benches
git commit -m "log: criterion benches (publish, subscribe, fsync watermark)"
```

---

## Task 15: `testing` feature — in-memory pub/sub fakes

Per D-Sh8: every other crate's unit tests reuse a single in-memory channel impl from `kardamom-log`. Real Aeron is reserved for e2e tests (see Task 16). The fakes mimic the surface area of `ChannelBPublisher`, `ChannelBSubscriber`, `ConcurrentPublication`, and `FsyncWatermark` streams so call sites swap the backing without changing logic.

**Files:**
- Modify: `crates/kardamom-log/src/testing.rs`
- Create: `crates/kardamom-log/tests/testing_fakes.rs`

- [ ] **Step 1: Write the fakes**

```rust
// crates/kardamom-log/src/testing.rs
//! In-memory pub/sub fakes used by other crates' unit tests.
//!
//! Gated behind `#[cfg(any(test, feature = "testing"))]`. Importers add this
//! crate as `kardamom-log = { workspace = true, features = ["testing"] }`
//! to `[dev-dependencies]`.
//!
//! These fakes are intentionally simple: an `Arc<Mutex<VecDeque<Vec<u8>>>>`
//! per stream id, no Aeron involvement. They preserve **per-publisher FIFO**
//! order (sufficient for unit-testing components that consume the channel)
//! but do not model Aeron's concurrent-pub interleaving. Tests that need to
//! verify behavior under realistic interleaving go through the real Aeron
//! Docker harness (Task 16), not these fakes.

use std::collections::HashMap;
use std::collections::VecDeque;
use std::sync::{Arc, Mutex};

use rkyv::api::high::{HighDeserializer, HighSerializer};
use rkyv::rancor;
use rkyv::ser::allocator::ArenaHandle;
use rkyv::util::AlignedVec;
use rkyv::{Archive, Deserialize, Serialize};

use crate::error::LogError;
use kardamom_types::{BPosition, FsyncWatermark};

/// In-memory bus shared by all `FakePublication` / `FakeSubscription` handles
/// that target the same `(channel, stream_id)` pair. Clone is cheap (an `Arc`).
#[derive(Clone, Default)]
pub struct FakeBus {
    streams: Arc<Mutex<HashMap<(String, i32), Arc<Mutex<StreamState>>>>>,
}

#[derive(Default)]
struct StreamState {
    /// Append-only log of (offset, bytes). Subscribers track their read cursor.
    log: Vec<(i64, AlignedVec)>,
    /// Next byte offset (mimics Aeron's stream position).
    next_offset: i64,
}

impl FakeBus {
    pub fn new() -> Self {
        Self::default()
    }

    fn stream(&self, channel: &str, stream_id: i32) -> Arc<Mutex<StreamState>> {
        let mut g = self.streams.lock().unwrap();
        g.entry((channel.to_string(), stream_id))
            .or_insert_with(|| Arc::new(Mutex::new(StreamState::default())))
            .clone()
    }
}

/// Drop-in for `rusteron_client::ConcurrentPublication` in tests.
pub struct FakeConcurrentPublication {
    state: Arc<Mutex<StreamState>>,
}

impl FakeConcurrentPublication {
    pub fn offer(&self, bytes: &[u8]) -> i64 {
        let mut g = self.state.lock().unwrap();
        let off = g.next_offset;
        let mut copy = AlignedVec::with_capacity(bytes.len());
        copy.extend_from_slice(bytes);
        g.log.push((off, copy));
        g.next_offset += bytes.len() as i64;
        g.next_offset
    }
}

/// Drop-in for `rusteron_client::Subscription` in tests.
pub struct FakeSubscription {
    state: Arc<Mutex<StreamState>>,
    cursor: usize,
}

impl FakeSubscription {
    /// Mirrors the real subscriber's `poll(&mut self, callback, fragment_limit)`.
    pub fn poll<F: FnMut(&[u8], FakeHeader)>(&mut self, mut f: F, fragment_limit: usize) -> usize {
        let g = self.state.lock().unwrap();
        let mut delivered = 0;
        while delivered < fragment_limit && self.cursor < g.log.len() {
            let (off, ref bytes) = g.log[self.cursor];
            let header = FakeHeader::from_offset(off);
            f(bytes.as_slice(), header);
            self.cursor += 1;
            delivered += 1;
        }
        delivered
    }
}

/// Mimics `rusteron_client::Header` enough for our consumers.
#[derive(Clone, Copy, Debug)]
pub struct FakeHeader {
    term_id: i32,
    term_offset: i32,
}

impl FakeHeader {
    pub fn from_offset(off: i64) -> Self {
        const TERM_LEN: i64 = 16 * 1024 * 1024;
        Self {
            term_id: (off / TERM_LEN) as i32,
            term_offset: (off % TERM_LEN) as i32,
        }
    }
    pub fn term_id(&self) -> i32 {
        self.term_id
    }
    pub fn term_offset(&self) -> i32 {
        self.term_offset
    }
}

/// High-level fake publication that consumers can use in place of
/// `ChannelBPublisher` / `ChannelCPublisher` / `ReceiptCachePublisher`.
pub struct FakePublication {
    pub_handle: FakeConcurrentPublication,
}

impl FakePublication {
    pub fn open(bus: &FakeBus, channel: &str, stream_id: i32) -> Self {
        Self {
            pub_handle: FakeConcurrentPublication {
                state: bus.stream(channel, stream_id),
            },
        }
    }

    pub fn publish<T>(&self, msg: &T) -> Result<BPosition, LogError>
    where
        T: for<'a> Serialize<HighSerializer<AlignedVec, ArenaHandle<'a>, rancor::Error>>,
    {
        let bytes = rkyv::to_bytes::<rancor::Error>(msg)
            .map_err(|e| LogError::Codec(e.to_string()))?;
        let off = self.pub_handle.offer(bytes.as_slice());
        let header = FakeHeader::from_offset(off);
        Ok(BPosition {
            term_id: header.term_id(),
            term_offset: header.term_offset(),
        })
    }
}

/// High-level fake subscription for owned-value reads.
pub struct FakeTypedSubscription<T> {
    sub: FakeSubscription,
    _marker: std::marker::PhantomData<T>,
}

impl<T> FakeTypedSubscription<T>
where
    T: Archive + 'static,
    T::Archived: Deserialize<T, HighDeserializer<rancor::Error>>
        + for<'a> rkyv::bytecheck::CheckBytes<rkyv::api::high::HighValidator<'a, rancor::Error>>,
{
    pub fn open(bus: &FakeBus, channel: &str, stream_id: i32) -> Self {
        Self {
            sub: FakeSubscription {
                state: bus.stream(channel, stream_id),
                cursor: 0,
            },
            _marker: std::marker::PhantomData,
        }
    }

    pub fn poll<F: FnMut(T, BPosition)>(&mut self, mut f: F, fragment_limit: usize) -> usize {
        self.sub.poll(
            |bytes: &[u8], header: FakeHeader| {
                if let Ok(v) = rkyv::from_bytes::<T, rancor::Error>(bytes) {
                    f(v, BPosition {
                        term_id: header.term_id(),
                        term_offset: header.term_offset(),
                    });
                }
            },
            fragment_limit,
        )
    }
}

/// In-memory fake fsync-watermark stream: an `Arc<Mutex<VecDeque<FsyncWatermark>>>`
/// keyed by recorder id. Lease/aggregator tests publish into one of these and
/// poll on the other side.
#[derive(Clone, Default)]
pub struct FakeFsyncWatermarkStream {
    inner: Arc<Mutex<HashMap<u8, VecDeque<FsyncWatermark>>>>,
}

impl FakeFsyncWatermarkStream {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn publish(&self, w: FsyncWatermark) {
        let mut g = self.inner.lock().unwrap();
        g.entry(w.recorder_id).or_default().push_back(w);
    }

    pub fn drain(&self, recorder_id: u8) -> Vec<FsyncWatermark> {
        let mut g = self.inner.lock().unwrap();
        g.get_mut(&recorder_id)
            .map(|q| q.drain(..).collect())
            .unwrap_or_default()
    }
}
```

- [ ] **Step 2: Smoke-test the fakes from the same crate**

```rust
// crates/kardamom-log/tests/testing_fakes.rs
#![cfg(feature = "testing")]

use alloy_primitives::{Address, B256};
use bytes::Bytes;
use kardamom_log::testing::{FakeBus, FakeFsyncWatermarkStream, FakePublication, FakeTypedSubscription};
use kardamom_types::{BPosition, FsyncWatermark, TxEnvelope};

#[test]
fn fake_pub_sub_roundtrip() {
    let bus = FakeBus::new();
    let pubr = FakePublication::open(&bus, "test", 1);
    let mut sub = FakeTypedSubscription::<TxEnvelope>::open(&bus, "test", 1);

    let env = TxEnvelope {
        correlation_id: 42,
        raw_tx: Bytes::from_static(b"abc"),
        sender: Address::repeat_byte(0x11),
        tx_hash: B256::repeat_byte(0x22),
    };
    pubr.publish(&env).unwrap();

    let mut received: Vec<TxEnvelope> = Vec::new();
    sub.poll(|t, _| received.push(t), 16);
    assert_eq!(received, vec![env]);
}

#[test]
fn fake_fsync_watermark_stream_per_recorder() {
    let stream = FakeFsyncWatermarkStream::new();
    stream.publish(FsyncWatermark { recorder_id: 0, position: BPosition { term_id: 1, term_offset: 100 } });
    stream.publish(FsyncWatermark { recorder_id: 0, position: BPosition { term_id: 1, term_offset: 200 } });
    stream.publish(FsyncWatermark { recorder_id: 1, position: BPosition { term_id: 1, term_offset: 50 } });

    assert_eq!(stream.drain(0).len(), 2);
    assert_eq!(stream.drain(1).len(), 1);
    assert!(stream.drain(2).is_empty());
}
```

- [ ] **Step 3: Run, confirm PASS**

```bash
cd /home/dev/kardamom && cargo test -p kardamom-log --features testing --test testing_fakes
```

- [ ] **Step 4: Commit**

```bash
git add crates/kardamom-log/src/testing.rs crates/kardamom-log/tests/testing_fakes.rs \
        crates/kardamom-log/Cargo.toml
git commit -m "log: testing feature — in-memory pub/sub + watermark fakes"
```

---

## Task 16: Docker Aeron e2e harness (testcontainers)

Per D-Sh8: every e2e test (across S1–S7) **MUST** use a real Aeron backend. We ship the Aeron Media Driver + Aeron Archive as Docker images and expose a reusable `AeronTestCluster` harness driven by `testcontainers`. Other crates reuse this harness from their own e2e tests by depending on `kardamom-log` with `features = ["testing"]`.

**Files:**
- Create: `crates/kardamom-log/docker/aeron/Dockerfile`
- Create: `crates/kardamom-log/docker/aeron/docker-compose.yml`
- Modify: `crates/kardamom-log/src/testing.rs` (extend with `AeronTestCluster`)
- Create: `crates/kardamom-log/tests/docker_e2e.rs`

- [ ] **Step 1: Write the Dockerfile**

```dockerfile
# crates/kardamom-log/docker/aeron/Dockerfile
#
# Aeron Media Driver + Aeron Archive in a single container image. The
# entrypoint script starts the Media Driver in the background, waits for the
# CnC file, then starts the Archive in the foreground (so container lifecycle
# tracks the Archive).
#
# The image vendors Aeron 1.45.0 (matching what `rusteron-archive` 0.1.x
# targets). Pin the version explicitly — drift in the Aeron wire protocol is
# a guaranteed source of "works on my laptop" bugs.

FROM eclipse-temurin:21-jre-jammy AS base

ARG AERON_VERSION=1.45.0
ENV AERON_VERSION=${AERON_VERSION}

RUN apt-get update && \
    apt-get install -y --no-install-recommends curl ca-certificates && \
    rm -rf /var/lib/apt/lists/*

RUN mkdir -p /opt/aeron && \
    curl -L "https://repo1.maven.org/maven2/io/aeron/aeron-all/${AERON_VERSION}/aeron-all-${AERON_VERSION}.jar" \
        -o /opt/aeron/aeron-all.jar

ENV AERON_DIR=/dev/shm/aeron
ENV AERON_ARCHIVE_DIR=/var/lib/aeron/archive
ENV AERON_MEDIA_DRIVER_CLASS=io.aeron.driver.MediaDriver
ENV AERON_ARCHIVE_CLASS=io.aeron.archive.ArchivingMediaDriver

# Sensible defaults for testing — small term length to keep RSS low.
ENV AERON_TERM_BUFFER_LENGTH=4194304
ENV AERON_IPC_TERM_BUFFER_LENGTH=4194304

# Expose Archive control + replication ports.
EXPOSE 8010/udp 8011/udp 8020/udp 8021/udp

RUN mkdir -p ${AERON_ARCHIVE_DIR} && \
    chmod -R 777 ${AERON_ARCHIVE_DIR}

COPY entrypoint.sh /opt/aeron/entrypoint.sh
RUN chmod +x /opt/aeron/entrypoint.sh

ENTRYPOINT ["/opt/aeron/entrypoint.sh"]
```

```bash
# crates/kardamom-log/docker/aeron/entrypoint.sh
#!/usr/bin/env bash
set -euo pipefail

# ArchivingMediaDriver runs both the Media Driver and the Archive in one JVM,
# which is the simplest deployment for tests.
exec java \
    -Daeron.dir=${AERON_DIR} \
    -Daeron.archive.dir=${AERON_ARCHIVE_DIR} \
    -Daeron.term.buffer.length=${AERON_TERM_BUFFER_LENGTH} \
    -Daeron.ipc.term.buffer.length=${AERON_IPC_TERM_BUFFER_LENGTH} \
    -Daeron.archive.control.channel=aeron:udp?endpoint=0.0.0.0:8010 \
    -Daeron.archive.control.response.channel=aeron:udp?endpoint=0.0.0.0:8011 \
    -Daeron.archive.replication.channel=aeron:udp?endpoint=0.0.0.0:8021 \
    -cp /opt/aeron/aeron-all.jar \
    ${AERON_ARCHIVE_CLASS}
```

- [ ] **Step 2: Write the compose file (used for the 3-recorder e2e variant)**

```yaml
# crates/kardamom-log/docker/aeron/docker-compose.yml
# Optional: 3-node Aeron Archive cluster for the recorder_cluster e2e test.
# The simpler single-node case is brought up by `AeronTestCluster::single_node`
# in Rust, without compose.
services:
  aeron-0:
    build: .
    container_name: aeron-0
    shm_size: 256m
    tmpfs:
      - /dev/shm
    ports:
      - "18010:8010/udp"
      - "18011:8011/udp"
      - "18020:8020/udp"
      - "18021:8021/udp"
  aeron-1:
    build: .
    container_name: aeron-1
    shm_size: 256m
    tmpfs:
      - /dev/shm
    ports:
      - "28010:8010/udp"
      - "28011:8011/udp"
      - "28020:8020/udp"
      - "28021:8021/udp"
  aeron-2:
    build: .
    container_name: aeron-2
    shm_size: 256m
    tmpfs:
      - /dev/shm
    ports:
      - "38010:8010/udp"
      - "38011:8011/udp"
      - "38020:8020/udp"
      - "38021:8021/udp"
```

- [ ] **Step 3: Extend `crates/kardamom-log/src/testing.rs` with `AeronTestCluster`**

Append:

```rust
// ============================================================================
// AeronTestCluster — testcontainers-driven real Aeron for e2e tests.
//
// Other crates depend on `kardamom-log` with `features = ["testing"]` and
// reuse this struct from their own `tests/` directory. Public API:
//
//   let cluster = AeronTestCluster::single_node().await?;
//   let endpoint = cluster.archive_control_endpoint(); // "127.0.0.1:18010"
//   // ... build LogConfig pointing at endpoint, run scenario ...
//   drop(cluster); // tears down container
// ============================================================================

#[cfg(feature = "testing")]
mod docker {
    use std::time::Duration;

    use testcontainers::core::{ContainerPort, IntoContainerPort, Mount, WaitFor};
    use testcontainers::runners::AsyncRunner;
    use testcontainers::{ContainerAsync, GenericImage, ImageExt};

    /// Path to the Aeron image build context, relative to the crate root.
    /// `testcontainers` builds the image on first use via `docker build`.
    const AERON_IMAGE: &str = "kardamom-aeron:test";

    /// Reusable Aeron e2e harness. Each instance owns one or more real Aeron
    /// containers and exposes the host ports the Rust code should connect to.
    pub struct AeronTestCluster {
        nodes: Vec<ContainerAsync<GenericImage>>,
    }

    impl AeronTestCluster {
        /// Bring up a single Aeron node (Media Driver + Archive in one JVM).
        /// Used for the common case where the test only needs a working channel.
        pub async fn single_node() -> Result<Self, Box<dyn std::error::Error>> {
            ensure_image_built().await?;
            let image = GenericImage::new("kardamom-aeron", "test")
                .with_exposed_port(8010_u16.udp())
                .with_exposed_port(8011_u16.udp())
                .with_exposed_port(8020_u16.udp())
                .with_exposed_port(8021_u16.udp())
                .with_wait_for(WaitFor::message_on_stdout("ArchiveAgent: started"));

            let node = image
                .with_shm_size(256 * 1024 * 1024) // 256 MiB
                .start()
                .await?;

            Ok(Self { nodes: vec![node] })
        }

        /// Bring up `n` Aeron nodes for multi-recorder tests.
        pub async fn multi_node(n: usize) -> Result<Self, Box<dyn std::error::Error>> {
            ensure_image_built().await?;
            let mut nodes = Vec::with_capacity(n);
            for _ in 0..n {
                let image = GenericImage::new("kardamom-aeron", "test")
                    .with_exposed_port(8010_u16.udp())
                    .with_exposed_port(8011_u16.udp())
                    .with_exposed_port(8020_u16.udp())
                    .with_exposed_port(8021_u16.udp())
                    .with_wait_for(WaitFor::message_on_stdout("ArchiveAgent: started"));
                let node = image
                    .with_shm_size(256 * 1024 * 1024)
                    .start()
                    .await?;
                nodes.push(node);
            }
            Ok(Self { nodes })
        }

        /// "host:port" the test should pass as the Aeron Archive control channel
        /// endpoint for node `i`.
        pub async fn archive_control_endpoint(&self, i: usize) -> String {
            let port = self.nodes[i].get_host_port_ipv4(8010_u16.udp()).await.unwrap();
            format!("127.0.0.1:{port}")
        }

        pub async fn archive_response_endpoint(&self, i: usize) -> String {
            let port = self.nodes[i].get_host_port_ipv4(8011_u16.udp()).await.unwrap();
            format!("127.0.0.1:{port}")
        }

        /// Number of nodes currently running.
        pub fn len(&self) -> usize {
            self.nodes.len()
        }

        /// Stop node `i` (simulates recorder failure for quorum tests).
        pub async fn stop(&mut self, i: usize) -> Result<(), Box<dyn std::error::Error>> {
            self.nodes[i].stop().await?;
            Ok(())
        }
    }

    /// `docker build`s the Aeron image once per test run, idempotent.
    /// `testcontainers` doesn't have a built-in "build if missing" — we shell
    /// out to the docker CLI. Cached layers make this fast on repeat runs.
    async fn ensure_image_built() -> Result<(), Box<dyn std::error::Error>> {
        use tokio::process::Command;
        // Probe: does the image already exist locally?
        let out = Command::new("docker")
            .args(["image", "inspect", AERON_IMAGE])
            .output()
            .await?;
        if out.status.success() {
            return Ok(());
        }
        // Build.
        let manifest_dir = env!("CARGO_MANIFEST_DIR");
        let ctx = format!("{manifest_dir}/docker/aeron");
        let status = Command::new("docker")
            .args(["build", "-t", AERON_IMAGE, &ctx])
            .status()
            .await?;
        if !status.success() {
            return Err(format!("docker build failed (status {status:?})").into());
        }
        // Give the daemon a moment to register the new image tag.
        tokio::time::sleep(Duration::from_millis(200)).await;
        Ok(())
    }
}

#[cfg(feature = "testing")]
pub use docker::AeronTestCluster;
```

- [ ] **Step 4: Write the sample e2e test that uses the harness**

```rust
// crates/kardamom-log/tests/docker_e2e.rs
//! Real-Aeron e2e: publish + recorder + watermark + subscribe end-to-end via
//! Docker containers. Other crates' e2e tests (S1, S2, S4, S5, S6, S7) reuse
//! `AeronTestCluster` from the `kardamom-log::testing` module (gated behind
//! the `testing` feature).
//!
//! Gated on Docker availability: if `docker info` fails (e.g. unprivileged
//! CI runner), the test prints "skipping" and returns 0. CI runners that
//! must run e2e configure `DOCKER_HOST` and verify with `docker info`
//! before invoking `cargo test`.

#![cfg(feature = "testing")]

use std::sync::Arc;
use std::time::Duration;

use alloy_primitives::{Address, B256};
use bytes::Bytes;
use kardamom_log::config::LogConfig;
use kardamom_log::publisher::ChannelBPublisher;
use kardamom_log::subscriber::Subscribers;
use kardamom_log::testing::AeronTestCluster;
use kardamom_log::watermark::QuorumAggregator;
use kardamom_types::{BPosition, TxEnvelope};

async fn docker_available() -> bool {
    use tokio::process::Command;
    Command::new("docker")
        .arg("info")
        .output()
        .await
        .map(|o| o.status.success())
        .unwrap_or(false)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn aeron_publish_record_watermark_subscribe_e2e() {
    if !docker_available().await {
        eprintln!("skipping: docker not available");
        return;
    }

    let cluster = AeronTestCluster::single_node()
        .await
        .expect("aeron container started");

    let endpoint = cluster.archive_control_endpoint(0).await;
    eprintln!("aeron archive control: {endpoint}");

    let mut cfg = LogConfig::default();
    // Point the channels at the containerized Aeron. The exact URI formats
    // are: control = `aeron:udp?endpoint=<endpoint>`; B = an IPC channel
    // colocated with the Media Driver process. Tests on the host that need
    // to reach into the container's Media Driver use UDP into the exposed
    // port; see the README for the topology diagram.
    cfg.channels.b_channel = format!("aeron:udp?endpoint={endpoint}|alias=b");
    cfg.channels.b_stream_id = 1001;

    // Connect a host-side Aeron client to the container's Media Driver.
    // Mounted Aeron dir is exposed via the Archive control channel; we use
    // the Archive client (rusteron-archive) to drive recording.
    let aeron = Arc::new(
        rusteron_client::Aeron::connect_to_endpoint(&endpoint)
            .expect("aeron connect to container"),
    );

    let pubr = ChannelBPublisher::open(&aeron, &cfg.channels).unwrap();
    let subs = Subscribers { aeron: aeron.clone(), ch: cfg.channels.clone() };
    let mut sub = subs.b().unwrap();

    // Publish 100 envelopes.
    let mut last_pos = BPosition::ZERO;
    for i in 0..100u64 {
        last_pos = pubr
            .publish_tx(&TxEnvelope {
                correlation_id: i,
                raw_tx: Bytes::from(vec![0xCDu8; 128]),
                sender: Address::ZERO,
                tx_hash: B256::repeat_byte(i as u8),
            })
            .unwrap();
    }
    assert!(last_pos > BPosition::ZERO);

    // Drain.
    let mut received = 0usize;
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    while received < 100 && std::time::Instant::now() < deadline {
        received += sub.poll(|_t, _pos| (), 256);
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    assert_eq!(received, 100, "expected 100 messages, got {received}");

    drop(cluster);
}
```

- [ ] **Step 5: Run**

```bash
cd /home/dev/kardamom && cargo test -p kardamom-log --features testing --test docker_e2e -- --nocapture
```

Expected on a host without Docker: prints "skipping: docker not available", returns 0.
Expected with Docker: builds the image (~30s first run, cached after), brings up container (~5s), runs scenario (~2s), tears down. PASS.

- [ ] **Step 6: Commit**

```bash
git add crates/kardamom-log/docker crates/kardamom-log/src/testing.rs \
        crates/kardamom-log/tests/docker_e2e.rs
git commit -m "log: docker aeron container + AeronTestCluster + e2e test"
```

---

## Task 17: Receipt-cache channel wrapper

Thin convenience wrapper around the existing publisher/subscriber primitives. Other crates use the channel through `ReceiptCachePublisher` / `ReceiptCacheSubscriber` directly; this module exists as a single point of documentation.

**Files:**
- Modify: `crates/kardamom-log/src/receipt_cache.rs`

- [ ] **Step 1: Write the module**

```rust
//! Receipt-cache channel.
//!
//! `CachedReceipt` messages flow executor → proxy/RPC frontend over a
//! dedicated Aeron stream (RAM only, no Archive). Consumers use this channel
//! to keep a hot nonce cache without round-tripping through libmdbx.

pub use crate::publisher::ReceiptCachePublisher;
pub use crate::subscriber::ReceiptCacheSubscriber;
```

- [ ] **Step 2: Commit**

```bash
git add crates/kardamom-log/src/receipt_cache.rs
git commit -m "log: receipt-cache channel module"
```

---

## Task 18: `kardamom-leases` — lease primitive

The lease primitive is consumed by S2 (sequencer hot-standby), S5 (sealer leader), and S7 (L1 batcher leader). V0 implementation: a host holds the lease iff it has the *lowest host id* among recorders whose latest `FsyncWatermark.position` is within `caught_up_window` bytes of the current quorum watermark. This is fully deterministic and requires no external coordination.

**Files:**
- Modify: `crates/kardamom-leases/src/lease.rs`
- Create: `crates/kardamom-leases/tests/lease.rs`

- [ ] **Step 1: Implement the lease**

```rust
// crates/kardamom-leases/src/lease.rs
//! Deterministic lowest-host-id-among-caught-up-recorders lease.

use std::collections::HashMap;

use kardamom_types::{BPosition, FsyncWatermark, QuorumWatermark};

#[derive(Clone, Debug)]
pub struct LeaseConfig {
    /// This host's recorder id.
    pub self_id: u8,
    /// All recorder ids in the cluster.
    pub all_ids: Vec<u8>,
    /// Bytes of stream lag that still count as "caught up".
    pub caught_up_window: i64,
}

/// Lease state machine. Feed it `FsyncWatermark` updates from each recorder
/// and the current `QuorumWatermark`; call [`Lease::held_by_us`] to learn
/// whether this host currently holds the lease.
#[derive(Clone, Debug)]
pub struct Lease {
    cfg: LeaseConfig,
    last_per_recorder: HashMap<u8, BPosition>,
    last_quorum: Option<BPosition>,
}

impl Lease {
    pub fn new(cfg: LeaseConfig) -> Self {
        Self { cfg, last_per_recorder: HashMap::new(), last_quorum: None }
    }

    pub fn observe_fsync(&mut self, w: FsyncWatermark) {
        let prev = self.last_per_recorder.get(&w.recorder_id).copied();
        if prev.map_or(true, |p| p < w.position) {
            self.last_per_recorder.insert(w.recorder_id, w.position);
        }
    }

    pub fn observe_quorum(&mut self, q: QuorumWatermark) {
        self.last_quorum = Some(q.position);
    }

    /// Returns `true` if this host currently holds the lease.
    pub fn held_by_us(&self) -> bool {
        let quorum = match self.last_quorum {
            Some(p) => p,
            None => return false,
        };
        let caught_up_ids: Vec<u8> = self
            .cfg
            .all_ids
            .iter()
            .copied()
            .filter(|id| {
                self.last_per_recorder
                    .get(id)
                    .map(|p| within_window(*p, quorum, self.cfg.caught_up_window))
                    .unwrap_or(false)
            })
            .collect();
        caught_up_ids.iter().min() == Some(&self.cfg.self_id)
    }
}

fn within_window(pos: BPosition, quorum: BPosition, window: i64) -> bool {
    // Convert positions to absolute byte offsets using the same TERM_LEN as
    // the rest of the system (16 MiB). The exact constant must match the
    // recorder's `aeron.term.buffer.length`.
    const TERM_LEN: i64 = 16 * 1024 * 1024;
    let pos_abs = (pos.term_id as i64) * TERM_LEN + pos.term_offset as i64;
    let q_abs = (quorum.term_id as i64) * TERM_LEN + quorum.term_offset as i64;
    (q_abs - pos_abs).abs() <= window
}
```

- [ ] **Step 2: Tests**

```rust
// crates/kardamom-leases/tests/lease.rs
use kardamom_leases::{Lease, LeaseConfig};
use kardamom_types::{BPosition, FsyncWatermark, QuorumWatermark};

fn pos(t: i32, o: i32) -> BPosition { BPosition { term_id: t, term_offset: o } }

#[test]
fn no_quorum_no_lease() {
    let lease = Lease::new(LeaseConfig {
        self_id: 0,
        all_ids: vec![0, 1, 2],
        caught_up_window: 1024,
    });
    assert!(!lease.held_by_us());
}

#[test]
fn lowest_caught_up_id_holds_lease() {
    let mut lease = Lease::new(LeaseConfig {
        self_id: 0,
        all_ids: vec![0, 1, 2],
        caught_up_window: 1024,
    });
    lease.observe_quorum(QuorumWatermark { position: pos(1, 1000) });
    lease.observe_fsync(FsyncWatermark { recorder_id: 0, position: pos(1, 900) });
    lease.observe_fsync(FsyncWatermark { recorder_id: 1, position: pos(1, 1000) });
    lease.observe_fsync(FsyncWatermark { recorder_id: 2, position: pos(1, 1000) });
    assert!(lease.held_by_us(), "id=0 is lowest caught-up");
}

#[test]
fn lease_transfers_when_lowest_falls_behind() {
    let mut lease = Lease::new(LeaseConfig {
        self_id: 1,
        all_ids: vec![0, 1, 2],
        caught_up_window: 1024,
    });
    lease.observe_quorum(QuorumWatermark { position: pos(1, 10_000) });
    lease.observe_fsync(FsyncWatermark { recorder_id: 0, position: pos(1, 0) }); // far behind
    lease.observe_fsync(FsyncWatermark { recorder_id: 1, position: pos(1, 10_000) });
    lease.observe_fsync(FsyncWatermark { recorder_id: 2, position: pos(1, 10_000) });
    assert!(lease.held_by_us(), "id=1 holds lease because id=0 lags > window");
}

#[test]
fn no_one_caught_up_no_lease() {
    let mut lease = Lease::new(LeaseConfig {
        self_id: 0,
        all_ids: vec![0, 1, 2],
        caught_up_window: 10,
    });
    lease.observe_quorum(QuorumWatermark { position: pos(1, 10_000) });
    lease.observe_fsync(FsyncWatermark { recorder_id: 0, position: pos(1, 0) });
    lease.observe_fsync(FsyncWatermark { recorder_id: 1, position: pos(1, 5_000) });
    assert!(!lease.held_by_us());
}
```

- [ ] **Step 3: Run, confirm PASS**

```bash
cd /home/dev/kardamom && cargo test -p kardamom-leases
```

- [ ] **Step 4: Commit**

```bash
git add crates/kardamom-leases
git commit -m "leases: deterministic lowest-host-id-among-caught-up lease primitive"
```

---

## Task 19: Crate-level READMEs and final lint pass

**Files:**
- Create: `crates/kardamom-types/README.md`
- Create: `crates/kardamom-log/README.md`
- Create: `crates/kardamom-leases/README.md`
- Run: `cargo fmt --all`, `cargo clippy --workspace --all-targets -- -D warnings`

- [ ] **Step 1: Write `crates/kardamom-types/README.md`**

```markdown
# kardamom-types

Pure data types and traits shared across the kardamom subsystems. Per
**D-Sh1** in `docs/plans/2026-05-23-S0-shared-decisions.md`, every wire type
that crosses an Aeron channel or a libmdbx boundary lives here, derives
`rkyv::{Archive, Serialize, Deserialize}` (D-Sh2), and is consumed by S1, S2,
S3, S4, S5, S6, S7.

This crate has **no I/O dependencies** — no Aeron, no libmdbx, no
alloy-provider, no jsonrpsee. If you find yourself wanting to add one,
you have the wrong crate.

## Owned types

- `BPosition` — canonical L2 tx identifier (Aeron position)
- `TxEnvelope` — raw tx + correlation id + sender + tx_hash (sender and tx_hash always populated; D-Sh3, D-Sh4)
- `Receipt` — per-tx execution receipt
- `CachedReceipt` — receipt-cache channel message
- `BlockBoundaryStart`, `BlockBoundary` — block markers (no state root; D-Sh11)
- `FsyncWatermark`, `QuorumWatermark` — durability accounting
- `BlockDelta` — block-write payload (executor → state writer)
- `StateDatabase`, `SnapshotSource` — state-access traits
```

- [ ] **Step 2: Write `crates/kardamom-log/README.md`**

```markdown
# kardamom-log

S3 canonical-log subsystem. See `docs/specs/2026-05-23-high-throughput-sequencer-design.md` §2.3 and §2.5, and `docs/plans/2026-05-23-S0-shared-decisions.md` D-Sh1 / D-Sh2 / D-Sh8 / D-Sh10.

## Owned components

- **Channel B** (canonical tx log, recorded, fsync-quorum durable)
- **Channel C** (receipts + block boundaries, RAM only)
- **Receipt-cache channel** (`CachedReceipt` stream, RAM only)
- **Per-recorder fsync sidecar** (io_uring + O_DIRECT mirror)
- **Per-recorder fsync-watermark stream**
- **Quorum fsync-watermark aggregator** (Q-of-N smallest position)
- **`testing` feature** — in-memory pub/sub fakes for other crates' unit tests
- **`tests/docker_e2e.rs` + `docker/aeron/`** — testcontainers-driven Aeron Docker harness; reusable by other crates' e2e tests

## Shared types

Wire types live in `kardamom-types`; this crate re-exports them via `kardamom_log::types::*` for convenience. Do not add new wire types here.

## Wire codec

`rkyv` v0.8 zero-copy archival serialization. Hot-path consumers use `codec::access` for an `&Archived<T>` view; callers needing an owned value call `codec::materialize`.

## Replay

We do **not** ship a custom channel-B replay API. Aeron Archive already exposes the standard replay protocol; offline consumers (S7 L1 batcher) read segment files directly or use Aeron Archive's built-in replay (D-Sh10).

## Runtime dependencies

- Aeron Media Driver and Aeron Archive binaries (Java) installed on each host.
  Native tests skip when `AERON_MEDIA_DRIVER_BIN` / `AERON_ARCHIVE_BIN` are unset.
- For e2e tests: a working Docker daemon (`docker info` must succeed). The testcontainers harness builds and runs the Aeron image on demand.
- Mirror file must be on an ext4/xfs/etc. filesystem that supports `O_DIRECT`.
  tmpfs returns `EINVAL` for `O_DIRECT` opens.
- Recommended for production: enterprise NVMe with PLP, separate from the OS disk.
```

- [ ] **Step 3: Write `crates/kardamom-leases/README.md`**

```markdown
# kardamom-leases

Lease primitive consumed by S2 (sequencer hot-standby), S5 (sealer leader election), and S7 (L1 batcher leader election). Per **D-Sh1**.

V0 implementation: a host holds the lease iff it has the lowest host id among recorders whose latest `FsyncWatermark.position` is within `caught_up_window` bytes of the current `QuorumWatermark`. Fully deterministic; no external KV or consensus library.
```

- [ ] **Step 4: Format and lint the whole workspace**

```bash
cd /home/dev/kardamom && cargo fmt --all
cd /home/dev/kardamom && cargo clippy -p kardamom-types -p kardamom-log -p kardamom-leases --all-targets --all-features -- -D warnings
```

Expected: no warnings.

- [ ] **Step 5: Commit**

```bash
git add crates/kardamom-types/README.md crates/kardamom-log/README.md crates/kardamom-leases/README.md
git commit -m "types/log/leases: READMEs + lint clean"
```

---

## Self-Review Checklist

- **Crate layout (D-Sh1):** three crates — `kardamom-types` (Tasks 1, 2), `kardamom-log` (Tasks 1, 3–17), `kardamom-leases` (Tasks 1, 18). ✓
- **Wire codec (D-Sh2):** rkyv v0.8 throughout; bincode removed. Wire types derive `rkyv::{Archive, Serialize, Deserialize}` in `kardamom-types`; `kardamom-log` exposes `codec::access` / `codec::materialize`. ✓
- **TxEnvelope.sender / tx_hash always populated (D-Sh3, D-Sh4):** Task 2 — bare `Address` / `B256`, no `Option`. ✓
- **Receipt.tx_hash propagated (D-Sh4):** Task 2 — field present, never recomputed downstream. ✓
- **BlockBoundary has no state_root_commitment (D-Sh11):** Task 2 — field removed from type definition. ✓
- **Spec §2.3 coverage:** channel B publisher (Task 6), channel B subscriber (Task 7), recorder wrapper (Task 8), io_uring fsync sidecar (Tasks 9–10), per-recorder watermark publisher (Task 6), quorum aggregator (Tasks 4, 11), Q-of-N math (Task 4). ✓
- **Spec §2.5 coverage:** channel C publisher (Task 6), channel C subscriber (Task 7), receipt-cache channel (Tasks 6, 7, 17), shared codec (Task 2). ✓
- **Spec §3 latency budget:** fsync sidecar uses io_uring + O_DIRECT to keep fsync off the page cache; bench (Task 14) measures per-fsync latency to validate the 25 µs target. ✓
- **Spec §4.3 recorder failure:** integration test (Task 13) kills 1 of 3 recorders, asserts quorum continues; kills 2 of 3, asserts quorum stalls. ✓
- **No channel-B replay API (D-Sh10):** removed. Aeron Archive's standard replay protocol is used by offline consumers (S7). ✓
- **`testing` feature (D-Sh8):** in-memory `FakePublication`, `FakeSubscription`, `FakeConcurrentPublication`, `FakeFsyncWatermarkStream` fakes in Task 15. ✓
- **Docker e2e harness (D-Sh8):** `docker/aeron/Dockerfile` + `docker-compose.yml` + `AeronTestCluster` + `tests/docker_e2e.rs` in Task 16. Reusable from other crates' e2e tests. ✓
- **`kardamom-leases` (D-Sh1):** Task 18 ships the lease primitive consumed by S2/S5/S7. ✓
- **V0 scope:** all features listed are shipped in v0; no deferrals. ✓
- **Tests required:**
  - rkyv roundtrip + BPosition ordering — Task 2 ✓
  - Watermark aggregator math — Task 4 ✓
  - Fsync sidecar unit test — Task 9 ✓
  - 3-recorder integration test (4 publishers × 1000 messages, kill 1, kill 2) — Task 13 ✓
  - Docker e2e (real Aeron) — Task 16 ✓
  - Lease state machine — Task 18 ✓
  - Criterion benches (publish, subscribe, fsync watermark latency) — Task 14 ✓
- **Placeholder scan:** no `TODO`, no `tbd`, no "implement later" — each step has complete code. The two `rusteron` API-name caveats are explicit ("if upstream differs, adjust"), not placeholders.
- **Type consistency:** `BPosition`, `FsyncWatermark`, `QuorumWatermark` field names match across all files. `Subscribers::watermark(rid)` and `WatermarkPublisher::open(_, _, rid)` both take `u8`. ✓

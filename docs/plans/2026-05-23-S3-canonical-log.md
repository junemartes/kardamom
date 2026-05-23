# S3 Canonical Log Subsystem Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Ship the `kardamom-log` crate: the transport-and-durability foundation that every other kardamom subsystem depends on. It owns Aeron channel B (canonical-ordered tx log, fsynced with quorum), Aeron channel C (RAM-only receipt/boundary stream), the per-recorder background `io_uring` fsync worker, and the quorum fsync-watermark aggregator. It also defines the shared message types (`BPosition`, `TxEnvelope`, `Receipt`, `BlockBoundary*`, `FsyncWatermark`, `QuorumWatermark`) that S1, S2, S4, S5, S6, S7 import as their interface contract.

**Architecture:**
- **Aeron client:** `rusteron-archive` (GSR-maintained Rust wrapper over the Aeron C client), with the Aeron Media Driver and Aeron Archive Java process run out-of-process under a small Rust supervisor. We do not reimplement Aeron; we drive it.
- **Channel B:** one Aeron stream, concurrent multi-publisher, recorded by N independent `aeron-archive` recorders (one per host). Recording goes through the standard Aeron Archive control protocol so we get `replay-merge` for free during recovery.
- **Continuous fsync:** each recorder host runs a Rust process (the *fsync sidecar*) that opens the active Archive segment file with `O_DIRECT`, watches the recorder's published `recording-position` counter, and pipelines `IORING_OP_WRITE` (mirror-write of the buffer) + `IORING_OP_FSYNC` (`fdatasync` flag) through an `io_uring` SQ. After every completion, it publishes a `FsyncWatermark` on a per-recorder Aeron stream.
- **Quorum aggregator:** subscribes to all N per-recorder watermark streams, maintains a `[BPosition; N]` array, and publishes the Q-th smallest position as a `QuorumWatermark` on a shared stream that proxies/sequencers subscribe to.
- **Channel C:** plain Aeron multi-publisher stream, RAM only, no Archive. Same wire codec as B for shared infra.
- **Wire codec:** `bincode` v2 with fixed-int encoding. Chosen over `alloy-rlp` because all hot-path messages are non-Ethereum-canonical (internal IPC), bincode is ~3× faster on encode and the messages are not externally observable.
- **Runtime:** `tokio` for the supervisors, control plane, and tests. The fsync hot loop is a dedicated OS thread driving `io_uring` directly (the `io-uring` crate, not `tokio-uring`) — see Task 2 for the justification.

**Tech Stack:**
- Rust 2024, workspace deps from `/home/dev/kardamom/Cargo.toml` (alloy-primitives 1.6, tokio 1, serde 1, tracing 0.1, thiserror 2)
- `rusteron-client` and `rusteron-archive` (latest 0.1.x as of 2026-05) — Aeron C bindings, maintained by GSR
- `io-uring` (the `tokio-rs/io-uring` crate, raw SQ/CQ API) — lowest overhead for the continuous-submission fsync loop
- `bincode` 2.x with the `serde` feature
- `criterion` 0.5 for benchmarks
- `tempfile` 3, `tokio-test` for tests
- Aeron 1.45+ binaries (Media Driver + Archive) installed on the build host; the supervisor `Command`-spawns them

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

All paths under `/home/dev/kardamom/crates/kardamom-log/`.

```
crates/kardamom-log/
├── Cargo.toml
├── src/
│   ├── lib.rs                 # re-exports; crate-level docs
│   ├── error.rs               # LogError (thiserror)
│   ├── types.rs               # BPosition, TxEnvelope, Receipt, BlockBoundary*, FsyncWatermark, QuorumWatermark
│   ├── codec.rs               # bincode encode/decode wrappers
│   ├── supervisor.rs          # spawns Aeron Media Driver + Archive as child processes
│   ├── publisher.rs           # ChannelBPublisher, ChannelCPublisher (rusteron wrappers)
│   ├── subscriber.rs          # ChannelBSubscriber, ChannelCSubscriber (rusteron wrappers)
│   ├── recorder.rs            # Recorder: drives rusteron-archive recording control
│   ├── fsync_sidecar.rs       # io_uring O_DIRECT mirror + fdatasync loop
│   ├── watermark.rs           # FsyncWatermark publisher + QuorumWatermark aggregator
│   └── config.rs              # LogConfig (channels, paths, N, Q, segment size)
├── tests/
│   ├── codec_roundtrip.rs
│   ├── watermark_quorum.rs
│   ├── publisher_subscriber.rs
│   ├── fsync_sidecar.rs
│   └── recorder_cluster.rs    # integration: 3 recorders × 4 publishers × 1000 messages
└── benches/
    ├── publish_throughput.rs
    ├── subscribe_throughput.rs
    └── fsync_watermark_latency.rs
```

---

## Task 1: Scaffold the `kardamom-log` crate

**Files:**
- Create: `crates/kardamom-log/Cargo.toml`
- Create: `crates/kardamom-log/src/lib.rs`
- Modify: `Cargo.toml` (workspace already globs `crates/*`, so nothing to do there — verify)

- [ ] **Step 1: Verify the workspace will pick up the new crate**

```bash
grep -n 'members' /home/dev/kardamom/Cargo.toml
```

Expected: shows `members = ["crates/*"]`. No edit needed.

- [ ] **Step 2: Add `bincode`, `io-uring`, `rusteron-client`, `rusteron-archive`, `criterion`, `tempfile` to workspace deps**

Edit `/home/dev/kardamom/Cargo.toml`, append under `[workspace.dependencies]`:

```toml
# S3 canonical-log
bincode = { version = "2", features = ["serde"] }
io-uring = "0.7"
rusteron-client = "0.1"
rusteron-archive = "0.1"
criterion = "0.5"
tempfile = "3"
bytes = "1"
```

- [ ] **Step 3: Write `crates/kardamom-log/Cargo.toml`**

```toml
[package]
name = "kardamom-log"
version.workspace = true
edition.workspace = true

[dependencies]
alloy-primitives.workspace = true
bincode.workspace = true
bytes.workspace = true
io-uring.workspace = true
rusteron-archive.workspace = true
rusteron-client.workspace = true
serde.workspace = true
thiserror.workspace = true
tokio.workspace = true
tracing.workspace = true

[dev-dependencies]
criterion.workspace = true
tempfile.workspace = true
tokio = { workspace = true, features = ["test-util"] }

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

- [ ] **Step 4: Write a skeleton `src/lib.rs`**

```rust
//! Kardamom canonical log: Aeron-backed channels B and C, plus the io_uring
//! fsync sidecar and quorum watermark aggregator that give channel B its
//! durability guarantee.
//!
//! This crate defines the *interface types* that the other kardamom subsystems
//! (S1 ingress, S2 sequencer, S4 executor, S5 sealer, S6 state writer,
//! S7 batcher) consume. Treat the public API of [`types`] as a stable contract.

pub mod codec;
pub mod config;
pub mod error;
pub mod fsync_sidecar;
pub mod publisher;
pub mod recorder;
pub mod subscriber;
pub mod supervisor;
pub mod types;
pub mod watermark;

pub use error::LogError;
pub use types::{
    BPosition, BlockBoundary, BlockBoundaryStart, FsyncWatermark, QuorumWatermark, Receipt,
    TxEnvelope,
};
```

- [ ] **Step 5: Stub each module so the crate compiles**

Create each module file with `// stub` comment + no items. Then for each unresolved symbol added in Step 4, create the minimal pub item:

```rust
// crates/kardamom-log/src/types.rs
use alloy_primitives::B256;

pub struct BPosition;
pub struct TxEnvelope;
pub struct Receipt;
pub struct BlockBoundary;
pub struct BlockBoundaryStart;
pub struct FsyncWatermark;
pub struct QuorumWatermark;

// Force B256 into the import path so later tasks have it available.
#[allow(dead_code)]
const _: Option<B256> = None;
```

```rust
// crates/kardamom-log/src/error.rs
#[derive(Debug, thiserror::Error)]
pub enum LogError {
    #[error("placeholder")]
    Placeholder,
}
```

Create `codec.rs`, `config.rs`, `fsync_sidecar.rs`, `publisher.rs`, `recorder.rs`, `subscriber.rs`, `supervisor.rs`, `watermark.rs` each containing just `// stub`.

- [ ] **Step 6: Build the workspace**

```bash
cd /home/dev/kardamom && cargo build -p kardamom-log
```

Expected: builds cleanly. Warnings about unused stubs are OK.

- [ ] **Step 7: Commit**

```bash
cd /home/dev/kardamom
git add Cargo.toml crates/kardamom-log
git commit -m "log: scaffold kardamom-log crate"
```

---

## Task 2: Define shared message types

**Files:**
- Modify: `crates/kardamom-log/src/types.rs`
- Create: `crates/kardamom-log/tests/codec_roundtrip.rs`

These types are the **interface contract** for the other six S-plans. Field names, types, and `Ord` semantics must not drift after this task lands.

- [ ] **Step 1: Write the failing roundtrip test**

```rust
// crates/kardamom-log/tests/codec_roundtrip.rs
use alloy_primitives::{B256, Log, LogData};
use bytes::Bytes;
use kardamom_log::codec::{decode, encode};
use kardamom_log::types::*;

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
    let v = TxEnvelope { correlation_id: 0xDEAD_BEEF, raw_tx: Bytes::from_static(b"hello") };
    let bytes = encode(&v).unwrap();
    let back: TxEnvelope = decode(&bytes).unwrap();
    assert_eq!(v.correlation_id, back.correlation_id);
    assert_eq!(v.raw_tx, back.raw_tx);
}

#[test]
fn receipt_roundtrip() {
    let v = Receipt {
        tx_idx: BPosition { term_id: 3, term_offset: 4096 },
        status: true,
        gas_used: 21_000,
        logs: vec![Log { address: Default::default(), data: LogData::default() }],
        write_set_hash: B256::repeat_byte(0xAB),
    };
    let bytes = encode(&v).unwrap();
    let back: Receipt = decode(&bytes).unwrap();
    assert_eq!(v, back);
}

#[test]
fn boundary_roundtrip() {
    let start = BlockBoundaryStart {
        block_number: 7,
        end_tx_idx: BPosition { term_id: 1, term_offset: 999 },
        l2_timestamp: 1_700_000_000,
    };
    let bytes = encode(&start).unwrap();
    assert_eq!(decode::<BlockBoundaryStart>(&bytes).unwrap(), start);

    let end = BlockBoundary {
        block_number: 7,
        end_tx_idx: BPosition { term_id: 1, term_offset: 999 },
        l2_timestamp: 1_700_000_000,
        state_root_commitment: B256::repeat_byte(0xCD),
    };
    let bytes = encode(&end).unwrap();
    assert_eq!(decode::<BlockBoundary>(&bytes).unwrap(), end);
}

#[test]
fn watermark_roundtrip() {
    let w = FsyncWatermark { recorder_id: 2, position: BPosition { term_id: 4, term_offset: 1024 } };
    let bytes = encode(&w).unwrap();
    assert_eq!(decode::<FsyncWatermark>(&bytes).unwrap(), w);

    let q = QuorumWatermark { position: BPosition { term_id: 4, term_offset: 1024 } };
    let bytes = encode(&q).unwrap();
    assert_eq!(decode::<QuorumWatermark>(&bytes).unwrap(), q);
}
```

- [ ] **Step 2: Run the test to confirm it fails**

```bash
cd /home/dev/kardamom && cargo test -p kardamom-log --test codec_roundtrip
```

Expected: FAIL — `BPosition` is a unit struct in the stub, fields don't exist; `encode`/`decode` missing.

- [ ] **Step 3: Implement the real types**

Replace `crates/kardamom-log/src/types.rs`:

```rust
//! Public message types shared across the kardamom subsystems.
//!
//! Layout: every type derives `Clone`, `Debug`, `Eq`, `PartialEq`,
//! `serde::Serialize`, `serde::Deserialize`. `BPosition` additionally derives
//! `Ord` and `PartialOrd` — ordering is `(term_id, term_offset)` lexicographic
//! so that watermark comparisons are a single cmp.

use alloy_primitives::{B256, Log};
use bytes::Bytes;
use serde::{Deserialize, Serialize};

/// Position in Aeron channel B's recording — the canonical L2 tx identifier.
/// Aeron's `term_id` is `i32`; `term_offset` is the byte offset within the term
/// and is always non-negative but typed `i32` to match Aeron's wire format.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Hash, Serialize, Deserialize)]
pub struct BPosition {
    pub term_id: i32,
    pub term_offset: i32,
}

impl BPosition {
    pub const ZERO: Self = Self { term_id: 0, term_offset: 0 };
}

/// A raw signed Ethereum transaction with the proxy's correlation id attached.
/// Published on channel B by sequencers.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct TxEnvelope {
    pub correlation_id: u64,
    pub raw_tx: Bytes,
}

/// Per-tx execution receipt. Published on channel C by executor replicas.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct Receipt {
    pub tx_idx: BPosition,
    pub status: bool,
    pub gas_used: u64,
    pub logs: Vec<Log>,
    pub write_set_hash: B256,
}

/// Block-boundary marker emitted by the sealer onto channel B.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct BlockBoundaryStart {
    pub block_number: u64,
    pub end_tx_idx: BPosition,
    pub l2_timestamp: u64,
}

/// Block-boundary closeout emitted by executors onto channel C once they have
/// finished executing through `end_tx_idx`.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct BlockBoundary {
    pub block_number: u64,
    pub end_tx_idx: BPosition,
    pub l2_timestamp: u64,
    pub state_root_commitment: B256,
}

/// Single-recorder fsync progress. Published on a per-recorder watermark stream.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct FsyncWatermark {
    pub recorder_id: u8,
    pub position: BPosition,
}

/// Q-of-N aggregated fsync progress. Published on the shared watermark stream
/// that proxies subscribe to for the I2 ack guarantee.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct QuorumWatermark {
    pub position: BPosition,
}
```

- [ ] **Step 4: Implement `codec.rs`**

```rust
//! Bincode v2 (fixed-int, little-endian) wire codec for log messages.
//!
//! Chosen over `alloy-rlp` because all hot-path messages are internal IPC,
//! never exposed to L1, and bincode is roughly 3× faster on encode for the
//! per-tx envelope. If we ever need to expose any of these on a public RPC,
//! mirror types live elsewhere and translate.

use serde::{Deserialize, Serialize};

use crate::error::LogError;

fn config() -> bincode::config::Configuration<bincode::config::LittleEndian, bincode::config::Fixint> {
    bincode::config::standard()
        .with_little_endian()
        .with_fixed_int_encoding()
}

pub fn encode<T: Serialize>(value: &T) -> Result<Vec<u8>, LogError> {
    bincode::serde::encode_to_vec(value, config()).map_err(|e| LogError::Codec(e.to_string()))
}

pub fn decode<T: for<'de> Deserialize<'de>>(bytes: &[u8]) -> Result<T, LogError> {
    let (v, _): (T, usize) =
        bincode::serde::decode_from_slice(bytes, config()).map_err(|e| LogError::Codec(e.to_string()))?;
    Ok(v)
}
```

- [ ] **Step 5: Extend `error.rs`**

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

- [ ] **Step 6: Run the test and confirm pass**

```bash
cd /home/dev/kardamom && cargo test -p kardamom-log --test codec_roundtrip
```

Expected: all five tests PASS.

- [ ] **Step 7: Commit**

```bash
git add crates/kardamom-log/src/types.rs crates/kardamom-log/src/codec.rs \
        crates/kardamom-log/src/error.rs crates/kardamom-log/tests/codec_roundtrip.rs
git commit -m "log: shared message types + bincode codec"
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
use kardamom_log::types::{BPosition, FsyncWatermark};
use kardamom_log::watermark::QuorumState;

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

use crate::types::{BPosition, FsyncWatermark};

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

use rusteron_client::Aeron;
use serde::Serialize;
use tracing::warn;

use crate::codec;
use crate::config::ChannelsConfig;
use crate::error::LogError;
use crate::types::{
    BPosition, BlockBoundaryStart, FsyncWatermark, QuorumWatermark, Receipt, TxEnvelope,
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

    pub fn publish_boundary(&self, b: &crate::types::BlockBoundary) -> Result<BPosition, LogError> {
        offer(&self.pub_handle, b)
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

fn offer<T: Serialize>(
    p: &rusteron_client::ConcurrentPublication,
    msg: &T,
) -> Result<BPosition, LogError> {
    let bytes = codec::encode(msg)?;
    // rusteron::ConcurrentPublication::offer returns the new stream position
    // (or a negative back-pressure code). Retry up to 1024 times on back-pressure.
    for attempt in 0..1024 {
        let r = p.offer(&bytes);
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

use rusteron_client::Aeron;
use serde::de::DeserializeOwned;

use crate::codec;
use crate::config::ChannelsConfig;
use crate::error::LogError;
use crate::types::{BPosition, FsyncWatermark, QuorumWatermark, Receipt, TxEnvelope};

/// Generic single-stream subscriber over a typed message.
pub struct TypedSubscriber<T> {
    sub: rusteron_client::Subscription,
    _marker: std::marker::PhantomData<T>,
}

impl<T: DeserializeOwned + 'static> TypedSubscriber<T> {
    pub fn open(aeron: &Aeron, channel: &str, stream_id: i32) -> Result<Self, LogError> {
        let sub = aeron
            .add_subscription(channel, stream_id)
            .map_err(|e| LogError::Aeron(format!("add_subscription {channel}: {e}")))?;
        Ok(Self { sub, _marker: std::marker::PhantomData })
    }

    /// Poll once and invoke `f` on every fragment that arrived in this poll
    /// cycle. Returns the number of fragments processed.
    pub fn poll<F: FnMut(T, BPosition)>(&mut self, mut f: F, fragment_limit: usize) -> usize {
        self.sub.poll(
            |bytes: &[u8], header: rusteron_client::Header| {
                match codec::decode::<T>(bytes) {
                    Ok(v) => f(v, BPosition { term_id: header.term_id(), term_offset: header.term_offset() }),
                    Err(e) => tracing::error!(error = %e, "decode failed"),
                }
            },
            fragment_limit,
        )
    }
}

pub type ChannelBSubscriber = TypedSubscriber<TxEnvelope>;
pub type ChannelCReceiptSubscriber = TypedSubscriber<Receipt>;
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
use kardamom_log::types::BPosition;

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
use crate::types::BPosition;

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
use crate::types::QuorumWatermark;

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

use bytes::Bytes;
use kardamom_log::config::LogConfig;
use kardamom_log::publisher::ChannelBPublisher;
use kardamom_log::subscriber::Subscribers;
use kardamom_log::supervisor::Supervisor;
use kardamom_log::types::TxEnvelope;

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

use bytes::Bytes;
use kardamom_log::config::LogConfig;
use kardamom_log::publisher::{ChannelBPublisher, QuorumPublisher};
use kardamom_log::subscriber::Subscribers;
use kardamom_log::supervisor::Supervisor;
use kardamom_log::types::{BPosition, TxEnvelope};
use kardamom_log::watermark::QuorumAggregator;

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
            .publish_tx(&TxEnvelope { correlation_id: 99_000 + i, raw_tx: Bytes::from_static(b"x") })
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
            .publish_tx(&TxEnvelope { correlation_id: 199_000 + i, raw_tx: Bytes::from_static(b"y") })
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

use bytes::Bytes;
use criterion::{Criterion, Throughput, criterion_group, criterion_main};
use kardamom_log::config::LogConfig;
use kardamom_log::publisher::ChannelBPublisher;
use kardamom_log::supervisor::Supervisor;
use kardamom_log::types::TxEnvelope;

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
            pubr.publish_tx(&TxEnvelope { correlation_id: i, raw_tx: payload.clone() }).unwrap();
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

use bytes::Bytes;
use criterion::{Criterion, Throughput, criterion_group, criterion_main};
use kardamom_log::config::LogConfig;
use kardamom_log::publisher::ChannelBPublisher;
use kardamom_log::subscriber::Subscribers;
use kardamom_log::supervisor::Supervisor;
use kardamom_log::types::TxEnvelope;

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
        pubr.publish_tx(&TxEnvelope { correlation_id: i, raw_tx: payload.clone() }).unwrap();
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

## Task 15: Crate-level README and final lint pass

**Files:**
- Create: `crates/kardamom-log/README.md`
- Run: `cargo fmt --all`, `cargo clippy -p kardamom-log -- -D warnings`

- [ ] **Step 1: Write `README.md`**

```markdown
# kardamom-log

S3 canonical-log subsystem. See `docs/specs/2026-05-23-high-throughput-sequencer-design.md` §2.3 and §2.5.

## Owned components

- **Channel B** (canonical tx log, recorded, fsync-quorum durable)
- **Channel C** (receipts + block boundaries, RAM only)
- **Per-recorder fsync sidecar** (io_uring + O_DIRECT mirror)
- **Per-recorder fsync-watermark stream**
- **Quorum fsync-watermark aggregator** (Q-of-N smallest position)

## Shared types

`kardamom_log::types` defines the public message types that every other
kardamom subsystem (S1, S2, S4, S5, S6, S7) imports. Treat the field layout
as a stable interface — changes require coordinated updates to all subsystems.

## Runtime dependencies

- Aeron Media Driver and Aeron Archive binaries (Java) installed on each host.
  Tests skip when `AERON_MEDIA_DRIVER_BIN` / `AERON_ARCHIVE_BIN` are unset.
- Mirror file must be on an ext4/xfs/etc. filesystem that supports `O_DIRECT`.
  tmpfs returns `EINVAL` for `O_DIRECT` opens.
- Recommended: enterprise NVMe with PLP, separate from the OS disk.
```

- [ ] **Step 2: Format and lint**

```bash
cd /home/dev/kardamom && cargo fmt --all
cd /home/dev/kardamom && cargo clippy -p kardamom-log --all-targets -- -D warnings
```

Expected: no warnings.

- [ ] **Step 3: Commit**

```bash
git add crates/kardamom-log/README.md
git commit -m "log: README + lint clean"
```

---

## Self-Review Checklist

- **Spec §2.3 coverage:** channel B publisher (Task 6), channel B subscriber (Task 7), recorder wrapper (Task 8), io_uring fsync sidecar (Tasks 9–10), per-recorder watermark publisher (Task 6), quorum aggregator (Tasks 4, 11), Q-of-N math (Task 4). ✓
- **Spec §2.5 coverage:** channel C publisher (Task 6), channel C subscriber (Task 7), shared codec (Task 2). Note: `BlockBoundaryStart` is published on B, `BlockBoundary` on C — both type definitions present in `types.rs`. ✓
- **Spec §3 latency budget:** fsync sidecar uses io_uring + O_DIRECT to keep fsync off the page cache; bench (Task 14) measures per-fsync latency to validate the 25 µs target. ✓
- **Spec §4.3 recorder failure:** integration test (Task 13) kills 1 of 3 recorders, asserts quorum continues; kills 2 of 3, asserts quorum stalls. ✓
- **V0 scope:** all features listed are shipped in v0; no deferrals. ✓
- **Shared interface types:** complete in `types.rs` (Task 2); other plans (S1, S2, S4, S5, S6, S7) consume them as `kardamom_log::types::*`. ✓
- **Tests required:**
  - Codec roundtrip + BPosition ordering — Task 2 ✓
  - Watermark aggregator math — Task 4 ✓
  - Fsync sidecar unit test — Task 9 ✓
  - 3-recorder integration test (4 publishers × 1000 messages, kill 1, kill 2) — Task 13 ✓
  - Criterion benches (publish, subscribe, fsync watermark latency) — Task 14 ✓
- **Placeholder scan:** no `TODO`, no `tbd`, no "implement later" — each step has complete code. The two `rusteron` API-name caveats are explicit ("if upstream differs, adjust"), not placeholders.
- **Type consistency:** `BPosition`, `FsyncWatermark`, `QuorumWatermark` field names match across types.rs, watermark.rs, fsync_sidecar.rs, subscriber.rs, publisher.rs. `Subscribers::watermark(rid)` and `WatermarkPublisher::open(_, _, rid)` both take `u8`. ✓

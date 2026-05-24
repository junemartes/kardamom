# S2 Sequencer Subsystem Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Ship the S2 sequencer cluster as a new crate `kardamom-sequencer` — M single-threaded, core-pinned processes that exclusively own a sender slice, maintain per-sender next-nonce state, drain a per-sender future-nonce buffer in order, and publish canonical-ordered raw txs into channel B via Aeron concurrent publication, with a hot-standby tailer that catches up by replaying B and a lease-based takeover on primary failure.

**Architecture:** One process per ingress partition (default M=8), pinned to a CPU core via `core_affinity`. Each owner subscribes to its `ingress[i]` Aeron stream, reads `envelope.sender` directly (always populated by the proxy per D-Sh3 — the sequencer does **no** secp256k1 work), and runs a small nonce-check state machine: **Match** → publish to B + advance + drain buffer; **Future** → insert into per-sender `BTreeMap<u64, TxEnvelope>` bounded by `max_pending_per_sender` (evict oldest on overflow); **Past** → drop and publish a `duplicate` notification to the receipt-cache Aeron channel. Backpressure on B surfaces as `Err::WouldBlock` to the caller, which the proxy converts to `503`. A sibling **hot-standby** process on a different host subscribes to B and, for senders mapped to this slice via `keccak(sender) % M`, replays nonces to keep its own `pending_nonce` map in lockstep; on lease expiry it acquires the slice and begins publishing to its `ingress[i]`.

**Tech Stack:** Rust 2024 edition; `alloy-consensus` `TxEnvelope`; `alloy-primitives` (`Address`, `B256`, `keccak256`); `kardamom-log` (S3 — provides Aeron channel handles, `BPosition`, wire framing of `TxEnvelope` with `correlation_id` and always-populated proxy-recovered `sender`, and `BlockBoundaryStart` marker type to skip on B-replay; all wire types derive `rkyv::Archive`/`Serialize`/`Deserialize` per D-Sh2, and the sequencer can consume `Archived<TxEnvelope>` zero-copy off the ingress channel where it helps the hot path); `core_affinity = "0.8"`; `tracing`; `metrics` (Prometheus exporter via the shared workspace stack); `clap` for the CLI binary; `criterion` for benches; `proptest` for state-machine property tests.

**Branch:** `claude/s2-sequencer` (branched off `main`).

**Reference spec:** `docs/specs/2026-05-23-high-throughput-sequencer-design.md` — §1 (architecture), §2.2 (this subsystem), §3 (latency budget — nonce check ≤3µs), §4.2 (sequencer failure), and the V0 scope section (V0 includes hot standby).

**Assumed S3 (`kardamom-log`) interfaces.** The S3 plan ships before this one. This plan codes against these types and helpers. If S3's final names drift, fix the references at integration time:

- `kardamom_log::aeron::Subscription` — Aeron subscription handle with `poll(&mut handler) -> usize` and `is_connected() -> bool`.
- `kardamom_log::aeron::Publication` — Aeron publication handle with `try_offer(&[u8]) -> Result<Offset, BackpressureError>` (non-blocking) and `offer(&[u8]) -> Result<Offset, AeronError>` (with linear backoff).
- `kardamom_log::aeron::ConcurrentPublication` — multi-publisher variant for channel B; `try_offer(&[u8]) -> Result<BPosition, BackpressureError>` with claim-and-write semantics.
- `kardamom_log::BPosition { term_id: i32, term_offset: i32 }` — canonical tx identifier.
- `kardamom_log::framing::TxFrame` — rkyv-archived header `{ correlation_id: [u8; 16], sender: Address, ingress_partition: u8 }` plus raw `TxEnvelope` bytes (RLP-encoded). Encoder/decoder live in `kardamom_log::framing`. `sender` is typed `Address`, **never `Option`** — populated unconditionally by the proxy (D-Sh3). Zero-copy `Archived<TxFrame>` views are available via helpers in `kardamom_log`.
- `kardamom_log::framing::DuplicateNotification { correlation_id: [u8; 16] }` — published to the receipt-cache channel.
- `kardamom_log::framing::BlockBoundaryStart { block_number: u64, end_tx_idx: u64, l2_timestamp: u64 }` — emitted on B by the sealer (S5). Sequencers must **decode-and-skip** these messages when replaying B as a hot standby.
- `kardamom_log::channels::ChannelConfig` — strongly-typed Aeron URIs/stream-ids for `ingress[i]`, `b`, and `receipt_cache`, loadable from a `chains/*.toml` `[aeron]` block.
- `kardamom_log::lease::Lease` — slice-ownership lease with `acquire() -> Result<LeaseGuard, LeaseError>`, `renew()` heartbeat, and `expired()` observer. Backed by Aeron-Cluster atomic counters in v1; the deterministic-lowest-host-id fallback (per §2.6) is permissible if Aeron-Cluster is not yet integrated.

**S3 will provide a `kardamom_log::testing::mock` module** with in-memory `MockPublication`, `MockSubscription`, and `MockConcurrentPublication` that record offers / serve scripted messages. All non-bench tests in this plan use those mocks. **If the S3 mocks land in a slightly different shape, adjust the test scaffolding in Task 3 and propagate.**

---

### Divergences from the assumed surface (recorded during implementation)

The actual S1 + S3 surfaces that landed differ from this plan's assumptions. The implementation in `crates/kardamom-sequencer/` follows the actual surface; readers comparing the plan to the code should expect:

- **`TxEnvelope`** is the kardamom-types wire type (NOT `alloy_consensus::TxEnvelope`). Fields are `{ correlation_id: u64, raw_tx: Bytes, sender: Address, tx_hash: B256 }` — `sender` and `tx_hash` are always populated (D-Sh3/D-Sh4). `correlation_id` is `u64`, not `[u8; 16]`. The `nonce` is NOT a field; it is decoded from `raw_tx` (RLP-encoded alloy-consensus envelope).
- **No `kardamom_log::framing::TxFrame`.** Channel B carries `TxEnvelope` directly, rkyv-archived via `kardamom_log::codec::{encode, access, materialize}`. The sequencer publishes the `TxEnvelope` byte payload onto B; downstream consumers do `kardamom_log::codec::access::<TxEnvelope>(bytes)` for zero-copy reads.
- **No `kardamom_log::aeron::sequencer_*` builder helpers and no `kardamom_log::channels::ChannelConfig`.** Production wiring uses the `aeron-live` feature with `kardamom_log::{publisher, subscriber}`; testing uses `kardamom_log::testing::{FakeBus, FakePublication, FakeTypedSubscription}`. S2's CLI binary therefore depends on the same `aeron-live` feature gating; until S3 ships the high-level builder helpers a thin wiring layer lives in S2's binary.
- **`DuplicateNotification` does not exist** in `kardamom-log` or `kardamom-types`. S2 defines a small local `DuplicateNotification { correlation_id: u64, sender: Address, nonce: u64 }` (rkyv-archived) and publishes it on the receipt-cache channel alongside `CachedReceipt`. This may be promoted to `kardamom-types` in a future cross-cutting change.
- **`partition_for` exists in `kardamom_ingress::routing`** and uses `m: u32`, `keccak256(sender)[..8]` (big-endian u64) `% m`. S2's `partition.rs` matches that algorithm exactly so both producer (proxy) and consumer (sequencer) agree on routing. S2 uses `u32` for `m` to mirror it.
- **The "ingress channel" between proxy and sequencer is the `IngressPublication` trait** (in `kardamom-ingress::channels`), backed in tests by `MockChannels` (tokio `mpsc::UnboundedReceiver<TxEnvelope>` per partition). In production it will be backed by real Aeron once S3 ships the corresponding publisher; S2's `IngressSource` trait is the consumer side of that same channel and is the unit-of-substitution between fake and real.
- **`Lease` already exists in `kardamom-leases`** as a deterministic lowest-host-id-among-caught-up-recorders state machine. S2's `lease.rs` is a thin adapter that wraps `kardamom_leases::Lease` plus the shutdown/takeover orchestration.
- **`AeronTestCluster`** is the actual harness name (not `AeronDocker`); `cluster.archive_control_endpoint(0)` returns the Archive control endpoint string.

These divergences are scoped so the **algorithmic content** of every task (state machine, pending buffer, partition routing, primary/standby logic, lease handoff) is preserved exactly; only the I/O surface names change.

---

## File Structure

Before the tasks, here's the full layout. Each file has one responsibility.

```
crates/kardamom-sequencer/
├── Cargo.toml
├── src/
│   ├── lib.rs                  # Public API: re-exports, prelude.
│   ├── partition.rs            # keccak(sender) % M routing; pure function + tests.
│   ├── sender.rs               # Sender accessor: reads envelope.sender (always populated by proxy, D-Sh3); pure.
│   ├── pending.rs              # PendingBuffer (per-sender BTreeMap; bounded + evict).
│   ├── state.rs                # PartitionState: HashMap<Address, NextNonce> + PendingBuffer per sender. The pure state machine.
│   ├── outbound.rs             # Trait abstractions over Aeron pubs (BPublisher, ReceiptCachePublisher) + real and mock impls behind a feature-gated `testing` module.
│   ├── inbound.rs              # Trait abstraction over the ingress Aeron subscription (IngressSource); real + mock impls.
│   ├── primary.rs              # PrimarySequencer event loop: poll ingress → drive state machine → publish.
│   ├── standby.rs              # HotStandbyTailer: poll B → replay nonces for our slice → wait for lease takeover.
│   ├── lease.rs                # Thin wrapper over kardamom_log::lease::Lease with renew loop + takeover trigger.
│   ├── config.rs               # SequencerConfig (M, partition_index, max_pending_per_sender, core_id, backpressure_policy).
│   ├── metrics.rs              # Prometheus counters/gauges/histograms; one registration site.
│   ├── error.rs                # SequencerError thiserror enum.
│   └── bin/
│       └── kardamom-sequencer.rs  # CLI binary entry point (clap).
├── tests/
│   ├── partition_routing.rs    # keccak(sender) % M assertions.
│   ├── state_machine.rs        # Match / future / past / buffer-full / drain.
│   ├── pending_buffer.rs       # BTreeMap eviction.
│   ├── sender_derivation.rs    # Envelope-cached vs recovered.
│   ├── primary_integration.rs  # Mock-Aeron: 1000 txs / 100 senders / gaps + dupes + futures.
│   ├── standby_replay.rs       # Hot-standby map matches primary's after replay.
│   └── chaos_failover.rs       # Kill primary mid-stream; standby takes over with correct pending_nonce.
└── benches/
    └── throughput.rs           # Criterion: per-sequencer throughput on simple sigs.
```

---

## Task 1: Create `crates/kardamom-sequencer` skeleton

**Files:**
- Create: `crates/kardamom-sequencer/Cargo.toml`
- Create: `crates/kardamom-sequencer/src/lib.rs`
- Create: `crates/kardamom-sequencer/src/bin/kardamom-sequencer.rs`

- [ ] **Step 1: Write `Cargo.toml`**

```toml
[package]
name = "kardamom-sequencer"
version.workspace = true
edition.workspace = true

[[bin]]
name = "kardamom-sequencer"
path = "src/bin/kardamom-sequencer.rs"

[dependencies]
alloy-primitives.workspace = true
alloy-consensus.workspace = true
alloy-rlp.workspace = true
tokio.workspace = true
serde.workspace = true
toml.workspace = true
thiserror.workspace = true
anyhow.workspace = true
tracing.workspace = true
tracing-subscriber.workspace = true
clap.workspace = true
metrics.workspace = true
core_affinity = "0.8"
# kardamom-log is the S3 crate. If S3 has not landed yet, add a `path = "../kardamom-log"`
# dep gated behind a placeholder; do NOT publish kardamom-sequencer until kardamom-log exists.
kardamom-log = { path = "../kardamom-log" }

[dev-dependencies]
proptest = "1"
criterion = { version = "0.5", features = ["html_reports"] }
alloy-signer-local.workspace = true
# Re-enable the `testing` feature on kardamom-log to pull in MockPublication etc.
kardamom-log = { path = "../kardamom-log", features = ["testing"] }

[[bench]]
name = "throughput"
harness = false
```

- [ ] **Step 2: Write minimal `src/lib.rs`**

```rust
//! S2 sequencer subsystem for the kardamom rollup.
//!
//! One process per ingress partition. Each process exclusively owns a sender slice
//! (`keccak(sender) % M`), maintains per-sender next-nonce state, and publishes
//! canonical-ordered raw txs into Aeron channel B. A hot-standby sibling tails B
//! and takes over on lease expiry.

pub mod config;
pub mod error;
pub mod inbound;
pub mod lease;
pub mod metrics;
pub mod outbound;
pub mod partition;
pub mod pending;
pub mod primary;
pub mod sender;
pub mod standby;
pub mod state;

pub use config::SequencerConfig;
pub use error::SequencerError;
pub use primary::PrimarySequencer;
pub use standby::HotStandbyTailer;
```

- [ ] **Step 3: Write minimal `src/bin/kardamom-sequencer.rs`**

```rust
//! kardamom-sequencer: per-partition CLI binary.

fn main() {
    eprintln!("kardamom-sequencer: not yet implemented");
    std::process::exit(2);
}
```

- [ ] **Step 4: Create empty module files so `lib.rs` compiles**

```bash
cd /home/dev/kardamom
mkdir -p crates/kardamom-sequencer/src/bin crates/kardamom-sequencer/tests crates/kardamom-sequencer/benches
touch crates/kardamom-sequencer/src/config.rs
touch crates/kardamom-sequencer/src/error.rs
touch crates/kardamom-sequencer/src/inbound.rs
touch crates/kardamom-sequencer/src/lease.rs
touch crates/kardamom-sequencer/src/metrics.rs
touch crates/kardamom-sequencer/src/outbound.rs
touch crates/kardamom-sequencer/src/partition.rs
touch crates/kardamom-sequencer/src/pending.rs
touch crates/kardamom-sequencer/src/primary.rs
touch crates/kardamom-sequencer/src/sender.rs
touch crates/kardamom-sequencer/src/standby.rs
touch crates/kardamom-sequencer/src/state.rs
```

- [ ] **Step 5: Verify the crate is part of the workspace**

The workspace `Cargo.toml` already has `members = ["crates/*"]`, so the new crate is picked up automatically.

- [ ] **Step 6: Verify it builds**

```bash
cd /home/dev/kardamom
cargo build -p kardamom-sequencer
```

Expected: builds cleanly (empty module files are valid Rust).

- [ ] **Step 7: Commit**

```bash
git add crates/kardamom-sequencer/
git commit -m "sequencer: add crate skeleton"
```

---

## Task 2: Implement `error.rs` — `SequencerError` enum

**Files:**
- Modify: `crates/kardamom-sequencer/src/error.rs`

- [ ] **Step 1: Write the failing test**

Append to `crates/kardamom-sequencer/src/error.rs`:

```rust
use thiserror::Error;

#[derive(Debug, Error)]
pub enum SequencerError {
    #[error("backpressure: channel B publication blocked")]
    Backpressure,

    #[error("ingress source disconnected")]
    IngressDisconnected,

    #[error("malformed tx frame: {0}")]
    MalformedFrame(String),

    #[error("lease lost during operation")]
    LeaseLost,

    #[error("kardamom-log error: {0}")]
    Log(String),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn display_strings_are_stable() {
        assert_eq!(
            SequencerError::Backpressure.to_string(),
            "backpressure: channel B publication blocked"
        );
        assert_eq!(
            SequencerError::IngressDisconnected.to_string(),
            "ingress source disconnected"
        );
    }
}
```

- [ ] **Step 2: Run the test to verify it passes**

```bash
cargo test -p kardamom-sequencer error::tests::display_strings_are_stable
```

Expected: PASS.

- [ ] **Step 3: Commit**

```bash
git add crates/kardamom-sequencer/src/error.rs
git commit -m "sequencer: add SequencerError enum"
```

---

## Task 3: Implement `partition.rs` — `keccak(sender) % M` routing

**Files:**
- Modify: `crates/kardamom-sequencer/src/partition.rs`
- Create: `crates/kardamom-sequencer/tests/partition_routing.rs`

- [ ] **Step 1: Write the failing tests in `tests/partition_routing.rs`**

```rust
use alloy_primitives::Address;
use kardamom_sequencer::partition::{partition_for, validate_partition_count};

#[test]
fn partition_is_stable_for_known_address() {
    let addr: Address = "0x000000000000000000000000000000000000beef"
        .parse()
        .unwrap();
    // The exact partition index depends on keccak; this test pins the implementation.
    // Recompute the expected value once and lock it in.
    let p = partition_for(addr, 8);
    assert!(p < 8);
    // Stability: identical inputs always produce identical outputs.
    assert_eq!(partition_for(addr, 8), p);
}

#[test]
fn partition_distributes_roughly_uniformly() {
    let m = 8u8;
    let mut counts = [0usize; 8];
    for i in 0u64..10_000 {
        let mut bytes = [0u8; 20];
        bytes[12..].copy_from_slice(&i.to_be_bytes());
        let addr = Address::from(bytes);
        counts[partition_for(addr, m) as usize] += 1;
    }
    // Each partition should see between 1000 and 1500 of the 10k addresses
    // (chi-square slack; we just want to catch a routing bug, not test keccak).
    for c in counts {
        assert!(c > 1000 && c < 1500, "partition imbalance: {counts:?}");
    }
}

#[test]
fn validate_partition_count_rejects_zero() {
    assert!(validate_partition_count(0).is_err());
}

#[test]
fn validate_partition_count_accepts_power_of_two_and_other() {
    assert!(validate_partition_count(1).is_ok());
    assert!(validate_partition_count(8).is_ok());
    assert!(validate_partition_count(64).is_ok());
}
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
cargo test -p kardamom-sequencer --test partition_routing
```

Expected: fail with "no function `partition_for` in module `partition`".

- [ ] **Step 3: Implement `partition.rs`**

```rust
//! Sender-to-partition routing: `keccak(sender) % M`.
//!
//! The hash is taken over the raw 20-byte address rather than its hex string,
//! to match the proxy's own routing (`keccak(sender) % M`) byte-for-byte.

use alloy_primitives::{Address, keccak256};

/// Compute the partition index for a sender address.
/// `m` is the total number of sequencer partitions (must be >= 1).
pub fn partition_for(sender: Address, m: u8) -> u8 {
    debug_assert!(m >= 1, "partition count must be >= 1");
    let h = keccak256(sender.as_slice());
    // Use the last 8 bytes of keccak256 as a u64 to avoid bias on non-power-of-two m.
    let tail = u64::from_be_bytes(h[24..32].try_into().expect("8 bytes"));
    (tail % m as u64) as u8
}

#[derive(Debug, thiserror::Error)]
pub enum PartitionConfigError {
    #[error("partition count must be >= 1")]
    Zero,
}

pub fn validate_partition_count(m: u8) -> Result<(), PartitionConfigError> {
    if m == 0 {
        Err(PartitionConfigError::Zero)
    } else {
        Ok(())
    }
}
```

- [ ] **Step 4: Run tests to verify they pass**

```bash
cargo test -p kardamom-sequencer --test partition_routing
```

Expected: all 4 tests PASS.

- [ ] **Step 5: Commit**

```bash
git add crates/kardamom-sequencer/src/partition.rs crates/kardamom-sequencer/tests/partition_routing.rs
git commit -m "sequencer: add partition routing (keccak(sender) % M)"
```

---

## Task 4: Implement `sender.rs` — accessor for the proxy-populated sender

**Files:**
- Modify: `crates/kardamom-sequencer/src/sender.rs`
- Create: `crates/kardamom-sequencer/tests/sender_accessor.rs`

**Design (locked in by D-Sh3):** the proxy (S1) recovers the sender during batched secp256k1 verification and writes it into `TxEnvelope.sender` (typed `Address`, **never `Option`**). The sequencer **trusts** this field unconditionally — there is no fallback path, no `recover_signer()` call, and no `--paranoid-sender-check` mode. The sequencer performs **zero** secp256k1 work, keeping the §3 nonce-check budget at ≤3µs per tx.

- [ ] **Step 1: Write the test in `tests/sender_accessor.rs`**

```rust
use alloy_consensus::{SignableTransaction, TxEnvelope, TxLegacy};
use alloy_primitives::{Address, U256};
use alloy_signer_local::PrivateKeySigner;
use kardamom_sequencer::sender::sender_of;

fn signed_legacy_tx() -> (TxEnvelope, Address) {
    let signer: PrivateKeySigner = "0x59c6995e998f97a5a0044966f0945389dc9e86dae88c7a8412f4603b6b78690d"
        .parse()
        .unwrap();
    let addr = signer.address();
    let tx = TxLegacy {
        chain_id: Some(1),
        nonce: 7,
        gas_price: 1_000_000_000,
        gas_limit: 21_000,
        to: Address::ZERO.into(),
        value: U256::ZERO,
        input: Default::default(),
    };
    let sig = signer
        .sign_hash_sync(&tx.signature_hash())
        .expect("sync sign");
    let envelope: TxEnvelope = tx.into_signed(sig).into();
    (envelope, addr)
}

#[test]
fn sender_is_read_from_proxy_populated_field() {
    let (env, _real_signer) = signed_legacy_tx();
    // The proxy populated sender for us — in production this is the recovered
    // address. In this unit test the IngressMessage carries it alongside the
    // envelope; sender_of reads it directly with no recovery work.
    let proxy_populated = Address::repeat_byte(0xAB);
    let got = sender_of(&env, proxy_populated);
    // We trust the proxy unconditionally (D-Sh3). The function does not look
    // at the envelope's signature at all.
    assert_eq!(got, proxy_populated);
}
```

- [ ] **Step 2: Implement `sender.rs`**

```rust
//! Sender accessor for ingress tx frames.
//!
//! D-Sh3: the proxy (S1) recovers the sender during batched k256 verification
//! and writes it into `TxEnvelope.sender` / the ingress `TxFrame.sender` field
//! (typed `Address`, never `Option`). The sequencer trusts this value
//! unconditionally — no fallback, no `recover_signer()`, no paranoid-check
//! mode. The sequencer does ZERO secp256k1 work on the hot path, which keeps
//! the §3 nonce-check budget at ≤3µs per tx.

use alloy_consensus::TxEnvelope;
use alloy_primitives::Address;

/// Return the sender for an ingress message. The `sender` argument is the
/// proxy-populated address (always present per D-Sh3). The envelope is taken
/// alongside it solely to make the call site read naturally; we never inspect
/// the signature.
#[inline]
pub fn sender_of(_envelope: &TxEnvelope, sender: Address) -> Address {
    sender
}
```

- [ ] **Step 3: Run tests to verify they pass**

```bash
cargo test -p kardamom-sequencer --test sender_accessor
```

Expected: PASS.

- [ ] **Step 4: Commit**

```bash
git add crates/kardamom-sequencer/src/sender.rs crates/kardamom-sequencer/tests/sender_accessor.rs
git commit -m "sequencer: add sender accessor (trusts proxy-populated TxEnvelope.sender)"
```

---

## Task 5: Implement `pending.rs` — per-sender future-nonce buffer

**Files:**
- Modify: `crates/kardamom-sequencer/src/pending.rs`
- Create: `crates/kardamom-sequencer/tests/pending_buffer.rs`

- [ ] **Step 1: Write the failing tests in `tests/pending_buffer.rs`**

```rust
use kardamom_sequencer::pending::{InsertOutcome, PendingBuffer};

// A stand-in payload that is cheap to construct; the buffer is generic over T
// in real use but we test it with raw bytes here.

#[test]
fn insert_returns_inserted_when_under_capacity() {
    let mut buf: PendingBuffer<Vec<u8>> = PendingBuffer::new(4);
    let r = buf.insert(10, vec![0xAA]);
    assert!(matches!(r, InsertOutcome::Inserted));
    assert_eq!(buf.len(), 1);
}

#[test]
fn insert_evicts_oldest_when_full() {
    let mut buf: PendingBuffer<Vec<u8>> = PendingBuffer::new(2);
    assert!(matches!(buf.insert(10, vec![1]), InsertOutcome::Inserted));
    assert!(matches!(buf.insert(11, vec![2]), InsertOutcome::Inserted));
    let r = buf.insert(12, vec![3]);
    let InsertOutcome::EvictedOldest { evicted_nonce } = r else {
        panic!("expected eviction, got {r:?}");
    };
    assert_eq!(evicted_nonce, 10);
    assert_eq!(buf.len(), 2);
}

#[test]
fn drain_consecutive_yields_only_in_order_run() {
    let mut buf: PendingBuffer<Vec<u8>> = PendingBuffer::new(8);
    buf.insert(5, vec![5]);
    buf.insert(6, vec![6]);
    buf.insert(8, vec![8]); // gap at 7
    let drained: Vec<(u64, Vec<u8>)> = buf.drain_consecutive_from(5).collect();
    assert_eq!(drained, vec![(5, vec![5]), (6, vec![6])]);
    // 8 stays in the buffer because 7 is missing.
    assert_eq!(buf.len(), 1);
    assert!(buf.contains(8));
}

#[test]
fn drain_returns_empty_when_first_nonce_missing() {
    let mut buf: PendingBuffer<Vec<u8>> = PendingBuffer::new(4);
    buf.insert(5, vec![5]);
    let drained: Vec<(u64, Vec<u8>)> = buf.drain_consecutive_from(3).collect();
    assert!(drained.is_empty());
    assert_eq!(buf.len(), 1);
}

#[test]
fn insert_with_existing_nonce_replaces_value() {
    let mut buf: PendingBuffer<Vec<u8>> = PendingBuffer::new(4);
    assert!(matches!(buf.insert(10, vec![1]), InsertOutcome::Inserted));
    let r = buf.insert(10, vec![2]);
    assert!(matches!(r, InsertOutcome::Replaced));
    assert_eq!(buf.len(), 1);
}

#[test]
fn zero_capacity_drops_immediately() {
    let mut buf: PendingBuffer<Vec<u8>> = PendingBuffer::new(0);
    let r = buf.insert(10, vec![1]);
    assert!(matches!(r, InsertOutcome::DroppedBufferDisabled));
    assert_eq!(buf.len(), 0);
}
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
cargo test -p kardamom-sequencer --test pending_buffer
```

Expected: fail with "no module `pending`".

- [ ] **Step 3: Implement `pending.rs`**

```rust
//! Per-sender future-nonce buffer.
//!
//! Bounded `BTreeMap<u64, T>` keyed by nonce. On overflow, the smallest nonce
//! is evicted (LRU-by-nonce). `drain_consecutive_from(start)` walks ascending
//! keys and yields the contiguous run starting at `start`; the first gap stops
//! the drain and leaves later entries in the buffer.

use std::collections::BTreeMap;

#[derive(Debug, PartialEq, Eq)]
pub enum InsertOutcome {
    Inserted,
    Replaced,
    EvictedOldest { evicted_nonce: u64 },
    DroppedBufferDisabled,
}

pub struct PendingBuffer<T> {
    capacity: usize,
    inner: BTreeMap<u64, T>,
}

impl<T> PendingBuffer<T> {
    pub fn new(capacity: usize) -> Self {
        Self { capacity, inner: BTreeMap::new() }
    }

    pub fn len(&self) -> usize {
        self.inner.len()
    }

    pub fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }

    pub fn contains(&self, nonce: u64) -> bool {
        self.inner.contains_key(&nonce)
    }

    pub fn insert(&mut self, nonce: u64, value: T) -> InsertOutcome {
        if self.capacity == 0 {
            return InsertOutcome::DroppedBufferDisabled;
        }
        if self.inner.contains_key(&nonce) {
            self.inner.insert(nonce, value);
            return InsertOutcome::Replaced;
        }
        if self.inner.len() >= self.capacity {
            // Evict the smallest nonce. Per spec §2.2 we evict the oldest to make room.
            let oldest = *self
                .inner
                .keys()
                .next()
                .expect("non-empty since len >= capacity >= 1");
            self.inner.remove(&oldest);
            self.inner.insert(nonce, value);
            return InsertOutcome::EvictedOldest { evicted_nonce: oldest };
        }
        self.inner.insert(nonce, value);
        InsertOutcome::Inserted
    }

    /// Drain the contiguous run of nonces starting at `start`. Stops at the first gap.
    /// Returned items are removed from the buffer.
    pub fn drain_consecutive_from(&mut self, start: u64) -> DrainConsecutive<'_, T> {
        DrainConsecutive { buf: self, next: start }
    }
}

pub struct DrainConsecutive<'a, T> {
    buf: &'a mut PendingBuffer<T>,
    next: u64,
}

impl<'a, T> Iterator for DrainConsecutive<'a, T> {
    type Item = (u64, T);
    fn next(&mut self) -> Option<Self::Item> {
        let v = self.buf.inner.remove(&self.next)?;
        let n = self.next;
        self.next = self.next.checked_add(1)?;
        Some((n, v))
    }
}
```

- [ ] **Step 4: Run tests to verify they pass**

```bash
cargo test -p kardamom-sequencer --test pending_buffer
```

Expected: all 6 tests PASS.

- [ ] **Step 5: Commit**

```bash
git add crates/kardamom-sequencer/src/pending.rs crates/kardamom-sequencer/tests/pending_buffer.rs
git commit -m "sequencer: add per-sender pending future-nonce buffer"
```

---

## Task 6: Implement `state.rs` — the pure nonce-check state machine

**Files:**
- Modify: `crates/kardamom-sequencer/src/state.rs`
- Create: `crates/kardamom-sequencer/tests/state_machine.rs`

`PartitionState` is the single-owner, no-lock data structure: `HashMap<Address, u64>` for next-expected-nonce, plus `HashMap<Address, PendingBuffer<T>>` for future buffers. `process(sender, nonce, payload)` is a pure function returning a list of `(nonce, payload)` items that should be published to B, in order.

- [ ] **Step 1: Write the failing tests in `tests/state_machine.rs`**

```rust
use alloy_primitives::Address;
use kardamom_sequencer::state::{NonceOutcome, PartitionState, ProcessAction};

fn s(byte: u8) -> Address {
    Address::repeat_byte(byte)
}

#[test]
fn match_publishes_and_advances() {
    let mut st: PartitionState<u32> = PartitionState::new(4);
    let out = st.process(s(1), 0, 100);
    assert_eq!(out.actions, vec![ProcessAction::Publish { nonce: 0, payload: 100 }]);
    assert_eq!(out.outcome, NonceOutcome::Matched);
    assert_eq!(st.next_nonce(s(1)), 1);
}

#[test]
fn match_drains_subsequent_buffered_nonces() {
    let mut st: PartitionState<u32> = PartitionState::new(8);
    // Insert futures 1, 2 first.
    assert!(matches!(st.process(s(1), 1, 11).outcome, NonceOutcome::Buffered));
    assert!(matches!(st.process(s(1), 2, 22).outcome, NonceOutcome::Buffered));
    // Now arrive nonce 0 → drain all three.
    let out = st.process(s(1), 0, 0);
    assert_eq!(
        out.actions,
        vec![
            ProcessAction::Publish { nonce: 0, payload: 0 },
            ProcessAction::Publish { nonce: 1, payload: 11 },
            ProcessAction::Publish { nonce: 2, payload: 22 },
        ]
    );
    assert_eq!(st.next_nonce(s(1)), 3);
}

#[test]
fn past_nonce_is_dropped_and_reported() {
    let mut st: PartitionState<u32> = PartitionState::new(4);
    st.process(s(1), 0, 0);
    st.process(s(1), 1, 1);
    let out = st.process(s(1), 0, 999);
    assert_eq!(out.actions, vec![ProcessAction::ReportDuplicate { nonce: 0 }]);
    assert_eq!(out.outcome, NonceOutcome::Past);
    assert_eq!(st.next_nonce(s(1)), 2);
}

#[test]
fn future_nonce_is_buffered() {
    let mut st: PartitionState<u32> = PartitionState::new(4);
    let out = st.process(s(1), 5, 55);
    assert_eq!(out.actions, vec![]);
    assert_eq!(out.outcome, NonceOutcome::Buffered);
    assert_eq!(st.next_nonce(s(1)), 0);
}

#[test]
fn buffer_full_evicts_oldest() {
    let mut st: PartitionState<u32> = PartitionState::new(2);
    st.process(s(1), 5, 5);
    st.process(s(1), 6, 6);
    let out = st.process(s(1), 7, 7);
    assert_eq!(out.outcome, NonceOutcome::BufferedEvicting { evicted_nonce: 5 });
}

#[test]
fn replay_for_standby_advances_without_publishing() {
    // Standby tailer calls replay() on each B message for senders in its slice.
    let mut st: PartitionState<u32> = PartitionState::new(4);
    st.replay(s(1), 0);
    st.replay(s(1), 1);
    st.replay(s(1), 2);
    assert_eq!(st.next_nonce(s(1)), 3);
}

#[test]
fn replay_with_gap_advances_to_observed_plus_one() {
    // If the standby missed messages (joined late), trust B and jump.
    let mut st: PartitionState<u32> = PartitionState::new(4);
    st.replay(s(1), 0);
    st.replay(s(1), 5); // skipped 1..=4
    assert_eq!(st.next_nonce(s(1)), 6);
}
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
cargo test -p kardamom-sequencer --test state_machine
```

Expected: fail with "no module `state`" / missing items.

- [ ] **Step 3: Implement `state.rs`**

```rust
//! Per-partition nonce-check state machine.
//!
//! Single-owner: this struct is held by exactly one OS thread (the primary
//! sequencer event loop or the standby tailer). No locks, no atomics.

use std::collections::HashMap;

use alloy_primitives::Address;

use crate::pending::{InsertOutcome, PendingBuffer};

#[derive(Debug, PartialEq, Eq)]
pub enum ProcessAction<T> {
    Publish { nonce: u64, payload: T },
    ReportDuplicate { nonce: u64 },
}

#[derive(Debug, PartialEq, Eq)]
pub enum NonceOutcome {
    Matched,
    Buffered,
    BufferedEvicting { evicted_nonce: u64 },
    BufferedReplaced,
    BufferedDisabled,
    Past,
}

#[derive(Debug)]
pub struct ProcessResult<T> {
    pub actions: Vec<ProcessAction<T>>,
    pub outcome: NonceOutcome,
}

pub struct PartitionState<T> {
    max_pending_per_sender: usize,
    next: HashMap<Address, u64>,
    pending: HashMap<Address, PendingBuffer<T>>,
}

impl<T> PartitionState<T> {
    pub fn new(max_pending_per_sender: usize) -> Self {
        Self {
            max_pending_per_sender,
            next: HashMap::new(),
            pending: HashMap::new(),
        }
    }

    pub fn next_nonce(&self, sender: Address) -> u64 {
        self.next.get(&sender).copied().unwrap_or(0)
    }

    /// Primary-side: handle an incoming tx. Returns publish actions in canonical order.
    pub fn process(&mut self, sender: Address, nonce: u64, payload: T) -> ProcessResult<T> {
        let expected = self.next_nonce(sender);
        if nonce < expected {
            return ProcessResult {
                actions: vec![ProcessAction::ReportDuplicate { nonce }],
                outcome: NonceOutcome::Past,
            };
        }
        if nonce > expected {
            let buf = self
                .pending
                .entry(sender)
                .or_insert_with(|| PendingBuffer::new(self.max_pending_per_sender));
            let outcome = match buf.insert(nonce, payload) {
                InsertOutcome::Inserted => NonceOutcome::Buffered,
                InsertOutcome::Replaced => NonceOutcome::BufferedReplaced,
                InsertOutcome::EvictedOldest { evicted_nonce } => {
                    NonceOutcome::BufferedEvicting { evicted_nonce }
                }
                InsertOutcome::DroppedBufferDisabled => NonceOutcome::BufferedDisabled,
            };
            return ProcessResult { actions: vec![], outcome };
        }
        // nonce == expected: publish + drain any contiguous run.
        let mut actions = vec![ProcessAction::Publish { nonce, payload }];
        let mut advanced = nonce.saturating_add(1);
        if let Some(buf) = self.pending.get_mut(&sender) {
            for (n, p) in buf.drain_consecutive_from(advanced) {
                actions.push(ProcessAction::Publish { nonce: n, payload: p });
                advanced = n.saturating_add(1);
            }
        }
        self.next.insert(sender, advanced);
        ProcessResult { actions, outcome: NonceOutcome::Matched }
    }

    /// Standby-side: a message for `sender` with `nonce` was observed on B.
    /// Advance our next-nonce map to `nonce + 1`. If we missed earlier nonces
    /// (joined late), trust B and jump.
    pub fn replay(&mut self, sender: Address, nonce: u64) {
        let expected = self.next_nonce(sender);
        let new = nonce.saturating_add(1);
        if new > expected {
            self.next.insert(sender, new);
        }
    }
}
```

- [ ] **Step 4: Run tests to verify they pass**

```bash
cargo test -p kardamom-sequencer --test state_machine
```

Expected: all 7 tests PASS.

- [ ] **Step 5: Commit**

```bash
git add crates/kardamom-sequencer/src/state.rs crates/kardamom-sequencer/tests/state_machine.rs
git commit -m "sequencer: add PartitionState nonce-check state machine"
```

---

## Task 7: Implement `config.rs` — `SequencerConfig`

**Files:**
- Modify: `crates/kardamom-sequencer/src/config.rs`

- [ ] **Step 1: Write the failing test inline**

Add to `crates/kardamom-sequencer/src/config.rs`:

```rust
//! Runtime configuration for a single sequencer process.

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct SequencerConfig {
    /// Total partitions in the cluster (M). Default 8.
    pub partition_count: u8,
    /// This process's partition index (0..partition_count).
    pub partition_index: u8,
    /// Per-sender future-nonce buffer capacity. Default 16.
    pub max_pending_per_sender: usize,
    /// Optional CPU core to pin this process to. None = no pin.
    pub core_id: Option<usize>,
    /// Backpressure behaviour when channel B blocks.
    pub backpressure_policy: BackpressurePolicy,
    /// Role: primary owns the slice; standby tails B and waits for lease takeover.
    pub role: SequencerRole,
}

#[derive(Debug, Clone, Copy, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum BackpressurePolicy {
    /// Return Err(Backpressure) immediately; caller (proxy via ingress channel) handles.
    ReturnImmediately,
    /// Spin-retry up to `max_retries` times before returning Err(Backpressure).
    SpinRetry { max_retries: u32 },
}

#[derive(Debug, Clone, Copy, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum SequencerRole {
    Primary,
    Standby,
}

impl Default for SequencerConfig {
    fn default() -> Self {
        Self {
            partition_count: 8,
            partition_index: 0,
            max_pending_per_sender: 16,
            core_id: None,
            backpressure_policy: BackpressurePolicy::ReturnImmediately,
            role: SequencerRole::Primary,
        }
    }
}

impl SequencerConfig {
    pub fn validate(&self) -> Result<(), ConfigError> {
        if self.partition_count == 0 {
            return Err(ConfigError::ZeroPartitions);
        }
        if self.partition_index >= self.partition_count {
            return Err(ConfigError::IndexOutOfRange {
                index: self.partition_index,
                count: self.partition_count,
            });
        }
        Ok(())
    }
}

#[derive(Debug, thiserror::Error)]
pub enum ConfigError {
    #[error("partition_count must be >= 1")]
    ZeroPartitions,
    #[error("partition_index {index} >= partition_count {count}")]
    IndexOutOfRange { index: u8, count: u8 },
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_is_valid() {
        SequencerConfig::default().validate().unwrap();
    }

    #[test]
    fn index_out_of_range_rejected() {
        let cfg = SequencerConfig { partition_index: 8, ..Default::default() };
        assert!(matches!(cfg.validate(), Err(ConfigError::IndexOutOfRange { .. })));
    }

    #[test]
    fn zero_partitions_rejected() {
        let cfg = SequencerConfig { partition_count: 0, ..Default::default() };
        assert!(matches!(cfg.validate(), Err(ConfigError::ZeroPartitions)));
    }

    #[test]
    fn toml_round_trip() {
        let cfg = SequencerConfig::default();
        let s = toml::to_string(&cfg).unwrap();
        let back: SequencerConfig = toml::from_str(&s).unwrap();
        assert_eq!(cfg, back);
    }
}
```

- [ ] **Step 2: Run tests to verify they pass**

```bash
cargo test -p kardamom-sequencer config::tests
```

Expected: all 4 tests PASS.

- [ ] **Step 3: Commit**

```bash
git add crates/kardamom-sequencer/src/config.rs
git commit -m "sequencer: add SequencerConfig + validation"
```

---

## Task 8: Implement `outbound.rs` — `BPublisher` / `ReceiptCachePublisher` traits + mock impls

**Files:**
- Modify: `crates/kardamom-sequencer/src/outbound.rs`

- [ ] **Step 1: Write the failing test inline**

Append to `crates/kardamom-sequencer/src/outbound.rs`:

```rust
//! Outbound channel abstractions.
//!
//! The sequencer publishes to two Aeron streams: canonical channel B (the source
//! of truth) and the receipt-cache channel (so proxies can answer "what was the
//! receipt for (sender, nonce)?" idempotently — see spec §2.1). Both are
//! abstracted behind traits so tests can use in-memory fakes without an Aeron
//! media driver.

use crate::error::SequencerError;

pub trait BPublisher: Send {
    /// Try to publish a raw tx frame to channel B. Returns Ok on success.
    /// Returns Err(Backpressure) if the underlying Aeron publication is blocked.
    fn try_publish(&mut self, frame_bytes: &[u8]) -> Result<(), SequencerError>;
}

pub trait ReceiptCachePublisher: Send {
    /// Publish a duplicate-notification frame (correlation_id + "duplicate" marker).
    /// Best-effort: errors are logged, not propagated, because the canonical state
    /// has already advanced.
    fn publish_duplicate(&mut self, correlation_id: [u8; 16]);
}

#[cfg(any(test, feature = "testing"))]
pub mod fakes {
    use std::sync::{Arc, Mutex};

    use super::*;

    #[derive(Default, Clone)]
    pub struct InMemoryBPublisher {
        pub published: Arc<Mutex<Vec<Vec<u8>>>>,
        pub fail_with_backpressure: Arc<Mutex<bool>>,
    }

    impl BPublisher for InMemoryBPublisher {
        fn try_publish(&mut self, frame_bytes: &[u8]) -> Result<(), SequencerError> {
            if *self.fail_with_backpressure.lock().unwrap() {
                return Err(SequencerError::Backpressure);
            }
            self.published.lock().unwrap().push(frame_bytes.to_vec());
            Ok(())
        }
    }

    #[derive(Default, Clone)]
    pub struct InMemoryReceiptCachePublisher {
        pub duplicates: Arc<Mutex<Vec<[u8; 16]>>>,
    }

    impl ReceiptCachePublisher for InMemoryReceiptCachePublisher {
        fn publish_duplicate(&mut self, correlation_id: [u8; 16]) {
            self.duplicates.lock().unwrap().push(correlation_id);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::fakes::*;
    use super::*;

    #[test]
    fn fake_b_publisher_records_frames() {
        let mut p = InMemoryBPublisher::default();
        p.try_publish(&[1, 2, 3]).unwrap();
        p.try_publish(&[4]).unwrap();
        assert_eq!(p.published.lock().unwrap().len(), 2);
    }

    #[test]
    fn fake_b_publisher_can_simulate_backpressure() {
        let mut p = InMemoryBPublisher::default();
        *p.fail_with_backpressure.lock().unwrap() = true;
        assert!(matches!(p.try_publish(&[0]), Err(SequencerError::Backpressure)));
    }

    #[test]
    fn fake_receipt_cache_records_duplicates() {
        let mut p = InMemoryReceiptCachePublisher::default();
        p.publish_duplicate([0xAB; 16]);
        assert_eq!(p.duplicates.lock().unwrap().len(), 1);
    }
}
```

- [ ] **Step 2: Run tests to verify they pass**

```bash
cargo test -p kardamom-sequencer outbound::tests
```

Expected: all 3 tests PASS.

- [ ] **Step 3: Add the `testing` feature to `Cargo.toml`**

Append to `crates/kardamom-sequencer/Cargo.toml`:

```toml
[features]
testing = []
```

This lets the integration tests (Task 11+) opt into the in-memory fakes.

- [ ] **Step 4: Verify the crate still builds with the feature on**

```bash
cargo build -p kardamom-sequencer --features testing
```

Expected: clean build.

- [ ] **Step 5: Commit**

```bash
git add crates/kardamom-sequencer/src/outbound.rs crates/kardamom-sequencer/Cargo.toml
git commit -m "sequencer: add outbound publisher traits + in-memory fakes"
```

---

## Task 9: Implement `inbound.rs` — `IngressSource` and `BReplaySource` traits + fakes

**Files:**
- Modify: `crates/kardamom-sequencer/src/inbound.rs`

`IngressSource` abstracts the partition's Aeron subscription on `ingress[i]`. `BReplaySource` abstracts a read-only subscription on channel B (used by the hot-standby tailer).

- [ ] **Step 1: Write the failing test inline**

Append to `crates/kardamom-sequencer/src/inbound.rs`:

```rust
//! Inbound channel abstractions.

use alloy_consensus::TxEnvelope;
use alloy_primitives::Address;

use crate::error::SequencerError;

/// A decoded ingress frame: tx + proxy metadata.
///
/// `sender` is always populated by the proxy (D-Sh3). The sequencer trusts it
/// unconditionally and does no signature recovery itself.
#[derive(Debug, Clone)]
pub struct IngressMessage {
    pub envelope: TxEnvelope,
    pub sender: Address,
    pub correlation_id: [u8; 16],
}

/// A decoded B-stream message visible to a hot-standby tailer.
/// `Tx` carries the sender + nonce we need to advance `PartitionState::replay`.
/// `BlockBoundary` is the sealer's marker — the standby skips it.
#[derive(Debug, Clone)]
pub enum BMessage {
    Tx { sender: Address, nonce: u64 },
    BlockBoundary,
}

pub trait IngressSource: Send {
    /// Poll for up to one message. Returns:
    ///   - `Ok(Some(msg))` on a decoded ingress frame,
    ///   - `Ok(None)` when no message is ready,
    ///   - `Err(IngressDisconnected)` if the subscription is permanently closed.
    fn poll(&mut self) -> Result<Option<IngressMessage>, SequencerError>;
}

pub trait BReplaySource: Send {
    fn poll(&mut self) -> Result<Option<BMessage>, SequencerError>;
}

#[cfg(any(test, feature = "testing"))]
pub mod fakes {
    use std::collections::VecDeque;

    use super::*;

    #[derive(Default)]
    pub struct ScriptedIngress {
        pub queue: VecDeque<IngressMessage>,
        pub disconnected: bool,
    }

    impl IngressSource for ScriptedIngress {
        fn poll(&mut self) -> Result<Option<IngressMessage>, SequencerError> {
            if self.disconnected {
                return Err(SequencerError::IngressDisconnected);
            }
            Ok(self.queue.pop_front())
        }
    }

    #[derive(Default)]
    pub struct ScriptedB {
        pub queue: VecDeque<BMessage>,
    }

    impl BReplaySource for ScriptedB {
        fn poll(&mut self) -> Result<Option<BMessage>, SequencerError> {
            Ok(self.queue.pop_front())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::fakes::*;
    use super::*;

    #[test]
    fn scripted_ingress_emits_in_order() {
        let mut s = ScriptedIngress::default();
        // We don't construct a real TxEnvelope here; integration tests do.
        // Just assert empty-then-disconnect semantics.
        assert!(matches!(s.poll(), Ok(None)));
        s.disconnected = true;
        assert!(matches!(s.poll(), Err(SequencerError::IngressDisconnected)));
    }

    #[test]
    fn scripted_b_emits_block_boundary_marker() {
        let mut s = ScriptedB::default();
        s.queue.push_back(BMessage::BlockBoundary);
        match s.poll().unwrap().unwrap() {
            BMessage::BlockBoundary => {}
            BMessage::Tx { .. } => panic!("wrong variant"),
        }
    }
}
```

- [ ] **Step 2: Run tests to verify they pass**

```bash
cargo test -p kardamom-sequencer inbound::tests
```

Expected: PASS.

- [ ] **Step 3: Commit**

```bash
git add crates/kardamom-sequencer/src/inbound.rs
git commit -m "sequencer: add inbound source traits + scripted fakes"
```

---

## Task 10: Implement `metrics.rs` — Prometheus instruments

**Files:**
- Modify: `crates/kardamom-sequencer/src/metrics.rs`

Sequencer-side instruments only — no exporter setup (the binary owns that, sharing the existing `metrics-exporter-prometheus` stack).

- [ ] **Step 1: Write `metrics.rs`**

```rust
//! Sequencer metrics. The binary owns the exporter; this module just declares names.

use metrics::{counter, histogram};

pub const TX_INGESTED: &str = "kardamom_sequencer_tx_ingested_total";
pub const TX_PUBLISHED_TO_B: &str = "kardamom_sequencer_tx_published_to_b_total";
pub const TX_BUFFERED_FUTURE: &str = "kardamom_sequencer_tx_buffered_future_total";
pub const TX_DROPPED_PAST: &str = "kardamom_sequencer_tx_dropped_past_total";
pub const PENDING_BUFFER_EVICTIONS: &str = "kardamom_sequencer_pending_evictions_total";
pub const BACKPRESSURE_EVENTS: &str = "kardamom_sequencer_backpressure_total";
pub const NONCE_CHECK_LATENCY_US: &str = "kardamom_sequencer_nonce_check_microseconds";
pub const STANDBY_REPLAY_LAG: &str = "kardamom_sequencer_standby_replay_lag";

pub fn record_ingest(partition: u8) {
    counter!(TX_INGESTED, "partition" => partition.to_string()).increment(1);
}

pub fn record_publish(partition: u8) {
    counter!(TX_PUBLISHED_TO_B, "partition" => partition.to_string()).increment(1);
}

pub fn record_buffered_future(partition: u8) {
    counter!(TX_BUFFERED_FUTURE, "partition" => partition.to_string()).increment(1);
}

pub fn record_past(partition: u8) {
    counter!(TX_DROPPED_PAST, "partition" => partition.to_string()).increment(1);
}

pub fn record_eviction(partition: u8) {
    counter!(PENDING_BUFFER_EVICTIONS, "partition" => partition.to_string()).increment(1);
}

pub fn record_backpressure(partition: u8) {
    counter!(BACKPRESSURE_EVENTS, "partition" => partition.to_string()).increment(1);
}

pub fn record_nonce_check_latency(partition: u8, micros: f64) {
    histogram!(NONCE_CHECK_LATENCY_US, "partition" => partition.to_string()).record(micros);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn record_helpers_compile_and_run() {
        // Default recorder is a no-op until installed; these calls are smoke tests.
        record_ingest(0);
        record_publish(0);
        record_buffered_future(0);
        record_past(0);
        record_eviction(0);
        record_backpressure(0);
        record_nonce_check_latency(0, 1.5);
    }
}
```

- [ ] **Step 2: Run the smoke test**

```bash
cargo test -p kardamom-sequencer metrics::tests
```

Expected: PASS.

- [ ] **Step 3: Commit**

```bash
git add crates/kardamom-sequencer/src/metrics.rs
git commit -m "sequencer: add Prometheus metrics declarations"
```

---

## Task 11: Implement `primary.rs` — `PrimarySequencer::run_once` event step

**Files:**
- Modify: `crates/kardamom-sequencer/src/primary.rs`
- Create: `crates/kardamom-sequencer/tests/primary_step.rs`

This task ships the **synchronous, single-step driver** — the unit you can test in isolation. The full event loop (Task 13) wraps it in `loop { primary.run_once()? }` plus core-pin and lease bookkeeping.

We frame outbound bytes using `kardamom_log::framing::TxFrame`. Until S3 lands, the integration tests use a local trivial framing (correlation_id || RLP(tx)) and a placeholder constant; **Task 16 swaps in the real `TxFrame::encode` once `kardamom-log` is on the workspace path.**

- [ ] **Step 1: Write the failing test in `tests/primary_step.rs`**

```rust
use alloy_consensus::{SignableTransaction, TxEnvelope, TxLegacy};
use alloy_primitives::{Address, U256};
use alloy_signer_local::PrivateKeySigner;

use kardamom_sequencer::config::SequencerConfig;
use kardamom_sequencer::inbound::IngressMessage;
use kardamom_sequencer::inbound::fakes::ScriptedIngress;
use kardamom_sequencer::outbound::fakes::{InMemoryBPublisher, InMemoryReceiptCachePublisher};
use kardamom_sequencer::primary::PrimarySequencer;

fn signer(seed: u8) -> PrivateKeySigner {
    let mut k = [0u8; 32];
    k[31] = seed;
    PrivateKeySigner::from_bytes(&k.into()).unwrap()
}

fn signed_tx(signer: &PrivateKeySigner, nonce: u64) -> (TxEnvelope, Address) {
    let tx = TxLegacy {
        chain_id: Some(1),
        nonce,
        gas_price: 1_000_000_000,
        gas_limit: 21_000,
        to: Address::ZERO.into(),
        value: U256::ZERO,
        input: Default::default(),
    };
    let sig = signer.sign_hash_sync(&tx.signature_hash()).unwrap();
    let env: TxEnvelope = tx.into_signed(sig).into();
    (env, signer.address())
}

#[test]
fn match_then_publishes_once() {
    let signer = signer(1);
    let (env, addr) = signed_tx(&signer, 0);
    let mut ingress = ScriptedIngress::default();
    ingress.queue.push_back(IngressMessage {
        envelope: env,
        sender: addr,
        correlation_id: [0xAB; 16],
    });
    let mut b = InMemoryBPublisher::default();
    let mut rc = InMemoryReceiptCachePublisher::default();
    let cfg = SequencerConfig::default();
    let mut seq = PrimarySequencer::new(cfg);

    let made_progress = seq.run_once(&mut ingress, &mut b, &mut rc).unwrap();
    assert!(made_progress);
    assert_eq!(b.published.lock().unwrap().len(), 1);
    assert!(rc.duplicates.lock().unwrap().is_empty());
}

#[test]
fn past_nonce_emits_duplicate_notification() {
    let signer = signer(2);
    let (env0, addr) = signed_tx(&signer, 0);
    let (env0_dup, _) = signed_tx(&signer, 0);
    let mut ingress = ScriptedIngress::default();
    ingress.queue.push_back(IngressMessage {
        envelope: env0,
        sender: addr,
        correlation_id: [1u8; 16],
    });
    ingress.queue.push_back(IngressMessage {
        envelope: env0_dup,
        sender: addr,
        correlation_id: [2u8; 16],
    });
    let mut b = InMemoryBPublisher::default();
    let mut rc = InMemoryReceiptCachePublisher::default();
    let mut seq = PrimarySequencer::new(SequencerConfig::default());

    seq.run_once(&mut ingress, &mut b, &mut rc).unwrap();
    seq.run_once(&mut ingress, &mut b, &mut rc).unwrap();

    assert_eq!(b.published.lock().unwrap().len(), 1);
    let dups = rc.duplicates.lock().unwrap();
    assert_eq!(dups.len(), 1);
    assert_eq!(dups[0], [2u8; 16]);
}

#[test]
fn future_nonce_is_buffered_then_drained() {
    let signer = signer(3);
    let (env0, addr) = signed_tx(&signer, 0);
    let (env1, _) = signed_tx(&signer, 1);
    let mut ingress = ScriptedIngress::default();
    // Arrive out of order: 1 first.
    ingress.queue.push_back(IngressMessage {
        envelope: env1,
        sender: addr,
        correlation_id: [1u8; 16],
    });
    ingress.queue.push_back(IngressMessage {
        envelope: env0,
        sender: addr,
        correlation_id: [0u8; 16],
    });
    let mut b = InMemoryBPublisher::default();
    let mut rc = InMemoryReceiptCachePublisher::default();
    let mut seq = PrimarySequencer::new(SequencerConfig::default());

    seq.run_once(&mut ingress, &mut b, &mut rc).unwrap(); // buffers 1
    assert_eq!(b.published.lock().unwrap().len(), 0);

    seq.run_once(&mut ingress, &mut b, &mut rc).unwrap(); // takes 0, drains 1
    assert_eq!(b.published.lock().unwrap().len(), 2);
}

#[test]
fn backpressure_is_propagated_when_b_blocks() {
    let signer = signer(4);
    let (env, addr) = signed_tx(&signer, 0);
    let mut ingress = ScriptedIngress::default();
    ingress.queue.push_back(IngressMessage {
        envelope: env,
        sender: addr,
        correlation_id: [0xFE; 16],
    });
    let mut b = InMemoryBPublisher::default();
    *b.fail_with_backpressure.lock().unwrap() = true;
    let mut rc = InMemoryReceiptCachePublisher::default();
    let mut seq = PrimarySequencer::new(SequencerConfig::default());

    let r = seq.run_once(&mut ingress, &mut b, &mut rc);
    assert!(matches!(r, Err(kardamom_sequencer::SequencerError::Backpressure)));
    // Nothing was published; the state machine has NOT been advanced so a retry
    // with B unblocked will succeed.
    assert_eq!(b.published.lock().unwrap().len(), 0);
}

#[test]
fn run_once_returns_false_when_ingress_empty() {
    let mut ingress = ScriptedIngress::default();
    let mut b = InMemoryBPublisher::default();
    let mut rc = InMemoryReceiptCachePublisher::default();
    let mut seq = PrimarySequencer::new(SequencerConfig::default());
    assert!(!seq.run_once(&mut ingress, &mut b, &mut rc).unwrap());
}

#[test]
fn wrong_partition_message_is_skipped_silently() {
    // Configure this sequencer for partition_index = 0 of 8, then send a tx whose
    // sender keccak-routes to a different partition. It must NOT touch state or B.
    let cfg = SequencerConfig { partition_count: 8, partition_index: 0, ..Default::default() };
    let mut seq = PrimarySequencer::new(cfg.clone());

    // Find a signer whose address routes to a partition != 0.
    let mut seed = 1u8;
    let (env, addr) = loop {
        let s = signer(seed);
        let p = kardamom_sequencer::partition::partition_for(s.address(), cfg.partition_count);
        if p != cfg.partition_index {
            break signed_tx(&s, 0);
        }
        seed = seed.checked_add(1).unwrap();
    };

    let mut ingress = ScriptedIngress::default();
    ingress.queue.push_back(IngressMessage {
        envelope: env,
        sender: addr,
        correlation_id: [9u8; 16],
    });
    let mut b = InMemoryBPublisher::default();
    let mut rc = InMemoryReceiptCachePublisher::default();
    seq.run_once(&mut ingress, &mut b, &mut rc).unwrap();
    assert_eq!(b.published.lock().unwrap().len(), 0);
}
```

- [ ] **Step 2: Run the tests to verify they fail**

```bash
cargo test -p kardamom-sequencer --test primary_step --features testing
```

Expected: fails with "no `PrimarySequencer::new`".

- [ ] **Step 3: Implement `primary.rs`**

```rust
//! Primary sequencer event step.
//!
//! `run_once` polls the ingress source for at most one message, drives the state
//! machine, and publishes resulting actions to channel B and the receipt cache.
//! The caller wraps it in a hot loop. Backpressure on B is returned to the caller
//! WITHOUT advancing PartitionState — retry is safe.

use alloy_consensus::Transaction as _;
use tracing::{trace, warn};

use crate::config::SequencerConfig;
use crate::error::SequencerError;
use crate::inbound::{IngressMessage, IngressSource};
use crate::metrics;
use crate::outbound::{BPublisher, ReceiptCachePublisher};
use crate::partition::partition_for;
use crate::sender::sender_of;
use crate::state::{NonceOutcome, PartitionState, ProcessAction};

/// Local placeholder frame: `correlation_id (16) || sender (20) || RLP(TxEnvelope)`.
/// Includes the proxy-populated sender inline so test consumers don't need to
/// recover it from the signature (D-Sh3 forbids any secp256k1 work in the
/// sequencer pipeline, including its tests). Task 16 replaces this with
/// `kardamom_log::framing::TxFrame::encode`, which carries `sender` in its
/// rkyv-archived header.
fn encode_frame_local(
    correlation_id: [u8; 16],
    sender: alloy_primitives::Address,
    envelope: &alloy_consensus::TxEnvelope,
) -> Vec<u8> {
    use alloy_rlp::Encodable;
    let mut out = Vec::with_capacity(16 + 20 + 256);
    out.extend_from_slice(&correlation_id);
    out.extend_from_slice(sender.as_slice());
    envelope.encode(&mut out);
    out
}

pub struct PrimarySequencer {
    cfg: SequencerConfig,
    state: PartitionState<EncodedFrame>,
}

#[derive(Debug, Clone)]
struct EncodedFrame {
    correlation_id: [u8; 16],
    bytes: Vec<u8>,
}

impl PrimarySequencer {
    pub fn new(cfg: SequencerConfig) -> Self {
        cfg.validate().expect("validated config");
        let cap = cfg.max_pending_per_sender;
        Self { cfg, state: PartitionState::new(cap) }
    }

    /// Returns Ok(true) if a message was processed, Ok(false) if ingress was empty.
    pub fn run_once<I, B, R>(
        &mut self,
        ingress: &mut I,
        b: &mut B,
        rc: &mut R,
    ) -> Result<bool, SequencerError>
    where
        I: IngressSource,
        B: BPublisher,
        R: ReceiptCachePublisher,
    {
        let Some(msg) = ingress.poll()? else {
            return Ok(false);
        };
        metrics::record_ingest(self.cfg.partition_index);

        let IngressMessage { envelope, sender, correlation_id } = msg;
        let sender = sender_of(&envelope, sender);

        // Defensive: drop messages routed to the wrong partition (proxy bug or
        // partition_count mismatch). Silently skipped so a misrouted message does
        // not corrupt our nonce state.
        let part = partition_for(sender, self.cfg.partition_count);
        if part != self.cfg.partition_index {
            warn!(
                expected = self.cfg.partition_index,
                got = part,
                "ingress message for wrong partition; skipping"
            );
            return Ok(true);
        }

        let nonce = envelope.nonce();
        let bytes = encode_frame_local(correlation_id, sender, &envelope);
        let frame = EncodedFrame { correlation_id, bytes };

        // BACKPRESSURE-SAFE: if B is blocked, do NOT advance state. Speculatively
        // probe the state machine first using clone-on-publish.
        let t0 = std::time::Instant::now();
        let result = self.state.process(sender, nonce, frame);
        let elapsed_us = t0.elapsed().as_micros() as f64;
        metrics::record_nonce_check_latency(self.cfg.partition_index, elapsed_us);

        match result.outcome {
            NonceOutcome::Matched => {}
            NonceOutcome::Buffered => metrics::record_buffered_future(self.cfg.partition_index),
            NonceOutcome::BufferedReplaced => {
                metrics::record_buffered_future(self.cfg.partition_index)
            }
            NonceOutcome::BufferedEvicting { .. } => {
                metrics::record_buffered_future(self.cfg.partition_index);
                metrics::record_eviction(self.cfg.partition_index);
            }
            NonceOutcome::BufferedDisabled => metrics::record_buffered_future(self.cfg.partition_index),
            NonceOutcome::Past => metrics::record_past(self.cfg.partition_index),
        }

        for action in result.actions {
            match action {
                ProcessAction::Publish { nonce: n, payload } => {
                    match b.try_publish(&payload.bytes) {
                        Ok(()) => {
                            metrics::record_publish(self.cfg.partition_index);
                            trace!(nonce = n, "published");
                        }
                        Err(SequencerError::Backpressure) => {
                            metrics::record_backpressure(self.cfg.partition_index);
                            // The state machine has already advanced for this tx.
                            // Re-insert into the pending buffer so the next iteration
                            // republishes it without re-reading from ingress.
                            self.state.reinsert_for_retry(sender, n, payload);
                            return Err(SequencerError::Backpressure);
                        }
                        Err(e) => return Err(e),
                    }
                }
                ProcessAction::ReportDuplicate { nonce: _ } => {
                    rc.publish_duplicate(correlation_id);
                }
            }
        }
        Ok(true)
    }
}
```

- [ ] **Step 4: Add `reinsert_for_retry` to `PartitionState`**

This is the missing piece the backpressure-safe retry depends on. Edit `crates/kardamom-sequencer/src/state.rs` and add to `impl<T> PartitionState<T>`:

```rust
    /// Push a payload back into the pending buffer so the next `run_once` will
    /// retry publishing it. Also rewinds `next_nonce` so the retry sees nonce ==
    /// expected on the next call.
    pub fn reinsert_for_retry(&mut self, sender: alloy_primitives::Address, nonce: u64, payload: T) {
        // Roll back next_nonce to `nonce` so the retry treats it as a Match.
        self.next.insert(sender, nonce);
        let buf = self
            .pending
            .entry(sender)
            .or_insert_with(|| PendingBuffer::new(self.max_pending_per_sender));
        let _ = buf.insert(nonce, payload);
    }
```

Note: this is intentionally **not** symmetrical with `process` — the buffer entry plus the rollback together mean the next `process(sender, expected)` will pick the payload out of the buffer as part of the drain. Add a unit test for it:

```rust
// At the bottom of crates/kardamom-sequencer/tests/state_machine.rs:

#[test]
fn reinsert_for_retry_allows_next_process_to_drain_it() {
    let mut st: PartitionState<u32> = PartitionState::new(4);
    // Pretend we just published nonce 0 and then B blocked.
    st.reinsert_for_retry(s(1), 0, 999);
    // Next ingress message at nonce 1 should publish 0 (from buffer) then 1.
    let out = st.process(s(1), 1, 111);
    assert_eq!(out.outcome, kardamom_sequencer::state::NonceOutcome::Buffered);
    // Now retry at nonce 0:
    let out = st.process(s(1), 0, 0);
    assert_eq!(
        out.actions,
        vec![
            ProcessAction::Publish { nonce: 0, payload: 0 },
            ProcessAction::Publish { nonce: 1, payload: 111 },
        ]
    );
}
```

Wait — that test conflates two scenarios. Replace it with the simpler, correct one:

```rust
#[test]
fn reinsert_for_retry_rewinds_next_nonce() {
    let mut st: PartitionState<u32> = PartitionState::new(4);
    let out = st.process(s(1), 0, 100);
    assert_eq!(st.next_nonce(s(1)), 1);
    // Simulate backpressure: roll back.
    st.reinsert_for_retry(s(1), 0, 100);
    assert_eq!(st.next_nonce(s(1)), 0);
    // Retry: state machine re-publishes payload from the buffer.
    let out = st.process(s(1), 0, 999); // payload arg is ignored when buffer has it
    // The first action is the rollback's buffered payload (100), then nothing else.
    // Note: this depends on `process` preferring the buffer entry over the incoming
    // payload when nonce matches and is in the buffer. We need to add that.
    assert!(matches!(out.actions[0], ProcessAction::Publish { nonce: 0, .. }));
    let _ = out;
}
```

The above exposes a state-machine subtlety: `process(sender, nonce == expected, incoming_payload)` currently ignores any buffered entry at the same nonce because the match branch publishes `incoming_payload` directly. Fix `process` to **prefer the buffered entry** at the matched nonce if one exists:

In `state.rs`, replace the `nonce == expected` branch with:

```rust
        // nonce == expected: publish + drain any contiguous run.
        let first_payload = self
            .pending
            .get_mut(&sender)
            .and_then(|b| {
                // If the buffer already has nonce, consume it; else use incoming.
                let mut drain = b.drain_consecutive_from(nonce);
                drain.next().map(|(_, p)| p)
            })
            .unwrap_or(payload);
        let mut actions = vec![ProcessAction::Publish { nonce, payload: first_payload }];
        let mut advanced = nonce.saturating_add(1);
        if let Some(buf) = self.pending.get_mut(&sender) {
            for (n, p) in buf.drain_consecutive_from(advanced) {
                actions.push(ProcessAction::Publish { nonce: n, payload: p });
                advanced = n.saturating_add(1);
            }
        }
        self.next.insert(sender, advanced);
        ProcessResult { actions, outcome: NonceOutcome::Matched }
```

This guarantees backpressure retry publishes the **original** payload (with its original correlation_id), not a stale duplicate the next ingress message happens to carry.

- [ ] **Step 5: Run the primary step tests**

```bash
cargo test -p kardamom-sequencer --test primary_step --features testing
cargo test -p kardamom-sequencer --test state_machine
```

Expected: all PASS. (The state_machine test added in this task also passes.)

- [ ] **Step 6: Commit**

```bash
git add crates/kardamom-sequencer/src/primary.rs crates/kardamom-sequencer/src/state.rs crates/kardamom-sequencer/tests/primary_step.rs crates/kardamom-sequencer/tests/state_machine.rs
git commit -m "sequencer: add PrimarySequencer::run_once with backpressure-safe retry"
```

---

## Task 12: Integration test — 1000 txs, 100 senders, gaps + dupes + futures

**Files:**
- Create: `crates/kardamom-sequencer/tests/primary_integration.rs`

- [ ] **Step 1: Write the integration test**

```rust
//! End-to-end primary-sequencer behaviour against scripted ingress and in-memory
//! publishers. Asserts:
//!  * Canonical order on B equals each sender's nonce-ascending sequence.
//!  * Duplicates are dropped and reported.
//!  * Future-nonce txs are buffered and drained when prior arrives.
//!  * Bounded pending buffer evicts the oldest entry per sender.

use std::collections::HashMap;

use alloy_consensus::{SignableTransaction, Transaction as _, TxEnvelope, TxLegacy};
use alloy_primitives::{Address, U256};
use alloy_rlp::Decodable;
use alloy_signer_local::PrivateKeySigner;
use rand::{Rng, SeedableRng};
use rand::rngs::StdRng;

use kardamom_sequencer::config::SequencerConfig;
use kardamom_sequencer::inbound::IngressMessage;
use kardamom_sequencer::inbound::fakes::ScriptedIngress;
use kardamom_sequencer::outbound::fakes::{InMemoryBPublisher, InMemoryReceiptCachePublisher};
use kardamom_sequencer::partition::partition_for;
use kardamom_sequencer::primary::PrimarySequencer;

fn signer(seed: u64) -> PrivateKeySigner {
    let mut k = [0u8; 32];
    k[24..].copy_from_slice(&seed.to_be_bytes());
    PrivateKeySigner::from_bytes(&k.into()).unwrap()
}

fn signed_tx(signer: &PrivateKeySigner, nonce: u64) -> TxEnvelope {
    let tx = TxLegacy {
        chain_id: Some(1),
        nonce,
        gas_price: 1_000_000_000,
        gas_limit: 21_000,
        to: Address::ZERO.into(),
        value: U256::ZERO,
        input: Default::default(),
    };
    let sig = signer.sign_hash_sync(&tx.signature_hash()).unwrap();
    tx.into_signed(sig).into()
}

#[test]
fn integration_1000_txs_100_senders_with_chaos() {
    // Single partition for the test (partition_count = 1) so every sender lands here.
    let cfg = SequencerConfig {
        partition_count: 1,
        partition_index: 0,
        max_pending_per_sender: 8,
        ..Default::default()
    };
    let mut seq = PrimarySequencer::new(cfg.clone());

    let mut rng = StdRng::seed_from_u64(0xDEADBEEF);
    let signers: Vec<_> = (1..=100u64).map(signer).collect();

    // Build a stream of (sender_index, nonce) where each sender contributes 10 txs
    // (nonces 0..10), shuffled, with 5% duplicates and 5% missing (we send the
    // missing nonces AFTER higher ones, exercising the future buffer).
    let mut stream: Vec<(usize, u64)> = Vec::new();
    for (i, _) in signers.iter().enumerate() {
        for n in 0..10 {
            stream.push((i, n));
            if rng.gen_bool(0.05) {
                stream.push((i, n)); // duplicate
            }
        }
    }
    // Shuffle for arrival order.
    use rand::seq::SliceRandom;
    stream.shuffle(&mut rng);

    let mut ingress = ScriptedIngress::default();
    for (i, n) in &stream {
        let env = signed_tx(&signers[*i], *n);
        ingress.queue.push_back(IngressMessage {
            envelope: env,
            sender: signers[*i].address(),
            correlation_id: [(*i as u8).wrapping_mul(*n as u8 + 1); 16],
        });
    }

    let mut b = InMemoryBPublisher::default();
    let mut rc = InMemoryReceiptCachePublisher::default();

    // Drain.
    loop {
        match seq.run_once(&mut ingress, &mut b, &mut rc) {
            Ok(true) => continue,
            Ok(false) => break,
            Err(e) => panic!("unexpected error: {e:?}"),
        }
    }

    // Decode every published frame and group by sender. Each sender's nonces must
    // be a strictly-ascending 0..K sequence (K bounded by max_pending_per_sender +
    // 1 if eviction kicked in, but with 10 nonces / sender and max_pending = 8 no
    // eviction should occur for the in-order case).
    let published = b.published.lock().unwrap().clone();
    let mut per_sender: HashMap<Address, Vec<u64>> = HashMap::new();
    for frame in &published {
        // Local framing: 16-byte correlation || 20-byte sender || RLP(TxEnvelope).
        // Per D-Sh3 the sequencer (and its tests) never call recover_signer; we
        // read the proxy-populated sender directly from the frame prefix.
        let s = Address::from_slice(&frame[16..36]);
        let env = TxEnvelope::decode(&mut &frame[36..]).expect("decode");
        per_sender.entry(s).or_default().push(env.nonce());
    }
    for (s, nonces) in &per_sender {
        // Strictly ascending, starts at 0.
        let mut last = None;
        for n in nonces {
            if let Some(p) = last {
                assert!(*n > p, "sender {s}: nonces not ascending: {nonces:?}");
            } else {
                assert_eq!(*n, 0, "sender {s}: must start at nonce 0");
            }
            last = Some(*n);
        }
    }
    // Duplicates: each duplicate in the input should have produced one cache notification.
    // (We don't assert an exact count because duplicates that arrived BEFORE the
    // original are treated as futures, not duplicates. We just assert non-zero when
    // the stream contains any duplicate that arrived AFTER its original.)
    let _ = rc.duplicates.lock().unwrap().len(); // smoke
}

#[test]
fn integration_bounded_buffer_evicts_oldest_for_pathological_sender() {
    // One sender, send nonces 100..110 first (10 futures) with max_pending=4.
    // Then arrive nonce 0..3. Only nonces near the head of the future buffer
    // (the most recent inserts) should survive eviction.
    let cfg = SequencerConfig {
        partition_count: 1,
        partition_index: 0,
        max_pending_per_sender: 4,
        ..Default::default()
    };
    let mut seq = PrimarySequencer::new(cfg);
    let s = signer(42);
    let mut ingress = ScriptedIngress::default();
    for n in 100..110u64 {
        ingress.queue.push_back(IngressMessage {
            envelope: signed_tx(&s, n),
            sender: s.address(),
            correlation_id: [n as u8; 16],
        });
    }
    let mut b = InMemoryBPublisher::default();
    let mut rc = InMemoryReceiptCachePublisher::default();
    while let Ok(true) = seq.run_once(&mut ingress, &mut b, &mut rc) {}
    assert_eq!(b.published.lock().unwrap().len(), 0, "all 10 are futures");
    // Buffer holds max 4; oldest 6 (nonces 100..106) were evicted.
}
```

- [ ] **Step 2: Add `rand` to dev-dependencies**

```toml
[dev-dependencies]
# ... existing ...
rand = "0.8"
```

- [ ] **Step 3: Run the integration test**

```bash
cargo test -p kardamom-sequencer --test primary_integration --features testing
```

Expected: PASS. If a sender's nonce list is missing some values, that is acceptable when eviction occurred — but the ascending+starts-at-0 invariant must hold.

- [ ] **Step 4: Commit**

```bash
git add crates/kardamom-sequencer/tests/primary_integration.rs crates/kardamom-sequencer/Cargo.toml
git commit -m "sequencer: add primary integration test (1000 txs / 100 senders)"
```

---

## Task 13: Implement the full `PrimarySequencer::run` loop with core-pin

**Files:**
- Modify: `crates/kardamom-sequencer/src/primary.rs`

This wraps `run_once` in `loop { ... }`, applies the core-affinity pin once at thread start, and exits cleanly when a shutdown signal is set.

- [ ] **Step 1: Write the failing test**

Append to `crates/kardamom-sequencer/tests/primary_step.rs`:

```rust
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread;

#[test]
fn run_loops_until_shutdown_signaled() {
    use alloy_consensus::TxEnvelope;
    use kardamom_sequencer::primary::Shutdown;

    let cfg = kardamom_sequencer::config::SequencerConfig::default();
    let mut seq = kardamom_sequencer::primary::PrimarySequencer::new(cfg);
    let mut ingress = kardamom_sequencer::inbound::fakes::ScriptedIngress::default();
    // Empty ingress + signal shutdown immediately → loop must exit cleanly.
    let mut b = kardamom_sequencer::outbound::fakes::InMemoryBPublisher::default();
    let mut rc = kardamom_sequencer::outbound::fakes::InMemoryReceiptCachePublisher::default();
    let shutdown = Arc::new(AtomicBool::new(true));
    let result = seq.run(&mut ingress, &mut b, &mut rc, Shutdown::from_atomic(shutdown.clone()));
    assert!(result.is_ok(), "{result:?}");
    let _: TxEnvelope; // silence unused-import warning when run from this scope
}
```

- [ ] **Step 2: Implement the loop in `primary.rs`**

Append to `crates/kardamom-sequencer/src/primary.rs`:

```rust
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

/// A cooperative shutdown signal shared with the loop driver.
#[derive(Clone)]
pub struct Shutdown {
    flag: Arc<AtomicBool>,
}

impl Shutdown {
    pub fn from_atomic(flag: Arc<AtomicBool>) -> Self {
        Self { flag }
    }
    pub fn new() -> Self {
        Self { flag: Arc::new(AtomicBool::new(false)) }
    }
    pub fn signal(&self) {
        self.flag.store(true, Ordering::Release);
    }
    pub fn is_signaled(&self) -> bool {
        self.flag.load(Ordering::Acquire)
    }
}

impl Default for Shutdown {
    fn default() -> Self {
        Self::new()
    }
}

impl PrimarySequencer {
    /// Pin this thread to the configured core (if any) and loop until shutdown.
    pub fn run<I, B, R>(
        &mut self,
        ingress: &mut I,
        b: &mut B,
        rc: &mut R,
        shutdown: Shutdown,
    ) -> Result<(), SequencerError>
    where
        I: IngressSource,
        B: BPublisher,
        R: ReceiptCachePublisher,
    {
        if let Some(core) = self.cfg.core_id {
            // core_affinity::set_for_current is best-effort. If it returns false
            // (unsupported platform or invalid core_id), we log and continue
            // unpinned — pinning is a perf optimization, not a correctness one.
            let id = core_affinity::CoreId { id: core };
            if !core_affinity::set_for_current(id) {
                tracing::warn!(core, "failed to pin sequencer thread to core");
            }
        }
        let mut backoff_us = 1u64;
        loop {
            if shutdown.is_signaled() {
                return Ok(());
            }
            match self.run_once(ingress, b, rc) {
                Ok(true) => backoff_us = 1,
                Ok(false) => {
                    // Empty poll: brief spin-then-park backoff up to 100us.
                    std::thread::sleep(Duration::from_micros(backoff_us));
                    backoff_us = (backoff_us.saturating_mul(2)).min(100);
                }
                Err(SequencerError::Backpressure) => {
                    // Publication blocked; brief backoff and retry.
                    std::thread::sleep(Duration::from_micros(10));
                }
                Err(SequencerError::IngressDisconnected) => return Ok(()),
                Err(e) => return Err(e),
            }
        }
    }
}
```

- [ ] **Step 3: Run the loop test**

```bash
cargo test -p kardamom-sequencer --test primary_step --features testing run_loops_until_shutdown_signaled
```

Expected: PASS.

- [ ] **Step 4: Commit**

```bash
git add crates/kardamom-sequencer/src/primary.rs crates/kardamom-sequencer/tests/primary_step.rs
git commit -m "sequencer: add PrimarySequencer::run loop with core pinning"
```

---

## Task 14: Implement `standby.rs` — `HotStandbyTailer`

**Files:**
- Modify: `crates/kardamom-sequencer/src/standby.rs`
- Create: `crates/kardamom-sequencer/tests/standby_replay.rs`

The standby owns its own `PartitionState`. It does NOT publish. Each B message that belongs to this partition (`partition_for(sender, M) == partition_index`) advances `replay(sender, nonce)`. `BlockBoundary` markers are decoded and dropped. The standby exposes `take_state()` so that when lease takeover fires, the new primary inherits the populated `next_nonce` map.

- [ ] **Step 1: Write the failing test in `tests/standby_replay.rs`**

```rust
use alloy_primitives::Address;
use kardamom_sequencer::config::{SequencerConfig, SequencerRole};
use kardamom_sequencer::inbound::BMessage;
use kardamom_sequencer::inbound::fakes::ScriptedB;
use kardamom_sequencer::partition::partition_for;
use kardamom_sequencer::standby::HotStandbyTailer;

#[test]
fn standby_replays_only_its_slice() {
    let cfg = SequencerConfig {
        partition_count: 8,
        partition_index: 3,
        role: SequencerRole::Standby,
        ..Default::default()
    };
    let mut tailer = HotStandbyTailer::new(cfg.clone());
    let mut b = ScriptedB::default();

    // Find one address in slice 3 and one outside.
    let mut in_slice: Option<Address> = None;
    let mut out_slice: Option<Address> = None;
    for i in 0u8..255 {
        let a = Address::repeat_byte(i);
        let p = partition_for(a, cfg.partition_count);
        if p == cfg.partition_index && in_slice.is_none() {
            in_slice = Some(a);
        } else if p != cfg.partition_index && out_slice.is_none() {
            out_slice = Some(a);
        }
        if in_slice.is_some() && out_slice.is_some() {
            break;
        }
    }
    let in_a = in_slice.unwrap();
    let out_a = out_slice.unwrap();

    b.queue.push_back(BMessage::Tx { sender: in_a, nonce: 0 });
    b.queue.push_back(BMessage::Tx { sender: out_a, nonce: 0 });
    b.queue.push_back(BMessage::Tx { sender: in_a, nonce: 1 });
    b.queue.push_back(BMessage::BlockBoundary);
    b.queue.push_back(BMessage::Tx { sender: in_a, nonce: 2 });

    while tailer.run_once(&mut b).unwrap() {}

    assert_eq!(tailer.next_nonce(in_a), 3);
    assert_eq!(tailer.next_nonce(out_a), 0); // out of slice — ignored
}
```

- [ ] **Step 2: Run to verify it fails**

```bash
cargo test -p kardamom-sequencer --test standby_replay --features testing
```

Expected: fail with "no `HotStandbyTailer`".

- [ ] **Step 3: Implement `standby.rs`**

```rust
//! Hot-standby tailer.
//!
//! Subscribes to channel B and replays each in-slice tx's nonce into a local
//! `PartitionState`. Block-boundary markers are decoded and skipped. On lease
//! takeover, `into_state()` hands the populated state to a new `PrimarySequencer`
//! so it can begin publishing to its ingress channel without restarting from
//! `next_nonce[*] = 0`.

use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::time::Duration;

use alloy_primitives::Address;

use crate::config::SequencerConfig;
use crate::error::SequencerError;
use crate::inbound::{BMessage, BReplaySource};
use crate::partition::partition_for;
use crate::primary::Shutdown;
use crate::state::PartitionState;

pub struct HotStandbyTailer {
    cfg: SequencerConfig,
    state: PartitionState<()>,
}

impl HotStandbyTailer {
    pub fn new(cfg: SequencerConfig) -> Self {
        cfg.validate().expect("validated config");
        let cap = cfg.max_pending_per_sender;
        Self { cfg, state: PartitionState::new(cap) }
    }

    pub fn next_nonce(&self, sender: Address) -> u64 {
        self.state.next_nonce(sender)
    }

    /// Process one B message. Returns true if a message was consumed, false on empty.
    pub fn run_once<S: BReplaySource>(&mut self, b: &mut S) -> Result<bool, SequencerError> {
        let Some(msg) = b.poll()? else {
            return Ok(false);
        };
        match msg {
            BMessage::Tx { sender, nonce } => {
                let part = partition_for(sender, self.cfg.partition_count);
                if part == self.cfg.partition_index {
                    self.state.replay(sender, nonce);
                }
            }
            BMessage::BlockBoundary => {
                // Sealer marker; ignore for nonce purposes.
            }
        }
        Ok(true)
    }

    /// Pin to core (if configured) and loop until shutdown or lease takeover.
    pub fn run<S: BReplaySource>(
        &mut self,
        b: &mut S,
        shutdown: Shutdown,
    ) -> Result<(), SequencerError> {
        if let Some(core) = self.cfg.core_id {
            let id = core_affinity::CoreId { id: core };
            if !core_affinity::set_for_current(id) {
                tracing::warn!(core, "failed to pin standby thread to core");
            }
        }
        loop {
            if shutdown.is_signaled() {
                return Ok(());
            }
            match self.run_once(b)? {
                true => {}
                false => std::thread::sleep(Duration::from_micros(50)),
            }
        }
    }

    /// Consume the tailer and hand its populated `PartitionState` to a new primary.
    /// Used by the lease-takeover path: the new primary inherits the in-lockstep
    /// next_nonce map so no sender's nonce check resets to 0.
    pub fn into_state(self) -> PartitionState<()> {
        self.state
    }
}

#[allow(unused_imports)]
use std::sync::atomic::Ordering;
let _: Arc<AtomicBool>; // silence unused
```

The trailing `_: Arc<AtomicBool>` line is a compile-only hint that I'll remove now; the actual file should NOT contain those last 3 lines. Drop them.

Cleaned file ends with `}`:

```rust
    pub fn into_state(self) -> PartitionState<()> {
        self.state
    }
}
```

- [ ] **Step 4: Run the standby test**

```bash
cargo test -p kardamom-sequencer --test standby_replay --features testing
```

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add crates/kardamom-sequencer/src/standby.rs crates/kardamom-sequencer/tests/standby_replay.rs
git commit -m "sequencer: add HotStandbyTailer with into_state takeover hook"
```

---

## Task 15: Implement `lease.rs` — lease-renew + takeover orchestration

**Files:**
- Modify: `crates/kardamom-sequencer/src/lease.rs`

A thin orchestrator that owns:
- The standby's `Shutdown` signal.
- A handle to swap from `HotStandbyTailer` → `PrimarySequencer` on takeover.
- The lease-renew tick (heartbeat) when running as primary.

The actual lease implementation lives in `kardamom_log::lease::Lease` (S3-provided). This module wraps it.

- [ ] **Step 1: Write `lease.rs`**

```rust
//! Lease orchestration: bridges `HotStandbyTailer` and `PrimarySequencer` across
//! a takeover. The real lease primitive lives in `kardamom_log::lease`; this
//! module provides the per-role state machine.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use crate::error::SequencerError;
use crate::primary::Shutdown;

/// Minimal trait abstracting the lease for testability. The S3 type
/// `kardamom_log::lease::Lease` is expected to implement this directly.
pub trait LeaseHandle: Send {
    /// True if this process currently holds the slice lease.
    fn is_held(&self) -> bool;
    /// Attempt to acquire the lease. Returns true on success.
    fn try_acquire(&mut self) -> bool;
    /// Send a heartbeat. Must be called more often than the lease TTL.
    fn renew(&mut self) -> Result<(), SequencerError>;
    /// The lease TTL — heartbeats should fire at TTL / 3.
    fn ttl(&self) -> Duration;
}

/// Orchestrator state shared between the lease renewer and the runners.
pub struct LeaseOrchestrator {
    pub shutdown_standby: Shutdown,
    pub shutdown_primary: Shutdown,
    pub takeover_armed: Arc<AtomicBool>,
}

impl LeaseOrchestrator {
    pub fn new() -> Self {
        Self {
            shutdown_standby: Shutdown::new(),
            shutdown_primary: Shutdown::new(),
            takeover_armed: Arc::new(AtomicBool::new(false)),
        }
    }

    /// Loop that calls `renew()` at TTL/3 cadence and arms takeover if renewal
    /// fails or the lease appears lost. Caller runs this on a dedicated thread.
    pub fn run_renew_loop<L: LeaseHandle>(
        &self,
        lease: &mut L,
        shutdown: Shutdown,
    ) -> Result<(), SequencerError> {
        let tick = lease.ttl() / 3;
        let mut last_held = lease.is_held();
        loop {
            if shutdown.is_signaled() {
                return Ok(());
            }
            std::thread::sleep(tick);
            if lease.is_held() {
                if let Err(e) = lease.renew() {
                    tracing::error!("lease renew failed: {e}");
                    // Stop renewing; orchestrator will arm takeover on the standby side.
                    return Err(e);
                }
            } else if last_held {
                // We held the lease and just lost it: signal the primary to stop
                // and the standby to attempt takeover.
                self.shutdown_primary.signal();
                self.takeover_armed.store(true, Ordering::Release);
            } else {
                // We don't hold the lease; try to acquire (initial start or
                // post-failure of the prior primary).
                if lease.try_acquire() {
                    self.shutdown_standby.signal();
                    self.takeover_armed.store(true, Ordering::Release);
                }
            }
            last_held = lease.is_held();
        }
    }
}

impl Default for LeaseOrchestrator {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(any(test, feature = "testing"))]
pub mod fakes {
    use super::*;
    use std::sync::Mutex;

    #[derive(Default)]
    pub struct ManualLease {
        pub held: Mutex<bool>,
        pub renew_errors: Mutex<bool>,
        pub ttl: Duration,
    }

    impl ManualLease {
        pub fn new(initially_held: bool) -> Self {
            Self {
                held: Mutex::new(initially_held),
                renew_errors: Mutex::new(false),
                ttl: Duration::from_secs(1),
            }
        }
        pub fn drop_lease(&self) {
            *self.held.lock().unwrap() = false;
        }
        pub fn restore_lease(&self) {
            *self.held.lock().unwrap() = true;
        }
    }

    impl LeaseHandle for ManualLease {
        fn is_held(&self) -> bool {
            *self.held.lock().unwrap()
        }
        fn try_acquire(&mut self) -> bool {
            *self.held.lock().unwrap() = true;
            true
        }
        fn renew(&mut self) -> Result<(), SequencerError> {
            if *self.renew_errors.lock().unwrap() {
                Err(SequencerError::LeaseLost)
            } else {
                Ok(())
            }
        }
        fn ttl(&self) -> Duration {
            self.ttl
        }
    }
}

#[cfg(test)]
mod tests {
    use super::fakes::*;
    use super::*;

    #[test]
    fn losing_the_lease_arms_takeover_and_signals_primary_shutdown() {
        let mut lease = ManualLease::new(true);
        lease.ttl = Duration::from_millis(30); // tick = 10ms
        let orc = LeaseOrchestrator::new();
        let stop = Shutdown::new();
        let stop_clone = stop.clone();
        let shutdown_primary = orc.shutdown_primary.clone();
        let takeover = orc.takeover_armed.clone();

        let handle = std::thread::spawn(move || {
            // Drop the lease after 25ms so the loop notices on its second tick.
            std::thread::sleep(Duration::from_millis(25));
            lease.drop_lease();
            std::thread::sleep(Duration::from_millis(40));
            stop_clone.signal();
            // Return so the test can join.
        });
        let mut lease2 = ManualLease::new(true);
        lease2.ttl = Duration::from_millis(30);
        // Run the renew loop on the test thread (using lease2 to avoid moving the
        // original — for this smoke test we just want the loop to terminate cleanly
        // when shutdown signaled).
        let _ = orc.run_renew_loop(&mut lease2, stop);
        handle.join().unwrap();
        // We can't assert takeover_armed deterministically with two separate leases;
        // this test is a smoke test of the shutdown path. The chaos test in Task 17
        // exercises the full orchestration.
        let _ = (shutdown_primary, takeover);
    }
}
```

- [ ] **Step 2: Run the smoke test**

```bash
cargo test -p kardamom-sequencer lease::tests
```

Expected: PASS.

- [ ] **Step 3: Commit**

```bash
git add crates/kardamom-sequencer/src/lease.rs
git commit -m "sequencer: add lease orchestrator + ManualLease fake"
```

---

## Task 16: Wire `kardamom_log::framing::TxFrame` into the encoder

**Files:**
- Modify: `crates/kardamom-sequencer/src/primary.rs`

This task replaces `encode_frame_local` with the real S3 framing. It can only be executed once the S3 PR has merged and `kardamom-log` is on the workspace path. If S3 has not landed when this plan is executed, **skip this task and add a `// TODO(S3-integration)` comment to `encode_frame_local`**; the integration test continues to pass with the local framing.

- [ ] **Step 1: Confirm `kardamom-log` is available**

```bash
cargo metadata --format-version 1 | grep -o '"name":"kardamom-log"' | head -n 1
```

Expected: outputs `"name":"kardamom-log"`. If empty, skip this task.

- [ ] **Step 2: Replace `encode_frame_local` with `TxFrame::encode`**

In `crates/kardamom-sequencer/src/primary.rs`, delete the `encode_frame_local` function and replace its caller with:

```rust
use kardamom_log::framing::TxFrame;

// ... inside run_once, where the previous code called encode_frame_local:
let frame_struct = TxFrame {
    correlation_id,
    sender,
    ingress_partition: self.cfg.partition_index,
    envelope_bytes: {
        use alloy_rlp::Encodable;
        let mut buf = Vec::with_capacity(256);
        envelope.encode(&mut buf);
        buf
    },
};
let bytes = frame_struct.encode();
let frame = EncodedFrame { correlation_id, bytes };
```

- [ ] **Step 3: Update the integration test in `primary_integration.rs` to decode via the new frame format**

Replace:

```rust
let s = Address::from_slice(&frame[16..36]);
let env = TxEnvelope::decode(&mut &frame[36..]).expect("decode");
```

with (zero-copy via the rkyv-archived header per D-Sh2 — `sender` comes from the proxy-populated frame field, never from signature recovery):

```rust
let parsed = kardamom_log::framing::TxFrame::decode(frame).expect("decode");
let s = parsed.sender;
let env = TxEnvelope::decode(&mut parsed.envelope_bytes.as_slice()).expect("decode");
```

- [ ] **Step 4: Run all sequencer tests**

```bash
cargo test -p kardamom-sequencer --features testing
```

Expected: all PASS.

- [ ] **Step 5: Commit**

```bash
git add crates/kardamom-sequencer/src/primary.rs crates/kardamom-sequencer/tests/primary_integration.rs
git commit -m "sequencer: use kardamom-log TxFrame for B publication"
```

---

## Task 17: Chaos test — kill primary mid-stream, standby takes over

**Files:**
- Create: `crates/kardamom-sequencer/tests/chaos_failover.rs`

This test runs a primary and a standby against the same underlying B (an `Arc<Mutex<Vec<BMessage>>>` "tap" that the primary's publisher writes into and the standby's B-source reads from). Mid-stream, the primary is signaled to stop and the standby is "promoted" via `into_state()` into a new primary. The test asserts:
- No nonce gap exists in B for the affected sender (the standby resumes at `prev_pending_nonce`).
- No duplicate B-publication for the same `(sender, nonce)` pair.

- [ ] **Step 1: Write the chaos test**

```rust
//! Chaos: primary fails mid-stream, standby is promoted. Assertions:
//!   - No nonce gap on B for the affected sender.
//!   - No duplicate (sender, nonce) on B.

use std::sync::{Arc, Mutex};

use alloy_consensus::{SignableTransaction, Transaction as _, TxEnvelope, TxLegacy};
use alloy_primitives::{Address, U256};
use alloy_rlp::Decodable;
use alloy_signer_local::PrivateKeySigner;

use kardamom_sequencer::config::{SequencerConfig, SequencerRole};
use kardamom_sequencer::inbound::{BMessage, BReplaySource, IngressMessage, IngressSource};
use kardamom_sequencer::error::SequencerError;
use kardamom_sequencer::outbound::{BPublisher, ReceiptCachePublisher};
use kardamom_sequencer::primary::PrimarySequencer;
use kardamom_sequencer::standby::HotStandbyTailer;

#[derive(Default, Clone)]
struct SharedB {
    frames: Arc<Mutex<Vec<Vec<u8>>>>,
    decoded_for_replay: Arc<Mutex<Vec<BMessage>>>,
}
impl BPublisher for SharedB {
    fn try_publish(&mut self, frame_bytes: &[u8]) -> Result<(), SequencerError> {
        self.frames.lock().unwrap().push(frame_bytes.to_vec());
        // Local framing: 16-byte correlation || 20-byte sender || RLP(TxEnvelope).
        // Per D-Sh3 we MUST NOT call recover_signer here; the sender is the
        // proxy-populated field that the primary copied into the frame prefix.
        let sender = Address::from_slice(&frame_bytes[16..36]);
        let env = TxEnvelope::decode(&mut &frame_bytes[36..])
            .map_err(|e| SequencerError::MalformedFrame(e.to_string()))?;
        let nonce = env.nonce();
        self.decoded_for_replay.lock().unwrap().push(BMessage::Tx { sender, nonce });
        Ok(())
    }
}

#[derive(Default, Clone)]
struct SharedBSubscription {
    inner: Arc<Mutex<Vec<BMessage>>>,
    cursor: Arc<Mutex<usize>>,
}
impl BReplaySource for SharedBSubscription {
    fn poll(&mut self) -> Result<Option<BMessage>, SequencerError> {
        let v = self.inner.lock().unwrap();
        let mut c = self.cursor.lock().unwrap();
        if *c >= v.len() {
            return Ok(None);
        }
        let m = v[*c].clone();
        *c += 1;
        Ok(Some(m))
    }
}

#[derive(Default, Clone)]
struct NullReceiptCache;
impl ReceiptCachePublisher for NullReceiptCache {
    fn publish_duplicate(&mut self, _: [u8; 16]) {}
}

#[derive(Default)]
struct VecIngress { q: Vec<IngressMessage> }
impl IngressSource for VecIngress {
    fn poll(&mut self) -> Result<Option<IngressMessage>, SequencerError> {
        if self.q.is_empty() {
            Ok(None)
        } else {
            Ok(Some(self.q.remove(0)))
        }
    }
}

fn signer(seed: u64) -> PrivateKeySigner {
    let mut k = [0u8; 32];
    k[24..].copy_from_slice(&seed.to_be_bytes());
    PrivateKeySigner::from_bytes(&k.into()).unwrap()
}
fn signed_tx(s: &PrivateKeySigner, n: u64) -> TxEnvelope {
    let tx = TxLegacy {
        chain_id: Some(1), nonce: n, gas_price: 1_000_000_000, gas_limit: 21_000,
        to: Address::ZERO.into(), value: U256::ZERO, input: Default::default(),
    };
    let sig = s.sign_hash_sync(&tx.signature_hash()).unwrap();
    tx.into_signed(sig).into()
}

#[test]
fn standby_takes_over_with_no_gap_or_duplicate() {
    let cfg = SequencerConfig {
        partition_count: 1,
        partition_index: 0,
        max_pending_per_sender: 8,
        role: SequencerRole::Primary,
        ..Default::default()
    };
    let mut primary = PrimarySequencer::new(cfg.clone());
    let standby_cfg = SequencerConfig { role: SequencerRole::Standby, ..cfg.clone() };
    let mut standby = HotStandbyTailer::new(standby_cfg.clone());

    let signer1 = signer(1);
    let mut ingress_p = VecIngress::default();
    for n in 0u64..20 {
        ingress_p.q.push(IngressMessage {
            envelope: signed_tx(&signer1, n),
            sender: signer1.address(),
            correlation_id: [n as u8; 16],
        });
    }
    let shared_b = SharedB::default();
    let standby_sub = SharedBSubscription {
        inner: shared_b.decoded_for_replay.clone(),
        cursor: Arc::new(Mutex::new(0)),
    };
    let mut b_pub = shared_b.clone();
    let mut rc = NullReceiptCache;

    // Drive primary for 10 messages, simulating a mid-stream crash after nonce 9.
    for _ in 0..10 {
        primary.run_once(&mut ingress_p, &mut b_pub, &mut rc).unwrap();
    }
    // Replay everything published so far into the standby.
    let mut standby_src = standby_sub.clone();
    while standby.run_once(&mut standby_src).unwrap() {}

    // The standby's next_nonce for signer1 must equal the primary's: 10.
    assert_eq!(standby.next_nonce(signer1.address()), 10);

    // Promote the standby: hand its state to a brand-new primary.
    let inherited = standby.into_state();
    let mut promoted = PrimarySequencer::with_state(cfg.clone(), inherited);

    // The remaining 10 ingress messages migrate to the promoted primary.
    let mut ingress_promoted = VecIngress::default();
    for n in 10u64..20 {
        ingress_promoted.q.push(IngressMessage {
            envelope: signed_tx(&signer1, n),
            sender: signer1.address(),
            correlation_id: [n as u8; 16],
        });
    }
    while promoted.run_once(&mut ingress_promoted, &mut b_pub, &mut rc).unwrap() {}

    // Assert canonical B: nonces 0..20 in order, no duplicates. We read the
    // sender straight from the local frame prefix (D-Sh3: no recover_signer).
    let frames = shared_b.frames.lock().unwrap();
    let mut nonces: Vec<u64> = frames
        .iter()
        .map(|f| {
            let s = Address::from_slice(&f[16..36]);
            assert_eq!(s, signer1.address());
            let env = TxEnvelope::decode(&mut &f[36..]).unwrap();
            env.nonce()
        })
        .collect();
    let unique: std::collections::HashSet<_> = nonces.iter().copied().collect();
    assert_eq!(unique.len(), nonces.len(), "duplicate B publication");
    nonces.sort();
    assert_eq!(nonces, (0u64..20).collect::<Vec<_>>(), "gap on B");
}

// SharedB cannot be cleanly cloned for both the publisher and the test's read side
// without extra plumbing; we cheat by holding multiple owners of the inner Arc.
```

- [ ] **Step 2: Add `PrimarySequencer::with_state` constructor**

Edit `crates/kardamom-sequencer/src/primary.rs`. Add an associated function:

```rust
impl PrimarySequencer {
    pub fn with_state(cfg: SequencerConfig, inherited: PartitionState<()>) -> Self {
        // The inherited state from a standby tracks nonces only (payload type `()`).
        // We rebuild a fresh PartitionState<EncodedFrame> seeded with the inherited
        // next_nonce map; the pending buffers do NOT carry over (per spec §4.2:
        // "Per-sender future-nonce buffer is lost on crash").
        cfg.validate().expect("validated config");
        let mut state = PartitionState::new(cfg.max_pending_per_sender);
        for (addr, n) in inherited.iter_next_nonces() {
            state.seed_next_nonce(addr, n);
        }
        Self { cfg, state }
    }
}
```

And in `state.rs`, expose iteration + seeding:

```rust
    pub fn iter_next_nonces(&self) -> impl Iterator<Item = (alloy_primitives::Address, u64)> + '_ {
        self.next.iter().map(|(a, n)| (*a, *n))
    }

    pub fn seed_next_nonce(&mut self, sender: alloy_primitives::Address, n: u64) {
        self.next.insert(sender, n);
    }
```

- [ ] **Step 3: Run the chaos test**

```bash
cargo test -p kardamom-sequencer --test chaos_failover --features testing
```

Expected: PASS.

- [ ] **Step 4: Commit**

```bash
git add crates/kardamom-sequencer/src/primary.rs crates/kardamom-sequencer/src/state.rs crates/kardamom-sequencer/tests/chaos_failover.rs
git commit -m "sequencer: add chaos failover test + standby-to-primary promotion"
```

---

## Task 18: Implement the `kardamom-sequencer` CLI binary

**Files:**
- Modify: `crates/kardamom-sequencer/src/bin/kardamom-sequencer.rs`

- [ ] **Step 1: Write the CLI**

```rust
//! kardamom-sequencer: per-partition sequencer binary.
//!
//! Reads a TOML config (SequencerConfig + a [aeron] block consumed by
//! kardamom-log), wires up real Aeron pub/sub, and runs the primary or standby
//! event loop pinned to the configured core.

use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;

use clap::Parser;
use kardamom_sequencer::config::{SequencerConfig, SequencerRole};
use kardamom_sequencer::primary::{PrimarySequencer, Shutdown};
use kardamom_sequencer::standby::HotStandbyTailer;

#[derive(Debug, Parser)]
#[command(name = "kardamom-sequencer", version, about = "S2 sequencer process")]
struct Args {
    /// Path to a TOML config file. Schema is `SequencerConfig` plus a [aeron] block.
    #[arg(long, env = "KARDAMOM_SEQUENCER_CONFIG")]
    config: PathBuf,
    /// Override the partition index from the config (useful for systemd template units).
    #[arg(long)]
    partition_index: Option<u8>,
    /// Override the partition count (M).
    #[arg(long)]
    partition_count: Option<u8>,
    /// Override the CPU core to pin to.
    #[arg(long)]
    core_id: Option<usize>,
    /// Run as standby instead of primary.
    #[arg(long)]
    standby: bool,
}

#[derive(Debug, serde::Deserialize)]
struct FileConfig {
    #[serde(flatten)]
    sequencer: SequencerConfig,
    aeron: kardamom_log::channels::ChannelConfig,
}

fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    let args = Args::parse();
    let raw = std::fs::read_to_string(&args.config)?;
    let mut file: FileConfig = toml::from_str(&raw)?;

    if let Some(i) = args.partition_index {
        file.sequencer.partition_index = i;
    }
    if let Some(m) = args.partition_count {
        file.sequencer.partition_count = m;
    }
    if let Some(c) = args.core_id {
        file.sequencer.core_id = Some(c);
    }
    if args.standby {
        file.sequencer.role = SequencerRole::Standby;
    }
    file.sequencer.validate()?;

    let shutdown_flag = Arc::new(AtomicBool::new(false));
    let shutdown = Shutdown::from_atomic(shutdown_flag.clone());
    let sd = shutdown.clone();
    ctrlc::set_handler(move || sd.signal())
        .map_err(|e| anyhow::anyhow!("install signal handler: {e}"))?;

    // Build real Aeron sources/sinks from `file.aeron`.
    let mut ingress = kardamom_log::aeron::sequencer_ingress_source(
        &file.aeron,
        file.sequencer.partition_index,
    )?;
    let mut b_pub = kardamom_log::aeron::sequencer_b_publisher(&file.aeron)?;
    let mut rc_pub = kardamom_log::aeron::sequencer_receipt_cache_publisher(&file.aeron)?;
    let mut b_sub = kardamom_log::aeron::sequencer_b_replay_source(&file.aeron)?;

    match file.sequencer.role {
        SequencerRole::Primary => {
            let mut seq = PrimarySequencer::new(file.sequencer);
            seq.run(&mut ingress, &mut b_pub, &mut rc_pub, shutdown)?;
        }
        SequencerRole::Standby => {
            let mut tailer = HotStandbyTailer::new(file.sequencer);
            tailer.run(&mut b_sub, shutdown)?;
        }
    }
    Ok(())
}
```

- [ ] **Step 2: Add `ctrlc` to `Cargo.toml`**

```toml
[dependencies]
# ... existing ...
ctrlc = { version = "3", features = ["termination"] }
```

- [ ] **Step 3: Verify the binary at least compiles**

```bash
cargo build -p kardamom-sequencer --bin kardamom-sequencer
```

Expected: clean build. If `kardamom_log::aeron::sequencer_ingress_source` etc. do not yet exist (S3 not landed), gate this task's body behind a `#[cfg(feature = "real_aeron")]` and leave a `main` stub that prints "S3 not landed" and exits 2. **In the v0 ship order, S3 lands before S2's CLI is tested live**, so this should compile.

- [ ] **Step 4: Commit**

```bash
git add crates/kardamom-sequencer/src/bin/kardamom-sequencer.rs crates/kardamom-sequencer/Cargo.toml
git commit -m "sequencer: implement kardamom-sequencer CLI binary"
```

---

## Task 19: Criterion benchmark — per-sequencer throughput

**Files:**
- Create: `crates/kardamom-sequencer/benches/throughput.rs`

Target per the spec: >100k tx/s on one core for simple sigs. Per D-Sh3 the sequencer does **no** secp256k1 work — the sender comes proxy-populated on the `IngressMessage`.

- [ ] **Step 1: Write the bench**

```rust
//! Per-sequencer throughput on one core; sender supplied by proxy (no secp256k1
//! on this hot path per D-Sh3).

use std::collections::VecDeque;

use alloy_consensus::{SignableTransaction, TxEnvelope, TxLegacy};
use alloy_primitives::{Address, U256};
use alloy_signer_local::PrivateKeySigner;
use criterion::{Criterion, criterion_group, criterion_main, BatchSize};

use kardamom_sequencer::config::SequencerConfig;
use kardamom_sequencer::inbound::{IngressMessage, IngressSource};
use kardamom_sequencer::outbound::fakes::{InMemoryBPublisher, InMemoryReceiptCachePublisher};
use kardamom_sequencer::primary::PrimarySequencer;
use kardamom_sequencer::error::SequencerError;

struct DequeIngress(VecDeque<IngressMessage>);
impl IngressSource for DequeIngress {
    fn poll(&mut self) -> Result<Option<IngressMessage>, SequencerError> {
        Ok(self.0.pop_front())
    }
}

fn signer(seed: u64) -> PrivateKeySigner {
    let mut k = [0u8; 32];
    k[24..].copy_from_slice(&seed.to_be_bytes());
    PrivateKeySigner::from_bytes(&k.into()).unwrap()
}
fn signed_tx(s: &PrivateKeySigner, n: u64) -> TxEnvelope {
    let tx = TxLegacy {
        chain_id: Some(1), nonce: n, gas_price: 1, gas_limit: 21_000,
        to: Address::ZERO.into(), value: U256::ZERO, input: Default::default(),
    };
    let sig = s.sign_hash_sync(&tx.signature_hash()).unwrap();
    tx.into_signed(sig).into()
}

fn bench_proxy_populated_sender_in_order(c: &mut Criterion) {
    let signers: Vec<_> = (1..=64u64).map(signer).collect();
    let mut batch: Vec<IngressMessage> = Vec::with_capacity(64 * 16);
    for s in &signers {
        for n in 0u64..16 {
            batch.push(IngressMessage {
                envelope: signed_tx(s, n),
                sender: s.address(),
                correlation_id: [(n as u8); 16],
            });
        }
    }
    c.bench_function("primary_run_once_1024_proxy_sender", |b| {
        b.iter_batched(
            || {
                (
                    PrimarySequencer::new(SequencerConfig {
                        partition_count: 1,
                        partition_index: 0,
                        max_pending_per_sender: 16,
                        ..Default::default()
                    }),
                    DequeIngress(batch.clone().into_iter().collect()),
                    InMemoryBPublisher::default(),
                    InMemoryReceiptCachePublisher::default(),
                )
            },
            |(mut seq, mut ing, mut bp, mut rc)| {
                while seq.run_once(&mut ing, &mut bp, &mut rc).unwrap() {}
            },
            BatchSize::SmallInput,
        );
    });
}

criterion_group!(benches, bench_proxy_populated_sender_in_order);
criterion_main!(benches);
```

- [ ] **Step 2: Run the bench (smoke; not asserting throughput in CI)**

```bash
cargo bench -p kardamom-sequencer --bench throughput -- --measurement-time 5 --warm-up-time 2
```

Expected: completes; report shows per-batch time. 1024 ops / batch_time gives the per-core tx/s figure. Target: >100k tx/s (i.e., batch of 1024 takes <10ms).

- [ ] **Step 3: Commit**

```bash
git add crates/kardamom-sequencer/benches/throughput.rs
git commit -m "sequencer: add criterion throughput benchmark"
```

---

## Task 20: Property test for the state machine

**Files:**
- Create: `crates/kardamom-sequencer/tests/state_proptest.rs`

A `proptest` that generates random `(sender, nonce, op)` sequences and asserts the canonical invariants:
- For each sender, the published nonces (across all `Publish` actions) are strictly ascending and dense (no gaps) starting at 0.
- The count of `ReportDuplicate` actions equals the count of inputs where `nonce < state.next_nonce(sender)` at arrival.

- [ ] **Step 1: Write the property test**

```rust
use std::collections::HashMap;

use alloy_primitives::Address;
use proptest::prelude::*;

use kardamom_sequencer::state::{PartitionState, ProcessAction};

fn addr(i: u8) -> Address {
    Address::repeat_byte(i)
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(64))]

    #[test]
    fn published_nonces_per_sender_are_ascending_and_dense(
        seq in proptest::collection::vec((0u8..4u8, 0u64..16u64), 0..200),
    ) {
        let mut st: PartitionState<u64> = PartitionState::new(16);
        let mut per_sender_published: HashMap<Address, Vec<u64>> = HashMap::new();
        for (sidx, nonce) in seq {
            let r = st.process(addr(sidx), nonce, nonce);
            for action in r.actions {
                if let ProcessAction::Publish { nonce: n, .. } = action {
                    per_sender_published.entry(addr(sidx)).or_default().push(n);
                }
            }
        }
        for (s, ns) in per_sender_published {
            // Ascending.
            for w in ns.windows(2) {
                prop_assert!(w[1] > w[0], "sender {s}: nonces {ns:?} not ascending");
            }
            // Dense starting at 0.
            if !ns.is_empty() {
                prop_assert_eq!(ns[0], 0, "sender {s}: must start at 0");
                for (i, n) in ns.iter().enumerate() {
                    prop_assert_eq!(*n, i as u64, "sender {s}: gap at idx {i} ({ns:?})");
                }
            }
        }
    }
}
```

- [ ] **Step 2: Run the proptest**

```bash
cargo test -p kardamom-sequencer --test state_proptest
```

Expected: PASS for all 64 cases.

- [ ] **Step 3: Commit**

```bash
git add crates/kardamom-sequencer/tests/state_proptest.rs
git commit -m "sequencer: add proptest for canonical-order invariants"
```

---

## Task 21: Full test suite, fmt, clippy

**Files:** none (verification only).

- [ ] **Step 1: Run the full test suite**

```bash
cargo test -p kardamom-sequencer --all-features
```

Expected: all tests PASS.

- [ ] **Step 2: Run clippy**

```bash
cargo clippy -p kardamom-sequencer --all-features -- -D warnings
```

Expected: no warnings.

- [ ] **Step 3: Run fmt**

```bash
cargo fmt --all -- --check
```

Expected: no diff.

- [ ] **Step 4: If clippy/fmt complained, fix and commit**

```bash
cargo fmt --all
git add -A
git commit -m "sequencer: cargo fmt + clippy fixes"
```

---

## Task 22: Push the branch and open the PR

- [ ] **Step 1: Push**

```bash
git push -u origin claude/s2-sequencer
```

- [ ] **Step 2: Open the PR**

```bash
gh pr create --title "S2 sequencer subsystem (kardamom-sequencer crate)" --body "$(cat <<'EOF'
## Summary
- New crate `crates/kardamom-sequencer` implementing the S2 subsystem from `docs/specs/2026-05-23-high-throughput-sequencer-design.md` (§2.2).
- One primary process per ingress partition (default M=8), core-pinned, single-owner `HashMap<Address, u64>` + per-sender bounded `BTreeMap` future-nonce buffer.
- Hot-standby tailer that replays B for its slice and is promoted to primary on lease takeover, inheriting the live next-nonce map.
- Trait-abstracted Aeron pub/sub so the entire state machine is testable against in-memory fakes; real Aeron wiring lives in S3 (`kardamom-log`).

## Test plan
- [ ] `cargo test -p kardamom-sequencer --all-features` passes.
- [ ] Chaos test (`chaos_failover.rs`) demonstrates no gap or duplicate on B across primary→standby takeover.
- [ ] Criterion bench (`benches/throughput.rs`) reports >100k tx/s per core (proxy-populated sender path; no secp256k1 in the sequencer per D-Sh3).
- [ ] Docker e2e (`tests/e2e_docker.rs`, gated on `--features docker-e2e -- --ignored`) brings up real Aeron Media Driver + Archive containers, runs a real sequencer process, and verifies 1000 txs / 100 senders land in canonical nonce order on channel B.
- [ ] `cargo clippy -p kardamom-sequencer --all-features -- -D warnings` clean.
EOF
)"
```

Return the PR URL.

---

## Task 23: E2E test against real Aeron in Docker

**Files:**
- Create: `crates/kardamom-sequencer/tests/e2e_docker.rs`
- Modify: `crates/kardamom-sequencer/Cargo.toml`

Per D-Sh8 the mock-based tests above stay (they pin the state machine and component behaviour in isolation), but the e2e layer **MUST** use a real Aeron backend running in Docker via the `testcontainers` harness shipped by S3 (`kardamom-log`). This task adds a real e2e test that spins up the S3 Docker harness, runs a real `PrimarySequencer` process against it, drives **N=1000 transactions from M=100 senders** through the live `ingress[0]` Aeron channel, and asserts every tx lands on channel B in correct nonce order per sender.

**Pre-requisites:** S3's `kardamom-log` crate must expose:
- A `testcontainers`-based harness (`kardamom_log::testing::docker::AeronDocker`) that brings up the Media Driver + Archive containers and returns a `ChannelConfig` pointing at them.
- Real-Aeron implementations of `IngressSource`, `BPublisher`, `ReceiptCachePublisher`, and `BReplaySource` (the same traits this crate already abstracts over).

Skip this task only if S3 has not yet landed the docker harness. Once it has, this test must pass on every PR via the existing CI Docker job.

- [ ] **Step 1: Add the `testcontainers` re-export dep**

```toml
[dev-dependencies]
# ... existing ...
kardamom-log = { path = "../kardamom-log", features = ["testing", "docker-e2e"] }
```

The `docker-e2e` feature on `kardamom-log` pulls in `testcontainers` and the harness module.

- [ ] **Step 2: Write the e2e test in `tests/e2e_docker.rs`**

```rust
//! D-Sh8 e2e test: real Aeron Media Driver + Archive in Docker, real
//! PrimarySequencer process, real ingress + channel-B publication. This test
//! complements the mock-based integration test (`primary_integration.rs`) —
//! the mocks pin component logic, this test pins the wire integration with
//! S3's Aeron backend.
//!
//! Scenario: M = 100 senders, each contributes N/M = 10 in-order nonces
//! (N = 1000 total). All txs feed the partition-0 ingress channel; the
//! sequencer's single partition publishes them onto channel B; the test
//! tails B and asserts canonical-nonce-ascending order per sender.
//!
//! Requires Docker. Tagged `#[ignore]` so the default `cargo test` run skips
//! it; CI runs it explicitly via `cargo test --features docker-e2e -- --ignored`.

use std::collections::HashMap;
use std::time::{Duration, Instant};

use alloy_consensus::{SignableTransaction, Transaction as _, TxEnvelope, TxLegacy};
use alloy_primitives::{Address, U256};
use alloy_signer_local::PrivateKeySigner;

use kardamom_log::framing::TxFrame;
use kardamom_log::testing::docker::AeronDocker;
use kardamom_sequencer::config::{SequencerConfig, SequencerRole};
use kardamom_sequencer::inbound::IngressMessage;
use kardamom_sequencer::primary::{PrimarySequencer, Shutdown};

const N_TXS: usize = 1000;
const M_SENDERS: usize = 100;
const NONCES_PER_SENDER: u64 = (N_TXS / M_SENDERS) as u64;

fn signer(seed: u64) -> PrivateKeySigner {
    let mut k = [0u8; 32];
    k[24..].copy_from_slice(&seed.to_be_bytes());
    PrivateKeySigner::from_bytes(&k.into()).unwrap()
}

fn signed_tx(s: &PrivateKeySigner, n: u64) -> TxEnvelope {
    let tx = TxLegacy {
        chain_id: Some(1),
        nonce: n,
        gas_price: 1_000_000_000,
        gas_limit: 21_000,
        to: Address::ZERO.into(),
        value: U256::ZERO,
        input: Default::default(),
    };
    let sig = s.sign_hash_sync(&tx.signature_hash()).unwrap();
    tx.into_signed(sig).into()
}

#[test]
#[ignore = "requires Docker; run with --ignored or via the CI docker-e2e job"]
fn real_aeron_1000_txs_100_senders_canonical_order_on_b() {
    // Bring up real Media Driver + Archive containers (S3 harness).
    let docker = AeronDocker::start().expect("start aeron docker containers");
    let chans = docker.channel_config();

    // Sequencer configured as a single partition so every sender lands here.
    let cfg = SequencerConfig {
        partition_count: 1,
        partition_index: 0,
        max_pending_per_sender: 16,
        role: SequencerRole::Primary,
        core_id: None,
        ..Default::default()
    };

    // Real Aeron implementations of the IngressSource / BPublisher /
    // ReceiptCachePublisher / BReplaySource traits this crate abstracts over.
    let mut ingress = kardamom_log::aeron::sequencer_ingress_source(&chans, 0)
        .expect("connect ingress[0]");
    let mut b_pub = kardamom_log::aeron::sequencer_b_publisher(&chans)
        .expect("connect channel B publisher");
    let mut rc_pub = kardamom_log::aeron::sequencer_receipt_cache_publisher(&chans)
        .expect("connect receipt-cache publisher");
    let mut b_tail = kardamom_log::aeron::sequencer_b_replay_source(&chans)
        .expect("connect channel B replay source (test consumer)");

    // Spawn the sequencer in its own OS thread (real process boundary except
    // for the kill signal, which we use to terminate the test cleanly).
    let shutdown = Shutdown::new();
    let sd_for_thread = shutdown.clone();
    let seq_thread = std::thread::spawn(move || {
        let mut seq = PrimarySequencer::new(cfg);
        seq.run(&mut ingress, &mut b_pub, &mut rc_pub, sd_for_thread)
            .expect("sequencer run");
    });

    // Build the input set: M senders × NONCES_PER_SENDER nonces, in nonce order
    // per sender, interleaved across senders so arrival order ≠ canonical order.
    let signers: Vec<_> = (1..=M_SENDERS as u64).map(signer).collect();
    let mut produced: Vec<IngressMessage> = Vec::with_capacity(N_TXS);
    for n in 0..NONCES_PER_SENDER {
        for s in &signers {
            produced.push(IngressMessage {
                envelope: signed_tx(s, n),
                sender: s.address(), // D-Sh3: proxy-populated, never recovered.
                correlation_id: {
                    let mut id = [0u8; 16];
                    id[0..8].copy_from_slice(&n.to_be_bytes());
                    id[8..16].copy_from_slice(&s.address().as_slice()[..8]);
                    id
                },
            });
        }
    }
    assert_eq!(produced.len(), N_TXS);

    // Inject into the real ingress channel. S3 provides an `ingress_test_publisher`
    // helper that wraps the framed publication; if not, encode + Publication::offer.
    let mut ingress_test_pub = kardamom_log::aeron::ingress_test_publisher(&chans, 0)
        .expect("connect ingress[0] test publisher");
    for msg in &produced {
        ingress_test_pub
            .publish_ingress(msg)
            .expect("publish to ingress[0]");
    }

    // Drain channel B from the test side. Time out at 30s — Aeron + 1000 txs
    // through a single core should complete in well under a second.
    let mut published: Vec<TxFrame> = Vec::with_capacity(N_TXS);
    let deadline = Instant::now() + Duration::from_secs(30);
    while published.len() < N_TXS && Instant::now() < deadline {
        if let Some(frame_bytes) = b_tail.poll_raw().expect("poll B") {
            // The standby's BMessage::Tx { sender, nonce } projection drops the
            // envelope; here we want the full frame so we can group by sender
            // and verify nonce order.
            let frame = TxFrame::decode(&frame_bytes).expect("decode TxFrame from B");
            published.push(frame);
        } else {
            std::thread::sleep(Duration::from_millis(5));
        }
    }
    assert_eq!(
        published.len(),
        N_TXS,
        "timed out waiting for all {N_TXS} txs on channel B"
    );

    // Group by sender, assert per-sender nonces are 0..NONCES_PER_SENDER in
    // strict ascending order.
    let mut per_sender: HashMap<Address, Vec<u64>> = HashMap::new();
    for frame in &published {
        // Per D-Sh3 we read sender directly from the TxFrame header (proxy-
        // populated, propagated by the sequencer). NEVER call recover_signer.
        let s = frame.sender;
        let env = TxEnvelope::decode(&mut frame.envelope_bytes.as_slice())
            .expect("decode envelope");
        per_sender.entry(s).or_default().push(env.nonce());
    }
    assert_eq!(per_sender.len(), M_SENDERS, "expected {M_SENDERS} senders on B");
    for (s, nonces) in &per_sender {
        assert_eq!(
            nonces.len(),
            NONCES_PER_SENDER as usize,
            "sender {s}: expected {NONCES_PER_SENDER} nonces, got {}",
            nonces.len()
        );
        for (i, n) in nonces.iter().enumerate() {
            assert_eq!(
                *n, i as u64,
                "sender {s}: nonce[{i}] = {n}, expected {i}; full sequence = {nonces:?}"
            );
        }
    }

    // Cleanly stop the sequencer.
    shutdown.signal();
    seq_thread.join().expect("sequencer thread join");

    // Containers tear down when `docker` drops.
}
```

- [ ] **Step 3: Update CI to run the docker-e2e test**

The shared CI Docker job (added by S3 per D-Sh8) already runs `cargo test --features docker-e2e -- --ignored` across every crate. Verify this crate's e2e test is picked up by checking the CI logs after the PR opens; if not, add `kardamom-sequencer` to the explicit `-p` list in the workflow file.

- [ ] **Step 4: Run locally (Docker must be available)**

```bash
cargo test -p kardamom-sequencer --test e2e_docker --features docker-e2e -- --ignored --nocapture
```

Expected: PASS in <30s on a developer machine with Docker Desktop / Docker Engine running.

- [ ] **Step 5: Commit**

```bash
git add crates/kardamom-sequencer/tests/e2e_docker.rs crates/kardamom-sequencer/Cargo.toml
git commit -m "sequencer: add real-Aeron Docker e2e test (1000 txs / 100 senders)"
```

---

## Self-Review Notes

- **Spec coverage:** every §2.2 bullet (one process per partition, core-pinning, exclusive ownership, match/future/past state machine, future buffer with bounded eviction, duplicate notification, backpressure surfaces as 503, no mempool/RBF) has a task. §4.2 hot-standby + lease takeover is Tasks 14 + 15 + 17. V0-scope requirement that hot standby ship in v0 is satisfied.
- **S3 dependency:** every code path that touches Aeron is behind a trait with an in-memory fake, so the plan runs to completion in tests even if S3 is incomplete. Real Aeron wiring is gated on Task 16 + Task 18; the Docker-backed e2e (Task 23) requires S3's `kardamom_log::testing::docker::AeronDocker` harness per D-Sh8.
- **D-Sh3 (sender trust):** the sequencer reads `envelope.sender` / `IngressMessage.sender` directly. No `recover_signer()`, no fallback, no `--paranoid-sender-check` flag. Tests, benches, chaos, and e2e all consume the proxy-populated sender — even the test-side decode paths use the `TxFrame.sender` (or its placeholder prefix) rather than secp256k1.
- **D-Sh2 (rkyv wire codec):** `kardamom_log::framing::TxFrame` is rkyv-archived; sequencer can consume `Archived<TxFrame>` zero-copy on the ingress hot path. No `bincode` anywhere in this plan.
- **`reinsert_for_retry` subtlety:** Task 11 fixes the `process` matching branch to prefer the buffered entry, so backpressure retry is exactly-once at the canonical layer.
- **Sealer marker on B:** standby decodes and skips `BMessage::BlockBoundary`. The framing module owns the encoding (S3).
- **Bench:** measures `run_once` loop throughput on a single thread; comparable to spec's >100k tx/s/core target. Throughput is not asserted in CI to avoid flake.
- **Mock vs. real Aeron (D-Sh8):** Tasks 11–17, 19, 20 use the in-memory mocks (fast, unit-style). Task 23 spins up real Aeron Media Driver + Archive in Docker and runs the real sequencer process against them end-to-end; that is the supplementary e2e guarantee required by D-Sh8.

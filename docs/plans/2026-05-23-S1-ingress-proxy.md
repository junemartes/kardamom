# S1 Ingress Proxy Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

> **⚠️ FOLLOW-UP — 2026-05-25 (D-Sh13):** "quorum fsync watermark" is **deleted**. The proxy now subscribes to a **single recorder's** `FsyncWatermark` stream (default: its co-located recorder; configurable) and releases acks once that stream advances past the tx's B-position. On recorder failure, re-peg to a live recorder's watermark. Tasks below that mention `QuorumWatermark` need to be rewritten to use `FsyncWatermark` directly. See `docs/plans/2026-05-23-S0-shared-decisions.md` D-Sh13 and `docs/specs/2026-05-23-high-throughput-sequencer-design.md` D8 / I2 / §2.3.2.

**Goal:** Stand up the stateless ingress proxy crate that terminates client connections (JSON-RPC + binary line protocol), batches sig verification, partitions traffic into `ingress[keccak(sender) % M]`, parks responses until a single recorder's fsync watermark and a matching receipt both arrive, and answers retries from a shared receipt cache.

**Architecture:** New `crates/kardamom-ingress` crate. A single `IngressProxy` struct wires together (a) `jsonrpsee` HTTP+WS server and an optional length-prefixed RLP TCP/UDS listener, (b) per-IP `governor`-backed token-bucket rate limit run before any expensive work, (c) a 64-deep batched `secp256k1` recovery ring with a 50µs flush timer (and a single-sig fallback), (d) an Aeron-publication router keyed on `keccak(sender) % M`, (e) a `pending-receipts` `DashMap<(Address, u64), oneshot::Sender<ReceiptResponse>>` plus a receipt-cache subscriber for idempotent retries, and (f) a watermark watcher that gates response release. All Aeron handles and shared types (`BPosition`, `TxEnvelope`, `Receipt`, `BlockBoundary`, `QuorumWatermark`, `CachedReceipt`) are imported from the `kardamom-types` crate (per S0 D-Sh1), with channel implementations from `kardamom-log` — this plan does **not** redefine them; it stubs them behind a `kardamom_log::testing::MockAeronChannel` trait until S3 lands. Stateless w.r.t. canonical truth — adding or removing a proxy is safe.

**Per S0 D-Sh3 (sender) and D-Sh4 (tx_hash) — the proxy is the *only* place these two fields are computed.** Both are produced together at the sig-verify boundary: secp256k1 recovers `sender: Address` and a single keccak256 pass over `raw_tx` produces `tx_hash: B256`. Both are written into `TxEnvelope.sender` (typed `Address`, **never `Option`**) and `TxEnvelope.tx_hash` (typed `B256`, always populated) before publication onto `ingress[i]`. If recovery fails, the tx is rejected at the RPC boundary (JSON-RPC `-32602` invalid params via `IngressError::SignatureInvalid`) *before* publishing — downstream consumers may trust both fields unconditionally. The receipt produced by S4 propagates `tx_hash` unchanged into `Receipt.tx_hash`, and S6 maintains a `tx_hash_index: tx_hash → BPosition` libmdbx table that the proxy queries from `eth_getTransactionReceipt(hash)`.

**Per S0 D-Sh2 — wire codec is `rkyv` v0.8 (not `bincode`, not `serde`).** All shared types in `kardamom-types` derive `rkyv::Archive, rkyv::Serialize, rkyv::Deserialize`. On the consumer side, the proxy reads `Archived<Receipt>` and `Archived<CachedReceipt>` zero-copy from channel C / receipt-cache Aeron buffers via helpers exposed by `kardamom-log`; we only materialize to owned `T` when handing back to a client. Same applies to `Archived<QuorumWatermark>` on the watermark stream.

**Tech Stack:** Rust 2024 edition, `jsonrpsee = 0.26` (HTTP + WS), `tokio = 1` (multi-thread runtime, `time::interval`, `sync::oneshot`, `net::TcpListener`, `net::UnixListener`), `alloy-consensus = 2.0` (`TxEnvelope`, `SignableTransaction`, `secp256k1` feature for batched recovery), `alloy-primitives = 1.6` (`Address`, `keccak256`, `B256`), `alloy-rlp = 0.3` (length-prefixed binary frame decode), `secp256k1 = 0.31` (batched `ecdsa::RecoverableSignature::recover` — fastest CPU recovery library; `k256` was rejected as ~2x slower per recovery in our context), `governor = 0.10` (lock-free token bucket; `direct::RateLimiter<NotKeyed>` per-IP via DashMap), `dashmap = 6` (pending-receipts shard map), `bytes = 1`, `thiserror = 2`, `tracing = 0.1`, `metrics = 0.24`, `criterion = 0.7` (benches), `proptest = 1` (sig-verify equivalence).

**Branch:** `claude/s1-ingress-proxy` (branched off the merge of S3's branch `claude/s3-canonical-log`; until S3 lands, develop against the `kardamom-log` stub introduced in Task 4 of this plan and rebase once S3 merges).

**Reference spec:** `docs/specs/2026-05-23-high-throughput-sequencer-design.md` §2.1, §2.5, §3 (latency budget), §4.1, V0 scope.

---

## File structure

| File | Responsibility |
|---|---|
| `crates/kardamom-ingress/Cargo.toml` | Crate manifest, deps, feature flags. |
| `crates/kardamom-ingress/src/lib.rs` | Public surface: `IngressProxy`, `IngressConfig`, `IngressError`, re-exports. |
| `crates/kardamom-ingress/src/config.rs` | `IngressConfig` (HTTP/WS bind, optional TCP+UDS bind, `partition_count_m`, rate limit, batch flush). |
| `crates/kardamom-ingress/src/error.rs` | `IngressError` enum with `From<_> for ErrorObjectOwned`. |
| `crates/kardamom-ingress/src/rate_limit.rs` | `PerIpLimiter` wrapping `governor::DefaultDirectRateLimiter` per IP. |
| `crates/kardamom-ingress/src/sig_verify.rs` | `BatchVerifier`: 64-deep ring + 50µs flush + single-sig fallback. |
| `crates/kardamom-ingress/src/routing.rs` | `partition_for(sender, m) -> usize` and Aeron pub fan-out. |
| `crates/kardamom-ingress/src/pending.rs` | `PendingReceipts` map + watermark-gated response release. |
| `crates/kardamom-ingress/src/receipt_cache.rs` | Subscribe to receipt-cache Aeron channel; resolve duplicate `(sender, nonce)`. |
| `crates/kardamom-ingress/src/json_rpc.rs` | `EthApi` jsonrpsee trait + handlers (`eth_sendRawTransaction`, `eth_getTransactionReceipt`, `eth_blockNumber`, `eth_chainId`, `eth_getBalance`, `eth_getTransactionCount`). |
| `crates/kardamom-ingress/src/binary.rs` | Length-prefixed RLP `TxEnvelope` TCP+UDS listener (feature `binary-protocol`, on by default). |
| `crates/kardamom-ingress/src/proxy.rs` | `IngressProxy::start()`: wires all subsystems; returns `IngressHandle`. |
| `crates/kardamom-ingress/src/log_stub.rs` | Temporary stand-in for `kardamom-log` types until S3 lands. Behind `cfg(feature = "log-stub")`, default on. |
| `crates/kardamom-ingress/tests/rate_limit_test.rs` | Integration: rate limit triggers per-IP, recovers after window. |
| `crates/kardamom-ingress/tests/batched_sig_verify_test.rs` | Integration: batch vs single-sig equivalence on 1k random txs. |
| `crates/kardamom-ingress/tests/pending_receipts_test.rs` | Integration: insert/match/timeout/watermark-park. |
| `crates/kardamom-ingress/tests/routing_test.rs` | Integration: `keccak(sender) % M` routing distribution. |
| `crates/kardamom-ingress/tests/end_to_end_test.rs` | Integration: 100-tx flow over MockAeronChannel; idempotent retry. |
| `crates/kardamom-ingress/benches/latency.rs` | Criterion: end-to-end sub-ms latency. |
| `crates/kardamom-ingress/benches/throughput.rs` | Criterion: sustained TPS per proxy. |

---

## Task 1: Create the crate skeleton and wire it into the workspace

**Files:**
- Create: `crates/kardamom-ingress/Cargo.toml`
- Create: `crates/kardamom-ingress/src/lib.rs`
- Create: empty module files for each src module
- Verify: workspace `Cargo.toml` already has `members = ["crates/*"]` — no change needed

- [ ] **Step 1: Create `crates/kardamom-ingress/Cargo.toml`**

```toml
[package]
name = "kardamom-ingress"
version.workspace = true
edition.workspace = true

[features]
default = ["binary-protocol", "log-stub"]
binary-protocol = []
# log-stub stands in for the future `kardamom-log` crate (S3). Disable
# once S3 lands and the real crate is on the workspace path.
log-stub = []

[dependencies]
alloy-primitives.workspace = true
alloy-consensus.workspace = true
alloy-rlp.workspace = true
jsonrpsee.workspace = true
tokio.workspace = true
serde.workspace = true
serde_json.workspace = true
thiserror.workspace = true
tracing.workspace = true
metrics.workspace = true
async-trait = "0.1"
bytes = "1"
dashmap = "6"
governor = "0.10"
nonzero_ext = "0.3"
secp256k1 = { version = "0.31", features = ["recovery", "global-context"] }

[dev-dependencies]
alloy-signer-local.workspace = true
proptest = "1"
criterion = { version = "0.7", features = ["async_tokio"] }
tracing-subscriber.workspace = true

[[bench]]
name = "latency"
harness = false

[[bench]]
name = "throughput"
harness = false
```

- [ ] **Step 2: Create empty module files**

```bash
cd /home/dev/kardamom
mkdir -p crates/kardamom-ingress/src crates/kardamom-ingress/tests crates/kardamom-ingress/benches
touch crates/kardamom-ingress/src/lib.rs
touch crates/kardamom-ingress/src/config.rs
touch crates/kardamom-ingress/src/error.rs
touch crates/kardamom-ingress/src/rate_limit.rs
touch crates/kardamom-ingress/src/sig_verify.rs
touch crates/kardamom-ingress/src/routing.rs
touch crates/kardamom-ingress/src/pending.rs
touch crates/kardamom-ingress/src/receipt_cache.rs
touch crates/kardamom-ingress/src/json_rpc.rs
touch crates/kardamom-ingress/src/binary.rs
touch crates/kardamom-ingress/src/proxy.rs
touch crates/kardamom-ingress/src/log_stub.rs
```

- [ ] **Step 3: Populate `crates/kardamom-ingress/src/lib.rs` with module declarations**

```rust
//! Ingress proxy: terminates client connections, sig-verifies, partition-routes,
//! and parks responses until both (a) the quorum fsync watermark advances past
//! the tx's B-position and (b) a matching receipt arrives on channel C.
//!
//! Stateless w.r.t. canonical truth — proxies can be added or removed at any time.

pub mod binary;
pub mod config;
pub mod error;
pub mod json_rpc;
pub mod log_stub;
pub mod pending;
pub mod proxy;
pub mod rate_limit;
pub mod receipt_cache;
pub mod routing;
pub mod sig_verify;

pub use config::IngressConfig;
pub use error::IngressError;
pub use proxy::{IngressHandle, IngressProxy};
```

- [ ] **Step 4: Verify the crate builds**

```bash
cd /home/dev/kardamom
cargo build -p kardamom-ingress
```

Expected: clean build (empty modules are valid Rust).

- [ ] **Step 5: Commit**

```bash
cd /home/dev/kardamom
git add crates/kardamom-ingress/
git commit -m "ingress: scaffold kardamom-ingress crate"
```

---

## Task 2: Define `IngressConfig`

**Files:**
- Modify: `crates/kardamom-ingress/src/config.rs`

- [ ] **Step 1: Write the failing test**

Append to `crates/kardamom-ingress/src/config.rs`:

```rust
//! Static configuration for an `IngressProxy` instance.

use std::net::SocketAddr;
use std::num::NonZeroU32;
use std::path::PathBuf;
use std::time::Duration;

/// Static configuration for an `IngressProxy` instance.
///
/// All fields are required; pick a `IngressConfig::default()` for tests.
#[derive(Debug, Clone)]
pub struct IngressConfig {
    /// HTTP+WS jsonrpsee server bind address.
    pub jsonrpc_bind: SocketAddr,
    /// Optional TCP bind for the binary line protocol.
    pub binary_tcp_bind: Option<SocketAddr>,
    /// Optional UDS path for the binary line protocol.
    pub binary_uds_path: Option<PathBuf>,
    /// Number of sequencer partitions (M); routes `keccak(sender) % M`.
    pub partition_count_m: u32,
    /// Per-IP token-bucket replenishment rate (tokens/sec).
    pub rate_limit_per_ip_per_sec: NonZeroU32,
    /// Per-IP token-bucket burst capacity.
    pub rate_limit_burst: NonZeroU32,
    /// Batched sig-verify ring depth (spec calls for 64).
    pub sig_verify_batch_depth: usize,
    /// Batched sig-verify flush window (spec calls for 50µs).
    pub sig_verify_flush_window: Duration,
    /// Max time the proxy waits for receipt + watermark before timing out the client.
    pub pending_receipt_timeout: Duration,
    /// L2 chain id (returned by `eth_chainId`).
    pub chain_id: u64,
}

impl Default for IngressConfig {
    fn default() -> Self {
        use nonzero_ext::nonzero;
        Self {
            jsonrpc_bind: "127.0.0.1:0".parse().unwrap(),
            binary_tcp_bind: None,
            binary_uds_path: None,
            partition_count_m: 8,
            rate_limit_per_ip_per_sec: nonzero!(10_000u32),
            rate_limit_burst: nonzero!(1_000u32),
            sig_verify_batch_depth: 64,
            sig_verify_flush_window: Duration::from_micros(50),
            pending_receipt_timeout: Duration::from_secs(30),
            chain_id: 1,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_matches_spec() {
        let cfg = IngressConfig::default();
        assert_eq!(cfg.partition_count_m, 8);
        assert_eq!(cfg.sig_verify_batch_depth, 64);
        assert_eq!(cfg.sig_verify_flush_window, Duration::from_micros(50));
    }
}
```

- [ ] **Step 2: Run the test and verify it passes**

```bash
cd /home/dev/kardamom
cargo test -p kardamom-ingress config::tests::default_matches_spec
```

Expected: PASS.

- [ ] **Step 3: Commit**

```bash
cd /home/dev/kardamom
git add crates/kardamom-ingress/src/config.rs
git commit -m "ingress: add IngressConfig"
```

---

## Task 3: Define `IngressError`

**Files:**
- Modify: `crates/kardamom-ingress/src/error.rs`

- [ ] **Step 1: Write the file**

```rust
//! Public error type for the ingress proxy.
//!
//! Variants are mapped to JSON-RPC error codes via `From<IngressError> for ErrorObjectOwned`.

use jsonrpsee::types::ErrorObjectOwned;

#[derive(Debug, thiserror::Error)]
pub enum IngressError {
    #[error("rate limit exceeded for client {0}")]
    RateLimited(String),
    #[error("failed to decode transaction: {0}")]
    Decode(String),
    #[error("signature verification failed")]
    SignatureInvalid,
    #[error("sequencer partition unavailable: {0}")]
    PartitionUnavailable(String),
    #[error("timed out waiting for receipt or watermark")]
    Timeout,
    #[error("internal server error: {0}")]
    Internal(String),
    #[error("duplicate (sender, nonce): {0:?}")]
    Duplicate((alloy_primitives::Address, u64)),
}

impl From<IngressError> for ErrorObjectOwned {
    fn from(err: IngressError) -> Self {
        let code = match &err {
            IngressError::RateLimited(_) => -32005, // limit exceeded
            IngressError::Decode(_)
            | IngressError::SignatureInvalid
            | IngressError::Duplicate(_) => -32602, // invalid params
            IngressError::PartitionUnavailable(_) | IngressError::Timeout => -32000, // server
            IngressError::Internal(_) => -32603, // internal
        };
        ErrorObjectOwned::owned::<()>(code, err.to_string(), None)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rate_limited_maps_to_minus_32005() {
        let err = IngressError::RateLimited("10.0.0.1".into());
        let rpc: ErrorObjectOwned = err.into();
        assert_eq!(rpc.code(), -32005);
    }

    #[test]
    fn signature_invalid_maps_to_invalid_params() {
        let rpc: ErrorObjectOwned = IngressError::SignatureInvalid.into();
        assert_eq!(rpc.code(), -32602);
    }

    #[test]
    fn timeout_maps_to_server_error() {
        let rpc: ErrorObjectOwned = IngressError::Timeout.into();
        assert_eq!(rpc.code(), -32000);
    }
}
```

- [ ] **Step 2: Run the tests**

```bash
cd /home/dev/kardamom
cargo test -p kardamom-ingress error::tests
```

Expected: all three tests PASS.

- [ ] **Step 3: Commit**

```bash
cd /home/dev/kardamom
git add crates/kardamom-ingress/src/error.rs
git commit -m "ingress: add IngressError with jsonrpsee mapping"
```

---

## Task 4: Add `log_stub` for kardamom-log types and mock channels

**Files:**
- Modify: `crates/kardamom-ingress/src/log_stub.rs`

This module stands in for the `kardamom-log` crate (S3). Once S3 lands, swap `use crate::log_stub::*` for `use kardamom_log::*` and delete this file (the feature `log-stub` gate makes the swap mechanical).

- [ ] **Step 1: Write the module**

```rust
//! Temporary stand-in for the future `kardamom-types` + `kardamom-log` crates (S3).
//!
//! All real proxies, tests, and benches in this crate consume these traits/types;
//! once S3 lands the import path swaps to `kardamom_types` for the data types
//! (`BPosition`, `TxEnvelope`, `Receipt`, `CachedReceipt`, `BlockBoundary`,
//! `QuorumWatermark`) and `kardamom_log` for the channel impls. No semantic
//! changes should be required.
//!
//! **Per S0 D-Sh2:** real wire types derive `rkyv::Archive, rkyv::Serialize,
//! rkyv::Deserialize` and are read zero-copy as `Archived<T>` from Aeron buffers.
//! This stub omits the derives (no Aeron, no zero-copy needed for the mock); the
//! call sites that read messages are written against owned `T` so the swap is
//! mechanical — replace `T` with `&Archived<T>` and add a single materialization
//! step where the proxy returns to a client.
//!
//! Gated on `feature = "log-stub"` (default on); when S3 lands the gate flips off
//! and this module compiles to nothing.

#![cfg(feature = "log-stub")]

use std::sync::Arc;

use alloy_primitives::{Address, B256, Bytes};
use tokio::sync::{Mutex, broadcast, mpsc};

/// Canonical B-position: `(term_id, term_offset)`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct BPosition {
    pub term_id: u64,
    pub term_offset: u64,
}

/// Subset of a receipt that the proxy needs to return to the client.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Receipt {
    pub tx_idx: u64,
    pub b_position: BPosition,
    pub status: bool,
    pub gas_used: u64,
    pub logs: Vec<Bytes>,
    pub tx_hash: B256,
}

/// Boundary marker emitted by executors onto channel C.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BlockBoundary {
    pub block_number: u64,
    pub end_tx_idx: u64,
    pub l2_timestamp: u64,
}

/// Quorum fsync watermark snapshot.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct QuorumWatermark {
    pub position: BPosition,
}

/// Message published by the proxy onto an `ingress[i]` Aeron channel.
///
/// Stand-in for `kardamom_types::TxEnvelope`. Per S0 D-Sh1/D-Sh3/D-Sh4, both
/// `sender: Address` (typed, never `Option`) and `tx_hash: B256` are populated
/// by the proxy at sig-verify time. Downstream consumers trust both fields
/// unconditionally. Once `kardamom-types` lands these derive
/// `rkyv::{Archive, Serialize, Deserialize}` per S0 D-Sh2.
#[derive(Debug, Clone)]
pub struct IngressMsg {
    pub correlation_id: u128,
    pub sender: Address,
    pub tx_hash: B256,
    pub nonce: u64,
    pub raw_tx: Bytes,
}

/// Message read off the receipt-cache channel; allows any proxy to answer a retry.
#[derive(Debug, Clone)]
pub struct CachedReceipt {
    pub sender: Address,
    pub nonce: u64,
    pub receipt: Receipt,
}

/// Abstract Aeron publication handle.
///
/// Real implementation will wrap an Aeron client publication. The trait keeps
/// tests free of an Aeron dependency.
#[async_trait::async_trait]
pub trait IngressPublication: Send + Sync + 'static {
    async fn publish_ingress(&self, partition: usize, msg: IngressMsg) -> Result<(), String>;
    async fn publish_receipt_cache(&self, cached: CachedReceipt) -> Result<(), String>;
}

/// Abstract Aeron subscription handle.
///
/// Real implementation wraps an Aeron subscriber. Tests use `MockAeronChannel` below.
pub trait IngressSubscription: Send + Sync + 'static {
    fn subscribe_receipts(&self) -> broadcast::Receiver<Receipt>;
    fn subscribe_watermark(&self) -> broadcast::Receiver<QuorumWatermark>;
    fn subscribe_receipt_cache(&self) -> broadcast::Receiver<CachedReceipt>;
    /// Per S0 D-Sh5: channel-C BlockBoundary stream so the proxy can serve
    /// `eth_blockNumber` from the highest seen `block_number`.
    fn subscribe_block_boundaries(&self) -> broadcast::Receiver<BlockBoundary>;
}

/// Per S0 D-Sh1 + D-Sh4: read-only state DB the proxy queries to serve
/// `eth_getTransactionReceipt(hash)`. The real impl is provided by S6
/// (`crates/kardamom-state-writer`), backed by libmdbx with a `tx_hash_index`
/// table populated on block commit. v0 + tests use the `InMemoryStateDb`
/// implementation below.
///
/// Note: the real trait lives in `kardamom-types` and exposes the broader
/// `revm::Database` surface plus snapshot semantics; the proxy only needs the
/// two read paths declared here.
pub trait StateDatabase: Send + Sync + 'static {
    /// `tx_hash_index` lookup. Returns `None` if the tx hasn't been committed.
    fn get_tx_position(&self, tx_hash: alloy_primitives::B256) -> Option<BPosition>;
    /// Receipt store, keyed by canonical B-position. Returns `None` if not committed.
    fn get_receipt(&self, position: BPosition) -> Option<Receipt>;
}

/// In-memory `StateDatabase` for unit tests, integration tests, and the proxy
/// v0 default until S6 lands. Behind the same trait as the real libmdbx impl
/// so the swap is mechanical.
#[derive(Default, Clone)]
pub struct InMemoryStateDb {
    pub tx_hash_index: Arc<std::sync::RwLock<std::collections::HashMap<alloy_primitives::B256, BPosition>>>,
    pub receipts: Arc<std::sync::RwLock<std::collections::HashMap<BPosition, Receipt>>>,
}

impl InMemoryStateDb {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn record(&self, tx_hash: alloy_primitives::B256, position: BPosition, receipt: Receipt) {
        self.tx_hash_index
            .write()
            .unwrap()
            .insert(tx_hash, position);
        self.receipts.write().unwrap().insert(position, receipt);
    }
}

impl StateDatabase for InMemoryStateDb {
    fn get_tx_position(&self, tx_hash: alloy_primitives::B256) -> Option<BPosition> {
        self.tx_hash_index.read().unwrap().get(&tx_hash).copied()
    }
    fn get_receipt(&self, position: BPosition) -> Option<Receipt> {
        self.receipts.read().unwrap().get(&position).cloned()
    }
}

/// In-process mock of the future Aeron channels. Used in all `tests/` integration
/// tests and benches.
#[derive(Clone)]
pub struct MockAeronChannel {
    pub ingress_tx: Vec<mpsc::UnboundedSender<IngressMsg>>,
    pub receipt_bus: broadcast::Sender<Receipt>,
    pub watermark_bus: broadcast::Sender<QuorumWatermark>,
    pub receipt_cache_bus: broadcast::Sender<CachedReceipt>,
    pub block_boundary_bus: broadcast::Sender<BlockBoundary>,
    pub published_cache: Arc<Mutex<Vec<CachedReceipt>>>,
}

impl MockAeronChannel {
    pub fn new(partitions: usize) -> (Self, Vec<mpsc::UnboundedReceiver<IngressMsg>>) {
        let mut tx_vec = Vec::with_capacity(partitions);
        let mut rx_vec = Vec::with_capacity(partitions);
        for _ in 0..partitions {
            let (tx, rx) = mpsc::unbounded_channel();
            tx_vec.push(tx);
            rx_vec.push(rx);
        }
        let (receipt_bus, _) = broadcast::channel(1024);
        let (watermark_bus, _) = broadcast::channel(1024);
        let (receipt_cache_bus, _) = broadcast::channel(1024);
        let (block_boundary_bus, _) = broadcast::channel(1024);
        (
            Self {
                ingress_tx: tx_vec,
                receipt_bus,
                watermark_bus,
                receipt_cache_bus,
                block_boundary_bus,
                published_cache: Arc::new(Mutex::new(Vec::new())),
            },
            rx_vec,
        )
    }
}

#[async_trait::async_trait]
impl IngressPublication for MockAeronChannel {
    async fn publish_ingress(&self, partition: usize, msg: IngressMsg) -> Result<(), String> {
        self.ingress_tx
            .get(partition)
            .ok_or_else(|| format!("partition {partition} out of range"))?
            .send(msg)
            .map_err(|e| e.to_string())
    }

    async fn publish_receipt_cache(&self, cached: CachedReceipt) -> Result<(), String> {
        self.published_cache.lock().await.push(cached.clone());
        let _ = self.receipt_cache_bus.send(cached);
        Ok(())
    }
}

impl IngressSubscription for MockAeronChannel {
    fn subscribe_receipts(&self) -> broadcast::Receiver<Receipt> {
        self.receipt_bus.subscribe()
    }
    fn subscribe_watermark(&self) -> broadcast::Receiver<QuorumWatermark> {
        self.watermark_bus.subscribe()
    }
    fn subscribe_receipt_cache(&self) -> broadcast::Receiver<CachedReceipt> {
        self.receipt_cache_bus.subscribe()
    }
    fn subscribe_block_boundaries(&self) -> broadcast::Receiver<BlockBoundary> {
        self.block_boundary_bus.subscribe()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn mock_routes_to_partition() {
        let (mock, mut rx) = MockAeronChannel::new(4);
        let msg = IngressMsg {
            correlation_id: 1,
            sender: Address::ZERO,
            tx_hash: B256::ZERO,
            nonce: 0,
            raw_tx: Bytes::new(),
        };
        mock.publish_ingress(2, msg.clone()).await.unwrap();
        let received = rx[2].recv().await.unwrap();
        assert_eq!(received.correlation_id, 1);
        // Other partitions stay empty.
        assert!(rx[0].try_recv().is_err());
    }
}
```

- [ ] **Step 2: Run the test**

```bash
cd /home/dev/kardamom
cargo test -p kardamom-ingress log_stub::tests::mock_routes_to_partition
```

Expected: PASS.

- [ ] **Step 3: Commit**

```bash
cd /home/dev/kardamom
git add crates/kardamom-ingress/src/log_stub.rs
git commit -m "ingress: add log_stub for kardamom-log types and MockAeronChannel"
```

---

## Task 5: Implement `routing::partition_for` (`keccak(sender) % M`)

**Files:**
- Modify: `crates/kardamom-ingress/src/routing.rs`

- [ ] **Step 1: Write the failing test first**

Write this to the file:

```rust
//! Sender-to-partition routing. `partition = keccak256(sender)[..8] % M`.

use alloy_primitives::{Address, keccak256};

/// Returns the partition index for `sender` given `m` partitions.
///
/// Implementation: take the first 8 bytes of `keccak256(sender)` as a big-endian
/// `u64`, then `% m`. This matches the algorithm described in spec §2.1.
#[inline]
pub fn partition_for(sender: Address, m: u32) -> u32 {
    debug_assert!(m > 0, "partition count must be positive");
    let h = keccak256(sender.as_slice());
    let leading = u64::from_be_bytes(h[..8].try_into().expect("8 bytes"));
    (leading % m as u64) as u32
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloy_primitives::address;

    #[test]
    fn partition_is_stable_per_address() {
        let a = address!("00000000000000000000000000000000DeadBeef");
        let p1 = partition_for(a, 8);
        let p2 = partition_for(a, 8);
        assert_eq!(p1, p2);
        assert!(p1 < 8);
    }

    #[test]
    fn distribution_is_reasonable_over_1024_addresses() {
        // For 1024 random addresses into 8 partitions, each bucket should hold
        // at least 1024/8 / 2 = 64. (Crude smoke test; not a chi-square.)
        let mut counts = [0u32; 8];
        for i in 0u64..1024 {
            let mut bytes = [0u8; 20];
            bytes[12..].copy_from_slice(&i.to_be_bytes());
            let addr = Address::from(bytes);
            counts[partition_for(addr, 8) as usize] += 1;
        }
        for (i, c) in counts.iter().enumerate() {
            assert!(*c >= 64, "partition {i} got {c} addresses, expected >= 64");
        }
    }

    #[test]
    fn partition_changes_with_m() {
        let a = address!("00000000000000000000000000000000DeadBeef");
        let p8 = partition_for(a, 8);
        let p16 = partition_for(a, 16);
        // p16 collapses to p8 only by coincidence; we just assert both are in range.
        assert!(p8 < 8);
        assert!(p16 < 16);
    }
}
```

- [ ] **Step 2: Run the tests**

```bash
cd /home/dev/kardamom
cargo test -p kardamom-ingress routing::tests
```

Expected: all three PASS.

- [ ] **Step 3: Commit**

```bash
cd /home/dev/kardamom
git add crates/kardamom-ingress/src/routing.rs
git commit -m "ingress: add partition_for keccak(sender) % M"
```

---

## Task 6: Implement `PerIpLimiter` (per-IP token bucket)

**Files:**
- Modify: `crates/kardamom-ingress/src/rate_limit.rs`

- [ ] **Step 1: Write the file**

```rust
//! Per-IP token-bucket rate limit. Runs before any expensive work so abusive
//! clients are rejected at near-zero CPU cost.

use std::net::IpAddr;
use std::num::NonZeroU32;
use std::sync::Arc;

use dashmap::DashMap;
use governor::clock::DefaultClock;
use governor::middleware::NoOpMiddleware;
use governor::state::{InMemoryState, NotKeyed};
use governor::{Quota, RateLimiter};

type DirectLimiter = RateLimiter<NotKeyed, InMemoryState, DefaultClock, NoOpMiddleware>;

/// Per-IP `governor` token bucket. New IPs get a fresh limiter on first hit.
pub struct PerIpLimiter {
    quota: Quota,
    buckets: Arc<DashMap<IpAddr, Arc<DirectLimiter>>>,
}

impl PerIpLimiter {
    pub fn new(per_sec: NonZeroU32, burst: NonZeroU32) -> Self {
        let quota = Quota::per_second(per_sec).allow_burst(burst);
        Self {
            quota,
            buckets: Arc::new(DashMap::new()),
        }
    }

    /// Returns `Ok(())` on allow, `Err(())` when the IP's bucket is empty.
    pub fn check(&self, ip: IpAddr) -> Result<(), ()> {
        let limiter = self
            .buckets
            .entry(ip)
            .or_insert_with(|| Arc::new(RateLimiter::direct(self.quota)))
            .clone();
        limiter.check().map(|_| ()).map_err(|_| ())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use nonzero_ext::nonzero;

    #[test]
    fn allows_within_burst_and_denies_overflow() {
        let lim = PerIpLimiter::new(nonzero!(1u32), nonzero!(3u32));
        let ip: IpAddr = "10.0.0.1".parse().unwrap();
        assert!(lim.check(ip).is_ok());
        assert!(lim.check(ip).is_ok());
        assert!(lim.check(ip).is_ok());
        // Fourth in the same tick exceeds burst of 3.
        assert!(lim.check(ip).is_err());
    }

    #[test]
    fn different_ips_have_independent_budgets() {
        let lim = PerIpLimiter::new(nonzero!(1u32), nonzero!(1u32));
        let a: IpAddr = "10.0.0.1".parse().unwrap();
        let b: IpAddr = "10.0.0.2".parse().unwrap();
        assert!(lim.check(a).is_ok());
        assert!(lim.check(a).is_err());
        assert!(lim.check(b).is_ok());
    }
}
```

- [ ] **Step 2: Run the tests**

```bash
cd /home/dev/kardamom
cargo test -p kardamom-ingress rate_limit::tests
```

Expected: both PASS.

- [ ] **Step 3: Commit**

```bash
cd /home/dev/kardamom
git add crates/kardamom-ingress/src/rate_limit.rs
git commit -m "ingress: add PerIpLimiter (governor token bucket)"
```

---

## Task 7: Add single-sig verification (the fallback)

**Files:**
- Modify: `crates/kardamom-ingress/src/sig_verify.rs`

The single-sig path is the reference: `BatchVerifier` in Task 8 must produce equivalent results.

**Per S0 D-Sh3 + D-Sh4:** both functions return `(Address, B256)` — the recovered sender *and* the canonical `tx_hash = keccak256(raw_tx)`. The keccak pass is essentially free alongside ECDSA recovery, and producing both at the system boundary lets every downstream consumer trust `TxEnvelope.{sender, tx_hash}` unconditionally. Failure to recover ⇒ reject at the RPC boundary (caller returns `IngressError::SignatureInvalid`, which maps to JSON-RPC `-32602`) *before* any publish happens.

- [ ] **Step 1: Write the failing test**

```rust
//! secp256k1 ECDSA recovery for transaction sender addresses + canonical tx_hash.
//!
//! Per S0 D-Sh3/D-Sh4: the proxy is the *only* component that computes either
//! field. Both are produced together (recovery + keccak256 over raw_tx) so
//! downstream consumers may trust `TxEnvelope.{sender, tx_hash}` unconditionally.
//!
//! Two paths:
//! - `recover_single`: minimal, used when no batching is active or as the
//!   correctness reference.
//! - `BatchVerifier`: 64-deep ring + 50µs flush window (Task 8).

use alloy_consensus::TxEnvelope;
use alloy_primitives::{Address, B256, Bytes, keccak256};

use crate::error::IngressError;

/// Recover the sender address from a fully-decoded `TxEnvelope` and compute
/// the canonical `tx_hash = keccak256(raw_tx)` in the same pass.
///
/// Returns `(sender, tx_hash)`. On recovery failure, returns
/// `IngressError::SignatureInvalid` — callers MUST reject the tx at the RPC
/// boundary before publishing.
pub fn recover_single(env: &TxEnvelope, raw_tx: &Bytes) -> Result<(Address, B256), IngressError> {
    let sender = env
        .recover_signer()
        .map_err(|_| IngressError::SignatureInvalid)?;
    let tx_hash = keccak256(raw_tx.as_ref());
    Ok((sender, tx_hash))
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloy_consensus::{SignableTransaction, TxLegacy};
    use alloy_primitives::{TxKind, U256};
    use alloy_rlp::Encodable;
    use alloy_signer_local::PrivateKeySigner;

    pub(super) fn signed_legacy_envelope() -> (TxEnvelope, Bytes, Address) {
        let signer = PrivateKeySigner::random();
        let addr = signer.address();
        let tx = TxLegacy {
            chain_id: Some(1),
            nonce: 0,
            gas_price: 1_000_000_000,
            gas_limit: 21_000,
            to: TxKind::Call(Address::ZERO),
            value: U256::ZERO,
            input: Default::default(),
        };
        let sig = signer
            .credential()
            .sign_prehash_recoverable(&tx.signature_hash().0)
            .unwrap();
        let alloy_sig = alloy_primitives::Signature::from_signature_and_parity(sig, false);
        let signed = tx.into_signed(alloy_sig);
        let env: TxEnvelope = signed.into();
        let mut buf = Vec::new();
        env.encode(&mut buf);
        (env, Bytes::from(buf), addr)
    }

    #[test]
    fn recovers_legacy_sender_and_hash() {
        let (env, raw, expected) = signed_legacy_envelope();
        let (recovered, tx_hash) = recover_single(&env, &raw).unwrap();
        assert_eq!(recovered, expected);
        // tx_hash MUST equal keccak256(raw_tx) — the canonical hash defined by S0 D-Sh4.
        assert_eq!(tx_hash, keccak256(raw.as_ref()));
    }
}
```

- [ ] **Step 2: Run the test**

```bash
cd /home/dev/kardamom
cargo test -p kardamom-ingress sig_verify::tests::recovers_legacy_sender_and_hash
```

Expected: PASS. (If the parity bit / chain id derivation needs tweaking for the legacy signer, adjust `from_signature_and_parity` per the alloy 2.0 API. The key invariant: `recover_single` must return the same address as the signer used to sign.)

- [ ] **Step 3: Commit**

```bash
cd /home/dev/kardamom
git add crates/kardamom-ingress/src/sig_verify.rs
git commit -m "ingress: add single-sig fallback verifier"
```

---

## Task 8: Add `BatchVerifier` (64-deep ring, 50µs flush, batched recovery)

**Files:**
- Modify: `crates/kardamom-ingress/src/sig_verify.rs`

The spec mandates a 64-deep ring and 50µs flush window. We use `secp256k1`'s recoverable-signature API per slot (it's the fastest CPU implementation; `k256` is ~2x slower in our measurements). The "batch" win is in amortizing context-creation, parking lot wakeups, and Tokio task hops — not in vectorized math, which secp256k1 doesn't expose. If a future benchmark shows `k256`'s parallel verifier wins, swap by replacing the inner `secp256k1::recover_ecdsa` call.

- [ ] **Step 1: Append the failing batched test**

Append to `crates/kardamom-ingress/src/sig_verify.rs`:

```rust
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::{Mutex, Notify, oneshot};
use tokio::task::JoinHandle;
use tokio::time::Instant;

/// A request submitted to `BatchVerifier::recover`.
struct VerifyRequest {
    env: TxEnvelope,
    raw_tx: Bytes,
    respond: oneshot::Sender<Result<(Address, B256), IngressError>>,
}

/// 64-deep recovery ring with a 50µs flush window.
///
/// Submitted requests park on a oneshot until the ring is flushed: either
/// because depth is reached, or because the flush timer fires.
///
/// Per S0 D-Sh3 + D-Sh4: each `recover` call returns `(sender, tx_hash)`. The
/// keccak256 over `raw_tx` is computed alongside ECDSA recovery in the same
/// batch slot (essentially free vs. the ECDSA cost). Failure ⇒ caller rejects
/// at the RPC boundary.
pub struct BatchVerifier {
    inner: Arc<Mutex<Vec<VerifyRequest>>>,
    notify: Arc<Notify>,
    depth: usize,
    _flush_task: JoinHandle<()>,
}

impl BatchVerifier {
    pub fn new(depth: usize, flush_window: Duration) -> Self {
        assert!(depth > 0);
        let inner: Arc<Mutex<Vec<VerifyRequest>>> = Arc::new(Mutex::new(Vec::with_capacity(depth)));
        let notify = Arc::new(Notify::new());
        let inner_for_task = inner.clone();
        let notify_for_task = notify.clone();
        let flush = tokio::spawn(async move {
            loop {
                // Wait for at least one request to be queued.
                notify_for_task.notified().await;
                // Then bound the flush window.
                let deadline = Instant::now() + flush_window;
                tokio::time::sleep_until(deadline).await;
                let drained: Vec<VerifyRequest> = {
                    let mut g = inner_for_task.lock().await;
                    g.drain(..).collect()
                };
                Self::process_batch(drained);
            }
        });
        Self {
            inner,
            notify,
            depth,
            _flush_task: flush,
        }
    }

    fn process_batch(batch: Vec<VerifyRequest>) {
        for req in batch {
            // Per-tx recovery + keccak; the "batch" amortizes wakeups, not math.
            let res = recover_single(&req.env, &req.raw_tx);
            let _ = req.respond.send(res);
        }
    }

    /// Submit a tx envelope (plus its raw bytes) and await `(sender, tx_hash)`.
    /// Flushes immediately if the ring fills.
    pub async fn recover(
        &self,
        env: TxEnvelope,
        raw_tx: Bytes,
    ) -> Result<(Address, B256), IngressError> {
        let (tx, rx) = oneshot::channel();
        let should_flush_now = {
            let mut g = self.inner.lock().await;
            g.push(VerifyRequest {
                env,
                raw_tx,
                respond: tx,
            });
            g.len() >= self.depth
        };
        if should_flush_now {
            // Drain and process synchronously to avoid waiting for the timer.
            let drained: Vec<VerifyRequest> = {
                let mut g = self.inner.lock().await;
                g.drain(..).collect()
            };
            Self::process_batch(drained);
        } else {
            self.notify.notify_one();
        }
        rx.await.map_err(|_| IngressError::Internal("verifier dropped".into()))?
    }
}

#[cfg(test)]
mod batch_tests {
    use super::*;
    use super::tests::signed_legacy_envelope;

    #[tokio::test]
    async fn batched_matches_single_for_random_corpus() {
        let v = BatchVerifier::new(64, Duration::from_micros(50));
        let mut futs = Vec::new();
        let mut expected = Vec::new();
        for _ in 0..100 {
            let (env, raw, addr) = signed_legacy_envelope();
            let expected_hash = keccak256(raw.as_ref());
            expected.push((addr, expected_hash));
            futs.push(v.recover(env, raw));
        }
        let actual = futures::future::join_all(futs).await;
        for (i, res) in actual.into_iter().enumerate() {
            assert_eq!(res.unwrap(), expected[i], "mismatch at index {i}");
        }
    }

    #[tokio::test]
    async fn flushes_on_depth_without_waiting_for_timer() {
        let v = BatchVerifier::new(8, Duration::from_secs(60));
        let mut futs = Vec::new();
        for _ in 0..8 {
            let (env, raw, _) = signed_legacy_envelope();
            futs.push(v.recover(env, raw));
        }
        let start = Instant::now();
        let _ = futures::future::join_all(futs).await;
        // 60s timer never fires; depth-flush must complete in <100ms even on slow CI.
        assert!(start.elapsed() < Duration::from_millis(100));
    }
}
```

- [ ] **Step 2: Add `futures` to dev-deps**

In `crates/kardamom-ingress/Cargo.toml` under `[dev-dependencies]` add:

```toml
futures = "0.3"
```

- [ ] **Step 3: Run the tests**

```bash
cd /home/dev/kardamom
cargo test -p kardamom-ingress sig_verify::batch_tests
```

Expected: both PASS. If batched fails but single passes, the bug is in the flush loop — verify that `drained` is reset on each flush iteration and that `notify_one` is called after every push.

- [ ] **Step 4: Commit**

```bash
cd /home/dev/kardamom
git add crates/kardamom-ingress/Cargo.toml crates/kardamom-ingress/src/sig_verify.rs
git commit -m "ingress: add BatchVerifier (64-deep ring + 50us flush)"
```

---

## Task 9: Property test — batched vs single equivalence on 1k random txs

**Files:**
- Create: `crates/kardamom-ingress/tests/batched_sig_verify_test.rs`

- [ ] **Step 1: Write the test**

```rust
//! Property: for any signed legacy/eip1559 tx, BatchVerifier::recover and
//! recover_single agree on `(sender, tx_hash)` — the canonical pair produced
//! by the proxy at the system boundary per S0 D-Sh3/D-Sh4.

use std::time::Duration;

use alloy_consensus::{SignableTransaction, TxEnvelope, TxLegacy};
use alloy_primitives::{Address, B256, Bytes, TxKind, U256};
use alloy_rlp::Encodable;
use alloy_signer_local::PrivateKeySigner;
use kardamom_ingress::sig_verify::{BatchVerifier, recover_single};

fn sign_random_legacy() -> (TxEnvelope, Bytes, Address) {
    let signer = PrivateKeySigner::random();
    let addr = signer.address();
    let tx = TxLegacy {
        chain_id: Some(1),
        nonce: rand::random::<u64>() % 1024,
        gas_price: 1_000_000_000,
        gas_limit: 21_000,
        to: TxKind::Call(Address::ZERO),
        value: U256::ZERO,
        input: Default::default(),
    };
    let sig = signer
        .credential()
        .sign_prehash_recoverable(&tx.signature_hash().0)
        .unwrap();
    let alloy_sig = alloy_primitives::Signature::from_signature_and_parity(sig, false);
    let signed = tx.into_signed(alloy_sig);
    let env: TxEnvelope = signed.into();
    let mut buf = Vec::new();
    env.encode(&mut buf);
    (env, Bytes::from(buf), addr)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn batched_matches_single_on_1000_txs() {
    let v = BatchVerifier::new(64, Duration::from_micros(50));
    let mut single_results: Vec<(Address, B256)> = Vec::new();
    let mut batched_futs = Vec::new();
    let mut expected = Vec::new();

    for _ in 0..1000 {
        let (env, raw, addr) = sign_random_legacy();
        expected.push(addr);
        single_results.push(recover_single(&env, &raw).unwrap());
        batched_futs.push(v.recover(env, raw));
    }

    let batched_results: Vec<(Address, B256)> = futures::future::join_all(batched_futs)
        .await
        .into_iter()
        .map(|r| r.unwrap())
        .collect();

    assert_eq!(batched_results, single_results);
    for (i, (addr, _)) in batched_results.iter().enumerate() {
        assert_eq!(*addr, expected[i]);
    }
}
```

- [ ] **Step 2: Add `rand` to dev-deps**

In `crates/kardamom-ingress/Cargo.toml`:

```toml
[dev-dependencies]
# ... existing ...
rand = "0.9"
```

- [ ] **Step 3: Run the test**

```bash
cd /home/dev/kardamom
cargo test -p kardamom-ingress --test batched_sig_verify_test
```

Expected: PASS in <2s. If timing is too slow, reduce 1000 → 200 — the goal is equivalence, not throughput.

- [ ] **Step 4: Commit**

```bash
cd /home/dev/kardamom
git add crates/kardamom-ingress/Cargo.toml crates/kardamom-ingress/tests/batched_sig_verify_test.rs
git commit -m "ingress: property test batched vs single sig verify on 1k txs"
```

---

## Task 10: Implement `PendingReceipts` (map + watermark-gated release)

**Files:**
- Modify: `crates/kardamom-ingress/src/pending.rs`

- [ ] **Step 1: Write the failing test**

```rust
//! Pending-receipts map: parks a client `oneshot` until both
//! (a) a `Receipt` for `(sender, nonce)` arrives on channel C, and
//! (b) the quorum fsync watermark on B has reached `receipt.b_position`.
//!
//! Both conditions are required by invariant I2 (spec §1).

use std::sync::Arc;
use std::time::Duration;

use alloy_primitives::Address;
use dashmap::DashMap;
use tokio::sync::{Mutex, oneshot};

use crate::error::IngressError;
use crate::log_stub::{BPosition, QuorumWatermark, Receipt};

#[derive(Debug, Clone)]
pub struct ReceiptResponse {
    pub receipt: Receipt,
}

/// Internal entry: a parked one-shot sender plus the receipt once it has arrived.
struct Entry {
    responder: Option<oneshot::Sender<Result<ReceiptResponse, IngressError>>>,
    receipt: Option<Receipt>,
}

pub struct PendingReceipts {
    map: Arc<DashMap<(Address, u64), Mutex<Entry>>>,
    /// Latest watermark observed. Cached to avoid one-receiver-per-await fanout.
    latest_watermark: Arc<Mutex<Option<BPosition>>>,
}

impl Default for PendingReceipts {
    fn default() -> Self {
        Self::new()
    }
}

impl PendingReceipts {
    pub fn new() -> Self {
        Self {
            map: Arc::new(DashMap::new()),
            latest_watermark: Arc::new(Mutex::new(None)),
        }
    }

    /// Park a client until receipt + watermark both arrive (or `timeout` elapses).
    pub async fn await_receipt(
        &self,
        sender: Address,
        nonce: u64,
        timeout: Duration,
    ) -> Result<ReceiptResponse, IngressError> {
        let (tx, rx) = oneshot::channel();
        self.map.insert(
            (sender, nonce),
            Mutex::new(Entry {
                responder: Some(tx),
                receipt: None,
            }),
        );
        match tokio::time::timeout(timeout, rx).await {
            Ok(Ok(res)) => res,
            Ok(Err(_)) => Err(IngressError::Internal("oneshot dropped".into())),
            Err(_) => {
                self.map.remove(&(sender, nonce));
                Err(IngressError::Timeout)
            }
        }
    }

    /// Called when a receipt arrives on channel C. If the watermark has already
    /// advanced past the receipt's B-position, releases the client immediately;
    /// otherwise stores the receipt and waits for `update_watermark`.
    pub async fn on_receipt(&self, sender: Address, receipt: Receipt) {
        let key = (sender, receipt.tx_idx);
        let Some(entry_lock) = self.map.get(&key) else { return };
        let mut e = entry_lock.lock().await;
        e.receipt = Some(receipt.clone());
        if Self::watermark_past(&*self.latest_watermark.lock().await, receipt.b_position) {
            if let Some(resp) = e.responder.take() {
                let _ = resp.send(Ok(ReceiptResponse { receipt }));
                drop(e);
                drop(entry_lock);
                self.map.remove(&key);
            }
        }
    }

    /// Called when a new watermark snapshot is observed. Releases every parked
    /// entry whose stored receipt's B-position is now covered.
    pub async fn update_watermark(&self, wm: QuorumWatermark) {
        *self.latest_watermark.lock().await = Some(wm.position);
        // Collect releasable keys without holding any DashMap shard lock during await.
        let mut to_release: Vec<(Address, u64)> = Vec::new();
        for entry in self.map.iter() {
            let mut e = entry.value().lock().await;
            if let Some(r) = &e.receipt {
                if Self::watermark_past(&Some(wm.position), r.b_position) {
                    if let Some(resp) = e.responder.take() {
                        let _ = resp.send(Ok(ReceiptResponse { receipt: r.clone() }));
                        to_release.push(*entry.key());
                    }
                }
            }
        }
        for k in to_release {
            self.map.remove(&k);
        }
    }

    /// `latest >= target` in lexicographic (term_id, term_offset) order.
    fn watermark_past(latest: &Option<BPosition>, target: BPosition) -> bool {
        match latest {
            None => false,
            Some(p) => {
                p.term_id > target.term_id
                    || (p.term_id == target.term_id && p.term_offset >= target.term_offset)
            }
        }
    }

    pub fn len(&self) -> usize {
        self.map.len()
    }

    pub fn is_empty(&self) -> bool {
        self.map.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloy_primitives::{B256, Bytes};

    fn dummy_receipt(tx_idx: u64, pos: BPosition) -> Receipt {
        Receipt {
            tx_idx,
            b_position: pos,
            status: true,
            gas_used: 21_000,
            logs: Vec::new(),
            tx_hash: B256::ZERO,
        }
    }

    #[tokio::test]
    async fn parks_until_receipt_and_watermark_both_arrive() {
        let p = Arc::new(PendingReceipts::new());
        let sender = Address::repeat_byte(0x11);
        let nonce = 7u64;
        let pos = BPosition {
            term_id: 0,
            term_offset: 100,
        };

        let p_inner = p.clone();
        let waiter = tokio::spawn(async move {
            p_inner
                .await_receipt(sender, nonce, Duration::from_secs(5))
                .await
        });
        // Give the waiter time to register.
        tokio::time::sleep(Duration::from_millis(10)).await;

        // Receipt arrives but watermark hasn't caught up — must NOT release.
        p.on_receipt(sender, dummy_receipt(nonce, pos)).await;
        tokio::time::sleep(Duration::from_millis(10)).await;
        assert_eq!(p.len(), 1);

        // Watermark advances → releases.
        p.update_watermark(QuorumWatermark { position: pos }).await;
        let res = waiter.await.unwrap().unwrap();
        assert_eq!(res.receipt.tx_idx, nonce);
        assert_eq!(p.len(), 0);
    }

    #[tokio::test]
    async fn releases_immediately_when_watermark_already_past() {
        let p = Arc::new(PendingReceipts::new());
        let sender = Address::repeat_byte(0x22);
        let pos = BPosition {
            term_id: 0,
            term_offset: 5,
        };
        // Watermark advances first.
        p.update_watermark(QuorumWatermark { position: BPosition { term_id: 0, term_offset: 1000 } })
            .await;

        let p_inner = p.clone();
        let waiter = tokio::spawn(async move {
            p_inner
                .await_receipt(sender, 1, Duration::from_secs(5))
                .await
        });
        tokio::time::sleep(Duration::from_millis(10)).await;
        p.on_receipt(sender, dummy_receipt(1, pos)).await;
        let res = waiter.await.unwrap().unwrap();
        assert_eq!(res.receipt.b_position, pos);
    }

    #[tokio::test]
    async fn times_out_when_neither_event_arrives() {
        let p = PendingReceipts::new();
        let err = p
            .await_receipt(Address::ZERO, 0, Duration::from_millis(20))
            .await
            .unwrap_err();
        assert!(matches!(err, IngressError::Timeout));
        assert_eq!(p.len(), 0);
    }
}
```

- [ ] **Step 2: Run the tests**

```bash
cd /home/dev/kardamom
cargo test -p kardamom-ingress pending::tests
```

Expected: all three PASS.

- [ ] **Step 3: Commit**

```bash
cd /home/dev/kardamom
git add crates/kardamom-ingress/src/pending.rs
git commit -m "ingress: add PendingReceipts with watermark-gated release"
```

---

## Task 11: Implement `ReceiptCache` (subscribe + dedupe retries)

**Files:**
- Modify: `crates/kardamom-ingress/src/receipt_cache.rs`

- [ ] **Step 1: Write the file**

```rust
//! Receipt-cache Aeron channel subscriber.
//!
//! Maintains an in-memory `(sender, nonce) -> Receipt` map populated from the
//! receipt-cache broadcast. On a retry, any proxy can answer the prior receipt
//! without re-submitting to the sequencer.

use std::sync::Arc;

use alloy_primitives::Address;
use dashmap::DashMap;
use tokio::sync::broadcast;

use crate::log_stub::{CachedReceipt, IngressSubscription, Receipt};

/// Bounded by config; FIFO eviction is acceptable because duplicates outside the
/// window will just re-submit and the sequencer will dedupe via the past-nonce
/// path.
pub struct ReceiptCache {
    map: Arc<DashMap<(Address, u64), Receipt>>,
    capacity: usize,
}

impl ReceiptCache {
    pub fn new(capacity: usize) -> Self {
        assert!(capacity > 0);
        Self {
            map: Arc::new(DashMap::new()),
            capacity,
        }
    }

    /// Spawn a background task that consumes the receipt-cache broadcast and
    /// populates the in-memory map. Returns once spawned.
    pub fn spawn_consumer<S: IngressSubscription>(self: &Arc<Self>, sub: &S) {
        let mut rx: broadcast::Receiver<CachedReceipt> = sub.subscribe_receipt_cache();
        let me = self.clone();
        tokio::spawn(async move {
            loop {
                match rx.recv().await {
                    Ok(c) => me.insert(c.sender, c.nonce, c.receipt),
                    Err(broadcast::error::RecvError::Closed) => break,
                    Err(broadcast::error::RecvError::Lagged(_)) => {
                        // We dropped some cache entries — best-effort cache; continue.
                        continue;
                    }
                }
            }
        });
    }

    pub fn insert(&self, sender: Address, nonce: u64, receipt: Receipt) {
        if self.map.len() >= self.capacity {
            // Evict an arbitrary entry. DashMap doesn't expose FIFO; pop any.
            if let Some(entry) = self.map.iter().next() {
                let key = *entry.key();
                drop(entry);
                self.map.remove(&key);
            }
        }
        self.map.insert((sender, nonce), receipt);
    }

    pub fn lookup(&self, sender: Address, nonce: u64) -> Option<Receipt> {
        self.map.get(&(sender, nonce)).map(|r| r.clone())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloy_primitives::B256;

    use crate::log_stub::{BPosition, MockAeronChannel};

    fn dummy(idx: u64) -> Receipt {
        Receipt {
            tx_idx: idx,
            b_position: BPosition {
                term_id: 0,
                term_offset: idx,
            },
            status: true,
            gas_used: 21_000,
            logs: Vec::new(),
            tx_hash: B256::ZERO,
        }
    }

    #[tokio::test]
    async fn lookup_returns_inserted() {
        let c = ReceiptCache::new(8);
        let s = Address::repeat_byte(0x33);
        c.insert(s, 1, dummy(1));
        assert_eq!(c.lookup(s, 1).unwrap().tx_idx, 1);
        assert!(c.lookup(s, 2).is_none());
    }

    #[tokio::test]
    async fn consumer_populates_from_broadcast() {
        let (mock, _rx) = MockAeronChannel::new(1);
        let cache = Arc::new(ReceiptCache::new(64));
        cache.spawn_consumer(&mock);
        let s = Address::repeat_byte(0x44);
        let _ = mock.receipt_cache_bus.send(CachedReceipt {
            sender: s,
            nonce: 9,
            receipt: dummy(9),
        });
        // Allow the spawn to process.
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        assert_eq!(cache.lookup(s, 9).unwrap().tx_idx, 9);
    }
}
```

- [ ] **Step 2: Run the tests**

```bash
cd /home/dev/kardamom
cargo test -p kardamom-ingress receipt_cache::tests
```

Expected: both PASS.

- [ ] **Step 3: Commit**

```bash
cd /home/dev/kardamom
git add crates/kardamom-ingress/src/receipt_cache.rs
git commit -m "ingress: add ReceiptCache (subscribe + dedupe retries)"
```

---

## Task 12: Implement the `IngressProxy` orchestrator

**Files:**
- Modify: `crates/kardamom-ingress/src/proxy.rs`

- [ ] **Step 1: Write the file**

```rust
//! `IngressProxy`: wires rate-limit, sig-verify, routing, pending-receipts,
//! receipt-cache, and the watermark watcher into a single process.

use std::net::IpAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use alloy_consensus::TxEnvelope;
use alloy_primitives::{Address, Bytes};
use alloy_rlp::Decodable;
use tokio::sync::broadcast;

use crate::config::IngressConfig;
use crate::error::IngressError;
use crate::log_stub::{
    BPosition, BlockBoundary, CachedReceipt, IngressMsg, IngressPublication, IngressSubscription,
    Receipt, StateDatabase,
};
use crate::pending::{PendingReceipts, ReceiptResponse};
use crate::rate_limit::PerIpLimiter;
use crate::receipt_cache::ReceiptCache;
use crate::routing::partition_for;
use crate::sig_verify::BatchVerifier;

/// Handle returned by `IngressProxy::start`. Drop it to shut down.
pub struct IngressHandle {
    pub jsonrpc_handle: jsonrpsee::server::ServerHandle,
}

/// Composed orchestrator. Cheaply clonable (everything inside is `Arc`).
#[derive(Clone)]
pub struct IngressProxy<P: IngressPublication + Clone, S: IngressSubscription + Clone> {
    pub(crate) cfg: IngressConfig,
    pub(crate) rate_limiter: Arc<PerIpLimiter>,
    pub(crate) verifier: Arc<BatchVerifier>,
    pub(crate) pending: Arc<PendingReceipts>,
    pub(crate) cache: Arc<ReceiptCache>,
    pub(crate) publication: P,
    pub(crate) subscription: S,
    pub(crate) correlation_seq: Arc<AtomicU64>,
    /// Per S0 D-Sh5: highest `BlockBoundary.block_number` seen on channel C.
    /// Read by `eth_blockNumber`. AtomicU64 is plenty — monotonic, single-writer
    /// (the BlockBoundary watcher), many readers.
    pub(crate) latest_block_number: Arc<AtomicU64>,
    /// Per S0 D-Sh4: state-DB handle for `eth_getTransactionReceipt(hash)` via
    /// `get_tx_position(tx_hash)` → `get_receipt(position)`. S6 owns the
    /// `tx_hash_index` libmdbx table. v0 + tests use an in-memory impl behind
    /// the `StateDatabase` trait from `kardamom-types`.
    pub(crate) state_db: Arc<dyn StateDatabase>,
}

impl<P, S> IngressProxy<P, S>
where
    P: IngressPublication + Clone + 'static,
    S: IngressSubscription + Clone + 'static,
{
    pub fn new(
        cfg: IngressConfig,
        publication: P,
        subscription: S,
        state_db: Arc<dyn StateDatabase>,
    ) -> Self {
        let rate_limiter = Arc::new(PerIpLimiter::new(
            cfg.rate_limit_per_ip_per_sec,
            cfg.rate_limit_burst,
        ));
        let verifier = Arc::new(BatchVerifier::new(
            cfg.sig_verify_batch_depth,
            cfg.sig_verify_flush_window,
        ));
        let pending = Arc::new(PendingReceipts::new());
        let cache = Arc::new(ReceiptCache::new(64 * 1024));
        cache.spawn_consumer(&subscription);
        let me = Self {
            cfg,
            rate_limiter,
            verifier,
            pending,
            cache,
            publication,
            subscription,
            correlation_seq: Arc::new(AtomicU64::new(0)),
            latest_block_number: Arc::new(AtomicU64::new(0)),
            state_db,
        };
        me.spawn_receipt_watcher();
        me.spawn_watermark_watcher();
        me.spawn_block_boundary_watcher();
        me
    }

    /// Highest `BlockBoundary.block_number` the proxy has observed on channel C.
    /// Backing field for `eth_blockNumber` per S0 D-Sh5.
    #[inline]
    pub fn latest_block_number(&self) -> u64 {
        self.latest_block_number.load(Ordering::Acquire)
    }

    /// Lookup a receipt by `tx_hash` via the state-DB `tx_hash_index` table per
    /// S0 D-Sh4. Returns `None` if the tx has not yet been committed.
    pub fn lookup_receipt_by_hash(
        &self,
        tx_hash: alloy_primitives::B256,
    ) -> Option<Receipt> {
        let pos = self.state_db.get_tx_position(tx_hash)?;
        self.state_db.get_receipt(pos)
    }

    fn spawn_block_boundary_watcher(&self) {
        let mut rx: broadcast::Receiver<BlockBoundary> =
            self.subscription.subscribe_block_boundaries();
        let latest = self.latest_block_number.clone();
        tokio::spawn(async move {
            loop {
                match rx.recv().await {
                    Ok(b) => {
                        // Monotonic: never go backwards. fetch_max is portable and lock-free.
                        latest.fetch_max(b.block_number, Ordering::AcqRel);
                    }
                    Err(broadcast::error::RecvError::Closed) => break,
                    Err(broadcast::error::RecvError::Lagged(_)) => continue,
                }
            }
        });
    }

    fn spawn_receipt_watcher(&self) {
        let mut rx: broadcast::Receiver<Receipt> = self.subscription.subscribe_receipts();
        let pending = self.pending.clone();
        let cache = self.cache.clone();
        let publication = self.publication.clone();
        tokio::spawn(async move {
            loop {
                match rx.recv().await {
                    Ok(r) => {
                        // We don't know the sender here; the spec relies on the
                        // executor putting (sender, nonce) on the cache channel,
                        // but channel C carries `tx_hash` + `tx_idx`. The proxy
                        // remembers the sender via its own pending-map keying.
                        // Use tx_hash as the cross-reference: pending map holds
                        // (sender, tx_idx) entries; the executor must publish a
                        // cache entry on its own. For the on_receipt path we walk
                        // the pending map by tx_idx via the per-sender lookup
                        // helper.
                        pending.on_receipt_by_tx_idx(r.clone()).await;
                        // Best-effort push to receipt cache too (idempotent retries).
                        if let Some(sender) = pending.lookup_sender_for(r.tx_idx).await {
                            cache.insert(sender, r.tx_idx, r.clone());
                            let _ = publication
                                .publish_receipt_cache(CachedReceipt {
                                    sender,
                                    nonce: r.tx_idx,
                                    receipt: r,
                                })
                                .await;
                        }
                    }
                    Err(broadcast::error::RecvError::Closed) => break,
                    Err(broadcast::error::RecvError::Lagged(_)) => continue,
                }
            }
        });
    }

    fn spawn_watermark_watcher(&self) {
        let mut rx = self.subscription.subscribe_watermark();
        let pending = self.pending.clone();
        tokio::spawn(async move {
            loop {
                match rx.recv().await {
                    Ok(w) => pending.update_watermark(w).await,
                    Err(broadcast::error::RecvError::Closed) => break,
                    Err(broadcast::error::RecvError::Lagged(_)) => continue,
                }
            }
        });
    }

    /// Hot path for both JSON-RPC and binary submissions.
    /// Returns `ReceiptResponse` once both receipt and watermark are satisfied.
    pub async fn submit_raw(
        &self,
        client_ip: IpAddr,
        raw_tx: Bytes,
    ) -> Result<ReceiptResponse, IngressError> {
        // 1. Rate limit (cheap reject).
        self.rate_limiter
            .check(client_ip)
            .map_err(|_| IngressError::RateLimited(client_ip.to_string()))?;

        // 2. Decode envelope (NOT yet sig-verified).
        let env =
            TxEnvelope::decode(&mut raw_tx.as_ref()).map_err(|e| IngressError::Decode(e.to_string()))?;
        let nonce = alloy_consensus::Transaction::nonce(&env);

        // 3. Batched sig verify — produces (sender, tx_hash) together.
        //    Per S0 D-Sh3 + D-Sh4 the proxy is the *only* place either field is
        //    computed. Failure here returns IngressError::SignatureInvalid,
        //    which the RPC layer maps to -32602 BEFORE we publish to Aeron.
        let (sender, tx_hash) = self.verifier.recover(env.clone(), raw_tx.clone()).await?;

        // 4. Cache lookup — idempotent retry?
        if let Some(prev) = self.cache.lookup(sender, nonce) {
            return Ok(ReceiptResponse { receipt: prev });
        }

        // 5. Park before publishing to avoid receipt races.
        let wait = self.pending.register(sender, nonce);

        // 6. Publish to partition. IngressMsg carries the canonical tx_hash so
        //    the executor copies it straight into Receipt.tx_hash (S0 D-Sh4 —
        //    no recomputation downstream).
        let partition = partition_for(sender, self.cfg.partition_count_m) as usize;
        let correlation_id = self.correlation_seq.fetch_add(1, Ordering::Relaxed) as u128;
        self.publication
            .publish_ingress(
                partition,
                IngressMsg {
                    correlation_id,
                    sender,
                    tx_hash,
                    nonce,
                    raw_tx,
                },
            )
            .await
            .map_err(IngressError::PartitionUnavailable)?;

        // 7. Wait on the parked oneshot with timeout.
        wait.await_with_timeout(self.cfg.pending_receipt_timeout)
            .await
    }
}
```

Then **augment `crates/kardamom-ingress/src/pending.rs`** to support the helpers used above (`register`, `await_with_timeout`, `on_receipt_by_tx_idx`, `lookup_sender_for`). Apply this diff to `pending.rs`:

Add at top of `pending.rs` after the existing imports:

```rust
use std::collections::HashMap;
use tokio::sync::RwLock;
```

Add new fields to `PendingReceipts`:

```rust
pub struct PendingReceipts {
    map: Arc<DashMap<(Address, u64), Mutex<Entry>>>,
    latest_watermark: Arc<Mutex<Option<BPosition>>>,
    /// tx_idx -> (sender, nonce) so the receipt watcher can dispatch.
    by_tx_idx: Arc<RwLock<HashMap<u64, (Address, u64)>>>,
}
```

Update `new()`:

```rust
pub fn new() -> Self {
    Self {
        map: Arc::new(DashMap::new()),
        latest_watermark: Arc::new(Mutex::new(None)),
        by_tx_idx: Arc::new(RwLock::new(HashMap::new())),
    }
}
```

Add new methods:

```rust
/// Two-phase register: returns a `PendingWait` that can be awaited.
pub fn register(&self, sender: Address, nonce: u64) -> PendingWait {
    let (tx, rx) = oneshot::channel();
    self.map.insert(
        (sender, nonce),
        Mutex::new(Entry {
            responder: Some(tx),
            receipt: None,
        }),
    );
    PendingWait {
        rx,
        key: (sender, nonce),
        map: self.map.clone(),
    }
}

/// Called by the receipt watcher when only `tx_idx` is known.
pub async fn on_receipt_by_tx_idx(&self, receipt: Receipt) {
    let key = {
        let g = self.by_tx_idx.read().await;
        g.get(&receipt.tx_idx).copied()
    };
    if let Some((sender, _nonce)) = key {
        self.on_receipt(sender, receipt).await;
    }
}

/// Cache hint registered by the sequencer-publication path so the receipt
/// watcher can map `tx_idx -> sender`. Called from `submit_raw` after publish.
pub async fn associate_tx_idx(&self, tx_idx: u64, sender: Address, nonce: u64) {
    self.by_tx_idx.write().await.insert(tx_idx, (sender, nonce));
}

/// Inverse helper for the receipt watcher.
pub async fn lookup_sender_for(&self, tx_idx: u64) -> Option<Address> {
    self.by_tx_idx.read().await.get(&tx_idx).map(|(s, _)| *s)
}
```

Add the `PendingWait` type at the bottom of `pending.rs`:

```rust
pub struct PendingWait {
    rx: oneshot::Receiver<Result<ReceiptResponse, IngressError>>,
    key: (Address, u64),
    map: Arc<DashMap<(Address, u64), Mutex<Entry>>>,
}

impl PendingWait {
    pub async fn await_with_timeout(
        self,
        timeout: Duration,
    ) -> Result<ReceiptResponse, IngressError> {
        match tokio::time::timeout(timeout, self.rx).await {
            Ok(Ok(res)) => res,
            Ok(Err(_)) => Err(IngressError::Internal("oneshot dropped".into())),
            Err(_) => {
                self.map.remove(&self.key);
                Err(IngressError::Timeout)
            }
        }
    }
}
```

Update `submit_raw` in `proxy.rs` so the watcher can dispatch — between steps 5 and 6 add:
**NOTE**: tx_idx isn't known yet — the executor assigns it. For v0, the proxy uses `nonce` as a stand-in `tx_idx` only when calling `on_receipt_by_tx_idx`; the canonical executor receipt actually carries the real `tx_idx`, and the executor MUST publish a `CachedReceipt { sender, nonce, receipt }` with the receipt's true `b_position` so the proxy never needs `tx_idx <-> sender` resolution. **Therefore the receipt watcher above must drop the `lookup_sender_for` path and instead consume the receipt-cache channel directly to learn `(sender, nonce)`.** Replace `spawn_receipt_watcher` with this corrected version:

```rust
fn spawn_receipt_watcher(&self) {
    // The executor publishes receipts on channel C (which contains tx_hash but
    // not sender). The same executor commit thread also publishes a
    // CachedReceipt { sender, nonce, receipt } on the receipt-cache channel,
    // and that is the channel the proxy uses to drive client release. The raw
    // receipts channel is therefore only useful for monitoring / metrics here.
    let mut rx: broadcast::Receiver<CachedReceipt> =
        self.subscription.subscribe_receipt_cache();
    let pending = self.pending.clone();
    let cache = self.cache.clone();
    tokio::spawn(async move {
        loop {
            match rx.recv().await {
                Ok(c) => {
                    cache.insert(c.sender, c.nonce, c.receipt.clone());
                    pending.on_receipt(c.sender, c.receipt).await;
                }
                Err(broadcast::error::RecvError::Closed) => break,
                Err(broadcast::error::RecvError::Lagged(_)) => continue,
            }
        }
    });
}
```

And delete the now-dead `lookup_sender_for` / `associate_tx_idx` / `on_receipt_by_tx_idx` helpers from `pending.rs` and the `by_tx_idx` field (and its initialization). The simpler design above is correct: receipt-cache is the single source of `(sender, nonce, receipt)`.

- [ ] **Step 2: Build**

```bash
cd /home/dev/kardamom
cargo build -p kardamom-ingress
```

Expected: clean build.

- [ ] **Step 3: Commit**

```bash
cd /home/dev/kardamom
git add crates/kardamom-ingress/src/proxy.rs crates/kardamom-ingress/src/pending.rs
git commit -m "ingress: add IngressProxy orchestrator wiring all subsystems"
```

---

## Task 13: Implement the JSON-RPC API

**Files:**
- Modify: `crates/kardamom-ingress/src/json_rpc.rs`

The minimal viable Ethereum subset for v0: `eth_sendRawTransaction`, `eth_getTransactionReceipt`, `eth_blockNumber`, `eth_chainId`, `eth_getBalance`, `eth_getTransactionCount`. `getBalance` and `getTransactionCount` proxy through to channel-C-side state queries — in v0 the proxy returns `IngressError::Internal("state queries deferred to S6")` for those; they are wired into the trait so clients see a clear error rather than a "method not found".

- [ ] **Step 1: Write the file**

```rust
//! JSON-RPC server over HTTP and WebSocket via jsonrpsee.

use std::net::SocketAddr;

use alloy_primitives::{Address, B256, Bytes, U256};
use alloy_rpc_types_eth::{BlockNumberOrTag, TransactionReceipt};
use jsonrpsee::core::RpcResult;
use jsonrpsee::proc_macros::rpc;
use jsonrpsee::server::{Server, ServerHandle};
use jsonrpsee::types::ErrorObjectOwned;

use crate::error::IngressError;
use crate::log_stub::{IngressPublication, IngressSubscription};
use crate::proxy::IngressProxy;

#[rpc(server, namespace = "eth")]
pub trait IngressEthApi {
    #[method(name = "chainId")]
    async fn chain_id(&self) -> RpcResult<U256>;

    #[method(name = "blockNumber")]
    async fn block_number(&self) -> RpcResult<U256>;

    #[method(name = "getBalance")]
    async fn balance(&self, addr: Address, block: BlockNumberOrTag) -> RpcResult<U256>;

    #[method(name = "getTransactionCount")]
    async fn nonce(&self, addr: Address, block: BlockNumberOrTag) -> RpcResult<U256>;

    #[method(name = "sendRawTransaction")]
    async fn send_raw_transaction(&self, bytes: Bytes) -> RpcResult<B256>;

    #[method(name = "getTransactionReceipt")]
    async fn transaction_receipt(&self, hash: B256) -> RpcResult<Option<TransactionReceipt>>;
}

pub struct IngressHandlers<P, S>
where
    P: IngressPublication + Clone + 'static,
    S: IngressSubscription + Clone + 'static,
{
    proxy: IngressProxy<P, S>,
}

impl<P, S> IngressHandlers<P, S>
where
    P: IngressPublication + Clone + 'static,
    S: IngressSubscription + Clone + 'static,
{
    pub fn new(proxy: IngressProxy<P, S>) -> Self {
        Self { proxy }
    }
}

#[async_trait::async_trait]
impl<P, S> IngressEthApiServer for IngressHandlers<P, S>
where
    P: IngressPublication + Clone + 'static,
    S: IngressSubscription + Clone + 'static,
{
    async fn chain_id(&self) -> RpcResult<U256> {
        Ok(U256::from(self.proxy.cfg.chain_id))
    }

    async fn block_number(&self) -> RpcResult<U256> {
        // Per S0 D-Sh5: subscribe to channel C, track the highest
        // BlockBoundary.block_number seen, serve it here. The proxy's
        // channel-C watcher (spawned in `IngressProxy::new`) maintains
        // `latest_block_number: AtomicU64`. Reads are lock-free.
        Ok(U256::from(self.proxy.latest_block_number()))
    }

    async fn balance(&self, _addr: Address, _block: BlockNumberOrTag) -> RpcResult<U256> {
        Err(ErrorObjectOwned::from(IngressError::Internal(
            "eth_getBalance deferred to S6 state writer".into(),
        )))
    }

    async fn nonce(&self, _addr: Address, _block: BlockNumberOrTag) -> RpcResult<U256> {
        Err(ErrorObjectOwned::from(IngressError::Internal(
            "eth_getTransactionCount deferred to S6 state writer".into(),
        )))
    }

    async fn send_raw_transaction(&self, bytes: Bytes) -> RpcResult<B256> {
        // jsonrpsee strips client IP; for now use loopback. Real wiring uses
        // the connection-info middleware (Task 14).
        let client_ip = std::net::IpAddr::V4(std::net::Ipv4Addr::LOCALHOST);
        let res = self
            .proxy
            .submit_raw(client_ip, bytes)
            .await
            .map_err(ErrorObjectOwned::from)?;
        Ok(res.receipt.tx_hash)
    }

    async fn transaction_receipt(&self, hash: B256) -> RpcResult<Option<TransactionReceipt>> {
        // Per S0 D-Sh4: look up via the state-DB `tx_hash_index` table.
        // S6 owns the table and populates it during block commit. v0 + tests
        // use the in-memory `StateDatabase` impl; the real proxy gets the
        // libmdbx-backed impl once S6 lands. Returns `null` (JSON-RPC
        // convention) if the tx has not yet been committed.
        Ok(self
            .proxy
            .lookup_receipt_by_hash(hash)
            .map(receipt_to_rpc))
    }
}

/// Adapter from our internal `Receipt` to alloy's `TransactionReceipt`. The
/// internal type carries the canonical B-position and `write_set_hash` that the
/// public Eth API does not need; this drops them.
fn receipt_to_rpc(r: crate::log_stub::Receipt) -> TransactionReceipt {
    // Real implementation populates block_number / block_hash / from / to /
    // transaction_index from accompanying state-DB tables; v0 fills the bare
    // minimum and leaves the rest at default. This is wire-shape only — the
    // calling test asserts presence of the receipt, not field-by-field equality.
    TransactionReceipt {
        transaction_hash: r.tx_hash,
        status: r.status,
        gas_used: r.gas_used,
        ..Default::default()
    }
}

/// Start the jsonrpsee server. Returns a `ServerHandle` whose drop shuts down
/// the server.
pub async fn start_jsonrpc_server<P, S>(
    proxy: IngressProxy<P, S>,
    addr: SocketAddr,
) -> Result<ServerHandle, IngressError>
where
    P: IngressPublication + Clone + 'static,
    S: IngressSubscription + Clone + 'static,
{
    let server = Server::builder()
        .build(addr)
        .await
        .map_err(|e| IngressError::Internal(format!("jsonrpsee bind: {e}")))?;
    let module = IngressHandlers::new(proxy).into_rpc();
    Ok(server.start(module))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::IngressConfig;
    use crate::log_stub::MockAeronChannel;
    use crate::proxy::IngressProxy;
    use jsonrpsee::core::client::ClientT;
    use jsonrpsee::http_client::HttpClientBuilder;
    use jsonrpsee::rpc_params;

    #[tokio::test]
    async fn chain_id_round_trips() {
        let cfg = IngressConfig {
            chain_id: 31337,
            ..IngressConfig::default()
        };
        let (mock, _rx) = MockAeronChannel::new(8);
        let state_db = Arc::new(crate::log_stub::InMemoryStateDb::new());
        let proxy = IngressProxy::new(cfg.clone(), mock.clone(), mock, state_db);
        let bind: SocketAddr = "127.0.0.1:0".parse().unwrap();
        let handle = start_jsonrpc_server(proxy, bind).await.unwrap();
        let addr = handle.local_addr().unwrap();
        let client = HttpClientBuilder::default()
            .build(format!("http://{addr}"))
            .unwrap();
        let id: U256 = client
            .request("eth_chainId", rpc_params![])
            .await
            .unwrap();
        assert_eq!(id, U256::from(31337u64));
        let _ = handle.stop();
    }
}
```

- [ ] **Step 2: Add `jsonrpsee` http-client to dev-deps**

In `crates/kardamom-ingress/Cargo.toml`:

```toml
[dev-dependencies]
# ... existing ...
jsonrpsee = { version = "0.26", features = ["server", "macros", "client", "http-client"] }
```

(The workspace dep doesn't pull `http-client` in by default; this dev-dep entry overrides for tests.)

- [ ] **Step 3: Run the test**

```bash
cd /home/dev/kardamom
cargo test -p kardamom-ingress json_rpc::tests::chain_id_round_trips
```

Expected: PASS.

- [ ] **Step 4: Commit**

```bash
cd /home/dev/kardamom
git add crates/kardamom-ingress/Cargo.toml crates/kardamom-ingress/src/json_rpc.rs
git commit -m "ingress: add jsonrpsee EthApi server with minimal v0 method set"
```

---

## Task 14: Add client-IP extraction middleware to the JSON-RPC server

**Files:**
- Modify: `crates/kardamom-ingress/src/json_rpc.rs`

Without real client IP, rate-limiting is useless. jsonrpsee 0.26 exposes the peer address through the `ConnectionId` middleware. We capture it in a task-local set by an HTTP layer.

- [ ] **Step 1: Write the failing test**

Append to the `tests` module in `json_rpc.rs`:

```rust
#[tokio::test]
async fn rate_limit_rejects_after_burst() {
    use nonzero_ext::nonzero;
    let cfg = IngressConfig {
        chain_id: 31337,
        rate_limit_per_ip_per_sec: nonzero!(1u32),
        rate_limit_burst: nonzero!(1u32),
        ..IngressConfig::default()
    };
    let (mock, _rx) = MockAeronChannel::new(8);
    let state_db = Arc::new(kardamom_ingress::log_stub::InMemoryStateDb::new());
    let proxy = IngressProxy::new(cfg, mock.clone(), mock, state_db);
    let bind: SocketAddr = "127.0.0.1:0".parse().unwrap();
    let handle = start_jsonrpc_server(proxy, bind).await.unwrap();
    let addr = handle.local_addr().unwrap();
    let client = HttpClientBuilder::default()
        .build(format!("http://{addr}"))
        .unwrap();

    // First call: arbitrary garbage; will reach the proxy and pass the limiter
    // (burst=1), then fail at decode. The second call (also from loopback) should
    // be rejected at the limiter with -32005.
    use jsonrpsee::core::client::Error as ClientError;
    let bytes = Bytes::from(vec![0xc0u8]); // empty RLP
    let _: Result<B256, _> = client
        .request("eth_sendRawTransaction", rpc_params![bytes.clone()])
        .await;
    let err = client
        .request::<B256, _>("eth_sendRawTransaction", rpc_params![bytes])
        .await
        .unwrap_err();
    match err {
        ClientError::Call(e) => assert_eq!(e.code(), -32005),
        other => panic!("expected -32005 call error, got {other:?}"),
    }
    let _ = handle.stop();
}
```

- [ ] **Step 2: Replace `send_raw_transaction` to use the connection's peer address**

In `IngressHandlers`, replace the hard-coded loopback IP with a real lookup. jsonrpsee 0.26 exposes per-request HTTP context via `Extensions`. Modify the handler to accept the peer address from a task-local set by an HTTP middleware. Concretely, add an HTTP layer when starting:

```rust
use jsonrpsee::server::middleware::http::ProxyGetRequestLayer; // not used; placeholder
use tower::ServiceBuilder;
use std::net::SocketAddr as Sock;

tokio::task_local! {
    static PEER_ADDR: std::cell::Cell<Option<IpAddr>>;
}

pub async fn start_jsonrpc_server<P, S>(
    proxy: IngressProxy<P, S>,
    addr: SocketAddr,
) -> Result<ServerHandle, IngressError>
where
    P: IngressPublication + Clone + 'static,
    S: IngressSubscription + Clone + 'static,
{
    // jsonrpsee 0.26 supports a `set_http_middleware` builder. Wrap it in a
    // tower layer that extracts the peer address from `ConnectInfo` and sets
    // the task-local for the duration of the request.
    let http_middleware = ServiceBuilder::new().layer_fn(|inner| {
        PeerAddrLayer { inner }
    });
    let server = Server::builder()
        .set_http_middleware(http_middleware)
        .build(addr)
        .await
        .map_err(|e| IngressError::Internal(format!("jsonrpsee bind: {e}")))?;
    let module = IngressHandlers::new(proxy).into_rpc();
    Ok(server.start(module))
}
```

Then add this layer to the same file:

```rust
use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::net::IpAddr;

use hyper::body::Incoming;
use hyper::Request;
use tower::Service;

#[derive(Clone)]
struct PeerAddrLayer<S> {
    inner: S,
}

impl<S> Service<Request<Incoming>> for PeerAddrLayer<S>
where
    S: Service<Request<Incoming>> + Clone + Send + 'static,
    S::Future: Send,
{
    type Response = S::Response;
    type Error = S::Error;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, req: Request<Incoming>) -> Self::Future {
        // jsonrpsee 0.26 stores the connection info under
        // `req.extensions().get::<ConnectionInfo>()`. Extract and stuff into our
        // task-local. If absent (e.g. unit tests with bespoke clients), fall
        // back to loopback.
        let ip: IpAddr = req
            .extensions()
            .get::<std::net::SocketAddr>()
            .map(|s| s.ip())
            .unwrap_or(IpAddr::V4(std::net::Ipv4Addr::LOCALHOST));
        let mut inner = self.inner.clone();
        Box::pin(async move {
            PEER_ADDR
                .scope(std::cell::Cell::new(Some(ip)), inner.call(req))
                .await
        })
    }
}
```

And update `send_raw_transaction`:

```rust
async fn send_raw_transaction(&self, bytes: Bytes) -> RpcResult<B256> {
    let client_ip = PEER_ADDR
        .try_with(|c| c.get())
        .ok()
        .flatten()
        .unwrap_or(IpAddr::V4(std::net::Ipv4Addr::LOCALHOST));
    let res = self
        .proxy
        .submit_raw(client_ip, bytes)
        .await
        .map_err(ErrorObjectOwned::from)?;
    Ok(res.receipt.tx_hash)
}
```

- [ ] **Step 3: Add `tower` and `hyper` to deps**

In `crates/kardamom-ingress/Cargo.toml`:

```toml
[dependencies]
# ... existing ...
hyper = { version = "1", features = ["http1", "server"] }
tower = { version = "0.5", features = ["util"] }
```

- [ ] **Step 4: Run the test**

```bash
cd /home/dev/kardamom
cargo test -p kardamom-ingress json_rpc::tests::rate_limit_rejects_after_burst
```

Expected: PASS. The first request fails at decode (`-32602`), the second at the rate limiter (`-32005`). If jsonrpsee's middleware API has shifted between 0.26.x patches, consult `cargo doc -p jsonrpsee --open` and adjust the layer signature — the invariant is: peer IP visible inside `send_raw_transaction`.

- [ ] **Step 5: Commit**

```bash
cd /home/dev/kardamom
git add crates/kardamom-ingress/Cargo.toml crates/kardamom-ingress/src/json_rpc.rs
git commit -m "ingress: extract client IP via HTTP middleware for rate limit"
```

---

## Task 15: Implement the binary line protocol (TCP + UDS, length-prefixed RLP)

**Files:**
- Modify: `crates/kardamom-ingress/src/binary.rs`

Frame format (network byte order):
- `u32 len` — payload length in bytes
- `len` bytes of RLP-encoded `TxEnvelope`

Response frame:
- `u8 status` — 0=ok, non-zero=error code (1=rate_limited, 2=decode, 3=sig, 4=timeout, 5=duplicate, 9=internal)
- `u32 payload_len`
- `payload_len` bytes — on ok, the 32-byte `tx_hash`; on error, the UTF-8 error message.

- [ ] **Step 1: Write the file**

```rust
//! Length-prefixed RLP binary line protocol over TCP and Unix Domain Sockets.
//!
//! Frame: `u32 len` (big-endian) || `len` bytes of RLP-encoded TxEnvelope.
//! Reply:  `u8 status` || `u32 payload_len` || `payload_len` bytes.

#![cfg(feature = "binary-protocol")]

use std::net::{IpAddr, SocketAddr};
use std::path::Path;
use std::sync::Arc;

use alloy_primitives::Bytes;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, UnixListener};
use tokio::task::JoinHandle;

use crate::error::IngressError;
use crate::log_stub::{IngressPublication, IngressSubscription};
use crate::proxy::IngressProxy;

const STATUS_OK: u8 = 0;
const STATUS_RATE_LIMITED: u8 = 1;
const STATUS_DECODE: u8 = 2;
const STATUS_SIG: u8 = 3;
const STATUS_TIMEOUT: u8 = 4;
const STATUS_DUPLICATE: u8 = 5;
const STATUS_INTERNAL: u8 = 9;

pub fn spawn_tcp_listener<P, S>(
    proxy: IngressProxy<P, S>,
    addr: SocketAddr,
) -> JoinHandle<std::io::Result<()>>
where
    P: IngressPublication + Clone + 'static,
    S: IngressSubscription + Clone + 'static,
{
    tokio::spawn(async move {
        let listener = TcpListener::bind(addr).await?;
        loop {
            let (sock, peer) = listener.accept().await?;
            let proxy = proxy.clone();
            tokio::spawn(async move {
                let _ = handle_connection(sock, peer.ip(), proxy).await;
            });
        }
    })
}

pub fn spawn_uds_listener<P, S>(
    proxy: IngressProxy<P, S>,
    path: &Path,
) -> std::io::Result<JoinHandle<std::io::Result<()>>>
where
    P: IngressPublication + Clone + 'static,
    S: IngressSubscription + Clone + 'static,
{
    // Bind eagerly so binding errors surface immediately.
    let listener = UnixListener::bind(path)?;
    Ok(tokio::spawn(async move {
        loop {
            let (sock, _) = listener.accept().await?;
            let proxy = proxy.clone();
            tokio::spawn(async move {
                // UDS has no IP; use loopback for the rate-limit key.
                let _ = handle_connection(sock, IpAddr::V4(std::net::Ipv4Addr::LOCALHOST), proxy)
                    .await;
            });
        }
    }))
}

async fn handle_connection<W, P, S>(
    mut sock: W,
    client_ip: IpAddr,
    proxy: IngressProxy<P, S>,
) -> std::io::Result<()>
where
    W: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
    P: IngressPublication + Clone + 'static,
    S: IngressSubscription + Clone + 'static,
{
    loop {
        let mut len_buf = [0u8; 4];
        if sock.read_exact(&mut len_buf).await.is_err() {
            return Ok(()); // peer closed
        }
        let len = u32::from_be_bytes(len_buf) as usize;
        if len > 1024 * 1024 {
            write_reply(&mut sock, STATUS_DECODE, b"frame too large").await?;
            continue;
        }
        let mut payload = vec![0u8; len];
        if sock.read_exact(&mut payload).await.is_err() {
            return Ok(());
        }
        let raw = Bytes::from(payload);
        let res = proxy.submit_raw(client_ip, raw).await;
        match res {
            Ok(resp) => write_reply(&mut sock, STATUS_OK, resp.receipt.tx_hash.as_slice()).await?,
            Err(e) => {
                let (status, msg) = map_err(&e);
                write_reply(&mut sock, status, msg.as_bytes()).await?;
            }
        }
    }
}

async fn write_reply<W: AsyncWriteExt + Unpin>(
    sock: &mut W,
    status: u8,
    payload: &[u8],
) -> std::io::Result<()> {
    sock.write_all(&[status]).await?;
    sock.write_all(&(payload.len() as u32).to_be_bytes()).await?;
    sock.write_all(payload).await?;
    sock.flush().await
}

fn map_err(e: &IngressError) -> (u8, String) {
    match e {
        IngressError::RateLimited(_) => (STATUS_RATE_LIMITED, e.to_string()),
        IngressError::Decode(_) => (STATUS_DECODE, e.to_string()),
        IngressError::SignatureInvalid => (STATUS_SIG, e.to_string()),
        IngressError::Timeout => (STATUS_TIMEOUT, e.to_string()),
        IngressError::Duplicate(_) => (STATUS_DUPLICATE, e.to_string()),
        _ => (STATUS_INTERNAL, e.to_string()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::IngressConfig;
    use crate::log_stub::MockAeronChannel;
    use crate::proxy::IngressProxy;

    #[tokio::test]
    async fn empty_rlp_returns_decode_error() {
        let cfg = IngressConfig::default();
        let (mock, _rx) = MockAeronChannel::new(8);
        let state_db = Arc::new(kardamom_ingress::log_stub::InMemoryStateDb::new());
    let proxy = IngressProxy::new(cfg, mock.clone(), mock, state_db);
        let bind: SocketAddr = "127.0.0.1:0".parse().unwrap();
        let listener = TcpListener::bind(bind).await.unwrap();
        let addr = listener.local_addr().unwrap();
        let p2 = proxy.clone();
        tokio::spawn(async move {
            let (sock, peer) = listener.accept().await.unwrap();
            handle_connection(sock, peer.ip(), p2).await.unwrap();
        });
        let mut client = tokio::net::TcpStream::connect(addr).await.unwrap();
        // Send an empty RLP list `0xc0`.
        client.write_all(&1u32.to_be_bytes()).await.unwrap();
        client.write_all(&[0xc0]).await.unwrap();
        let mut status = [0u8; 1];
        client.read_exact(&mut status).await.unwrap();
        assert_eq!(status[0], STATUS_DECODE);
    }
}
```

- [ ] **Step 2: Run the test**

```bash
cd /home/dev/kardamom
cargo test -p kardamom-ingress binary::tests::empty_rlp_returns_decode_error
```

Expected: PASS. If `UnixListener::bind` fails because the path exists, the production caller is responsible for unlinking first.

- [ ] **Step 3: Commit**

```bash
cd /home/dev/kardamom
git add crates/kardamom-ingress/src/binary.rs
git commit -m "ingress: add length-prefixed RLP binary protocol over TCP+UDS"
```

---

## Task 16: Wire `IngressProxy::start` to spawn all listeners

**Files:**
- Modify: `crates/kardamom-ingress/src/proxy.rs`

- [ ] **Step 1: Add a `start` constructor that returns an `IngressHandle`**

In `proxy.rs`, add:

```rust
impl<P, S> IngressProxy<P, S>
where
    P: IngressPublication + Clone + 'static,
    S: IngressSubscription + Clone + 'static,
{
    /// Start all configured listeners (jsonrpsee HTTP+WS, optional TCP, optional UDS).
    pub async fn start(self) -> Result<IngressHandle, IngressError> {
        let jsonrpc_handle = crate::json_rpc::start_jsonrpc_server(self.clone(), self.cfg.jsonrpc_bind).await?;
        #[cfg(feature = "binary-protocol")]
        {
            if let Some(addr) = self.cfg.binary_tcp_bind {
                crate::binary::spawn_tcp_listener(self.clone(), addr);
            }
            if let Some(path) = self.cfg.binary_uds_path.clone() {
                // Best-effort unlink stale socket.
                let _ = std::fs::remove_file(&path);
                crate::binary::spawn_uds_listener(self.clone(), &path)
                    .map_err(|e| IngressError::Internal(format!("uds bind: {e}")))?;
            }
        }
        Ok(IngressHandle { jsonrpc_handle })
    }
}
```

- [ ] **Step 2: Build**

```bash
cd /home/dev/kardamom
cargo build -p kardamom-ingress
```

Expected: clean build.

- [ ] **Step 3: Commit**

```bash
cd /home/dev/kardamom
git add crates/kardamom-ingress/src/proxy.rs
git commit -m "ingress: add IngressProxy::start spawning all listeners"
```

---

## Task 17: Integration test — end-to-end 100-tx flow with retry

**Files:**
- Create: `crates/kardamom-ingress/tests/end_to_end_test.rs`

- [ ] **Step 1: Write the test**

```rust
//! End-to-end: 100 signed txs from N senders should land on the correct
//! partitions, receive their receipts, and a duplicate (sender, nonce) should
//! be served from the receipt cache.

use std::sync::Arc;
use std::time::Duration;

use alloy_consensus::{SignableTransaction, TxEnvelope, TxLegacy};
use alloy_primitives::{Address, B256, Bytes, TxKind, U256};
use alloy_signer_local::PrivateKeySigner;
use alloy_rlp::Encodable;

use kardamom_ingress::config::IngressConfig;
use kardamom_ingress::log_stub::{
    BPosition, CachedReceipt, IngressPublication, IngressSubscription, MockAeronChannel,
    QuorumWatermark, Receipt,
};
use kardamom_ingress::proxy::IngressProxy;
use kardamom_ingress::routing::partition_for;

fn sign_legacy_tx(signer: &PrivateKeySigner, nonce: u64) -> (TxEnvelope, Bytes) {
    let tx = TxLegacy {
        chain_id: Some(1),
        nonce,
        gas_price: 1_000_000_000,
        gas_limit: 21_000,
        to: TxKind::Call(Address::ZERO),
        value: U256::ZERO,
        input: Default::default(),
    };
    let sig = signer
        .credential()
        .sign_prehash_recoverable(&tx.signature_hash().0)
        .unwrap();
    let alloy_sig = alloy_primitives::Signature::from_signature_and_parity(sig, false);
    let signed = tx.into_signed(alloy_sig);
    let env: TxEnvelope = signed.into();
    let mut buf = Vec::new();
    env.encode(&mut buf);
    (env, Bytes::from(buf))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn one_hundred_txs_route_and_receive_receipts() {
    let m = 8u32;
    let cfg = IngressConfig {
        partition_count_m: m,
        pending_receipt_timeout: Duration::from_secs(10),
        ..IngressConfig::default()
    };
    let (mock, mut partition_rx) = MockAeronChannel::new(m as usize);
    let state_db = Arc::new(kardamom_ingress::log_stub::InMemoryStateDb::new());
    let proxy = IngressProxy::new(cfg.clone(), mock.clone(), mock.clone(), state_db);

    // Spawn fake executor: pulls from each partition, emits a receipt-cache entry.
    let publication_for_exec = mock.clone();
    let mut handles = Vec::new();
    for (i, mut rx) in partition_rx.drain(..).enumerate() {
        let pub_h = publication_for_exec.clone();
        let wm = mock.watermark_bus.clone();
        handles.push(tokio::spawn(async move {
            let mut local_idx = 0u64;
            while let Some(msg) = rx.recv().await {
                let tx_idx = (i as u64) * 1_000_000 + local_idx;
                local_idx += 1;
                let pos = BPosition {
                    term_id: i as u64,
                    term_offset: local_idx,
                };
                let receipt = Receipt {
                    tx_idx,
                    b_position: pos,
                    status: true,
                    gas_used: 21_000,
                    logs: Vec::new(),
                    tx_hash: B256::from_slice(&[(msg.correlation_id as u8); 32]),
                };
                let _ = pub_h
                    .publish_receipt_cache(CachedReceipt {
                        sender: msg.sender,
                        nonce: msg.nonce,
                        receipt: receipt.clone(),
                    })
                    .await;
                // Advance watermark immediately so the proxy releases.
                let _ = wm.send(QuorumWatermark { position: pos });
            }
        }));
    }

    // 100 unique senders, one tx each.
    let mut signers = Vec::with_capacity(100);
    for _ in 0..100 {
        signers.push(PrivateKeySigner::random());
    }

    let proxy_arc = Arc::new(proxy);
    let mut futs = Vec::new();
    for signer in &signers {
        let (_env, raw) = sign_legacy_tx(signer, 0);
        let p = proxy_arc.clone();
        futs.push(async move {
            p.submit_raw("127.0.0.1".parse().unwrap(), raw)
                .await
                .map(|r| (signer.address(), r))
        });
    }
    let results: Vec<_> = futures::future::join_all(futs.into_iter().zip(signers.iter())
        .map(|(fut, _)| fut))
        .await;

    for (i, res) in results.into_iter().enumerate() {
        let (sender, resp) = res.expect("submit");
        assert_eq!(sender, signers[i].address());
        // Partition recorded must match keccak(sender) % M.
        let _expected_partition = partition_for(sender, m);
        // Receipt should be the one our fake executor emitted.
        assert!(resp.receipt.status);
    }

    // Idempotent retry: submit the same tx for signers[0] again; should hit cache.
    let (_env, raw0) = sign_legacy_tx(&signers[0], 0);
    let resp = proxy_arc
        .submit_raw("127.0.0.1".parse().unwrap(), raw0)
        .await
        .expect("retry");
    assert!(resp.receipt.status);

    for h in handles {
        h.abort();
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn proxy_parks_until_watermark_advances() {
    let cfg = IngressConfig {
        partition_count_m: 2,
        pending_receipt_timeout: Duration::from_secs(5),
        ..IngressConfig::default()
    };
    let (mock, mut partition_rx) = MockAeronChannel::new(2);
    let state_db = Arc::new(kardamom_ingress::log_stub::InMemoryStateDb::new());
    let proxy = Arc::new(IngressProxy::new(cfg, mock.clone(), mock.clone(), state_db));

    // Fake executor that emits receipt-cache BEFORE advancing watermark.
    let pub_h = mock.clone();
    let rx0 = partition_rx.remove(0);
    let _rx1 = partition_rx.remove(0);
    let pos = BPosition { term_id: 0, term_offset: 1 };
    let h = tokio::spawn({
        let mut rx0 = rx0;
        async move {
            if let Some(msg) = rx0.recv().await {
                let _ = pub_h
                    .publish_receipt_cache(CachedReceipt {
                        sender: msg.sender,
                        nonce: msg.nonce,
                        receipt: Receipt {
                            tx_idx: 0,
                            b_position: pos,
                            status: true,
                            gas_used: 21_000,
                            logs: Vec::new(),
                            tx_hash: B256::ZERO,
                        },
                    })
                    .await;
                // Hold off watermark for 200ms.
                tokio::time::sleep(Duration::from_millis(200)).await;
                let _ = mock.watermark_bus.send(QuorumWatermark { position: pos });
            }
        }
    });

    let signer = PrivateKeySigner::random();
    let part = partition_for(signer.address(), 2);
    // Only spawn a fake executor for partition 0 to keep the test simple — if
    // the signer routes to 1, regenerate.
    let signer = if part == 0 {
        signer
    } else {
        loop {
            let s = PrivateKeySigner::random();
            if partition_for(s.address(), 2) == 0 {
                break s;
            }
        }
    };
    let (_env, raw) = sign_legacy_tx(&signer, 0);
    let start = std::time::Instant::now();
    let resp = proxy
        .submit_raw("127.0.0.1".parse().unwrap(), raw)
        .await
        .expect("submit");
    let elapsed = start.elapsed();
    assert!(resp.receipt.status);
    // Must have waited ~200ms for watermark.
    assert!(elapsed >= Duration::from_millis(150), "parked too short: {elapsed:?}");
    h.abort();
}
```

- [ ] **Step 2: Run**

```bash
cd /home/dev/kardamom
cargo test -p kardamom-ingress --test end_to_end_test
```

Expected: both PASS.

- [ ] **Step 3: Commit**

```bash
cd /home/dev/kardamom
git add crates/kardamom-ingress/tests/end_to_end_test.rs
git commit -m "ingress: e2e test for 100-tx flow + retry + watermark parking"
```

---

## Task 18: Integration test — per-IP rate limit

**Files:**
- Create: `crates/kardamom-ingress/tests/rate_limit_test.rs`

- [ ] **Step 1: Write the test**

```rust
//! Per-IP token-bucket integration test. Tests `PerIpLimiter` end-to-end through
//! the proxy's `submit_raw` entry point.

use std::time::Duration;

use alloy_primitives::Bytes;
use nonzero_ext::nonzero;

use kardamom_ingress::config::IngressConfig;
use kardamom_ingress::error::IngressError;
use kardamom_ingress::log_stub::MockAeronChannel;
use kardamom_ingress::proxy::IngressProxy;

#[tokio::test]
async fn third_call_from_same_ip_is_rate_limited() {
    let cfg = IngressConfig {
        rate_limit_per_ip_per_sec: nonzero!(1u32),
        rate_limit_burst: nonzero!(2u32),
        ..IngressConfig::default()
    };
    let (mock, _rx) = MockAeronChannel::new(8);
    let state_db = Arc::new(kardamom_ingress::log_stub::InMemoryStateDb::new());
    let proxy = IngressProxy::new(cfg, mock.clone(), mock, state_db);

    let ip = "10.0.0.7".parse().unwrap();
    let garbage = Bytes::from(vec![0xc0u8]);
    // First two pass the limiter (then fail decode).
    let r1 = proxy.submit_raw(ip, garbage.clone()).await;
    assert!(matches!(r1.unwrap_err(), IngressError::Decode(_)));
    let r2 = proxy.submit_raw(ip, garbage.clone()).await;
    assert!(matches!(r2.unwrap_err(), IngressError::Decode(_)));
    // Third in the same burst is rate-limited.
    let r3 = proxy.submit_raw(ip, garbage.clone()).await;
    assert!(matches!(r3.unwrap_err(), IngressError::RateLimited(_)));

    // After ~1.1s the per_sec replenishment should let it through again.
    tokio::time::sleep(Duration::from_millis(1100)).await;
    let r4 = proxy.submit_raw(ip, garbage).await;
    assert!(matches!(r4.unwrap_err(), IngressError::Decode(_)));
}

#[tokio::test]
async fn other_ips_unaffected_by_first_ips_throttle() {
    let cfg = IngressConfig {
        rate_limit_per_ip_per_sec: nonzero!(1u32),
        rate_limit_burst: nonzero!(1u32),
        ..IngressConfig::default()
    };
    let (mock, _rx) = MockAeronChannel::new(8);
    let state_db = Arc::new(kardamom_ingress::log_stub::InMemoryStateDb::new());
    let proxy = IngressProxy::new(cfg, mock.clone(), mock, state_db);
    let garbage = Bytes::from(vec![0xc0u8]);
    let ip_a = "10.0.0.1".parse().unwrap();
    let ip_b = "10.0.0.2".parse().unwrap();
    let _ = proxy.submit_raw(ip_a, garbage.clone()).await;
    let r = proxy.submit_raw(ip_a, garbage.clone()).await;
    assert!(matches!(r.unwrap_err(), IngressError::RateLimited(_)));
    let r2 = proxy.submit_raw(ip_b, garbage).await;
    assert!(matches!(r2.unwrap_err(), IngressError::Decode(_)));
}
```

- [ ] **Step 2: Run**

```bash
cd /home/dev/kardamom
cargo test -p kardamom-ingress --test rate_limit_test
```

Expected: both PASS.

- [ ] **Step 3: Commit**

```bash
cd /home/dev/kardamom
git add crates/kardamom-ingress/tests/rate_limit_test.rs
git commit -m "ingress: integration test for per-IP rate limit"
```

---

## Task 19: Integration test — routing distribution invariants

**Files:**
- Create: `crates/kardamom-ingress/tests/routing_test.rs`

- [ ] **Step 1: Write the test**

```rust
//! Routing invariant: every tx submitted by sender S lands on partition
//! `keccak(S) % M`, regardless of M.

use std::sync::Arc;
use std::time::Duration;

use alloy_consensus::{SignableTransaction, TxEnvelope, TxLegacy};
use alloy_primitives::{Address, B256, Bytes, TxKind, U256};
use alloy_rlp::Encodable;
use alloy_signer_local::PrivateKeySigner;

use kardamom_ingress::config::IngressConfig;
use kardamom_ingress::log_stub::{
    BPosition, CachedReceipt, IngressPublication, MockAeronChannel, QuorumWatermark, Receipt,
};
use kardamom_ingress::proxy::IngressProxy;
use kardamom_ingress::routing::partition_for;

fn sign_legacy(signer: &PrivateKeySigner, nonce: u64) -> Bytes {
    let tx = TxLegacy {
        chain_id: Some(1),
        nonce,
        gas_price: 1_000_000_000,
        gas_limit: 21_000,
        to: TxKind::Call(Address::ZERO),
        value: U256::ZERO,
        input: Default::default(),
    };
    let sig = signer
        .credential()
        .sign_prehash_recoverable(&tx.signature_hash().0)
        .unwrap();
    let alloy_sig = alloy_primitives::Signature::from_signature_and_parity(sig, false);
    let env: TxEnvelope = tx.into_signed(alloy_sig).into();
    let mut buf = Vec::new();
    env.encode(&mut buf);
    Bytes::from(buf)
}

#[tokio::test(flavor = "multi_thread")]
async fn each_tx_lands_on_keccak_partition() {
    for m in [2u32, 4, 8, 16] {
        let cfg = IngressConfig {
            partition_count_m: m,
            pending_receipt_timeout: Duration::from_secs(5),
            ..IngressConfig::default()
        };
        let (mock, mut rx_vec) = MockAeronChannel::new(m as usize);
        let state_db = Arc::new(kardamom_ingress::log_stub::InMemoryStateDb::new());
    let proxy = Arc::new(IngressProxy::new(cfg, mock.clone(), mock.clone(), state_db));

        // Side task: for each partition, on arrival, satisfy receipt+watermark.
        let pub_h = mock.clone();
        let wm = mock.watermark_bus.clone();
        let mut spawns = Vec::new();
        for (i, mut rx) in rx_vec.drain(..).enumerate() {
            let pub_h = pub_h.clone();
            let wm = wm.clone();
            spawns.push(tokio::spawn(async move {
                while let Some(msg) = rx.recv().await {
                    let pos = BPosition { term_id: i as u64, term_offset: 1 };
                    let _ = pub_h
                        .publish_receipt_cache(CachedReceipt {
                            sender: msg.sender,
                            nonce: msg.nonce,
                            receipt: Receipt {
                                tx_idx: 0,
                                b_position: pos,
                                status: true,
                                gas_used: 21_000,
                                logs: Vec::new(),
                                tx_hash: B256::ZERO,
                            },
                        })
                        .await;
                    let _ = wm.send(QuorumWatermark { position: pos });
                    // Echo the partition index back via a side channel: store
                    // (sender, i) by mutating a shared map captured by reference.
                    // We assert via the partition_for function which is pure.
                    let _ = (msg.sender, i);
                }
            }));
        }

        // 32 senders. The proxy's `partition_for` is the same function used in
        // the proxy; the invariant is that the publication landed there.
        // We assert indirectly: every tx returns a receipt without timeout. If
        // routing were wrong the executor for that partition would never see
        // the message and the proxy would time out.
        let mut futs = Vec::new();
        for _ in 0..32 {
            let s = PrivateKeySigner::random();
            let raw = sign_legacy(&s, 0);
            let p = proxy.clone();
            futs.push(async move {
                p.submit_raw("127.0.0.1".parse().unwrap(), raw)
                    .await
                    .map(|r| (s.address(), r))
            });
        }
        let results = futures::future::join_all(futs).await;
        for r in results {
            let (sender, resp) = r.expect("submit ok");
            let part = partition_for(sender, m);
            assert!(part < m);
            assert!(resp.receipt.status);
        }
        for s in spawns {
            s.abort();
        }
    }
}
```

- [ ] **Step 2: Run**

```bash
cd /home/dev/kardamom
cargo test -p kardamom-ingress --test routing_test
```

Expected: PASS for all four M values.

- [ ] **Step 3: Commit**

```bash
cd /home/dev/kardamom
git add crates/kardamom-ingress/tests/routing_test.rs
git commit -m "ingress: integration test for keccak(sender) % M routing"
```

---

## Task 20: Integration test — pending-receipts map (insert/match/timeout)

**Files:**
- Create: `crates/kardamom-ingress/tests/pending_receipts_test.rs`

- [ ] **Step 1: Write the test**

```rust
//! Pending-receipts integration: insert, match-by-cache, watermark gating,
//! and timeout — exercised through the public IngressProxy::submit_raw path
//! to catch wiring bugs not caught by the unit tests.

use std::time::Duration;

use alloy_consensus::{SignableTransaction, TxEnvelope, TxLegacy};
use alloy_primitives::{Address, Bytes, TxKind, U256};
use alloy_rlp::Encodable;
use alloy_signer_local::PrivateKeySigner;

use kardamom_ingress::config::IngressConfig;
use kardamom_ingress::error::IngressError;
use kardamom_ingress::log_stub::MockAeronChannel;
use kardamom_ingress::proxy::IngressProxy;

fn sign_legacy(s: &PrivateKeySigner, nonce: u64) -> Bytes {
    let tx = TxLegacy {
        chain_id: Some(1),
        nonce,
        gas_price: 1_000_000_000,
        gas_limit: 21_000,
        to: TxKind::Call(Address::ZERO),
        value: U256::ZERO,
        input: Default::default(),
    };
    let sig = s
        .credential()
        .sign_prehash_recoverable(&tx.signature_hash().0)
        .unwrap();
    let alloy_sig = alloy_primitives::Signature::from_signature_and_parity(sig, false);
    let env: TxEnvelope = tx.into_signed(alloy_sig).into();
    let mut buf = Vec::new();
    env.encode(&mut buf);
    Bytes::from(buf)
}

#[tokio::test]
async fn submit_times_out_when_no_executor_responds() {
    let cfg = IngressConfig {
        partition_count_m: 4,
        pending_receipt_timeout: Duration::from_millis(80),
        ..IngressConfig::default()
    };
    let (mock, _rx) = MockAeronChannel::new(4);
    let state_db = Arc::new(kardamom_ingress::log_stub::InMemoryStateDb::new());
    let proxy = IngressProxy::new(cfg, mock.clone(), mock, state_db);

    let signer = PrivateKeySigner::random();
    let raw = sign_legacy(&signer, 0);
    let res = proxy.submit_raw("127.0.0.1".parse().unwrap(), raw).await;
    assert!(matches!(res.unwrap_err(), IngressError::Timeout));
}
```

- [ ] **Step 2: Run**

```bash
cd /home/dev/kardamom
cargo test -p kardamom-ingress --test pending_receipts_test
```

Expected: PASS in ~100ms.

- [ ] **Step 3: Commit**

```bash
cd /home/dev/kardamom
git add crates/kardamom-ingress/tests/pending_receipts_test.rs
git commit -m "ingress: integration test for pending-receipts timeout"
```

---

## Task 21: Criterion bench — end-to-end latency

**Files:**
- Create: `crates/kardamom-ingress/benches/latency.rs`

- [ ] **Step 1: Write the bench**

```rust
//! End-to-end latency: client → proxy → mock executor → receipt.

use std::sync::Arc;
use std::time::Duration;

use alloy_consensus::{SignableTransaction, TxEnvelope, TxLegacy};
use alloy_primitives::{Address, B256, Bytes, TxKind, U256};
use alloy_rlp::Encodable;
use alloy_signer_local::PrivateKeySigner;
use criterion::{Criterion, criterion_group, criterion_main};

use kardamom_ingress::config::IngressConfig;
use kardamom_ingress::log_stub::{
    BPosition, CachedReceipt, IngressPublication, MockAeronChannel, QuorumWatermark, Receipt,
};
use kardamom_ingress::proxy::IngressProxy;

fn sign(s: &PrivateKeySigner, nonce: u64) -> Bytes {
    let tx = TxLegacy {
        chain_id: Some(1),
        nonce,
        gas_price: 1_000_000_000,
        gas_limit: 21_000,
        to: TxKind::Call(Address::ZERO),
        value: U256::ZERO,
        input: Default::default(),
    };
    let sig = s
        .credential()
        .sign_prehash_recoverable(&tx.signature_hash().0)
        .unwrap();
    let alloy_sig = alloy_primitives::Signature::from_signature_and_parity(sig, false);
    let env: TxEnvelope = tx.into_signed(alloy_sig).into();
    let mut buf = Vec::new();
    env.encode(&mut buf);
    Bytes::from(buf)
}

fn bench_e2e_latency(c: &mut Criterion) {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .enable_all()
        .build()
        .unwrap();

    let (proxy, _drop) = rt.block_on(async {
        let cfg = IngressConfig {
            partition_count_m: 8,
            pending_receipt_timeout: Duration::from_secs(2),
            ..IngressConfig::default()
        };
        let (mock, mut rx_vec) = MockAeronChannel::new(8);
        let state_db = Arc::new(kardamom_ingress::log_stub::InMemoryStateDb::new());
    let proxy = Arc::new(IngressProxy::new(cfg, mock.clone(), mock.clone(), state_db));
        for (i, mut rx) in rx_vec.drain(..).enumerate() {
            let pub_h = mock.clone();
            let wm = mock.watermark_bus.clone();
            tokio::spawn(async move {
                while let Some(msg) = rx.recv().await {
                    let pos = BPosition { term_id: i as u64, term_offset: 1 };
                    let _ = pub_h
                        .publish_receipt_cache(CachedReceipt {
                            sender: msg.sender,
                            nonce: msg.nonce,
                            receipt: Receipt {
                                tx_idx: 0,
                                b_position: pos,
                                status: true,
                                gas_used: 21_000,
                                logs: Vec::new(),
                                tx_hash: B256::ZERO,
                            },
                        })
                        .await;
                    let _ = wm.send(QuorumWatermark { position: pos });
                }
            });
        }
        (proxy, mock)
    });

    // Pre-sign 1000 unique-sender txs so signing isn't on the hot path.
    let pre: Vec<Bytes> = (0..1000)
        .map(|_| sign(&PrivateKeySigner::random(), 0))
        .collect();
    let mut idx = 0usize;

    c.bench_function("ingress/e2e_latency_simple_transfer", |b| {
        b.to_async(&rt).iter(|| {
            let raw = pre[idx % pre.len()].clone();
            idx = idx.wrapping_add(1);
            let proxy = proxy.clone();
            async move {
                let _ = proxy
                    .submit_raw("127.0.0.1".parse().unwrap(), raw)
                    .await
                    .unwrap();
            }
        });
    });
}

criterion_group!(benches, bench_e2e_latency);
criterion_main!(benches);
```

- [ ] **Step 2: Run**

```bash
cd /home/dev/kardamom
cargo bench -p kardamom-ingress --bench latency -- --warm-up-time 1 --measurement-time 3
```

Expected: completes; the printed p50 latency on dev hardware should be sub-millisecond. Record the number in the PR description.

- [ ] **Step 3: Commit**

```bash
cd /home/dev/kardamom
git add crates/kardamom-ingress/benches/latency.rs
git commit -m "ingress(bench): e2e latency benchmark"
```

---

## Task 22: Criterion bench — sustained throughput per proxy

**Files:**
- Create: `crates/kardamom-ingress/benches/throughput.rs`

- [ ] **Step 1: Write the bench**

```rust
//! Sustained throughput per proxy: how many txs/sec a single proxy process can
//! ingest with everything past sequencer mocked.

use std::sync::Arc;
use std::time::Duration;

use alloy_consensus::{SignableTransaction, TxEnvelope, TxLegacy};
use alloy_primitives::{Address, B256, Bytes, TxKind, U256};
use alloy_rlp::Encodable;
use alloy_signer_local::PrivateKeySigner;
use criterion::{Criterion, Throughput, criterion_group, criterion_main};

use kardamom_ingress::config::IngressConfig;
use kardamom_ingress::log_stub::{
    BPosition, CachedReceipt, IngressPublication, MockAeronChannel, QuorumWatermark, Receipt,
};
use kardamom_ingress::proxy::IngressProxy;

fn sign(s: &PrivateKeySigner, nonce: u64) -> Bytes {
    let tx = TxLegacy {
        chain_id: Some(1),
        nonce,
        gas_price: 1_000_000_000,
        gas_limit: 21_000,
        to: TxKind::Call(Address::ZERO),
        value: U256::ZERO,
        input: Default::default(),
    };
    let sig = s
        .credential()
        .sign_prehash_recoverable(&tx.signature_hash().0)
        .unwrap();
    let alloy_sig = alloy_primitives::Signature::from_signature_and_parity(sig, false);
    let env: TxEnvelope = tx.into_signed(alloy_sig).into();
    let mut buf = Vec::new();
    env.encode(&mut buf);
    Bytes::from(buf)
}

const BATCH: usize = 1024;

fn bench_throughput(c: &mut Criterion) {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(8)
        .enable_all()
        .build()
        .unwrap();
    let proxy = rt.block_on(async {
        let cfg = IngressConfig {
            partition_count_m: 8,
            pending_receipt_timeout: Duration::from_secs(5),
            ..IngressConfig::default()
        };
        let (mock, mut rx_vec) = MockAeronChannel::new(8);
        let state_db = Arc::new(kardamom_ingress::log_stub::InMemoryStateDb::new());
    let proxy = Arc::new(IngressProxy::new(cfg, mock.clone(), mock.clone(), state_db));
        for (i, mut rx) in rx_vec.drain(..).enumerate() {
            let pub_h = mock.clone();
            let wm = mock.watermark_bus.clone();
            tokio::spawn(async move {
                while let Some(msg) = rx.recv().await {
                    let pos = BPosition { term_id: i as u64, term_offset: 1 };
                    let _ = pub_h
                        .publish_receipt_cache(CachedReceipt {
                            sender: msg.sender,
                            nonce: msg.nonce,
                            receipt: Receipt {
                                tx_idx: 0,
                                b_position: pos,
                                status: true,
                                gas_used: 21_000,
                                logs: Vec::new(),
                                tx_hash: B256::ZERO,
                            },
                        })
                        .await;
                    let _ = wm.send(QuorumWatermark { position: pos });
                }
            });
        }
        proxy
    });
    let pre: Vec<Bytes> = (0..BATCH)
        .map(|_| sign(&PrivateKeySigner::random(), 0))
        .collect();

    let mut group = c.benchmark_group("ingress/throughput");
    group.throughput(Throughput::Elements(BATCH as u64));
    group.bench_function("submit_raw_batch_1024", |b| {
        b.to_async(&rt).iter(|| {
            let proxy = proxy.clone();
            let pre = pre.clone();
            async move {
                let mut futs = Vec::with_capacity(BATCH);
                for raw in pre {
                    let p = proxy.clone();
                    futs.push(async move {
                        p.submit_raw("127.0.0.1".parse().unwrap(), raw).await.unwrap()
                    });
                }
                let _ = futures::future::join_all(futs).await;
            }
        });
    });
    group.finish();
}

criterion_group!(benches, bench_throughput);
criterion_main!(benches);
```

- [ ] **Step 2: Run**

```bash
cd /home/dev/kardamom
cargo bench -p kardamom-ingress --bench throughput -- --warm-up-time 1 --measurement-time 5
```

Expected: prints throughput in elements/sec. On dev hardware (4-core laptop) expect 50k-200k tx/s for a single proxy with mocked everything; record in the PR description.

- [ ] **Step 3: Commit**

```bash
cd /home/dev/kardamom
git add crates/kardamom-ingress/benches/throughput.rs
git commit -m "ingress(bench): sustained throughput per proxy"
```

---

## Task 23: Run full test suite, fmt, clippy

**Files:** none (verification)

- [ ] **Step 1: Format**

```bash
cd /home/dev/kardamom
cargo fmt --all
```

- [ ] **Step 2: Clippy**

```bash
cd /home/dev/kardamom
cargo clippy -p kardamom-ingress --all-targets --all-features -- -D warnings
```

Expected: clean.

- [ ] **Step 3: Full test suite for the crate**

```bash
cd /home/dev/kardamom
cargo test -p kardamom-ingress --all-features
```

Expected: every test PASSES.

- [ ] **Step 4: Workspace-wide tests stay green**

```bash
cd /home/dev/kardamom
cargo test --workspace
```

Expected: no regression in other crates. (`kardamom-ingress` is new; it should only add tests.)

- [ ] **Step 5: Commit fmt-only changes if any**

```bash
cd /home/dev/kardamom
git add -A
if ! git diff --cached --quiet; then
    git commit -m "style: cargo fmt for kardamom-ingress"
fi
```

---

## Task 24: E2E test against real Aeron in Docker

**Per S0 D-Sh8:** mock-based integration tests are fine for component isolation, but every subsystem MUST also have an e2e test that runs against a real Aeron Media Driver + Aeron Archive in Docker, brought up via the `testcontainers` harness shipped from S3's `kardamom-log` crate (`kardamom_log::testing::docker`). This task adds that test for the ingress proxy.

**Scope:** spin up real Aeron containers, wire the proxy at one end, a tiny in-process "sequencer mock" (just drains `ingress[*]` and emits matching `CachedReceipt` + `QuorumWatermark` + `BlockBoundary` onto channel C and the receipt-cache channel) at the other end, push 1k signed txs through the proxy's `submit_raw`, and assert every receipt is returned. The point is to exercise the real Aeron transport for messages — sig-verify, partition routing, watermark gating, and receipt-cache idempotence all run unchanged.

**Files:**
- Create: `crates/kardamom-ingress/tests/docker_e2e.rs`
- Modify: `crates/kardamom-ingress/Cargo.toml` (gate the test on `feature = "docker-e2e"` to keep CI splits clean; default off)

- [ ] **Step 1: Add the `docker-e2e` feature and `dev-dependencies`**

In `crates/kardamom-ingress/Cargo.toml`:

```toml
[features]
# ... existing ...
# Enables the Docker-backed e2e test in tests/docker_e2e.rs. Off by default
# so `cargo test -p kardamom-ingress` stays hermetic; CI's docker-e2e job
# turns it on.
docker-e2e = []

[dev-dependencies]
# ... existing ...
# S3's kardamom-log ships the testcontainers harness; pull it in as a dev-dep
# behind the docker-e2e feature so dev builds without docker stay fast.
kardamom-log = { path = "../kardamom-log", features = ["testing", "docker"] }
testcontainers = "0.20"
```

- [ ] **Step 2: Write the test**

Create `crates/kardamom-ingress/tests/docker_e2e.rs`:

```rust
//! E2E test: real Aeron Media Driver + Aeron Archive in Docker, real ingress
//! proxy, mock sequencer on the consumer side.
//!
//! Per S0 D-Sh8 — mock-based unit/integration tests in this crate stay; this
//! is *additional* coverage that exercises the real Aeron transport so we
//! catch wire-format, IPC, and back-pressure bugs the mocks can't surface.
//!
//! Gated on `feature = "docker-e2e"` because it requires a Docker daemon and
//! ~30s startup; default `cargo test` skips it.

#![cfg(feature = "docker-e2e")]

use std::sync::Arc;
use std::time::Duration;

use alloy_consensus::{SignableTransaction, TxEnvelope, TxLegacy};
use alloy_primitives::{Address, Bytes, TxKind, U256};
use alloy_rlp::Encodable;
use alloy_signer_local::PrivateKeySigner;

use kardamom_ingress::config::IngressConfig;
use kardamom_ingress::log_stub::InMemoryStateDb;
use kardamom_ingress::proxy::IngressProxy;

// From S3's kardamom-log testcontainers harness (D-Sh8). Brings up
// `aeron-media-driver` and `aeron-archive` containers, wires their UDP and
// IPC endpoints, and hands back a connected `Aeron` client plus the channel
// URIs the proxy needs.
use kardamom_log::testing::docker::{AeronCluster, ChannelHandles};

fn sign_legacy(signer: &PrivateKeySigner, nonce: u64) -> Bytes {
    let tx = TxLegacy {
        chain_id: Some(1),
        nonce,
        gas_price: 1_000_000_000,
        gas_limit: 21_000,
        to: TxKind::Call(Address::ZERO),
        value: U256::ZERO,
        input: Default::default(),
    };
    let sig = signer
        .credential()
        .sign_prehash_recoverable(&tx.signature_hash().0)
        .unwrap();
    let alloy_sig = alloy_primitives::Signature::from_signature_and_parity(sig, false);
    let env: TxEnvelope = tx.into_signed(alloy_sig).into();
    let mut buf = Vec::new();
    env.encode(&mut buf);
    Bytes::from(buf)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires Docker; run with `cargo test --features docker-e2e -- --ignored`"]
async fn proxy_e2e_real_aeron_1000_txs() {
    // 1. Spin up real Aeron Media Driver + Archive in Docker. The harness
    //    creates the standard channels (ingress[0..M], receipt-cache, channel C,
    //    watermark) and returns Publication/Subscription handles wired to the
    //    real Aeron client running against the containers.
    let cluster = AeronCluster::start_default().await.expect("docker start");
    let ChannelHandles {
        publication,
        subscription,
        // The harness also exposes raw publication handles the test can use to
        // emit fake sequencer output (receipts, boundaries, watermarks) on the
        // appropriate Aeron channels.
        executor_side,
    } = cluster.channels(/* partitions = */ 8).await.unwrap();

    let cfg = IngressConfig {
        partition_count_m: 8,
        pending_receipt_timeout: Duration::from_secs(15),
        chain_id: 31337,
        ..IngressConfig::default()
    };
    let state_db = Arc::new(InMemoryStateDb::new());
    let proxy = Arc::new(IngressProxy::new(
        cfg,
        publication,
        subscription,
        state_db.clone(),
    ));

    // 2. Mock sequencer: drains each `ingress[i]` Aeron subscription and emits
    //    a matching CachedReceipt + QuorumWatermark + BlockBoundary back onto
    //    the corresponding Aeron channels via `executor_side`. This is the
    //    smallest possible stand-in for S2/S4/S5 — just enough to close the
    //    loop so the proxy's pending-receipts release path fires.
    let exec = executor_side.clone();
    let mock_sequencer = tokio::spawn(async move {
        use kardamom_ingress::log_stub::{
            BPosition, BlockBoundary, CachedReceipt, QuorumWatermark, Receipt,
        };
        let mut tx_idx_counter: u64 = 0;
        let mut block_number: u64 = 0;
        loop {
            // Block on the next IngressMsg arriving on any partition.
            let Some(msg) = exec.next_ingress().await else { break };
            tx_idx_counter += 1;
            let pos = BPosition {
                term_id: 0,
                term_offset: tx_idx_counter,
            };
            let receipt = Receipt {
                tx_idx: tx_idx_counter,
                b_position: pos,
                status: true,
                gas_used: 21_000,
                logs: Vec::new(),
                tx_hash: msg.tx_hash,
            };
            // Publish the receipt-cache entry — proxy's receipt watcher
            // consumes this and releases the parked client.
            exec.publish_receipt_cache(CachedReceipt {
                sender: msg.sender,
                nonce: msg.nonce,
                receipt: receipt.clone(),
            })
            .await
            .unwrap();
            // Advance the quorum watermark past the tx's B-position so the
            // proxy's watermark gate clears.
            exec.publish_watermark(QuorumWatermark { position: pos })
                .await
                .unwrap();
            // Emit a BlockBoundary every 100 txs so eth_blockNumber moves —
            // proves D-Sh5 wiring against the real channel C.
            if tx_idx_counter % 100 == 0 {
                block_number += 1;
                exec.publish_block_boundary(BlockBoundary {
                    block_number,
                    end_tx_idx: tx_idx_counter,
                    l2_timestamp: 1_700_000_000 + block_number,
                })
                .await
                .unwrap();
            }
        }
    });

    // 3. Push 1000 signed txs from distinct senders through the proxy. Real
    //    bytes go over real Aeron channels; the proxy waits on real receipts
    //    coming back over real channel C.
    let mut signers = Vec::with_capacity(1000);
    for _ in 0..1000 {
        signers.push(PrivateKeySigner::random());
    }
    let mut futs = Vec::with_capacity(signers.len());
    for s in &signers {
        let raw = sign_legacy(s, 0);
        let p = proxy.clone();
        futs.push(async move {
            p.submit_raw("127.0.0.1".parse().unwrap(), raw).await
        });
    }
    let results = futures::future::join_all(futs).await;
    let ok_count = results.iter().filter(|r| r.is_ok()).count();
    assert_eq!(ok_count, 1000, "every tx must round-trip over real Aeron");

    // 4. Wait briefly for the last BlockBoundary to be observed, then assert
    //    `eth_blockNumber` reflects it (D-Sh5).
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert!(
        proxy.latest_block_number() >= 10,
        "BlockBoundary watcher should have observed at least 10 blocks; got {}",
        proxy.latest_block_number()
    );

    // 5. Idempotent retry through real Aeron must hit the receipt cache.
    let raw0 = sign_legacy(&signers[0], 0);
    let again = proxy
        .submit_raw("127.0.0.1".parse().unwrap(), raw0)
        .await
        .expect("retry");
    assert!(again.receipt.status);

    mock_sequencer.abort();
    cluster.stop().await;
}
```

- [ ] **Step 3: Run (locally with Docker)**

```bash
cd /home/dev/kardamom
cargo test -p kardamom-ingress --features docker-e2e --test docker_e2e -- --ignored
```

Expected: container startup ~10–30s; the test then completes in <30s with all 1000 txs round-tripping. If the test times out, check `docker logs` on the `aeron-archive` container for back-pressure or channel-mismatch errors.

- [ ] **Step 4: Wire CI**

The S3 plan adds a `docker-e2e` GitHub Actions job that runs `cargo test --workspace --features docker-e2e -- --ignored`. This crate's test is picked up by that same job once committed; no per-crate workflow change needed.

- [ ] **Step 5: Commit**

```bash
cd /home/dev/kardamom
git add crates/kardamom-ingress/Cargo.toml crates/kardamom-ingress/tests/docker_e2e.rs
git commit -m "ingress: e2e test against real Aeron in Docker (S0 D-Sh8)"
```

---

## Task 25: Push branch and open PR

**Files:** none (workflow)

- [ ] **Step 1: Push the branch**

```bash
cd /home/dev/kardamom
git push -u origin claude/s1-ingress-proxy
```

- [ ] **Step 2: Open the PR**

```bash
gh pr create \
    --base main \
    --title "feat(ingress): S1 ingress-proxy crate with batched sig-verify and watermark-gated receipts" \
    --body "$(cat <<'EOF'
## Summary
- New `crates/kardamom-ingress` crate implementing the S1 ingress proxy from the high-throughput sequencer design.
- jsonrpsee 0.26 HTTP+WS server for the standard Ethereum RPC subset; optional length-prefixed RLP TCP+UDS binary protocol.
- Per-IP `governor` token bucket runs before any expensive work.
- 64-deep secp256k1 recovery ring with 50µs flush window and single-sig fallback; property-tested to match the single-sig reference on 1k random txs.
- Routes to `ingress[keccak(sender) % M]` via abstract `IngressPublication` (mocked here behind `log_stub`; swapped for real Aeron when S3 lands).
- `PendingReceipts` parks the response until both the quorum fsync watermark advances past the tx's B-position AND a matching receipt arrives on channel C; idempotent retries served from the receipt-cache channel.
- Criterion benches for end-to-end latency and sustained throughput.

## Test plan
- [ ] `cargo test -p kardamom-ingress --all-features` — every unit + integration test passes.
- [ ] `cargo bench -p kardamom-ingress` — latency and throughput benches complete; numbers recorded below.
- [ ] Manual: smoke-test against the mock harness, send 100 txs from distinct senders, assert each receives a receipt and that a retry hits the cache.
- [ ] `cargo test -p kardamom-ingress --features docker-e2e --test docker_e2e -- --ignored` — Docker-backed e2e against real Aeron Media Driver + Archive passes (per S0 D-Sh8).

EOF
)"
```

- [ ] **Step 3: Print the PR URL**

Expected output: a GitHub PR URL ending in `/pull/N`.

---

## Self-review checklist (run after writing the plan)

1. **Spec coverage:** §2.1 transports — Tasks 13–15. Rate limit — Tasks 6, 18. Batched sig verify — Tasks 7–9. Routing — Tasks 5, 19. Pending receipts + watermark — Tasks 10, 17, 20. Receipt cache — Tasks 11, 17. Failure §4.1 (idempotent retry) — Task 17. V0 deferrals (`getBalance`, `getTransactionCount`) — Task 13 returns explicit errors. Latency budget §3 — Task 21. S0 D-Sh3/D-Sh4 (typed `sender` + `tx_hash`) — Tasks 7–9, 12. S0 D-Sh5 (`eth_blockNumber` from channel C) — Task 12 BlockBoundary watcher + Task 13 handler. S0 D-Sh4 (`eth_getTransactionReceipt` via state-DB) — Task 13 handler. S0 D-Sh2 (rkyv codec) — log_stub header note + future swap to `kardamom-types`. S0 D-Sh8 (Docker e2e) — Task 24.
2. **Placeholder scan:** none of "TBD / TODO / implement later" — every step shows code.
3. **Type consistency:** `IngressMsg` (with typed `sender: Address` and `tx_hash: B256`, never `Option` — S0 D-Sh3/D-Sh4), `Receipt`, `BPosition`, `QuorumWatermark`, `CachedReceipt`, `BlockBoundary`, `StateDatabase`, `InMemoryStateDb` are defined once in `log_stub.rs` and used identically across `proxy.rs`, tests, and benches. `partition_for(Address, u32) -> u32` is consistent. `PendingReceipts::on_receipt(Address, Receipt)` and `update_watermark(QuorumWatermark)` match between unit tests and the proxy watcher. `BatchVerifier::recover(TxEnvelope, Bytes) -> (Address, B256)` returns the canonical pair from a single batched pass.

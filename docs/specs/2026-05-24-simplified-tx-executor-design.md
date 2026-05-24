# Simplified Tx-Executor — System Design

**Status:** Draft
**Date:** 2026-05-24
**Scope:** Alternative system design that collapses the prior 7-component pipeline into 3 components. Sits alongside the prior design (PRs #12–#20) rather than replacing it on `main`; the two are evaluated against each other in implementation.

---

## Overview

The prior architecture (`docs/specs/2026-05-23-high-throughput-sequencer-design.md`) decomposed the L2 pipeline into seven components: ingress proxy, sequencer, canonical-log recorder (channel B), block-STM executor, block sealer, state writer, and L1 batcher. The decomposition was driven by horizontal scalability targets (1M+ tx/s aspirational) and explicit replication boundaries.

In practice, EVM's shared-state model makes cross-host execution parallelism hard regardless of how the pipeline is sliced; the prior design's parallelism gain is dominated by what Block-STM provides *within* one executor process, not by running multiple executor processes. Recognizing that, this design collapses everything between the ingress proxy and the L1 batcher into a single `tx-executor` component. The result is three components instead of seven, ~30% lower end-to-end ack latency, and one canonical log (a WAL) instead of two channels (B + C).

The prior design remains a viable alternative for operators who want the multi-replica execution / receipt-divergence-panic safety story. This design optimizes for operational simplicity and minimum ack latency at the cost of a tighter single-host scalability ceiling.

---

## Goals and non-goals

### Goals
- **Latency:** sub-millisecond post-execution + post-fsync ack. Target ~150–180µs end-to-end on the LAN.
- **Throughput:** 100k–500k tx/s sustained for the v0 single-thread executor; up to ~1M tx/s with Block-STM in v1, all on one host.
- **Determinism:** the active executor is the single source of truth for canonical order and state. Standbys verify by replaying the WAL at promotion time.
- **Crash fault tolerance:** survive single-host failure with no data loss as long as the host's NVMe is recoverable post-crash; bounded data loss otherwise (recoverable via standby tail).
- **Operational simplicity:** three deployable components total. One canonical log (WAL). No external KV (etcd, ZooKeeper). No separate sealer process.
- **Backward compatibility with existing crates:** salvage `kardamom-types`, `kardamom-log` testing primitives, `kardamom-leases`, the entire ingress proxy (S1), and most of S6's libmdbx schema and S7's batcher/contract — see Migration.

### Non-goals
- Multi-host execution parallelism. Single-active executor; horizontal scaling is out of scope.
- Multi-replica execution determinism enforcement (the prior design's `Receipt.write_set_hash` cross-replica check is dropped — only one replica is producing receipts).
- Public mempool / gossip / replacement-by-fee. Direct client→proxy submission only.
- Byzantine fault tolerance. CFT — single trusted operator.
- Fraud proofs / validity proofs at L1. Settlement contract is a pure DA sink (same as prior design, D-Sh11).
- Cross-chain composability.

---

## Decisions

| # | Decision | Rationale |
|---|---|---|
| D1 | Three-component topology (proxy, tx-executor, batcher) | Operational simplicity; matches actual EVM parallelism boundary |
| D2 | Single-active tx-executor + hot WAL-tailing standbys | CFT model — operator-trusted. Failover replays WAL tail (already in standby RAM) |
| D3 | Ack iff local NVMe fsync of WAL passes the tx's offset | Simplest durability boundary. Sub-ms achievable with enterprise NVMe + io_uring + PLP |
| D4 | Per-sender pending nonce buffer (same as prior design's sequencer) | Tolerates client out-of-order sends (MetaMask UI race conditions) |
| D5 | Executor emits BlockBoundary records inline into the WAL every 250ms | No separate sealer process. Eliminates lease election entirely |
| D6 | WAL is the canonical log; no separate channels B and C | One stream type, one codec, one set of recovery semantics |
| D7 | L1 batcher reads on-disk WAL segments (same offline pattern as prior D-Sh10) | Decoupled from live executor; can be down arbitrarily |
| D8 | State persistence: in-process libmdbx, written by a background thread per block boundary | Same as prior S6 design but in-process; no IPC, no separate state-writer service |
| D9 | rkyv wire codec; reuse `kardamom-types` types where applicable | Continuity with prior crates |
| D10 | V0 executor is sequential revm; Block-STM is v1 (same deferral as prior design) | Block-STM revm integration is the longest pole; not blocking on it for v0 |

---

## Architecture

### Topology

```
[client]
   │ JSON-RPC over HTTP/WS  or  binary line protocol over TCP/UDS
   ▼
[ingress proxy cluster, stateless, replicated]
   │ sig verify (batched), rate limit, decode
   │ computes (sender, tx_hash); emits TxEnvelope
   │ Aeron multi-publisher → tx-executor's ingress channel
   ▼
┌───────────────────────────────────────────────────────────────────┐
│  [tx-executor: single-active, kardamom-aeron pinned OS thread]    │
│                                                                   │
│  ingress queue (lock-free MPSC)                                   │
│       │                                                           │
│       ▼                                                           │
│  nonce gate: per-sender HashMap<Address, NextNonce>               │
│              + per-sender BTreeMap<u64, TxEnvelope> pending buf   │
│       │                                                           │
│       ▼                                                           │
│  arrival-index assignment (monotonic counter)                     │
│       │                                                           │
│       ▼                                                           │
│  W Block-STM workers (v0: 1 thread; v1: W = cores − 2)            │
│       │   execute(idx) → revm against MV-memory                   │
│       │   validate(idx) → conflict? re-execute : commit           │
│       ▼                                                           │
│  commit thread (single, in-index order):                          │
│      appends WalRecord::Tx { tx_hash, idx, receipt, bal, delta }  │
│  every 250ms appends WalRecord::BlockBoundary { num, idx, ts }    │
│       │                                                           │
│       ▼                                                           │
│  WAL append buffer (in-memory, mmap-backed, append-only)          │
│       │       │                                                   │
│       │       └──► WAL replication channel (Aeron-backed) ───────►│ standby executors tail in RAM
│       ▼                                                           │
│  io_uring fsync thread (continuous, batched O_DSYNC)              │
│       │                                                           │
│       ▼                                                           │
│  fsync_position atomic counter                                    │
│       │                                                           │
│       └──► fsync-watermark stream (Aeron-backed) ────────────────►│ proxies subscribe; release receipt when watermark ≥ tx's WAL offset
│                                                                   │
│  state thread (background):                                       │
│      every BlockBoundary, opens libmdbx write-txn, applies        │
│      buffered deltas, commits. Executor reads state via mdbx      │
│      snapshot (snapshot-swap protocol same as prior S6)           │
└───────────────────────────────────────────────────────────────────┘
   │
   │ (WAL replication, fsync-watermark, optional metrics)
   ▼
[hot-standby tx-executors] tail WAL in RAM; on active failure, deterministic-lowest-host-id (kardamom-leases) promotes, resumes at tail
   │
   │ (no live coupling)
   ▼
[L1 batcher, offline]
   reads on-disk WAL segments from any recorder host's NVMe
   extracts raw txs per BlockBoundary range
   packs into Ethereum 4844 blobs (max 6/L1-block, ~750KB compressed)
   posts to KardamomL2Settlement.sol (DA-sink contract)
```

### System invariants

- **I1: Canonical order = WAL append position.** Nothing else is canonical. The WAL's monotonic byte offset (or `(segment_id, segment_offset)`) is the tx identifier used by every downstream consumer.
- **I2: Ack iff WAL-fsynced.** The proxy releases the receipt to the client only after the active executor's `fsync_position` atomic counter passes the tx's WAL offset. No quorum wait. Standbys are for availability, not for the ack boundary.
- **I3: Execution is deterministic from `(stream A → arrival index → Block-STM scheduler)`.** The active executor is the single source of truth. Standbys verify by replay at promotion time (not in the hot path).
- **I4: State DB is a derived view of the WAL.** Cold start: load libmdbx snapshot at `meta.last_committed_block` and replay WAL records from `meta.last_committed_position` forward.

---

## Tx-executor internals

The tx-executor is one process containing six logical units, all sharing an Arc-friendly state DB handle and a series of single-producer/single-consumer (or MPSC) channels for the data path. Where Aeron is involved, it's confined to a dedicated OS thread (the standard `!Send + !Sync` pattern we learned in S3).

### Units

1. **Ingress receiver (Aeron thread):** subscribes to the proxy → executor Aeron channel. Pulls `Archived<TxEnvelope>`s zero-copy; forwards owned `TxEnvelope` via crossbeam channel to the nonce gate.
2. **Nonce gate (one thread, lock-free state):** owns `HashMap<Address, NextNonce>` and `HashMap<Address, BTreeMap<u64, TxEnvelope>>` (per-sender pending buffers). For each tx:
   - `tx.nonce == expected`: pass through; advance counter; drain any newly-eligible buffered tx.
   - `tx.nonce > expected`: buffer (bounded; default 16/sender).
   - `tx.nonce < expected`: drop + emit `CachedReceipt` "duplicate" on the receipt-cache channel.
3. **Arrival-index assigner (inline with the gate):** stamps each `TxEnvelope` with a monotonic `arrival_idx: u64`.
4. **Block-STM worker pool (W threads, v0 has W=1):** pulls `(arrival_idx, TxEnvelope)` from the queue. Runs revm against an MV-memory layer over the current state snapshot. Records `(read_set, write_set, receipt, bal)`. On validation conflict, re-executes against the latest committed values. v0 = one worker, no conflicts possible by construction. v1 = W = `num_cores − 2`.
5. **Commit thread:** drains finished `(arrival_idx, …)` in monotonic order. For each tx, emits a `WalRecord::Tx`. Every 250ms (rounded to next multiple), inserts a `WalRecord::BlockBoundary` between txs.
6. **WAL writer (one thread, Aeron + io_uring):** owns the append-only WAL file(s). Two concurrent activities:
   - **Append:** copies commit-thread records into the WAL's `mmap`-backed buffer; immediately publishes them on the WAL replication channel for standbys.
   - **Continuous fsync (io_uring):** pipelined `IORING_OP_WRITE` (or `O_DIRECT` mirror) + `IORING_OP_FSYNC(FDATASYNC)` so the kernel queue stays full. On completion, advances the atomic `fsync_position` counter; publishes the new watermark on the fsync-watermark Aeron stream.

### State thread (separate background thread)

7. **State writer:** subscribes to the commit thread's output (or reads back from the WAL — either source works since they're equivalent). At each `BlockBoundary`, opens one libmdbx write-txn and applies the buffered `state_delta`s for that block. Maintains the `tx_hash_index: tx_hash → (segment_id, segment_offset)` table for `eth_getTransactionReceipt(hash)`. Schema otherwise identical to the prior S6 design (minus `state_root_commitment` per D-Sh11).

### Threading model

- **One dedicated OS thread per Aeron-touching role** (ingress receiver, WAL writer, fsync). Rationale: `rusteron_client::Aeron` is `!Send + !Sync`; we cannot let tokio move tasks across worker threads. This is the same pattern established in S3.
- **Block-STM workers** are plain OS threads, share MV-memory via `Arc`/atomics. Pinned to specific cores via `core_affinity`.
- **Commit thread, nonce gate, state thread** are plain OS threads; communicate via crossbeam channels.
- **No tokio runtime inside the executor.** Async is for the proxy and the batcher.

---

## WAL format

The WAL is an append-only sequence of length-prefixed rkyv-archived records, segmented into ~256 MB files (configurable). One canonical record type, multi-variant:

```rust
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize)]
pub enum WalRecord {
    Tx {
        arrival_idx: u64,
        tx_hash: B256,
        sender: Address,
        nonce: u64,
        raw_tx: Bytes,        // for L1 batcher reconstruction
        receipt: Receipt,
        bal: BlockAccessList, // EIP-7928-style (address, slot) touched
        state_delta: StateDelta,
    },
    BlockBoundary {
        block_number: u64,
        end_arrival_idx: u64,
        l2_timestamp: u64,
    },
    DuplicateNotification {
        sender: Address,
        nonce: u64,
        prior_tx_hash: B256,
    },
}
```

**File layout:** under `KARDAMOM_WAL_DIR`, segments named `wal-<segment_id:020d>.bin`. Each segment is `mmap`-mapped with `MAP_POPULATE`; writes go into the mapped region and `msync(MS_ASYNC)` is paired with the io_uring fsync of the underlying fd. On segment rollover, the previous segment's last fsync completes before the next opens.

**WAL position:** `(segment_id: u64, segment_offset: u64)`. Equivalent to `BPosition` in the prior design and used the same way (downstream identifiers).

**Replication channel:** as each record is appended, the same bytes are published on an Aeron multi-publisher stream that hot standbys subscribe to. Standbys hold the records in RAM and (optionally) write to their own local WAL in the background; they don't need to fsync because they're not the ack source.

---

## Data flow & latency budget

### End-to-end trace (simple-transfer tx, LAN, target sub-ms)

```
t=0      Client TCP send
t+50µs   Proxy NIC, decode frame
t+80µs   Sig enters batch-verify ring; (sender, tx_hash) computed
t+130µs  Batch verify flushes
t+135µs  Proxy: Aeron pub → executor's ingress channel
t+140µs  Executor's ingress receiver dequeues
t+143µs  Nonce gate (HashMap lookup, advance)
t+145µs  Worker picks Execute(arrival_idx)
t+190µs  revm done (~45µs simple transfer)
t+192µs  Commit thread: append WalRecord::Tx to mmap buffer
t+193µs  WAL writer: Aeron pub (replication) + io_uring submit (fsync)
t+215µs  NVMe completion (~22µs, enterprise + PLP)
         fsync_position advances; watermark published
t+217µs  Proxy receives Receipt on receipt channel; watermark already past offset
t+218µs  Proxy releases response
t+268µs  Client TCP receives receipt
```

**Total: ~268µs** end-to-end. Execution + fsync overlap because the commit thread's mmap append happens before io_uring submission completes, and io_uring's submit-and-wait is interleaved with revm running on the next tx.

### Latency budget

| Source | Typical cost | On critical path? |
|---|---|---|
| Client↔NIC LAN RTT | 50µs each way | yes |
| Sig verify (batched) | ~5–10µs amortized | yes |
| Aeron IPC hops (2× — proxy→exec, exec→proxy receipt) | ~5µs each | yes |
| Nonce gate lookup | ~3µs | yes |
| Block-STM execute + validate (v0 sequential — no validation cost) | ~45µs simple, +5–50µs per re-exec at v1 | yes |
| Commit thread + mmap append | ~3µs | yes |
| io_uring fsync (NVMe + PLP) | ~22µs | yes |
| Receipt publish + delivery | ~5µs | yes |

**Slack vs 1000µs target: ~700µs.** Comfortable for v0; v1 Block-STM under hot-contract workloads will eat into this.

### Throughput math (v0, sequential revm)

- Per-tx CPU on critical path: sig verify (amortized) ≈10µs at proxy, nonce gate ≈3µs, revm ≈45µs, commit + WAL ≈10µs at executor.
- One executor host @ one Block-STM worker: bounded by revm ≈45µs/tx ⇒ ~22k tx/s ceiling per worker. With v0=1 worker, this is the ceiling.
- v1 = W=8 workers w/ Block-STM, well-behaved workload: ~150k tx/s realistic; ~300k tx/s achievable on contention-light synthetics.
- WAL bandwidth: 150k tx/s × ~250B/record ≈ 37 MB/s. NVMe sustains; replication on 10GbE comfortable.

---

## Failure handling

### Active executor crash
- **Detection:** standbys observe the replication stream lapse + heartbeat absence (default 500ms timeout).
- **Recovery:** standbys race a deterministic-lowest-host-id election (reuse `kardamom-leases` from the prior design). Winner promotes:
  1. Replays any WAL records it has in RAM that haven't been fsynced (re-emits them on a fresh WAL on its own disk; same `arrival_idx`).
  2. Reads `meta.last_committed_position` from libmdbx; reconciles with WAL — if libmdbx is ahead of WAL (shouldn't happen with the current order: commit → WAL append → fsync → libmdbx-on-boundary), roll back libmdbx via MVCC.
  3. Begins accepting from stream A.
- **User impact:** ack flow for in-flight txs may time out at the proxy; client retries are idempotent (nonce-based — sender's stored nonce in libmdbx is the dedup key).
- **Data loss window:** anything in the active's RAM that wasn't yet replicated to a standby AND wasn't fsynced. With continuous io_uring fsync ≤ 100µs latency and continuous Aeron replication ≤ 5µs latency, the window is microseconds.

### Standby crash
- **Recovery:** standby restarts; catches up by reading WAL replication from the active from its last known position. If too far behind, falls back to copying WAL segments from active over rsync/HTTP.
- **User impact:** zero.

### NVMe stall on active
- **Detection:** `fsync_position` watermark stops advancing.
- **Recovery:** if stall < 1s, proxies queue (in-flight acks delayed but completed once NVMe recovers). If > 1s, operator policy decides whether to demote active to a standby with healthy NVMe.
- **User impact:** ack tail latency spike; no data loss.

### Proxy crash
- Same as the prior S1 design: L4 load-balancer reroutes; in-flight client requests time out; retries idempotent.

### Whole-host failure (active)
- Active host loses power: anything not yet fsynced AND not replicated is lost (microseconds-window). Standby promotes per the above; replays WAL tail from RAM; begins accepting.

### L1 batcher crash
- Stateless w.r.t. live pipeline; restarts; resumes from `lastBatchIndex` view on the L1 contract.

### Determinism violation detection
- **Limitation acknowledged:** with only one active executor, there's no cross-replica `write_set_hash` check in the hot path. Detection is delayed to standby promotion: the promoted standby replays the WAL and re-executes to verify; if it produces a different `state_delta` for any tx, the chain halts and operators investigate.
- Operators worried about silent corruption on the active should run an offline "shadow executor" that re-executes the WAL on a separate host and compares; it doesn't need to be in the ack path.

---

## State persistence (libmdbx)

Same schema and operational characteristics as the prior S6 design:

- Tables: `accounts`, `storage`, `code`, `headers` (no `state_root_commitment` per D-Sh11), `receipts` (optional), `tx_hash_index`, `meta`.
- libmdbx 0.6 (or `signet-libmdbx 0.8` if license preference demands MIT/Apache).
- MVCC version horizon sized for ~4 blocks of 25 MB writes = ~100 MB live snapshot pages.
- Snapshot-swap protocol on block boundary: pause reader briefly, open new `mdbx_txn`, resume. Microsecond pause.
- Cold-start recovery: read `meta`, open snapshot, replay WAL from `last_committed_position`.

Differences from prior S6 design:
- Lives in-process with the executor (no separate `kardamom-state` process; the state thread is a thread inside the tx-executor crate).
- No external IPC; channel from commit thread to state thread is a crossbeam mpsc.

---

## Testing strategy

Same shape as the prior design, scaled down to fewer components.

### Unit
Per-crate tests using `kardamom-log`'s `testing` feature's in-memory channel fakes. Standard.

### Determinism conformance
- Run two tx-executor instances against the same recorded stream-A trace. Assert byte-identical WAL output.
- Replay an archived WAL twice on the same executor (cold start each time); assert byte-identical libmdbx state at every block boundary.
- Differential test against single-threaded revm reference on historical mainnet tx corpora.

### Re-execution stress (v1)
- Same as prior design's Block-STM stress: hot-contract workloads (N senders writing the same slot); assert correctness, measure re-execution rate.

### Chaos / E2E
- All chaos tests run against real Aeron in Docker (same harness from `kardamom-log` shipped in PR #13/#20).
- Kill active executor mid-flight; assert standby promotion within bounded latency; no nonce gaps in the resulting WAL.
- Inject NVMe stall on fsync; assert proxies queue; assert recovery.
- Whole-host kill on active; assert standby promotion, no data loss with ≥ 1 standby alive.

### Performance
- Per-stage metering: sig verify, nonce gate, revm, commit, WAL append, fsync watermark, libmdbx burst.
- End-to-end p50/p99/p999 latency at sustained load (10k, 100k, 500k tx/s).
- v1 Block-STM stress: hot-contract workload mix; effective parallelism.

### L1 round-trip
- Post a batch of L2 blocks to the settlement contract on Anvil; reconstruct the L2 chain from L1 calldata + blobs; assert reconstructed receipts match the WAL.

### Soak
- 24h+ runs under bench-harness mix. Watch for: WAL fragmentation, mmap region exhaustion, libmdbx freelist growth, Aeron buffer leaks, fsync watermark drift, replication lag.

---

## Migration / salvage from prior PRs

The prior 8 PRs (#12–#20) remain open and CI-green as an alternative; this design proceeds on its own branch. Salvage from the prior crates:

| Crate / file | Survives? | Notes |
|---|---|---|
| `crates/kardamom-types` | ✅ Mostly | `BPosition` → `WalPosition` (same shape, renamed). `TxEnvelope`, `Receipt`, `BlockBoundary` survive. `BlockBoundaryStart` deleted (no sealer). `FsyncWatermark`, `QuorumWatermark`, `CachedReceipt` survive. `StateDatabase` / `SnapshotSource` traits survive (now implemented in-process). Add: `BlockAccessList`, `StateDelta`, `WalRecord` enum. |
| `crates/kardamom-log` | ✅ Partial | Aeron primitives and `testing` feature fakes survive (used for stream A, WAL replication channel, fsync-watermark stream, receipt-cache channel). The B/C-specific high-level adapters (`ChannelB`, `ChannelC` from PR #20) are renamed/repurposed for stream A and WAL-replication. Recorder + io_uring fsync sidecar move into the tx-executor crate. |
| `crates/kardamom-leases` | ✅ Wholesale | Used for standby promotion lease election. No changes. |
| `crates/kardamom-ingress` (S1) | ✅ Wholesale | The proxy is unchanged. It still emits `TxEnvelope` and subscribes to a receipt channel + fsync-watermark stream. The receipt channel is now part of the tx-executor's output and the fsync-watermark is the executor's local watermark. |
| `crates/kardamom-sequencer` (S2) | 🟡 Folded | Nonce gate + per-sender pending buffer + dedup logic moves into `kardamom-tx-executor` as one of the threads. Hot-standby tailer logic is repurposed for the executor's hot standby. |
| `crates/kardamom-executor` (S4) | 🟡 Folded | revm integration + state interface + write-set hashing fold into `kardamom-tx-executor` as the Block-STM worker pool. v0 keeps it sequential (W=1). |
| `crates/kardamom-sealer` (S5) | ❌ Deleted | Block boundaries emitted inline by the executor's commit thread. |
| `crates/kardamom-state` (S6) | 🟡 Folded | Schema + libmdbx integration + snapshot-swap protocol fold into the executor's state thread. The `kardamom-state` crate as a separate deployable goes away. |
| `crates/kardamom-batcher` (S7) | ✅ Mostly | Solidity contract + 4844 packing + reconstruction logic survive unchanged. Input source changes from "channel B archive segment files" to "WAL segment files" — same shape (length-prefixed rkyv records), different file naming. |
| `crates/kardamom-e2e` (PR #20) | ✅ Mostly | Full-pipeline test rewritten to bring up the simpler topology (proxy + tx-executor + batcher); fewer setup steps. Real Aeron Docker harness same. |

### New crate

- **`crates/kardamom-tx-executor`** — the big new crate. Contains:
  - Ingress receiver (Aeron subscriber, dedicated thread)
  - Nonce gate (thread + state)
  - Arrival-index assigner
  - Block-STM worker pool (v0: 1 worker; v1: N workers)
  - Commit thread
  - WAL writer (mmap + Aeron replication pub + io_uring fsync)
  - State thread (libmdbx writer + snapshot manager)
  - Hot-standby tailer mode (same binary, different config)
  - Lease coordination (via `kardamom-leases`)

Estimated size: ~12k–18k LOC. The most complex of the new crates but still smaller than the sum of `kardamom-sequencer` + `kardamom-executor` + `kardamom-sealer` + `kardamom-state` in the prior design (~25k LOC combined).

### Implementation order
1. `kardamom-types` updates (add `WalRecord`, `BlockAccessList`, `StateDelta`; rename `BPosition` → `WalPosition`).
2. `kardamom-tx-executor` skeleton + WAL writer + fsync sidecar.
3. Sequential revm integration (v0).
4. State thread (libmdbx) integration — reusing schema from S6 PR.
5. Hot-standby tailer mode + lease coordination.
6. Wire to existing `kardamom-ingress` (S1).
7. Update `kardamom-batcher` to read WAL files instead of Aeron Archive.
8. End-to-end Docker test exercising proxy + tx-executor + batcher against real Aeron.
9. Sustained-load benchmarks.

Each step is its own PR.

---

## Out-of-scope / follow-ups

- **Multi-host execution parallelism** (the prior design's nominal but never-realized win).
- **Block-STM revm integration** — v1, separate spec, same deferral as prior design's S4.
- **Validator subsystem** for state-root attestation on L1 — deferred (prior D-Sh11).
- **State sync / fast sync** for fresh nodes — separate follow-up.
- **Replacement-by-fee** — needs same-nonce arbitration logic.
- **Public mempool / gossip** — out of scope; CFT model assumes direct submission.
- **DA targets other than Ethereum 4844 blobs** — out of scope.
- **Byzantine fault tolerance** — out of scope.

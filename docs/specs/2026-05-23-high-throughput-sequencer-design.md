# High-Throughput Sequencer — System Design

**Status:** Draft
**Date:** 2026-05-23
**Scope:** System-level design. Hands off to per-subsystem specs (S1–S7) listed at the end.

---

## Overview

Kardamom today is a single-process Rust EVM rollup node built around revm. This document specifies how it evolves into a distributed, fault-tolerant sequencer/executor pipeline whose performance envelope is comparable to HFT-grade exchange matchers: **1M+ tx/s sustained, sub-millisecond post-execution acknowledgement**, on commodity LAN hardware.

The design is framework-shaped: the same topology should be deployable as a general-purpose EVM L2 or as an app-specific chain (perp DEX, order book), with plug points for client transports and the L1 settlement target.

---

## Goals and non-goals

### Goals
- **Throughput:** 1M+ tx/s sustained, generic EVM workload.
- **Latency:** sub-millisecond post-execution ack. The ack carries the full receipt (status, gas, logs) and is durable across single-host failure.
- **Determinism:** all executor replicas produce byte-identical receipts from the canonical log.
- **Crash fault tolerance:** survive single-host failures with no data loss and no user-visible interruption beyond a brief tail-latency excursion.
- **Framework reuse:** clean subsystem boundaries; deployable across multiple chain configurations.

### Non-goals (v1)
- Byzantine fault tolerance among sequencer operators. CFT only; operator is trusted.
- Fraud proofs and validity proofs at L1. Settlement contract integration only.
- Public mempool, gossip, replacement-by-fee. Direct client→proxy submission only.
- Cross-chain composability (asynchronous messaging, IBC-style).
- State sync / snapshot ingest for fresh nodes. Cold start from genesis only; snapshot ingest is a follow-up spec.

---

## Decisions

| # | Decision | Rationale |
|---|---|---|
| D1 | System-level design doc producing 7 subsystem specs | Pipeline is tightly coupled; needs a single architectural commitment before per-subsystem plans |
| D2 | Generic high-throughput rollup framework target | Must handle arbitrary EVM workloads; no static access-set assumptions |
| D3 | 1M+ tx/s, sub-ms post-execution ack | HFT-class envelope |
| D4 | CFT with hot standby per role | Sub-ms incompatible with BFT consensus; rollup model lets L1 fraud-proof layer (deferred) catch operator misbehavior |
| D5 | Multi-active sequencer publishers, no leader; single shared Aeron archive | Aeron multi-publisher into one channel serializes into a canonical byte stream; the archive's term/offset numbering *is* the canonical order |
| D6 | In-process Block-STM per executor replica; sender-partitioning lives only at the sequencer | STM handles arbitrary access sets; per-replica STM gives parallelism, replicas give redundancy. Cross-process sharding by `tx.to` collapses on hot contracts (USDC, WETH) |
| D7 | Post-execution ack: client sees receipt after the tx is fsynced on Q recorders, executed, and a receipt published | Single clean guarantee for clients |
| D8 | N total recorders with quorum Q (default N=3, Q=2); RAM propagation + continuous background `io_uring` fsync | RAM keeps the pipeline fast; fsync runs in parallel with execution; quorum tolerates N−Q simultaneous failures with no ack stall |
| D9 | Continuous receipt log + virtual block boundaries every 250ms | Inclusion latency decoupled from block cadence; virtual blocks are snapshot/checkpoint units |
| D10 | Native Ethereum 4844 blob batcher to fixed L1 settlement contract | One canonical DA path for v1; other DA targets deferred |

---

## Architecture

### Topology

```
[client]
   │ JSON-RPC over HTTP/WS  or  binary line protocol over TCP/UDS
   ▼
[ingress proxy cluster, stateless, replicated]
   │ sig verify (batched), rate limit, decode
   │ Aeron pub → ingress[keccak(sender) % M]
   ▼
[sequencer cluster: M processes, multi-active]
   │ exclusive ownership of sender slice
   │ pending-nonce HashMap (lock-free; single owner)
   │ Aeron concurrent-pub → channel B (all sequencers → one shared stream)
   ▼
[canonical archive: channel B]
   │ Aeron multi-publisher; N hosts each running Aeron Archive recorder (default N=3)
   │ RAM propagation < 10µs LAN
   │ continuous io_uring fsync thread per recorder (background, parallel)
   │ quorum fsync-watermark stream (Q-of-N, default Q=2) consumed by proxies
   ▼
[executor cluster: N replicas]
   │ each replica: 1 reader + W Block-STM workers + 1 commit thread
   │ MV-memory + read-set replay + scheduler
   │ deterministic byte-identical receipts across replicas
   │ Aeron pub → channel C (receipts + block boundaries)
   ▼
[receipt channel C]   ← block sealer publishes boundary markers into B (not C)
   │ multi-publisher; RAM only (regenerable from B + state snapshot)
   │
   ├──► [ingress proxy]    ← matches (sender, nonce); waits for fsync watermark; releases response
   ├──► [state writer]     ← per-host; burst-applies receipts to libmdbx at block boundaries
   └──► [L1 batcher]       ← singleton + standby; packs sealed blocks into 4844 blobs
            │
            ▼
       [Ethereum L1: settlement contract]
```

All hot-path components run on the same LAN. Recommended deployment: N=3 recorder hosts with one executor replica per host, separate hosts for ingress proxies and the L1 batcher.

### System invariants

- **I1: Canonical order = Aeron Archive position on channel B.** Nothing else is canonical.
- **I2: Ack iff durable + executed.** The proxy never returns success to the client unless (a) the tx's B-position has reached the *quorum fsync watermark* — fsynced on Q recorders out of N total (§2.3) — AND (b) a receipt for that tx-position has arrived on channel C from at least one executor replica. With the default N=3, Q=2, the quorum guarantees survival of any 1 simultaneous host failure with no ack stall.
- **I3: Execution is a deterministic function of channel B.** All executor replicas must produce byte-identical receipts. Sources of non-determinism (system time, randomness, network) are forbidden in tx execution; the block-sealer is the sole provider of `block.timestamp`.
- **I4: State DB is a derived view.** Channel B (plus genesis state) is the source of truth. The state DB is a performance cache, rebuildable by replaying the archive.

---

## Components

### 2.1 Ingress proxy
**Responsibility:** terminate client connections, validate, sign-check, partition-route, return receipts.

- Transports: JSON-RPC over HTTP/WS for ecosystem compatibility; binary line protocol over TCP/UDS for low-latency clients. Both feed the same internal pipeline.
- Per-IP token-bucket rate limit before sig verify (cheap reject for abuse).
- Batched secp256k1 verification. Sigs accumulate in a small ring (64-deep, ≤50µs flush window); the `k256` batch verifier amortizes pubkey recovery. Single-sig fallback when the ring is empty.
- Routes to `ingress[keccak(sender) % M]` via Aeron publication. Carries a `correlation_id` for the response path.
- Maintains a **pending-receipts map** keyed by `(sender, nonce) → client connection handle`. Subscribes to channel C; on receipt arrival, waits for the fsync watermark on B to pass that tx's position, then returns the receipt and removes the entry.
- A side **receipt-cache** Aeron channel allows any proxy to answer "what was the receipt for `(sender, nonce)`?" for retry idempotency.
- Stateless w.r.t. canonical truth; safe to add or remove proxies at any time.

### 2.2 Sequencer
**Responsibility:** exclusive ownership of a sender slice; nonce-check; publish into canonical channel B.

- One process per ingress partition (M total; default M=8, configurable). Pinned to a CPU core. Owns its `HashMap<Address, NextNonce>` exclusively — no locks, no atomics, because no other sequencer can see these addresses.
- Sender partitioning lives only at this tier. It exists to make pending-nonce state ownership exclusive; it does *not* propagate to the executor (which reads the merged canonical channel B).
- Pull from `ingress[i]`, deserialize the (already sig-checked) tx envelope, check `tx.nonce` against `pending_nonce[sender]`:
  - **Match:** publish raw tx bytes (with `correlation_id`) to channel B via Aeron concurrent publication; advance `pending_nonce`.
  - **Future (`> expected`):** insert into per-sender `BTreeMap<Nonce, Tx>` pending buffer. Drain in order when prior nonces arrive.
  - **Past (`< expected`):** drop, log, push a `correlation_id → "duplicate"` notification back through the receipt-cache channel.
- **No mempool, no gossip, no replacement-by-fee in v1.** Replacement-by-fee requires same-nonce arbitration and fee escalation rules; deferred to a separate spec.
- Backpressure: if Aeron publication on B blocks (downstream slow), the sequencer applies pushback to the ingress channel; the proxy then either queues briefly or returns `503` per its rate-limit policy.

### 2.3 Canonical archive (channel B)
**Responsibility:** source of truth for L2 ordering and durability.

- Aeron stream with concurrent multi-publisher; one `Recorder` process per host. **Total recorders N and quorum Q are independently configurable.** Default deployment: N=3, Q=2 (tolerate 1 failure with no ack stall — standard CFT). Lightweight staging: N=2, Q=2 (any failure stalls acks; safety preserved as long as at least one survives).
- Recorders run **Aeron Archive replication** — independent recordings on each host kept in lockstep by Aeron's `replay-merge` mechanism. Each recorder owns a local enterprise NVMe with PLP.
- **In-RAM propagation:** publishers and recorders communicate via Aeron's standard log buffers; messages reach all N recorder RAMs within Aeron-IPC budget (single-digit µs LAN).
- **Continuous background fsync per recorder:** an `io_uring` thread submits write+`fdatasync` ops as data arrives; pipelined submit-and-complete, no batch interval. Each recorder publishes its own `fsynced_position` on a per-recorder watermark stream.
- **Quorum fsync watermark.** A small aggregator (one per proxy host, or a side service) consumes all N per-recorder watermark streams and emits the *Q-th smallest* position — i.e., the position fsynced on at least Q recorders. This is the watermark proxies subscribe to for the I2 ack guarantee.
- **B-position `(term_id, term_offset)`** is the canonical tx identifier used by every downstream component.

### 2.4 Executor (replica)
**Responsibility:** read B, run Block-STM over revm, publish receipts to C.

- Typically one executor replica co-located with each recorder host. Each executor process runs:
  - One **reader thread** consuming B in order.
  - **W Block-STM worker threads** (default `W = num_cores − 2` per host).
  - One **commit thread** that drains committed receipts in tx-index order and publishes to channel C.
- **Block-STM scheduler.** Per-tx tasks are `Execute(i)` and `Validate(i)`. Workers pull lowest-pending-idx tasks lock-free. On `Execute(i)`: run revm reading from MV-memory, recording read-set, writing tagged with idx `i`. On `Validate(i)`: replay read-set against current MV-memory; if any value changed, invalidate and re-execute. Conflicts trigger re-execution of the affected tx and re-validation of all dependent txs.
- **MV-memory** sits in front of a read-only mdbx snapshot. Reads not satisfied from MV-memory fall through to the snapshot; writes append `(idx, value)` versions.
- **revm integration:** wrap `revm::Database` with the MV-memory layer; intercept `storage(addr, key)` and `basic(addr)` to record reads and resolve versions; intercept storage writes to append versions. This is the **Block-STM-revm integration** subproject (S4), the longest pole.
- After 250ms (virtual block window), receive the sealer's `BlockBoundary` marker on B, flush MV-memory delta to the state writer's queue, reset MV-memory.
- Replicas are byte-identically deterministic by construction (same input log, same algorithm, no clock reads inside execution). Receipts published by multiple replicas to C are duplicates; consumers dedupe by `tx_idx`.

### 2.5 Receipt channel (C)
**Responsibility:** distribute executed receipts and block boundaries.

- Aeron stream, multi-publisher (each executor replica). RAM only — no fsync (regenerable from B + libmdbx snapshot).
- Two message types:
  - `Receipt { tx_idx, status, gas_used, logs, write_set_hash }`
  - `BlockBoundary { block_number, end_tx_idx, l2_timestamp, state_root_commitment }` — emitted by executors when they reach a boundary marker on B. (The sealer originates boundaries on B for canonical ordering; executors re-emit on C so that C-only consumers — state writer, L1 batcher — see them inline with receipts.)
- Consumers: ingress proxies (client response), state writer (libmdbx commits), L1 batcher (posting), monitoring.
- Consumers dedupe by `tx_idx`. If two replicas publish receipts for the same `tx_idx` with *different* `write_set_hash`, that is a determinism violation: **panic, alert, halt the chain.**

### 2.6 Block sealer
**Responsibility:** define block boundaries; provide deterministic `block.timestamp`.

- Singleton process with hot standby. **Leader election: deterministic by lowest host-id among caught-up recorders.** Avoids an external KV (etcd/Aeron-Cluster) for v1.
- Every 250ms wall-clock (rounded to the next multiple): read `current_B_position` (an atomic Aeron counter), emit `BlockBoundaryStart { block_number, end_tx_idx, l2_timestamp }` to **channel B** (canonical-ordered with txs). The sealer's marker carries no state root — it just declares the block boundary in canonical order.
- Executors see the marker in B, finish executing txs up to `end_tx_idx`, compute the post-block state-root commitment, and emit the full `BlockBoundary` (with state root) on channel C.
- Sealer failover: the new sealer reads "last boundary was block N at position p" from B's tail, emits N+1 at the next tick.

### 2.7 State writer
**Responsibility:** apply committed state to libmdbx without blocking execution.

- One per executor host. Consumes its **local executor's commit-thread output directly** (each replica produces identical receipts by determinism, so reading the local copy avoids network and dedup). Falls back to channel C if the local executor is restarting.
- Batches receipts per block boundary; opens a single libmdbx write transaction per block; applies all account, storage, code writes; commits.
- libmdbx is configured with a large MVCC version horizon so the executor's read-only snapshot stays valid across multiple block boundaries without page reuse.
- The snapshot underlying MV-memory advances atomically at each block boundary (snapshot swap protocol, §5).

### 2.8 L1 batcher
**Responsibility:** post sealed blocks to Ethereum L1.

- Singleton + hot standby (same lease mechanism as sealer).
- Consumes block boundaries from C and the corresponding raw txs from B (between previous boundary and current).
- Packs into Ethereum 4844 blobs (max 6 blobs/L1-block, 128KB each ≈ 750KB/L1-block compressed). Cadence: configurable, default every ~10 L1 blocks.
- Posts to the L2 settlement contract. Integrates with the existing `crates/deployer` and contract groundwork (ETHLockbox, factory).
- v1 scope does **not** include fraud proofs or validity proofs.

---

## Data flow

### End-to-end trace (one simple-transfer tx, LAN, target sub-ms)

```
t=0      Client TX send
t+50µs   Proxy NIC, decode frame
t+80µs   Sig enters batch-verify ring
t+130µs  Batch verify flushes
t+135µs  Aeron pub → ingress[keccak(sender) % M]
t+140µs  Sequencer[i] dequeues
t+143µs  Nonce check, advance
t+145µs  Aeron concurrent-pub → channel B
t+150µs  Recorders A+B+C ingest in RAM
         ├── Execution path:
         │   Executor reader sees position p
t+155µs  │   Worker picks Execute(p)
t+200µs  │   revm done
t+205µs  │   Validate(p), no conflict
t+207µs  │   Commit thread → Receipt on C
t+210µs  │   Proxy receives Receipt(p)
         │
         └── Fsync path (parallel, started t+150µs):
             io_uring submit write+fdatasync to NVMe
t+175µs      NVMe completion (~25µs enterprise NVMe + PLP)
             fsync-watermark stream advances past p

t+210µs  Proxy has receipt(p); watermark already past p → release
t+260µs  Client TCP receives receipt
```

Total: ~260µs. The execution path is the binding constraint; fsync overlaps it.

### Latency budget

| Source | Typical cost | On critical path? |
|---|---|---|
| Client ↔ NIC LAN RTT | 50µs each way | yes |
| Sig verify (batched) | ~5–10µs amortized | yes |
| Aeron IPC hops (3×) | ~5µs each | yes |
| Sequencer nonce check | ~3µs | yes |
| Block-STM execute + validate | ~50µs simple; +5–50µs per re-exec | yes |
| Receipt publish + delivery | ~5µs | yes |
| Background fsync (RAM→disk) | ~25µs | only if slower than execution |

### What can blow the budget
- Hot-contract conflicts → re-execution loop, +5–50µs per re-exec
- Heavy contracts (many SLOADs) → +50–500µs over simple transfer
- MV-memory snapshot miss → cold libmdbx page read, +10–50µs
- NUMA cross-socket on un-pinned thread → +1–5µs per access
- Aeron flow-control backpressure (slow subscriber) → unbounded
- Sealer block boundary lands mid-flight → +1 boundary-emit cycle (no per-tx delay)
- NVMe stall (GC, queue saturation) → fsync becomes binding; sub-ms blown until drained

### Throughput math (1M tx/s target)

- Per-tx hot-path CPU: sig verify (amortized) ≈10µs, sequencer ≈3µs, Aeron pubs ≈10µs, revm ≈15–45µs, validate ≈1µs, commit ≈2µs → ~40µs total.
- 1M tx/s × 40µs = ~40 CPU-seconds per wall-second = ~40 cores busy across all tiers and replicas.
- Network on each Aeron channel: 1M × ~200B = 200MB/s on B, 1M × ~150B = 150MB/s on C. Fits on 10GbE; comfortable on 25GbE.
- libmdbx bursts at 250ms cadence: 250k receipts × ~100B state delta ≈ 25MB per write-txn. NVMe handles in single-digit ms; background, invisible to hot path.

Numbers are aspirational. The bench harness in `crates/bench` is the ground-truth instrument.

---

## Failure handling

### 4.1 Proxy failure
- **Detection:** L4 load-balancer health check or absent heartbeat in a small registry.
- **Recovery:** LB routes new connections to survivors; in-flight requests on the dead proxy are lost.
- **User impact:** client times out, retries. Retry sees same nonce; the proxy looks up `(sender, nonce)` in the shared receipt-cache channel and returns the prior receipt. **Idempotent.**

### 4.2 Sequencer failure
- **Detection:** Aeron subscription heartbeat absent; or ownership lease expiry.
- **Recovery:** hot standby for that slice takes ownership. Standby has been tailing B for senders in its slice; pending-nonce state is in lockstep. On takeover it begins publishing to its ingress channel.
- **User impact:** at most one in-flight tx lost (sequencer crashed before publish) or duplicated (crashed after publish); both resolve via idempotent client retry. Per-sender future-nonce buffer is lost on crash — documented limitation; clients with stranded future-nonce txs must resend.

### 4.3 Recorder failure (channel B host loss)
- **Detection:** Aeron archive replication lag exceeds threshold; survivors observe the lost peer.
- **Recovery:** survivors continue. With N=3, Q=2, a single recorder loss still satisfies the quorum (2 of 2 survivors fsynced) — acks continue without stall. A replacement catches up via `replay-merge`, concurrent with live ingest.
- **User impact:** zero unless quorum cannot be assembled (e.g., N=3, Q=2, and 2 recorders fail simultaneously). At that point acks stall until a replacement is up. Operator policy decides whether stalled writes return `503` or simply block.

### 4.4 Executor replica failure
- **Detection:** absence of receipts from this replica on C; or health stream lapses.
- **Recovery:** other replicas keep producing receipts. Dead replica is restarted, catches up by replaying B from its last libmdbx snapshot.
- **User impact:** zero as long as one replica produces receipts.
- **Receipt divergence (different `write_set_hash` for same `tx_idx`):** determinism violation. **Halt the chain, alert operators.** Single canonical halt-and-investigate posture, not a quorum vote — divergence indicates a Block-STM bug or hardware fault, not a value to compromise.

### 4.5 Sealer / batcher singleton failure
- **Detection:** lease expiry (default 1s).
- **Recovery:** hot standby acquires the lease, resumes. For the sealer: boundary markers are themselves recorded in B, so the new sealer reads "last boundary was block N" and emits N+1.
- **User impact:** at most one block boundary delayed by up to ~1s. RPC `eth_blockNumber` stalls briefly. Tx ack flow is unaffected.

### 4.6 State writer failure
- **Detection:** state-writer health stream.
- **Recovery:** restart; resume from last committed block boundary; replay C from there. libmdbx's MVCC handles partial txns cleanly.
- **User impact:** zero on the hot path. RPC state-query latency may briefly spike. If the writer falls behind by more than the libmdbx version horizon, the executor will see snapshot exhaustion — chain halts and operators intervene.

### 4.7 Whole-host failure
- **Detection:** missing heartbeats across all roles on the host.
- **Recovery:** per-tier failover as above.
- **User impact:** brief throughput dip; ≤1s sealer pause; no data loss with F surviving recorders.

### 4.8 Correlated-failure non-goals
- **All N recorder hosts lose power simultaneously.** Lost data = whatever is in-RAM-but-not-fsynced at crash time. Continuous io_uring fsync + PLP NVMe shrinks this to microseconds. Residual operator risk; addressable by spreading recorders across separate power domains and/or raising N.
- **Byzantine sequencer.** Out of scope under CFT. L1 fraud proofs (deferred) would address it at the settlement layer.

---

## State persistence (libmdbx)

State DB is the only component that persists derived state. Everything else is canonical (channel B fsynced archive) or regenerable from canonical.

### Schema sketch
- `accounts`: `Address → (nonce, balance, code_hash, storage_root)`
- `storage`: `(Address, StorageKey) → U256` — flat layout, no per-account trie
- `code`: `code_hash → Bytecode`
- `headers`: `block_number → (state_root_commitment, end_tx_idx, l2_timestamp)`
- `receipts` (optional): `tx_idx → encoded Receipt` — can be served from C-archive instead
- `meta`: durable cursors — `last_committed_block`, `last_committed_end_tx_idx`, `last_fsynced_B_position`

### MVCC version horizon

Executors hold a long-lived read-only `mdbx_txn` to back their MV-memory snapshot. libmdbx must not reuse pages still referenced by that read-txn. Freelist sizing rule of thumb: at 1M tx/s × ~100B state delta × 250ms blocks = ~25MB written per block; size for ~4 blocks of horizon (1s of writes). Configure `geometry.size_upper` and `MDBX_DBI` accordingly.

### Snapshot swap protocol

1. State writer commits block N (mdbx write-txn ends).
2. State writer signals "block N committed."
3. Executor reader pauses pulling new B messages; workers continue processing in-flight.
4. Executor opens new `mdbx_txn` against the post-N snapshot, swaps under MV-memory, drops the old read-txn.
5. Reader resumes.

Pause is bounded — opening a new mdbx read-txn is microseconds.

### Cold-start recovery
1. Load `meta`: `last_committed_block`, `last_committed_end_tx_idx`, `last_fsynced_B_position`.
2. Open mdbx read-txn at that snapshot — executor's initial state.
3. Reader starts consuming B from `last_committed_end_tx_idx`.
4. Executor replays B → MV-memory → state writer until live.
5. Proxy + L1 batcher come online once executor reaches live tail.

A fresh node (no state DB) replays from genesis. State sync / fast sync is a follow-up spec.

### Compaction

libmdbx is copy-on-write; long-running databases fragment. Schedule `mdbx_env_copy_compact` to a hot mirror once per day, then swap. Standard operational hygiene; not a hot-path concern.

---

## Testing strategy

The hardest invariants to preserve are determinism across replicas (I3) and canonical-order-equals-B-position (I1). The strategy prioritizes them.

### Unit
Standard per-crate tests. The existing bench harness (`crates/bench`) extended per new subsystem.

### Determinism conformance
- Run N executor replicas against the same recorded B archive. Assert byte-identical receipts including `write_set_hash`.
- Property test: replay an archive twice on the same replica; assert byte-identical state DB at every block boundary.
- Differential test against a single-threaded revm reference on a corpus of historical txs.

### Re-execution stress
- Synthetic workload: N accounts all writing the same storage slot (worst case for STM). Assert correctness; measure re-execution rate.
- Replay public mainnet blocks with high DEX activity (Uniswap, Curve); verify against geth/reth state roots.

### Chaos
Each failure mode in §4 gets a deterministic test:
- Kill recorder mid-flight; assert acks continue (with quorum still satisfied) and stall once quorum is unmet.
- Kill sequencer; assert standby takeover; no nonce gaps in B.
- Inject NVMe stall on fsync; assert proxy parks (doesn't ack) and recovers.
- Force executor divergence (corrupt one replica's MV-memory); assert receipt-divergence halt fires within bounded latency.
- Network partition between recorders; assert replication catches up post-heal.

Framework: `turmoil` or a custom Tokio-driven harness for deterministic time + I/O.

### Performance
- Per-stage metering: ingress sig-verify, sequencer nonce, Aeron pub, Block-STM execute, validate, commit, state-writer burst.
- End-to-end p50/p99/p999 latency under sustained load at 100k/500k/1M tx/s.
- Throughput ceiling: queue depth saturation point on each tier.
- Hot-contract workload mix (10% DEX swap, 70% transfers, 20% misc); measure Block-STM re-exec rate and effective parallelism.

### L1 round-trip
- Post a batch of L2 blocks to the L1 settlement contract on Anvil; reconstruct the L2 chain from L1 calldata + blob data alone; assert reconstructed receipts match what executors emitted. Strongest end-to-end validity check.

### Soak
24h+ runs under bench-harness load mix. Watch for memory leaks (MV-memory, Aeron buffers, mdbx freelist), file-handle leaks, fsync watermark drift, libmdbx fragmentation.

---

## Decomposition into implementation specs

The system-level design hands off to 7 subsystem specs. Each is its own brainstorm → spec → plan → implement cycle.

| # | Spec | Owner crate(s) | Depends on |
|---|---|---|---|
| **S1** | Ingress proxy + binary line protocol + batched sig verify | new `crates/ingress` | nothing — can start immediately |
| **S2** | Sequencer: sender-partition + nonce state + pending buffer + Aeron pubs | new `crates/sequencer` | S3 |
| **S3** | Canonical log: Aeron archive replication + continuous io_uring fsync + watermark stream | new `crates/log` (channels B and C live here) | nothing — foundational |
| **S4** | Block-STM revm integration: MV-memory, read-set tracking, scheduler | new `crates/executor` (replaces current `crates/node::executor`) | research / revm fork or upstream PRs |
| **S5** | Block sealer + deterministic lease election | new `crates/sealer` | S3 |
| **S6** | State writer: libmdbx schema, snapshot swap, MVCC horizon | new `crates/state` (extending current `crates/node`) | S4 |
| **S7** | L1 batcher: 4844 packing, settlement contract integration, outbox | new `crates/batcher` + contract additions | existing `crates/deployer`, contracts |

### Critical-path order
1. **S3 + S4 start in parallel.** S3 unblocks everything downstream; S4 is the research-grade longest pole.
2. **S2 lands after S3** (sequencer needs the log infrastructure).
3. **S1 lands in parallel with S2** (proxy only depends on channel definitions, not internals).
4. **S5, S6 land after S4.**
5. **S7 lands last** (needs sealed blocks).

### Cross-cutting (no separate spec)
- Conformance / chaos / perf test harness (extends existing bench).
- Aeron + io_uring + libmdbx Rust crate selection and shared utility code.
- Observability (existing Prometheus + tracing stack extended).

### Recommended first follow-on spec
**S3 (canonical-log)** — it gates the most downstream work.

---

## V0 scope (initial implementation)

The first implementation cycle (v0) defers the longest-pole research items and ships the full pipeline at a lower performance ceiling. Subsequent versions reintroduce them per the decomposition above.

**V0 deferrals:**
- **S4 Block-STM is deferred.** V0 executor uses **sequential revm** (adapted from the existing `crates/node::executor`). Replicas remain byte-identically deterministic; parallelism within a replica is set aside.
- **Throughput target relaxed.** With sequential revm per replica, the realistic ceiling is ~50–100k tx/s per replica on simple transfers, dropping with contract complexity. The 1M+ tx/s and HFT-class workload-mix benchmarks are explicit non-goals for v0; they are the bar S4 v1 must clear.
- **Latency target preserved.** Sub-millisecond post-execution ack remains in scope — sequential revm is fast enough (~15–45µs per simple tx) that the rest of the budget (sig verify, Aeron IPC, fsync) still adds up to <1ms.

**V0 components ship in dependency order:** S3 → S1 → S2 → S4 (v0 sequential) → S5 → S6 → S7.

**Once v0 is end-to-end:** S4 v1 (Block-STM revm integration) gets its own brainstorm + design spec + plan, and slots in as a drop-in replacement behind the executor's existing channel-B-in / channel-C-out interface. No other component should require changes for the upgrade.

---

## Out-of-scope / follow-ups

- **State sync / snapshot ingest** for fresh nodes joining a live chain.
- **Replacement-by-fee** at the sequencer (same-nonce arbitration, fee escalation).
- **Public mempool / gossip** for ecosystem compatibility.
- **Fraud proofs / validity proofs** at the L1 settlement contract.
- **Cross-chain composability** (async messaging, IBC-style).
- **DA targets other than Ethereum 4844 blobs** (EigenDA, Celestia, Avail).
- **Byzantine fault tolerance** among sequencer operators.
- **External lease store** (etcd, Aeron-Cluster) as upgrade path from deterministic lowest-host-id election.

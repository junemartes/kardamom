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
| D11 | Split persistence into **channel A** (M per-sequencer exclusive-publisher archives carrying full tx data) and **channel B** (one canonical concurrent multi-publisher orderer carrying tiny `TxRef`s, ~16 B) | Aeron concurrent publication CAS-contends on the term cursor — keeping bulk data off it lets data writes run at exclusive-publisher (memcpy) speed. Aggregate write bandwidth scales linearly with M sequencers. Ack durability is `max(channel-A fsync, channel-B quorum fsync)`; both run in parallel with execution. Same single-tx latency, ~10× higher sustainable throughput, cleaner conceptual split (data vs. ordering) — same pattern Kafka uses |

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
   │ exclusive ownership of sender slice; lock-free pending-nonce HashMap
   │ DUAL WRITE per validated tx:
   │   1. Full TxEnvelope → its own channel A[i]  (Aeron EXCLUSIVE publisher; one Archive per sequencer host; persisted via io_uring fsync)
   │   2. Reference TxRef { sequencer_id, position_a } → channel B  (Aeron CONCURRENT multi-publisher; tiny payload; persisted via io_uring fsync; quorum N/Q replicated)
   ▼
[channel A cluster: M persisted Archives]   [channel B: canonical orderer]
   exclusive-publisher per sequencer          concurrent multi-publisher
   parallel fsync across M NVMe              tiny 16B refs; cheap N/Q quorum fsync
   ~250 B/msg × M parallel streams           ~16 B/msg × 1 ordered stream
   │                                          │
   │       canonical order = B-position       │
   └─────────────────────────┬────────────────┘
                             ▼
[executor cluster: N replicas]
   │ each replica: subscribes to ALL M channel A's + channel B
   │ buffers A messages keyed by (sequencer_id, position_a) until referenced by B
   │ processes refs from B in canonical order; joins to buffered A entry
   │ 1 reader + W Block-STM workers + 1 commit thread
   │ Aeron pub → channel C (receipts + block boundaries)
   ▼
[receipt channel C]   ← block sealer publishes boundary markers into B (tiny; same channel-B style)
   │ multi-publisher; RAM only (regenerable from A's + B + state snapshot)
   │
   ├──► [ingress proxy]    ← matches (sender, nonce); waits for BOTH A's fsync watermark (past tx's A-pos) AND B's quorum fsync (past tx's B-pos); releases response
   ├──► [state writer]     ← per-host; burst-applies receipts to libmdbx at block boundaries
   └──► [L1 batcher]       ← reads channel A archives (full tx data) + B (ordering); packs sealed blocks into 4844 blobs
            │
            ▼
       [Ethereum L1: settlement contract]
```

All hot-path components run on the same LAN. Recommended deployment: N=3 recorder hosts with one executor replica per host, separate hosts for ingress proxies and the L1 batcher.

### System invariants

- **I1: Canonical order = Aeron Archive position on channel B.** Nothing else is canonical. Channel B carries tiny `TxRef { sequencer_id, position_a }` records (~16 B); the actual tx data lives in the per-sequencer channel A referenced.
- **I2: Ack iff fully durable + executed.** The proxy never returns success to the client unless **all three** hold:
   1. The tx's `position_a` on its channel A has been fsynced on the sequencer's local NVMe (channel A is exclusive-publisher per sequencer, so this is a single-host fsync watermark).
   2. The tx's B-position has reached the *quorum fsync watermark* — fsynced on Q recorders out of N total (§2.3) — for channel B.
   3. A receipt for that tx-position has arrived on channel C from at least one executor replica.

  With the default N=3, Q=2, this guarantees survival of any 1 simultaneous channel-B-recorder failure AND any 1 simultaneous channel-A-host failure (different blast radii — see §4.3). In practice, channel A's fsync (~22µs single-NVMe) dominates the durability barrier; channel B's quorum fsync runs in parallel and on smaller payloads.
- **I3: Execution is a deterministic function of (channel A's data joined by channel B's ordering).** All executor replicas process B's refs in B-position order, look up the data on the appropriate channel A, and must produce byte-identical receipts. Sources of non-determinism (system time, randomness, network) are forbidden in tx execution; the block-sealer is the sole provider of `block.timestamp`.
- **I4: State DB is a derived view.** Channels A + B (plus genesis state) are the source of truth. The state DB is a performance cache, rebuildable by replaying the archives.

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
**Responsibility:** exclusive ownership of a sender slice; nonce-check; **dual-write to its own persisted channel A and to the canonical orderer channel B**.

- One process per ingress partition (M total; default M=8, configurable). Pinned to a CPU core. Owns its `HashMap<Address, NextNonce>` exclusively — no locks, no atomics.
- Sender partitioning exists to make pending-nonce state ownership exclusive; it does *not* propagate to the executor.
- Pull from `ingress[i]`, deserialize the (already sig-checked) tx envelope, check `tx.nonce` against `pending_nonce[sender]`:
  - **Match:** **dual write:**
    1. Publish full `TxEnvelope` (with `correlation_id`, `sender`, `tx_hash`) to **channel A[i]** via Aeron **exclusive** publication (the sequencer is the only publisher to its own channel A — no CAS contention). Note the assigned `position_a` from the publication.
    2. Publish `TxRef { sequencer_id: i, position_a }` (~16 bytes) to **channel B** via Aeron **concurrent** multi-publisher. This is the canonical ordering write.
    3. Advance `pending_nonce`.
  - **Future (`> expected`):** insert into per-sender `BTreeMap<Nonce, Tx>` pending buffer. Drain in order when prior nonces arrive.
  - **Past (`< expected`):** drop, log, push a `correlation_id → "duplicate"` notification back through the receipt-cache channel.
- **Why split A and B:** channel A is exclusive-publisher (one writer, no CAS contention; near-memcpy speed) and parallelizes fsync across M sequencer hosts; channel B is concurrent multi-publisher but with tiny payloads, so the cross-publisher serialization point is cheap. Aggregate write bandwidth scales linearly with M; canonical-orderer contention stays bounded.
- **No mempool, no gossip, no replacement-by-fee in v1.** Replacement-by-fee requires same-nonce arbitration and fee escalation rules; deferred to a separate spec.
- Backpressure: if channel A or B publication blocks, the sequencer applies pushback to the ingress channel; the proxy then either queues briefly or returns `503`.

### 2.3 Canonical archives (channels A and B)

The persistence tier owns **two distinct kinds of Aeron archives**, with different write patterns and different fsync stories.

#### 2.3.1 Channel A archives (M of them, one per sequencer)
**Responsibility:** durable, per-sequencer storage of full transaction bytes.

- One Aeron stream per sequencer, **exclusive-publisher** (the sequencer process is the only writer). No CAS contention — write speed approaches `memcpy + memory barrier`.
- One `Recorder` process colocated with each sequencer, recording its channel A to local NVMe.
- Continuous `io_uring` fsync per recorder (same model as channel B's recorders): pipelined `write + fdatasync`, no batch interval. Each recorder publishes its own `fsynced_position_a[i]` on a per-A watermark stream.
- **No quorum on channel A by default** — a single sequencer host's NVMe is the durability boundary for its slice. Single-host loss before fsync = bounded data loss for *that sequencer's slice only* (txs in flight on other sequencers are unaffected). Operators can opt into per-A replication (mirror channel A's writes to a sibling host) if cross-host safety is required; off by default to keep fsync latency at single-NVMe roundtrip.
- **A-position** is `(sequencer_id: u8, term_id: i32, term_offset: i32)`.

#### 2.3.2 Channel B archive (the canonical orderer)
**Responsibility:** source of truth for L2 ordering across all sequencers.

- One Aeron stream, **concurrent multi-publisher** (M sequencers all push tiny refs). Payload is `TxRef { sequencer_id, position_a }` plus an occasional `BlockBoundaryStart` from the sealer — every record fits in ~16 B.
- One `Recorder` process per host; **total recorders N and quorum Q are independently configurable.** Default deployment: N=3, Q=2 (tolerate 1 failure with no ack stall — standard CFT). Lightweight staging: N=2, Q=2.
- Recorders run Aeron Archive replication kept in lockstep via `replay-merge`. Each recorder owns a local enterprise NVMe with PLP.
- Continuous `io_uring` fsync per recorder; each publishes its own `fsynced_position_b` on a per-recorder watermark stream.
- **Quorum fsync watermark.** A small aggregator emits the Q-th smallest fsync position across the N recorders. This is what proxies subscribe to.
- **B-position `(term_id, term_offset)`** is the canonical tx identifier.

#### Why two channel types

Channel B's writers contend on a CAS cursor (Aeron concurrent publication semantics). Putting full tx data on B means every multi-megabyte/sec of throughput drags the publishers through that contended cursor *and* through the same N-replicated recording. Channel A removes the data path from that contention point entirely; each sequencer writes to its own exclusive Archive at near-memcpy speed, in parallel with every other sequencer. Channel B then only carries 16-byte references — fast to CAS, fast to memcpy, cheap to N/Q-replicate.

This is the same separation Kafka uses for partition data vs cluster metadata. For Aeron specifically, switching the bulk-data writes from concurrent to exclusive publication is a measured ~10× per-publisher throughput improvement.

### 2.4 Executor (replica)
**Responsibility:** join B's ordering with A's data, run Block-STM over revm, publish receipts to C.

- Typically one executor replica co-located with each channel-B recorder host. Each executor process runs:
  - **M+1 reader threads** (one per channel A subscription + one for channel B). The A-readers buffer incoming `TxEnvelope`s into per-A queues keyed by `position_a`. The B-reader walks the canonical orderer.
  - **Channel-A buffer:** in-RAM hashmap `(sequencer_id, position_a) → TxEnvelope`. Bounded by the in-flight window (oldest unreferenced position drives eviction). Typical size: a few thousand entries, < 100 MB.
  - **W Block-STM worker threads** (default `W = num_cores − 2` per host).
  - One **commit thread** that drains committed receipts in B-position order and publishes to channel C.
- **B-to-A join.** B-reader receives `TxRef { sequencer_id, position_a }`. Looks up the buffered envelope from the corresponding A-reader queue. If not yet arrived (rare — A-publish usually races ahead of B), waits briefly with bounded timeout. Hands off `(b_position, TxEnvelope)` to the Block-STM scheduler. Removes the entry from the A-buffer.
- **Block-STM scheduler.** Per-tx tasks are `Execute(i)` and `Validate(i)` where `i` is the canonical B-index. Workers pull lowest-pending-idx tasks lock-free. On `Execute(i)`: run revm reading from MV-memory, recording read-set, writing tagged with idx `i`. On `Validate(i)`: replay read-set against current MV-memory; if any value changed, invalidate and re-execute.
- **MV-memory** sits in front of a read-only mdbx snapshot. Reads not satisfied from MV-memory fall through to the snapshot; writes append `(idx, value)` versions.
- **revm integration:** wrap `revm::Database` with the MV-memory layer (subproject S4, the longest pole).
- After the sealer's `BlockBoundaryStart` arrives on B, finish executing txs up to `end_b_index`, flush MV-memory delta to the state writer's queue, reset MV-memory, and emit `BlockBoundary` on channel C.
- Replicas are byte-identically deterministic by construction (same B order, same A bytes joined via refs, same algorithm, no clock reads inside execution). Receipts published by multiple replicas to C are duplicates; consumers dedupe by B-position.

### 2.5 Receipt channel (C)
**Responsibility:** distribute executed receipts and block boundaries.

- Aeron stream, multi-publisher (each executor replica). RAM only — no fsync (regenerable from B + libmdbx snapshot).
- Two message types:
  - `Receipt { tx_idx, tx_hash, status, gas_used, logs, write_set_hash }` — `tx_hash` is carried unchanged from the inbound `TxEnvelope` (computed once by the proxy at sig-verify time).
  - `BlockBoundary { block_number, end_tx_idx, l2_timestamp }` — emitted by executors when they reach a boundary marker on B. (The sealer originates boundaries on B for canonical ordering; executors re-emit on C so that C-only consumers — state writer, monitoring — see them inline with receipts.) **No state-root commitment**: state-root attestation is a validator concern, deferred (see Out-of-scope).
- Consumers: ingress proxies (client response), state writer (libmdbx commits), monitoring. The L1 batcher does **not** consume C; it reads B's archive offline (§2.8).
- Consumers dedupe by `tx_idx`. If two replicas publish receipts for the same `tx_idx` with *different* `write_set_hash`, that is a determinism violation: **panic, alert, halt the chain.**

### 2.6 Block sealer
**Responsibility:** define block boundaries; provide deterministic `block.timestamp`.

- Singleton process with hot standby. **Leader election: deterministic by lowest host-id among caught-up recorders.** Avoids an external KV (etcd/Aeron-Cluster) for v1.
- Every 250ms wall-clock (rounded to the next multiple): read `current_B_position` (an atomic Aeron counter), emit `BlockBoundaryStart { block_number, end_tx_idx, l2_timestamp }` to **channel B** (canonical-ordered with txs). The sealer's marker just declares the block boundary in canonical order.
- Executors see the marker in B, finish executing txs up to `end_tx_idx`, flush the block delta to the state writer, and emit `BlockBoundary { block_number, end_tx_idx, l2_timestamp }` on channel C. No state-root commitment is computed — that is a validator concern, deferred.
- Sealer failover: the new sealer reads "last boundary was block N at position p" from B's tail, emits N+1 at the next tick.

### 2.7 State writer
**Responsibility:** apply committed state to libmdbx without blocking execution.

- One per executor host. Consumes its **local executor's commit-thread output directly** (each replica produces identical receipts by determinism, so reading the local copy avoids network and dedup). Falls back to channel C if the local executor is restarting.
- Batches receipts per block boundary; opens a single libmdbx write transaction per block; applies all account, storage, code writes; commits.
- libmdbx is configured with a large MVCC version horizon so the executor's read-only snapshot stays valid across multiple block boundaries without page reuse.
- The snapshot underlying MV-memory advances atomically at each block boundary (snapshot swap protocol, §5).

### 2.8 L1 batcher
**Responsibility:** post raw L2 tx data to Ethereum L1 as a data-availability sink.

- **Decoupled from the live pipeline.** Reads from the on-disk Aeron Archive segment files of channels A and B (or via the Aeron Archive's standard offline replay protocol). Does **not** subscribe to channel C; does **not** query any live sequencer/executor process; can be down for arbitrary periods without affecting tx flow.
- Reads **channel B** canonically for the ordering (`TxRef` records + `BlockBoundaryStart` markers from the sealer). For each ref, reads the corresponding **channel A** archive at `position_a` to fetch the actual `TxEnvelope` bytes. Groups txs into per-block batches at boundary markers.
- Singleton + hot standby (same lease mechanism as sealer).
- Packs batches into Ethereum 4844 blobs (max 6 blobs/L1-block, 128KB each ≈ 750KB/L1-block compressed). Cadence: configurable, default every ~10 L1 blocks.
- Posts to the L2 settlement contract — **a pure data-availability sink**. The contract records `(block_range, blob_versioned_hashes)` and emits an event. **No state-root commitment is posted**: state-root attestation is a validator concern, deferred to a future validator subsystem.
- Integrates with the existing `crates/deployer` and contract groundwork (ETHLockbox, ERC-7955 factory).
- v0 scope does **not** include fraud proofs, validity proofs, or any state-root anchoring.

---

## Data flow

### End-to-end trace (one simple-transfer tx, LAN, target sub-ms)

```
t=0      Client TX send
t+50µs   Proxy NIC, decode frame
t+80µs   Sig enters batch-verify ring
t+130µs  Batch verify flushes; (sender, tx_hash) computed
t+135µs  Aeron pub → ingress[keccak(sender) % M]
t+140µs  Sequencer[i] dequeues
t+143µs  Nonce check, advance
t+145µs  Aeron EXCLUSIVE-pub → channel A[i] (full TxEnvelope; no CAS contention)
t+147µs  Aeron CONCURRENT-pub → channel B (TxRef, 16B)
t+150µs  Channel-A[i] recorder ingests in RAM; channel-B recorders (N=3) ingest TxRef in RAM
         ├── Execution path:
         │   Executor's B-reader sees TxRef at b_position p
         │   Joins to channel-A[i] buffer entry for position_a (already buffered, t+150µs)
t+155µs  │   Worker picks Execute(p)
t+200µs  │   revm done
t+205µs  │   Validate(p), no conflict
t+207µs  │   Commit thread → Receipt on C
t+210µs  │   Proxy receives Receipt(p)
         │
         ├── Channel A fsync path (parallel, started t+145µs):
         │   io_uring submit write+fdatasync to local NVMe
t+170µs  │   NVMe completion (~25µs enterprise NVMe + PLP)
         │   fsync_position_a[i] advances past position_a
         │
         └── Channel B quorum-fsync path (parallel, started t+147µs):
             N recorders' io_uring submit write+fdatasync (16B records — small)
t+172µs      Q-th NVMe completion (~25µs slowest of Q=2 of N=3 recorders)
             quorum_fsync_position_b advances past b_position p

t+210µs  Proxy has receipt(p);
         A fsync past position_a (since t+170µs) ✓
         B quorum-fsync past b_position p (since t+172µs) ✓
         release immediately
t+260µs  Client TCP receives receipt
```

Total: ~260µs end-to-end. Same overall budget as the prior model — fsync still runs in parallel with execution and only sets a floor. **What changed:** the data fsync (channel A) is now a single-host NVMe roundtrip instead of N-replicated; the quorum fsync (channel B) acts on 16B records and is effectively never on the critical path.

### Latency budget

| Source | Typical cost | On critical path? |
|---|---|---|
| Client ↔ NIC LAN RTT | 50µs each way | yes |
| Sig verify (batched) | ~5–10µs amortized | yes |
| Aeron IPC hops (3×) | ~5µs each | yes |
| Sequencer nonce check | ~3µs | yes |
| Channel A exclusive-publish (full TxEnvelope) | ~2µs | yes |
| Channel B concurrent-publish (16B TxRef) | ~2µs | yes |
| Executor B-to-A join (in-RAM buffer lookup) | ~1µs | yes |
| Block-STM execute + validate | ~50µs simple; +5–50µs per re-exec | yes |
| Receipt publish + delivery | ~5µs | yes |
| Channel A single-NVMe fsync (RAM→disk) | ~25µs | only if slower than execution |
| Channel B quorum fsync (Q-of-N) of 16B records | ~25µs | only if slower than execution |

### What can blow the budget
- Hot-contract conflicts → re-execution loop, +5–50µs per re-exec
- Heavy contracts (many SLOADs) → +50–500µs over simple transfer
- MV-memory snapshot miss → cold libmdbx page read, +10–50µs
- NUMA cross-socket on un-pinned thread → +1–5µs per access
- Aeron flow-control backpressure (slow subscriber) → unbounded
- Sealer block boundary lands mid-flight → +1 boundary-emit cycle (no per-tx delay)
- NVMe stall (GC, queue saturation) → fsync becomes binding; sub-ms blown until drained

### Throughput math (1M tx/s target)

- Per-tx hot-path CPU: sig verify (amortized) ≈10µs, sequencer ≈3µs, Aeron pubs (A exclusive + B concurrent) ≈4µs, revm ≈15–45µs, validate ≈1µs, commit ≈2µs → ~35µs total.
- 1M tx/s × 35µs = ~35 CPU-seconds per wall-second = ~35 cores busy across all tiers and replicas.
- **Write parallelism win:** channel A bandwidth scales with M sequencers. At M=8, each channel A carries 1M/8 × ~200B = 25 MB/s; aggregate cluster write throughput = 200 MB/s across 8 hosts in parallel (no CAS contention). Compare to the prior single-channel-B design where 200 MB/s went through ONE concurrent multi-publisher cursor + N-replicated recorder.
- Channel B carries 1M × 16B = 16 MB/s. Tiny. Q/N replication is essentially free at this rate.
- Channel C: 1M × ~150B = 150 MB/s. Same as prior (receipts unchanged).
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

### 4.3 Channel B recorder failure
- **Detection:** Aeron archive replication lag exceeds threshold; survivors observe the lost peer.
- **Recovery:** survivors continue. With N=3, Q=2, a single recorder loss still satisfies the quorum (2 of 2 survivors fsynced) — acks continue without stall. A replacement catches up via `replay-merge`, concurrent with live ingest.
- **User impact:** zero unless quorum cannot be assembled. Channel B carries tiny refs, so quorum is cheap and the failure window is small.

### 4.3.1 Channel A recorder failure (sequencer-host NVMe loss)
- **Detection:** sequencer's local fsync watermark stops advancing; channel A subscribers see the publication pause.
- **Recovery:** by default each channel A is single-host (no per-A replication for simplicity / latency). A host loss = its slice of in-flight txs is stuck until the sequencer comes back up with its NVMe.
- **User impact:** clients whose senders hash to the dead sequencer's partition see ack timeouts and 503s until that sequencer's standby (different host) takes over. Other partitions continue unaffected. Senders that were partway through the failed channel A: their in-flight txs without an A-fsync are lost; client retries are idempotent (nonce-based).
- **Optional hardening:** operators can enable per-A mirror replication to a sibling host (adds an additional fsync on the critical path; default off). With mirroring, channel A loss tolerates a single host failure with no client impact, at the cost of ~25µs more on the critical path.

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
- `headers`: `block_number → (end_tx_idx, l2_timestamp)` — no state-root commitment (D-Sh11)
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

**Mock vs. real Aeron — strict rule:** unit tests use the `kardamom-log` `testing` feature's in-memory channel fakes (fast, isolated). **End-to-end tests MUST use a real Aeron Media Driver + Aeron Archive running in Docker** — the `kardamom-log` crate ships a `testcontainers`-based harness that every subsystem reuses. Mocks are not acceptable at the e2e layer. CI runs the Docker e2e suite on every PR.

### Unit
Standard per-crate tests using the in-memory `testing` feature. The existing bench harness (`crates/bench`) extended per new subsystem.

### Determinism conformance
- Run N executor replicas against the same recorded B archive. Assert byte-identical receipts including `write_set_hash`.
- Property test: replay an archive twice on the same replica; assert byte-identical state DB at every block boundary.
- Differential test against a single-threaded revm reference on a corpus of historical txs.

### Re-execution stress
- Synthetic workload: N accounts all writing the same storage slot (worst case for STM). Assert correctness; measure re-execution rate.
- Replay public mainnet blocks with high DEX activity (Uniswap, Curve); verify against geth/reth state roots.

### Chaos / E2E
Each failure mode in §4 gets a deterministic test. **All chaos tests run against the Docker Aeron harness** — they exercise real Media Driver / Archive failure modes, not mocked ones.
- Kill recorder mid-flight; assert acks continue (with quorum still satisfied) and stall once quorum is unmet.
- Kill sequencer; assert standby takeover; no nonce gaps in B.
- Inject NVMe stall on fsync; assert proxy parks (doesn't ack) and recovers.
- Force executor divergence (corrupt one replica's MV-memory); assert receipt-divergence halt fires within bounded latency.
- Network partition between recorders (via `docker network disconnect`); assert replication catches up post-heal.

Framework: `testcontainers` (Rust) driving the Docker Aeron harness; `turmoil` or a custom Tokio-driven harness for any test that genuinely needs deterministic time + I/O without real network involvement (kept for unit-level chaos tests only).

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

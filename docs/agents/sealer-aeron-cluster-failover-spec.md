# Fault-Tolerant Sealer via Aeron Cluster (Raft) — Spec

- **Date:** 2026-06-20
- **Status:** Approved plan; Phase 0 (cluster-client feasibility) in progress
- **Plan:** `~/.claude/plans/dapper-jingling-galaxy.md`

## Goal

In MDC/cluster mode the `kardamom-sealer` process is the **sole publisher of the canonical
`tx_ordering` stream** — it reads the merged sequencer ref inputs, dedups racing replicas,
republishes the survivors as the single total order every executor reads, stamps
`BlockBoundaryStart.end_tx_idx` against a canonical record count, emits a boundary every
250 ms, and drives ingress's `on-quorum` acks. If it dies, the L2 produces **no** ordering: a
hard single point of failure. Make this role fault-tolerant by replacing the single sealer
process with an **Aeron Cluster** — a Raft-backed, replicated, deterministic state machine
with automatic leader election and failover — plugged into the existing Rust pipeline behind
its current transport trait seams.

## Non-goals

- Replacing the sealer in IPC/dev mode (the pure-Rust path is retained for local dev and the
  deterministic Rust test suite).
- Changing sequencer / executor / ingress **business logic** (only trait-impl wiring + a new
  adapter crate + a Java service).
- Changing the `tx_data` path, da_watcher, batcher, or any existing rkyv wire types.
- Seeding `block_number` from an existing live chain height on first cluster bootstrap
  (migration deferred; v1 assumes genesis or an explicit seed).
- JVM cluster production hardening beyond what the e2e gate needs.

## Design (simplest viable)

Only the tiny `tx_ordering` ref/boundary stream moves into the cluster. The cluster is the
implementation behind two existing Rust trait seams — **not** a set of bridge processes:

- `ClusterRefPublisher impl kardamom_sequencer::outbound::TxOrderingRefPublisher`
  (`crates/sequencer/src/outbound.rs`): encodes a thin SBE envelope
  `{ kind, canonical_id = tx_hash|source_hash (32 B), payload = rkyv bytes }` and offers it to
  cluster ingress; a back-pressured offer maps to `SequencerError::Backpressure` (the
  sequencer's existing rewind/retry path is unchanged).
- `ClusterTxOrderingSubscription impl kardamom_executor::reader::TxOrderingSubscription`
  (`crates/executor/src/reader.rs`): `next()` polls cluster egress, decodes the envelope to a
  `TxOrderingMessage` (relayed `TxRef`/`DepositRef` from the opaque payload, or a generated
  `BlockBoundaryStart`), assigns `BPosition::from_index(canonical_idx)`, returns
  `(BPosition, msg)`. Leader failover / reconnect is handled inside the cluster client; the
  executor reader thread and its existing `DedupWindow` are unchanged.
- `ClusterWatermarkSource`: surfaces the cluster's quorum-committed position as the durable
  watermark ingress's `on-quorum` gate consumes ("durable" = Raft-committed by a quorum).

The **Java ClusteredService** (Gradle subproject `cluster/sealer-service/`, co-located with
the JVM Consensus Module on each of 3 nodes on r1–r3) is the deterministic state machine:
`onSessionMessage` parses only the 32-byte `canonical_id`, dedups via a FIFO first-seen window
(mirrors Rust `CanonicalDedup`/`DedupWindow`), and on first-seen relays `{ index, opaque
payload }` to egress; a 250 ms **cluster timer** (`onTimerEvent`) emits `BlockBoundaryStart {
block_number, end_tx_idx = canonical_count, l2_timestamp = floor(leaderClock,250) }`; snapshots
persist `{ dedup window, canonical_count, block_number }`.

### Failover invariants
- **O1 single writer / fencing** — intrinsic (only the leader's egress is authoritative).
- **O2 gapless continuation** — replicated state + snapshots give the new leader the committed
  `{count, block_number, dedup window}`; no gap, no block-number regress.
- **O3 idempotent overlap** — re-delivered records share `canonical_id`; the executor's
  existing `DedupWindow` absorbs them; both sides count deduped records, so `end_tx_idx` stays
  aligned.
- **O4 image rotation** — eliminated: the cluster client handles failover/reconnect; there is
  no parallel Aeron canonical stream to rotate.

## Interfaces

- **Cluster wire**: SBE thin envelope (kind, canonical_id 32 B, opaque payload) + a generated
  `BlockBoundaryStart` SBE message — schema `cluster/cluster-messages.xml`, Rust codec in
  `crates/cluster-wire/`.
- **Cluster client**: `crates/cluster-client/` — connect, offer ingress, poll egress, expose
  committed position, follow leader on failover. `!Send` (one client per owning thread, as the
  existing rusteron clients).
- **Trait seams reused**: `TxOrderingRefPublisher`, `TxOrderingSubscription`, the ingress
  ack-gate watermark seam (confirmed in Phase 2).

## Aeron-spec references (researched)
- Consensus Module is JVM-only; C++ port still runs against a Java CM
  (https://aeron.io/docs/aeron-cluster/operating-aeron-cluster/).
- Deterministic replicated timers — leader clock only; TimerEvents replicated
  (https://aeron.io/docs/aeron-cluster/cluster-timers/).
- Consensus module drives the Archive to record the Raft log (durability unifies into the
  cluster).
- **Phase 0 binding finding:** rusteron 0.1.163 vendors `aeron-cluster` as **Java only** (no
  C/C++ cluster client, no `aeron_cluster_*` symbols), but the vendored
  `aeron-cluster/src/main/resources/cluster/aeron-cluster-codecs.xml` carries the full client
  session protocol → a Rust-native cluster client over rusteron pub/sub is feasible.

## Testing strategy

Deterministic layers (no real cluster/timers/network): SBE codec round-trip + cross-language
golden (Rust↔Java); Java state-machine + snapshot JUnit; Rust trait-adapter tests over
in-memory cluster fakes, including driving the **unchanged** sequencer/executor reader through
the new impls. Semi-deterministic: Aeron `TestCluster` in-JVM harness for failover/snapshot
(explicit leader stop/start, position-await — no sleeps). Non-deterministic gate
(`docker-e2e`, excluded from default `cargo test`): 3-node real-cluster smoke + the headline
**leader-kill continuity** test (block_number keeps advancing, receipts within 1–5 s RTO).
Full enumerated suite (33 tests, grouped by invariant) is in the plan file.

## Alternatives considered & rejected

- **Lease/epoch-fenced single-active sealer** (pure-Rust, no Raft): lower cost, in-grain with
  the codebase's "symmetric replicas + downstream dedup" philosophy, meets all constraints.
  Rejected by explicit user choice for the literal Aeron-Cluster/Raft mechanism; retained as
  the documented fallback if the Phase 0 cluster-client spike proves infeasible.
- **Out-of-process bridge adapters** (separate ingress/egress relay binaries onto a parallel
  Aeron canonical stream): rejected — reimplements the Aeron channel transport. Replaced by
  the in-process trait adapters above.

## Implementation status

**Landed & tested (deterministic, green in CI without a live cluster):**

- `crates/cluster-client` — Rust-native Aeron cluster *client*: the session-protocol SBE
  codec (`protocol`, schema id 111) and the sans-IO `SessionDriver` (connect handshake,
  keep-alive, `NewLeaderEvent` redirect, app framing). 18 unit tests.
- `crates/cluster-adapter` — the app `wire` envelope codec, the three trait adapters
  (`ClusterRefPublisher`, `ClusterTxOrderingSubscription`, `ClusterWatermark`), the `live`
  gateway (drives the `SessionDriver` over `kardamom_log`'s `AeronRuntime` ingress/egress),
  and the `cluster_ref_publisher` / `cluster_tx_ordering_subscription` wiring factories.
  17 unit + 2 end-to-end tests (publisher → in-Rust service mock → subscription).
- `cluster/sealer-service` (Java) — `CanonicalSealerState` POJO + `SealerClusteredService`.
  10 deterministic JUnit tests; the service compiles against `io.aeron:aeron-cluster:1.44.0`.

The `live` gateway compiles against rusteron and its pure helpers are unit-tested; its full
behaviour against a real cluster is covered by the gated e2e (below).

**Phase 0 gate — answered.** A Rust-native cluster client (no JVM client, no C++ port, no
JNI) is feasible and implemented: the full client session protocol round-trips, the driver
handles election/redirect, and the live transport reuses the codebase's existing Aeron
primitives.

**Remaining integration (needs a live 3-node cluster to develop/validate against):**

1. **Binary wiring** — add a `[cluster]` config section (default disabled) to
   `kardamom-sequencer`, `kardamom-executor`, and `kardamom-ingress`; when enabled, construct
   the cluster-backed trait impl via the factories instead of the Aeron `kardamom_log`
   handle (a `Box<dyn TxOrderingRefPublisher>` / `Box<dyn TxOrderingSubscription>` swap at the
   single construction site in each binary). The IPC/MDC path is untouched when disabled.
2. **Consensus Module launcher + cluster failover tests** — a `ConsensusModule` +
   `ClusteredServiceContainer` main in `cluster/sealer-service/service`, plus Aeron
   `TestCluster` JUnit tests (egress continuity across a leader kill; snapshot catch-up).
3. **Deploy** — a `deploy/cluster/nomad/cluster.nomad.hcl` running the JVM cluster node on
   r1–r3 (member endpoints), the cluster-node Docker image (JDK + service jar), removal of
   `sealer.nomad` in cluster mode, and the `docker-e2e` leader-kill continuity test.

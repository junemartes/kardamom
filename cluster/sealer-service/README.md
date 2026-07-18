# kardamom sealer cluster service (Java)

The canonical-ordering state machine that runs inside an **Aeron Cluster**
(Raft) — the fault-tolerant replacement for the single `kardamom-sealer`
process. It dedups racing sequencer records, assigns the canonical record index,
and stamps 250 ms block boundaries; the Raft Consensus Module replicates it
across the cluster and fails the leader over automatically.

Aeron's Consensus Module is JVM-only, so this logic lives in Java. The Rust
pipeline talks to the cluster through `crates/cluster-adapter` (in-process trait
adapters over a Rust-native cluster client) — see
`docs/agents/sealer-aeron-cluster-failover-spec.md`.

## Layout

- **`core/`** — `CanonicalSealerState`: the pure, deterministic state machine
  (dedup window + canonical count + boundary timer + snapshot). **No Aeron
  dependency**, so its JUnit tests run with only JUnit on the classpath. This is
  a faithful port of the Rust sealer logic (`crates/sealer/src/emitter.rs`, the
  republish loop in `crates/sealer/src/bin/kardamom-sealer.rs`) and the
  executor `DedupWindow` (`crates/executor/src/reader.rs`).
- **`service/`** — `SealerClusteredService implements
  io.aeron.cluster.service.ClusteredService`: the thin Aeron plumbing
  (ingress decode, egress framing, boundary timer, snapshot I/O) that delegates
  all logic to `core`. Depends on `io.aeron:aeron-cluster:1.44.0`.

## App envelope (kept in lockstep with the Rust `cluster-adapter::wire`)

```
ingress  [kind:u8=0][canonical_id:32][record_type:u8][fields…]   (id parsed for dedup;
                                                                   payload from offset 1 relayed)
egress   relayed:  [kind:u8=1][index:u64-LE][payload_len:u32-LE][relayed payload]
         boundary: [kind:u8=2][block_number:u64-LE][end_tx_idx:u64-LE][l2_timestamp:u64-LE]
```

## Dedup window sizing (`-Dkardamom.cluster.dedupCapacity`)

The first-seen window is the ONLY thing preventing a lagging racing replica's
re-offers from being ordered twice: a replica that stalls (GC pause, SIGSTOP,
cgroup throttle, receive backlog) and resumes after its twin pushed more than
`dedupCapacity` *unique* ids through the sealer re-offers records whose ids
were FIFO-evicted — and they are accepted as fresh. The invariant is
quantitative: **`dedupCapacity` > worst-case replica stall × peak unique-record
throughput**. The default (`1 << 17` = 131072, see
`SealerClusteredService.DEFAULT_DEDUP_CAPACITY`) tolerates a ~13 s stall at
10k tx/s (~20 MB heap, ~4 MB snapshot). Every member must use the SAME value —
the window is part of the deterministic state machine, and a snapshot never
loads into a smaller window than it was taken with.

## Build & test

Requires a JDK 17 (`JAVA_HOME`). The Gradle wrapper downloads Gradle 8.7 on
first run.

```sh
# Deterministic state-machine tests (no Aeron jars needed):
./gradlew :core:test

# Compile the Aeron ClusteredService adapter too:
./gradlew build
```

The deterministic `:core` suite (10 tests) is the Java half of the feature's
test matrix (groups B/C in the spec). Cluster failover (Aeron `TestCluster`
harness) and the docker e2e leader-kill test are the gated, real-cluster layers.

## Running in a cluster (deploy target)

Each of the 3 cluster nodes (r1–r3, co-located with the existing Aeron Archive)
runs a JVM hosting the `ConsensusModule` + a `ClusteredServiceContainer`
wrapping `SealerClusteredService`, with member endpoints
(`ingress/consensus/log/catchup/archive`) from the cluster config. The Rust
sequencers/executors connect via `cluster-adapter` (cluster mode). See the spec
for the full topology and the Nomad job sketch.

# Replicated Sequencer Shards (P=2 Racing Replicas) — Spec

- **Date:** 2026-07-15
- **Status:** Implemented
- **Motivation:** a sequencer crash stalled its sender-shard for the restart
  window (~30 s SLO), and a sequencer *node* loss stalled the shard until the
  node returned (no spare sequencer-role node to reschedule onto).

## Design

Run **two racing replicas per shard**, active/active, shared-nothing. Both
replicas of a shard subscribe to the same per-shard `tx_data` UDP-multicast
stream and both offer refs to the Aeron Cluster. No leader, no lease, no
failover protocol: a replica death costs nothing because its twin never
stopped.

Safety is by construction, from two properties the pipeline already has:

1. **Deterministic refs.** A `TxRef` is a pure function of the shared stream —
   `{tx_hash, shard_id, tx_data_position, tx_data_session_id}` — so replicas
   emit **byte-identical** wire records; whichever copy wins the race, the
   relayed canonical payload is the same.
2. **First-seen dedup at the ordering authority.** The clustered sealer dedups
   by 32-byte `canonical_id` (`TxRef.tx_hash` — its doc comment has always
   said "dedup of duplicate refs from racing sequencers"). This is the same
   mechanism that already absorbs the M duplicate `DepositRef`s every deposit
   produces today.

Per-sender **nonce order survives the race**: each replica emits a sender's
refs nonce-ordered, Aeron Cluster ingress preserves per-session order, and a
first-seen merge of two identically-ordered streams cannot invert their
relative order. Proven in
`crates/sequencer/tests/replicated_shard_racing.rs` across seeded adversarial
interleavings.

## Placement (deterministic, node-derived)

`sequencer.nomad.hcl` becomes two groups of `count = 2` on the 2
sequencer-role nodes, with the partition derived from **node meta**, not the
alloc index (which would make replica/shard pairing scheduler-luck):

| group  | partition                    | node-0 serves | node-1 serves | metrics | cluster-egress |
|--------|------------------------------|---------------|---------------|---------|----------------|
| seq-a  | `${meta.node_index}`         | shard 0       | shard 1       | :9001   | :40210         |
| seq-b  | `${meta.node_index}` + offset 1 | shard 1    | shard 0       | :9011   | :40211         |

So the two replicas of any shard are **always on different nodes**: a node
loss leaves every shard with one live replica. `meta.node_index` is stamped by
`nomad.hcl.j2` from the inventory (`node_index` var, generated from the
`<class>-<i>` name; static inventories fall back to `sequencer_id`).

The rotation itself is `SequencerConfig::rotate_partition` behind the new
`--partition-offset` flag: `partition ← (index + offset) % count`, with
`sequencer_id` following the rotated partition (invariant
`sequencer_id == partition_index`).

## Restart / rejoin semantics

A (re)starting replica **joins the live stream only** — no archive replay.
Its twin covered the outage, so replay would only re-offer records at risk of
falling outside the sealer's dedup window (the double-ordering hazard).

Nonce-floor hydration is only a **lower bound** (the deployed binary wires an
empty state DB, and even a real committed-state read trails the twin's
in-flight ordering); rejoin correctness comes from the **stream-adaptive
fast-forward**: when a sender's pending buffer has held a contiguous run
strictly above the floor, unchanged, for longer than `nonce_floor_lag_ms`
(default 5000 ms, well above ordering/commit latency), the gap provably is not
in flight — live-join has no replay and the twin already ordered the missing
nonces — so the floor adopts the lowest buffered nonce and the run publishes.
Floors only ever skip forward (per-publisher nonce order is preserved), and
re-offers of refs the twin already published are absorbed by the cluster's
first-seen dedup. Each adoption emits a `warn!` and bumps
`kardamom_sequencer_nonce_floor_fastforward_total`.

Pinned by `restarted_replica_with_empty_state_db_regains_coverage` (empty
state DB — the production wiring) and
`misaligned_hydration_floor_fast_forwards_to_the_join_point` (committed floor
strictly below the live-join nonce), alongside the original aligned-floor
test; the chaos `sequencer-replica-kill` case asserts the restarted replica's
`tx_published_to_b_total` advances post-restart.

## Observability

Both replica groups export on all interfaces — seq-a on `:9001`, seq-b on
`:9011` — and both are scraped (the dev Prometheus stack targets both ports
with `replica: a|b` labels; the Nomad jobs stamp
`KARDAMOM_HOST_ID=node<i>-seq-{a,b}` so every series identifies its replica).
Because both replicas of a shard process the same stream, stream-derived
per-shard totals (`tx_ingested`, `tx_published_to_b`, ...) exist **twice per
`partition`** — aggregate them `max by (partition)`, never `sum` (the Grafana
dashboard does; backpressure counters are real per-replica events and stay
summed). A rejoining replica shows a burst on the "Nonce-floor fast-forwards"
panel (`kardamom_sequencer_nonce_floor_fastforward_total`); sustained non-zero
is the chronic-lag alert signal.

## Failure modes after this change

- **Replica crash / hard kill** (`sequencer-replica-kill`, and now
  `graceful-`/`hard-sequencer` too): shard stays live, **no stall** — chaos
  asserts pipeline progress during the outage and 4/4 allocs within the
  restart SLO.
- **Sequencer node loss**: every shard retains one replica (cross-placement);
  redundancy degrades until the node returns.
- **Both replicas of a shard down** (double failure): that shard stalls —
  same blast radius as any single failure before this change.

## Non-goals / future

- P > 2 or shard scaling (K > nodes) — needs the epoch/stable-hashing design
  (dynamic resharding), for which this change is the availability groundwork.
- Sealer dedup-window resizing: replicas add duplicate *volume* but no new
  unique ids, and live-join keeps re-offers inside the window; revisit only if
  replay-on-restart is ever introduced.

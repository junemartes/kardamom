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
falling outside the sealer's dedup window (the double-ordering hazard). Nonce
floors hydrate from committed state via the existing stateless-sequencer
cache-miss path; the interim is absorbed by the pending buffer. The
`cold_rejoining_replica_emits_a_suffix_and_changes_nothing` test pins this.

## Observability

Scraping `:9001` across the sequencer nodes (what `kardamom-load` and the
Prometheus job do) now yields exactly **one replica per shard** (seq-a =
node-0:shard-0, node-1:shard-1) — per-shard totals keep their pre-replication
meaning. The seq-b twins on `:9011` are additional, not double-counted.

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

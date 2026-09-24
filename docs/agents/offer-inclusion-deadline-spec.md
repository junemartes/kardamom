# Offer Inclusion Deadline — Spec

- **Date:** 2026-09-23
- **Status:** Implemented 2026-09-24 (#432, #433), except the chaos assertion; see Deviations
- **Extends:** `sealer-aeron-cluster-failover-spec.md` (the cluster dedup window),
  `sequencer-lag-resync-spec.md` (the receipt-floor resync filter),
  `replicated-sequencer-shards-spec.md` (P racing replicas per shard)
- **Follows:** the executor `DedupWindow` removal (#427). The executor now trusts the sealer's
  relayed stream. The sealer is the pipeline's one dedup point.

## Deviations, as built

The design landed in two pull requests: #432 (the envelope field, the wire, the
proxy stamp) and #433 (the sealer rule, the window, the client notice, the
deploy wiring). Five things differ from the design above.

1. **The reason is `TxErrorReason::PastDeadline`, not `Expired`.** That name was
   already taken by the sequencer's nonce-gap `tx_ttl`, a different event with a
   different remedy.
2. **The snapshot is version 7, not 6.** The void stack (#421) already took 6.
3. **No frame-length compatibility branch.** The rollout in this document sniffs
   the frame length so an old sequencer can talk to a new sealer. The chain has
   no live deployment and #264 reset it, so the frame is a flag day and the
   branch would be dead code.
4. **Markers do not meet the window cap.** This document does not say what a full
   window does to an epoch or a remote batch. Refusing one would stall that lane
   rather than shed load, and markers are a trickle where transactions are a
   flood. Every member takes the same branch, so the replicated state stays
   identical.
5. **Log lines, not Prometheus counters.** The sealer has no metrics endpoint;
   the executor re-exports only its boundary stream. `PAST-DEADLINE` and
   `WINDOW-FULL` print at power-of-two counts, like `CONTIGUITY-REJECT` and
   `VOID-VOTE`, which is what the chaos suite greps.

One addition is not in the design: the sealer **clamps** the stored deadline to
`blockNumber + inclusionHorizonBlocks`. Without it, a proxy stamping a deadline
far in the future would pin an id in the window for as long as it liked, and the
window's bound would again be a promise rather than a property. The clamp only
shortens, so it admits nothing the stamp would not.

**Still open:** test-plan item 4. `sequencer-lapse` now reports how many
re-offers the sealer refused for being late, but at the shard's 64-block horizon
(128 s at the 2000 ms container tick) a 30 s freeze expires nothing, so the count
reads zero. Making it the assertion this document asks for needs the shard's
horizon set below its freeze length, on both the proxy and the sealer, and a
cluster run to confirm the thawed backlog is refused rather than re-ordered.

## Goal

Make the sealer's dedup exact, so that no replica stall can order one transaction twice.

Today the sealer keeps a bounded, FIFO-evicted first-seen window over canonical ids
(`CanonicalSealerState.dedup`, capacity `dedupCapacity = 2^17`). The window is the only
rule that stops a stalled racing replica's re-offer from being accepted as fresh. Its
guarantee is quantitative: `dedupCapacity > worst-case replica stall × peak unique-record
throughput`. The receipt-floor resync filter narrows the hazard, because a replica skips
an offer once a receipt proves it executed. It does not remove the hazard: a replica that
sees no receipt for a record (multicast lapse, late subscribe) publishes it, and the window
is again the only guard.

This spec moves the guarantee from sizing to the data. Every transaction offer carries a
deadline, in the sealer's own block numbers. The sealer rejects an offer past its deadline.
The sealer keeps an id in its window until that id's deadline passes. After that, no copy
of the id can be accepted, so forgetting the id is safe by construction.

## Non-goals

- Epoch and remote-origin records keep their guards (L1 origin must advance; the peer lane
  cursor). They get a sealer-assigned deadline for pruning only. See "Markers".
- No change to the executor, validator, or batcher. The executor already trusts the relayed
  stream.
- No change to how the cluster client replays on reconnect. The canonical-index cursor in
  `ClusterTxOrderingSubscription` already drops replay overlap exactly.
- No poisoned-log recovery policy. A canonical duplicate that reaches execution becomes a
  `NonceTooLow` skip receipt today, and stays that way.

## Design

### The deadline

Ingress stamps every envelope with `max_inclusion_block: u64`:

```
max_inclusion_block = latest_block_number + inclusion_horizon_blocks
```

- `latest_block_number` is the highest `BlockBoundary.block_number` the proxy has observed
  on `tx_receipts` (`Proxy::latest_block_number`, already maintained by
  `BlockBoundaryWatcher`).
- `inclusion_horizon_blocks` is proxy config. Default 64. At the 250 ms tick that is 16 s;
  at the 2000 ms deploy tick it is 128 s.

The deadline lives on the `tx_data` envelope, not on the `tx_ordering` reference. So every
racing replica reads the same value from the shared envelope and relays it unchanged. The
racing offers stay byte-identical, which the sealer's first-wins dedup relies on.

A lagging proxy stamps an earlier deadline. That is the safe direction: the transaction
expires sooner, the client hears about it, and resubmits. No proxy can stamp a deadline
that lets a stale copy in.

### The sealer rule

`onRecord` runs, in order:

1. Dedup lookup. A known id is a duplicate: dropped, not counted, not relayed. Unchanged.
2. Deadline check. If `blockNumber > deadline`, the offer is expired: rejected with a new
   egress outcome `EXPIRED`, not counted, not entered in the window.
3. Contiguity guard. Unchanged.
4. Insert, relay, count. Unchanged.

`blockNumber` is the block the next tick stamps: the open block. So a transaction with
deadline `D` can be ordered in any block up to and including `D`. The rule reads only
replicated state. It never reads `cluster.time()`, so every member decides the same way.

The dedup lookup stays first. A stale re-offer of a record that is still in the window is
absorbed as a duplicate, exactly as today, and never reaches the contiguity guard with its
stale nonce.

### The window, pruned by deadline

The window becomes a map from canonical id to deadline, plus an index from deadline to ids
(`TreeMap<Long, List<ByteBuffer>>`, or one bucket per open block, because deadlines arrive
near-monotone). On every `onTick`, after the block number advances, the sealer drops every
id whose deadline is below the new `blockNumber`. An id leaves the window only when no
offer carrying it can still pass the deadline check. There is no FIFO eviction.

The window is now bounded by data, not by a count: at most `inclusion_horizon_blocks ×
peak records per block` live ids. `dedupCapacity` stays as a hard cap, but it changes
meaning: it is backpressure, not eviction. A fresh offer that would grow the window past
the cap is rejected with `WINDOW_FULL`. The sequencer treats it like backpressure and
retries. The safe direction is again reject-and-retry, never forget.

Every member must still agree on `dedupCapacity`, because the cap decides accept-or-reject
in the replicated state machine. The must-agree contract line at startup stays.

### Markers

Epoch and remote-origin offers carry no deadline. The sealer assigns
`deadline = blockNumber + inclusion_horizon_blocks` at first sight, for pruning only. A
re-offer of a pruned marker id is rejected by the existing guards: an epoch whose L1
origin does not advance, or a remote batch whose `firstSeq` is not the lane cursor or whose
anchor does not advance. So the markers need no wire change, and the sealer needs its own
`inclusion_horizon_blocks` setting for them. That setting is replicated configuration, like
`dedupCapacity`, and every member must agree on it.

### Snapshot

Snapshot version 6 adds 8 bytes per window entry: the deadline. A version-5 snapshot loads
each id with `deadline = blockNumber + inclusion_horizon_blocks`. That keeps every
pre-upgrade id for one full horizon, which is the conservative direction.

### Sequencer

The sequencer copies `max_inclusion_block` from the envelope into the offer frame. It reads
`EXPIRED` from egress and surfaces it as `TxErrorReason::Expired` on `tx_errors`, the same path
as a contiguity reject. It reads `WINDOW_FULL` as backpressure and rewinds through
`reinsert_for_retry`.

The publish-confirmation ledger keeps its rewind-and-republish rule. A republished copy of a
committed ref is absorbed as a duplicate while its id is in the window, which is exactly
the interval in which it can be accepted. A republished copy past its deadline is rejected
as `EXPIRED`; the sequencer surfaces that to ingress instead of retrying forever.

The receipt-floor resync filter stays. It still saves the sealer needless re-offers, and it
still cannot publish a gap. Its safety no longer depends on `dedup_capacity`. The
watermark enter threshold can move from a fraction of the capacity to a number of blocks.

### Ingress

Ingress stamps the deadline at publish time. On `TxErrorReason::Expired` it releases the parked
client with a JSON-RPC error that names the deadline, so the client resubmits. A resubmitted
transaction has the same `tx_hash`. If the first copy was ordered inside its deadline, the
sequencer's receipt floor rejects the resubmission as `Past`. If no receipt reached the
sequencer, the copy is offered again, ordered again, and executes as a `NonceTooLow` skip
receipt. That path is loud (`kardamom_executor_invalid_tx_skipped_total`) and it is the
same path any duplicate takes today.

## Why this is not the rejected "envelope timestamp" alternative

`sequencer-lag-resync-spec.md` rejected staleness detection at the sealer for three reasons.
This design answers each one:

- *Clocks cannot be compared across hosts.* The deadline is a block number. The proxy reads
  it from the sealer's own boundaries, and the sealer compares it with its own replicated
  `blockNumber`. No host clock takes part.
- *Time is the wrong unit; the window evicts by count.* The window no longer evicts by count.
  It prunes by deadline, so the unit of the deadline and the unit of the window are the same.
- *The sealer must never reject on staleness, because it cannot tell a late duplicate from a
  legitimately delayed first copy.* With a deadline the distinction does not matter. An
  offer past its deadline is not accepted, whether it is the first copy or the tenth. That
  is a contract the client sees: a transaction is either ordered by its deadline or reported
  expired. Nothing accepted is dropped, because acceptance now means "ordered by the
  deadline".

## Wire changes

| Where | Change |
| --- | --- |
| `kardamom_types::TxEnvelope` (`tx_data`, rkyv) | new field `max_inclusion_block: u64` |
| Cluster ingress frame `KIND_INGRESS_RECORD` | new `deadline: u64 LE` after `nonce`, before the canonical id; `KIND_BATCH` entries the same |
| Cluster egress | new reject kinds `EGRESS_KIND_EXPIRED` and `EGRESS_KIND_WINDOW_FULL`, both offered only to the offering session |
| `kardamom_types::TxErrorReason` | new variant `Expired { max_inclusion_block, at_block }` |
| Sealer snapshot | version 6, 8 bytes per window entry |

The Java service parses one more fixed-offset field. It still parses nothing inside the
relayed payload.

## Config

| Knob | Default | Notes |
| --- | --- | --- |
| `--inclusion-horizon-blocks` (proxy) | 64 | how far past the last seen boundary a stamp reaches |
| `-Dkardamom.cluster.inclusionHorizonBlocks` (sealer) | 64 | marker deadlines and legacy-snapshot deadlines; must-agree across members |
| `-Dkardamom.cluster.dedupCapacity` (sealer) | `131072` | now a hard cap with `WINDOW_FULL` backpressure; must-agree stays |

## Observability

- `kardamom_sealer_offers_expired_total` and `kardamom_sealer_window_full_total` on the
  sealer, exported through the existing `kardamom_sealer_*` re-export.
- `kardamom_sealer_window_size` gauge, so an operator sees the window approach the cap.
- `kardamom_sequencer_ref_expired_total` on the sequencer.
- `kardamom_ingress_tx_expired_total` on ingress.

## Rollout

1. Deploy the sealer first. It reads the deadline when the frame carries one, and treats a
   frame without one as `deadline = u64::MAX` for the check and `blockNumber + horizon` for
   pruning. The frame length tells the two apart.
2. Deploy ingress and the sequencer. From then on every transaction offer carries a deadline.
3. Remove the no-deadline branch in the sealer in a later release.

## Test plan

1. **`CanonicalSealerStateTest`:** an offer at `blockNumber == deadline` is relayed; at
   `deadline + 1` it is rejected; a duplicate inside the window is dropped before the
   deadline check; an id is gone from the window on the first tick past its deadline, and
   a re-offer after that is rejected as expired, not accepted as fresh; the cap rejects
   with `WINDOW_FULL` and evicts nothing; a version-5 snapshot loads with horizon deadlines.
2. **Sequencer unit:** `EXPIRED` surfaces as `TxErrorReason::Expired`; `WINDOW_FULL` takes the
   rewind path; the ledger stops republishing an expired ref.
3. **Ingress unit:** the stamp equals `latest_block_number + horizon`; `Expired` releases the
   client with the JSON-RPC error.
4. **Chaos `sequencer-lapse`:** with the horizon set below the freeze length, the thawed
   replica's backlog is rejected as expired, the twin's copies were ordered, and every
   executor converges with zero `invalid_tx_skipped`. This is the assertion the bounded
   window could never make.

## Failure modes after this change

- Replica stalls longer than the horizon: its re-offers are rejected as expired. Nothing is
  ordered twice. The twin's copies were ordered inside the deadline, or the client hears
  `Expired` and resubmits.
- Proxy lags behind the boundaries: it stamps early deadlines, and more transactions expire.
  Clients resubmit. Throughput drops; correctness does not.
- Throughput spike fills the window: offers are rejected with `WINDOW_FULL` until a tick
  prunes. The sequencer retries. Nothing is forgotten early.
- Members disagree on `inclusionHorizonBlocks`: marker deadlines differ, so the replicated
  state diverges. The must-agree contract check makes this loud at startup, like
  `dedupCapacity` today.

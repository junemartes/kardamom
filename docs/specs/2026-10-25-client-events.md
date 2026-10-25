# Client events from one-way stage publications, and the egress on its own nodes

Status: draft, revision 1, 2026-10-25. The decisions in section 1 are the user's (2026-10-25).

## 1. Decisions

1. The ingress never looks up receipts. It has no receipt cache, no receipt query and no receipt
   feed.
2. Receipts reach clients by push: WebSocket subscriptions and webhooks. The notifier serves both.
   The notifier and the egress stay two processes.
3. There is no "sealed" status. The client events come from one-way stage publications:

   | Stage | Publisher | Meaning |
   |---|---|---|
   | `accepted` | ingress | The ingress checked the transaction and published it on `tx_data`. |
   | `sent` | sequencer | The sequencer offered the transaction to the sealer. |
   | `dropped` | sequencer | The transaction will not land. The event carries the reason. |
   | `executed` | executor, each of them | The executor applied the transaction. One event for each executor, with the block and the index, not the receipt. |
   | `receipt` | egress | The egress holds the merged receipt. The event carries the receipt. |
   | `posted` | batcher | The batch that holds the block of the transaction was sent to L1. |

4. The egress runs on its own nodes (`role=egress`), not on the executor nodes.

For now the executors and the egress both publish, so that the stages can be measured apart: the
time of each executor, and the time from the executors to the merged receipt. A switch turns the
executor events off after the evaluation (Q7).

The `dropped` stage is not in the user's list. It is here because without it a client sees `sent`
and then nothing (section 4.3).

## 2. Why

A staging spike on 2026-10-24 (main 1e2901d0, then f68ed82f with #810) showed the problem:

- `kardamom-load` sends one `eth_getTransactionReceipt` right after a submit. With
  `--ack validated` the submit answers before the execution. The read comes about 1 ms after the
  ack, and the execution takes about 1.9 ms (p50, `kardamom_ingress_execute_latency_seconds`).
- So the read misses the ingress cache, the egress and the executor fallback of #796. The tool
  then settles the transaction from its sweeper (every 5 s, entries older than 2 s), and reports
  p50 4.3 s to 5 s, although the chain receipted each transaction in about 2 ms.
- A poll can always come before the receipt. A push cannot.

The ingress also relays the sealer order as `tx_status` "sealed" from its own cluster client
(`crates/ingress/src/cluster.rs`). That keeps a cluster session and a status path in the
ingress, against the stateless ingress of `docs/specs/2026-10-24-elastic-ingress.md`.

## 3. Today

### 3.1 The receipt paths of the ingress

- `eth_getTransactionReceipt`: the memory cache, then the egress (`--receipt-query-endpoints`),
  then an executor state DB (#796). `crates/ingress/src/proxy/mod.rs`, `receipt_query.rs`.
- `kardamom_subscribeReceipts`: a WebSocket feed from the `tx_receipts` fan-in of the ingress
  (`crates/ingress/src/json_rpc.rs:140`, `receipt_subscription.rs`). Users: `kardamom-load`
  in subscribe and feed-confirm modes (`crates/bench/src/load/feed.rs`), and the interop feed
  (`crates/interop-feed`, `crates/da_watcher/src/interop`). The validator serves its own copy
  for interop (`crates/validator/src/interop/serve`). That copy is out of scope.

### 3.2 The cluster tap of the ingress

The ingress is a client of the sealer cluster. It reads the cluster egress on its own thread and:

- folds the durable count into the watermark of the on-quorum ack;
- publishes "sealed" on `tx_status` for each relayed `TxRef`;
- forwards the `ClusterStatus` (the posted head, the DA-lag flag, the record lag) to
  `crates/ingress/src/chain.rs`, which halts admission on a record-lag or a DA-lag halt.

### 3.3 The inputs of the notifier

| Stream | Publisher today |
|---|---|
| `tx_status` | the ingress (sealed, and the ingress statuses) |
| `egress_receipts` | the egress (with `KARDAMOM_NOTIFIER_RECEIPTS_FROM_EGRESS=on`), or `tx_receipts` of every executor |
| `tx_errors` | the sequencers |

### 3.4 The egress placement

`deploy/cluster/nomad/egress.nomad.hcl` constrains on `meta.role = executor` with
`distinct_hosts`, and `egress_count` is 2. On staging the instances run on executor-1 and
executor-2, so the store syncs share the disk of the state DB and the checkpoints. Idle with the
canary: about 95 MHz, 435 MiB and a store of 184 MB for each instance.

`docs/specs/egress-node-spec.md` (August 2026) describes another design: an egress role of the
validator. The `kardamom-egress` crate replaced it. Section 9 marks it superseded.

## 4. The design

### 4.1 The topic `tx_events`

One MDC topic with many publishers. A publication is a batch:

```
TxEventBatch {
    origin: Origin,          // ingress, sequencer, egress, batcher, and the instance index
    events: Vec<TxEvent>,
}
TxEvent =
    Accepted { hash }
  | Sent { hash, lane }
  | Dropped { hash, sender, nonce, reason }
  | Executed { hash, block, index }            // from one executor, its own copy
  | Receipt { hash, block, index, receipt }    // from the egress: the receipt bytes of egress_receipts
  | RolledBack { from_block }                  // an egress rollback frame
  | Posted { batch_index, first_block, last_block, l1_tx }
  | Included { batch_index, l1_block }
```

- The codec is rkyv, as the other streams. The format goes in `formats.toml` (until #664
  removes it) with `writes = 1`.
- A publisher fills a batch for each poll cycle of its loop, and sends it at the end of the
  cycle. It never waits for the notifier: an offer that fails is counted
  (`kardamom_tx_events_dropped_total{origin}`) and the batch is dropped.
- `Posted` and `Included` name a block range, not hashes. The notifier maps them to hashes
  (4.4).

### 4.2 The publishers

| Publisher | The change |
|---|---|
| ingress | `Accepted` for each transaction that it published on `tx_data`. |
| sequencer | `Sent` for each offer to the sealer, `Dropped` for each drop (the drops of #768, which the egress also takes). |
| executor | `Executed` for each transaction that it applied, behind its own switch (Q7). |
| egress | `Receipt` and `RolledBack` from the merged receipts, the same frames as `egress_receipts`. |
| batcher | `Posted` when it sends a batch, `Included` when the L1 receipt of the batch transaction comes. |

The egress keeps `egress_receipts` for the sequencers. The executors keep `tx_receipts`. The
`Executed` events carry no receipt bytes, so they stay small: 32 bytes of hash and two numbers.

### 4.3 Copies, order and loss

- **Copies.** Two sequencers race each transaction, three executors apply it, and two egress
  instances publish each receipt. The notifier keeps the first event of each `(hash, stage)`
  and drops the copies (the stage table of 4.4). A `Posted` or `Included` copy is dropped by
  `(batch_index, stage)`. Clients already deduplicate by `tx_hash` plus stage.
- **Order.** The topic has no order across publishers: `sent` can come before `accepted`, and
  `receipt` before `executed`. A client must still see the stages in order. So when an event
  comes, the notifier first emits every earlier stage of the order that it did not emit yet,
  marked `implied`, then the stage itself. A real event of an implied stage that comes later is
  dropped as a copy.
  - The order: `accepted` < `sent` < `executed` < `receipt` < `posted` < `included`.
  - `dropped` implies `accepted` only: a sequencer can drop before it offers (`txpool_full`).
  - A drop is provisional (as `kardamom_getTransactionStatus` says today): a `receipt` after
    `dropped` is emitted, with the stages before it that are not emitted yet.
  - The stage latencies of section 6 count only the stages that the notifier observed, not the
    implied ones.
- **Loss.** MDC offers are best effort, for example `NOT_CONNECTED` during a restart.
  - `accepted`, `sent` and `dropped` are hints. A lost one is not sent again.
  - `executed` is a hint, as the stages before it.
  - `receipt` must not be lost. The egress keeps its receipts. After a gap in the egress
    publications (a new publisher session, or a gap in the batch sequence of an origin), the
    notifier reads the missing receipts from the egress. Section 8, Q2, holds the mechanism.
  - `posted` and `included` come from L1. After a gap, the notifier reads the batcher's
    posted cursor or the `BatchPosted` logs (Q3).
- **Rollback.** `RolledBack { from_block }` revokes every `executed` and `receipt` at or above
  `from_block`.
  The notifier sends a `rolled_back` event to each subscriber of an affected hash. A client must
  treat `executed` and `receipt` as revocable until `posted`.

### 4.4 The notifier

- It reads only `tx_events`. The taps on `tx_status`, `tx_receipts`, `egress_receipts` and
  `tx_errors` go after the move (step 6 of section 7).
- **The stage table.** One fixed table for each instance, allocated at the start, sized as the
  ring (250,000 entries on the notifier nodes). It holds no `Vec`, no `String` and no box, and
  the event path allocates nothing for a hash that no client follows.
  - An entry is `{ hash: [u8; 32], emitted: u8, observed: u8, executors: u8, block: u64,
    accepted_ms: u32 }`, about 56 bytes, so the table uses about 14 MB.
  - `emitted` and `observed` are bitmaps with one bit for each stage. `emitted` decides the
    copies and the implied stages. `observed` keeps the stages that really came, for the
    latencies.
  - `executors` has one bit for each executor index, so the notifier sees which executors
    reported.
  - `block` comes from the first `executed` or `receipt`. It maps a `Posted` range to its
    hashes, so the table replaces a separate map from hash to block.
  - `accepted_ms` is the time of the first observed event of the hash, in milliseconds from
    the start of the instance. The latencies of section 6 count from it.
  - The table uses open addressing on the first 8 bytes of the hash, with a bounded probe. A
    full probe overwrites the oldest entry in it (the smallest `accepted_ms`). A hash older than
    the table gets `posted` only through a status query (4.5).
  - The batch decode reads the archived rkyv bytes in place, with no copy of each event.
  - A rollback clears the `executed` and `receipt` bits (and `executors`) of each entry at or
    above `from_block`, and sets `block` to 0. The new receipt after the rollback is then
    emitted again.
  - The table is owned by one thread, the ingest thread of the notifier. No lock.
- **The receipt feed.** `kardamom_subscribeReceipts` moves from the ingress to the notifier,
  next to its status feed, with the same frames. Its users (3.1) point to the notifier.
- **Sharding.** Today every instance reads every event, and only the webhooks are sharded. The
  measure on staging is about 0.2 core for each 1,000 tx/s. With the executor events, about 11
  events come in for each transaction before the copies are dropped: 1 accepted, 2 sent, 3
  executed, 2 receipts, and the batch events. So 10,000 tx/s is about 3 to 4 cores of decode,
  and about half that without the executor events. So each instance reads every
  batch but keeps only the hashes of its shard (the rendezvous hash of the webhooks), and a
  WebSocket client subscribes on the instance of its shard, or the instance forwards. The
  decode of a batch stays on every instance. Q4 holds the step past that.

### 4.5 Single lookups on the egress

- `eth_getTransactionReceipt` and `kardamom_getTransactionStatus` stay on the egress HTTP
  endpoint (`:8548`), for clients that do not stream.
- **A short wait.** A read for a hash that the egress does not hold waits up to 50 ms for its
  receipt, and then answers `null`. The wait is bounded: at most 4,096 waiting reads for each
  instance, and a read above the bound answers `null` at once. The owner thread is not blocked:
  the wait is a map from hash to a reply channel, which `take_receipts` drains.
- **The fallback for old receipts** moves from the ingress to the egress: a hash that the egress
  index no longer holds asks the executor query (`--executor-query-endpoints`), with the bounds
  of #796.
- The edge sends the receipt reads to the egress. On staging the load balancer serves only the
  ingress JSON-RPC (kardamom-infra). A new target for the egress is part of this change.

### 4.6 The ingress after the change

- No receipt cache, no `--receipt-query-endpoints`, no executor receipt fallback.
- `eth_getTransactionReceipt` answers the JSON-RPC error `-32601` with a message that names the
  egress endpoint.
- No `kardamom_subscribeReceipts`.
- No cluster tap: no "sealed", no `tx_status`.
  - The on-quorum ack (`--ack-policy on-quorum`) needs the durable count of the tap. It goes,
    and `--ack validated` becomes the only ack (Q5).
  - The halt of `chain.rs` needs the `ClusterStatus`. It comes from the egress (Q1).

### 4.7 The egress nodes

- Two nodes, `egress-0` and `egress-1`, with the class `role=egress`, on cpx22 (2 vCPU, 4 GB).
  The ramp of section 7, step 1, confirms the size before the apply.
- The job constrains on `meta.role = egress`, with `distinct_hosts`. The store is on the disk of
  the node.
- The readers find the egress through `kardamom-egress.service.consul`, so they need no change.
- The move, one instance at a time: create the nodes, change the constraint, roll one instance,
  which fills its store from its peer (the catch-up of the egress), check it, then roll the
  other. With two instances one always serves.
- `exec_frames` then crosses the network, not the host. The extra hop is well under 1 ms.

## 5. What this removes

- The ingress: the receipt cache, the receipt query, the fallback of #796, `kardamom_subscribeReceipts`,
  the cluster tap and its thread, `tx_status`, the on-quorum ack, and their tests and flags.
- The notifier: its receipt source switch (`--receipts-from-egress`, the executor fan-in).
- The streams `tx_status` and, for the notifier, `tx_errors`. The sequencers keep `tx_errors` while the
  ingress reads it (`crates/ingress/src/proxy/watchers.rs`) and Q6 decides it.
- The metric `kardamom_ingress_seal_latency_seconds`, the canary stage `sealed` and their panels. The
  notifier exports the stage latencies instead (section 6).

## 6. Observability

- `kardamom_notifier_stage_latency_seconds{stage}`: the time from the first `accepted` of a hash
  to the first event of each later stage, on the clock of the notifier.
- `kardamom_notifier_executed_latency_seconds{executor}`: the time from `accepted` to the
  `executed` of each executor. So a slow executor shows.
- `kardamom_notifier_merge_latency_seconds`: the time from the first `executed` to the first
  `receipt`: the cost of the hop to the egress and its merge.
- `kardamom_tx_events_published_total{origin}` and `kardamom_tx_events_dropped_total{origin}` on each
  publisher.
- `kardamom_notifier_event_gaps_total{origin}` and `kardamom_notifier_catchup_receipts_total`.
- The canary reads its stages from the notifier feed. The performance dashboard takes the
  notifier stage latencies in place of the ingress seal latency.

## 7. The rollout

Every deploy stays backward compatible: a breaking change goes only through an upgrade transaction and a feature flag. Each step has a switch, off by default.

1. **Measure.** Ramp with the current layout. Record the egress CPU and memory (`nomad alloc
   status -stats`) and the notifier CPU on notifier-0. This sizes 4.4 and 4.7.
2. **The egress nodes** (kardamom-infra, then the job constraint). The move of 4.7.
3. **The publishers.** `KARDAMOM_TX_EVENTS=on` adds the `tx_events` publications in the ingress, the
   sequencer, the egress and the batcher. `KARDAMOM_TX_EVENTS_EXECUTORS=on` adds the `executed`
   events of the executors. The old streams stay.
4. **The notifier reads both.** `KARDAMOM_NOTIFIER_EVENTS=on`: the notifier takes `tx_events`, and
   ignores the old streams for the stages that `tx_events` carries. Compare the two on the
   canary for a day.
5. **The clients.** The receipt feed on the notifier, the short wait and the fallback on the egress,
   the edge target, `kardamom-load --receipt-rpc` and its feed on the notifier, the canary.
6. **The removals** of section 5, with the old switches. This step is not reversible without a
   deploy of the release before, so it comes last, after a week on staging.

## 8. Open questions, with the recommended answer

- **Q1. The source of the `ClusterStatus` for the ingress halt.** Recommended: the egress. It
  takes the order from the executors, and the status frames come with it, if `exec_frames`
  carries them. If not, the sequencer, which has a cluster session. To check in the review.
- **Q2. The catch-up of `receipt`.** Recommended: each egress publication carries a sequence
  number for each origin. A gap makes the notifier read `GET /v1/frames` or the receipt index
  of the egress from the last receipt it took.
- **Q3. The catch-up of `posted`.** Recommended: the notifier reads `kardamom_batcher` posted
  cursor at start and after a gap, from the L1 `BatchPosted` logs through the L1 indexer.
- **Q4. Notifier scale past one decode for each instance.** Recommended: later, a topic for each
  shard, with the publishers splitting their batches by the shard of the hash. Not in this
  change.
- **Q5. The on-quorum ack goes.** Recommended: yes. Staging and the load tests use
  `--ack validated`, and the push of `receipt` gives a client the same certainty sooner.
- **Q6. `tx_errors`.** Recommended: the sequencer drops (`Dropped`) cover the client side. The
  ingress keeps its `tx_errors` watcher only while the on-quorum ack exists, so it goes with Q5.

- **Q7. The executor events after the evaluation.** Recommended: keep the switch, and decide
  from the two latencies of section 6. If the merge latency stays below 1 ms at the peak, turn
  the executor events off and keep `receipt` only. If a client needs the earliest signal, keep
  them, and only for the first executor copy to the clients.

## 9. The PRs

| PR | Repo | Content |
|---|---|---|
| P1 | kardamom-infra | the egress nodes |
| P2 | kardamom | the egress constraint `role=egress`, the README move steps |
| P3 | kardamom | the `tx_events` format and codec, the publishers (the executors included), `KARDAMOM_TX_EVENTS` and `KARDAMOM_TX_EVENTS_EXECUTORS` |
| P4 | kardamom | the notifier on `tx_events`: the stage table with its bitmaps, copies, implied stages, rollback, the stage metrics, `KARDAMOM_NOTIFIER_EVENTS` |
| P5 | kardamom | the receipt feed on the notifier, the egress short wait and fallback, the load tool and the canary |
| P6 | kardamom-infra | the edge target for the egress reads |
| P7 | kardamom | the removals of section 5, `docs/specs/egress-node-spec.md` marked superseded, the docs |


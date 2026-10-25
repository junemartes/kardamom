# Client events from one-way stage publications, and the egress on its own nodes

Status: draft, revision 2, 2026-10-25. The decisions in section 1 are the user's (2026-10-25).
Section 10 lists the changes from revision 1.

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
- The ack `--ack receipt` (`AckMode::Receipt`, the default of `crates/ingress/src/config.rs`
  and of the job variable `ack` in `deploy/cluster/nomad/ingress.nomad.hcl`) holds
  `eth_sendRawTransaction` until the receipt comes on the `tx_receipts` fan-in
  (`crates/ingress/src/pending/mod.rs`, `proxy/watchers.rs`). The ack policies
  (`--ack-policy on-offer`, `on-local-fsync`, `on-quorum`, `on-local-fsync-and-quorum`,
  `crates/ingress/src/service/settings.rs`) only shape that wait.
- Every client that awaits a receipt polls `eth_getTransactionReceipt` on the ingress:

  | Client | Code |
  |---|---|
  | the canary | `crates/canary/src/rpc.rs:264-270` |
  | the chaos suite | `crates/chaos/src/rpc.rs:335` (`await_receipt`), `:354` (`receipt_status`) |
  | the e2e harness and its scenarios | `crates/e2e/src/harness/l2.rs:122-124` (`L2Client`), `launch.rs`, `bin/kardamom-semantics.rs` |
  | the load tool | `crates/bench/src/load/engine/mod.rs:102` (`receipt_status`) |
  | the proof `ingress-pair-loss-recover` | `docs/failure-modes.md:555`, `:727`: the receipt of every accepted transaction after both ingresses die |
  | any client of the edge | the staging load balancer serves one JSON-RPC port, the ingress |

### 3.2 The cluster tap of the ingress

The ingress is a client of the sealer cluster. It reads the cluster egress on its own thread
(`crates/ingress/src/cluster.rs:1-24`) and:

- folds the durable count into the watermark of the on-quorum ack;
- publishes "sealed" on `tx_status` for each relayed `TxRef`;
- exports the record-lag gauges, the `kardamom_ingress_cluster_*` gauges, itself;
- forwards the `ClusterStatus` to `crates/ingress/src/chain.rs`.

`chain.rs` keeps the sealer's lifecycle and pauses the submits on it:

- `record_lag` while a status says that the record-lag guard refuses;
- `da_lag` while a status says that the DA-lag guard refuses;
- `sealer_no_quorum` while no status comes for `SEALER_SILENCE` (10 s), or for
  `SNAPSHOT_SILENCE` (60 s) after a status with the snapshot notice
  (`crates/types/src/cluster_status.rs:12`, `:20`). Any status ends a `sealer_no_quorum` halt.

The sealer sends a status on every boundary tick, 2 s in the deploy. The status frame is egress
kind 9 (`EGRESS_KIND_STATUS`, `crates/cluster-adapter/src/wire/mod.rs:323`).

### 3.3 The inputs of the notifier

| Stream | Publisher today |
|---|---|
| `tx_status` | the ingress (`Sealed`) and the sequencers (`Offered`, `Rejected`) |
| `egress_receipts` | the egress (with `KARDAMOM_NOTIFIER_RECEIPTS_FROM_EGRESS=on`), or `tx_receipts` of every executor |
| `tx_errors` | the sequencers |

The sequencers publish their statuses on `tx_status` through `SideChannels::publish_status`
(`crates/sequencer/src/outbound.rs:203`). The e2e scenario
`crates/e2e/src/scenarios/tx_status.rs` checks the stages `offered`, `sealed` and `executed` on
the notifier feed. The notifier already keeps a set of the stages that it saw for each
transaction, one bit for each `TxStageKind` (`crates/types/src/tx_status.rs`, `TxStageKind::bit`).

### 3.4 The egress placement

`deploy/cluster/nomad/egress.nomad.hcl` constrains on `meta.role = executor` with
`distinct_hosts`, and `egress_count` is 2. On staging the instances run on executor-1 and
executor-2, so the store syncs share the disk of the state DB and the checkpoints. Idle with the
canary: about 95 MHz, 435 MiB and a store of 184 MB for each instance.

`docs/specs/egress-node-spec.md` (August 2026) describes another design: an egress role of the
validator. The `kardamom-egress` crate replaced it. P9 of section 9 marks it superseded.

## 4. The design

### 4.1 The topic `tx_events`

One MDC topic with many publishers. A publication is a batch:

```
TxEventBatch {
    origin: Origin,          // ingress, sequencer, executor, egress, batcher, and the instance index
    seq: u64,                // the batch number of this origin, from 0 at its start
    events: Vec<TxEvent>,
}
TxEvent =
    Accepted { hash }
  | Sent { hash, lane }
  | Dropped { hash, sender, nonce, reason }    // reason: a DropReason or a TxErrorReason (4.2)
  | Executed { hash, block, position }         // from one executor, its own copy
  | Receipt { hash, block, position, receipt } // from the egress: the receipt of egress_receipts
  | RolledBack { from_block }                  // an egress rollback frame
  | Posted { batch_index, first_block, last_block, l1_tx }
  | Included { batch_index, l1_block }
  | ChainStatus { status }                     // from the sequencer: the ClusterStatus (4.6)
```

- The codec is rkyv, as the other streams. The format goes in `formats.toml` (until #664
  removes it) with `writes = 1`.
- A publisher fills a batch for each poll cycle of its loop. It sends the batch at the end of
  the cycle.
- A publisher never waits for the notifier. A failed offer is counted
  (`kardamom_tx_events_dropped_total{origin}`), and the batch is dropped.
- `position` is the canonical position of the transaction (`BPosition`). The egress catch-up of
  4.5 reads by it.
- `Posted` and `Included` name a block range, not hashes. The notifier maps them to hashes
  (4.4).

### 4.2 The publishers

| Publisher | The change |
|---|---|
| ingress | `Accepted` for each transaction that it published on `tx_data`. |
| sequencer | `Sent`, `Dropped` and `ChainStatus`, by the map below. |
| executor | `Executed` for each transaction that it applied, behind its own switch (Q7). |
| egress | `Receipt` and `RolledBack` from the merged receipts, the same frames as `egress_receipts`. |
| batcher | `Posted` when it sends a batch, `Included` when the L1 receipt of the batch transaction comes. |

The egress keeps `egress_receipts` for the sequencers. The executors keep `tx_receipts`. The
`Executed` events carry no receipt bytes, so they stay small: 32 bytes of hash and two numbers.

The map of each sequencer output to `tx_events`:

| Sequencer output today | Code | `tx_events` |
|---|---|---|
| `TxStatus` `Offered { sender, nonce }` on `tx_status` | `sequencer.rs:598` (v1), `v2/mod.rs:510` | `Sent` |
| `TxStatus` `Rejected { sender, nonce, reason }` on `tx_status` | `sequencer.rs:380` (v1), `v2/mod.rs:580` | `Dropped`, reason `Rejected(TxErrorReason)`. Final: the sequencer never orders it. |
| `TxDrop { sender, nonce, reason }` on `tx_drops` | the drops of #768 | `Dropped`, reason `Drop(DropReason)`. Provisional (4.3). |
| a status frame of the cluster egress (kind 9) | `bin/kardamom-sequencer/feeds.rs:494` (`on_status_frame`) | `ChainStatus` |
| the v2 verdicts `Accepted` and `Rejected` on `tx_verdicts` to an ingress | `v2/verdicts.rs:51`, `:62` | none. The verdicts answer the ingress (the admission ack), not the clients. |

### 4.3 Copies, order and loss

- **Copies.**
  - Two sequencers race each transaction, so two `sent` events come.
  - Three executors apply it, so three `executed` events come.
  - Two egress instances publish each receipt, so two `receipt` events come.
  - The notifier keeps the first event of each `(hash, stage)`. It drops the copies (the stage
    table of 4.4).
  - A copy of `Posted` or `Included` is dropped by `(batch_index, stage)`.
  - Clients already deduplicate by `tx_hash` plus stage.
- **Order.** The topic has no order across publishers. `sent` can come before `accepted`, and
  `receipt` before `executed`. A client must still see the stages in order. So the notifier
  applies these rules:
  - The order is `accepted` < `sent` < `executed` < `receipt` < `posted` < `included`.
  - When an event comes, the notifier emits each earlier stage that it did not emit. It marks
    them `implied`. Then it emits the stage of the event.
  - A real event of an implied stage that comes later is not emitted again.
  - A late `executed` still sets its bit in `observed` and in `executors`. So the latency of a
    slow executor stays in section 6.
  - `dropped` implies `accepted` only. A sequencer can drop before it offers (`txpool_full`).
  - A `Drop` reason is provisional, as `kardamom_getTransactionStatus` says today. A `receipt`
    after it is emitted, after the stages before it that are not emitted yet.
  - A `Rejected` reason is final. The sequencer does not order the transaction.
  - The latencies of section 6 count only the stages that the notifier observed. They do not
    count the implied ones.
- **Loss.** MDC offers are best effort, for example `NOT_CONNECTED` during a restart.
  - `accepted`, `sent`, `dropped` and `executed` are hints. A lost one is not sent again.
  - `receipt` must not be lost. A gap in the `seq` of an egress origin, or a new session of an
    origin, starts the catch-up of 4.5.
  - `posted` and `included` come from L1. After a gap, the notifier reads them again (Q3).
- **Rollback.** `RolledBack { from_block }` revokes every `executed` and `receipt` at or above
  `from_block`. The notifier sends a `rolled_back` event to each subscriber of an affected hash.
  A client must treat `executed` and `receipt` as revocable until `posted`.

### 4.4 The notifier

- It reads only `tx_events`. The taps on `tx_status`, `tx_receipts`, `egress_receipts` and
  `tx_errors` go in step 7 of section 7.
- **The stage table.** Each instance has one fixed table. It allocates the table at the start,
  with the size of the ring (250,000 entries on the notifier nodes). The table holds no `Vec`,
  no `String` and no box. The event path allocates nothing for a hash that no client follows.
  - An entry is `{ hash: [u8; 32], emitted: u8, observed: u8, executors: u8, block: u64,
    accepted_ms: u32 }`, about 56 bytes. So the table uses about 14 MB.
  - `emitted` and `observed` are bitmaps, one bit for each stage. They extend the bits of
    `TxStageKind::bit`.
  - `emitted` decides the copies and the implied stages.
  - `observed` keeps the stages that really came, for the latencies.
  - `executors` has one bit for each executor index. So the notifier sees which executors
    reported.
  - `block` comes from the first `executed` or `receipt`. It maps a `Posted` range to its
    hashes. So no separate map from hash to block is necessary.
  - `accepted_ms` is the time of the first observed event of the hash, in milliseconds from the
    start of the instance. The latencies of section 6 count from it.
  - The table uses open addressing on the first 8 bytes of the hash, with a bounded probe.
  - A full probe overwrites its oldest entry, the one with the smallest `accepted_ms`.
  - A hash older than the table gets `posted` only through a status query (4.5).
  - The batch decode reads the archived rkyv bytes in place. It does not copy each event.
  - A rollback clears the `executed` and `receipt` bits and `executors` of each entry at or above
    `from_block`. It sets `block` to 0. Then the new receipt after the rollback is emitted again.
  - One thread owns the table: the ingest thread of the notifier. No lock is necessary.
- **The receipt feed.** `kardamom_subscribeReceipts` moves from the ingress to the notifier,
  next to its status feed, with the same frames. Its users (3.1) point to the notifier.
- **The load.**
  - The measure on staging is about 0.2 core for each 1,000 tx/s, today, with every instance
    reading every event.
  - With the executor events, about 11 events come in for each transaction before the copies
    are dropped: 1 accepted, 2 sent, 3 executed, 2 receipts, and the batch events.
  - So 10,000 tx/s needs about 3 to 4 cores of decode. Without the executor events, it needs
    about half that.
  - The shards of P5 (section 9) split that work. Q4 holds the step after them.
- **The shards (P5).**
  - Each instance reads every batch.
  - Each instance keeps only the hashes of its shard in its table. The shard is the rendezvous
    hash of the webhooks.
  - A WebSocket client subscribes on the instance of its shard.
  - An instance that gets a subscription of another shard forwards it to that instance.
  - The decode of each batch stays on every instance.

### 4.5 The egress reads

- `eth_getTransactionReceipt` and `kardamom_getTransactionStatus` stay on the egress HTTP
  endpoint (`:8548`), for clients that do not stream.
- **The short wait.**
  - A read for a hash that the egress does not hold waits up to 50 ms for its receipt. Then it
    answers `null`.
  - Today the HTTP task already asks the owner thread with `Input::Receipt { reply }`
    (`crates/egress/src/http.rs:262-270`). A miss now puts the reply channel in a map from hash
    to reply channels. That costs one map operation on the owner thread for each waiting read.
  - The timeout lives in the HTTP task. The owner thread never waits.
  - `take_receipts` answers the waiting reads of each new receipt. It also removes each entry
    whose reply channel is closed, so dead waits do not fill the bound.
  - The bound is 4,096 waiting reads for each instance. A read above the bound answers `null` at
    once.
- **The fallback for old receipts** moves from the ingress to the egress. A hash that the egress
  index no longer holds asks the executor query (`--executor-query-endpoints`), with the bounds
  of #796.
- **The receipt range read.** `GET /v1/receipts?from=<position>` answers the receipts after the
  canonical position `from`, in order, up to a byte limit, from the receipt index
  (`crates/egress/src/receipts.rs`, the `order` of the newest receipts). Each egress instance has
  the same positions, so a reader can use either instance.
  - When `from` is below the oldest receipt of the index, it answers `410` with the position of
    the oldest receipt that it holds.
  - The index keeps the newest `KARDAMOM_EGRESS_RECEIPTS` receipts (1,000,000 by default).
- **The catch-up of the notifier.** After a gap of an egress origin, the notifier reads
  `/v1/receipts` from the newest position that it took.
  - On a `410`, the gap is older than the index. The notifier then asks
    `eth_getTransactionReceipt` for each followed hash of its table that has no `receipt` bit. A
    hash that no client follows gets nothing.
  - It counts both cases (`kardamom_notifier_catchup_receipts_total{source}`).
- **The second endpoint.** The edge load balancer serves one JSON-RPC port, so it cannot send one
  method to another backend. So the egress reads get their own endpoint: a separate host or port
  on the load balancer, with the egress instances as targets (P8). Each client of 3.1 learns that
  endpoint:
  - the canary, the chaos suite, the e2e harness and the load tool get a flag for it;
  - the proof `ingress-pair-loss-recover` reads the receipts from the egress;
  - the docs tell the edge clients the new endpoint.

### 4.6 The ingress after the change

- No receipt cache, no `--receipt-query-endpoints`, no executor receipt fallback.
- `eth_getTransactionReceipt` answers the JSON-RPC error `-32601` with a message that names the
  egress endpoint.
- No `kardamom_subscribeReceipts`.
- **The ack.** `--ack validated` becomes the only ack, and the default of the binary and of the job
  variable `ack`.
  - `--ack receipt` goes, and with it the wait on the `tx_receipts` fan-in (`PendingReceipts`).
  - The ack policies go: `on-offer`, `on-local-fsync`, `on-quorum` and
    `on-local-fsync-and-quorum`. They only shape the receipt wait.
  - The client impact: a client that relied on `receipt` gets its answer before the execution.
    It must take the receipt from the notifier feed, or poll the egress.
  - The admission ack (`AckMode::Admission`, on the `tx_verdicts` of the v2 loop) waits for no
    receipt, and has no job variable. This spec does not change it.
- **No cluster tap.** No "sealed", no `tx_status`, no `kardamom_ingress_cluster_*` gauges.
- **The halt status from `tx_events`.**
  - Each sequencer publishes each status frame of its cluster session as `ChainStatus`. The
    status frame does not reach the egress: `exec_frames` carries only the kinds 1 and 2
    (`crates/engine/src/reader/tee.rs:62-63`).
  - Both twins publish, so each status comes twice. The ingress keeps the status with the higher
    `sealed_head`. At an equal `sealed_head`, it keeps the one that came last.
  - `chain.rs` keeps its rules of 3.2 on these statuses, with the same budgets. A `ChainStatus`
    is a status for the silence rule: no `ChainStatus` for 10 s, or for 60 s after one with the
    snapshot notice, raises `sealer_no_quorum`, and the ingress pauses the submits. The next
    `ChainStatus` ends it.
  - So a gap of both sequencers pauses the submits after 10 s, as a lost quorum does today. With
    both sequencers down, no submit can land anyway.
  - The sequencer forwards every status frame, not only the ones with the snapshot notice. That
    is one event each 2 s for each twin.
  - The `ChainStatus` events go to the ingress over `tx_events`, so the ingress subscribes to the
    topic. It decodes only `ChainStatus` from the sequencer origins, and skips the rest of each
    batch.

### 4.7 The egress nodes

- Two nodes, `egress-0` and `egress-1`, with the class `role=egress`, on cpx22 (2 vCPU, 4 GB). The
  ramp of section 7, step 1, confirms the size before the apply.
- The store is on the disk of the node.
- The readers find the egress through `kardamom-egress.service.consul`, so they need no change.
- `exec_frames` then crosses the network, not the host. The extra hop is well under 1 ms.
- **The switch.** `KARDAMOM_EGRESS_NODES=on` sets the job constraint to `meta.role = egress`, with
  `distinct_hosts`. Off, the constraint stays `meta.role = executor`. The default is off.
- **The move.** A new constraint does not move a running allocation. So the move is:
  1. Create the nodes (P1).
  2. Deploy with `KARDAMOM_EGRESS_NODES=on`.
  3. Stop one instance with `nomad alloc stop`. Nomad places it again on an egress node. The new
     instance fills its store from its peer (the catch-up of the egress).
  4. Check the new instance: its head, its gauges and the readers.
  5. Stop the other instance, and check it.

  With two instances, one always serves.

## 5. What this removes

- **The ingress:**
  - the receipt cache, the receipt query and the fallback of #796
    (`crates/ingress/src/receipt_query.rs`);
  - `kardamom_subscribeReceipts`;
  - `--ack receipt` and the ack policies (4.6), with `PendingReceipts`;
  - the cluster tap and its thread, the `Sealed` publish on `tx_status`, and the record-lag
    gauges and the `kardamom_ingress_cluster_*` gauges of the tap;
  - their tests and flags.
- **The notifier:** its receipt source switch (`--receipts-from-egress`, the executor fan-in).
- **The streams:**
  - `tx_status`, after the sequencers publish `Sent` and `Dropped` (4.2);
  - `tx_errors` for the notifier. The ingress reads `tx_errors` only for the receipt ack
    (`crates/ingress/src/proxy/watchers.rs`), so it goes with that ack (Q6).
- **The switches:** `KARDAMOM_INGRESS_RECEIPTS_FROM_EGRESS` (job variable
  `receipt_query_endpoints` of the ingress) and `KARDAMOM_NOTIFIER_RECEIPTS_FROM_EGRESS`, and
  their kardamom-infra variables (P8).
- **The metrics and their users:**
  - `kardamom_ingress_seal_latency_seconds`, with its panels in the performance dashboard and
    in `kardamom-bottlenecks.json:715`;
  - the `kardamom_ingress_cluster_*` gauges, with their panels in `kardamom-availability.json`
    (for example `:739`, `:749`, `:885`);
  - the canary stage `sealed`, with its outcome `missing_sealed`
    (`crates/canary/src/probes/mod.rs:200`) and its place in the list of outcomes that start at 0.
  - The notifier exports the stage latencies in their place (section 6). The panels of the
    sealed head take `ChainStatus` from the notifier or the sequencer (P6).
- **The e2e scenario** `crates/e2e/src/scenarios/tx_status.rs` moves to `tx_events`: the stages
  `accepted`, `sent`, `executed` and `receipt` on the notifier feed (P6).

## 6. Observability

- `kardamom_notifier_stage_latency_seconds{stage}`: the time from the first `accepted` of a hash
  to the first event of each later stage, on the clock of the notifier.
- `kardamom_notifier_executed_latency_seconds{executor}`: the time from `accepted` to the
  `executed` of each executor. So a slow executor shows.
- `kardamom_notifier_merge_latency_seconds`: the time from the first `executed` to the first
  `receipt`: the cost of the hop to the egress and its merge.
- `kardamom_tx_events_published_total{origin}` and `kardamom_tx_events_dropped_total{origin}` on
  each publisher.
- `kardamom_notifier_event_gaps_total{origin}` and
  `kardamom_notifier_catchup_receipts_total{source}`.
- `kardamom_ingress_chain_status_age_seconds`: the time since the last `ChainStatus` that the
  ingress took.
- The canary reads its stages from the notifier feed. The performance dashboard takes the
  notifier stage latencies in place of the ingress seal latency.

## 7. The rollout

Every deploy stays backward compatible. A breaking change goes only through an upgrade
transaction and a feature flag. Each step has a switch, off by default.

1. **Measure.** Ramp with the current layout. Record the egress CPU and memory
   (`nomad alloc status -stats`) and the notifier CPU on notifier-0. This sizes 4.4 and 4.7.
2. **The egress nodes.** P1, then P2 with `KARDAMOM_EGRESS_NODES=on`, then the move of 4.7.
3. **The publishers.** `KARDAMOM_TX_EVENTS=on` adds the `tx_events` publications in the ingress,
   the sequencer, the egress and the batcher. `KARDAMOM_TX_EVENTS_EXECUTORS=on` adds the
   `executed` events of the executors. The old streams stay.
4. **The notifier reads both.** `KARDAMOM_NOTIFIER_EVENTS=on`: the notifier takes `tx_events`. It
   ignores the old streams for the stages that `tx_events` carries. Compare the two on the canary
   for a day.
5. **The ingress halt from `tx_events`.** `KARDAMOM_INGRESS_CHAIN_STATUS=events`: the ingress takes
   its status from `ChainStatus`. The tap stays, and compares its status with the events. Then
   the tap stops publishing `Sealed`.
6. **The clients.** P6, P7 and P8: the receipt feed on the notifier, the egress reads, the second
   endpoint, the clients of 3.1, the canary and the e2e scenario. The ack default becomes
   `validated` on every network.
7. **The removals** of section 5 (P9). This step is one way: only a deploy of the release before
   undoes it. So it comes last, after a week on staging, after steps 1 to 6.

## 8. Open questions, with the recommended answer

- **Q1. The source of the `ClusterStatus` for the ingress halt.** Answered in revision 2: the
  sequencer, as `ChainStatus` on `tx_events` (4.6). The egress does not see the status frames.
- **Q2. The catch-up of `receipt`.** Answered in revision 2: the `seq` of each origin, and
  `GET /v1/receipts` on the egress (4.5).
- **Q3. The catch-up of `posted`.** Recommended: the notifier reads the batcher's posted cursor at
  the start and after a gap, from the L1 `BatchPosted` logs through the L1 indexer.
- **Q4. Notifier scale past one decode for each instance.** Recommended: later, a topic for each
  shard. The publishers then split their batches by the shard of the hash. Not in this change.
- **Q5. The receipt ack and the ack policies go.** Recommended: yes (4.6). Staging and the load
  tests use `--ack validated`, and the push of `receipt` gives a client the same certainty sooner.
- **Q6. `tx_errors`.** Recommended: the `Dropped` events cover the client side. The ingress reads
  `tx_errors` only for the receipt ack, so it goes with Q5.
- **Q7. The executor events after the evaluation.** Recommended: keep the switch. Decide from the
  two latencies of section 6. If the merge latency stays below 1 ms at the peak, turn the
  executor events off, and keep `receipt` only. If a client needs the earliest signal, keep them,
  and send only the first executor copy to the clients.
- **Q8. io_uring in the notifier.** It does not replace the stage table: the table is memory
  state, and io_uring is a way to submit I/O. It can serve the output side: the WebSocket sends,
  the webhook posts, and the outbox and cursor writes with their syncs. The `tx_events` ingest
  gains nothing, because Aeron uses shared memory. Recommended: not in this change. Profile the
  notifier at the peak of the ramp after P4. If the system calls of the output side are the cost,
  write a separate spec for an io_uring runtime (tokio-uring, monoio or glommio) on those paths.

## 9. The PRs

Each PR merges alone. Each new path is behind a switch that is off by default, until P9.

| PR | Repo | Content | Depends on |
|---|---|---|---|
| P1 | kardamom-infra | the egress nodes | none |
| P2 | kardamom | `KARDAMOM_EGRESS_NODES` and the constraint `role=egress`, the README move steps | none (off by default); the move needs P1 |
| P3 | kardamom | the `tx_events` format and codec, the publishers (the executors and `ChainStatus` included), `KARDAMOM_TX_EVENTS` and `KARDAMOM_TX_EVENTS_EXECUTORS` | none |
| P4 | kardamom | the notifier on `tx_events`: the stage table with its bitmaps, the copies, the implied stages, the rollback, the stage metrics, `KARDAMOM_NOTIFIER_EVENTS` | P3 |
| P5 | kardamom | the shards of the notifier (4.4) | P4 |
| P6 | kardamom | the ingress halt from `ChainStatus` (`KARDAMOM_INGRESS_CHAIN_STATUS`), the e2e scenario on `tx_events`, the dashboard panels | P3 |
| P7 | kardamom | the receipt feed on the notifier; the egress short wait, the fallback and `/v1/receipts`; the notifier catch-up; the receipt endpoint flag of the canary, the chaos suite, the e2e harness and the load tool | P4 |
| P8 | kardamom-infra | the second endpoint for the egress reads, and the infra variables of the new switches | P7 |
| P9 | kardamom | the removals of section 5, the ack default `validated`, `docs/specs/egress-node-spec.md` marked superseded, the docs | P6, P7, P8 on staging for a week |

## 10. Changes from revision 1

| Finding of the review | Change |
|---|---|
| 1 (major). `exec_frames` does not carry the `ClusterStatus` | 3.2, 4.1, 4.6: the sequencer publishes `ChainStatus` on `tx_events`, the ingress keeps the newest by `sealed_head`, and the silence rule of `chain.rs` holds on a gap. Q1 answered. Step 5 of section 7. |
| 2 (major). The sequencers also publish `tx_status` | 3.3: the sequencer publishers. 4.2: the map of each sequencer output to `tx_events`. 5: `tx_status` goes after that map, and the e2e scenario moves (P6). |
| 3 (major). `--ack receipt` uses the receipt fan-in | 3.1: the receipt ack and the ack policies. 4.6: `validated` becomes the only ack and the default, with the client impact. 5, step 6 of section 7, P9. |
| 4 (major). The egress has no receipt range read | 4.1: `seq` and `position`. 4.5: `GET /v1/receipts`, its `410`, and the notifier catch-up with the case of a gap older than the index. Q2 answered. |
| 5 (major). Many clients poll the ingress for receipts | 3.1: the table of the clients. 4.5: the second endpoint and what each client learns. P7 and P8 come before P9. |
| 6 (minor). The short wait on the owner thread | 4.5: the timeout in the HTTP task, the one map operation, and the removal of closed reply channels. |
| 7 (minor). A late `executed` | 4.3: a late `executed` still sets `observed` and `executors`. |
| 8 (minor). Missed removals | 5: the gauges of the tap, the bottlenecks and availability panels, the two switches and their infra variables, `receipt_query.rs`, `missing_sealed`. |
| 9 (minor). A new constraint does not move an allocation | 4.7: `KARDAMOM_EGRESS_NODES`, then `nomad alloc stop` one instance at a time. P2 merges alone. |
| 10 (minor). P4 is large | 9: the shards are P5. Each PR names what it depends on. |
| 11 (minor). STE in 4.3 and 4.4 | 4.3 and 4.4: short sentences in lists, one instruction in each. |

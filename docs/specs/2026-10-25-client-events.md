# Client events from one-way stage publications, and the egress on its own nodes

Status: draft, revision 7, 2026-10-25. The decisions in section 1 are the user's (2026-10-25).
Section 10 lists the changes of each revision.

## 1. Decisions

1. The ingress never looks up receipts. It has no receipt cache, no receipt query and no receipt
   feed.
2. Receipts reach clients by push. The notifier serves the WebSocket subscriptions. The notifier
   and the egress stay two processes.
   - The egress serves every other RPC read endpoint: `eth_getTransactionReceipt`,
     `kardamom_getTransactionStatus` and the other read methods. The spec of #819 (decision 46)
     owns "the egress serves every read". This spec covers only the receipt and status reads
     (4.5).
   - The webhooks of the notifier are removed (the user, 2026-10-25: "Let's remove the webhooks
     for now then. Later on we should add support via a task queue like sqs"). The removal takes
     the registry, the outbox, the cursors, the delivery loop, the forwarding of registrations to
     the peers, the sharding by subscription, the state directory `/opt/kardamom/notifier` and
     the move procedure of that state. So the notifier becomes stateless.
   - Offline delivery comes later, through a task queue such as SQS, in its own spec.
3. There is no "sealed" status. The client events come from one-way stage publications:

   | Stage | Publisher | Stream that the notifier reads |
   |---|---|---|
   | `accepted` | ingress | `tx_events` (new) |
   | `sent` | sequencer | `tx_events` (new, in place of the `Offered` status of each transaction) |
   | `dropped` | sequencer | `tx_drops` (exists, #768) |
   | `rejected` | sequencer | `tx_events` (new, kind 3) |
   | `executed` | executor, each of them | `tx_receipts` (exists), behind `KARDAMOM_NOTIFIER_EXECUTED` (decision 9) |
   | `receipt` | egress | `egress_receipts` (exists), with its rollback frames |
   | `posted` | batcher | `tx_events` (new, once for each batch) |
   | `included` | batcher | `tx_events` (new, once for each batch) |

4. The egress runs on its own nodes (`role=egress`), not on the executor nodes.
5. The notifier publications are best effort. They never halt the chain. A lost event, a lost
   batch, a stalled notifier or a notifier that is down never stops admission, ordering or
   execution (4.8).
6. The events have a negligible cost on the hot path: no allocation and no encode for each
   event, one offer for each poll cycle, and no wait (4.8).
7. An explicit halt of the ingress (record lag or DA lag, from `chain_status`) ends when its
   newest status is older than 30 s, and the ingress then admits again. A silence of
   `chain_status` never starts a halt, and never keeps one. The reason is decision 5: a
   best-effort stream never stops admission. The guards of the sealer stay the real stop
   (4.6).
8. Drops and refusals are pushed as events too (the user, 2026-10-25): `dropped` from `tx_drops`,
   and `rejected` from the sequencer (`tx_events` kind 3). The egress status API
   (`kardamom_getTransactionStatus`) also serves them.
9. The notifier may read receipts from the executors (the user, 2026-10-25: "It is fine for the
   notifier to read from the executor"). The `executed` tap from `tx_receipts`, behind
   `KARDAMOM_NOTIFIER_EXECUTED`, is an exception to #819 decision 45 (the services read receipts
   from the egress).

For now the notifier reads both the executors and the egress, so that the stages can be measured
apart: the time of each executor, and the time from the executors to the merged receipt. The
executor tap stays behind `KARDAMOM_NOTIFIER_EXECUTED`, off by default, for the evaluation only.
The user decided that it may stay (decision 9). The executors and the egress
publish nothing new: their streams already carry these stages.

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

One MDC topic with many publishers. It carries only the stages that no existing stream carries:
`accepted`, `sent`, a final rejection, `posted` and `included`.

**The hot frame.** The ingress and the sequencer publish a fixed-layout frame, not rkyv. The
encode is a copy of fixed records into a buffer.

```
header (16 bytes, little endian):
    magic: u16        // 0x4B45 ("KE")
    version: u8       // 1
    kind: u8          // 1 = accepted, 2 = sent, 3 = rejected
    origin: u16       // the instance index of the publisher
    count: u16        // the number of records
    seq: u64          // the frame number of this origin and kind, from 0 at its start
records, `count` of them, each of a fixed size for its kind:
    accepted:  hash [u8; 32]                          // 32 bytes
    sent:      hash [u8; 32], lane: u8                // 33 bytes
    rejected:  hash [u8; 32], reason: u16             // 34 bytes, a TxErrorReason code
```

- A frame holds one kind, and at most 1,024 records (about 34 KB, below the maximum message of
  the term).
- The format goes in `formats.toml` (until #664 removes it) with `writes = 1`, and a golden
  layout test, as the other fixed layouts.
- The notifier reads the records in place from the Aeron buffer. It copies only a hash that a
  client follows.

**The batcher frame.** `Posted { batch_index, first_block, last_block, l1_tx }` and
`Included { batch_index, l1_block }` come once for each batch, off the hot path. Their codec is
rkyv, with their own format entry.

- `Posted` and `Included` name a block range, not hashes. The notifier maps them to hashes
  (4.4).

**The stream `chain_status`** carries one `ChainStatus { status: ClusterStatus }` for each
status frame. Its codec is rkyv, and its format goes in `formats.toml` with `writes = 1`.

### 4.2 The publishers

| Publisher | The change |
|---|---|
| ingress | An `accepted` record for each transaction that it published on `tx_data` (4.8). |
| sequencer | A `sent` record for each offer to the sealer, in place of its `TxStatus` `Offered` publish of each transaction. A `rejected` record for each rejection. `ChainStatus` on `chain_status`. |
| executor | Nothing. The notifier taps `tx_receipts` behind its switch (Q7). |
| egress | Nothing. The notifier taps `egress_receipts`, with the rollback frames. |
| batcher | `Posted` when it sends a batch, `Included` when the L1 receipt of the batch transaction comes. |

The map of each sequencer output:

| Sequencer output today | Code | After the change |
|---|---|---|
| `TxStatus` `Offered { sender, nonce }` on `tx_status`, one publish for each transaction | `sequencer.rs:583` (`record_one_published`, v1), `v2/mod.rs:496` (`flush`) | a `sent` record in the hot frame of the cycle |
| `TxStatus` `Rejected { sender, nonce, reason }` on `tx_status` | `sequencer.rs:380` (v1), `v2/mod.rs:580` | a `rejected` record. Final: the sequencer never orders it. |
| `TxDrop { sender, nonce, reason }` on `tx_drops` | the drops of #768 | no change. The notifier taps `tx_drops`. Provisional (4.3). |
| a status frame of the cluster egress (kind 9) | `bin/kardamom-sequencer/feeds.rs:494` (`on_status_frame`) | `ChainStatus` on `chain_status` (4.6) |
| the v2 verdicts `Accepted` and `Rejected` on `tx_verdicts` to an ingress | `v2/verdicts.rs:51`, `:62` | no change. The verdicts answer the ingress (the admission ack), not the clients. |

The two other kinds of `TxStatus` (`crates/types/src/tx_status.rs:70-85`) also have a stage, so the
removal of `tx_status` leaves no kind unmapped:

| `TxStatus` kind | Where it comes from today | After the change |
|---|---|---|
| `Accepted` | the v2 sequencer, as its admission verdict on `tx_verdicts` (`v2/verdicts.rs:51`) | the client stage `accepted` comes from the ingress. The verdict stays on `tx_verdicts`. |
| `Executed` | the notifier derives it from each receipt (`crates/notifier/src/taps.rs:44`) | the notifier derives `receipt` from `egress_receipts`, and `executed` from `tx_receipts` |

### 4.3 Copies, order and loss

- **Copies.**
  - Two sequencers race each transaction, so two `sent` records come, and a drop can come twice
    on `tx_drops`.
  - Three executors apply it, so three receipts come on `tx_receipts` while the tap is on.
  - Two egress instances publish each receipt on `egress_receipts`, so two copies come.
  - The notifier keeps the first event of each `(hash, stage)`. It drops the copies (the stage
    table of 4.4).
  - A copy of `Posted` or `Included` is dropped by `(batch_index, stage)`.
  - Clients already deduplicate by `tx_hash` plus stage.
- **Order.** The streams have no order across publishers. `sent` can come before `accepted`, and
  `receipt` before `executed`. The notifier sends only the latest state of a hash (user decision,
  2026-10-25). It does not fill in the stages that it missed. These rules apply:
  - The order is `accepted` < `sent` < `executed` < `receipt` < `posted` < `included`.
  - Each hash has a state: the highest stage that the notifier sent for it.
  - An event above the state is sent, and it becomes the state.
  - An event at or below the state came too late. The notifier does not send it.
  - So a client sees the stages in order, and it can see a stage skipped. For example,
    `accepted` then `receipt`, with no `sent` and no `executed`.
  - A late event still sets its bit in `observed`, and a late `executed` sets its bit in
    `executors`. So the latencies of section 6 keep every stage that came, also a slow executor.
  - `dropped` is not a stage of the order, and it does not change `state`. It sets its bit in
    `observed`, and the bit decides a copy: the notifier sends `dropped` once, and only while
    `state` is below `executed`. A drop on `tx_drops` is provisional, as
    `kardamom_getTransactionStatus` says today. So a `receipt` after a `dropped` is sent.
  - A `rejected` record is final. The sequencer does not order the transaction. It sets `state`
    to the terminal value, above `included`, so no later event is sent: a late `sent` after a
    `rejected` is dropped.
- **Loss.** Every stage is best effort (decision 5). MDC offers can fail, for example with
  `NOT_CONNECTED` during a restart, and a publisher drops a batch that it cannot offer (4.8).
  - A lost `accepted`, `sent`, `dropped`, `executed`, `posted` or `included` is not sent again.
  - A lost `receipt` has a catch-up that is best effort and capped (4.5). A receipt that the
    catch-up misses is not pushed. The client can still read it from the egress.
  - The notifier counts each gap that it sees: a jump in the `seq` of a hot frame, and a new or
    a lost image of a publisher (`kardamom_notifier_event_gaps_total{origin}`).
- **Rollback.** A rollback frame of `egress_receipts` revokes every `executed` and `receipt` at or
  above its block. The notifier sends a `rolled_back` event to each subscriber of an affected
  hash. A client must treat `executed` and `receipt` as revocable until `posted`.

### 4.4 The notifier

- **Its inputs.**
  - `tx_events`: `accepted`, `sent`, `rejected`, `posted` and `included`.
  - `egress_receipts`: `receipt`, and the rollback frames. The tap exists today.
  - `tx_drops`: `dropped`. The egress already reads this stream (`crates/egress/src/main.rs:179`).
  - `tx_receipts` of every executor: `executed`, only with `KARDAMOM_NOTIFIER_EXECUTED=on` (Q7).
  - The taps on `tx_status` and `tx_errors` go in step 8 of section 7.
- **The stage table.** Each instance has one fixed table. It allocates the table at the start,
  with the size of the ring (250,000 entries on the notifier nodes). The table holds no `Vec`,
  no `String` and no box. The event path allocates nothing for a hash that no client follows.
  - An entry is `{ hash: [u8; 32], state: u8, observed: u8, executors: u8, block: u64,
    accepted_ms: u32 }`, about 56 bytes. So the table uses about 14 MB.
  - `state` is the highest stage that the notifier sent (4.3). One comparison decides a copy and
    a late event. Its values:

    | `state` | Meaning |
    |---|---|
    | 0 | no event sent yet |
    | 1 | `accepted` |
    | 2 | `sent` |
    | 3 | `executed` |
    | 4 | `receipt` |
    | 5 | `posted` |
    | 6 | `included` |
    | 7 | `rejected`, terminal |

  - `observed` is a bitmap, one bit for each stage that really came, for the latencies and for
    the copy of `dropped`. It keeps the bits of `TxStageKind::bit`
    (`crates/types/src/tx_status.rs:112-120`), and uses the free bits for the new stages:

    | Bit | Stage |
    |---|---|
    | 1 | `sent` (the bit of `Offered`) |
    | 2 | `receipt` (the bit of `Sealed`, which goes) |
    | 4 | `executed` |
    | 8 | `rejected` |
    | 16 | `accepted` |
    | 32 | `posted` |
    | 64 | `included` |
    | 128 | `dropped` |

    `rolled_back` is an event, not a stage, so it has no bit.
  - `executors` has one bit for each executor index. So the notifier sees which executors
    reported.
  - `block` comes from the first `executed` or `receipt`. It maps a `Posted` range to its
    hashes. So no separate map from hash to block is necessary.
  - `accepted_ms` is the time of the first observed event of the hash, in milliseconds from the
    start of the instance. The latencies of section 6 count from it.
  - The table uses open addressing on the first 8 bytes of the hash, with a bounded probe.
  - A full probe overwrites its oldest entry, the one with the smallest `accepted_ms`.
  - A hash older than the table gets `posted` only through a status query (4.5).
  - The decode reads the hot frames and the archived rkyv receipts in place. It does not copy
    each event.
  - A rollback changes only an entry at or above `from_block` whose `state` is `executed` or
    `receipt`. It sets `state` back to `sent` (or to `accepted` if `sent` is not in `observed`),
    clears the `executed` and `receipt` bits and `executors`, and sets `block` to 0. Then the new
    receipt after the rollback is sent again.
  - An entry at `posted` or above does not change. The notifier counts it
    (`kardamom_notifier_rollback_past_posted_total`): a rollback past a posted block needs an
    operator.
  - One thread owns the table: the ingest thread of the notifier. No lock is necessary.
- **The receipt feed.** `kardamom_subscribeReceipts` moves from the ingress to the notifier,
  next to its status feed, with the same frames. Its users (3.1) point to the notifier.
- **The load.**
  - The measure on staging is about 0.2 core for each 1,000 tx/s, today, with every instance
    reading every event.
  - About 5 events come in for each transaction before the copies are dropped: 1 accepted, 2
    sent and 2 receipts. The hot records cost almost nothing to read; the receipts cost most.
  - The executor tap adds 3 full receipts for each transaction. So 10,000 tx/s needs about 2
    cores of decode without the tap, and about twice that with it.
  - The shards of P5 (section 9) split that work. Q4 holds the step after them.
- **The shards (P5).**
  - Each instance reads every batch.
  - Each instance keeps only the hashes of its shard in its table. The shard is a rendezvous hash
    of the transaction hash over the instance count.
  - A WebSocket client subscribes on the instance of its shard.
  - An instance that gets a subscription of another shard forwards it to that instance.
  - The decode of each batch stays on every instance.
- **No state to move.** With the webhooks removed (decision 2), the notifier holds no state on
  disk. Its ring and its stage table are memory only. So more instances are a change of
  `KARDAMOM_NOTIFIER_COUNT` and of the notifier nodes only, with no copy of a state directory. A
  client reconnects when an instance moves.

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
- **The catch-up of the notifier.** It is best effort and capped (decision 5). A loss is
  acceptable.
  - A gap is a new or a lost image of an egress publisher on `egress_receipts`, or a jump in the
    block of its receipts.
  - After a gap, the notifier reads `/v1/receipts` from the position of the newest receipt that
    it took. Each receipt carries its position (`BPosition`).
  - The cap: one request each second for each notifier instance, and at most 4,096 receipts or
    1 MiB in each answer. A larger gap takes more seconds, or stays a gap.
  - On a `410`, the gap is older than the index. The notifier then asks
    `eth_getTransactionReceipt` for each followed hash of its table that has no `receipt` bit,
    with at most 64 reads in flight. A hash that no client follows gets nothing.
  - It counts both cases (`kardamom_notifier_catchup_receipts_total{source}`).
  - **The cost on the egress owner thread.** One request is one `Input` to the owner, and one
    copy of at most 4,096 entries of the receipt index. That is less than 1 ms, once each second
    for each notifier instance. So it adds less than 0.1 % to the owner thread, and nothing while
    the stream has no gap.
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
- **The halt status from `chain_status`.**
  - Each sequencer publishes each status frame of its cluster session as `ChainStatus` on
    `chain_status`. The status frame does not reach the egress: `exec_frames` carries only the
    kinds 1 and 2 (`crates/engine/src/reader/tee.rs:62-63`).
  - Both twins publish, so each status comes twice. The ingress keeps the status with the higher
    `sealed_head`. At an equal `sealed_head`, it keeps the one that came last.
  - The rate is one frame each 2 s for each twin. The ingress does not read `tx_events`.
  - **The ingress halts only on an explicit halt in a status.** `record_lag` while a status says
    that the record-lag guard refuses, `da_lag` while a status says that the DA-lag guard
    refuses (3.2).
  - **An explicit halt has a lifetime of 30 s** (decision 7). Each status of the same halt
    renews it. The sealer repeats its state about every 2 s, so one lost status changes
    nothing. When no status comes for 30 s (both sequencers down, or the `chain_status`
    publication stalls), the halt ends and the ingress admits again. The bound is the bound of
    the age alert of section 6, so the operator sees the same moment. While the chain still
    refuses, the sealer guards refuse the references, and the client gets `dropped`.
  - **A silence of `chain_status` does not halt.** This replaces the silence rule of `chain.rs`
    (`check_silence`, with `SEALER_SILENCE` of 10 s and `SNAPSHOT_SILENCE` of 60 s, raising
    `sealer_no_quorum`). `chain_status` is best effort (decision 5), so its silence is not proof
    of a lost quorum, and a best-effort stream must not stop admission.
  - **Why this is safe.** Without a quorum, the chain already refuses:
    - The sealer commits nothing, so no transaction is ordered or executed.
    - The sequencer's offer to the cluster fails with back-pressure, also while its cluster
      session reconnects (`crates/sequencer/src/sequencer.rs:1147`). It keeps the references,
      notes a publish stall for its resync, and its pending pool is bounded.
    - An offer that a leader without a quorum takes gets no receipt. Its reference passes the
      confirm timeout of 15 s (`crates/sequencer/src/resync/mod.rs:162`), and the sequencer
      offers it again. The window of the sealer drops a copy that did commit.
    - So the client sees `accepted`, then `dropped` (`txpool_full` or `expired`, on `tx_drops`),
      or the `receipt` after the quorum comes back.
    - The record-lag and DA-lag guards are in the sealer too (`onRefRefused` in
      `SealerClusteredService.java`). The ingress halt is only an early refusal: it saves a
      client a later drop.
  - The one cost: while the quorum is lost and the statuses do not come, the ingress keeps its
    `validated` ack for transactions that then drop. The alert on
    `kardamom_ingress_chain_status_age_seconds` (section 6) tells the operator.
  - The rule of `chain.rs` on the executors (every live executor halted pauses the submits) does
    not change.

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
- **On staging (2026-10-25).** P1 (kardamom-infra #81) and P2 (#820) are merged and deployed. The
  egress runs on `egress-0` and `egress-1`.
- **The lesson of #831.** The deploy that turned the switch on was also a new release. A change of
  the image together with the constraint makes Nomad place both instances again in one rolling
  update, so step 3 never happens. The second new instance filled from the first, and both
  stores started at the sealer window. The batcher then lacked blocks 298870 to 298903, and the
  user accepted a DA gap on staging. So:
  - Turn the switch on in a deploy that changes nothing else of the egress job. Then the change is
    an in-place update, and the move of step 3 runs one instance at a time.
  - #831 holds the fixes: a new instance fills from the posted head of the batcher, and the deploy
    refuses an image change and a constraint change of the egress in one update.

### 4.8 The hot path budget

Decisions 5 and 6 bind every publisher of `tx_events` and `chain_status`.

**The publish path of `accepted` and `sent`.**

- Each publisher keeps one buffer for each kind, allocated at its start: a 16-byte header and
  room for 1,024 records.
- For each event it copies the 32-byte hash (and the lane for `sent`) into the next record. No
  allocation, no encode, no hash computation: the hash is already there.
- It offers the frame once for each poll cycle, or at once when the buffer is full. The offer is
  non-blocking: `offer` on the publication, not `try_claim`. A full frame (1,024 records) is
    about 34 KB, above one MTU payload (`-Daeron.mtu.length=1344` in the deploy,
    `crates/chaos/src/cases/fleet/seeding/tests.rs:62`), and `try_claim` takes at most one MTU
    payload. `offer` fragments the frame, and the notifier reads it through a fragment
    assembler. The frame stays below the maximum message of the term (a term of 4 MiB gives
    512 KiB).
- On back-pressure or `NOT_CONNECTED`, it drops the frame and counts it
  (`kardamom_tx_events_dropped_total{origin}`). It never waits, and never retries.
- It then clears the count and moves `seq` on, so a drop shows as a gap to the notifier.

**Where the append and the offer go.**

- **The ingress.** `submit_raw` (`crates/ingress/src/proxy/submit.rs:53`) publishes each
  transaction from its own request task, in `offer` (`submit.rs:441`), through `publish_tx_data`
  (`:451`, `LiveIngressPublication::publish_tx_data` in `crates/ingress/src/aeron_adapters.rs:75`).
  The ingress has no poll cycle. So:
  - After `publish_tx_data` succeeds, the task pushes the hash into a bounded queue with no
    lock, allocated at the start (`try_push`). On a full queue it drops the hash and counts it.
  - One task drains the queue each 1 ms into the frame buffer, and offers the frame.
  - The request task does one copy and one atomic operation for each transaction.
- **The sequencer, v1.** `Sequencer::record_one_published` (`crates/sequencer/src/sequencer.rs:583`)
  publishes `TxStatus::offered` for each reference today. It appends a `sent` record instead.
  `run_once` (`sequencer.rs:840`) offers the frame at the end of the cycle.
- **The sequencer, v2.** `flush` (`crates/sequencer/src/v2/mod.rs:496`) publishes
  `TxStatus::offered` for each candidate today (`:510`). It appends a `sent` record instead.
  `run_once` (`v2/mod.rs:255`) offers the frame at the end of the cycle.
- So for the sequencer, the change takes work away from the hot path: today it encodes and
  offers one `TxStatus` for each transaction (`publish_best_effort`,
  `crates/sequencer/src/outbound.rs:203`).
- **The batcher** publishes once for each batch, off the hot path.

**The taps of the notifier add no work to the publisher threads.** The notifier reads
`egress_receipts` already. Its taps of `tx_drops` and of `tx_receipts` add one subscriber to
those publications. The media driver of the publisher then sends each frame to one more
destination. That costs bandwidth on the driver thread, not time on the service thread. For
`tx_receipts` it is three full receipts for each transaction, so that tap stays behind its
switch (Q7), and the A/B ramp measures it.

**The proof.**

- **Allocation tests.** The repository profiles the hot paths with DHAT:
  `crates/sequencer/tests/alloc_profile.rs` (the `run_once` loop) and
  `crates/bench/tests/alloc_profile_ingress.rs` (`submit_raw`). Each gets a case with
  `KARDAMOM_TX_EVENTS` on. It asserts zero extra allocations for each transaction against the
  case with the switch off.
- **A micro-benchmark** of the append and the frame offer, next to
  `crates/sequencer/benches/throughput.rs` and `v2_loop.rs`: the cost for each event, in ns.
- **A staging A/B ramp.** The same ramp with `KARDAMOM_TX_EVENTS` off, then on. The budget:
  - below 1 % more CPU on the ingress and on the sequencer at each step;
  - no change in the p99 of the stage latencies (the ingress execute latency, and the load tool
    p99), within the noise of two runs with the switch off.
- **The failure test.** An e2e case: the notifier is down, or its subscriptions are gone. The
  chain keeps its rate. Only `kardamom_tx_events_dropped_total` moves: a publication with no
  subscriber answers `NOT_CONNECTED`. The same case stops the sequencers' `chain_status`, and
  the ingress keeps its admission.

## 5. What this removes

- **The ingress:**
  - the receipt cache, the receipt query and the fallback of #796
    (`crates/ingress/src/receipt_query.rs`);
  - `kardamom_subscribeReceipts`;
  - `--ack receipt` and the ack policies (4.6), with `PendingReceipts`;
  - the cluster tap and its thread, the `Sealed` publish on `tx_status`, and the record-lag
    gauges and the `kardamom_ingress_cluster_*` gauges of the tap;
  - their tests and flags.
- **The ingress halt on silence:** the silence rule of `chain.rs` (`check_silence`, the use of
  `SEALER_SILENCE` and `SNAPSHOT_SILENCE` by the ingress) and its `sealer_no_quorum` halt (4.6).
  `chain.rs:172` is the only place that raises `HaltCause::SealerNoQuorum`. So these users of
  the halt change in P6, with the switch `KARDAMOM_INGRESS_CHAIN_STATUS=events`, not later:

  | User of the halt | Path in main | The replacement |
  |---|---|---|
  | The chaos recovery stage, which waits for the chain view to leave `sealer_no_quorum` | `crates/chaos/src/stages/recovery.rs:1-8` | It waits until `kardamom_ingress_chain_status_age_seconds` is below 10 s on each ingress, or for the first `accepted` after the restore. |
  | The chaos cases that assert the refusal "the sealer is halted on sealer_no_quorum" | `crates/chaos/src/cases/combined.rs` (2 places), `crates/chaos/src/cases/chain_status.rs` (3 places) | They assert the new effect: the ingress admits, the sequencer back-pressures, the transaction drops (`dropped` on `tx_drops`), or its receipt comes after the restore. |
  | The proofs `ingress-sealer-loss-recover` and the two combined losses | `docs/failure-modes.md:68`, `:115`, `:160`, `:755`, `:773` | The effect lines become "the ingress admits, the sequencer back-pressures, the transaction drops". Line 115 keeps the sequencer's own pause of 10 s with no boundary, which does not change. |
  | The alert `KardamomHaltSealerNoQuorum` | `deploy/alerts.yml:237` | It goes. The age alert on `kardamom_ingress_chain_status_age_seconds` above 30 s (section 6) takes its place. |
  | The gate excuse for a `sealer_no_quorum` halt | `deploy/cluster/ansible/roles/workloads/tasks/gate.yml` (3 places, about `:145-158`) | It goes: the halt no longer comes. |
  | The runbooks | `docs/runbooks/sealer_no_quorum.md`, `sealer-fleet-rebuild.md`, `deploy-rollback.md` | `sealer_no_quorum.md` becomes the runbook of the age alert. The two others name the age alert. |
  | The chaos suite doc | `docs/chaos-suite.md` (3 places) | It names the new effect and the new wait of the recovery stage. |
  | The chain status dashboard | `deploy/grafana/provisioning/dashboards-json/kardamom-chain-status.json` | The `sealer_no_quorum` panel shows the status age instead. |
- **The notifier:** its receipt source switch (`--receipts-from-egress`). It reads
  `egress_receipts` always, and `tx_receipts` only behind `KARDAMOM_NOTIFIER_EXECUTED`.
- **The streams:**
  - `tx_status`, after the sequencers publish `sent` and `rejected` records (4.2). With it goes
    the `TxStatus` `Offered` publish of each transaction in the sequencer;
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
  to the first observed event of each later stage, on the clock of the notifier. The stages come
  from several taps: `accepted` and `sent` from `tx_events`, `dropped` from `tx_drops`,
  `executed` from `tx_receipts`, `receipt` from `egress_receipts`, `posted` and `included` from
  `tx_events`. So each latency includes the transport of its stream to the notifier.
- `kardamom_notifier_executed_latency_seconds{executor}`: the time from `accepted` to the receipt
  of each executor on `tx_receipts`. So a slow executor shows. Only with the executor tap on.
- `kardamom_notifier_merge_latency_seconds`: the time from the first `executed` to the first
  `receipt`: the cost of the hop to the egress and its merge. Only with the executor tap on.
- The drops of the publishers (as built in P3, #821 and #822): a frame that an offer refuses counts
  in `kardamom_log_best_effort_dropped_total{stream_id="1013"}`. The offer runs on the Aeron
  thread, so the publisher does not see each result, and the counter has no origin or kind. A
  full queue of the ingress counts in `kardamom_tx_events_queue_full_total{origin}`. The gap count
  of the notifier (`kardamom_notifier_event_gaps_total{origin}`) gives the view for each origin.
- A publisher sends a frame when it is full or 1 ms old, not once for each poll cycle. That caps
  the copy of a frame at 1,000 a second for each kind.
- `kardamom_notifier_event_gaps_total{origin}` and
  `kardamom_notifier_catchup_receipts_total{source}`.
- `kardamom_notifier_rollback_past_posted_total`: a rollback that reached an entry at `posted` or
  above (4.4). An alert fires on any increase.
- `kardamom_ingress_chain_status_age_seconds`: the time since the last `ChainStatus` that the
  ingress took. An alert fires above 30 s. It does not start a halt, and at 30 s an explicit
  halt ends (4.6, decision 7).
- The canary reads its stages from the notifier feed. The performance dashboard takes the
  notifier stage latencies in place of the ingress seal latency.

## 7. The rollout

Every deploy stays backward compatible. A breaking change goes only through an upgrade
transaction and a feature flag. Each step has a switch, off by default.

1. **Measure.** Ramp with the current layout. Record the egress CPU and memory
   (`nomad alloc status -stats`) and the notifier CPU on notifier-0. This sizes 4.4 and 4.7.
2. **The egress nodes.** P1, then P2 with `KARDAMOM_EGRESS_NODES=on`, then the move of 4.7.
3. **The publishers.** `KARDAMOM_TX_EVENTS=on` adds the `tx_events` publications of the ingress,
   the sequencer and the batcher, and `chain_status`. The sequencer keeps its `TxStatus`
   publishes until step 8. The old streams stay.
4. **The A/B ramp of 4.8.** The same ramp with `KARDAMOM_TX_EVENTS` off and on. The step passes
   only inside the budget of 4.8.
5. **The notifier reads both.** `KARDAMOM_NOTIFIER_EVENTS=on`: the notifier takes `tx_events` and
   its taps. It ignores the old streams for the stages that the new inputs carry. Compare the two
   on the canary for a day. `KARDAMOM_NOTIFIER_EXECUTED=on` adds the executor tap for the
   evaluation of Q7.
6. **The ingress halt from `chain_status`.** `KARDAMOM_INGRESS_CHAIN_STATUS=events`: the ingress
   takes its explicit halts from `ChainStatus`, with no silence rule. The tap stays, and compares
   its status with the events. Then the tap stops publishing `Sealed`.
7. **The clients.** P6, P7 and P8: the receipt feed on the notifier, the egress reads, the second
   endpoint, the clients of 3.1, the canary and the e2e scenario. The ack default becomes
   `validated` on every network.
8. **The removals** of section 5 (P9). This step is one way: only a deploy of the release before
   undoes it. So it comes last, after a week on staging, after steps 1 to 7.

## 8. Open questions, with the recommended answer

- **Q1. The source of the `ClusterStatus` for the ingress halt.** Answered in revision 2: the
  sequencer, as `ChainStatus` on the stream `chain_status` (4.6). The egress does not see the status frames.
- **Q2. The catch-up of `receipt`.** Answered in revision 4: a new or a lost image, or a jump in
  the block, starts a capped, best-effort read of `GET /v1/receipts` on the egress (4.5).
- **Q3. The catch-up of `posted`.** Recommended: the notifier reads the batcher's posted cursor at
  the start and after a gap, from the L1 `BatchPosted` logs through the L1 indexer.
- **Q4. Notifier scale past one decode for each instance.** Recommended: later, a topic for each
  shard. The publishers then split their batches by the shard of the hash. Not in this change.
- **Q5. The receipt ack and the ack policies go.** Recommended: yes (4.6). Staging and the load
  tests use `--ack validated`, and the push of `receipt` gives a client the same certainty sooner.
- **Q6. `tx_errors`.** Recommended: the `dropped` and `rejected` events cover the client side. The ingress reads
  `tx_errors` only for the receipt ack, so it goes with Q5.
- **Q7. The executor tap after the evaluation.** The user decided that the notifier may read the
  executors (decision 9). So the tap stays, behind `KARDAMOM_NOTIFIER_EXECUTED`, off by default.
  The open part is the default only. Recommended: turn it on for the evaluation, and decide from
  the two latencies of section 6 and from the driver bandwidth that the A/B ramp measures on the
  executor nodes. If the merge latency stays below 1 ms at the peak, keep it off by default. If a
  client needs the earliest signal, turn it on, and send only the first executor copy to the
  clients.
- **Q8. io_uring in the notifier.** It does not replace the stage table: the table is memory
  state, and io_uring is a way to submit I/O. It can serve the output side: the WebSocket sends.
  The webhook posts and the outbox writes go with the webhooks (decision 2). The `tx_events` ingest
  gains nothing, because Aeron uses shared memory. Recommended: not in this change. Profile the
  notifier at the peak of the ramp after P4. If the system calls of the output side are the cost,
  write a separate spec for an io_uring runtime (tokio-uring, monoio or glommio) on those paths.

## 9. The PRs

Each PR merges alone. Each new path is behind a switch that is off by default, until P9.

| PR | Repo | Content | Depends on |
|---|---|---|---|
| P1 | kardamom-infra | the egress nodes. **Done:** kardamom-infra #81, applied on staging | none |
| P2 | kardamom | `KARDAMOM_EGRESS_NODES` and the constraint `role=egress`, the README move steps. **Done:** #820, deployed on staging (see the lesson of #831 in 4.7) | none (off by default); the move needs P1 |
| P3 | kardamom | **Done:** #821 (the `tx_events` frame, its format and stream) and #822 (the `accepted` and `sent` publishers of the ingress and the sequencer, `KARDAMOM_TX_EVENTS`, the allocation cases and the micro-benchmark). Left: the `rejected` record of the sequencer (kind 3), the batcher frame and its `posted`/`included` publisher, the `chain_status` stream, and the e2e case with the notifier down | none |
| P4 | kardamom | **Done (core):** #823 (the stage table: the state and the bitmaps, the latest-state rule, the rollback) and #824 (the events path: `tx_events`, the `egress_receipts` tap with the rollbacks, the `tx_receipts` tap behind `KARDAMOM_NOTIFIER_EXECUTED`, the gaps, the stage metrics, `KARDAMOM_NOTIFIER_EVENTS`). Left: the `dropped` events from the `tx_drops` tap and the `rejected` events of `tx_events` kind 3 (decision 8) | P3 |
| P4a | kardamom | the removal of the notifier webhooks (decision 2): the registry, the outbox, the cursors, the delivery loop, the forwarding of registrations to the peers, the sharding by subscription, `--peers`, the state directory `/opt/kardamom/notifier` and its volume, the move procedure of the webhook state in `deploy/cluster/README.md`, their tests and docs. The full removal list follows the table. No switch: no client uses webhooks, and staging had no webhook state. The rollout checks first that notifier-0 holds no registration (`/opt/kardamom/notifier` is empty). It comes before P7, the client feed work | none |
| P5 | kardamom | the shards of the notifier (4.4). P4a removes `--peers`, so P5 brings back a peer list for the WebSocket forwarding, or redirects the client to the instance that owns its shard | P4 |
| P6 | kardamom | the ingress halt from the explicit halts of `ChainStatus`, with no silence rule and a lifetime of 30 s for an explicit halt (`KARDAMOM_INGRESS_CHAIN_STATUS`), the age gauge and its alert, the users of the `sealer_no_quorum` halt of section 5 (the chaos recovery stage and cases, the proofs, the alert, the gate excuse, the runbooks, `docs/chaos-suite.md`, the chain status dashboard), the e2e scenario on `tx_events`, the dashboard panels | P3. Waits for #819 decisions 48-49 (no custom refusal kinds, no DA-lag guard) |
| P7 | kardamom | after P4a. The receipt feed (`kardamom_subscribeReceipts`) on the notifier; the egress short wait, the executor fallback and `/v1/receipts`; the capped catch-up of the notifier; the receipt endpoints of the clients: the canary, the chaos suite, the e2e harness and the load tool | P4, P4a |
| P8 | kardamom-infra | the second endpoint for the egress reads, and the infra variables of the new switches | P7 |
| P9 | kardamom | the removals of section 5, the ack default `validated`, `docs/specs/egress-node-spec.md` marked superseded, the docs | P6, P7, P8 on staging for a week |

The removal list of P4a, in full:

- the webhook metrics and panels in `deploy/grafana/provisioning/dashboards-json/kardamom-notifier.json:82-121`;
- `paths.notifier_dir` in `group_vars/all.yml:164` and `:186`, and the directory of the common role;
- `--dir` and `KARDAMOM_NOTIFIER_DIR`;
- the webhook sections of `docs/tx-status-events.md:214`, `docs/json-rpc.md:307`, `docs/failure-modes.md:1390` and `crates/types/src/tx_status.rs:9`;
- the state paragraphs of `docs/specs/2026-10-24-elastic-ingress.md:535` and `:589-606`, marked superseded;
- the off-identity fixture of the deploy tests: the notifier args change with no switch;
- in kardamom-infra: the state notes in `docs/staging-launch.md`, the `hosts.ini` comment, and the `keep_disk` reason of the notifier nodes.

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
| Revision 2 review, note 1. `TxStatus` `Accepted` and `Executed` | 4.2: a second table maps them. |
| Revision 2 review, note 2. The ingress decodes `tx_events` for `ChainStatus` | 4.1, 4.2, 4.6, P3: `ChainStatus` goes on its own stream `chain_status`, with the sequencers as its only publishers. |

### 10.1 Changes from revision 3

The user's rules of 2026-10-25: "the notifier publication is best effort. It shouldn't ever halt
the chain", and "we should ensure this has a negligible impact on the hot path".

| Change | Where |
|---|---|
| The two rules are decisions 5 and 6. | 1 |
| The ingress halts only on an explicit halt of a `ChainStatus` (record lag, DA lag). A silence of `chain_status` does not halt. This replaces the silence rule of `chain.rs` (`check_silence`, `SEALER_SILENCE`, `SNAPSHOT_SILENCE`). The safety: without a quorum the sealer commits nothing, and the sequencer back-pressures and drops. | 4.6, 5, 6 (the age alert), 7 step 6, P6 |
| No new work on the hot services for the stages that existing streams carry: the notifier taps `egress_receipts` (with the rollback frames) for `receipt`, `tx_receipts` for `executed` (behind `KARDAMOM_NOTIFIER_EXECUTED`), and `tx_drops` for `dropped`. The executors and the egress publish nothing new. `tx_events` carries only `accepted`, `sent`, `rejected`, `posted` and `included`. | 1, 4.1, 4.2, 4.3, 4.4, 6, 7, Q7, P3, P4 |
| The receipt catch-up is best effort and capped: one request each second for each notifier instance, at most 4,096 receipts or 1 MiB, and 64 lookups in flight after a `410`. A loss is acceptable. The cost on the egress owner thread is less than 1 ms each second. | 4.3, 4.5, Q2, P7 |
| A hot path budget: a fixed-layout frame, an append with no allocation and no encode, one non-blocking offer for each cycle, a drop on back-pressure. The places in the ingress and the sequencer. The proof: the DHAT allocation cases, a micro-benchmark, a staging A/B ramp (below 1 % CPU, no p99 change), and an e2e case with the notifier down. | 4.8, 7 step 4, P3 |
| The sequencer `sent` record replaces its `TxStatus` `Offered` publish of each transaction, so the sequencer hot path does less work than today. | 4.2, 4.8, 5 |
| A fix of revision 3: the `TxStatus` kind `Accepted` comes from the v2 sequencer verdict on `tx_verdicts`, not from the ingress, and `Executed` is derived by the notifier from the receipts. | 4.2 |
| Revision 5. The user: send the latest state, not the missed stages | 4.3, 4.4, P4: the latest-state rule replaces the implied stages, and `state: u8` replaces the `emitted` bitmap. |

### 10.2 Changes from revisions 4 and 5

| Change | Where |
|---|---|
| Revision 5, the user: the notifier sends only the latest state of a hash, and drops an earlier event that comes too late. `state: u8` replaces the `emitted` bitmap. | 4.3, 4.4, P4 |
| Review of revision 4, finding 1 (major): the users of the `sealer_no_quorum` halt break at step 6, not at P9. Section 5 lists each one with its path and its replacement, and P6 carries them. | 5, P6 |
| Review of revision 4, finding 2 (major): a stale explicit halt had no end. It now ends when its newest status is older than 30 s, and the ingress admits again. | 1 (decision 7), 4.6, P6 |
| Review of revision 5, finding 3: `dropped` sets its bit in `observed`, is sent once, and leaves `state` unchanged. `rejected` sets `state` to a terminal value above `included`. The `state` values are listed. | 4.3, 4.4 |
| Review of revision 5, finding 4: a rollback resets only an entry at `executed` or `receipt`. An entry at `posted` or above is counted (`kardamom_notifier_rollback_past_posted_total`) and kept. | 4.4 |
| Review of revision 5, finding 5: the `observed` bits keep `TxStageKind::bit` and take the free bits for `receipt`, `posted`, `included` and `dropped`. `rolled_back` has no bit. | 4.4 |
| Review of revision 4, finding 3 (minor): a frame of about 34 KB does not fit `try_claim`. The publishers use `offer` with fragmentation, and the notifier uses a fragment assembler. | 4.8 |

### 10.3 Changes for revision 7

| Change | Sections |
|---|---|
| The user, 2026-10-25: the subscriptions are the notifier, and the egress serves every other read endpoint (#819 decision 46 owns the rest of the reads). | 1 (decision 2) |
| The user: the webhooks are removed for now, and offline delivery comes later through a task queue such as SQS. The notifier becomes stateless. A new PR P4a removes them, and more notifier instances are then a count change only. | 1 (decision 2), 4.4, Q8, 9 (P4a, P7) |
| The user: drops and refusals are pushed as events too, and the egress status API serves them. `rejected` gets its own row. | 1 (decisions 3 and 8), P3, P4 |
| The user: the notifier may read the executors. The `executed` tap is decision 9, an exception to #819 decision 45. Q7 keeps only its default open. | 1 (decision 9), Q7 |
| The egress runs on its own nodes on staging, and the lesson of #831: an image change and a constraint change in one deploy place both instances again. | 4.7 |
| The metric deviation of P3: the drop counters as built, and a frame flush when full or 1 ms old. | 6 |
| P1, P2, P3 (#821, #822) and the core of P4 (#823, #824) are done. The rest of P3 and P4 and the scope of P7 are listed. P6 waits for #819 decisions 48-49. | 9 |

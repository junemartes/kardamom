# Hot-path agents: the transaction path off tokio

- Status: revision 3, approved by the design review on 2026-10-23; waiting for the decision of the
  user. Not implemented.
- The user asked on 2026-10-23 to move the hot path off tokio.
- Related: #774 (the timer tick of the batch verifier, and the split of a batch over the blocking
  pool) and #776 (the async publish ack), both open;
  #773 (the egress retry queue); #768 (the `dropped` status); the DEDICATED-thread experiment of
  the media driver; `docs/specs/2026-10-21-one-way-pipeline.md`.
- Revision 2 answers the review of revision 1 (findings F1 to F10), and revision 3 the review of
  revision 2 (F11 to F14). Section 11 maps each finding to its change.

## 1. The problem, measured

### 1.1 The spike of 2026-10-23

Staging, 2 x 500 tx/s for 10 minutes, `--ack validated`, receipts confirmed through the WebSocket
feed. The client saw a receipt latency of p50 3–4 ms, p99 7–11 ms and a max of 171–175 ms.

The step metrics over 13:35–13:43Z (means unless named):

| Segment | Value | Source |
|---|---|---|
| The submit in the ingress | 1.84 ms | `kardamom_step_seconds{step="ingress.submit"}` |
| — the recovery, with its wait | 1.47 ms | `step="ingress.recover"` |
| — the recovery of one batch (about 5 signatures) | 0.54 ms | `step="ingress.recover_batch"` |
| — the publish, to the ack of the Aeron thread | 0.35 ms | `step="ingress.publish_round_trip"` |
| The publish to the sealed index, p50 / p99 | 1.88 / 6.82 ms | `kardamom_ingress_seal_latency_seconds` |
| The publish to the first receipt, p50 / p99 | 1.90 / 6.42 ms | `kardamom_ingress_execute_latency_seconds` |
| The sequencer offer, to its ack | 0.11 ms | `step="sequencer.offer_round_trip"` |
| One snapshot of a sealer member | 32 ms | `step="sealer.snapshot"` |

### 1.2 What the numbers say

- The ingress was about half of the median. About 1 ms of `ingress.recover` was the tokio timer:
  the batch window of 50 µs waited on `tokio::time::sleep_until`, and the timer has a resolution
  of 1 ms. #774 removes that wait.
- The recovery of one batch (0.54 ms) is mostly wakes, not work: a batch of about 5 was split over
  4 blocking workers on a 4-vCPU node, for about 0.21 ms of recovery. #774 also splits a batch only
  from 16 transactions, and uses half of the cores at most. The shorter window of #774 makes most
  batches hold one transaction, so the split is rare at these rates anyway.
- The publish paid one hop to the blocking pool (`spawn_blocking` around a blocking publish,
  `crates/ingress/src/aeron_adapters.rs`). #776 removes it with an async ack.
- After #774 and #776 the ingress should cost about 0.3 ms. The rest of the median, about
  1.9 ms, runs from the publish to the receipt: the sequencer, the Raft commit of the sealers
  across three data centers, the executor, the egress and the receipt pump.
- The p99 and the max do not come from the ingress. The sealer snapshot (32 ms each) is one known
  source. The 171 ms max is not explained yet.

### 1.3 Consequence for this spec

The first stage must come from a measured breakdown of the 1.9 ms, not from a guess. So stage 0
(section 4) measures each hop after #774 and #776 deploy. The agent model of section 3 stays the
target shape. Section 5 describes the ingress agent, because the ingress is the stage with the
most scheduler hops today. But it ships only if stage 0 shows a remaining scheduler cost there.

## 2. Goals and limits

- Goal: remove the scheduler from the per-transaction path. A transaction crosses each stage with
  no timer, no blocking-pool hop and no task wake, except one wake to answer the client.
- Goal: a bounded, measured cost per stage, with the step metrics.
- Goal: 10k tx/s per ingress pair without a rise in p99.
- Limit: tokio stays at the edges: HTTP, the JSON-RPC parse, WebSocket, the admin endpoints and
  every call to Redis or to another HTTP service.
- Limit: no change to a wire format, to a decision rule, to a replicated state, or to the answer
  semantics of the validated ack (one-way spec, section 3.1: `txpool is full` only when the offer
  fails within its deadline).
- Limit: the CPU work of the recovery does not run on a tokio worker. `recovery_does_not_block_the_reactor`
  (`crates/ingress/tests/sig_verify_batch.rs`) enforces this. An agent keeps the rule and drops the
  pool hop.
- Non-goal: kernel bypass (DPDK, `ef_vi`, AF_XDP). The cloud nodes cannot use it.
- Non-goal: the Java sealer. It already runs on Agrona agents.

## 3. The model

### 3.1 Agents

An agent is one thread that owns one stage. Its loop polls its inputs with no wait, does the work,
and offers its outputs. With no work, it runs its idle strategy.

- The agent owns its state. Section 5.6 names each structure that it shares, and why.
- The agent never calls `.await`, never sleeps on a timer, and makes no system call in a busy pass.
  An Aeron offer and an Aeron poll are not system calls.
- The idle strategy is short on a shared vCPU: a bounded spin, a few `std::thread::yield_now`,
  then a park. A producer that pushes into the ring of a parked agent unparks it (3.2), so a quiet
  agent costs no periodic wake. The park has a cap: `AeronRuntime::idle_cap()`, which the deploy
  sets from `KARDAMOM_IDLE_CAP_US` (50 µs on staging). No new setting.
- The agent records its loop with the `LoopMeter` names of `kardamom_obs::pipeline`, so the
  Bottlenecks dashboard shows it.

### 3.2 Rings and the wake handshake

- The edge has many producers (the handler tasks), so the ring of an agent at the edge is a
  `crossbeam_queue::ArrayQueue` (MPMC, lock-free, already a dependency). A ring between two agents
  is SPSC; its crate is chosen when the first such pair exists (stage 2 or later).
- The wake handshake follows Agrona: the agent sets an `AtomicBool` "parked" before it parks, and
  checks the ring once more after it sets the flag. A producer pushes, then reads the flag, and
  calls `Thread::unpark` only when it is set. So a busy agent sees no unpark, and no push is lost
  between the last poll and the park. A `loom` test proves the handshake (section 8).
- A full ring is back pressure. The handler answers at once, with a reason of its own:
  `kardamom_ingress_tx_rejected_total{reason="ring-full"}`, apart from `txpool-full` (a refused
  offer at its deadline), so the dashboard tells the two apart.

### 3.3 The edge

- A handler task does the parse and the cheap checks, then pushes the work item and a
  `tokio::sync::oneshot::Sender` into the ring, and awaits the receiver. The oneshot is used as
  it is, with no wrapper type.
- The agent sends the answer on the oneshot. One send is one atomic operation and one wake.
- A dropped answer (`RecvError`, the agent is gone) maps to `IngressError::Internal`.

### 3.4 Threads and cores

- Each agent has a name and an optional core list in its configuration. Without a core list, the
  agent does not pin. No agent pins on the cloud nodes (Q5).
- A busy agent costs a full core while it is busy. Section 7 gives the budget.

### 3.5 Where the code lives

No new crate yet. The first agent lives in the ingress crate. The loop shape already exists twice
(the Aeron thread with `IdleBackoff` and `ThreadMeter`, and the sequencer loops). When a second
agent exists, a later PR extracts the shared shape. The agent is a struct with a `poll(&mut self)
-> usize` method, so a test drives it with no thread (section 8).

## 4. Stage 0: measure each hop

Before any agent ships, staging gets the breakdown of the 1.9 ms from the publish to the receipt.

1. Deploy #774 (the timer and the split of a batch), #776 (the async publish) and #773 (the egress
   retry queue). Run the spike again (section 9). This is the fair baseline for any agent.
2. Read the existing hop metrics for each segment: `sequencer.offer_round_trip`, the sealer
   `sealer.message` and `sealer.egress_offer`, the executor `executor.boundary` and
   `executor.receipt_publish`, the egress loop metrics, and the seal and execute latency of the
   ingress.
3. Where a segment has no metric, add one step metric in a small PR. The candidates: the sequencer,
   from the poll of `tx_data` to the offer; the executor, from the cluster frame to the receipt
   publish; the egress, from a receipt frame to its `egress_receipts` offer; the ingress receipt
   pump, from `egress_receipts` to the WebSocket send.
4. Record the result in this spec (a table like 1.1), and choose the first agent stage from it:
   the stage with the largest scheduler cost per transaction. A hop that waits on the network or on
   Raft is not a scheduler cost and is out of scope.

## 5. The ingress agent (a candidate stage)

### 5.1 Scope

- Only `--ack validated`. The receipt ack and the admission ack register a waiter before the
  publish (`crates/ingress/src/proxy/submit.rs`) and stay on tokio. The flag is refused with
  another ack mode.
- The flag: `kardamom-ingress --verify-agent on|off`, default `off`, set by a job variable.

### 5.2 The path

```text
JSON-RPC handler (tokio)
   hash → duplicate cache (a hit answers the hash) → decode → push (envelope, raw, oneshot)
   ▼
ring (ArrayQueue) ──▶ verify agent (one thread)
                        protocol limits → recovery → nonce view
                        → offer on tx_data[lane] (its own clone of the publication)
                        → answer: hash, or the error
```

- One verify agent. One core recovers about 23k signatures/s (`crates/ingress/tests/stage_costs.rs`),
  so one agent covers 10k tx/s on one ingress. One agent keeps one publisher session on each lane
  and the order of arrival, as today.
- If one agent is ever too slow: split the recovery over workers, and keep one publisher that
  offers in arrival order (a sequence barrier, the Disruptor shape). Never round robin over
  publishers (5.4).

### 5.3 The duplicate cache

- The hash and the duplicate check stay in the handler, before the decode and the ring, as today
  (`crates/ingress/src/proxy/validated.rs`). A duplicate never enters the ring and costs no decode
  and no recovery.
- The agent inserts a hash into the cache after its offer succeeds, as today: only a published hash
  enters the cache.

### 5.4 The publish and its deadline

- The agent offers on its own clone of the concurrent publication of each lane. rusteron marks
  `AeronPublication` `Clone + Send + Sync`, and an Aeron concurrent publication takes offers from
  many threads. No exclusive publication and no `try_claim`: rusteron does not mark
  `AeronExclusivePublication` `Send`, and this spec adds no `unsafe`.
- A refused offer does not answer at once. The agent keeps the item in a pending slot for its lane
  with the deadline of today (`OFFER_TIMEOUT`, 5 s). Each pass retries the head of each lane once,
  with no block, and keeps the order of the lane. At the deadline the answer is `txpool is full`.
  This is the rule of `drain_pending` (`crates/log/src/aeron_live/pending.rs`). The agent reuses
  that rule, not a copy of it: a log-crate PR first lifts the pure core (`drain_pending_inner`: the
  queue, the deadline rule and the block per publication) into a `pub` type that is generic over
  the offer and the ack. The Aeron thread and the agent then both use it.
- Why not round robin over N publishers: the sequencer parks at most 16 transactions per sender
  (`crates/sequencer/src/v2/mod.rs`, `max_parked`) and answers `TooManyPending` above it. Two
  images of one ingress can deliver one sender's nonces in bursts of up to 64 fragments per poll,
  so a hot sender would get drops. More sessions would also mean more replay windows in the sealer
  and more recordings for the join. One publisher avoids all three.
- The stamp for the seal clock (`stamps.stamp`) moves into the agent, before the offer.

### 5.5 The nonce view

- Today the check reads the local account layer (`LiveAccounts`), then Redis on a miss, with an
  `await` on the path.
- The agent reads only the local layer. A sender with no row admits, as a sender with no row in
  Redis does today.
- On a miss, the agent pushes the sender into a bounded channel to the receipt pump, the one writer
  of `LiveAccounts` (`crates/cache/src/live.rs`). The pump never awaits Redis: for each miss it
  spawns a task that reads Redis, and that task sends the row back on the channel of the pump. The
  pump applies the row with the monotone rule (`apply_one`), between two frames. The next
  transaction of that sender then finds the row. No second writer exists, and no receipt waits
  for Redis.
- Effect: the first transaction of a sender with no local row skips the Redis check. This happens
  after a restart (the layer is empty) and after an entry expires (the layer has a TTL and a
  capacity). A stale nonce then passes the ingress, and the sequencer drops it with `nonce too low`.
  The `dropped` status of #768 reports each one, so #768 is a dependency of this stage. The rule
  "a `nonce too low` answer is always true" stays true.

### 5.6 Shared structures that the agent touches

| Structure | Access | Why it stays shared |
|---|---|---|
| The duplicate cache (`RecentMap`: `DashMap` + `ArrayQueue`) | insert after an offer | the handlers read it before the ring (5.3) |
| `LiveAccounts` (`DashMap`) | read | the receipt pump is its one writer (5.5) |
| The cluster status (`watch`) | read | the guards of the sealer come from the cluster session |
| The pause slot | read | an operator pause reaches every path |

Each is a read-mostly structure with sharded or lock-free access. None is a mutex on the path.

### 5.7 Shutdown

1. The intake stops (`begin_drain`, `crates/ingress/src/proxy/mod.rs`): a new submit answers as it
   does today in a drain.
2. The agent empties its ring and its pending slots. Each item gets an answer: the hash, or the
   error at its deadline.
3. The agent stops, and its thread is joined.
4. The publications close, then the Aeron runtime drops, in the drop order of today.

### 5.8 Failure

- A panic in the agent ends the process (the runner does not catch it), so Nomad restarts the
  ingress, as for a panic of the Aeron thread today.
- A dropped answer maps to `IngressError::Internal` (3.3).
- A full ring answers `ring-full` (3.2).

### 5.9 Metrics

- The step `ingress.recover` keeps its name, recorded inside the agent.
- New: the ring depth gauge (`kardamom_queue_depth{queue="ingress.verify_ring"}`), the loop meter
  of the agent, and the counts of `ring-full` and of refused offers.

## 6. Later stages

Each later stage gets its own section and its own PR, in the order that stage 0 gives.

| Stage | Agent | Input | Output |
|---|---|---|---|
| Sequencer admission | one agent for each lane | `tx_data` images | the cluster offer |
| Executor reader | one agent | the cluster egress or the member feed | the execution ring |
| Executor execution | one agent | the execution ring | the commit ring, `exec_frames`, `tx_receipts` |
| Egress merge | one agent | `exec_frames`, `tx_receipts` | the store, `egress_order`, `egress_receipts` |
| Receipt pump (ingress, notifier) | one agent | `egress_receipts` | the WebSocket fan-out (tokio) |

- The state commit stays on its own thread. Commits run at about 0.6/s and hold many blocks, so
  they are not on the latency path.
- The sequencer stage must keep the twin rule: both twins offer the same bytes in the same order.

## 7. The core budget on staging

| Node | vCPUs | Today | With the ingress agent |
|---|---|---|---|
| ingress-0/1 | 4 (cloud, shared) | 4 tokio workers (`main.rs` sets 4), the blocking pool, the Aeron thread, the Aeron client conductor, the `cluster-watermark` thread | the same, plus one verify agent; the blocking pool then runs no recovery |
| sequencer-0/1 | 4 (cloud, shared) | 4 sequencer loops (`spawn_blocking`), 2 tokio workers, the Aeron thread | unchanged |
| executor-0..2 | 4–6 (cpuset on a dedicated host) | reader, execution, commit, egress | unchanged |
| sealer-0..2 | 4–6 (cpuset) | Java agents | unchanged |

- The media driver is a separate Nomad job on the same node, in SHARED mode (one thread).
- The ingress job reserves 500 CPU shares (`deploy/cluster/nomad/ingress.nomad.hcl`). That is a
  weight, not a core: the agent competes with the other threads of the node.
- One agent with a short spin and a park costs little at rest. The acceptance (section 9) bounds
  the CPU at rest.
- The DEDICATED mode of the media driver adds threads. It runs on the dedicated hosts first, not
  on the 4-vCPU cloud nodes.

## 8. Tests

- The agent runs with no thread in a test: a `poll` over scripted inputs, as the sequencer tests
  drive their loops (`crates/sequencer/src/inbound.rs`). Deterministic cases:
  - a full ring answers `ring-full`;
  - a duplicate costs no recovery (it never enters the ring);
  - the order of arrival holds on each lane;
  - a refused offer is retried, keeps the order, and answers `txpool is full` at its deadline;
  - a nonce below the local row answers `nonce too low`;
  - a miss pushes the sender to the pump, and the next transaction of that sender is checked;
  - a drain answers every queued item.
- A `loom` test of the wake handshake (3.2): no push is lost between the last poll and the park.
- `recovery_does_not_block_the_reactor` still passes: the recovery runs on the agent thread.
- An end-to-end run of the docker e2e suite with `--verify-agent on`.

## 9. Rollout and measure

1. Stage 0 (section 4).
2. If stage 0 picks the ingress: the agent behind `--verify-agent`, default `off`. Turn it on for
   one ingress only. The load tool sends one side to each ingress, so one spike compares both at the
   same time. The agent adds no publisher session (5.4), so the sequencers see the same images.
3. The spike: 2 x 500 tx/s for 10 minutes, then the ramp to 2 x 2000 tx/s, with the receipt
   latency confirmed through the WebSocket feed (`kardamom-load --feed-confirm`). Compare per side:
   the receipt latency p50, p99 and max, `ingress.recover`, `ingress.submit`, the CPU of the node at
   rest and under load, the context switches, and the latency of the read methods under load.
4. Acceptance: the p50 of the agent side is lower by at least 0.2 ms, the p99 does not rise, the
   read methods do not slow down, and the CPU at rest rises by less than 0.25 of a core.
5. Rollback: turn the flag off and deploy. The flag changes no state and no wire format.
6. When the agent wins, the default changes, and a later PR removes the tokio path.
7. The later stages follow the same steps, one PR each.

## 10. Open questions, now answered

- Q1, the ring: `ArrayQueue` at the edge (many producers). An SPSC crate only when a pair of agents
  exists.
- Q2, the producer handle: one ring for each agent. No handle per connection or per worker.
- Q3, the duplicate cache: shared, checked in the handler before the ring (5.3).
- Q4, the route: one verify agent and one publisher; no round robin (5.4).
- Q5, pinning: no pin on the cloud nodes. On the dedicated hosts, only after a core plan per host.
- Q6, the idle cap: `AeronRuntime::idle_cap()`, with the producer unpark of 3.2, so a quiet agent
  costs no periodic wake.

## 11. Changes from the reviews

| Finding | Change |
|---|---|
| F1, the gain is in the wrong place | Section 1 has the measured breakdown. Stage 0 measures each hop before an agent ships. The ingress agent is a candidate, not the first stage by default. |
| F2, a cheaper first step | #774 (the timer) and #776 (the async publish) are that step. The inline recovery of F2 is not taken: it breaks the rule that the recovery does not run on a tokio worker (section 2). |
| F3, drops and the ack semantics | One agent and one publisher, no round robin. A refused offer waits for its deadline. Only `--ack validated`. |
| F4, the exclusive publication | A clone of the concurrent publication. No `try_claim`, no `unsafe`. |
| F5, the duplicate cache | The hash and the check stay in the handler, before the ring. |
| F6, the nonce view | The receipt pump stays the one writer; a miss goes to it on a channel. The TTL case is named. #768 is a dependency. |
| F7, wrong facts | #774 is open. Section 7 lists every thread. The ingress reserves CPU shares, not cores. No file outside the repository. The idle cap reuses `KARDAMOM_IDLE_CAP_US`. |
| F8, locks on the path | Section 5.6 lists each shared structure and why it stays. |
| F9, style | No new crate yet; the oneshot is used as it is; the agent is a struct with `poll`. |
| F10, missing sections | Sections 5.7 (shutdown), 5.8 (failure), 5.9 (metrics), 8 (tests) and 9.5 (rollback). |
| F11, the split of a batch | Section 1.2 names it, and #774 now includes the fix: no split below 16 transactions, half of the cores at most. The estimate is about 0.3 ms. Stage 0 measures after it. |
| F12, `drain_pending` is not callable | Section 5.4 names the log-crate PR that lifts its pure core into a `pub` generic type. |
| F13, Redis in the pump | Section 5.5: a spawned task reads Redis and sends the row back; the pump never awaits Redis. |
| F14, the order in the handler | Section 5.2: hash, duplicate cache, then decode, as today. |

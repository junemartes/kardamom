# Hot-path agents: the transaction path off tokio

- Status: draft for review, 2026-10-23. Not implemented.
- The user asked on 2026-10-23 to move the hot path off tokio.
- Related: #774 (the timer tick in the batch verifier), #773 (the egress retry queue), the
  DEDICATED-thread experiment of the media driver, `docs/specs/2026-10-21-one-way-pipeline.md`.

## 1. The problem

The path of one transaction crosses the tokio scheduler many times. Each crossing is a wake, a
queue and a possible move to another core. Each one costs microseconds, and a timer costs a full
millisecond.

The ingress path today (`eth_sendRawTransaction`, `--ack validated`):

| Step | Mechanism | Cost on staging (2026-10-23 spike, 2 x 500 tx/s) |
|---|---|---|
| 1. JSON-RPC handler | tokio task | — |
| 2. To the batch verifier | `tokio::sync::mpsc` | — |
| 3. The batch window | `tokio::time::sleep_until` (fixed in #774) | about 1 ms of the 1.4 ms `ingress.recover` |
| 4. The recovery | `spawn_blocking`, 1 to 8 threads | 0.45–0.68 ms for a batch of about 5 |
| 5. The nonce check | local layer, then Redis on a miss (`await`) | a network round trip on a miss |
| 6. The publish | `spawn_blocking(pub_handle.publish)` | one more pool hop |
| 7. The Aeron command | `crossbeam` channel to the shared Aeron thread | a wake of that thread |
| 8. The answer | the handler task wakes | — |

The receipt latency of the spike was p50 3–4 ms and p99 7–11 ms. The seal and the execution
were each about 2 ms at p50. So a large share of the median is scheduling, not work.

The same pattern is in the sequencer (admission and the offer to the sealer), the executor (the
reader and the execution loop) and the egress (the merge and the receipt pump).

## 2. Goals and limits

- Goal: remove the scheduler from the per-transaction path. A transaction crosses each stage
  with no timer, no blocking-pool hop and no task wake, except one wake to answer the client.
- Goal: a bounded, measured cost per stage, with the step metrics that exist now.
- Goal: 10k tx/s per ingress pair without a rise in p99.
- Limit: tokio stays at the edges. HTTP, JSON-RPC parsing, WebSocket, the admin endpoints and
  every call to Redis or to another HTTP service stay on tokio.
- Limit: no change to a wire format, to a decision rule or to a replicated state. This spec
  changes only threads and queues inside one process.
- Non-goal: kernel bypass (DPDK, `ef_vi`, AF_XDP). The cloud nodes cannot use it.
- Non-goal: the Java sealer. It already runs on Agrona agents.

## 3. The model

### 3.1 Agents

An agent is one thread that owns one stage:

```text
loop {
    work = poll(inputs)        // rings and Aeron subscriptions, no wait
    if work > 0 { idle.reset() } else { idle.idle() }
}
```

- The agent owns its state. It shares no lock with another thread.
- The agent never calls `.await`, never sleeps on a timer and never makes a system call in a busy
  pass. An Aeron offer and an Aeron poll are not system calls.
- The idle strategy backs off in three steps: spin, then `std::thread::yield_now`, then park with
  a cap (as `IdleBackoff` and `IdleCap` in `crates/log/src/aeron_live/idle.rs` do now). The cap
  sets the worst wake delay of an idle agent. A busy agent never parks.
- The agent counts its busy and idle time and its queue depths, with the names of
  `kardamom_obs::pipeline`, so the Bottlenecks dashboard shows it like any other loop.

### 3.2 Rings

- Stages connect through bounded single-producer, single-consumer rings. The slots are allocated
  once, at the start.
- Many producers into one agent use one ring for each producer, not one shared ring. The agent
  polls its rings in turn.
- A full ring is back pressure. The producer does not wait: it answers the error of its stage
  (for the ingress, `txpool is full`).
- The ring crate is an open question (section 8, Q1).

### 3.3 The edge

- A tokio handler does the parse and hands the raw transaction to an agent ring with a reply
  slot. Then it waits on the reply slot.
- The agent fills the reply slot and wakes that one task. That is the only wake on the path.
- The reply slot is a `tokio::sync::oneshot`. One send costs one atomic operation and one wake.
  The agent does not block on it.

### 3.4 Threads and cores

- Each agent has a name, a core list and an idle cap from the configuration. Pinning is optional.
  Without a core list, the agent does not pin.
- A busy-spin agent costs a full core while it is busy, and a fraction while it backs off.
- The core budget is a design input (section 6). The ingress and sequencer nodes have 4 vCPUs on
  shared cloud hosts. There, CPU steal can stop a pinned thread, so the idle cap must stay low
  and no stage may assume a dedicated core.

### 3.5 The shared crate

A new crate, `kardamom-agent`, holds the common shape, so each stage reuses one form:

- `Agent`: a trait with `poll(&mut self) -> usize` and `name()`. It has no `dyn` use: each runner is
  generic over its agent.
- `Runner<A: Agent>`: the thread, the loop, the idle strategy, the pinning and the metrics.
- `Inbox<T>`: the set of rings of one agent, with one producer handle for each producer.
- `Reply<T>`: the reply slot of the edge.

## 4. Stage 1: the ingress submit path

### 4.1 The target

```text
JSON-RPC handler (tokio)
   │ raw bytes + reply slot
   ▼
ring ──▶ verify agent k (k = 1..N, round robin over the agents)
           decode → protocol limits → recovery → duplicate cache → nonce view
           → offer on tx_data[lane] (its own publication)
           → reply: hash, or the error
```

- N verify agents. One core recovers about 23k signatures/s (`tests/stage_costs.rs`), so N = 1 or
  2 covers 10k tx/s on one ingress. N is a setting.
- The handler picks the agent round robin. Each handler task has its own producer handle on each
  ring (3.2), or the handlers of one tokio worker share one handle (Q2).

### 4.2 The duplicate cache

- Today: `RecentMap`, a `DashMap` plus a lock-free order ring, shared by all handlers.
- With agents: the cache stays shared and read-mostly. A hit answers the hash. Only a published
  hash enters the cache, and only the agent that published it writes it. The `DashMap` shards keep
  the write cost low at N = 2.
- Option: give each agent its own cache, and route by the transaction hash. Then no cache is
  shared, but a route by hash needs the hash before the recovery, and the hash costs one keccak in
  the handler. Q3 decides.

### 4.3 The nonce view

- Today: the local account layer, then Redis on a miss, with an `await` on the path.
- An agent cannot wait for Redis. The agent reads only the local layer. A sender with no row in
  the local layer admits, as a sender with no row in Redis does today.
- A miss sends the sender to a tokio task that reads Redis and fills the local layer, so the next
  transaction of that sender finds a row. The answer of the first transaction does not wait for it.
- Effect: a stale nonce of a new sender can pass the ingress once, and the sequencer drops it with
  `nonce too low` (a `dropped` status on the egress). The rule "a `nonce too low` answer is always
  true" stays true.

### 4.4 The publish

- Each verify agent opens its own exclusive publication on each `tx_data` lane. It offers with
  `try_claim` or `offer` directly, not through the command channel of the shared Aeron thread.
- A refused offer (back pressure, not connected) answers `txpool is full` at once. It does not
  wait in a retry queue.
- Effect on the sequencer: each lane gets N images from each ingress, not one. The order of one
  sender stays FIFO, because one sender maps to one agent only if the route is by sender (Q4).
  With a round robin, two transactions of one sender can reach the sequencer in another order on
  two images. The sequencer already orders by nonce and parks a gap, so this costs a park, not a
  drop. Q4 decides.
- The stamp for the receipt pump (`stamps.stamp`) moves into the agent, before the offer.

### 4.5 What stays on tokio

The HTTP server, the JSON-RPC parse, `eth_call` and every read method, WebSocket subscriptions,
the Redis client, the receipt pump and the admin endpoints.

## 5. Later stages

Each later stage gets its own spec section and its own PR, after stage 1 shows its gain on
staging.

| Stage | Agent | Input | Output |
|---|---|---|---|
| 2. Sequencer admission | one agent for each lane | `tx_data` images | the cluster offer |
| 3. Executor reader | one agent | the cluster egress or the member feed | the execution ring |
| 4. Executor execution | one agent | the execution ring | the commit ring, `exec_frames`, `tx_receipts` |
| 5. Egress merge | one agent | `exec_frames`, `tx_receipts` | the store, `egress_order`, `egress_receipts` |
| 6. Notifier and ingress receipt pump | one agent | `egress_receipts` | the WebSocket fan-out (tokio) |

- The state commit stays on its own thread. It is not on the latency path: commits run at about
  0.6/s and hold many blocks.
- Stage 2 must keep the twin rule: both twins offer the same bytes in the same order.

## 6. The core budget on staging

| Node | vCPUs | Today | With stage 1 |
|---|---|---|---|
| ingress-0/1 | 4 (cloud, shared) | tokio workers, the blocking pool, the Aeron thread, the media driver (SHARED) | 1–2 verify agents, 2 tokio workers, the Aeron thread, the media driver |
| sequencer-0/1 | 4 (cloud, shared) | 4 sequencer loops, tokio, the media driver | unchanged in stage 1 |
| executor-0..2 | 4–6 (cpuset on a dedicated host) | reader, execution, commit, egress | unchanged in stage 1 |
| sealer-0..2 | 4–6 (cpuset) | Java agents, the media driver | unchanged |

- On a 4-vCPU node, two spinning agents and a spinning media driver leave one core for tokio.
  So the idle cap of each agent must keep its idle cost low, and the measure (section 7) must
  show the CPU of each node at rest and under load.
- The media driver in DEDICATED mode adds 3 threads. It fits the dedicated hosts. On the 4-vCPU
  cloud nodes it competes with the agents. The DEDICATED experiment runs on the sealers and the
  executors first.

## 7. Rollout and measure

1. The crate `kardamom-agent`, with unit tests of the loop, the rings, the back pressure and the
   idle strategy. No service uses it yet.
2. Stage 1 behind a flag: `kardamom-ingress --agents <N>`, default 0 (the tokio path). The deploy
   variable of the ingress job sets it. One ingress can run the agents while the other does not,
   so one spike compares both sides at the same time.
3. Measure on staging with the spike of `scratchpad/bench/run-load.sh`: 2 x 500 tx/s for 10 min,
   then the ramp. Compare per side: the receipt latency p50, p99 and max, `ingress.recover`, the
   steps of the submit path, the CPU of the node at rest and under load, and the context switches.
4. Keep the flag until the agents path wins on both latency and CPU at rest. Then the default
   changes, and a later PR removes the tokio path (P13 style cleanup).
5. Stages 2 to 6 follow the same steps, one PR each.

Acceptance for stage 1: the p50 of the receipt latency falls by at least 0.5 ms against the tokio
path with #774, the p99 does not rise, and the CPU of the ingress node at rest rises by less than
half a core.

## 8. Open questions

- Q1. The ring: `rtrb` (SPSC, wait-free), `crossbeam_queue::ArrayQueue` (MPMC, lock-free, already a
  dependency), or a hand-written ring with pre-allocated slots. The proposal: `rtrb` for SPSC,
  because each ring has one producer.
- Q2. The producer handle of a tokio handler: one ring for each handler task needs a registration
  per connection. One ring for each tokio worker needs the worker id. The proposal: one
  `ArrayQueue` (MPMC) for each agent at the edge only, because the edge has many producers, and
  SPSC rings between agents.
- Q3. The duplicate cache: shared (`DashMap`) or one for each agent with a route by hash. The
  proposal: shared, because N is 1 or 2.
- Q4. The route to an agent: round robin (even load, two images can reorder one sender) or by
  sender (FIFO per sender, but the sender is known only after the recovery). The proposal: round
  robin, and the sequencer parks the gap.
- Q5. Pinning on shared cloud vCPUs: pin, or let the kernel place the agents. The proposal: no pin
  on cloud nodes, pin on the dedicated hosts.
- Q6. The idle cap of an agent: the latency of the first transaction after a quiet period against
  the CPU at rest. The proposal: 50 µs, the same order as the batch window of #774.

## 9. Risks

- A busy agent on a shared vCPU can starve tokio. The measure in section 7 must include the read
  methods (`eth_call`, receipt reads) under load, not only the submit path.
- A panic in an agent stops its stage. The runner ends the process, so Nomad restarts it, as a
  panic in the Aeron thread does today.
- The local nonce view admits more stale nonces after a restart (4.3). The `dropped` status of #768
  reports each one.

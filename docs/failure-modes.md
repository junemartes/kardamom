# Failure modes

How each kardamom actor fails, what it costs, how it recovers, and where that
behavior is verified. Grounded in the failover specs (`docs/agents/`), the
chaos suite (`crates/chaos`, one case per function — case names appear like
`cluster-leader-kill` throughout; most run in CI via
`.github/workflows/cluster-e2e.yml`), and the recovery code itself.

Chaos answers "does the pipeline survive faults under load?". Its counterpart,
the **chain-semantics suite**
(`docs/agents/chain-semantics-e2e-suite-spec.md`), answers "does the chain mean
the right thing?" — bridge round-trips, nonce ordering, validator/executor and
DA parity, state-DB integrity — on both a single-host stack (`just
test-e2e-local`) and the same DinD cluster (the `semantics` shard). Several
entries below were found by it, and are marked.

![Kardamom service architecture](img/architecture.jpg)

The design in one line: everything on the hot path is either **replicated
shared-nothing** (ingress, executor), **sharded with retry semantics**
(sequencer), or **Raft-replicated with fail-stall-on-quorum-loss** (sealer);
everything off the hot path is allowed to die and catch up (batcher,
da-watcher) or die loudly (validator).

## Halts

A service that cannot continue safely **halts**: it stays up, keeps its state,
serves its metrics and its query endpoints, makes no progress, fails its
readiness check, and says why and what to do. It does not exit: an exit loses
the cause and invites a restart against the same fault, which is how the
staging incident of 2026-10 ran for two days. The halt is one shared type,
`kardamom_obs::halt::Halt`:

| field | meaning |
|---|---|
| `cause` | a stable id: the `cause` label of the gauge and the alert |
| `detail` | the numbers: the block, the two hashes, the cursor and the floor |
| `recovery` | the runbook id, `docs/runbooks/<id>.md`: the steps, in order |
| `since` | when the halt was raised |
| `clears` | `auto`: the service retries its cause on a backoff and resumes when it goes; `operator`: the service waits for `POST /halt/clear` after the runbook |

What it drives: the gauge `kardamom_halt{service, cause, recovery}` is 1 while
the halt stands; the `/halt` route beside `/metrics` and `/ready` serves the
whole record as JSON; `/ready` answers 503 while halted, so a rolling deploy
stops at a halted service; one Alertmanager rule per cause
(`deploy/alerts.yml`), whose annotation names the cause, the `/halt` route of
the instance, and the runbook. An operator halt clears with
`curl -X POST http://127.0.0.1:<port>/halt/clear` on the service's node: the
route accepts a loopback peer only, which is how the repository guards every
admin action (there is no credential; placement is the guard). A unit test in
`crates/obs` asserts every recovery id has a runbook and every cause an alert
rule.

| cause | service | clears | runbook |
|---|---|---|---|
| `l1_source_disagreement` | indexer, da-watcher (with two L1 sources) | auto | `docs/runbooks/l1_source_disagreement.md` |
| `l1_chain_break` | indexer, da-watcher | auto | `docs/runbooks/l1_chain_break.md` |
| `l1_unreachable` | batcher, indexer, da-watcher | auto | `docs/runbooks/l1_unreachable.md` |
| `replay_unavailable` | batcher | operator | `docs/runbooks/replay_unavailable.md` |
| `da_lag` | the sealer, as the ingress observes it (`service="sealer"`) | auto | `docs/runbooks/da_lag.md` |
| `sealer_no_quorum` | the sealer, as the ingress observes it (`service="sealer"`) | auto | `docs/runbooks/sealer_no_quorum.md` |
| `validator_divergence` | validator | operator | `docs/runbooks/validator_divergence.md` |

`docs/runbooks/revert_to_posted_head.md` is the last resort the
`replay_unavailable` runbook sends the operator to: no cause names it directly.
The metrics ports of the deploy: batcher 9002, da-watcher 9005, ingress and
validator 9006, l1-indexer 9009. The chaos cases assert the halt record, not
only the log line (`da-lag-halt`, and the chain-semantics divergence drills).

**Service events: halted, paused, resumed.** The services share their
lifecycle state on the `events` Aeron stream (id 1019, best effort, RAM only;
`kardamom_types::service::ServiceEvent`). `Halted` is the service's own fault
and pages. `Paused` means the service waits on something outside itself: a
root halt upstream, or an operator's pause (`POST /pause?note=...` and
`POST /resume` on its exporter, loopback only). A paused service makes no
progress, keeps its state, serves its metrics and queries, fails `/ready`, and
resumes by itself when the root clears; it never pages. `Resumed` is published
once on the way back. Every service publishes its state at once on a change
and every 5 s; a record with no heartbeat for 15 s is `gone`. The sealer has
no Rust runtime on the stream, so the ingress observes it on its cluster
session and publishes it as `sealer/cluster`: `da_lag` from the status frame,
`sealer_no_quorum` after 10 s without one.

| root | reaction |
|---|---|
| the sealer has no quorum | the ingresses pause submits (typed error naming the root); the sequencers pause offering, from their own egress silence |
| the sealer's DA-lag guard | the ingresses pause submits; the guard itself stays in the cluster log |
| every executor halted | the ingresses pause submits; one executor halted changes nothing |
| a validator divergence | the output attester pauses: no output root reaches L1 |
| the batcher halted | the chain status shows it; the DA-lag guard enforces |
| the da-watcher halted | the chain status shows deposits delayed |
| the l1-indexer halted | `kardamom-reconstruct` refuses it (`indexer_halt` on its API; the indexer is not on the stream) |

Nothing that changes the canonical order reads the stream: a lost event can
delay a pause or a resume, never change the order. `kardamom_chainStatus` on an
ingress returns the posted and sealed heads, the roots, the sealer, the
ingress, and every service's latest state. A pause exports
`kardamom_paused{reason, root_service, cause}` and fires the info alert
`KardamomServicePaused`; the inhibit rule in `deploy/alertmanager-inhibit.yml`
mutes it while the root's halt alert fires, so one incident pages once, with
the root's runbook.

**The DA-lag guard and the posted head.** The batcher publishes its confirmed
cursor (the last L2 block on L1) on the cluster ingress as a system record
(`KIND_POSTED_CURSOR`) at start and after every confirmed post. The sealer
keeps it in its replicated state and refuses user records while
`sealed_head - posted_head > DA_LAG_BUDGET_BLOCKS` (default 10,000; zero turns
the guard off, in the open). Deposits and boundaries still enter. Every member
takes the same decision: the cursor is in the log and the budget is shared
configuration. The ingress halts the sealer's observed lifecycle on `da_lag`
from the status frame the sealer fans out on every tick
(`kardamom_halt{service="sealer", cause="da_lag"}`), pauses its own submits on
that root, answers a refused submit with the typed JSON-RPC error
`chain halted: da_lag at sealer (...)` (code -32010, `data.cause = "da_lag"`,
`data.runbook`), and serves `safe` and `finalized` from the posted head
(`kardamom_blockNumberByTag`; the batcher does not observe L1 finality today,
so `finalized` is the posted head too). The same posted head is the floor of the
sealer's egress retention: the window of `kardamom.cluster.retention` frames is
a minimum, and a frame past it leaves only when its block is posted, so the
batcher always replays from its cursor. The frames above the posted head ride
the Raft snapshot, so a restored member keeps the floor. The guard bounds the
stretched window to the budget's blocks plus one flush, in heap.

## Sealer — the Aeron Cluster (Raft)

The ordering authority, and historically the hard SPOF: the old standalone
sealer's crash froze every executor permanently (`sealer-hard`, kept only to
reproduce the legacy gap; issue #58). The 3-member Aeron Cluster replaces that
with three distinct, tested modes:

![Sealer cluster failure states](img/states-sealer-cluster.jpg)

- **Leader hard-kill** (`cluster-leader-kill`) — quorum survives, a leader is
  re-elected (possibly the restarted member re-winning, since it has the most
  up-to-date log — the suite asserts *the pipeline keeps committing*, not
  *the leader changed*), clients redirect via `NewLeaderEvent`. Replicated
  state + snapshots hand the new leader `{dedup window, canonical count,
  block_number}`: no gap, no block-number regress.
- **Follower kill** (`cluster-follower-kill`) — quorum 2/3 holds; **zero
  stall** is asserted and the leader must be unchanged. The member restarts
  and rejoins.
- **Member state loss** (`cluster-member-rejoin`, `node-replace-sealer`) — a
  follower comes back with no Raft log. Before it starts, it copies the
  latest snapshot, and the log after it, from a peer (an Aeron
  ClusterBackup). It restores the snapshot and catches up from the leader.
  A blank member starts at log position 0 only on the bootstrap of a new
  cluster (`KARDAMOM_CLUSTER_BOOTSTRAP=1`). Without a peer, it waits and logs
  `cluster SEED waiting-for-peer`.
- **Quorum loss** (`cluster-quorum-loss-recover`) — two nodes killed: the
  pipeline **must stall** (the suite asserts the executor block gauge goes
  *flat* — progress without quorum would be unsafe, unreplicated ordering).
  One node returning restores quorum, but this is the one case where client
  cluster *sessions* die (the outage exceeds the session timeout): re-election
  + session re-establishment + log replay takes ~50 s+ observed (SLO 180 s),
  then the backlog drains gaplessly. The second node returns last, and the
  pipeline must progress again with all three members.
- **Total loss** (`cluster-total-loss-recover`) — all three nodes killed: no
  member is left, the pipeline **must stall**. Every node returns with its
  own log and snapshots, the members elect a leader among themselves, and
  the backlog drains.
  What this does not cover: all three members *wiped*. No in-cluster copy
  is left; the members start from a seed rebuilt from L1 (the sealer fleet
  rebuild below, `sealer-fleet-total-wipe-recover`).

- **Redis total loss** (`redis-total-loss-recover`) — Redis is a cache with no
  persistence; the executors' state is the truth. The whole redis job
  (primary, replica, three sentinels) is stopped for 30 s: the readers
  degrade to the executor query, the pipeline progresses, the mirrors retry
  their writes. The job returns empty, the sentinels name a primary, and every
  state mirror rebuilds the projection from its executor's newest checkpoint.
  See `docs/specs/2026-09-13-redis-account-cache-design.md`, section 9.3.

Every chaos case ends with a **recovery probe**: after the case load ended
and the executors converged, two 30 s loads run at once, each at half the
case rate. The first runs on the case's account from its next nonce, through
the ingress the case load used: the sender that had transactions in flight
during the outage. The second runs on the smoke gate's account, which no
case load spends, through the other ingress: a sender with nothing in
flight. Every offered transaction of both must get a receipt, and each must
be accepted at a quarter of its rate or more. A failure of the first alone
is a stuck sender; a failure of the second is a pipeline, or an ingress,
that did not recover. The case load cannot prove either: a submit refused
during the outage leaves a nonce hole, every later submit of that sender
parks and fails, and the chaos verdict does not count a failed submit. The
executor block gauge cannot prove it either: it advances on empty blocks.
The first fleet-shard run showed the gap: after the quorum loss, 1,282 of
1,524 submits never landed and the case still passed.

**Coordinated failures** (`chaos-coordinated` shard). The single-replica
cases prove that a twin covers a loss; these prove the recovery when no twin
is left, or when the roles fail together:

- **Both ingresses** (`ingress-pair-loss-recover`) — both ingress tasks
  hard-killed: the whole client edge is gone. Nomad restarts both, both
  exporters must answer, the pipeline must progress, and the probe submits
  through each ingress.
- **A whole sequencer lane** (`sequencer-lane-loss-recover`) — both replicas
  of lane 0 hard-killed, one on each sequencer node. The lane's senders are
  unordered until a replica returns with empty state and learns each
  sender's floor from the executors. Both replicas must come back healthy
  and `kardamom_sequencer_ref_below_floor` must read zero on both.
- **Pipeline blackout** (`pipeline-blackout-recover`) — every ingress,
  sequencer, sealer, executor and aux node killed at once; only the control
  node stays, with the orchestrator and the L1. All nodes start in one call,
  with no arranged order. Every job must return to its count, the sealers
  must elect a leader within 180 s, both ingresses must be live, and the
  pipeline must progress. The blackout can lose the transaction data of an
  entry that the sealer already ordered. Before the void rule below, all
  three executors then crash-looped on that entry (`join timeout: TxRef ...
  not found within 30000 ms`) and the chain was wedged for good. Now every
  consumer votes, the sealer voids the entry, and the chain moves again. The
  case prints the number of void decisions as evidence.

**Removal of an entry that no consumer can execute (void).** The sealer
orders a transaction reference before the archives make its data durable, so
a failure such as the blackout above can leave an entry with no data. The
sealer can remove such an entry with a canonical *void record*. The rule has
no clock in it:

- A consumer votes (`KIND_VOID_REQUEST`) only after the join budget ends and
  every archive refuses the range. A consumer that has the data never votes.
- The sealer appends the void record only when **every** configured voter has
  voted for the same `(index, tx_hash)`. One voter that is down blocks the
  void, and the chain waits for it. This is the safe side: that voter can be
  the one that executed the entry.
- On a void the sealer removes the hash from its dedup window and sets the
  sender's expected nonce back, so the sender can submit the same bytes again.

Three constants bound the rule. Every member must run the same values.

| Setting | Default | Meaning |
|---|---|---|
| `kardamom.cluster.voidVoters` | empty | The voter ids. Empty refuses every vote, which is the behavior before this rule. |
| `kardamom.cluster.voidWindow` | 65536 (the egress retention) | The newest indices that a void can name. An older entry cannot be removed. |
| `MAX_OPEN_VOTES` | 1024 | The most indices with open votes. More are refused. |

The order path pays one 52-byte copy for each reference and no allocation.
The votes and the window are in the snapshot (version 6). With voters
configured, a full window adds 65536 x 68 bytes (about 4.4 MB) to each
snapshot. A void needs the last vote before `voidWindow` more records are
ordered after the entry: with live ingress at more than about 1000 records
per second and a 60 s join budget, the entry leaves the window first, and the
chain stops as it did before this rule.

The voter list must equal the set of consumers that execute. The deploy
builds both sides from the executor count: executor `i` votes with id `i`
(`--void-voter-id`, its allocation index), the validator with the next id,
the batcher with the one after, and `cluster.nomad.hcl` gives the sealer the
same range. A consumer outside the list cannot stop a void. If it executed
the entry, it stops with `VoidOfExecutedEntry` when it reads the void record.
A consumer waits 120 s for the void record and then restarts; the sealer
keeps its vote. A sender gets no notice of a void yet: the receipt never
comes, and the sender submits again.

**Durability of the Raft log.** The kill-based cases above prove the
restart logic, not the durability against a power loss: a process kill or a
container kill leaves the host kernel and its page cache alive, so every
member finds its whole log again. The sealer therefore syncs the Raft log and
the archive to disk (`kardamom.cluster.fileSyncLevel`, deployed at 1: the
data of every write batch). At level 0 an entry that a quorum acknowledged
could exist only in the page caches of its members, and a rack-level power
loss, the event `pipeline-blackout-recover` stands for, would drop it. Only
a test that cuts the power of a VM can prove this end to end.

## Executor

![Executor failure states](img/states-executor.jpg)

- **Process crash** (`graceful-executor` / `hard-executor`) — Nomad restarts
  it; startup reads the libMDBX `meta` cursors (`last_committed_block`,
  `last_committed_end_tx_position`) and replays the canonical stream from the
  Aeron archives via a replay-merge, skip-counting past the durable cursor.
  State is committed durably per block, so there is no double-apply and no
  genesis re-sync; the cluster subscription's canonical-index cursor drops
  any reconnect overlap.
- **Whole-node loss** (`node-failure-executor`) — with `distinct_hosts` there
  is no spare node to reschedule onto: the fleet degrades 3/3 → 2/3 and must
  keep progressing; the returned node rejoins to 3/3. Replicas are
  deterministic state machines, so one dead or lagging replica never blocks
  the others.
- **Whole-fleet loss** (`executor-fleet-loss-recover`) — all three executor
  nodes killed at once, every exporter observed dark, then all three return.
  Each executor resumes from its own state directory and catches up on the
  backlog the sealers kept ordering, within the canonical retention window.
- **Whole-fleet state loss** (`executor-fleet-wipe-recover`) — all three
  executor tasks killed and every state DB wiped, with each node's own
  checkpoints kept. No peer is live to serve a checkpoint, so every executor
  must restore from its local checkpoint and replay the tail; the case
  requires one restore line per executor. All three wiped *with* their
  checkpoints has no live source at all: that is the rebuild-from-L1
  backstop.
- **Whole-fleet total loss** (`executor-fleet-total-wipe-recover`) — the
  executor job stopped, and every state DB **and every checkpoint** wiped: no
  executor holds state and no peer can serve any. The harness rebuilds an
  executor image from L1 and the DA proxy on the host
  (`kardamom-reconstruct --through-block --executor-image`), installs it on
  every executor node, and starts the job. Every executor must resume from
  the image's cursor, with a replay request the sealer accepts and with no
  checkpoint restore or peer fetch, and the fleet must catch up. The
  end-of-shard audit then compares the resumed executors with the validator
  table by table. The job is stopped, not killed: an executor that starts on
  an empty directory of a young chain replays from genesis on its own.
- **Machine replacement** (`node-replace-executor`) — the node is replaced
  through the Terraform root the way a cloud provider replaces a server: a
  new address and empty volumes, then the substrate play a new machine
  gets. Consul must forget the old record (the control node force-leaves
  it), Nomad must place the lost executor on the new client, and the
  executor must catch up from nothing. Nothing but the root's address plan
  moves, since every peer resolves the node by name. A restarted node
  (`node-failure-executor`) keeps its address and its disks; this is the
  path that loses both.
- **State-DB volume loss** (`state-checkpoint-restore`) — a *wiped* state DB
  (not just a process crash) would otherwise force a re-sync from genesis,
  replaying the entire canonical stream — unbounded as the chain ages. With
  `--checkpoint-dir` the executor writes periodic consistent snapshots
  (`compact_to`, an online RO copy that never blocks the writer) and, on a
  cold start against an empty state dir, restores the newest checkpoint
  *before* opening the env — so the normal resume path replays only the short
  tail. Because the replicas are deterministic at the same block, a **peer**
  executor's checkpoint is a valid restore source; the chaos case wipes
  executor-0's state, re-replicates executor-1's checkpoint, and asserts
  executor-0 restores from it (not a genesis re-sync) and rejoins.
- **Replay-window overrun** (`replay-window-resync`) — the cluster retains a
  *bounded* canonical window (`kardamom.cluster.retention`, default 65,536
  frames ≈ 5 minutes at 200 tps); a `REPLAY_FROM` below its floor is refused
  with `REPLAY_UNAVAILABLE`. That refusal is terminal for the local state: a
  cursor below the floor can never catch up, and a fresh/wiped node cannot
  "re-sync from genesis" either — genesis aged out of the window with
  everything else. (The floor also jumps to the head when a member restores
  from a cluster snapshot, so this is not only an "old node" event.) The
  executor self-heals with **peer state**: each replica serves its newest
  checkpoint over `--checkpoint-serve-addr` (:9014) and a node that hits the
  refusal — or cold-starts with no checkpoint at all — fetches the newest
  qualifying peer checkpoint (`--checkpoint-peers`), parks the stale DB under
  `<state_dir>/stale/`, restores, and resumes from the checkpoint's cursor,
  all in-process: the pipeline runs again in the same process, so a
  revolution costs the fetch and the restore, and burns no orchestrator
  restart attempt (#298; `kardamom_executor_resync_total` counts these by
  outcome). If no peer can
  offer a checkpoint at/above the floor, the node stays down and says so: the
  remaining paths are an operator-restored checkpoint or rebuild-from-L1
  (`kardamom-reconstruct`).
- **Wedged (frozen)** — an executor whose block gauge is flat *while sealer
  boundaries keep advancing* is the load harness's FROZEN verdict; that
  contrast (rather than absolute progress) is what distinguishes a wedged
  replica from a quiescent chain.
- **Leader failover above it** is invisible: the cluster client hides
  reconnects from the reader thread.

## Ingress (×N, active/active)

- **Replica, node, or its media-driver death** (`hard-ingress`,
  `archive-driver-loss`) — costs only that replica's in-flight clients, who
  retry against a survivor. Replicas are shared-nothing (no leader, no sticky
  sessions); any replica can accept any sender's tx.
- **The failure this design had to solve:** a tx accepted by replica A whose
  client retries against replica B. Receipts are multicast to **all**
  replicas (invariant I-B in `docs/agents/resilient-ingress-spec.md`), so B
  answers from its `SeenReceipts` cache without republishing. The dangerous
  mode is a **frozen multicast group** — one stuck subscriber stalling receipt
  fan-out; the `ci-cluster.sh` ingress-churn step reproduces exactly that.
- **Ack-policy window:** the default `on-offer` acks once the tx is offered
  to the pipeline, so a replica dying after the ack but before its
  publication is durable can lose that tx. The `on-quorum` gate (ack only
  after the Raft cluster commits) exists for when that window matters.

**Receipts across an ingress restart.** The receipt cache lives in the
memory of one ingress process, so a restarted ingress holds no receipt from
before its start. `eth_getTransactionReceipt` therefore asks an executor's
state DB on a cache miss, and a receipt found there enters the cache. The
`ingress-pair-loss-recover` chaos case kills both ingresses under load and
requires every accepted transaction's receipt afterwards.

## Sequencer (2 shards × 2 racing replicas)

Each shard is served by two active/active replicas on different nodes (Nomad
groups `seq-a`/`seq-b`, cross-placed via `meta.node_index` — see
`docs/agents/replicated-sequencer-shards-spec.md`). Both consume the shard's
`tx_data` multicast stream and both offer byte-identical refs; the cluster's
first-seen dedup keeps one.

- **Replica crash / hard kill** (`sequencer-replica-kill`,
  `graceful-`/`hard-sequencer`) — **no stall**: the twin never stopped. Chaos
  asserts live pipeline progress during the outage, 4/4 allocs back within
  the restart SLO, and that the restarted replica actually publishes refs
  again. The restarted replica joins live (no archive replay — its twin
  covered the gap, and replay could overshoot the sealer's dedup window);
  hydrated nonce floors are only a lower bound, and **receipt-floor resync**
  advances a stalled floor on execution evidence from the tx_receipts
  stream — buffered nonces the twin already got executed drop as proven
  duplicates and the run unsticks (the stream-adaptive `nonce_floor_lag_ms`
  fast-forward this replaces was REMOVED for publishing canonical nonce
  gaps; the config key still parses but is inert — see
  `docs/agents/sequencer-lag-resync-spec.md`).
- **Replica lagged/paused** (`sequencer-lapse`) — a replica frozen past the
  boundary-silence window detects the lapse itself (egress boundary-arrival
  gap → sticky lag flag → resync mode) and skips only receipt-proven
  duplicates on resume; everything unproven publishes and the cluster dedup
  absorbs it. A predecessor session corpse can no longer cycle the restarted
  replica's session (foreign-session event filter, issue #99).
- **Sequencer node loss** — cross-placement guarantees every shard keeps one
  replica; redundancy (not availability) degrades until the node returns.
- **Both replicas of one shard down** — that shard stalls; this is now the
  double-failure case.
- **Backpressure, not loss** — a refused cluster offer maps to
  `SequencerError::Backpressure` and the rewind/retry path; the failure mode
  is latency, never a dropped record.
- **Racing duplicates are the design** — deduped by the cluster's first-seen
  window on the 32-byte `canonical_id`, with per-sender nonce order preserved
  (per-session order + identical per-replica streams); pinned by
  `crates/sequencer/tests/replicated_shard_racing.rs`.

![Edge and off-hot-path failure states](img/states-edge-offpath.jpg)

## Validator (off hot path, fail-stop by design)

The failure philosophy is inverted: **halting is the feature**. On any
divergence — re-executed receipts or BAL disagreeing with the executor's, or
an MPT state-root mismatch — it stops rather than continuing on bad state,
and stays stopped until an operator intervenes. A crashed validator costs
verification coverage, never L2 liveness; nothing on the hot path consumes it.

A divergence is a state, not a dead process. **A proven divergence is the
`validator_divergence` halt** (the latch records the reason before the engine
surfaces it). The validator writes its verdict to a `verdict` file beside its
state and mirrors it in the `validator_verdict_standing` gauge. The process
stays up, serves its metrics and the `/halt` record, fails `/ready`, and waits
for the operator's clear after `docs/runbooks/validator_divergence.md`. A
restart (a crash, a node loss, a deploy) that finds a standing verdict raises
the same halt and waits again, so a deploy never passes over a divergence.
Either clear ends both the halt and the file: `POST /halt/clear` on the node,
or `kardamom-validator --state-dir <dir> --clear-verdict`. Then the validator
runs the pipeline again from its cursor. Every other engine failure — a stream
error, or a replay-window overrun (`REPLAY_UNAVAILABLE`, the validator cursor
aged out of the cluster's bounded retention) — exits 1: an availability
problem, restartable, never to be confused with an integrity one.

A replay-window overrun self-repairs like the executor's recovery-D loop
(#143): fetch a peer checkpoint at/above the retention floor from an
executor's serve endpoint, park the stale DB, and run the pipeline again in
the same process; that revolution adopts it (#298, no exit and no
orchestrator restart, so a lost race against the retention window costs one
fetch, not one restart attempt) —
including a marker-driven one-time hashed-mirror + trie bootstrap: executor
checkpoints carry a trie FROZEN AT GENESIS (seeded into every env, never
updated by the trie-off writer), so adoption must rebuild it wholesale — a
presence probe would pass on the stale trie and the incremental walker would
extend it into silently wrong roots the shadow-check cannot catch (it
rebuilds from the same stale mirror) — and resumes verified execution from
there.
The adoption trust class is the same as #78 catch-up, made explicit: blocks
through the adopted checkpoint are **unverified by this validator**
(`validator_resync_total{outcome="peer-checkpoint"}` counts adoptions); the
divergence latch only ever covers blocks it actually re-executed. The
trustless alternative remains rebuild-from-L1 (`kardamom-reconstruct`) into
the state dir.

Catch-up semantics (#78) make that coverage cost explicit:

- **Behind the head** (fresh start against a running chain, or restart): the
  per-block BALs ride a lossy `tx_bal` multicast whose term buffer only holds
  the recent window, so backlog blocks more than `BACKLOG_LOOKBEHIND` (16)
  behind the live head have unrecoverable BALs — the validator **commits them
  unverified immediately** instead of burning the full BAL wait per block
  (which made catch-up slower than the chain grows). Continuous verification
  is a property of a *caught-up* validator, at the head.
- **Brief lapse** (pause/stall shorter than the live term buffer): fully
  covered — the missed BALs are still buffered on resume, verification
  continues without a coverage gap. The `validator-lapse` chaos case pauses
  the validator for 30 s under load and asserts it catches back up, keeps
  verifying, `validator_bal_missing_total` doesn't materially grow (a small
  tolerance absorbs edge-of-window blocks), and there are zero divergences.
- **Lapse longer than the term buffer**: those blocks age out and are
  committed unverified (counted in `validator_bal_missing_total`) — recovering
  them would need archive-backed refetch, which was prototyped and
  deliberately discarded because a co-located recorder + follow-live replay
  starves the validator's live poll path (see PR #78's discussion).

## Batcher (live service, cluster-egress-driven)

A crash costs **DA freshness only** — L2 keeps sequencing and executing.
Since #39 the batcher is a long-lived service and the third cluster-egress
consumer (next to the executor and validator): it tails the canonical
ordering from the Aeron Cluster egress, joins tx_data through the same
engine reader stack (join-miss archive refetch included), and posts each
packed batch to L1 as it closes. A restart replays the canonical stream from
its durable cursor — written only after a confirmed post — and skips blocks
L1 already covers, so the failure mode is a growing L1-posting lag, not data
loss and never a double post (the contract CAS rejects those loudly). Its
real dependencies are one surviving state database and L1 gas/RPC health,
and both are halts, not exits. An L1 that does not answer at start or on
every attempt of a post is the `l1_unreachable` halt (the batcher starts
again after a backoff). A refused replay is first answered by the rebuild
below. When the rebuild cannot fill the gap, the batcher holds the
`replay_unavailable` halt (operator: recover the range from the spool or a
surviving copy, or revert the chain). The sealer's retention never prunes
below the batcher's published cursor (see Halts), so a refused replay means
the range is below the posted head, never that the window passed. See
`docs/agents/batcher-live-l1-spec.md`.

**Retention is a latency, not a loss, while one state database and one
archive survive.** The resume has three sources, in order:

1. The spool: every block the batcher consumed and did not post yet, on
   its own disk. A restart continues the pending group from it.
2. The sealer's replay from the cursor, inside its egress retention.
3. A rebuild from what the nodes already keep. The state database of every
   executor and of the validator holds the ordering: the `headers` table
   maps a block to its canonical end and its L1 origin, and the `receipts`
   table holds one row per canonical position with the transaction's hash.
   The `tx_data` archives hold every raw transaction the ingress accepted,
   on both ingress nodes. The link between them is the `TxRef` the egress
   record carried; the state writer now keeps it with the receipt (the
   `tx_hash_index` row gains the shard id, the publisher session and the
   archive position, 13 bytes; an 8-byte row written before still
   decodes). When the sealer answers `REPLAY_UNAVAILABLE` past the cursor,
   the batcher reads each missing block's references from the query
   endpoints it names with `--block-refs-source`
   (`kardamom_getBlockRefs`: the block's canonical end, its L1 origin and
   timestamp, and `(tx_hash, tx_idx, shard_id, session_id, position)` per
   transaction in canonical order, deposits excluded), fetches the bytes
   from the archives through the join-miss refetch the engine uses, checks
   each hash against its bytes and each block's end against its
   predecessor's, closes the blocks as the live feed closes them, fills the
   spool and the pending group, and resumes at the sealer's floor. The
   rebuilt range packs to the bytes the live path posts.

A second refusal after the rebuild is the `replay_unavailable` halt, and so
is a refusal without a query endpoint or without the refetch endpoints. A block that
carried a cross-chain message cannot be rebuilt this way: its remote-epoch
record is in no archive, and the query endpoint refuses the block instead
of answering a shorter list. Nothing is pruned below the posted head: the
reference rows live with the receipts, which nothing deletes, and the
rebuild never touches the archives' retention.

**L1 endpoints.** `--l1-rpc` takes a list. A request goes to the best
endpoint first and falls back to the next on an error or an HTTP 429; a
failing endpoint ranks last for the requests after it. The batcher reads
only what the contract commits to, so it does not cross-check hashes; the
followers do (below).

Each batch's payload is dispersed to EigenDA through the proxy
(`nomad/da-proxy.nomad.hcl`), and the certificate the disperser returns is
posted to `KardamomL2Settlement`: L1 records the ordering + the
certificates, and EigenDA holds the **bytes** (for 14 days; the inbox
indexer archives them past that). The proxy checks a certificate against
EigenDA's verifier contract on every put and get, and the bytes against the
certificate's KZG commitment on every get, so a reader trusts its proxy,
not the disperser. The offline segment-file mode (`--channel-b-segment`,
dry-run by default) remains for archive inspection and the corruption-heal
tooling.

**A skewed resume cursor is a refusal.** A consumer resumes at a record
index and a block number, and the two select frames on separate axes. A
pair that does not name one point of the stream would skip records or apply
them twice, and no consumer-side check sees it, because the consumer seeds
every counter from the same cursor. The sealer holds the boundaries, so it
checks the index against the end of the block before the named one and the
end of the named block, where it retains them, and answers
`REPLAY_UNAVAILABLE` (`cluster REPLAY ... SKEWED`) for a pair outside. The
refusal routes the consumer into its repair path. A cold start sends the
block end exactly; a reconnect inside an open block sends an index between
the two ends; a start from genesis has no boundary to check.

**A resume cursor ahead of the head is a refusal.** A sealer that lost its
stream, for example in a total wipe, can come back behind its consumers. A
consumer whose index or block is past the sealer's head applied records the
sealer does not hold. The sealer answers `REPLAY_AHEAD` (egress kind 11),
which carries its head, and logs `cluster REPLAY ... AHEAD
head=(index,block)`. A `REPLAY_DONE` would let the consumer drop each new
record below its cursor as a duplicate and diverge with no signal. The
consumer checks the same rule on its side: a `REPLAY_DONE` whose head is
below the delivery cursor also stops it. Both give `ClusterBehindCursor`.
No repair path takes this error. The replay-unavailable repair fetches a
peer checkpoint, but after a sealer wipe every peer is ahead of the head
too, so a repair would only park the local state and loop. The executor,
the validator and the batcher exit with status 1. The orchestrator restarts
each one, and each restart stops at the same refusal
until the sealer serves the stream again.

**A lying or absent L1 endpoint.** The batcher's start reads the settlement
contract: `lastBatchIndex`, and the `l2BlockEnd` the contract stores with
that batch. Two `eth_call`s, no event scan, no wait on the inbox indexer.
An endpoint that swallows the `BatchPosted` logs, or an indexer behind the
head, cannot stall a start; the indexer serves only the last batch's blobs
when no cursor file exists, and the `BatchPosted` log plus the DA proxy
serve them when the indexer has not reached the batch. An L1 that does not
answer at start is retried in-process with a bounded backoff: the exporter
stays up, `kardamom_batcher_resume_failures_total` counts every failure,
and the alert `KardamomBatcherResumeFailures` pages on the first. A
lying endpoint's one signal that cannot hide is the age of the last post as
L1 serves it: `kardamom_batcher_last_post_age_seconds` is read from L1 on
every probe tick, never from the batcher's memory, and
`KardamomBatcherLastPostStale` pages when it passes twice the idle flush
wait. The chaos-l1 shard proves these: `l1-liar` (a wrong block hash, a
broken parent chain, swallowed settlement logs), `l1-null-receipts` (null
receipts and swallowed logs, with a batcher restart inside the fault),
`two-day-outage` (the staging incident's order) and
`batcher-outage-past-retention` (below).

**An outage past the sealers' retention.** A batcher frozen or down for
longer than the egress retention finds, on restart, that the sealers refuse
its replay. The spool's pending group is posted first, even when the
refusal stops the reader before the group is due: the spool is the only
copy of those blocks. The range past the spool comes back through the
rebuild from references above. `batcher-outage-past-retention` freezes the
batcher until the floor passes its cursor, thaws it, and asserts the spool
posted, the gap rebuilt, and the record on L1 contiguous past the floor;
the shard's persisted-state stage then proves the rebuild from L1 through
the recovered range.

## Data-availability recovery (rebuild-from-L1)

The bottom-of-the-stack backstop: even if **every** in-cluster durable copy is
lost — the Raft log on a quorum of sealers *and* every node's `tx_ordering` /
`tx_data` archive — the L2 state is still recoverable from L1 and the DA
layer alone, because the posted payloads carry the full ordered `raw_tx`
stream.

The backstop covers what L1 holds. A range the batcher never posted is on
no L1 and in no DA store; its copies are the sealer's egress retention, the
ordering in every state database and the bytes on the `tx_data` archives
(the batcher section above). A block rebuilt from L1 carries no archive
reference: its bytes are on L1 already, and the batcher never asks for it.
The rebuild sets the `l1_rebuilt_end_tx_position` meta mark to the end of
the last rebuilt block. A `tx_hash_index` row at or below the mark stops
at the position. `kardamom_getBlockRefs` answers such a block with the
JSON-RPC error -32001 and the cause, and the batcher asks the next query
endpoint. The deep compare of two state databases compares the position
of every row. It accepts a row without a reference against a row with one
only at or below the mark of the node that keeps the shorter row. The
reference is not in the trie, so the roots of every consumer stay equal.

`kardamom-reconstruct` walks the `BatchPosted` event log, fetches each batch's
payload from the DA proxy (or the indexer's archive) by the certificate L1
committed to, decodes the KAR1 payload back into ordered blocks, and
re-executes them through the **same**
engine the live executor/validator use (`kardamom_engine::replay`) into a fresh
trie-aware state DB. Because the state root is a pure function of genesis + the
ordered transactions (receipts and canonical positions don't enter the trie),
the reconstructed root is byte-identical to the canonical one. The
`reconstruct_l1_e2e` test proves the whole loop end-to-end against a real L1
(anvil): post → discard the originals → read L1 → fetch payloads → re-execute →
assert root parity.

**The rebuilt state is resumable.** A KAR1 version 3 block carries its
canonical end index and its L1 origin, which the rest of the payload cannot
give: epoch markers and deposits take canonical slots and never reach the
payload. So the rebuilt cursor, header rows and receipt positions equal the live
chain's, and `--executor-image` writes the image an executor resumes on (the
trie, the hashed mirror and the stored root removed, after the root check).
The sealer refuses a resume whose index lies outside the block it names, so
a wrong cursor is loud. A state rebuilt through a version 2 payload is correct
and not resumable. See `docs/specs/2026-09-20-rejoin-from-l1-rebuild.md`,
which also gives the procedure for a wiped sealer set: `--sealer-seed` writes
the seed a new sealer cluster starts from at the rebuilt head (the sealer
fleet rebuild below).

Scope: L2 transactions, interop deliveries and L1 deposits. Deposits are
absent from the DA payload: a deposit is unsigned, so a payload-carried deposit
would be an unverifiable claim. With `--lockbox`, the rebuild derives them from
L1: when a block's L1 origin moves from M to N, the block leads with the epochs
M+1..N, each derived from that L1 block's lockbox logs through `derive_epoch`,
the rule the da-watcher and the validator use. Each epoch takes a marker slot
and one slot per deposit at the head of its block, as on the live stream. The
first step from origin 0 takes epoch N only: the da-watcher starts at the
finalized block it first sees. A block whose items need more slots than its
canonical range holds is refused. A smaller need is accepted, because a vacant
slot (a voided entry or its void record) never reaches the payload; a missing
deposit then shows as a root mismatch at `--expect-root`. Without `--lockbox`
the rebuild leaves deposits out, and a chain with deposits rebuilds to a wrong
root.

**A gap in the record is loud.** Every case of the chaos-l1 shard ends with
the persisted-state stage: the state at the validator's drained head is
rebuilt from L1 and the DA layer alone and must carry the validator's root,
and the harness checks that every posted batch starts at the block after
the previous one's end. A batcher that could not post a range, for any of
the faults the shard serves, fails the shard there. The followers of the
rebuild (the inbox indexer, the da-watcher) read one L1 source today: a
wrong block hash that reaches their anchor halts them until an operator
restarts the da-watcher and re-indexes the archive, and a swallowed log is
invisible to one source. The two-source followers remove both; the cases
name these assertions as deferred until then.

## Sealer fleet rebuild (every sealer wiped)

All three members lose their cluster and archive directories. No member
holds a log or a snapshot, so the canonical stream cannot continue. The
chain restarts after H, the `l2BlockEnd` of the last posted batch, and the
blocks after H are reverted: their receipts are revoked. The procedure is
`docs/runbooks/sealer-fleet-rebuild.md`:

1. `kardamom-reconstruct --through-block H --lockbox <addr>` writes the
   sealer seed, the executor image, and, in a second run, a state that keeps
   the trie for the validator. `--lockbox` puts the L1 deposits into the
   rebuilt state.
2. Every member starts from the seed (`-Dkardamom.cluster.seedSnapshot`), with
   an empty remote-origin allowlist. The cluster opens block H + 1 at index
   E_H, the canonical end of H.
3. Every executor and the validator resume on the rebuilt state at
   `(E_H, H + 1)`.
4. The sequencers start, and then the da-watcher with `--l1-resume-after M`,
   where M is the L1 origin of H. A sequencer reads the epochs live, with no
   replay, so the da-watcher must not publish before the sequencers subscribe.

Every copy of the reverted chain must go, because each one resumes or
publishes past the new stream:

| Copy | What it does if it stays |
|---|---|
| An executor's or the validator's state DB | Its cursor lies past the new head. The sealer answers `REPLAY_AHEAD` and the consumer stops (`ClusterBehindCursor`). A sealer without that answer lets the consumer drop new records below its cursor as duplicates. |
| A checkpoint (executors, validator) | A later restore or peer fetch adopts a state of the reverted chain. |
| The batcher's spool | It continues the confirmed cursor, so the batcher posts reverted blocks. |
| The account cache (Redis) | A row applies only above its stored position, and the new positions start lower. The rows stay stale. |
| A running da-watcher | It continues at its own cursor, past M. The epochs between M and that cursor never reach the new chain, and their deposits are lost. |

The da-watcher seeds its cursor at the finalized tip on every start. That
skips no epoch on a running chain only when the tip did not move while it was
down. After a seed, the chain's origin is M, far behind the tip, so the flag is
required. A da-watcher that restarts later with a stale flag sends epochs at
or below the sealer's origin; the sealer drops each one as a regression.

The validator resumes on a rebuilt state that keeps the trie, with no step of
its own. Its cursor comes from the same meta keys as an executor's, and its
verify floor is H. It needs no other file: the prover spool, the claims and
the epoch verifier start empty.

**The chaos case** `sealer-fleet-total-wipe-recover` (the fleet shard, last)
proves the procedure end to end. It stops the ingresses, lets the batcher
post everything, stops the batcher, and mines L1 blocks until one more epoch
seals: the blocks after H then hold epochs and no transaction, so the revert
takes no receipt from the load. Then it stops every job, rebuilds the state
at H twice (the executor image with the seed, and the validator's state),
and wipes every copy of the old chain. One executor keeps its old state on
purpose. The assertions:

- all three members log `sealer state SEEDED` at H and E_H, none logs
  `FRESH`, all confirm the seed and take the first snapshot; started again
  without the seed property, all restore the snapshot;
- the sealer serves the fresh executors from `(E_H, H + 1)`, and answers the
  stale executor with `REPLAY_AHEAD`; no executor restores or fetches a
  checkpoint;
- the validator commits past H on its rebuilt state;
- the da-watcher's first ticks publish every finalized L1 block after M;
- the batcher posts again, and L1's record stays contiguous from H;
- the recovery probe passes, and the end-of-shard audit compares the
  executors with the validator and rebuilds the head from L1.

The output attester is not deployed. It posts a root for every block the
validator commits, not only for posted blocks, so where it runs, L1 can hold
roots of reverted blocks. The revert rolls them back
(`revert_to_posted_head.md`, step 4). The attester then resumes after the
newest output that remains, and the validator resumes at H. The withdrawals
between the two are not collected again.

## DA-watcher

Tick-based with an in-memory cursor: any RPC or publish error leaves the
cursor unadvanced and the next tick retries the same `(cursor, tip]` range —
at-least-once within a run. A block that does not descend from the published
one raises the `l1_chain_break` halt, and an L1 that does not answer the
`l1_unreachable` halt; both clear on the next good tick. The l1-indexer's
follower halts the same way on its chain check. Duplicates after a retry or restart are absorbed
downstream by the first-seen dedup on `source_hash`. A dead watcher stalls
deposits only, and it reads *finalized* L1 blocks, so reorgs are out of scope
by construction.

**Two L1 sources.** The followers (the da-watcher and the indexer) read L1
through a set of endpoints (`--l1-rpc`, a list; `--l1-light-client-rpc`,
the light client). A block's ids and a log query are accepted when two
sources agree, or when the light client serves them. The finalized tip is
the lowest one the agreeing sources report. One public endpoint alone is
trusted only when it is the only one configured. A source that errors or
answers HTTP 429 rotates out for a backoff, and the set goes on with the
rest; with every source out, or fewer than the rule needs, the tick fails
and repeats. A disagreement the light client does not settle is a halt
every tick, with both answers in the log and in
`kardamom_l1_source_disagreement_total`; it is never resolved by a majority
of public endpoints, since two can share a backend. With a light client the
source that disagrees with it is the liar, and rotates out. The halt cause
is one typed value (`SourceHalt`: `l1_source_disagreement`,
`l1_sources_out`) that the error, the log line and the counter carry.

**A lying L1 endpoint.** The watcher chains consecutive blocks by their
parent hashes. A broken parent chain halts it at the first lying block
(`kardamom_da_watcher_tick_total{outcome="chain_break"}` moves every tick),
and it resumes by itself when the endpoint serves the chain again. A wrong
block hash is caught one block late: the lying hash is already its anchor,
and already in the epoch it published, so the halt lasts until a restart
seeds the cursor at the tip. A swallowed log is invisible to one source; two sources see it (above).
`KardamomDaWatcherTickErrors` pages on a sustained error rate. The
chaos-l1 cases `l1-liar` and `two-day-outage` serve each lie and check the
halt and the resume; the inbox indexer, which chains blocks the same way
through a persisted cursor, is checked beside it.

## Notifier

Off the hot path by construction: it reads `tx_status`, `tx_receipts` and
`tx_errors` as one more multi-destination-cast subscriber, so a dead or slow
notifier costs its own clients and nothing else. The `tx_status` publishers
(sequencer, ingress) offer best effort and never block; a back-pressured
frame is dropped after a bounded retry and counted
(`kardamom_log_best_effort_dropped_total{stream_id="1018"}`). The receipt
stream stays the truth: a client that misses a status reads the receipt.

A restart loses the in-memory ring; a WebSocket client that reconnects
replays what the new ring holds. Webhook subscriptions and their outboxes
live on disk, so a restart resumes delivery from the persisted cursor,
at least once. The two instances shard subscriptions by a rendezvous hash
of the id and both store every registration; a lost instance's shard moves
to the twin when the instance count changes.

## L1-governed upgrades (feature flags)

A feature flag is turned on by an **upgrade transaction**: an L1 call to
`ETHLockbox.initiateUpgrade`, authorized to the factory owner (a Safe in
production), which the DA-watcher derives into a system deposit that writes the
`KardamomChainState` predeploy. Because it rides the deposit path, it inherits
that path's failure modes wholesale — finalized-only reads, at-least-once
delivery, first-seen dedup — and adds no new ones. Design:
`docs/specs/2026-08-16-l1-upgrade-feature-flags-design.md`.

What matters operationally is that **mixed-version fleets fail-stop rather than
fork**, in two distinct places:

- An **old validator** (a binary whose `derive_epoch` does not know about
  `UpgradeInitiated`, running with L1 verification) halts at the epoch
  containing the upgrade — it re-derives that L1 block, sees a deposit it
  cannot account for, and reports `DepositsMismatch`. Loud and attributable,
  not a silent divergence.
- An **old executor** would apply the `setFeature` deposit fine (it is ordinary
  deposit data on the wire) but lacks the block-close hook, so a *new*
  validator's write-set comparison fails on the first active block.

Hence the rollout rule: **ship the binaries first, flip the flag second.** The
activation timestamp exists to give operators that window.

The one liveness gap is inherited from the watcher's in-memory cursor: a
watcher that restarts *after* the upgrade's L1 block finalized but before
observing it re-seeds at the current tip and skips that epoch — the same
seed-skip that affects user deposits. The runbook is therefore to confirm the
L2 receipt (keyed by the domain-1 `source_hash` of the L1 log position) before
treating an upgrade as applied; the L1 `upgradeNonce` makes a re-send
unambiguous.

Covered by the chain-semantics suite's S13a/b/c (immediate activation,
scheduled activation, and both authority gates), which assert an EXACT beacon
count per block on the executor *and* the validator — a "greater than zero"
check would pass against a feature that activated once and stopped.

## Substrate (the shared failure domain)

- **ArchivingMediaDriver** — one combined Media Driver + Archive JVM per node
  (the `aeron` Nomad system job). A driver death takes down every service on
  that node in one blow: transport *and* that node's durability recorder.
  `archive-driver-loss` kills it under ingress-0 and asserts the pipeline
  rides through (active/active), the system task restarts within the SLO
  (archive segments persist on the node volume), and the ingress job returns
  to full strength.
- **Archives** — durable `tx_ordering` (folded into the Raft log + per-member
  archive) and per-sequencer `tx_data`; they underpin executor resume and the
  batcher. `tx_data` is **already 2× node-redundant**: it is a UDP-multicast
  stream, and both ingress replicas run an archive recorder that joins the group,
  so each ingress node's archive captures *every* publisher's shard streams. The
  two archives are byte-identical (same recording ids, verified by content
  compare), which makes the peer an exact restore source. What was missing —
  and what `archive-tx-data-wipe` + `kardamom-archive-rereplicate` add — is the
  path back to full redundancy after a loss: a wiped node's archive is restored
  by file-mirroring the surviving peer's segments + catalog (rusteron-archive
  does not expose Aeron's network `replicate()`), and the restored archive
  passes Aeron's own `ArchiveTool verify`. Without it, losing one copy leaves
  the *next* loss fatal, and a volume wipe hangs the executor's
  `resolve_recording`.

  Two files must never be transplanted from a **live** source, and both bit
  the chaos suite before they were understood: `archive-mark.dat` (the live
  daemon heartbeats it, so a copy looks *active* to the destination's
  restarting Archive, which crash-loops on `active Mark file detected` until
  the copied heartbeat ages out — the recurring "aeron did not reach ≥ 8
  running" restart-SLO failure), and the catalog's per-entry checksums for
  actively-recording entries (rewritten mid-copy → torn entries that fail a
  CRC-armed verify; issue #98). `mirror_archive` therefore never copies the
  mark file — the destination daemon recreates its own — and copies the
  catalog via a **stable read** (two consecutive identical snapshots, catalog
  before segments), as does the chaos restore; the wipe case's post-restore
  verify is CRC-armed. One Aeron 1.45 caveat bounds what that gate can check:
  catalog **entry** checksums go stale when a restored archive is adopted
  (active recordings get their recovered stop positions patched in without an
  entry-checksum recompute — `ArchiveTool.verify` provably does this, and the
  adoption path shows the same signature), so the gate treats per-frame CRC32
  failures, missing files, and structural errors as fatal but tolerates —
  counting and logging — `invalid Catalog checksum` on adopted entries. Frame
  CRCs are the authoritative integrity signal. If an operator wants entry
  checksums consistent again after adoption, `ArchiveTool checksum
  io.aeron.archive.checksum.Crc32 -a` recomputes and persists them — but it
  **blesses whatever bytes are present** (verified: it happily blesses a
  deliberately corrupted descriptor), so run it only after establishing the
  content is trustworthy, never as an automated step.

  **Corruption** (present-but-wrong bytes) is covered separately
  (`archive-corruption`): the archive driver records per-data-frame **CRC32s**
  (`aeron.archive.record.checksum`) and validates them on replay
  (`aeron.archive.replay.checksum`), so a CRC-armed
  `ArchiveTool verify -a -checksum` detects a length-preserving byte flip that
  a size check cannot see. Repair is *targeted*: `kardamom-archive-rereplicate
  --diff` names exactly the segments that diverge from the mirror,
  `--heal --segments` copies only those (daemon stopped, as with the full
  mirror), and the CRC verdict arbitrates which side was corrupt — mirror
  inequality alone only proves one of them is. On the **live** path the
  executor's join-miss refetcher rotates to the mirror archive when a replay
  produces no fragments (the corrupt-recording signature once replay-side CRC
  validation is on), so a reader never stays pinned to a bad copy. The offline
  segment reader also fail-stops on structural damage (a zeroed or undersized
  frame header with data behind it is `Corruption`, no longer a silent
  truncation that read as a live tail).
- **The observation path itself** (issue #76, fixed) — `docker kill` of a
  privileged DinD node stalls host-dockerd `docker exec` runner-wide for
  minutes, blacking out every exec-based probe at once; for three days this
  masqueraded as "all executors dead" while the pipeline was healthy. Lesson
  encoded in the harness: chaos probes now hit the executors' exporters
  **directly over the cluster bridge** (`0.0.0.0:9004` bind), with exec as
  fallback, and every service's exporter runs on a dedicated thread so a
  wedged service runtime can't take `/metrics` down with it. When reading
  chaos failures, distinguish "the pipeline stalled" from "the probes went
  dark" before diagnosing.

## Known gaps (untested failure surface)

- **All-wiped fleets** — the persisted-state stage of every shard now
  rebuilds the state at the validator's drained head from L1 and the DA
  store alone (`kardamom-reconstruct --through-block --expect-root`) and
  requires the validator's committed root. No chaos case yet wipes all three
  sealers or all three executors with their checkpoints and then rejoins
  them from that rebuilt state: the executor resumes from a cluster cursor
  the rebuilt database does not carry.
- **Archive *data* loss** — total loss has the rebuild-from-L1 path (above,
  `reconstruct_l1_e2e`); single-node `tx_data` archive loss has the
  re-replicate-from-peer path (`archive-tx-data-wipe` chaos case +
  `kardamom-archive-rereplicate`); single-segment *corruption* has the
  CRC-verify + targeted-heal path (`archive-corruption` chaos case). Still
  open: `tx_ordering` archive re-replication (today it self-heals only via the
  Java cluster's Raft log replication on rejoin).
- **L1 outage** — the followers cross-check two L1 sources, and the
  batcher rebuilds a range the sealer no longer retains from the state
  databases' references and the `tx_data` archives (the batcher section).
  The `chaos-l1` shard serves the followers a lying L1 through
  `kardamom-l1-fault-proxy`: `l1-liar`, `l1-null-receipts`,
  `two-day-outage` and `batcher-outage-past-retention`. The batcher's
  resume through the lies, the stale-post alert, the recovery past the
  sealers' retention, the contiguous record and the rebuild parity are
  proven. Still open: the shard's followers read one source (the proxy),
  so the halt on a swallowed log, the disagreement counter and the
  resume by themselves after a wrong hash are not yet cases; gas spikes
  on a real L1 are not served by the proxy.
- ~~**Validator divergence injection**~~ — **CLOSED**: the chain-semantics
  suite's `s7_corrupt_bal_halts_validator` publishes a corrupt `BlockDelta`
  onto the real `tx_bal` channel (executor SIGSTOPped so nothing competes)
  and asserts the documented halt — the halting log line and the
  `validator_divergence` halt record.
  (Lapse recovery is covered by `validator-lapse`.)
- ~~**Withdrawals could never be attested**~~ — **FIXED** (found by the
  chain-semantics suite's S2 bridge round-trip). The validator's attester
  collected withdrawal leaves from the committed `BlockDelta`, but the engine
  finalizes every delta with an EMPTY receipts vec (receipts travel on
  tx_receipts instead — `PendingDelta::finalize`). So
  `collect_withdrawal_leaves` always returned nothing: every posted output
  carried `leaves=0` and committed to the empty withdrawals root, no
  `MessagePassed` leaf was ever provable, and **no withdrawal could be
  finalized on L1** — the L2→L1 half of the bridge was inert. The attester's
  unit tests passed throughout because they feed a delta that *does* carry
  receipts, a shape the live pipeline never produces. Fixed by
  `AttestingReceiptSink`, which tees leaves off the receipt stream (where the
  logs actually are) and flushes them per block boundary. Regression-tested
  end-to-end by `s2_bridge_withdrawal_round_trip`.
- ~~**The persisted `receipts` / `tx_hash_index` tables are always empty**~~
  — **CLOSED**. The executor's commit path fills the block delta's receipts
  (`exec_boundary.rs`), so the state writer populates both tables; the
  end-of-shard persisted-state audit requires a non-empty `receipts` table
  and compares it across every executor and the validator. The read path
  "`eth_getTransactionReceipt(hash)` → `get_tx_position` → `get_receipt`" is
  live: the executor's query endpoint serves it, and the ingress falls back
  to it when its own receipt cache misses. So a receipt survives an ingress
  restart, and a client that got a hash from an accepted submit finds its
  receipt afterwards. The fallback is bounded by the client's token bucket,
  the query client's in-flight bound and its timeout; a shed query answers
  `null`, the same as "not committed yet".
- ~~**Validator ignores SIGTERM**~~ — **FIXED** (found by the
  chain-semantics suite's graceful-shutdown phase). The validator survived
  90 s+ of a single SIGTERM while the executor exited immediately from the
  same shutdown shape, so Nomad SIGKILLed it on every stop/deploy. Root
  cause: `TxReceiptsSubscriberHandle` carries an `AeronRuntime` clone (for
  MDS destination churn) and the validator moved the whole handle into its
  receipts pump task — an ownership cycle, since the runtime shuts down only
  when its last clone drops, that shutdown is what ends `recv()`, and the
  pump was holding the clone that prevented it. `drop(rt)` in `main` became a
  no-op, the engine's tx_data subscriptions never closed, and the join never
  returned. Fixed by `TxReceiptsSubscriberHandle::into_receiver()` (drops the
  clone, keeps the receiver), applied in the validator and pre-emptively in
  the ingress, which had the same shape masked by `main` returning without a
  join. Regression-tested by the suite's graceful shutdown (20 s bound).
- **Bridge / injection / DA-parity cases are Target-L only** — the
  chain-semantics `semantics` shard runs nonce ordering, RPC liveness and
  validator/executor consistency against the real cluster, but S1/S2
  (bridge) still need a contract-deploy phase, the message-passer predeploy
  and attester vars in the shared bring-up path; S7 (corrupt-BAL injection)
  and S9b (SIGKILL recovery) drive process signals and raw Aeron publications
  that are unreachable from outside the cluster; and S8 (DA parity) waits on
  the batcher's live posting being rewired for the cluster topology. Those
  guarantees are proven on a real pipeline, just not yet on the deployed one.
- **`archive-tx-data-wipe`'s restart SLO looks too tight** — the case
  regularly fails with `aeron did not reach >= 8 running ... within 60s
  (have 7)` after the destructive wipe, on runs that are otherwise green
  (observed on main both before and after the semantics work, and passing on
  re-run). While that shard is red, real regressions behind it are invisible.
- **Load-harness scrapes still ride `docker exec`** — the chaos *probes*
  moved to direct HTTP (issue #76), but `kardamom-load --metrics-via-docker`
  remains the default; a runner-wide exec stall can still degrade its
  keep-pace verdicts (chaos-mode leniency masks it today).

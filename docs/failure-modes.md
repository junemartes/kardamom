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
  What this does not cover: all three members *wiped*. The failure model
  owns no in-cluster recovery for that; it is the rebuild-from-L1 backstop
  below.

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
  genesis re-sync; the `DedupWindow` absorbs any reconnect overlap.
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
  executor image from L1 and the DA store on the host
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

Exit codes keep the two halt classes distinguishable: **exit 2 is reserved
for a proven divergence** (the latch records the reason before the engine
surfaces it) — the page-the-humans signal. Every other engine failure — a
stream error, or a replay-window overrun (`REPLAY_UNAVAILABLE`, the validator
cursor aged out of the cluster's bounded retention) — exits 1: an
availability problem, restartable, never to be confused with an integrity
one.

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
real dependencies are cluster replay retention (an aged-out cursor is a
fail-stop: unpostable ordering is a permanent DA gap and must be loud) and
L1 gas/RPC health. See `docs/agents/batcher-live-l1-spec.md`.

Each batch is a real EIP-4844 blob transaction to `KardamomL2Settlement`: L1
records the ordering + KZG versioned hashes, and the blob **bytes** are
written to the DA store keyed by versioned hash (mirroring the
EL-holds-commitments / DA-layer-holds-bytes split, since blob sidecars are
pruned by the consensus layer after ~18 days). The offline segment-file mode
(`--channel-b-segment`, dry-run by default) remains for archive inspection
and the corruption-heal tooling.

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

## Data-availability recovery (rebuild-from-L1)

The bottom-of-the-stack backstop: even if **every** in-cluster durable copy is
lost — the Raft log on a quorum of sealers *and* every node's `tx_ordering` /
`tx_data` archive — the L2 state is still recoverable from L1 alone, because the
posted blobs carry the full ordered `raw_tx` stream.

`kardamom-reconstruct` walks the `BatchPosted` event log, fetches each batch's
blobs from the DA store by the versioned hashes L1 committed to, decodes the
KAR1 payload back into ordered blocks, and re-executes them through the **same**
engine the live executor/validator use (`kardamom_engine::replay`) into a fresh
trie-aware state DB. Because the state root is a pure function of genesis + the
ordered transactions (receipts and canonical positions don't enter the trie),
the reconstructed root is byte-identical to the canonical one. The
`reconstruct_l1_e2e` test proves the whole loop end-to-end against a real L1
(anvil): post → discard the originals → read L1 → fetch blobs → re-execute →
assert root parity.

**The rebuilt state is resumable.** A KAR1 version 3 block carries its
canonical end index and its L1 origin, which the rest of the payload cannot
give: epoch markers and deposits take canonical slots and never reach the
blob. So the rebuilt cursor, header rows and receipt positions equal the live
chain's, and `--executor-image` writes the image an executor resumes on (the
trie, the hashed mirror and the stored root removed, after the root check).
The sealer refuses a resume whose index lies outside the block it names, so
a wrong cursor is loud. A state rebuilt through a version 2 blob is correct
and not resumable. See `docs/specs/2026-09-20-rejoin-from-l1-rebuild.md`,
which also gives the flag-day procedure for a wiped sealer set and the seed
hook that would replace it.

Scope: L2 transactions. Deposits are absent from the DA payload (the batcher
skips `DepositRef`s) but are independently re-derivable from L1 `DepositInitiated`
events via the `da_watcher` path — interleaving them into the reconstruction is
a documented follow-up, so a deposit-bearing range currently reconstructs its
non-deposit state exactly and is flagged rather than silently diverging.

## DA-watcher

Tick-based with an in-memory cursor: any RPC or publish error leaves the
cursor unadvanced and the next tick retries the same `(cursor, tip]` range —
at-least-once within a run. Duplicates after a retry or restart are absorbed
downstream by the first-seen dedup on `source_hash`. A dead watcher stalls
deposits only, and it reads *finalized* L1 blocks, so reorgs are out of scope
by construction.

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
- **Deposit interleaving in reconstruction** — rebuild-from-L1 covers L2
  transactions; re-deriving L1 deposits from `DepositInitiated` events and
  interleaving them in canonical order is a follow-up.
- **L1 outage** — the batcher's behavior under sustained L1 RPC failure /
  gas spikes is designed (lag + catch-up) but not chaos-tested.
- ~~**Validator divergence injection**~~ — **CLOSED**: the chain-semantics
  suite's `s7_corrupt_bal_halts_validator` publishes a corrupt `BlockDelta`
  onto the real `tx_bal` channel (executor SIGSTOPped so nothing competes)
  and asserts the documented fail-stop — the halting log line and exit 2.
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

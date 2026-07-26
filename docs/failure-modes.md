# Failure modes

How each kardamom actor fails, what it costs, how it recovers, and where that
behavior is verified. Grounded in the failover specs (`docs/agents/`), the
chaos suite (`deploy/cluster/scripts/chaos.sh` — case names appear like
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
  then the backlog drains gaplessly.

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
  `<state_dir>/stale/`, restores, and resumes from the checkpoint's cursor
  (`kardamom_executor_resync_total` counts these by outcome). If no peer can
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
one. The validator has no peer to fetch a checkpoint from (it is a singleton
with a trie-bearing state env), so a resync-required validator is rebuilt
from L1 (`kardamom-reconstruct`) or from an operator-restored state copy.

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

## Batcher (offline, archive-driven)

A crash costs **DA freshness only** — L2 keeps sequencing and executing.
Because it reads the canonical stream from the durable Aeron archives rather
than live channels, it can restart late and catch up; the failure mode is a
growing L1-posting lag, not data loss. Its real dependencies are archive
availability and L1 gas/RPC health.

When live posting is enabled (`--dry-run=false` with `--l1-rpc`, `--l1-key`,
`--settlement`, `--da-store`) it broadcasts each batch as a real EIP-4844 blob
transaction to `KardamomL2Settlement`: L1 records the ordering + KZG versioned
hashes, and the blob **bytes** are written to the DA store keyed by versioned
hash (mirroring the EL-holds-commitments / DA-layer-holds-bytes split, since
blob sidecars are pruned by the consensus layer after ~18 days).

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
  mark file — the destination daemon recreates its own — and the catalog copy
  from a live source remains a known gap until #98 lands.

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
- **The persisted `receipts` / `tx_hash_index` tables are always empty** —
  same root cause, still open. Because `BlockDelta.receipts` is always empty,
  the state writer never populates either table, so
  `StateDatabase::{get_receipt, get_tx_position}` can only ever return `None`
  and the documented "`eth_getTransactionReceipt(hash)` → `get_tx_position` →
  `get_receipt`" read path cannot work. Receipts survive only in the ingress's
  in-RAM cache, so a restart loses them. Populating the delta's receipts on
  the executor's commit path would fix it, but it adds a per-tx clone to the
  hot path — worth its own PR with a saturation run.
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

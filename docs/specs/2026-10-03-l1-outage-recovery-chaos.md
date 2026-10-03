# L1 outage recovery: the chaos cases and what they require

Status: designed, not built. Grounded in the staging incident of 2026-10-01 to 2026-10-03.

## 1. The incident, as a sequence

1. The public L1 endpoint served a wrong block hash for one Sepolia block, null receipts for
   whole blocks, and no `BatchPosted` logs for the settlement. Every follower read that one
   endpoint.
2. The indexer's chain check caught the lie at block 11818710 and halted, loudly in its log
   and in a metric nobody watched. The da-watcher halted the same way near the head.
3. A deploy restarted the batcher. At start it asks the indexer for the last posted batch,
   and the indexer was behind, so the batcher fail-stopped. It restarted 447 times over two
   days. Nomad counted the job as running.
4. The sealer's egress retention floor passed the batcher's cursor. The spool (#471) was
   deployed during the outage and held nothing. L2 blocks 18375 to 56073 are not on L1 and
   not in any in-cluster copy: the ordering lives in the sealer's egress retention and on
   L1, nowhere else. The state database keeps headers, receipts and a hash index, not the
   ordered transactions.
5. A second deploy restarted everything against the same endpoint and reproduced the halt.

Two days passed because no mechanism could act: no second source to route around the lie,
no alert on the one unambiguous signal (the age of the last post on L1), and no copy of
the ordering to recover from once retention ran out. The chain kept sealing and executing,
so every L2 signal stayed green.

## 2. What "always recover" means

After any combination of these faults, the batcher posts a contiguous DA record to L1
without an operator's hand, as long as one of the following holds: one executor's or the
validator's database survived, or L1 holds the range already. The faults:

- an L1 endpoint that lies (wrong hash, broken parent chain, missing logs or receipts) or
  rate-limits or is down, for any length of time;
- the batcher down, frozen or crash-looping for longer than the sealer's retention;
- a deploy in the middle of either;
- all of the above at once, in the incident's order.

And the operator learns about the fault within minutes, not days.

## 3. Design

### 3.1 The lost range is rebuilt from what the nodes already keep

The ordering and the bytes both survive a retention overrun; what is missing is the link
between them.

- The executors' and the validator's state database holds the ordering: the `headers`
  table maps a block to its canonical end position and its L1 origin, and the `receipts`
  table holds one row per canonical position with the transaction's hash. Four copies, on
  four nodes.
- The `tx_data` archives hold every raw transaction the ingress accepted, durably, on both
  ingress nodes, byte-identical. The engine's join-miss refetch already reads a transaction
  from them by its `TxRef` (shard id and archive position).
- The egress record carried the `TxRef` of each transaction, and the state database did not
  keep it. That is the link the batcher lacks once the egress is gone.

So the state writer keeps the `TxRef` beside the position: the `tx_hash_index` row gains
the shard id and the archive position (9 bytes), written with the receipt, off the
execution thread. The executor's query endpoint and the validator serve
`kardamom_getBlockRefs(number)`: the block's canonical end position, its L1 origin, and the
ordered list of `(tx_hash, shard_id, position)`. Deposits are not in the list; a payload
never carries them.

The batcher's resume gains a third source after the spool and the sealer: when the sealer
answers `REPLAY_UNAVAILABLE` past the cursor, the batcher reads each missing block's refs
from an executor or the validator, fetches the bytes from the `tx_data` archive through the
same refetch the engine uses, checks each hash against its bytes and each block's end
position against the previous block's, packs the KAR1 blocks exactly as the live path does,
fills its spool, and continues. Retention is then a latency, not a loss, while one state
database and one archive survive. No new table of bytes, no new write on the hot path
beyond nine bytes per transaction.

The sealer's Raft log also holds the ordering past the egress window; an export from it is
a heavier alternative and stays a follow-up, since the state database is the copy every
node already keeps and serves.

### 3.2 Two L1 sources, cross-checked, with rotation

The followers (indexer, da-watcher, batcher) take a list of L1 endpoints, not one. Rules:

- A block's hash is accepted when two sources agree, or when the light client serves it
  inside its window. One source alone is used only for data the chain commits to elsewhere
  and checks (a payload against its certificate, a log against the receipt root).
- A source that errors, rate-limits (429) or disagrees is marked and rotated out for a
  backoff. The follower keeps going on the rest. With every source out, it halts loudly, as
  today.
- A disagreement is a halt with both answers in the log and a counter
  (`kardamom_l1_source_disagreement_total`). It is never resolved by majority of public
  endpoints, because two public endpoints can share a backend.

The indexer's chain check stays: it caught the lie. What changes is that a caught lie no
longer stops the chain's DA, because the batcher's resume does not depend on the indexer
being current (3.3).

### 3.3 The batcher's resume does not wait on the indexer

Today the batcher at start reads `lastBatchIndex` from the contract and waits for the
indexer to hold that batch, so it can learn the L2 block the batch ended at. The contract
has that number: `postBatch` stores `l2BlockEnd` with the index. The batcher reads it from
the contract through its L1 sources and never waits on the indexer. The indexer is for the
rebuild and the archive, not for the batcher's cursor.

### 3.4 The alerts that would have paged

| Metric | Rule |
|---|---|
| `kardamom_batcher_last_post_age_seconds` (from L1, not from the batcher's memory) | over `2 × idle_flush` for 10 min |
| `kardamom_l1_indexer_tick_total{outcome="error"}`, `da_watcher` likewise | error rate over 50% for 15 min |
| `kardamom_batcher_resume_failures_total` | any increase |
| `kardamom_l1_source_disagreement_total` | any increase |

The rules go into the monitoring job's Alertmanager config (infra #47) and the staging
deploy's smoke verifies they load.

### 3.5 Readiness

The rolling-deploys spec's `/ready` routes fail on a halted follower and on a batcher that
has not posted within its budget, so a deploy stops at the first halted service instead of
restarting the rest against the same fault.

### 3.6 Every halt is a state with a cause and a recovery

A service that cannot continue safely halts: it stops making progress, keeps its state
intact, keeps serving its metrics and its query endpoints, fails its readiness check, and
says why and what to do. It does not exit, because an exit loses the cause and invites a
restart against the same fault. The halt is one shared type in `kardamom_obs`:

```
Halt {
    cause:    HaltCause,      // a stable id: l1_source_disagreement, l1_chain_break,
                              // replay_unavailable, da_lag, validator_divergence, ...
    detail:   String,         // the numbers: the block, the two hashes, the cursor and the floor
    recovery: RecoveryId,     // names docs/runbooks/<id>.md: the steps, in order
    since:    Timestamp,
    clears:   Auto | Operator // whether the service resumes by itself when the cause goes
}
```

What it drives:

- the gauge `kardamom_halt{service, cause, recovery}` = 1 while halted, and a `/halt`
  route beside `/metrics` and `/ready` that serves the whole record as JSON;
- one Alertmanager rule per cause, whose annotation carries the cause, the detail and the
  runbook link; the alert text is the recovery procedure, not a metric name;
- `clears = Auto` halts resume on their own when the cause clears (an L1 source comes
  back, the floor is reachable again); `Operator` halts resume on `kardamom-admin clear
  <service>` after the runbook's steps, as the validator's verdict does.

The runbooks live in `docs/runbooks/`, one per `RecoveryId`, and a test asserts every
`RecoveryId` has one. The chaos cases assert the halt record, not only the log line.

### 3.7 The sealer halts before the chain becomes unrecoverable

The two-day loss happened because the sealer kept sealing while nothing posted. The
sealer gains a DA-lag guard: the batcher publishes its confirmed cursor (the last L2 block
on L1) on the cluster's ingress as a system record; the sealer refuses new transactions
when the sealed head is more than `da_lag_budget` blocks past that cursor, and the ingress
answers them with a typed error (`chain halted: DA lag; cause and recovery at /halt`).
Deposits and boundaries continue, so the chain's L1 view stays current. The budget sits
well inside what the payload store and the retention keep, so a halt of this kind is
always inside the recoverable set. When the batcher posts again, the sealer resumes by
itself (`clears = Auto`).

The budget is a chain value with a deploy default (`DA_LAG_BUDGET_BLOCKS`, 10,000: about
three hours at one block a second). A chain that would rather stay live and risk the loss
sets it to zero to turn the guard off; that is a choice, made in the open.

### 3.8 The fork: revert to the posted head as the last resort

The rollup answer to lost unposted blocks is a fork: the canonical chain is what L1
derives, everything past the last posted block is unsafe, and on loss the chain reverts to
the posted head. Kardamom has the pieces: the recovery lines of
`docs/specs/2026-09-07-recovery-lines-and-l1-rollback.md` define a consistent cut and the
L1 rollback; a rebuild from L1 gives the state at the posted head; the sealer's resume
checks refuse a cursor that does not name a point of the stream.

It is the last resort and not the first, because it revokes receipts: every transaction in
the reverted range was confirmed to its sender and is undone. On the staging incident
that is 38,000 blocks. So the order is: 3.7 halts the chain while everything is still
recoverable; 3.1 recovers the range from a surviving copy; the revert runs only when no
copy survives, with the operator's word, through the recovery-line procedure, and it is a
chaos case so the procedure is proven and timed.

Clients see the distinction today only through receipts. The ingress gains the `safe` and
`finalized` block tags: `safe` is the last block posted to L1, `finalized` the last block
whose batch L1 finalized. A client that cannot accept a revert waits for `safe`.

### 3.9 The snapshot set: the whole cluster's state, archives included

Every durable thing in the cluster is append-only or checkpointable, so a copy of all of
them at one time is a restorable whole. The set:

| Piece | Source | How the copy is taken |
|---|---|---|
| the sealer's Raft log and snapshots | any member | `ClusterBackup` (on the classpath; design B of `docs/specs/2026-09-12-cluster-log-purge-and-seed.md`) as a sidecar on the aux node, following the live cluster |
| the `tx_data` archives | either ingress node | the segment mirror of `kardamom-archive-rereplicate`, continuous; sealed segments never change, the catalog is copied at the cut |
| the executors' and validator's state | each node | the checkpoint the node already serves over its checkpoint port |
| the batcher's spool and cursor, the indexer's store, the DA stand-in's files | their nodes | file copies |
| the manifest | the backup job | every piece's position at the cut, the images manifest, the chain values |

The order of the cut gives consistency without a global stop: the sealer's snapshot is
taken first and the consumers' checkpoints after it, so every consumer's position is at or
past the restored sealer's replay floor and replays forward from the restored log; the
archive copy is taken last, so it covers everything the log names. A restore is a launch
from the set: `ClusterTool.seedRecordingLogFromSnapshot` seeds each member, the archives
and checkpoints land on their volumes, and the chain continues from the cut. The batcher
resumes from its cursor against L1, which is the one copy outside the set.

The backed-up Raft log also holds the ordering past the live egress window, which makes it
a second rebuild source for a lost range beside the block refs of 3.1.

A storage-level snapshot of each volume (the provider's) is the cheap tier below this: it
is crash-consistent per volume, the Aeron catalogs and the state database recover from
that on start, and the same order rule applies. It is not coordinated and not tested until
the chaos case runs it.

## 4. The chaos cases

All run on the container cluster, in a new shard `chaos-l1` (the retention knobs of the
`chaos-retention` shard apply). They need one new piece: an L1 fault proxy in front of the
in-cluster anvil, deployable as a job on the aux node. The e2e harness has the mock
(`crates/e2e/src/harness/l1_verified.rs`, `Fault`); it becomes a binary,
`kardamom-l1-fault-proxy`, with a control endpoint the chaos harness drives, and gains the
faults the incident showed: `NullReceipts`, `SwallowLogs` for an address, `RateLimit`,
`Down`.

| Case | Injection | Assertion |
|---|---|---|
| `l1-liar` | the proxy serves `WrongBlockHash` at a block, then `BrokenParentChain`, then `SwallowLogs` for the settlement, each for 10 min of chain time | the indexer and da-watcher halt within 3 ticks with the disagreement counter up; the batcher keeps posting through the second source; the alert rule fires; after the fault clears the halted followers resume by themselves and the archive is complete |
| `l1-null-receipts` | the proxy answers null receipts and empty logs for the settlement while serving blocks | same as above; in addition the batcher's resume after a restart reads `l2BlockEnd` from the contract and continues, with no wait on the indexer |
| `batcher-outage-past-retention` | SIGSTOP the batcher; load until the sealer's floor passes its cursor and a snapshot lands; thaw. Then repeat with the spool wiped | the batcher recovers the range from the spool, then from an executor's block refs and the `tx_data` archive; L1's record is contiguous (`l2BlockStart == previous l2BlockEnd + 1` for every batch); the rebuild stage proves root parity through the recovered range |
| `two-day-outage` | the incident's order: liar at T0; batcher restart at T1; a deploy of the same images at T2; floor passes at T3; fault cleared at T4 | no manual step; the batcher is posting again within one flush after T4; the alert fired before T1; the record is contiguous; rebuild parity holds |
| `da-lag-halt` | the batcher frozen with SIGSTOP; load until the sealed head passes `da_lag_budget` past the posted head | the sealer halts new transactions with the typed error; deposits still land; `kardamom_halt{cause="da_lag"}` is 1 with the runbook id; the batcher thaws, posts, and the sealer resumes with no operator step |
| `restore-from-snapshot-set` | take a set under load; then wipe every node's state, archives and cluster dirs; restore from the set | the chain continues from the cut with the same images; the executors' roots match the validator's; the batcher posts a contiguous record; the time to restore is reported |
| `revert-to-posted-head` | the batcher frozen past the retention floor, the spool, every state database's refs and both `tx_data` archives wiped (the unrecoverable case) | the services halt with `replay_unavailable` and the runbook id; the operator procedure of 3.8 (scripted in the case) reverts the chain to the posted head; the rebuilt state matches L1; the chain seals again from there; the revoked receipts are listed |

Each case ends with the shard's persisted-state stage (rebuild-from-L1 against the
validator's root), so a silent gap in the record fails the shard.

A fifth proof is the deploy isolation test: a job set with the proxy and two L1 endpoints
renders and validates.

## 5. Order and estimate

| Step | Work | Proof |
|---|---|---|
| 1 | the batcher reads `l2BlockEnd` from the contract (3.3); the alert metrics and rules (3.4) | unit tests; `l1-null-receipts` passes its resume assertion |
| 2 | the fault proxy binary and job; the `chaos-l1` shard with `l1-liar` and `l1-null-receipts` | the cases reproduce the incident on main and go green with step 1 and 3 |
| 3 | two L1 sources with cross-check and rotation (3.2) in the follower crates | `l1-liar`'s "keeps posting" assertion |
| 4 | the `TxRef` in the state database, `kardamom_getBlockRefs`, the batcher's third resume source through the archive refetch (3.1) | `batcher-outage-past-retention` |
| 5 | `two-day-outage` composite; failure-modes.md updated; the known gap "L1 outage" closed | the shard green on two runs |
| 6 | the halt contract (3.6): the type, the gauge, the `/halt` route, the runbooks, the rules; every existing fail-stop (validator verdict, batcher resume, indexer chain break, da-watcher) becomes a halt | a test that every `RecoveryId` has a runbook; the chaos cases assert the halt record |
| 7 | the DA-lag guard (3.7) and the `safe`/`finalized` tags | `da-lag-halt` |
| 8 | the revert procedure (3.8) scripted and timed | `revert-to-posted-head` |
| 9 | the snapshot set (3.9): the backup job with `ClusterBackup`, the archive mirror and the checkpoints; `just restore <env> <set>` | `restore-from-snapshot-set` |

Steps 1 and 2 land first: they turn the incident into a red test. Steps 3 and 4 make it
green. Step 5 proves the combination.

## 6. Decided

- A lost range is rebuilt from the state database (the ordering, four copies) and the
  `tx_data` archives (the bytes, two copies), joined by the `TxRef` the state writer now
  keeps. No new store of bytes: the nodes already hold everything but the link.
- A lie is never outvoted by public endpoints. Two sources agreeing is the bar; the light
  client is the tie-breaker inside its window.
- The batcher never waits on the indexer. The contract is the truth for the cursor.
- A halt is a state, never an exit. Every halt names its cause and its runbook, in the
  alert text.
- The chain halts before it can lose a block it has confirmed. A fork to the posted head
  is the last resort, gated by the operator, and proven by a chaos case.

## 7. Open questions

1. The `tx_data` archives grow without bound today. A pruning rule that keeps every
   transaction up to the L1-finalized batch plus a floor is a follow-up; until then the
   rebuild source is complete.
2. Whether the da-watcher's published epochs need the same two-source rule before
   publication. Today a wrong hash reaches the chain's L1-origin records (the staging chain
   carries such records now). The rule in 3.2 covers it when the da-watcher is a follower
   under the same source set.

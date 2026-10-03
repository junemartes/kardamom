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

### 3.1 A durable copy of the ordering: the block payload store

The executors and the validator see every block's ordered `raw_tx` bytes when they execute
it. They keep them. The state crate gains a `block_payloads` table: block number to the
KAR1 block the batcher would pack (the same encoder the batcher uses, so the bytes are the
posted bytes). The state writer thread writes it with the block's header row, off the
execution thread, so the hot path pays one append per block.

The executor's query endpoint and the validator serve `kardamom_getBlockPayload(number)`.
The batcher's resume gains a third source after the spool and the sealer: when the sealer
answers `REPLAY_UNAVAILABLE` past the cursor, the batcher reads the missing range from an
executor or the validator, checks each block's canonical end index and parent linkage,
fills its spool with it, and continues. Retention is then a latency, not a loss, while one
database survives. The rebuild-from-L1 path stays the backstop for the rest.

The store is bounded: blocks L1 covers are pruned after finality, by the same cursor the
indexer keeps, with a floor of `N` blocks for the batcher's catch-up.

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
| `batcher-outage-past-retention` | SIGSTOP the batcher; load until the sealer's floor passes its cursor and a snapshot lands; thaw. Then repeat with the spool wiped | the batcher recovers the range from the spool, then from an executor's payload store; L1's record is contiguous (`l2BlockStart == previous l2BlockEnd + 1` for every batch); the rebuild stage proves root parity through the recovered range |
| `two-day-outage` | the incident's order: liar at T0; batcher restart at T1; a deploy of the same images at T2; floor passes at T3; fault cleared at T4 | no manual step; the batcher is posting again within one flush after T4; the alert fired before T1; the record is contiguous; rebuild parity holds |

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
| 4 | the block payload store, the query method, the batcher's third resume source (3.1) | `batcher-outage-past-retention` |
| 5 | `two-day-outage` composite; failure-modes.md updated; the known gap "L1 outage" closed | the shard green on two runs |

Steps 1 and 2 land first: they turn the incident into a red test. Steps 3 and 4 make it
green. Step 5 proves the combination.

## 6. Decided

- The ordering's durable copy lives in the state database of the executors and the
  validator, not in a new service: they hold the bytes at the moment they execute them, the
  write is off the execution thread, and three executors plus the validator are four copies
  on four nodes.
- A lie is never outvoted by public endpoints. Two sources agreeing is the bar; the light
  client is the tie-breaker inside its window.
- The batcher never waits on the indexer. The contract is the truth for the cursor.

## 7. Open questions

1. The payload store's pruning floor: blocks L1 covers can go after finality, but a
   rebuild of a recent range is faster from the store than from the DA layer. A default of
   100,000 blocks (about two days at one block a second) and a deploy value.
2. Whether the da-watcher's published epochs need the same two-source rule before
   publication. Today a wrong hash reaches the chain's L1-origin records (the staging chain
   carries such records now). The rule in 3.2 covers it when the da-watcher is a follower
   under the same source set.

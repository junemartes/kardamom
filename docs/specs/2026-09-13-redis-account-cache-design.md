# Account State and a Receipt Index in Redis — Design

**Date:** 2026-09-13
**Status:** Draft for owner review. No code yet.
**Scope:** A Redis projection of the account state (nonce, balance) and a shared receipt
index. The ingress and the sequencer read it with low latency. mdbx on the executors stays
the source of truth. mdbx can always rebuild Redis.

---

## 1. Motivation

The ingress and the sequencer have no shared view of the account state.

- The ingress admits a transaction with no nonce check and no balance check
  (`crates/ingress/src/proxy/submit.rs`, `validate_submission`).
- `eth_getTransactionCount` and `eth_getBalance` return a "deferred" error
  (`crates/ingress/src/json_rpc.rs`).
- The sequencer learns a cold sender's nonce with one JSON-RPC call to an executor
  (`crates/sequencer/src/lookup.rs`, `crates/state/src/nonce_query.rs`). Each call opens an
  mdbx snapshot.
- Every ingress replica keeps its receipt cache in RAM and loses it on a restart.

The ingress and the sequencer are stateless and scale horizontally today. Redis does not add
scale. It adds capability:

1. Admission checks at the door. The ingress rejects nonce-too-low and insufficient funds
   before the sequencer and the sealer see the transaction.
2. The ingress serves `eth_getTransactionCount` and `eth_getBalance`.
3. The sequencer's cold-sender lookup becomes one GET, not an mdbx snapshot per call.
4. Retry answers survive an ingress restart and are shared across replicas.

The costs are these. Redis is the first external stateful dependency on the admission path.
A cold sender pays one network round trip (100 to 300 µs in the data center). Receipts get a
new trust root. The projection must be repaired, not just discarded.

## 2. Goals / Non-goals

**Goals**

- G1. A reader sees a sender's nonce and balance within milliseconds of the transaction that
  changed them, not at the next block boundary.
- G2. A value a reader trusts never leads the canonical truth. Stale is safe. False-fresh is
  not.
- G3. A Redis outage degrades the readers to today's behaviour. It never stalls the pipeline.
- G4. mdbx rebuilds Redis without a replay of history.
- G5. No dynamic dispatch, no mutex around a connection, actor-state structs, drop-driven
  shutdown (`docs/STYLE.md`).

**Non-goals**

- A signature cache. See section 3, decision D1.
- Storage slots or code in Redis. Only account nonce and balance.
- A change to the BAL (`tx_bal`). It stays a per-block validator artifact.
- A change to the `Receipt` type or the mdbx receipts table.

## 3. Key decisions

- **D1. No signature cache.** The ingress is the only component that computes `sender`
  (`crates/ingress/src/sig_verify.rs`). The live executor does not re-verify, by decision
  (`crates/executor/src/bin/kardamom-executor/main.rs`). A poisoned `sig:<tx_hash>` entry
  would put an attacker-chosen sender into a canonical envelope. The validator halts after
  the fact, and there is no L2 reorg. The cache also loses on cost: one round trip per submit
  to save 42 µs of CPU on rare retries. A shared receipt index replaces it. The identity
  guard in `submit.rs` makes a poisoned receipt fail closed to `Duplicate`.
- **D2. The freshness feed is the receipt batch frame.** The `tx_receipts` frame gains the
  merged post-batch rows of every account the batch touched. See 5.1 for the options.
- **D3. Three read layers.** An in-process `LiveAccounts` map fed by the batch rows, then
  Redis on a miss, then the executor query on a Redis miss. A warm sender never touches the
  network.
- **D4. The balance check rejects directly from a fresh cache entry.** The owner accepts the
  false-reject class this opens (section 7).
- **D5. Redis primary, replica and Sentinel from day one.** No persistence. The mirror
  rebuilds from the newest executor checkpoint.
- **D6. One monotone write rule everywhere.** A row applies only if its `end_tx_idx` is
  greater than the stored one. The same rule runs in the local layer, in the Redis Lua
  script, and in the rebuild.

## 4. Flow overview

```text
executors (3) --tx_receipts (ReceiptBatch: receipts + account rows)--> kardamom-state-mirror (3)
                      |                                                    | one pipeline per batch
                      |                                                    | Lua: set if end_tx_idx >
                      |                                                    v
                      |                                  Redis primary <-> replica   Sentinel x3
                      v                                                    ^
   ingress (2):   LiveAccounts -> Redis on miss -> executor query on miss
   sequencer (4): LiveAccounts -> Redis on miss -> executor query on miss
   mirror --rebuild: scan the newest executor checkpoint; live rows win by >
```

### 4.1 Propagation latency

| Stage | Normal case | Bound |
|---|---|---|
| tx executes, receipt batch on `tx_receipts` | < 1 ms | must-deliver retry |
| tx executes, block boundary (a BAL is per block) | 0 to 250 ms | the sealer's boundary cadence |
| boundary, `BalFrame` on the wire | 1 to 5 ms | `PUBLISH_DEADLINE` 500 ms; a full handoff channel drops the frame |
| batch on the wire, mirror, Redis pipeline, `WAIT`, head | 2 to 20 ms | the client timeout; seconds during a Sentinel failover |

Redis has no hard propagation bound. The local layer is always at least as fresh as Redis,
because the mirror is one more hop from the same stream. Both apply the same monotone rule,
so they converge. The batch rows remove the 0 to 250 ms boundary lag that a BAL-fed design
would keep.

## 5. Detailed design

### 5.1 The feed: account rows on the receipt batch frame

Three options were weighed.

| Option | Coverage | Size | Verdict |
|---|---|---|---|
| Sender post-state fields on the receipt | sender only; a recipient or a deposit-funded account stays stale until the boundary | 40 B fixed | partial |
| Sub-block BAL frames | complete | fat (storage, code) | breaks the validator's per-block claim index and the wire-identical merge contract |
| Account rows on the receipt batch frame | complete, per batch, delivered with the receipts | 60 B per touched account, once per batch; bounded by gas like `logs` | **chosen** |

The chosen option costs no execution work. `Executor::execute_tx`
(`crates/exec-core/src/executor/scope.rs`) returns the receipt and the write set together.
The write set already holds every touched account's post-state. A receipt batch is already
one wire frame (`Vec<Receipt>`, `crates/log/src/aeron_live/handles/tx_receipts.rs`). The
batch is adaptive: it holds what queued during the previous publish, so at a low rate a batch
is one receipt and nothing waits (`crates/engine/src/actor/commit_thread.rs`). A fixed
emission frequency is not needed. It would add its period to every client acknowledgement.
A `Receipt` field was considered: 26 files build a `Receipt` literal, against about six
sites that touch the batch frame.

**Wire type** (`crates/types/src/receipt.rs`):

```rust
pub struct AccountRow { pub address: Address, pub nonce: u64, pub balance: U256 }

/// One transaction's receipt with the rows its write set produced.
/// The exec thread builds one per transaction.
pub struct ReceiptRows { pub receipt: Receipt, pub accounts: Vec<AccountRow> }

pub struct ReceiptBatch {
    pub receipts: Vec<Receipt>,
    /// Merged post-batch value of every account the batch touched,
    /// in address order. Last write wins in transaction order.
    pub accounts: Vec<AccountRow>,
}
// The end position is the last receipt's `tx_idx` (`end_tx_idx()`).
// A batch with no receipts is never published.
```

**Data flow.** The exec thread sends `ExecToCommit::Receipt(ReceiptRows)`, with the rows of
the transaction's own write set (`crates/engine/src/actor/exec_records.rs`). The commit thread
batches these items as today. The executor's live publisher merges the items' rows into the
frame at the wire edge (`ReceiptBatch::merge`), so the validator's sink still sees per-tx
rows. A skip receipt adds no rows. A deposit adds the funded account's row. The batching
policy does not change: adaptive, no timer, `RECEIPT_BATCH_MAX = 64`. The live publisher is
all-or-nothing: a retry republishes the whole batch with its rows, and a reader that already
applied them sees `==` and discards.

**Subscriber.** The receiver keeps its per-receipt `recv`, which fans a frame out and drops
the rows, so the ingress and the sequencer compile unchanged. A consumer that wants the rows
(the validator pump, later the mirror and the readers' local layer) calls `recv_batch` and
gets the whole frame. A receiver must use one of the two, never both.

**Validator.** The pump buffers each frame's rows until the validator has executed through
`end_tx_idx`, then checks each row against its own post-state at that position
(`crates/validator/src/seams.rs`, next to the receipt cross-check). A forged row is a
divergence and halts the validator. This check is the defence against a forged or divergent
row; nothing in the readers or the mirror can tell a wrong row from a right one.

**Bandwidth.** About 60 B per touched account per batch. Under load a 64-receipt batch dedups
the fee sink and hot accounts. Measure on the load shard and record the number here. The
`FEED_CAPACITY` and `receipt_cache_capacity` horizons count receipts, not bytes, so they hold.

### 5.2 The local layer: `LiveAccounts`

One struct in `kardamom-cache`, owned by the ingress proxy and by each sequencer partition.

- A bounded map `Address -> {nonce, balance, tx_idx, seen_at}`. Every row of every batch is
  inserted or updated through the write rule, tagged with the batch's `end_tx_idx`. Entries
  expire after `ttl_ms` (default 30 s), so memory is bounded by the accounts touched in the
  window. `capacity` is a `NonZeroUsize`. At capacity the oldest `seen_at` is evicted.
- The rows apply before the batch's receipts fan out. A client released by its receipt sees
  the new state on its next submit.
- Three executors publish the same receipts with different batch boundaries, and the
  frames fan in out of order. The monotone rule makes the order irrelevant: a row tagged
  with a higher `end_tx_idx` holds a later post-state, so `>` keeps the newest value
  whatever arrives first. The applied position is therefore a **watermark**, not a
  contiguous prefix of frames: the highest position P such that every receipt at or below
  P has been seen (`SeenReceipts` already dedups per receipt). A gap is that watermark
  stalled past `max_stale_txs` while newer positions keep arriving. A gap clears the map,
  counts `degraded_total{reason="gap"}`, and the reader falls to Redis.
- Freshness is a position-lag rule: the highest seen position minus the watermark. Never a
  wall clock.
- Config `[live_accounts] { capacity, ttl_ms }`, independent of `[cache]`. The local layer
  works with Redis off. A local miss then goes straight to the executor query. This lets it
  ship and soak before Redis exists.
- The feed is the `tx_receipts` subscription each reader already holds. No new subscription.
  No `tx_bal` on reader nodes.

### 5.3 Keys

| Key | Value | Writer | TTL |
|---|---|---|---|
| `acct:<addr>` | hash `{nonce, balance, tx_idx}` | mirror | none |
| `rcpt:<addr>:<nonce>` | rkyv `Receipt` bytes + `tx_hash` | mirror | receipt horizon (10 min) |
| `head:<mirror_id>` | `{tx_idx, applied_at}` | that mirror | liveness TTL (5 s) |
| `pending:<addr>` | u64 optimistic floor | sequencer, informational | 60 s |

The reader's head is the max over live `head:*` keys. Each mirror advances its head to its
receipt watermark (5.2): the highest position P such that it has applied every receipt at or
below P. Receipts are must-deliver from every executor replica, so a stalled watermark means
a lost subscription, and the head stalls until it resumes. Cross-account atomicity is not
needed: admission reads one sender.

### 5.4 Consistency rules

- **Write rule.** Apply `acct:<addr>` from a batch row only if `batch.end_tx_idx >
  stored.tx_idx`. On `==`, compare the content and count `producer_disagreement_total`. Never
  overwrite. On `<`, discard. The `==` compare fires only when two producers chose the same
  batch boundary, which is rare, so it is a smoke signal, not the defence (see section 7). A read-through answer is written back through the same rule,
  tagged with the executor's `x-state-tx-idx` header, never with the head.
- **Read rule.** Read `LiveAccounts` first. On a local miss, one `HGETALL acct:<sender>`. On
  a Redis miss, the executor query, and write the answer back into both layers. A missing
  entry in every layer means unknown, and unknown admits. The nonce check runs first, and
  it is free for a warm sender. On `nonce < stored.nonce`, consult the local receipt cache,
  then the Redis `rcpt` key. Answer with the receipt through the existing `tx_hash` identity
  guard, or with `IngressError::Duplicate` (`-32602`) when no receipt exists. The receipt
  index is what makes the nonce-too-low reject safe for a retry: a client that resubmits a
  landed transaction gets its receipt, on any replica, after any restart. This keeps the S5
  retry contract and makes it faster. `nonce >= stored.nonce` always publishes.
  `balance < cost` rejects as `IngressError::InsufficientFunds`, code `-32000`, message
  `insufficient funds for gas * price + value`. `cost = gas_limit * max_fee_per_gas + value`
  with `checked_mul` and `checked_add` on `U256`. A legacy envelope uses `gas_price`. An
  overflow admits.
- **Head rule.** The submit path never reads `head:*`. A background task reads
  `MGET head:0 head:1 head:2` (the mirror ids are the executor indexes) every 100 ms and
  stores the max in an `AtomicU64`.
- **Staleness rule.** Fresh means `latest_seen_tx_idx - head <= max_stale_txs`. Beyond that,
  both checks are skipped and `degraded_total` rises. Position lag, never wall clock, so an
  idle chain is not stale.
- **Fallback rule.** Every error, timeout, miss, gap, or staleness trip admits. Redis may make
  admission faster. It may never be the sole cause of a stall.
- **Rebuild rule.** A rebuild never replays history:
  1. Subscribe to `tx_receipts` and apply live rows from the first batch seen, position E.
  2. Wait until the co-located executor's newest checkpoint in `/opt/kardamom/checkpoints`
     has `last_committed_end_tx_position >= E`.
  3. Open that checkpoint read-only, never the live env. Scan `accounts`. Write every value
     tagged with the checkpoint's end position through the write rule. Live rows after it win.
  A rebuild is mandatory on a non-contiguous resume, a detected head regression, an
  operator-declared restore or reconstruct, and a daily audit schedule.
- **Failover rule.** Async replication can lose the tail. The mirror persists its applied head
  in a local file (`/opt/kardamom/mirror/head`). On start or reconnect, if the Redis head is
  below the local head, the mirror runs a full rebuild. The mirror ends each batch pipeline
  with `WAIT 1 <timeout>`. That cost lands on the mirror only. A `WAIT` timeout (a dead or
  lagging replica) is counted and the mirror proceeds. It never stalls on `WAIT`.
- **Trust rule.** Redis is reachable only from cluster nodes, with `requirepass` and an ACL.
  The mirror user writes. The ingress and sequencer users read, plus `pending:*` writes. The
  replica sets `masterauth`. Sentinel sets `auth-user` and `auth-pass` for the master.

### 5.5 `kardamom-cache` (`crates/cache`)

Shared by the mirror, the ingress and the sequencer. Dependency: the `redis` crate with
`tokio-comp`, `sentinel`, `connection-manager` and `script` features, in
`[workspace.dependencies]` with a one-line reason.

- `CacheConfig` (serde, `deny_unknown_fields`): `sentinels: Vec<String>` (empty = off),
  `master_name`, `timeout_ms: NonZeroU64`, `max_stale_txs: NonZeroU64`, `username`,
  `password_env`. `enabled()` mirrors `LookupConfig::enabled()`. Re-exported into
  `IngressConfig` and `SequencerConfig` as `[cache]`, like `[cluster]`.
- `AccountCache`: a concrete struct. `account(addr)`, `receipt(addr, nonce)`,
  `write_rows(batch)`, `write_receipts(batch)`, `heads()`. The connection manager is owned,
  not shared behind a mutex.
- `LiveAccounts` (5.2).
- `keys.rs` holds the key layout. `script.rs` holds the monotone Lua script as a `const`.
- Metrics: `kardamom_cache_lookups_total{layer,outcome}`, `kardamom_cache_lookup_seconds`,
  `kardamom_cache_degraded_total{reason}`, `kardamom_cache_head_lag_txs`.

### 5.6 `kardamom-state-mirror` (`crates/state-mirror`)

An actor-state struct: `Mirror::new(cfg, subscription, cache)` and `run(self)`. Side tasks end
in `Drop`.

- Subscribes to `tx_receipts` only, through `kardamom_log`.
- Per batch: `write_rows` and `write_receipts` in one pipeline, then the contiguous prefix and
  `head:<id>`.
- The local head file for the failover rule.
- `--rebuild` runs the rebuild rule. It reuses the checkpoint discovery in
  `kardamom_engine::bin_support`.
- Metrics: `kardamom_state_mirror_batches_applied_total`, `_producer_disagreement_total`,
  `_head_tx_idx`, `_rebuild_seconds`, `_redis_info_*` (wraps `INFO`; no `redis_exporter`).

### 5.7 Ingress (`crates/ingress`)

- `validate_submission`: after recovery, read `LiveAccounts`, then
  `AccountCache::account(sender)` on a miss. On `nonce < stored.nonce`, consult the local
  receipt cache, then `receipt(sender, nonce)` from Redis, and answer per the read rule. On
  `nonce >= stored.nonce`, apply the balance check and publish. The admission path never
  calls the executor query (see section 11, PR 4).
- `aeron_adapters.rs`: the batch hook applies rows to `LiveAccounts` before the receipts fan
  out to the watcher that releases parked clients.
- `IngressError::InsufficientFunds` maps to `-32000`.
- The `[cache]` presence flag gates the account read, the receipt read and the head task
  together. With the flag off, no Redis call exists on any path.
- `json_rpc.rs`: `eth_getTransactionCount("latest")` and `eth_getBalance` from the layers.
  `"pending"` from `pending:<addr>` when present.
- `args.rs`: `--cache-sentinels`, `--cache-master-name`, `--cache-max-stale-txs`,
  `--live-accounts-capacity`, `--live-accounts-ttl-ms`.

### 5.8 Sequencer (`crates/sequencer`)

- `lookup.rs`: the lookup task tries `LiveAccounts`, then `AccountCache`, then the executor
  endpoints. The result flows through `advance_floor` (max-merge) as today. It is never a
  seed. The sequencer's own floor is optimistic and is never read back as a floor.
- The sentinel flag is added to `deploy/cluster/scripts/render-sequencer-job.py` and
  `check-contract.py`, never hand-edited into `sequencer.nomad.hcl`.
- Optional: publish the optimistic floor to `pending:<addr>` on change. Write-only.

### 5.9 Executor query (`crates/state/src/nonce_query.rs`)

Add `eth_getBalance` next to `eth_getTransactionCount`, same wire shape, plus an
`x-state-tx-idx` header next to `x-state-block`.

## 6. Deployment

- `deploy/cluster/nomad/redis.nomad.hcl`: three task groups. `primary` on `aux-0`. `replica`
  on `ingress-1`. `sentinel` with count 3 on `aux-0`, `ingress-0`, `ingress-1`. `aux` has
  count 1, so a replica there adds nothing. The executor nodes are memory-pressured. The
  ingress nodes are light, and Sentinel is tiny. No new node class. Pattern:
  `rpc-proxy.nomad.hcl`. No persistence (`--save ""`, no AOF), `maxmemory` bound,
  `allkeys-lru`. Consul services `redis-primary`, `redis-replica`, `redis-sentinel`. Clients
  discover the primary through Sentinel.
- `deploy/cluster/docker/redis.Dockerfile`: `FROM redis:7-alpine` plus `redis.conf`, so the
  image goes through the digest-pin path like the Aeron image.
- `deploy/cluster/nomad/state-mirror.nomad.hcl`: role `executor`, count 3, `distinct_hosts`.
  Bind mounts `/opt/kardamom/aeron-mount`, `/opt/kardamom/checkpoints:ro`, and a new
  `/opt/kardamom/mirror`. No mount of the live state dir. Aeron port block `40360-40369`.
- `deploy/cluster/ansible/group_vars/all.yml`: `ports.redis: 6379`, `ports.sentinel: 26379`,
  `ports.state_mirror_metrics: 9007`, `paths.mirror_dir`.
- `roles/images` and `roles/workloads` defaults: add `redis` and `state-mirror`. Deploy
  `redis` before `ingress`.
- `deploy/prometheus.yml`: scrape the mirror on 9007. Grafana:
  `deploy/grafana/provisioning/dashboards-json/kardamom-state-mirror.json`.
- CI: `-p kardamom-state-mirror` in the build list. A new `chaos-cache` shard. The workflow
  edit ships as `docs/ci/cluster-e2e-cache-shard.yml.draft` if the bot cannot push workflows.

## 7. Security argument and accepted risks

- **No sender forgery path.** Redis never holds a `sender`. The receipt index is keyed by
  `(sender, nonce)` and guarded by `tx_hash`; a poisoned entry fails closed to `Duplicate`.
- **A forged row halts the validator.** The rows are not part of the deterministic per-tx
  transition. The validator's structural row check is what makes them trustworthy.
- **Balance false rejects (accepted, D4).** A stale-low balance within the freshness window
  rejects a funded sender until the account is touched again or the next audit rebuild.
  Deposits execute through the engine and appear in the rows, so the exposure is mirror lag.
  A lost write after a failover is the persistent case; the failover rule and the daily audit
  bound it.
- **In-flight spend.** Same-sender transactions not yet in a batch are not deducted. This is a
  false accept, absorbed by the executor's invalid-tx skip, as today.
- **Divergent producer.** Three executors publish the same receipts with different batch
  boundaries. The `==` content compare in the write rule fires only on an aligned boundary,
  so it detects almost nothing. The defence is the validator's row check (5.1), which halts
  the fleet on a forged row. Until then the readers serve whatever row won by `>`. Producer
  attribution through the Aeron `session_id` and two-of-three agreement are a follow-up.
- **`eth_getTransactionCount("latest")`** serves executed-not-yet-durable state, up to K=4
  blocks ahead of mdbx durability. Canonical, not yet fsynced.
- **Wire change.** The `tx_receipts` frame type changes. Every subscriber recompiles.
  `Receipt` and the mdbx receipts table do not change.

## 8. Rollout

1. Batch rows ship alone. Every subscriber recompiles with the new frame.
2. `kardamom-cache` and the executor query extension ship with no caller.
3. The mirror and Redis ship dark. Nothing reads.
4. `LiveAccounts` ships with the admission checks and the RPCs, Redis off.
5. Redis readers ship behind the `[cache]` presence flag, off by default.
6. Flag day: sentinels configured in `config/ingress.toml` and the sequencer template. Roll
   the ingress first (`max_parallel = 1`), then the sequencer lanes. The existing shards run
   with the flag on and prove the flip is safe.
7. The `chaos-cache` shard and the `s17` scenarios, against the flag-on configuration.

## 9. Test plan

### 9.1 Unit and component

- `kardamom-cache`: `monotone_script_rejects_lower_and_equal_positions`,
  `equal_position_disagreement_is_counted`, `live_accounts_rows_insert_and_update`,
  `live_accounts_gap_clears_and_degrades`, `live_accounts_ttl_evicts`,
  `local_is_never_older_than_redis`.
- Engine and log: `batch_rows_are_the_merged_write_sets`, `batch_of_one_carries_one_tx_rows`,
  `republished_prefix_carries_identical_rows`.
- Validator: `validator_halts_on_a_forged_row`.
- Ingress: `cache_miss_admits`, `receipt_cache_answers_before_nonce_floor_rejects`,
  `nonce_too_low_maps_to_32602`, `future_nonce_is_never_rejected`,
  `stale_head_skips_both_checks`, `insufficient_funds_rejects_when_fresh`,
  `cache_error_admits`, `local_hit_makes_no_redis_call`,
  `receipt_release_happens_after_rows_apply`.
- Sequencer: `cached_floor_never_exceeds_executor_floor`, `a_leading_floor_never_seals_a_gap`,
  `pending_floor_is_never_read_back`.

### 9.2 Chain semantics (`s17`)

The ids `s14`, `s15` and `s16` were taken when PR 4 landed, so the scenarios are `s17`.

- `s17a_nonce_floor_cache_matches_executor_count`: the cached nonce equals or lags the
  executor, never leads.
- `s17b_resubmit_of_landed_still_returns_the_receipt`.
- `s17c_cold_cache_admits_everything`.
- `s17d_deposit_funded_sender_is_admitted_within_max_stale_txs`.
- `s17e_forged_receipt_row_halts_the_validator`.
- `s17f_get_balance_and_nonce_answer_or_degrade_promptly`.
- `s17g_recipient_can_spend_within_one_batch`: A pays B, B submits at once, B is admitted
  before the block boundary.

### 9.3 Chaos (`chaos-cache` shard)

Every case drives the reader counters with a cold-address `eth_getBalance` through the
ingress: a local miss, so the read touches Redis. The pipeline must progress throughout.

- `redis-partition-ingress`: `iptables DROP` of ingress-0's packets to Redis and the
  sentinels. Its reads count `degraded_total`, its submits land, and it uses Redis again
  when the rule is removed.
- `redis-primary-kill`: `docker kill` of the primary. Nomad restarts it empty, or the
  sentinels promote the replica first; either way the readers degrade, then recover, and
  the mirror head advances.
- `redis-primary-freeze`: SIGSTOP of the primary for 20 s, past the sentinels' 5 s
  down-after. The readers degrade, the sentinels promote the replica, and after the thaw
  the readers and the mirror use the promoted primary.
- `mirror-kill-rebuild`: the three mirrors killed and the primary flushed. The restarted
  mirrors find Redis cold and rebuild from the executors' newest checkpoint: the rebuild
  counter rises, the head advances, and a genesis account no live batch touched has a row.
- Deferred: `failover-head-regression` depends on replication lag at the moment of a
  promotion, which this harness cannot arrange deterministically. The mirror's regression
  rebuild is covered by its unit tests.

## 10. Implementation plan (stacked PRs)

| PR | Content | Exit |
|---|---|---|
| 1 | this spec | owner review |
| 2a | `ReceiptBatch` rows (5.1) | chain semantics green; validator row check passes on the load shard; bandwidth recorded here |
| 2b | `kardamom-cache`, `eth_getBalance`, `x-state-tx-idx` | unit tests with `testcontainers` pass |
| 3 | `kardamom-state-mirror`, Redis and mirror jobs, Ansible | cluster-e2e green; the mirror head tracks the executor |
| 4 | `LiveAccounts` readers, admission checks, RPCs, Redis off | unit tests green; the existing chain-semantics shard holds the S5 retry contract |
| 4b | Redis readers behind `[cache]` | flag off is byte-for-byte PR 4 |
| 5 | flag day (config only) | every existing shard green with the flag on |
| 6 | `chaos-cache` shard, `s17a..s17g` | shard green |

## 11. Implementation notes

Deviations from the design above, recorded as they land.

- **PR 2a (batch rows).** The frame is `ReceiptBatch { receipts, accounts }`. The end
  position is the last receipt's `tx_idx`, not a field: a batch with no receipts is never
  published. The merge of per-tx rows into the frame happens at the wire edge, in the
  executor's live publisher, not in the commit thread. The commit thread carries
  `ReceiptRows` (one receipt with its own rows) so the validator's sink sees per-tx rows.
- **Parallel execution.** The Block-STM path has no per-tx write sets. It attaches the
  block's merged rows to the block's last receipt and none to the others, so a row is never
  tagged with a position before the writes it carries. Between boundaries, the readers see
  no rows for such a block.
- **Retry.** The live publisher is all-or-nothing. A retry republishes the whole batch with
  its rows. The suffix resume exists only in the per-item default path and in tests.
- **Validator row check, coverage.** The sink compares a published batch's rows against its
  latest local values at the moment it processes the local receipt at the batch's end. It
  checks only when that local receipt carries rows (under parallel validation, only a
  block's last receipt does). A row for an account with no recorded local value is
  unverified, counted, not a divergence. Rows whose frame arrives after the sink passed the
  end position are dropped at insert and counted as unverified. Two counters report the
  coverage: `validator_rows_verified_total` and `validator_rows_unverified_total`. Under
  parallel validation nearly every mid-block batch counts as unverified by construction, so
  a high unverified rate on a parallel validator is expected, not a fault. A history-based
  check for late frames is a follow-up.
- **Deploy constraint.** An old ingress or sequencer cannot decode the new frame, and its
  parked clients would hang. The executors and every `tx_receipts` consumer roll together.
  The Ansible full redeploy does this; a partial rollout must not split them.
- **Bandwidth.** Not yet measured. The load shard run of PR 2a records the number here.
- **PR 2b (`kardamom-cache`).** The watermark rule of 5.2 and 5.3 is not computable from
  the receipt stream: positions are not dense (a receipt's `tx_idx` is a stream position,
  not a count), and a marker consumes a slot with no receipt. The head is therefore the
  highest applied batch end position, monotone. Staleness is the newest position a reader
  has seen minus the head. A missed frame is bounded by the local TTL (30 s) and by the
  daily rebuild in Redis, and with three producers a hole needs all three copies lost.
  The position tag in Redis is a zero-padded 20-digit decimal, compared as a string in
  Lua, because a position index can exceed 2^53 and a Lua number is a double. The
  `[cache]` section has a direct `url` for tests and single-node development; sentinels
  take precedence. Failover heals on the next command: the client re-asks the sentinels
  and swaps the connection through an `ArcSwap`, no lock. The executor query gained
  `eth_getBalance` and the `x-state-tx-idx` header; a request with no params is now
  `-32602` for both methods.
- **PR 3 (`kardamom-state-mirror`, Redis and mirror jobs).** The mirror subscribes to
  `tx_receipts` and applies each batch: rows and receipts in one pipeline each, then the
  head, then `WAIT` for one replica. A failed batch write retries until it lands; an outage
  past 10 s schedules a rebuild, because frames may have been lost behind the mirror. On
  start the mirror resumes when any live head is at or beyond its local head file, and
  rebuilds when Redis carries no live head (cold) or a head below the local one
  (regression). A daily audit rebuild is the default. The rebuild restores the executor's
  newest checkpoint into the mirror's own directory with the same verified path the
  executor uses, and opens that copy read only; the checkpoints mount is read only. The
  Redis image, primary, replica, and sentinels ship without `requirepass` or ACLs in the
  container cluster: the network is isolated, and authentication is the flag-day item of
  the trust rule. Redis `INFO` metrics are not exported yet; the mirror's own counters are.
- **Not covered by rows.** Block-close writes without a receipt (the health beacon, system
  upgrades) produce no rows. Those predeploy accounts stay stale in the projection until a
  rebuild. None is a sender, so admission is unaffected.
- **PR 4 (`LiveAccounts` readers, Redis off).** The admission path reads the local layer
  only. It never calls the executor query: a flood of fresh senders would turn every miss
  into an mdbx snapshot on an executor. A local miss admits. The executor query serves the
  two account RPCs and the sequencer's cold-sender lookup. The two RPCs take the per-IP
  rate limit like a submit, and the query client bounds its in-flight requests, so a read
  flood is shed at the ingress. The RPCs serve the head only: `latest`, `pending`, `safe`
  and `finalized` are the same block, a number other than the head is `-32602`. The local
  layer's capacity bounds resident accounts, not writes, and defaults to 2^18 (about 30 MB)
  so the layer holds every account touched within the TTL at the load-shard rate. The
  sequencer applies only the rows of its own vslots, the same filter as its receipts, so it
  never holds every account of the chain. A row window exists between the pump applying a
  batch's rows and the receipt watcher populating the receipt cache: a retry that lands in
  that window gets a bare `Duplicate` instead of its receipt. The Redis receipt index of PR
  4b narrows it (the mirror is one more hop from the same source, so it cannot close it).
  The chain-semantics scenarios of 9.2 ship with PR 5. The ingress query client lives in
  `kardamom_cache::query`; the sequencer keeps its own until PR 4b.
- **PR 4b (Redis readers behind `[cache]`).** The reader never stalls on Redis:
  `CacheReader::spawn` returns at once, a background task connects with a backoff and polls
  the mirror heads every 100 ms, and every read before the first connection, during a
  reconnect, past the timeout, or on an error answers "unknown" and counts a degraded read.
  A read during a reconnect is skipped rather than paid, so an outage does not cost every
  cold submit the full timeout. Only the balance check is gated on freshness: a committed
  nonce from any layer is a lower bound on the truth, so a past-nonce reject from a stale
  entry is still correct, and a stale-low balance is the only false reject. This deviates
  from 5.4, which skipped both checks. The staleness unit is canonical records
  (`BPosition::as_index`, the sealer's republished record count), not bytes. The mirror
  count the reader polls is the executor count, passed by the binary, not a config knob.
  The sequencer's nonce lookup reads Redis inside the query task, then the executors; it
  now uses `kardamom_cache::ExecutorQuery`, and its Redis nonce needs no freshness gate.
  There is no write-back of an executor answer into the layers: a query answers one field,
  a row needs both, and the RPC is rate limited. `[cache]` lives in the TOML files
  (`config/ingress.toml`, `config/sequencer.toml.tpl`, both static `file()` templates), so
  the flag day edits those, not `render-sequencer-job.py`. `pending:<addr>` stays open.
- **PR 5 (flag day).** The flag day ships before the `chaos-cache` shard: a shard cannot test
  a dark feature, and the seven existing shards running with `[cache]` on are the proof that
  the flip is safe. The flip is config only: the sentinel list in `config/ingress.toml` and
  `config/sequencer.toml.tpl`, the same three node records the mirror uses. The operator's
  next `make up` applies it to production. Authentication stays deferred: the container
  cluster has no secrets path for a Redis password (Nomad templates render from checked-in
  files), the network is isolated, and a poisoned entry fails closed to `Duplicate` or to an
  admit. `requirepass`, `masterauth`, and the sentinel `auth-pass` land together with a
  secrets path, as one change to the image, the job, and the two reader configs.
- **PR 6 (`chaos-cache` shard).** The cases are those of 9.3. The freeze case exposed a
  gap in the reader: a frozen primary accepts a TCP connection and never answers the
  handshake, so an unbounded reconnect waited for the thaw and never followed the
  promotion. One connection attempt is now bounded by ten command timeouts. The reader
  counters of the ingress (`kardamom_cache_degraded_total`, `kardamom_cache_lookups_total`)
  and the mirror's head and rebuild counters (port 9007 on the executor nodes) are the
  case observables.

## 12. Open questions

- Producer attribution on `tx_receipts` (Aeron `session_id`) to turn "first row wins" into
  "two of three agree".
- Whether `pending:<addr>` is worth its write traffic, or `eth_getTransactionCount("pending")`
  stays unimplemented.
- The `max_stale_txs` default. It depends on the measured load-shard receipt rate.

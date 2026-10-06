# Redis Cluster support with independent storage scaling

Status: designed, not built. Tracks issue #397. Milestone 1, the review of the freshness
protocol in section 5.5, is open. Grounded in main at `a0ecc123f`.

## 1. Problem

The account cache runs on one Redis primary with one replica under three sentinels. The
primary holds every key: all account rows, the receipt index and the mirror heads. So:

- Memory capacity is the memory of one node. Accounts and receipts grow with the chain and
  with the transaction rate. The receipt index alone holds `rate × receipt TTL` entries.
- Write throughput is one primary core. Today each of the three mirrors applies the frames
  of all three executors, so one account row costs nine writes (section 2.3).
- The freshness gate is a global position: the maximum mirror head. A head proves nothing
  about keys on another node. Redis Cluster splits keys across primaries, and each primary
  fails over on its own. A global head cannot certify a shard that lost its tail.

Redis Cluster spreads keys over hash slots on many primaries. Capacity then grows by
adding nodes, independently of the sequencer lanes, the vslots and the executor count.
This spec makes Cluster an option for `[cache]`. It replaces the global head with a
freshness mark per bucket of accounts, so that a shard failover or a slot move never
makes a stale balance look fresh.

## 2. What exists on main

### 2.1 Placement

Since #439 and #454, Redis runs on nodes of its own. Placement uses the role set
`meta.roles`, not node names:

- `deploy/cluster/nomad/redis.nomad.hcl:37-100`: group `primary`, constraint
  `set_contains redis-primary`, static port 6379.
- `deploy/cluster/nomad/redis.nomad.hcl:102-174`: group `replica`, `--replicaof` the
  primary's node record, read once at start from Consul (`:146-155`).
- `deploy/cluster/nomad/redis.nomad.hcl:176-251`: three sentinels, `distinct_hosts`,
  quorum 2, `down-after-milliseconds 5000` (`:216-231`).
- `deploy/cluster/nomad/redis.nomad.hcl:79` and `:143`: every instance announces its node
  record with `--replica-announce-ip` (#371). Without it the sentinels held two identities
  for one instance, and a node replicated from itself.
- `deploy/cluster/ansible/inventories/production/hosts.example.ini:37-40`: production
  has `redis-0` (primary), `redis-1` (replica) and `redis-2` (third sentinel).
- `deploy/cluster/ansible/group_vars/all.yml:221-224`: the local profile packs the
  sentinels onto the ingress nodes and `aux-0`.
- `deploy/cluster/docker/redis/redis.conf:16-20`: no RDB, no AOF, `maxmemory 1gb`,
  `allkeys-lru`. A restarted instance comes back empty. The image is `redis:7.4-alpine`
  (`deploy/cluster/docker/redis/Dockerfile:34`).

### 2.2 The client crate

`crates/cache` holds the transport, the key layout and the reader:

- `crates/cache/src/config.rs:15-42`: `[cache]` has `sentinels`, `master_name`, `url`,
  `timeout_ms` (200), `max_stale_txs` (8,192), `username`, `password_env`,
  `receipt_ttl_secs` (600) and `head_ttl_secs` (5). `sentinels` take precedence over
  `url` (`:23`). `enabled()` is true when any address is set (`:63`).
- `crates/cache/src/client.rs:78-139`: a `Source` enum, URL or Sentinel. Each sentinel
  is asked on its own, and the first answer that a primary accepts wins (#371).
- `crates/cache/src/client.rs:141-303`: one `MultiplexedConnection` behind an `ArcSwap`.
  A failure schedules one background reconnect. Each command has its own 200 ms timeout
  (`:231-245`).
- `crates/cache/src/keys.rs:23-45`: `acct:<addr>`, `rcpt:<addr>:<nonce>`,
  `head:<mirror_id>`, `pending:<addr>`. No key has a hash tag.
- `crates/cache/src/script.rs:15-28`: the monotone write rule in Lua, one account per
  call. Position tags are 20-digit decimals compared as strings (`keys.rs:19`, `:54`).
- `crates/cache/src/reader.rs:93-100`: `fresh()` is true when the local layer's newest
  position minus the highest live mirror head is at most `max_stale_txs`.
- `crates/cache/src/reader.rs:199-208`: a background task polls the heads every 100 ms
  and stores the maximum in one atomic.

### 2.3 The writer

`crates/state-mirror` runs one mirror next to each executor:

- `crates/state-mirror/src/main.rs:132-141`: each mirror subscribes to the `tx_receipts`
  endpoints of all executors under MDS. So each mirror applies every producer's frames.
- `crates/state-mirror/src/mirror.rs:191-196`: one batch is one pipeline of row scripts
  and one pipeline of receipt `SET`s.
- `crates/state-mirror/src/mirror.rs:200-223`: then `SET head:<id>`, then `WAIT 1`. A
  zero answer is counted, never fatal.
- `crates/state-mirror/src/mirror.rs:117-137`: at start, the mirror rebuilds when no
  live head exists (cold) or when the maximum head is below the local head file
  (regression). This check runs only at start.
- `crates/state-mirror/src/mirror.rs:167-189`: a write outage longer than 10 s schedules
  a rebuild (`:20`). A shorter outage schedules nothing.
- `crates/state-mirror/src/mirror.rs:142-165`: a rebuild runs inside `apply` and awaits
  the whole scan. The actor drains no live frames meanwhile.
- `crates/state-mirror/src/rebuild.rs:73-99`, `:183-202`: the rebuild waits for a
  checkpoint at or past the first live position, then streams the accounts table in
  chunks of 512 rows (`:25`).
- `crates/types/src/receipt.rs:65-92`: `ReceiptBatch` has receipts and merged account
  rows. Its end is the last receipt's position. It carries no producer identity and no
  predecessor.
- `crates/executor/src/bin/kardamom-executor/wiring.rs:103-127`: the executor publishes
  one frame per batch, at least once.

### 2.4 The readers

- `crates/ingress/src/proxy/submit.rs:226-256`: admission reads the local layer, then
  `redis.account()`, then pairs the view with the global `redis.fresh()`.
- `crates/ingress/src/proxy/submit.rs:258-292`: a past nonce reads the local receipt
  cache, then the Redis receipt index. Another hash is `Duplicate`. No receipt publishes.
- `crates/ingress/src/proxy/submit.rs:381-401`: only the balance check needs freshness.
  A stale balance admits and counts a degraded check.
- `crates/ingress/src/proxy/accounts.rs:61-63`, `:79-86`: `eth_getBalance` and
  `eth_getTransactionCount` serve the Redis value with no freshness gate.
- `crates/sequencer/src/bin/kardamom-sequencer/feeds.rs:658-683`: the cold-sender lookup
  reads the Redis nonce, then the executors. A nonce is a lower bound at any lag.

### 2.5 Every Redis command, and what Cluster does to it

| Command shape | Where | Keys | On Redis Cluster |
|---|---|---|---|
| `HGETALL acct:<a>` | `client.rs:311-321` | one | works |
| `GET rcpt:<a>:<n>` | `client.rs:392-406` | one | works |
| pipeline of `EVALSHA` row scripts | `client.rs:342-351` | one per row, many slots | fails: redis-rs 1.6.0 rejects a pipeline that spans slots |
| pipeline of `SET rcpt:… EX` | `client.rs:365-384` | many slots | fails, same rule |
| `SCRIPT LOAD` per batch | `client.rs:247-256`, `:341` | none | goes to all nodes; one dead node fails it |
| `MGET head:0 head:1 head:2` | `client.rs:414-424` | many slots | split per shard by the client; not atomic |
| `SET head:<id> EX` | `client.rs:431-440` | one | works, but certifies nothing on other shards |
| `WAIT 1 <ms>` | `client.rs:476-481` | none | goes to all primaries on other connections; the answer is meaningless |
| `SET pending:<a> EX 60` | `client.rs:447-456` | one | works; no production caller, only `crates/cache/tests/redis_e2e.rs:151` |
| Sentinel discovery | `client.rs:99-138` | n/a | not used |

Nothing uses `MULTI`/`EXEC`, `WATCH`, pub/sub, `SCAN` or `KEYS`. The chaos suite uses
`FLUSHALL` on the primary (`crates/chaos/src/cases/cache.rs:598-605`) and asks the
sentinels for the primary (`:243-262`).

The client rules above come from the locked dependency. The workspace declares
`redis = "1"` with `tokio-comp`, `connection-manager`, `sentinel` and `script`
(`Cargo.toml:53`). `Cargo.lock:5621-5623` locks `redis 1.6.0`. In that version:

- `src/cluster_handling/async_connection/routing.rs:95-127`: a pipeline must target one
  slot, or it fails with `CrossSlot` ("Received crossed slots in pipeline").
- `src/cluster_handling/routing.rs:558`, `:642-664`: `WAIT` goes to all primaries and
  returns the minimum.
- `src/cluster_handling/routing.rs:564`, `:613-641`: `SCRIPT LOAD` goes to all nodes and
  needs every node to succeed.
- `src/script.rs:195-214`: `invoke_async` recovers `NOSCRIPT` through `SCRIPT LOAD`.
- `src/cluster_handling/client.rs:484-493`: reads go to primaries unless the builder
  opts in to replica reads.

### 2.6 A gap that exists today on Sentinel

Async replication can lose the tail of the primary's writes in a failover. The lost tail
takes `head:<id>` back with it. The next batch then writes a new head past the lost rows.
The readers see a fresh maximum head over rows that miss the lost tail. The mirror
detects a regression only at start (`mirror.rs:117-137`) or after an outage over 10 s
(`mirror.rs:185-187`). A fast failover that loses a tail can make a stale-low balance look
fresh. A stale-low balance is the one input that rejects a funded sender. The protocol
in section 5.5 closes this gap on every topology.

## 3. Goals and non-goals

Goals:

- `[cache]` accepts a Redis Cluster as a third topology. Off, direct and Sentinel stay.
- Cache capacity and throughput scale by adding Redis nodes. The Redis layout does not
  depend on sequencer IDs, lanes, vslots or the ownership map.
- A balance read is fresh only when the bucket of that account proves its coverage. No
  shard's state certifies another shard's keys.
- Failover, slot migration, shard loss and total loss degrade reads to "unknown". They
  never cause a false `InsufficientFunds` and never stall a submit.
- The public JSON-RPC behavior does not change: the receipt identity check, the
  `Duplicate` rule, the nonce lower bound and every fallback stay.
- Migration from Sentinel runs through a separate Cluster with a rebuild. Rollback is a
  config change back to the still-fed Sentinel cache.

Non-goals:

- No autoscaler and no Kardamom resize command. An operator runbook resizes.
- No replica reads.
- No persistence. Redis stays a cache; the executors are the truth.
- No change of the bucket count after launch. A new count needs a new namespace.
- No Redis authentication in this work. It stays the flag-day item of
  `docs/specs/2026-09-13-redis-account-cache-design.md`, section 11.
- No cache handling of a chain revert (`docs/specs/2026-10-03-l1-outage-recovery-chaos.md`,
  section 3.8). That work must change the key namespace (section 5.3).
- No new Redis command beyond Redis 7.4. No atomic slot migration of Redis 8.

## 4. Terms

- **Bucket**: one of 4,096 fixed groups of accounts. The bucket of an address is
  `u16::from_be_bytes(keccak256(address)[0..2]) >> 4`.
- **Hash slot**: one of the 16,384 Redis Cluster slots. Not a vslot.
- **Shard**: one Redis primary with its replica.
- **Mark**: the hash that holds the freshness state of one bucket.
- **Covered end**: the field `covered` of a mark, a position tag. Section 5.5.1 gives its
  meaning.
- **Producer**: one executor that publishes `tx_receipts`.
- **Chain**: the sequence of one producer's batches as one mirror receives them, without a
  gap.
- **Bucket write**: one atomic Lua call on the keys of one bucket.

## 5. Design

### 5.1 Config, parsed at the boundary

`[cache]` gains one field:

```toml
[cache]
cluster_seeds = ["redis://redis-cluster.service.consul:6379"]
```

The serde type parses once into a typed topology (R9, R6). Nothing downstream checks
field combinations again:

```rust
#[serde(try_from = "RawCacheConfig")]
pub struct CacheConfig {
    pub topology: Topology,
    pub timeout_ms: NonZeroU64,
    pub max_stale_txs: NonZeroU64,
    pub auth: Auth,
    pub receipt_ttl_secs: NonZeroU64,
}

pub enum Topology {
    Off,
    Direct(NodeAddr),
    Sentinel(SentinelSet),
    Cluster(ClusterSeeds),
}
```

The parse rules:

1. `cluster_seeds` not empty and (`url` set or `sentinels` not empty): a config error.
2. `cluster_seeds` not empty: `Cluster`. Each seed parses as a Redis URL at load time.
3. `sentinels` not empty: `Sentinel`. A `url` beside it is ignored, as today.
4. `url` set: `Direct`.
5. Otherwise: `Off`.

`ClusterSeeds` and `SentinelSet` wrap a non-empty list, so an empty one cannot exist.
Cluster uses database zero. The username and the password apply to every node that the
client discovers. `head_ttl_secs` goes away with the heads (section 5.3). No checked-in
config sets it.

### 5.2 The client

The workspace enables the `cluster-async` feature of `redis 1.6.0`. The transport becomes
an enum with one variant per topology (R6). No `dyn`:

- `Node`: today's `ArcSwap` connection, `Source` and reconnect, for direct and Sentinel.
- `Cluster`: one `redis::cluster_async::ClusterConnection`. It is `Clone` and multiplexed.
  It follows `MOVED` and `ASK`, and it refreshes the slot map by itself.

The cluster builder sets `connection_timeout` to the connect bound of `client.rs:31`, the
`response_timeout` to `timeout_ms`, and a small `retries` count. It never enables replica
reads.

One deadline bounds each logical call. A `tokio::time::timeout_at` wraps the whole call,
so redirects, `TRYAGAIN`, topology refresh and script reload all fit inside it. One
admission shares one deadline across its account read and its receipt read.

Scripts run as `EVALSHA`, routed by the bucket's hash tag. On `NOSCRIPT` the client sends
`EVAL` with the script body to the same slot, within the same deadline. That also caches
the script on that node. The client never sends `SCRIPT LOAD` in Cluster mode: it goes to
all nodes, and it fails exactly when a node is down. A promoted replica or a new primary
has no script until the first `EVAL`.

A pipeline holds commands of one bucket only. Work for many buckets runs as concurrent
single-bucket calls, at most 16 in flight per caller.

`client.rs` has 576 lines today. The transport, the bucket calls and the Sentinel
discovery move into sibling modules to meet R3.

### 5.3 Key layout and hash tags

Every key of a bucket carries the bucket's hash tag, so one bucket lives in one hash slot.
The prefix `kc1` names the layout version.

| Key | Value | Writer | TTL |
|---|---|---|---|
| `kc1:{kc1-b0123}:mark` | hash: `covered`, `build_token`, `build_next` | mirror | none |
| `kc1:{kc1-b0123}:acct:<addr>` | hash: `nonce`, `balance`, `tx_idx` | mirror | none |
| `kc1:{kc1-b0123}:rcpt:<sender>:<nonce>` | rkyv `Receipt` | mirror | `receipt_ttl_secs` |

The tag holds the bucket as four decimal digits, `b0000` to `b4095`. A receipt key uses
the bucket of its sender.

Bucket identity never changes when Redis resizes. A slot move carries the whole bucket,
because all its keys share one slot. The 4,096 buckets spread over 16,384 slots, so a
resize moves buckets in small units.

The layout drops three keys:

- `head:<mirror_id>`: the marks replace it.
- `pending:<addr>`: it has no production writer (section 2.5).
- The local head file (`crates/state-mirror/src/head.rs`): a mark that regresses shows
  as a lag, so no local file is needed.

Why buckets and not one mark per account: a row with `tx_idx = T` says "this was the
state at T". It cannot say "nothing changed since T". Only a mark that every batch
advances can say that, and an account that no batch touches still needs it. A mark per
account needs a write per account per batch. A mark per bucket needs at most one write
per bucket.

### 5.4 The feed: a chain envelope

`ReceiptBatch` gains two fields, set by the producer:

- `producer`: the executor index.
- `prev_end`: the end of this producer's previous published batch. `None` when the
  producer cannot prove that it published every earlier batch, for example after a
  restart.

A mirror cannot infer `prev_end` from what it received. Positions are not dense: a
`BPosition` is a log position, so `previous + 1` is not a continuity test.

Each mirror applies the frames of its co-located producer only. Its index equals its
`mirror_id` (`crates/state-mirror/src/main.rs:148`). The other two mirrors cover a
producer that is down. This cuts the row writes from nine to three per row.

The frame change is a wire change. The executors and every `tx_receipts` consumer roll
together, as for the account rows before
(`docs/specs/2026-09-13-redis-account-cache-design.md`, section 11).

### 5.5 The freshness protocol

This is the review item of milestone 1.

#### 5.5.1 The invariant

For each bucket `b`, the protocol keeps this invariant **I(b)**:

> If the mark of `b` holds `covered = C`, then for every account `a` of `b` whose state
> changed at a position at or below `C`, the row of `a` is absent, or its `tx_idx` is at
> or past that change.

A row is always a true state at its `tx_idx`: it comes from a canonical batch, and the
monotone rule never lowers `tx_idx` (`crates/cache/src/script.rs:15-28`).

So under I(b), a row read together with `covered = C` is at least as new as position `C`,
or it is absent. Absent means unknown, and unknown admits.

#### 5.5.2 The bucket write

The mirror changes a bucket only through one Lua call per bucket per batch,
`BUCKET_APPLY(from, to, rows, receipts)`. All its keys are in one slot. The call does
these steps atomically:

1. Apply each row through the monotone rule, with that row's position tag.
2. `SET` each receipt with the receipt TTL.
3. Read `covered`. If it is present and `covered >= from`, set it to `max(covered, to)`
   and answer `ADVANCED`. Otherwise leave it and answer `GAP`.
4. Answer the new `covered` and the row codes.

The rows apply also on `GAP`. They are true and new, and I(b) does not depend on them.

**Lemma 1.** A bucket write keeps I(b) when its rows include every account of `b` that
changed in `(from, to]`. Proof: the covered end moves only when `covered >= from`. Then
`(covered, to]` is inside `(from, to]`, and the call writes a row at least that new for
every account that changed there.

#### 5.5.3 The chain in the mirror

The mirror keeps, for its producer's chain:

- `chain_end`: the end of the last batch it applied.
- `last[b]`: for each bucket, the end of its last bucket write. It is `chain_start` until
  the chain first touches `b`. That is an array of 4,096 positions.

For each batch `(P, H]` of its producer:

1. If `prev_end = Some(P)` and `P = chain_end`, the chain continues.
2. Otherwise the chain restarts. `chain_start` becomes `P`, and every `last[b]` becomes
   `P`. With `prev_end = None`, the mirror applies the batch with `from = H`, so nothing
   advances, then restarts the chain at `H`.
3. For each bucket `b` that the batch touches, send
   `BUCKET_APPLY(from = last[b], to = H, rows of b, receipts of b's senders)`. Then
   `last[b] = H`.
4. `chain_end = H`.

The precondition of Lemma 1 holds: the chain has no row of `b` in `(last[b], P]`, and the
batch holds every row of `b` in `(P, H]`. This needs no earlier write to have succeeded.
A `GAP` only means that the covered end lags. It catches up when any chain reaches it.

A duplicate frame (`H <= chain_end`) is skipped.

#### 5.5.4 The sweep

A bucket that no batch touches still needs its mark to advance. Every 250 ms the mirror
sends `BUCKET_APPLY(from = last[b], to = chain_end)` with no rows for each bucket with
`chain_end - last[b] >= sweep_lag`, then sets `last[b] = chain_end`. Lemma 1 holds: the
chain has no row of `b` in that range. The default `sweep_lag` is half of
`max_stale_txs`. So a sweep writes each idle bucket at most about once per second at the
load-shard rate.

An idle chain needs no sweep and no lease. Freshness is a position lag, never wall clock.
When the chain stops, the readers' local head moves on, the lag grows, and reads turn
stale.

#### 5.5.5 The read

The reader calls one read-only script, `BUCKET_READ`, with the keys of the mark and of
the account. One call returns `covered` and the row together, from one node, at one
instant. `NOSCRIPT` recovers through `EVAL_RO` (section 5.2).

- **Balance check** (admission): fresh when `covered` is present and
  `live_head.saturating_sub(covered) <= max_stale_txs`. This is the formula of
  `crates/cache/src/reader.rs:97-100`, now per bucket. Not fresh: admit and count
  `degraded{reason="uncertified"}`.
- **Nonce** (admission, sequencer lookup, RPC): a plain single-key `HGETALL`. A
  committed nonce is a lower bound at any lag.
- **Receipt**: a single-key `GET`. Identity checks stay as in `submit.rs:258-292`.
- **RPC reads** (`accounts.rs:61-63`): unchanged, no freshness gate (open question 7).

The reader's head poll (`reader.rs:199-208`) goes away. The background task only
connects. `CacheReader::spawn` loses its mirror-count argument.

#### 5.5.6 The rebuild of a bucket set

A mirror counts a bucket as lagging when its last known `covered` trails `chain_end` by
more than `max_stale_txs` for longer than 10 s. Then it rebuilds all its lagging buckets
in one checkpoint scan. Mirror `e` waits `5 s × e` more before it starts, so the three
mirrors seldom rebuild the same bucket at once. A total loss makes every bucket lag. A
full rebuild of all buckets also runs on the daily audit (`mirror.rs:216-221`).

The actor keeps draining frames during a rebuild. It applies them as usual and also keeps
its own chain's batches in a bounded spool. The scan runs on a separate task. The actor
and the task talk through channels (R5).

For each bucket `b` in the set:

1. **Begin**: set `build_token = T` (random) and `build_next = 0` in the mark. Leave
   `covered` as it is. I(b) does not depend on the build fields.
2. **Chunks**: the scan reads the newest checkpoint at `C_ck` once, routes rows by
   bucket, and sends chunks of up to 512 rows, numbered from 0. The call
   `BUCKET_BUILD_CHUNK(T, i, rows at C_ck)` applies only when `build_token = T` and
   `build_next = i`, then increments `build_next`. When `build_next > i` and the token
   matches, it is a retry of a chunk that landed; it answers success and changes nothing.
   Any other state aborts this bucket's build.
3. **Seal with catch-up**: `BUCKET_BUILD_SEAL(T, n, rows, to = chain_end)` checks
   `build_token = T` and `build_next = n`, where `n` is the number of chunks that the scan
   sent for `b`. An empty bucket has `n = 0`. Then it applies the merged spool rows of `b`
   from the batches past `C_ck` and sets `covered = max(covered, chain_end)`. It clears
   the build fields. Then `last[b] = chain_end`.

The checkpoint must lie inside the spool: the mirror waits for a checkpoint with
`C_ck >= spool_start`. The executor writes one every 20 s (`rebuild.rs:26-28`). The spool
holds at least two checkpoint intervals of the own chain. A spool overflow restarts the
build with a newer checkpoint.

**Lemma 2.** A seal keeps I(b). The chunk counter changes in the same atomic call as the
chunk's rows. So `build_next = n` proves that all `n` chunks are present on this node.
Then every account of the checkpoint has a row at or past `C_ck`. The spool rows cover
`(C_ck, chain_end]`.

A second builder replaces the token, so the first one's chunks fail and it stops. Its
rows that landed are true and stay. A builder that dies leaves a stale token; the next
begin replaces it.

#### 5.5.7 Why failover and resharding keep the invariant

The argument rests on four facts:

- **F1, prefix.** A Redis primary replicates its effects in execution order. Redis 7
  replicates a script's effects as one `MULTI`/`EXEC` unit. A replica drops an unfinished
  `MULTI` when it loses the link. So a promoted replica holds a prefix of whole calls.
  Every call keeps I(b), so every prefix keeps I(b).
- **F2, empty restart.** A node without persistence restarts empty. No mark means I(b)
  holds trivially.
- **F3, slot moves.** `MIGRATE` copies keys with their values. While the keys of a slot
  are split between two nodes, a multi-key call answers `TRYAGAIN` or `ASK` and runs
  nothing. A key that a failover loses during the import becomes absent, and absent rows
  keep I(b).
- **F4, eviction.** Section 5.7 sets `volatile-lru`. Only keys with a TTL are evicted:
  receipts. Marks and rows are never evicted. A full node refuses writes, which shows as
  a `GAP` and a lag.

The read (5.5.5) sees one node at one instant, so it sees a state where I(b) holds.

The mark and the rows of a bucket always move, replicate and fail over together. That is
the reason for the hash tag. Nothing in this argument depends on `WAIT`, a lease, or a
head on another node.

Assumptions:

- **A1**: F1 and F3 hold for Redis 7.4. Milestone 2 tests both.
- **A2**: the producer emits a row for every account whose nonce or balance changed in a
  batch. Block-close writes with no receipt (the health beacon, system upgrades) emit no
  rows (`docs/specs/2026-09-13-redis-account-cache-design.md`, section 11). Those accounts
  are predeploys and never send, so admission is not affected.
- **A3**: the producer sets `prev_end` only when it published every earlier batch.
- **A4**: positions are never reused. A chain revert must change the `kc1` prefix.
- **A5**: Redis is trusted. This is the trust rule of the 2026-09-13 spec. The protocol
  does not defend against a forged mark.

#### 5.5.8 Failure timelines

| Timeline | What happens | Why no false reject | How it recovers |
|---|---|---|---|
| One shard loses its tail in a failover; other shards advance | That shard's marks and rows fall back together (F1) | Its buckets lag; reads there are stale and admit | Chains continue with `GAP`; lagging buckets rebuild |
| A fast Sentinel failover loses a tail (section 2.6) | The mark falls back with the rows | The lag shows; no global head hides it | Same as above |
| An old row survives beside an advanced mark | Cannot happen: they share one call and one slot (F1, F3) | n/a | n/a |
| A whole shard restarts empty | Its marks are absent (F2) | Absent mark is not fresh | Chains answer `GAP`; buckets rebuild |
| Total loss of all shards | Every mark is absent | Nothing is fresh; all reads admit | Full rebuild, like `redis-total-loss-recover` today |
| An untouched funded account is lost in a rebuild | The seal needs every chunk (Lemma 2) | A missing chunk blocks the seal | The build restarts |
| A chunk lands but its reply is lost | The retry sees `build_next > i` | No double count | Retry succeeds |
| A scan stops halfway while live writes land | Live writes never seal | No covered end moves past what is proven | The next build starts again |
| Two builders on one bucket | The later token wins | The earlier builder cannot seal | The later builder seals |
| A mark is absent and a new batch lands | `BUCKET_APPLY` answers `GAP` | The mark stays absent | Rebuild |
| A slot moves while live writes run | Calls on the split slot get `TRYAGAIN` | Nothing runs on half a bucket (F3) | The mirror retries; readers degrade inside their deadline |
| A demoted primary takes writes before it learns its role | Those writes vanish on resync | The new primary holds a prefix; its marks fall back with its rows (F1) | Chains answer `GAP`; rebuild if no chain covers the range |
| One mirror loses frames | Its chain restarts at the next `prev_end` | Its writes answer `GAP` until another chain covers the range | No rebuild when another chain covers it |
| An executor restarts | Its first batch has `prev_end = None` | Nothing advances from that batch | The other chains cover; this chain restarts |
| Redis memory is full | Writes fail; receipts evict first (F4) | Marks stop; reads turn stale | Operator adds capacity (section 5.9) |

### 5.6 Cost

- **Writes per batch**: one bucket write per touched bucket, per mirror, for its own
  producer only. Today one row costs nine writes; it then costs three.
- **Round trips**: bucket writes run in parallel, at most 16 in flight. Each targets one
  slot. The multiplexed connection pipelines them on the wire.
- **Sweep**: at most one write per idle bucket per `sweep_lag` positions. That is at most
  4,096 small writes per mirror per sweep.
- **Memory**: about 4,096 marks of tens of bytes. Rows and receipts do not grow. The
  spool holds about 40 s of the own chain in the mirror process.
- **Reads**: one `EVALSHA_RO` per balance check instead of one `HGETALL`. No head poll.

Milestone 5 measures these numbers. This spec claims no speedup.

### 5.7 Deploy

A new job, `deploy/cluster/nomad/redis-cluster.nomad.hcl`:

- One group `node`, `count = var.node_count` (default 6), constraint
  `${meta.roles} set_contains redis-cluster`, and `distinct_hosts`. One instance per
  host. So a replica is never on its primary's host.
- Host network, static ports 6379 (clients) and 16379 (cluster bus).
- `--cluster-announce-ip` is the node's private address (open question 6).
- A bind mount `/opt/kardamom/redis-cluster:/data` holds `nodes.conf` only. A restart or
  a redeploy on the same node keeps the node ID. The deploy never copies or templates
  `nodes.conf`.
- `restart { delay = "15s" }`. That is more than twice `cluster-node-timeout`. A primary
  that restarts empty must not return before its replica is promoted. Otherwise its
  replica resyncs from the empty primary.
- One Consul service `redis-cluster` with a TCP check. Readers use it as the seed.

A new config file in the image, `deploy/cluster/docker/redis/redis-cluster.conf`:

```
cluster-enabled yes
cluster-config-file /data/nodes.conf
cluster-node-timeout 5000
cluster-require-full-coverage no
cluster-replica-validity-factor 0
save ""
appendonly no
maxmemory-policy volatile-lru
```

- `cluster-require-full-coverage no`: a dead shard degrades only its own buckets. With
  `yes`, every node refuses every query while one slot has no owner.
- `cluster-replica-validity-factor 0`: a replica always fails over. A lost tail is safe
  (section 5.5.7). A shard without a primary is not.
- `maxmemory` comes from a job variable, with the task memory set above it.

Bootstrap is an Ansible task after the job runs:

1. Read `CLUSTER INFO` from every node.
2. When every node shows `cluster_known_nodes:1` and `cluster_slots_assigned:0`, run
   `redis-cli --cluster create <nodes> --cluster-replicas 1 --cluster-yes`.
3. Otherwise run `redis-cli --cluster check` and fail on any error. Never reset a node.
4. Check that each replica is on another host than its primary.

The node class: `node_classes` in `group_vars/all.yml` gets `redis_cluster` with count 0
by default, so the Sentinel profile does not change. The production inventory example gets
a `[redis_cluster]` group of six hosts with `node_roles=redis-cluster`.

The mirrors: `state-mirror.nomad.hcl` gets variables for the job name, the mirror
directory and the `[cache]` section. The migration (section 7) runs it twice.

The readers: `config/ingress.toml` and `config/sequencer.toml.tpl` swap `sentinels` for
`cluster_seeds`.

### 5.8 Storage scaling

Capacity is the number of primaries times `maxmemory`. It grows by adding pairs of nodes.
It does not depend on the executor count, the sequencer lanes or the vslots.

A rough size, to be measured in milestone 5:

- Account rows: about 200 bytes each.
- Receipts: about 400 bytes each, times `rate × receipt_ttl_secs`. At 4,800 tx/s and
  600 s that is about 1.2 GB.

### 5.9 Resize runbook

Grow:

1. Provision two nodes with `node_roles=redis-cluster` and raise `node_count` by two.
2. `redis-cli --cluster add-node <new-a>:6379 <any>:6379`.
3. `redis-cli --cluster add-node <new-b>:6379 <any>:6379 --cluster-slave
   --cluster-master-id <new-a id>`.
4. Move slots in batches of 256 with `redis-cli --cluster reshard`. Between batches,
   check the reader p99 latency, the bucket lag, the degraded count and memory.
5. `redis-cli --cluster check`.

Shrink:

1. Move all slots off the primary in batches.
2. Check that it owns zero slots and zero keys.
3. Remove its replica, then the primary, with `redis-cli --cluster del-node`.
4. Drain the two Nomad nodes, lower `node_count`, and remove the role.

A failed move: inspect the open slots with `redis-cli --cluster check`, then
`redis-cli --cluster fix`. Never `CLUSTER RESET` a node that owns slots. Never restore an
old `nodes.conf`.

## 6. Chaos cases

A new shard, `chaos-cache-cluster`, runs the Cluster profile with load. Every case holds
the fallback rule (the pipeline progresses and the readers count degraded reads) and
shows recovery.

Several cases use a **funded probe**. It is a sender with a low balance `L` that receives
a transfer to a high balance `H` during the fault. Then it submits a transaction whose
cost lies between `L` and `H`. The case fails on `InsufficientFunds`. This is the direct
proof of "no stale-low balance looks fresh".

| Case | Fault | Proof |
|---|---|---|
| `cluster-primary-kill` | Kill one primary | Only that shard's buckets degrade; others stay certified; the replica takes over; reads recover |
| `cluster-primary-freeze` | `SIGSTOP` one primary past the node timeout, then thaw it | The funded probe is admitted; the thawed node rejoins as a replica; its lost writes cause a rebuild or a `GAP` catch-up |
| `cluster-shard-loss` | Stop a primary and its replica; restart both empty | Only that shard's buckets rebuild; the bucket rebuild counter rises for those buckets only |
| `cluster-total-loss` | Stop the whole job; start it empty; bootstrap | Every mirror rebuilds; readers recover; the pipeline never stalls |
| `cluster-reshard-under-load` | Move 1,024 slots between primaries during load | Funded probe admitted; reader calls stay inside the deadline; no `CrossSlot` error |
| `cluster-scale-out` | Add a pair and rebalance during load | Same as above; `--cluster check` is clean |
| `mirror-chain-break` | Drop `tx_receipts` to one mirror for 5 s | That mirror's writes answer `GAP`, then catch up; no rebuild |
| `cluster-partition-ingress` | Drop the Redis ports from one ingress | Admits and degrades; recovers; no stall |
| `sentinel-fast-failover` | On the Sentinel profile: kill the primary right after a funded transfer | The funded probe is admitted. On main this case can fail (section 2.6) |

The existing `chaos-cache` cases stay for Sentinel. `redis-total-loss-recover` and
`mirror-kill-rebuild` change their observable from the mirror heads to the certified
bucket count.

New metrics give the observables:

- `kardamom_cache_degraded_total{reason="uncertified"}` on the readers.
- `kardamom_state_mirror_bucket_writes_total{outcome="advanced"|"gap"}`.
- `kardamom_state_mirror_buckets_certified` (gauge).
- `kardamom_state_mirror_bucket_rebuilds_total`.
- `kardamom_cache_script_reload_total`.

## 7. Migration from Sentinel

Milestone 3 ships the bucket protocol on Sentinel first. The two caches then run the same
code, and only the topology differs.

1. Provision the `redis_cluster` nodes, deploy `redis-cluster`, and bootstrap.
2. Deploy a second mirror job, `state-mirror-cluster`, on the executor nodes. It has its
   own directory, `/opt/kardamom/mirror-cluster`, and `cluster_seeds`. Its mirrors rebuild
   every bucket. The Sentinel mirrors keep running.
3. Gate: all three cluster mirrors report 4,096 certified buckets for 10 minutes. A parity
   probe reads 10,000 random addresses from both caches and finds no disagreement at
   aligned positions.
4. Switch the sequencers and one ingress to `cluster_seeds`. Watch the degraded count,
   the reject counts and the latency. Then switch the second ingress.
5. Keep the Sentinel cache fed through the rollback window (open question 5). Rollback is
   the config change back to `sentinels`. It copies no keys.
6. Retire: stop `state-mirror` and `redis`, and remove the Sentinel roles.

Receipts written only to the old cache are lost for a cluster reader. A receipt miss
publishes, and the sequencer decides. That is accepted.

## 8. Milestones

Each milestone ends with its proof.

1. **Review of the freshness protocol.** This spec, section 5.5, the timelines of 5.5.8
   and the differences in section 9.
   - Proof: the owner accepts the protocol on the pull request, or names the changes. No
     code lands before that.
2. **Transport and config.** `Topology`, the `cluster-async` feature, the transport enum,
   the deadline, `EVAL` recovery, primary-only reads. The mirror refuses `Cluster` until
   milestone 3.
   - Proof: unit tests for every parse rule of 5.1.
   - Proof: a docker-e2e test against a six-node Redis Cluster. It shows that a
     one-slot pipeline works and a cross-slot one fails, and that `NOSCRIPT` after a
     promotion recovers inside the deadline. It shows that reads hit primaries and that a
     frozen primary costs at most the deadline.
   - Proof: a test of A1. A scripted bucket write under a killed primary never leaves
     half a call on the replica. A multi-key call on a slot in migration answers
     `TRYAGAIN` or `ASK`.
   - Proof: `redis_e2e` and the `chaos-cache` shard pass unchanged.
3. **Bucket protocol on every topology.** The envelope, chains, bucket writes, sweep,
   read script, bucket rebuild with spool, metrics and dashboards.
   - Proof: a model test. A simulated Redis cuts op logs to random prefixes, drops frames,
     interleaves producers and splits slots. Over 10,000 seeds, no read returns a row
     older than its mark claims.
   - Proof: the `chaos-cache` shard and the new `sentinel-fast-failover` case pass on
     Sentinel. The `s17` account-layer scenarios pass.
4. **Deploy.** The job, the image config, bootstrap, node class, mirror job variables and
   the runbooks.
   - Proof: cluster-e2e passes on a Cluster profile.
   - Proof: `redis-cli --cluster check` is clean after a deploy and after a redeploy.
   - Proof: one grow and one shrink from section 5.9 on the local profile.
5. **Chaos, benchmarks and the staging migration.**
   - Proof: every case of section 6 passes twice in a row.
   - Proof: a published benchmark of Sentinel and Cluster at matched resources. It gives
     reader p50, p95 and p99, mirror writes per second, rebuild time, memory per account
     and degraded counts, with the raw results.
   - Proof: one migration and one rollback on staging, from section 7.

## 9. Decided, and differences from the issue text

Decided, as the issue agrees:

- Cluster is optional. Off, direct and Sentinel stay, with today's auth fields and the
  Sentinel-over-URL precedence.
- Redis layout is independent of sequencer IDs, lanes, vslots and ownership.
- Migration runs through a separate Cluster and a rebuild. Receipt misses are accepted.
- An operator runbook resizes. No autoscaler.
- Admission keeps account-specific freshness, receipt identity checks, monotone rows,
  public JSON-RPC behavior and bounded fallback.
- 4,096 fixed buckets with hash tags.
- Primary-only reads, one deadline per call, bounded concurrency, script recovery by key.

Different from the issue text, for review in milestone 1:

- **Placement.** The issue predates #439 and #454. Sentinel already runs on nodes of its
  own. Cluster gets its own role, `redis-cluster`.
- **No per-account certification directory, no digests.** The issue keeps a directory
  entry and a Keccak digest per account in the mark. Here one atomic bucket write per
  slot and the replication prefix (F1) give the same guarantee under the trusted-Redis
  model. The directory would double the key data of the cache.
- **No generations, active pointer or lease.** Freshness is a position lag, so a dead
  mirror needs no lease to expire. A bucket rebuilds in place under a build token.
  Rows are never wrong, only old, so stale rows need no separate namespace.
- **No connection-affine `WAIT`.** The proof does not use replica acknowledgement. In
  Cluster mode `WAIT` from redis-rs reaches other connections (section 2.5).
- **One protocol on every topology.** It fixes the Sentinel gap of section 2.6.
- **The mirror applies its own producer only.** Writes fall from nine to three per row.
- **`volatile-lru`, not `noeviction`.** Receipts evict first. Marks and rows never evict.
- **Dropped keys.** `head:<id>`, `pending:<addr>` and the local head file.

## 10. Open questions

These need the owner's decision:

1. **The simpler protocol.** Accept section 5.5 in place of the issue's certification
   directory, digests, generations and leases? It depends on F1 and F3, which milestone 2
   tests.
2. **One protocol on Sentinel too.** Ship the bucket layout on Sentinel in milestone 3
   (recommended), or keep the legacy layout there?
3. **Eviction.** `volatile-lru` (recommended) or `noeviction`, as the issue says?
4. **Local profile.** Six one-instance containers for the Cluster shard, or three
   primaries without replicas? Without replicas, the failover cases run only on staging.
5. **Rollback window.** How long the Sentinel cache stays fed after the switch. Seven days
   is the proposal.
6. **Announce address.** The private IP (recommended) or the Consul node record? #371
   fixed Sentinel with node records. Cluster bus gossip uses addresses, and redis-rs must
   resolve whatever the slot map returns.
7. **RPC freshness.** `eth_getBalance` serves a Redis value with no gate today. Keep that
   (no public change), or gate it on the bucket mark and fall back to the executor?
8. **Authentication.** Ship Cluster without auth, like Sentinel today, or make Cluster wait
   for the secrets path of the flag day?

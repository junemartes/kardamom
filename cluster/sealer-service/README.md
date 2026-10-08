# Kardamom sealer cluster service (Java)

The sealer is the canonical-ordering state machine. It runs inside an Aeron Cluster (Raft).

- It dedups the records that racing sequencer replicas offer.
- It gives each record a canonical index.
- It stamps a block boundary on each tick.
- The Raft Consensus Module replicates the state and moves the leader on a failure.

The Aeron Consensus Module is JVM-only, so this logic is in Java. The Rust pipeline talks to the
cluster through `crates/cluster-adapter`. For the design, see
[`docs/agents/sealer-aeron-cluster-failover-spec.md`](../../docs/agents/sealer-aeron-cluster-failover-spec.md) (further reading).

## Layout

- `core/`: `CanonicalSealerState`, the pure and deterministic state machine.
  - It holds the dedup window, the canonical count, the boundary stamp, the void ledger and the snapshot.
  - It has no Aeron dependency. Its JUnit tests need only JUnit on the classpath.
  - It is the one dedup point of the pipeline. The executor trusts the relayed stream and keeps no window.
- `service/`: `SealerClusteredService`, which implements `io.aeron.cluster.service.ClusteredService`.
  - It is the thin Aeron layer: ingress decode, egress framing, timers, snapshot I/O, the admin server.
  - It delegates all logic to `core`.
  - It depends on `io.aeron:aeron-cluster:1.44.0`.

## App envelope

The Java side (`SealerWire.java`) and the Rust side (`crates/cluster-adapter/src/wire`) use the
same layouts. All integers are little-endian. A frame starts with a one-byte kind.

### Ingress kinds (client to cluster)

| Kind | Name | Layout |
|---|---|---|
| 0 | Record | `[kind=0][sender:20][nonce:u64][deadline:u64][tip:u128][canonical_id:32][record_type:u8][fields…]` |
| 1 | Replay request | `[kind=1][from_index:u64][from_block:u64]` |
| 2 | Subscribe | `[kind=2]` |
| 3 | Batch | `[kind=3][count:u16]` then, for each entry, `[len:u32][one complete kind-0 frame]` |
| 4 | Origin record | `[kind=4][canonical_id:32][l1_origin:u64][slot_count:u32][record_type:u8][fields…]` |
| 5 | Remote-origin record | `[kind=5][canonical_id:32][origin_chain_id:u64][anchor_number:u64][slot_count:u32][first_seq:u64][last_seq:u64][record_type:u8][fields…]` |
| 6 | Void request | `[kind=6][voter_id:u8][index:u64][tx_hash:32]` |
| 7 | Posted cursor | `[kind=7][posted_head:u64]` |
| 8 | Seed record | `[kind=8][digest:32]` |
| 9 | Recorded cursor | `[kind=9][executor_id:u8][recorded_through:u64]` (10 bytes) |

- Kind 0: the guard header is `sender`, `nonce`, `deadline` and `tip`.
  - The service reads the header for the contiguity guard, the deadline check and the ordering window.
  - The service relays the payload from `canonical_id` on. The executor never sees the header.
  - An all-zero `sender` is exempt from the contiguity guard.
  - A frame shorter than 85 bytes is malformed.
- Kind 1: a consumer sends it to resume. The sealer checks the cursor `(from_index, from_block)` in this order:
  1. A cursor past the head of the sealer gets `REPLAY_AHEAD` (egress kind 11). The cursor is past the head when `from_index` is above the canonical count, or `from_block` is above the block that the next tick stamps.
  2. A pair that does not name one point of the stream gets `REPLAY_UNAVAILABLE`. The log line ends with `SKEWED`.
  3. A cursor below the retention floor gets `REPLAY_UNAVAILABLE`. The log line ends with `UNAVAILABLE`.
  4. Any other cursor gets the retained frames from the cursor, then `REPLAY_DONE`. A consumer exactly at the head gets `REPLAY_DONE` and no frame.
- Kind 2: the session is a canonical-stream consumer. A publisher-only session (a sequencer) never sends it.
- Kind 3: the service handles each entry as a single offered record. A malformed entry drops the rest of the batch.
- Kind 4: it carries an epoch of L1 deposits.
  - It has no guard header. The service does not parse the payload.
  - The service checks it in this order: the dedup lookup, the regression check, the gap check. Only an epoch that passes every check enters the dedup window.
  - A known id is a duplicate. The service drops it.
  - A non-advancing origin is a regression. The service drops it as malformed.
  - Once the sealer holds an origin, it accepts only the epoch of L1 block `l1_origin + 1`. Any other epoch gets an egress kind 12 that names the expected block. The first epoch at origin 0 can start at any block.
  - An accepted epoch closes the open block, adopts `l1_origin` for later boundaries, and is relayed.
- Kind 5: it carries a batch of cross-chain messages from a peer chain.
  - It has no guard header.
  - A peer chain id that is not in `remoteOrigins` gets an egress kind 6.
  - The service checks `slot_count == 2 + last_seq - first_seq`, the `first_seq` lane cursor and the anchor.
- Kind 6: the void request is a vote, not a command.
  - A consumer with a voter id sends it when the entry at `index` has no data and every archive refuses the range.
  - The service appends a void record only when every configured voter votes for the same `(index, tx_hash)`.
  - A snapshot restore keeps only the votes of the configured voters. A vote of a voter that the configuration no longer names drops. The next vote of a configured voter then decides an entry that every configured voter voted for.
  - The service refuses a vote from a stranger, with a wrong hash, for a slot that is not a `TxRef`, outside the void window, or for an entry that is already voided.
  - The ledger holds at most 1024 indices with open votes. The voter id must be below 64.
- Kind 7: the posted cursor is the system record of the batcher. It has no guard header.
  - `posted_head` is the last L2 block that the batcher confirmed on L1. The batcher sends it at start and after each change.
  - The sealer keeps the head in the replicated state. It never lets the head move down.
  - A head above the sealed head is malformed. The service drops it.
  - After a head that moves up, the sealer prints `cluster POSTED-CURSOR` and sends an egress kind 9 to every session.
- Kind 8: the seed record is a system record of the sealer. See [Seeded start](#seeded-start).
  - Only the service offers it. A frame from a client session, or a frame that is not 33 bytes, is malformed and the service drops it.
  - It carries the SHA-256 of the seed file.
- Kind 9: the recorded cursor is a system record of an executor. It has no guard header. See [Record-lag guard](#record-lag-guard).
  - Every canonical index at or below `recorded_through` is joined and recorded by the executor, or voided. `executor_id` is the void voter id of the executor.
  - The sealer keeps the cursor of each executor in the replicated state. A cursor never moves down.
  - A frame that is not 10 bytes, an `executor_id` that is not a configured voter, or a cursor at or above the canonical count is malformed. The service drops it.
  - When the best cursor moves up, the sealer sends an egress kind 9 to every session.
  - The frame is shorter than a kind-0 frame. A sealer that does not know kind 9 drops it as malformed.

### Relayed record types

The relayed payload is `[canonical_id:32][record_type:u8][fields…]`.

| `record_type` | Name | Fields |
|---|---|---|
| 0 | `TxRef` | `[shard_id:u8][term_id:i32][term_offset:i32][tx_data_session_id:i32]` |
| 1 | `DepositRef` | `[term_id:i32][term_offset:i32]` |
| 2 | Epoch | an rkyv `EpochRecord` |
| 3 | Remote epoch | an rkyv `RemoteEpochRecord` |
| 4 | Void record | `[index:u64]` |

- The void record is the payload `[tx_hash:32][record_type=4][index:u64]`.
  - The service generates it. It never relays one from a session.
  - It takes one canonical slot.
  - Every consumer drops the `TxRef` at `index`. The service removes the hash from the dedup window and sets the sender nonce back.

### Egress kinds (cluster to client)

| Kind | Name | Layout | Sent to |
|---|---|---|---|
| 1 | Relayed record | `[kind=1][index:u64][payload_len:u32][relayed payload]` | consumers |
| 2 | Boundary | `[kind=2][block_number:u64][end_tx_idx:u64][l2_timestamp:u64][l1_origin:u64]` | all sessions |
| 3 | Replay unavailable | `[kind=3][oldest_index:u64][oldest_block:u64]` | the requester |
| 4 | Replay done | `[kind=4][up_to_index:u64][up_to_block:u64]` | the requester |
| 5 | Contiguity reject | `[kind=5][sender:20][nonce:u64][expected:u64]` | the offering session |
| 6 | Remote-origin reject | `[kind=6][origin_chain_id:u64][first_seq:u64][expected_next_seq:u64][reason:u8]` | the offering session |
| 7 | Past deadline | `[kind=7][sender:20][nonce:u64][max_inclusion_block:u64][at_block:u64]` | the offering session |
| 8 | Window full | `[kind=8][sender:20][nonce:u64]` | the offering session |
| 9 | Status | `[kind=9][posted_head:u64][sealed_head:u64][budget_blocks:u64][halted:u8][retained_frames:u64][floor_index:u64][floor_block:u64][best_recorded:u64][record_lag_budget:u64][record_lag_halted:u8]` (67 bytes) | all sessions |
| 10 | DA-lag reject | `[kind=10][sender:20][nonce:u64][sealed_head:u64][posted_head:u64][budget_blocks:u64]` | the offering session |
| 11 | Replay ahead | `[kind=11][head_index:u64][head_block:u64]` | the requester |
| 12 | Origin gap | `[kind=12][offered_origin:u64][expected_origin:u64]` | the offering session |
| 13 | Record-lag reject | `[kind=13][sender:20][nonce:u64][sealed_index:u64][recorded_index:u64][budget:u64]` | the offering session |

- `index` is the 0-based canonical record index.
- Consumers are the sessions that sent a kind 2 or a kind 1. While no consumer is known, the service sends relayed records to all sessions.
- Kind 5: the sequencer rewinds its unconfirmed ledger to `expected` and republishes.
- Kind 6: `reason` is 1 `seq_mismatch`, 2 `anchor_regressed`, 3 `slot_count_mismatch`, 4 `unknown_origin` or 5 `bad_range`.
  - This egress kind 6 is not the ingress kind 6 (the void request). The two numbers are on different sides of the wire.
- Kind 7 and kind 8: see the next section.
- Kind 9: the status of the chain. `halted` is `1` when the DA-lag guard refuses user records.
  - The last 17 bytes are the record-lag guard. `best_recorded` is `u64::MAX` before the first recorded cursor. `record_lag_halted` is `1` when the record-lag guard refuses user records.
  - A reader that knows only the first 50 bytes ignores the last 17. The Rust reader reads a 50-byte frame as no recorded cursor and the guard off.
  - This egress kind 9 is not the ingress kind 9 (the recorded cursor).
  - The service sends it on each boundary tick, on each posted cursor that moves up, on each recorded cursor that moves the best cursor up, and to a session that announces itself with a kind 1 or a kind 2.
  - It is not retained. A session that announces itself gets the current status.
- Kind 10: the DA-lag guard refused a record. The sequencer drops the record and reports the `da-lag` reason. See [DA-lag guard](#da-lag-guard).
- Kind 11: the cursor of the requester is past the head of the sealer. See [Replay ahead of the head](#replay-ahead-of-the-head).
  - `head_index` is the canonical count. `head_block` is the block that the next tick stamps.
- Kind 12: the sealer refused an epoch, because an earlier epoch is missing. The sequencer offers its unconfirmed epochs again from `expected_origin`.
  - The sealer logs `cluster ORIGIN-GAP` at powers of two.
  - The check reads only replicated state, so every member refuses the same epoch.
- Kind 13: the record-lag guard refused a record. `sealed_index` is the last ordered canonical index. `recorded_index` is the best recorded cursor. The sequencer drops the record and reports the `record-lag` reason. See [Record-lag guard](#record-lag-guard).
  - The sealer sends it only while the guard is on. A sequencer that does not know kind 13 drops the frame, so turn the guard on only after every sequencer knows it.

## Egress back-pressure

An offer to a session never waits. The service thread also runs the boundary tick and relays records for every session, so a wedged client must not stop it.

- The first offer of a frame is one attempt. A back-pressure result (`BACK_PRESSURED`, `ADMIN_ACTION` or `NOT_CONNECTED`) puts the frame in the backlog of that session. `ADMIN_ACTION` gets one immediate retry first.
- While a session has a backlog, every new frame for it goes to the backlog. The session gets its frames in emission order: a replay, then `REPLAY_DONE`, then the live stream.
- The service drains the backlogs at the start of each ingress message and each timer event. The boundary timer makes a drain run at least once for each tick.
- The service closes a session once, when one of these limits is reached. It then prints `cluster EGRESS-CLOSE` with the reason.

| Limit | Value | Reason text |
|---|---|---|
| No frame leaves the backlog | 1 second. The clock restarts each time a frame leaves. | `offer deadline exhausted (back-pressure)` |
| The backlog holds more bytes | 64 MiB. A replay of the default retention fits well below it. | `egress backlog over <n> bytes` |
| A terminal offer result (not `CLOSED`) | at once | `terminal offer result <n>` |

- A session that is closing gets no more frames. The client reconnects, replays and recovers.
- A role change drops all backlogs. The clients of a leader that steps down reconnect to the new leader and replay.
- A follower never builds a backlog. Only the leader offers to clients.
- The backlogs are member-local. They do not change the replicated state, the log or the snapshot.
- A drain runs on a log callback. On an idle cluster a back-pressured session gets one egress window for each tick. A large replay to a slow client then takes longer. It does not block other sessions.

## Replay ahead of the head

A consumer can resume past the head of the sealer. This happens after a sealer wipe, when the sealer starts again behind its consumers.

- The consumer applied records that the sealer does not hold.
- The sealer answers `REPLAY_AHEAD` (egress kind 11) with its head. It logs `cluster REPLAY ... AHEAD head=(index,block)`.
- A `REPLAY_DONE` would make the consumer drop each new record below its cursor as a duplicate. The consumer would diverge with no signal.
- The consumer stops with the error `ClusterBehindCursor`. It also stops on a `REPLAY_DONE` whose head is below its cursor.
- See [the failure model](../../docs/failure-modes.md#sealer-the-aeron-cluster-raft) for the effect on each consumer.

## Dedup window and inclusion deadline

The first-seen window is the one thing that stops a lagging racing replica from getting its
re-offer ordered twice. It never evicts an id. It prunes by deadline.

- Each ingress record carries a `deadline`: the last block the sealer may order it into.
  - The ingress proxy stamps it as the newest block boundary it saw plus `--inclusion-horizon-blocks`.
  - A proxy that has seen no boundary stamps `i64::MAX`. The sealer reads the field as a signed 64-bit integer.
- The sealer checks a record in this order:
  1. The dedup lookup. An id that is in the window is a duplicate. The sealer drops it.
  2. The deadline. If the open block number is above the deadline, the result is `PAST_DEADLINE`.
  3. The capacity. If the window holds `dedupCapacity` ids, the result is `WINDOW_FULL`.
  4. The contiguity guard.
- The sealer stores the deadline clamped to `blockNumber + inclusionHorizonBlocks`.
  - The clamp only shortens the deadline. It bounds the window even if a proxy stamps a far deadline.
  - A record with no deadline of its own (an epoch or a remote-origin record) is held until `blockNumber + inclusionHorizonBlocks`.
  - An epoch record and a remote-origin record do not meet the capacity check.
    - Nothing republishes them, so a refusal would lose the deposits or the messages.
    - They are few: one per L1 block or peer batch. The window holds each for one inclusion horizon.
    - So the window grows above `dedupCapacity` by only the markers of that horizon.
- On each boundary tick the sealer drops every id whose deadline is below the new block number.
  - No copy of a pruned id can be accepted again. So a late re-offer is refused, not read as fresh.

| Result | Egress frame | What the sequencer does |
|---|---|---|
| `PAST_DEADLINE` | kind 7 | It drops the ledger entry. It reports the error to the client. The client resubmits. |
| `WINDOW_FULL` | kind 8 | It republishes the record later. The next tick that passes a deadline frees space. |

- `PAST_DEADLINE` reaches a client as the `past-deadline` reason and as a JSON-RPC error. See [the client JSON-RPC API](../../docs/json-rpc.md) and [the status events](../../docs/tx-status-events.md).
- `WINDOW_FULL` is back-pressure. The sealer takes no decision on the record.
- `dedupCapacity` is a hard cap. The default is `1 << 17` (131072 ids), about 20 MB of heap and 4 MB of snapshot.
  - An id stays in the window for at most `inclusionHorizonBlocks + 1` blocks.
  - So the window size is about the unique-record rate times the horizon times the block interval.
  - With the deploy values (64 blocks, 2000 ms), the default capacity covers about 1000 unique records each second.
  - The same capacity bounds the per-sender expected-nonce map (LRU) in the contiguity guard.
- The sealer horizon must equal the ingress horizon.
  - The sealer property is `-Dkardamom.cluster.inclusionHorizonBlocks`. The ingress flag is `--inclusion-horizon-blocks`, with env `KARDAMOM_INCLUSION_HORIZON_BLOCKS`. Both default to 64.
  - The deploy variables are `cluster_inclusion_horizon_blocks` and `inclusion_horizon_blocks`. The contract check (`just check-contract`, `just validate` and CI) fails if they differ.
- Every member must use the same `dedupCapacity` and the same horizon. They decide accept or reject inside the replicated state machine.
  - A snapshot that holds more ids than `dedupCapacity` plus 4096 (the marker slack), or more senders than the capacity, does not load.

## DA-lag guard

The DA-lag guard stops the chain from sealing far ahead of the data that the batcher posted to L1.

- The sealer refuses a user record when `budget > 0` and `sealed_head - posted_head > budget`.
  - The default budget is 10000 blocks. `0` turns the guard off.
  - The refused record is not ordered. The sealer sends an egress kind 10 to the offering session.
  - A refused record moves nothing. The nonce of the sender does not change, so a new submission is accepted as fresh.
- The guard runs after the dedup check and the deadline check, and before the capacity check and the contiguity guard.
- These inputs still enter while the guard refuses:
  - a record with an all-zero `sender` (deposits)
  - an origin record and a remote-origin record
  - the boundary tick
- The posted head comes from the ingress kind 7. The sealer replicates it, so every member decides the same way.
- The sealer sends the status (egress kind 9) to all sessions. The ingress raises a `da_lag` halt from the `halted` flag and clears it when the flag is `0`.
- The ingress `da_lag` halt pauses the submits of the ingress. See [the client JSON-RPC API](../../docs/json-rpc.md) and [the failure model](../../docs/failure-modes.md#halts-and-service-events).
- Every member must use the same budget. See `kardamom.cluster.daLagBudgetBlocks` in [Settings](#settings).

## Record-lag guard

The record-lag guard stops the chain from ordering far ahead of the transaction data that the executors recorded.

- Each executor sends its recorded cursor (ingress kind 9). The sealer keeps the cursor of each executor in the replicated state.
- The best cursor is the maximum over the configured executors. One recorded copy is enough for the other executors and the consumers. One dead or slow executor does not stop the chain.
- The sealer refuses a user record when `budget > 0` and `sealed_index - best_recorded > budget`. `sealed_index` is the last ordered canonical index.
  - The code default budget is `0`: the guard is off.
  - The refused record is not ordered. The sealer sends an egress kind 13 to the offering session.
  - A refused record moves nothing. The nonce of the sender does not change, so a new submission is accepted as fresh.
- While no executor sent a cursor, the guard refuses nothing. A new cluster, or a member that restores a snapshot with no cursor, does not halt before the first cursor.
- The guard runs at the same point as the DA-lag guard, and before it. When both guards refuse, the record gets the egress kind 13.
- These inputs still enter while the guard refuses:
  - a record with an all-zero `sender` (deposits)
  - an origin record and a remote-origin record
  - a void request and the void record
  - the boundary tick
- The status (egress kind 9) carries the best cursor, the budget and the `record_lag_halted` flag.
- Every member must use the same budget. See `kardamom.cluster.recordLagBudget` in [Settings](#settings).

## Ordering window

With priority fees on, a window of records sits in front of the record path.

- The window holds up to `orderingWindow` records (the deploy uses 20).
- It flushes in `(tip descending, arrival ascending)` order. One sender's records stay in nonce order.
- It closes on log events only, so every member relays the same order:
  - the entry count reaches the window size
  - a 5 ms cluster timer expires
  - a boundary tick
  - an origin record or a remote-origin record
  - a snapshot
- Dedup, the deadline check, the capacity check, the contiguity guard and the index assignment run at the flush, in the flush order.
- `0` passes every record through at once.
- Every member must use the same value. The snapshot carries it. A member that restores a snapshot taken with another value halts.
- The deploy sets the window and the sequencer `[fees] priority` from one value, `PRIORITY_FEES`. See [priority fees](../../docs/priority-fees.md).
- The node sets the Aeron timer wheel tick to 1 ms, so the 5 ms timer fires on time.

## Start modes

The member decides how it starts once, before it launches. The line `cluster START mode=<mode>` shows the mode.

| Mode | When | What the member does |
|---|---|---|
| `RESUME` | The cluster directory holds a recording log with at least one entry. | It starts from its own recording log. |
| `GENESIS` | The directory is blank and the bootstrap is on. | It starts at log position 0. |
| `SEED_FROM_PEER` | The directory is blank and the bootstrap is off. | It copies the latest snapshot from a peer, then starts from it. |

- A blank member has no recording log entry.
- The bootstrap is on when either input says `true`. An unset or empty input is off. Any other text stops the start.
  - The property `-Dkardamom.cluster.bootstrap=true`. Use it for a launcher that always starts a new cluster, such as a local test stack.
  - The file `bootstrap` in the Nomad task directory (`$NOMAD_TASK_DIR`). The member reads it at each start. The deploy writes `true` into it only during the first deploy of a new cluster. See [the deploy README](../../deploy/cluster/README.md#sealer-bootstrap).
- A blank member must not start at position 0 in a running cluster. It cannot catch up after the leader purges its log. Blank members that elect each other start a second history.

### Peer seed

A blank member without the bootstrap copies state from a running peer before it launches.

- Each round clears the cluster directory and the archive directory. It then runs an Aeron ClusterBackup with `LATEST_SNAPSHOT` against the consensus endpoints of the other members.
- The backup writes the snapshot and the recording log into the own directories of the member. The recording log keeps the real log position, so the member joins at the snapshot position, not at 0.
- The round ends when the backup reaches `BACKING_UP`. The member then launches. It restores the snapshot and catches up from the leader.
- A round ends with no result when no peer answers for 20 s. The next round starts after a backoff of 1 s that doubles up to 30 s.
- A blank member never falls back to position 0. It waits and prints `cluster SEED waiting-for-peer`.
- If the cluster has no snapshot yet, the member holds the whole log. The line `cluster SEED from-peer` shows `snapshotPosition=-1`.

## Seeded start

A sealer cluster that lost all its state can start after a block `H` that `kardamom-reconstruct` rebuilt from L1. The consumers resume at `H`, so a start at genesis would put the sealer behind them. See [Replay ahead of the head](#replay-ahead-of-the-head).

1. Run `kardamom-reconstruct --sealer-seed <file>` (see [the L1 data path](../../docs/l1-data-path.md#kardamom-reconstruct)).
2. Give the same file to every member in `-Dkardamom.cluster.seedSnapshot=<path>`.
3. Start the members as a new cluster, with empty cluster and archive directories and the bootstrap on.

- The seed file is versioned and big-endian. It holds:
  - the magic `KSED` and the version (1)
  - the chain id, `H`, the canonical end `E_H` of `H`, the timestamp of `H` and its L1 origin
  - the state root
  - the senders and the next nonce of each
- The digest is the SHA-256 of the file. `sha256sum` of the file equals the `digest=` in the log.
- A member that restores a snapshot ignores the seed.
- An unreadable or invalid seed stops the start.
- A member with a non-empty `remoteOrigins` refuses a seed and does not start. The seed holds no peer anchor, so a seeded cluster runs with interop off.

The seeded state:

- The next block is `H + 1`. The next canonical index is `E_H`. The open block is empty.
- The timestamp and the L1 origin come from the seed.
- The posted head is `H`, so the DA-lag guard does not halt the chain at once.
- The dedup window and the void ledger are empty.
- The nonce guard holds the seeded senders, the eldest first. A guard with a smaller capacity keeps the most recent senders.
- The egress opens at `(E_H, H + 1)`. A consumer at the rebuilt head resumes there.

The seed record (ingress kind 8) proves that every member started from the same seed:

- While the seed is not confirmed, every member offers the record in each new leadership term. Aeron needs every member to offer the same service message at the same log point.
- The first record in the log confirms the seed. A second record changes nothing.
- The leader then asks for a Raft snapshot. A member that joins later restores the seeded state from it and never replays the record.
- A member that started at genesis, or from another seed, prints `sealer SEED-EPOCH FATAL` and stops before it relays a record. A blank member that replays the log from position 0 without the seed fails this way.

## Settings

The service reads these JVM system properties. The deploy passes them in `JAVA_TOOL_OPTIONS`.

| Property | Code default | Every member must match | Meaning |
|---|---|---|---|
| `kardamom.cluster.members` | none (required) | yes | The member list: `id,ingress,consensus,log,catchup,archive` for each member, separated by `\|`. |
| `kardamom.cluster.memberId` | `-1` | no | This member id. A value below 0 means: find the member whose ingress host equals `nodeIp`. |
| `kardamom.cluster.nodeIp` | none | no | This node IP. It is required when `memberId` is not set. |
| `aeron.dir` | `/opt/kardamom/aeron-mount/dir` | no | The Aeron media-driver directory. |
| `kardamom.cluster.dir` | `/opt/kardamom/cluster` | no | The cluster (Raft log and mark file) directory. |
| `kardamom.archive.dir` | `/opt/kardamom/archive` | no | The Aeron archive directory. |
| `kardamom.cluster.ingressStreamId` | `101` | yes | The ingress stream id that clients offer to. |
| `kardamom.cluster.tickMs` | `2000` | recommended | The block interval in ms. The leader arms the boundary timer with it. |
| `kardamom.cluster.dedupCapacity` | `131072` | yes | The hard cap of the dedup window. |
| `kardamom.cluster.inclusionHorizonBlocks` | `64` | yes | The deadline clamp. It must equal the ingress horizon. |
| `kardamom.cluster.orderingWindow` | `0` | yes | The ordering window size. `0` is off. |
| `kardamom.cluster.remoteOrigins` | empty (env `KARDAMOM_REMOTE_ORIGINS`) | yes | The peer chain ids that the sealer accepts kind-5 records from. Empty turns interop off. The property wins over the env var. A bad entry stops the start. |
| `kardamom.cluster.voidVoters` | empty (env `KARDAMOM_VOID_VOTERS`) | yes | The voter ids, for example `0,1,2,3,4`. Empty refuses every void request. Each id must be below 64. |
| `kardamom.cluster.voidWindow` | `65536` | yes | The number of newest canonical indices that a void can name. |
| `kardamom.cluster.daLagBudgetBlocks` | `10000` (env `DA_LAG_BUDGET_BLOCKS`) | yes | The DA-lag budget in blocks. `0` turns the guard off. The property wins over the env var. An empty value gives the default. A value that is not a number, or is negative, stops the start. |
| `kardamom.cluster.recordLagBudget` | `0` (env `KARDAMOM_RECORD_LAG_BUDGET`) | yes | The record-lag budget in canonical records. `0` turns the guard off. The property wins over the env var. An empty value gives the default. A value that is not a number, or is negative, stops the start. In this release a value above `0` also stops the start, because the snapshot writer writes version 10. |
| `kardamom.cluster.retention` | `65536` | recommended | The minimum number of egress frames kept for replay. |
| `kardamom.cluster.adminPort` | `0` (off) | no | The admin server port. |
| `kardamom.cluster.readyLagBytes` | `4194304` (4 MiB) | no | The most that the service can lag the commit position and still be ready. |
| `kardamom.cluster.snapshotIntervalS` | `300` | no | The interval of the automatic snapshot. `0` turns it off. |
| `kardamom.cluster.joinWatchdogS` | `60` | no | The member exits with code 3 if its election stays in `INIT` for this long. `0` turns it off. |
| `kardamom.cluster.fileSyncLevel` | `0` | no | The sync level of the Raft log and the archive. Values: `0`, `1`, `2`. Any other value stops the start. |
| `kardamom.cluster.bootstrap` | off | no | `true` starts a blank member at log position 0. The file `bootstrap` in `$NOMAD_TASK_DIR` has the same effect. See [Start modes](#start-modes). |
| `kardamom.cluster.seedSnapshot` | none | yes, on a new seeded cluster | The path of the seed file. A cluster with no snapshot starts after the head of the seed, not at genesis. See [Seeded start](#seeded-start). |

- Block interval: the code default `tickMs` is 2000 ms. The deploy also passes `2000`.
  - The state machine floors the `l2_timestamp` of each boundary to a multiple of 250 ms.
  - The constant `TICK_INTERVAL_MS` (250) is that floor. It is also the interval of the test constructors.
- `voidVoters`: the deploy sets the list to `0` to `executor_count + 1`. Executor `i` has voter id `i`. The validator has `executor_count`. The batcher has `executor_count + 1`.
- `remoteOrigins`, `voidVoters` and `voidWindow` are not in the snapshot. A different value on one member makes it decide differently from the others.
- `fileSyncLevel`: `0` leaves a write in the page cache. `1` syncs the data of each write batch. `2` syncs the data and the file metadata.
  - At level 0 an entry that a quorum acknowledged can exist only in page caches. A power loss that takes the members together drops it.
  - The deploy passes `1` (variable `cluster_file_sync_level`, env `KARDAMOM_CLUSTER_FILE_SYNC_LEVEL`).
- `retention` is a minimum window. A frame past the window leaves only when its block is at or below the posted head.
  - The batcher can always replay from its cursor, because the sealer keeps each frame that is not posted.
  - The stretch has a bound: the budget plus one flush of blocks, in heap.
  - The snapshot carries the retained frames. A member that restores a snapshot keeps the floor at the posted head. It answers older ranges with `REPLAY_UNAVAILABLE`.
  - With no budget (`0`), nothing bounds the stretch.
  - Use the same value on every member, so the replay range does not change after a failover.
- `daLagBudgetBlocks`: the start-up line `cluster da-lag budget` shows the value. See [DA-lag guard](#da-lag-guard).
- `recordLagBudget`: the start-up line `cluster record-lag budget` shows the value. See [Record-lag guard](#record-lag-guard).
  - The deploy does not pass it yet, so the guard is off.
  - While the member writes snapshot version 10, a value above `0` stops the start. A version-10 snapshot holds no recorded cursors, so a member that restores one would decide differently from its peers. The release that writes snapshot version 11 lifts this check. See [Snapshot](#snapshot).
  - The budget and `daLagBudgetBlocks` are not in the snapshot.
- `seedSnapshot`: the deploy passes the job variable `cluster_seed_snapshot`.
  - The variable is empty in a normal deploy. An empty path means no seed.
  - When the variable is set, the deploy passes an empty `remoteOrigins`.
  - Every member mounts `/opt/kardamom/seed` read-only. The procedure is the runbook [`sealer-fleet-rebuild`](../../docs/runbooks/sealer-fleet-rebuild.md).
- The deploy does not pass `dedupCapacity`, `voidWindow`, `readyLagBytes` or `joinWatchdogS`. They keep the code defaults. The deploy sets the bootstrap through the task file, not through the property.
- The Aeron settings that the node fixes: client sessions time out after 90 s, at most 256 sessions, an 8 MB log term, and the application version is 0.3.0.

## Admin server

The admin server reports the member status over HTTP. It is off when `adminPort` is `0`. The deploy uses port `40205`.

| Request | Answer |
|---|---|
| `GET /status` | Always 200 with the status JSON. |
| `GET /ready` | 200 when the member is ready. 503 when it is not. The body is the status JSON. |
| any other path | 404 |

- The server listens on `0.0.0.0`. Two members on one host cannot share the port, so the port is off by default.
- The status JSON has `memberId`, `role`, `election`, `commitPosition`, `servicePosition` and `ready`.
- A member is ready when all of these are true:
  - the role is `LEADER` or `FOLLOWER`
  - the election state is `CLOSED`
  - `commitPosition - servicePosition` is at most `readyLagBytes`
- A member in an election, or a follower that still catches up, is not ready. A closing member reads `CLOSED` for the role and is never ready.
- If the status cannot be read, `/ready` and `/status` answer 503 with an `error` field.
- The Nomad job registers a Consul check of the path `/ready`. A rolling deploy waits for it before it stops the next member.

## Log lines

The service prints on stdout. Each line starts with a UTC instant, then the text in the table.
The chaos suite and operators read these lines. The sealer has no other observability surface.

| Line starts with | Meaning |
|---|---|
| `cluster role=<role> memberId=` | The member role changed. |
| `cluster TERM` | A leadership term began (`leadershipTermId`, `leaderMemberId`, `logPosition`, `role`, `block`). All members print it, also on replay. |
| `cluster boundary-clock TICK` | A heartbeat. It prints once for each 30 boundary ticks. It shows that the boundary clock runs. |
| `cluster boundary-clock REVIVE` | The leader found no tick for three tick intervals and armed the timer again. |
| `cluster SESSION open` / `cluster SESSION close` | A client session opened or closed. The close line has the reason. |
| `cluster CONTIGUITY-REJECT` | The guard refused a record with a nonce gap. |
| `cluster PAST-DEADLINE` | The sealer refused a record past its deadline. |
| `cluster DA-LAG-REJECT` | The DA-lag guard refused a record (`nonce`, `sealedHead`, `postedHead`, `budget`, `totalDaLagRejected`). |
| `cluster RECORD-LAG-REJECT` | The record-lag guard refused a record (`nonce`, `sealedIndex`, `bestRecorded`, `budget`, `totalRecordLagRejected`). |
| `cluster POSTED-CURSOR` | The posted head moved up (`postedHead`, `sealedHead`, `retained`, `halted`). |
| `cluster WINDOW-FULL` | The dedup window is at capacity. The line shows the size and the capacity. |
| `cluster REMOTE-ORIGIN-REJECT` | The sealer refused a remote-origin record. The line shows the reason code. |
| `cluster VOID-VOTE` | A void vote changed the count (`voter`, `index`, `result`, `votes` as `n/total`). A repeated vote prints nothing. |
| `cluster DROPPED malformed` | The service dropped a malformed frame. The line names the frame type. A non-zero count means that the Java and Rust layouts differ. |
| `cluster REPLAY` | A replay request. A served replay ends with `served=`, `queued=` and `dropped=`. A refusal ends with `AHEAD head=(index,block)`, `SKEWED` or `UNAVAILABLE`. `AHEAD` means that the cursor is past the head. `SKEWED` means that `from_index` is outside the block that `from_block` names. |
| `cluster EGRESS-CLOSE` | The service closed a client session because it could not take its frames. The line has the reason. See [Egress back-pressure](#egress-back-pressure). |
| `sealer snapshot TAKEN` | A snapshot was taken (`block`, `canonicalCount`). A member that replays old log entries also prints it. |
| `sealer snapshot RESTORED` | The member started from a snapshot. The line shows `block`, `canonicalCount`, `retained` and `postedHead`. |
| `sealer state FRESH at genesis` | The member started with no snapshot and no seed. |
| `sealer state SEEDED` | The member started from a seed (`block`, `endTx`, `senders`, `stateRoot`, `digest`). |
| `sealer seed CONFIRMED` | The first seed record is in the log (`block`, `canonicalCount`). |
| `sealer seed SNAPSHOT` | The seeded leader asked for a snapshot (`requested=true` or `false`). A refused request leaves the periodic snapshot to take one. |
| `sealer SEED-EPOCH FATAL` | The log holds a seed record, but this member started at genesis or from another seed. The member stops. |
| `sealer SEED-EPOCH offer FAILED` | The offer of the seed record failed. The next leadership term offers it again. |
| `cluster START mode=` | The start mode of the member (`RESUME`, `GENESIS` or `SEED_FROM_PEER`). |
| `cluster SEED start` / `cluster SEED from-peer` | A blank member starts to copy a snapshot from a peer, and then ends the copy (`snapshotPosition`, `round`). |
| `cluster SEED waiting-for-peer` | A round of the peer seed ended with no result (`outcome`, `backupState`, `retryInMs`). The member waits and does not start at genesis. |
| `cluster SEED backup error` | The ClusterBackup of the peer seed reported an error. |
| `cluster SNAPSHOT triggered` / `cluster SNAPSHOT attempt failed` | The scheduler asked for a cluster snapshot, or the attempt failed. The next tick tries again. |
| `cluster LAUNCH RETRY` | The launch found a stale mark file from a killed process. The node waits and retries, up to 6 attempts. |
| `cluster LAUNCH REPAIR` | The archive had a torn last fragment. The node truncates it and launches again. |
| `cluster JOIN WEDGE` | The election stayed in `INIT` for longer than `joinWatchdogS`. The process halts with exit code 3. |
| `cluster TERMINATION` | The consensus module or the service container asked for a shutdown. |
| `cluster node up`, `cluster admin endpoint`, `cluster snapshot scheduler`, `cluster join watchdog`, `cluster da-lag budget`, `cluster record-lag budget`, `cluster ordering window`, `cluster remote-origin allowlist`, `cluster seed`, `cluster void voters` | Start-up lines. They show the settings that the member uses. |

- The lines `CONTIGUITY-REJECT`, `PAST-DEADLINE`, `DA-LAG-REJECT`, `RECORD-LAG-REJECT`, `WINDOW-FULL`, `REMOTE-ORIGIN-REJECT` and `DROPPED` print when their count is a power of two (1, 2, 4, 8, and so on).
- There is no Prometheus counter for these events.

## Snapshot

- The member reads snapshot versions 1 to 11. It writes version 10.
  - The writer stays one version behind the reader. A member of the previous release reads up to version 10, so it can restore every snapshot that this release writes. A rollback does not stop the old members.
  - A version-10 snapshot holds no recorded cursor. A member that restores one has no cursor until the next cursor record. With the record-lag budget at `0` this changes no decision.
  - The release that turns on the cursor publisher and the record-lag guard writes version 11. Before that release, a `recordLagBudget` above `0` stops the start.
  - A snapshot before version 9 restores a posted head of `0`. A snapshot before version 10 restores a state that started at genesis.
  - A snapshot before version 11 restores no recorded cursor. The record-lag guard then refuses nothing until the first cursor.
- The state section holds:
  - the dedup window with the deadline of each id
  - the per-sender nonces
  - the remote-origin state
  - the void ledger
  - the ordering window size
  - the posted head, after the ordering window size
  - the seed status (1 byte) and the seed digest (32 bytes), after the posted head
  - from version 11 only, the recorded cursors, after the seed digest: `[count:u8]`, then `[executor_id:u8][recorded_through:u64]` for each executor, in executor-id order
    - A restore drops the cursor of an executor that is not a configured voter.
- The retained egress frames follow the state section. See `retention` in [Settings](#settings).
- A snapshot that was taken with another `orderingWindow` does not load. The member halts.
- The Raft snapshot interval is `snapshotIntervalS`. The leader triggers it. The action is a log entry, so all members snapshot at the same position.

## Build and test

Requires a JDK 17 (`JAVA_HOME`). The Gradle wrapper downloads Gradle 8.7 on first run.

```sh
# Deterministic state-machine tests (no Aeron jars needed):
./gradlew :core:test

# Compile the Aeron ClusteredService adapter and run its tests:
./gradlew build
```

- The `:core` tests cover the state machine without Aeron.
- The `:service` tests run an in-process Aeron `TestCluster` (failover, replay, fan-out, snapshot restore, the admin server).
- The docker e2e leader-kill test is the gated, real-cluster layer.

## Running in a cluster

Each of the three cluster nodes runs one JVM. The JVM hosts the `ConsensusModule` and a
`ClusteredServiceContainer` that wraps `SealerClusteredService`. The main class is
`io.kardamom.sealer.cluster.ClusterNode`.

- The member endpoints (`ingress`, `consensus`, `log`, `catchup`, `archive`) come from `kardamom.cluster.members`.
- The deploy uses ports 40200 to 40204 for these endpoints, and 40205 for the admin server.
- The Rust sequencers and executors connect through `cluster-adapter` in cluster mode.
- The Nomad job is `deploy/cluster/nomad/cluster.nomad.hcl`. See [the deploy README](../../deploy/cluster/README.md).

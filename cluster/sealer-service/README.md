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
| 11 | Feature activation | `[kind=11][l1_origin:u64][feature_id:u64][activation_ms:u64]` (25 bytes) |

- Kind 0: the guard header is `sender`, `nonce`, `deadline` and `tip`.
  - The service reads the header for the contiguity guard, the deadline check and the ordering window.
  - The service relays the payload from `canonical_id` on. The executor never sees the header.
  - An all-zero `sender` is exempt from the contiguity guard.
  - A frame shorter than 86 bytes is malformed: it must hold the record type after the id. Kinds 4 and 5 also need the record type after their header.
  - With `carryBytes`, a kind-0 record of type 6 (the canonical entry) is malformed: only a full entry (type 5) becomes a canonical entry.
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
  - A void sets the expected nonce of the sender to the lower of the expected nonce and the nonce of the voided entry. Two voids of one sender, in either order, leave the lowest voided nonce open.
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
  - When the best cursor moves up, the sealer sends an egress kind 9 to every session. It prints `cluster RECORDED-CURSOR` at power-of-two counts of these moves.
  - The frame is shorter than a kind-0 frame. A sealer that does not know kind 9 drops it as malformed.
- With `carryBytes`, a member drops kinds 6 and 9 as unknown kinds. Its snapshot holds no void ledger and no recorded cursor.
- Kind 11: the feature activation is a system record. It has no guard header. See [Decision version](#decision-version).
  - The upgrade transaction of the epoch at `l1_origin` schedules `feature_id` at `activation_ms`. No sequencer sends it.
  - The member follows no feature: the record changes no state. Each frame prints `cluster FEATURE-ACTIVATION` with the outcome `IGNORED`.
  - A frame that is not 25 bytes is malformed.

### Relayed record types

The relayed payload is `[canonical_id:32][record_type:u8][fields…]`.

| `record_type` | Name | Fields |
|---|---|---|
| 0 | `TxRef` | `[shard_id:u8][term_id:i32][term_offset:i32][tx_data_session_id:i32]` |
| 1 | `DepositRef` | `[term_id:i32][term_offset:i32]` |
| 2 | Epoch | an rkyv `EpochRecord` |
| 3 | Remote epoch | an rkyv `RemoteEpochRecord` |
| 4 | Void record | `[index:u64]` |
| 5 | Full entry, on ingress only | `[lane:u8][envelope]` |
| 6 | Canonical entry, in the retained file only | `[lane:u8][envelope]` |

- The void record is the payload `[tx_hash:32][record_type=4][index:u64]`.
  - The service generates it. It never relays one from a session.
  - It takes one canonical slot.
  - Every consumer drops the `TxRef` at `index`. The service removes the hash from the dedup window and sets the sender nonce back.
- With `carryBytes`, the service relays a full entry (type 5) as a canonical entry (type 6): the same payload with the record type 6. The retained file keeps the kind-1 frame with the envelope. Each egress session gets the relayed reference (egress kind 15) in its place, live and in a replay. No egress session gets the envelope.
- Without `carryBytes`, the service relays a type 5 as is, like every other payload.

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
| 9 | Status | `[kind=9][posted_head:u64][sealed_head:u64][budget_blocks:u64][halted:u8][retained_frames:u64][floor_index:u64][floor_block:u64][best_recorded:u64][record_lag_budget:u64][record_lag_halted:u8][snapshot_follows:u8]` (68 bytes) | all sessions |
| 10 | DA-lag reject | `[kind=10][sender:20][nonce:u64][sealed_head:u64][posted_head:u64][budget_blocks:u64]` | the offering session |
| 11 | Replay ahead | `[kind=11][head_index:u64][head_block:u64]` | the requester |
| 12 | Origin gap | `[kind=12][offered_origin:u64][expected_origin:u64]` | the offering session |
| 13 | Record-lag reject | `[kind=13][sender:20][nonce:u64][sealed_index:u64][recorded_index:u64][budget:u64]` | the offering session |
| 15 | Relayed reference | `[kind=15][index:u64][canonical_id:32][record_type:u8]` (42 bytes) | consumers, with `carryBytes` |
| 17 | Sealer info | `[kind=17][decision_version:u8][cluster_session_id:i64]` (10 bytes) | each session when it opens, and every session at each new term |

- Kind 17: a sequencer reports the version that it got last for its current session (`kardamom_sequencer_sealer_version`). A reader that does not know kind 17 drops it.

- `index` is the 0-based canonical record index.
- Consumers are the sessions that sent a kind 2 or a kind 1. While no consumer is known, the service sends relayed records to all sessions.
- Kind 5: the sequencer rewinds its unconfirmed ledger to `expected` and republishes.
- Kind 6: `reason` is 1 `seq_mismatch`, 2 `anchor_regressed`, 3 `slot_count_mismatch`, 4 `unknown_origin` or 5 `bad_range`.
  - This egress kind 6 is not the ingress kind 6 (the void request). The two numbers are on different sides of the wire.
- Kind 7 and kind 8: see the next section.
- Kind 9: the status of the chain. `halted` is `1` when the DA-lag guard refuses user records.
  - The last 17 bytes are the record-lag guard. `best_recorded` is `u64::MAX` before the first recorded cursor. `record_lag_halted` is `1` when the record-lag guard refuses user records.
  - A reader that knows only the first 50 bytes ignores the last 17. The Rust reader reads a 50-byte frame as no recorded cursor and the guard off.
  - Byte 67 is the snapshot notice. `snapshot_follows` is `1` on the status before a snapshot: the consensus module sends nothing until the snapshot is complete. A client then waits for the next frame for 60 s, not 10 s. See [Snapshot](#snapshot). A reader reads a 67-byte frame as no snapshot.
  - The frame is 68 bytes. With `carryBytes`, the tail `[retained_bytes:u64]` follows the flag at bytes 68 to 75 (76 bytes): the bytes of the retained file above the posted head. See [Retained file](#retained-file).
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
| A frame above the maximum message length of the session's egress publication (its term divided by 8) | at once | `frame above the max message length of the egress publication kind=<k> index=<i> length=<n> repeats=<r> channel=<c>`. `index` is the canonical index of a relayed frame, or `-1` for a kind with no index. `repeats` counts the closes in a row on the same frame (the same kind and index) for the response channel. A value above 0 means the client reconnects with an egress term that is too small. A session close that the service did not ask for clears the count of its channel, and so does a role change. |

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

- Each executor with `--exec-cursor` on sends its recorded cursor (ingress kind 9). The sealer keeps the cursor of each executor in the replicated state.
  - The executor sends a cursor that moved when 100 ms passed since the last send, or at once when it moved by 1024 records. It never sends a lower cursor than the last one that the session took.
  - The flag is off by default. Switch it on only after every member reads kind 9.
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
- With `carryBytes`, the round then copies the segment files of the retained file from the admin endpoint of a peer (`GET /retained`).
  - The round reads the sealer snapshot that the backup copied. It walks the copy from the front to the head of that snapshot, as a restore does. See [Retained file](#retained-file).
  - The line `cluster SEED retained-copy` shows the peer and the segment count. A copy that fails the walk prints `cluster SEED retained-copy FAILED` with the reason, and the round tries the next peer.
  - A round in which no peer serves a copy that passes ends with no result, and prints `cluster SEED retained-copy MISMATCH`. After 5 such rounds the seed prints `cluster SEED retained-copy GAVE-UP` and stops the start. See [Recover a member that stops on its retained file](#recover-a-member-that-stops-on-its-retained-file).
  - A cluster with no snapshot needs no copy: the member replays the whole log.

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
| `kardamom.cluster.carryBytes` | `false` | yes | `true` keeps the retained frames in an append-only file, not on the heap, and writes snapshot version 12. It also relays a full entry as a canonical entry, and drops the void request and the recorded cursor. The release gate refuses a rolling change of it. It needs `adminPort`, the env var `KARDAMOM_RETAINED_TOKEN` and a `snapshotIntervalS` above `0`. Any value other than `true`, `false` or empty stops the start. See [Retained file](#retained-file). |
| `kardamom.cluster.feedAeronDir` | empty (off) | no | The directory of the media driver that the member shares with its paired executor. A value turns the member feed on. It needs `carryBytes=true`; without it the start stops. See [Member feed](#member-feed). |
| `kardamom.cluster.feedRingBytes` | `16777216` (16 MiB) | no | The frame ring of the member feed, off the heap. A power of two of at least 8 MiB; another value stops the start. |
| env `KARDAMOM_RETAINED_TOKEN` | none | yes, with `carryBytes` | The shared token that a request for the retained file must carry. The member reads it only from the env var, because `ps` shows the system properties. A log line never prints it. |
| `kardamom.cluster.retainedSegmentBytes` | `67108864` (64 MiB) | recommended | The size of a new segment of the retained file. The smallest value is 1 MiB, one session message. A smaller value stops the start. |
| `kardamom.cluster.adminPort` | `0` (off) | no | The admin server port. |
| `kardamom.cluster.readyLagBytes` | `4194304` (4 MiB) | no | The most that the service can lag the commit position and still be ready. |
| `kardamom.cluster.snapshotIntervalS` | `300` | no | The interval of the automatic snapshot, in seconds. The leader asks for a snapshot on every boundary tick whose block number is a multiple of `snapshotIntervalS * 1000 / tickMs`. `0` turns it off. An interval shorter than one tick stops the start. |
| `kardamom.cluster.joinWatchdogS` | `60` | no | The member exits with code 3 if its election stays in `INIT` for this long. It exits with code 6 when the completion of its leader election failed: the control toggle is active, but the election state is not `CLOSED`, for longer than the archive message timeout plus the driver timeout. It exits with code 7 or 8 when an election stalls for the stall limit (see [Election stalls](#election-stalls)). `0` turns the join watchdog off, and with it the exit on a closed component and the stall rules. |
| `kardamom.cluster.catchupStallS` | `300` | no | A member that tried a catch-up, and makes no progress for this long, drops its recording log and exits with code 4, unless every peer answers with an open election. The relaunch seeds the member from a peer. `0` turns the seed off: the member then never drops its log. |
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
- `carryBytes`: the deploy does not pass it yet, so the retained frames stay on the heap.
  - Switch it only with a new chain or a coordinated cut-over, never on a live chain. A member with the file stops on a snapshot before version 12, and a member without the file stops on a version-12 snapshot. Both print `sealer RETAINED-FILE FATAL`.
  - `snapshotIntervalS=0` with `carryBytes` stops the start: the purge of the file runs at each snapshot, so the file would never shrink.
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
- The job spec names no decision version: every member runs version 3. The line `sealer decision rules` shows the version after the start. See [Decision version](#decision-version).
- The Aeron settings that the node fixes: client sessions time out after 90 s, at most 256 sessions, an 8 MB log term, sparse ingress images, and the application version is 0.3.0.
- One session message, its 32-byte session header included, holds at most 1 MiB: the 8 MB log term divided by 8.
  - A message above one MTU travels as many Aeron fragments. The consensus module and the service reassemble them.
  - A channel carries at most its own term divided by 8. Each Rust client sets the term of its ingress publication and of its egress channel in `[cluster]`, with `ingress_term` and `egress_term`. The client refuses a payload above its ingress bound before the offer.

    | Name | Term | Largest message | Busy log (3 terms) |
    |---|---|---|---|
    | `control` (default) | 1 MiB | 128 KiB | 3 MiB |
    | `entries` | 2 MiB | 256 KiB | 6 MiB |
    | `large` | 8 MiB | 1 MiB | 24 MiB |

  - The member runs an embedded media driver. The ingress image and the egress publication of each session live on its tmpfs and count against the 1024 MB memory limit of the job, with the 384 MB heap. All logs are sparse, so a quiet or short-lived session takes only the pages it writes. A session asks for a larger term only on the channel that carries large messages.
  - An egress frame above the maximum message length of a session's egress publication closes that session. See [Egress back-pressure](#egress-back-pressure).
  - Deploy order: deploy the clients before the sealers, and raise a consumer's `egress_term` before any producer sends larger entries. A client with a smaller egress term than a frame (an older client, or a pinned `term-length`) is closed, reconnects, replays the same frame, and is closed again. The `EGRESS-CLOSE` line counts these repeats (`repeats=<n>`) for each response channel.

### Retained file

With `carryBytes=true`, the member keeps its retained frames in an append-only file in `<kardamom.cluster.dir>/retained`.

- The file is a sequence of segment files, `retained-<start>.seg`. `<start>` is the position of the first byte, with 20 digits. A position counts the bytes of all frames from the start of the file.
- Each frame is a record `[len:u32 LE][frame]`. A record never crosses a segment. The records of a segment end where the next segment starts. A segment file can be longer: the rest holds the zeros of the preallocation.
- The service thread appends each frame through the memory-mapped buffer of the last segment. The common path needs no system call.
- A helper thread prepares the next segment: it writes zeros into all of it, syncs it and maps it. The file `segment.next` is the prepared segment. The service thread names it by its start position when it takes it, and syncs the directory.
- The heap keeps only an index: the end index and the file position of each retained boundary.
- The floors and the eviction rule are the same as on the heap. The file serves the same replay bytes.
- The snapshot carries the floors, the frame count, the front and head positions, and the index. It does not carry the frames. See [Snapshot](#snapshot).
  - Before the member writes the snapshot, it forces the mapped segments to disk up to the head.
  - Then it deletes each segment that ends at or below the front position of the snapshot before. So a peer seed from the latest snapshot finds every segment that it needs.
  - The purge lags the front: a segment leaves the disk one to two snapshot intervals after the front passes it. Plan for the disk of two snapshot intervals and two segments above the floor.
- A restore makes the file match the snapshot:
  1. It deletes the segments at or above the head position, and the segments below the front position.
  2. It walks the records from the front to the head. Each record must be a whole relayed frame or boundary frame inside its segment, and each boundary must match the index. The walk must end exactly at the head, with the frame count and the boundary count of the snapshot. Any other byte, such as the zeros of a preallocated segment, stops the member with `sealer RETAINED-FILE FATAL`.
  3. It writes zeros into the last segment from the head position on.
  4. The log replay appends the frames after the snapshot.
- The eviction and the replay read the records of the file. Every record from the front to the head is one that the member wrote, or that the walk checked, so the reads act alike on every member.
- A member that starts with no snapshot deletes every segment. The log replay writes the file again from position 0.
- Any I/O error on the file stops the member with `sealer RETAINED-FILE FATAL`. A full disk fails the preallocation of the helper thread. The member stops at the next boundary tick, or at the next segment change. A file error never changes the replicated state or the egress.
- The admin endpoint serves the segments to a member that a peer seeds, only below the head that the member published at its last boundary tick, snapshot or restore. See [Admin server](#admin-server).

### Member feed

With `feedAeronDir`, each member sends its retained frames to its paired executor over IPC, on the media driver of the executor node. The leader and the followers send the same frames. The wire layout, the answer rules and the states are in section 7.8 of `docs/specs/2026-10-10-raft-carried-transactions.md`.

- Stream 1006 carries the feed, member to executor. Stream 1007 carries the replay requests, executor to member.
- The feed frames are kinds 1 and 2, the answers 3, 4 and 11, and kind 16 (`FEED_GAP`). Each answer carries the id of its request as an 8-byte tail. The feed carries no status.
- The service thread writes each retained frame into a ring off the heap. It never waits for the feed. The feeder thread (`sealer-feeder-<id>`) offers the frames to the publication. Only the feeder waits on a back-pressured offer.
- A replay request goes from the feeder to the service through a second ring. The service checks the cursor as for egress kind 1, walks the retained file from it (at most 256 KiB in each duty cycle), sends `REPLAY_DONE`, and then sends each new frame. The walk does not depend on log traffic: the service runs it in `doBackgroundWork`.
- A live frame that does not fit the ring ends the answer with `FEED_GAP`. The member then writes nothing until the next request. A full ring during a walk only pauses the walk.
- A lost subscriber ends the answer. The feeder drops the unsent frames.
- The feeder has its own Aeron client. When the driver goes, the client closes, and the feeder opens a new one every 1 s. A feed error never stops the member and never changes the replicated state.
- The feed log is not sparse, so the driver of the executor node pays for its 24 MiB. The member pays for its 16 MiB ring.
- Log lines: `sealer FEED START`, `OPEN`, `NO-DRIVER`, `CONNECTED`, `DISCONNECTED`, `CLOSED`, `DROPPED`, and for each request `REPLAY`, `LIVE`, `GAPPED` and `IDLE`.

### Recover a member that stops on its retained file

A member stops with `sealer RETAINED-FILE FATAL` when its file does not match its snapshot, and its peer seed stops with `cluster SEED retained-copy GAVE-UP` when no peer serves a matching copy. The member keeps its recording log, so the next start resumes from the same snapshot and stops again. Seed it again:

1. Make sure that a quorum of the other members runs, and that they took a snapshot after the failure.
2. Stop the member: `nomad alloc stop <alloc>`, or stop its job.
3. Wipe its state on the host: the cluster directory (with `retained/` in it) and the archive directory. Keep the directories themselves.
4. Start the member. It finds no recording log, so it seeds from a peer and copies the retained file (`cluster START mode=SEED_FROM_PEER`).
5. Check the log for `cluster SEED retained-copy` with no `FAILED`, then `sealer snapshot RESTORED`.

- If the seed gives up again, the peers purged the segments during the copy. Wait for the next snapshot of the peers, and start the member again.

### Member names

The member list names each member by its Consul node name, for example `sealer-0.node.dc1.consul`. Every node resolves these names through its own Consul agent.

- The media driver looks a name up each time a component adds a channel, and again while a destination sends no status. Each election adds channels: the log publication and its destinations, the log subscription, and the archive replication.
- The driver keeps the last address of each name (`PeerNameResolver`). When a lookup fails, it uses that address and logs `cluster DNS FAILED`. A good lookup replaces the address, so a member that moves to a new address is found at the next good lookup.
- The member's own name resolves to the node address (`kardamom.cluster.nodeIp`) with no lookup.
- Without the last address, a short outage of the Consul agent fails an add in an election. A failed add of the ingress subscription at the end of an election leaves a leader that no client can reach, and the pipeline stalls until the member restarts.

### Decision version

Every member applies the same log. Two members on two rule versions can decide one entry differently, and the replicated state forks.

- Every member runs version 3 from its start. No setting selects the version, and no record changes it.
- A relayed frame of kinds 0, 3, 4 and 5 needs its record-type byte. A frame that ends at the record type drops as malformed.
- The member reports its version in the sealer info (egress kind 17): to each session when it opens, and to every open session at each new leadership term (a leader change can lose a frame of the old leader).
- A feature activation record (ingress kind 11) changes no state. The member prints `cluster FEATURE-ACTIVATION` with the outcome `IGNORED`. No sequencer sends the record.
- The snapshot is version 14: version 10, then the rules. See [Snapshot](#snapshot).
- Admin `/status` reports `decisionVersion` (3), and `decisionActivationOrigin` and `decisionActivationMs`: the newest feature activation that the snapshot tail holds, both 0 when it holds none.

`FEATURE_SEALER_V3` (feature id 2) turns on the nonce burn of a skipped transaction. The executor, the validator, the replay and the zk guest read the flag at each block, so a replay of a block from before the activation, or from a time that the feature was off, uses no nonce on a skip. The sealer does not read the flag.

- A new network sends `ETHLockbox.initiateUpgrade(2, 0)` once after its first deploy. Until that block, the sealer guard expects `n + 1` after a skip at nonce `n`, and the chain stays at `n`: the sender is stuck.
- Off is no longer supported. `just sealer-upgrade-check <env> 2 0` passes. The check refuses feature 2 with any activation time other than 0: off, or on at a later time. Off stops the nonce burn, so a sender is stuck after a skip at its chain nonce. A member of an older release still switches its rules on kind 11 from a sequencer of an older release, so in a mixed fleet an off transaction also forks the state.

The procedure of an upgrade transaction:

1. Deploy the release to every sealer member and to every executor, validator, ingress and sequencer. Roll the prover's zk guest and its verifying key to that release too: the guest links the same rules.
2. Run `just sealer-upgrade-check <env> <feature> <activation_ms>` with the feature id and the activation time of the transaction. It must pass before the Safe signs the upgrade transaction.
   - It refuses a feature that is not a decimal number, and feature 2 (`FEATURE_SEALER_V3`) with an activation time other than 0.
   - It reads admin `/status` of every configured member and refuses unless each one reports `snapshotReadsMax` of 14 or more.
   - It refuses while a configured member is down.
   - It refuses unless the executor, validator, ingress and sequencer jobs have the meta `nonce_burn = "true"` and every live allocation of them runs that job version.
   - It refuses unless the executor and validator jobs have the meta `chain_state_install = "true"` and every live allocation of them runs that job version. On a genesis without the `KardamomChainState` code, the first upgrade deposit installs the code. An older executor or validator runs it as a call to an empty account and forks the state at that deposit.
   - The check does not cover the prover. Roll the zk guest and its verifying key with the executor release before the upgrade transaction.
3. The Safe signs `ETHLockbox.initiateUpgrade(feature, activation_ms)`.

- A rollback of the sealer to the release before this one is safe while `FEATURE_SEALER_V3` stays on: that release reads snapshot version 14 with version 3 and the held activation, and decides every entry as this release does.
- A sealer that starts from a seed file, or at genesis, runs version 3.
- The trust: any client of the cluster can send kind 11, as it can send kind 0 and kind 7. The record changes no state. The cluster network must admit only the services of the chain.

| Version | Rule change |
|---|---|
| 1 | The first rules. |
| 2 | A void sets the expected nonce of the sender to the lower of the expected nonce and the voided nonce. |
| 3 | A relayed frame of kinds 0, 3, 4 and 5 needs its record-type byte. With `carryBytes`, a member orders full entries, relays them as canonical entries, and takes no void request. The contiguity guard does not change. |

- `carryBytes` is a setting that every member must match, and the release gate refuses a rolling change of it. The snapshot layout carries it: a member with `carryBytes` stops on a snapshot that is not version 12, and a member without it stops on version 12.

## Admin server

The admin server reports the member status over HTTP. It is off when `adminPort` is `0`. The deploy uses port `40205`.

| Request | Answer |
|---|---|
| `GET /status` | Always 200 with the status JSON. |
| `GET /ready` | 200 when the member is ready. 503 when it is not. The body is the status JSON. |
| `GET /metrics` | 200 with the gauge `kardamom_sealer_disk_free_bytes{member_id}` in the Prometheus text format. |
| `GET /retained` | With `carryBytes`: 200 with one `<name> <size>` line for each segment below the published head. `size` counts only the bytes below that head. 403 without the token. Else 404. |
| `GET /retained/<name>` | With `carryBytes`: 200 with the bytes of one segment below the published head. 403 without the token. 404 for a name that is not a served segment, or a segment that the purge deleted. |
| any other path | 404 |

- The server listens on `0.0.0.0`. Two members on one host cannot share the port, so the port is off by default.
- The server answers `/status`, `/ready` and `/metrics` on one thread. The paths under `/retained` run on two threads of their own, so a segment copy does not hold up `/ready`.
- The retained file holds the bytes of every transaction above the posted head, and the server listens on all addresses. So each request under `/retained` must carry `Authorization: Bearer <KARDAMOM_RETAINED_TOKEN>`. The job passes the same token to every member.
  - The token and the segment bytes travel in cleartext HTTP on the node network. The token keeps out a client that does not have it. It does not hide the bytes from a host that reads the network.
- `diskFreeBytes` is `null`, and the gauge is `NaN`, when the disk cannot be read. The member stays ready.
- The status JSON has `memberId`, `role`, `election`, `commitPosition`, `servicePosition`, `snapshotWrites`, `snapshotReadsMin`, `snapshotReadsMax`, `snapshotLatest`, `decisionVersion`, `decisionActivationOrigin`, `decisionActivationMs`, `diskFreeBytes` and `ready`.
  - `decisionVersion` is the version of the rules of the member: 3. `decisionActivationOrigin` and `decisionActivationMs` are the L1 origin and the time of the newest feature activation that the snapshot tail holds, both 0 when it holds none. See [Decision version](#decision-version).
  - `diskFreeBytes` is the free space of the disk of the cluster directory: the Raft log and the retained file. `/metrics` exports the same value.
  - `snapshotWrites`, `snapshotReadsMin` and `snapshotReadsMax` are the snapshot versions of this release, as `formats.toml` lists them.
  - `snapshotLatest` is the version of the newest snapshot this member restored or took. It is 0 before either.
  - A deploy reads `snapshotLatest` on every member before it rolls. It refuses a release whose `reads_max` is below that version, because that member could not restore its own snapshot after the roll.
- A member is ready when all of these are true:
  - the role is `LEADER` or `FOLLOWER`
  - the election state is `CLOSED`
  - `commitPosition - servicePosition` is at most `readyLagBytes`
- A member in an election, or a follower that still catches up, is not ready. A closing member reads `CLOSED` for the role and is never ready.
- If the status cannot be read, `/ready` and `/status` answer 503 with an `error` field.
- The Nomad job registers a Consul check of the path `/ready`. A rolling deploy waits for it before it stops the next member.

## Election stalls

Aeron 1.44 has three election waits with no bound. The join watchdog turns each into an exit. The relaunch then starts from the member's own state.

- `LEADER_READY` has no timeout. The leader waits until each member that voted, or after the leader heartbeat timeout a quorum, appends at its position in its term. A member that voted, but can never join the term, keeps the leader there.
- A follower whose service applied a log tail that the new leader does not hold fails each join at once. The join check finds the leader's join position below the member's log position, and each new election takes the position from the commit counter again. Only a relaunch replays the log again, up to the leader's log.
- `FOLLOWER_LOG_REPLICATION` checks its deadline only after the archive signals both the end of the replication and the stop of the recording. A replication that fails before its recording starts sends no stop, and the election waits for ever.

The rules:

- **Progress.** A new highest commit position of the member, a new highest position of its log recording in `FOLLOWER_LOG_REPLICATION`, a new highest commit position of a peer while the member waits in `LEADER_READY`, or a closed election. A leader reads the commit positions of its peers from their admin servers each 5 s. A replay of the member's own log from a lower position is no progress.
- **Leader stall.** The member waits in `LEADER_READY` with no progress for longer than the stall limit. It logs `cluster LEADER STALL` and exits with code 7.
- **Follower stall.** The member reached a follower state, so it found a leader, and it then makes no progress for the stall limit. It logs `cluster FOLLOWER STALL` and exits with code 8. The relaunch keeps the recording log.
- **Catch-up stall.** A member in the follower stall that tried a catch-up (`FOLLOWER_LOG_REPLICATION` or a `FOLLOWER_CATCHUP` state) first asks its peers for their admin status. Each round asks all peers at once and ends after 2 s, name lookups included.
  - Every peer answers with an open election: the leader that the member found is not live, so its log tail is not the cause, and no peer can seed the member. The member exits with code 8 and keeps its log.
  - A peer has a closed election: the member waits until `catchupStallS`, asks again, and then drops its recording log and exits with code 4. The relaunch seeds from that peer.
  - A peer does not answer, or the member has no admin port (`adminPort` 0, so the probe knows no peer): the answer is unknown. The member asks again at each sample. If no answer ends the wait, it drops its log at `catchupStallS` and seeds, as a member with no probe does.
- **INIT.** An election that stays in `INIT` gets the INIT wedge (exit code 3) at `joinWatchdogS`. The follower stall does not act on a sample in `INIT`.

The stall limit comes from the member's Aeron timeouts: two join attempts of a follower, each with two leader heartbeat timeouts, one archive message timeout and one driver timeout. That is 2 x (2 x 10 + 10 + 10) = 80 s at the defaults, and 2 x (2 x 10 + 30 + 30) = 160 s at the CI stall tolerance of 30 s. A healthy election in CI closes `LEADER_READY` within 10 s.

With the admin port set, no rule fires on a healthy cluster:

- A slow follower that replays or catches up moves its commit position. The follower counts it as progress, and a waiting leader reads it from the follower's admin server.
- A lone member with quorum lost, and a coordinated restart, wait in `CANVASS`. They reach no follower state until a leader exists.
- A snapshot runs with a closed election. A closed election is progress.

With `adminPort` 0 (the code default, and the e2e harness) the members serve no status:

- A leader reads no peer progress. So the leader stall can fire while a slow follower replays for longer than the stall limit. The relaunch costs one more election, and the follower keeps its replay.
- A follower that tried a catch-up never gets the answer "no closed election". It seeds at `catchupStallS`, as before the probe existed.

Not covered: `LEADER_LOG_REPLICATION` also has no timeout in Aeron 1.44. A leader waits there until a quorum has replicated its log. No rule of the join watchdog acts on that state.

## Log lines

The service prints on stdout. Each line starts with a UTC instant, then the text in the table.
The chaos suite and operators read these lines. The admin server also exports one gauge (see [Admin server](#admin-server)). There is no other observability surface.

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
| `cluster RECORDED-CURSOR` | The best recorded cursor moved up (`executorId`, `through`, `best`, `halted`, `totalAdvances`). It prints at power-of-two counts of the moves, because the best cursor moves at the cadence of the executors. `halted` is the record-lag flag. |
| `cluster WINDOW-FULL` | The dedup window is at capacity. The line shows the size and the capacity. |
| `cluster REMOTE-ORIGIN-REJECT` | The sealer refused a remote-origin record. The line shows the reason code. |
| `cluster VOID-VOTE` | A void vote changed the count (`voter`, `index`, `result`, `votes` as `n/total`). A repeated vote prints nothing. |
| `cluster DROPPED malformed` | The service dropped a malformed frame. The line names the frame type. A non-zero count means that the Java and Rust layouts differ. |
| `cluster REPLAY` | A replay request. A served replay ends with `served=`, `queued=` and `dropped=`. A refusal ends with `AHEAD head=(index,block)`, `SKEWED` or `UNAVAILABLE`. `AHEAD` means that the cursor is past the head. `SKEWED` means that `from_index` is outside the block that `from_block` names. |
| `cluster EGRESS-CLOSE` | The service closed a client session because it could not take its frames. The line has the reason. See [Egress back-pressure](#egress-back-pressure). |
| `sealer snapshot START` | The service starts its snapshot (`block`). The archive recording of the snapshot is open. |
| `sealer snapshot TAKEN` | A snapshot was taken (`block`, `canonicalCount`, and `ms`, the wall time of the snapshot in milliseconds). A member that replays old log entries also prints it. |
| `sealer snapshot RESTORED` | The member started from a snapshot. The line shows `block`, `canonicalCount`, `retained` and `postedHead`. |
| `sealer state FRESH at genesis` | The member started with no snapshot and no seed. |
| `sealer state SEEDED` | The member started from a seed (`block`, `endTx`, `senders`, `stateRoot`, `digest`). |
| `sealer seed CONFIRMED` | The first seed record is in the log (`block`, `canonicalCount`). |
| `sealer seed SNAPSHOT` | The seeded leader asked for a snapshot (`requested=true` or `false`). A refused request leaves the periodic snapshot to take one. |
| `sealer SEED-EPOCH FATAL` | The log holds a seed record, but this member started at genesis or from another seed. The member stops. |
| `sealer RETAINED-FILE FATAL` | An I/O step of the retained file failed: the preallocation, a force, a rename, a delete, or a restore whose walk fails. Or the snapshot holds the other retention layout. The line names the step. The member stops. |
| `sealer SEED-EPOCH offer FAILED` | The offer of the seed record failed. The next leadership term offers it again. |
| `cluster START mode=` | The start mode of the member (`RESUME`, `GENESIS` or `SEED_FROM_PEER`). |
| `cluster SEED start` / `cluster SEED from-peer` | A blank member starts to copy a snapshot from a peer, and then ends the copy (`snapshotPosition`, `round`). |
| `cluster SEED waiting-for-peer` | A round of the peer seed ended with no result (`outcome`, `backupState`, `retryInMs`). The member waits and does not start at genesis. |
| `cluster SEED backup error` | The ClusterBackup of the peer seed reported an error. |
| `cluster SEED retained-copy` / `cluster SEED retained-copy FAILED` | The peer seed copied the retained file from a peer (`peer`, `segments`), or the copy from that peer failed, with the reason. |
| `cluster SEED retained-copy MISMATCH` / `cluster SEED retained-copy GAVE-UP` | No peer served a copy that matches the backed-up snapshot in this round (`failures`), or the seed stopped the start after 5 such rounds. |
| `cluster admin retained request FAILED` | A request under `/retained` failed while the server answered it. |
| `cluster SNAPSHOT requested` / `cluster SNAPSHOT attempt failed` | On a due tick (`block`), the leader asked for the periodic snapshot, or its control toggle held another request. The next due tick tries again. |
| `cluster SNAPSHOT triggered` | The snapshot of the leader is complete: the first timer after it fired. `stallMs` is the time from the boundary tick before the snapshot to that timer. No status frame left the leader in that time. |
| `cluster LAUNCH RETRY` | The launch found a stale mark file from a killed process. The node waits and retries, up to 6 attempts. |
| `cluster LAUNCH REPAIR` | The archive had a torn last fragment. The node truncates it and launches again. |
| `cluster JOIN WEDGE` | The election stayed in `INIT` for longer than `joinWatchdogS`. The process halts with exit code 3. |
| `cluster LEADER WEDGE` | The completion of the member's leader election failed, so the leader has no ingress subscription: its control toggle is active, but its election state is not `CLOSED`, for longer than the archive message timeout plus the driver timeout. The process halts with exit code 6, and the members elect again. |
| `cluster ELECTION` | The election state changed between two samples of the join watchdog, one second apart (`state`, `commitPosition`, `candidateTermId`, `logLeadershipTermId`). A short state can pass with no line. `candidateTermId` is the highest term that the member voted in or stood for (from the node state file). `logLeadershipTermId` is the term of the last term entry of the recording log: a leader writes it when it starts its term, and a follower when it joins the leader's log. It reads `unknown` when the member has no term entry, or when the read meets an entry that the consensus module writes at that moment. |
| `cluster REPLICATION` | The member is in `FOLLOWER_LOG_REPLICATION`, and the position of its active log recording changed (`recordingId`, `position`, `commitPosition`). `(no active recording)` means that the replication ended, or never started, with no recording. |
| `cluster LEADER STALL` | The member waited in `LEADER_READY` with no progress on itself or its peers for the stall limit. The process halts with exit code 7, and the members elect again. See [Election stalls](#election-stalls). |
| `cluster FOLLOWER STALL` | The member found a leader and then made no progress for the stall limit. The process halts with exit code 8, and the relaunch keeps the recording log. After a catch-up, the member exits only when every peer answers with an open election. A line with `deferred` shows that a peer has a closed election: the `CATCHUP STALL` rule then decides at `catchupStallS`. |
| `cluster CATCHUP STALL` | The member tried a catch-up and made no progress for `catchupStallS`, and not every peer answered with an open election. The member drops its recording log, and the process halts with exit code 4. The relaunch seeds from a peer. |
| `cluster ERROR LOG` | Before each exit of the join watchdog, the member prints the error logs of its consensus module, its media driver and its service container, one line each. |
| `cluster DNS FAILED` / `cluster DNS RESOLVED` | A lookup of a member name failed, and the media driver uses the last address of the name. The second line shows the first good lookup after the outage. A name with no last address stays unresolved, and the Aeron add fails. See [Member names](#member-names). |
| `cluster COMPONENT CLOSED` | The consensus module or the service container closed with no stop request, for example after an error in its start. The error log in the cluster directory holds the cause. The process halts with exit code 5. |
| `cluster TERMINATION` | The consensus module or the service container asked for a shutdown. |
| `cluster node up`, `cluster admin endpoint`, `cluster snapshot scheduler`, `cluster join watchdog`, `cluster da-lag budget`, `cluster record-lag budget`, `cluster ordering window`, `cluster remote-origin allowlist`, `cluster seed`, `cluster carry bytes`, `cluster void voters` | Start-up lines. They show the settings that the member uses. |

- The lines `CONTIGUITY-REJECT`, `PAST-DEADLINE`, `DA-LAG-REJECT`, `RECORD-LAG-REJECT`, `WINDOW-FULL`, `REMOTE-ORIGIN-REJECT` and `DROPPED` print when their count is a power of two (1, 2, 4, 8, and so on).
- There is no Prometheus counter for these events.

## Snapshot

- The member reads snapshot versions 1 to 14. It writes version 14, or version 12 with `carryBytes`.
  - Version 14 is version 10, then the decision rules after the seed digest: `[decision_version:u8][sealed_origin:u64][held:u8]`, then `[origin:u64][time_ms:u64]` for each held activation, oldest first (at most three). The member writes version 3 and `sealed_origin`, the L1 origin of the last sealed block. It keeps the held activations of the snapshot that it restored, and adds none. A member of the release before this one restores the same rules, and runs version 3 while `FEATURE_SEALER_V3` stays on. A tail of version 2 restores as version 3. See [Decision version](#decision-version).
  - Version 13 is version 10, then `[decision_version:u8][activation_origin:u64][activation_ms:u64]`: one held activation.
  - Version 12 is the state section of version 10 without the void ledger, then the retention section of the retained file. A member with `carryBytes` takes no void request and no recorded cursor, so it holds neither.
  - This release does not write version 10. A member of the release before it restores version 10 with decision version 2.
  - A version-14 snapshot, as a version-10 one, holds no recorded cursor. A member that restores one has no cursor until the next cursor record. With the record-lag budget at `0` this changes no decision.
  - The release that turns on the cursor publisher and the record-lag guard writes version 11. Before that release, a `recordLagBudget` above `0` stops the start.
  - A snapshot before version 9 restores a posted head of `0`. A snapshot before version 10 restores a state that started at genesis.
  - A snapshot before version 11 restores no recorded cursor. The record-lag guard then refuses nothing until the first cursor.
- The state section holds:
  - the dedup window with the deadline of each id
  - the per-sender nonces
  - the remote-origin state
  - the void ledger, in versions 6 to 11, 13 and 14
  - the ordering window size
  - the posted head, after the ordering window size
  - the seed status (1 byte) and the seed digest (32 bytes), after the posted head
  - from version 11 only, the recorded cursors, after the seed digest: `[count:u8]`, then `[executor_id:u8][recorded_through:u64]` for each executor, in executor-id order
    - A restore drops the cursor of an executor that is not a configured voter.
  - from versions 13 and 14 only, the decision rules, after the seed digest
- The retention section follows the state section. Its layout depends on the version. Both layouts are big-endian.
  - Versions 8 to 11, 13 and 14, on the heap: `[count:i32]`, then `[boundary:u8][key:u64][len:u32][frame]` for each retained frame.
  - Version 12, the retained file: `[floor_index:u64][floor_block:u64][count:u64][front:u64][head:u64][n:i32]`, then `[block:u64][end_tx_idx:u64][end_pos:u64]` for each retained boundary, then `[len:i32]`, the length of the section before it. The section is the end of the snapshot, so a peer seed finds it with no parse of the state. See [Retained file](#retained-file).
  - A member with `carryBytes` stops on a snapshot that is not version 12. A member without it stops on version 12.
- A snapshot that was taken with another `orderingWindow` does not load. The member halts.
- The Raft snapshot interval is `snapshotIntervalS`. The service asks for the snapshot on a boundary tick whose block number is a multiple of the interval in blocks. Only the leader's control toggle takes the request. The action is a log entry, so all members snapshot at the same position.
- A snapshot stops the cluster. The consensus module appends no timer and no ingress until the service and the module record their snapshots in the archive. With `fileSyncLevel` above `0`, each recording is a chain of disk syncs (the catalog, the directory, each write, the recording log). On a loaded disk this took 11 s.
  - So the status of the due tick carries the snapshot notice (`snapshot_follows`). The notice leaves before the leader sets its toggle.
  - After the notice, the ingress, the sequencer and the session watchdog of each client wait 60 s (`SNAPSHOT_SILENCE`), not 10 s, for the next frame. A stall past 60 s is still a lost quorum.
  - A snapshot that an operator asks for with `ClusterTool` carries no notice.

### Manual snapshot

A manual snapshot (`ClusterTool <cluster dir> snapshot` on the leader) sends no snapshot notice. The clients keep the 10 s budget. On a loaded disk the stall can pass 10 s, and then:

- each ingress halts with `sealer_no_quorum` and pauses submits,
- each sequencer pauses,
- each session watchdog forces a reconnect.

The halt clears by itself at the next status frame. To avoid it, do not ask for a manual snapshot while the disk is loaded. Let the periodic snapshot take it, or set `snapshotIntervalS` lower for a while. Before a manual snapshot, tell the operators of the clients that a short `sealer_no_quorum` halt can come.

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

# Executor-archived transaction data

- Status: design and implementation plan. Not implemented.
- Scope: the executor stream, the join path of the executors, the transaction source of the
  validator and the batcher, the void voter set, the recorded-head guard, the ack policies, and
  the retention of the executor archives.
- Every claim about the current code cites `path:line` on `main`.

## 1. Summary

### 1.1 The change

- Each executor publishes the transactions that it joins, in canonical order. This is the
  **executor stream** (topic `exec_txs`).
- The Aeron archive on the executor node records the executor stream of its own executor.
- The ingress keeps its `tx_data` archive recording. It is a resilience source for the
  executors only. Nothing waits for it.
- The executors are the only consumers of `tx_data` and of the ingress `tx_data` archives.
- The validator, the batcher, and every other consumer of transaction bytes read the executor
  streams and the executor archives only. They do not join `tx_data`. They do not vote.
- The join order of an executor becomes:
  1. live `tx_data`;
  2. the ingress `tx_data` archives;
  3. the executor archives of the other executors, by canonical index;
  4. a vote to void the entry.
- The voter set of the void becomes the executors only.
- The sealer gets the recorded cursor of each executor. A new guard refuses user records while
  the sealed index is too far past the best recorded cursor.

### 1.2 Invariants that the design keeps

| Id | Invariant |
|---|---|
| X1 | No disk write is on the path from the ingress to the executors. The sequencer and the sealer never wait for an archive. |
| X2 | At canonical index `i`, every executor executes the same bytes, or every executor drops the entry. An executor drops `i` only when it reads `Void(i)` in the canonical order. |
| X3 | The sealer appends `Void(i)` only when every executor voted for `(i, tx_hash)`. An executor that joined `i` never votes for `i`. |
| X4 | An executor votes for `i` only when every source gave a definite "absent" answer: every ingress archive answered `RangeAbsent`, and every other executor answered `not_held`. An executor that does not answer is not "absent". |
| X5 | A consumer outside the executors executes or posts the bytes at `i` only when `keccak256(raw_tx) == TxRef(i).tx_hash`, with the `TxRef` from the canonical order. It drops `i` only when it reads `Void(i)` in the canonical order. |
| X6 | An executor archive keeps every record that L1 does not hold yet, and every record of the void window. |
| X7 | With the guard on, the sealed index is never more than the budget past the best recorded cursor for user records. Deposits, boundaries, votes and void records still enter. |

### 1.3 What the design does not guarantee

- It does not make a record durable before the sealer orders it. The sealer still orders a
  reference before any archive holds the bytes. The void stays the answer to a lost entry.
- The recorded cursor is as durable as the archive sync level of the executor node. At level 0
  a power loss can drop recorded bytes.
- The executors trust the `sender` field of a live envelope, as today. A forged sender is the
  validator's finding (section 5.4).
- The best recorded cursor proves one copy. It does not prove two copies. The ingress archives
  are the second copy for the executors.

## 2. Data flow

### 2.1 Current data flow

| Step | Behavior today | Citation |
|---|---|---|
| Publish | Each ingress publishes 8 `tx_data` lanes as dynamic MDC publications. | `docs/aeron-discovery.md:211-213` |
| Record | Each ingress runs one recorder thread. It records every `tx_data` publisher in the catalog, its own and the other ingress's. | `crates/ingress/src/bin/kardamom-ingress/recorders.rs:90-99`, `docs/aeron-discovery.md:159-166` |
| Archive record | Ingress nodes advertise `archive_topics = tx_data`. Executor nodes advertise no topic. | `deploy/cluster/ansible/roles/nomad/templates/nomad.hcl.j2:86`, `deploy/cluster/nomad/aeron.system.nomad.hcl:225-241` |
| Readiness | An ingress serves only when each of its own lanes has a live recording. | `docs/aeron-discovery.md:165-166`, `crates/ingress/src/bin/kardamom-ingress/recorders.rs:172` |
| Order | The sequencers read `tx_data` and offer the `TxRef`. The sealer orders it before an archive holds the bytes. | `docs/failure-modes.md:378`, `cluster/sealer-service/core/src/main/java/io/kardamom/sealer/VoidLedger.java:14-15` |
| Join | The executor, the validator and the batcher open 8 live `tx_data` subscriptions and join by `(shard, session, position)`. | `crates/engine/src/bin_support.rs:193-219`, `crates/engine/src/reader/join.rs:27-50`, `crates/executor/src/bin/kardamom-executor/main.rs:380`, `crates/engine/src/bin_support.rs:411-419` (validator), `crates/batcher/src/live/run.rs:303` |
| Join miss | After 10 s the reader refetches the range from the ingress archives. It repeats until the join budget ends: 30 s on a resume, 60 s on a fresh start. | `crates/engine/src/reader/join.rs:125`, `crates/engine/src/bin_support.rs:131-137`, `crates/engine/src/reader/join.rs:220-251` |
| Unjoinable | When every ingress archive answers `RangeAbsent`, a voter votes and waits up to 120 s for `Void(i)`. A consumer with no voter id stops. | `crates/engine/src/reader/join.rs:240-251`, `crates/engine/src/reader/threads.rs:314-347`, `crates/engine/src/reader/join.rs:129` |
| Voters | The voters are the executors, the validator and the batcher: ids `0..executor_count+1`. | `deploy/cluster/nomad/cluster.nomad.hcl:191`, `deploy/cluster/nomad/validator.nomad.hcl:248`, `deploy/cluster/nomad/batcher.nomad.hcl:256`, `docs/failure-modes.md:261-268` |
| Void decide | The sealer decides when the vote mask equals the voter mask. | `cluster/sealer-service/core/src/main/java/io/kardamom/sealer/VoidLedger.java:167-187` |
| Void effect | The void removes the hash from the dedup window and sets the expected nonce of the sender back. | `cluster/sealer-service/core/src/main/java/io/kardamom/sealer/CanonicalSealerState.java:838-845` |
| Batcher rebuild | A range past the sealer retention is rebuilt from `kardamom_getBlockRefs` and the bytes on the ingress archives. | `crates/batcher/src/live/rebuild.rs:1-4`, `crates/batcher/src/live/rebuild.rs:58-66`, `crates/batcher/src/live/refs_store.rs:1-14`, `crates/state/src/schema.rs:11` |
| Sync level | Neither the aeron job nor the image entrypoint sets the archive sync level. The Aeron default 0 applies. The code comments say level 1. | `crates/log/docker/aeron/entrypoint.sh:46-62`, `deploy/cluster/nomad/aeron.system.nomad.hcl:211`, `crates/log/src/recorder.rs:14-19` |

```text
                 tx_data (8 lanes x 2 ingress, MDC)
 ingress-0/1 ─────────────────────────────────────────────┐
   │  │                                                    │
   │  └─ recorder: every tx_data publisher ─> ingress archive (x2)
   │                                                       │
   ▼                                                       ▼
 sequencer ── TxRef ──> sealer (Raft) ── canonical order ──┬──> executor x3 ──┐
                                                           ├──> validator     ├─ join tx_data,
                                                           └──> batcher  ─────┘  refetch ingress
                                                                                 archives, vote
```

### 2.2 New data flow

| Step | New behavior |
|---|---|
| Publish | Unchanged. |
| Ingress record | Unchanged. The ingress records every `tx_data` publisher. No producer and no consumer waits for it. |
| Order | Unchanged. |
| Executor join | Live `tx_data`, then the ingress archives, then the peer executor archives, then the vote (section 4). |
| Executor publish | The reader thread of each executor sends each joined record to its publisher thread. The publisher thread writes the executor stream. |
| Executor record | The archive on the executor node records the executor stream of its own executor. |
| Recorded cursor | Each executor sends its recorded cursor to the sealer. The sealer replicates it. |
| Validator, batcher | They read the canonical order from the sealer, and the bytes from the executor streams. A miss refetches from an executor archive by canonical index. They never vote. |
| Voters | The executors only: ids `0..executor_count`. |
| Batcher rebuild | The bytes come from the executor archives, through an executor locator. |

```text
 ingress-0/1 ── tx_data ──┬──> sequencer ── TxRef ──> sealer ── canonical order ──┬──> executor x3
                          │                                                       ├──> validator
                          ├──> executor x3 (live join)                            └──> batcher
                          └──> ingress archive x2 (executors only, nobody waits)

 executor-k ── exec_txs (IPC) ──> archive on executor-k node    (local, flow-controlled)
            └─ exec_txs (MDC) ──> validator, batcher            (live, dedupe by index)
            └─ RECORDED_CURSOR ──> sealer                       (replicated, guard)
            └─ kardamom_getExecLocator ──> peers, validator, batcher (where index i is)

 executor-j (miss on i) ── ingress archives ──> peer executor archives ──> vote (executors only)
```

## 3. Wire, stream and format changes

### 3.1 The executor stream

| Item | Value |
|---|---|
| Topic | `exec_txs`, a new `Topic` variant in `crates/log/src/discovery/record.rs:23-33`. |
| Stream id | `1005`. It is free. The used ids are 1002 to 1004, 1010, 1015 to 1019, the `tx_data` base 2000, and the cluster ingress 101 (`crates/log/src/config/mod.rs:610-656`, `deploy/cluster/nomad/cluster.nomad.hcl:338`). |
| Term length | The driver default, 4 MiB (`deploy/cluster/nomad/aeron.system.nomad.hcl:199`, `crates/log/src/discovery/record.rs:58-70`). |
| Publishers | Every executor. |
| Consumers | The validator, the batcher, and later consumers of transaction bytes. They attach one destination for each executor, as for `tx_receipts`. |
| Dedup | A consumer keys records by canonical index. The first record that passes the check of section 5.2 wins. The others drop. |

Each executor opens two publications. One publisher thread writes both, in the same order:

| Publication | Channel | Purpose |
|---|---|---|
| Recorded | `aeron:ipc?alias=exec-txs`, stream 1005, an exclusive publication | The local archive records it. An IPC publication cannot run ahead of its slowest subscriber, so the recording never loses a frame. An exclusive publication has its own session, also when executors share one media driver. |
| Live | dynamic MDC, topic `exec_txs`, stream 1005 | The consumers subscribe. It is lossy, like `tx_data`. A consumer repairs a gap from an archive. |

- One spied UDP publication could replace the pair. Phase 2 checks on Aeron 1.45 whether a
  local spy recording holds back a network publication. If it does, the pair becomes one
  publication. The design does not depend on it.
- A slow archive back-pressures the recorded publication. The publisher thread then blocks,
  the channel from the reader fills, and the reader thread blocks. This executor stalls. The
  other executors do not. An executor never executes a record that its archive could not take.

Record layout (rkyv, `crates/types/src/exec_record.rs`, new):

```text
ExecTxRecord {
    index:    u64,         // the canonical index of the TxRef (BPosition::as_index)
    tx_ref:   TxRef,       // the reference as the canonical order carried it
    envelope: TxEnvelope,  // the bytes this executor joined (crates/types/src/envelope.rs:12-35)
}
```

- The record carries only `TxRef` entries. Epochs, deposits, remote epochs, boundaries and
  voids are in the canonical order already (`crates/engine/src/reader/threads.rs:435-466`).
- The `tx_ref` field lets an executor insert a fetched record into its own `tx_data` join
  buffer under `TxDataKey::from_ref` (`crates/engine/src/reader/join.rs:43-49`).
- The size is about 130 B plus `raw_tx`, plus the 32 B Aeron frame header. A 110 B transfer
  takes about 270 B.

### 3.2 The locator log and the locator query

A consumer must find canonical index `i` in a recording. Aeron replays by position, not by
content. Each executor therefore keeps a locator log.

- File: `<state_dir>/exec_stream/locators.log`. The publisher thread owns it. No other thread
  writes it.
- Entry: `[index:u64][session_id:i32][position:i64][crc32:u32]`, 24 bytes.
- The publisher thread appends one entry for the first record of each session, then one entry
  every 1024 records. It writes the entry after the offer, so an entry never points past its
  record.
- An entry names the end position of the record before it. The first entry of a session names
  the first recording position that the recorder reported, before any offer. That is the start
  of the record, or a lower bound.
- A torn or missing tail entry costs nothing. The lookup takes the previous entry, which is a
  lower bound, and the replay is longer.
- A lookup of `i` takes the newest entry with `index <= i` and replays that session from that
  position. The replay filters `index == i`.
- The locator log is not in the state DB and not in a checkpoint. The state DB schema does not
  change (`crates/state/src/meta.rs:64`).

The query method on the executor query endpoint (`--nonce-query-addr`,
`crates/state/src/nonce_query.rs:250-256`):

- `kardamom_getExecLocator`, params `[index]` or `[index, tx_hash]`.
- One owner thread holds the answers state. The query server asks it over a channel with a
  oneshot reply. No lock is shared.

| Answer | Condition |
|---|---|
| `{"status":"located","archive_id","session_id","position"}` | This executor joined `i` (its state has the receipt at `i`, or its current run joined `i`), and a retained recording covers the locator. |
| `{"status":"not_held"}` | This executor reached `i` and did not join it: it parks at `i`, or its state shows `i` vacant. |
| `{"status":"not_reached"}` | This executor has not reached `i`. |
| `{"status":"lost"}` | This executor's state includes `i`, and no retained recording covers it (archive wipe, a restore from a peer checkpoint, or a prune). |

- "Reached" follows the prior attempt: a reader reaches `i` when it joins `i`, parks at `i`,
  or its first record is above `i`.
- `lost` is never "absent". The executor that answers `lost` executed `i`, or adopted a state
  that includes `i`.
- `kardamom_getBlockRefs` (`crates/state/src/nonce_query.rs:252`) gains an optional
  `exec_locator` per block, from the locator log of that executor. The validator answers
  without it.

### 3.3 Discovery and `archive_topics`

- Every executor registers one `kardamom-mdc-publisher` record for its live publication, with
  `topic = exec_txs` and `stream_id = 1005` (`docs/aeron-discovery.md:99-111`).
- Executor nodes advertise `archive_topics = exec_txs`. The template changes in
  `deploy/cluster/ansible/roles/nomad/templates/nomad.hcl.j2:86`. Ingress nodes keep
  `tx_data`. The da-watcher node keeps `tx_deposits`.
- The archive record carries `archive_id = ${node.unique.name}`
  (`deploy/cluster/nomad/aeron.system.nomad.hcl:238-239`). A locator answer names this id. The
  asker resolves the control endpoint from the archive records.
- Order matters. An archive record with an unknown topic fails to parse
  (`crates/log/src/discovery/record.rs:279-296`). The refetch client then skips the whole
  record (`crates/log/src/refetch.rs:95-102`). The runtime that knows `exec_txs` must deploy
  before the node meta changes. Executor nodes advertise no topic today, so an early skip
  loses nothing.
- `docs/aeron-discovery.md` gets the topic, the publisher row, the archive row and the new
  executor publications in the job table (`docs/aeron-discovery.md:28-38`, `211-223`).

### 3.4 The sealer wire

Ingress kinds 0 to 8 and egress kinds 1 to 12 are in use
(`cluster/sealer-service/service/src/main/java/io/kardamom/sealer/cluster/SealerWire.java:65-283`).

| Kind | Direction | Layout | Meaning |
|---|---|---|---|
| `KIND_RECORDED_CURSOR = 9` | client to sealer | `[kind:9][executor_id:u8][recorded_through:u64 LE]`, 10 bytes | Every `TxRef` index at or below `recorded_through` was joined and recorded by this executor, or voided. |
| `EGRESS_KIND_RECORD_LAG_REJECT = 13` | sealer to the offering session | `[kind:13][sender:20][nonce:u64][sealed_index:u64][recorded_index:u64][budget:u64]` | The guard refused a user record. |
| `EGRESS_KIND_STATUS = 9`, tail | sealer to every session | the current 50 bytes, then `[best_recorded:u64][record_lag_budget:u64][record_lag_halted:u8]` | The status frame grows by 17 bytes, to 67. `best_recorded` is `u64::MAX` while no executor has sent a cursor. |

Compatibility of each change:

- An old sealer member treats an unknown kind as an ingress record only when the frame has
  `MIN_INGRESS_LEN` bytes or more. A shorter frame drops as malformed
  (`cluster/sealer-service/service/src/main/java/io/kardamom/sealer/cluster/SealerClusteredService.java:502-509`).
  The 10-byte cursor frame drops. The frame must stay below `MIN_INGRESS_LEN`.
- An old Rust reader rejects an unknown egress kind with `BadEgressKind`
  (`crates/cluster-adapter/src/wire/egress.rs:122`). The engine drops such a frame with a
  warning (`crates/engine/src/reader/cluster/mod.rs:408-414`). An old sequencer would not see
  a refusal. So the sealer sends kind 13 only while the guard is on, and the guard turns on
  only after every sequencer understands kind 13.
- An old Rust reader reads the status frame at fixed offsets and ignores a longer tail
  (`crates/cluster-adapter/src/wire/egress.rs:180-190`). A new reader treats a 50-byte frame
  as "no recorded cursor".
- `TxErrorReason::RecordLag { sealed_index, recorded_index, budget }` is a new rkyv variant
  after `DaLag` (`crates/types/src/tx_error.rs:79-88`). An old decoder fails on it. The
  ingress and the notifier deploy before the guard turns on.
- `HaltCause::RecordLag` (id `record_lag`) is a new variant after `SealerNoQuorum`
  (`crates/types/src/service.rs:21-54`). The ingress raises it for `service="sealer"`, so the
  ingress deploys with it.
- The Rust constants go to `crates/cluster-adapter/src/wire/mod.rs:140-236`, with tests in
  `crates/cluster-adapter/src/wire/tests.rs`.

### 3.5 Snapshot and state formats

| Format | Change |
|---|---|
| Sealer snapshot | Version 11 (`CanonicalSealerState.java:130`). The tail after the seed digest (`CanonicalSealerState.java:1297-1356`) gets `[count:u8]` and `count x [executor_id:u8][recorded_through:u64]`. A v10 snapshot restores an empty map. |
| Sealer settings | `kardamom.cluster.recordLagBudget` (env `KARDAMOM_RECORD_LAG_BUDGET`). Code default 0, the guard off. Every member must match. Not in the snapshot, like the DA-lag budget. |
| Executor state DB | No change. The locator log is a side file. |
| Executor checkpoint | No change. A restored executor starts a new locator log at its restore point. It answers `lost` below that point. |
| Validator state DB, batcher spool | No change. |
| Ingress `tx_data` archives | No change. |

### 3.6 Durability of the executor archive

- The open change that adds `archive_file_sync_level` to the aeron job (default 1) applies to
  every node, because the aeron job is a system job
  (`deploy/cluster/nomad/aeron.system.nomad.hcl:1-5`). Executor nodes get level 1 with it.
- At level 1 the archive runs `fdatasync` on each write batch before the recording position
  passes it. The recorded cursor is then power-safe on one node (`crates/log/README.md:65-73`).
- At level 0 the recorded cursor proves only the page cache. The guard and `on-recorded` are
  then crash-safe, not power-safe.
- The executor node also writes the MDBX state DB. Put the archive directory on the same NVMe
  with power-loss protection, or on its own volume. Phase 2 measures the cost.
- The executor process does not run without the recording. When the recorder reads no recording
  position for the loss wait, the publisher fails, the reader stops, and the process exits. The
  restart waits for a recording of the new session.
- The loss wait is the driver timeout of the archive client (`AERON_DRIVER_TIMEOUT`, the Aeron
  stall tolerance of the deploy: 10 s by default, 30 s in CI) plus 5 s, and at least 10 s. It is
  the rule of the Aeron client start budget (`DriverBudget` in `crates/log`). A stall that
  every Aeron party survives does not end the recorder.
- The executor process does not start without the recording. Its readiness waits for a live
  recording of its recorded publication, as the ingress does for its lanes
  (`crates/ingress/src/bin/kardamom-ingress/recorders.rs:172`). It reuses
  `record_stream_until_stopped` (`crates/log/src/recorder.rs:221`).
- The executor node becomes a recorder node. The driver memory note covers recorder nodes
  (`deploy/cluster/nomad/aeron.system.nomad.hcl:243-252`). One IPC publication adds about
  12 MiB of term buffers. The 768 MB reservation holds it.

## 4. The executor join algorithm

### 4.1 Steps for one `TxRef` at canonical index `i`

1. **Live.** Wait for the envelope in the join buffer, as today
   (`crates/engine/src/reader/join.rs:220-236`).
2. **Ingress archives.** After `join_refetch_after` (10 s), refetch from the ingress
   `tx_data` archives, and repeat until `join_timeout` ends, as today
   (`crates/engine/src/reader/join.rs:256-331`). The outcome is `Joined`, `Unjoinable` (every
   ingress archive answered `RangeAbsent`) or `TimedOut` (an ingress archive gave no definite
   answer) (`crates/engine/src/reader/join.rs:134-144`).
3. **Peer executor archives.** On `Unjoinable` and on `TimedOut`, enter the park:
   - Ask every peer `kardamom_getExecLocator(i, tx_hash)`.
   - On `located`, replay that peer's archive from the locator, filter `index == i`, and check
     the record (section 4.3). A good record ends the park: insert it, send it to exec, publish
     it on this executor's stream. No vote.
   - On `not_reached`, a failed replay, a failed check, a transport error or no answer, ask
     that peer again after 1 s.
   - On `not_held`, the answer is final for this park.
   - On `lost`, no vote. Section 4.4 gives the repair.
4. **Vote.** Vote only when both hold:
   - the join outcome was `Unjoinable`;
   - every peer answered `not_held`.
   Then send `KIND_VOID_REQUEST` and repeat it every 5 s
   (`crates/engine/src/reader/void.rs:39`, `crates/engine/src/reader/void.rs:135-149`).
5. **Wait for the void.** Read ahead in the canonical order, as today
   (`crates/engine/src/reader/void.rs:96-131`). `Void(i)` drops the entry. A peer that serves
   the record before this executor voted ends the park with the record.
6. **Give up.** After `void_wait` (120 s) or `MAX_READ_AHEAD` messages, stop with
   `JoinTimeout` and restart (`crates/engine/src/reader/threads.rs:333-336`). The sealer keeps
   the vote.

On a resume, before step 1, the executor preloads its own archive tail:

- It reads the locator of its resume index from its locator log.
- It replays that old session from the locator to the stop position of the recording.
- It inserts each record above the resume index into the join buffer.
- This covers records that the earlier run joined and published but did not commit.

### 4.2 Timeouts and budgets

| Name | Value | Source |
|---|---|---|
| `join_refetch_after` | 10 s | `crates/engine/src/reader/join.rs:125` |
| `join_timeout` | 30 s on resume, 60 s fresh | `crates/engine/src/bin_support.rs:131-137` |
| locator query | 2 s connect and read | new, the prior attempt's bound |
| peer replay | the refetch drain: 500 ms idle, 8 s cap | `crates/log/src/refetch.rs:63-64` |
| peer re-ask | 1 s | new, the prior attempt's value |
| revote | 5 s | `crates/engine/src/reader/void.rs:39` |
| `void_wait` | 120 s | `crates/engine/src/reader/join.rs:129` |
| read-ahead | 65 536 messages | `crates/engine/src/reader/void.rs:32` |
| void window | 65 536 indices | `docs/failure-modes.md:253` |

- The worst path to a vote is the join budget plus one peer round: about 60 s + 3 x 10 s.
- The void window then bounds the rate. Today a vote fails above about 1000 records per second
  with a 60 s budget (`docs/failure-modes.md:258-259`).
- The guard of section 6 removes that limit. A parked fleet records nothing past `i - 1`, so
  the sealer stops user records after the budget. With the budget below the void window, the
  entry cannot leave the window.

### 4.3 The check of a fetched record

An executor executes a record from a peer archive, or from its own archive, only when all of
these hold:

- the bytes decode as `ExecTxRecord`;
- `record.index == i` and `record.tx_ref == TxRef(i)` from the canonical order;
- `record.envelope.tx_hash == TxRef(i).tx_hash`;
- `keccak256(record.envelope.raw_tx) == TxRef(i).tx_hash`;
- `verify_record_identity(&record.envelope)` passes: the signature recovers the sender
  (`crates/exec-core/src/stateless.rs:253-272`).

A failed check counts as no answer from that peer. It logs a warning and counts
`outcome="mismatch"`. It never counts as `not_held`.

### 4.4 Determinism: no two executors diverge

1. **Bytes.** An executor executes at `i` only bytes whose keccak equals `TxRef(i).tx_hash`.
   The hash comes from the canonical order, which every executor reads in the same order. So
   `raw_tx` is the same on every executor.
2. **Sender.** On the live and ingress-archive paths, the executor trusts the proxy's
   `sender`, as today (`crates/types/src/envelope.rs:16-19`,
   `crates/executor/src/bin/kardamom-executor/main.rs:411-416`). Those bytes are the same
   bytes on every executor, because they come from one `tx_data` frame. On a fetched path, the
   identity check passes only when the sender is the signer. An honest proxy stamps the
   signer, so the fetched sender equals the live sender.
3. **A forged sender.** If the proxy forged a sender, the live executors execute the forged
   envelope, and the fetched copy fails the identity check. The asker then gets no usable
   answer, so it does not vote. The holders never vote. No void occurs. The asker stalls, and
   the validator halts on the forged identity (`crates/validator/src/bin/kardamom-validator/wiring/run.rs:170`).
   This is a liveness cost in a fault that the trust model excludes, not a divergence.
4. **Skip.** An executor drops `i` only on `Void(i)` (`crates/engine/src/reader/threads.rs:314-347`).
   The sealer appends `Void(i)` only when every executor voted
   (`VoidLedger.java:181-186`). An executor that joined `i` does not vote. So `Void(i)` exists
   only when no executor joined `i`.
5. **A vote is final evidence.** A vote needs `RangeAbsent` from every ingress archive and
   `not_held` from every peer. `RangeAbsent` is a definite answer about one copy
   (`crates/log/src/error.rs:21-28`). A peer that answered `not_held` reached `i` without the
   bytes. After that, no source can produce the bytes, so no voter can later join `i`.
6. **The residual case.** If a bug breaks step 5, a voter that joins `i` after its vote can
   meet `Void(i)` later. The reader then stops with `VoidOfExecutedEntry`
   (`crates/engine/src/reader/threads.rs:353-362`). The stop is loud, and the repair is a peer
   checkpoint. To keep this case out, a reader that sent a vote for `i` never executes `i` in
   the same run. It waits for `Void(i)`.
7. **`lost` answers.** A peer that answers `lost` holds a state with `i`. The asker never votes
   while such an answer stands. When every peer answered `not_held` or `lost` and at least one
   answered `lost`, the asker takes the peer-checkpoint repair: it fetches a checkpoint at or
   above the block of `i` from a `lost` peer (`docs/failure-modes.md:334-343`). The repair
   aligns the asker with the peer that executed `i`.

## 5. The validator, the batcher and other consumers

### 5.1 The consumer reader

- The engine gets a second join source. The wiring seam names it. The executor wiring keeps
  the `tx_data` source. The validator and the batcher wirings name the executor stream source.
- `EngineWiring::TxData` (`crates/engine/src/actor/wiring.rs:97-105`) becomes
  `EngineWiring::TxSource`. Its two implementations:

| Source | Used by | Buffer key | Miss path | Vote |
|---|---|---|---|---|
| `TxDataSource` | executor | `TxDataKey` | ingress archives, then peer executor archives | yes |
| `ExecStreamSource` | validator, batcher | canonical index | executor archives by locator | never |

- The batcher builds its reader stack by hand (`crates/batcher/src/live/run.rs:297-349`). It
  names `ExecStreamSource` there.
- No `dyn`. The two sources are concrete types behind an associated type.

### 5.2 The check of a record (validator independence)

A consumer accepts the record at `i` only when:

- the canonical order carries a `TxRef` at `i`;
- `record.tx_ref == TxRef(i)`;
- `keccak256(record.envelope.raw_tx) == TxRef(i).tx_hash`.

The validator also checks the identity on every record, as today
(`crates/engine/src/actor/exec_records.rs:137-142`,
`crates/validator/src/bin/kardamom-validator/wiring/run.rs:170`). The validator never trusts
the executor's choice to skip. It drops `i` only on `Void(i)` in the canonical order. A record
that fails a check counts in `kardamom_exec_stream_record_rejected_total{reason}` and drops. The
consumer waits for another copy.

### 5.3 Live read and dedup

- One live subscription of `exec_txs` with one destination for each executor.
- The buffer is a bounded `BTreeMap<u64, TxEnvelope>`. An insert keeps the first record that
  passes the check. The validator's `KeyedBuffer` has this shape
  (`crates/validator/src/buffers.rs:54-130`). The record moves to a shared crate so the batcher
  uses the same code.
- Three executors publish each record. The consumer takes the first and drops the other two.

### 5.4 Catch-up after a restart

- The sealer replays the canonical order from the consumer's cursor, as today.
- The live exec stream does not hold old records. The first miss starts the refetch:
  1. Ask each executor `kardamom_getExecLocator(i)`.
  2. Replay the named archive from the locator with the bounded replay of the refetch client
     (`crates/log/src/refetch.rs:308`). One replay delivers up to the drain cap. Every record
     goes into the buffer.
  3. The next misses hit the buffer. At the end of a stopped recording, ask again for the next
     index. A later session covers it.
- `ArchiveRefetcher::fetch_tx_data` generalizes to a typed record replay. The replay stays
  pinned to the session (`crates/log/src/refetch.rs:25-30`, `crates/log/src/refetch.rs:73-75`).
  The executor stream archives come from `EndpointSource::Discovered { topic: ExecTxs }`
  (`crates/log/src/refetch.rs:81-104`), filtered by the `archive_id` of the locator.
- Past the sealer retention, the validator keeps its peer-checkpoint adoption
  (`docs/failure-modes.md:481-491`), and the batcher keeps the rebuild from references
  (section 5.6).

### 5.5 A gap in the stream

| Situation | Behavior |
|---|---|
| A live frame lost | The miss refetches from the archive of any executor that answers `located`. |
| No executor reached `i` yet | Every answer is `not_reached`. The consumer waits. It reads ahead for `Void(i)` during the wait. |
| Every executor is down | No answer. The consumer waits and exports `kardamom_exec_stream_wait_seconds`. It is a pause, not a halt. |
| `Void(i)` arrives | The consumer drops `i`. It sends a vacant slot to exec, as a voter does (`crates/engine/src/reader/threads.rs:345-346`). |
| Every copy fails the check | The validator halts, and an operator clears the halt (decision 7, section 13). The batcher waits and alerts: it posts only checked bytes. |

The wait has no deadline. A consumer outside the executors cannot decide an entry, so a
deadline would only restart it into the same entry. The prior attempt shows that loop for a
non-voting consumer.

### 5.6 The batcher

- The live feed reads records from `ExecStreamSource`. It closes blocks as today.
- The batcher posts only bytes that an executor joined. L1 and the executors cannot disagree
  on an entry.
- `rebuild.rs` keeps its `EnvelopeSource` seam (`crates/batcher/src/live/rebuild.rs:38-47`).
  `ArchiveEnvelopes` (`crates/batcher/src/live/rebuild.rs:64-66`) becomes `ExecArchiveEnvelopes`:
  - `kardamom_getBlockRefs` gives the refs and the `exec_locator` of each block;
  - one replay from the locator delivers the block's records;
  - the hash check at `crates/batcher/src/live/rebuild.rs:261-289` stays.
- The validator's query endpoint has no `exec_locator`. The batcher asks the next endpoint
  for the bytes.
- `--block-refs-source` keeps its list (`deploy/cluster/nomad/batcher.nomad.hcl:269-270`).
- The batcher no longer needs `--archive-control-response-endpoint` for `tx_data`. It still
  needs a response and a replay endpoint for the executor archives.

### 5.7 Other consumers

- No other process joins `tx_data` today. The readers are the executor, the validator, the
  batcher and the sequencer (`crates/engine/src/bin_support.rs:193`,
  `crates/sequencer/src/bin/kardamom-sequencer/main.rs:272`). The sequencer reads `tx_data` before
  the order exists. It stays on `tx_data`.
- A new consumer of ordered transaction bytes uses `ExecStreamSource`.
- The e2e harness (`crates/e2e/src/harness/launch.rs`) launches the validator and the batcher
  with the new flags.

## 6. The recorded-head guard

### 6.1 Is the guard still required?

The ingress archives stay. They cover an executor fleet outage as today
(`docs/failure-modes.md:305-308`). The guard becomes a second line. The recommendation is to
keep it, on by default, for four reasons:

1. **The ingress archives can miss a range.** A driver loss ends both recordings before the
   last frames (`archive-driver-loss`, the prior attempt section 1). A volume wipe with a
   lost mirror, or a power loss at sync level 0, does the same. If every executor is down at
   that time, every entry ordered meanwhile becomes a void.
2. **It keeps a parked entry inside the void window.** Today a vote can arrive after the entry
   left the window, and the chain stops (`docs/failure-modes.md:258-259`). With the guard, a
   parked fleet stops user records after the budget.
3. **It bounds the work after an outage.** The executors catch up at most the budget plus
   deposits.
4. **It costs little.** One 10-byte log entry per executor per 100 ms.

The cost: during an executor fleet outage the chain refuses user transactions after the
budget, with a typed error. Today it accepts them into a backlog. No receipt arrives during
the outage in either case. The client resubmits.

### 6.2 The rule

- The sealer keeps `recorded[executor_id]` in the replicated state. A cursor only moves up.
- `best_recorded = max(recorded)` over the configured executors.
- The sealer refuses a user record when `recordLagBudget > 0` and
  `canonicalCount - (best_recorded + 1) > recordLagBudget`.
- The check sits next to the DA-lag check: after dedup and deadline, before the window and the
  contiguity guard (`CanonicalSealerState.java:770-777`). A refused record moves nothing. The
  zero sender is exempt, so deposits enter.
- While the map is empty, the guard refuses nothing. A bring-up, or a restore from a v10
  snapshot, then does not halt before the first executor sends its cursor.
- The cursor frame from an id outside the voter mask drops as malformed.

### 6.3 Max or min over the executors

| Choice | Effect |
|---|---|
| max (recommended) | One recorded copy is enough. Every executor and consumer can fetch from it (section 4). One dead or lagging executor does not stop the chain (`docs/failure-modes.md:289`). |
| min | One dead executor stops all user records after the budget. This breaks the rule above. |
| k-th highest | k copies. A later setting `recordQuorum`, not in this design. The ingress archives already give a second copy to the executors. |

### 6.4 The budget

The budget is in canonical records, because the sealer sees references, not bytes.

| Bound | Value |
|---|---|
| One `tx_data` term | 4 MiB (`deploy/cluster/nomad/aeron.system.nomad.hcl:199`). A transfer frame is about 224 B, so one term holds about 18 700 transfers. A term holds about 3 700 records of 1 KiB calldata. |
| Void window | 65 536 indices (`docs/failure-modes.md:253`). The budget plus the deposits during a wait must stay below it. |
| Steady recorded lag | The cursor cadence (100 ms) plus the recording latency. At 10 000 tx/s that is about 1 100 records. At 200 tx/s (the chaos load, `crates/chaos/src/knobs.rs:267`) about 25 records. |

- Default: **16 384 records**. It is one term of transfers on a single lane, a quarter of the
  void window, and about 15 times the steady lag at 10 000 tx/s.
- At 200 tx/s the guard trips about 80 s into an executor fleet outage. At 10 000 tx/s after
  about 1.6 s.
- For large calldata the budget is more than one term. The ingress archives cover that case.
  The guard does not claim to.
- Sizing rule for an operator: `rate x 0.15 s x 10 <= budget <= voidWindow / 4`.
- Every member must use the same value. A change needs the coordinated sealer restart of
  section 9.4.

### 6.5 The cursor publisher

- The publisher thread keeps `(end_position, index)` for each record not yet recorded. It
  polls the recording position every 20 ms.
- The reader also sends a progress mark for each dispatched message, so the cursor passes
  epochs, deposits and boundaries with no new record.
- `recorded_through` = the highest mark whose position the recording passed.
- The executor sends the cursor when it moved and 100 ms passed, or when it moved by 1024
  records. It follows the posted-cursor pattern (`crates/engine/src/reader/cluster/mod.rs:133-163`,
  `crates/batcher/src/live/posted_cursor.rs:13-63`).

### 6.6 Halt, alerts and metrics

| Item | Value |
|---|---|
| Halt cause | `record_lag`, service `sealer`, raised by the ingress from the status flag, clears `auto`. It follows `da_lag` (`crates/ingress/src/chain.rs:104-119`). When both flags stand, the ingress names `record_lag`, the root nearer the source. |
| Runbook | `docs/runbooks/record_lag.md`, with Cause, Confirm, Steps, Clear (`docs/runbooks/README.md:1-16`). The unit test in `crates/obs` checks it. |
| Alert | `KardamomHaltRecordLag` on `kardamom_halt{cause="record_lag"} == 1` in `deploy/alerts.yml`, next to `KardamomHaltDaLag` (`deploy/alerts.yml:184-193`). |
| Client error | `chain halted: record_lag at sealer`, code -32010, as `da_lag` (`docs/runbooks/da_lag.md:16-18`). The JSON-RPC mapping is at `crates/ingress/src/json_rpc.rs:437`. |
| Ingress gauges | `kardamom_ingress_cluster_recorded_head`, `kardamom_ingress_cluster_record_lag`. |
| Executor gauges | `kardamom_executor_exec_stream_recorded_index`, `kardamom_executor_exec_stream_session_id`, and the counter `kardamom_executor_exec_stream_publish_blocked_ms_total` (milliseconds, because a `metrics` counter is an integer). |
| Sealer log | `cluster RECORDED-CURSOR executor=<id> recorded=<n> best=<n> halted=<b>`, on change only. |
| Engine counter | `kardamom_engine_peer_fetch_total{outcome}` with `located`, `not_held`, `not_reached`, `lost`, `unreachable`, `mismatch`. |

The runbook steps: find why no executor records (all down, all parked at one entry, all
archives stalled); follow `executor` failure modes; do not set the budget to 0 to make the
chain live, as for `da_lag` (`docs/runbooks/da_lag.md:26-29`).

## 7. The ack policies

Today:

- `AckPolicy` has four modes (`crates/types/src/ack_policy.rs:20-32`).
- `OnQuorum` takes its watermark from the cluster egress progress
  (`crates/ingress/src/bin/kardamom-ingress/watermark.rs:1-4`).
- `OnLocalFsync` and `OnLocalFsyncAndQuorum` wait on the `FsyncWatermark` stream
  (`crates/ingress/src/pending/mod.rs:385-390`). The ingress subscribes it
  (`crates/ingress/src/aeron_adapters.rs:340`). No process publishes it: the handle exists
  (`crates/log/src/aeron_live/handles/simple.rs:323`), and only test fakes write it
  (`crates/log/src/testing/fakes.rs:498`). A submit under these modes waits until its timeout.
- The default and the deploy are `on-offer` (`crates/ingress/src/bin/kardamom-ingress/main.rs:118-131`,
  `deploy/cluster/nomad/ingress.nomad.hcl:26-30`).

The change:

| Mode | New meaning |
|---|---|
| `on-offer` | Unchanged. |
| `on-quorum` | Unchanged. The reference is Raft-committed. |
| `on-recorded` (replaces `on-local-fsync`) | The ingress releases when `best_recorded >= receipt.tx_idx`, from the status frame. One executor archive holds the bytes at its sync level. |
| `on-local-fsync-and-quorum` | Removed. An executor reads only committed egress, so a recorded record is also quorum-committed. The mode would equal `on-recorded`. |

- The removal breaks no deploy. Nothing uses the two modes, and they never release.
- The sealer sends a status frame when `best_recorded` moves, as it does for the posted cursor
  (`SealerClusteredService.java:777-800`). The latency of `on-recorded` is about one cursor
  cadence, 100 ms to 200 ms.
- These go with the two modes: `FsyncWatermark` (`crates/types/src/watermark.rs:7-13`), the
  fsync channel template and stream id 1010 (`crates/log/src/config/mod.rs:400-404`,
  `crates/log/src/config/mod.rs:573-576`); the ingress flag `--recorder-id`
  (`crates/ingress/src/bin/kardamom-ingress/main.rs:85-88`). `QuorumWatermark` stays. The executor flag
  `--recorder-id` stays: it names the executor's plane (`crates/executor/src/bin/kardamom-executor/main.rs:187`).

## 8. Retention and pruning of the executor archives

Today nothing prunes an archive. The batcher rebuild relies on that
(`docs/failure-modes.md:539`).

The executor prunes its own recordings:

- `floor_index = min(end_index(posted_head), canonical_count - voidWindow) - keep_margin`.
  - `end_index(posted_head)` is the canonical end of the posted block from the `headers` table
    (`crates/state/src/schema.rs:9`). L1 holds everything at or below it.
  - `voidWindow` is the sealer's void window. A peer may need a record inside it.
  - `keep_margin` is `--exec-archive-keep-records`, default 1 048 576 (about 1.5 hours at
    200 tx/s). It gives a lagging validator room before it needs a checkpoint.
- The executor learns the posted head from the status frame. The engine cluster subscription
  ignores the status frame today (`crates/engine/src/reader/cluster/mod.rs:266`). It exposes it
  on a watch channel.
- Every 10 minutes:
  - a stopped recording whose last locator index is below `floor_index` is purged
    (`purgeRecording`);
  - the active recording purges whole segments below the locator of `floor_index`
    (`purgeSegments` on a segment boundary);
  - the locator log drops the entries below the oldest kept recording.
- A prune never passes the posted head, so the batcher rebuild keeps its source.
- The ingress `tx_data` archives keep their present retention. Section 11 asks about them.

## 9. Failure modes

| Failure | Effect | Recovery |
|---|---|---|
| Ingress death | In-flight clients retry on the other ingress. Frames cut by the kill can reach some executors and not the archives. | The executors that got the frames record them. The others fetch from those archives. No void. The validator and the batcher read the executor stream, so they never park on it. |
| One executor death | Its stream and cursor stop. `best_recorded` follows the live executors. | Nomad restarts it. It preloads its own tail, then joins live, then the ingress archives, then the peers. The consumers read the other two streams. |
| All executors down | No records, no receipts. After the budget, the sealer refuses user records with `RecordLag` and the ingress shows the sealer halted on `record_lag`. Deposits continue. The validator and the batcher wait. | The executors return and catch up from the ingress archives. If the ingress archives miss a range, every executor parks, every one answers `not_held`, and every one votes. The void resets the senders. The guard bounds the count to the budget. |
| Executor archive loss | That executor answers `lost` for its retained range. Its new records go to a new recording. | The consumers and the peers read the other two archives. Redundancy recovers as the lost range passes the prune floor. Section 11 asks about a re-record tool. |
| Executor archive corruption | A replay with the CRC check produces no fragments, or a record fails the hash check. | The reader asks the next executor (`docs/failure-modes.md:883-885`). The hash check catches a flip that the CRC misses. |
| A void | Unchanged mechanics. The voters are the executors. | The sender resubmits. The validator and the batcher follow `Void(i)` with no vote. |
| A lagging executor | Its joins miss on live `tx_data`. It answers `not_reached` to a parked peer, so that peer waits for it. | It catches up from the ingress archives or from the peer archives. When it reaches `i`, it joins or parks and answers. |
| A validator restart | Within the sealer retention, it replays the order and refetches bytes from the executor archives by locator. | Past the sealer retention, it adopts a peer checkpoint, as today. Past the executor archive floor, the same. |
| A batcher restart past the sealer retention | `REPLAY_UNAVAILABLE` (`docs/failure-modes.md:530-537`). | It rebuilds from `kardamom_getBlockRefs` and the executor archives. The prune floor keeps every unposted record. A refusal after the rebuild is `replay_unavailable`, as today. |
| `archive-driver-loss` | The driver dies under ingress-0. Both ingress recordings end before the last frames of an entry. Executors 0 and 1 got the frames live. | Executors 0 and 1 record and serve the entry. Executor 2 fetches it from them. The validator and the batcher read it from the stream. No vote, no loop. |
| Driver loss on an executor node | That executor, its stream and its archive stop. | Same as one executor death. The archive segments persist on the node volume (`docs/failure-modes.md:858`). |
| A forged sender from a proxy | The live executors execute it. A fetching executor rejects the copy and stalls. | The validator halts on `RecordIdentity`. Out of the CFT model (`crates/types/src/envelope.rs:16-17`). |
| Mixed sealer versions during a roll | An old member drops kind 9. The recorded map differs between members. | Harmless while the guard is off. The guard turns on only after every member runs the new image (section 10). |

## 10. Performance

| Item | Effect |
|---|---|
| Leaves the hot path | The validator and the batcher leave the `tx_data` publications. A lane publication sends one unicast copy per destination (`docs/aeron-discovery.md:14-20`). The destinations drop from 9 (2 sequencers, 3 executors, validator, batcher, 2 archives) to 7. Ingress egress drops by about 22%. |
| Stays on the hot path | Nothing new before the executors. |
| Executor reader | One envelope clone (the `Bytes` is shared) and one channel send per `TxRef`. The rkyv encode runs on the publisher thread. |
| Executor egress | rate x 270 B x 2 consumers per executor, plus the local IPC copy. 200 tx/s: 108 KB/s. 10 000 tx/s: 5.4 MB/s. |
| Consumer ingress | 3 copies of each record, one per executor. 200 tx/s: 160 KB/s. 10 000 tx/s: 8.1 MB/s. Today one copy of about 224 B. |
| Executor disk write | rate x 270 B. 200 tx/s: 4.7 GB/day. 10 000 tx/s: 233 GB/day. With the prune floor, the kept size is the posted lag plus the void window plus the margin: about 300 MB at the defaults for transfers. |
| `fdatasync` | One per archive write batch at level 1, on the executor node. Phase 2 measures the effect on the commit loop. |
| Sealer | One cursor log entry per executor per 100 ms. One status frame per cursor advance to each session. |

At the 10 Ggas target (333 000 tx/s, `docs/specs/2026-08-16-10-ggas-cluster-shape.md:9-13`),
three copies to each consumer cost about 270 MB/s. A consumer then subscribes to one executor
and fails over. That knob is out of this design (section 11).

## 11. Migration

### 11.1 Both paths stay alive

- The ingress archives stay, so there is no removal phase.
- The validator and the batcher get a flag `--tx-source tx-data|exec-stream`. The default
  stays `tx-data` until their switch phase. The deploy flips the flag.
- On `exec-stream`, the validator and the batcher never vote. Until the voter set changes,
  the sealer still needs their ids, so no void can complete. A lost entry then waits. This is
  the safe side. Keep the window short: deploy the switch and the voter-set change in one
  maintenance window.

### 11.2 Wire breaks and chain reset

- No chain reset. No state DB migration.
- The new stream, the cursor kind, the status tail and the snapshot v11 are additive (section
  3.4, 3.5).
- The guard turns on only after every sealer, sequencer, ingress and notifier runs the new
  image.
- An old sealer image cannot read a v11 snapshot. A sealer rollback past v11 invalidates the
  v11 snapshots first (`ClusterTool invalidate-latest-snapshot`) and replays the log from the
  last v10 snapshot.

### 11.3 Deploy order

1. Runtime with `Topic::ExecTxs` everywhere.
2. Executors publish and record. Node meta `archive_topics = exec_txs` on executor nodes.
3. Sealer with kind 9 and v11, the guard off. Executors send cursors.
4. Sequencer, ingress and notifier with kind 13, `RecordLag` and `record_lag`.
5. Validator, then batcher, on `--tx-source exec-stream`.
6. Coordinated sealer restart (11.4): executor-only voters and the guard budget together. The
   validator and the batcher lose `--void-voter-id` in the same deploy.

### 11.4 The coordinated sealer restart

- `voidVoters` and `recordLagBudget` decide accept or reject in the replicated state, and they
  are not in the snapshot (`cluster/sealer-service/README.md:311`, `cluster/sealer-service/README.md:327`).
- A rolling change runs members with two values at once. A vote that one member decides and
  another does not forks the canonical state.
- So the change stops all three members, then starts all three with the new values. The
  pipeline stalls as in a quorum loss, about 50 s, SLO 180 s (`docs/failure-modes.md:206`).
- `VoidLedger.onVote` compares `after != config.voterMask` (`VoidLedger.java:181`). An open
  vote from a removed voter keeps its bit, so the mask never equals the smaller set. The
  decide becomes `(after & voterMask) == voterMask`, and the restore drops the bits outside
  the mask.

### 11.5 Rollback

| Phase | Rollback |
|---|---|
| Executor stream, cursors | Roll the executor image back. The extra archive and the locator log stay unused. |
| Validator, batcher switch | Flip `--tx-source` back to `tx-data`. |
| Voter set and guard | Coordinated sealer restart with `0..executor_count+1` and budget 0. The validator and the batcher get `--void-voter-id` back, on `tx-data`. |
| Ack policy | Roll the ingress back. No deploy uses the removed modes. |
| Sealer image after v11 | Invalidate the v11 snapshots, then roll back (11.2). |

## 12. Implementation plan

Every phase is one pull request. Each one passes CI alone and follows `docs/STYLE.md`. The
graph:

```text
 P1 ──┬──> P2 ──┬──────────────┬──> P4 ──┬──> P10 (guard on)
      │         │              │         └──> P12 (ack policy)
      │         ├──> P5 (peer step)
      │         ├──> P6 (validator) ──┐
      │         ├──> P7 (batcher)  ───┴──> P9 (executor-only voters)
      │         └──────────────┬──> P11 (pruning)
      └──> P3 ──┬──────────────┘
                ├──> P4
                └──> P8 (reject path, halt) ──> P10
```

- P2 and P3 run in parallel after P1.
- P5, P6 and P7 run in parallel after P2.
- P4 needs P2 and P3. P8 needs P3 only. P11 needs P2 and P3.
- P9 needs P6 and P7. P10 needs P4 and P8. P12 needs P4.
- P9 and P10 merge as two pull requests. Their deploy is one coordinated sealer restart
  (section 11.4).

The open change that sets `archive_file_sync_level` in the aeron job is a prerequisite of P2.

### P1. Types, topic and stream

- Code:
  - `crates/types/src/exec_record.rs`: `ExecTxRecord`. Export from `crates/types/src/lib.rs`.
  - `crates/log/src/discovery/record.rs`: `Topic::ExecTxs`, `as_str`, `parse`, `term_length`.
  - `crates/log/src/config/mod.rs`: `exec_txs_channel`, `exec_txs_stream_id = 1005`, and the
    collision check in `validate`.
  - `crates/log/src/aeron_live/handles/`: `ExecTxsPublisherHandle` and
    `ExecTxsSubscriberHandle` with the macro of `simple.rs`.
  - `crates/log/src/discovery/plane/streams.rs`: the publisher and the subscription methods.
  - `deploy/cluster/config/channels.toml.tpl`: the channel and the stream id.
- Tests: rkyv round trip in `crates/types/tests/rkyv_roundtrip.rs`; topic parse and term
  length; config default and collision; the Docker-gated discovered transport test in
  `crates/log/tests/` for one publisher and one subscriber.
- Chaos: none.
- Docs: `docs/aeron-discovery.md` topic table.

### P2. The executor publishes and records its stream

- Code:
  - `crates/engine/src/reader/ports.rs`: an `ExecStreamSink` port. The reader sends
    `(index, tx_ref, envelope)` after a join, and a progress mark after each message.
    The validator and the batcher wirings use a no-op sink type.
  - `crates/engine/src/reader/threads.rs`: `on_tx_ref` sends to the sink before
    `ReaderToExec::Tx` (`crates/engine/src/reader/threads.rs:289-306`).
  - `crates/executor/src/exec_stream/` (new): `ExecStreamPublisher` owns both publications,
    the locator log and the recorded cursor computation. It runs on `rt_pub`
    (`crates/executor/src/bin/kardamom-executor/main.rs:175-201`).
  - `crates/executor/src/exec_stream/locators.rs`: the locator log, append and lookup.
  - The executor starts the local recording with `record_stream_until_stopped` and waits for
    it before readiness.
  - `deploy/cluster/ansible/roles/nomad/templates/nomad.hcl.j2:86`: `exec_txs` on executor
    nodes. `deploy/cluster/ansible/roles/contract/vars/main.yml:29` follows.
- Tests: the publisher orders records as the reader sends them; the locator log survives a
  torn tail; a lookup returns a lower bound; the recorded cursor never passes the recording
  position; back-pressure blocks the sink and does not drop.
- Chaos: `hard-executor` and `graceful-executor` assert that the restarted executor records a
  new session and that its cursor gauge advances.
- Docs: `docs/aeron-discovery.md` (publications, archive topics), `docs/failure-modes.md`
  executor section, `crates/log/README.md` durability model.
- Rules: R5 (one owner thread, channels), R6 (sink as an associated type), R14 (reuse
  `record_stream_until_stopped`).
- Deviations in P2 (#531):
  - The recorded publication is exclusive (`AeronRuntime::open_exclusive_publication`). A shared
    IPC publication puts the records of every executor on one media driver into one session.
  - The blocked counter is `kardamom_executor_exec_stream_publish_blocked_ms_total`, in
    milliseconds. The gauge `kardamom_executor_exec_stream_session_id` is new. The chaos check
    reads it.
  - The first locator of a session names the first reported recording position, a lower bound.
  - The end of the local recording is fatal (section 3.6). The loss wait follows the driver
    timeout of the archive client, with the rule of the client start budget.
  - A static plane whose `exec_txs_channel` is IPC opens only the recorded publication.
  - The spied-UDP question of section 3.1 stays open. P2 keeps the two publications.

### P3. The sealer recorded cursor (guard code, off)

- Code:
  - `SealerWire.java`: `KIND_RECORDED_CURSOR = 9`, `EGRESS_KIND_RECORD_LAG_REJECT = 13`, the
    status tail.
  - `CanonicalSealerState.java`: `onRecordedCursor`, `bestRecorded`, `recordLagHalted`, the
    check in `onRecord`, snapshot v11.
  - `SealerClusteredService.java`: dispatch kind 9; status with the tail; offer status on an
    advance.
  - `SealerEgress.java`: the reject frame.
  - `crates/cluster-adapter/src/wire/`: the constants, `encode_ingress_recorded_cursor`, the
    status tail decode, `EgressItem::RecordLagReject`.
  - `crates/engine/src/reader/cluster/mod.rs`: `RecordedCursorPublisher`, like
    `PostedCursorPublisher`; the status frame on a watch channel.
- Tests: Java state tests for the guard, the max rule, the zero-sender exemption, the
  first-cursor rule, the voter-mask filter, and the v10 and v11 snapshot load; Rust wire
  round trips and the 50-byte status frame.
- Chaos: none. The budget is 0.
- Docs: `cluster/sealer-service/README.md` (kinds, guard, settings, snapshot).

### P4. The executor sends its recorded cursor

- Code:
  - `crates/executor/src/exec_stream/`: the cursor cadence of section 6.5 drives the
    `RecordedCursorPublisher` of P3.
  - `crates/ingress/src/chain.rs` and `crates/ingress/src/metrics.rs`: the status tail into
    `kardamom_ingress_cluster_recorded_head` and `_record_lag`.
  - The sealer log line `cluster RECORDED-CURSOR`.
- Tests: the cadence (time and record count); a cursor never moves down; the ingress gauges
  from a 67-byte and a 50-byte status frame.
- Chaos: `hard-executor` asserts that the sealer's best recorded cursor keeps moving while one
  executor is down.
- Docs: `docs/observability.md`, `cluster/sealer-service/README.md` log lines.

### P5. The executor peer step

- Code:
  - `crates/state/src/nonce_query.rs`: `kardamom_getExecLocator`, and `exec_locator` in
    `kardamom_getBlockRefs`.
  - `crates/state/src/exec_answers.rs` (new): the owner thread of the answers state.
  - `crates/engine/src/reader/peer_fetch.rs` (new): the asks, the archive replay, the check.
  - `crates/engine/src/reader/void.rs`: `ParkPlan`, the peer poll inside the park, the
    no-execute-after-vote rule, the `lost` repair signal.
  - `crates/engine/src/reader/join.rs`: the park also on `TimedOut`, with no vote.
  - The own-tail preload at resume.
  - `crates/log/src/refetch.rs`: a typed record replay that `fetch_tx_data` and the exec
    stream share, and a target archive by `archive_id`.
  - `crates/executor/src/bin/kardamom-executor/args.rs`: `--exec-peers`, `--exec-self`.
  - `deploy/cluster/nomad/executor.nomad.hcl`: render the peers as
    `http://executor-<i>.node.<dc>.consul:<executor_query_port>`.
- Tests: from the prior `tests_peer.rs`: one peer holds, every peer `not_held`, no answer
  then `not_held`, `not_reached` until close, a void ends the wait, wrong hash, wrong sender,
  `lost`; the answers state; the JSON round trip through the real query server.
- Chaos: `archive-driver-loss` prints the number of peer fetches as evidence and asserts no
  restart loop. A new case `exec-peer-fetch`: drop the `tx_data` UDP to executor-2 and stop the
  ingress recordings for one window, then assert that executor-2 converges with no void.
- Docs: `docs/failure-modes.md` void section and executor section.
- Deviations in P5:
  - The answering executor learns its archive id from a third flag, `--exec-archive-id`, rendered
    as `${node.unique.name}`, the `archive_id` of its archive record.
  - The answers state keeps no joined set. In the current run, an index below the reached bound
    with no park is joined. Below the run, the state DB decides: a receipt at the index is
    joined (`located`, or `lost` with no locator at or below it), no receipt is `not_held`.
  - A replay that the named archive refuses (`RangeAbsent`) counts as `lost`.
  - A replay keeps the records after the entry by canonical index (at most 65 536), and the
    reader checks each one at its turn. The own-tail preload uses the same store.
  - `lost` from a peer, with every other peer final, stops the reader with `PeerRecordLost` at
    the block that holds the entry. The block is the first boundary in the read-ahead whose end
    passes the entry. The executor binary repairs it with the replay-window repair.
  - With no peer configured (the validator and the batcher), the join path is unchanged.
  - `exec-peer-fetch` stops the ingress recordings with iptables `u32` matches on the Aeron
    stream id: the ingress nodes drop the `tx_data` data frames for 60 s, so each recording ends
    and a new one starts after the gap. Executor-2 drops them until it fetches from a peer.

### P6. The validator reads the executor stream

- Code:
  - `crates/engine/src/actor/wiring.rs`: `TxSource` replaces `TxData`.
  - `crates/engine/src/reader/exec_stream.rs` (new): `ExecStreamSource`, the index buffer,
    the locator refetch, the non-voting wait.
  - `crates/engine/src/bin_support.rs`: `open_inbound` takes the source by flag.
  - `crates/validator/src/bin/kardamom-validator/`: `--tx-source`; the buffer moves from
    `crates/validator/src/buffers.rs:54-130` to a shared crate.
  - `deploy/cluster/nomad/validator.nomad.hcl`: the flag.
- Tests: dedup of three copies; a rejected record; a gap filled by refetch; a void with no
  vote; a restart that refetches its cursor range.
- Chaos: `validator-lapse` and `validator-join` run on the new source. A new case
  `validator-exec-archive-catchup`: stop the validator past the live window, start it, assert
  `blocks_verified` rises with no checkpoint adoption.
- Docs: `docs/failure-modes.md` validator section, `docs/observability.md`.

### P7. The batcher reads the executor stream

- Code:
  - `crates/batcher/src/live/run.rs`: `ExecStreamSource` in `spawn_reader_stack`
    (`crates/batcher/src/live/run.rs:297-349`).
  - `crates/batcher/src/live/rebuild.rs`: `ExecArchiveEnvelopes`.
  - `crates/batcher/src/live/refs_store.rs`: `exec_locator` in `BlockRefs`.
  - `crates/batcher/src/bin/kardamom-batcher.rs`: `--tx-source`.
  - `deploy/cluster/nomad/batcher.nomad.hcl`: the flag.
- Tests: `rebuild_tests.rs` with the exec-archive source; a block with a missing record names
  it; the live feed packs the same bytes as before.
- Chaos: `batcher-outage-past-retention` runs on the new source. Its rebuild must read the
  executor archives.
- Docs: `docs/l1-data-path.md`, `docs/failure-modes.md` batcher section.

### P8. The guard reject path and the halt

- Code:
  - `crates/types/src/tx_error.rs`: `TxErrorReason::RecordLag`.
  - `crates/types/src/service.rs`: `HaltCause::RecordLag`, `ALL`.
  - `crates/sequencer/src/bin/kardamom-sequencer/feeds.rs`: `on_record_lag_frame`, like
    `on_da_lag_frame` (`feeds.rs:317-345`).
  - `crates/ingress/src/chain.rs`: raise `record_lag`; `crates/ingress/src/json_rpc.rs`: the
    error.
  - `docs/runbooks/record_lag.md`, `docs/runbooks/README.md`, `deploy/alerts.yml`.
- Tests: the `crates/obs` runbook and alert tests; the sequencer refusal path; the ingress
  status mapping with both flags.
- Chaos: none. The budget is still 0.
- Docs: `docs/failure-modes.md` halts table.

### P9. Executor-only voters

- Code:
  - `VoidLedger.java`: the masked decide and the restore filter.
  - `deploy/cluster/nomad/cluster.nomad.hcl:191`: `range(var.executor_count)`.
  - The validator and the batcher jobs drop `--void-voter-id`. The validator and the batcher
    code drop the voter field, since they never vote.
  - `deploy/cluster/README.md`: the coordinated sealer restart.
- Tests: Java `VoidLedger` with a stale bit; the reader follows a void with no voter id.
- Chaos: `pipeline-blackout-recover` asserts that every void decision has three executor
  votes. `archive-driver-loss` asserts zero voids.
- Docs: `docs/failure-modes.md` void section and voter table, `cluster/sealer-service/README.md`.

### P10. The guard on

- Code: `deploy/cluster/nomad/cluster.nomad.hcl`: `cluster_record_lag_budget = 16384`; the
  Ansible variable `KARDAMOM_RECORD_LAG_BUDGET`, as `KARDAMOM_DA_LAG_BUDGET_BLOCKS`
  (`deploy/cluster/README.md:298`).
- Tests: the Ansible deploy tests for the variable.
- Chaos: a new case `record-lag-halt`, like `da-lag-halt` (`docs/failure-modes.md:153-157`):
  freeze every executor, assert the typed refusal, the sealer halted on `record_lag`, the
  ingresses paused, and a resume with no operator step. `executor-fleet-loss-recover` accepts
  the refusals during the outage.
- Docs: `docs/failure-modes.md` guard section, `docs/chaos-suite.md`.

### P11. Executor archive pruning

- Code: `crates/executor/src/exec_stream/prune.rs`; the status-frame watch from P3; `--exec-archive-keep-records`.
- Tests: the floor computation; no purge above the posted head; the locator log trim.
- Chaos: a new case `exec-archive-prune` with a small margin: assert the disk use falls and a
  batcher rebuild still works.
- Docs: `docs/failure-modes.md` substrate section.

### P12. The ack policy

- Code: `crates/types/src/ack_policy.rs`; `crates/ingress/src/pending/mod.rs`; the ingress
  arguments; remove `FsyncWatermark`, the fsync handles and the config fields.
- Tests: `pending/tests.rs` for `on-recorded`; the requires table.
- Chaos: none.
- Docs: `docs/failure-modes.md` ack window (`docs/failure-modes.md:362-365`), `docs/json-rpc.md`.

### 12.1 What carries over from the prior attempt

| Part | Use |
|---|---|
| `void.rs` `ParkPlan`, the optional voter id, the read-ahead during the peer wait | P5 and P6, almost as is. |
| `peer_fetch.rs` state machine, answers, re-ask cadence, labels | P5. The transport changes from envelope bytes over JSON-RPC to a locator plus an archive replay. |
| `held_envelopes.rs` owner thread and the "reached" bound | P5 `exec_answers.rs`. The ring of envelopes goes; the map of locators stays. |
| `nonce_query.rs` method plumbing | P5, with the new method name and answers. |
| The fetched-envelope check (prior section 4.3) | Section 4.3 here, unchanged. |
| `tests_peer.rs` fake peers over loopback HTTP | P5, with a fake archive for the replay. |
| `cluster.nomad.hcl` executor-only voters, the validator and batcher jobs without voter ids | P9. |
| `executor.nomad.hcl` peer list rendering | P5. |
| `crates/chaos/src/cases/archive.rs` evidence print | P5. |
| The peer-fetch metric and its docs in `docs/observability.md` | P5, with the new `lost` outcome. |

The 128 MiB in-memory ring does not carry over. The archive replaces it. The archive survives
a restart, so the open case "an executor executed the entry and restarted past it" of the prior
attempt closes: that executor answers `located` from its archive, or `lost`, and never
`not_held`.

### 12.2 Style rules to check in each phase

- R1: comments state the protocol rule. No phase names, no spec names.
- R5: the publisher thread, the locator log and the answers state each have one owner and
  talk over channels.
- R6: `TxSource` and `ExecStreamSink` are associated types. No `dyn`.
- R9: parse the cursor frame, the locator answer and the record once at the boundary.
- R12: checked arithmetic for `canonicalCount - (best_recorded + 1)` and every wire value.
- R14: one replay helper for `tx_data` and `exec_txs`; one keyed buffer for the validator and
  the batcher.
- R16: the park loop body stays one step.

## 13. Decisions

The user decided the open points of this design as follows.

1. **Voter-set rollout.** One coordinated restart of all sealer members (11.4). The one-time stall is acceptable. There is no replicated voter-set record.
2. **The guard default.** On by default, at 16 384 records, with the maximum over the executors.
3. **The ingress readiness gate.** Keep it. It is a start gate, not a per-transaction wait, so it does not touch the hot path.
4. **Ingress archive retention.** In scope, as the last phase (P13): the ingress `tx_data` archives prune at the same floor as the executor archives (section 8).
5. **Consumer fan-in.** Accept three copies for each consumer for now. A "one primary executor" setting can follow if the bandwidth matters.
6. **Executor archive loss.** Accept reduced redundancy until the prune floor passes the lost range. The peers and the ingress archives cover the range. There is no re-record tool in this design.
7. **All copies fail the check on the validator.** The validator halts, and an operator clears the halt. A record that does not match its canonical hash is an integrity fault, not an availability fault. P6 defines the halt cause and its runbook.
8. **The stall on archive back-pressure.** Confirmed. An executor whose archive cannot take a record stalls. The other executors carry the chain.

### P13. Ingress archive pruning

- Code: the ingress `tx_data` archives prune below the floor of section 8, with the same rule and the same guard against a range that an executor still needs.
- Tests: the floor computation; no prune above the floor; a prune never removes a range that the void window still covers.
- Docs: `docs/aeron-discovery.md`, `docs/failure-modes.md` (Substrate).

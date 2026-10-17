# Transaction bytes in the sealer's Raft log

- Status: approved by the user on 2026-10-11. Amended for the user decision of 2026-10-17
  (decision 10, section 16): the sealer keeps no retained file, and the serving service keeps
  the transactions for the batcher and the attester. Partly implemented (section 13).
- Issue: refs #582.
- Reviewed by dev-32 (the batcher resume and DA side) and kardamom-3-00 (the executor side). Their points are in this version.
- Decided by the user:
  - The void voting goes. The `VoidLedger`, the voter ids, the join, the refetch, the peer fetch
    and the record-lag guard duplicate the Raft log of Aeron Cluster.
  - Option B (decision 9, 2026-10-11). The sequencers stay and offer the full transaction into
    the cluster. There is no envelope path from the ingress into the cluster, and no join.
    Section 15 gives option A, the first choice, and why the user replaced it.
- Decided by the user on 2026-10-11 (section 16):
  - The design target is up to 5,000 tx/s with mixed sizes.
  - The feed from member `i` to executor `i` is IPC through one shared media driver.
  - Staging moves by a chain reset. Production uses the drained cut-over.
  - The starting limits of decision 4 are accepted. The benchmark tunes them.
  - No byte budget halts the chain. The serving service keeps every unposted entry on disk
    (decision 10).
  - The `tx_heads` stream is in scope.
  - A member feeds only its own executor, over IPC. A separate service serves the validator,
    the batcher and external nodes (section 8.1, out of scope).
  - The boundary stream digest is a later follow-up.
- Decided by the user on 2026-10-17 (decisions 10 to 13):
  - The sealer keeps no retained file. Its Raft log and its snapshots are its only record.
  - The serving service reads only from the executors. It keeps the append-only log of the
    transactions above the posted head for the batcher and the attester.
  - The member feed replays from the member's own Raft log, from a snapshot.
  - The executors keep their output only until the serving service stores it (section 8.3).
  - Two read tiers: the internal services read the executor (hot); the external readers read
    the serving service, read replicas of one log (cold). The ingress asks the serving service
    for a receipt that the executors pruned (section 8.4).
  - The notifier is removed for now: no WebSocket status feed and no webhooks (section 8.4).
  - The target: a transaction lands within 50 ms of its submit (section 10.0).
- Scope: the ingress size cap, the sequencer offer with bytes, the `tx_heads` stream, the sealer
  state machine and its snapshot, the input of the executors, and the removal of the void path.
  The service that serves the canonical stream to the validator, the batcher and external nodes
  is out of scope. This spec states only what it needs from that service, and the executor
  side of the handoff (sections 8.1 and 8.3).
- Every claim about the current code cites `path:line` on `main` at `0e0c34f25`. The amendment of
  2026-10-17 cites `main` at `1d2529033`.

## 1. Summary

### 1.1 The change

- The ingress validates each transaction and publishes the envelope on `tx_data`, as today. It
  offers no transaction data into the cluster. A new cap refuses a transaction above 128 KiB.
- The sequencers stay. Each offers a **full entry** for each transaction: the guard header
  (sender, nonce, deadline, tip), the transaction hash, the lane and the envelope.
- Both twins of a lane offer each entry, so the Raft log holds each transaction twice. The sealer
  keeps the first copy.
- The sealer runs the record path of today, without the void. Each ordered record carries its
  bytes. The sealer emits one canonical entry with the full envelope. There is no envelope store,
  no waiting reference and no join.
- Each sealer member publishes the canonical stream, with bytes, to its paired executor: the
  **member feed**. The member and the executor run on one host and share one media driver, so
  the feed is IPC (section 7.6).
- The executors execute strictly in canonical order. There is no join, no refetch, no void and
  no vote.
- The members serve no other consumer of the bytes. The validator, the batcher, executors
  without a paired member and external nodes read the canonical stream from a separate serving
  service. That service has its own design (section 8.1). It reads the output of the executors,
  not the members.
- A member keeps no frame with bytes. Its Raft log and its snapshots are its only record. The
  purge keeps the 3 newest snapshots and their log, and the log above the stored head of the
  serving service (section 5.5). The feed replays from that log (section 7.3).
- The serving service keeps every entry above the posted head and above the attested head, with
  its bytes and its receipt, for as long as L1 does not hold it. No byte budget stops the chain.
  The executors keep their output only until the serving service stores it (section 8.3).
- The executors, the validator and the batcher no longer read `tx_data`. Only the sequencers read
  it. The ingress `tx_data` recordings go.

### 1.2 Invariants

| Id | Invariant |
|---|---|
| R1 | Transaction bytes enter the cluster only as Raft log input, through the sequencer sessions. The canonical stream is a function of the committed log only. |
| R2 | A canonical transaction entry always carries its envelope. A committed entry without bytes cannot exist. |
| R3 | Every member emits the same canonical stream, byte for byte. A consumer can switch between two sources of that stream at any index. |
| R4 | The service thread of a member never waits for a consumer. A slow executor never slows Raft. |
| R5 | An executor executes index `i` only after it executed `i - 1`. It never skips an index. |
| R6 | The bytes that one sequencer replica holds, in its parked buffer and its unconfirmed ledger, never pass a fixed bound (section 6.2). |
| R7 | Every committed entry of a block above the posted head stays in at least one store until L1 holds it. The member's Raft log keeps it until the serving service stores it (the stored head). The executor's output keeps it until then too. The serving service keeps it until the posted head and the attested head pass it (sections 5.5, 8.1 and 8.3). |

### 1.3 What the design does not guarantee

- It does not make a transaction durable before the cluster commits it. The twins hold it in
  memory until then. A lane that loses both twins loses its transactions in flight, as today.
  The client gets no ack under the default `on-quorum` policy (section 6.4), so it retries.
- It does not check the signature in the sealer. The executors trust the `sender` field that the
  ingress stamped, as today. The validator checks the signature (section 8).
- It does not remove the leader as the bandwidth ceiling. The leader receives every entry, twice,
  and sends it to each follower (section 10).
- It does not bound the disk of the serving service while L1 posts stop. That disk grows with
  the DA lag. The disk of a member and of an executor grows only while the serving service
  stores nothing. An alert warns before a disk fills (section 11).
- It does not stop ordered junk from an unfunded new key. The size cap bounds each entry. A
  stricter rule is a follow-up (section 6.6).

## 2. Current flow

### 2.1 The pieces and what each does today

| Step | Behavior today | Citation |
|---|---|---|
| Ingress publish | The ingress picks the lane from the sender and publishes the `TxEnvelope` on `tx_data[lane]`. It stamps `max_inclusion_block`, the inclusion deadline. | `crates/ingress/src/proxy/submit.rs:322-344` |
| Ingress record | One recorder thread for each lane records `tx_data` in the ingress archive. RPC waits for the recorder barrier. | `crates/ingress/src/bin/kardamom-ingress/recorders.rs:37-60`, `crates/ingress/src/bin/kardamom-ingress/main.rs:404-413` |
| Envelope | `TxEnvelope { correlation_id, raw_tx, sender, tx_hash, max_inclusion_block }`. The rkyv fixed part is 80 bytes. | `crates/types/src/envelope.rs:12-35`, `formats.toml:224-231` |
| Sequencer read | Two lanes, two racing replicas each. A replica reads `tx_data`, decodes the nonce and the fee fields from `raw_tx`, runs the fee gate, and keeps a `RefMetadata`. It drops the bytes. | `crates/sequencer/src/sequencer.rs:811-896`, `docs/failure-modes.md:474-476` |
| Reference | `TxRef { tx_hash, shard_id, tx_data_position, tx_data_session_id }`. The kind-0 frame is 99 bytes: an 85-byte guard header and id, the record type, 13 bytes of archive pointer. | `crates/types/src/txref.rs:42-60`, `crates/cluster-adapter/src/wire/ingress.rs:67-80`, `cluster/sealer-service/service/src/main/java/io/kardamom/sealer/cluster/SealerWire.java:32-62` |
| Offer | Up to 13 refs in one kind-3 batch, below one Aeron MTU, because the cluster ingress path does not survive a fragmented session message. A back-pressured offer rebuffers and retries. | `crates/sequencer/src/sequencer.rs:458-468`, `crates/sequencer/src/sequencer.rs:486-492`, `crates/sequencer/src/outbound/cluster.rs:49-99` |
| Ordering window | With priority fees on, 20 records wait at most 5 ms and flush by tip. | `cluster/sealer-service/README.md:258-275` |
| Dedup | A first-seen window on the 32-byte id, pruned by deadline, capacity 131,072. | `cluster/sealer-service/core/src/main/java/io/kardamom/sealer/CanonicalSealerState.java:227-234`, `:663-671` |
| Deadline | A record past its deadline gets `PAST_DEADLINE`. The deadline is clamped to the open block plus 64. | `CanonicalSealerState.java:843`, `:674`, `:718-721` |
| Guards | The record-lag guard, then the DA-lag guard, then the window cap. | `CanonicalSealerState.java:855-866` |
| Contiguity | The expected nonce of each sender. A gap gets `CONTIGUITY_REJECT`. | `CanonicalSealerState.java:867-874` |
| Relay | The leader sends the relayed frame (egress kind 1, 59 bytes for a `TxRef`) to every consumer session, and the boundary (kind 2) to every session. It keeps at least 65,536 frames, and every frame above the posted head, for replay. | `cluster/sealer-service/service/src/main/java/io/kardamom/sealer/cluster/SealerEgress.java:420-449`, `:641-645`, `:668-678` |
| Egress back-pressure | A per-session backlog. The leader closes a session after 1 s with no progress or above 64 MiB. | `cluster/sealer-service/service/src/main/java/io/kardamom/sealer/cluster/SessionBacklogs.java:55`, `:63`, `:245-260` |
| Join | The executor, the validator and the batcher open 8 live `tx_data` subscriptions and join each `TxRef` by `(shard, session, position)`. | `crates/engine/src/bin_support.rs:193-208`, `crates/engine/src/reader/join.rs:14-89` |
| Refetch | After 10 s the reader refetches from the ingress archives until the join budget ends: 30 s on a resume, 60 s fresh. | `crates/engine/src/reader/join.rs:121-131`, `:281-322`, `crates/engine/src/bin_support.rs:131-137` |
| Vote | When every archive answers `RangeAbsent`, a reader with a voter id votes `KIND_VOID_REQUEST` every 5 s and waits up to 120 s. Without a voter id it stops. | `crates/engine/src/reader/threads.rs:335-368`, `crates/engine/src/reader/void.rs:32-149`, `crates/log/src/error.rs:27-34` |
| Voters | Executors `0..executor_count`, the validator and the batcher. | `deploy/cluster/nomad/cluster.nomad.hcl:117-126`, `:228` |
| Void decide | The sealer appends `Void(i)` when the vote mask equals the voter mask. The void frees the hash and sets the expected nonce to the lower value (decision version 2, #568). | `cluster/sealer-service/core/src/main/java/io/kardamom/sealer/VoidLedger.java:167-191`, `CanonicalSealerState.java:925-933`, `CanonicalSealerState.java:111` |
| `exec_txs` (P1, P2) | Each executor publishes the records it joined on stream 1005 and records them locally. No consumer reads it outside a test. | `crates/types/src/exec_record.rs:20-29`, `crates/executor/src/exec_stream/open.rs:35-196`, `crates/log/src/config/mod.rs:724` |
| Recorded cursor (P3, P4) | Behind `--exec-cursor`, the executor sends `KIND_RECORDED_CURSOR`. The sealer keeps the best cursor. | `crates/executor/src/bin/kardamom-executor/args.rs:85-91`, `:194-201`, `cluster/sealer-service/service/src/main/java/io/kardamom/sealer/cluster/SealerClusteredService.java:899-927` |
| Record-lag guard (P3, P8) | Off (budget 0). A budget above 0 stops the start while the snapshot writer is v10. The reject path, the halt, the runbook and the alert exist. | `cluster/sealer-service/core/src/main/java/io/kardamom/sealer/RecordedCursors.java:77-78`, `cluster/sealer-service/service/src/main/java/io/kardamom/sealer/cluster/ClusterNode.java:685-694`, `deploy/alerts.yml:225-233`, `docs/runbooks/record_lag.md` |
| Executor resume | From the state DB: the last committed block and the last fsynced reader position. Then a cluster replay from that cursor. | `crates/engine/src/actor/types.rs:96-109`, `crates/engine/src/bin_support.rs:338-342` |
| Cluster client | The client picks members round-robin and follows `NewLeaderEvent`. Only the leader sends egress. | `crates/cluster-adapter/src/live/endpoints.rs:50-65`, `crates/cluster-adapter/src/live/session_loop.rs:445-470`, `cluster/sealer-service/README.md:163` |
| Ingress observer | Each ingress subscribes to the relayed stream to see sealed hashes for the `on-quorum` ack. | `crates/ingress/src/cluster.rs:150-178` |
| Batcher resume | Spool, then sealer replay, then a rebuild from `kardamom_getBlockRefs` and the `tx_data` archives. | `crates/batcher/src/live/run.rs:506-522`, `:641-669`, `crates/batcher/src/live/refs_store.rs:174-180`, `docs/failure-modes.md:709-729` |
| Log purge, floor | Each member purges its log to the newest snapshot that is older than the 3 newest and at or below the posted head. | `cluster/sealer-service/service/src/main/java/io/kardamom/sealer/cluster/PurgePlanner.java:65-73` |

```text
 ingress x2 ── tx_data (8 lanes, MDC) ──┬──> sequencer x4 ── TxRef (99 B) ──> sealer (Raft) ──┐
   └─ recorder ─> ingress archive x2    │                                                    │ egress (leader)
                                        ├──> executor x3 ─┐                                   ├──> executor x3
                                        ├──> validator    ├─ join, refetch, vote <────────────├──> validator
                                        └──> batcher  ────┘                                   ├──> batcher
                                                                                              └──> ingress x2
```

### 2.2 Why the sealer orders references today

- The cost model puts the sequencer order, dedup and contiguity at "O(1), sub-µs" per
  transaction (`docs/specs/2026-08-16-pipeline-cost-model.md`, stage 3). A reference keeps that
  stage small in bytes too: 99 bytes for each offer, whatever the size of the transaction.
- The cluster shape for 10 Ggas/s names the single ordering point and the leader's egress
  fan-out as the first limits (`docs/specs/2026-08-16-10-ggas-cluster-shape.md`, tier 2 and
  "What breaks first").
- The price is the join: a reference can commit before any archive holds its bytes
  (`VoidLedger.java:14-15`). The void voting, the record-lag guard and the executor stream are the
  answers to that gap. This design closes the gap at its source and pays in Raft bandwidth.
  Section 10 gives the numbers.

## 3. New flow

```text
 ingress x2 ── tx_data (8 lanes, as today) ──> sequencer x4 (2 twins a lane)
                                                  │ full entry (kind 0, record type 5):
                                                  │ guard header + hash + lane + envelope
                                                  ▼
                                 sealer (Raft log: full entries, one copy from each twin)
                                                  │
                         every member: canonical entry = record + envelope (RT_TX)
                                                  │
            ┌─────────────────────────────────────┤
            ▼ member feed (IPC), member i -> executor i    ▼ egress (leader only)
        executor 0..2  (strict order, no join, no vote)    ingress x2: references only (kind 15)
            │ output: frames + receipts (8.3)
            ▼                                   stored head (ingress kind 11) ──> sealer purge floor
        serving service (separate design, 8.1) ──── stored head ──> executor output prune
            │
            ▼
 validator (attester), batcher, unpaired executors, external nodes
```

| Step | New behavior |
|---|---|
| Ingress validate | Unchanged: decode, signature recovery, the checks. New: a size cap of 128 KiB for each transaction (section 6.6). |
| Ingress publish | Unchanged: the envelope on `tx_data[lane]`. The ingress offers no transaction data into the cluster. It also publishes a header on `tx_heads` (section 6.5). |
| Sequencer | It reads `tx_data` as today. It offers a full entry: kind 0 with record type 5 (`RT_TX_OFFER`), the guard header, the lane and the envelope (section 6.1). Both twins of a lane offer each entry. |
| Sealer | The record path of today, without the void (section 5.1). The first twin copy wins the dedup. |
| Canonical entry | A relayed frame of record type 6 (`RT_TX`) with the lane and the envelope. |
| Executor | It reads the feed of its paired member. It falls back to the serving service (section 7). It keeps its output until the serving service stores it (section 8.3). |
| Serving service | It reads the frames and the receipts from the executors, stores them, and sends its stored head back (sections 8.1 and 8.3). |
| Validator, batcher | They read the serving service (section 8.1). No member sends them bytes. |
| Ingress observer | It subscribes as today and gets 42-byte kind-15 frames (section 6.4). |

## 4. Wire and log formats

All integers are little-endian, as today (`cluster/sealer-service/README.md:25-28`).

### 4.1 The full entry (ingress kind 0, record type 5, new)

```text
[kind=0][sender:20][nonce:u64][deadline:u64][tip:u128][canonical_id:32][record_type=5][lane:u8][envelope]
```

- The fixed part is 87 bytes. The envelope is the rkyv `TxEnvelope`: 80 bytes + `raw_tx`. So an
  entry is `167 + raw_tx` bytes. In a kind-3 batch each entry adds a 4-byte length:
  `171 + raw_tx` bytes.
- The guard header is today's (`SealerWire.java:32-59`). The sealer reads it as today.
- `canonical_id` is the transaction hash. `lane` is `lane_for(sender)`.
- `envelope` is the frozen layout of `aeron-stream-records` (`formats.toml:207-231`). The sealer
  does not parse it.
- The 13-byte archive pointer of `RT_TXREF` goes. Nothing reads an archive.
- The batch limit becomes a byte limit: at most 64 KiB of entries in one message. A larger entry
  goes alone in its message. With the size cap of section 6.6, one entry is at most
  128 KiB + 171 bytes.

### 4.2 The canonical entry (relayed record type 6, new)

```text
egress kind 1: [kind=1][index:u64][payload_len:u32][payload]
payload:       [canonical_id:32][record_type=6][lane:u8][envelope]
```

- 13 + 34 + 80 + `raw_tx` = `127 + raw_tx` bytes. Today a `TxRef` frame is 59 bytes
  (`SealerWire.java:244`, `SealerEgress.java:652-666`).
- The sealer builds the payload from the ordered record: `canonical_id`, the lane and the
  envelope. The guard header stays out, as today.
- The reader copies the payload to an aligned buffer before it reads the rkyv part, as it does
  for the epoch records today.
- Record types 1, 2 and 3 (deposit reference, epoch, remote epoch) do not change.

### 4.3 Other wire changes

| Item | Change |
|---|---|
| Ingress kind 2 (subscribe) | Unchanged. The egress sends a subscriber kind 15, not kind 1, for each record type 6 entry. No egress session gets the bytes. |
| Egress kind 15 `RELAYED_REF` (new) | `[kind=15][index:u64][canonical_id:32][record_type:u8]`, 42 bytes. Every egress subscriber gets it for each record type 6 entry. Other record types stay kind 1. |
| Ingress kind 6 (void request), kind 9 (recorded cursor) | Retired. A member drops them as unknown kinds (`SealerClusteredService.java:547-551`). |
| Egress kind 13 (record-lag reject) | Retired. |
| Egress kind 14 | Not used. An earlier draft used it for `ENVELOPE_FULL` (section 15, option A). |
| Egress kind 9 (status) | The 17-byte record-lag tail stays as a fixed field with the "no cursor" values (`best_recorded = u64::MAX`, budget 0, not halted). There is no byte tail: a member retains no bytes (section 5.5). The frame stays 68 bytes, as with `carryBytes` off. The 76-byte frame with `[retained_bytes:u64]` of #608 goes with the retained file. A reader still accepts a longer frame and ignores the tail. |
| Ingress kind 11 `STORED_CURSOR` (new) | `[kind=11][stored_head:u64]`, 9 bytes. The serving service sends it, as the batcher sends kind 7. It has no guard header. The sealer keeps the highest value (section 5.5). |
| Egress kind 16 `FEED_GAP` (new, feed only) | `[kind=16][next_index:u64][next_block:u64]`, 17 bytes (section 7.2). |
| Record types 0 (`TxRef`) and 4 (void) | Retired after the cut-over (section 12). |

### 4.4 Fragmented session messages

- Today a cluster session message must fit one MTU, because "the hand-rolled cluster ingress path
  does not survive fragmented session messages" (`crates/sequencer/src/sequencer.rs:458-468`).
  The egress encoder states the same limit (`crates/cluster-adapter/src/wire/egress.rs:414-415`).
- An 8 KB entry does not fit one MTU (1,344 bytes, `deploy/cluster/nomad/cluster.nomad.hcl:379`).
  It is about 7 fragments. The first pull request (section 13, PR 1) finds and fixes the cause
  on both sides:
  - The ingress side of the cluster: a session message of up to 1 MiB through
    `LiveIngress::offer` (`crates/cluster-adapter/src/live/mod.rs:96-114`). The sequencer
    session uses it.
  - The egress side and the feed: each subscription behind the fragment assembler that the
    stream subscriptions already use (`crates/log/src/aeron_live/thread.rs:30-94`).
  - A test sends 64 KiB and 1 MiB messages both ways through a `TestCluster`.
- The Aeron limit is a term length divided by 8. The cluster log term is 8 MiB
  (`cluster/sealer-service/README.md:394`), so one message can be 1 MiB.

### 4.5 Bytes for each transaction

| Item | Today | Transfer (raw 110 B) | 2 KB call | 8 KB call |
|---|---|---|---|---|
| Full entry in the log, one copy | 103 B (a reference) | 281 B | 2,219 B | 8,363 B |
| Log input per transaction, 2 twin copies, framing excluded | 206 B | 562 B | 4,438 B | 16,726 B |
| Log input with framing (about 3 %) | about 0.21 KB | about 0.58 KB | about 4.6 KB | about 17.2 KB |
| Canonical frame (feed, serving service) | 59 B | 237 B | 2,175 B | 8,319 B |
| Kind-15 frame (ingress observer) | 59 B | 42 B | 42 B | 42 B |

- The framing is the 32-byte session message header and the 32-byte Aeron frame header for each
  1,312-byte fragment.
- Both twins of a lane offer each entry, and Aeron Cluster appends each message to the log before
  the service sees it. So the log holds each transaction twice. The dedup drops the second copy.
  Option A (section 15) put the bytes in the log once, at the cost of a store and a join. The
  hedge of section 6.3 brings the log back to one copy, if the benchmark needs it.

## 5. The sealer state machine

### 5.1 The record path

The record path of today (`CanonicalSealerState.java:826-880`) stays. The record now carries its
envelope, and the void goes:

1. **Dedup.** The id is in the dedup window: a duplicate. Drop it. The losing twin copy drops here.
2. **Deadline.** The open block is above the deadline: `PAST_DEADLINE`.
3. **DA-lag guard.** As today, in blocks only. No byte budget refuses a record (section 5.5).
4. **Capacity.** The dedup window is full: `WINDOW_FULL`.
5. **Contiguity.** The nonce must be the expected nonce, as today
   (`CanonicalSealerState.java:867-874`). Else `CONTIGUITY_REJECT`.
6. **Order.** Insert the id in the dedup window, assign the next canonical index, advance the
   expected nonce, and relay the `RT_TX` entry with the envelope.

- The ordering window (`cluster/sealer-service/README.md:258-275`) stays in front of this path.
  The window flushes into step 1. It holds the envelopes of the records that wait in it, at most
  20 records or 5 ms.
- A refused record leaves no state. The sequencer handles each answer as today. It frees a refused
  nonce and parks the later entries above it (#561, `docs/failure-modes.md:520-529`).
- Every input of this path is in the log. Every member decides alike (R1, R3).

### 5.2 No store and no join

- The bytes and the order arrive in one log entry. No order waits for its bytes, and no bytes
  wait for their order. So the sealer has no envelope store, no waiting references, no TTL, no
  lane cap and no pairing. The settings `laneEnvelopeBytes` and `waitingRefs` of option A do not
  exist.
- Only the sequencer's checks decide which transactions reach the log. A transaction that the
  sequencer refuses (a fee reject, a past nonce, a future nonce that never fills) never enters
  Raft (section 15).

### 5.3 The void and the guards go

- The `VoidLedger` (`VoidLedger.java`), `onVoidRequest` and `appendVoid`
  (`CanonicalSealerState.java:919-933`) go. A canonical transaction entry always has its bytes
  (R2), so no entry needs a void.
- The recorded cursors (`RecordedCursors.java`), `onRecordedCursor`
  (`CanonicalSealerState.java:1312-1322`) and the record-lag guard go.
- The settings `voidVoters`, `voidWindow` and `recordLagBudget` go
  (`cluster/sealer-service/README.md:356-359`, `ClusterNode.java:44`, `:143-144`).

### 5.4 Decision version 3

- The change decides an ordered entry differently: the record carries its bytes, and no void
  exists. So `DECISION_VERSION` goes to 3 (`CanonicalSealerState.java:111`), with the
  coordinated restart of `cluster/sealer-service/README.md:396-422`.
- The decision version stays as a mechanism (#568). Version 2's rule (the lower nonce after a
  void) has no input after version 3, because no void exists.

### 5.5 Retention, the snapshot, the purge and the floor

**No retained frames (decision 10).** With `carryBytes` on, a member keeps no canonical frame. Its
Raft log and its snapshots are its only record of the stream.

- The retained file of #608 goes: the segment files, the helper thread, the index, the copy with
  a peer seed, the admin endpoint `/retained` and its token.
- The egress retains no frame. It sends each live frame as today: kind 15 for a record of type 6,
  kind 1 for the other record types, kind 2 for a boundary. An egress replay request (ingress
  kind 1) gets `REPLAY_UNAVAILABLE` with the head as its floor. After the cut-over no production
  consumer sends one: the ingress observer never replays (`crates/ingress/src/cluster.rs:113-123`),
  the executors read their feed, and the validator and the batcher read the serving service.
- The feed replays from the Raft log (section 7.3).
- With `carryBytes` off, the heap store of today stays: the `TxRef` frames, the `retention`
  window and the posted-head floor (`SealerEgress.java:420-449`).
- The status frame has no byte tail (section 4.3).
- **The earlier rule moves.** The rule "keep the data until L1 holds it, do not halt" now applies
  to the serving service (section 8.1). The DA-lag guard in blocks stays as today
  (`DEFAULT_DA_LAG_BUDGET_BLOCKS = 10_000`, `CanonicalSealerState.java:128`; 60,000 on staging).
  No byte budget refuses a record or halts the chain (decision 5).
- **Out of scope: the inclusion cost.** A long DA outage leaves a large backlog above the posted
  head. How the batcher posts that backlog to L1, and at what cost, is a future optimisation. This
  spec does not design it. The batcher posts by its rules of today (#569).

**The Raft log and its purge.** The log holds the full entries, two twin copies of each. The
purge keeps the newest snapshots and their log, as today (`PurgePlanner.java:65-73`):

- **The margin.** The purge point is a snapshot older than the `keepSnapshots` newest
  (`kardamom.cluster.logPurgeKeepSnapshots`, default 3). At a 300 s interval the log keeps at
  least 15 minutes.
- **The floor.** Today the purge point must be at or below the posted head. With this amendment
  the floor is `max(posted_head, stored_head)`. The stored head is the newest block `S` such that
  the serving service holds every frame and every receipt of each block up to `S` on its durable
  store (section 8.3).

| Log on each member | Transfer | 2 KB | 8 KB |
|---|---|---|---|
| Per transaction (2 twin copies, section 4.5) | 0.58 KB | 4.6 KB | 17.2 KB |
| Per block at 1,000 tx/s | 1.16 MB | 9.2 MB | 34.5 MB |
| 15 minutes at 1,000 tx/s | 522 MB | 4.1 GB | 15.5 GB |
| Above the floor | grows only while the stored head does not move: an outage of the serving service, or of every executor. Section 10.0 gives the rate. |

**Is a floor still needed once the batcher reads the serving service? Yes, but not the posted
head.** The floor moves from the posted head to the stored head:

- **Not the posted head.** With that floor, a DA outage grows the log of each member by the full
  log input: two twin copies of each entry, about 36 GB an hour at the mix and 5,000 tx/s. That is
  the growth that decision 10 moves to the serving service.
- **Not "no floor".** The serving service reads only from the executors, and the executors read
  from the members. So an entry that the sealer ordered, but that no executor wrote out and the
  serving service did not store, lives only in the Raft log. When every executor, or the serving
  service, stops for longer than the margin, a purge with no floor deletes committed entries that
  no other store holds. No reader can then get them, and the batcher cannot post that range.
  R7 forbids that.
- **The stored head.** It keeps exactly that range. In a DA outage the stored head moves, so the
  log does not grow. In an outage of the serving service or of every executor, the log grows:
  that range is in no other store.
- **Why `max`.** A block at or below the posted head is on L1, so the log never needs it. The
  batcher posts only what it read from the serving service, so the posted head passes the stored
  head only when the service serves a block before its stored head covers it. And the start needs
  no special case: before the serving service sends its first kind 11, the stored head is 0, and
  the floor is the posted head of today.
- **The input.** The serving service sends ingress kind 11 `STORED_CURSOR` (section 4.3) through
  a publisher-only cluster session, as the batcher sends kind 7. The sealer keeps the highest
  value, and replicates it, so every member plans the purge from the same value. It does not
  change the decision of any record, so it needs no new decision version. The snapshot carries it
  (below).

**Disk full.** The Raft log cannot write. The member logs a fatal error and stops, as Aeron does
today. Section 11 gives the effect and the alert. `kardamom_sealer_disk_free_bytes` (#608) stays:
it now watches the log.

**Gauges.** The admin `/status` of a member shows `storedHead` beside `postedHead`. The status frame
gets no new field. The sealer logs `cluster STORED-CURSOR` when the stored head moves up.

**The snapshot (version 12).** `CanonicalSealerState.java:1474-1538` and
`SealerEgress.java:558-610` change as follows:

| Section | v10 (written today) | v12 |
|---|---|---|
| Dedup window, sender map, origins, remote peers | unchanged | unchanged |
| Void ledger (v6) | present | gone |
| Ordering window, posted head, seed (v8 to v10) | unchanged | unchanged |
| Stored head (new, kind 11) | absent | after the posted head |
| Recorded cursors (v11, never written) | absent | absent |
| Retained frames | `TxRef` frames, 59 B | none. The retained-file section of #608 goes. |

- **v12 is not released.** `formats.toml` still writes 10 for `sealer-snapshot`, and v12 is
  written only under the activation flag `kardamom.cluster.carryBytes`. No deployed release turns
  that flag on. So v12 changes in place: no version 13 is needed. The flag turns on only at the
  cut-over (PR 13, section 12).
- **Restore.** A member with `carryBytes` on restores v12, and also v10: it drops the retained
  `TxRef` frames of v10 and starts with no retained frame. Every member drops the same frames, so
  the restore is deterministic. A member with `carryBytes` off still refuses v12, because v12 has
  no void ledger. So the drained cut-over (section 12.3) needs no import of the v10 frames.
- There is no envelope store and no waiting-reference section (section 5.2). The ordering
  window holds at most 20 records with their envelopes.
- The snapshot size is the state of today, minus the void ledger and the retained frames, plus
  8 bytes. It does not grow with the DA lag.
- The heap holds the same data. The sealer runs with `-Xmx384m` and 1,024 MB of job memory
  (`deploy/cluster/nomad/cluster.nomad.hcl:362`, `:413`). The accepted starting value is a 2 GiB
  heap. The heap holds no retained frame and no envelope store. A feed replay holds a second copy
  of the state on the walk thread (section 7.3). The benchmark measures the snapshot time, the GC
  pauses with a full dedup window, and the heap during a walk, and tunes the heap.
- **Precondition: the host memory.** On staging, each dedicated host (8 cores) runs sealer
  member `i` (cpuset 0-3) and executor `i` (cpuset 4-7) as two node containers. ded-3 took
  sealer-0 and executor-0 in infra #51 (`kardamom-infra`,
  `ansible/inventories/staging/hosts.ini:27-66`). The infra repository states no memory size for
  these hosts, and the node containers set no memory limit. So PR 13 does not deploy until the
  operator confirms that each dedicated host holds the 2 GiB sealer heap, the sealer page cache
  for the log, the executor's 1,536 MB job (`deploy/cluster/nomad/executor.nomad.hcl:325-326`)
  and its MDBX map, with margin. The deploy preflight checks the host memory against a stated
  minimum.
- **Precondition: the disk.** The operator states the disk size of each member, each executor
  and each host of the serving service. Section 10.0 gives the growth rates.
- **The snapshot pause.** Every member takes the snapshot at the same log position
  (`cluster/sealer-service/README.md:513`), so a long write pauses the service thread of every
  member at once. The snapshot holds no frame, so its size does not grow with the DA lag. The
  bound is 2 s. The benchmark measures it (PR 12).

## 6. The sequencer and the ingress

### 6.1 The sequencer offer

- The sequencer reads `tx_data` as today. It already gets the full envelope
  (`crates/sequencer/src/sequencer.rs:907-955`). Today it decodes the fields it needs and drops
  the bytes (`crates/sequencer/src/sequencer.rs:860-896`).
- Now `RefMetadata` (`crates/sequencer/src/sequencer.rs:958-967`) keeps the envelope. `raw_tx` is
  `bytes::Bytes`, so the clone is a reference count, not a copy.
- It offers the full entry of section 4.1 in a kind-3 batch with a byte limit of 64 KiB. A
  large entry goes in fragments (section 4.4).
- A back-pressured offer uses the rewind and retry path of today
  (`crates/sequencer/src/sequencer.rs:458-492`).
- The answers do not change: `PAST_DEADLINE`, `CONTIGUITY_REJECT`, `WINDOW_FULL`,
  `DA_LAG_REJECT`. The sequencer frees a refused nonce, as today (#561).
- **Output.** One replica orders about 2,500 tx/s at the target. It sends about 2.5 MB/s with the
  mix and 21.5 MB/s with 8 KB only. Today it sends about 0.23 MB/s. Two lanes share a node, so one
  node sends about 43 MB/s (0.34 Gbit/s) at 8 KB only.

### 6.2 Byte bounds in the sequencer

The parked buffer and the unconfirmed ledger now hold bytes. Each gets a byte bound. The bounds
are local to one replica. They are not replicated, and a full bound does not touch other lanes or
Raft.

| Buffer | Bound (proposed starting value) | When full |
|---|---|---|
| Parked future nonces | `--parked-bytes`, 64 MiB for each replica, beside the count of 16 for each sender (`crates/sequencer/src/config.rs:16`) | It evicts the furthest future nonce, as `RejectedTooFar` does today. |
| Unconfirmed ledger | `--unconfirmed-bytes`, 256 MiB for each replica | It back-pressures the lane: the replica reads no new entry until a receipt frees space. |

- The unconfirmed ledger holds bytes until a receipt confirms the entry (`confirm_timeout_ms`
  15,000). At 2,500 tx/s for each lane and 1 s of receipt latency, it holds about 2.4 MB with
  the mix and 21 MB with 8 KB only. The 256 MiB bound covers about 12 s at 8 KB only.
- The benchmark tunes both values (section 10.6).
- Metrics: `kardamom_sequencer_parked_bytes`, `kardamom_sequencer_unconfirmed_bytes`,
  `kardamom_sequencer_byte_evictions_total`.

### 6.3 The twins, the re-offer and the hedge

- Both replicas of a lane offer each entry, as today. The sealer keeps the first copy
  (`docs/failure-modes.md:434`). Both copies carry the bytes, and both enter the log.
- **One re-offer path.** A leader change loses the offers in flight (cluster ingress is at most
  once, `docs/failure-modes.md:257-258`). The unconfirmed ledger re-offers them with their bytes
  after `confirm_timeout_ms` (`crates/sequencer/src/unconfirmed.rs:1-13`). The dedup drops the
  copies that did commit.
- **Durability.** A transaction in flight lives in the memory of both twins until the commit. One
  twin can die without a loss. A lane that loses both twins loses its transactions in flight, as
  today.
- **The hedged offer (a later option).** If the benchmark shows that the twin copy is the limit:
  - each replica owns half of the vslots and offers them at once;
  - the other replica offers an entry of the other half only when the entry is not ordered within
    a short time (for example 20 to 50 ms);
  - the sequencer reads the kind-15 frames (section 4.3) as its quick "ordered" signal.
  The hedge brings the log back to one copy. The cost is extra latency for half of the senders
  while one replica is down. This program does not build it (section 16.3).

### 6.4 The ack policy

| Policy | Today (`crates/types/src/ack_policy.rs:22-31`) | After |
|---|---|---|
| `on-offer` | Ack after the offer to the pipeline. | Unchanged: ack after the `tx_data` publication took the offer. |
| `on-quorum` (default) | Ack after the sealer ordered the hash. | Unchanged meaning. The observer subscribes as today and sees 42-byte kind-15 frames, not the bytes. |
| `on-local-fsync`, `on-local-fsync-and-quorum` | Wait for the ingress recorder's fsync. | Retired with the recorders. |

### 6.5 What the sequencer reads, and `tx_heads`

- The sequencer keeps reading the full envelope on `tx_data`. It needs the bytes, because it
  offers them (section 6.1).
- Only the sequencers subscribe to `tx_data` now. The executors, the validator and the batcher
  stop.
- **`tx_heads` (in scope, decision 6).** The ingress publishes a header-only `TxHeader` of about
  160 bytes on a new stream `tx_heads` (base 3000 + lane; the `tx_data` range 2000 to 2255 must
  stay free, `crates/log/src/config/mod.rs:503-545`).
  - The header carries no `raw_tx`. It holds the sender, the nonce and the hash, and the fixed
    fields of the guard header and the fee gate: the deadline, the fee fields, the gas limit, the
    value and the correlation id.
  - `TxEnvelope` is a frozen layout, so the header is a new record on a new stream
    (`docs/formats.md:149`).
  - Under option B the sequencer cannot read `tx_heads` in place of `tx_data`: it must have the
    bytes. So `tx_data` stays. The evaluation of option B proposed to drop `tx_heads`. The user
    keeps it in scope. Section 16.1 asks which reader uses it. Until the answer, PR 11 builds the
    record, the stream and the publisher only.
- The ingress `tx_data` recorders, the recorder barrier and `--archive-durability` go
  (`crates/ingress/src/bin/kardamom-ingress/main.rs:111-117`, `:341-361`, `:404-413`). No reader
  of the archives is left.

### 6.6 The ingress size cap

- Today the only limit on the size of a transaction is the 1 MiB frame
  (`crates/ingress/src/binary.rs:38`). The gas cap of 16,777,216 (`crates/types/src/limits.rs:14`)
  allows about 1 MB of calldata.
- The ingress refuses a transaction whose `raw_tx` is above 128 KiB: `--max-tx-bytes`, default
  131,072. That is the order of geth's default of 128 KB. The refusal is a JSON-RPC error that
  states the size and the cap. `kardamom_ingress_rejected_total{reason="too_large"}` counts it.
- The cap bounds each log entry at 128 KiB + 171 bytes, and each entry in the sequencer buffers.
- **Follow-up (section 16.3).** A stricter rule for a large transaction from a sender whose
  balance is unknown: for example a refusal above 4 KB, or a balance lookup before the admission.
  Also a byte rate for each sender at the ingress, beside the per-IP bucket
  (`crates/ingress/src/rate_limit.rs`). An unfunded new key can still put ordered junk into the
  log (section 15).

## 7. The executor input: the member feed

### 7.1 The publication

| Item | Value |
|---|---|
| Topic | `sealer_feed`, a new `Topic` variant (`crates/log/src/discovery/record.rs:24-36`). |
| Stream id | 1006 for the feed, 1007 for the feed control. Both are free (`crates/log/src/config/mod.rs:503-545`). |
| Channel | `aeron:ipc` on the media driver that member `i` and executor `i` share (section 7.6). There is no UDP feed: a member feeds only its co-located executor (decision 7). |
| Publisher | One exclusive publication on each member, owned by a feeder thread in the member JVM. |
| Subscriber | The paired executor only. |
| Frames | The egress frames: kind 1 (relayed), kind 2 (boundary), and the replay answers 3, 4, 11. The feed adds kind 16 (`FEED_GAP`). On the feed, the answers carry the request id as a tail (section 7.8). One codec for the feed and the egress. |
| No status | The feed carries no kind 9. The status is not retained, so it has no place in a replay, and the executor does not read it. The boundary of each tick shows that the feed is alive. |

- The service thread runs on every member, the leader and the followers alike. Each member applies
  the same log, so each member's feed carries the same frames (R3).
- An egress offer is a no-op on a follower (`cluster/sealer-service/README.md:163`). The feed is
  not an egress offer. It is a member-local publication, so a follower publishes it too.

### 7.2 The service thread never blocks

- The service thread writes each frame into a one-to-one ring buffer (Agrona
  `OneToOneRingBuffer`), at the point where it emits the frame (`SealerEgress.retain`, #632).
  The write never waits. A member retains no frame (section 5.5): the hook stays, and the store
  behind it goes.
- The feeder thread reads the ring and offers each frame to the publication. A back-pressured
  offer makes the feeder thread wait. The service thread does not wait (R4).
- **Bound.** `kardamom.cluster.feedRingBytes`, member-local. Default: 16 MiB, which is about
  67,000 transfer frames or 2,000 frames of 8 KB. The sealer job has 1 GiB with a 384 MiB heap,
  and the Aeron tmpfs counts against it, so the ring is a quarter of the first proposal (64 MiB).
  The feed term (section 7.8) holds more frames beside it.
- **Ring full.** The service writes a live frame only while the free space holds the frame plus a
  128-byte reserve. When it does not, the service writes one gap marker into the reserve and marks
  the feed `gapped`:
  - `FEED_GAP`: `[kind=16][next_index:u64][next_block:u64]`, 17 bytes, the first index and block
    that the feed did not carry. Kind 16 is a feed-only kind. The cluster egress never sends it.
  - The executor gets the marker after the frames before it, and sends a replay request from its
    cursor at once. It does not wait for the 6 s silence of section 7.5, and it does not switch
    to the serving service.
  - A `gapped` feed writes no live frame until the replay of section 7.3 reaches the head.
- **No subscriber.** When the publication is not connected, the feeder drops the ring and marks
  the feed `idle`. The service writes no live frame to an idle feed. The executor starts each
  connection with a replay request.

### 7.3 The catch-up path: a walk of the Raft log

- The executor sends a replay request on the feed control stream (1007):
  `[kind=1][from_index:u64][from_block:u64][request_id:u64]`, the layout of ingress kind 1 with
  the request id as a tail (section 7.8).
- The feeder thread passes the request to the service thread through a second one-to-one ring.
  The service thread polls it in `doBackgroundWork`. The request is member-local: it is not log
  input, and it changes no replicated state.
- The service checks the cursor with the egress checks of today (`ReplayVerdict`, #632):
  - a cursor past the head: `REPLAY_AHEAD`;
  - a cursor below the feed floor (below): `REPLAY_UNAVAILABLE`;
  - else it starts a walk of its own Raft log from a snapshot, then `REPLAY_DONE`.

**Why a walk, and not a read of the log.** The log holds the inputs: both twin copies, the
refused records, the timer events and the session events. It does not hold the canonical frames.
A canonical frame exists only after the state machine orders its record. So the walk runs the
state machine again over the log, from a snapshot, as a member does when it restarts. The frames
of the walk are a function of the committed log only, so they equal the live frames byte for byte
(R1, R3).

**The walk (decision 10, replaces the walk of the retained file).**

1. **Pick the start.** The member picks the newest snapshot of its own recording log whose
   canonical position is at or below the cursor: its canonical count at or below `from_index`,
   and its block at or below `from_block`. The purge marks (`PurgeView.Mark`) already carry the
   log position and the block of each snapshot. They also get the canonical count.
2. **Load it.** The walk thread replays that snapshot from the member's local archive and loads
   it into a private state machine: a second `CanonicalSealerState` and a frame builder with no
   session, no feed and no timer. `SnapshotIo.readServicePayload` already reads a snapshot from
   an archive image.
3. **Replay the log.** The walk thread opens an archive replay of the member's log recording from
   the log position of the snapshot. It decodes each log message (a session message, a timer
   event, a session open or close, a new leadership term) and applies it through the same
   dispatch as the clustered service. The clock is the timestamp of the message. A timer re-arm
   and every answer to a session are no-ops in the walk.
4. **Map to feed frames.** The walk keeps only the kind-1 and kind-2 frames that the dispatch
   emits. It drops each kind-1 frame with `index < from_index` and each kind-2 frame with
   `block_number < from_block`. It writes the rest into the feed ring, in emission order. When
   the walk reaches the cursor, it checks that the cursor names one point of the stream (the
   rule of 7.8). Else it ends the answer with `REPLAY_UNAVAILABLE`.
5. **Reach the head.** The walk never reads past the log position that the service has applied.
   The service publishes that position after each message. When the walk reaches it, the walk
   stops and marks itself done. On its next duty cycle, the service thread checks that the walk
   is at its last applied position. If so, it writes `REPLAY_DONE` and turns the feed `live`.
   Else the walk goes on. The walk reads the log faster than the cluster writes it, so it
   reaches the head.

- **One code path (R14).** The dispatch of the log messages moves out of
  `SealerClusteredService` into one class. The clustered service and the walk both use it. The
  clock and the timer re-arm are its inputs.
- **One writer of the ring (R5).** While the feed is `walking`, the walk thread is the only writer
  of the feed ring. While the feed is `live`, the service thread is the only writer. The state of
  the feed is a volatile field, and it hands the ring from one writer to the other. A full ring
  pauses the walk. It does not gap the feed. The service writes no live frame during the walk:
  the log already holds them, so the walk reaches them.
- **The newest request wins.** A new request ends the running walk at once, and starts a new one.
- **The feed floor.** The canonical position of the oldest snapshot that the member holds with
  its whole log after it: the purge point. The guaranteed window is from the newest snapshot. In
  practice it is at least the `keepSnapshots` newest intervals (15 minutes at the defaults), and
  more while the stored head holds the purge back (section 5.5).
- **The purge overtakes a walk.** The purge can delete the log under a walk that started from the
  oldest snapshot. The walk then ends the answer with `FEED_GAP` at the last frame that it wrote.
  The executor asks again from its new cursor. That cursor is higher, so a newer snapshot can
  serve it, or the executor gets `REPLAY_UNAVAILABLE`.
- **The fallback.** `REPLAY_UNAVAILABLE` sends the executor to the serving service, then to a peer
  checkpoint, as the reader of #635 does (section 7.5,
  `crates/engine/src/bin_support.rs:529-576`).
- **The cost.** A walk reads the log from the snapshot: about twice the frame bytes, because of
  the twin copies. It runs the state machine on the walk thread, on another core of the member's
  cpuset, and it holds a second copy of the replicated state on the heap. The snapshot size bounds
  that copy. The benchmark measures the walk rate and the heap (section 10.6).
- **A member that restarts.** It replays its own log from its snapshot before it serves a walk. Its
  feed is `idle` meanwhile. The executor reads the serving service until the feed answers.

### 7.4 Executor restart and resume

1. The executor reads its resume point from the state DB, as today
   (`crates/engine/src/actor/types.rs:96-109`).
2. It subscribes to the feed and sends a replay request from that cursor.
3. It drops each frame below its cursor and keeps a bounded pending set above a gap, with the
   ingest logic of today's cluster reader (`crates/engine/src/reader/cluster/mod.rs:236-422`,
   `MAX_PENDING` at `:61`). The feed is one more transport under the same reader.
4. It executes each `RT_TX` entry in order. No join, no refetch, no vote.

A member that restarts replays its own log from its snapshot. Its feed is `idle` until its
executor asks again. The executor follows the serving service meanwhile (section 7.5).

### 7.5 Switching to a remote member

The executor reads one source at a time, in this order:

| Order | Source | Use it while |
|---|---|---|
| 1 | The feed of its paired member | The feed delivers a frame or a status at least every 3 boundary ticks (6 s). |
| 2 | The serving service (section 8.1) | The feed is silent, or it answered `REPLAY_UNAVAILABLE` and the service's floor is lower. |
| 3 | A peer checkpoint | Every source answered `REPLAY_UNAVAILABLE` (`crates/engine/src/bin_support.rs:529-576`). |

- The switch happens at a frame boundary. The executor closes the old source and asks the new one
  for a replay from its cursor. R3 makes the frames at each index equal.
- The executor goes back to its feed when both hold:
  - the member admin endpoint answered ready (`cluster/sealer-service/README.md:423-445`) for
    30 s without a break;
  - the feed's replay from the executor's cursor reached `REPLAY_DONE`, which is the head.
  Until then the executor stays on the serving service. One not-ready answer restarts the 30 s.
- `kardamom.cluster.readyLagBytes` (`cluster/sealer-service/README.md:362`) scales with the log
  bytes. The default 4 MiB is about 500 log entries of 8 KB, so a busy follower would flap. The
  new default is 64 MiB, and the setting gets a hysteresis: a member turns not ready above the
  value and ready again below half of it.
- `kardamom_executor_input_source{source="feed|service"}` shows the source. An alert fires when an
  executor reads the serving service for more than 10 minutes.

### 7.6 Pairing, placement and the shared media driver

- Member `i` feeds executor `i`. Both counts are 3 today
  (`deploy/cluster/ansible/group_vars/all.yml:235-236`).
- Staging already puts sealer member `i` and executor `i` on one dedicated host, in two node
  containers with separate cpusets (`kardamom-infra`,
  `ansible/inventories/staging/hosts.ini:57-66`). The user chose IPC through one media driver
  for the pair.

**Today.** Each node container has its own `/opt/kardamom`, a host directory of that node
(`kardamom-infra`, `ansible/roles/node_containers/tasks/main.yml:93-97`). The executor node runs
the shared `ArchivingMediaDriver` of the aeron system job on `/opt/kardamom/aeron-mount/dir`
(`deploy/cluster/nomad/aeron.system.nomad.hcl:12-20`, `:201-210`). A sealer node runs no shared
driver: the member boots its own `ClusteredMediaDriver` on a private
`/opt/kardamom/aeron-mount/cluster-dir` (`deploy/cluster/nomad/aeron.system.nomad.hcl:124-138`).

**The shared driver.**

| Item | Rule |
|---|---|
| Owner | The aeron system job on the executor node. It owns the driver of the pair. The sealer member is only a client of it. |
| Directory | A host tmpfs `/run/kardamom/pair-<i>/aeron` on the dedicated host. The node_containers role mounts it into both node containers at `/opt/kardamom/pair-aeron`. |
| Executor node | The aeron system job sets `AERON_DIR=/opt/kardamom/pair-aeron/dir` on a paired node. The executor and every other service on the node use it, as they use `aeron-mount/dir` today. |
| Sealer node | The cluster job binds `/opt/kardamom/pair-aeron` and passes `-Dkardamom.cluster.feedAeronDir=/opt/kardamom/pair-aeron/dir`. The member's own `ClusteredMediaDriver` stays on its private `cluster-dir`. Raft never uses the shared driver. |
| Feed client | The feeder thread opens its own Aeron client on `feedAeronDir`. Its error handler closes only that client. It never ends the member JVM. |
| Memory | The feed log lives in the shared driver. Its channel is `sparse=false`, so the driver writes every page of the 3 terms when it creates the log (Aeron 1.44 `MappedRawLog`), and the kernel charges the pages to the cgroup of the driver: the aeron system job on the executor node, not the sealer. The control log is the executor's publication, in the same driver. The sealer pays only for the two rings, off its heap (section 10.5). |
| Liveness | Aeron clients find the driver through the CnC file and its heartbeat, not through a process id. Two containers on one kernel that map the same tmpfs file see one driver. |
| Unpaired node | A node without a pair keeps `aeron-mount/dir`. `feedAeronDir` empty means the member opens no feed. |

**Start order.** None is required.

- The member starts without the driver. The feeder retries the client every 1 s and keeps the
  feed `idle` meanwhile. The service thread does not wait for it.
- The executor starts without the member. It reads the serving service until the feed answers
  (section 7.5).

**Restarts.**

| Restart | Effect | Recovery |
|---|---|---|
| The executor node container, or its aeron system job | The shared driver goes. The feeder's client sees the driver timeout and closes. The feed is `idle`. Raft does not notice. | The new driver writes a new CnC file. The feeder opens a new client within 1 s of it, then a new publication. The executor starts with a replay request. |
| The executor process only | The IPC publication loses its subscriber. The feeder drops the ring (`idle`, section 7.2). | The executor restarts and sends a replay request from its state DB cursor. |
| The sealer node container or the member | The feed publication goes. The executor sees the image close. | The executor switches to the serving service at once, not after 6 s. It returns to the feed under the rule of section 7.5. |
| The dedicated host | The member and the executor go together. The tmpfs goes. | The cluster keeps its quorum on the other two hosts. Both restart; the member rejoins from its log or a peer seed. |

- The local profile (`terraform/containers`) gets the same pair mount, so the chaos shards test the
  IPC feed. A host that cannot share a directory has no feed. Its executor reads the serving
  service.
- The leader's CPU runs beside an executor on one host. The cpusets keep them on separate cores.
  The cluster shape warns that co-location moves the latency knee
  (`docs/specs/2026-08-16-10-ggas-cluster-shape.md`, "What breaks first"). The benchmark measures
  the leader CPU on the paired host.

### 7.7 Executors without a paired member

- An elastic executor, or an executor whose index has no member, reads the serving service
  (section 8.1). It costs the leader nothing.
- A paired executor whose member is down also reads the serving service (section 7.5).

### 7.8 The feed wire layout

This section is the contract between the member (the feeder) and the reader of the executor.
All integers are little-endian. Offsets are in bytes from the start of the frame.

**The feed stream (1006, member to executor).**

- Channel: `aeron:ipc` on `feedAeronDir`, stream 1006. The member owns one exclusive
  publication with `term-length=8m|sparse=false`. One Aeron message is one frame. The largest
  frame is below 1 MiB, the term divided by 8, so the reader polls through a fragment assembler.
- The frames are egress frames, with one codec (`EgressFrames`):

| Kind | Name | Layout (offset: field) | Length |
|---|---|---|---|
| 1 | Relayed record | 0: `kind=1`; 1: `index:u64`; 9: `payload_len:u32`; 13: `payload` | 13 + `payload_len` |
| 2 | Boundary | 0: `kind=2`; 1: `block_number:u64`; 9: `end_tx_idx:u64`; 17: `l2_timestamp:u64`; 25: `l1_origin:u64` | 33 |
| 3 | `REPLAY_UNAVAILABLE` | 0: `kind=3`; 1: `oldest_index:u64`; 9: `oldest_block:u64`; 17: `request_id:u64` | 25 |
| 4 | `REPLAY_DONE` | 0: `kind=4`; 1: `up_to_index:u64`; 9: `up_to_block:u64`; 17: `request_id:u64` | 25 |
| 11 | `REPLAY_AHEAD` | 0: `kind=11`; 1: `head_index:u64`; 9: `head_block:u64`; 17: `request_id:u64` | 25 |
| 16 | `FEED_GAP` | 0: `kind=16`; 1: `next_index:u64`; 9: `next_block:u64`; 17: `request_id:u64` | 25 |

- Kinds 1 and 2 are the canonical frames. Their bytes are equal on every member (R3), and equal
  to the bytes of the cluster egress for record types other than 6.
- Kinds 3, 4 and 11 on the feed are the egress frames with the tail `request_id`. The egress
  reader (`EgressItem::decode`) reads the fields that it knows and ignores the tail, so one codec
  reads both. The cluster egress sends these kinds without the tail.
- The feed carries no other kind: no status (kind 9, section 7.1), no reject, no kind 15. A
  reader drops a kind that it does not know.

**The control stream (1007, executor to member).**

- Channel: `aeron:ipc` on the same driver, stream 1007. The executor owns the publication. A
  64 KiB term is enough. The member subscribes.
- One message, the replay request: 0: `kind=1`; 1: `from_index:u64`; 9: `from_block:u64`;
  17: `request_id:u64`. Length 25. It is the layout of ingress kind 1 with the id as a tail.
- The member reads the first 25 bytes and ignores a longer tail. It drops a message that is
  shorter, or of another kind, and logs `sealer FEED ... DROPPED`.
- The reader picks the ids. Each new request gets a new id. The member does not check the order
  of the ids: it answers the newest request that it read.

**The answer rules.**

1. The member answers each request that it reads, also a request that comes before the feed has
   a subscriber, for example while the image of the reader appears. The frames of the answer wait
   in the ring until the subscriber connects. The ring bounds that wait: a full ring pauses the
   walk.
2. The newest request wins. A request ends the answer to every earlier request at once. The
   member discards each frame of an earlier answer that it did not yet publish.
3. Each answer frame (kinds 3, 4, 11, 16) carries the id of its request. The reader drops an
   answer frame with an id other than the id of its newest request.
4. Frames of an earlier answer that the member already published can still arrive after a new
   request, before the first frame of the new answer. They are canonical frames of the same
   stream: the reader drops each one below its cursor, and applies the next one at its cursor.
5. The reader resends its request, with a new id, when no frame arrives within 2 s of the
   request. A first request can reach a member whose feeder has no client yet, or whose
   subscription is not yet connected.

**The states of a feed.**

| State | The member writes | It leaves the state |
|---|---|---|
| `idle` | nothing | on a replay request |
| `walking` | the answer: the frames from the cursor, from the walk of the Raft log (section 7.3) | at the head, after `REPLAY_DONE`: `live` |
| `live` | each new canonical frame, when the service emits it | when a frame does not fit the ring: `gapped`; on a replay request |
| `gapped` | nothing | on a replay request |

- A feed starts `idle`. It turns `idle` again when the publication loses its subscriber, and
  after a `REPLAY_AHEAD` or a `REPLAY_UNAVAILABLE`.
- **The answer to a request.** The checks and their order are those of egress kind 1:
  1. A cursor past the head: `REPLAY_AHEAD` with the head. The feed turns `idle`.
  2. A cursor that does not name one point of the stream, or a cursor below the feed floor:
     `REPLAY_UNAVAILABLE` with the floors. The feed turns `idle`.
  3. Else each canonical frame with `index >= from_index` (kind 1) or
     `block_number >= from_block` (kind 2), in emission order, then `REPLAY_DONE`, then the
     live frames. `up_to` is the head when the walk reaches it: the canonical count and the open
     block. No live frame comes before `REPLAY_DONE`.
- **`FEED_GAP`.** The member writes it once, as the last frame of the answer, when a live frame
  does not fit the ring. It also writes it when the purge overtakes the walk. The feed is
  then `gapped`.
  - `(next_index, next_block)` is the cursor after the last frame that the answer carried:
    - after a boundary: `next_index` is its `end_tx_idx`, and `next_block` is its
      `block_number + 1`;
    - after a relayed frame: `next_index` is its `index + 1`, and `next_block` stays;
    - when the answer carried no frame: `(from_index, from_block)` of the request.
  - A replay request from `(next_index, next_block)` is valid, and it carries every frame that
    the feed did not carry. `next_index` is a lower bound: after an origin record, the next
    relayed index can be higher (see the index rule below).
  - The reader sends a request from its own cursor at once. It does not wait for the silence
    of section 7.5.
- **No subscriber.** The member discards the unsent frames and turns `idle`. The reader sends a
  replay request at the start of each connection.

**The index is not always contiguous.** A relayed frame takes the slots of its record, and the
frame does not state the count. The reader computes it from the payload:

| Record type | Slots |
|---|---|
| 2, epoch | `epoch_slots`: 1 for the marker, plus 1 for each deposit (`crates/cluster-adapter/src/wire/mod.rs`) |
| 3, remote epoch | `remote_epoch_slots`: `2 + last_seq - first_seq` |
| any other | 1 |

- The next index after a relayed frame is its `index` plus its slots. A relayed frame whose
  index is not that next index is a hole. A jump of more than one after an origin record is not
  a hole.
- A boundary closes the block: its `end_tx_idx` equals the next index, and its `block_number`
  is one above the block of the boundary before it. A boundary that breaks either rule is a hole.
- On a hole the reader does not apply the frame. It sends a replay request from its cursor.

**Bounds of the member.**

| Item | Value |
|---|---|
| Ring | `kardamom.cluster.feedRingBytes`, default 16 MiB, a power of two of at least 8 MiB. Off the heap, in the sealer JVM. |
| Reserve | 128 bytes of the ring, for the one `FEED_GAP`: its record is 40 bytes. A write that wraps takes a padding record of up to 32 bytes at the end of the ring, and the record plus 8 bytes (48) at its start: 80 bytes at most. A frame goes in only while the free space holds the frame twice (for the padding at the end of the ring) plus the reserve. |
| Walk | One walk thread on each member, member-local. It runs the state machine over the log from a snapshot (section 7.3). It holds one more copy of the replicated state while it runs. A full ring pauses the walk; it does not gap the feed. The service thread only checks the hand-off at the head. |
| Feed term | 8 MiB, 3 terms: 24 MiB in the shared driver. The largest frame is below one session message (1 MiB). |
| Control ring | 64 KiB, off the heap. |
| Client retry | 1 s, while `feedAeronDir` has no driver. |

## 8. The consumers

| Consumer | Reads today | Reads after |
|---|---|---|
| Executor `0..2` | Cluster egress (references) + 8 `tx_data` lanes + ingress archives; votes (`crates/executor/src/bin/kardamom-executor/main.rs:494-513`, `:339`) | Its member's feed; the serving service as the fallback. No `tx_data`, no archive, no vote. It keeps its output (the frames, the receipts, the index) only until the serving service stores it (section 8.3). It checks `keccak256(raw_tx) == canonical_id` for each `RT_TX` entry (about 1 µs for a transfer). On a mismatch it does not execute the entry and does not halt: it drops that source, replays from the other source at the same index, counts `kardamom_executor_source_mismatch_total{source}`, and an alert fires. |
| Elastic executor | As above | The serving service (section 8.1). |
| Validator | Cluster egress + `tx_data` + ingress archives; votes (`crates/validator/src/bin/kardamom-validator/wiring/startup.rs:241-286`, `wiring/run.rs:170-178`) | The serving service (section 8.1). It checks `keccak256(raw_tx) == canonical_id` and that the signature recovers the sender (`crates/exec-core/src/stateless.rs:253-272`). A failed check halts it with `validator_divergence`. It does not trust the sealer's bytes. Its attester reads the receipts above the attested head from the serving service after a restart (section 8.3). |
| Batcher | Cluster egress + `tx_data` + ingress archives; votes; the rebuild from `kardamom_getBlockRefs` and the archives (`crates/batcher/src/live/run.rs:292-357`, `:641-669`) | The serving service (section 8.1). It packs the KAR1 blocks from the `raw_tx` of each `RT_TX` entry. The spool, its group and cursor rules do not change. It keeps a publisher-only cluster session for the posted cursor (kind 7). |
| Ingress observer | Full relayed frames (`crates/ingress/src/cluster.rs:173-174`) | The cluster egress: kind-15 frames, no bytes (section 6.4). |
| Ingress receipt fallback | The receipt cache, then the executor state DB over HTTP (`crates/ingress/src/proxy/mod.rs:451-475`) | The cache, then an executor, which holds only the receipts above the stored head, then the serving service (section 8.4). |
| State mirror | `tx_receipts` only (`crates/state-mirror/src/main.rs:137-146`) | Unchanged. |
| Notifier | `tx_status`, `tx_receipts`, `tx_errors` (`crates/notifier/src/taps.rs:56-74`) | Removed for now (decision 12). A separate PR removes the code. |
| Sequencer | `tx_data`; publisher-only session (`crates/sequencer/src/outbound/cluster.rs:121-172`) | `tx_data`, as today. It offers the full entry (section 6.1). |
| da-watcher, L1 follower | `l1_blocks`, `tx_deposits` | Unchanged. |

### 8.1 Out of scope: the external serving service (separate design)

The members serve no consumer of the bytes except the co-located executor, over IPC (decision 7).
Neither the leader nor a follower serves the validator, the batcher, unpaired executors or
external nodes. A separate service serves them. It has its own design (#585). The user also
calls it the egress service. This spec states only what it needs from that service:

- **The stream.** The canonical stream in canonical order: kind 1 (all record types, with the
  envelope for record type 6) and kind 2 (boundary). The frames are byte for byte the frames of
  a member feed (R3), so a consumer uses one codec for both. The service reads no sealer status,
  so its stream carries no kind 9. None of its readers needs one: the batcher counts its sealed
  head from its own blocks (`crates/batcher/src/live/feed.rs:95-98`).
- **The replay.** A replay from a cursor `(index, block)`, with the answers of today: kinds 3, 4
  and 11 (`SealerEgress.java:201-253`).
- **What it keeps (decision 10).** An append-only log of the canonical frames, and the receipts
  of each block, on its own disk. It keeps every block above `min(posted_head, attested_head)`,
  for the two readers that need a long window:
  - the batcher, which posts to L1 (section 8.2);
  - the withdrawal attester in the validator, which posts output roots to L1 (section 8.3).
  The attested head is the newest block of an output root that is not deleted in
  `WithdrawalOutputOracle` (`crates/validator/src/attester/oracle.rs`). With no oracle, the floor
  is the posted head. Open question 16.1-8 asks if the floor also waits for the challenge
  window. The service reads both heads from L1. It can keep a longer window for other external
  readers: the public history RPC, indexers and explorers (section 8.4). Its design sets that
  window.
- **The rule "keep it, do not halt".** The service keeps the data until L1 holds it. No byte
  budget stops the chain (decision 5). Its disk grows with the DA lag (section 10.0).
- **What it reads (decision 10).** The output of the executors only, not the members (section
  8.3). It opens one publisher-only cluster session, as the sequencer and the batcher do, only to
  send its stored head (ingress kind 11, section 5.5). It reads no cluster egress, and no member
  opens a feed for it.
- **The stored head.** It sends the newest block that it holds on its durable store to every
  executor and to the sealer (section 8.3).
- **The cold tier (decision 11).** It is the cold tier of section 8.4: horizontally scaled read
  replicas of one append-only log, with one writer that reads the executors. It also answers the
  receipt lookup of the ingress for a receipt that the executors pruned.
- **The order of work.** The validator and the batcher read it after the cut-over, so the service
  runs before PR 13 deploys.

### 8.2 The batcher's resume sources

1. The spool: unchanged.
2. The replay from the cursor, now from the serving service. It holds every frame above the
   posted head (section 8.1). The members do not keep those frames: a member keeps its log only
   above `max(posted_head, stored_head)` (section 5.5), and an executor keeps its output only
   above the stored head (section 8.3).
3. The rebuild from `kardamom_getBlockRefs` and the `tx_data` archives retires. Its source of
   bytes is gone, and the replay of step 2 already holds the bytes of every unposted block.
   - A range below the floor exists only after every member lost its state. The chain then
     reverts to the posted head (`docs/runbooks/revert_to_posted_head.md`), as today.
   - `kardamom_getBlockRefs` stays for the list of reverted transactions in
     `docs/runbooks/sealer-fleet-rebuild.md:66-79`. Its archive fields are zero for new blocks.
     The state DB schema does not change (`crates/state/src/meta.rs:64`).
   - The rebuild mark (`KEY_L1_REBUILT_END_TX_POSITION`, `crates/state/src/meta.rs:27-36`,
     checked in `crates/reconstruct/src/seed.rs:175-178`) does not change.
- `--block-refs-source`, `ArchiveRebuilder` and `RefsStore` go
  (`crates/batcher/src/live/rebuild.rs`, `crates/batcher/src/live/refs_store.rs`).
- The snapshot set of `docs/specs/2026-10-03-l1-outage-recovery-chaos.md` section 3.9 drops the
  `tx_data` archives. The serving service holds the bytes of every unposted block. The sealer's
  Raft log holds them only above the stored head.
- The batcher keeps its cluster session as a publisher only, for the posted cursor (ingress
  kind 7, `crates/batcher/src/live/posted_cursor.rs`). The DA-lag guard of the sealer needs it.
  It reads no cluster egress.
- Unaffected: `l1_blocks` and the L1 follower, the DA-lag flush of the batcher (#569), the
  batcher's resume from the contract, its spool and cursor formats.

### 8.3 The executor output, the handoff and the stored head

Decision 10 makes the serving service the durable store of the canonical stream above the
posted head. The executor is a short buffer in front of it. This section states the executor side
of the handoff. The serving service side is part of its design (#585).

**What the executor keeps.** Execution needs the state, so the state stays. Only the output side
becomes a buffer. The state DB has 11 tables (`crates/state/src/schema.rs:51-63`). Each block
writes all of them in one transaction (`crates/state/src/writer/apply.rs:302-324`).

| Table | Today | After |
|---|---|---|
| `accounts`, `storage`, `code` | state; kept for ever | kept: execution reads them |
| `meta` | the resume cursors, the schema version, the rebuild mark (`crates/state/src/meta.rs:8-60`) | kept. New key `output_floor_block`: the newest block whose output the executor pruned. |
| `account_trie`, `storage_trie`, `hashed_accounts`, `hashed_storage` | genesis rows only on the executor (the trie is off, `crates/state/src/writer/mod.rs:186-188`) | kept, unchanged |
| `headers` | one 48-byte row for each block; execution reads only the last (`crates/state/src/recovery/mod.rs:75-93`) | kept. A row is 48 bytes, about 2 MB a day at 2 s blocks, and the integrity sweep needs the rows from block 0 with no gap (`crates/state/src/integrity/checks.rs:182-216`). |
| `receipts` | output; kept for ever; no execution path reads it | **pruned** at or below the stored head |
| `tx_hash_index` | output; kept for ever | **pruned** at or below the stored head, with its receipts |
| `block_frames` (new) | none | **the buffer**: key the block number, value the canonical frames of the block (its kind-1 frames and its kind-2 boundary), byte for byte as the feed delivered them. Written in the same transaction as the receipts. Pruned at or below the stored head. |

- The state DB holds no transaction bytes today: no block body, no transaction list. A block's
  receipts are the range between two header ends (`crates/state/src/snapshot.rs:155-180`). So the
  serving service cannot get the bytes from the tables of today. `block_frames` holds them.
- **Checkpoints.** A checkpoint is a copy of the whole MDBX image
  (`crates/state/src/checkpoint/mod.rs:172-203`). After a prune it holds only the buffer, so it is
  smaller. An executor that restores an old checkpoint executes again from its cursor, writes the
  output again, and prunes the part at or below the stored head at once.
- **Rebuild from L1.** `crates/reconstruct` writes the receipts and the index as a side effect,
  and never reads them. It writes no `block_frames`: L1 does not hold the envelope fields. Every
  rebuilt block is at or below the posted head, so the serving service does not need its frames.
  The next prune drops the receipts and the index.
- **Checks.** The integrity sweep keeps its 1:1 rule between `receipts` and `tx_hash_index`
  (`crates/state/src/integrity/checks.rs:241-246`): both are pruned to one floor. `deep_compare`
  and the persisted-state stage of chaos (`crates/chaos/src/stages/state.rs`) compare the output
  above the highest `output_floor_block` of the compared DBs.

**What the serving service reads (proposal, open question 16.1-4).** The smallest option is a
new range read on the executor's state query server, the server that already answers
`eth_getTransactionReceipt` (`crates/state/src/nonce_query.rs:197-209`):

- The request: `[from_block:u64][stored_head:u64][max_bytes:u32][wait_ms:u32]`.
- The answer, for each committed block from `from_block`, up to `max_bytes`: the block number, the
  `block_frames` value, and the receipts of the block, in index order. With no committed block
  at `from_block`, the server waits up to `wait_ms` for one (a long poll).
- The body is binary, as the checkpoint server is (`crates/state/src/checkpoint_transfer.rs`),
  not JSON with hex.
- A block below `output_floor_block`: the answer says "pruned", with the floor.
- Why this option:
  - One source holds the frames and the receipts of a block, from one MDBX transaction. So the
    serving service never joins two streams, and never sees a block with half its output.
  - The state DB is the replay source. The executor needs no Aeron archive and no new stream.
  - The ack rides on the request. The executor needs no client of its own.
- The other options, and why they are larger:
  - The `tx_receipts` stream (1002) and its boundaries (1003). They are live only, with no
    archive and no replay (`crates/log/src/config/mod.rs:298`), and they carry no transaction
    bytes. The service would need a second source for the bytes and a join.
  - A new recorded Aeron stream from the executor, like `exec_txs` (1005). It needs an archive,
    its purge, and a replay protocol on each executor node, and the receipts still come apart.
  - A direct read of the executor's MDBX file. The service would have to run on the executor host,
    and MDBX has one writer process.

**The stored head.**

- **Definition.** The newest block `S` such that the serving service holds every frame and every
  receipt of each block up to `S` on its durable store. "Durable" is the number of copies that
  open question 16.1-5 decides (section 8.4).
- **How the executor learns it.** Each range read carries `stored_head`. The serving service sends
  its stored head to every executor, also to the executors that it does not read from: a range
  read with `max_bytes = 0` is the ack alone. The executor keeps the highest value that it got.
- **The prune.** After each block commit, the executor deletes the `receipts`, `tx_hash_index` and
  `block_frames` rows of the blocks at or below `min(stored_head, last_committed_block)`, in one
  write transaction, and sets `output_floor_block`. A prune is at most a few blocks of rows in
  steady state.
- **How the sealer learns it.** The serving service sends ingress kind 11 with the same value
  (section 5.5).

**When the serving service is down.** The stored head stops. The executor keeps all its output
above the last stored head. It does not stop or slow execution:

- No back-pressure. A slowed executor delays every receipt. The sealer keeps the log above the
  stored head anyway (section 5.5), so back-pressure saves no data. It would turn an outage of
  the serving service into an outage of the receipts, against decision 5.
- **The bound is the disk.** The output buffer grows by `rate x (frame + receipt + index)` bytes.
  A receipt and its index entry are about 0.3 KB for a transaction without logs
  (`crates/types/src/receipt.rs`). At 5,000 tx/s that is about 2.7 MB/s with transfers only,
  6.2 MB/s with the mix and 43 MB/s with 8 KB only: about 22 GB an hour at the mix
  (section 10.0).
- **Alerts.** `KardamomExecutorOutputUnstored`: the stored head has not moved for 10 minutes while
  the executor commits. `KardamomExecutorDiskLow`: free disk under 25 %, or under 6 hours at the
  current growth rate. At a full disk the commit fails and the executor stops, as an MDBX write
  error stops it today.

**More than one executor.**

- The serving service reads the frames and the receipts from one executor at a time, in a
  configured order. It switches to the next one when the source is down or answers "pruned"
  below its need, as the executor reader switches its own sources (section 7.5).
- It checks `keccak256(raw_tx) == canonical_id` for each `RT_TX` frame that it reads, as the
  executor does (#595). On a mismatch it does not store the block, and it reads that block again
  from the next executor.
- **Agreement (proposal, open question 16.1-7).** Before it stores a block, the service compares
  a digest of the block from a second executor: the keccak of the `block_frames` value and of the
  receipt bytes. The receipts are deterministic, and the chaos stage already compares them byte
  for byte across executors. The second read is the digest only, not the bytes.
  - The digests agree: the service stores the block.
  - They differ: the service stores neither, does not move the stored head, counts
    `kardamom_serving_executor_mismatch_total`, and an alert fires. It does not choose an
    executor. The validator is the judge: it re-executes and halts on a divergence.
  - The cost: the stored head follows the second-fastest executor. With one executor up, the
    stored head stops, and the sealer log and the executor buffer grow.
- **Why the check matters now.** The validator reads the canonical stream through the serving
  service, so through an executor. It checks each hash and signature, but it cannot tell an entry
  that the sealer ordered from a valid signed entry that a faulty executor added. Two executors
  that agree close most of that gap. The stream digest of decision 8 closes it.

**The readers of the executor output today, and after.**

| Reader | Reads today | Reads after |
|---|---|---|
| Ingress `eth_getTransactionReceipt` | The in-memory cache from `tx_receipts`, then one executor's state DB over HTTP (`crates/ingress/src/proxy/mod.rs:451-475`). The first HTTP answer wins (`crates/cache/src/query.rs:188-191`). | The cache, then an executor, which holds only the receipts above the stored head, then the serving service (decision 11, section 8.4). |
| Ingress `kardamom_subscribeReceipts`, the `eth_sendRawTransaction` wait, `eth_blockNumber`, the fee methods | The live `tx_receipts` and boundary streams (`crates/ingress/src/proxy/watchers.rs:27-33`, `:210-229`) | Unchanged. |
| Ingress resubmit check | The cache, then the Redis key `rcpt:<addr>:<nonce>`, 600 s (`crates/ingress/src/proxy/submit.rs:280-308`) | Unchanged. |
| State mirror and Redis | `tx_receipts`, written to Redis (`crates/state-mirror/src/mirror.rs:191-196`) | Unchanged. |
| Notifier | `tx_receipts`, `tx_status`, `tx_errors` (`crates/notifier/src/taps.rs:68-84`) | Removed for now (decision 12). |
| Sequencer | `tx_receipts` for the floors and the skip releases (`crates/sequencer/src/bin/kardamom-sequencer/feeds.rs:479-602`) | Unchanged. |
| Validator receipt check | The executor's receipts on `tx_receipts` (`crates/validator/src/buffers.rs:459-500`) | Unchanged. |
| Withdrawal attester (in the validator) | Its own re-executed receipts (`crates/validator/src/attester/sinks.rs:1-23`); the leaves live in memory until a post (`crates/validator/src/attester/state.rs:44-63`) | Unchanged for live blocks. **New need:** after a restart, the validator resumes from its own state cursor, which can be above the attested head (`crates/validator/src/attester/mod.rs:27-29`, `state.rs:106-119`). The attester then reads the withdrawal logs of the blocks between the attested head and that cursor from the receipts that the serving service keeps. This is why the serving service keeps the receipts above the attested head. |
| Batcher | The cluster egress, `tx_data`, and `kardamom_getBlockRefs` for a rebuild (`crates/batcher/src/live/refs_store.rs:174-200`) | The serving service (section 8.2). |
| `kardamom_getBlockRefs`, fleet-rebuild runbook | The executor or validator state DB (`crates/state/src/snapshot.rs:142-217`) | An executor answers only above its output floor. The runbook (`docs/runbooks/sealer-fleet-rebuild.md:66-79`) reads the validator's state DB, which is not pruned, or the serving service. |
| Canary `old_receipt` probe | The ingress RPC, so the executor state DB (`crates/canary/src/probes/read.rs:65-75`) | The ingress RPC, so the serving service for an old receipt (section 8.4). |
| Chaos `Check::ReceiptFromState` | The ingress after a restart, served from executor state (`crates/chaos/src/cases/combined/checks.rs:99-205`) | The check counts a receipt from the executor (`state_hit`) for a block above the stored head, and a new outcome for a receipt from the serving service. |
| Chaos persisted-state stage, e2e | The state DB files offline (`crates/chaos/src/stages/state.rs`); the ingress RPC (`crates/e2e/src/harness/l2.rs:122-126`) | The compare starts above the output floors. The e2e receipts are recent, so they come from the cache or an executor. |

### 8.4 The read paths: two tiers

The user decided on 2026-10-17 how the data is served (decision 11). There are two tiers. The
hot tier is the executor. The cold tier is the serving service.

| Tier | Source | Readers | What it holds |
|---|---|---|---|
| Hot | The executor: the live `tx_receipts` stream and its boundaries, and the state query server | Internal services: the ingress (the live receipt stream into its cache, then the executor lookup), the state mirror and Redis, the sequencer | The live stream, and the output above the stored head |
| Cold | The serving service: horizontally scaled read replicas of one append-only log | External services: the batcher, the attester (the L1 withdrawal proposer), the public history RPC, indexers and explorers, the validator, the unpaired executors, and the ingress fallback for a pruned receipt | Every block above `min(posted_head, attested_head)`, and the window of its design |

- **The hot tier gets no new hop.** The internal readers keep the sources of today. This is the
  path where the latency matters.
- **The batcher and the attester read only the cold tier.** The serving service holds every
  block that L1 does not hold yet. The executor holds only the blocks above the stored head, so it
  cannot serve their whole range.
- **The notifier (decision 12).** The user removed it for now: no WebSocket status feed and no
  webhooks. So it is in neither tier, and the serving service keeps nothing for it. A separate PR
  removes the code and `docs/tx-status-events.md`.
- **The validator.** It reads the canonical stream from the serving service, live and when it
  catches up (section 8). It reads nothing from an executor except the live `tx_receipts` for its
  receipt check, as today.
- **The ingress receipt lookup.** `eth_getTransactionReceipt` reads in this order:
  1. the in-memory cache, filled from the live `tx_receipts` stream;
  2. an executor, over the state query server, as today;
  3. new: the serving service, when the executor does not have the receipt.
  - A receipt in the cache is final: a block never changes after its commit. So no tier serves a
    stale receipt.
  - Today the lookup by hash does not read Redis (`crates/ingress/src/proxy/mod.rs:451-475`).
    Redis holds receipts by sender and nonce for 600 s, for the resubmit check only
    (`crates/ingress/src/proxy/submit.rs:280-308`). That path does not change.
  - An executor cannot tell a pruned receipt from a receipt that does not exist yet: the prune
    removes the index row too. So the ingress asks the serving service after each miss on the
    executors. Today the first HTTP answer wins (`crates/cache/src/query.rs:188-191`). After the
    change, a miss goes on to the serving service.
  - A client that polls for a pending transaction now causes a lookup on the serving service for
    each poll. The per-IP limiter of the ingress already sheds misses
    (`crates/ingress/src/proxy/mod.rs:461-463`). The serving service answers a miss from its own
    hash index. Its design sizes that load.
  - The serving service answers the same JSON-RPC method and the same receipt bytes as the
    executor's state query server (`crates/state/src/nonce_query.rs:197-209`). So the ingress
    keeps one client, with a second list of endpoints.
- **No gap between the tiers.** The executor prunes its output only at or below the stored head:
  the newest block that the serving service holds on its durable store (section 8.3). So each
  receipt, and each frame, is always in at least one tier. A lookup between the two reads can miss
  on the executor after a prune and find the receipt on the serving service.

**The serving service scales out as read replicas of one append-only log.** This spec states the
shape. The design of #585 settles the choices that the code does not settle (open questions
16.1-5 and 16.1-6):

- **One writer.** One writer node reads the executors (section 8.3) and appends each block to the
  log: the frames of the block and its receipts, in block order. It is the only node that reads
  the executors, sends the stored head and sends ingress kind 11.
- **The replicas follow the writer.** Each replica copies the log of the writer in order and
  builds its own indexes (the canonical index, the block, the transaction hash). A replica serves
  a block only after it holds the whole block. Each replica serves the replay of section 8.1 and
  the receipt lookup.
- **The stored head.** It is the newest block that the required number of copies hold on disk:
  the writer alone, or the writer and one or more replicas (open question 16.1-5). The writer
  learns the progress of each replica, and computes the stored head from it.
- **Writer failover.** When the writer stops, one replica becomes the writer and goes on from its
  own last block. The executors keep their output above the stored head, so the new writer reads
  the blocks that it lacks from an executor. No block is lost while the copies of the stored head
  survive.
- **The choices left to #585:**
  - how a replica copies the log: an Aeron stream from the writer with a replay from the
    writer's archive, a pull of the log files from the writer over HTTP (as the peer seed of the
    sealer copies files), or a shared store;
  - how a replica becomes the writer: a lock in Consul, a Nomad singleton job, or an operator
    step;
  - how many copies the stored head needs.

## 9. What goes and what stays

### 9.1 Removed

| Area | Item | Where |
|---|---|---|
| Sealer | `VoidLedger`, `onVoidRequest`, `appendVoid`, kind 6, `RT_VOID` | `VoidLedger.java`, `CanonicalSealerState.java:919-933`, `SealerClusteredService.java:1006-1023` |
| Sealer | `RecordedCursors`, kind 9, the record-lag guard, egress kind 13, the status tail | `RecordedCursors.java`, `CanonicalSealerState.java:1312-1343`, `SealerClusteredService.java:899-927`, `SealerEgress.java:469-495` |
| Sealer | Settings `voidVoters`, `voidWindow`, `recordLagBudget`, `MAX_OPEN_VOTES` | `ClusterNode.java:44`, `:94-101`, `:143-144`, `:608-612`, `:685-694` |
| Sealer (decision 10) | The retained file of #608: `RetainedFile`, `RetainedWalk`, `RetainedSection`, `RetainedCopy`, `RetainedEndpoint`, `RetainedToken`, `SegmentDir`, `SegmentPreallocator`, the `/retained` endpoint, `KARDAMOM_RETAINED_TOKEN`, `kardamom.cluster.retainedSegmentBytes`, the retained-file copy of the peer seed, the 76-byte status, the v12 retention section. `kardamom_sealer_disk_free_bytes` stays. | `cluster/sealer-service/service/src/main/java/io/kardamom/sealer/cluster/` (the new issue of section 13, PR 15) |
| Engine | The join buffer, `JoinWait`, `JoinRecovery`, the void park, `voter_id`, `void_wait`, `TxDataReader`, `VoidOfExecutedEntry`, `JoinTimeout` | `crates/engine/src/reader/join.rs`, `crates/engine/src/reader/void.rs`, `crates/engine/src/reader/threads.rs:77-131`, `:274-383`, `crates/engine/src/reader/ports.rs:29-155`, `crates/exec-core/src/error.rs:120-131` |
| Engine | `open_tx_data_subs`, `archive_join_recovery`, `bounded_join_timeout` | `crates/engine/src/bin_support.rs:131-253` |
| Executor | The `exec_stream` module, `--exec-cursor`, `--void-voter-id`, the refetch endpoints | `crates/executor/src/exec_stream/`, `crates/executor/src/bin/kardamom-executor/args.rs:79-91`, `:187-201` |
| Validator, batcher | `--void-voter-id`, the refetch endpoints, the batcher rebuild | `crates/validator/src/bin/kardamom-validator/args.rs:118-137`, `crates/batcher/src/bin/kardamom-batcher.rs:125-142`, `:180`, `crates/batcher/src/live/rebuild.rs`, `crates/batcher/src/live/refs_store.rs`, `crates/batcher/src/multi_archive_reader.rs:31-34` |
| Types | `ExecTxRecord`, `RecordLagStatus`, the `exec_txs` topic, stream 1005 | `crates/types/src/exec_record.rs`, `crates/types/src/cluster_status.rs:28-42`, `crates/log/src/discovery/record.rs:34`, `crates/log/src/config/mod.rs:405-411` |
| Types | `TxErrorReason::RecordLag`, `HaltCause::RecordLag`: never produced again. They stay in the enums as reserved variants, because the rkyv streams carry the discriminant and a variant is only added at the end (`crates/types/src/service.rs:49-51`). | `crates/types/src/tx_error.rs:94-98`, `crates/types/src/service.rs:53` |
| Log | `RecorderKind::ExecTxs`, the `tx_data` part of `refetch.rs`, `ExecTxs` handles | `crates/log/src/recorder.rs:186-200`, `crates/log/src/refetch.rs:251-300`, `crates/log/src/aeron_live/handles/simple.rs:325-344` |
| Ingress | The `tx_data` recorders, the barrier, `--archive-durability`, the local-fsync ack policies, `fsync_watermark` (1010), the record-lag gauges | `crates/ingress/src/bin/kardamom-ingress/recorders.rs`, `main.rs:111-117`, `:341-413`, `crates/ingress/src/metrics.rs:45-50`, `crates/log/src/config/mod.rs:730` |
| Sequencer | The record-lag refusal step | `crates/sequencer/src/bin/kardamom-sequencer/feeds/steps.rs:56-90`, `crates/sequencer/src/outbound/cluster.rs:129` |
| Deploy | `void_voters`, `exec_cursor`, `--void-voter-id` in three jobs, the refetch ports and endpoints, `archive_topics = exec_txs` and `tx_data`, the must-match entries | `deploy/cluster/nomad/cluster.nomad.hcl:117-126`, `:228`, `:379`, `deploy/cluster/nomad/executor.nomad.hcl:80-89`, `:161`, `:188-189`, `:230-251`, `deploy/cluster/nomad/validator.nomad.hcl:199`, `:246-267`, `deploy/cluster/nomad/batcher.nomad.hcl:213`, `:254-271`, `deploy/cluster/ansible/roles/nomad/templates/nomad.hcl.j2:80-89`, `deploy/cluster/ansible/roles/workloads/defaults/main.yml:150-152`, `:252-264`, `deploy/cluster/config/channels.toml.tpl:66-80` |
| Metrics, alerts | `kardamom_ingress_cluster_recorded_head`, `_record_lag`, `kardamom_executor_exec_stream_*`, the `record_lag` alert | `crates/ingress/src/metrics.rs:45-50`, `crates/executor/src/exec_stream/metrics.rs:7`, `deploy/alerts.yml:225-233` |
| Runbooks | `record_lag.md`; the void steps of `deploy-rollback.md`; the rebuild source of `replay_unavailable.md` | `docs/runbooks/record_lag.md`, `docs/runbooks/deploy-rollback.md:119`, `:150`, `docs/runbooks/replay_unavailable.md:18-27` |
| Docs | The void section, the voter table, the executor stream, the third batcher source, the ack window | `docs/failure-modes.md:302-349`, `:434-446`, `:709-729`, `:460-463`, `cluster/sealer-service/README.md:30-104`, `:236-257`, `docs/aeron-discovery.md` |
| Specs | `2026-10-06-executor-archived-tx-data.md` and `2026-09-21-void-unexecutable-entry.md` get a status line: superseded by this spec. | `docs/specs/` |

### 9.2 The merged work

| Pull request | Work | After this design |
|---|---|---|
| #521 (P1) | `exec_txs` topic, `ExecTxRecord`, stream 1005 | Retired. It was considered for the network fallback. The serving service of section 8.1 takes that role, with the frames of the feed. One codec for the feed and the service is less code (STYLE R14). |
| #531 (P2) | The executor publishes and records `exec_txs` | Retired. The pattern (an exclusive publication that one thread owns, the end of the recording is fatal) informs the feeder thread. |
| #519 (P3) | Recorded cursor, record-lag guard (off), the void on configured voters, the v11 reader | Retired. The v11 reader goes when `reads_min` rises to 12 (section 12). The rule "read N+1 one release before writing it" stays the method. |
| #540 (P4) | The executor sends its cursor | Retired. |
| #534 (P8) | The reject path, the halt, the runbook, the alert | Retired. The enum variants stay reserved (section 9.1). |
| #568 | Decision version | Kept as a mechanism. This change raises it to 3. |
| #561 | The sequencer frees a refused nonce | Kept. Each refusal of a full entry uses it. |
| #552 | Readers hardened for a rollback; unknown kinds drop | Kept. It makes the retired kinds safe. |
| #482, #501 | Posted-head floor, log purge | The posted-head floor of the heap retention stays while `carryBytes` is off. With it on, a member retains no frame, and the log purge floor is `max(posted_head, stored_head)` (section 5.5). |
| #608 (PR 5) | The retained file | Removed by decision 10 (PR 15). Kept: the disk-free gauge and the periodic snapshot of #611. |
| #632 (PR 7) | The member feed | Kept: the ring, the feeder, the control stream, the states, `FEED_GAP`. Its replay walk changes source: the Raft log, not the file (section 7.3, PR 16). |
| #481 | `kardamom_getBlockRefs` and the archive rebuild | The archive rebuild retires. The method stays for the fleet-rebuild runbook. |
| #507 | Archive sync level | Kept. The sealer archive and the `tx_deposits` and `l1_blocks` recordings use it. |
| #522 | Refetch from an older recording | Kept for `tx_deposits`. The `tx_data` use goes. |
| #584 | ANSI-stripped `Evidence::count_lines` (`crates/chaos/src/evidence.rs:51-59`) | Landed on its own. It does not depend on this design. |
| #543 (harness part) | One cleanup owner for the iptables rules | Retires with the `exec-peer-fetch` case of P5. That case is the only user of it, and it does not exist on `main`. |

Open work that this design supersedes, once the user confirms:

| Item | Title | Action |
|---|---|---|
| #543 (PR) | executor: fetch an entry from a peer's `exec_txs` archive before the void vote | Close as superseded. Its `count_lines` fix landed as #584. |
| #542 (PR) | batcher: read the executor stream behind `--tx-source` | Close as superseded. |
| #554 (PR) | validator: read the executor stream behind `--tx-source` | Close as superseded. |
| #570 (issue) | a run of lost entries makes every executor exit on each join timeout | Close as superseded: no entry is lost (R2). |
| #537 (issue) | `archive-tx-data-wipe`: a frame in flight at the driver kill is in no archive | The cut-over release (PR 13) closes it: no executor joins `tx_data`. |

**Ownership.** kardamom-3-00 co-owns the executor side once the user confirms: the feed reader,
the source switching, and the removal of the join, the refetch and the void in the engine (PR 10
and the engine part of PR 14).

## 10. Throughput and resources

### 10.0 The design target

The user set the target: **up to 5,000 tx/s, with mixed sizes**. On 2026-10-17 the user added a
latency target: **a transaction lands within 50 ms of its submit** (decision 13). The percentile,
and whether "lands" is the canonical seal or the receipt at the ingress, are open (16.1-10). The
mix below is an example for the arithmetic. The benchmark uses the same mix, plus a pure 8 KB row as the worst case.

- Example mix: 80 % transfers (110 B raw), 15 % 2 KB calls, 5 % 8 KB calls. The mean raw size is
  about 805 B. The log input is about 2.0 KB a transaction (two twin copies), and a canonical
  frame about 932 B.
- The serving service and the executor buffer store the frame plus the receipt and its index
  entry: about 0.3 KB more for each transaction without logs (section 8.3). That is about
  0.54 KB a transaction with transfers only, 1.23 KB with the mix, 2.48 KB with 2 KB only and
  8.62 KB with 8 KB only.

| At 5,000 tx/s | Transfers only | Example mix | 2 KB only | 8 KB only |
|---|---|---|---|---|
| Out of one sequencer replica (2,500 tx/s) | 0.72 MB/s | 2.5 MB/s | 5.7 MB/s | 21.5 MB/s |
| Log input at the leader (2 twin copies) | 2.9 MB/s | 10.1 MB/s | 23 MB/s | 86 MB/s |
| Replication out (2 followers) | 5.8 MB/s | 20.1 MB/s | 46 MB/s | 172 MB/s |
| Egress out (2 ingresses, kind-15 frames of 42 B) | 0.42 MB/s | 0.42 MB/s | 0.42 MB/s | 0.42 MB/s |
| Leader out, total | 6.2 MB/s | 20.5 MB/s | 46 MB/s | 173 MB/s (1.38 Gbit/s) |
| Log written in 15 minutes, each member | 2.6 GB | 9.0 GB | 21 GB | 78 GB |
| Stored bytes for each 2 s block (frames + receipts) | 5.4 MB | 12.3 MB | 25 MB | 86 MB |
| Disk growth while no post lands, each copy of the serving service | 2.7 MB/s | 6.2 MB/s | 12.4 MB/s | 43 MB/s |
| The same, in one hour | 9.7 GB | 22 GB | 45 GB | 155 GB |
| The same, at the block guard (10,000 blocks, about 5.6 hours) | 54 GB | 123 GB | 248 GB | 862 GB |
| Disk growth while no post lands, each member and each executor | none above the margin and the buffer | none | none | none |
| While the serving service stores nothing: each member (log) | 2.9 MB/s, 10.4 GB an hour | 10.1 MB/s, 36 GB an hour | 23 MB/s, 83 GB an hour | 86 MB/s, 310 GB an hour |
| While the serving service stores nothing: each executor (output buffer) | 2.7 MB/s, 9.7 GB an hour | 6.2 MB/s, 22 GB an hour | 12.4 MB/s, 45 GB an hour | 43 MB/s, 155 GB an hour |
| Raw bytes to post to DA | 0.55 MB/s | 4 MB/s | 10 MB/s | 41 MB/s |

Findings from the arithmetic, for the benchmark to confirm:

- **Network.** The leader sends only the replication and the 42-byte kind-15 frames. The mix
  needs about 20.5 MB/s out of the leader, inside a 1 Gbit/s link. A stream of only 8 KB calls
  needs 1.38 Gbit/s, above a 1 Gbit/s link: the worst case needs 10 GbE. The leader also receives
  86 MB/s at 8 KB only. The serving service is not in these numbers (section 8.1): its writer
  reads the frames and the receipts from one executor, about 6.2 MB/s at the mix, and a digest
  from a second one (section 8.3). It adds no load to the leader. Only the staging run can
  measure the network (section 10.6).
  - The evaluation of option B gives 30 MB/s and 2.05 Gbit/s. Those numbers include the full
    egress to the validator and the batcher. Decision 7 removes that egress.
  - Option A would need 12 MB/s and 0.71 Gbit/s: one copy in the log. The hedge of section 6.3
    brings option B close to that.
- **The twin copy.** Option B puts each transaction in the log twice. So the log bytes, the
  replication and the log disk are about 2 x those of option A. The feed and the serving
  service do not change: they hold canonical frames.
- **The sequencer.** Its input does not change: it already reads the full envelope. Its output
  grows from about 0.23 MB/s to 2.5 MB/s for each replica at the mix, and to 21.5 MB/s at 8 KB
  only. Two lanes share a node: about 43 MB/s (0.34 Gbit/s) at 8 KB only.
- **The disk retention (decision 10).** No byte budget halts the chain (decision 5). While no
  post lands, only the serving service grows: about 22 GB an hour for each copy at the mix, and
  about 155 GB an hour at 8 KB only. The members and the executors do not grow, because the
  stored head moves. Before decision 10 each of the 3 members grew by about 53 GB an hour at the
  mix (the log and the retained file). The disk size of the serving service sets how long the
  chain rides out a DA outage. The alert of section 11 gives the time to a full disk.
- **An outage of the serving service.** The stored head stops. Each member keeps its log, about
  36 GB an hour at the mix, and each executor keeps its output buffer, about 22 GB an hour. The
  alerts of section 11 fire.
- **The block guard.** The DA-lag guard in blocks stays as today. At 10,000 blocks of 2 s it
  refuses user records after about 5.6 hours without a post. In that time each copy of the
  serving service grows by about 123 GB at the mix. On staging (60,000 blocks, about 33 hours) it
  is about 740 GB. So the disk of the serving service can fill before the block guard refuses, at
  the larger sizes.
- **The heap.** A member retains no frame. The heap holds the state, and a second copy of the
  state while a feed walk runs (section 7.3). So the DA lag does not grow the heap.
- **DA.** 4 MB/s raw at the mix, 41 MB/s at 8 KB only. The second is above the DA estimates of
  the cluster shape (`docs/specs/2026-08-16-10-ggas-cluster-shape.md`, tier 7). DA, not Raft,
  is then the first limit.

**If one Raft group does not carry the target.** The follow-up is a second Raft group: the lanes
are split over two sealer clusters, and a deterministic merge orders their streams, as the
cluster shape proposes for the sequencer (`docs/specs/2026-08-16-10-ggas-cluster-shape.md`,
tier 2). This spec does not design it. It is an open follow-up, opened only when the staging run
shows the limit.

### 10.1 The leader is the ceiling

Every byte of every transaction passes the leader two ways:

- in: from the sequencers, two twin copies (the log input of section 4.5);
- replication: to each follower (2 followers).

The egress sends only 42 bytes a transaction to each ingress (kind 15). The paired executors
read their member's feed, so they cost the leader nothing. The validator, the batcher and the
unpaired executors read the serving service (section 8.1), so they cost the leader nothing
either. Today the 3 executors, the validator and the batcher read the leader's egress.

### 10.2 Network at the leader

| Rate | Size | In | Replication out | Egress out | Total out |
|---|---|---|---|---|---|
| 1,000 tx/s | today (refs) | 0.21 MB/s | 0.42 MB/s | 0.41 MB/s (7 consumers x 59 B) | 0.83 MB/s |
| 1,000 tx/s | transfer | 0.58 MB/s | 1.16 MB/s | 0.08 MB/s (2 ingresses x 42 B) | 1.24 MB/s |
| 1,000 tx/s | 2 KB | 4.6 MB/s | 9.1 MB/s | 0.08 MB/s | 9.2 MB/s |
| 1,000 tx/s | 8 KB | 17.2 MB/s | 34.5 MB/s | 0.08 MB/s | 34.5 MB/s |
| 4,000 tx/s | transfer | 2.3 MB/s | 4.6 MB/s | 0.34 MB/s | 5.0 MB/s |
| 4,000 tx/s | 2 KB | 18.3 MB/s | 36.6 MB/s | 0.34 MB/s | 37 MB/s (0.30 Gbit/s) |
| 4,000 tx/s | 8 KB | 69 MB/s | 138 MB/s | 0.34 MB/s | 138 MB/s (1.1 Gbit/s) |
| 10,000 tx/s | transfer | 5.8 MB/s | 11.6 MB/s | 0.84 MB/s | 12.4 MB/s |
| 10,000 tx/s | 2 KB | 46 MB/s | 91 MB/s | 0.84 MB/s | 92 MB/s (0.74 Gbit/s) |
| 10,000 tx/s | 8 KB | 172 MB/s | 345 MB/s | 0.84 MB/s | 346 MB/s (2.8 Gbit/s) |
| 333,000 tx/s | transfer | 193 MB/s | 386 MB/s | 28 MB/s | 414 MB/s (3.3 Gbit/s) |

- The rows count two twin copies in the log (section 4.5).

- 4,000 tx/s is about today's measured sustained rate of 3,800 tx/s
  (`docs/specs/2026-08-16-pipeline-cost-model.md`, "System context").
- 333,000 tx/s is the blended mix of 10 Ggas/s (`docs/specs/2026-08-16-10-ggas-cluster-shape.md`).
  At that rate the leader writes 193 MB/s of log with an fsync for each batch, and sends
  3.3 Gbit/s. One Raft group then needs a 10 GbE network and a fast NVMe. The cluster shape
  already names the single ordering point as the first limit. With bytes in the log, the way past
  it is the sharding of the sealer, not more references.
- The DA wall of the cluster shape (15 to 37 MB/s compressed to raw at 10 Ggas/s) is lower than
  the Raft wall. So the Raft bytes do not set the first limit of the chain. They set the
  hardware of the sealer nodes.

### 10.3 CPU at the leader

- The service thread copies each envelope once into a canonical frame and once into the feed
  ring. That is about 2 x the frame bytes of `memcpy` for each transaction: under 1 µs for a
  transfer, about 2 µs for an 8 KB call.
- A feed walk (section 7.3) runs the state machine again on the walk thread, on another core of
  the member's cpuset. It does not run on the service thread.
- The service thread also reads the losing twin copy, and drops it at the dedup step. The
  copy is not parsed past the guard header.
- The consensus module and the archive of the leader handle `rate x log bytes`. This cost is in
  Aeron, not in our code. The benchmark measures it.

### 10.4 Disk on each member, each executor and the serving service

See sections 5.5, 8.1 and 8.3:

- **A member.** The log grows by `rate x log bytes`. The purge keeps the 3 newest snapshot
  intervals (15 minutes) and the range above `max(posted_head, stored_head)`. A member retains no
  frame. While no post lands, the stored head moves, so the member does not grow. While the
  serving service stores nothing, the log grows at about 36 GB an hour at the mix.
- **An executor.** The state grows as today. The output buffer (`receipts`, `tx_hash_index`,
  `block_frames`) holds only the blocks above the stored head: seconds of output in steady state.
  While the serving service stores nothing, it grows at about 22 GB an hour at the mix.
- **The serving service.** Each copy holds the frames and the receipts above
  `min(posted_head, attested_head)`, plus the window of its own design. While no post lands, it
  grows at about 22 GB an hour at the mix (section 10.0). No byte budget bounds it (decision 5).
- The snapshot holds the state of today. It does not grow with the DA lag.

### 10.5 The feed

- The feed is IPC only, so it costs no network. Its cost is the copy into the ring
  (section 10.3), and the walk of the Raft log during a replay: a read of the log from a
  snapshot, and a second run of the state machine (section 7.3).
- Memory, for each pair:

| Item | Size | Paid by |
|---|---|---|
| Frame ring | 16 MiB, off the heap | the sealer job (1,024 MB, 384 MB heap) |
| Control ring | 64 KiB, off the heap | the sealer job |
| Walk state | one more copy of the replicated state, on the heap, only while a walk runs; the snapshot size bounds it | the sealer job (the 2 GiB heap of section 5.5) |
| Feeder client | the conductor thread and its buffers, below 1 MiB; the CnC file belongs to the driver | the sealer job |
| Feed log | 3 terms of 8 MiB, plus 4 KiB of metadata: 24 MiB | the aeron system job of the executor node (768 MB, 160 MB heap), because the log is not sparse |
| Control log | 3 terms of 64 KiB: 192 KiB | the aeron system job of the executor node |

- The sealer budget under load is about 734 MB of the 1,024 MB: the heap and the session logs on
  the member's own tmpfs at a failover (`cluster/sealer-service/README.md`, the term table). The
  feed adds about 17 MB to it. That leaves about 270 MB for the JVM outside its heap
  (metaspace, code cache, thread stacks, the GC). The first proposal of a 64 MiB ring left
  about 220 MB.
- The aeron job of an executor node holds its 160 MB heap, the 12 MiB `exec_txs` log and the
  logs of the node's clients. The feed adds 24 MiB. The executor-node tmpfs is 2 GB
  (`aeron_tmpfs_size`). On staging the pair tmpfs of section 7.6 replaces it on a paired node;
  PR 13 sizes it with the same 24 MiB.

### 10.6 The benchmark

The target is in section 10.0. The benchmark gives the curve that the decision needs.

- **Where.** The local container cluster (`just container-up`, then `SHARD=load just shard`
  in `deploy/cluster/justfile:99-125`). All nodes share one host, so the numbers compare the two
  builds. They are not absolute.
  - The container cluster cannot measure the network limit of the leader: its traffic never
    leaves the host. Its 2 KB and 8 KB rows measure CPU, disk, the snapshot and GC only.
  - A run on staging, with the members on three hosts, is required before a decision on the
    size. It measures the network rows of section 10.2.
- **Builds.** `main` (references) and the flagged build of PR 12 (bytes). Same images, same
  settings.
- **Load.** `kardamom-load` (`crates/bench/src/bin/load.rs`) with `--fixed-rate` and
  `--target-tps`. PR 2 adds a workload `calldata` with `--calldata-bytes N`: a transfer with N
  bytes of calldata to an EOA, so the size is the only variable.
- **Matrix.** Rates 500, 1,000, 2,000, 4,000 and 6,000 tx/s (6,000 is today's admission ceiling)
  x raw sizes 110, 2,048 and 8,192 bytes. Each cell runs 5 minutes after a 1-minute warm-up.
- **Measurements for each cell.**

| Measurement | Source |
|---|---|
| Commit latency p50, p99: ingress publish to the sealed id | the ingress observer, a new histogram `kardamom_ingress_seal_latency_seconds` |
| Receipt latency p50, p99 | `kardamom-load` |
| Sustained rate with zero loss | `kardamom-load --assert-all-delivered` |
| Leader CPU: process, service thread, consensus thread | container CPU and per-thread `/proc` sampling |
| Network bytes in and out of each member | node exporter or `docker stats` |
| Log bytes written a second, fsync time | the archive directory size, `iostat` |
| Snapshot size and time, with a full dedup window (bound: 2 s, section 5.5) | `sealer snapshot TAKEN`, with a new `ms=` field |
| Feed walk: the walk rate (frames a second from a snapshot to the head), the heap during a walk | the walk log line with `ms=` and `frames=`, `-Xlog:gc` |
| Disk growth while the batcher is stopped: the serving service grows, the members and the executors do not | the disk size of each, `iostat` |
| GC pause p99 of the leader | `-Xlog:gc` |
| Feed lag: member head minus executor index | `kardamom_executor_input_lag` (new) |
| Refusals | `WINDOW_FULL`, `PAST_DEADLINE`, `CONTIGUITY_REJECT` counts |
| Sequencer bytes: parked, unconfirmed, evictions (section 6.2) | the sequencer metrics |
| The twin copy: log bytes of the losing copies; commit latency with one twin stopped | the archive size; `kardamom_ingress_seal_latency_seconds` |

- **Matrix rows for the target.** 5,000 tx/s with the example mix of section 10.0, and 5,000 tx/s
  with 8 KB only, beside the matrix above.
- **The decision.** One Raft group carries the target when, at 5,000 tx/s with the mix and with
  8 KB only, the p99 commit latency stays under 100 ms, the leader CPU stays under 70 %, and the
  snapshot stays under 2 s. The container run answers for CPU, disk, GC and the snapshot. The
  staging run answers for the network. The result also tunes `--parked-bytes`,
  `--unconfirmed-bytes`, `feedRingBytes`, `readyLagBytes` and the sealer heap.
- **The hedge.** If the twin copy is the limit (the leader's network or the log disk), the
  hedged offer of section 6.3 is the next step. The result says so.
- The 100 ms bound is this spec's proposal. It is above the user target of 50 ms (decision 13).
  The gate changes when open question 16.1-10 is answered. Today's receipt p99 is 47 ms at the edge
  (`docs/specs/2026-08-16-pipeline-cost-model.md`, "Gas throughput and latency").

## 11. Failure modes

| Failure | Effect | Recovery |
|---|---|---|
| A follower dies | Quorum holds. Its executor's feed is silent. | The executor reads the serving service after 6 s. The member restarts, replays its log, and the executor returns to the feed. |
| The leader dies | An election. Full entries in flight can be lost (at most once). | The sequencer re-offers them, with their bytes, from its unconfirmed ledger. That is the only re-offer path. The dedup drops the copies that did commit. The executors on the feeds of the surviving members see no gap. |
| One sequencer twin dies | The other twin already offered every entry with its bytes. | None needed. No gap, no loss, as today. |
| An ingress dies after its `tx_data` publish | The sequencer holds the transaction. | None needed. Nothing is lost. |
| An executor is slow | Its feed ring fills. The service thread does not wait. Other members and executors do not notice. | The feed gaps. The executor asks for a replay from its cursor and catches up through a walk of its member's Raft log (section 7.3). |
| An executor restarts | None for the cluster. | It replays from its state DB cursor through its feed (section 7.4). |
| An executor is behind the feed floor | Its cursor is below the oldest snapshot that its member holds with its log (section 7.3). | `REPLAY_UNAVAILABLE`, then the serving service, then a peer checkpoint (section 7.5, #635). |
| A member is far behind or below the purge point | Its feed lags or is idle. | The join watchdog reseeds the member from a peer (`docs/failure-modes.md:229-240`). Its executor reads the serving service meanwhile. |
| A network partition isolates a member and its executor | The minority member cannot commit. The executor sees no new frame. | The executor switches to the serving service, if it reaches it. Else it waits. It never executes a frame that the majority did not commit, because the member applies only committed entries. |
| A sequencer lane is down (both twins) | No entry for that lane. The transactions in flight on `tx_data` and in the parked buffers are lost, as today. | The lane stalls until a replica returns. The other lane continues. An `on-quorum` client gets no ack, so it retries. |
| A future-nonce flood | Senders fill the parked buffer of a replica with large future-nonce transactions. Nothing enters Raft. | The byte bound evicts the furthest future nonce (section 6.2). The other lanes and Raft do not notice. |
| Receipts are slow | The unconfirmed ledger of a replica fills with bytes. | The bound back-pressures that lane (section 6.2). |
| A snapshot restore | The member restores its state. A v12 snapshot holds no frame. | It replays the log after the snapshot. Its feed serves replays from a walk of that log. |
| All members restart | No commit until a quorum returns. | Each member restores its snapshot and replays its log. The executors reconnect to the feeds and replay. The sequencers re-offer. |
| All members lose their state | The Raft logs and the snapshots are gone. The serving service still holds the blocks above the posted head. | The fleet rebuild from L1 at the posted head (`docs/runbooks/sealer-fleet-rebuild.md`). The unposted range is lost, as today. A rebuild that keeps the unposted range from the serving service is a later follow-up (section 16.3). |
| L1 posts stop for a long time | The serving service keeps every block above the posted head on disk, and grows at the rates of section 10.0. The stored head moves, so the members and the executors do not grow. No byte budget refuses records. The block guard refuses user records at its block budget, as today. | The batcher posts again. The serving service then frees the blocks below `min(posted_head, attested_head)`. How the batcher posts a large backlog is out of scope (section 5.5). |
| The disk of a member fills | It happens only while the stored head does not move (an outage of the serving service or of every executor). The member cannot write its log. It logs a fatal error and stops. All members keep the same log, so disks of one size fill at about the same time. When a quorum stops, the cluster commits nothing: the chain stops. | The alert `KardamomSealerDiskLow` fires first: free disk under 25 %, or under 6 hours at the current growth rate (`kardamom_sealer_disk_free_bytes` and its derivative). The operator restores the serving service or adds disk. A stopped member restarts and replays its log. |
| A member's feed bytes differ from the leader's | A bug: R3 is broken. | An executor whose entry fails its hash check drops that source and replays from the other source, with an alert (section 8). The validator checks every canonical entry it reads. A boundary carries no digest of the stream. A stream digest is a later follow-up (section 16.3). |
| A feed ring fills | The member writes a `FEED_GAP` marker (section 7.2). | The executor asks for a replay at once and stays on the feed. |
| The serving service stops (every writer and replica) | The stored head stops. The batcher, the attester, the validator and the unpaired executors stop reading. The ingress serves receipts from its cache and the executors only. Each member keeps its log, and each executor keeps its output buffer (section 10.0). | `KardamomExecutorOutputUnstored` fires after 10 minutes, then the disk alerts. When the service returns, its writer reads the blocks above its last block from an executor, the stored head moves, and the members and the executors prune. |
| The writer of the serving service stops | The replicas serve reads. The stored head stops until a new writer runs. | A replica becomes the writer (section 8.4) and reads the blocks that it lacks from an executor. |
| The serving service loses blocks above the posted head | A disk loss of fewer copies than the stored head needs loses nothing: the other copies hold the blocks. A loss of every copy loses the blocks between the posted head and the stored head: the members and the executors already pruned them. | The chain reverts to the posted head (`docs/runbooks/revert_to_posted_head.md`), as after the loss of every member. Open question 16.1-5 sets the copies that make this rare. |
| Every executor is down | No block is executed. The serving service has nothing new, so the stored head stops. The sealer keeps ordering into its log. | The log holds the range above the stored head (section 5.5). The executors return, replay from their feeds or a peer checkpoint, and the serving service goes on. |
| Two executors disagree on a block | The serving service stores neither copy, and the stored head stops (section 8.3). | `kardamom_serving_executor_mismatch_total` and an alert. The validator re-executes and halts on a divergence. The operator replaces the faulty executor from a peer checkpoint. |
| The disk of an executor fills | It happens only while the stored head does not move. The commit fails, and the executor stops. | `KardamomExecutorDiskLow` fires first. The paired executors of the other hosts fill at about the same rate. |

## 12. Migration

### 12.1 Formats (`docs/formats.md`, `formats.toml`)

| Format | Today | After the cut-over |
|---|---|---|
| `sealer-ingress-kinds` | writes 8, reads 0..9 | writes 8, reads 0..8. No new ingress kind: record type 5 rides kinds 0 and 3. Kinds 6 and 9 drop as unknown. |
| `sealer-egress-kinds` | writes 12, reads 1..13 | writes 16, reads 1..16. Kind 13 is never sent. Kind 14 is not used. Kind 16 is sent on the feed only. The status frame stays 68 bytes: the 76-byte frame of the retained file goes (decision 10). |
| `sealer-record-types` | writes 4, reads 0..4 | writes 6, reads 1..6. Types 0 and 4 are never written. |
| `sealer-snapshot` | writes 10, reads 1..11 | writes 12, reads 10..12. A later release sets `reads_min = 12`. v12 has no retention section and carries the stored head (section 5.5). It changes in place, because no release wrote it. |
| `aeron-stream-records` | `TxEnvelope` frozen | Unchanged. The envelope travels in its frozen layout. |
| `discovery-record` | `Topic::ExecTxs` | `Topic::SealerFeed` added. `ExecTxs` is removed only after no node advertises it (`crates/log/src/discovery/record.rs:279-296`). |
| `sealer-feed` (new) | none | The feed frames are the egress kinds. The feed control request is the kind-1 layout. |
| `tx-heads` (new, PR 11) | none | Frozen layout of `TxHeader`. |
| `state-db` | 11 tables | The new table `block_frames` and the meta key `output_floor_block` (section 8.3). A reader of the base ignores an unknown table and key, so the change is forward-compatible. The schema version rises only if the state DB rules of `docs/formats.md` require it. |
| `executor-output` (new) | none | The range read of section 8.3: the request and the binary answer. Frozen at its first release. Its transport follows open question 16.1-4. |
| `batcher-spool`, `batcher-cursor` | unchanged | Unchanged. |

- Each change that the base cannot read gets a `[one_way.<id>]` and a `[coordinated.<id>]`
  waiver with its reason.
- `sealer-ingress-kinds` also gets kind 11 `STORED_CURSOR` (section 4.3), above kind 10 of the
  nonce release (#634). A member of the base drops kind 11 as an unknown kind, so the serving
  service can send it before the sealer reads it.
- **The P3 snapshot rule.** Today the writer stays one version behind the reader: reads 11,
  writes 10 (`CanonicalSealerState.java:164`, `:181`). The removal of the cursor and the void
  sections does not get its own two-release step. It rides the coordinated cut-over: the release
  is one-way and coordinated already (decision version 3, new kinds that change the state), so a
  mixed fleet never runs. The new release reads v10, the snapshot that the drained old release
  wrote, and writes v12 at once.

### 12.2 Staging: a chain reset

The user chose a chain reset for staging.

1. Merge PR 13. Its images carry the new formats with the flags on.
2. Tear down the staging chain and launch a new one from genesis: `just teardown`, then
   `just launch` in `kardamom-infra` (`docs/staging-launch.md:120-123`). The launch deploys the
   new chain contracts, so the old bridge state, the canary history and the L1 record stay
   behind with the old contracts.
3. The node_containers role creates the pair mounts of section 7.6 before the node containers
   start.
4. Check: `just smoke`, the canary, and one chaos shard against staging.

- No v10 snapshot is read, and no drain is needed. The waivers of section 12.1 still apply,
  because the release gate compares the registry of the deployed release with the target.
- dev-32 recommended the drained cut-over on staging as a rehearsal of production. The user chose
  the reset. The drained cut-over gets its rehearsal in the container cluster instead: PR 13
  adds a test that runs section 12.3 there under load.

### 12.3 Production: the drained cut-over

Production keeps its chain. The cut-over reuses the coordinated restart of the decision version
(`cluster/sealer-service/README.md:405-416`).

1. Deploy every reader first, behind the flags of section 13 (off): the ingress, the sequencers,
   the executors, the validator and the batcher read the new kinds but do not write them.
2. Pause the submits on every ingress (`POST /pause?note=cut-over`, `crates/obs/src/serve.rs:57-59`).
   Wait until the sealer's canonical count stops. The boundary tick still closes empty blocks.
   Call `D` the block that holds the last ordered transaction.
3. Flush the batcher: `POST /flush` on the batcher (new, PR 13). It is an admin route of the
   service server, loopback only, like `/pause`. It makes the open group due at once, so the
   batcher posts every block up to the newest block that it consumed.
   - A submit pause alone does not close the group today: the group closes on its count, its
     bytes, its timer or the DA-lag rule (`crates/batcher/src/live/feed.rs:85-102`). On staging
     the idle timer can be hours.
   - The batcher could instead flush on `Paused{Operator}` of the ingresses on the `events`
     stream. That makes the batcher infer the operator's intent from other services. An explicit
     route is simpler to test and to script.
4. Wait until all of these hold:
   - the posted head is at or above `D` (`kardamom_ingress_cluster_posted_head`);
   - when the attester runs: the attested head is at or above `D`. The attester keeps its leaves
     in memory only (`crates/validator/src/attester/state.rs:44-63`), and the serving service
     holds no block at or below the cut. So an output root that does not cover `D` before the cut
     strands the withdrawals of the blocks above it. The attester posts on its interval
     (`--attester-post-interval`); the operator waits for it, or lowers the interval;
   - the cursor of every consumer equals the sealer's canonical count and the open block: each
     executor (feed or egress), the validator and the batcher. Then no consumer holds a `TxRef`
     past the cut that it cannot resolve after the floor moves. The block cursors count too, not
     only the record cursors (#598);
   - no void vote is open (`cluster VOID-VOTE` lines all have `result=DECIDED`).
5. Take a sealer snapshot (v10). Copy it, and take a checkpoint of each executor and of the
   validator, to the backup volume. Keep these copies until the first v12 snapshot is one day
   old. They are the input of the rollback of section 12.5.
6. Stop the writers: the ingresses and the sequencers.
7. Purge the sealer job. Start the new sealer release (decision version 3). It restores the v10
   snapshot:
   - it drops the void ledger, which holds no open vote;
   - it drops every retained `TxRef` frame and keeps no frame (section 5.5). A drained v10
     snapshot still holds frames: the last `retention` frames (65,536) stay even when all are
     posted, and the boundaries of the empty blocks after `D`. No consumer reads them after step 8,
     because every consumer resumes at the head. Every member drops the same frames, so no import
     of the v10 frames is needed (#598);
   - the sealer setting `recordLagBudget` is 0, or the guard is gone. The executors with
     `--carry-bytes` send no recorded cursor (#635).
8. Turn the flags on. Start the executors, the validator and the batcher; they resume at the head
   with no `TxRef` frame to resolve. The validator and the batcher now read the serving service
   (section 8.1), so it runs before this step. It starts empty, at the cut: every block at or
   below `D` is posted, and the attested head is at or above `D`. Its first stored head is `D`,
   so the sealer floor stays the posted head until it stores the first new block. Then the
   sequencers, then the ingresses. Unpause.
   - The executors of the old release hold receipts for all blocks. The first prune after the
     cut-over drops the output at or below the stored head. Old receipts then come from the
     validator's state DB only. Open question 16.1-9 asks if the serving service must import the
     old receipts, so that `eth_getTransactionReceipt` keeps answering for them.

- A consumer that restores a checkpoint older than the cut-over cannot replay the old range: the
  floor is at the cut. It takes a newer checkpoint, or the replay answers `REPLAY_UNAVAILABLE`
  and it fetches a peer checkpoint. The deploy takes a checkpoint of each executor after step 8.
### 12.4 Deploy order

1. PR 1 to PR 11 merge with the flags off. Each deploys as a normal rolling release. They change
   no format that the base cannot read.
2. PR 12 (benchmark) runs on the flagged build in a container cluster and on staging
   (section 10.6).
3. PR 13 is the cut-over release: the waivers, the flags on by default, the runbook of 12.3.
   Staging takes it by the reset of 12.2. Production takes it by the cut-over of 12.3.
4. PR 14 removes the dead code. It is a normal release, because the cut-over already stopped every
   writer of the removed kinds.

**Decision 10 adds PR 15 to PR 17.** PR 15, 16 and 17 change the sealer and v12. No release
writes v12 before PR 13, so they need no waiver of their own:

1. #634 (PR 6, with the retained-file section in v12) merges as it is. It has passed three review
   rounds, and the removal touches the same classes. One removal after it is less work than a
   rework of #634.
2. PR 16 (the feed walk of the Raft log) merges next. The feed then no longer reads the file.
3. PR 15 (the removal of the file and its v12 section) merges after PR 16, and before PR 13.
4. PR 17 (the stored cursor) merges after #634, and before PR 13.
5. PR 13 sets `writes = 12` and turns `carryBytes` on. From then v12 is frozen.

### 12.5 Rollback (production)

This section applies to production. Staging has no rollback across the reset: a failed staging
release is fixed forward, or the reset runs again with the release before.

- Before the first v12 snapshot, a rollback is the coordinated restart into the old release: it
  restores the v10 snapshot of step 5, and the executors and the validator restore their step-5
  checkpoints. Every envelope ordered after it is lost, so do it only with the
  submits still paused.
- After the first v12 snapshot, the old release cannot read the snapshot. The rollback is:
  pause, drain until `posted_head == sealed_head`, then the fleet rebuild from L1 at the posted
  head into the old release (`docs/runbooks/sealer-fleet-rebuild.md`). Nothing is lost, because
  L1 holds every block.
- The deploy record gets a rollback floor at the cut-over release (`docs/formats.md:155-171`).
  `just rollback` does not cross it. The floor applies to staging too. `docs/runbooks/deploy-rollback.md` gets the path above.

## 13. Implementation plan

Each item is one pull request and one issue. Each passes `just style` and CI alone and follows
`docs/STYLE.md`. All PRs can start now: the input path is decided (decision 9).

```text
 PR1 fragmentation ──┬──> PR4 wire ──┬──> PR5 retained file (merged, #608; removed by PR15)
 PR2 bench tooling   │               ├──> PR6 record path, v3, v12 (#634) ──┬──> PR17 stored cursor ──┐
 PR3 size cap        │               │                                      └──> PR15 file removal ───┤
 PR11 tx_heads       │               ├──> PR7 member feed (merged, #632) ──> PR16 feed log walk ──────┤
                     │               ├──> PR8 sequencer full entry ───────────────────────────────────┤
                     │               ├──> PR9 ingress observer ───────────────────────────────────────┤
                     └───────────────┴──> PR10 engine reader (after PR7) ─────────────────────────────┤
                                                                                                      ▼
   PR2, PR3, PR6, PR7, PR8, PR9, PR10, PR15, PR16, PR17 ──> PR12 benchmark run ──> PR13 cut-over ──> PR14 removal
                         (PR13 also needs the serving service and the executor handoff, sections 8.1 and 8.3)
```

| PR | Title | Depends on |
|---|---|---|
| 1 | cluster: fragmented session messages up to 1 MiB | none |
| 2 | bench: calldata workload, seal latency, snapshot time | none |
| 3 | ingress: cap a transaction at 128 KiB | none |
| 4 | wire: full entry, canonical entry, kinds 15 and 16, status tail | 1 |
| 5 | sealer: retained frames in an append-only file (merged as #608; removed by PR 15) | 4 |
| 6 | sealer: record path with bytes, decision version 3, snapshot v12 | 4, 5 |
| 7 | sealer: the member feed over IPC (merged as #632) | 4, 5 |
| 8 | sequencer: offer the full entry, with byte bounds | 1, 4 |
| 9 | ingress: observer reads kind 15 | 4 |
| 10 | engine: read `RT_TX` from the feed or the serving service | 4, 7 |
| 11 | ingress: publish `tx_heads` | none |
| 12 | bench: run the matrix and gate on the target | 2, 3, 6, 7, 8, 9, 10, 15, 16, 17 |
| 13 | release: cut-over to bytes in the Raft log | 12, 15, 16, 17, the serving service and the executor handoff |
| 14 | cleanup: remove the void, the join and the archives | 13 |
| 15 | sealer: remove the retained file and its v12 section | 6, 16 |
| 16 | sealer: the member feed replays from the Raft log | 7 |
| 17 | sealer: the stored cursor (ingress kind 11) and the purge floor | 6 |

The executor side of the handoff (section 8.3: `block_frames`, the range read, the prune, the
alerts) and the ingress fallback to the serving service (section 8.4) are issues that the design
of #585 opens, because their transport follows open question 16.1-4.

**PR 1. cluster: fragmented session messages up to 1 MiB.**

- Scope: section 4.4. The ingress side of the cluster (`LiveIngress::offer`) and the egress and
  feed subscriptions behind the fragment assembler. No behavior change.
- Acceptance: a `TestCluster` test sends 64 KiB and 1 MiB messages both ways. The existing tests
  pass.

**PR 2. bench: calldata workload, seal latency, snapshot time.**

- Scope: section 10.6. The `calldata` workload with `--calldata-bytes N`, the histogram
  `kardamom_ingress_seal_latency_seconds`, the `ms=` field on `sealer snapshot TAKEN`, a `just`
  recipe that runs the matrix and writes a table.
- Acceptance: a baseline run on `main` in the container cluster, committed to `docs/benches/`.

**PR 3. ingress: cap a transaction at 128 KiB.**

- Scope: section 6.6. `--max-tx-bytes` (default 131,072), the JSON-RPC error with the size and
  the cap, `kardamom_ingress_rejected_total{reason="too_large"}`, `docs/json-rpc.md`.
- Acceptance: a test accepts a 128 KiB `raw_tx` and refuses 128 KiB + 1 byte, on the JSON-RPC
  path and the binary path.

**PR 4. wire: full entry, canonical entry, kinds 15 and 16, status tail.**

- Scope: sections 4.1 to 4.3. Java and Rust constants and codecs for record type 5
  (`RT_TX_OFFER`), record type 6 (`RT_TX`), egress kind 15, feed kind 16, and the status tail
  `[retained_bytes:u64]`. Readers only. `formats.toml`: `reads_max` raised.
- Acceptance: round-trip tests in Java and in Rust on shared golden bytes. A reader of today
  still reads the frames of today. `just check-formats main` passes.

**PR 5. sealer: retained frames in an append-only file.**

- Scope: section 5.5. The segment files, the memory-mapped append, the helper thread that
  preallocates, the index on the heap, the purge of segments, the truncation on restore, the copy
  with a peer seed, `kardamom_sealer_disk_free_bytes`. Behind `kardamom.cluster.carryBytes`
  (off: frames on the heap, as today).
- Acceptance: in a `TestCluster`, every member has equal file bytes. A restore truncates the file
  and the log replay rebuilds it. The heap does not grow with the retained bytes. A failed
  preallocation stops the member with a fatal error.
- Status: merged as #608. Decision 10 removes it (PR 15).

**PR 6. sealer: record path with bytes, decision version 3, snapshot v12.**

- Scope: sections 5.1 to 5.4 and the snapshot of 5.5. Record type 5 through the record path of
  today, the `RT_TX` relay, kind 15 on the egress, decision version 3 (no void), the v12 writer
  and reader. Behind `kardamom.cluster.carryBytes`.
- Acceptance: Java state tests: the second twin copy drops at the dedup; each refusal is the same
  as today; equal snapshot bytes on every member; a v10 restore; the coordinated restart to
  version 3.

**PR 7. sealer: the member feed over IPC.**

- Scope: sections 7.1 to 7.3 and 7.6. The ring, the feeder thread and its own Aeron client on
  `feedAeronDir`, the feed control stream, the replay walk from the retained file, the `idle` and
  `gapped` states, `FEED_GAP`.
- Status: merged as #632. PR 16 changes the source of its replay walk.
- Acceptance: a `TestCluster` test: the feed of each member equals the leader's feed byte for
  byte; a stalled subscriber does not slow the commit; a full ring writes `FEED_GAP`, and the
  replay reaches the head.

**PR 8. sequencer: offer the full entry, with byte bounds.**

- Scope: sections 6.1 to 6.3. `RefMetadata` keeps the envelope, the record type 5 entry, the
  64 KiB byte limit for a batch, the fragmented offer, `--parked-bytes`, `--unconfirmed-bytes`
  and their metrics. Behind `--carry-bytes`. The hedge is not in scope.
- Acceptance: unit tests: a full parked bound evicts the furthest future nonce; a full
  unconfirmed bound back-pressures the lane. An integration test: both twins offer, and each
  transaction commits once.

**PR 9. ingress: observer reads kind 15.**

- Scope: section 6.4. The observer reads kind 15 for record type 6 and acks `on-quorum` from it.
  Behind `--carry-bytes`.
- Acceptance: a test acks `on-quorum` from kind-15 frames. The observer never gets an envelope.

**PR 10. engine: read `RT_TX` from the feed or the serving service.**

- Scope: sections 7.4, 7.5 and 8. The executor reads `RT_TX` from its feed and falls back to the
  serving service, then a peer checkpoint. The hash check on each entry, and the source switch on
  a mismatch. The validator's hash and signature check. The batcher packs from the entry.
  Behind `--carry-bytes`; the join path stays while the flag is off.
- Acceptance: tests for the source order and the switch at a frame boundary, the mismatch path,
  and a validator halt on a bad signature. The batcher packs the same KAR1 bytes as from the join.

**PR 11. ingress: publish `tx_heads`.**

- Scope: section 6.5. The frozen `TxHeader` record, the `tx-heads` format entry, the stream
  (base 3000 + lane), the publisher in the ingress. No reader until section 16.1 answers.
- Acceptance: a round-trip test of `TxHeader`. The ingress publishes one header for each
  `tx_data` envelope, on the same lane.

**PR 12. bench: run the matrix and gate on the target.**

- Scope: section 10.6 on `main` and on the flagged build, in the container cluster and on staging.
  The results go into this spec as section 17.
- Acceptance: section 17 answers whether one Raft group carries 5,000 tx/s with the mix and with
  8 KB only, gives the tuned values, and says if the hedge of section 6.3 is needed.

**PR 13. release: cut-over to bytes in the Raft log.**

- Scope: the flags on by default, the waivers of section 12.1, the pair mount and
  `feedAeronDir` in the Nomad jobs and in `terraform/containers`, `POST /flush` on the batcher, the
  runbook of 12.3 and its rehearsal case, the rollback floor, `KardamomSealerDiskLow`,
  `KardamomExecutorOutputUnstored`, `KardamomExecutorDiskLow`, `recordLagBudget=0`, the chaos
  cases of 13.1. The pair mount of the staging hosts is a `kardamom-infra` change to the
  node_containers role, merged before the reset of 12.2. No import of the v10 frames: a member
  with `carryBytes` drops them (section 5.5).
- Depends on: PR 12, PR 15, PR 16, PR 17, the serving service of section 8.1 and the executor
  handoff of section 8.3 in place.
- Acceptance: `cutover-rehearsal` passes in the container cluster. The staging reset passes
  `just smoke`, the canary and one chaos shard.

**PR 14. cleanup: remove the void, the join and the archives.**

- Scope: everything in section 9.1. `reads_min` of the snapshot goes to 12 in the release after.
- Acceptance: no reference to the removed items is left. The chaos shards pass.

**PR 15. sealer: remove the retained file and its v12 section.**

- Scope: sections 5.5 and 9.1 (decision 10). Remove the retained file of #608 and every class
  that only it uses, the `/retained` endpoint and its token, the retained-file copy of the peer
  seed, the 76-byte status, and the retention section of v12. With `carryBytes` on, the egress
  retains no frame and answers an egress replay with `REPLAY_UNAVAILABLE`. A member with
  `carryBytes` on restores v10 and drops its frames. Keep `kardamom_sealer_disk_free_bytes`.
- Depends on: #634 merged, PR 16 merged. Before PR 13.
- Acceptance: no reference to the removed classes is left. A `TestCluster` with `carryBytes` on
  has equal v12 snapshot bytes on every member, with no retention section. A member with
  `carryBytes` on restores a v10 snapshot with retained frames, and every member then holds the
  same state. A member with `carryBytes` off refuses v12. The status frame is 68 bytes.

**PR 16. sealer: the member feed replays from the Raft log.**

- Scope: section 7.3. The walk thread, the snapshot load into a private state machine, the
  archive replay of the log recording, the one dispatch shared with the clustered service, the
  feed floor from the purge marks (with the canonical count), the hand-off at the head, the
  `FEED_GAP` when the purge overtakes a walk. It replaces the walk of the retained file in #632.
- Depends on: #632 (merged).
- Acceptance: a `TestCluster` test: a walk from each kept snapshot gives the frames of the live
  feed byte for byte, through an epoch with deposits and a leader change; the commit latency
  does not rise during a walk; a cursor below the oldest kept snapshot gets
  `REPLAY_UNAVAILABLE`; a purge that overtakes a walk ends it with `FEED_GAP`, and the next
  request reaches `REPLAY_DONE`.

**PR 17. sealer: the stored cursor and the purge floor.**

- Scope: section 5.5. Ingress kind 11 `STORED_CURSOR`, the replicated stored head, its place in
  v12, the purge floor `max(posted_head, stored_head)`, `storedHead` on `/status`, the log line
  `cluster STORED-CURSOR`. The Rust encoder of kind 11 in `crates/cluster-adapter`.
  `sealer-ingress-kinds` writes 11.
- Depends on: #634 merged. Before PR 13.
- Acceptance: a `PurgePlanner` test: with a stored head above the posted head, the purge point
  moves to the stored head; with no kind 11, the floor is the posted head. A `TestCluster` test:
  every member restores the same stored head from v12. A member of the base drops kind 11.

Rules to check in each pull request: R1 (comments carry no phase or spec names), R5 (the feeder,
the ring and the walk thread each have one owner, and the feed ring has one writer at a time), R6 (the feed is an associated type of
the reader's wiring, not `dyn`), R9 (parse each frame once at the boundary), R12 (checked
arithmetic for the byte counters and the wire lengths), R14 (one codec and one ingest path for the
feed and the serving service), R16 (the release drain stays a one-step loop body).

### 13.1 Chaos cases

| Case | Change | Assertion |
|---|---|---|
| `archive-driver-loss`, `archive-tx-data-wipe`, `archive-corruption` (`crates/chaos/src/cases/archive.rs`) | Remove the `tx_data` parts. Keep the `tx_deposits` archive loss. | The deposit path recovers. |
| `pipeline-blackout-recover` (`crates/chaos/src/cases/coordinated.rs:104-150`) | Drop the void-decision count. | Every acknowledged transaction has a receipt after the blackout. No `VOID` line exists. |
| `hard-executor`, `graceful-executor` (`crates/chaos/src/cases/component.rs:7-34`, `exec_stream.rs`, `recorded_cursor.rs`) | Drop the stream and cursor checks. | The restarted executor resumes through its feed (`input_source="feed"`) with no gap. |
| `batcher-outage-past-retention` (`crates/chaos/src/cases/l1/outage.rs`) | The first half stays: freeze past the floor and a snapshot, thaw, recover from the spool and the replay of the serving service. The second half proves the disk retention: freeze the batcher with SIGSTOP and load until the serving service holds more than 1 GiB above the posted head; thaw. | First half: the rebuild line (`crates/chaos/src/cases/l1/batcher.rs:22-23`) count does not grow. Second half: no `DA_LAG_REJECT` while the block budget is not passed; `kardamom_halt{cause="da_lag"}` stays 0; the log of each member and the output buffer of each executor stay below their size before the freeze plus the margin; after the thaw the batcher replays from the serving service and posts, and the serving service frees the posted blocks. Both halves: L1's record is contiguous; root parity holds. |
| `sealer-fleet-total-wipe-recover` (`crates/chaos/src/cases/fleet/sealer_wipe.rs`) | None. The rebuild mark does not change. | As today. |
| `validator-join` | Rename to `validator-catchup`. It runs once the serving service exists (section 8.1). | The validator catches up through the serving service. |
| `feed-member-kill` (new) | Kill sealer member 1 under load. | Executor 1 switches to the serving service within 10 s and back after the restart. No gap. The executor roots equal the validator's. |
| `feed-slow-executor` (new) | SIGSTOP executor 2 longer than the ring holds, then SIGCONT. | The commit latency of the other members does not rise. Executor 2 catches up through a walk of its member's Raft log. |
| `leader-kill` (new) | Kill the leader under `on-quorum` load with 8 KB transactions. | The sequencers re-offer from their unconfirmed ledgers. Every acknowledged transaction has a receipt. No transaction is ordered twice. |
| `sequencer-byte-cap` (new, small `--parked-bytes`) | Flood lane 0 with large future-nonce transactions from new keys. | The parked bound of lane 0 holds and evicts. No flood transaction enters the Raft log. The commit latency of lane 1 does not rise. |
| `sequencer-twin-kill` (new) | Kill one twin of lane 0 under load with 8 KB transactions. | No gap and no loss. Every acknowledged transaction has a receipt. |
| `cutover-rehearsal` (new, container cluster only) | Run the drained cut-over of section 12.3 under load, from the release before PR 13. | The posted head reaches `D`; every consumer cursor equals the canonical count; the new members restore the v10 snapshot; the chain continues; root parity holds. |
| `feed-driver-restart` (new) | Restart the aeron system job on executor node 1 under load. | Member 1's commit latency does not rise. The feeder reconnects. Executor 1 returns to its feed with no gap. |
| `sealer-disk-full` (new, container cluster only) | Fill the disk of member 2 while the serving service is stopped. | `KardamomSealerDiskLow` fires before the disk is full. Member 2 stops with a fatal error. Quorum holds on members 0 and 1. After the operator frees space, member 2 restarts and catches up. |
| `feed-log-walk` (new) | Stop executor 2 for longer than the ring holds and less than the feed floor, then for longer than the feed floor. | First: executor 2 catches up through a walk of its member's log, with `input_source="feed"`, and the walk frames equal the live frames. Second: the feed answers `REPLAY_UNAVAILABLE`, and executor 2 catches up from the serving service. |
| `serving-service-outage` (new) | Stop the serving service under load for 15 minutes, then start it. | The chain goes on. `KardamomExecutorOutputUnstored` fires. The stored head stops, and the members keep their log above it. After the start the writer reads the missed blocks from an executor, the stored head reaches the head, and the members and the executors prune. Every receipt is in at least one tier during the run. |
| `receipt-tier-fallback` (new) | Ask the ingress for a receipt below the stored head, after a restart of the ingress. | The receipt comes from the serving service, with the same bytes as the executor gave before its prune. |
| Planned and dropped | `exec-peer-fetch`, `record-lag-halt`, `validator-exec-archive-catchup`, `exec-archive-prune` of the archived-data spec | Not built. |

## 14. Docs that change with the code

- `docs/failure-modes.md`: the sealer section (the record with bytes, the feed walk of the log,
  the stored-head floor and a full disk), the executor output buffer and its alerts, the void
  section goes, the executor section (the feed), the sequencer section (the full entry, the byte
  bounds, the twin copy), the ingress section (the size cap, the ack policies, the receipt
  fallback to the serving service), the batcher resume sources.
- `cluster/sealer-service/README.md`: kinds (kind 11), record types, settings, snapshot v12 with
  no retention section, the feed and its walk of the log, the purge floor, the decision version
  table (version 3). The "Retained file" section goes (PR 15).
- `docs/aeron-discovery.md`: the `sealer_feed` topic and streams; `exec_txs` and the `tx_data`
  archive topics go.
- `docs/observability.md`: the new metrics of sections 5.5, 6.2, 6.6, 7.5 and 10.6.
- `docs/l1-data-path.md`: the batcher reads bytes from the serving service.
- `deploy/alerts.yml`: `KardamomSealerDiskLow`, `KardamomExecutorOutputUnstored`,
  `KardamomExecutorDiskLow`.
- `docs/tx-status-events.md`: the separate PR that removes the notifier removes it (decision 12).
- `docs/json-rpc.md`: the error for a transaction above the size cap (section 6.6).

## 15. Considered and not chosen

- **Option A: references, with an ingress envelope session.** This was the first choice. The
  ingress offers each envelope into the cluster through its own session (ingress kind 10). The
  sequencer offers an 87-byte reference. The sealer holds the envelopes in a replicated store and
  pairs each with its reference. The user replaced it with option B (decision 9), for these
  reasons:
  - **DDoS.** In A every transaction that passes the ingress enters the Raft log before any nonce
    or fee check: future nonces, many transactions at one nonce, low fees, past nonces. A key and
    a signature cost the attacker nothing. About 64 future-nonce transactions of 1 MiB fill the
    64 MiB lane store, and `ENVELOPE_FULL` then refuses every honest envelope of the lane. In B
    such junk stays in the bounded sequencer memory and never reaches Raft.
  - **A re-offer amplifier.** The ingress re-offers each unsealed envelope every 2 s for up to
    130 s. Each re-offer is a new log entry, so one refused envelope can enter the log about 65
    times.
  - **The gap moves; it does not close.** A reference without its envelope waits and is refused
    at its deadline. An envelope without its reference stays until its TTL. These are replicated
    wait states, a smaller form of the void. B has no such gap: the bytes and the order arrive in
    one entry.
  - **Two re-offer paths** after a leader change (the ingress ledger and the sequencer ledger), in
    either order. B has one.
  - **No real latency gain.** Raft commits a prefix, so a reference commits only after its
    envelope. The bytes are on the commit path in both options. A adds a serial step: the ingress
    publishes on `tx_data` only after its cluster offer.
  - **What A saves.** One copy in the log: about half of B's log bytes and replication
    (section 10.0). The hedge of section 6.3 gets B close to that, if the benchmark needs it.
- **No sequencer.** The ingress offers each envelope with its guard header, and the
  sealer orders it with its nonce contiguity. It was not chosen as the first step:
  - the sequencer's work would move into the sealer or the ingress: the fee gate, the parking of
    future nonces, the sender floors from receipts, Redis and the executors, the epoch relay, and
    the rewind on a refusal (`docs/failure-modes.md:474-540`);
  - the sealer has no account state for the fee gate;
  - two active/active ingresses take one sender's transactions, so each ingress would need the
    sender's nonce order across both;
  - the pre-ordering DDoS surface of option A applies to it too.
  It stays a possible later step after the feed runs.
- **The hedged offer now.** The hedge of section 6.3 brings the log back to one copy. It adds
  latency for half of the senders while one twin is down. It waits for the benchmark.
- **A side stream from the ingress to the sealer nodes.** It is not log input, so the members could
  see different bytes. The user ruled it out: bytes enter only as Raft log input.
- **The retained file on each member (#608).** The first version of this spec kept the canonical
  frames above the posted head in an append-only file on each member, and the feed replayed from
  it. The user removed it on 2026-10-17 (decision 10). The file put the DA-lag growth on every
  member, three times, beside the log. The serving service now holds that range once, for the
  readers that need it, and the feed replays from the Raft log.
- **A feed replay from a read of the Raft log, with no state machine.** The log holds the inputs,
  not the canonical entries. A canonical frame exists only after the state machine orders its
  record. So the walk of section 7.3 runs the state machine again from a snapshot.
- **The posted-head floor on the Raft-log purge, after decision 10.** It keeps the log of each
  member above the posted head, so a DA outage grows each member by about 36 GB an hour at the
  mix. The stored head of the serving service replaces it (section 5.5).
- **No floor on the Raft-log purge.** The serving service reads only the executors. An entry that
  no executor wrote out and the serving service did not store lives only in the log. A purge with
  no floor loses it when every executor or the serving service is down for longer than the margin
  (section 5.5).
- **Back-pressure on an executor when the serving service is down.** It saves no data, because the
  sealer keeps the log above the stored head, and it stops the receipts (section 8.3).
- **`ExecTxRecord` for the remote consumers.** The serving service uses the canonical frames of
  the feed, with replay (sections 8.1 and 9.2).
- **A byte budget that halts the chain.** A byte budget on the retained bytes would refuse user
  records while L1 posts stop. The user ruled it out (decision 5). The serving service keeps the
  data on disk instead (decision 10).
- **The members serve the remote consumers.** The leader's egress, or a UDP feed from each
  follower, could carry the bytes to the validator, the batcher and external nodes. The user ruled
  it out (decision 7). A member feeds only its own executor. A separate service serves the rest
  (section 8.1).

## 16. Decisions of the user

Decisions 1 to 9 are of 2026-10-11. Decisions 10 to 13 are of 2026-10-17.


1. **Target rate and size.** Up to 5,000 tx/s with mixed sizes. Section 10.0 states it as the
   design target. The benchmark must show whether one Raft group carries it, with the 8 KB rows,
   and the staging run measures the network. If one group does not carry it, a second Raft group
   (lane sharding) is the open follow-up. This spec does not design it.
2. **Placement and the feed.** Sealer member `i` and executor `i` run on one host and share one
   media driver; the feed is IPC (section 7.6). Decision 7 removes the UDP feed and the egress
   fallback. An executor without a local member reads the serving service.
3. **Staging migration.** A chain reset (section 12.2). Production uses the drained cut-over
   (section 12.3), and the backups and the rollback of section 12.5 apply to production.
4. **Starting limits.** Accepted: a 2 GiB sealer heap and `readyLagBytes` at 64 MiB. The
   benchmark tunes them. Decision 5 removes the byte DA-lag budget. Decision 9 removes the limits
   of the envelope store and the waiting references (64 MiB for each lane, 16,384), because
   option B has neither.
5. **No byte-budget halt.** The chain does not halt on a byte budget. The committed data stays
   for as long as L1 does not hold it. Decision 10 moves that data from the members to the
   serving service (section 8.1). A full disk stays a failure mode, with an alert
   (section 11). How the batcher posts a large backlog (the inclusion cost) is a future
   optimisation, out of scope.
6. **`tx_heads`.** In scope (PR 11). The stream is header-only: the sender, the nonce and the
   hash, with the fixed fields of section 6.5. It carries no `raw_tx`. Under decision 9 the
   sequencer still needs `tx_data`; section 16.1 asks which reader uses `tx_heads`.
7. **Who serves the bytes.** A member feeds only its own co-located executor, over IPC. The
   leader and the followers do not serve the validator, the batcher or external nodes. A
   separate service serves them, with its own design (section 8.1). This closes the question
   "remote consumers from a follower": no. Decision 10 makes that service read from the
   executors.
8. **The stream digest.** Not in this program. It is a later follow-up (section 16.3).
9. **The input path: option B.** After a separate evaluation, the sequencer carries the full
   transaction into the cluster (sections 4.1, 5.1 and 6.1). There is no envelope path from the
   ingress into the cluster and no join. The twins stay, and the log holds two copies. A hedged
   offer is a later option, only if the benchmark needs it (section 6.3). The ingress caps a
   transaction at 128 KiB (section 6.6). The reasons are in section 15: the DDoS surface and the
   re-offer amplifier of option A, its replicated wait states, and no real latency gain. Every PR
   of section 13 can start.
10. **The retention moves to the serving service (2026-10-17).**
    - The sealer keeps no retained file. The append-only file of #590 and #608 goes (PR 15). The
      retention and purge rules of the sealer are the Raft-log rules: the snapshots, and the
      purge with a margin of 3 snapshots (section 5.5).
    - The serving service (#585) reads only from the executors, not from the sealer. The
      append-only log of the transactions lives in that service (sections 8.1 and 8.3).
    - A long window matters only to the batcher, which posts to L1, and to the attester of the
      withdrawal off-ramp, which posts output roots to L1. The serving service keeps every block
      above the posted head and the attested head for them. The rule "keep the data until L1
      holds it, do not halt" now applies to the serving service.
    - The member feed replays from the member's own Raft log, from a snapshot, not from a file
      (section 7.3, PR 16). An executor that is further behind falls back to the serving service
      or a peer checkpoint, as the reader of #595 (#635) does.
    - A member feeds only its own co-located executor (decision 7, unchanged).
    - The executor is a short buffer in front of the serving service. It keeps its output (the
      frames, the receipts, the transaction index) until the serving service stores it and sends
      back its stored head (section 8.3). Its state stays.
    - From the review of this decision: the Raft-log purge keeps a floor, but at
      `max(posted_head, stored_head)`, not at the posted head (section 5.5). Snapshot v12 drops its
      retention section and carries the stored head; v12 is not released, so it changes in place.
11. **Two read tiers (2026-10-17).** The hot tier is the executor: the internal services (the
    ingress, the state mirror and Redis, the sequencer) read it, with no new hop. The cold tier is
    the serving service: horizontally scaled read replicas of one append-only log, with one writer
    that reads the executors. The batcher, the attester, the public history RPC, indexers,
    explorers and the validator read it. The batcher and the attester read
    only the cold tier. The ingress asks the serving service for a receipt that the executors
    pruned. The executor prunes only at the stored head, so each receipt is always in at least
    one tier (section 8.4).
12. **The notifier (2026-10-17).** It is removed for now: no WebSocket status feed and no
    webhooks. A separate PR removes the code. The serving service keeps nothing for it
    (section 8.4).
13. **The latency target (2026-10-17).** A transaction lands within 50 ms of its submit
    (section 10.0). Open question 16.1-10 asks for the percentile and the meaning of "lands".

### 16.1 Still open

1. **A reader for `tx_heads`.** Under option B the sequencer must read the bytes on `tx_data`, so
   `tx_heads` does not replace it. The evaluation proposed to drop `tx_heads`; the user keeps it
   in scope. Which service reads it: for example the hedged sequencer, or a future
   lane resize? Until the answer, PR 11 builds the publisher only.
2. **The ingress observer.** The leader still sends a 42-byte kind-15 frame for each transaction
   to each ingress, for the `on-quorum` ack (section 6.4). It carries no bytes. Is this inside
   decision 7, or does the ack move to the serving service too?
3. **Superseded work.** Close #543, #542, #554 and #570 as superseded (section 9.2)?
4. **The read interface between the executor and the serving service (section 8.3).** The
   proposal: a new state DB table `block_frames`, and a binary range read on the executor's state
   query server, with the stored head as the ack in each request. The other options: the live
   `tx_receipts` stream with a new stream for the bytes, or a recorded Aeron stream from each
   executor. The code does not settle it. The design of #585 proposes; the user decides.
5. **The durable copies of the stored head (sections 8.3 and 8.4).** The stored head frees the
   sealer log and the executor buffers. After that the serving service holds the only copy of
   the blocks between the posted head and the stored head. Does the stored head need one copy
   (the writer's disk), or two or more (the writer and replicas on other hosts)? A loss of every
   copy reverts the chain to the posted head.
6. **How the replicas of the serving service follow the writer, and how a replica becomes the
   writer (section 8.4).** The options for the copy: an Aeron stream with a replay from the
   writer's archive, a pull of the log files over HTTP, or a shared store. The options for the
   failover: a Consul lock, a Nomad singleton job, or an operator step.
7. **One executor or two before the serving service stores a block (section 8.3).** The proposal:
   two executors agree on a digest of the block. It closes most of the gap that the validator
   cannot see (an entry that a faulty executor adds), until the stream digest of decision 8. The
   cost: the stored head stops while only one executor runs.
8. **The challenge window.** The serving service keeps the blocks above the attested head. A
   challenger, or a later ZK prover, may need the blocks of an output until its challenge window
   ends (`finalizationWindow`, 1 day). No challenger daemon exists today. Does the floor wait for
   the output to be final, or only for its post?
9. **Old receipts at the cut-over (section 12.3).** The serving service starts empty at the cut.
   The executors then prune the receipts of the blocks before the cut. Must the serving service
   import them from an executor before the first prune, so that `eth_getTransactionReceipt` keeps
   answering for them? Else only the validator's state DB holds them.
10. **The latency target (decision 13, section 10.0).** The user target is "a transaction lands
    within 50 ms of its submit". The benchmark gate of section 10.6 is a p99 commit latency under
    100 ms. Which percentile does the 50 ms bound (p50, p99, or another)? Does "lands" mean the
    canonical seal (the `on-quorum` ack, `kardamom_ingress_seal_latency_seconds`) or the receipt
    at the ingress (`kardamom-load` receipt latency)? The gate of section 10.6 changes with the
    answer.

### 16.2 Closed in review

- **Executor checks.** Each executor checks `keccak256(raw_tx) == canonical_id` and switches
  source on a mismatch (section 8). kardamom-3-00 asked for it.
- **`batcher-outage-past-retention`.** Replay only is right. The second half now proves the disk
  retention with no byte-budget halt (section 13.1, decision 5). dev-32 answered.

### 16.3 Later follow-ups

- **A stream digest (decision 8).** Each boundary carries a running hash of the canonical stream,
  so an executor that switches source detects a divergent member. It changes the boundary frame.
- **The inclusion cost (decision 5).** How the batcher posts a large backlog to L1 after a long
  DA outage.
- **A second Raft group (decision 1).** Only when the staging run shows that one group does not
  carry the target.
- **The external serving service (decisions 7, 10 and 11).** Its own design (#585, sections 8.1,
  8.3 and 8.4).
- **A fleet rebuild that keeps the unposted range (decision 10).** After every member loses its
  state, the serving service still holds the blocks above the posted head. A rebuild could seed
  the sealer from them, not from L1 alone (section 11).
- **The attester restart gap.** The validator resumes from its own state cursor, which can be
  above the attested head (`crates/validator/src/attester/mod.rs:27-29`). The attester must then
  read the withdrawal logs of the blocks in between from the serving service (section 8.3). This
  exists before this design, and the serving service makes the fix possible.
- **The hedged offer (decision 9).** Only if the benchmark shows that the twin copy is the limit
  (section 6.3).
- **Large transactions from unknown senders (decision 9).** A stricter rule for a large
  transaction from a sender whose balance is unknown, and a byte rate for each sender at the
  ingress (section 6.6).

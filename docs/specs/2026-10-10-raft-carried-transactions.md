# Transaction bytes in the sealer's Raft log

- Status: approved by the user on 2026-10-11. Not implemented.
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
  - No byte budget halts the chain. The cluster keeps every unposted entry on disk.
  - The `tx_heads` stream is in scope.
  - A member feeds only its own executor, over IPC. A separate service serves the validator,
    the batcher and external nodes (section 8.1, out of scope).
  - The boundary stream digest is a later follow-up.
- Scope: the ingress size cap, the sequencer offer with bytes, the `tx_heads` stream, the sealer
  state machine and its snapshot, the input of the executors, and the removal of the void path.
  The service that serves the canonical stream to the validator, the batcher and external nodes
  is out of scope.
- Every claim about the current code cites `path:line` on `main` at `0e0c34f25`.

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
  service. That service has its own design (section 8.1).
- The cluster keeps every entry above the posted head, with its bytes, for as long as L1 does
  not hold it. No byte budget stops the chain. The retained frames live in an append-only file
  on disk, not on the heap (section 5.5).
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
| R7 | Nothing is pruned below the posted head: not the retained frames, not the Raft log (#482, #501). |

### 1.3 What the design does not guarantee

- It does not make a transaction durable before the cluster commits it. The twins hold it in
  memory until then. A lane that loses both twins loses its transactions in flight, as today.
  The client gets no ack under the default `on-quorum` policy (section 6.4), so it retries.
- It does not check the signature in the sealer. The executors trust the `sender` field that the
  ingress stamped, as today. The validator checks the signature (section 8).
- It does not remove the leader as the bandwidth ceiling. The leader receives every entry, twice,
  and sends it to each follower (section 10).
- It does not bound the disk of a member while L1 posts stop. The disk grows with the DA lag.
  An alert warns before the disk fills (section 11).
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

 validator, batcher, unpaired executors, external nodes <── serving service (separate design, 8.1)
```

| Step | New behavior |
|---|---|
| Ingress validate | Unchanged: decode, signature recovery, the checks. New: a size cap of 128 KiB for each transaction (section 6.6). |
| Ingress publish | Unchanged: the envelope on `tx_data[lane]`. The ingress offers no transaction data into the cluster. It also publishes a header on `tx_heads` (section 6.5). |
| Sequencer | It reads `tx_data` as today. It offers a full entry: kind 0 with record type 5 (`RT_TX_OFFER`), the guard header, the lane and the envelope (section 6.1). Both twins of a lane offer each entry. |
| Sealer | The record path of today, without the void (section 5.1). The first twin copy wins the dedup. |
| Canonical entry | A relayed frame of record type 6 (`RT_TX`) with the lane and the envelope. |
| Executor | It reads the feed of its paired member. It falls back to the serving service (section 7). |
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
| Egress kind 9 (status) | The 17-byte record-lag tail stays as a fixed field with the "no cursor" values (`best_recorded = u64::MAX`, budget 0, not halted). The byte tail `[retained_bytes:u64]` follows it: 75 bytes. A reader reads 50, 67 or 75 bytes (`crates/cluster-adapter/src/wire/egress.rs:204-229`). An old reader ignores the longer tail. |
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
| Canonical frame (feed, retained file) | 59 B | 237 B | 2,175 B | 8,319 B |
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

**Retained frames.** The egress keeps at least `retention` frames (65,536), and every frame above
the posted head (`SealerEgress.java:420-449`). The frames now carry bytes.

| Retained set | Transfer | 2 KB | 8 KB |
|---|---|---|---|
| 65,536 frames | 15.5 MB | 143 MB | 545 MB |
| One block at 1,000 tx/s (2,000 transactions) | 474 KB | 4.3 MB | 16.6 MB |

- The posted-head floor (#482) keeps every frame above the posted head. The DA-lag guard bounds
  that range in blocks (`DEFAULT_DA_LAG_BUDGET_BLOCKS = 10_000`,
  `CanonicalSealerState.java:128`; 60,000 on staging). In blocks, the bound does not bound the
  bytes:
  - at 10 tx/s and 60,000 blocks: 20 transfers a block, 4.7 KB a block, 284 MB at the budget;
  - at 1,000 tx/s and 60,000 blocks: 474 KB a block, 28 GB at the budget.
- **No byte budget (decision 5).** The cluster keeps every committed entry until L1 holds it. No
  byte budget refuses a record or halts the chain. The block guard of today stays as it is.
- **The retained file.** The retained frames move off the heap into an append-only file on the
  disk of each member:
  - The file is a sequence of segment files. The service thread appends each frame to the
    current segment through a memory-mapped buffer. A helper thread preallocates the next
    segment, so the append needs no system call in the common path.
  - The heap keeps only an index: the canonical index, the block and the file position of each
    retained boundary.
  - The file needs no fsync on each frame. The Raft log is the durable source. After a crash, the
    member truncates the file at the position of its snapshot, and the log replay appends the
    rest.
  - The purge deletes each segment whose last frame is at or below the posted head and below the
    `retention` floor.
  - The replays of the feed (section 7.3) read the file.
  - A member that a peer seeds gets the file with the snapshot.
- **Disk full.** The helper thread cannot preallocate the next segment, or the Raft log cannot
  write. The member logs a fatal error and stops. It never serves a partial file. Section 11
  gives the effect and the alert.
- **Out of scope: the inclusion cost.** A long DA outage leaves a large backlog above the posted
  head. How the batcher posts that backlog to L1, and at what cost, is a future optimisation. This
  spec does not design it. The batcher posts by its rules of today (#569).
- **Gauges.** The status frame (egress kind 9) gets the tail `[retained_bytes:u64]`: the bytes of
  the retained file above the posted head. The ingress exports it as
  `kardamom_ingress_cluster_retained_bytes`, beside the block gauges
  (`crates/ingress/src/metrics.rs:33-41`). Each member exports the free space of its disk as
  `kardamom_sealer_disk_free_bytes`. The alerts are in section 11.

**The snapshot (version 12).** `CanonicalSealerState.java:1474-1538` and
`SealerEgress.java:558-610` change as follows:

| Section | v10 (written today) | v12 |
|---|---|---|
| Dedup window, sender map, origins, remote peers | unchanged | unchanged |
| Void ledger (v6) | present | gone |
| Ordering window, posted head, seed (v8 to v10) | unchanged | unchanged |
| Recorded cursors (v11, never written) | absent | absent |
| Retained frames | `TxRef` frames, 59 B | none: the floor and the head position of the retained file, and its index |

- There is no envelope store and no waiting-reference section (section 5.2). The ordering
  window holds at most 20 records with their envelopes.
- The snapshot size is the state of today, minus the void ledger, plus the index of the retained
  file. It does not grow with the DA lag. The snapshot forces the mapped segments to disk up to
  its position.
- The heap holds the same data. The sealer runs with `-Xmx384m` and 1,024 MB of job memory
  (`deploy/cluster/nomad/cluster.nomad.hcl:362`, `:413`). The accepted starting value is a 2 GiB
  heap. The retained frames do not count against it, and there is no envelope store, so the heap
  has a margin. The benchmark measures the snapshot time and the GC pauses with a full dedup
  window, and tunes the heap.
- **Precondition: the host memory.** On staging, each dedicated host (8 cores) runs sealer
  member `i` (cpuset 0-3) and executor `i` (cpuset 4-7) as two node containers. ded-3 took
  sealer-0 and executor-0 in infra #51 (`kardamom-infra`,
  `ansible/inventories/staging/hosts.ini:27-66`). The infra repository states no memory size for
  these hosts, and the node containers set no memory limit. So PR 13 does not deploy until the
  operator confirms that each dedicated host holds the 2 GiB sealer heap, the sealer page cache
  for the log and the retained file, the executor's 1,536 MB job
  (`deploy/cluster/nomad/executor.nomad.hcl:325-326`) and its MDBX map, with margin. The deploy
  preflight checks the host memory against a stated minimum.
- **Precondition: the disk.** The operator also states the disk size of each member. Section
  10.0 gives the growth rate while no post lands.
- **The snapshot pause.** Every member takes the snapshot at the same log position
  (`cluster/sealer-service/README.md:513`), so a long write pauses the service thread of every
  member at once. The retained frames are not in the snapshot, so its size does not grow with the
  DA lag. The bound is 2 s. The benchmark measures it (PR 12).

**The Raft log and its purge (#501).** The log now holds the full entries, two twin copies of
each. The purge rule does not change (`PurgePlanner.java:65-73`). The log keeps at least 3 snapshot intervals (15 minutes at
300 s), and everything above the posted head.

| Log on each member | Transfer | 2 KB | 8 KB |
|---|---|---|---|
| Per transaction (2 twin copies, section 4.5) | 0.58 KB | 4.6 KB | 17.2 KB |
| Per block at 1,000 tx/s | 1.16 MB | 9.2 MB | 34.5 MB |
| 15 minutes at 1,000 tx/s | 522 MB | 4.1 GB | 15.5 GB |
| At the posted-head floor | no byte bound: the log above the posted head grows with the DA lag, plus the 15 minutes above. Section 10.0 gives the rate. |

- The retained file holds the canonical frames, and the log holds the inputs: two twin copies.
  So the disk holds the bytes about three times while no post lands, plus the kept snapshots
  (3 by default). The benchmark measures it.

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
| Frames | The egress frames, unchanged: kind 1 (relayed), kind 2 (boundary), kind 9 (status), and the replay answers 3, 4, 11. The feed adds kind 16 (`FEED_GAP`). One codec for the feed and the egress. |

- The service thread runs on every member, the leader and the followers alike. Each member applies
  the same log, so each member's feed carries the same frames (R3).
- An egress offer is a no-op on a follower (`cluster/sealer-service/README.md:163`). The feed is
  not an egress offer. It is a member-local publication, so a follower publishes it too.

### 7.2 The service thread never blocks

- The service thread writes each frame into a one-to-one ring buffer (Agrona
  `OneToOneRingBuffer`), at the point where it retains the frame
  (`SealerEgress.java:420-449`). The write never waits.
- The feeder thread reads the ring and offers each frame to the publication. A back-pressured
  offer makes the feeder thread wait. The service thread does not wait (R4).
- **Bound.** `kardamom.cluster.feedRingBytes`, member-local. Proposed default: 64 MiB, which is
  about 283,000 transfer frames or 8,000 frames of 8 KB.
- **Ring full.** The service writes a live frame only while the free space holds the frame plus a
  64-byte reserve. When it does not, the service writes one gap marker into the reserve and marks
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

### 7.3 The catch-up path

- The executor sends a replay request on the feed control stream (1007):
  `[kind=1][from_index:u64][from_block:u64]`, the layout of ingress kind 1.
- The feeder thread passes the request to the service thread through a second one-to-one ring.
  The service thread polls it in `doBackgroundWork`. The request is member-local: it is not log
  input, and it changes no replicated state.
- The service answers as it answers an egress replay today (`SealerEgress.java:201-253`):
  - a cursor past the head: `REPLAY_AHEAD`;
  - a cursor below the retained floor: `REPLAY_UNAVAILABLE`;
  - else the retained frames from the cursor, then `REPLAY_DONE`.
- The service walks the retained frames from the retained file (section 5.5) into the ring, at
  most 4 MiB in each duty cycle. It does not write live frames during the walk. The file already
  holds them, so the walk reaches the head and the feed turns `live`. A full ring pauses the walk to the next duty cycle.
  It does not gap the feed.
- The retained frames are the catch-up source. The Raft log is not: the log holds the inputs
  (both twin copies, and the refused records), not the canonical entries. A canonical entry
  exists only after the state machine orders it. The retained window holds every frame above the posted head and at
  least 65,536 frames, on every member, through the snapshot and the log replay (section 5.5).
- `REPLAY_UNAVAILABLE` sends the executor to the peer checkpoint, as today
  (`crates/engine/src/bin_support.rs:529-576`).

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

## 8. The consumers

| Consumer | Reads today | Reads after |
|---|---|---|
| Executor `0..2` | Cluster egress (references) + 8 `tx_data` lanes + ingress archives; votes (`crates/executor/src/bin/kardamom-executor/main.rs:494-513`, `:339`) | Its member's feed; the serving service as the fallback. No `tx_data`, no archive, no vote. It checks `keccak256(raw_tx) == canonical_id` for each `RT_TX` entry (about 1 µs for a transfer). On a mismatch it does not execute the entry and does not halt: it drops that source, replays from the other source at the same index, counts `kardamom_executor_source_mismatch_total{source}`, and an alert fires. |
| Elastic executor | As above | The serving service (section 8.1). |
| Validator | Cluster egress + `tx_data` + ingress archives; votes (`crates/validator/src/bin/kardamom-validator/wiring/startup.rs:241-286`, `wiring/run.rs:170-178`) | The serving service (section 8.1). It checks `keccak256(raw_tx) == canonical_id` and that the signature recovers the sender (`crates/exec-core/src/stateless.rs:253-272`). A failed check halts it with `validator_divergence`. It does not trust the sealer's bytes. |
| Batcher | Cluster egress + `tx_data` + ingress archives; votes; the rebuild from `kardamom_getBlockRefs` and the archives (`crates/batcher/src/live/run.rs:292-357`, `:641-669`) | The serving service (section 8.1). It packs the KAR1 blocks from the `raw_tx` of each `RT_TX` entry. The spool, its group and cursor rules do not change. |
| Ingress observer | Full relayed frames (`crates/ingress/src/cluster.rs:173-174`) | The cluster egress: kind-15 frames, no bytes (section 6.4). |
| Ingress receipt fallback | The receipt cache, then the executor state DB over HTTP (`crates/ingress/src/proxy/mod.rs:439-463`) | Unchanged. |
| State mirror | `tx_receipts` only (`crates/state-mirror/src/main.rs:137-146`) | Unchanged. |
| Notifier | `tx_status`, `tx_receipts`, `tx_errors` (`crates/notifier/src/taps.rs:56-74`) | Unchanged. |
| Sequencer | `tx_data`; publisher-only session (`crates/sequencer/src/outbound/cluster.rs:121-172`) | `tx_data`, as today. It offers the full entry (section 6.1). |
| da-watcher, L1 follower | `l1_blocks`, `tx_deposits` | Unchanged. |

### 8.1 Out of scope: the external serving service (separate design)

The members serve no consumer of the bytes except the co-located executor, over IPC (decision 7).
Neither the leader nor a follower serves the validator, the batcher, unpaired executors or
external nodes. A separate service serves them. It has its own design. This spec states only what
it needs from that service:

- **The stream.** The canonical stream in canonical order: kind 1 (all record types, with the
  envelope for record type 6), kind 2 (boundary) and kind 9 (status). The frames are byte for byte
  the frames of a member feed (R3), so a consumer uses one codec for both.
- **The replay.** A replay from a cursor `(index, block)`, with the answers of today: kinds 3, 4
  and 11 (`SealerEgress.java:201-253`).
- **The floor.** It holds every frame above the posted head (R7), so the batcher resumes from it
  (section 8.2).
- **What it reads.** The canonical stream that the members emit. It does not open a cluster
  egress session, and no member opens a feed for it. Its source is part of its own design.
- **The order of work.** The validator and the batcher read it after the cut-over, so the service
  runs before PR 13 deploys.

### 8.2 The batcher's resume sources

1. The spool: unchanged.
2. The replay from the cursor, now from the serving service. It holds every frame above the
   posted head (section 8.1). Every member also keeps those frames: the retention floor
   (`SealerEgress.java:420-449`), the retained file (section 5.5) and the log purge floor
   (`PurgePlanner.java:65-73`).
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
  `tx_data` archives. The sealer's Raft log and snapshots hold the bytes of every unposted block.
- Unaffected: `l1_blocks` and the L1 follower, the DA-lag flush of the batcher (#569), the
  batcher's resume from the contract, its spool and cursor formats.

## 9. What goes and what stays

### 9.1 Removed

| Area | Item | Where |
|---|---|---|
| Sealer | `VoidLedger`, `onVoidRequest`, `appendVoid`, kind 6, `RT_VOID` | `VoidLedger.java`, `CanonicalSealerState.java:919-933`, `SealerClusteredService.java:1006-1023` |
| Sealer | `RecordedCursors`, kind 9, the record-lag guard, egress kind 13, the status tail | `RecordedCursors.java`, `CanonicalSealerState.java:1312-1343`, `SealerClusteredService.java:899-927`, `SealerEgress.java:469-495` |
| Sealer | Settings `voidVoters`, `voidWindow`, `recordLagBudget`, `MAX_OPEN_VOTES` | `ClusterNode.java:44`, `:94-101`, `:143-144`, `:608-612`, `:685-694` |
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
| #482, #501 | Posted-head floor, log purge | Kept. The retained file follows the floor (section 5.5). |
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

The user set the target: **up to 5,000 tx/s, with mixed sizes**. The mix below is an example
for the arithmetic. The benchmark uses the same mix, plus a pure 8 KB row as the worst case.

- Example mix: 80 % transfers (110 B raw), 15 % 2 KB calls, 5 % 8 KB calls. The mean raw size is
  about 805 B. The log input is about 2.0 KB a transaction (two twin copies), and a canonical
  frame about 932 B.

| At 5,000 tx/s | Transfers only | Example mix | 2 KB only | 8 KB only |
|---|---|---|---|---|
| Out of one sequencer replica (2,500 tx/s) | 0.72 MB/s | 2.5 MB/s | 5.7 MB/s | 21.5 MB/s |
| Log input at the leader (2 twin copies) | 2.9 MB/s | 10.1 MB/s | 23 MB/s | 86 MB/s |
| Replication out (2 followers) | 5.8 MB/s | 20.1 MB/s | 46 MB/s | 172 MB/s |
| Egress out (2 ingresses, kind-15 frames of 42 B) | 0.42 MB/s | 0.42 MB/s | 0.42 MB/s | 0.42 MB/s |
| Leader out, total | 6.2 MB/s | 20.5 MB/s | 46 MB/s | 173 MB/s (1.38 Gbit/s) |
| Log written in 15 minutes, each member | 2.6 GB | 9.0 GB | 21 GB | 78 GB |
| Retained bytes for each 2 s block | 2.4 MB | 9.3 MB | 22 MB | 83 MB |
| Disk growth while no post lands, each member (log + retained file) | 4.1 MB/s | 14.7 MB/s | 34 MB/s | 128 MB/s |
| The same, in one hour | 15 GB | 53 GB | 122 GB | 460 GB |
| The same, at the block guard (10,000 blocks, about 5.6 hours) | 82 GB | 294 GB | 676 GB | 2.6 TB |
| Raw bytes to post to DA | 0.55 MB/s | 4 MB/s | 10 MB/s | 41 MB/s |

Findings from the arithmetic, for the benchmark to confirm:

- **Network.** The leader sends only the replication and the 42-byte kind-15 frames. The mix
  needs about 20.5 MB/s out of the leader, inside a 1 Gbit/s link. A stream of only 8 KB calls
  needs 1.38 Gbit/s, above a 1 Gbit/s link: the worst case needs 10 GbE. The leader also receives
  86 MB/s at 8 KB only. The serving service is not in these numbers (section 8.1). Only the
  staging run can measure the network (section 10.6).
  - The evaluation of option B gives 30 MB/s and 2.05 Gbit/s. Those numbers include the full
    egress to the validator and the batcher. Decision 7 removes that egress.
  - Option A would need 12 MB/s and 0.71 Gbit/s: one copy in the log. The hedge of section 6.3
    brings option B close to that.
- **The twin copy.** Option B puts each transaction in the log twice. So the log bytes, the
  replication and the log disk are about 2 x those of option A. The retained frames and the
  feed do not change: they hold canonical frames.
- **The sequencer.** Its input does not change: it already reads the full envelope. Its output
  grows from about 0.23 MB/s to 2.5 MB/s for each replica at the mix, and to 21.5 MB/s at 8 KB
  only. Two lanes share a node: about 43 MB/s (0.34 Gbit/s) at 8 KB only.
- **The disk retention.** No byte budget halts the chain (decision 5). While no post lands, the
  disk of each member grows by the log and the retained file: about 53 GB an hour at the mix, and
  about 460 GB an hour at 8 KB only. The disk size of a member sets how long it rides out a DA
  outage. The alert of section 11 gives the time to a full disk.
- **The block guard.** The DA-lag guard in blocks stays as today. At 10,000 blocks of 2 s it
  refuses user records after about 5.6 hours without a post. In that time the disk of each member
  grows by about 294 GB at the mix. On staging (60,000 blocks, about 33 hours) it is about 1.76 TB.
  So the disk can fill before the block guard refuses, at the larger sizes.
- **The heap.** The retained frames live in the append-only file of section 5.5, not on the heap.
  The heap keeps an index of positions. So the DA lag does not grow the heap.
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

- The service thread copies each envelope once into a canonical frame, once into the retained
  file and once into the feed ring. That is about 3 x the frame bytes of `memcpy` for each
  transaction: under 1 µs for a transfer, about 3 µs for an 8 KB call.
- The service thread also reads the losing twin copy, and drops it at the dedup step. The
  copy is not parsed past the guard header.
- The consensus module and the archive of the leader handle `rate x log bytes`. This cost is in
  Aeron, not in our code. The benchmark measures it.

### 10.4 Disk on each member

See section 5.5:

- The log grows by `rate x log bytes`. The purge keeps 15 minutes and the range above the posted
  head.
- The retained file grows by `rate x frame bytes`. The purge keeps the range above the posted head
  and the `retention` floor.
- No byte budget bounds the range above the posted head. While no post lands, the disk grows at
  the rates of section 10.0: about 53 GB an hour at the mix.
- The snapshot holds the state of today and the index of the retained file. It does not grow
  with the DA lag.

### 10.5 The feed

- The feed is IPC only, so it costs no network. Its cost is the copy into the ring
  (section 10.3) and the read of the retained file during a replay.

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
| Retained file: write rate, replay read rate, disk growth while the batcher is stopped | the file size, `iostat`, `kardamom_ingress_cluster_retained_bytes` |
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
- The 100 ms bound is this spec's proposal. Today's receipt p99 is 47 ms at the edge
  (`docs/specs/2026-08-16-pipeline-cost-model.md`, "Gas throughput and latency").

## 11. Failure modes

| Failure | Effect | Recovery |
|---|---|---|
| A follower dies | Quorum holds. Its executor's feed is silent. | The executor reads the serving service after 6 s. The member restarts, replays its log, and the executor returns to the feed. |
| The leader dies | An election. Full entries in flight can be lost (at most once). | The sequencer re-offers them, with their bytes, from its unconfirmed ledger. That is the only re-offer path. The dedup drops the copies that did commit. The executors on the feeds of the surviving members see no gap. |
| One sequencer twin dies | The other twin already offered every entry with its bytes. | None needed. No gap, no loss, as today. |
| An ingress dies after its `tx_data` publish | The sequencer holds the transaction. | None needed. Nothing is lost. |
| An executor is slow | Its feed ring fills. The service thread does not wait. Other members and executors do not notice. | The feed gaps. The executor asks for a replay from its cursor and catches up from the retained window. |
| An executor restarts | None for the cluster. | It replays from its state DB cursor through its feed (section 7.4). |
| An executor is behind the floor | Its cursor is below every member's retained floor. | `REPLAY_UNAVAILABLE`, then a peer checkpoint, as today. |
| A member is far behind or below the purge point | Its feed lags or is idle. | The join watchdog reseeds the member from a peer (`docs/failure-modes.md:229-240`). Its executor reads the serving service meanwhile. |
| A network partition isolates a member and its executor | The minority member cannot commit. The executor sees no new frame. | The executor switches to the serving service, if it reaches it. Else it waits. It never executes a frame that the majority did not commit, because the member applies only committed entries. |
| A sequencer lane is down (both twins) | No entry for that lane. The transactions in flight on `tx_data` and in the parked buffers are lost, as today. | The lane stalls until a replica returns. The other lane continues. An `on-quorum` client gets no ack, so it retries. |
| A future-nonce flood | Senders fill the parked buffer of a replica with large future-nonce transactions. Nothing enters Raft. | The byte bound evicts the furthest future nonce (section 6.2). The other lanes and Raft do not notice. |
| Receipts are slow | The unconfirmed ledger of a replica fills with bytes. | The bound back-pressures that lane (section 6.2). |
| A snapshot restore | The member restores its state and the index of its retained file. | It truncates the file at the snapshot's position and replays the log after the snapshot. Its feed serves replays from the file. |
| All members restart | No commit until a quorum returns. | Each member restores its snapshot and replays its log. The executors reconnect to the feeds and replay. The sequencers re-offer. |
| All members lose their state | The retained frames and the log are gone. | The fleet rebuild from L1 at the posted head (`docs/runbooks/sealer-fleet-rebuild.md`). The unposted range is lost, as today. |
| L1 posts stop for a long time | The members keep every entry above the posted head on disk: the log and the retained file grow at the rates of section 10.0. No byte budget refuses records. The block guard refuses user records at its block budget, as today. | The batcher posts again. The purge then frees the disk. How the batcher posts a large backlog is out of scope (section 5.5). |
| The disk of a member fills | The member cannot write its log or its retained file. It logs a fatal error and stops (section 5.5). All members retain the same bytes, so disks of one size fill at about the same time. When a quorum stops, the cluster commits nothing: the chain stops. | The alert `KardamomSealerDiskLow` fires first: free disk under 25 %, or under 6 hours at the current growth rate (`kardamom_sealer_disk_free_bytes` and its derivative). The operator restores the posts or adds disk. A stopped member restarts, truncates its retained file at its snapshot, and replays its log. |
| A member's feed bytes differ from the leader's | A bug: R3 is broken. | An executor whose entry fails its hash check drops that source and replays from the other source, with an alert (section 8). The validator checks every canonical entry it reads. A boundary carries no digest of the stream. A stream digest is a later follow-up (section 16.3). |
| A feed ring fills | The member writes a `FEED_GAP` marker (section 7.2). | The executor asks for a replay at once and stays on the feed. |

## 12. Migration

### 12.1 Formats (`docs/formats.md`, `formats.toml`)

| Format | Today | After the cut-over |
|---|---|---|
| `sealer-ingress-kinds` | writes 8, reads 0..9 | writes 8, reads 0..8. No new ingress kind: record type 5 rides kinds 0 and 3. Kinds 6 and 9 drop as unknown. |
| `sealer-egress-kinds` | writes 12, reads 1..13 | writes 16, reads 1..16. Kind 13 is never sent. Kind 14 is not used. Kind 16 is sent on the feed only. The status frame is 75 bytes. |
| `sealer-record-types` | writes 4, reads 0..4 | writes 6, reads 1..6. Types 0 and 4 are never written. |
| `sealer-snapshot` | writes 10, reads 1..11 | writes 12, reads 10..12. A later release sets `reads_min = 12`. |
| `aeron-stream-records` | `TxEnvelope` frozen | Unchanged. The envelope travels in its frozen layout. |
| `discovery-record` | `Topic::ExecTxs` | `Topic::SealerFeed` added. `ExecTxs` is removed only after no node advertises it (`crates/log/src/discovery/record.rs:279-296`). |
| `sealer-feed` (new) | none | The feed frames are the egress kinds. The feed control request is the kind-1 layout. |
| `tx-heads` (new, PR 11) | none | Frozen layout of `TxHeader`. |
| `state-db`, `batcher-spool`, `batcher-cursor` | unchanged | Unchanged. |

- Each change that the base cannot read gets a `[one_way.<id>]` and a `[coordinated.<id>]`
  waiver with its reason.
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
   - the cursor of every consumer equals the sealer's canonical count: each executor (feed or
     egress), the validator and the batcher. Then no consumer holds a `TxRef` past the cut that it
     cannot resolve after the floor moves;
   - no void vote is open (`cluster VOID-VOTE` lines all have `result=DECIDED`).
5. Take a sealer snapshot (v10). Copy it, and take a checkpoint of each executor and of the
   validator, to the backup volume. Keep these copies until the first v12 snapshot is one day
   old. They are the input of the rollback of section 12.5.
6. Stop the writers: the ingresses and the sequencers.
7. Purge the sealer job. Start the new sealer release (decision version 3). It restores the v10
   snapshot:
   - it drops the void ledger, which holds no open vote;
   - it drops the retained `TxRef` frames, which are all at or below the posted head, so the
     floor moves to the head.
8. Turn the flags on. Start the executors, the validator and the batcher; they resume at the head
   with no `TxRef` frame to resolve. The validator and the batcher now read the serving service
   (section 8.1), so it runs before this step. Then the sequencers, then the ingresses. Unpause.

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
 PR1 fragmentation ──┬──> PR4 wire ──┬──> PR5 retained file ──┬──> PR6 record path, v3, v12 ──┐
 PR2 bench tooling   │               │                        └──> PR7 member feed ───────────┤
 PR3 size cap        │               ├──> PR8 sequencer full entry ───────────────────────────┤
 PR11 tx_heads       │               ├──> PR9 ingress observer ───────────────────────────────┤
                     └───────────────┴──> PR10 engine reader (after PR7) ─────────────────────┤
                                                                                              ▼
   PR2, PR3, PR6, PR7, PR8, PR9, PR10 ──> PR12 benchmark run ──> PR13 cut-over ──> PR14 removal
                                     (PR13 also needs the serving service, section 8.1)
```

| PR | Title | Depends on |
|---|---|---|
| 1 | cluster: fragmented session messages up to 1 MiB | none |
| 2 | bench: calldata workload, seal latency, snapshot time | none |
| 3 | ingress: cap a transaction at 128 KiB | none |
| 4 | wire: full entry, canonical entry, kinds 15 and 16, status tail | 1 |
| 5 | sealer: retained frames in an append-only file | 4 |
| 6 | sealer: record path with bytes, decision version 3, snapshot v12 | 4, 5 |
| 7 | sealer: the member feed over IPC | 4, 5 |
| 8 | sequencer: offer the full entry, with byte bounds | 1, 4 |
| 9 | ingress: observer reads kind 15 | 4 |
| 10 | engine: read `RT_TX` from the feed or the serving service | 4, 7 |
| 11 | ingress: publish `tx_heads` | none |
| 12 | bench: run the matrix and gate on the target | 2, 3, 6, 7, 8, 9, 10 |
| 13 | release: cut-over to bytes in the Raft log | 12, the serving service |
| 14 | cleanup: remove the void, the join and the archives | 13 |

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
  runbook of 12.3 and its rehearsal case, the rollback floor, `KardamomSealerDiskLow`, the chaos
  cases of 13.1. The pair mount of the staging hosts is a `kardamom-infra` change to the
  node_containers role, merged before the reset of 12.2.
- Depends on: PR 12, and the serving service of section 8.1 in place.
- Acceptance: `cutover-rehearsal` passes in the container cluster. The staging reset passes
  `just smoke`, the canary and one chaos shard.

**PR 14. cleanup: remove the void, the join and the archives.**

- Scope: everything in section 9.1. `reads_min` of the snapshot goes to 12 in the release after.
- Acceptance: no reference to the removed items is left. The chaos shards pass.

Rules to check in each pull request: R1 (comments carry no phase or spec names), R5 (the feeder,
the ring and the retained-file helper each have one owner), R6 (the feed is an associated type of
the reader's wiring, not `dyn`), R9 (parse each frame once at the boundary), R12 (checked
arithmetic for the byte counters and the wire lengths), R14 (one codec and one ingest path for the
feed and the serving service), R16 (the release drain stays a one-step loop body).

### 13.1 Chaos cases

| Case | Change | Assertion |
|---|---|---|
| `archive-driver-loss`, `archive-tx-data-wipe`, `archive-corruption` (`crates/chaos/src/cases/archive.rs`) | Remove the `tx_data` parts. Keep the `tx_deposits` archive loss. | The deposit path recovers. |
| `pipeline-blackout-recover` (`crates/chaos/src/cases/coordinated.rs:104-150`) | Drop the void-decision count. | Every acknowledged transaction has a receipt after the blackout. No `VOID` line exists. |
| `hard-executor`, `graceful-executor` (`crates/chaos/src/cases/component.rs:7-34`, `exec_stream.rs`, `recorded_cursor.rs`) | Drop the stream and cursor checks. | The restarted executor resumes through its feed (`input_source="feed"`) with no gap. |
| `batcher-outage-past-retention` (`crates/chaos/src/cases/l1/outage.rs`) | The first half stays: freeze past the floor and a snapshot, thaw, recover from the spool and the replay of the serving service. The second half proves the disk retention: freeze the batcher with SIGSTOP and load until the retained file holds more than 1 GiB above the posted head; thaw. | First half: the rebuild line (`crates/chaos/src/cases/l1/batcher.rs:22-23`) count does not grow. Second half: no `DA_LAG_REJECT` while the block budget is not passed; `kardamom_halt{cause="da_lag"}` stays 0; the heap does not grow with the retained bytes; after the thaw the batcher replays, posts, and the retained file shrinks to the floor. Both halves: L1's record is contiguous; root parity holds. |
| `sealer-fleet-total-wipe-recover` (`crates/chaos/src/cases/fleet/sealer_wipe.rs`) | None. The rebuild mark does not change. | As today. |
| `validator-join` | Rename to `validator-catchup`. It runs once the serving service exists (section 8.1). | The validator catches up through the serving service. |
| `feed-member-kill` (new) | Kill sealer member 1 under load. | Executor 1 switches to the serving service within 10 s and back after the restart. No gap. The executor roots equal the validator's. |
| `feed-slow-executor` (new) | SIGSTOP executor 2 longer than the ring holds, then SIGCONT. | The commit latency of the other members does not rise. Executor 2 catches up through a feed replay. |
| `leader-kill` (new) | Kill the leader under `on-quorum` load with 8 KB transactions. | The sequencers re-offer from their unconfirmed ledgers. Every acknowledged transaction has a receipt. No transaction is ordered twice. |
| `sequencer-byte-cap` (new, small `--parked-bytes`) | Flood lane 0 with large future-nonce transactions from new keys. | The parked bound of lane 0 holds and evicts. No flood transaction enters the Raft log. The commit latency of lane 1 does not rise. |
| `sequencer-twin-kill` (new) | Kill one twin of lane 0 under load with 8 KB transactions. | No gap and no loss. Every acknowledged transaction has a receipt. |
| `cutover-rehearsal` (new, container cluster only) | Run the drained cut-over of section 12.3 under load, from the release before PR 13. | The posted head reaches `D`; every consumer cursor equals the canonical count; the new members restore the v10 snapshot; the chain continues; root parity holds. |
| `feed-driver-restart` (new) | Restart the aeron system job on executor node 1 under load. | Member 1's commit latency does not rise. The feeder reconnects. Executor 1 returns to its feed with no gap. |
| `sealer-disk-full` (new, container cluster only) | Fill the disk of member 2 while the batcher is stopped. | `KardamomSealerDiskLow` fires before the disk is full. Member 2 stops with a fatal error and serves no partial file. Quorum holds on members 0 and 1. After the operator frees space, member 2 restarts and catches up. |
| Planned and dropped | `exec-peer-fetch`, `record-lag-halt`, `validator-exec-archive-catchup`, `exec-archive-prune` of the archived-data spec | Not built. |

## 14. Docs that change with the code

- `docs/failure-modes.md`: the sealer section (the record with bytes, the retained file and a
  full disk), the void section goes, the executor section (the feed), the sequencer section (the
  full entry, the byte bounds, the twin copy), the ingress section (the size cap, the ack
  policies), the batcher resume sources.
- `cluster/sealer-service/README.md`: kinds, record types, settings, snapshot v12, the feed, the
  decision version table (version 3).
- `docs/aeron-discovery.md`: the `sealer_feed` topic and streams; `exec_txs` and the `tx_data`
  archive topics go.
- `docs/observability.md`: the new metrics of sections 5.5, 6.2, 6.6, 7.5 and 10.6.
- `docs/l1-data-path.md`: the batcher reads bytes from the serving service.
- `deploy/alerts.yml`: `KardamomSealerDiskLow`.
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
- **The catch-up from the member's Raft log.** The log holds the inputs, not the canonical entries
  (section 7.3).
- **`ExecTxRecord` for the remote consumers.** The serving service uses the canonical frames of
  the feed, with replay (sections 8.1 and 9.2).
- **A byte budget that halts the chain.** A byte budget on the retained bytes would refuse user
  records while L1 posts stop. The user ruled it out (decision 5). The cluster keeps the data on
  disk instead.
- **The members serve the remote consumers.** The leader's egress, or a UDP feed from each
  follower, could carry the bytes to the validator, the batcher and external nodes. The user ruled
  it out (decision 7). A member feeds only its own executor. A separate service serves the rest
  (section 8.1).

## 16. Decisions of the user (2026-10-11)

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
5. **No byte-budget halt.** The chain does not halt on a byte budget. The cluster keeps the
   committed data for as long as L1 does not hold it. The retained entries move off the heap to
   the append-only file on disk (section 5.5). A full disk stays a failure mode, with an alert
   (section 11). How the batcher posts a large backlog (the inclusion cost) is a future
   optimisation, out of scope.
6. **`tx_heads`.** In scope (PR 11). The stream is header-only: the sender, the nonce and the
   hash, with the fixed fields of section 6.5. It carries no `raw_tx`. Under decision 9 the
   sequencer still needs `tx_data`; section 16.1 asks which reader uses `tx_heads`.
7. **Who serves the bytes.** A member feeds only its own co-located executor, over IPC. The
   leader and the followers do not serve the validator, the batcher or external nodes. A
   separate service serves them, with its own design (section 8.1). This closes the question
   "remote consumers from a follower": no.
8. **The stream digest.** Not in this program. It is a later follow-up (section 16.3).
9. **The input path: option B.** After a separate evaluation, the sequencer carries the full
   transaction into the cluster (sections 4.1, 5.1 and 6.1). There is no envelope path from the
   ingress into the cluster and no join. The twins stay, and the log holds two copies. A hedged
   offer is a later option, only if the benchmark needs it (section 6.3). The ingress caps a
   transaction at 128 KiB (section 6.6). The reasons are in section 15: the DDoS surface and the
   re-offer amplifier of option A, its replicated wait states, and no real latency gain. Every PR
   of section 13 can start.

### 16.1 Still open

1. **A reader for `tx_heads`.** Under option B the sequencer must read the bytes on `tx_data`, so
   `tx_heads` does not replace it. The evaluation proposed to drop `tx_heads`; the user keeps it
   in scope. Which service reads it: for example the hedged sequencer, the notifier, or a future
   lane resize? Until the answer, PR 11 builds the publisher only.
2. **The ingress observer.** The leader still sends a 42-byte kind-15 frame for each transaction
   to each ingress, for the `on-quorum` ack (section 6.4). It carries no bytes. Is this inside
   decision 7, or does the ack move to the serving service too?
3. **Superseded work.** Close #543, #542, #554 and #570 as superseded (section 9.2)?

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
- **The external serving service (decision 7).** Its own design (section 8.1).
- **The hedged offer (decision 9).** Only if the benchmark shows that the twin copy is the limit
  (section 6.3).
- **Large transactions from unknown senders (decision 9).** A stricter rule for a large
  transaction from a sender whose balance is unknown, and a byte rate for each sender at the
  ingress (section 6.6).

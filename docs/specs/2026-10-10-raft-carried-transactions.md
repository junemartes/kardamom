# Transaction bytes in the sealer's Raft log

- Status: approved by the user on 2026-10-11. Amended for the user decision of 2026-10-17
  (decision 10, section 16): the sealer keeps no retained file, and the serving service keeps
  the transactions for the batcher and the attester. Amended for the owner's upgrade rule of
  2026-10-18 (decisions 19 and 20, #661): every deploy is backwards compatible, and an upgrade
  transaction turns on each breaking change. The cut-over and decision version 3 activate by an
  upgrade transaction, not by a chain reset or a coordinated restart (sections 5.4 and 12).
  Partly implemented (section 13).
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
  - Staging moves by a chain reset. Production uses the drained cut-over. (Replaced by decision
    20: both networks move by an upgrade transaction, section 12.)
  - The starting limits of decision 4 are accepted. The benchmark tunes them.
  - No byte budget halts the chain. The serving service keeps every unposted entry on disk
    (decision 10).
  - The `tx_heads` stream is in scope.
  - A member feeds only its own executor, over IPC. A separate service serves the validator,
    the batcher and external nodes (section 8.1, out of scope).
  - The boundary stream digest is a later follow-up.
- Decided by the user on 2026-10-17 (decisions 10 to 18):
  - The sealer keeps no retained file. Its Raft log and its snapshots are its only record.
  - The serving service tails the sealer members (a follower first) for the transactions, and
    reads the receipts from the executors. It keeps the append-only log of the transactions above
    the posted head for the batcher and the attester (decision 16 replaces "only from the
    executors" of decision 10).
  - The member feed replays from the member's own Raft log, from a snapshot.
  - The executors keep their receipts only until the serving service stores them (section 8.3).
  - Two read tiers: the internal services read the executor (hot); the external readers read
    the serving service, read replicas of one log (cold). The ingress asks the serving service
    for a receipt that the executors pruned (section 8.4). (Decision 27 replaces the replicas
    with independent instances.)
  - The notifier is removed for now: no WebSocket status feed and no webhooks (section 8.4).
  - The target: a transaction lands within 50 ms of its submit. The ideal is a p99 of at most
    20 ms from the ingress receipt of the submit to the executor's receipt emission (section
    10.0).
  - The answers to the open questions of the amendment (decision 14): one executor is enough; the
    serving service keeps each attested range until its challenge window ends; it imports the
    existing receipts at the cut-over; the read interface, the copies and the failover go to the
    design of #585.
  - The sealer appends at sync level 0 and forces its log every 1 s, so it knows its durable
    position. The serving service exposes only data up to the majority durable position, which it
    polls from the admin endpoint of each member. The sealer needs
    nothing from the serving service for correctness. After a power loss on a majority, the
    cluster continues from its surviving log, and the executors that ran past it roll back by
    themselves. A transaction is confirmed when the batcher posted it to the DA layer (decisions
    15 and 16, sections 5.5 and 11.1).
- Decided by the owner on 2026-10-18 (decisions 19 to 33, #661):
  - Every deploy is backwards compatible. A breaking change ships behind a chain feature flag
    that is off. An L1 upgrade transaction turns the flag on. After the activation on every
    network, a later release removes the old logic.
  - The cut-over to carried transactions is the activation of one chain flag, the carry flag. It
    replaces `kardamom.cluster.carryBytes` and `--carry-bytes`. The old path stays until the
    activation on every network (section 12).
  - Decision version 3 of the sealer also activates by an upgrade transaction (#662, PR #666).
  - No coordinated restart, quiesce check, cluster purge, waiver or format gate is a deploy path.
    Compatibility fixtures (#663) and a gate check of the active flags of the chain (#664) replace
    the format gate. There is no mixed-version chaos shard.
  - The switch of the carry flag (decision 21): feature id 3. The sealer marks the switch in the
    ordered stream, and the executors switch at the next block boundary. Version 3 must be active
    first.
  - There is no nonce release. A skipped transaction uses its nonce at the executor, with
    `FEATURE_SEALER_V3` (decision 22, #676).
  - A feature activation works both ways: an upgrade transaction can also turn a feature off
    (decision 23, #670). The sequencer is trusted.
  - A history key (generation, term) on each block replaces the rollback record of #655
    (decision 24, #675).
  - After a start, a member holds its feed, its egress and kind 17 until the election is stable
    (decision 25, #673).
  - The batcher posts only blocks that a majority of the members snapshotted (decision 26, #671).
  - The serving service is a horizontally scalable group of independent instances. The sealer
    purge follows the head that 2 instances hold (decision 27, #585).
  - The failure review adds decisions 28 to 33 (#672, #677, #678, #680, #681, #663).
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
  service. That service has its own design (section 8.1). It tails a sealer member for the
  frames, and reads the receipts from the executors.
- A member keeps no frame with bytes. Its Raft log and its snapshots are its only record. It
  appends at sync level 0 and forces the log every 1 s (section 5.5). The purge keeps the 3 newest
  snapshots and their log, and the log above the stored head of the serving service. The feed
  replays from that log (section 7.3).
- The serving service keeps every entry above the posted head, with its bytes and its receipt,
  for as long as L1 does not hold it. It also keeps each range of an output root until the
  challenge window of that output ends (decision 14). No byte budget stops the chain.
  The executors keep their receipts only until the serving service stores them (section 8.3).
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
| R7 | Every entry that the sealer made durable, of a block above the posted head, stays in at least one durable store until L1 holds it. The member's Raft log keeps it until K instances of the serving service store its frames (the frames-stored head, K = 2 by default). The serving service keeps it until the posted head and the final head pass it: the challenge window of its output root has ended (sections 5.5, 8.1 and 8.3). The executors keep each receipt until the receipts-stored head passes it (#679). |
| R8 | The serving service exposes only blocks at or below the majority durable position: the newest position that a majority of the members forced to disk (sections 5.5 and 8.1). The batcher posts only blocks at or below the snapshotted head: posted head ≤ snapshotted head ≤ majority durable position ≤ commit position (section 8.2, #671). So the batcher posts only data that a power loss, a lost tail or the Aeron 1.44 double vote cannot take back. The sealer needs nothing from the serving service for correctness: the purge floor at the frames-stored head affects only liveness and disk. |

### 1.3 What the design does not guarantee

- It does not make a transaction durable before the cluster commits it. The twins hold it in
  memory until then. A lane that loses both twins loses its transactions in flight, as today.
  The client gets no ack under the default `on-quorum` policy (section 6.4), so it retries.
- It does not check the signature in the sealer. The executors trust the `sender` field that the
  ingress stamped, as today. The validator checks the signature (section 8).
- It does not remove the leader as the bandwidth ceiling. The leader receives every entry, twice,
  and sends it to each follower (section 10).
- It does not bound the disk of the serving service while L1 posts stop. That disk grows with the DA
  lag, on top of the one-day challenge window that it always keeps (section 10.0). The disk of a
  member and of an executor grows only while the serving service stores nothing. An alert warns
  before a disk fills (section 11).
- It does not keep the entries of the last second that no member forced to disk. After a power
  loss on a majority, the cluster continues from its surviving log, and the executors that ran
  past it roll back (section 11.1). A receipt comes before its entry is durable. A transaction is
  confirmed only when the batcher posted it to the DA layer.
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
            │ receipts (8.3)          a follower member: frames; every member: its durable report
            ▼                                                   ▼
        serving service (separate design, 8.1) ──── stored head ──> executor receipt prune,
                                                                    sealer purge floor (kind 12)
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
| Serving service | It tails a sealer member (a follower first) for the frames, polls every member for its durable position, and reads the receipts from the executors. It exposes only data that is durable in the sealer, stores it, and sends its stored head back (sections 8.1 and 8.3). |
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
| Ingress kind 6 (void request), kind 9 (recorded cursor) | Retired. A member drops them as unknown kinds (`SealerClusteredService.java:547-551`). A member drops them already after the activation of the carry flag (section 5.4). |
| Ingress kind 11 `FEATURE_ACTIVATION` (new, #662, PR #666) | `[kind=11][l1_origin:u64][feature_id:u64][activation_ms:u64]`, 25 bytes. The sequencer sends it in front of the epoch that holds the upgrade transaction, on the same session. It has no guard header. It switches the decision rules on and off (section 5.4, #670). The sequencer is trusted: the sealer does not check the record against L1 (decision 23). |
| Egress kind 17 `SEALER_INFO` (new) | `[kind=17][decision_version:u8][cluster_session_id:i64]`, 10 bytes. Under version 3 the sealer sends it to every open session at each switch, and then to each session when it opens. Under version 2 it sends none (#666). It also reports the carry rules after the activation of the carry flag: the sequencers start the full-entry offer only then (decision 21). After a start, the leader sends no kind 17 until the election is stable (decision 25). The id ties the frame to the session whatever the order of the client's own session-start marker. |
| Egress kind 13 (record-lag reject) | Retired. |
| Egress kind 14 | Not used. An earlier draft used it for `ENVELOPE_FULL` (section 15, option A). |
| Egress kind 9 (status) | The 17-byte record-lag tail stays as a fixed field with the "no cursor" values (`best_recorded = u64::MAX`, budget 0, not halted). There is no byte tail: a member retains no bytes (section 5.5). The frame stays 68 bytes, as before the activation of the carry flag. The 76-byte frame with `[retained_bytes:u64]` of #608 goes with the retained file. A reader still accepts a longer frame and ignores the tail. |
| Ingress kind 12 `STORED_CURSOR` (new, #639) | `[kind=12][instance_id:u64][stored_head:u64]`, 17 bytes. Each instance of the serving service sends its own frames-stored head, as the batcher sends kind 7. It has no guard header. The sealer keeps the newest head of each instance, clamps it to its own sealed head, and takes the K-th highest head as the purge floor (section 5.5, decision 27). Kind 11 is the feature activation, so the stored cursor takes kind 12. |
| Egress kind 16 `FEED_GAP` (new, feed only) | `[kind=16][next_index:u64][next_block:u64]`, 17 bytes (section 7.2). |
| The switch marker of the carry flag (new) | The sealer marks the switch in the ordered stream, at its switch entry. The executors switch at the next block boundary after the marker (decision 21). PR 13 sets its layout. |
| The history key on each block (new, #675) | Each boundary carries the key (generation, term) after the activation of the carry flag: `BlockBoundaryStart` and `BlockBoundary`. The term is the Aeron `leadershipTermId` of the term in which the sealer sealed the block. The generation is the clock time of the last fleet rebuild in ms, 0 at genesis (section 11.1, decision 24). #675 sets the layout. |
| Record types 0 (`TxRef`) and 4 (void) | Not written after the activation of the carry flag (section 12). A later release removes them (PR 14). |

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

- **What version 3 is.** No void, and the burn of a skipped nonce (decision 22):
  - **No void.** On the carried path the record carries its bytes, so no void exists (section
    5.3). Version 2's rule (the lower nonce after a void) has no input after that.
  - **The burn (#676).** With `FEATURE_SEALER_V3` active at a block, the executor (every block
    driver: the live engine, the validator, the replay and the zk guest) burns the nonce of a
    skipped transaction. A skip at nonce `n`, when the chain nonce of the sender is `n`, sets the
    nonce to `n + 1`. It charges no fee and makes no other state change. A skip that cannot burn
    (an undecodable transaction, `nonce_too_high` or `nonce_too_low`) stays as today.
  - **The sealer guard does not change.** It already expects `n + 1` after an ordered entry. So
    after a burn, the guard and the chain agree.
  - **The frame shape.** Version 3 needs the record-type byte of each offer. Under version 2 a
    frame of kinds 0, 3, 4 and 5 that ends at the record type relays, as before #634 (#666).
- **No nonce release (decision 22).** The nonce release of #634 and #627 (ingress kind 10, the
  release marks, the sequencer's release queue) goes. It never activated on any network, so no
  network needs it (#676). After a burning skip, the ingress and the sequencer move the floor of
  the sender to `n + 1`. #676 picks a backwards compatible way to tell a burning skip from an
  older one.
- **The sealer and the executor switch at different points.** The sealer switches at a log entry,
  and the executor at a block boundary, so they can differ by about one block. This is harmless:
  the guard behaves the same in both versions.
- The decision version stays as a mechanism (#568).
- **The switch is an upgrade transaction (decision 19, #662, PR #666).** The rules are replicated
  state. They change only at one log entry, the same entry on every member:
  - A member starts on version 2. No job setting selects the version. The image reads the rules
    of both versions.
  - `ETHLockbox.initiateUpgrade` on L1 schedules `FEATURE_SEALER_V3` (feature id 2) with an
    activation time in ms. It reaches L2 as a system deposit that calls
    `KardamomChainState.setFeature`.
  - The origin record (ingress kind 4) cannot carry the feature: a new field in its rkyv payload
    breaks an older sealer and an older executor. So the sequencer sends a new system record, the
    feature activation (ingress kind 11, section 4.3), in front of the epoch that holds the
    deposit, on the same session. It sends it again each time it offers the epoch again. It takes
    the last schedule of the feature in the epoch, because the last `setFeature` wins on L2.
  - Kind 11 applies only for the epoch that it fronts, and only above the origin of the pending
    activation. So the twin copy, a repeat and a record of another epoch change nothing.
  - **The sequencer is trusted (decision 23).** The sealer does not check a kind 11 record against
    L1. The cluster network must admit only the services of the chain.
- **Both ways (decision 23, #670).** An upgrade transaction can also turn a feature off: a later
  `setFeature` with a time in the future suspends it, and `type(uint64).max` turns it off for
  good. Every component switches back at one deterministic point, as it switches on:
  - The sealer keeps the latest activation time of each feature that it follows. The newest
    applied kind 11 record wins, in log order.
  - At each log entry the rules are version 3 if and only if that time is not 0 and the cluster
    time of the entry is at or above it. So a member switches on and off at one log index, on
    every member and in every replay.
  - At each switch, in either direction, the leader sends kind 17 with the new version to every
    session, and the member takes a snapshot.
  - A switch back to version 2 must leave a state that version 2 decides correctly (#670).
  - The executor reads the flag at each block boundary, so it follows both ways.
- **The snapshot.** Before the first activation record, the snapshot is version 10, byte for byte
  the snapshot of the release before. After it, the snapshot is version 13: version 10, then the
  decision rules with the latest activation time. A replay from any snapshot switches at the same
  entries.
- **The seed carries the rules (#667).** The seed builder reads the activation word of each
  followed feature in `KardamomChainState` at the head of the seed. A sealer that starts from such
  a seed holds the activation, and switches as after a feature activation record. So a fleet
  rebuild after the activation keeps version 3, and the operator does not send the upgrade
  transaction again.
- **The order of the operator.** The operator sends the upgrade transaction only after every
  sealer member and every sequencer runs the release (`just sealer-upgrade-check`, and the gate
  of #664). A member of an older release drops kind 11 and refuses version 13.
- **Version 3 and the carry flag.** `kardamom.cluster.carryBytes` becomes the carry flag of the
  chain, feature id 3 (decisions 20 and 21, section 12):
  - Version 3 must be active before the carry flag. The sealer ignores an activation of the
    carry flag unless version 3 is active.
  - Before the activation of the carry flag: the record path of today. The void stays in use,
    and the snapshot is version 10 or 13 with the void ledger.
  - After it: the record path of section 5.1. The member drops the void request and the recorded
    cursor as unknown kinds, and the snapshot is version 12 with no void ledger. A version 12
    snapshot implies version 3.
  - Today the release gate refuses a rolling change of `carryBytes` (the must-match list), and a
    member stops on a snapshot of the other layout (`sealer RETAINED-FILE FATAL`). As a chain
    flag, it changes at one log entry, as version 3 does (section 12.2).
- **No coordinated restart (decision 19).** A member replays the log after its snapshot with the
  rules of its state, and each switch is in the log. So a replay switches at the same entries,
  and the log needs no quiesce. The pause, the stop of the sequencers, the quiesce check and the
  purge of the sealer job are not a deploy path. #666 refuses only a rolling deploy onto a sealer
  job that names `decisionVersion=3` (a build between #634 and #666), and names a rebuild from a
  seed as the remedy.
- **A rollback of the release after the switch.** A member of an older release refuses snapshot
  version 13. So after the first activation record is in the log, the gate of #664 refuses a
  rollback of the sealer past the release that brought the flag. A later release removes the
  version 2 rules, after the activation on every network.
- **The switch of feature 2 is removed (#696).** After the activation on staging, every member
  runs version 3 and ignores kind 11, and no sequencer sends it. Feature 3 must bring back a kind
  11 sender and a block-boundary rule in the sealer.

### 5.5 Retention, the snapshot, the purge and the floor

**No retained frames (decision 10).** After the activation of the carry flag, a member keeps no
canonical frame. Its Raft log and its snapshots are its only record of the stream.

- The retained file of #608 goes: the segment files, the helper thread, the index, the copy with
  a peer seed, the admin endpoint `/retained` and its token.
- The egress retains no frame. It sends each live frame as today: kind 15 for a record of type 6,
  kind 1 for the other record types, kind 2 for a boundary. An egress replay request (ingress
  kind 1) gets `REPLAY_UNAVAILABLE` with the head as its floor. After the activation no production
  consumer sends one: the ingress observer never replays (`crates/ingress/src/cluster.rs:113-123`),
  the executors read their feed, and the validator and the batcher read the serving service.
- The feed replays from the Raft log (section 7.3).
- Before the activation, the heap store of today stays: the `TxRef` frames, the `retention`
  window and the posted-head floor (`SealerEgress.java:420-449`). A consumer that lags behind
  the switch reads the `TxRef` frames below it with its `TxRef` decoder: every reader keeps both
  the `TxRef` and the full-entry decoders until #599 (decision 21).
- The status frame has no byte tail (section 4.3).
- **The earlier rule moves.** The rule "keep the data until L1 holds it, do not halt" now applies
  to the serving service (section 8.1). The DA-lag guard in blocks stays as today
  (`DEFAULT_DA_LAG_BUDGET_BLOCKS = 10_000`, `CanonicalSealerState.java:128`; 60,000 on staging).
  No byte budget refuses a record or halts the chain (decision 5).
- **Out of scope: the inclusion cost.** A long DA outage leaves a large backlog above the posted
  head. How the batcher posts that backlog to L1, and at what cost, is a future optimisation. This
  spec does not design it. The batcher posts by its rules of today (#569), and only blocks at or
  below the snapshotted head (section 8.2, decision 26).

**Durability: the scheduled sync (decision 16).** After the activation of the carry flag, the
sealer appends to its Raft log at Aeron sync level 0 (#641, #645): a commit waits for no disk
sync. A member forces its log to disk on a schedule, and then knows its durable position:

- **The carry flag drives sync level 0 (decision 21).** Until the carry flag is active, the
  member keeps its sync on each append. So #645 can merge at any time, and no deploy order can
  lose data. The rollback of the executors runs only on the carried path (section 11.1).

- **The flusher.** A member-local thread runs every `kardamom.cluster.syncIntervalMs` (1,000 ms).
  It reads the recording position `p` of the member's log recording, forces the recording file,
  and then every byte at or below `p` is on disk. Aeron has no time-based sync (#645), so the
  member owns this thread.
- **The durable position of a member (decision 17).** Each member reports
  `durable = min(locally synced log position, local commit position)`, with the canonical head
  (index, block) at that log position. The service thread records the canonical head after each
  log message that it applies, with its log position, so the member can map a log position to the
  canonical head. Every member applies the same log, so every member maps one log position to one
  canonical head (R3).
- **Who reads it.** The member answers it on its admin endpoint, on the admin thread: not the
  service thread and not a Raft thread (#645 adds the report). Each instance of the serving
  service polls every member and computes the majority durable position (section 8.1). Nothing on
  the commit path reads it.
- **The snapshotted block (#671).** Each member also reports the block of its newest snapshot on
  disk. The serving service polls it with the durable position, and publishes the snapshotted
  head: the newest snapshot block that a majority of the members holds. The batcher posts only up
  to it (section 8.2).
- **The hot path does not wait.** The commit, the feed and the execution do not wait for the
  force. A receipt still comes from the executor in about 5 ms (section 10.0).
- **Snapshots.** The flusher also forces each new snapshot recording and the recording log after
  a snapshot. The purge plans only from a snapshot that the flusher forced.
- **The loss window.** A power loss on the hosts of a majority loses at most the entries after
  the durable position of each member: about 1 s plus the time of one force. Section 11.1 gives
  the recovery.

**The Raft log and its purge.** The log holds the full entries, two twin copies of each. The
purge keeps the newest snapshots and their log, as today (`PurgePlanner.java:65-73`):

- **The margin.** The purge point is a snapshot older than the `keepSnapshots` newest
  (`kardamom.cluster.logPurgeKeepSnapshots`, default 3). At a 300 s interval the log keeps at
  least 15 minutes.
- **The floor (decisions 16 and 27).** Today the purge point must be at or below the posted head.
  After the activation of the carry flag, it must be at or below the frames-stored head instead:
  the K-th highest head of the instances of the serving service (K = 2 by default). The head of an
  instance is the newest block `S` such that the instance holds every frame of each block up to
  `S` on its own durable store. The receipts do not hold the purge back: the receipts-stored head
  sets only the prune of the executors (#679, section 8.3).

| Log on each member | Transfer | 2 KB | 8 KB |
|---|---|---|---|
| Per transaction (2 twin copies, section 4.5) | 0.58 KB | 4.6 KB | 17.2 KB |
| Per block at 1,000 tx/s | 1.16 MB | 9.2 MB | 34.5 MB |
| 15 minutes at 1,000 tx/s | 522 MB | 4.1 GB | 15.5 GB |
| Above the floor | grows only while the frames-stored head does not move: an outage of K instances of the serving service. Section 10.0 gives the rate. |

**Why the floor is the frames-stored head, and why it is one-way.**

- **The serving service reads the sealer.** It tails a sealer member for the canonical frames
  (section 8.1). It exposes a block once the member made it durable, and it syncs its own disk
  apart from that. So a block that the service exposed but did not yet store is only in the Raft
  log. A purge past the stored head would take away the copy that the service reads again after
  its own crash.
- **Not the posted head.** With that floor, a DA outage grows the log of each member by the full
  log input: two twin copies of each entry, about 36 GB an hour at the mix and 5,000 tx/s. With
  the stored head, a DA outage does not grow the log: the serving service keeps that data.
- **One-way.** The sealer needs nothing from the serving service for correctness. Every byte that
  the service exposes is already durable in the sealer. A stored head that does not move only
  keeps more log: it costs disk, and at a full disk the liveness of the member. It never changes
  a decision of the sealer.
- **The start.** Until K instances have sent kind 12, the frames-stored head is 0, and the member
  purges nothing. The serving service runs before the activation (section 12.2), so
  this lasts seconds.
- **The input.** Each instance of the serving service sends ingress kind 12 `STORED_CURSOR`
  (section 4.3) with its instance id, through a publisher-only cluster session, as the batcher
  sends kind 7 (#639). The sealer keeps the newest head of each instance, and replicates it, so
  every member plans the purge from the same values:
  - It clamps each head to its own sealed head. After a rewrite, an instance can report a head
    above the new sealed head, and that head never moves the floor.
  - The floor is the K-th highest head. An instance that stops reporting keeps its last head. So
    the purge never passes a block that fewer than K instances store. A lost instance holds the
    floor back only when it is one of the K highest.
  - No single writer exists, so no lock and no fencing are needed.
- **The kind and the snapshot.** Kind 12 changes
  the decision of no record, so it needs no new decision version. The v12 snapshot carries it
  (below). Before the activation of the carry flag, a member drops kind 12, as a member of the
  base drops an unknown kind. So the snapshot before the activation keeps the bytes of the base
  (#663).

**Disk full.** The Raft log cannot write. The member logs a fatal error and stops, as Aeron does
today. Section 11 gives the effect and the alert. `kardamom_sealer_disk_free_bytes` (#608) stays:
it now watches the log. Two safety nets come first (decision 30, #678):

- **A reserve file.** Each member keeps a reserve file of a few GB on its disk (a setting). It
  deletes the file when its free disk falls below the reserve, so it still has room to finish its
  writes and to snapshot. It writes the file again when the disk recovers.
- **An ingress pause.** Each member reports its free disk on its admin endpoint. When a member
  of a quorum is below the reserve, the ingress refuses new submits with a clear error (for
  example `chain paused: sealer disk low`), and reports the pause in `kardamom_chainStatus`.
  Reads, receipts, the sealer, the executors and the serving service keep running. The pause
  lifts by itself when the purge frees space. The pause is outside Raft, so it needs no flag.
- `KardamomSealerDiskLow` fires before the pause. A serving service of at least 2 instances makes
  this rare.

**Gauges.** The admin `/status` of a member shows the stored head of each instance and the
purge floor, `postedHead`, the durable position, the snapshotted block (#671), the free disk
(#678) and `leadershipTermId` (#673). `kardamom_sealer_durable_lag_seconds` is the age of the
newest durable entry, and `kardamom_sealer_sync_seconds` is the time of each scheduled force. The
status frame gets no new field. The sealer logs `cluster STORED-CURSOR` when a stored head moves
up.

**The snapshot (version 12).** `CanonicalSealerState.java:1474-1538` and
`SealerEgress.java:558-610` change as follows:

| Section | v10 (written today) | v12 |
|---|---|---|
| Dedup window, sender map, origins, remote peers | unchanged | unchanged |
| Void ledger (v6) | present | gone |
| Ordering window, posted head, seed (v8 to v10) | unchanged | unchanged |
| Stored heads (new, kind 12) | absent | after the posted head: the newest head of each instance id |
| History key generation (new, #675) | absent | present: the clock time of the last fleet rebuild, 0 at genesis |
| Recorded cursors (v11, never written) | absent | absent |
| Retained frames | `TxRef` frames, 59 B | none. The retained-file section of #608 goes. |

- **v12 is not released.** A member writes v12 only after the activation of the carry flag. No
  network has activated it, so v12 changes in place. Version 13 is the snapshot of #666 (version
  10 with the decision rules, section 5.4). v12 is frozen at the first activation of the carry
  flag on a network.
- **Restore.** A member of the new release restores v10, v12 and v13. A v10 or v13 snapshot from
  before the switch holds retained `TxRef` frames. Every member drops them alike, at a point that
  PR 13 sets. A member of an older release refuses v12, because v12 has no void ledger. So the
  gate of #664 refuses a rollback past the release that brought the carry flag, after its
  activation.
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
| Unconfirmed ledger | `--unconfirmed-bytes`, 256 MiB for each replica | It back-pressures the lane: the replica reads no new entry until an ordered signal frees space. |

- The unconfirmed ledger holds bytes until the sealer's ordered signal (egress kind 15 on the
  session of the sequencer) clears the entry (decision 31, #680). Today a receipt confirms the
  entry (`confirm_timeout_ms` 15,000). At 2,500 tx/s for each lane and 1 s of receipt latency,
  it holds about 2.4 MB with the mix and 21 MB with 8 KB only. The 256 MiB bound covers about
  12 s at 8 KB only. With the ordered signal, it holds only the entries of the commit latency.
- The benchmark tunes both values (section 10.6).
- Metrics: `kardamom_sequencer_parked_bytes`, `kardamom_sequencer_unconfirmed_bytes`,
  `kardamom_sequencer_byte_evictions_total`.

### 6.3 The twins, the re-offer and the hedge

- Both replicas of a lane offer each entry, as today. The sealer keeps the first copy
  (`docs/failure-modes.md:434`). Both copies carry the bytes, and both enter the log.
- **One re-offer path.** A leader change loses the offers in flight (cluster ingress is at most
  once, `docs/failure-modes.md:257-258`). The unconfirmed ledger re-offers them with their bytes
  (`crates/sequencer/src/unconfirmed.rs:1-13`). The dedup drops the copies that did commit.
- **The ordered signal clears the ledger (decision 31, #680).** The sequencers read egress kind 15
  on their session. An ordered entry leaves the re-offer ledger at once. A sequencer offers an
  entry again only after a session loss or a leader change, and only when it has no ordered
  signal for it. So with no receipts (for example every executor down), no entry re-enters the
  Raft log every 15 s. Receipts still set the nonce floors, as today.
- After a power loss on a majority, the same path re-offers the lost entries that had no ordered
  signal yet. An entry that the sealer ordered and then lost does not come again: its client
  submits again (section 11.1).
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
- **The hold after a start (decision 25, #673).** After all members restart with different log
  tails, Aeron 1.44 can let one member vote in two terms (#620), and a committed tail can be cut
  back. So after a start a member holds every output until it is stable:
  - **Stable** means: every peer reports the same leadership term as the member, with its
    election CLOSED, and the leader has committed an entry in that term. The admin status gets a
    `leadershipTermId` field, and the peer probe of the join watchdog (`PeerProbe`) reads it.
  - After a full restart, when a peer does not answer, a majority that agrees is enough after the
    stall limit of today (80 s to 160 s). So one dead member does not block the chain.
  - Until then the member feed sends no frame, the serving service exposes nothing from that
    member, and the leader sends no kind 17. The sequencers wait for kind 17 before they send.
  - The hold changes no decision rule and no Raft log, so it needs no flag (#661).

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
| 1 | The feed of its paired member | The feed delivers a frame at least every 3 boundary ticks (6 s), and keeps its image. |
| 2 | The serving service (section 8.1) | The feed is silent, lost its image, sent an entry that fails the hash check, or answered `REPLAY_UNAVAILABLE` or `REPLAY_AHEAD`. |
| 3 | A peer checkpoint | Every source answered `REPLAY_UNAVAILABLE`, or never connected in this run of the reader (`crates/engine/src/bin_support.rs:529-576`). |

- A peer checkpoint replaces the state, so the reader takes one only when no source can serve the
  cursor. A silent or lost source does not count: a member that restarts (section 7.4) can serve
  the cursor a few seconds later. The reader then tries the other source again.

- The switch happens at a frame boundary. The executor closes the old source and asks the new one
  for a replay from its cursor. R3 makes the frames at each index equal.
- The executor goes back to its feed when both hold, in this order:
  - the feed subscription has one image of the member's publication for 30 s without a break. A
    lost image, or a new image (a new correlation id), restarts the 30 s;
  - then a probe replay of the feed from the executor's cursor reaches `REPLAY_DONE`, which is the
    head of the member. The reader drops the frames of the probe, and it sends a new request when
    it switches.
  Until then the executor stays on the serving service. A probe that ends in another way restarts
  the 30 s.
- While the executor reads the serving service, it discards the frames of the feed at each timer
  step. A member that stays live writes on, and a backlog would hold the answer of the probe
  behind it. During the probe, each frame of the feed, also a dropped one, shows that the feed is
  alive.
- The probe is the readiness check. A member that lags answers its replay only when its walk
  reaches its own head, so a member that lags cannot take the executor back before it reaches the
  head. The executor has no HTTP client, and it does not read the member admin endpoint.
- The reader does not use `kardamom.cluster.readyLagBytes`. The member uses it for its own admin
  endpoint only (`cluster/sealer-service/README.md:362`). It scales with the log bytes. The
  default 4 MiB is about 500 log entries of 8 KB, so a busy follower would flap. The new default is
  64 MiB, and the setting gets a hysteresis: a member turns not ready above the value and ready
  again below half of it.
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
| Executor `0..2` | Cluster egress (references) + 8 `tx_data` lanes + ingress archives; votes (`crates/executor/src/bin/kardamom-executor/main.rs:494-513`, `:339`) | Its member's feed; the serving service as the fallback. No `tx_data`, no archive, no vote. It keeps its receipts and their index only until the serving service stores them (section 8.3). It checks `keccak256(raw_tx) == canonical_id` for each `RT_TX` entry (about 1 µs for a transfer). On a mismatch it does not execute the entry and does not halt: it drops that source, replays from the other source at the same index, counts `kardamom_executor_source_mismatch_total{source}`, and an alert fires. |
| Elastic executor | As above | The serving service (section 8.1). |
| Validator | Cluster egress + `tx_data` + ingress archives; votes (`crates/validator/src/bin/kardamom-validator/wiring/startup.rs:241-286`, `wiring/run.rs:170-178`) | The serving service (section 8.1). It checks `keccak256(raw_tx) == canonical_id` and that the signature recovers the sender (`crates/exec-core/src/stateless.rs:253-272`). A failed check halts it with `validator_divergence`. It does not trust the sealer's bytes. Its attester reads the receipts above the attested head from the serving service after a restart (section 8.3, a follow-up). |
| Batcher | Cluster egress + `tx_data` + ingress archives; votes; the rebuild from `kardamom_getBlockRefs` and the archives (`crates/batcher/src/live/run.rs:292-357`, `:641-669`) | The serving service (section 8.1). It packs the KAR1 blocks from the `raw_tx` of each `RT_TX` entry. The spool, its group and cursor rules do not change. It keeps a publisher-only cluster session for the posted cursor (kind 7). |
| Ingress observer | Full relayed frames (`crates/ingress/src/cluster.rs:173-174`) | The cluster egress: kind-15 frames, no bytes (section 6.4). |
| Ingress receipt fallback | The receipt cache, then the executor state DB over HTTP (`crates/ingress/src/proxy/mod.rs:451-475`) | The cache, then an executor, which holds only the receipts above the stored head, then the serving service (section 8.4). |
| State mirror | `tx_receipts` only (`crates/state-mirror/src/main.rs:137-146`) | Unchanged. |
| Notifier | `tx_status`, `tx_receipts`, `tx_errors` (`crates/notifier/src/taps.rs:56-74`) | Removed for now (decision 12). A separate PR removes the code. |
| Sequencer | `tx_data`; publisher-only session (`crates/sequencer/src/outbound/cluster.rs:121-172`) | `tx_data`, as today. It offers the full entry (section 6.1). |
| da-watcher, L1 follower | `l1_blocks`, `tx_deposits` | Unchanged. |

### 8.1 Out of scope: the external serving service (separate design)

A member feeds its co-located executor over IPC (decision 7), and it also feeds the serving
service (decision 16). The members serve no other consumer of the bytes: neither the leader nor a
follower serves the validator, the batcher, unpaired executors or external nodes. The serving
service serves them. It has its own design (#585). The user also calls it the egress service.
This spec states only what it needs from that service:

- **Independent instances (decision 27).** The service is a horizontally scalable Nomad group
  with a `scaling` block, a minimum of 2 and `distinct_hosts`: one instance for each host, on
  hosts apart from the sealers where the fleet allows it. Each instance tails a sealer member,
  stores the frames and the receipts on its own disk, syncs its own disk, and serves reads. No
  instance depends on another one at run time. Each rule below holds for each instance.
- **What it reads (decision 16).**
  - **The canonical frames from the sealer.** It tails a follower member first. When that member
    is unavailable, it falls back to the other members, the leader last. It reads the frames with
    their bytes. The transport is the member feed over UDP, beside the IPC feed (decision 27,
    which closes 16.1-17).
  - **The durable position of every member** (decision 17): see "The majority durable position"
    below.
  - **The receipts from the executors.** It reads the receipts of a block from one executor, only
    for a block that is durable in the sealer (section 8.3).
  - It opens one publisher-only cluster session, as the sequencer and the batcher do, only to send
    its frames-stored head with its instance id (ingress kind 12, section 5.5). It reads no
    cluster egress.
  - **The snapshotted block of every member** (#671, section 5.5). It publishes the snapshotted
    head for the batcher (section 8.2).
- **What it exposes.** It serves a block to a reader only when the block is at or below the
  majority durable position. So every byte that it exposes is durable on a majority of the
  members (R8). The batcher reads it, so the batcher posts only data that is durable in the
  sealer.
- **The majority durable position (decision 17).**
  - **The report.** Each member answers `durable = min(locally synced log position, local commit
    position)`, with the canonical head at that position, on its admin endpoint (section 5.5).
  - **The poll.** Each instance of the serving service polls every member once for each scheduled
    sync, about once a second. With three members it takes the median of the three values: the
    newest position that two members, a majority, forced to disk. In general it takes the value
    that a majority of the members reached.
  - **A member that does not answer.** With two answers out of three, the instance takes the
    lower of the two. Both members forced at least that much, and two members are a majority, so
    the value is still durable on a majority. With one answer, the instance holds the exposure
    where it is. The exposed position never moves down.
  - **Why a majority.** Raft elects the most complete log among the members that vote, and every
    election majority holds at least one member that forced the majority durable position. So a
    power loss never takes back an exposed block (section 11.1).
  - **The cost for the leader.** One small HTTP answer a second for each instance, on its admin
    thread. No Raft entry, no extra log traffic, and no work on the service thread.
  - **The hold after a start.** An instance exposes nothing from a member that holds its outputs
    after a start (decision 25, section 7.1).
- **Its own disk: two heads (#679).** It syncs its own disk apart from the sealer. It reports two
  heads, and the K-of-N copy rule (K = 2 by default) applies to each:
  - the **frames-stored head**: the newest block whose frames it holds on its own durable store.
    It sends this head to the sealer in kind 12 (the purge floor, one-way: section 5.5);
  - the **receipts-stored head**: the newest block whose receipts it also holds. It sends this
    head to every executor (the receipt prune, section 8.3).
  - So when every executor is down, the instance stores no receipts, but the frames-stored head
    still moves, and the sealer purge goes on.
  - After its own crash, it reads the blocks above its heads from the sealer and an executor
    again.
- **After a history rewrite (#585, G4).** A rewrite happens only above the snapshotted head: the
  Aeron 1.44 double vote (#620) or a lost tail after a power loss. An instance can then hold
  frames of the old history, and its head can be above the new sealed head.
  - On each reconnect, it compares with the replay of the member, as the executor does (section
    11.1, one code path, STYLE R14). It drops its frames above the divergence point.
  - It resumes from its exposed head, never from its received cursor. On each reconnect it drops
    the frames that it received but did not expose.
  - The sealer clamps each kind 12 head to its sealed head (section 5.5).
- **A new instance** (a scale-up, or a replacement) first copies the stored range from a peer
  instance, then tails a sealer from the head of the peer. It counts toward the copy count only
  when it reaches the floor. A scale-down keeps at least K instances at the floor.
- **No dependency of the sealer on it.** The sealer never needs the serving service to recover:
  every byte that the service exposes is durable in the sealer already (section 11.1).
- **The stream.** The canonical stream in canonical order: kind 1 (all record types, with the
  envelope for record type 6) and kind 2 (boundary). The frames are byte for byte the frames of
  a member feed (R3), so a consumer uses one codec for both. The service serves no kind 9. None of
  its readers needs one: the batcher counts its sealed head from its own blocks
  (`crates/batcher/src/live/feed.rs:95-98`). A reader drops a kind 9 that it gets, as on the feed.
- **Liveness on this transport.** The executor's reader uses the boundary (kind 2), as on the
  feed. The sealer closes a block on each tick (2 s), also an empty block. The service serves a
  boundary after the scheduled sync makes it durable (1 s) and after its own read delay. The
  reader leaves the service after 6 s with no frame (the silence window of section 7.5, three
  ticks). So the read delay of the service must stay well below 3 s. Its design states the bound,
  and an alert fires on `kardamom_sealer_durable_sync_seconds` above 1 s (#585, G21).
- **The replay.** A replay from a cursor `(index, block)`, with the answers of today: kinds 3, 4
  and 11 (`SealerEgress.java:201-253`).
- **What it keeps (decisions 10 and 14).** An append-only log of the canonical frames, and the
  receipts of each block, on its own disk. It keeps every block above
  `min(posted_head, final_head)`, for the two readers that need a long window:
  - the batcher, which posts to L1 (section 8.2);
  - the withdrawal attester in the validator, which posts output roots to L1 (section 8.3), and
    a challenger that checks an output root within its challenge window.
- **The final head.** The newest block of an output root whose challenge window has ended:
  `WithdrawalOutputOracle.isFinalizable` is true for it
  (`contracts/src/L1/WithdrawalOutputOracle.sol:295-310`). The window is `finalizationWindow`,
  1 day, on the settlement clock of the oracle: a pause of the oracle extends it. So the service
  keeps an attested range for its whole window, also after the batcher posted it (decision 14).
  The blocks above the attested head are above the final head, so the service keeps them too.
  With no oracle, the floor is the posted head. The service reads both heads from L1.
- **The receipt history.** The floor is a minimum. The service can keep a longer window for other
  external readers: the public history RPC, indexers and explorers (section 8.4). The user wants
  the receipt history to stay available after the executors prune (decision 14, the import of
  section 12.3). So the design of #585 sets the receipt window, and the floor above does not
  bound it.
- **The rule "keep it, do not halt".** The service keeps the data until L1 holds it. No byte
  budget stops the chain (decision 5). Its disk grows with the DA lag (section 10.0).
- **The cold tier (decisions 11 and 27).** It is the cold tier of section 8.4: independent
  instances that each tail the sealer and read the receipts from the executors. It also answers
  the receipt lookup of the ingress for a receipt that the executors pruned, and the polls for a
  pending transaction, which come to it after the removal of the notifier (#647, G22). Its design
  sizes that load.
- **The order of work.** The validator and the batcher read it after the activation of the carry
  flag, so the service runs before the upgrade transaction (section 12.2).

#### The transport contract

The executor reads the member feed and the serving service through one interface, `Transport`
(`crates/engine/src/reader/carried/transport.rs`). The feed transport (`AeronFeed`) implements it.
The transport of the serving service implements the same interface. One reader applies the frame
rules of section 7.8 to both, and one pending set orders the frames of both (R14).

| Method | Input | Output |
|---|---|---|
| `connection` | none | The id of the current connection, or `None` when the source cannot take a request now. Each new connection has a new id. For the feed: the correlation id of the image of the member's publication. |
| `request` | `Request { from_index, from_block, request_id }`: the cursor and a new id | none. The source answers the newest request and ends each older answer. A lost request is not an error. |
| `poll` | the longest wait | `Frame(bytes)`: one whole frame. `Idle`: no frame within the wait. `Closed`: the process stops. |
| `close` | none | none. The source discards the frames that it sent and the reader did not read. |

- The reader sends a request only while `connection` is not `None`. It starts each connection,
  also a new id that replaces the old one with no gap between, with a
  request, and it sends a new request with a new id for each new answer.
- The reader leaves a source only between two frames. It calls `close` on the old source, and sends
  a request from its cursor to the new one.

**The frames.** The transport delivers the frames of the member feed, byte for byte (section 7.8):

- kind 1 (relayed; record type 6 with the envelope) and kind 2 (boundary): the canonical frames;
- kinds 3, 4, 11 and 16: the answers, each with the `request_id` of its request at offset 17.

The stream of the service holds no kind 9 (status): the service carries the canonical frames only,
and it reads no sealer status (decision 16). The reader drops a kind 9 that it gets, as it
does on the feed. **Liveness on both transports is the boundary (kind 2).** The sealer closes a
block on each tick (2 s), also an empty block, so a live source sends a boundary at least every
tick: the feed at once, the serving service after the scheduled sync (1 s) and its read delay. The
silence window of 6 s (three ticks) applies to both. The design of the serving service bounds its
read delay well below 3 s (section 8.1). The reader drops each kind that it does not know.

**The error cases.**

| Case | What the reader does |
|---|---|
| Not connected | It sends no request. It leaves the source when no frame comes within the silence window (6 s). A source that loses its connection after a request: the reader leaves it at once. A source that never connected in this run counts as absent. |
| A new connection | It sends a new request with a new id to the same source. |
| No answer | No frame of the answer within 2 s of the request: it sends a new request with a new id. |
| An answer with an old id | It drops the frame. |
| A canonical frame below the cursor | It drops the frame. |
| A hole (the index or block rule of section 7.8) | It does not apply the frame. It sends one new request from its cursor to the same source. It drops each later frame that does not follow the cursor, until a frame of the new answer comes: those frames come from the old answer. The 2 s resend covers a lost request. |
| Gap (`FEED_GAP`, kind 16) | It sends a new request from its cursor to the same source. |
| Replay unavailable (kind 3, with the floor `oldest_index`, `oldest_block`) | It leaves the source. When every source answered `REPLAY_UNAVAILABLE` or is absent, it stops with `ClusterReplayUnavailable` and the lowest floor, and the executor fetches a peer checkpoint (section 7.3). Else it tries the other source. |
| Replay ahead (kind 11, with the head) | It leaves the source. When every source answered `REPLAY_AHEAD` or is absent, it stops with `ClusterBehindCursor` and the highest head. Else it tries the other source. |
| Silent too long (no frame for 6 s) | It leaves the source. |
| A frame of a known kind that does not decode | It leaves the source. |
| `keccak256(raw_tx)` is not `canonical_id` | It does not execute the entry. It counts `kardamom_executor_source_mismatch_total{source}`, leaves the source, and replays from the other source at the same index. |

- A source that the reader left as silent, lost or bad stays in the order: the reader tries it
  again when the other source fails. The reader keeps the fault of the latest answer of each
  source. A new request to the source clears it, and so does a `REPLAY_DONE`. So a peer
  checkpoint needs the latest answer of each source to be `REPLAY_UNAVAILABLE`, or the source to
  be absent, and the floor comes from those latest answers.
- The return to the feed follows section 7.5: the feed keeps one image for 30 s, then a probe
  replay of the feed from the cursor reaches `REPLAY_DONE`.

### 8.2 The batcher: what it posts, and its resume sources

**The batcher posts only snapshotted blocks (decision 26, #671).** The sealer must never come back
below the head that the batcher posted to L1: a lower head forks L1. The majority durable position
does not cover two cases: the Aeron 1.44 double vote (#620), which can drop a committed tail, and
a coordinated disk failure with no snapshot left. So:

- The rule: posted head ≤ snapshotted head ≤ majority durable position ≤ commit position.
- The serving service publishes the snapshotted head: the newest snapshot block that a majority
  of the members holds (section 5.5). The batcher ends each batch at or below it.
- A restarted member restores from its snapshot, so its sealed head is at or above the block of
  that snapshot, and so at or above the posted head. The double vote can drop only blocks above
  the snapshotted head. Those blocks are not posted, and the executors roll them back (section
  11.1).
- **The post interval.** Staging keeps its interval of 1 hour. Production posts at each new
  snapshotted head (the sealer snapshots every 300 s), so about every 5 minutes. The batcher gets
  a mode that posts when the snapshotted head moves, beside its fixed interval. The deploy profile
  selects the mode and the interval.
- The new admin field and the new field of the serving service are additions that older readers
  ignore. The batcher limit changes no format.
- **The spool check (decision 28, #672).** At its start and after each reconnect, the batcher
  checks its spool against the serving service. It drops every spooled block whose hash the
  service does not hold, and builds the group again from the service.

**The resume sources.**

1. The spool: unchanged.
2. The replay from the cursor, now from the serving service. It holds every frame above the
   posted head (section 8.1). The members do not keep those frames: a member keeps its log only
   above the frames-stored head (section 5.5).
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
  Raft log holds them only above the frames-stored head.
- The batcher keeps its cluster session as a publisher only, for the posted cursor (ingress
  kind 7, `crates/batcher/src/live/posted_cursor.rs`). The DA-lag guard of the sealer needs it.
  It reads no cluster egress.
- Unaffected: `l1_blocks` and the L1 follower, the DA-lag flush of the batcher (#569), the
  batcher's resume from the contract, its spool and cursor formats.

### 8.3 The executor output, the handoff and the stored head

The serving service is the durable store of the canonical stream above the posted head. It gets
the frames from the sealer (section 8.1), and the receipts from the executors. For the receipts,
the executor is a short buffer in front of it (decision 16). This section states the executor side
of the handoff. The serving service side is part of its design (#585).

**What the executor keeps.** Execution needs the state, so the state stays. Only the receipts
become a buffer. The state DB has 11 tables (`crates/state/src/schema.rs:51-63`). Each block
writes all of them in one transaction (`crates/state/src/writer/apply.rs:302-324`).

| Table | Today | After |
|---|---|---|
| `accounts`, `storage`, `code` | state; kept for ever | kept: execution reads them |
| `meta` | the resume cursors, the schema version, the rebuild mark (`crates/state/src/meta.rs:8-60`) | kept. New key `output_floor_block`: the newest block whose receipts the executor pruned. |
| `account_trie`, `storage_trie`, `hashed_accounts`, `hashed_storage` | genesis rows only on the executor (the trie is off, `crates/state/src/writer/mod.rs:186-188`) | kept, unchanged |
| `headers` | one 48-byte row for each block; execution reads only the last (`crates/state/src/recovery/mod.rs:75-93`) | kept. A row is 48 bytes, about 2 MB a day at 2 s blocks, and the integrity sweep needs the rows from block 0 with no gap (`crates/state/src/integrity/checks.rs:182-216`). The rollback of section 11.1 compares them. |
| `receipts` | output; kept for ever; no execution path reads it | **the buffer**: pruned at or below the stored head |
| `tx_hash_index` | output; kept for ever | **the buffer**: pruned at or below the stored head, with its receipts |

- The executor holds no transaction bytes for the serving service. The service reads the frames
  from the sealer, so the executor needs no new table.
- **Checkpoints.** A checkpoint is a copy of the whole MDBX image
  (`crates/state/src/checkpoint/mod.rs:172-203`). After a prune it holds only the buffer of
  receipts, so it is smaller. An executor that restores an old checkpoint executes again from its
  cursor, writes the receipts again, and prunes the part at or below the stored head at once.
- **Rebuild from L1.** `crates/reconstruct` writes the receipts and the index as a side effect,
  and never reads them. The next prune drops them.
- **Checks.** The integrity sweep keeps its 1:1 rule between `receipts` and `tx_hash_index`
  (`crates/state/src/integrity/checks.rs:241-246`): both are pruned to one floor. `deep_compare`
  and the persisted-state stage of chaos (`crates/chaos/src/stages/state.rs`) compare the receipts
  above the highest `output_floor_block` of the compared DBs.

**What the serving service reads from the executor (proposal; the design of #585 decides,
decision 14).** A receipt read on the executor's state query server, the server that already
answers `eth_getTransactionReceipt` (`crates/state/src/nonce_query.rs:197-209`):

- The request: `[from_block:u64][to_block:u64][stored_head:u64][max_bytes:u32][wait_ms:u32]`.
  `to_block` is the block of the majority durable position (section 8.1).
- The answer, for each committed block from `from_block` to `to_block`, up to `max_bytes`: the
  block number and its receipts, in index order. With no committed block at `from_block`, the
  server waits up to `wait_ms` (a long poll).
- The body is binary, as the checkpoint server is (`crates/state/src/checkpoint_transfer.rs`).
- A block below `output_floor_block`: the answer says "pruned", with the floor.
- The live `tx_receipts` stream does not serve this: it has no replay and no archive
  (`crates/log/src/config/mod.rs:298`).

**The two stored heads (decision 27, #679).** One stored head would couple the frames and the
receipts: with every executor down, the service stores no receipts, and the sealer purge would
stop too. So the serving service reports two heads, each the head that K instances hold (K = 2 by
default, section 8.4):

- **The frames-stored head** sets the sealer purge floor (ingress kind 12, section 5.5).
- **The receipts-stored head** sets only the executor prune. In this section, "the stored head"
  is the receipts-stored head.
- **Definition.** The newest block `S` such that K instances hold every frame and every receipt
  of each block up to `S` on their own durable stores.
- **How the executor learns it.** Each receipt read carries `stored_head`. Each instance sends its
  own head to every executor, also to the executors that it does not read from: a read with
  `max_bytes = 0` is the ack alone. The executor prunes only up to the head that K instances
  hold. The design of #585 sets how the executor learns that head.
- **The prune.** After each block commit, the executor deletes the `receipts` and `tx_hash_index`
  rows of the blocks at or below `min(stored_head, last_committed_block)`, in one write
  transaction, and sets `output_floor_block`.
- **How the sealer learns its head.** Each instance sends its frames-stored head in ingress kind
  12, with its instance id (section 5.5). The link is one-way: it moves only the purge floor.

**When the serving service is down.** The stored head stops. The executor keeps its receipts
above the last stored head. It does not stop or slow execution:

- No back-pressure. A slowed executor delays every receipt, and the sealer keeps the log above
  the stored head anyway (section 5.5). Back-pressure would turn an outage of the serving service
  into an outage of the receipts, against decision 5.
- **The bound is the disk.** A receipt and its index entry are about 0.3 KB for a transaction
  without logs (`crates/types/src/receipt.rs`): about 1.5 MB/s at 5,000 tx/s, or 5.4 GB an hour,
  whatever the size of the transaction (section 10.0).
- **Alerts.** `KardamomExecutorOutputUnstored`: the stored head has not moved for 10 minutes while
  the executor commits. `KardamomExecutorDiskLow`: free disk under 25 %, or under 6 hours at the
  current growth rate. At a full disk the commit fails and the executor stops, as an MDBX write
  error stops it today.

**More than one executor (decision 14).**

- The serving service reads the receipts from one executor at a time, in a configured order. It
  switches to the next one when the source is down or answers "pruned" below its need.
- One executor is enough: a block counts as stored when the service holds the frames from the
  sealer and the receipts from one executor. No second executor confirms it.
- **The cross-check.** The frames come from the sealer and the receipts from an executor, so the
  service checks that the `tx_hash` of the receipt at each index equals the `canonical_id` of the
  frame at that index. On a mismatch it does not store the block, reads its receipts again from
  the next executor, and an alert fires. It also checks `keccak256(raw_tx) == canonical_id` for
  each `RT_TX` frame, as the executor does (#595).
- **What the checks do not catch.** A wrong receipt for the right transaction. The validator
  re-executes each block, and its receipt check compares its receipts with the receipts of the
  executors on `tx_receipts` (`crates/validator/src/buffers.rs:459-500`).

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
| Withdrawal attester (in the validator) | Its own re-executed receipts (`crates/validator/src/attester/sinks.rs:1-23`); the leaves live in memory until a post (`crates/validator/src/attester/state.rs:44-63`) | Unchanged for live blocks. **It attests only blocks at or below the posted head**, which it reads from L1 (decision 28, #672). The posted head never goes back, so an attested root never names a removed block. Withdrawals then wait for the next post: up to 5 minutes in production and up to 1 hour on staging. **After an attester restart** the validator resumes from a checkpoint at or below the attested head, so the attester sees every withdrawal again from there (decision 32, #681). Today the validator resumes from its own state cursor, which can be above the attested head (`crates/validator/src/attester/mod.rs:27-29`, `state.rs:106-119`). The attester keeps its leaves across the switch of the carry flag (decision 21). |
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
| Cold | The serving service: a horizontally scalable group of independent instances (decision 27) | External services: the batcher, the attester (the L1 withdrawal proposer), the public history RPC, indexers and explorers, the validator, the unpaired executors, and the ingress fallback for a pruned receipt | Every block above `min(posted_head, final_head)`, and the receipt window of its design |

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
  receipt, and each frame, is always in at least one tier, for as long as the receipt window of
  the serving service keeps it (section 8.1). A lookup between the two reads can miss
  on the executor after a prune and find the receipt on the serving service.

**The serving service scales out as independent instances (decision 27, #585).** This replaces
the shape of one writer with read replicas (decision 11). It also settles the number of copies
(G2) and the writer failover (G10). The design of #585 states the details:

- **Independent instances.** A Nomad group with a `scaling` block, a minimum of 2, and
  `distinct_hosts`. Each instance tails a sealer member (a follower first, then the others) over
  the UDP member feed, stores the frames and the receipts on its own disk, syncs its own disk, and
  serves reads. No instance depends on another one at run time. Each instance polls the members
  for their durable positions and snapshotted blocks, and exposes data only up to the majority
  durable position.
- **The copy count.** The heads that the sealer purge and the executor prune follow are the heads
  that at least K instances hold (a setting, K = 2 by default, sections 5.5 and 8.3). One instance
  can fail with no loss. Each instance sends its own kind 12 with an instance id, and the sealer
  keeps the floor from the K-th highest head. So no single writer exists, and no lock or fencing
  is needed.
- **A new instance** (a scale-up, or a replacement) first copies the stored range from a peer
  instance, then tails a sealer from the head of the peer. It counts toward the copy count only
  when it reaches the floor.
- **Reads.** Consul publishes every instance. The batcher, the validator, the attester, external
  nodes and the ingress fallback read from any instance at or above the head that they need. Each
  instance gives the snapshotted head for the batcher (#671).
- **Scale-down** keeps at least K instances at the floor.

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
| #568 | Decision version | Kept as a mechanism. An upgrade transaction raises it to 3 (#666, section 5.4). |
| #561 | The sequencer frees a refused nonce | Kept. Each refusal of a full entry uses it. |
| #552 | Readers hardened for a rollback; unknown kinds drop | Kept. It makes the retired kinds safe. |
| #482, #501 | Posted-head floor, log purge | The posted-head floor of the heap retention stays until the activation of the carry flag. After it, a member retains no frame, and the log purge floor is the stored head of the serving service (section 5.5). |
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
| #537 (issue) | `archive-tx-data-wipe`: a frame in flight at the driver kill is in no archive | The activation of the carry flag closes it: no executor joins `tx_data` after it. |

**Ownership.** kardamom-3-00 co-owns the executor side once the user confirms: the feed reader,
the source switching, and the removal of the join, the refetch and the void in the engine (PR 10
and the engine part of PR 14).

## 10. Throughput and resources

### 10.0 The design target

The user set the target: **up to 5,000 tx/s, with mixed sizes**. On 2026-10-17 the user added a
latency target (decisions 13 and 14):

- **The requirement:** a transaction lands within 50 ms of its submit.
- **The ideal:** a p99 of at most 20 ms from the ingress receipt of the submit to the executor's
  emission of the receipt on `tx_receipts`.
- The benchmark gate measures that interval (section 10.6). The executor emits each receipt when
  it executes the transaction: the commit thread does not wait for the block
  (`crates/engine/src/actor/commit_thread.rs:237-249`). So the 2 s block is not in the interval.
  The interval holds the ingress publish, the sequencer, the ordering window (up to 5 ms,
  `cluster/sealer-service/README.md:258-275`), the Raft commit, the feed and the execution.
  Today's receipt p99 is 47 ms at the edge (`docs/specs/2026-08-16-pipeline-cost-model.md`, "Gas
  throughput and latency"), so the ideal needs a gain of more than half.

The mix below is an example for the arithmetic. The benchmark uses the same mix, plus a pure 8 KB
row as the worst case.

- Example mix: 80 % transfers (110 B raw), 15 % 2 KB calls, 5 % 8 KB calls. The mean raw size is
  about 805 B. The log input is about 2.0 KB a transaction (two twin copies), and a canonical
  frame about 932 B.
- The serving service stores the frame plus the receipt and its index entry: about 0.3 KB more
  for each transaction without logs (section 8.3). The executor buffer stores only the receipt and
  its index entry. That is about
  0.54 KB a transaction with transfers only, 1.23 KB with the mix, 2.48 KB with 2 KB only and
  8.62 KB with 8 KB only.
- The serving service always keeps the challenge window of 1 day (decision 14), plus the post
  interval of the attester. The rows below give that steady state, and the growth on top of it
  while no post lands.

| At 5,000 tx/s | Transfers only | Example mix | 2 KB only | 8 KB only |
|---|---|---|---|---|
| Out of one sequencer replica (2,500 tx/s) | 0.72 MB/s | 2.5 MB/s | 5.7 MB/s | 21.5 MB/s |
| Log input at the leader (2 twin copies) | 2.9 MB/s | 10.1 MB/s | 23 MB/s | 86 MB/s |
| Replication out (2 followers) | 5.8 MB/s | 20.1 MB/s | 46 MB/s | 172 MB/s |
| Egress out (2 ingresses, kind-15 frames of 42 B) | 0.42 MB/s | 0.42 MB/s | 0.42 MB/s | 0.42 MB/s |
| Leader out, total | 6.2 MB/s | 20.5 MB/s | 46 MB/s | 173 MB/s (1.38 Gbit/s) |
| Log written in 15 minutes, each member | 2.6 GB | 9.0 GB | 21 GB | 78 GB |
| Stored bytes for each 2 s block (frames + receipts) | 5.4 MB | 12.3 MB | 25 MB | 86 MB |
| Steady state, each copy of the serving service: the 1-day challenge window | 233 GB | 530 GB | 1.07 TB | 3.7 TB |
| Disk growth while no post lands, each copy of the serving service, above the steady state | 2.7 MB/s | 6.2 MB/s | 12.4 MB/s | 43 MB/s |
| The same, in one hour | 9.7 GB | 22 GB | 45 GB | 155 GB |
| The same, at the block guard (10,000 blocks, about 5.6 hours) | 54 GB | 123 GB | 248 GB | 862 GB |
| Disk growth while no post lands, each member and each executor | none above the margin and the buffer | none | none | none |
| While the serving service stores nothing: each member (log) | 2.9 MB/s, 10.4 GB an hour | 10.1 MB/s, 36 GB an hour | 23 MB/s, 83 GB an hour | 86 MB/s, 310 GB an hour |
| While the serving service stores nothing: each executor (receipt buffer) | 1.5 MB/s, 5.4 GB an hour | 1.5 MB/s, 5.4 GB an hour | 1.5 MB/s, 5.4 GB an hour | 1.5 MB/s, 5.4 GB an hour |
| Raw bytes to post to DA | 0.55 MB/s | 4 MB/s | 10 MB/s | 41 MB/s |

Findings from the arithmetic, for the benchmark to confirm:

- **Network.** The leader sends only the replication and the 42-byte kind-15 frames. The mix needs
  about 20.5 MB/s out of the leader, inside a 1 Gbit/s link. A stream of only 8 KB calls needs 1.38
  Gbit/s, above a 1 Gbit/s link: the worst case needs 10 GbE. The leader also receives 86 MB/s at 8
  KB only. The serving service is not in these numbers (section 8.1): each instance tails a
  follower member over the UDP member feed (decision 27), about 4.7 MB/s of frames at the mix and
  42 MB/s at 8 KB only, and reads the receipts from one executor, about 1.5 MB/s. The follower
  sends that, for each instance that tails it, beside the replication that it receives. It adds
  no load to the leader. Only the staging run can measure the
  network (section 10.6).
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
- **The disk retention (decisions 10 and 14).** No byte budget halts the chain (decision 5). Each
  copy of the serving service always holds the 1-day challenge window: about 530 GB at the mix
  and 5,000 tx/s, and 3.7 TB at 8 KB only. While no post lands, only the serving service grows
  above that: about 22 GB an hour for each copy at the mix, and about 155 GB an hour at 8 KB
  only. The members and the executors do not grow, because the
  stored head moves. Before decision 10 each of the 3 members grew by about 53 GB an hour at the
  mix (the log and the retained file). The disk size of the serving service sets how long the
  chain rides out a DA outage. The alert of section 11 gives the time to a full disk.
- **An outage of the serving service.** The stored head stops. Each member keeps its log, about
  36 GB an hour at the mix, and each executor keeps its receipts, about 5.4 GB an hour. The
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
- The flusher forces the log once a second, on its own thread (section 5.5). The commit path does
  not wait for it. The benchmark measures the time of each force.
- The service thread also reads the losing twin copy, and drops it at the dedup step. The
  copy is not parsed past the guard header.
- The consensus module and the archive of the leader handle `rate x log bytes`. This cost is in
  Aeron, not in our code. The benchmark measures it.

### 10.4 Disk on each member, each executor and the serving service

See sections 5.5, 8.1 and 8.3:

- **A member.** The log grows by `rate x log bytes`. The purge keeps the 3 newest snapshot intervals
  (15 minutes) and the range above the stored head. A member retains no frame. While no post lands,
  the stored head moves, so the member does not grow. While the serving service stores nothing, the
  log grows at about 36 GB an hour at the mix.
- **An executor.** The state grows as today. The receipt buffer (`receipts`, `tx_hash_index`)
  holds only the blocks above the stored head: seconds of receipts in steady state. While the
  serving service stores nothing, it grows at about 5.4 GB an hour at 5,000 tx/s.
- **The serving service.** Each copy holds the frames and the receipts above
  `min(posted_head, final_head)`: at least the 1-day challenge window, about 530 GB at the mix
  (section 10.0). It also holds the receipt window of its own design and the receipts imported
  at the cut-over (section 12.3). While no post lands, it grows by about 22 GB an hour at the mix.
  No byte budget bounds it (decision 5).
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
| Submit to receipt p50, p99, p99.9: from the ingress receipt of the submit to the arrival of the receipt on `tx_receipts` at the same ingress (decision 13). One clock. It is an upper bound of the interval by one Aeron hop. | a new histogram `kardamom_ingress_submit_receipt_seconds` (PR 12) |
| Sustained rate with zero loss | `kardamom-load --assert-all-delivered` |
| Leader CPU: process, service thread, consensus thread | container CPU and per-thread `/proc` sampling |
| Network bytes in and out of each member | node exporter or `docker stats` |
| Log bytes written a second, fsync time | the archive directory size, `iostat` |
| Snapshot size and time, with a full dedup window (bound: 2 s, section 5.5) | `sealer snapshot TAKEN`, with a new `ms=` field |
| Feed walk: the walk rate (frames a second from a snapshot to the head), the heap during a walk | the walk log line with `ms=` and `frames=`, `-Xlog:gc` |
| Disk growth while the batcher is stopped: the serving service grows, the members and the executors do not | the disk size of each, `iostat` |
| The scheduled sync: the time of each force, and the durable lag (the age of the newest durable entry); the commit latency with the flusher on and off | `kardamom_sealer_sync_seconds`, `kardamom_sealer_durable_lag_seconds` |
| GC pause p99 of the leader | `-Xlog:gc` |
| Feed lag: member head minus executor index | `kardamom_executor_input_lag` (new) |
| Refusals | `WINDOW_FULL`, `PAST_DEADLINE`, `CONTIGUITY_REJECT` counts |
| Sequencer bytes: parked, unconfirmed, evictions (section 6.2) | the sequencer metrics |
| The twin copy: log bytes of the losing copies; commit latency with one twin stopped | the archive size; `kardamom_ingress_seal_latency_seconds` |

- **Matrix rows for the target.** 5,000 tx/s with the example mix of section 10.0, and 5,000 tx/s
  with 8 KB only, beside the matrix above.
- **The decision.** One Raft group carries the target when, at 5,000 tx/s with the mix and with 8 KB
  only, the p99 of the submit-to-receipt interval stays at or below 50 ms, the leader CPU stays
  under 70 %, and the snapshot stays under 2 s. The result also states whether the p99 reaches the
  ideal of 20 ms, and how the interval splits into the seal latency and the rest. The container run
  answers for CPU, disk, GC and the snapshot. The staging run answers for the network. The result
  also tunes `--parked-bytes`, `--unconfirmed-bytes`, `feedRingBytes`, `readyLagBytes` and the
  sealer heap.
- **The hedge.** If the twin copy is the limit (the leader's network or the log disk), the
  hedged offer of section 6.3 is the next step. The result says so.
- The gate measures the interval of decisions 13 and 14. The 50 ms requirement is read at p99, as
  the ideal is. The 100 ms bound on the commit latency of the first version of this spec goes.
  Today's receipt p99 is 47 ms at the edge (`docs/specs/2026-08-16-pipeline-cost-model.md`, "Gas
  throughput and latency").

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
| All members lose their state | The Raft logs and the snapshots are gone. The serving service still holds the blocks above the posted head. | The fleet rebuild reads the serving service up to its stored head, then L1 for any range that the service lacks (decision 26, #671, `docs/runbooks/sealer-fleet-rebuild.md`). The rebuilt sealer starts at the stored head of the service, at or above the posted head, with a new generation of the history key (#675). |
| L1 posts stop for a long time | The serving service keeps every block above the posted head on disk, and grows above its steady state at the rates of section 10.0. The stored head moves, so the members and the executors do not grow. No byte budget refuses records. The block guard refuses user records at its block budget, as today. | The batcher posts again. The serving service then frees the blocks below `min(posted_head, final_head)`. How the batcher posts a large backlog is out of scope (section 5.5). |
| The disk of a member fills | It happens only while the frames-stored head does not move (an outage of K instances of the serving service). All members keep the same log, so disks of one size fill at about the same time. | The alert `KardamomSealerDiskLow` fires first: free disk under 25 %, or under 6 hours at the current growth rate (`kardamom_sealer_disk_free_bytes` and its derivative). Below the reserve, the member deletes its reserve file, and the ingress pauses the submits while a member of a quorum is low (section 5.5, #678). The pause lifts when the purge frees space. The operator restores the serving service or adds disk. A member that still fills its disk logs a fatal error and stops; it restarts and replays its log. |
| A member's feed bytes differ from the leader's | A bug: R3 is broken. | An executor whose entry fails its hash check drops that source and replays from the other source, with an alert (section 8). The validator checks every canonical entry it reads. A boundary carries no digest of the stream. A stream digest is a later follow-up (section 16.3). |
| A feed ring fills | The member writes a `FEED_GAP` marker (section 7.2). | The executor asks for a replay at once and stays on the feed. |
| A power loss on a majority of the members (sync level 0) | Each member loses the entries after its synced position: about 1 s. The canonical head goes back. The paired executors can be ahead of it. | The recovery of section 11.1: Raft elects the most complete surviving log, and the cluster continues from it. The paired executors roll back by themselves. |
| The serving service stops (every instance) | The stored heads stop. The batcher, the attester, the validator and the unpaired executors stop reading. The ingress serves receipts from its cache and the executors only. Each member keeps its log, and each executor keeps its receipts (section 10.0). | `KardamomExecutorOutputUnstored` fires after 10 minutes, then the disk alerts. When the service returns, each instance reads the blocks above its last block from a sealer member and an executor, the stored heads move, and the members and the executors prune. |
| The member that the serving service tails stops | The service gets no new frame from it. | It falls back to another member, the leader last (section 8.1). It reads from its stored head again. |
| One instance of the serving service stops | The other instances serve reads. The heads that K instances hold stop only when the stopped instance is one of the K highest. | Nomad starts a replacement. It copies the stored range from a peer instance, then tails a sealer (section 8.4). |
| The serving service loses blocks above the posted head | A disk loss of fewer copies than the stored head needs loses nothing: the other copies hold the blocks. A loss of every copy loses the blocks between the posted head and the stored head: the members and the executors already pruned them. | The chain reverts to the posted head (`docs/runbooks/revert_to_posted_head.md`), as after the loss of every member. The design of #585 sets the copies that make this rare. |
| Every executor is down | No block is executed. The serving service gets the frames from the sealer but no receipts, so the receipts-stored head stops. The frames-stored head still moves, so the sealer purge goes on (#679). The sequencers do not re-offer the ordered entries (#680). | The executors return, replay from their feeds or a peer checkpoint, and the serving service goes on. |
| Two members lose their disks, and one survives (#677) | The survivor alone has no quorum. A blank member cannot seed from a peer, because the peer seed needs a closed election. | The runbook of decision 29: stop the survivor, copy its cluster and archive directories to one blank member, and start both. They form a quorum with the log of the survivor. The third member then seeds from a peer as usual. |
| An executor serves a wrong receipt | The serving service checks each receipt's `tx_hash` against the frame from the sealer (section 8.3). | On a mismatch it reads the receipts again from the next executor, and an alert fires. A wrong receipt for the right transaction shows at the validator's receipt check. |
| The disk of an executor fills | It happens only while the stored head does not move. The commit fails, and the executor stops. | `KardamomExecutorDiskLow` fires first. The paired executors of the other hosts fill at about the same rate. |

### 11.1 Recovery after a lost tail of the Raft log

**The causes.**

- **A power loss on a majority.** After the activation of the carry flag, the sealer appends at
  sync level 0 and forces its log every 1 s (section 5.5, decision 16). A power loss on the hosts
  of a majority of the members loses, on each of them, the entries after its synced position:
  about 1 s plus one force.
- **The Aeron 1.44 double vote (#620).** After all members restart together with different log
  tails, one member can vote in two terms. The leader of the lower term then commits its longer
  tail, and the leader of the higher term later cuts it back.
- `H` is the head before the loss. `H'` is the head after the restart.

**The recovery: the cluster continues from its surviving log.**

- The members restart from their snapshots and their logs. Raft elects the most complete log
  among the members that vote. `H'` is the end of that log. The cluster continues from `H'`. It
  waits for no other service, and it restores nothing.
- **The hold after a start (decision 25, #673).** Each member holds its feed, its exposure on the
  serving service, and kind 17 until the election is stable (section 7.1). So no executor
  executes an entry that the double vote can still cut back, and #620 needs no rollback. The
  rollback below stays for the losses that the design accepts: up to 1 s of entries after a power
  loss on a majority, and a coordinated disk failure.
- The entries `(H', H]` are gone. They are at most about 1 s of entries. A sequencer offers an
  entry again after the leader change only when it has no ordered signal for it (decision 31,
  #680). An entry that the sealer ordered and then lost does not come again. Its client sees the
  receipt go away (below) and submits again.
- **Why no longer retention in the sequencers (#642, closed).** Decision 16 keeps the loss window
  at about 1 s. A re-offer gives the lost entries a new order anyway, so the executors that ran
  them roll back in any case. And a longer retention ties the ledger bound of each sequencer
  replica to the serving service (section 6.2). So the sequencer keeps its rule of today.

**What the serving service exposed and what the batcher posted stay.** The serving service exposes
a block only at or below the majority durable position (R8, section 8.1). A majority of the
members forced it, and a forced log survives a power loss. Every election majority holds at least
one of those members, and Raft elects the most complete log among the voters, so the new head
`H'` is at or above it. The batcher posts only blocks at or below the snapshotted head (decision
26, section 8.2). A restarted member restores from its snapshot, so its sealed head is at or above
the posted head, also after the double vote. The posted head is the confirmation bound for a
client (below).

**A coordinated disk failure.** The disks of a majority fail together. This is very rare.

- With a snapshot left, the cluster restarts from a sealer snapshot (a surviving member's, by the
  peer seed of today) and overrides the blocks after it. The snapshot is at or above the posted
  head (decision 26).
- With no snapshot left, the fleet rebuild reads the serving service, then L1 (#671).
  `kardamom-reconstruct --sealer-seed` gets a source on the serving service: it replays the stream
  up to the stored head of the service, and it takes from L1 any range that the service lacks. The
  rebuilt sealer starts at the stored head of the service, which is at or above the posted head.
  A runbook covers it.
- The rebuild starts a new Aeron log, so the terms start again at 0. So the seed carries a new
  generation of the history key: the clock time of the rebuild in ms (decision 24, below).
- **Two blank members and one survivor (decision 29, #677).** The survivor alone has no quorum,
  and a blank member cannot seed from a peer. The runbook: stop the survivor, copy its cluster
  and archive directories to one blank member, and start both. They form a quorum with the log of
  the survivor. The third member then seeds from a peer. A chaos case follows the runbook and
  checks that no committed entry is lost.

**The executor on a divergence.** A paired executor runs entries in about 5 ms, before they are
durable, so it can hold blocks, and published receipts, that the new stream does not have. On
each start and each new connection, it compares the stream with its own history, finds the
divergence point `d`, restores the newest checkpoint below `d` (local, then a peer with
`max_block`), and executes the new stream from it. With no checkpoint below `d`, it stops for the
operator. #655 built this path (`crates/engine/src/reader/carried/compare.rs`). It runs only on
the carried path, so it is live only after the activation of the carry flag. The serving service
runs the same comparison on each reconnect (one code path, STYLE R14, section 8.1).

How the consumers of the receipts learn of the rewrite has two forms: the history key, which is
the target, and the rollback record of #655, which is the current state.

**The target: the history key (decision 24, #675).** Each block carries the key
**(generation, term)**, compared in that order. On a divergence the executor restores a checkpoint
and publishes the new history again. Each consumer finds the rewrite itself, with one rule.

- **term:** the Aeron `leadershipTermId` of the term in which the sealer sealed the block. The
  sealer learns it from `onNewLeadershipTermEvent`, in log order, so every member and every replay
  stamps the same value. Within one term the log of a leader only grows. A log is cut back only
  when a new term starts, and the new history is always in the higher term. This also holds under
  the double vote (#620).
- **generation:** 0 at genesis. A sealer fleet rebuild sets it to the clock time of the rebuild in
  ms. `kardamom-reconstruct --sealer-seed` writes it into the seed file, and the sealer keeps it
  in its state and its snapshots.
  - The time comes from an NTS-authenticated clock (for example `time.cloudflare.com`), or from a
    host clock that chrony keeps in step with it.
  - The design assumes that two coordinated failures never happen within one millisecond, and
    that clock skew is not an issue.
  - The tool refuses a generation at or below the highest generation in its rebuild source.
- **The consumer rule.** For a receipt or a boundary at a position that the consumer already
  holds:

| Key compared with the held version | Action |
|---|---|
| Equal, same transaction | A copy from another executor: ignore it. |
| Higher | The history is replaced: drop everything held at or above the position, then apply. |
| Lower | A late frame of an old history: ignore it. |
| Equal, different transaction | Impossible under Raft: halt. |

- A consumer that restarts asks for a replay from a little below its cursor, and the same rule
  repairs what it missed. So a consumer that is down during the rewrite does not miss it (G5).
- One shared library implements the rule for the ingress cache, the state mirror, the sequencer
  floors, the validator and the serving service (STYLE R14).
- `BlockBoundaryStart` and `BlockBoundary` carry the key. This is a layout change, so a member and
  an executor write it only after the activation of the carry flag (#661).
- **The last step** of #675 removes the `ReceiptRollback` record, the receipt ledger, the running
  marker, the rollback marker and the `AppliedRollbacks` set of #655.

**The current state: the rollback record of #655.** Until #675 lands, the executor signals a
rewrite with an explicit record. It too runs only on the carried path, after the activation of the
carry flag.

- **The comparison.** The window is the last `--rollback-compare-blocks` committed blocks (default
  32). The executor compares the `tx_hash` of each receipt by index and the end index, the
  timestamp and the L1 origin of each header. Above its cursor, it compares the receipts that it
  already published, from its receipt ledger. `StreamDiverged` (below the cursor) restores a
  checkpoint. `PublishedDiverged` (only above it) keeps the state and voids the published receipts.
  With no difference, it publishes nothing, so a clean restart sends no rollback. When the receipt
  buffer of section 8.3 lands, the floor of the window becomes `output_floor_block`.
- **A divergence below the window.** The process compares again with a window 4 times wider, up
  to 4096 blocks, then stops with `DivergedBelowWindow` for the operator. So the executor never
  publishes a `to_block` from a guess at the floor.
- **The receipt ledger and the running marker.** The commit thread writes each receipt to the
  receipt ledger (`<state-dir>/marker-receipts`, a ring of 65,536 slots) before it publishes it.
  The running marker (`<state-dir>/marker-running`) holds the boot id of the host while a run is
  live. A marker of another boot means that the ledger can be short, and the first frame above
  the cursor counts as a difference.
- **The durable rollback marker.** The process writes `<state-dir>/marker-rollback` with a disk
  sync before it parks the diverged state. Before the first receipt of the next run, the commit
  thread publishes `ReceiptRollback { to_block, id }`, and removes the marker only after the
  publish succeeds. So the signal survives a crash.
- **The record.** `to_block = d - 1`. `id` is a hash of `to_block` and the first frame of block `d`
  in the new stream, so one divergence has one `id`. The serving service publishes the same record
  with the same `id` after its comparison (#585, G4). The record is its own 40-byte
  `receipt-rollback` frame on `tx_receipts`, not a variant of the stream enum. A consumer of an
  older release fails to decode it as a batch, and skips it.
- **The consumers.** The ingress, the state mirror and the sequencer apply each `id` once
  (`kardamom_cache::AppliedRollbacks`, 64 ids):
  - the ingress drops the cached receipts above `to_block` and their hashes in its first-wins set,
    empties its local account layer and its cold-account map, lowers its head and its fee history,
    and closes each `kardamom_subscribeReceipts` subscription with an error that names the
    rollback;
  - the state mirror deletes the `acct:` and `rcpt:` keys of Redis and rebuilds its account rows;
  - the sequencer resets each nonce floor that a receipt above `to_block` set;
  - the validator withdraws the buffered receipts, rows and BALs of every session above
    `to_block`. It applies each copy: a late copy only leaves positions unverified.
- **The limit (G5).** A consumer that is down while the record goes out misses it. The history key
  closes this.

**What a client sees.**

- A receipt from the executor comes in about 5 ms, before its entry is durable. After a loss, a
  receipt of a block above `H'` goes away: `eth_getTransactionReceipt` answers `null`, and then a
  new receipt if the transaction is ordered again (a new block, a new index, maybe another
  status), or `null` for good.
- The serving service exposed nothing above the majority durable position, so it needs no
  correction after a power loss.
- The notifier no longer exists (decision 12). There is no status feed to correct.
- **Confirmation (decision 16).** A transaction is confirmed when the batcher posted its block to
  the DA layer: the posted head. The ingress answers the block tag `finalized` with the posted
  head (`kardamom_blockNumberByTag`).

**The nonce floors and the sealer guard after a loss.**

- **The sealer guard.** Its expected nonces are replicated state at `H'`, so they match the chain.
  A re-offer that leaves a gap gets `CONTIGUITY_REJECT`, and the sequencer frees the nonce as
  today (#561).
- **The sequencer floors.** A floor learned from a receipt of a replaced block is wrong. The
  sequencer resets it to unknown and learns it again by the resync of today (the local layer,
  Redis, then the executor).

**Who detects the loss.**

| Detector | Signal | Action |
|---|---|---|
| Raft | The election after the restart | The most complete surviving log wins. The cluster continues after the hold. |
| A paired executor | The comparison at its start or new connection finds `d` | It restores a checkpoint below `d` and publishes the new history |
| An instance of the serving service | The comparison on its reconnect finds `d` | It drops its frames above `d` |
| The consumers of `tx_receipts` | Target: a higher history key at a held position. Today: the `receipt-rollback` frame, once for each `id` | The ingress, the state mirror, the sequencers and the validator drop what the new history replaced |

**Proof.** A chaos case `sealer-tail-loss` (section 13.1). A process kill keeps the page cache,
so the case puts the cluster directories of two members on a device that drops unsynced writes
(device-mapper `dm-flakey` with `drop_writes`), and stops both under load. It checks that the
loss is at most about 1 s, that no block that the serving service exposed changes, that the
paired executors roll back by themselves, and that their roots match the validator's. It also
runs the double-vote order of #620: no executor executes a cut-back entry, and the sealed head
never goes below the posted head (#671, #673). The case runs after the activation of the carry
flag, because the rollback runs only on the carried path.

## 12. Migration

### 12.1 Formats

Every deploy is backwards compatible (decision 19). So a member, a client or a consumer writes
each new format below only after the activation of its flag, and the release reads the old and
the new layout. Today `formats.toml` and the format gate (`just check-formats`) check the formats.
#664 removes both after #663 runs in CI. Then the compatibility fixtures of #663 check each
format: the new release reads the samples of the last release, and with every new flag off it
writes the same bytes. There is no mixed-version chaos shard (decision 33).

| Format | Today | After the activation |
|---|---|---|
| `sealer-ingress-kinds` | kinds 0 to 9 | With no activation, the clients write kinds up to 8 (#666). Kind 11, the feature activation (#666), switches the rules on and off (section 5.4). Kind 12 `STORED_CURSOR` (#639), `[kind=12][instance_id:u64][stored_head:u64]`: a member applies it only after the activation of the carry flag (section 5.5). Record type 5 rides kinds 0 and 3. After the activation of the carry flag, kinds 6 and 9 drop as unknown. Kind 10 (the nonce release of #634) goes (#676). A member of an older release drops kinds 11 and 12 as unknown kinds. |
| `sealer-egress-kinds` | kinds 1 to 13 | Kind 17 under version 3 (#666); it also reports the carry rules. The switch marker of the carry flag (section 4.3). After the activation of the carry flag: kinds 15 and 16 (16 on the feed only). Kind 13 is never sent. Kind 14 is not used. The status frame stays 68 bytes: the 76-byte frame of the retained file goes (decision 10). |
| `sealer-record-types` | types 0 to 4 | After the activation of the carry flag: types 5 and 6. Types 0 and 4 are not written. |
| `sealer-snapshot` | v10 | v10 before any activation. v13 after the first activation record of version 3 (#666): v10 with the decision rules and the latest activation time (#670). v12 after the activation of the carry flag. v12 has no retention section, and carries the stored heads of the instances and the generation of the history key (section 5.5). It changes in place until the first activation of the carry flag on a network. A later release stops the restore of v10 and v13 (PR 14). |
| `aeron-stream-records` | `TxEnvelope` frozen | Unchanged. The envelope travels in its frozen layout. |
| `discovery-record` | `Topic::ExecTxs` | `Topic::SealerFeed` added. `ExecTxs` is removed only after no node advertises it (`crates/log/src/discovery/record.rs:279-296`). |
| `sealer-feed` (new) | none | The feed frames are the egress kinds. The feed control request is the kind-1 layout. |
| `tx-heads` (new, PR 11) | none | Frozen layout of `TxHeader`. |
| `state-db` | 11 tables | The meta key `output_floor_block` (section 8.3). A reader of the base ignores an unknown key, so the change is forward-compatible. |
| `executor-output` (new) | none | The receipt read of section 8.3: the request and the binary answer. Frozen at its first release. Its transport follows the design of #585. |
| `receipt-rollback` (new, #655) | none | The 40-byte frame `ReceiptRollback { to_block, id }` on `tx_receipts` (section 11.1). Only an executor on the carried path sends it. A consumer of an older release fails to decode it as a batch, and skips it. The last step of #675 removes it. |
| `executor-markers`, `receipt-ledger` (new, #655) | none | The rollback marker, the running marker and the receipt ledger in the state directory of an executor on the carried path (section 11.1). The last step of #675 removes them. |
| `sealer-seed` | the seed of today | The active decision rules: the activation time of each followed feature at the head of the seed (#667). The generation of the history key: the clock time of the rebuild in ms (#675). The sealer reads both seed versions. |
| Boundaries on `tx_receipts` (`BlockBoundaryStart`, `BlockBoundary`) | no history key | The history key (generation, term), written only after the activation of the carry flag (section 11.1, #675). |
| `batcher-spool`, `batcher-cursor` | unchanged | Unchanged. |

- **No waiver.** Until #664 removes the format gate, a format that the base cannot read names its
  flag in `formats.toml` (`activation = { flag = ... }`), as #666 does for kinds 11 and 17. No
  format of this program gets a `[one_way]` or a `[coordinated]` waiver.
- The scheduled sync and the durable position (#645) change no format and no decision. Whether
  #645 needs a flag is a check of #665 (section 12.4).
- **The snapshot rule.** Before the activation, the new release writes the snapshot of the base
  (v10, or v13 after the activation record of version 3). So a mixed fleet runs, and a rollback
  is free (#663). The removal of the cursor and the void sections rides the activation of the
  carry flag: after it, every member writes v12. The gate of #664 then refuses a rollback past
  the release that brought the flag.

### 12.2 The cut-over: an upgrade transaction (decision 20)

The owner replaced the chain reset of staging and the drained cut-over of production with the
protocol upgrade process (#661, the decision of 2026-10-18 on #598). Staging and production take
the same path. So staging also tests a network upgrade.

**The carry flag.**

- One chain feature flag in `KardamomChainState`, feature id 3, with an optional activation time.
  It replaces `kardamom.cluster.carryBytes` and `--carry-bytes`: they are no longer operator
  settings.
- Before the switch, the old path runs: the `TxRef` frames, the void, the join and the archives.
  After it, the carried path runs. The old path stays in the code until the activation on every
  network. A later release removes it (PR 14, #599).

**The switch of each component (decision 21).**

- **The sealer** learns the activation from a feature activation record (ingress kind 11, section
  5.4), and switches at the first log entry at or after the activation time. It marks the switch
  in the ordered stream (section 4.3).
- **Version 3 first.** Version 3 must be active before the carry flag. The sealer ignores an
  activation of the carry flag unless version 3 is active.
- **The executors** switch at the next block boundary after the switch marker. Each block driver
  also reads the flag (`crates/exec-core/src/features.rs`).
- **The sequencers** start the full-entry offer after kind 17 reports the carry rules on their
  session.
- **The readers** keep both the `TxRef` and the full-entry decoders until #599. So a consumer that
  lags behind the switch reads the frames below it.
- **The seed** carries the active rules (#667). A sealer that starts from a seed after the
  activation keeps the carry rules.
- **The validator** keeps its attester leaves across the switch.
- **Sync level 0** turns on with the carry flag. Until the flag is active, the member keeps its
  sync on each append (#645, section 5.5).
- **Both ways (decision 23).** A later upgrade transaction can turn the carry flag off. Every
  component then switches back at one deterministic point, as it switches on. PR 13 builds and
  tests the way back, until #599 removes the old path.

**Before the upgrade transaction.**

- Every service runs the release of PR 13. #664 adds the gate check.
- Version 3 is active on the network (section 5.4).
- The serving service runs (section 8.1). Its receipt import can start (section 12.3).
- The pair mounts of section 7.6 exist. The node_containers role (`kardamom-infra`) creates them
  before the node containers start.
- `recordLagBudget` stays 0, as deployed. An executor on the carried path sends no recorded
  cursor (the review of #635 on #598).
- The host memory and the disk meet the preconditions of section 5.5.

**The steps.** Staging first, then production.

1. Deploy PR 13 as a rolling release, with the carry flag off. With the flag off, it writes the
   formats of the release before (#663).
2. Check: `just smoke`, the canary and one chaos shard. The old path runs unchanged.
3. Send the upgrade transaction: `ETHLockbox.initiateUpgrade` with the carry flag and an
   activation time.
4. Check again: `just smoke`, the canary and one chaos shard. Each paired executor reads its
   member feed (`kardamom_executor_input_source{source="feed"}`). The validator and the batcher
   read the serving service. The executor roots equal the validator's.

- The chaos case `cutover-rehearsal` runs the same steps in the container cluster, under load
  (section 13.1).
- No step pauses the submits, flushes the batcher, purges the sealer job, or restores a drained
  snapshot. The chain keeps its history on both networks.
- Call `D` the last block before the switch.

### 12.3 The receipt import (decision 14)

- **The import.** Before the serving service sends its first stored head, it imports the existing
  receipts from one executor: the `receipts` and `tx_hash_index` rows of every block at or below
  `D`, through the receipt read of section 8.3. It has no frames of these blocks: their frames
  carry no bytes, and L1 holds the bytes. So the service answers a receipt lookup for them, and a
  replay below `D` gets `REPLAY_UNAVAILABLE`.
- **No race.** The service sends its first stored head, `D` or above, only when the import is
  done. An executor prunes nothing before its first stored head, so the import does not race a
  prune. Until then the sealer purges nothing (section 5.5).
- **The challenge window.** The output roots of the day before `D` are still in their challenge
  window. The service holds their receipts from the import. Their transaction bytes are on L1.
- **The size.** The import size is the receipt history of the chain: about 0.3 KB for each
  transaction without logs, more with logs. The operator states it, and the disk of the service,
  before the upgrade transaction. The import can start from a checkpoint copy before the upgrade
  transaction, and catch up to `D` after the switch.
- Staging keeps its chain now, so it imports too.

### 12.4 Deploy order

1. PR 1 to PR 11 merge with the flags off. Each deploys as a normal rolling release. They change
   no format that the base cannot read.
2. PR 12 (benchmark) runs on the flagged build in a container cluster and on staging
   (section 10.6).
3. PR 13 ships the carried path behind the carry flag, off. It deploys as a rolling release.
   Then each network sends the upgrade transaction (section 12.2): staging first, then
   production.
4. PR 14 (#599) removes the old path. It merges only after the activation of the carry flag on
   every network.

**Decision 10 adds PR 15 to PR 17.** PR 15, 16 and 17 change the sealer and v12. No network
writes v12 before the activation of the carry flag, so they change v12 in place:

1. #634 (PR 6, with the retained-file section in v12) merged as it was. The removal touches the
   same classes. One removal after it is less work than a rework of #634.
2. PR 16 (#638, the feed walk of the Raft log) merges next. The feed then no longer reads the file.
3. PR 15 (#637, the removal of the file and its v12 section) merges after PR 16, and before
   PR 13.
4. PR 17 (#639, the stored cursor, ingress kind 12) merges after #634, and before PR 13.
5. v12 is frozen at the first activation of the carry flag on a network.

**Decision 19 adds #666, #663 and #664.**

- #666 (#662, decision version 3 by an upgrade transaction) merges before PR 13. Version 3
  activates on a network before the carry flag.
- PR 13 passes the compatibility fixtures of #663. There is no mixed-version chaos shard.
- #664 adds the gate check of the active flags of the chain, and removes the format gate after
  #663 runs in CI.

**Decisions 15 and 16 add PR 19 and PR 20** (section 11.1). PR 18 (#642, the sequencer
retention) is dropped. PR 19 (#643) merged as #655. Its floor becomes the `output_floor_block` of
the receipt buffer when that buffer lands. PR 20 (#644, the recovery from the surviving log) needs
the scheduled sync and the durable position (#645).

**The order of sync level 0 (decision 18).** #645 sets sync level 0 and adds the 1 s scheduled sync
and the durable report itself. #645 does not merge before #643 (the executor rollback, merged as
#655). So no release runs at sync level 0 without the scheduled sync, or without the executor
rollback.

- The rollback of #655 runs only on the carried path (section 11.1). So sync level 0 must not run
  on a cluster before the activation of the carry flag on that cluster.
- The carry flag drives sync level 0 (decision 21): until the flag is active, the member keeps
  its sync on each append. So #645 can merge at any time, and no deploy order can lose data.

### 12.5 Rollback

This section applies to staging and production alike.

- **Before the activation, a rollback is free.** With the carry flag off, the release writes the
  formats of the release before (#663). So `just rollback` returns to it as a normal rolling
  release.
- **After the activation.** The gate of #664 refuses a rollback past the release that brought the
  carry flag. A sealer of an older release refuses v12, and an older executor does not read the
  carried frames. A fault after the activation is fixed forward.
- The step-5 backups of the drained cut-over, the coordinated restart into the old release, and
  the rollback floor of the formats (`docs/formats.md`) go.
- The fleet rebuild from the serving service and L1 (`docs/runbooks/sealer-fleet-rebuild.md`,
  #671) stays the path after the loss of every member (section 11). It is not a rollback path.

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
| 13 | release: cut-over to bytes in the Raft log, behind the carry flag | 12, 15, 16, 17, 19, 21, 23, #663, #664, the serving service and the executor handoff |
| 14 | cleanup: remove the void, the join and the archives (#599) | 13, and the activation of the carry flag on every network |
| 15 | sealer: remove the retained file and its v12 section (#637) | 6, 16 |
| 16 | sealer: the member feed replays from the Raft log (#638) | 7 |
| 17 | sealer: the stored cursor (ingress kind 12) and the purge floor (#639) | 6 |
| 18 | (dropped: #642 closed by decision 16, section 11.1) | none |
| 19 | executor: roll back by itself after a power loss (#643, merged as #655) | none; its floor moves to the receipt buffer of section 8.3 when it lands |
| 20 | sealer: recovery from the surviving log after a power loss (#644) | 17, 25, 27, #645 (the scheduled sync and the durable position) |
| 21 | sealer: activate decision version 3 by an upgrade transaction (#662, PR #666) | 6 |
| 22 | executor: a skipped transaction uses its nonce; remove the nonce release (#676) | 21 |
| 23 | sealer seed: carry the decision rules (#667) | 21 |
| 24 | sealer: switch decision version 3 off again (#670) | 21 |
| 25 | sealer: hold the feed, the egress and kind 17 until the election is stable (#673) | none |
| 26 | consumers: the history key (generation, term) replaces the rollback record (#675) | 13, 19 |
| 27 | batcher: post only snapshotted blocks; rebuild from the serving service and L1 (#671) | the serving service |
| 28 | attester and batcher: attest at or below the posted head; check the spool (#672) | 27 |
| 29 | sealer: a runbook and a chaos case for two blank members (#677) | none |
| 30 | sealer: a disk reserve file, and an ingress pause while a quorum is low on disk (#678) | none |
| 31 | serving service: a frames-stored head and a receipts-stored head (#679) | 17, the serving service |
| 32 | sequencer: clear an entry from the re-offer ledger on the ordered signal (#680) | 9 |
| 33 | validator: resume at or below the attested head after an attester restart (#681) | none |

The executor side of the handoff (section 8.3: the receipt read, the prune, the alerts) and the
ingress fallback to the serving service (section 8.4) are issues that the design of #585 opens,
because their transport follows that design (decision 14).

The PRs that put a change behind `--carry-bytes` or `kardamom.cluster.carryBytes` keep that setting
until PR 13. PR 13 replaces each one with the carry flag of the chain (decision 20). PR 21 to PR 24
and #663, #664 and #665 come from the upgrade rule (decision 19, #661). PR 25 to PR 33 come from
the failure review of 2026-10-18 (decisions 24 to 32).

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
- Status: merged as #634. PR 21 (#666) replaces its coordinated restart to version 3 with an
  upgrade transaction (section 5.4).

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
  The histogram `kardamom_ingress_submit_receipt_seconds` of the submit-to-receipt interval
  (decision 13). The results go into this spec as section 17.
- Acceptance: section 17 answers whether one Raft group carries 5,000 tx/s with the mix and with
  8 KB only, gives the tuned values, and says if the hedge of section 6.3 is needed.

**PR 13 (#598). release: cut-over to bytes in the Raft log, behind the carry flag.**

- Scope (section 12.2):
  - the carry flag of the chain, off, in place of `kardamom.cluster.carryBytes` and
    `--carry-bytes`;
  - the switch of each component at the activation (decision 21, section 12.2): feature id 3,
    the switch marker in the ordered stream, the executors at the next block boundary, the
    full-entry offer after kind 17 reports the carry rules, both decoders in every reader, the
    attester leaves across the switch, sync level 0 driven by the flag, and the way back
    (decision 23);
  - the pair mount and `feedAeronDir` in the Nomad jobs and in `terraform/containers`;
  - the runbook of the upgrade step and its rehearsal case;
  - `KardamomSealerDiskLow`, `KardamomExecutorOutputUnstored`, `KardamomExecutorDiskLow`;
  - `recordLagBudget=0`, and the chaos cases of 13.1.
- The pair mount of the staging hosts is a `kardamom-infra` change to the node_containers role,
  merged before the upgrade transaction on staging.
- Not in scope: the waivers, the rollback floor, `POST /flush` on the batcher, the drain and the
  chain reset. No import of the v10 frames: the switch drops them (section 5.5).
- Depends on: PR 12, PR 15, PR 16, PR 17, PR 21, PR 23, #663 and #664, the serving service of
  section 8.1 and the executor handoff of section 8.3 in place.
- Acceptance:
  - `cutover-rehearsal` passes in the container cluster: deploy, check, the upgrade transaction,
    check.
  - The compatibility fixtures of #663 pass with the carry flag off.
  - On staging: the rolling deploy passes `just smoke`, the canary and one chaos shard. After the
    upgrade transaction they pass again.

**PR 14. cleanup: remove the void, the join and the archives.**

- Scope: everything in section 9.1: the old path. It merges only after the activation of the
  carry flag on every network (decision 19). The restore of v10 and v13 goes in the release after.
- Acceptance: no reference to the removed items is left. The chaos shards pass.

**PR 15 (#637). sealer: remove the retained file and its v12 section.**

- Scope: sections 5.5 and 9.1 (decision 10). Remove the retained file of #608 and every class
  that only it uses, the `/retained` endpoint and its token, the retained-file copy of the peer
  seed, the 76-byte status, and the retention section of v12. After the activation of the carry
  flag, the egress retains no frame and answers an egress replay with `REPLAY_UNAVAILABLE`, and
  the member drops its `TxRef` frames. Keep `kardamom_sealer_disk_free_bytes`.
- Depends on: #634 merged, PR 16 merged. Before PR 13.
- Acceptance: no reference to the removed classes is left. A `TestCluster` with `carryBytes` on
  has equal v12 snapshot bytes on every member, with no retention section. A member with
  `carryBytes` on restores a v10 snapshot with retained frames, and every member then holds the
  same state. A member with `carryBytes` off refuses v12. The status frame is 68 bytes. (PR 13
  turns `carryBytes` into the carry flag.)

**PR 16 (#638). sealer: the member feed replays from the Raft log.**

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

**PR 17 (#639). sealer: the stored cursor and the purge floor.**

- Scope: section 5.5. Ingress kind 12 `STORED_CURSOR` with an instance id, the replicated head
  of each instance, its place in v12, the clamp to the sealed head, the purge floor at the K-th
  highest head (one-way, K = 2 by default), the heads on `/status`, the log line
  `cluster STORED-CURSOR`. The Rust encoder of kind 12 in `crates/cluster-adapter`.
- Depends on: #634 merged. Before PR 13.
- Acceptance: a `PurgePlanner` test: after the activation of the carry flag, the purge point never
  passes the K-th highest stored head, whatever the posted head; with fewer than K instances, the
  member purges nothing; a kind 12 above the sealed head never moves the floor. A `TestCluster`
  test: every member restores the same heads from v12. A member of the base drops kind 12, and so
  does a member before the activation of the carry flag.

**PR 18 (#642).** Dropped by decision 16. The sequencer keeps its rule of today (section 11.1).

**PR 19 (#643). executor: roll back by itself after a power loss.**

- Status: merged as #655.
- Scope: section 11.1, the automatic rollback, on the carried path only.
  - The comparison at each start and new connection, over a window of committed blocks, with the
    receipts (`tx_hash` by index), the headers, and the published receipts of the receipt ledger.
  - The wait on `REPLAY_AHEAD`, and the wider window below the floor (up to 4096 blocks, then a
    stop for the operator).
  - The choice of the newest checkpoint below the divergence (local, then a peer with
    `max_block`), the park and the restore.
  - The receipt ledger, the running marker with the boot id, and the durable rollback marker.
  - The `receipt-rollback` frame `ReceiptRollback { to_block, id }`, one `id` for each
    divergence.
  - The reactions, once for each `id`: the ingress (the cache, the first-wins set, the account
    layer, the subscriptions, the fee history), the state mirror (the `acct:` and `rcpt:` keys,
    the account rebuild) and the sequencer (the floors learned above `to_block`). The validator
    withdraws the receipts, rows and BALs of every session above `to_block`.
- Acceptance: an executor with equal history resumes with no rollback, and a clean restart
  publishes nothing; a diverged executor restores the newest checkpoint below `d` and reaches the
  root of the new stream; with no checkpoint below `d` it stops; the consumers drop the receipts
  above `to_block`, once for each `id`.
- Follow-ups: the floor moves to `output_floor_block` when the receipt buffer of section 8.3
  lands. A consumer that is down while the record goes out still misses it. PR 26 (#675)
  replaces the record with the history key, and its last step removes the record, the ledger and
  the markers.

**PR 20 (#644). sealer: recovery from the surviving log after a power loss.**

- Scope: section 11.1. The proof that a 3-member cluster with sync level 0 and the scheduled sync
  elects the most complete surviving log after a power loss on two members, loses at most about
  1 s, and continues after the hold of PR 25. The coordinated disk failure: the restart from a
  sealer snapshot, or the rebuild from the serving service and L1 (PR 27). The reaction of the
  serving service when its member's stream goes below a block that it exposed. The runbook. The
  chaos case `sealer-tail-loss`, with the double-vote order of #620.
- Depends on: #645 (the scheduled sync and the durable position), #639, PR 25, PR 27.
- Acceptance: a `TestCluster` test: two members restart with logs cut back to their durable
  positions; the elected head is at or above the majority durable position that the serving
  service last computed; every member then holds the same stream. `sealer-tail-loss` passes, and
  the sealed head never goes below the posted head.

**PR 21 (#662, PR #666). sealer: activate decision version 3 by an upgrade transaction.**

- Scope: section 5.4. The sealer starts on version 2 and switches to version 3 at one log entry,
  through the feature activation record (ingress kind 11) that the sequencer sends in front of
  the epoch of the upgrade transaction. Snapshot v10 before the activation record, v13 after it.
  The `[one_way]` and `[coordinated]` waivers of #634 go. PR 22 removes the nonce release that
  #666 still gates.
- Depends on: #634 merged. Before PR 13.
- Acceptance: a golden stream with the activation in the middle gives the version 2 outcomes
  before it and the version 3 outcomes after it; a replay from a snapshot before the activation,
  with a pending activation, and after the switch gives the same outcomes.

**PR 22 (#676). executor: a skipped transaction uses its nonce; remove the nonce release.**

- Scope: section 5.4, decision 22. With `FEATURE_SEALER_V3` active at the block, every block
  driver burns the nonce of a skipped transaction. The ingress and the sequencer move the floor
  after a burning skip, in a backwards compatible way. Remove ingress kind 10, the release marks,
  `onRelease`, the release queue of the sequencer and its `ActivationWatch` release logic, the
  release gate of #666, the docs and the e2e case S13d.
- Depends on: PR 21.
- Acceptance: a skip with the flag on burns the nonce, and with the flag off it does not; a
  `nonce_too_high` skip does not burn; the validator and the replay agree with the live engine;
  the e2e case shows a stuck sender before the upgrade and a burnt nonce after it.

**PR 23 (#667). sealer seed: carry the decision rules.**

- Scope: section 5.4. The seed builder reads the activation word of each followed feature at the
  head of the seed. A new seed layout carries it. A sealer that starts from it holds the
  activation. The rebuild runbook drops the step that sends the upgrade again.
- Depends on: PR 21. Before PR 13, so that the seed also carries the carry flag.
- Acceptance: a seed of a state with the flag active starts the sealer on version 3; a seed with
  no flag starts it on version 2.

**PR 24 (#670). sealer: switch decision version 3 off again.**

- Scope: section 5.4, decision 23. The sealer keeps the latest activation time of each followed
  feature, and switches on and off at one log index. Kind 17 and a snapshot at each switch. The
  sequencer follows kind 17 in both directions. `just sealer-upgrade-check` covers the off
  direction.
- Depends on: PR 21.
- Acceptance: a golden stream with on, off and on again; a replay from a snapshot between the
  switches; an e2e case that turns the feature on, off and on.

**PR 25 (#673). sealer: hold the feed, the egress and kind 17 until the election is stable.**

- Scope: section 7.1, decision 25. The stability condition, `leadershipTermId` on the admin
  status, the hold of the member feed, of the exposure on the serving service and of kind 17, and
  the majority rule after the stall limit.
- Depends on: none. It needs no flag.
- Acceptance: `sealer-tail-loss` with the double-vote order of #620: no executor executes a
  cut-back entry, and no rollback goes out.

**PR 26 (#675). consumers: the history key replaces the rollback record.**

- Scope: section 11.1, decision 24. The key (generation, term) on `BlockBoundaryStart` and
  `BlockBoundary`, written only after the activation of the carry flag. The sealer stamps the
  key, and the seed carries the generation. The shared consumer library and its use in the
  ingress cache, the state mirror, the sequencer floors, the validator and the serving service.
  The executor publishes the new history again after a restore. The last step removes the
  `ReceiptRollback` record, the receipt ledger, the markers and `AppliedRollbacks` of #655.
- Depends on: PR 13, PR 19.
- Acceptance: each row of the consumer rule has a test; a consumer that is down during a rewrite
  repairs its state from a replay below its cursor.

**PR 27 (#671). batcher: post only snapshotted blocks; rebuild from the serving service and L1.**

- Scope: sections 5.5, 8.2 and 11.1, decision 26. The snapshotted block on the admin status of a
  member, the snapshotted head of the serving service, the batch limit, the post mode at each
  snapshotted head and its deploy profile, and the source on the serving service for
  `kardamom-reconstruct --sealer-seed`, with its runbook.
- Depends on: the serving service.
- Acceptance: `sealer-tail-loss` with the double-vote order of #620: the sealed head never goes
  below the posted head.

**PR 28 (#672). attester and batcher: attest at or below the posted head; check the spool.**

- Scope: sections 8.2 and 8.3, decision 28.
- Depends on: PR 27.
- Acceptance: the attester refuses a block above the posted head; the spool drops a block with a
  hash that the serving service does not have; a chaos case with a rollback above the posted head
  while a group is spooled and an attestation is due: no root and no post names a removed block.

**PR 29 (#677). sealer: a runbook and a chaos case for two blank members.**

- Scope: section 11.1, decision 29.
- Acceptance: the chaos case wipes two members, follows the runbook, and no committed entry is
  lost.

**PR 30 (#678). sealer: a disk reserve file, and an ingress pause while a quorum is low on disk.**

- Scope: section 5.5, decision 30. The reserve file setting, the free disk on the admin status,
  the ingress pause and its `kardamom_chainStatus` field, and `KardamomSealerDiskLow` before the
  pause. No flag: the pause is outside Raft.
- Acceptance: `sealer-disk-full` (section 13.1).

**PR 31 (#679). serving service: a frames-stored head and a receipts-stored head.**

- Scope: sections 5.5 and 8.3, decision 27. The frames-stored head sets the purge floor, and the
  receipts-stored head sets the executor prune. The K-of-N copy rule applies to each.
- Depends on: PR 17, the serving service.
- Acceptance: with every executor down, the frames-stored head moves and the sealer purges.

**PR 32 (#680). sequencer: clear an entry from the re-offer ledger on the ordered signal.**

- Scope: section 6.3, decision 31. The sequencers read egress kind 15 on their session, and offer
  an entry again only after a session loss or a leader change, with no ordered signal for it.
- Depends on: PR 9 (kind 15).
- Acceptance: with every executor down, no ordered entry enters the Raft log again; after a leader
  change, an entry with no ordered signal is offered again.

**PR 33 (#681). validator: resume at or below the attested head after an attester restart.**

- Scope: section 8.3, decision 32.
- Acceptance: a test restarts the attester with withdrawals between the attested head and the
  validator cursor, and each one lands in a root.

Rules to check in each pull request: R1 (comments carry no phase or spec names), R5 (the feeder, the
ring and the walk thread each have one owner, and the feed ring has one writer at a time), R6 (the
feed is an associated type of the reader's wiring, not `dyn`), R9 (parse each frame once at the
boundary), R12 (checked arithmetic for the byte counters and the wire lengths), R14 (one codec and
one ingest path for the feed and the serving service), R16 (the release drain stays a one-step loop
body).

### 13.1 Chaos cases

| Case | Change | Assertion |
|---|---|---|
| `archive-driver-loss`, `archive-tx-data-wipe`, `archive-corruption` (`crates/chaos/src/cases/archive.rs`) | Remove the `tx_data` parts. Keep the `tx_deposits` archive loss. | The deposit path recovers. |
| `pipeline-blackout-recover` (`crates/chaos/src/cases/coordinated.rs:104-150`) | Drop the void-decision count. | Every acknowledged transaction has a receipt after the blackout. No `VOID` line exists. |
| `hard-executor`, `graceful-executor` (`crates/chaos/src/cases/component.rs:7-34`, `exec_stream.rs`, `recorded_cursor.rs`) | Drop the stream and cursor checks. | The restarted executor resumes through its feed (`input_source="feed"`) with no gap. |
| `batcher-outage-past-retention` (`crates/chaos/src/cases/l1/outage.rs`) | The first half stays: freeze past the floor and a snapshot, thaw, recover from the spool and the replay of the serving service. The second half proves the disk retention: freeze the batcher with SIGSTOP and load until the serving service holds more than 1 GiB above the posted head; thaw. | First half: the rebuild line (`crates/chaos/src/cases/l1/batcher.rs:22-23`) count does not grow. Second half: no `DA_LAG_REJECT` while the block budget is not passed; `kardamom_halt{cause="da_lag"}` stays 0; the log of each member and the receipt buffer of each executor stay below their size before the freeze plus the margin; after the thaw the batcher replays from the serving service and posts, and the serving service frees the posted blocks. Both halves: L1's record is contiguous; root parity holds. |
| `sealer-fleet-total-wipe-recover` (`crates/chaos/src/cases/fleet/sealer_wipe.rs`) | None. The rebuild mark does not change. | As today. |
| `validator-join` | Rename to `validator-catchup`. It runs once the serving service exists (section 8.1). | The validator catches up through the serving service. |
| `feed-member-kill` (new) | Kill sealer member 1 under load. | Executor 1 switches to the serving service within 10 s and back after the restart. No gap. The executor roots equal the validator's. |
| `feed-slow-executor` (new) | SIGSTOP executor 2 longer than the ring holds, then SIGCONT. | The commit latency of the other members does not rise. Executor 2 catches up through a walk of its member's Raft log. |
| `leader-kill` (new) | Kill the leader under `on-quorum` load with 8 KB transactions. | The sequencers re-offer only the entries with no ordered signal (#680). Every acknowledged transaction has a receipt. No transaction is ordered twice. |
| `sequencer-byte-cap` (new, small `--parked-bytes`) | Flood lane 0 with large future-nonce transactions from new keys. | The parked bound of lane 0 holds and evicts. No flood transaction enters the Raft log. The commit latency of lane 1 does not rise. |
| `sequencer-twin-kill` (new) | Kill one twin of lane 0 under load with 8 KB transactions. | No gap and no loss. Every acknowledged transaction has a receipt. |
| `cutover-rehearsal` (new, container cluster only) | Under load, from the release before PR 13: deploy PR 13 as a rolling release with the carry flag off, check, send the upgrade transaction of the carry flag, check (section 12.2). | Before the activation, the old path runs, and the chain does not pause during the deploy. At the activation, every member switches at the same log entry, and the executors switch at the same block. After it, each paired executor reads its feed, and the chain continues with no pause; root parity holds. The serving service imports the receipts at or below `D` before its first stored head, and the ingress answers a receipt of a block before the switch after the executors prune. |
| `feed-driver-restart` (new) | Restart the aeron system job on executor node 1 under load. | Member 1's commit latency does not rise. The feeder reconnects. Executor 1 returns to its feed with no gap. |
| `sealer-disk-full` (new, container cluster only) | Fill the disk of member 2 while the serving service is stopped. | `KardamomSealerDiskLow` fires first. Below the reserve, member 2 deletes its reserve file, and the ingress refuses new submits with `chain paused: sealer disk low` while reads and receipts go on (#678). After the serving service returns and the purge frees space, the pause lifts by itself. |
| `feed-log-walk` (new) | Stop executor 2 for longer than the ring holds and less than the feed floor, then for longer than the feed floor. | First: executor 2 catches up through a walk of its member's log, with `input_source="feed"`, and the walk frames equal the live frames. Second: the feed answers `REPLAY_UNAVAILABLE`, and executor 2 catches up from the serving service. |
| `egress-member-fallback` (new) | Stop the follower member that an instance of the serving service tails, then a second member. | The instance falls back to another member, the leader last. With two answers it exposes up to the lower durable position; with one, it holds the exposure. The exposed position never moves down. |
| `serving-service-outage` (new) | Stop every instance of the serving service under load for 15 minutes, then start them. Then stop one instance of 2. | The chain goes on. `KardamomExecutorOutputUnstored` fires. The stored heads stop, and the members keep their log above them. After the start each instance reads the missed frames from a sealer member and their receipts from an executor, the heads reach the majority durable position, and the members and the executors prune. With one instance of 2 stopped, no block is lost, and the heads that K instances hold wait for the replacement. Every receipt is in at least one tier during the run. |
| `receipt-tier-fallback` (new) | Ask the ingress for a receipt below the stored head, after a restart of the ingress. | The receipt comes from the serving service, with the same bytes as the executor gave before its prune. |
| `sealer-tail-loss` (new) | After the activation of the carry flag, put the cluster directories of two members on a device that drops unsynced writes (`dm-flakey` with `drop_writes`), and stop both under load with sync level 0 and the 1 s scheduled sync. | The members restart with logs cut back to their durable positions. The cluster continues from the surviving log, with a loss of at most about 1 s. No block that the serving service exposed changes. The paired executors roll back by themselves, publish one `receipt-rollback` frame for each divergence (a higher history key after #675), and match the validator's root. No `ClusterBehindCursor` stop. A second run uses the double-vote order of #620: no executor executes a cut-back entry (#673), and the sealed head never goes below the posted head (#671). |
| `two-blank-members` (new, #677) | Wipe the disks of two members. Follow the runbook of decision 29. | The survivor and the copied member form a quorum. The third member seeds from a peer. No committed entry is lost. |
| `rollback-above-posted` (new, #672) | Cause a rollback above the posted head while a batcher group is spooled and an attestation is due. | No output root and no post names a removed block. The batcher drops the spooled blocks that the serving service does not hold. |
| `executors-down-no-reoffer` (new, #679, #680) | Stop every executor under load for 5 minutes. | No ordered entry enters the Raft log again. The frames-stored head moves, and the sealer purges. After the start, the receipts-stored head catches up. |
| Planned and dropped | `exec-peer-fetch`, `record-lag-halt`, `validator-exec-archive-catchup`, `exec-archive-prune` of the archived-data spec | Not built. |

## 14. Docs that change with the code

- `docs/failure-modes.md`: the sealer section (the record with bytes, the feed walk of the log,
  the scheduled sync and the durable position, the stored-head floor and a full disk), the
  executor receipt buffer and its alerts, the void
  section goes, the executor section (the feed), the sequencer section (the full entry, the byte
  bounds, the twin copy), the ingress section (the size cap, the ack policies, the receipt
  fallback to the serving service), the batcher resume sources, the recovery after a power loss
  on a majority (section 11.1), which replaces the refusal of a cursor ahead of the head
  (`docs/failure-modes.md:854-866`).
- `cluster/sealer-service/README.md`: kinds (kind 11 by #666, kind 12), record types, settings,
  snapshot v12 with no retention section, the feed and its walk of the log, the purge floor, the
  decision version (version 3 by an upgrade transaction, on and off, #666 and #670), the burn in
  place of the nonce release (#676), the hold after a start (#673), the history key (#675). The
  "Retained file" section goes (PR 15).
- `docs/runbooks/`: the upgrade step of section 12.2 for the carry flag. `deploy-rollback.md`
  states the rollback rule of section 12.5. `sealer-fleet-rebuild.md`: the rebuild from the
  serving service and L1 (#671), the new generation (#675), and no second upgrade transaction
  (#667). A new runbook for two blank members (#677).
- `docs/aeron-discovery.md`: the `sealer_feed` topic and streams; `exec_txs` and the `tx_data`
  archive topics go.
- `docs/observability.md`: the new metrics of sections 5.5, 6.2, 6.6, 7.5 and 10.6.
- `docs/l1-data-path.md`: the batcher reads bytes from the serving service.
- `deploy/alerts.yml`: `KardamomSealerDiskLow`, `KardamomExecutorOutputUnstored`,
  `KardamomExecutorDiskLow`.
- `docs/tx-status-events.md`: the separate PR that removes the notifier removes it (decision 12).
- `docs/json-rpc.md`: the error for a transaction above the size cap (section 6.6).

## 15. Considered and not chosen

- **The nonce release (#627, #634).** The sequencer sent ingress kind 10 for a skip receipt, and
  the sealer lowered its guard. A lost release left the sender stuck (G6). The owner replaced it
  on 2026-10-18: a skipped transaction uses its nonce (decision 22, #676).
- **An explicit rollback record as the end state (#655).** The executor publishes
  `ReceiptRollback { to_block, id }`, and keeps a receipt ledger and markers to send it only when
  needed. A consumer that is down when the record goes out misses it (G5). The history key on each
  block replaces it (decision 24, #675).
- **One writer with read replicas for the serving service (decision 11).** It needs a writer
  failover and a lock. Independent instances with a K-of-N copy rule need neither (decision 27).
- **A mixed-version chaos shard (#663).** The owner kept only the compatibility fixtures
  (decision 33).

- **A chain reset for staging and a drained cut-over for production (decision 3).** The first
  versions of this spec reset the staging chain and drained production through a coordinated
  restart: a pause of the submits, a batcher flush, a v10 snapshot, a purge of the sealer job and
  a start of the new release. The owner replaced both on 2026-10-18 (decision 20): a deploy is
  always backwards compatible, and an upgrade transaction turns the carry flag on (section 12.2).
- **A coordinated restart for decision version 3.** #634 shipped version 3 with a quiesce check
  and a purge of the cluster job. The owner's rule forbids it (decision 19). An upgrade
  transaction switches the version at one log entry (section 5.4, #666).

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
- **No floor on the Raft-log purge.** The serving service reads the frames from the sealer. A
  block that it exposed but did not store yet is only in the log. A purge with no floor takes away
  the copy that the service reads again after its own crash (section 5.5).
- **The serving service reads the frames from the executors (decision 10).** The executors would
  keep a `block_frames` buffer, and the sealer would need the serving service to restore a lost
  log tail. The user replaced it (decision 16): the service tails the sealer, so every byte that
  it exposes is durable in the sealer, and the sealer needs nothing from it.
- **Two executors agree before a block counts as stored.** The serving service would compare a
  digest of each block from a second executor. The user chose one executor (decision 14). The
  frames now come from the sealer, and the service checks each receipt against them.
- **A halt after a lost tail of the Raft log.** The executors stop with `ClusterBehindCursor`, and
  an operator acts (#615). The user chose recovery (decision 15, section 11.1).
- **A hold and a restore from the serving service after a lost tail (decision 15).** On each new
  term the sealer would refuse user records until the serving service reported its stored head,
  and the service would send lost frames back. The user replaced it (decision 16): the scheduled
  sync keeps the loss window at about 1 s, and the cluster continues from its surviving log.
- **A sequencer retention until the entry is durable elsewhere (decision 15, #642).** It would
  re-offer the entries of a lost tail. The user dropped it (decision 16): the window is about 1 s,
  the re-offers get a new order anyway, and it ties the sequencer bound to the serving service.
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

Decisions 1 to 9 are of 2026-10-11. Decisions 10 to 18 are of 2026-10-17. Decisions 19 and 20 are of
2026-10-18. Decisions 19 to 33 are of 2026-10-18. Decision 16 replaces parts of decisions 10, 11
and 15, decision 20 replaces decision 3, and decision 27 replaces parts of decisions 11, 16 and 17;
each replaced part says so.


1. **Target rate and size.** Up to 5,000 tx/s with mixed sizes. Section 10.0 states it as the
   design target. The benchmark must show whether one Raft group carries it, with the 8 KB rows,
   and the staging run measures the network. If one group does not carry it, a second Raft group
   (lane sharding) is the open follow-up. This spec does not design it.
2. **Placement and the feed.** Sealer member `i` and executor `i` run on one host and share one
   media driver; the feed is IPC (section 7.6). Decision 7 removes the UDP feed and the egress
   fallback. An executor without a local member reads the serving service.
3. **Staging migration.** A chain reset (the former section 12.2). Production uses the drained
   cut-over (the former section 12.3), and the backups and the rollback of the former section 12.5
   apply to production.
   (Replaced by decision 20: both networks take a rolling deploy and an upgrade transaction.)
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
    - The append-only log of the transactions lives in the serving service (#585, sections 8.1
      and 8.3). (Replaced by decision 16: "it reads only from the executors, not from the
      sealer". It now tails the sealer for the frames.)
    - A long window matters only to the batcher, which posts to L1, and to the attester of the
      withdrawal off-ramp, which posts output roots to L1. The serving service keeps every block
      above the posted head and the attested head for them. The rule "keep the data until L1
      holds it, do not halt" now applies to the serving service. Decision 14 extends the window
      to the end of the challenge window.
    - The member feed replays from the member's own Raft log, from a snapshot, not from a file
      (section 7.3, PR 16). An executor that is further behind falls back to the serving service
      or a peer checkpoint, as the reader of #595 (#635) does.
    - A member feeds only its own co-located executor (decision 7). (Decision 16 adds the serving
      service as a second reader of a member.)
    - The executor is a short buffer in front of the serving service. It keeps its receipts and
      their index until the serving service stores them and sends back its stored head (section
      8.3). Its state stays. (Decision 16 removes the frames from that buffer.)
    - From the review of this decision: snapshot v12 drops its retention section and carries the
      stored head; v12 is not released, so it changes in place. (Decision 16 sets the purge floor
      to the stored head alone, not `max(posted_head, stored_head)`.)
11. **Two read tiers (2026-10-17).** The hot tier is the executor: the internal services (the
    ingress, the state mirror and Redis, the sequencer) read it, with no new hop. The cold tier is
    the serving service: horizontally scaled read replicas of one append-only log, with one writer
    that tails the sealer and reads the receipts from the executors (decision 16). The batcher, the
    attester, the public history RPC, indexers, explorers and the validator read it. The batcher and
    the attester read only the cold tier. The ingress asks the serving service for a receipt that
    the executors pruned. The executor prunes only at the stored head, so each receipt is always in
    at least one tier (section 8.4). (Decision 27 replaces the one writer and the read replicas
    with independent instances.)
12. **The notifier (2026-10-17).** It is removed for now: no WebSocket status feed and no
    webhooks. A separate PR removes the code. The serving service keeps nothing for it
    (section 8.4).
13. **The latency target (2026-10-17).** A transaction lands within 50 ms of its submit. The
    ideal is a p99 of at most 20 ms from the ingress receipt of the submit to the executor's
    receipt emission. The benchmark gate measures that interval (sections 10.0 and 10.6).
14. **The answers to the open questions of decisions 10 to 13 (2026-10-17).**
    - **16.1-10, the latency target:** "ideally 20 ms p99 from ingestion to receipt emission".
      Decision 13 records it. The 50 ms landing figure stays the stated requirement. The gate
      reads it at p99.
    - **16.1-7, the executors:** one executor is enough before a block counts as stored. The hash
      check of each entry (#595) stays (section 8.3).
    - **16.1-8, the challenge window:** the serving service keeps each attested range until its
      1-day challenge window ends, also after the batcher posted it. Its floor is
      `min(posted_head, final_head)` (sections 8.1 and 10.0).
    - **16.1-9, the cut-over:** the serving service imports the existing receipts from the
      executors at the cut-over, so the history stays available after the executors prune
      (section 12.3).
    - **16.1-4, 16.1-5 and 16.1-6:** the read interface between the executor and the serving
      service, the number of durable copies of the stored head, and the replication and the
      failover of the serving service move to its design (#585).

15. **Recovery, not a halt (2026-10-17).** After a power loss on a majority of the members, the
    chain goes on. The executors that ran entries that the cluster lost roll back by themselves:
    they restore a checkpoint below the divergence and execute the new stream (#643). This
    replaces the operator path of #615. (Replaced by decision 16: the hold, the restore from the
    serving service, and the sequencer retention of #642. The R8 of decision 15, "the cluster never
    overrides at or below the stored head", is replaced by the R8 of decision 16.)
16. **The sealer syncs on a schedule, and the serving service tails the sealer (2026-10-17).**
    - **The scheduled sync.** The sealer appends at sync level 0 (#641, #645). It forces its log to
      disk every 1 s, and then knows its durable position. The hot path does not wait: a receipt
      still comes from the executor in about 5 ms (section 5.5).
    - **The serving service tails the sealers.** It reads the transaction data from a sealer
      member, a follower first, and falls back to the other members. It exposes data only up to
      a durable position (decision 17 makes it the majority durable position). It syncs its own
      disk apart from that (section 8.1).
    - **No dependency of the sealer on the serving service for correctness.** Every byte that
      the service exposes is already durable in the sealer, so the sealer never needs the service
      to recover. One one-way link stays: the Raft-log purge does not pass the stored head of the
      service (#639). It affects only liveness and disk (section 5.5).
    - **Receipts.** The serving service reads the receipts from the executors, for blocks that
      are durable in the sealer. The executors keep the receipts until the service stored them
      (section 8.3).
    - **Recovery.** After a power loss on a majority, the cluster continues from its surviving
      log: Raft elects the most complete log, and the loss window is about 1 s. The executors that
      ran past that point roll back (#643). A coordinated disk failure is very rare: the cluster
      then restarts from a sealer snapshot and overrides the later blocks, never below the posted
      head (section 11.1, #644).
    - **Confirmation.** A transaction is confirmed when the batcher posted its block to the DA
      layer. The batcher reads the serving service, so it posts only data that is durable in the
      sealer. This replaces the earlier idea of `safe` as the stored head (section 11.1).
    - **Issues.** #642 closes. #644 becomes the recovery from the surviving log. #639 keeps the
      one-way purge floor. #585 tails the sealers. #645 adds the scheduled sync and the durable
      position.
17. **The majority durable position (2026-10-17).** The serving service exposes data only up to
    the majority durable position, and computing it must not penalize the leader (section 8.1).
    - Each member reports `durable = min(locally synced log position, local commit position)` on
      its admin endpoint, on a thread apart from the service and Raft threads (#645 adds the
      report).
    - The writer of the serving service polls every member once for each scheduled sync (about
      1 s) and takes the median of the three values: the majority durable position.
    - The leader's cost is one small HTTP answer a second for each writer, on its admin thread:
      no Raft entry, no extra log traffic, and no work on the service thread.
    - With a member that does not answer, the writer takes the lower of the two values that it
      has. With only one answer, it holds the exposure.
    - Only the writer polls: the replicas follow it. (Decision 27: each instance polls, because no
      single writer exists.)
18. **The answers of 2026-10-17 to two points of decisions 16 and 17.**
    - **The import.** The receipt history import at the cut-over stays (decision 14, section
      12.3). Only the frame restore into the sealer goes (decision 16).
    - **The order of sync level 0 (16.1-14).** #645 adds the 1 s scheduled sync and the durable
      report itself, beside sync level 0. #645 does not merge before the executor rollback
      (#643). So sync level 0 never runs without the scheduled sync or the rollback (section
      12.4).
    - (Amended on 2026-10-18 from #655 and decision 21.) The rollback runs only on the carried
      path. So sync level 0 must not run on a cluster before the activation of the carry flag.
      The carry flag drives it: the member keeps its sync on each append until the flag is
      active. So #645 can merge at any time (section 12.4).

19. **The upgrade rule (the owner, 2026-10-18, #661).**
    - A deploy is always backwards compatible.
    - A breaking change ships behind a chain feature flag that is off. After the release runs
      everywhere, an L1 upgrade transaction turns the flag on: `ETHLockbox.initiateUpgrade`, then
      `UpgradeInitiated`, then a system deposit, then the flag in `KardamomChainState`, with an
      optional activation time.
    - After the activation on every network, a later release removes the old logic.
    - No coordinated restart, quiesce check, cluster purge, `KARDAMOM_ALLOW_ONE_WAY` or
      `[coordinated]` waiver is a deploy path. #664 removes `formats.toml` and the format gate.
      The compatibility fixtures of #663 and a gate check of the active flags of the chain
      replace them (decision 33: no mixed-version shard).
    - Decision version 3 activates by an upgrade transaction (#662, PR #666, section 5.4). The
      feature activation is ingress kind 11, so the stored cursor is ingress kind 12.
20. **The cut-over by an upgrade transaction (2026-10-18, on #598).** This replaces the chain reset
    of decision 3 and the drained cut-over.
    - PR 13 ships the carried path with the carry flag off, as a rolling, backwards compatible
      release.
    - An upgrade transaction turns on one chain flag. Every component switches at the same point:
      the sealer at the first log entry after the activation in its Raft log, and the executors at
      the same block.
    - `kardamom.cluster.carryBytes` and `--carry-bytes` become this chain flag. They are no longer
      operator settings.
    - The release does not wipe the chain. The old path stays until the activation on every
      network. A later release removes the void, the join and the archives (PR 14, #599).
    - The staging step: deploy, check, send the upgrade transaction, check again. The chaos case
      `cutover-rehearsal` does the same (section 12.2).
    - The receipt import of decision 14 stays, on both networks (section 12.3).
21. **The switch of the carry flag (2026-10-18; closes 16.1-18).**
    - The feature id of the carry flag is 3.
    - The sealer marks the switch in the ordered stream. The executors switch at the next block
      boundary after the marker.
    - The sequencers start the full-entry offer after kind 17 reports the carry rules.
    - Every reader keeps both the `TxRef` and the full-entry decoders until #599.
    - Version 3 must be active before the carry flag. The sealer ignores an activation of the
      carry flag unless version 3 is active.
    - The seed carries the active rules (#667).
    - The validator keeps its attester leaves across the switch.
    - The carry flag drives sync level 0 of #645: the member keeps its sync on each append until
      the flag is active (sections 5.5 and 12.4).
22. **No nonce release (2026-10-18, #676).** A skipped transaction uses its nonce at the executor,
    with `FEATURE_SEALER_V3`. Ingress kind 10, the release marks and the release queue of the
    sequencer go, with #627. Version 3 is: no void, and the burn (section 5.4).
23. **A feature activation works both ways (2026-10-18, #670 and the comment on #661).**
    - An upgrade transaction can turn a feature off: a future activation time suspends it, and
      `type(uint64).max` turns it off for good.
    - Every component switches back at one deterministic point, as it switches on (section 5.4).
    - The sequencer is trusted. A feature activation record (kind 11) needs no L1 proof in the
      sealer.
24. **The history key replaces the rollback record (2026-10-18, #675).**
    - Each block carries the key (generation, term). The term is the `leadershipTermId` of the
      sealing term. The generation is the clock time of the last fleet rebuild in ms, from an NTS
      clock, and 0 at genesis.
    - A higher key replaces the held history, a lower key is ignored, and an equal key with a
      different transaction halts.
    - One shared consumer library implements the rule (STYLE R14).
    - The last step removes the `ReceiptRollback` record, the receipt ledger and the markers of
      #655. Until then #655 is the current state (section 11.1).
25. **The hold after a start (2026-10-18, #673).** The member feed, the exposure on the serving
    service and kind 17 wait until every member reports the same term with a CLOSED election
    (section 7.1).
26. **The batcher posts only snapshotted blocks (2026-10-18, #671).**
    - Posted head ≤ snapshotted head ≤ majority durable position ≤ commit position.
    - Staging posts every 1 hour. Production posts at each new snapshotted head, about every
      5 minutes.
    - The rebuild after a coordinated disk failure with no snapshot reads the serving service,
      then L1 (sections 8.2 and 11.1).
27. **The serving service scales out (2026-10-18, the comments on #585 and #639, #679).**
    - A horizontally scalable Nomad group of independent instances (`scaling`, a minimum of 2,
      `distinct_hosts`). No single writer exists.
    - The purge floor is the head that K = 2 instances hold. Each instance sends its own kind 12
      with an instance id, and the sealer clamps each head to its sealed head (#639).
    - Each instance compares on each reconnect, as the executor does, and resumes from its
      exposed head.
    - Two heads: the frames-stored head sets the sealer purge, and the receipts-stored head sets
      the executor prune (#679).
    - The transport is the member feed over UDP, beside the IPC feed (G17). This closes 16.1-17.
    - This replaces the one writer and the read replicas of decisions 11, 16 and 17 (sections 8.1
      and 8.4).
28. **The attester and the batcher spool (2026-10-18, #672).** The attester attests only blocks at
    or below the posted head. The batcher checks its spool against the serving service at its
    start and after each reconnect (sections 8.2 and 8.3).
29. **Two blank members and one survivor (2026-10-18, #677).** A runbook and a chaos case (section
    11.1).
30. **A disk reserve and an ingress pause (2026-10-18, #678).** A reserve file on each member, and
    an ingress pause while a member of a quorum is below the reserve (section 5.5).
31. **The ordered signal clears the re-offer ledger (2026-10-18, #680).** An entry leaves the
    ledger on egress kind 15. A sequencer offers an entry again only after a session loss or a
    leader change, with no ordered signal for it (section 6.3).
32. **The validator resumes at or below the attested head (2026-10-18, #681).** After an attester
    restart, the attester sees every withdrawal again (section 8.3).
33. **No mixed-version chaos shard (2026-10-18, #663 and the comment on #661).** #663 keeps only
    the compatibility fixtures (section 12.1).

### 16.1 Still open

1. **A reader for `tx_heads`.** Under option B the sequencer must read the bytes on `tx_data`, so
   `tx_heads` does not replace it. The evaluation proposed to drop `tx_heads`; the user keeps it
   in scope. Which service reads it: for example the hedged sequencer, or a future
   lane resize? Until the answer, PR 11 builds the publisher only.
2. **The ingress observer.** The leader still sends a 42-byte kind-15 frame for each transaction
   to each ingress, for the `on-quorum` ack (section 6.4). It carries no bytes. Is this inside
   decision 7, or does the ack move to the serving service too?
3. **Superseded work.** Close #543, #542, #554 and #570 as superseded (section 9.2)?
4. **Moved to the design of #585 (decision 14).** The read interface between the executor and
   the serving service (section 8.3). The proposal of this spec: a binary receipt read on the
   executor's state query server.
5. **Answered by decision 27.** The stored heads are the heads that K = 2 instances hold.
6. **Answered by decision 27.** No single writer exists: the instances are independent, and a new
   instance copies the stored range from a peer (section 8.4).

Questions 7 to 10 are answered (decision 14).

11. **Aeron Cluster with a shorter log on a majority (section 11.1).** With sync level 0, a member
    can restart with a log below the commit position that it acknowledged, and an unforced
    snapshot can be lost too. The design assumes that the members elect the most complete
    surviving log and serve it. #644 must prove it with `sealer-tail-loss`.
12. **Answered by decision 16.** There is no hold.
13. **Answered by decision 16.** The sequencer keeps its rule of today.
14. **Answered by decision 18.** #645 adds the scheduled sync and the durable report with sync
    level 0, and it does not merge before #643.
15. **Answered by decision 16.** Confirmation is the post to the DA layer.
16. **Answered by decision 17.** The serving service uses the majority durable position.
17. **How a follower member sends its stream to the serving service (section 8.1).** Aeron Cluster
    sends egress only from the leader. The options:
    - (a) the member feed over the network: the feeder of a follower publishes the feed frames on
      a UDP channel to the serving service, beside its IPC feed. A UDP feed was ruled out for
      executors (decision 7); this is one subscriber;
    - (b) an archive replay: the serving service replays the follower's log recording from its
      archive and runs the sealer state machine itself, as the feed walk does (section 7.3). It
      needs the sealer code in the serving service;
    - (c) the walk of section 7.3 on the follower, published to the serving service.
    - The durable position does not ride on this channel: each instance polls the admin
      endpoints (decision 17).
    - **Answered by decision 27:** option (a), the member feed over UDP (G17).
18. **Answered by decision 21.** The switch of each component at the activation of the carry flag.
    PR 13 sets the layout of the switch marker, and the point at which a member drops its
    retained `TxRef` frames.

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
- **A fleet rebuild that keeps the unposted range (decision 10).** Decided by decision 26 (#671):
  the rebuild reads the serving service, then L1 (section 11.1).
- **A seed mode for two blank members (#677).** Later, if needed: a seed mode that reads a peer
  archive with no closed election.
- **The attester restart gap.** Decided by decision 32 (#681): after an attester restart, the
  validator resumes from a checkpoint at or below the attested head (section 8.3).
- **The hedged offer (decision 9).** Only if the benchmark shows that the twin copy is the limit
  (section 6.3).
- **Large transactions from unknown senders (decision 9).** A stricter rule for a large
  transaction from a sender whose balance is unknown, and a byte rate for each sender at the
  ingress (section 6.6).

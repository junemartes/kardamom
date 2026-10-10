# Transaction bytes in the sealer's Raft log

- Status: design and implementation plan. Not implemented.
- Issue: refs #582.
- Decided by the user:
  - The void voting goes. The `VoidLedger`, the voter ids, the join, the refetch, the peer fetch
    and the record-lag guard duplicate the Raft log of Aeron Cluster.
  - Variant A (section 15 lists the variant that was not chosen). The sequencers stay and offer
    only references. The ingress offers the transaction data into the cluster through its own
    cluster session.
- Still open for the user: the target rate and the transaction size (section 10.6).
- Scope: the ingress, the sequencer offer, the sealer state machine and its snapshot, the input
  of the executors, the consumers of the canonical stream, and the removal of the void path.
- Every claim about the current code cites `path:line` on `main` at `0e0c34f25`.

## 1. Summary

### 1.1 The change

- The ingress is a cluster client. It offers each validated transaction envelope into the
  sealer cluster through its own cluster session. The envelope enters the Raft log as log input.
  It never reaches the sealer nodes through a side stream.
- The sequencers stay. They offer a small **reference** for each transaction: the guard header
  (sender, nonce, deadline, tip), the transaction hash and the lane. They do not offer bytes.
- The sealer state machine holds each envelope until a reference names it. Then it emits one
  canonical entry that carries the full envelope.
- A reference whose envelope is not in the log yet waits, up to its deadline. At the deadline the
  sealer refuses it with `PAST_DEADLINE`. The sequencer frees the nonce, as #561 does today.
- An envelope that no reference names expires at its deadline.
- Each sealer member publishes the canonical stream, with bytes, to its paired executor: the
  **member feed**. The feed is an Aeron publication on the member node. It is IPC when the
  executor runs on the same node.
- The executors execute strictly in canonical order. There is no join, no refetch, no void and
  no vote.
- The validator, the batcher and any executor without a paired member read the same canonical
  stream through the cluster egress, as today. The frames now carry the bytes.
- The executors, the validator and the batcher no longer read `tx_data`. Only the sequencers read
  it. The ingress `tx_data` recordings go.

### 1.2 Invariants

| Id | Invariant |
|---|---|
| R1 | Transaction bytes enter the cluster only as Raft log input, through a cluster session. The canonical stream is a function of the committed log only. |
| R2 | A canonical transaction entry always carries its envelope. A committed entry without bytes cannot exist. |
| R3 | Every member emits the same canonical stream, byte for byte. A consumer can switch from one member's feed to the leader's egress at any index. |
| R4 | The service thread of a member never waits for a consumer. A slow executor never slows Raft. |
| R5 | An executor executes index `i` only after it executed `i - 1`. It never skips an index. |
| R6 | The sealer holds an unreferenced envelope at most until its deadline, and the held bytes never pass a fixed bound. |
| R7 | Nothing is pruned below the posted head: not the retained frames, not the Raft log (#482, #501). |

### 1.3 What the design does not guarantee

- It does not make an envelope durable before the cluster commits it. An ingress that dies after
  its offer and before the commit loses that envelope. The client gets no ack under the default
  `on-quorum` policy (section 6.4), so it retries.
- It does not check the signature in the sealer. The executors trust the `sender` field that the
  ingress stamped, as today. The validator checks the signature (section 8).
- It does not remove the leader as the bandwidth ceiling. The leader receives every envelope and
  sends it to each follower and to each remote consumer (section 10).

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
 ingress x2 ── envelope batch (kind 10, own cluster session) ──────────────┐
    │                                                                      ▼
    └── tx_data (8 lanes) ──> sequencer x4 ── reference (87 B) ──> sealer (Raft log: envelopes + references)
                                                                           │
                       every member: canonical entry = reference + envelope (RT_TX)
                                                                           │
            ┌──────────────────────────────────────────────────────────────┼─────────────────────┐
            ▼ member feed, member i -> executor i                          ▼ egress (leader only) │
        executor 0..2  (strict order, no join, no vote)          validator, batcher, elastic executors
                                                                 ingress x2: references only (kind 15)
```

| Step | New behavior |
|---|---|
| Ingress validate | Unchanged: decode, signature recovery, the checks. |
| Ingress offer | The ingress offers the envelope in a kind-10 batch through its envelope session (section 6). |
| Ingress publish | After the publication takes the offer, the ingress publishes the envelope on `tx_data[lane]` for the sequencers, as today. A later step sends a fixed header instead (section 6.5). |
| Sequencer | It reads `tx_data` as today. It offers a reference: kind 0 with record type 5 (`RT_TX_ORDER`) and the lane. |
| Sealer | It holds the envelope. A reference passes the checks of today, then pairs with its envelope (section 5). |
| Canonical entry | A relayed frame of record type 6 (`RT_TX`) with the lane and the envelope. |
| Executor | It reads the feed of its paired member. It falls back to the leader's egress (section 7). |
| Validator, batcher | They read the egress, as today. The frames carry the bytes. |
| Ingress observer | It subscribes in reference mode and gets 42-byte frames (section 6.4). |

## 4. Wire and log formats

All integers are little-endian, as today (`cluster/sealer-service/README.md:25-28`).

### 4.1 The envelope batch (ingress kind 10, new)

```text
[kind=10][count:u16]
  then for each entry:
  [len:u32][canonical_id:32][deadline:u64][lane:u8][envelope:len-41 bytes]
```

- `canonical_id` is the transaction hash, the same id the reference carries.
- `deadline` is `max_inclusion_block`, the deadline the ingress stamped.
- `lane` is `lane_for(sender)`. The sealer uses it for the per-lane cap (section 5.1).
- `envelope` is the rkyv `TxEnvelope`, the frozen layout of `aeron-stream-records`
  (`formats.toml:207-231`). The sealer does not parse it.
- The entry overhead is 4 + 32 + 8 + 1 = 45 bytes. With the 80-byte rkyv part, an entry is
  `125 + raw_tx` bytes.
- One message holds at most 64 KiB of entries. One entry may be larger than that: the largest
  accepted transaction frame is 1 MiB (`crates/ingress/src/binary.rs:38`). Such an entry goes
  alone in its message.

### 4.2 The reference (ingress kind 0, record type 5, new)

```text
[kind=0][sender:20][nonce:u64][deadline:u64][tip:u128][canonical_id:32][record_type=5][lane:u8]
```

- 87 bytes. In a kind-3 batch each entry adds a 4-byte length: 91 bytes.
- The guard header is today's (`SealerWire.java:32-59`). The sealer reads it as today.
- The 13-byte archive pointer of `RT_TXREF` goes. Nothing reads an archive.
- At 91 bytes an entry, 13 references fit one 1,344-byte MTU with margin, as today. With section 4.5 the batch limit
  becomes a byte limit, not a count.

### 4.3 The canonical entry (relayed record type 6, new)

```text
egress kind 1: [kind=1][index:u64][payload_len:u32][payload]
payload:       [canonical_id:32][record_type=6][lane:u8][envelope]
```

- 13 + 34 + 80 + `raw_tx` = `127 + raw_tx` bytes. Today a `TxRef` frame is 59 bytes
  (`SealerWire.java:244`, `SealerEgress.java:652-666`).
- The sealer builds the payload from the reference (`canonical_id`, lane) and the held envelope.
  The guard header stays out, as today.
- The reader copies the payload to an aligned buffer before it reads the rkyv part, as it does
  for the epoch records today.
- Record types 1, 2 and 3 (deposit reference, epoch, remote epoch) do not change.

### 4.4 Other wire changes

| Item | Change |
|---|---|
| Ingress kind 2 (subscribe) | Optional tail `[mode:u8]`. `0` or no tail: full frames, as today. `1`: reference mode (egress kind 15 in place of kind 1 for record type 6). |
| Egress kind 14 `ENVELOPE_FULL` (new) | `[kind=14][canonical_id:32][lane:u8][held_bytes:u64][cap:u64]`, 50 bytes, to the offering session. The cap of section 5.1 refused the envelope. |
| Egress kind 15 `RELAYED_REF` (new) | `[kind=15][index:u64][canonical_id:32][record_type:u8]`, 42 bytes. A reference-mode session gets it for each record type 6 entry. Other record types stay kind 1. |
| Ingress kind 6 (void request), kind 9 (recorded cursor) | Retired. A member drops them as unknown kinds (`SealerClusteredService.java:547-551`). |
| Egress kind 13 (record-lag reject), status tail | Retired. The status frame goes back to 50 bytes. A reader reads either size (`crates/cluster-adapter/src/wire/egress.rs:204-229`). |
| Record types 0 (`TxRef`) and 4 (void) | Retired after the cut-over (section 12). |

### 4.5 Fragmented session messages

- Today a cluster session message must fit one MTU, because "the hand-rolled cluster ingress path
  does not survive fragmented session messages" (`crates/sequencer/src/sequencer.rs:458-468`).
  The egress encoder states the same limit (`crates/cluster-adapter/src/wire/egress.rs:414-415`).
- An 8 KB envelope does not fit one MTU (1,344 bytes, `deploy/cluster/nomad/cluster.nomad.hcl:379`).
  The first pull request (section 13, PR 1) finds and fixes the cause on both sides:
  - The ingress side: a session message of up to 1 MiB through `LiveIngress::offer`
    (`crates/cluster-adapter/src/live/mod.rs:96-114`).
  - The egress side: the cluster egress subscription behind the fragment assembler that the
    stream subscriptions already use (`crates/log/src/aeron_live/thread.rs:30-94`).
  - A test sends 64 KiB and 1 MiB messages both ways through a `TestCluster`.
- The Aeron limit is a term length divided by 8. The cluster log term is 8 MiB
  (`cluster/sealer-service/README.md:394`), so one message can be 1 MiB.

### 4.6 Bytes for each transaction

| Item | Today | Transfer (raw 110 B) | 2 KB call | 8 KB call |
|---|---|---|---|---|
| Envelope entry in the log | 0 | 235 B | 2,173 B | 8,317 B |
| References in the log (2 racing replicas) | 2 x 103 = 206 B | 2 x 91 = 182 B | 182 B | 182 B |
| Log input per transaction, framing excluded | 206 B | 417 B | 2,355 B | 8,499 B |
| Log input with framing (about 3 %) | about 0.21 KB | about 0.43 KB | about 2.4 KB | about 8.75 KB |
| Canonical frame (egress, feed, retained) | 59 B | 237 B | 2,175 B | 8,319 B |
| Reference-mode frame (ingress observer) | 59 B | 42 B | 42 B | 42 B |

- The framing is the 32-byte session message header and the 32-byte Aeron frame header for each
  1,312-byte fragment.
- The envelope enters the log once: only one ingress gets a transaction. The racing replicas
  double only the 91-byte reference. Section 15 shows why this matters.

## 5. The sealer state machine

### 5.1 The envelope store

The envelope store is new replicated state in `CanonicalSealerState`.

- Key: the 32-byte `canonical_id`. Value: the envelope bytes, the deadline, the lane.
- Insert, for each entry of a kind-10 batch, in this order:
  1. The id is in the dedup window (already ordered): drop the entry. It is a late copy.
  2. The id is in the store: drop the entry. It is a re-offer or a second ingress.
  3. The deadline is below the open block: drop the entry. It expired.
  4. The held bytes of the lane plus the entry pass the lane cap, or the store holds
     `dedupCapacity` entries: refuse with egress kind 14.
  5. Store the entry with the deadline clamped to the open block plus
     `inclusionHorizonBlocks`, as the dedup window clamps (`CanonicalSealerState.java:674`).
  6. A reference waits for this id (section 5.2): release it now.
- Expiry: the boundary tick drops every entry whose deadline is below the new block number, in
  the same pass as the dedup prune (`CanonicalSealerState.java:663-671`).
- **TTL.** An entry lives until the block after its deadline: at most
  `inclusionHorizonBlocks + 1` blocks. With the deploy values (64 blocks, 2,000 ms) that is
  130 s.
- **Memory bound.** `kardamom.cluster.envelopeBytes`, a must-match setting. Proposed default:
  64 MiB, split equally over the lanes. The lane cap is `envelopeBytes / lane count`. A lane with
  no sequencer then cannot fill the store for the other lanes.
- A refused entry changes nothing in the state. The ingress offers it again (section 6.3).
- **Residency.** An envelope waits for its reference for one sequencer round trip, normally under
  a millisecond. It stays until its TTL only when no reference comes: the sequencer refused it
  (fee gate, a past nonce), or every replica of its lane is down. So the store holds about
  `rate x 130 s x size` only while a lane is down. At 1,000 transfers a second on one lane that is
  31 MB. The cap then refuses, and the ingress pushes back to the clients (section 6.3). That is
  correct: the lane cannot order anything.
- A client cannot fill the store on its own. Each envelope needs a valid signature at the
  ingress, and each sender's transaction costs the same TTL.

### 5.2 The reference path

The reference keeps today's checks. The record path (`CanonicalSealerState.java:826-880`)
becomes:

1. **Dedup.** The id is in the dedup window, or a reference with this id waits: a duplicate.
   Drop it. A duplicate of a waiting reference adds its session to the waiting entry (at most 4
   sessions), so both racing twins get the final answer.
2. **Deadline.** The open block is above the deadline: `PAST_DEADLINE`.
3. **DA-lag guard.** As today. It also refuses when the retained bytes pass the byte budget
   (section 5.5): `DA_LAG_REJECT`.
4. **Capacity.** The dedup window is full: `WINDOW_FULL`.
5. **Contiguity.**
   - The sender has a waiting head: the nonce must be the last queued nonce plus 1. Else
     `CONTIGUITY_REJECT`. A good nonce joins the sender's queue. No answer yet.
   - Else the nonce must be the expected nonce, as today (`CanonicalSealerState.java:867-874`).
     Else `CONTIGUITY_REJECT`.
6. **Pair.** The store holds the envelope: take it, insert the id in the dedup window, assign the
   next canonical index, advance the expected nonce, and relay the `RT_TX` entry.
7. **Wait.** The store does not hold it: the reference becomes the waiting head of its sender.
   The expected nonce does not move.

- The ordering window (`cluster/sealer-service/README.md:258-275`) stays in front of this path.
  The window flushes into step 1.
- **Release.** When the envelope of a waiting head arrives (section 5.1, step 6), the head runs
  step 6. Then the sender's queue drains in nonce order through step 6, until an entry has no
  envelope. That entry becomes the new head.
- **Refusal at the deadline.** On each boundary tick, a waiting head whose deadline is below the
  new block gets `PAST_DEADLINE` (egress kind 7) on every session that offered it. Each queued
  reference of that sender gets `CONTIGUITY_REJECT` with the head's nonce as the expected nonce.
  The expected nonce stays at the head's nonce.
  - The sequencer already handles both answers. It frees the refused nonce and parks the later
    references above it (#561, `docs/failure-modes.md:520-529`).
- **Bound.** `kardamom.cluster.waitingRefs`, a must-match setting. Proposed default: 16,384
  waiting references, heads and queues together, about 120 bytes each in the heap. A full set
  answers `WINDOW_FULL`, which the sequencer already republishes.
- Other senders do not wait. A waiting head blocks only its own sender.
- Every input of this path is in the log. Every member decides alike (R1, R3).

**Why a reference rarely waits.** The ingress publishes on `tx_data` only after its envelope
publication took the offer (section 6.1). The envelope then reaches the leader before the
sequencer reads the transaction. A reference waits in two cases only:

- the leader lost the envelope offer in a leader change (cluster ingress is at most once,
  `docs/failure-modes.md:257-258`). The ingress re-offers it (section 6.2).
- the ingress is back-pressured and the sequencer is fast. The ingress keeps the order: it does
  not publish on `tx_data` before the offer.

### 5.3 The void and the guards go

- The `VoidLedger` (`VoidLedger.java`), `onVoidRequest` and `appendVoid`
  (`CanonicalSealerState.java:919-933`) go. A canonical transaction entry always has its bytes
  (R2), so no entry needs a void.
- The recorded cursors (`RecordedCursors.java`), `onRecordedCursor`
  (`CanonicalSealerState.java:1312-1322`) and the record-lag guard go.
- The settings `voidVoters`, `voidWindow` and `recordLagBudget` go
  (`cluster/sealer-service/README.md:356-359`, `ClusterNode.java:44`, `:143-144`).

### 5.4 Decision version 3

- The change decides an ordered entry differently: pairing, waiting, no void. So
  `DECISION_VERSION` goes to 3 (`CanonicalSealerState.java:111`), with the coordinated restart
  of `cluster/sealer-service/README.md:396-422`.
- The decision version stays as a mechanism (#568). Version 2's rule (the lower nonce after a
  void) has no input after version 3, because no void exists.

### 5.5 Retention, the snapshot, the purge and the floor

**Retained frames.** The egress keeps at least `retention` frames (65,536), and every frame above
the posted head (`SealerEgress.java:420-449`). The frames now carry bytes.

| Retained set | Transfer | 2 KB | 8 KB |
|---|---|---|---|
| 65,536 frames | 15.5 MB | 143 MB | 545 MB |
| One block at 1,000 tx/s (2,000 transactions) | 474 KB | 4.3 MB | 16.6 MB |

- The posted-head floor (#482) keeps every frame above the posted head. The DA-lag budget bounds
  that range in blocks (`DEFAULT_DA_LAG_BUDGET_BLOCKS = 10_000`,
  `CanonicalSealerState.java:128`; 60,000 on staging). In blocks, the bound no longer bounds
  the bytes:
  - at 10 tx/s and 60,000 blocks: 20 transfers a block, 4.7 KB a block, 284 MB at the budget;
  - at 1,000 tx/s and 60,000 blocks: 474 KB a block, 28 GB at the budget.
- So the DA-lag guard gets a byte budget beside the block budget:
  `kardamom.cluster.daLagBudgetBytes`, a must-match setting. The guard refuses a user record when
  the retained bytes above the posted head pass it. The retained bytes are a function of the
  log, so every member decides alike. Proposed default: 1 GiB. The benchmark sets the value
  (section 10.6).
- The batcher posts a group at the latest when its last block is half the block budget past the
  posted head (#569). It gets the same rule for the byte budget: the status frame (egress kind 9)
  carries the retained bytes, and the batcher posts at half the byte budget.

**The snapshot (version 12).** `CanonicalSealerState.java:1474-1538` and
`SealerEgress.java:558-610` change as follows:

| Section | v10 (written today) | v12 |
|---|---|---|
| Dedup window, sender map, origins, remote peers | unchanged | unchanged |
| Void ledger (v6) | present | gone |
| Ordering window, posted head, seed (v8 to v10) | unchanged | unchanged |
| Recorded cursors (v11, never written) | absent | absent |
| Envelope store | absent | `[count:u32]` then `[id:32][deadline:u64][lane:u8][len:u32][bytes]` |
| Waiting references | absent | `[count:u32]` then the 87-byte reference, the session ids, the queue order |
| Retained frames | `TxRef` frames, 59 B | `RT_TX` frames, `127 + raw_tx` bytes |

- The snapshot size is at most `envelopeBytes` + 2 MB of waiting references + the retained
  frames, which the byte budget bounds: about 1.1 GiB with the proposed defaults.
- The heap holds the same data. The sealer runs with `-Xmx384m` and 1,024 MB of job memory
  (`deploy/cluster/nomad/cluster.nomad.hcl:362`, `:413`). The proposed defaults need about
  2 GiB of heap. The benchmark measures the snapshot time and the GC pauses at that size.

**The Raft log and its purge (#501).** The log now holds the envelopes. The purge rule does not
change (`PurgePlanner.java:65-73`). The log keeps at least 3 snapshot intervals (15 minutes at
300 s), and everything above the posted head.

| Log on each member | Transfer | 2 KB | 8 KB |
|---|---|---|---|
| Per transaction | 0.43 KB | 2.4 KB | 8.75 KB |
| Per block at 1,000 tx/s | 0.86 MB | 4.8 MB | 17.5 MB |
| 15 minutes at 1,000 tx/s | 387 MB | 2.2 GB | 7.9 GB |
| At the posted-head floor | the byte budget bounds the user records, so the log above the posted head is at most about `budget x (log bytes / frame bytes)`: 1.8 GiB for transfers, 1.1 GiB for 2 KB, 1.05 GiB for 8 KB, plus the 15 minutes above. |

- The snapshot copies the retained frames, and the log holds the inputs again. So the disk holds
  the bytes about twice, plus the kept snapshots (3 by default). The benchmark measures it.

## 6. The ingress

### 6.1 The envelope session

- Each ingress opens one more cluster session: a publisher-only session, like the sequencer's
  (`crates/sequencer/src/outbound/cluster.rs:160-172`). It never subscribes. So an egress close of
  the observer session does not stop the offers.
- One task owns the session. The submit path sends each validated envelope to it over a bounded
  channel. The task:
  - takes every envelope that waits, up to 64 KiB, into one kind-10 batch;
  - offers the batch;
  - on success, hands each envelope to the `tx_data` publisher.
- There is no linger timer. Under load the channel fills while an offer runs, so the batches grow
  by themselves. At low load each batch has one entry and the latency stays at one offer.
- Today one offer is a round trip to the session thread (`crates/cluster-adapter/src/live/mod.rs:96-114`).
  The batch makes that cost one for each batch, as the sequencer's batch does
  (`crates/sequencer/src/sequencer.rs:458-468`).

### 6.2 The ledger and the re-offer

- The task keeps each offered envelope in a ledger until one of these:
  - the observer sees its id in the canonical stream (section 6.4);
  - the sealer refuses its reference past the deadline (the sequencer reports the
    `PastDeadline` error on `tx_errors`, as today);
  - its deadline passes.
- An envelope that is not sealed 2 s after its offer is offered again, every 2 s. The store drops
  a copy that it holds, and the dedup window drops a copy that is ordered (section 5.1). So the
  re-offer is safe, and it fills an envelope that a leader change lost.
- The ledger is bounded by bytes: `--envelope-ledger-bytes`, proposed 64 MiB.

### 6.3 Back-pressure on the ingress side

| Signal | What the ingress does |
|---|---|
| The offer returns `BackPressured` or `NotConnected` | The task keeps the batch and retries with a backoff from 10 µs to 1 ms. Nothing is lost. |
| Egress kind 14 (`ENVELOPE_FULL`) | The task keeps the envelope in the ledger and offers it again after the next boundary. The tick frees space by deadline. |
| The channel or the ledger is over 75 % | New submits get the existing `Overloaded` error (JSON-RPC `-32005`, `crates/ingress/src/error.rs:165-171`). The readiness check fails, so haproxy sends new load to the other ingress. |
| Under 50 % again | The ingress takes submits and turns ready. |

- The ingress never drops an envelope that it acknowledged under `on-offer`. A process crash can
  lose the ledger; that is the `on-offer` window that `docs/failure-modes.md:460-463` states.
- Metrics: `kardamom_ingress_envelope_offer_total{outcome}`, `kardamom_ingress_envelope_ledger_bytes`,
  `kardamom_ingress_envelope_reoffer_total`, `kardamom_ingress_envelope_batch_entries` (histogram).

**The ingress bandwidth into the cluster.** Each transaction enters through one ingress. The sum
over all ingresses is `rate x (125 + raw_tx)` bytes a second, plus the re-offers. The leader
receives all of it.

| Rate | Transfer | 2 KB | 8 KB |
|---|---|---|---|
| 1,000 tx/s | 0.24 MB/s | 2.2 MB/s | 8.3 MB/s |
| 4,000 tx/s | 0.94 MB/s | 8.7 MB/s | 33 MB/s |
| 10,000 tx/s | 2.4 MB/s | 22 MB/s | 83 MB/s |

With 2 ingresses each sends half. The sequencers add `rate x 182` bytes (section 4.6).

### 6.4 The ack policy

| Policy | Today (`crates/types/src/ack_policy.rs:22-31`) | After |
|---|---|---|
| `on-offer` | Ack after the offer to the pipeline. | Ack after the envelope publication took the offer. |
| `on-quorum` (default) | Ack after the sealer ordered the hash. | Unchanged meaning. The observer subscribes in reference mode (kind 2, mode 1) and sees 42-byte frames, not the bytes. |
| `on-local-fsync`, `on-local-fsync-and-quorum` | Wait for the ingress recorder's fsync. | Retired with the recorders. |

### 6.5 What the sequencer reads

- The sequencer needs these fields, not the bytes: the sender, the hash, the nonce, the gas
  limit, the fee fields, the value, the deadline and the correlation id. It decodes them from
  `raw_tx` and then drops the envelope (`crates/sequencer/src/sequencer.rs:860-896`,
  `crates/sequencer/src/tx_decode.rs:42-62`).
- **First step: no change.** The ingress keeps publishing the full envelope on `tx_data`. The
  sequencer code that reads it does not change. Only the sequencers subscribe to `tx_data` now.
- **Later step (PR 12): a fixed header.** The ingress publishes a `TxHeader` of about 160 bytes on
  a new stream `tx_heads` (base 3000 + lane; the `tx_data` range 2000 to 2255 must stay free,
  `crates/log/src/config/mod.rs:503-545`). The sequencer then needs no decode. `TxEnvelope` is a
  frozen layout, so the header is a new record on a new stream (`docs/formats.md:149`). For a
  transfer it saves little; for an 8 KB call it removes 98 % of the sequencer input.
- The ingress `tx_data` recorders, the recorder barrier and `--archive-durability` go
  (`crates/ingress/src/bin/kardamom-ingress/main.rs:111-117`, `:341-361`, `:404-413`). No reader
  of the archives is left.

## 7. The executor input: the member feed

### 7.1 The publication

| Item | Value |
|---|---|
| Topic | `sealer_feed`, a new `Topic` variant (`crates/log/src/discovery/record.rs:24-36`). |
| Stream id | 1006 for the feed, 1007 for the feed control. Both are free (`crates/log/src/config/mod.rs:503-545`). |
| Channel | `aeron:ipc` when the executor runs on the member node. Else UDP unicast from member `i` to executor `i`. |
| Publisher | One exclusive publication on each member, owned by a feeder thread in the member JVM. |
| Subscriber | The paired executor only. |
| Frames | The egress frames, unchanged: kind 1 (relayed), kind 2 (boundary), kind 9 (status), and the replay answers 3, 4, 11. One codec for the feed and the egress. |

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
- **Ring full.** The service stops writing live frames and marks the feed `gapped`. The executor
  sees an index gap at the next frame it gets, and asks for a replay (section 7.3).
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
- The service walks the retained frames into the ring, at most 4 MiB in each duty cycle. It does
  not write live frames during the walk. The retained deque already holds them, so the walk
  reaches the head and the feed turns `live`. A full ring pauses the walk to the next duty cycle.
  It does not gap the feed.
- The retained frames are the catch-up source. The Raft log is not: the log holds the inputs
  (envelopes and references), not the canonical entries. A canonical entry exists only after the
  state machine pairs it. The retained window holds every frame above the posted head and at
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
executor asks again. The executor follows the leader's egress meanwhile (section 7.5).

### 7.5 Switching to a remote member

The executor reads one source at a time, in this order:

| Order | Source | Use it while |
|---|---|---|
| 1 | The feed of its paired member | The feed delivers a frame or a status at least every 3 boundary ticks (6 s). |
| 2 | The cluster egress, from the leader | The feed is silent, or it answered `REPLAY_UNAVAILABLE` and the leader's floor is lower. The client picks members round-robin and follows the leader (`crates/cluster-adapter/src/live/endpoints.rs:50-65`). |
| 3 | A peer checkpoint | Every source answered `REPLAY_UNAVAILABLE` (`crates/engine/src/bin_support.rs:529-576`). |

- The switch happens at a frame boundary. The executor closes the old source and asks the new one
  for a replay from its cursor. R3 makes the frames at each index equal.
- The executor goes back to its feed when the member admin endpoint answers ready
  (`cluster/sealer-service/README.md:423-445`) and its feed answers a replay from the cursor.
- `kardamom_executor_input_source{source="feed|egress"}` shows the source. An alert fires when an
  executor reads the egress for more than 10 minutes.

### 7.6 Pairing and placement

- Member `i` feeds executor `i`. Both counts are 3 today
  (`deploy/cluster/ansible/group_vars/all.yml:235-236`).
- Today the sealers and the executors run on separate node classes. So the first release pairs
  them over UDP unicast. This already takes the executors off the leader's egress.
- Co-location (one node runs sealer member `i` and executor `i`, feed over IPC) is the target of
  the design. It changes the node classes and puts the leader's CPU beside an executor. The
  cluster shape warns that co-location moves the latency knee
  (`docs/specs/2026-08-16-10-ggas-cluster-shape.md`, "What breaks first"). Section 16 asks the
  user.

### 7.7 Executors without a paired member

- An elastic executor, or an executor whose index has no member, reads the cluster egress. It
  costs the leader one more full stream (section 10).

## 8. The consumers

| Consumer | Reads today | Reads after |
|---|---|---|
| Executor `0..2` | Cluster egress (references) + 8 `tx_data` lanes + ingress archives; votes (`crates/executor/src/bin/kardamom-executor/main.rs:494-513`, `:339`) | Its member's feed; the egress as the fallback. No `tx_data`, no archive, no vote. |
| Elastic executor | As above | The cluster egress with bytes. |
| Validator | Cluster egress + `tx_data` + ingress archives; votes (`crates/validator/src/bin/kardamom-validator/wiring/startup.rs:241-286`, `wiring/run.rs:170-178`) | The cluster egress with bytes. It checks `keccak256(raw_tx) == canonical_id` and that the signature recovers the sender (`crates/exec-core/src/stateless.rs:253-272`). A failed check halts it with `validator_divergence`. It does not trust the sealer's bytes. |
| Batcher | Cluster egress + `tx_data` + ingress archives; votes; the rebuild from `kardamom_getBlockRefs` and the archives (`crates/batcher/src/live/run.rs:292-357`, `:641-669`) | The cluster egress with bytes. It packs the KAR1 blocks from the `raw_tx` of each `RT_TX` entry. The spool, its group and cursor rules do not change. |
| Ingress observer | Full relayed frames (`crates/ingress/src/cluster.rs:173-174`) | Reference mode (section 6.4). |
| Ingress receipt fallback | The receipt cache, then the executor state DB over HTTP (`crates/ingress/src/proxy/mod.rs:439-463`) | Unchanged. |
| State mirror | `tx_receipts` only (`crates/state-mirror/src/main.rs:137-146`) | Unchanged. |
| Notifier | `tx_status`, `tx_receipts`, `tx_errors` (`crates/notifier/src/taps.rs:56-74`) | Unchanged. |
| Sequencer | `tx_data`; publisher-only session (`crates/sequencer/src/outbound/cluster.rs:121-172`) | Unchanged at first; `tx_heads` later (section 6.5). |
| da-watcher, L1 follower | `l1_blocks`, `tx_deposits` | Unchanged. |

**The batcher's resume sources.**

1. The spool: unchanged.
2. The sealer replay from the cursor: unchanged. Every member keeps every frame above the posted
   head, now with its bytes: the retention floor (`SealerEgress.java:420-449`), the snapshot
   (section 5.5) and the log purge floor (`PurgePlanner.java:65-73`).
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
| #521 (P1) | `exec_txs` topic, `ExecTxRecord`, stream 1005 | Retired. It was considered for the network fallback. It is not needed: the egress session already carries the same canonical frames, with replay. One codec for the feed and the egress is less code (STYLE R14). |
| #531 (P2) | The executor publishes and records `exec_txs` | Retired. The pattern (an exclusive publication that one thread owns, the end of the recording is fatal) informs the feeder thread. |
| #519 (P3) | Recorded cursor, record-lag guard (off), the void on configured voters, the v11 reader | Retired. The v11 reader goes when `reads_min` rises to 12 (section 12). The rule "read N+1 one release before writing it" stays the method. |
| #540 (P4) | The executor sends its cursor | Retired. |
| #534 (P8) | The reject path, the halt, the runbook, the alert | Retired. The enum variants stay reserved (section 9.1). |
| #568 | Decision version | Kept as a mechanism. This change raises it to 3. |
| #561 | The sequencer frees a refused nonce | Kept. The refusal of a waiting reference uses it. |
| #552 | Readers hardened for a rollback; unknown kinds drop | Kept. It makes the retired kinds safe. |
| #482, #501 | Posted-head floor, log purge | Kept. The byte budget joins the floor (section 5.5). |
| #481 | `kardamom_getBlockRefs` and the archive rebuild | The archive rebuild retires. The method stays for the fleet-rebuild runbook. |
| #507 | Archive sync level | Kept. The sealer archive and the `tx_deposits` and `l1_blocks` recordings use it. |
| #522 | Refetch from an older recording | Kept for `tx_deposits`. The `tx_data` use goes. |
| Chaos harness fixes | ANSI-stripped `Evidence::count_lines` (`crates/chaos/src/evidence.rs:51-59`), one cleanup owner for the iptables rules | Kept. They do not depend on this design. |

## 10. Throughput and resources

### 10.1 The leader is the ceiling

Every byte of every transaction passes the leader three ways:

- in: from the ingresses and the sequencers (the log input of section 4.6);
- replication: to each follower (2 followers);
- egress: to each remote consumer with full frames (the validator and the batcher, plus each
  elastic executor), and 42 bytes a transaction to each reference-mode ingress.

The paired executors read their member's feed, so they cost the leader nothing. Today the 3
executors read the leader's egress.

### 10.2 Network at the leader

| Rate | Size | In | Replication out | Egress out | Total out |
|---|---|---|---|---|---|
| 1,000 tx/s | today (refs) | 0.21 MB/s | 0.42 MB/s | 0.41 MB/s (7 consumers x 59 B) | 0.83 MB/s |
| 1,000 tx/s | transfer | 0.43 MB/s | 0.86 MB/s | 0.56 MB/s | 1.4 MB/s |
| 1,000 tx/s | 2 KB | 2.4 MB/s | 4.8 MB/s | 4.4 MB/s | 9.2 MB/s |
| 1,000 tx/s | 8 KB | 8.75 MB/s | 17.5 MB/s | 16.7 MB/s | 34 MB/s |
| 4,000 tx/s | transfer | 1.7 MB/s | 3.4 MB/s | 2.2 MB/s | 5.6 MB/s |
| 4,000 tx/s | 2 KB | 9.6 MB/s | 19 MB/s | 18 MB/s | 37 MB/s |
| 4,000 tx/s | 8 KB | 35 MB/s | 70 MB/s | 67 MB/s | 137 MB/s (1.1 Gbit/s) |
| 10,000 tx/s | transfer | 4.3 MB/s | 8.6 MB/s | 5.6 MB/s | 14 MB/s |
| 10,000 tx/s | 2 KB | 24 MB/s | 48 MB/s | 44 MB/s | 92 MB/s (0.74 Gbit/s) |
| 10,000 tx/s | 8 KB | 88 MB/s | 175 MB/s | 167 MB/s | 342 MB/s (2.7 Gbit/s) |
| 333,000 tx/s | transfer | 143 MB/s | 286 MB/s | 186 MB/s | 472 MB/s (3.8 Gbit/s) |

- 4,000 tx/s is about today's measured sustained rate of 3,800 tx/s
  (`docs/specs/2026-08-16-pipeline-cost-model.md`, "System context").
- 333,000 tx/s is the blended mix of 10 Ggas/s (`docs/specs/2026-08-16-10-ggas-cluster-shape.md`).
  At that rate the leader writes 143 MB/s of log with an fsync for each batch, and sends
  3.8 Gbit/s. One Raft group then needs a 10 GbE network and a fast NVMe. The cluster shape
  already names the single ordering point as the first limit. With bytes in the log, the way past
  it is the sharding of the sealer, not more references.
- The DA wall of the cluster shape (15 to 37 MB/s compressed to raw at 10 Ggas/s) is lower than
  the Raft wall. So the Raft bytes do not set the first limit of the chain. They set the
  hardware of the sealer nodes.

### 10.3 CPU at the leader

- The service thread copies each envelope once into the store, once into a canonical frame, and
  once into the feed ring. That is about 3 x the frame bytes of `memcpy` for each transaction:
  under 1 µs for a transfer, about 3 µs for an 8 KB call.
- The consensus module and the archive of the leader handle `rate x log bytes`. This cost is in
  Aeron, not in our code. The benchmark measures it.

### 10.4 Disk on each member

See section 5.5: the log grows by `rate x log bytes`; the purge keeps 15 minutes and the range
above the posted head; the snapshot holds the store, the waiting references and the retained
frames, at most about 1.1 GiB with the proposed defaults.

### 10.5 The feed

- The feed costs no network when it is IPC. Over UDP it costs `rate x frame bytes` on the member
  node, the same as one egress consumer, but on a follower's network for 2 of the 3 executors.

### 10.6 The benchmark

The user has not given a target rate or size. The benchmark gives the curve that the decision
needs.

- **Where.** The local container cluster (`just container-up`, then `SHARD=load just shard`
  in `deploy/cluster/justfile:99-125`). All nodes share one host, so the numbers compare the two
  builds. They are not absolute. A second run on staging gives the absolute numbers.
- **Builds.** `main` (references) and the flagged build of PR 9 (bytes). Same images, same
  settings.
- **Load.** `kardamom-load` (`crates/bench/src/bin/load.rs`) with `--fixed-rate` and
  `--target-tps`. PR 2 adds a workload `calldata` with `--calldata-bytes N`: a transfer with N
  bytes of calldata to an EOA, so the size is the only variable.
- **Matrix.** Rates 500, 1,000, 2,000, 4,000 and 6,000 tx/s (6,000 is today's admission ceiling)
  x raw sizes 110, 2,048 and 8,192 bytes. Each cell runs 5 minutes after a 1-minute warm-up.
- **Measurements for each cell.**

| Measurement | Source |
|---|---|
| Commit latency p50, p99: ingress offer to the sealed id | the ingress observer, a new histogram `kardamom_ingress_seal_latency_seconds` |
| Receipt latency p50, p99 | `kardamom-load` |
| Sustained rate with zero loss | `kardamom-load --assert-all-delivered` |
| Leader CPU: process, service thread, consensus thread | container CPU and per-thread `/proc` sampling |
| Network bytes in and out of each member | node exporter or `docker stats` |
| Log bytes written a second, fsync time | the archive directory size, `iostat` |
| Snapshot size and time | `sealer snapshot TAKEN`, with a new `ms=` field |
| GC pause p99 of the leader | `-Xlog:gc` |
| Feed lag: member head minus executor index | `kardamom_executor_input_lag` (new) |
| Refusals | `ENVELOPE_FULL`, `WINDOW_FULL`, waiting-reference `PAST_DEADLINE` counts |

- **The decision.** For each size, the highest rate at which p99 commit latency stays under the
  user's bound and the leader CPU stays under 70 %. The user gives the target rate and size.
  The result says if one Raft group carries it, and sets `envelopeBytes`, `daLagBudgetBytes`,
  `feedRingBytes` and the sealer heap.

## 11. Failure modes

| Failure | Effect | Recovery |
|---|---|---|
| A follower dies | Quorum holds. Its executor's feed is silent. | The executor reads the leader's egress after 6 s. The member restarts, replays its log, and the executor returns to the feed. |
| The leader dies | An election. Envelope and reference offers in flight can be lost (at most once). | The ingress re-offers envelopes not sealed after 2 s. The sequencer re-offers references from its unconfirmed ledger. A reference that arrives before its re-offered envelope waits. The executors on the feeds of the surviving members see no gap. |
| An envelope is lost and not re-offered (the ingress also died) | Its reference waits. | At the deadline: `PAST_DEADLINE`, the nonce is free (#561). The client did not get an `on-quorum` ack, so it retries. |
| An executor is slow | Its feed ring fills. The service thread does not wait. Other members and executors do not notice. | The feed gaps. The executor asks for a replay from its cursor and catches up from the retained window. |
| An executor restarts | None for the cluster. | It replays from its state DB cursor through its feed (section 7.4). |
| An executor is behind the floor | Its cursor is below every member's retained floor. | `REPLAY_UNAVAILABLE`, then a peer checkpoint, as today. |
| A member is far behind or below the purge point | Its feed lags or is idle. | The join watchdog reseeds the member from a peer (`docs/failure-modes.md:229-240`). Its executor reads the leader's egress meanwhile. |
| A network partition isolates a member and its executor | The minority member cannot commit. The executor sees no new frame. | The executor switches to the egress of the majority's leader, if it reaches it. Else it waits. It never executes a frame that the majority did not commit, because the member applies only committed entries. |
| A sequencer lane is down | No reference for that lane. Its envelopes wait in the store. | The lane cap fills and refuses that lane's envelopes. The ingress returns `Overloaded` for that lane's senders. The other lane continues. |
| The ingress cannot reach the cluster | Offers back-pressure. | The ledger fills, the ingress turns not ready, haproxy uses the other ingress. |
| A snapshot restore | The member restores the store, the waiting references and the retained frames. | It replays the log after the snapshot. Its feed serves replays from the restored frames. |
| All members restart | No commit until a quorum returns. | Each member restores its snapshot and replays its log. The executors reconnect to the feeds and replay. The ingress and the sequencers re-offer. |
| All members lose their state | The retained frames and the log are gone. | The fleet rebuild from L1 at the posted head (`docs/runbooks/sealer-fleet-rebuild.md`). The unposted range is lost, as today. |
| The DA-lag byte budget is passed | The sealer refuses user records with `DA_LAG_REJECT`. | The batcher posts. The guard clears by itself, as the block guard does. |
| A member's feed bytes differ from the leader's | A bug: R3 is broken. | The validator checks every canonical entry it reads. A boundary does not carry a digest of the stream today; section 16 asks if it should. |

## 12. Migration

### 12.1 Formats (`docs/formats.md`, `formats.toml`)

| Format | Today | After the cut-over |
|---|---|---|
| `sealer-ingress-kinds` | writes 8, reads 0..9 | writes 10, reads 0..10. Kinds 6 and 9 drop as unknown. Kind 2 gets the mode tail. |
| `sealer-egress-kinds` | writes 12, reads 1..13 | writes 15, reads 1..15. Kind 13 is never sent. The status frame is 50 bytes. |
| `sealer-record-types` | writes 4, reads 0..4 | writes 6, reads 1..6. Types 0 and 4 are never written. |
| `sealer-snapshot` | writes 10, reads 1..11 | writes 12, reads 10..12. A later release sets `reads_min = 12`. |
| `aeron-stream-records` | `TxEnvelope` frozen | Unchanged. The envelope travels in its frozen layout. |
| `discovery-record` | `Topic::ExecTxs` | `Topic::SealerFeed` added. `ExecTxs` is removed only after no node advertises it (`crates/log/src/discovery/record.rs:279-296`). |
| `sealer-feed` (new) | none | The feed frames are the egress kinds. The feed control request is the kind-1 layout. |
| `tx-heads` (new, PR 12) | none | Frozen layout of `TxHeader`. |
| `state-db`, `batcher-spool`, `batcher-cursor` | unchanged | Unchanged. |

- Each change that the base cannot read gets a `[one_way.<id>]` and a `[coordinated.<id>]`
  waiver with its reason.
- **The P3 snapshot rule.** Today the writer stays one version behind the reader: reads 11,
  writes 10 (`CanonicalSealerState.java:164`, `:181`). The removal of the cursor and the void
  sections does not get its own two-release step. It rides the coordinated cut-over: the release
  is one-way and coordinated already (decision version 3, new kinds that change the state), so a
  mixed fleet never runs. The new release reads v10, the snapshot that the drained old release
  wrote, and writes v12 at once.

### 12.2 Cut-over, not a chain reset

The recommendation is a drained cut-over. It keeps the chain, and it reuses the coordinated
restart of the decision version (`cluster/sealer-service/README.md:405-416`).

1. Deploy every reader first, behind the flags of section 13 (off): the ingress, the sequencers,
   the executors, the validator and the batcher read the new kinds but do not write them.
2. Pause the submits on every ingress.
3. Wait until:
   - every executor and the validator executed the sealed head;
   - no void vote is open (`cluster VOID-VOTE` lines all have `result=DECIDED`);
   - the batcher posted the sealed head (`posted_head == sealed_head`). The batcher flushes its
     group when the submits pause.
4. Take a sealer snapshot (v10). Stop the writers: the ingresses and the sequencers.
5. Purge the sealer job. Start the new sealer release (decision version 3). It restores the v10
   snapshot:
   - it drops the void ledger, which holds no open vote;
   - it drops the retained `TxRef` frames, which are all at or below the posted head, so the
     floor moves to the head;
   - it starts with an empty envelope store.
6. Turn the flags on. Start the executors, the validator and the batcher; they resume at the head
   with no `TxRef` frame to resolve. Then the sequencers, then the ingresses. Unpause.

- A consumer that restores a checkpoint older than the cut-over cannot replay the old range: the
  floor is at the cut. It takes a newer checkpoint, or the replay answers `REPLAY_UNAVAILABLE`
  and it fetches a peer checkpoint. The deploy takes a checkpoint of each executor after step 6.
- **A chain reset** (genesis, empty state) is the simpler alternative for staging. It needs no v10
  read and no drain. It loses the chain, the bridge state and the canary history. Section 16 asks
  the user.

### 12.3 Deploy order

1. PR 1 to PR 8 merge with the flags off. Each deploys as a normal rolling release. They change no
   format that the base cannot read.
2. PR 9 (benchmark) runs on the flagged build in a container cluster, not on staging.
3. PR 10 is the cut-over release: the waivers, the flags on by default, the runbook of 12.2.
4. PR 11 removes the dead code. It is a normal release, because the cut-over already stopped every
   writer of the removed kinds.

### 12.4 Rollback

- Before the first v12 snapshot, a rollback is the coordinated restart into the old release: it
  restores its own v10 snapshot. Every envelope ordered after it is lost, so do it only with the
  submits still paused.
- After the first v12 snapshot, the old release cannot read the snapshot. The rollback is:
  pause, drain until `posted_head == sealed_head`, then the fleet rebuild from L1 at the posted
  head into the old release (`docs/runbooks/sealer-fleet-rebuild.md`). Nothing is lost, because
  L1 holds every block.
- The deploy record gets a rollback floor at the cut-over release (`docs/formats.md:155-171`).
  `just rollback` does not cross it. `docs/runbooks/deploy-rollback.md` gets the path above.

## 13. Implementation plan

Each item is one pull request. Each passes `just style` and CI alone and follows `docs/STYLE.md`.

```text
 PR1 (fragmentation) ──┬──> PR3 (wire) ──┬──> PR4 (sealer store, pairing) ──┬──> PR5 (feed)
 PR2 (bench tooling) ──┘                 ├──> PR6 (ingress)                  │
                                         └──> PR7 (sequencer reference)      │
                                                       PR8 (engine reader) <─┘
                                    PR4..PR8 ──> PR9 (benchmark, gate) ──> PR10 (cut-over) ──> PR11 (removal)
                                                                                         └──> PR12 (tx_heads, optional)
```

1. **Fragmented session messages.** The cluster ingress and egress carry messages up to 1 MiB
   (section 4.5). Tests: 64 KiB and 1 MiB both ways in a `TestCluster`. No behavior change.
2. **Benchmark tooling.** The `calldata` workload, the seal-latency histogram, the snapshot `ms=`
   field, a `just` recipe that runs the matrix and writes a table. A baseline run on `main` goes
   into `docs/benches/`.
3. **Wire.** Java and Rust constants and codecs: kind 10, kind 2 mode, record types 5 and 6, egress
   14 and 15. Readers only. `formats.toml`: `reads_max` raised.
4. **Sealer store and pairing.** The envelope store, the waiting references, the byte budget, the
   v12 snapshot reader and writer, all behind `kardamom.cluster.carryBytes` (off: v10, no store).
   Java state tests: pairing in both orders, TTL expiry, the lane cap, a duplicate of a waiting
   reference, the refusal at the deadline and its queue, equal snapshot bytes on every member,
   v10 restore.
5. **Member feed.** The ring, the feeder thread, the feed control, the replay walk, the `idle` and
   `gapped` states. `TestCluster` test: the feed of each member equals the leader's egress byte
   for byte; a stalled subscriber does not slow the commit.
6. **Ingress.** The envelope session, the batch task, the ledger and re-offer, the back-pressure
   and readiness, the reference-mode observer. Behind `--carry-bytes`.
7. **Sequencer.** The `RT_TX_ORDER` reference, behind `--carry-bytes`. The batch limit becomes a
   byte limit.
8. **Engine reader.** `RT_TX` entries carry the envelope. The feed transport and the source order
   of section 7.5 for the executor. The validator's hash and signature check. The batcher packs
   from the entry. Behind `--carry-bytes`; the join path stays while the flag is off.
9. **Benchmark run and gate.** The matrix of section 10.6 on both builds. The results go into this
   spec as section 17. The user decides on the target and the defaults.
10. **Cut-over release.** Decision version 3, the flags on by default, the waivers, the pairing in
    the Nomad jobs, the runbook of 12.2, the rollback floor. The chaos cases of 13.1.
11. **Removal.** Everything in section 9.1. `reads_min` of the snapshot to 12 in the release after.
12. **`tx_heads` (optional).** The ingress publishes `TxHeader`, and the sequencer reads it. The
    sequencers stop reading `tx_data`. `tx_data` goes.

Rules to check in each pull request: R1 (comments carry no phase or spec names), R5 (the feeder,
the ring and the envelope task each have one owner), R6 (the feed is an associated type of the
reader's wiring, not `dyn`), R9 (parse each frame once at the boundary), R12 (checked arithmetic
for the byte counters and the wire lengths), R14 (one codec and one ingest path for the feed and
the egress), R16 (the release drain stays a one-step loop body).

### 13.1 Chaos cases

| Case | Change | Assertion |
|---|---|---|
| `archive-driver-loss`, `archive-tx-data-wipe`, `archive-corruption` (`crates/chaos/src/cases/archive.rs`) | Remove the `tx_data` parts. Keep the `tx_deposits` archive loss. | The deposit path recovers. |
| `pipeline-blackout-recover` (`crates/chaos/src/cases/coordinated.rs:104-150`) | Drop the void-decision count. | Every acknowledged transaction has a receipt after the blackout. No `VOID` line exists. |
| `hard-executor`, `graceful-executor` (`crates/chaos/src/cases/component.rs:7-34`, `exec_stream.rs`, `recorded_cursor.rs`) | Drop the stream and cursor checks. | The restarted executor resumes through its feed (`input_source="feed"`) with no gap. |
| `batcher-outage-past-retention` (`crates/chaos/src/cases/l1/outage.rs`) | Both halves stay. The second half (spool wiped) now recovers from the sealer replay, not from `kardamom_getBlockRefs`. | The rebuild line (`crates/chaos/src/cases/l1/batcher.rs:22-23`) count does not grow. L1's record is contiguous. Root parity holds. |
| `sealer-fleet-total-wipe-recover` (`crates/chaos/src/cases/fleet/sealer_wipe.rs`) | None. The rebuild mark does not change. | As today. |
| `validator-join` | Rename to `validator-catchup`. | The validator catches up through the egress with bytes. |
| `feed-member-kill` (new) | Kill sealer member 1 under load. | Executor 1 switches to the egress within 10 s and back after the restart. No gap. The executor roots equal the validator's. |
| `feed-slow-executor` (new) | SIGSTOP executor 2 longer than the ring holds, then SIGCONT. | The commit latency of the other members does not rise. Executor 2 catches up through a feed replay. |
| `envelope-loss` (new) | Drop the envelope session traffic of one ingress to the leader for 20 s, then restore. | References wait. The re-offer fills them. A reference past its deadline is refused and its nonce is free. No sender stays stuck. |
| `leader-kill-envelopes` (new) | Kill the leader under `on-quorum` load. | Every acknowledged transaction has a receipt. |
| `envelope-cap` (new, small `envelopeBytes`) | Stop both replicas of lane 0 for 3 minutes. | Lane 0's senders get `Overloaded`. Lane 1 continues. After the start, lane 0 drains. |
| Planned and dropped | `exec-peer-fetch`, `record-lag-halt`, `validator-exec-archive-catchup`, `exec-archive-prune` of the archived-data spec | Not built. |

## 14. Docs that change with the code

- `docs/failure-modes.md`: the sealer section (the store, the waiting references, the byte
  budget), the void section goes, the executor section (the feed), the ingress section (the
  envelope session, the ack policies), the batcher resume sources.
- `cluster/sealer-service/README.md`: kinds, record types, settings, snapshot v12, the feed, the
  decision version table (version 3).
- `docs/aeron-discovery.md`: the `sealer_feed` topic and streams; `exec_txs` and the `tx_data`
  archive topics go.
- `docs/observability.md`: the new metrics of sections 6.3, 7.5 and 10.6.
- `docs/l1-data-path.md`: the batcher reads bytes from the egress.
- `docs/json-rpc.md`: the `Overloaded` error on a full envelope ledger.

## 15. Considered and not chosen

- **Variant B: no sequencer.** The ingress offers each envelope with its guard header, and the
  sealer orders it with its nonce contiguity. It was not chosen as the first step:
  - the sequencer's work would move into the sealer or the ingress: the fee gate, the parking of
    future nonces, the sender floors from receipts, Redis and the executors, the epoch relay, and
    the rewind on a refusal (`docs/failure-modes.md:474-540`);
  - the sealer has no account state for the fee gate;
  - two active/active ingresses take one sender's transactions, so each ingress would need the
    sender's nonce order across both;
  - and the racing pair would double every envelope in the log, because Aeron Cluster appends
    every ingress message before the service sees it.
  It stays a possible later step after the store and the feed run.
- **The sequencer offers the envelope.** The racing replicas would put every envelope in the log
  twice. Variant A puts it in once.
- **A side stream from the ingress to the sealer nodes.** It is not log input, so the members could
  see different bytes. The user ruled it out: bytes enter only as Raft log input.
- **The catch-up from the member's Raft log.** The log holds the inputs, not the canonical entries
  (section 7.3).
- **`ExecTxRecord` for the remote consumers.** The egress session already carries the canonical
  frames with replay (section 9.2).

## 16. Open questions for the user

1. **Target rate and size.** Which rate and transaction size must one Raft group carry? Section
   10.6 measures the curve. The defaults of the caps and the heap follow from the answer.
2. **Placement.** Run sealer member `i` and executor `i` on one node (IPC feed, 3 fewer nodes,
   shared CPU), or keep the node classes and feed over UDP?
3. **Cut-over or chain reset** on staging (section 12.2)?
4. **Defaults.** `envelopeBytes` 64 MiB, `waitingRefs` 16,384, `daLagBudgetBytes` 1 GiB,
   `feedRingBytes` 64 MiB, the sealer heap 2 GiB. Accept them as the starting values for the
   benchmark?
5. **`tx_heads`.** Build PR 12 in this program, or only when the benchmark shows the sequencer
   input matters?
6. **Remote consumers from a follower.** Each follower could also serve the feed over UDP to the
   validator, the batcher and the elastic executors, so the leader sends only the replication.
   Build it now, or only when the benchmark shows the leader's egress as the limit?
7. **A stream digest.** Should each boundary carry a running hash of the canonical stream, so an
   executor that switches source detects a divergent member? It changes the boundary frame.
8. **Executor checks.** Should the executors also check `keccak256(raw_tx) == canonical_id`? It
   costs about 1 µs for each transfer. The validator already checks it.
9. **`batcher-outage-past-retention`.** The second half can no longer reach the rebuild, because
   the floor keeps every unposted frame with its bytes. Is "replay only" the right assertion
   (owner of the batcher resume path to confirm)?

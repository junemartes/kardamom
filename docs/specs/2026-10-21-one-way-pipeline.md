# The one-way pipeline

- Status: decided by the user on 2026-10-21. Not implemented yet (section 9).
- Amended by the user on 2026-10-25: decisions 45 to 52 (D12 to D19), in the section "Amendment
  of 2026-10-25". Where the amendment and the older text differ, the amendment wins. The older
  text stays, marked "superseded by decision N (2026-10-25)".
- Issues: refs #582, #585, #706, #709, #712.
- Amends `docs/specs/2026-10-10-raft-carried-transactions.md` (the carry spec). Section 16 of the
  carry spec lists these decisions as decisions 34 to 43. D11 is decision 44, and D12 to D19 are
  decisions 45 to 52. Where this spec and the carry spec differ, this spec wins.
- Amends the design comments of #585 (the serving service, called the egress here) and #706 (the
  sequencer gateway).

## Amendment of 2026-10-25

The user decided on 2026-10-25 that no stage sends a message back to an earlier stage, and that
every read goes to the egress. Two analysis notes of 2026-10-25 list the back arrows and the read
paths of today. This amendment removes them.

### A.1 The back arrows and the read paths of today

| Back arrow | From, to | Goes by |
|---|---|---|
| The ingress waits for the receipt (`ack=receipt`, the default of `deploy/cluster/nomad/ingress.nomad.hcl:32-39`) | executor, ingress | decision 47 |
| The admission credit and the v1 republish of the sequencer read `tx_receipts` | executor, sequencer | decisions 45 and 51 |
| The nonce and balance view: Redis (the state mirror reads `tx_receipts`) and the state query of an executor (port 9024) | executor, ingress | decisions 45 and 46 |
| The refusal kinds 5, 7, 8, 10, 13 and 18 on the egress session, and the `TxDrop` of kind 18 on `tx_drops` | sealer, sequencer | decision 48 |
| `tx_errors` and `tx_verdicts` | sequencer, ingress | decision 50 |
| The posted cursor (ingress kind 7). It feeds the DA-lag guard (10,000 blocks) and the posted rule of the retention floor | batcher, sealer | decision 49 |
| The status frame (egress kind 9). The ingress pauses on `da_lag` and on no quorum | sealer, ingress | decisions 47 and 49 |
| The void (ingress kind 6) and the recorded cursor (ingress kind 9). Both are inert under decision version 5 | executor, sealer | decision 51 |

- **The reads of today.** The ingress serves every read method from sources that are not the
  egress: a cache that `tx_receipts` fills, the state query of an executor, Redis, the cluster
  egress session (`safe`, `finalized` and `kardamom_chainStatus`), and the boundaries of the
  executors (`eth_blockNumber` and the fee methods).
- **The egress of today** serves only `/v1/frames`, a receipt read of the newest receipts in
  memory, and `kardamom_getTransactionStatus` (section 10.4).

### A.2 The decisions

45. **Receipts come only from the egress (D12).** Every service reads the transaction receipts
    from the egress, never from an executor:
    - the sequencer: the account rows and the admission credit;
    - the state mirror, so the egress feeds Redis;
    - the notifier;
    - the validator: its receipt cross-check reads `egress_receipts`. It then checks the copy
      that the clients see;
    - the JSON-RPC reads (decision 46).

    After this change, the egress is the only reader of `tx_receipts` (1002), its boundaries
    (1003) and the receipt and account reads of the executor state query. The peer checkpoint
    fetch is not a receipt read, and it stays.
    Reason: one source gives every reader the same view, and each executor has one output.

46. **The egress serves every JSON-RPC read (D13).** The egress serves `eth_getTransactionReceipt`,
    `eth_getBalance`, `eth_getTransactionCount`, `eth_blockNumber`, the fee methods,
    `kardamom_blockNumberByTag` (with `safe` and `finalized`), `kardamom_chainStatus` and
    `kardamom_getTransactionStatus`.
    - The notifier serves the subscriptions (WebSocket), not the egress (user decision of
      2026-10-25, resolving the conflict with #817). No webhooks for the new stage events for now.
    - The load balancer sends the read methods to the egress. It sends only
      `eth_sendRawTransaction` and `kardamom_sendRawTransactionAsync` to the ingress.
    - The ingress has no read path and no receipt stream.
    - The egress needs a durable receipt store, an account view, and the heads from a source that
      is not a sealer session. A.5 gives the source of each read.

    Reason: the process that holds the receipts answers for them. The ingress then keeps only the
    submit, and it needs no state.

47. **`ack=validated` is the only ack mode of the ingress (D14).** `ack=receipt`, `--verdicts`,
    the ack policies `on-offer` and `on-quorum`, the egress tap of the ingress and its quorum
    observer go.
    Reason: the answer then needs nothing from a later stage. The ingress holds no waiter and no
    cluster session.

48. **No custom refusal kinds (D15).** The sealer refusal kinds 5, 7, 8, 10, 13 and 18 go. The
    `TxDrop` on `tx_drops` that kind 18 causes goes too. Aeron back-pressure and back-off
    replace them: a publication that cannot offer waits or retries, and a full buffer slows the
    producer. A.3 states what replaces each kind and what the client sees.
    Reason: a refusal is a message to an earlier stage, with its own wire kind, handler and client
    error. Back-pressure gives the bound with no message.

49. **Back-pressure bounds the DA lag (D16).** The sealer does not receive the posted cursor
    (batcher ingress kind 7). The DA-lag guard goes. The bound comes from Aeron back-pressure
    through the egress (A.4). The sealer and its member feed never block: a full feed ring stays
    a gap, as today. The floor of the egress store takes the posted head from L1 (`l1_blocks`,
    #807). The ingress pause on `da_lag` goes.
    Reason: the sealer then reads nothing from a later stage. The bound acts where the load comes
    in, at the ingress, and not on the order.

50. **No `tx_errors` and no `tx_verdicts` (D17).** The sequencer sends nothing to the ingress. A
    rejected or dropped transaction is visible in two ways (user decision of 2026-10-25,
    resolving the conflict with #817):
    - the status API of the egress (`kardamom_getTransactionStatus`);
    - a pushed event through the notifier, which also pushes the receipts.
    Reason: with decision 47, no reader of these streams answers a client. The drops already go
    to the egress on `tx_drops` (section 3.3).

51. **Carry is the only mode after the staging activation (D18).** After the carry window rollout
    on staging (`docs/runbooks/carry-window-rollout.md`), the carried path is the only mode. The
    reference mode (the `TxRef` and the `tx_data` join, the void, the recorded cursor, the
    refetch, `exec_txs`) and the other legacy modes stay only until then. A later PR removes them
    (A9 in section 9.2 lists what it removes). If another network still runs a reference-born
    chain then, the removal waits for its activation too (decision 19).
    Reason: two modes double the code of each reader and keep the back arrows of the reference
    mode alive.

52. **A carry-born chain runs the target mode from block 1 (D19).** A chain that starts with
    `KARDAMOM_GENESIS_CARRY=on` (#816) has features 3, 4 and 5 active at genesis. It runs the
    carried path, the exact window, the egress reads and `ack=validated` from block 1. The feature
    of A6 (section 9.2) is active at its genesis too. It never runs a legacy mode, so the legacy
    removal does not change it.
    Reason: a new chain has no history in reference mode, so it has nothing to migrate.

### A.3 What replaces each refusal kind (decision 48)

Under decision versions 4 and 5, the sealer answers a positioned or carried reference only with
kinds 10, 13 and 18. It skips an expired copy with a counter and no frame
(`SealerClusteredService.java:1272-1301`). Kinds 5, 7 and 8 come only from the record path below
version 4.

| Kind | Today | After | The client sees |
|---|---|---|---|
| 5 `CONTIGUITY_REJECT` | The record path below version 4 refuses a record that leaves a nonce gap. The sequencer rewinds and offers again. | Nothing in the target mode. Versions 4 and 5 key the window on the position, not on the nonce. A nonce gap reaches the executor, which writes a skip receipt. The kind goes with the legacy path (A9). | A skip receipt (`status` `0x0`, `gasUsed` `0x0`) from the egress. The client sends again. |
| 7 `PAST_DEADLINE` | The record path below version 4 refuses a record past its inclusion block. | Nothing in the target mode. The deadline field carries the position. A copy a window behind its image is `EXPIRED`: the sealer skips it with a counter. The kind goes with the legacy path (A9). | No receipt and the status `unknown` for a reference that came too late. The loss rule of the sequencer frees the nonce. |
| 8 `WINDOW_FULL` | The record path below version 4 answers when the dedup window is full. The sequencer offers again. | Aeron back-pressure on the cluster session of the sequencer: the offer fails, and the sequencer retries with back-off. The lane bound of A.4 stops new entries. The kind goes with the legacy path (A9). | A slower submit, then `txpool is full` when the offer of the ingress fails. |
| 10 `DA_LAG_REJECT` | The DA-lag guard refuses a user record when the sealed head is more than the budget past the posted head. The sequencer maps it to `DaLag`. The ingress pauses on `da_lag`. | The back-pressure chain of A.4. | `txpool is full` while the chain is back-pressured. No `-32010` error with the cause `da_lag`. |
| 13 `RECORD_LAG_REJECT` | The record-lag guard. It is off under version 5 (section 5.2). | Nothing. The guard and the recorded cursor go with the legacy path (A9). | No change. |
| 18 `REF_REFUSED` | Under feature 5, the sealer refuses a type-0 reference. The sequencer publishes a `TxDrop` with the reason `SealerRefusedReference`. | The sealer still does not order a type-0 reference under feature 5. It sends no frame: it counts the reference, as an expired one. A sequencer of this release offers no type 0 after the switch (its kept version and floor, section 5 of the carry activation spec). A carry-born chain has no type 0 (decision 52). | Only a sequencer of an older release or with a wrong floor can cause it. The client then sees no receipt and the status `unknown`. |

The decision does not name egress kinds 6 (`REMOTE_ORIGIN_REJECT`) and 12 (`ORIGIN_GAP`). They
answer the epoch and remote-origin records of a sequencer, not a user transaction. They stay
(open question 8).

### A.4 The bound on the DA lag (decision 49)

The chain of back-pressure, from the batcher to the client:

1. The batcher bounds its unposted range. It reads its egress feed only while its spool holds
   fewer than `--da-lag-budget-blocks` blocks above its posted head. The budget moves from the
   sealer to the batcher, and it is a local setting.
2. The egress sends the batcher its frames on a reliable, flow-controlled publication: its own
   stream, or a tagged flow-control group that holds only the batcher. When the batcher stops
   reading, the publication fills, and the egress stops reading `exec_frames`. The validator
   stays best effort on `egress_order` and resumes from the range read, so a slow validator does
   not slow the chain.
3. Each executor publishes `exec_frames` reliably, with minimum flow control over the egress
   instances (`fc=min`). When the slowest instance stops reading, the offer of the executor waits
   with back-off, and the executor stops executing. Aeron removes a receiver that does not answer
   within its timeout from the flow control, so a dead egress instance does not stop the
   executors.
4. The executor stops reading its member feed. The feed ring fills and gaps. The sealer does not
   block. When the executor reads again, it resumes from its cursor inside the window, or from
   the range read of the egress (section 6.4).
5. No new block executes, so no new receipt reaches `egress_receipts`. The credit of each lane
   runs out: the forwarded entries of the lane with no egress receipt reach the lane bound. The
   sequencer stops reading `tx_data`.
6. The `tx_data` publication of the ingress fills. The offer of the ingress fails within its
   deadline, and the call answers `txpool is full` (section 3.1).

The limits:

- **The lane bound.** Today the credit is a limit for each sender (`max_forwarded`,
  `crates/sequencer/src/admission/mod.rs:62-70`), so many senders can still flood a lane. Step 5
  needs a bound for each lane. The sum of the lane bounds must stay below the replay window of
  the sealer (65,536 frames). Then an executor that the egress slows never falls below the
  window (open question 6).
- **The empty blocks.** Boundaries and deposits use no credit, so the sealer seals them during a
  stall. At one boundary for each block, 65,536 frames last about 18 hours with 1 s blocks. A
  longer stall leaves the executors below the window with no egress copy above the store
  (open question 7).

The consequences:

- The batcher sends no posted cursor and keeps no cluster session.
- The sealer removes the guard, the budget (`kardamom.cluster.daLagBudgetBlocks`), egress kind 10
  and the posted head in its state. The guard changes which records the sealer orders, so the
  removal ships behind a new feature flag (section 7).
- Egress kind 9 (the status) stays for the session liveness of the sequencer: the snapshot
  notice (`crates/sequencer/src/bin/kardamom-sequencer/feeds.rs:494-501`). Its posted head and
  its halted flag carry 0 after the switch.
- **The retention floor.** The exact window of the sealer (D11) needs no posted head. The posted
  rule of a member below feature 5 needs the posted cursor, so the removal of the cursor comes
  after feature 5 is active on every network. The floor of the egress store is
  `min(posted_head, final_head)`: the posted head from `l1_blocks` and the final head from the
  withdrawal oracle (#807).
- The ingress pause on `da_lag` goes, with the `da_lag` cause of the sealer, its runbook and the
  chaos cases `da-lag-halt` and `canary-da-lag`. The ingress pause on `sealer_no_quorum` goes
  too: the ingress holds no sealer session (decision 47). The back-pressure covers both cases.
- The stored cursor (ingress kind 12, #639) is a message from the egress to the sealer. It is not
  built. The Raft log purge follows the snapshots only (open question 8).

### A.5 The source of each read in the egress (decision 46)

| Method | Source in the egress |
|---|---|
| `eth_getTransactionReceipt` | The durable receipt store: the receipts in the segments of the frames, with an index from the hash to the receipt. The floor is the floor of the frames (open question 3). |
| `eth_getBalance`, `eth_getTransactionCount` | The account view: the rows of `egress_receipts` in memory, then Redis on a miss. The state mirror feeds Redis from `egress_receipts`. The head block only, as today. |
| `eth_blockNumber`, `latest`, `pending` | The head of the store: the newest block that an executor executed and the egress stored. |
| `eth_feeHistory`, `eth_maxPriorityFeePerGas`, `eth_gasPrice` | The boundaries of each block with the base fee, and the tip rates of the receipts. The ring rebuilds from the store at a start. |
| `safe`, `finalized` | The posted head from `l1_blocks` (#807). The L1 follower publishes only finalized L1 blocks (open question 1). |
| `kardamom_chainStatus` | `posted_head` from `l1_blocks`. `sealed_head` becomes the head of the store, which lags the sealer by the executor delay. The halts come from the `events` stream. The sealer state comes from the age of the newest frame (open question 2). `da_lag_budget_blocks`, `best_recorded` and the `record_lag_*` fields go. |
| `kardamom_getTransactionStatus` | The receipts and the drops, as built (section 10.4). |
| `kardamom_subscribeReceipts`, `kardamom_subscribeTxStatus` | The notifier, fed by `egress_receipts` and the drop and refusal events (decisions 46 and 50, updated on 2026-10-25). No webhooks for the new stage events for now. |
| `eth_chainId` | A setting. |

The load balancer must read the method of each request (open question 9).

### A.6 The text that this amendment supersedes

- This spec: section 1 (the one back edge), section 3.5 (the ack switch and the reads of the
  ingress), section 6.3 (the ingress row), section 6.4 (the DA-lag guard stays), and the ingress
  part of P10 in section 9.
- The carry spec: decision 11 (the executor as the hot read tier), section 5.5 (the DA-lag guard
  stays), section 6.4 (the ack policy), section 8 (the posted cursor of the batcher, the ingress
  observer, the receipt fallback of the ingress, the state mirror on `tx_receipts`), and section
  8.4 (the two read tiers).
- `docs/specs/2026-10-23-carry-activation.md`: section 4.3 (kind 18 and its `TxDrop`), and the
  sentence "The DA-lag guard does not change" in section 8.
- Issue [#678](https://github.com/junemartes/kardamom/issues/678) (a sealer disk reserve and an
  ingress pause while a quorum is low on disk): the back-pressure chain of decision 49 likely
  replaces the ingress pause. The disk reserve itself is not a back edge.
- `docs/failure-modes.md` and `docs/json-rpc.md` describe the current state. The PRs of section
  9.2 update them.

### A.7 Open questions for the user

1. **The `safe` and `finalized` heads.** The egress learns the posted head only from finalized L1
   blocks. So `safe` moves later than today, by the L1 finality delay (about 13 minutes on
   Ethereum). Is that correct? Or does `safe` need an L1 post at inclusion, and `finalized` the
   final head of the oracle?
2. **The sealer state in `kardamom_chainStatus`.** The egress holds no sealer session. It sees a
   stopped sealer only as the age of its newest frame. Is that enough? The other options are an
   admin poll of the members by the egress, or a sealer entry on the `events` stream.
3. **The retention of the receipts.** Does the receipt store keep the floor of the frames
   (`min(posted_head, final_head)`, at least 1 day)? Or does the public history need a longer
   window? An older receipt answers `null`.
4. **A rejected transaction with no `tx_errors`.** A wallet that reads only
   `eth_getTransactionReceipt` sees `null` for a dropped transaction. A lost entry (an expired
   copy, a type-0 reference, a lost forward) has the status `unknown`, not `dropped`. Must the
   sequencer report an entry that its loss rule frees as a drop on `tx_drops`?
5. **A submit while the chain is back-pressured.** The ingress waits for its offer deadline, then
   answers `txpool is full`. How long is the deadline? Does the ingress fail at once, or hold the
   call up to the client timeout? Does it answer an HTTP 429 or 503?
6. **The lane bound.** Do you accept a new bound for each lane on the forwarded entries with no
   egress receipt? What is its size? The sum over all lanes must stay below the replay window.
7. **A long batcher stall.** After about 18 hours of boundaries, the executors fall below the
   window, and the egress holds no copy above its store. The options: the egress keeps storing
   to disk and stops only at a local block budget; the window grows; or an operator restores from
   a seed.
8. **The frames that the decisions do not name.** Egress kinds 6 and 12 to the sequencer, kind 17
   (the version report), and the `tx_status` side stream of the sequencer. This spec keeps kinds
   6, 12 and 17, and keeps `tx_status` until the notifier reads only the egress. Is that correct?
9. **The routing of the load balancer.** The method is in the JSON body, and a batch can mix a
   send with reads. Which option: a load balancer that reads the body, two URLs, or an egress that
   forwards the send methods to the ingress?
10. **The Raft-log purge floor.** Today the sealer purges its Raft log only below the posted head
    (`PurgePlanner`), and the posted head comes from the posted cursor of the batcher (ingress
    kind 7). Decision 49 removes the posted cursor from the sealer. On 2026-10-24 the user decided
    that the purge floor moves to the stored head of the egress, through ingress kind 12, with the
    egress fed by the executors ([#639](https://github.com/junemartes/kardamom/issues/639) and its
    two decision comments). Kind 12 is a message from a later stage to the sealer, so it is a back
    edge in the sense of decision 48. Which floor holds:
    - #639: the egress stored head through kind 12 (an exception to decision 48);
    - the snapshot margin only: the sealer purges below its newest snapshot and keeps a fixed
      margin, and a reader below that margin recovers from the egress or a peer checkpoint;
    - another source that sends no message to the sealer?

11. **The conflict with the notifier spec of #817.** The user decided on 2026-10-25:
    - the notifier serves the subscriptions, and the egress serves every other read method
      (decision 46, updated);
    - drops and refusals are pushed as events through the notifier, and the egress status API
      serves them too (decision 50, updated);
    - no webhooks for the new stage events for now.

    Still open: the executor `executed` tap of #817 (a read of the executors against decision
    45). The proposal of #817 keeps it behind `KARDAMOM_NOTIFIER_EXECUTED`, off by default, for the
    evaluation only. Also open: the ingress halt on an explicit `da_lag` status of #817 against
    decision 49.

## 1. The principle

Each stage sends only to the next stage, and each stage sends the transaction data:

```text
client ─TLS─▶ LB ─HTTP (sends)─▶ ingress ─▶ sequencer ─▶ sealer ─▶ executor ─▶ egress ─▶ batcher
               │                (own fleet)                                    ├──▶ validator
               │                                                               ├──▶ sequencers, state mirror, notifier (receipts)
               └──────────────── HTTP (reads) ────────────────────────────────▶└──▶ clients (JSON-RPC reads, webhooks, WebSocket)
```

- Each stage reads only the stage in front of it. It reads no side stream and no archive of an
  earlier stage. So no stage joins a reference with its bytes, and no gap between a reference and
  its bytes can exist.
- The egress is the only output of the executors. The account rows to the sequencers are the one
  back edge. They carry account state, not transactions. (Superseded by decisions 45 and 48
  (2026-10-25), as the next item states.)
- **No stage sends a message to an earlier stage (decision 48).** No stage sends an answer, a
  refusal, a drop notice or a cursor upstream. The only back edge is Aeron back-pressure: a full
  publication slows its producer.
  - The receipts of the egress go to the sequencers, the state mirror and the notifier. They
    carry account state and the admission credit (decision 45). They are not an answer to a
    stage. Their delay is the signal of the back-pressure (amendment A.4).
  - The session frames of the sealer to its own clients carry no transaction and no cursor: the
    version report (kind 17) and the status for the liveness of a session (kind 9).
- Every read goes to the egress. The ingress takes only the submit (decision 46).
- TLS ends at the load balancer. The ingress has no TLS code. The answer to a request goes back on
  the connection that carried the request, so the process that took the request answers it.

## 2. Decisions of the user (2026-10-21)

| Id | Decision |
|---|---|
| D1 | `eth_sendRawTransaction` returns the hash when the ingress has validated the transaction and published it on `tx_data[lane]` (the user, 2026-10-22). The sequencer sends nothing back to the ingress. A later drop goes one way to the egress (section 3). This replaces the first form of D1, the ack of the sequencer on `tx_verdicts`. Decision 47 (2026-10-25) makes this the only ack mode, and decision 50 removes `tx_verdicts` and `tx_errors`. |
| D2 | The ingress reads no receipts. Receipts go only to the egress. The strong confirmation goes through the egress: webhooks and the WebSocket status feed. `eth_sendRawTransactionSync` (EIP-7966) is not served. Decision 46 (2026-10-25): the ingress serves no read method either. The egress serves every read. |
| D3 | The ingress runs on its own fleet, apart from the sequencers (the user, 2026-10-22). It keeps no storage. This replaces the first form of D3, the ingress inside the sequencer process: the in-process mode of #725 goes. |
| D4 | Each twin sequencer offers one cluster message for each transaction: the reference first, then the envelope. The sealer reads the reference, checks the window of decision version 4, and skips the bytes of a reference that it saw. Otherwise it orders the reference and its bytes (section 5). |
| D5 | The executors read the transaction bytes from the order. They do not join with `tx_data`. The void votes, the voter ids, `VoidLedger`, the archive refetch, the peer fetch (#543) and the `tx_data` recorders go. #709 cannot happen. |
| D6 | The executors send their output to the egress only: the order with its bytes, the receipts and the account rows (section 6). |
| D7 | The batcher and the validator read the egress. |
| D8 | The sequencers read the account rows from the egress. A delay of the rows is accepted: the client ack does not wait for them. Decision 45 (2026-10-25): the sequencers also take the admission credit from the egress receipts, with a bound for each lane (amendment A.4). |
| D9 | The validator independence holds without a stream hash. Each executor runs on the host of a sealer member and reads the order of that member. The egress gets one copy of the order from each executor and compares the copies, the transaction bytes included. |
| D10 | The feature id of the carry flag is 4, not 3. `FEATURE_SEALER_V4` took id 3 (#699). The carry flag needs decision version 4 active. |
| D11 | The sealer keeps only a small replay window. It sends the order to its paired executor on the member feed. A reader that falls below the window, an executor too, recovers from the range read of the egress. The egress, not the sealer, holds the blocks above the posted head (the user, 2026-10-23; section 6.4). This is decision 44 of the carry spec. |
| D12 to D19 | Decisions 45 to 52 of 2026-10-25: the receipts only from the egress (D12), every read on the egress (D13), `ack=validated` only (D14), no refusal kinds (D15), the DA-lag bound by back-pressure (D16), no `tx_errors` and `tx_verdicts` (D17), carry as the only mode after the staging activation (D18), and a carry-born chain in the target mode from block 1 (D19). See the section "Amendment of 2026-10-25". |

The carry spec decisions that change:

- **Decision 12 (the notifier is removed for now) is replaced by D2.** The egress has the client
  side of the notifier: webhooks and `kardamom_subscribeTxStatus`.
- **Decisions 16 and 27, the transport G17 (the egress tails a sealer member over UDP), are
  replaced by D6.** The egress reads the executors. The rest of decision 27 stays: independent
  instances, one per host, a minimum of 2, the stored head that K = 2 instances hold, the copy
  from a peer at a scale-up.
- **Decision 21 changes the feature id to 4 (D10).**
- **Section 6.4 (the ack policy) and open question 16.1-2 (the ingress observer) are answered by
  D1.** The ingress does not observe the order. The kind-15 frame has no reader. The sealer does
  not send it to the sequencer sessions.
- **Open question 16.1-3 is answered by D5.** #543, #542, #554 and #570 close as superseded.
- **The retained file of the carry spec is not needed with D11.** The sealer holds no frame past
  its window, so its heap and its snapshot have a fixed bound at any DA lag (#751), and the
  snapshot stays small.

## 3. The ack after validation (D1, D2)

### 3.1 The call

1. The ingress computes the hash of the raw bytes. A hash in the duplicate cache (section 3.4)
   answers with the same hash at once: no signature recovery, no second publication.
2. It decodes the transaction, recovers the sender, and runs the static checks: size, chain id,
   gas limit, intrinsic gas, and the fee cap against the fee floor.
3. It checks the nonce against its account view: a nonce below the known nonce of the sender
   answers `nonce too low`. The view comes from the account rows of the egress, and a row only
   moves forward. So the view can lag the chain but never lead it: a rejection is always true,
   and a stale view only misses some rejections. The ingress checks no balance: a balance can
   grow, so a stale balance could reject a valid transaction.
4. It publishes the envelope on `tx_data[lane]` of the owner lane. When the offer succeeds, the
   call answers with the hash. When the offer fails within its deadline (back-pressure, no
   connected twin), the call answers `txpool is full`, and the client can send again.

### 3.2 What the answer means

- The ingress validated the transaction and handed it to the twins of its lane, as a public
  Ethereum RPC hands a transaction to its node.
- The sequencer can still drop it: a nonce below the floor of its mempool, a nonce gap that
  expires, a full parked list, a replacement that is underpriced, or a balance that does not
  cover the worst-case cost. An accepted transaction can also drop later, as in Ethereum: a
  forwarded entry is lost, or a lane loses both twins.

### 3.3 Drops go to the egress

- The sequencer records each drop on the egress as a status of the transaction hash, with a
  reason. It sends nothing to the ingress. Each twin publishes its drops on the `tx_drops` stream
  of its lane, a few bytes for each drop, and the egress subscribes to the stream of each lane. A
  drop that both twins report is one status. A transaction that the sealer orders later wins over
  a drop of one twin: the egress keeps the receipt.
- A client sees a drop through the egress: `eth_getTransactionReceipt` answers null, and the
  transaction status (`kardamom_getTransactionStatus`, the WebSocket status feed and the
  webhooks) answers `dropped` with the reason.
- The reasons use the error texts of geth, so Ethereum tools understand them:

| Reason | Error text | Where |
|---|---|---|
| The nonce is below the known nonce | `nonce too low` | the ingress (answer), or the sequencer (drop) |
| The same hash was seen | `already known` | the ingress (answer, from the duplicate cache) |
| The balance does not cover the worst-case cost | `insufficient funds for gas * price + value` | the sequencer (drop) |
| The same sender and nonce, a fee bump below 10% | `replacement transaction underpriced` | the sequencer (drop) |
| The parked list of the sender, or of the lane, is full | `txpool is full` | the sequencer (drop), or the ingress when the offer fails |
| The fee cap is below the base fee | `max fee per gas less than block base fee` | the sequencer (drop) |
| A parked entry expired | `transaction expired` | the sequencer (drop) |

Replace by fee: a transaction with the same sender and nonce as a parked entry, and a tip and a
fee cap that are each at least 10% higher, replaces that entry. A forwarded entry cannot be
replaced: the sealer may have ordered it.

### 3.4 The duplicate cache

- A fixed-size cache of the hashes of recent transactions, with the recovered sender of each.
  The oldest entry goes when the cache is full. The size is a setting (default 65,536 entries,
  about 4 MiB).
- A client that sends the same transaction again gets the same hash, with no signature recovery
  and no publication. The recovery is the expensive step of the call.
- The cache is not a correctness mechanism. The decision window of the sealer stays the dedup
  of the order.

### 3.5 What the ingress keeps and drops

- It keeps: the hash, the duplicate cache, the decode, the signature recovery, the static
  checks, the account view (the nonce check and `eth_getTransactionCount`), the rate limits, the
  routing to the lane, and the publication on `tx_data`. (Amended by decision 46 (2026-10-25):
  the account view serves only the nonce check. `eth_getTransactionCount` moves to the egress.)
- **The nonce check (decisions 45 and 46).** The ingress reads the nonce from Redis, through a
  short local cache. The state mirror feeds Redis from `egress_receipts`. A Redis value only
  moves forward, so a cached value can lag the chain but never lead it: the rule of section 3.1
  holds. The ingress reads no receipt stream.
- It drops: the pending map and its waiters, the receipt cache and the receipt watchers, the
  quorum watermark gate, the observer session, and the `tx_errors` and `tx_verdicts` streams. The
  sequencer drops their publishers.
- It also drops (decisions 46, 47 and 49): every read method, the state query of the executors,
  the sealer session with its status frame, the pauses on `da_lag` and `sealer_no_quorum`, and
  the ack modes `receipt`, `--verdicts`, `on-offer` and `on-quorum`.
- `eth_getTransactionReceipt` reads the egress. `eth_getBalance` and `eth_getTransactionCount`
  read the account view, which the egress feeds. (Superseded by decision 46 (2026-10-25): the
  load balancer sends every read method to the egress. The ingress serves none.)
- `kardamom_sendRawTransactionAsync` becomes another name of `eth_sendRawTransaction`.
- The ack mode is a configuration switch of the ingress (`--ack validated`). The old mode stays
  until every client moved. The load tools and the canary measure the executed latency through
  the egress after the switch. (Superseded by decision 47 (2026-10-25): `--ack validated` is the
  only mode. The switch and the old mode go.)

## 4. The ingress fleet (D3)

- The ingress runs on its own fleet: the `ingress` Nomad job, on hosts apart from the sequencers.
  The JSON-RPC port and the Consul service `ingress-jsonrpc` stay in that job. The load balancer
  targets the ingress hosts.
- The ingress keeps no storage, so an ingress host needs no fast disk.
- The load balancer cannot route by sender. So any ingress takes any transaction, and publishes it
  on `tx_data[lane]` of the owner lane, to the twins of that lane.
- The ingress gets nothing back from the twins: the call answers after the publication (section 3).
- Both twins read the same `tx_data` image, so they give the same reference (lane, session,
  position). Nothing records `tx_data`.

## 5. The sealer: reference and bytes in one message (D4)

### 5.1 The full reference (ingress kind 0, record type 7, new)

```text
[kind=0][sender:20][nonce:u64][deadline:u64][tip:u128][canonical_id:32][record_type=7][lane:u8]
[term_id:i32][term_offset:i32][session_id:i32][envelope]
```

- The first 99 bytes are the reference of decision version 4 (`TXREF_RECORD_LEN`,
  `SealerWire.java:250-262`), with record type 7 in place of 0. The deadline carries the tagged
  position (`POSITION_TAG`, `SealerWire.java:264-279`).
- The envelope follows: the frozen rkyv `TxEnvelope`, 80 bytes + `raw_tx`, as in record type 5
  of the carry spec (section 4.1).
- Record type 5 (`RT_TX_OFFER`) has no session id, so the window of version 4 cannot key it. Record
  type 7 replaces it. Record type 5 stays only for the retained-file members of the carry spec.

### 5.2 The record path under decision version 5

Decision version 5 is version 4 with the carried bytes. The carry flag (feature id 4) turns it on.

1. The sealer reads the 99-byte reference at its fixed offsets, as `onPositionedRef` does for
   version 4 (`SealerClusteredService.java:1042-1058`).
2. It checks the window of the (lane, session) (`SessionWindows.check`).
3. `DUPLICATE` or `EXPIRED`: it skips the envelope. It reads no byte of it and copies nothing.
   This is the copy of the second twin.
4. `FRESH`: it runs the DA-lag guard, marks the window, and writes one relayed frame of record
   type 6 (`RT_TX`) to the ordered output: `[canonical_id:32][6][lane:u8][envelope]`. It copies the
   two parts from the log buffer into the output ring. It allocates nothing.
5. A record type 0 reference still takes the path of version 4, so a sequencer that has not
   switched yet loses nothing. Its entry still needs the join until the cleanup.

Under version 5, the record-lag guard is off, and the sealer drops the void requests (ingress
kind 6) and the recorded cursors (kind 9) as unknown kinds.

### 5.3 The switch

- `VersionSwitch` (`core/.../VersionSwitch.java`) becomes a switch of three versions. Feature 3
  turns version 4 on. Feature 4 turns version 5 on, and only when version 4 is active. The switch
  happens at a block boundary, as today.
- The snapshot version becomes 16: it carries the state of feature 4. The upgrade transaction of
  feature 4 comes only after every member reads snapshot 16.
- The sealer reports version 5 in egress kind 17. A sequencer sends record type 7 after it reads
  version 5 in kind 17, and record type 0 before.

### 5.4 The readers of the ordered output

- `ClusterTxOrderingSubscription` drops `EgressItem::Tx` today
  (`crates/engine/src/reader/cluster/mod.rs:240-245`). It yields `Ordered::Tx` instead. The engine
  reader then runs `on_carried` for it: no join, no refetch and no void
  (`crates/engine/src/reader/threads.rs:486-506`).
- So the executors, the validator and the live batcher read the bytes from the order with one
  change, and they still join the record type 0 references until the cleanup.
- `CanonicalTx::parse` checks `keccak256(raw_tx) == canonical_id` (`wire/entry.rs:64-104`).

## 6. The egress fed by the executors (D6 to D9)

### 6.1 What each executor sends

| Stream | Content | Today |
|---|---|---|
| `exec_frames` (new) | Each frame of the ordered output that the executor executed, byte for byte, in order, with the bytes of each `RT_TX` | none |
| `tx_receipts` (1002) and its boundaries (1003) | The receipts of each batch, the account rows that the batch wrote, the rollbacks | as today |
| `tx_bal` (1004) | One BAL frame for each block | as today |

- The executor publishes `exec_frames` after the commit of each block, so the egress never gets a
  frame of a block that the executor rolled back.
- Every executor publishes. The egress reads all of them.

### 6.2 What the egress does

- **It compares.** For each canonical index it keeps the first copy and compares the copy of each
  other executor with it, the transaction bytes included (D9). A difference counts
  `kardamom_egress_copy_mismatch_total{executor}` and fires an alert. It does not stop the egress.
  Until the member feed serves version 5 (#638), each executor reads the order from the cluster
  egress of the leader, not from its own member. The comparison still finds a fault of one
  executor, which is the fault that D9 is about.
- **It stores.** It keeps an append-only log of the frames and the receipts of each block on its
  disk, as the #585 design says: every block above `min(posted_head, final_head)`.
- **It serves the internal readers.** A live Aeron stream `egress_order` with the frames of each
  stored block, a range read from a canonical index for a reader that resumes, and one stream of
  account rows for each lane, `egress_rows`.
- **It serves the clients.** `eth_getTransactionReceipt`, the WebSocket feed
  `kardamom_subscribeTxStatus` and the webhooks of `crates/notifier`. A slow client costs only that
  client. Decision 46 (2026-10-25) adds every JSON-RPC read method (amendment A.5).
- **It scales out.** Independent instances, one per host, a minimum of 2 (decision 27 of the carry
  spec).

### 6.3 The readers

Amended on 2026-10-25 by decisions 45, 46 and 49.

| Reader | Reads after the switch |
|---|---|
| Batcher | `egress_order`, and the range read when it resumes. It packs the KAR1 blocks from the bytes of each `RT_TX`. It does not join and does not rebuild from `tx_data` archives. Decision 49: it reads a reliable, flow-controlled publication of the egress, within its spool budget. It keeps no cluster session and sends no posted cursor. |
| Validator | `egress_order`, and the range read when it resumes. It checks the hash and the signature of each transaction. Decision 45: its receipt cross-check reads `egress_receipts`. |
| Sequencer | `egress_rows` of its lanes, in place of the account rows of `tx_receipts`. Decision 45: the admission credit comes from the same receipts, with a bound for each lane (amendment A.4). |
| State mirror | Decision 45: `egress_receipts`, in place of `tx_receipts`. It feeds Redis. |
| Notifier | Decision 45: `egress_receipts` and the drops of the egress, until P8 moves its client side into the egress. |
| Ingress | `eth_getTransactionReceipt` reads the egress. (Superseded by decision 46 (2026-10-25): the ingress reads nothing from the egress. Its nonce check reads Redis, section 3.5.) |
| Clients | Decision 46: every JSON-RPC read except the subscriptions, through the load balancer (amendment A.5). The notifier serves the subscriptions. |

- A reader keeps its old source until the carry flag is active on every network. (Amended by
  decision 51 (2026-10-25): the old sources go after the activation on staging.)
- The egress is on the path of the chain now: the batcher and the sequencers stop when every
  instance stops. When the egress stops, the account tables of the sequencers stop moving, the
  pending lists fill, and admission refuses new transactions of those senders. No forwarded entry
  is called lost, because the loss rule counts blocks of rows.
- With decision 46, every client read stops too when every egress instance stops. The submit
  goes on until the lane bounds fill (amendment A.4).

### 6.4 The sealer window and the recovery from the egress (D11)

- **What the sealer keeps.** The frames of its replay window: the last `retention` frames (a
  setting, 65,536 today). It drops older frames at once. The rule that holds a frame until the DA
  layer posts its block goes from the sealer. So the retained frames have a fixed bound on the heap
  and in the snapshot, at any DA lag, and the snapshot stays small.
- **The live path.** Each member sends its frames to its paired executor on the member feed, as
  today. A request of a cursor inside the window gets the frames from the cursor. A cursor below
  the window gets `REPLAY_UNAVAILABLE`.
- **The recovery.** A reader below the window resumes in this order:
  1. its own state, when its cursor is inside the window;
  2. the range read of the egress, from its cursor to the head of the egress store, then the live
     source;
  3. a peer checkpoint, when its cursor is below the floor of the egress store.
- **The DA lag.** The egress keeps every block above `min(posted_head, final_head)` (section 6.2).
  The DA-lag guard of the sealer stays: it refuses new transactions when the sealed head is more
  than the budget past the posted head. But the sealer holds no frame for it. (Superseded by
  decision 49 (2026-10-25): the guard and the posted cursor go. Aeron back-pressure through the
  egress bounds the DA lag, amendment A.4.)
- **The retention floor (decision 49).** The sealer needs no posted head: its window is exact.
  The floor of the egress store is `min(posted_head, final_head)`. The egress reads the posted
  head from `l1_blocks` of the L1 follower, and the final head from the withdrawal oracle (#807).
  The sealer gets neither.
- **The margin.** The egress sends nothing back to the sealer (section 1), so the sealer cannot
  wait for it. The window must cover the delay from the seal to the store of the egress with a wide
  margin: at 4,000 transactions a second, 65,536 frames are about 16 s. Each egress instance
  exports the distance of its stored head from the sealed head, and an alert fires when it nears
  the window. A reader that falls below both the window and the egress store recovers from a peer
  checkpoint.
- **The margin under back-pressure (decision 49).** When the egress slows the executors, the
  lane bounds stop the user transactions. The sum of the lane bounds must stay below the window,
  so a slowed executor stays inside it. Boundaries and deposits still enter, at about one frame
  for each block (amendment A.4, open questions 6 and 7).

## 7. Upgrade and rollout

- Every change is backwards compatible. A breaking change ships behind a chain feature flag that
  is off, and an L1 upgrade transaction turns it on (the upgrade policy, #661).
- D1 and D3 do not change a chain rule. They are configuration switches of the sequencer and the
  ingress.
- D4 and D5 change the order. They activate with the carry flag (feature id 4, D10).
- D6 to D8 change the readers. Each reader keeps its old source until the carry flag is active on
  every network.
- Decision 49 changes which records the sealer orders: the DA-lag guard refuses some records
  today. Its removal ships behind a new feature flag, the next free feature id, and every member
  switches at the same log entry. Decisions 45 to 47 and 50 change no chain rule: they are reader
  and configuration switches.

## 8. Throughput

- Both twins offer each transaction with its bytes. The Raft log holds both copies before the
  window drops one. With about 200 bytes a transaction, the log takes about 4 MB/s at 10,000 tx/s
  and about 400 MB/s at 1,000,000 tx/s. The leader sends that to two followers.
- Aeron 1.44.0 has no hook in the consensus module that drops a message before the log append.
- The 1,000,000 tx/s goal needs a later change: for example, only one twin offers the bytes, and
  the other offers the reference alone.

## 9. Implementation plan

Each item is one pull request. Each passes `just style` and CI alone, and follows `docs/STYLE.md`.

### 9.1 The plan of 2026-10-21

The ingress part of P10 ("ingress: read the receipts from the egress") is superseded by decision
46 (2026-10-25): the ingress reads nothing from the egress. Section 9.2 orders the remaining work.

| PR | Title | Depends on |
|---|---|---|
| P1 | ingress: the ack after validation, the duplicate cache, the nonce check; sequencer: the drops on `tx_drops` with the error texts of geth, replace by fee; egress: the `dropped` status (D1) | #707 |
| P2 | wire: record type 7, the full reference, Java and Rust codecs on shared golden bytes | none |
| P3 | sealer: decision version 5, the carry flag (feature 4), snapshot 16 (D4, D10) | P2 |
| P4 | engine: the cluster reader yields `Ordered::Tx` for `RT_TX` (section 5.4) | P2 |
| P5 | sequencer: offer record type 7 after kind 17 reports version 5, with the byte bounds of the carry spec (section 6.2 of that spec) | P2, P3 |
| P6 | executor: publish `exec_frames` (section 6.1) | P4 |
| P7 | egress: the service: compare, store, `egress_order`, the range read, `egress_rows`, the receipt read (section 6.2) | P6 |
| P8 | egress: the client side of the notifier moves into the egress | P7 |
| P9 | batcher and validator: read the egress | P7 |
| P10 | sequencer: read `egress_rows`; ingress: read the receipts from the egress | P7, P1 |
| P11 | sequencer: remove the in-process ingress of #725; the `ingress` job stays (D3) | P1 |
| P12 | release: the upgrade transaction of feature 4 on staging, after P3 to P10 deploy | P3 to P10 |
| P13 | cleanup: record types 0 and 4, the join, the void, the `tx_data` recorders (#599) | P12 on every network |
| P14 | sealer: the replay window only, no hold until the posted head; executor: the recovery from the range read of the egress; egress: the distance alert (D11, section 6.4) | P6, P7 |

The open items of the carry spec that this plan does not need for staging: the member feed from
the Raft log (#638), the stored cursor (#639), the recovery after a power loss (#644, #645), the
history key (#675). They stay open in their issues.

### 9.2 The plan of the amendment (2026-10-25)

In this order:

| PR | Title | Depends on |
|---|---|---|
| A1 | spec: the amendment of 2026-10-25 | none |
| A2 | egress: the durable receipt store (the receipts in the segments of the frames, an index from the hash to the receipt, the floor of the frames), the boundaries with the base fee, and the account view (the rows of `egress_receipts`, then Redis on a miss) | P7, #807 |
| A3 | egress: the JSON-RPC read methods and the subscriptions of amendment A.5; the chain status from `l1_blocks`, the `events` stream and the frame age; the client side of the notifier (P8) | A2 |
| A4 | sequencer, state mirror, notifier, validator: read `egress_receipts` by default (decision 45). The sequencer gets the lane bound of its credit. The state mirror feeds Redis from the egress. The validator cross-check reads `egress_receipts`. The egress stays the only reader of `tx_receipts` | P10, A2 |
| A5 | ingress: `ack=validated` only. It removes `ack=receipt`, `--verdicts`, the ack policies, the observer session, the receipt cache and its watchers. The nonce check reads Redis through a short cache. Sequencer: remove the publishers of `tx_errors` and `tx_verdicts`. Notifier: read the drops of the egress. Nomad: remove the variables `ack` and `ack_policy` (decisions 47 and 50) | A4 |
| A6 | back-pressure in place of the refusal kinds (decisions 48 and 49): `exec_frames` reliable with `fc=min`; the egress publication to the batcher reliable and flow-controlled; the spool budget of the batcher, and no cluster session in the batcher. Sealer, behind a new feature: no DA-lag guard, no posted cursor, no kind 10, no kind-18 frame. Sequencer: remove the handlers of kinds 10 and 18 and their `TxDrop`. Ingress: remove the pause on `da_lag`. The docs, the runbooks and the chaos cases change with it | A4; feature 5 active on every network |
| A7 | deploy: the load balancer sends the read methods to the egress and the send methods to the ingress. The ingress removes its read methods, its executor query and its sealer session | A3, A5 |
| A8 | release: the upgrade transaction of the feature of A6 on staging | A6 deployed |
| A9 | cleanup: the legacy removal (decision 51), listed below | the carry window rollout on staging |

A9 removes:

- record types 0 (`RT_TXREF`) and 4, and the `TxRef` decoders of every reader;
- the `tx_data` join in the executors, the validator and the batcher, and the rebuild of the
  batcher from `kardamom_getBlockRefs` and the archives;
- the void: ingress kind 6, `VoidLedger`, the void votes and the voter ids;
- the recorded cursor: ingress kind 9, `RecordedCursors`, the record-lag guard and egress kind 13;
- the archive refetch and the peer fetch;
- the `exec_txs` stream and its archive;
- the `tx_data` recorders (#599);
- the record path of the sealer below decision version 4, with egress kinds 5, 7 and 8 and the
  contiguity guard;
- the type-0 refusal of feature 5 and egress kind 18;
- the posted rule of the heap members below feature 5;
- the v1 sequencer loop and its republish from `tx_receipts`.

After A9, a node of a reference-born chain starts from a checkpoint at or above the activation
block. It cannot replay the blocks of the reference mode from the order. A9 supersedes P13.

## 10. The egress as built (version 1)

This section records how P6 to P10 built section 6. Where it differs from section 6, this section
is the current state, and the difference is a follow-up.

### 10.1 Streams

| Stream | Id | Publisher | Content |
|---|---|---|---|
| `exec_frames` | 1008 | each executor with `--exec-frames` | `[seq:u64][frame]` for each canonical frame |
| `egress_order` | 1009 | each egress | the merged canonical frames |
| `egress_receipts` | 1011 | each egress | the merged receipts, their account rows and the rollbacks |
| `tx_drops` | 1012 | each sequencer, with the lane id in its discovery record | one 72-byte rkyv `TxDrop` for each drop: the hash, the sender, the nonce and the reason |

- An executor sends best effort. It does not wait for the egress. A full publication drops the
  frame and counts `kardamom_executor_egress_frames_dropped_total`.
- The sequence counts the dropped frames too. So the egress sees a gap in a copy.
- A sequencer sends each drop best effort. Every rejection on `tx_errors` that names a hash also
  goes on `tx_drops`. A buffered entry below a receipt floor, and a duplicate that a receipt
  proves, go only on `tx_drops`, as `nonce too low`: the client gets no error for them.
- The reason of a drop maps from the rejection: `DuplicatedTx` and `SealerNonceAhead` are
  `nonce too low`; `AlreadyKnown` and `NonceInFlight` are `replacement transaction underpriced`;
  `Evicted`, `DaLag` and `RecordLag` are `txpool is full`; `Expired` and `PastDeadline` are
  `transaction expired`; `FeeTooLow` is `max fee per gas less than block base fee`;
  `InsufficientFunds` is `insufficient funds for gas * price + value`; `FeeInvalid` is
  `max priority fee per gas higher than max fee per gas`, the seventh text of geth.

### 10.2 The merge and the compare

- The egress delivers a frame when a copy shows that the frame follows the last delivered frame:
  the copy sent it with the next sequence, right after the last delivered frame.
- A gap in one copy never reorders the stream. The copy of another executor fills the gap.
- The egress keeps a digest of the last `--compared` delivered keys. It compares each later copy
  with the first one. A difference counts `kardamom_egress_copy_mismatch_total`. The egress does
  not stop. The validator decides which copy is wrong.
- At most `--max-waiting` frames ahead of the head wait. A frame past that bound counts
  `kardamom_egress_waiting_dropped_total`.

### 10.3 The store

- The store keeps only the frames. It does not keep the receipts on disk.
- Each segment file holds `--segment-blocks` whole blocks. A 25-byte head starts each segment:
  the next index, the next block and the key of the last frame. The frames follow as
  `[len:u32][frame]`, little endian.
- At open the store reads every segment once. It rebuilds the head and cuts a torn frame at the
  end of the last segment.
- The store keeps `--keep-blocks` blocks below its head. The prune does not follow
  `min(posted_head, final_head)` yet (follow-up).
- The store syncs to disk each `--sync-ms` milliseconds.

### 10.4 The HTTP side

The listener binds `--bind` (default `127.0.0.1:8548`).

- `GET /v1/frames?from_index=<i>&from_block=<b>&limit=<bytes>` returns the frames after the
  cursor. The headers `x-egress-after-*` give the cursor after the last frame, and the headers
  `x-egress-next-*` give the head of the store. A cursor below the floor of the store gets `410`,
  with the floor in the headers `x-egress-floor-*`.
- `POST /` with `eth_getTransactionReceipt` answers in the format of the state query of an
  executor: the rkyv bytes of the receipt as `0x` hex, and the header `x-state-block`. The egress
  keeps the newest `--receipts` receipts for this call. A dropped hash answers `null`.
- `POST /` with `kardamom_getTransactionStatus` answers the status of one hash:
  `{"status":"executed"}` when the egress holds the receipt, `{"status":"dropped","reason":...,
  "sender":...,"nonce":...}` when a sequencer dropped it, and `{"status":"unknown"}` otherwise.
  The egress keeps the newest `--drops` drops. It keeps the first report of a hash, so the drops
  of the two twins are one status. A receipt wins: a receipt forgets the drop of its hash, and a
  drop of a hash with a receipt changes nothing.
- `GET /health` answers `200 ok`.
- The metrics listener binds `--metrics-addr` (default `127.0.0.1:9013`).

### 10.5 The readers

| Reader | Switch | Reads |
|---|---|---|
| Validator | `--egress <url>` | the range read up to the head, then `egress_order` |
| Batcher | `--egress <url>` | the range read up to the head, then `egress_order` |
| Sequencer | `receipts_from_egress` | `egress_receipts`, with the account rows of every lane |
| Ingress | `--receipt-query-endpoints <url>` | `eth_getTransactionReceipt` of the egress |
| Notifier | `--receipts-from-egress` | `egress_receipts` |

Each switch is off by default. Without it, the reader keeps its old source.

### 10.6 Differences from section 6 (follow-ups)

- **No `egress_rows`.** Each sequencer reads all of `egress_receipts` and keeps the rows of its
  lanes. One rows stream for each lane is a follow-up.
- **The notifier stays a separate service.** It reads `egress_receipts` in place of the
  `tx_receipts` of the executors. The webhooks and `kardamom_subscribeTxStatus` stay in
  `crates/notifier`. The move into the egress process is a follow-up.
- **The prune** keeps a fixed number of blocks (section 10.3).
- **No drops on `egress_receipts`.** Its frames are receipt batches and rollbacks, told apart by
  length, so a drop has no clean place there. The WebSocket status feed and the webhooks do not
  show a drop yet. The ingress does not forward `kardamom_getTransactionStatus` yet. (Superseded
  by decision 46 (2026-10-25): the load balancer sends it to the egress. The ingress never
  forwards it.)
- **The deploy** of the egress and the switches of the readers come with P7c.

The differences from the amendment of 2026-10-25:

- **The receipts are in memory only.** The store keeps no receipt. The receipt read keeps the
  newest `--receipts` receipts. Decision 46 needs the durable receipt store (A2).
- **`exec_frames` is best effort.** An executor drops a frame when its publication is full
  (section 10.1). Decision 49 needs a reliable publication with `fc=min` (A6).
- **`egress_order` is best effort for the batcher.** Decision 49 needs a reliable,
  flow-controlled publication to the batcher (A6).
- **The readers keep the executor source by default.** The switches of section 10.5 are off by
  default. Decision 45 makes the egress the only source (A4).
- **The ingress forwards `eth_getTransactionReceipt`** to the egress
  (`--receipt-query-endpoints`). Decision 46 sends the read to the egress through the load
  balancer (A7).
- **The drops copy `tx_errors`.** Every rejection on `tx_errors` that names a hash also goes on
  `tx_drops` (section 10.1). Decision 50 removes `tx_errors`. Decisions 48 and 49 remove the
  reasons `DaLag`, `RecordLag` and `SealerRefusedReference`.
- **The default ack of the Nomad job is `receipt`**
  (`deploy/cluster/nomad/ingress.nomad.hcl:32-39`). Staging sets `validated`. Decision 47 keeps
  only `validated` (A5).

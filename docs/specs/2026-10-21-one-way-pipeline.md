# The one-way pipeline

- Status: decided by the user on 2026-10-21. Not implemented yet (section 9).
- Issues: refs #582, #585, #706, #709, #712.
- Amends `docs/specs/2026-10-10-raft-carried-transactions.md` (the carry spec). Section 16 of the
  carry spec lists these decisions as decisions 34 to 43. Where this spec and the carry spec
  differ, this spec wins.
- Amends the design comments of #585 (the serving service, called the egress here) and #706 (the
  sequencer gateway).

## 1. The principle

Each stage sends only to the next stage, and each stage sends the transaction data:

```text
client ─TLS─▶ LB ─HTTP─▶ ingress ─▶ sequencer ─▶ sealer ─▶ executor ─▶ egress ─▶ batcher
                         (one process)                                     ├──▶ validator
                                                                           ├──▶ clients (webhooks, WebSocket, receipt reads)
                                                                           └──▶ sequencers (account rows)
```

- Each stage reads only the stage in front of it. It reads no side stream and no archive of an
  earlier stage. So no stage joins a reference with its bytes, and no gap between a reference and
  its bytes can exist.
- The egress is the only output of the executors. The account rows to the sequencers are the one
  back edge. They carry account state, not transactions.
- TLS ends at the load balancer. The ingress has no TLS code. The answer to a request goes back on
  the connection that carried the request, so the process that took the request answers it.

## 2. Decisions of the user (2026-10-21)

| Id | Decision |
|---|---|
| D1 | `eth_sendRawTransaction` has the semantics of Ethereum. It returns the hash when the sequencer that owns the sender accepts the transaction into its mempool. It does not wait for the sealer or for the execution (section 3). |
| D2 | The ingress reads no receipts. Receipts go only to the egress. The strong confirmation goes through the egress: webhooks and the WebSocket status feed. `eth_sendRawTransactionSync` (EIP-7966) is not served. |
| D3 | The ingress runs inside the sequencer process (#706 step 4). The ingress fleet goes. |
| D4 | Each twin sequencer offers one cluster message for each transaction: the reference first, then the envelope. The sealer reads the reference, checks the window of decision version 4, and skips the bytes of a reference that it saw. Otherwise it orders the reference and its bytes (section 5). |
| D5 | The executors read the transaction bytes from the order. They do not join with `tx_data`. The void votes, the voter ids, `VoidLedger`, the archive refetch, the peer fetch (#543) and the `tx_data` recorders go. #709 cannot happen. |
| D6 | The executors send their output to the egress only: the order with its bytes, the receipts and the account rows (section 6). |
| D7 | The batcher and the validator read the egress. |
| D8 | The sequencers read the account rows from the egress. A delay of the rows is accepted: the client ack does not wait for them. |
| D9 | The validator independence holds without a stream hash. Each executor runs on the host of a sealer member and reads the order of that member. The egress gets one copy of the order from each executor and compares the copies, the transaction bytes included. |
| D10 | The feature id of the carry flag is 4, not 3. `FEATURE_SEALER_V4` took id 3 (#699). The carry flag needs decision version 4 active. |

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

## 3. The ack at admission (D1, D2)

### 3.1 The call

1. The ingress decodes the transaction, recovers the sender, and runs the static checks (size,
   chain id, gas limit, intrinsic gas).
2. It sends the envelope to the twins of the owner lane on `tx_data[lane]`, as today.
3. Each twin runs the admission of the v2 loop (`crates/sequencer/src/v2/mod.rs`). Each twin sends
   one ack for each transaction to the ingress that published it, on the `tx_verdicts` stream of
   that ingress (#707): `accepted`, or `rejected` with a reason.
4. The ingress answers the call on the first ack. It ignores the second ack of the other twin.
   A `rejected` ack answers only when both twins rejected, or after the reject timer, as in the
   #697 design. An `accepted` ack always wins.
5. Without an ack in the ack timeout (default 2 s), the call answers with the error
   `transaction not acknowledged`. The client can send the transaction again: a second copy of
   an accepted transaction gets `already known`.

### 3.2 What accepted means

- The sequencer took the transaction into its mempool: its pending list or its parked list.
- The checks of an Ethereum node passed: the signature (at the ingress), the nonce is not below
  the row nonce, the balance covers the worst-case cost with the pending entries, the fee cap
  covers the base fee, and the transaction fits the size and the gas limit.
- A transaction with a nonce gap is accepted and parked, as Ethereum queues it.
- An accepted transaction can still drop later, as in Ethereum: a parked entry expires, a
  forwarded entry is lost, or a lane loses both twins. The client sees it through the egress (no
  receipt, or a `rejected` status event).

### 3.3 The errors

The ack reasons map to the error texts of geth, so Ethereum tools understand them:

| Reason | Error text |
|---|---|
| The nonce is below the row nonce | `nonce too low` |
| The balance does not cover the worst-case cost | `insufficient funds for gas * price + value` |
| The same hash is in the pending list or the parked list | `already known` |
| The same sender and nonce, a fee bump below 10% | `replacement transaction underpriced` |
| The parked list of the sender, or of the lane, is full | `txpool is full` |
| The fee cap is below the base fee | `max fee per gas less than block base fee` |

Replace by fee: a transaction with the same sender and nonce as a parked entry, and a tip and a
fee cap that are each at least 10% higher, replaces that entry. A forwarded entry cannot be
replaced: the sealer may have ordered it.

### 3.4 What the ingress keeps and drops

- It keeps: the decode, the signature recovery, the static checks, the rate limits, the routing
  to the lane, the publication on `tx_data`, and the waiters for the acks.
- It drops: the receipt cache, the receipt watchers, the quorum watermark gate, the observer
  session, the account layer and the `tx_receipts` subscription.
- `eth_getTransactionReceipt` reads the egress. `eth_getBalance` and `eth_getTransactionCount`
  read the account rows of the sequencer process for its own senders, and the egress for the
  other senders.
- `kardamom_sendRawTransactionAsync` stays. It acks after the publication on `tx_data`, without the
  admission.
- The ack mode is a configuration switch of the ingress (`--ack admission`). The old mode stays
  until every client moved. The load tools and the canary measure the executed latency through
  the egress after the switch.

## 4. The ingress inside the sequencer (D3)

- The sequencer binary runs the ingress service on its own runtime. The JSON-RPC port and the
  Consul service `ingress-jsonrpc` move to the sequencer job. The load balancer targets the
  sequencer hosts.
- The load balancer cannot route by sender. So the ingress of any sequencer process takes any
  transaction, and publishes it on `tx_data[lane]` of the owner lane. When the process owns the
  lane, the publication is to its own twin partner and to itself.
- The ingress of a process gets its acks from the twins of each lane on its `tx_verdicts` stream.
  An ack is a few bytes.
- `tx_data` stays as the forward from the process that took the request to the twins of the owner
  lane. Both twins read the same image, so they give the same reference (lane, session,
  position). Nothing records `tx_data`.
- The `ingress` Nomad job goes after every network runs the merged job.

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
  client.
- **It scales out.** Independent instances, one per host, a minimum of 2 (decision 27 of the carry
  spec).

### 6.3 The readers

| Reader | Reads after the switch |
|---|---|
| Batcher | `egress_order`, and the range read when it resumes. It packs the KAR1 blocks from the bytes of each `RT_TX`. It does not join and does not rebuild from `tx_data` archives. |
| Validator | `egress_order`, and the range read when it resumes. It checks the hash and the signature of each transaction. |
| Sequencer | `egress_rows` of its lanes, in place of the account rows of `tx_receipts`. |
| Ingress | `eth_getTransactionReceipt` reads the egress. |
| Clients | Webhooks and `kardamom_subscribeTxStatus` of the egress. |

- A reader keeps its old source until the carry flag is active on every network.
- The egress is on the path of the chain now: the batcher and the sequencers stop when every
  instance stops. When the egress stops, the account tables of the sequencers stop moving, the
  pending lists fill, and admission refuses new transactions of those senders. No forwarded entry
  is called lost, because the loss rule counts blocks of rows.

## 7. Upgrade and rollout

- Every change is backwards compatible. A breaking change ships behind a chain feature flag that
  is off, and an L1 upgrade transaction turns it on (the upgrade policy, #661).
- D1 and D3 do not change a chain rule. They are configuration switches of the sequencer and the
  ingress.
- D4 and D5 change the order. They activate with the carry flag (feature id 4, D10).
- D6 to D8 change the readers. Each reader keeps its old source until the carry flag is active on
  every network.

## 8. Throughput

- Both twins offer each transaction with its bytes. The Raft log holds both copies before the
  window drops one. With about 200 bytes a transaction, the log takes about 4 MB/s at 10,000 tx/s
  and about 400 MB/s at 1,000,000 tx/s. The leader sends that to two followers.
- Aeron 1.44.0 has no hook in the consensus module that drops a message before the log append.
- The 1,000,000 tx/s goal needs a later change: for example, only one twin offers the bytes, and
  the other offers the reference alone.

## 9. Implementation plan

Each item is one pull request. Each passes `just style` and CI alone, and follows `docs/STYLE.md`.

| PR | Title | Depends on |
|---|---|---|
| P1 | sequencer, ingress: the ack at admission, the error texts of geth, replace by fee (D1) | #707 |
| P2 | wire: record type 7, the full reference, Java and Rust codecs on shared golden bytes | none |
| P3 | sealer: decision version 5, the carry flag (feature 4), snapshot 16 (D4, D10) | P2 |
| P4 | engine: the cluster reader yields `Ordered::Tx` for `RT_TX` (section 5.4) | P2 |
| P5 | sequencer: offer record type 7 after kind 17 reports version 5, with the byte bounds of the carry spec (section 6.2 of that spec) | P2, P3 |
| P6 | executor: publish `exec_frames` (section 6.1) | P4 |
| P7 | egress: the service: compare, store, `egress_order`, the range read, `egress_rows`, the receipt read (section 6.2) | P6 |
| P8 | egress: the client side of the notifier moves into the egress | P7 |
| P9 | batcher and validator: read the egress | P7 |
| P10 | sequencer: read `egress_rows`; ingress: read the receipts from the egress | P7, P1 |
| P11 | sequencer: the ingress runs inside the sequencer process; deploy: one job (D3) | P1 |
| P12 | release: the upgrade transaction of feature 4 on staging, after P3 to P10 deploy | P3 to P10 |
| P13 | cleanup: record types 0 and 4, the join, the void, the `tx_data` recorders, the ingress job (#599) | P12 on every network |

The open items of the carry spec that this plan does not need for staging: the member feed from
the Raft log (#638), the stored cursor (#639), the recovery after a power loss (#644, #645), the
history key (#675). They stay open in their issues.

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
  show a drop yet. The ingress does not forward `kardamom_getTransactionStatus` yet.
- **The deploy** of the egress and the switches of the readers come with P7c.

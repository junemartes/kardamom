# Void an entry that no consumer can execute

Date: 2026-09-21. Issue: #313 (a). Status: design, for review before code.

## 1. Problem

The sealer orders a `TxRef` when a sequencer offers it. The ingress archives record the
transaction data on a different path, with no link to the order. So an entry can be in the
canonical order before its data is durable.

An archive wipe shows the result (issue #376, the `archive-tx-data-wipe` case). The Raft log
keeps the entry. No archive keeps the bytes of `tx_data`. Each consumer stops at that entry,
gets a join timeout, and exits. The chain does not move again.

Note (2026-09-23): the `pipeline-blackout-recover` case was the first example of this design.
Its join timeouts had a different cause: a bounded replay in the refetch (#422). With that fix
the blackout loses no data, and the case passes with zero void decisions. The void rule is for a
real loss of data only.

The owner decision (2026-09-21): do not make the order wait for the archives. The order path is
latency sensitive. Remove the entry that no consumer can execute.

## 2. Why a local skip is wrong

The smallest change is one line: on a join timeout, skip the entry. This change is not safe.

- Replica A has the envelope and executes the entry. Replica B has a slow archive and skips it.
- The two state roots are different from that block on.
- The batcher can post the transaction to L1 while the executors skip it.

A consumer must never drop an entry on its own clock. The removal must be a record in the
canonical order. Then every consumer sees the same decision at the same index.

## 3. Design

One decider, many followers. The sealer is the only component that gives an order, so the sealer
makes the decision. The sealer does not read an archive. It counts requests.

### 3.1 Steps

1. A consumer gets no envelope for entry `i` in the join budget.
2. The consumer asks every configured archive. PR #412 tells "this archive does not have the
   range" apart from "this archive did not answer". Only the first answer counts as a refusal.
3. When every archive refuses, the consumer sends `VOID_REQUEST(i, tx_hash, voter_id)` to the
   sealer on its cluster session. It sends the request again at an interval until step 6.
4. The sealer keeps the requests for `i` in its canonical state. The state is in the snapshot.
   The sealer refuses a request when `i` is not in its void table (section 3.4), when the hash
   does not agree with the table, or when `i` already has a void record.
5. When every configured voter has a request for `i`, the sealer appends a `Void(i, tx_hash)`
   record to the canonical order. The record takes one canonical slot, as an epoch record does.
6. Each consumer reads ahead in the order stream while it waits at `i` (section 3.5). When it
   finds `Void(i)`, it drops entry `i` and continues. When the envelope arrives first, it
   executes the entry as usual.
7. A consumer that finds `Void(i)` for an entry that it executed exits with an error. This
   state is a divergence. It must not be silent.

### 3.2 Voters

The voters are the consumers that make durable results from the entry:

- each executor,
- the validator,
- the batcher.

The batcher must be a voter. It posts the transaction bytes to L1. If it has the envelope, it
never sends a request, so no void occurs, and L1 and the executors agree.

The live batcher uses the engine join path (`crates/batcher/src/live/run.rs`, the same
`bounded_join_timeout`). So it stops at a lost entry as an executor does, and it is a natural
voter. The offline `multi_archive_reader.rs` returns an error for a missing envelope. It must
learn to drop an entry that has a void record.

The voter set must equal the set of consumers that execute. This is a deployment invariant,
not a tuning value. A consumer that is not in the set cannot stop a void. If it holds the
envelope, it executes the entry, then reads the void record and stops with
`VoidOfExecutedEntry`. The stop is correct, but the consumer is then lost until a resync.

The voter set is sealer configuration (`kardamom.cluster.voidVoters`). A cluster session has no
identity today, so the request carries the `voter_id`. The sealer does not authenticate the id.
One process on the cluster network can send every id. This is the trust level of a sequencer
session today: the cluster network is the boundary.

### 3.3 What a void does to the chain

- The transaction is in no block. It has no receipt. Its nonce stays free.
- The DA payload needs no change. A block frame carries transaction bytes and the cursor
  `end_tx_idx`. Epoch and deposit records already take slots that never reach the blob. A void
  slot is the same. `kardamom-reconstruct` and the prover read the blob, so they need no change.
- The sealer must set the expected nonce of the sender back to the nonce of the void entry. If
  it does not, the sealer refuses the new submit with a contiguity reject.
- The sender submits the same signed bytes again, so the hash is the same. One dedup window
  uses the hash as the key: `dedup` in `CanonicalSealerState`. The sealer is the only dedup
  point (#427 removed the engine reader's window). A void must remove the hash from the sealer
  window. If it does not, the sealer drops the new submit as a duplicate.
- Later entries of the same sender that are already in the order fail the nonce check at
  execution. This is the behavior for a nonce gap today. Their hashes stay in the sealer
  window, so the sender cannot submit the same bytes again while the window holds them.
  Section 6 has the question.
- The ingress must tell the sender. Section 6 has the question.

### 3.4 The void table in the sealer

A `Relayed` record is `(index, payload)`. The sealer never reads the payload. It knows the
sender and the nonce only when it orders the ingress record. `expectedNonce` is an LRU map by
sender, not by index. The request cannot supply the sender or the nonce, because the consumer
has no envelope.

So the canonical state gets a bounded table: `index -> (canonical id, sender, nonce)`. The
sealer adds a row for each ingress record (`KIND_INGRESS_RECORD`). Epoch, deposit and remote
epoch records get no row, so a request for such a slot is refused. The bound is the egress
retention. A request for an index below the table is refused, as a replay below the retention
is refused today. The table is a snapshot field.

### 3.5 The read-ahead in the reader

This is the largest code change. Today the reader thread handles one order message at a time
and blocks in `JoinWait`. The new reader does this at a lost entry `i`:

- It parks entry `i`. It sends no later entry to the executor. The order is total, so no entry
  of another sender can pass entry `i`.
- It keeps reading the order stream into a bounded queue. It looks only for `Void(i)`.
- On `Void(i)` it drops entry `i` and drains the queue in order.
- It does not look for the envelope again during the wait. The wait starts only after every
  archive refused the range, and no publisher sends an old envelope again.
- The read-ahead cannot lose the egress position. The session thread of the client drains the
  egress into a channel with no bound, so a parked reader never causes `REPLAY_UNAVAILABLE`.
  The bound of the queue is a memory bound only: 65536 messages, about 5 MB. The number is the
  void window of the sealer: after that many records the sealer refuses the vote, so no void
  record can come. When the queue is full, or after `void_wait` (120 s), the reader exits as
  it does today on a join timeout. The sealer keeps the vote, so the restart loses nothing.
- The reader sends the vote again each 5 s, by the clock. Each vote is one entry in the log of
  the sealer, so the interval is not shorter. The reader reads the clock at each message. The
  sealer emits a boundary on a timer also with no traffic, so the interval holds on an idle
  chain. A boundary count is not the clock: the tick is 250 ms in production and 2000 ms in the
  container profile.

Two rules complete the follower:

- A dropped entry and its void record each take one canonical slot. The executor counts every
  slot (`expected_tx_idx`) and compares the count with `end_tx_idx` at each block boundary. So
  the reader sends a new message with no transaction, `ReaderToExec::Vacant`, for each of the
  two slots. The epoch marker is the template. The batcher feed ignores the message.
- A void record for an index below the resume cursor of the consumer is ignored. The
  checkpoint of the consumer is already past that entry, and the reader cannot know whether the
  checkpoint has it. Step 7 of section 3.1 applies only to an entry that this process passed.

The wire form of the record is `[tx_hash:32][record_type = 4][index:u64]`. The field that holds
the canonical id in other records holds the hash of the removed transaction. The sealer makes
the record itself. The relayed payload is a plain byte layout, not rkyv, so the Java service
can write it.

A consumer that replays the order from before `i` finds `Void(i)` in the queue in milliseconds,
because the replay frames arrive fast. So the replay path needs no wire change.

### 3.6 Coherence property

For each canonical index `i`, all consumers take the same action. Either all execute entry `i`,
or all drop it. The proof: a consumer drops `i` only when it reads `Void(i)`. `Void(i)` is a
canonical record. The sealer appends it only when no voter has executed `i`, because each voter
said so itself. A consumer that starts later replays the same order and reads the same record.

### 3.7 Cost on the hot path

Zero. The sealer orders a `TxRef` as it does today. The new code runs only after a join budget
ends, which means the chain is already stopped at that entry.

The cost is on the failure path. The chain waits at entry `i` for: the join budget, plus one
refetch round for each archive, plus the time for the slowest voter to send its request. The
join budget sets the floor: `bounded_join_timeout` gives 30 s on a resume and 60 s on a fresh
start. So a lost entry stops the chain for more than 30 s today. Section 6 asks for the target.

## 4. Touched sites

| # | Site | Change |
|---|------|--------|
| 1 | `crates/types/src/tx_ordering.rs` | new variant `Void`; the variant macro forces each match site |
| 2 | `crates/cluster-adapter/src/wire` | `RT_VOID = 4`, `KIND_VOID_REQUEST = 6`, encoders, decoders |
| 3 | `SealerWire.java`, `SealerClusteredService.java`, `CanonicalSealerState.java` | request table, void table, voter set, append the record, nonce reset, dedup removal, two snapshot fields |
| 4 | `crates/engine/src/reader` (`threads.rs`, `join.rs`, `cluster/mod.rs`) | send the request, read-ahead queue, drop on void, error on a void of an executed entry |
| 5 | `crates/batcher/src` (`live/run.rs`, `multi_archive_reader.rs`) | the live batcher votes through the engine reader; the offline reader drops an entry that has a void record |
| 6 | `crates/sequencer/src/outbound/cluster.rs` | decode the new variant (no action) |
| 7 | `docs/failure-modes.md` | the void rule, the voter set, the two new constants |
| 8 | `crates/chaos/src/shard.rs` | schedule `pipeline-blackout-recover` (38 to 39 cases) |

The validator uses the engine reader, so site 4 covers it. No change: the DA frame,
`kardamom-reconstruct`, the guest, the `Receipt` type.

Wire changes: one ingress kind, one record type. The count of the match sites of
`TxOrderingMessage` outside the tests is 23 lines in 6 files, plus 4 lines in 2 test kits.

## 5. Failure cases

- **A voter is down.** No void occurs until it comes back. The chain waits. This is the safe
  side: the voter that is down can be the one that executed the entry. An operator can remove
  a dead voter from the set.
- **An archive is slow, not empty.** The archive gives no refusal, so the consumer sends no
  request. The consumer keeps the behavior of today (exit and restart on a join timeout).
- **A bad voter asks for a void of good data.** The other voters have the envelope and send no
  request. No void occurs.
- **The sealer restarts during a vote.** The request table is in the snapshot and in the log.
  Consumers send the request again at an interval, so a lost request is not a lost vote.
- **One live voter has the envelope in memory, and every archive lost it.** That voter sends no
  request, so no void occurs. The other voters cannot get the bytes, so the chain waits with no
  exit. The fix is a peer envelope fetch: the voter that has the bytes serves them. It is a
  follow-up and is not in this design. An archive wipe does not need it, because there no
  process has the bytes in memory after the restart.
- **The entry leaves the void window before the last vote.** The window is the newest 65536
  indices. Sequencers keep ordering while the consumers wait. With live ingress at more than
  about 1000 records per second and a 60 s join budget, entry `i` leaves the window before the
  last vote, the sealer refuses the vote, and the chain stops as it does today. A wider window
  is not the fix: each entry is 68 bytes in every snapshot (65536 entries are about 4.4 MB).
  The fix is a shorter join budget on this path. See question 4.
- **The ingress still has the transaction.** The chain drops the entry. The sender submits it
  again. A republish by the ingress is a possible later step and is not in this design.

- **An archive leaves the discovered membership.** With discovered archive membership, an
  archive that is down can leave the list. The remaining archives then all refuse, and the
  consumer votes. The void is consistent on every replica, but the data possibly still exists
  on the archive that is down. A fixed archive list does not have this case: an archive that
  does not answer is not a refusal.

## 6. Questions for the owner

1. **Notice to the sender: a deviation, please confirm.** Option 2 as first given said "a
   failed receipt". Option B, a failed receipt in the block, changes the `Receipt` type, the DA
   frame, reconstruct and the guest (13 files read `status`), and it uses the nonce. This design
   proposes option A: the executor puts a `Voided(tx_hash)` notice on the receipt stream, and
   the ingress answers the hash with a "dropped, submit again" error. The limit of option A:
   after a blackout the ingress lost its in-process receipt cache (#337), so the sender gets
   "unknown" and a timeout, as today.
2. **All voters, or a quorum.** This design needs all voters. A quorum is faster when a voter
   is down, but a voter that executed the entry and is down then diverges.
3. **Later entries of the sender.** After a void of nonce 5, the entries with nonce 6 and 7 fail
   at execution, and their hashes stay in the sealer dedup window. Option A: leave it, the
   sender signs again after the window passes. Option B: the void also removes the later hashes
   of that sender from the window. Option B needs a sender index in the void table. This design
   uses option A.
4. **Stall target.** How long can the chain wait at a lost entry? The answer sets the join
   budget on this path. It also decides whether a void can occur with live ingress: the last
   vote must arrive before 65536 more records are ordered. Two costs depend on the answer:
   - The reader waits at one lost entry at a time. A blackout that loses N entries costs N
     join budgets in sequence. A possible rule: after the first refusal for a publisher
     session that ended, the reader skips the wait for the later entries of that session.
   - A consumer that restarts after a void meets `TxRef(i)` again. It uses one full join budget
     before it finds `Void(i)` in the replay.

## 7. Pull request plan (one stack)

1. Types and wire: the variant, the record type, the ingress kind. Rust and Java, with tests.
2. Sealer: the request table, the voter set, the record, the nonce reset, the snapshot.
3. Engine reader: the request, the read-ahead, the follower rule. Covers executor and validator.
4. Batcher: the follower rule and the vote.
5. Sender notice (option A) and `docs/failure-modes.md`.
6. Chaos: schedule `pipeline-blackout-recover`. Add a case in which one replica misses data
   that its peers have, and assert that no void occurs.

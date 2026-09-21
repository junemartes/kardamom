# Void an entry that no consumer can execute

Date: 2026-09-21. Issue: #313 (a). Status: design, for review before code.

## 1. Problem

The sealer orders a `TxRef` when a sequencer offers it. The ingress archives record the
transaction data on a different path, with no link to the order. So an entry can be in the
canonical order before its data is durable.

The `pipeline-blackout-recover` chaos case shows the result. The case kills every pipeline node
at the same time. The Raft log keeps the entry. No archive keeps the last bytes of `tx_data`.
After the restart each consumer stops at that entry, gets a join timeout, and exits. The chain
does not move again.

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
5. When every configured voter has a request for `i`, the sealer appends a `Void(i, tx_hash)`
   record to the canonical order. The record takes one canonical slot, as an epoch record does.
6. Each consumer reads ahead in the order stream while it waits at `i`. When it finds
   `Void(i)`, it drops entry `i` and continues. When the envelope arrives first, it executes the
   entry as usual.
7. A consumer that finds `Void(i)` for an entry that it executed exits with an error. This
   state is a divergence. It must not be silent.

### 3.2 Voters

The voters are the consumers that make durable results from the entry:

- each executor,
- the validator,
- the batcher.

The batcher must be a voter. It posts the transaction bytes to L1. If it has the envelope, it
never sends a request, so no void occurs, and L1 and the executors agree.

The voter set is sealer configuration (`kardamom.sealer.void.voters`). A cluster session has no
identity today, so the request carries the `voter_id`.

### 3.3 What a void does to the chain

- The transaction is in no block. It has no receipt. Its nonce stays free.
- The DA payload needs no change. A block frame carries transaction bytes and the cursor
  `end_tx_idx`. Epoch and deposit records already take slots that never reach the blob. A void
  slot is the same. `kardamom-reconstruct` and the prover read the blob, so they need no change.
- The sealer must set the expected nonce of the sender back to the nonce of the void entry. If
  it does not, the sealer refuses the new submit with a contiguity reject.
- Later entries of the same sender that are already in the order fail the nonce check at
  execution. This is the behavior for a nonce gap today.
- The ingress must tell the sender. Section 6 has the question.

### 3.4 Coherence property

For each canonical index `i`, all consumers take the same action. Either all execute entry `i`,
or all drop it. The proof: a consumer drops `i` only when it reads `Void(i)`. `Void(i)` is a
canonical record. The sealer appends it only when no voter has executed `i`, because each voter
said so itself. A consumer that starts later replays the same order and reads the same record.

### 3.5 Cost on the hot path

Zero. The sealer orders a `TxRef` as it does today. The new code runs only after a join budget
ends, which means the chain is already stopped at that entry.

The cost is on the failure path. The chain waits at entry `i` for: the join budget, plus one
refetch round for each archive, plus the time for the slowest voter to send its request. The
join budget in the cluster profile sets the floor. Section 6 asks for the target.

## 4. Touched sites

| # | Site | Change |
|---|------|--------|
| 1 | `crates/types/src/tx_ordering.rs` | new variant `Void`; the variant macro forces each match site |
| 2 | `crates/cluster-adapter/src/wire` | `RT_VOID = 4`, `KIND_VOID_REQUEST = 6`, encoders, decoders |
| 3 | `SealerWire.java`, `SealerClusteredService.java`, `CanonicalSealerState.java` | request table, voter set, append the record, nonce reset, snapshot field |
| 4 | `crates/engine/src/reader` (`threads.rs`, `join.rs`, `cluster/mod.rs`) | send the request, read ahead, drop on void, error on a void of an executed entry |
| 5 | `crates/batcher/src/multi_archive_reader.rs` | the same follower rule, and the batcher votes |
| 6 | `crates/sequencer/src/outbound/cluster.rs` | decode the new variant (no action) |
| 7 | `docs/failure-modes.md` | the void rule, the voter set, the two new constants |
| 8 | `crates/chaos/src/shard.rs` | schedule `pipeline-blackout-recover` (38 to 39 cases) |

The validator uses the engine reader, so site 4 covers it. No change: the DA frame,
`kardamom-reconstruct`, the guest, the `Receipt` type.

Wire changes: one ingress kind, one record type. The count of the match sites of
`TxOrderingMessage` outside the tests is 25 lines in 8 files.

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
- **The ingress still has the transaction.** The chain drops the entry. The sender submits it
  again. A republish by the ingress is a possible later step and is not in this design.

## 6. Questions for the owner

1. **Notice to the sender.** Option A: the executor puts a `Voided(tx_hash)` notice on the
   receipt stream, and the ingress answers the hash with a "dropped, submit again" error.
   Option B: a failed receipt in the block. Option B changes the `Receipt` type, the DA frame,
   reconstruct and the guest (13 files read `status`). This design uses option A.
2. **All voters, or a quorum.** This design needs all voters. A quorum is faster when a voter
   is down, but a voter that executed the entry and is down then diverges.
3. **Stall target.** How long can the chain wait at a lost entry? The answer sets the join
   budget on this path.

## 7. Pull request plan (one stack)

1. Types and wire: the variant, the record type, the ingress kind. Rust and Java, with tests.
2. Sealer: the request table, the voter set, the record, the nonce reset, the snapshot.
3. Engine reader: the request, the read-ahead, the follower rule. Covers executor and validator.
4. Batcher: the follower rule and the vote.
5. Sender notice (option A) and `docs/failure-modes.md`.
6. Chaos: schedule `pipeline-blackout-recover`. Add a case in which one replica misses data
   that its peers have, and assert that no void occurs.

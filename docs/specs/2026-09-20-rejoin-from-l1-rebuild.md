# Rejoin the pipeline from a state rebuilt from L1

Status: executor half, sealer half and deposits built.

## 1. Problem

`kardamom-reconstruct` rebuilds the L2 state from L1 and the DA store alone, and the chaos
suite proves its state root against the validator's at the end of every shard. That proves
the state is derivable from the chain. It does not give a way back into the running
pipeline:

- All executors lose their state DB and their checkpoints. No peer holds a checkpoint, and
  a replay from genesis is refused once the chain outgrew the sealer's retention window.
- All sealers lose their cluster and archive directories.

## 2. What the resume cursor is

An executor, the validator and the batcher resume from a pair: a record index and a block
number. The index is `last_fsynced_b_position`, the sealer's `canonicalCount` at the last
committed boundary (an exclusive end), and the block is `last_committed_block + 1`. The
sealer serves records with `index >= from_index` and boundaries with `block >= from_block`.

The index counts canonical records, not transactions. Two record kinds take canonical
slots and never reach the DA payload: the epoch marker, one per finalized L1 block even
when empty, and each deposit inside it. Remote-epoch markers and their messages take slots
too, and those do travel in the payload.

Before this change the rebuild wrote a synthetic cursor: a count of the items it replayed.
That value is always too low, and nothing rejected it. The executor seeds its alignment
counter and its reader index from the same cursor, so both sides advance together whatever
the start was. A cursor below the truth applies records twice. A cursor above it skips
records. Neither fires a detector.

## 3. Executor half (built)

Options, with touched sites:

| Option | Wire change | Touched sites | Verdict |
|---|---|---|---|
| 1. The rebuild derives the cursor | needs the L1 origin in the payload first | 3 | An empty epoch takes a slot and changes no state, so a miscount passes the root check. Rejected. |
| 2. The batcher carries the block's end index | KAR1 version 2 to 3, off-chain only | 4 | **Chosen.** |
| 3. The executor asks the sealer | new request kind and new egress frame | 5 to 6, two languages | Does not help when the sealer is the lost role. Rejected, except its cheap half below. |
| 4. Install the rebuilt DB as a checkpoint | none | 0 | Solves packaging only; the cursor still comes from the image. Rejected. |

**The format.** A KAR1 version 3 block header carries a `BlockCursor` after the timestamp:
`end_tx_idx` (u64) and `l1_origin` (u64). The batcher already holds both on the closed
block and dropped them at one line. Version 2 blobs stay on L1 for good, so the decoder
keeps reading them; their blocks have no cursor. A payload is one version. `postBatch`
never parses the blob, the records commitment is computed before framing, and the zk guest
does not link the framing code, so no contract, prover or guest changes. The price: L1 does
not authenticate the field.

**The rebuild.** Replay anchors a block's items to the payload's end index. Any epoch
closes the open block before the sealer relays it, so the items the payload carries are the
tail of the block's index range, and the slots before them belong to epoch markers and
deposits. So the rebuilt cursor, the header rows (end index, timestamp, L1 origin) and every
receipt position equal the live chain's. A block whose end leaves no room for its own items
after the previous end is refused. `--executor-image` then removes the trie, the hashed
mirror and the stored root after the root check passed: an executor writes with the trie
off, so all three would go stale at its first block, fail the integrity sweep, and mislead
a validator that adopts the image as a checkpoint.

**The coherence property.** The executor resumes with `next_index = E_H` and
`next_block = H + 1`, where `E_H` is the sealer's `canonicalCount` at boundary H. It applies
exactly the records with index at or above `E_H`, and the rebuilt state holds every record
below it. Nothing is skipped and nothing is applied twice, *if* `E_H` is right. The sealer
makes that a checked condition: it refuses a replay request whose index lies outside the
block it names, against its own retained boundaries. So a wrong cursor, from a wrong DA
field or from any other source, is a loud `REPLAY_UNAVAILABLE` and not a silent divergence.

**Limits.** A rebuild through a block of a version 2 blob gives a correct state with no
cursor; the tool says so and refuses `--executor-image`. A mixed history is fine: the cursor
comes from block H's own frame. A block with a vacant slot (a voided entry or its void
record) rebuilds its items at the tail of its range, so their receipt positions can differ
from the live chain's; the root does not.

## 4. Sealer half (built)

A sealer cluster that lost all its state starts after block H from a seed file. This
replaces the flag day at a new genesis.

**Procedure.**

1. Rebuild the state at H from L1 with `kardamom-reconstruct --sealer-seed <file>`. H is
   the last posted block, or `--through-block`. The tool refuses a head that it did not
   rebuild from L1, and a head from a version 2 payload.
2. Give every member the same file in `-Dkardamom.cluster.seedSnapshot=<file>`. Start the
   members as a new cluster, with empty cluster and archive directories.
3. Install the rebuilt state on every executor and the validator. A consumer
   at the rebuilt head resumes at `(E_H, H + 1)`. A consumer that keeps the
   state of the old stream lies past the new head and must not resume on it.
4. Remove every other copy of the old stream: the checkpoints, the batcher's
   spool, and the account cache.
5. Start the sequencers, then the da-watcher with `--l1-resume-after M`,
   where M is the L1 origin of H. A watcher that starts at the finalized tip
   loses the epochs between M and the tip. A sequencer reads the epochs live,
   so an epoch published before the sequencers subscribe is lost too.

`docs/runbooks/sealer-fleet-rebuild.md` gives the commands.

**The seed file** (version 1, big-endian) holds the chain id, H, E_H, the timestamp and
the L1 origin of H, the state root, and the senders with their next nonces.
`crates/reconstruct/src/seed.rs` and `SealerSeed.java` hold the layout. The digest is the
SHA-256 of the file. The sealer logs it in `sealer state SEEDED`.

**The seeded state.** The next block is H + 1, and the next index is E_H. The open block
is empty. The posted head is H, so the DA-lag guard does not halt the chain at once. The
dedup window and the void ledger start empty. The nonce guard starts with the senders of
the rebuilt blocks, in the order they last sent, each at the nonce of its rebuilt
account. A guard with a smaller capacity keeps the most recent senders. A sender that the
guard does not hold starts at any nonce, as on the live chain. A member with a non-empty
remote-origin allowlist refuses a seed, because the seed holds no peer anchor.

**The seed record.** While the seed is not confirmed, every member offers a seed record
with the digest on each new leadership term. The first record in the log confirms the
seed, and the leader then asks for a snapshot. A member that started at genesis, or from
another seed, stops at the record, so a blank member that replays the log from 0 without
the seed fails loudly. After the snapshot, a blank member restores the seeded state from
a peer's snapshot and never replays the record. Before the snapshot, a blank member needs
the seed file.

## 5. Deposits (built)

Deposits ride inside an epoch record, and the batcher skips epochs by design: a deposit is
unsigned, so a blob-carried deposit would be an unverifiable claim. L1 fixes a deposit's
identity and its order inside its epoch, and the block's `l1_origin`, which version 3
carries, fixes its L2 placement.

**The derivation.** `kardamom-reconstruct --lockbox <addr>` walks the blocks in order. When
a block's origin moves from M to N, the block leads with the epochs M+1..N. Each epoch is
`derive_epoch` over its L1 block's hash and lockbox logs: the rule the da-watcher and the
validator use. The first step from origin 0 takes epoch N only, because the da-watcher
starts at the finalized block it first sees. An origin below the one before is refused. A
version 2 block carries no origin, leads with no epoch, and leaves the origin unchanged.

**The replay.** Each epoch takes its marker slot, then each deposit takes a slot and runs
through `execute_deposit_tx` (mint first, nonce check off, gas price 0), before remote
epochs and transactions. The sealer forces a boundary before any epoch when the open block
holds a record, so an epoch leads its block and a live block holds at most one epoch. A
unit test drives the live exec thread and the replay over the same stream, both through
the real writer with the trie on, and requires the same root and receipt positions.

**The slot check.** A block whose items (epoch markers, deposits, remote records,
transactions) need more slots than `end_tx_idx` minus the previous end is refused. Equality
is not required: a voided entry and its void record take slots, apply nothing, and never
reach the payload. So a missing deposit passes the slot check and fails the root check.
Without `--lockbox` the rebuild leaves deposits out. The chaos suite never deposits and
does not pass the flag.

## 6. Proof

- Unit: the version 3 round trip, the version 2 decode, the mixed-version refusal; the live
  positions and the same root with and without the field; the refused cursor; the stripped
  image; the sealer's cursor check.
- Unit: the live exec thread and the replay give the same root and deposit positions; a
  block whose epochs need more slots than its range holds is refused; the origin steps.
- `reconstruct_l1_e2e`: the state rebuilt from anvil and the DA store carries the cursor,
  and a lockbox deposit on anvil is derived and applied.
- Chain semantics S8: a workload with a deposit rebuilds to the validator's root.
- Chaos, every shard: the rebuilt cursor and header equal the validator's at the same block.
- Chaos, `executor-fleet-total-wipe-recover`: all three executors lose state and
  checkpoints, the harness rebuilds an executor image from L1 on the host, installs it on
  each node, and the executors resume from it with no checkpoint restore; the recovery
  probe and the end-of-shard persisted-state audit then prove the result.
- Chaos, `sealer-fleet-total-wipe-recover`: all three sealer members lose their
  directories. The members start from the seed at the posted head, the executors and the
  validator resume on the rebuilt state at `(E_H, H + 1)`, an executor that keeps the old
  chain gets `REPLAY_AHEAD`, and the da-watcher resumes after the seed's L1 origin.

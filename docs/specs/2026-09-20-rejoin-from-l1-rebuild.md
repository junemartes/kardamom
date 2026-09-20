# Rejoin the pipeline from a state rebuilt from L1

Status: executor half in implementation. Sealer half and deposits: designed, not built.

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
comes from block H's own frame. If an L1 epoch and a remote epoch both lead the same block,
the remote messages' rebuilt positions assume the L1 epoch came first.

## 4. Sealer half (designed, not built)

No continuity path exists today. A fresh cluster starts hard-coded at `canonicalCount = 0`
and `blockNumber = 1`, and no config key, flag or file seeds it. Three surviving components
hold values that only move up: the batcher cursor (it would re-post block numbers L1
already covers), the ingress durability watermark (its on-quorum gate becomes a no-op), and
the sequencer floors (every earlier sender dead-ends).

**The only safe procedure today is a flag day at a new genesis:** rebuild the state at
block H from L1, publish it as the new genesis allocation, deploy a new settlement, and
start every role with empty volumes, the batcher's cursor file included.

**The smallest product change for continuity** is a seed hook in three files: a
`kardamom.cluster.seedSnapshot` key in `ClusterNode.java`, read in the fresh branch of
`SealerClusteredService.onStart`, which calls the existing `CanonicalSealerState.load`. The
snapshot format already carries every field. The seed needs `canonicalCount = E_H`,
`blockNumber = H + 1`, `lastBoundaryCount = E_H`, `lastL2Timestamp`, and `l1Origin`. With
version 3 blobs the rebuilt DB holds all five. The per-sender nonces and the dedup window
may start empty. Every surviving consumer must still restart. This hook is the next step,
after the executor half is proven in the chaos suite.

## 5. Deposits (not started)

Deposits ride inside an epoch record, and the batcher skips epochs by design: a deposit is
unsigned, so a blob-carried deposit would be an unverifiable claim. L1 fixes a deposit's
identity and its order inside its epoch, and the block's `l1_origin`, which version 3 now
carries, fixes its L2 placement. Interleaving them in the rebuild is phase E of
`docs/agents/l1-origin-deposit-derivation-spec.md`. Until then a deposit-bearing range
rebuilds its non-deposit state only. The chaos suite never deposits.

## 6. Proof

- Unit: the version 3 round trip, the version 2 decode, the mixed-version refusal; the live
  positions and the same root with and without the field; the refused cursor; the stripped
  image; the sealer's cursor check.
- `reconstruct_l1_e2e`: the state rebuilt from anvil and the DA store carries the cursor.
- Chaos, every shard: the rebuilt cursor and header equal the validator's at the same block.
- Chaos, `executor-fleet-total-wipe-recover`: all three executors lose state and
  checkpoints, the harness rebuilds an executor image from L1 on the host, installs it on
  each node, and the executors resume from it with no checkpoint restore; the recovery
  probe and the end-of-shard persisted-state audit then prove the result.

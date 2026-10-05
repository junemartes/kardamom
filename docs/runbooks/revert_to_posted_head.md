# revert_to_posted_head

## Cause

The blocks after the last block posted to L1 are lost: the sealer's retention
passed them, the spool and every block payload store hold nothing, and no copy
survives. The chain reverts to the posted head.

This is the last resort. It revokes every receipt in the reverted range: each
transaction there was confirmed to its sender and is undone. Run it only after
`replay_unavailable.md` found no surviving copy, and with the operator's word.

## Confirm

1. The batcher is halted with `replay_unavailable`, and the steps of that
   runbook found no copy of the range.
2. Read the posted head from the settlement contract: `lastBatchIndex` and the
   `l2BlockEnd` of that batch. Call it `H`.
3. Read the sealed head from an executor: `kardamom_executor_block_number`.
   The range `H + 1 ..= sealed head` is the range the revert discards.

## Steps

1. Stop the ingress replicas. No new transaction enters while the chain
   reverts.
2. List the revoked receipts: for every block in `H + 1 ..= sealed head`, read
   the receipts from an executor's state database (`kardamom-reconstruct
   --list-receipts` where available, or the executor query endpoint). Keep the
   list; the senders are told from it.
3. Take a consistent cut. The chain's lane cursors must not deliver a message
   the restored history did not send. For every outbound lane, read `sent` at
   block `H` and `delivered` on the peer. If a peer delivered a message sent
   after `H`, the peer reverts too, to its own cut of the same round. The
   checkpoint markers of the recovery lines define the round.
4. Roll back the L1 outputs past the cut: `rollbackOutputs(fromIndex)` on the
   `WithdrawalOutputOracle`, from the recovery principal, for every output
   whose L2 block is above `H`. Pause the oracle first and unpause it after,
   so no withdrawal finalizes during the revert. An output past its
   finalization window cannot roll back; a problem below that floor is handled
   forward, with a new cut.
5. Rebuild the state at `H` from L1 and the DA layer:
   `kardamom-reconstruct --through-block H --expect-root <root at H>`, with
   `--executor-image`. The rebuilt database carries the resume cursor of block
   `H` from the posted payload.
6. Reset the sealers: stop the cluster job, clear the cluster and archive
   directories of every member, and seed the fresh cluster at `H + 1` with the
   rebuilt cursor (`kardamom.cluster.seedSnapshot`, where the sealer build has
   it; else the flag day: a new genesis allocation at `H`, a new settlement,
   and every role from empty volumes).
7. Install the rebuilt state on every executor and the validator, with their
   checkpoints removed, and start them. Each resumes at the cursor of `H`.
8. Reset the batcher: its cursor file and its spool. Clear its halt. It
   reconciles against L1 at batch `lastBatchIndex` and continues from `H + 1`.
9. Start the ingress replicas. The chain seals again from `H + 1`.
10. Verify: the rebuilt state root at `H` equals the validator's root at `H`,
    and the first new batch posts with `l2BlockStart == H + 1`.
11. Publish the list of revoked receipts to the senders.

## Clear

The revert clears the `replay_unavailable` halt of the batcher through
`POST /halt/clear` on its node (step 8). No other halt stands after the chain
seals again. Record the revert, its range, and the time it took: the
`revert-to-posted-head` chaos case proves and times this procedure.

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
   the receipts from the state database of an executor. Keep the list. The senders are told from it.
3. Take a consistent cut. The chain's lane cursors must not deliver a message
   the restored history did not send. For every outbound lane, read `sent` at
   block `H` and `delivered` on the peer. If a peer delivered a message sent
   after `H`, the peer reverts too, to its own cut of the same round. Each chain
   records its block of a round in the `CheckpointMarker` predeploy. Read it
   with `roundBlock(round)`.
4. Roll back the L1 outputs past the cut.
   - Call `rollbackOutputs(fromIndex)` on the `WithdrawalOutputOracle`. Use the `recovery` account. Do it for every output whose L2 block is above `H`.
   - Call `pause()` first and `unpause()` after. No withdrawal then finalizes during the revert.
   - An output past its finalization window cannot roll back. Handle a problem below that floor forward, with a new cut.
5. Rebuild the state at `H` from L1 and the DA layer:
   `kardamom-reconstruct --through-block H --expect-root <root at H>`, with
   `--executor-image`, `--sealer-seed <seed file>` and, for a chain with
   bridge deposits, `--lockbox <address>`. The rebuilt database carries the
   resume cursor of block `H` from the posted payload. The seed file holds
   the same head for the sealers.
6. Reset the sealers: stop the cluster job and clear the cluster and archive
   directories of every member. Start the fresh cluster from the seed file.
   - Give every member the same file in `-Dkardamom.cluster.seedSnapshot`.
     The deploy has no switch for it. Add the property to the JVM options of
     the members, and make the file readable in the container.
   - Open the bootstrap for this start: a deploy with
     `KARDAMOM_CLUSTER_BOOTSTRAP=1` of a job that Nomad does not know.
   - The sealers log `sealer state SEEDED` with the digest of the file, then
     `sealer seed CONFIRMED`. The sealers open block `H + 1`.
   - The chain keeps its genesis and its settlement contract.
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
seals again. Record the revert, its range, and the time it took.

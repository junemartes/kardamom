# replay_unavailable

## Cause

The sealer refused the batcher's replay from its cursor: the range is below
the sealer's retention floor, and no copy of the ordering is at hand.

## Confirm

1. Read `/halt` on the batcher. The detail names the cursor (record index and
   block) and the sealer's floor.
2. Read the batcher's spool directory. The spool holds the blocks the batcher
   consumed and did not post yet. If the spool ends before the floor, the gap
   between the spool's end and the floor is the lost range.
3. Read `lastBatchIndex` and `l2BlockEnd` from the settlement contract. The
   posted head is the last block L1 holds.

## Steps

1. Do not restart the batcher. A restart loses the detail and gets the same
   refusal.
2. Recover the range from a surviving copy, in this order:
   1. The spool: if it holds every block from the posted head to the floor, the
      batcher continues from it on the clear. Nothing else is needed.
   2. An executor's or the validator's block payload store
      (`kardamom_getBlockPayload`), where one exists: copy the missing range
      into the spool with the recovery tool, then clear.
   3. L1 and the DA layer: the range the sealer refused is not on L1 by
      definition, so this source holds nothing new.
3. If no copy survives, the chain reverts to the posted head. Follow
   `revert_to_posted_head.md`. That procedure revokes the receipts of the
   unposted range and is the last resort.

## Clear

This halt needs an operator (`operator`). After the range is in the spool, or
after the revert, run `POST /halt/clear` on the batcher's node. The batcher
starts its resume again: it reconciles against L1, loads the spool, and
continues.

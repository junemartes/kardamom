# replay_unavailable

## Cause

The sealer refused the replay from the cursor of the batcher. The range is below the floor of the sealer. The batcher could not rebuild the gap.

## Confirm

1. Read `/halt` on the batcher. The detail holds the text of the failure.
   - After a failed rebuild, it is the error of the rebuild.
   - After a second refusal, it says that the floor of the sealer moved to a block.
2. Read the batcher's spool directory. The spool holds the blocks the batcher
   consumed and did not post yet. If the spool ends before the floor, the gap
   between the spool's end and the floor is the lost range.
3. Read `lastBatchIndex` and `l2BlockEnd` from the settlement contract. The
   posted head is the last block L1 holds.

## Steps

1. Read the `/halt` detail before you do anything else. Record it.
2. Recover the range from a surviving copy, in this order:
   1. The spool: if it holds every block from the posted head to the floor, the
      batcher continues from it on the clear. Nothing else is needed.
   2. The rebuild of the batcher. It reads the block references from the query endpoints of the executors and the validator (`--block-refs-source`). It reads the bytes of the transactions from the `tx_data` archives. Fix what the `/halt` detail names, then clear.
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

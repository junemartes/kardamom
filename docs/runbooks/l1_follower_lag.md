# l1_follower_lag

This runbook belongs to an alert, not to a halt: `KardamomL1FollowerLag`.

## Cause

No L1 follower instance published the finalized L1 blocks for two finality
steps (64 blocks, about 13 minutes). Both instances are down, halted, or
stuck behind a slow source. The consumers of `l1_blocks` wait, so deposits and
batch posts stall next.

## Confirm

1. Read `kardamom_l1_follower_published_block_number` and
   `kardamom_l1_indexer_l1_finalized_block_number` of each instance. The
   difference is the lag.
2. Read `/halt` of each instance (metrics port 9009). A halt names its cause
   and its runbook.
3. Read `kardamom_l1_follower_l1_reads_total` and the instance log. Reads that
   do not move mean a frozen process; reads that fail mean a source fault.

## Steps

1. If an instance is halted, follow the runbook its halt names.
2. If no allocation runs, read the job status (`nomad job status l1-indexer`)
   and the placement failures. Each instance needs its own node with the
   `indexer` role.
3. If an instance runs and reads but is slow, read its source latency in its
   log. Add a faster endpoint to `L1_FOLLOWERS_RPC`, or raise
   `L1_MAX_LOG_RANGE` on a provider with a larger log span.

## Clear

The alert clears when an instance publishes within two finality steps of the
finalized tip. The follower reads the next range at once while it is behind.

# l1_follower_wake_overdue

This runbook belongs to an alert, not to a halt: `KardamomL1FollowerWakeOverdue`.

## Cause

An L1 follower instance planned to read L1 at a time
(`kardamom_l1_follower_next_wake_seconds`) and did not read by two minutes
after it. The process froze, or a read hangs. Its `/ready` answers 503.

## Confirm

1. Read `kardamom_l1_follower_next_wake_seconds` and
   `kardamom_l1_indexer_last_tick_unix_seconds` of the instance. Both stand
   still.
2. Read the instance log for the last read. A read with no answer points at a
   source that hangs.

## Steps

1. Restart the allocation (`nomad alloc restart <alloc>`). The follower
   resumes from its cursor; the consumers drop the records it publishes again.
2. If the next wake is overdue again, remove the slow source from
   `L1_FOLLOWERS_RPC`.

## Clear

The alert clears when the instance reads again and plans its next wake.

# origin_gap

## Cause

The sealer refuses the next L1 epoch because an earlier epoch is missing, and
this sequencer does not hold the missing epoch. Or the sequencer holds 4096
relayed epochs that no boundary confirms.

The sealer accepts only the epoch of L1 block `l1_origin + 1`. It answers any
other epoch with an `ORIGIN_GAP` reject that names the expected block. A
sequencer keeps every epoch it relayed until a boundary carries its L1 block,
and on a reject it offers its epochs again from the expected block. This halt
stands only when no epoch in its queue fills the gap. Deposits stop. User
transactions continue, and the L2 blocks keep the old L1 origin.

## Confirm

1. Read `/halt` on the sequencer (port 9001 for lane 0, 9011 for lane 1). The
   detail names the L1 block that the sealer expects, or the queue size and
   its oldest L1 block.
2. Read the sealer log on the leader: `cluster ORIGIN-GAP ... offered=<n>
   expected=<m>`. The `expected` value is the missing L1 block.
3. Read `kardamom_sequencer_epochs_unconfirmed` and
   `kardamom_sequencer_origin_gap_total` on each sequencer. A twin that holds
   the missing epoch fills the gap without help, and the halt clears within
   seconds.
4. Read `kardamom_da_watcher_epoch_origin_block_number` on the da-watcher. If it
   is past the expected block and the da-watcher restarted after the sealer's
   last epoch, the da-watcher skipped the blocks between them: it seeds its
   cursor at the finalized tip on every start.

## Steps

1. If every sequencer reports the same expected block `m`, no replica holds the
   epoch. Restart the da-watcher with `--l1-resume-after <m - 1>`. It publishes
   the epochs from block `m` again, and each sequencer offers them in order. The
   sealer drops the copies of the epochs it ordered already.
2. If the detail says that the queue is full, the sealer takes the offers and
   orders none of them. Find why: read the sealer leader's log for `DROPPED`
   lines and the `origin-record-regression` reason. A sequencer whose queue is
   full reads no new epoch, so restart the sequencers first (each starts with an
   empty queue), and then the da-watcher with `--l1-resume-after <m - 1>`, as in
   step 1. The sequencers read the epochs live, so they must subscribe before
   the da-watcher publishes.
3. Do not skip the missing block. A skipped epoch drops its deposits for good.

## Clear

This halt clears by itself (`auto`). The sequencer retries every millisecond
and clears the halt when its epoch lane moves again: a boundary confirms the
missing block (a twin filled it), the missing epoch arrives on `tx_deposits`,
or a boundary confirms the oldest epochs of a full queue.

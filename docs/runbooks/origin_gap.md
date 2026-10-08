# origin_gap

## Cause

The sealer refuses the next L1 epoch because an earlier epoch is missing, and
this sequencer did not get the missing epoch for 90 s. Or the sequencer holds
4096 relayed epochs that no boundary confirms.

An epoch can be lost between the da-watcher's publish and the sealer's
commit: a leader kill or a quorum loss drops the offer, and a sequencer that
restarts loses the epochs it held. Two parties heal this without an operator:

- A sequencer keeps every epoch it relayed until a boundary carries its L1
  block. On an `ORIGIN_GAP` reject it offers its epochs again from the
  expected block.
- The da-watcher follows the sealer's boundaries. When no boundary confirms
  its published epochs for 30 s, it publishes them again, in order, from the
  block after the sealer's origin.

So a sequencer waits 90 s (three re-publish periods) for the missing epoch
before it raises this halt. The halt means that the da-watcher does not
publish the epoch again: it is down, halted, or does not follow the sealer.
Deposits stop. User transactions continue, and the L2 blocks keep the old L1
origin.

## Confirm

1. Read `/halt` on the sequencer (port 9001 for lane 0, 9011 for lane 1). The
   detail names the L1 block that the sealer expects, or the queue size and
   its oldest L1 block.
2. Read the sealer log on the leader: `cluster ORIGIN-GAP ... offered=<n>
   expected=<m>`. The `expected` value is the missing L1 block.
3. Read `kardamom_sequencer_l1_origin` on a sequencer: the sealer's L1 origin,
   `m - 1`.
4. Read the da-watcher on the aux node (`curl -s http://127.0.0.1:9005/metrics`
   and `/halt`):
   - `kardamom_da_watcher_l1_confirmed_origin` is `m - 1` when the da-watcher
     follows the sealer. It is absent when the da-watcher runs without
     `--config`, or when its boundary session never connected: read its log
     for `following the sealer's boundaries` and `no boundary from the sealer`.
   - `kardamom_da_watcher_epochs_unconfirmed` above 0, and
     `kardamom_da_watcher_epochs_republished_total` that grows every 30 s: the
     da-watcher publishes the epochs again, and the sequencers do not take
     them. Read the sequencers' `tx_deposits` subscription.
   - A halt on the da-watcher (`l1_chain_break`, `l1_cursor_unreadable`, an L1
     that does not answer) stops its publishes and its re-publishes.

## Steps

1. Repair the da-watcher. If it is halted, follow the runbook of its halt. If
   it is down, start it. A start resumes after the sealer's L1 origin by
   itself: it logs `resuming after the sealer's L1 origin` with
   `sealer_origin=<m - 1>`, and publishes the epochs from block `m`. Each
   sequencer offers them in order, and the sealer drops the copies of the
   epochs it ordered already.
2. Fallback, when the da-watcher cannot reach the sealer cluster (it logs
   `no boundary from the sealer within the start wait`): run the da-watcher
   job once with `--l1-resume-after <m - 1>`, as `sealer-fleet-rebuild.md`
   step 6.4 shows. The flag overrides the cursor file and the wait for the
   first boundary. Remove the flag at the next deploy.
3. If the detail says that the queue is full, the sealer takes the offers and
   orders none of them. Find why: read the sealer leader's log for `DROPPED`
   lines and the `origin-record-regression` reason. A sequencer whose queue is
   full reads no new epoch, so restart the sequencers (each starts with an
   empty queue). The da-watcher publishes the unconfirmed epochs again within
   30 s.
4. Do not skip the missing block. A skipped epoch drops its deposits for good.

## Clear

This halt clears by itself (`auto`). The sequencer retries every millisecond
and clears the halt when its epoch lane moves again: a boundary confirms the
missing block (a twin filled it), the missing epoch arrives on `tx_deposits`
(the da-watcher published it again), or a boundary confirms the oldest epochs
of a full queue.

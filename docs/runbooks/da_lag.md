# da_lag

## Cause

The sealed head is more than `DA_LAG_BUDGET_BLOCKS` past the last block posted
to L1, so the sealer refuses new transactions until the batcher posts again.

## Confirm

1. Read `/halt` on an ingress. The detail names the sealed head, the posted
   head, and the budget.
2. Read `kardamom_batcher_last_posted_block` on the batcher, and `/halt` on
   the batcher. The batcher is the cause: it is halted, frozen, or cannot
   reach L1.
3. A client sees the JSON-RPC error `chain halted: DA lag` (code -32010) on
   `eth_sendRawTransaction`. Deposits and block boundaries continue.

## Steps

1. Find why the batcher does not post. Its own `/halt` names the cause:
   `l1_unreachable` or `replay_unavailable`. Follow that runbook.
2. If the batcher process is gone, start it. It resumes from its cursor and the
   spool.
3. Do not set the budget to zero to make the chain live again. Zero turns the
   guard off, and the chain then seals blocks nobody can post. That is the
   loss the guard exists to prevent. A chain that accepts the risk sets zero in
   the open, in its deploy, before an incident.

## Clear

This halt clears by itself (`auto`). The batcher publishes its confirmed cursor
on every post. When the cursor comes within the budget of the sealed head, the
sealer accepts transactions again, and the ingress clears the halt on the next
status it reads from the cluster.

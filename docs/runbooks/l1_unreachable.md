# l1_unreachable

## Cause

No L1 source answers: every endpoint errors, rate-limits, or times out.

## Confirm

1. Read `/halt` on the halted service (the batcher or the l1-indexer). The
   detail carries the last error. A da-watcher paused with the root
   `l1-indexer/l1_blocks` and this cause is not halted: the follower's stream
   carries no record (both follower instances are down or halted), or the
   da-watcher waits for a record no archive holds
   (`kardamom_da_watcher_waiting_for_l1_block`). Follow the follower's state:
   `nomad job status l1-indexer` and its `/halt` on port 9009.
2. Query the endpoint by hand: `eth_blockNumber` on each `--l1-rpc` URL. A 429
   is a rate limit, a connection error is an outage.
3. Check `kardamom_batcher_last_post_age_seconds` and the `tick_total` counters
   of the l1-indexer. They show how long L1 has been out.

## Steps

1. If the endpoint rate-limits, raise the plan or add a second endpoint to the
   `--l1-rpc` list. Two sources let the follower rotate around a limit.
2. If the endpoint is down, wait for it, or switch the list to another one.
3. While the batcher cannot post, the sealed head moves away from the posted
   head. Watch `kardamom_halt{cause="da_lag"}` on the ingress: the DA-lag
   guard halts new transactions before the chain loses its recoverable range.
   This is the designed order. Do not raise the budget to avoid it.

## Clear

This halt clears by itself (`auto`). The service retries L1 on a backoff and
resumes when a source answers. The batcher then posts the spooled blocks.

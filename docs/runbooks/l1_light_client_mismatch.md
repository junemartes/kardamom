# l1_light_client_mismatch

## Cause

The last header of the L1 follower's finality step is not the light client's
finalized header for that block number. Two public sources agreed on the
header, so both lie the same way, or the light client is wrong.

## Confirm

1. Read `/halt` on the halted follower (`l1-indexer`, metrics port 9009). The
   detail names the block number, the hash the sources agreed on, and the hash
   of the light client.
2. Ask a third L1 endpoint, from another provider, for the block by number.
   Compare its hash with both hashes in the detail.
3. Read the light client's finalized block (`eth_getBlockByNumber` with
   `finalized`) and its sync status. A light client that lost its sync
   committee serves an old or a wrong head.

## Steps

1. If the third endpoint agrees with the light client, both public sources
   lie. Remove them from the follower's `L1_FOLLOWERS_RPC` list, add two
   endpoints of other providers, and deploy.
2. If the third endpoint agrees with the public sources, the light client is
   wrong. Restart the light client job from a fresh checkpoint
   (`L1_LIGHT_CLIENT_CHECKPOINT`), and wait until it serves the finalized head.
3. Check the other follower instance. If it published the step, the consumers
   have the step from it. If its sources are the same, it is halted too.

## Clear

This halt waits for an operator (`operator`). The follower published nothing
of the step, and its cursor did not move. After the steps, clear the halt on
the follower's node:

```sh
curl -s -X POST http://127.0.0.1:9009/halt/clear
```

The follower reads the step again at once. A second mismatch raises the halt
again.

# l1_follower_disagreement

## Cause

A consumer of the `l1_blocks` stream (the da-watcher, the batcher) received two
records of one L1 block number with different hashes. The two follower
instances published different blocks: the sources of one instance lie in a way
its two-source check did not catch, for example two endpoints of one provider.

## Confirm

1. Read `/halt` on the halted consumer. The detail names the block number and
   the two hashes. The first hash is the record the consumer took.
2. Read `kardamom_l1_follower_published_block_number` and the log of each
   follower instance (`l1-indexer-0`, `l1-indexer-1`). Find the instance that
   indexed each hash: `curl -s -X POST -d '{"jsonrpc":"2.0","id":1,
   "method":"indexer_l1_block","params":[<number>]}' http://<node>:8549`.
3. Ask an L1 endpoint of a third provider for the block by number. Its hash
   names the honest instance.
4. Read the validator's epoch check (`validator_epoch_faults_total`). A lie
   that reached the chain also fails there.

## Steps

1. Stop the lying instance's allocation, or its whole job when both lie.
2. Replace the lying instance's sources (`L1_FOLLOWERS_RPC`): two endpoints of
   two providers. Wipe its archive (`/opt/kardamom/l1-indexer` on its node):
   its cursor holds the lie. Deploy; it re-indexes from its start block.
3. If the consumer took the lying record first, its cursor holds the lie.
   Follow the `l1_chain_break` runbook for that consumer: a da-watcher restart
   resumes after the sealer's L1 origin and reads that block's record again.
4. If the lie reached the chain (step 4 of Confirm), follow the
   `validator_divergence` runbook.

## Clear

This halt waits for an operator (`operator`). After the steps, clear it on the
consumer's node:

```sh
curl -s -X POST http://127.0.0.1:<port>/halt/clear
```

The consumer takes records again from its cursor. A second disagreement raises
the halt again.

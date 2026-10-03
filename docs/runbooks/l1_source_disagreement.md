# l1_source_disagreement

## Cause

Two L1 sources answered differently for one block or one log query, and no
light client settled it.

## Confirm

1. Read `/halt` on the halted follower. The detail names the block, the two
   answers, and the two sources.
2. Check `kardamom_l1_source_disagreement_total` on the follower. It increased
   at the time of the halt.
3. Ask a third source for the block: `eth_getBlockByNumber` on a node you
   trust, or the light client. The answer tells you which source lies.

## Steps

1. Do not resolve the disagreement by majority of public endpoints. Two public
   endpoints can share one backend.
2. Identify the lying source with the third answer from the step above.
3. Remove the lying source from the follower's `--l1-rpc` list, or wait for the
   source's operator to fix it. A source that lies once is not trusted again
   without a reason.
4. If the light client was down, restart it. Its answer settles a disagreement
   inside its window.

## Clear

This halt clears by itself (`auto`). The follower retries the read on every
tick and resumes when two sources agree, or when the light client serves the
block. No command is needed.

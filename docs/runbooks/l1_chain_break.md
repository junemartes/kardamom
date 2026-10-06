# l1_chain_break

## Cause

A finalized L1 block does not descend from the block the follower holds: its
parent hash is not the hash the follower indexed.

## Confirm

1. Read `/halt` on the halted follower (the l1-indexer or the da-watcher). The
   detail names the block number, the parent hash the source served, and the
   hash the follower expected.
2. Ask a second L1 source for the block before it, by number, and compare its
   hash with the hash in the detail. If the second source agrees with the
   follower, the first source lies. If the second source agrees with the
   first, the follower's cursor is wrong.

## Steps

1. If the source lies, remove it from the follower's `--l1-rpc` list, or wait
   for its operator to fix it. Finality forbids a change under a finalized
   block, so a different hash is a lie, not a reorg.
2. If every source agrees against the follower, the follower's cursor holds a
   hash it got from a lie earlier. For the l1-indexer: stop it, remove the
   archive's cursor, and restart it with `--start-block` at a block you trust.
   The archive writes are idempotent, so a re-index of a range is safe. For
   the da-watcher: its anchor is in its cursor file
   (`/opt/kardamom/da-watcher/l1-cursor`), so a plain restart halts again.
   Run the job with `--l1-resume-after` at the block in the halt detail
   minus 1, as `sealer-fleet-rebuild.md` step 6.4 shows. The flag overrides
   the file, and the first tick reads that block's hash from the sources
   again and writes the file. Remove the flag at the next deploy. Do not
   only delete the file: the da-watcher then starts at the finalized tip, and
   the deposits of the blocks before the tip are lost.
3. Check the chain's L1-origin records for the lie. A da-watcher that published
   an epoch with a wrong hash before the halt left it in the canonical log. The
   validator's epoch check catches it on the executor side.

## Clear

This halt clears by itself (`auto`). The follower retries the block on every
tick and resumes when the source serves a block that descends from the
indexed one. After a cursor reset (step 2), the restart clears it. The
da-watcher checks the link again after a restart, against the hash in its
cursor file.

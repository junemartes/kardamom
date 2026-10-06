# l1_cursor_unreadable

## Cause

The da-watcher's L1 cursor file exists, but the da-watcher cannot read it or
parse it. With `--config` (the deploy), the file holds the sealer's confirmed
L1 origin. Without `--config`, it holds the last L1 block whose epoch the
da-watcher published. A file that does not read is evidence of a fault, so the
da-watcher does not guess a position: it publishes nothing.

## Confirm

1. Read `/halt` on the aux node: `curl -s http://127.0.0.1:9005/halt`. The
   detail names the file, the error, and the first bytes of the contents.
2. Read the file on the aux node: `cat /opt/kardamom/da-watcher/l1-cursor`. A
   good file holds one line: the L1 block number, one space, and the block
   hash as `0x` and 64 hex digits. An empty file, a partial line, or a
   permission error confirms this cause.

## Steps

1. With `--config` (the deploy), remove the file and clear the halt:

   ```sh
   ssh aux-0 "rm -f /opt/kardamom/da-watcher/l1-cursor &&
     curl -s -X POST http://127.0.0.1:9005/halt/clear"
   ```

   The da-watcher then waits for the sealer's first boundary, and resumes
   after the sealer's L1 origin. Its log shows `resuming after the sealer's L1
   origin`, and the next pass writes the file again. If no boundary arrives
   within 20 s, it starts at the finalized tip, and it follows the sealer's
   origin back as soon as a boundary arrives. Go to step 4.
2. Without `--config`, or as a fallback, find N, the last L1 block whose epoch
   the chain holds: the sealer's L1 origin. Read it on any sequencer:
   `kardamom_sequencer_l1_origin`. If no sequencer answers, read the highest
   value of the da-watcher's origin gauge before the halt in Prometheus:
   `max_over_time(kardamom_da_watcher_epoch_origin_block_number[7d])`. If you
   are not sure, use a lower block. A lower N publishes epochs that the sealer
   already holds, and the sealer drops them. A higher N skips the epochs
   between the true value and N: the sealer refuses the next epoch as an
   origin gap, and every sequencer halts on `origin_gap` (`origin_gap.md`).
3. Do one of these:
   - Write the file again, and clear the halt. Use the hash that L1 holds for
     N. The da-watcher runs as user 10001.

     ```sh
     HASH=$(cast block "$N" --field hash --rpc-url "$L1_RPC")
     ssh aux-0 "echo '$N $HASH' > /opt/kardamom/da-watcher/l1-cursor.new &&
       mv /opt/kardamom/da-watcher/l1-cursor.new /opt/kardamom/da-watcher/l1-cursor &&
       chown 10001:10001 /opt/kardamom/da-watcher/l1-cursor &&
       curl -s -X POST http://127.0.0.1:9005/halt/clear"
     ```

   - Or run the job with `--l1-resume-after N`, as
     `sealer-fleet-rebuild.md` step 6.4 shows. The flag overrides the file,
     and the first tick writes the file again. Remove the flag at the next
     deploy.

   Without `--config`, do not only delete the file. Without a file, that
   da-watcher starts at the finalized tip and skips the blocks between N and
   the tip. The sealer refuses its epochs as an origin gap, and deposits stop
   until the da-watcher runs with `--l1-resume-after N`.
4. Find why the file broke: a full disk, a manual edit, or a failed volume.
   Read `kardamom_da_watcher_l1_cursor_persist_failures_total`. A count above
   0 means that the writes fail.

## Clear

An operator clears this halt (`operator`). After the clear, the da-watcher
reads the file again, or finds it removed. If the file still does not read,
the da-watcher halts again. A restart with `--l1-resume-after` does not read
the file, so it starts without the halt.

# l1_cursor_unreadable

## Cause

The da-watcher's L1 cursor file exists, but the da-watcher cannot read it or
parse it. The file holds the last L1 block whose epoch the da-watcher
published. Without it, the da-watcher cannot know where to resume, so it
publishes nothing.

## Confirm

1. Read `/halt` on the aux node: `curl -s http://127.0.0.1:9005/halt`. The
   detail names the file, the error, and the first bytes of the contents.
2. Read the file on the aux node: `cat /opt/kardamom/da-watcher/l1-cursor`. A
   good file holds one line: the L1 block number, one space, and the block
   hash as `0x` and 64 hex digits. An empty file, a partial line, or a
   permission error confirms this cause.

## Steps

1. Find N, the last L1 block whose epoch the chain holds. Read the highest
   value of the da-watcher's origin gauge before the halt in Prometheus:
   `max_over_time(kardamom_da_watcher_epoch_origin_block_number[7d])`. If you
   are not sure, use a lower block. A lower N publishes epochs that the sealer
   already holds, and the sealer drops them. A higher N loses the deposits of
   the blocks between the true value and N.
2. Do one of these:
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
3. Do not only delete the file. Without a file, the da-watcher starts at the
   finalized tip, and the deposits of the blocks between N and the tip are
   lost.
4. Find why the file broke: a full disk, a manual edit, or a failed volume.
   Read `kardamom_da_watcher_l1_cursor_persist_failures_total`. A count above
   0 means that the writes fail.

## Clear

An operator clears this halt (`operator`). After the clear, the da-watcher
reads the file again. If the file still does not read, the da-watcher halts
again. A restart with `--l1-resume-after` does not read the file, so it starts
without the halt.

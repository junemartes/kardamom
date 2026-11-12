# sealer-two-blank-members

## Cause

Two members lose their Raft data and the surviving member cannot form a quorum.

## Confirm

1. Identify the survivor from its persistent `cluster/recording.log`, Raft archive,
   and last committed term and position. Preserve that member's complete data.
   Record the last client receipts, balances, nonces and executor head for validation.
2. Check that the other members lost their local Raft data. A blank member logs
   `cluster SEED waiting-for-peer`. The survivor reports an open election.
   Peer seeding needs a closed election, so waiting alone cannot restore this quorum.
3. If a second member still has intact data, restart that member first. If every
   member lost its data, follow [sealer-fleet-rebuild.md](sealer-fleet-rebuild.md).

## Steps

1. Save the full registered `cluster` job definition. Stop that job without purging
   it. Wait until every allocation is terminal and every cluster task has exited.
   Keep the member tasks stopped through the copy. Do not stop the shared executor
   archive or copy from a live Raft writer.
2. Preserve the failed members' remaining files for investigation. Prepare one
   failed member as the copy target. Clear its `cluster` and `seed` contents, and
   its Raft files directly under `archive`. Preserve `archive/dir`: that directory
   belongs to the shared executor archive. Do not clear the survivor's directories.
3. On the stopped survivor, under `/opt/kardamom`, record a file-hash manifest:

   ```sh
   find cluster archive -path archive/dir -prune -o -type f \
     ! -name 'cluster-mark*' ! -name 'archive-mark*' \
     -exec sha256sum {} + > /tmp/member-recovery.sha256
   ```

4. Copy the survivor's `cluster` and Raft `archive` data to the target. Include
   `recording.log`, `node-state.dat`, the catalog, snapshot recordings, Raft log
   segments and retained data. Exclude process-local mark files and links, and the
   shared archive. A tar stream starts on the survivor with:

   ```sh
   cd /opt/kardamom
   tar --exclude='cluster-mark*' --exclude='archive-mark*' \
     --exclude='archive/dir' -cf - cluster archive
   ```

   Extract it under `/opt/kardamom` on the target while its member task stays
   stopped. Check both sides of the transfer for failure. Copy the manifest and
   run `sha256sum --quiet -c /tmp/member-recovery.sha256` from that same directory
   on the target. A failed transfer or hash check leaves the target out of the
   restart until its partial Raft copy is removed or replaced successfully.
5. Restore the saved job definition. Keep the original member IDs and endpoints.
   The survivor and copied member start in `RESUME` mode and form a quorum. The
   remaining blank member seeds from that quorum. Never enable genesis bootstrap
   to bypass a failed peer seed. Do not create a new chain history or discard the
   survivor's recording log.
6. Check a closed election with one term and leader on a majority. Require a new
   `RESUME` line for the copied member and a new `SEED from-peer` line for the
   remaining member. Each must take a new snapshot at or beyond the pre-fault
   head. Earlier log lines do not prove recovery.

## Clear

The ingress clears its quorum halt when status frames resume. Confirm that every
executor converges, old receipts retain their identity, and balances and nonces
match the recorded pre-fault state plus new transactions. Require successful new
transactions from both an existing sender and a fresh sender, then validator BAL
and trie verification without divergence. Keep the preserved files until these
checks pass. The `cluster-two-blank-recover` chaos case exercises this procedure;
its deployed validation remains required.

The copy uses Aeron's archive and recording-log layout. Aeron's
[Cluster Backup documentation](https://aeron.io/docs/aeron-cluster/cluster-backup/)
also describes restoring those files and excluding the archive mark file.

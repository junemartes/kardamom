# sealer-fleet-rebuild

## Cause

Every sealer member lost its cluster and archive directories. No member holds a
Raft log or a snapshot, so no member can restart the canonical stream. The
chain restarts after the last block that L1 holds, from a state rebuilt from L1
and the DA layer. The blocks after that block are reverted.

This procedure revokes every receipt of the reverted blocks. Run it only with
the operator's word.

## Confirm

1. Read `nomad job status cluster`. No member is `running` for long, or no
   member elects a leader.
2. On each sealer node, list `/opt/kardamom/cluster`. A lost member has no
   `cluster-mark.dat` and no recording log. A member build that logs
   `cluster START mode=` logs `cluster SEED waiting-for-peer` on each round,
   because no peer holds a snapshot.
3. If one member still holds its directories, this procedure is wrong. The
   blank members copy that member's snapshot. Follow `sealer_no_quorum.md`.

## Steps

The commands run on an operator host with `nomad`, `cast`, `jq`, `curl`,
`rsync`, SSH to every node, and the release binaries. Set these values first:

```sh
export NOMAD_ADDR=http://<control-0>:4646
L1_RPC=<the L1 endpoint>
SETTLEMENT=<the KardamomL2Settlement proxy>
DA_PROXY=http://aux-0.node.dc1.consul:3100
LOCKBOX=<the ETHLockbox proxy>
mkdir -p rebuild && cd rebuild
```

The genesis is `deploy/cluster/config/genesis/dev.toml`. With
`PRIORITY_FEES=on`, append `fees.toml` to it, as the validator job does:
`cat dev.toml fees.toml > genesis.toml`.

### 1. Stop the pipeline and find the posted head H

1. Save each job and stop it. The saved file is the job you start again
   later. `nomad job stop` without `-purge` keeps the job's history. Stop the
   batcher last, after `lastBatchIndex` held still for a minute: every block
   it posts is a block that the revert keeps.

   ```sh
   for job in ingress sequencer da-watcher state-mirror cluster executor validator batcher; do
     nomad job inspect "$job" > "$job.json"
   done
   for job in ingress sequencer da-watcher state-mirror cluster; do nomad job stop "$job"; done
   cast call "$SETTLEMENT" 'lastBatchIndex()(uint64)' --rpc-url "$L1_RPC"
   nomad job stop batcher
   ```

2. Read H, the `l2BlockEnd` of the last posted batch:

   ```sh
   LAST=$(cast call "$SETTLEMENT" 'lastBatchIndex()(uint64)' --rpc-url "$L1_RPC")
   H=$(cast call "$SETTLEMENT" 'batches(uint64)(uint64,uint64,bytes32)' "$LAST" \
         --rpc-url "$L1_RPC" | sed -n 2p)
   ```

3. Record the old head and the reverted transactions while the executors still
   serve their old state. Read `kardamom_executor_block_number` on each
   executor, and use the executor with the highest head. The list holds L2
   transactions only: the deposits of the reverted blocks come again from L1
   in step 6.

   ```sh
   OLD_HEAD=$(curl -s http://executor-0.node.dc1.consul:9004/metrics \
     | awk '/^kardamom_executor_block_number/ {print $2}')
   for b in $(seq $((H + 1)) "$OLD_HEAD"); do
     curl -s http://executor-0.node.dc1.consul:9024 -H 'content-type: application/json' \
       -d "{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"kardamom_getBlockRefs\",\"params\":[$b]}" \
       | jq -r --arg b "$b" '.result.refs[]? | "\($b) \(.tx_hash)"'
   done > revoked-receipts.txt
   ```

4. Stop the executors and the validator: `nomad job stop executor` and
   `nomad job stop validator`.

### 2. Rebuild the state at H

Run the rebuild twice into two new directories. The first run writes the
executor image and the sealer seed. The second run keeps the trie, which the
validator needs. `--lockbox` derives the L1 deposits into the rebuilt state:
on a chain with deposits, the root is wrong without it.

```sh
kardamom-reconstruct --l1-rpc "$L1_RPC" --settlement "$SETTLEMENT" --da-proxy "$DA_PROXY" \
  --chain genesis.toml --lockbox "$LOCKBOX" --through-block "$H" \
  --state-dir executor-image --executor-image --sealer-seed seed.bin
kardamom-reconstruct --l1-rpc "$L1_RPC" --settlement "$SETTLEMENT" --da-proxy "$DA_PROXY" \
  --chain genesis.toml --lockbox "$LOCKBOX" --through-block "$H" \
  --state-dir validator-db
```

Check both report lines (`reconstructed head=...`):

- `head=` is H in both.
- `state_root=` is the same in both.
- `end_tx_idx=` is a number, not `none`. This is E_H, the resume index.

The first run also logs `sealer seed written` with `l1_origin=`. This is M, the
L1 origin of H. Bytes 40 to 47 of the seed hold the same value:

```sh
M=$(od -An -t u8 --endian=big -j 40 -N 8 seed.bin | tr -d ' ')
```

### 3. Wipe the old state

The old state belongs to the reverted chain. A consumer that resumes on it, or
restores one of its checkpoints, skips the records of the new chain.

1. On each sealer node, empty the cluster and archive directories. Keep
   `/opt/kardamom/archive/dir`: it is the node's shared Aeron archive, not the
   member's.

   ```sh
   for i in 0 1 2; do
     ssh sealer-$i 'find /opt/kardamom/cluster -mindepth 1 -delete &&
       find /opt/kardamom/archive -mindepth 1 -maxdepth 1 ! -name dir -exec rm -rf {} +'
   done
   ```

2. On each executor node, empty the state and the checkpoints:

   ```sh
   for i in 0 1 2; do
     ssh executor-$i 'find /opt/kardamom/state /opt/kardamom/checkpoints -mindepth 1 -delete'
   done
   ```

3. On the aux node, empty the validator's state and checkpoints, and remove
   the batcher's spool and cursor file. A spool that holds blocks after H
   continues the batcher's cursor, so the batcher would post reverted blocks.
   Without a cursor file, the batcher reads its cursor from the last posted
   batch: `(E_H, H + 1)`.

   ```sh
   ssh aux-0 'find /opt/kardamom/state/validator /opt/kardamom/checkpoints -mindepth 1 -delete &&
     rm -rf /opt/kardamom/batcher/spool /opt/kardamom/batcher/cursor.json'
   ```

4. Empty the account cache. Its rows carry positions of the reverted chain,
   and a row applies only above the stored position, so the new chain cannot
   overwrite them. Run `FLUSHALL` on the Redis primary that the sentinels name:

   ```sh
   redis-cli -h aux-0.node.dc1.consul -p 26379 SENTINEL get-master-addr-by-name kardamom
   redis-cli -h <primary> -p 6379 FLUSHALL
   ```

### 4. Seed the three members

1. Copy the seed to every sealer node:

   ```sh
   for i in 0 1 2; do
     ssh sealer-$i 'mkdir -p /opt/kardamom/seed' && scp seed.bin sealer-$i:/opt/kardamom/seed/seed.bin
   done
   ```

2. Give every member the seed. The cluster job mounts `/opt/kardamom/seed` and
   passes `-Dkardamom.cluster.seedSnapshot`, empty in a normal deploy. A seed
   carries no remote-origin anchor, so a seeded member runs with interop off:
   its allowlist must be empty. The job's variable `cluster_seed_snapshot`
   does both; on the saved job, `jq` does the same:

   ```sh
   jq '.Job.TaskGroups[].Tasks[].Env.JAVA_TOOL_OPTIONS |= (
         sub("-Dkardamom.cluster.seedSnapshot=[^ ]*"; "-Dkardamom.cluster.seedSnapshot=/opt/kardamom/seed/seed.bin")
         | sub("-Dkardamom.cluster.remoteOrigins=[^ ]*"; "-Dkardamom.cluster.remoteOrigins="))' \
     cluster.json > cluster-seeded.json
   ```

3. A member build that logs `cluster START mode=` starts a blank member at log
   position 0 only while the bootstrap is open. Open it:
   `nomad var put nomad/jobs/cluster bootstrap=true`.
4. Start the members: `nomad job run -json cluster-seeded.json`.
5. Read each member's log. Every member logs `sealer state SEEDED` with
   `block=H` and `endTx=E_H`, and the same `digest=`. No member logs
   `sealer state FRESH at genesis`. Then every member logs
   `sealer seed CONFIRMED`, and the members log `sealer snapshot TAKEN`.

   ```sh
   for a in $(nomad job allocs -t '{{range .}}{{if eq .ClientStatus "running"}}{{.ID}} {{end}}{{end}}' cluster); do
     nomad alloc logs "$a" cluster | grep -E 'sealer state|sealer seed|snapshot TAKEN'
   done
   ```

6. Close the bootstrap: `nomad var purge nomad/jobs/cluster`.

Keep the seed property on every member until the snapshot exists. Before it, a
blank member replays the log from position 0 and needs the seed file. After it,
a blank member restores the snapshot and ignores the property.

### 5. Install the rebuilt state

The services run as user 10001. Remove the lock file of the tool's writer.

```sh
for i in 0 1 2; do
  rsync -a executor-image/ executor-$i:/opt/kardamom/state/
  ssh executor-$i 'rm -f /opt/kardamom/state/mdbx.lck && chown -R 10001:10001 /opt/kardamom/state'
done
rsync -a validator-db/ aux-0:/opt/kardamom/state/validator/
ssh aux-0 'rm -f /opt/kardamom/state/validator/mdbx.lck &&
  chown -R 10001:10001 /opt/kardamom/state/validator'
```

### 6. Start the consumers and the producers

1. Start the executors and the validator: `nomad job run -json executor.json`,
   then `nomad job run -json validator.json`. Each executor logs
   `resuming from persisted state cursor via cluster canonical replay` with
   `resume_block=H` and `resume_record_count=E_H`. The leader logs
   `cluster REPLAY ... from=(E_H,H+1) served=`. A log of
   `restored state from checkpoint` or `fetched checkpoint from peer` means a
   checkpoint of the old chain survived: stop, and do step 3 again.
2. Start the state mirrors: `nomad job run -json state-mirror.json`. They find
   the cache empty and rebuild it from an executor's newest checkpoint.
3. Start the sequencers: `nomad job run -json sequencer.json`. They must run
   before the da-watcher starts. A sequencer reads the da-watcher's epochs
   live, with no replay, so an epoch published before the sequencers
   subscribe never reaches the sealer.
4. Start the da-watcher after block M. Without the flag, it starts at the
   finalized tip, and the deposits of the blocks between M and the tip are
   lost. If M is 0, the chain holds no epoch: leave out the flag.

   ```sh
   jq --arg m "$M" '.Job.TaskGroups[0].Tasks[0].Config.args += ["--l1-resume-after", $m]' \
     da-watcher.json > da-watcher-resume.json
   nomad job run -json da-watcher-resume.json
   ```

   Read the da-watcher's metrics on the aux node:
   `kardamom_da_watcher_epochs_published_total` equals
   `kardamom_da_watcher_l1_finalized_block_number` minus M.
5. Start the ingresses and the batcher:

   ```sh
   for job in ingress batcher; do nomad job run -json "$job.json"; done
   ```

### 7. Verify

1. The executor block gauge advances past H.
2. The first new batch starts at the block after H:

   ```sh
   NEXT=$((LAST + 1))
   cast call "$SETTLEMENT" 'batches(uint64)(uint64,uint64,bytes32)' "$NEXT" --rpc-url "$L1_RPC" | sed -n 1p
   ```

   The value is `H + 1`.
3. Send `revoked-receipts.txt` to the senders. Each transaction there got a
   receipt on the reverted chain and is undone. A sender submits it again.

## Clear

This procedure has no halt to clear. After the members log
`sealer snapshot TAKEN`, the next `just deploy` registers the cluster job
without the seed property and the da-watcher job without
`--l1-resume-after`. The deploy rolls the sealer members one at a time; each
one restores the snapshot. A da-watcher that restarts with a stale
`--l1-resume-after` sends epochs that the sealer already holds; the sealer
drops them as a regression.

The output attester is off in the deploy. Where it runs, L1 can hold output
roots for the reverted blocks: roll them back as `revert_to_posted_head.md`
step 4 says, before step 6 here.

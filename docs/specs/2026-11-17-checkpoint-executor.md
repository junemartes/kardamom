# The checkpoint executor on the backup node

- Status: decided by the user on 2026-11-17 (section 8). Not implemented yet.
- Reason: the 8-9 ms p99 tails of the staging benchmarks of 2026-11-16.
- Decision of the user on 2026-11-17: the checkpoint writes move to the backup machine. A slower
  recovery is acceptable, because a checkpoint matters only when a node or the cluster recovers.

## 1. The problem

- Each executor runs on the machine of one sealer member (`KARDAMOM_EXEC_ON_SEALERS=on`), and
  both write to one NVMe RAID1.
- With `--checkpoint-log`, each executor copies its whole state database as a new base every
  6 hours (`--checkpoint-base-max-age-secs 21600`), at 16 MiB/s
  (`--checkpoint-copy-mib-per-sec`).
- Aeron Cluster 1.44 commits at `min(quorumPosition(), own append position)` on the leader
  (`ConsensusModuleAgent.updateLeaderPosition`). So the archive writes of the leader are always
  on the commit path.
- On 2026-11-16, the base copy of executor-0 ran on the machine of the leader (member 0) from
  14:10 to 14:19:32 UTC. The writes of the leader archive slowed down by about 70 % for each byte.
  The commits that waited more than 5 ms went from 0.04 % to 0.41 %, and the client p99 went from
  about 4 ms to 8-9 ms. The copies on the two follower machines caused no tail.
- The copy keeps the free pages, and its read snapshot makes the database file grow
  (`crates/state/src/checkpoint/copy_rate.rs`). The bases grew from 9 GB to about 30 GB in one
  day, so each copy now takes 30 minutes or more.

## 2. The design

- **The checkpoint executor.** A fourth executor runs on the node of role `backup` (the backup
  sealer machine, outside the quorum). It executes every block, and it writes the bases and the
  block log. It serves them on `:9014`, as the other executors do.
- **The hot executors.** The three executors on the sealer machines make no checkpoint
  (`--checkpoint-interval-secs 0`, `--checkpoint-log` off). They keep `--checkpoint-dir` and
  `--checkpoint-peers`, so a cold start and a repair still restore a peer checkpoint. The
  executor already has this restore-only mode.
- **The peers.** Every list of checkpoint peers names the checkpoint executor: the executors
  (`--checkpoint-peers`) and the validator.
- The hot path does not change. No hop is added.

## 3. Rules of the checkpoint executor

1. **It publishes no receipts.** Today each executor publishes its copy of each receipt to the
   multicast group of `tx_receipts`, and each reader drops the copies by transaction hash in a
   window sized by `tx_receipts_executor_count`. A late copy from an executor that lags could
   fall outside that window. So the checkpoint executor gets a switch that turns off its receipt
   publication.
2. **It is no void voter and sends no cursor.** It runs with no `--void-voter-id` and with
   `--exec-cursor` off. So the sealer never waits for it.
3. **It may lag.** Its disks are two 7,200 rpm disks in RAID1, and the backup archive already
   keeps them about 99 % busy under load. A lag only makes its checkpoints older.
4. **It must read past the sealer window.** The sealer keeps 65,536 frames, about 30 s at
   2,000 tx/s. A cursor below the window gets `REPLAY_UNAVAILABLE`, and the executor then needs a
   peer checkpoint. With the hot executors in restore-only mode, no newer peer checkpoint exists.
   So the checkpoint executor reads the range read of the egress for a cursor below the window.
   Today `--egress` needs `--carry-bytes`, and a carry executor reads the member feed of a member
   on its own machine. The backup machine has no member. So `--egress` must also work for an
   executor that reads the cluster egress.
5. **Its average rate must reach the rate of the chain.** Rule 4 covers a burst. A disk that
   cannot keep up at the average rate leaves the executor behind for good. The rollout measures
   this (section 6).

## 4. The state mirrors

- A cold rebuild of a state mirror reads the checkpoints of the executor on its machine
  (`/opt/kardamom/checkpoints`, read only). With the hot executors in restore-only mode, that
  directory holds no new checkpoint.
- Change: the state mirror fetches the newest checkpoint from a peer (`GET /checkpoint/latest`),
  with the fetch code of the executor. Its list of peers names the checkpoint executor.
- A rebuild still writes the restored copy on the machine of the mirror. A rebuild happens only
  after a loss of Redis, so it does not touch a normal benchmark.

## 5. Failure rules

- **The backup machine is down.** No new checkpoint is made. The hot executors run on. A node
  that must restore waits for the backup machine, or for a rebuild from L1.
  The alert on the age of the newest checkpoint fires (new, section 7).
- **The checkpoint executor diverges.** It stops, as every executor does. The hot executors
  are not touched.
- **A hot executor diverges.** Its rollback takes the newest peer checkpoint below the divergence
  (`GET /checkpoint/latest?max_block=<block - 1>`). This path exists. It now always goes to the
  peer, because a hot executor has no local checkpoint.
- **No second source (decision 3).** The hot executors make no base. A loss of the backup
  machine leaves no new checkpoint until it returns.

## 6. Rollout

1. The code (sections 3 and 4) behind switches, off by default. No deploy changes.
2. The deploy: the Nomad group of the checkpoint executor on the node of role `backup`, the
   variable for the restore-only mode of the hot executors (the job refuses 0 today), the peer
   lists, the fixtures and the switch table of the README.
3. Staging, step 1: the checkpoint executor runs beside the hot executors, which still make their
   bases. A 2,000 tx/s cell and a 4,000 tx/s cell of 300 s measure its lag. If it cannot keep the
   average rate, the backup machine needs an NVMe disk before step 2. Production runs under
   constant load, so the production backup machine gets an NVMe disk (decision 2).
4. Staging, step 2: the hot executors switch to restore-only. The benchmark repeats during the
   hour when the base of executor-0 would have run. The p99 must not rise in that hour.
5. The chaos cases that move checkpoints (`state-checkpoint-restore`, `replay-window-resync`,
   `retention-overrun`) take the checkpoint from the checkpoint executor.

## 7. Metrics

- The age of the newest checkpoint, and its block, on each executor and on the checkpoint
  executor. An alert fires when the newest checkpoint is older than 12 hours.
- The lag of the checkpoint executor behind the sealed head, in blocks.

## 8. Decisions of the user (2026-11-17)

1. Rule 4: a checkpoint executor that reads the cluster egress reads the range read of the egress
   for a cursor below the sealer window. `--egress` no longer needs `--carry-bytes`.
2. The disks: measure the spinning disks first (section 6, step 3). Production gets an NVMe disk,
   because it runs under constant load.
3. No second source for now: the hot executors make no base.
4. The state mirrors fetch the newest checkpoint from a peer (section 4).

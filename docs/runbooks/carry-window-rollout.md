# carry-window-rollout

## Cause

This procedure turns on the carry window (feature 5, `FEATURE_CARRY_WINDOW`) on a live chain, and moves the
executors to the bytes that the sealer carries. The chain does not stop, and no member restarts all at once.

- The exact window bounds the heap and the snapshot of each member at any DA lag.
- Each executor runs on the node of its sealer member and reads the member feed of that member.
- After the switch, no reader of the order meets a transaction reference that it cannot join.

The order is not circular. Each step needs only the steps before it. Do not start a step until every check of the
step before it passes. Step 9 is an upgrade transaction on L1: it needs the explicit approval of the operator.

The switches and the gates of the deploy are in
[The carry window](../../deploy/cluster/README.md#the-carry-window).

## Confirm

1. Feature 4 is active, and every member runs decision version 5:

   ```sh
   for i in 0 1 2; do
     curl -s "http://sealer-$i.node.dc1.consul:40205/status" \
       | jq '{memberId, decisionVersion, snapshotReadsMax, snapshotLatest, retainedStore, carryWindow, feed}'
   done
   ```

2. The release holds every change of the carry window: the decoder of a relayed full reference in every reader, the
   sealer of snapshot version 18, the member feed on the heap, the sequencer that keeps its sealer version, the
   readers with the egress cursor gauges, the egress with the reference mark, and this deploy.
3. The egress runs (`KARDAMOM_EGRESS=1`, `KARDAMOM_EXEC_FRAMES=on`), on two executor nodes.

The commands below run on an operator host with `nomad`, `curl`, `jq`, and the Consul DNS names of the nodes.
Each metrics line carries the labels of its exporter, such as
`kardamom_executor_block_number{service="executor",host_id="executor-0"} 42`. So match a gauge name with `[{ ]` after
it.
`<egress-node>` is a node that runs an egress instance (`nomad job status egress`). Set every switch in the
environment of the staging deploy, then deploy.

## Steps

### 1. Deploy the release

Deploy the release with every switch of the carry window off and no `KARDAMOM_SEALER_VERSION_FLOOR`.

Checks:

- Each member: `snapshotReadsMax` 18, `decisionVersion` 5, `snapshotLatest` 16, `retainedStore` `heap`, and
  `kardamom_sealer_window_exact` 0.

  ```sh
  curl -s "http://sealer-$i.node.dc1.consul:40205/metrics" \
    | grep -E '^kardamom_sealer_(window_exact|feature_state|feed_state|full_ref_below_v5_total|ref_refused_total)[{ ]'
  ```

- Every reader job (executor, validator, batcher, egress) has the meta `relayed_full_ref = "true"`, and every live
  allocation runs that job version:

  ```sh
  for job in executor validator batcher egress; do
    nomad job inspect "$job" | jq -r '.Job.Meta.relayed_full_ref, .Job.Version'
    nomad job allocs -json "$job" | jq -r '.[] | select(.ClientStatus == "running") | .JobVersion' | sort -u
  done
  ```

- No `FATAL` line in the sealer logs.

Rollback: allowed. No member wrote snapshot version 18.

### 2. Turn on the v2 loop of the sequencers

Deploy with `KARDAMOM_SEQUENCER_ADMISSION=on`. The sequencers roll one replica at a time. Each one gets the
`[admission]` table in its `sequencer.toml`, with 512 forwarded and 512 parked transactions for each sender, and the
job gets the meta `v2_loop = "true"`.

Why: the v1 loop of the sequencer offers each transaction as a reference of record type 0, at every decision version
and every floor. Only the v2 loop offers the bytes of a transaction (record type 7). So the floor of step 3, the stop
of the references in step 4, and the carry window need the v2 loop. Without it, the sealer refuses every transaction
after the switch. The floor gate and the upgrade check of feature 5 refuse while the sequencer job lacks `v2_loop`.

Checks:

- Each sequencer replica holds the table in its config:

  ```sh
  for alloc in $(nomad job allocs -json sequencer | jq -r '.[] | select(.ClientStatus == "running") | .ID'); do
    nomad alloc fs "$alloc" sequencer/local/sequencer.toml | grep -A1 '^\[admission\]'
  done
  ```

  Each one shows `enabled = true`.
- Receipts keep landing (the canary).
- `kardamom_sequencer_pending_depth` exists on each sequencer (metrics port 9001 + 10 × lane).

### 3. Set the sequencer version floor

Deploy with `KARDAMOM_SEALER_VERSION_FLOOR=5`, the lowest `decisionVersion` of the members. The release gate refuses
a floor above that value, and a floor above 0 while a reader job lacks `relayed_full_ref`.

Checks:

- Each sequencer: `kardamom_sequencer_sealer_version` 5 (metrics port 9001 + 10 × lane).
- `kardamom_sealer_full_ref_below_v5_total` stays 0 on every member.

### 4. Watch the references stop

Restart one sequencer of each pair: `nomad alloc restart <alloc>`. Then watch R on each egress instance for 30
minutes under the canary:

```sh
curl -s "http://<egress-node>.node.dc1.consul:9013/metrics" | grep -E '^kardamom_egress_ref_frame_max_(index|block)[{ ]'
```

Checks:

- R does not move on any instance.
- Write down the highest `kardamom_egress_ref_frame_max_index` and its block.

### 5. Move the batcher and the validator to the egress

Before the deploy:

- `kardamom_batcher_last_posted_block` (batcher, port 9002) and `validator_committed_block` (validator, port 9006)
  are above the highest `kardamom_egress_ref_frame_max_block`.

Deploy with `KARDAMOM_BATCHER_EGRESS=on` and `KARDAMOM_VALIDATOR_EGRESS=on`. The release gate checks the blocks
above, and refuses while an egress instance runs a release without the reference mark or does not answer.

Checks:

- `kardamom_batcher_egress_cursor_index` and `kardamom_validator_egress_cursor_index` are above the highest R.
- The batcher and the validator jobs carry the meta `reads_egress = "true"`.
- The validator agrees, and the batcher posts.

### 6. Apply the infrastructure change, then turn on the member feed

Apply the change of kardamom-infra (see "What staging needs" below). Then deploy with `KARDAMOM_MEMBER_FEED=on`.
The sealer rolls member by member, and the shared media driver starts on each sealer node.

Checks:

- Each member: `kardamom_sealer_feed_state` 1 (idle), and `feed` `idle` in its status.
- The `aeron` job runs on each sealer node: `nomad job status aeron`.

### 7. Move the executors to the sealer nodes

Deploy with `KARDAMOM_MEMBER_FEED=on` and `KARDAMOM_EXEC_ON_SEALERS=on`. The executor job moves one executor at a
time: `executor-2`, then `executor-1`, then `executor-0`. Each step waits for the health check of the moved
executor.

A known short window: the sequencers, the ingresses and the batcher already ask `sealer-<i>` for the nonce queries
and the block references. Until `executor-1` has moved, only `executor-2` answers them. The window lasts one step of
the move, a few minutes. This is acceptable on staging. Do the move at low load.

Checks:

- Each group `executor-<i>` runs on `sealer-<i>`: `nomad job status executor`.
- `kardamom_executor_block_number` (port 9004 on each sealer node) follows the sealed head.
- Receipts land (the canary). `kardamom_egress_copy_mismatch_total` does not move.

### 8. Run the upgrade check

Pick the activation time: about 30 minutes ahead, at low load. Then run:

```sh
just sealer-upgrade-check staging 5 <activation_ms>
```

It must pass. It refuses unless every member reads snapshot version 18, runs decision version 5 and keeps its frames
on the heap; the sequencer job has `sealer_version_kept` and `v2_loop`, the batcher and the validator jobs have `reads_egress`, and
the egress job has `ref_mark`; and every reader cursor is above the highest R of the egress instances.

### 9. Send the upgrade transaction on Sepolia

**This step needs the explicit approval of the operator. Do nothing in this step without that approval.**

The Safe signs `ETHLockbox.initiateUpgrade(5, <activation_ms>)`, with the time of step 8.

Checks:

- Each member logs `cluster FEATURE-ACTIVATION` with `feature=5` and `outcome=STORED`.
- `kardamom_sealer_feature_state{feature="5"}` is 1 on each member.
- From here, the members write snapshot version 18. A release that reads only up to version 17 stops on it. Do not
  roll the sealer back to such a release. See "A rollback after step 9" below.

### 10. Watch the switch

At the activation time:

Checks:

- Each member logs `cluster CARRY-WINDOW` with `carryWindow=true windowOnly=true`, at the same block.
- `kardamom_sealer_feature_state{feature="5"}` is 2, and `kardamom_sealer_window_exact` is 1, on each member.
- `kardamom_sealer_retained_frames` falls within `retention` frames on each member.
- `kardamom_sealer_snapshot_bytes` falls at the next snapshot.
- `kardamom_sealer_ref_refused_total` stays 0. Receipts keep landing.

### 11. Move the executors to carry mode, one at a time

Deploy with `KARDAMOM_CARRY_EXECUTORS=0`. After its checks pass, deploy with `0,1`, then with `0,1,2`. The release
gate refuses an executor unless every member reports `carryWindow` true, its member runs the feed, the executor job
has `carried_ref_fault`, and its `kardamom_executor_block_number` minus 32 is above the highest
`kardamom_egress_ref_frame_max_block`.

Checks, for each executor that moved:

- The executor logs `kardamom-executor: canonical entries via the member feed`.
- `kardamom_sealer_feed_state` is 3 (live) on the member of the executor.
- Receipts land. `kardamom_egress_copy_mismatch_total` does not move. No `UnjoinableReference` error.

### 12. Soak for 24 hours

Run the canary for 24 hours. Record these values, for the trigger of the retained file:

- `kardamom_step_seconds{step="sealer.snapshot"}`, p99 and max.
- `kardamom_sealer_snapshot_bytes` and `kardamom_sealer_retained_bytes`.
- The GC pauses of the sealer, p99 and max, from `/opt/kardamom/cluster/jvm-pauses.log`, and the heap after a GC.
- `retention` divided by the transaction rate.

### Before a load test above 1,000 transactions a second

Raise `KARDAMOM_CLUSTER_RETENTION` with a rolling deploy. The rule is `retention >= 60 × (planned tx/s + 0.5)`.

- 131,072 frames covers 2,000 tx/s. That is the cap in frames.
- The release gate also refuses a value whose frames, at the mean frame size of the members, pass 48 MiB. At a mean
  of 300 bytes, 131,072 frames (about 39.3 MB) fit. A mean above 384 bytes gives a lower value: the refusal names it.
- The gate refuses a lower value unless `KARDAMOM_ALLOW_MUST_MATCH=retention`.
- A rate above about 2,180 tx/s needs the retained file or a change to the replay path.

### A rollback after step 9

After the first snapshot of version 18, a sealer member of a release that reads only up to version 17 stops at its
next start. Nothing in the deploy refuses that rollback yet:

- `just rollback` (`tasks/revert.yml`) runs no release gate.
- A deploy of an older manifest from this checkout reads `reads_max` from the `formats.toml` of this checkout, not of
  the older release.

The snapshot check of the rollback is in work in #783. Until it ships, check by hand before any rollback of the
sealer:

1. Read `snapshotLatest` of each member:

   ```sh
   for i in 0 1 2; do curl -s "http://sealer-$i.node.dc1.consul:40205/status" | jq '{memberId, snapshotLatest}'; done
   ```

2. Read `reads_max` of `[format.sealer-snapshot]` in the `formats.toml` of the release to roll back to:
   `git show <revision>:formats.toml`.
3. If a member reports `snapshotLatest` above that `reads_max`, do not roll the sealer back. Fix forward.

### After a rollback of the egress

An egress of an older release stores references but does not raise its reference mark. So after a rollback of the
egress, the mark can be too low, and R is not true.

1. The release gate refuses the move of a reader to the egress (step 5), and the upgrade check refuses feature 5
   (step 8), while any egress allocation runs a job version without the meta `ref_mark`.
2. Deploy the release of the egress again.
3. Write down B, the highest `kardamom_executor_block_number` at the time of that deploy. Every frame that the older
   egress stored is at or below B.
4. Do not do step 5, 8 or 11 until each reader and each executor is past B:
   - `kardamom_batcher_last_posted_block` and `validator_committed_block` above B;
   - for step 11, `kardamom_executor_block_number` minus 32 above B.

After the switch of step 10, the sealer refuses each new reference, so a rollback of the egress then adds none.

This rule is manual. A follow-up can remove it: `Store::open` of the egress can rebuild the mark from the segments, as
the highest reference of every segment (`RefMark::top` over each one), and not only read the side file. Then a
deploy of the new release after a rollback gives a true R again.

### What staging needs (kardamom-infra)

The deploy does not change these. Apply them before step 6.

- On each sealer node, the directories `/opt/kardamom/state`, `/opt/kardamom/checkpoints` and `/opt/kardamom/mirror`,
  writable by the executor and the state mirror.
- On each sealer node, room in the tmpfs `/opt/kardamom/aeron-mount` for the shared media driver: the term buffers
  of the executor streams and the 24 MiB log of the feed.
- On each sealer node, memory for the member (heap plus 640 MB), the shared driver (768 MB), the executor
  (1,536 MB), the state mirror (512 MB), and the page cache of the state database.
- The ports 9004, 9014, 9024 (executor) and 9007 (state mirror) free on each sealer node and open between the nodes.
- No disk for a retained file, and no secret for a token.
- The executor nodes stay: the egress runs there.

### To go back

- An executor goes back to reference mode at any time: leave it out of `KARDAMOM_CARRY_EXECUTORS`.
- Keep `KARDAMOM_SEQUENCER_ADMISSION=on` while a member holds feature 5. The release gate refuses the v1 loop then, unless
  `KARDAMOM_ALLOW_MUST_MATCH=admission`: the v1 loop offers record type 0, and the sealer refuses every such transaction.
- Feature 5 can turn off with an upgrade transaction (activation time `18446744073709551615`). The check allows any
  activation time for feature 5.
- Feature 3 or 4 off stops decision version 5, and with it the rules of feature 5. The upgrade check refuses it
  while a member holds feature 5. Turn feature 5 off first, and empty `KARDAMOM_CARRY_EXECUTORS`.
- A fleet rebuild from a seed keeps the rules: the seed carries the decision rules at its head, and a seeded member
  runs them. Check that `kardamom-reconstruct` logs `sealer seed written` with `decision_version=5` and
  `carry_window=true`. A seed of version 1 holds no rules: before a rebuild from such a seed, set
  `KARDAMOM_SEALER_VERSION_FLOOR=0`, empty `KARDAMOM_CARRY_EXECUTORS`, and send features 3, 4 and 5 again.

## Clear

The procedure is done when:

- every member reports `carryWindow` true and `kardamom_sealer_window_exact` 1;
- every executor runs in carry mode on the node of its member, and its member reports `kardamom_sealer_feed_state` 3;
- the canary ran for 24 hours with no lost receipt, no `UnjoinableReference` error, and no move of
  `kardamom_egress_copy_mismatch_total`.

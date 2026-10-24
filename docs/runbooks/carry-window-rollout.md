# carry-window-rollout

## Cause

This procedure turns on the carry window (feature 5, `FEATURE_CARRY_WINDOW`) on a live chain, and moves the
executors to the bytes that the sealer carries. The chain does not stop, and no member restarts all at once.

- The exact window bounds the heap and the snapshot of each member at any DA lag.
- Each executor runs on the node of its sealer member and reads the member feed of that member.
- After the switch, no reader of the order meets a transaction reference that it cannot join.

The order is not circular. Each step needs only the steps before it. Do not start a step until every check of the
step before it passes. Step 8 is an upgrade transaction on L1: it needs the explicit approval of the operator.

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
`<egress-node>` is a node that runs an egress instance (`nomad job status egress`). Set every switch in the
environment of the staging deploy, then deploy. Each step keeps the switches and the floor of the steps before it.

Each gate of the carry window runs only when the deploy makes its move: a new floor, a reader that does not read the
egress yet, or an executor that does not carry yet. A later deploy with the same values reads nothing. A gate that
refuses changes no job. The messages and what to do are in [deploy-rollback.md](deploy-rollback.md).

## Steps

### 1. Deploy the release

Deploy the release with every switch of the carry window off and no `KARDAMOM_SEALER_VERSION_FLOOR`.

Checks:

- Each member: `snapshotReadsMax` 18, `decisionVersion` 5, `snapshotLatest` 16, `retainedStore` `heap`, and
  `kardamom_sealer_window_exact` 0.

  ```sh
  curl -s "http://sealer-$i.node.dc1.consul:40205/metrics" \
    | grep -E '^kardamom_sealer_(window_exact|feature_state|feed_state|full_ref_below_v5_total|ref_refused_total) '
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

### 2. Set the sequencer version floor

Deploy with `KARDAMOM_SEALER_VERSION_FLOOR=5`, the lowest `decisionVersion` of the members. The preflight refuses a
value other than empty or 0 to 5. When the floor changes to a value above 0, the release gate refuses:

- a sealer job that Nomad does not know, or a member that does not run;
- a floor above the lowest `decisionVersion` of the members;
- an executor, validator, batcher or egress job without the meta `relayed_full_ref`, or a live allocation of an older
  job version. Without the egress job, the egress passes.

Checks:

- Each sequencer: `kardamom_sequencer_sealer_version` 5 (metrics port 9001 + 10 × lane).
- `kardamom_sealer_full_ref_below_v5_total` stays 0 on every member.

### 3. Watch the references stop

Restart one sequencer of each pair: `nomad alloc restart <alloc>`. Then watch R on each egress instance for 30
minutes under the canary:

```sh
curl -s "http://<egress-node>.node.dc1.consul:9013/metrics" | grep -E '^kardamom_egress_ref_frame_max_(index|block) '
```

Checks:

- R does not move on any instance.
- Write down the highest `kardamom_egress_ref_frame_max_index` and its block.

### 4. Move the batcher and the validator to the egress

Before the deploy:

- `kardamom_batcher_last_posted_block` (batcher, port 9002) and `validator_committed_block` (validator, port 9006)
  are above the highest `kardamom_egress_ref_frame_max_block`.

Deploy with `KARDAMOM_BATCHER_EGRESS=on` and `KARDAMOM_VALIDATOR_EGRESS=on`. For each reader whose registered job does
not carry the meta `reads_egress` yet, the release gate refuses:

- an egress job without the meta `ref_mark`, or a live egress allocation of an older job version;
- an egress instance that does not report `kardamom_egress_ref_frame_max_index` and `kardamom_egress_ref_frame_max_block`;
- a reader whose block above is not above the highest `kardamom_egress_ref_frame_max_block`. A reader that does not
  answer counts as `-1`.

Checks:

- `kardamom_batcher_egress_cursor_index` and `kardamom_validator_egress_cursor_index` are above the highest R.
- The batcher and the validator jobs carry the meta `reads_egress = "true"`.
- The validator agrees, and the batcher posts.

### 5. Apply the infrastructure change, then turn on the member feed

Apply the change of kardamom-infra (see "What staging needs" below). Then deploy with `KARDAMOM_MEMBER_FEED=on`.
The sealer rolls member by member, and the shared media driver starts on each sealer node. The feed is not a setting
that every member must match, so it needs no `KARDAMOM_ALLOW_MUST_MATCH`. The sealer job changes, so the release gate
reads every member: each member must run.

Checks:

- Each member: `kardamom_sealer_feed_state` 1 (idle), and `feed` `idle` in its status.
- The `aeron` job runs on each sealer node: `nomad job status aeron`.

### 6. Move the executors to the sealer nodes

Deploy with `KARDAMOM_MEMBER_FEED=on` and `KARDAMOM_EXEC_ON_SEALERS=on`. The preflight refuses
`KARDAMOM_EXEC_ON_SEALERS=on` without `KARDAMOM_MEMBER_FEED=on`, or with an executor count other than the sealer
count. The executor job moves one executor at a time: `executor-2`, then `executor-1`, then `executor-0`. Each step
waits for the health check of the moved executor.

A known short window: the sequencers, the ingresses and the batcher already ask `sealer-<i>` for the nonce queries
and the block references. Until `executor-1` has moved, only `executor-2` answers them. The window lasts one step of
the move, a few minutes. This is acceptable on staging. Do the move at low load.

Checks:

- Each group `executor-<i>` runs on `sealer-<i>`: `nomad job status executor`.
- `kardamom_executor_block_number` (port 9004 on each sealer node) follows the sealed head.
- Receipts land (the canary). `kardamom_egress_copy_mismatch_total` does not move.

### 7. Run the upgrade check

Pick the activation time: about 30 minutes ahead, at low load. Then run:

```sh
just sealer-upgrade-check staging 5 <activation_ms>
```

It must pass. It refuses unless:

- every member runs, reads snapshot version 18, runs decision version 5, and keeps its frames on the heap
  (`retainedStore` `heap`);
- the sequencer job has `sealer_version_kept`, the batcher and the validator jobs have `reads_egress`, and the egress
  job has `ref_mark`, each with every live allocation on that job version;
- every egress instance reports its reference mark;
- a batcher and a validator run, and every `kardamom_batcher_egress_cursor_index` and
  `kardamom_validator_egress_cursor_index` is above the highest `kardamom_egress_ref_frame_max_index`.

### 8. Send the upgrade transaction on Sepolia

**This step needs the explicit approval of the operator. Do nothing in this step without that approval.**

The Safe signs `ETHLockbox.initiateUpgrade(5, <activation_ms>)`, with the time of step 7.

Checks:

- Each member logs `cluster FEATURE-ACTIVATION` with `feature=5` and `outcome=STORED`.
- `kardamom_sealer_feature_state{feature="5"}` is 1 on each member.
- From here, the members write snapshot version 18, and the release gate refuses a rollback of the release.

### 9. Watch the switch

At the activation time:

Checks:

- Each member logs `cluster CARRY-WINDOW` with `carryWindow=true windowOnly=true`, at the same block.
- `kardamom_sealer_feature_state{feature="5"}` is 2, and `kardamom_sealer_window_exact` is 1, on each member.
- `kardamom_sealer_retained_frames` falls within `retention` frames on each member.
- `kardamom_sealer_snapshot_bytes` falls at the next snapshot.
- `kardamom_sealer_ref_refused_total` stays 0. Receipts keep landing.

### 10. Move the executors to carry mode, one at a time

Deploy with `KARDAMOM_CARRY_EXECUTORS=0`. After its checks pass, deploy with `0,1`, then with `0,1,2`. The preflight
refuses a list with an index that repeats or has no member, or a list without `KARDAMOM_EXEC_ON_SEALERS=on` and
`KARDAMOM_EGRESS=1`, or with `KARDAMOM_EXEC_CURSOR=on`. For each listed executor that does not carry yet, the release
gate refuses:

- a group `executor-<i>` that does not run on the sealer nodes;
- an executor job without the meta `carried_ref_fault`;
- a member that does not run, or that reports `carryWindow` false;
- the member `cluster-<i>` that reports the feed `off`;
- an egress job without `ref_mark`, a live egress allocation of an older job version, or an egress instance that does
  not report its reference mark;
- a `kardamom_executor_block_number` of the executor, minus 32, that is not above the highest
  `kardamom_egress_ref_frame_max_block`.

Checks, for each executor that moved:

- The executor logs `kardamom-executor: canonical entries via the member feed`.
- `kardamom_sealer_feed_state` is 3 (live) on the member of the executor.
- Receipts land. `kardamom_egress_copy_mismatch_total` does not move. No `UnjoinableReference` error.

### 11. Soak for 24 hours

Run the canary for 24 hours. Record these values, for the trigger of the retained file:

- `kardamom_step_seconds{step="sealer.snapshot"}`, p99 and max.
- `kardamom_sealer_snapshot_bytes` and `kardamom_sealer_retained_bytes`.
- The GC pauses of the sealer, p99 and max, from `/opt/kardamom/cluster/jvm-pauses.log`, and the heap after a GC.
- `retention` divided by the transaction rate.

### Before a load test above 1,000 transactions a second

Raise `KARDAMOM_CLUSTER_RETENTION` with a rolling deploy. The rule is `retention >= 60 × (planned tx/s + 0.5)`.

- 131,072 frames covers 2,000 tx/s. That is the cap in frames: the preflight refuses a larger value.
- When the value changes, the release gate also refuses a value whose frames, at the mean frame size of the members,
  pass 48 MiB. At a mean of 300 bytes, 131,072 frames (about 39.3 MB) fit. A mean above 384 bytes gives a lower value:
  the refusal names it.
- The gate refuses a lower value than the registered sealer unless `KARDAMOM_ALLOW_MUST_MATCH=retention`.
- A rate above about 2,180 tx/s needs the retained file or a change to the replay path.

### After a rollback of the egress

An egress of an older release stores references but does not raise its reference mark. So after a rollback of the
egress, the mark can be too low, and R is not true.

1. The release gate refuses the move of a reader to the egress (step 4) and the move of an executor to carry mode
   (step 10), and the upgrade check refuses feature 5 (step 7), while any egress allocation runs a job version without
   the meta `ref_mark`.
2. Deploy the release of the egress again.
3. Write down B, the highest `kardamom_executor_block_number` at the time of that deploy. Every frame that the older
   egress stored is at or below B.
4. Do not do step 4, 7 or 10 until each reader and each executor is past B:
   - `kardamom_batcher_last_posted_block` and `validator_committed_block` above B;
   - for step 10, `kardamom_executor_block_number` minus 32 above B.

After the switch of step 9, the sealer refuses each new reference, so a rollback of the egress then adds none.

### What staging needs (kardamom-infra)

The deploy does not change these. Apply them before step 5.

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
- Feature 5 can turn off with an upgrade transaction (activation time `18446744073709551615`). The check allows any
  activation time for feature 5.
- Feature 3 or 4 off stops decision version 5, and with it the rules of feature 5. The upgrade check refuses it
  while a member holds feature 5. Turn feature 5 off first, and empty `KARDAMOM_CARRY_EXECUTORS`.
- Before a fleet rebuild from a seed, set `KARDAMOM_SEALER_VERSION_FLOOR=0`, and empty `KARDAMOM_CARRY_EXECUTORS`. A
  seeded member runs decision version 3 with no feature. Send features 3, 4 and 5 again.

## Clear

The procedure is done when:

- every member reports `carryWindow` true and `kardamom_sealer_window_exact` 1;
- every executor runs in carry mode on the node of its member, and its member reports `kardamom_sealer_feed_state` 3;
- the canary ran for 24 hours with no lost receipt, no `UnjoinableReference` error, and no move of
  `kardamom_egress_copy_mismatch_total`.

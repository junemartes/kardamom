# Stateless ingress on cpx32, and the egress as the transaction source

- Status: revision 3, approved by the design review on 2026-10-24; accepted. The open questions take
  the recommended answers (the user's instruction of 2026-10-24: drive to completion without
  questions). Not implemented.
- Revision 2 answers the design review of revision 1 (findings R1 to R10). Revision 3 answers the
  review of revision 2 (findings S1 to S8). Section 14 maps each finding to its change.
- The user decided on 2026-10-24:
  - All services get their transaction data from the egress.
  - No Aeron archive records `tx_data` or `tx_deposits`. Only the sealer commits and keeps the
    data streams. The `l1_blocks` archive (the L1 follower nodes) and the `exec_txs` archive (the
    executor nodes) stay.
  - The notifier is a separate service. It does not run on the ingress nodes. It runs on two new
    cpx12 nodes (1 shared AMD vCPU, 2 GB each), one instance on each node. notifier-0 comes first,
    and notifier-1 follows after a load test of notifier-0. The fallback is cpx22.
  - Staging keeps two fixed ingress nodes, ingress-0 and ingress-1. Both change to cpx32
    (4 shared AMD vCPU, 8 GB, 160 GB) in place, one at a time. There is no autoscaling for now.
  - The L1 indexer (on da-watcher-0) and the Redis replica (on redis-1) stay where they are.
  - The archiving media driver runs only on a node that records a topic.
- Builds on:
  - `docs/specs/2026-10-23-carry-activation.md` (the carry spec): its rollout (section 12) moves
    the batcher and the validator to `--egress`, turns on feature 5, and moves the executors to the
    member feed with the egress as the second source.
  - `docs/specs/2026-10-21-one-way-pipeline.md` (the one-way spec): D5, D7, D11, P8 and P13.
  - `docs/specs/2026-10-23-hot-path-agents.md` (the hot-path spec): the verify agent (section 5)
    and Q5.
  - #792 (the split of a verify batch by a minimum per worker, and `ingress.recover_wait`), open.
  - #794 (carry C9, the carry runbook `docs/runbooks/carry-window-rollout.md`), open.
  - kardamom-infra #67 (load-0), merged. kardamom-infra #79 (ingress-0 and ingress-1 on cpx32 with
    `keep_disk`, and notifier-0 on cpx12), approved. kardamom-infra #78 (the on-node
    directories), open.
- Section 9 keeps the elastic ingress pool as later work, with its blockers.

## 1. The problem

### 1.1 What runs on staging today

| Node | Type | Services | Driver | The archive records |
|---|---|---|---|---|
| ingress-0, ingress-1 | cx33 | ingress, notifier | archiving | `tx_data` |
| sequencer-0, sequencer-1 | cx33 | sequencer | archiving | nothing |
| da-watcher-0 (roles da-watcher, indexer) | cx23 | da-watcher, L1 indexer | archiving | `tx_deposits`, `l1_blocks` |
| executor-0..2 (node containers) | dedicated | executor, egress (2 of 3), state mirror | archiving | `exec_txs` |
| redis-0/1, batcher-0, monitoring-0, validator-0 | cx23, cx33 | Redis, batcher, monitoring, validator | archiving | nothing |
| sealer-0..2 (node containers) | dedicated | sealer member | the cluster's own driver | none |
| load-0 | ccx33 (8 dedicated vCPU) | the load tool; no Nomad node | none | none |

Sources:

- The server types: kardamom-infra `terraform/hetzner/staging.tfvars:83-97`, and load-0 from #67
  (merged). The role sets:
  kardamom-infra `ansible/inventories/staging/hosts.ini:113-144`.
- The driver: the `aeron` system job runs one `ArchivingMediaDriver` on each node that is not a
  control node or a sealer node (`deploy/cluster/nomad/aeron.system.nomad.hcl:147-172`, `:247`).
  The header says that the workers' archive "simply goes unused" (`:25-30`).
- The recorded topic of each node comes from its role set
  (`deploy/cluster/ansible/roles/nomad/templates/nomad.hcl.j2:85`, `:89`).
- The notifier runs on the ingress nodes: it constrains on `meta.role = ingress`
  (`deploy/cluster/nomad/notifier.nomad.hcl:108-118`).
- The egress: 2 instances on the executor nodes, `distinct_hosts`
  (`deploy/cluster/nomad/egress.nomad.hcl:73-83`, `:158-169`). Staging runs it
  (`KARDAMOM_EGRESS=1` in the variables of the infra workflow).
- On staging, the readers still read the sealer. The infra workflow passes
  `KARDAMOM_VALIDATOR_EGRESS` and `KARDAMOM_BATCHER_EGRESS` from repository variables
  (`.github/workflows/deploy-staging.yml:114-115`). The first is `off`, and the second is not set. `--ack validated` is on (`KARDAMOM_INGRESS_ACK=validated`).

### 1.2 Findings

- **F1. The deposits already travel by value in the order.** The sealer orders an epoch record
  (`RT_EPOCH`, record type 2, `crates/cluster-adapter/src/wire/mod.rs:504`). The record holds
  its deposits by value (`crates/types/src/epoch.rs:227`, `:244`). The sealer relays it from the
  canonical id on (`RelayShape.java:19-20`) and keeps it as it is (`EgressFrames.java:89-101`).
  A legacy `DepositRef` stops a reader (`crates/engine/src/reader/threads.rs:566-583`).
- **F2. The egress already carries the deposits.** The executor tee forwards each kind-1 and
  kind-2 frame byte for byte on `exec_frames` (`crates/engine/src/reader/tee.rs:1-17`, `:61-65`).
  The egress stores whole frames (`crates/egress/src/store.rs:1-16`). It serves them on
  `egress_order` (`crates/egress/src/main.rs:161-165`) and on the range read
  `GET /v1/frames` (`crates/egress/src/http.rs:4-12`). So both carry each epoch with its deposit
  bytes. Nothing is missing for the deposits.
- **F3. Nothing reads the `tx_deposits` archive.** `ArchiveRefetcher::fetch_deposits`
  (`crates/log/src/refetch.rs:328-381`) has no caller. `JoinRecovery` has only
  `recover_tx_data` (`crates/engine/src/reader/ports.rs:104-131`). The batcher says so
  (`crates/batcher/src/live/run.rs:322-325`). The only live reader of `tx_deposits` is the
  sequencer origin pump (`crates/sequencer/src/bin/kardamom-sequencer/feeds.rs:705-723`). A lost
  epoch is repaired by the da-watcher, which publishes again what the sealer did not confirm
  (`crates/da_watcher/src/watcher/follow.rs:1-17`). So "the executor restart replay of deposits"
  does not exist. Several comments still say it does (section 4.4).
- **F4. The batcher rebuild in egress mode runs only on the refetch flags.** In egress mode the
  batcher opens no lane and no refetch (`run.rs:403-422`). A refused replay goes to the rebuild
  from references (`run.rs:682-684`, `:825-866`). The rebuild needs the `tx_data` archives
  (`crates/batcher/src/live/rebuild.rs:128-145`). It runs only because the batcher job still
  passes the refetch flags (`deploy/cluster/nomad/batcher.nomad.hcl:278-279`). When 5.1 removes
  them, `join_recovery` is `None`, and the rebuild already ends in `ReplayUnavailable`
  (`run.rs:845-852`, `:825-838`). So no new halt rule is needed.
- **F11. A reader of the egress reads one instance at a time.** The reader holds one base URL,
  `http://kardamom-egress.service.<dc>.consul:8548`
  (`deploy/cluster/ansible/roles/workloads/defaults/main.yml:201`), and Consul resolves it for each
  connection (`crates/engine/src/reader/carried/egress.rs:28-50`). A 410 from the instance that it
  reaches becomes `ClusterReplayUnavailable` (`crates/engine/src/reader/carried/mod.rs:322-331`),
  with no try of the other instance.
- **F12. A new egress instance has a high floor.** An instance with no peer that fills its place in
  time starts again after the next boundary of a copy (`crates/egress/src/owner.rs:289-317`,
  `:18-26`; `crates/egress/src/main.rs:96-106`). Its floor is then far above a cursor that the
  other instance still serves. The comment of the job says that such an instance cannot join
  (`egress.nomad.hcl:27-29`). That comment is stale.
- **F13. The egress reader has no second source.** `open_egress` pairs the egress transport with
  `NoSource` (`crates/engine/src/bin_support.rs:424-433`). `CarriedSubscription` takes two
  transports (`carried/mod.rs:59-80`). The transports today are the member feed, the egress, a
  memory transport and `NoSource` (`carried/transport.rs:64`, `carried/feed.rs:51`,
  `carried/egress.rs:60`, `carried/memory.rs:105`). No transport reads a cluster session.
- **F5. The void vote and the record-lag guard are already off on staging.** A member on decision
  version 5 drops the void request (kind 6) and the recorded cursor (kind 9) as unknown kinds
  (`cluster/sealer-service/README.md:619`). The record-lag guard has a default budget of 0, which
  is off (`CanonicalSealerState.java:125`), and the deploy sets no budget. Its input is the
  `exec_txs` recording, not `tx_data` (`crates/executor/src/exec_stream/mod.rs:1-23`).
- **F6. The sequencer nodes record nothing.** Their `archive_topics` is empty
  (`nomad.hcl.j2:85`). So they can run a plain driver now. The same holds for the redis, batcher,
  monitoring and validator nodes.
- **F7. The aeron job has one value of the idle cap for every node.** `idle_cap_us` is a job
  variable (`aeron.system.nomad.hcl:66-76`, `:257`), from `KARDAMOM_IDLE_CAP_US`, default 50
  (`deploy/cluster/ansible/roles/workloads/defaults/main.yml:58`, `:236`).
- **F8. The notifier names the ingress nodes.** Its peers are `ingress-${i}.node.<dc>.consul`
  (`notifier.nomad.hcl:100`), and `--instance-count` is a fixed `"2"` (`:185`). It reserves
  1,024 MB (`:230-233`).
- **F9. The ingress waits for its recorders.** With `--archive-durability`
  (`deploy/cluster/nomad/ingress.nomad.hcl:307`), it starts one recorder for each `tx_data` lane
  and serves only after they are ready (`crates/ingress/src/service.rs:186-199`, `:255-263`).
- **F10. A new driver cannot start at once after the old one.** A new driver refuses to start
  while the CnC heartbeat of the old one is younger than the driver timeout. So Nomad restarts a
  driver 5 s after the stall tolerance, 15 s at the default (`aeron.system.nomad.hcl:97-102`).

## 2. Goals and limits

- Goal: every reader of user transactions and deposits takes them from the egress, or from the
  member feed of its paired sealer member. No reader joins `tx_data`.
- Goal: no archive records `tx_data` or `tx_deposits`. A node that records no topic runs a plain
  media driver.
- Goal: the notifier runs on its own two nodes, with no knowledge of the ingress nodes.
- Goal: ingress-0 and ingress-1 run on cpx32, with a verifier that fits a 4-vCPU node.
- Limit: every deploy is backward compatible (#661). This spec changes no chain rule, no wire
  format and no replicated state.
- Limit: the `l1_blocks` and `exec_txs` archives stay.
- Limit: the live `tx_data` stream stays. The ingress publishes on it and the sequencers read
  it. Only its recording goes.
- Non-goal: the elastic ingress pool (section 9).
- Non-goal: a move of the L1 indexer or the Redis replica.
- Non-goal: the code cleanup of P13 of the one-way spec. Section 11 lists it as the last PR.

## 3. The design in short

1. **Part A, the egress as the source.** The carry rollout moves the executors, the validator and
   the batcher to the egress. This spec adds three changes:
   - the egress reader asks every egress instance before it reports a range as unavailable, and
     takes the lowest floor (4.3);
   - the batcher in egress mode gets the cluster session of the sealer as its second source
     (4.3);
   - the halt of the batcher names the egress floor, and skips the fetch of block references.
2. **Part B, no `tx_data` and no `tx_deposits` archive.** The ingress and the da-watcher stop
   their recordings. The archive lists and the refetch go from the deploy. The aeron job runs the
   archiving driver only on a node that records a topic.
3. **Part C, the notifier on its own nodes.** Two cpx12 nodes, `notifier-0` and `notifier-1`, of
   the new role `notifier`. The job constrains on the role, takes its count from
   `notifier_count`, and names its peers by the notifier nodes. It reserves 512 MB. Its driver
   runs with an idle cap of 1,000 µs. notifier-0 comes first, with a count of 1. notifier-1
   follows, with a re-shard to a count of 2 (6.3).
4. **Part D, the ingress nodes on cpx32.** An in-place resize, one node at a time. The verifier
   gets a setting for its worker count: 3 on cpx32.
5. **The order.** The measure of a driver switch in CI, the release, notifier-0, the notifier
   move, the resize, notifier-1, the plain driver where nothing records, then Part B after step 10
   of the carry rollout.
6. **After Part B, feature 5 is one way** on that network, and the sequencer floor stays 5 (5.5).

## 4. Part A: the egress as the only source of transaction data

### 4.1 What the egress carries

- `egress_order` and the range read carry every kind-1 record frame and every kind-2 boundary
  (F2). That is `RT_TX` with its envelope, `RT_EPOCH` with its deposits, `RT_REMOTE_EPOCH`,
  `RT_VOID`, and, in reference mode only, `RT_TXREF`.
- After feature 5, the sealer refuses a type-0 reference (carry spec 4.3). So no `RT_TXREF`
  frame comes after R, the highest stored reference (carry spec 7.2).
- The range read answers a cursor below its floor with HTTP 410 and the floor in headers
  (`http.rs:183-189`, `store.rs:145-153`). The reader takes it as `REPLAY_UNAVAILABLE`
  (`crates/engine/src/reader/carried/egress.rs:163-165`, `:183-187`).
- The egress keeps a fixed number of blocks: `--keep-blocks`, 1,000,000 by default
  (`crates/egress/src/main.rs:66-67`, `crates/egress/src/owner.rs:359-372`). At one block each
  2 s that is about 23 days.
- What is missing: nothing for the deposits. The egress has no gauge of its floor. This spec adds
  `kardamom_egress_floor_block` and `kardamom_egress_head_block` (section 4.3).

### 4.2 Each reader: today and after

| Reader | Today (staging) | On a restart, today | On a gap, today | After the carry rollout and this spec |
|---|---|---|---|---|
| Executor | Reference mode: the cluster session, 8 `tx_data` lanes and the refetch (`crates/executor/src/bin/kardamom-executor/main.rs:289-324`) | `REPLAY_FROM` from the cursor of its state DB (`main.rs:268`). A refused replay stages a peer checkpoint (`crates/engine/src/bin_support.rs:727-760`). | A join miss refetches from the `tx_data` archives after 10 s (`crates/engine/src/reader/join.rs:126`), then votes a void or stops (`threads.rs:374-411`). | Carry mode: the member feed, then the egress range read, then a peer checkpoint (carry spec 6). No lane, no refetch (`main.rs:311-313`). A gap asks the source again from the cursor (`crates/engine/src/reader/carried/answer.rs:297-303`). |
| Validator | The cluster session, the lanes and the refetch (`crates/validator/src/bin/kardamom-validator/wiring/startup.rs:272-286`) | It resumes from its state DB (`startup.rs:184-191`). | Join miss: refetch, as the executor. A refused replay: a peer checkpoint in the process (`wiring/revolve.rs:44-59`, `:104-118`). | `--egress` only (`crates/validator/src/bin/kardamom-validator/args.rs:60-65`). A gap re-reads the range. Below the egress floor: a peer checkpoint. No change in this spec. |
| Batcher | The cluster session, the lanes and the refetch (`run.rs:372-398`) | The spool, then the sealer replay, then a rebuild (`run.rs:569-575`). | A refused replay rebuilds from block references and the `tx_data` archives (`run.rs:825-866`). | `--egress` (`run.rs:403-422`): every egress instance, then the cluster session of the sealer (4.3). Below both: the halt `ReplayUnavailable`. |
| State mirror | `tx_receipts` of the executors (`crates/state-mirror/src/main.rs:1-12`) | It resumes, or rebuilds from the checkpoint of its executor (`crates/state-mirror/src/mirror.rs:176-196`). | A write outage above 10 s, the audit or an executor rollback starts a rebuild (`mirror.rs:20-22`, `:150-174`). | No change. It reads receipts and account rows, not transaction bytes. A move to `egress_receipts` is a separate change. |
| Sequencer | `tx_data` live, `tx_deposits` live | — | It has no refetch. A lost `tx_data` frame is not ordered. A lost epoch comes again from the da-watcher (F3). | No change. These are its inputs, not the order. |

### 4.3 The egress reader: every instance, then the sealer

**Every instance before a refusal (F11, F12).**

- The egress transport gets the list of the egress instances: `--egress-instances` (env
  `KARDAMOM_EGRESS_INSTANCES`), comma-separated base URLs. The deploy names every executor node,
  `http://executor-<i>.node.<dc>.consul:8548`, as the egress job does for its own peers
  (`egress.nomad.hcl:134-144`). A node with no instance refuses the connection, which costs one
  failed connect.
- The live stream and the first range read stay on the service URL of today.
- On a 410, the transport asks each other instance of the list for the same range, one after the
  other.
  - The first instance that serves the range becomes the source of this replay.
  - Only when every instance that answers gives a 410 does the transport report `Unavailable`,
    with the lowest floor of those answers.
  - An instance that does not answer is not a refusal. When no instance answers, the transport
    reports a failed connection, as today, and the reader asks again.
- This change is in the engine, so the validator and the carry-mode executors get it too.
- A metric: `kardamom_reader_egress_floor_retries_total`, the 410 answers that another
  instance served.

**The cluster session as the second source of the batcher (F13).**

- A new transport, `ClusterTransport`, reads the cluster session of the sealer: `REPLAY_FROM` at
  the cursor, then the live frames, as the executor in reference mode reads today. It implements
  `Transport` (`carried/transport.rs`).
- **A switch, off by default.** The job variable `batcher_second_source = cluster|none`, default
  `none`. With `none`, the batcher opens `NoSource` as today. With `cluster`, it opens
  `CarriedSubscription::new(egress, cluster, cursor)`: the egress first, the sealer second. The job
  meta `egress_two_sources = "true"` is set only with `cluster`.
  - Why off by default: before feature 5, the sealer window still holds type-0 references (carry
    spec 1.3, F2). A 410 from every egress instance would then send the batcher to the sealer, which
    answers with a reference. With the `Unjoinable` verdict of carry C5 (#785), the batcher would
    stop with `UnjoinableReference`, not with the halt of today.
  - The deploy turns it on at step 4 of 5.4, after every executor is in carry mode and every
    reader cursor is past R. The preflight of the switch refuses `cluster` unless every member
    reports `carryWindow` true.
- With `cluster`, the batcher opens the two sources: the egress
  first, the sealer second. Today it opens `NoSource` as the second source
  (`bin_support.rs:424-433`), and keeps a cluster session only to publish the posted cursor
  (`run.rs:403-422`). The same session serves both.
- After feature 5, each `RT_TX` frame of a heap member holds its bytes (carry spec 1.1), and no
  type-0 reference comes (carry spec 4.3). So the sealer window is a source that needs no join.
- **The window is short.** Under the carry window, a member keeps exactly `retention` frames, and
  it no longer keeps the frames above the posted head (carry spec 4.4). At 65,536 frames that is
  about 30 to 60 s at the planned rates. The DA-lag guard allows 10,000 unposted blocks, about
  5.5 hours at one block each 2 s. So the second source covers a double egress loss only when the
  posted cursor of the batcher is within one window of the sealed head. In normal operation the
  batcher posts often enough for that. In a DA outage it does not.
- **The retention rule.** This spec adds one rule to the preflight of carry C6: `retention` must
  also cover the frames between two posts of the batcher at the planned rate, with a margin of 2.
  The rule makes the second source cover the normal case. It does not cover a DA outage.
- **The hazard.** The carry spec keeps the bytes on the cluster sessions for now, and plans a later
  change that strips them (carry spec 2, non-goal 3). That change must keep the bytes on the
  session of the batcher, or give the batcher another second source.
- **The residual risk.** A DA outage longer than the window, plus a loss of both egress stores,
  leaves no copy of the unposted blocks except the `exec_txs` recordings, which no reader reads.
  Section 13 (Q5) asks the user.
- The validator keeps `NoSource` as its second source. Below the egress floor it takes a peer
  checkpoint, as today.

**The halt.** When both sources refuse, the batcher halts with `ReplayUnavailable`. With the
refetch flags gone (5.1), the rebuild already ends there (F4). The change (E1, size S):

- the halt text names the lowest egress floor and the sealer floor;
- in egress mode the batcher does not fetch the block references, because no archive can fill
  them.

**Why the halt is rare.** The DA-lag guard refuses user records when the sealed head is more than
10,000 blocks past the posted head (`cluster/sealer-service/README.md:246-251`). The egress keeps
1,000,000 blocks (`crates/egress/src/main.rs:66-67`). So the posted cursor is inside the store of
an instance with a full history, except in a DA outage of about 23 days.

**The gauges and the checks.**

- The egress exports `kardamom_egress_floor_block` and `kardamom_egress_head_block`.
- An alert fires when `kardamom_batcher_last_posted_block` is below the floor of one instance: that
  instance cannot serve the batcher, so one more loss halts it.
- The deploy refuses `--keep-blocks` below `20 x daLagBudgetBlocks`.

### 4.4 Stale comments

These comments describe a `tx_deposits` replay that the code does not have. The docs PR (E9)
corrects them:

- `deploy/cluster/nomad/executor.nomad.hcl:17-24`
- `crates/da_watcher/src/bin/kardamom-da-watcher.rs:214-216`
- `crates/da_watcher/src/lib.rs:7-12`
- `crates/ingress/src/service/settings.rs:45-47`
- `crates/executor/src/bin/kardamom-executor/main.rs:575`
- `crates/validator/src/bin/kardamom-validator/main.rs:5`
- `crates/validator/src/bin/kardamom-validator/wiring/startup.rs:241-242`
- `crates/batcher/src/live/run.rs:526`

## 5. Part B: no `tx_data` and no `tx_deposits` archive

### 5.1 What goes

| Item | Where | Change |
|---|---|---|
| `--archive-durability` of the ingress | `ingress.nomad.hcl:304-307` | removed from the job. The flag stays in the binary until P13. |
| The `tx_data` recorders | `crates/ingress/src/service/recorders.rs` | not started. Code removal in P13. |
| `--archive-durability` of the da-watcher | `deploy/cluster/nomad/da-watcher.nomad.hcl:174-177` | removed from the job. The da-watcher then publishes with no wait for a recording (`kardamom-da-watcher.rs:575-631`). |
| The topics `tx_data` and `tx_deposits` | `nomad.hcl.j2:85` | `archive_topics` becomes `exec_txs` on an executor node, else empty. |
| `tx_data_archive_endpoints`, `tx_deposits_archive_endpoints` | `deploy/cluster/config/channels.toml.tpl:66-79`, `deploy/cluster/ansible/roles/contract/vars/main.yml:159-160`, `deploy/cluster/ansible/tests/test_channels.py:65` | removed. The `l1_blocks` list (`channels.toml.tpl:82`) stays. |
| The join-miss refetch | `--replay-destination-endpoint` and `--archive-control-response-endpoint` of the executor (`executor.nomad.hcl:324-334`), the validator (`validator.nomad.hcl:308`) and the batcher (`batcher.nomad.hcl:275-279`), with their ports | removed from these jobs. The da-watcher keeps them: it reads the history of `l1_blocks` (`da-watcher.nomad.hcl:162`). |
| The archiving driver on the ingress and sequencer nodes | `aeron.system.nomad.hcl` | a plain driver (5.2). |
| The chaos cases of the `tx_data` archive | `driver_loss`, `tx_data_wipe` and `corruption` (`crates/chaos/src/cases/archive.rs:34-54`, `:231`) | E8 aims `driver_loss` and `corruption` at an `exec_txs` archive of an executor node, and retires `tx_data_wipe`. |
| The contract check of the `tx_data` list | `deploy/cluster/ansible/roles/contract/vars/main.yml:159-160` | removed with the lists (E7). |

The `l1_blocks` archive on the follower nodes and the `exec_txs` archive on the executor nodes
stay. On staging, da-watcher-0 has the role `indexer`, so it keeps the archiving driver for
`l1_blocks` (`hosts.ini:144`, `nomad.hcl.j2:89`).

### 5.2 The driver rule

**The rule.** A node runs the archiving driver only when it records a topic. Every other node runs
a plain `MediaDriver` with no archive.

After the decisions of the user, the archiving nodes are the L1 follower nodes (`l1_blocks`) and
the executor nodes (`exec_txs`). The plain nodes are the ingress, sequencer, notifier, redis,
batcher, monitoring and validator nodes, and the da-watcher node of a profile where it has no
`indexer` role.

**The node meta.** `nomad.hcl.j2` gets a third meta value next to `archive_topics`:

```text
aeron_driver = archive  when archive_topics or archive_topics_follower is not empty
               quiet    when 'notifier' is in roles_set
               plain    otherwise
```

**The job picks the driver with three task groups.** The aeron job gets a `dynamic "group"` over a
local map of three kinds. Each group constrains on `${meta.aeron_driver}`:

| Group | Main class | Archive ports, volume and Consul record | Idle cap | Memory |
|---|---|---|---|---|
| `aeron` (kind `archive`) | `io.aeron.archive.ArchivingMediaDriver` | yes, as today (`aeron.system.nomad.hcl:192-305`) | `idle_cap_us` | 768 MB |
| `aeron-plain` (kind `plain`) | `io.aeron.driver.MediaDriver` | no | `idle_cap_us` | 768 MB, then the measured value |
| `aeron-quiet` (kind `quiet`) | `io.aeron.driver.MediaDriver` | no | `idle_cap_quiet_us`, default 1,000 | `driver_quiet_memory_mb`, default 768, then about 512 |

- Why groups, and not one task with the class from the node meta: the archive group owns the
  archive ports and the `kardamom-aeron-archive` service. One task with a class from the meta
  would still register an archive record with no archive behind it. A group per kind removes the
  record and the ports from a plain node.
- The group `aeron` keeps its name and its task. It gets one more constraint:
  `${meta.aeron_driver} = archive`.
- **The meta on a live node.** A Nomad constraint on a missing attribute fails. So before the new
  job, every live node gets `aeron_driver = archive` as dynamic node meta
  (`nomad node meta apply`, Nomad 1.9.5, `deploy/cluster/ansible/group_vars/all.yml:6`). Then
  the release changes no live driver (section 10, step 1). A later switch of one node is one
  `nomad node meta apply` on that node. The agent template writes the computed value, so a node
  that is built later gets its kind at its first start.
- Each group keeps the update block of today: `max_parallel = 1` and a stagger of 30 s
  (`aeron.system.nomad.hcl:184-190`).

**The image holds both classes.** The image starts `${AERON_ARCHIVE_CLASS}` from `aeron-all.jar`
(`crates/log/docker/aeron/entrypoint.sh:64-81`, `crates/log/docker/aeron/Dockerfile:50`).
`aeron-all.jar` holds `io.aeron.driver.MediaDriver` too. Two changes to the entrypoint (E3):

- **The wait for the old driver.** Before it starts the JVM, the entrypoint checks `cnc.dat` in
  `AERON_DIR`. While the heartbeat of that file is younger than the driver timeout, it waits, up
  to the driver timeout plus 5 s. This applies to every kind.
  - **The mechanism.** The entrypoint is a shell script, so the check needs a main class. E3 adds
    a small class, `io.kardamom.aeron.DriverStale`, to the jar that the image already builds for
    its idle strategy (`crates/log/docker/aeron/Dockerfile:35-37`,
    `CappedBackoffIdleStrategy.java`). Its `main` calls `CommonContext.isDriverActive` on
    `AERON_DIR` with the driver timeout, and exits 0 when no driver is active. The entrypoint
    runs it in a loop. A read of the heartbeat offset of `cnc.dat` from the shell is not chosen:
    the offset is bound to the Aeron version.
  - **The test.** A shell test of the image, next to `idle-cap-cases.txt`: with a fresh `cnc.dat`
    of a running driver, the start waits. When that driver stops, the start follows within the
    driver timeout plus 5 s. With no `cnc.dat`, the start is at once.
  - **The common case is the full timeout.** The job sets no `aeron.dir.delete.on.shutdown`
    (`aeron.system.nomad.hcl:246-272`, `crates/log/docker/aeron/entrypoint.sh:64-81`), so a stopped driver leaves `cnc.dat` in place. Its last
    heartbeat is at the stop, so the new driver waits about the driver timeout: 10 s at the
    default. Today only Nomad's restart delay protects a restart of the same
  task (F10). It does not protect the start of a task of another group on the same `aeron.dir`.
- **The start wait.** It looks for `archive.catalog` and `cnc.dat` for 30 s
  (`entrypoint.sh:84-100`). With the plain class, it waits only for a new `cnc.dat`.
- The archive system properties are harmless to a plain driver.

**No consumer lists an archive on a plain node.**

- The Consul record `kardamom-aeron-archive` exists only in the archive group. So a plain node has
  none. The discovered archive lists (`plane.watch_archives`, `bin_support.rs:265-285`) then
  hold only the archive nodes.
- `channels.toml.tpl` renders its static lists from the tags of that record (`:78-82`). With no
  `tx_data` or `tx_deposits` tag left, those lists are empty before 5.1 removes them.
- The `l1_blocks` and `exec_txs` consumers do not change.

**A switch on a running node restarts the driver.** Every Aeron client on that node loses its
driver.

- **What Nomad does.** `nomad node meta apply` starts a node-update evaluation. Nomad stops the
  allocation of the old group and places the allocation of the new group at once, on the same
  `aeron.dir`. The new driver then finds the fresh heartbeat of the old one. Without the wait of
  E3, its first start fails, and the 30 s start wait adds to the downtime.
- **Why not `nomad alloc stop` first.** Nomad places a stopped allocation of a system job again on
  the same node, from the same group. So a stop before the meta change only adds one more restart.
- **The order on one node.**
  1. Check that the node runs the E3 image (the job meta `driver_waits_stale = "true"`).
  2. `nomad node meta apply -node-id <id> aeron_driver=<kind>`.
  3. The new driver waits for the old heartbeat to go stale (up to 15 s at the default timeout),
     then starts.
  4. The clients on the node stop on the driver timeout (10 s, `AERON_DRIVER_TIMEOUT`). Nomad
     restarts them after their restart delay (5 s, for example `ingress.nomad.hcl:182-187`). A
     client that starts before the new driver fails its start and restarts again.
  5. Check the services of the node before the next node.
- **Measure first.** The container shard measures the downtime of each kind (section 10.1). No
  staging node switches before that measure. The values below are estimates: the driver timeout,
  the stale wait, the driver start and one or two client restarts.

| Node class | Clients | Estimated downtime | Effect |
|---|---|---|---|
| ingress (one of two) | ingress | 30 to 60 s | The other ingress serves. The LB TCP check takes up to 30 s to drop the target (kardamom-infra `terraform/hetzner/main.tf:338-344`), so some connections fail. Do it at low load. |
| sequencer (one of a twin pair) | sequencer | 30 to 60 s | The twin offers each transaction too. |
| notifier | notifier | none | The notifier nodes start with the quiet driver. |
| validator, batcher | validator, batcher | 30 to 60 s | They pause and catch up from the egress. |
| redis, monitoring | none on Aeron | none for a service | The driver restarts alone. |

### 5.3 The void vote and the record-lag guard

- **The void vote.** A reader votes a void only after a join that every listed `tx_data` archive
  refused with `RangeAbsent` (`join.rs:149-178`, `threads.rs:374-411`). After the carry rollout,
  no reader joins. So no reader votes. On staging the sealer already drops the vote (F5).
  - The hazard of a drain is gone: today the deletion of the last archive with a range would
    make every later join of that range a void. With no join, there is no such void.
  - P13 removes `RangeAbsent`, `RefusedArchives`, the void vote, the voter ids and the reader
    side of `VoidLedger`.
- **The record-lag guard.** It measures the `exec_txs` recording of the executors, not
  `tx_data` (F5). It is off on staging by its budget and by version 5. This spec does not change
  it. The `exec_txs` archive stays.

### 5.4 The safe order

The order of the findings holds. This spec adds two steps (marked new):

1. The batcher and the validator read `--egress` (carry spec 12, step 4).
2. Feature 5 is on, with the window (carry spec 12, steps 8 and 9).
3. Every executor is in carry mode, and every reader cursor is past R (carry spec 12, step 10).
4. (new) The readers of 4.3 are deployed: the egress reader asks every instance. Then the deploy
   sets `batcher_second_source = cluster`, so the batcher has the cluster session as its second
   source. This is safe only now: every executor is in carry mode, and feature 5 refuses type 0.
5. The deploy removes the ingress recorder, `--archive-durability` of the ingress and of the
   da-watcher, and the `tx_data` and `tx_deposits` topics. The preflight of this step refuses
   unless:
   - the batcher and validator jobs have the meta `reads_egress = "true"` (carry spec 9), and
     every live allocation runs that version;
   - every member reports `carryWindow` true;
   - `KARDAMOM_CARRY_EXECUTORS` lists every executor;
   - the batcher job has the meta `egress_two_sources = "true"` (step 4);
   - the gates of 5.5 are in the release.
6. (new) The ingress nodes switch to the plain driver, one at a time (5.2).
7. The deploy removes `tx_data_archive_endpoints`, `tx_deposits_archive_endpoints` and the
   refetch flags and ports of the readers.
8. P13 removes the code, after step 7 on every network.

- **Why it is not circular.** Steps 1 to 3 need no archive change. Step 5 needs only steps 1 to 4.
- **Rollback.** Until step 5, a deploy turns each part back. After step 5, the way back of the
  carry runbook is closed (5.5).
- **The old recordings.** The recordings of `tx_data` on the ingress nodes and of `tx_deposits`
  on da-watcher-0 stay on disk under `/opt/kardamom/archive` after the switch. Delete them after
  the soak (section 10, step 11).

### 5.5 After Part B, feature 5 is one way

- **Why.** The carry runbook allows feature 5 off, and an executor back in reference mode, at any
  time (#794, `docs/runbooks/carry-window-rollout.md`, "To go back"). After Part B, both break the
  chain:
  - A reader in reference mode has no archive for a join miss.
  - With feature 5 off, the sealer orders type-0 references again (carry spec 4.4, "Off again").
    One sequencer restart with a floor below 5 then sends a type 0, and every carry-mode executor
    stops with `UnjoinableReference` (carry spec 6).
- **The rule.** After Part B, feature 5 stays on for the life of that network, and the sequencer
  floor stays 5.
- **The gates.** They apply while the ingress job has no `--archive-durability` (the job meta
  `tx_data_recorded = "false"`, set by E6):
  - the upgrade check refuses any new activation of feature 5 while it is on. That covers
    `2^64 - 1` (off), and a time ahead, which turns it off until that time (carry spec 4.1);
  - the deploy refuses `KARDAMOM_SEALER_VERSION_FLOOR` below 5;
  - the deploy refuses an empty `KARDAMOM_CARRY_EXECUTORS`, and a list that leaves out an executor.
- **The floor must be set on each deploy.** An empty `KARDAMOM_SEALER_VERSION_FLOOR` is 0 (carry
  spec 5). So after Part B every staging deploy must set the floor. The infra workflow passes it
  from a repository variable, `KARDAMOM_SEALER_VERSION_FLOOR=5` (infra follow-up I5). The gate
  then refuses a deploy with no value.
- **A fleet rebuild from a seed** needs floor 0 (carry spec 5). After Part B it also needs the
  `tx_data` archive back first, in this order:
  1. Set `aeron_driver = archive` on ingress-0, then on ingress-1, one at a time (5.2). The
     ingress nodes run the plain driver after Part B (section 10, step 9).
  2. Restore `archive_topics = tx_data` on the ingress nodes.
  3. Deploy the ingress with `--archive-durability`. Without step 1, its start waits on recorders
     that no archive serves (F9).
  4. Follow the runbook. The gates then fall away by the job meta.
- **The runbook.** "To go back" of `docs/runbooks/carry-window-rollout.md` gets the same rule.
  C9 (#794) can take the line.

## 6. Part C: the notifier on its own nodes

> **Superseded in part (2026-10-25).** The client-events spec, decision 2
> (`docs/specs/2026-10-25-client-events.md`), removes the notifier webhooks. So the peers list,
> `--instance-count` and the webhook checks in this section are superseded (lines 123, 163,
> 544-549, 805-806, 860 and 904 of revision 3). The notifier keeps no state, and a change of
> its count moves nothing.

### 6.1 The infra change (kardamom-infra)

- **Two phases.** kardamom-infra #79 (approved) adds notifier-0 now. notifier-1 follows after a
  load test of notifier-0. Each phase is one static server in `terraform/hetzner/staging.tfvars`:

```hcl
notifier-0 = { role = "notifier", tier = "worker", server_type = "cpx12", private_ip = "10.20.0.58", placement_group = "services", keep_disk = true }
notifier-1 = { role = "notifier", tier = "worker", server_type = "cpx12", private_ip = "10.20.0.59", placement_group = "services", keep_disk = true }
```

- **The addresses.** #79 takes .58 and reserves .59 for notifier-1 in the address plan of
  `docs/staging-launch.md`. The plan uses .50-.52, .55-.57, .60 and .70 (load-0, from #67).
- **The placement group.** `services` holds 6 servers today (`staging.tfvars:92-97`). With the two
  notifier nodes it holds 8, below the limit of 10 (`main.tf:205-210`). A spread group puts
  the two notifier nodes on different hosts.
- **The role.** #79 adds `notifier` to the list of roles of `variables.tf:170`.
- **No public address.** The default of `public` is false. The notifier nodes route through gw-0.
- **The server budget.** #67 made it 18 with load-0. #79 sets 20: 14 static servers, notifier-1,
  and the pool maxima 2 + 2 (`main.tf:36`, `:68-73`). So notifier-1 needs no budget change.
- **The cores.** Before #79 the static servers use 38 shared cores against a project limit of 40
  (`staging.tfvars:14-15`). The resize of the ingress nodes adds none (4 to 4). notifier-0 makes
  39 and notifier-1 makes 40. load-0 (ccx33) counts against the dedicated quota, 8 vCPU. The API
  does not show the limits. Check them in the console before each apply (section 10).
- **The inventory.** `ansible/inventories/staging/hosts.ini`:

```ini
[notifier]
notifier-0 ansible_host=10.20.0.58 kardamom_node=notifier-0 role=notifier node_roles=notifier tier=worker node_index=0
notifier-1 ansible_host=10.20.0.59 kardamom_node=notifier-1 role=notifier node_roles=notifier tier=worker node_index=1
```

  #79 adds the line of notifier-0. The phase of notifier-1 adds its line.

  The group `notifier` goes into `[cloud:children]` (`hosts.ini:146-155`).
- **The directories.** (Superseded 2026-10-25: the webhooks are removed, and the notifier keeps no
  directory. See `docs/specs/2026-10-25-client-events.md`, decision 2.) The node build runs the
  `common` role, which makes `/opt/kardamom/notifier` with mode 0777 (`deploy/cluster/ansible/roles/common/tasks/main.yml:81-99`).
  #78 says that only a node build makes the directories. A new node is a node build, so #78 does
  not block this step.

### 6.2 The deploy change (kardamom)

- **The constraint.** `meta.roles` `set_contains` `notifier`, as the batcher does
  (`batcher.nomad.hcl:178-182`). It replaces `meta.role = ingress` (`notifier.nomad.hcl:108-111`).
- **The count.** A job variable `notifier_count`, default 2, with `distinct_hosts`. The deploy
  takes it from `KARDAMOM_NOTIFIER_COUNT`. `--instance-count` takes the same value
  (`notifier.nomad.hcl:185`). A preflight refuses a count above the number of Nomad nodes with
  the role `notifier`: with more allocations than nodes, the deployment never turns healthy.
- **The peers.** The Consul node records of the notifier nodes:
  `http://notifier-${i}.node.<dc>.consul:8547` for `i` in `range(var.notifier_count)`. They
  replace the ingress records (`notifier.nomad.hcl:100`). A node record exists before the
  allocation starts, so the list does not change with the life of an allocation.
- **The memory.** The reservation becomes 512 MB (`notifier.nomad.hcl:230-233`). The measured
  maximum over 6 h on the ingress nodes is 236 to 296 MiB. CPU: about 5 % at rest, and a maximum of
  40 to 65 % of one core.
- **The driver.** The notifier nodes run the `aeron-quiet` group (5.2). The idle cap of 1,000 µs
  adds up to about 1 ms to a notifier event. The notifier is off the transaction path. On the
  ingress nodes the driver sits at its 768 MB limit at 77 to 82 % CPU at rest, mostly a spin at the
  50 µs cap. On a notifier node the driver carries only the streams of the notifier, so about
  512 MB is expected.
- **The order of the memory.** Until it is measured, the driver reservation stays 768 MB. On a 2 GB
  node that leaves little room (the fit below). So: measure the driver on notifier-0 in the ramp,
  then lower `driver_quiet_memory_mb`, then add notifier-1 (section 10, step 5).
- **The fit on cpx12.** 2 GB of RAM holds:
  - the notifier, 512 MB;
  - the driver, 768 MB at first, about 512 MB after the measure;
  - the node exporter, 64 MB (`deploy/cluster/nomad/node-exporter.system.nomad.hcl:90-93`);
  - the Consul, Nomad and Docker agents;
  - the Aeron tmpfs `/opt/kardamom/aeron-mount`. It is RAM too. Its limit is 2 GB on every node
    (`aeron_tmpfs_size`, `deploy/cluster/ansible/group_vars/all.yml:173`; the mount at
    `roles/common/tasks/main.yml:106-112`). The term buffers in it are charged to the cgroup of
    the process that creates them (`aeron.system.nomad.hcl:309-317`).
- **The tmpfs on the notifier nodes.** It holds only the images of the notifier streams:
  `tx_status`, `tx_errors` and `egress_receipts`, from their publishers. Each image is 3 terms of
  4 MiB (`aeron.system.nomad.hcl:252-255`). An estimate of 10 to 15 images gives 120 to 180 MiB.
  The notifier nodes set `aeron_tmpfs_size: 384m` as a host variable (#79 or the phase of
  notifier-1). The measure of section 10 checks the use.
- **The fallback** is cpx22, by an in-place resize.
- **The notifier poll loops** keep their own waits: the job sets no `KARDAMOM_IDLE_CAP_US`, so each
  loop keeps its default (`crates/log/src/aeron_live/idle.rs:19-23`).
- **The README.** The node class table of `deploy/cluster/README.md:34` loses the notifier from
  the ingress row, and gets a `notifier` row.

### 6.3 The clients and the order

- **The WebSocket clients.** On staging the LB serves only the RPC port
  (kardamom-infra `main.tf:331-345`). No public client reaches `:8547`. The internal client is the
  canary, on `ws://kardamom-notifier.service.consul:8547` (`deploy/cluster/nomad/canary.nomad.hcl:141`).
  It follows the Consul service, so it reconnects to the new nodes with no change. The monitoring
  job scrapes `kardamom-notifier-metrics` through Consul (`monitoring.nomad.hcl:148-150`).
- **The state.** (Superseded 2026-10-25, from here to the end of phase 1's merge: the webhooks are
  removed, the notifier keeps no state, and a move or a count change moves nothing. See
  `docs/specs/2026-10-25-client-events.md`, decision 2.) `/opt/kardamom/notifier` holds the webhook
  subscriptions, the outboxes and the cursors (`crates/notifier/src/main.rs:79-85`). A registration is stored on both instances
  (`notifier.nomad.hcl:11-14`).
- **The shards follow the instance index, not the node.** The owner of a subscription is a
  rendezvous hash over `--instance-index ${NOMAD_ALLOC_INDEX}` and `--instance-count`
  (`notifier.nomad.hcl:184-185`, `crates/notifier/src/shard.rs:1-5`). The allocation on
  ingress-1 can have index 0. The move gives the indexes again.
- **The choice: merge, not a copy by index.** The move merges both directories onto the new node:
  - The subscriptions are the same on both (the peers list).
  - For each subscription, it takes the outbox and the cursor of the old owner, by the index of
    the old allocation (`nomad alloc status` gives `NOMAD_ALLOC_INDEX`).
  - Delivery is at least once, so a cursor that is behind sends an event again, and loses none.
  - A merge does not depend on which index lands on which node.
- **Phase 1, one node (notifier-0).** `notifier_count = 1`, so `--instance-count 1`, and every
  shard is on instance 0. The peers list names notifier-0 only.
  1. Silence the canary feed check.
  2. Stop the notifier job. Read the index of each old allocation first.
  3. Merge both old directories into `/opt/kardamom/notifier` on notifier-0.
  4. Deploy the job with the role constraint and `KARDAMOM_NOTIFIER_COUNT=1`.
  5. The WebSocket feed is down for a few minutes. Check the canary feed, then turn its check on.
- **Phase 2, the re-shard (notifier-1).** After the load test of notifier-0 and the apply of
  notifier-1:
  1. Stop the notifier job.
  2. Copy the directory of notifier-0 to notifier-1. The registrations of phase 1 are only on
     notifier-0, because its peers list named only itself.
  3. Deploy with `KARDAMOM_NOTIFIER_COUNT=2`. The rendezvous hash moves only the subscriptions
     whose owner changes (`shard.rs:1-5`). Each moved subscription resumes from its copied outbox
     and cursor on notifier-1.
  4. A registration reaches both instances again through the peers list.
- **Before the resize.** A resize powers off an ingress node. Today that also stops one notifier,
  which cannot move to the other ingress node (`distinct_hosts`). So the notifier moves first.

### 6.4 The notifier as its own class, or in the egress (one-way P8)

One-way P8 moves the client side of the notifier into the egress
(`docs/specs/2026-10-21-one-way-pipeline.md:312`). Section 10.6 of that spec keeps the notifier
separate for now (`:410-412`).

| | Its own class (this spec) | Folded into the egress |
|---|---|---|
| Hosts | two cpx12 nodes | the executor nodes, inside the cpuset of the executor |
| Load on the transaction path | none: a slow client costs only the notifier node | the WebSocket fan-out and the webhook outboxes share cores with the merge and the store |
| HA | two instances on two hosts | the egress instances, 2 today |
| Cost | two more servers, 2 cores, 2 server slots | none |
| Code | the job change of 6.2 | P8: a new client surface in the egress, the outbox store, a new public path |
| A failure | a notifier crash stops events, not receipts | a crash of the fan-out code stops a reader source of the chain |

Recommendation: its own class now. Revisit P8 when the egress runs on its own fleet, not on the
executor nodes.

## 7. Part D: the ingress nodes on cpx32

### 7.1 The resize

- **The change.** `server_type = "cpx32"` for ingress-0, then for ingress-1, in
  `staging.tfvars:86-87`. A server type change is not in `ignore_changes` (`main.tf:279-280`).
- **What the provider does (findings, provider 1.69.0).** A type change is an in-place update, not
  a replacement. The server powers off, the type changes, and the server starts again. The IPs
  and the LB target stay.
- **The disk.** On infra main, `hcloud_server.static` sets no `keep_disk` (`main.tf:243-293`), so
  the disk would grow from 80 to 160 GB, with no way back to cx33. #79 adds `keep_disk` to the
  server variable and sets it to true on ingress-0, ingress-1 and notifier-0. The disk then stays
  80 GB, and cx33 stays possible. After Part B the ingress keeps no recording, so 80 GB is enough.
- **load-0.** #67 has merged, so an apply from infra main keeps load-0. The plan of each apply must
  still show 0 to destroy.
- **One server at a time.** Each step on one node:
  1. Check that the other ingress is healthy, and that the load is low.
  2. Drain the Nomad node. The ingress stops with its drain: `/health` answers 503, new submits
     get `Draining` (`crates/ingress/src/proxy/mod.rs:553-567`). With `--ack validated` no
     submit is parked.
  3. Apply the type change of this node only.
  4. The node starts, and joins Consul and Nomad with the same name and IP. Make it eligible
     again.
  5. Check: the ingress allocation is healthy, the LB target is healthy, and the canary passes.
- **The LB check is TCP** (`main.tf:338-344`): 10 s, 3 retries. The 503 of a drain does not take
  the target out. The listener closes at the end of the drain, and the TCP check drops the target
  within about 30 s. An HTTP check on `/health` is in the later list (section 9). For two fixed
  nodes and a planned resize at low load, the TCP check is accepted.
- **The kill timeout.** The ingress asks for 40 s (`ingress.nomad.hcl:229`). Nomad caps a kill
  timeout at the client `max_kill_timeout`, which the deploy does not set. The Nomad default is
  30 s. With `--ack validated` nothing parks, so 30 s is enough.

### 7.2 The verifier

**The evidence.** Measured per node at about 1,700 tx/s (the ramp of 2026-10-24, #792):

- The recovery CPU is only 0.15 to 0.27 of a core.
- The rest of the node uses about 2.1 to 2.3 cores, without the notifier and the archive:
  - the RPC and tokio workers, about 1.0 to 1.3;
  - the Aeron client and media driver threads, about 1.0. They spin even at rest.
- The recovery of one transaction costs 76 µs (EPYC) to 120 µs (Skylake) (#792).
- After the resize both nodes are EPYC, so the two sides of a load test compare.

**The design choice: a setting for the workers, no pin.**

- `--verify-workers <n>` (env `KARDAMOM_INGRESS_VERIFY_WORKERS`), a `NonZeroUsize` parsed once at
  the CLI. Without it, the rule of today: half of `available_parallelism`, at most 8
  (`crates/ingress/src/sig_verify.rs:293-303`). On a 4-vCPU node that is 2.
- The deploy sets 3 on cpx32: a job variable `ingress_verify_workers`.
- The split rule of #792 stays. #792 replaces the fixed threshold (16 today, `sig_verify.rs:112`)
  with a minimum per worker: a batch gets one more worker only for about 5 more recoveries. So a
  batch of `n` requests uses `min(workers, n / 5)` workers, and at least one.
  - With 3 workers, a batch splits in 3 only from about 15 requests. At 1,700 tx/s per node a
    batch holds about 10 requests (#792), so most batches use 2 workers, and the third serves the
    peaks.
  - The setting is the ceiling of the rule, not a fixed split.
- Then the verify agent of the hot-path spec (section 5) can follow, if its stage 0 picks the
  ingress.
- Measure in the ramp, before and after:
  - `kardamom_step_seconds{step="ingress.recover"}`;
  - `kardamom_step_seconds{step="ingress.recover_wait"}` (new in #792);
  - `kardamom_queue_depth{queue="ingress.recover_intake"}`;
  - `node_schedstat_waiting_seconds_total`: the staging node exporter runs every default
    collector (`deploy/cluster/ansible/roles/workloads/tasks/main.yml:159`,
    `node-exporter.system.nomad.hcl:32-41`), and `schedstat` is one of them.

**Why no pin.** An exclusive pin of 3 cores (Nomad `cores = 3`, or thread affinity) leaves 1 core
for the rest, which uses about 2.1 to 2.3 cores. So the `tx_data` publish and the receipt path
would queue. The hot-path spec says no pin on cloud vCPUs (Q5). A pin stays an experiment on one
node, if the numbers show that the verifier still competes for the CPU (Q1).

## 8. Failure modes

| Case | What happens | Mitigation |
|---|---|---|
| One egress instance down | The other serves the range read and `egress_order`. The restarted instance fills from its peer (`crates/egress/src/main.rs:96-106`). | 2 instances, `distinct_hosts`. |
| One egress instance with a high floor (a fresh or rebased instance, F12) | A reader that reaches it gets a 410. The transport asks the other instance, which serves the range (4.3). | E1a. The counter `kardamom_reader_egress_floor_retries_total`. |
| Every egress instance down | The batcher reads the sealer window (4.3), then waits. The validator stops moving. The carry executors run on the member feed, but cannot recover below the sealer window. The sequencers get no account rows, so admission refuses the senders with pending transactions (one-way spec 6.3). The notifier gets no receipts (`--receipts-from-egress`). The DA-lag guard stops user records after 10,000 unposted blocks. | The alerts of the egress. Restart an instance with its store. |
| Both egress stores lost | The new instances start after the next boundary of a copy (F12). The blocks between the posted head and that boundary have no egress copy. With `batcher_second_source = cluster`, the batcher reads them from the sealer window, but only when its posted cursor is within one window of the sealed head: `retention` frames, about 30 to 60 s at the planned rates (4.3). Else it halts. In a DA outage longer than the window, no copy is left except the `exec_txs` recordings (Q5). The validator takes a peer checkpoint. | E1b, the `retention` rule of 4.3, Q5. |
| An egress behind | The readers lag. The distance gauge `kardamom_egress_head_distance_frames` and its alert show it. No loss. | One-way spec 6.4. |
| The batcher below every egress floor and the sealer window | It halts with `ReplayUnavailable` and both floors (4.3). | The floor alert of 4.3. `--keep-blocks` is far above the DA-lag budget. |
| A driver switch on one node | Its clients restart, an estimated 30 to 60 s (5.2). | The stale wait of E3. One node at a time, at low load. The measure in the container shard first. |
| Feature 5 off after Part B | Type-0 references come back, and a reference reader has no archive. The carry executors stop. | The gates of 5.5 refuse it. |
| The second ingress fails during a resize | No ingress serves RPC until one is back. | Low load. Check the other ingress first. Stop the step if it is not healthy. |
| A resize fails or hangs | The node stays off. One ingress serves. | Hetzner console. The other ingress carries the load. |
| One notifier node lost (phase 2) | The other instance serves the WebSocket feed. The webhook shard of the lost instance waits: a shard moves only when the instance count changes (`notifier.nomad.hcl:11-14`). | The node comes back. Staging accepts the wait. |
| The notifier node lost (phase 1) | No notifier runs. No events, no webhooks. Receipts still land. | The node comes back. Phase 2 restores the HA. |
| A notifier count above the notifier nodes | The deployment never turns healthy. | The preflight of 6.2. |
| cpx12 too small | The notifier or the driver is killed for memory, or the events lag. | The ramp check (section 10). The fallback is cpx22. |
| A lost `tx_data` frame after Part B | The sequencer does not order it, as today: the sequencer has no refetch (F3). The client sees no receipt. | No change. One-way D1. |

## 9. Later: the elastic ingress pool

The user's last pool shape was min 2 and max 3 cpx32. These blockers stay open:

- **A deploy resets the count.** `register.yml:36-46` posts `/v1/jobs` with no `PreserveCounts`.
  `wait_allocations.yml:26` reads the job count, and the fake Nomad in
  `deploy/cluster/ansible/tests/test_deploy.py:287` uses it.
- **The LB.** The targets are the static `rpc_target` set, with a precondition of at least 2
  (`main.tf:350-363`). A pool needs a `label_selector` target (`group-id=kardamom-staging-ingress`,
  `use_private_ip`) and an HTTP check on `/health`, so that a drain (503) takes the node out.
- **The drain.** The ingress job has no `shutdown_delay`. It needs one above the LB detection
  (about 30 to 40 s). `kill_timeout` 40 s is above the Nomad default `max_kill_timeout` of 30 s,
  which no config changes.
- **The system jobs.** The aeron job sets no `node_pool`, so a node in a pool gets no driver. The
  node exporter sets `node_pool = "all"` (`node-exporter.system.nomad.hcl:51`).
- **The plugin.** The autoscaler role builds the hcloud-server plugin itself, and its URL and
  checksum are empty (kardamom-infra `ansible/roles/autoscaler/defaults/main.yml:12-14`). The role
  asserts both (`tasks/main.yml:6-18`). The plugin config sets no public network key
  (`autoscaler.hcl.j2:50-56`). The pool user data assumes no public address
  (`terraform/hetzner/user-data.yaml.tftpl:6-9`), but the Hetzner API gives one by default.
- **#78.** A new on-node directory reaches only a node that is built after it.
- **The ingress id.** `--ingress-id ${NOMAD_ALLOC_INDEX}` (`ingress.nomad.hcl:282`). A canary
  could reuse an index (not confirmed).
- **Fixed counts.** `ingress.nomad.hcl:170` (`count = 2`, a literal). `monitoring.nomad.hcl:119`
  and `canary.nomad.hcl:78` iterate `var.ingress_count`, so a pool must keep that value current.
  The chaos suite (`crates/chaos/src/harness.rs:112`, `:137`,
  `:257-258`; `crates/chaos/src/cases/archive.rs`). Part C removes the notifier from this list.
- **The pool floor.** `variables.tf:109` needs `min >= 2`, and `autoscaler_ingress_min` is 2. Min 2
  fits.
- **The budget.** Each pool slot needs a server slot and 4 cores.
- **The signal (estimates).** Accepted tx/s per ingress
  (`rate(kardamom_ingress_tx_accepted_total[1m])`), out above about 1,000 per node for 1 to 2
  min, in below about 400 per node for 10 to 15 min, a cooldown of 10 min, a step of 1. A guard on
  `ingress.recover` or `ingress.recover_intake`. CPU is a poor signal: the driver spins at rest.
- **The tests.** A chaos shard for a scale-out and a scale-in under load.

## 10. The staging rollout

> **Superseded in part (2026-10-25).** The client-events spec, decision 2
> (`docs/specs/2026-10-25-client-events.md`), removes the notifier webhooks. So the peers list,
> `--instance-count` and the webhook checks in this rollout are superseded (lines 123, 163,
> 544-549, 805-806, 860 and 904 of revision 3). The notifier keeps no state, and a change of
> its count moves nothing.

Each step names its checks. Do not start a step until the checks of the step before pass.
**A step marked "needs the user's explicit approval" does not start without it.**

0. **The switch case first.** Run the switch case of E4 on the existing container profile: the
   archive driver on every node, then one node of each kind switched to plain and back. It
   measures the downtime of a driver switch. No staging node switches its driver before this
   measure passes. The no-archive profile belongs to E8.
1. **Release 1.** It holds E1, E1a, E1b, E2, E3, E4 and E5 (section 11), with every switch at
   today's value. `batcher_second_source` is `none`. Before the aeron job, apply `aeron_driver = archive` to every live node (5.2),
   so every node keeps the archive group.
   - Every `aeron` allocation keeps its id: no driver restarted.
   - The notifier still runs on the ingress nodes. The canary passes.
2. **Infra #79, notifier-0.** #67 (load-0) has merged. Apply notifier-0 only (`-target`).
   **Needs the user's explicit approval: a Terraform apply that creates a paid server.**
   - Before: the core limit in the console covers 39 shared cores. The plan adds 1 server and
     destroys 0.
   - After: notifier-0 joins Consul and Nomad with `meta.roles = notifier` and
     `meta.aeron_driver = quiet`. The `aeron-quiet` group runs on it.
3. **The notifier move, phase 1** (6.3): `KARDAMOM_NOTIFIER_COUNT=1`, the merge of both old
   directories on notifier-0. It runs before any resize.
   - One notifier allocation, on notifier-0. The canary feed check passes.
     `kardamom-notifier-metrics` is scraped.
4. **Infra #79, the resize** (7.1): ingress-0, then ingress-1, one apply each, at low load, each
   with a plan that changes one server type and destroys 0. **Needs the user's explicit approval:
   a Terraform apply and a paid server type, once for each server.**
5. **The notifier check in the next ramp** (the load test of notifier-0).
   - Read the notifier event latency (the canary feed lag, and the time from a receipt to its
     event) and the node CPU of notifier-0.
   - Read the driver memory and the tmpfs use on notifier-0. Set `driver_quiet_memory_mb` to the
     measured peak plus a margin, about 512.
   - If notifier-0 runs out of CPU or memory: resize it to cpx22. **Needs the user's explicit
     approval.**
   - Then notifier-1 (I4). **Needs the user's explicit approval: a Terraform apply that creates a
     paid server.** Check the core limit first (40 shared). Then the re-shard of phase 2 (6.3)
     with `KARDAMOM_NOTIFIER_COUNT=2`. Check: two notifier allocations, one on each node, and a
     webhook registration on one instance reaches the other.
6. **The verifier.** After #792: deploy `ingress_verify_workers = 3`. Run the ramp. Compare the
   values of 7.2 with the ramp of 2026-10-24.
7. **The plain driver where nothing records.** Set `aeron_driver = plain` on sequencer-0, then
   sequencer-1, then validator-0, batcher-0, monitoring-0, redis-0 and redis-1, one at a time, at
   low load.
   - Each node: the `aeron-plain` allocation runs, the services on it are healthy, and the node
     has no `kardamom-aeron-archive` record. The downtime is within the measure of step 0.
   - Each sequencer: `kardamom_sequencer_sealer_version` 5, receipts land.
8. **The readers of 4.3.**
   1. After step 4 of the carry rollout: deploy the validator and the batcher with
      `--egress-instances`. Restart one egress instance with an empty store. The counter
      `kardamom_reader_egress_floor_retries_total` moves, and no reader halts.
   2. After step 10 of the carry rollout (every executor in carry mode, every reader cursor past
      R): deploy the batcher with `batcher_second_source = cluster` (5.4, step 4). Its preflight
      must pass. The job meta `egress_two_sources` is `"true"`.
9. **Part B** (5.4), after step 10 of the carry rollout:
   - Deploy E6. Its preflight must pass. The gates of 5.5 are on from this deploy.
   - Set `aeron_driver = plain` on ingress-0, then on ingress-1.
   - Consul has no record with the tag `tx_data` or `tx_deposits`. The rendered
     `channels.toml` of a reader lists no `tx_data` and no `tx_deposits` archive.
   - The ingress starts with no recorder. The da-watcher publishes epochs with no recording.
   - Receipts land. The validator agrees. The batcher posts.
   - The gates: an upgrade check of feature 5 off is refused, and a deploy with floor 4 is
     refused.
10. **The cleanup deploy.** Deploy E7.
11. **Soak** under the canary (Q6). Then delete the old `tx_data` recordings on the ingress nodes
    and the `tx_deposits` recordings on da-watcher-0. **Needs the user's explicit approval: it
    deletes data on staging nodes.**

### 10.1 Tests

- **Rust unit tests.**
  - The egress transport: a 410 from one instance and frames from the other serve the range. A 410
    from every instance reports `Unavailable` with the lowest floor. A refused connect is not a
    refusal. One instance that does not answer and one 410 is not `Unavailable`.
  - `ClusterTransport`: a replay from the cursor, then the live frames, on a scripted session.
  - The batcher in egress mode: the egress below the floor, the sealer window serves. Both below
    the floor: the halt `ReplayUnavailable` names both floors, and no block references are
    fetched.
  - The batcher in cluster mode keeps the rebuild.
  - The egress floor and head gauges move with a prune and with an append.
  - `--verify-workers`: 0 is refused at the parse. With no value, the rule of today. With 3, a
    batch of 10 uses 2 workers and a batch of 15 uses 3, under the rule of #792.
- **Java and shell tests of the image** (E3): a start next to a fresh `cnc.dat` waits, and starts
  when the heartbeat is stale. A plain start waits for `cnc.dat` only.
- **Deploy tests** (`deploy/cluster/ansible/tests/test_deploy.py`, `test_channels.py`):
  - `aeron_driver` for each role set: `archive` with `indexer` or `executor`, `quiet` with
    `notifier`, `plain` otherwise.
  - The aeron job: each group constrains on its kind. Only the group `aeron` has the archive ports
    and the `kardamom-aeron-archive` service. A node with `aeron_driver = archive` keeps the
    allocation of today.
  - `archive_topics` is empty on an ingress node and on a da-watcher node (E6).
  - `channels.toml` renders no `tx_data` or `tx_deposits` list (E7).
  - The notifier job: the role constraint, `count = notifier_count`, `--instance-count` equal to
    the count, the peers `notifier-<i>`, 512 MB. A count above the notifier nodes is refused.
  - The ingress job has no `--archive-durability`. The da-watcher job has none.
  - The preflight of 5.4 refuses with one reader off the egress, with one executor not in carry
    mode, and with the batcher meta missing. A refusal writes no job.
  - The gates of 5.5: with `tx_data_recorded = "false"`, refuse feature 5 off, a new activation of
    feature 5 with a time ahead, a floor below 5, and an empty or partial
    `KARDAMOM_CARRY_EXECUTORS`. With the archive back, the gates do not apply.
  - The `retention` rule of 4.3 against the post interval of the batcher.
  - `--keep-blocks` below `20 x daLagBudgetBlocks` is refused.
- **Infra tests** (kardamom-infra `terraform/hetzner/tests`): #79 has them for notifier-0 and
  `keep_disk`. I4 adds notifier-1 and the services group of 8.
- **Multi-process e2e** (`crates/e2e`): a new scenario on the stack of the carry scenario
  (carry spec 11.3), with no archive except `exec_txs`:
  1. Send transfers and a deposit.
  2. Restart the validator and the batcher with cursors below the sealer window. Both recover
     from the egress range read. The deposit executes once.
  3. Restart a carry executor. It reads the epoch with its deposits from the egress.
  4. Wipe the store of one egress instance and restart it. The readers read the other instance.
  5. Stop both egress instances for 30 s. The batcher reads the sealer window, then the egress
     again. The validator waits, then resumes.
- **The container shard.** The `carry` shard of carry spec C8 gets a no-archive profile: every
  node except the executor and follower nodes runs `aeron-plain`, and the notifier runs on its own
  node class. Cases under load:
  - switch the driver of one node of each kind, and measure the downtime (5.2, step 0 of section
    10; this case ships in E4);
  - restart one egress instance with an empty store;
  - stop one notifier node, and check the WebSocket feed of the other;
  - `driver_loss` and `corruption` on an `exec_txs` archive (5.1).
  - `tx_data_wipe` retires (5.1).
- **The elastic shard** (scale-out and scale-in under load) belongs to section 9.

## 11. Pull requests

Each PR passes `just style` and CI alone, and follows `docs/STYLE.md`.

| PR | Repo | Title | Depends on | Size |
|---|---|---|---|---|
| E1 | kardamom | batcher: the halt names both floors, and egress mode fetches no block references; egress: floor and head gauges; deploy: the `--keep-blocks` check and the floor alert | none | S |
| E1a | kardamom | engine: the egress transport asks every instance before `Unavailable`, the lowest floor, the retry counter; deploy: `--egress-instances` for the validator, the batcher and the carry executors | none | M |
| E1b | kardamom | engine, batcher: `ClusterTransport`, the cluster session as the second source of the batcher in egress mode behind `batcher_second_source` (default `none`), its preflight, the job meta `egress_two_sources` only with `cluster`; deploy: the `retention` rule against the post interval | E1a | M |
| E2 | kardamom | ingress: `--verify-workers`; deploy: `ingress_verify_workers` | #792 | S |
| E3 | kardamom | aeron image: the class `DriverStale` and the wait for a stale `cnc.dat` before the start, the start wait of the plain class, the shell test; job meta `driver_waits_stale` | none | S |
| E4 | kardamom | deploy: the aeron job by `aeron_driver` (three groups), the node meta, the meta apply of release 1, `idle_cap_quiet_us`, `driver_quiet_memory_mb`, tests; chaos: the container case that switches the driver of one node of each kind and measures the downtime | E3 | M |
| E5 | kardamom | deploy: the notifier on the `notifier` role: constraint, `notifier_count` with its preflight, peers, instance count, 512 MB, README | none | S |
| I1 | infra | #67, load-0 | none | merged |
| I2 | infra | #79: ingress-0 and ingress-1 on cpx32, `keep_disk`, notifier-0 on cpx12, the role, `server_budget = 20` | I1 | approved |
| I3 | infra | `aeron_tmpfs_size: 384m` on the notifier nodes | I2 | S |
| I4 | infra | notifier-1 on cpx12, the inventory line, the tests | I2, step 5 of section 10 | S |
| I5 | infra | the workflow: `KARDAMOM_SEALER_VERSION_FLOOR` from a repository variable, set to 5 | before step 9 of section 10 | S |
| E6 | kardamom | deploy: no `tx_data` and no `tx_deposits` recording: the ingress and da-watcher flags, `archive_topics`, the job meta `tx_data_recorded`, the preflight of 5.4, the gates of 5.5, tests | carry C6 (#791), E1, E1a, E1b, E4 | M |
| C9 | kardamom | #794 (open): the line of 5.5 in "To go back" of `docs/runbooks/carry-window-rollout.md` | none | S |
| E7 | kardamom | deploy: remove the two archive lists, the contract checks and the refetch flags and ports of the readers | E6 on staging | S |
| E8 | kardamom | e2e and chaos: the no-archive scenario, the no-archive profile of the `carry` shard, the cases of 10.1, `driver_loss` and `corruption` on `exec_txs`, `tx_data_wipe` retired | E1a, E1b, E4 to E7, carry C7, C8 | L |
| E9 | kardamom | docs: `docs/failure-modes.md`, `deploy/cluster/README.md`, `crates/log/README.md`, the stale comments of 4.4, the stale comment of `egress.nomad.hcl:27-29` (F12), a note at P8 and P13 of the one-way spec | E6 | S |
| E10 | kardamom | cleanup (part of one-way P13): `refetch.rs`, the recorders of `tx_data` and `tx_deposits`, `archive_join_recovery`, the batcher rebuild, `RangeAbsent`, the void vote | E7 on every network | L |

Sizes: S is up to about 200 lines, M up to about 600, L above that. E1, E1a, E2, E3 and E5 can
start at once. The release of step 1 holds E1 to E5. The switch case of E4 is the measure of step 0
of section 10.

## 12. Considered and not chosen

- **One aeron task with the main class from the node meta.** It keeps the archive ports and the
  Consul record on a node with no archive (5.2).
- **A node-meta idle cap.** It splits the value between the deploy and the Nomad agent template.
  The quiet group keeps it in the deploy.
- **The notifier in the egress now (P8).** Section 6.4.
- **One cpx22 with two notifiers.** It loses the HA of two hosts. Two cpx12 keep it.
- **Keep the `tx_data` archive as a copy for the batcher.** It is the only reason for the
  ingress recorders and the archive on the ingress nodes. The sealer window (4.3) is a second
  source that needs no join.
- **`nomad alloc stop` before the meta change.** Nomad places the system allocation again (5.2).
- **A copy of the notifier directory by node name.** The shards follow the allocation index, not
  the node (6.3).
- **A fixed halt in egress mode with no second source (revision 1).** One fresh egress instance
  could halt the batcher while the other held the range (F11, F12).
- **Min 1 ingress.** The user set min 2 (section 9).

## 13. Open questions for the user

Each question takes its recommended answer (the user's instruction of 2026-10-24).

- **Q1. Pin 3 cores, or 3 workers with no pin?** Recommendation: 3 workers, no pin, and measure
  first (7.2). Decided: 3 workers, no pin, and measure first. Pin only as an experiment on one node, if `ingress.recover_wait` and
  `node_schedstat_waiting_seconds_total` show that the verifier still waits for the CPU.
- **Q2. The notifier state at the move.** Merge the old directories (6.3, a stop of a few
  minutes), or start empty (the webhook clients register again). Recommendation: merge. Decided: merge.
- **Q3. The notifier as its own class, or in the egress (P8).** Recommendation: its own class now.
  Revisit P8 when the egress has its own fleet (6.4). Decided: its own class now.
- **Q4. Feature 5 one way after Part B (5.5).** After Part B the carry window cannot turn off on
  that network, and a fleet rebuild from a seed needs the `tx_data` archive back first.
  Recommendation: accept it. The alternative is to keep the `tx_data` archive, which the user ruled
  out. Decided: accept it.
- **Q5. The second source of the batcher, and the risk that stays.**
  - The residual risk: a DA outage longer than the sealer window, plus a loss of both egress
    stores, leaves no copy of the unposted blocks except the `exec_txs` recordings, which no
    reader reads (4.3). Recommendation: accept it on staging. Open an issue for a recovery from the
    `exec_txs` recordings, or a third egress instance, before a production network.
    Decided: accept it on staging, with the issue before a production network.
  - The strip of the bytes: the carry spec plans a later change that strips the bytes from the
    cluster sessions. The batcher then loses its second source. Recommendation: that change keeps
    the bytes on the sessions that ask for them, and the batcher asks.
    Decided: the strip change keeps the bytes on the sessions that ask for them.
- **Q6. When to delete the old recordings** (section 10, step 11). Recommendation: after a soak of
  7 days, not 24 hours. Decided: after a soak of 7 days.

The answers of revision 1 that changed:

- `keep_disk` at the resize (Q2 of revision 1): #79 sets it to true. No longer open.
- The loss of both egress stores (Q5 of revision 1): no longer "accept". The batcher reads the
  sealer window as its second source (4.3).

## 14. Changes from the reviews

### 14.1 Revision 2: the review of revision 1

| Finding of the review of revision 1 | Change |
|---|---|
| R1 (major). One 410 from a fresh egress instance halts the batcher | F11, F12. Section 4.3: the transport asks every instance before `Unavailable`, and takes the lowest floor. Section 8: a new row. Section 11: E1a. |
| R2 (major). Both egress stores lost; the stale job comment | F12, F13. Section 4.3: the cluster session as the second source of the batcher, the `retention` rule, the hazard of the strip. Section 8: the row. Section 13: Q5 of revision 1 moves to this design; the new Q5. Section 11: E1b, and the comment in E9. |
| R3 (major). A meta switch places the new driver on a fresh heartbeat | Section 5.2: the stale wait in the entrypoint (E3), why not `nomad alloc stop`, the order on one node, the estimates of 30 to 60 s. Section 10: step 0 measures before any staging switch. Section 11: E3 and the case in E4. |
| R4 (major). Part B closes the way back of the carry window | Section 5.5: feature 5 one way, the floor stays 5, the gates, the seed rebuild. Section 5.4: step 5 and the rollback. Section 13: Q4. Section 11: E6 and C9 (#794). |
| R5 (major). The notifier shards follow the index; one node first | Section 6.3: the merge by the old index, phase 1 with count 1, phase 2 and the re-shard. Section 6.2: the count preflight. Section 8: the phase rows. Section 10: steps 2, 3 and 5. |
| R6. Stale facts | Header: #67 merged, #79 approved. Section 1.1: load-0. `KARDAMOM_BATCHER_EGRESS` from a repository variable (`deploy-staging.yml:114-115`). `da-watcher.nomad.hcl:174-177`. Section 9: `var.ingress_count`. Sections 6.1, 7.1, 8 and 10 for #67 and #79. |
| R7. E1 is not a new rule | F4. Section 4.3, "The halt". Section 11: E1 is S. |
| R8. `max_kill_timeout`; the rule of #792 | Section 7.1: the default is 30 s. Section 7.2: the minimum per worker. Section 10.1: the test. |
| R9. The tmpfs on cpx12 | Section 6.2: the tmpfs in the fit, its size on the notifier nodes. Section 10: the measure. Section 11: I3. |
| R10. The `tx_data` chaos cases and the contract test | Section 5.1: two rows. Section 10.1 and E8: aimed at `exec_txs`, or retired. |

### 14.2 Revision 3: the review of revision 2

| Finding of the review of revision 2 | Change |
|---|---|
| S1 (major). The second source of the batcher has no switch, and meets type-0 references before feature 5 | Section 4.3: `batcher_second_source = cluster|none`, default `none`, its preflight, the meta only with `cluster`. Section 10: step 1 (`none`) and step 8.2 (on after carry step 10). Section 11: E1b. |
| S2 (major). The sealer window is short under the carry window | Section 4.3: the window against the DA-lag budget, the retention rule kept, the residual risk. Section 8: the row of both stores lost. Section 13: Q5. |
| S3. The stale check needs a main class | Section 5.2: the class `DriverStale` in the jar of the image, why not the heartbeat offset, the shell test, and the full timeout because `cnc.dat` stays. Section 11: E3. |
| S4. The seed rebuild needs the archive driver back | Section 5.5: the order, with `aeron_driver = archive` on the ingress nodes before the recorders. |
| S5. Step 0 ran the profile of E8 before release 1 | Section 10: step 0 is the switch case of E4 on the existing container profile. |
| S6. An empty floor is 0 | Section 5.5: every deploy sets the floor. Section 11: I5. |
| S7. The driver memory before notifier-1 | Section 6.2: measure, then lower `driver_quiet_memory_mb`, then notifier-1. |
| S8. The `reader` label repeats `service` | Section 4.3 and section 10: the label goes (`crates/obs/src/lib.rs:219-220`). |

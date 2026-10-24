# Stateless ingress on cpx32, and the egress as the transaction source

- Status: revision 1, draft for review. Not implemented.
- The user decided on 2026-10-24:
  - All services get their transaction data from the egress.
  - No Aeron archive records `tx_data` or `tx_deposits`. Only the sealer commits and keeps the
    data streams. The `l1_blocks` archive (the L1 follower nodes) and the `exec_txs` archive (the
    executor nodes) stay.
  - The notifier is a separate service. It does not run on the ingress nodes. It runs on two new
    cpx12 nodes (1 shared AMD vCPU, 2 GB each), one instance on each node. The fallback is cpx22.
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
  - #792 (the split of a verify batch from 4 requests, and `ingress.recover_wait`), open.
  - kardamom-infra #67 (load-0), open, and kardamom-infra #78 (the on-node directories), open.
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

Sources:

- The server types: kardamom-infra `terraform/hetzner/staging.tfvars:83-97`. The role sets:
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
- On staging, the readers still read the sealer: `KARDAMOM_VALIDATOR_EGRESS=off`, and
  `KARDAMOM_BATCHER_EGRESS` is not set (the infra workflow `.github/workflows/deploy-staging.yml:104-121`
  and its repository variables). `--ack validated` is on (`KARDAMOM_INGRESS_ACK=validated`).

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
- **F4. The batcher still needs the `tx_data` archives in egress mode.** In egress mode it opens
  no lane and no refetch (`run.rs:403-422`). But a refused replay goes to the rebuild from
  references (`run.rs:682-684`, `:825-866`). The rebuild needs the `tx_data` archives, and fails
  without them (`run.rs:848-852`, `crates/batcher/src/live/rebuild.rs:128-145`). This is the one
  reader change that this spec adds.
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
   the batcher to the egress. This spec adds one change: a batcher in egress mode does not
   rebuild from the archives. A refused egress replay is a halt.
2. **Part B, no `tx_data` and no `tx_deposits` archive.** The ingress and the da-watcher stop
   their recordings. The archive lists and the refetch go from the deploy. The aeron job runs the
   archiving driver only on a node that records a topic.
3. **Part C, the notifier on its own nodes.** Two cpx12 nodes, `notifier-0` and `notifier-1`, of
   the new role `notifier`. The job constrains on the role, takes its count from
   `notifier_count`, and names its peers by the notifier nodes. It reserves 512 MB. Its driver
   runs with an idle cap of 1,000 µs.
4. **Part D, the ingress nodes on cpx32.** An in-place resize, one node at a time. The verifier
   gets a setting for its worker count: 3 on cpx32.
5. **The order.** Release, infra (load-0 first), the notifier nodes, the notifier move, the plain
   driver where nothing records, the resize, then Part B after step 10 of the carry rollout.

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
| Batcher | The cluster session, the lanes and the refetch (`run.rs:372-398`) | The spool, then the sealer replay, then a rebuild (`run.rs:569-575`). | A refused replay rebuilds from block references and the `tx_data` archives (`run.rs:825-866`). | `--egress` (`run.rs:403-422`). A refused egress replay halts with `ReplayUnavailable`, with no rebuild (4.3). |
| State mirror | `tx_receipts` of the executors (`crates/state-mirror/src/main.rs:1-12`) | It resumes, or rebuilds from the checkpoint of its executor (`crates/state-mirror/src/mirror.rs:176-196`). | A write outage above 10 s, the audit or an executor rollback starts a rebuild (`mirror.rs:20-22`, `:150-174`). | No change. It reads receipts and account rows, not transaction bytes. A move to `egress_receipts` is a separate change. |
| Sequencer | `tx_data` live, `tx_deposits` live | — | It has no refetch. A lost `tx_data` frame is not ordered. A lost epoch comes again from the da-watcher (F3). | No change. These are its inputs, not the order. |

### 4.3 The batcher in egress mode

- **The rule.** In egress mode the batcher does not rebuild. A refused replay from the egress is
  the halt `ReplayUnavailable`, with the floor of the egress in the halt text.
  - `recover_and_serve` (`run.rs:825-838`) gets the source of the refusal. With `--egress`, it
    returns the halt at once. It does not call `join_recovery` (`run.rs:286-294`).
  - The cluster mode keeps the rebuild, until P13.
- **Why a halt is enough.** The batcher cursor is the posted cursor. The DA-lag guard refuses user
  records when the sealed head is more than 10,000 blocks past the posted head
  (`cluster/sealer-service/README.md:246-251`). The egress keeps 1,000,000 blocks. So the posted
  cursor is inside the egress store, except in a DA outage of about 23 days. In such an outage
  the boundary ticks still seal empty blocks (`README.md:255-258`).
- **The gauges.** The egress exports `kardamom_egress_floor_block` and
  `kardamom_egress_head_block`. An alert fires when
  `kardamom_batcher_last_posted_block - min(kardamom_egress_floor_block) < 100000`.
- **The preflight.** The deploy refuses `--keep-blocks` below `20 x daLagBudgetBlocks`.

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
| `--archive-durability` of the da-watcher | `deploy/cluster/nomad/da-watcher.nomad.hcl:173-176` | removed from the job. The da-watcher then publishes with no wait for a recording (`kardamom-da-watcher.rs:575-631`). |
| The topics `tx_data` and `tx_deposits` | `nomad.hcl.j2:85` | `archive_topics` becomes `exec_txs` on an executor node, else empty. |
| `tx_data_archive_endpoints`, `tx_deposits_archive_endpoints` | `deploy/cluster/config/channels.toml.tpl:66-79`, `deploy/cluster/ansible/roles/contract/vars/main.yml:159-160`, `deploy/cluster/ansible/tests/test_channels.py:65` | removed. The `l1_blocks` list (`channels.toml.tpl:82`) stays. |
| The join-miss refetch | `--replay-destination-endpoint` and `--archive-control-response-endpoint` of the executor (`executor.nomad.hcl:324-334`), the validator (`validator.nomad.hcl:308`) and the batcher (`batcher.nomad.hcl:275-279`), with their ports | removed from these jobs. The da-watcher keeps them: it reads the history of `l1_blocks` (`da-watcher.nomad.hcl:162`). |
| The archiving driver on the ingress and sequencer nodes | `aeron.system.nomad.hcl` | a plain driver (5.2). |

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
`aeron-all.jar` holds `io.aeron.driver.MediaDriver` too. One change: the start wait looks for
`archive.catalog` and `cnc.dat` for 30 s (`entrypoint.sh:84-100`). With the plain class, it waits
only for `cnc.dat`. The archive system properties are harmless to a plain driver.

**No consumer lists an archive on a plain node.**

- The Consul record `kardamom-aeron-archive` exists only in the archive group. So a plain node has
  none. The discovered archive lists (`plane.watch_archives`, `bin_support.rs:265-285`) then
  hold only the archive nodes.
- `channels.toml.tpl` renders its static lists from the tags of that record (`:78-82`). With no
  `tx_data` or `tx_deposits` tag left, those lists are empty before 5.1 removes them.
- The `l1_blocks` and `exec_txs` consumers do not change.

**A switch on a running node restarts the driver.** Every Aeron client on that node loses its
driver.

- The order on one node: Nomad stops the old driver, waits for the restart delay (F10), and
  starts the new one. The clients stop on the driver timeout (10 s,
  `AERON_DRIVER_TIMEOUT`), and Nomad restarts them after their restart delay (5 s, for example
  `ingress.nomad.hcl:182-187`).
- The rollout switches one node at a time, by a change of its node meta (section 10). The
  downtime per node is an estimate, to be measured in the container shard (section 10.1):

| Node class | Clients | Estimated downtime | Effect |
|---|---|---|---|
| ingress (one of two) | ingress | 20 to 40 s | The other ingress serves. The LB TCP check takes up to 30 s to drop the target (kardamom-infra `terraform/hetzner/main.tf:338-344`), so some connections fail. Do it at low load. |
| sequencer (one of a twin pair) | sequencer | 20 to 40 s | The twin offers each transaction too. |
| notifier | notifier | none | The notifier nodes start with the quiet driver. |
| validator, batcher | validator, batcher | 20 to 40 s | They pause and catch up from the egress. |
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
4. (new) The batcher of 4.3 is deployed: in egress mode it never asks for an archive.
5. The deploy removes the ingress recorder, `--archive-durability` of the ingress and of the
   da-watcher, and the `tx_data` and `tx_deposits` topics. The preflight of this step refuses
   unless:
   - the batcher and validator jobs have the meta `reads_egress = "true"` (carry spec 9), and
     every live allocation runs that version;
   - every member reports `carryWindow` true;
   - `KARDAMOM_CARRY_EXECUTORS` lists every executor;
   - the batcher job has the meta `egress_halt_no_rebuild = "true"` (step 4).
6. (new) The ingress nodes switch to the plain driver, one at a time (5.2).
7. The deploy removes `tx_data_archive_endpoints`, `tx_deposits_archive_endpoints` and the
   refetch flags and ports of the readers.
8. P13 removes the code, after step 7 on every network.

- **Why it is not circular.** Steps 1 to 3 need no archive change. Step 5 needs only steps 1 to 4.
- **Rollback.** Until step 5, a deploy turns each part back. After step 5, a reader in
  reference mode still runs on a heap member, because each `RT_TX` frame holds its bytes (carry
  spec 1.1) and feature 5 refuses type 0. But it has no archive for a join miss.
- **The old recordings.** The recordings of `tx_data` on the ingress nodes and of `tx_deposits`
  on da-watcher-0 stay on disk under `/opt/kardamom/archive` after the switch. Delete them after
  the soak (section 10, step 12).

## 6. Part C: the notifier on its own nodes

### 6.1 The infra change (kardamom-infra)

- **Two static servers** in `terraform/hetzner/staging.tfvars`:

```hcl
notifier-0 = { role = "notifier", tier = "worker", server_type = "cpx12", private_ip = "10.20.0.58", placement_group = "services" }
notifier-1 = { role = "notifier", tier = "worker", server_type = "cpx12", private_ip = "10.20.0.59", placement_group = "services" }
```

- **The addresses.** 10.20.0.58 and 10.20.0.59 are free. The plan uses .50-.52, .55-.57 and
  .60 in the services range, and #67 adds .70 (kardamom-infra `docs/staging-launch.md:35`).
- **The placement group.** `services` holds 6 servers today (`staging.tfvars:92-97`). With the two
  notifier nodes it holds 8, below the limit of 10 (`main.tf:205-210`). A spread group puts
  the two notifier nodes on different hosts.
- **The role.** `notifier` goes into the list of roles of `variables.tf:170`.
- **No public address.** The default of `public` is false. The notifier nodes route through gw-0.
- **The server budget.** Today 13 static servers plus the pool maxima 2 + 2 give 17, which is
  the budget (`staging.tfvars:16`). #67 makes it 18 with load-0. The two notifier nodes need 20.
  `server_budget = 20`.
- **The cores.** Today the static servers use 38 shared cores against a project limit of 40
  (`staging.tfvars:14-15`). The notifier nodes add 2: 40. The resize of the ingress nodes adds
  none (4 to 4). load-0 has 8 dedicated cores. The limit in the console must cover the total
  before the apply (section 10, step 3).
- **The inventory.** `ansible/inventories/staging/hosts.ini`:

```ini
[notifier]
notifier-0 ansible_host=10.20.0.58 kardamom_node=notifier-0 role=notifier node_roles=notifier tier=worker node_index=0
notifier-1 ansible_host=10.20.0.59 kardamom_node=notifier-1 role=notifier node_roles=notifier tier=worker node_index=1
```

  The group `notifier` goes into `[cloud:children]` (`hosts.ini:146-155`).
- **The directories.** The node build runs the `common` role, which makes
  `/opt/kardamom/notifier` with mode 0777 (`deploy/cluster/ansible/roles/common/tasks/main.yml:81-99`).
  #78 says that only a node build makes the directories. A new node is a node build, so #78 does
  not block this step.

### 6.2 The deploy change (kardamom)

- **The constraint.** `meta.roles` `set_contains` `notifier`, as the batcher does
  (`batcher.nomad.hcl:178-182`). It replaces `meta.role = ingress` (`notifier.nomad.hcl:108-111`).
- **The count.** A job variable `notifier_count`, default 2, with `distinct_hosts`. The workloads
  role passes it from the node class count. `--instance-count` takes the same value
  (`notifier.nomad.hcl:185`).
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
  512 MB is expected. The rollout measures it, then lowers `driver_quiet_memory_mb`.
- **The fit on cpx12.** 2 GB: the notifier 512 MB, the driver 768 MB (512 MB after the measure),
  the node exporter 64 MB (`deploy/cluster/nomad/node-exporter.system.nomad.hcl:90-93`), and
  the Consul, Nomad and Docker agents. The fallback is cpx22, by an in-place resize.
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
- **The state.** `/opt/kardamom/notifier` holds the webhook subscriptions, the outboxes and the
  cursors (`crates/notifier/src/main.rs:79-85`). A registration is stored on both instances
  (`notifier.nomad.hcl:11-14`). The move copies the directory of `ingress-<i>` to `notifier-<i>`,
  so the shard of each instance keeps its outbox.
- **The move.** Stop the notifier job. Copy the two directories. Deploy the job with the new
  constraint. The WebSocket feed is down for a few minutes. The canary feed check fails in that
  window: silence it first.
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
- **The disk.** `hcloud_server.static` sets no `keep_disk` (`main.tf:243-293`). The disk then
  grows from 80 to 160 GB, and a later move back to cx33 is not possible. With `keep_disk = true`
  the disk stays 80 GB, and cx33 stays possible. After Part B the ingress keeps no recording, so
  80 GB is enough. Recommendation: `keep_disk = true` (Q2).
- **The hazard of load-0.** load-0 is in the local state (it was applied on 2026-10-17), but only
  in the open #67. An apply from infra main destroys it. Merge #67, or rebase on it, first. The
  plan of each apply must show 0 to destroy.
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
  timeout at the client `max_kill_timeout`, which the deploy does not set. Nomad's default, as far
  as we know, is 30 s (not checked against the Nomad source). With `--ack validated` nothing
  parks, so 30 s is enough.

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
- The split threshold of #792 stays: 4 requests (today 16, `sig_verify.rs:112`).
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
| Every egress instance down | The batcher and the validator stop moving. The carry executors run on the member feed, but cannot recover below the sealer window. The sequencers get no account rows, so admission refuses the senders with pending transactions (one-way spec 6.3). The notifier gets no receipts (`--receipts-from-egress`). The DA-lag guard stops user records after 10,000 unposted blocks. | The alerts of the egress. Restart an instance with its store. |
| Both egress stores lost | An empty instance cannot join a running chain (`egress.nomad.hcl:27-29`). The blocks above the sealer window and the posted head have no copy that a reader can read. Their bytes exist in the `exec_txs` recording of each executor (`exec_stream/mod.rs:1-23`), but no reader recovers from it. | Q5. Today the `tx_data` archives are a second copy for the batcher rebuild. Part B removes it. |
| An egress behind | The readers lag. The distance gauge `kardamom_egress_head_distance_frames` and its alert show it. No loss. | One-way spec 6.4. |
| The batcher below the egress floor | It halts with `ReplayUnavailable` and the floor (4.3). | The floor alert of 4.3. `--keep-blocks` is far above the DA-lag budget. |
| A driver switch on one node | Its clients restart, 20 to 40 s (5.2). | One node at a time, at low load. The twin or the other ingress serves. |
| The second ingress fails during a resize | No ingress serves RPC until one is back. | Low load. Check the other ingress first. Stop the step if it is not healthy. |
| A resize fails or hangs | The node stays off. One ingress serves. | Hetzner console. The other ingress carries the load. |
| An apply from infra main | It destroys load-0. | #67 first. The plan shows 0 to destroy. |
| One notifier node lost | The other instance serves the WebSocket feed. The webhook shard of the lost instance waits: a shard moves only when the instance count changes (`notifier.nomad.hcl:11-14`). | The node comes back. Staging accepts the wait. |
| cpx12 too small | The notifier or the driver is killed for memory, or the events lag. | The ramp check (section 10, step 5). The fallback is cpx22. |
| A lost `tx_data` frame after Part B | The sequencer does not order it, as today: the sequencer has no refetch (F3). The client sees no receipt. | No change. One-way D1. |
| A reader in reference mode after Part B | It opens its lanes, but no archive serves a join miss. With feature 5 on, no type-0 reference comes, so it never joins. | The preflight of 5.4 keeps the carry gates. |

## 9. Later: the elastic ingress pool

The user's last pool shape was min 2 and max 3 cpx32. These blockers stay open:

- **A deploy resets the count.** `register.yml:36-46` posts `/v1/jobs` with no `PreserveCounts`.
  `wait_allocations.yml:26` reads the job count, and the fake Nomad in
  `deploy/cluster/ansible/tests/test_deploy.py:287` uses it.
- **The LB.** The targets are the static `rpc_target` set, with a precondition of at least 2
  (`main.tf:350-363`). A pool needs a `label_selector` target (`group-id=kardamom-staging-ingress`,
  `use_private_ip`) and an HTTP check on `/health`, so that a drain (503) takes the node out.
- **The drain.** The ingress job has no `shutdown_delay`. It needs one above the LB detection
  (about 30 to 40 s). `kill_timeout` 40 s is above the Nomad default `max_kill_timeout`, which no
  config sets.
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
- **Fixed counts.** `ingress.nomad.hcl:170` (`count = 2`), `monitoring.nomad.hcl:119`,
  `canary.nomad.hcl:78`, and the chaos suite (`crates/chaos/src/harness.rs:112`, `:137`,
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

Each step names its checks. Do not start a step until the checks of the step before pass.
**A step marked "needs the user's explicit approval" does not start without it.**

1. **Release 1.** It holds E1 to E5 (section 11) with every switch at today's value. Before the
   aeron job, apply `aeron_driver = archive` to every live node (5.2), so every node keeps the
   archive group.
   - Every `aeron` allocation keeps its id: no driver restarted.
   - The notifier still runs on the ingress nodes. The canary passes.
2. **Merge infra #67, or rebase the infra PRs on it.** **Needs the user's explicit approval** (a
   change to the live infra state).
3. **The notifier nodes.** Apply I2. **Needs the user's explicit approval: it creates two paid
   servers and runs a Terraform apply.**
   - Before: the core limit in the console covers the total. The plan adds 2 servers and destroys
     0.
   - After: notifier-0 and notifier-1 join Consul and Nomad with `meta.roles = notifier` and
     `meta.aeron_driver = quiet`. The `aeron-quiet` group runs on both.
4. **The notifier move** (6.3). Silence the canary feed check. Stop the job. Copy the directories.
   Deploy.
   - Two notifier allocations, on notifier-0 and notifier-1. The canary feed check passes.
     `kardamom-notifier-metrics` is scraped.
5. **The notifier check in the next ramp.**
   - Read the notifier event latency (the canary feed lag, and the time from a receipt to its
     event) and the node CPU of both notifier nodes.
   - Read the driver memory on those nodes. Set `driver_quiet_memory_mb` to the measured peak plus
     a margin, about 512.
   - If a notifier node runs out of CPU or memory: resize it to cpx22. **Needs the user's explicit
     approval.**
6. **The plain driver where nothing records.** Set `aeron_driver = plain` on sequencer-0, then
   sequencer-1, then validator-0, batcher-0, monitoring-0, redis-0 and redis-1, one at a time.
   - Each node: the `aeron-plain` allocation runs, the services on it are healthy, and the node
     has no `kardamom-aeron-archive` record.
   - Each sequencer: `kardamom_sequencer_sealer_version` 5, receipts land.
7. **Resize ingress-0 to cpx32** (7.1). **Needs the user's explicit approval: a Terraform apply
   and a paid server type.**
8. **Resize ingress-1 to cpx32** (7.1). **Needs the user's explicit approval.**
9. **The verifier.** After #792: deploy `ingress_verify_workers = 3`. Run the ramp. Compare the
   values of 7.2 with the ramp of 2026-10-24.
10. **Part B** (5.4), after step 10 of the carry rollout and the deploy of E1:
    - Deploy E6. Its preflight must pass.
    - Set `aeron_driver = plain` on ingress-0, then on ingress-1.
    - Consul has no record with the tag `tx_data` or `tx_deposits`. The rendered
      `channels.toml` of a reader lists no `tx_data` and no `tx_deposits` archive.
    - The ingress starts with no recorder. The da-watcher publishes epochs with no recording.
    - Receipts land. The validator agrees. The batcher posts.
11. **The cleanup deploy.** Deploy E7.
12. **Soak for 24 hours** under the canary. Then delete the old `tx_data` recordings on the
    ingress nodes and the `tx_deposits` recordings on da-watcher-0. **Needs the user's explicit
    approval: it deletes data on staging nodes.**

### 10.1 Tests

- **Rust unit tests.**
  - The batcher in egress mode: an egress 410 gives the halt `ReplayUnavailable` with the floor,
    and calls no archive.
  - The batcher in cluster mode keeps the rebuild.
  - The egress floor and head gauges move with a prune and with an append.
  - `--verify-workers`: 0 is refused at the parse. With no value, the rule of today. With 3, the
    flush loop runs 3 workers.
- **Deploy tests** (`deploy/cluster/ansible/tests/test_deploy.py`, `test_channels.py`):
  - `aeron_driver` for each role set: `archive` with `indexer` or `executor`, `quiet` with
    `notifier`, `plain` otherwise.
  - The aeron job: each group constrains on its kind. Only the group `aeron` has the archive ports
    and the `kardamom-aeron-archive` service. A node with `aeron_driver = archive` keeps the
    allocation of today.
  - `archive_topics` is empty on an ingress node and on a da-watcher node (E6).
  - `channels.toml` renders no `tx_data` or `tx_deposits` list (E7).
  - The notifier job: the role constraint, `count = notifier_count`, `--instance-count` equal to
    the count, the peers `notifier-<i>`, 512 MB.
  - The ingress job has no `--archive-durability`. The da-watcher job has none.
  - The preflight of 5.4 refuses with one reader off the egress, with one executor not in carry
    mode, and with the batcher meta missing. A refusal writes no job.
  - `--keep-blocks` below `20 x daLagBudgetBlocks` is refused.
- **Infra tests** (kardamom-infra `terraform/hetzner/tests`): the role `notifier` is accepted, the
  budget precondition holds at 20, the services group holds 8.
- **Multi-process e2e** (`crates/e2e`): a new scenario on the stack of the carry scenario
  (carry spec 11.3), with no archive except `exec_txs`:
  1. Send transfers and a deposit.
  2. Restart the validator and the batcher with cursors below the sealer window. Both recover
     from the egress range read. The deposit executes once.
  3. Restart a carry executor. It reads the epoch with its deposits from the egress.
  4. Stop both egress instances for 30 s. The batcher and the validator wait, then resume.
- **The container shard.** The `carry` shard of carry spec C8 gets a no-archive profile: every
  node except the executor and follower nodes runs `aeron-plain`, and the notifier runs on its own
  node class. Chaos cases under load:
  - switch the driver of one ingress node, and measure the downtime of 5.2;
  - restart one egress instance;
  - stop one notifier node, and check the WebSocket feed of the other.
- **The elastic shard** (scale-out and scale-in under load) belongs to section 9.

## 11. Pull requests

Each PR passes `just style` and CI alone, and follows `docs/STYLE.md`.

| PR | Repo | Title | Depends on | Size |
|---|---|---|---|---|
| E1 | kardamom | batcher: in egress mode a refused replay halts, no archive rebuild; egress: floor and head gauges; deploy: the `--keep-blocks` check and the floor alert; batcher job meta `egress_halt_no_rebuild` | none | M |
| E2 | kardamom | ingress: `--verify-workers`; deploy: `ingress_verify_workers` | #792 | S |
| E3 | kardamom | aeron image: wait only for `cnc.dat` with the plain class | none | S |
| E4 | kardamom | deploy: the aeron job by `aeron_driver` (three groups), the node meta, `idle_cap_quiet_us`, `driver_quiet_memory_mb`, tests | E3 | M |
| E5 | kardamom | deploy: the notifier on the `notifier` role: constraint, `notifier_count`, peers, instance count, 512 MB, README | none | S |
| I1 | infra | #67, load-0 (exists) | none | S |
| I2 | infra | terraform and inventory: notifier-0 and notifier-1 on cpx12, the role, `server_budget = 20`, the address plan in `docs/staging-launch.md` | I1 | S |
| I3 | infra | terraform: ingress-0, then ingress-1, on cpx32, with `keep_disk` (Q2); one commit for each server | I1 | S |
| E6 | kardamom | deploy: no `tx_data` and no `tx_deposits` recording: the ingress and da-watcher flags, `archive_topics`, the preflight of 5.4, tests | carry C6 (#791), E1, E4 | M |
| E7 | kardamom | deploy: remove the two archive lists, the contract checks and the refetch flags and ports of the readers | E6 on staging | S |
| E8 | kardamom | e2e and chaos: the no-archive scenario, the no-archive profile of the `carry` shard, the cases of 10.1 | E4 to E7, carry C7, C8 | L |
| E9 | kardamom | docs: `docs/failure-modes.md`, `deploy/cluster/README.md`, `crates/log/README.md`, the stale comments of 4.4, a note at P8 and P13 of the one-way spec | E6 | S |
| E10 | kardamom | cleanup (part of one-way P13): `refetch.rs`, the recorders of `tx_data` and `tx_deposits`, `archive_join_recovery`, the batcher rebuild, `RangeAbsent`, the void vote | E7 on every network | L |

Sizes: S is up to about 200 lines, M up to about 600, L above that. E1 to E5 can start at once.
The release of step 1 holds E1 to E5.

## 12. Considered and not chosen

- **One aeron task with the main class from the node meta.** It keeps the archive ports and the
  Consul record on a node with no archive (5.2).
- **A node-meta idle cap.** It splits the value between the deploy and the Nomad agent template.
  The quiet group keeps it in the deploy.
- **The notifier in the egress now (P8).** Section 6.4.
- **One cpx22 with two notifiers.** It loses the HA of two hosts. Two cpx12 keep it.
- **Keep the `tx_data` archive as a copy for the batcher.** It is the only reason for the
  ingress recorders and the archive on the ingress nodes. Q5 names the gap that it closes.
- **Min 1 ingress.** The user set min 2 (section 9).

## 13. Open questions for the user

- **Q1. Pin 3 cores, or 3 workers with no pin?** Recommendation: 3 workers, no pin, and measure
  first (7.2). Pin only as an experiment on one node, if `ingress.recover_wait` and
  `node_schedstat_waiting_seconds_total` show that the verifier still waits for the CPU.
- **Q2. `keep_disk` at the resize.** With it, the disk stays 80 GB and a move back to cx33 stays
  possible. Without it, the disk grows to 160 GB, with no way back. Recommendation:
  `keep_disk = true`.
- **Q3. The notifier state at the move.** Copy `/opt/kardamom/notifier` from each ingress node
  (a stop of a few minutes), or start empty (the webhook clients register again).
  Recommendation: copy.
- **Q4. The notifier as its own class, or in the egress (P8).** Recommendation: its own class now.
  Revisit P8 when the egress has its own fleet (6.4).
- **Q5. The loss of both egress stores after Part B.** The `tx_data` archives are then gone, and
  no reader recovers from the `exec_txs` recordings. Recommendation: accept it on staging, with 2
  egress instances. Open an issue for a recovery from the `exec_txs` recordings, or a third egress
  instance, before a production network.
- **Q6. When to delete the old recordings** (section 10, step 12). Recommendation: after a soak of
  7 days, not 24 hours.

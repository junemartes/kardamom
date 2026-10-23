# Carry storage by an upgrade transaction

- Status: revision 1, draft for review.
- Issue: #771 (the carried path is not complete on staging). This spec closes gap 3 (the
  cut-over of a live chain) and gap 1 (the executors read the member feed).
- The user decided on 2026-10-23:
  - A live chain switches to carry storage by an upgrade transaction: a new feature activation.
    There is no new chain and no stop-all cut-over.
  - Topology A: each executor runs on the node of its paired sealer member, and reads the feed
    of that member on `aeron:ipc`.
  - The upgrade policy holds (#661): a breaking change ships behind a feature flag, and every
    deploy is backward compatible.
- Builds on (assumed merged first): #779 (snapshot version 17), #781 (the deploy switch
  `KARDAMOM_CARRY_BYTES` and the pinned executor groups), the preflight of #780, and #765 (the
  exact window under version 5).
- Related specs: `docs/specs/2026-10-10-raft-carried-transactions.md` (the carry spec),
  `docs/specs/2026-10-21-one-way-pipeline.md` (the one-way spec, decision D11).

## 1. The problem

### 1.1 What runs on staging today

- Feature 4 (`FEATURE_SEALER_V5`, the carry flag) is active since 2026-10-21. Every member runs
  decision version 5 (`crates/types/src/upgrades.rs:87-93`, `VersionSwitch.java:72-86`).
- Every member keeps its retained frames on the heap. The deploy does not set
  `kardamom.cluster.carryBytes`.
- Under version 5, a sequencer offers each transaction as a full reference (record type 7), but
  only after the sealer reports version 5 on its session (egress kind 17).
  `Sequencer::carried` sends the envelope only when `carries_bytes()` is true
  (`crates/sequencer/src/v2/mod.rs:426-433`).
- The member orders a full reference and relays one canonical entry (`RT_TX`) with the envelope
  (`SealerClusteredService.java:1170-1180`, `SealerEgress.java:646-647`,
  `EgressFrames.java:49-55`).
- A heap member gives each egress session the frame as it is (`HeapFrames.java:252-259`). So
  each executor on its cluster session already gets the bytes of each `RT_TX` entry. It runs the
  entry with no join (`crates/engine/src/reader/cluster/mod.rs:259-264`).
- A heap member keeps the posted rule under version 5: a frame past the window leaves only when
  its block is posted (`SealerEgress.java:174-177`, `HeapFrames.java:138`). The heap grows with
  the DA lag, as before #765.

### 1.2 Why `carryBytes` cannot change on a live chain

`kardamom.cluster.carryBytes=true` does five things at once. Each one depends on the setting, not
on the replicated state:

| What | Where | Effect on a mixed fleet |
|---|---|---|
| The store of the retained frames: the file, not the heap | `SealerClusteredService.java:382-384` | The snapshot layout differs. |
| The snapshot version: 12 (17 with #779), not 14 to 16 | `SealerClusteredService.java:466-475`, `:1595-1597` | A member stops on a snapshot of the other layout (`requireRetentionLayout`, `:504-516`). |
| The void configuration `DISABLED` | `SealerClusteredService.java:352-354` | The void ledger and the snapshot differ. |
| The vote kinds (6, 9) drop as unknown | `SealerClusteredService.java:747-749` | Under version 5 both sides drop them, so no difference today. |
| The relay of a full entry (type 5) as a canonical entry, and no direct record path | `SealerClusteredService.java:964`, `:1008-1015` | A type-5 record relays differently. The direct path is off, so the record path allocates. |

So the release gate lists `carryBytes` in `workloads_sealer_must_match`
(`deploy/cluster/ansible/roles/workloads/defaults/main.yml:333-345`), and the gate refuses a
rolling change (`gate.yml:362-385`). The README says the switch changes "only with a new chain or
a coordinated cut-over" (`SealerClusteredService.java:498-503`).

### 1.3 Findings that change the rough design

The rough design of the task was checked against the code. These findings change it.

- **F1. The executors already get the bytes.** #771 gap 1 says the executors read references. On
  a heap member under version 5, they get each `RT_TX` entry with its bytes (1.1). A reference
  reaches them only when a sequencer offers a type-0 reference. That happens in two cases:
  - A sequencer session that opened, before kind 17 of that session arrives. Until then the
    version is 0 (`crates/sequencer/src/sealer_rules.rs:77-84`), and the sequencer sends type 0.
  - The sealer still orders a type-0 reference under version 5 (README "Version 5", "Other
    records").
  The halt of the validator with `--egress` on 2026-10-23 (`TxRef ... not found`) fits these
  references. This spec stops them (section 4.5 and section 5).
- **F2. The file strips the bytes from the sessions.** A file member gives each session a
  canonical entry as its relayed reference, kind 15 (`RetainedFile.java:491-499`,
  `EgressFrames.java:88-101`). The cluster reader of an executor drops kind 15
  (`crates/engine/src/reader/cluster/mod.rs:258`). So if a live chain turned on the file as it is,
  every executor that still reads its cluster session would lose every transaction at that point.
  This spec keeps the bytes on the sessions (section 4.6).
- **F3. Every config-dependent rule must become state-dependent.** The table of 1.2 lists five
  rules that read the setting. A "dormant" file only works when none of them reads the setting.
  Section 4.2 moves each one to the state.
- **F4. A peer seed fails with a dormant file.** With the file set, a blank member always runs
  the copy of the retained file. The copy refuses a snapshot without a file section
  (`PeerSeed.java:135-136`, `RetainedCopy.java:55-60`). So a member with the dormant
  configuration could not be seeded before the activation. Section 4.10 runs the copy only for a
  snapshot that holds an active file.
- **F5. The feed floor is the egress floor.** The member feed checks a request against the floors
  of the egress (`MemberFeed.java:152`), then walks the file from `scanStart`
  (`MemberFeed.java:158`). In the transition, the egress floor is below the file. Without a
  change, the feed would skip the heap frames silently. Section 4.7 adds the file floor.
- **F6. A carry-mode executor cannot join a reference.** It opens no `tx_data` lane
  (`crates/executor/src/bin/kardamom-executor/main.rs:311-313`). Its reader passes a reference on
  as an ordinary record (`crates/engine/src/reader/carried/answer.rs:210`). Section 6 makes a
  reference a fault of the source.
- **F7. A copy at the activation is not bounded.** Under the posted rule the heap holds every
  frame above the posted head. A copy of the heap into the file at the activation would stall
  every member for a time that grows with the DA lag. The rough design keeps the old frames on the
  heap until they leave. This spec keeps that part (section 4.3).

## 2. Goals and limits

- Goal: a live chain moves its retained frames from the heap to the retained file at one log
  entry, the same entry on every member and in every replay.
- Goal: each executor moves to the member feed of its paired member, one at a time, with no
  stop of the chain.
- Goal: the window of #765 then holds: the frames of a member have a fixed bound at any DA lag.
- Limit: every deploy is backward compatible. No deploy changes a rule of the replicated state.
  Only the upgrade transaction of the new feature does.
- Limit: no stop of all members, no purge, no new chain.
- Limit: no change to a wire format that a client reads, except new fields on admin `/status`.
- Non-goal: a network feed in place of `aeron:ipc` (topology B).
- Non-goal: the removal of the heap store. A later release can remove it after the activation on
  every network.

## 3. The design in short

1. **A release with a ready file.** The deploy gives each member the file directory, the token,
   the admin port and the feed settings. The member keeps the heap store, the heap snapshot and
   every rule of today. The setting is local to the member: it decides nothing in the state. So a
   rolling deploy is safe.
2. **Feature 5, the carry storage.** An upgrade transaction schedules
   `FEATURE_CARRY_STORAGE` (feature id 5). It follows the rules of features 3 and 4: the
   sequencer sends kind 11 in front of the epoch, and the switch happens at a block boundary.
   The switch is one-way.
3. **The point B.** At the end of the log entry whose boundary turns feature 5 on, every member
   opens an empty retained file. B is the record index and the block of the next frame.
   - Each frame from B goes to the file.
   - The frames before B stay on the heap until they leave by the eviction rule.
   - From B, the member drops a type-0 transaction reference.
   - The member asks for a snapshot, as at a version switch.
4. **The snapshot in the transition.** Version 18: the state of version 16, then the schedule
   and the latch of feature 5, then the heap section, then the file section.
5. **The exact window after the posted head passes B.** Until then the posted rule stays, so a
   batcher that reads the sealer keeps its frames.
6. **The feed starts at B.** A request below B gets `REPLAY_UNAVAILABLE` with B as the floor.
7. **The executors move one at a time**, after their committed block passes B and the compare
   window. A carry-mode executor needs no hybrid code: a reference is a fault of the source.
8. **The readers move to the egress** once their cursor passes B.

## 4. The sealer

### 4.1 Feature 5 and the latch

- **The feature.** `FEATURE_CARRY_STORAGE: u64 = 5` in `crates/types/src/upgrades.rs`, and the
  same constant in `VersionSwitch.java`. Only the sealer follows it. The executor does not read
  it. Feature 5 is free: the ids in use are 1 to 4 (`crates/exec-core/src/features.rs:50`,
  `crates/types/src/upgrades.rs:77-93`).
- **The schedule.** `VersionSwitch` gets a third `Schedule`, `storage`, next to `v4` and `carry`
  (`VersionSwitch.java:91-95`). `onActivation` takes feature 5 with the same origin and epoch
  rules (`VersionSwitch.java:140-166`).
- **EARLY.** The state takes an activation of feature 5 only while version 5 runs, as it takes
  feature 4 only while version 4 or 5 runs (`VersionSwitch.java:156-158`). A record that comes
  under version 3 or 4 changes nothing (outcome `EARLY`).
- **The latch.** At each boundary, the state evaluates `storage.onAt(...)` as it evaluates the
  version (`VersionSwitch.java:175-201`). When it gives "on" for the first time, the state sets
  the latch: `storageOn = true`. The latch never goes back.
  - A held activation with a time ahead, or `2^64 - 1`, before B postpones or cancels the switch,
    as today. After B, the state still stores the record, but the latch stays on. The member logs
    `cluster FEATURE-ACTIVATION outcome=ONE_WAY` and counts it.
  - Version 5 must stay on after B. If feature 4 or 3 turns off later, the latch stays. The
    file stays, and the eviction falls back to the posted rule (4.4). The gate refuses such an
    upgrade transaction (section 9).
- **B.** The service reads the latch after each log entry, as `afterEntry` reads the version
  (`SealerClusteredService.java:432-442`). When the latch turned on in the entry, the service
  records B in the state: `storageFromIndex = canonicalCount`, `storageFromBlock = blockNumber`
  (the open block). B is replicated state: the snapshot carries it (4.8).
- **Why a latch, not decision version 6.** A version must be able to go back (decision 23 of the
  carry spec). The store cannot go back in one entry: the frames in the file would need a copy to
  the heap, and the executors that read the feed would lose it. A latch keeps the version
  machinery as it is.

### 4.2 The ready configuration: the setting decides nothing

The setting only says that the member can hold the file. The state decides whether it does.

- **The property.** A new property `kardamom.cluster.carryStorage`, with the values `off`
  (default) and `ready`. `ready` needs the admin port, the token and the snapshot interval, as
  `CarryBytes.Inputs.require` checks today (`CarryBytes.java:49-62`). The feed settings need
  `ready` (`FeedConfig.java:95-103`, with the new name).
- **The old property.** This release refuses `kardamom.cluster.carryBytes=true` at the start,
  with a message that names the activation. No live chain uses it (#779). The property leaves
  `workloads_sealer_must_match`. The new property is not in that list.
- **Each rule moves to the state:**

| Rule today | Reads | After this change |
|---|---|---|
| The store (`:382-384`) | the setting | The heap store from the start. At B, the tiered store (4.3). A restore of an active snapshot opens the tiered store. |
| The snapshot version (`:466-475`, `:1595-1597`) | the setting | The state: version 18 when the state holds a schedule or the latch of feature 5, else the version of today. A v17 restore keeps writing 17. |
| The layout check (`:504-516`) | the setting | The member stops only when the snapshot holds an active file and the member is not `ready` (4.8). |
| The void configuration (`:352-354`) | the setting | The configuration of the job, as on a heap member. A v12 or v17 restore keeps `DISABLED`, as today. |
| The vote kinds (`:747-749`) | the setting and the version | The version only: `version < 5`. A v12 or v17 restore keeps them off. |
| The type-5 relay (`:1008-1015`) | the setting | The latch. Before B, a type-5 record relays as is, as on a heap member. |
| The direct record path (`:964`) | the setting | `window.capacity() == 0` only (4.5). |

- **The v17 chain.** A member that restores version 17 (a chain born with `carryBytes`) runs the
  file from its start. The latch is on, and B is `(0, 0)`. No live chain has such a snapshot, so
  this only keeps the reads of #779.
- **A start at genesis or from a seed** runs the heap store, with the latch off. The setting does
  not change that. So a member that replays the log from position 0 decides as its peers.

### 4.3 The transition: the tiered store

- **The store.** A new `TieredFrames implements RetainedFrames`: a heap tier (`HeapFrames`) and a
  file tier (`RetainedFile`). `RetainedFrames` is sealed (`RetainedFrames.java:20`), so it gets a
  third permitted type.
  - `append`: to the file tier.
  - `count`: the sum of the tiers.
  - `frontIsBoundary`, `frontKey`, `evictFront`: the heap tier while it holds a frame, else the
    file tier. The frames before B are older than the frames after B, so the front is always in
    the heap tier first.
  - `boundaryEnd`: the tier that holds the boundary.
  - `wanted`: the heap frames, then the file frames.
  - `writeSnapshot` and `restore`: the heap section, then the file section (4.8). The floors are
    the lower of the two.
  - `status`: the file form, with the retained bytes of the file above the posted head
    (`RetainedFile.java:339-342`).
  - `publishedHead`, `checkHealth`, `close`: the file tier.
  - `forSession`: the frame as it is (4.6).
  - `carriesBytes`: true.
- **The swap.** `SealerEgress.retained` is a final field today (`SealerEgress.java:100`). The
  egress gets one method, `openFileTier(RetainedFile)`: it wraps the heap store in a
  `TieredFrames` and opens the file tier empty (`RetainedFile.clear()`). The service calls it in
  `afterEntry` when the latch turned on, before the snapshot request.
- **The file at B.** Every member opens the file at position 0 at B. A member that runs B again
  in a replay clears the file of its earlier run first. So the positions are a function of the
  log from B on every member.
- **The cost of B.** No copy. The swap allocates the tier objects once and starts the segment
  helper. The first segment is preallocated in the background, so the first append can wait for
  it. The `ready` member asks for one spare segment at its start, so the wait does not happen at
  B.
- **The heap tier drains.** Each eviction from the front empties it further. Once it is empty, it
  stays empty: no frame goes to it after B.

### 4.4 The eviction rule

Today: `windowOnly = version == 5 && retained.carriesBytes()` (`SealerEgress.java:174-177`).

After this change: `windowOnly = version == 5 && retained.carriesBytes() && postedHead >=
storageFromBlock`.

- **Why the posted head.** Before B, the readers of the order can meet type-0 references
  (finding F1). The egress cannot give them bytes, so the batcher and the validator read the
  sealer until their cursor passes B. With the exact window at B, a batcher that restarts with a
  cursor below the window gets `REPLAY_UNAVAILABLE` and has no source with bytes. So the exact
  window waits until the batcher has posted past B. From then on, a batcher below the window reads
  the egress, which holds bytes from B.
- **Determinism.** The posted head and B are replicated state. `setPostedHead` and
  `setDecisionVersion` both prune (`SealerEgress.java:157-177`). So every member evicts the same
  frames at the same log position, from the log or from a snapshot.
- **The v17 chain.** `storageFromBlock` is 0, so the rule is the rule of #765.
- **The bound in the transition.** Until the posted head passes B, the frames grow with the DA
  lag, as on staging today. The DA-lag guard bounds them (`daLagBudgetBlocks`). After that, the
  file holds `retention` frames plus the segments of the last snapshot
  (`RetainedFile.java:350-360`), and the heap tier is empty.

### 4.5 The record path after B

- **A type-0 transaction reference drops.** From the first block after B, and while version 5
  runs, the member drops a reference of record type 0 (`RT_TXREF`) and counts it
  (`cluster DROPPED txref-carry-storage`, a new reason of the existing drop counter).
  - Why: no reader after B can join a reference. A carry-mode executor has no lane (F6), and the
    egress cannot add the bytes.
  - It is a rule of the state, so it changes at B on every member. The latch is in the state, so
    the record path reads it in the same entry as the boundary.
  - Its twin offers the same transaction as type 7, so the transaction is still ordered. A
    sequencer of this release sends no type 0 under version 5 (section 5).
  - Under version 3 or 4 (a later switch back), the member does not drop type 0: the rule
    needs version 5.
  - An epoch, a deposit and a remote-origin record keep their rules.
- **The direct path.** Today a file member never takes the direct path
  (`SealerClusteredService.java:964`). So every record of a file member goes through a
  `HeldRecord`, which allocates. The type-7 path and the type-0 path never call `relayOf`, so the
  setting is not needed there. After this change, `direct = window.capacity() == 0`. A type-6 or
  type-5 record still falls to the held path, where `relayOf` refuses or converts it.
- **The allocation of the file append.** `RetainedFile.append` calls `segments.lastEntry()`
  (`RetainedFile.java:165`), which returns a new entry object, and wraps it in an `Optional`.
  `FrameKey.of(staged)` (`:179`) makes a record. The append keeps the last segment in a field, and
  reads the kind and the key from the staged buffer with no object. Only a boundary allocates its
  index mark (one for each block). `CarriedPathAllocationTest` gets the cases `carried_fresh_file`
  and `carried_copy_file`.

### 4.6 The session form

- A `TieredFrames` gives each session the frame as it is. A session that reads the cluster egress
  keeps getting the bytes of each `RT_TX` entry after B (finding F2).
- Why: until every executor reads the feed, some executors read their cluster session. The
  batcher and the validator read the sealer until their cursor passes B (4.4).
- The cost does not change: on staging, the leader sends the bytes to each consumer session
  today.
- A later change can strip the bytes from the sessions, when no reader of the order uses a
  cluster session. That is out of scope.
- The v17 chain keeps the kind-15 form of `RetainedFile.forSession`.

### 4.7 The member feed

- **Off before B.** `Feed.open` needs the file (`Feed.java:54-56`). Before B the member runs
  `Feed.Off`. At B, the service starts the feeder thread on the file tier, after
  `openFileTier`. A restore of an active snapshot starts it at the start, as today.
- **The floor.** The feed refuses a request below B with `REPLAY_UNAVAILABLE` and the floor
  `(storageFromIndex, storageFromBlock)`. The floor of the feed is the higher of the egress floor
  and B (finding F5). After the heap tier drains, the two floors are the same.
- **Member-local.** The feed settings decide nothing in the state. A `ready` member without the
  feed settings runs no feed. So the feed settings are not must-match.

### 4.8 The snapshot: version 18

| Section | v16 (heap, staging today) | v17 (#779, a chain born with the file) | v18 (this spec) |
|---|---|---|---|
| State of v16 | yes | yes | yes |
| Schedule of feature 5 | no | no | `[held:u8]`, then `[origin:u64][time_ms:u64]` for each held activation |
| Latch and B | no | no (implied on, B = 0) | `[on:u8][from_index:u64][from_block:u64]` |
| Heap section | `HeapFrames` layout | no | `HeapFrames` layout: the heap tier, count 0 when it is empty |
| File section | no | `RetainedSection` with its length, at the end | when the latch is on: `RetainedSection` with its length, at the end |

- **When a member writes 18.** When its state holds a schedule of feature 5 or the latch is on.
  Before the first kind 11 of feature 5, a member writes 14, 15 or 16 as today. So the release
  changes no snapshot byte until the upgrade transaction.
- **The file section stays last.** `RetainedSection.ofSnapshot` reads it from the end
  (`RetainedSection.java:75-80`). So the peer seed finds it with no parse of the state, as today.
- **The restore rules:**

| Snapshot | Member `ready` | Member `off` |
|---|---|---|
| 14, 15, 16 | heap store, latch off | heap store, latch off |
| 18, latch off | heap store, schedule held | heap store, schedule held |
| 18, latch on | tiered store: heap tier, then file tier checked by `RetainedWalk` | stop: `sealer CARRY-STORAGE FATAL` |
| 17 | file store, latch on, B = 0 | stop, as today |
| 12 | file store, version 3 rules (as #779) | stop, as today |

- **Equality.** Every member writes the same bytes at the same log position, in each phase:
  before the schedule, with the schedule held, in the transition, and after the heap tier drains.
- **`formats.toml`.** `sealer-snapshot`: `reads_max = 18`, and the activation is feature 5 with
  `writes = 18`. `docs/formats.md` gets a feature form of `activation`
  (`activation = { feature = 5, writes = 18 }`) next to the flag form.

### 4.9 The admin status

`MemberStatus.toJson` (`MemberStatus.java:120-137`) gets these fields:

- `carryStorage`: `off`, `ready` or `active`. `active` means the latch is on.
- `carryStorageFromIndex` and `carryStorageFromBlock`: B, 0 before it.
- `carryStorageActivationOrigin` and `carryStorageActivationMs`: the newest held activation of
  feature 5, 0 when none.
- `retainedHeapFrames` and `retainedFileFrames`: the frames of each tier.

The gate (section 9) and the rollout (section 12) read them.

### 4.10 The peer seed

- `PeerSeed.copiedRetained` runs the copy of the file only when the backed-up snapshot holds an
  active file: version 17, or version 18 with the latch on. For any other snapshot it passes with
  no copy (finding F4).
- `RetainedCopy.run` accepts version 18 with the latch on (`RetainedCopy.java:55-60` checks one
  version today).
- A blank member seeded from a snapshot before B gets no file. If the backed-up log holds B, the
  member opens its file at B in the replay, as its peers did.

## 5. The sequencer

- **Kind 11 of feature 5.** The list of followed features gets feature 5
  (`crates/sequencer/src/outbound/cluster.rs:152`). The rule of the record does not change: the
  last schedule of the feature in the epoch, in front of the epoch, on each offer of the epoch.
- **No offer before kind 17.** On a new session, the sequencer holds its offers until the sealer
  reports the version of that session (`SealerRules::on_session_start`,
  `crates/sequencer/src/sealer_rules.rs:77-84`). Then it offers type 7 under version 5, and type 0
  under version 3 or 4, as today.
  - Every sealer of the releases since version 4 sends kind 17 when a session opens. So the hold
    ends at once on a live chain.
  - The hold has a deadline: 5 s. After it, the sequencer offers by the rules of version 0, and
    counts `kardamom_sequencer_sealer_version_wait_expired_total`. A sealer of this release always
    reports, so the deadline only protects an older sealer.
  - This change is backward compatible: it changes no frame. It removes the type-0 offers of
    finding F1.
- **Job meta.** The sequencer job gets the meta `carry_storage = "true"` for the gate.
- **Already true.** The sequencer sends type 7 under version 5 (`crates/sequencer/src/v2/mod.rs:426-433`).

## 6. The executors

- **The move.** Each executor moves to the node of its paired member first, in reference mode.
  Then it moves to carry mode (`--carry-bytes`). Both are separate deploy steps (section 10).
- **When an executor can move to carry mode.** Its committed block must be at or above
  `storageFromBlock + rollback_compare_blocks` (32 by default,
  `crates/executor/src/bin/kardamom-executor/args.rs:17`). At its first carry start, the executor
  compares the feed with the committed blocks of that window
  (`crates/executor/src/bin/kardamom-executor/turns.rs:251-255`). Every frame of the window is then
  in the file or in the egress with its bytes.
- **No hybrid code.** A carry-mode executor reads three sources: the feed, then the egress
  (`--egress`), then a peer checkpoint (`args.rs:115-123`). It needs one change:
  - The carried reader treats a type-0 reference as a fault of the source,
    `Fault::Unjoinable { index }` (`crates/engine/src/reader/carried/answer.rs:210`). The switch
    then goes to the next source. A reference can only come from below B: from the egress range
    of a cursor below B, or from a v12 chain.
  - After B the stream holds no type-0 reference (4.5). So in normal operation the fault never
    fires.
- **The first carry run.** The executor has no running marker and no ledger yet. `start_run`
  takes a missing marker as a whole run (`marker.rs:172-186`), and the ledger opens empty. So the
  first run needs no migration.
- **Back to reference mode.** An executor can move back at any time after B. Its cluster session
  gets the bytes (4.6). So the move is reversible, one executor at a time.

## 7. The readers

### 7.1 The batcher and the validator

- They keep reading the sealer until their cursor passes B. The heap and the file keep their
  frames by the posted rule until then (4.4).
- Then the operator switches each to `--egress` (`crates/batcher/src/bin/kardamom-batcher.rs:137`,
  `crates/validator/src/bin/kardamom-validator/args.rs:64`).
- The check before the switch: the cursor of the reader is above B, and the egress stores no
  reference at or above that cursor (7.2).

### 7.2 The egress

- The egress stores what the executors forward on `exec_frames`. Below B it holds references for
  the type-0 offers of finding F1. From B it holds only frames with bytes.
- New gauge: `kardamom_egress_ref_frame_max_index`, the highest canonical index of a stored
  `RT_TXREF` frame. A reader can switch to the egress when its cursor is above it.
- No other change. The egress compares the copies of the executors as today.

## 8. The cases

| Case | What happens |
|---|---|
| A member restarts before B | It restores a heap snapshot (14 to 16, or 18 with the schedule). It replays the log and opens its file at B in the replay. |
| A member restores a snapshot from before B, after B | The same: the replay reaches B, clears the file of its earlier run, and appends the same frames. |
| A member restores a snapshot from after B | It restores the tiered store. `RetainedWalk` checks the file. A file that does not match stops the member (`RetainedFile.java:405-425`); wipe it and let the peer seed copy the file. |
| A blank member seeded before B | Aeron backup copies a heap snapshot and the log. No file copy. It opens its file at B in the replay. |
| A blank member seeded after B | The backed-up snapshot is 18 with the latch on: `RetainedCopy` copies the segments from a peer with the token (4.10). |
| A member without `ready` reaches B | It stops with `sealer CARRY-STORAGE FATAL` and does not fork: it never applies an entry with other rules. The gate prevents this (section 9). Fix: deploy `ready` to it; it replays to B again. |
| A member of an older release at B | It ignores kind 11 of feature 5, keeps the heap, and orders type-0 references that its peers drop. That forks the state. The gate refuses the upgrade transaction while such a member runs (section 9). |
| A rollback of the release after the first kind 11 of feature 5 | The members write version 18. A release that reads only up to 17 stops on it. The preflight of #780 refuses the rollback (`reads_max` below `snapshotLatest`). Before the kind 11, a rollback is safe. |
| An upgrade transaction that turns feature 5 off after B | One-way: the latch stays on (4.1). The gate refuses it. |
| An upgrade transaction that postpones feature 5 before B | Allowed, as for features 3 and 4. |
| Feature 4 or 3 turns off after B | The latch stays. The posted rule comes back, and the members order type-0 references again. Carry-mode executors fault on them. The gate refuses such a transaction after B. |
| A leader change just after B | The sessions reconnect and replay from their cursors. The heap tier and the file tier both serve, so no reader of a cluster session loses a frame. |
| A fleet rebuild from a seed after B | A seeded member runs version 3 with the heap store (`SealerSeed` holds no rules). The operator sends the upgrade transactions of features 3, 4 and 5 again. Until then, the executors run in reference mode. This is the gap of #667 for features 3 and 4 too. |
| A power loss on a majority after the move | The carry-mode executors roll back by themselves (`docs/failure-modes.md:929-933`). This is a gain of the move. |

## 9. The gate

`just sealer-upgrade-check <env> 5 <activation_ms>` (`upgrade_check.yml`) gets these checks:

- Every configured member reports `snapshotReadsMax >= 18`.
- Every configured member reports `carryStorage` `ready` or `active`.
- Every configured member reports `decisionVersion == 5`. Feature 5 is `EARLY` below version 5.
- The sequencer job has the meta `carry_storage = "true"`, and every live allocation runs that
  job version (`upgrade_check_job.yml`).
- The executor and validator jobs have a meta `carried_ref_fault = "true"` (section 6).
- The activation time is not `2^64 - 1` when any member reports `carryStorage = active`.
- New rule for features 3 and 4: when any member reports `carryStorage = active`, refuse any time
  other than 0. A switch back of version 5 would break the carry-mode executors.

The gate tests go to `deploy/cluster/ansible/tests/test_deploy.py`, next to the feature-4 tests
of #779.

## 10. The deploy

### 10.1 How the switch of #781 changes

#781 makes `KARDAMOM_CARRY_BYTES=on` one switch for the sealer and the executors, and it relies on
`KARDAMOM_ALLOW_MUST_MATCH=carryBytes`. This spec splits it into three switches:

| Switch | What it sets | When |
|---|---|---|
| `KARDAMOM_CARRY_BYTES=on` (meaning changes) | The sealer: `carryStorage=ready`, `feedAeronDir`, `feedRingBytes`, the token through the Nomad variables of each member group, the shared driver on the sealer nodes. No change to the executors. | Before the upgrade transaction. A rolling deploy, member by member. No allowance. |
| `KARDAMOM_EXEC_ON_SEALERS=on` (new) | The executor groups `executor-<i>`, pinned to `sealer-<i>`, in reference mode. The readers that reach an executor by its node follow it. | Before or after the upgrade transaction. |
| `KARDAMOM_CARRY_EXECUTORS=<list>` (new) | `--carry-bytes` (and `--egress` with `KARDAMOM_EGRESS=1`) on each listed group, such as `0`, then `0,1`, then `0,1,2`. | After B, one executor at a time. |

- **No must-match flip.** `carryBytes` leaves the must-match list (4.2). The test
  `test_carry_mode_on_a_live_chain_needs_the_allowance` of #781 changes: a rolling deploy of `ready`
  passes with no allowance.
- **The executor roll.** The pinned groups roll together, because Nomad orders no groups (#781,
  open question 1). This spec takes option (a): the executor job uses the staged roll of the
  sealer (`sealer_step.yml`), one group at a time, with a health check between groups.
- **The preflight of `KARDAMOM_CARRY_EXECUTORS`.** It refuses the list unless:
  - every member reports `carryStorage = active`;
  - each listed executor reports `kardamom_executor_block_number` at or above
    `carryStorageFromBlock + 32`;
  - `KARDAMOM_EXEC_ON_SEALERS=on`.
- **The preflight of #781 stays** for the token, the snapshot interval, the exec cursor, and the
  executor count equal to the sealer count.

### 10.2 What staging needs (kardamom-infra)

The list of #781 stays. In short:

- On each sealer node: the directories of the executor and the state mirror, room in the
  `aeron-mount` tmpfs for the shared driver, memory for the member, the shared driver, the
  executor (1,536 MB), the state mirror (512 MB) and the page cache of the state DB.
- Disk on each sealer node for the retained file. Until the posted head passes B, the file holds
  the frames above the posted head (the DA lag). After it, `retention` frames plus two 64 MiB
  segments. At 200 tx/s and about 300 bytes a frame, 30 minutes of DA lag is about 110 MB.
- The ports 9004, 9014, 9024 and 9007 open between the nodes.
- The environment secret `KARDAMOM_RETAINED_TOKEN` in the deploy workflow.
- The executor nodes stay: the egress runs there (#781, open question 2).

## 11. Tests

### 11.1 Unit tests (Java, `DriverlessLog`)

- **Activation at B.** Three `ready` members apply one stream with kind 11 of feature 5 and a
  boundary that turns it on. All three log the same B, open the file at the same entry, and write
  the same snapshot bytes after each entry.
- **Restore at each point.** For each entry of the stream, one member takes a snapshot, stops and
  restores (the pattern of `SealerVersionFiveTest.a_carrying_member_restored_at_any_entry_decides_as_its_peer`
  in #779). The cuts cover: before the schedule, with the schedule held, the entry of B, the
  transition with a non-empty heap tier, the drain of the heap tier, and after the posted head
  passes B. The restored member gets the same egress outcomes, replay answers, status and final
  snapshot bytes as its peer.
- **Replay from before B.** A member restores a snapshot from before B while its file holds
  frames of an earlier run. The replay clears the file at B and ends with the bytes of its peer.
- **Eviction equality.** Under a DA lag, the members keep the posted rule until the posted head
  passes `storageFromBlock`. Then all evict to `retention` at the same entry.
- **The ref drop.** After B, a type-0 reference drops and is counted. Before B, and under version
  4, it is ordered.
- **The session form.** A session gets `RT_TX` with its bytes before and after B.
- **The feed floor.** A feed request below B gets `REPLAY_UNAVAILABLE` with B. A request at B
  gets every frame from B.
- **The layout rules.** The restore table of 4.8, one case each: a member `off` stops on 18 with
  the latch on, and starts on 18 with the latch off.
- **One-way.** A kind 11 that turns feature 5 off after B leaves the latch on and counts
  `ONE_WAY`.
- **Peer seed.** `PeerSeedTest` and `RetainedFileSeedTest`: a heap snapshot passes with no copy; a
  v18 snapshot with the latch on copies and walks the file.
- **Allocation.** `CarriedPathAllocationTest`: the type-7 path of a file member allocates nothing.
- **Formats.** `FormatRegistryTest`: the loader reads 1 to 18.

### 11.2 Rust unit tests

- The sequencer holds a new session until kind 17, and offers type 7 after a report of version 5.
- The sequencer sends kind 11 of feature 5 in front of its epoch.
- The carried reader faults on a type-0 reference, and the switch goes to the next source.
- The egress gauge of the highest stored reference.

### 11.3 Multi-process e2e (`crates/e2e`)

A new scenario in `crates/e2e/src/scenarios/carry_storage.rs`, run from
`crates/e2e/tests/chain_semantics/upgrades.rs` on the stack of `upgrade_stack_config`:

1. Start three members with `carryStorage=ready`. Give each member its own feed driver directory:
   the feed uses fixed stream ids on IPC, so two members cannot share one driver.
2. Send features 3 and 4 with `upgrade_and_await` (`crates/e2e/src/scenarios/upgrade.rs`). Wait
   for `kardamom_sequencer_sealer_version` 5.
3. Send transfers. Then send feature 5. Wait for `carryStorage = active` on every member, and
   check that B is the same on each.
4. Restart executor 0 with `--carry-bytes` on the driver of member 0. Send transfers. Every
   receipt lands, and the validator agrees.
5. Restart member 1 from its snapshot. Receipts keep landing.

### 11.4 The container shard `carry` (CI only)

The plan of #781 ("E2E shard"), with these changes:

- Deploy with `KARDAMOM_CARRY_BYTES=on` and `KARDAMOM_EXEC_ON_SEALERS=on`.
- After `up`: deploy a lockbox (`kardamom-deploy`), and send the upgrade transactions of features
  3, 4 and 5. Wait for `carryStorage = active`.
- Move the executors to carry mode one at a time with `KARDAMOM_CARRY_EXECUTORS`.
- Run `smoke_gate`, 60 s at 100 tx/s, the converged-executors check, and `assert_persisted_state`.
- Chaos cases in the shard: kill the leader within 2 s after B; wipe member 2 after B (the peer
  seed copies the file); restart a member from a snapshot from before B.

### 11.5 Gate tests

`test_deploy.py`: one case for each check of section 9 and each preflight of 10.1, with no job
write on a refusal.

## 12. The staging rollout

Each step names its checks. Do not start a step until the checks of the step before pass.

1. **Deploy the release** with `KARDAMOM_CARRY_BYTES` unset. It holds every PR of section 13.
   - Each member: `snapshotReadsMax` 18, `carryStorage` `off`, `decisionVersion` 5,
     `snapshotLatest` 16.
   - `kardamom_sequencer_sealer_version` 5 on every partition.
   - No `FATAL` line in the sealer logs.
   - Rollback: allowed. Nothing wrote version 18.
2. **Infra.** Apply the kardamom-infra change of 10.2. Check the directories, the tmpfs size, the
   free memory and the free disk on each sealer node.
3. **Ready the file.** Deploy with `KARDAMOM_CARRY_BYTES=on`. The deploy rolls member by member.
   - Each member: `carryStorage` `ready`; the log line `cluster carry bytes` with the directory.
   - `snapshotLatest` stays 16 on every member. The heap frame count does not change.
   - The shared driver runs on each sealer node.
4. **Move the executors to the sealer nodes.** Deploy with `KARDAMOM_EXEC_ON_SEALERS=on`. The
   executor groups roll one at a time.
   - Each executor runs on `sealer-<i>` and `kardamom_executor_block_number` follows the sealed
     head.
   - Receipts land (the canary).
   - `kardamom_egress_copy_mismatch_total` does not move.
5. **The upgrade check.** Pick the activation time: about 30 minutes ahead, at low load. Run
   `just sealer-upgrade-check staging 5 <activation_ms>`. It must pass.
6. **The upgrade transaction on Sepolia. THE USER MUST APPROVE THIS STEP EXPLICITLY.** The Safe
   signs `ETHLockbox.initiateUpgrade(5, <activation_ms>)`. Nothing in this step happens without
   that approval.
   - Each member logs `cluster FEATURE-ACTIVATION` for feature 5 with outcome `STORED`, and
     reports `carryStorageActivationMs`.
   - From here, `snapshotLatest` is 18, and a release rollback is refused (#780).
7. **B.** At the activation time:
   - Each member logs `cluster CARRY-STORAGE` with the same `fromIndex` and `fromBlock`, and
     reports `carryStorage` `active`.
   - `retainedFileFrames` grows. `retainedHeapFrames` does not grow.
   - Receipts keep landing. The executors still read their cluster sessions.
   - `cluster DROPPED txref-carry-storage` stays 0.
8. **The posted head passes B.** Wait for `postedHead` above `carryStorageFromBlock`.
   - `retainedHeapFrames` goes to 0, and the retained frames fall to `retention`.
9. **Move the batcher and the validator to the egress.** For each, check that its cursor is above
   `carryStorageFromBlock` and above `kardamom_egress_ref_frame_max_index`. Then deploy with
   `--egress`.
   - The validator agrees, and the batcher posts.
10. **Move the executors to carry mode, one at a time.** `KARDAMOM_CARRY_EXECUTORS=0`, then `0,1`,
    then `0,1,2`. Before each step, check `kardamom_executor_block_number` at or above
    `carryStorageFromBlock + 32` for the executor.
    - The log line `canonical entries via the member feed`.
    - Receipts land. `kardamom_egress_copy_mismatch_total` does not move.
    - The executor follows the sealed head. No `Unjoinable` fault.
11. **Soak for 24 hours** under the canary. Then close #771.

Drills on staging (a leader kill after B, a member wipe) need the approval of the user, and are
not part of this plan. The chaos shard covers them in CI.

## 13. Pull requests

Each PR passes `just style` and CI alone, and follows `docs/STYLE.md`.

| PR | Title | Depends on | Size |
|---|---|---|---|
| C1 | types, sequencer: `FEATURE_CARRY_STORAGE`, kind 11 of feature 5, the hold until kind 17, job meta `carry_storage` | none | M |
| C2 | sealer core: the schedule and the latch of feature 5, B in the state, the type-0 drop after B, snapshot 18 state section, `formats.toml` | #779 | M |
| C3 | sealer service: `carryStorage=ready`, the config-to-state moves of 4.2, `TieredFrames`, the swap at B, the eviction rule of 4.4, the session form, snapshot 18 retention, the restore table | C2 | L |
| C4 | sealer service: the feed at B with the B floor, the peer seed copy rule, the allocation of the file append, admin `/status` fields | C3 | M |
| C5 | engine, executor, egress: the `Unjoinable` fault, the job meta `carried_ref_fault`, the egress gauge | none | S |
| C6 | deploy: the three switches of 10.1, the staged executor roll, the preflights, the feature-5 checks of `upgrade_check.yml`, `carryBytes` out of must-match | #781, #780, C4, C1 | L |
| C7 | e2e: the carry storage scenario | C1, C4, C5 | M |
| C8 | chaos: the `carry` shard with the activation step | C6, C7 | L |
| C9 | docs: `cluster/sealer-service/README.md` (feature 5, the runbook), `docs/failure-modes.md`, `docs/formats.md`, `deploy/cluster/README.md` | C6 | S |
| infra | kardamom-infra: the sealer node resources of 10.2 | none | S |

Sizes: S is up to about 200 lines, M up to about 600, L above that. C1 and C5 can start at once.
C2 to C4 are one chain. The release of step 12.1 holds C1 to C6.

## 14. Considered and not chosen

- **The member feed over the heap, with no file.** The one-way spec (D11) says that the file is
  not needed: with the exact window, the heap has a fixed bound. A feed that walks `HeapFrames`
  would remove the file, the token, the copy, the ready state and the tiered store. Only the
  eviction rule would need an activation. It was not chosen because the heap snapshot carries
  every frame of the window. At the 10,000 tx/s goal with a 60 s margin, the window is about
  600,000 frames, about 150 MB on the heap and in each snapshot. Every member pauses for the
  snapshot at the same log position. The file snapshot holds only the index. This trade needs the
  user (question Q1).
- **Copy the heap into the file at B.** No hybrid store. But the copy is as large as the heap,
  which grows with the DA lag (F7). It would stall every member at once.
- **Trim the heap to the window at B, then copy.** Bounded, but a batcher that reads the sealer
  loses its frames above the posted head.
- **Start the file empty at B and drop the heap.** No copy and no tiers. But a leader change just
  after B would answer `REPLAY_UNAVAILABLE` to every session that replays, and a reference-mode
  executor then needs a peer checkpoint.
- **Decision version 6.** A version must be able to go back. The store cannot (4.1).
- **Keep `carryBytes` must-match and stop all members.** The user ruled it out.

## 15. Open questions for the user

- **Q1.** The file (this spec) or the heap feed of section 14? The file keeps the snapshot small
  at high rates. The heap feed is much less code.
- **Q2.** Feature 5 is one-way. Decision 23 of the carry spec says that an activation works both
  ways. Accept the exception?
- **Q3.** After B, the sealer drops a type-0 transaction reference (4.5). A sequencer of an older
  release then loses its offers, unless its twin offers the same transaction. Accept?
- **Q4.** After B, the sessions keep the bytes (4.6). Strip them later, in a separate change?
- **Q5.** The executor groups roll one at a time with the staged roll (option (a) of #781, open
  question 1)?
- **Q6.** The egress stays on the executor nodes for now (#781, open question 2)?
- **Q7.** A fleet rebuild from a seed starts on version 3 with the heap. Accept that the operator
  sends features 3, 4 and 5 again, or extend the seed to carry the rules (#667)?
- **Q8.** The activation time on staging: about 30 minutes ahead at low load, or 0 (at once)?
- **Q9.** This release refuses `kardamom.cluster.carryBytes=true` at the start (4.2). No live
  chain uses it. Accept?

## 16. Docs that change with the code

- `cluster/sealer-service/README.md`: the feature 5 section, the replay window (the posted head
  and B), the settings table (`carryStorage` in place of `carryBytes`), the snapshot table, and a
  runbook "Activate carry storage".
- `docs/failure-modes.md`: the replay window (`:167`), the retained file (`:171-184`), the new
  fatal line, and the seed rebuild after B.
- `docs/formats.md`: the feature form of `activation`, and the sealer snapshot row.
- `deploy/cluster/README.md`: the three switches and the rollout of section 12.
- `docs/specs/2026-10-21-one-way-pipeline.md`: a note at D11 that points to this spec.

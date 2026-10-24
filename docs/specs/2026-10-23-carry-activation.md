# Carry activation: the exact window and the member feed on the heap

- Status: revision 4, approved by the design review on 2026-10-24; accepted by the user ("Let's do
  it and implement e2e", 2026-10-23). Not implemented.
- Issue: #771 (the carried path is not complete on staging). This spec closes gap 3 (the
  switch of a live chain) and gap 1 (the executors read the member feed).
- The user decided on 2026-10-23:
  - A live chain switches by an upgrade transaction: a new feature activation. There is no new
    chain and no stop of all members.
  - Topology A: each executor runs on the node of its paired sealer member, and reads the feed
    of that member on `aeron:ipc`.
  - The upgrade policy holds (#661): a breaking change ships behind a feature flag, and every
    deploy is backward compatible.
- The coordinator decided on 2026-10-24, after the design review of revision 1: the member feed
  and the exact window run on the heap, not in the retained file. This follows D11 of
  `docs/specs/2026-10-21-one-way-pipeline.md:44` and `:59-61`. Section 15 keeps the file as a
  later spec, with a measured trigger.
- Builds on (assumed merged first): #779 (snapshot version 17, the file layout), #781 (the shared
  driver on the sealer nodes and the pinned executor groups), the preflight of #780, and #765
  (the exact window under version 5).
- Section 17 maps each finding of the review of revision 1 to its change, and section 18 each
  finding of the reviews of revisions 2 and 3.
- Also builds on #768 (the `tx_drops` stream and `TxDrop`), for the drop record of section 5.

## 1. The problem

### 1.1 What runs on staging today

- Feature 4 (`FEATURE_SEALER_V5`, the carry flag) is active since 2026-10-21. Every member runs
  decision version 5 (`crates/types/src/upgrades.rs:87-93`, `VersionSwitch.java:72-86`).
- Every member keeps its retained frames on the heap. The deploy does not set
  `kardamom.cluster.carryBytes`.
- Under version 5, a sequencer offers each transaction as a full reference (record type 7). It
  does this only after the sealer reports version 5 on its session (egress kind 17).
  `Sequencer::carried` sends the envelope only when `carries_bytes()` is true
  (`crates/sequencer/src/v2/mod.rs:426-433`).
- The member orders a full reference. It relays one canonical entry (`RT_TX`) with the envelope
  (`SealerClusteredService.java:1170-1180`, `SealerEgress.java:646-647`,
  `EgressFrames.java:49-55`).
- A heap member gives each egress session the frame as it is (`HeapFrames.java:252-259`). So
  each executor on its cluster session already gets the bytes of each `RT_TX` entry. It runs the
  entry with no join (`crates/engine/src/reader/cluster/mod.rs:259-264`).
- A heap member keeps the posted rule under version 5. A frame past the window leaves only when
  its block is posted (`SealerEgress.java:174-177`, `HeapFrames.java:138`). So the heap and the
  snapshot grow with the DA lag. The snapshot holds every retained frame
  (`HeapFrames.java:184-196`).

### 1.2 Why the window and the feed are off on staging

- The exact window needs `retained.carriesBytes()` (`SealerEgress.java:174-177`). Only the file
  says true. #765 added that condition, because the executors and the batcher could still meet a
  reference, and the egress cannot give a reference its bytes.
- The member feed needs the file: `Feed.open` returns `Off` without it (`Feed.java:54-56`), and
  `FeedConfig.parse` refuses a feed without `carryBytes` (`FeedConfig.java:38-46`).
- `carryBytes` is a must-match setting
  (`deploy/cluster/ansible/roles/workloads/defaults/main.yml:333-345`). A member stops on a
  snapshot of the other layout (`SealerClusteredService.java:504-516`).

### 1.3 Findings

- **F1. The executors already get the bytes of a type-7 offer.** #771 gap 1 says that the
  executors read references. That is true only for the entries of a type-0 offer. On a heap
  member under version 5, each entry of a type-7 offer reaches the executors as `RT_TX`, with its
  bytes (1.1).
- **F2. The references come from type-0 offers.** A reference reaches a reader only when a
  sequencer offers a type-0 reference. There are two causes:
  - A sequencer session opens. Until kind 17 of that session arrives, the version of the
    sequencer is 0 (`crates/sequencer/src/sealer_rules.rs:77-84`). So the sequencer sends type 0.
  - The sealer still orders a type-0 reference under version 5 (README "Version 5", "Other
    records").
  The halt of the validator with `--egress` on 2026-10-23 (`TxRef ... not found`) fits these
  references.
- **F3. The heap already holds what the feed needs.** Under version 5, each heap frame of a
  type-7 offer holds the envelope. So the feed can walk the heap frames. No file is needed.
- **F4. The feed is local.** The feed sends frames on its own IPC publication, through a ring
  on its own thread (`MemberFeed.java`). It reads `SealerEgress.replayVerdict`. It writes
  nothing that the state or a cluster session reads. A full ring gaps only the feed. So a deploy
  setting can turn it on, member by member.
- **F5. A carry-mode executor cannot join a reference.** It opens no `tx_data` lane
  (`crates/executor/src/bin/kardamom-executor/main.rs:311-313`). Its reader passes a reference on
  as an ordinary record (`crates/engine/src/reader/carried/answer.rs:210`). The switch between
  its sources has no rule for a fault that every source gives: it switches again and again
  (`crates/engine/src/reader/carried/switch.rs:95-139`, the last arm).
- **F6. A type 7 below version 5 halts the reference-mode executors.** Below version 5, the
  sealer relays a full reference with its record type 7. The Rust decoder refuses that type
  (`crates/cluster-adapter/src/wire/egress.rs:375`, `UnrelayedRecordType`). The cluster reader
  then drops the frame (`crates/engine/src/reader/cluster/mod.rs:285-290`). So the executor has a
  gap at that index and stops. A sequencer with a high version on a chain below version 5 makes
  such a frame. Section 7.4 fixes the decoder.
- **F7. The v2 sequencer has no answer path.** Staging runs the v2 loop of the sequencer
  (`crates/sequencer/src/bin/kardamom-sequencer/main.rs:239-275`, `admission.enabled`). The v2
  loop keeps no candidate after its offer and handles no sealer answer
  (`crates/sequencer/src/v2/mod.rs:417`, `:426-433`). So a refusal of the sealer cannot make it
  offer the transaction again. Section 4.3 and section 5 use a drop record instead.
- **F8. The egress store rebuilds its head from the last segment only.** `Store::open` scans only
  the last segment (`crates/egress/src/store.rs:81-105`, `:290-300`). So a value that the egress
  computes from the frames, such as the highest stored reference, would go back at a restart.
  Section 7.2 keeps it in a side file.

## 2. Goals and limits

- Goal: the exact window of #765 runs on heap members. The heap and the snapshot of a member then
  have a fixed bound at any DA lag.
- Goal: each executor reads the member feed of its paired member. The executors move one at a
  time, with no stop of the chain.
- Goal: no reader of the order meets a reference after the activation.
- Limit: every deploy is backward compatible. Only an upgrade transaction changes a rule of the
  replicated state.
- Limit: no stop of all members, no purge, no new chain.
- Limit: no change to a wire format that an older client reads. The new egress kind goes only
  to a session that offered a type-0 reference.
- Non-goal: the retained file (section 15).
- Non-goal: a network feed in place of `aeron:ipc` (topology B).
- Non-goal: strip the bytes from the cluster sessions. A later change does that (Q4 of revision
  1, accepted).

## 3. The design in short

1. **The readers decode a relayed type 7.** A reader that gets a full reference below version 5
   runs it as a canonical entry. This change ships first, to every reader.
2. **The sequencer keeps its version.** It keeps the highest version that any of its sessions
   reported. It lowers it only on a kind 17 of the current session that says lower. A start
   floor gives the first value. So a sequencer of this release sends no type-0 reference under
   version 5.
3. **The member feed on the heap.** `MemberFeed` walks the heap frames by a frame number, not by a
   file position. A deploy setting turns it on, member by member.
4. **Feature 5 (`FEATURE_CARRY_WINDOW`).** An upgrade transaction schedules it. While version 5
   and feature 5 are both on:
   - the exact window holds on every member;
   - the sealer refuses a type-0 transaction reference. It answers the session with a reject
     frame. The sequencer publishes a drop record for the hash, so the client sees the status
     `dropped` and sends again.
5. **Both ways.** Feature 5 follows the rules of features 3 and 4. It can turn off and on again.
   Decision 23 of the carry spec holds.
6. **Snapshot version 18.** Version 16, then the state and the schedule of feature 5. The heap
   section does not change. A member writes 18 only when it holds an activation of feature 5.
7. **The order.** The readers get the decoder. The sequencers stop the type-0 offers. The batcher
   and the validator move to the egress. Then feature 5 turns on the window. Then the executors
   move to the feed.

## 4. The sealer

### 4.1 Feature 5

- **The id.** `FEATURE_CARRY_WINDOW: u64 = 5` in `crates/types/src/upgrades.rs`, and the same
  constant in `VersionSwitch.java`. The ids in use are 1 to 4
  (`crates/exec-core/src/features.rs:50`, `crates/types/src/upgrades.rs:77-93`). Only the sealer
  follows feature 5. The executor does not read it.
- **The schedule.** `VersionSwitch` gets a third `Schedule`, `window`, next to `v4` and `carry`
  (`VersionSwitch.java:91-95`). `onActivation` takes feature 5 with the same rules of origin and
  epoch (`VersionSwitch.java:140-166`).
- **EARLY.** The state stores an activation of feature 5 only while version 5 runs. This is the
  rule of feature 4 under version 4 (`VersionSwitch.java:156-158`). A record that comes under
  version 3 or 4 changes nothing (outcome `EARLY`).
- **UNSUPPORTED.** A member with the retained file (a chain born with `carryBytes`, snapshot 12
  or 17) never stores an activation of feature 5. The record gets the new outcome `UNSUPPORTED`.
  The member counts it in `kardamom_sealer_feature_unsupported_total` and logs
  `cluster FEATURE-ACTIVATION-UNSUPPORTED`.
  - Why: the snapshot of a file member (12 or 17) has no place for the schedule. A file member
    that stored it would lose it at its next restore, and fork from its peers.
  - The choice is deterministic: `carryBytes` is a must-match setting, so every member of such a
    chain gives the same outcome. The service tells the state at the start
    (`state.fileStore(true)`), before the first entry.
  - The gate refuses feature 5 while any member reports a file store (section 9).
- **The flag.** At each boundary, the state evaluates `window.onAt(...)` as it evaluates the
  version (`VersionSwitch.java:175-201`). The result is the flag `carryWindow` of the next block.
  The rules of feature 5 apply while `carryWindow` is on and the version is 5:
  - The flag can turn off: a later activation with a time ahead turns it off until that time,
    and `2^64 - 1` keeps it off.
  - When version 5 turns off (feature 3 or 4 off), the rules of feature 5 stop. The flag keeps its
    value, so the rules come back with version 5. This is how version 5 keeps the schedule of
    feature 4 today (README "Version 5", "The activation").
- **The view.** `VersionSwitch.View` gets the flag and the newest held activation of feature 5.

### 4.2 The log position where the rules change

There are two rules. They change at two defined points of the same log entry.

- **The record rule (the type-0 refusal, 4.3).** It reads the flag of the state.
  - The state sets the flag at the boundary, inside the log entry.
  - So the rule applies from the first record of the block that the boundary opens.
- **The two kinds of boundary:**
  - A timer tick. The boundary is the whole entry. The next record is in the next entry.
  - A forced boundary. Only an origin record (`KIND_ORIGIN_RECORD`, kind 4) forces a boundary.
    The boundary goes out before the record (`SealerClusteredService.java:1554-1558`,
    `offerAdvance`). The forcing record is an epoch, not a transaction reference. The entry holds
    no other record.
  - A kind-3 batch holds only records of kind 0 (`onBatch`,
    `SealerClusteredService.java:778-800`). So a batch never holds a boundary. The record rule
    is the same for every record of one batch.
- **The eviction rule (the exact window, 4.4).** The egress adopts it at the end of the log entry
  that holds the boundary, in `afterEntry` (`SealerClusteredService.java:432-442`). This is the
  point where the egress adopts a new version today. The frames of that entry go in under the
  old rule. The prune at the end of the entry applies the new rule. For a forced boundary, that
  entry holds the boundary frame and the epoch frame.
- **Every member and every replay.** Both points are functions of the log. A snapshot falls only
  between entries. So a replay, and a restore from a snapshot on either side, switch at the same
  points.
- After a switch of the flag, the member asks for a snapshot, as after a version switch. So a
  replay has a start point on each side.

### 4.3 The type-0 refusal

- While the rules of feature 5 apply, the member refuses a transaction reference of record type
  0 (`RT_TXREF`). It does not order it, and it does not drop it silently.
- **The reject frame.** It answers the offering session with a new egress kind,
  `EGRESS_KIND_REF_REFUSED = 18`:
  `[kind:18][canonical_id:32][sender:20][nonce:u64 LE][decision_version:u8]`. Kind 18 is free
  (`SealerWire.java:331-472`). The frame is 62 bytes.
  - The canonical id is the hash of the transaction. The sequencer needs it for the drop record.
  - The version tells the sequencer that the sealer runs version 5.
- **Why a frame.** With `--ack validated`, the client has the hash before the sealer sees the
  offer. A silent drop would lose a transaction that the client was told is accepted.
- **The sequencer side (section 5).**
  - The sequencer raises its kept version to the version of the frame.
  - It publishes a `TxDrop` for the hash on `tx_drops` (#768), with the new reason
    `SealerRefusedReference`.
  - The egress then gives the hash the status `dropped`. The client sends the transaction again.
    The new submit goes out as type 7.
  - The sequencer re-admits nothing by itself (finding F7).
- **The status `dropped` is provisional for this reason.** The twin sequencer can offer the same
  transaction as type 7. Then the sealer orders the copy of the twin, and its receipt arrives
  after the drop. A receipt of the hash always wins over a drop (#768,
  `crates/types/src/tx_drop.rs`), so the status flips to `executed`.
  - The window: from the drop to the receipt of the copy of the twin, about one receipt latency
    (a few ms on staging, at most the 50 ms target).
  - A resend in that window, or after it, is refused by the nonce guard, because the copy of the
    twin used the nonce. The client then sees `executed` for the first hash.
  - The drop text: `transaction dropped by the sealer, send it again`. Where a code is needed, it
    is the geth code -32000.
  - The client text in the docs of the status: "safe to send again; the status can still become
    executed".
- **When a type 0 comes after the switch.** Only from a sequencer of an older release, or from a
  sequencer with a wrong floor. The gate and the deploy prevent both (sections 9 and 10). If it
  still happens, the client sees the loss. It is not silent.
- **Queues and metrics.** This rule adds no queue. The metrics:
  - `kardamom_sealer_ref_refused_total`: the refusals on each member. The member logs
    `cluster REF-REFUSED` at most once a second.
  - `kardamom_sequencer_ref_refused_total{partition}`: the kind-18 frames that a sequencer got.
  - `kardamom_sequencer_tx_drops_total{reason="sealer_refused_reference"}`: the drop records of
    #768 with the new reason.
- **Other records.** An epoch, a deposit, a remote-origin record and a full reference keep their
  rules. Under version 3 or 4, or with the flag off, a type-0 reference is ordered as today.
- **Why the rule exists.** Section 5 stops the type-0 offers of this release. The rule protects
  the carry-mode executors from a sequencer of an older release, or from a wrong floor.
- **A type 7 below version 5.** The member counts each full reference that it orders below
  version 5 in `kardamom_sealer_full_ref_below_v5_total`. Such a frame comes from a wrong floor
  (section 5). The readers run it (section 7.4).

### 4.4 The exact window

- **The rule.** `windowOnly = version == 5 && (carryWindow || retained.carriesBytes())`.
  - On a heap member, feature 5 turns the window on.
  - On a member of a chain born with the file (version 17), `carriesBytes()` keeps the rule of
    #765. No live chain has such a member.
- **Where the egress computes it.** Today only `setDecisionVersion` computes `windowOnly`
  (`SealerEgress.java:174-177`). `setPostedHead` only prunes with the current value
  (`SealerEgress.java:162-165`). This spec replaces `setDecisionVersion(int)` with one method,
  `setRules(int version, boolean carryWindow)`. It computes `windowOnly`, then prunes. The
  service calls it at these points, in this order:
  1. `openEgress` (`SealerClusteredService.java:542-548`), at a start from genesis, from a seed
     or from a snapshot: first `restore` or `openEmpty`, then `setRules`, then `setPostedHead`.
     So the restored frames are pruned by the rule of the snapshot.
  2. `afterEntry`, at the end of each entry in which the version or the flag changed.
  `setPostedHead` keeps its job: it stores the posted head and prunes with the current rule.
- **The drain.** At the entry that turns the window on, the prune drops each frame past the
  window. The retained frames fall within `retention` frames
  (`kardamom.cluster.retention`, 65,536 by default) at the end of that entry.
- **The bound.** With the window on, the heap holds at most `retention` frames, and the snapshot
  holds the same frames. At about 300 bytes a frame, 65,536 frames are about 20 MB. Today the
  snapshot holds every frame above the posted head.
- **The size of `retention`.** `retention` must cover at least 60 s at the planned rate. A frame
  is one record or one boundary. The sealer seals one block every 2 s on staging
  (`kardamom.cluster.tickMs=2000`, `deploy/cluster/nomad/cluster.nomad.hcl:454`).
  - The rule: `retention >= 60 x (planned tx/s + 0.5)`.
  - **The cap: 131,072 frames.** A replay over a cluster session queues every wanted frame at
    once (`handleReplayRequest`, `SealerEgress.java:269-275`). A session whose backlog passes
    `SessionBacklogs.BYTE_LIMIT`, 64 MiB (`SessionBacklogs.java:74`), is closed
    (`SealerEgress.java:238-244`). So a reader that replays from near the floor of a larger
    window is closed, reconnects, and replays again, with no end.
  - The cap in bytes: `retention x mean frame bytes <= 32 MiB`, half of the backlog limit. The
    mean is `kardamom_sealer_retained_bytes / kardamom_sealer_retained_frames` at the time of the
    deploy. The preflight of C6 checks it.
  - The ceiling in frames: 131,072. It assumes transfers of about 300 bytes a frame: 131,072
    frames are then about 39 MB, below 64 MiB. This design supports no larger value. With larger
    frames, the cap in bytes gives a lower value.
  - The spike of staging at 1,000 tx/s needs 60,030 frames. The default of 65,536
    (`SealerWire.java:501`, `cluster.nomad.hcl:31-34`) covers about 65 s. So the default holds.
  - A load test at 2,000 tx/s needs 120,060 frames: set 131,072, the cap.
  - The cap covers 60 s up to about 2,180 tx/s (131,072 / 60 - 0.5).
  - The 10,000 tx/s goal needs 600,030 frames, about 180 MB. That needs the file spec of section 15, or a change to the
    replay path: a paced replay, or a larger backlog limit that a setting gives. Trigger 3 of
    section 15 fires at about 2,180 tx/s, well before 10,000 tx/s.
  - The deploy sets it: `KARDAMOM_CLUSTER_RETENTION` gives the job variable `cluster_retention`
    (`deploy/cluster/ansible/roles/workloads/defaults/main.yml:154`, `:320`). The operator raises
    it before a load test, with a rolling deploy.
  - `retention` is not a must-match setting, with two conditions:
    1. A rolling change may only raise the value. The gate refuses a lower value unless
       `KARDAMOM_ALLOW_MUST_MATCH` names `retention` (C6).
    2. The value stays at or below the cap. The preflight refuses a value above 131,072.
  - During a roll, two members can have two windows. That changes no decision and no state byte.
    It changes only the frames that each member can replay, and the size of the heap section of
    its snapshot. No protocol compares snapshot bytes across members. The byte equality of
    section 11 holds for members with the same value. The sealer README says this (C9).
- **Off again.** When the flag turns off, the posted rule comes back at the end of that entry.
  No frame comes back: the window keeps its frames, and new frames stay until they are posted.

### 4.5 The member feed on the heap

- **The walk.** `MemberFeed` walks the retained file today by its position: `scanStart`,
  `frontPos`, `view` and `next` (`MemberFeed.java:152-158`, `:208`). This spec moves these four
  calls to `RetainedFrames`, with a walk key of type `long`:
  - `RetainedFile`: the key is the file position, as today.
  - `HeapFrames`: the key is the frame number, the count of frames that the store appended
    since its start. The front is the appended count minus the frame count. The frame at key `k`
    is in the index slot `(first + k - front) % slots`.
  - `walkStart(fromBlock)` on the heap: the key after the retained boundary of `fromBlock - 1`,
    else the front. It scans the index once for each feed request. Requests are rare.
  - `view(key, buffer)` wraps the bytes of the frame without a copy. `HeapFrames.grow` moves the
    bytes (`HeapFrames.java:84-95`), so the walk wraps the view again at each step. The key does
    not change when the ring grows.
- **One thread.** The feed runs its walk on the service thread (`Feed.java:6-10`), as today. The
  heap arrays have no reader on another thread.
- **The settings.** `FeedConfig.parse` no longer needs `carryBytes` (`FeedConfig.java:38-46`).
  `Feed.open` opens the feed on the store of the member, heap or file (`Feed.java:54-56`).
  `kardamom.cluster.feedAeronDir` is not a must-match setting.
- **The floor.** The feed answers a cursor below the egress floor with `REPLAY_UNAVAILABLE`, as
  today. No other floor exists: the heap holds every frame from the egress floor.
- **Before feature 5.** The feed can run, but the frames below the cursor of an executor can hold
  references (F2). An executor moves to carry mode only after its cursor passes the last
  reference (section 6).

### 4.6 The snapshot: version 18

- **A new version is needed.** The flag and the schedule of feature 5 are replicated state. The
  state of version 16 holds neither. A member that restored version 16 would lose a held
  activation, refuse no type-0 reference, and keep the posted rule, while its peers do not. That
  forks the state.
- **The layout.** Version 18 is the state of version 16, then the section of feature 5, then the
  heap section of today:

| Section | v16 (staging today) | v18 |
|---|---|---|
| State of v16 | yes | yes |
| Feature 5 | no | `[carry_window:u8][held:u8]`, then `[origin:u64][time_ms:u64]` for each held activation, oldest first, at most three |
| Heap section | `HeapFrames` layout (`HeapFrames.java:184-196`) | the same |

- **When a member writes 18.** When the state holds an activation of feature 5. Before the first
  kind 11 of feature 5, a member writes 14, 15 or 16 as today. So the release changes no
  snapshot byte until the upgrade transaction.
- **Restore.** This release reads 1 to 18:

| Snapshot | Who writes it | Restore on a heap member | Restore on a file member |
|---|---|---|---|
| 1 to 11, 13 | older releases | heap store, flag off, no schedule | stop: `sealer RETAINED-FILE FATAL` (today) |
| 12 | a file member of an older release | stop (today) | file store, the rules of version 3 (#779). Feature 5: `UNSUPPORTED` |
| 14, 15, 16 | heap members, today | heap store, flag off, no schedule | stop (today) |
| 17 | a file member (#779) | stop (today) | file store, the rules of v16. Feature 5: `UNSUPPORTED` |
| 18 | a heap member that holds an activation of feature 5 | heap store, the flag and the schedule | stop: a file member never writes 18 |

- **Rollback.** A member of a release that reads only up to 17 stops on 18. The preflight of #780
  refuses such a rollback (`reads_max` below `snapshotLatest`).
- **Equality.** Every member writes the same bytes at the same log position. This holds for the
  retained frames too, because each member evicts the same frames at the same position (4.2).
- **`formats.toml`.** `sealer-snapshot`: `reads_max = 18`. The activation is feature 5, with
  `writes = 18`. `docs/formats.md` gets a feature form of `activation`
  (`activation = { feature = 5, writes = 18 }`) next to the flag form.
  `sealer-egress-kinds` gets kind 18.

### 4.7 The admin status and the gauges

`MemberStatus.toJson` (`MemberStatus.java:120-137`) gets these fields:

- `retainedStore`: `heap` or `file`.
- `carryWindow`: `true` when the rules of feature 5 apply.
- `carryWindowActivationOrigin` and `carryWindowActivationMs`: the newest held activation of
  feature 5, 0 when none.
- `retainedFrames` and `retainedBytes`.
- `feed`: `off`, `idle`, `walking`, `live` or `gapped`.

The metrics page gets the same values as Prometheus gauges, so the rollout and the alerts can
read them:

| Gauge | Value |
|---|---|
| `kardamom_sealer_retained_frames` | the retained frame count |
| `kardamom_sealer_retained_bytes` | the bytes of the retained frames |
| `kardamom_sealer_retained_store` | 0 heap, 1 file |
| `kardamom_sealer_window_exact` | 1 when `windowOnly` is true, else 0 |
| `kardamom_sealer_decision_version` | the version of the open block |
| `kardamom_sealer_feature_state{feature="3"\|"4"\|"5"}` | 0 no activation held, 1 held and off, 2 on |
| `kardamom_sealer_feature_activation_ms{feature}` | the time of the newest held activation |
| `kardamom_sealer_feature_unsupported_total` | the records of feature 5 on a file member |
| `kardamom_sealer_snapshot_bytes` | the size of the newest snapshot |
| `kardamom_sealer_snapshot_version` | the version of the newest snapshot |
| `kardamom_sealer_ref_refused_total` | the type-0 refusals |
| `kardamom_sealer_full_ref_below_v5_total` | the full references ordered below version 5 |
| `kardamom_sealer_feed_state` | 0 off, 1 idle, 2 walking, 3 live, 4 gapped |

## 5. The sequencer

All of this section is the v2 loop, which staging runs
(`crates/sequencer/src/bin/kardamom-sequencer/main.rs:239-275`). The v1 loop does not change.

- **The kept version.** `SealerVersion` keeps the highest version that any session of the
  process reported.
  - A kind 17 of the current session lowers it only when it says lower.
  - A new session does not reset it to 0. Today `SealerRules::on_session_start` does
    (`crates/sequencer/src/sealer_rules.rs:77-84`).
  - There is no hold, no deadline and no queue.
- **The first value.** A new process has no report yet. It starts from the setting
  `--sealer-version-floor` (env `KARDAMOM_SEALER_VERSION_FLOOR`, default 0).
  - The deploy sets the floor to the lowest `decisionVersion` of the members at the time of the
    deploy. On staging that is 5.
  - The deploy sets a floor above 0 only when every reader runs the decoder of section 7.4
    (section 10.1). A wrong floor then costs no halt.
  - A kind 17 that says lower lowers the version. So a wrong floor lasts until the first kind 17,
    a few milliseconds after the session opens.
  - Before a fleet rebuild from a seed, the runbook sets the floor to 0. A seeded member runs
    version 3 (#667).
- **The result.** A sequencer of this release sends no type-0 reference while every member runs
  version 5. So the references of F2 stop with this release, before feature 5.
- **Kind 11 of feature 5.** The list of followed features gets feature 5
  (`crates/sequencer/src/outbound/cluster.rs:152`). The rule of the record does not change.
- **Kind 18.** The egress feed thread of the sequencer (`EgressWatermarkFeed`,
  `crates/sequencer/src/bin/kardamom-sequencer/feeds.rs:128`) reads kind 18 next to kind 17
  (`on_rules_frame`, `feeds.rs:275`). For each frame it does three things:
  1. It raises the kept version to the version of the frame.
  2. It publishes a `TxDrop` (#768, `crates/types/src/tx_drop.rs`) for the canonical id of the
     frame on `tx_drops` of its lane, with the sender, the nonce and the reason
     `DropReason::SealerRefusedReference`.
  3. It counts `kardamom_sequencer_ref_refused_total`.
  - The new reason goes at the end of `DropReason`: its layout is frozen. Its text is
    `transaction dropped by the sealer, send it again`, code -32000. The status is provisional
    (4.3).
  - The feed thread uses a clone of the `TxDropsPublisherHandle` of the publish loop, not a
    second publication. The handle is `Clone`: the macro `declare_channel_handles!` makes a
    `Clone` publisher struct around one `PubHandle` (`crates/log/src/aeron_live/handles/simple.rs`
    on #768, the doc of the macro and `:273-286`).
  - `publish_best_effort` does not wait. The Aeron thread retries a back-pressured offer for a
    bounded time, then drops the frame and counts it. So the feed thread gets no new queue.
  - The v2 loop keeps no candidate after its offer (finding F7), so it re-admits nothing. The
    client sends the transaction again, and it goes out as type 7.
- **Job meta.** The sequencer job gets the meta `sealer_version_kept = "true"` for the gate.

## 6. The executors

- **The move.** Each executor moves to the node of its paired member first, in reference mode
  (`KARDAMOM_EXEC_ON_SEALERS`, section 10). Later it moves to carry mode (`--carry-bytes`) on the
  heap feed of its member.
- **When an executor can move to carry mode.** All of these must be true:
  - The rules of feature 5 apply on every member.
  - Its member runs the feed (`kardamom_sealer_feed_state` above 0).
  - Its committed block, minus the compare window (`rollback_compare_blocks`, 32 by default,
    `crates/executor/src/bin/kardamom-executor/args.rs:17`), is above the block of the last
    reference in the egress (`kardamom_egress_ref_frame_max_index`, 7.2). At its first carry
    start, the executor compares the stream with the committed blocks of that window
    (`turns.rs:251-255`). So every frame that it reads holds its bytes.
- **The first carry run.** The executor has no running marker and no ledger yet. `start_run`
  takes a missing marker as a whole run (`marker.rs:172-186`), and the ledger opens empty. So the
  first run needs no migration.
- **No hybrid code, one guard.** A carry-mode executor reads the feed, then the egress
  (`--egress`), then a peer checkpoint (`args.rs:115-123`). It gets one change:
  - The carried reader takes a type-0 reference as a fault of its source,
    `Fault::Unjoinable { index }` (`answer.rs:210`). The switch goes to the other source.
  - When both sources give `Unjoinable` at the same index, `Switch::verdict`
    (`switch.rs:95-139`) gives a new verdict, `Verdict::Unjoinable { index }`. The reader stops
    with `ExecutorError::UnjoinableReference { index }`. Without this verdict, the reader switches
    between the sources with no end.
  - In normal operation the fault never fires. It needs a reference above the cursor, which only
    a sequencer of an older release or a wrong floor can make.
- **Back to reference mode.** An executor can move back at any time. Its cluster session gets the
  bytes, as today. So the move is reversible, one executor at a time.

## 7. The readers

### 7.1 The batcher and the validator

- With the exact window, a batcher or a validator below the window gets `REPLAY_UNAVAILABLE` from
  the sealer. So both must read the egress (`--egress`) before feature 5 turns on
  (`crates/batcher/src/bin/kardamom-batcher.rs:137`,
  `crates/validator/src/bin/kardamom-validator/args.rs:64`).
- A reader can move to the egress when its cursor is above `kardamom_egress_ref_frame_max_index`.
  Below it, the egress range holds a reference that the reader cannot join.
- New gauges, for the gate and the rollout: `kardamom_batcher_egress_cursor_index` and
  `kardamom_validator_egress_cursor_index`, the canonical index of the newest frame that the
  reader took from the egress.

### 7.2 The egress

- The egress stores what the executors forward on `exec_frames`. It holds a reference for each
  type-0 offer of F2.
- **R, the highest stored reference.** Two gauges: `kardamom_egress_ref_frame_max_index`, the
  highest canonical index of a stored `RT_TXREF` frame, and `kardamom_egress_ref_frame_max_block`,
  its block. After the sequencer release of section 5, R stops moving.
- **R is durable.** `Store::open` rebuilds the head from the last segment only (finding F8). So R
  goes in a side file of the store, `ref-mark`: `[index:u64 LE][block:u64 LE]`, 16 bytes.
  - `append_one` (`crates/egress/src/store.rs:211-224`) notes the highest `RT_TXREF` frame of
    the batch. `append` (`store.rs:118-119`) writes the mark once per batch, when that frame is
    above the mark.
  - It writes a temporary file, syncs it, and renames it over `ref-mark`. A crash leaves the old
    mark or the new mark, never a part.
  - Implementation note (2026-10-24): the file holds the mark plus a lead of 65,536 indexes and
    32 blocks, and the store writes it only when the mark passes it. Two syncs for each batch
    stalled the owner thread of the egress under the v1 loop, and with it `egress_receipts`. The
    mark in memory stays exact. A clean stop writes the exact mark. After a crash R is at most the
    lead above the stored references.
  - The order: the frames of the batch, then the mark, then the sync of the segment
    (`store.rs:128`). So after a crash the mark is at or above every reference that the segment
    holds. A mark above the segment only delays a switch. It never lets a reader meet a
    reference.
  - The hook in `append_one` also covers the catch-up frames of `Input::Caught`. They reach the
    store through the same `append` (`take_caught` and `flush`, `crates/egress/src/owner.rs:228-235`).
  - `Store::open` reads the mark. A missing mark is `(0, 0)`: no reference is stored.
  - A store copied from a peer at a scale-up (decision 27) copies the mark with the segments.
  - `Owner::start_at_floor` resets the store to the floor of a peer (`rebase`,
    `crates/egress/src/owner.rs:298-302`, then `Store::reset`, `store.rs:185-192`). `reset` keeps
    the higher of its own mark and the mark of the peer, when the start comes with one. It never
    lowers the mark. A mark that stays high only delays a switch of a reader. It never lets a
    reader meet a reference.
- No other change. The egress compares the copies of the executors as today.

### 7.3 Why the order is not circular

1. The readers get the decoder of 7.4. It needs no feature.
2. The sequencer release (section 5) stops the type-0 offers. It needs no feature either.
3. So R stops moving on each egress instance. R is durable (7.2), so a restart of the egress does
   not lower it.
4. The readers move to the egress once their cursor is above R. They need no feature either.
5. Feature 5 turns on the window only after step 4. The gate checks step 4 (section 9).
6. The executors move to the feed after feature 5. The refusal of 4.3 then guarantees that no
   reference comes after R.

No step waits for a later step.

### 7.4 The decoder of a relayed type 7

- Below version 5, the sealer relays a full reference with record type 7 (finding F6). The
  payload holds the canonical id, the reference fields, and the envelope.
- `RelayedPayload::into_item` (`crates/cluster-adapter/src/wire/egress.rs:368-381`) decodes a
  relayed `RT_TXREF_OFFER` as `EgressItem::Tx`. It reads the payload with a payload form of
  `TxRefOffer::parse` (`crates/cluster-adapter/src/wire/entry.rs:171-211`), which starts at the
  canonical id. Then `TxRefOffer::canonical` (`entry.rs:237`) gives the `CanonicalTx`: the id,
  the lane and the envelope.
- A relayed `RT_TX_OFFER` (type 5) stays refused. No sequencer sends type 5.
- **The hazard that stays.** A relayed type 7 whose envelope does not hash to its id decodes to
  `BadTx`, and the reader drops it. The reader then has a gap at that index. This is accepted:
  the sequencer is trusted (decision 23 of the carry spec), and only a sequencer writes type 7.
  The counter `kardamom_sealer_full_ref_below_v5_total` (4.3) shows each type 7 below version 5.
- Every reader of the order then runs the entry with no join: the cluster reader of an executor
  (`crates/engine/src/reader/cluster/mod.rs:259-264`), the carried reader, the validator, the
  batcher and the egress.
- This change is backward compatible: it only accepts a frame that is refused today.
- It must reach every reader before the deploy sets a sequencer floor above 0 (section 10.1).
  So it ships in the first PR (C0), and each reader job gets the meta
  `relayed_full_ref = "true"`.

## 8. The cases

| Case | What happens |
|---|---|
| A member stops before the switch and starts after it | It restores its snapshot from before the switch (14 to 16, or 18 with a held activation). It replays the log and switches at the same entry as its peers (4.2). Its frames and its snapshot bytes then equal those of its peers. |
| A member restores a snapshot from after the switch | Version 18 with the flag on. `openEgress` restores the frames, then applies the exact window, then the posted head (4.4). |
| A blank member seeded by a peer | Aeron backup copies a snapshot and the log. The snapshot holds the frames. No file copy, no token. |
| A member of an older release at the switch | It ignores kind 11 of feature 5. It keeps the posted rule. It orders the type-0 references that its peers refuse. That forks the state. The gate refuses feature 5 while such a member runs (section 9). |
| A rollback of the release after the first kind 11 of feature 5 | The members write version 18. A release that reads only up to 17 stops on it. The preflight of #780 refuses the rollback. Before that kind 11, a rollback is safe. |
| Feature 5 turns off, then on again | Off: the posted rule comes back, and type 0 is ordered again. On: the window drains to `retention` frames at the end of the entry, and type 0 is refused again. Every member switches at the same entries. |
| Feature 3 or 4 turns off while feature 5 is on | Version 5 stops, so the rules of feature 5 stop. The sequencers send type 0 under version 4, and the carry-mode executors stop with `UnjoinableReference`. The gate refuses such a transaction while feature 5 is on (section 9). |
| A leader change just after the switch | The sessions reconnect and replay from their cursors. A cursor inside the window gets its frames. A cursor below it gets `REPLAY_UNAVAILABLE`. |
| A reference-mode executor below the window | `REPLAY_UNAVAILABLE`. It has no egress source, so it takes a peer checkpoint (`docs/failure-modes.md:472`). |
| A carry-mode executor below the window | It reads the egress range, then a peer checkpoint. |
| A sequencer of an older release, or a wrong floor, after the switch | Its type-0 offers get kind 18. A sequencer of this release publishes a `TxDrop`, and the client sees `dropped` and sends again. An older sequencer drops kind 18 as an unknown kind, and the transaction stays unordered unless its twin offers it as type 7. The client then sees no receipt. The gate requires the sequencer release. |
| A sequencer floor above the version of the chain | The sequencer offers type 7 until the first kind 17 lowers its version. The sealer orders each such offer below version 5 as type 7, and counts it. Every reader with the decoder of 7.4 runs it. |
| A member with the retained file (snapshot 12 or 17) gets kind 11 of feature 5 | Outcome `UNSUPPORTED`, counted, never stored (4.1). Every member of such a chain gives the same outcome. The gate refuses feature 5 on such a chain. |
| An egress instance restarts | It reads R from `ref-mark` (7.2). R does not go back, so the gate and the preflight do not see a false low value. |
| Every egress instance is down while the window is exact | A reader below the window cannot recover until an egress instance is back. The DA-lag guard does not change. The distance alert of the egress (one-way spec 6.4) fires first. |
| A member without the feed setting | It runs no feed. Its executor stays in reference mode. Nothing else changes. |
| The feed ring is full | The feed gaps (`GAPPED`). The executor asks again. The state and the sessions do not change. |
| A fleet rebuild from a seed | A seeded member runs version 3 with no feature (`SealerSeed` holds no rules). The runbook sets the sequencer floor to 0, moves the executors to reference mode, and sends features 3, 4 and 5 again. #667 stays open for the seed with the rules. |
| A power loss on a majority after the move | The carry-mode executors roll back by themselves (`docs/failure-modes.md:929-933`). This is a gain of the move. |

## 9. The gate

`just sealer-upgrade-check <env> 5 <activation_ms>` (`upgrade_check.yml`) gets these checks for
feature 5:

- Every configured member reports `snapshotReadsMax >= 18`.
- Every configured member reports `decisionVersion == 5`. Below version 5 the record is `EARLY`.
- Every configured member reports `retainedStore = heap`. A file member answers `UNSUPPORTED`
  (4.1).
- The sequencer job has the meta `sealer_version_kept = "true"`, and every live allocation runs
  that job version (`upgrade_check_job.yml`).
- The batcher and validator jobs have the meta `reads_egress = "true"`, and every live allocation
  runs that job version.
- **The readers are past R.** The check reads two kinds of value:
  - R from each egress instance: `kardamom_egress_ref_frame_max_index` on the metrics endpoint
    of each instance. It takes the highest R of all instances.
  - The cursor of each reader: `kardamom_batcher_egress_cursor_index` on each batcher, and
    `kardamom_validator_egress_cursor_index` on each validator, from the metrics endpoint of
    each reader.
  - It refuses unless every reader cursor is above the highest R. It refuses while an egress
    instance or a reader does not answer.

New rule for features 3 and 4: refuse any activation time other than 0 while any member reports
`kardamom_sealer_feature_state{feature="5"}` at 1 or 2. A switch back of version 5 would stop
the carry-mode executors. To turn feature 3 or 4 off, turn feature 5 off first, and move the
executors to reference mode.

Feature 5 itself can turn off: the check allows any activation time for feature 5.

The gate tests go to `deploy/cluster/ansible/tests/test_deploy.py`, next to the feature-4 tests.

## 10. The deploy

### 10.1 How #781 changes

#781 makes `KARDAMOM_CARRY_BYTES=on` one switch for the sealer and the executors. It sets
`carryBytes`, the token, the feed and the pinned groups, and it needs
`KARDAMOM_ALLOW_MUST_MATCH=carryBytes`. With the heap feed, three switches replace it:

| Switch | What it sets | When |
|---|---|---|
| `KARDAMOM_MEMBER_FEED=on` (replaces `KARDAMOM_CARRY_BYTES`) | On each member: `feedAeronDir` and `feedRingBytes`. The shared driver on the sealer nodes. No `carryBytes`, no token. | Any time after the release. A rolling deploy, member by member. No allowance. |
| `KARDAMOM_EXEC_ON_SEALERS=on` (new) | The executor groups `executor-<i>`, pinned to `sealer-<i>`, in reference mode. The readers that reach an executor by its node follow it. | Any time after the release. |
| `KARDAMOM_CARRY_EXECUTORS=<list>` (new) | `--carry-bytes` and `--egress` on each listed group, such as `0`, then `0,1`, then `0,1,2`. | After feature 5, one executor at a time. |

- **The token goes.** The Nomad variables of each member group and the template of #781 are not
  needed. The admin file endpoint and the file copy are not used.
- **The executor roll.** The pinned groups would roll together, because Nomad orders no groups
  (#781, open question 1). The executor job uses the staged roll of the sealer
  (`sealer_step.yml`), one group at a time, with a health check between groups (option (a),
  accepted).
- **The sequencer floor.** The deploy reads `decisionVersion` on every member and passes the
  lowest value as `KARDAMOM_SEALER_VERSION_FLOOR`. It refuses a floor above 0 unless every reader
  job (executor, validator, batcher, egress) has the meta `relayed_full_ref = "true"`, and every
  live allocation runs that job version (section 7.4).
- **The preflight of `KARDAMOM_CARRY_EXECUTORS`.** It refuses the list unless:
  - every member reports `carryWindow` true;
  - the member of each listed executor reports `kardamom_sealer_feed_state` above 0;
  - each listed executor reports `kardamom_executor_block_number`, minus 32, above
    `kardamom_egress_ref_frame_max_block`;
  - the executor job has the meta `carried_ref_fault = "true"`;
  - `KARDAMOM_EXEC_ON_SEALERS=on`.
- **The preflight of #781 stays** for the exec cursor and the executor count equal to the sealer
  count. Its checks of the token and the snapshot interval go.

### 10.2 What staging needs (kardamom-infra)

The list of #781, without the file:

- On each sealer node: the directories of the executor and the state mirror, room in the
  `aeron-mount` tmpfs for the shared driver and the 24 MiB feed log, and memory for the member,
  the shared driver (768 MB), the executor (1,536 MB), the state mirror (512 MB) and the page
  cache of the state DB.
- The ports 9004, 9014, 9024 and 9007 open between the nodes.
- No disk for a retained file, and no secret for a token.
- The executor nodes stay: the egress runs there (#781, open question 2, accepted).

## 11. Tests

### 11.1 Java unit tests (`DriverlessLog`)

- **Byte equality at every cut.** Three members apply one stream: type-7 offers, type-0 offers,
  kind 11 of feature 5, the switch, and a DA lag. After each entry, the retained frames and the
  snapshot bytes of the three members are equal, byte for byte.
- **Restore at each point.** For each entry, one member takes a snapshot, stops and restores (the
  pattern of `SealerVersionFiveTest` in #779). The cuts cover: before the schedule, with the
  schedule held, the entry of the switch, the entry after it, and with the window on. The
  restored member gets the same egress outcomes, replay answers, status, retained frames and
  final snapshot bytes as its peer.
- **A member stopped before the switch and started after it.** It replays the switch from the
  log and ends with the bytes of its peers.
- **The forced boundary.** An origin record (kind 4) forces the boundary that turns feature 5
  on. The boundary frame and the epoch frame go in under the old eviction rule. The prune at
  the end of that entry applies the window. The first type-0 reference in the next entry gets
  kind 18. The bytes of the three members stay equal.
- **A batch after the switch.** A kind-3 batch of type-0 and type-7 records, after the switch:
  each type-0 record gets kind 18, and each type-7 record is ordered.
- **The drain.** Under a DA lag, the retained frames fall within `retention` frames at the end
  of the entry that turns the window on.
- **Off and on again.** Feature 5 turns off, then on. The posted rule comes back, then the
  window. Each member switches at the same entries, and the bytes stay equal.
- **The refusal.** With the rules of feature 5, a type-0 reference gets kind 18 on its session,
  with its canonical id and version 5, and is not ordered. With the flag off, or under version
  4, it is ordered.
- **A type 7 below version 5.** It is ordered, relayed with type 7, and counted in
  `kardamom_sealer_full_ref_below_v5_total`.
- **UNSUPPORTED.** A file member (snapshot 17) gets kind 11 of feature 5: outcome
  `UNSUPPORTED`, the counter moves, and the snapshot bytes do not change.
- **The restore table.** One case for each row of 4.6, versions 12 and 17 included.
- **The feed on the heap.** A feed request inside the window gets every frame from its cursor.
  A request below the floor gets `REPLAY_UNAVAILABLE`. A walk across a growth of the byte ring
  gives the same frames. A walk while the window evicts ends with the frames, or gaps
  (`MemberFeed.java:208-211`), and the next request serves.
- **Allocation.** `CarriedPathAllocationTest`: the type-7 path allocates nothing with the feed on.
- **Formats.** `FormatRegistryTest` reads 1 to 18. A new case checks a golden v18 snapshot: the
  bytes that the PR of the layout writes. A later change to those bytes fails the test, unless it
  takes a new version number.

### 11.2 Rust unit tests

- The decoder of 7.4: a relayed type-7 frame on shared golden bytes decodes as `EgressItem::Tx`
  with the id, the lane and the envelope. A relayed type 5 stays refused. The cluster reader of
  an executor runs the entry with no join, and the validator and the batcher read it.
- The sequencer keeps version 5 across a new session, lowers it on a kind 17 of the current
  session that says 4, and starts from the floor.
- The sequencer sends kind 11 of feature 5 in front of its epoch.
- On kind 18, the sequencer raises its version and publishes one `TxDrop` with the reason
  `SealerRefusedReference` and the hash of the frame. It re-admits nothing.
- The egress gives the hash of that drop the status `dropped`.
- Kind 18 decodes on the shared golden bytes (`WireGoldenTest` and the Rust wire tests).
- The carried reader faults on a type-0 reference. The switch goes to the other source.
- Two `Unjoinable` faults at the same index stop the reader with
  `UnjoinableReference { index }`. Faults at two different indexes do not.
- The egress store: a stored reference moves R. A restart of the store reads the same R from
  `ref-mark`, also when the reference is in an older segment than the last one. A store with no
  mark opens with `(0, 0)`.
- The egress store: a `reset` after a stored reference keeps R. A `reset` with a higher mark of
  the peer raises R. A `reset` with a lower mark of the peer keeps R.
- The reader cursor gauges.

### 11.3 Multi-process e2e (`crates/e2e`)

A new scenario in `crates/e2e/src/scenarios/carry_window.rs`, run from
`crates/e2e/tests/chain_semantics/upgrades.rs` on the stack of `upgrade_stack_config`:

1. Start three members. Give member 0 the feed, on its own driver directory: the feed uses fixed
   stream ids on IPC.
2. Send features 3 and 4 with `upgrade_and_await` (`crates/e2e/src/scenarios/upgrade.rs`). Wait
   for `kardamom_sequencer_sealer_version` 5.
3. Restart a sequencer. It sends no type 0: R does not move. Restart the egress: R stays the same.
4. Send transfers. Send feature 5. Wait for `carryWindow` on every member.
5. Restart executor 0 with `--carry-bytes` on the driver of member 0. Send transfers. Every
   receipt lands, and the validator agrees.
6. Restart member 1 from its snapshot. Receipts keep landing.
7. Turn feature 5 off, then on again. Receipts keep landing.

A second case starts a sequencer with floor 5 on a chain on version 4. Its first offers go out as
type 7. Every executor and the validator run them, and every receipt lands.

### 11.4 The container shard `carry` (CI only)

The plan of #781 ("E2E shard"), with these changes:

- Deploy with `KARDAMOM_MEMBER_FEED=on` and `KARDAMOM_EXEC_ON_SEALERS=on`, and the batcher and
  the validator on `--egress`.
- After `up`: deploy a lockbox (`kardamom-deploy`). Send features 3, 4 and 5. Wait for
  `carryWindow`.
- Move the executors to carry mode one at a time with `KARDAMOM_CARRY_EXECUTORS`.
- Run `smoke_gate`, 60 s at 100 tx/s, the converged-executors check, and `assert_persisted_state`.
- Chaos cases: kill the leader within 2 s after the switch; stop a member before the switch and
  start it after; turn feature 5 off and on under load; restart an egress instance.

### 11.5 Gate tests

`test_deploy.py`: one case for each check of section 9 and each preflight of 10.1, with no job
write on a refusal. These cases are in the list:

- refuse feature 3 off, and feature 4 off, while feature 5 is held or on;
- refuse feature 5 while a member reports `retainedStore = file`;
- refuse feature 5 when one reader cursor is at or below the highest R of the egress instances;
- refuse a sequencer floor above 0 while a reader job lacks the meta `relayed_full_ref`.

## 12. The staging rollout

Each step names its checks. Do not start a step until the checks of the step before pass.

1. **Deploy the release.** It holds every PR of section 13. No switch of section 10 is on, and
   the sequencer floor is 0.
   - Each member: `snapshotReadsMax` 18, `decisionVersion` 5, `snapshotLatest` 16,
     `retainedStore` heap, `kardamom_sealer_window_exact` 0.
   - Every reader job (executor, validator, batcher, egress) has the meta
     `relayed_full_ref = "true"`, and every live allocation runs it.
   - No `FATAL` line in the sealer logs.
   - Rollback: allowed. Nothing wrote version 18.
2. **The sequencer floor.** Deploy the sequencers with the floor from the members (5).
   - Each sequencer: `kardamom_sequencer_sealer_version` 5.
   - `kardamom_sealer_full_ref_below_v5_total` stays 0.
3. **The references stop.** Restart one sequencer of each pair. Then watch R on each egress
   instance for 30 minutes under the canary. It must not move. Write down the highest R and its
   block.
4. **The readers move to the egress.**
   - Before the deploy: the posted cursor of the batcher (`kardamom_batcher_last_posted_block`)
     and the block of the validator are above the highest `kardamom_egress_ref_frame_max_block`
     of the egress instances.
   - Deploy the batcher and the validator with `--egress`.
   - After the deploy, the proof: each reader cursor gauge is above the highest R.
   - The validator agrees, and the batcher posts.
5. **Infra and the feed.** Apply the kardamom-infra change of 10.2. Deploy with
   `KARDAMOM_MEMBER_FEED=on`. The deploy rolls member by member.
   - Each member: `kardamom_sealer_feed_state` 1 (idle).
   - The shared driver runs on each sealer node.
6. **The executors move to the sealer nodes.** Deploy with `KARDAMOM_EXEC_ON_SEALERS=on`. The
   groups roll one at a time.
   - Each executor runs on `sealer-<i>`. `kardamom_executor_block_number` follows the sealed head.
   - Receipts land (the canary). `kardamom_egress_copy_mismatch_total` does not move.
7. **The upgrade check.** Pick the activation time: about 30 minutes ahead, at low load. Run
   `just sealer-upgrade-check staging 5 <activation_ms>`. It must pass.
8. **The upgrade transaction on Sepolia. THE USER MUST APPROVE THIS STEP EXPLICITLY.** The Safe
   signs `ETHLockbox.initiateUpgrade(5, <activation_ms>)`. Nothing in this step happens without
   that approval.
   - Each member logs `cluster FEATURE-ACTIVATION` for feature 5 with outcome `STORED`.
   - `kardamom_sealer_feature_state{feature="5"}` is 1 on each member.
   - From here, `snapshotLatest` is 18, and the preflight of #780 refuses a release rollback.
9. **The switch.** At the activation time:
   - Each member logs `cluster CARRY-WINDOW on` at the same block.
     `kardamom_sealer_feature_state{feature="5"}` is 2, and `kardamom_sealer_window_exact` is 1.
   - `kardamom_sealer_retained_frames` falls within `retention` frames on each member.
   - `kardamom_sealer_snapshot_bytes` falls at the next snapshot.
   - `kardamom_sealer_ref_refused_total` stays 0. Receipts keep landing.
10. **The executors move to carry mode, one at a time.** `KARDAMOM_CARRY_EXECUTORS=0`, then
    `0,1`, then `0,1,2`. The preflight checks the block of each executor against the block of R.
    - The log line `canonical entries via the member feed`.
    - `kardamom_sealer_feed_state` 3 (live) on the member of the executor.
    - Receipts land. `kardamom_egress_copy_mismatch_total` does not move. No `Unjoinable` fault.
11. **Soak for 24 hours** under the canary. Record the gauges of section 15. Then close #771.

Before a load test above 1,000 tx/s, raise `retention` by the rule of 4.4, with a rolling deploy:
131,072 for 2,000 tx/s, the cap. A load test above about 2,180 tx/s needs the file spec of section
15 or a change to the replay path (4.4).

Drills on staging (a leader kill after the switch, feature 5 off and on) need the approval of
the user, and are not part of this plan. The chaos shard covers them in CI.

## 13. Pull requests

Each PR passes `just style` and CI alone, and follows `docs/STYLE.md`. One PR defines snapshot
version 18 in full and writes it. No later PR changes those bytes under the same number.

| PR | Title | Depends on | Size |
|---|---|---|---|
| C0 | cluster-adapter: decode a relayed type 7 as `EgressItem::Tx`; reader job meta `relayed_full_ref` | none | S |
| C1 | wire: egress kind 18 `REF_REFUSED`, Java and Rust codecs on shared golden bytes, `sealer-egress-kinds` to 18 in `formats.toml` | none | S |
| C2 | sealer: feature 5, the flag and the schedule, `UNSUPPORTED` on a file member, snapshot 18 (the whole layout, with its golden test), the type-0 refusal, the counter of a type 7 below version 5, `setRules` and the exact window, admin and Prometheus gauges, `sealer-snapshot` in `formats.toml` | #779, C1 | L |
| C3 | sealer: the member feed on the heap (the walk on `RetainedFrames`, `FeedConfig` and `Feed` without the file), the feed gauge | C2 | M |
| C4 | types, sequencer: `FEATURE_CARRY_WINDOW`, kind 11 of feature 5, the kept version and its floor, kind 18 with the `TxDrop` and `DropReason::SealerRefusedReference`, job meta | #768, C0, C1 | M |
| C5 | engine, executor, egress, batcher, validator: the `Unjoinable` fault and verdict, R with its durable mark and its `reset` rule, the reader cursor gauges, job metas | none | M |
| C6 | deploy: the three switches of 10.1, the staged executor roll, the sequencer floor and its check, the preflights, the feature-5 and feature-3/4 checks of `upgrade_check.yml`, the raise-only check of `retention` and its cap in frames and in bytes | #781, #780, C0 to C5 | L |
| C7 | e2e: the carry window scenario and the floor case | C3, C4, C5 | M |
| C8 | chaos: the `carry` shard with the activation step | C6, C7 | L |
| C9 | docs: `cluster/sealer-service/README.md` (feature 5, the runbook, `retention` with its cap, and the snapshot sizes that differ during a roll), `docs/failure-modes.md`, `docs/formats.md`, `deploy/cluster/README.md`, a note at D11 of the one-way spec | C6 | S |
| infra | kardamom-infra: the sealer node resources of 10.2 | none | S |

Sizes: S is up to about 200 lines, M up to about 600, L above that. C0, C1 and C5 can start at
once. C0 must deploy to every reader before step 12.2. The release of step 12.1 holds C0 to C6.

## 14. Considered and not chosen

- **The retained file at the activation (revision 1).** It kept the snapshot small at high
  rates, but it needed a one-way latch, a store of two tiers, a ready state, the token, the
  peer file copy, and a new snapshot layout with two sections. The heap design needs none of
  them. Section 15 keeps the file for later.
- **A hold of the offers on each new session until kind 17.** It needs a deadline and a queue.
  The kept version needs neither.
- **A silent drop of a type-0 reference.** With `--ack validated` it loses a transaction that the
  client was told is accepted.
- **Turn on the window with a condition on the posted head.** The gate check of section 9 is
  simpler: the operator sends feature 5 only after the readers read the egress.
- **Keep `carryBytes` must-match and stop all members.** The user ruled it out.

## 15. The retained file: a later spec, with a trigger

The file keeps only an index on the heap and in the snapshot. It is worth its cost only when the
heap window costs too much. Measure these values on staging after step 12.11, and under each load
test:

- `kardamom_step_seconds{step="sealer.snapshot"}`, p99 and max: the pause of every member at the
  snapshot.
- `kardamom_sealer_snapshot_bytes` and `kardamom_sealer_retained_bytes`.
- The GC pauses of the sealer JVM, p99 and max, from its pause log (`jvm_pause_log_opts`,
  `deploy/cluster/nomad/cluster.nomad.hcl:454`), and its heap after a GC, against
  `cluster_heap_mb` (384 MB by default, `cluster.nomad.hcl:138-141`).
- The window in time: `retention` divided by the transaction rate. It must cover the delay from
  the seal to the store of the egress with a wide margin (one-way spec 6.4).

The trigger for the file spec (accepted in review). Any one of these starts it:

1. The snapshot step p99 is above 50 ms.
2. The retained bytes are above a quarter of the heap.
3. The `retention` of the rule of 4.4 is above the cap of 131,072 frames (4.4): a planned rate
   above about 2,180 tx/s. It fires before the 10,000 tx/s goal.
4. The GC pause p99 of the sealer is above 20 ms.

## 16. Open questions for the user

None.

The answers to the questions of revision 3, from the review:

- `retention` stays a setting that does not have to match. A rolling change may only raise it,
  and it stays at or below the cap (4.4).
- The drop text: `transaction dropped by the sealer, send it again`, geth code -32000, with a
  provisional status (4.3).

The answers to the questions of revision 2, from the review:

- The sequencer floor: accepted, with the decoder of 7.4.
- The trigger for the file spec: accepted, with the GC pause p99 as a fourth signal.
- The gate refuses feature 3 or 4 off while feature 5 is held or on: accepted. The gate also
  refuses feature 5 while a member reports a file store.
- `retention`: it covers at least 60 s at the planned rate (4.4). The operator raises it before a
  load test.

The answers to the questions of revision 1, from the review:

- Strip the bytes from the sessions later: yes, in a separate change.
- The staged roll of the executor groups (option (a) of #781): yes.
- The egress stays on the executor nodes: yes.
- A fleet rebuild from a seed: accepted. #667 stays open for the seed with the rules, and the
  runbook of section 8 covers the rebuild.
- The activation time on staging: about 30 minutes ahead, at low load.
- `carryBytes`: moot. This design does not use it.

## 17. Changes from revision 1

| Finding of the review | Change |
|---|---|
| Decision: the heap feed, not the file | Sections 3 to 6 and 10 are new. The latch, B, `TieredFrames`, `carryStorage=ready`, the token, the file endpoint, the file copy, the file section of v18 and `openFileTier` are gone. Section 15 keeps the file for later. |
| 1. The window before the readers | Section 7.3 gives the order and why it is not circular. Section 9 checks the readers before feature 5. |
| 3. The sequencer hold | Section 5: the kept version with a floor, no hold. Section 4.3: kind 18, not a silent drop. |
| 5. Two `Unjoinable` marks | Section 6: the verdict and the error. Section 11.2: the test. |
| 6. The PR split and the snapshot number | Section 13: C2 defines v18 in full. Section 11.1: the golden v18 case. |
| 7. The `FeedConfig` citation | `FeedConfig.java:38-46`. |
| 8. Where `windowOnly` is computed | Section 4.4: `setRules`, its call points and their order. |
| 9. The log position of the switch | Section 4.2, with the forced boundary. Section 11.1: the test. |
| 10. The drain wording | "falls within `retention` frames" (4.4, 11.1, 12.8). |
| 11. The failure modes of the heap design | Section 8: new rows. |
| 12. Prometheus gauges | Section 4.7, and the readers and the egress in 7.1 and 7.2. |
| 14. Tests that catch a fork | Section 11.1: byte equality at every cut, the forced boundary, the restore at each point, off and on again. Section 11.5: the feature-3/4 refusal. |
| 15. Long sentences | Section 1.3, section 4.4 and section 8 now use short sentences. |
| Questions | Section 16. |

## 18. Changes from revisions 2 and 3, and the notes of the approval

| Finding of the review of revision 2 | Change |
|---|---|
| 1 (blocker). Kind 18 has no republish path in the v2 loop | Sections 4.3 and 5: kind 18 carries the hash and the version. The v2 sequencer raises its version and publishes a `TxDrop` with the new reason. It re-admits nothing. The text names the only sources of a type 0 after the switch. No citation of the v1 loop is left. Section 1.3, F7. |
| 2 (major). R is not durable | Section 7.2: the `ref-mark` side file, its write order and its read at `open`. Section 7.3: R is durable. Section 11.2 and 11.3: a restart of the egress. Section 1.3, F8. |
| 3 (major). A type 7 below version 5 halts the readers | Section 7.4: the decoder. Section 4.3: the sealer counter. Section 13: C0 ships first. Section 12.1 and 12.2: the floor follows the decoder. Section 1.3, F6. |
| 4 (major). A file member loses a feature-5 schedule | Section 4.1: `UNSUPPORTED`. Section 4.6: the restore table with versions 12 and 17. Section 4.7: `retainedStore`. Section 9: the gate refuses feature 5 on a file chain. |
| 5. No epoch in a kind-3 batch | Section 4.2 and section 11.1: only kind 4 forces a boundary, and a batch holds no boundary. |
| 6. The walk gap | Section 11.1: "ends with the frames, or gaps, and the next request serves". |
| 7. Where R and the cursors come from | Section 9: R from each egress instance, the cursors from the readers. |
| 9. The egress kinds bump | Section 13: C1. |
| 10. STE | Section 1.3 F1, section 4.2 and section 5 use short sentences. |
| The questions | Section 16. `retention` gets its rule and the value for staging in 4.4. |
| Review of revision 3, 1 (major). A full-window replay passes the backlog limit | Section 4.4: the cap of 131,072 frames, the 10,000 tx/s goal needs the file spec or a replay change, trigger 3 fires at about 2,180 tx/s. Section 12: the last line. Section 15: trigger 3. |
| Review of revision 3, 2. The drop publisher | Section 5: a clone of `TxDropsPublisherHandle`, which is `Clone`. |
| Review of revision 3, 3. The provisional status | Section 4.3: the window, the nonce guard, the drop text, the code and the client text. |
| Review of revision 3, 4. The mark per batch | Section 7.2: once per `append` batch, before the segment sync, and the frames of `Input::Caught`. |
| Review of revision 3, 5. The check before the readers move | Section 12, step 4: the posted cursor and the validator block against the reference block first. |
| Review of revision 3, 6. The hazard of a bad type 7 | Section 7.4. |
| Review of revision 3, 7. STE | Section 4.3, "The sequencer side", is a list of short sentences. |
| Review of revision 3, Q1 and Q2 | Section 4.4: raise only, the cap, the README note. Section 13: C6 and C9. Section 16: none open. |
| Approval notes, 1. The cap in bytes | Section 4.4: `retention x mean frame bytes <= 32 MiB`, from the gauges, checked by C6. 131,072 frames stay the ceiling, for about 300 bytes a frame. |
| Approval notes, 2. The mark at a reset | Section 7.2: `reset` keeps the higher mark. Section 11.2 and C5: the test. |
| Approval notes, 3. STE | Section 4.4: the sentence is split at "The 10,000 tx/s goal". |

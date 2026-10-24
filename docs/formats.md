# Stored and wire formats

A deploy rolls one service at a time, and a failed deploy goes back to the
previous release. Thus two releases run side by side and read what the
other one wrote. This page gives the rules that keep that safe. The
guarantee covers one step: release N and release N-1.

## The registry

`formats.toml` at the repository root lists every stored or wire format
that crosses a release boundary. It is at the root because tests, CI and
the deploy tooling read it, as they read `Cargo.toml` and `clippy.toml`.

Each `[format.<id>]` entry has these keys:

| Key | Meaning |
|---|---|
| `writes` | The version that the release writes with its default configuration. |
| `reads_min` | The oldest version that the release reads. |
| `reads_max` | The newest version that the release reads. |
| `shared` | `true` when two releases of a mixed fleet read each other's output: wire messages, files that pass between services, and snapshots that a member can get from another member. `false` for a file that one process writes and reads, such as the state database, the spool, and the cursors. |
| `permanent` | Optional, default `false`. `true` when the data stays for all time, as a KAR1 batch on L1 does. |
| `code` | One or more `path#symbol` strings: a file that defines the format, and a name in that file. |
| `activation` | Optional. `{ flag = "...", writes = N }`: the version that the writer writes when an operator switches the flag on. `{ feature = N, writes = M }`: the version that the writer writes after the activation of feature `N` is in the log. A list of both forms is also valid. |
| `layout` | Optional table `[format.<id>.layout]`: a fingerprint of each part of a frozen layout. |

For a set of message kinds, the version is the highest kind number. A
format with no version constant has a frozen layout and version 1. A TOML
comment above each entry tells what the format is and how an old reader
fails.

A `path#symbol` location is a pointer for the reader of the registry, not a
guarantee. The test finds the symbol as a whole word in the file, and
nothing more. The tests in the next section tie the registry to the code.

## The tests that tie the registry to the code

The registry parser refuses an entry unless `reads_min <= writes <= reads_max`.
The activation version must also be inside the read range.

The version tests:

| Crate | Formats | What the test checks |
|---|---|---|
| `kardamom-state` | `state-db`, `checkpoint-manifest`, `block-log` | the constants |
| `kardamom-batcher` | `kar1-batch` | `decode` accepts exactly `reads_min..=reads_max`, and `VERSION` is `writes` |
| `kardamom-cluster-adapter` | sealer ingress kinds, egress kinds, record types | the egress decoder and the record type decoder accept exactly the read range; the Rust ingress kinds stay at or below `reads_max` |
| `kardamom-cluster-client` | `cluster-app-version` | the major of the app version |
| `kardamom-reconstruct` | `sealer-seed` | the constant |
| `kardamom-cluster-adapter` | `sealer-feed-control` | the golden frame `feed_request` from the Rust encoder |
| `kardamom-log` | `discovery-record` | the constant |
| sealer `FormatRegistryTest` | `sealer-snapshot` | the loader accepts exactly `reads_min..=reads_max`, and the write constant is `writes` |
| sealer `FormatRegistryTest` | ingress kinds, egress kinds, record types, seed, app version | the constants |
| sealer `FormatRegistryTest`, `WireGoldenTest` | `sealer-feed-control` | version 1, the 25-byte length, and the golden frame `feed_request` from the Java writer and reader |

The layout tests of the frozen formats:

| Format | Crate | Fingerprint |
|---|---|---|
| `batcher-spool` | `kardamom-batcher` | rkyv archived size and alignment of `ClosedBlock`, `RecordedTx`, `RemoteEpochRecord` |
| `batcher-cursor` | `kardamom-batcher` | the JSON of a fixed sample |
| `l1-cursor` | `kardamom-da-watcher` | the line of a fixed sample, and its parse |
| `aeron-stream-records` | `kardamom-types` | rkyv archived size and alignment of the seven record types |
| `receipt-rollback` | `kardamom-types` | rkyv archived size and alignment of `ReceiptRollback` |
| `block-log` | `kardamom-state` | rkyv archived size and alignment of `WriteBatch`, and the bytes of a fixed segment head |
| `executor-markers` | `kardamom-executor` | the text of a fixed rollback marker, and its parse |
| `receipt-ledger` | `kardamom-executor` | the slot count and the bytes of a fixed slot, and its decode |
| `egress-ref-mark` | `kardamom-egress` | the bytes of a fixed mark, and its read |
| `notifier-outbox` | `kardamom-notifier` | rkyv archived size and alignment of `OutboxRecord` |
| `redis-cache` | `kardamom-cache` | the keys of fixed samples |
| `canary-journal`, `canary-store`, `canary-market` | `kardamom-canary` | the JSON of fixed samples |

What the layout tests do not cover:

- An rkyv size fingerprint covers only the fixed part of the archive. It
  does not cover the data behind a vector or a string, two fields of one
  size that change places, or enum variants that change order.
- The record types inside the seven stream records, such as the log
  entries of a receipt, have no fingerprint of their own.
- The values in the Redis cache are rkyv receipts. The `Receipt`
  fingerprint covers their fixed part.

## The pull request check

`just check-formats BASE` compares `formats.toml` with the file at the
base revision. CI runs it on each pull request, with the base branch as
the base. For each format that both files list, it applies these rules:

| Rule | Holds when | When it does not hold, add |
|---|---|---|
| Rollback | `base reads_min <= writes <= base reads_max` | `[one_way.<id>]` with `version` = the head `writes` |
| Mixed fleet, for a shared format | `base reads_min <= writes <= base reads_max` | also `[coordinated.<id>]` with `version` = the head `writes` |
| Rolling deploy | `reads_min <= base writes <= reads_max` | `[coordinated.<id>]` with `version` = the incompatible head read boundary (`reads_min` or `reads_max`) |
| Retired | the head lists every format of the base | `[retired.<id>]` with `version` = the base `writes` |

Each waiver also has a `reason`. The check also fails in these cases, and
no waiver covers them:

- A new waiver covers no finding.
- A permanent format narrows either end of its read range, drops its permanent designation, or is retired.
- A `layout` fingerprint changes, and `writes` stays the same. Give the
  format a new version, or a new id.

These rules apply too:

- Only a waiver that is new against the base counts. A waiver that the
  base already has does not cover a finding of this change.
- A waiver covers one version only.
- A format that the base does not list is new. It passes.
- When the base has no `formats.toml`, only the head file is checked.
- When `activation.writes` falls outside the base read range, the check prints a note:
  switch the flag on, or send the activation of the feature, only after no node runs the base.

## The pull request check is incremental

The pull request check compares a change with its base branch, not with the
release that runs. Two changes can each pass and still break N-1 together.
For example:

1. Change A raises `reads_max` to 11. It passes.
2. Change B raises `writes` to 11. It passes, because A is in its base.
3. One deploy ships A and B together. The release that runs reads only up
   to 10, so a rollback is not safe.

The release gate is the deploy preflight. It compares the `formats.toml` of
the deployed revision with the `formats.toml` of the target. It uses the
same library call as the pull request check:

```rust
let base = Registry::read(deployed_path)?;
let head = Registry::read(target_path)?;
let report = Comparison::new(&base, &head).report();
```

The deploy preflight is the release gate of the workloads role
(`deploy/cluster/ansible/roles/workloads/tasks/gate.yml`). It reads the
registry of the accepted release from the deploy record, runs
`kardamom-deploy formats --base <that registry> --root <target tree>` for
the findings, and decides by the findings alone: a waiver in `formats.toml`
does not cover a finding at deploy time, because the waiver was given against a pull
request base, not against the release that runs. The gate rules are in
"The release gate" of `deploy/cluster/README.md`. Every release ships
`kardamom-deploy`, signed, so the gate runs the binary of the release that
it deploys.

Each finding carries the `shared` and `permanent` flags of the format.
The gate treats a `rollback` finding as one-way, and a `rolling`,
`mixed_fleet` or `retired` finding as coordinated. One exception applies
to a retired format:

- The operator names it in `KARDAMOM_ALLOW_RETIRED`, and the format is
  neither shared nor permanent. The rolling deploy then carries the
  change, with no rollback floor. The deploy record keeps the list.
- Give the allowance only when the release removes the service that
  owned the format, as the removal of the notifier did for
  `notifier-outbox`. A rollback then starts the old service on its own
  file, which no release changed in the meantime.
- Do not give it for a format that a running service renames, for
  example a new layout of a frozen format under a new id (rule 6). The
  new release stops updating the old file, and a rollback starts the old
  service on stale data. Take the coordinated path, or keep the new
  release writing the old file through one release.
- The gate refuses an allowance that covers no such finding.

The pull request check still needs the `[retired.<id>]` waiver.

## Rules for a format change

1. Add a field only at the tail. Make the reader accept a record that is longer than it knows.
2. Add an enum variant only at the end. Make the reader skip a variant that it does not know.
3. Ship the reader one release before the writer:
   - Release N reads version V+1 and still writes V. Set `reads_max = V+1`.
   - Release N+1 writes V+1. Set `writes = V+1`.
   - Then N+1 can roll back to N, because N reads V+1.
   - Deploy N before N+1. The deploy preflight checks this.
4. Put a writer that you cannot ship in two steps behind an activation flag.
   Switch the flag on only after every reader runs the new release.
5. A change to a version constant also changes `formats.toml`. The tests fail until the two agree.
6. A frozen layout does not change. A new shape goes in a new file, a new key prefix, or a new stream.
7. Keep a format in the registry while a supported release reads or writes it.
8. Never narrow the read range of a permanent format or retire its reader. Every old version stays readable.
9. Give a new key of `formats.toml` itself a default. The check must still
   parse the file of an older base.

## One-way and coordinated releases

- **One-way.** The head writes a version that the base cannot read. After
  the first write, a rollback is not safe.
  - The deploy refuses a one-way release unless `KARDAMOM_ALLOW_ONE_WAY`
    names its format ids.
  - The deploy then records a rollback floor in the deploy record, and
    `just rollback` does not cross it.
  - The automatic revert that the floor must turn off is not in the
    repository yet.
- **Coordinated.** A mixed fleet cannot run. Either the head cannot read
  what the base writes, or the format is shared and the base cannot read
  what the head writes. Do not roll the release member by member. Stop the
  writers, or follow the runbook for the format, and then start the new
  release.
- A state database schema change is both. There is no migration, so the
  new release cannot read an old database.

## Reader hardening

A rollback is safe only when the old release reads what the new release
wrote, and a rolling deploy is safe only when a mixed fleet decides alike.
Each reader below holds one of these rules. A test of the owning crate
proves each one.

| Reader | Rule | What it makes safe |
|---|---|---|
| Sealer ingress dispatch | Every member drops a frame of a kind it does not know, counts it (`cluster DROPPED unknown-kind`), and never orders it. Kind 0 carries one user record, and kind 3 carries a batch of them; every other known kind is a control message. Kind 11, the feature activation, is a control message: a record of feature 3 stores the schedule of decision version 4, a record of feature 4 stores the schedule of decision version 5 while version 4 or 5 runs, a record of feature 5 stores the schedule of the carry window while version 5 runs on a member without the retained file, and a record of another feature changes no state. No client sends kind 10 or kind 11. The release before this one orders an unknown kind of 85 bytes or more as a user record, so during the first roll to this release no client may send an unknown kind. | A mixed fleet does not fork on a new kind: a member of the old release drops the frame, and a member of the new release sends a state-changing kind only after every member reads it. |
| Sealer snapshot with a retained file | A member with `kardamom.cluster.carryBytes=true` writes version 17: the state of version 16, then the file section. It restores version 17 and version 12, and stops on any other version. A member without the flag stops on both. `SealerVersionFiveTest` restarts a carrying member from version 17 at each entry, and the member decides as its peer. | A restored member runs the decision version, the switch and the schedule of feature 4 of its peers. A member without the flag never writes version 17, so a rolling deploy of a chain without the flag is safe. |
| Sealer snapshot with the carry window | A heap member that holds an activation of feature 5 writes version 18: the state of version 16, then the carry window section, then the heap section. Before that record, it writes 14, 15 or 16. A member with the retained file answers feature 5 with `UNSUPPORTED` and stops on version 18. `FormatRegistryTest` checks golden version-18 bytes, and `SealerCarryActivationTest` restores a member at each entry of the switch. | A release changes no snapshot byte until the upgrade transaction of feature 5. A release that reads only up to version 17 stops on version 18, so the preflight refuses a rollback after it. |
| Sealer `/status` | The admin endpoint reports `snapshotWrites`, `snapshotReadsMin`, `snapshotReadsMax`, and `snapshotLatest`, the version of the newest snapshot the member restored or took. | The release gate of a deploy and `just rollback` refuse a sealer release whose `reads_max` is below `snapshotLatest` of any member. That member could not restore its own snapshot after the restart. The check reads `formats.toml` at the revision of the sealer release. The gate checks only a release that changes the sealer job. The check skips a sealer job that Nomad does not know, and refuses while a member does not run. |
| Rust egress reader | A frame of a kind the reader does not know drops with a warning. Kind 17 (the sealer's rules): an older reader drops it, and only the sequencer reads it. | A sealer can send a new egress kind to the clients of the release before. |
| Batcher spool | The spool of a layout version lives in `spool/v<N>`. A release opens only its own directory. Every other entry of the root, and a spool whose block does not decode, is dropped with a warning and `kardamom_batcher_spool_dropped_total{reason}`. The range is read again from the sealer. | A spool of either release never stops the batcher. A rollback and a roll forward both start, and the sealer, which keeps every frame above the posted head, serves the dropped range. |
| Checkpoint manifest and peer fetch | The manifest states `schema_version`, and the peer header `x-checkpoint-schema` carries it. An adopter skips an image of a schema it does not read, before the copy, with a log line and `kardamom_checkpoint_unreadable_schema_skips_total`. An image without a stated schema passes, and says its schema when it opens. | After a schema bump, an executor or a validator of the old release does not adopt a checkpoint of the new release, from its own disk or from a peer. It takes the next checkpoint that it reads. |
| da-watcher L1 cursor | The reader takes the block number and the hash, and ignores any field after the hash. A line with fewer fields, or a field that does not parse, is still the `l1_cursor_unreadable` halt. | A later release can add a field at the tail, and this release still reads the file after a rollback. |
| Sealer feed control | The member reads the first 25 bytes of a replay request on stream 1007 and ignores a longer tail. It drops a shorter message or another kind, with `sealer FEED ... DROPPED`. The executor writes the request and resends it after 2 s with no answer. | A later release can add a field at the tail of the request. An older member answers it from the first 25 bytes. |
| Sealer egress reader | `EgressItem::decode` reads the fields of a kind that it knows and ignores the bytes after them. On the member feed, the answers (kinds 3, 4, 11 and 16) carry the request id as an 8-byte tail after the 17 bytes of the egress layout. The golden frame `feed_replay_done` is a feed answer that the egress reader reads as kind 4. | The member feed and the cluster egress share one codec. A field added at the tail of a frame does not break a reader of the release before. |
| `tx_receipts` receipt stream | A frame of 40 bytes is a `ReceiptRollback`. Every other frame is a `ReceiptBatch`: 16 bytes with no receipt, which the publisher never sends, and longer than 256 bytes with one. Two compile-time checks keep the lengths apart. A reader of the release before fails to decode the rollback frame as a batch, logs it, and skips it. | A rolling deploy can publish the rollback record before every consumer reads it. A consumer of the release before keeps the receipts above the rollback, as before. |
| Executor markers and receipt ledger | An executor with `--carry-bytes` writes `marker-rollback`, `marker-running` and `marker-receipts` in its state directory. The release before ignores them. A release that finds no running marker trusts the ledger, and a release that finds no ledger compares nothing above the committed block. | A rollback and a roll forward both start. A rollback of the release leaves a pending rollback marker unpublished: the release before restores any checkpoint and does not publish the record. The roll forward then publishes it. The consumers drop the receipts above it: the ingress reads them again from an executor on a cache miss, and the validator leaves them unverified. |
| Batcher cursor | serde JSON. The reader ignores a field it does not know. A new field gets `#[serde(default)]`. | The cursor of either release reads in the other. |

## Not in the registry

These settings have the same risk, but they are not formats. The release
gate refuses to roll them:

- The sealer settings that every member must match. See `cluster/sealer-service/README.md`.
- The rules that decide the sealer's replicated state. A new rule ships
  behind a feature flag, and the flag switches every member at one record
  in the log of the sealer. See "Decision version" in
  `cluster/sealer-service/README.md`.
- The shard map. Use the resize playbook.
- The L1 contracts. Their upgrades go forward only.

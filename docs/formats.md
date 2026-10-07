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
| `activation` | Optional. `{ flag = "...", writes = N }`: the version that the writer writes when an operator switches the flag on. |
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
| `kardamom-state` | `state-db`, `checkpoint-manifest` | the constants |
| `kardamom-batcher` | `kar1-batch` | `decode` accepts exactly `reads_min..=reads_max`, and `VERSION` is `writes` |
| `kardamom-cluster-adapter` | sealer ingress kinds, egress kinds, record types | the egress decoder and the record type decoder accept exactly the read range; the Rust ingress kinds stay at or below `reads_max` |
| `kardamom-cluster-client` | `cluster-app-version` | the major of the app version |
| `kardamom-reconstruct` | `sealer-seed` | the constant |
| `kardamom-log` | `discovery-record` | the constant |
| sealer `FormatRegistryTest` | `sealer-snapshot` | the loader accepts exactly `reads_min..=reads_max`, and the write constant is `writes` |
| sealer `FormatRegistryTest` | ingress kinds, egress kinds, record types, seed, app version | the constants |

The layout tests of the frozen formats:

| Format | Crate | Fingerprint |
|---|---|---|
| `batcher-spool` | `kardamom-batcher` | rkyv archived size and alignment of `ClosedBlock`, `RecordedTx`, `RemoteEpochRecord` |
| `batcher-cursor` | `kardamom-batcher` | the JSON of a fixed sample |
| `l1-cursor` | `kardamom-da-watcher` | the line of a fixed sample, and its parse |
| `aeron-stream-records` | `kardamom-types` | rkyv archived size and alignment of the seven record types |
| `notifier-outbox` | `kardamom-notifier` | rkyv archived size and alignment of `OutboxRecord` |
| `redis-cache` | `kardamom-cache` | the keys of fixed samples |

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
| Rollback | `writes <= base reads_max` | `[one_way.<id>]` with `version` = the head `writes` |
| Mixed fleet, for a shared format | `writes <= base reads_max` | also `[coordinated.<id>]` with `version` = the head `writes` |
| Rolling deploy | `reads_min <= base writes` | `[coordinated.<id>]` with `version` = the head `reads_min` |
| Retired | the head lists every format of the base | `[retired.<id>]` with `version` = the base `writes` |

Each waiver also has a `reason`. The check also fails in these cases, and
no waiver covers them:

- A new waiver covers no finding.
- A permanent format raises `reads_min`.
- A `layout` fingerprint changes, and `writes` stays the same. Give the
  format a new version, or a new id.

These rules apply too:

- Only a waiver that is new against the base counts. A waiver that the
  base already has does not cover a finding of this change.
- A waiver covers one version only.
- A format that the base does not list is new. It passes.
- When the base has no `formats.toml`, only the head file is checked.
- When `activation.writes > base reads_max`, the check prints a note:
  switch the flag on only after no node runs the base.

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

The deploy preflight is not in the repository yet. Until it is, the
operator runs the same check by hand before a deploy:
`just check-formats <deployed revision>`. When the deployed release is more
than one change behind, the waivers of the earlier changes are in the base
of that comparison too, and they do not count. The operator then confirms
each finding by hand.

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
8. Never raise `reads_min` of a permanent format. Every old version stays readable.
9. Give a new key of `formats.toml` itself a default. The check must still
   parse the file of an older base.

## One-way and coordinated releases

- **One-way.** The head writes a version that the base cannot read. After
  the first write, a rollback is not safe.
  - The deploy must refuse a one-way release unless `KARDAMOM_ALLOW_ONE_WAY`
    names its format ids.
  - The deploy must then record a rollback floor and turn off the automatic
    revert for the jobs that write the format.
  - The deploy preflight that does this is not in the repository yet.
    Until it is, the operator does these steps.
- **Coordinated.** A mixed fleet cannot run. Either the head cannot read
  what the base writes, or the format is shared and the base cannot read
  what the head writes. Do not roll the release member by member. Stop the
  writers, or follow the runbook for the format, and then start the new
  release.
- A state database schema change is both. There is no migration, so the
  new release cannot read an old database.

## Not in the registry

These settings have the same risk, but they are not formats. The deploy
preflight must refuse to roll them:

- The sealer settings that every member must match. See `cluster/sealer-service/README.md`.
- The shard map. Use the resize playbook.
- The L1 contracts. Their upgrades go forward only.

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
| `code` | One or more `path#symbol` strings: a file that defines the format, and a name in that file. |
| `activation` | Optional. `{ flag = "...", writes = N }`: the version that the writer writes when an operator switches the flag on. |

For a set of message kinds, the version is the highest kind number. A
format with no version constant has a frozen layout and version 1. A TOML
comment above each entry tells what the format is and how an old reader
fails.

These checks keep the file true:

- The registry parser refuses an entry unless `reads_min <= writes <= reads_max`.
- The activation version must be inside the read range.
- A test in each crate that owns a version constant compares the constant with the file:
  - `kardamom-state`: `state-db`, `checkpoint-manifest`.
  - `kardamom-batcher`: `kar1-batch`.
  - `kardamom-cluster-adapter`: the sealer kinds and record types.
  - `kardamom-cluster-client`: `cluster-app-version`.
  - `kardamom-reconstruct`: `sealer-seed`.
  - `kardamom-log`: `discovery-record`.
- `FormatRegistryTest` in the sealer compares the snapshot version, the
  ingress and egress kinds, the seed version and the app version.
- A test in `kardamom-formats` checks that each `code` file holds its symbol.

## The release check

`just check-formats BASE` compares `formats.toml` with the file at the
base revision. CI runs it on each pull request, with the base branch as
the base. For each format that both files list, it applies two rules:

| Rule | Holds when | When it does not hold |
|---|---|---|
| Rollback | `writes <= base reads_max` | The base cannot read what the head writes. Add `[one_way.<id>]` with `version` = the head `writes`, and a reason. |
| Rolling deploy | `reads_min <= base writes` | The head cannot read what the base writes. Add `[coordinated.<id>]` with `version` = the head `reads_min`, and a reason. |

- A format that the base does not list is new. It passes.
- When the base has no `formats.toml`, only the head file is checked.
- An entry covers one version only. A later change to the same format needs a new entry.

## Rules for a format change

1. Add a field only at the tail. Make the reader accept a record that is longer than it knows.
2. Add an enum variant only at the end. Make the reader skip a variant that it does not know.
3. Ship the reader one release before the writer:
   - Release N reads version V+1 and still writes V. Set `reads_max = V+1`.
   - Release N+1 writes V+1. Set `writes = V+1`.
   - Then N+1 can roll back to N, because N reads V+1.
4. Put a writer that you cannot ship in two steps behind an activation flag.
   Switch the flag on only after every reader runs the new release.
5. A change to a version constant also changes `formats.toml`. The tests fail until the two agree.
6. A frozen layout does not change. A new shape goes in a new file, a new key prefix, or a new stream.
7. Keep a format in the registry while a supported release reads or writes it.

## One-way and coordinated releases

- **One-way.** The head writes a version that the base cannot read. After
  the first write, a rollback is not safe. During a rolling deploy, an old
  reader also cannot read the new output. Thus a one-way release is also a
  coordinated release for the readers of that format.
  - The deploy must refuse a one-way release unless `KARDAMOM_ALLOW_ONE_WAY`
    names its format ids.
  - The deploy must then record a rollback floor and turn off the automatic
    revert for the jobs that write the format.
  - The deploy preflight that does this is not in the repository yet.
    Until it is, the operator does these steps.
- **Coordinated.** The head cannot read what the base writes. Do not roll
  it member by member. Stop the writers, or follow the runbook for the
  format, and then start the new release.
- A state database schema change is both. There is no migration, so the
  new release cannot read an old database.

## Not in the registry

These settings have the same risk, but they are not formats. The deploy
refuses to roll them:

- The sealer settings that every member must match. See `cluster/sealer-service/README.md`.
- The shard map. Use the resize playbook.
- The L1 contracts. Their upgrades go forward only.

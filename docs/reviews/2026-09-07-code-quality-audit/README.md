# Code quality audit — 2026-09-07

Audit of the working copy `b3b30178bc4b` (parent `6bc25d99d536`, "fix(cluster): move
log replication off the catch-up port (#257)") against eleven code quality rules. No
code was changed. This document is the summary. The per-crate appendices hold every
finding with a `file:line` reference.

## Files in this directory

| file | content |
|---|---|
| `README.md` | this summary |
| `crate-<group>.md` | one appendix per reviewer group, all findings in tables |
| `mechanical-lists.md` | every long function, large file, unreachable `pub` item, and hidden `too_many_arguments` site |
| `clippy-pedantic-sites.md` | every clippy pedantic warning (2,526 rows) |

## Rules

| id | rule |
|---|---|
| R1 | Comments are self-contained. No historical or legacy context. No comment on self-explanatory code. |
| R2 | A function over 100 code lines is split. One between 51 and 100 is split case by case. |
| R3 | A file has at most 500 code lines. |
| R4 | No manual `drop`. Use a helper function whose scope ends the value. |
| R5 | No mutex where a channel works. No channel or mutex where ownership works. |
| R6 | No dynamic dispatch. Use generics. |
| R7 | No pile of generics on one item. Group bounds into a supertrait with associated types. |
| R8 | No `pub` field or item that nothing outside needs. |
| R9 | No defensive input checks in the body. Parse once into a typed value at the boundary. |
| R10 | Prefer functional style over imperative loops and mutable accumulators. |
| R11 | `clippy::pedantic` passes. Group method arguments into structs where needed. |

## Scope and method

- **In scope, full pass:** the 21 Rust crates under `crates/` and the two guest crates
  under `guest/`. About 77,000 code lines in 457 files.
- **In scope, light pass:** the Java sealer under `cluster/sealer-service/`, for the
  three language-neutral rules only (R1, R2, R3).
- **Out of scope:** shell and Python scripts, Solidity, the vendored
  `uniswap-v2-contracts/` and `bench-contracts/`.
- **Mechanical rules** (R2, R3, R8 in part, R11) come from tooling. R2 uses clippy
  `too_many_lines` at threshold 50. R3 uses a code-line count that skips blank and
  comment-only lines. R8 uses the `unreachable_pub` rustc lint. R11 uses
  `cargo clippy --workspace --all-targets --all-features -- -W clippy::pedantic`, plus a
  `--force-warn` pass for lints that `allow` attributes hide. The `e2e` crate was checked
  with its `full-pipeline-e2e` feature on, because default features compile almost none
  of it.
- **Judgment rules** come from eleven parallel reviewers on Claude Opus, one per crate
  group. Each reviewer read every non-test file in its group and wrote one appendix.
  All eleven appendices were then citation-checked by script and spot-read. The
  `batcher` appendix had four line references corrected (three test fakes in
  `da_watcher` and one constant in `deployer/src/ids.rs`).
- Test code (`tests/`, `benches/`, `*_tests.rs`, inline `#[cfg(test)]`) is reported in
  separate sections and counted separately.

## Headline numbers

| measure | count |
|---|---|
| production functions over 50 code lines | 117 (26 of them over 100) |
| test functions over 50 code lines | 115 |
| files over 500 code lines | 13 (40 by raw line count) |
| `unreachable_pub` sites | 38 (29 in binaries and libraries, 9 in test helpers) |
| clippy pedantic warnings | 2,526 across 57 lints (2,131 in production files) |
| functions with more than 7 arguments, hidden by `allow` | 26 (largest: 16, 15, 15, 12, 12, 12) |
| `allow(clippy::...)` attributes | 50, of which 32 are `too_many_arguments`; plus 5 `allow(dead_code)` |
| reviewer findings, production code | about 1,380 table rows |

Reviewer rows per group and rule. R2 rows are the reviewers' helper proposals; the
canonical R2 list is in `mechanical-lists.md`.

| group | R1 | R2 | R3 | R4 | R5 | R6 | R7 | R8 | R9 | R10 |
|---|---|---|---|---|---|---|---|---|---|---|
| batcher, da_watcher, deployer | 26 | 13 | 5 | 6 | 20 | 1 | 8 | 25 | 7 | 15 |
| bench, executor | 50 | 23 | 5 | 5 | 22 | 2 | 1 | 33 | 10 | 20 |
| e2e | 17 | 6 | 2 | 0 | 3 | 0 | 1 | 16 | 5 | 7 |
| engine | 26 | 10 | 5 | 7 | 12 | 18 | 3 | 9 | 9 | 12 |
| exec-core, footprint, reconstruct, guest | 36 | 17 | 4 | 0 | 2 | 0 | 0 | 6 | 6 | 15 |
| ingress, interop-feed | 34 | 3 | 0 | 3 | 40 | 1 | 5 | 14 | 7 | 4 |
| log, obs | 47 | 10 | 6 | 0 | 20 | 12 | 9 | 21 | 10 | 9 |
| sequencer, cluster-adapter, cluster-client | 38 | 11 | 3 | 2 | 25 | 1 | 2 | 20 | 12 | 9 |
| state, types | 32 | 13 | 5 | 4 | 6 | 3 | 0 | 25 | 12 | 11 |
| stm | 36 | 13 | 5 | 15 | 84 | 1 | 3 | 21 | 9 | 12 |
| validator | 36 | 13 | 5 | 6 | 17 | 6 | 1 | 33 | 12 | 13 |

R3 and R5 rows include KEEP and JUSTIFIED verdicts. The R5 rows that need a change
number 32; the other 219 are justified cross-thread seams.

## Hot spots

Four files carry a large share of the debt across every rule:

| file | code lines | why it is a hot spot |
|---|---|---|
| `crates/stm/src/execute.rs` | 3,153 | 5 functions over 100 lines, 15 manual drops, about 90 sync sites, 136 pedantic warnings, 11 doc comments attached to the wrong item |
| `crates/bench/src/bin/stm-p2.rs` | 1,676 | `main` is 618 lines, `run_mdbx_ab` takes 16 arguments, 94 pedantic warnings |
| `crates/engine/src/reader.rs` | 1,032 | 489 source lines plus a 543-line inline test module, 51 pedantic warnings |
| `crates/exec-core/src/executor/scope.rs` | 809 | 7 functions with 8 to 10 arguments behind `allow` |

## Defects found on the way

These are not style findings. Each one was confirmed by reading the code during the
merge, not only reported by a reviewer. Fix them regardless of any refactor.

1. **`RemoteEpochRecord::last_seq` underflows on an empty batch.**
   `crates/types/src/xchain.rs:388` computes `first_seq + len - 1`; `Default` makes the
   empty case constructible.
2. **Broken format string.** `crates/state/src/checkpoint/mod.rs:277` contains "could not
   be" followed by 26 spaces and then "quarantined".
3. **`TxObs.args` is dead data on a hot path.** `crates/footprint/src/lib.rs:49` is filled
   at `lib.rs:81-89` and `crates/stm/src/schedule.rs:83`, up to six `U256` words per
   transaction, and read nowhere.
4. **Metrics that always report zero.** In `crates/stm/src/execute.rs`, `commit_fold_ns`
   (line 845) and `predict_ns` (line 896) are never written but are read into
   `commit_fold_us` and `predict_us` at lines 3904-3906. `parallel_span_ns`, `ramp_ns` and
   `commit_ns` (lines 882-887) are neither written nor read.
5. **A condition variable nobody waits on.** `BlockCtx::done_cv`
   (`crates/stm/src/execute.rs:1301`) is notified at five sites and waited on at none.
6. **`block_tail` ignores 3 of its 15 arguments.** `crates/stm/src/execute.rs:3454` reads
   `let _ = (keep_hot, tail_on_workers, pin_cores);`.
7. **A written-only atomic.** `settled` at `crates/bench/src/bin/stm-p2.rs:543` is stored
   at line 596 and never loaded.
8. **Four dead public modules in `kardamom-log`.** No type from `publisher.rs`,
   `subscriber.rs`, `supervisor.rs`, or `replay.rs` is used outside the crate. Other crates
   name them only in doc comments. That is about 1,150 raw lines.
9. **Dead items.** `e2e::pipeline` (its only caller is its own unit test);
   `Executor::skip` at `crates/exec-core/src/executor/scope.rs:711`;
   `AttestingWriterQueue` at `crates/validator/src/attester.rs:375`;
   `ClaimIndex.reads` at `crates/validator/src/parallel/claims.rs:56` (written at line 80,
   read nowhere); the `metrics::record_*` family in `sequencer`, superseded by
   `HotMetrics`; `execute_block_parallel_scoped` in `crates/validator/src/parallel/engine.rs`,
   a duplicate of the parallel path that only tests call.
10. **Unbounded channels on stall paths.** The resync channel pair at
    `crates/sequencer/src/resync/mod.rs:478-479` and the sequencer's live feed channels
    are unbounded, and so are the message channels in `kardamom-log`
    (`aeron_live/runtime.rs:319, 343, 360` and `aeron_live/handles/tx_receipts.rs:241, 286`).
    They grow without limit in the stall case the resync path exists for.
11. **Unbounded rate-limiter map.** `PerIpLimiter` at `crates/ingress/src/rate_limit.rs:32`
    adds one `DashMap` entry per distinct client IP and never removes one.
12. **Misattached CLI help.** `crates/bench/src/bin/load.rs:84` puts the `--retry-submit`
    help text on `--subscribe`.
13. **Stale build documentation.** `README.md`, the `justfile` (`check-aeron` target, line
    166), `crates/cluster-client/Cargo.toml:11` and `crates/cluster-adapter/Cargo.toml:10`
    all describe an `aeron-live` feature that no crate defines. `just check-aeron` fails
    today with "does not contain this feature".
14. **Comments that contradict the code.** `crates/state/src/meta.rs:13` says the schema
    version is "currently 1" while `SCHEMA_VERSION = 2`; `crates/state/src/schema.rs`
    says "Seven named tables" for 11; `crates/stm/src/lib.rs:12` says the crate "is not
    wired into the live executor yet" while `crates/executor/src/parallel.rs:54` imports
    it; `crates/executor/src/config.rs:12` links to a `crate::ExecutorConfig` that does not
    exist in that crate; `crates/log/src/subscriber.rs:4` says the module sits behind a
    feature that `lib.rs:20` says does not exist.

## R1 — comments with history

378 comment sites in production code carry history, and 74 more in test code. The
common shapes:

- **Issue and PR numbers.** `#81`, `#86`, `#156`, `#250`, `#252`, `PR-4`. The number
  is a pointer to a conversation, not a rule. Keep the rule, drop the number.
- **Phase and milestone labels.** `Phase 0`, `F02.1`, `F07.3`, `E1`, `E2`, `P2`,
  `M+1 v1`, `S12/S14`. Name the behavior, not the milestone.
- **Spec documents by name.** 22 sites link to `docs/agents/*-spec.md` or
  `docs/specs/*`. One of them (`docs/specs/interop-outbox-messaging-spec.md`) does not
  exist. State the contract in the comment.
- **Obituaries.** "used to", "previously", "no longer", "the old contract", "the deleted
  multiprocess e2e", "an earlier timer-driven design". Describe the present code only.
- **Incident stories.** "found by the speculative-release adversarial test", "the one-shot
  read failed three times in one CI day", "Profiling the driver JVM found ...". Keep the
  invariant, drop the story.
- **Merged doc pairs.** In `crates/stm/src/execute.rs`, eleven doc blocks that belonged
  to deleted items now sit above unrelated items. `#[allow(clippy::too_many_arguments)]`
  at line 4403 also attaches to the wrong function.
- **Broken doc links.** `ExecScope`, a free `execute_tx`, `write_set_from_evm_state`,
  `run_durable_watermark_loop`, `ReceiptCache*Handle`, `crate::reexec`, and
  `tests/full_pipeline_e2e.rs` are all named in links and none exists.

The Java sealer has two: `SealerEgress.java:251` ("which used to receive, and then drop")
and `SealerReplayTest.java:117` ("that design no longer exists").

The root `Cargo.toml` carries a `NOTE:` block about a rusteron static-link bug and a
`dashmap` comment that describes the executor's join buffer. Both are history in a
manifest.

## R2 — long functions

Clippy counts 117 production functions over 50 code lines and 115 test functions. The
full list is in `mechanical-lists.md`. The 26 over 100 lines are the mandatory splits.
The 91 between 51 and 100 are decided case by case; the reviewers' R2 tables in the
crate appendices name the helpers for the ones they read, and a function with one
linear body and no repeated shape can stay. The 26 over 100 lines:

| code lines | function | location |
|---|---|---|
| 618 | `main` | `crates/bench/src/bin/stm-p2.rs:1308` |
| 458 | `block_tail` | `crates/stm/src/execute.rs:3295` |
| 407 | `run_mdbx_ab` | `crates/bench/src/bin/stm-p2.rs:825` |
| 373 | `run_pipelined` | `crates/bench/src/bin/stm-p2.rs:362` |
| 347 | `main` | `crates/validator/src/bin/kardamom-validator/main.rs:69` |
| 252 | `with_pool` | `crates/stm/src/execute.rs:1877` |
| 242 | `push_prepared` | `crates/stm/src/execute.rs:2803` |
| 216 | `run` | `crates/bench/src/load/mod.rs:101` |
| 205 | `run_worker_block` | `crates/stm/src/execute.rs:3980` |
| 196 | `main` | `crates/bench/src/bin/stm-p0.rs:126` |
| 188 | `generate` | `crates/bench/src/stm/uniswap.rs:115` |
| 170 | `spawn_tx_ordering_reader` | `crates/engine/src/reader.rs:480` |
| 158 | `main` | `crates/da_watcher/src/bin/kardamom-da-watcher.rs:227` |
| 156 | `launch_with_l1` | `crates/e2e/src/harness/mod.rs:264` |
| 146 | `execute_one` | `crates/stm/src/execute.rs:4421` |
| 142 | `apply` | `crates/state/src/writer/mod.rs:238` |
| 141 | `begin_block_deferred_inner` | `crates/stm/src/execute.rs:2296` |
| 138 | `main` | `crates/executor/src/bin/kardamom-executor/main.rs:47` |
| 123 | `on_boundary` | `crates/engine/src/actor/exec_thread.rs:674` |
| 122 | `main` | `crates/sequencer/src/bin/kardamom-sequencer/main.rs:172` |
| 117 | `run_replay_merge` | `crates/log/src/replay.rs:241` |
| 113 | `evaluate` | `crates/bench/src/load/accounting.rs:136` |
| 111 | `watch_and_challenge` | `crates/batcher/src/optimistic.rs:138` |
| 103 | `run` | `crates/batcher/src/live.rs:477` |
| 101 | `launch` | `crates/e2e/src/harness/sealer.rs:53` |
| 102 | `deep_compare` | `crates/state/src/integrity/compare.rs:24` |

Every service binary has a `main` over 100 lines that does argument parsing, state
recovery, wiring, serving, and shutdown in one body. The reviewers name the helpers to
extract for each in the crate appendices (see the R2 tables).

## R3 — large files

Thirteen files pass 500 code lines. They fall into two groups.

**Over the limit only because of an inline test module.** Move the tests to a sibling
file, as `crates/validator/src/parallel/engine_tests.rs` and
`crates/engine/src/reader/cluster/tests.rs` already do. No logic split is needed.

| file | code lines | production lines after the move |
|---|---|---|
| `crates/engine/src/reader.rs` | 1,032 | 489 |
| `crates/exec-core/src/executor/scope.rs` | 809 | 492 |
| `crates/validator/src/epoch_verify.rs` | 605 | 299 |
| `crates/validator/src/attester.rs` | 555 | 342 |
| `crates/types/src/xchain.rs` | 541 | 282 |
| `crates/da_watcher/src/interop/watcher.rs` | 518 | about 180 |

**Large in their own right.** The reviewers wrote module-by-module split plans for
each; see the R3 tables in the appendices.

| file | code lines | plan in short |
|---|---|---|
| `crates/stm/src/execute.rs` | 3,153 | an `execute/` directory with `config`, `view`, `metrics`, `prepare`, `touch`, `graph`, `recycle`, `handle`, `session`, `tail`, `worker`, `stm` modules (`crate-stm.md`) |
| `crates/bench/src/bin/stm-p2.rs` | 1,676 | a `stm-p2/` directory with `main`, `args`, `alloc`, `scenario`, `common`, `mock_ab`, `mdbx_ab`, `pipeline` (`crate-bench.md`) |
| `crates/engine/src/actor/exec_thread.rs` | 629 | `exec_state`, `exec_records`, `exec_markers`, `exec_boundary`, in the pattern `exec_settle.rs` already uses (`crate-engine.md`) |
| `crates/e2e/src/harness/mod.rs` | 569 | `config`, `launch`, `control`, `shutdown`, `load_sampler` (`crate-e2e.md`) |

Test files over 500 code lines: `crates/stm/tests/equivalence.rs` (1,154; split into a
`common` module plus four themed test binaries), `crates/engine/src/actor/exec_tests.rs`
(750), `crates/validator/src/parallel/engine_tests.rs` (657).

The reviewers also marked 24 files under the limit as KEEP, with the reason, so that
the split work stops at the right place.

No Java file passes 500 code lines. `CanonicalSealerState.java` is the largest at 732
raw lines.

## R4 — manual drops

40 explicit `drop(x)` sites in production code, plus 33 in test code that mostly close
a channel sender to signal EOF. Three classes:

- **Lock guard release.** The fix is a helper whose return ends the borrow.
  Nine of them sit in one hand-rolled acquire loop in `crates/stm/src/execute.rs`
  (lines 4067 to 4134); one `fn next_job(...) -> Option<u32>` replaces all nine.
  Others: `execute.rs:1410, 1461, 2166, 2533, 2876`, `ingress/src/pending/mod.rs:222`,
  `validator/src/buffers.rs:99`, `validator/src/flight.rs:108`,
  `validator/src/interop/store.rs:79, 132`.
- **Resource release.** Aeron runtimes, cluster guards, mdbx transactions,
  file handles. Two are redundant last statements before `Ok(())`
  (`kardamom-da-watcher.rs:434`, `kardamom-sequencer/main.rs:343`). The rest need a
  scoped helper: `fn read_schema_version(env)` for `state/src/writer/mod.rs:448`,
  `fn stored_digest(env)` for `state/src/genesis.rs:97`, `fn write_image(...)` for
  `state/src/checkpoint_transfer.rs:295`, `fn flush_folded(guard)` for
  `bench/src/harness.rs:220`.
- **Channel sender close.** These are hard to remove because a worker's
  `recv()` loop ends on EOF. The reviewers propose a `shutdown(shared, reap_tx, tail_tx)`
  helper in `stm` and a `feed(records) -> Receiver<_>` test helper in `engine` so the
  drop lives in one place.

## R5 — mutexes and channels

251 sites were given a verdict. 219 are JUSTIFIED: real cross-thread seams, or a
measured hot path. 32 need a change:

**REPLACE_WITH_OWNERSHIP (11).**

- `crates/stm/src/execute.rs:3549` — `Vec<Mutex<Vec<usize>>>` where each chunk is the
  sole writer of its slot.
- `crates/bench/src/bin/stm-p2.rs:550` — an `Arc<Mutex<Vec<_>>>` the code already
  unwinds with `Arc::try_unwrap(..).into_inner()`. Return the `Vec` from the thread.
- `crates/bench/src/benchmark.rs:284-286` — per-task atomics and a mutex read only after
  the join. Return them from the `JoinHandle`.
- `crates/da_watcher/src/watcher.rs:208-209, 264` — two `Arc`s for a single task, plus
  an `impl EpochPublisher for Arc<P>` that exists only for them. The sibling
  `interop::watcher::spawn` already moves both by value.
- `crates/ingress/src/seen_receipts.rs:33` — a mutex on a set that one task touches.
- `crates/validator/src/lib.rs:61` — a `Mutex<Option<String>>` written once; use
  `OnceLock<String>`.
- `crates/validator/src/parallel/engine.rs:303` — `Vec<OnceLock<_>>` with one writer and
  one reader per slot; give `WorkerPool` a `map(n, body) -> Vec<T>`.
- `crates/state/src/writer/mod.rs:95` — a `bounded(0)` channel that exists only to feed
  `mem::replace`. Make the sender an `Option` and `take()` it.

**REPLACE_WITH_CHANNEL (3).**

- `crates/ingress/src/sig_verify.rs:69, 71` — a `Mutex<Vec<VerifyRequest>>` plus a
  `Notify` is a hand-built channel. `mpsc::unbounded_channel` with `recv_many` replaces
  both.
- `crates/state/src/swap.rs:31-47` — an `ArcSwapOption` that duplicates the value the
  `tokio::sync::watch` slot beside it already holds.

**UNNECESSARY (18).** Sixteen `Metrics` atomics in `crates/stm/src/execute.rs` that only
the feed thread or only the tail thread writes, or that nothing writes (plain `u64`
fields on `BlockSession` work), the `done_cv` condvar, `settled` in `stm-p2.rs`, and a
single-threaded `Arc<Mutex<..>>` in `log/src/testing.rs:442`. Full list in
`crate-stm.md` and `crate-log.md`.

## R6 — dynamic dispatch

45 `dyn` sites. The reviewers first kept 20 of them in `engine`. A second pass against
the consumers of each site shows that every one can go. The plan, in the order the
changes depend on each other:

1. **Delete four dead forwarding impls.** Every `EngineWiring` impl in the workspace
   names concrete types for `TxData`, `TxOrdering`, `WriterQueue` and `Epoch`, so
   `impl Trait for Box<dyn Trait>` at `engine/src/reader.rs:97, 107, 381` and
   `engine/src/actor/ports.rs:66` has no user.
2. **`RemoteEpochObserver` (5 sites in `engine` and `validator`).** Add
   `type RemoteEpoch: RemoteEpochObserver` to `EngineWiring`, next to `Epoch`, and make
   the field `Option<W::RemoteEpoch>`. The validator always passes one concrete verifier.
3. **`BlockExec<D>` (the boxed closure alias, 4 sites).** Add a `BlockExecStrategy<D>`
   trait with one `execute` method and `type BlockExec: BlockExecStrategy<SnapshotDb<Self>>`
   on the wiring. The builders in `executor/src/parallel.rs`, `validator/src/parallel/engine.rs`
   and `validator/src/prover.rs` return named structs. Roles without a strategy name a
   unit `NoBlockExec` type.
4. **`JoinRecoveryFactory` and `Option<Box<dyn JoinRecovery>>` (4 sites).** Add a
   `JoinRecoveryFactory` trait with `type Recovery: JoinRecovery` and
   `fn build(self) -> Option<Self::Recovery>`, and `type JoinRecovery: JoinRecoveryFactory`
   on the wiring. The factory is `Send`; `build` runs inside the reader thread, so the
   thread-bound Aeron client stays on its thread. The archive factory struct holds the
   config and stream ids that the closure captures today.
5. **`&mut dyn FnMut` sink arguments (6 sites).** Once nothing boxes `JoinRecovery`, the
   trait no longer needs to be dyn-compatible, so the sinks on `recover_tx_data`,
   `recover_deposits`, and the log refetcher's `fetch_tx_data` and `fetch_deposits`
   become `impl FnMut`.
6. **The validator receipts chain (`Box<dyn TxReceiptsPublication>`, 3 sites).** The
   chain has 0 to 2 runtime-optional wrappers. Keep one fixed type
   `ExtractingReceiptSink<AttestingReceiptSink<ValidatorReceiptSink>>` and move the
   runtime choice into an `Option` field inside each wrapper. One instantiation, no box.
   Make `ExtractingReceiptSink<S>` generic over its inner sink.
7. **`DeliverFn` in the Aeron runtime (`log/src/aeron_live/mod.rs:103`, 4 sites).** The
   handlers form a closed set, so replace the boxed closure with an enum:
   `Deliver::Typed(TypedDeliver)` for the eight `(BPosition, T)` streams (`TxError`,
   `EpochRecord`, `RemoteEpochRecord`, `FsyncWatermark`, `QuorumWatermark`,
   `BlockBoundary`, `Deposit`, `BalFrame`), `Deliver::TxData(sender)` for the
   session-aware `(TxDataLoc, TxEnvelope)` stream, `Deliver::ReceiptBatch(sender)` for the
   `Vec<Receipt>` fan-out, and `Deliver::RawFrames(sender)` for the cluster egress relay
   in `cluster-adapter`. `TypedDeliver` is itself an enum with one variant per message
   type, each holding its `UnboundedSender`. `AssembledDeliver` matches on the enum. A new
   message type is then a compile error at the match, not a silent new closure shape. The
   `rusteron_client::Handler` callback objects stay: they are the C client's FFI boundary.
8. **The rest, unchanged from the first pass.** `Box<dyn Error>` at five sites in
   `log/src/testing.rs` becomes `anyhow::Result`. Four `should_stop: &mut dyn FnMut`
   closures in `log/src/recorder.rs` take `&CancellationToken`. The `&dyn Fn` leaf encoder
   on `state/src/trie/walker.rs:72, 100, 251` goes, because both callers pass the same
   body. `stm/src/execute.rs:4433` takes a generic `F: FnMut`. `Deployer<DynProvider>` at
   `deployer/src/main.rs:218` gets two constructors. The `Pin<Box<dyn Future>>` at
   `ingress/src/json_rpc.rs:407` names its concrete future type.

One constraint shapes steps 3 and 4. The wiring pattern names every port as an
associated type, and an `impl Trait` return value has no name that an
`impl EngineWiring` block can write. So the builders return named structs, not
`impl Trait`. `impl Trait` in return position only fits where the consumer takes the
value as a direct generic parameter.

## R7 — generics to supertraits

Six repeated bound sets account for most of the 33 rows. Each becomes one supertrait
with a blanket impl:

| bound set | sites | proposed trait |
|---|---|---|
| `rkyv::Archive` + `Deserialize` + `CheckBytes` (3 to 5 bounds) | 9 in `log`, 3 in `batcher` | `WireMessage` in `log/src/codec.rs`; `ArchivedRecord` in `batcher` |
| `StateDatabase + Clone + Sync + 'static` | 3 in `executor`, 3 in `stm` | `SharedState` / `StmSnapshot` |
| `<S, Q, P, E>` on `ExecState`, `spawn_exec`, `exec_settle` | 3 in `engine` | `ExecPorts` with associated `Snapshots`, `WriterSignal`, `WriterQueue`, `Epoch`, `RemoteEpoch`; `EngineWiring: ExecPorts` |
| `<P, S>` with 4 to 6 bounds on 9 impl blocks | `ingress` proxy and handlers | `ProxyBackend { type Pub; type Sub; }` |
| `<I, B, R>` on `Sequencer::run` and `run_once` | 2 | `SequencerPorts { type In; type Refs; type Errors; }` |
| `<T, F, Fut>` where `Fut` only names `F::Output` | `e2e/metrics.rs:75`, `ingress/proxy/mod.rs:33` | `F: AsyncFnMut() -> ...`, drop `Fut` |

## R8 — unnecessary `pub`

The compiler pass found 38 unreachable `pub` items: 29 in binaries and libraries
(`sequencer` adapters and feeds, `validator` args, pumps and adoption, `ingress`
recorders and `PeerAddrLayer`, three `pub mod` under `log/src/aeron_live/handles`) and 9
in test helper modules. The list is in `mechanical-lists.md`.

The reviewers found about 220 more items that are reachable but used only inside their
own crate. The largest surfaces:

- `kardamom-state`: about 80 items, mostly the `schema`, `meta`, and `trie` codecs.
- `kardamom-bench`: about 30 library items reachable only from inside the library.
- `kardamom-validator`: 39 `pub` items in the binary's own modules, and 24 library
  items with no external user.
- `kardamom-log`: the four dead modules above, plus 7 `pub type` subscriber aliases.
- `batcher/src/live.rs`: 7 items only `live.rs` uses; `deployer/src/addresses.rs`: 6
  items only `deployer.rs` uses.
- `ingress`: only `IngressConfig`, `IngressHandle`, `IngressProxy` and `MockChannels`
  reach another crate, so 6 modules can drop to `pub(crate)`.

Suggested rule for the workspace: add `unreachable_pub = "warn"` under `[lints.rust]`
in the workspace `Cargo.toml`, and make binaries default to `pub(crate)`
(`kardamom-executor` already does).

## R9 — defensive checks instead of types

99 rows. The pattern repeats at four scales:

- **The same check at many layers.** The "channel URI has no NUL byte" check runs at 17
  sites in `log`; the MDS-enabled check at 6. In `bench/load/mod.rs:115` an emptiness
  check runs at four layers and is missing at a fifth, where a `u32` indexes a slice
  unchecked. In `e2e`, `ensure!(path.is_file())` runs at six sites in four modules. Each
  wants one newtype with a fallible constructor: `ChannelUri`, `NonEmptyPlan`,
  `ExistingFile`.
- **Raw scalars checked in the body.** `members >= 1` (`NonZeroUsize`), `seed != 0`
  (`NonZeroU64`), `first >= 1 && last >= first` (`BlockRange::new`), `message.value != 0`
  checked in both `exec-core` executors (a `ValuelessMessage` at the derivation boundary).
- **`assert!` on decoded input in non-test code.** `guest/kardamom-zk-guest/src/bin/batch.rs:31, 47`
  assert on the batch after `rkyv::from_bytes`; a `ContiguousBlocks` constructor validates
  once.
- **`cluster-client/src/protocol.rs`.** The SBE decoder checks template id, version, and
  length per field with `expect(...)`; parse the header once into a typed
  `MessageHeader` and dispatch on it (`crate-sequencer.md`).

The `types` crate is where most of these newtypes belong; `crate-state.md` lists which
raw values other crates validate that a `types` newtype could own.

## R10 — imperative where functional is clearer

127 rows in production code, 52 in tests. The reviewers also marked 11 loops as KEEP
where the body needs early return with a side effect or sits on a hot path. Common shapes:

- `let mut v = Vec::new(); for x in xs { v.push(f(x)?) }` →
  `xs.iter().map(f).collect::<Result<Vec<_>>>()?`. 40 sites; the same
  `for_each_row(&txn, db, |..| out.push(..))` shape appears at 7 sites in `state`.
- `let mut found = false; let mut sum = 0.0; for ...` → `filter_map(..).reduce(..)`.
- A `bool` flag threaded through a `.map` closure → `position(..)`.
- 30 loose `let mut` accumulators in `run_mdbx_ab` → one `MdbxAbTotals` struct with an
  `accumulate(&mut self, &StmOutcome)` method.
- `for (k, v) in &seen { ensure!(...) }` → `find(..)` then one `bail!`.
- `loop { match rx.recv() ... }` → `for msg in rx`.

## R11 — clippy pedantic

Clippy pedantic does not pass. CI runs default clippy with `-D warnings` (`justfile:147`),
which is clean, so the 2,526 pedantic warnings are all new work.

| crate | total | production | doc_markdown | missing_errors_doc | must_use_candidate | cast_possible_truncation | cast_precision_loss | other |
|---|---|---|---|---|---|---|---|---|
| log | 307 | 296 | 113 | 83 | 43 | 7 | 0 | 61 |
| e2e | 232 | 216 | 21 | 104 | 23 | 24 | 5 | 55 |
| bench | 215 | 182 | 37 | 17 | 3 | 16 | 80 | 62 |
| sequencer | 216 | 147 | 78 | 14 | 23 | 35 | 8 | 58 |
| stm | 186 | 171 | 15 | 20 | 4 | 48 | 3 | 96 |
| batcher | 182 | 128 | 60 | 39 | 10 | 23 | 3 | 47 |
| engine | 173 | 158 | 79 | 20 | 15 | 9 | 5 | 45 |
| validator | 151 | 125 | 36 | 18 | 25 | 13 | 3 | 56 |
| ingress | 150 | 111 | 57 | 13 | 17 | 9 | 13 | 41 |
| exec-core | 144 | 116 | 26 | 25 | 23 | 8 | 3 | 59 |
| state | 133 | 118 | 10 | 49 | 34 | 6 | 0 | 34 |
| types | 104 | 104 | 32 | 8 | 54 | 4 | 0 | 6 |
| executor | 84 | 27 | 34 | 0 | 1 | 17 | 2 | 30 |
| cluster-adapter | 56 | 54 | 6 | 11 | 19 | 4 | 0 | 16 |
| da_watcher | 51 | 51 | 3 | 4 | 10 | 6 | 3 | 25 |
| deployer | 49 | 41 | 11 | 4 | 20 | 1 | 0 | 13 |
| footprint | 45 | 45 | 0 | 0 | 10 | 13 | 14 | 8 |
| cluster-client | 33 | 33 | 10 | 4 | 15 | 1 | 0 | 3 |
| reconstruct, obs, interop-feed | 15 | 8 | 2 | 3 | 3 | 0 | 0 | 7 |

Three lints are more than half of the total and are mechanical to clear: `doc_markdown`
(630, backticks around identifiers in docs), `missing_errors_doc` (436, an `# Errors`
section on every `pub fn -> Result`), and `must_use_candidate` (352). The cast lints
(`cast_possible_truncation` 244, `cast_precision_loss` 142, `cast_lossless` 40,
`cast_possible_wrap` 25, `cast_sign_loss` 18) need a decision per site: `try_from`, a
wider type, or a documented `allow`. `too_many_lines` at the pedantic default of 100
flags 56 functions; this audit's R2 threshold of 50 flags 232.

### Functions with more than 7 arguments

Default clippy is clean on `too_many_arguments` only because 32 `allow` attributes hide
26 functions (one allow is module-wide, on `crates/batcher/src/prover_submit.rs`). The
reviewers propose one argument-group struct per cluster:

| args | function | proposed struct |
|---|---|---|
| 16 | `run_mdbx_ab` (`bench/src/bin/stm-p2.rs:825`) | `Workload<'_>`, `EngineOpts`, `RunOpts`; the signature becomes three arguments |
| 15 | `block_tail` (`stm/src/execute.rs:3295`) | `TailInput`, `TailTiming`, `TailStats`, `TailDeps`; delete the three unused arguments |
| 15 | `run_pipelined` (`bench/src/bin/stm-p2.rs:362`) | the same `Workload` and `EngineOpts` |
| 12 | `execute_one` (`stm/src/execute.rs:4421`) | `TxJob<'_>` and `ExecCtx<'_>`; the signature becomes `execute_one(evm, job, ctx, fresh_reads)` |
| 12 | `spawn_publish_loops` (`sequencer/.../feeds.rs:258`) | `PublishLoops<P>` (`crate-sequencer.md`) |
| 12 | `ExecState::new` and `spawn_exec` (`engine/src/actor/exec_thread.rs:199, 897`) | the `ExecPorts` supertrait collapses four type parameters and their five port arguments (`crate-engine.md`) |
| 11 | `execute_xchain_tx` (`exec-core/src/executor/xchain.rs:53`) | `TxSlot` |
| 11 | `pacer` (`bench/src/load/engine.rs:201`) | no struct proposed in the appendix; group the rate knobs into one struct and make the function `pub(crate)` |
| 10 | `run_deploy` (`deployer/src/main.rs:251`) | `DeployArgs` with a nested `OracleArgs` |
| 10 | `execute_deposit_tx`, `execute_once` (`exec-core`) | `TxSlot` |
| 8-9 | the five other `Executor` entry points in `exec-core/src/executor/scope.rs` | `TxSlot`; every count lands under 8 and all seven allows go |
| 8-9 | `msg_leaf`, `walk_account`, `walk_storage`, `execute_block_parallel`, `sign`, `submit_task`, `ramp_to_max`, `generate` | see the crate appendices |

`TxSlot` (`crate-exec-core.md`) is the model: a `Copy` struct with `tx_idx`, `position`,
`index_in_block`, and `cumulative_gas_before`, passed to every execute and skip entry
point instead of four loose scalars.

The other hidden lints: `mut_from_ref` at `stm/src/execute.rs:1646`, `type_complexity` at
`exec-core/src/delta.rs:39`, three `identity_op` in `cluster-client`, one
`needless_range_loop` in `stm-p2.rs`, and seven `dead_code` in test helpers.

## Java sealer (light pass)

- R1: two historical comments, listed above.
- R2: one method over 50 code lines, `CanonicalSealerState.load` at
  `cluster/sealer-service/core/src/main/java/io/kardamom/sealer/CanonicalSealerState.java:638`
  (73 lines).
- R3: no file over 500 code lines.

## Suggested order of work

1. Fix the fourteen defects above. Each is small and independent.
2. Move inline test modules out of the six files that are large only because of them.
   This is mechanical and removes half of the R3 list.
3. Add `TxSlot` and the other argument-group structs, delete the 32 `allow` attributes,
   and add `too_many_arguments` to the CI lint set.
4. Delete the dead modules and items (R8 and defect 8 and 9), then add
   `unreachable_pub = "warn"` to the workspace lints.
5. Sweep the R1 comments crate by crate. Delete history, keep invariants, fix the broken
   links.
6. Split `execute.rs`, `stm-p2.rs`, `exec_thread.rs`, and `harness/mod.rs` on the plans in
   the appendices, and extract helpers from the 26 functions over 100 lines while the
   files are open. Decide the 91 functions between 51 and 100 lines one by one as each
   file is touched.
7. Clear the three bulk pedantic lints (`doc_markdown`, `missing_errors_doc`,
   `must_use_candidate`), then decide the cast lints per site, then enable
   `clippy::pedantic` in CI.
8. Apply the R5, R6, R7, R9, and R10 changes crate by crate from the appendices.

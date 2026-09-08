# Status: engine

Scope: `crates/engine`. Gates: (1) `cargo clippy -p kardamom-engine --all-targets -- -D warnings`
clean, (2) `cargo clippy -p kardamom-engine --all-targets -- -W clippy::pedantic` shows 0
warnings under `crates/engine/src/**`, (3) `cargo test -p kardamom-engine` all pass,
(4) `cargo fmt -p kardamom-engine` applied. Phase C adds a fifth check:
(5) `cargo check --workspace --all-targets` clean. All five pass as of this writing
(54/54 tests, 0 deny-warnings, 0 pedantic warnings in this crate's files, workspace
check clean).

This file has two parts: the Phase A section below (as revised after the
coordinator's review — five fix items applied, three rows moved from Deferred to
Done), and the Phase C section (R12, R13, R15, R16, R14) after it.

# Phase A

## Done

### R2 long methods (10/10)
- `reader.rs:480 spawn_tx_ordering_reader` — split into `reader/threads.rs`'s
  `OrderingLoop` struct (`new`, `next_idx`, `send`, `on_tx_ref`,
  `warn_on_buffer_growth`, `expand_epoch`, `expand_remote_epoch`, `send_expanded<T>`).
- `reader.rs:733 join_envelope` — split into `reader/join.rs`'s `refetch_once` and
  `next_slice`; the loop body is now ~10 lines.
- `actor/exec_thread.rs:674 on_boundary` — split into `actor/exec_boundary.rs`'s
  `check_alignment`, `run_block_exec`, `handoff_bal`, `handoff_shadow`, `on_boundary`.
- `actor/exec_thread.rs:386 on_tx` — split into `actor/exec_records.rs`'s
  `scope_or_init`, `capture_shadow`, `log_bal_progress`, `on_tx`.
- `actor/commit_thread.rs:59 spawn_commit` — split into `Batch` struct,
  `collect_batch`, `publish_batch`, `publish_boundary`.
- `actor.rs:112 Executor::run` — split into `spawn_readers` plus a `Threads` struct
  (`join`, `join_pipeline`, `join_tx_data`) that owns the four handle groups, with
  `ThreadResult`/`SpawnedReaders` type aliases (also fixes the `type_complexity`
  pedantic warning on the return type; `ReaderResult` renamed to `ThreadResult` since
  it also types the exec and commit threads, per coordinator review; `Threads` is the
  Phase C R15 shape).
- `replay.rs:174 drive_block` — rewritten as a `try_fold` over a `BlockItem`/`ExecItem`
  chained iterator with a `BlockAcc` accumulator, plus `apply_one`/`seal_block`; this
  also folds the two near-identical loops R10 flagged at `replay.rs:194-257`.
- `shadow.rs:112 process_block` — split into `to_observations`, `emit_metrics`,
  `log_summary`.
- `reader/cluster/mod.rs:140 ingest` — split into `ingest_record`, `ingest_boundary`,
  `check_pending_overflow`.
- `bin_support.rs:396 replay_unavailable_fallback` — split into `stage_peer_checkpoint`,
  `log_resume_prepared`, with the original doc comment kept intact ahead of its own
  signature (see Errors and fixes note in the session; a self-introduced doc-splitting
  bug from this edit was caught and fixed before this state).

### R3 large files (5/5)
- `reader.rs` (1032 code lines) — split into `reader/{mod,ports,join,threads,tests}.rs`.
  Every public path used by `kardamom-batcher`, `kardamom-executor`, and
  `kardamom-cluster-adapter` is preserved (verified by grep and by `cargo check` on
  those crates, see Verification below).
- `actor/exec_thread.rs` (629 code lines) — split into `actor/{exec_state,exec_records,
  exec_markers,exec_boundary}.rs`; `exec_thread.rs` keeps `IDLE_SETTLE_PROBE`, `Recv`,
  `Flow`, `run`, `recv_next`, `spawn_exec` (180 lines).
- `bin_support.rs` (345 lines) — KEEP, confirmed under the limit, no change.
- `replay.rs` (312 lines) — KEEP, confirmed under the limit, no change.
- `reader/cluster/mod.rs` (193 lines) — KEEP, confirmed under the limit, no change.

### R4 manual drops (31/32; the 32nd is listed under "Not done, judged wrong")
- `persist.rs:193,211,254,284` — removed via a `with_queue(handle, |q| ...)`
  block-scoped helper whose scope ends the borrow, and a block-scoped handle drop for
  the writer-dropped test.
- `actor/exec_tests.rs:58,161,231,287,355,440,499,567,688,764,869` (now split across
  `actor/exec_tests/{streaming,bal_shadow,block_close,interop}.rs`) and
  `actor/exec_tests.rs:76,372,459,521,589` — removed via `feed()`/`feed_commits()`
  helpers in `actor/test_support.rs` (bounded channel pre-loaded, sender dropped
  inside the helper).
- `actor/exec_pipeline_tests.rs:78,153` — removed via `feed()`.
- `actor/exec_resume_tests.rs:69,144,212,268` — removed via `feed()`.
- `actor/commit_tests.rs:49,100,175,236,283` — removed via `feed_commits()`.
- Per coordinator review: `feed`/`feed_commits` now build an `unbounded()` channel
  (was `bounded(records.len().max(1))`, a `.max(1)` fixup R13 forbids). The sender
  still drops at the end of the helper, which is the property the tests need.

### R5 sync primitives (12/12, all confirmed JUSTIFIED, no change) plus the one fix row
- `reader.rs:133,454,460` (now `reader/{join,ports}.rs`), `reader/cluster/mod.rs:32,33`,
  `actor.rs:139,140`, `actor/exec_thread.rs:71,75`, `shadow.rs:92`, `persist.rs:61` —
  all JUSTIFIED, no change.
- `bin_support.rs:163` (unbounded tokio mpsc for the Aeron poll task) — JUSTIFIED to
  stay unbounded, but the recommended fix (a depth gauge) was applied: added
  `TX_DATA_QUEUE_DEPTH` const + `describe_gauge!` in `metrics.rs`, and
  `LiveTxDataSub::next()` now sets the gauge on every read.

### R6 dynamic dispatch (4 deletions done; all other rows deferred, see below)
- Deleted the four dead forwarding impls with no user anywhere in the workspace
  (confirmed by grep): `reader.rs:97` (`impl TxDataSubscription for Box<dyn ...>`),
  `reader.rs:107` (`impl TxOrderingSubscription for Box<dyn ...>`), `reader.rs:381`
  (`impl EpochObserver for Box<dyn ...>`), `actor/ports.rs:66`
  (`impl StateWriterQueue for Box<dyn ...>`). `actor/wiring.rs`'s doc example was
  updated to stop referencing the deleted impls.

### R8 unnecessary pub (9/9)
- `bin_support.rs:49 load_genesis` — made private.
- `shadow.rs:73 write_cells`, `shadow.rs:42 FEE_SINK` — `pub(crate)`.
- `shadow.rs:54-57 ShadowTxCapture` fields, `shadow.rs:62-67 ShadowBlock` fields —
  `pub(crate)` on the fields.
- `reader/cluster/mod.rs:32-33 ReplayCursor` fields — `pub(crate)`.
- `reader.rs:168 JoinBuffer::len` (now `reader/join.rs`) — `pub(crate)`.
- `reader.rs:172 JoinBuffer::is_empty` — deleted outright rather than narrowed to
  `pub(crate)` as the appendix suggested. The row's own evidence says it has no
  caller anywhere in the workspace, including in this crate's tests, so there is
  nothing for `len_without_is_empty` to pair it with; deleting it is strictly less
  dead code than keeping an unused `pub(crate)` method, and the pedantic gate
  confirms `len_without_is_empty` does not fire on the now-narrower `len`.
- `metrics.rs:57 HEALTH_BEACON_BEATS_TOTAL` — `pub(crate)`, plus added the missing
  `describe_counter!` line in `describe()`.

### R9 defensive validation (4/9; the rest are deferred, see below)
- `reader/cluster/mod.rs:104-107` check-then-unwrap — rewritten with the `BTreeMap`
  `Entry` API (`Entry::Occupied` + `entry.remove()`), per coordinator review; one
  lookup, no reinsert, no unwrap (an earlier remove-then-reinsert-on-miss pass did
  two map operations per not-ready call; the entry API does one).
- `reader.rs:739-743,809-815` (`TxDataKey`) — moved here from Deferred: a crate-local
  `struct TxDataKey { shard: u8, session: i32, position: BPosition }` (in
  `reader/join.rs`) replaces the loose `(u8, i32, BPosition)` triple through
  `JoinBuffer::insert`/`take`, `JoinWait` (formerly `join_envelope`), and the
  `tx_data` reader's insert call. No `ShardId`/`SessionId` newtype from
  `kardamom-types` is needed for this; Phase B swaps the field types when those
  newtypes exist. Confirmed by grep that `JoinBuffer::insert`/`take` have no caller
  outside this crate (`kardamom-batcher` only constructs `JoinBuffer::new()` and
  passes it through).
- `actor/exec_settle.rs:52-57` check-then-unwrap — rewritten with
  `VecDeque::pop_front_if` (stable on the toolchain in use), removing the
  `.expect("front checked")`.
- The `verify_record_identity` / two-layer `BoundaryMisaligned` row
  (`actor/exec_thread.rs:396` and `reader/cluster/mod.rs:184` vs.
  `actor/exec_thread.rs:698`, now `actor/exec_records.rs` and
  `actor/exec_boundary.rs`): the identity-check half needed no code change — it is
  already documented as deliberate defense-in-depth, priced at one ecrecover per
  transaction, at `actor/types.rs:93`. The distinct-`BoundaryMisaligned`-variants
  half is deferred (see below), since `ExecutorError` is not defined in this crate.

### R10 imperative style (9/12; 1 is a documented Keep with no action needed beyond
adding the requested comment, 1 stays Keep with no action, 1 is listed under "Not
done, judged wrong")
- `shadow.rs:74` write-cells loop — rewritten as an iterator chain in `write_cells`.
  The appendix's suggested `.keys()` calls do not compile: `WriteSet.accounts` and
  `.storage` are `SmallVec`s, not maps, so the rewrite uses
  `.iter().map(|(addr, _)| ...)` / `.iter().map(|((addr, key), _)| ...)` instead of
  `.keys()`, with the same output shape.
- `shadow.rs:103` exclude-set build — rewritten as `HashSet::from([Cell::Account(FEE_SINK)])`.
- `bin_support.rs:93` genesis alloc build — per coordinator review, replaced with a
  call to `kardamom_types::Genesis::to_alloc()` (the shared builder that already
  exists in `kardamom-types`) plus the per-entry `tracing::info!` loop. This is a
  call to a public method, not a cross-crate change, and removes both the two-pass
  duplication an earlier iterator-chain rewrite left in place and the second keccak
  per code entry that rewrite introduced.
- `reader.rs:604,667` (now `reader/threads.rs`) — kept the imperative early-return
  shape as the brief exempts, but extracted the shared body into
  `send_expanded<T>(...)`, called from both the epoch and remote-epoch arms.
- `reader.rs:774` (now `reader/join.rs refetch_once`) — Keep, no change. The sink is
  a `&mut dyn FnMut` by trait contract, so the `recovered` counter must stay a
  captured mutable.
- `actor/commit_thread.rs:76-94` — extracted `collect_batch(&rx) -> Batch`, keeping
  the loop itself imperative (it must stop at a boundary and at the batch cap).
- `actor/commit_thread.rs:116` — Keep, no change to the loop shape, plus added the
  requested comment above `let mut from` explaining why a slice iterator cannot
  express the must-deliver resume point.
- `actor/exec_thread.rs:747` (now in `actor/exec_boundary.rs`'s `run_block_exec`) —
  Keep, no change: early return with a side effect, exempted by the brief.
- `actor/exec_settle.rs:73` — already functional (`fold`), no change needed.

### R1 comments (24/24 production + 6/6 tests)
Every row in both `crate-engine.md`'s R1 table and its Tests/R1 table was resolved:
present-tense rewrite, deletion of historical narrative, or a confirmed Keep for the
non-historical present-tense rows (`exec_thread.rs:816`/`bin_support.rs:185`-style
"keep the present rule" rows, `actor/exec_tests.rs:381`, `actor/test_support.rs:106`).
This includes `reader.rs:3,94,182,352,611,688,722` (now spread across
`reader/{mod,ports,join,threads}.rs`), `actor/exec_thread.rs:37,63,118,164,816,879`
(now spread across `exec_thread.rs`/`exec_boundary.rs`; the `879` "New block opens..."
line was deleted per the fix column), `actor/exec_settle.rs:11,124`,
`actor/wiring.rs:3`, `actor/types.rs:121,136`, `actor/commit_thread.rs:109`,
`actor/ports.rs:30`, `bin_support.rs:11,185,472`, `metrics.rs:3`, `lib.rs:27`,
`state.rs:9`, `replay.rs:282`, `actor/exec_resume_tests.rs:1` (retitled "Resume from
a mid-chain cursor", body rewritten to match the current `ResumePoint` contract:
the reader delivers only post-cursor records with absolute indices, and the exec
thread seeds its counters from the cursor), `actor/exec_tests.rs:537,737` (now in
`actor/exec_tests/{block_close,interop}.rs`), `reader.rs:1367,1389` (now
`reader/tests.rs`, both "Before this fix"/"the old ... key" phrasings rewritten to
present-tense statements of the invariant). Also fixed `actor.rs`'s module doc,
which said "See `reader.rs`" for what is now a directory — changed to "See the
`reader` module". Per coordinator review, also fixed a remaining historical-narrative
comment the first pass missed: `reader/tests.rs`'s
`channel_b_reader_expands_a_remote_epoch_into_marker_plus_messages` test had
"collided every position-keyed receipt consumer ... S14's two-message batch
false-diverged the validator" — rewritten to state the invariant directly (each
message needs a distinct position; no ticket id, no incident story).

### R11 clippy::pedantic (173/173 production sites; 0 remain under
`crates/engine/src/**`, confirmed by `cargo clippy -p kardamom-engine --all-targets
-- -W clippy::pedantic`)
Grouped by original file (counts from `inputs-engine.md`'s 173-row table; all now
fixed with real docs, not boilerplate):
- `actor.rs` (12: 9 `doc_markdown`, `missing_panics_doc`+`missing_errors_doc` on
  `run`, 1 `needless_pass_by_value`) — all 12 fixed. The `needless_pass_by_value` on
  `run`'s `cfg: ExecutorConfig` no longer needs an `#[allow]`: per coordinator
  review, `cfg` now moves into `spawn_exec` (via the `ExecInputs` struct, Phase C row
  6) instead of being cloned, so the lint stops firing on its own. This moves out of
  the "R11 clippy::pedantic exception" row that used to sit under Deferred to Phase B.
- `actor/commit_tests.rs` (5), `actor/commit_thread.rs` (1),
  `actor/exec_pipeline_tests.rs` (2), `actor/exec_settle.rs` (4),
  `actor/exec_tests.rs` (8, now spread across `actor/exec_tests/*.rs`),
  `actor/exec_thread.rs` (16, now spread across `exec_thread.rs` and the
  `exec_{state,records,markers,boundary}.rs` split), `actor/ports.rs` (6),
  `actor/test_support.rs` (1), `actor/types.rs` (5), `actor/wiring.rs` (7),
  `bin_support.rs` (25), `lib.rs` (2), `persist.rs` (6), `reader.rs` (51, now spread
  across `reader/{mod,ports,join,threads,tests,cluster}.rs`),
  `reader/cluster/mod.rs` (6), `reader/cluster/tests.rs` (4), `replay.rs` (4),
  `shadow.rs` (7), `state.rs` (1) — all fixed.
- Representative fix classes used: real `# Errors`/`# Panics` sections (no
  boilerplate — each names the actual failure condition), `#[must_use]` only where
  ignoring the value is a real bug, `as` casts replaced with `try_from`/widened
  types, or given a scoped `#[allow(..., reason = "...")]` naming the bound that
  makes the cast safe (e.g. `u64::try_from(...).unwrap_or(u64::MAX)` for a timeout
  cast in `reader/threads.rs`; `cast_precision_loss` allows on block-number-to-`f64`
  gauge casts, reasoned "block numbers stay far below 2^52"; test-fixture casts in
  `reader/cluster/tests.rs` given `i32::try_from(i).expect("small test fixture")` or
  a scoped allow with reason).

### Mechanical rows (`inputs-engine.md`)
- The 3 large-file rows (`reader.rs` 1032/1466, `actor/exec_tests.rs` 750/893,
  `actor/exec_thread.rs` 629/937) — covered under R3 above.
- `metrics.rs:71 describe` (70 lines) — reviewed, not split. It is a single linear
  list of `describe_counter!`/`describe_gauge!` calls with no natural helper to
  name; case-by-case judgment under the 51-100 line rule.
- The 12 test-function line-count rows between 51 and 95 lines
  (`actor/exec_tests.rs:741,28,21,119,324,475,541,667` now split across
  `actor/exec_tests/*.rs`; `actor/exec_pipeline_tests.rs:28,134,204`;
  `actor/exec_resume_tests.rs:27,107`; `reader.rs:930,1007,1136,1402` now
  `reader/tests.rs`) — reviewed under the case-by-case 51-100 line rule. Each is a
  single-purpose linear test body already named for what it tests, with no helper
  named in `crate-engine.md`'s R2 table (which covers only production code and one
  named test split, `actor/exec_tests.rs`'s R3 directory split, already done). Left
  as-is.
- `run` (`actor.rs:112`, 56 lines) — covered under R2 above (already split).
- `actor/exec_thread.rs:199 ExecState::new` (12 args) and `actor/exec_thread.rs:897
  spawn_exec` (12 args) — moved here from Deferred (Phase C row 6): both are
  crate-private (`ExecState::new` is `pub(super)`, `spawn_exec` is `pub(crate)`, with
  its only callers `Executor::run` and this crate's tests), so the argument-group
  struct is in-crate, not blocked on cross-crate coordination. Grouped into
  `ExecInputs<S, Q, P, E> { cfg, rx, tx, snapshots, sw_signal, sw_queue, start, hooks
  }` and `ExecHooks<Db, E> { bal_tx, shadow_tx, block_exec, epoch_observer,
  remote_epoch_observer }`, both in `actor/exec_state.rs`. `spawn_exec` now takes one
  argument; `#[allow(clippy::too_many_arguments)]` is gone from both. The `ExecPorts`
  supertrait that would also collapse the 4 type parameters `S, Q, P, E` into one
  stays Phase B, as planned (see R7 below).

### R14 (pulled forward alongside row 6): `ExecRig` test builder
Building `ExecInputs`/`ExecHooks` moved every one of the 18 `spawn_exec` test call
sites (`actor/exec_resume_tests.rs`, `actor/exec_pipeline_tests.rs`,
`actor/exec_tests/{streaming,interop,block_close,bal_shadow}.rs`) in the same pass,
so each site was edited once instead of twice. `actor/test_support.rs` gained
`struct ExecRig<S, Q, P>` with `fn new(snapshots, sw_signal, sw_queue) -> Self` (every
hook and `cfg`/`start` default — every current test uses `ExecutorConfig::default()`
and `ResumePoint::GENESIS` unless overridden), builder methods `start`, `bal`,
`shadow`, `block_exec`, `remote`, and `fn spawn(self, rx, tx) -> JoinHandle<...>`.
This is the `dry-engine.md` R14 tests row (`ExecRig`, ~170 lines), done in the
version needed for row 6; it does not also absorb the writer-log `Arc<Mutex<...>>`
or the two channel constructions dry-engine.md's fuller version describes — that
narrower slice is left undone (noted under Phase C R14 below).

## Deferred to Phase B

Per the launching agent's explicit instruction: the `EngineWiring` wiring changes
(`ExecPorts`, `RemoteEpoch`, `BlockExecStrategy`, `JoinRecoveryFactory` trait) and
the `ExecState::new`/`spawn_exec` argument-group changes are deferred because
`kardamom-executor`, `kardamom-validator`, `kardamom-bench`, and the e2e suite
consume these signatures. Every row below either depends on that wiring, needs a
type that lives outside `crates/engine`, or is itself a signature/visibility change
another crate calls.

### R6 dynamic dispatch (14 rows, plus 1 in Tests)
- `actor/exec_thread.rs:89,211,909` and `actor/wiring.rs:137`
  (`remote_epoch_observer: Option<Box<dyn RemoteEpochObserver>>`) — needs
  `EngineWiring::RemoteEpoch`.
- `reader.rs:209,218` (now `reader/ports.rs`, `sink: &mut dyn FnMut(...)`),
  `reader.rs:225` (`JoinRecoveryFactory` closure type), `reader.rs:495,735` (now
  `reader/{threads,join}.rs`, `Option<Box<dyn JoinRecovery>>`) — needs
  `JoinRecoveryFactory` to become a trait with `type Recovery: JoinRecovery`.
- `actor/types.rs:139` (`BlockExec<D> = Box<dyn Fn(...) -> ...>`) — needs
  `BlockExecStrategy<D>` trait plus `type BlockExec` on the wiring.
- `bin_support.rs:232,247,297` — implements/builds the `JoinRecovery` seams above;
  follows once the trait changes.
- Tests: `actor/exec_tests.rs:703` (now `actor/exec_tests/interop.rs`,
  `Some(Box::new(RecordingRemoteObserver(...)))`) — becomes unboxed once
  `EngineWiring::RemoteEpoch` exists.

### R7 too many generics (3/3; unchanged by row 6)
- `actor/exec_thread.rs:897 spawn_exec` (now `ExecInputs<S,Q,P,E>`'s 4 type params),
  `actor/exec_thread.rs:64 ExecState<S,Q,P,E>` (4 type params) — needs the
  `ExecPorts` supertrait to collapse `S, Q, P, E` into one `W: ExecPorts` parameter.
  Row 6's `ExecInputs`/`ExecHooks` grouped the 12 *arguments* into structs, but the 4
  *type parameters* they still carry are unchanged; that collapse is still Phase B,
  tied to the `EngineWiring`/`RemoteEpoch`/`BlockExecStrategy` wiring in R6 above.
- `actor/exec_settle.rs:31 impl<S,Q,P,E> ExecState<S,Q,P,E>` — same, removes the
  duplicated 4-type-param header shared with `exec_thread.rs`.

### R9 defensive validation (7/9)
- `bin_support.rs:75` (`ChainId(NonZeroU64)`) — needs `NonZeroU64` (R13), and
  `resolve_genesis` is called from `kardamom-executor`'s and `kardamom-validator`'s
  `main.rs`.
- `bin_support.rs:273` (`RefetchEndpoints` newtype) — `archive_join_recovery`'s
  signature is called positionally from `kardamom-executor`'s `main.rs`,
  `kardamom-validator`'s `main.rs`, and `kardamom-batcher`'s `live.rs`; changing it
  to take `Option<RefetchEndpoints>` is a cross-crate signature change.
- `reader.rs:229-253` (`dedup_window`/`buffer_warn_threshold` as `NonZeroUsize`) —
  `dedup_window` is done (see Phase C R13 below); `buffer_warn_threshold` needs no
  type, per R13's own verdict.
- `reader.rs:700` (`ExecutorError::LegacyDepositRef`) and
  `reader/cluster/mod.rs:225` (`ExecutorError::ReplayBufferOverflow`) — `ExecutorError`
  is defined in `crates/exec-core`, outside this directory's scope.
- The distinct-`BoundaryMisaligned`-variants half of the
  `actor/exec_thread.rs:396`/`reader/cluster/mod.rs:184` vs.
  `actor/exec_thread.rs:698` row — same reason, `ExecutorError` lives in
  `crates/exec-core`.

(`TxDataKey` moved to Done above; the R11 `needless_pass_by_value` exception on
`actor.rs:113` moved to Done above, both per coordinator review.)

### R10 imperative style (1/12)
- `reader.rs:509` (now `reader/threads.rs`, hand-rolled `loop { match ... next() }`)
  — the suggested fix gives `TxOrderingSubscription` a `fn records(self) -> impl
  Iterator<Item = Result<...>>` adaptor. `TxOrderingSubscription` is a public trait
  implemented outside this crate (`kardamom-cluster-adapter`, and exercised by
  `kardamom-log`'s test doubles), so adding this method and changing the call site's
  contract (mapping `TxOrderingClosed` to a `None` from the adaptor) is a
  cross-crate trait-shape change.

## Not done, judged wrong

All four rows below were reviewed and accepted by the coordinator; the R6 row is
explicitly in scope for Phase C to resolve (enum vs. generic for the validator's
attester tee).

- `shadow.rs:196` (R10): the appendix suggests `obs.iter().for_each(|o|
  stats.learn_obs(o));`. This trips `clippy::pedantic`'s `needless_for_each` lint,
  which gate 2 (pedantic, zero warnings) forbids. Reverted to a plain `for` loop;
  the imperative form is the one that satisfies both R10's intent (no stray mutable
  accumulator — there is none here) and the pedantic gate.
- `actor/exec_pipeline_tests.rs:249` (R10, `let mut boundaries = Vec::new(); ...
  recv_timeout` deadline-polling loop, now at line 246-256): the appendix's implied
  `try_iter().collect()` fix (matching the sibling row at `exec_pipeline_tests.rs:100`,
  which was applied) would race the asynchronous idle-probe settlement this test
  exercises — `try_iter` can drain zero, one, or two of the three expected boundaries
  before the idle probe fires, since nothing else signals when settlement has
  finished. The `recv_timeout`-with-deadline loop is required to reliably observe
  all three boundaries. Left as the one exception to the sibling fix in the same
  file.
- `actor/exec_pipeline_tests.rs:269` (R4, the 32nd `drop(tx_r2e)` — now at line 266,
  in `exec_settles_inflight_commits_while_idle`): the channel must stay open while
  the test polls the idle probe (records may still arrive on it up to that point,
  and the whole point of the test is to observe settlement with no further reader
  input after this point). Using the `feed()` helper here would close the channel
  before the idle-probe assertions run, changing what the test observes rather than
  merely tidying its shape. Left as a manual `drop` at the point the test's own
  narrative calls for EOF.
- `actor/ports.rs:74` (R6, `impl TxReceiptsPublication for Box<dyn
  TxReceiptsPublication>`): the R6 addendum says "every one can go", including this
  row. A workspace grep found a real external caller —
  `crates/validator/src/bin/kardamom-validator/main.rs:61,295,298` boxes a
  `TxReceiptsPublication` sink and calls through this impl. Deleting it would break
  the validator binary, which is out of this crate's scope to change. Kept, per the
  launching agent's explicit instruction, which matches what the grep confirms.

## Verification of cross-crate consumers (Phase A)

`kardamom-batcher`, `kardamom-executor`, and `kardamom-cluster-adapter` were checked
against the split module structure: `cargo check -p kardamom-cluster-adapter --lib
--tests` and `cargo check -p kardamom-batcher -p kardamom-executor --lib` both
complete with no errors attributable to this crate's changes (the only errors seen,
in `kardamom-batcher`'s lib and `kardamom-deployer`, are a pre-existing environment
gap — missing `contracts/out/*.json` Foundry build artifacts — unrelated to this
audit). `kardamom-validator`'s and `kardamom-deployer`'s full builds are blocked by
the same pre-existing gap, not by anything in this change.

# Phase C

Order followed: R13, R12, rows 6-8 (folded into the Phase A section above per
coordinator instruction, since they moved Deferred → Done there), R15, R16, R14.
Inputs: `docs/reviews/2026-09-07-code-quality-audit/arith-engine.md` (R12, R13) and
`dry-engine.md` (R14) in the main workspace, read-only, not copied here.

## Done

### R13 non-zero types (in-crate rows: 2 typed, 3 need-no-type, confirmed)
- `reader.rs:253`/`reader/join.rs:103` `ReaderConfig::dedup_window` — typed
  `NonZeroUsize`. The boundary is `ReaderConfig::default()` (`reader/join.rs`),
  in-crate. Grepped the workspace for `ReaderConfig { ... dedup_window: ... }`
  literals outside `crates/engine`: none exist — every external construction site
  (`crates/batcher/src/live.rs:548`, `crates/executor/tests/m_plus_one_join.rs:480`)
  uses `..ReaderConfig::default()` and never names this field.
- `reader.rs:414` `DedupWindow::new`'s `capacity` — typed `NonZeroUsize` (follows
  from the field above; `DedupWindow` is `pub(super)`, no external construction).
  `DedupWindow::first_seen`'s guard now reads `self.capacity.get()`.
- `actor/exec_settle.rs:29` `COMMIT_PIPELINE_DEPTH` — typed
  `const COMMIT_PIPELINE_DEPTH: NonZeroUsize = NonZeroUsize::new(4).expect("4 is
  nonzero")` (const `Option::unwrap`-family calls are stable on this toolchain).
  Was already safe (a literal), but the type now makes the depth-cap guard's
  correctness independent of that literal staying nonzero.
- `reader.rs:246` `buffer_warn_threshold` — needs no type, per the row's own verdict
  (zero only widens the warn window; accounted for). No change.
- `actor/types.rs:59` `self.block > 0` — needs no type; a genesis test, not a zero
  guard before a division. No change.
- `actor/exec_thread.rs:844` `self.shadow_serial > 0` — needs no type; an emptiness
  test that skips an empty block's handoff. No change.

### R12 safe arithmetic (15/15 FIX rows; 5/5 HOT_PATH_KEEP and 17/17 PROVEN rows
reviewed, kept as-is)
FIX rows, all applied:
- `reader/cluster/mod.rs:133` `ni + slot_width(&msg)` — `checked_add` →
  `ExecutorError::State`; `try_deliver` is now `Result<Option<...>, ExecutorError>`,
  and `next()` propagates with `?`.
- `reader/cluster/mod.rs:108` `next_block.store(nb + 1, ...)` — `nb.saturating_add(1)`.
- `actor/exec_thread.rs:878` (now `actor/exec_boundary.rs`) `current_block =
  block_number + 1` — `block_number.saturating_add(1)`.
- `actor/exec_thread.rs:236` (now `actor/exec_state.rs`) `current_block: start.block +
  1` — `start.block.saturating_add(1)`.
- `bin_support.rs:355` `ReplayCursor::new(start.record_count, start.block + 1)` —
  `start.block.saturating_add(1)`.
- `actor/commit_thread.rs:118` `from += published` — `from =
  from.saturating_add(published).min(receipts.len())`, so an over-large count from
  the `TxReceiptsPublication` trait cannot drop the unpublished suffix.
- `bin_support.rs:236` `self.tx_data_stream_base + shard_id as i32` — the cast was
  already `i32::from(shard_id)` (PROVEN, no `as`); the addition is now
  `checked_add(...).ok_or_else(|| format!(...))?`, returned as the trait's `Err(String)`.
- `replay.rs:225,251` (now one call site, `BlockAcc::apply_one`) `cumulative_gas +=
  receipt.gas_used` — `checked_add(...).ok_or(ReplayError::GasOverflow { block_number
  })?`. `ReplayError` is defined in this crate (`replay.rs`), so the new variant is
  in-scope, unlike the exec-core `ExecutorError` rows below.
- `reader.rs:744,820` (now `reader/join.rs`'s `JoinWait::new`/`wait_for`)
  `Instant::now() + timeout` — `JoinWait::new` computes the one deadline with
  `Instant::checked_add(cfg.join_timeout).ok_or_else(|| ExecutorError::State(...))?`;
  each per-slice wait computes its own end via `Instant::now().checked_add(timeout)`,
  clamped to `self.deadline` on overflow (`map_or(self.deadline, |t|
  t.min(self.deadline))` — an overflowed sum only means "past the deadline already",
  which is the correct outcome for an oversized `timeout`, not a fixup).
- `reader.rs:535,541` (now one call site, `reader/threads.rs`) `timeout_ms =
  cfg.join_timeout.as_millis() as u64` — already `u64::try_from(...).unwrap_or(u64::MAX)`
  from Phase A; the two sites collapsed into one when `spawn_tx_ordering_reader` was
  split, so this is one FIX now, not two.
- `reader.rs:777` (now `reader/join.rs`'s `refetch_once`) `recovered += 1` —
  `recovered = recovered.saturating_add(1)`.
- `actor/commit_thread.rs:51` (now `MustDeliver::retry`) `*attempts += 1` —
  `self.attempts = self.attempts.saturating_add(1)`.

HOT_PATH_KEEP rows (5), reviewed, no change: `actor/exec_thread.rs:376,441,538,661`
(`tx_index_in_block` and its `+ 1` uses, reset each boundary, bounded by one block's
records) and `replay.rs:227,253` (same counter, reset each block in `BlockAcc::new`).

PROVEN rows (17), reviewed, no change except the cast-style cleanup the coordinator
asked for on the two sites that carried an `#[allow]`:
- `reader.rs:561,590,606,653,669` (now `reader/threads.rs`) `next_tx_idx.next()` —
  `u64` headroom is 5.8 million years at 100k records/s.
- `actor/exec_thread.rs:419,522` (now `exec_records.rs`) `current_block.saturating_sub(1)`
  — opposite-mistake check, already saturating.
- `reader.rs:794` (now `reader/join.rs`) `deadline.saturating_duration_since(...)` —
  opposite-mistake check, already saturating.
- `actor/exec_thread.rs:543,378,726` (now `exec_records.rs`/`exec_boundary.rs`) —
  `shadow_serial`, and the two `block_apply_elapsed` accumulators, all reset each
  boundary.
- `replay.rs:207,208,228,229,230,254,255,256,290` `counters.* += 1` — `u64` range
  against one increment per record or block.
- `reader.rs:550` (now `reader/threads.rs`) `cur > last_warn_len * 2` — every
  `JoinBuffer` entry holds at least one word, so `len < usize::MAX / 2` always.
- `shadow.rs:74` `Vec::with_capacity(ws.accounts.len() + ws.storage.len())` — both
  maps stay resident at once.
- `shadow.rs:145,164,165,166,168,169,170,171` (now inside `GradedBlock::emit_metrics`)
  `usize`/`u32` → `u64`/`f64` casts — widen on 64-bit targets, or exact below 2^53
  (metrics gauges only); the function-level `#[allow(cast_precision_loss)]` stays.
- `actor/exec_settle.rs:58`, `reader/cluster/mod.rs:115` `.set(b.block_number as f64)`
  — exact below 2^53; the scoped `#[allow(cast_precision_loss)]`s stay, per the
  coordinator's note that these are fine as-is.
- `reader.rs:1009,1011,1067,1122,1186,1196` (now `reader/tests.rs`) and
  `commit_tests.rs:162,233`, `persist.rs:163` — test-loop-index casts over small
  fixed ranges; kept as plain casts (no `#[allow]` was present, nothing to remove).
- `reader/cluster/tests.rs:11,36,106` `off as u8`/`i as i32` — PROVEN-safe (loop range
  `0..5`), but coordinator review still asked these off the `as` cast, since they
  carried a scoped `#[allow(cast_possible_truncation, cast_sign_loss)]`. Changed
  `relayed_txref` to `u8::try_from(off).expect("test fixture: off is a small
  non-negative offset")`; the two-lint `#[allow]` is gone. The `i as i32` sites were
  already `i32::try_from(i).expect(...)` from Phase A.

**Flagged, not implemented — the "Join buffer bounds" focus note** in
`arith-engine.md`: `JoinBuffer` has no capacity cap, no TTL, no eviction. Two paths
(archive refetch re-inserting a range on every retry slice; a `tx_data` fragment
whose `TxRef` never arrives) can grow it without bound, ending in an OOM kill rather
than a bounded failure. This is neither an R12 row (no arithmetic) nor an R13 row (no
zero-guard); it is a new bounded-capacity feature with new failure semantics (a hard
error at a cap, matching `reader/cluster/mod.rs`'s `MAX_PENDING` shape). Left for the
coordinator to schedule; not attempted here.

### R15 methods not standalone functions
- `actor/commit_thread.rs`: `collect_batch`, `publish_batch`, `publish_boundary`
  became methods on `CommitLoop<C> { tx_receipts_pub, rx }`, with a `run(mut self)`
  method `spawn_commit` calls. `retry_must_deliver`'s `&mut attempts` became a small
  `MustDeliver { attempts }` struct with a `retry` method, one instance per publish
  call (matches the original per-call retry-count reset).
- `reader/join.rs`: `join_envelope`, `refetch_once`, `next_slice`, `wait_for_envelope`
  became methods on `JoinWait<'a> { buffer, cfg, key: TxDataKey, deadline, recovery }`
  (`new`, `run`, `refetch_once`, `next_slice`, `wait_for`). `refetch_once` copies
  `self.buffer`/`self.key` into locals before borrowing `self.recovery` mutably, so
  the archive-refetch closure captures the locals, not `self` — avoids the
  overlapping-borrow trap the Phase A `scope_or_init` postmortem flagged.
- `actor.rs`: `spawn_readers` stays a function (a constructor, not behavior, per
  advisor guidance against over-engineering); `join_pipeline`/`join_tx_data` became
  `Threads { tx_data, tx_ordering, exec, commit }`'s methods, with a `join(self)`
  entry point. `join_pipeline`/`join_tx_data` take the specific owned handles as
  parameters (destructured from `self` in `join`) rather than `&self`, since
  `JoinHandle::join` consumes by value and can't be called through a shared borrow.
- `bin_support.rs`: `stage_peer_checkpoint`, `log_resume_prepared` became methods on
  `ResyncFallback<'a> { checkpoint_peers, state_dir, expected_genesis,
  adopted_unverified }`. `replay_unavailable_fallback` stays the `pub` entry point
  both binaries call; it builds one `ResyncFallback` and calls both methods.
- `shadow.rs`: `to_observations` (renamed `into_observations`: `clippy::pedantic`'s
  `wrong_self_convention` rejects a `to_*` method that consumes `self` by value)
  became a method on `ShadowBlock`, since it consumes exactly `ShadowBlock`'s own
  `captures`/`block_number` fields. `emit_metrics`/`log_summary` became methods on a
  new `GradedBlock { block_number, g, serial_records, accumulator_reads }`.
  `process_block`/`run`/`train` became methods on `Shadow { stats, exclude }`
  (`new`, `run`, `process_block`, `train`); the one production call site
  (`spawn_from_env`) and the one test call site (`bal_shadow.rs`) both updated.
- `replay.rs`: `apply_one` became a method on `BlockAcc` (`&mut self`, returns
  `Result<(), ReplayError>` instead of consuming and returning `BlockAcc` by value,
  since a method's natural shape mutates `self`). `seal_block`/`drive_block` became
  methods on a new `Replay<'a> { queue, signal, source, chain_id, counters }`, which
  owns the counters across the whole run (`replay_blocks` builds one `Replay` and
  calls `drive_block` per block via `try_for_each`, reading `replay.counters` back
  out at the end for the outcome).
- `OrderingLoop` (`reader/threads.rs`) and `ExecState` (`actor/exec_state.rs` +
  its arm-module `impl` blocks) already had the right shape from Phase A; no change.

### R16 no nested loops
- Production: reviewed every `for`/`while` in `crates/engine/src` (grep for `for .*
  in .*\{` across non-test files) — none is nested inside another loop.
  `Shadow::run`'s receive loop around `process_block`'s training loop is the one the
  coordinator named; `process_block` calls `self.train(&obs)`, a method, so the
  "training loop" is a helper call, not a loop nested inside `run`'s loop.
  `send_expanded` inside `OrderingLoop`'s call sites was already a method (Phase A).
- Tests: `reader/tests.rs` had two `for (i, expected) in ... { match &out[1 + i] {
  ... } }` sites (`channel_b_reader_expands_an_epoch_into_marker_plus_deposits`,
  `channel_b_reader_expands_a_remote_epoch_into_marker_plus_messages`) — a `match`
  nested inside a `for` counts as the nesting this rule targets. Extracted
  `assert_deposit_at(i, expected, got)` and `assert_xchain_at(i, origin, expected,
  got)`; each loop body is now one call. `actor/exec_tests/bal_shadow.rs`'s one `for`
  loop (`exec_hands_off_shadow_captures_at_boundary`) has no nested match — reviewed,
  left as-is.

### R14 (production rows already discharged, or done now)
- `layered_storage_read` (`exec_thread.rs:559-585` / `replay.rs:265-274`) — done now.
  New `pub(crate) fn layered_storage_read<'a>(parent: Option<&'a PendingDelta>, snap:
  &'a impl StateDatabase) -> impl Fn(Address, B256) -> Result<U256, ExecutorError> +
  'a` in `replay.rs`; `actor/exec_boundary.rs`'s `apply_block_close_actions` and
  `replay.rs`'s `seal_block` both call it (`replay.rs` passes `None` for `parent`,
  since offline replay has no pipelined layer). This is the row the coordinator
  called consensus-critical (a diverging read order between the two changes which
  block sees an active feature) and asked to do first; done first.
- `defer` guard (`exec_thread.rs:402-411,504-511,633-645`, now `exec_records.rs`) —
  done now. `fn defer(&mut self, rec: BufferedRecord) -> Flow` pushes and returns
  `Flow::Continue`; each of the three call sites still guards on
  `self.block_exec.is_some()` first (moving `rec`'s owned fields — `envelope`, for
  example — into the `defer` call unconditionally would break the non-buffered
  path's later `&envelope` use), so the guard itself is not folded into `defer`, only
  the "push + return" half is.
- `try_handoff` (`exec_thread.rs:821-838,851-865`, now `exec_boundary.rs`) — done
  now. `fn try_handoff<T>(tx: &Sender<T>, item: T, block_number: u64, what: &str,
  dropped: &str, on_full: impl FnOnce())`; `handoff_bal`/`handoff_shadow` both call
  it, with `on_full` carrying the shadow-only dropped-block metric increment. The two
  original warn messages ("dropping this block's frame (publisher pump stalled?)" vs
  "dropping this block's capture") are preserved via the `dropped` parameter.
- `signed_legacy` (`test_support.rs:27-52`, `reader.rs:845-865`, `replay.rs:314-334`)
  — done now, the in-crate slice of this row (about 30 more copies exist across the
  workspace; that wider consolidation is out of this crate's scope). Made
  `actor::test_support::legacy` `pub(crate)` and `actor.rs`'s `mod test_support`
  `pub(crate)` under `#[cfg(test)]`. `reader/tests.rs`'s `envelope` and
  `replay.rs::tests::transfer` are now one-line wrappers that call it.
- `block_scope` (`exec_thread.rs:414-426,517-529`) — already discharged in Phase A as
  `scope_or_init`. Same shape, no further change.
- `expand_record` (`reader.rs:573-634,635-686`) and the `send_or_stop` macro
  (`reader.rs:562-571` etc.) — already discharged in Phase A as `OrderingLoop::
  send_expanded`; the repeated `if exec_out.send(..).is_err() { return Ok(()) }`
  sites mostly disappeared with it, matching the original row's own prediction.
- `record_replayed` (`replay.rs:224-230,250-256`) — already discharged in Phase A as
  the `try_fold`/`BlockAcc` rewrite (one path now, not two), then reshaped again in
  this phase's R15 pass into `BlockAcc::apply_one`.
- The four-site 12-item argument list (`actor.rs:162-175`, `exec_thread.rs:199-211,
  897-909,920-933`) — discharged by row 6's `ExecInputs`/`ExecHooks` (Phase A section
  above): `spawn_exec`/`ExecState::new` each now have one call site building one
  struct, so the duplicated list is gone.
- Five `impl Trait for Box<dyn Trait>` forwarders (`actor/ports.rs:66-70,74-82`,
  `reader.rs:97-105,107-111,381-385`) — KEEP, differs in method set, confirmed
  correct: the R6 addendum already resolved this (4 deleted as dead, 1 kept because
  the validator calls it; see the Phase A R6/"Not done, judged wrong" entries). A
  `forward_boxed!` macro would cost more lines than it saves.
- `persist.rs:34-38,88-92` `MdbxSnapshotSource::new`/`MdbxWriterSignal::new` — KEEP,
  differs in role (different traits implemented). No action needed; confirmed.
- `reader/cluster/tests.rs:44-52,113-120` `drain_tags` shape — already discharged in
  Phase A: the R10 test-loop fix there (`std::iter::from_fn(|| sub.next().ok())
  .map(label).collect()`) is the same consolidation `drain_tags` describes, under a
  different name (`label`, not a `drain_tags` function).

## Deferred to Phase B

### R13 non-zero types (4 rows deferred, constructing crate named)
- `actor/types.rs:74` `ExecutorConfig::receipt_queue_depth` — constructed with a
  literal integer field in `crates/executor/tests/{determinism,diff_reference,
  m_plus_one_join}.rs`, `crates/executor/benches/sequential_throughput.rs`,
  `crates/executor/src/bin/kardamom-executor/main.rs`, and
  `crates/validator/tests/forged_envelope_chaos.rs`. Typing this field breaks every
  one of those struct literals.
- `bin_support.rs:196` `open_tx_data_subs`'s `shards: u8` — the value is parsed by
  clap in `crates/executor/src/bin/kardamom-executor/args.rs:41` and
  `crates/validator/src/bin/kardamom-validator/args.rs:38` (also read by
  `crates/batcher`'s binary); construction happens outside this crate.
- `bin_support.rs:70` `resolve_genesis`'s `chain_id_flag` and `actor/types.rs:71`
  `ExecutorConfig::chain_id` — same reason: `--chain-id` is parsed by clap in
  `crates/executor/src/bin/kardamom-executor/args.rs:63` and
  `crates/validator/src/bin/kardamom-validator/args.rs:46`, then passed in. Also
  needs `NonZeroU64` (R13 was already deferred here for that reason in Phase A).
- `reader.rs:243` `ReaderConfig::join_poll_interval` — a `Duration`, not a
  `NonZeroUsize`, but the same cross-crate-construction problem: it is set with a
  literal `Duration::from_micros(100)` in
  `crates/executor/tests/m_plus_one_join.rs:482`.

### R14 (cross-crate row)
- `From<kardamom_state::RecoveryPoint> for ResumePoint` — both duplicate build sites
  are in the binaries, not this crate: `crates/executor/src/bin/kardamom-executor/
  state.rs:84-88` and `crates/validator/src/bin/kardamom-validator/main.rs:121-125`.
  Adding the `impl From<...>` itself would live in `actor/types.rs` (in-crate), but
  it depends on `kardamom_state::RecoveryPoint`, and its only two call sites are in
  the binaries — implementing it without also updating those two sites leaves it
  unused. Deferred alongside the binaries' own update.

## Not done, judged wrong / time-boxed

None. The R14 test-dedup rows and the fuller `ExecRig` shape that were time-boxed
out of the first Phase C pass (`boundary_msg`, `tx_msg`, `run_ordering`, `funded`,
the `commit_tests.rs` reorg, and `ExecRig` owning the writer log and the
exec-to-commit channel) were all done in the coordinator-review follow-up below,
per "scope cuts are not ours to make."

## Gates (Phase C, first pass)

`cargo test -p kardamom-engine`: 54/54 pass. `cargo clippy -p kardamom-engine
--all-targets -- -D warnings`: clean. `cargo clippy -p kardamom-engine --all-targets
-- -W clippy::pedantic`: 0 warnings in `crates/engine/src/**` (one new warning
surfaced mid-phase, `explicit_iter_loop` on `Shadow::run`'s `for block in rx.iter()`;
fixed to `for block in rx`). `cargo fmt -p kardamom-engine`: applied, stable. `cargo
check --workspace --all-targets`: clean (the coordinator's seeded `contracts/out`
artifacts resolved the `kardamom-deployer`/`kardamom-batcher` gap noted in Phase A;
no other cross-crate break from this phase's changes).

# Phase C follow-up (coordinator review)

Three items, in the order the coordinator gave them.

## 1. R14 test rows finished

Every row time-boxed out of the first Phase C pass is now done — the same pure
test-dedup, no production-code change, with the existing 54 tests as the
correctness check (all still pass; see Gates below).

- **`funded(signer, nonce) -> MockStateDatabase`** (`actor/test_support.rs`) — the
  `MockStateDatabase::builder().account(signer.address(), U256::from(10u128.pow(18)),
  nonce, KECCAK_EMPTY).build()` shape, at every one of its ~10 sites across
  `exec_tests/{streaming,bal_shadow}.rs`, `exec_resume_tests.rs`, and
  `exec_pipeline_tests.rs`.
- **`boundary_msg(block_number, end_count, l2_timestamp) -> ReaderToExec`** and
  **`tx_msg(signer, to, idx, nonce, value) -> ReaderToExec`** (`actor/test_support.rs`)
  — replace the hand-built `ReaderToExec::Boundary(BlockBoundaryStart { .. })` and
  `ReaderToExec::Tx { .. }` literals at all ~18 send sites across
  `exec_tests/{streaming,bal_shadow,block_close,interop}.rs`, `exec_pipeline_tests.rs`,
  and `exec_resume_tests.rs`. `tx_msg` covers the common case where a tx's wire
  position equals its canonical index; the two sites where they differ
  (`streaming.rs`'s `deposit_credit_is_visible_to_later_txs_in_the_block`, and the
  deposit fixture itself) keep their literal `ReaderToExec::Tx`/`::Deposit` construction.
  `l1_origin` is 0 at every current call site, folded into `boundary_msg` rather than
  taken as a parameter.
- **`run_ordering(queue, buf, cfg) -> Result<Vec<ReaderToExec>, ExecutorError>`**
  (`reader/tests.rs`) — the spawn-`VecTxOrderingSub`-join-drain shape, replacing 8 of
  the 9 `spawn_tx_ordering_reader` call sites in this file's unit tests (returns the
  reader thread's `Result`, so both the success-path tests and the one error-path
  test, `channel_b_reader_join_timeout_aborts`, use it). The 9th site,
  `channel_b_reader_tolerates_a_publisher_lag`, keeps its own shape: it pre-populates
  `buf` from a background thread and joins that thread too, which `run_ordering`'s
  signature has no room for.
- **`commit_tests.rs` reorg** — moved `fn receipt(tag, offset) -> Receipt` above the
  first test that uses it (it previously sat mid-file, used by only the later
  tests), and every inline `BPosition { term_id: 0, term_offset: N }` literal in this
  file (six of them, across three tests and `receipt` itself) now reads
  `test_support::pos(N)`. No new helper was needed; `pos` already existed.
- **`ExecRig`'s fuller shape** — `ExecRig` now owns the exec-to-commit channel
  outright: `new` builds a `bounded(RIG_COMMIT_CAPACITY)` pair internally (constant,
  128 — comfortably above the largest receipt+boundary count any current test
  produces), and `spawn` returns `(JoinHandle<...>, Receiver<ExecToCommit>)` instead
  of taking a pre-built `Sender` and returning only the handle. A new
  `ExecRig::recording(snapshots, sw_signal) -> (Self, WriterLog)` convenience
  constructor covers the common `RecordingQueue`-over-a-fresh-log case (about 14 of
  the 18 `spawn_exec` test sites); the `ApplyingRecordingQueue`-based tests in
  `block_close.rs` (2 sites) still build their own writer log by hand, since that
  queue type needs the log before the rig exists, but no longer build the
  exec-to-commit channel by hand. Every one of the 18 `spawn_exec` test call sites
  (the same set `ExecRig` already touched for row 6) was updated in this pass, since
  `spawn`'s return shape changed for all of them.

### A real bug this surfaced, and its fix

Moving channel ownership into `ExecRig` exposed a genuine regression from the
in-progress `ExecRig` work itself (not present in the original hand-written test
code, and not present before this follow-up's `spawn` redesign): `ExecRig::spawn`
initially returned only the `JoinHandle`, dropping its own `rx_e2c` field the
moment `spawn` returned. For every test that does not care about receipts and so
never asked for the receiver, that receiver — the exec-to-commit channel's only
receiving end — closed while the exec thread was still running, sometimes only
microseconds after `spawn()` returned. `ExecState::settle_ready`'s
`self.tx.send(ExecToCommit::Boundary(b))` then failed immediately with a
disconnected-channel error, which the exec loop treats as `Flow::Stop` — a clean
shutdown signal, not a panic — and the exec thread exited before submitting most or
all of its boundaries to the writer. Seven tests failed this way, each with a
different symptom depending on exactly when the race landed (`log.len()` short by
one or more, `try_recv()` on an unrelated channel returning `Disconnected`, an
index-out-of-bounds on a log entry that was never written). All seven passed
individually before this row and only failed once `spawn`'s ownership shape
changed here, so this was caught, not inherited.

The fix: `spawn` now always returns `(JoinHandle<...>, Receiver<ExecToCommit>)`,
under the rule that the receiver must come back to the caller to bind (even to an
unused, but named, `_rx_e2c`) rather than staying a field `spawn` silently drops.
A `let (h, _) = rig.spawn(rx_r2e);` destructuring pattern would reproduce the exact
same bug (a wildcard pattern drops its matched value at that statement, unlike a
named `_`-prefixed binding, which lives to the end of its scope) — call sites bind
a real name in every case. Confirmed with `cargo test -p kardamom-engine --lib`
run three times in a row (54/54 each time) after the fix, and a `--test-threads=1`
isolation run of the specific failing test before the fix reproduced it
deterministically (ruling out a parallel-test-run flake).

## 2. `commit_thread.rs` R9 fix

Per coordinator review: `publish_batch`'s `from = from.saturating_add(published)
.min(receipts.len())` clamp, and its comment, are gone.
`from = from.saturating_add(published);` is the only line now. A
`TxReceiptsPublication::publish_receipts` sink that reports it published more
receipts than it was handed is a broken trait implementation; R9 asks that this
surface, not be silently absorbed. It now does: `&receipts[from..]` panics with an
out-of-bounds slice index the next time through the loop, instead of the previous
code silently treating the over-report as "everything published." `saturating_add`
itself stays — that guards the unrelated, legitimate R12 concern (an untrusted
`usize` addition overflowing), not the broken-sink case.

## 3. `try_handoff` as an extension trait (R11, R15)

`exec_boundary.rs`'s six-parameter free function `try_handoff` is now a
`SenderHandoff<T>` extension trait implemented for `crossbeam_channel::Sender<T>`
(an extension trait because `Sender` is foreign to this crate, matching the pattern
the coordinator named). Each call site now names one `HandoffLabels { what,
dropped }` const instead of two string-literal parameters:

```rust
const BAL_HANDOFF: HandoffLabels = HandoffLabels {
    what: "BAL",
    dropped: "frame (publisher pump stalled?)",
};
const SHADOW_HANDOFF: HandoffLabels = HandoffLabels {
    what: "footprint-shadow",
    dropped: "capture",
};
```

`handoff_bal` and `handoff_shadow` now read `btx.try_handoff(item, block_number,
BAL_HANDOFF, || {})` and `stx.try_handoff(blk, block_number, SHADOW_HANDOFF, || {
metrics::counter!(..).increment(1); })`. The two original warn-log wordings
("dropping this block's frame (publisher pump stalled?)" vs. "dropping this
block's capture") are preserved exactly, via `labels.dropped`.

## Gates (Phase C follow-up, final)

`cargo test -p kardamom-engine`: 54/54 pass (run three times after the `ExecRig`
bug fix; stable). `cargo clippy -p kardamom-engine --all-targets -- -D warnings`:
clean (one intermediate `type_complexity` error on `ExecRig::recording`'s return
type, fixed with a named `WriterLog` type alias — a named type, not an `#[allow]`,
per the standing rule against guarding lints away). `cargo clippy -p kardamom-engine
--all-targets -- -W clippy::pedantic`: 0 warnings in `crates/engine/src/**`. `cargo
fmt -p kardamom-engine`: applied, stable. `cargo check --workspace --all-targets`:
clean.

# Round B

Scope: the `engine` section of `reaudit-main.md`, the R6/R7 rows the Phase B
section above deferred (the `RemoteEpochObserver`/`JoinRecovery` dyn seams),
and item 6 of the round-B task (`bin_support.rs`'s `&mut dyn FnMut` sinks and
`Box<dyn JoinRecovery>`).

## `reaudit-main.md` findings, mapped to the merged layout

- R6 `reader.rs`'s `ParentStorageReader = dyn Fn(...) -> Result<U256, String>`
  and `RemoteEpochObserver::observe(.., parent_storage: &ParentStorageReader<'_>)`
  — done. New `ParentState<'a, S>` value in
  `crates/exec-core/src/delta.rs` (`{ delta, parent, snapshot }`, with a
  `fn storage(&self, addr, slot) -> Result<U256, ExecutorError>` method that
  reads delta, then parent, then snapshot, in that order — re-exported at
  `kardamom_engine::delta::ParentState` through the crate's existing
  `pub use kardamom_exec_core::delta` re-export, so no new plumbing was
  needed for engine or validator code to see it). `RemoteEpochObserver` is
  now `RemoteEpochObserver<S: StateDatabase>`, taking `&ParentState<'_, S>`;
  the `String` error is `ExecutorError`.
- R14 `exec_thread.rs`'s inline three-tier closure (matching
  `apply_block_close_actions`'s comment about the required read order) —
  done. `actor/exec_markers.rs`'s `on_remote_epoch` now builds one
  `ParentState::new(delta, parent.as_ref(), snapshot)` and passes it to
  `observe`, instead of a hand-written closure. `apply_block_close_actions`
  (`kardamom_exec_core::features`) also now takes `parent: Option<&PendingDelta>,
  snapshot: &S` and builds its own `ParentState` internally for its two reads
  (replacing the `F: FnMut` generic parameter); its old `read` wrapper
  (delta-first, then `read_slot`) is exactly what `ParentState::storage`
  does now, so the wrapper is gone. The old `layered_storage_read` helper in
  `replay.rs` (the 2-tier parent+snapshot half) is deleted; both of its
  callers (`exec_boundary.rs`'s `apply_block_close_actions` and
  `replay.rs`'s `seal_block`) now pass `parent`/`snapshot` straight through.
- R9 `exec_thread.rs`'s `let Self { delta, parent, snapshot,
  remote_epoch_observer, .. } = self;` destructure — kept (still needed to
  split the borrow: `delta`/`parent`/`snapshot` build the `ParentState`
  immutably while `remote_epoch_observer` is borrowed mutably), but the
  closure it fed is gone.
- R1 `reader.rs`'s "(audit H9)" on the `RemoteEpochObserver` doc — done,
  along with retitling the doc's `parent_storage` reference to `parent`.
- R7 `EngineWiring`: added `type RemoteEpoch: RemoteEpochObserver<SnapshotDb<Self>>
  + 'static;`, mirroring the existing `type Epoch: EpochObserver`. `RoleHooks<W>`'s
  `remote_epoch_observer` field is now `Option<W::RemoteEpoch>` (was
  `Option<Box<dyn RemoteEpochObserver>>`). `ExecState<S, Q, P, E>` gained a 5th
  type parameter `R: RemoteEpochObserver<S::Db> + 'static` (same treatment as
  `E`), threaded through `ExecHooks`, `ExecInputs`, and every
  `impl<S, Q, P, E> ExecState<S, Q, P, E>` block (`exec_thread.rs`,
  `exec_markers.rs`, `exec_records.rs`, `exec_boundary.rs`, `exec_settle.rs`),
  plus `spawn_exec`. Added `NoRemoteEpochCheck` next to `NoEpochCheck` in
  `reader/ports.rs`, for roles/tests that trust the pair's origin sequence as
  sent. `actor/test_support.rs`'s `ExecRig<S, Q, P>` gained a defaulted 4th
  parameter `R = NoRemoteEpochCheck`; its `.remote(observer)` builder is now
  generic (`fn remote<R: RemoteEpochObserver<S::Db> + 'static>(self, observer: R)
  -> ExecRig<S, Q, P, R>`), consuming the default-typed rig and returning one
  typed for the test's own observer — a struct field can't change type
  through `&mut self`. `actor/exec_tests/interop.rs`'s two test observers
  (`RecordingRemoteObserver`, `RejectingRemoteObserver`) now
  `impl<S: StateDatabase> RemoteEpochObserver<S> for ...`, unboxed.

## Item 6: `bin_support.rs`'s `&mut dyn FnMut` sinks and `Box<dyn JoinRecovery>`

`JoinRecovery` had exactly one implementation anywhere in the workspace
(`bin_support.rs`'s `ArchiveJoinRecovery`, shared by the executor, validator,
and batcher binaries through one function, `archive_join_recovery`) and no
test double. So this is R6's "closed set" case: replaced the trait +
`Box<dyn FnOnce/JoinRecovery>` pair with two plain structs in
`reader/ports.rs`:

- `pub struct JoinRecovery { refetcher: ArchiveRefetcher, tx_data_stream_base,
  tx_deposits_stream_id }`, with inherent methods `recover_tx_data`/
  `recover_deposits` (the old trait methods' bodies, `&mut dyn FnMut(..)` ->
  `impl FnMut(..)`, since `kardamom_log::refetch::ArchiveRefetcher`'s own
  `fetch_tx_data`/`fetch_deposits` already take `impl FnMut` generically).
- `pub struct JoinRecoveryFactory { cfg: RefetchConfig, tx_data_stream_base,
  tx_deposits_stream_id }` with `pub fn build(self) -> JoinRecovery`. Kept
  the name `JoinRecoveryFactory` (the validator binary's
  `wiring/startup.rs:282` names the type but never constructs one; it only
  forwards `bin_support::archive_join_recovery`'s return value, so this is a
  zero-external-edit change).

`bin_support.rs::archive_join_recovery` now returns
`Some(JoinRecoveryFactory { cfg, tx_data_stream_base, tx_deposits_stream_id })`
directly; the `ArchiveJoinRecovery` struct and its `impl JoinRecovery` are
deleted. `reader/threads.rs:354`: `recovery_factory.and_then(|f| f())` ->
`recovery_factory.map(JoinRecoveryFactory::build)` (behavior-preserving: the
inner `Option` the old closure returned was always `Some` at the one call
site, so `build`'s unconditional return changes nothing observable).
`reader/threads.rs`/`reader/join.rs`'s `Option<Box<dyn JoinRecovery>>` fields
are now `Option<JoinRecovery>`. `reader/join.rs`'s one call site
(`r.recover_tx_data(.., &mut |loc, env| {..})`) needed explicit closure
parameter types (`loc: TxDataLoc, env: TxEnvelope`), since a generic
`impl FnMut` parameter (unlike the old `&mut dyn FnMut` with its named
concrete type) gives type inference nothing to anchor the closure's
parameters to.

No cross-crate call sites: `archive_join_recovery`'s callers (executor,
validator, batcher binaries) all pass its `Option<JoinRecoveryFactory>`
straight through without constructing or inspecting it.

## Coordinator follow-up 1: `actor/ports.rs` `Box<dyn TxReceiptsPublication>`

Done. R6 has no external-caller exemption, so the forwarding impl for the
boxed trait object is gone. Added `pub enum Either<A, B> { Left(A), Right(B) }`
to `actor/ports.rs`, with `impl<A: TxReceiptsPublication, B: TxReceiptsPublication>
TxReceiptsPublication for Either<A, B>` (a `match self { Left(a) => a.., Right(b)
=> b.. }` forward on both trait methods). This is a generic, reusable "pick one
of two concrete shapes at runtime, no allocation, no vtable" primitive; nesting
it once per optional decorator layer composes to any depth. Also fixed
`TxReceiptsPublication::publish_receipts`'s default body (a `for` loop with an
`if let Err` inside) to a `find_map` (see the R16 section below), and updated
`wiring.rs`'s module doc and its `impl EngineWiring for ValidatorWiring` example
to show `Either` instead of `Box<dyn ...>`. Re-exported `Either` from `actor.rs`
and `lib.rs`, alongside `EngineWiring`.

Not applied (not owned by this group): the validator binary's
`crates/validator/src/bin/kardamom-validator/wiring/run.rs` still boxes. Exact
replacement, since `AttestingReceiptSink<P>` and `ExtractingReceiptSink<P>` are
already generic over their inner sink (confirmed by reading
`crates/validator/src/attester/sinks.rs` and `crates/validator/src/interop/sink.rs`):

```rust
use kardamom_engine::{
    EngineWiring, Either, Executor, ExecutorError, Inbound, Outbound, RoleHooks,
};
// (drop `TxReceiptsPublication` from this import list; nothing in this file
// names the trait directly once `Box<dyn ...>` is gone)

/// The attester tee, chosen at runtime by whether an attester is wired.
type TeeSink = Either<
    kardamom_validator::attester::AttestingReceiptSink<kardamom_validator::ValidatorReceiptSink>,
    kardamom_validator::ValidatorReceiptSink,
>;
/// The outbox-extraction wrapper, chosen at runtime by whether interop
/// serving is wired. `Attested::build_sink`'s two `Option`s each become one
/// `Either` layer.
type ValidatorTxReceipts = Either<
    kardamom_validator::interop::ExtractingReceiptSink<TeeSink>,
    TeeSink,
>;

impl Attested {
    pub(crate) fn build_sink(self) -> Ready {
        let streams = &self.written.streamed.streams;
        let sink = kardamom_validator::ValidatorReceiptSink::new(
            streams.receipts.clone(),
            streams.divergence.clone(),
        )
        .with_flight(streams.flight.clone());
        let tee: TeeSink = match self.attester_handle.as_ref() {
            Some(h) => Either::Left(kardamom_validator::attester::AttestingReceiptSink::new(
                sink, h.clone(),
            )),
            None => Either::Right(sink),
        };
        let tx_receipts_pub: ValidatorTxReceipts = match streams.interop_serve.as_ref() {
            Some(s) => Either::Left(kardamom_validator::interop::ExtractingReceiptSink::new(
                tee,
                self.written.streamed.opened.state.chain_id,
                s.claims.clone(),
                s.store.clone(),
                streams.divergence.clone(),
            )),
            None => Either::Right(tee),
        };
        // (the commit-poller spawn below this is unchanged)
        Ready { attested: self, tx_receipts_pub }
    }
}

pub(crate) struct Ready {
    attested: Attested,
    tx_receipts_pub: ValidatorTxReceipts, // was Box<dyn TxReceiptsPublication>
}

impl EngineWiring for ValidatorWiring {
    // ...unchanged...
    type TxReceipts = ValidatorTxReceipts; // was Box<dyn TxReceiptsPublication>
    // ...unchanged...
}
```

## R16, extended (nesting between a loop and a branch, in either direction)

Swept every function touched or read in this crate for a `for`/`while`/`loop`
with an `if`/`else`/`match` directly in its body, and for an `if`/`else`/`match`
arm whose body directly contains a `for`/`while`/`loop`. Fixed each with either
an iterator chain or a named helper (the loop's/branch's body becomes one call),
per the rule's own text. Every site:

- `actor/ports.rs` `TxReceiptsPublication::publish_receipts`'s default body:
  `for (i, r) in ... { if let Err(e) = ... { return (i, Some(e)) } }` ->
  `receipts.iter().enumerate().find_map(...)`.
- `actor/exec_thread.rs` `ExecState::run`: the main dispatch loop had a `match`
  on `recv_next()` and a second `match` on the message, both directly in the
  `loop`. Split into `recv_and_dispatch` (one step) and `dispatch` (the message
  match); `run` is now `while let Flow::Continue = self.recv_and_dispatch()? {}`.
- `actor/exec_boundary.rs` `run_block_exec`: `for r in out.receipts { push; if
  send(...).is_err() { return Stop } }` -> `out.receipts.into_iter().any(...)`.
- `actor/exec_settle.rs` `settle_ready`: a `while let Some(...) = pop_front_if(..)
  { ...; if send(...).is_err() { return Stop } }` -> extracted
  `forward_settled_boundary` (returns `bool`), loop condition carries a `stopped`
  flag (`while !stopped && let Some(..) = ...`).
- `actor/exec_settle.rs` `on_closed`: `if let Some(...) = self.inflight.back() {
  if wait_committed(..).is_ok() { for (b,_) in self.inflight.drain(..) {..} } }`
  (a loop inside a branch inside a branch) -> `let-else` guard, then
  `flush_inflight_boundaries()`.
- `actor/commit_thread.rs` `CommitLoop::collect_batch`: `while ... { match
  try_recv() {..} }` -> extracted `try_drain_one` (returns whether to keep
  draining, and sets a `closed` out-param).
- `actor/commit_thread.rs` `publish_batch`: `while from < len { ...; if let
  Some(e) = err {..} }` -> extracted `publish_batch_step`.
- `actor/commit_thread.rs` `CommitLoop::run`: `loop { let-else; ...; if let
  Some(b) = ... {..}; if batch.closed { return } }` -> extracted
  `publish_one_batch` (returns whether the batch closes the stream); `run` is
  now `while !closed && let Some(batch) = self.collect_batch() { closed = ... }`.
- `actor/test_support.rs` `drain_commits`: `while let Ok(m) = recv() { match m
  {..} }` -> extracted `push_commit`.
- `actor/exec_pipeline_tests.rs` (test): the deadline-poll loop in
  `exec_settles_inflight_commits_while_idle` had a `match` directly inside;
  extracted `poll_one_boundary`. Left the loop's shape and timing exactly as
  the earlier coordinator review required (see the Phase C follow-up note on
  this same test) — only the match moved out, nothing about polling changed.
- `reader/threads.rs` `send_expanded`: `for item in items { ...; if let
  Flow::Stop = ... { return Stop } }` -> `items.into_iter().any(...)`.
- `reader/cluster/mod.rs` `ClusterTxOrderingSubscription::next`: `loop { if let
  Some(out) = try_deliver()? { return } let-else; match decode(..) {..} }` (two
  branches directly in the loop) -> extracted `ingest_one_frame`; `next`'s loop
  is now one `let-else` guard plus a `return`.
- `bin_support.rs`: no R16 site found (this file's one remaining issue was the
  unrelated `kardamom-log` API-shape break below).

`crates/validator/src/parallel/**` and `witness.rs` (owned by this group too):
- `parallel/engine.rs` `build_seed`: the address loop had a `continue` plus
  two `match`es; extracted `seed_one_account`. The storage loop's `if let
  Some(v) = storage_seed(..) { insert }` became `.filter_map(..)`.
- `parallel/claims.rs` `first_mismatch`: `for (k,v) in claimed { match
  computed.get(k) {..return..} }` -> `claimed.iter().find_map(..)`, matching
  the sibling `first_unclaimed`'s existing `.find().map()` shape.
- `parallel/engine_tests/fixtures.rs` `seq_capture` (test): `for (i, rec) in
  ... { ...; if assert_status {..} }` -> extracted a `SeqCaptureAcc` struct
  (delta, bal, cumulative) with an `apply_record` method, since the loop
  needed 4 mutated/read values threaded through and a bare extracted function
  would have needed 8 parameters (over the R11 bound).
- `witness.rs` `anchor_block_witness`: the fixed-point re-walk `loop { match
  verify(..).and_then(..) { Ok(..) => return, Err(MissingNode{..}) => { if
  !add_missing_target(..) { return Err(..) } }, Err(e) => return Err(..) } }`
  -> extracted `try_anchor_round` (returns `Result<Option<B256>, EngineError>`,
  `None` means retry); the loop is now a `let-else` guard plus a `return`.

Reviewed and left alone (matched a search pattern but is not a real nesting,
or is a different, pre-existing rule not asked for in this pass):
- `stm`'s own sweep is reported separately (forked to keep this report's size
  down); see the stm status doc's Round B section.
- `crates/validator/src/parallel/engine_tests/parity.rs`: two sites with a
  `for` inside a `for`, but neither loop has a branch inside — a plain nested
  loop (the *original* R16 clause, not this pass's extended one) in test
  assertion code (`for w in out.receipts.windows(2) { assert!(..) }` nested
  under a `for batch_size in [...] {..}`). Not fixed: out of scope for this
  specific follow-up, flagged for whoever next touches R16's original clause
  in this file.
- `actor.rs`'s `.fold(None, |acc, d| match acc {..})` and similar
  iterator-adapter closures containing a `match`/`if`: not a loop keyword, so
  not in scope; this is the iterator-chain style the rule itself recommends.

## Unrelated compile break fixed along the way: `bin_support.rs`'s `LiveTxDataSub`

`kardamom_log::aeron_live::AeronRuntime::open_tx_data_subscription` changed its
return type from a raw `tokio::sync::mpsc::UnboundedReceiver` to a
`TxDataSubscription` wrapper (that crate's own concurrent work this round,
unrelated to this group's task). This broke `LiveTxDataSub`, which is in this
crate. Fixed by holding the wrapper directly and driving its `PollRecv` impl
by hand with a park/unpark waker (`blocking_recv`, mirroring the same pattern
`kardamom_log::refetch`'s own `recv_timeout` already uses internally, just
with no timeout) since the new wrapper exposes only an async `recv` and a
non-blocking `try_recv`, and this reader thread runs off the tokio runtime.
One casualty: the wrapper no longer exposes a queue-depth read, so the
`TX_DATA_QUEUE_DEPTH` gauge this function used to update is gone until
`kardamom_log` adds one back; noted in a comment at the call site, not
silently dropped.

## Cross-file items for other groups (exact lines)

- `crates/validator/tests/forged_envelope_chaos.rs` (not owned by this
  group; not under `crates/validator/src/parallel` or `witness.rs`): its
  `TestWiring`'s `impl EngineWiring for TestWiring` (line 92) is missing the
  new `type RemoteEpoch` item, so `cargo check -p kardamom-validator
  --all-targets` fails to compile this one integration test binary (the lib
  and the binary both compile clean; this is the only break found). Fix:
  add `NoRemoteEpochCheck` to the `use kardamom_engine::{...}` import list
  (line 41-43) and add `type RemoteEpoch = NoRemoteEpochCheck;` after
  `type Epoch = NoEpochCheck;` (line 99).
- `crates/executor/**` (not edited by anyone this round): every
  `impl EngineWiring` there (the binary's `wiring.rs`, and
  `tests/{determinism,diff_reference,replay_integration,m_plus_one_join}.rs`,
  `benches/sequential_throughput.rs`) needs `type RemoteEpoch =
  kardamom_engine::reader::NoRemoteEpochCheck;` added, since the executor
  role wires nothing here. Not checked (no `kardamom-executor` build was run
  this round; `kardamom-validator`'s and `kardamom-batcher`'s `main.rs`
  passing `remote_epoch_observer: None`/no reference to the type at all
  needed zero changes, confirmed by grep — only the `type` item itself is
  missing wherever `impl EngineWiring` is written out).
- `crates/validator/src/interop/extract.rs`, `crates/validator/src/bin/kardamom-validator/wiring/run.rs`
  (`RemoteEpochVerifier`, `type RemoteEpoch = kardamom_validator::interop::RemoteEpochVerifier;`,
  `remote_epoch_observer: Option<RemoteEpochVerifier>`): already updated to
  the new `RemoteEpochObserver<S>`/`ParentState<'_, S>` shape by the time
  this round's `cargo check -p kardamom-validator` ran — concurrent work by
  the interop/group-A agents, not this group. No action needed.

## Gates

- `cargo clippy -p kardamom-engine --all-features --no-deps --lib --tests --
  -D warnings -W clippy::pedantic -D unreachable_pub`: clean.
- `cargo test -p kardamom-engine --lib`: 54/54 pass.
- `cargo fmt -p kardamom-engine -- --check`: clean.
- Forbidden-pattern grep over `crates/engine`, test dirs excluded: clean
  except `actor/wiring.rs`/`actor/ports.rs`'s three doc-comment mentions of
  `Box<dyn TxReceiptsPublication>` (prose, not code; the one code site is
  the reported not-done item above).
- `cargo check -p kardamom-validator --all-features --all-targets`: one
  failure (`forged_envelope_chaos.rs`, reported above); lib, binary, and
  every other test target compile clean. `cargo test -p kardamom-validator
  --lib`: 90/90 pass.

## Round B follow-up 2 (coordinator's two post-round briefs)

Two coordinator briefs landed after Round B closed:
`followup-roundb-B.md` and `followup-roundb-B-2.md` (an Opus reviewer's
line-by-line pass over the whole diff, `review-roundb-B.md`). This section's
work closes the reviewer's two hard-rule findings against this crate (R6,
R7) plus the smaller R8/R9/R11/R13/R16 findings and most of the unstarted
brief items.

### R7: `ExecPorts` supertrait, five type parameters collapsed to one

`ExecState<S, Q, P, E, R>`, `ExecInputs<S, Q, P, E, R>`, and
`spawn_exec<S, Q, P, E, R>` each took five separate type parameters, with
every `impl` block in `exec_thread.rs`, `exec_markers.rs`, `exec_records.rs`,
`exec_boundary.rs`, and `exec_settle.rs` repeating all five bounds. Added an
`ExecPorts` supertrait on `EngineWiring` (`actor/wiring.rs`) and changed
every one of those types/fns/impls to `<W: ExecPorts>`. Done first, before
R6, since R6 adds one more associated type to `EngineWiring` that R7's
collapse absorbs for free instead of becoming a sixth loose parameter.

### R6: `BlockExec<D>` (`Box<dyn Fn>`) closed over a trait

`actor/types.rs`'s `BlockExec<D>` was `Box<dyn Fn(&D, ...) -> Result<...> +
Send>` — the one `Box<dyn` the style grep's single-line pattern missed,
because rustfmt splits `Box<` and `dyn Fn(` across two lines. Added
`BlockExecStrategy<D>` (one method, the closure's old signature),
`NoBlockExec` (a zero-sized default, mirroring `NoEpochCheck`/
`NoRemoteEpochCheck` in `reader/ports.rs`), `type BlockExec:
BlockExecStrategy<SnapshotDb<Self>>` on `EngineWiring`, and
`RoleHooks.block_exec: Option<W::BlockExec>`. Also added `impl<A:
BlockExecStrategy<D>, B: BlockExecStrategy<D>, D> BlockExecStrategy<D> for
Either<A, B>` in `actor/ports.rs`, since the validator's `wiring/run.rs`
picks between two live, non-`None` strategies at runtime (the parallel
verifier and the prover's sequential path) — a single associated type can't
name either alone, and `Either` already exists in this crate for exactly
this shape (see the `TxReceiptsPublication` coordinator-follow-up round).

Also fixed `scripts/style-check.sh`'s forbidden-pattern grep (repo root, not
under any one crate) to catch the wrapped form: it now greps `Box<$` with
`-A1` and checks the next line starts with `dyn `, in addition to the
existing single-line `Box<dyn` pattern. Verified: `grep -rnE --include='*.rs'
-A1 -e 'Box<$' crates/engine crates/exec-core crates/stm
crates/validator/src/parallel crates/validator/src/witness.rs | grep -E -B1
'^[^:]+-[0-9]+-\s*dyn '` returns nothing across all five owned directories.

Out-of-crate replacements (not this group's files — routed, not applied):
- `crates/executor/src/parallel.rs`: `stm_block_exec` becomes a
  `StmBlockExec<S>` struct implementing `BlockExecStrategy<S>` (full
  replacement code is in the sub-agent's report, forwarded to the
  coordinator); `crates/executor/src/bin/kardamom-executor/wiring.rs` adds
  `type BlockExec = kardamom_executor::parallel::StmBlockExec<StateSnapshot>;`.
- `crates/validator/src/parallel/engine.rs`'s `parallel_block_exec` becomes
  a `ParallelBlockExec` struct implementing `BlockExecStrategy<D>` (this
  file is under `parallel/**`, owned by a different group this round, not
  this crate); `crates/validator/src/prover.rs`'s sequential path becomes a
  `SequentialBlockExec` struct the same way;
  `crates/validator/src/bin/kardamom-validator/wiring/run.rs`'s
  `type BlockExec = kardamom_engine::Either<ParallelBlockExec,
  SequentialBlockExec>;`. Full struct code in the sub-agent's report.

### R8, R9, R11, R13, R16 — smaller findings, all closed

- **R13**: `reader/join.rs`'s `dedup_window: NonZeroUsize::new(1 <<
  20).expect(..)` ran at `Default::default()` time — lifted to a `const
  DEFAULT_DEDUP_WINDOW: NonZeroUsize`, the same shape `exec_settle.rs`'s
  `COMMIT_PIPELINE_DEPTH` already used.
- **R16, four sites**: `reader/join.rs`'s `JoinWait::run` (four `if`s in the
  loop body) and `wait_for` (two `if`s) each became one `poll_once`/
  `wait_step` helper returning a `JoinStep { GiveUp, Take(env), Retry }`
  enum, with a dispatching `match` outside the loop and the loop itself
  reduced to a call plus dispatch. `reader/threads.rs`'s `tx_ordering`
  reader loop (two `match`es plus a trailing `if let Flow::Stop`, with
  multi-statement arms) became `OrderingLoop::step(..) -> Result<Flow,
  ExecutorError>`, with the loop as `while let Flow::Continue =
  state.step(..)? {}`; the `tx_data` reader loop got the same treatment
  (`tx_data_step`).
- **R9**: `actor/exec_markers.rs`'s `let Self { delta, parent, snapshot,
  remote_epoch_observer, .. } = self;` destructure is gone. The disjoint
  borrow compiles as written: `if let Some(obs) =
  self.remote_epoch_observer.as_mut() { let parent_state =
  ParentState::new(&self.delta, self.parent.as_ref(), &self.snapshot);
  obs.observe(record, &parent_state)?; }` — confirmed by reading the file
  and by the clean `cargo check`/`clippy` gate below. The reaudit's original
  claim (NLL accepts disjoint field borrows through `self`) was correct;
  the earlier "kept, still needed to split the borrow" status-doc claim in
  this file's Phase-C section was wrong. `exec_boundary.rs`'s similar-looking
  destructure is unchanged — it is genuinely needed there (`delta` goes in
  as `&mut`).
- **R8**: `reader/ports.rs`'s `JoinRecoveryFactory`'s three `pub` fields are
  now `pub(crate)` (its one constructor, `bin_support::archive_join_recovery`,
  is in-crate). Stray finding while here: `JoinRecovery::recover_deposits`
  has zero callers anywhere in the workspace — `pub`, so `unreachable_pub`
  does not fire, but dead by R8's own rule. Not removed (may be a reserved
  seam for a future deposit-refetch path); flagged for the coordinator to
  rule on.
- **R11 (`BalHandoff` tuple)**: `actor/types.rs`'s `pub type BalHandoff =
  (BlockBoundary, kardamom_types::BlockDelta, revm::state::bal::Bal);`
  became `struct BalHandoff { boundary, delta, bal }`. `exec_boundary.rs`'s
  construction updated. Out-of-crate: `crates/executor/src/bal.rs:129`'s
  `let (boundary, delta, bal) = match rx.recv_timeout(..)` becomes `let
  BalHandoff { boundary, delta, bal } = match rx.recv_timeout(..)` (import
  already present) — routed, not applied.
- **`JoinRecovery` typed error**: `recover_tx_data`/`recover_deposits`
  returned `Result<u64, String>`; now a typed `JoinRecoveryError` in
  `reader/ports.rs`/`reader/mod.rs`, the same shape `ParentStorageReader`
  already used.
- **`actor/exec_records.rs`'s `Option::insert`**: `Ok(scope.as_mut().expect
  ("scope just initialized"))` following a separate `if scope.is_none() {..}`
  check became `match scope { Some(s) => Ok(s), None =>
  Ok(scope.insert(sc)) }` — no `.expect`.

### Brief-2 unstarted items 19-24: done, routed, or explicitly deferred

- **Item 19** (typed config fields): `ExecutorConfig.chain_id: NonZeroU64`,
  `receipt_queue_depth: NonZeroUsize`, and `bin_support`'s `shards:
  NonZeroU8` are done in-crate. Eight out-of-crate struct literals found
  (six in `crates/executor/tests/*.rs` + one bench, one in
  `crates/validator/tests/forged_envelope_chaos.rs`), plus two
  `chain_id`-shorthand sites in the two binaries' `main.rs`/`wiring/run.rs`
  that need a `NonZeroU64::new(chain_id).expect(..)` — flagged with a caveat:
  that `.expect` is sound only if a bare `--chain-id 0` is already rejected,
  which `resolve_genesis` does not currently check (it only rejects a
  mismatch against a loaded genesis file, not zero outright). Exact
  replacement lines are in the sub-agent's report, routed to the coordinator
  and group A (executor/validator owners).
- **Item 20** (`ShardId`/`SessionId`): not done. `TxRef`'s wire fields in
  `kardamom-types` are plain `u8`/`i32`, so a local `crates/engine`-only
  newtype would still need `.0` unwraps at the actual wire boundary
  (`recover_tx_data(shard_id: u8, session_id: i32, ..)`) and would convert
  nothing there — the newtype belongs in `kardamom-types` next to `TxRef`,
  outside this group's ownership this round. The stale "Phase B... once
  those exist" comment in `reader/join.rs` is reworded to state only the
  current fact, no phase/future reference.
- **Item 21** (`From<RecoveryPoint>`): done, in-crate (`actor/types.rs`;
  `crates/engine` already depends on `kardamom-state`). Two out-of-crate
  hand-built copies this removes: `crates/executor/src/bin/kardamom-executor/state.rs`
  and `crates/validator/src/bin/kardamom-validator/wiring/startup.rs` —
  routed, not applied.
- **Item 22** (`WorkerPool::new(&[usize])`): `crates/stm/**`, not this
  crate's file — no `crates/engine/**` call site constructs a `WorkerPool`
  directly, so nothing to do here; the stm sub-agent owns this item.
- **Item 23** (`ExecutorError` variants belonging in exec-core): report
  only, unchanged from the Phase B row this file already carried —
  `ExecutorError` is defined in `crates/exec-core/src/error.rs`, outside
  this crate. The named variants: `LegacyDepositRef` (`reader.rs:700`),
  `ReplayBufferOverflow` (`reader/cluster/mod.rs:225`), and a
  `BoundaryMisaligned` split across two distinct call sites sharing one
  variant. Still true; routed to whoever owns exec-core's error module.
- **Item 24** (`test-support` cargo feature): not done. Grepped the whole
  workspace for `ChannelHarness`, `signed_legacy`, `open_inbound`,
  `await_engine_or_shutdown`, `restore_if_fresh`: none exist anywhere. They
  are `dry-validator.md`'s proposed new APIs, not renamed existing code.
  Building them means writing new surface matched against test/bench files
  in `crates/executor/**` and `crates/validator/**` this group cannot
  verify compile without touching those crates. Left undone rather than
  ship something unverified, given the size of the rest of this round's
  mandatory (hard-rule) items; flagged for a fresh pass reading
  `dry-validator.md`'s R14 rows 13-14 (production code) and 2-3 (tests).

### Gates (Round B follow-up 2)

- `cargo clippy -p kardamom-engine --all-targets --all-features -- -D
  warnings -W clippy::pedantic -D unreachable_pub`: clean (the `kardamom-state`
  `unreachable_pub` blocker this round's earlier gates worked around with
  `--no-deps` is gone — fixed upstream by the group that owns it).
- `cargo test -p kardamom-engine --all-features`: 54 passed, 1 doctest
  ignored (the `EngineWiring` example, `ignore`-annotated as before), 0
  failed.
- `cargo fmt -p kardamom-engine -- --check`: clean.
- Forbidden-pattern grep (both the single-line and the wrapped-`Box<`
  form): empty.
- `cargo check -p kardamom-validator --all-features --all-targets`: blocked
  only by `kardamom-stm`'s own in-progress work this round (a `JobWait`
  shape mismatch from the concurrent stm sub-agent, not introduced by this
  section's changes; `kardamom-engine` itself compiles fine as a
  dependency in that build). `cargo check -p kardamom-executor
  --all-features --all-targets`: blocked only by the out-of-crate callers
  listed above (items 2/8/19/21), which this group does not own.

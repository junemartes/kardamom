# Status: batcher group (batcher, da_watcher, deployer)

Phase A. Every row from `crate-batcher.md` and `inputs-batcher.md` appears in
exactly one list below.

All three gates pass for all three crates (`kardamom-batcher`,
`kardamom-da-watcher`, `kardamom-deployer`): default clippy (`-D warnings`),
`clippy::pedantic` (zero warnings in this group's own files), `cargo test`,
and `cargo fmt`. See the reply for the exact commands and counts.

## Note on `mechanical-lists.md`

Its "R2 per crate" and "R2 test functions over 50 code lines" rows for this
group are a stricter accounting (threshold 50, or 100 for the pedantic
`too_many_lines` default) than `crate-batcher.md`'s own tables; every
production row it names is the same row already listed under R2 above. Its
test-function rows are covered here: the five that exceed the pedantic
default 100-line threshold (`optimistic_e2e.rs:78`,
`optimistic_proof_e2e.rs:92`, `proof_submission_e2e.rs:57`,
`anvil_e2e.rs:157`, `section6_conformance.rs:197`) got the
`#[allow(clippy::too_many_lines)]` treatment under R2/R11 above. The rest
(`docker_e2e.rs:61` `write_synthetic_archives` 104 lines — gated behind the
`docker-e2e` feature, so outside the `--all-targets` pedantic gate;
`deploy_e2e.rs:97` `multi_l2_deploy_and_atomic_upgrade` 94 lines, already
`#[ignore]`d as flaky; `section6_conformance.rs:60` `write_archives` 84
lines; `anvil_e2e.rs:62` `deploy_settlement_and_post_batch_emits_event` 71
lines; `multi_archive_reader.rs:69,155` 67/56 lines;
`docker_e2e.rs:179` 53 lines) are test-fixture builders and scenarios under
the pedantic default threshold and not in this pass's curated R2 scope; not
done, judged consistent with the same-shaped helpers left alone elsewhere
in this file. Its "R3 files over 500 lines" row for
`da_watcher/src/interop/watcher.rs` and its "R11 functions with more than 7
arguments" rows for `deployer/src/main.rs:251` and
`batcher/src/prover_submit.rs:24` duplicate rows already listed under R3
and R2/R7 above. Its per-file pedantic-warning counts for
`multi_archive_reader.rs` and `live.rs` are covered by the R11 section
below.

## Done

### R1 comments (production)

- R1, `da_watcher/src/rpc_source.rs:9`: deleted "Ported from" line.
- R1, `da_watcher/src/bin/kardamom-da-watcher.rs:459,500`: both back-pressure
  comments now name the `BACK_PRESSURED` token, present tense, no duplication
  of "used to" prose.
- R1, `da_watcher/src/interop/feed.rs:1`: module deleted (see R8).
- R1, `da_watcher/src/lib.rs:35`: rewrote the spec-inheritance section in
  present tense; dropped the "new-architecture port" history and the
  `docs/agents/l1-deposit-monitor-spec.md` reference.
- R1, `da_watcher/src/lib.rs:44`: deleted "a separate follow-up".
- R1, `da_watcher/src/watcher.rs:181`: dropped the "old per-deposit" compare;
  kept the invariant only.
- R1, `da_watcher/src/interop/mod.rs:23`: "spec §10 tier T0" → states the
  feed-trust rule directly.
- R1, `da_watcher/src/interop/mod.rs:2`: deleted the
  `docs/specs/interop-outbox-messaging-spec.md` reference (confirmed
  nonexistent).
- R1, `batcher/src/l1.rs:17`: dropped the dead `crate::reexec` link; names
  `kardamom-reconstruct` in prose instead.
- R1, `batcher/src/frame.rs:15`: "No chain is in production..." → "Only
  version 2 is accepted."
- R1, `batcher/src/frame.rs:3`: "at this stage" removed.
- R1, `batcher/src/settlement.rs:25`: removed "In v0" framing; states the
  current behavior only.
- R1, `batcher/src/settlement.rs:38`: deleted the "future task" plan note
  (item itself stays; deletion is R8, deferred).
- R1, `batcher/src/batch.rs:4`: "Today, the" removed.
- R1, `batcher/src/archive_reader.rs:3`: "Today, the" removed.
- R1, `batcher/src/archive_reader.rs:178`: rewrote "Tests and the future
  writer-side adapter" as "This crate's tests and the writer-side adapter".
- R1, `batcher/src/batcher.rs:10`: "a single-instance design for v1" → "a
  single-instance design", present tense throughout.
- R1, `batcher/src/multi_archive_reader.rs:12`: deleted "In v0" and "a future
  scale-up".
- R1, `batcher/src/multi_archive_reader.rs:76`: deleted "In v0" and the
  segment-roll deferral plan note.
- R1, `batcher/src/optimistic.rs:204`: deleted "In v0"; states the rule
  (rewritten during the R2 helper extraction).
- R1, `batcher/src/live.rs:11,13`: dropped both `docs/agents/*.md`
  references (during the R3 split into `live/`).
- R1, `batcher/src/live.rs:440`: "post_confirmed stamps..." → backtick'd,
  present-tense (now in `live/feed.rs`).

### R1 comments (tests)

- R1, `batcher/tests/section6_conformance.rs:14`: rewrote to state what the
  test does today (stub versioned hashes, not sidecar broadcasting, which is
  no longer a future task).
- R1, `batcher/tests/docker_e2e.rs:24`: kept, updated wording (the item,
  `PostBatchParams`, was not deleted — R8 deferred).
- R1, `batcher/tests/docker_e2e.rs:26-30`: rewrote to state what the
  synthetic-archive path does and does not exercise, present tense, no plan
  note.
- R1, `batcher/tests/recon_roundtrip.rs:97`: "spec §16 Q8" → states the rule.

### R2 long methods / argument groups

- R2, `da_watcher/src/bin/kardamom-da-watcher.rs:227` (`main`, 160 lines):
  split into `serve`, `open_publishers`, `start_deposits_recorder`,
  `spawn_watchers`, `await_shutdown_or_fail_stop`. Also fixes R4 (below).
- R2, `batcher/src/optimistic.rs:138` (`watch_and_challenge`, 115 lines):
  split into `claimed_arrays_from_log`, `first_divergent_offset` (also R10),
  `read_block_proof`.
- R2, `da_watcher/src/interop/watcher.rs:178` (`spawn`, 103 lines): split
  into `persist_cursor`, `handle_outcome`, `record_tick`.
- R2, `batcher/src/live.rs:477` (`run`, 103 lines, now `live/run.rs:85`):
  split into `start_l1_side`, `resolve_run_config`.
- R2, `batcher/src/live.rs:254` (`post_confirmed`, 88 lines, now
  `live/sender.rs`): split into `reconcile_after_error`,
  `record_post_metrics`.
- R7/R11, `crates/deployer/src/main.rs:250` (`run_deploy`, 10 args): grouped
  into `DeployArgs` with nested `OracleArgs`; dropped the
  `#[allow(clippy::too_many_arguments)]`.
- R7/R11, `crates/deployer/src/spec.rs:137`
  (`encode_proof_oracle_init_args`, 7 args): grouped into `ProofOracleInit`;
  updated the three batcher e2e test call sites that construct it.
- R7, `crates/batcher/src/prover_submit.rs:12` (module-wide
  `#![allow(clippy::too_many_arguments)]`): narrowed to the `sol!`
  invocation only (moved inside the macro's attribute list, since a
  standalone attribute on the macro call does not propagate to the
  generated item).

### R3 large files

- R3, `da_watcher/src/interop/watcher.rs` (515 → ~180 code lines): moved
  `mod tests` to `da_watcher/tests/interop_watcher.rs` as an integration
  test against the public API (`testing`/`test-support` features, wired via
  a self dev-dependency in `da_watcher/Cargo.toml`, matching the
  `cluster-adapter`/`sequencer` pattern already in the workspace). Logic
  kept as one file, unsplit, per the appendix.
- R3, `batcher/src/live.rs` (486 → split): split into `live/cursor.rs`,
  `live/sender.rs`, `live/feed.rs`, `live/run.rs`, with `live.rs` as the
  thin module-declaration file plus `live_metric_names`. Every external
  public path preserved via re-exports (`BatchCursor`, `read_last_batch_index`,
  `LiveSender`, `LiveArgs`, `run`, `connect_l1`).
- R3, `da_watcher/src/watcher.rs` (423, KEEP the logic): moved `mod tests`
  to `da_watcher/tests/l1_watcher.rs` as an integration test. Logic
  unchanged (156 code lines, one enum, one pass, one loop).
- R3, `deployer/src/deployer.rs` (361, KEEP): no action; matches the
  appendix's KEEP verdict.

### R3 large files (tests)

- R3, `batcher/tests/optimistic_e2e.rs` (293, KEEP): no action.

### R4 manual drops

- R4, `da_watcher/src/bin/kardamom-da-watcher.rs:434` (`drop(aeron_rt)`):
  redundant drop deleted; `aeron_rt` now lives in `serve()` and drops at
  scope end (part of the R2 split above).
- R4, `da_watcher/src/interop/watcher.rs:372` (`drop(shutdown)`, test):
  kept, per the appendix's own "keep" option (the sender must stay alive
  across the preceding await).
- R4, `da_watcher/src/interop/watcher.rs:649` (`drop(source)`, test):
  wrapped the "first life" in a block, so the scope end is the crash;
  explicit `drop` removed (now in `da_watcher/tests/interop_watcher.rs`).
- R4, `batcher/tests/docker_e2e.rs:247` (`drop(cluster)`): deleted; it was
  the last statement before the function's implicit end.
- R4, `batcher/tests/metrics_endpoint.rs:43` (`drop(l)`): `free_port`
  rewritten as one expression with no named binding to drop.
- R4, `da_watcher/tests/metrics_endpoint.rs:34` (`drop(l)`): same fix (the
  two `free_port` copies stay duplicated; see R3 tests, deferred).

### R5 sync primitives and channels

- R5, `da_watcher/src/watcher.rs:208,209` (`Arc::new(publisher)`,
  `Arc::new(source)`, REPLACE_WITH_OWNERSHIP): both moved into the
  `async move` block by value, matching `interop::watcher::spawn`.
- R5, `da_watcher/src/watcher.rs:264` (`impl EpochPublisher for Arc<P>`,
  UNNECESSARY): deleted along with the two `Arc`s above.
- R5, `da_watcher/src/watcher.rs:43` (`shutdown: oneshot::Sender<()>`,
  JUSTIFIED): kept, no change.
- R5, `da_watcher/src/watcher.rs:210` (`oneshot::channel`, JUSTIFIED): kept.
- R5, `da_watcher/src/interop/watcher.rs:188` (`oneshot::channel`,
  JUSTIFIED): kept.
- R5, `batcher/src/live.rs:542` (now `live/run.rs`, `mpsc::channel(1<<14)`,
  JUSTIFIED): kept.
- R5, `batcher/src/live.rs:34` (`Receiver` import, JUSTIFIED): kept.
- R5, `da_watcher/src/bin/kardamom-da-watcher.rs:280`
  (`CancellationToken::new()`, JUSTIFIED): kept.
- R5, `da_watcher/src/bin/kardamom-da-watcher.rs:285` (`oneshot::channel`,
  JUSTIFIED): kept.
- R5, `da_watcher/src/interop/mock.rs:58,60,62,63,66,68,70` (script,
  emitted_lagged, swallow, next_lagged_id, subscribed_dests, items,
  close_epoch; JUSTIFIED, some with a fold-into-one-Mutex suggestion): left
  alone, per "leave JUSTIFIED sites alone." The fold-into-`FeedScript`
  refinement is a same-crate, low-risk cleanup not attempted this pass.
- R5, `da_watcher/src/source.rs:102-105` (test fake locks, JUSTIFIED, fold
  suggested): left alone, same reason.
- R5, `da_watcher/src/publisher.rs:50-51` (test fake locks, JUSTIFIED): left
  alone.
- R5, `da_watcher/src/interop/publisher.rs:56-61` (test fake locks,
  JUSTIFIED but wrong granularity): left alone; the `seen`/`published`
  two-lock refinement not attempted this pass.

### R6 dynamic dispatch

- R6, `deployer/src/main.rs:218` (`Deployer<DynProvider>`): split `deployer()`
  into `signed_deployer(rpc_url, owner, key) -> (Deployer<impl Provider>, Address)`
  and `readonly_deployer(rpc_url, owner) -> Deployer<impl Provider>`.
  `print_addresses` made generic over `P: Provider + Clone`. `DynProvider`
  import removed.

### R7 too many generics

- R7, `batcher/src/archive_reader.rs:69,95,169`: added `ArchivedRecord`
  supertrait (using associated-type-bound syntax,
  `rkyv::Archive<Archived: ...>`, since a plain `where Self::Archived: ...`
  clause on the trait is not implied at usage sites); `TypedSegmentReader`,
  its `Iterator` impl, and `access_owned` now read `T: ArchivedRecord`.
- R7, `batcher/src/archive_reader.rs:183`: added a companion `SegmentRecord`
  trait with a blanket impl; `append_frame<T: SegmentRecord>`.
- (module-wide allow and the two argument-group structs are listed under R2
  above.)

### R8 unnecessary pub

- R8, `batcher/src/live.rs:349,365,50,124,161,193,58` (`FeedConfig`,
  `run_feed`, `BatcherFileConfig`, `L1Truth`, `read_l1_truth`, `reconcile`,
  `live_metric_names`): all narrowed to `pub(crate)` as part of the R3
  split into `live/`.
- R8, `deployer/src/addresses.rs:45,39,29,34,75,87` (`proxy_full_initcode`,
  `factory_init_data`, `factory_impl_salt`, `factory_proxy_salt`,
  `app_impl_address`, `app_proxy_address`): all narrowed to `pub(crate)`;
  confirmed via workspace grep that only `deployer.rs` (same crate) used
  them.
- R8, `da_watcher/src/interop/mock.rs:113,176` (`origin_chain_id`,
  `subscribed_dests`): both deleted; confirmed via workspace grep no caller
  anywhere in `crates/` (the underlying fields stay, still read internally).
- R8, `da_watcher/src/interop/feed.rs` (whole module, and its re-export at
  `interop/mod.rs`): deleted. Confirmed via workspace grep (including
  `validator` and `e2e`) that nothing imports
  `kardamom_da_watcher::interop::feed::*` or the re-exported names at
  `interop::*`; `source.rs` and `mock.rs` now import `kardamom_interop_feed`
  directly. Verified `kardamom-validator` and `e2e` (including
  `--features full-pipeline-e2e`) still compile.
- R8, "the three binary crates are clean" (batcher/da_watcher/deployer
  `src/bin`): confirmed still true; no `pub` item added to any binary by
  this pass's refactors.

### R10 imperative style

- R10, `batcher/src/optimistic.rs:219`: `first_divergent_offset` (see R2)
  uses `.find().map()`.
- R10, `batcher/src/rereplicate.rs:154`: rewritten as
  `read_dir(..)?.collect::<io::Result<Vec<_>>>()?.into_iter().filter(..).count()`
  (preserves the original's I/O-error propagation, unlike the appendix's
  `filter_map(Result::ok)` suggestion, which would silently swallow a
  `DirEntry` read error).
- R10, `batcher/src/blob.rs:51`: `prefixed.chunks(..).map(encode_one_blob).collect()`.
- R10, `batcher/src/blob.rs:91`: `chunk.chunks(..).enumerate().for_each(..)`.
- R10, `batcher/src/blob.rs:109`: `raw.chunks_exact(..).flat_map(..).copied().collect()`.
- R10, `batcher/src/l1.rs:137` (`read_posted_batches`):
  `logs.iter().map(..).collect::<Result<Vec<_>, _>>()`.
- R10, `deployer/src/main.rs:338` (Upgrade ops): `ids.iter().flat_map(..).collect()`.
- R10, `deployer/src/main.rs:271` (Deploy ops loop): kept as a loop, per the
  appendix's own instruction (the body `bail!`s and calls a fallible
  helper).
- R10, `deployer/src/deployer.rs:384` (`dedup_impl_specs`): no change
  needed — the current code has no empty `else` branch to drop (both arms
  do real work); the loop is correctly kept, matching the appendix's KEEP
  verdict.
- R10, `deployer/build.rs:170` (`hex_decode`):
  `s.as_bytes().chunks(2).map(..).collect::<Result<Vec<u8>>>()`.

### R10 imperative style (tests)

- R10, `batcher/tests/section6_conformance.rs:317`: `found` flag loop →
  `.iter().find(..).expect(..)`, with the assertions moved out of the loop
  body (also fixes the `topic0`/`topics` similar-names pedantic warning).
- R10, `batcher/tests/anvil_e2e.rs:121`: same `.find()` rewrite.

### R11 clippy pedantic (282 sites from `inputs-batcher.md`)

All 282 sites are fixed. Every file below shows zero `clippy::pedantic`
warnings on a fresh, isolated `cargo clippy -p <crate> --all-targets --
-W clippy::pedantic` (each crate checked alone, to rule out build-cache
cross-talk with the other ten parallel agents). Fixes applied, per site: a
backtick around an identifier (`doc_markdown`), a `# Errors` or `# Panics`
section naming the real conditions (`missing_errors_doc`,
`missing_panics_doc`), `#[must_use]` where ignoring the value is a bug,
`try_from`/a wider type/a documented `#[allow]` naming the bound for cast
lints, `let`...`else`, a redundant-closure or redundant-continue removal, a
match-arm merge, a similar-name rename, an `Option<&T>` fix, an
`assigning_clones` fix, or (for `da_watcher/src/interop/mock.rs:113,176` and
the whole of `da_watcher/src/interop/feed.rs`) deletion as dead code — see
R8/R1 above for those two. Original `file:line` citations (lines have since
moved from the refactors above; each file's current pedantic count is zero):

- `crates/batcher/benches/pack_throughput.rs`: 19 (1 sites)
- `crates/batcher/build.rs`: 40 (1 sites)
- `crates/batcher/src/archive_reader.rs`: 4,10,12,28,50,61,62,77,90,161,162,165,181,182,183,194 (16 sites)
- `crates/batcher/src/batch.rs`: 5,45,65,78 (4 sites)
- `crates/batcher/src/batcher.rs`: 69,115,135,156,157 (5 sites)
- `crates/batcher/src/bin/kardamom-batch-claimer.rs`: 51,57 (2 sites)
- `crates/batcher/src/bin/kardamom-batch-watcher.rs`: 61 (1 sites)
- `crates/batcher/src/bin/kardamom-batcher.rs`: 4,12,13,41,47,49,53,101,109,114,118,119,230 (13 sites)
- `crates/batcher/src/blob.rs`: 27,34,64,90 (4 sites)
- `crates/batcher/src/compress.rs`: 9,13 (2 sites)
- `crates/batcher/src/da_store.rs`: 31,43,56,62,63,72,89 (7 sites)
- `crates/batcher/src/frame.rs`: 8,106,247 (3 sites)
- `crates/batcher/src/l1.rs`: 52,70,123,156,187 (5 sites)
- `crates/batcher/src/lib.rs`: 13,16,22 (3 sites)
- `crates/batcher/src/live.rs`: 2,7,90,98,110,132,143,161,193,254,334,335,365,389,410,472,473,477 (18 sites)
- `crates/batcher/src/multi_archive_reader.rs`: 3,4,6,16,43,44,48,51,72,74,90,114,120,132,145,146,151,152,207,231,233 (21 sites)
- `crates/batcher/src/optimistic.rs`: 47,59,138,177,234 (5 sites)
- `crates/batcher/src/prover_submit.rs`: 48,77 (2 sites)
- `crates/batcher/src/recon.rs`: 21 (1 sites)
- `crates/batcher/src/rereplicate.rs`: 64,78,141,182,187,193,205,256,269 (9 sites)
- `crates/batcher/src/settlement.rs`: 30,53 (2 sites)
- `crates/batcher/tests/anvil_e2e.rs`: 1,63,127,157,165 (5 sites)
- `crates/batcher/tests/archive_reader.rs`: 105 (1 sites)
- `crates/batcher/tests/blob_packing.rs`: 54 (1 sites)
- `crates/batcher/tests/compress_roundtrip.rs`: 7 (1 sites)
- `crates/batcher/tests/docker_e2e.rs`: 16,18,26,61 (4 sites)
- `crates/batcher/tests/metrics_endpoint.rs`: 16 (1 sites)
- `crates/batcher/tests/multi_archive_reader.rs`: 1,12,142,144 (4 sites)
- `crates/batcher/tests/optimistic_e2e.rs`: 43,51,53,59,78 (5 sites)
- `crates/batcher/tests/optimistic_proof_e2e.rs`: 45,54,55,92 (4 sites)
- `crates/batcher/tests/proof_submission_e2e.rs`: 42,50,52,57 (4 sites)
- `crates/batcher/tests/recon_proptest.rs`: 23,26,27,28 (4 sites)
- `crates/batcher/tests/recon_roundtrip.rs`: 23,27,28,35,84,101,161 (7 sites)
- `crates/batcher/tests/section6_conformance.rs`: 5,6,67,75,197,294,322 (7 sites)
- `crates/da_watcher/src/bin/kardamom-da-watcher.rs`: 107,227,401 (3 sites)
- `crates/da_watcher/src/interop/cursor.rs`: 68,75,99 (3 sites)
- `crates/da_watcher/src/interop/mock.rs`: 86,113,118,125,143,156,171,176 (8 sites)
- `crates/da_watcher/src/interop/publisher.rs`: 35,45,66,71,90,97 (6 sites)
- `crates/da_watcher/src/interop/source.rs`: 141,320,337 (3 sites)
- `crates/da_watcher/src/interop/watcher.rs`: 122,313,337,562,565 (5 sites)
- `crates/da_watcher/src/lib.rs`: 32 (1 sites)
- `crates/da_watcher/src/publisher.rs`: 36,43,63,78 (4 sites)
- `crates/da_watcher/src/source.rs`: 69,89,111,112,117,126,130 (7 sites)
- `crates/da_watcher/src/watcher.rs`: 112,135,160,242 (4 sites)
- `crates/deployer/build.rs`: 134,135,198 (3 sites)
- `crates/deployer/src/addresses.rs`: 20,29,34,39 (4 sites)
- `crates/deployer/src/deployer.rs`: 175,224,257,274,308 (5 sites)
- `crates/deployer/src/embedded.rs`: 17,21,23,28,33,38,44 (7 sites)
- `crates/deployer/src/ids.rs`: 42,52,70,85,92,98,106 (7 sites)
- `crates/deployer/src/main.rs`: 42,51,55,56,65,66,93,149,341 (9 sites)
- `crates/deployer/src/spec.rs`: 51,58,117,124,129,138 (6 sites)
- `crates/deployer/tests/deploy_e2e.rs`: 24,59,66,98 (4 sites)
- `crates/deployer/tests/factory_address_sync.rs`: 4 (1 sites)

## Deferred to Phase B

(none — every in-scope row this pass touched is same-crate; no signature or
visibility change here is consumed by another crate group)

## Not done, judged wrong

### R2 (51-100 line functions not in this pass's curated list)

- R2, `deployer/src/main.rs:122` (`main`, 84 lines): not selected for this
  pass; a sequential CLI-command dispatch with no repeated shape.
- R2, `batcher/src/bin/kardamom-batcher.rs:177` (`main`, 84 lines): not
  selected for this pass; same shape.
- R2, `da_watcher/src/watcher.rs:95` (`process_once`, 76 lines): not
  selected; R3's own verdict on this file is "KEEP the logic: one error
  enum, one pass, one loop" — splitting it into the appendix's proposed
  helpers would fragment a single linear polling pass that R3 explicitly
  said to leave alone.
- R2, `da_watcher/src/bin/kardamom-da-watcher.rs:144` (`resolve_paths`, 72
  lines): not selected; two independent, already-factored match arms.
- R2, `da_watcher/src/interop/source.rs:234` (`next_batch`, 75 lines): not
  selected for this pass.
- R2, `batcher/src/bin/kardamom-archive-rereplicate.rs:66` (`main`, 76
  lines): not selected for this pass.
- R2, `batcher/src/frame.rs:247` (`decode`, 51 lines): not selected; at the
  threshold, one linear decode pass.
- R2 (mechanical-lists.md test rows), `batcher/tests/docker_e2e.rs:61`
  (`write_synthetic_archives`, 104 lines): not selected; gated behind the
  `docker-e2e` feature (outside the `--all-targets` pedantic gate), and the
  same shape as `write_archives` below.
- R2 (mechanical-lists.md test rows), `deployer/tests/deploy_e2e.rs:97`
  (`multi_l2_deploy_and_atomic_upgrade`, 94 lines): not selected; already
  `#[ignore]`d as a known anvil flake.
- R2 (mechanical-lists.md test rows), `batcher/tests/section6_conformance.rs:60`
  (`write_archives`, 84 lines), `batcher/tests/anvil_e2e.rs:62`
  (`deploy_settlement_and_post_batch_emits_event`, 71 lines),
  `batcher/tests/multi_archive_reader.rs:69,155` (67/56 lines),
  `batcher/tests/docker_e2e.rs:179` (53 lines): not selected; test-fixture
  builders and scenarios under the pedantic default 100-line threshold, not
  in this pass's curated R2 scope.
- R2, `batcher/src/prover_submit.rs:48` (`submit_next_proof`, 52 lines): not
  selected for this pass.

### R3 (files/dedup not in this pass's curated list)

- R3, `da_watcher/src/bin/kardamom-da-watcher.rs` (move
  `LiveTxDepositsPublisher`/`LiveRemoteEpochsPublisher` into the library
  behind an `aeron` feature): not selected; this pass's R3 scope for this
  file was the R2 `main` split and the R4 drop fix only. Moving the two
  impls into the library would make `kardamom-log`'s `aeron_live` types a
  library-visible (not binary-only) dependency, a larger change than a
  same-pass style fix.
- R3, `batcher/tests/section6_conformance.rs` / `docker_e2e.rs` /
  `multi_archive_reader.rs` (shared archive-building test helpers): not
  done; the duplicated helpers still exist in each file. Consolidating them
  into `tests/common/archive.rs` is a cross-file test refactor not
  attempted this pass.
- R3, `batcher/tests/metrics_endpoint.rs` + `da_watcher/tests/metrics_endpoint.rs`
  (shared `scrape`/`free_port` helper crate): not done; both `drop()` sites
  were fixed independently (see R4) but the duplication itself remains.

### R5 (fold-into-one-lock refinements)

(listed under Done above as "left alone" — JUSTIFIED verdicts, no separate
entry needed here.)

### R9 (not in this pass's curated scope)

- R9, `da_watcher/src/rpc_source.rs:146,189` (duplicate `topic0` check):
  not done.
- R9, `batcher/src/rereplicate.rs:269` (`SegmentName` newtype): the
  case-sensitive-extension pedantic lint at this site was fixed (R11), but
  the parse-once `SegmentName` newtype itself was not introduced.
- R9, `deployer/src/main.rs:153` (minter/chain-id pairing): NOT done
  deliberately. The appendix's suggested fix — zip `l2_chain_ids` and
  `l2_minters` into `Vec<(u64, Address)>` at the boundary — would silently
  produce zero `Op::Deploy` entries whenever `l2_minters` is empty (the
  legitimate case when deploying only `WithdrawalOutputOracle`, which needs
  no minter), because the current code deliberately iterates
  `l2_chain_ids` and only indexes `l2_minters` inside the arms that need
  it. Zipping would break that path. Left as the original indexed loop.
- R9, `deployer/src/main.rs:414` (attester/challenger clap group): not
  done.
- R9, `da_watcher/src/bin/kardamom-da-watcher.rs:147` (`--lockbox` as typed
  `Address`): not done.
- R9, `batcher/src/settlement.rs:62,67,70` (`PostBatchParams` checks): not
  done; tied to the R8 deletion of `PostBatchParams`, deferred.

### R10 (functional rewrites judged unsafe or out of scope)

- R10, `batcher/src/l1.rs:160` (`recover_blocks`): a `try_fold`/`map`
  rewrite was implemented, then reverted after it caused a real stack
  overflow in `corrupted_da_blob_is_rejected_against_its_commitment`
  (`crates/batcher/tests/recon_roundtrip.rs`) under the default test-thread
  stack. `verify_blob_against_hash`'s KZG check already uses most of the
  default stack; the extra frames an iterator/closure chain adds here are
  enough to overflow it. Kept as the original nested loop, with a comment
  recording why.
- R10, `da_watcher/src/watcher.rs:130` (`by_block` grouping): kept as the
  3-line loop, per the appendix's own caveat ("a small gain; keep the loop
  if the fold reads worse") — judged the loop reads at least as well as the
  `fold` form here.
- R10, `deployer/src/deployer.rs:274,282` (`l2ChainIdAt` loops): not done.
  The appendix's own note says the win here is concurrency
  (`futures::future::try_join_all`), not style — out of scope for a
  style-only pass, and it would change the observable RPC concurrency and
  timing, not just the code shape.
- R10, `deployer/build.rs:192` / `batcher/build.rs:34` (`walk_sol_files`
  dedup): not done. The two copies are byte-identical, but sharing them
  needs either a new internal crate or a path dependency between the two
  build scripts — a larger change than a same-file style fix.

### R7 (out of curated scope)

- R7, `batcher/src/settlement.rs:53` (`PostBatchParams::new`, 7 args): not
  done; tied to the R8 deletion of `PostBatchParams` (test-only, deferred).

### R8 (out of curated scope: not `live.rs`/`addresses.rs`/binaries)

- R8, `batcher/src/multi_archive_reader.rs:233,151` (`load_a_index`,
  `a_archive_len`): not done.
- R8, `batcher/src/settlement.rs:30,42` (`versioned_hashes_from_commitments`,
  `PostBatchParams`): not done; test-only, R1 comments updated in place
  instead of deleting.
- R8, `batcher/src/da_store.rs:62,72` (`len`, `is_empty`): not done.
- R8, `deployer/src/spec.rs:108,75,10` (`encode_init_calldata`,
  `build_spec`, `DeploymentSpec`): not done.
- R8, `deployer/src/ids.rs:34` (`ALL`): not done.

## Coordinator follow-up: FIX list, REVERSED R2, and Phase C

Everything in the coordinator's follow-up review is complete: the 7-item FIX
list, the 3 REVERSED R2 splits, and Phase C in the requested order
R13 → R12 → R15 → R16 → R14. All four gates (see the counts at the end of
this section) pass clean for all three crates after every change below.

### FIX list (7/7)

1. `batcher/src/blob.rs`: both `debug_assert!` calls deleted (the grep-banned
   pattern).
2. `batcher/src/live/feed.rs` (`post_group`): the `.expect("pending
   blocks")` panic path removed; the pending-group invariant is now upheld
   by construction instead of asserted at the call site.
3. `deployer/src/main.rs`: `start_l1_side`'s 5-tuple return replaced with a
   named `L1Side` struct; `resolve_run_config`'s 3-tuple replaced with a
   named `RunConfig` struct (`batcher/src/live/run.rs`); `run`'s 105 lines
   split with no `#[allow(too_many_lines)]` left in place.
4. (grouped with 3 above — the struct extraction and the split were one
   change.)
5. `batcher/src/rereplicate.rs`: the `.ends_with(".rec")` check restored to
   an exact byte comparison, with a documented
   `#[allow(clippy::case_sensitive_file_extension_comparisons)]` explaining
   why a case-insensitive match would be wrong here (this crate only ever
   writes lowercase `.rec` segment names itself).
6. `#[allow(clippy::too_many_lines)]` removed from all five e2e test files
   (`anvil_e2e.rs`, `optimistic_e2e.rs`, `optimistic_proof_e2e.rs`,
   `proof_submission_e2e.rs`, `section6_conformance.rs`); each split into
   setup/act/assert helpers (`Scenario` structs where the fixture has
   real state), with shared anvil/dev-account/tx-builder scaffolding moved
   into `batcher/src/testkit.rs` (new, behind `feature = "test-support"`,
   mirroring `deployer/src/testkit.rs`'s existing pattern).
7. `deployer/src/deployer.rs` / `deployer/src/main.rs`: `run_deploy`'s
   nested nine-line loop extracted into `DeployArgs::ops_for` (a method
   returning `Vec<Result<Op>>`, driven with `.flat_map(..).collect()` at
   the call site); the unchecked `l2_minters[i]` indexing replaced with
   `.get(i).context("--l2-minter count must match --l2-chain-id...")?`.

### REVERSED R2 (3/3 split; 3 confirmed no-action)

- `batcher/src/bin/kardamom-batcher.rs::main` (84 lines): split into
  `scan_offline_archives(cli) -> Batcher<MockSender>` and
  `post_or_dry_run(cli, sent_batches)`.
- `da_watcher/src/watcher.rs::process_once` (76 lines): split into
  `read_tick_range<S: L1Source>` (new `TickRange` struct) and
  `publish_one_epoch<S, P>` (new `PublishStep` enum); `process_once` is now
  ~16 lines calling both. All 11 `l1_watcher.rs` behavioral tests confirmed
  unchanged.
- `batcher/src/bin/kardamom-archive-rereplicate.rs::main` (76 lines): split
  into `run_diff(cli)`, `run_heal(cli)`, `run_mirror(cli)`.
- No action, per the coordinator's explicit reversal to KEEP:
  `deployer/src/main.rs::main` (84 lines),
  `da_watcher/src/bin/kardamom-da-watcher.rs::resolve_paths` (72 lines),
  `da_watcher/src/interop/source.rs::next_batch` (75 lines).

### Accepted with no action

`deployer::ProofOracleInit` arg struct, the `signed_deployer`/
`readonly_deployer` split, and deployer's `let`...`else` test rewrites —
all already in place from Phase A; the coordinator's review confirmed them
as-is.

### R13 NonZero types (done)

Every wire/CLI-facing "never legitimately zero" value now carries that
invariant at the type level, parsed once at the CLI or constructor
boundary, with no `.max(1)`/`.max(2)` fixup left anywhere in the group:

- `batcher/src/batcher.rs`: `BatcherConfig.blocks_per_batch` → `NonZeroUsize`
  (`Default` uses `NonZeroUsize::MIN`).
- `batcher/src/live/feed.rs`: `FeedConfig.blocks_per_batch` → `NonZeroUsize`.
- `batcher/src/live/run.rs`: `LiveArgs.shards` → `NonZeroU8`,
  `.blocks_per_batch` → `NonZeroUsize`, `.flush_ms` → `NonZeroU64`.
- `batcher/src/bin/kardamom-batcher.rs`: `Cli.blocks_per_batch`,
  `.shards`, `.flush_ms` → the same three `NonZero*` types (clap parses
  each directly via its `FromStr`/`Display` impls).
- `batcher/src/rereplicate.rs`: `read_stable`/`read_stable_with`'s
  `attempts: usize` (previously clamped with `.max(2)`) → `NonZeroUsize`;
  `STABLE_READ_ATTEMPTS: NonZeroUsize = NonZeroUsize::new(10).unwrap()`; the
  `.max(2)` clamp deleted, loop is now `for _ in 1..attempts.get()`.
- `da_watcher/src/bin/kardamom-da-watcher.rs`: `Args.poll_interval_secs`
  (**DEFECT**: 0 reaches `tokio::time::interval`, which panics on a zero
  period) → `NonZeroU64`; `.interop_peer_chain_id`, `.self_chain_id` →
  `Option<NonZeroU64>`; `.interop_retry_interval_secs` → `NonZeroU64`.
- `da_watcher/src/interop/source.rs`: `WsRemoteChainSource
  .max_reconnect_attempts` → `NonZeroU32`; a module-level
  `DEFAULT_MAX_RECONNECT_ATTEMPTS` constant (avoids a runtime `.expect()`
  inside `new()` that would trip `missing_panics_doc`).
- `deployer/src/spec.rs` / `deployer/src/testkit.rs`: `ProofOracleInit
  .challenge_window_secs` / `OracleInitArgs.challenge_window_secs`
  (**DEFECT**: a zero challenge window finalizes an optimistic claim
  instantly, with no dispute period at all) → `NonZeroU64`. Verified by
  reading `contracts/src/L1/KardamomProofOracle.sol` directly:
  `initialize` does not itself reject 0.
- `deployer/src/main.rs`: `Cli::Deploy`/`Cli::Upgrade.l2_chain_ids` →
  `Vec<NonZeroU64>` (chain id 0 would otherwise produce a valid-looking but
  wrong CREATE2 salt); `Cli::Deploy.finalization_window` →
  `NonZeroU64`.

Both defects (the poll-interval panic and the instant-finalization window)
are real, pre-existing bugs this pass fixed, not just style. All call
sites `.get()` across into APIs owned by other groups (`kardamom_engine`'s
`u8` shard count, `Duration::from_millis`/`from_secs`) that were not
converted to `NonZero*` themselves, since those crates are out of scope
here.

### R12 safe arithmetic (done, FIX-tagged sites)

Every FIX-tagged site now uses `checked_*` with a typed error for a
wire/counter/L1-sourced value, `saturating_*` only for a report field (with
a comment distinguishing it from a control value), or `try_from` for a
narrowing cast — no bare `+`/`-`/`as` left on any of these paths. New shared
helpers, so the fix is one function instead of four inline `checked_add`
calls each:

- `batcher/src/frame.rs`: `Reader::capacity_hint(n: u32, min_elem_bytes:
  usize) -> usize` caps a wire-provided count's `Vec::with_capacity` hint
  against the buffer's actual remaining bytes, so a corrupt/adversarial
  `u32` count can no longer request a huge allocation before the real
  bounds-checked read would catch a short buffer. Used at all 4 call sites
  (`blocks`, `remote_epochs` ×2, `txs`).
- `batcher/src/live/sender.rs`: `LiveSender::next_index(&self) ->
  Result<u64>` (`self.prev_index.checked_add(1)`), used in both
  `post_confirmed` and `reconcile_after_error` instead of a bare `+ 1`
  computed twice; `attempt` now `saturating_add`s (a retry counter, not a
  control value).
- `deployer/src/deployer.rs`: `factory_count_u64(count: U256, field: &str)
  -> Result<u64, DeployError>` replaces `U256::to::<u64>()`'s panic-on-
  overflow with a typed error; used for both `l2ChainIdCount` and
  `idCount`.
- `batcher/src/optimistic.rs`: `spool_sequences`'s capacity-hint underflow
  fixed (`end.checked_sub(start).and_then(checked_add(1))
  .and_then(try_from).unwrap_or(0)` — `end < start` is a malformed
  settlement entry; the loop itself is already safe since `start..=end` is
  empty in that case, only the old `Vec::with_capacity` hint could
  underflow to a near-`u64::MAX` allocation request); `claim_next_batch`'s
  `highestClaimedBatch + 1`, `watch_and_challenge`'s
  `last_finalized + 1` and `l2BlockStart + block_offset`, all now
  `checked_add` with a typed `BatcherError::L1`.
- `batcher/src/l1.rs`: `post_batch`'s `prev_batch_index + 1` →
  `checked_add`.
- `batcher/src/prover_submit.rs`: `submit_next_proof`'s `last_finalized + 1`
  → `checked_add`.
- `batcher/src/rereplicate.rs`: `report.bytes_copied += bytes` (×2, in
  `mirror_archive` and `heal_from_mirror`) → `saturating_add`, each with a
  "report field, not a control value" comment.
- `da_watcher/src/interop/watcher.rs`: `*cursor = last_seq + 1` →
  `checked_add`, with a new `InteropError::CursorOverflow` variant and a
  matching `handle_outcome` arm that fail-stops the pair (see R15 below —
  now `InteropLoop::handle_outcome`).
- `deployer/src/main.rs`: `UpgradeArgs::ops_for`'s version bump
  (`current.version + 1`) → `checked_add` with
  `.context("registry version overflowed u64")`.

One documented deviation from a strict typed-error reading:
`da_watcher/src/interop/source.rs::absorb` advances its cursor with
`.and_then(|s| s.checked_add(1))`, falling back to `None` on overflow
rather than returning a typed error. `absorb`'s signature is
`Option<Vec<OutboxMessage>>`, not a `Result`, and the `None` path is
already the existing "caller must resubscribe" path an actually-rewound
caller takes — so the fallback lands on infrastructure that already
exists, at a call site (`seq == u64::MAX`) that is unreachable with a real
peer feed. Changing `absorb`'s signature to thread a `Result` through was
judged more churn than this unreachable case warrants; the choice is
commented at the site.

The remaining sites from `arith-batcher.md`/`dry-batcher.md` were
correctly tagged HOT_PATH_KEEP (4: per-tx/per-block counters on the
happy path, where a checked add would cost a branch per iteration with no
observable safety gain — array lengths already bound the values) or
PROVEN (12: the appendix's own proof that the value cannot overflow in
context, for example a loop bound already `<= len()`); both categories are
left unchanged, as instructed.

### R15 methods not functions (done)

- `deployer/src/spec.rs`: `encode_proof_oracle_init_args(args)` →
  `ProofOracleInit::encode(self)`. Confirmed via
  `grep -rn encode_proof_oracle_init_args crates/` that the only caller was
  `deployer/src/testkit.rs`; the free-function re-export dropped from
  `deployer/src/lib.rs`.
- `deployer/src/main.rs`: `run_deploy(args)` → `DeployArgs::run(self)`;
  `run_upgrade(rpc_url, private_key, owner, args)` →
  `UpgradeArgs::run(self, rpc_url, private_key, owner)` (kept asymmetric on
  purpose: `Upgrade`'s own flags carry no rpc/key/owner state, unlike
  `Deploy`'s); `oracle_init_args(&args.oracle)` →
  `OracleArgs::init_args(&self)`.
- `batcher/src/optimistic.rs`: `claimed_arrays_from_log` and
  `read_block_proof` (both take real state: provider, oracle address,
  spool) → methods on a new `ClaimWatch<'_, P>` struct. `first_divergent_
  offset` stays a free function — it takes two slices and no state, so
  putting it on a struct only because the audit table named it would be a
  worse design than the current pure function; noted at the site.
- `da_watcher/src/interop/watcher.rs`: `persist_cursor` and
  `handle_outcome` (both use the cursor file, origin, and retry-interval
  state) → methods on a new `InteropLoop` struct. `record_tick` stays a
  free function for the same reason as `first_divergent_offset` above — a
  pure two-string metrics helper with no loop state.
- `da_watcher/src/bin/kardamom-da-watcher.rs`: `open_publishers` and
  `start_deposits_recorder` (both read the Aeron runtime/channels/config)
  → methods on a new `DaWatcherService` struct. `spawn_watchers` and
  `await_shutdown_or_fail_stop` stay free functions — neither touches the
  service's state, only the watcher handles and paths passed to them;
  noted at the site.
- Confirmed fine as-is, no action (per the coordinator): `FeedLoop`
  (`batcher/src/live/feed.rs`, already existed from the R2/FIX-list split),
  `LiveSender`, and `deployer/build.rs`.

### R16 no nested loops (done)

Beyond the nested loops already removed incidentally by the FIX list and
R15 work (`run_deploy`'s minter loop, `section6_conformance.rs`'s tx-order
check), a final sweep found and fixed:

- `deployer/src/deployer.rs::addresses`: the `for l2 in l2s { for i in
  0..count { .. } }` double loop extracted into a new
  `Deployer::entries_for_l2(&self, l2) -> Result<Vec<RegistryEntry>>`
  method; `addresses` now iterates only its outer `l2s` loop.
- `batcher/tests/docker_e2e.rs`: the inner `for (i, tx) in
  reconstructed[0].txs.iter().enumerate() { assert_eq!(..) }` (nested
  inside the outer `for rec in reader` / `match`) replaced with a
  `.map().collect()` plus one `assert_eq!` on the whole vector.
- `batcher/tests/blob_proptest.rs::high_byte_always_zero`: the
  `for blob in &blobs { for chunk in .. }` double loop flattened to one
  `for chunk in blobs.iter().flat_map(..)`.

Left unchanged, both already justified in-place:
- `batcher/src/l1.rs::recover_blocks`'s `for d in descriptors { for vh in
  &d.versioned_hashes { .. } }`: already carries a comment (from Phase A's
  R10 pass) explaining that an iterator/closure rewrite here previously
  caused a real stack overflow under `verify_blob_against_hash`'s KZG
  check on the default test-thread stack. Kept as a loop deliberately.
- `batcher/src/frame.rs`'s `encode`/`decode` (`for block in &payload.blocks
  { .. for rec in &block.remote_epochs { .. } .. for tx in &block.txs {
  .. } }`): inherent to the wire format's own shape (blocks containing
  remote-epoch and tx sub-collections); `decode`'s mirror-image nesting
  already existed pre-audit. Not a repeated/duplicated iteration pattern,
  so no helper extraction attempted.

### R14 dedup (done, one item flagged as a cross-crate dependency)

- `batcher/tests/section6_conformance.rs` and `batcher/tests/docker_e2e.rs`
  each had their own near-identical M+1 archive fixture builder
  (`write_m_plus_one_archives` / `write_synthetic_archives`, same shape at
  different sizes: 2 sequencers, N txs each, one boundary). Consolidated
  into `batcher::testkit::write_m_plus_one_archives(dir, txs_per_sequencer,
  block_number, l2_timestamp) -> (PathBuf, HashMap<u8, PathBuf>, Vec<u64>)`
  — the returned `Vec<u64>` is the canonical correlation-id order, so each
  caller asserts reconstruction against it instead of hardcoding the
  fixture's own id scheme. `testkit`'s `#[cfg(..)]` gate extended to
  include `feature = "docker-e2e"` so `docker_e2e.rs` can reach it without
  also needing `test-support`. Both the anvil-backed
  `section6_conformance_m_plus_one_to_l1_and_back` test and the Docker-backed
  `aeron_cluster_starts_and_batcher_round_trips_the_m_plus_one_topology`
  (`--ignored`, run against a real Docker daemon) pass against the shared
  helper.
- `fn pos` (×6 across `archive_reader.rs`, `recon_roundtrip.rs`,
  `recon_proptest.rs`, `docker_e2e.rs`, and two spots in
  `multi_archive_reader.rs`) replaced with the existing
  `BPosition::from_index`; `multi_archive_reader.rs` additionally got new
  `tx_ref_at`/`boundary_at` fixture-builder helpers, which also fixed a
  pedantic `too_many_lines` regression the rename introduced.
- `deployer/src/testkit.rs`'s `anvil_with_erc7955`/`deploy_settlement`/
  `deploy_settlement_and_oracle` and `batcher/src/testkit.rs`'s
  `AcceptingVerifier`/dev consts/`env_tx`/`advance_past_window`: both new
  testkit modules exist specifically to fold near-duplicate e2e setup
  scaffolding out of every test file that needed it (part of FIX item 6
  above; listed again here since it is also an R14 win).
- **Not done — flagged as a cross-crate dependency, not mine to fix**:
  `batcher/tests/metrics_endpoint.rs` and
  `da_watcher/tests/metrics_endpoint.rs` each still carry an identical
  12-line `free_port`/`scrape` pair. The real fix is a shared
  `kardamom_obs::testkit` (or similar) module in `kardamom-obs`, which
  both crates already depend on for `kardamom_obs::init`; `kardamom-obs`
  is owned by a different group in this audit, so adding to it here would
  step outside this pass's ownership boundary and risk colliding with
  that group's concurrent work. Recommend the obs-crate owner add the
  shared helper and have both call sites here switch to it in a follow-up.

### Other deviations flagged for the coordinator

- `batcher/src/prover_submit.rs`: the `sol!`-generated `IKardamomProofOracle
  ::initialize` genuinely takes 8 arguments (fixed by the v2 oracle's ABI
  in `contracts/`); removing
  `#[allow(clippy::too_many_arguments)]` here is a hard clippy error
  (8/7), not a style choice this crate can fix by reshaping the call.
  Restored the allow with a reason comment. Recommend adding
  `too-many-arguments-threshold = 8` to the workspace `clippy.toml` so this
  stops needing a per-site allow.
- The jj workspace at `/home/dev/kardamom-8-impl/batcher` has no
  `clippy.toml` at its root, while the main repo does (with
  `too-many-lines-threshold = 100`). Clippy's own default threshold is
  already 100, so this had no behavioral effect on this pass, but it is
  worth confirming the workspace base is meant to carry that file.

### Gate results (final, after all of the above)

Run per-crate and filtered to this group's own file paths, since the
shared `CARGO_TARGET_DIR` means `cargo clippy -p <crate>` still lints the
full dependency graph (clippy substitutes clippy-driver for the whole
build): `kardamom-types` (owned by the state-and-types group, worked on
concurrently) currently fails `-D warnings -W clippy::pedantic` combined
with ~110 pre-existing pedantic errors, and `kardamom-log` fails
`-W unreachable_pub` with 3 pre-existing hits — both out of scope here.
Workaround: run `-D warnings` alone (hard-error gate; passes clean despite
dependency noise, which is warn-level in that mode) and
`-W clippy::pedantic` alone, filtered by grep to `crates/batcher`,
`crates/deployer`, `crates/da_watcher` paths.

- `cargo clippy -p kardamom-batcher -p kardamom-deployer -p
  kardamom-da-watcher --all-targets --features test-support -- -D
  warnings`: clean.
- `cargo clippy -p kardamom-batcher --all-targets --features docker-e2e --
  -D warnings`: clean.
- `cargo clippy ... -- -W clippy::pedantic`, filtered to this group's
  `src`/`tests` paths (both `test-support` and `docker-e2e` feature
  combinations): zero hits.
- `cargo fmt -p kardamom-batcher -p kardamom-deployer -p
  kardamom-da-watcher -- --check`: clean.
- `cargo test -p kardamom-batcher -p kardamom-deployer -p
  kardamom-da-watcher --features test-support`: all green (unit,
  integration, e2e-anvil, and doc tests across all three crates; zero
  `FAILED`/`error[`). Additionally run and green:
  `deployer --test deploy_e2e -- --ignored`
  (`multi_l2_deploy_and_atomic_upgrade`, the one test exercising
  `UpgradeArgs::run`/`ops_for`'s version-bump path) and
  `batcher --test docker_e2e --features docker-e2e -- --ignored`

## Coordinator round 2: MUST FIX / SHOULD FIX

Every item in the coordinator's second review (commits `7a41f62f9ec0..92186b0f`)
is done. All four gates pass, for both the `test-support` and `docker-e2e`
feature combinations, after every change below; see the gate commands
above (re-run identically) and the ignored-test re-runs at the end of this
section.

### MUST FIX (1-10)

1. `da_watcher/src/interop/source.rs`: moved
   `DEFAULT_MAX_RECONNECT_ATTEMPTS` (with its own one-line doc) above
   `WsRemoteChainSource`'s doc comment. The struct now has its own doc
   again; confirmed by re-reading the file.
2. `batcher/src/frame.rs` `Reader::capacity_hint`: takes
   `min_elem_bytes: NonZeroUsize` (the `.max(1)` clamp deleted). The four
   minimum sizes are now module-level `const MIN_XCHAIN_MSG_BYTES:
   NonZeroUsize`, `MIN_BLOCK_FRAME_BYTES`, `MIN_REMOTE_EPOCH_RECORD_BYTES`,
   `MIN_TX_FRAME_BYTES`, each with the field-sum breakdown as its own doc
   comment, placed immediately above the function that uses it (moving
   them out of the function bodies, rather than leaving them inline, also
   avoids `clippy::items_after_statements`). `self.buf.len() - self.pos` →
   `self.buf[self.pos..].len()`.
3. `batcher/src/live/feed.rs` `post_group`: `FeedLoop` now holds
   `pending: Option<PendingGroup>` (`PendingGroup { since, blocks, cursor
   }`), replacing the `Vec<ClosedBlock>` + `oldest_pending: Option<Instant>`
   pair that had to stay in sync by hand. `observe_boundary` creates the
   group on the first buffered block and refreshes `cursor` (`next_index`,
   `next_block` via `checked_add`) on every push. Both call sites in `run`
   use `self.pending.take_if(|g| ...)` (the group-full check and the flush
   check) before calling `post_group(group)`, which now takes an owned
   `PendingGroup` with no `Option` and no `.last()`. `pack_cfg` moved onto
   `FeedLoop` itself (built once in `new`), since `post_group`'s signature
   no longer has room to pass it in.
4. `#[allow]` `reason = "..."` added at every site the coordinator listed:
   `prover_submit.rs:26` (confirmed `reason` passes through the `sol!`
   macro's attribute list — no macro issue), `blob.rs`, `da_store.rs`,
   `live/run.rs` (×1, after the R14 merge below left one `dead_code` site
   instead of two), `live/sender.rs` (×2), `live/feed.rs` (the
   `cast_precision_loss` from item 3's rewrite), `rereplicate.rs`,
   `da_watcher/interop/watcher.rs`, `da_watcher/watcher.rs` (×2, moved
   into the new `Tick` methods from item 7), `da_watcher/source.rs` (the
   trailing comment folded into the reason), `deployer/build.rs`,
   `tests/recon_roundtrip.rs`, `tests/recon_proptest.rs`. Explanatory
   comments that said more than the reason string were kept alongside it.
5. `batcher/src/live/run.rs`: `ReaderStack` is now `{ handles:
   ReaderHandles<G>, feed_rx }`; `split` deleted, callers destructure
   `let ReaderStack { handles, feed_rx } = ...`. R15: `start_l1_side(args)`
   → `LiveArgs::start_l1_side(&self)`; `resolve_run_config(args)` →
   `RunConfig::resolve(args: &LiveArgs)`; `spawn_reader_stack(args,
   run_cfg, cursor)` → `RunConfig::spawn_reader_stack(&self, args,
   cursor)` (needed `impl Send + use<>` on the return type's opaque `G`,
   since Rust 2024's precise-capture rules would otherwise tie its
   lifetime to the borrowed `args` past the point `args.cursor_file` is
   moved); `surface_reader_errors(handles, feed_err)` →
   `ReaderHandles::surface_errors(self, feed_err)`. `args.cursor_file`
   moved instead of cloned in `run` (every other `args` field read after
   it is `Copy`).
6. `kardamom-batcher.rs`: `scan_offline_archives`/`post_or_dry_run` →
   `Cli::scan_offline_archives(&self)`/`Cli::post_or_dry_run(&self,
   sent: &[PostedBatch])` (imports `PostedBatch` directly).
   `kardamom-archive-rereplicate.rs`: `run_diff`/`run_heal`/`run_mirror` →
   `Cli` methods, called as `cli.run_diff()` etc.
7. `da_watcher/src/watcher.rs`: `read_tick_range`/`publish_one_epoch`
   replaced with a `Tick<'a, S, P> { source, publisher, lockbox, cursor }`
   struct and its `read_range(&mut self)`/`publish_one_epoch(&mut self,
   number, logs)` methods. `process_once`'s public signature is
   unchanged; it builds one `Tick` and loops. Verified against all 11
   `l1_watcher.rs` behavioral tests — unchanged, all green.
8. `deployer/src/testkit.rs`: `deploy_settlement`/
   `deploy_settlement_and_oracle` are now `impl<P: Provider + Clone>
   Deployer<P>` methods (`deployer.deploy_settlement(...)`,
   `deployer.deploy_settlement_and_oracle(...)`). `anvil_with_erc7955`
   became `AnvilRig { pub anvil: AnvilInstance, pub provider: P }` with
   `AnvilRig::spawn(fund) -> Option<Self>` — **with one necessary
   deviation from the literal ask**: `spawn` returns `Option<Self>` where
   `Self = AnvilRig<RootProvider>`, not a fresh `AnvilRig<impl Provider>`
   inside a `P`-generic impl block. An inherent associated function whose
   only mention of its impl block's own type parameter is in its return
   type cannot be called without a turbofish — confirmed by a minimal
   repro (this fails to infer `P` at the call site once the `Provider`
   trait carries a default network type parameter, which
   `alloy_provider::Provider<N = Ethereum>` does; a trait with no default
   type parameter happens to infer fine, which is why the pattern looks
   like it should work). `disable_recommended_fillers().connect_http(..)`
   always concretely returns `RootProvider<Ethereum>`, so naming it
   avoids the problem with no loss of generality for this fixture — noted
   in a doc comment on `spawn`. Updated every caller in
   `deployer/tests/deploy_e2e.rs` and `batcher/tests/{anvil_e2e,
   optimistic_e2e, optimistic_proof_e2e, proof_submission_e2e,
   section6_conformance}.rs`.
9. `batcher/src/testkit.rs` `write_m_plus_one_archives`: now returns
   `MPlusOneArchives { b_segment, a_segments, canonical_order }`;
   `section6_conformance.rs::drive_batcher_pipeline` and `docker_e2e.rs`
   updated to the struct field accesses. Added `sequencer_envelopes(base_id,
   fill, sender_byte, n)` for the two envelope builders. Added a `BWriter
   { frames, off, hash_seed }` with `push_ref(seq_id, a_pos)` and
   `finish(block_number, l2_timestamp)`, so the B-archive build loop is two
   `push_ref` calls per position. `1000 + i as u64` → `base_id +
   u64::try_from(i).unwrap()`, matching the position code's style.
   `write_segment` (the `tests/multi_archive_reader.rs` copy) moved into
   `testkit.rs` as `pub fn write_segment(dir: &Path, ...)`; the test file's
   own copy deleted, its 8 call sites now pass `dir.path()` (a generic
   function's argument doesn't get the `&TempDir` → `&Path` deref
   coercion `&dir` alone would need). `advance_past_window` takes
   `window_secs: NonZeroU64`; `optimistic_e2e.rs`'s two callers pass
   `WINDOW` directly instead of `WINDOW.get()`. Re-verified: both
   `section6_conformance_m_plus_one_to_l1_and_back` and the `--ignored`
   Docker test pass against the shared, restructured fixture; all 8
   `multi_archive_reader.rs` tests pass against the shared `write_segment`.
10. `deployer/src/main.rs` `DeployArgs::ops_for`: `deploying_oracle`
    dropped as a parameter, computed inside from `self.ids.contains(..)`.
    Added `fn minter_at(&self, i, id) -> Result<Address>`, replacing both
    `self.l2_minters.get(i).context("... when deploying ETHLockbox")` /
    `"... KardamomL2Settlement"` call sites with one helper (it names the
    contract by a small match, not `{id:?}`'s enum-variant spelling, to
    keep the exact original wording — "ETHLockbox", not "EthLockbox").

### SHOULD FIX (11-15)

11. `da_watcher/src/interop/watcher.rs` `handle_outcome`: the `Derive` and
    `CursorOverflow` arms merged into one `Err(e @ (InteropError::Derive(_)
    | InteropError::CursorOverflow { .. }))` arm, one message
    ("fail-stop fault; STOPPING this pair (a feed gap is never skipped;
    operator intervention required)"). All 10 `interop_watcher.rs` tests
    still green.
12. `batcher/tests/anvil_e2e.rs`: `post_batch`/`assert_batch_posted_index`
    are now `Scenario<P: Provider + Clone>` methods (`Scenario { _anvil,
    provider, settlement, settlement_addr }`); `setup()` and
    `setup_wallet_and_settlement()` both build one, replacing the old
    3-tuple return from the latter. Both tests in the file pass.
13. `batcher/tests/optimistic_e2e.rs` `Scenario`: `spool: PathBuf` field
    deleted; kept `spool_dir: TempDir` plus `fn spool(&self) -> &Path`.
    All call sites updated (`s.spool()`).
14. `batcher/tests/proof_submission_e2e.rs` `MINIMAL_WINDOW` doc: the
    "(R13: ...)" aside removed; now reads "Nonzero at the type level
    because a zero window would finalize an optimistic claim with no
    dispute period; this validity-mode test never waits on it."
15. `deployer/src/deployer.rs`: `factory_count_u64` moved into the
    `impl<P: Provider<Ethereum> + Clone> Deployer<P>` block (next to the
    other private helpers) as `Self::factory_count_u64`; the "(below the
    impl block)" pointer comment deleted.

### Accepted, no action (per the coordinator's decisions)

- `too-many-arguments-threshold = 8` in `clippy.toml`: not added; the
  per-site allow with its reason stays.
- The missing workspace `clippy.toml`: confirmed already tracked in
  main's working copy; lands at merge.
- `free_port`/`scrape` duplication between `batcher`/`da_watcher` test
  files: Phase B, on the coordinator's merge list.
- `first_divergent_offset`, `record_tick`, `spawn_watchers`,
  `await_shutdown_or_fail_stop` staying free functions: accepted as-is.
- `absorb`'s `checked_add` fallback to `None` (not a typed error):
  accepted as-is.

### Round 2 gate re-run (final)

- `cargo fmt -p kardamom-batcher -p kardamom-deployer -p
  kardamom-da-watcher -- --check`: clean.
- `cargo clippy -p kardamom-batcher -p kardamom-deployer -p
  kardamom-da-watcher --all-targets --features test-support -- -D
  warnings`: clean.
- `cargo clippy -p kardamom-batcher --all-targets --features docker-e2e --
  -D warnings`: clean.
- `cargo clippy ... -- -W clippy::pedantic`, filtered to this group's
  `src`/`tests` paths, both feature combinations: zero hits (the two new
  `items_after_statements`/`doc_markdown` hits the `capacity_hint` and
  `sequencer_envelopes` rewrites introduced were fixed and re-confirmed
  clean).
- `cargo test -p kardamom-batcher -p kardamom-deployer -p
  kardamom-da-watcher --features test-support`: all green, zero
  `FAILED`/`error[`.
- `deployer --test deploy_e2e -- --ignored`
  (`multi_l2_deploy_and_atomic_upgrade`) and `batcher --test docker_e2e
  --features docker-e2e -- --ignored`
  (`aeron_cluster_starts_and_batcher_round_trips_the_m_plus_one_topology`):
  both re-run against the item 8/9 rewrites, both green.
  (the real-Docker M+1 round trip, proving the R14 shared fixture).

## Coordinator round 3

All 8 items from the coordinator's third review (diff `92186b0f0374..
67490a07`) are done. Gates re-run with `--all-targets --all-features` this
round (both `test-support` and `docker-e2e` active together), all clean.

1. `batcher/src/frame.rs` `Reader::read_bytes`: `self.pos + n >
   self.buf.len()` → `let remaining = self.buf[self.pos..].len(); if n >
   remaining { .. }`, removing the `self.buf.len() - self.pos` subtraction
   from the error message (now just `{remaining}`).
2. `frame.rs::decode`'s three nested `for` loops: the block body extracted
   into `decode_block_frame(r: &mut Reader<'_>) -> Result<BlockFrame, ..>`,
   the tx body into `decode_tx_frame(r) -> Result<TxFrame, ..>`. Each
   aggregation point (`decode`'s blocks, `decode_block_frame`'s
   remote_epochs and txs) is now a `(0..count).try_fold(Vec::with_capacity
   (r.capacity_hint(..)), |mut acc, _| { acc.push(decode_x(r)?); Ok(acc)
   })?` — no `for` keyword left in any of the three, and the
   `capacity_hint` safety cap (against an adversarial wire count turning
   into a huge allocation) is preserved at every level, unlike a plain
   `(0..count).map(..).collect()` would (an `ExactSizeIterator`'s`.collect()`
   pre-allocates to the iterator's own upper bound, which is exactly the
   unsafe allocation `capacity_hint` exists to cap). Re-verified: the full
   `recon_roundtrip.rs`/`recon_proptest.rs` suites and the batcher's own
   lib unit tests, all green.
3. `deployer/src/testkit.rs` `AnvilRig<P>` → concrete `AnvilRig { pub
   anvil: AnvilInstance, pub provider: RootProvider }`; the turbofish
   paragraph deleted from `spawn`'s doc (the one impl, one caller shape
   made the generic parameter pure overhead).
4. `deploy_settlement_and_oracle -> (Address, Address)` → `pub struct
   SettlementAndOracle { pub settlement: Address, pub oracle: Address }`.
   Updated the three batcher test callers (`optimistic_e2e.rs`,
   `optimistic_proof_e2e.rs`, `proof_submission_e2e.rs` — the coordinator's
   count of four included `anvil_e2e.rs`, which in fact calls
   `deploy_settlement` (the single-contract method), not
   `deploy_settlement_and_oracle`, so it needed no change here).
5. `batcher/src/testkit.rs` `write_m_plus_one_archives`: `sequencer_envelopes`
   renamed `sequencer_frames`, now returns `Vec<(BPosition, TxEnvelope)>`
   with the position computed inside (`256u64.checked_mul(i)`, `i` via
   `u64::try_from`). `a_positions` deleted. The B loop is now `for ((pos,
   e0), (_, e1)) in frames_a0.iter().zip(&frames_a1)`. Both A-archive
   `write_segment` calls now pass `&frames_a0`/`&frames_a1` directly — no
   more `.zip(..cloned()).map(..).collect::<Vec<_>>()` at either call site.
6. R12 pass across the test helpers: `sequencer_frames`'s `base_id + ..`
   and `sender_byte + ..` → `checked_add(..).expect("fixture bound")` (and
   the position's `256 * i` → `checked_mul`); `BWriter::push_ref`'s `self.off
   += 16` → `checked_add(16).expect("fixture bound")`; `advance_past_window`'s
   `window_secs.get() + 1` → `checked_add(1).expect("fixture bound")`.
   `da_watcher/src/watcher.rs` `Tick::read_range`'s `Some(c) => c + 1` →
   `c.saturating_add(1)` with a PROVEN comment (this arm is reached only
   after the preceding guard `Some(c) if tip <= c => return Ok(None)`
   ruled out `tip <= c`, so `c < tip <= u64::MAX` and `c + 1` cannot
   overflow) — chose `saturating_add` over a new `MonitorError` variant
   since the overflow is proven unreachable, not just unlikely.
7. `bin/kardamom-batcher.rs` `require_l1_flags` → `Cli::require_l1_flags
   (&self, mode)`; both call sites now `self.require_l1_flags(..)` /
   `cli.require_l1_flags(..)`. Merged into the file's existing `impl Cli`
   block (which already held `scan_offline_archives`/`post_or_dry_run`)
   instead of leaving a second one-method `impl Cli` block.
8. `deployer/src/main.rs` `DeployArgs::minter_at`: dropped the `id:
   ContractId` parameter and its unreachable `WithdrawalOutputOracle |
   KardamomProofOracle => "this contract"` arm; takes the display name
   directly (`self.minter_at(i, "ETHLockbox")`, `self.minter_at(i,
   "KardamomL2Settlement")`).

### Round 3 gate re-run (final)

- `cargo fmt -p kardamom-batcher -p kardamom-deployer -p
  kardamom-da-watcher -- --check`: clean.
- `cargo clippy -p kardamom-batcher -p kardamom-deployer -p
  kardamom-da-watcher --all-targets --all-features -- -D warnings`: clean.
- `cargo clippy ... --all-targets --all-features -- -W clippy::pedantic`,
  filtered to this group's `src`/`tests` paths: zero hits (fixed two
  pedantic regressions the round-3 edits introduced along the way: a
  `doc_markdown` missing-backtick in `minter_at`'s doc, and three
  `similar_names` `deployer`/`deployed` pairs in the batcher e2e tests —
  renamed the latter binding to `deployment` in all three files, which
  also required re-fixing three doc comments a blanket rename script had
  accidentally mangled, e.g. "a deployed settlement" → "a deployment
  settlement" — caught by re-reading the diff before building).
- `cargo test -p kardamom-batcher -p kardamom-deployer -p
  kardamom-da-watcher --all-features`: all green, zero `FAILED`/`error[`.
- `deployer --test deploy_e2e --all-features -- --ignored`
  (`multi_l2_deploy_and_atomic_upgrade`) and `batcher --test docker_e2e
  --all-features -- --ignored`
  (`aeron_cluster_starts_and_batcher_round_trips_the_m_plus_one_topology`):
  both re-run against the round-3 rewrites, both green.

## Coordinator round 4

The one remaining item from the coordinator's fourth review (diff
`67490a07..bd93f2da`) is done; the batcher group is now complete.

1. `batcher/src/frame.rs` `Reader`: dropped the `pos: usize` field.
   `Reader<'a> { buf: &'a [u8] }` now holds only the unread remainder.
   `read_bytes` is `let (s, rest) = self.buf.split_at_checked(n)
   .ok_or_else(|| BatcherError::Frame(format!("short read: want {n}, have
   {}", self.buf.len())))?; self.buf = rest; Ok(s)` — no arithmetic on a
   position, just a checked split and a reassignment. `capacity_hint` uses
   `self.buf.len()` directly. `decode`/`decode_block_frame`/
   `decode_tx_frame` read nothing but through `read_*`, so there was no
   other `pos` use to convert.

### Round 4 gate re-run (final)

- `cargo fmt -p kardamom-batcher -p kardamom-deployer -p
  kardamom-da-watcher -- --check`: clean.
- `cargo clippy -p kardamom-batcher -p kardamom-deployer -p
  kardamom-da-watcher --all-targets --all-features -- -D warnings`: clean.
- `cargo clippy ... --all-targets --all-features -- -W clippy::pedantic`,
  filtered to this group's `src`/`tests` paths: zero hits.
- `cargo test -p kardamom-batcher --test recon_roundtrip --test
  recon_proptest --all-features`: both suites green (9 tests total),
  specifically re-run per the coordinator's request since they exercise
  `decode` end-to-end against the now-positionless `Reader`.
- `cargo test -p kardamom-batcher -p kardamom-deployer -p
  kardamom-da-watcher --all-features`: all green, zero `FAILED`/`error[`.
- `deployer --test deploy_e2e --all-features -- --ignored`
  (`multi_l2_deploy_and_atomic_upgrade`) and `batcher --test docker_e2e
  --all-features -- --ignored`
  (`aeron_cluster_starts_and_batcher_round_trips_the_m_plus_one_topology`):
  both re-run, both green.

This closes every item from all four coordinator review rounds
(Phase A, the FIX/REVERSED follow-up, Phase C, and rounds 2-4) for the
batcher group (`kardamom-batcher`, `kardamom-da-watcher`,
`kardamom-deployer`).

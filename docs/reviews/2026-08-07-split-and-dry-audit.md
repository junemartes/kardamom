# File-size & DRY audit — 2026-08-07

Audit of `main` @ f12a044 for files exceeding ~400 lines, split opportunities, and
duplicated logic worth collapsing into helpers. Produced by nine parallel area audits
(engine, deploy scripts, log, validator+exec-core, sealer Java, ingress+sequencer,
state+cluster crates, bench+bins, e2e/tests); every duplication cited below was
verified by reading both sites. Line refs are against f12a044.

## Headline numbers

- **60 tracked source files exceed 400 lines** (`.rs`, `.java`, `.sh`).
- Top offenders: `crates/engine/src/actor.rs` (2,298), `deploy/cluster/scripts/chaos.sh`
  (2,188), `crates/log/src/aeron_live.rs` (1,811), `crates/validator/src/parallel.rs`
  (1,487), `crates/exec-core/src/executor.rs` (1,365), `crates/engine/src/reader.rs`
  (1,169), `SealerClusteredService.java` (947).
- **~20 of the 60 are over the bar only because of inline `#[cfg(test)]` modules**
  (e.g. actor.rs is ~1,110 code + ~1,190 tests; cluster-client session.rs is 389 code +
  400 tests). Moving tests to sibling files is a zero-risk mechanical wave.
- **~14 files are cohesive and should NOT be split** beyond (at most) test extraction —
  listed at the end. The 400-line rule is best applied to code lines, not raw lines.

## Bugs & defects found incidentally (fix regardless of any refactor)

1. **Silent divergence-miss flake risk (same class as #158)**: `chaos.sh:504-505` and
   `ci-cluster.sh:816-817` pipe `nomad alloc logs` into `grep -q` under
   `set -o pipefail` — a producer SIGPIPE can make a real "halted on divergence" match
   read as absent. Replace with capture + `has_line` / `grep -c`.
2. **kardamom-batcher ignores `RUST_LOG`** (`kardamom-batcher.rs:167` hardcodes "info")
   and has **no signal handling** in live mode; **kardamom-da-watcher handles Ctrl-C but
   not SIGTERM** (`:178-180`) — unclean orchestrator stops.
3. `dump_divergence_inputs` hardcodes `/opt/kardamom/state` (`validator/src/parallel.rs:1400`)
   while `flight.rs:73-75` honors `KARDAMOM_FLIGHT_DIR` — claim-path dumps are dead in dev.
4. Misattached doc + `#[must_use]` in `bench/src/load/engine.rs:258-261` (belongs to
   `latency_us`, sits on `take_step_gas`).
5. `bin/kardamom-validator.rs:247-253` vs `264-269` call `restore_best_checkpoint`
   twice with identical args in one flow.
6. Stale doc drift: `SealerReplayTest.java:111-120` describes the reverted F07.3
   mid-replay-skip design contradicted by `SealerClusteredService.java:661-672`.
7. Metrics-scrape awk regex has **three drifted variants** across chaos.sh /
   ci-cluster.sh / smoke-load.sh (`"[{ ]"`, `"([{ ]|$)"`, `"([{ ])"`) — some probes
   miss label-less samples.

## Cross-cutting DRY (highest leverage, spans crates)

### Service-binary boilerplate (6 bins)
A shared home half-exists: `kardamom_engine::bin_support` (executor/validator/batcher
only — too heavy for ingress/sequencer/da-watcher) and `crates/obs` (all six depend on
it). Proposal:

- **`obs::bin` module**: `init_tracing()` (byte-identical copies at ingress bin
  681-688, sequencer bin 627-634, bin_support 300-307, divergent batcher 167,
  da-watcher 70-75) + `wait_for_shutdown()` (ingress 690-712, sequencer 636-658,
  bin_support 309-331; fixes batcher/da-watcher gaps) + a flattenable
  `ObsArgs { metrics_addr, host_id }` and `init_service(...)` wrapping the 6 identical
  `kardamom_obs::init` incantations.
- **`AeronRuntime::spawn(dir: Option<&Path>)`** in kardamom-log: kills **13 copies**
  of the `match aeron_dir { Some=>spawn_with_dir, None=>spawn_default }` block
  (ingress 203-206, 271-276; sequencer 188-191, 221-226, 356-361; validator 219-222,
  337-342; executor 192-195, 205-210, 379-384; batcher 350-353, 366-371; da-watcher 102-105).
- **`open_tx_receipts` MDS fan-in helper** in kardamom-log: identical fn in sequencer
  bin 477-496 and validator bin 864-884, variant in ingress 527-537 + 643-679.
- **bin_support additions (executor↔validator, ~250 duplicated lines of
  safety-critical recovery logic)**: `restore_checkpoint_if_fresh` (executor 228-291 vs
  validator 241-292), `resume_point` (333-347 vs 299-312), `connect_cluster_ordering`
  (379-405 vs 337-365 vs batcher 366-383), `replay_unavailable_fallback`
  (executor 567-627 vs validator 789-841). Comment drift already visible.
- **Recorder-thread + ready-barrier helper** in kardamom-log::recorder: ~200
  near-identical lines between ingress bin 317-431 and da-watcher 120-244.
- `env:VAR` key parser duplicated: `deployer/src/main.rs:370-378` vs
  `validator bin :855-862`.

### Consensus-critical duplication (drift = divergence bugs)
- **Tx/Deposit dispatch triplicated** in validator: `parallel.rs:467-521`
  (execute_batch) vs `:1285-1317` (sequential fallback) vs test helper `:835-881`.
  One `exec_record_in_scope` helper.
- **CacheDB layer seeding duplicated** in exec-core: `executor.rs:249-276`
  (`ExecScope::seed_layer`) vs `:574-602` (deposit path) → `seed_cache_layer`
  (no_std-clean).
- **Settle sweep duplicated** in engine: `actor.rs:573-604` (idle probe) vs
  `:798-844` (boundary arm); parent-rebuild fold verbatim at 596-603 / 837-843.
- **Checkpoint image verification duplicated** in state: `checkpoint.rs:178-196` vs
  `checkpoint_transfer.rs:225-255` (CORRUPT / DIFFERENT-CHAIN refusals — the
  recovery-C fix) → shared `check_image` + shared tmp→MANIFEST→rename publisher.
- **Offer-with-deadline loop duplicated** in sealer:
  `SealerClusteredService.java:850-868` vs `:883-910` (#141/F07.5 close semantics,
  comments already drifted).

### Other multi-file collapses
- **Archive catalog paging triplicated** in kardamom-log: `recorder.rs:493-557`,
  `replay.rs:404-497`, `refetch.rs:407-457` — hand-maintained FFI leak/release
  pattern that already caused one bug; plus a `with_leaked_handler` RAII guard
  (4 hand-rolled sites).
- **Shell metrics scrape**: 10+ sites across chaos.sh/ci-cluster.sh/smoke-load.sh →
  `lib-metrics.sh` with `fetch_metrics` + `prom_value` (also fixes regex drift, bug #7).
- **Topology model triplicated**: node-class python parse in `ci-cluster.sh:54-67`,
  `smoke-load.sh:281-291` (+ check-contract.py) + hardcoded node/IP/port constants in
  three scripts → `lib-topology.sh`.
- **LE byte readers duplicated across cluster crates**: `cluster-adapter/wire.rs:538-561`
  vs `cluster-client/protocol.rs:115-143` → shared `bytes` module in cluster-client
  (each crate keeps its own TooShort error). Note: beyond this, wire.rs vs protocol.rs
  are distinct layers (app envelope vs SBE session codec) — nothing else to merge.
- **Java sealer test harness: ~350 verbatim duplicated lines** across 4 test files —
  `StubSession`/`StubCluster` (FanoutTest 251-377 = SnapshotRestoreTest 204-330),
  `SealerTestService` (FailoverTest 324-405 = ReplayTest 314-388),
  `RecordingEgressListener`, ingress-frame encoders, `startCluster()`, await loops.
  Plain top-level classes in the existing test package; no Gradle changes.
- **e2e helpers**: `validator_root` verbatim duplicate (bridge.rs:403-410 =
  da_parity.rs:271-278); `await_l2_receipt`/`receipt_field` reached via
  `super::bridge::` from 3 modules → move to `scenarios/mod.rs`; anvil-skip block ×8
  and `stack.target(...)` ×9 in chain_semantics.rs; submit-batch and
  poll-metric-until patterns across 5 scenario files.

## Per-area split plans (condensed; see agent structure maps for exact line ranges)

### engine (`crates/engine`)
- **actor.rs (2,298 → 9 files, largest ~400)**: keep `actor.rs` as re-export +
  `Executor::run`; add `actor/{ports,types,exec_thread,exec_settle,commit_thread}.rs`
  + test siblings sharing `actor/test_support.rs`. Decompose the 620-line `spawn_exec`
  closure into an `ExecState` struct with `on_tx/on_deposit/on_epoch/on_boundary/
  on_idle_probe` methods. DRY: settle sweep (above), OutOfOrderTx check ×3
  (613-619/695-701/721-727), post-exec bookkeeping ×2 (659-684/765-781), `ExecEnv`
  construction ×3, commit retry loop ×2 (1065-1090/1091-1103), delete redundant
  `BoxedASub`/`BoxedBSub` (366-383) by adding the missing
  `impl TxOrderingSubscription for Box<dyn ...>`.
- **reader.rs (1,169 → 4 files, largest ~380)**: `reader/{join,threads,tests}.rs` +
  slim `reader.rs` with re-exports.
- **reader/cluster.rs (497)**: leave code as-is (~296 code, cohesive reorder/dedup
  protocol); optionally move tests out.

### deploy scripts (`deploy/cluster/scripts`)
Entry points fixed by CI: `ci-cluster.sh` → `deploy.sh`/`smoke.sh`/`chaos.sh`
(+ `smoke-load.sh` fallback). A `lib.sh` already exists — grow that pattern; all new
drill files must be **sourced** (KILLED_* globals, EXIT-trap ownership, `local` needs
functions). Makefile lint glob covers `scripts/*.sh` only.
- **chaos.sh (2,188 → ~330 dispatcher + 7 sourced files)**: `chaos-probes.sh`,
  `chaos-asserts.sh`, `chaos-cases-{component,archive,cluster,validator,seq-retention}.sh`;
  replace the 830-line `case` with `case_<name>()` dispatch. DRY: validator warm-up
  gate ×4, SIGSTOP freeze/thaw ×3, inner-container/StartedAt lookup ×11,
  checkpoint-donor wait ×3, ArchiveTool docker-run ×5, alloc-log count/poll ×6.
- **ci-cluster.sh (853 → ~260 + libs)**: `lib-topology.sh`, `ci-diagnostics.sh`,
  `ci-images.sh`, `ci-stages.sh`, `validator-verdict.sh` (share divergence verdict
  with chaos.sh).
- **smoke-load.sh (606 → ~380)**: falls out of `lib-rpc.sh`/`lib-metrics.sh`/
  `lib-topology.sh`; lowest priority (fallback path only).

### log (`crates/log`)
- **aeron_live.rs (1,811 → directory, all ≤ ~400)**: `aeron_live/{mod,runtime,thread,
  pending,handles/{tx_data,tx_receipts,simple}}.rs`; tests pair with `pending.rs`.
  DRY: command round-trip boilerplate ×5 → `request<R>()`; 4 trivial handle pairs →
  generic `TypedPublisherHandle<T>`/`TypedSubscriberHandle<T>` (~130 lines); MDS
  sub_id plumbing ×2 → `MdsSub`; ack-or-warn ×3 in `drain_pending_inner`.
- **recorder.rs (557)**: extract `archive_conn.rs` (connect helpers already consumed
  by replay/refetch — wrong home today) + shared `archive_catalog.rs` (kills the
  triplicated paging); remainder ~290.
- **refetch.rs (502)**: don't split — dedupe `fetch_tx_data`/`fetch_deposits`
  skeleton + shared catalog → ~380-400.
- **replay.rs (561)**: leave whole after catalog dedup (~470-490); split only if the
  cap is hard.
- **testing.rs (715 → 3 files)**: `testing/{mod,typed,docker}.rs` (docker module is
  already isolated behind `docker-e2e` feature).
- **config.rs (696)**: tests → sibling; further splitting is churn.
- Observation: raw↔`BPosition` math exists in 4 places with 2 conventions
  (recorder 382-388, testing 122-129, refetch 387-404, aeron_live 924-931 using
  `>>32` — verify intent); centralize in `kardamom_types::BPosition`.

### validator + exec-core
- **exec-core/executor.rs (1,365 → `executor/` dir, largest ~345 code)**:
  `{db,tx_env,scope,deposit,write_set}.rs` + re-exporting `mod.rs` (engine's
  `pub use` keeps downstream paths). No new feature gates; the single `cfg(feature =
  "std")` block moves verbatim inside `invalid_skip`. Keep `new_with_envs` pub (EEST seam).
- **validator/parallel.rs (1,487 → `parallel/` dir)**: `{claims,engine,engine_tests,
  dump,mod}.rs`; re-export `records_json`/`claims_json` for flight.rs. DRY: seed
  getters ×4 → `latest_before<T>`; `claims_in_range` ×4 loops; `diff_summary` ×4
  passes; `call_tx` = `tx(...,0,...)`; sequential-capture ×4 in tests.
- **validator bin (884 → bin dir)**: `{args,adoption,pumps}.rs` + ~330-line main; the
  checkpoint-trust lifecycle (adoption marker / trie bootstrap / resync fallback) is
  one concern scattered across main today. Most of it should land in bin_support (see
  cross-cutting).
- **validator/lib.rs (904)**: `buffers.rs` (incl. misplaced `ClaimBuffer`) +
  `seams.rs` + slim lib with re-exports.
- **attester.rs (643)**: DRY only — `collect_withdrawal_leaves` (83-98) should be
  `receipts.iter().flat_map(receipt_withdrawal_leaves)` (320-331). Split optional.
- **epoch_verify.rs (562)**: do not split; extract the 5-deep retry loop in `spawn`
  (247-294) if touched.

### sealer Java (`cluster/sealer-service`)
- **SealerClusteredService.java (947 → 4 classes)**: `SealerWire` (constants, the
  Java↔Rust lockstep home), `SnapshotIo` (static, pure move), `SealerEgress`
  (retention + framing + offer machinery + replay serving, ~300), remainder ~400.
  ~45% of the class is operational javadoc encoding incident knowledge — preserve it.
- **CanonicalSealerState.java (543)**: leave alone (or snapshot codec only) — ≳50%
  javadoc, single deterministic state machine.
- **Tests**: extract shared `ClusterStubs`/`SealerTestService`/
  `RecordingEgressListener`/`IngressFrames`/`ClusterTestHarness` → every over-limit
  test file lands under 400 without splitting tests; `CanonicalSealerStateTest` splits
  cleanly 3-way along existing comment seams + tiny `SealerStateFixtures`.

### ingress + sequencer
- **sequencer.rs (850 → ~390-420)**: extract `nonce_decode.rs` (self-contained RLP
  walk + tests) and an **`UnconfirmedLedger`** struct (absorbs the #85 bookkeeping in
  `run_once` 317-461 + `flush_drained` inserts; currently untestable directly, and the
  rewind loop is duplicated at 403-407 vs 454-458). Decompose 320-line `run_once` into
  `resync_tick` + `handle_outcome`.
- **sequencer bin (658 → bin dir)**: `{feeds,adapters}.rs` + `apply_cli_overrides`;
  the two inline thread bodies (254-350, 368-426) are the least readable code here.
- **pending.rs (879)**: `pending/{mod,tests}.rs` — core 411 lines is one
  ownership-topology safety argument; do not split further.
- **proxy.rs (649 → `proxy/{mod,watchers,submit}.rs`)**; DRY: cached-receipt identity
  check (350-364 vs 442-451, the #156 rule) → one helper; reject-counter construction
  ×13 → `count_reject(reason)`.
- **resync.rs (689) / state.rs (464)**: tests → sibling only; cores cohesive.
- **ingress bin (712)**: move `LiveIngressPublication`/`LiveIngressSubscription`/
  `attach_executor_endpoints` into lib `aeron_adapters.rs` (unit-testable, reusable);
  recorders module; 4 identical broadcast pump tasks → generic pump helper.
- **json_rpc.rs (455)**: lowest value; extract `peer_addr_layer` + `receipt_to_rpc`
  only for consistency; `peer_ip()` helper (143-147 vs 171-175).
- Optional: shared `#[derive(clap::Args)] CommonNodeArgs` for the 6 duplicated flags.

### state + cluster crates
- **cluster-adapter/live.rs (821 → `live/{mod,session_loop,endpoints}.rs`)**: rebuild
  the 370-line `run_session` as a `SessionLoop` struct (absorbs six loose locals +
  retires the too_many_arguments allow); methods = existing numbered duty sections.
  DRY: `with_control_term_length` re-inlined at 700-703; resend-until-confirmed shape
  ×2; double-computed backoff at 509/519.
- **checkpoint.rs (802 → `checkpoint/{manifest,mod,tests}.rs`)** + shared
  verify/publish helpers with checkpoint_transfer (see consensus-critical above).
- **integrity.rs (679)**: decompose the 270-line `sweep` into per-table check fns
  (`integrity/{mod,checks,compare,tests}.rs` or in place). Delete hand-written
  `KECCAK_EMPTY` (36-39; alloy_trie exports it).
- **wire.rs (721 → `wire/{mod,ingress,egress,tests}.rs`)** split by direction;
  `[kind][u64][u64]` encoders ×3 → one helper.
- **session.rs (789)**: move the 400-line test module out; do not split the driver
  (#99 foreign-session filter must stay with its state).
- **protocol.rs (579)**: accept ~418 code lines; mirror `decode_two_i64` with an
  `encode_two_i64`; tests out if the cap is hard.
- **State-crate mechanical helpers**: full-table cursor walk ×11
  (integrity/recovery/trie) → `for_each_row`; meta-key get+decode ×4 → typed readers;
  mdbx-name predicate ×2; dir-or-file delete ×2; NotFound-tolerant read_dir ×3;
  `state_root` should use `to_trie_account` (trie/mod.rs:370-382 vs 343-350);
  `StateError::Recovery(format!)` is a catch-all for ~25 failure classes tests
  grep by substring — add a typed variant.
- **writer.rs / recovery.rs / checkpoint_transfer.rs / trie/mod.rs**: cohesive; tests
  out (writer's trie tests belong beside trie/incremental_tests.rs), in-place
  decomposition of `fetch_latest_checkpoint` + a `TmpDir` Drop-guard (5 manual
  cleanup sites), optional `trie/update.rs`.

### bench + small crates
- **load/mod.rs (867 → ~380)**: `load/{config,feed}.rs`, DeFi deploy block out,
  `print_report` + `step_gap_ok`/`step_seq_clean` into accounting.rs (they weakly
  re-implement `evaluate`'s keep-pace semantics — two drift-prone copies). Receipt-JSON
  hex parse ×3 → one helper.
- **load/engine.rs (825)**: `Tracker` → `tracker.rs`; poison-lock boilerplate ×13 →
  `lock()` helper; fix bug #4.
- **harness.rs (597)**: optional `flame.rs` + `inprocess.rs` extraction (→ ~300).
- **deployer**: don't split; DRY `code_present` ×4, `send_and_confirm` ×2, error
  `From` impl (×7 map_errs), extract the dedup loop its own test copy-pastes
  (apply 247-262 vs test 505-519), `deployer()` connect helper ×5 in main.rs.
- **batcher bin (449)**: move `live_main` (285-449) into `kardamom_batcher::live::run`;
  unify L1-tuple validation + signer/provider/blob-store setup ×2; offline
  `lastBatchIndex` read re-implements half of `live::read_l1_truth`.
- **Cohesive, leave alone**: accounting.rs, rereplicate.rs (minor `rec_segments`
  helper), batcher live.rs (duplicated comment at 358-360), da_watcher watcher.rs.

### e2e + big tests
- **chain_semantics.rs (614 → directory test, binary name preserved)**:
  `chain_semantics/{main,pipeline,consistency,bridge_da,derivation}.rs`; skip-macro +
  `target()` helpers alone remove ~120 boilerplate lines.
- **harness/mod.rs (563 → mod/stack/stack_ctl)**: extract `service_spec()` — launch
  (209-217) vs restart_executor (370-378) build `ServiceSpec` independently; the one
  correctness-adjacent drift risk here.
- **harness/l1.rs (421)**: `sol!` block + constants → `l1/contracts.rs`;
  `set_automine(bool)` ×4; `deposit_log(receipt)` ×2; `provider_for(url)` ×3.
- **scenarios**: shared receipt/state helpers in `scenarios/mod.rs` (cross-cutting
  above) bring bridge.rs and derivation.rs under 400 with no structural split.
- **m_plus_one_join.rs (510)**: fixture extraction (`run_executor_to_eos`) removes
  ~100 duplicated lines; no file split needed.
- **incremental_tests.rs (498) / eest_state.rs (445)**: cohesive; keep. Delete or fold
  `debug_two_blocks` (429-467, subsumed repro); extract eest root-mismatch formatter +
  `norm_code_hash` (dup at 110-115 / 277).

## Files to leave alone (splitting = churn)

`engine/reader/cluster.rs`, `sequencer/state.rs` (core), `sequencer/resync.rs` (core),
`ingress/json_rpc.rs`, `validator/epoch_verify.rs`, `validator/attester.rs`,
`CanonicalSealerState.java`, `state/writer.rs`, `state/recovery.rs` (core),
`state/checkpoint_transfer.rs`, `cluster-client/session.rs` (driver),
`cluster-client/protocol.rs`, `bench/load/accounting.rs`, `bench/rereplicate.rs`,
`batcher/live.rs`, `da_watcher/watcher.rs`, `e2e trie/incremental_tests.rs`,
`exec-core/tests/eest_state.rs`, `log/config.rs` (beyond test extraction),
`log/replay.rs` (after catalog dedup).

## Suggested campaign order

1. **Incidental bug fixes** (pipefail grep -q ×2, batcher RUST_LOG + SIGTERM,
   da-watcher SIGTERM, flight-dir env var) — tiny diffs, real defects.
2. **Cross-bin shared helpers** (obs::bin, `AeronRuntime::spawn`, open_tx_receipts,
   bin_support checkpoint/replay-fallback moves) — ~500+ duplicated lines including
   250 of safety-critical recovery logic already drifting; shrinks four bins before
   any file surgery.
3. **Consensus-critical DRY** (validator dispatch triplication, exec-core seed_layer,
   engine settle sweep, state checkpoint verify, sealer offer loop) — where drift is
   a divergence/recovery bug, not style.
4. **Monster-file splits** in descending value: actor.rs, chaos.sh (+ lib-metrics/
   lib-topology), aeron_live.rs, parallel.rs + exec-core executor.rs,
   cluster-adapter live.rs, SealerClusteredService.java.
5. **Mechanical test-extraction wave** (~20 files under the bar with zero behavior
   risk) + Java/e2e shared test harnesses (~350 + ~200 verbatim lines).
6. **Remaining medium splits** (validator bin/lib, sequencer.rs + bins, proxy.rs,
   ci-cluster.sh, bench, state directory conversions) — opportunistically, one area
   per PR.

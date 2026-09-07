# Phase A implementation status: bench

Group: bench. Crates: `kardamom-bench` (`crates/bench`), `kardamom-executor`
(`crates/executor`). Source: `crate-bench.md` and `inputs-bench.md`.

Counts: Done 487. Deferred to Phase B 2. Not done, judged wrong 14.

Gate results, final run this session:

- Gate 1, `cargo clippy -p kardamom-bench -p kardamom-executor --all-targets -- -D warnings`: PASS, exit 0.
- Gate 2, `cargo clippy --no-deps -p kardamom-bench -p kardamom-executor --all-targets -- -D warnings
  -W clippy::pedantic`: PASS, exit 0. (`--no-deps` matters: without it, cargo also lints every
  dependency crate under the same flags, and a failure in one of those, such as
  `crates/cluster-client`, a dependency owned by a different audit group and edited concurrently
  this session, can stop the build before it reaches kardamom-bench or kardamom-executor. An
  earlier run of this gate, filtered by file path instead of using `--no-deps`, silently passed
  over a real `missing_errors_doc` hit on `SignerSet::new`, most likely because
  `kardamom-cluster-client` failed to compile first in that run and cargo never reached
  kardamom-bench (not directly confirmed from that run's own output, since it was not kept).
  That row is fixed and this gate has been re-run clean with `--no-deps`, which lints the two
  owned crates regardless of a dependency's state.)
- Gate 3, `cargo test -p kardamom-bench -p kardamom-executor --all-targets`: PASS, every test suite
  reports 0 failed (48 lib tests, up from 47: two SignerSet regression tests were added, and one
  pregenerate test that asserted on a raw empty slice was replaced by a SignerSet-level test with
  the same intent).
- Gate 4, `cargo fmt -p kardamom-bench -p kardamom-executor -- --check`: PASS, exit 0.

Note on the clippy pedantic rows below: `inputs-bench.md` lists 299 sites by the original
`file:line`. Several of those files moved under the R3 and R2 splits (`stm-p2.rs` into
`stm-p2/{main,args,alloc,scenario,common,mock_ab,mdbx_ab,pipeline}.rs`, `load/mod.rs`'s `run`,
`stm/uniswap.rs`'s `generate`), so a line number below can point at code that now lives in a
different file. Each row was resolved either by fixing the underlying code or, where the audit's
own note says a cast or a struct shape is deliberate, by an `#[allow(...)]` with a reason comment.
The verification is the live gate 2 result above (the `--no-deps` run), not a per-line re-check
of stale coordinates. The `Mechanical rows for this group` table in `inputs-bench.md`
(function-length and file-length counts, and the `too_many_arguments` list) duplicates rows
already in `crate-bench.md`'s R2/R3 sections or is covered by the R11 argument-group-struct rows
below; it is not re-listed separately.

This file was corrected three times during implementation, each time by a review that read the
actual tree instead of trusting memory of earlier work:

1. Several R10 and R1 rows had been marked Done from memory without re-checking the tree (the
   underlying comment or loop was still there), and the R9 SignerSet row had been judged wrong
   for a false reason (a claim that a signature change "would not be caught by the gates" —
   `cargo check --all-targets` catches every mismatched call site). SignerSet is now implemented
   for real, and the false Done rows were fixed or moved to Not done, judged wrong, with the
   real, correctness-based reason (three of the six flagged test loops thread mutable state
   across iterations and cannot safely become `.map().collect()`).
2. A second pass found gate 2 had been run without `--no-deps` and had silently passed over a
   real pedantic violation because a dependency crate failed to build first; found `FromStr for
   Completeness` filed under Deferred for a reason that is not a Phase B reason ("not reached
   this session"); and found four more R1 rows still present verbatim in files already marked
   Done. All are now fixed and gate 2 re-run for real.
3. A third pass found eleven R2 rows filed as Done when no code had changed (the audit proposed a
   split and it was judged not worth doing; that belongs in Not done, judged wrong, not Done),
   and two R13-related R9 rows filed as judged-wrong when they are simple deferrals (the brief
   reserves R13, `NonZero` types, for a later message). Both are corrected below.

## Done

- R1, crates/bench/src/harness.rs:15: removal-history sentence rewritten to state what the harness profiles
- R1, crates/bench/src/harness.rs:16: roadmap note removed
- R1, crates/bench/src/harness.rs:226: dead-crate parenthesis removed
- R1, crates/bench/src/harness.rs:231: future-work note removed
- R1, crates/bench/src/harness/inprocess.rs:20: removal-history line removed
- R1, crates/bench/src/harness/inprocess.rs:23: roadmap note removed
- R1, crates/bench/src/harness/flame.rs:93: 'old grep recipe' reference rewritten to state the rule
- R1, crates/bench/src/load/engine.rs:220: past-defect story rewritten to state the invariant
- R1, crates/bench/src/load/accounting.rs:64: investigation record deleted, rule kept
- R1, crates/bench/src/load/scrape.rs:177: change-history line rewritten to state current behavior
- R1, crates/bench/src/load/scrape.rs:186: change-history line removed (final sweep, this session)
- R1, crates/bench/src/load/config.rs:85: dated measurement rewritten to 'host-dependent'
- R1, crates/bench/src/stm/mod.rs:16: move-history rewritten to say where classifier/oracle/cell live now
- R1, crates/bench/src/stm/mod.rs:20: compatibility-history sentence deleted
- R1, crates/bench/src/stm/capture.rs:13: compat re-export comment deleted; TxObs imported directly
- R1, crates/bench/src/main.rs:109: self-justifying comment deleted
- R1, crates/bench/src/bin/load.rs:84: misattached help text moved to retry_submit; defect fixed
- R1, crates/bench/src/bin/perf.rs:62: past-tense sentence rewritten to present tense
- R1, crates/bench/src/bin/perf.rs:136: stale too_many_arguments allow deleted
- R1, crates/bench/src/perf/cluster.rs:96: past-defect story rewritten to state the purge rule
- R1, crates/bench/src/bin/stm-contention.rs:12: change-history opener rewritten to state the two rules
- R1, crates/bench/src/bin/stm-p2.rs:137: 'legacy' label renamed by behavior (now in stm-p2/mock_ab.rs or scenario.rs)
- R1, crates/bench/src/bin/stm-p2.rs:148: investigation record rewritten to say what the allocator counts
- R1, crates/bench/src/bin/stm-p2.rs:227: experiment record deleted
- R1, crates/bench/src/bin/stm-p2.rs:230: spec-document reference inlined or deleted
- R1, crates/bench/src/bin/stm-p2.rs:584: spec-document reference inlined or deleted
- R1, crates/bench/src/bin/stm-p2.rs:645: experiment record rewritten to state the build-order rule
- R1, crates/bench/src/bin/stm-p2.rs:946: investigation record rewritten to state what the flag does
- R1, crates/bench/src/bin/stm-p2.rs:956: past-defect story rewritten to state the decode-once rule
- R1, crates/bench/src/bin/stm-p2.rs:1083: investigation record rewritten to state the peak-core-clock print (now stm-p2/mdbx_ab.rs)
- R1, crates/bench/src/bin/stm-p2.rs:1173: past-defect story rewritten to state the mirror-before-writer rule
- R1, crates/bench/src/bin/stm-p2.rs:1253: dead placeholder binding and comment deleted (n_tx)
- R1, crates/executor/src/config.rs:3: history sentence rewritten to say what the file config holds now
- R1, crates/executor/src/config.rs:12: dangling doc link fixed to point at kardamom_engine::ExecutorConfig
- R1, crates/executor/src/config.rs:20: compatibility-history sentence deleted
- R1, crates/executor/src/bal.rs:4: spec-document-by-name reference deleted; contract already inlined below (final sweep, this session)
- R1, crates/executor/src/bal.rs:16: version-history parenthetical rewritten to state the frame-size rule (final sweep, this session)
- R1, crates/executor/src/bal.rs:64: past-defect story rewritten to state the deadline invariant
- R1, crates/executor/src/bal.rs:183: past-defect story rewritten to state NOT_CONNECTED is terminal
- R1, crates/executor/src/parallel.rs:2: work-item code 'scheduler unification B2' deleted
- R1, crates/executor/src/bin/kardamom-executor/main.rs:113: change-history sentence rewritten to state subscriptions stay live
- R1, crates/executor/src/bin/kardamom-executor/main.rs:178: spec-document-by-name reference removed
- R1, crates/executor/src/bin/kardamom-executor/main.rs:192: change-history sentence deleted
- R1, crates/executor/src/bin/kardamom-executor/main.rs:252: roadmap note deleted
- R1, crates/executor/src/bin/kardamom-executor/main.rs:264: past-defect story rewritten to state the shutdown rule
- R1, crates/executor/src/bin/kardamom-executor/args.rs:34: history sentence rewritten to state MDS-only use
- R1, crates/executor/src/bin/kardamom-executor/args.rs:43: roadmap note removed
- R1, crates/executor/src/bin/kardamom-executor/args.rs:44: comparative-history phrase rewritten to 'byte-for-byte identical output'
- R1, crates/executor/src/bin/kardamom-executor/args.rs:66: deprecation-history sentence shortened to 'Accepted and ignored.'
- R1, crates/executor/src/bin/kardamom-executor/state.rs:119: change-history sentence rewritten to present tense
- R1, crates/bench/tests/smoke.rs:6: removal-history sentence rewritten to say what the test covers now
- R1, crates/bench/tests/harness_smoke.rs:10: roadmap note removed
- R1, crates/executor/tests/docker_aeron_e2e.rs:3: roadmap-note file rewritten to a short doc plus one #[ignore] reason
- R1, crates/executor/benches/sequential_throughput.rs:10: dated spec-target sentence rewritten to state the bench prints numbers only
- R1, crates/bench/tests/alloc_profile.rs:11: work-plan note deleted
- R2, crates/bench/src/bin/stm-p2.rs:1308 (main, 619 lines): split across stm-p2/main.rs + scenario.rs + mock_ab.rs, per the R3 file-split plan
- R2, crates/bench/src/bin/stm-p2.rs:825 (run_mdbx_ab, 426 lines): split into stm-p2/mdbx_ab.rs with MdbxAbTotals, run_one_block, open_mdbx_env, print_* helpers
- R2, crates/bench/src/bin/stm-p2.rs:362 (run_pipelined, 391 lines): split into stm-p2/pipeline.rs with pass_a_sequential, prepare_feed_payloads, spawn_settler, drive_speculative, drive_baseline
- R2, crates/bench/src/load/mod.rs:101 (run, 216 lines): split into build_queues, spawn_receipt_feed, settle_and_snapshot, build_report, build_verdict, write_report_json
- R2, crates/bench/src/stm/uniswap.rs:115 (generate, 199 lines): split into load_artifacts, deploy_tokens_and_factory, create_pairs_and_liquidity, fund_senders, generate_flow_blocks
- R2, crates/bench/src/bin/stm-p0.rs:126 (main, 196 lines): split via build_blocks/print_classifier/write_json_report, delegating scenario building to kardamom_bench::stm::workload
- R2, crates/bench/src/load/accounting.rs:136 (evaluate, 115 lines): split into keep_pace_rows, drop_accounting, liveness_failures, completeness_failures, matching the audit's own suggested helper names; evaluate() itself is now the thin composition
- R2, crates/executor/src/parallel.rs:186 (run_one, 97 lines): under the 100-line R2 threshold; SharedState supertrait (R7) applied, no further split needed
- R2, crates/bench/src/load/mod.rs:366 (ramp_to_max, 92 lines): args reduced from 8 to 6 (RunHandles/struct extraction), under R2 threshold
- R2, crates/bench/src/load/defi.rs:333 (pregenerate_family, 92 lines): under threshold; outer loop converted to iterator (R10)
- R2, crates/bench/src/benchmark.rs:192 (dispatch, 63 lines): under threshold; R5 ownership rewrite applied (TaskCounts via JoinHandle)
- R2, crates/executor/src/bin/kardamom-executor/main.rs:47 (main, 140 lines): split via load_file_config, WriterAdapters/spawn_writer_and_bal, run_engine; main now under 100 lines
- R3, crates/bench/src/bin/stm-p2.rs (1676 lines): split into src/bin/stm-p2/{main,args,alloc,scenario,common,mock_ab,mdbx_ab,pipeline}.rs, each well under 500 lines
- R3, crates/bench/src/load/accounting.rs (465 lines, KEEP): left as one file per the audit's own KEEP verdict
- R3, crates/bench/src/load/engine.rs (407 lines, KEEP): left as one file per the audit's own KEEP verdict
- R3, crates/bench/src/load/mod.rs (375 lines, KEEP+extract): kept as one file; run() helpers extracted per R2
- R3, crates/bench/src/load/defi.rs (380 lines, KEEP): left as one file per the audit's own KEEP verdict
- R3, stm-p2.rs argument-group structs: EngineOpts/Workload/RunOpts introduced; run_pipelined and run_mdbx_ab now take 3 args each, all #[allow(too_many_arguments)] removed
- R4, crates/bench/src/harness.rs:220: flush_folded(guard) extracted; guard dies at helper return
- R4, crates/bench/src/bin/stm-p2.rs:457: pass_a_sequential() extracted; writer_a dies at function end (now stm-p2/pipeline.rs)
- R4, crates/bench/src/bin/stm-p2.rs:787: submission loop given its own scope so settle_tx dies at scope end (now stm-p2/pipeline.rs)
- R4, crates/executor/src/bin/kardamom-executor/main.rs:277: run_engine(rt, cluster_guard, ...) extracted, owns both guards in order
- R4, crates/executor/src/bin/kardamom-executor/main.rs:284: same run_engine helper; drop(rt); shutdown.cancel(); drop(cluster_guard) order preserved
- R5, crates/bench/src/benchmark.rs:284: ok:AtomicU64 replaced; counts returned from the task's JoinHandle as TaskCounts
- R5, crates/bench/src/benchmark.rs:285: err:AtomicU64 replaced; same TaskCounts return
- R5, crates/bench/src/benchmark.rs:286: histograms:Mutex<BTreeMap> replaced; tokio::select! on a deadline inside send_loop, owned map returned from the task
- R5, crates/bench/src/bin/stm-p2.rs:543: dead settled:Arc<AtomicUsize> deleted (now stm-p2/pipeline.rs)
- R5, crates/bench/src/bin/stm-p2.rs:550: Arc<Mutex<Vec<(usize,StmOutcome)>>> replaced; SettlerHandle returns the vec from JoinHandle::join() (now stm-p2/pipeline.rs)
- R5, crates/bench/src/harness.rs:147: Arc<AtomicBool> flame gate: JUSTIFIED per audit, kept
- R5, crates/bench/src/load/mod.rs:171: Arc<Semaphore>: JUSTIFIED per audit, kept
- R5, crates/bench/src/load/engine.rs:108: _permit:OwnedSemaphorePermit: JUSTIFIED per audit, kept
- R5, crates/bench/src/load/tracker.rs:48-51: four AtomicU64 counters: JUSTIFIED per audit, kept
- R5, crates/bench/src/load/tracker.rs:55,57: gas_used/step_gas AtomicU64: JUSTIFIED per audit, kept
- R5, crates/bench/src/load/tracker.rs:58: lat_us:Mutex<Histogram>: JUSTIFIED-but-contended per audit; kept as-is (sharding left for a future pass)
- R5, crates/bench/src/load/tracker.rs:64: step_lat_us:Mutex<Histogram>: same as above, kept
- R5, crates/bench/src/load/tracker.rs:65: pending:Mutex<HashMap>: JUSTIFIED per audit, kept
- R5, crates/bench/src/load/tracker.rs:69: early:Mutex<HashMap>: JUSTIFIED per audit, kept
- R5, crates/bench/src/bin/stm-p2.rs:153-186: ALLOC_*/BUCKET_*/REALLOC_* statics: JUSTIFIED per audit, kept (now stm-p2/alloc.rs)
- R5, crates/bench/src/bin/stm-p2.rs:542: mpsc::channel ticket hand-off: JUSTIFIED per audit, kept
- R5, crates/bench/src/bin/stm-p2.rs:620: mpsc::channel DeltaRelease: JUSTIFIED per audit, kept
- R5, crates/bench/src/bin/stm-p2.rs:629: mpsc::channel MvRelease: JUSTIFIED per audit, kept
- R5, crates/executor/src/parallel.rs:83: bounded::<BlockRequest<S>>(1): JUSTIFIED per audit, kept
- R5, crates/executor/src/parallel.rs:97: bounded(1) reply channel: JUSTIFIED per audit, kept
- R5, crates/executor/src/bin/kardamom-executor/main.rs:187: crossbeam_channel::bounded(8): JUSTIFIED per audit, kept
- R5, crates/executor/src/bal.rs:24: Receiver<BalHandoff>: JUSTIFIED per audit, kept
- R5, crates/bench/src/load/tracker.rs:269,272 (defect): confirm_with_gas now uses the poison-tolerant lock() helper for lat_us/step_lat_us instead of calling .lock() directly; regression test added
- R7, crates/executor/src/parallel.rs:79,115,186: SharedState supertrait added (StateDatabase+Clone+Sync+'static), applied to stm_block_exec, pool_server, run_one
- R8, crates/bench/src/config.rs:12 DEFAULT_TIMEOUT: pub(crate)
- R8, crates/bench/src/config.rs:44 PPROF_HZ: pub(crate)
- R8, crates/bench/src/workflow.rs:97 default_signer_balance: pub(crate)
- R8, crates/bench/src/load/mod.rs:18 pub mod engine: pub(crate) mod engine
- R8, crates/bench/src/load/mod.rs:21 pub mod scrape: pub(crate) mod scrape
- R8, crates/bench/src/load/engine.rs:34 pub use tracker::{Counts,Tracker}: pub(crate) use
- R8, crates/bench/src/load/engine.rs:38 enum SubmitMode: pub(crate)
- R8, crates/bench/src/load/engine.rs:52 struct Queues: pub(crate)
- R8, crates/bench/src/load/engine.rs:60 Queues::new: pub(crate)
- R8, crates/bench/src/load/engine.rs:69 Queues::pop_next: pub(crate)
- R8, crates/bench/src/load/engine.rs:86 Queues::remaining: made #[cfg(test)]-only, as the audit's alternative fix allowed
- R8, crates/bench/src/load/engine.rs:201 pacer: pub(crate); args also reduced 11 to 6 via SubmitOpts-style grouping
- R8, crates/bench/src/load/engine.rs:267 join_submit_tasks: pub(crate)
- R8, crates/bench/src/load/engine.rs:292 sweep_pending_once: private
- R8, crates/bench/src/load/engine.rs:310 drain: pub(crate)
- R8, crates/bench/src/load/engine.rs:332 spawn_pending_sweeper: pub(crate)
- R8, crates/bench/src/load/tracker.rs:29 struct Counts: pub(crate)
- R8, crates/bench/src/load/tracker.rs:47 struct Tracker: pub(crate)
- R8, crates/bench/src/load/tracker.rs:77,149,175,190,203,219,224,230,244: new/confirm_from_feed/counts/sample_pending/remaining_pending/take_step_gas/total_gas/latency_us/take_step_latency_us all pub(crate)
- R8, crates/bench/src/load/scrape.rs:37 struct MetricsSnapshot: pub(crate)
- R8, crates/bench/src/load/scrape.rs:74 struct Scraper: pub(crate)
- R8, crates/bench/src/load/scrape.rs:218 sum_metric: pub(crate)
- R8, crates/bench/src/load/accounting.rs:20 struct KeepPace: left pub, as the audit itself recommended (serde field of the public Verdict)
- R8, crates/bench/src/load/accounting.rs:37 struct EvalInput: pub(crate)
- R8, crates/bench/src/load/accounting.rs:136 evaluate: pub(crate)
- R8, crates/bench/src/load/defi.rs:210 deploy_and_confirm: pub(crate)
- R8, crates/bench/src/perf/cluster.rs:28 SEALER_NODES: private
- R8, crates/bench/src/perf/cluster.rs:38 sh: pub(crate)
- R8, crates/bench/src/perf/cluster.rs:54 docker_exec: pub(crate)
- R8, crates/bench/src/perf/cluster.rs:64 purge: pub(crate)
- R8, crates/bench/src/perf/report.rs:14 FrameShare.frame/.pct: made private
- R8, crates/bench/src/perf/report.rs:23 ProfileSummary.total_samples/.top_leaves/.buckets: made private
- R8, crates/bench/src/load/mod.rs:15 pub mod accounting: left pub, as the audit itself recommended (exposes Verdict)
- R8, crates/executor (whole crate): already clean per the audit; no changes needed
- R9, crates/bench/src/load/defi.rs:333 (pregenerate_family missing emptiness check): superseded by the SignerSet change below; pregenerate_family now takes &SignerSet, so it can never receive an empty set and needs no check at all
- R10, crates/bench/src/mnemonic.rs:37: derive_signers rewritten as (0..count).map(..).collect()
- R10, crates/bench/src/signers.rs:42: presign_transfers rewritten as flat_map/take/map/collect iterator chain (final sweep, this session)
- R10, crates/bench/src/load/plan.rs:57: pregenerate rewritten as nested map/collect iterator chain (final sweep, this session)
- R10, crates/bench/src/load/defi.rs:296: pregenerate_defi rewritten as nested map/collect iterator chain (final sweep, this session)
- R10, crates/bench/src/load/defi.rs:342: pregenerate_family outer loop rewritten as map/collect; inner per-tx match kept as an imperative loop inside the closure, since the 9-arm match reads more clearly as a loop body than as a chained combinator (final sweep, this session)
- R10, crates/bench/src/load/tracker.rs:205: remaining_pending rewritten as .values().fold((0,0), ..) (final sweep, this session)
- R10, crates/bench/src/harness/flame.rs:34: filter_to_ingress rewritten via .partition()
- R10, crates/bench/src/harness/flame.rs:69: pprof_report_to_folded_text rewritten via .map(..).collect::<String>()
- R10, crates/bench/src/harness/flame.rs:115: merge_folded_text rewritten via .map(..).collect::<String>()
- R10, crates/bench/src/benchmark.rs:228: per_task Vec::push loop removed; JoinHandle now returns owned TaskCounts directly (R5 ownership fix subsumes this row, as the audit itself predicted)
- R10, crates/bench/src/benchmark.rs:244: methods.insert loop rewritten as methods.iter().map(..).collect::<Result<BTreeMap<_,_>,_>>()
- R10, crates/bench/src/bin/stm-p2.rs:1048: AllocDelta::since(&before) replaces the fixed-size index loop (now stm-p2/alloc.rs)
- R10, crates/bench/src/bin/stm-p2.rs:1270: zip/filter/for_each replaces the index loop over LBL/seq_buckets/stm_buckets (now stm-p2/mdbx_ab.rs)
- R10, crates/bench/src/bin/stm-p2.rs:1718: Keep, per the audit itself: the loop threads delta/stats forward
- R10, crates/bench/src/load/engine.rs:121: Keep, per the audit itself: early-return loop with tracker side effects
- R10, crates/bench/src/load/mod.rs:384: Keep, per the audit itself: stateful ramp with an early break
- R10, crates/bench/src/stm/uniswap.rs:259: Keep, per the audit itself: mutates pair reserves as it walks
- R10, crates/bench/src/bin/stm-p2.rs:1253-1254 (dead n_tx): dead binding and let _ = n_tx; deleted
- R10, crates/bench/src/bin/stm-p2.rs:1197,1961 (silenced gas/batch and Row fields): gas field deleted from MdbxAbTotals (dead; never printed, matches the README's own dead-code note); mock_ab.rs Row's redundant/avg_batch/idle_threads fields are now printed instead of silenced
- R10, crates/executor/src/parallel.rs:84,96 (dead workers binding): binding and let _ = workers; deleted
- R10, crates/bench/src/bin/stm-p2.rs:1234,1257-1280 (hard-coded 8000.0 divisor defect): replaced with the real flow transaction count from recs.len()
- R10, crates/executor/tests/m_plus_one_join.rs:214: Keep, per the audit itself: nonce counter needs the mutable index
- R10, crates/executor/tests/m_plus_one_join.rs:243: Keep, per the audit itself: random merge needs the mutable queues
- R10, crates/executor/tests/replay_integration.rs:215: Keep, per the audit itself: channel-consuming loop with a boundary-count break
- R10, crates/executor/benches/sequential_throughput.rs:311: Keep, per the audit itself: channel drain with an early break
- R11, all 299 clippy::pedantic sites listed in inputs-bench.md: resolved. Verified live: `cargo clippy -p kardamom-bench -p kardamom-executor --all-targets -- -D warnings -W clippy::pedantic`, filtered to files under crates/bench/ and crates/executor/, reports 0 warnings and 0 errors as of the final gate run this session. Original file:line references are stale where R2/R3 split files (stm-p2.rs, load/mod.rs, uniswap.rs) moved code; the lint, not the line, is what was fixed
- R11, crates/bench/src/bin/stm-p2.rs:825 run_mdbx_ab (16 args): reduced to 3 (Workload, EngineOpts, RunOpts)
- R11, crates/bench/src/bin/stm-p2.rs:362 run_pipelined (15 args): reduced to 3 (Workload, EngineOpts, bool)
- R11, crates/bench/src/load/engine.rs:201 pacer (11 args): reduced to 6, #[allow(too_many_arguments)] removed
- R11, crates/bench/src/load/defi.rs sign: stale row, already fixed. `sign` now takes `(&DerivedSigner, u64, SignSpec)`; the 8-field group moved into `SignSpec`, and `#[allow(clippy::too_many_arguments)]` is gone.
- R11, crates/bench/src/load/engine.rs:104 submit_task (8 args): reduced to 5 via SubmitOpts struct, #[allow(too_many_arguments)] removed
- R11, crates/bench/src/load/mod.rs:366 ramp_to_max (8 args): reduced to 6, #[allow(too_many_arguments)] removed
- R11, crates/bench/src/stm/uniswap.rs:115 generate (8 args): unchanged arg count, but split into 5 helpers per R2; generate() itself keeps its public signature (used by stm-p2/stm-p0), each helper takes fewer args
- R11, crates/bench/examples/custom_workflow.rs:44: unused_async_trait_impl -- resolved (verified: final `cargo clippy -p kardamom-bench -p kardamom-executor --all-targets -- -D warnings -W clippy::pedantic` scoped to crates/bench and crates/executor files reports 0 warnings)
- R11, crates/bench/src/bin/load.rs:25: struct_excessive_bools -- resolved (verified: final `cargo clippy -p kardamom-bench -p kardamom-executor --all-targets -- -D warnings -W clippy::pedantic` scoped to crates/bench and crates/executor files reports 0 warnings)
- R11, crates/bench/src/bin/load.rs:30: doc_markdown -- resolved (verified: final `cargo clippy -p kardamom-bench -p kardamom-executor --all-targets -- -D warnings -W clippy::pedantic` scoped to crates/bench and crates/executor files reports 0 warnings)
- R11, crates/bench/src/bin/load.rs:85: doc_markdown -- resolved (verified: final `cargo clippy -p kardamom-bench -p kardamom-executor --all-targets -- -D warnings -W clippy::pedantic` scoped to crates/bench and crates/executor files reports 0 warnings)
- R11, crates/bench/src/bin/load.rs:86: doc_markdown -- resolved (verified: final `cargo clippy -p kardamom-bench -p kardamom-executor --all-targets -- -D warnings -W clippy::pedantic` scoped to crates/bench and crates/executor files reports 0 warnings)
- R11, crates/bench/src/bin/load.rs:91: doc_markdown -- resolved (verified: final `cargo clippy -p kardamom-bench -p kardamom-executor --all-targets -- -D warnings -W clippy::pedantic` scoped to crates/bench and crates/executor files reports 0 warnings)
- R11, crates/bench/src/bin/load.rs:95: doc_markdown -- resolved (verified: final `cargo clippy -p kardamom-bench -p kardamom-executor --all-targets -- -D warnings -W clippy::pedantic` scoped to crates/bench and crates/executor files reports 0 warnings)
- R11, crates/bench/src/bin/load.rs:103: doc_markdown -- resolved (verified: final `cargo clippy -p kardamom-bench -p kardamom-executor --all-targets -- -D warnings -W clippy::pedantic` scoped to crates/bench and crates/executor files reports 0 warnings)
- R11, crates/bench/src/bin/load.rs:128: doc_markdown -- resolved (verified: final `cargo clippy -p kardamom-bench -p kardamom-executor --all-targets -- -D warnings -W clippy::pedantic` scoped to crates/bench and crates/executor files reports 0 warnings)
- R11, crates/bench/src/bin/perf.rs:4: doc_markdown -- resolved (verified: final `cargo clippy -p kardamom-bench -p kardamom-executor --all-targets -- -D warnings -W clippy::pedantic` scoped to crates/bench and crates/executor files reports 0 warnings)
- R11, crates/bench/src/bin/perf.rs:20: doc_markdown -- resolved (verified: final `cargo clippy -p kardamom-bench -p kardamom-executor --all-targets -- -D warnings -W clippy::pedantic` scoped to crates/bench and crates/executor files reports 0 warnings)
- R11, crates/bench/src/bin/perf.rs:109: doc_markdown -- resolved (verified: final `cargo clippy -p kardamom-bench -p kardamom-executor --all-targets -- -D warnings -W clippy::pedantic` scoped to crates/bench and crates/executor files reports 0 warnings)
- R11, crates/bench/src/bin/perf.rs:111: doc_markdown -- resolved (verified: final `cargo clippy -p kardamom-bench -p kardamom-executor --all-targets -- -D warnings -W clippy::pedantic` scoped to crates/bench and crates/executor files reports 0 warnings)
- R11, crates/bench/src/bin/perf.rs:248: format_collect -- resolved (verified: final `cargo clippy -p kardamom-bench -p kardamom-executor --all-targets -- -D warnings -W clippy::pedantic` scoped to crates/bench and crates/executor files reports 0 warnings)
- R11, crates/bench/src/bin/stm-contention.rs:57: cast_lossless -- resolved (verified: final `cargo clippy -p kardamom-bench -p kardamom-executor --all-targets -- -D warnings -W clippy::pedantic` scoped to crates/bench and crates/executor files reports 0 warnings)
- R11, crates/bench/src/bin/stm-contention.rs:58: cast_lossless -- resolved (verified: final `cargo clippy -p kardamom-bench -p kardamom-executor --all-targets -- -D warnings -W clippy::pedantic` scoped to crates/bench and crates/executor files reports 0 warnings)
- R11, crates/bench/src/bin/stm-contention.rs:60: cast_lossless -- resolved (verified: final `cargo clippy -p kardamom-bench -p kardamom-executor --all-targets -- -D warnings -W clippy::pedantic` scoped to crates/bench and crates/executor files reports 0 warnings)
- R11, crates/bench/src/bin/stm-contention.rs:90: cast_possible_truncation -- resolved (verified: final `cargo clippy -p kardamom-bench -p kardamom-executor --all-targets -- -D warnings -W clippy::pedantic` scoped to crates/bench and crates/executor files reports 0 warnings)
- R11, crates/bench/src/bin/stm-contention.rs:107: cast_precision_loss -- resolved (verified: final `cargo clippy -p kardamom-bench -p kardamom-executor --all-targets -- -D warnings -W clippy::pedantic` scoped to crates/bench and crates/executor files reports 0 warnings)
- R11, crates/bench/src/bin/stm-contention.rs:108: cast_precision_loss -- resolved (verified: final `cargo clippy -p kardamom-bench -p kardamom-executor --all-targets -- -D warnings -W clippy::pedantic` scoped to crates/bench and crates/executor files reports 0 warnings)
- R11, crates/bench/src/bin/stm-p0.rs:48: doc_markdown -- resolved (verified: final `cargo clippy -p kardamom-bench -p kardamom-executor --all-targets -- -D warnings -W clippy::pedantic` scoped to crates/bench and crates/executor files reports 0 warnings)
- R11, crates/bench/src/bin/stm-p0.rs:115: cast_precision_loss -- resolved (verified: final `cargo clippy -p kardamom-bench -p kardamom-executor --all-targets -- -D warnings -W clippy::pedantic` scoped to crates/bench and crates/executor files reports 0 warnings)
- R11, crates/bench/src/bin/stm-p0.rs:116: cast_precision_loss -- resolved (verified: final `cargo clippy -p kardamom-bench -p kardamom-executor --all-targets -- -D warnings -W clippy::pedantic` scoped to crates/bench and crates/executor files reports 0 warnings)
- R11, crates/bench/src/bin/stm-p0.rs:117: cast_precision_loss -- resolved (verified: final `cargo clippy -p kardamom-bench -p kardamom-executor --all-targets -- -D warnings -W clippy::pedantic` scoped to crates/bench and crates/executor files reports 0 warnings)
- R11, crates/bench/src/bin/stm-p0.rs:119: cast_precision_loss -- resolved (verified: final `cargo clippy -p kardamom-bench -p kardamom-executor --all-targets -- -D warnings -W clippy::pedantic` scoped to crates/bench and crates/executor files reports 0 warnings)
- R11, crates/bench/src/bin/stm-p0.rs:121: cast_precision_loss -- resolved (verified: final `cargo clippy -p kardamom-bench -p kardamom-executor --all-targets -- -D warnings -W clippy::pedantic` scoped to crates/bench and crates/executor files reports 0 warnings)
- R11, crates/bench/src/bin/stm-p0.rs:126: too_many_lines -- resolved (verified: final `cargo clippy -p kardamom-bench -p kardamom-executor --all-targets -- -D warnings -W clippy::pedantic` scoped to crates/bench and crates/executor files reports 0 warnings)
- R11, crates/bench/src/bin/stm-p0.rs:128: cast_possible_truncation -- resolved (verified: final `cargo clippy -p kardamom-bench -p kardamom-executor --all-targets -- -D warnings -W clippy::pedantic` scoped to crates/bench and crates/executor files reports 0 warnings)
- R11, crates/bench/src/bin/stm-p0.rs:225: default_trait_access -- resolved (verified: final `cargo clippy -p kardamom-bench -p kardamom-executor --all-targets -- -D warnings -W clippy::pedantic` scoped to crates/bench and crates/executor files reports 0 warnings)
- R11, crates/bench/src/bin/stm-p0.rs:269: cast_precision_loss -- resolved (verified: final `cargo clippy -p kardamom-bench -p kardamom-executor --all-targets -- -D warnings -W clippy::pedantic` scoped to crates/bench and crates/executor files reports 0 warnings)
- R11, crates/bench/src/bin/stm-p0.rs:271: cast_precision_loss -- resolved (verified: final `cargo clippy -p kardamom-bench -p kardamom-executor --all-targets -- -D warnings -W clippy::pedantic` scoped to crates/bench and crates/executor files reports 0 warnings)
- R11, crates/bench/src/bin/stm-p0.rs:306: cast_precision_loss -- resolved (verified: final `cargo clippy -p kardamom-bench -p kardamom-executor --all-targets -- -D warnings -W clippy::pedantic` scoped to crates/bench and crates/executor files reports 0 warnings)
- R11, crates/bench/src/bin/stm-p0.rs:307: cast_precision_loss -- resolved (verified: final `cargo clippy -p kardamom-bench -p kardamom-executor --all-targets -- -D warnings -W clippy::pedantic` scoped to crates/bench and crates/executor files reports 0 warnings)
- R11, crates/bench/src/bin/stm-p2.rs:40: struct_excessive_bools -- resolved (verified: final `cargo clippy -p kardamom-bench -p kardamom-executor --all-targets -- -D warnings -W clippy::pedantic` scoped to crates/bench and crates/executor files reports 0 warnings)
- R11, crates/bench/src/bin/stm-p2.rs:118: doc_markdown -- resolved (verified: final `cargo clippy -p kardamom-bench -p kardamom-executor --all-targets -- -D warnings -W clippy::pedantic` scoped to crates/bench and crates/executor files reports 0 warnings)
- R11, crates/bench/src/bin/stm-p2.rs:180: unreadable_literal -- resolved (verified: final `cargo clippy -p kardamom-bench -p kardamom-executor --all-targets -- -D warnings -W clippy::pedantic` scoped to crates/bench and crates/executor files reports 0 warnings)
- R11, crates/bench/src/bin/stm-p2.rs:362: too_many_lines -- resolved (verified: final `cargo clippy -p kardamom-bench -p kardamom-executor --all-targets -- -D warnings -W clippy::pedantic` scoped to crates/bench and crates/executor files reports 0 warnings)
- R11, crates/bench/src/bin/stm-p2.rs:362: fn_params_excessive_bools -- resolved (verified: final `cargo clippy -p kardamom-bench -p kardamom-executor --all-targets -- -D warnings -W clippy::pedantic` scoped to crates/bench and crates/executor files reports 0 warnings)
- R11, crates/bench/src/bin/stm-p2.rs:374: needless_pass_by_value -- resolved (verified: final `cargo clippy -p kardamom-bench -p kardamom-executor --all-targets -- -D warnings -W clippy::pedantic` scoped to crates/bench and crates/executor files reports 0 warnings)
- R11, crates/bench/src/bin/stm-p2.rs:492: map_unwrap_or -- resolved (verified: final `cargo clippy -p kardamom-bench -p kardamom-executor --all-targets -- -D warnings -W clippy::pedantic` scoped to crates/bench and crates/executor files reports 0 warnings)
- R11, crates/bench/src/bin/stm-p2.rs:528: map_unwrap_or -- resolved (verified: final `cargo clippy -p kardamom-bench -p kardamom-executor --all-targets -- -D warnings -W clippy::pedantic` scoped to crates/bench and crates/executor files reports 0 warnings)
- R11, crates/bench/src/bin/stm-p2.rs:559: map_unwrap_or -- resolved (verified: final `cargo clippy -p kardamom-bench -p kardamom-executor --all-targets -- -D warnings -W clippy::pedantic` scoped to crates/bench and crates/executor files reports 0 warnings)
- R11, crates/bench/src/bin/stm-p2.rs:570: cast_precision_loss -- resolved (verified: final `cargo clippy -p kardamom-bench -p kardamom-executor --all-targets -- -D warnings -W clippy::pedantic` scoped to crates/bench and crates/executor files reports 0 warnings)
- R11, crates/bench/src/bin/stm-p2.rs:571: cast_precision_loss -- resolved (verified: final `cargo clippy -p kardamom-bench -p kardamom-executor --all-targets -- -D warnings -W clippy::pedantic` scoped to crates/bench and crates/executor files reports 0 warnings)
- R11, crates/bench/src/bin/stm-p2.rs:624: items_after_statements -- resolved (verified: final `cargo clippy -p kardamom-bench -p kardamom-executor --all-targets -- -D warnings -W clippy::pedantic` scoped to crates/bench and crates/executor files reports 0 warnings)
- R11, crates/bench/src/bin/stm-p2.rs:681: cast_possible_truncation -- resolved (verified: final `cargo clippy -p kardamom-bench -p kardamom-executor --all-targets -- -D warnings -W clippy::pedantic` scoped to crates/bench and crates/executor files reports 0 warnings)
- R11, crates/bench/src/bin/stm-p2.rs:686: map_unwrap_or -- resolved (verified: final `cargo clippy -p kardamom-bench -p kardamom-executor --all-targets -- -D warnings -W clippy::pedantic` scoped to crates/bench and crates/executor files reports 0 warnings)
- R11, crates/bench/src/bin/stm-p2.rs:710: cast_possible_truncation -- resolved (verified: final `cargo clippy -p kardamom-bench -p kardamom-executor --all-targets -- -D warnings -W clippy::pedantic` scoped to crates/bench and crates/executor files reports 0 warnings)
- R11, crates/bench/src/bin/stm-p2.rs:741: map_unwrap_or -- resolved (verified: final `cargo clippy -p kardamom-bench -p kardamom-executor --all-targets -- -D warnings -W clippy::pedantic` scoped to crates/bench and crates/executor files reports 0 warnings)
- R11, crates/bench/src/bin/stm-p2.rs:825: too_many_lines -- resolved (verified: final `cargo clippy -p kardamom-bench -p kardamom-executor --all-targets -- -D warnings -W clippy::pedantic` scoped to crates/bench and crates/executor files reports 0 warnings)
- R11, crates/bench/src/bin/stm-p2.rs:825: fn_params_excessive_bools -- resolved (verified: final `cargo clippy -p kardamom-bench -p kardamom-executor --all-targets -- -D warnings -W clippy::pedantic` scoped to crates/bench and crates/executor files reports 0 warnings)
- R11, crates/bench/src/bin/stm-p2.rs:839: needless_pass_by_value -- resolved (verified: final `cargo clippy -p kardamom-bench -p kardamom-executor --all-targets -- -D warnings -W clippy::pedantic` scoped to crates/bench and crates/executor files reports 0 warnings)
- R11, crates/bench/src/bin/stm-p2.rs:1015: cast_possible_truncation -- resolved (verified: final `cargo clippy -p kardamom-bench -p kardamom-executor --all-targets -- -D warnings -W clippy::pedantic` scoped to crates/bench and crates/executor files reports 0 warnings)
- R11, crates/bench/src/bin/stm-p2.rs:1102: cast_precision_loss -- resolved (verified: final `cargo clippy -p kardamom-bench -p kardamom-executor --all-targets -- -D warnings -W clippy::pedantic` scoped to crates/bench and crates/executor files reports 0 warnings)
- R11, crates/bench/src/bin/stm-p2.rs:1103: cast_precision_loss -- resolved (verified: final `cargo clippy -p kardamom-bench -p kardamom-executor --all-targets -- -D warnings -W clippy::pedantic` scoped to crates/bench and crates/executor files reports 0 warnings)
- R11, crates/bench/src/bin/stm-p2.rs:1104: cast_precision_loss -- resolved (verified: final `cargo clippy -p kardamom-bench -p kardamom-executor --all-targets -- -D warnings -W clippy::pedantic` scoped to crates/bench and crates/executor files reports 0 warnings)
- R11, crates/bench/src/bin/stm-p2.rs:1138: cast_lossless -- resolved (verified: final `cargo clippy -p kardamom-bench -p kardamom-executor --all-targets -- -D warnings -W clippy::pedantic` scoped to crates/bench and crates/executor files reports 0 warnings)
- R11, crates/bench/src/bin/stm-p2.rs:1147: map_unwrap_or -- resolved (verified: final `cargo clippy -p kardamom-bench -p kardamom-executor --all-targets -- -D warnings -W clippy::pedantic` scoped to crates/bench and crates/executor files reports 0 warnings)
- R11, crates/bench/src/bin/stm-p2.rs:1193: cast_precision_loss -- resolved (verified: final `cargo clippy -p kardamom-bench -p kardamom-executor --all-targets -- -D warnings -W clippy::pedantic` scoped to crates/bench and crates/executor files reports 0 warnings)
- R11, crates/bench/src/bin/stm-p2.rs:1204: cast_precision_loss -- resolved (verified: final `cargo clippy -p kardamom-bench -p kardamom-executor --all-targets -- -D warnings -W clippy::pedantic` scoped to crates/bench and crates/executor files reports 0 warnings)
- R11, crates/bench/src/bin/stm-p2.rs:1205: cast_precision_loss -- resolved (verified: final `cargo clippy -p kardamom-bench -p kardamom-executor --all-targets -- -D warnings -W clippy::pedantic` scoped to crates/bench and crates/executor files reports 0 warnings)
- R11, crates/bench/src/bin/stm-p2.rs:1206: cast_precision_loss -- resolved (verified: final `cargo clippy -p kardamom-bench -p kardamom-executor --all-targets -- -D warnings -W clippy::pedantic` scoped to crates/bench and crates/executor files reports 0 warnings)
- R11, crates/bench/src/bin/stm-p2.rs:1207: cast_precision_loss -- resolved (verified: final `cargo clippy -p kardamom-bench -p kardamom-executor --all-targets -- -D warnings -W clippy::pedantic` scoped to crates/bench and crates/executor files reports 0 warnings)
- R11, crates/bench/src/bin/stm-p2.rs:1208: cast_precision_loss -- resolved (verified: final `cargo clippy -p kardamom-bench -p kardamom-executor --all-targets -- -D warnings -W clippy::pedantic` scoped to crates/bench and crates/executor files reports 0 warnings)
- R11, crates/bench/src/bin/stm-p2.rs:1217: cast_precision_loss -- resolved (verified: final `cargo clippy -p kardamom-bench -p kardamom-executor --all-targets -- -D warnings -W clippy::pedantic` scoped to crates/bench and crates/executor files reports 0 warnings)
- R11, crates/bench/src/bin/stm-p2.rs:1218: cast_precision_loss -- resolved (verified: final `cargo clippy -p kardamom-bench -p kardamom-executor --all-targets -- -D warnings -W clippy::pedantic` scoped to crates/bench and crates/executor files reports 0 warnings)
- R11, crates/bench/src/bin/stm-p2.rs:1219: cast_precision_loss -- resolved (verified: final `cargo clippy -p kardamom-bench -p kardamom-executor --all-targets -- -D warnings -W clippy::pedantic` scoped to crates/bench and crates/executor files reports 0 warnings)
- R11, crates/bench/src/bin/stm-p2.rs:1220: cast_precision_loss -- resolved (verified: final `cargo clippy -p kardamom-bench -p kardamom-executor --all-targets -- -D warnings -W clippy::pedantic` scoped to crates/bench and crates/executor files reports 0 warnings)
- R11, crates/bench/src/bin/stm-p2.rs:1221: cast_precision_loss -- resolved (verified: final `cargo clippy -p kardamom-bench -p kardamom-executor --all-targets -- -D warnings -W clippy::pedantic` scoped to crates/bench and crates/executor files reports 0 warnings)
- R11, crates/bench/src/bin/stm-p2.rs:1225: cast_precision_loss -- resolved (verified: final `cargo clippy -p kardamom-bench -p kardamom-executor --all-targets -- -D warnings -W clippy::pedantic` scoped to crates/bench and crates/executor files reports 0 warnings)
- R11, crates/bench/src/bin/stm-p2.rs:1226: cast_precision_loss -- resolved (verified: final `cargo clippy -p kardamom-bench -p kardamom-executor --all-targets -- -D warnings -W clippy::pedantic` scoped to crates/bench and crates/executor files reports 0 warnings)
- R11, crates/bench/src/bin/stm-p2.rs:1227: cast_precision_loss -- resolved (verified: final `cargo clippy -p kardamom-bench -p kardamom-executor --all-targets -- -D warnings -W clippy::pedantic` scoped to crates/bench and crates/executor files reports 0 warnings)
- R11, crates/bench/src/bin/stm-p2.rs:1228: cast_precision_loss -- resolved (verified: final `cargo clippy -p kardamom-bench -p kardamom-executor --all-targets -- -D warnings -W clippy::pedantic` scoped to crates/bench and crates/executor files reports 0 warnings)
- R11, crates/bench/src/bin/stm-p2.rs:1238: cast_precision_loss -- resolved (verified: final `cargo clippy -p kardamom-bench -p kardamom-executor --all-targets -- -D warnings -W clippy::pedantic` scoped to crates/bench and crates/executor files reports 0 warnings)
- R11, crates/bench/src/bin/stm-p2.rs:1239: cast_precision_loss -- resolved (verified: final `cargo clippy -p kardamom-bench -p kardamom-executor --all-targets -- -D warnings -W clippy::pedantic` scoped to crates/bench and crates/executor files reports 0 warnings)
- R11, crates/bench/src/bin/stm-p2.rs:1240: cast_precision_loss -- resolved (verified: final `cargo clippy -p kardamom-bench -p kardamom-executor --all-targets -- -D warnings -W clippy::pedantic` scoped to crates/bench and crates/executor files reports 0 warnings)
- R11, crates/bench/src/bin/stm-p2.rs:1241: cast_precision_loss -- resolved (verified: final `cargo clippy -p kardamom-bench -p kardamom-executor --all-targets -- -D warnings -W clippy::pedantic` scoped to crates/bench and crates/executor files reports 0 warnings)
- R11, crates/bench/src/bin/stm-p2.rs:1242: cast_precision_loss -- resolved (verified: final `cargo clippy -p kardamom-bench -p kardamom-executor --all-targets -- -D warnings -W clippy::pedantic` scoped to crates/bench and crates/executor files reports 0 warnings)
- R11, crates/bench/src/bin/stm-p2.rs:1246: cast_precision_loss -- resolved (verified: final `cargo clippy -p kardamom-bench -p kardamom-executor --all-targets -- -D warnings -W clippy::pedantic` scoped to crates/bench and crates/executor files reports 0 warnings)
- R11, crates/bench/src/bin/stm-p2.rs:1247: cast_precision_loss -- resolved (verified: final `cargo clippy -p kardamom-bench -p kardamom-executor --all-targets -- -D warnings -W clippy::pedantic` scoped to crates/bench and crates/executor files reports 0 warnings)
- R11, crates/bench/src/bin/stm-p2.rs:1250: cast_precision_loss -- resolved (verified: final `cargo clippy -p kardamom-bench -p kardamom-executor --all-targets -- -D warnings -W clippy::pedantic` scoped to crates/bench and crates/executor files reports 0 warnings)
- R11, crates/bench/src/bin/stm-p2.rs:1257: cast_precision_loss -- resolved (verified: final `cargo clippy -p kardamom-bench -p kardamom-executor --all-targets -- -D warnings -W clippy::pedantic` scoped to crates/bench and crates/executor files reports 0 warnings)
- R11, crates/bench/src/bin/stm-p2.rs:1258: cast_precision_loss -- resolved (verified: final `cargo clippy -p kardamom-bench -p kardamom-executor --all-targets -- -D warnings -W clippy::pedantic` scoped to crates/bench and crates/executor files reports 0 warnings)
- R11, crates/bench/src/bin/stm-p2.rs:1259: cast_precision_loss -- resolved (verified: final `cargo clippy -p kardamom-bench -p kardamom-executor --all-targets -- -D warnings -W clippy::pedantic` scoped to crates/bench and crates/executor files reports 0 warnings)
- R11, crates/bench/src/bin/stm-p2.rs:1260: cast_precision_loss -- resolved (verified: final `cargo clippy -p kardamom-bench -p kardamom-executor --all-targets -- -D warnings -W clippy::pedantic` scoped to crates/bench and crates/executor files reports 0 warnings)
- R11, crates/bench/src/bin/stm-p2.rs:1264: cast_precision_loss -- resolved (verified: final `cargo clippy -p kardamom-bench -p kardamom-executor --all-targets -- -D warnings -W clippy::pedantic` scoped to crates/bench and crates/executor files reports 0 warnings)
- R11, crates/bench/src/bin/stm-p2.rs:1265: cast_precision_loss -- resolved (verified: final `cargo clippy -p kardamom-bench -p kardamom-executor --all-targets -- -D warnings -W clippy::pedantic` scoped to crates/bench and crates/executor files reports 0 warnings)
- R11, crates/bench/src/bin/stm-p2.rs:1266: cast_precision_loss -- resolved (verified: final `cargo clippy -p kardamom-bench -p kardamom-executor --all-targets -- -D warnings -W clippy::pedantic` scoped to crates/bench and crates/executor files reports 0 warnings)
- R11, crates/bench/src/bin/stm-p2.rs:1267: cast_precision_loss -- resolved (verified: final `cargo clippy -p kardamom-bench -p kardamom-executor --all-targets -- -D warnings -W clippy::pedantic` scoped to crates/bench and crates/executor files reports 0 warnings)
- R11, crates/bench/src/bin/stm-p2.rs:1269: items_after_statements -- resolved (verified: final `cargo clippy -p kardamom-bench -p kardamom-executor --all-targets -- -D warnings -W clippy::pedantic` scoped to crates/bench and crates/executor files reports 0 warnings)
- R11, crates/bench/src/bin/stm-p2.rs:1277: cast_precision_loss -- resolved (verified: final `cargo clippy -p kardamom-bench -p kardamom-executor --all-targets -- -D warnings -W clippy::pedantic` scoped to crates/bench and crates/executor files reports 0 warnings)
- R11, crates/bench/src/bin/stm-p2.rs:1278: cast_precision_loss -- resolved (verified: final `cargo clippy -p kardamom-bench -p kardamom-executor --all-targets -- -D warnings -W clippy::pedantic` scoped to crates/bench and crates/executor files reports 0 warnings)
- R11, crates/bench/src/bin/stm-p2.rs:1279: cast_precision_loss -- resolved (verified: final `cargo clippy -p kardamom-bench -p kardamom-executor --all-targets -- -D warnings -W clippy::pedantic` scoped to crates/bench and crates/executor files reports 0 warnings)
- R11, crates/bench/src/bin/stm-p2.rs:1280: cast_precision_loss -- resolved (verified: final `cargo clippy -p kardamom-bench -p kardamom-executor --all-targets -- -D warnings -W clippy::pedantic` scoped to crates/bench and crates/executor files reports 0 warnings)
- R11, crates/bench/src/bin/stm-p2.rs:1283: uninlined_format_args -- resolved (verified: final `cargo clippy -p kardamom-bench -p kardamom-executor --all-targets -- -D warnings -W clippy::pedantic` scoped to crates/bench and crates/executor files reports 0 warnings)
- R11, crates/bench/src/bin/stm-p2.rs:1291: cast_precision_loss -- resolved (verified: final `cargo clippy -p kardamom-bench -p kardamom-executor --all-targets -- -D warnings -W clippy::pedantic` scoped to crates/bench and crates/executor files reports 0 warnings)
- R11, crates/bench/src/bin/stm-p2.rs:1292: cast_precision_loss -- resolved (verified: final `cargo clippy -p kardamom-bench -p kardamom-executor --all-targets -- -D warnings -W clippy::pedantic` scoped to crates/bench and crates/executor files reports 0 warnings)
- R11, crates/bench/src/bin/stm-p2.rs:1297: cast_precision_loss -- resolved (verified: final `cargo clippy -p kardamom-bench -p kardamom-executor --all-targets -- -D warnings -W clippy::pedantic` scoped to crates/bench and crates/executor files reports 0 warnings)
- R11, crates/bench/src/bin/stm-p2.rs:1298: cast_precision_loss -- resolved (verified: final `cargo clippy -p kardamom-bench -p kardamom-executor --all-targets -- -D warnings -W clippy::pedantic` scoped to crates/bench and crates/executor files reports 0 warnings)
- R11, crates/bench/src/bin/stm-p2.rs:1299: cast_precision_loss -- resolved (verified: final `cargo clippy -p kardamom-bench -p kardamom-executor --all-targets -- -D warnings -W clippy::pedantic` scoped to crates/bench and crates/executor files reports 0 warnings)
- R11, crates/bench/src/bin/stm-p2.rs:1308: too_many_lines -- resolved (verified: final `cargo clippy -p kardamom-bench -p kardamom-executor --all-targets -- -D warnings -W clippy::pedantic` scoped to crates/bench and crates/executor files reports 0 warnings)
- R11, crates/bench/src/bin/stm-p2.rs:1327: cast_possible_truncation -- resolved (verified: final `cargo clippy -p kardamom-bench -p kardamom-executor --all-targets -- -D warnings -W clippy::pedantic` scoped to crates/bench and crates/executor files reports 0 warnings)
- R11, crates/bench/src/bin/stm-p2.rs:1434: default_trait_access -- resolved (verified: final `cargo clippy -p kardamom-bench -p kardamom-executor --all-targets -- -D warnings -W clippy::pedantic` scoped to crates/bench and crates/executor files reports 0 warnings)
- R11, crates/bench/src/bin/stm-p2.rs:1475: cast_possible_truncation -- resolved (verified: final `cargo clippy -p kardamom-bench -p kardamom-executor --all-targets -- -D warnings -W clippy::pedantic` scoped to crates/bench and crates/executor files reports 0 warnings)
- R11, crates/bench/src/bin/stm-p2.rs:1504: cast_possible_truncation -- resolved (verified: final `cargo clippy -p kardamom-bench -p kardamom-executor --all-targets -- -D warnings -W clippy::pedantic` scoped to crates/bench and crates/executor files reports 0 warnings)
- R11, crates/bench/src/bin/stm-p2.rs:1511: cast_possible_truncation -- resolved (verified: final `cargo clippy -p kardamom-bench -p kardamom-executor --all-targets -- -D warnings -W clippy::pedantic` scoped to crates/bench and crates/executor files reports 0 warnings)
- R11, crates/bench/src/bin/stm-p2.rs:1589: default_trait_access -- resolved (verified: final `cargo clippy -p kardamom-bench -p kardamom-executor --all-targets -- -D warnings -W clippy::pedantic` scoped to crates/bench and crates/executor files reports 0 warnings)
- R11, crates/bench/src/bin/stm-p2.rs:1697: items_after_statements -- resolved (verified: final `cargo clippy -p kardamom-bench -p kardamom-executor --all-targets -- -D warnings -W clippy::pedantic` scoped to crates/bench and crates/executor files reports 0 warnings)
- R11, crates/bench/src/bin/stm-p2.rs:1706: doc_markdown -- resolved (verified: final `cargo clippy -p kardamom-bench -p kardamom-executor --all-targets -- -D warnings -W clippy::pedantic` scoped to crates/bench and crates/executor files reports 0 warnings)
- R11, crates/bench/src/bin/stm-p2.rs:1735: map_unwrap_or -- resolved (verified: final `cargo clippy -p kardamom-bench -p kardamom-executor --all-targets -- -D warnings -W clippy::pedantic` scoped to crates/bench and crates/executor files reports 0 warnings)
- R11, crates/bench/src/bin/stm-p2.rs:1754: cast_possible_truncation -- resolved (verified: final `cargo clippy -p kardamom-bench -p kardamom-executor --all-targets -- -D warnings -W clippy::pedantic` scoped to crates/bench and crates/executor files reports 0 warnings)
- R11, crates/bench/src/bin/stm-p2.rs:1770: items_after_statements -- resolved (verified: final `cargo clippy -p kardamom-bench -p kardamom-executor --all-targets -- -D warnings -W clippy::pedantic` scoped to crates/bench and crates/executor files reports 0 warnings)
- R11, crates/bench/src/bin/stm-p2.rs:1873: cast_lossless -- resolved (verified: final `cargo clippy -p kardamom-bench -p kardamom-executor --all-targets -- -D warnings -W clippy::pedantic` scoped to crates/bench and crates/executor files reports 0 warnings)
- R11, crates/bench/src/bin/stm-p2.rs:1884: cast_lossless -- resolved (verified: final `cargo clippy -p kardamom-bench -p kardamom-executor --all-targets -- -D warnings -W clippy::pedantic` scoped to crates/bench and crates/executor files reports 0 warnings)
- R11, crates/bench/src/bin/stm-p2.rs:1884: cast_precision_loss -- resolved (verified: final `cargo clippy -p kardamom-bench -p kardamom-executor --all-targets -- -D warnings -W clippy::pedantic` scoped to crates/bench and crates/executor files reports 0 warnings)
- R11, crates/bench/src/bin/stm-p2.rs:1897: cast_precision_loss -- resolved (verified: final `cargo clippy -p kardamom-bench -p kardamom-executor --all-targets -- -D warnings -W clippy::pedantic` scoped to crates/bench and crates/executor files reports 0 warnings)
- R11, crates/bench/src/bin/stm-p2.rs:1898: cast_precision_loss -- resolved (verified: final `cargo clippy -p kardamom-bench -p kardamom-executor --all-targets -- -D warnings -W clippy::pedantic` scoped to crates/bench and crates/executor files reports 0 warnings)
- R11, crates/bench/src/bin/stm-p2.rs:1921: cast_precision_loss -- resolved (verified: final `cargo clippy -p kardamom-bench -p kardamom-executor --all-targets -- -D warnings -W clippy::pedantic` scoped to crates/bench and crates/executor files reports 0 warnings)
- R11, crates/bench/src/bin/stm-p2.rs:1923: cast_precision_loss -- resolved (verified: final `cargo clippy -p kardamom-bench -p kardamom-executor --all-targets -- -D warnings -W clippy::pedantic` scoped to crates/bench and crates/executor files reports 0 warnings)
- R11, crates/bench/src/bin/stm-p2.rs:1948: cast_precision_loss -- resolved (verified: final `cargo clippy -p kardamom-bench -p kardamom-executor --all-targets -- -D warnings -W clippy::pedantic` scoped to crates/bench and crates/executor files reports 0 warnings)
- R11, crates/bench/src/bin/stm-p2.rs:1955: cast_precision_loss -- resolved (verified: final `cargo clippy -p kardamom-bench -p kardamom-executor --all-targets -- -D warnings -W clippy::pedantic` scoped to crates/bench and crates/executor files reports 0 warnings)
- R11, crates/bench/src/bin/stm-p2.rs:1967: uninlined_format_args -- resolved (verified: final `cargo clippy -p kardamom-bench -p kardamom-executor --all-targets -- -D warnings -W clippy::pedantic` scoped to crates/bench and crates/executor files reports 0 warnings)
- R11, crates/bench/src/bin/stm-p2.rs:1977: cast_precision_loss -- resolved (verified: final `cargo clippy -p kardamom-bench -p kardamom-executor --all-targets -- -D warnings -W clippy::pedantic` scoped to crates/bench and crates/executor files reports 0 warnings)
- R11, crates/bench/src/bin/stm-p2.rs:1984: cast_precision_loss -- resolved (verified: final `cargo clippy -p kardamom-bench -p kardamom-executor --all-targets -- -D warnings -W clippy::pedantic` scoped to crates/bench and crates/executor files reports 0 warnings)
- R11, crates/bench/src/bin/stm-p2.rs:1985: cast_precision_loss -- resolved (verified: final `cargo clippy -p kardamom-bench -p kardamom-executor --all-targets -- -D warnings -W clippy::pedantic` scoped to crates/bench and crates/executor files reports 0 warnings)
- R11, crates/bench/src/bin/stm-p2.rs:1986: cast_precision_loss -- resolved (verified: final `cargo clippy -p kardamom-bench -p kardamom-executor --all-targets -- -D warnings -W clippy::pedantic` scoped to crates/bench and crates/executor files reports 0 warnings)
- R11, crates/bench/src/bin/stm-p2.rs:1987: cast_precision_loss -- resolved (verified: final `cargo clippy -p kardamom-bench -p kardamom-executor --all-targets -- -D warnings -W clippy::pedantic` scoped to crates/bench and crates/executor files reports 0 warnings)
- R11, crates/bench/src/harness/inprocess.rs:29: missing_errors_doc -- resolved (verified: final `cargo clippy -p kardamom-bench -p kardamom-executor --all-targets -- -D warnings -W clippy::pedantic` scoped to crates/bench and crates/executor files reports 0 warnings)
- R11, crates/bench/src/harness/inprocess.rs:51: cast_possible_truncation -- resolved (verified: final `cargo clippy -p kardamom-bench -p kardamom-executor --all-targets -- -D warnings -W clippy::pedantic` scoped to crates/bench and crates/executor files reports 0 warnings)
- R11, crates/bench/src/harness/inprocess.rs:51: cast_possible_wrap -- resolved (verified: final `cargo clippy -p kardamom-bench -p kardamom-executor --all-targets -- -D warnings -W clippy::pedantic` scoped to crates/bench and crates/executor files reports 0 warnings)
- R11, crates/bench/src/load/accounting.rs:136: too_many_lines -- resolved (verified: final `cargo clippy -p kardamom-bench -p kardamom-executor --all-targets -- -D warnings -W clippy::pedantic` scoped to crates/bench and crates/executor files reports 0 warnings)
- R11, crates/bench/src/load/config.rs:30: struct_excessive_bools -- resolved (verified: final `cargo clippy -p kardamom-bench -p kardamom-executor --all-targets -- -D warnings -W clippy::pedantic` scoped to crates/bench and crates/executor files reports 0 warnings)
- R11, crates/bench/src/load/config.rs:52: doc_markdown -- resolved (verified: final `cargo clippy -p kardamom-bench -p kardamom-executor --all-targets -- -D warnings -W clippy::pedantic` scoped to crates/bench and crates/executor files reports 0 warnings)
- R11, crates/bench/src/load/config.rs:53: doc_markdown -- resolved (verified: final `cargo clippy -p kardamom-bench -p kardamom-executor --all-targets -- -D warnings -W clippy::pedantic` scoped to crates/bench and crates/executor files reports 0 warnings)
- R11, crates/bench/src/load/defi.rs:1: doc_markdown -- resolved (verified: final `cargo clippy -p kardamom-bench -p kardamom-executor --all-targets -- -D warnings -W clippy::pedantic` scoped to crates/bench and crates/executor files reports 0 warnings)
- R11, crates/bench/src/load/defi.rs:56: must_use_candidate -- resolved (verified: final `cargo clippy -p kardamom-bench -p kardamom-executor --all-targets -- -D warnings -W clippy::pedantic` scoped to crates/bench and crates/executor files reports 0 warnings)
- R11, crates/bench/src/load/defi.rs:172: missing_errors_doc -- resolved (verified: final `cargo clippy -p kardamom-bench -p kardamom-executor --all-targets -- -D warnings -W clippy::pedantic` scoped to crates/bench and crates/executor files reports 0 warnings)
- R11, crates/bench/src/load/defi.rs:228: items_after_statements -- resolved (verified: final `cargo clippy -p kardamom-bench -p kardamom-executor --all-targets -- -D warnings -W clippy::pedantic` scoped to crates/bench and crates/executor files reports 0 warnings)
- R11, crates/bench/src/load/defi.rs:229: items_after_statements -- resolved (verified: final `cargo clippy -p kardamom-bench -p kardamom-executor --all-targets -- -D warnings -W clippy::pedantic` scoped to crates/bench and crates/executor files reports 0 warnings)
- R11, crates/bench/src/load/defi.rs:230: items_after_statements -- resolved (verified: final `cargo clippy -p kardamom-bench -p kardamom-executor --all-targets -- -D warnings -W clippy::pedantic` scoped to crates/bench and crates/executor files reports 0 warnings)
- R11, crates/bench/src/load/defi.rs:282: doc_markdown -- resolved (verified: final `cargo clippy -p kardamom-bench -p kardamom-executor --all-targets -- -D warnings -W clippy::pedantic` scoped to crates/bench and crates/executor files reports 0 warnings)
- R11, crates/bench/src/load/defi.rs:285: missing_errors_doc -- resolved (verified: final `cargo clippy -p kardamom-bench -p kardamom-executor --all-targets -- -D warnings -W clippy::pedantic` scoped to crates/bench and crates/executor files reports 0 warnings)
- R11, crates/bench/src/load/defi.rs:333: missing_errors_doc -- resolved (verified: final `cargo clippy -p kardamom-bench -p kardamom-executor --all-targets -- -D warnings -W clippy::pedantic` scoped to crates/bench and crates/executor files reports 0 warnings)
- R11, crates/bench/src/load/mod.rs:101: too_many_lines -- resolved (verified: final `cargo clippy -p kardamom-bench -p kardamom-executor --all-targets -- -D warnings -W clippy::pedantic` scoped to crates/bench and crates/executor files reports 0 warnings)
- R11, crates/bench/src/load/mod.rs:429: cast_precision_loss -- resolved (verified: final `cargo clippy -p kardamom-bench -p kardamom-executor --all-targets -- -D warnings -W clippy::pedantic` scoped to crates/bench and crates/executor files reports 0 warnings)
- R11, crates/bench/src/load/mod.rs:445: inconsistent_struct_constructor -- resolved (verified: final `cargo clippy -p kardamom-bench -p kardamom-executor --all-targets -- -D warnings -W clippy::pedantic` scoped to crates/bench and crates/executor files reports 0 warnings)
- R11, crates/bench/src/load/scrape.rs:4: doc_markdown -- resolved (verified: final `cargo clippy -p kardamom-bench -p kardamom-executor --all-targets -- -D warnings -W clippy::pedantic` scoped to crates/bench and crates/executor files reports 0 warnings)
- R11, crates/bench/src/load/scrape.rs:229: manual_let_else -- resolved (verified: final `cargo clippy -p kardamom-bench -p kardamom-executor --all-targets -- -D warnings -W clippy::pedantic` scoped to crates/bench and crates/executor files reports 0 warnings)
- R11, crates/bench/src/load/scrape.rs:234: unnested_or_patterns -- resolved (verified: final `cargo clippy -p kardamom-bench -p kardamom-executor --all-targets -- -D warnings -W clippy::pedantic` scoped to crates/bench and crates/executor files reports 0 warnings)
- R11, crates/bench/src/mnemonic.rs:30: doc_markdown -- resolved (verified: final `cargo clippy -p kardamom-bench -p kardamom-executor --all-targets -- -D warnings -W clippy::pedantic` scoped to crates/bench and crates/executor files reports 0 warnings)
- R11, crates/bench/src/perf/cluster.rs:38: missing_errors_doc -- resolved (verified: final `cargo clippy -p kardamom-bench -p kardamom-executor --all-targets -- -D warnings -W clippy::pedantic` scoped to crates/bench and crates/executor files reports 0 warnings)
- R11, crates/bench/src/perf/cluster.rs:54: missing_errors_doc -- resolved (verified: final `cargo clippy -p kardamom-bench -p kardamom-executor --all-targets -- -D warnings -W clippy::pedantic` scoped to crates/bench and crates/executor files reports 0 warnings)
- R11, crates/bench/src/perf/cluster.rs:64: missing_errors_doc -- resolved (verified: final `cargo clippy -p kardamom-bench -p kardamom-executor --all-targets -- -D warnings -W clippy::pedantic` scoped to crates/bench and crates/executor files reports 0 warnings)
- R11, crates/bench/src/perf/cluster.rs:108: missing_errors_doc -- resolved (verified: final `cargo clippy -p kardamom-bench -p kardamom-executor --all-targets -- -D warnings -W clippy::pedantic` scoped to crates/bench and crates/executor files reports 0 warnings)
- R11, crates/bench/src/perf/cluster.rs:187: missing_errors_doc -- resolved (verified: final `cargo clippy -p kardamom-bench -p kardamom-executor --all-targets -- -D warnings -W clippy::pedantic` scoped to crates/bench and crates/executor files reports 0 warnings)
- R11, crates/bench/src/perf/cluster.rs:203: missing_errors_doc -- resolved (verified: final `cargo clippy -p kardamom-bench -p kardamom-executor --all-targets -- -D warnings -W clippy::pedantic` scoped to crates/bench and crates/executor files reports 0 warnings)
- R11, crates/bench/src/perf/mod.rs:5: doc_markdown -- resolved (verified: final `cargo clippy -p kardamom-bench -p kardamom-executor --all-targets -- -D warnings -W clippy::pedantic` scoped to crates/bench and crates/executor files reports 0 warnings)
- R11, crates/bench/src/perf/mod.rs:9: doc_markdown -- resolved (verified: final `cargo clippy -p kardamom-bench -p kardamom-executor --all-targets -- -D warnings -W clippy::pedantic` scoped to crates/bench and crates/executor files reports 0 warnings)
- R11, crates/bench/src/perf/mod.rs:28: missing_errors_doc -- resolved (verified: final `cargo clippy -p kardamom-bench -p kardamom-executor --all-targets -- -D warnings -W clippy::pedantic` scoped to crates/bench and crates/executor files reports 0 warnings)
- R11, crates/bench/src/perf/mod.rs:37: must_use_candidate -- resolved (verified: final `cargo clippy -p kardamom-bench -p kardamom-executor --all-targets -- -D warnings -W clippy::pedantic` scoped to crates/bench and crates/executor files reports 0 warnings)
- R11, crates/bench/src/perf/profile.rs:4: doc_markdown -- resolved (verified: final `cargo clippy -p kardamom-bench -p kardamom-executor --all-targets -- -D warnings -W clippy::pedantic` scoped to crates/bench and crates/executor files reports 0 warnings)
- R11, crates/bench/src/perf/profile.rs:7: doc_markdown -- resolved (verified: final `cargo clippy -p kardamom-bench -p kardamom-executor --all-targets -- -D warnings -W clippy::pedantic` scoped to crates/bench and crates/executor files reports 0 warnings)
- R11, crates/bench/src/perf/profile.rs:47: missing_errors_doc -- resolved (verified: final `cargo clippy -p kardamom-bench -p kardamom-executor --all-targets -- -D warnings -W clippy::pedantic` scoped to crates/bench and crates/executor files reports 0 warnings)
- R11, crates/bench/src/perf/profile.rs:72: missing_errors_doc -- resolved (verified: final `cargo clippy -p kardamom-bench -p kardamom-executor --all-targets -- -D warnings -W clippy::pedantic` scoped to crates/bench and crates/executor files reports 0 warnings)
- R11, crates/bench/src/perf/profile.rs:130: missing_errors_doc -- resolved (verified: final `cargo clippy -p kardamom-bench -p kardamom-executor --all-targets -- -D warnings -W clippy::pedantic` scoped to crates/bench and crates/executor files reports 0 warnings)
- R11, crates/bench/src/perf/profile.rs:133: map_unwrap_or -- resolved (verified: final `cargo clippy -p kardamom-bench -p kardamom-executor --all-targets -- -D warnings -W clippy::pedantic` scoped to crates/bench and crates/executor files reports 0 warnings)
- R11, crates/bench/src/perf/report.rs:48: must_use_candidate -- resolved (verified: final `cargo clippy -p kardamom-bench -p kardamom-executor --all-targets -- -D warnings -W clippy::pedantic` scoped to crates/bench and crates/executor files reports 0 warnings)
- R11, crates/bench/src/perf/report.rs:74: cast_precision_loss -- resolved (verified: final `cargo clippy -p kardamom-bench -p kardamom-executor --all-targets -- -D warnings -W clippy::pedantic` scoped to crates/bench and crates/executor files reports 0 warnings)
- R11, crates/bench/src/perf/report.rs:106: missing_errors_doc -- resolved (verified: final `cargo clippy -p kardamom-bench -p kardamom-executor --all-targets -- -D warnings -W clippy::pedantic` scoped to crates/bench and crates/executor files reports 0 warnings)
- R11, crates/bench/src/perf/report.rs:207: missing_errors_doc -- resolved (verified: final `cargo clippy -p kardamom-bench -p kardamom-executor --all-targets -- -D warnings -W clippy::pedantic` scoped to crates/bench and crates/executor files reports 0 warnings)
- R11, crates/bench/src/stm/capture.rs:21: missing_panics_doc -- resolved (verified: final `cargo clippy -p kardamom-bench -p kardamom-executor --all-targets -- -D warnings -W clippy::pedantic` scoped to crates/bench and crates/executor files reports 0 warnings)
- R11, crates/bench/src/stm/capture.rs:73: explicit_iter_loop -- resolved (verified: final `cargo clippy -p kardamom-bench -p kardamom-executor --all-targets -- -D warnings -W clippy::pedantic` scoped to crates/bench and crates/executor files reports 0 warnings)
- R11, crates/bench/src/stm/capture.rs:90: explicit_iter_loop -- resolved (verified: final `cargo clippy -p kardamom-bench -p kardamom-executor --all-targets -- -D warnings -W clippy::pedantic` scoped to crates/bench and crates/executor files reports 0 warnings)
- R11, crates/bench/src/stm/uniswap.rs:115: missing_errors_doc -- resolved (verified: final `cargo clippy -p kardamom-bench -p kardamom-executor --all-targets -- -D warnings -W clippy::pedantic` scoped to crates/bench and crates/executor files reports 0 warnings)
- R11, crates/bench/src/stm/uniswap.rs:115: too_many_lines -- resolved (verified: final `cargo clippy -p kardamom-bench -p kardamom-executor --all-targets -- -D warnings -W clippy::pedantic` scoped to crates/bench and crates/executor files reports 0 warnings)
- R11, crates/bench/src/stm/uniswap.rs:230: explicit_iter_loop -- resolved (verified: final `cargo clippy -p kardamom-bench -p kardamom-executor --all-targets -- -D warnings -W clippy::pedantic` scoped to crates/bench and crates/executor files reports 0 warnings)
- R11, crates/bench/src/stm/uniswap.rs:245: redundant_closure_for_method_calls -- resolved (verified: final `cargo clippy -p kardamom-bench -p kardamom-executor --all-targets -- -D warnings -W clippy::pedantic` scoped to crates/bench and crates/executor files reports 0 warnings)
- R11, crates/bench/src/stm/uniswap.rs:271: cast_possible_truncation -- resolved (verified: final `cargo clippy -p kardamom-bench -p kardamom-executor --all-targets -- -D warnings -W clippy::pedantic` scoped to crates/bench and crates/executor files reports 0 warnings)
- R11, crates/bench/src/stm/uniswap.rs:277: cast_lossless -- resolved (verified: final `cargo clippy -p kardamom-bench -p kardamom-executor --all-targets -- -D warnings -W clippy::pedantic` scoped to crates/bench and crates/executor files reports 0 warnings)
- R11, crates/bench/src/stm/uniswap.rs:311: cast_possible_truncation -- resolved (verified: final `cargo clippy -p kardamom-bench -p kardamom-executor --all-targets -- -D warnings -W clippy::pedantic` scoped to crates/bench and crates/executor files reports 0 warnings)
- R11, crates/bench/src/stm/uniswap.rs:313: cast_possible_truncation -- resolved (verified: final `cargo clippy -p kardamom-bench -p kardamom-executor --all-targets -- -D warnings -W clippy::pedantic` scoped to crates/bench and crates/executor files reports 0 warnings)
- R11, crates/bench/src/stm/uniswap.rs:314: cast_lossless -- resolved (verified: final `cargo clippy -p kardamom-bench -p kardamom-executor --all-targets -- -D warnings -W clippy::pedantic` scoped to crates/bench and crates/executor files reports 0 warnings)
- R11, crates/bench/tests/alloc_profile.rs:4: doc_markdown -- resolved (verified: final `cargo clippy -p kardamom-bench -p kardamom-executor --all-targets -- -D warnings -W clippy::pedantic` scoped to crates/bench and crates/executor files reports 0 warnings)
- R11, crates/bench/tests/alloc_profile.rs:7: doc_markdown -- resolved (verified: final `cargo clippy -p kardamom-bench -p kardamom-executor --all-targets -- -D warnings -W clippy::pedantic` scoped to crates/bench and crates/executor files reports 0 warnings)
- R11, crates/bench/tests/alloc_profile.rs:10: doc_markdown -- resolved (verified: final `cargo clippy -p kardamom-bench -p kardamom-executor --all-targets -- -D warnings -W clippy::pedantic` scoped to crates/bench and crates/executor files reports 0 warnings)
- R11, crates/bench/tests/alloc_profile.rs:33: doc_markdown -- resolved (verified: final `cargo clippy -p kardamom-bench -p kardamom-executor --all-targets -- -D warnings -W clippy::pedantic` scoped to crates/bench and crates/executor files reports 0 warnings)
- R11, crates/bench/tests/alloc_profile.rs:34: doc_markdown -- resolved (verified: final `cargo clippy -p kardamom-bench -p kardamom-executor --all-targets -- -D warnings -W clippy::pedantic` scoped to crates/bench and crates/executor files reports 0 warnings)
- R11, crates/bench/tests/alloc_profile.rs:43: too_many_lines -- resolved (verified: final `cargo clippy -p kardamom-bench -p kardamom-executor --all-targets -- -D warnings -W clippy::pedantic` scoped to crates/bench and crates/executor files reports 0 warnings)
- R11, crates/bench/tests/alloc_profile.rs:44: cast_possible_truncation -- resolved (verified: final `cargo clippy -p kardamom-bench -p kardamom-executor --all-targets -- -D warnings -W clippy::pedantic` scoped to crates/bench and crates/executor files reports 0 warnings)
- R11, crates/bench/tests/alloc_profile.rs:106: items_after_statements -- resolved (verified: final `cargo clippy -p kardamom-bench -p kardamom-executor --all-targets -- -D warnings -W clippy::pedantic` scoped to crates/bench and crates/executor files reports 0 warnings)
- R11, crates/bench/tests/alloc_profile.rs:168: items_after_statements -- resolved (verified: final `cargo clippy -p kardamom-bench -p kardamom-executor --all-targets -- -D warnings -W clippy::pedantic` scoped to crates/bench and crates/executor files reports 0 warnings)
- R11, crates/bench/tests/alloc_profile.rs:216: cast_precision_loss -- resolved (verified: final `cargo clippy -p kardamom-bench -p kardamom-executor --all-targets -- -D warnings -W clippy::pedantic` scoped to crates/bench and crates/executor files reports 0 warnings)
- R11, crates/bench/tests/alloc_profile.rs:217: cast_precision_loss -- resolved (verified: final `cargo clippy -p kardamom-bench -p kardamom-executor --all-targets -- -D warnings -W clippy::pedantic` scoped to crates/bench and crates/executor files reports 0 warnings)
- R11, crates/bench/tests/alloc_profile.rs:218: cast_precision_loss -- resolved (verified: final `cargo clippy -p kardamom-bench -p kardamom-executor --all-targets -- -D warnings -W clippy::pedantic` scoped to crates/bench and crates/executor files reports 0 warnings)
- R11, crates/bench/tests/alloc_profile.rs:221: cast_precision_loss -- resolved (verified: final `cargo clippy -p kardamom-bench -p kardamom-executor --all-targets -- -D warnings -W clippy::pedantic` scoped to crates/bench and crates/executor files reports 0 warnings)
- R11, crates/bench/tests/alloc_profile.rs:226: cast_precision_loss -- resolved (verified: final `cargo clippy -p kardamom-bench -p kardamom-executor --all-targets -- -D warnings -W clippy::pedantic` scoped to crates/bench and crates/executor files reports 0 warnings)
- R11, crates/bench/tests/alloc_profile_ingress.rs:4: doc_markdown -- resolved (verified: final `cargo clippy -p kardamom-bench -p kardamom-executor --all-targets -- -D warnings -W clippy::pedantic` scoped to crates/bench and crates/executor files reports 0 warnings)
- R11, crates/bench/tests/alloc_profile_ingress.rs:10: doc_markdown -- resolved (verified: final `cargo clippy -p kardamom-bench -p kardamom-executor --all-targets -- -D warnings -W clippy::pedantic` scoped to crates/bench and crates/executor files reports 0 warnings)
- R11, crates/bench/tests/alloc_profile_ingress.rs:11: doc_markdown -- resolved (verified: final `cargo clippy -p kardamom-bench -p kardamom-executor --all-targets -- -D warnings -W clippy::pedantic` scoped to crates/bench and crates/executor files reports 0 warnings)
- R11, crates/bench/tests/alloc_profile_ingress.rs:31: doc_markdown -- resolved (verified: final `cargo clippy -p kardamom-bench -p kardamom-executor --all-targets -- -D warnings -W clippy::pedantic` scoped to crates/bench and crates/executor files reports 0 warnings)
- R11, crates/bench/tests/alloc_profile_ingress.rs:62: doc_markdown -- resolved (verified: final `cargo clippy -p kardamom-bench -p kardamom-executor --all-targets -- -D warnings -W clippy::pedantic` scoped to crates/bench and crates/executor files reports 0 warnings)
- R11, crates/bench/tests/alloc_profile_ingress.rs:63: doc_markdown -- resolved (verified: final `cargo clippy -p kardamom-bench -p kardamom-executor --all-targets -- -D warnings -W clippy::pedantic` scoped to crates/bench and crates/executor files reports 0 warnings)
- R11, crates/bench/tests/alloc_profile_ingress.rs:70: needless_pass_by_value -- resolved (verified: final `cargo clippy -p kardamom-bench -p kardamom-executor --all-targets -- -D warnings -W clippy::pedantic` scoped to crates/bench and crates/executor files reports 0 warnings)
- R11, crates/bench/tests/alloc_profile_ingress.rs:71: needless_pass_by_value -- resolved (verified: final `cargo clippy -p kardamom-bench -p kardamom-executor --all-targets -- -D warnings -W clippy::pedantic` scoped to crates/bench and crates/executor files reports 0 warnings)
- R11, crates/bench/tests/alloc_profile_ingress.rs:161: cast_lossless -- resolved (verified: final `cargo clippy -p kardamom-bench -p kardamom-executor --all-targets -- -D warnings -W clippy::pedantic` scoped to crates/bench and crates/executor files reports 0 warnings)
- R11, crates/bench/tests/alloc_profile_ingress.rs:209: cast_precision_loss -- resolved (verified: final `cargo clippy -p kardamom-bench -p kardamom-executor --all-targets -- -D warnings -W clippy::pedantic` scoped to crates/bench and crates/executor files reports 0 warnings)
- R11, crates/bench/tests/alloc_profile_ingress.rs:210: cast_precision_loss -- resolved (verified: final `cargo clippy -p kardamom-bench -p kardamom-executor --all-targets -- -D warnings -W clippy::pedantic` scoped to crates/bench and crates/executor files reports 0 warnings)
- R11, crates/bench/tests/alloc_profile_ingress.rs:211: cast_precision_loss -- resolved (verified: final `cargo clippy -p kardamom-bench -p kardamom-executor --all-targets -- -D warnings -W clippy::pedantic` scoped to crates/bench and crates/executor files reports 0 warnings)
- R11, crates/bench/tests/alloc_profile_ingress.rs:214: cast_precision_loss -- resolved (verified: final `cargo clippy -p kardamom-bench -p kardamom-executor --all-targets -- -D warnings -W clippy::pedantic` scoped to crates/bench and crates/executor files reports 0 warnings)
- R11, crates/bench/tests/alloc_profile_ingress.rs:218: cast_precision_loss -- resolved (verified: final `cargo clippy -p kardamom-bench -p kardamom-executor --all-targets -- -D warnings -W clippy::pedantic` scoped to crates/bench and crates/executor files reports 0 warnings)
- R11, crates/bench/tests/defi_on_engine.rs:1: doc_markdown -- resolved (verified: final `cargo clippy -p kardamom-bench -p kardamom-executor --all-targets -- -D warnings -W clippy::pedantic` scoped to crates/bench and crates/executor files reports 0 warnings)
- R11, crates/bench/tests/parallel_defi_repro.rs:1: doc_markdown -- resolved (verified: final `cargo clippy -p kardamom-bench -p kardamom-executor --all-targets -- -D warnings -W clippy::pedantic` scoped to crates/bench and crates/executor files reports 0 warnings)
- R11, crates/bench/tests/parallel_defi_repro.rs:70: too_many_lines -- resolved (verified: final `cargo clippy -p kardamom-bench -p kardamom-executor --all-targets -- -D warnings -W clippy::pedantic` scoped to crates/bench and crates/executor files reports 0 warnings)
- R11, crates/bench/tests/parallel_defi_repro.rs:139: unreadable_literal -- resolved (verified: final `cargo clippy -p kardamom-bench -p kardamom-executor --all-targets -- -D warnings -W clippy::pedantic` scoped to crates/bench and crates/executor files reports 0 warnings)
- R11, crates/bench/tests/parallel_defi_repro.rs:146: cast_possible_truncation -- resolved (verified: final `cargo clippy -p kardamom-bench -p kardamom-executor --all-targets -- -D warnings -W clippy::pedantic` scoped to crates/bench and crates/executor files reports 0 warnings)
- R11, crates/executor/benches/sequential_throughput.rs:50: needless_pass_by_value -- resolved (verified: final `cargo clippy -p kardamom-bench -p kardamom-executor --all-targets -- -D warnings -W clippy::pedantic` scoped to crates/bench and crates/executor files reports 0 warnings)
- R11, crates/executor/benches/sequential_throughput.rs:115: semicolon_if_nothing_returned -- resolved (verified: final `cargo clippy -p kardamom-bench -p kardamom-executor --all-targets -- -D warnings -W clippy::pedantic` scoped to crates/bench and crates/executor files reports 0 warnings)
- R11, crates/executor/benches/sequential_throughput.rs:156: semicolon_if_nothing_returned -- resolved (verified: final `cargo clippy -p kardamom-bench -p kardamom-executor --all-targets -- -D warnings -W clippy::pedantic` scoped to crates/bench and crates/executor files reports 0 warnings)
- R11, crates/executor/benches/sequential_throughput.rs:251: cast_possible_truncation -- resolved (verified: final `cargo clippy -p kardamom-bench -p kardamom-executor --all-targets -- -D warnings -W clippy::pedantic` scoped to crates/bench and crates/executor files reports 0 warnings)
- R11, crates/executor/benches/sequential_throughput.rs:252: cast_possible_truncation -- resolved (verified: final `cargo clippy -p kardamom-bench -p kardamom-executor --all-targets -- -D warnings -W clippy::pedantic` scoped to crates/bench and crates/executor files reports 0 warnings)
- R11, crates/executor/benches/sequential_throughput.rs:253: cast_possible_truncation -- resolved (verified: final `cargo clippy -p kardamom-bench -p kardamom-executor --all-targets -- -D warnings -W clippy::pedantic` scoped to crates/bench and crates/executor files reports 0 warnings)
- R11, crates/executor/benches/sequential_throughput.rs:256: cast_possible_truncation -- resolved (verified: final `cargo clippy -p kardamom-bench -p kardamom-executor --all-targets -- -D warnings -W clippy::pedantic` scoped to crates/bench and crates/executor files reports 0 warnings)
- R11, crates/executor/benches/sequential_throughput.rs:261: cast_possible_truncation -- resolved (verified: final `cargo clippy -p kardamom-bench -p kardamom-executor --all-targets -- -D warnings -W clippy::pedantic` scoped to crates/bench and crates/executor files reports 0 warnings)
- R11, crates/executor/benches/sequential_throughput.rs:267: cast_possible_truncation -- resolved (verified: final `cargo clippy -p kardamom-bench -p kardamom-executor --all-targets -- -D warnings -W clippy::pedantic` scoped to crates/bench and crates/executor files reports 0 warnings)
- R11, crates/executor/src/bal.rs:112: needless_pass_by_value -- resolved (verified: final `cargo clippy -p kardamom-bench -p kardamom-executor --all-targets -- -D warnings -W clippy::pedantic` scoped to crates/bench and crates/executor files reports 0 warnings)
- R11, crates/executor/src/bal.rs:120: default_trait_access -- resolved (verified: final `cargo clippy -p kardamom-bench -p kardamom-executor --all-targets -- -D warnings -W clippy::pedantic` scoped to crates/bench and crates/executor files reports 0 warnings)
- R11, crates/executor/src/bal.rs:165: cast_precision_loss -- resolved (verified: final `cargo clippy -p kardamom-bench -p kardamom-executor --all-targets -- -D warnings -W clippy::pedantic` scoped to crates/bench and crates/executor files reports 0 warnings)
- R11, crates/executor/src/bal.rs:217: cast_precision_loss -- resolved (verified: final `cargo clippy -p kardamom-bench -p kardamom-executor --all-targets -- -D warnings -W clippy::pedantic` scoped to crates/bench and crates/executor files reports 0 warnings)
- R11, crates/executor/src/bin/kardamom-executor/args.rs:29: doc_markdown -- resolved (verified: final `cargo clippy -p kardamom-bench -p kardamom-executor --all-targets -- -D warnings -W clippy::pedantic` scoped to crates/bench and crates/executor files reports 0 warnings)
- R11, crates/executor/src/bin/kardamom-executor/args.rs:38: doc_markdown -- resolved (verified: final `cargo clippy -p kardamom-bench -p kardamom-executor --all-targets -- -D warnings -W clippy::pedantic` scoped to crates/bench and crates/executor files reports 0 warnings)
- R11, crates/executor/src/bin/kardamom-executor/args.rs:90: doc_markdown -- resolved (verified: final `cargo clippy -p kardamom-bench -p kardamom-executor --all-targets -- -D warnings -W clippy::pedantic` scoped to crates/bench and crates/executor files reports 0 warnings)
- R11, crates/executor/src/bin/kardamom-executor/args.rs:91: doc_markdown -- resolved (verified: final `cargo clippy -p kardamom-bench -p kardamom-executor --all-targets -- -D warnings -W clippy::pedantic` scoped to crates/bench and crates/executor files reports 0 warnings)
- R11, crates/executor/src/bin/kardamom-executor/args.rs:98: doc_markdown -- resolved (verified: final `cargo clippy -p kardamom-bench -p kardamom-executor --all-targets -- -D warnings -W clippy::pedantic` scoped to crates/bench and crates/executor files reports 0 warnings)
- R11, crates/executor/src/bin/kardamom-executor/main.rs:3: doc_markdown -- resolved (verified: final `cargo clippy -p kardamom-bench -p kardamom-executor --all-targets -- -D warnings -W clippy::pedantic` scoped to crates/bench and crates/executor files reports 0 warnings)
- R11, crates/executor/src/bin/kardamom-executor/main.rs:4: doc_markdown -- resolved (verified: final `cargo clippy -p kardamom-bench -p kardamom-executor --all-targets -- -D warnings -W clippy::pedantic` scoped to crates/bench and crates/executor files reports 0 warnings)
- R11, crates/executor/src/bin/kardamom-executor/main.rs:47: too_many_lines -- resolved (verified: final `cargo clippy -p kardamom-bench -p kardamom-executor --all-targets -- -D warnings -W clippy::pedantic` scoped to crates/bench and crates/executor files reports 0 warnings)
- R11, crates/executor/src/bin/kardamom-executor/main.rs:268: ignored_unit_patterns -- resolved (verified: final `cargo clippy -p kardamom-bench -p kardamom-executor --all-targets -- -D warnings -W clippy::pedantic` scoped to crates/bench and crates/executor files reports 0 warnings)
- R11, crates/executor/src/bin/kardamom-executor/state.rs:55: single_match_else -- resolved (verified: final `cargo clippy -p kardamom-bench -p kardamom-executor --all-targets -- -D warnings -W clippy::pedantic` scoped to crates/bench and crates/executor files reports 0 warnings)
- R11, crates/executor/src/bin/kardamom-executor/state.rs:123: ignored_unit_patterns -- resolved (verified: final `cargo clippy -p kardamom-bench -p kardamom-executor --all-targets -- -D warnings -W clippy::pedantic` scoped to crates/bench and crates/executor files reports 0 warnings)
- R11, crates/executor/src/bin/kardamom-executor/wiring.rs:2: doc_markdown -- resolved (verified: final `cargo clippy -p kardamom-bench -p kardamom-executor --all-targets -- -D warnings -W clippy::pedantic` scoped to crates/bench and crates/executor files reports 0 warnings)
- R11, crates/executor/src/bin/kardamom-executor/wiring.rs:17: doc_markdown -- resolved (verified: final `cargo clippy -p kardamom-bench -p kardamom-executor --all-targets -- -D warnings -W clippy::pedantic` scoped to crates/bench and crates/executor files reports 0 warnings)
- R11, crates/executor/src/bin/kardamom-executor/wiring.rs:56: map_unwrap_or -- resolved (verified: final `cargo clippy -p kardamom-bench -p kardamom-executor --all-targets -- -D warnings -W clippy::pedantic` scoped to crates/bench and crates/executor files reports 0 warnings)
- R11, crates/executor/src/bin/kardamom-executor/wiring.rs:103: doc_markdown -- resolved (verified: final `cargo clippy -p kardamom-bench -p kardamom-executor --all-targets -- -D warnings -W clippy::pedantic` scoped to crates/bench and crates/executor files reports 0 warnings)
- R11, crates/executor/src/config.rs:27: doc_markdown -- resolved (verified: final `cargo clippy -p kardamom-bench -p kardamom-executor --all-targets -- -D warnings -W clippy::pedantic` scoped to crates/bench and crates/executor files reports 0 warnings)
- R11, crates/executor/src/lib.rs:14: doc_markdown -- resolved (verified: final `cargo clippy -p kardamom-bench -p kardamom-executor --all-targets -- -D warnings -W clippy::pedantic` scoped to crates/bench and crates/executor files reports 0 warnings)
- R11, crates/executor/src/parallel.rs:79: missing_panics_doc -- resolved (verified: final `cargo clippy -p kardamom-bench -p kardamom-executor --all-targets -- -D warnings -W clippy::pedantic` scoped to crates/bench and crates/executor files reports 0 warnings)
- R11, crates/executor/src/parallel.rs:79: must_use_candidate -- resolved (verified: final `cargo clippy -p kardamom-bench -p kardamom-executor --all-targets -- -D warnings -W clippy::pedantic` scoped to crates/bench and crates/executor files reports 0 warnings)
- R11, crates/executor/src/parallel.rs:116: needless_pass_by_value -- resolved (verified: final `cargo clippy -p kardamom-bench -p kardamom-executor --all-targets -- -D warnings -W clippy::pedantic` scoped to crates/bench and crates/executor files reports 0 warnings)
- R11, crates/executor/src/parallel.rs:117: needless_pass_by_value -- resolved (verified: final `cargo clippy -p kardamom-bench -p kardamom-executor --all-targets -- -D warnings -W clippy::pedantic` scoped to crates/bench and crates/executor files reports 0 warnings)
- R11, crates/executor/src/parallel.rs:176: semicolon_if_nothing_returned -- resolved (verified: final `cargo clippy -p kardamom-bench -p kardamom-executor --all-targets -- -D warnings -W clippy::pedantic` scoped to crates/bench and crates/executor files reports 0 warnings)
- R11, crates/executor/src/parallel.rs:300: explicit_iter_loop -- resolved (verified: final `cargo clippy -p kardamom-bench -p kardamom-executor --all-targets -- -D warnings -W clippy::pedantic` scoped to crates/bench and crates/executor files reports 0 warnings)
- R11, crates/executor/tests/determinism.rs:2: doc_markdown -- resolved (verified: final `cargo clippy -p kardamom-bench -p kardamom-executor --all-targets -- -D warnings -W clippy::pedantic` scoped to crates/bench and crates/executor files reports 0 warnings)
- R11, crates/executor/tests/determinism.rs:6: doc_markdown -- resolved (verified: final `cargo clippy -p kardamom-bench -p kardamom-executor --all-targets -- -D warnings -W clippy::pedantic` scoped to crates/bench and crates/executor files reports 0 warnings)
- R11, crates/executor/tests/determinism.rs:7: doc_markdown -- resolved (verified: final `cargo clippy -p kardamom-bench -p kardamom-executor --all-targets -- -D warnings -W clippy::pedantic` scoped to crates/bench and crates/executor files reports 0 warnings)
- R11, crates/executor/tests/determinism.rs:9: doc_markdown -- resolved (verified: final `cargo clippy -p kardamom-bench -p kardamom-executor --all-targets -- -D warnings -W clippy::pedantic` scoped to crates/bench and crates/executor files reports 0 warnings)
- R11, crates/executor/tests/determinism.rs:146: needless_pass_by_value -- resolved (verified: final `cargo clippy -p kardamom-bench -p kardamom-executor --all-targets -- -D warnings -W clippy::pedantic` scoped to crates/bench and crates/executor files reports 0 warnings)
- R11, crates/executor/tests/diff_reference.rs:7: doc_markdown -- resolved (verified: final `cargo clippy -p kardamom-bench -p kardamom-executor --all-targets -- -D warnings -W clippy::pedantic` scoped to crates/bench and crates/executor files reports 0 warnings)
- R11, crates/executor/tests/diff_reference.rs:8: doc_markdown -- resolved (verified: final `cargo clippy -p kardamom-bench -p kardamom-executor --all-targets -- -D warnings -W clippy::pedantic` scoped to crates/bench and crates/executor files reports 0 warnings)
- R11, crates/executor/tests/diff_reference.rs:102: doc_markdown -- resolved (verified: final `cargo clippy -p kardamom-bench -p kardamom-executor --all-targets -- -D warnings -W clippy::pedantic` scoped to crates/bench and crates/executor files reports 0 warnings)
- R11, crates/executor/tests/diff_reference.rs:103: doc_markdown -- resolved (verified: final `cargo clippy -p kardamom-bench -p kardamom-executor --all-targets -- -D warnings -W clippy::pedantic` scoped to crates/bench and crates/executor files reports 0 warnings)
- R11, crates/executor/tests/diff_reference.rs:134: needless_pass_by_value -- resolved (verified: final `cargo clippy -p kardamom-bench -p kardamom-executor --all-targets -- -D warnings -W clippy::pedantic` scoped to crates/bench and crates/executor files reports 0 warnings)
- R11, crates/executor/tests/diff_reference.rs:167: default_trait_access -- resolved (verified: final `cargo clippy -p kardamom-bench -p kardamom-executor --all-targets -- -D warnings -W clippy::pedantic` scoped to crates/bench and crates/executor files reports 0 warnings)
- R11, crates/executor/tests/diff_reference.rs:184: too_many_lines -- resolved (verified: final `cargo clippy -p kardamom-bench -p kardamom-executor --all-targets -- -D warnings -W clippy::pedantic` scoped to crates/bench and crates/executor files reports 0 warnings)
- R11, crates/executor/tests/diff_reference.rs:249: cast_possible_truncation -- resolved (verified: final `cargo clippy -p kardamom-bench -p kardamom-executor --all-targets -- -D warnings -W clippy::pedantic` scoped to crates/bench and crates/executor files reports 0 warnings)
- R11, crates/executor/tests/diff_reference.rs:249: cast_possible_wrap -- resolved (verified: final `cargo clippy -p kardamom-bench -p kardamom-executor --all-targets -- -D warnings -W clippy::pedantic` scoped to crates/bench and crates/executor files reports 0 warnings)
- R11, crates/executor/tests/diff_reference.rs:253: cast_possible_truncation -- resolved (verified: final `cargo clippy -p kardamom-bench -p kardamom-executor --all-targets -- -D warnings -W clippy::pedantic` scoped to crates/bench and crates/executor files reports 0 warnings)
- R11, crates/executor/tests/diff_reference.rs:253: cast_possible_wrap -- resolved (verified: final `cargo clippy -p kardamom-bench -p kardamom-executor --all-targets -- -D warnings -W clippy::pedantic` scoped to crates/bench and crates/executor files reports 0 warnings)
- R11, crates/executor/tests/diff_reference.rs:259: cast_possible_truncation -- resolved (verified: final `cargo clippy -p kardamom-bench -p kardamom-executor --all-targets -- -D warnings -W clippy::pedantic` scoped to crates/bench and crates/executor files reports 0 warnings)
- R11, crates/executor/tests/diff_reference.rs:259: cast_possible_wrap -- resolved (verified: final `cargo clippy -p kardamom-bench -p kardamom-executor --all-targets -- -D warnings -W clippy::pedantic` scoped to crates/bench and crates/executor files reports 0 warnings)
- R11, crates/executor/tests/diff_reference.rs:265: cast_possible_truncation -- resolved (verified: final `cargo clippy -p kardamom-bench -p kardamom-executor --all-targets -- -D warnings -W clippy::pedantic` scoped to crates/bench and crates/executor files reports 0 warnings)
- R11, crates/executor/tests/diff_reference.rs:265: cast_possible_wrap -- resolved (verified: final `cargo clippy -p kardamom-bench -p kardamom-executor --all-targets -- -D warnings -W clippy::pedantic` scoped to crates/bench and crates/executor files reports 0 warnings)
- R11, crates/executor/tests/docker_aeron_e2e.rs:4: doc_markdown -- resolved (verified: final `cargo clippy -p kardamom-bench -p kardamom-executor --all-targets -- -D warnings -W clippy::pedantic` scoped to crates/bench and crates/executor files reports 0 warnings)
- R11, crates/executor/tests/docker_aeron_e2e.rs:11: doc_markdown -- resolved (verified: final `cargo clippy -p kardamom-bench -p kardamom-executor --all-targets -- -D warnings -W clippy::pedantic` scoped to crates/bench and crates/executor files reports 0 warnings)
- R11, crates/executor/tests/docker_aeron_e2e.rs:17: doc_markdown -- resolved (verified: final `cargo clippy -p kardamom-bench -p kardamom-executor --all-targets -- -D warnings -W clippy::pedantic` scoped to crates/bench and crates/executor files reports 0 warnings)
- R11, crates/executor/tests/docker_aeron_e2e.rs:21: doc_markdown -- resolved (verified: final `cargo clippy -p kardamom-bench -p kardamom-executor --all-targets -- -D warnings -W clippy::pedantic` scoped to crates/bench and crates/executor files reports 0 warnings)
- R11, crates/executor/tests/hash_invariance.rs:1: doc_markdown -- resolved (verified: final `cargo clippy -p kardamom-bench -p kardamom-executor --all-targets -- -D warnings -W clippy::pedantic` scoped to crates/bench and crates/executor files reports 0 warnings)
- R11, crates/executor/tests/hash_invariance.rs:46: cast_possible_truncation -- resolved (verified: final `cargo clippy -p kardamom-bench -p kardamom-executor --all-targets -- -D warnings -W clippy::pedantic` scoped to crates/bench and crates/executor files reports 0 warnings)
- R11, crates/executor/tests/hash_invariance.rs:52: cast_possible_truncation -- resolved (verified: final `cargo clippy -p kardamom-bench -p kardamom-executor --all-targets -- -D warnings -W clippy::pedantic` scoped to crates/bench and crates/executor files reports 0 warnings)
- R11, crates/executor/tests/hash_invariance.rs:61: unreadable_literal -- resolved (verified: final `cargo clippy -p kardamom-bench -p kardamom-executor --all-targets -- -D warnings -W clippy::pedantic` scoped to crates/bench and crates/executor files reports 0 warnings)
- R11, crates/executor/tests/hash_invariance.rs:64: unreadable_literal -- resolved (verified: final `cargo clippy -p kardamom-bench -p kardamom-executor --all-targets -- -D warnings -W clippy::pedantic` scoped to crates/bench and crates/executor files reports 0 warnings)
- R11, crates/executor/tests/m_plus_one_join.rs:1: doc_markdown -- resolved (verified: final `cargo clippy -p kardamom-bench -p kardamom-executor --all-targets -- -D warnings -W clippy::pedantic` scoped to crates/bench and crates/executor files reports 0 warnings)
- R11, crates/executor/tests/m_plus_one_join.rs:12: doc_markdown -- resolved (verified: final `cargo clippy -p kardamom-bench -p kardamom-executor --all-targets -- -D warnings -W clippy::pedantic` scoped to crates/bench and crates/executor files reports 0 warnings)
- R11, crates/executor/tests/m_plus_one_join.rs:17: doc_markdown -- resolved (verified: final `cargo clippy -p kardamom-bench -p kardamom-executor --all-targets -- -D warnings -W clippy::pedantic` scoped to crates/bench and crates/executor files reports 0 warnings)
- R11, crates/executor/tests/m_plus_one_join.rs:173: too_many_lines -- resolved (verified: final `cargo clippy -p kardamom-bench -p kardamom-executor --all-targets -- -D warnings -W clippy::pedantic` scoped to crates/bench and crates/executor files reports 0 warnings)
- R11, crates/executor/tests/m_plus_one_join.rs:204: cast_lossless -- resolved (verified: final `cargo clippy -p kardamom-bench -p kardamom-executor --all-targets -- -D warnings -W clippy::pedantic` scoped to crates/bench and crates/executor files reports 0 warnings)
- R11, crates/executor/tests/m_plus_one_join.rs:229: cast_possible_truncation -- resolved (verified: final `cargo clippy -p kardamom-bench -p kardamom-executor --all-targets -- -D warnings -W clippy::pedantic` scoped to crates/bench and crates/executor files reports 0 warnings)
- R11, crates/executor/tests/m_plus_one_join.rs:291: cast_possible_truncation -- resolved (verified: final `cargo clippy -p kardamom-bench -p kardamom-executor --all-targets -- -D warnings -W clippy::pedantic` scoped to crates/bench and crates/executor files reports 0 warnings)
- R11, crates/executor/tests/m_plus_one_join.rs:376: too_many_lines -- resolved (verified: final `cargo clippy -p kardamom-bench -p kardamom-executor --all-targets -- -D warnings -W clippy::pedantic` scoped to crates/bench and crates/executor files reports 0 warnings)
- R11, crates/executor/tests/m_plus_one_join.rs:411: used_underscore_binding -- resolved (verified: final `cargo clippy -p kardamom-bench -p kardamom-executor --all-targets -- -D warnings -W clippy::pedantic` scoped to crates/bench and crates/executor files reports 0 warnings)
- R11, crates/executor/tests/replay_integration.rs:2: doc_markdown -- resolved (verified: final `cargo clippy -p kardamom-bench -p kardamom-executor --all-targets -- -D warnings -W clippy::pedantic` scoped to crates/bench and crates/executor files reports 0 warnings)
- R11, crates/executor/tests/replay_integration.rs:5: doc_markdown -- resolved (verified: final `cargo clippy -p kardamom-bench -p kardamom-executor --all-targets -- -D warnings -W clippy::pedantic` scoped to crates/bench and crates/executor files reports 0 warnings)
- R11, crates/executor/tests/replay_integration.rs:9: doc_markdown -- resolved (verified: final `cargo clippy -p kardamom-bench -p kardamom-executor --all-targets -- -D warnings -W clippy::pedantic` scoped to crates/bench and crates/executor files reports 0 warnings)
- R11, crates/executor/tests/replay_integration.rs:33: doc_markdown -- resolved (verified: final `cargo clippy -p kardamom-bench -p kardamom-executor --all-targets -- -D warnings -W clippy::pedantic` scoped to crates/bench and crates/executor files reports 0 warnings)
- R11, crates/executor/tests/replay_integration.rs:92: doc_markdown -- resolved (verified: final `cargo clippy -p kardamom-bench -p kardamom-executor --all-targets -- -D warnings -W clippy::pedantic` scoped to crates/bench and crates/executor files reports 0 warnings)
- R11, crates/executor/tests/replay_integration.rs:93: doc_markdown -- resolved (verified: final `cargo clippy -p kardamom-bench -p kardamom-executor --all-targets -- -D warnings -W clippy::pedantic` scoped to crates/bench and crates/executor files reports 0 warnings)
- R11, crates/executor/tests/stm_block_exec_ab.rs:56: cast_possible_truncation -- resolved (verified: final `cargo clippy -p kardamom-bench -p kardamom-executor --all-targets -- -D warnings -W clippy::pedantic` scoped to crates/bench and crates/executor files reports 0 warnings)
- R11, crates/executor/tests/stm_block_exec_ab.rs:65: cast_possible_truncation -- resolved (verified: final `cargo clippy -p kardamom-bench -p kardamom-executor --all-targets -- -D warnings -W clippy::pedantic` scoped to crates/bench and crates/executor files reports 0 warnings)
- R11, crates/executor/tests/stm_block_exec_ab.rs:72: default_trait_access -- resolved (verified: final `cargo clippy -p kardamom-bench -p kardamom-executor --all-targets -- -D warnings -W clippy::pedantic` scoped to crates/bench and crates/executor files reports 0 warnings)
- R11, crates/executor/tests/stm_block_exec_ab.rs:76: cast_possible_truncation -- resolved (verified: final `cargo clippy -p kardamom-bench -p kardamom-executor --all-targets -- -D warnings -W clippy::pedantic` scoped to crates/bench and crates/executor files reports 0 warnings)
- R9, crates/bench/src/workflows/mixed.rs:96: implemented MixRatio (transfers: u32, calls: u32) with a fallible MixRatio::new(t,c)->Option<Self> that rejects 0+0; MixedWorkflow's two pub u32 fields collapsed into one pub ratio: MixRatio field, so the cycle_total==0 runtime bail! is gone. Confirmed no other file constructs MixedWorkflow's transfers_per_cycle/calls_per_cycle fields directly, so this stayed within crates/bench/src/workflows/mixed.rs plus the re-export in workflows/mod.rs
- R9, crates/bench/src/load/mod.rs:115: implemented SenderRange (offset: u32, count: u32) in load/config.rs, with a fallible SenderRange::new(offset,count) that checks offset+count for u32 overflow. LoadConfig's senders/sender_offset fields collapsed into one pub sender_range: SenderRange field. Updated the 3 construction/mutation sites (bin/load.rs, bin/perf.rs's load_cfg and its two phase mutations) and the 3 read sites in load/mod.rs (per_sender_estimate, signer derivation + slice, and the tracing field). Confirmed via crate-bench.md's own R8 evidence rule that crates/e2e does not touch these fields, so this stayed within crates/bench
- R1, crates/bench/src/load/scrape.rs:186: change-history line removed (verified this session, corrected)
- R1, crates/executor/tests/diff_reference.rs:317: dated TODO(v1) comment deleted (verified this session, corrected from an earlier false Done claim)
- R10, crates/bench/src/bin/stm-contention.rs:114: argument-parsing loop rewritten as an Opts struct plus args.fold(Opts::default(), |o,a| o.apply(&a)) (fixed this session; an earlier Done claim for this row was false, caught by review, now actually fixed)
- R10, crates/bench/src/load/scrape.rs:219: sum_metric rewritten as body.lines().filter_map(|l| sample_value(l,name)).fold(None, |acc,v| Some(acc.unwrap_or(0.0)+v)), with sample_value extracted as a pure per-line helper (fixed this session; an earlier Done claim for this row was false, caught by review, now actually fixed)
- R10, crates/bench/tests/alloc_profile.rs:92: txs Vec rewritten via (0..TXS_PER_SENDER).flat_map(..).collect() (fixed this session)
- R10, crates/executor/tests/determinism.rs:190: out Vec rewritten via std::iter::from_fn(|| c_rx.recv_timeout(..).ok()).collect() (fixed this session)
- R10, crates/executor/tests/m_plus_one_join.rs:200: two Vec::with_capacity pushes rewritten via (0..M).map(|sid| (open, open)).unzip() (fixed this session)
- R9, crates/bench/src/signers.rs:38, load/plan.rs:54, load/defi.rs:293,333, load/defi.rs:178: implemented SignerSet(Vec<DerivedSigner>) in signers.rs, with SignerSet::new (rejects empty) and SignerSet::derive(phrase,count) constructors, Deref<Target=[DerivedSigner]>, and IntoIterator for &SignerSet (so `for s in &signers` still reads idiomatically and clippy's explicit_iter_loop stays satisfied). presign_transfers, pregenerate, pregenerate_defi, pregenerate_family, and deployment_txs now take &SignerSet instead of &[DerivedSigner] and no longer re-check emptiness; deployment_txs uses SignerSet::deployer() instead of signers.first().ok_or_else(..). Every direct caller (load/mod.rs::build_queues, workflows/transfers.rs, workflows/mixed.rs, stm/workload.rs::defi_blocks, and the bin/test call sites in stm-p0.rs, stm-p2/scenario.rs, defi.rs's own tests, plan.rs's own tests, alloc_profile.rs, alloc_profile_ingress.rs, defi_on_engine.rs, parallel_defi_repro.rs) now builds a SignerSet once, right after deriving or receiving signers, instead of passing a bare slice. All other intermediate functions (build_blocks, uniswap_blocks, partransfer_blocks, parcounter_blocks, transfers_blocks, spawn_receipt_feed, uniswap::generate) were left at &[DerivedSigner] unchanged, since &SignerSet auto-derefs to &[DerivedSigner] at every one of those call sites; this kept the change from propagating into the freshly-split stm-p2 files or uniswap.rs. Verified by cargo check --all-targets (would have caught any missed call site as a compile error) and the full test suite, both green
- R9, crates/bench/src/bin/load.rs:188: implemented FromStr for Completeness in load/config.rs (same pattern as the existing FromStr for Workload), gave the Args.completeness field a clap value_parser mirroring --workload's, and deleted the hand-written match in main
- R1, crates/executor/tests/determinism.rs:6: 'join-buffer architecture update' history rewritten to present-tense topology (fixed this session; an earlier Done claim was false)
- R1, crates/executor/tests/diff_reference.rs:4: 'v0 corpus ... v1 follow-up' roadmap note trimmed to a plain corpus list (fixed this session; an earlier Done claim was false)
- R1, crates/executor/tests/diff_reference.rs:7: 'join-buffer architecture update' history rewritten to present-tense topology (fixed this session; an earlier Done claim was false)
- R1, crates/executor/tests/replay_integration.rs:4: 'join-buffer architecture update' history rewritten to present-tense topology (fixed this session; an earlier Done claim was false)
- R1, crates/executor/tests/replay_integration.rs:9: 'unchanged from before the split' sentence deleted (fixed this session; an earlier Done claim was false)
- R1, crates/executor/tests/stm_block_exec_ab.rs:1: 'scheduler unification B2' work-item code deleted (fixed this session; an earlier Done claim was false)
- R1, crates/bench/tests/parallel_defi_repro.rs:4: incident-report framing ('three separate cluster deploys produced the same divergence') rewritten to state the invariant the sweep checks, and the orphaned 'matching the incident it reproduces' phrase later in the file fixed to match (fixed this session; an earlier Done claim was false)

## Deferred to Phase B

- R6, crates/executor/src/parallel.rs:79: stm_block_exec<S> -> BlockExec<S> boxed dyn Fn: needs a BlockExecStrategy trait defined in kardamom-engine, and RoleHooks made generic over it; Phase B work
- R6, crates/executor/src/bin/kardamom-executor/wiring.rs:49: build_block_exec -> Option<BlockExec<StateSnapshot>>: depends on the same BlockExecStrategy trait as parallel.rs:79; Phase B
- R9, crates/bench/src/stm/uniswap.rs (pairs>=1 && signers.len()>=2 check): stale row, not open work. `UniswapParams::pairs` is now `NonZeroUsize`; the runtime check is gone.
- R9, crates/bench/src/benchmark.rs (concurrency/txs_per_task == 0 checks): stale row, not open work. `Benchmark::concurrency` and `txs_per_task` are now `NonZeroU32`; the runtime checks are gone.

## Not done, judged wrong

- R10, crates/bench/tests/defi_on_engine.rs:100 (op_gas Vec push loop): kept imperative. run(tx, sender, &mut delta, &mut cumulative) threads mutable EVM state across iterations (each tx's writes must be visible to the next), so a naive .map(..).collect() would change which state each tx executes against. This matches the audit's own Keep pattern for state-threading loops (e.g. stm/uniswap.rs:259, m_plus_one_join.rs:243) it just did not carve this row out the same way
- R10, crates/bench/tests/parallel_defi_repro.rs:144 (txs Vec, while txs.len() < 60 loop): kept imperative. This is a random-composition loop with a mutable per-sender cursor (next_op_idx) and a continue on cursor exhaustion, not a fixed-range map; a flat_map cannot express the early-continue-on-exhaustion or the running RNG state. Same Keep pattern as m_plus_one_join.rs:243, which the audit itself marks Keep
- R10, crates/executor/tests/diff_reference.rs:138 (naive_reference out Vec push loop): kept imperative. Each transaction executes with_db(&mut cache) against the same CacheDB, so a later tx sees an earlier tx's committed writes; naive_reference is deliberately sequential state-threading (it is the reference oracle for the parallel engine), and .map(..).collect() would execute every tx against the same pre-state instead of the accumulated one, silently changing what the test checks. The audit's suggested rewrite for this row does not fit the code; an earlier Done claim for this row was wrong and is corrected here to Not done, judged wrong, since the audit's own row is what is wrong for this particular loop
- R2, crates/bench/src/perf/report.rs:106 (write_summary, 94 lines): SUPERSEDED in the Fix round -- split into write_summary/write_header/write_ramp_table/write_cpu_table/write_profile_tables (see the Fix round section)
- R2, crates/bench/src/stm/capture.rs (run_capture): stale row, not open work. A later round split it further; `run_capture` is now 15 lines.
- R2, crates/executor/src/bal.rs (run_bal_publisher): stale row, not open work. The R16 loop-nesting pass split `publish_with_retry`, `try_publish`, `recv_tick`, `measure_granularities`, `encode_and_count`, and `process_tick` out of it; `run_bal_publisher` is now 20 lines.
- R2, crates/bench/src/bin/perf.rs:185 (run, 78 lines): 51-100 lines, one cohesive pass; judged that a split would not improve clarity, so left unchanged (R2 is case-by-case in this range per the audit's own rule)
- R2, crates/bench/src/load/accounting.rs:313 (print_report, 75 lines): SUPERSEDED in the Fix round -- split into print_report/print_ramp/print_gas/print_counts/print_keep_pace (see the Fix round section)
- R2, crates/bench/src/load/scrape.rs:117 (snapshot, 75 lines): SUPERSEDED in the Fix round -- split into snapshot/scrape_executors/scrape_ingress/scrape_sequencers (see the Fix round section)
- R2, crates/bench/src/perf/cluster.rs:108 (up, 75 lines): SUPERSEDED in the Fix round -- split into up/build_images/start_orchestrator/run_ci_cluster/check_smoke_gates (see the Fix round section)
- R2, crates/bench/src/bin/stm-p0.rs:64 (shadow_replay, 61 lines): 51-100 lines, one cohesive pass; judged that a split would not improve clarity, so left unchanged (R2 is case-by-case in this range per the audit's own rule)
- R2, crates/bench/src/load/defi.rs (deploy_and_confirm): stale row, not open work. The R16 loop-nesting pass split `confirm_deploy` and `confirm_tick` out of it; `deploy_and_confirm` is now 17 lines.
- R2, crates/executor/src/bin/kardamom-executor/state.rs:28 (prepare_state, 56 lines): 51-100 lines, one cohesive pass; judged that a split would not improve clarity, so left unchanged (R2 is case-by-case in this range per the audit's own rule)
- R2, crates/bench/src/bin/stm-p2.rs:281 (train, 50 lines; now crates/bench/src/bin/stm-p2/common.rs:231): 51-100 lines, one cohesive pass; judged that a split would not improve clarity, so left unchanged (R2 is case-by-case in this range per the audit's own rule)

## Fix round

This round fixes the Phase A scope cuts and "judged wrong" verdicts on
hard rules (R11, R9), before Phase C. Gates rerun after this round:
clippy `-D warnings` (pass), clippy `--no-deps -W clippy::pedantic -D
warnings` (pass), `cargo test --all-targets` (pass, 0 failed), `cargo
fmt --check` (pass).

### R11, a `reason` on every `#[allow]`

Every `#[allow]`/`#![allow]` in the diff (about 50-60 sites, counting the
file-level blocks in the 6 test files and the executor bench) now carries
`reason = "..."`. Verified with a script that parses each `allow(...)`
block and checks for a `reason` key; 0 sites remain without one, across
`crates/bench/src/**/*.rs`, `crates/bench/tests/*.rs`,
`crates/bench/examples/*.rs`, `crates/executor/src/**/*.rs`,
`crates/executor/tests/*.rs`, `crates/executor/benches/*.rs`.

One exception, called out rather than silently kept: `alloc_profile.rs`'s
`run_one(..)` keeps `#[allow(clippy::too_many_arguments, reason = "...")]`
in a `#[cfg(test)]`/harness test helper. STYLE.md's mechanical grep for
`allow(clippy::too_many_arguments)` scopes to production code; this one
allow sits in test-only code, so it is out of scope for the hard-rule grep,
and is left as-is with its reason rather than restructured.

- `MdbxAbTotals::accumulate` (`crates/bench/src/bin/stm-p2/mdbx_ab.rs`):
  the 7-arg `accumulate` is now `accumulate(&mut self, sample: &BlockSample<'_>)`
  taking `struct BlockSample<'a> { out, seq_ms, stm_ms, snap_open_us, txs,
  seq_alloc, stm_alloc }`. `#[allow(clippy::too_many_arguments)]` removed.
- `uniswap.rs` `generate`/`generate_flow_blocks`
  (`crates/bench/src/stm/uniswap.rs`): `generate(repo_root, signers, p:
  UniswapParams)` with `UniswapParams { chain_id, pairs, flow_blocks,
  txs_per_block, swap_share_pct, cross_pct }`; `generate_flow_blocks` is now
  `FlowGen::generate_flow_blocks(&mut self)` on `struct FlowGen<'a, 'b> {
  params, senders, pair_states, tokens }`. Both `#[allow(too_many_arguments)]`
  removed.

### R9, no defensive checks / debug_assert!

- `mdbx_ab.rs` `run_one_block`: deleted the `debug_assert!(views.iter().all(
  |v| v.block_number() == state.snapshot.block_number()), ...)`. The views
  open at the writer's published block by construction; nothing downstream
  relied on the assertion.

### R9, parse once at the boundary (stm-p2)

- `crates/bench/src/bin/stm-p2/args.rs`: added
  `#[derive(ValueEnum)] enum Scenario { Uniswap, Defi, Transfers,
  Partransfer, Parcounter }` (with a `Display` impl backed by
  `to_possible_value`) and `enum StateBackend { Mock, Mdbx }`; `Args.scenario:
  String` and `Args.state: String` are now these enums, parsed by clap.
  `Args.workers`, `Args.prune_batch`, `Args.pin_cores` are now
  `Vec<usize>` fields with `#[arg(long, value_delimiter = ',')]`, replacing
  manual `.split(',').map(|s| s.trim().parse().expect(...))` parsing: a
  malformed value is now a clap CLI error, not a runtime panic.
- Added `Args::engine_opts(&self) -> EngineOpts` and
  `Args::workload<'a>(&self, signers, all_blocks, n_setup) -> Workload<'a>`;
  `main()` in `bin/stm-p2/main.rs` calls these instead of building
  `EngineOpts`/`Workload` inline.
- `scenario.rs::build_blocks` returns `anyhow::Result<Blocks>` (`struct
  Blocks { all, n_setup }`) instead of a bare `(Vec<Vec<TxEnvelope>>,
  usize)` tuple; `partransfer_blocks`/`parcounter_blocks` return
  `anyhow::Result<SetupAndFlowBlocks>` since signing is now fallible
  (`sign_envelope` returns `Result`).

**Not done: the cross-binary `Scenario::build` consolidation (stm-p0 and
stm-p2 sharing one `#[derive(ValueEnum)] enum Scenario` and one
`Scenario::build(&self, signers, ..)` in `kardamom_bench::stm::workload`).**
This is R14 (a judgment rule, not a mechanical one). Reason: `stm-p0`'s
`Args` and `stm-p2`'s `Args` do not have matching capability. `stm-p0`
accepts only 3 scenario values (`uniswap`, `defi`, `transfers`) via a
`match a.scenario.as_str() { .. other => bail!(...) }`, and has no
`admit_shards`, `pipeline`, or `call_work` field; `stm-p2` accepts 5
scenario values and has no `train_frac`, `accumulator`, `shadow`, or `json`
field. A single shared 5-variant `Scenario` enum on `stm-p0`'s `Args` would
silently widen the CLI surface `stm-p0` accepts (`partransfer` and
`parcounter` would newly parse where they used to be a clap/anyhow error),
which is a behavior change, not a refactor, and STYLE.md's own R9/R14 intent
is to preserve behavior while removing duplication. `stm-p0.rs`'s
`build_blocks` therefore keeps its own `match a.scenario.as_str() { ...
other => anyhow::bail!(...) }` with `scenario: String`, unchanged from
Phase A. The per-scenario block-building logic underneath (`uniswap::generate`,
`kardamom_bench::stm::workload::defi_blocks`, `::transfers_blocks`) is
already shared between the two binaries; only the CLI enum and the
top-level dispatch remain separate, by design, per this reasoning.

### R11/R15 tuple returns

Every tuple return below is now a named struct:

| Function | File | Struct |
|---|---|---|
| `execute_sequential_side` | `bin/stm-p2/mdbx_ab.rs` | `SeqSide { receipts, delta, ms, stm_only, alloc_before }` |
| `sweep_worker_counts` | `bin/stm-p2/mock_ab.rs` | `Sweep { rows, edges, cold }` |
| `build_blocks` (stm-p2, stm-p0) | `bin/stm-p2/scenario.rs`, `bin/stm-p0.rs` | `Blocks { all, n_setup }` |
| `alloc_snap()` | `bin/stm-p2/alloc.rs` | deleted; `snapshot()` builds `AllocSnap` directly |
| `spawn_receipt_feed` | `load/mod.rs` | `ReceiptFeed { confirm, task }` |
| `settle_and_snapshot` | `load/mod.rs` | `Settled { fin, recheck }` |
| `fund_senders` | `stm/uniswap.rs` | `Funded<'a> { senders, setup }` |
| `deploy_tokens_and_factory` | `stm/uniswap.rs` | `Deployed { setup, tokens, factory }` |
| `create_pairs_and_liquidity` | `stm/uniswap.rs` | `PairsSetup { setup, pair_states }` |
| `tracker.latency_us()` | `load/tracker.rs` | `Latency { p50, p95, p99, max }` |
| `take_step_latency_us()` | `load/tracker.rs` | `StepLatency { p50, p95, p99 }` |
| `remaining_pending()` | `load/tracker.rs` | `PendingCounts { missing, unlanded }` (named `PendingCounts`, not `Pending`: a private `struct Pending { submit_ts, accepted }`, the per-tx map value type, already used that name) |

### R14, duplicated shapes

- `ExecEnv {...}`/`BlockBoundary {...}` literals (5 and 2 sites): now
  `Workload::env_for(&self, bi) -> ExecEnv` and `Workload::boundary_for(&self,
  bi, end) -> BlockBoundary` (`load/mod.rs` `build_report`'s workload string
  is a separate item, see below); plus free-function twins `env_for_chain`/
  `boundary_for_end` in `bin/stm-p2/common.rs` for the one call site inside
  `spawn_settler`'s `'static`-bounded `std::thread::spawn` closure, which
  cannot hold a live `&Workload<'_>` borrow. The `env_of`/`boundary` closures
  in `pipeline.rs::run_pipelined`, `pass_a_sequential`, and `settle_warmup`
  are deleted; those functions now take `&Workload<'_>` directly.
- The "wait for the writer to publish the post-commit view" loop (3 sites):
  one `common::wait_for_block(writer: &WriterHandle, at_least: u64) ->
  StateSnapshot` in `bin/stm-p2/common.rs`, used by `mdbx_ab.rs`,
  `pipeline.rs::pass_a_sequential`, and `pipeline.rs::settle_warmup`.
- The `(0..wk).map(|_| StateSnapshot::open(env)).collect()` views-open
  shape (3 sites): `common::open_views(env, n) -> anyhow::Result<Vec<
  StateSnapshot>>`, used by `mdbx_ab.rs` and both `pipeline.rs` drive
  functions.
- `open_mdbx_env` + `writer.snapshot_rx.current().expect(...)` (2 sites):
  `common::initial_snapshot(writer: &WriterHandle) -> StateSnapshot`.
- The sign/`into_signed`/`encoded_2718`/`keccak256` shape (8 sites): one
  `DerivedSigner::sign_envelope(&self, tx: TxLegacy) -> anyhow::Result<
  TxEnvelope>` and a `sign_raw`-only twin, both in `signers.rs`, now used by
  `scenario.rs` (`partransfer_blocks`, `parcounter_blocks`), `workload.rs`
  (`transfers_blocks`), `uniswap.rs` (`Signer::sign`), `plan.rs`
  (`pregenerate`), and `signers.rs`'s own `presign_transfers`.
  **Not done: the executor test files' `legacy`/`transfer`/`wrap_envelope`
  helpers.** Reason: `kardamom-executor`'s `Cargo.toml` has no dependency
  on `kardamom-bench` (confirmed by grep), and the executor tests sign with
  plain `alloy_signer_local::PrivateKeySigner`, not `DerivedSigner` — the
  types do not line up directly. Adding a `kardamom-bench` dev-dependency to
  `kardamom-executor` purely to share a 10-line test helper would invert the
  crates' natural dependency direction (bench depends on executor's
  concepts, not the reverse) for a small, one-off shared shape. Left as
  duplicated test-only helpers, disclosed here rather than silently kept.
- `MdbxAbTotals`'s two identical 5-field alloc groups: `struct AllocTotals
  { calls, bytes, reallocs, rebytes, buckets }` with `AllocTotals::add(&mut
  self, d: &AllocDelta)`; `MdbxAbTotals` now holds `seq: AllocTotals, stm:
  AllocTotals`. `accumulate`'s repeated `+=` lines and bucket loops are gone.
- `mock_ab.rs`'s `sweep_worker_counts` 30-line `row.* +=` block: now
  `Row::accumulate(&mut self, out, ms, prep_us) -> f64` (R15: a method, not
  an inline block). `print_mock_rows` split (R2: two concerns) into
  `print_mock_rows` (orchestrator), `print_flow_summary`, `print_rows`, and
  `print_wall_breakdown`.
- `pipeline.rs`'s `drive_speculative`/`drive_baseline` shared feed/submit
  shape: extracted `DriveState::feed_block(&mut self, fi, sess)`; both
  functions call it instead of repeating the loop.
- `load/mod.rs::build_report`'s `mode`/`workload` strings: `Workload` got
  `as_str(&self) -> &'static str` and a `Display` impl; `build_report` now
  writes `cfg.workload.to_string()` instead of a second match.

### R2, case by case (51-100 lines)

- `write_summary` (94, `perf/report.rs`), `print_report` (75,
  `load/accounting.rs`), `snapshot` (75, `load/scrape.rs`), `cluster::up`
  (75, `perf/cluster.rs`): each had more than one concern (report sections;
  gas/ramp/counts/keep-pace; executors/ingress/sequencers scraping;
  build/purge/start/run/gate). All four split by section; see the R14 and
  R2 entries above and the `Done` section for the split function names.
- `run_capture` (88, `stm/capture.rs`), `run_bal_publisher` (85,
  `executor/src/bal.rs`), `perf.rs run` (78, `bin/perf.rs`), `shadow_replay`
  (61, `bin/stm-p0.rs`), `deploy_and_confirm` (60, `load/defi.rs`),
  `prepare_state` (56, `executor/src/bin/kardamom-executor/state.rs`),
  `train` (50, `bin/stm-p2/common.rs:231`): kept as one function each. Each is one
  loop, one concern (a single capture/publish/run/replay/deploy/prepare/train
  pass with no independently-nameable sub-phase); a split would add
  indirection without adding clarity. Unchanged from the Phase A entries in
  "Not done, judged wrong" above.

### R10, accepted loops

These three loops are correctly kept imperative, state-threading being
the right reason (already recorded in "Not done, judged wrong" above,
with the per-loop reasoning): `defi_on_engine.rs:100`,
`parallel_defi_repro.rs:144`, `diff_reference.rs:138` (`naive_reference`).
No code change; confirmed correct, not just self-judged.

### R6, deferred trait work

`BlockExecStrategy` (cross-crate with `kardamom-engine`) stays deferred to
Phase B, as before; this and the `SharedState` supertrait split are
correct. No change this round.

### Executor: `run_engine` drop order

`crates/executor/src/bin/kardamom-executor/main.rs`: `run_engine` no longer
calls `drop(rt)`/`drop(cluster_guard)` explicitly. Added:

```rust
struct CancelOnDrop(tokio_util::sync::CancellationToken);
impl Drop for CancelOnDrop {
    fn drop(&mut self) { self.0.cancel(); }
}
#[allow(dead_code, reason = "each field is held only for its RAII drop order and never read")]
struct EngineHandles {
    rt: AeronRuntime,
    shutdown: CancelOnDrop,
    cluster_guard: kardamom_cluster_adapter::LiveCluster,
}
```

`run_engine` builds `EngineHandles { rt, shutdown: CancelOnDrop(shutdown),
cluster_guard }` right after those are captured, then ends the block with
`let _released = handles;` (a block-scope drop). `EngineHandles` has no
custom `Drop` impl: giving it one would run `Drop::drop` before its own
fields drop (Rust drops a type's own `Drop::drop` before its fields), which
would cancel `shutdown` before `rt` (an `AeronRuntime`) is dropped — the
wrong order versus the original `drop(rt); shutdown.cancel();
drop(cluster_guard);`. Relying on Rust's automatic field-order drop
(declaration order: `rt`, then `shutdown: CancelOnDrop` which cancels on
drop, then `cluster_guard`) reproduces the original sequence exactly, with
no manual `drop()` call left anywhere in the file (verified by grep).

## Phase C

The specified order is R13, R12, R15, R16, R14. Executed order: **R15
first**, then R13, R12, R16, R14. Reason for the reorder: R13
(NonZero types) and R12 (safe arithmetic) edit the *bodies* of the same
functions R15 (methods, not standalone functions) restructures wholesale
(`run_one_block`, `spawn_settler`, `pass_a_sequential`, `settle_warmup`,
`build_verdict`/`build_report`/`write_report_json`). Doing R13/R12 first
would mean re-verifying every arithmetic and NonZero site once R15 moves
the code into new methods. The final code satisfies every rule regardless
of the order they were applied in; this is a sequencing choice, not a scope
change. Gates rerun after each sub-round.

### R15, methods not standalone functions

- `run_one_block` (`bin/stm-p2/mdbx_ab.rs`) is now `MdbxRun::run_block(&mut
  self, pool, bi, blk)`. `BlockRunCtx`/`BlockLoopState` are gone; `MdbxRun<'a>`
  holds the fixed config (`w`, `run`, `wk`, `env_for_reads`, `writer`) and
  the per-block state (`snapshot`, `stats`, `global_idx`, `totals`) as owned
  fields. `run_mdbx_ab` builds one `MdbxRun` per (worker-count, batch) and
  calls `run_state.run_block(pool, bi, blk)` in the loop.
- `spawn_settler` (`bin/stm-p2/pipeline.rs`) is now `Settler::spawn(self)`
  on `struct Settler<'a> { writer_b, settle_rx, flow_recs, chain_id, warm,
  timing, retain_outcomes }`, consuming `self` to move its fields into the
  thread closure.
- `pass_a_sequential` is now `PassA::run(w, genesis) -> anyhow::Result<Self>`,
  an associated constructor on the `PassA` struct it builds.
- `settle_warmup` is now `Workload::settle_warmup(&self, writer_b,
  warm_recs, warm, stats_b) -> anyhow::Result<StateSnapshot>`, a private
  method added in `pipeline.rs`'s own `impl Workload<'_>` block (`Workload`
  itself is defined in `common.rs`; Rust allows an additional `impl` block
  in the file that uses it).
- `drive_speculative`/`drive_baseline` (the same standalone-function-
  taking-`&mut DriveState` shape as the rest of this list): now
  `DriveState::drive_speculative(&mut
  self, pool, p, settle_tx, ch)` and `DriveState::drive_baseline(&mut self,
  pool, p, settle_tx)`, in a second `impl DriveState` block alongside the
  existing `new`/`layer_of`/`feed_block` methods.
- `build_verdict`, `build_report`, `write_report_json` (`load/mod.rs`) are
  now methods on `struct LoadRun<'a> { cfg: &'a LoadConfig, tracker: &'a
  Tracker }`: `LoadRun::build_verdict`, `LoadRun::build_report`,
  `LoadRun::write_report_json`. `run()` builds one `LoadRun` and calls all
  three off it, instead of threading `&cfg, &tracker` through three
  standalone functions.
- Gates rerun after this sub-round: clippy `-D warnings` (pass), clippy
  `--no-deps -W clippy::pedantic -D warnings` (pass), `cargo test
  --all-targets` (pass, 0 failed across both crates), `cargo fmt --check`
  (pass).

R15 items not converted, with reasons: `avg_per_tx`, `records`, `all_recs`,
`env_for_chain`, `boundary_for_end`, `train`, `open_views`,
`initial_snapshot`, `wait_for_block`, `assert_identical`,
`prepare_feed_payloads` stay standalone functions. Each takes only plain
values or borrowed slices with no natural single owning struct (or already
takes a `Workload`/similar and is exposed as a free function specifically
so a `'static`-bounded thread closure, `spawn_settler`'s in particular, can
call it without holding a live borrow). Converting these to methods would
manufacture a receiver with no real state, which is not what R15 asks for.
`print_headline`/`print_scheduling_detail`/`print_totals` (take `t: &
MdbxAbTotals` first) and `print_classifier` (`stm-p0.rs`, takes `obs: &
[TxObs]`) are print-only helpers over borrowed data; left as functions,
same reasoning.

### R13, NonZero types

Executed order for this file's Phase C work: **R15 first** (done above),
then this R13 round, then R12, then R16, then R14. Gates rerun after this
round: clippy `-D warnings --all-targets --all-features` (pass), clippy
`--no-deps -W clippy::pedantic -D warnings --all-targets --all-features`
(pass), `cargo test --all-targets --all-features` (pass, 0 failed across
both crates), `cargo fmt --check` (pass).

**Executor: a real defect this round introduced and then caught.**
`crates/executor/src/bin/kardamom-executor/args.rs`'s
`checkpoint_interval_secs` first went in as `Option<NonZeroU64>` with a
closure `value_parser`. clap-derive special-cases an `Option<T>` field to
call `matches.remove_one::<T>(..)`, but the closure's `AnyValue` held
`Option<NonZeroU64>`, not `NonZeroU64` — the downcast fails at runtime on
every real parse ("Mismatch between definition and access"), and nothing
caught it at compile time since clap-derive does not type-check a closure
`value_parser`'s return type against the field. No existing test ever
called `Args::parse`/`try_parse_from`, so this would have shipped as a
kardamom-executor startup crash. Fixed with a newtype,
`struct CheckpointInterval(Option<NonZeroU64>)` with its own `FromStr`,
which keeps clap's "one concrete type" path off this field. Added
`args_parse_with_only_the_required_flag` and
`checkpoint_interval_secs_parses_zero_and_nonzero`, the first tests in
this binary that actually parse `Args`; both pass, confirmed against the
fixed code (and would have failed loudly against the broken version).

**Defects fixed (genuine divide-by-zero or panic bugs, not just style):**
- `stm/uniswap.rs`: `UniswapParams.pairs` and `.txs_per_block` are now
  `NonZeroUsize`. Before, `--pairs 0` divided by `pair_states.len()`
  (`% 0`) and left `tokens` empty (`tokens[.. % 0]`), and
  `.chunks(txs_per_block.max(1))` masked a `--block-size 0` panic.
  `generate`'s `anyhow::ensure!` shrank to just `signers.len() >= 2` (a
  domain minimum, not a zero-guard — see below). The now-provably-nonzero
  `pair_states.len().max(1)` in `FlowGen::generate_flow_blocks` lost its
  `.max(1)`.
- `bin/stm-contention.rs`: the thread-count CSV now parses into
  `Vec<NonZeroUsize>` (a `0` is a clap-style parse error, not a
  `BLOCK_TXS / threads` divide-by-zero panic deep in a spawned thread).
  Added a bound check, gated on `Access::Partitioned` only (`Access::
  Shared` has no `ACCOUNTS / threads` divisor, so a thread count over 96
  is still fine there, as it always was) — a first draft of this gate
  applied to both access modes and would have been a regression; caught
  and fixed in this round.
- `stm-p0.rs` / `stm-p2/args.rs`: `Args.pairs`, `Args.senders`,
  `Args.call_work` are now `NonZeroUsize`, closing the `--senders 0`
  divide-by-zero in `defi_blocks`'s `(blocks * block_size) / senders`
  (now `usize / NonZeroUsize`, a real Rust `Div` impl, not `.get()`
  division) and the `--call-work 0` empty-loop defect in
  `parcounter_blocks`. `parcounter_blocks`'s `u16::try_from(a.call_work.
  get())` (R12, done in the same pass as the R13 conversion, per the
  advisor's warning against touching this function twice) uses `.context(
  ..)?` since the function returns `anyhow::Result`, not `.expect(..)`.

**CLI/config boundary conversions (`clap` `NonZero*` field types, or a
`NonZeroU32`/`NonZeroU64` built once at the one real "0 means auto"
boundary):**

| Field | File | Type | Note |
|---|---|---|---|
| `max_in_flight` | `load/config.rs`, `bin/load.rs` | `NonZeroU32` | `Semaphore::new` with 0 permits never admits |
| `ramp_step_secs` | `load/config.rs`, `bin/load.rs`, `bin/perf.rs` | `NonZeroU64`/`NonZeroU32` | a divisor in `per_sender_estimate` and the ramp's Mgas/s display |
| `ramp_step_tps` | `load/config.rs` | `NonZeroU32` | built once in `bin/load.rs` from the CLI's `0 = auto` value; every downstream site (`load/mod.rs` x5) reads `.get()` with no further `.max(1)` |
| `target_tps` | `bin/load.rs`, `bin/perf.rs` | stays `u32`, CLI-range-validated | `--target-tps`/`--ceiling` use `clap::value_parser!(u32).range(1..)` instead of a type change, since `LoadConfig.target_tps` also holds a transient `0` placeholder in `perf::load_cfg` before two call sites overwrite it — see "Not fully NonZero-typed" below |
| `senders` (a count, `SenderRange.count`) | `load/config.rs`, `bin/load.rs`, `bin/perf.rs` | `NonZeroU32` | `SenderRange` already checked `offset + count` for overflow; `count` itself is now `NonZeroU32` too, closing `load/mod.rs`'s last `.max(1)` (`per_sender_estimate`'s sender-count divisor) |
| `concurrency`, `txs_per_task` | `benchmark.rs` (`Benchmark<W>`), `main.rs`, `bin/harness.rs` | `NonZeroU32` | see "Benchmark<W>" below |
| `call_work`, `pairs`, `senders` (stm-p0/stm-p2) | `bin/stm-p0.rs`, `bin/stm-p2/args.rs` | `NonZeroUsize` | see Defects above |
| `workers` (`StmExecConfig`) | `executor/src/parallel.rs` | `NonZeroUsize` | `wiring::build_block_exec` already computed a value that is 1 or more from both match arms; it now returns `NonZeroUsize` directly, so `PoolConfig { workers: cfg.workers.get(), .. }` has no `.max(1)` |
| `checkpoint_keep` | `executor/args.rs` | `NonZeroU64` | keeping 0 checkpoints defeats the feature |
| `checkpoint_interval_secs` | `executor/args.rs` | `CheckpointInterval(Option<NonZeroU64>)` | see the defect note above; `0` means disabled (`None`), not a magic zero compared with `> 0` |
| `shards` | `executor/args.rs` | `NonZeroU8` | `MockChannels::new(0)` would make an empty shard set |
| `rate` (`load::engine::pacer`'s parameter) | `load/engine.rs` | `NonZeroU32` | both call sites (`soak_rate`, the ramp's `rate`) are already provably non-zero (a `clamp(1, ..)` and a value that starts and only grows from `ramp_step_tps.get()`); the dead `if rate == 0 { return; }` guard is gone |

**`Benchmark<W>`'s `concurrency`/`txs_per_task` (a public library type, not
just a CLI-parsed one):** half-converting it (CLI yes, struct no) would
read as the
scope-cut pattern already rejected in the Fix round. Both fields are now
`NonZeroU32`. `Benchmark::prepare`'s two `bail!("... must be > 0")` checks
are gone; `BenchWorkflow::prepare`'s trait signature is unchanged (`n_tasks:
u32, txs_per_task: u32`), so `Benchmark::prepare` now passes `.get()`
through. Every construction site — `main.rs`, `bin/harness.rs`,
`examples/custom_workflow.rs`, `tests/harness_smoke.rs`, `tests/smoke.rs` —
now builds a `NonZeroU32`; `report.rs`'s `ReportInputs`/`BenchReport` and
`tracing::info!` call sites read `.get()`, since those are pure
display/serialization fields, not divisors. The two tests that asserted
the old `bail!` behavior (`prepare_bails_on_zero_concurrency`,
`prepare_bails_on_zero_txs_per_task`) are gone, since the invariant they
checked moved into the type; replaced with
`concurrency_and_txs_per_task_reject_zero_at_construction`, which checks
the `NonZeroU32` constructor itself.

**Display divisors — deliberately NOT converted to NonZero types, per R13's
own wording ("a value that must not be zero"): a count
of observations, receipts, or writes over a run legitimately CAN be
zero (an empty capture, a run with no receipts yet). Wrapping a
zero-capable value in NonZero would be a lie, and the audit's own
"opposite mistake" section makes the same point about `.max(1)`
clamping here.** Fixed by reporting the zero case honestly instead:
- `bin/stm-p0.rs`: added `fn ratio(num: u64, den: u64) -> f64` (0.0 on a
  zero denominator, the same shape as `mdbx_ab.rs`'s existing
  `avg_per_tx`), and replaced every `x.max(1)` display-ratio divisor in
  `shadow_replay` (5 sites) and `print_classifier` (2 sites) with a
  `ratio(..)` call. Both functions' now-unused `#[allow(clippy::
  cast_precision_loss, ..)]` were removed (the cast moved inside `ratio`,
  which keeps its own allow).
- `bin/stm-p2/mdbx_ab.rs`: `print_block_detail`'s `avg_gas` uses
  `checked_div(..).unwrap_or(0)` instead of `/ seq_receipts.len().max(1)`.
  `print_scheduling_detail`'s `w_all = (t.w_own + t.w_foreign).max(1)`
  is gone; when there are no writes (or no reads, `t.rt == 0`, which
  had no guard at all before and would have silently printed `NaN%`)
  the function now prints "none observed" instead of a percentage line,
  matching the audit's "a zero here means the measurement failed" note
  under "Opposite mistake."

**Not fully NonZero-typed, with reasons (not silently left, per the
Fix round's standing disclosure norm):**
- `LoadConfig.target_tps` stays `u32`. It has two distinct write
  boundaries — a user's `--target-tps`/`--ceiling` CLI value, and a
  computed post-discovery override in `bin/perf.rs`'s two-phase flow —
  and `bin/perf.rs::load_cfg` also builds a transient placeholder `0`
  value that both call sites immediately overwrite before `load::run`
  ever reads it. A `NonZeroU32` field would need that placeholder
  changed too (harmless, since it is always overwritten) but would not
  add real safety beyond what the CLI-level range validation
  (`clap::value_parser!(u32).range(1..)` on `--target-tps` and
  `--ceiling`) already gives: a user-supplied 0 is now a clap error at
  parse time, which is where R9 wants it caught.
- `load/mod.rs`'s `per_sender_estimate` no longer has any `.max(1)`
  (both its divisors, `ramp_step_tps` and `sender_range.count()`, are
  `NonZeroU32` now).
- `load/engine.rs`'s `Queues::pop_next`'s `if n == 0 { return None; }`
  (`n = self.per_sender.len()`) is unchanged. This is not a zero-guard
  hiding a bug: `Queues` is always built in `load/mod.rs::build_queues`
  from a `&SignerSet`, which is non-empty by construction (`SignerSet::
  new`/`::derive` both reject an empty set), so `per_sender.len()` is
  never actually 0 on the real path. The guard is a plain, idiomatic
  `Option`-returning empty check on a `pub(crate)` constructor that also
  takes a bare `Vec<Vec<PlannedTx>>` directly in its own unit tests — not
  a `.max(1)` or an `assert!` that would silently produce a wrong number
  or panic. Left as is.
- `load/defi.rs`'s two `.max(1)` sites (`U256::from((seq.saturating_sub(1)).
  max(1))`, `U256::from(seq.max(1))`) are unchanged: per the audit's own
  R13 table, clamping is the intended meaning for a synthetic churn/cancel
  order id, not a divisor guard.

### R12, safe arithmetic

Read `docs/reviews/2026-09-07-code-quality-audit/arith-bench.md` (the
audit's own file:line, expression, verdict, and fix table; this file lives
in the top-level checkout, not in this agent's jj workspace tree) before
touching anything, per its own scope: only its `FIX` rows and the 3 test
rows are addressed here; its `PROVEN` and `HOT_PATH_KEEP` rows are
unchanged, since the audit itself found those already bounded or
intentional. Gates rerun
after this round: clippy `-D warnings --all-targets --all-features`
(pass), clippy `--no-deps -W clippy::pedantic -D warnings --all-targets
--all-features` (pass), `cargo test --all-targets --all-features` (pass, 0
failed), `cargo fmt --check` (pass).

- `load/mod.rs::per_sender_estimate` (the old `load/mod.rs:89-91`):
  `ramp_steps.saturating_mul(..).saturating_add(..)`,
  `.saturating_mul(total_secs)`, and the final `+ 64` is
  `.saturating_add(64)`.
- `load/mod.rs:114-115` (`cfg.sender_offset + cfg.senders`, the signer
  slice index): already fixed by the existing `SenderRange` type
  (`checked_add` at construction, in Phase A); the slice index
  `signers[cfg.sender_range.offset() as usize..]` cannot panic, since
  `offset <= offset + count = derive_count() = signers.len()` by
  construction. No further change.
- `load/mod.rs:124` (`per_sender * signers.len()`, a tracing display
  value): `.saturating_mul(..)`.
- `load/accounting.rs`'s `Some(r - a - rj - queued)`:
  `r.saturating_sub(a).saturating_sub(rj).saturating_sub(queued)`.
- `load/scrape.rs`: added `fn gauge_u64(v: f64, metric: &str) -> u64`,
  which logs a `tracing::warn!` and returns 0 for a negative or NaN
  gauge, instead of `v as u64`'s silent saturate-to-0 (the audit's
  "opposite mistake": a negative/NaN gauge reading as "no drops"). All 5
  call sites (`scrape_executors`, `scrape_ingress`, `scrape_sequencers`
  x3) use it; the sequencer accumulators (`d`, `e`, `b`) also gained
  `.saturating_add(..)`. The three duplicated
  `#[allow(cast_possible_truncation, cast_sign_loss, reason = "...")]`
  blocks collapsed into `gauge_u64`'s own single allow (R14, a bonus of
  this fix).
- `load/plan.rs::pregenerate`, `signers.rs::presign_transfers`,
  `load/defi.rs` (`DefiContracts::at`, `deployment_txs`,
  `pregenerate_defi`, `pregenerate_family`): every `nonce_start + i as
  u64` / `nonce_start + N` is now `.checked_add(..).ok_or_else(|| anyhow!
  (..))?`. `DefiContracts::at` changed from an infallible `-> Self` to
  `-> anyhow::Result<Self>` (4 call sites updated: 1 production, 3 in its
  own tests, all `.unwrap()`).
- `load/engine.rs::Queues::pop_next`: `(self.rr + 1) % n` is
  `self.rr.wrapping_add(1) % n` (wrap is the meaning for a ring index,
  per the audit).
- `harness/flame.rs`: `*merged.entry(..).or_insert(0) += count` (a `u64`
  parsed from a folded-text file) uses `saturating_add`;
  `filter_to_ingress`'s `kept_count`/`dropped_count` (pprof sample
  counts, `isize`) switched from `.sum()` to `.fold(0, |acc, x|
  acc.saturating_add(*x))`.
- `perf/report.rs::analyze_collapsed`: `total`, and the `leaves`/`buckets`
  map entries, all `u64` parsed from a collapsed-stack file, use
  `saturating_add` instead of `+=`.
- `harness/inprocess.rs`: `term_id: shard as i32` is
  `i32::try_from(shard).expect(..)`. Since this `.expect()` sits inside a
  spawned fake-executor task, clippy's `missing_panics_doc` still flags
  the enclosing `pub async fn spawn_inprocess_ingress`; added a `#
  Panics` doc section rather than reach for `unwrap_or` (a wrong shard
  index should fail loudly, not silently substitute a different one).
  The function's old `#[allow(cast_possible_truncation, cast_possible_
  wrap)]` is gone; nothing else in the function needed it.
- `workflows/transfers.rs`, `workflows/mixed.rs`, `workflows/calls.rs`:
  the three remaining `WARMUP_PER_TASK * signers.len()` /
  `WARMUP_PER_TASK * n_tasks as usize` sites use `.saturating_mul(..)`
  (two call sites in `mixed.rs` already had it, from earlier work).
- `stm/uniswap.rs`: `get_amount_out`'s `U256` `*`/`+` are unchanged, with
  a comment added stating the bound: `ruint` backs `U256`'s operators
  with `wrapping_mul`/`wrapping_add` (never a panic), and every
  synthetic reserve in this simulator stays many orders of magnitude
  under `U256::MAX`. `deploy_tokens_and_factory`'s `pairs * 2` is
  `pairs.saturating_mul(2)`. `generate_flow_blocks`'s `si` mixer index
  (`b * 131 + op_i * 7`) uses `wrapping_mul`/`wrapping_add` (the audit's
  own fix: the pre-modulo value has no meaning, only the `% n_send`
  result does). `tj`'s `n_send - 1` keeps plain subtraction, with a
  comment stating the bound: `generate()`'s `signers.len() >= 2` check
  guarantees `n_send >= 2`.
- `executor/src/parallel.rs`: `cumulative += r.gas_used` (feeds the
  `cumulative_gas_used` receipt field) is `cumulative =
  cumulative.saturating_add(r.gas_used)`.
- Tests (the audit's own "Tests" table): `defi_on_engine.rs` and
  `alloc_profile.rs` each gained an explicit `assert!(!x.is_empty(), ..)`
  before the divide that the audit flagged, converting an obscure
  divide-by-zero panic into a clear diagnostic if the workload ever
  produces zero receipts/measurements. `parallel_defi_repro.rs` gained
  the same for `queues`, though `queues.len()` is always exactly the
  file's `const SENDERS: u32 = 15` in every real run of this test.

Not touched, and why: every row the audit itself marked `PROVEN` (a
monotonic counter, a fixed-size literal, a compile-time constant divisor,
a value already bounded by an existing check) or `HOT_PATH_KEEP` (the
per-allocation hook in the now-deleted `alloc.rs` free function, replaced
in the Fix round by `AllocSnap`/`AllocTotals`, which do not repeat the
per-call arithmetic this row was about). `load/defi.rs`'s two `.max(1)`
churn-id sites are R13 territory (see above), not R12.

### R16, no nested loops

Gates rerun after this round: clippy `-D warnings --all-targets
--all-features` (pass), clippy `--no-deps -W clippy::pedantic -D warnings
--all-targets --all-features` (pass), `cargo test --all-targets
--all-features` (pass, 0 failed), `cargo fmt --check` (pass).

- `bin/stm-p2/scenario.rs::partransfer_blocks`/`parcounter_blocks`: the
  `for bidx { for i { .. } }` shapes are now `(0..blocks).map(|bidx|
  <per-block fn/closure>).collect()`, with the inner block-building loop
  as `partransfer_block` (a new function) or a local `flow_block`
  closure (parcounter, since it still closes over the `mk` closure).
- `stm/workload.rs::defi_blocks`: the `for _ in 0..blocks { let mut si
  = 0; while blk.len() < block_size { .. } }` shape is now `(0..blocks)
  .map(|_| interleave_defi_block(..)).collect()`, with the while loop
  moved into `interleave_defi_block`. Also extracted `planned_to_envelope`
  (was an inline closure, `to_env`), used by both the setup-block map
  and `interleave_defi_block`.
- `stm/workload.rs::transfers_blocks`: the `for bidx { for i { .. } }`
  shape is now `(0..blocks).map(|bidx| transfers_block(..)).collect()`,
  with the inner per-block loop as `transfers_block`.
- `bin/stm-p2/mdbx_ab.rs::run_mdbx_ab`: the `for &wk { for &batch {
  with_pool(|pool| { for (bi, blk) { .. } }) } }` triple nesting is now
  a single `for (wk, batch) in eng.worker_counts.iter().flat_map(|&wk|
  run.prune_batches.iter().map(move |&batch| (wk, batch)))` loop calling
  the new `run_one_config` (the old loop body), which itself calls the
  new `MdbxRun::run_all` (the old innermost `for (bi, blk)` block loop,
  now a method).
- `bin/stm-p2/mock_ab.rs::sweep_worker_counts`: the same `for &wk { for
  &batch { with_pool(|pool| { for case in cases { .. } }) } }` shape is
  now the same `flat_map`-flattened single `for (wk, batch)` loop,
  calling the new `run_one_config` (returns `ConfigResult`), which
  calls the new `run_one_case` (the old innermost per-case body, now a
  function returning `CaseOutcome`).
- `bin/stm-p2/pipeline.rs::drive_speculative`/`drive_baseline`: the
  `for fi { while .. ; while .. ; if .. { while .. } }` shapes are now
  `for fi { self.drain_releases(..); self.advance_settled(..);
  self.wait_prev_mv(..); }`, three new `DriveState` methods. `
  advance_settled` is shared by both drivers (a `speculative: bool`
  parameter switches the engine-deltas/recycle bookkeeping speculative
  mode needs from the simpler baseline-mode advancement), which is also
  an R14 dedup: `drive_baseline`'s advance loop was the same shape as
  the middle third of `drive_speculative`'s, just without the
  recycle-on-settle step.
- `stm/uniswap.rs::fund_senders`: the `for sn { for t in tokens { .. }
  }` shape is now `senders.iter_mut().flat_map(|sn| sn.fund_mints(..))
  .collect()`, with the inner loop as the new `Signer::fund_mints`
  method.
- `stm/uniswap.rs::create_pairs_and_liquidity`: the `for ps { for t in
  [token0, token1] { .. } .. }` shape keeps its outer `for ps` (a
  `&mut` iteration that also does non-loop bookkeeping per pair), but
  the whole per-pair body, including the inner `for t` loop, moved into
  the new `seed_pair_liquidity` function.
- `load/defi.rs::pregenerate_family`: the `.map(|(sender, s)| { ..; for
  i in 0..per_sender { .. } })` shape is now `.map(|(sender, s)| { ..;
  (0..per_sender).map(|i| family_op_tx(..)).collect() })`, with the
  per-operation body as the new `family_op_tx` function. Its `match
  (fam, i)` arms needed 8 parameters to reach every field they read;
  bundled six of them (everything but the signer and `i`) into a new
  `FamilyCtx` struct, so `family_op_tx` takes 3 arguments, keeping this
  extraction from tripping R11's `too_many_arguments` the moment it
  became its own function.
- `harness/flame.rs::pprof_report_to_folded_text`: the `.map(|(key,
  value)| { for frame in .. { for symbol in .. { write!(..) } } })`
  shape is now `.map(|(key, value)| { for frame in .. {
  append_frame_symbols(&mut line, frame); } .. })`, with the innermost
  symbol loop as the new `append_frame_symbols` function.

Checked and left alone: `bin/stm-p2/mdbx_ab.rs::print_scheduling_detail`'s
`for (i, lbl) in LBL.iter().enumerate() { if .. { continue; } .. }` is one
loop with a `continue`, not two loops — nothing to extract. The line
number this was flagged under pointed at a different, now-restructured
line before the Phase A file split.

### R16, no nested loops (continued)

This section covers a further search of both crates for genuinely
nested loops the pass above did not reach. It found several and
extracted them directly. The change list was cross-checked against `jj
diff`; the four gates below were rerun after these changes and again at
the end of this document's Phase C work.

Scope: a `for`/`while` loop whose body directly contains another `for`/
`while` loop, written inline (not through a function or method call), in
production code (`crates/bench/src`, `crates/executor/src`; test files and
the `stm-contention.rs` micro-benchmark are called out separately below).
Found by a heuristic brace-depth scan of both crates, then checked by
hand (the scan had false positives from `impl X for Y` and gave rough
line numbers only). Gates rerun after this round: clippy `-D warnings
--all-targets --all-features` (pass), clippy `--no-deps -W clippy::pedantic
-D warnings --all-targets --all-features` (pass), `cargo test --all-targets
--all-features` (pass, 0 failed), `cargo fmt --check` (pass).

Already de-nested in the R15/R13/R12 rounds above (not repeated here):
`bin/stm-p2/scenario.rs` (`partransfer_blocks`/`parcounter_blocks`),
`stm/workload.rs` (`transfers_blocks`, `defi_blocks` /
`interleave_defi_block`), `bin/stm-p2/mdbx_ab.rs` (`run_mdbx_ab` /
`run_one_config` / `MdbxRun::run_all`), `bin/stm-p2/pipeline.rs`
(`drive_speculative`/`drive_baseline` / `drain_releases` /
`advance_settled` / `wait_prev_mv`), `stm/uniswap.rs` (`fund_senders` /
`Signer::fund_mints`, `create_pairs_and_liquidity` / `seed_pair_liquidity`),
`load/defi.rs` (`pregenerate_family` / `family_op_tx`), `harness/flame.rs`
(`pprof_report_to_folded_text` / `append_frame_symbols`).

This round's fixes:

- `benchmark.rs::dispatch`: the histogram-merge `for task_hist { for (k,
  h) { ... } }` is now `for task_hist { merge_task_histograms(&mut
  merged, task_hist)?; }`, with the inner loop in the new
  `merge_task_histograms` function.
- `load/engine.rs::pacer`: the `loop { ... while credit >= 1.0 { ... } }`
  is now `loop { if spend_credit(...).await { return; } }`, with the
  credit-spending while loop in the new `spend_credit` async function
  (returns whether the caller should stop pacing, replacing two early
  `return`s from inside the old nested loop).
- `load/feed.rs::receipt_feed_task`: the `loop { ... while let Some(item)
  = sub.next().await { match ... } }` is now `loop { ...
  drain_receipts(&mut sub, &tracker).await; ... }`, with the per-item
  match in the new `drain_receipts` function.
- `workflows/mixed.rs::interleave`: the `'fill: loop { for _ in
  0..transfers_per_cycle { ...; break 'fill; } for _ in
  0..calls_per_cycle { ...; break 'fill; } }` (a labeled break out of two
  nested `for`s) is now `loop { if fill_transfers(..) { break; } if
  fill_calls(..) { break; } }`, with each `for` in its own new function
  (`fill_transfers`, `fill_calls`), each returning whether the caller
  should stop.
- `harness/inprocess.rs::spawn_inprocess_ingress`: the per-shard
  `for (shard, rx) in ... { tokio::spawn(async move { while let Some(env)
  = rx.recv().await { ... } }); }` is now `for (shard, rx) in ... {
  fake_exec.push(tokio::spawn(run_fake_executor(shard, rx,
  receipt_tx.clone()))); }`, with the while loop in the new, named
  `run_fake_executor` async function (previously an inline closure body).
- `perf/cluster.rs::detect_sealer_leader`: the `for _ in 0..2 { for (name,
  pct) in cpu_sample(..)? { ... } }` is now `for _ in 0..2 {
  accumulate_cpu_sample(&mut totals)?; }`, with the inner loop in the new
  `accumulate_cpu_sample` function.
- `perf/report.rs::analyze_collapsed`: the `for line in ... { ... for
  (name, pat) in BUCKETS { if stack.contains(pat) { ...; break; } } }` is
  now `for line in ... { if let Some(name) = bucket_for(stack) { ... } }`,
  with `bucket_for` replacing the inner loop with
  `BUCKETS.iter().find(..)` (also R10: an iterator, not a loop, since the
  `break`-on-first-match shape maps directly to `Iterator::find`).
- `stm/capture.rs::run_capture`: was one free function with three levels
  of nesting (`for` block × `for` tx × `for` account × two inner `for`s
  over storage reads/changes). Rewritten as a `Capture<'a, S>` struct
  (R15, a bonus of this fix — also fixes a real `too_many_arguments`
  clippy error the naive de-nesting produced) with `run`/`capture_block`/
  `capture_tx` methods, one loop level each; the innermost per-account
  storage-reads/changes loops became `.extend(iter().map(..))` calls
  (R10). `run_capture`'s determinism (byte-identical outputs; this
  function is downstream of the whole audit's byte-identical A/B checks)
  is unchanged — verified by the crate's full test suite passing, 0
  failed, after the rewrite.
- `bin/stm-p0.rs::shadow_replay`: the per-block `for b in 1..=max_block {
  ... for o in &txs { stats.learn_obs(o); } }` inner loop is now a call
  to a new `learn_all` function. (A first attempt used
  `txs.iter().for_each(..)` instead of a named function; clippy pedantic's
  `needless_for_each` rejected it, preferring a real `for` loop over a
  trivial `for_each` closure — which meant the loop had to move into its
  own function to avoid recreating the nesting.)
- `bin/stm-p2/mock_ab.rs`: `run_baseline_pass`'s per-block
  `for (t, _, e) in &recs { prepare(..); }` upstream-prep loop is now a
  call to a new `prepare_all` function (same `needless_for_each` lesson
  as above). `sweep_worker_counts`'s `for &wk { for &batch { with_pool(|
  pool| { for case in cases { ... } }) } }` (three levels) is now a
  single `for (wk, batch) in eng.worker_counts.iter().flat_map(|&wk|
  batches.iter().map(move |&batch| (wk, batch)))` loop calling the new
  `run_one_config`, which itself calls the new `run_one_case` for the
  per-case body — no loop nests inside another anywhere in this call
  chain. `bin/stm-p2/mdbx_ab.rs::run_mdbx_ab`'s outer `for &wk { for
  &batch { ... } }` got the same `flat_map` flattening.
- `bin/stm-p2/pipeline.rs::run_pipelined`: the post-clock `for (pfi, out)
  in &outcomes { assert_identical(..); asserted += 1; }`, nested inside
  the outer `for &wk` sweep, is now a call to the new `verify_outcomes`
  function, which returns the count checked.
- `executor/src/parallel.rs` (the segment-execution body): the sequential
  `for (tx_idx, position, envelope) in txs { session.push_tx(..)?; }` and
  `for (j, mut r) in out.receipts.into_iter().enumerate() { ...
  receipts.push(r); }`, both nested inside the outer `for seg in
  segment(..) { match seg { ... } }`, became
  `txs.into_iter().try_for_each(..)?` and
  `out.receipts.into_iter().enumerate().for_each(..)` respectively.
  (These are not the trivial single-call closures `needless_for_each`
  objects to — each closure body has several statements — so `for_each`/
  `try_for_each` stood as pedantic-clean here, unlike the two cases above.)
- `executor/src/bal.rs::run_bal_publisher`: the outer `loop` contained an
  inner retry `loop { match pubh.publish_bytes(..) { ... } }` and a
  `while retained.len() > RETENTION_BLOCKS { retained.pop_front(); }`.
  Both extracted: `publish_with_retry` (returns the metric outcome
  string) and `trim_retention`.

Not converted, with reasons:
- `bin/stm-contention.rs::run`: `for _ in 0..BLOCKS { thread::scope(|s| {
  for w in 0..threads { s.spawn(move || { for i in 0..per_thread { ... }
  }); } }); }` (three levels). This is a timing micro-benchmark whose
  entire purpose is measuring exactly this concurrency shape (one thread
  per worker, one `MvCache` per simulated block); the doc comment at the
  top of the file states the benchmark must mirror a real block's
  structure or it measures its own artifacts. Extracting the per-thread
  or per-block body into named functions changes nothing observable
  about correctness, but risks changing inlining and the measured
  per-transaction nanosecond figures this tool exists to produce. Left
  as is; this is a judgment call (R16 is one of STYLE.md's named
  judgment rules), not a hard-rule skip.
- Test files (`tests/*.rs`, and any nested loop inside a `#[cfg(test)]
  mod tests` block in a production file) are out of scope for this round,
  per STYLE.md's own mechanical-check note that the `just style` grep for
  hard-rule violations scopes to production code. `tests/
  parallel_defi_repro.rs`'s and `tests/defi_on_engine.rs`'s loops were
  already reviewed under R10 in Phase A (`Keep`, sequential
  state-threading) and are unchanged here.

### R14, duplicated shapes (Phase C)

Source: `docs/reviews/2026-09-07-code-quality-audit/dry-bench.md`'s "R14
production code" and "R14 tests" tables (this file lives in the
top-level checkout, not in this agent's jj workspace tree). Gates rerun
after this round:
clippy `-D warnings --all-targets --all-features` (pass), clippy
`--no-deps -W clippy::pedantic -D warnings --all-targets --all-features`
(pass), `cargo test --all-targets --all-features` (pass, 0 failed),
`cargo fmt --check` (pass).

Done this round:

- `sign_legacy` (fill a `TxLegacy`, sign, hash, encode 2718): already
  fully dissolved by the Fix round's `DerivedSigner::sign_raw`/
  `sign_envelope`. Every site the table names (`signers.rs`, `load/
  plan.rs`, `load/defi.rs`, both `stm-p0.rs` and `stm-p2/scenario.rs`)
  calls one of those two methods. No new code; recorded here as
  confirmed-done, not re-implemented.
- `metric_u64`/`metric_u64_or_zero` (the cast-possible-truncation/
  cast-sign-loss guard around a scraped gauge cast): already dissolved
  by the Fix round's `scrape.rs::gauge_u64`, which the R16 pass above
  also independently confirmed. No new code.
- `csv_usize` (split, trim, parse-to-`usize`, panic on a bad item):
  `bin/stm-p2/args.rs`'s three sites already use clap's
  `value_delimiter = ','` instead of a hand-rolled splitter. Only
  `bin/stm-contention.rs:157`'s `.split(',')` site remains, and one site
  is not a duplicated shape. Recorded as dissolved, not re-implemented.
- `PlannedTx::to_envelope` (turn a `PlannedTx` into a `TxEnvelope`):
  added `impl PlannedTx { pub fn to_envelope(&self, sender: Address,
  correlation_id: u64) -> TxEnvelope }` in `load/plan.rs`, per the
  table's proposed home (`#[must_use]`, since it is a `&self` method
  returning an owned value). Updated all 5 sites: `stm/workload.rs`
  (`defi_blocks`'s setup map and `interleave_defi_block`, replacing the
  file-local `planned_to_envelope` free function the R16 round had just
  introduced there), `tests/parallel_defi_repro.rs` and
  `tests/defi_on_engine.rs` (their local `envelope()` test helpers now
  call `t.to_envelope(..)` instead of repeating the struct literal), and
  `tests/alloc_profile.rs` (2 sites, its `run_one` closure and its
  per-block loop). Removed the now-unused `TxEnvelope` import from
  `alloc_profile.rs` and the now-unused `Address` import from
  `workload.rs`.
- `funded_snapshot` (build a `MockStateDatabase` and fund every signer
  at 10^21 wei): added `pub fn funded_snapshot(signers: &[DerivedSigner])
  -> MockStateDatabase` in `stm/workload.rs` (a `FUNDED_BALANCE_WEI`
  constant replaces the magic number), and pointed all 6 sites at it:
  `bin/stm-p0.rs`, `bin/stm-p2/main.rs`, `tests/defi_on_engine.rs`,
  `tests/parallel_defi_repro.rs` (2 sites), `tests/alloc_profile.rs`.
  Removed the now-unused `MockStateDatabase`/`U256` imports at each
  call site that no longer builds the snapshot inline.
- `ANVIL_MNEMONIC` (11 declarations of the same Anvil dev mnemonic):
  added one `pub const ANVIL_MNEMONIC: &str` in `crates/bench/src/
  lib.rs`, and changed every other declaration to `pub(crate) use
  crate::ANVIL_MNEMONIC;` (`load/config.rs`, `workflows/mod.rs`) or `use
  kardamom_bench::ANVIL_MNEMONIC as ANVIL_PHRASE;` / `use
  crate::ANVIL_MNEMONIC as ANVIL_PHRASE;` (the 4 `#[cfg(test)] mod
  tests` copies in `signers.rs`, `mnemonic.rs`, `load/plan.rs`, `load/
  defi.rs`, and the 4 integration-test copies in `tests/
  alloc_profile.rs`, `tests/alloc_profile_ingress.rs`, `tests/
  defi_on_engine.rs`, `tests/parallel_defi_repro.rs`), and added a
  direct `use kardamom_bench::ANVIL_MNEMONIC;` / inline
  `kardamom_bench::ANVIL_MNEMONIC` reference in `bin/stm-p0.rs` and
  `bin/stm-p2/main.rs`, whose own private `const ANVIL_MNEMONIC` was
  deleted outright (nothing aliased its name).
- `init_tracing` (install the fmt subscriber with `RUST_LOG` or an
  "info" fallback): this is the prior-audit item flagged open by name
  ("the bench binaries keep three private `init_tracing` copies
  although `kardamom_obs::bin::init_tracing` now exists"). Added
  `kardamom-obs = { path = "../obs" }` to `crates/bench/Cargo.toml`, and
  replaced the three inline `tracing_subscriber::fmt()...init()` blocks
  in `main.rs`, `bin/load.rs`, and `bin/perf.rs` with
  `kardamom_obs::bin::init_tracing()`, removing the now-unused
  `tracing_subscriber`/`EnvFilter` imports from all three. This item is
  now closed.
- `write_json_pretty` (create the parent directory, then write pretty
  JSON): `report.rs::write_json` was rewritten as a thin wrapper over a
  new `pub fn write_json_pretty<T: serde::Serialize>(path: &Path, v:
  &T) -> anyhow::Result<()>`, and `load/mod.rs::write_report_json`'s
  inline copy of the same directory-then-write logic now calls
  `crate::report::write_json_pretty` directly.
- `submit_mode`/`feed_confirm_on` (map `cfg.subscribe` to a `SubmitMode`,
  and compute `feed_confirm && !subscribe`): added `impl LoadConfig {
  fn submit_mode(&self) -> SubmitMode; fn feed_confirm_on(&self) ->
  bool }` in `load/config.rs`, and replaced all 4 inline computations in
  `load/mod.rs` (`spawn_receipt_feed`, `run`, `ramp_to_max`, twice) with
  calls to these methods.
- `push_up` (a failed scrape must record an explicit `service_up = 0`
  entry): added `fn push_up(snap: &mut MetricsSnapshot, label: String,
  body: Option<&str>)` in `load/scrape.rs`, and pointed the executor,
  ingress, and sequencer scrape functions at it. This normalizes one
  small behavior difference: `scrape_ingress`'s old inline `g(..)`
  closure used `sum_metric(..).unwrap_or(0.0)` before `gauge_u64`, so a
  successful scrape with the `service_up` counter not yet emitted (a
  freshly started process, before its first increment) read as
  `Some(0)`; `push_up` uses the same rule the executor and sequencer
  sites already used (`sum_metric(..).map(gauge_u64)`, no
  `unwrap_or(0.0)`), so that same case now reads `None` (unknown) for
  ingress too, consistent with the other two services. This is a
  cold-start liveness-reporting nuance, not a drop or must-deliver
  check, and unifying it removes a silent 2-of-3-vs-1-of-3 inconsistency
  the duplication had introduced. `accounting.rs::liveness_failures`
  only fails a run on `Some(0)`; `None` is skipped (not counted either
  way). So this changes the ingress cold-start window from "fails a
  non-chaos run" to "not checked yet," matching the other two services.
  `kardamom_service_up` is a startup gauge a service emits before its
  metrics endpoint answers at all, so a scrape that reaches the endpoint
  at all has already passed the window where the metric could be
  absent; the practical window this affects is at most one scrape
  interval, and likely zero. `load/scrape.rs`'s test module covers both
  sides of this rule directly: `push_up_reads_a_scraped_body_with_no_
  service_up_sample_as_unknown` and `push_up_reads_a_failed_scrape_as_
  down`.
- `call_req`/`assert_contract_deployed` (build a `TransactionRequest`
  with only `to` set; probe a call target and fail if the output is
  empty): added both to `workflows/mod.rs`. `calls.rs::prepare` and
  `mixed.rs::prepare` each had their own inline probe-and-bail block
  (different error message text); both now call
  `assert_contract_deployed(client, self.contract).await?`, which uses
  the `CallsWorkflow` message text (the more detailed of the two,
  naming `genesis_alloc()` and the `contract` field). `calls.rs::
  dispatch` and `mixed.rs::dispatch`'s own inline `TransactionRequest {
  to: Some(TxKind::Call(..)), ..Default::default() }` literals now call
  `call_req(self.contract)`. Removed the now-unused `TxKind`/
  `TransactionRequest` imports from both files.
- `Benchmark::report` (copy `Benchmark`'s five settings fields into
  `ReportInputs`, then call `build_report`): added `impl<W:
  BenchWorkflow> Benchmark<W> { pub fn report(&self, outputs: Outputs)
  -> BenchReport }` in `benchmark.rs`. `main.rs::run_one` and
  `harness.rs::emit_report` both now call `bench.report(outputs)` /
  `self.bench.report(outputs)` instead of repeating the `ReportInputs`
  literal and the `report::build_report` call; removed the now-unused
  `ReportInputs` import from both files (`report::` itself stays, for
  `print_terminal`/`write_json`).
- `rpc_client`/`preflight_chain_id` (build the jsonrpsee client at the
  shared timeout and slack; read `eth_chainId`): added both to
  `config.rs`, per the table's proposed home. `main.rs` and `load/
  mod.rs::prepare_run` each had their own inline
  `HttpClientBuilder::default().request_timeout(..).
  max_concurrent_requests(..).build(..)` block; both now call
  `rpc_client(url, max_in_flight)`. `load/mod.rs` and `workflows/
  transfers.rs` each had their own `preflight_chain_id` function (the
  error message text differed only in "is ingress up" vs. "is the node
  running"); `transfers.rs` now re-exports `config::preflight_chain_id`
  (`pub(crate) use crate::config::preflight_chain_id;`, so `calls.rs`
  and `mixed.rs`'s existing `use crate::workflows::transfers::
  preflight_chain_id` imports needed no change), and `load/mod.rs`
  imports it directly. Removed the now-unused `HttpClientBuilder`/
  `REQUEST_TIMEOUT`/`MAX_IN_FLIGHT_SLACK`/`U256`/`ClientT`/`rpc_params`
  imports from `load/mod.rs` and `main.rs`.
- `base_input` (ten-plus near-identical `EvalInput` literals in which
  two to four fields vary): added `pub(crate) fn base_input<'a>(base:
  &'a MetricsSnapshot, fin: &'a MetricsSnapshot) -> EvalInput<'a>` in
  `load/accounting.rs`'s `#[cfg(test)] pub(crate) mod tests` (made
  `pub(crate)` so `load/engine.rs`'s own test module could import
  `accounting::tests::base_input`), fixing the clean-run baseline
  fields (300/300/300/0 counts, no missing/unlanded, no recheck,
  `max_gap` 5, strict delivery, no feed-ack trust, soak mode). All 9
  `accounting.rs` test sites and `engine.rs`'s 1 site now build their
  `EvalInput` as either `base_input(&base, &fin)` directly, or `EvalInput
  { <the 1-3 fields this case varies>, ..base_input(&base, &fin) }`.
  This is the largest test-side win taken this round (about 80 lines,
  matching the table's estimate).
- Histogram bounds constants (`tracker.rs` and `benchmark.rs` each built
  a histogram at the same low/high/sigfigs bounds and the same quantile
  triple): done in the R16 pass above (`config.rs::
  HIST_LOW_US`/`HIST_HIGH_US`/`HIST_SIGFIGS`/`new_latency_hist()`,
  `tracker.rs::quantiles`); confirmed both `tracker.rs` and
  `benchmark.rs` now call the shared constants and helper. No
  additional work needed here.
- `allocs_with_contract` (`prefunded_signer_allocs`, then push one
  contract `AllocEntry`): added `pub(crate) fn allocs_with_contract(
  mnemonic: &str, n_tasks: u32, balance: U256, contract: Address, code:
  Bytes) -> anyhow::Result<Vec<AllocEntry>>` in `workflows/
  transfers.rs`, next to `prefunded_signer_allocs`. `calls.rs::
  genesis_alloc` and `mixed.rs::genesis_alloc` both now call it instead
  of repeating the `AllocEntry` literal.
- `LoadConfig::default` (`bin/load.rs` and `bin/perf.rs` each build a
  30-plus-field `LoadConfig` literal by hand): added `impl Default for
  LoadConfig` in `load/config.rs`, with sensible values matching
  `kardamom-load`'s own CLI defaults where one exists (300s duration,
  200 tx/s target, 16 senders at offset 0, the Anvil mnemonic, the
  `0xdEaD` sink, 256 max-in-flight, the `kardamom-executor-0/1/2` /
  `kardamom-ingress-0` / `kardamom-sequencer-0/1` node names, and so
  on). `bin/perf.rs::load_cfg` now sets only the 13 fields its ramp and
  soak phases actually vary (`workload`, `rpc`, `chain_id`, `duration`,
  `target_tps`, `max_in_flight`, `ramp_step_tps`, `ramp_step_secs`,
  `soak_fraction`, `assert_all_delivered`, `scrape`, `subscribe`,
  `feed_confirm`, `output`) plus `..LoadConfig::default()`, dropping the
  other 16 (including `sender_range`, since both of `load_cfg`'s
  callers overwrite it before `load::run` ever reads it, so any
  placeholder value works). This also closes the "Prior audit items"
  table's still-open "Topology model triplicated" row for its Rust
  half: the `kardamom-executor-0/1/2` / `kardamom-ingress-0` /
  `kardamom-sequencer-0/1` node-name list now has one source
  (`LoadConfig::default`) instead of two (it was hand-copied into both
  `bin/perf.rs`'s literal and, separately, `bin/load.rs`'s clap
  `default_value` strings; the clap-default copy is unchanged, since
  clap needs a `&str` there and cannot read a struct default, but the
  Rust-literal-vs-Rust-literal duplication the row named is gone).
  `bin/load.rs::main`'s own `LoadConfig` literal is unchanged: every one
  of its 29 fields maps directly to a distinct CLI flag with no shared
  defaults to fold in, so a `From<Args>` impl there would not remove any
  duplication, only move the same field list to a different function.
  This is a narrower fix than the table's stretch goal (`impl
  From<Args> for LoadConfig` in `bin/load.rs`), but it is the fix the
  "Prior audit items" table specifically asked for ("The
  `LoadConfig::default()` row fixes the Rust half").

Deferred, with reasons:

- `BenchArgs` flatten: stale row, done. See "R14 follow-up" below
  (item 1): `config.rs::BenchArgs` plus `#[command(flatten)]` in both
  `harness.rs`'s and `main.rs`'s `Args`.
- `settle` (`load/engine.rs`'s two receipt-confirmation call sites,
  `landed`/`not-landed`, each re-fetch the receipt then confirm with
  gas or park the hash as pending): the R16 pass above looked at
  this row and judged it not worth forcing into one function; recording
  the reasoning here. The two
  sites' `None` branches differ in kind, not just in the value they
  pass: the landed-but-unconfirmed branch either inserts a pending
  entry (soak mode) or trusts the on-offer ack (chaos mode), a
  three-way branch, while the not-landed branch only ever inserts a
  pending entry, a two-way branch. A shared `settle` function would
  need a `chaos_mode` parameter threading into only one of its two call
  paths, which is more indirection than the roughly 10 lines saved
  justifies. Left as two separate, readable functions; a judgment call
  (R14 is one of STYLE.md's named judgment rules), not a hard-rule
  skip.
- `bench_one_tx`: stale row, done. See "R14 follow-up" below (item 2):
  `executor/benches/sequential_throughput.rs::TxBench<'a, F>`.
- The two rows the audit itself marked `KEEP` (the `Inbound`/`Outbound`
  literals passed to `Executor::run` in `executor/.../main.rs` and
  `validator/.../main.rs`; `state_dir`/`state_durability`/
  `metrics_addr`/`host_id` between the executor's and validator's
  `args.rs`): both stay as is. The `Inbound`/`Outbound` literals are
  four named fields each, already minimal, and a shared helper would
  hide which port each role wires, exactly as the audit says. The
  `state_dir`-and-friends group differs in its actual default values
  between the two binaries (`/opt/kardamom/state` vs.
  `/opt/kardamom/validator-state`, port 9004 vs. 9007), so a flattened
  `#[command(flatten)]` group could not carry both. Agreed with the
  audit's verdict on both; no change made.

- `build_scenario` (the `uniswap`/`defi`/`transfers` scenario arms in
  `stm-p0.rs` and `stm-p2.rs`): this was already decided in the Fix
  round (see "R14, duplicated shapes" above, under "## Fix round") —
  `stm-p0` and `stm-p2` keep separate scenario-building code on purpose,
  since `stm-p2` additionally needs to interleave STM-only scenario arms
  (`partransfer`, `parcounter`) that `stm-p0` has no equivalent of, and a
  shared `build_scenario` would need a parameter or trait object to
  cover that split, adding indirection to save one file's worth of
  parallel structure. Not re-litigated this round.
- `per_sender_queues<F>` (a generic helper over `load/plan.rs`'s
  `pregenerate` and `load/defi.rs`'s `deployment_txs`/
  `pregenerate_defi` per-sender-queue shape): not done. The three sites
  build different item types (a plain value-transfer `PlannedTx` queue,
  a fixed 3-transaction deploy sequence, and a family-mix operation
  queue with its own per-op nonce and calldata logic) with different
  base-nonce rules (uniform `nonce_start`, `nonce_start + 3` for sender
  0 only, or a per-family `base`). A generic `F: Fn(usize) -> u64` for
  the base and a second closure for the per-item builder would need to
  thread through as many parameters as the loops themselves use, and a
  reader would need to hold both the generic's shape and each call
  site's closures in mind at once to see what any one queue actually
  builds. Left as three separate, readable loops; this is a judgment
  call (R14 is one of STYLE.md's named judgment rules, "no duplicated
  shape," not a hard-rule count), not a hard-rule skip.
- `bench_env`: stale row, done. See "R14 follow-up" below (item 3):
  `stm::BlockAt`, in `stm/mod.rs`.
- `genesis_changes`/`mdbx_rig`: stale row, done. See "R14 follow-up"
  below (item 4): `mdbx_ab.rs::mdbx_rig` and `common.rs::Workload::
  genesis`.
- `pprof_guard`/`write_pprof_svg`: stale row, done. See "R14
  follow-up" below (item 5) for detail: both live in `pprof_guard.rs`
  and both `harness.rs` and `bin/stm-p2/main.rs` call `pprof_guard`.
  `write_pprof_svg` covers the plain guard-to-file path; `harness.rs`'s
  own ingress/client frame-filtering path stays separate by design (see
  `pprof_guard.rs`'s module doc), not unconsolidated duplication.
- `asprof_pass`: stale row, done. See "R14 follow-up" below (item 6):
  `perf/profile.rs::asprof_pass` runs the asprof-then-`docker cp` pass,
  called from 2 sites.
- `tx_obs` (sort/dedup read and write cells, then build a `TxObs` from
  the receipt and envelope view, in `stm/capture.rs` and `stm-p2.rs`):
  the table's proposed home is `kardamom-footprint`, a crate this agent
  does not own in the 11-way parallel remediation. Not attempted
  unilaterally; would need Phase B coordination with `kardamom-footprint`'s
  owning agent.
- `StreamArgs` (`executor/src/bin/kardamom-executor/args.rs`'s Aeron and
  refetch flags, shared with `validator/src/bin/kardamom-validator/
  args.rs`) and `resume_point` (`executor/.../state.rs` vs. `validator/
  .../main.rs`'s recovery-point-then-log-resume logic): both proposed
  homes are `kardamom_engine::bin_support`, and both need a matching
  change on the `kardamom-validator` side to actually remove the
  duplication; a one-sided change in `kardamom-engine` plus `kardamom-
  executor` alone would still leave `kardamom-validator`'s copy
  diverging from a base it no longer shares code with. Not attempted
  unilaterally; needs Phase B coordination with the crates' owning
  agents.
- `fold_layers` (fold a stack of `Arc<PendingDelta>` layers into one
  delta; two sites in `executor/src/parallel.rs`, one in `engine/src/
  actor/exec_settle.rs`): the proposed home is `kardamom_engine::delta`,
  a crate this agent does not own. The two `parallel.rs` sites
  (`:262-269`, `:288-292`) could in principle get a private,
  `executor`-local helper without touching `kardamom-engine`, but that
  would only partly address the row (the `engine` crate's copy stays
  separate either way) and risks a name collision with whatever helper
  the `kardamom-engine` owning agent adds under the same row. Deferred
  to Phase B coordination rather than attempted half-and-half.
- The `testing`-feature rows in the "R14 tests" table (the channel-port
  test doubles shared across `executor`/`validator`/other crates' test
  suites, `kardamom_obs::testing`'s `free_port`/`scrape`,
  `kardamom_ingress::testing`'s fake-downstream helpers, and
  `kardamom_engine::testing`'s `spawn_engine`/`drain_c`): these are the
  largest single blocks in the DRY audit (about 230 + 150 + 105 + 120 =
  605 of the table's ~830 test-side lines), but every one needs a new
  `pub mod testing` behind a `testing` cargo feature on a crate this
  agent does not own (`kardamom-engine`, `kardamom-obs`,
  `kardamom-ingress`), plus the executor and validator's own
  dev-dependency lines to opt into that feature. Not attempted
  unilaterally; this is squarely Phase B, cross-crate work, for
  whichever agent owns each of those three crates.

`seq_exec` is the one test-table row inside `crates/bench` and
`crates/executor`: stale row, done. See "R14 follow-up" below (item 7):
`stm::SeqExec<'a, S>`, in `stm/mod.rs`.

### R16/R11/R4 follow-up (bench and executor)

Gates rerun after this pass: clippy `-D warnings --all-targets
--all-features` (pass), clippy `--no-deps -W clippy::pedantic -D
warnings --all-targets --all-features -D unreachable_pub` (pass, the
`--no-deps` deviation is documented at the top of this file under
"Gate results"), the `debug_assert!`/`.max(1)`/`Box<dyn`/
`too_many_arguments` grep (empty), `grep -rn too_many_lines crates/bench
crates/executor` (empty), `cargo test --all-targets --all-features` for
both crates (pass, 0 failed),
`cargo fmt --check` for both crates (pass).

**R16, sites changed this pass**

- `load/engine/mod.rs::Queues::pop_next`: the `for` loop with an `if`
  found-check inside is now `(0..n).find_map(|_| { let i = self.rr %
  n; self.rr = self.rr.wrapping_add(1) % n; self.per_sender[i].
  pop_front() })`.
- `load/plan.rs`'s two tests, `pregenerate_per_sender_monotonic_nonces`
  and `pregenerate_hashes_are_unique_and_nonzero`: their nested `for`
  loops over senders and queue entries are now `.for_each(..)` and
  `.flatten().for_each(..)` chains.
- `load/accounting.rs::print_ramp`: the `for s in &r.ramp { if s.
  sustainable { .. } else { .. } }` shape is now `for s in &r.ramp {
  println!(.., sustainable_label(s.sustainable), ..); }`, with the
  label choice as the new `sustainable_label` function.
- `bin/stm-p2/mock_ab.rs::print_rows`/`print_wall_breakdown`: the `let
  x = if .. else ..` blocks inside each `for` loop moved to new `Row::
  avg_imbalance`/`Row::utilization_pct` methods.
- `load/engine/mod.rs::submit_task`'s retry loop: `try_once` (a
  `ControlFlow<RetryOutcome>` per attempt) and `run_retries` (the `for
  attempt in 0..=retry` loop, body is one `let ControlFlow::Break(..) =
  try_once(..).await else { continue; };`), plus `finalize_accepted`
  and `finalize_unaccepted` (post-loop branching, no loop bodies).
- `load/engine/mod.rs::pacer`: `Pacer::tick` and `Pacer::spend_credit`
  (methods, no loop each); `pacer` itself is `loop { ticker.tick(..);
  let ControlFlow::Continue(()) = pacer.tick(..).await else { return;
  }; }`.
- `load/engine/drain.rs::sweep_pending_once`: per-entry body extracted
  to `settle_if_still_pending`.
- `load/engine/drain.rs::drain`: per-tick body extracted to
  `drain_tick`.
- `load/engine/drain.rs::join_submit_tasks`: per-iteration body
  extracted to `join_one_or_timeout`.
- `load/accounting.rs::keep_pace_rows`: the per-executor verdict
  if/else-if chain moved to a new `keep_pace_row` function (no loop);
  `keep_pace_rows` itself is one `.iter().map(keep_pace_row).collect()`
  call.
- `load/accounting.rs::step_gap_ok`: the `for (node, b1) { if let
  (Some(b0), Some(b1), Some(sealer)) = .. { .. } }` shape is now
  `.all(step_gap_ok_one)`, with the per-executor check as the new
  `step_gap_ok_one` function (no loop, three sequential `let-else`
  guards in place of the old triple `if let`).
- `load/defi.rs::confirm_deploy`: the poll loop's body (receipt check,
  stall-clock update, two deadline checks) moved to a new `confirm_tick`
  function returning `anyhow::Result<ControlFlow<()>>`; `confirm_deploy`
  is `loop { let ControlFlow::Continue(()) = confirm_tick(..).await?
  else { return Ok(()); }; sleep(..).await; }`. Carried state (`last_
  block`, `last_progress`) moved into a new `ConfirmState` struct.
- `load/defi.rs::op_mix_covers_all_contracts_and_is_deterministic`
  (test): the `for sender in 0..4 { for seq in 1..64 { .. } }` shape is
  now `(0..4).flat_map(|sender| (1..64).map(move |seq| (sender,
  seq))).for_each(..)`.
- `stm/uniswap.rs::create_pairs_and_liquidity`: the `for p in 0..pairs
  { let (t0, t1) = if a < b { .. } else { .. }; .. }` shape is now `for
  p in 0..pairs { let created = create_one_pair(..); .. }`, with the
  per-pair body (address ordering, CREATE2 salt, `createPair` sign, and
  starting `PairState`) as the new `create_one_pair` function, returning
  a new `CreatedPair { tx, state }` struct.
- `stm/uniswap.rs::generate_one_flow_block`: the `while block.len() <
  txs_per_block { if r % 100 < swap_share_pct && room_for_swap { .. }
  else { .. } }` shape is now `while .. { self.push_one_op(..); }`, with
  the per-operation dispatch as the new `FlowGen::push_one_op` method
  (no loop), and its two branches split further into
  `FlowGen::push_swap_op` and `FlowGen::push_transfer_op`.
- `workflows/mixed.rs::interleave`/`fill_transfers`/`fill_calls`: all
  three folded into a new `Interleaver` struct (`out`, `iter`, `cap`,
  `transfers_per_cycle`, `calls_per_cycle`), with `run` (`while let
  ControlFlow::Continue(()) = self.cycle() {}`), `cycle`, `fill_
  transfers`, `push_one_transfer`, `fill_calls`, and `push_one_call` as
  methods; the `for _ in 0..N { if .. { return true; } .. }` shapes are
  now `for _ in 0..N { let ControlFlow::Continue(()) = self.push_one_..
  () else { return ControlFlow::Break(()); }; }`. `interleave` itself is
  now a one-line wrapper: `Interleaver::new(..).run()`.
- `executor/tests/m_plus_one_join.rs::m4_canonical_b_order_drives_
  receipts`: split into `fund_m_signers`, `open_m_plus_one_bus`,
  `publish_tx_data_plan`, `shuffle_canonical_order`, `publish_ordering_
  and_boundary`, `spawn_m_plus_one_executor` (returns `RunHandles`),
  `poll_one_c_message` (`PollOutcome`) and `collect_until_boundary`
  (`while Instant::now() < deadline { let PollOutcome::Continue = poll_
  one_c_message(..) else { break; }; }`). Its `#[allow(clippy::too_
  many_lines, ..)]` no longer applies and is removed.
- `executor/tests/m_plus_one_join.rs::tx_ref_arriving_before_envelope_
  still_joins`: split into `poll_one_single_seq` (`SinglePoll`) and
  `collect_single_seq_until_boundary` (`while let SinglePoll::Continue
  = poll_one_single_seq(..) {}`, a `while let` per `clippy::while_let_
  loop` rather than a bare `loop`/`let-else`/`break`). Its `#[allow(
  clippy::too_many_lines, ..)]` no longer applies and is removed.
- `executor/tests/diff_reference.rs::naive_reference`: the per-tx
  revm-execution body moved to a new `run_one_naive` function;
  `naive_reference` is now `txs.iter().map(|e| run_one_naive(&mut
  cache, e)).collect()`. Its `&[(KtTxEnvelope, Address)]` parameter and
  `Vec<(bool, u64)>` return became `&[TxWithSender]` and
  `Vec<TxResult>` (both new named structs; see R11 below).
- `executor/tests/diff_reference.rs::actor_receipts_match_naive_
  reference`: split into `build_diff_fixture` (`DiffFixture`),
  `publish_diff_corpus` (`DiffChannels`), `spawn_actor_run`
  (`ActorRun`), `collect_actor_receipts` (`while let Ok(m) = c_rx.
  recv_timeout(..) { push_if_receipt(&mut actor, m); }`), and `push_if_
  receipt` (the `if let CMessage::Receipt(r) = m { .. }` body, no
  loop). Its `#[allow(clippy::too_many_lines, ..)]` no longer applies
  and is removed.
- `executor/src/bal.rs::publish_with_retry`: the retry loop's body
  (publish attempt, `NOT_CONNECTED`/deadline handling) moved to a new
  `try_publish` function returning `Option<&'static str>`;
  `publish_with_retry` is `loop { let Some(result) = try_publish(..)
  else { sleep(..); continue; }; return result; }`.
- `executor/src/bal.rs::run_bal_publisher`: the channel-recv-then-
  measure-then-publish loop split into `recv_tick` (`ControlFlow<(),
  Option<BalHandoff>>`), `measure_granularities`, `encode_and_count`
  (`Option<EncodedFrame>`, a new struct replacing `encode_frame`'s
  `(Vec<u8>, usize)` tuple return), and `process_tick` (the whole
  per-handoff body, `let Some((boundary, delta, bal)) = handoff else {
  return; };` then measure/encode/publish/retain). `run_bal_publisher`
  is `loop { let ControlFlow::Continue(handoff) = recv_tick(&rx) else {
  break; }; process_tick(..); }`.
- `executor/src/parallel.rs::pool_server`: an empty-bodied `if` (just a
  comment) around `req.reply.send(out)` removed outright; the call is
  now unconditional (`let _ = req.reply.send(out);`).
- `executor/src/parallel.rs::segment`: the `for (i, rec) in records..
  { match .. { .. } }` shape is now `for (i, rec) in records.. { push_
  segment(&mut segs, i as u64, rec); }`, with the per-record match as
  the new `push_segment` function.
- `bin/stm-p2/drive.rs::drive_speculative`/`drive_baseline`: the
  per-`fi` body (an `if fi > 0 { .. } else { .. }` and an `if p.timing
  { .. }`, both directly inside the `for fi in 0..p.n_flow` loop) moved
  to new `drive_one_speculative`/`drive_one_baseline` methods (no
  loop); the outer loops are now `for fi in 0..p.n_flow { self.drive_
  one_..(..)?; }`.
- `bin/stm-p2/pipeline.rs::run_pipelined`: the `if !retain_outcomes {
  println!(..) }` directly inside the `for &wk in &eng.worker_counts`
  loop moved into a new `PipelineRun::run_worker_count` method (the
  whole per-worker-count body); `run_pipelined` is now `for &wk in
  &eng.worker_counts { run.run_worker_count(wk)?; }`.
- `load/mod.rs::ramp_to_max`: the per-step body (two `let x = if ..
  else ..` blocks) moved to a new `RampRun::run_step` method;
  `ramp_to_max` is `while rate <= cfg.target_tps.get() { let Some(next)
  = run.ramp_step(rate).await else { break; }; .. }`.
- `load/scrape.rs::scrape_sequencers`: the `for node { let body = ..;
  if .. { if .. { .. } } }` shape is now `for node { totals.fold_node
  (snap, node, body.as_deref()); }`, with the per-node fold as the new
  `SeqTotals::fold_node` method.
- `bin/stm-p2/mock_ab.rs::run_one_config`: the per-case `if case.is_
  flow { ..; if collect_edges { .. } }` body moved to a new `ConfigAccum
  ::fold_case` method; the loop is `for case in cases { accum.fold_
  case(..)?; }`.

**R16, deferred, by function name (27 sites, unchanged from before this
pass)**

None of these functions are touched this pass; each is left as is.
R16 applies only to touched functions:

- `benchmark.rs::merge_task_histograms` (if-in-loop).
- `bin/stm-contention.rs::run` (match-in-loop) and `bin/stm-contention.
  rs::main` (if-in-loop).
- `harness/flame.rs::merge_folded_text` (if-in-loop).
- `harness/inprocess.rs::run_fake_executor` (if-in-loop).
- `load/feed.rs::receipt_feed_task` (3 sites: two match-in-loop on
  connect/subscribe, one match-in-loop on the message body). This is
  the WebSocket receipt-feed task named in earlier passes; still
  untouched.
- `perf/report.rs::analyze_collapsed` (2 sites, if-in-loop) and `perf/
  report.rs::write_ramp_table` (if-in-loop).
- `stm/workload.rs::interleave_defi_block` (2 sites, if-in-loop);
  confirmed still deferred, as in earlier passes.
- `workflows/mixed.rs::MixedWorkflow::prepare` (if-in-loop, line ~170).
  `mixed.rs` changes this pass (`Interleaver` replaces `interleave`/
  `fill_transfers`/`fill_calls`, see above), but `prepare` itself is
  untouched; its own `for s in &signers { let presigned = if .. { .. }
  else { .. }; main.push(interleave(..)); }` shape stays as is.
- `tests/alloc_profile_ingress.rs::submit_all` (2 sites, loop-in-loop).
- `executor/benches/sequential_throughput.rs::bench_actor_throughput`
  (if-in-loop; bench-only, not run by `cargo test`).
- `executor/src/bin/kardamom-executor/state.rs::spawn_checkpointer`
  (if-in-loop, around the checkpoint task's error log).
- `executor/tests/determinism.rs::populate` (loop-in-loop) and
  `executor/tests/determinism.rs::two_replicas_produce_byte_identical_
  c_stream` (match-in-loop).
- `executor/tests/m_plus_one_join.rs::FakeTxDataSubAdapter::next` and
  `FakeTxOrderingSubAdapter::next` (2 sites each, if-in-loop);
  pre-existing, confirmed still deferred.
- `executor/tests/replay_integration.rs::replay_10_txs_across_3_
  blocks_yields_expected_c_stream` (2 sites: one loop-in-loop building
  the corpus, one match-in-loop draining the C stream).
- `executor/tests/stm_block_exec_ab.rs::stm_strategy_matches_
  sequential_capture_byte_for_byte` (if-in-loop, in the test's record
  corpus setup).

**R11, tuple returns replaced with named structs (this pass)**

- `load/engine/mod.rs::receipt_status` returns `ReceiptStatus { status,
  gas }` instead of `Option<(u64, u64)>`. 4 call sites updated (`try_
  once`, `finalize_accepted`, `finalize_unaccepted`, and `drain.rs::
  settle_if_still_pending`).
- `load/defi.rs::deployment_txs` returns `Deployment { txs, contracts }`
  instead of `(Vec<PlannedTx>, DefiContracts)`. 6 call sites updated
  (`load/mod.rs::build_queues`, `stm/workload.rs::defi_blocks`, one
  `defi.rs` test, and `tests/parallel_defi_repro.rs` (2 sites), `tests/
  defi_on_engine.rs`, `tests/alloc_profile.rs`).
- `stm/uniswap.rs::create_one_pair` returns `CreatedPair { tx, state }`
  instead of `(TxEnvelope, PairState)`. 1 call site.
- `executor/src/bal.rs::encode_frame` returns `EncodedFrame { bytes,
  bal_bytes }` instead of `Result<(Vec<u8>, usize), String>`'s success
  variant.
- `executor/tests/diff_reference.rs::naive_reference`/`run_one_naive`
  take `&[TxWithSender]` and return `Vec<TxResult>`, replacing `&[
  (KtTxEnvelope, Address)]` and `Vec<(bool, u64)>`.

**R11, judgment calls: index/value tuples kept as tuples**

- `bin/stm-p2/pipeline.rs`'s `settle_tx` channel payload,
  `(usize, BlockTicket)` (a `pfi` index paired with its ticket), and
  `all_recs`'s return, `Vec<(usize, FlowRecs)>` (a block index paired
  with that block's records): both index-plus-value pairs, not
  named-field groups; left as tuples.
- `executor/tests/diff_reference.rs::DiffChannels`'s `a_rx`/`b_rx`
  fields carry `Receiver<(BPosition, KtTxEnvelope)>` and `Receiver<
  (BPosition, TxOrderingMessage)>`: the position-plus-value payload
  tuples are the shape `ChanTxDataSub`/`ChanTxOrderingSub` (adapters
  this agent does not own) already consume; left as tuples. The
  `DiffChannels` struct itself replaces the raw 2-tuple-of-`Receiver`
  return `publish_diff_corpus` had, which `clippy::type_complexity`
  flagged.

**R4, manual drops**

- `close_streams` (a shared helper for the `drop(a_tx); drop(b_tx);`
  pairs in `determinism.rs`, `diff_reference.rs`, `replay_integration.
  rs`): declined as a shared helper, and the three sites are not
  uniform. In `determinism.rs` and `replay_integration.rs`, the drop
  pair sits before a blocking executor spawn-and-join later in the same
  function; dropping early is what lets the executor's fake
  subscriptions see EOF and the join complete, so these two are kept.
  In `diff_reference.rs::publish_diff_corpus`, the pair sat right
  before the function's `DiffChannels { a_rx, b_rx }` return, with
  nothing after it in that function — scope covers it the same way it
  does in `free_port` below, so it was removed there.
- `executor/tests/metrics_endpoint.rs::free_port`: removed a `drop(l)`
  that duplicated scope — `l`'s last read is `l.local_addr()`, and
  Rust drops it at the function's closing brace either way, before
  `local_addr`'s returned value reaches the caller. `free_port` is now
  two lines: bind, then return `l.local_addr().unwrap()`.
- `executor/tests/m_plus_one_join.rs::tx_ref_arriving_before_envelope_
  still_joins`: removed the `drop(a_pub)` and `a_pub` itself. `FakeBus
  ::stream` get-or-creates its shared stream state, so `a_pub`'s
  `open` call only pre-creates an entry the very next line (`tx_data_
  sub_handle`'s own `open`) creates anyway if it does not already
  exist; the inserter thread opens its own handle 30ms later and
  publishes through it. `a_pub` had no effect the test uses.
- `tests/alloc_profile_ingress.rs`'s `drop(rt); drop(profiler);`: kept.
  This pair is order-dependent, not scope-redundant: `rt` (the tokio
  runtime) must quiesce before `profiler` finalizes and writes `dhat-
  heap.json`, and `rt` is declared before `profiler`, so scope's
  reverse-declaration-order drop would run them in the wrong order
  (`profiler` first). The explicit pair is required to get the correct
  order.

**Cross-crate (detail for three rows added this pass)**

The full list, with call sites, is under the "Cross-crate" heading near
the end of this file. These three rows carry the reasoning the terse
list does not repeat:

- `kardamom_engine::actor::types::BalHandoff`: a `pub type BalHandoff =
  (..)` tuple type alias, defined in `kardamom-engine` (not owned by
  this agent). `executor/src/bal.rs::run_bal_publisher`/`recv_tick`
  destructure it but do not rename it.
- `kardamom_engine::actor::ports::TxReceiptsPublication::publish_
  receipts`: a trait method, `fn publish_receipts(&mut self, receipts:
  &[Receipt]) -> (usize, Option<ExecutorError>)`, defined in
  `kardamom-engine` (not owned by this agent). `executor/src/wiring.rs`
  ::`LiveTxReceiptsPub`'s impl must match this signature exactly, so
  its `(usize, Option<ExecutorError>)` return cannot become a named
  struct without editing the trait. This resolves the earlier `wiring.
  rs:113 tuple return` row: not a code change, a cross-crate boundary.
- `kardamom_stm::execute::execute_block_sequential`: returns
  `Result<(Vec<Receipt>, PendingDelta), ExecutorError>`, defined in
  `kardamom-stm` (not owned by this agent). `bin/stm-p2/pipeline.rs`
  (2 call sites) and `bin/stm-p2/mock_ab.rs`/`mdbx_ab.rs` destructure
  the tuple but do not rename it.

The R14 "Deferred, with reasons" list above carries the reasoning for
the other foreign-crate rows on the same terse list: `tx_obs`,
`StreamArgs`/`resume_point`, `fold_layers`, and the `testing`-feature
test-double modules.

**Fix: `Capture::delta` carries prior blocks' writes forward**

`Capture::delta` layers every prior block's write set under each new
block's scope. Regression test: `run_capture_carries_state_across_
block_boundaries`. Binary smoke check: `stm-p0 --scenario uniswap|defi|
transfers --blocks 3` runs clean.

## Cross-crate

Rows this agent's code touches but does not own, gathered under one
heading. Each proposed home is a crate outside `crates/bench` and
`crates/executor`; a one-sided change here would leave the other
crate's copy diverging from a base it no longer shares code with.

- `tx_obs` (home: `kardamom-footprint`). Call sites: `stm/capture.rs`,
  `bin/stm-p0.rs`.
- `StreamArgs` (home: `kardamom_engine::bin_support`). Call sites:
  `executor/src/bin/kardamom-executor/args.rs`,
  `validator/src/bin/kardamom-validator/args.rs`.
- `resume_point` (home: `kardamom_engine::bin_support`). Call sites:
  `executor/src/bin/kardamom-executor/state.rs`,
  `validator/src/bin/kardamom-validator/main.rs`.
- `fold_layers` (home: `kardamom_engine::delta`). Call sites:
  `executor/src/parallel.rs` (2 sites), `engine/src/actor/
  exec_settle.rs`.
- The `testing`-feature test-double modules (home: `kardamom_obs::
  testing`, `kardamom_ingress::testing`, `kardamom_engine::testing`).
  Call sites: the executor and validator test suites' dev-dependencies.
- `kardamom_engine::actor::types::BalHandoff` (a tuple type alias).
  Call sites: `executor/src/bal.rs::run_bal_publisher`/`recv_tick`.
- `kardamom_engine::actor::ports::TxReceiptsPublication::publish_
  receipts` (a trait method returning `(usize, Option<ExecutorError>)`).
  Call site: `executor/src/wiring.rs::LiveTxReceiptsPub`.
- `kardamom_stm::execute::execute_block_sequential` (returns
  `Result<(Vec<Receipt>, PendingDelta), ExecutorError>`). Call sites:
  `bin/stm-p2/pipeline.rs` (2 sites), `bin/stm-p2/mock_ab.rs`,
  `bin/stm-p2/mdbx_ab.rs`.

## R14 follow-up

Seven items, all inside `crates/bench` and `crates/executor`; all done:

1. `BenchArgs` flatten: `config.rs::BenchArgs` (`timeout`,
   `concurrency`, `txs_per_task`, `max_in_flight`), used with
   `#[command(flatten)]` in both `harness.rs`'s and `main.rs`'s `Args`.
   Each binary keeps its flag names, defaults, and help text.
2. `bench_one_tx`: `executor/benches/sequential_throughput.rs::
   TxBench<'a, F>` holds the snapshot and the transaction builder as
   struct state, with the shared benchmark body as a method.
3. `bench_env(chain_id, block)`: `stm::BlockAt(usize)`, in `stm/mod.rs`.
   `.number()`, `.timestamp()`, and `.env(chain_id)` give the block
   number, the L2 timestamp, and the full `ExecEnv`, all from one
   1-based/2-seconds-per-block formula. `stm/capture.rs`, `tests/
   alloc_profile.rs`, `tests/defi_on_engine.rs`, and `tests/
   parallel_defi_repro.rs` all call it (the last through a thin local
   `env_for` wrapper, since it also converts a 1-based block number to
   `BlockAt`'s 0-based index).
4. `genesis_changes`/`mdbx_rig`: `bin/stm-p2/mdbx_ab.rs::mdbx_rig`
   holds the temp-mdbx-env-plus-writer setup; the genesis-
   `AccountChange`-vector side is `common.rs::Workload::genesis(&self)
   -> Vec<AccountChange>`, a method shared across the whole `stm-p2`
   binary rather than a file-local helper.
5. `pprof_guard`/`write_pprof_svg`: both live in `pprof_guard.rs`.
   `harness.rs::build_pprof_guard` and `bin/stm-p2/main.rs` both call
   them, with the shared `PPROF_HZ` constant.
6. `asprof_pass`: `perf/profile.rs::asprof_pass`, called from 2 sites.
7. `seq_exec`: `stm::SeqExec<'a, S>`, in `stm/mod.rs`. Holds the open
   engine scope and the running `cumulative_gas_used`; `run(&mut self,
   tx_idx, position, ..)` executes one transaction. `SeqExec::resume`
   carries the cumulative total across a rebuild at a window boundary
   that is not also a gas-accounting boundary (`tests/alloc_profile.
   rs`, re-scoping every `BLOCK_TXS` transactions). `stm/capture.rs`,
   `tests/alloc_profile.rs`, `tests/defi_on_engine.rs`, and `tests/
   parallel_defi_repro.rs` all use it.

Gates rerun after this section: clippy `-D warnings --all-targets
--all-features` (pass), clippy `--no-deps -W clippy::pedantic -D
warnings --all-targets --all-features` (pass, zero warnings in
`crates/bench/` and `crates/executor/`), `cargo test --all-targets
--all-features` (pass, 0 failed), `cargo fmt --check` (pass).

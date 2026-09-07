# bench

## Summary

`crates/bench/src/bin/stm-p2.rs` is the dominant problem: 1676 code lines in one file,
with a 619-line `main`, a 426-line `run_mdbx_ab` (16 arguments) and a 391-line
`run_pipelined` (15 arguments). It also holds dead shared state (an `AtomicUsize` that
is written but never read), hard-coded `8000.0` divisors where a transaction count
belongs, and three dead bindings that only silence warnings. The second problem is
comment rot: 40 comments in non-test code carry history, incident reports, or
references to spec documents by name. The `load` module is otherwise well built; its
`Tracker` synchronization is justified, but `Benchmark::dispatch`'s per-task
`Mutex` and atomics are ownership transfer in disguise. R8 is broad but low risk: about
30 `pub` items in the `kardamom-bench` library are reachable only from inside the
library. The executor crate is clean: no `dyn` of its own, no oversized files, and the
binary already uses `pub(crate)` everywhere. Counts: R1 40, R2 23, R3 1 (plus 4 KEEP),
R4 5, R5 15, R6 2, R7 1, R8 31, R9 9, R10 17.

## R1 comments

| file:line | snippet (max 80 chars) | why | fix |
|---|---|---|---|
| crates/bench/src/harness.rs:15 | "This is the stand-in left in place after the removal of `kardamom-node`" | Removal history | State what the harness profiles now |
| crates/bench/src/harness.rs:16 | "A full in-process Aeron pipeline harness ... is a follow-up item." | Roadmap note | Move to the issue tracker |
| crates/bench/src/harness.rs:226 | "(only the node did)" | Refers to a deleted crate | Delete the parenthesis |
| crates/bench/src/harness.rs:231 | "The full pipeline harness restores span-based flame graphs." | Future work | Move to the issue tracker |
| crates/bench/src/harness/inprocess.rs:20 | "used since the removal of `kardamom-node`" | Removal history | Delete |
| crates/bench/src/harness/inprocess.rs:23 | "A full in-process Aeron pipeline harness is a follow-up item." | Roadmap note | Move to the issue tracker |
| crates/bench/src/harness/flame.rs:93 | "This matches the old `grep ';'` recipe from the docs." | Refers to a past recipe | State the rule: drop bare-root lines |
| crates/bench/src/load/engine.rs:220 | "The old half-second cap released a rate/2 burst at the start of each step" | Past-defect story | State the invariant: cap credit at 4 ticks |
| crates/bench/src/load/accounting.rs:64 | "Direct mdbx inspection confirmed this for every sampled \"missing\" hash." | Investigation record | Delete; keep the rule that follows |
| crates/bench/src/load/scrape.rs:177 | "Before this change, the node was silently missing from service_up" | Change history | State: a failed scrape is a down entry |
| crates/bench/src/load/scrape.rs:186 | "Requiring the sample line made every clean run report `None`" | Change history | State: absent counter means zero |
| crates/bench/src/load/config.rs:85 | "one load shard's ceiling can swing from 800 to 18" | Dated measurement | Say the ceiling is host-dependent |
| crates/bench/src/stm/mod.rs:16 | "The classifier, oracle, and cell model moved to `kardamom-footprint`" | Move history | Say where they live now |
| crates/bench/src/stm/mod.rs:20 | "so the `kardamom-stm-p0` binary's import surface stays unchanged." | Compatibility history | Delete |
| crates/bench/src/stm/capture.rs:13 | "This re-export keeps `capture::TxObs` resolving for the stm-p0 binary." | Compatibility history | Delete, or import `TxObs` directly in the bin |
| crates/bench/src/main.rs:109 | "It stays a regular function, not a `const` function, because" | Justifies a non-change | Delete |
| crates/bench/src/bin/load.rs:84 | "The number of per-submit retry attempts on a transient failure." | Wrong doc: it documents `retry_submit`, but sits on `subscribe` | Move the line to `retry_submit` at line 100 |
| crates/bench/src/bin/perf.rs:62 | "A directory that `kardamom-perf run` previously produced." | Past tense | "A directory `kardamom-perf run` produces." |
| crates/bench/src/bin/perf.rs:136 | `#[allow(clippy::too_many_arguments)]` on a 2-argument function | Stale annotation | Delete the attribute |
| crates/bench/src/perf/cluster.rs:96 | "Purging a missing cluster failed the whole `up` with \"No such container\"" | Past-defect story | State the rule: purge only an existing cluster |
| crates/bench/src/bin/stm-contention.rs:12 | "Two problems affected the first version of this benchmark:" | Change history | State the two rules the benchmark follows |
| crates/bench/src/bin/stm-p2.rs:137 | "the legacy per-worker FIFO scheduler, with stealing and eager coverage" | "legacy" label | Name the scheduler by behavior |
| crates/bench/src/bin/stm-p2.rs:148 | "Read-path timing, graph elision, and a sampling profiler all failed to explain" | Investigation record | Say what the allocator counts |
| crates/bench/src/bin/stm-p2.rs:227 | "mimalloc as the backing store was tried and reverted" | Experiment record | Delete |
| crates/bench/src/bin/stm-p2.rs:230 | "See the spec's pipeline-span-inflation note" | Spec document by name | Inline the fact, or delete |
| crates/bench/src/bin/stm-p2.rs:584 | "see the spec's span-inflation note." | Spec document by name | Inline the fact, or delete |
| crates/bench/src/bin/stm-p2.rs:645 | "The naive order, building after the release, measured much lower throughput" | Experiment record | State the rule: build before the release |
| crates/bench/src/bin/stm-p2.rs:946 | "The per-block timeline showed a uniform slowdown across both EVM and reads" | Investigation record | State what the flag does |
| crates/bench/src/bin/stm-p2.rs:956 | "Charging decode to only one side inflated every ratio." | Past-defect story | State: decode once, outside both timers |
| crates/bench/src/bin/stm-p2.rs:1083 | "The busy worker is the boosted core ... this settles it." | Investigation record | State: print the peak core clock |
| crates/bench/src/bin/stm-p2.rs:1173 | "Without this, parcounter measured all backend reads" | Past-defect story | State the rule: mirror before the writer applies |
| crates/bench/src/bin/stm-p2.rs:1253 | "// placeholder, replaced below" | Wrong: nothing replaces it; `n_tx` is dead | Delete the binding and the comment |
| crates/executor/src/config.rs:3 | "In the past, the executor took all runtime tuning through CLI flags." | History | Say what the file config holds now |
| crates/executor/src/config.rs:12 | "this differs from [`crate::ExecutorConfig`] (in `actor.rs`)" | Dangling link: no such item in this crate | Point at `kardamom_engine::ExecutorConfig` |
| crates/executor/src/config.rs:20 | "so existing `kardamom_executor::config::ClusterConfig` paths still resolve." | Compatibility history | Delete |
| crates/executor/src/bal.rs:4 | "See docs/agents/bal-attribution-parallel-validation-spec.md." | Spec document by name | Inline the contract |
| crates/executor/src/bal.rs:16 | "(unchanged from v1; ... fat frames once shrank the validator's lapse window)" | Version history | State: receipts are excluded to keep frames small |
| crates/executor/src/bal.rs:64 | "at 2s (the tick length), a `NOT_CONNECTED` window drained at exactly the arrival rate" | Past-defect story | State: the deadline must stay under the block interval |
| crates/executor/src/bal.rs:183 | "Spinning the full deadline on every frame capped this pump's drain rate at 2 frames/s" | Past-defect story | State: `NOT_CONNECTED` is terminal, not retried |
| crates/executor/src/parallel.rs:2 | "(`--parallel-execution`; scheduler unification B2)" | Work-item code | Delete "scheduler unification B2" |
| crates/executor/src/bin/kardamom-executor/main.rs:113 | "The resume-gated replay-merge this replaces pointed at the consumer's local archive" | Change history | State: subscriptions stay live always |
| crates/executor/src/bin/kardamom-executor/main.rs:178 | "See docs/agents/bal-attribution-parallel-validation-spec.md." | Spec document by name | Inline the contract |
| crates/executor/src/bin/kardamom-executor/main.rs:192 | "The legacy writer-queue tee is superseded by the publisher thread." | Change history | Delete |
| crates/executor/src/bin/kardamom-executor/main.rs:252 | "(phase 2 would give it its own L1 dependency)" | Roadmap note | Delete |
| crates/executor/src/bin/kardamom-executor/main.rs:264 | "Waiting only for SIGTERM left an errored executor lingering \"alive\"" | Past-defect story | State: exit on the first of signal or engine end |
| crates/executor/src/bin/kardamom-executor/args.rs:34 | "The old shared single-channel path ignores it." | History | State: used only when MDS is on |
| crates/executor/src/bin/kardamom-executor/args.rs:43 | "a streaming pipeline is a planned follow-up" | Roadmap note | Move to the issue tracker |
| crates/executor/src/bin/kardamom-executor/args.rs:44 | "byte-for-byte as before" | Comparative history | "byte-for-byte identical output" |
| crates/executor/src/bin/kardamom-executor/args.rs:66 | "This field stays so old invocations don't error" | Deprecation history | Keep one line: "Accepted and ignored." |
| crates/executor/src/bin/kardamom-executor/state.rs:119 | "The first tick fires immediately; the old thread slept first." | Change history | State: the first tick fires immediately |

## R2 long methods

| file:line | fn name | approx code lines | helpers to extract |
|---|---|---|---|
| crates/bench/src/bin/stm-p2.rs:1308 | `main` | 619 | `build_blocks(&Args,&[DerivedSigner])`, `uniswap_blocks`, `defi_blocks`, `parcounter_blocks`, `partransfer_blocks`, `transfers_blocks`, `run_mock_ab(&Workload,&EngineOpts)`, `print_mock_rows(&[Row])` |
| crates/bench/src/bin/stm-p2.rs:825 | `run_mdbx_ab` | 426 | `MdbxAbTotals` struct + `accumulate(&mut totals,&out)`, `run_one_block`, `open_mdbx_env(&genesis)`, `print_totals(&totals,w)` |
| crates/bench/src/bin/stm-p2.rs:362 | `run_pipelined` | 391 | `pass_a_sequential`, `prepare_feed_payloads`, `spawn_settler`, `drive_speculative`, `drive_baseline`, `verify_outcomes` |
| crates/bench/src/load/mod.rs:101 | `run` | 216 | `build_queues(&cfg,&signers,chain_id)`, `spawn_receipt_feed(&cfg,&signers,&tracker)`, `settle_and_snapshot(&cfg,&scraper,...)`, `build_report(&cfg,&tracker,verdict,ramp)` |
| crates/bench/src/stm/uniswap.rs:115 | `generate` | 199 | `load_artifacts(repo_root)`, `deploy_tokens_and_factory`, `create_pairs_and_liquidity`, `fund_senders`, `generate_flow_blocks` |
| crates/bench/src/bin/stm-p0.rs:126 | `main` | 196 | `build_blocks(&Args,&[DerivedSigner])`, `defi_blocks`, `transfers_blocks`, `print_classifier(&Stats,&[TxObs])`, `write_json_report` |
| crates/bench/src/load/accounting.rs:136 | `evaluate` | 115 | `keep_pace_rows(&EvalInput)->(Vec<KeepPace>,Vec<String>)`, `drop_accounting(&EvalInput)`, `liveness_failures(&EvalInput)`, `completeness_failures(&EvalInput,&drops)` |
| crates/executor/src/parallel.rs:186 | `run_one` | 97 | `fork_views(&snapshot,workers)`, `run_tx_segment(pool,..)->SegmentOut`, `run_singleton_segment(..)->SegmentOut`, `fold_segments(seg_layers)` |
| crates/bench/src/perf/report.rs:106 | `write_summary` | 94 | `write_header(&mut md,..)`, `write_ramp_table`, `write_cpu_table`, `write_profile_tables` |
| crates/bench/src/load/mod.rs:366 | `ramp_to_max` | 92 | `run_ramp_step(..)->RampStep`, `step_is_sustainable(&before,&after,&s0,&s1,&cfg)`, `log_ramp_step(&RampStep)` |
| crates/bench/src/load/defi.rs:333 | `pregenerate_family` | 92 | `family_op(fam,i,seq,&contracts)->anyhow::Result<(Address,Bytes,u64)>`, `sender_base_nonce(sender,nonce_start)`, `sign_queue(s,..)` |
| crates/bench/src/stm/capture.rs:21 | `run_capture` | 88 | `block_env(bi)`, `execute_and_observe(snap,&delta,env,..)->(TxObs,WriteSet)`, `cells_from_bal(&alloy_bal)` |
| crates/executor/src/bal.rs:112 | `run_bal_publisher` | 85 | `measure_sizes(&bal,&measure,block)`, `publish_with_deadline(&pubh,&bytes,block)->&'static str`, `retain(&mut ring,block,bytes)` |
| crates/bench/src/bin/perf.rs:185 | `run` | 78 | `discovery_phase(&a,&out)->LoadReport`, `soak_phase(&a,soak_rate,&out)->JoinHandle`, `profile_phase(&a,&out)->(String,String,Vec<..>)` |
| crates/bench/src/load/accounting.rs:313 | `print_report` | 75 | `print_ramp(&r.ramp)`, `print_gas(&r)`, `print_counts(&v)`, `print_keep_pace(&v.keep_pace)` |
| crates/bench/src/load/scrape.rs:117 | `snapshot` | 75 | `scrape_executors(&mut snap)`, `scrape_ingress(&mut snap)`, `scrape_sequencers(&mut snap)` |
| crates/bench/src/perf/cluster.rs:108 | `up` | 75 | `build_images(repo_root)`, `start_orchestrator(root)`, `run_ci_cluster()->String`, `check_smoke_gates(&out)` |
| crates/bench/src/benchmark.rs:192 | `dispatch` | 63 | `spawn_senders(&self,client,main)->Vec<JoinHandle<TaskAccum>>`, `merge_histograms(methods,per_task)` |
| crates/bench/src/bin/stm-p0.rs:64 | `shadow_replay` | 61 | `ShadowTotals` struct + `accumulate`, `print_block_row(b,&g)`, `print_aggregate(&totals)` |
| crates/bench/src/load/defi.rs:210 | `deploy_and_confirm` | 60 | `submit_deploys(client,deploys)`, `await_receipt_while_advancing(client,&d,started)` |
| crates/executor/src/bin/kardamom-executor/state.rs:28 | `prepare_state` | 56 | `restore_from_checkpoint(args,expected_genesis)`, `open_env(args)`, `read_cursor(&env)->ResumePoint` |
| crates/executor/src/bin/kardamom-executor/main.rs:47 | `main` | 140 (split by 3 blank-line-separated stages) | `open_streams(&args,&rt,&channels)`, `build_hooks(&args,rt_pub,&channels)`, `shutdown_sequence(join,rt,cluster_guard,writer)` |
| crates/bench/src/bin/stm-p2.rs:281 | `train` | 50 | `observe_tx(&touches,&ws,&receipt,envelope)->TxObs` |

## R3 large files

| file | code lines | split plan or KEEP (reason) |
|---|---|---|
| crates/bench/src/bin/stm-p2.rs | 1676 | SPLIT into a `src/bin/stm-p2/` directory. See the plan below. |
| crates/bench/src/load/accounting.rs | 465 (139 outside `#[cfg(test)]`) | KEEP. One cohesive concern: the verdict. Two thirds of the file is its test module. |
| crates/bench/src/load/engine.rs | 407 (185 outside `#[cfg(test)]`) | KEEP. Queue, submit, pace, and drain are one open-loop send engine. More than half the file is its test module. |
| crates/bench/src/load/mod.rs | 375 | KEEP at this size, but extract the four helpers named in R2 for `run`. The module is the harness driver, and the parts are already in sibling modules. |
| crates/bench/src/load/defi.rs | 380 | KEEP. One workload family: contracts, deploy, and pre-generation. |

### stm-p2.rs split plan

Files under `crates/bench/src/bin/stm-p2/`:

- `main.rs` (about 80 lines): parse `Args`, derive signers, build blocks, pick a mode,
  wrap the pprof guard. No workload or measurement code.
- `args.rs` (about 130 lines): the `Args` clap struct, plus `DebugFlags::from_env()`.
  Today six environment variables (`KARDAMOM_STM_PHASE_TIMING`, `KARDAMOM_STM_ONLY`,
  `KARDAMOM_SEQ_ON_THREAD`, `KARDAMOM_SEQ_CORE`, `KARDAMOM_BENCH_PROGRESS`,
  `KARDAMOM_PIPE_ASSERT`) are read inside the per-block loop, at lines 938, 953 and 967.
  Read them once at startup.
- `alloc.rs` (about 90 lines): `CountingAlloc`, the bucket statics, `bucket_of`,
  `alloc_snap`, `bucket_snap`, and a new `AllocDelta { calls, bytes, reallocs, rebytes,
  buckets }` with `AllocDelta::since(&before)`. This removes the eight loose
  accumulators at lines 919 to 924 and the index loop at lines 1048 to 1053.
- `scenario.rs` (about 330 lines): `build_blocks(&Args, &[DerivedSigner]) ->
  anyhow::Result<(Vec<Vec<TxEnvelope>>, usize)>`, with one private function per
  scenario. This also removes the copy of the defi and transfers builders that
  `src/bin/stm-p0.rs:156` to `:241` holds; promote the shared version into
  `kardamom_bench::stm`, so both binaries call it.
- `common.rs` (about 110 lines): `records`, `assert_identical`, `train`, the `FlowRecs`
  and `FeedPayload` aliases, and `open_mdbx_env(&[AccountChange])`, which lines 392 to
  402 and 872 to 882 duplicate.
- `mock_ab.rs` (about 230 lines): `BlockCase`, `Row`, `run_mock_ab`, `print_rows`.
- `mdbx_ab.rs` (about 250 lines): `run_mdbx_ab`, with the 30 loose `let mut`
  accumulators at lines 901 to 926 replaced by an `MdbxAbTotals` struct and an
  `accumulate(&mut self, &StmOutcome)` method.
- `pipeline.rs` (about 280 lines): `run_pipelined`, split as named in R2.

### stm-p2.rs argument-group structs

`run_mdbx_ab` takes 16 arguments and `run_pipelined` takes 15. Nine of them are the same
engine knobs. Introduce three structs:

```rust
/// The engine knobs. These map onto `PoolConfig`.
struct EngineOpts {
    worker_counts: Vec<usize>,
    parallel_worth_ns: u64,
    dispatch_by_sender: bool,
    eager_chain: bool,
    bag_scheduler: bool,
    admit_shards: usize,
    sticky_assign: bool,
    pin_cores: Vec<usize>,
    keep_hot: bool,
}
impl EngineOpts {
    fn pool_config(&self, workers: usize, prune_batch: usize, tail_on_workers: bool) -> PoolConfig;
}

/// The block stream under test.
struct Workload<'a> {
    signers: &'a [DerivedSigner],
    all_blocks: &'a [Vec<TxEnvelope>],
    n_setup: usize,
    warmup_blocks: usize,
    chain_id: u64,
}
impl Workload<'_> {
    /// The first timed flow block.
    fn warm(&self) -> usize { self.n_setup + self.warmup_blocks }
}

/// What the run prints and checks.
struct RunOpts { per_block: bool, prune_batches: Vec<usize>, debug: DebugFlags }
```

The signatures then become:

```rust
fn run_pipelined(w: &Workload<'_>, eng: &EngineOpts, speculative: bool) -> anyhow::Result<()>;
fn run_mdbx_ab(w: &Workload<'_>, eng: &EngineOpts, run: &RunOpts) -> anyhow::Result<()>;
```

Three arguments each, so the three `#[allow(clippy::too_many_arguments)]` attributes at
lines 359, 824 and the `warm` computation at line 416 and line 930 all go away.

## R4 manual drops

| file:line | snippet | class | fix |
|---|---|---|---|
| crates/bench/src/harness.rs:220 | `drop(flame_guard);` | Resource release (buffered file writer) | Extract `fn flush_folded(guard: FlushGuard<BufWriter<File>>) -> anyhow::Result<()>`; the guard dies at the helper's return, before the read |
| crates/bench/src/bin/stm-p2.rs:457 | `drop(writer_a);` | Resource release (state writer thread) | Extract `fn pass_a_sequential(..) -> anyhow::Result<PassA>`; `writer_a` dies at that function's end |
| crates/bench/src/bin/stm-p2.rs:787 | `drop(settle_tx);` | Channel sender close, to signal EOF | HARD to remove. Give the submission loop its own scope: `let ticket_source = { ... move settle_tx in ... };` so the sender dies when the loop scope ends |
| crates/executor/src/bin/kardamom-executor/main.rs:277 | `drop(rt);` | Resource release (Aeron runtime; closes subscriptions to unblock readers) | HARD to remove without restructuring `main`. Extract `fn run_engine(rt, cluster_guard, ..) -> Result<..>` that owns both guards |
| crates/executor/src/bin/kardamom-executor/main.rs:284 | `drop(cluster_guard);` | Resource release (cluster session, to close tx_ordering) | Same helper as above; the two drops must stay ordered, so one scope covering both is the right shape |

## R5 sync primitives and channels

| file:line | primitive | verdict | reason | fix |
|---|---|---|---|---|
| crates/bench/src/benchmark.rs:284 | `ok: AtomicU64` | REPLACE_WITH_OWNERSHIP | One sender task owns each `TaskAccum`; `dispatch` reads it only after the join | Return the counts from the task's `JoinHandle` |
| crates/bench/src/benchmark.rs:285 | `err: AtomicU64` | REPLACE_WITH_OWNERSHIP | Same as above | Same as above |
| crates/bench/src/benchmark.rs:286 | `histograms: Mutex<BTreeMap<..>>` | REPLACE_WITH_OWNERSHIP | Single writer, read after join. The `Arc<Mutex<..>>` exists only because `tokio::time::timeout` drops the future on expiry | Replace `timeout(..)` with a `tokio::select!` on a deadline inside `send_loop`, then return the owned map from the task |
| crates/bench/src/harness.rs:147 | `Arc<AtomicBool>` (flame gate) | JUSTIFIED | The tracing `FilterFn` runs on every tokio worker thread | Keep |
| crates/bench/src/load/mod.rs:171 | `Arc<Semaphore>` | JUSTIFIED | Bounds in-flight submits across many spawned tasks | Keep |
| crates/bench/src/load/engine.rs:108 | `_permit: OwnedSemaphorePermit` | JUSTIFIED | RAII in-flight slot held for the task's life | Keep |
| crates/bench/src/load/tracker.rs:48-51 | four `AtomicU64` counters | JUSTIFIED | Incremented by many concurrent submit tasks, the feed task, and the sweeper | Keep |
| crates/bench/src/load/tracker.rs:55,57 | `gas_used`, `step_gas` `AtomicU64` | JUSTIFIED | Same writers | Keep |
| crates/bench/src/load/tracker.rs:58 | `lat_us: Mutex<Histogram>` | JUSTIFIED, but contended | Every confirmation takes this lock. At 10k tx/s this is a global serialization point | Keep, or shard into per-task histograms and merge at the end |
| crates/bench/src/load/tracker.rs:64 | `step_lat_us: Mutex<Histogram>` | JUSTIFIED, but contended | Same as above | Same as above |
| crates/bench/src/load/tracker.rs:65 | `pending: Mutex<HashMap>` | JUSTIFIED | Written by submit tasks, the feed, the sweeper, and the drain | Keep |
| crates/bench/src/load/tracker.rs:69 | `early: Mutex<HashMap>` | JUSTIFIED | Resolves the feed-against-ack race | Keep |
| crates/bench/src/bin/stm-p2.rs:153-186 | `ALLOC_*`, `BUCKET_*`, `REALLOC_*` statics | JUSTIFIED | A `GlobalAlloc` runs on every thread; statics must be atomic | Keep |
| crates/bench/src/bin/stm-p2.rs:542 | `mpsc::channel::<(usize, BlockTicket)>` | JUSTIFIED | Hands tickets to the settler thread | Keep |
| crates/bench/src/bin/stm-p2.rs:543 | `Arc<AtomicUsize>` `settled` | UNNECESSARY | `settled_c.store` at line 596 is the only access. Nothing ever loads it | Delete `settled`, `settled_c`, and the store |
| crates/bench/src/bin/stm-p2.rs:550 | `Arc<Mutex<Vec<(usize, StmOutcome)>>>` | REPLACE_WITH_OWNERSHIP | Only the settler thread writes; the main thread reads after `settler.join()`. The code already unwinds it with `Arc::try_unwrap(..).expect(..).into_inner().unwrap()` at lines 790 to 794 | Change the settler closure's return type to `anyhow::Result<Vec<(usize, StmOutcome)>>` and take the vector from `join()` |
| crates/bench/src/bin/stm-p2.rs:620 | `mpsc::channel::<DeltaRelease>` | JUSTIFIED | Streams fold releases from the engine to the submission loop | Keep |
| crates/bench/src/bin/stm-p2.rs:629 | `mpsc::channel::<MvRelease>` | JUSTIFIED | Streams pre-fold mv-cache releases | Keep |
| crates/executor/src/parallel.rs:83 | `bounded::<BlockRequest<S>>(1)` | JUSTIFIED | Crosses the scoped `with_pool` borrow into a `'static` closure, as the module doc explains | Keep |
| crates/executor/src/parallel.rs:97 | `bounded(1)` reply channel | JUSTIFIED | Carries the per-block response back | Keep; a one-shot channel type would say this more clearly |
| crates/executor/src/bin/kardamom-executor/main.rs:187 | `crossbeam_channel::bounded(8)` | JUSTIFIED | Bounded hand-off from the exec thread to the BAL publisher thread, with back pressure by design | Keep |
| crates/executor/src/bal.rs:24 | `Receiver<BalHandoff>` | JUSTIFIED | The publisher thread's only input | Keep |

Related defect, same area: `crates/bench/src/load/tracker.rs:23` defines a
poison-tolerant `lock()` helper, but `confirm_with_gas` at lines 269 and 272 calls
`self.lat_us.lock()` and `self.step_lat_us.lock()` directly. On a poisoned mutex those
two calls drop the sample in silence. Use `lock()` in both places.

## R6 dynamic dispatch

No `dyn` is declared in either crate. Two sites in `crates/executor` produce a boxed
closure whose type alias lives in `kardamom-engine`
(`crates/engine/src/actor/types.rs:139`, `pub type BlockExec<D> = Box<dyn Fn(..) + Send>`).

| file:line | snippet | class | generic replacement |
|---|---|---|---|
| crates/executor/src/parallel.rs:79 | `pub fn stm_block_exec<S>(cfg) -> BlockExec<S>` | Stored `dyn Fn` closure | Define `trait BlockExecStrategy<D>: Send { fn execute(&self, ..) -> Result<BlockExecOutput, ExecutorError>; }` in the engine, return a concrete `StmBlockExec<S>` holding `req_tx`, and make `RoleHooks` generic over the strategy. The module doc at line 8 says the boxed closure exists only to satisfy `'static`; a struct holding the sender satisfies it directly |
| crates/executor/src/bin/kardamom-executor/wiring.rs:49 | `fn build_block_exec(args) -> Option<BlockExec<StateSnapshot>>` | Stored `dyn Fn` closure | Return `Option<StmBlockExec<StateSnapshot>>` once the trait above exists |

`Box<dyn Error>` does not appear in either crate; every fallible function already uses
`anyhow::Result` or a typed `ExecutorError`.

## R7 too many generics

| file:line | item | params/bounds | supertrait proposal |
|---|---|---|---|
| crates/executor/src/parallel.rs:79 | `stm_block_exec<S>` (also `pool_server` at :115 and `run_one` at :186) | 1 type parameter, 4 bounds: `StateDatabase + Clone + Sync + 'static` | Add `pub trait SharedState: StateDatabase + Clone + Sync + 'static {}` with `impl<T: StateDatabase + Clone + Sync + 'static> SharedState for T {}`, next to `StateDatabase`. All three signatures then read `<S: SharedState>`, and the bound list stops being repeated three times |

## R8 unnecessary pub

Evidence rule used: `kardamom-bench` is a library plus five binaries. The binaries and
the `tests/` files are separate crates, so items they use must stay `pub`. Only
`crates/e2e` depends on the library from outside the package, and it uses just
`kardamom_bench::mnemonic` and `kardamom_bench::signers::DerivedSigner`
(`crates/e2e/src/harness/l2.rs:23-24`). Every item below was grepped across
`crates/bench/src/bin`, `crates/bench/src/main.rs`, `crates/bench/tests`,
`crates/bench/examples` and `crates/e2e`, and has zero hits.

| file:line | item | evidence (who uses it) | fix |
|---|---|---|---|
| crates/bench/src/config.rs:12 | `DEFAULT_TIMEOUT` | Only `benchmark.rs:55`. The binaries use `DEFAULT_TIMEOUT_STR` | `pub(crate)` |
| crates/bench/src/config.rs:44 | `PPROF_HZ` | Only `harness.rs:192` | `pub(crate)` |
| crates/bench/src/workflow.rs:97 | `default_signer_balance` | Only `workflows/{transfers,calls,mixed}.rs` | `pub(crate)` |
| crates/bench/src/load/mod.rs:15 | `pub mod accounting` | Only `load/mod.rs` and `load/config.rs` | Keep the module `pub` for `Verdict`, but see the rows below |
| crates/bench/src/load/mod.rs:18 | `pub mod engine` | Only `load/mod.rs` and `load/accounting.rs` | `pub(crate) mod engine` |
| crates/bench/src/load/mod.rs:21 | `pub mod scrape` | Only `load/mod.rs`, `load/accounting.rs`, `load/engine.rs` tests | `pub(crate) mod scrape` |
| crates/bench/src/load/engine.rs:34 | `pub use tracker::{Counts, Tracker}` | The only route out of the private `tracker` module; nothing outside the crate takes it | `pub(crate) use` |
| crates/bench/src/load/engine.rs:38 | `enum SubmitMode` | Only `load/mod.rs:178,377` | `pub(crate)` |
| crates/bench/src/load/engine.rs:52 | `struct Queues` | Only `load/mod.rs:159` | `pub(crate)` |
| crates/bench/src/load/engine.rs:60 | `Queues::new` | Only `load/mod.rs:159` | `pub(crate)` |
| crates/bench/src/load/engine.rs:69 | `Queues::pop_next` | Only `engine.rs:237` | `pub(crate)` |
| crates/bench/src/load/engine.rs:86 | `Queues::remaining` | Only the test module at `engine.rs:371,377` | `pub(crate)`, or delete and count in the test |
| crates/bench/src/load/engine.rs:201 | `pacer` | Only `load/mod.rs:249,387` | `pub(crate)` |
| crates/bench/src/load/engine.rs:267 | `join_submit_tasks` | Only `load/mod.rs:268` | `pub(crate)` |
| crates/bench/src/load/engine.rs:292 | `sweep_pending_once` | Only `engine.rs:312,341` | private |
| crates/bench/src/load/engine.rs:310 | `drain` | Only `load/mod.rs:269` | `pub(crate)` |
| crates/bench/src/load/engine.rs:332 | `spawn_pending_sweeper` | Only `load/mod.rs:208` | `pub(crate)` |
| crates/bench/src/load/tracker.rs:29 | `struct Counts` | Only `load/mod.rs`, `accounting.rs` | `pub(crate)` |
| crates/bench/src/load/tracker.rs:47 | `struct Tracker` | Only `load/mod.rs`, `engine.rs`, `feed.rs` | `pub(crate)` |
| crates/bench/src/load/tracker.rs:77,149,175,190,203,219,224,230,244 | `new`, `confirm_from_feed`, `counts`, `sample_pending`, `remaining_pending`, `take_step_gas`, `total_gas`, `latency_us`, `take_step_latency_us` | All callers are in `load/` | `pub(crate)`, matching the eight methods there that already are |
| crates/bench/src/load/scrape.rs:37 | `struct MetricsSnapshot` | Only `load/mod.rs`, `accounting.rs` | `pub(crate)` |
| crates/bench/src/load/scrape.rs:74 | `struct Scraper` | Only `load/mod.rs:64,169` | `pub(crate)` |
| crates/bench/src/load/scrape.rs:218 | `sum_metric` | Only `scrape.rs` itself and its tests | `pub(crate)` |
| crates/bench/src/load/accounting.rs:20 | `struct KeepPace` | Reachable only as a `Verdict` field | Keep `pub` (serde field of the public `Verdict`) |
| crates/bench/src/load/accounting.rs:37 | `struct EvalInput` | Only `load/mod.rs:311` and the engine tests | `pub(crate)` |
| crates/bench/src/load/accounting.rs:136 | `evaluate` | Only `load/mod.rs:311` and the engine tests | `pub(crate)` |
| crates/bench/src/load/defi.rs:210 | `deploy_and_confirm` | Only `load/mod.rs:166` | `pub(crate)` |
| crates/bench/src/perf/cluster.rs:28 | `SEALER_NODES` | Only `cluster.rs:206` | private |
| crates/bench/src/perf/cluster.rs:38 | `sh` | Only `cluster.rs` and `perf/profile.rs` | `pub(crate)` |
| crates/bench/src/perf/cluster.rs:54 | `docker_exec` | Only `cluster.rs` and `perf/profile.rs` | `pub(crate)` |
| crates/bench/src/perf/cluster.rs:64 | `purge` | Only `cluster.rs:133`. The binary mentions "purge" in prose only, at `bin/perf.rs:10,45` | `pub(crate)` |
| crates/bench/src/perf/report.rs:14 | `FrameShare.frame`, `FrameShare.pct` | Read only by `write_summary` in the same module | Make the fields private |
| crates/bench/src/perf/report.rs:23 | `ProfileSummary.total_samples`, `.top_leaves`, `.buckets` | Read only by `write_summary` in the same module. `bin/perf.rs` passes the value through, never reads a field | Make the fields private |

`crates/executor` is clean for R8: the binary uses `pub(crate)` on every item
(`args.rs`, `state.rs`, `wiring.rs`), and the library's `pub` surface (`config`, `bal`,
`parallel`) is used by the binary and by `crates/validator`-side tests.

## R9 defensive validation

| file:line | snippet | why | typed replacement |
|---|---|---|---|
| crates/bench/src/signers.rs:38 | `if n == 0 { anyhow::bail!("at least one signer is required"); }` | The same check appears at three layers: here, `load/plan.rs:54`, and `load/defi.rs:293` | Parse once: `struct SignerSet(Vec<DerivedSigner>)` with `SignerSet::derive(phrase, count) -> anyhow::Result<Self>` that rejects an empty set. Then `presign_transfers`, `pregenerate`, and `pregenerate_defi` take `&SignerSet` and never re-check |
| crates/bench/src/load/plan.rs:54 | `if signers.is_empty() { anyhow::bail!("at least one signer is required"); }` | Second copy of the same check | Same as above |
| crates/bench/src/load/defi.rs:293 | `if signers.is_empty() { anyhow::bail!("at least one signer is required"); }` | Third copy | Same as above |
| crates/bench/src/load/defi.rs:333 | `pregenerate_family` has no emptiness check | The same public shape as `pregenerate_defi`, but it silently returns an empty vector | `&SignerSet` makes the check unnecessary and the gap impossible |
| crates/bench/src/load/defi.rs:178 | `signers.first().ok_or_else(\|\| anyhow!("at least one signer required"))` | A fourth phrasing of the same rule | `SignerSet::deployer(&self) -> &DerivedSigner`, infallible |
| crates/bench/src/stm/uniswap.rs:125 | `anyhow::ensure!(pairs >= 1 && signers.len() >= 2);` | Two raw `usize` checks with no message, in non-test code | `NonZeroUsize` for `pairs`, `&SignerSet` with a `len() >= 2` constructor for the workload |
| crates/bench/src/benchmark.rs:120,123 | `if self.concurrency == 0 { bail!(..) }` and the same for `txs_per_task` | Raw `u32` fields checked at run time, on every `prepare` call | Type the fields `NonZeroU32`; clap parses them at the CLI boundary. `prepare` then cannot fail this way |
| crates/bench/src/workflows/mixed.rs:96 | `if cycle_total == 0 { bail!("transfers_per_cycle + calls_per_cycle must be > 0") }` | A run-time check on two raw `u32` fields | `struct MixRatio { transfers: u32, calls: u32 }` with `MixRatio::new(t, c) -> Option<Self>` that rejects `0 + 0` |
| crates/bench/src/load/mod.rs:115 | `&signers[cfg.sender_offset as usize..]` | Raw `u32` used to index a slice, with no check. A `sender_offset` above `senders` panics with an index message. This is the opposite failure of the checks above | Parse the pair at the CLI boundary into `struct SenderRange { offset: u32, count: u32 }` with a fallible constructor, and derive `offset + count` signers from it |
| crates/bench/src/bin/load.rs:188 | `match args.completeness.to_lowercase().as_str() { "accepted" => .., other => bail!(..) }` | A raw `String` parsed by hand in `main`, while the sibling `Workload` enum already implements `FromStr` | Implement `FromStr for Completeness` and give clap a `value_parser`, as `--workload` at line 97 already does |

## R10 imperative style

| file:line | snippet | functional form |
|---|---|---|
| crates/bench/src/mnemonic.rs:37 | `let mut out = Vec::with_capacity(..); for i in 0..count { .. out.push(..) }` | `(0..count).map(\|i\| derive_one(phrase, i)).collect::<anyhow::Result<Vec<_>>>()` |
| crates/bench/src/signers.rs:42 | `'outer: for nonce_offset .. { for signer .. { if out.len() == count { break 'outer } .. } }` | `(0..txs_per_signer).flat_map(\|n\| signers.iter().map(move \|s\| (s, n))).take(count).map(sign_one).collect::<Result<_,_>>()` |
| crates/bench/src/load/plan.rs:57 | Two nested `push` loops building `Vec<Vec<PlannedTx>>` | `signers.iter().enumerate().map(\|(i,s)\| (0..per_sender).map(\|k\| sign(..)).collect::<Result<Vec<_>,_>>()).collect::<Result<Vec<_>,_>>()` |
| crates/bench/src/load/defi.rs:296 | Same nested `push` shape in `pregenerate_defi` | Same iterator chain as above |
| crates/bench/src/load/defi.rs:342 | Same nested `push` shape in `pregenerate_family` | Same iterator chain as above |
| crates/bench/src/load/tracker.rs:205 | `let mut missing = 0; let mut unlanded = 0; for v in p.values() { if v.accepted { missing += 1 } else { unlanded += 1 } }` | `p.values().fold((0,0), \|(m,u), v\| if v.accepted { (m+1,u) } else { (m,u+1) })`, or two `filter().count()` calls |
| crates/bench/src/load/scrape.rs:219 | `let mut total = 0.0; let mut matched = false; for line in .. { .. total += v; matched = true }` | `body.lines().filter_map(\|l\| sample_value(l, name)).fold(None, \|acc, v\| Some(acc.unwrap_or(0.0) + v))` |
| crates/bench/src/harness/flame.rs:34 | `for (frames, count) in .. { if .. { kept.insert(..); kept_count += .. } else { dropped_count += .. } }` | `let (kept, dropped): (HashMap<_,_>, HashMap<_,_>) = report.data.iter().partition(\|(f,_)\| frames_contains_ingress(f));` then `.values().sum()` for each count |
| crates/bench/src/harness/flame.rs:69 | `let mut out = String::new(); for (key, value) in .. { .. out.push_str(&line) }` | `report.data.iter().map(fold_line).collect::<String>()` |
| crates/bench/src/harness/flame.rs:115 | `let mut out = String::with_capacity(..); for (stack, count) in &merged { out.push_str(..) }` | `merged.iter().map(\|(s,c)\| format!("{s} {c}\n")).collect::<String>()` |
| crates/bench/src/benchmark.rs:228 | `let mut per_task = Vec::with_capacity(..); for accum in accums { .. per_task.push(h) }` | `accums.into_iter().map(\|a\| (a.ok.load(..), a.err.load(..), a.histograms.lock()..)).collect()`, then `fold` the counters. This also disappears if R5's ownership fix lands |
| crates/bench/src/benchmark.rs:244 | `for m in methods { merged.insert(..) }` then a nested merge loop | `methods.iter().map(\|m\| Ok(((*m).to_string(), Histogram::new_with_bounds(..)?))).collect::<Result<BTreeMap<_,_>,_>>()?` |
| crates/bench/src/perf/report.rs:87 | `BUCKETS.iter().map(..).collect()` is already functional, but line 49's `for line in collapsed.lines()` folds three accumulators at once | Acceptable as one pass. If split, `fold` over a small `Acc { total, leaves, buckets }` reads better |
| crates/bench/src/bin/stm-contention.rs:114 | `let mut with_sink = false; let mut access = Access::Shared; for a in args.by_ref() { match .. } }` | `args.fold(Opts::default(), \|o, a\| o.apply(&a))` |
| crates/bench/src/bin/stm-p2.rs:1048 | `for i in 0..6 { seq_buckets[i].0 += b1[i].0 - b0[i].0; .. }` | Index loop over fixed-size arrays. Use `izip!`/nested `zip`: `seq_buckets.iter_mut().zip(b1.iter().zip(b0.iter())).for_each(..)`. An `AllocDelta::since` method (see R3) removes it entirely |
| crates/bench/src/bin/stm-p2.rs:1270 | `for i in 0..6 { if seq_buckets[i].0 + stm_buckets[i].0 == 0 { continue } .. }` | `LBL.iter().zip(&seq_buckets).zip(&stm_buckets).filter(\|..\| ..).for_each(print_row)` |
| crates/bench/src/bin/stm-p2.rs:1718 | `for (bi, blk) in all_blocks.iter().enumerate() { .. cases.push(BlockCase{..}) }` | Keep. The loop threads `delta` and `stats` forward, so the imperative form is needed |
| crates/bench/src/load/engine.rs:121 | `let mut accepted = false; for attempt in 0..=retry { .. }` | Keep. The loop early-returns with side effects on the tracker |
| crates/bench/src/load/mod.rs:384 | `while rate <= cfg.target_tps { .. if sustainable { .. } else { break } }` | Keep. A stateful ramp with an early break |
| crates/bench/src/stm/uniswap.rs:259 | The flow-block loop | Keep. It mutates pair reserves as it walks, so the state carries forward |

Two more dead-code items in the same area, found while checking R10:

- `crates/bench/src/bin/stm-p2.rs:1253` `let n_tx = (rt / rt.max(1)).max(1);` followed by
  `let _ = n_tx;` at line 1254. The value is always 1 and nothing uses it.
- `crates/bench/src/bin/stm-p2.rs:1197` `let _ = (gas, batch);` and line 1961
  `let _ = \|r: &Row\| (r.avg_batch, r.redundant, r.idle_threads);`. Both exist only to
  silence unused warnings. Either print the fields or delete them.
- `crates/executor/src/parallel.rs:84` binds `workers`, and line 96 discards it with
  `let _ = workers;` inside the closure. Delete the binding at line 84.

One reporting defect in the same function: `crates/bench/src/bin/stm-p2.rs:1234`
prints `8 * 1000usize` as the transaction count, and lines 1257 to 1280 divide by a
hard-coded `8000.0`. Both should use the real flow transaction count, which the loop
already knows from `recs.len()`.

## Tests

### R1 comments

| file:line | snippet (max 80 chars) | why | fix |
|---|---|---|---|
| crates/bench/tests/smoke.rs:6 | "This replaces the former in-process-`Node` smoke test, from before the removal" | Removal history | Say what the test covers now |
| crates/bench/tests/harness_smoke.rs:10 | "A full in-process Aeron pipeline harness ... is a follow-up item." | Roadmap note | Move to the issue tracker |
| crates/bench/tests/parallel_defi_repro.rs:4 | "Three separate cluster deploys produced the same divergence" | Incident report | State the invariant the sweep checks |
| crates/executor/tests/determinism.rs:6 | "Wiring after the join-buffer architecture update: M=1 tx_data" | Change history | State the topology in the present tense |
| crates/executor/tests/diff_reference.rs:4 | "v0 corpus: transfers, a contract `SSTORE`, and a revert. A mainnet-vector corpus is a v1 follow-up." | Version history plus roadmap | List the corpus; move the follow-up out |
| crates/executor/tests/diff_reference.rs:7 | "Wiring after the join-buffer architecture update ... the public `Executor::run` signature changed." | Change history | State the topology in the present tense |
| crates/executor/tests/diff_reference.rs:317 | "TODO(v1): import a mainnet-style tx corpus (historical Uniswap swaps," | Dated work item | Move to the issue tracker |
| crates/executor/tests/replay_integration.rs:4 | "Wiring after the join-buffer architecture update: single-sequencer (M=1)" | Change history | State the topology in the present tense |
| crates/executor/tests/replay_integration.rs:9 | "boundaries on tx_receipts are unchanged from before the split." | Change history | Delete |
| crates/executor/tests/stm_block_exec_ab.rs:1 | "Merge gate for `--parallel-execution` (scheduler unification B2)." | Work-item code | Delete "scheduler unification B2" |
| crates/executor/tests/docker_aeron_e2e.rs:3 | "Status: deferred to a follow-up PR." plus a 22-line plan and a "Tracking:" list | The whole file is a roadmap note in comment form | Move the plan to the issue tracker; keep a one-line `#[ignore]` reason |
| crates/executor/benches/sequential_throughput.rs:10 | "The spec target is over 50k tx/s on plain transfers, on one core." | Spec reference with a dated number | State that the bench prints numbers and asserts nothing |
| crates/bench/tests/alloc_profile.rs:11 | "These numbers feed allocation reduction work" | Work-plan note | Delete |

### R3 large files

No test file passes 500 code lines. The largest,
`crates/executor/tests/m_plus_one_join.rs`, holds 523 raw lines and about 400 code
lines, and it is cohesive: two tests over one fake-bus topology, plus the adapters both
share. KEEP.

`crates/executor/tests/diff_reference.rs`, `determinism.rs` and `replay_integration.rs`
each rebuild the same fake M+1 wiring (a `legacy`/`transfer` signer helper, `a_tx`/`b_tx`
senders, and a collecting receipts publication). A shared `tests/common/mod.rs` would
remove roughly 150 duplicated lines across the three files.

### R6 dynamic dispatch

None found. No `dyn` appears in any test, bench, or example file in either crate.

### R10 imperative style

| file:line | snippet | functional form |
|---|---|---|
| crates/bench/tests/alloc_profile.rs:92 | `let mut txs: Vec<..> = Vec::new();` filled by a nested loop | `queues.iter().enumerate().flat_map(\|(i,q)\| q.iter().map(move \|t\| (i, t.clone()))).collect()` |
| crates/bench/tests/defi_on_engine.rs:100 | `let mut op_gas = Vec::new();` filled in a loop | `.map(..).collect()` |
| crates/bench/tests/parallel_defi_repro.rs:144 | `let mut txs: Vec<(usize, &PlannedTx)> = Vec::new();` filled in a loop | `.flat_map(..).collect()` |
| crates/executor/tests/m_plus_one_join.rs:200 | Two `Vec::with_capacity` values pushed in one `for sid in 0..M` loop | `(0..M).map(\|sid\| (FakeTxDataPublication::open(..), FakeTxDataSubscription::open(..))).unzip()` |
| crates/executor/tests/m_plus_one_join.rs:214 | `let mut plan = Vec::new();` plus a mutable `by_sid_nonce` index vector | Keep the nonce counter, but build `plan` with `flat_map` over `(0..M)` |
| crates/executor/tests/m_plus_one_join.rs:243 | `while shuffled.len() < plan.len() { .. }` merge of M queues | Keep. The random merge needs the mutable queues |
| crates/executor/tests/determinism.rs:190 | `let mut out = Vec::new();` filled in a loop | `.map(..).collect()` |
| crates/executor/tests/diff_reference.rs:138 | `let mut out = Vec::new();` filled in a loop | `.map(..).collect()` |
| crates/executor/tests/replay_integration.rs:215 | Three mutable counters advanced in one `recv` loop | Keep. The loop consumes a channel and breaks on a boundary count |
| crates/executor/benches/sequential_throughput.rs:311 | `let mut got = 0u64; loop { .. }` | Keep. It drains a channel with an early break |

# stm

## Summary
`crates/stm/src/execute.rs` is the hot spot: 3153 code lines, 5 functions over 50 code lines,
about 90 sync-primitive sites, and 15 manual `drop` calls. Eleven doc comments in that file are
merged pairs: a doc block for a deleted item now sits above an unrelated item, so the text
describes the wrong thing. `#[allow(clippy::too_many_arguments)]` at line 4403 also attaches to
the wrong function. Three `Metrics` atomics (`parallel_span_ns`, `ramp_ns`, `commit_ns`) are never
written or read, `commit_fold_ns` and `predict_ns` are never written, and `BlockCtx::done_cv` is
notified in five places but no thread waits on it. Seven more `Metrics` atomics are touched only
by the single feed thread. `block_tail` takes 15 arguments and ignores 3 of them
(`let _ = (keep_hot, tail_on_workers, pin_cores);` at line 3454). Counts: R1 36 (plus 4 in
tests), R2 13, R3 2 files to split of 5 assessed, R4 15, R5 84 verdict rows, R6 1, R7 3,
R8 21, R9 9, R10 12 (plus 7 loops kept on purpose).

## R1 comments

| file:line | snippet (max 80 chars) | why | fix |
|---|---|---|---|
| crates/stm/src/lib.rs:12 | "This crate is an offline milestone: ... not wired into the live executor yet." | Stale status note. `crates/executor/src/parallel.rs` already uses this crate. | Delete the paragraph. |
| crates/stm/src/lib.rs:68 | "This replaces a byte-at-a-time FNV loop that cost about 50 cycles per 20-byte key" | Historical comparison with removed code. | Keep only "Hashes 8 bytes at a time; keys are high-entropy." |
| crates/stm/src/mv.rs:51 | "Widening this to 1024 was tested: the theory was that about 180 live cells ..." | Records a past experiment. | Keep "Do not widen without a new measurement." Drop the story. |
| crates/stm/src/mv.rs:60 | "Folding only the first 8 bytes clustered structured addresses, so fold the tail instead." | Describes a rejected earlier version. | State the rule: "Fold the last 8 bytes; they carry the entropy." |
| crates/stm/src/pool.rs:7 | "the validator's BAL-seeded batch execution (which used to spawn one OS thread ...)" | "used to" history. | Delete the parenthesis. |
| crates/stm/src/pool.rs:10 | "History: these threads used to be `std::thread::scope` spawns, one set per block." | A whole history paragraph with dated timings. | Delete lines 10-16. |
| crates/stm/src/pool.rs:32 | "(The private predecessor let the unwind kill the lane thread. After that ...)" | History of a fixed bug. | Delete the parenthesis; keep the present-tense containment rule. |
| crates/stm/src/execute.rs:9 | "Wound-wait runtime detection ... is not here yet, by design ... lands later" | Roadmap note. | Keep the invariant sentence only. Delete the plan. |
| crates/stm/src/execute.rs:637 | "`None` when the envelope does not decode: the #92 skip path." | Issue reference. | "`None` when the envelope does not decode. The transaction gets a skip receipt." |
| crates/stm/src/execute.rs:722 | "Spins before a dry worker parks. ... Yielding partway through the spin was tested." | Doc for `SPIN_BEFORE_PARK` sits on `PARALLEL_WORTH_NS`. Two docs merged. | Move the text to line 757. Drop the test history. |
| crates/stm/src/execute.rs:740 | "Recalibrated after a frequency root-cause fix. The original 8us threshold came from" | Dated measurement history. | State the current threshold and its basis in present tense. |
| crates/stm/src/execute.rs:771 | "Gas-limit-derived hard cap on transactions per block ... follow-up)." | Doc for `MAX_BLOCK_TXS` sits on `ADMIT_BATCH`. | Move it above line 779. Delete "noted follow-up" (arenas already recycle). |
| crates/stm/src/execute.rs:871 | "Guessing which dominates has been wrong twice." | Historical anecdote. | Delete the sentence. |
| crates/stm/src/execute.rs:918 | "Reasoning about which side of a transfer is foreign has been wrong twice; this counts it." | Historical anecdote. | "Counts writes whose account domain belongs to another worker." |
| crates/stm/src/execute.rs:988 | "`false` selects the legacy per-worker FIFO scheduler." | "legacy" carries version history. | "`false` selects the per-worker FIFO scheduler." |
| crates/stm/src/execute.rs:1030 | "Measured default: batching 8 completions ... about 33% of block wall time" | Dated measurement. | Keep the rule. Drop the percentages. |
| crates/stm/src/execute.rs:1317 | "Steal one ready transaction from the longest other queue. ... Taken from the back" | Doc for `steal` sits on `fifo_ready`. Two docs merged. | Move lines 1317-1327 above line 1343. |
| crates/stm/src/execute.rs:1352 | "A previous `len > 1` guard, meant to leave the owner its work, silently disabled" | History of a removed guard. | "Any queued transaction is stealable; queues hold 0 or 1 items almost always." |
| crates/stm/src/execute.rs:1418 | "Apply parked completions to the live DAG ... Takes no global lock" | Doc for `prune` sits on `complete_inline`. Two docs merged. | Move lines 1418-1421 above line 1487. |
| crates/stm/src/execute.rs:1427 | "Prune batching only ever existed to amortize the old global graph lock" | History of a removed lock. | Delete the sentence. |
| crates/stm/src/execute.rs:1581 | "A spent block's droppables, shipped to the reaper thread." | Doc for a deleted type sits on `RecyclePools`. | Delete line 1581. |
| crates/stm/src/execute.rs:1620 | "One sealed block handed to the persistent tail thread: drain, release the pool slot" | Doc for `TailJob` sits on `ShardTables`. | Move it above line 1686. |
| crates/stm/src/execute.rs:1946 | "The tail no longer builds a leftover Vec, so this is where spent results are dropped." | "no longer" history. | "Spent results drop here; their read buffers return to the pool." |
| crates/stm/src/execute.rs:2180 | "Per-domain last toucher: the index the edges come from. Feed-owned, since ..." | Doc for a deleted field sits on `preds_buf`. | Delete lines 2180-2185. |
| crates/stm/src/execute.rs:2639 | "Keep measuring while declining. Without this the gate is a trap door ..." | Repeats the method doc at line 2616 word for word. | Delete the block comment. |
| crates/stm/src/execute.rs:2830 | "Round-robin on first sight was tested: it spread domains more evenly, but cost" | Records a rejected experiment. | "Hash the domain to a worker. Hashing is stable across blocks, which keeps state warm." |
| crates/stm/src/execute.rs:3177 | "`submit`, plus a streaming delta release: the tail sends this block's folded delta" | Doc for `submit_streaming` sits on `submit_streaming_mv`. | Move lines 3177-3182 above line 3232. |
| crates/stm/src/execute.rs:3276 | "The block tail: everything after the pool is released." | Doc for `block_tail` sits on `rewrite_frag_sink`. | Move lines 3276-3280 above line 3295. |
| crates/stm/src/execute.rs:3438 | "Fusing hash and fold measured worse twice; overlap won" | Historical measurement. | Describe the current overlap only. |
| crates/stm/src/execute.rs:3444 | "Validation used to be its own phase before the commit" | "used to" history. | Delete the clause. |
| crates/stm/src/execute.rs:3495 | "Fully serial was the first cut. It works fine when the tail is much shorter" | Second merged comment plus history. | Merge into one present-tense note on `KARDAMOM_STM_SERIAL_TAIL`. |
| crates/stm/src/execute.rs:3716 | "This bug was found by the speculative-release adversarial pipeline test ... latent since" | Test and history reference. | "The prefix must start from the full pre-block view, layers included." |
| crates/stm/src/execute.rs:4243 | "found by the speculative-release adversarial test's abort storms" | Test reference plus history. | Keep the ordering rule. Delete the discovery note. |
| crates/stm/src/execute.rs:4358 | "Comparing engines by their outer walls alone has misattributed overhead twice." | Historical anecdote. | Delete the sentence. |
| crates/stm/src/execute.rs:4398 | "Execute one transaction against its multi-version view ... (#92 skip semantics" | Doc for `execute_one`, plus its `#[allow]`, sits on `take_read_buf`. | Move lines 4398-4403 above line 4421. Drop "#92". |
| crates/stm/src/execute.rs:4515 | "The intermediate `logs.clone()` ... was allocation for its own sake." | History of removed code. | "Build wire logs straight from the borrowed result." |

Dead statements found while reading (same fix pass): `let _ = i;` at src/execute.rs:3331,
`let _ = tx_idx;` at src/execute.rs:2810 and 4460 (both variables are used later), and
`let _ = (keep_hot, tail_on_workers, pin_cores);` at src/execute.rs:3454.

## R2 long methods

| file:line | fn name | approx code lines | helpers to extract |
|---|---|---|---|
| crates/stm/src/execute.rs:3295 | `block_tail` | 476 | `check_results_present`, `send_early_mv_release`, `apply_serial_prefix`, `fold_hash_validate`, `commit_clean`, `repair_wounded`, `build_outcome` |
| crates/stm/src/execute.rs:2803 | `push_prepared` | 250 | `assign_worker`, `store_slot`, `admit_sharded`, `admit_serial`, `register_edges` |
| crates/stm/src/execute.rs:1877 | `with_pool` | 257 | `spawn_reaper`, `spawn_tail_thread`, `spawn_workers`, `build_handle` |
| crates/stm/src/execute.rs:3980 | `run_worker_block` | 207 | `wait_for_binding`, `build_worker_evm`, `next_job`, `record_write_domains`, `complete_job` |
| crates/stm/src/execute.rs:4421 | `execute_one` | 161 | `skip_result`, `run_evm`, `capture_bal_fragment`, `return_journal_map`, `build_receipt` |
| crates/stm/src/execute.rs:2296 | `begin_block_deferred_inner` | 150 | `sweep_parked_mv`, `take_recycled_arena`, `build_block_ctx`, `install_ctx` |
| crates/stm/src/execute.rs:275 | `MvView::basic_inner` | 82 | `probe_mv_layers`, `probe_delta_layers`, `probe_base_cache` |
| crates/stm/src/pool.rs:110 | `WorkerPool::new` | 80 | `spawn_lane`, `wait_for_job`, `drain_chunks`, `record_panic` |
| crates/stm/src/execute.rs:1487 | `BlockCtx::prune` | 67 | `drain_worker_buffer`, `close_node_and_collect`, `dispatch_ready` |
| crates/stm/src/execute.rs:210 (pool.rs) | `WorkerPool::run` | 62 | `publish_job`, `await_completion`, `take_panic` |
| crates/stm/src/execute.rs:2695 | `flush_admit_batch` | 59 | `discover_shard_edges`, `release_batch_guards` |
| crates/stm/src/execute.rs:2541 | `advance_base` | 57 | `group_by_shard`, `insert_grouped` |
| crates/stm/src/execute.rs:397 | `MvView::storage_inner` | 51 | Same three probe helpers as `basic_inner`. |

Argument-group structs for the two worst signatures:

- `block_tail` (15 arguments). Group them into three structs.
  `struct TailInput<S> { ctx: BlockCtx<S>, n_txs: usize, delta_out: Option<DeltaOut> }`,
  `struct TailTiming { exec_wall: Duration, drain: Duration }`,
  `struct TailStats { cold: usize, edges: usize, dispatch: Vec<u32> }`,
  `struct TailDeps<'a> { reaper: &'a Sender<Reap>, avg_tx_ns: &'a AtomicU64, recycle: &'a Arc<RecyclePools>, lanes: &'a WorkerPool }`.
  Delete `keep_hot`, `tail_on_workers` and `pin_cores`: line 3454 already ignores them.
  The signature then reads `block_tail(input, timing, stats, deps)`.
- `execute_one` (12 arguments). Group them into two structs.
  `struct TxJob<'a> { local_idx: u32, tx_idx: TxIndex, position: BPosition, envelope: &'a TxEnvelope, decoded: Option<&'a DecodedTx> }`,
  `struct ExecCtx<'a> { mv: &'a MvCache, metrics: &'a Metrics, env: ExecEnv, sink_start_balance: U256, bal_base: Option<u64> }`.
  The signature then reads `execute_one(evm, job, ctx, fresh_reads)`.

## R3 large files

| file | code lines | split plan or KEEP (reason) |
|---|---|---|
| crates/stm/src/execute.rs | 3153 | Split into an `execute/` module directory. `execute/config.rs`: `PoolConfig`, `DEFAULT_PRUNE_BATCH`, `PARALLEL_WORTH_NS`, `STALL_TIMEOUT`, `SPIN_BEFORE_PARK`, `STEAL_WORTH_NS`, `PARK_POLL`, `MAX_BLOCK_TXS`, `ADMIT_BATCH`, `STICKY_CAP` (lines 619-779, 936-1053). `execute/view.rs`: `BlockInput`, `BaseCache`, `MvView` and their `Database`/`DatabaseRef` impls (lines 38-488). `execute/metrics.rs`: `PaddedLen`, `PaddedLen64`, `Metrics`, `StmOutcome` (lines 490-605, 820-934). `execute/prepare.rs`: `Prepared`, `domain_hash`, `domain_hash64`, `prepare` (lines 606-720). `execute/touch.rs`: `TouchSlot`, `TouchTable`, `ShardTables` (lines 1055-1143, 1622-1652). `execute/graph.rs`: `TxSlot`, `WorkerQueue`, `Node`, `BlockCtx` and its impl (lines 781-818, 1145-1565). `execute/recycle.rs`: `RecyclePools`, `SpentArena`, `Reap`, the reaper loop (lines 1581-1618, 1928-1993). `execute/handle.rs`: `PoolState`, `PoolShared`, `PoolHandle`, `with_pool`, the tail thread body (lines 1567-1579, 1821-2170, 2205-2687). `execute/session.rs`: `BlockSession`, `LayerBinder`, `BoundLayers`, `TailJob`, `DeltaOut`, `DeltaRelease`, `MvRelease`, `BlockTicket`, `push_tx`, `push_prepared`, `flush_admit_batch`, `submit*`, `seal` (lines 1677-1819, 2172-2203, 2689-3274). `execute/tail.rs`: `Results`, `rewrite_frag_sink`, `block_tail`, the `HashOut` lane machinery (lines 1654-1675, 3276-3920). `execute/worker.rs`: `WorkerEvm`, `worker_loop`, `run_worker_block`, `take_read_buf`, `execute_one` (lines 3922-4264, 4387-4620). `execute/sequential.rs`: `execute_block_stm`, `execute_block_sequential`, `execute_block_sequential_decoded` (lines 4266-4385). Keep `execute/mod.rs` as the public re-export surface, so no external caller changes. |
| crates/stm/tests/equivalence.rs | 1154 | Split into `tests/common/mod.rs` (the helpers `signers`, `tx`, `db`, `env`, `records`, `assert_identical`, `counter_stats`, `COUNTER_SEL`, lines 20-128) plus four test files: `tests/equivalence_basic.rs` (lines 130-337), `tests/equivalence_scheduler.rs` (lines 339-634, plus the bag tests at 1085-1219), `tests/equivalence_streaming.rs` (lines 636-1083), `tests/equivalence_sharded.rs` (lines 1221-1372). Each file then runs as its own test binary, so the suite also parallelizes. |
| crates/stm/src/mv.rs | 276 | KEEP. One type, one concern (the sharded multi-version store), and every function is short. |
| crates/stm/src/pool.rs | 287 | KEEP. One type with a documented safety model; splitting would separate the `unsafe impl` from its argument. |
| crates/stm/src/schedule.rs | 225 | KEEP. One builder plus its batch wrapper. |

## R4 manual drops

| file:line | snippet | class | fix |
|---|---|---|---|
| crates/stm/src/execute.rs:1410 | `drop(q);` | lock guard release | Wrap the push in `{ let mut q = qh.q.lock()...; ... }`, then notify after the block. |
| crates/stm/src/execute.rs:1461 | `drop(list);` | lock guard release | Extract `fn close_and_collect(&self, job: u32) -> (Vec<u32>, usize)`. The helper's scope ends the borrow. |
| crates/stm/src/execute.rs:2161 | `drop(handle);` | channel sender close (the handle owns a `tail` sender clone) | Wrap `let r = f(&handle);` in its own block. Hard to remove outright: the tail thread exits on sender close. |
| crates/stm/src/execute.rs:2162 | `drop(reap_tx);` | channel sender close | Hard to remove. The reaper's `recv()` loop ends on EOF. Keep, but move both closes into `fn shutdown(shared, reap_tx, tail_tx)`. |
| crates/stm/src/execute.rs:2163 | `drop(tail_tx);` | channel sender close | Same as above. Hard to remove. |
| crates/stm/src/execute.rs:2166 | `drop(st);` | lock guard release | Put the `st.shutdown = true;` write in a block, then notify. |
| crates/stm/src/execute.rs:2533 | `drop(st);` | lock guard release | Put the abort loop in a block inside `abort_active`, then notify. |
| crates/stm/src/execute.rs:2876 | `drop(load);` | lock guard release (`RefCell::borrow`) | Extract `fn least_loaded(&self, workers: usize) -> Option<usize>`. Its return ends the borrow. |
| crates/stm/src/execute.rs:3697 | `drop(delta_arc);` | other (releases an Arc the wound path will not use) | Make the fold arm return `Option<Arc<PendingDelta>>` and match on it. The `None` arm then owns nothing. |
| crates/stm/src/execute.rs:4067 | `drop(q);` | lock guard release | Part of one hand-rolled acquire loop. Extract `fn next_job(&self, qh: &WorkerQueue, worker: usize) -> Option<u32>` and scope each lock inside it. |
| crates/stm/src/execute.rs:4071 | `drop(q);` | lock guard release | Same helper. |
| crates/stm/src/execute.rs:4079 | `drop(q);` | lock guard release | Same helper. |
| crates/stm/src/execute.rs:4091 | `drop(q);` | lock guard release | Same helper. |
| crates/stm/src/execute.rs:4103 | `drop(q);` | lock guard release | Same helper. The comment says the drop enforces lock order, so the helper must keep that order. |
| crates/stm/src/execute.rs:4134 | `drop(q);` | lock guard release | Same helper. |

## R5 sync primitives and channels

Grouped by the struct or function that owns the primitive.

### `MvCache` (src/mv.rs)
| file:line | primitive | verdict | reason | fix |
|---|---|---|---|---|
| crates/stm/src/mv.rs:72,77,94 | `Vec<RwLock<FastMap<Address, Versions>>>` | JUSTIFIED | Every worker publishes and reads account versions concurrently. Sharding is a measured choice. | None. |
| crates/stm/src/mv.rs:78,96 | `Vec<RwLock<FastMap<(Address,B256), Versions>>>` | JUSTIFIED | Same, for storage slots. | None. |
| crates/stm/src/mv.rs:81,99 | `RwLock<FastMap<B256, Bytes>>` | JUSTIFIED | Concurrent CREATE publishes and reads. Append-only. | None. |

### `WorkerPool::Shared` (src/pool.rs)
| file:line | primitive | verdict | reason | fix |
|---|---|---|---|---|
| crates/stm/src/pool.rs:78,112 | `job: Mutex<Option<Job>>` | JUSTIFIED | The pointer must publish to N workers at once, and the condvar needs a mutex. A channel gives one receiver, not a broadcast. | None. |
| crates/stm/src/pool.rs:79,113 | `wake: Condvar` | JUSTIFIED | Workers park between jobs. | None. |
| crates/stm/src/pool.rs:86,87 | `done: Condvar`, `done_lock: Mutex<()>` | JUSTIFIED | Documented measurement: a spin-only wait cost more than 7 minutes on one benchmark. | None. |
| crates/stm/src/pool.rs:88,116 | `generation: AtomicU64` | JUSTIFIED | Stamps the job so a worker never runs a stale one. | None. |
| crates/stm/src/pool.rs:89,117 | `n_chunks: AtomicUsize` | JUSTIFIED | Published to every worker before wake. | None. |
| crates/stm/src/pool.rs:90,118 | `next_chunk: AtomicUsize` | JUSTIFIED | The single `fetch_add` is the whole work-claim mechanism. | None. |
| crates/stm/src/pool.rs:92,119 | `active: AtomicUsize` | JUSTIFIED | The completion counter the caller waits on. It carries the safety argument. | None. |
| crates/stm/src/pool.rs:94,120 | `panic: Mutex<Option<PoolPanic>>` | JUSTIFIED | First panic wins across workers, and `run` clears it per job. | None. |
| crates/stm/src/pool.rs:95,121 | `shutdown: AtomicBool` | JUSTIFIED | Read by every worker in the park loop. | None. |

### `BaseCache` (src/execute.rs)
| file:line | primitive | verdict | reason | fix |
|---|---|---|---|---|
| crates/stm/src/execute.rs:170,181 | `Vec<RwLock<FastMap<Address, Option<AccountInfo>>>>` | JUSTIFIED | Shared read-through cache. The doc explains why per-worker memos scaled badly. | None. |
| crates/stm/src/execute.rs:171,184 | `Vec<RwLock<FastMap<(Address,B256), U256>>>` | JUSTIFIED | Same, for storage. | None. |
| crates/stm/src/execute.rs:172,186 | `RwLock<FastMap<B256, Bytecode>>` | JUSTIFIED | Same, for code. Not sharded, but read-mostly. | None. |

### `WorkerQueue` (src/execute.rs)
| file:line | primitive | verdict | reason | fix |
|---|---|---|---|---|
| crates/stm/src/execute.rs:800,2368 | `q: Mutex<VecDeque<u32>>` | JUSTIFIED | Not a plain producer-to-consumer hand-off: `steal` pops the back, and the stall path pushes the front (lines 1374, 4089). A channel cannot do either. | None. |
| crates/stm/src/execute.rs:810,2369 | `len: AtomicUsize` | JUSTIFIED | Lock-free length hint. The doc names the measured contention it removes. | None. |
| crates/stm/src/execute.rs:811,2370 | `cv: Condvar` | JUSTIFIED | Workers park on it with a bounded timeout. | None. |
| crates/stm/src/execute.rs:817,2371 | `parked: AtomicBool` | JUSTIFIED | Elides a futex syscall per dispatch. | None. |

### `Metrics` (src/execute.rs)
| file:line | primitive | verdict | reason | fix |
|---|---|---|---|---|
| crates/stm/src/execute.rs:836 | `admit_ns: AtomicU64` | UNNECESSARY | Written only at line 3117, on the single feed thread. | Move to a plain `u64` on `BlockSession`; fold once in `submit`. |
| crates/stm/src/execute.rs:839 | `feed_pre_ns: AtomicU64` | UNNECESSARY | Written only at line 2914, feed thread. | Same. |
| crates/stm/src/execute.rs:841 | `feed_dag_ns: AtomicU64` | UNNECESSARY | Written only at line 3016, feed thread. | Same. |
| crates/stm/src/execute.rs:895 | `decode_ns: AtomicU64` | UNNECESSARY | Written only at line 2794, feed thread. | Same. |
| crates/stm/src/execute.rs:903 | `redundant_edges: AtomicU64` | UNNECESSARY | Written only at line 3096, feed thread. | Same. |
| crates/stm/src/execute.rs:925 | `fifo_covered: AtomicU64` | UNNECESSARY | Written only at line 3107, feed thread. | Same. |
| crates/stm/src/execute.rs:914 | `feed_ns: AtomicU64` | UNNECESSARY | Written only at lines 2980 and 3124, feed thread. | Same. |
| crates/stm/src/execute.rs:908 | `commit_hash_ns: AtomicU64` | UNNECESSARY | Written only at line 3801, on the tail thread, from the local `hash_ns`. Then read back at line 3901. | Use the local `hash_ns` directly in the outcome. |
| crates/stm/src/execute.rs:909 | `commit_delta_ns: AtomicU64` | UNNECESSARY | Written only at line 3804, tail thread, from local `delta_ns`. | Use the local `delta_ns` directly. |
| crates/stm/src/execute.rs:882 | `parallel_span_ns: AtomicU64` | UNNECESSARY | Never written and never read. The outcome computes the span from `first_dispatch_ns`. | Delete the field. |
| crates/stm/src/execute.rs:886 | `ramp_ns: AtomicU64` | UNNECESSARY | Never written and never read. | Delete the field. |
| crates/stm/src/execute.rs:887 | `commit_ns: AtomicU64` | UNNECESSARY | Never written and never read. | Delete the field. |
| crates/stm/src/execute.rs:845 | `commit_fold_ns: AtomicU64` | UNNECESSARY | Never written. Read at line 3906, so `commit_fold_us` always reports 0. | Delete the field and the outcome field, or write it in `fold_inline`. |
| crates/stm/src/execute.rs:896 | `predict_ns: AtomicU64` | UNNECESSARY | Never written. Read at line 3904, so `predict_us` always reports 0. | Delete both, or record the predict split in `prepare`. |
| crates/stm/src/execute.rs:846 | `commit_lane_ns: AtomicU64` | JUSTIFIED | Written by every tail lane at line 3595. | None. |
| crates/stm/src/execute.rs:847,850,851,854 | `prune_ns`, `prune_calls`, `prune_forced`, `completions` | JUSTIFIED | Any worker plus the tail thread call `prune`. | None. |
| crates/stm/src/execute.rs:856 | `idle_ns: AtomicU64` | JUSTIFIED | Written by every worker at line 4181. | None. |
| crates/stm/src/execute.rs:859 | `steals: AtomicU64` | JUSTIFIED | Written by every worker at line 4120. | None. |
| crates/stm/src/execute.rs:862,864,867,868 | `reads_total`, `reads_mv_hit`, `reads_base_hit`, `reads_backend` | JUSTIFIED | Each worker folds its local counters once per block (line 250). | None. |
| crates/stm/src/execute.rs:873,874 | `evm_ns`, `publish_ns` | JUSTIFIED | Written by every worker inside `execute_one`. | None. |
| crates/stm/src/execute.rs:879 | `busy_ns: AtomicU64` | JUSTIFIED | Folded per worker at block exit. | None. |
| crates/stm/src/execute.rs:891,892 | `first_dispatch_ns`, `last_done_ns` | JUSTIFIED | `fetch_min`/`fetch_max` across workers. | None. |
| crates/stm/src/execute.rs:920,921 | `writes_own`, `writes_foreign` | JUSTIFIED | Folded per worker at block exit. | None. |
| crates/stm/src/execute.rs:926 | `fifo_stalls: AtomicU64` | JUSTIFIED | Written by every worker at line 4087. | None. |
| crates/stm/src/execute.rs:928 | `read_ns: AtomicU64` | JUSTIFIED | Folded per worker at line 268. | None. |
| crates/stm/src/execute.rs:826 | `PaddedLen(AtomicU32)` | JUSTIFIED | Per-worker completion length, on its own cache line. | None. |
| crates/stm/src/execute.rs:831,933 | `PaddedLen64(AtomicU64)` | JUSTIFIED | Per-worker busy nanoseconds, on its own cache line. | None. |

### `Node` and `BlockCtx` (src/execute.rs)
| file:line | primitive | verdict | reason | fix |
|---|---|---|---|---|
| crates/stm/src/execute.rs:1168 | `open: AtomicBool` | JUSTIFIED | Flipped under the `children` lock; this makes "is p outstanding" and "register my edge" one step. | None. |
| crates/stm/src/execute.rs:1172 | `children: Mutex<Vec<u32>>` | JUSTIFIED | The feed pushes and the finishing worker drains. The doc gives the protocol. | None. |
| crates/stm/src/execute.rs:1177 | `indegree: AtomicU32` | JUSTIFIED | Decremented by any completing worker. | None. |
| crates/stm/src/execute.rs:1180 | `worker: AtomicUsize` | JUSTIFIED | Written by the feed, read by any pruner. | None. |
| crates/stm/src/execute.rs:1190 | `queued: AtomicBool` | JUSTIFIED | Ordered against the queue push; the doc names the wedge it prevents. | None. |
| crates/stm/src/execute.rs:1200 | `fifo_preds: Mutex<Vec<u32>>` | JUSTIFIED | The feed writes, then any taker reads. A `OnceLock` would fit the write-once shape, but the arena reuses nodes across blocks and needs the in-place clear at line 3048. | None. |
| crates/stm/src/execute.rs:1235,2348 | `binding: OnceLock<BoundLayers>` | JUSTIFIED | Exactly the write-once cross-thread publish the late bind needs. | None. |
| crates/stm/src/execute.rs:1245,2354 | `slots: Vec<OnceLock<TxSlot>>` | JUSTIFIED | The feed sets, workers read lock-free. | None. |
| crates/stm/src/execute.rs:1246,2359 | `results: Vec<OnceLock<Result<TxResult,_>>>` | JUSTIFIED | Each worker sets its own index; the tail reads all. | None. |
| crates/stm/src/execute.rs:1250,2364 | `bag: crossbeam ArrayQueue<u32>` | JUSTIFIED | The shared runnable set of the bag scheduler. | None. |
| crates/stm/src/execute.rs:1258,1259,2381,2382 | `admitted`, `finished: AtomicU32` | JUSTIFIED | The drain condition, read by the tail thread and every worker. | None. |
| crates/stm/src/execute.rs:1260,2383 | `sealed: AtomicBool` | JUSTIFIED | Same. | None. |
| crates/stm/src/execute.rs:1264,2384 | `completed: Vec<Mutex<Vec<u32>>>` | JUSTIFIED | The owner pushes; any pruner drains. | None. |
| crates/stm/src/execute.rs:1273,2385 | `completed_len: Vec<PaddedLen>` | JUSTIFIED | Lets a prune skip an untouched buffer without its mutex. | None. |
| crates/stm/src/execute.rs:1275,2386 | `pending: AtomicU64` | JUSTIFIED | Counts DAG updates owed across all workers. | None. |
| crates/stm/src/execute.rs:1301,2408 | `done_cv: Condvar` | UNNECESSARY | Notified at lines 1482, 1556, 1777, 2531 and 4235. No thread ever waits on it; the drain loop uses `yield_now` at line 2054. | Delete the field and all five `notify_all` calls. |
| crates/stm/src/execute.rs:1302,2409 | `aborted: AtomicBool` | JUSTIFIED | Read by every worker and by the tail's drain loop. | None. |
| crates/stm/src/execute.rs:1306,2410 | `double_exit: AtomicU32` | JUSTIFIED | A scheduler-bug counter written from any worker. | None. |

### Pool and recycle plumbing (src/execute.rs)
| file:line | primitive | verdict | reason | fix |
|---|---|---|---|---|
| crates/stm/src/execute.rs:1579,1898,1905 | `PoolShared = (Mutex<PoolState<S>>, Condvar)` | JUSTIFIED | Workers, the feed and the tail thread all read and swap the current block. | None. |
| crates/stm/src/execute.rs:1589,1922 | `arenas: Mutex<Vec<SpentArena>>` | JUSTIFIED | The reaper pushes, the feed pops. | None. |
| crates/stm/src/execute.rs:1590,1923 | `mv_clean: Mutex<Vec<MvCache>>` | JUSTIFIED | Same two threads. | None. |
| crates/stm/src/execute.rs:1591,1924 | `mv_parked: Mutex<Vec<Arc<MvCache>>>` | JUSTIFIED | Same two threads. | None. |
| crates/stm/src/execute.rs:1596,1925 | `read_bufs: Mutex<Vec<Vec<ReadRecord>>>` | JUSTIFIED | The reaper refills, every worker drains in batches of 64. | None. |
| crates/stm/src/execute.rs:1600,1926 | `deltas: Mutex<Vec<PendingDelta>>` | JUSTIFIED | The consumer returns, the tail thread pops. | None. |
| crates/stm/src/execute.rs:1604,1605,1612,1613 | `SpentArena` `OnceLock` vectors | JUSTIFIED | The same arrays that `BlockCtx` reuses. | None. |
| crates/stm/src/execute.rs:1658 | `Results<'a>(&[OnceLock<...>])` | JUSTIFIED | A read-only shared view over the same cells. | None. |
| crates/stm/src/execute.rs:1837,1908,2131 | `avg_tx_ns: Arc<AtomicU64>` | JUSTIFIED | The tail thread writes, the feed thread reads. | None. |
| crates/stm/src/execute.rs:1693,3161,3212,3255 | `out: mpsc::Sender<Result<StmOutcome,_>>` per block | JUSTIFIED | Carries one block's outcome from the tail thread to the caller. | Consider a one-shot channel; `mpsc` allows more than one send. |
| crates/stm/src/execute.rs:1796,3192,3234 | `tx: mpsc::Sender<DeltaRelease>` | JUSTIFIED | Streaming release to the pipeline consumer. | None. |
| crates/stm/src/execute.rs:1798,3191 | `mv_tx: Option<mpsc::Sender<MvRelease>>` | JUSTIFIED | Early mv release to the consumer. | None. |
| crates/stm/src/execute.rs:1810 | `rx: mpsc::Receiver<...>` in `BlockTicket` | JUSTIFIED | The caller's side of the outcome channel. | None. |
| crates/stm/src/execute.rs:1852,2155 | `tail: mpsc::Sender<TailJob<S>>` | JUSTIFIED | The tail thread's inbox. | None. |
| crates/stm/src/execute.rs:1909 | `mpsc::channel::<Reap>()` | JUSTIFIED | Ships spent arenas to the reaper thread. | None. |
| crates/stm/src/execute.rs:1910 | `mpsc::channel::<TailJob<S>>()` | JUSTIFIED | Ships sealed blocks to the tail thread. | None. |

### Tail and admission locals (src/execute.rs)
| file:line | primitive | verdict | reason | fix |
|---|---|---|---|---|
| crates/stm/src/execute.rs:2703 | `edges: AtomicUsize` in `flush_admit_batch` | JUSTIFIED | Every admission lane increments it. | None. |
| crates/stm/src/execute.rs:3456 | `val_ns: AtomicU64` in `block_tail` | JUSTIFIED | Every tail lane adds to it. | None. |
| crates/stm/src/execute.rs:3549 | `wounded_parts: Vec<Mutex<Vec<usize>>>` | REPLACE_WITH_OWNERSHIP | Chunk `ci` is the sole writer of `wounded_parts[ci]`, exactly like `hashes` in the same function. The mutex protects nothing. | Preallocate `Vec<Vec<usize>>` and write disjointly through the `HashOut` pattern already used at line 3553, or have each lane return its list. |
| crates/stm/src/execute.rs:1629,1631 | `ShardTables(Vec<UnsafeCell<TouchTable>>)` with `unsafe impl Sync` | JUSTIFIED | Not a lock, but the same concern. The one-chunk-per-lane contract is documented and enforced by `WorkerPool::run`. | None. Keep the SAFETY comment next to the `unsafe impl`. |

Test-only sites (src/mv.rs:342, src/pool.rs:298, 315, 333, 346, 362, 373) are all JUSTIFIED:
each shares a counter with spawned threads.

## R6 dynamic dispatch

| file:line | snippet | class | generic replacement |
|---|---|---|---|
| crates/stm/src/execute.rs:4433 | `fresh_reads: &mut dyn FnMut() -> Vec<ReadRecord>` | stored `dyn Fn` closure (a borrowed callback) | Make the function generic: `fn execute_one<S: StateDatabase, F: FnMut() -> Vec<ReadRecord>>(..., fresh_reads: &mut F)`. The only caller (line 4201) passes a concrete closure, so this monomorphizes with no cost. |

No `Box<dyn Error>` exists in this crate: every fallible path returns the concrete
`ExecutorError`. No trait objects exist either.

## R7 too many generics

| file:line | item | params/bounds | supertrait proposal |
|---|---|---|---|
| crates/stm/src/execute.rs:4269 | `execute_block_stm<S>` | 1 type param, 4 bounds (`StateDatabase + Sync + Clone + 'static`) | Add `pub trait StmSnapshot: StateDatabase + Sync + Clone + 'static {}` with `impl<T: StateDatabase + Sync + Clone + 'static> StmSnapshot for T {}`. The signature becomes `execute_block_stm<S: StmSnapshot>`. |
| crates/stm/src/execute.rs:1877 | `with_pool<S, R>` | 2 type params, 3 bounds on `S` | Reuse a narrower `pub trait StmBackend: StateDatabase + Sync + 'static {}` with a blanket impl. `R` stays, since it is the closure's return type. |
| crates/stm/src/execute.rs:2208 | `PoolHandle::begin_block<'p>` | 3 bounds on `S` (`StateDatabase + Sync` from the impl, plus `Clone` on the method) | The same `StmSnapshot` supertrait removes the extra `where S: Clone`. |

`BlockSession<'p, 'a, S: StateDatabase + Sync>` (src/execute.rs:2173) has three generic
parameters, but two are lifetimes and only two bounds apply. It does not need a supertrait.
The lifetime pair could collapse: `'p` always outlives `'a` in every call site.

## R8 unnecessary pub

| file:line | item | evidence (who uses it) | fix |
|---|---|---|---|
| crates/stm/src/execute.rs:834-933 | `pub struct Metrics` and all 36 `pub` fields | No hit for `Metrics` outside `crates/stm`. `StmOutcome` already exposes every number. | `pub(crate) struct Metrics` with private fields. |
| crates/stm/src/execute.rs:831 | `pub struct PaddedLen64(pub AtomicU64)` | Used only at src/execute.rs:933 and 2414. | `pub(crate)` on the type and the field, like `PaddedLen` at line 826. |
| crates/stm/src/execute.rs:41-56 | `pub struct BlockInput` and its 4 `pub` fields | Used only at src/execute.rs:1750 and 3997. No hit outside the crate. | Make the struct and its fields `pub(crate)`. |
| crates/stm/src/execute.rs:655 | `pub fn domain_hash64` | Called only at src/execute.rs:706. | `pub(crate)`. |
| crates/stm/src/execute.rs:1035 | `pub const DEFAULT_PRUNE_BATCH` | Used only at src/execute.rs:1041. | `pub(crate)`, or keep `pub` and document it as a tuning reference. |
| crates/stm/src/execute.rs:636-649 | `Prepared` fields `decoded`, `domains`, `domain_hashes`, `primary`, `cold` | `crates/bench` names the type only (stm-p2.rs:356) and never reads a field. | Keep the type `pub`; make the fields private and construct through `prepare`. |
| crates/stm/src/execute.rs:539 | `StmOutcome::declined` | No hit outside `crates/stm`. | Keep for now; it is part of one public result struct. Flag only if the struct is trimmed. |
| crates/stm/src/execute.rs:569 | `StmOutcome::double_exit` | No hit outside `crates/stm`. | Same. |
| crates/stm/src/schedule.rs:36-44 | `pub struct BlockSchedule` and its 4 `pub` fields | Produced only by `build`, which only the file's own tests call. | Move behind `#[cfg(test)]`, or make the whole item `pub(crate)`. |
| crates/stm/src/schedule.rs:98-110 | `pub struct DagBuilder`, `pub cold`, `pub edges` | Used only inside `build` at src/schedule.rs:179. | `pub(crate)`. |
| crates/stm/src/schedule.rs:117 | `pub fn DagBuilder::admit` | Called only at src/schedule.rs:182. | `pub(crate)`. |
| crates/stm/src/schedule.rs:167 | `pub fn build` | Called only by the tests in the same file (lines 236, 249, 290). | `#[cfg(test)]`, or `pub(crate)`. |
| crates/stm/src/schedule.rs:48 | `pub fn scheduling_view` | No caller anywhere in the workspace. | Delete, or `pub(crate)`. |
| crates/stm/src/lib.rs:43 | `pub struct FnvBuild` | Used only inside `crates/stm`. | `pub(crate)`. |
| crates/stm/src/lib.rs:45 | `pub struct Fnv` | Only exists because `FnvBuild` is public. | `pub(crate)` with `FnvBuild`. |
| crates/stm/src/lib.rs:97 | `pub type FastMap` | Used only inside `crates/stm`. | `pub(crate)`. |
| crates/stm/src/mv.rs:154 | `pub fn publish_slot` | Called at src/mv.rs:122 and in this file's tests only. | `pub(crate)`. |
| crates/stm/src/mv.rs:165 | `pub fn publish_code` | Called at src/mv.rs:119 and in tests only. | `pub(crate)`. |
| crates/stm/src/mv.rs:237 | `pub fn final_delta` | Called only at src/execute.rs:3730. | `pub(crate)`. |
| crates/stm/src/mv.rs:261 | `pub fn read_code` | Called at src/execute.rs:105 and 379 only. | `pub(crate)`. |
| crates/stm/src/mv.rs:199 | `pub fn scrub` | Called at src/execute.rs:1983 and 2324 only. | `pub(crate)`. |

Confirmed still needed as `pub`: `MvCache`, `AccountVersion`, `publish_account` (used by
`crates/bench/src/bin/stm-contention.rs`), `PoolConfig`, `StmOutcome`, `PoolHandle`,
`with_pool`, `prepare`, `Prepared`, `PARALLEL_WORTH_NS`, `DeltaRelease`, `MvRelease`,
`BlockTicket`, `WorkerPool`, `PoolPanic`, and `DecodedTx` (re-export).
`execute_block_stm`, `execute_block_sequential` and `LayerBinder` are used only by
`crates/stm/tests/equivalence.rs`, which is a separate crate, so their `pub` is required.

## R9 defensive validation

| file:line | snippet | why | typed replacement |
|---|---|---|---|
| crates/stm/src/execute.rs:1092 | `assert!(capacity_pow2.is_power_of_two(), ...)` | A raw `usize` is checked at run time in non-test code. The only caller (line 2146) already calls `next_power_of_two()`. | `struct Pow2(usize)` with `fn new(n: usize) -> Pow2 { Pow2(n.next_power_of_two()) }`. `TouchTable::new` then takes `Pow2` and cannot fail. |
| crates/stm/src/execute.rs:1746 | `assert!(mv_layers.is_empty(), "mv layers cannot serve the fee sink ...")` | Encodes a two-argument rule as a run-time check. | Replace the `(mv_layers, sink_final)` pair with `enum SinkSource { Probe, Given(Option<AccountInfo>) }` and make `Given` the only variant that accepts mv layers. |
| crates/stm/src/execute.rs:2308 | `assert!(snapshots.len() >= workers.max(1), ...)` | A raw `Vec` is length-checked inside the constructor. | `struct PerWorker<S>(Vec<S>)` with `fn new(v: Vec<S>, workers: usize) -> Result<Self, ExecutorError>`. Build it once at the API boundary. |
| crates/stm/src/execute.rs:2813 | `if i >= MAX_BLOCK_TXS { return Err(...) }` | A per-push bound check on every transaction. | Keep the check, but move it into a `BlockCapacity` counter type whose `next()` returns `Option<u32>`. |
| crates/stm/src/execute.rs:2478 | `debug_assert_eq!(txs.len(), prepared.len(), "one Prepared per tx")` | Two parallel slices must stay aligned; the check runs in debug only. | Take `&[(TxIndex, BPosition, TxEnvelope, Prepared)]`, or zip at the caller. The pairing is then a type fact. |
| crates/stm/src/execute.rs:4312 | `debug_assert_eq!(txs.len(), decoded.len(), "one decode slot per tx")` | Same parallel-slice hazard. | Same fix. |
| crates/stm/src/execute.rs:1881,2220,2387,2699,3546,4284 | `.max(1)` on `workers`, `prune_batch`, `admit_shards`, lanes | `with_pool` already normalizes `cfg.workers` and `cfg.prune_batch` at lines 1881-1886, yet five later layers repeat the clamp. | Give `PoolConfig` a validated twin with `NonZeroUsize` fields, built once in `with_pool`. Later layers then read the value directly. |
| crates/stm/src/execute.rs:71,83,289,310,332,2556 | `if code_hash == B256::ZERO { KECCAK_EMPTY } else { code_hash }` | The same normalization repeats at six layers of the read stack. | `struct CodeHash(B256)` with `fn from_delta(raw: B256) -> CodeHash` that maps zero to `KECCAK_EMPTY` once. Store the normalized value in the delta. |
| crates/stm/src/pool.rs:214 | `if n_chunks == 0 { return Ok(()) }` | A raw `usize` guard at the entry of the hot pool call. | Take `NonZeroUsize`, and let the caller decide what an empty job means. |

## R10 imperative style

| file:line | snippet | functional form |
|---|---|---|
| crates/stm/src/mv.rs:62 | `let mut h = 0xcbf2...; for b in bytes.iter().rev().take(8) { h ^= ...; h = h.wrapping_mul(...) }` | `bytes.iter().rev().take(8).fold(0xcbf2_9ce4_8422_2325u64, |h, b| (h ^ *b as u64).wrapping_mul(0x100_0000_01b3))` |
| crates/stm/src/mv.rs:208 and 218 | Two identical `for sh in &self.accounts { ... }` / `for sh in &self.storage { ... }` blocks | Extract `fn scrub_shards<K, V>(shards: &[Shard<K, V>])` and call it twice. The bodies are byte-identical apart from the field. |
| crates/stm/src/mv.rs:239 and 247 | Two near-identical fold loops in `final_delta` | Same shape; extract a helper that folds one shard vector into a closure-supplied sink. |
| crates/stm/src/execute.rs:611 | `let mut h: u64 = ...; for b in bytes { h ^= ...; h = h.wrapping_mul(...) }` | `bytes.iter().fold(0xcbf2_9ce4_8422_2325u64, |h, b| (h ^ *b as u64).wrapping_mul(0x100_0000_01b3)) % workers as u64` |
| crates/stm/src/execute.rs:685 | `let mut domains = Vec::new(); let mut domain_hashes = Vec::new(); for c in predicted { ... push ... }` | One `fold` over `predicted.filter(|c| *c != DomainKey::Account(FEE_SINK))` that carries `(domains, hashes, primary)`. The `primary` rule then reads as one closure. |
| crates/stm/src/execute.rs:1949 | `let mut bufs = Vec::new(); for c in results.iter_mut() { if let Some(Ok(mut r)) = c.take() { ... bufs.push(b) } }` | `let bufs: Vec<_> = results.iter_mut().filter_map(|c| c.take()).filter_map(Result::ok).map(|mut r| { let mut b = std::mem::take(&mut r.reads); b.clear(); b }).collect();` |
| crates/stm/src/execute.rs:2548 | `let mut acc_by_shard = ...; for (addr, ...) in delta.accounts.iter() { acc_by_shard[..].push(..) }` | `delta.accounts.iter().fold(shards, |mut acc, e| { acc[BaseCache::shard(..)].push(..); acc })`, or keep the loop and note it groups. |
| crates/stm/src/execute.rs:2566 | `for (sh, entries) in acc_by_shard.into_iter().enumerate() { if entries.is_empty() { continue } ... }` | `.into_iter().enumerate().filter(|(_, e)| !e.is_empty())` |
| crates/stm/src/execute.rs:2582 | Same `continue`-on-empty shape for storage | Same filter. |
| crates/stm/src/execute.rs:3582 | `let mut local = Vec::new(); for i in base..end { if ...any(...) { local.push(i) } }` | `let local: Vec<usize> = (base..end).filter(|i| results_ref.get(*i).reads.iter().any(|rec| !mv.validate(*i as u32, rec))).collect();` |
| crates/stm/src/pool.rs:123 | `let mut threads = Vec::with_capacity(workers); for li in 0..workers { threads.push(...) }` | `let threads: Vec<_> = (0..workers).map(|li| { ... }).collect();` |
| crates/stm/src/schedule.rs:180 | `for (i, env) in envelopes.iter().enumerate() { ... for p in preds { s.children[p].push(i); s.indegree[i] += 1 } }` | The outer loop must stay: `dag.admit` mutates the builder in canonical order. The inner `for p in preds` can stay too, since it writes two vectors. Flag only the outer one as already near-functional. |

Loops kept on purpose, with the reason:

- src/execute.rs:1446-1477 (`complete_inline` ready buffer). The fixed stack buffer plus spill
  vector avoids a per-completion allocation on the hot completion path.
- src/execute.rs:1491-1534 (`prune`). It drains mutexes and pushes ready work with side effects.
- src/execute.rs:3056-3102 (`push_prepared` edge loop). It is the serial feed, the measured
  bottleneck, and every iteration takes a lock.
- src/execute.rs:3320-3339 (presence prepass). It returns early with a moved error value.
- src/execute.rs:3417-3435 and 3742-3786 (commit prefix and repair). Both carry running sums and
  mutate results in place.
- src/execute.rs:4054-4263 (`run_worker_block`). A queue state machine with early exits.
- src/pool.rs:138-186 and 243-260 (worker wait and caller spin). Both are documented spin and
  park protocols.

## Tests

### R1 comments

| file:line | snippet (max 80 chars) | why | fix |
|---|---|---|---|
| crates/stm/tests/equivalence.rs:585 | "This test pins the legacy FIFO scheduler's mechanics (eager coverage, ...)" | "legacy" carries version history. | "This test pins the FIFO scheduler's mechanics." |
| crates/stm/tests/equivalence.rs:275 | "Not asserted, since a fast machine may win every race, but visible when it happens" | Explains a non-obvious timing limit; keep. | None. |
| crates/stm/tests/equivalence.rs:441 | "as the first version of this test did." | History of a previous test. | Keep the rule: "Inject the threshold; a machine-load-dependent test is flaky." |
| crates/stm/tests/equivalence.rs:762 | "The speculative-release adversarial case: block 2 is built, fed, and submitted" | Present-tense description of the scenario; keep. | None. |

### R3 large files

| file | code lines | split plan or KEEP (reason) |
|---|---|---|
| crates/stm/tests/equivalence.rs | 1154 | Split as described in the main R3 table: one `tests/common/mod.rs` for the fixture helpers, plus four topic files (basic, scheduler, streaming, sharded). |

### R6 dynamic dispatch

None found.

### R10 imperative style

| file:line | snippet | functional form |
|---|---|---|
| crates/stm/tests/equivalence.rs:53 | `let mut raw = Vec::new(); env.encode_2718(&mut raw);` | Required by the `encode_2718` signature. Keep. |
| crates/stm/tests/equivalence.rs:262 | `let mut fallbacks = 0; for rep in 0..25 { ... if out.fallback { fallbacks += 1 } ... }` | The loop also asserts per repetition, so the counter must stay. Keep. |
| crates/stm/tests/equivalence.rs:583 | `let mut shaped = false; for _attempt in 0..20 { ... }` | A retry loop with early exit. Keep. |
| crates/stm/tests/equivalence.rs:687, 820, 997 | `let mut wound_reps = 0usize; let mut reps = 0usize;` | Two counters over a race-hunting loop that also asserts. Keep. |

# stm

## Summary

The group holds 7,197 lines in five files. `execute.rs` (4,620 lines) carries most of the
duplication. Three shapes dominate the production code. First, the empty-code-hash
normalization rule appears at six sites in `execute.rs` and once more in
`exec-core/src/executor/db.rs`; each copy also rebuilds the same `AccountInfo` literal. Second,
`submit`, `submit_streaming_mv` and `submit_streaming` are three copies of one 25-line body.
Third, `execute_one` re-implements `Executor::execute_tx_decoded`: the error triage, the
status/logs match, the BAL fragment loop and the 18-line `Receipt` literal all exist twice, in
two crates. `mv.rs` doubles every account/slot method. The test file repeats a 24-line
`lying_stats` fixture five times and a `with_pool`/`run_block` block seven times. Estimated
reduction: about 370 lines in production code and about 265 lines in tests, roughly 635 in
total. The prior cross-cutting audit names no item inside `crates/stm`; its neighbouring
exec-core item (`seed_cache_layer`) is done, and its `execute_tx` dispatch item is still open in
the form recorded here.

## R14 production code

| sites | shape | helper (name, signature, home) | lines saved |
| --- | --- | --- | --- |
| `crates/stm/src/execute.rs:68-78`, `:81-95`, `:286-296`, `:307-317`, `:329-339`, `:2553-2563`; `crates/exec-core/src/executor/db.rs:58-80` | Build an `AccountInfo` from `(nonce, balance, code_hash)` and map `B256::ZERO` to `KECCAK_EMPTY`. A consensus rule with a long comment, copied 7 times. | `pub fn account_info(nonce: u64, balance: U256, code_hash: B256) -> AccountInfo` in `kardamom_exec_core::executor::db` (re-export from `executor`) | 48 |
| `crates/stm/src/execute.rs:3142-3175`, `:3189-3230`, `:3232-3273` | Flush the batch, destructure `BlockSession`, set `sealed`, wake every queue, send a `TailJob`. Only `delta_out` differs. | `fn submit_inner(self, delta_out: Option<DeltaOut>) -> Result<BlockTicket, ExecutorError>` in `stm::execute` (private on `BlockSession`) | 55 |
| `crates/stm/src/execute.rs:4461-4620`; `crates/exec-core/src/executor/scope.rs:419-557` | Execute one tx and build its receipt: `transact` error triage (3 arms), status-and-logs match, BAL fragment loop, `contract_address`, and the 18-field `Receipt` literal. | `pub fn classify_evm_error(...) -> ...`, `pub fn status_and_wire_logs(&ExecutionResult) -> (ReceiptStatus, Vec<WireLog>)`, and `pub struct ReceiptParts` + `impl From<ReceiptParts> for Receipt`, all in `kardamom_exec_core::executor::scope` | 55 |
| `crates/stm/src/execute.rs:4305-4346`, `:4348-4385` | Sequential block loop: build `Executor`, loop, time, fold delta, push receipt, print the timing line. Only the decoded arm differs. | Make `execute_block_sequential` call `execute_block_sequential_decoded` with `decoded: Option<&[Option<DecodedTx>]>` (`None` selects the inline arm), in `stm::execute` | 35 |
| `crates/stm/src/execute.rs:3867-3918` (18 `m.<x>_ns.load(..) / 1_000` lines), `:3846`, `:3871` | Read one atomic nanosecond counter and divide by 1,000 into a matching `_us` field. | `macro_rules! us_from { ($m:expr, $($f:ident),*) => ... }` or `fn us(c: &AtomicU64) -> u64` in `stm::execute` | 25 |
| `crates/stm/src/execute.rs:1430-1461`, `:1513-1533` | Close a node: take the child list, `open.swap(false)`, log the same "left the graph twice" message, decrement each child indegree, collect the ready ones, clear the list. | `fn close_node(&self, job: u32, ready: &mut Vec<u32>)` on `BlockCtx` in `stm::execute` | 22 |
| `crates/stm/src/mv.rs:143-152`, `:154-163` and `:174-181`, `:183-191` | Sorted-insert publish and highest-below-index read, once per cell kind, over a `Vec<Shard<K,V>>`. | `fn publish_in<K: Eq + Hash, V>(shards: &[Shard<K,V>], sh: usize, key: K, idx: u32, v: V)` and `fn read_in<K,V: Copy>(shards: &[Shard<K,V>], sh: usize, key: &K, idx: u32) -> Option<(u32, V)>`, free functions in `stm::mv` | 20 |
| `crates/stm/src/mv.rs:208-217`, `:218-227` and `:239-246`, `:247-254` | Two identical `for sh in &self.<kind>` loops in `scrub`, and two identical last-version folds in `final_delta`. | `fn scrub_shards<K, V>(shards: &[Shard<K, V>], cap: usize)` and `fn fold_top<K: Copy, V, F: FnMut(&K, &V)>(shards: &[Shard<K,V>], f: F)` in `stm::mv` | 18 |
| `crates/stm/src/execute.rs:2548-2576`, `:2577-2592` | Group delta entries by base-cache shard, then take one write lock per non-empty shard and insert. | `fn insert_by_shard<K, V>(shards: &[RwLock<FastMap<K,V>>], entries: impl Iterator<Item = (usize, K, V)>)` in `stm::execute` | 14 |
| `crates/stm/src/execute.rs:3525-3529`, `:3620-3624`, `:3665-3669`, `:3791-3795` | Build and send a `DeltaRelease { block, delta, corrected }` on the optional `DeltaOut` channel. | `fn send_release(out: &Option<DeltaOut>, block: u64, delta: Arc<PendingDelta>, corrected: bool)` in `stm::execute` | 15 |
| `crates/stm/src/execute.rs:3417-3435`, `:3770-3781` | The accumulator prefix rule: add gas to `cumulative`, add `fee_delta` to `sink_running`, rewrite the fee-sink write and the BAL fragment. Two copies of a consensus rule. | `fn apply_prefix(r: &mut TxResult, cumulative: &mut u64, sink_running: &mut U256)` in `stm::execute` | 10 |
| `crates/stm/src/execute.rs:1478-1483`, `:1552-1557`, `:1774-1777`, `:2528-2532`, `:3158-3160`, `:3209-3211`, `:3252-3254`, `:4232-4235` | Wake every worker queue's condvar, then `done_cv`. | `fn wake_all(&self)` on `BlockCtx` in `stm::execute` | 14 |
| `crates/stm/src/mv.rs:91-101`; `crates/stm/src/execute.rs:178-188` | Build N shards of `RwLock<FastMap<K, V>>` with `FnvBuild`. | `pub fn shard_vec<K, V>(n: usize) -> Vec<RwLock<FastMap<K, V>>>` in `stm::lib` (next to `FastMap`) | 10 |
| `crates/stm/src/execute.rs:3351`, `:3394-3398`, `:3702-3706`, `:3722` | `ctx.binding.get().expect("layers bound before execution")`. | `fn bound(&self) -> &BoundLayers` on `BlockCtx` in `stm::execute` | 8 |
| `crates/stm/src/execute.rs:3418-3421`, `:3679-3682`, `:3737-3740` | `match cell.get_mut() { Some(Ok(r)) => r, _ => unreachable!("presence prepass ...") }`. | `fn result_mut(cell: &mut OnceLock<Result<TxResult, ExecutorError>>) -> &mut TxResult` in `stm::execute` (mirrors `Results::get` at `:1662-1667`) | 6 |
| `crates/stm/src/execute.rs:2469-2487`, `:2670-2686` | Decline check, `begin_block_per_worker`, feed every tx, `seal`. `run_block_prepared` only adds the `Prepared` zip. | Make `run_block` call `run_block_prepared` with `prepared` computed by `prepare`, in `stm::execute` | 10 |
| `crates/stm/src/pool.rs:132-136`; `crates/stm/src/execute.rs:2118-2125` | `if !pins.is_empty() { core_affinity::set_for_current(CoreId { id: pins[i % pins.len()] }) }`. | `pub fn pin_current(cores: &[usize], i: usize)` in `stm::pool` | 6 |
| `crates/stm/src/execute.rs:62-101` vs `:298-341`; `:126-146` vs `:409-427` | Probe `mv_layers` at `u32::MAX`, then `layers.iter().map(as_ref).chain(base)`, then the snapshot. `BlockInput` and `MvView` carry one copy each. | `fn probe_account(&self, addr) -> Option<AccountInfo>` and `fn probe_slot(&self, addr, key) -> Option<U256>` on `BlockInput`, called by `MvView` (it adds only the `n_base_hit` count), in `stm::execute` | 30 |
| `crates/stm/src/execute.rs:2720-2728`, `:2948-2956`, `:2962-2970`, `:3060-3100` | Register an edge on predecessor `p`: lock `p.children`, test `p.open`, `indegree.fetch_add`, push, count. | `fn try_edge(&self, p: u32, child: u32) -> bool` on `BlockCtx` in `stm::execute` | 18 |
| `crates/stm/src/execute.rs:3505-3539` vs `:3540-3635` | KEEP, differs in phase order. The serial arm validates first and skips the fold and hash on a wound; the lane arm hashes and validates together, under the fold. Sharing the body would either lose the serial arm's skip or force the lane arm to join twice. Only the `DeltaRelease` send inside each (row above) should be shared. | — | 0 |
| `crates/stm/src/mv.rs:58-68` vs `crates/stm/src/execute.rs:190-193` | KEEP, differs in intent. `MvCache::shard_of` folds the eight tail bytes because folding the head clustered structured addresses; `BaseCache::shard` reads byte 19 alone. Merging them would change one cache's measured distribution. | — | 0 |

## R14 tests

| sites | shape | helper (name, signature, home) | lines saved |
| --- | --- | --- | --- |
| `crates/stm/tests/equivalence.rs:230-253`, `:653-676`, `:775-798`, `:956-979`, `:1333-1356` | The 24-line "lying stats" fixture: four `TxObs` that claim the counter writes a sender-derived slot, then `Stats::learn`. Verbatim five times. | `fn lying_stats() -> Stats` next to `counter_stats` at `:112` | 95 |
| `:363-379`, `:410-426`, `:588-604`, `:1095-1111`, `:1235-1251`, `:476-484`, `:536-544` | `with_pool(PoolConfig { .. }, \|pool\| pool.run_block(vec![database.clone(); w], PendingDelta::new(), env(), &recs, stats).unwrap())`. | `fn run_pool(db: &MockStateDatabase, recs: &[(TxIndex, BPosition, TxEnvelope)], stats: &Stats, cfg: PoolConfig) -> StmOutcome` in `equivalence.rs` | 50 |
| `:254-259`, `:677-682`, `:803-807`, `:808-812`, `:980-984`, `:985-989`, `:1157-1161`, `:1357-1361` | Build a block of N counter calls at one nonce: `records((0..n).map(\|i\| tx(&sg[i], nonce, TxKind::Call(COUNTER), 0, &COUNTER_SEL)).collect())`. | `fn counter_block(sg: &[PrivateKeySigner], nonce: u64) -> Vec<(TxIndex, BPosition, TxEnvelope)>` in `equivalence.rs` | 30 |
| `:686-759`, `:820-903`, `:997-1082` | The wound hunt: `wound_reps`/`reps` counters, a 200-rep loop, `if wound_reps > 0 && rep >= 24 { break }`, and the closing `eprintln!`. | `fn hunt_wounds(label: &str, body: impl FnMut(usize) -> bool)` in `equivalence.rs` (the closure returns "wounded") | 30 |
| `:700-707`, `:831-844`, `:847-854`, `:859-871`, `:924-936`, `:1008-1016`, `:1018-1030`, `:1059-1072` | Feed a session and submit it: `for (t, p, e) in &recs { sess.push_tx(*t, *p, e.clone()).unwrap(); }` plus `let (tx, rx) = channel(); sess.submit_streaming(tx, spec)`. | `fn feed(sess: &mut BlockSession<'_, '_, MockStateDatabase>, recs: &[(TxIndex, BPosition, TxEnvelope)])` in `equivalence.rs` | 25 |
| `:136-143`, `:1173-1178`, `:1281-1288` | The same six (or four) interleaved transfers between `sg[i]` and `sg[i+1]`. `:136-143` and `:1281-1288` are byte-identical. | `fn transfer_block(sg: &[PrivateKeySigner]) -> Vec<TxEnvelope>` in `equivalence.rs` | 12 |
| `:567-570`, `:1114-1119`, `:1198-1201`, `:1257-1262` | The hot-chain builder: `(0..rounds).flat_map(\|n\| sg.iter().map(move \|s\| (s, n)).collect::<Vec<_>>()).map(\|(s, n)\| tx(s, n, Call(COUNTER), 0, &COUNTER_SEL))`. | `fn counter_chain(sg: &[PrivateKeySigner], rounds: u64, senders: usize) -> Vec<TxEnvelope>` in `equivalence.rs` | 12 |
| `:99-107`, `:739-750` | Assert `accounts`, `storage` and `code` equal, one `assert_eq!` per field, with a labelled message. | `fn assert_delta_eq(a: &PendingDelta, b: &PendingDelta, label: &str)`, called by `assert_identical` at `:89`, in `equivalence.rs` | 8 |
| `:325-328`, `:814-817`, `:991-994` | `let env2 = ExecEnv { block_number: 2, ..env() };`. | `fn env_at(block: u64) -> ExecEnv` in `equivalence.rs` | 5 |
| `crates/stm/src/pool.rs:294-307`, `:312-328`, `:332-341`, `:345-368`, `:372-383` | `let pool = WorkerPool::new(N, Vec::new()); ... pool.run(n, &\|..\| ..).expect("no panic")` with a `Vec<AtomicUsize>` counter array. Small and readable; the counter array is the only real repeat. | `fn counters(n: usize) -> Vec<AtomicUsize>` in `pool::tests` | 5 |
| `crates/stm/src/mv.rs:292-298`; `crates/stm/tests/equivalence.rs` fixtures | KEEP, differs in crate. `mv::tests::av` is a 7-line unit-test helper with no second site inside the group. | — | 0 |

## Prior audit items

| item | status (open / partly) | note |
| --- | --- | --- |
| Tx/Deposit dispatch triplicated in validator | not in this group | No stm site. |
| CacheDB layer seeding duplicated in exec-core | done | `seed_cache_layer` now exists at `crates/exec-core/src/executor/db.rs:138`; `ExecScope::seed_layer` at `scope.rs:116` calls into it. |
| Consensus-critical duplication, general | open, new site | The cross-cutting list did not name `crates/stm`. It now duplicates two consensus rules from exec-core: the empty-code-hash normalization (`db.rs:73-77`) and the whole `Receipt` build plus skip triage (`scope.rs:442-555`). Both are listed above. |
| Service-binary boilerplate, AeronRuntime, recorder, archive paging, shell metrics, topology, LE readers, Java harness, e2e helpers | not in this group | No `crates/stm` site. |

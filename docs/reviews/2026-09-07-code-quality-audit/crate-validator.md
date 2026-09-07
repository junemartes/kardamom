# validator

## Summary
The crate is well factored, but three problems repeat. First, comments carry
project history: phase numbers, milestone labels, spec file names, issue
numbers, and "used to" narratives appear in 36 places. Second, the binary
exports almost everything as `pub`; `args.rs`, `pumps.rs`, and `adoption.rs`
hold 39 `pub` items in a binary crate, and the library exports 24 more items
that no other crate reads. Third, `main.rs` holds one 349-line `main`
function, and `parallel/engine.rs` keeps a duplicated reference copy of the
whole parallel path (`execute_block_parallel_scoped`). Hot spots are
`src/bin/kardamom-validator/main.rs`, `src/parallel/engine.rs`,
`src/attester.rs`, and `src/epoch_verify.rs`. Note that both large library
files are large only because of their inline test modules: `epoch_verify.rs`
holds 299 production code lines, `attester.rs` holds 342. Counts: R1 36,
R2 13, R3 5, R4 6, R5 17, R6 6, R7 1, R8 33, R9 12, R10 13. Tests add
R1 6, R3 3, R10 3.

## R1 comments
| file:line | snippet (max 80 chars) | why | fix |
|---|---|---|---|
| crates/validator/src/epoch_verify.rs:1 | `Epoch verification against L1, phase 1 of` | phase label + spec doc name | State what the module checks today. |
| crates/validator/src/epoch_verify.rs:2 | `docs/agents/l1-origin-deposit-derivation-spec.md` | spec doc by name | Remove the path. |
| crates/validator/src/epoch_verify.rs:24 | `The deferred verdict is the honest cost of phase 1` | phase label | Say "the content check is deferred", no phase. |
| crates/validator/src/epoch_verify.rs:26 | `Preventing it is phase 2, where executors take their own L1` | roadmap note | Remove; keep the present limit only. |
| crates/validator/src/epoch_verify.rs:222 | `see the spec's l1_origin_genesis edge case` | spec doc reference | State the rule inline. |
| crates/validator/src/epoch_verify.rs:396 | `This is the deferred half of phase 1.` | phase label | Say "the background task records the verdict". |
| crates/validator/src/witness.rs:13 | `Since phase 3, the driver itself lives in the no_std exec core` | historical marker | Say "the driver lives in the exec core". |
| crates/validator/src/witness.rs:165 | `Three checks must all pass (phase 3):` | phase label | Drop the parenthesis. |
| crates/validator/src/parallel/mod.rs:1 | `Seeded parallel batch re-execution, v3.` | version marker | Drop "v3". |
| crates/validator/src/parallel/mod.rs:2 | `See docs/agents/bal-attribution-parallel-validation-spec.md.` | spec doc by name | Remove the path. |
| crates/validator/src/parallel/engine.rs:170 | `WriteSet projection instead diverged on every live transfer.` | past bug narrative | State the rule: build both sides through the capture path. |
| crates/validator/src/parallel/engine.rs:358 | `It stays here, compiling, as the reference for the pooled path` | legacy-code note | Delete the function (see R3). |
| crates/validator/src/parallel/engine.rs:446 | `no_std exec core in phase 3 (the zk guest links the same driver)` | phase label | Say the guest links the same driver. |
| crates/validator/src/parallel/engine.rs:471 | `The previous design spawned one OS thread per batch per` | historical design note | State the present rule: one persistent pool. |
| crates/validator/src/seams.rs:70 | `The typed skip cause (#241) is part of the deterministic` | issue number | Remove `(#241)`. |
| crates/validator/src/seams.rs:97 | `Comparing those against retained BAL frames caused` | past incident | State the invariant in present tense. |
| crates/validator/src/buffers.rs:69 | `This fixes a leak from data that lands just after its take gave up.` | past-fix reference | Say "this stops the buffer from holding dead entries". |
| crates/validator/src/attester.rs:8 | `A challenge (milestone-1` | milestone label | Name the mechanism, not the milestone. |
| crates/validator/src/attester.rs:28 | `A follow-up item, which needs an OutputDeleted watcher` | roadmap note | Move to an issue tracker. |
| crates/validator/src/attester.rs:370 | `attester here collected nothing, and every posted output carried` | past bug narrative | State where leaves come from today. |
| crates/validator/src/attester.rs:415 | `from the delta, which is what the binary used to do through` | explicit "used to" | Delete the clause. |
| crates/validator/src/interop/mod.rs:1 | `docs/specs/egress-node-spec.md v2,` | spec doc + version | Remove the path and version. |
| crates/validator/src/interop/verify.rs:2 | `(docs/specs/interop-outbox-messaging-spec.md §10)` | spec doc by name | Remove the path. |
| crates/validator/src/interop/verify.rs:7 | `This module is the checker's PHASE-1 SKELETON:` | phase label | Say what it checks and what it does not. |
| crates/validator/src/interop/extract.rs:1 | `Origin-side outbox extraction (spec §5):` | spec section reference | Remove the section number. |
| crates/validator/src/interop/serve.rs:2 | `the validator's first; until now it exposed only Prometheus` | historical aside | Delete the clause. |
| crates/validator/src/interop/serve.rs:127 | `UNSIGNED in E1 — E2 adds the per-validator key;` | phase labels | Say "attestations are unsigned". |
| crates/validator/src/bin/kardamom-validator/main.rs:13 | `Milestone 1: re-execute from genesis, or resume through the same` | milestone label | Describe the behaviour only. |
| crates/validator/src/bin/kardamom-validator/main.rs:249 | `caused false-divergence restart cascades.` | past incident | State the invariant. |
| crates/validator/src/bin/kardamom-validator/main.rs:276 | `// Milestone-1 default: no automatic attestation.` | milestone label | Say "default: no attestation". |
| crates/validator/src/bin/kardamom-validator/main.rs:416 | `Epoch verification (phase 1). Sequence rules 1-2 are local and` | phase label | Drop the parenthesis. |
| crates/validator/src/bin/kardamom-validator/pumps.rs:73 | `(never-joined or dead multicast image, #144)` | issue number | Remove `#144`. |
| crates/validator/src/bin/kardamom-validator/pumps.rs:184 | `timer and the last != block dedup are gone — each publish IS a` | describes removed code | Say "each publish is a new committed block". |
| crates/validator/src/bin/kardamom-validator/args.rs:117 | `(phase 1 of docs/agents/l1-origin-deposit-derivation-spec.md).` | phase + spec doc | Remove the reference. |
| crates/validator/src/bin/kardamom-validator/adoption.rs:103 | `the same way as the executor's recovery-D path.` | named internal path | Describe the steps instead. |
| crates/validator/src/parallel/engine.rs:328 | `Slots are already in block order. This sort stays as a cheap,` | explains a no-op sort | Delete the sort and the comment (see R10). |

## R2 long methods
| file:line | fn name | approx code lines | helpers to extract |
|---|---|---|---|
| crates/validator/src/bin/kardamom-validator/main.rs:69 | `main` | 349 | `open_state(&args)` (env, recovery, resume point); `open_streams(&rt, &channels, &args)` (tx_data, join recovery, cluster, pumps); `build_receipt_sink(...)` (attester tee + interop tee); `build_role_hooks(&args, ...)` (block_exec, epoch and remote-epoch observers); `finish(joined, &divergence, &args)` (exit codes and resync). |
| crates/validator/src/bin/kardamom-validator/pumps.rs:32 | `spawn_bal_pump` | 77 | `open_bal_sub(rt, channels)`; `index_claims(&frame, &claims, &extract_claims)` (the decode and insert block, lines 87-123); `reopen(&bal_rt, ...)` for the silence path. |
| crates/validator/src/witness.rs:63 | `anchor_block_witness` | 91 | `initial_targets(witness, delta) -> (acct, slot)` (lines 80-100); `walk_proofs(tx, tables, &acct, &slot, pre_root)` (lines 106-137); `add_missing_target(&mut acct, &mut slot, err) -> bool` (lines 143-158). |
| crates/validator/src/parallel/engine.rs:363 | `execute_block_parallel_scoped` | 71 | None: delete the function and let the tests call the pooled path (see R3). |
| crates/validator/src/parallel/engine.rs:253 | `execute_block_parallel` | 69 | `fork_snapshots(pool, snapshot)` (lines 293-297); `run_batches(pool, ranges, ...) -> Result<Vec<BatchOutcome>>` (lines 303-327); `fold_outcomes(outcomes, tx_count) -> BlockOutcome` (lines 332-354). |
| crates/validator/src/epoch_verify.rs:249 | `spawn` | 66 | `verify_with_retry(source, lockbox, &epoch, anchor) -> VerifyVerdict` (the inner retry loop, lines 275-323); `record_verdict(&div, &epoch, verdict)`. |
| crates/validator/src/prover.rs:190 | `spawn_prover_spool` | 64 | `pin_pre_state(&mut held, &mut pending, snap) -> Pinned` (lines 210-229); `take_records(&flight, next, at) -> Option<...>` (lines 230-241). |
| crates/validator/src/interop/extract.rs:189 | `decode_message_sent` | 75 | `decode_callback(&decoded.callback) -> Option<Callback>` (lines 213-224); `check_leaf(origin, &decoded, &callback) -> Result<B256, _>` (lines 226-252). |
| crates/validator/src/attester.rs:494 | `spawn_attester` | 62 | `build_poster(&cfg) -> Result<OutputPoster<_>, AttesterError>` (lines 497-505); `run_attester(poster, rx, interval)` (the async body, lines 510-560); `post_due(&mut state, &poster)` (lines 533-558). |
| crates/validator/src/parallel/engine.rs:142 | `execute_batch` | 63 | `run_records(&mut scope, records, first_index, &mut batch_bal) -> (Vec<Receipt>, PendingDelta)` (lines 151-183); `verify_units(claims, &computed_idx, first_index, last_index, granularity)` (lines 191-219). |
| crates/validator/src/prover.rs:99 | `spool_block` | 55 | `pre_state_root(snap)` (lines 114-123); `write_frame(dir, block, &bytes, &outputs)` (lines 149-153). |
| crates/validator/src/parallel/claims.rs:67 | `from_alloy` | 56 | `sorted_writes(changes, key, value) -> Vec<(u64, T)>`: the same sort-by-index pattern repeats four times (lines 71-119). |
| crates/validator/src/parallel/dump.rs:15 | `records_json` | 54 | `tx_json`, `deposit_json`, `xchain_json`, one per match arm. |

## R3 large files
| file | code lines | split plan or KEEP (reason) |
|---|---|---|
| crates/validator/src/epoch_verify.rs | 605 (299 production) | Move the inline `#[cfg(test)] mod tests` (line 434, 380 lines) to `epoch_verify_tests.rs`, the pattern `parallel/engine_tests.rs` already uses. The production body is one cohesive checker; KEEP it as one module. |
| crates/validator/src/attester.rs | 555 (342 production) | Split into a directory: `attester/oracle.rs` (the `sol!` binding, `AttesterError`, `OutputPoster`, lines 45-173); `attester/state.rs` (`Output`, `build_output`, `AttestState`, lines 95-334); `attester/sinks.rs` (`AttesterHandle`, `AttestingWriterQueue`, `AttestingReceiptSink`, leaf collection, lines 336-483); `attester/mod.rs` (docs, `AttesterConfig`, `spawn_attester`). Move the tests beside each part. |
| crates/validator/src/parallel/engine_tests.rs | 657 | Split by theme: `engine_tests/fixtures.rs` (lines 21-228: `tx`, `exec_record`, `seq_capture`, `claims_for`); `engine_tests/parity.rs` (parallel vs sequential, K>1, depth-K parent); `engine_tests/forged.rs` (the four fail-stop cases); `engine_tests/dispatch.rs` (pooled-vs-scoped A/B and the mdbx fork test). |
| crates/validator/src/parallel/engine.rs | 366 | Delete `execute_block_parallel_scoped` (lines 363-433). It duplicates the ranges computation and the whole fold of `execute_block_parallel`, and only the tests call it. After that the file is cohesive: KEEP. |
| crates/validator/src/bin/kardamom-validator/main.rs | 379 | Not over 500, but see R2. Move the wiring helpers into `wiring.rs` beside `pumps.rs` and `adoption.rs`. |

## R4 manual drops
| file:line | snippet | class | fix |
|---|---|---|---|
| crates/validator/src/buffers.rs:99 | `drop(g);` | lock guard release | Wrap lines 88-98 in `fn insert_locked(&self, ...)`, then call `self.cv.notify_all()` after it returns. |
| crates/validator/src/flight.rs:108 | `drop(g);` | lock guard release | Build the payload in `fn snapshot_json(&self) -> serde_json::Value`, whose scope ends the borrow before `fs::write`. |
| crates/validator/src/interop/store.rs:79 | `drop(g);` | lock guard release | Move lines 63-78 into `fn append_locked(&self, ...)`, then bump `items` in the caller. |
| crates/validator/src/interop/store.rs:132 | `drop(g);` | lock guard release | Same pattern as line 79. |
| crates/validator/src/bin/kardamom-validator/main.rs:502 | `drop(rt);` | resource release (Aeron runtime) | Hard to remove. The order (cancel pumps, then release the runtime clone) is load-bearing. Put both lines in `fn stop_streams(pump_shutdown, rt, cluster_guard)` so the scope end does the work. |
| crates/validator/src/bin/kardamom-validator/main.rs:503 | `drop(cluster_guard);` | resource release (cluster session) | Same helper as line 502. |

Test-only drops (`tests/forged_envelope_chaos.rs:170`, `:171`,
`src/parallel/engine_tests.rs:812`) are outside the R4 scope for tests. The
two sender drops signal EOF and are hard to remove.

## R5 sync primitives and channels
| file:line | primitive | verdict | reason | fix |
|---|---|---|---|---|
| crates/validator/src/lib.rs:60 | `AtomicBool halted` | JUSTIFIED | Read by the exec thread, the commit thread, and async tasks. | Keep. |
| crates/validator/src/lib.rs:61 | `Mutex<Option<String>>` reason | REPLACE_WITH_OWNERSHIP | The value is written once ("the first reason wins") and only read after. | Use `OnceLock<String>`. `swap` on `halted` then `set` needs no lock. |
| crates/validator/src/buffers.rs:54 | `Mutex<KeyedInner<K,V>>` | JUSTIFIED | Async producer, sync consumer, keyed wait with a deadline. | Keep. |
| crates/validator/src/buffers.rs:81 | `Condvar cv` | JUSTIFIED | Gives `wait_timeout`, which no tokio channel offers on the sync side. | Keep. |
| crates/validator/src/flight.rs:43 | `Mutex<VecDeque<BlockCapture>>` | JUSTIFIED | The exec thread pushes; the commit thread and the spool task read. | Keep. |
| crates/validator/src/interop/store.rs:40 | `Mutex<FeedInner>` | JUSTIFIED | The engine seam writes; many WS handlers read. | Keep. |
| crates/validator/src/interop/store.rs:42 | `watch::Sender<u64>` items | JUSTIFIED | Wakes every subscriber, and a fresh subscriber can tap before it scans. | Keep. |
| crates/validator/src/interop/store.rs:111 | `Mutex<VecDeque<(u64,B256)>>` | JUSTIFIED | The snapshot poller writes; WS handlers read. | Keep. |
| crates/validator/src/interop/store.rs:120 | `watch::Sender<u64>` items | JUSTIFIED | Same role as line 42. | Keep. |
| crates/validator/src/parallel/engine.rs:303 | `Vec<OnceLock<Result<BatchOutcome,_>>>` | REPLACE_WITH_OWNERSHIP | `pool.run` joins every lane before it returns, so each slot has exactly one writer and one reader. Line 326 must `expect("pool ran every chunk")`, which proves the type does not carry the invariant. | Give `WorkerPool` a `map(n, body) -> Vec<T>` that returns the per-index results by value. |
| crates/validator/src/epoch_verify.rs:241 | `mpsc::Sender<EpochRecord>` (cap 64) | JUSTIFIED | Hands epochs from the sync exec thread to the async verifier; `try_send` never blocks. | Keep. |
| crates/validator/src/epoch_verify.rs:255 | `mpsc::channel::<EpochRecord>` | JUSTIFIED | Same channel as line 241. | Keep. |
| crates/validator/src/attester.rs:345 | `UnboundedSender<AttesterMsg>` | JUSTIFIED, but change the bound | Cross-thread feed from sync engine threads, so a channel is right. Unbounded is wrong: a stalled L1 grows the queue without limit. | Use a bounded channel with `try_send`, and count drops, as `EpochVerifier` does. |
| crates/validator/src/attester.rs:507 | `unbounded_channel()` | JUSTIFIED, but change the bound | Same site as line 345. | Same fix. |
| crates/validator/src/bin/kardamom-validator/main.rs:200 | `CancellationToken` | JUSTIFIED | One token stops three pump tasks. | Keep. |
| crates/validator/src/bin/kardamom-validator/pumps.rs:186 | `snap_rx.watch()` | JUSTIFIED | One wake per commit, many readers. | Keep. |
| crates/validator/src/prover.rs:197 | `snap_rx.watch()` | JUSTIFIED | Same channel as pumps. | Keep. |

## R6 dynamic dispatch
| file:line | snippet | class | generic replacement |
|---|---|---|---|
| crates/validator/src/bin/kardamom-validator/main.rs:61 | `type TxReceipts = Box<dyn TxReceiptsPublication>` | trait-object dispatch | Define `enum ReceiptSink { Plain(..), Attesting(..), Extracting(..), AttestingExtracting(..) }` and implement `TxReceiptsPublication` for it. The set of chains is closed and known at build time. |
| crates/validator/src/bin/kardamom-validator/main.rs:295 | `let tx_receipts_pub: Box<dyn TxReceiptsPublication>` | trait-object dispatch | Same enum as line 61. |
| crates/validator/src/bin/kardamom-validator/main.rs:298 | `let mut chain: Box<dyn TxReceiptsPublication>` | trait-object dispatch | Same enum as line 61. |
| crates/validator/src/bin/kardamom-validator/main.rs:451 | `Option<Box<dyn RemoteEpochObserver>>` | trait-object dispatch, never varies | The value is always `Some(RemoteEpochVerifier::new(..))`. Make `RoleHooks::remote_epoch_observer` a concrete `Option<W::RemoteEpoch>` on the wiring trait, and drop the box. |
| crates/validator/src/parallel/engine.rs:475 | `Box::new(move |snapshot, parent, ...| ...)` | stored `dyn Fn` closure (`BlockExec<D>`) | `BlockExec<D>` is an alias in `kardamom_engine`. Make `RoleHooks.block_exec` a generic `F: Fn(..) -> Result<..>` parameter, so the validator's closure is monomorphized. |
| crates/validator/src/prover.rs:47 | `Box::new(move |snapshot, parent, ...| ...)` | stored `dyn Fn` closure (`BlockExec<D>`) | Same change as engine.rs:475. |

No `Box<dyn Error>` exists in this crate; every error path already uses
`anyhow`, `ExecutorError`, or a `thiserror` enum.

## R7 too many generics
| file:line | item | params/bounds | supertrait proposal |
|---|---|---|---|
| crates/validator/src/parallel/claims.rs:23 | `fn last_in_range<K: Copy + Ord, T, U>` | 3 type parameters | The third parameter exists only so the code field can project to a hash. Define `trait ClaimField { type Key: Copy + Ord; type Stored; type Compared; fn project(v: &Self::Stored) -> Self::Compared; }`, implement it for the four field kinds, then the helper takes one parameter: `fn last_in_range<F: ClaimField>(map: &BTreeMap<F::Key, Vec<(u64, F::Stored)>>, from: u64, to: u64) -> BTreeMap<F::Key, F::Compared>`. |

No struct in the crate has 3 or more type parameters, and no where-clause
carries 4 or more bounds.

## R8 unnecessary pub
| file:line | item | evidence (who uses it) | fix |
|---|---|---|---|
| crates/validator/src/bin/kardamom-validator/args.rs:14 | `pub struct ValidatorFileConfig` and its field | Binary crate. Only `main.rs` reads it. | `pub(crate)`. |
| crates/validator/src/bin/kardamom-validator/args.rs:26 | `pub struct Args` and its 30 `pub` fields | Binary crate. Only `main.rs` reads them. | `pub(crate)` on the struct and every field. |
| crates/validator/src/bin/kardamom-validator/args.rs:180 | `pub fn resolve_attester_key` | Binary crate. Only `main.rs` calls it. | `pub(crate)`. |
| crates/validator/src/bin/kardamom-validator/pumps.rs:32 | `pub fn spawn_bal_pump` | Binary crate. Only `main.rs` calls it. | `pub(crate)`. |
| crates/validator/src/bin/kardamom-validator/pumps.rs:144 | `pub fn spawn_receipts_pump` | Binary crate. Only `main.rs` calls it. | `pub(crate)`. |
| crates/validator/src/bin/kardamom-validator/pumps.rs:174 | `pub fn spawn_commit_poller` | Binary crate. Only `main.rs` calls it. | `pub(crate)`. |
| crates/validator/src/bin/kardamom-validator/adoption.rs:30 | `pub fn adopt_checkpoint_if_fresh` | Binary crate. Only `main.rs` calls it. | `pub(crate)`. |
| crates/validator/src/bin/kardamom-validator/adoption.rs:83 | `pub fn bootstrap_trie_if_adopted` | Binary crate. Only `main.rs` calls it. | `pub(crate)`. |
| crates/validator/src/bin/kardamom-validator/adoption.rs:113 | `pub fn resync_after_engine_error` | Binary crate. Only `main.rs` calls it. | `pub(crate)`. |
| crates/validator/src/seams.rs:17 | `pub const BAL_WAIT` | Read only at src/seams.rs:110. | Private const. |
| crates/validator/src/seams.rs:19 | `pub const RECEIPT_WAIT` | Read only at src/seams.rs:197. | Private const. |
| crates/validator/src/seams.rs:25 | `pub fn write_set_eq` | Called only at src/seams.rs:147. | Private fn. |
| crates/validator/src/seams.rs:65 | `pub fn receipt_consistent` | Called only at src/seams.rs:233. | Private fn. |
| crates/validator/src/attester.rs:85 | `pub fn collect_withdrawal_leaves` | Called only at src/attester.rs:389 and in inline tests. | `pub(crate)`. |
| crates/validator/src/attester.rs:396 | `pub fn receipt_withdrawal_leaves` | Called only at src/attester.rs:89 and :471. | Private fn. |
| crates/validator/src/attester.rs:98 | `pub struct Output` fields `state_root`, `withdrawals_root` | Only `output_root` is read outside the tests (src/attester.rs:538). | Keep `output_root` public; make the other two private, or return only the root. |
| crates/validator/src/attester.rs:106 | `pub fn build_output` | Called only at src/attester.rs:537 and in inline tests. | `pub(crate)`. |
| crates/validator/src/attester.rs:202 | `pub struct AttestState` and its 9 `pub` methods | Used only by `spawn_attester` (src/attester.rs:519) and inline tests. | `pub(crate)`. |
| crates/validator/src/attester.rs:375 | `pub struct AttestingWriterQueue` | No caller in the workspace. Its own doc says the live engine cannot use it. | Delete it, and delete the doc paragraph at lines 366-374. |
| crates/validator/src/epoch_verify.rs:171 | `pub fn compare_against_l1` | Called only at src/epoch_verify.rs:390 and in inline tests. | `pub(crate)`. |
| crates/validator/src/epoch_verify.rs:224 | `pub fn check_sequence` | Called only at src/epoch_verify.rs:406 and in inline tests. | `pub(crate)`. |
| crates/validator/src/epoch_verify.rs:87 | `pub enum EpochFault` | Never named outside this file. | `pub(crate)`. |
| crates/validator/src/parallel/claims.rs:56 | `pub reads: BTreeMap<Address, Vec<B256>>` | Written at src/parallel/claims.rs:80 and never read anywhere. | Delete the field and its fill code. |
| crates/validator/src/parallel/claims.rs:161 | `pub struct ClaimSlice` and its 4 `pub` fields | Used only in engine.rs and claims.rs. | `pub(crate)`. |
| crates/validator/src/parallel/mod.rs:41 | `pub use engine::{BatchOutcome, build_seed, execute_batch, execute_block_parallel_scoped}` | Only `crates/bench/tests/parallel_defi_repro.rs` uses this module, and it imports only `ClaimIndex` and `execute_block_parallel`. | Export those two; make the rest `pub(crate)`. Delete `execute_block_parallel_scoped`. |
| crates/validator/src/interop/extract.rs:56 | `pub const SENT_MESSAGES_SLOT_INDEX` | Read only at src/interop/extract.rs:64. | Private const. |
| crates/validator/src/interop/extract.rs:61 | `pub fn sent_messages_slot` | Called at src/interop/extract.rs:281 and in sink tests. | `pub(crate)`. |
| crates/validator/src/interop/extract.rs:79 | `pub fn xchain_anchor_hash` | Called only at src/interop/extract.rs:257. | Private fn. |
| crates/validator/src/interop/sink.rs:39 | `pub const CLAIM_WAIT` | Read only at src/interop/sink.rs:67. | Private const. |
| crates/validator/src/interop/verify.rs:40 | `pub enum RemoteEpochFault` | Never named outside this file. | `pub(crate)`. |
| crates/validator/src/interop/verify.rs:120 | `pub fn check_remote_epoch` | Called only at src/interop/verify.rs:197 and in inline tests. | `pub(crate)`. |
| crates/validator/src/prover.rs:159 | `pub fn assemble_prover_input` | Called only at src/prover.rs:145. | Private fn. |
| crates/validator/src/metrics.rs:5-30 | 12 `pub const *_TOTAL` name constants | Read only inside src/metrics.rs. | Private consts. The wrapper functions are the public surface. |

## R9 defensive validation
| file:line | snippet | why | typed replacement |
|---|---|---|---|
| crates/validator/src/attester.rs:497 | `let key = cfg.private_key.trim().trim_start_matches("0x");` | `AttesterConfig.private_key` is a raw `String` cleaned and parsed deep inside the spawn. | Parse in `args.rs` into `AttesterKey(PrivateKeySigner)` with a fallible constructor. `AttesterConfig` then holds the signer. |
| crates/validator/src/attester.rs:500 | `let url = cfg.l1_rpc_url.parse()` | Same raw-`String` field, parsed at use time. | Let clap parse `--l1-rpc-url` into a `Url` newtype once. |
| crates/validator/src/bin/kardamom-validator/args.rs:180 | `pub fn resolve_attester_key(key: &str)` | Takes a raw `&str`, then branches on an `env:` prefix. | A clap `value_parser` that returns `AttesterKey`, so no later caller sees the raw string. |
| crates/validator/src/bin/kardamom-validator/main.rs:277 | `_ => anyhow::bail!("attestation needs --l1-rpc-url, ...")` | `bail!` on CLI input in non-test code. | A clap argument group with `requires_all`, so clap rejects the partial set. |
| crates/validator/src/attester.rs:231 | `interval: interval.max(1)` | The doc at line 190 states ">= 1", and the clamp repeats at a second layer. | `PostInterval(NonZeroU64)` parsed by clap. |
| crates/validator/src/interop/store.rs:49 | `retention_blocks: retention_blocks.max(1)` | Same clamp pattern, second site. | `RetentionBlocks(NonZeroU64)` parsed once from `--feed-retention-blocks`. |
| crates/validator/src/interop/store.rs:118 | `retention_blocks: retention_blocks.max(1)` | Same clamp, third site. | Same newtype. |
| crates/validator/src/parallel/claims.rs:267 | `let bs = batch_size.max(1);` | Same clamp for the batch size. | `BatchSize(NonZeroUsize)` from `--validation-batch-size`. |
| crates/validator/src/parallel/engine.rs:194, :277, :379 | `u64::from(granularity.max(1))` | The same wire field is clamped at three call sites. | `Granularity(NonZeroU16)` built once when the BAL frame is decoded (pumps.rs:107). |
| crates/validator/src/parallel/engine.rs:474 and src/bin/kardamom-validator/main.rs:378 | `workers.max(1)` after `n.min(40)` | The worker count is bounded at two layers. | `WorkerCount(NonZeroUsize)` clamped once by the clap value parser. |
| crates/validator/src/epoch_verify.rs:350 | `e.to_string().contains("not found")` | Classifies a chain fault by matching error text. | Give `L1EpochSource` a typed error enum with a `BlockNotFound` variant. |
| crates/validator/src/prover.rs:107 | `if snap.block_number() != block.saturating_sub(1) { return Err(...) }` | A check-then-error on two loose `u64` values. | Pass a `PinnedPreState { snapshot, block }` built by one fallible constructor, so the pairing cannot be wrong at the call site. |

## R10 imperative style
| file:line | snippet | functional form |
|---|---|---|
| crates/validator/src/witness.rs:80 | `let mut acct_targets = BTreeSet::new();` then four `for` loops | `let acct_targets: BTreeSet<_> = witness.accounts.iter().map(|a| Nibbles::unpack(keccak256(a.address))).chain(delta.accounts.keys().map(...)).chain(delta.storage.keys().map(|(a,_)| ...)).collect();` and build `slot_targets` with a `fold` into the map. |
| crates/validator/src/parallel/engine.rs:44 | `let mut addrs = ...; addrs.extend(..); addrs.extend(..); sort_unstable(); dedup();` | `let addrs: BTreeSet<Address> = claims.balance.keys().chain(claims.nonce.keys()).chain(claims.code.keys()).copied().collect();` — the set gives sorted, unique keys for free. |
| crates/validator/src/parallel/engine.rs:324 | `let mut outcomes = Vec::new(); for s in slots { outcomes.push(s.into_inner().expect(..)?) }` | `let mut outcomes: Vec<_> = slots.into_iter().map(\|s\| s.into_inner().expect(..)).collect::<Result<_, _>>()?;` |
| crates/validator/src/parallel/engine.rs:411 | `let mut outcomes = Vec::new(); for r in results { outcomes.push(r?) }` | `let mut outcomes: Vec<_> = results.into_iter().collect::<Result<_, _>>()?;` |
| crates/validator/src/parallel/engine.rs:330 | `outcomes.sort_by_key(\|o\| o.first_index);` | The slots are filled in range order, so the sort is a no-op. Delete it and the comment on line 328. |
| crates/validator/src/parallel/engine.rs:335 | `for o in outcomes.iter() { ...; for r in &o.receipts { ... } }` | Hot path: this folds the block's receipts and gas. Keep the loop, but replace the inner `r.clone()` with an owned `into_iter()` over the outcome's receipts, so the fold does no copy. |
| crates/validator/src/attester.rs:397 | `let mut found = Vec::new(); for log in &receipt.logs { if ... { found.push(..) } }` | `receipt.logs.iter().filter(\|l\| l.address == withdrawals::MESSAGE_PASSER).filter_map(\|l\| withdrawals::decode_message_passed(&l.topics, &l.data)).collect()` |
| crates/validator/src/attester.rs:455 | `for (b, mut leaves) in flushed { leaves.sort_by_key(..); self.handle.submit_leaves(..) }` | Extract `fn nonce_ordered(leaves: Vec<(U256,B256)>) -> Vec<B256>`, then the loop reads as one call per block. The loop itself must stay: it drives a side effect in order. |
| crates/validator/src/interop/extract.rs:80 | `let mut buf = Vec::with_capacity(25 + 16); buf.extend_from_slice(..) x3` | Use a fixed `[u8; 41]` filled by slice assignment, matching `sent_messages_slot` at line 62. One shape for one job. |
| crates/validator/src/interop/extract.rs:155 | `let mut out = Vec::new();` with two nested `for` loops and `?` | `receipts.iter().flat_map(\|r\| r.logs.iter().map(move \|l\| (r, l))).filter_map(...).try_fold(Vec::new(), ...)`. The `?` inside makes `try_fold` the right combinator. |
| crates/validator/src/epoch_verify.rs:263 | `loop { let Some(epoch) = rx.recv().await else { return }; ... }` | `while let Some(epoch) = rx.recv().await { ... }`, the form `attester.rs:525` already uses. |
| crates/validator/src/epoch_verify.rs:275 | `let mut attempt = 0u32; loop { attempt += 1; ... }` | `for attempt in 1..=VERIFY_ATTEMPTS { ... }`, with the give-up branch after the loop. The mutable counter and the `attempt < VERIFY_ATTEMPTS` guard then disappear. |
| crates/validator/src/prover.rs:133 | `for r in records { if let BufferedRecord::Tx { envelope, .. } = r { digest.add_tx(..) } }` | `records.iter().filter_map(\|r\| match r { BufferedRecord::Tx { envelope, .. } => Some(envelope), _ => None }).for_each(\|e\| digest.add_tx(&e.raw_tx));` |

`src/attester.rs:160` (`for i in (0..count).rev()`) is an index loop, but each
step awaits an L1 call and returns early on the first non-deleted output.
Keep it.

## Tests

### R1 comments
| file:line | snippet (max 80 chars) | why | fix |
|---|---|---|---|
| crates/validator/src/parallel/engine_tests.rs:530 | `used to fail-stop the engine before the BufferedRecord::XChain arm` | historical narrative | State the rule the test pins. |
| crates/validator/src/parallel/engine_tests.rs:370 | `The depth-K regression: under the pipelined commit, the snapshot can` | names a past defect | Say "under the pipelined commit the snapshot can be K blocks stale". |
| tests/forged_envelope_chaos.rs:25 | `documented blind spot from before this check existed, pinned as a test` | historical narrative | State what the check covers now. |
| tests/forged_envelope_chaos.rs:277 | `This is the documented blind spot from before this check existed:` | same narrative, repeated | Same fix. |
| tests/stateless_reexec.rs:267 | `The recomputed BAL can no longer` | "no longer" | Say "the recomputed BAL differs from the input". |
| tests/witness_anchoring.rs:224 | `Prover fixture export (spec 3c): when KARDAMOM_EMIT_PROVER_FIXTURE=dir` | spec section reference | Drop "(spec 3c)". |

### R3 large files
| file | code lines | split plan or KEEP (reason) |
|---|---|---|
| crates/validator/src/parallel/engine_tests.rs | 657 | Split into `fixtures.rs`, `parity.rs`, `forged.rs`, `dispatch.rs`. See the R3 table above for the line ranges. |
| tests/witness_anchoring.rs | 301 | KEEP. One end-to-end contract: capture, anchor, guest, live trie. |
| tests/withdrawal_e2e.rs | 296 | KEEP. One anvil-backed flow, with a shared `setup`. |

### R6 dynamic dispatch
None found.

### R10 imperative style
| file:line | snippet | functional form |
|---|---|---|
| tests/stateless_reexec.rs:270 | `let mut tampered = false;` with a `for` loop and a `break` | `let tampered = forged.iter_mut().find_map(\|a\| a.balance_changes.first_mut()).map(\|c\| *c.post_balance += U256::from(1u64)).is_some();` |
| tests/forged_envelope_chaos.rs:198 | `let mut out = Vec::new(); while let Ok(m) = c_rx.recv_timeout(..) { out.push(m) }` | `let out: Vec<_> = std::iter::from_fn(\|\| c_rx.recv_timeout(Duration::from_secs(5)).ok()).collect();` |
| crates/validator/src/parallel/engine_tests.rs:196 | `let mut cumulative = 0u64; for (i, rec) in records.iter().enumerate() { ... }` | Keep. The loop threads three mutable values (`bal`, `delta`, `cumulative`) and asserts per record; a `fold` would be harder to read. |

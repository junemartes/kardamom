# e2e

## Summary
The harness code is clean on the structural rules: it has no `dyn`, no `Box<dyn Error>`,
and no manual `drop` outside the bench. The real problems are size and staleness.
`src/harness/mod.rs` holds 569 code lines and one 156-line function (`launch_with_l1`);
`SealerCluster::launch` (103) and `L1::launch` (95) follow. Comments carry a lot of
history: issue `#250`, `PR-4`, "the deleted multiprocess e2e", "the old contract", and
two doc paths that no longer exist (`tests/full_pipeline_e2e.rs`, `tests/chain_semantics.rs`).
`src/pipeline.rs` is a dead module: its only caller is its own unit test. About a dozen
`pub` items in `harness` are reachable but never used outside the harness itself.
Counts: R1 16, R2 6, R3 1, R4 0 (3 in the bench), R5 3, R6 0, R7 1, R8 12, R9 5, R10 7.
The 200 assert hits are almost all `anyhow::ensure!` in `src/scenarios`, which are the
scenarios' own verdicts; R9 lists only the 5 that check inputs in harness code.
Note: `src/scenarios/bridge.rs:257,264,265` hold an `Arc<Mutex<..>>` sampler pair, but
scenarios are test code, so R5 does not cover them.

## R1 comments
| file:line | snippet (max 80 chars) | why | fix |
|---|---|---|---|
| crates/e2e/src/lib.rs:17 | "(nonce ordering, nonce gaps, and RPC liveness today)" | stale list; 15 scenario modules exist | drop the enumeration |
| crates/e2e/src/lib.rs:20 | "`tests/chain_semantics.rs` — binds the scenarios to the local stack" | the path is `tests/chain_semantics/main.rs` | fix the path |
| crates/e2e/src/pipeline.rs:3 | "The pipeline composition lives in `tests/full_pipeline_e2e.rs`" | that file does not exist | delete the module (see R8) |
| crates/e2e/src/harness/mod.rs:409 | "launching at once (#250)." | issue reference | state the bound, drop the number |
| crates/e2e/src/harness/mod.rs:431 | "timeouts (issue #250). The bound is generous on purpose" | issue reference | keep the reason, drop `#250` |
| crates/e2e/src/harness/mod.rs:684 | "regression test for the receipts-pump ownership cycle that made" | history plus "Before the fix, ... over 90 seconds" | state the rule: 20s limit, clean exit required |
| crates/e2e/src/harness/inject.rs:52 | "A bare BlockDelta once failed rkyv validation at the subscription" | "once failed" is history | state the rule: the frame must be structurally valid |
| crates/e2e/src/harness/services.rs:8 | "This is the same mechanism the deleted multiprocess e2e used." | refers to removed code | delete the sentence |
| crates/e2e/src/harness/services.rs:34 | "Profiling the driver JVM found scheduling starvation, not garbage" | dated investigation note | keep the invariant, drop the finding |
| crates/e2e/src/harness/l1/mod.rs:3 | "This reuses the pattern that `crates/validator/tests/withdrawal_e2e.rs`" | cross-file provenance | describe the steps, drop the source |
| crates/e2e/src/harness/mod.rs:54 | "mode: seeded BAL batches on the whole-block path). S12/S14 set it so" | spec ids as shorthand | name the scenarios |
| crates/e2e/src/scenarios/mod.rs:8 | "`ci-cluster.sh` DinD cluster (PR-4 swaps the metrics transport for" | PR reference, and Target-C already exists | describe the current state |
| crates/e2e/src/scenarios/rpc_liveness.rs:127 | "checks identity-honest semantics. The old contract answered with the" | contrasts with a removed contract | state the current contract only |
| crates/e2e/src/scenarios/da_parity.rs:193 | "No caller used that gate anywhere before this scenario." | history | delete |
| crates/e2e/src/scenarios/xchain.rs:12 | "node (spec §5); until then this is the strongest destination-side proof" | roadmap note; `xchain_two_stacks` already lands it | delete the "until then" clause |
| crates/e2e/src/scenarios/divergence.rs:44 | "was not enough for a cold validator on a contended runner (#250)." | issue ref, repeated at :131 and :218 | one present-tense note on the shared constant |
| crates/e2e/src/scenarios/upgrade.rs:252 | "(issue #250: the one-shot read failed three times in one CI day)." | dated incident note | state why the read polls |

## R2 long methods
| file:line | fn name | approx code lines | helpers to extract |
|---|---|---|---|
| crates/e2e/src/harness/mod.rs:264 | `LocalStack::launch_with_l1` | 156 | `prepare_root_and_genesis`, `write_archive_log_config`, `build_l1_wiring`, `await_stack_ready` (the three `poll_until` barriers) |
| crates/e2e/src/harness/sealer.rs:53 | `SealerCluster::launch` | 103 | `reserve_endpoint_sets`, `format_member_strings`, `spawn_member`, `await_leader` |
| crates/e2e/src/harness/l1/mod.rs:66 | `L1::launch` | 95 | `prime_anvil` (setCode/setBalance/impersonate), `deploy_bridge_contracts`, `resolve_deployed_addresses` |
| crates/e2e/src/bin/kardamom-semantics.rs:152 | `run_case` | 80 | `nonce_params(base)`, `consistency_params(base)`, `l1_batch_case(t, l1)` |
| crates/e2e/src/harness/services.rs:261 | `spawn_executor_at` | 56 | `write_cluster_config(spec, name)` (shared with validator and ingress), `add_archive_endpoints` |
| crates/e2e/src/harness/services.rs:344 | `spawn_validator` | 52 | `write_cluster_config`, `add_attester_args`, `add_serve_feed_args` |

## R3 large files
| file | code lines | split plan or KEEP (reason) |
|---|---|---|
| crates/e2e/src/harness/mod.rs | 569 | Split. `harness/config.rs`: `StackConfig`, `Genesis`, `Genesis::path`, `Default`. `harness/launch.rs`: `launch_opt`, `launch`, `launch_with_l1`, `assemble_spec`, `materialise_genesis`, readiness barriers. `harness/control.rs`: `freeze_block_clock`, `crash_executor`, `suspend_executor`, `resume_executor`, `suspend_da_watcher`, `restart_executor`, `wait_validator_exit`, `validator_log`, `validator_alive`. `harness/shutdown.rs`: `ShutdownReport`, `shutdown_graceful`, `drain_until_settled`, `dump_tails`, `Drop`. `harness/load_sampler.rs`: `LoadSampler` and its `Drop` (45 self-contained lines). `mod.rs` keeps the crate docs, the `LocalStack` struct, and re-exports. |
| crates/e2e/src/scenarios/xchain.rs | 397 | KEEP. Under 500 code lines (532 raw). The file is one scenario with two arms that share `feed_msg`, `RECEIVER_INIT_CODE`, and `read_slot`. A split would separate the fixtures from their only users. |

## R4 manual drops
None found in harness or pipeline code. The only `drop` calls are `benches/e2e_throughput.rs:139-141`
(bench code, outside this rule's scope). They are hard to remove: they force a drop order that
`AeronRuntime` needs before testcontainers' async drop, and line 140 must run inside `rt.block_on`.

## R5 sync primitives and channels
| file:line | primitive | verdict | reason | fix |
|---|---|---|---|---|
| crates/e2e/src/harness/l1_verified.rs:59 | `Arc<std::sync::Mutex<Fault>>` | JUSTIFIED | `set_fault` is called from the test task; the value is read by every connection task | Keep. A `tokio::sync::watch<Fault>` would read without a lock and remove the "copy out before await" rule at line 94. |
| crates/e2e/src/harness/l1_verified.rs:62 | `Arc<AtomicU64>` (`served`) | JUSTIFIED | a counter incremented by many connection tasks, read by the test | keep |
| crates/e2e/src/harness/mod.rs:792 | `Arc<AtomicBool>` (`LoadSampler::stop`) | JUSTIFIED | a stop flag between the sampler thread and `Drop` | Keep. `Drop` can block up to 500 ms on the join. An `mpsc::Receiver::recv_timeout(500ms)` would give both the cadence and an immediate stop. |

## R6 dynamic dispatch
None found. The crate has no `dyn` and no `Box<_>`. Errors already go through `anyhow::Result`.

## R7 too many generics
| file:line | item | params/bounds | supertrait proposal |
|---|---|---|---|
| crates/e2e/src/harness/metrics.rs:75 | `poll_until<T, F, Fut>` | 3 type params, 2 bounds | Take one param: `F: AsyncFnMut() -> Result<Option<T>>`. This drops `Fut` and its `Future<Output = ...>` bound. `tests/chain_semantics/derivation.rs:15` already uses `AsyncFn`, so the pattern is in the crate. |

## R8 unnecessary pub
| file:line | item | evidence (who uses it) | fix |
|---|---|---|---|
| crates/e2e/src/pipeline.rs:18 | `pub fn channel_uri_for` | only `src/pipeline.rs:29,33` (its own `#[cfg(test)]` test) | delete the module and its `pub mod pipeline` in lib.rs |
| crates/e2e/src/harness/proc.rs:75 | `Proc::pid` | no caller in the crate | delete |
| crates/e2e/src/harness/mod.rs:768 | `LocalStack::shutdown_report` | no caller; the field is only read at mod.rs:701,705 | delete the accessor; make `ShutdownReport` fields private |
| crates/e2e/src/harness/proc.rs:223 | `free_tcp_port` | only services.rs | `pub(crate)` |
| crates/e2e/src/harness/proc.rs:229 | `free_udp_port` | only services.rs, sealer.rs, aeron.rs | `pub(crate)` |
| crates/e2e/src/harness/proc.rs:167 | `wait_for_log_line` | only sealer.rs:134 | `pub(crate)` |
| crates/e2e/src/harness/proc.rs:194 | `wait_for_file` | only aeron.rs:97,103 | `pub(crate)` |
| crates/e2e/src/harness/proc.rs:118 | `Proc::resume` | only mod.rs:638 | `pub(crate)` |
| crates/e2e/src/harness/metrics.rs:46 | `scrape_blocking` | only metrics.rs:67 | private |
| crates/e2e/src/harness/metrics.rs:15 | `Scrape(pub String)` | `.0` read only at metrics.rs:23 | make the field private |
| crates/e2e/src/harness/aeron.rs:20 | `aeron_all_jar` | only aeron.rs:52 | private |
| crates/e2e/src/harness/sealer.rs:21 | `cluster_jar` | only sealer.rs:55 | private |
| crates/e2e/src/harness/services.rs:60 | `bin_dir` | only services.rs:72 | private |
| crates/e2e/src/harness/aeron.rs:43,47 | `MediaDriver::archive_dir`, `archive_control_endpoint` | only mod.rs:306-311 | `pub(crate)` fields |
| crates/e2e/src/harness/l1/mod.rs:34 | `FINALIZATION_WINDOW` | only l1/mod.rs:115,213 | `pub(crate)` |
| crates/e2e/src/harness/l1/contracts.rs:14 | `L2_MINTER` | only l1/mod.rs:121 | `pub(crate)` |

## R9 defensive validation
| file:line | snippet | why | typed replacement |
|---|---|---|---|
| crates/e2e/src/harness/mod.rs:257 | `ensure!(!cfg.l1, "use LocalStack::launch_opt ...")` | a runtime check for API misuse that a type can prevent | give `launch` a config type with no `l1` field, or make `l1` an `Option<L1Config>` that only `launch_opt` accepts |
| crates/e2e/src/harness/sealer.rs:54 | `ensure!(members >= 1, "cluster needs at least one member")` | raw `usize` checked at the call | take `NonZeroUsize`, or a `SealerMembers` newtype with a fallible constructor |
| crates/e2e/src/harness/l2.rs:292 | `debug_assert!(seed != 0, "xorshift seed must be non-zero")` | raw `u64` checked inside the function | take `NonZeroU64`; callers build the seed once |
| crates/e2e/src/harness/aeron.rs:23 and :31, src/harness/sealer.rs:24 and :33, src/harness/services.rs:73 | `ensure!(p.is_file(), "... not found")` | the same check-then-error repeated at five sites in three modules | one `fn required_file(path: PathBuf, hint: &str) -> Result<ExistingFile>` newtype in `proc.rs`, called once per lookup |
| crates/e2e/src/harness/mod.rs:267 | `ensure!(genesis.is_file(), "genesis not found at ...")` | same pattern, sixth site | same `required_file` helper |

## R10 imperative style
| file:line | snippet | functional form |
|---|---|---|
| crates/e2e/src/harness/mod.rs:217 | `let mut replaced = false;` threaded through a `.map` closure, then `ensure!(replaced, ..)` | `let i = src.lines().position(\|l\| l.trim_start().starts_with("chain_id")).context(..)?;` then rebuild with `enumerate().map(..)` |
| crates/e2e/src/harness/mod.rs:330 | `let mut sequencers = Vec::with_capacity(..); for i in 0..cfg.shards { push(spawn?) }` | `(0..cfg.shards).map(\|i\| services::spawn_sequencer(&spec, i)).collect::<Result<Vec<_>>>()?` |
| crates/e2e/src/harness/mod.rs:454 | `let mut v = vec![..]` then four conditional `push` blocks | chain iterators: `[ingress, executor].into_iter().chain(validator.map(..)).chain(da_watcher.map(..)).chain(sequencers.iter().enumerate().map(..)).collect()` (`dump_tails` at mod.rs:774 already uses this shape) |
| crates/e2e/src/harness/metrics.rs:21 | `let mut found = false; let mut sum = 0.0;` then `found.then_some(sum)` | `lines().filter(..).filter_map(parse).reduce(\|a, b\| a + b)` — `reduce` returns `None` for an empty set, which is exactly the "metric absent" case |
| crates/e2e/src/harness/sealer.rs:58 | `let mut endpoint_sets = ..; for _ in 0..members { push([free_udp_port()?; 5]) }` | `(0..members).map(\|_\| Ok([free_udp_port()?, free_udp_port()?, free_udp_port()?, free_udp_port()?, free_udp_port()?])).collect::<Result<Vec<_>>>()?` |
| crates/e2e/src/harness/sealer.rs:86 | `let mut procs = ..; for id in 0..members { push(spawn?) }` | same `map(..).collect::<Result<Vec<_>>>()`; move the body into a `spawn_member(id)` helper (see R2) |
| crates/e2e/src/scenarios/derivation.rs:429 | `for (sh, count) in &seen { ensure!(*count == 1, ..) }` | `if let Some((sh, c)) = seen.iter().find(\|(_, c)\| **c != 1) { bail!(..) }` |

## Tests

### R1 comments
| file:line | snippet (max 80 chars) | why | fix |
|---|---|---|---|
| crates/e2e/tests/chain_semantics/bridge_da.rs:77 | "(no caller used its gate anywhere before this scenario)" | history | delete the parenthetical |
| crates/e2e/tests/chain_semantics/xchain.rs:14 | "load-bearing: before this slice, the whole-block path had no" | describes a past code state | state the current requirement only |
| crates/e2e/tests/chain_semantics/derivation.rs:5 | "docs/agents/l1-origin-deposit-derivation-spec.md. The checks run" | spec doc by name | name the rule, not the document |
| crates/e2e/tests/chain_semantics/pipeline.rs:99 | "The RPC golden vectors (docs/agents/l1-client-suite-port-spec.md)" | spec doc by name | as above |
| crates/e2e/tests/chain_semantics/main.rs:2 | "local stack (`docs/agents/chain-semantics-e2e-suite-spec.md`)" | spec doc by name | as above |
| crates/e2e/src/harness/l1_verified.rs:290 | "// Below the threshold, nothing changes. This lets a test arm a" | inside `#[cfg(test)]`; explains an invariant in present tense | KEEP |

### R3 large files
None found. The largest test file is `tests/chain_semantics/xchain.rs` at 178 code lines.

### R6 dynamic dispatch
None found. `tests/chain_semantics/derivation.rs:13` already uses a generic `AsyncFn` bound
instead of a trait object.

### R10 imperative style
| file:line | snippet | functional form |
|---|---|---|
| crates/e2e/benches/e2e_throughput.rs:73 | `while !drain_stop_for_task.load(..) { match timeout(..).await { .. } }` | the loop only needs to stop on `Ok(None)` or the flag; a `tokio::sync::watch` receiver in a `tokio::select!` removes the poll and the 50 ms timeout arm |
| crates/e2e/src/scenarios/xchain.rs:222 | `for m in &messages { feed.push_message(m.clone()); }` | `messages.iter().cloned().for_each(\|m\| feed.push_message(m));` — or move the values in when `messages` is not read again |

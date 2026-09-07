# batcher

## Summary
The three crates are clean of `dyn` and of manual locks in production code. The
main problems are size and shape, not safety. Five functions pass 75 code lines:
`optimistic::watch_and_challenge` (115), `kardamom-da-watcher::main` (160),
`interop::watcher::spawn` (103), `live::run` (103), and `live::post_confirmed`
(88). One file passes 500 code lines, `da_watcher/src/interop/watcher.rs` (515),
but 430 of its raw lines are an inline test module; the fix is to move the tests
out, not to split the logic. Doc comments carry many historical notes: "Ported
from", "used to", "In v0", "moved to", "a future task", and a dead link to a
module that does not exist (`crate::reexec`). `live.rs` exports 7 items that only
`live.rs` itself uses, and `deployer/src/addresses.rs` exports 6 items that only
`deployer.rs` uses. Three functions carry an `allow(too_many_arguments)`, and
`prover_submit.rs` puts that allow on the whole module. Counts: R1 17, R2 9,
R3 5, R4 6, R5 18, R6 1, R7 6, R8 17, R9 6, R10 12.

## R1 comments
| file:line | snippet (max 80 chars) | why | fix |
|---|---|---|---|
| crates/da_watcher/src/rpc_source.rs:9 | `//! Ported from \`crates/node/src/l1_source_rpc.rs\`.` | history of the move | delete the sentence |
| crates/da_watcher/src/bin/kardamom-da-watcher.rs:459 | `// prose ("back-pressure") never fires, so every stall used to` | "used to" is history | say only "match Aeron's `BACK_PRESSURED` token" |
| crates/da_watcher/src/bin/kardamom-da-watcher.rs:500 | same text, duplicated | history, and duplicated | same fix in both places |
| crates/da_watcher/src/interop/feed.rs:1 | `//! Re-export shim: the interop feed wire contract moved to the shared` | migration note | delete the module (see R8) |
| crates/da_watcher/src/lib.rs:35 | `//! the new-architecture port of that work. It keeps the L1-side logic` | history | state what the crate does now |
| crates/da_watcher/src/lib.rs:44 | `//!   separate follow-up.` | plan note | delete |
| crates/da_watcher/src/lib.rs:68 | `// The deposit-derivation rule moved to \`kardamom_types::epoch\`` | migration note | say "the rule lives in `kardamom_types::epoch` so the verifier shares it" |
| crates/da_watcher/src/lib.rs:71 | `// it, so existing callers keep working.` | history | delete |
| crates/da_watcher/src/watcher.rs:181 | `// This does not use the old per-deposit "log and carry on"` | compares to a removed design | keep only "an epoch must never be skipped" |
| crates/da_watcher/src/interop/mod.rs:23 | `//! v1 is **feed-trust** (spec §10 tier T0)` | spec-section reference | name the rule, not the section |
| crates/da_watcher/src/interop/mod.rs:2 | `//! (\`docs/specs/interop-outbox-messaging-spec.md\` §5–§6)` | doc-file reference | delete the path |
| crates/batcher/src/l1.rs:17 | `//! to [\`crate::reexec::reconstruct_state\`].` | the module does not exist | link `kardamom_reconstruct` or drop the link |
| crates/batcher/src/frame.rs:15 | `//! No chain is in production; version 1 payloads are not accepted.` | dated statement | say "only version 2 is accepted" |
| crates/batcher/src/frame.rs:3 | `//! This format has no \`state_root\` field (by design, at this stage).` | "at this stage" is dated | say "the format carries no state root" |
| crates/batcher/src/settlement.rs:25 | `/// In v0, this function accepts either a precomputed commitment` | version note plus a plan | describe the current behavior |
| crates/batcher/src/settlement.rs:38 | `/// The actual transaction broadcast (with sidecar) is a future task.` | plan note | delete, or delete the item (R8) |
| crates/batcher/src/batch.rs:4 | `//! The sealer emits boundaries onto B. Today, the` | "Today" is dated | delete "Today" |
| crates/batcher/src/archive_reader.rs:3 | `//! Today, the batcher reads on-disk Aeron Archive segment files.` | dated | present tense, no "Today" |
| crates/batcher/src/archive_reader.rs:178 | `/// Tests and the future writer-side adapter use this helper.` | plan note | say what the helper does |
| crates/batcher/src/batcher.rs:10 | `//! This is a single-instance design for v1.` | version note | say "single instance; no election or standby" |
| crates/batcher/src/multi_archive_reader.rs:12 | `//!    each into a \`(BPosition -> TxEnvelope)\` map. In v0, the offline path` | version note | delete "In v0" |
| crates/batcher/src/multi_archive_reader.rs:76 | `/// In v0, each archive is one segment file.` | version note plus a plan | state the constraint only |
| crates/batcher/src/optimistic.rs:204 | `// calldata has them. In v0, the divergence offset comes from` | version note | state the rule |
| crates/batcher/src/live.rs:11 | `//! from L1; see \`docs/agents/l1-origin-deposit-derivation-spec.md\`.` | doc-file reference | delete the path |
| crates/batcher/src/live.rs:13 | `//! Durability model (see \`docs/agents/batcher-live-l1-spec.md\`):` | doc-file reference | delete the path |
| crates/batcher/src/live.rs:440 | `// post_confirmed stamps last_batch_index.` | restates the next call | delete |

## R2 long methods
| file:line | fn name | approx code lines | helpers to extract |
|---|---|---|---|
| crates/da_watcher/src/bin/kardamom-da-watcher.rs:227 | `main` | 160 | `open_publishers(&aeron_rt, &channels, ...)`; `start_deposits_recorder(...) -> JoinHandle` (lines 280-335); `spawn_watchers(l1, interop, pubs) -> Vec<(&str, WatcherHandle)>`; `await_shutdown_or_fail_stop(watchers) -> bool` (lines 400-426) |
| crates/batcher/src/optimistic.rs:138 | `watch_and_challenge` | 115 | `read_pending(oracle) -> Option<(batch_index, entry, claim)>`; `claimed_arrays_from_log(provider, oracle, batch_index)` (lines 192-217); `first_divergent_offset(claimed, local) -> Option<u64>` (lines 219-225); `read_block_proof(dir) -> Option<(Vec<u8>, Vec<u8>)>` (lines 233-245) |
| crates/da_watcher/src/interop/watcher.rs:178 | `spawn` | 103 | `persist_cursor(cursor_file, cursor, origin_label)` (lines 206-233); `record_tick(origin_label, outcome)`; `handle_outcome(...) -> ControlFlow` (lines 234-286) |
| crates/batcher/src/live.rs:477 | `run` | 103 | `start_l1_side(&args) -> (provider, da_store, L1Truth, BatchCursor, u64)` (lines 485-497); `build_reader_stack(&args, cursor) -> (handles, feed_rx)` (lines 500-559); `report_reader_errors(feed_result, handles)` (lines 591-605) |
| crates/batcher/src/live.rs:254 | `post_confirmed` | 88 | `send_with_retry(&mut self, batch) -> Result<()>` (the whole `loop`); `reconcile_after_error(&self, batch, e) -> Landed \| Transient` (lines 279-307); `record_post_metrics(&self, batch)` (lines 332-342) |
| crates/deployer/src/main.rs:122 | `main` | 84 | `check_minter_counts(&contract_ids, &l2_chain_ids, &l2_minters)` (lines 144-160); `run_bootstrap_7955(rpc_url)` (lines 190-203) |
| crates/batcher/src/bin/kardamom-batcher.rs:177 | `main` | 84 | `open_reader(&cli) -> MultiArchiveReader` (lines 186-199); `scan_archives(reader, &cfg) -> (ScanCounts, Vec<PostedBatch>)` (lines 201-237); `post_all(&cli, batches)` (lines 240-258) |
| crates/da_watcher/src/watcher.rs:95 | `process_once` | 76 | `logs_by_block(source, lockbox, from, tip) -> BTreeMap<..>` (lines 126-133); `derive_one(source, number, logs) -> EpochRecord` (lines 141-146); `publish_epoch(publisher, epoch, cursor) -> ControlFlow` (lines 149-194) |
| crates/da_watcher/src/bin/kardamom-da-watcher.rs:144 | `resolve_paths` | 72 | `resolve_l1(args) -> Option<L1Path>` (lines 145-159); `resolve_interop(args) -> Option<InteropPath>` (lines 161-215); `resolve_start_seq(cursor_file, flag) -> u64` (lines 187-199) |
| crates/da_watcher/src/interop/source.rs:234 | `next_batch` | 75 | `resubscribe(cause: &str)` — the three `drop_session` arms at 263-310 repeat the same warn plus counter plus drop, differing only by the `cause` label |
| crates/batcher/src/bin/kardamom-archive-rereplicate.rs:66 | `main` | 76 | `run_diff(&cli)` (lines 70-92); `run_heal(&cli)` (lines 94-129); `run_mirror(&cli)` (lines 131-155) |
| crates/batcher/src/frame.rs:247 | `decode` | 51 | `decode_block(r) -> BlockFrame` (lines 266-296); `decode_header(r) -> (u32, bool)` (lines 249-262) |
| crates/batcher/src/prover_submit.rs:48 | `submit_next_proof` | 52 | `next_unproven(oracle) -> (u64, SettlementEntry)` (lines 54-74); `read_proof_files(dir) -> Option<(Vec<u8>, Vec<u8>)>` (lines 76-81); `check_public_values(pv, entry)` (lines 86-97) |

## R3 large files
| file | code lines | split plan or KEEP (reason) |
|---|---|---|
| crates/da_watcher/src/interop/watcher.rs | 515 | Only lines 1-293 are logic; lines 295-724 are `mod tests`. Move the tests to `crates/da_watcher/tests/interop_watcher.rs` (the `mock` and `fakes` modules are already gated behind `test-support`). That leaves about 180 code lines. Do NOT split the logic: `process_once` and `spawn` are one loop and its body. |
| crates/batcher/src/live.rs | 486 | Split into a `live/` module: `live/cursor.rs` (`BatchCursor`, `L1Truth`, `read_last_batch_index`, `read_l1_truth`, `reconcile`, lines 75-214), `live/sender.rs` (`LiveSender`, lines 216-345), `live/feed.rs` (`FeedConfig`, `run_feed`, `post_group`, lines 347-445), `live/run.rs` (`LiveArgs`, `run`, `connect_l1`, `BatcherFileConfig`). Tests at 609-666 follow `cursor.rs`. |
| crates/da_watcher/src/watcher.rs | 423 | KEEP the logic (156 code lines: one error enum, one pass, one loop). Move `mod tests` (lines 270-576) to `crates/da_watcher/tests/l1_watcher.rs`. |
| crates/da_watcher/src/bin/kardamom-da-watcher.rs | 351 | Move `LiveTxDepositsPublisher` and `LiveRemoteEpochsPublisher` (lines 438-510) into the library, next to the traits they implement (`publisher.rs` and `interop/publisher.rs`, behind an `aeron` feature). They are the production impls of library traits, and both repeat the same `BACK_PRESSURED` mapping. Then split `main` per R2. |
| crates/deployer/src/deployer.rs | 361 | KEEP. One struct, one lifecycle (`ensure_factory`, `apply`, `addresses`, `verify`) plus three small free helpers. Splitting it would spread the factory address derivation across files. |

## R4 manual drops
| file:line | snippet | class | fix |
|---|---|---|---|
| crates/da_watcher/src/bin/kardamom-da-watcher.rs:434 | `drop(aeron_rt);` | resource release (Aeron runtime) | redundant: it is the last statement before `Ok(())`. Move the watcher and recorder work into `async fn serve(rt: &AeronRuntime, ...)`, so the runtime drops when `serve` returns. |
| crates/da_watcher/src/interop/watcher.rs:372 | `drop(shutdown);` | channel sender close | test code. Hard to remove: the sender must stay alive across the `await` above, or dropping it would request the shutdown the test claims did not happen. Keep, or bind it to a named guard that outlives the await. |
| crates/da_watcher/src/interop/watcher.rs:649 | `drop(source); // the crash: no persist happened` | other (simulated crash) | test code. Wrap the "first life" in a block `{ ... }`, so the scope end is the crash. |
| crates/batcher/tests/docker_e2e.rs:247 | `drop(cluster);` | resource release (Docker container) | put the cluster in a helper whose scope ends the container, or return early from a `run_with_cluster` closure. |
| crates/batcher/tests/metrics_endpoint.rs:43 | `drop(l);` | resource release (TcpListener) | make the bind a block: `let a = { TcpListener::bind(..)?.local_addr()? };`. |
| crates/da_watcher/tests/metrics_endpoint.rs:34 | `drop(l);` | resource release (TcpListener) | same fix; the two `free_port` helpers are duplicates and could move to a shared test helper. |

## R5 sync primitives and channels
| file:line | primitive | verdict | reason | fix |
|---|---|---|---|---|
| crates/da_watcher/src/watcher.rs:208 | `Arc::new(publisher)` | REPLACE_WITH_OWNERSHIP | exactly one task uses it; nothing else holds a clone | move `publisher` into the `async move` block by value, as `interop::watcher::spawn` (line 191) already does |
| crates/da_watcher/src/watcher.rs:209 | `Arc::new(source)` | REPLACE_WITH_OWNERSHIP | same | same |
| crates/da_watcher/src/watcher.rs:264 | `impl EpochPublisher for Arc<P>` | UNNECESSARY | this impl exists only to make the two `Arc`s above work | delete it with the `Arc`s |
| crates/da_watcher/src/watcher.rs:43 | `pub shutdown: oneshot::Sender<()>` | JUSTIFIED | cross-task cancellation signal; `drop` also signals | keep |
| crates/da_watcher/src/watcher.rs:210 | `oneshot::channel::<()>()` | JUSTIFIED | the `select!` shutdown arm | keep |
| crates/da_watcher/src/interop/watcher.rs:188 | `oneshot::channel::<()>()` | JUSTIFIED | same pattern, per pair | keep |
| crates/batcher/src/live.rs:542 | `tokio::sync::mpsc::channel(1 << 14)` | JUSTIFIED | a std reader thread `blocking_send`s to an async task | keep |
| crates/batcher/src/live.rs:34 | `use tokio::sync::mpsc::Receiver` | JUSTIFIED | receiving side of the above | keep |
| crates/da_watcher/src/bin/kardamom-da-watcher.rs:280 | `CancellationToken::new()` | JUSTIFIED | async shell stops a `!Send` archive thread | keep |
| crates/da_watcher/src/bin/kardamom-da-watcher.rs:285 | `oneshot::channel::<Result<i64,String>>()` | JUSTIFIED | the recorder thread reports readiness once | keep |
| crates/da_watcher/src/interop/mock.rs:58 | `script: Mutex<Vec<FeedItem>>` | JUSTIFIED | shared by every jsonrpsee subscription task | keep, but see the next row |
| crates/da_watcher/src/interop/mock.rs:60 | `emitted_lagged: Mutex<BTreeSet<u64>>` | JUSTIFIED | shared, but a second lock over the same logical state | fold `script`, `emitted_lagged`, `swallow` and `next_lagged_id` into one `Mutex<FeedScript>`: four locks guard one script |
| crates/da_watcher/src/interop/mock.rs:62 | `swallow: Mutex<u64>` | JUSTIFIED | shared counter | fold into `Mutex<FeedScript>` (above) |
| crates/da_watcher/src/interop/mock.rs:63 | `next_lagged_id: AtomicU64` | JUSTIFIED | id allocation under `&self` | fold into `Mutex<FeedScript>`; `push_lagged` (line 143) already takes the `script` lock straight after |
| crates/da_watcher/src/interop/mock.rs:66 | `subscribed_dests: Mutex<Vec<u64>>` | JUSTIFIED | assertion log written by every handler task | keep |
| crates/da_watcher/src/interop/mock.rs:68 | `items: watch::Sender<usize>` | JUSTIFIED | one producer wakes N subscription tasks | keep |
| crates/da_watcher/src/interop/mock.rs:70 | `close_epoch: watch::Sender<u64>` | JUSTIFIED | broadcast session close to N tasks | keep |
| crates/da_watcher/src/source.rs:102-105 (test fake) | `tips`/`logs`/`hashes`/`block_hash_fails: Mutex<..>` | JUSTIFIED | `L1Source` takes `&self` and requires `Send + Sync`, so the fake needs interior mutability | keep; one `Mutex<MockScript>` would replace four locks |
| crates/da_watcher/src/publisher.rs:50-51 (test fake) | `published`/`fail_with_backpressure: Arc<Mutex<..>>` | JUSTIFIED | the test holds one handle while the watcher task holds another | keep |
| crates/da_watcher/src/interop/publisher.rs:56-61 (test fake) | `published`, `seen`, `deduped`, `fail_with_backpressure`: four `Arc<Mutex<..>>` | JUSTIFIED (but wrong granularity) | shared with the watcher task, yes; but `publish` (lines 325-334) takes `seen` and then `published` as two separate locks, so two concurrent publishers can interleave between the dedup insert and the push | replace with one `Arc<Mutex<Inner { published, seen, deduped }>>` and take it once |

## R6 dynamic dispatch
| file:line | snippet | class | generic replacement |
|---|---|---|---|
| crates/deployer/src/main.rs:218 | `Deployer<DynProvider>` (`.erased()` at lines 227, 230) | trait-object dispatch | the erasure exists only because the signed and unsigned branches build different provider types. Split `deployer()` into `signed_deployer(rpc, owner, key) -> Deployer<impl Provider>` and `readonly_deployer(rpc, owner) -> Deployer<impl Provider>`; each caller already knows which one it needs (`run_verify` and `run_addresses` never sign). |

No `Box<dyn Error>` and no stored `dyn Fn` exist in these three crates.

## R7 too many generics
| file:line | item | params/bounds | supertrait proposal |
|---|---|---|---|
| crates/batcher/src/archive_reader.rs:69 | `impl<T> TypedSegmentReader<T>` | 1 param, 3 bounds | define `pub trait ArchivedRecord: rkyv::Archive where Self::Archived: rkyv::Deserialize<Self, HighDeserializer<rancor::Error>> + for<'a> CheckBytes<HighValidator<'a, rancor::Error>> {}` with a blanket impl, then write `impl<T: ArchivedRecord>` |
| crates/batcher/src/archive_reader.rs:95 | `impl<T> Iterator for TypedSegmentReader<T>` | 1 param, 3 bounds — the same clause repeated | use `ArchivedRecord` |
| crates/batcher/src/archive_reader.rs:169 | `fn access_owned<T>` | 1 param, 3 bounds — the same clause a third time | use `ArchivedRecord` |
| crates/batcher/src/archive_reader.rs:183 | `fn append_frame<T>` | 1 param, one nested HRTB serializer bound | add a companion `pub trait SegmentRecord` with a blanket impl, so the call sites read `T: SegmentRecord` |
| crates/batcher/src/prover_submit.rs:12 | `#![allow(clippy::too_many_arguments)]` | module-wide allow for a `sol!` product | move the allow onto the `sol!` invocation (lines 24-32) with `#[allow(clippy::too_many_arguments)]`, so hand-written code in the module stays checked |
| crates/deployer/src/main.rs:250 | `run_deploy` with 10 arguments plus an allow | 10 args | pass one `DeployArgs { rpc_url, private_key, owner, ids, l2_chain_ids, minters: Vec<(u64, Address)>, output_oracle, oracle: OracleArgs { attester, challenger, finalization_window } }` and drop the allow |
| crates/deployer/src/spec.rs:137 | `encode_proof_oracle_init_args` with 7 arguments plus an allow | 7 args | take one `ProofOracleInit { settlement, verifier, batch_vkey, block_vkey, genesis_root, challenge_window_secs, min_bond_wei }` |
|  crates/batcher/src/settlement.rs:53 | `PostBatchParams::new` with 7 arguments | 7 args | build from a `&PostedBatch` plus `(settlement, prev_batch_index, versioned_hashes)`; the batch already carries four of the seven |

## R8 unnecessary pub
| file:line | item | evidence (who uses it) | fix |
|---|---|---|---|
| crates/batcher/src/live.rs:349 | `pub struct FeedConfig` | grep across `crates/`: only `live.rs` (lines 570, 368) | `pub(crate)`, or private after the R3 split |
| crates/batcher/src/live.rs:365 | `pub async fn run_feed` | only `live.rs:576` | `pub(crate)` |
| crates/batcher/src/live.rs:50 | `pub struct BatcherFileConfig` | only `live.rs:501` | `pub(crate)`; its `pub cluster` field is read only at line 502 and 526 |
| crates/batcher/src/live.rs:124 | `pub struct L1Truth` | only `live.rs` (161, 279, 487) | `pub(crate)`; its two `pub` fields are read only inside `live.rs` |
| crates/batcher/src/live.rs:161 | `pub async fn read_l1_truth` | only `live.rs` (279, 487) | `pub(crate)` |
| crates/batcher/src/live.rs:193 | `pub fn reconcile` | only `live.rs:488` and its own tests | `pub(crate)` |
| crates/batcher/src/live.rs:58 | `pub mod live_metric_names` | every use is inside `live.rs` | `pub(crate)`, unless the names are a documented scrape contract |
| crates/batcher/src/multi_archive_reader.rs:233 | `pub fn load_a_index` | only `multi_archive_reader.rs:136` | make it private |
|  crates/batcher/src/settlement.rs:30 | `pub fn versioned_hashes_from_commitments` | only `crates/batcher/tests/settlement_binding.rs` | test-only helper for a path the doc calls "a future task": delete it, or gate it behind a `testing` feature |
| crates/batcher/src/settlement.rs:42 | `pub struct PostBatchParams` (7 `pub` fields) | only `tests/settlement_binding.rs`; the real post path builds calldata in `l1.rs:93` | same: delete or gate |
| crates/batcher/src/da_store.rs:62 | `pub fn len` / `:72 pub fn is_empty` | only this file's own `#[cfg(test)]` module | `pub(crate)` or delete |
| crates/batcher/src/multi_archive_reader.rs:151 | `pub fn a_archive_len` | only `tests/multi_archive_reader.rs` | keep if the test is the contract; otherwise gate it |
| crates/deployer/src/addresses.rs:45 | `pub fn proxy_full_initcode` | only `deployer.rs:201` | `pub(crate)` |
| crates/deployer/src/addresses.rs:39 | `pub fn factory_init_data` | only `deployer.rs:186` | `pub(crate)` |
| crates/deployer/src/addresses.rs:29 | `pub fn factory_impl_salt` | only `deployer.rs:195` and this file's tests | `pub(crate)` |
| crates/deployer/src/addresses.rs:34 | `pub fn factory_proxy_salt` | only `deployer.rs:200` and this file's tests | `pub(crate)` |
| crates/deployer/src/addresses.rs:75 | `pub fn app_impl_address` | only `deployer.rs:157` and `:393` | `pub(crate)` |
| crates/deployer/src/addresses.rs:87 | `pub fn app_proxy_address` | only `deployer.rs:159` | `pub(crate)` |
| crates/deployer/src/spec.rs:108 | `pub fn encode_init_calldata` | only `spec.rs:86` and `deployer.rs:158` (re-exported at `lib.rs:21`) | `pub(crate)` and drop the re-export |
| crates/deployer/src/spec.rs:75 | `pub fn build_spec` | only `deployer.rs:233` (re-exported at `lib.rs:20`) | `pub(crate)` and drop the re-export |
| crates/deployer/src/spec.rs:10 | `pub struct DeploymentSpec` (7 `pub` fields) | only `spec.rs` and `deployer.rs`; `spec_to_abi` at `deployer.rs:410` is the only reader | `pub(crate)`; the fields need no `pub` outside the crate |
| crates/deployer/src/ids.rs:34 | `pub const ALL` | only `ids.rs:183`, inside its own test | `pub(crate)`, or `#[cfg(test)]` |
| crates/da_watcher/src/interop/mock.rs:113 | `pub fn origin_chain_id` | no caller anywhere in `crates/` | delete |
| crates/da_watcher/src/interop/mock.rs:176 | `pub fn subscribed_dests` | no caller anywhere in `crates/` (only `subscription_count` at :171 is used) | delete |
| crates/da_watcher/src/interop/feed.rs:1-7 | the whole `pub use kardamom_interop_feed::*` shim | every user inside the crate imports `crate::interop::feed::...`; no other crate imports `kardamom_da_watcher::interop::feed` | delete the module; import `kardamom_interop_feed` directly in `source.rs:45` and `mock.rs:43` |

The three binary crates are clean: `crates/batcher/src/bin`, `crates/da_watcher/src/bin`
and `crates/deployer/src/bin` declare no `pub` item.

## R9 defensive validation
| file:line | snippet | why | typed replacement |
|---|---|---|---|
| crates/da_watcher/src/rpc_source.rs:146 | `if *topic0 != UpgradeInitiated::SIGNATURE_HASH { return Err(..) }` | `decode_lockbox_log` (line 128-131) already dispatched on `topic0`; the check runs twice | let `decode_lockbox_log` pass the already-matched `&Log` inner to a private decoder that assumes the signature, or return a `LockboxTopic` newtype from one `match` |
| crates/da_watcher/src/rpc_source.rs:189 | same check for `DepositInitiated` | same duplicate layer | same |
| crates/batcher/src/rereplicate.rs:269 | `if name.contains('/') \|\| name.contains('\\\\') \|\| !name.ends_with(".rec")` | a raw `&[String]` is validated inside the copy loop, one element at a time | parse at the CLI boundary into `SegmentName(String)` with `SegmentName::parse(&str) -> Result<Self>`; `heal_from_mirror` then takes `&[SegmentName]` and does no checks |
| crates/deployer/src/main.rs:153 | `if !minter_consumers.is_empty() && l2_chain_ids.len() != l2_minters.len()` | the pairing is checked in `main`, then `run_deploy` indexes `l2_minters[i]` at lines 276 and 301, far from the check | build `Vec<(u64, Address)>` (chain id and minter, zipped) at the boundary and pass that; the index and the check both disappear |
| crates/deployer/src/main.rs:414 | `attester.context("--attester required to deploy WithdrawalOutputOracle")?` | required flags are checked deep in the call graph, per op | model the oracle flags as one clap group (`requires = "challenger"`), so clap rejects the combination |
| crates/da_watcher/src/bin/kardamom-da-watcher.rs:147 | `Address::from_str(lockbox)` on `lockbox: Option<String>` | the flag is typed `Option<String>` then parsed by hand | declare `lockbox: Option<Address>`; clap parses it, as `crates/batcher/src/bin/kardamom-batcher.rs:94` already does for `--settlement` |
| crates/batcher/src/settlement.rs:62,67,70 | three `if ... return Err(BatcherError::L1(..))` in `PostBatchParams::new` | the same length and range facts are already guaranteed by `pack_blocks` (`batcher.rs:226`) and by `post_batch` (`l1.rs:79`) | the item is test-only (see R8); delete it, or make it a `From<&PostedBatch>` that cannot fail |

## R10 imperative style
| file:line | snippet | functional form |
|---|---|---|
| crates/batcher/src/optimistic.rs:219 | `let mut offset = None; for (i, r) in call.blockRoots.iter().enumerate() { .. break }` | `call.blockRoots.iter().enumerate().find(\|(i, r)\| roots.get(*i) != Some(r)).map(\|(i, _)\| i as u64)` |
| crates/batcher/src/rereplicate.rs:154 | `let mut verified = 0usize; for entry in read_dir(..) { if .. { verified += 1 } }` | `std::fs::read_dir(source_dir)?.filter_map(Result::ok).filter(\|e\| e.path().extension().is_some_and(\|x\| x == "rec")).count()` |
| crates/batcher/src/blob.rs:51 | `let mut blobs = Vec::new(); let mut offset = 0; while offset < prefixed.len() { .. }` | `prefixed.chunks(USABLE_BYTES_PER_BLOB).map(encode_one_blob).collect()` |
| crates/batcher/src/blob.rs:91 | `let mut field_idx = 0; let mut src = 0; while src < chunk.len() { .. }` | `chunk.chunks(USABLE_BYTES_PER_FIELD).enumerate().for_each(..)` — this is the blob pack hot path, so measure before changing it; the `chunks` form compiles to the same copies |
| crates/batcher/src/blob.rs:109 | `let mut out = Vec::with_capacity(..); for field_idx in 0..(..) { out.extend_from_slice(..) }` | `raw.chunks_exact(FIELD_ELEMENT_BYTES_USIZE).flat_map(\|f\| &f[1..]).copied().collect()` |
| crates/batcher/src/l1.rs:137 | `let mut out = Vec::with_capacity(logs.len()); for log in logs { .. out.push(..) }` | `logs.iter().map(\|log\| { .. }).collect::<Result<Vec<_>, _>>()` |
| crates/batcher/src/l1.rs:160 | nested `for d in descriptors { for vh in .. { .. } }` building `blocks` | inner loop: `d.versioned_hashes.iter().map(\|vh\| { let b = source.fetch_blob(*vh)?; verify_blob_against_hash(*vh, &b)?; Ok(b) }).collect::<Result<Vec<_>,_>>()?`; outer: `try_fold` or `flat_map` over the results |
| crates/da_watcher/src/watcher.rs:130 | `let mut by_block = BTreeMap::new(); for log in logs { entry(..).or_default().push(log) }` | `logs.into_iter().fold(BTreeMap::new(), \|mut m, log\| { m.entry(log.block_number()).or_insert_with(Vec::new).push(log); m })` — a small gain; keep the loop if the fold reads worse |
| crates/deployer/src/main.rs:338 | `let mut ops = Vec::new(); for id in &ids { for chain_id in &l2_chain_ids { ops.push(..) } }` | `ids.iter().flat_map(\|id\| l2_chain_ids.iter().map(move \|c\| Op::Upgrade { .. })).collect()` — the body is pure, so this converts cleanly |
| crates/deployer/src/main.rs:271 | the same shape in `run_deploy` | keep the loop: the body can `bail!` (line 307) and calls `oracle_init_args(..)?`, so early return with a side effect is needed |
| crates/deployer/src/deployer.rs:274 | `let mut out = Vec::with_capacity(count); for i in 0..count { out.push(factory.l2ChainIdAt(..).await?) }` | an index loop over an on-chain array; each step awaits, so keep it, or use `futures::future::try_join_all((0..count).map(..))` to make the round trips concurrent |
| crates/deployer/src/deployer.rs:282 | `let mut entries = Vec::new(); for l2 in l2s { .. for i in 0..count { .. } }` | same shape; concurrency, not style, is the win here |
| crates/deployer/src/deployer.rs:384 | `for s in specs { if let Some(addr) = .. { .. } else { .. } }` in `dedup_impl_specs` | the `else` branch has an empty body with a comment; write it as `s.target_impl = *seen.entry(key).or_insert_with(\|\| app_impl_address(..))` only after checking that the FIRST spec must keep `Address::ZERO` — the current form is correct and the entry form is not, so KEEP and drop the empty `else` |
| crates/deployer/build.rs:170 | `let mut out = Vec::with_capacity(s.len()/2); for pair in s.as_bytes().chunks(2) { .. }` | `s.as_bytes().chunks(2).map(\|p\| Ok((hex_nibble(p[0])? << 4) \| hex_nibble(p[1])?)).collect::<Result<Vec<_>>>()` |
| crates/deployer/build.rs:192 / crates/batcher/build.rs:34 | `walk_sol_files` + `walk_sol_files_into(dir, &mut out)` | two byte-identical recursive collectors in two build scripts; replace both with one shared helper, or `walkdir`-style recursion returning `Vec<PathBuf>` directly |

## Tests

### R1 comments
| file:line | snippet (max 80 chars) | why | fix |
|---|---|---|---|
| crates/batcher/tests/section6_conformance.rs:14 | `//!    sent to anvil, because 4844 sidecar broadcasting is a future task.` | plan note; `l1.rs:post_batch` now sends a real sidecar, so it is also wrong | state what the test asserts today |
| crates/batcher/tests/docker_e2e.rs:24 | `//! \`PostBatchParams\` could be assembled.` | describes an item R8 recommends deleting | update with the item |
| crates/batcher/tests/docker_e2e.rs:26-30 | `//! The full path ... lands when the high-level archive wrappers ship` | plan note | delete |
| crates/batcher/tests/recon_roundtrip.rs:97 | `// Remote-epoch (interop) DA representation — spec §16 Q8.` | spec-section reference | name the rule |

### R3 large files
| file | code lines | split plan or KEEP (reason) |
|---|---|---|
| crates/batcher/tests/optimistic_e2e.rs | 293 | KEEP. One anvil fixture plus the claim and challenge scenarios; splitting would duplicate the fixture. |
| crates/batcher/tests/section6_conformance.rs | 266 | KEEP, but the archive-building helpers (lines 79-150) are duplicated in `docker_e2e.rs` (lines 65-171) and `multi_archive_reader.rs`; move them to a shared `tests/common/archive.rs`. |
| crates/batcher/tests/multi_archive_reader.rs | 255 | KEEP; same shared-helper note. |
| crates/batcher/tests/metrics_endpoint.rs and crates/da_watcher/tests/metrics_endpoint.rs | 40 each | `free_port` and `scrape` are byte-identical in both files; move them to a shared test helper crate. |

### R6 dynamic dispatch
None found.

### R10 imperative style
| file:line | snippet | functional form |
|---|---|---|
| crates/batcher/tests/section6_conformance.rs:317 | `let mut found = false; for .. { found = true }` | `.any(..)` |
| crates/batcher/tests/anvil_e2e.rs:121 | `let mut found = false;` over posted batches | `.any(..)` or `.iter().position(..)` |
| crates/batcher/tests/section6_conformance.rs:106 | `let mut hash_seed: u8 = 0;` incremented per record | `(0u8..).zip(records)` |
| crates/batcher/tests/section6_conformance.rs:102 | `let mut b_off = 0i32;` index counter into the ordering buffer | `(0..).step_by(FRAME_STRIDE).zip(..)`, if the stride is fixed |

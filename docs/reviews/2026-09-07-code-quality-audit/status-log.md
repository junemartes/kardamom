# Status: log group (crates/log, crates/obs)

Phase A. This file lists every row from `crate-log.md` and `inputs-log.md` in
one of three lists: Done, Deferred to Phase B, Not done judged wrong.

## Gates

1. `cargo clippy -p kardamom-log -p kardamom-obs --all-targets -- -D warnings`: pass.
2. `cargo clippy -p kardamom-log -p kardamom-obs --all-targets --features testing,docker-e2e -- -W clippy::pedantic`: zero warnings in `crates/log` and `crates/obs`. (Remaining pedantic warnings in the workspace are in `crates/types`, owned by another group.)
3. `cargo test -p kardamom-log -p kardamom-obs`: pass. Docker-gated tests (`docker-e2e` feature: `aeron_live_e2e.rs`, `offer_connect_race.rs`, `offer_starvation.rs`) were not run, per the brief.
4. `cargo fmt -p kardamom-log -p kardamom-obs`: applied, idempotent on rerun.
5. Dependent crates (`kardamom-da-watcher`, `kardamom-executor`, `kardamom-cluster-adapter`, `kardamom-sequencer`, `kardamom-engine`, `kardamom-ingress`, `kardamom-batcher`, `kardamom-validator`, `kardamom-state`) and `e2e` (`--features full-pipeline-e2e`, check only) all build clean. No guest crate depends on `kardamom-log` or `kardamom-obs`.

## Done

### R1 comments (crate-log.md)

- lib.rs:10 — deleted the removal-history sentence; doc now names only the current model.
- watermark.rs:1, watermark.rs:14 — deleted the module; its durability note moved into `lib.rs`.
- codec.rs:8 — trimmed to "This crate uses rkyv v0.8."
- offer_retry.rs:10 — replaced the 9-line history with the present-tense retry rule.
- publisher.rs:17 — deleted with the module.
- subscriber.rs:4, :5, :28 — deleted with the module (the wrong feature-gate claim, the broken caveat reference, and the broken doc link all went with it).
- supervisor.rs:8, :127 — deleted with the module.
- config/mod.rs:4 — now names the real consumers, not "the quorum aggregator."
- config/mod.rs:137 — rewritten in present tense; names current users.
- config/mod.rs:193 — TODO(consul-watch) moved out of the doc comment into a plain code comment (not a doc-comment roadmap note).
- config/mod.rs:221 — spec doc reference replaced with an inline summary of the rule.
- recorder.rs:8, :22, :268 — broken `run_durable_watermark_loop` doc links deleted; text names the real caller.
- recorder.rs:148 — "historical 60 s" reworded to "the floor is 60 s."
- recorder.rs:248 — the dead `recorder_id`/`archive_dir` parameters this comment excused are gone (see R8 below); comment deleted with them.
- recorder.rs:269 (and the repeats at 290, 320, 336) — collapsed to one present-tense RAII note.
- recorder.rs:382 — rewritten to say why the fetch runs now, not what it used to feed.
- refetch.rs:9, :17 — postmortem/past-tense framing replaced with the direct constraint statement.
- refetch.rs:76 — rewritten to describe what the endpoint is for.
- replay.rs:286 — deleted with the module.
- aeron_live/mod.rs:48, :51 — deleted the stale `ReceiptCache*Handle` bullet and the "quorum aggregator" reference; doc now names real consumers only.
- aeron_live/runtime.rs:294, :354 — comparisons to past designs deleted; doc states the current merge and stays silent on "unchanged."
- aeron_live/thread.rs:27 — incident story replaced with the assembler rule (frames over one MTU).
- aeron_live/thread.rs:112 — "fix for the cluster tx_ordering freeze" replaced with the invariant statement (publish never blocks poll).
- aeron_live/thread.rs:133 — measurement narrative dropped; cadence rule kept.
- aeron_live/pending.rs:24 — dated profiling number dropped; backoff rule kept.
- aeron_live/pending.rs:74, :97 — "old `offer_blocking`" / "old blocking deadline" reworded to describe the current queue and give the deadline's reason.
- aeron_live/handles/tx_receipts.rs:3 (and "legacy" at 102, 121, 205, 239, 342) — all reworded to "single-channel IPC mode."
- aeron_live/handles/tx_receipts.rs:25 — TODO(consul-watch) moved out of the doc comment.
- aeron_live/handles/tx_receipts.rs:167 — removed-design history replaced with the current one-frame-one-batch statement.
- testing.rs:64 — the "historical name" note dropped; the split (`testing/fakes.rs`) states the type's purpose only.
- testing.rs:438 — "Lease/aggregator tests" reworded to name the real users in `testing/fakes.rs`.
- obs/lib.rs:3 — spec doc reference replaced with an inline summary.
- obs/lib.rs:44 — migration history sentence deleted.
- obs/lib.rs:50 — reworded to say the dashboards read this name (not "for compatibility").
- obs/lib.rs:80 — removed-design note replaced with the current one-runtime rule.
- obs/lib.rs:131 — "(#122)" issue reference deleted from the log message text (message text otherwise unchanged).
- obs/bin.rs:5 — migration history replaced with a statement of what the module gives every binary.

### R2 long methods (crate-log.md)

- replay.rs:241 `run_replay_merge`, replay.rs:441 `resolve_recording` — deleted with the module (dead code, zero external callers).
- refetch.rs:133 `fetch_tx_data` — split out `resolve_session_recording()` and `prepare_replay()`; `drain()` now returns `u64` and is shared with `fetch_deposits`.
- refetch.rs:228 `fetch_deposits` — shares `prepare_replay()` and `drain()` with `fetch_tx_data`; loop body only.
- aeron_live/thread.rs:163 `handle_cmd` — split into `enqueue_publish`, `cmd_open_publication`, `cmd_open_subscription`, `cmd_remove_destination`; return type dropped from `Result<(), LogError>` to `()` (also fixes the `unnecessary_wraps` pedantic row below).
- obs/lib.rs:64 `init` — split into `retry_settings`, `build_exporter`, `build_with_retry`, `register_build_gauges`.
- aeron_live/thread.rs:64 `run_aeron_thread` — partial: extracted `poll_subscriptions`. Did not split out `drain_commands`/`wait_next`; see "Not done, judged wrong" below.

### R3 large files (crate-log.md)

- testing.rs (485 lines) — split into `testing/mod.rs`, `testing/fakes.rs`, `testing/cluster.rs`. All existing public paths (`kardamom_log::testing::*`) preserved via `pub use`.
- refetch.rs, replay.rs, aeron_live/runtime.rs, recorder.rs — kept as-is per the appendix's own KEEP verdicts (replay.rs later deleted as dead code, see R8).

### R4 manual drops (crate-log.md)

- None in non-test code, confirmed. No change needed.

### R5 sync primitives (crate-log.md)

- testing.rs:442 `Arc<Mutex<HashMap<u8,VecDeque<..>>>>` (`FakeFsyncWatermarkStream`) — changed to `Rc<RefCell<..>>`; only single-threaded caller is `crates/log/tests/testing_fakes.rs`.
- supervisor.rs:23, :55 (`oneshot` shutdown signal) — deleted with the module.
- replay.rs:142 `unbounded_channel::<(L,T)>` — deleted with the module.
- runtime.rs:31 `CbSender<RuntimeCmd>`, runtime.rs:34 `Arc<AeronThread>`, runtime.rs:110 `bounded(1)` ack, runtime.rs:169 `bounded(1)` handshake, thread.rs:66 `CbReceiver<RuntimeCmd>`, pending.rs:95 `Option<CbSender<Result<BPosition,LogError>>>`, refetch.rs:102 `HashMap<..,UnboundedReceiver<..>>`, refetch.rs:528 `Arc<Unpark>`, testing.rs:36 and :67,145 `Arc<Mutex<StreamMap>>`/`Arc<Mutex<StreamState>>` — reviewed, confirmed JUSTIFIED as the appendix states; left unchanged (no fix suggested for these rows).
- runtime.rs:170 (cmd bus), runtime.rs:319, :343, :360, tx_receipts.rs:241, :286 (unbounded channels with a "bound it" fix suggestion) — these rows themselves are JUSTIFIED (correct primitive for the job); the bounding suggestion is deferred to Phase B (see Deferred list, Defect 10) per the brief's explicit instruction to defer unbounded-channel bounding.

### R6 dynamic dispatch (crate-log.md)

- recorder.rs (4 sites: `record_stream_until_stopped`, `start_stream`, `start_inner`, `find_or_start_recording`) — `should_stop: &mut dyn FnMut() -> bool` changed to `stop: &CancellationToken` (`tokio_util` already a dependency).

### R7 too many generics (crate-log.md)

- codec.rs — added `trait WireMessage: rkyv::Archive<Archived: Deserialize<Self, HighDeserializer<Error>> + for<'a> CheckBytes<HighValidator<'a, Error>>> + Send + Sized + 'static {}` with a blanket impl.
- aeron_live/runtime.rs — `open_subscription<T>`, `open_subscription_merged<T>`, `open_subscription_with_id<T>`, `typed_deliver<T>` all simplified to `T: WireMessage`.
- testing/fakes.rs `FakeTypedSubscription<T>` — simplified to `T: WireMessage`.
- replay.rs `run_replay_merge`, `ReplayMergeSubscriber`, `open_with_loc` and subscriber.rs `TypedSubscriber<T>` — deleted with their modules.

### R8 unnecessary pub (crate-log.md)

- publisher.rs, subscriber.rs, supervisor.rs, replay.rs — all deleted (zero users anywhere in the workspace; other crates named them only in doc comments, confirmed by a workspace-wide grep before deletion).
- recorder.rs `connect_client`, `Recorder::start_b_mdc` — deleted (no caller; `start_stream` covers the case).
- config/mod.rs `QuorumConfig` struct and `LogConfig::quorum` field — deleted; added a `quorum_section_is_rejected` regression test in `config/tests.rs` to pin that an unknown `[quorum]` section is now rejected, not silently accepted.
- aeron_live/handles/{simple,tx_data}.rs `.raw()` (4 methods total) — deleted, no caller.
- obs/lib.rs `DURATION_BUCKETS`, `BUILD_INFO`, `SERVICE_UP` — all made private.

### R9 defensive validation (crate-log.md)

- replay.rs:289 `matching_recordings > 1` check — deleted with the module.
- aeron_live/runtime.rs:314 `if uris.is_empty()` — signature changed from `uris: &[&str]` to `(first: &str, rest: &[&str])`, so the empty case cannot compile. (This is an in-group-only helper used solely inside `aeron_live`; grepped, no external caller.)

### R10 imperative style (crate-log.md)

- refetch.rs:196 `drain` — now returns `u64` (a fold over received items); caller writes `let delivered = drain(rx, sink);`.
- refetch.rs:454 `Rc<Cell<Vec<_>>>` take/set dance — changed to `Rc<RefCell<Vec<_>>>`; `list_recordings` now does `recs.borrow().iter().map(..).max()` directly.
- aeron_live/pending.rs:162, :163 — `blocked: Vec<u32>` with `.contains()` changed to `HashSet<u32>`; the pop-front/rebuild loop changed to `pending.retain_mut(...)`, keeping FIFO order in one pass. Verified with the 9 `drain_pending_tests`, all pass before and after.
- testing.rs:154 (now `testing/fakes.rs`) `FakeSubscription::poll` — the `while delivered < limit` counting loop replaced with a slice bound (`remaining.len().min(fragment_limit)`) plus a `for` loop over the bounded sub-slice.
- obs/lib.rs:124 — `while attempt < bind_retries` changed to `for attempt in 1..=bind_retries`.

### Tests: R1 comments (crate-log.md, Tests section)

- offer_starvation.rs:7, :20, :128 — history and incident-story text replaced with the pinned invariant (a parked publish must not delay a live delivery).
- offer_connect_race.rs:4, :69 — removed-design comparisons replaced with the current rule (an offer waits for the subscriber).
- aeron_live_e2e.rs:1 — "new Send-friendly" reworded to drop "new."
- aeron_live/pending.rs:225 (`#[cfg(test)]` comment) — reworded from naming the past bug to stating the invariant.
- offer_retry.rs:124 — deleted with `offer_with_deadline` (see Defect 8, dead-code cascade).
- config/tests.rs:238 — "used to wrap" reworded to state the constructor rejects a negative base.
- obs/tests/init_port_in_use.rs:7 — "original ... now" framing reworded to state the current contract.

### Tests: R3, R6 (crate-log.md, Tests section)

- Both "None found" per the appendix; no change needed.

### Defects (crate-log.md summary / narrative)

- Defect 8 (four dead modules: `publisher.rs`, `subscriber.rs`, `supervisor.rs`, `replay.rs`) — deleted. Cascade: `offer_retry.rs`'s `offer_with_deadline()`, `SPIN_ATTEMPTS`, and 5 tests were only called from `publisher.rs`; deleted as transitively dead. The production retry path (`aeron_live/pending.rs`) is unaffected and is covered by the 9 `drain_pending_tests` plus `offer_starvation.rs`/`offer_connect_race.rs`.
- Defect 14 (`subscriber.rs:4` wrong feature-gate comment) — deleted with the module.

### Pedantic sites (inputs-log.md, 311 rows)

All 311 rows are accounted for below, grouped by file. "DELETED" means the row's file no longer exists (dead module removed, see R8/Defect 8). "SPLIT" means the row's file (`testing.rs`) was split into `testing/{mod,fakes,cluster}.rs`; the specific lint at that line was fixed in the new location. "FIXED" means the file still exists and the lint was fixed in place. Every fix used `cargo clippy --fix --allow-no-vcs` for the mechanical lints (`doc_markdown`, `map_unwrap_or`, `needless_raw_string_hashes`, `uninlined_format_args`, `semicolon_if_nothing_returned` where safe, `must_use_candidate`) and a hand edit for lints that needed judgment (`missing_errors_doc`/`missing_panics_doc` prose, `cast_*`, `needless_pass_by_value`, `match_same_arms`, `single_match_else`, `used_underscore_binding`, `similar_names`, `unnecessary_wraps`, `unnecessary_debug_formatting`, `manual_let_else`, `explicit_iter_loop`, `unused_async`).

- `crates/log/src/aeron_live/handles/simple.rs` [18 rows, FIXED]: doc_markdown 1,2,87,97,103,113,119,129; missing_errors_doc 46,65,89,105,121,137,152; must_use_candidate 93,109,125.
- `crates/log/src/aeron_live/handles/tx_data.rs` [7 rows, FIXED]: doc_markdown 1,10,39; missing_errors_doc 17,30,49; must_use_candidate 34.
- `crates/log/src/aeron_live/handles/tx_receipts.rs` [21 rows, FIXED]: doc_markdown 1,97,203,340; missing_errors_doc 123,138,176,180,184,196,240,259,280,301,306,351,363,385,404,408; must_use_candidate 335.
- `crates/log/src/aeron_live/mod.rs` [1 row, FIXED]: doc_markdown 101.
- `crates/log/src/aeron_live/pending.rs` [4 rows, FIXED]: must_use_candidate 45; semicolon_if_nothing_returned 114,362; single_match_else 141.
- `crates/log/src/aeron_live/runtime.rs` [19 rows, FIXED]: missing_errors_doc 123,132,142,165,211,232,254,264,275,302,331,355,455,473; unnecessary_debug_formatting 146; doc_markdown 163,297,348; needless_pass_by_value 424.
- `crates/log/src/aeron_live/thread.rs` [10 rows, FIXED]: doc_markdown 32; needless_pass_by_value 65,66; match_same_arms 94,123,151; explicit_iter_loop 113; unnecessary_wraps 163; cast_possible_truncation 198,219.
- `crates/log/src/codec.rs` [4 rows, FIXED]: missing_errors_doc 21,31,42; doc_markdown 58.
- `crates/log/src/config/mod.rs` [39 rows, FIXED]: missing_errors_doc 39,55,276; doc_markdown 83,95,107,115,124,132,133,135,139,146,154,201,207,214,225,235,236,240,248,254,336,341,347,353; must_use_candidate 255,263,302,329,337,342,348,354; cast_sign_loss 306,333 (both `#[allow]`ed with a comment: the value is already range-checked by `validate`, so the cast is provably safe); cast_lossless 338,355.
- `crates/log/src/config/tests.rs` [1 row, FIXED]: needless_raw_string_hashes 55.
- `crates/log/src/lib.rs` [2 rows, FIXED]: doc_markdown 2,6.
- `crates/log/src/offer_retry.rs` [2 rows, FIXED]: cast_possible_truncation 91 (`#[allow]`ed: nanos capped at `u64::MAX` on overflow); semicolon_if_nothing_returned 173.
- `crates/log/src/publisher.rs` [25 rows, DELETED]: doc_markdown 3,5,7,13,79,82,87,115,116,118,128,131,148,154,193; missing_errors_doc 94,119,138,150,155,165,176,186; must_use_candidate 111; cast_possible_wrap 236.
- `crates/log/src/recorder.rs` [31 rows, FIXED]: doc_markdown 1,6,177,179,181,199,200,204,277,396,398,399,405,525,528; missing_errors_doc 53,101,113,227,289,319; unnecessary_debug_formatting 59,123 (`.display()` used for `Path` in format args); must_use_candidate 91,188,462; used_underscore_binding 92 (`_aeron` renamed to `aeron_client`); cast_possible_truncation 154; manual_let_else 367; unnecessary_wraps 411 (`find_or_start_recording` now returns `Option<i64>`); semicolon_if_nothing_returned 435.
- `crates/log/src/refetch.rs` [18 rows, FIXED]: doc_markdown 1,7,8,69,71,85,128,224,492; must_use_candidate 116; missing_errors_doc 133,228; missing_panics_doc 133,228; cast_lossless 394,402,409; similar_names 442 (`res` renamed to `list_result`).
- `crates/log/src/replay.rs` [12 rows, DELETED]: doc_markdown 92,222,549,579; missing_errors_doc 134,223,555,581; too_many_lines 241; manual_let_else 259,365; single_match_else 365.
- `crates/log/src/subscriber.rs` [16 rows, DELETED]: missing_errors_doc 46,142,150,158,166,174,182,193,201; cast_sign_loss 81,99; doc_markdown 115,118,121,140,190.
- `crates/log/src/supervisor.rs` [8 rows, DELETED]: must_use_candidate 27; missing_errors_doc 37; unused_async 68,88; similar_names 69,89; unnecessary_debug_formatting 119; explicit_iter_loop 131.
- `crates/log/src/testing.rs` [57 rows, SPLIT into testing/{mod,fakes,cluster}.rs]: doc_markdown 41,42,76,251,266,267,307,314,316,317,318,362,363,379,385,399; must_use_candidate 51,71,77,121,124,132,135,138,173,201,271,280,296,302,324,371,407,446,455,610,615,619,623; missing_panics_doc 77,152,450,455,579,593; cast_possible_wrap 81,103; cast_possible_truncation 127; missing_errors_doc 181,309,381,387,544,566,630; map_unwrap_or 549,656.
- `crates/log/src/watermark.rs` [1 row, DELETED]: doc_markdown 6.
- `crates/log/tests/aeron_live_e2e.rs` [3 rows, FIXED]: map_unwrap_or 30; cast_possible_truncation 88,89 (fixed via an extracted `let fill = i as u8;` with `#[allow]`).
- `crates/log/tests/offer_connect_race.rs` [2 rows, FIXED]: doc_markdown 14; map_unwrap_or 34.
- `crates/log/tests/offer_starvation.rs` [2 rows, FIXED]: map_unwrap_or 53; similar_names 102 (`#[allow]`ed with a reason).
- `crates/log/tests/testing_fakes.rs` [4 rows, FIXED]: similar_names 161,162,201,202 (`#[allow]`ed on `b_reader_joins_against_a_buffer_in_canonical_order`, with a reason).
- `crates/obs/src/lib.rs` [1 row, FIXED]: missing_errors_doc 64.
- `crates/obs/tests/init.rs` [2 rows, FIXED]: doc_markdown 3; uninlined_format_args 23.
- `crates/obs/tests/init_without_runtime.rs` [1 row, FIXED]: doc_markdown 12.

`must_use_candidate`: `#[must_use]` was added at every flagged site (about 53 total). Each is a constructor, getter, or pure computation, where discarding the return value has no effect and is always a caller bug. None warranted an `#[allow]`.

### Mechanical rows (inputs-log.md, "Mechanical rows for this group")

- `run_replay_merge` (replay.rs:241, 117 lines) — deleted with the module.
- `fetch_tx_data` (refetch.rs:133, 74 lines) — split (see R2 above).
- `resolve_recording` (replay.rs:441, 71 lines) — deleted with the module.
- `handle_cmd` (aeron_live/thread.rs:163, 68 lines) — split (see R2 above).
- `init` (obs/lib.rs:64, 65 lines) — split (see R2 above).
- `crates/log/src/aeron_live/handles/mod.rs:7,8,9` (`pub mod simple;` / `pub mod tx_data;` / `pub mod tx_receipts;`) — changed to `pub(super) mod`. `handles` itself is a private `mod` in `aeron_live/mod.rs`, which already re-exports every item via `pub use handles::{simple,tx_data,tx_receipts}::{...}`; the sub-modules only need to be visible to their parent, not fully `pub`.
- File-level pedantic totals (`testing.rs` 57, `config/mod.rs` 39, `recorder.rs` 31, `publisher.rs` 25, `tx_receipts.rs` 21, `runtime.rs` 19, `refetch.rs` 18, `simple.rs` 18) — these are aggregate counts of the same rows listed individually above; no separate action needed beyond the per-line fixes already recorded.

## Deferred to Phase B

- R6 refetch.rs:138 `fetch_tx_data`'s `sink: &mut dyn FnMut(TxDataLoc, TxEnvelope)`, refetch.rs:232 `fetch_deposits`'s `sink: &mut dyn FnMut(BPosition, Deposit)` — `crates/engine/src/bin_support.rs`'s `JoinRecovery` trait takes `&mut dyn FnMut` matching this shape; changing to `impl FnMut` would ripple into that trait signature, outside this group.
- R6 testing.rs:544, :566, :630, :655, :701 (now `testing/cluster.rs`, `AeronTestCluster`'s Docker methods) — `Result<_, Box<dyn std::error::Error>>` to `anyhow::Result<_>` needs `anyhow` added as a dependency of `crates/log`. It is already a workspace dependency (`anyhow = "1"` in the root `Cargo.toml`) but is not yet listed in `crates/log/Cargo.toml`. **Dependency needed: add `anyhow.workspace = true` to `crates/log/Cargo.toml`.**
- R6 aeron_live/mod.rs:103 `DeliverFn = Box<dyn FnMut(&[u8], BPosition, i32) + Send>` — explicitly out of scope per the brief (the enum replacement changes `cluster-adapter`, which is Phase B).
- R9 publisher.rs:53 `ChannelUri` newtype for the 17-site NUL-byte check — the check's underlying signatures (`open_publication(&str)`, `open_subscription_with_deliver(&str)`, and equivalents) are called from `crates/executor`, `crates/cluster-adapter`, and `e2e`. A newtype would change those call sites' argument types, which is a Phase B signature change.
- R9 obs/lib.rs:71 `host_id.is_empty()` check, proposed `HostId::new(&str) -> Result<HostId>` parsed at the CLI boundary — `init(&str, ...)` is called by every binary in the workspace (all services that call `kardamom_obs::init`); changing its parameter type is a signature change outside this group.
- Defect 10 (unbounded message channels, R5 "bound it" suggestions at aeron_live/runtime.rs:170,319,343,360 and aeron_live/handles/tx_receipts.rs:241,286; the replay.rs:142 instance was deleted with its module, see Done) — deferred per the brief. The consumers on the other end of each channel set the drain rate; bounding needs a coordinated backpressure decision with those consumers, most of which live in other groups.

## Not done, judged wrong

- R8 testing.rs:441 `FakeFsyncWatermarkStream` — kept `pub`, not moved behind `#[cfg(test)]` or deleted as the appendix suggests. `crates/log/tests/testing_fakes.rs` is an integration test in `tests/`, which needs the item public; `#[cfg(test)]` does not apply there. Moved into `testing/fakes.rs` with the rest of the R3 split, still public.
- R2 refetch.rs:413 `list_recordings` and recorder.rs:535 `active_recording_for_stream`, and R10 refetch.rs:439 and recorder.rs:592 (the shared `page_catalog` helper suggestion) — not unified into one helper. The two loops use different accumulators (`refetch.rs` collects into a `Vec` behind `Rc<RefCell<_>>` for a one-shot resolve; `recorder.rs` runs on a 500 ms poll tick with handler-release timing comments tied to the archive session's lifecycle). Forcing a shared helper would either lose the poll-tick behavior in `recorder.rs` or add a mode flag that makes the "shared" helper harder to read than the two originals. Each loop's own imperative-style items (R10 refetch.rs:454, listed under Done) were still fixed individually. **Reversed in Phase C under R14**: both loops now call the shared `AeronArchive::for_each_recording_of_stream` (in `archive_catalog.rs`), which owns only the paging and the `Handler::leak`/`release()` pair; each call site still supplies its own accumulator closure, so the differing-accumulator objection above no longer applies.
- R2 recorder.rs `connect_archive_with_timeout` (49 lines, marked "borderline" in the appendix) — left as one function. It is under the 50-line R2 threshold the appendix itself flags as borderline, and its three suggested helpers (`archive_context`, `control_channels`, `message_timeout_ns`) would each be called from exactly one place.
- R2 aeron_live/thread.rs:64 `run_aeron_thread`'s `drain_commands`/`wait_next` split — not done beyond the `poll_subscriptions` extraction already made. Splitting `drain_commands` out would move the `Shutdown`/`Disconnected` early-return across a helper-function boundary, and the early-return's exact point (before or after the current iteration's `poll_subscriptions` call) is part of the shutdown-latency contract; keeping it inline preserves the existing control flow with certainty. **Reversed in Phase C under R15/R16**: `run_aeron_thread` is now `AeronThread::run`, and the command-drain loop is `AeronThread::drain_commands(&mut self) -> ControlFlow<(), bool>`. `Break(())` is returned at the exact point the old inline `return Ok(())` fired, so the shutdown-latency contract is unchanged; the concern above (moving the early return across a function boundary) is resolved by `ControlFlow` carrying the return decision back to the caller instead of the callee returning early itself.
- R6 aeron_live/handles/tx_receipts.rs:264 (`ReceiptsTransport` newtype spanning `ch.tx_receipts_mds_enabled()` at 264, 281, 364, 390) — not unified. The four checks sit at four distinct public entry points (`open_auto` twice, `open_mds` twice, each a separate constructor for a separate handle type), so there is no single call site to parse `ChannelsConfig` into `ReceiptsTransport` once without changing those four signatures. **Reversed in Phase C under R14**: solved without touching the four signatures. A private `MdsSubscriber` trait (`tx_receipts.rs`) gives `open`/`open_mds`/`endpoint_of`/`mds` as required items and a default `open_auto` body; `TxReceiptsSubscriberHandle` and `TxReceiptsBoundarySubscriberHandle` implement it, and their public `open`/`open_mds`/`open_auto` methods become one-line calls into the trait. `require_mds` dedups the repeated MDS-off guard.
- R9 config/mod.rs `BasePort` newtype (rows at :276, :306, :333) — not built. An eager `TryFrom<i32>` on the base port would reject a negative base even when MDS is disabled, where today's code never reads that value. That is an observable behavior change for MDS-disabled configs with a garbage (but unused) base-port field, which the brief's behavior-preservation rule forbids introducing silently. **Reversed in Phase C under R13, per the coordinator's explicit instruction**: this is now the intended behavior. `tx_receipts_endpoint_base_port` is `Option<BasePort>` (`BasePort` wraps `NonZeroU16`); `None` (the default) means MDS-off, same as before, but an explicit `0` or negative TOML value is now rejected at parse time, MDS on or off. See "Intended behavior changes" in the Phase C section below.
- R9 refetch.rs `TermLayout` newtype (rows at :395, :403) — not built. Both checks run inside a rusteron FFI callback (`AeronArchiveRecordingDescriptorConsumerFuncCallback`), which has no `Result` return path; restructuring the checks into a fallible constructor would need the callback boundary to change, which risks altering how descriptor errors surface. **Reversed in Phase C under R13**: `raw_position` (and its constructor) is a plain function, not the FFI callback itself; `TermLayout` is built once per recording inside `list_recordings`'s callback (the callback still returns `()`, but a `Result<TermLayout, String>` value is stored on `FoundRecording`, not propagated through the callback), and `FoundRecording::raw_position` re-raises that stored result on each call, matching the previous per-call check's failure mode exactly.
- R10 aeron_live/handles/tx_receipts.rs:228 (`for` loop to `.into_iter().for_each(...)`) — reverted to a plain `for` loop. The functional rewrite triggers clippy pedantic's `needless_for_each`, which the mandatory pedantic gate rejects; the gate takes precedence over this row's suggested style.
- R10 obs/tests/init_without_runtime.rs:34 (`let mut last_err = None; for _ in 0..5 { ... }` to `(0..5).find_map(...)`) — not rewritten. The loop body calls `kardamom_obs::init(...).await`; stable Rust has no async closures usable with `find_map`, so the suggested functional form does not translate directly without extra machinery that would obscure the retry, not simplify it.
- R10 (Tests) offer_starvation.rs:115 and offer_connect_race.rs:99 (shared `poll_until` helper via a new `tests/common/mod.rs`) — not built. Both files are gated on the `docker-e2e` feature and require a real Aeron Media Driver; per the brief, these tests were never run this session, so a structural change spanning two integration-test binaries could not be verified end-to-end. Left as two separate inline loops to avoid an unverified behavior change.
- Mechanical rows, test-function `too_many_lines` (inputs-log.md): `b_reader_joins_against_a_buffer_in_canonical_order` (testing_fakes.rs:151, 73 lines), `back_pressured_publish_does_not_starve_a_live_subscription` (offer_starvation.rs:77, 71 lines), `init_and_scrape_on_current_thread_runtime` (init_without_runtime.rs:21, 57 lines), `aeron_live_send_friendly_round_trip` (aeron_live_e2e.rs:40, 56 lines) — not split. The audit's own R2 table (source only) and Tests R3 table ("None found") do not name test functions as R2 targets; these four are single-scenario integration/unit tests where the setup, action, and assertion read as one sequential story, and `offer_starvation.rs`/`aeron_live_e2e.rs` are docker-e2e-gated so a split could not be verified by running them this session.

## Phase C

Two coordinator-requested fixes, three reversed Phase A rows (annotated in
place above), and R13/R12/R14/R15/R16 from `arith-log.md` and `dry-log.md`
(read-only inputs in the main workspace, not copied here).

### Coordinator fixes (done)

- `refetch.rs`: `prepare_replay` now logs `stream_id`, `session_id`, and
  `from_raw` itself on the "nothing recorded at/after" path, since the
  caller no longer has `from_raw` to log once the field moved into
  `prepare_replay`'s signature.
- `config/tests.rs::quorum_section_is_rejected`: comment rewritten to
  present tense ("`[quorum]` is not a config section...").

### R13 non-zero types (7 rows)

**Done:**
- `config/mod.rs:173` `tx_receipts_endpoint_base_port: i32` → `Option<BasePort>` (`BasePort` wraps `NonZeroU16`). Rejects an explicit `0` or negative TOML value at parse time, MDS on or off (an intended behavior change, see below). `validate` keeps only the cross-field bound (`base + 2*count + 1 <= 65535`), which the type cannot carry alone. Two new tests: `non_mds_base_port_zero_is_rejected`, `non_mds_base_port_absent_loads`.
- `refetch.rs:395` archive term length → `TermLayout` (`crates/log/src/term_layout.rs`, shared with the R12 `decode_position` fix below). Built once per recording in `list_recordings`'s catalog callback; `FoundRecording::raw_position` re-raises the stored `Result` on each call instead of re-checking.
- `testing.rs:566` (now `testing/cluster.rs`) `multi_node(n: usize)` → `multi_node(n: NonZeroUsize)`. No caller anywhere in the workspace (dead code today), so the signature change is free.

**Deferred to Phase B:**
- `config/mod.rs:199` `tx_receipts_executor_count: u32` → `Option<NonZeroU32>`, and `tx_receipts.rs:33`'s `executor_count == 0` check that depends on it. Grepped: `crates/ingress/src/bin/kardamom-ingress/main.rs:240`, `crates/sequencer/src/bin/kardamom-sequencer/main.rs:281`, and `crates/validator/src/bin/kardamom-validator/pumps.rs:151` all read `channels.tx_receipts_executor_count` as a raw `u32` field (`.unwrap_or(channels.tx_receipts_executor_count)`), not through a method. Retyping the field breaks those three call sites.

**Not applicable (struct deleted in Phase A):**
- `config/mod.rs:363` `pub n: usize` and `config/mod.rs:365` `pub q: usize` — these were `QuorumConfig`'s fields. `QuorumConfig` and the `quorum` field were deleted in Phase A (R8, no reader outside a test). There is nothing left to retype.

### R12 safe arithmetic (43 rows: 16 FIX, 3 HOT_PATH_KEEP, 24 PROVEN)

**FIX rows — Done:**
- `aeron_live/thread.rs:312,313` (`decode_position`'s `term_id`/`term_offset` shift) — fixed via `TermLayout`. `PubEntry` (new, in `pending.rs`) pairs each open publication with a `TermLayout` read once from `AeronPublication::get_constants()` (`TermLayout::from_publication`) at open time (`AeronThread::cmd_open_publication`). `drain_pending`'s offer closure decodes a successful offer's raw position through `entry.layout.decode(code)`, which derives the shift from the publication's real term length and adds `initial_term_id` — both missing from the old fixed `>> 32` decode. **Traced where the decoded `BPosition` flows**: through `OfferResult::Delivered(Result<BPosition, LogError>)` to the pending publish's ack channel, which every `PubHandle::publish`/`publish_bytes` caller reads. This value is never compared against a subscriber-side `header_loc` position anywhere in this workspace (grepped); it is logged and returned to the publish caller only. So the fix is a pure correctness change to a value callers see but do not cross-check — not a behavior change observable as a test failure anywhere in this group or its dependents. One new unit test in `term_layout.rs` (`decode_inverts_position_of`) round-trips `decode(position_of(pos))` for several `(term_length, initial_term_id)` pairs, including a nonzero `initial_term_id` and a position past the first term — the only verification this fix gets without a real Media Driver.
- `publisher.rs:242,243,236` — deleted with the module (Phase A).
- `config/mod.rs:306,333` (`tx_receipts_endpoint_base_port as u32 + ...`) — fixed via `ChannelsConfig::tx_receipts_slot(replica_idx, slot)` (new private method), using `checked_add` end to end and returning `None` on overflow instead of an unsigned wrap. Folded into the R14 dedup (`tx_receipts_endpoint`/`tx_receipts_boundary_endpoint` both call it).
- `config/mod.rs:338,355` (`tx_data_stream_id_base + sequencer_id as i32`, `fsync_watermark_tx_data_stream_id_base + ...`) — `validate` gained two range checks (`base <= i32::MAX - 255`), so `tx_data_stream_id`/`fsync_watermark_tx_data_stream_id` use `saturating_add` as defense-in-depth for a directly built `ChannelsConfig` that skips `validate` (a loaded config never saturates there). The return type stays `i32` — `tx_data_stream_id` is called by `crates/ingress` and `crates/engine`.
- `aeron_live/handles/tx_receipts.rs:126,161,355,371` (`ch.tx_receipts_stream_id + 1`) — replaced by `ChannelsConfig::tx_receipts_boundary_stream_id()` (new method, `saturating_add(1)`), with `validate` gaining a `tx_receipts_stream_id != i32::MAX` check.
- `subscriber.rs:81,99` — deleted with the module (Phase A).
- `refetch.rs:314` (`self.next_endpoint + attempt`) — `wrapping_add`, matching how `next_endpoint` itself already advances (`rotate`, and the same function's own fallback path).

**HOT_PATH_KEEP rows — kept as is, no change:** `offer_retry.rs:82` (`attempt += 1`), `refetch.rs:199,279` (`delivered += 1`, now inside `ArchiveRefetcher::drain`, same bound).

**PROVEN rows — kept as is, no change (verdict re-verified against the current code, since several moved file or line during Phase A/C restructuring):**
`config/mod.rs:279`-equivalent (the `highest` overflow check, now inside `validate`, same `i64::from` widening before the add); `refetch.rs:409,402,394,395` (now inside `TermLayout::position_of`/`TermLayout::new`, same widening/power-of-two-first-check reasoning); `refetch.rs:385` (`replay_bounds`, unchanged); `refetch.rs:458`-equivalent and `recorder.rs:593`-equivalent (both now the shared `archive_catalog::for_each_recording_of_stream`'s `max_id = max_id.map_or(id, |cur| cur.max(id))`, same `i64` catalog-id bound); `replay.rs:505,460` — deleted with the module; `recorder.rs:154` (`connect_timeout` nanos, unchanged); `offer_retry.rs:91` (`elapsed_ms`, unchanged); `obs/lib.rs:126`-equivalent (now `Exporter::build_with_retry`'s `for attempt in 1..=bind_retries`, same loop-bound bound); `aeron_live/thread.rs:198,219`-equivalent (now inside `AeronThread::cmd_open_publication`/`cmd_open_subscription`, same `(len - 1) as u32` bound); `aeron_live/pending.rs:65,66,115` (unchanged); `refetch.rs:552` (now inside `ArchiveRefetcher`'s private `recv_timeout`, unchanged); `thread.rs:180,188`-equivalent (`ADD_SUB_TIMEOUT` in `add_sub_destination`; `OFFER_TIMEOUT` in `enqueue_publish`), `refetch.rs:498,543`-equivalent (`CONNECT_TIMEOUT`, `DRAIN_CAP`), `supervisor.rs:112` — deleted with the module; `testing.rs:81,103,127,128`-equivalent (now `testing/fakes.rs`, unchanged fake-stream-offset arithmetic).

### R14 production code (12 rows)

**Done:**
- Row 1 (`publisher.rs`/`subscriber.rs`, a second transport implementation) — deleted in Phase A (R8/Defect 8), before this row's line numbers were ever current.
- Row 2 (archive catalog paging, 3 copies) — `AeronArchive::for_each_recording_of_stream` (extension trait `ArchiveCatalog`, `crates/log/src/archive_catalog.rs`, since `AeronArchive` is a foreign type and the orphan rule blocks an inherent `impl`). `refetch.rs::list_recordings` and `recorder.rs::active_recording_for_stream` (now `Recorder::active_recording_for_stream`) both call it; the third copy (`replay.rs`) was deleted in Phase A.
- Row 3 (rkyv trait-bound block x4 in `runtime.rs`) — already done in Phase A via `codec::WireMessage` (the row's proposed `TypedMsg` name, same shape).
- Row 4 (`dir_cstring`) — `crate::ffi::dir_cstring(&Path) -> Result<CString, LogError>` (new `crates/log/src/ffi.rs`). Used by `recorder.rs::connect_archive_with_timeout` and `aeron_live/runtime.rs::spawn_with_dir`.
- Row 5 (`c_uri`, 11 copies) — `crate::ffi::c_uri(uri: &str, what: &str) -> Result<CString, LogError>`. Used by `recorder.rs` (control request/response channels, `start_inner`'s stream channel), `aeron_live/thread.rs` (`open_pub`, `open_sub`, `add_sub_destination`), and `refetch.rs::start_bounded_replay`. One wording for all (dry-log's own recommendation); the exact NUL-error text now differs slightly from some of the 11 originals — this is the sanctioned tradeoff, not an oversight. This is the deferred R9 `ChannelUri` row's *body* without the type change (the row itself stays deferred: `c_uri` takes `&str`, not a `ChannelUri` newtype, so no public signature changes).
- Row 6 (`declare_channel_handles!` for `tx_data.rs`) — the macro gained a second arm (`subscriber $name(item = $ty, subscribe = $path);`) for a subscriber whose item type is not `(BPosition, $msg)` and whose open call is not the generic `open_subscription::<$msg>`. `tx_data.rs` now declares both handles through the macro; `TxDataPublisherHandle`/`TxDataSubscriberHandle`'s public methods are unchanged (verified: `cargo check --workspace --all-targets` compiles executor, ingress, engine, and e2e, the four consumers, with no changes on their side).
- Row 7 (`connect_client`) — deleted in Phase A (R8, no caller).
- Row 8 (`wait_for_cmd`, the two `recv_timeout` idle branches) — `AeronThread::wait_for_cmd(&mut self, wait: Duration) -> ControlFlow<(), bool>` (`Continue(true)` = a command was handled, `Continue(false)` = timed out, `Break(())` = stop). This deviates from dry-log's suggested `Result<bool, LogError>` signature: the busy branch needs to distinguish "handled" from "timed out" (only "handled" re-triggers `backoff.reset()`), which a bare `Result<bool,_>` cannot carry without also losing the stop signal's distinctness from a normal return. `ControlFlow<(), bool>` carries both. This also settles the Phase A `drain_commands`/`wait_next` "judged wrong" row (see the annotation on that row above).
- Row 9 (`tx_receipts_endpoint`/`tx_receipts_boundary_endpoint`, `+0`/`+1`) — `ChannelsConfig::tx_receipts_slot(replica_idx, slot)`, folded into the R12 `checked_add` fix above.
- Row 12 (`header_loc`, `thread.rs` vs `replay.rs`) — KEEP, per dry-log's own verdict (differing header types, no shared trait); `replay.rs`'s copy is gone (module deleted), so only one copy remains regardless.

**Deferred to Phase B:**
- Row 8, `MdsSubscriber` (originally listed as a judged-wrong row in Phase A, now solved instead — see the reversal note above; not deferred, moving this note here would be wrong. Struck.)
- Row 11 (`fetch_tx_data`/`fetch_deposits` shared `sub_uri` builder) — this row says KEEP (differs in policy: session-keyed vs rotate-all). No action needed; already the audit's own verdict.

**Not done, judged wrong:**
- None beyond what is listed above (rows 1, 3, 7, 11, 12 needed no action beyond confirming the audit's own KEEP/already-done verdicts).

### R14 tests (11 rows)

**Done:**
- Row (obs `free_port`/scrape, 3 copies across `init.rs`, `init_port_in_use.rs`, `init_without_runtime.rs`) — `crates/obs/tests/common/mod.rs` with `pub fn free_port() -> SocketAddr` and `pub async fn scrape(addr, budget: Duration) -> String`. Unified on the raw-`TcpStream`-on-a-blocking-task shape (`init_without_runtime.rs`'s original, chosen "on purpose" per its own module doc, to depend on nothing but the exporter's listener) for all three files, not the `reqwest` shape `init.rs` used before. This let `reqwest` come out of `crates/obs/Cargo.toml`'s `[dev-dependencies]` entirely (confirmed unused: `cargo check -p kardamom-obs --all-targets` compiles without it). All three tests still pass, including the two that spawn a real exporter (`init_exposes_build_info_and_service_up`, `init_retries_addr_in_use_then_fails_or_recovers`).
- Row (`tx_ref`/`TxRef` literal, 8 sites) — `crates/log/tests/common/mod.rs`, new (not `kardamom_log::testing`: `alloy-primitives`/`bytes` are dev-dependencies only, deliberately excluded from the library per `crates/log/Cargo.toml`'s own comment, so these could not live in `testing/fakes.rs` as dry-log suggested without adding them as real dependencies — a call I did not make). `pub fn tx_ref(shard_id: u8, pos: BPosition) -> TxRef` and `pub fn tx_envelope(correlation_id: u64, fill: u8) -> TxEnvelope`, used in `testing_fakes.rs` (6 sites) and `codec_roundtrip.rs` (2 sites) — the two files this session could run to verify. All 8 tests across both files still pass.
- Row (`FakeSubscription`/`FakeTypedSubscription`/`FakeTxDataSubscription`/`FakeTxOrderingSubscription` decode-then-callback body) — `FakeSubscription::poll_decoded<T, F>(&mut self, f: F, fragment_limit) -> usize` (new, `testing/fakes.rs`), decoding via `T: crate::codec::WireMessage` and calling `f(decoded, position, session_id)`. The three typed `poll` methods are now one-line adapters reordering/dropping arguments to their own signature (`FakeTxOrderingSubscription::poll` puts position first, the other two put the decoded value first — `poll_decoded` itself does not reorder, each caller does). `FakeHeader::position()` (new) replaces the `BPosition { term_id: header.term_id(), term_offset: header.term_offset() }` literal repeated 4 times. All `testing_fakes.rs` tests still pass.

**Deferred to Phase B (docker-e2e, not run this session):**
- Row (`docker_available()`, 3 copies, plus the `require_docker` wrapper) — not built. Same reasoning as the Phase A `poll_until` row: `offer_starvation.rs`, `offer_connect_race.rs`, and `aeron_live_e2e.rs` are all `docker-e2e`-gated and were never run this session (per the brief). A shared `pub async fn require_docker()` in `testing/cluster.rs` is straightforward to write and would compile-check clean, but its correctness (does the assert message still read right, does every caller's surrounding context still make sense) cannot be verified without running these three tests against a real Docker daemon.
- Row (`single_node_runtime` bring-up, 3 copies) — same reasoning; not built.
- Row (`recv_within` deadline-receive loop, 4 copies) — same reasoning; not built. This is the Phase A `poll_until` row under a different name; same disposition, same reason.
- Row (`env`/`TxEnvelope` literal in `offer_starvation.rs`, `aeron_live_e2e.rs`, `offer_connect_race.rs`) — not unified with `crates/log/tests/common::tx_envelope`. `offer_starvation.rs`'s local `env()` uses a 48-byte `raw_tx`; `tx_envelope` (sized for `testing_fakes.rs`, the file this session could verify) uses 32 bytes. Unifying would change the payload length in a docker-gated test this session cannot run to confirm is inconsequential.
- Row (config `tests.rs` `load`/`mds_toml`) — **Done**, not deferred (this row's file is `crates/log/src/config/tests.rs`, not docker-gated; listed here only because dry-log's table groups it under "R14 tests"). `fn load(toml: &str) -> Result<LogConfig, LogError>` and `fn mds_toml(base_port: &str, count: u32) -> String`, both new in `config/tests.rs`. Used to convert `mds_nonpositive_base_port_rejected`, `mds_base_port_overflowing_u16_rejected`, `mds_valid_base_port_accepted`, plus four more single-line `write_tmp`+`from_toml_path` pairs, to one-line `load(...)` calls. All 23 config tests still pass.
- Row (`FakeConcurrentPublication`/`FakeSubscription` struct-literal constructors `bus.stream(channel, stream_id)`, 6 sites) — not built. Given the remaining session budget, this row was not reached; the constructors already exist as inherent `open` methods on the four fake pub/sub types (`FakePublication::open`, `FakeTypedSubscription::open`, `FakeTxDataPublication::open`, `FakeTxDataSubscription::open`, etc.), so the duplication dry-log flags is the six struct-literal *bodies inside those existing `open` methods*, not a missing constructor; a `FakeConcurrentPublication::new`/`FakeSubscription::new` factored out from those bodies is small but was not done.

### R15 methods, not standalone functions

**Named targets — Done:**
- `aeron_live/thread.rs`: `enqueue_publish`, `cmd_open_publication`, `cmd_open_subscription`, `cmd_remove_destination`, `poll_subscriptions`, `handle_cmd` are now methods on a new `AeronThread` struct (`aeron: Rc<AeronClient>, cmd_rx, pubs: Vec<PubEntry>, subs, pending, dests, backoff`). `open_pub`, `open_sub`, `add_sub_destination` are methods too (not explicitly named by the coordinator, but the same shape: a loose function taking the thread's state as its first parameters). `run_aeron_thread` (the old free-function loop body) is now a two-line adapter: `AeronThread::new(aeron, cmd_rx).run()`. `decode_position` (loose function) is gone; decoding is `TermLayout::decode`, a method, called from `drain_pending`'s offer closure.
- `obs/src/lib.rs`: `retry_settings`, `build_exporter` (renamed `build`), `build_with_retry`, `register_build_gauges` are now methods on a new `Exporter` struct (`service: &'static str, metrics_addr: SocketAddr, host_id: String`). `init` keeps its public signature and builds an `Exporter` internally.
- `refetch.rs`: `prepare_replay` was already a method (Phase A). `resolve_session_recording` → `FoundRecording::resolve_session` (associated function: no natural `&self`, it picks one recording out of a `Vec`). `raw_position` → `FoundRecording::raw_position(&self, pos)`. `drain` → `ArchiveRefetcher::drain` (associated function: generic over the channel item type, does not touch `ArchiveRefetcher` state, but lives in its `impl` block rather than at module scope). `list_recordings` and `start_bounded_replay` also moved into `impl ArchiveRefetcher` as associated functions, for the same reason.
- The catalog page helper (coordinator's reversal #5) → `AeronArchive::for_each_recording_of_stream`, an extension-trait method (`ArchiveCatalog`), not `ArchiveSession::for_each_recording_of_stream` as the coordinator's message first suggested. `ArchiveSession` (`recorder.rs`) is not always available at the two call sites: `recorder.rs::active_recording_for_stream` only has `archive: &Archive` (the `Recorder` type stores just the archive, not the whole session plus its `aeron_client`), so a method on `ArchiveSession` would not reach that call site without changing `Recorder`'s ownership shape (out of scope; `Recorder::start_stream`/`record_stream_until_stopped` are both `pub fn`, so that would be a Phase B signature change). `refetch.rs::list_recordings` has `session: &ArchiveSession` and calls `session.archive.for_each_recording_of_stream(...)`.

**Judged wrong (no target renamed to fit):**
- `fetch_descriptor` (`recorder.rs`) stayed a private free function inside `impl Recorder` (an associated function, not `&self`) rather than a full method — it takes `archive: &Archive` and `recording_id: i64`, neither of which is `Recorder` state at the call site (`start_inner` has a local `archive: Archive` not yet wrapped in `Self`). Moving it into the `impl Recorder` block (done) satisfies "attached to a struct via impl"; making it take `&self` would not, since `Self` does not exist yet when it is called.

### R16 no nested loops

Swept every `for`/`while`/`loop {` in `crates/log/src` and `crates/obs/src`
(20 loop constructs total) with a brace-depth script: for each loop-start
line, check whether another loop-start line appears before its own closing
brace. **Result: zero nested loops found**, in the current (post-R15)
code. The one nest that existed before this session — `run_aeron_thread`'s
command-drain `loop { match cmd_rx.try_recv() ... }` inside the outer poll
`loop`, in the pre-Phase-C `thread.rs` — is gone: `drain_commands` is now
a separate method (see R15), called once per outer-loop iteration, so
there is no longer a loop nested inside another loop's lexical body.

### Prior-audit items (dry-log.md's own table)

All four "done" items (`obs::bin`, `AeronRuntime::spawn`, `open_tx_receipts`
MDS helper, the recorder-thread-plus-ready-barrier helper) are unchanged
and still true. The two "open" items are now done, both under R14/R15
above: "Archive catalog paging triplicated" → `AeronArchive::for_each_recording_of_stream`;
"`with_leaked_handler` RAII guard (4 hand-rolled sites)" → 2 of the 4 sites
(`recorder.rs::active_recording_for_stream`'s old body, `refetch.rs::list_recordings`'s
old body) now go through the shared helper's one `Handler::leak`/`release()`
pair; the third (`replay.rs`) was deleted in Phase A; the fourth
(`recorder.rs::fetch_descriptor`, a single-descriptor `list_recording`
call, not a paging loop) is a different shape and still hand-rolls its own
`Handler::leak`/`release()` — this was in scope for R14's paging row but
not this fourth, non-paging site, which dry-log's own row list does not
name. The "partly" item (`declare_channel_handles!` macro) is now "done":
`tx_data.rs` uses it (R14 row 6 above); no other hand-written pair
remains outside the macro in this group.

### Intended behavior changes (Phase C)

- **`tx_receipts_endpoint_base_port` rejects an explicit `0` or negative value even when MDS is off.** Per the coordinator's explicit instruction (R13 reversal). Before: a non-MDS config with a garbage base port loaded silently. After: `LogConfig::from_toml_path` returns `LogError::Config`. Two new tests pin both directions (`non_mds_base_port_zero_is_rejected`, `non_mds_base_port_absent_loads`).
- **`tx_receipts_executor_count` still accepts `0`.** Not changed (deferred, see R13 above) — noted here only to be explicit that this sibling field's behavior is unchanged, in case a reader expects it to have moved in lockstep with the base port.
- **`decode_position` (now `TermLayout::decode`) derives the term shift from the publication's real term length and adds `initial_term_id`, instead of a fixed 32-bit shift with `initial_term_id` omitted.** This is a correctness fix (R12), not a wire-format or public-API change: the decoded `BPosition` is returned to the publish caller through the existing ack channel and is not compared against any subscriber-side position in this workspace (grepped). A caller relying on the old (wrong) decoded value for anything beyond logging would see a different number now; no such caller exists in this workspace today.
- **A delivered publish whose raw position overflows `i32` after the term shift now acks an error instead of an `Ok` with a wrong position.** New in Phase C (`pending.rs`'s `OfferResult::Delivered(Result<BPosition, LogError>)`). Bound: at a 16 MiB term (`bits = 24`), this needs a position past 8 exbibytes on one Aeron stream — not a path any real deployment reaches, but now detected rather than silently wrapped.
- **`aeron_live/handles/mod.rs`'s three sub-modules are `pub(super)`, not `pub`.** Carried over from Phase A; re-confirmed still correct after the `declare_channel_handles!` macro change (the macro's re-export, `pub(crate) use declare_channel_handles;`, is unaffected by the modules' own visibility).

### Gates

1. `cargo clippy -p kardamom-log -p kardamom-obs --all-targets -- -D warnings`: pass.
2. `cargo clippy -p kardamom-log -p kardamom-obs --all-targets --features testing,docker-e2e -- -W clippy::pedantic`: zero warnings in `crates/log`/`crates/obs` (remaining pedantic warnings in the workspace are in `crates/types`, owned by another group).
3. `cargo test -p kardamom-log --features testing -p kardamom-obs`: pass (35 + 3 + 5 unit/integration tests in `kardamom-log`, 6 in `kardamom-obs`, including the two obs tests that spawn a real exporter). Docker-gated tests (`aeron_live_e2e.rs`, `offer_connect_race.rs`, `offer_starvation.rs`) not run.
4. `cargo fmt -p kardamom-log -p kardamom-obs`: applied.
5. `cargo check --workspace --all-targets`: pass (new gate for Phase C, since `declare_channel_handles!`'s macro change and the config method additions are consumed outside this group). `cargo check -p e2e --all-targets --features full-pipeline-e2e`: pass. No guest crate depends on `kardamom-log`/`kardamom-obs` (re-confirmed).

## Phase C, review round 2

Nine fix items from the coordinator's review, applied and re-verified
(all 5 gates re-run clean after each). Three items previously accepted
as-is stay unchanged (the decode fix, `BasePort`, the four
56-73-line test functions, `fetch_descriptor`, `FoundRecording.term_layout`).

1. **`refetch.rs` `ensure_session`/`ensure_runtime` return references.**
   `ensure_session(&mut self, endpoints) -> Result<&ArchiveSession, LogError>`;
   `ensure_runtime(&mut self) -> Result<&AeronRuntime, LogError>`. The
   `# Panics` sections on `fetch_tx_data`/`fetch_deposits` are gone, and
   so are their two `expect("ensured above")` calls each. Getting there
   needed more than a signature change: a `&mut self` method that
   returns a borrowed reference conservatively locks the *whole* `self`
   for that reference's lifetime (not just the field it touches), so the
   old code's pattern — hold the subscription's `&mut` entry from
   `self.tx_data_subs` alive across a later borrow of `self.session` —
   no longer compiles once that later borrow goes through a method call
   instead of a direct field access. Both `fetch_tx_data` and
   `fetch_deposits` now: (a) ensure the local subscription is open
   *before* touching `session` (subscription-before-replay order is a
   correctness requirement — Aeron drops frames published before a
   subscription's image forms — not just a borrow-checker convenience,
   so this order was never negotiable); (b) get `session` and call
   `start_bounded_replay`; (c) re-fetch the subscription with
   `self.tx_data_subs.get_mut(&key)` for draining. Step (c) cannot reuse
   the `Entry` API's single lookup from step (a) (that borrow is long
   dead by step (c), and re-borrowing across the `session`-borrowing
   step in between is exactly the conflict being removed), so it revisits
   the map a second time. Rather than `.expect()` a key that "must" still
   be there, this returns `LogError::Aeron(...)` instead — no unwrap, no
   new `# Panics` section, and a logic bug degrades to a returned error
   instead of a panic. `ensure_runtime`/`ensure_session` still have one
   `.expect()` each internally (`self.rt.as_ref().expect("the branch
   above just set this to Some")`), which is provably safe one line
   above and does not need a doc section for the same reason
   `Vec::get(0)` right after `vec.push(x)` would not.
2. **`refetch.rs` `ReplayPlan` struct.** Replaces `prepare_replay`'s
   `Option<(i64, i64, String)>` return and `start_bounded_replay`'s
   `(replay_endpoint: &str, from_raw: i64, len: i64)` trailing
   parameters with `struct ReplayPlan { from_raw, len, endpoint }` and
   `start_bounded_replay(session, rec, stream_id, plan: &ReplayPlan)`.
3. **`thread.rs` `Destination` struct.** Replaces the
   `(u32, String, AeronAsyncDestination)` tuple with named fields
   `sub_id`, `uri`, `handle` (the last carrying an `#[allow(dead_code)]`
   plus an RAII comment, matching `recorder.rs`'s `_archive` field
   precedent — the field's only purpose is its `Drop` impl).
4. **`thread.rs` publication/subscription table index.** `(self.pubs.len()
   - 1) as u32` and its `subs` twin (each under a
   `#[allow(clippy::cast_possible_truncation)]`) are now
   `u32::try_from(self.pubs.len() - 1).map_err(|_| LogError::Aeron(...))`,
   propagated through `cmd_open_publication`/`cmd_open_subscription`'s
   existing `Result` return. No allow, no silent truncation.
5. **`config/mod.rs` `tx_receipts_slot`.** `2 * replica_idx` is now
   `replica_idx.checked_mul(2)`, chained with the existing
   `checked_add`/`checked_add`/`try_from` steps.
6. **`recorder.rs` `connect_archive_with_timeout`.** The 1 s floor moved
   from the nanosecond domain (`u64::try_from(nanos).unwrap_or(u64::MAX).max(1e9)`,
   which silently saturated to `u64::MAX` on overflow) to the `Duration`
   domain (`connect_timeout.max(Duration::from_secs(1))`), converted to
   nanoseconds once. A `try_from` failure now maps to
   `LogError::Aeron(...)` instead of silently saturating.
7. **`archive_catalog.rs` the `None => break` arm.** Rewritten as
   `let Some(id) = max_id else { break };`, with a comment stating the
   actual invariant (a page that reaches `count == PAGE`, `PAGE > 0`,
   always delivered at least one descriptor) instead of the word
   "defensive".
8. **`testing/fakes.rs` casts and constructors.** The three
   `#[allow(clippy::cast_possible_wrap)]`/`cast_possible_truncation]`
   sites are now `i64::try_from(bytes.len()).expect("fixture payload
   fits i64")` and `i32::try_from(off / TERM_LEN).expect(...)` (with a
   `# Panics` section added to `FakeHeader::from_offset_session`, since
   unlike the `refetch.rs` case above, there is no error-returning
   alternative for an infallible-by-convention test-fixture builder).
   `FakeConcurrentPublication::new(bus, channel, stream_id)` and
   `FakeSubscription::new(bus, channel, stream_id)` replace the six
   `Type { state: bus.stream(channel, stream_id), .. }` struct literals
   across `FakePublication::open`, `FakeTypedSubscription::open`,
   `FakeTxDataPublication::open_with_session`,
   `FakeTxDataSubscription::open`, `FakeTxOrderingPublication::open`,
   `FakeTxOrderingSubscription::open`.
9. **Docker-gated test dedups, done and run.** `testing/cluster.rs`
   gained `AeronTestCluster::single_node_runtime(stream_id_base) ->
   (Self, AeronRuntime, LogConfig)` (bring-up: cluster, `aeron_dir_host`,
   config, runtime), `require_docker()` (the `docker info` assert), and
   `recv_within(sub, budget, want) -> Option<TxEnvelope>` (deadline
   receive with a 50 ms per-attempt timeout), all re-exported from
   `kardamom_log::testing`. `crates/log/tests/common::tx_envelope` gained
   a `raw_len: usize` parameter (dry-log's suggested alternative to two
   builders) so `offer_starvation.rs`/`offer_connect_race.rs` keep their
   48-byte payload and `aeron_live_e2e.rs` keeps its 64-byte payload —
   neither changed. `aeron_live_e2e.rs`'s own drain-up-to-50-messages
   loop does not fit `recv_within`'s single-match shape (it collects
   every message, not the first one matching a predicate), so it stays
   inline; `recv_within` applies cleanly to `offer_connect_race.rs` (one
   call) and `offer_starvation.rs` (two calls: the warm-up wait and the
   final starvation-check wait). All three `docker-e2e` tests were run
   for real this round (Docker was available on this host):
   `cargo test -p kardamom-log --features docker-e2e --test aeron_live_e2e -- --ignored`,
   `--test offer_connect_race -- --ignored`, `--test offer_starvation --
   --ignored` — all three `ok`, confirming the shared-helper refactor
   preserves behavior, not just compiles.

Gates re-run after all nine items: clippy `-D warnings` clean, pedantic
clean (0 warnings in `crates/log`/`crates/obs`), `cargo test -p
kardamom-log --features testing -p kardamom-obs` all pass, `cargo fmt`
applied, `cargo check --workspace --all-targets` and `cargo check -p e2e
--features full-pipeline-e2e` both clean. Plus, new this round: the three
`docker-e2e` `--ignored` tests actually run and passing (see item 9).

## Round 3

Nine more small fixes from the coordinator's third review: `Destination`'s
`handle` field renamed `_handle` (RAII-only convention, no `#[allow]`
needed); `thread.rs`'s publication/subscription table id is now computed
with `u32::try_from` before the `push`, not after; `archive_catalog.rs`'s
`from_record_id = id + 1` is `checked_add(1)` with a `LogError`;
`refetch.rs` merged `session: Option<ArchiveSession>` and
`connected: Option<String>` (which could disagree) into one
`live: Option<Live { endpoint, session }>`, with `ensure_runtime` using
`match &mut self.rt { Some(rt) => rt, slot @ None => slot.insert(spawn()?) }`
(no `expect`) and `reconnect` now returning `Result<&ArchiveSession, LogError>`
directly through `self.live.insert(..)` (`ensure_session` keeps one
`expect`, on the already-connected fast path only, where the borrow
checker genuinely forces it — the reason is in the string); `refetch.rs`'s
`tx_data_subs`/`deposit_subs` "vanished" `ok_or_else` checks (which could
never actually fail) are gone, replaced by a remove-before/insert-after
pattern (`HashMap::remove` returns an owned value, so it does not hold a
borrow across the `ensure_session` call the way `entry`/`get_mut` would);
two copies of that small shape, one per map, since the value types and
open calls differ (R14's "otherwise fine" case); `testing/cluster.rs`'s
`single_node_runtime` returns a named `SingleNodeRig { cluster, rt, cfg }`
instead of a 3-tuple; `recv_within`'s `Instant::now() + budget` is now
`checked_add` with an `expect` (and a `# Panics` section, newly needed);
`testing/fakes.rs`'s `next_offset += len` is `checked_add` with an
`expect`; `tests/aeron_live_e2e.rs`'s last remaining bare
`#[allow(clippy::cast_possible_truncation)]` cast is `u8::try_from(i)`.

Gates re-run: `cargo clippy -p kardamom-log -p kardamom-obs --all-targets
--features testing,docker-e2e -- -D warnings` clean; the same with `-W
clippy::pedantic` clean (0 warnings in `crates/log`/`crates/obs`);
`cargo test -p kardamom-log --features testing -p kardamom-obs` all
pass; `cargo fmt -p kardamom-log -p kardamom-obs -- --check` clean;
`cargo check --workspace --all-targets` clean. `cargo check -p
kardamom-log --features docker-e2e --all-targets` also compiles clean
(the `SingleNodeRig` change touches all three docker-e2e test files).

## Round B (group C: da_watcher, interop-feed, cluster-adapter, cluster-client, sequencer, log, ingress, obs)

Item 4 of this round's brief: the four deferred R6/R9/R13/R14 rows Phase A/C left for Phase B
because their fix crosses into other crates.

- **`DeliverFn` (`aeron_live/mod.rs`)**: **removed entirely** — no `Box<dyn` remains anywhere
  in `crates/log` or `crates/cluster-adapter`. R6 has no wiring-seam exemption, so the earlier
  "kept as the one erasure point, documented" answer in this section was reversed once that
  was made explicit; the two designs offered (a generic subscription handle, or a typed
  channel the Aeron thread sends into) are both implemented, split by what each consumer needs:
  - A new `RawFrame { bytes: Vec<u8>, pos: BPosition, session: i32 }` (`pub`, in
    `aeron_live/mod.rs`) is the one concrete, non-generic payload that crosses
    `RuntimeCmd::OpenSubscription` to the dedicated Aeron thread. Decoding into a caller's own
    message type (over any `crate::codec::WireMessage` a downstream crate defines — the set
    stays genuinely open, proven by `kardamom-validator`'s own `BalFrame` instantiation) moves
    off the Aeron thread entirely, onto the consumer, decoded lazily on `recv`/`try_recv`.
  - `FrameSink` (`pub(super)`, in `aeron_live/mod.rs`) is a plain two-variant enum — `Tokio` or
    `Crossbeam` — naming which of the two channel kinds this crate's subscriptions ever need,
    not a trait object: `Tokio` for every async consumer in `crates/log` itself; `Crossbeam`
    for `kardamom-cluster-adapter`'s session thread, a plain OS thread that waits on this
    receiver alongside another crossbeam channel via `crossbeam_channel::Select` (tokio's
    channels do not implement `Select`'s `SelectHandle` trait, so that consumer could not use
    the `Tokio` variant). This set is closed by this module's own design, unlike `T` — two
    delivery mechanisms, not two message types.
  - `AeronRuntime::open_subscription_raw`/`open_subscription_raw_crossbeam` replace
    `open_subscription_with_deliver`, returning a `RawFrame` receiver of the matching kind
    instead of taking a closure. `TypedSubscription<T>` (`open_subscription`,
    `open_subscription_merged`, `open_subscription_with_id`) and the new `TxDataSubscription`
    (`open_tx_data_subscription`) wrap the tokio receiver and decode on `recv`/`try_recv`; a
    malformed frame logs and is skipped, matching the old inline-decode closures' tolerance
    exactly (pinned by a new test, see below). `crates/cluster-adapter/src/live/mod.rs`'s
    `open_egress` uses the crossbeam variant directly, since its session thread needs the raw
    bytes on a `Select`-compatible receiver; `session_loop.rs`'s `frame_rx` field and
    `drain_egress`'s `self.driver.on_egress(&frame.bytes)` changed type accordingly, no
    behavior change (egress frames were always relayed verbatim, pos/session unused).
  - `TxReceiptsSubscriberHandle`'s batch fan-out (one raw frame → several `(BPosition,
    Receipt)` items) needed a small owned buffer instead of a stateless per-frame decode:
    `pending: VecDeque<(BPosition, Receipt)>`, refilled by `decode_receipt_batch` each time it
    empties. `into_receiver()` now returns a new `TxReceiptsReceiver` (same `recv`/`try_recv`,
    no `MdsSub`/`AeronRuntime` — the whole point of `into_receiver`) instead of the raw tokio
    type; `crates/ingress/src/aeron_adapters.rs` (owned by this group) gained one
    `impl_pump_source!(TxReceiptsReceiver, Receipt)` line for it.
  - `refetch.rs`'s `drain`/`recv_timeout` (a from-scratch `poll_recv`-driven timeout, run from
    a plain thread with no tokio runtime entered) could no longer operate on a raw
    `UnboundedReceiver<T>` directly, since `TypedSubscription`/`TxDataSubscription` decode
    lazily instead of storing decoded items in the channel. New `PollRecv` trait (`pub`, one
    `poll_recv` method) implemented by both, so `drain`/`recv_timeout` stay generic — the same
    manual `Waker`-driven design, unchanged behavior, now over the trait instead of the
    concrete tokio type.
  - Two new tests pin the decode-on-read behavior no Docker-gated test reaches:
    `runtime::tests::typed_subscription_skips_a_malformed_frame_and_decodes_the_next` and
    `tx_receipts::tests::one_batch_frame_fans_out_to_every_receipt_in_order`. Neither needs an
    `AeronRuntime` — both build a bare `unbounded_channel` and send `RawFrame`s by hand.
- **`HostId` (`lib.rs`)**: `pub struct HostId(String)` with `impl FromStr` (rejects empty),
  `AsRef<str>`, `Display`. `init(host_id: &str, ..)` is unchanged — its own `is_empty` check
  stays, since binaries this group does not own still pass a raw `&str`. The three binaries
  this group owns (`da_watcher`, `sequencer`, `ingress`) changed their `--host-id` clap field
  from `String` to `kardamom_obs::HostId` (clap derives the parser from `FromStr`), so an
  empty host id now fails argument parsing instead of reaching `init`. Every other binary's
  `kardamom_obs::init(...)`/`init_service!(...)` call needs no change.
- **`tx_receipts_executor_count` (`config/mod.rs`)**: `u32` → `Option<NonZeroU32>`; `0` is now
  `None` ("no known executor count", MDS still enabled with a warning), any positive count is
  `Some`. `Default` sets `None`. `validate()`'s `2 * i64::from(self.tx_receipts_executor_count)`
  is now `.map_or(0, std::num::NonZeroU32::get)` first. A `0` in a config file now fails to
  parse instead of loading as "no executor count"; `config/tests.rs`'s
  `mds_executor_count_zero_is_rejected` pins the new rejection. Callers this group owns:
  `crates/ingress/src/bin/kardamom-ingress/main.rs` and
  `crates/sequencer/src/bin/kardamom-sequencer/main.rs`, both changed from `.unwrap_or(..)` to
  `.or(channels.tx_receipts_executor_count.map(NonZeroU32::get)).unwrap_or(0)`. Not owned by
  this group: `crates/validator/src/bin/kardamom-validator/pumps.rs:163` — already updated to
  the new type by the time this round checked it (another agent's concurrent edit).
- **Refetch sinks (`refetch.rs`)**: `fetch_tx_data`'s and `fetch_deposits`'s
  `sink: &mut dyn FnMut(..)` are now `sink: impl FnMut(..)` (owned, not `&mut dyn`).
  `crates/engine/src/bin_support.rs`'s `ArchiveJoinRecovery` (the only caller, owned by
  another group) needed **no change**: `&mut dyn FnMut(A, B)` already implements `FnMut(A,
  B)` via std's blanket impl, so its existing `sink: &mut dyn FnMut(..)` parameter satisfies
  the new `impl FnMut(..)` bound as-is. Confirmed with `cargo check -p kardamom-engine
  --all-features`: clean, unchanged.

### Other findings fixed while in these files

- Pre-existing gate-blocker: `crates/log/tests/common/mod.rs`'s `tx_envelope`/`tx_ref` were
  `pub` with no external consumer (the module is `mod common;`-included per test file, never
  a library item); narrowed to `pub(crate)`.
- `redundant_closure_for_method_calls` (pedantic): `config/mod.rs`'s `.map_or(0, |n|
  n.get())` (introduced by the `tx_receipts_executor_count` change above) is
  `.map_or(0, std::num::NonZeroU32::get)`.

### R16, second pass (no single-line guard exemption)

The coordinator's second R16 message named four specific sites by shape; all four are fixed
here (the fifth and sixth, in `da_watcher`, are reported in `status-batcher.md`'s section):

- `refetch.rs::fetch_deposits`'s `if len <= 0 { continue; }` inside its `for rec in recs`
  loop: the loop is split in two. A first pass builds `plans: Vec<(FoundRecording,
  ReplayPlan)>` (still a `for`, but with no branch in its body — only the fallible
  `replay_bounds` call, propagated with `?`, which still ends the whole function on a real
  archive error). The second pass iterates `plans.into_iter().filter(|(_, plan)| plan.len >
  0)`, so the length check is gone from inside a loop body entirely. The loop's other
  branches (the subscription-reuse `if let`/`else`, the `if replay_result.is_ok() {..} else
  {0}`, the `if let Err(e) = replay_result { return }`) are unchanged — the coordinator's
  message named only the length check, and pulling the resource lifecycle logic (a
  subscription must go back into `deposit_subs` on every path) into an iterator chain would
  need a much larger rewrite than what was asked.
- `crates/obs/src/testkit.rs::scrape`'s `for _ in 0..40 { if let Ok(r) = .. && .. { return }
  sleep().await; }`: the `if` moves into a new `try_scrape(url) -> ControlFlow<String>`
  helper; the loop is now `for _ in 0..40 { match try_scrape(url).await { Break(body) =>
  return body, Continue(()) => sleep().await } }`.

- `cargo clippy -p kardamom-log --all-targets --all-features -- -D warnings -W
  clippy::pedantic -D unreachable_pub`: clean.
- `cargo test -p kardamom-log --lib`: 38 pass (36 plus the two new decode-on-read tests
  above). Docker-gated tests
  (`docker-e2e`/`aeron_live_e2e.rs`, `offer_connect_race.rs`, `offer_starvation.rs`) not run,
  per the brief.
- `cargo fmt -p kardamom-log -- --check`: clean.
- Forbidden-pattern grep: prints nothing for `crates/log/src/**` outside `tests?/` — no
  `Box<dyn` remains at all now that `DeliverFn` is gone (see above). Except:
  `crates/log/src/testing/cluster.rs`'s five `Box<dyn std::error::Error>` return types match
  the grep literally, but that module is a Docker-test harness gated behind
  `docker-e2e`/`testing` (`crates/log/src/testing/mod.rs`'s own doc: "Gated behind
  `#[cfg(any(test, feature = \"testing\"))]`"); the grep's exclusion list only names
  `test_support`, not `testing/`, which looks like a gap in the pattern rather than a real
  production-code hit. Left unchanged; flagging here rather than silently passing it.
- **obs (`crates/obs/src/**`)**: `HostId` above; also added `kardamom_obs::testkit`
  (`free_port`, `scrape`, feature `test-support`, `reqwest` optional dep) per item 6 — see
  that item's own report. `cargo clippy -p kardamom-obs --lib --all-features -- -D warnings -W
  clippy::pedantic -D unreachable_pub`: clean. `cargo clippy -p kardamom-obs --all-targets
  ...`: blocked by `crates/obs/tests/common/mod.rs`'s pre-existing `pub fn free_port`/`pub
  async fn scrape` (unreachable pub) — that file is under `crates/obs/tests/**`, outside this
  group's file list (`crates/obs/src/**` only), so left unfixed and reported here instead.

## Round B, second follow-up: behavior notes and a migration

- **`refetch.rs::fetch_deposits` now computes every replay plan before it
  replays any.** The loop used to compute one recording's bounds, replay
  it, then move to the next. It now builds `plans` for every recording
  first, then replays. A `replay_bounds` error on recording N now aborts
  the call before recording 1 is replayed, where the old code replayed
  and delivered recordings `1..N-1` first, then failed on N. This is
  correct: `fetch_deposits` is a best-effort recovery path
  (`ArchiveJoinRecovery`), its caller retries the whole call on error,
  and deposit volume is tiny, so re-replaying an already-delivered
  recording on retry is cheap and idempotent (the caller's join index
  dedups by position). No caller was found that depends on the old
  partial-progress delivery order.
- **Migration: `tx_receipts_executor_count = 0` in an existing
  `channels.toml` now fails to load.** The field is
  `Option<NonZeroU32>`; `0` in TOML is a parse error, where it used to
  parse as a literal `0` meaning "no known count." An operator config
  that spells the disabled case as `0` must change it to omit the key
  entirely (the default is `None`, which means the same thing). This is
  also documented on the field's doc comment in
  `crates/log/src/config/mod.rs`.
- **`ChannelUri`, a new non-empty-string newtype for the fixed Aeron
  channel fields** (`tx_ordering_channel`, `tx_receipts_channel`,
  `tx_errors_channel`, `tx_deposits_channel`, `tx_remote_epochs_channel`,
  `tx_bal_channel`, `quorum_watermark_channel`,
  `archive_control_request_channel`, `archive_control_response_channel`),
  added in `crates/log/src/config/mod.rs`. `Deref<Target = str>` means
  almost every existing `&cfg.some_channel` call site needed no change.
  `tx_receipts_control_channel` stays `String` (empty is its "MDS off"
  sentinel, not a URI); the `*_channel_template` fields stay `String`
  (not a URI until `{sid}`/`{rid}` is substituted).
  **Note for the coordinator**: an earlier Round B message deferred this
  item ("`ChannelUri` waits for the bench workspace merge"); the second
  follow-up's item 13 asked for it explicitly, so it is added here. If
  the deferral still stands, this needs reverting before the bench merge
  lands, since `crates/bench/**` reads these same fields (see the
  cross-file caller list below).
  One caller outside this group's files needs a matching change:
  `crates/validator/src/bin/kardamom-validator/pumps.rs:73`'s `BalPump`
  struct has a `bal_channel: String` field; `pumps.rs:50` now assigns it
  a `ChannelUri` (via `channels.tx_bal_channel.clone()`), which fails to
  compile until that field's type changes to `ChannelUri` (or the
  assignment adds `.to_string()`). `crates/executor/src/bin/kardamom-executor/main.rs:176`,
  `crates/e2e/benches/e2e_throughput.rs:53`, and
  `crates/e2e/src/harness/inject.rs:111,151,175` all continue to compile
  unchanged, through `Deref`/`From<&str>`.
- **`AssembledDeliver::handle_aeron_fragment_handler` (`aeron_live/thread.rs`)
  copies every fragment (`buffer.to_vec()`) on the Aeron polling thread,
  for every subscription.** This is the necessary cost of the
  decode-on-read design (`TypedSubscription`/`TxReceiptsReceiver`
  decoding off the Aeron thread, from an earlier round): the fragment
  assembler's buffer is borrowed and reused by rusteron on the next
  poll, and the channel this hands frames to needs an owned `Vec<u8>` to
  outlive that reuse. Removing the copy means decoding back on the Aeron
  thread, which reverses that design. Left as is; not measured under
  load in this round.

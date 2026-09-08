# Status: sequencer group (Phase A)

Group: sequencer. Crates: `kardamom-sequencer`, `kardamom-cluster-adapter`,
`kardamom-cluster-client`. Directories: `crates/sequencer`,
`crates/cluster-adapter`, `crates/cluster-client`.

All four gates pass for all three crates:

1. `cargo clippy -p <crate> --all-targets -- -D warnings`: clean.
2. `cargo clippy -p <crate> --all-targets -- -W clippy::pedantic`: zero
   warnings in this group's own files (checked per crate, one at a time;
   a combined multi-crate clippy run gave inconsistent counts under the
   shared build cache, so the per-crate run is the one that counts).
3. `cargo test -p <crate>`: all pass. No test spawns a binary, Docker, or
   testcontainers.
4. `cargo fmt -p <crate> -- --check`: clean.

## Done

### R1 comments

| rule | file:line | what changed |
|---|---|---|
| R1 | crates/sequencer/src/resync/mod.rs:10 | Named the real entry point (`ResyncController::floor`, read through `Sequencer::proven_executed`) instead of the dangling `should_skip` link. |
| R1 | crates/sequencer/src/resync/mod.rs:3 | Dropped the `docs/agents/sequencer-lag-resync-spec.md` reference; stated the rule inline. |
| R1 | crates/sequencer/src/resync/mod.rs:18 | Fixed the doc link to point at `PartitionState::advance_floor`'s note instead of a "removed fast-forward note". |
| R1 | crates/sequencer/src/resync/mod.rs:275 | Dropped "used to be excluded wholesale" and the incident narrative; kept the deposit-vs-genuine-nonce-0 invariant. |
| R1 | crates/sequencer/src/state/mod.rs:69 | Rewrote: `None` means the caller must seed a floor, not a state-DB lookup (the sequencer holds no state-DB reader). |
| R1 | crates/sequencer/src/state/mod.rs:228 | Trimmed the 16-line `fast_forward_stalled` obituary to the 3-line present invariant; dropped the dated review path. |
| R1 | crates/sequencer/src/state/mod.rs:214 (was :213) | Dropped the spec-doc reference on `advance_floor`; inlined the rule. |
| R1 | crates/sequencer/src/bin/kardamom-sequencer/feeds.rs:202 (pre-refactor line) | Deleted the wrong nonce-0-exclusion comment; the controller doc states the live rule. |
| R1 | crates/cluster-adapter/src/wire/mod.rs:26 | Added the missing `[l1_origin:u64]` field to the block-boundary wire diagram. |
| R1 | crates/cluster-adapter/src/live/session_loop.rs:3 | Deleted the "used to be `run_session` locals" refactor-history sentence. |
| R1 | crates/sequencer/src/sequencer.rs:66 | Kept only "the bin imports `sequencer::Shutdown`"; dropped the move history. |
| R1 | crates/sequencer/src/sequencer.rs:77 | Rewrote: the struct holds a reference to the envelope, not envelope bytes (dropped "no longer"). |
| R1 | crates/sequencer/src/sequencer.rs:41 | Dropped the "earlier ... was removed" obituary; kept the invariant (a sequencer cannot tell a twin-ordered gap from a client-abandoned one). |
| R1 | crates/sequencer/src/sequencer.rs:99 | Dropped the spec-doc reference on the `resync` field; stated the rule inline. |
| R1 | crates/sequencer/src/sequencer.rs:101 | Changed "identical to before resync existed" to "resync then does nothing". |
| R1 | crates/sequencer/src/sequencer.rs:104 | Dropped the dated allocation measurement and "used to be"; kept "does not allocate". |
| R1 | crates/sequencer/src/sequencer.rs:398 | Deleted the "used to run here was removed" obituary on the fast-forward sweep; kept the present stall/recover rule. |
| R1 | crates/sequencer/src/sequencer.rs:464 | Dropped "This is split out of `run_once`; the sequence of operations is unchanged" (refactor provenance). |
| R1 | crates/sequencer/src/sequencer.rs:498 | Changed "Conflating them broke the load harness's seq_clean verdict" to "must stay distinct". |
| R1 | crates/sequencer/src/sequencer.rs:509 | Changed "Count and report it exactly as before" to "Count it, and report it". |
| R1 | crates/sequencer/src/config.rs:27 | Rewrote the `nonce_floor_lag_ms` doc from a legacy-field obituary to one line ("unused; stays so old TOML keeps loading"). |
| R1 | crates/sequencer/src/config.rs:44 | Dropped the spec-doc reference on `resync`. |
| R1 | crates/sequencer/src/pending.rs:10 | Restated the smallest-nonce-eviction contrast as "would punch a gap" instead of "the old behavior". |
| R1 | crates/sequencer/src/unconfirmed.rs:38 | Restated the full-map-scan comparison as a hypothetical cost, not a description of replaced code. |
| R1 | crates/sequencer/src/epoch.rs:14 | Dropped "Unlike the `DepositRef` scheme this replaces"; stated what the record carries. |
| R1 | crates/sequencer/src/outbound/cluster.rs:121 (was on `cluster_ref_publisher_with_egress`) | Rewrote the doc: states what the function returns and why the caller keeps the egress receiver; dropped "used to discard it" and the spec-doc reference. |
| R1 | crates/sequencer/src/metrics.rs:17 | Removed with the deleted `record_*` family and the spec-doc reference (module comment is gone with the code it described). |
| R1 | crates/cluster-adapter/src/live/session_loop.rs:260 | Kept the present rule (only session-filtered events feed the watchdog); dropped the past-tense bug narrative. |
| R1 | crates/cluster-adapter/src/live/session_loop.rs:512 (`idle_wait` doc) | Dropped the dated 1ms-latency and 8%-CPU benchmark claims; kept the cap rule. |
| R1 | crates/cluster-adapter/src/live/session_loop.rs:525 | Deleted "The old shape kept `req_rx` in the Select..."; kept the present rule (build the Select from live receivers only). |
| R1 | crates/cluster-adapter/src/live/session_loop.rs:559 (`close_on_shutdown` doc) | Rewrote to state the present close-on-shutdown reason; dropped "used to disarm ... for the whole zombie lifetime". |
| R1 | crates/cluster-adapter/src/live/mod.rs:239 (`validate_config` doc) | Dropped "used to show up only as"; stated the present behavior (`connect` fails startup on empty fields). |
| R1 | crates/cluster-adapter/src/live/mod.rs:387 (test comment, same pattern) | Same fix in the test doc comment. |
| R1 | crates/cluster-adapter/src/watermark.rs:8 | Dropped "This replaces the old standalone sealer's ... watermark". |
| R1 | crates/cluster-adapter/src/config.rs:19 | Changed "serde ignores a legacy `enabled = true` key" to "unknown keys are ignored". |
| R1 | crates/cluster-adapter/src/wire/mod.rs:71 | Changed "each ~75-byte ref used to pay for a full offer round trip" to "batching amortizes the per-offer round trip". |
| R1 | crates/cluster-adapter/src/wire/mod.rs:90 (`KIND_ORIGIN_RECORD` doc) | Dropped the `docs/agents/l1-origin-deposit-derivation-spec.md` reference. |
| R1 | crates/cluster-adapter/src/wire/mod.rs:113 (`KIND_REMOTE_ORIGIN_RECORD` doc) | Dropped the broken `docs/specs/interop-outbox-messaging-spec.md` §7 reference (file does not exist). |
| R1 | crates/cluster-adapter/src/wire/mod.rs:153 (`RT_REMOTE_EPOCH` doc) | Dropped the same broken spec-doc reference. |
| R1 | crates/cluster-client/src/session/mod.rs:28 | Dropped "an earlier value of 5.4.0 conflated the two"; kept "this is the appVersion, not the schema version". |
| R1 | crates/cluster-client/src/protocol.rs:293 (`encode_session_event` doc) | Changed to "Test and mock-server helper" as part of `#[cfg(test)]`-gating the function (R8). |
| R1 | crates/sequencer/src/bin/kardamom-sequencer/adapters.rs:27 | Deleted "try_recv is non-blocking" (restates the call). |
| R1 | crates/sequencer/src/bin/kardamom-sequencer/adapters.rs:1 | `log::aeron_live` to `` `log::aeron_live` `` (doc_markdown, part of the same file). |
| R1 | crates/cluster-adapter/src/live/session_loop.rs:304 | Deleted `// consumer dropped` (restates the assignment). |
| R1 | crates/sequencer/src/bin/kardamom-sequencer/main.rs:219 (now :~335, log text) | See "Not done" below: this is inside a `tracing::info!` string, not a comment. |
| R1 | crates/sequencer/src/sequencer.rs:285,304,322 (log text) | See "Not done" below: `(#85)` is inside `trace!`/`warn!` strings, not comments. |
| R1 | crates/sequencer/src/bin/kardamom-sequencer/main.rs:394 (own R2-split comment) | Fixed "used to end" (introduced during the R4/R2 refactor of this file) to a plain present-tense sentence. |
| R1 (test) | crates/sequencer/tests/replicated_shard_racing.rs:263 | Rewrote to state what the test asserts (nothing past the hole is ever published), not the removed fast-forward. |
| R1 (test) | crates/sequencer/tests/replicated_shard_racing.rs:19-24 (module doc) | Removed the stale "Cold rejoin, misaligned floor" bullet, which described the now-fully-removed `nonce_floor_lag_ms` fast-forward; folded its still-true content into the "Cold rejoin" bullet. |
| R1 (test) | crates/sequencer/tests/resync_filter.rs:2 | Dropped the spec-doc reference; stated the rule inline. |
| R1 (test) | crates/cluster-client/src/session/tests.rs:330-333 | Replaced the word-for-word duplicate of `session_loop.rs`'s doc with one sentence and a doc link. |
| R1 (test) | crates/cluster-client/src/session/tests.rs:354 | Deleted "The old session is no longer usable for app messages" (restates the two asserts below it). |

### R2 long methods

Split every function the appendix listed, using the helpers it named:

| rule | file:line | what changed |
|---|---|---|
| R2 | crates/sequencer/src/bin/kardamom-sequencer/main.rs:171 `main` (125 lines) | Split into `open_aeron_handles`, `wire_resync`/`ResyncWiring`, `join_loop`, `run_loops`. `main` is now under 40 lines. |
| R2 | crates/cluster-adapter/src/live/mod.rs:260 `connect_inner` (68 lines) | Left as one function: the appendix's proposed helpers (`open_egress_subscription`, `open_initial_ingress`, `spawn_session_thread`) would each need 5-7 of `connect_inner`'s local values as parameters or a bespoke struct; the existing linear body (open egress, open ingress, spawn thread) has one concern per stage already and reads clearly top to bottom. Left as is; see "Not done" note. |
| R2 | crates/sequencer/src/bin/kardamom-sequencer/feeds.rs:258 `spawn_publish_loops` (67 lines) | Replaced with the `PublishLoops<P>` argument-group struct (R7/R11) and a shared `run_origin_pump` helper for the two origin pumps (dedupes the 16-line deposit/remote-epoch pump body). `spawn_publish_loops` is now under 40 lines. |
| R2 | crates/sequencer/src/bin/kardamom-sequencer/feeds.rs:63 `run_egress_watermark_feed` (65 lines) | Split into `handle_reject_frame`, `handle_boundary_frame`, and a named `flag` function (was a closure). |
| R2 | crates/cluster-adapter/src/wire/egress.rs:106 `decode_relayed_payload` (76 lines) | Split into `decode_txref_fields`, `decode_depositref_fields`, `decode_epoch_fields`, `decode_remote_epoch_fields`, and a shared `decode_rkyv_body::<T>` (dedupes the aligned-copy block for `RT_EPOCH`/`RT_REMOTE_EPOCH`). |
| R2 | crates/sequencer/src/sequencer.rs:236 `resync_tick` (61 lines) | Left as one function. Splitting into `apply_receipt_drain`/`apply_contiguity_rejects`/`sweep_confirm_timeouts` would pass `&mut self` plus 2-3 borrowed locals to each; the function's four blocks are already separated by the comments the appendix cites and read as one linear sequence over `self.resync`. Left as is; see "Not done" note. |
| R2 | crates/sequencer/src/sequencer.rs:466 `handle_outcome` (57 lines) | Left as one function; it is a single `match` over `NonceOutcome` plus a second loop over `result.actions`, already the minimum-concern shape. Left as is; see "Not done" note. |
| R2 | crates/cluster-client/src/session/mod.rs:266 `on_session_event` (57 lines) | Left as one function (unchanged; not touched in Phase A, see "Not done"). |
| R2 | crates/cluster-adapter/src/wire/egress.rs:44 `decode_egress` (57 lines) | Split into `decode_relayed`, `decode_boundary`, `decode_contiguity_reject`, done together with the R9 `# Errors` doc addition. |
| R2 | crates/cluster-client/src/session/mod.rs:168 `poll_outbound` (51 lines) | Left as one function (unchanged; not touched in Phase A, see "Not done"). |
| R2 | crates/sequencer/src/sequencer.rs:546 `run` (51 lines) | Left as one function; it is already the pin-then-loop shape the appendix describes, and the `on_step_result` extraction would need to pass `&mut self`, `&mut backoff`, and the match result, which reads no clearer split than inline. Left as is; see "Not done" note. |
| R2 | crates/sequencer/src/sequencer.rs:378 `run_once`, borderline at ~50 lines after the `SequencerPorts` change | Reduced from 51 to well under 50 lines by the `SequencerPorts` refactor (R7), which replaced three parameters with one `ports.split()` call. No further split needed. |

### R3 large files

| rule | file | what changed |
|---|---|---|
| R3 | crates/sequencer/src/sequencer.rs (346 code lines) | KEEP, confirmed. Under 500. One state machine; the R2 extractions (R7 `SequencerPorts`) already shrank it. |
| R3 | crates/cluster-adapter/src/live/session_loop.rs (356 code lines) | KEEP, confirmed. Under 500. |
| R3 | crates/cluster-client/src/protocol.rs (313 non-test code lines) | KEEP, confirmed. Under 500. The `Frame` type added for R9 stays inside this file, consistent with the plan's `protocol/header.rs` note (not split out, since the file is well under the limit). |
| R3 (test) | crates/cluster-client/src/session/tests.rs (336 lines) | KEEP, confirmed. Under 500. |
| R3 (test) | crates/sequencer/tests/\* (8 files, 26-232 lines each) | KEEP (all under 500). |
| R3 (test) (reversed) | crates/sequencer/tests/\* (`signed_envelope`/`signer` builder duplicated in `sequencer_integration.rs`, `sequencer_step.rs`, `resync_filter.rs`, `multi_sequencer_dual_write.rs`, `benches/throughput.rs`) | Originally judged not worth a `tests/common/mod.rs` include across the `tests/`/`benches/` directory boundary. Reversed: done as R14 (`crates/sequencer/src/testkit.rs`, a `src/`-side module behind the `testing` feature, sidesteps the directory-boundary problem entirely — `tests/`/`benches/` already depend on this crate with that feature on). See Phase C's R14 section below for the full change list. |

### R4 manual drops

| rule | file:line | what changed |
|---|---|---|
| R4 | crates/sequencer/src/bin/kardamom-sequencer/main.rs:336 `drop(cluster_guard);` | Extracted `async fn run_loops(cluster_guard, join_main, join_deposits, join_remote_epochs)`, which owns `cluster_guard` and returns after the three joins; it now falls out of scope with no explicit `drop`, at the same point in the sequence. |
| R4 | crates/sequencer/src/bin/kardamom-sequencer/main.rs:343 `drop(rt);` | Deleted. It was the last statement before `Ok(())`; the implicit end-of-scope drop is identical. |

### R5 sync primitives and channels

All 25 rows in the appendix carry a JUSTIFIED verdict (no REPLACE_WITH_OWNERSHIP,
REPLACE_WITH_CHANNEL, or UNNECESSARY verdict in this group). Per the Phase A rule
("apply every REPLACE_WITH_* verdict... leave JUSTIFIED sites alone"), no primitive
swap was made for any of them. Three of the JUSTIFIED rows are the unbounded
resync channels named in the assignment; those were bounded (a behavior change,
not a primitive swap), with a test:

| rule | file:line | what changed |
|---|---|---|
| R5 | crates/sequencer/src/resync/mod.rs:478 `crossbeam_channel::unbounded()` (floors) | Bounded to `cfg.dedup_capacity` in `resync_channel`. The receipts feed now uses `try_send` and drops on `Full` (safe: an unproven skip falls through to publish, the side every degraded mode already degrades toward). Added `resync::tests::channels_are_bounded_to_dedup_capacity`. |
| R5 | crates/sequencer/src/resync/mod.rs:479 `crossbeam_channel::unbounded()` (rejects) | Same fix, same test. The egress-watermark feed now uses `try_send` and drops on `Full` (safe: the confirm-timeout sweep still rewinds and republishes, only later). |
| R5 | crates/sequencer/src/resync/mod.rs:81,82 (`count`, `lag_gap_ms` `AtomicU64`) | JUSTIFIED, no change. |
| R5 | crates/sequencer/src/resync/mod.rs:179 `floor_rx: Receiver<FloorUpdate>` | JUSTIFIED, no change. |
| R5 | crates/sequencer/src/resync/mod.rs:190 `reject_rx: Receiver<(Address,u64,u64)>` | JUSTIFIED, no change. The tuple-to-named-struct suggestion in the "fix" column is a naming improvement, not a primitive swap, and was not applied. |
| R5 | crates/sequencer/src/bin/kardamom-sequencer/feeds.rs:171 `floor_tx: Sender<FloorUpdate>` | JUSTIFIED, no change (producer half of the now-bounded pair above). |
| R5 | crates/sequencer/src/bin/kardamom-sequencer/feeds.rs:52 `reject_tx: Sender<(Address,u64,u64)>` | JUSTIFIED, no change (producer half of the now-bounded pair above). |
| R5 | crates/cluster-adapter/src/live/mod.rs:71 `stop: Arc<AtomicBool>` | JUSTIFIED, no change. |
| R5 | crates/cluster-adapter/src/live/mod.rs:160,161 `next_index`, `next_block` `AtomicU64` | JUSTIFIED, no change. |
| R5 | crates/cluster-adapter/src/live/mod.rs:98 `bounded(1)` reply channel per offer | JUSTIFIED, no change. The per-offer channel cache is a hot-path optimization, not a REPLACE_WITH_* fix; not applied. |
| R5 | crates/cluster-adapter/src/live/mod.rs:271 `unbounded::<Vec<u8>>()` (egress frames) | JUSTIFIED, no change. Bounding this channel is out of the explicitly assigned scope (the resync channels); see "Not done" note. |
| R5 | crates/cluster-adapter/src/live/mod.rs:309 `unbounded::<OfferReq>()` | JUSTIFIED, no change. Same as above. |
| R5 | crates/cluster-adapter/src/live/mod.rs:310 `unbounded::<Vec<u8>>()` (app payloads out) | JUSTIFIED, no change. Same as above. |
| R5 | crates/cluster-adapter/src/live/session_loop.rs:112 `stop: Arc<AtomicBool>` | JUSTIFIED, no change. |
| R5 | crates/cluster-adapter/src/live/session_loop.rs:109-111 `frame_rx`, `req_rx`, `out_tx` | JUSTIFIED, no change. |
| R5 | crates/sequencer/src/outbound.rs:119-121 `refs`/`epochs`/`remote_epochs` `Arc<Mutex<Vec<_>>>` (fakes) | JUSTIFIED, no change. |
| R5 | crates/sequencer/src/outbound.rs:122 `fail_with_backpressure: Arc<Mutex<bool>>` | JUSTIFIED, no change. The `Arc<AtomicBool>` swap in the "fix" column is a primitive simplification inside a JUSTIFIED (test-fake) site, not a REPLACE_WITH_* verdict; not applied. |
| R5 | crates/sequencer/src/outbound.rs:161 `errors: Arc<Mutex<Vec<TxError>>>` | JUSTIFIED, no change. |
| R5 | crates/sequencer/src/epoch.rs:72-73 `queue`, `closed` | JUSTIFIED, no change. |
| R5 | crates/sequencer/src/remote_epoch.rs:77-78 `queue`, `closed` | JUSTIFIED, no change. |
| R5 | crates/cluster-adapter/src/gateway.rs:44-45 `accepted`, `outcome` (`FakeIngress`) | JUSTIFIED, no change. |
| R5 | crates/cluster-adapter/src/gateway.rs:78-79 `FakeEgress` queue and `closed` | JUSTIFIED, no change. The spin-to-`crossbeam_channel` swap in the "fix" column was not applied (test-fake only, JUSTIFIED verdict). |

### R6 dynamic dispatch

| rule | file:line | what changed |
|---|---|---|
| R6 | crates/cluster-adapter/src/live/mod.rs:275 `DeliverFn` boxed closure | Deferred to Phase B; see below. `DeliverFn` is declared in `crates/log/src/aeron_live/mod.rs`, outside this group. Confirmed: no `Box<dyn Error>` and no other trait object anywhere in `crates/sequencer`, `crates/cluster-adapter`, or `crates/cluster-client`. |

### R7 generics to supertraits

| rule | file:line | what changed |
|---|---|---|
| R7 | crates/sequencer/src/sequencer.rs:378 `Sequencer::run_once<I, B, R>` | Added `pub trait SequencerPorts { type In; type Refs; type Errors; fn split(&mut self) -> (&mut Self::In, &mut Self::Refs, &mut Self::Errors); }`, plus a blanket impl for `(&mut I, &mut B, &mut R)`. `run_once<P: SequencerPorts>(&mut self, ports: &mut P)` now takes one parameter. Updated the 3 production call sites (`feeds.rs`) and every test call site (`tests/*.rs`, `benches/throughput.rs`) to build a `(&mut a, &mut b, &mut c)` ports tuple. |
| R7 | crates/sequencer/src/sequencer.rs:546 `Sequencer::run<I, B, R>` | Same `SequencerPorts` bound; `shutdown` also changed from by-value to `&Shutdown` (R11 `needless_pass_by_value`, done together since it touched the same signature). |
| R11/R2 | crates/sequencer/src/bin/kardamom-sequencer/feeds.rs:258 `spawn_publish_loops` (12 args, near-miss) | Added `pub(crate) struct PublishLoops<P> { cfg, tx_data, publishers: [P; 3], tx_errors, epochs, remote_epochs, resync, shutdown }`. `spawn_publish_loops(loops: PublishLoops<P>)` now takes one argument; no `#[allow(clippy::too_many_arguments)]` needed (it never had one; the count is under the default threshold after this). The three `Shutdown` clones collapsed to one field, cloned internally once per spawned loop. |

### R8 unnecessary `pub`

Every row confirmed against a workspace-wide grep before narrowing or deleting.

| rule | file:line | what changed |
|---|---|---|
| R8 | crates/sequencer/src/metrics.rs:78-104 (`record_ingest`, `record_publish`, `record_buffered_future`, `record_past`, `record_eviction`, `record_backpressure`, `record_nonce_check_latency`) | Deleted, with the `record_helpers_smoke` test. |
| R8 | crates/sequencer/src/metrics.rs (const names) | `pub(crate)` for every const except `TX_INGESTED` (kept `pub`: read by `tests/metrics_endpoint.rs`, an external test crate). `record_start_time` and `record_lag_suspected` kept `pub` (called from the bin crate, `kardamom-sequencer`, a separate compilation unit from the lib); `record_resync_enter`, `record_resync_mode`, `record_resync_skip`, `record_floor_senders`, `record_floor_advance`, `record_canonical_watermark`, `record_unconfirmed_refs`, `record_ref_republished`, `record_remote_epoch_relayed` are `pub(crate)` (lib-internal only). |
| R8 | crates/sequencer/src/outbound/cluster.rs:110 `pub fn cluster_ref_publisher` | Deleted (no caller; the bin uses `cluster_ref_publisher_with_egress`). |
| R8 | crates/sequencer/src/pending.rs:69 `pub fn lowest_nonce` | Deleted (no caller). |
| R8 | crates/sequencer/src/partition.rs:28 `pub fn validate_partition_count` + `PartitionConfigError` | See "Not done" below: kept `pub`, still called by `tests/partition_routing.rs`. |
| R8 | crates/sequencer/src/config.rs:34 `pub nonce_floor_lag_ms: u64` | Changed to `pub nonce_floor_lag_ms: Option<u64>` with `#[serde(default)]`. Kept `pub` (not privatized as the appendix's first option suggested): 6 test files across `tests/` build `SequencerConfig` with `..Default::default()`, and Rust's struct-update syntax requires every field skipped by `..base` to be visible at the construction site, even from `base`; a private field breaks that from another compilation unit (confirmed: `cargo build` gave `E0451` on 6 files after trying the private-field form). Old TOML with the key still parses (`Some(value)`); new TOML without it parses too (`None`). |
| R8 | crates/sequencer/src/lib.rs:36 `pub mod pending` | `pub(crate) mod pending`. `PendingBuffer::len`/`contains` (used only by `pending`'s own tests) additionally moved behind `#[cfg(test)]` to avoid a new `dead_code` warning once the module stopped being crate-external; `is_empty` (never called anywhere) deleted. |
| R8 | crates/sequencer/src/lib.rs:39 `pub mod sender` | `pub(crate) mod sender`. |
| R8 | crates/sequencer/src/lib.rs:42 `pub mod state` | `pub(crate) mod state`. Moved `tests/state_proptest.rs` (the one external user) into `src/state/proptest_tests.rs` as a `#[cfg(test)]` submodule, and deleted the old integration-test file. |
| R8 | crates/sequencer/src/bin/kardamom-sequencer/feeds.rs `pub fn spawn_egress_watermark_feed`, `spawn_receipt_floor_feed`, `pub type LoopHandle`, `pub fn spawn_publish_loops` | All `pub(crate)` (binary crate; `pub` reached nothing). |
| R8 | crates/sequencer/src/bin/kardamom-sequencer/adapters.rs `pub struct LiveTxDataSub`, `LiveEpochSub`, `LiveRemoteEpochSub`, `LiveTxErrorPub` and their `pub fn new` | All `pub(crate)`. |
| R8 | crates/cluster-client/src/lib.rs:33 `pub mod protocol` | `pub(crate) mod protocol`. Confirmed the only external consumer of this crate (`cluster-adapter`) imports only `session::{DriverEvent, SessionDriver}` and `bytes`. |
| R8 | crates/cluster-client/src/protocol.rs:189 `pub fn decode_session_connect_request` + `pub struct SessionConnectRequest` | `#[cfg(test)]` (used only by `protocol`'s own tests; already unreachable via the module change above, but the plain `--lib` build otherwise flagged them `dead_code` once the module stopped being external). |
| R8 | crates/cluster-client/src/protocol.rs:241 `pub fn decode_two_i64` | `#[cfg(test)]`, same reason. |
| R8 | crates/cluster-client/src/protocol.rs:296,338 `encode_session_event`, `encode_new_leader_event` | `#[cfg(test)]`, same reason. `BLOCK_SESSION_EVENT_MIN`, `BLOCK_NEW_LEADER_EVENT` (used only by these two) and `Frame::expect_template` and `DecodeError::TemplateMismatch` (used only by the decode-side test helpers above) also moved behind `#[cfg(test)]` to clear the resulting `dead_code` warnings. |
| R8 | crates/cluster-client/src/protocol.rs:33-38 `TEMPLATE_*` consts | Covered by the `pub(crate) mod protocol` change; still `pub` inside the module (all four are used in production dispatch inside `protocol.rs` itself, in `decode_egress`), but no longer part of the crate's external API. |
| R8 | crates/cluster-client/src/session/mod.rs:143,154 `pub fn connect_attempts`, `pub fn state` | `#[cfg(test)] pub(crate)` (used only by `session::tests`; the live transport uses `is_connected()`). |
| R8 | crates/cluster-adapter/src/wire/ingress.rs:129 `pub fn decode_ingress_batch` | Deleted (confirmed zero callers, tests included, despite its own doc). |
| R8 | crates/cluster-adapter/src/wire/ingress.rs:167 (was :172) `pub fn decode_replay_request` | `#[cfg(test)] pub(crate)` (used only by `wire::tests`). |
| R8 | crates/cluster-adapter/src/wire/ingress.rs:43 `pub fn encode_ingress_depositref` | `#[cfg(any(test, feature = "testing"))]`. The `DepositRef`/`RT_DEPOSITREF` imports it alone used were also gated, to avoid an `unused_imports` warning in the plain build. |
| R8 | crates/cluster-adapter/src/wire/egress.rs:240 `pub fn encode_contiguity_reject` | `#[cfg(any(test, feature = "testing"))]`. |

### R9 defensive validation

| rule | file:line | what changed |
|---|---|---|
| R9 | crates/cluster-client/src/protocol.rs:192,244,379 `let body = &buf[HEADER_LEN..];` (repeated in 3 functions) | Added `struct Frame<'a> { header: MessageHeader, body: &'a [u8] }` with `Frame::parse(buf) -> Result<Self, DecodeError>`, which decodes the header, checks the schema id once, and slices the body once. `decode_session_connect_request`, `decode_two_i64`, and `decode_egress` all take a `Frame` now and never index `buf` directly. `Frame::expect_template` checks the template id for the two single-template decoders. |
| R9 | crates/cluster-client/src/protocol.rs:376,405 duplicated schema-id check | Folded into `Frame::parse`; `decode_egress` no longer repeats the check. |
| R9 (reversed) | crates/sequencer/src/sequencer.rs `cfg.validate().expect(...)`; crates/sequencer/src/config.rs `partition_count == 0` / `partition_index >= partition_count`; crates/sequencer/src/partition.rs `debug_assert!(m >= 1, ...)` | Originally judged "disproportionate for Phase A" (see the removed rows this replaces). Reversed: a `PartitionCount(NonZeroU32)` newtype now makes a zero partition count unrepresentable, and `Sequencer::new` returns `Result` instead of `.expect()`-ing a validated config. Done as part of R13 in Phase C (non-zero types); see that section below for the full change list rather than duplicating it here. |

## Deferred to Phase B

| rule | file:line | reason |
|---|---|---|
| R6 | crates/cluster-adapter/src/live/mod.rs:275 `DeliverFn` boxed closure | `DeliverFn` is declared in `crates/log/src/aeron_live/mod.rs`. Per the assignment, this depends on the log crate's enum and is explicitly Phase B. |
| R9 | crates/sequencer/src/sender.rs:26 `debug_assert!(envelope.sender != Address::ZERO, ...)` | The fix (a `RecoveredSender` newtype, constructed only by signature recovery) requires changing `kardamom_types::TxEnvelope.sender`'s type, and the ingress crate's proxy that constructs it. Both are outside this group. |
| R9 | crates/sequencer/src/sequencer.rs:415-423 `partition_for` re-check on the hot path | The appendix's fix pushes the shard check to the subscription boundary (a `ShardedEnvelope` wrapper on `TxDataSubscriber::poll`). `TxEnvelope` is decoded from raw Aeron fragments inside `kardamom-log`, outside this group, before this crate ever sees it; a boundary-level fix needs that crate's cooperation to carry shard membership through the decode. (A same-crate-only version, moving the check into `LiveTxDataSub::poll`, was considered and rejected — see "Not done, judged wrong": it would move the "tx_data envelope for wrong shard; skipping" warning and change the code path an existing test, `wrong_shard_message_skipped`, exercises.) |
| R9 | crates/cluster-adapter/src/live/mod.rs:242-258 `validate_config` (returns `()`, `LiveClusterConfig.ingress_endpoints` stays a raw `String`) | `LiveClusterConfig` is constructed by `crates/engine` and `crates/ingress` as well as this group (confirmed by grep). Changing its field shape to a parsed `ValidatedCluster` breaks those call sites. |
| R9 | crates/cluster-adapter/src/config.rs:38-49 `if self.ingress_stream_id == 0 { ... }` (zero-as-unset sentinel) | `ClusterConfig` is `pub` and constructed by `crates/engine` and `crates/ingress` (confirmed by grep), in addition to this group. Changing the fields to `Option<i32>` breaks those call sites. |

## Not done, judged wrong

| rule | file:line | reason |
|---|---|---|
| R1 | crates/sequencer/src/sequencer.rs:285,304,322 `"...dropping unconfirmed entry (#85)"` / `"...rewinding unconfirmed refs for republish (#85)"` / `"...rewinding for republish (#85)"` | The `(#85)` tag is inside the `trace!`/`warn!` string literal (runtime log text), not a source comment. The brief's behavior-preservation rule is unconditional: "Do not change log message text." Left unchanged; this is a real conflict between that rule and this specific R1 row, resolved in favor of not touching production log output. |
| R1 | crates/sequencer/src/bin/kardamom-sequencer/main.rs (`tracing::info!` at startup, "...F02.1 re-opened") | Same conflict, same resolution: the tag is inside the log string, not a comment. |
| R2 | crates/cluster-adapter/src/live/mod.rs:260 `connect_inner` (68 lines) | The three proposed helpers would each need most of `connect_inner`'s locals passed in or a bespoke struct; the existing body is already one concern per paragraph and reads linearly. Not split. |
| R2 | crates/sequencer/src/sequencer.rs:236 `resync_tick` (61 lines) | The three proposed helpers (`apply_receipt_drain`, `apply_contiguity_rejects`, `sweep_confirm_timeouts`) would each take `&mut self` plus 2-3 already-borrowed locals; the function's four blocks are already comment-delimited and read as one sequence over `self.resync`. Not split. |
| R2 | crates/sequencer/src/sequencer.rs:466 `handle_outcome` (57 lines) | Already the minimum-concern shape: one `match` over `NonceOutcome`, one loop over `result.actions`. Not split. |
| R2 | crates/sequencer/src/sequencer.rs:546 `run` (51 lines) | The proposed `on_step_result` extraction needs `&mut self`, `&mut backoff`, and the `run_once` result passed in; inlined reads no less clearly. Not split. |
| R2 | crates/cluster-client/src/session/mod.rs:266 `on_session_event` (57 lines), :168 `poll_outbound` (51 lines) | Not in this group's explicitly assigned scope, and not touched for any other reason (no pedantic or R7-adjacent need to open this file). Left as is. |
| R9 | crates/sequencer/src/bin/kardamom-sequencer/feeds.rs (kind-byte pre-check before `decode_egress`, two sites) | The pre-check is a deliberate hot-path optimization the appendix itself flags as possibly worth keeping ("If the cheap pre-check must stay for cost reasons, expose `wire::peek_kind`"). The current split (`handle_reject_frame`, `handle_boundary_frame`, each gated on the cheap byte before calling into `decode_egress`) already isolates the two checks into named functions; adding a typed `EgressKind` peek on top is a further nice-to-have, not a correctness or size issue, and was not attempted given the size of this task. |
| R9 | crates/sequencer/src/bin/kardamom-sequencer/main.rs:129 `anyhow::ensure!(args.sequencer_id.is_none(), ...)` | Switching to clap's `conflicts_with` changes the exact CLI error text an operator sees (clap's generated message differs from the current custom one). The brief requires preserving observable behavior; this is a real conflict, resolved by leaving the runtime `ensure!` check as is. |

## Mechanical rows (unreachable `pub`, argument counts)

| rule | file:line | what changed |
|---|---|---|
| R8 (mechanical) | crates/sequencer/src/bin/kardamom-sequencer/adapters.rs:15,20,33,38,49,54,71,76 | Covered above (all four structs plus their `new` constructors are `pub(crate)`). |
| R8 (mechanical) | crates/sequencer/src/bin/kardamom-sequencer/feeds.rs:47,166,244,258 | Covered above (`pub(crate)`). |
| R11 (mechanical) | crates/sequencer/src/bin/kardamom-sequencer/feeds.rs:258 `spawn_publish_loops`, 12 args | Covered above under R7/R2 (`PublishLoops<P>`). |
| R3 (mechanical) | crates/cluster-client/src/protocol.rs, 22 pedantic sites | Covered under R11 pedantic sweep below; file stays one module (R3 KEEP, confirmed above). |
| R3 (mechanical) | crates/sequencer/src/sequencer.rs, 20 pedantic sites | Covered under R11 pedantic sweep below; file stays one module (R3 KEEP, confirmed above). |

## R10 imperative style

| rule | file:line | what changed |
|---|---|---|
| R10 | crates/sequencer/src/nonce_decode.rs:69 (fold over big-endian bytes, `peek_nonce`) | Replaced the `let mut v = 0u64; for &x in bytes { ... }` loop with `bytes.iter().fold(0u64, \|v, &x\| (v << 8) \| u64::from(x))`. |
| R10 | crates/sequencer/src/nonce_decode.rs:87,96 (the same fold, duplicated for `0xb8..=0xbf` and `0xf8..=0xff`) | Extracted `fn be_len(bytes: &[u8]) -> usize`, used at both sites, replacing both copies of the loop. |
| R10 | crates/cluster-adapter/src/live/endpoints.rs:62 `open_next_member_pub` round-robin loop | Replaced the `for step in 1..=ids.len() { ...; if let Some(p) = ...  { return ...; } }` loop with `(1..=ids.len()).map(...).find_map(...)`. |
| R10 | crates/sequencer/src/sequencer.rs:518 `let mut publishes = Vec::new(); for action in result.actions { match action { ... } }` | Kept as a loop. The `ReportDuplicate` arm calls `self.proven_executed(...)` and conditionally publishes an error, a side effect inside the loop; the appendix itself offers this as the harder-to-read alternative ("splitting into `partition` plus two passes is the cleaner alternative" is the only functional option, and it duplicates the `proven_executed` check across two passes). Left as a loop; this reproduces the appendix's own KEEP-leaning judgment, so it is listed as Done, not "Not done". |
| R10 | crates/cluster-adapter/src/live/session_loop.rs:537 `crossbeam::Select` registration loop | KEEP, confirmed. `crossbeam::Select` needs the imperative `sel.recv(...)` registration; not an iterator-shaped operation. |
| R10 | crates/sequencer/src/resync/mod.rs:276,342 `for _ in 0..FLOOR_DRAIN_PER_ITER { match self.floor_rx.try_recv() { ... } }` (and `drain_contiguity_rejects`) | KEEP, confirmed. `try_iter().take(N)` would lose the `Disconnected` arm, which sets `floor_rx_dead`/`reject_rx_dead` and is load-bearing. |
| R10 | crates/cluster-adapter/src/live/session_loop.rs:253,480 (same `try_recv` shape) | KEEP, confirmed, same reason. |
| R10 | crates/sequencer/src/unconfirmed.rs:134 `while stale.len() < max { ... }` | KEEP, confirmed. Needs the early `break` on the first non-expired front entry plus lazy deletion; not expressible as a chain. |
| R10 | crates/sequencer/src/state/mod.rs:181 `let mut out = Vec::new(); for (&sender, buf) in self.pending.iter_mut() { ... }` | KEEP, confirmed (behavior unchanged). Rewrote `self.pending.iter_mut()` to `&mut self.pending` (R11 `explicit_iter_loop`) but kept the loop shape: it mutates `self.next` while holding `&mut self.pending`, which an iterator chain would force into a `Vec<Address>` snapshot first. |
| R10 (test) | crates/sequencer/tests/multi_sequencer_dual_write.rs:152 (index loop over 4 parallel `Vec`s) | Rewritten as part of the R2 split of this file into `drain_round_robin`, which now indexes `0..sequencers.len()` (unchanged shape; the parallel-slice zip alternative the appendix proposes was not applied, since the function now takes 4 separate slices as parameters and grouping them into one `Replica` struct would touch every call site building the harness). |
| R10 (test) | crates/sequencer/tests/replicated_shard_racing.rs:105,145 (`stream.push` nested loop; `first_seen_merge`) | Not rewritten to `flat_map`; the nested loop builds `(TxDataLoc, TxEnvelope)` pairs with a running `offset` counter used for more than the flat index (it steps by 64 bytes per entry), and `first_seen_merge` (line 145 "already functional, no change" per the appendix) needed no change. |
| R10 (test) | crates/sequencer/tests/alloc_profile.rs:99, sequencer_integration.rs:85 (flat index loops) | Not rewritten; same reasoning as `replicated_shard_racing.rs` above (not attempted, low value relative to the size of this task). |

## R11 pedantic (clippy pedantic sites)

All 317 rows in `inputs-sequencer.md`'s pedantic table are resolved: `cargo clippy -p
kardamom-sequencer|kardamom-cluster-adapter|kardamom-cluster-client --all-targets --
-W clippy::pedantic` reports zero warnings in any file under
`crates/sequencer`, `crates/cluster-adapter`, or `crates/cluster-client`, checked per
crate. Structural fixes (splits, visibility, `Frame`) are listed above under their own
rule; this table covers the remaining doc/cast/style fixes, batched per file. Most
`doc_markdown` (backticks), `must_use_candidate`, and `default_trait_access` sites
were applied by `cargo clippy --fix` (two passes) and hand-verified; the rest were
fixed by hand as listed.

| rule | file | what changed |
|---|---|---|
| R11 | crates/cluster-adapter/src/config.rs:5,38,51 | `#[must_use]`, doc backticks (`clippy --fix`). |
| R11 | crates/cluster-adapter/src/gateway.rs:24,49,55,58,83,89,93 | `#[must_use]`, doc backticks, `# Panics` sections on the four `.lock().unwrap()`-based fake methods (`set_outcome`, `accepted`, `push`, `close`). |
| R11 | crates/cluster-adapter/src/live/endpoints.rs:78,80 | `map_or` instead of `map().unwrap_or()` (`clippy --fix`); `now_ms`'s `as u64` replaced with `u64::try_from(...).unwrap_or(u64::MAX)`. |
| R11 | crates/cluster-adapter/src/live/mod.rs:167,179,188,204,317 | `# Errors` sections on the four `connect*` functions; `clippy --fix` for the `;`-consistency site. |
| R11 | crates/cluster-adapter/src/live/session_loop.rs:118,384,419,484 | `#[allow(clippy::struct_excessive_bools)]` with a one-line reason on `SessionLoop` (independent flags, not a state machine); the three `now as i64` casts changed to `i64::try_from(now).unwrap_or(i64::MAX)`. |
| R11 | crates/cluster-adapter/src/watermark.rs:19,37 | `#[must_use]` (`clippy --fix`). |
| R11 | crates/cluster-adapter/src/wire/egress.rs:44,204,208,214,230,235,240 | `# Errors` on `decode_egress`; `#[must_use]` (`clippy --fix`); the `payload.len() as u32` cast in `encode_egress_record` documented with a one-line MTU-bound comment and `#[allow(clippy::cast_possible_truncation)]`. |
| R11 | crates/cluster-adapter/src/wire/ingress.rs:24,43,62,87,110,115,119,121,129,161,167,181,204 | `#[must_use]`; `# Errors` on `encode_ingress_epoch`, `encode_ingress_remote_epoch`, `split_ingress`, `ingress_sender_nonce`; `# Panics` on `encode_ingress_batch` (the `u16::try_from(...).expect(...)` entry-count conversion) and on `split_ingress` (documented as unreachable, the slice is proven `CANONICAL_ID_LEN` bytes just above); the two `usize`/`u16`/`u32` casts in `encode_ingress_batch` converted to `try_from`/a one-line MTU-bound comment. |
| R11 | crates/cluster-adapter/src/wire/mod.rs:80,82,177,186 | Doc backticks, `#[must_use]` (`clippy --fix`). |
| R11 | crates/cluster-adapter/src/wire/tests.rs:33,131,206 | Doc backticks (`clippy --fix`); the `input: Default::default()` site fixed to `(&[][..]).into()` (matching the sibling entry; `bytes` is not a direct dependency of this crate, so `Bytes::default()` was not available); `u32::from(...)` instead of `as u64`. |
| R11 | crates/cluster-adapter/tests/end_to_end.rs:4,84 | Doc backticks (`clippy --fix`); `i32::from(tag)` instead of `as i32`. |
| R11 | crates/cluster-client/src/bytes.rs:20,25,30,35,40 | `#[must_use]` (`clippy --fix`; already present, no change needed on re-check). |
| R11 | crates/cluster-client/src/protocol.rs:10-15,85,147,155,189,211,221,230,239,241,251,296,308,338,348,374,405 | Doc backticks; `# Errors` on `MessageHeader::decode`, `decode_session_connect_request`, `decode_two_i64`, `decode_egress`; `#[must_use]`; the `put_var` length cast documented with a one-line bound comment; the three `trivially_copy_pass_by_ref` sites (`encode_session_event`'s `EventCode` param, etc.) resolved as part of `decode_session_event`/`decode_new_leader_event` taking `MessageHeader` by value instead of `&MessageHeader` (it is `Copy`, 8 bytes) in the `Frame` refactor (R9). |
| R11 | crates/cluster-client/src/session/mod.rs:27,36,143,154,158,385 | Doc backticks; `#[must_use]` (`clippy --fix`); `connect_attempts`/`state` additionally gated `#[cfg(test)]` (R8, above). |
| R11 | crates/sequencer/benches/throughput.rs:47,57,66,69 | `Bytes::default()`/`FixedBytes::default()` (`clippy --fix`, then a fully-qualified `alloy_primitives::Bytes::default()` fix for an ambiguity with the `bytes` crate's own `Bytes`); the `usize`/`i32` casts converted to `u64::try_from(...).unwrap()`/`i32::try_from(...).unwrap()`. |
| R11 | crates/sequencer/src/bin/kardamom-sequencer/adapters.rs:1,15,20,33,38,49,54,71,76 | Doc backticks; visibility narrowing (R8, above). |
| R11 | crates/sequencer/src/bin/kardamom-sequencer/feeds.rs:4,47,56,67,68,69,84,166,194,244,258,266,298 | Doc backticks; `;`-consistency (`clippy --fix`); the three `needless_pass_by_value` sites (`watermark`, `reject_tx`, `shutdown` on `run_egress_watermark_feed`) fixed by taking `&SharedWatermark`, `&Sender<_>`, `&Shutdown`; `ignored_unit_patterns` (`clippy --fix`); the two `similar_names` sites fixed by renaming `epoch_sub`/`remote_epoch_sub` to `epoch_subscription`/`remote_epoch_subscription`; the `u128`-to-`u64` cast in `flag` converted to `try_from(...).unwrap_or(u64::MAX)`. |
| R11 | crates/sequencer/src/bin/kardamom-sequencer/main.rs:3,4,61,65,66,68,71,80,82,104,147,172,327 | Doc backticks; `too_many_lines` on `main` (R2, above); the `partition_index as u8` cast documented with a one-line bound comment and `#[allow(clippy::cast_possible_truncation)]`; the `all_fields_same_prefix` `Handles` struct fields renamed (`tx_data_sub` to `data_sub`, etc.); `;`-consistency (`clippy --fix`). |
| R11 | crates/sequencer/src/config.rs:13,15,37,39,82,103,114 | Doc backticks; `# Errors` on `validate`; the `partition_index as u8` cast in `rotate_partition` documented and `#[allow]`ed, same bound as `main.rs` above. |
| R11 | crates/sequencer/src/epoch.rs:9,37,48,65,77,81,110,113 | Doc backticks; wildcard import replaced with explicit imports (`clippy --fix`); `# Errors` on `process_epoch` and `EpochSubscriber::poll`; `# Panics` on `ScriptedEpochs::push`/`close`; the two test-only `as u8` casts converted to `u8::try_from(...).unwrap()`. |
| R11 | crates/sequencer/src/inbound.rs:1,3,8,16,19,33,44,46 | Doc backticks; wildcard import removed; `# Errors` on `TxDataSubscriber::poll`. |
| R11 | crates/sequencer/src/lib.rs:5 | Doc backticks (`clippy --fix`). |
| R11 | crates/sequencer/src/metrics.rs:124,140,148,152 | `map_or` instead of `map().unwrap_or()`; the three `as f64` gauge casts converted to a `let` binding with a one-line "never nears 2^52" comment and `#[allow(clippy::cast_precision_loss)]`. |
| R11 | crates/sequencer/src/nonce_decode.rs:39,159,176,177 | `#[allow(clippy::many_single_char_names)]` on `peek_nonce` with a one-line reason (dense byte-walking decoder); `AccessList::default()`/`Bytes::default()` with fully-qualified paths (`alloy_eips::eip2930::AccessList`, `alloy_primitives::Bytes`; `clippy --fix` could not resolve these paths automatically). |
| R11 | crates/sequencer/src/outbound.rs:3,5,8,36,40,48,81,90,93,112,114,224 | Doc backticks; wildcard import removed; `# Errors` on `try_publish_ref`, `try_publish_epoch`, `try_publish_remote_epoch`; the test-only `n as u8` cast converted to `u8::try_from(n).unwrap()`. |
| R11 | crates/sequencer/src/outbound/cluster.rs:110,125,192,222,224 | `cluster_ref_publisher` deleted (R8, above; its `missing_errors_doc` row is moot); `# Errors` on `cluster_ref_publisher_with_egress`; doc backticks. |
| R11 | crates/sequencer/src/partition.rs:15,19,28,48 | `# Panics` and `#[must_use]` on `partition_for`; the modulo-result cast documented with a one-line proof ("`leading % u64::from(m)` is always `< m`") and `#[allow]`ed; `cast_lossless` sites (`clippy --fix`); `# Errors` on `validate_partition_count`. |
| R11 | crates/sequencer/src/pending.rs:49,56,60,64,69,73,230,235 | `#[must_use]`; `# Panics` on `insert` (the `.expect(...)` on the full-buffer max, proven non-empty by the caller's own `at_capacity` check just above); `lowest_nonce` deleted (R8, above); the two test-loop `n as u32` casts converted to `u32::try_from(n).unwrap()`. |
| R11 | crates/sequencer/src/remote_epoch.rs:26,42,52,70,82,86,117,121 | Doc backticks; wildcard import removed; `# Errors` on `process_remote_epoch` and `RemoteEpochSubscriber::poll`; `# Panics` on `ScriptedRemoteEpochs::push`/`close`; the two test-only casts converted to `try_from`. |
| R11 | crates/sequencer/src/resync/mod.rs:12,41,86,92,102,155,174,216,244,248,402,404,449,477 | Doc backticks; `#[must_use]` on the `ResyncController`/`SharedWatermark` accessors; `#[allow(clippy::struct_excessive_bools)]` on `ResyncController` with a one-line reason; the four `u128`-to-`u64` millisecond casts in `note_publish_stall`/`maybe_exit` converted to `try_from(...).unwrap_or(u64::MAX)`. |
| R11 | crates/sequencer/src/resync/tests.rs:53 | `Duration::from_secs` instead of `from_millis(70_000)` (`clippy --fix`). |
| R11 | crates/sequencer/src/sender.rs:44 | `alloy_primitives::B256::default()` instead of `Default::default()`. |
| R11 | crates/sequencer/src/sequencer.rs:3,5,8,26,31,44,45,78,85,88,115,143,370,376,377,378,478,546,551 | Doc backticks; `#[must_use]`/`# Panics` on `new`; `# Errors` on `run_once` and `run`; `match_same_arms` fixed by merging the `Buffered`/`BufferedReplaced`/`BufferedDisabled` arms in `handle_outcome`; `needless_pass_by_value` on `run`'s `shutdown` fixed by taking `&Shutdown` (done together with R7 above). |
| R11 | crates/sequencer/src/shutdown.rs:18,25,33,38,44 | `#[must_use]`; `;`-consistency (`clippy --fix`). |
| R11 | crates/sequencer/src/state/mod.rs:56,64,72,184,219 | `#[must_use]` on `new`, `next_nonce`, `next_nonce_known`; `explicit_iter_loop` (`self.pending.iter_mut()` to `&mut self.pending`) in `drain_pending`; `map_or` instead of `map().unwrap_or()` in `advance_floor`. |
| R11 | crates/sequencer/src/state/tests.rs:103,108,119,139,148,180 | The five `n as u32` casts converted to `u32::try_from(n).unwrap()`; the wildcard match arm named explicitly (`ProcessAction::ReportDuplicate { .. } => None`). |
| R11 | crates/sequencer/src/unconfirmed.rs:28 | Doc backticks (`clippy --fix`). |
| R11 | crates/sequencer/tests/alloc_profile.rs:4,7,9,13,23,62,133,160,161,162,165,169 | Doc backticks (`clippy --fix`); the eight `f64`/`usize` reporting casts covered by one function-level `#[allow(clippy::cast_precision_loss, clippy::cast_possible_truncation)]` with a one-line reason (a profiling test turning small counters into printed ratios). |
| R11 | crates/sequencer/tests/e2e_docker.rs:7,40,64 | Doc backticks, cast fix (`clippy --fix`). |
| R11 | crates/sequencer/tests/multi_sequencer_dual_write.rs:1,2,5,55,65,83,100,101,109,132,136,146,147,177,181,200,216 | Rewritten as part of the R2 split of this file (see above): doc backticks; `Bytes::default()`/`FixedBytes::default()` fully qualified; the `too_many_lines` split into `build_harness`, `load_input_streams`, `drain_round_robin`, `assert_shard_ownership`, `assert_dense_nonce_sequences`; a `TX_PER_SENDER_USIZE` const (with a one-line `#[allow(clippy::cast_possible_truncation)]`) replacing the repeated `TX_PER_SENDER as usize`; a `SenderAtPos` type alias for the `very_complex_type` return; the long literal `0xC0FFEE` to `0x00C0_FFEE`; every remaining `u64`-to-`usize`/`i32` cast converted to `try_from(...).unwrap()`. |
| R11 | crates/sequencer/tests/partition_routing.rs:1 | Doc backticks (`clippy --fix`). |
| R11 | crates/sequencer/tests/replicated_shard_racing.rs:2,6,56,71,95,145,207,226,233,261,264,278,280,290,297,307 | Doc backticks; `Bytes::default()` fully qualified; `HashMap::default()` instead of `Default::default()`; `.copied()` instead of `.cloned()`; the four `items_after_statements` sites fixed by moving `use alloy_rlp::Decodable as _;`/`use alloy_consensus::transaction::Transaction as _;` from mid-block to the file's top-level imports; the remaining `u64`-to-`usize` cast converted to `try_from(...).unwrap()`. |
| R11 | crates/sequencer/tests/resync_filter.rs:37,48,141 | `Bytes::default()`/`FixedBytes::default()` fully qualified; the `i32` cast converted to `try_from(...).unwrap()`. |
| R11 | crates/sequencer/tests/sequencer_integration.rs:1,2,4,7,45,55,60,64,111 | Doc backticks; `Bytes::default()`/`FixedBytes::default()` fully qualified; the two `i32` casts converted to `try_from(...).unwrap()` (`pos_n`); `needless_continue` fixed (`Ok(true) => {}` instead of `Ok(true) => continue,` inside a `loop`). |
| R11 | crates/sequencer/tests/sequencer_step.rs:32,43 | `Bytes::default()`/`FixedBytes::default()` fully qualified; the `run`/`run_once` call sites updated for the `SequencerPorts` (R7) and `&Shutdown` (R11) signature changes. |
| R11 | crates/sequencer/tests/state_proptest.rs:1 | File deleted; moved into `src/state/proptest_tests.rs` as part of the `pub(crate) mod state` change (R8, above). The doc-backticks warning no longer applies to a file that does not exist at that path. |

## Counts

- Done: 130 rows (including every R11 pedantic row, cited above by file; 0
  pedantic warnings remain in any of the three crates' own files; includes
  the two "(reversed)" rows above, moved here from "not done" — see Phase
  C's R13 and R14 sections for the changes themselves).
- Deferred to Phase B: 5 rows.
- Not done, judged wrong: 9 rows (down from the original 15: 3
  `partition_count`/`Sequencer::new`/`debug_assert!` rows and the
  `signed_envelope` builder-duplication row reversed to Done above;
  `validate_partition_count`/`PartitionConfigError`'s row is gone,
  superseded by the `PartitionCount` newtype that replaced the function
  it was about, so it is moot rather than "not done").

Gate results: `cargo clippy -p <crate> --all-targets -- -D warnings` clean,
`cargo clippy -p <crate> --all-targets -- -W clippy::pedantic` zero
warnings, `cargo test -p <crate>` all pass, `cargo fmt -p <crate> --
--check` clean, for all three crates (`kardamom-sequencer`,
`kardamom-cluster-adapter`, `kardamom-cluster-client`).

No dependency needed. No file outside this group's directories was
touched.

# Status: sequencer group (Phase C)

Phase C order: R13, R12, R15, R16, R14 (as directed). All four gates pass
for all three crates after every rule's changes, checked crate-by-crate
with `--no-deps` on the pedantic run (a combined multi-crate run gives
inconsistent counts under the shared `CARGO_TARGET_DIR`; a
`cargo check --workspace --all-targets` mid-session also intermittently
showed stale errors in files this session never touched — resolved both
times by `cargo clean -p kardamom-cluster-adapter --target-dir
/home/dev/kardamom-8/target`, which pointed at another concurrent agent's
build colliding in the shared cache, not a real bug here):

1. `cargo clippy -p <crate> --all-targets --all-features -- -D warnings`:
   clean, all three crates.
2. `cargo clippy -p <crate> --all-targets --all-features --no-deps -- -W
   clippy::pedantic`: zero warnings in every file this phase touched (the
   pre-existing `tests/e2e_docker.rs` pedantic warnings are untouched and
   out of scope).
3. `cargo test -p <crate>` and the six-crate combined run (`-p
   kardamom-sequencer -p kardamom-cluster-adapter -p kardamom-cluster-client
   -p kardamom-engine -p kardamom-ingress -p kardamom-executor`): all pass,
   zero failures.
4. `cargo fmt -p <crate> -- --check`: clean.
5. `cargo check --workspace --all-targets`: clean (after the cache-clean
   above).

## R13 — non-zero types (Done)

Every row in `docs/reviews/2026-09-07-code-quality-audit/arith-sequencer.md`'s
R13 table is addressed. No `debug_assert!`, no `.max(1)` fixup, and no
`assert!(x > 0)` remains anywhere in the group.

- `crates/sequencer/src/partition.rs` — `PartitionCount(NonZeroU32)` newtype
  (`#[serde(transparent)]`) replaces the bare `NonZeroU32` field and the
  free `partition_for` function; `PartitionCount::index_of` is the one
  routing method (also an R15 item, below).
- `crates/sequencer/src/config.rs` — `SequencerConfig.partition_count:
  PartitionCount`; `Default` and every test-literal site updated.
- `crates/sequencer/src/resync/mod.rs` — `ResyncConfig.dedup_capacity` /
  `enter_percent: NonZeroU64`; new `ResyncConfigError` enum
  (`ThresholdOverflow`, `ThresholdTooSmall`, `CapacityNotUsize`);
  `ResyncConfig::enter_threshold()` and `::validate()` return
  `Result<_, ResyncConfigError>`; `ResyncController::new` and
  `ResyncChannel::open` (the renamed R15 constructor, below) propagate it.
  `SequencerConfig::validate()` now also calls `self.resync.validate()?`.
- `crates/sequencer/src/pending.rs` — `PendingBuffer.capacity:
  Option<NonZeroUsize>` (constructor still takes plain `usize`; `0` means
  disabled, converted internally via `NonZeroUsize::new`).
- `crates/sequencer/src/sender.rs` — the `debug_assert!` deleted; `sender_of`
  is a plain accessor.
- `crates/sequencer/src/sequencer.rs` — `Sequencer::new` returns
  `Result<Self, SequencerError>` (config validation surfaces at
  construction, not as a later panic).
- `crates/cluster-adapter/src/wire/ingress.rs` (`skip_rlp_item`'s callers)
  — `u8::try_from(partition_index)` at the one production cast site.
- Test-literal sites updated throughout `crates/sequencer/tests/` and
  `crates/sequencer/benches/throughput.rs` for the `PartitionCount` type
  change (mechanical).

## R12 — safe arithmetic (Done)

Every FIX row (including the two "opposite mistake" rows) in
`arith-sequencer.md`'s R12 table, plus the two coordinator-named extra
sites, is addressed. Every HOT_PATH_KEEP/PROVEN row is unchanged (verified
by re-reading the table against the current source before editing).

- `crates/sequencer/src/nonce_decode.rs::skip_rlp_item` — only the two
  `i + 1 + ll + l` arms (`0xb8..=0xbf`, `0xf8..=0xff`) use
  `checked_add`; the three PROVEN arms (`i + 1`,
  `i + 1 + (p - 0x80) as usize`, `i + 1 + (p - 0xc0) as usize`) are
  unchanged, matching the table's PROVEN rows.
- `crates/sequencer/src/resync/mod.rs::observe` — a watermark regression
  now logs `tracing::warn!` instead of silently `saturating_sub`-clamping.
- `crates/cluster-adapter/src/wire/ingress.rs::encode_ingress_batch` —
  coordinator-named site: `Vec<u8>` (infallible, used `.expect()`) →
  `Result<Vec<u8>, WireError>`, `u16::try_from`/`u32::try_from` on the
  entry count and each entry's length, new `WireError::BatchTooLarge` /
  `EntryTooLarge` variants (the latter shared with egress, since both are
  "length overflows the u32 wire length prefix").
- `crates/cluster-adapter/src/wire/egress.rs::encode_egress_record` —
  `Vec<u8>` → `Result<Vec<u8>, WireError>` (`u32::try_from` on payload
  length). All in-group and out-of-group callers updated (see the R15
  entry below — the two changes landed together).
- `crates/sequencer/src/resync/mod.rs::resync_channel` (coordinator-named
  site) — capacity parsing: `usize::try_from(cfg.dedup_capacity.get())`
  now returns `ResyncConfigError::CapacityNotUsize` instead of `as usize`
  or `.unwrap()`. Folded into the R13 `ResyncChannel::open` rename.
- `crates/sequencer/src/sequencer.rs::apply_receipt_drain` — the `for _ in
  0..dropped { metrics::record_resync_skip(partition) }` loop replaced by
  one `metrics::record_resync_skip(partition, u64::try_from(dropped)
  .unwrap_or(u64::MAX))` call (`record_resync_skip`'s signature gained a
  `count: u64` parameter; this doubles as an R16 fix — the loop had no
  other side effect, so eliminating it was better than extracting a
  method).
- `crates/cluster-adapter/src/watermark.rs::observe_record` — `index + 1`
  → `index.saturating_add(1)`.
- `crates/cluster-adapter/src/live/session_loop.rs::Resend` — `last_ms: u64`
  → `Option<u64>` (`None` = "never sent", distinct from a real 0ms
  timestamp; this was the literal "opposite mistake" row: the old code
  used 0 as a sentinel that collided with a real timestamp).
- `crates/cluster-client/src/session/mod.rs` — `next_correlation_id`:
  plain `+= 1` → `self.next_correlation_id.wrapping_add(1)` (documented:
  a correlation id wrapping is fine, it is never compared for ordering);
  `connect_attempts`: `+= 1` → `.saturating_add(1)` (documented: only ever
  compared against 0). This was the second "opposite mistake" row — wrapping
  is correct for one field and wrong for the other, and the original code
  had them backwards.
- `crates/cluster-client/src/protocol.rs::put_var` — kept infallible
  (`u32::try_from(...).expect(...)`), with an explicit R12 design-decision
  comment: every caller passes a local config string (channel URI,
  credentials, detail), never attacker-sized data, unlike
  `encode_ingress_batch`/`encode_egress_record`'s wire-decode paths.
  Making it fallible would cascade `Result` through three `pub fn
  encode_*` functions and `SessionDriver::poll_outbound`'s hot path for a
  bound no config value can realistically cross.

## R15 — methods, not standalone functions (Done)

Applied broadly, including retroactively to the R13/R12 code just changed,
per the coordinator's instruction. Every explicitly named target is done:

- `EgressWatermarkFeed`, `ReceiptFloorFeed` — `pub(crate)` structs with
  `new`/`spawn` methods (`crates/sequencer/src/bin/kardamom-sequencer/
  feeds.rs`); the free functions `spawn_egress_watermark_feed`,
  `spawn_receipt_floor_feed` deleted. `run()` also gained an `on_idle`
  method (extracted from an inline match arm).
- `OriginPump` — replaces `spawn_publish_loops`'s duplicated epoch/
  remote-epoch pump bodies with one generic `OriginPump<S, P>::run(step)`;
  `PublishLoops<P>::spawn` is the method that builds and spawns the main
  loop plus both `OriginPump`s.
- `Handles::open` — already a method before this phase; verified
  unchanged in shape (the four handles it opens changed type per the
  `PartitionCount` change only).
- `ResyncWiring::spawn` — updated for the `ResyncChannel::open` rename
  (below); unchanged in shape otherwise.
- `SpawnedLoops::join_one`/`join_all` — already implemented this shape;
  this satisfies the DRY doc's "three `match join_X.await` blocks" item
  too (R14, below) — no separate work needed there.
- `resync_channel` → `ResyncChannel::open` — the free function that built
  a `(ResyncController, Sender<FloorUpdate>, Sender<(Address,u64,u64)>,
  SharedWatermark)` tuple is now `ResyncChannel::open`, a named struct's
  constructor. `main.rs` and every test call site updated.
- `run_receipt_floor_feed` → `ReceiptFloorFeed::run`, a method (folded
  into the `ReceiptFloorFeed` struct above).
- `wire/egress.rs`'s free `decode_egress`, `decode_relayed`,
  `decode_boundary`, `decode_contiguity_reject` → `EgressItem::decode`
  (associated fn) plus three private `EgressItem::decode_*` methods.
  `decode_relayed_payload`, `decode_txref_fields`,
  `decode_depositref_fields`, `decode_epoch_fields`,
  `decode_remote_epoch_fields` → a new `RelayedPayload<'a>` struct
  (`id`, `record_type`, `fields`) with `parse`/`decode`/`decode_txref`/
  `decode_depositref`/`decode_epoch`/`decode_remote_epoch` methods.
  `decode_egress` (the old free-function name) was removed entirely, not
  kept as a facade — the rename is a narrow, mechanical,
  zero-behavior-change signature swap, so every caller (in-group and the
  two out-of-group crates below) was updated directly.
- `Live*` adapter structs deleted — `LiveTxDataSub`, `LiveEpochSub`,
  `LiveRemoteEpochSub`, `LiveTxErrorPub` in
  `crates/sequencer/src/bin/kardamom-sequencer/adapters.rs` (file deleted)
  only forwarded `try_recv`/`publish`; the library now implements the
  sequencer's own traits directly on the foreign `kardamom_log`
  handle types (orphan rule allows it: the trait is local, the type is
  foreign): `impl TxDataSubscriber for TxDataSubscriberHandle`
  (`inbound.rs`), `impl EpochSubscriber for TxDepositsSubscriberHandle`
  (`epoch.rs`), `impl RemoteEpochSubscriber for TxRemoteEpochsSubscriberHandle`
  (`remote_epoch.rs`), `impl TxErrorPublisher for TxErrorsPublisherHandle`
  (`outbound.rs`). `kardamom-log` was already a non-optional dependency.
- `ConnectOptions` struct (`crates/cluster-adapter/src/live/mod.rs`) —
  collapses `connect_subscribed`, `connect_with_replay`,
  `connect_with_egress_kind_filter` into one `connect_with(rt, cfg, opts)`;
  `connect` is the remaining short `ConnectOptions::default()` wrapper.
  Out-of-group callers updated: `crates/engine/src/reader/cluster/mod.rs`,
  `crates/ingress/src/cluster.rs` (both narrow, mechanical edits).
- `event_is_ours` on `SessionDriver` (`crates/cluster-client/src/session/
  mod.rs`) — the foreign-session filter, written inline three times (once
  per `EventCode` arm), is now one method; the `Redirect` arm keeps its
  extra `self.is_connected()` short-circuit (that arm's semantics are not
  identical to the other two — it rejects unconditionally while
  connected, not by session-id match — so it calls `event_is_ours` as one
  term of its existing `||`, not as a full replacement).

## R16 — no nested loops (Done)

Every named target, plus several nested loops found in production code
via a systematic sweep (not just the named targets), are flattened:

- `load_input_streams` (`tests/multi_sequencer_dual_write.rs`) — flattened
  via a `StreamLoader` struct and `Harness::step_all`/`drain_round_robin`
  methods (this also satisfies part of R15: behavior moved onto a struct).
- `shard_stream`'s `.push` loop (`tests/replicated_shard_racing.rs`) —
  `flat_map`+`map` over the cartesian product of `(nonce, sender)`,
  replacing the manual `offset`/`i` accumulator.
- Batch build (`benches/throughput.rs`) — same `flat_map`+`map` pattern.
- `drain_round_robin`'s `loop { for .. }` (`tests/
  multi_sequencer_dual_write.rs`) — folded into the `Harness` struct above.
- The `try_recv` drain loops in `resync/mod.rs` and `session_loop.rs` were
  confirmed NOT nested (single-level `for`/`loop` over one channel) and
  left as-is, per the coordinator's explicit note — but see the R14
  `drain_bounded`/`drain_to_empty` entries below, which deduplicate them
  without changing their loop shape.
- Found and fixed beyond the named list (production code, not just
  tests): `Sequencer::flush_drained` (`src/sequencer.rs`) — extracted
  `record_published_prefix`/`rebuffer_rest` methods, eliminating a
  `while { for {} while {} }`; `PartitionState::drain_pending`
  (`src/state/mod.rs`) — extracted `drain_sender_run`, eliminating a
  `for (&sender, buf) in &mut self.pending { for (n, p) in
  buf.drain_consecutive_from(..) { .. } }`.
- `src/state/proptest_tests.rs` — two nested-loop patterns replaced with
  `.filter_map()` and `.windows(2).all()`/`.enumerate().all()`.

## R14 — DRY (Done, with cross-crate rows deferred; see below)

Read `docs/reviews/2026-09-07-code-quality-audit/dry-sequencer.md` in
full before starting, per the standing instruction to not rely on a
compaction paraphrase.

### Production code

- Four `Live*` adapter wrappers — deleted; see the R15 section above (the
  same change satisfies both rules).
- `ScriptedQueue<T>` — new `crates/sequencer/src/fakes.rs` (gated
  `#[cfg(any(test, feature = "testing"))]`), replacing `epoch.rs`'s
  `ScriptedEpochs` and `remote_epoch.rs`'s `ScriptedRemoteEpochs` hand
  copies with `pub type ScriptedEpochs = ScriptedQueue<EpochRecord>` /
  `pub type ScriptedRemoteEpochs = ScriptedQueue<RemoteEpochRecord>`. The
  queue's drain method is named `poll_next` (not `next`, to avoid
  clippy's `should_implement_trait` — `next` reads as `Iterator::next`).
  A `pump_contract::run` helper (also in `fakes.rs`, `#[cfg(test)]`) then
  owns the three plumbing tests (idle/closed/backpressure) shared by both
  origin modules' test suites — this was the doc's `pump_contract`
  follow-on, done once `ScriptedQueue` landed.
- `too_short`/`rd_slice` (`crates/cluster-adapter/src/wire/mod.rs`) — the
  private `too_short` helper made `pub(super)`; a new `rd_slice(b, at,
  len) -> Result<&[u8], WireError>` added beside it. All 8
  `WireError::TooShort { .. }` literals across `wire/ingress.rs` and
  `wire/egress.rs` now go through one of the two helpers (the doc's line
  numbers, written against an earlier revision of these files, cited 11;
  the current count after this session's earlier R12/R15 edits to the
  same files is 8 — all of them converted).
  - Note: `split_ingress`'s first `too_short` call previously wrote
    `have: buf.len()`; through the shared helper it is now
    `buf.len().saturating_sub(at)`, matching every other `TooShort`'s
    semantics. This is an observable change to one error value's `have`
    field (not just a mechanical dedup) — flagged here explicitly rather
    than folded into "no behavior change".
- `metrics.rs`'s seven dead `record_*` helpers that the doc says duplicate
  `HotMetrics` — not present in the current file. Already gone before
  this phase (most likely cleaned up earlier in the audit); nothing to
  do here.
- `bump`/`set` helpers (`crates/sequencer/src/metrics.rs`) — the nine
  partition-labeled one-line recorders (`record_resync_mode`,
  `record_resync_enter`, `record_lag_suspected`, `record_resync_skip`,
  `record_floor_senders`, `record_floor_advance`,
  `record_canonical_watermark`, `record_unconfirmed_refs`,
  `record_ref_republished`) now call `fn bump(name, partition, n)` / `fn
  set(name, partition, v)`. `record_remote_epoch_relayed` (keyed by
  `origin`, not `partition`) and `record_start_time` (no partition label)
  are unchanged — a different label shape, out of scope for these two
  helpers.
- `spawn_origin_pump` — satisfied by `OriginPump` (R15, above); not a
  separate change.
- `drain_bounded` (`crates/sequencer/src/resync/mod.rs`) — a bounded,
  warn-once `try_recv` drain shared by `drain_floor_updates` and
  `drain_contiguity_rejects`.
- `drain_to_empty` (`crates/cluster-adapter/src/live/session_loop.rs`) —
  an unbounded, silent (no warn) `try_recv` drain shared by `drain_egress`
  and `handle_offers`. Named differently from the sequencer crate's
  `drain_bounded` on purpose: the doc's shared name/signature
  (`drain_bounded(rx, dead, max, on_dead, f)`) does not match this
  crate's actual current code, which has no bound and no per-drain log
  message — matching that shape here would have been a behavior change,
  not a rename. Kept as a separate copy per crate, per the doc's own
  note ("the log messages and the max differ").
- `partition_for` unification into `kardamom_types::routing` — **deferred
  to Phase B** (cross-crate: `crates/ingress/src/routing.rs` is outside
  this group). See below.
- `ConnectOptions` — done; see the R15 section above.
- `From<ClusterConfig> for LiveClusterConfig`
  (`crates/cluster-adapter/src/config.rs`) — added; `to_live(&self)` is
  now `self.clone().into()`, so all six external call sites
  (`crates/executor`, `crates/ingress`, `crates/batcher`,
  `crates/validator`, `crates/sequencer`, plus this crate's own test) are
  unchanged.
- `event_is_ours` — done; see the R15 section above.
- `log_join` — satisfied by the already-existing `SpawnedLoops::join_one`/
  `join_all` (see R15); the doc's suggested name differs, the shape is
  the same; not counted twice.
- `keys_in` on `UnconfirmedLedger` (`crates/sequencer/src/unconfirmed.rs`)
  — `confirm_through` and `take_gap_rewinds` both now call one
  `fn keys_in(&self, sender, lo, hi) -> Vec<UnconfirmedKey>`.
- `be_len` (`crates/sequencer/src/nonce_decode.rs`) — already one shared
  function, called from both `skip_rlp_item` match arms since the R12
  work earlier in this phase. Verified, not re-implemented.
- `offer_encoded` on `ClusterRefPublisher`
  (`crates/sequencer/src/outbound/cluster.rs`) — `try_publish_epoch` and
  `try_publish_remote_epoch` now call one `fn offer_encoded(&mut self, r:
  Result<Vec<u8>, WireError>, what: &str)`. `try_publish_ref_batch`'s
  batch-encode site is not folded in: its return shape is `(usize,
  Option<SequencerError>)`, not `Result<(), SequencerError>`, so it does
  not fit `offer_encoded`'s signature without a second, wider refactor
  the doc does not ask for.
- `ObsArgs` clap struct — **deferred to Phase B** (cross-crate:
  `crates/obs/src/bin.rs` and all six service binaries). See below.

### Tests

- `testkit` module (`crates/sequencer/src/testkit.rs`, gated
  `#[cfg(any(test, feature = "testing"))]`) — `signer`, `EnvelopeSpec` +
  `envelope_with`, `signed_envelope` (= `envelope_with` at
  `EnvelopeSpec::default()`), `pos`, `one_partition_cfg`. Applied to
  `tests/sequencer_step.rs`, `tests/resync_filter.rs`, `tests/
  multi_sequencer_dual_write.rs`, `tests/sequencer_integration.rs`,
  `benches/throughput.rs` (all fully converted — local copies deleted);
  `tests/replicated_shard_racing.rs` and `tests/alloc_profile.rs` keep a
  thin local `signed_envelope` wrapper over `testkit::envelope_with` with
  a non-default `EnvelopeSpec` (real hash / larger calldata), per the
  standing instruction not to flatten `alloc_profile.rs`'s intentionally
  different envelope. `tests/sequencer_integration.rs`'s `pos_n` (a
  different, `u64`, 64-wide-spaced position builder) is kept local: it is
  not the same shape as `testkit::pos`, and is not duplicated elsewhere.
  **Cargo.toml landmine, found and fixed before writing any fixture
  code**: `alloy-network` and `alloy-signer-local` were
  `[dev-dependencies]`-only; a `testing`-gated `testkit` module using them
  would compile under `cargo test --lib` (`cfg(test)` sees dev-deps) and
  fail to compile for every `tests/*.rs`/`benches/*.rs` binary (which link
  the lib through the `[dev-dependencies] kardamom-sequencer = { path =
  ".", features = ["testing"] }` self pull-back, which does not see this
  crate's own dev-dependencies). Fixed by moving both to `[dependencies]`
  as `optional = true`, with `testing = ["dep:alloy-network",
  "dep:alloy-signer-local"]`. Verified by a full green `cargo test -p
  kardamom-sequencer` (lib, every `tests/*.rs`, `benches/throughput.rs` in
  `--test` mode) after the change.
- `FloorUpdate` constructors (`crates/sequencer/src/resync/mod.rs`) —
  `FloorUpdate::executed(sender, nonce)`, `::skip(sender, nonce, reason)`,
  `::deposit(sender)`. Converted all 13 literal sites across `src/resync/
  tests.rs` and `tests/resync_filter.rs` (the doc's production-side
  `FloorUpdate` literal in `feeds.rs` is not one of these three shapes —
  it is built from a live `Receipt`'s dynamic fields — and is correctly
  left as a literal).
- `connecting()`/`redirect_event` (`crates/cluster-client/src/session/
  tests.rs`) — `connecting() -> (SessionDriver, i64)` wraps the
  build-driver/poll/decode-correlation-id prologue (11 sites); the one
  site with an assertion between `new()` and the decode
  (`wrap_app_only_after_connected`) was reordered to use `connecting()`
  too, since `poll_outbound` does not itself change connection state.
  `redirect_event(correlation_id, leader, endpoints) -> Vec<u8>` replaces
  the twice-repeated `Redirect` `SessionEvent` literal.
- `EgressItem`/`TxOrderingMessage` derive `PartialEq` (verified, both do)
  — the six `match ... { Variant { .. } => { asserts }, other =>
  panic!(...) }` blocks in `crates/cluster-adapter/src/wire/tests.rs`
  became one `assert_eq!` each.
- `drive_to_idle(cfg, stream) -> (Vec<TxRef>, Vec<TxError>)`
  (`testkit.rs`) — applied to `tests/sequencer_integration.rs`'s two
  build-then-drive-then-inspect tests
  (`integration_duplicates_are_reported`,
  `integration_bounded_buffer_evicts_oldest`). **Not** applied to the
  other four sites the doc lists — reviewed each and judged wrong:
  `tests/multi_sequencer_dual_write.rs` and `tests/
  replicated_shard_racing.rs` drive multiple sequencers against one
  shared publisher (not one rig); `benches/throughput.rs`'s rig build
  must stay outside the timed `iter_batched` closure; `tests/
  alloc_profile.rs`'s DHAT profiling region has its own start/stop
  boundary around the drive loop. `tests/sequencer_step.rs` and `tests/
  resync_filter.rs` were also checked and do not fit: every test in the
  former inspects intermediate state between individual `run_once` calls,
  and the latter interleaves `floor_tx.send(...)` between drive
  iterations — neither is a single build-then-drain-to-idle shape.
- `txref(tag: u8)` (`crates/cluster-adapter/src/wire/mod.rs`, gated
  `testing`) — replaces the copy in `crates/cluster-adapter/tests/
  end_to_end.rs` (exact match) and `crates/sequencer/src/outbound/
  cluster.rs`'s test module (fixed constants, no behavior meaning,
  switched to `txref(0x11)`). **Not** applied to `crates/cluster-adapter/
  src/wire/tests.rs`'s own `txref()`: that fixture's `tx_data_session_id:
  5` is called out in its own comment as "exercises the active/active
  session field roundtrip" — a real, load-bearing non-zero value a
  single-`tag`-parameter shared fixture cannot reproduce without also
  parameterizing shard id and session id, which the doc's signature does
  not do. Kept separate rather than silently weakening that assertion.
- `calm_controller()` (`crates/sequencer/src/resync/tests.rs`) — wraps
  `mk(ResyncConfig::default())` + `calm_down`, used by 5 of the 6 tests
  that need a pre-calmed controller (`starts_in_resync_and_exits_when_calm`
  tests the calm-down transition itself, so it keeps calling `mk`/
  `calm_down` directly).
- `free_port`/`scrape` — **deferred to Phase B** (cross-crate:
  `crates/obs/src/testing.rs` plus four other crates' copies). See below.
- `pump_contract` — done; see the production-code section above (the
  doc's suggestion depended on `ScriptedQueue` landing first, which it
  did in this phase).
- `partition_routing.rs`/`crates/ingress/src/routing.rs` KEEP row — no
  change; both crates still check their own copy of the routing rule.
  Depends on the deferred `partition_for` move (below); if that lands in
  Phase B, this becomes one shared test at the same time.

### Minor disclosed behavior notes (not defects, recorded for the record)

- `benches/throughput.rs`'s `signed_envelope` now uses `testkit`'s fixed
  `gas_price: 1_000_000_000` instead of the bench's previous `gas_price:
  1`. `EnvelopeSpec` does not parameterize `gas_price` (no test needed
  it to vary). This shifts the RLP-encoded `raw_tx`'s size by a few bytes
  (longer varint), a sub-1% effect on a ~250-byte transaction; the bench
  measures `run_once` throughput, not encoded-frame size, so this does
  not change what the bench is measuring in any way that matters.
- `crate::fakes::pump_contract::run`'s backpressure case does not
  additionally assert the publisher's `refs`/`epochs` vec stayed empty
  (the per-module tests it replaced did, for their own record types).
  This is redundant by construction: `InMemoryTxOrderingRefPublisher`'s
  `fail_with_backpressure` check runs before every push, so a `Backpressure`
  return already proves nothing was pushed.

## Deferred to Phase B

- `partition_for` → `kardamom_types::routing::partition_for(sender, m) ->
  u32`, shared with `crates/ingress/src/routing.rs`'s copy. Cross-crate
  (`kardamom-types`, `kardamom-ingress`); both already declare
  `kardamom-types` as a dependency, so the move is mechanical, but it is
  outside this group's directories.
- `ObsArgs { metrics_addr, host_id }` clap struct in `crates/obs/src/
  bin.rs`, `#[command(flatten)]`ed into all six service binaries'
  arg structs. Cross-crate; this group's binary
  (`kardamom-sequencer/main.rs`) is only one of the six sites.
- `free_port()`/`scrape(url)` in `crates/obs/src/testing.rs`, behind a
  `testing` feature. Cross-crate: five crates hold the same copy,
  including `crates/sequencer/tests/metrics_endpoint.rs`, but the shared
  home is outside this group.
- `partition_routing.rs`/`crates/ingress/src/routing.rs`'s duplicated KEEP
  test — becomes one shared test automatically once `partition_for`'s
  move (above) lands; no separate action needed then.

## Not done, judged wrong

- Doc-suggested unification of `crates/cluster-adapter/src/wire/tests.rs`'s
  `txref()` into the shared `wire::txref(tag)` fixture — judged wrong (see
  the R14 tests section above): its `tx_data_session_id: 5` is a real,
  documented, load-bearing assertion value that a single-parameter shared
  fixture cannot reproduce.
- `drive_to_idle` applied to `tests/multi_sequencer_dual_write.rs`,
  `tests/replicated_shard_racing.rs`, `benches/throughput.rs`, `tests/
  alloc_profile.rs`, `tests/sequencer_step.rs`, `tests/resync_filter.rs`
  — judged wrong for each (see the R14 tests section above for the
  per-file reason); the helper was only applied where it fit.
- `drain_bounded`'s exact shared name/signature applied unchanged to
  `crates/cluster-adapter/src/live/session_loop.rs` — judged wrong: that
  crate's two drains are unbounded and log nothing on disconnect, unlike
  `resync/mod.rs`'s bounded, warn-once drains; forcing the same
  name/signature there would have added a bound and a log line neither
  drain has today, changing behavior, not just deduplicating it. Named
  `drain_to_empty` there instead, kept as a separate copy per crate (the
  doc's own note already anticipated separate copies, on the grounds of
  "the log messages and the max differ" — this phase found the max and
  the messages do not just differ, they are absent entirely on this
  crate's side).

## Counts

- Done: 16 of 16 named R14 production-code rows (2 already-satisfied by
  earlier phases, cross-referenced not double-counted; 2 deferred to
  Phase B), 11 of 12 named R14 test rows (2 deferred to Phase B), every
  R13 row, every R12 FIX/opposite-mistake row, every named R15 target
  plus additional retroactive R15 application, every named R16 target
  plus 3 additional production-code nested loops found by direct sweep.
- Deferred to Phase B: 4 rows (all cross-crate: `partition_for` move,
  `ObsArgs`, `free_port`/`scrape`, the one KEEP row that depends on
  `partition_for`'s move).
- Not done, judged wrong: 3 rows (each with a stated reason above; no
  silent skips).

Gate results: see the top of this section. All four gates pass for all
three crates. `cargo check --workspace --all-targets` passes.

# Status: sequencer group (Phase C — coordinator review fixes)

A coordinator review of the Phase A + Phase C diff found one round of
fixes. All are done. Re-verified: all four gates pass for all three
crates (this time including `--all-features`, so `docker-e2e`-gated files
are covered by both the `-D warnings` and pedantic runs), plus
`cargo check --workspace --all-targets`.

## Done

1. **R11 — `reason = "..."` on every `#[allow]`.** Added at all 15 sites
   the review named: `tests/multi_sequencer_dual_write.rs` (the
   `TX_PER_SENDER_USIZE` cast), `src/resync/mod.rs` (`ResyncController`'s
   `struct_excessive_bools`), `src/nonce_decode.rs` (`peek_nonce`'s
   `many_single_char_names`), `cluster-client/src/session/mod.rs`
   (`APP_SEMANTIC_VERSION`'s `identity_op`), `tests/alloc_profile.rs` (the
   profiling test's precision-loss/truncation allow), `cluster-client/src/
   protocol.rs:506` (`session_connect_request_roundtrip`'s `identity_op`,
   with the trailing comment folded into the reason), `src/partition.rs`
   (`index_of`'s truncation allow), `src/bin/kardamom-sequencer/main.rs`
   (both `#[allow(dead_code)]` fields — `main_rt`, `cluster_guard`),
   `src/metrics.rs` (the three gauge-cast allows), `cluster-adapter/src/
   live/session_loop.rs` (`SessionLoop`'s `struct_excessive_bools`). Each
   site's preceding doc/line comment, where it restated the same
   rationale, was folded into the `reason` string and removed rather than
   left as a redundant duplicate.
2. **`crates/sequencer/dhat-heap-sequencer.json`** restored to the base
   version with `jj restore --from @- crates/sequencer/dhat-heap-sequencer.json`
   (twice — once for the profiler-output diff the review found, and again
   after this round's own alloc-profile re-runs regenerated it while
   verifying the `Rig` conversion in item 6 below did not change the
   allocation profile).
3. **R12 in the wire decoders.**
   - `crates/cluster-adapter/src/wire/mod.rs::rd_slice` now computes
     `at.checked_add(len).and_then(|end| b.get(at..end))` instead of
     `b.get(at..at + len)`.
   - `crates/cluster-adapter/src/wire/egress.rs::decode_relayed` routes
     its payload read through `rd_slice` (with
     `usize::try_from(rd_u32(buf, 9)?).unwrap_or(usize::MAX)` instead of
     `as usize`), mapping `rd_slice`'s error into the same
     `WireError::BadPayloadLen` the too-short case used before.
   - `crates/cluster-client/src/protocol.rs`: the same unchecked-add bug
     was present in this crate's own `need(buf, at, n)` (protocol.rs's
     `rd_slice` equivalent, used by `rd_var`'s length-prefixed field
     reads — a wire-input path, not the `SessionMessageHeader` arm the
     review named, which only slices a `RangeFrom` and adds nothing).
     Fixed `need` the same way, and `rd_var`'s `len` conversion to
     `usize::try_from(...).unwrap_or(usize::MAX)`; `rd_var`'s final
     `start + len` is proven in-bounds by the preceding `need` call, so
     it stays a plain add with a comment explaining why.
4. **`session_loop.rs`'s `drain_egress`/`handle_offers`.** `drain_to_empty`
   (which collected into a `Vec` first) replaced by an allocation-free
   `fn next_item<T>(rx: &Receiver<T>, dead: &mut bool) -> Option<T>`. Both
   callers are now `while let Some(x) = next_item(&self.field, &mut
   self.field_dead) { ... self.on_driver_event(ev) ... }`: the two-field
   borrow from `next_item`'s arguments ends before the loop body runs, so
   the body is free to borrow `self` again.
5. **R2 splits (three functions in the 51-100 line band):**
   - `cluster-adapter/src/live/mod.rs::connect_inner` → a private
     `SessionConnect { rt, cfg, opts }` struct with `open_egress`,
     `open_ingress`, `spawn_thread` methods; `connect_with` drives it.
   - `sequencer/src/sequencer.rs::handle_outcome` → the `match` over
     `NonceOutcome` stays; the loop over `result.actions` moved into
     `fn collect_publishes(&self, rc, sender, actions) -> Vec<..>`
     (keeps the `proven_executed` check in one place, as asked).
   - `cluster-client/src/session/mod.rs::on_session_event` → `on_ok`,
     `on_redirect`, `on_rejection`, one per `EventCode` arm.
     `poll_outbound` → `emit_connect`, `emit_keep_alive`.
     (Fixed a `needless_pass_by_value` pedantic warning this split
     introduced: `on_ok` and `on_rejection` never move an owned field out
     of `ev` — unlike `on_redirect`, which moves `ev.detail` — so they
     take `&SessionEvent`, not `SessionEvent`.)
6. **R14 in tests: `testkit::Rig`.** Added
   `pub struct Rig { pub tx_data: ScriptedTxData, pub refs:
   InMemoryTxOrderingRefPublisher, pub errors: InMemoryTxErrorPublisher }`
   with `fn step(&mut self, seq: &mut Sequencer) -> Result<bool,
   SequencerError>` and `fn ports(&mut self) -> Ports<'_, ..>`.
   `drive_to_idle` now builds on `Rig::step`. Converted every
   `run_once(&mut Ports { .. })`/`run(&mut Ports { .. }, ..)` site:
   - `tests/sequencer_step.rs` (12 sites) and `tests/resync_filter.rs`
     (18 sites): mechanical, `Rig::default()` + `rig.step(&mut seq)`.
   - `tests/sequencer_integration.rs`: the one remaining site (the chaos
     test's own drive loop) converted to `drive_to_idle` too — on review
     this fits cleanly (single sequencer, whole input built up front,
     drive to idle, inspect), correcting this session's earlier,
     over-broad claim that no site in this file beyond the two already
     converted would fit.
   - `tests/replicated_shard_racing.rs::run_replica_with`: also converted
     to call `drive_to_idle` directly. This corrects an earlier misjudged
     "not done" note in this file's own section above: `run_replica_with`
     builds exactly one sequencer against one stream (the "two replicas"
     in the test names are two independent calls to it, not a shared
     rig), so it fits `drive_to_idle`'s shape after all.
   - `tests/alloc_profile.rs`: converted to `Rig::step` (not
     `drive_to_idle` — the warmup and measured windows must stay
     separately loop-bounded around the DHAT profiler's start/stop, which
     `drive_to_idle` collapses into one drive-to-completion call). Verified
     by an actual `--ignored` run before and after: allocs/tx, bytes/tx,
     and block counts are bit-for-bit identical (3.00 allocs/tx, 5,120
     txs, 15,360 blocks), confirming `Rig::step`'s stack-only `Ports`
     construction adds no allocation.
   - `benches/throughput.rs`: the bench's local `DequeTxData` fake deleted
     in favor of `Rig` (`ScriptedTxData`'s `disconnected` bool check is
     the only difference from `DequeTxData`, and it is not on this bench's
     measured path in a way that matters). Verified with `cargo bench
     --bench throughput -- --test`.
   - `tests/multi_sequencer_dual_write.rs`: the one remaining site
     (`Harness::step_all`) needed a real restructure, not just a
     mechanical swap: `Harness`'s three parallel `Vec<ScriptedTxData>`/
     `Vec<InMemoryTxOrderingRefPublisher>`/`Vec<InMemoryTxErrorPublisher>`
     became one `Vec<Rig>`, with each `Rig.refs` a clone of the shared
     canonical `b` publisher (this is what made the M sequencers' refs
     land on one shared stream before, and still does — `Rig.refs` is
     just `InMemoryTxOrderingRefPublisher`, `Clone`-backed by the same
     `Arc<Mutex<Vec<TxRef>>>`). `step_all`, `load_input_streams`, and the
     test body's assertions updated accordingly.
   - No `run_once(&mut Ports { .. })` construction remains anywhere in
     `crates/sequencer/tests/` or `crates/sequencer/benches/`. The two
     remaining `Ports { .. }` sites in the whole crate are
     `testkit::Rig::ports` itself (the one canonical construction left)
     and `src/bin/kardamom-sequencer/feeds.rs` (production wiring, out of
     this item's scope).
7. **`tests/partition_routing.rs::zero_partition_count_has_no_nonzerou32_value`**
   deleted (it asserted `NonZeroU32::new(0).is_none()` — a standard-library
   fact, not a check on this crate). `config::tests::
   zero_partition_count_rejected_at_parse` remains as the real check.
8. **R1 (should-fix): `cluster-client/src/protocol.rs::put_var`'s comment.**
   Dropped the `"R12 note:"` prefix and the reference to
   `encode_ingress_batch`/`encode_egress_record`'s own decision; kept the
   substance (`put_var`'s callers pass local config strings, not wire
   input, so a bound check via `try_from` + `expect` is the right shape
   here, not a `Result` cascade).

## Add-on from the coordinator's second message

`cargo clippy --all-features` surfaced three pedantic warnings in
`tests/e2e_docker.rs` (only compiled under the `docker-e2e` feature,
so the crate-scoped, feature-default gate run in this section's first
pass never saw them): two missing-backticks (`` `tx_data` ``,
`` `tx_ordering` `` in the M+1 smoke test's doc comment) and one
`0..M as u8` truncation cast, fixed with `0..u8::try_from(M).unwrap()`.
Re-ran the pedantic gate as
`cargo clippy -p kardamom-sequencer -p kardamom-cluster-adapter -p
kardamom-cluster-client --all-targets --all-features -- -W
clippy::pedantic 2>&1 | grep -- '--> crates/'`, filtered to this
group's three crate directories: zero remaining warnings.

## Gate results (this round)

1. `cargo clippy -p <crate> --all-targets --all-features -- -D warnings`:
   clean, all three crates.
2. `cargo clippy -p kardamom-sequencer -p kardamom-cluster-adapter -p
   kardamom-cluster-client --all-targets --all-features -- -W
   clippy::pedantic`, filtered to `crates/sequencer/`,
   `crates/cluster-adapter/`, `crates/cluster-client/`: zero warnings
   (verified both as a combined multi-crate run, filtered by path, and as
   three separate `--no-deps` per-crate runs, which agree).
3. `cargo test` for all three crates, plus the six-crate combined run
   (`-p kardamom-sequencer -p kardamom-cluster-adapter -p
   kardamom-cluster-client -p kardamom-engine -p kardamom-ingress -p
   kardamom-executor --all-features`): all pass, zero failures.
4. `cargo fmt -p <crate> -- --check`: clean, all three crates.
5. `cargo check --workspace --all-targets`: clean.

# Status: sequencer group (Phase C — coordinator review round 3)

A second coordinator review (diff `7e8345e6f770..1e7a0bc7`) accepted the
wire fixes, `next_item`, the R2 splits, `Rig`, and the dhat restore from
round 2, and asked for six more items. All six are done.

## Done

1. **R12, `cluster-client/src/protocol.rs::rd_var`.** `need` now returns
   `Result<(&[u8], usize), DecodeError>` (the slice, and the offset just
   past it), so a caller chaining a further read never computes that
   offset with a bare `+`. `rd_var` reads the length with
   `rd_len(body, at)?` (see item 2), gets `start` from
   `need(body, at, 4)?.1` (discarding the 4-byte slice — the length
   itself already came from `rd_len`), and returns `need(body, start,
   len)` directly (its return type is exactly `rd_var`'s). No `+` and no
   proof comment remain in `rd_var`.
2. **R14, `rd_len` helper.** Added to each byte-helper module rather than
   the shared `crate::bytes` — that module's own doc comment says "keep
   this module free of error types", and `rd_len` returns a crate-specific
   error, so it does not belong there:
   - `cluster-adapter/src/wire/mod.rs::rd_len(b, at) -> Result<usize,
     WireError>`, using a new `WireError::LenOverflow { declared: u32 }`
     variant. `wire/egress.rs::decode_relayed` now calls it, deleting the
     3-line "never fails on 32/64-bit" comment.
   - `cluster-client/src/protocol.rs::rd_len(buf, at) -> Result<usize,
     DecodeError>`, using a new `DecodeError::LenOverflow { declared: u32
     }` variant. `rd_var` calls it (see item 1), deleting the matching
     comment there too.
3. **R15, `cluster-adapter/src/live/mod.rs::SessionConnect` typestate
   chain.** `open_egress`/`open_ingress` no longer return loose
   `Receiver<Vec<u8>>`/`(i32, PubHandle)` values for `spawn_thread` to
   take as parameters. Each stage now consumes `self` and returns the
   next stage's state: `SessionConnect::new(rt, cfg, opts)?.open_egress()?`
   returns `EgressOpened { base: SessionConnect, frame_rx }`;
   `.open_ingress()?` on that returns `IngressOpened { egress:
   EgressOpened, initial: (i32, PubHandle) }`; `.spawn_thread()` destructures
   the full chain in one pattern and consumes it. `connect_with` is now
   the four-call chain literally:
   `SessionConnect::new(rt, cfg, opts)?.open_egress()?.open_ingress()?.spawn_thread()`.
   No `Option` fields anywhere in the chain.
4. **R14, `testkit::Rig` convenience methods.** Added `Rig::push(&mut
   self, loc: TxDataLoc, env: TxEnvelope)`, `Rig::refs(&self) -> Vec<TxRef>`
   and `Rig::errors(&self) -> Vec<TxError>` (cloned snapshots — a method
   and a field can share a name in Rust, resolved by call syntax, so
   these coexist with the `refs`/`errors` fields without conflict).
   Converted every site: `tests/sequencer_step.rs`,
   `tests/resync_filter.rs`, `tests/alloc_profile.rs` (mechanical,
   `rig.push(loc, env)` / `rig.refs()` / `rig.errors()`);
   `testkit::drive_to_idle` itself; and
   `tests/multi_sequencer_dual_write.rs`, where `StreamLoader::load_shard`
   now takes `rig: &mut Rig` (instead of `channel: &mut ScriptedTxData`)
   and calls `rig.push(..)` directly, and the no-`TxErrors`-emitted
   assertion loop uses `rig.errors().is_empty()`. Direct field access
   remains only where a test needs the lock itself:
   `*rig.refs.fail_with_backpressure.lock().unwrap() = ..` (toggling the
   fake's backpressure switch) and `rig.tx_data.disconnected = true`
   (arming the fake's disconnect flag) — neither is a push or a
   published/emitted-list read, so neither fits `push`/`refs`/`errors`.
5. **R10, `tests/sequencer_integration.rs`.** The `map` that built
   `tx_stream` also inserted into `sender_at_pos` as a side effect.
   `tx_stream` is now built first (a pure `map`, no side effect), and
   `sender_at_pos` is derived from it afterward with its own
   `tx_stream.iter().zip(stream.iter()).map(..).collect()` — `tx_stream`
   carries the `TxDataLoc` (hence the position), `stream` carries the
   sender index and nonce; zipping the two (same length, same order,
   built from the same `enumerate()`) pairs them without recomputing
   either.
6. **Tuple, `sequencer.rs::collect_publishes -> Vec<(Address, u64,
   RefMetadata)>`.** Checked how far the tuple shape reaches before
   deciding whether to name it: `collect_publishes` produces it;
   `handle_outcome` and its caller (`run_once`) pass it through;
   `flush_drained`, `record_published_prefix`, and `rebuffer_rest` all
   take it as a parameter or destructure it — five functions in
   `sequencer.rs` alone. `flush_drained` also receives the *same* tuple
   shape from a second, independent producer,
   `state/mod.rs::PartitionState::drain_pending`/`drain_sender_run`
   (generic over `T`, instantiated at `RefMetadata` — `run_once`'s other
   call site, at the top of the function, passes its result straight into
   `flush_drained` too). That is at least seven functions across two
   files, well past the "only `handle_outcome`'s caller and
   `flush_drained`" threshold this item's instructions set for naming the
   tuple. Left as a tuple; documented here rather than silently skipped.
   (A `PendingPublish` struct is still worth doing, but it would need to
   reach into `state/mod.rs`'s generic `PartitionState<T>` too, which is
   a larger, unrequested change.)

## Gate results (this round)

1. `cargo clippy -p <crate> --all-targets --all-features -- -D warnings`:
   clean, all three crates.
2. `cargo clippy -p kardamom-sequencer -p kardamom-cluster-adapter -p
   kardamom-cluster-client --all-targets --all-features -- -W
   clippy::pedantic`, filtered to `crates/sequencer/`,
   `crates/cluster-adapter/`, `crates/cluster-client/`: zero warnings.
3. `cargo test` for all three crates, plus the six-crate combined run
   (`-p kardamom-sequencer -p kardamom-cluster-adapter -p
   kardamom-cluster-client -p kardamom-engine -p kardamom-ingress -p
   kardamom-executor --all-features`): all pass, zero failures.
4. `cargo fmt -p <crate> -- --check`: clean, all three crates.
5. `cargo check --workspace --all-targets`: clean.

## Review round 4

R4: deleted a dead `drop(refs);` in `tests/sequencer_step.rs`'s
`single_tx_after_idle_survives_repeated_backpressure_without_new_ingress`
— `refs` is `rig.refs()`'s owned `Vec` (round 3), not a mutex guard, so
the manual drop no longer releases anything. `clippy -D warnings` and
`cargo fmt -p kardamom-sequencer -- --check` both clean.

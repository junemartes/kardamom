# Status: ingress group (crates/ingress, crates/interop-feed)

Gates, both crates: `cargo clippy --all-targets -- -D warnings` clean;
`cargo clippy --all-targets -- -W clippy::pedantic` shows zero warnings in
this group's own files; `cargo test` passes (excluding the excluded
categories); `cargo fmt` clean.

This file has two sections in time order: "Done" / "Deferred to Phase B" /
"Not done, judged wrong" below cover Phase A, the initial R1-R11 pass. The
"## Phase C" section near the end covers the coordinator's review follow-up:
one drop-order fix, one reversed deferral, two new rules (R15, R16), and the
R12/R13/R14 passes from `arith-ingress.md` and `dry-ingress.md`. All four
Phase A gates, plus a new `cargo check --workspace --all-targets` gate, were
rerun after Phase C and are reported at the end of that section.

## Done

### R1 comments

| rule | file:line | what changed |
|---|---|---|
| R1 | crates/ingress/src/pending/mod.rs:29 | Deleted the #81 issue story; kept the one-removal-site rule. |
| R1 | crates/ingress/src/pending/mod.rs:116 | Rewrote to state what `watch` gives (cache-latest, lock-free), no "hand-built" comparison. |
| R1 | crates/ingress/src/pending/mod.rs:124 | Rewrote to state the O(released + log parked) cost, no "used to" framing. |
| R1 | crates/ingress/src/pending/mod.rs:173 | Rewrote to state the wait owns the only strong `Arc`; deleted the #81 reference. |
| R1 | crates/ingress/src/pending/mod.rs:230 | "The re-read is a lock-free `borrow`." (deleted "now"). |
| R1 | crates/ingress/src/pending/mod.rs:404 | Deleted the #81 reference; kept the identity-guard rule. |
| R1 | crates/ingress/src/aeron_adapters.rs:7 | Deleted "used to live inside the binary"; module doc states it is unit-testable without a media driver. |
| R1 | crates/ingress/src/aeron_adapters.rs:187 | Rewrote to state the rule (pump task must not hold an `AeronRuntime` clone), no incident story. |
| R1 | crates/ingress/src/aeron_adapters.rs:193 | Rewrote to state the binary's cluster observer feeds the bus; deleted the migration history. |
| R1 | crates/ingress/src/receipt_cache.rs:13 | Deleted "old `CachedReceipt`" mention; states one subscription feeds both indexes. |
| R1 | crates/ingress/src/receipt_cache.rs:49 | Deleted "Before this change" framing; states one allocation is shared. |
| R1 | crates/ingress/src/receipt_cache.rs:69 | Deleted the description of deleted code (`if .. && let`); kept the deadlock rule, moved into `take_victim_key`'s doc. |
| R1 | crates/ingress/src/sig_verify.rs:108 | Superseded by the R5 mpsc rewrite; no scratch-buffer-swap code or comment remains. |
| R1 | crates/ingress/src/sig_verify.rs:131 | Rewrote `process_batch`'s doc to present-tense facts (42µs/tx, parallel scaling), no "this fixes two measured problems" framing. |
| R1 | crates/ingress/src/sig_verify.rs:165 | Deleted the CI allocation-gate measurement; kept the shared-cursor design rule. |
| R1 | crates/ingress/src/proxy/mod.rs:127 | Rewrote `FEED_CAPACITY` doc to the present sizing only (32k ~ 7s at 4,800 tx/s). |
| R1 | crates/ingress/src/proxy/submit.rs:67 | Deleted "issue #156"; states the rule (response must never carry another tx's hash). |
| R1 | crates/ingress/src/proxy/submit.rs:117 | Deleted "(issue #156)". |
| R1 | crates/ingress/src/proxy/submit.rs:148 | Deleted "issue #86"; states a parked submit pins a connection, so the valve sheds instead. |
| R1 | crates/ingress/src/proxy/submit.rs:169 | Deleted the spec-document reference; states the checks run before sig-verify to save a recovery. |
| R1 | crates/ingress/src/json_rpc.rs:127 | Deleted the comment restating the callee and a private field name. |
| R1 | crates/ingress/src/json_rpc.rs:262 | "The executor populates the fields; ingress only reshapes them." (deleted "now"). |
| R1 | crates/ingress/src/json_rpc.rs:300 | Deleted "It was hardcoded to Legacy before"; kept the 0x7E deposit note, moved into `receipt_envelope`'s doc. |
| R1 | crates/ingress/src/config.rs:53 | Deleted the jsonrpsee-default comparison; states the value must exceed offered rate times receipt latency. |
| R1 | crates/ingress/src/config.rs:84 | Deleted the "64k gave only 13.7s" history; kept the present horizon (128k ~ 27s at 4,800 tx/s). |
| R1 | crates/ingress/src/bin/kardamom-ingress/main.rs:66 | Deleted the TODO(consul-watch) pointer; states membership is static at startup. |
| R1 | crates/ingress/src/bin/kardamom-ingress/main.rs:94 | Deleted "change the default back"; states the default is `on-offer` because nothing publishes a quorum watermark. |
| R1 | crates/ingress/src/bin/kardamom-ingress/main.rs:171 | Deleted the "v0 config loading" / future-Deserialize framing; states the TOML file supplies only `[cluster]`. |
| R1 | crates/ingress/src/bin/kardamom-ingress/main.rs:189 | Deleted "for the v0 deployment ... follow-up"; states the binary protocol stays off. |
| R1 | crates/interop-feed/src/lib.rs:5 | Deleted the spec-document/version reference; states both sides use these DTOs. |
| R1 | crates/interop-feed/src/lib.rs:24 | Rewrote the "v1 runs in FEED-TRUST mode" section: states the wire carries no finality tier. |
| R1 | crates/interop-feed/src/lib.rs:270 | Deleted "E1"/"E2"/"P2" phase markers; states `signature` is optional, `None` means unusable. |
| R1 | crates/interop-feed/src/lib.rs:292 | "Optional today." (deleted "absent until E2"). |
| R1 | crates/interop-feed/src/lib.rs:311 | Deleted "(E2)". |
| R1 (tests) | crates/ingress/tests/sig_verify_batch.rs:114 | Deleted "issue #252"; states the two present rules (CPU time, ratio bound). |
| R1 (tests) | crates/ingress/tests/replicated_cluster_test.rs:10 | Deleted the spec-document/clause-id reference; module doc states the two invariants (publish once, execute once). Also removed D1/D2/D4 clause labels from five test doc comments in the same file, and a "Phase A" label. |
| R1 (tests) | crates/ingress/tests/protocol_limits_test.rs:2 | Deleted the spec-document reference (W1b). |
| R1 (tests) | crates/ingress/src/pending/tests.rs:79 | Deleted "before this fix" history; states the rule (`Evicted` must release the parked submit). |
| R1 (tests) | crates/ingress/src/pending/tests.rs:357 | "--- Cancelled-future cleanup ---" (deleted "the #81 follow-up"). |

### R2 long functions (appendix + mechanical list)

| rule | file:line | what changed |
|---|---|---|
| R2 | crates/ingress/src/bin/kardamom-ingress/main.rs:166 (`main`, 94 lines) | Split into `build_config`, `open_aeron_side`, `spawn_cluster_watermark`, `shutdown`. Folds in the R4 `drop(rt)` fix (see below). |
| R2 | crates/ingress/src/json_rpc.rs:185 (`subscribe_receipts`, 55 lines) | Split into `next_event`, `apply_sender_filter`, `send_event`. |
| R2 | crates/ingress/src/json_rpc.rs:268 (`receipt_to_rpc`, 52-54 lines) | Split into `rpc_logs` and `receipt_envelope`. |

Mechanical long-function rows (51-100 lines) decided case by case, per the
rule: split when the appendix names helpers or the body repeats a shape;
otherwise leave and say so. None of the eight below have an appendix
helper list, and each is one linear test/bench scenario, so all eight are
left unsplit (see "Not done, judged wrong" below): `stage_costs.rs:32`,
`end_to_end_test.rs:26`, `benches/throughput.rs:47`,
`end_to_end_test.rs:191`, `benches/latency.rs:44`, `routing_test.rs:20`,
`end_to_end_test.rs:285`, `end_to_end_test.rs:119`,
`receipt_subscription_test.rs:136`.

### R3 large files

None found in the appendix (both crates stay under 500 code lines). No
action needed.

### R4 manual drops

| rule | file:line | what changed |
|---|---|---|
| R4 | crates/ingress/src/pending/mod.rs:222 | New `fn park(&self, entry_guard, weak, tx_idx)` takes the mutex guard by value; it drops at the function's end, no explicit `drop`. |
| R4 | crates/ingress/src/pending/mod.rs:257 | New `fn lookup_weak` returns a `Weak` directly, so `on_tx_error`'s deferred-release path never holds a strong `Arc`; no `drop` needed. |
| R4 | crates/ingress/src/bin/kardamom-ingress/main.rs:317 | `main` now keeps `_rt: AeronRuntime` bound in its own scope (never moved into a helper); it drops when `main` returns. The leading underscore only silences the unused-binding warning. |

### R5 sync primitives and channels

| rule | file:line | verdict | what changed |
|---|---|---|---|
| R5 | crates/ingress/src/sig_verify.rs:69 | REPLACE_WITH_CHANNEL | `Mutex<Vec<VerifyRequest>>` replaced by `mpsc::unbounded_channel`; the flush task uses `recv_many` racing the flush-window deadline in a `select!`, preserving the depth-flush and window-flush behavior. |
| R5 | crates/ingress/src/sig_verify.rs:71 | REPLACE_WITH_CHANNEL | `Notify` deleted; `recv_many` wakes on arrival. |
| R5 | crates/ingress/src/seen_receipts.rs:33 | REPLACE_WITH_OWNERSHIP | `SeenReceipts` moved out of `IngressProxy` (was `Arc<SeenReceipts>` with an internal `Mutex`). The `tx_receipts` watcher task in `proxy/watchers.rs` now owns one `SeenReceipts` by value, captured in its `AsyncFnMut` closure; `insert` takes `&mut self`, no lock. |
| R5 | crates/ingress/src/receipt_cache.rs:28-29 | JUSTIFIED, with a suggested cleanup | Applied the suggested cleanup: dropped the inner `Arc` around each `DashMap` field, since `ReceiptCache` is only ever used behind an outer `Arc<ReceiptCache>`. |

The remaining ~24 R5 rows in the appendix are JUSTIFIED and kept as is
(no code change): `pending/mod.rs:63` (`DashMap`, `oneshot` x3),
`pending/mod.rs:119-120,131-132` (watch senders, `Mutex<ParkedIndex>`,
`park_seq` atomic — see "Not done, judged wrong" for the one row I tried
and reverted), `sig_verify.rs:176,49,215`, `tx_error_dedup.rs:84`,
`rate_limit.rs:32` (JUSTIFIED as of the audit; independently replaced
under the defect fix, see below), `proxy/mod.rs:108,113,118,121,187,188,33`,
`channels.rs:45,47,51,54,59,71,72-76,87,91-95` (the last one's suggested
constant is now `BUS_CAPACITY`, applied), `aeron_adapters.rs:141-145,97,108,232`
(the last one's constant is also `BUS_CAPACITY`; the suggested
`publish_watermark` wrapper is not applied, see deferred list),
`json_rpc.rs:192-193`, `bin/kardamom-ingress/recorders.rs:51`,
`bin/kardamom-ingress/main.rs:222`.

### R6 dynamic dispatch

| rule | file:line | what changed |
|---|---|---|
| R6 | crates/ingress/src/json_rpc.rs:407 | `PeerAddrService::Future` is now `tokio::task::futures::TaskLocalFuture<Cell<Option<IpAddr>>, S::Future>` (named), not `Pin<Box<dyn Future>>`. `call` returns `PEER_ADDR.scope(cell, inner.call(req))` directly, no `Box::pin`. |

### R7 too many generics

| rule | file:line | what changed |
|---|---|---|
| R7 | crates/ingress/src/binary.rs:82 (`handle_connection<W,P,S>`) | New `trait AsyncStream: AsyncRead + AsyncWrite + Unpin {}` (blanket impl) collapses `W`'s bounds; new `ProxyBackend` (in `channels.rs`) collapses `P,S` into one type param `B`. Callers use `handle_connection::<_, (P, S)>(...)`. |
| R7 | crates/ingress/src/proxy/mod.rs:33 (`spawn_broadcast_watcher<T,F,Fut>`) | Replaced by `drain_broadcast<T, F: AsyncFnMut(T)>`, a non-spawning async fn; call sites do `tokio::spawn(drain_broadcast(rx, async move |x| {...}))`. Drops the `Fut` parameter. Also fixed `needless_continue` (was `Err(Lagged) => continue` inside a `loop`). |
| R7 | crates/ingress/src/json_rpc.rs:98 (`struct IngressHandlers<P,S>`) | Now `IngressHandlers<Backend: ProxyBackend>`, holding `IngressProxy<Backend::Pub, Backend::Sub>`. `IngressHandlers` is internal-only (see R8), so this was safe without touching `IngressProxy`'s own signature. |
| R7 | crates/ingress/src/json_rpc.rs:399 (`impl<S,Body> Service for PeerAddrService<S>`) | New `trait HttpService<Body>: Service<hyper::Request<Body>> + Clone + Send + 'static where Self::Future: Send + 'static {}` (blanket impl) collapses the repeated bound set to `S: HttpService<Body>` (plus a restated `S::Future: Send` bound; a `where`-clause on a trait declaration is not automatically implied at every use site in stable Rust). |

### R8 unnecessary pub

| rule | file:line | what changed |
|---|---|---|
| R8 | crates/ingress/src/channels.rs:71 (`tx_data_tx`) | `pub` to `pub(crate)`. |
| R8 | crates/ingress/src/channels.rs:74 (`local_fsync_bus`) | `pub` to `pub(crate)`. |
| R8 | crates/ingress/src/channels.rs:75 (`block_boundary_bus`) | `pub` to `pub(crate)`. `receipt_bus`, `watermark_bus`, `tx_error_bus` stay `pub`: own tests use them directly (`tests/*.rs` grep confirmed). |
| R8 | crates/ingress/src/rate_limit.rs:30 (`PerIpLimiter`) | `pub` to `pub(crate)`; the module `rate_limit` itself is now `pub(crate)` in `lib.rs` (no external or own-test reference). |
| R8 | crates/ingress/src/rate_limit.rs:18 (`RateLimited`) | `pub` to `pub(crate)`. |
| R8 | crates/ingress/src/receipt_cache.rs:27 (`ReceiptCache`) | `pub` to `pub(crate)`; module `receipt_cache` is `pub(crate)` in `lib.rs`. |
| R8 | crates/ingress/src/seen_receipts.rs:32 (`SeenReceipts`, `DEFAULT_CAPACITY`) | Both `pub` to `pub(crate)`; module `seen_receipts` is `pub(crate)`. |
| R8 | crates/ingress/src/tx_error_dedup.rs:83 (`TxErrorDedup`, `DEFAULT_WINDOW`, `DEFAULT_CAPACITY`) | All `pub` to `pub(crate)`; module `tx_error_dedup` is `pub(crate)`. |
| R8 | crates/ingress/src/binary.rs:29-35 (`STATUS_*` x7) | All `pub` to `pub(crate)`; module `binary` is `pub(crate)`. |
| R8 | crates/ingress/src/binary.rs:37 (`MAX_FRAME_BYTES`) | `pub` to `pub(crate)`. |
| R8 | crates/ingress/src/binary.rs:82 (`handle_connection`) | `pub` to `pub(crate)`. |
| R8 | crates/ingress/src/proxy/mod.rs:54 (`pack_correlation_id`) | `pub` to `pub(crate)`. `ingress_id_of` stays `pub` (own tests use it directly). |
| R8 | crates/ingress/src/json_rpc.rs:98 (`IngressHandlers`) | `pub` to `pub(crate)` (module `json_rpc` stays `pub`: `start_jsonrpc_server` is used by own tests). |
| R8 | crates/ingress/src/pending/mod.rs:152 (`with_error_grace`, `DEFAULT_TX_ERROR_GRACE`) | Both `pub` to `pub(crate)`; module `pending` is `pub(crate)`. |
| R8 | `lib.rs` module declarations | `binary`, `pending`, `rate_limit`, `receipt_cache`, `seen_receipts`, `tx_error_dedup` changed from `pub mod` to `pub(crate) mod` (confirmed via grep: zero references from `tests/`, `benches/`, or `src/bin/` for any of the six). Added `pub use pending::ReceiptResponse;` to `lib.rs`, since `IngressProxy::submit_raw` is genuinely `pub` and returns it (`private_interfaces` under `-D warnings` otherwise). |
| R8 (mechanical, unreachable_pub) | crates/ingress/src/bin/kardamom-ingress/recorders.rs:26,38,85 | `RecorderReady`, `spawn_tx_data_recorders`, `wait_for_recorders`: `pub` to `pub(crate)` (binary target; nothing outside the binary can use `pub` here anyway). |
| R8 (mechanical) | crates/ingress/src/json_rpc.rs:385,395 | `PeerAddrLayer`, `PeerAddrService<S>`: `pub` to `pub(super)` (the enclosing `mod peer_addr_layer` is itself private; `pub` was unreachable, `pub(super)` is the tightest correct visibility for parent-module access). |
| R8 (mechanical) | crates/ingress/tests/common/mod.rs:15,39,44,66 | `sign_legacy_tx`, `sign_legacy`, `sign_legacy_with_gas`, `sign_eip4844`: `pub` to `pub(crate)` (each integration test binary compiles its own private copy of this module; `pub` was unreachable outside that one binary). |
| R8 (mechanical) | crates/ingress/tests/sig_verify_batch.rs:18 | `fixtures::signed`: `pub` to `pub(crate)` (the `fixtures` module itself is private to this one test binary). |

Only `IngressConfig`, `IngressHandle`, `IngressProxy`, and `MockChannels`
reach another crate (confirmed by grepping `crates/` for
`kardamom_ingress` outside `crates/ingress/`, excluding this group's own
tests/benches). `IngressPublication`/`IngressSubscription` re-exports in
`lib.rs`, and the `config`, `error`, `json_rpc`, `proxy`, `routing`,
`sig_verify`, `cluster`, `aeron_adapters`, `metrics`, `channels` modules,
stay `pub`: each has at least one item used from `tests/`, `benches/`, or
`src/bin/` (own compilation units), confirmed by grep, per the appendix's
note.

### R9 defensive validation

| rule | file:line | what changed |
|---|---|---|
| R9 | crates/ingress/src/sig_verify.rs:88 | `BatchVerifier::new`/`with_parallelism` take `depth: NonZeroUsize`. Deleted `assert!(depth > 0)`. `IngressConfig::sig_verify_batch_depth` is now `NonZeroUsize` too (confirmed safe: only `crates/bench` constructs `IngressConfig` externally, via `..IngressConfig::default()`, never naming this field). |
| R9 | crates/ingress/src/seen_receipts.rs:46 | `SeenReceipts::new` takes `capacity: NonZeroUsize`. Deleted the assert. |
| R9 | crates/ingress/src/tx_error_dedup.rs:97 | `TxErrorDedup::new` takes `capacity: NonZeroUsize`. Deleted the assert. |
| R9 | crates/ingress/src/receipt_cache.rs:35 | `ReceiptCache::new` takes `capacity: NonZeroUsize`. Deleted the assert. `IngressConfig::receipt_cache_capacity` is now `NonZeroUsize` (same external-safety check as above). |
| R9 | crates/ingress/src/binary.rs:98 | New `FrameLen` newtype with `TryFrom<u32>`, checked once against `MAX_FRAME_BYTES`; `handle_connection` reads `len.get()` with no re-check. |
| R9 | crates/ingress/src/proxy/submit.rs:178 | New `fn validate_protocol_limits(&ConsensusEnvelope) -> Result<(), IngressError>` merges the `Eip4844` rejection and the gas-cap check into one call, next to each other, as the appendix asked. (Not a full newtype: `GasLimit` would only be read once, so the merged function is the concrete win.) |

`crates/ingress/src/routing.rs:11` (`debug_assert!(m > 0)`) is deferred;
see below.

### R10 imperative style

| rule | file:line | what changed |
|---|---|---|
| R10 | crates/ingress/src/aeron_adapters.rs:45 | `for` loop with `Vec::with_capacity`/`push` replaced by `(0..shards).map(...).collect::<Result<Vec<_>, _>>()?`. |
| R10 | crates/ingress/src/channels.rs:84 | Two `Vec`s filled by one loop replaced by `(0..shards).map(|_| mpsc::unbounded_channel()).unzip()`. |
| R10 | crates/ingress/src/sig_verify.rs:177 | `Vec::with_capacity`/`push` loop for `spawn_blocking` handles replaced by `(0..workers).map(...).collect()`. |
| R10 | crates/ingress/src/receipt_cache.rs:76 | Victim-key selection moved into `fn take_victim_key<K,V>(map) -> Option<K>`, so the iterator-drop-before-remove rule lives in one function, not a comment. |
| R10 (tests) | crates/ingress/tests/replicated_cluster_test.rs:131 | `drains` `Vec` loop replaced by `.into_iter().enumerate().map(...).collect()`. |
| R10 (tests) | crates/ingress/tests/end_to_end_test.rs:47 | `handles` `Vec` loop replaced by `partition_rx.drain(..).map(...).collect()`. |
| R10 (tests) | crates/ingress/tests/batched_sig_verify_test.rs:16 | Three `Vec`s filled in one loop replaced by one `.map()` pass into a combined `Vec<(Address, (Address,B256), Future)>`, then a plain destructuring loop distributes into the three vectors (the signing and single-path recovery work runs exactly once, in the map). |

Not flagged, confirmed correct to keep: `proxy/mod.rs:40` (now
`drain_broadcast`'s loop), `cluster.rs:48`, `json_rpc.rs:199`
(`next_event`'s `tokio::select!` loop) — all `match`-on-`recv()` loops
with early-return/continue semantics that a plain iterator adapter
cannot express. `bin/kardamom-ingress/recorders.rs:45` already used
`map`/`unzip`.

### Defect fix (README "Defects found on the way", #11)

| file:line | what changed |
|---|---|
| crates/ingress/src/rate_limit.rs:32 | `PerIpLimiter` rewritten on top of `governor::RateLimiter::dashmap` (keyed limiter, already available: `governor`'s `dashmap` feature is on by default and `dashmap` is already a dependency — no new dependency). Every `check()` call amortized-sweeps idle buckets via `retain_recent()` once every 4096 calls (`maybe_sweep`/`sweep`, the latter directly callable by a test). Regression test `sweep_evicts_a_fully_refilled_bucket` asserts a bucket that fully refills while idle is removed; `a_still_active_bucket_survives_a_sweep` asserts a recently spent bucket is not. |

### R11 clippy pedantic (153 rows, inputs-ingress.md)

All 153 rows are resolved: a scoped `cargo clippy --all-targets -- -W
clippy::pedantic` pass (filtered to files under `crates/ingress/` and
`crates/interop-feed/`) shows zero warnings after the changes below. A few
rows' lint names shifted during the refactor (for example
`ignored_unit_patterns` at `rate_limit.rs:52` disappeared because the
R5 rewrite removed the line it was on); those are noted.

| rule | file:line | lint | what changed |
|---|---|---|---|
| R11 | crates/ingress/benches/latency.rs:25 | `default_trait_access` | Replaced `Default::default()` with the concrete type's `::default()` (or a targeted `#[allow]` where naming the type needs a new dependency). |
| R11 | crates/ingress/benches/latency.rs:67 | `cast_possible_truncation` | Replaced the cast with `try_from`, a widening conversion, or a proven-bound `#[allow]` with a one-line comment naming the bound. |
| R11 | crates/ingress/benches/latency.rs:67 | `cast_possible_wrap` | Replaced the cast with `try_from` or a proven-bound `#[allow]` with a one-line comment naming the bound. |
| R11 | crates/ingress/benches/throughput.rs:26 | `default_trait_access` | Replaced `Default::default()` with the concrete type's `::default()` (or a targeted `#[allow]` where naming the type needs a new dependency). |
| R11 | crates/ingress/benches/throughput.rs:69 | `cast_possible_truncation` | Replaced the cast with `try_from`, a widening conversion, or a proven-bound `#[allow]` with a one-line comment naming the bound. |
| R11 | crates/ingress/benches/throughput.rs:69 | `cast_possible_wrap` | Replaced the cast with `try_from` or a proven-bound `#[allow]` with a one-line comment naming the bound. |
| R11 | crates/ingress/src/aeron_adapters.rs:40 | `missing_errors_doc` | Added a `# Errors` section, or the item narrowed to `pub(crate)` so the lint no longer applies. |
| R11 | crates/ingress/src/aeron_adapters.rs:87 | `doc_markdown` | Added backticks around the flagged identifier in the doc comment. |
| R11 | crates/ingress/src/aeron_adapters.rs:95 | `doc_markdown` | Added backticks around the flagged identifier in the doc comment. |
| R11 | crates/ingress/src/aeron_adapters.rs:106 | `doc_markdown` | Added backticks around the flagged identifier in the doc comment. |
| R11 | crates/ingress/src/aeron_adapters.rs:149 | `missing_errors_doc` | Added a `# Errors` section, or the item narrowed to `pub(crate)` so the lint no longer applies. |
| R11 | crates/ingress/src/aeron_adapters.rs:232 | `must_use_candidate` | Added `#[must_use]`, or the item narrowed to `pub(crate)` so the lint no longer applies. |
| R11 | crates/ingress/src/bin/kardamom-ingress/main.rs:3 | `doc_markdown` | Added backticks around the flagged identifier in the doc comment. |
| R11 | crates/ingress/src/bin/kardamom-ingress/main.rs:10 | `doc_markdown` | Added backticks around the flagged identifier in the doc comment. |
| R11 | crates/ingress/src/bin/kardamom-ingress/main.rs:60 | `doc_markdown` | Added backticks around the flagged identifier in the doc comment. |
| R11 | crates/ingress/src/bin/kardamom-ingress/main.rs:73 | `doc_markdown` | Added backticks around the flagged identifier in the doc comment. |
| R11 | crates/ingress/src/bin/kardamom-ingress/main.rs:76 | `doc_markdown` | Added backticks around the flagged identifier in the doc comment. |
| R11 | crates/ingress/src/bin/kardamom-ingress/main.rs:80 | `doc_markdown` | Added backticks around the flagged identifier in the doc comment. |
| R11 | crates/ingress/src/bin/kardamom-ingress/main.rs:228 | `cast_possible_truncation` | Replaced the cast with `try_from`, a widening conversion, or a proven-bound `#[allow]` with a one-line comment naming the bound. |
| R11 | crates/ingress/src/bin/kardamom-ingress/main.rs:242 | `cast_possible_truncation` | Replaced the cast with `try_from`, a widening conversion, or a proven-bound `#[allow]` with a one-line comment naming the bound. |
| R11 | crates/ingress/src/bin/kardamom-ingress/recorders.rs:1 | `doc_markdown` | Added backticks around the flagged identifier in the doc comment. |
| R11 | crates/ingress/src/bin/kardamom-ingress/recorders.rs:6 | `doc_markdown` | Added backticks around the flagged identifier in the doc comment. |
| R11 | crates/ingress/src/bin/kardamom-ingress/recorders.rs:28 | `doc_markdown` | Added backticks around the flagged identifier in the doc comment. |
| R11 | crates/ingress/src/bin/kardamom-ingress/recorders.rs:30 | `doc_markdown` | Added backticks around the flagged identifier in the doc comment. |
| R11 | crates/ingress/src/bin/kardamom-ingress/recorders.rs:32 | `doc_markdown` | Added backticks around the flagged identifier in the doc comment. |
| R11 | crates/ingress/src/bin/kardamom-ingress/recorders.rs:39 | `needless_pass_by_value` | Changed the parameter to take by reference (`&Path`/`&ChannelsConfig`/`&AeronConfig`). |
| R11 | crates/ingress/src/bin/kardamom-ingress/recorders.rs:40 | `needless_pass_by_value` | Changed the parameter to take by reference (`&Path`/`&ChannelsConfig`/`&AeronConfig`). |
| R11 | crates/ingress/src/bin/kardamom-ingress/recorders.rs:41 | `needless_pass_by_value` | Changed the parameter to take by reference (`&Path`/`&ChannelsConfig`/`&AeronConfig`). |
| R11 | crates/ingress/src/binary.rs:59 | `missing_errors_doc` | Added a `# Errors` section, or the item narrowed to `pub(crate)` so the lint no longer applies. |
| R11 | crates/ingress/src/binary.rs:82 | `missing_errors_doc` | Added a `# Errors` section, or the item narrowed to `pub(crate)` so the lint no longer applies. |
| R11 | crates/ingress/src/binary.rs:124 | `cast_possible_truncation` | Replaced the cast with `try_from`, a widening conversion, or a proven-bound `#[allow]` with a one-line comment naming the bound. |
| R11 | crates/ingress/src/channels.rs:8 | `doc_markdown` | Added backticks around the flagged identifier in the doc comment. |
| R11 | crates/ingress/src/channels.rs:24 | `doc_markdown` | Added backticks around the flagged identifier in the doc comment. |
| R11 | crates/ingress/src/channels.rs:36 | `doc_markdown` | Added backticks around the flagged identifier in the doc comment. |
| R11 | crates/ingress/src/channels.rs:37 | `doc_markdown` | Added backticks around the flagged identifier in the doc comment. |
| R11 | crates/ingress/src/channels.rs:39 | `doc_markdown` | Added backticks around the flagged identifier in the doc comment. |
| R11 | crates/ingress/src/channels.rs:41 | `doc_markdown` | Added backticks around the flagged identifier in the doc comment. |
| R11 | crates/ingress/src/channels.rs:52 | `doc_markdown` | Added backticks around the flagged identifier in the doc comment. |
| R11 | crates/ingress/src/channels.rs:70 | `doc_markdown` | Added backticks around the flagged identifier in the doc comment. |
| R11 | crates/ingress/src/channels.rs:80 | `doc_markdown` | Added backticks around the flagged identifier in the doc comment. |
| R11 | crates/ingress/src/channels.rs:83 | `must_use_candidate` | Added `#[must_use]`, or the item narrowed to `pub(crate)` so the lint no longer applies. |
| R11 | crates/ingress/src/cluster.rs:91 | `missing_errors_doc` | Added a `# Errors` section, or the item narrowed to `pub(crate)` so the lint no longer applies. |
| R11 | crates/ingress/src/cluster.rs:113 | `cast_possible_truncation` | Replaced the cast with `try_from`, a widening conversion, or a proven-bound `#[allow]` with a one-line comment naming the bound. |
| R11 | crates/ingress/src/cluster.rs:113 | `cast_sign_loss` | Replaced the cast with `try_from` or a proven-bound `#[allow]` with a one-line comment naming the bound. |
| R11 | crates/ingress/src/json_rpc.rs:5 | `doc_markdown` | Added backticks around the flagged identifier in the doc comment. |
| R11 | crates/ingress/src/json_rpc.rs:61 | `doc_markdown` | Added backticks around the flagged identifier in the doc comment. |
| R11 | crates/ingress/src/json_rpc.rs:84 | `doc_markdown` | Added backticks around the flagged identifier in the doc comment. |
| R11 | crates/ingress/src/json_rpc.rs:146 | `redundant_closure_for_method_calls` | Replaced the closure with a method reference. |
| R11 | crates/ingress/src/json_rpc.rs:175 | `redundant_closure_for_method_calls` | Replaced the closure with a method reference. |
| R11 | crates/ingress/src/json_rpc.rs:334 | `missing_errors_doc` | Added a `# Errors` section, or the item narrowed to `pub(crate)` so the lint no longer applies. |
| R11 | crates/ingress/src/json_rpc.rs:414 | `map_unwrap_or` | Replaced `.map(f).unwrap_or(v)` with `.map_or(v, f)`. |
| R11 | crates/ingress/src/json_rpc.rs:417 | `redundant_closure_for_method_calls` | Replaced the closure with a method reference. |
| R11 | crates/ingress/src/metrics.rs:12 | `doc_markdown` | Added backticks around the flagged identifier in the doc comment. |
| R11 | crates/ingress/src/metrics.rs:16 | `doc_markdown` | Added backticks around the flagged identifier in the doc comment. |
| R11 | crates/ingress/src/pending/mod.rs:3 | `doc_markdown` | Added backticks around the flagged identifier in the doc comment. |
| R11 | crates/ingress/src/pending/mod.rs:88 | `cast_precision_loss` | File-level `#[allow(clippy::cast_precision_loss)]` with a reason comment (measurement/rate code; precision never approached), or `#[allow]` on a metrics gauge assignment. |
| R11 | crates/ingress/src/pending/mod.rs:145 | `must_use_candidate` | Added `#[must_use]`, or the item narrowed to `pub(crate)` so the lint no longer applies. |
| R11 | crates/ingress/src/pending/mod.rs:152 | `must_use_candidate` | Added `#[must_use]`, or the item narrowed to `pub(crate)` so the lint no longer applies. |
| R11 | crates/ingress/src/pending/mod.rs:192 | `doc_markdown` | Added backticks around the flagged identifier in the doc comment. |
| R11 | crates/ingress/src/pending/mod.rs:238 | `doc_markdown` | Added backticks around the flagged identifier in the doc comment. |
| R11 | crates/ingress/src/pending/mod.rs:336 | `semicolon_if_nothing_returned` | Added the trailing `;`. |
| R11 | crates/ingress/src/pending/mod.rs:415 | `missing_errors_doc` | Added a `# Errors` section, or the item narrowed to `pub(crate)` so the lint no longer applies. |
| R11 | crates/ingress/src/proxy/mod.rs:44 | `needless_continue` | Removed the redundant `continue` (loop body already ends there). |
| R11 | crates/ingress/src/proxy/mod.rs:54 | `must_use_candidate` | Added `#[must_use]`, or the item narrowed to `pub(crate)` so the lint no longer applies. |
| R11 | crates/ingress/src/proxy/mod.rs:55 | `cast_lossless` | Replaced `as` with the widening `From`/`u64::from` conversion. |
| R11 | crates/ingress/src/proxy/mod.rs:60 | `must_use_candidate` | Added `#[must_use]`, or the item narrowed to `pub(crate)` so the lint no longer applies. |
| R11 | crates/ingress/src/proxy/mod.rs:95 | `doc_markdown` | Added backticks around the flagged identifier in the doc comment. |
| R11 | crates/ingress/src/proxy/mod.rs:100 | `doc_markdown` | Added backticks around the flagged identifier in the doc comment. |
| R11 | crates/ingress/src/proxy/mod.rs:109 | `doc_markdown` | Added backticks around the flagged identifier in the doc comment. |
| R11 | crates/ingress/src/proxy/mod.rs:111 | `doc_markdown` | Added backticks around the flagged identifier in the doc comment. |
| R11 | crates/ingress/src/proxy/mod.rs:114 | `doc_markdown` | Added backticks around the flagged identifier in the doc comment. |
| R11 | crates/ingress/src/proxy/mod.rs:203 | `doc_markdown` | Added backticks around the flagged identifier in the doc comment. |
| R11 | crates/ingress/src/proxy/mod.rs:227 | `doc_markdown` | Added backticks around the flagged identifier in the doc comment. |
| R11 | crates/ingress/src/proxy/mod.rs:229 | `doc_markdown` | Added backticks around the flagged identifier in the doc comment. |
| R11 | crates/ingress/src/proxy/mod.rs:264 | `missing_errors_doc` | Added a `# Errors` section, or the item narrowed to `pub(crate)` so the lint no longer applies. |
| R11 | crates/ingress/src/proxy/submit.rs:30 | `missing_errors_doc` | Added a `# Errors` section, or the item narrowed to `pub(crate)` so the lint no longer applies. |
| R11 | crates/ingress/src/proxy/submit.rs:97 | `doc_markdown` | Added backticks around the flagged identifier in the doc comment. |
| R11 | crates/ingress/src/proxy/submit.rs:103 | `missing_errors_doc` | Added a `# Errors` section, or the item narrowed to `pub(crate)` so the lint no longer applies. |
| R11 | crates/ingress/src/proxy/submit.rs:125 | `unused_self` | Made the method an associated function (dropped `&self`; call sites use `Self::`). |
| R11 | crates/ingress/src/proxy/submit.rs:211 | `doc_markdown` | Added backticks around the flagged identifier in the doc comment. |
| R11 | crates/ingress/src/proxy/watchers.rs:2 | `doc_markdown` | Added backticks around the flagged identifier in the doc comment. |
| R11 | crates/ingress/src/rate_limit.rs:36 | `must_use_candidate` | Added `#[must_use]`, or the item narrowed to `pub(crate)` so the lint no longer applies. |
| R11 | crates/ingress/src/rate_limit.rs:46 | `missing_errors_doc` | Added a `# Errors` section, or the item narrowed to `pub(crate)` so the lint no longer applies. |
| R11 | crates/ingress/src/rate_limit.rs:52 | `ignored_unit_patterns` | Superseded: `check()` no longer has a `.map(|_| ())` step (`check_key`'s `Ok` is already `()`). |
| R11 | crates/ingress/src/receipt_cache.rs:1 | `doc_markdown` | Added backticks around the flagged identifier in the doc comment. |
| R11 | crates/ingress/src/receipt_cache.rs:3 | `doc_markdown` | Added backticks around the flagged identifier in the doc comment. |
| R11 | crates/ingress/src/receipt_cache.rs:12 | `doc_markdown` | Added backticks around the flagged identifier in the doc comment. |
| R11 | crates/ingress/src/receipt_cache.rs:22 | `doc_markdown` | Added backticks around the flagged identifier in the doc comment. |
| R11 | crates/ingress/src/receipt_cache.rs:34 | `missing_panics_doc` | Added a `# Panics` section, removed the panic (`NonZeroUsize`), or the item narrowed to `pub(crate)`. |
| R11 | crates/ingress/src/receipt_cache.rs:34 | `must_use_candidate` | Added `#[must_use]`, or the item narrowed to `pub(crate)` so the lint no longer applies. |
| R11 | crates/ingress/src/receipt_cache.rs:44 | `doc_markdown` | Added backticks around the flagged identifier in the doc comment. |
| R11 | crates/ingress/src/receipt_cache.rs:83 | `must_use_candidate` | Added `#[must_use]`, or the item narrowed to `pub(crate)` so the lint no longer applies. |
| R11 | crates/ingress/src/receipt_cache.rs:89 | `must_use_candidate` | Added `#[must_use]`, or the item narrowed to `pub(crate)` so the lint no longer applies. |
| R11 | crates/ingress/src/receipt_cache.rs:93 | `must_use_candidate` | Added `#[must_use]`, or the item narrowed to `pub(crate)` so the lint no longer applies. |
| R11 | crates/ingress/src/receipt_cache.rs:97 | `must_use_candidate` | Added `#[must_use]`, or the item narrowed to `pub(crate)` so the lint no longer applies. |
| R11 | crates/ingress/src/receipt_cache.rs:154 | `cast_possible_truncation` | Replaced the cast with `try_from`, a widening conversion, or a proven-bound `#[allow]` with a one-line comment naming the bound. |
| R11 | crates/ingress/src/routing.rs:10 | `missing_panics_doc` | Added a `# Panics` section, removed the panic (`NonZeroUsize`), or the item narrowed to `pub(crate)`. |
| R11 | crates/ingress/src/routing.rs:10 | `must_use_candidate` | Added `#[must_use]`, or the item narrowed to `pub(crate)` so the lint no longer applies. |
| R11 | crates/ingress/src/routing.rs:14 | `cast_possible_truncation` | Replaced the cast with `try_from`, a widening conversion, or a proven-bound `#[allow]` with a one-line comment naming the bound. |
| R11 | crates/ingress/src/routing.rs:14 | `cast_lossless` | Replaced `as` with the widening `From`/`u64::from` conversion. |
| R11 | crates/ingress/src/seen_receipts.rs:1 | `doc_markdown` | Added backticks around the flagged identifier in the doc comment. |
| R11 | crates/ingress/src/seen_receipts.rs:45 | `missing_panics_doc` | Added a `# Panics` section, removed the panic (`NonZeroUsize`), or the item narrowed to `pub(crate)`. |
| R11 | crates/ingress/src/seen_receipts.rs:45 | `must_use_candidate` | Added `#[must_use]`, or the item narrowed to `pub(crate)` so the lint no longer applies. |
| R11 | crates/ingress/src/seen_receipts.rs:60 | `missing_panics_doc` | Added a `# Panics` section, removed the panic (`NonZeroUsize`), or the item narrowed to `pub(crate)`. |
| R11 | crates/ingress/src/sig_verify.rs:37 | `missing_errors_doc` | Added a `# Errors` section, or the item narrowed to `pub(crate)` so the lint no longer applies. |
| R11 | crates/ingress/src/sig_verify.rs:80 | `must_use_candidate` | Added `#[must_use]`, or the item narrowed to `pub(crate)` so the lint no longer applies. |
| R11 | crates/ingress/src/sig_verify.rs:81 | `map_unwrap_or` | Replaced `.map(f).unwrap_or(v)` with `.map_or(v, f)`. |
| R11 | crates/ingress/src/sig_verify.rs:87 | `missing_panics_doc` | Added a `# Panics` section, removed the panic (`NonZeroUsize`), or the item narrowed to `pub(crate)`. |
| R11 | crates/ingress/src/sig_verify.rs:87 | `must_use_candidate` | Added `#[must_use]`, or the item narrowed to `pub(crate)` so the lint no longer applies. |
| R11 | crates/ingress/src/sig_verify.rs:210 | `missing_errors_doc` | Added a `# Errors` section, or the item narrowed to `pub(crate)` so the lint no longer applies. |
| R11 | crates/ingress/src/sig_verify.rs:264 | `default_trait_access` | Replaced `Default::default()` with the concrete type's `::default()` (or a targeted `#[allow]` where naming the type needs a new dependency). |
| R11 | crates/ingress/src/sig_verify.rs:271 | `similar_names` | Renamed the shadowing/similar binding (`signed` to `signed_tx`). |
| R11 | crates/ingress/src/tx_error_dedup.rs:5 | `doc_markdown` | Added backticks around the flagged identifier in the doc comment. |
| R11 | crates/ingress/src/tx_error_dedup.rs:18 | `doc_markdown` | Added backticks around the flagged identifier in the doc comment. |
| R11 | crates/ingress/src/tx_error_dedup.rs:82 | `doc_markdown` | Added backticks around the flagged identifier in the doc comment. |
| R11 | crates/ingress/src/tx_error_dedup.rs:96 | `missing_panics_doc` | Added a `# Panics` section, removed the panic (`NonZeroUsize`), or the item narrowed to `pub(crate)`. |
| R11 | crates/ingress/src/tx_error_dedup.rs:96 | `must_use_candidate` | Added `#[must_use]`, or the item narrowed to `pub(crate)` so the lint no longer applies. |
| R11 | crates/ingress/tests/common/mod.rs:24 | `default_trait_access` | Replaced `Default::default()` with the concrete type's `::default()` (or a targeted `#[allow]` where naming the type needs a new dependency). |
| R11 | crates/ingress/tests/common/mod.rs:31 | `similar_names` | Renamed the shadowing/similar binding (`signed` to `signed_tx`). |
| R11 | crates/ingress/tests/common/mod.rs:52 | `default_trait_access` | Replaced `Default::default()` with the concrete type's `::default()` (or a targeted `#[allow]` where naming the type needs a new dependency). |
| R11 | crates/ingress/tests/common/mod.rs:75 | `default_trait_access` | Replaced `Default::default()` with the concrete type's `::default()` (or a targeted `#[allow]` where naming the type needs a new dependency). |
| R11 | crates/ingress/tests/common/mod.rs:78 | `default_trait_access` | Replaced `Default::default()` with the concrete type's `::default()` (or a targeted `#[allow]` where naming the type needs a new dependency). |
| R11 | crates/ingress/tests/end_to_end_test.rs:188 | `doc_markdown` | Added backticks around the flagged identifier in the doc comment. |
| R11 | crates/ingress/tests/end_to_end_test.rs:208 | `items_after_statements` | Moved the `const` to the top of the function. |
| R11 | crates/ingress/tests/end_to_end_test.rs:282 | `doc_markdown` | Added backticks around the flagged identifier in the doc comment. |
| R11 | crates/ingress/tests/replicated_cluster_test.rs:4 | `doc_markdown` | Added backticks around the flagged identifier in the doc comment. |
| R11 | crates/ingress/tests/replicated_cluster_test.rs:43 | `doc_markdown` | Added backticks around the flagged identifier in the doc comment. |
| R11 | crates/ingress/tests/replicated_cluster_test.rs:53 | `doc_markdown` | Added backticks around the flagged identifier in the doc comment. |
| R11 | crates/ingress/tests/replicated_cluster_test.rs:75 | `doc_markdown` | Added backticks around the flagged identifier in the doc comment. |
| R11 | crates/ingress/tests/replicated_cluster_test.rs:140 | `cast_possible_truncation` | Replaced the cast with `try_from`, a widening conversion, or a proven-bound `#[allow]` with a one-line comment naming the bound. |
| R11 | crates/ingress/tests/replicated_cluster_test.rs:140 | `cast_possible_wrap` | Replaced the cast with `try_from` or a proven-bound `#[allow]` with a one-line comment naming the bound. |
| R11 | crates/ingress/tests/sig_verify_batch.rs:28 | `default_trait_access` | Replaced `Default::default()` with the concrete type's `::default()` (or a targeted `#[allow]` where naming the type needs a new dependency). |
| R11 | crates/ingress/tests/sig_verify_batch.rs:101 | `borrow_as_ptr` | Used `&raw mut` instead of an implicit reference-to-pointer coercion. |
| R11 | crates/ingress/tests/sig_verify_batch.rs:102 | `cast_sign_loss` | Replaced the cast with `try_from` or a proven-bound `#[allow]` with a one-line comment naming the bound. |
| R11 | crates/ingress/tests/sig_verify_batch.rs:155 | `cast_precision_loss` | File-level `#[allow(clippy::cast_precision_loss)]` with a reason comment (measurement/rate code; precision never approached), or `#[allow]` on a metrics gauge assignment. |
| R11 | crates/ingress/tests/sig_verify_batch.rs:156 | `cast_precision_loss` | File-level `#[allow(clippy::cast_precision_loss)]` with a reason comment (measurement/rate code; precision never approached), or `#[allow]` on a metrics gauge assignment. |
| R11 | crates/ingress/tests/sig_verify_batch.rs:163 | `cast_precision_loss` | File-level `#[allow(clippy::cast_precision_loss)]` with a reason comment (measurement/rate code; precision never approached), or `#[allow]` on a metrics gauge assignment. |
| R11 | crates/ingress/tests/sig_verify_batch.rs:164 | `cast_precision_loss` | File-level `#[allow(clippy::cast_precision_loss)]` with a reason comment (measurement/rate code; precision never approached), or `#[allow]` on a metrics gauge assignment. |
| R11 | crates/ingress/tests/sig_verify_batch.rs:184 | `map_unwrap_or` | Replaced `.map(f).unwrap_or(v)` with `.map_or(v, f)`. |
| R11 | crates/ingress/tests/sig_verify_batch.rs:208 | `cast_precision_loss` | File-level `#[allow(clippy::cast_precision_loss)]` with a reason comment (measurement/rate code; precision never approached), or `#[allow]` on a metrics gauge assignment. |
| R11 | crates/ingress/tests/stage_costs.rs:18 | `default_trait_access` | Replaced `Default::default()` with the concrete type's `::default()` (or a targeted `#[allow]` where naming the type needs a new dependency). |
| R11 | crates/ingress/tests/stage_costs.rs:41 | `cast_precision_loss` | File-level `#[allow(clippy::cast_precision_loss)]` with a reason comment (measurement/rate code; precision never approached), or `#[allow]` on a metrics gauge assignment. |
| R11 | crates/ingress/tests/stage_costs.rs:47 | `cast_precision_loss` | File-level `#[allow(clippy::cast_precision_loss)]` with a reason comment (measurement/rate code; precision never approached), or `#[allow]` on a metrics gauge assignment. |
| R11 | crates/ingress/tests/stage_costs.rs:50 | `explicit_iter_loop` | Changed `for x in v.iter()` to `for x in &v`. |
| R11 | crates/ingress/tests/stage_costs.rs:53 | `cast_precision_loss` | File-level `#[allow(clippy::cast_precision_loss)]` with a reason comment (measurement/rate code; precision never approached), or `#[allow]` on a metrics gauge assignment. |
| R11 | crates/ingress/tests/stage_costs.rs:85 | `explicit_iter_loop` | Changed `for x in v.iter()` to `for x in &v`. |
| R11 | crates/ingress/tests/stage_costs.rs:88 | `cast_precision_loss` | File-level `#[allow(clippy::cast_precision_loss)]` with a reason comment (measurement/rate code; precision never approached), or `#[allow]` on a metrics gauge assignment. |
| R11 | crates/ingress/tests/stage_costs.rs:117 | `cast_precision_loss` | File-level `#[allow(clippy::cast_precision_loss)]` with a reason comment (measurement/rate code; precision never approached), or `#[allow]` on a metrics gauge assignment. |
| R11 | crates/ingress/tests/stage_costs.rs:118 | `cast_precision_loss` | File-level `#[allow(clippy::cast_precision_loss)]` with a reason comment (measurement/rate code; precision never approached), or `#[allow]` on a metrics gauge assignment. |
| R11 | crates/ingress/tests/stage_costs.rs:119 | `cast_precision_loss` | File-level `#[allow(clippy::cast_precision_loss)]` with a reason comment (measurement/rate code; precision never approached), or `#[allow]` on a metrics gauge assignment. |
| R11 | crates/interop-feed/src/lib.rs:82 | `must_use_candidate` | Added `#[must_use]`, or the item narrowed to `pub(crate)` so the lint no longer applies. |
| R11 | crates/interop-feed/src/lib.rs:196 | `missing_errors_doc` | Added a `# Errors` section, or the item narrowed to `pub(crate)` so the lint no longer applies. |
| R11 | crates/interop-feed/src/lib.rs:262 | `must_use_candidate` | Added `#[must_use]`, or the item narrowed to `pub(crate)` so the lint no longer applies. |

## Deferred to Phase B

| rule | file:line | reason |
|---|---|---|
| R7 | crates/ingress/src/proxy/mod.rs:85 (`struct IngressProxy<P, S>`) | `IngressProxy` is one of the four items that reach `crates/bench` (own group), which names it with an explicit two-parameter alias: `bench/tests/alloc_profile_ingress.rs:60: type Proxy = IngressProxy<MockChannels, MockChannels>;`. Collapsing to `IngressProxy<B: ProxyBackend>` breaks that alias. Also structurally blocked on its own five `impl<P,S> IngressProxy<P,S>` blocks (in `proxy/mod.rs` x2, `submit.rs`, `watchers.rs`): `impl<B: ProxyBackend> Foo for IngressProxy<B::Pub, B::Sub>` is rejected by rustc (E0207, unconstrained type parameter) since the struct itself stays `<P,S>`. Confirmed by the coordinator to stay deferred (Phase C review: "Fine"). |
| R5 | crates/ingress/src/aeron_adapters.rs:232 (`watermark_sender`) | JUSTIFIED, with a suggested narrowing to `fn publish_watermark(&self, w: QuorumWatermark)` instead of returning the raw `Sender`. `watermark_sender()` is called from `crates/ingress/src/bin/kardamom-ingress/main.rs` (own binary, so not strictly a cross-group block) — deferred here only because it is a public-API-shaped change to a struct method I'd rather land with the `R6`/engine-wiring-style changes other groups are doing to `EngineWiring`-adjacent surfaces, not because anything blocks it. Low priority; safe to pick up in Phase B or on request. |
| R13 | crates/ingress/src/config.rs (`IngressConfig::partition_count_m: u32`, `IngressConfig::chain_id: u64`) | Coordinator-owned Phase B follow-up: retype both to `NonZeroU32`/`NonZeroU64` and update the `crates/bench` call sites that build `IngressConfig { .. }` literals against the current `u32`/`u64` fields, together, at merge. `IngressProxy::new`'s `NonZeroU32::new(cfg.partition_count_m).expect("partition_count_m must be non-zero")` stays as is until then. |

**Reversed in Phase C:** the R9 `routing.rs:11` row that stood here in Phase
A (`debug_assert!(m > 0)`, deferred on the belief that `crates/sequencer`
called this crate's `partition_for` directly) is now in Phase C's R13 table,
Done. My Phase A grep for the bare string `partition_for` matched
`crates/sequencer`'s own, separately defined `kardamom_sequencer::partition::partition_for`
function, not a call into this crate's version — the coordinator caught
this; every actual external call site is inside this crate's own
`tests/`. See the Phase C R13 table below.

## Not done, judged wrong

| rule | file:line | reason |
|---|---|---|
| R5 | crates/ingress/src/pending/mod.rs:63 (`tokio::sync::Mutex<Entry>`) | Tried the suggested `std::sync::Mutex` swap (verified by hand that no `.lock()` guard is held across an `.await` at any of the three call sites: `on_receipt`, `on_tx_error`'s deferred closure, `release_satisfied`'s loop). It does not compile: `std::sync::MutexGuard` is `!Send`, and rustc's async-fn `Send` inference for `on_receipt` conservatively includes the guard's storage slot in the generated future's state across the whole function body (both `if`/`else` branches share one lexical binding), even though it is moved out (into `park`) before the only `.await` that follows it. `tokio::spawn`ing the watcher task then fails with "future cannot be sent between threads safely". `tokio::sync::Mutex`'s guard is `Send`, which is exactly why the original code used it; kept unchanged. |
| R2 | crates/ingress/tests/stage_costs.rs:32 (`ingress_stage_costs`, 90 lines) | Single linear measurement body (five timed phases, three `eprintln!` reports); appendix names no helper split; R2's own rule leaves 51-100 line functions unsplit absent a repeated shape or a named helper. |
| R2 | crates/ingress/tests/end_to_end_test.rs:26 (`one_hundred_txs_route_and_receive_receipts`, 68 lines) | Single linear scenario (spawn fake executor, submit 100 txs, assert, retry); no repeated shape; appendix names no helper. |
| R2 | crates/ingress/benches/throughput.rs:47 (`bench_throughput`, 67 lines) | Single linear bench setup; appendix names no helper. |
| R2 | crates/ingress/tests/end_to_end_test.rs:191 (`mds_duplicate_receipts_dedup_resolves_submit_once`, 62 lines) | Single linear scenario; appendix names no helper. |
| R2 | crates/ingress/benches/latency.rs:44 (`bench_e2e_latency`, 60 lines) | Single linear bench setup; appendix names no helper. |
| R2 | crates/ingress/tests/routing_test.rs:20 (`each_tx_lands_on_keccak_partition`, 59 lines) | Single linear scenario (one loop over `m` values, no separable concern); appendix names no helper. |
| R2 | crates/ingress/tests/end_to_end_test.rs:285 (`racing_replica_rejection_is_overridden_by_twin_success`, 57 lines) | Single linear scenario; appendix names no helper. |
| R2 | crates/ingress/tests/end_to_end_test.rs:119 (`proxy_parks_until_watermark_advances`, 54 lines) | Single linear scenario; appendix names no helper. |
| R2 | crates/ingress/tests/receipt_subscription_test.rs:136 (`subscription_streams_deduped_and_filtered_receipts`, 51 lines) | Single linear scenario; appendix names no helper. |

## Dependency needed

None. The defect fix (`PerIpLimiter`) uses `governor::RateLimiter::dashmap`,
already reachable: `governor`'s `dashmap` feature is on by default and
`dashmap` is already a `[dependencies]` entry in `crates/ingress/Cargo.toml`.

## Phase C

Coordinator review of the Phase A diff: accepted (behavior preserved, gates
pass, dependent crates compile). Follow-up: one drop-order fix, one reversed
deferral, two new rules (R15, R16, applied both retroactively to Phase A code
and to this pass), then R13, R12, R15, R16, R14 from `arith-ingress.md` and
`dry-ingress.md`. No new dependency crates; no signature/name/visibility
change used by another crate, except where explicitly noted as staying
deferred to Phase B.

### FIX: drop-order regression in `main.rs`

Phase A's `main` bound `_rt` (the `AeronRuntime`) and let it drop implicitly
at the end of `main`'s scope, after `_cluster_guard`, reversing the original's
explicit `drop(rt)` before `_cluster_guard` went out of scope.

Fixed by folding the fix into the R15 restructuring below: `main.rs` now has
an `IngressService` (owns `args`, `log_cfg`, `file_cfg`, `stop`) whose `run`
method opens Aeron, the optional cluster watermark watcher, and the proxy,
and returns a `RunningIngress` that owns `handle`, `stop`, `recorder_handles`,
`rt`, and `cluster_guard`. `RunningIngress::shutdown(self)` consumes `self`;
its fields drop in **declaration** order (Rust drops struct fields in
declaration order, unlike locals, which drop in reverse), and `rt` is
declared before `cluster_guard`, with a comment at both the field and the
struct doc pointing at each other. So `rt` drops before `cluster_guard`,
inside `shutdown`, matching the original order — with no manual `drop` call
needed.

### REVERSED: `routing::partition_for`

Un-deferred (see the note in "Deferred to Phase B" above). `partition_for`
now takes `m: NonZeroU32`; the `debug_assert!` and its `# Panics` section are
gone. All three in-crate unit tests (`routing.rs`) and every `tests/` call
site (`routing_test.rs`, `end_to_end_test.rs`, `replicated_cluster_test.rs`)
were updated to pass `NonZeroU32::new(m).unwrap()` (or a local `nz()` helper
in `routing.rs`'s own tests).

`IngressConfig::partition_count_m` itself **stays `u32`**, not `NonZeroU32`:
`crates/bench/src/harness/inprocess.rs:72` sets `partition_count_m: shards`
in an `IngressConfig { .. }` literal, where `shards: u32` is
`spawn_inprocess_ingress`'s own parameter, and `crates/bench/tests/alloc_profile_ingress.rs:182`
reads `cfg.partition_count_m as usize`. Converting the field would break both,
cross-crate. Instead, `IngressProxy` gained a new field,
`partition_count_m: NonZeroU32`, computed once in `IngressProxy::new` from
`cfg.partition_count_m` (`NonZeroU32::new(..).expect(..)`, with a `# Panics`
doc: every producer of `IngressConfig` in this workspace already rules out
zero at its own boundary — the CLI's `NonZeroU8`, or `IngressConfig::default()`'s
literal 8). `IngressProxy::partition_for` and `submit.rs::publish_validated`
both read the new field instead of re-parsing `self.cfg.partition_count_m` on
every call.

`IngressProxy<P, S>`'s own R7 collapse (the second Phase A "Deferred"
row) stays deferred — confirmed by the coordinator ("Fine").

### R15: methods, not standalone functions

New rule: attach behavior to a struct with `impl`; pass a method's inputs as
struct state, not loose parameters. Applied to the six named retrofit
targets, plus a few more found along the way (marked "beyond the named
list").

| target (named in the review) | file | what changed |
|---|---|---|
| `validate_protocol_limits` | `proxy/submit.rs` | `ConsensusEnvelope` (`alloy_consensus::TxEnvelope`) is foreign, so an inherent method is not possible (orphan rule); became `trait CheckProtocolLimits { fn check_protocol_limits(&self) -> Result<(), IngressError>; }` implemented for `ConsensusEnvelope`. Call site: `env.check_protocol_limits()?`. |
| `next_event`, `send_event`, `apply_sender_filter` | `json_rpc.rs` | New `struct ReceiptSubscription { receipts, errors, filter, sink }` owns the two feed receivers, the filter, and the sink; `next_event`/`send_event` are now `&mut self`/`&self` methods, and a new `run(self)` method replaces `subscribe_receipts`'s loop body. `apply_sender_filter` became `SenderFilter(Option<HashSet<Address>>)` with an `allows(&self, addr)` method, called as `self.filter.allows(..)` from inside `next_event`'s `tokio::select!` (a field-level borrow, disjoint from the `self.receipts`/`self.errors` borrows the `select!` arms hold — a whole-`self` method call in that position would not borrow-check). |
| `receipt_to_rpc`, `rpc_logs`, `receipt_envelope` | `json_rpc.rs` | Both `kardamom_types::Receipt` and `alloy_rpc_types_eth::TransactionReceipt` are foreign, so `impl From<&Receipt> for TransactionReceipt` is blocked by the orphan rule; added a local newtype, `struct RpcReceipt(TransactionReceipt)`, and one `impl From<&Receipt> for RpcReceipt`. `rpc_logs` and `receipt_envelope`'s bodies are inlined into that one `from` — they had no state of their own beyond the `Receipt` being converted, so a further split into more methods would just be loose parameters again. Call sites: `RpcReceipt::from(&r).0`. |
| `take_victim_key` | `receipt_cache.rs` | Inlined into `ReceiptCache::evict_if_full`, the one caller, instead of kept as a same-shape method. A method taking the same `&DashMap<K, V>` parameter it always took would be a free function with an unused `&self` in disguise (Phase A's own R11 pass removed exactly that shape elsewhere); `evict_if_full` is already `&self` and is where the eviction policy belongs. **Caught a self-introduced deadlock while gating this pass**: the first inlined version put `map.iter().next().map(\|e\| *e.key())` directly in the `if let` condition (`if len >= cap && let Some(key) = map.iter()... { map.remove(&key) }`). Rust extends a condition's temporaries to the end of the `if let` body, so the `Iter`'s shard read guard was still live when `map.remove` asked that same shard for its write guard — a self-deadlock on one thread, exactly the failure mode the original two-function split existed to avoid (see the doc comment `take_victim_key` carried in Phase A). `insert_past_capacity_evicts_without_deadlock`, a regression test for exactly this, hung indefinitely and caught it. Fixed by computing `victim` in its own `let` statement, ending that statement (and dropping the iterator) before the `if let Some(key) = victim { map.remove(&key) }` that follows; verified with the same test (0.01s, passing) and a full crate test run. |
| `drain_broadcast` | `proxy/mod.rs`, `proxy/watchers.rs` | New `struct BroadcastWatcher<T> { rx: broadcast::Receiver<T> }` with `fn new(rx)` and `async fn run<F: AsyncFnMut(T)>(self, f)`. All five watcher-spawn sites in `watchers.rs` now read `tokio::spawn(BroadcastWatcher::new(rx).run(async move |x| {..}))`. |
| `build_config`, `open_aeron_side`, `spawn_cluster_watermark`, `shutdown` | `bin/kardamom-ingress/main.rs` | New `struct IngressService { args, log_cfg, file_cfg, stop }` holds the first three as methods (`build_config(&self)`, `open_aeron_side(&self)`, `spawn_cluster_watermark(&self, subscription)`), plus `run(self) -> Result<RunningIngress>`. `shutdown` became `RunningIngress::shutdown(self)` (see the FIX above). `main` is now `IngressService::new(args, resolved, file_cfg).run().await?; running.shutdown().await;`. |

Beyond the named list, found while working the crate:

| target | file | what changed |
|---|---|---|
| `lookup`, `lookup_weak`, `remove_slot`, `set_queue_depth` | `pending/mod.rs` | `type PendingMap = Arc<DashMap<..>>` became `struct PendingMap(Arc<Index>)` (a `type Index = DashMap<..>` alias, to keep `clippy::type_complexity` quiet) with all four as methods, plus `new`/`insert`/`len`. |
| `answer_from_cache` | `proxy/submit.rs` | Was `Self::answer_from_cache(&v)`, an `IngressProxy`-scoped associated fn taking `v: &ValidatedSubmission` as its only real input. Moved to `impl ValidatedSubmission { fn answer_from_cache(&self) }`; call sites are `v.answer_from_cache()`. |
| `flush_loop` | `sig_verify.rs` | See R16 below — this is also an R15 change (a `FlushLoop` struct, not a free function taking four loose parameters). |

### R16: no nested loops

New rule: prefer iterators; if a loop must nest, the inner loop becomes a
helper method.

| target | file | what changed |
|---|---|---|
| `flush_loop`'s inner `select!` loop (named in the review) | `sig_verify.rs` | `flush_loop(rx, depth, flush_window, parallelism)` became `struct FlushLoop { rx, depth, flush_window, parallelism }` with `async fn run(self)` (the outer batch loop) and `async fn fill_until_deadline(&mut self, buf)` (the inner racing-select loop, no longer textually nested inside `run`'s loop body). This is also where the R12 `checked_add` fix on the deadline landed (see R12 below). |
| triple-nested `for threads { thread::scope(\|sc\| for w { for i {..} }) }` | `tests/stage_costs.rs` | Added `fn worker_range(worker, threads, len) -> Range<usize>` and `fn recover_range(envs, raws, range)`. The `for w in 0..threads { sc.spawn(..) }` became `(0..threads).for_each(\|w\| { .. sc.spawn(move \|\| recover_range(envs, raws, range)) })`. Only the outer `for threads in [1, 2, 4]` remains a `for` loop; its body has no nested loop syntax left. |
| `for n in sizes { .. for (env,raw,addr) in cases { spawn } .. for h in handles { await } }`, twice | `tests/sig_verify_batch.rs` (`every_caller_gets_its_own_sender`, `fanning_out_a_full_ring_scales_with_cores`) | Both inner `for`-spawn / `for`-await pairs became `.into_iter().map(..).collect()` (spawn) + `futures::future::join_all(handles).await.into_iter().for_each(..)` (await). Only the outer `for n`/`for parallelism` loop remains. |
| nested spawn/submit/collect loops inside `for m in [..]` / `for replica in 0..K` | `tests/routing_test.rs`, `tests/end_to_end_test.rs`, `tests/replicated_cluster_test.rs` | **Done, on top of the `test_support` consolidation (below).** `routing_test.rs`'s fake-executor loop (`for mut rx in rx_vec.drain(..) { ... while let Some(envelope) = rx.recv().await { ... } }`, nested inside `for m in [...]`) is gone, replaced by one call, `spawn_fake_executor(&mock, rx_vec)`; the deferred nested loop no longer exists. `end_to_end_test.rs`'s matching loop, in `one_hundred_txs_route_and_receive_receipts`, is the same replacement; its other four tests' `loop { let s = PrivateKeySigner::random(); if partition_for(...) == 0 { break s; } }` blocks became `signer_for_shard(0, 2)` calls (not nested loops themselves, but the same duplicated shape the R14 row below also removes). `replicated_cluster_test.rs`'s `Cluster`/`FakeExec` keeps its own bespoke per-shard drain task — its exactly-once, multi-term-id bookkeeping does not match `spawn_fake_executor`'s single-shared-position-stream contract, so forcing it in would change behavior — but its `nonce_of` and `signer_for_shard` are shared now, and it was never a nested-loop site itself (`Cluster::start`'s one `for` over shards, each spawning one task, has no *inner* loop in its own body). |
| `PARALLEL_THRESHOLD`-guarded worker loop, `process_batch`'s blocking-pool workers | `sig_verify.rs` | Reviewed: the `(0..workers).map(..).collect()` fan-out and each worker's `loop { cursor.lock_ignore_poison().next() .. }` are two independent loops (one spawns tasks, the other runs inside a spawned task), not one nested inside the other's body. No change needed. |

### R12: safe arithmetic

16 rows: 4 FIX, 1 FIX (opposite mistake), 11 PROVEN.

| file:line (Phase A numbering) | verdict | what changed |
|---|---|---|
| `routing.rs:14`, `(leading % m as u64) as u32` | FIX | `m: NonZeroU32` (see REVERSED above); `u64::from(m.get())`. |
| `main.rs:228`, `args.shards as u8` | FIX | Resolved by R13: `Args::shards` is `NonZeroU8` end to end, so there is no `as u8` cast left at either call site. |
| `main.rs:242`, `LiveIngressPublication::open(&rt, &channels, args.shards as u8)` | FIX | Same as above. |
| `binary.rs:124`, `sock.write_all(&(payload.len() as u32).to_be_bytes())` | FIX | Phase A had already changed this to `u32::try_from(payload.len()).unwrap_or(u32::MAX)`, a silent-saturate that would write a corrupted (too-small) length header and desync the wire framing for an over-`u32::MAX` payload. Phase C changed it to propagate a real `io::Error` (`ErrorKind::InvalidInput`) instead, since `write_reply` already returns `io::Result`. |
| `sig_verify.rs:105`, `let deadline = Instant::now() + flush_window;` | FIX | `Instant::now().checked_add(flush_window)`; on `None` (never reached with the 50µs default, the only producer today), `fill_until_deadline` returns immediately instead of panicking or waiting out a window that could never elapse. Lives in `FlushLoop::fill_until_deadline` now (see R16). |
| `sig_verify.rs:89`, `let parallelism = parallelism.max(1);` | FIX (opposite mistake) | `with_parallelism`'s `parallelism` parameter is `NonZeroUsize`; the silent clamp is gone. Resolved by R13 (below). |
| `binary.rs:97`, `let len = u32::from_be_bytes(len_buf) as usize;` | PROVEN, unchanged | Still a widening cast, still bounded by `MAX_FRAME_BYTES` immediately after. |
| `proxy/mod.rs`, `pack_correlation_id`'s `u64::from(ingress_id) << 48 \| ..` | PROVEN, unchanged | Line shifted (now inside `pack_correlation_id`, unchanged body) but the widening-then-mask reasoning is identical. |
| `proxy/mod.rs`, `ingress_id_of`'s `(correlation_id >> 48) as u16` | PROVEN, unchanged | |
| `proxy/mod.rs`, `self.correlation_seq.fetch_add(1, Ordering::Relaxed)` | PROVEN, unchanged | |
| `proxy/submit.rs`, `partition_for(v.sender, ..) as usize` | PROVEN, unchanged | Now reads `self.partition_count_m` (see REVERSED) instead of `self.cfg.partition_count_m`; the widening-cast reasoning on the result is unaffected. |
| `cluster.rs:83`, `BPosition::from_index(count - 1)` | PROVEN, unchanged | The `count == 0` early return above it is untouched (R13 also keeps this row as-is; see below). |
| `pending/mod.rs`, `metrics::gauge!(..).set(map.len() as f64)` | PROVEN, unchanged | Now `self.0.len() as f64` inside `PendingMap::set_queue_depth`; same bound (`pending_shed_depth`, default 16,384). |
| `pending/mod.rs`, `park_seq.fetch_add(1, Ordering::Relaxed)` | PROVEN, unchanged | |
| `json_rpc.rs`, `log_index: Some(log_index as u64)` | PROVEN, unchanged | Now inside `RpcReceipt::from`'s inlined log-mapping (see R15); still a widening `usize` to `u64` cast. |
| `aeron_adapters.rs`, `Vec::with_capacity(shards as usize)` | PROVEN, no longer present | Phase A's R10 rewrite of `LiveIngressPublication::open` replaced the manual `Vec::with_capacity` + push loop with `(0..shards.get()).map(..).collect()`, which has no explicit cast of this shape left. Noted as superseded rather than re-verified in place. |

### R13: non-zero types

15 rows: 1 DEFECT, 11 FIX, 3 keep-as-is.

| file:line (Phase A numbering) | verdict | what changed |
|---|---|---|
| `routing.rs:11`, `debug_assert!(m > 0, ..)` | DEFECT, FIX | See REVERSED above: `NonZeroU32`, parsed at the `--shards` clap arg (`NonZeroU8`, widened once) and re-derived once in `IngressProxy::new`. |
| `config.rs:22`, `pub partition_count_m: u32` | FIX, with a documented deviation | **Stays `u32`** on `IngressConfig` (bench carve-out, see REVERSED above). The boundary is enforced one layer up: `NonZeroU8` at the CLI, `NonZeroU32` stored on `IngressProxy`. |
| `main.rs:75`, `shards: u32` | FIX | `Args::shards: NonZeroU8`, `#[arg(long, default_value = "8")]` (`default_value_t` does not typecheck against a `NonZero*` field; `default_value` does, since `NonZeroU8: FromStr`). |
| `main.rs:181`, `partition_count_m: args.shards` | FIX | `IngressService::build_config`: `partition_count_m: u32::from(args.shards.get())`. |
| `aeron_adapters.rs:43`, `shards: u8` (`LiveIngressPublication::open`) | FIX | `shards: NonZeroU8`; `(0..shards)` became `(0..shards.get())`. |
| `recorders.rs:42`, `shards: u8` (`spawn_tx_data_recorders`) | FIX | Same change, same reasoning. |
| `sig_verify.rs:88`, `assert!(depth > 0);` | FIX, already done in Phase A | `depth: NonZeroUsize` since Phase A's R9 pass; verified still in place, no Phase C change needed. |
| `sig_verify.rs:89`, `let parallelism = parallelism.max(1);` | FIX | `with_parallelism(depth, flush_window, parallelism: NonZeroUsize)`; the clamp is gone. Four `tests/sig_verify_batch.rs` call sites updated to pass `NonZeroUsize::new(n).unwrap()` instead of a bare integer. |
| `sig_verify.rs:82`, `.map(\|n\| n.get().clamp(1, 8))` | FIX | `BatchVerifier::new`: `std::thread::available_parallelism().map_or(NonZeroUsize::MIN, \|n\| n.min(nonzero!(8usize)))` — keeps `NonZeroUsize` through the `min`, no `.get()`/re-wrap round trip. `tests/sig_verify_batch.rs`'s own `available_parallelism().map_or(1, \|n\| n.get().min(8))` (a second copy of the same pattern, not in the audit row but the same shape) was fixed the same way. |
| `receipt_cache.rs:35`, `assert!(capacity > 0);` | FIX, already done in Phase A | `capacity: NonZeroUsize` since Phase A's R9 pass; verified. |
| `seen_receipts.rs:46`, `assert!(capacity > 0, ..)` | FIX, already done in Phase A | Verified. |
| `tx_error_dedup.rs:97`, `assert!(capacity > 0, ..)` | FIX, already done in Phase A | Verified. |
| `config.rs:42`, `pub chain_id: u64` | FIX, with a documented deviation | **Stays `u64`** on `IngressConfig`, same carve-out reasoning as `partition_count_m`: `crates/bench/src/harness/inprocess.rs:71` sets `chain_id` (a `u64` parameter) via struct-update shorthand in an `IngressConfig { .. }` literal. The boundary moved to the CLI: `Args::chain_id: NonZeroU64`, `#[arg(long, default_value = "1")]`, `chain_id: args.chain_id.get()` in `build_config`. |
| `interop-feed/src/lib.rs:105`, `pub origin_chain_id: u64` (also `dest_chain_id`, `AttestationDto::chain_id`) | **Deferred to Phase B** | Cross-crate: `crates/validator` and `crates/da_watcher` both consume `kardamom-interop-feed`'s DTOs directly (`crates/validator/src/interop/serve.rs`, `crates/da_watcher/src/interop/feed.rs`). The row's own boundary is "at the wire decode," i.e. a type change on `OutboxMessageDto`/`AttestationDto`'s public fields — exactly the kind of cross-crate signature change Phase A's rules defer to Phase B. No `FeedDecodeError` variant was added, since no decode-time rejection exists yet to reject into. |
| `cluster.rs:80`, `if count == 0 {` | Keep as is | Verified unchanged: a "no durable position yet" sentinel guarding `count - 1`, not a divisor. |
| `config.rs:64`, `pub pending_shed_depth: usize` | Keep as is | Verified unchanged: 0 is a documented test hook (shed everything), not a non-zero value. |
| `config.rs:31`, `pub rate_limit_per_ip_per_sec: NonZeroU32` | Already correct | Verified unchanged; still no CLI flag or TOML field for either rate-limit field (noted, not this rule's scope). |

### R14: duplicated shape

Every row whose sites all live in `crates/ingress` or `crates/interop-feed`.
Cross-crate rows, and rows entangled with a deferred cross-crate row, are
listed as deferred with their reason.

**Production, done:**

| row | file(s) | what changed |
|---|---|---|
| Each watcher clones its captured `Arc`s a second time inside the closure (the row proposed `spawn_broadcast_watcher<T, C, F, Fut>(rx, ctx, f)`) | `proxy/watchers.rs`, `proxy/mod.rs` | **Already resolved in Phase A**, before this row was read: the R5 pass replaced `spawn_broadcast_watcher<T,F,Fut>` (a `Fn(C, T) -> Fut` taking a separate `ctx` parameter, which needed the double clone this row flags) with `drain_broadcast<T, F: AsyncFnMut(T)>`, a native async closure that captures its state directly with one `move`. Adding a `ctx: C` parameter back, as this row's helper signature proposes, would reintroduce the double clone and contradict R15 (loose parameters instead of struct/closure state). Phase C's R15 pass renamed `drain_broadcast` to `BroadcastWatcher::run` (see the R15 table); the single-clone-per-watcher shape is unchanged. |
| A `for _ in 0..shards` loop fills two `Vec`s that `unzip` gives directly | `channels.rs:108` (`MockChannels::new`) | **Already resolved in Phase A**: `(0..shards).map(\|_\| mpsc::unbounded_channel()).unzip()` was already in place; verified unchanged. |
| Three near-identical `PumpSource` impls | `aeron_adapters.rs` | `macro_rules! impl_pump_source!($handle, $item)`, used for `FsyncWatermarkSubscriberHandle`, `TxReceiptsBoundarySubscriberHandle`, `TxErrorsSubscriberHandle`. The generic `UnboundedReceiver` impl above it was already distinct (different `Self` shape) and is untouched. |
| TCP/UDS accept-then-spawn-then-handle loops | `binary.rs` | `trait Accept { type Stream; async fn accept_one(&self) -> io::Result<(Self::Stream, IpAddr)>; }`, implemented for `TcpListener` and `UnixListener`; one shared `async fn accept_loop<L: Accept, P, S>(listener, proxy)`. `spawn_tcp_listener`/`spawn_uds_listener` now only bind and call it. |
| `purge`/`insert`'s repeated "pop, compare timestamp, remove" block | `tx_error_dedup.rs` | `fn remove_if_current(&mut self, key, at: Instant)` on `impl Inner`, called from both `purge` and `insert`. |
| `PEER_ADDR`-with-fallback, duplicated across two handlers | `json_rpc.rs` | `fn client_ip() -> IpAddr`, used by `send_raw_transaction` and `send_raw_transaction_async`. |
| `lock().unwrap_or_else(PoisonError::into_inner)`, 3 sites | `sig_verify.rs` (cursor lock), `pending/mod.rs` (`parked` lock, 2 sites) | New `crates/ingress/src/sync_util.rs`: `trait LockIgnorePoison<T> { fn lock_ignore_poison(&self) -> MutexGuard<'_, T>; }` implemented for `std::sync::Mutex<T>` — a trait method, not a free function, so this does not reopen R15. All three sites call `.lock_ignore_poison()`. |
| `IngressError::Internal(format!("{ctx}: {e}"))`, repeated (the row proposed `pub fn internal<E: Display>(ctx: &'static str) -> impl FnOnce(E) -> Self`, curried for point-free `.map_err(IngressError::internal("ctx"))`) | `json_rpc.rs` (3), `aeron_adapters.rs` (6), `proxy/mod.rs` (1) | Implemented as a plain two-argument method instead of the curried form: `impl IngressError { fn internal(ctx: impl Display, e: impl Display) -> Self }`. Every call site still needs a one-line closure (`.map_err(\|e\| IngressError::internal("ctx", e))`), so this is a smaller ergonomic win than the curried shape, but it is a more ordinary method signature; noted as a deliberate deviation from the row's suggested shape, same intent. Used at 10 of the 13 total `IngressError::Internal(format!(..))` sites in the crate. The other 3 do not fit the `"ctx: {e}"` shape: `aeron_adapters.rs`'s `.ok_or_else(\|\| ..."shard {shard} out of range")` wraps no underlying error; `submit.rs`'s receipt-identity error interpolates four values, not one context plus one error; `json_rpc.rs`'s two `"deferred to S6 state writer"` errors carry no wrapped error either. Left as plain `IngressError::Internal(..)` literals. |
| Manual 3-field `Callback`/`CallbackDto` copies, 2 sites | `interop-feed/src/lib.rs` | `impl From<Callback> for CallbackDto` and the reverse; `from_outbox_message`/`into_outbox_message` now read `m.callback.map(CallbackDto::from)` / `self.callback.map(Callback::from)`. |

**Production, deferred:**

| row | file(s) | reason |
|---|---|---|
| `SubscriptionBuses` shared struct behind `impl<T: AsRef<SubscriptionBuses>> IngressSubscription for T` | `channels.rs`, `aeron_adapters.rs` | The real duplication is between `LiveIngressSubscription`'s and `MockChannels`' `IngressSubscription` impls (five near-identical `subscribe_x(&self) -> broadcast::Receiver<X> { self.x.subscribe() }` methods, once per type). Applying the shared struct only to `LiveIngressSubscription`, as suggested, to avoid touching `MockChannels`, removes none of that duplication, since nothing else would use it. `MockChannels`' individual bus fields (`receipt_bus`, `watermark_bus`, ...) are read directly, cross-crate, by `crates/bench/tests/alloc_profile_ingress.rs`. Doing this properly needs `bench`'s coordination — Phase B. |
| `lag_or_break` macro | `json_rpc.rs` | Reviewed, judged not worth it: after the R15 `ReceiptSubscription` move, the duplication is two one-line `Err(RecvError::Lagged(n)) => ReceiptEvent::Lagged { skipped: n }` match arms, one per feed, inside one `tokio::select!`. A macro for two one-line, already-adjacent arms would obscure the `select!`'s control flow more than it would save. |
| `main.rs`'s `ObsArgs`-shaped duplication | `bin/kardamom-ingress/main.rs` | Cross-crate (`kardamom-obs`); deferred per the coordinator's instruction to defer cross-crate rows. |
| `docker_e2e.rs` + sequencer's `e2e_docker.rs` → `kardamom_log::testing` | (test file, not run) | Cross-crate (`kardamom-log`), and `docker_e2e.rs` itself is excluded from this pass's `cargo test` run under the "no Docker tests" rule; deferred. |
| `free_port`/`scrape` → `kardamom-obs` | test helpers | Cross-crate (`kardamom-obs`); deferred. |

**Tests, done:**

| row | file(s) | what changed |
|---|---|---|
| The same timeout-then-unwrap-three-times chain reads one subscription frame, 3 sites | `tests/receipt_subscription_test.rs` | `async fn next_event(sub: &mut Subscription<Value>, what: &str) -> Value`, local to that test file, replacing each `tokio::time::timeout(..).await.expect(..).unwrap().unwrap()` with `next_event(&mut sub, "..").await`. This is unrelated to `json_rpc.rs`'s own (now `ReceiptSubscription::next_event`, an R15 change) — the audit's row is a same-named but distinct, test-local helper. |
| `test_support` module consolidation: `sign_and_encode`/`sign_legacy`/`sign_legacy_tx`/`sign_legacy_with_gas`/`sign_eip4844` (deletes `tests/common/mod.rs`), `nonce_of`, `receipt_for`/`spawn_fake_executor`, `signer_for_shard`, `receipt`/`pos` builders, `dummy_receipt`, `start_test_server`/`http_client` | new `crates/ingress/src/test_support.rs`, behind the new `test-support` Cargo feature | **Un-deferred and landed.** `crates/ingress/Cargo.toml`: added `[features] test-support = ["dep:alloy-signer-local", "dep:k256", "jsonrpsee/http-client"]`; moved `alloy-signer-local` and `k256` from `[dev-dependencies]` to `optional = true` regular `[dependencies]` (no new dependency crate — both were already dev-deps); added the self dev-dependency `kardamom-ingress = { path = ".", features = ["test-support"] }` so `tests/` and `benches/` see the module. `lib.rs`: `#[cfg(any(test, feature = "test-support"))] pub mod test_support;`. `tests/common/mod.rs` is deleted. Every `mod common;` / `common::sign_*` call site (`pending_receipts_test.rs`, `protocol_limits_test.rs`, `batched_sig_verify_test.rs`, `routing_test.rs`, `end_to_end_test.rs`, `replicated_cluster_test.rs`, `receipt_subscription_test.rs`) now imports from `kardamom_ingress::test_support`. `sig_verify.rs`'s own `#[cfg(test)] mod test_support { signed_legacy_envelope() }` is deleted; its five call sites use `crate::test_support::sign_legacy_tx(&PrivateKeySigner::random(), 0)`. `receipt_cache.rs`'s `make_receipt` and `pending/tests.rs`'s `dummy_receipt`/`pos` are deleted, replaced by `crate::test_support::{receipt, dummy_receipt, pos}`. `receipt_subscription_test.rs`'s own `start_stack`/`receipt_for` are deleted, replaced by `test_support::{start_test_server, receipt}`; `json_rpc.rs`'s own test does the same. `benches/latency.rs` and `benches/throughput.rs` keep their own per-shard drain-task structure (`term_id: shard index`, not `spawn_fake_executor`'s shared single-stream `term_id: 0` — a real behavioral difference, not just a naming one; see the `binary.rs`-test row below for the one site that similarly did not fit), but their `sign`/`nonce_of` calls and the fake-receipt struct literal now read `test_support::{sign_legacy, receipt_for}` — `receipt_for` computes `nonce_of` internally, so both benches' explicit `nonce_of` call is also gone. All 91 tests across 17 binaries still pass (verified below). |
| `binary.rs`'s own test (`empty_rlp_returns_decode_error`) | `crates/ingress/src/binary.rs` | **Reviewed, not migrated.** The audit's `start_test_server`/`http_client` row names `binary.rs:148-160` as a site, but the current test drives `handle_connection` directly over a raw `TcpListener`, with no `start_jsonrpc_server` call at all — it does not return the same `(MockChannels, rx, SocketAddr, ServerHandle)` shape `start_test_server` gives, so forcing it in would not be a like-for-like replacement. Left as is. |

**Tests, deferred (cross-crate, coordinator-owned):**

| row | file(s) | reason |
|---|---|---|
| `spawn_fake_executor`'s bench call site | `crates/bench/tests/alloc_profile_ingress.rs` | Cross-crate. The coordinator asked that this stay untouched here and be handled at merge, together with the `SubscriptionBuses`/`MockChannels` row above that also reaches into this same file's `mock.receipt_bus`/`mock.watermark_bus` field access. |

**Tests, keep as is:**

| row | file(s) | reason |
|---|---|---|
| `submit_raw("127.0.0.1".parse().unwrap(), ..)`, repeated | test files | dry-ingress.md's own row keeps this as is (an optional `const LOCAL` would only help clarity); no action taken. |

**Prior-audit KEEP rows, verified unchanged:** the three bounded caches
(`ReceiptCache`, `SeenReceipts`, `TxErrorDedup`), the two `IngressError`
classifications, interop-feed's cursor types, and the two watermark watchers
(`spawn_quorum_watermark_watcher`, `spawn_local_fsync_watermark_watcher`) —
all still structurally distinct for the reasons `dry-ingress.md` gives; no
Phase C change touches them beyond `BroadcastWatcher` (R15), which preserves
their behavior exactly.

### Gates, rerun after Phase C

- `cargo clippy -p kardamom-ingress -p kardamom-interop-feed --all-targets --features binary-protocol -- -D warnings`: clean.
- `cargo clippy .. -- -W clippy::pedantic`, filtered to this group's own files: zero warnings.
- `cargo test -p kardamom-ingress -p kardamom-interop-feed --features binary-protocol` (excluding binary-spawning, Docker, and `full-pipeline-e2e`/chaos/cluster categories, per the standing rule): all green — 17 test binaries, 91 tests, 0 failed. This run caught a real, self-introduced deadlock first (see the `take_victim_key` row in R15): `receipt_cache::tests::insert_past_capacity_evicts_without_deadlock` hung indefinitely under the first inlined version of `evict_if_full`. Fixed and reverified before this final green run.
- `cargo fmt -p kardamom-ingress -p kardamom-interop-feed --check`: clean.
- `cargo check --workspace --all-targets` (new gate): clean for every package except `kardamom-sequencer`, which failed intermittently across three runs of this gate over the course of Phase C (varying errors: an `E0603` private-module import in `tests/state_proptest.rs`, and an `E0308` type mismatch in `src/outbound/cluster.rs`) — consistent with the sequencer group's own background agent actively editing that crate concurrently on this shared machine. `cargo check -p kardamom-sequencer --all-targets` alone, and `cargo check --workspace --all-targets --exclude kardamom-sequencer`, were both clean at different points in the same session. Nothing in either failure references `kardamom-ingress` or `kardamom-interop-feed`; both are inside `crates/sequencer`'s own files, which this group does not touch. `kardamom-validator`, `kardamom-da-watcher`, and `kardamom-bench` — the three crates that read this group's public surface — check clean, which is what this gate is actually verifying for this diff.

### Follow-up round: `RunningIngress` field names, `test_support`, Phase B note

Coordinator review of the Phase C diff: accepted the `IngressService`/`RunningIngress` shape, `FlushLoop`, the `Accept` trait, and the deadlock catch. Three follow-ups:

1. **`RunningIngress`'s two `#[allow(dead_code)]` fields renamed to `_rt` and `_cluster_guard`.** A leading underscore silences the lint on its own, so the attributes are gone; the field-order doc comment (declaration order controls drop order — see the FIX section above) is kept, updated to the new names, on both the field declarations and `IngressService::run`'s construction site and `RunningIngress::shutdown`'s doc.
2. **The `test_support` consolidation is un-deferred and landed** (see the R14 and R16 tables above, now marked Done): every dry-ingress.md test row (`sign_*`, `nonce_of`, `receipt_for`/`spawn_fake_executor`, `signer_for_shard`, `receipt`/`pos`/`dummy_receipt`, `start_test_server`/`http_client`) lives in a new `crates/ingress/src/test_support.rs`, gated by a new `test-support` Cargo feature; `alloy-signer-local` and `k256` moved from `[dev-dependencies]` to `optional = true` regular `[dependencies]` (no new dependency crate); a self dev-dependency (`kardamom-ingress = { path = ".", features = ["test-support"] }`) turns the feature on for this crate's own `tests/`/`benches/`. `tests/common/mod.rs` is deleted. The three previously-deferred R16 test files (`routing_test.rs`, `end_to_end_test.rs`, `replicated_cluster_test.rs`) were then restructured on top of the consolidated helpers, as asked — see the R16 table's updated row. The cross-crate bench call site (`crates/bench/tests/alloc_profile_ingress.rs`) was left untouched, for the coordinator to handle at merge. All 91 tests still pass after this round (reran the full suite).
3. **`IngressProxy::new`'s `.expect("partition_count_m must be non-zero")` stays, unchanged**, pending the coordinator's Phase B follow-up (see the new R13 row under "Deferred to Phase B" above): `IngressConfig.partition_count_m` and `chain_id` become `NonZeroU32`/`NonZeroU64`, together with the `crates/bench` call sites, at merge.

**Gates, rerun after this round:** `cargo clippy -- -D warnings`: clean. `cargo clippy -- -W clippy::pedantic`, this group's own files: zero warnings (after adding `#[must_use]` and `# Panics` docs `test_support.rs` needed once its functions became real pedantic-scrutinized public API). `cargo fmt --check`: clean. `cargo test`: 17 binaries, 91 tests, 0 failed — unchanged from before this round, confirming the `test_support` migration and the R16 restructuring preserved behavior exactly. `cargo check --workspace --all-targets`: intermittent during this round in a new way — `crates/ingress/src/cluster.rs:50`'s `wire::decode_egress(&bytes)` call (code this group has never touched, in either Phase A or C) failed to resolve on some runs and compiled clean on others, seconds apart, with the errors pointing at `crates/cluster-adapter/src/wire/egress.rs` and `crates/cluster-adapter/src/gateway.rs` — a shared dependency neither `crates/ingress` nor `crates/interop-feed` owns. `kardamom-engine` and `kardamom-sequencer` showed the identical `decode_egress`/`encode_egress_record` breakage in the same runs. Repeated, isolated checks of this group's own packages (`cargo check -p kardamom-ingress -p kardamom-interop-feed --all-targets`, and `-p kardamom-validator -p kardamom-da-watcher -p kardamom-bench --all-targets` for the three dependent crates) were clean every time, including immediately after a workspace-wide run had just failed on the `cluster-adapter` symbol — consistent with another group actively editing `crates/cluster-adapter`'s `wire` module concurrently on this shared machine, not with anything in this diff.

### Second follow-up round: named structs, `NonZeroU32`, a shared `TxLegacy` builder

Coordinator review of the first follow-up round: accepted. Six small R11/R12/R13/R14/R15 items in `test_support.rs` and two of its test-file call sites:

1. **`start_test_server` and `sign_legacy_tx` return named structs instead of tuples** (R11/R15): `TestServer { mock, shard_rx, addr, handle }` and `SignedTx { env, raw, sender }`, both `pub` fields, both in `test_support.rs`. Every call site (`json_rpc.rs`'s own test, `sig_verify.rs`'s `tests`/`batch_tests`, `tests/batched_sig_verify_test.rs`, `tests/receipt_subscription_test.rs`) destructures the struct by name (`let TestServer { addr, .. } = ..`, `let SignedTx { env, raw, .. } = ..`) instead of by tuple position.
2. **`signer_for_shard` takes `m: NonZeroU32` directly** (R13), instead of a `u32` it wrapped internally with `.expect("m is non-zero")`. `tests/end_to_end_test.rs` (four call sites, all `signer_for_shard(0, 2)`) gained a shared `const TWO_SHARDS: NonZeroU32 = NonZeroU32::new(2).unwrap();`; `tests/replicated_cluster_test.rs`'s one call site converts its existing `m: u32` variable at the call: `signer_for_shard(0, NonZeroU32::new(m).expect("m is non-zero"))`.
3. **`sign_legacy_tx` and `sign_legacy_with_gas` shared their `TxLegacy { .. }` literal** (R14): a new private `fn legacy_tx(nonce: u64, gas_limit: u64) -> TxLegacy` feeds both; `sign_legacy_tx` calls it with the fixed `21_000` default gas limit, `sign_legacy_with_gas` with its caller-supplied one.
4. **Two casts in `test_support.rs` made explicit** (R12): `start_test_server`'s `cfg.partition_count_m as usize` is now `usize::try_from(cfg.partition_count_m).expect("u32 fits usize")`; `spawn_fake_executor`'s `fetch_add(1, ..) + 1` (an unchecked `Add` that can panic-on-overflow in debug or wrap in release) is now `fetch_add(1, ..).checked_add(1).expect("fixture positions fit i32")`.
5. **`tests/receipt_subscription_test.rs`'s three-times-repeated `IngressConfig { chain_id: 1, ..IngressConfig::default() }`** became one `fn chain_one() -> IngressConfig` (R14); the fourth call site, which also sets `pending_shed_depth: 0`, uses `IngressConfig { pending_shed_depth: 0, ..chain_one() }`.
6. **`tests/stage_costs.rs`'s file-level `#![allow(clippy::cast_precision_loss)]`** now carries its explanation as `reason = "..."` on the attribute itself (R11, the `lint_reasons` attribute form), instead of as a preceding module-doc paragraph; the old paragraph is deleted.

**Gates, rerun after this round:** `cargo clippy -- -D warnings`: clean. `cargo clippy -- -W clippy::pedantic`, this group's own files: zero warnings. `cargo fmt --check`: clean. `cargo test -p kardamom-ingress -p kardamom-interop-feed --features binary-protocol`: 17 binaries, 91 tests, 0 failed — unchanged, confirming the struct/NonZero/shared-builder changes preserved behavior exactly.

## Round B (group C: da_watcher, interop-feed, cluster-adapter, cluster-client, sequencer, log, ingress, obs)

Item 5 of this round's brief: `IngressConfig.partition_count_m`/`chain_id` parsed once at the
config boundary as `NonZeroU32`/`NonZeroU64`.

- `crates/ingress/src/config.rs`: `IngressConfig.partition_count_m: u32` →
  `NonZeroU32`; `.chain_id: u64` → `NonZeroU64` (doc notes EIP-155 forbids 0). `Default` uses
  `nonzero_ext::nonzero!(8u32)` / `nonzero!(1u64)`, matching the file's existing style for its
  other `NonZero*` fields.
- `crates/ingress/src/proxy/mod.rs`: `IngressProxy::new`'s `NonZeroU32::new(cfg
  .partition_count_m).expect("must be non-zero")` (an R9 defensive check on a value the type
  now rules out) is gone; `partition_count_m: cfg.partition_count_m` directly. The doc
  comments that justified the old plain-`u32` field (for `crates/bench`'s literals) are
  deleted, since the field itself no longer needs that justification.
- `crates/ingress/src/json_rpc.rs`: `chain_id()` RPC handler reads `.chain_id.get()`; its own
  test's `IngressConfig { chain_id: 31337, .. }` literal is `NonZeroU64::new(31337).unwrap()`.
- `crates/ingress/src/test_support.rs`: `start_test_server`'s `usize::try_from(cfg
  .partition_count_m)` is `usize::try_from(cfg.partition_count_m.get())`.
- `crates/ingress/src/bin/kardamom-ingress/main.rs`: `build_config` passes
  `NonZeroU32::from(args.shards)` (widening `NonZeroU8` → `NonZeroU32`, no `.get()`
  round-trip) and `args.chain_id` directly, instead of `u32::from(args.shards.get())` /
  `args.chain_id.get()`; the `shards = cfg.partition_count_m` log field is `cfg
  .partition_count_m.get()` (`tracing`'s `Value` impls do not cover `NonZero*`).

Item 4's `DeliverFn` removal (see `status-log.md`) rippled into this crate:
`crates/ingress/src/aeron_adapters.rs`'s `TxReceiptsSubscriberHandle::into_receiver()` now
returns a new `kardamom_log::aeron_live::TxReceiptsReceiver`, not a raw tokio
`UnboundedReceiver`. Added one `impl_pump_source!(TxReceiptsReceiver, Receipt)` line next to
the other three; no other change, since the macro already targets exactly this shape
(`recv().await -> Option<(BPosition, Item)>`), and `TxReceiptsReceiver` has it. The generic
blanket `impl<T> PumpSource for UnboundedReceiver<(BPosition, T)>` stays — this crate's own
`pump_fans_out_and_ends_on_close` test builds one directly.

### Cross-file items this agent could not do (owned elsewhere)

- `crates/ingress/tests/*.rs` (owned by the tests-directory agent, not this group):
  `replicated_cluster_test.rs:151`, `end_to_end_test.rs:29,86,159,243,310`,
  `receipt_subscription_test.rs:30`, `routing_test.rs:22`, `pending_receipts_test.rs:17` all
  build `IngressConfig { partition_count_m: <u32 literal or var>, .. }` /
  `{ chain_id: 1, .. }`; each needs the literal wrapped in `NonZeroU32::new(..).unwrap()` /
  `NonZeroU64::new(..).unwrap()`, or (for a variable) `.try_into().unwrap()`.
- `crates/ingress/benches/latency.rs:24` and `throughput.rs:26`: same
  `partition_count_m: 8` literal, not owned by this group.
- `crates/bench/src/harness/inprocess.rs:71-72` and
  `crates/bench/tests/alloc_profile_ingress.rs:182` (a separate crate, `crates/bench`, not
  owned by this group): `inprocess.rs` builds `IngressConfig { chain_id, partition_count_m:
  shards, .. }` from its own `u64`/`u32` locals; `alloc_profile_ingress.rs` does `MockChannels
  ::new(cfg.partition_count_m as usize)`, which needs `.get() as usize` (or `usize::try_from`).

### Gates

- `cargo clippy -p kardamom-ingress --lib --bins --all-features -- -D warnings -W
  clippy::pedantic -D unreachable_pub`: clean, after also narrowing 10 pre-existing
  `unreachable_pub` items in `pending/mod.rs` (`PendingReceipts` and its methods,
  `PendingWait::await_with_timeout`) to `pub(crate)` — the `pending` module is already
  `pub(crate)`, so nothing outside the crate could reach them; found while running this
  round's gate, not part of the `reaudit-main.md` list.
- `cargo clippy -p kardamom-ingress --all-targets ...`: the remaining failures are all in
  `benches/*.rs` and `tests/*.rs` (both listed above, neither owned by this group).
- `cargo test -p kardamom-ingress --lib --all-features`: 57 pass.
- `cargo fmt -p kardamom-ingress -- --check`: clean.
- Forbidden-pattern grep: prints nothing for `crates/ingress/src/**` outside `tests?/`/
  `test_support`.

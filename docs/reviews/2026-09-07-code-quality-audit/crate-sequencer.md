# sequencer

## Summary

No file in this group is over 500 code lines. The raw hot spots shrink a lot when you
remove blanks and comments: `sequencer.rs` is 346 code lines of 603 raw,
`session_loop.rs` is 356 of 568, and `protocol.rs` is 313 non-test code lines of 579
raw. The dominant problem is comment debt, not size: 26 comments carry refactor
history, removed-feature obituaries, issue tags (`#85`, `F02.1`), or named spec and
review documents. Three comments are now wrong and mislead a reader
(`resync/mod.rs:10` links a deleted method, `state/mod.rs:69` describes a state-DB
lookup that does not exist, `feeds.rs:202` describes a nonce-0 rule the code no longer
applies). The second problem is dead public surface: the whole `metrics::record_*`
free-function family is superseded by `HotMetrics` and unused, `protocol` is a `pub`
module that no other crate touches, and `cluster_ref_publisher` and
`PendingBuffer::lowest_nonce` have no callers. Sync use is clean: every primitive is a
real cross-thread seam and there is no `dyn` and no `Box<dyn Error>` anywhere in the
three crates. Counts: R1 26, R2 11, R3 3, R4 2, R5 24, R6 1, R7 2, R8 16, R9 11,
R10 8.

## R1 comments

| file:line | snippet (max 80 chars) | why | fix |
|---|---|---|---|
| crates/sequencer/src/resync/mod.rs:10 | `- Response ([`should_skip`](ResyncController::should_skip)): while in` | Dangling link. `should_skip` does not exist anywhere in the repo. | Name the real entry point (`floor` plus `Sequencer::proven_executed`). |
| crates/sequencer/src/state/mod.rs:69 | `partition has never seen the sender. The cache-miss hydration path` | Wrong. Says `None` triggers a state-DB lookup. The sequencer holds no state-DB reader; the caller seeds 0. | Rewrite: `None` means the caller must seed a floor. |
| crates/sequencer/src/bin/kardamom-sequencer/feeds.rs:202 | `// Nonce-0 receipts are excluded from floor evidence.` | Wrong. The code forwards every receipt and lets `deposit`/`skip_reason` decide. | Delete. The controller doc already states the live rule. |
| crates/cluster-adapter/src/wire/mod.rs:26 | `block boundary:  [kind:u8 = 2][block_number:u64][end_tx_idx:u64][l2_timestamp:u64]` | Wrong layout. `decode_egress` also reads `l1_origin:u64` at offset 25. | Add `[l1_origin:u64]` to the diagram. |
| crates/cluster-adapter/src/live/session_loop.rs:3 | `struct's fields are the loop-carried state that used to be `run_session` locals` | Pure refactor history. | Delete both sentences. |
| crates/sequencer/src/sequencer.rs:66 | `// Re-export: the shutdown signal lived here before it moved to` | Historical move note. | Keep only "the bin imports `sequencer::Shutdown`". |
| crates/sequencer/src/sequencer.rs:77 | `This struct no longer carries the envelope bytes. The proxy already` | "no longer" is history. | Say what it holds: a ref, not envelope bytes. |
| crates/sequencer/src/sequencer.rs:41 | `An earlier stream-adaptive floor fast-forward was removed, because it` | Removed-feature obituary. | Keep the invariant, drop the history. |
| crates/sequencer/src/sequencer.rs:99 | `docs/agents/sequencer-lag-resync-spec.md). `None` when the binary` | Names a spec document. | State the rule inline. |
| crates/sequencer/src/sequencer.rs:101 | `did not wire the receipts and egress-watermark feeds (tests, IPC dev runs). Behavior is then identical to before resync existed.` | "before resync existed" is history. | Say "resync then does nothing". |
| crates/sequencer/src/sequencer.rs:104 | `allocate (the per-call `counter!` boxing used to be 6 of 10` | Dated measurement plus "used to be". | Say "recording through these handles does not allocate". |
| crates/sequencer/src/sequencer.rs:285 | `"contiguity reject proves commitment; dropping unconfirmed entry (#85)"` | Issue tag in a runtime log line. Same at 304 and 322. | Drop `(#85)`. |
| crates/sequencer/src/sequencer.rs:398 | `// The nonce-floor fast-forward sweep that used to run here was removed.` | Obituary for deleted code. | Delete. `state/mod.rs` already documents the rule. |
| crates/sequencer/src/sequencer.rs:464 | `the publish actions for `flush_drained`. This is split out of `run_once`; the sequence of operations is unchanged.` | Refactor provenance. | Delete the second sentence. |
| crates/sequencer/src/sequencer.rs:498 | `// Two different things surface as `Past`. Conflating them broke the load harness's seq_clean verdict.` | "broke the harness" is history. | State the two cases only. |
| crates/sequencer/src/sequencer.rs:509 | `//   floor proof. Count and report it exactly as before.` | "as before" has no referent. | Say "count and report it". |
| crates/sequencer/src/config.rs:27 | `This field is unused. It is accepted only for config compatibility. It used to bound…` | Legacy-field obituary on a dead field. | Remove the field and the comment (see R8). |
| crates/sequencer/src/config.rs:44 | `docs/agents/sequencer-lag-resync-spec.md. `resync.dedup_capacity`` | Names a spec document. | Keep only the contract sentence. |
| crates/sequencer/src/state/mod.rs:228 | `// NOTE: `fast_forward_stalled` (the stream-adaptive nonce-floor fast-forward) was removed.` | 16-line obituary that ends with a dated review path `docs/reviews/2026-07-17-30-commit-review/…(round 4)`. | Keep the 3-line invariant ("a stalled sender must stall here"), drop the history and the path. |
| crates/sequencer/src/state/mod.rs:213 | `advances the floor. See docs/agents/sequencer-lag-resync-spec.md.` | Spec document by name. | Inline the rule. |
| crates/sequencer/src/pending.rs:10 | `smallest nonce (the old behavior) punches a gap directly in front of the run.` | Contrasts with removed behavior. | State the present rule: the furthest-future nonce loses. |
| crates/sequencer/src/unconfirmed.rs:38 | `The old full-map scan was O(rate times receipt-latency) per iteration` | Describes the replaced implementation. | Keep the complexity claim for the present code only. |
| crates/sequencer/src/epoch.rs:14 | `Unlike the `DepositRef` scheme this replaces, the deposits travel inside the record.` | Compares to a removed scheme. | State that deposits travel inside the record. |
| crates/sequencer/src/outbound/cluster.rs:121 | `function used to discard it. … Drop the returned `LiveEgress` to restore the old discard behavior.` | History plus a spec-doc name. | Say what it returns and why the caller keeps it. |
| crates/sequencer/src/metrics.rs:17 | `// Lag detection and receipt-floor resync. See docs/agents/sequencer-lag-resync-spec.md.` | Spec document by name. | Delete the reference. |
| crates/sequencer/src/bin/kardamom-sequencer/main.rs:219 | `"…coverage of established senders until resync floors catch up (F02.1 re-opened)"` | Internal ticket id in an operator log line. | Drop `(F02.1 re-opened)`. |
| crates/cluster-adapter/src/live/session_loop.rs:260 | `Counting raw channel bytes instead kept the only escape hatch … disarmed` | Describes a fixed bug in the past tense. | Keep the present rule: only session-filtered events feed the watchdog. |
| crates/cluster-adapter/src/live/session_loop.rs:512 | `An unconditional 1ms sleep here added up to 1ms of latency to every sequencer offer` | Historical benchmark of removed code. | Keep the cap rule. |
| crates/cluster-adapter/src/live/session_loop.rs:525 | `The old shape kept `req_rx` in the Select. It slept `wait` on the empty-ready artifact.` | Describes the previous implementation. | Keep the present rule: build the Select from live receivers only. |
| crates/cluster-adapter/src/live/session_loop.rs:559 | `the same static endpoint, those foreign frames used to disarm the` | "used to" history. | State the present close-on-shutdown reason. |
| crates/cluster-adapter/src/live/mod.rs:239 | `An empty or missing section used to show up only as a silently dead session thread` | "used to" history. | Say "`connect` fails startup when either field is empty". |
| crates/cluster-adapter/src/watermark.rs:8 | `replaces the old standalone sealer's archive-recording-position watermark` | Compares to a removed component. | Delete the sentence. |
| crates/cluster-adapter/src/config.rs:19 | `serde ignores a legacy `enabled = true` key in an old config file` | Legacy-compat note. | Keep one line: "unknown keys are ignored". |
| crates/cluster-adapter/src/wire/mod.rs:71 | `cost: each ~75-byte ref used to pay for a full offer round trip.` | "used to" history. | Say "batching amortizes the per-offer round trip". |
| crates/cluster-client/src/session/mod.rs:28 | `the SBE schema version (an earlier value of 5.4.0 conflated the two).` | Records a past mistake. | Keep only "this is the appVersion, not the schema version". |
| crates/cluster-client/src/protocol.rs:293 | `This is mainly for tests and a future in-Rust cluster mock.` | Speculative future work. | Say "test and mock helper". |
| crates/sequencer/src/bin/kardamom-sequencer/adapters.rs:27 | `// try_recv is non-blocking. The Sequencer's run loop handles backoff` | Restates the call name. | Delete the first sentence. |
| crates/cluster-adapter/src/live/session_loop.rs:304 | `self.egress_alive = false; // consumer dropped` | Restates the assignment. | Delete. |

## R2 long methods

| file:line | fn name | approx code lines | helpers to extract |
|---|---|---|---|
| crates/sequencer/src/bin/kardamom-sequencer/main.rs:171 | `main` | 125 | `open_aeron_handles(&args, &channels) -> Handles`; `wire_resync(&cfg, egress, receipts, shutdown) -> (Controller, Tasks)`; `join_all(handles)` for the three `match join_*.await` blocks. |
| crates/cluster-adapter/src/live/mod.rs:260 | `connect_inner` | 68 | `open_egress_subscription(&rt, &cfg) -> Result<Receiver<Vec<u8>>>`; `open_initial_ingress(&rt, &cfg) -> Result<(i32, PubHandle)>`; `spawn_session_thread(...) -> Result<(Arc<AtomicBool>, JoinHandle)>`. |
| crates/sequencer/src/bin/kardamom-sequencer/feeds.rs:258 | `spawn_publish_loops` | 67 | `spawn_main_loop(cfg, subs, pub, resync, shutdown)`; `spawn_origin_pump(name, sub, pub, shutdown)` used twice (the deposit and remote-epoch pumps at 299 and 320 are the same 16 lines). Take an argument-group struct (see R7). |
| crates/sequencer/src/bin/kardamom-sequencer/feeds.rs:63 | `run_egress_watermark_feed` | 65 | `handle_reject_frame(&frame, &reject_tx, partition) -> bool`; `handle_boundary_frame(&frame, &watermark, &mut last_at)`; keep `flag` as a named fn instead of a closure. |
| crates/cluster-adapter/src/wire/egress.rs:106 | `decode_relayed_payload` | 76 | `decode_txref_fields(id, fields)`; `decode_depositref_fields(id, fields)`; `decode_rkyv_body::<T>(fields)` shared by `RT_EPOCH` and `RT_REMOTE_EPOCH` (the aligned-copy block is duplicated). |
| crates/sequencer/src/sequencer.rs:236 | `resync_tick` | 61 | `apply_receipt_drain(&mut self, r)`; `apply_contiguity_rejects(&mut self, r)`; `sweep_confirm_timeouts(&mut self)`. Each maps 1:1 to a comment block already present. |
| crates/sequencer/src/sequencer.rs:466 | `handle_outcome` | 57 | `record_outcome_metrics(&mut self, rc, sender, outcome)`; `collect_publishes(&mut self, rc, sender, actions) -> Vec<...>`. |
| crates/cluster-client/src/session/mod.rs:266 | `on_session_event` | 57 | `event_is_ours(&self, ev) -> bool` (the same ownership test appears in three arms); `on_ok(ev)`; `on_redirect(ev)`; `on_failure(ev)`. |
| crates/cluster-adapter/src/wire/egress.rs:44 | `decode_egress` | 57 | `decode_relayed(buf)`; `decode_boundary(buf)`; `decode_contiguity_reject(buf)`. The replay arms are already one-liners. |
| crates/cluster-client/src/session/mod.rs:168 | `poll_outbound` | 51 | `advance_retry_state(&mut self, now_ms)` (the leading `match &self.state`); `emit_connect(&mut self, now_ms) -> Vec<u8>`; `emit_keep_alive(&mut self, now_ms) -> Option<Vec<u8>>`. |
| crates/sequencer/src/sequencer.rs:546 | `run` | 51 | `pin_to_core(&self)`; `on_step_result(&mut self, r, &mut backoff) -> ControlFlow<Result<(),_>>`. |

Borderline, at exactly 50: `crates/sequencer/src/sequencer.rs:378` `run_once`. Split
lines 405-447 into `decode_ingress(&mut self, channel_a) -> Option<(Address, u64, RefMetadata)>`
and the method drops to about 25.

## R3 large files

| file | code lines | split plan or KEEP (reason) |
|---|---|---|
| crates/sequencer/src/sequencer.rs | 346 (603 raw) | KEEP. Under the 500 threshold, and it is one state machine: `Sequencer` plus its step and loop. Splitting would separate `resync_tick` from the `run_once` ordering contract it depends on. Shrink it with the R2 extractions instead. |
| crates/cluster-adapter/src/live/session_loop.rs | 356 (568 raw) | KEEP. Already the product of one split (`live/mod.rs` holds the seams, `endpoints.rs` the parsing). Every method mutates the same `SessionLoop` fields. If it grows past 500, move `Resend` plus the four resend and watchdog timers into `live/timers.rs`. |
| crates/cluster-client/src/protocol.rs | 313 non-test (456 with tests, 579 raw) | KEEP. It is one SBE codec whose parts share `put_header`, `rd_var`, and `expect`. If a split is wanted later, use `protocol/header.rs` (`MessageHeader`, `expect`, the `rd_*` helpers), `protocol/ingress.rs` (connect, keep-alive, close, `wrap_session_message`), `protocol/egress.rs` (`SessionEvent`, `NewLeaderEvent`, `decode_egress`). |

No other file in the group exceeds 500 code lines. The next largest are
`crates/cluster-client/src/session/mod.rs` (250) and
`crates/cluster-adapter/src/live/mod.rs` (242).

## R4 manual drops

| file:line | snippet | class | fix |
|---|---|---|---|
| crates/sequencer/src/bin/kardamom-sequencer/main.rs:336 | `drop(cluster_guard);` | Resource release (cluster session thread; it also closes the egress channel that unblocks the watermark feed). | Hard to remove as written, because the two `await`s after it must see the closed channel. Extract `async fn run_loops(...) -> ()` that owns `cluster_guard` and returns after the three `join_*` awaits; the guard then falls out of scope at the same point with no explicit `drop`. |
| crates/sequencer/src/bin/kardamom-sequencer/main.rs:343 | `drop(rt);` | Resource release (Aeron runtime). | Removable now. It is the last statement before `Ok(())`, so the implicit end-of-scope drop is identical. Delete it. |

No manual drops in `cluster-adapter` or `cluster-client` sources. `LiveCluster`'s
`Drop` impl (`live/mod.rs:76`) is a real RAII guard, not a manual drop.

## R5 sync primitives and channels

| file:line | primitive | verdict | reason | fix |
|---|---|---|---|---|
| crates/sequencer/src/resync/mod.rs:81 | `count: Arc<AtomicU64>` | JUSTIFIED | Written by the egress-watermark blocking task, read by the publish loop thread. | None. |
| crates/sequencer/src/resync/mod.rs:82 | `lag_gap_ms: Arc<AtomicU64>` | JUSTIFIED | Sticky cross-thread flag with `fetch_max`/`swap`; a channel would not give "keep the largest unconsumed value". | None. |
| crates/sequencer/src/resync/mod.rs:179 | `floor_rx: Receiver<FloorUpdate>` | JUSTIFIED | Receipts task to publish loop, across a tokio/blocking boundary. | None. |
| crates/sequencer/src/resync/mod.rs:190 | `reject_rx: Receiver<(Address,u64,u64)>` | JUSTIFIED | Watermark task to publish loop. | Replace the `(Address, u64, u64)` tuple with a named `ContiguityReject` struct (see R9). |
| crates/sequencer/src/resync/mod.rs:478 | `crossbeam_channel::unbounded()` (floors) | JUSTIFIED | Real cross-thread queue. | Bound it. Receipts arrive at line rate and are drained 1024 per iteration; the publish loop stalls exactly when resync triggers, so an unbounded queue grows without limit in the failure case it exists for. |
| crates/sequencer/src/resync/mod.rs:479 | `crossbeam_channel::unbounded()` (rejects) | JUSTIFIED | Same seam. | Same bound. |
| crates/sequencer/src/bin/kardamom-sequencer/feeds.rs:171 | `floor_tx: Sender<FloorUpdate>` | JUSTIFIED | Producer half of the above. | None. |
| crates/sequencer/src/bin/kardamom-sequencer/feeds.rs:52 | `reject_tx: Sender<(Address,u64,u64)>` | JUSTIFIED | Producer half of the above. | None. |
| crates/sequencer/src/bin/kardamom-sequencer/feeds.rs:185 | `tokio::sync::mpsc::UnboundedReceiver<(BPosition, Receipt)>` | JUSTIFIED | Aeron poll thread to the async floor task. | Bound it, for the same reason as the crossbeam pair. |
| crates/cluster-adapter/src/live/mod.rs:71 | `stop: Arc<AtomicBool>` | JUSTIFIED | `LiveCluster::drop` signals the session thread. | None. |
| crates/cluster-adapter/src/live/mod.rs:160 | `next_index: Arc<AtomicU64>` | JUSTIFIED | Consumer subscription writes the cursor, the session thread reads it for replay. | None. |
| crates/cluster-adapter/src/live/mod.rs:161 | `next_block: Arc<AtomicU64>` | JUSTIFIED | Same. | None. |
| crates/cluster-adapter/src/live/mod.rs:98 | `bounded(1)` reply channel, per offer | JUSTIFIED | Request/reply hand-off to the session thread. | Hot path: this allocates one channel per `offer`. Cache a single reply channel per `LiveIngress` clone (it is already `!Sync` per caller in practice) or use `Arc<(Mutex, Condvar)>` reused across offers. |
| crates/cluster-adapter/src/live/mod.rs:271 | `unbounded::<Vec<u8>>()` (egress frames) | JUSTIFIED | Aeron deliver callback to the session thread. | Bound it. A stalled session thread grows this queue without limit at egress line rate. |
| crates/cluster-adapter/src/live/mod.rs:309 | `unbounded::<OfferReq>()` | JUSTIFIED | Publisher threads to the session thread. | Bound it. Backpressure on the offer queue is exactly the signal `OfferOutcome::BackPressured` exists for. |
| crates/cluster-adapter/src/live/mod.rs:310 | `unbounded::<Vec<u8>>()` (app payloads out) | JUSTIFIED | Session thread to `LiveEgress`. | Bound it. |
| crates/cluster-adapter/src/live/session_loop.rs:112 | `stop: Arc<AtomicBool>` | JUSTIFIED | Loop-exit flag set by the owner. | None. |
| crates/cluster-adapter/src/live/session_loop.rs:109-111 | `frame_rx`, `req_rx`, `out_tx` | JUSTIFIED | The thread's three seams. | None. |
| crates/sequencer/src/outbound.rs:119-121 | `refs/epochs/remote_epochs: Arc<Mutex<Vec<_>>>` (fakes) | JUSTIFIED | `#[derive(Clone)]` fake shared between the test driver and the publisher handle; some tests hold two clones. | None. |
| crates/sequencer/src/outbound.rs:122 | `fail_with_backpressure: Arc<Mutex<bool>>` | JUSTIFIED | Shared toggle. | Use `Arc<AtomicBool>`. A `Mutex<bool>` here buys nothing and forces `.lock().unwrap()` at every call site. |
| crates/sequencer/src/outbound.rs:161 | `errors: Arc<Mutex<Vec<TxError>>>` | JUSTIFIED | Same shared-handle reason. | None. |
| crates/sequencer/src/epoch.rs:72-73 | `queue: Arc<Mutex<VecDeque<_>>>`, `closed: Arc<Mutex<bool>>` | JUSTIFIED | Cloneable scripted fake. | `closed` should be `Arc<AtomicBool>`. |
| crates/sequencer/src/remote_epoch.rs:77-78 | Same pair | JUSTIFIED | Same. | Same. |
| crates/cluster-adapter/src/gateway.rs:44-45 | `accepted: Arc<Mutex<Vec<Vec<u8>>>>`, `outcome: Arc<Mutex<OfferOutcome>>` | JUSTIFIED | `FakeIngress` is cloned into the publisher while the test reads it. | `outcome` is a `Copy` enum; use `Arc<AtomicU8>` or keep the Mutex but drop the `Vec` clone in `accepted()`. |
| crates/cluster-adapter/src/gateway.rs:78-79 | `FakeEgress` queue and `closed` | JUSTIFIED | Used across threads in `cluster-adapter/tests/end_to_end.rs`; the `yield_now` spin proves it. | Replace the spin with a `crossbeam_channel` so `recv` blocks instead of burning a core. |

## R6 dynamic dispatch

| file:line | snippet | class | generic replacement |
|---|---|---|---|
| crates/cluster-adapter/src/live/mod.rs:275 | `let deliver: DeliverFn = Box::new(move \|bytes, _pos, _session\| { … });` | Stored `dyn FnMut` closure. `DeliverFn = Box<dyn FnMut(&[u8], BPosition, i32) + Send>` is declared in `crates/log/src/aeron_live/mod.rs:103`, outside this group. | Make `open_subscription_with_deliver` generic: `fn open_subscription_with_deliver<F: FnMut(&[u8], BPosition, i32) + Send + 'static>(…, deliver: F)`. The runtime stores one closure per subscription, so monomorphizing is cheap. Requires a change in the `log` crate. |

No `Box<dyn Error>` and no trait objects in `crates/sequencer`,
`crates/cluster-adapter`, or `crates/cluster-client`. All polymorphism already goes
through generic bounds (`TxOrderingRefPublisher`, `TxDataSubscriber`,
`ClusterIngress`).

## R7 too many generics

| file:line | item | params/bounds | supertrait proposal |
|---|---|---|---|
| crates/sequencer/src/sequencer.rs:378 | `Sequencer::run_once<I, B, R>` | 3 params, 3 bounds (`I: TxDataSubscriber`, `B: TxOrderingRefPublisher`, `R: TxErrorPublisher`) | Add `pub trait SequencerPorts: Send { type In: TxDataSubscriber; type Refs: TxOrderingRefPublisher; type Errors: TxErrorPublisher; fn split(&mut self) -> (&mut Self::In, &mut Self::Refs, &mut Self::Errors); }`. `run_once<P: SequencerPorts>(&mut self, ports: &mut P)` then carries one parameter, and the three call sites in `feeds.rs` and the tests build one `Ports` value. |
| crates/sequencer/src/sequencer.rs:546 | `Sequencer::run<I, B, R>` | 3 params, 3 bounds, identical set | Same `SequencerPorts` supertrait. |

Near miss, worth noting: `crates/sequencer/src/bin/kardamom-sequencer/feeds.rs:258`
`spawn_publish_loops` has one type parameter but **12 value arguments** and carries
`#[allow(clippy::too_many_arguments)]`. Group them:
`struct PublishLoops<P> { cfg: SequencerConfig, tx_data: LiveTxDataSub, publishers: [P; 3], tx_errors: LiveTxErrorPub, epochs: LiveEpochSub, remote_epochs: LiveRemoteEpochSub, resync: Option<ResyncController>, shutdown: Shutdown }`
and drop the three separate `Shutdown` clones (they are clones of one token, so the
struct can hold one and clone it internally). That removes the `allow` too.

## R8 unnecessary pub

| file:line | item | evidence (who uses it) | fix |
|---|---|---|---|
| crates/sequencer/src/metrics.rs:78-104 | `record_ingest`, `record_publish`, `record_buffered_future`, `record_past`, `record_eviction`, `record_backpressure`, `record_nonce_check_latency` | Zero callers outside `metrics.rs`. Only the `record_helpers_smoke` test at line 170 calls them. `HotMetrics` replaced all seven. | Delete all seven and the smoke test. |
| crates/sequencer/src/metrics.rs:10-15, 18-34, 42-43, 121 | `TX_PUBLISHED_TO_B`, `TX_BUFFERED_FUTURE`, `TX_DROPPED_PAST`, `PENDING_BUFFER_EVICTIONS`, `BACKPRESSURE_EVENTS`, `NONCE_CHECK_DURATION_SECONDS`, `RESYNC_*`, `RECEIPT_FLOOR_*`, `CANONICAL_WATERMARK`, `REF_*`, `REMOTE_*`, `START_TIME_SECONDS` | Only `TX_INGESTED` is used outside the module (`tests/metrics_endpoint.rs:16,20`). The rest are read only by the `record_*`/`HotMetrics` code in the same file. | Make them `pub(crate)`. Keep `TX_INGESTED` public for the endpoint test. |
| crates/sequencer/src/outbound/cluster.rs:110 | `pub fn cluster_ref_publisher` | No caller anywhere. The bin uses `cluster_ref_publisher_with_egress`. | Delete. |
| crates/sequencer/src/pending.rs:69 | `pub fn lowest_nonce` | No caller anywhere, tests included. | Delete. |
| crates/sequencer/src/partition.rs:28 | `pub fn validate_partition_count` + `PartitionConfigError` | Only `tests/partition_routing.rs` calls it. `SequencerConfig::validate` re-implements the same check. | Delete, or fold into a `PartitionCount` newtype (see R9). |
| crates/sequencer/src/config.rs:34 | `pub nonce_floor_lag_ms: u64` | Read by nothing. Its own doc says "This field is unused." | Replace with `#[serde(default)] _nonce_floor_lag_ms: Option<u64>` (private) so old TOML still parses, or drop `deny_unknown_fields` for that key. |
| crates/sequencer/src/lib.rs:36 | `pub mod pending` | No use outside the crate, not even from the bin or tests. | `pub(crate) mod pending`. |
| crates/sequencer/src/lib.rs:39 | `pub mod sender` | Same. | `pub(crate) mod sender`. |
| crates/sequencer/src/lib.rs:42 | `pub mod state` | Only `tests/state_proptest.rs` uses it. No other crate does. | Keep public only if the proptest must stay external; otherwise `pub(crate)` and move the proptest inline. |
| crates/sequencer/src/bin/kardamom-sequencer/feeds.rs:47,166,244,258 | `pub fn spawn_egress_watermark_feed`, `pub fn spawn_receipt_floor_feed`, `pub type LoopHandle`, `pub fn spawn_publish_loops` | Binary crate. `pub` reaches nothing. | Drop `pub` on all four. |
| crates/sequencer/src/bin/kardamom-sequencer/adapters.rs:15,33,49,71 | `pub struct LiveTxDataSub`, `LiveEpochSub`, `LiveRemoteEpochSub`, `LiveTxErrorPub` and their `pub fn new` | Binary crate. Same. | Drop `pub`. |
| crates/cluster-client/src/lib.rs:33 | `pub mod protocol` | The only external consumer of this crate is `cluster-adapter`, which imports `session::{DriverEvent, SessionDriver}` and `bytes` only. `protocol` is used by `session/mod.rs` and by this crate's own tests. | `pub(crate) mod protocol`, or keep it public but mark the test-only items (below) `pub(crate)`. |
| crates/cluster-client/src/protocol.rs:189 | `pub fn decode_session_connect_request` and `pub struct SessionConnectRequest` (with 6 pub fields) | Used only by `session/tests.rs`. | `pub(crate)`. |
| crates/cluster-client/src/protocol.rs:241 | `pub fn decode_two_i64` | Used only by `session/tests.rs`. | `pub(crate)`. |
| crates/cluster-client/src/protocol.rs:296, 338 | `pub fn encode_session_event`, `pub fn encode_new_leader_event` | Used only by `session/tests.rs`. Their own doc says "mainly for tests". | `pub(crate)`, or gate on `#[cfg(any(test, feature = "testing"))]`. |
| crates/cluster-client/src/protocol.rs:33-38 | `TEMPLATE_SESSION_MESSAGE_HEADER`, `TEMPLATE_SESSION_EVENT`, `TEMPLATE_NEW_LEADER_EVENT` | Zero uses outside `protocol.rs`. `TEMPLATE_SESSION_CLOSE_REQUEST` is used only by `session/tests.rs`. | `pub(crate)` for all four. |
| crates/cluster-client/src/session/mod.rs:143,154 | `pub fn connect_attempts`, `pub fn state` | Used only by `session/tests.rs`. The live transport uses `is_connected()`. | `pub(crate)`. |
| crates/cluster-adapter/src/wire/ingress.rs:129, 167 | `pub fn decode_ingress_batch`, `pub fn decode_replay_request` | `decode_ingress_batch` has zero callers, tests included, despite its "used by tests" doc. `decode_replay_request` is used only by `wire/tests.rs`. | Delete `decode_ingress_batch`. Make `decode_replay_request` `pub(crate)`. |
| crates/cluster-adapter/src/wire/ingress.rs:43 | `pub fn encode_ingress_depositref` | Only `wire/tests.rs` uses it. No production caller: the sequencer publishes epochs, not `DepositRef`s. | Gate on `#[cfg(any(test, feature = "testing"))]` or delete with the `RT_DEPOSITREF` path. |
| crates/cluster-adapter/src/wire/egress.rs:240 | `pub fn encode_contiguity_reject` | Only `wire/tests.rs`. The real encoder is the Java service. | Gate behind the `testing` feature. |

## R9 defensive validation

| file:line | snippet | why | typed replacement |
|---|---|---|---|
| crates/sequencer/src/sequencer.rs:116 | `cfg.validate().expect("validated config");` | The same config is already validated at `main.rs:179`. This is the same check one layer down, and it panics instead of returning. | `struct ValidConfig(SequencerConfig)` with `fn parse(cfg: SequencerConfig) -> Result<Self, ConfigError>`. `main` builds it once; `Sequencer::new(ValidConfig)` cannot fail. |
| crates/sequencer/src/config.rs:83-92 | `if self.partition_count == 0 { … } if self.partition_index >= self.partition_count { … }` | Third place the same partition rules are checked (`partition::validate_partition_count`, and the `debug_assert!` below). | Represent them once: `struct PartitionCount(NonZeroU32)` and `struct PartitionIndex { index: u32, of: PartitionCount }` with a fallible constructor. |
| crates/sequencer/src/partition.rs:16 | `debug_assert!(m >= 1, "partition count must be >= 1");` | Assert on a raw `u32` in non-test code. It is unchecked in release builds, so `% 0` panics there anyway. | Take `PartitionCount` (a `NonZeroU32` newtype) and delete the assert. |
| crates/sequencer/src/partition.rs:28 | `pub fn validate_partition_count(m: u32)` | A standalone check-then-error over a raw `u32`, called only by a test. | Fold into `PartitionCount::new(u32) -> Result<Self, _>`. |
| crates/sequencer/src/sender.rs:26 | `debug_assert!(envelope.sender != Address::ZERO, …)` | Assert on data from another process, disabled in release. The module doc says the field is trusted unconditionally, which contradicts asserting on it. | The proxy should hand over `TxEnvelope { sender: RecoveredSender }` where `RecoveredSender` is a newtype whose only constructor is signature recovery. Then `sender_of` and the assert both disappear. |
| crates/sequencer/src/sequencer.rs:415-423 | `let part = partition_for(sender, self.cfg.partition_count); if part != self.cfg.partition_index { warn!(…); return Ok(true); }` | Explicitly labelled "Defensive". Re-derives routing per envelope on the hot path, although the tx_data subscription is already per shard. | Push the check to the subscription boundary: `ShardSubscription` yields only `ShardedEnvelope { sender: ShardMember<P> }`, constructed once when the fragment is decoded. The loop then needs no re-check. |
| crates/sequencer/src/bin/kardamom-sequencer/feeds.rs:108,130 | `if frame.first() == Some(&wire::EGRESS_KIND_CONTIGUITY_REJECT) { … decode_egress(&frame) … }` and `if frame.first() != Some(&wire::EGRESS_KIND_BOUNDARY) { continue; }` | The kind byte is inspected here, then `decode_egress` dispatches on the same byte again. The same check at two layers. | Decode once: `match decode_egress(&frame) { Ok(EgressItem::ContiguityReject{..}) => …, Ok(EgressItem::Boundary(b)) => …, _ => continue }`. If the cheap pre-check must stay for cost reasons, expose `wire::peek_kind(&frame) -> Option<EgressKind>` returning a typed enum, and let `decode_egress` take that kind. |
| crates/cluster-client/src/protocol.rs:192, 244, 379 | `let body = &buf[HEADER_LEN..];` | Raw slice index that is panic-free only because `MessageHeader::decode` proved 8 bytes just above. The invariant is implicit and repeated in three functions. | `struct Frame<'a> { header: MessageHeader, body: &'a [u8] }` with `Frame::parse(buf) -> Result<Frame, DecodeError>`. Every decoder then takes a `Frame` and never indexes `buf`. |
| crates/cluster-client/src/protocol.rs:376 and 405 | `if h.schema_id != SCHEMA_ID { return Err(SchemaMismatch …) }` in `decode_egress`, and the same test inside `expect` | The schema check is written twice for the two entry paths. | Do it once inside `Frame::parse` (above), so no decoder can see a wrong-schema frame. |
| crates/cluster-adapter/src/live/mod.rs:242-258 | `validate_config` checks `egress_channel.is_empty()` and `member_ids(&cfg.ingress_endpoints).is_empty()` | Good boundary check, but it returns `()`. `LiveClusterConfig.ingress_endpoints` stays a raw `String`, so `endpoint_for_member` and `member_ids` re-parse it on every reconnect and rotation (`session_loop.rs:311, 456`). | Have validation return `struct ValidatedCluster { endpoints: Vec<(i32, String)>, egress_channel: ChannelUri, … }`. The session loop then indexes a parsed list instead of re-splitting a string in the reconnect path. |
| crates/sequencer/src/bin/kardamom-sequencer/main.rs:129 | `anyhow::ensure!(args.sequencer_id.is_none(), "--sequencer-id cannot be combined with --partition-offset…")` | A hand-rolled CLI mutual-exclusion check inside the override logic. | Express it in the parser: `#[arg(long, conflicts_with = "partition_offset")] sequencer_id: Option<u8>`. clap then rejects it before `apply_cli_overrides` runs. |
| crates/cluster-adapter/src/config.rs:38-49 | `if self.ingress_stream_id == 0 { self.ingress_stream_id = 101; } …` | Zero is used as "unset". An operator cannot configure stream id 0, and the sentinel is checked at every `to_live()`. | Use `Option<i32>` fields with `#[serde(default)]` and `unwrap_or(101)`, or `#[serde(default = "default_ingress_stream_id")]`. |

## R10 imperative style

| file:line | snippet | functional form |
|---|---|---|
| crates/sequencer/src/nonce_decode.rs:69 | `let mut v = 0u64; for &x in bytes { v = (v << 8) \| u64::from(x); }` | `bytes.iter().fold(0u64, \|v, &x\| (v << 8) \| u64::from(x))`. |
| crates/sequencer/src/nonce_decode.rs:87 | `let mut l = 0usize; for &x in b.get(i+1..i+1+ll)? { l = (l << 8) \| x as usize; }` | `b.get(i+1..i+1+ll)?.iter().fold(0usize, \|l, &x\| (l << 8) \| x as usize)`. Extract it as `fn be_len(bytes: &[u8]) -> usize`, since line 96 repeats it verbatim. |
| crates/sequencer/src/nonce_decode.rs:96 | Same loop, second copy | Same helper. |
| crates/cluster-adapter/src/live/endpoints.rs:62 | `for step in 1..=ids.len() { let id = ids[(start+step) % ids.len()]; if let Some(p) = open_leader_pub(…) { return Some((id, p)); } }` | `(1..=ids.len()).map(\|s\| ids[(start + s) % ids.len()]).find_map(\|id\| open_leader_pub(rt, endpoints, id, stream_id).map(\|p\| (id, p)))`. |
| crates/sequencer/src/sequencer.rs:518 | `let mut publishes = Vec::new(); for action in result.actions { match action { Publish{..} => publishes.push(..), ReportDuplicate{..} => { … } } }` | `result.actions.into_iter().filter_map(\|a\| match a { Publish{nonce,payload} => Some((sender, nonce, payload)), ReportDuplicate{..} => { self.report_duplicate(rc, …); None } }).collect()`. The `ReportDuplicate` arm has a side effect, so keep the loop if a side-effecting `filter_map` reads worse; splitting into `partition` plus two passes is the cleaner alternative. |
| crates/cluster-adapter/src/live/session_loop.rs:537 | `let mut live = 0; if !self.frame_rx_dead { sel.recv(&self.frame_rx); live += 1; } if !self.req_rx_dead { … }` | Not worth changing. `crossbeam::Select` needs the imperative registration, and `live` is a two-branch count. Leave as is. |
| crates/sequencer/src/resync/mod.rs:276 | `for _ in 0..FLOOR_DRAIN_PER_ITER { match self.floor_rx.try_recv() { … } }` | Keep. `try_iter().take(N)` loses the `Disconnected` arm that logs once and sets `floor_rx_dead`, which is load-bearing. Same verdict for `drain_contiguity_rejects` at line 342 and both `try_recv` loops in `session_loop.rs` (253, 480). |
| crates/sequencer/src/unconfirmed.rs:134 | `while stale.len() < max { … if now.duration_since(queued_at) < timeout { break; } … }` | Keep. The loop needs an early `break` on the first non-expired front entry, plus lazy deletion of stale queue slots. An iterator chain would not express the front-peek stop. |
| crates/sequencer/src/state/mod.rs:181 | `let mut out = Vec::new(); for (&sender, buf) in self.pending.iter_mut() { … out.push(..) … }` | Keep. The loop mutates `self.next` while holding a `&mut` borrow of `self.pending`; the comment at line 182 says so. An iterator chain would force a `Vec<Address>` snapshot first, which is what the current form avoids. This is on the publish path. |

## Tests

Test files get the light pass. No test file exceeds 500 code lines (largest:
`crates/cluster-client/src/session/tests.rs` at 336,
`crates/sequencer/tests/sequencer_step.rs` at 232,
`crates/cluster-adapter/src/wire/tests.rs` at 226). No `dyn` anywhere in test code.

### R1 comments (tests)

| file:line | snippet (max 80 chars) | why | fix |
|---|---|---|---|
| crates/sequencer/tests/replicated_shard_racing.rs:263 | `The removed fast-forward used to adopt the post-hole run after 5 seconds,` | Describes deleted code. | Say what the test asserts: a hole is never published past. |
| crates/sequencer/tests/resync_filter.rs:2 | `docs/agents/sequencer-lag-resync-spec.md). A skip happens only with` | Names a spec document. | State the rule inline. |
| crates/sequencer/tests/alloc_profile.rs:15 | `The inputs mirror `benches/throughput.rs`: RLP-encoded signed legacy` | Cross-file coupling note that goes stale silently. | Describe the inputs without the reference, or share one fixture module. |
| crates/sequencer/tests/alloc_profile.rs:61 | `Build a signed legacy transaction, wrapped the way the proxy publishes` | Same builder is copied in five test files (`sequencer_integration.rs:49`, `sequencer_step.rs:37`, `resync_filter.rs:42`, `multi_sequencer_dual_write.rs:59`, `benches/throughput.rs:51`). | Move it to a shared `tests/common/mod.rs` and delete the duplicated docs. |
| crates/cluster-client/src/session/tests.rs:333 | `the cluster serves frames and REPLAY_DONE into an image that no longer` | Duplicates `session_loop.rs:39` word for word. | Keep one copy, next to the production constant. |
| crates/cluster-client/src/session/tests.rs:354 | `// The old session is no longer usable for app messages.` | Restates the two asserts below it. | Delete. |

### R3 large files (tests)

| file | code lines | split plan or KEEP (reason) |
|---|---|---|
| crates/cluster-client/src/session/tests.rs | 336 | KEEP. Under the threshold and it is one driver's behaviour suite. If it grows, split into `tests/connect.rs`, `tests/foreign_session.rs`, `tests/self_heal.rs`. |
| crates/sequencer/tests/* (8 files, 26-232 lines each) | — | KEEP, but factor out the shared `signed_envelope`/`buf` transaction builder duplicated in five of them into `tests/common/mod.rs`. |

### R6 dynamic dispatch (tests)

None found.

### R10 imperative style (tests)

| file:line | snippet | functional form |
|---|---|---|
| crates/sequencer/tests/multi_sequencer_dual_write.rs:152 | `for i in 0..M as usize { … sequencers[i].run_once(&mut channels_a[i], &mut b_pubs[i], &mut rcs[i]) … }` | Index loop over four parallel slices. Zip them: `for (((s, a), b), rc) in sequencers.iter_mut().zip(&mut channels_a).zip(&mut b_pubs).zip(&mut rcs)`, or group the four into one `Replica` struct and iterate `replicas.iter_mut()`. |
| crates/sequencer/tests/replicated_shard_racing.rs:105 | `let mut stream = Vec::new(); let mut offset = 0i32; for nonce in … { for (i, s) in signers.iter().enumerate() { stream.push(…); offset += 64; } }` | `(0..TX_PER_SENDER).flat_map(\|nonce\| signers.iter().enumerate().map(move \|(i, s)\| …)).enumerate().map(\|(k, e)\| (TxDataLoc::new(0, BPosition{ term_id: 0, term_offset: k as i32 * 64 }), e)).collect()`. The `offset` counter is `index * 64`. |
| crates/sequencer/tests/alloc_profile.rs:99 | `let mut i = 0u64; for nonce in 0..total_nonces { for s in &signers { … i += 1; } }` | `i` is the flat index. Use `flat_map(...).enumerate()` and drop the manual counter. |
| crates/sequencer/tests/sequencer_integration.rs:85 | `let mut stream: Vec<(usize, u64)> = Vec::new();` then push loop | Same `flat_map`/`collect` shape as above. |
| crates/sequencer/tests/replicated_shard_racing.rs:141 | `let mut seen = HashSet::new(); interleaved.iter().filter(\|r\| seen.insert(r.tx_hash))…` | Already functional. No change. |

# log

## Summary

The `log` group has 34 Rust files and about 4700 code lines. No file passes 500 code
lines, so R3 finds nothing; `testing.rs` (485) is the only near miss. The largest
problem is R8: four whole public modules — `publisher.rs`, `subscriber.rs`,
`supervisor.rs`, and `replay.rs` (about 1150 raw lines) — have no user anywhere in
the workspace. Other crates name them only inside doc comments. The second problem
is R1: 47 comments in source files carry removal notes, incident stories, or
"old/previous/legacy" framing, and three doc links point at deleted items
(`run_durable_watermark_loop`, `ReceiptCache*Handle`, a private `ADD_PUB_TIMEOUT`).
One comment is plainly wrong: `subscriber.rs:4` says the module sits behind an
`aeron-live` feature that `lib.rs:20` says does not exist. R9 is next: the same
"channel URI has no NUL byte" check runs at 17 sites, and the MDS-enabled check
runs at 6. Counts: R1 47, R2 8 (plus 2 borderline), R3 0, R4 0 in non-test code,
R5 20, R6 12, R7 9, R8 21, R9 10, R10 9; tests add 10 R1 rows and 4 R10 rows.
Every "channel" hit outside the Aeron URI strings is a real Rust channel, and
every one is JUSTIFIED; the only sync-primitive risk is that all 8 message
channels are unbounded.

## R1 comments

| file:line | snippet (max 80 chars) | why | fix |
|---|---|---|---|
| crates/log/src/lib.rs:10 | "The previous N-way Q-of-N quorum aggregator and custom recorders no longer exist." | states what was deleted | delete the sentence; describe only the current model |
| crates/log/src/watermark.rs:1 | "(Removed) Quorum fsync-watermark aggregator." | the whole file is a removal note with a spec-doc path | move the durability model into `lib.rs`; delete the module |
| crates/log/src/watermark.rs:14 | "dead code in the push-model cleanup (docs/agents/push-model-spec.md)" | names a spec doc and a past cleanup | delete |
| crates/log/src/codec.rs:8 | "This crate uses rkyv v0.8, not the earlier bincode choice." | names a superseded choice | keep "This crate uses rkyv v0.8." only |
| crates/log/src/offer_retry.rs:10 | "An earlier loop spun a fixed 1024 times (microseconds) and treated every ..." | 9 lines of bug history | replace with the present-tense rule: retry until connected or deadline |
| crates/log/src/publisher.rs:17 | "the rusteron 0.1.16x bindings only take `&CStr`" | pins a dependency version in prose | say "the bindings take `&CStr`" |
| crates/log/src/subscriber.rs:4 | "Gated behind the `aeron-live` cargo feature." | wrong: `lib.rs:20` says the feature does not exist | delete the line |
| crates/log/src/subscriber.rs:5 | "See `publisher.rs` for the same caveats about `rusteron-client` API drift." | `publisher.rs` holds no such caveats | delete |
| crates/log/src/subscriber.rs:28 | "Mirror of [`crate::publisher::ADD_PUB_TIMEOUT`]." | the target is private; the doc link is broken | state the value and its reason inline |
| crates/log/src/supervisor.rs:8 | "Restart policy: version 0 logs loudly and exits ... follow-up task." | roadmap note | say what the code does now |
| crates/log/src/supervisor.rs:127 | "Version 0: if any child dies, log loudly and exit." | duplicates the module doc above | delete one of the two |
| crates/log/src/config/mod.rs:4 | "passed to the supervisor and the quorum aggregator" | the aggregator is deleted | name the real consumers |
| crates/log/src/config/mod.rs:137 | "remain because the surviving pieces still reference them" | "surviving" frames a past deletion | list the current users in present tense |
| crates/log/src/config/mod.rs:193 | "TODO(consul-watch): register each executor as a Consul service" | tracker-style TODO in a doc comment | move to the issue tracker |
| crates/log/src/config/mod.rs:221 | "See `docs/specs/interop-outbox-messaging-spec.md`." | spec doc by name | summarise the rule here |
| crates/log/src/recorder.rs:8 | "[`run_durable_watermark_loop`]. The old N-recorder Q-of-N quorum aggregator" | broken doc link plus removal note | delete both |
| crates/log/src/recorder.rs:22 | "The durable-watermark loop ([`run_durable_watermark_loop`]) polls this" | the item does not exist | delete |
| crates/log/src/recorder.rs:148 | "It never drops below the recorder's historical 60 s" | "historical" value | say "the floor is 60 s" |
| crates/log/src/recorder.rs:248 | "`recorder_id` and `archive_dir` stay in the signature for caller compatibility" | explains a dead parameter by history | drop both parameters |
| crates/log/src/recorder.rs:268 | "[`run_durable_watermark_loop`] both run on this thread" | broken doc link | name the real caller |
| crates/log/src/recorder.rs:269 | "held, never read (its last reader left with the durable-watermark decoder)" | history; repeated at 290, 320, 336 | keep one present-tense RAII note |
| crates/log/src/recorder.rs:382 | "its term length fed the deleted durable-watermark decoder" | names deleted code | say why the fetch still runs |
| crates/log/src/refetch.rs:9 | "\"restart and replay from the local archive\" was never a recovery path" | past-tense framing | state the constraint directly |
| crates/log/src/refetch.rs:17 | "Design constraints (from the validator BAL-refetch postmortem)" | postmortem reference | drop the source; keep the constraints |
| crates/log/src/refetch.rs:76 | "reuses the destination endpoint freed by the removal of the resume replay-merge" | removal history | say what the endpoint is for |
| crates/log/src/replay.rs:286 | "Stitching multiple recordings or sessions is future work." | roadmap note | state the limit, not the plan |
| crates/log/src/aeron_live/mod.rs:48 | "- `ReceiptCache{Publisher,Subscriber}Handle`: the proxy-executor receipt cache" | these types are not exported and do not exist | delete the bullet |
| crates/log/src/aeron_live/mod.rs:51 | "watermark streams feeding the quorum aggregator" | the aggregator is deleted | name the real consumer |
| crates/log/src/aeron_live/runtime.rs:294 | "the same merge the shared-multicast path produced from multiple images" | compares against a past design | describe the current merge only |
| crates/log/src/aeron_live/runtime.rs:354 | "With a single publisher ... so behavior is unchanged." | "unchanged" against what is not stated | delete |
| crates/log/src/aeron_live/thread.rs:27 | "This once happened live when `Vec<Receipt>` batch frames crossed the MTU" | 5-line incident story | keep the rule: the assembler is required for frames over one MTU |
| crates/log/src/aeron_live/thread.rs:112 | "This is the fix for the cluster `tx_ordering` freeze." | names a past bug | state the invariant: a publish never blocks the poll |
| crates/log/src/aeron_live/thread.rs:133 | "a plain sleep put up to 100 microseconds of latency under every ack-waited" | past measurement narrative | keep the cadence rule; drop the measurement story |
| crates/log/src/aeron_live/pending.rs:24 | "Profiling showed that cadence dominating the sequencer's CPU ... about 66%" | dated measurement | state the backoff rule |
| crates/log/src/aeron_live/pending.rs:74 | "offered in a blocking spin/sleep loop (the old `offer_blocking` ...)" | names removed code | describe the current queue |
| crates/log/src/aeron_live/pending.rs:97 | "matching the old blocking deadline" | comparison to removed code | give the deadline's reason |
| crates/log/src/aeron_live/handles/tx_receipts.rs:3 | "legacy shared IPC channel" | "legacy" also at 102, 121, 205, 239, 342 | call it "single-channel IPC mode" |
| crates/log/src/aeron_live/handles/tx_receipts.rs:25 | "see TODO(consul-watch) on `ChannelsConfig::tx_receipts_executor_count`" | tracker TODO | move to the tracker |
| crates/log/src/aeron_live/handles/tx_receipts.rs:167 | "The previous receipt-per-frame path paid a blocking cross-thread ack" | removed-design history | state that one frame carries one batch |
| crates/log/src/testing.rs:64 | "This keeps the historical name `Concurrent`, so existing test code does not churn." | explicit legacy-name note | rename the type, or drop the note |
| crates/log/src/testing.rs:438 | "Lease/aggregator tests publish into one of these" | the aggregator is deleted | name the real users |
| crates/obs/src/lib.rs:3 | "See `docs/specs/2026-05-29-prometheus-grafana-design.md`." | dated spec doc | summarise the design here |
| crates/obs/src/lib.rs:44 | "Promoted out of `crates/node/src/metrics.rs`" | migration history | delete |
| crates/obs/src/lib.rs:50 | "Stays `kardamom_build_info` for compatibility with the existing RPC dashboard." | compatibility history | say the dashboards read this name |
| crates/obs/src/lib.rs:80 | "An earlier design used a dedicated thread ... The team removed that design" | removed-design note | state the current rule: one runtime |
| crates/obs/src/lib.rs:131 | "metrics port in use (squatter not yet reaped?); retrying bind (#122)" | issue number in a log message | delete "(#122)" |
| crates/obs/src/bin.rs:5 | "The previous copies lived in `kardamom_engine::bin_support` ... had started to drift" | migration history | say what this module gives every binary |

## R2 long methods

| file:line | fn name | approx code lines | helpers to extract |
|---|---|---|---|
| crates/log/src/replay.rs:241 | run_replay_merge | 131 | `check_coverage(desc, stream_id)`; `build_merge(session, params, desc)`; `poll_merge(merge, archive, stop, loc, deliver)`; `handle_empty_poll(archive)` |
| crates/log/src/refetch.rs:133 | fetch_tx_data | 82 | `resolve_session_recording(recs, session_id)`; `prepare_replay(rec, from)` (bounds plus URI); `replay_subscription(rt, subs, key, uri)`; `drain_into(rx, sink) -> u64` |
| crates/log/src/replay.rs:441 | resolve_recording | 77 | `page_catalog(archive, stream_id, found)`; `latest_descriptor(found)`; keep the wait loop as the outer body |
| crates/log/src/aeron_live/thread.rs:163 | handle_cmd | 77 | `enqueue_publish(pending, pub_id, bytes, ack)`; `cmd_open_publication(...)`; `cmd_open_subscription(...)`; `cmd_remove_destination(dests, sub_id, uri)` |
| crates/obs/src/lib.rs:64 | init | 71 | `retry_settings() -> (u32, Duration)`; `build_exporter(service, addr, host_id)`; `build_with_retry(...)`; `register_build_gauges(version, git_sha)` |
| crates/log/src/refetch.rs:228 | fetch_deposits | 57 | share `prepare_replay`, `replay_subscription`, `drain_into` with `fetch_tx_data`; keep only the per-recording loop |
| crates/log/src/aeron_live/thread.rs:64 | run_aeron_thread | 55 | `drain_commands(cmd_rx, ...) -> ControlFlow`; `poll_subscriptions(subs) -> bool`; `wait_next(cmd_rx, backoff, worked, pending)` |
| crates/log/src/refetch.rs:413 | list_recordings | 51 | `page_catalog(archive, stream_id)` shared with replay.rs and recorder.rs; replace the `Rc<Cell<Vec<_>>>` shim with a `RefCell` accumulator |
| crates/log/src/recorder.rs:113 | connect_archive_with_timeout | 49 (borderline) | `archive_context(aeron_dir)`; `control_channels(actx, cfg)`; `message_timeout_ns(connect_timeout)` |
| crates/log/src/recorder.rs:535 | active_recording_for_stream | 49 (borderline) | fold into the shared `page_catalog` helper above |

## R3 large files

| file | code lines | split plan or KEEP (reason) |
|---|---|---|
| crates/log/src/testing.rs | 485 | Under the 500 threshold, but it holds two unrelated concerns. Split into `testing/fakes.rs` (FakeBus, FakeHeader, publication and subscription fakes) and `testing/cluster.rs` (the `docker-e2e` `AeronTestCluster`, `spawn_node`, `ensure_image_built`). The second half already sits in its own `mod docker`. |
| crates/log/src/refetch.rs | 418 | KEEP. One client type plus its private helpers. The R2 split gives it enough structure. |
| crates/log/src/replay.rs | 365 | KEEP. One subscriber type plus its thread body. |
| crates/log/src/aeron_live/runtime.rs | 337 | KEEP. Command bus plus `PubHandle`; both halves share `RuntimeCmd`. |
| crates/log/src/recorder.rs | 333 | KEEP. Connect helpers plus the `Recorder` type; cohesive around one archive session. |
| every other file | < 300 | KEEP. |

## R4 manual drops

None found in non-test code. All 8 `drop(x)` calls sit in test files
(`crates/log/tests/aeron_live_e2e.rs:120-121`, `offer_starvation.rs:179-180`,
`offer_connect_race.rs:114-115`, `crates/obs/tests/init.rs:11`,
`init_port_in_use.rs:47`). `crates/log/src/testing.rs:472` is a `drop(cluster)`
inside a comment, not a call. Related, but not a `drop` call:
`crates/log/src/aeron_live/runtime.rs:431` calls `std::mem::forget(handler)` to
keep a leaked FFI handler alive for the process; a `ManuallyDrop` field on the
runtime would state that intent in the type.

## R5 sync primitives and channels

| file:line | primitive | verdict | reason | fix |
|---|---|---|---|---|
| crates/log/src/supervisor.rs:23 | `Option<oneshot::Sender<()>>` | JUSTIFIED | signals a spawned tokio task to stop | none; a `CancellationToken` would match the rest of the crate |
| crates/log/src/supervisor.rs:55 | `oneshot::channel()` | JUSTIFIED | same shutdown signal | none |
| crates/log/src/aeron_live/runtime.rs:31 | `CbSender<RuntimeCmd>` | JUSTIFIED | many tokio tasks to one `!Send` Aeron thread | none |
| crates/log/src/aeron_live/runtime.rs:34 | `Arc<AeronThread>` | JUSTIFIED | shared RAII owner; the last clone joins the thread | none |
| crates/log/src/aeron_live/runtime.rs:110 | `crossbeam_channel::bounded(1)` ack | JUSTIFIED | reply channel for one request into the Aeron thread | none |
| crates/log/src/aeron_live/runtime.rs:169 | `bounded::<Result<(),LogError>>(1)` | JUSTIFIED | thread-start handshake | none |
| crates/log/src/aeron_live/runtime.rs:170 | `crossbeam_channel::unbounded` cmd bus | JUSTIFIED | command bus into the Aeron thread | bound it; an unbounded command bus can grow without limit |
| crates/log/src/aeron_live/runtime.rs:319 | `unbounded_channel::<(BPosition,T)>` | JUSTIFIED | Aeron thread to async consumer | consider a bounded channel plus a drop policy |
| crates/log/src/aeron_live/runtime.rs:343 | `unbounded_channel` (with sub id) | JUSTIFIED | same path | same |
| crates/log/src/aeron_live/runtime.rs:360 | `unbounded_channel::<(TxDataLoc,TxEnvelope)>` | JUSTIFIED | same path, tx_data payloads | same; these frames are large |
| crates/log/src/aeron_live/thread.rs:66 | `CbReceiver<RuntimeCmd>` | JUSTIFIED | the Aeron thread's only inbox | none |
| crates/log/src/aeron_live/pending.rs:95 | `Option<CbSender<Result<BPosition,LogError>>>` | JUSTIFIED | per-frame publish ack back to the caller | none |
| crates/log/src/aeron_live/handles/tx_receipts.rs:241 | `unbounded_channel()` | JUSTIFIED | receipt fan-out to the consumer task | bound it |
| crates/log/src/aeron_live/handles/tx_receipts.rs:286 | `unbounded_channel()` | JUSTIFIED | MDS receipt path | bound it |
| crates/log/src/replay.rs:142 | `unbounded_channel::<(L,T)>` | JUSTIFIED | replay thread to async consumer | bound it; replay is a bulk source |
| crates/log/src/refetch.rs:102 | `HashMap<..,UnboundedReceiver<..>>` | JUSTIFIED | one receiver per replay subscription, filled by the Aeron thread | none |
| crates/log/src/refetch.rs:528 | `Arc<Unpark>` waker | JUSTIFIED | `Wake` requires `Arc` | none |
| crates/log/src/testing.rs:36 | `Arc<Mutex<StreamMap>>` | JUSTIFIED | `crates/executor/tests/m_plus_one_join.rs:314,322,406` clones the bus into `thread::spawn` | none |
| crates/log/src/testing.rs:67,145 | `Arc<Mutex<StreamState>>` | JUSTIFIED | same cross-thread test use | none |
| crates/log/src/testing.rs:442 | `Arc<Mutex<HashMap<u8,VecDeque<..>>>>` | UNNECESSARY | only `crates/log/tests/testing_fakes.rs:37` uses it, single-threaded | `RefCell` behind the same API, or delete the type (see R8) |

## R6 dynamic dispatch

### R6 addendum (after review)

`DeliverFn` (`aeron_live/mod.rs:103`) is a closed set, so the decision is an enum, not a
box. `Deliver::Typed(TypedDeliver)` covers the eight `(BPosition, T)` streams (`TxError`,
`EpochRecord`, `RemoteEpochRecord`, `FsyncWatermark`, `QuorumWatermark`, `BlockBoundary`,
`Deposit`, `BalFrame`), with `TypedDeliver` an enum of one variant per message type;
`Deliver::TxData` covers the session-aware tx_data stream; `Deliver::ReceiptBatch` the
`Vec<Receipt>` fan-out; `Deliver::RawFrames` the cluster egress relay in
`cluster-adapter`. `AssembledDeliver` matches on it. The `rusteron_client::Handler`
objects stay; they are the C client's FFI boundary.

| file:line | snippet | class | generic replacement |
|---|---|---|---|
| crates/log/src/refetch.rs:138 | `sink: &mut dyn FnMut(TxDataLoc, TxEnvelope)` | stored `dyn Fn` argument | `sink: impl FnMut(TxDataLoc, TxEnvelope)`; the method is not object-safe-bound anywhere |
| crates/log/src/refetch.rs:232 | `sink: &mut dyn FnMut(BPosition, Deposit)` | same | `impl FnMut(BPosition, Deposit)` |
| crates/log/src/recorder.rs:298 | `should_stop: &mut dyn FnMut() -> bool` | dyn closure argument | take `&CancellationToken`; the only caller passes `\|\| stop.is_cancelled()` |
| crates/log/src/recorder.rs:327 | `should_stop: &mut dyn FnMut() -> bool` | same | same |
| crates/log/src/recorder.rs:344 | `should_stop: &mut dyn FnMut() -> bool` | same | same |
| crates/log/src/recorder.rs:417 | `should_stop: &mut dyn FnMut() -> bool` | same | same |
| crates/log/src/aeron_live/mod.rs:103 | `pub type DeliverFn = Box<dyn FnMut(&[u8], BPosition, i32) + Send>` | trait-object dispatch | genuinely heterogeneous: one `Vec<SubEntry>` holds handlers for many message types. Keep the box, or replace with an enum `Deliver::{Typed(..), TxData(..), ReceiptBatch(..)}` covering the 3 real shapes |
| crates/log/src/testing.rs:544 | `Result<Self, Box<dyn std::error::Error>>` | `Box<dyn Error>` | `anyhow::Result<Self>`; add `anyhow.workspace = true` to `crates/log` (already a workspace dependency) |
| crates/log/src/testing.rs:566 | `Result<Self, Box<dyn std::error::Error>>` | same | same |
| crates/log/src/testing.rs:630 | `Result<(), Box<dyn std::error::Error>>` | same | same |
| crates/log/src/testing.rs:655 | `Result<Node, Box<dyn std::error::Error>>` | same | same |
| crates/log/src/testing.rs:701 | `Result<(), Box<dyn std::error::Error>>` | same | same |

## R7 too many generics

| file:line | item | params/bounds | supertrait proposal |
|---|---|---|---|
| crates/log/src/replay.rs:241 | `run_replay_merge<T, L, F>` | 3 params, 5 bounds | add `trait WireMessage: rkyv::Archive + Send + 'static where Self::Archived: Deserialize<Self, HighDeserializer<Error>> + for<'a> CheckBytes<HighValidator<'a, Error>> {}` with a blanket impl, then `fn run_replay_merge<T: WireMessage, M: LocMap>`; `trait LocMap { type Loc: Send + 'static; fn loc(&self, pos: BPosition, session: i32) -> Self::Loc; }` folds `L` and `F` into one parameter |
| crates/log/src/replay.rs:111 | `impl<T, L> ReplayMergeSubscriber<T, L>` | 2 params, 4 bounds | `impl<T: WireMessage, M: LocMap>` with `type Loc` as the associated type |
| crates/log/src/replay.rs:134 | `open_with_loc<F>` | adds a 3rd param | take `M: LocMap` |
| crates/log/src/aeron_live/runtime.rs:275 | `open_subscription<T>` | 1 param, 5 bounds | `T: WireMessage` |
| crates/log/src/aeron_live/runtime.rs:302 | `open_subscription_merged<T>` | 1 param, 5 bounds | `T: WireMessage` |
| crates/log/src/aeron_live/runtime.rs:331 | `open_subscription_with_id<T>` | 1 param, 5 bounds | `T: WireMessage` |
| crates/log/src/aeron_live/runtime.rs:382 | `typed_deliver<T>` | 1 param, 5 bounds | `T: WireMessage` |
| crates/log/src/subscriber.rs:40 | `impl<T> TypedSubscriber<T>` | 1 param, 4 bounds | `T: WireMessage` (the same trait; put it in `codec.rs` next to `materialize`) |
| crates/log/src/testing.rs:195 | `impl<T> FakeTypedSubscription<T>` | 1 param, 4 bounds | `T: WireMessage` |

## R8 unnecessary pub

| file:line | item | evidence (who uses it) | fix |
|---|---|---|---|
| crates/log/src/publisher.rs:88 | `pub struct TxDataPublisher` | no hit outside `crates/log/src/publisher.rs` | delete the module, or gate it behind a feature |
| crates/log/src/publisher.rs:133 | `pub struct TxOrderingPublisher` | only doc-comment mentions (`crates/sequencer/src/outbound.rs:26`) | delete |
| crates/log/src/publisher.rs:171 | `pub struct QuorumPublisher` | no hit anywhere | delete |
| crates/log/src/publisher.rs:165 | `pub fn publish_message` | no caller | delete with the module |
| crates/log/src/subscriber.rs:35 | `pub struct TypedSubscriber<T>` | only used inside `subscriber.rs` | delete the module |
| crates/log/src/subscriber.rs:134 | `pub struct Subscribers` | no hit anywhere | delete |
| crates/log/src/subscriber.rs:86 | `pub fn poll_zero_copy` | no caller | delete |
| crates/log/src/subscriber.rs:193 | `pub fn watermark_a` | no caller | delete |
| crates/log/src/subscriber.rs:119-129 | 7 `pub type` subscriber aliases | only doc-comment mentions in engine, sequencer, executor | delete |
| crates/log/src/supervisor.rs:21 | `pub struct Supervisor` | no hit outside `crates/log/src/supervisor.rs` | delete the module; the cluster starts the driver itself |
| crates/log/src/replay.rs:76 | `pub struct ReplayMergeSubscriber` | only doc-comment mentions (`crates/ingress/.../main.rs:78`) | delete, or keep behind a feature with a tracking note |
| crates/log/src/replay.rs:201 | `pub fn receiver_mut` | no caller in the workspace | delete |
| crates/log/src/replay.rs:555,581 | `open_tx_data_replay`, `open_tx_deposits_replay` | no caller | delete with the module |
| crates/log/src/recorder.rs:53 | `pub fn connect_client` | no caller | make private or delete |
| crates/log/src/recorder.rs:289 | `pub fn Recorder::start_b_mdc` | no caller | delete; `start_stream` covers it |
| crates/log/src/config/mod.rs:361 | `pub struct QuorumConfig` and `LogConfig::quorum` | only `crates/log/src/config/tests.rs:49` reads it | delete the field and the struct |
| crates/log/src/aeron_live/handles/tx_data.rs:34 | `pub fn raw()` (also simple.rs:93,109,125) | no `.raw()` call on any log handle in the workspace | delete the four methods |
| crates/log/src/testing.rs:441 | `pub struct FakeFsyncWatermarkStream` | only `crates/log/tests/testing_fakes.rs` | move behind `#[cfg(test)]` or delete |
| crates/obs/src/lib.rs:46 | `pub const DURATION_BUCKETS` | no other crate reads it | make it private to `init` |
| crates/obs/src/lib.rs:52 | `pub const BUILD_INFO` | no other crate reads it | make private |
| crates/obs/src/lib.rs:56 | `pub const SERVICE_UP` | `crates/bench/src/load/scrape.rs:33` re-declares the literal instead | make private, or have bench import this const |

## R9 defensive validation

| file:line | snippet | why | typed replacement |
|---|---|---|---|
| crates/log/src/publisher.rs:53 | `CString::new(s).map_err(\|e\| ... "channel uri contains NUL")` | the same NUL check runs at 17 sites (subscriber.rs:47, thread.rs:248,255,287, recorder.rs:60,124,139,141,346, replay.rs:309,325,327,332, refetch.rs:478) | parse each URI once at config load into `ChannelUri(CString)` with a fallible constructor; hand `&CStr` down |
| crates/log/src/config/mod.rs:276 | `if base <= 0 \|\| highest > i64::from(u16::MAX)` | raw `i32` port base checked at load, then cast with `as u32` at two more sites | `BasePort(u16)` newtype with `TryFrom<i32>`; `tx_receipts_endpoint` then needs no cast |
| crates/log/src/config/mod.rs:306 | `self.tx_receipts_endpoint_base_port as u32 + 2 * replica_idx` | re-derives a value `validate` already proved safe | use the `BasePort` newtype |
| crates/log/src/config/mod.rs:333 | same cast for the boundary endpoint | second copy of the same arithmetic | one `endpoint_uri(kind, replica)` on the newtype |
| crates/log/src/aeron_live/handles/tx_receipts.rs:264 | `if !ch.tx_receipts_mds_enabled()` | the same predicate is re-checked at 281, 364, 390, plus `Option` unwraps at 143 and 152 | parse `ChannelsConfig` once into `enum ReceiptsTransport { Ipc { channel }, Mds { control, endpoints } }` |
| crates/log/src/refetch.rs:395 | `if term_len <= 0 \|\| (term_len & (term_len - 1)) != 0` | validates an archive descriptor on every call | build `TermLayout { bits, initial_term_id }` once per `FoundRecording` |
| crates/log/src/refetch.rs:403 | `if term_count < 0 { return Err(...) }` | second check on the same descriptor | fold into `TermLayout::position_of(pos)` |
| crates/log/src/replay.rs:289 | `if desc.matching_recordings > 1 { return Err(...) }` | check-then-error on a struct the resolver just built | have `resolve_recording` return `GaplessRecording`, which cannot be built when coverage breaks |
| crates/log/src/aeron_live/runtime.rs:314 | `if uris.is_empty() { return Err(...) }` | guards a slice the caller controls | take `(first: &str, rest: &[&str])`, so the empty case cannot compile |
| crates/obs/src/lib.rs:71 | `if host_id.is_empty() { return Err(anyhow!("host_id must be non-empty")) }` | raw `&str` validated inside the library | `HostId::new(&str) -> Result<HostId>` parsed at the CLI boundary; `init` then takes `HostId` |

## R10 imperative style

| file:line | snippet | functional form |
|---|---|---|
| crates/log/src/refetch.rs:196 | `let mut delivered = 0u64; drain(rx, \|x\| { sink(x); delivered += 1; })` | make `drain` return `u64` (a `fold` over received items); the caller then writes `let delivered = drain(rx, sink);` |
| crates/log/src/refetch.rs:439 | `let mut from_record_id: i64 = 0; loop { ... }` | one shared `page_catalog` helper; the third copy of this loop lives at replay.rs:483 and recorder.rs:568 |
| crates/log/src/refetch.rs:454 | `let v = recs.take(); let max_id = v.iter().map(...).max(); recs.set(v);` | replace `Rc<Cell<Vec<_>>>` with `Rc<RefCell<Vec<_>>>`, then `recs.borrow().iter().map(..).max()` with no take/set dance |
| crates/log/src/aeron_live/pending.rs:163 | `let mut keep: VecDeque<..> = VecDeque::with_capacity(..); while let Some(item) = pending.pop_front()` | `pending.retain_mut(\|item\| ...)`; it keeps FIFO order and drops the second allocation |
| crates/log/src/aeron_live/pending.rs:162 | `let mut blocked: Vec<u32> = Vec::new();` with `blocked.contains(&id)` | `HashSet<u32>`; the scan is linear per pending frame on the Aeron thread |
| crates/log/src/aeron_live/handles/tx_receipts.rs:228 | `for r in batch { let _ = msg_tx.send((pos, r)); }` | `batch.into_iter().for_each(\|r\| { let _ = msg_tx.send((pos, r)); })` |
| crates/log/src/testing.rs:154 | `let mut delivered = 0; while delivered < limit && self.cursor < g.log.len()` | `g.log[self.cursor..].iter().take(fragment_limit)` plus a `count()`, then advance the cursor once |
| crates/obs/src/lib.rs:124 | `let mut attempt: u32 = 0; while attempt < bind_retries && ...` | `for attempt in 1..=bind_retries { if !is_addr_in_use(..) { break } ... }` |
| crates/log/src/recorder.rs:592 | `match found.borrow().latest { Some(max_id) => from_record_id = max_id + 1, None => break }` | fold into the shared `page_catalog` helper; `find_or_start_recording`'s `logged_waiting` flag stays imperative on purpose (log once inside a wait loop) |

Not flagged on purpose: `crates/log/src/aeron_live/thread.rs:113` (`for entry in
subs.iter_mut() { worked \|= ... }`). This is the per-iteration hot path, and it
must poll every subscription, so `any` (which short-circuits) is wrong here.

## Tests

### R1 comments

| file:line | snippet (max 80 chars) | why | fix |
|---|---|---|---|
| crates/log/tests/offer_starvation.rs:7 | "The old publish path offered in a blocking spin/sleep loop" | 20 lines of bug history in the module doc | state the invariant the test pins: a parked publish must not delay a live delivery |
| crates/log/tests/offer_starvation.rs:20 | "This once left two executors pinned forever at block 48" | incident story | delete |
| crates/log/tests/offer_starvation.rs:128 | "old code would block the shared thread" | names removed code | say the stream has no subscriber, so every offer parks |
| crates/log/tests/offer_connect_race.rs:4 | "the previous offer loop gave up after a fixed spin burst of about 1024 tries" | removed-design history | state the rule: an offer waits for the subscriber |
| crates/log/tests/offer_connect_race.rs:69 | "With the old fixed-spin offer this returns NOT_CONNECTED in microseconds" | compares against removed code | say what must happen now |
| crates/log/tests/aeron_live_e2e.rs:1 | "Real-Aeron e2e against the new Send-friendly `aeron_live` adapters." | "new" dates the comment | drop "new" |
| crates/log/src/aeron_live/pending.rs:225 | "These pin the behavior that fixes the cluster `tx_ordering` freeze" | inline `#[cfg(test)]` comment naming a past bug | state the invariant |
| crates/log/src/offer_retry.rs:124 | "Regression test: ... The old fixed 1024-spin loop returned Err here." | removed-design history | state the required behavior |
| crates/log/src/config/tests.rs:238 | "A negative base used to wrap through `as u32` into a nonsense port." | past-tense bug note | say the constructor rejects a negative base |
| crates/obs/tests/init_port_in_use.rs:7 | "the original fail-fast contract, now budgeted" | "original ... now" framing | state the current contract |

### R3 large files

None found. The largest test file is `crates/log/tests/testing_fakes.rs` at 202
code lines.

### R6 dynamic dispatch

None found. No `dyn` appears in any `tests/` file or `#[cfg(test)]` module in this
group.

### R10 imperative style

| file:line | snippet | functional form |
|---|---|---|
| crates/obs/tests/init_without_runtime.rs:34 | `let mut last_err = None; for _ in 0..5 { ... }` | `(0..5).find_map(\|_\| try_init().ok())`, then `expect` on the result |
| crates/log/tests/offer_starvation.rs:115 | `let mut warmed = false; while !warmed && Instant::now() < deadline` | a `poll_until(deadline, \|\| ...)` helper shared with `offer_connect_race.rs:100` |
| crates/log/tests/offer_connect_race.rs:99 | `let mut got: Option<TxEnvelope> = None; while got.is_none() && ...` | same helper; both files repeat the receive-with-deadline loop |
| crates/log/tests/testing_fakes.rs:219 | `let mut canonical: Vec<u64> = Vec::new();` filled inside a `poll` callback | keep as is; the fake's callback API forces the accumulator |

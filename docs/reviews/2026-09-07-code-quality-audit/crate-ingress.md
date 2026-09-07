# ingress

## Summary
The `ingress` crate is well factored, but its comments carry a large amount of history. R1 is the biggest problem: 24 comment sites name issue numbers (#81, #86, #156, #252), spec documents, or a prior implementation. `pending/mod.rs`, `sig_verify.rs`, `receipt_cache.rs`, and `proxy/submit.rs` are the hot spots. R5 is the second problem by volume: 40 sync and channel sites. Most are justified, but three need action: the `BatchVerifier` `Mutex` plus `Notify` pair is a hand-built channel, `SeenReceipts` locks a map that only one task touches, and the `watch` senders in `PendingReceipts` use no channel behavior at all. R8 shows a wide `pub` surface: only 4 items (`IngressConfig`, `IngressHandle`, `IngressProxy`, `MockChannels`) reach another crate, so 6 modules can drop to `pub(crate)`. No file passes 500 code lines, so R3 is empty. `interop-feed` is a clean wire-contract crate; its only findings are R1 phase markers (E1, E2, P2, v1).

Counts: R1 = 30, R2 = 3, R3 = 0, R4 = 3, R5 = 40, R6 = 1, R7 = 5, R8 = 14, R9 = 7, R10 = 4. Tests: R1 = 6, R10 = 3.

## R1 comments
| file:line | snippet (max 80 chars) | why | fix |
|---|---|---|---|
| crates/ingress/src/pending/mod.rs:29 | "The #81 "pending-registry cleanup on cancelled RPC futures" follow-up leaked" | Issue reference plus history. | State the rule: one removal site, in `Drop`. Delete the #81 story. |
| crates/ingress/src/pending/mod.rs:116 | "that the old `Arc<Mutex<Watermarks>>` hand-built" | Names a prior implementation. | Say what `watch` gives: latest value, lock-free read. |
| crates/ingress/src/pending/mod.rs:124 | "A watermark tick used to snapshot and walk the entire registry" | Past-tense comparison with old code. | Say the tick costs O(released + log parked) and allocates nothing. |
| crates/ingress/src/pending/mod.rs:173 | "The cancelled-future path used to leak the entry forever; see the #81" | History plus issue reference. | Say the wait owns the only strong `Arc`, so every end path frees it. |
| crates/ingress/src/pending/mod.rs:230 | "The re-read is a lock-free `borrow` now." | "now" implies a prior state. | Say "The re-read is a lock-free `borrow`." |
| crates/ingress/src/pending/mod.rs:404 | "the #81 follow-up. `Drop` also reaps the map slot, guarded by" | Issue reference. | Delete the reference. Keep the identity-guard rule. |
| crates/ingress/src/aeron_adapters.rs:7 | "Both used to live inside the `kardamom-ingress` binary." | Move history. | Delete. Say the module is unit-testable without a media driver. |
| crates/ingress/src/aeron_adapters.rs:187 | "It once made the validator unkillable by SIGTERM." | Incident history. | State the rule: the pump task must not hold an `AeronRuntime` clone. |
| crates/ingress/src/aeron_adapters.rs:193 | "used to publish this on Aeron ... only the producer moved." | Migration history. | Say the binary's cluster observer feeds this bus. |
| crates/ingress/src/receipt_cache.rs:13 | "the old `CachedReceipt` side channel is no longer needed" | Names a removed type. | Say one subscription feeds both indexes. |
| crates/ingress/src/receipt_cache.rs:49 | "Before this change, each index stored its own full Receipt clone" | History. | Say both indexes share one `Arc<Receipt>`. |
| crates/ingress/src/receipt_cache.rs:69 | "The previous form, `if .. && let Some(entry) = map.iter().next()`" | Describes deleted code. | Keep lines 63-69, the deadlock rule. Delete lines 69-75. |
| crates/ingress/src/sig_verify.rs:108 | "The old drain().collect() allocated a fresh Vec on every flush window." | History. | Say the scratch buffer is reused across flushes. |
| crates/ingress/src/sig_verify.rs:131 | "This fixes two measured problems:" ... "the old loop ran strictly in sequence" | History framing. | State the two present facts: recovery costs 42µs of CPU; it parallelizes. |
| crates/ingress/src/sig_verify.rs:165 | "which the CI allocation gate caught as +1374 B/op over the 4254 B/op baseline" | Dated CI measurement. | Say `split_off` reallocates per chunk; a shared cursor moves each item once. |
| crates/ingress/src/proxy/mod.rs:127 | "At 8192, a subscriber stalled for about 1.7s ... overflowed the ring" | History of a prior value. | Keep only the present sizing: 32k holds about 7s at 4,800 tx/s. |
| crates/ingress/src/proxy/submit.rs:67 | "is how issue #156 surfaced: an in-cluster nonce-unordered case" | Issue reference. | State the rule: the response must never carry another tx's hash. |
| crates/ingress/src/proxy/submit.rs:117 | "The one place for the cached-receipt identity rule (issue #156)" | Issue reference. | Delete "(issue #156)". |
| crates/ingress/src/proxy/submit.rs:148 | "Before issue #86 fixed this, parked submits pinned every connection slot" | Issue reference plus history. | Say a parked submit pins a connection, so the valve sheds instead. |
| crates/ingress/src/proxy/submit.rs:169 | "Protocol-limit checks (W1b, docs/agents/l1-client-suite-port-spec.md)" | Spec document by name. | Say the checks run before sig-verify to save a recovery. |
| crates/ingress/src/json_rpc.rs:127 | "The proxy's tx_receipts watcher ... maintains `latest_block_number: AtomicU64`." | Restates the callee and leaks a private field name. | Delete. `latest_block_number()` is self-explanatory. |
| crates/ingress/src/json_rpc.rs:262 | "The executor now populates everything else at execution time" | "now" implies a prior state. | Say the executor populates the fields; ingress only reshapes them. |
| crates/ingress/src/json_rpc.rs:300 | "It was hardcoded to Legacy before, so every deposit surfaced as type-0x00" | History. | Say the function maps the real EIP-2718 type. Keep the 0x7E note. |
| crates/ingress/src/config.rs:53 | "jsonrpsee's default of 100 capped end-to-end throughput" | Compares with a prior default. | Say the value must exceed offered rate times receipt latency. |
| crates/ingress/src/config.rs:84 | "64k gave only 13.7s ... looked like phantom must-deliver violations" | History of a prior value. | Keep the present horizon: 128k gives about 27s at 4,800 tx/s. |
| crates/ingress/src/bin/kardamom-ingress/main.rs:66 | "See the TODO(consul-watch) on `ChannelsConfig::tx_receipts_executor_count`." | Points at a TODO in another crate. | Say membership is static at startup. Drop the pointer. |
| crates/ingress/src/bin/kardamom-ingress/main.rs:94 | "Change the default back to `on-quorum` ... once the aggregator is wired in." | Future plan plus "back". | Say the default is `on-offer` because nothing publishes a quorum watermark. |
| crates/ingress/src/bin/kardamom-ingress/main.rs:171 | "v0 config loading ... A future revision will derive Deserialize" | Phase marker and future plan. | Say the TOML file supplies only the `[cluster]` section. |
| crates/ingress/src/bin/kardamom-ingress/main.rs:189 | "for the v0 deployment ... in a follow-up that drives the config from TOML" | Phase marker and future plan. | Say the binary protocol stays off. |
| crates/interop-feed/src/lib.rs:5 | "Shared by BOTH sides on purpose (`docs/specs/egress-node-spec.md` v2)" | Spec document by name and version. | Say both the validator and the watcher use these DTOs. |
| crates/interop-feed/src/lib.rs:24 | "## v1 runs in FEED-TRUST mode ... Spec §5 ... §10 ... a later slice" | Phase markers and spec sections. | Say the wire carries no finality tier, so a destination trusts its feed. |
| crates/interop-feed/src/lib.rs:270 | "**E1 serves this UNSIGNED**: `signature` is absent until E2 lands ... (interop P2" | Three phase markers. | Say `signature` is optional; a consumer that needs one treats `None` as unusable. |
| crates/interop-feed/src/lib.rs:292 | "absent until E2 adds attestation keys" | Phase marker. | Say the field is optional today. |
| crates/interop-feed/src/lib.rs:311 | "consumed by peer chains' quorum checks (E2) and monitoring" | Phase marker. | Delete "(E2)". |

## R2 long methods
| file:line | fn name | approx code lines | helpers to extract |
|---|---|---|---|
| crates/ingress/src/bin/kardamom-ingress/main.rs:166 | `main` | 96 | `build_config(&Args) -> IngressConfig`; `open_aeron_side(&Args, &LogConfig) -> (Publication, Subscription)`, which holds the recorder spawn and barrier; `spawn_cluster_watermark(&Args, &file_cfg, &subscription, &stop) -> Option<LiveCluster>`; `shutdown(handle, stop, recorder_handles, rt)`. |
| crates/ingress/src/json_rpc.rs:185 | `subscribe_receipts` | 55 | `next_event(&mut receipts, &mut errors, &sink) -> Option<ReceiptEvent>`, which owns the `select!`; `apply_sender_filter(&filter, addr) -> bool`; `send_event(&sink, &ReceiptEvent) -> Result<(), _>`. |
| crates/ingress/src/json_rpc.rs:268 | `receipt_to_rpc` | 54 | `rpc_logs(&Receipt) -> Vec<alloy_rpc_types_eth::Log>`; `receipt_envelope(tx_type, ReceiptWithBloom) -> ReceiptEnvelope`. |

## R3 large files
None found. No file in either crate passes 500 code lines. The largest are
`crates/ingress/src/json_rpc.rs` at 345 code lines, `crates/interop-feed/src/lib.rs` at
257, and `crates/ingress/src/pending/mod.rs` at 235. All three stay cohesive:
`json_rpc.rs` is one RPC surface plus its wire adapters, `interop-feed/src/lib.rs` is one
wire contract, and `pending/mod.rs` is one ownership invariant.

## R4 manual drops
| file:line | snippet | class | fix |
|---|---|---|---|
| crates/ingress/src/pending/mod.rs:222 | `drop(e);` | Lock guard release. | Move the gate check and the park insert into a helper, `fn park(&self, entry, tx_idx)`, that takes the guard by value. The guard then ends with the helper, before `release_satisfied().await`. |
| crates/ingress/src/pending/mod.rs:257 | `drop(entry);` | Resource release (strong `Arc`, to let the entry die during the grace sleep). | Hard to remove by scope alone. Change `lookup` to a sibling `lookup_weak(map, key) -> Option<Weak<Mutex<Entry>>>`, so this path never holds a strong `Arc`. |
| crates/ingress/src/bin/kardamom-ingress/main.rs:317 | `drop(rt);` | Resource release (`AeronRuntime`). | Move the serve-and-shutdown body into `async fn run(...) -> Result<()>` that borrows `rt`. `rt` then drops when `main` returns. |

## R5 sync primitives and channels
| file:line | primitive | verdict | reason | fix |
|---|---|---|---|---|
| crates/ingress/src/pending/mod.rs:63 | `DashMap<(Address,u64), Weak<Mutex<Entry>>>` | JUSTIFIED | RPC handler tasks insert and remove; four watcher tasks look up. | Keep. |
| crates/ingress/src/pending/mod.rs:63 | `tokio::sync::Mutex<Entry>` (per entry) | JUSTIFIED | The waiter and the watcher tasks share the take-once responder. | No guard is held across an `await`. Use `std::sync::Mutex` and drop the async lock cost. |
| crates/ingress/src/pending/mod.rs:119 | `tokio::sync::watch::Sender<Option<BPosition>>` (quorum) | JUSTIFIED | One writer task, many reader tasks. | Only `send_replace` and `borrow` are called, so no channel behavior is used. Pack `BPosition` into an `AtomicU64`, or use `arc_swap`. |
| crates/ingress/src/pending/mod.rs:120 | `tokio::sync::watch::Sender<Option<BPosition>>` (local) | JUSTIFIED | Same as the quorum slot. | Same fix. |
| crates/ingress/src/pending/mod.rs:131 | `std::sync::Mutex<ParkedIndex>` | JUSTIFIED | The receipts watcher inserts; both watermark watchers drain. | Keep. Contention is low and the critical section is O(log n). |
| crates/ingress/src/pending/mod.rs:132 | `AtomicU64` (`park_seq`) | JUSTIFIED | A `fetch_add` tiebreaker for the `BTreeMap` key, from several tasks. | Keep. |
| crates/ingress/src/pending/mod.rs:55 | `oneshot::Sender<Result<ReceiptResponse, IngressError>>` | JUSTIFIED | One value passes from a watcher task to the parked handler. | Keep. |
| crates/ingress/src/pending/mod.rs:177 | `oneshot::channel()` | JUSTIFIED | Same site, construction. | Keep. |
| crates/ingress/src/pending/mod.rs:408 | `oneshot::Receiver<...>` in `PendingWait` | JUSTIFIED | The receiver's `Drop` is what reaps the slot. | Keep. |
| crates/ingress/src/sig_verify.rs:69 | `Arc<tokio::sync::Mutex<Vec<VerifyRequest>>>` | REPLACE_WITH_CHANNEL | The mutex only hands `VerifyRequest` values from submit tasks to the flush task. | Use `mpsc::unbounded_channel::<VerifyRequest>()` and `recv_many(&mut buf, depth)` in the flush task. That removes the mutex, the `Notify`, and the scratch swap. |
| crates/ingress/src/sig_verify.rs:71 | `Arc<Notify>` | REPLACE_WITH_CHANNEL | It exists only to wake the flush task after a push into the mutex. | The `mpsc` receiver's `recv_many` already wakes on arrival. Delete the `Notify`. |
| crates/ingress/src/sig_verify.rs:176 | `Arc<std::sync::Mutex<vec::IntoIter<VerifyRequest>>>` | JUSTIFIED | A shared work-stealing cursor over the blocking pool, with a measured allocation reason. | Keep. |
| crates/ingress/src/sig_verify.rs:49 | `oneshot::Sender<Result<(Address,B256), IngressError>>` | JUSTIFIED | One result passes from a blocking worker to the awaiting caller. | Keep. |
| crates/ingress/src/sig_verify.rs:215 | `oneshot::channel()` | JUSTIFIED | Same site, construction. | Keep. |
| crates/ingress/src/seen_receipts.rs:33 | `std::sync::Mutex<Inner>` | REPLACE_WITH_OWNERSHIP | Only `spawn_tx_receipts_watcher` touches this set, and that spawns one task. No other code path calls `insert`. | Move `SeenReceipts` into the watcher task by value, not through `Arc` on the proxy. `Inner` then needs no lock. |
| crates/ingress/src/tx_error_dedup.rs:84 | `std::sync::Mutex<Inner>` | JUSTIFIED | Two tasks share it: the receipts watcher calls `record_success`, the errors watcher calls `observe_error`. | Keep. |
| crates/ingress/src/receipt_cache.rs:28 | `Arc<DashMap<(Address,u64), Arc<Receipt>>>` | JUSTIFIED | The receipts watcher writes; every RPC handler task reads. | Drop the inner `Arc`. The struct already lives behind `Arc<ReceiptCache>`, so this is a needless pointer hop. |
| crates/ingress/src/receipt_cache.rs:29 | `Arc<DashMap<B256, Arc<Receipt>>>` | JUSTIFIED | Backs `eth_getTransactionReceipt` from many handler tasks. | Same fix. |
| crates/ingress/src/rate_limit.rs:32 | `Arc<DashMap<IpAddr, Arc<DirectLimiter>>>` | JUSTIFIED | Every RPC handler task reads and inserts per client IP. | Drop the outer `Arc`. Better: use `governor`'s keyed `RateLimiter<IpAddr, DashMapStateStore, _>`, which also bounds the map. The map grows without limit today. |
| crates/ingress/src/proxy/mod.rs:108 | `Arc<AtomicU64>` (`correlation_seq`) | JUSTIFIED | Many handler tasks call `fetch_add`. | Keep. |
| crates/ingress/src/proxy/mod.rs:113 | `Arc<AtomicU64>` (`latest_block_number`) | JUSTIFIED | One writer task, many readers, `fetch_max` only. | Keep. |
| crates/ingress/src/proxy/mod.rs:118 | `broadcast::Sender<Receipt>` (`receipt_feed`) | JUSTIFIED | One producer fans out to N subscription tasks. | Keep. |
| crates/ingress/src/proxy/mod.rs:121 | `broadcast::Sender<TxError>` (`tx_error_feed`) | JUSTIFIED | Same pattern. | Keep. |
| crates/ingress/src/proxy/mod.rs:187 | `broadcast::channel(FEED_CAPACITY)` | JUSTIFIED | Construction of `receipt_feed`. | Keep. |
| crates/ingress/src/proxy/mod.rs:188 | `broadcast::channel(FEED_CAPACITY)` | JUSTIFIED | Construction of `tx_error_feed`. | Keep. |
| crates/ingress/src/proxy/mod.rs:33 | `broadcast::Receiver<T>` in `spawn_broadcast_watcher` | JUSTIFIED | The one drain helper for all four watcher tasks. | Keep. |
| crates/ingress/src/channels.rs:45,47,51,54,59 | trait methods return `broadcast::Receiver<T>` | JUSTIFIED | The proxy needs a multi-consumer stream per bus. | Keep the fan-out. The concrete `broadcast::Receiver` in the trait signature blocks any other transport; return `impl Stream<Item = T>` instead. |
| crates/ingress/src/channels.rs:71 | `Vec<mpsc::UnboundedSender<TxEnvelope>>` | JUSTIFIED | One sender per shard, one consumer per shard. | Keep. This is a test double, so unbounded is acceptable. |
| crates/ingress/src/channels.rs:72-76 | 5 `broadcast::Sender<_>` fields | JUSTIFIED | Test-side fan-out that mirrors the live buses. | Keep. |
| crates/ingress/src/channels.rs:87 | `mpsc::unbounded_channel()` | JUSTIFIED | Per-shard construction. | Keep. |
| crates/ingress/src/channels.rs:91-95 | 5 `broadcast::channel(1024)` | JUSTIFIED | Construction of the five buses. | Name the 1024 as a constant. It differs from `FEED_CAPACITY` (32768) with no stated reason. |
| crates/ingress/src/aeron_adapters.rs:141-145 | 5 `broadcast::Sender<_>` fields | JUSTIFIED | One pump task per stream fans out to many watcher tasks. | Keep. |
| crates/ingress/src/aeron_adapters.rs:155-159 | 5 `broadcast::channel(1024)` | JUSTIFIED | Construction of the live buses. | Name the 1024 as a constant, shared with `channels.rs`. |
| crates/ingress/src/aeron_adapters.rs:97 | `broadcast::Sender<S::Item>` in `spawn_pump` | JUSTIFIED | The one fan-out pump for all four streams. | Keep. The pump drops the send result, so a lagging watcher loses items silently. Count the drop. |
| crates/ingress/src/aeron_adapters.rs:108 | `mpsc::UnboundedReceiver<(BPosition, T)>` as a `PumpSource` | JUSTIFIED | The detached receiver keeps the `AeronRuntime` out of the pump task. | Keep. |
| crates/ingress/src/aeron_adapters.rs:232 | `watermark_sender()` returns `broadcast::Sender<QuorumWatermark>` | JUSTIFIED | The binary's cluster thread must publish into this bus. | Narrow the surface: expose `fn publish_watermark(&self, w: QuorumWatermark)` instead of the raw sender. |
| crates/ingress/src/json_rpc.rs:192 | `broadcast::Receiver<Receipt>` | JUSTIFIED | One receiver per WebSocket subscription task. | Keep. |
| crates/ingress/src/json_rpc.rs:193 | `broadcast::Receiver<TxError>` | JUSTIFIED | Same. | Keep. |
| crates/ingress/src/bin/kardamom-ingress/recorders.rs:51 | `oneshot::channel()` (readiness) | JUSTIFIED | One readiness report crosses from an std thread to the async barrier. | Keep. |
| crates/ingress/src/bin/kardamom-ingress/main.rs:222 | `CancellationToken` | JUSTIFIED | One stop signal reaches N recorder threads and the watermark thread. | Keep. |

Aeron URI strings, for example `tx_data_channel(sid)` and `tx_receipts_control_channel`,
are domain names, not Rust channels. This table excludes them. `crates/interop-feed` holds
no sync primitive and no channel.

## R6 dynamic dispatch
| file:line | snippet | class | generic replacement |
|---|---|---|---|
| crates/ingress/src/json_rpc.rs:407 | `type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;` | Trait-object dispatch for a `tower::Service` associated type. | The concrete future is nameable: `type Future = tokio::task::futures::TaskLocalFuture<std::cell::Cell<Option<IpAddr>>, S::Future>;`. Return `PEER_ADDR.scope(cell, inner.call(req))` directly. That removes one heap allocation per HTTP request on the hot path. |

No `Box<dyn Error>` exists in either crate. `anyhow` is already a dependency of `ingress`.

## R7 too many generics
| file:line | item | params/bounds | supertrait proposal |
|---|---|---|---|
| crates/ingress/src/binary.rs:82 | `handle_connection<W, P, S>` | 3 type params, 9 bounds | Define `trait ProxyBackend: Send + Sync + 'static { type Pub: IngressPublication + Clone + 'static; type Sub: IngressSubscription + Clone + 'static; }`. The function then takes `W: AsyncStream` and `IngressProxy<B::Pub, B::Sub>`. Add `trait AsyncStream: AsyncRead + AsyncWrite + Unpin {}` with a blanket impl to collapse the `W` bounds. |
| crates/ingress/src/proxy/mod.rs:33 | `spawn_broadcast_watcher<T, F, Fut>` | 3 type params, 5 bounds | `Fut` exists only to name `F::Output`. Take `F: AsyncFn(T) + Send + 'static` (or an `async_fn_in_trait` handler trait `trait Watch<T> { async fn on(&mut self, item: T); }`) and drop the third parameter. |
| crates/ingress/src/proxy/mod.rs:85 | `struct IngressProxy<P, S>` | 2 type params, 4 bounds, repeated on 5 impl blocks | The same `ProxyBackend` supertrait: `struct IngressProxy<B: ProxyBackend>` with `B::Pub` and `B::Sub` as associated types. One bound replaces four, on every impl block. |
| crates/ingress/src/json_rpc.rs:98 | `struct IngressHandlers<P, S>` | 2 type params, 6 bounds, repeated on 4 impl blocks | Same `ProxyBackend` supertrait. |
| crates/ingress/src/json_rpc.rs:399 | `impl<S, Body> Service<hyper::Request<Body>> for PeerAddrService<S>` | 2 type params, 6 bounds | Add `trait HttpService<Body>: Service<hyper::Request<Body>> + Clone + Send + 'static where Self::Future: Send + 'static {}` with a blanket impl. The impl then reads `impl<S: HttpService<Body>, Body: Send + 'static>`. |

## R8 unnecessary pub
| file:line | item | evidence (who uses it) | fix |
|---|---|---|---|
| crates/ingress/src/channels.rs:71 | `pub tx_data_tx` | No read outside `channels.rs`. `MockChannels::new` returns the receivers. | Make the field private. |
| crates/ingress/src/channels.rs:74 | `pub local_fsync_bus` | No use anywhere outside `channels.rs`. | Make the field private, or delete it if no test ever drives a local-fsync gate. |
| crates/ingress/src/channels.rs:75 | `pub block_boundary_bus` | No use anywhere outside `channels.rs`. | Make the field private. |
| crates/ingress/src/rate_limit.rs:30 | `pub struct PerIpLimiter` | Only `proxy/mod.rs` uses it. No test or bench imports it. | `pub(crate)`. |
| crates/ingress/src/rate_limit.rs:18 | `pub struct RateLimited` | Only `rate_limit.rs` returns it; `submit.rs` discards it. | `pub(crate)`. |
| crates/ingress/src/receipt_cache.rs:27 | `pub struct ReceiptCache` | Only `proxy/mod.rs` uses it. | `pub(crate)`, and make `crate::receipt_cache` a private module. |
| crates/ingress/src/seen_receipts.rs:32 | `pub struct SeenReceipts` (and `DEFAULT_CAPACITY:29`) | Only `proxy/mod.rs` and `watchers.rs` use them. | `pub(crate)`. |
| crates/ingress/src/tx_error_dedup.rs:83 | `pub struct TxErrorDedup` (and `DEFAULT_WINDOW:54`, `DEFAULT_CAPACITY:59`) | Only `proxy/mod.rs` and `watchers.rs` use them. | `pub(crate)`. |
| crates/ingress/src/binary.rs:29-35 | `pub const STATUS_OK` and 6 sibling status constants | Only `binary.rs` reads them, including its own inline test. | `pub(crate)`. Keep them public only if an external client crate must decode the frames. |
| crates/ingress/src/binary.rs:37 | `pub const MAX_FRAME_BYTES` | Only `binary.rs` reads it. | `pub(crate)`. |
| crates/ingress/src/binary.rs:82 | `pub async fn handle_connection` | Called by the two spawn helpers in the same file, and by its inline test. | `pub(crate)`. |
| crates/ingress/src/proxy/mod.rs:54 | `pub fn pack_correlation_id` | Only `next_correlation_id` and the inline test call it. `ingress_id_of` is the one an external test uses. | `pub(crate)`. |
| crates/ingress/src/json_rpc.rs:98 | `pub struct IngressHandlers` | Only `start_jsonrpc_server` constructs it, in the same file. | `pub(crate)`. |
| crates/ingress/src/pending/mod.rs:152 | `pub fn with_error_grace` (and `DEFAULT_TX_ERROR_GRACE:109`) | Only `src/pending/tests.rs` calls the constructor. | `pub(crate)`, or gate the constructor behind `#[cfg(test)]`. |

Only 4 items reach another crate: `IngressConfig`, `IngressHandle`, `IngressProxy`, and
`MockChannels`, all through `crates/bench`. The `ingress` integration tests and benches are
separate compilation units, so `routing::partition_for`, `sig_verify::recover_single`,
`sig_verify::BatchVerifier::with_parallelism`, `json_rpc::start_jsonrpc_server`,
`proxy::ingress_id_of`, and the `metrics` name constants must stay `pub`. Every `pub` item
in `crates/interop-feed` is used by `crates/validator` or `crates/da_watcher`.

## R9 defensive validation
| file:line | snippet | why | typed replacement |
|---|---|---|---|
| crates/ingress/src/sig_verify.rs:88 | `assert!(depth > 0);` | Panics on a raw `usize` constructor input, in non-test code. | Take `depth: NonZeroUsize`. Parse the CLI or config value once at the boundary. |
| crates/ingress/src/seen_receipts.rs:46 | `assert!(capacity > 0, "SeenReceipts capacity must be > 0");` | Same check, same shape, third copy in the crate. | Take `capacity: NonZeroUsize`. |
| crates/ingress/src/tx_error_dedup.rs:97 | `assert!(capacity > 0, "TxErrorDedup capacity must be > 0");` | Same check, fourth copy. | Take `capacity: NonZeroUsize`. |
| crates/ingress/src/receipt_cache.rs:35 | `assert!(capacity > 0);` | Same check, with no message. | Take `capacity: NonZeroUsize`. Make `IngressConfig::receipt_cache_capacity` a `NonZeroUsize`, so the check happens once. |
| crates/ingress/src/routing.rs:11 | `debug_assert!(m > 0, "partition count must be positive");` | Guards a raw `u32` that a release build never checks. A zero divides by zero. | Make `IngressConfig::partition_count_m` a `NonZeroU32` and take `m: NonZeroU32`. |
| crates/ingress/src/binary.rs:98 | `if len > MAX_FRAME_BYTES { ... "frame too large" }` | Check-then-error on a raw `u32` read off the socket. | Parse into a `FrameLen` newtype with `TryFrom<u32>`, then read `len.get()` with no re-check. |
| crates/ingress/src/proxy/submit.rs:178 | `if env.gas_limit() > kardamom_types::limits::TX_GAS_LIMIT_CAP` | Check-then-error on a raw `u64` inside the submit path. | Parse once into a `GasLimit` newtype with a fallible constructor, next to the `Eip4844` rejection above it. Both then read as one `validate_protocol_limits(&env)?` call. |

`crates/interop-feed`'s `OutboxMessageDto::into_outbox_message` (line 196) already follows
the wanted pattern: it parses once at the wire boundary into `OutboxMessage`, with a
`FeedDecodeError`. Nothing re-checks the value downstream. Take it as the reference.

## R10 imperative style
| file:line | snippet | functional form |
|---|---|---|
| crates/ingress/src/aeron_adapters.rs:45 | `let mut tx_data = Vec::with_capacity(shards as usize); for sid in 0..shards { ... tx_data.push(h); }` | `let tx_data = (0..shards).map(\|sid\| TxDataPublisherHandle::open(rt, channels, sid).map_err(...)).collect::<Result<Vec<_>, _>>()?;` |
| crates/ingress/src/channels.rs:84 | Two `Vec`s filled by one `for _ in 0..shards` loop | `let (tx_vec, rx_vec): (Vec<_>, Vec<_>) = (0..shards).map(\|_\| mpsc::unbounded_channel()).unzip();` |
| crates/ingress/src/sig_verify.rs:177 | `let mut handles = Vec::with_capacity(workers); for _ in 0..workers { handles.push(tokio::task::spawn_blocking(...)); }` | `let handles: Vec<_> = (0..workers).map(\|_\| { let cursor = cursor.clone(); tokio::task::spawn_blocking(move \|\| { ... }) }).collect();` |
| crates/ingress/src/receipt_cache.rs:76 | `let victim = map.iter().next().map(\|e\| *e.key()); if let Some(key) = victim { map.remove(&key); }` | The two statements are load-bearing: they drop the shard read guard before `remove`. Keep the form, but move it into `fn take_victim_key(map) -> Option<K>` so the guard scope is the function body, not a comment. |

Not flagged: `crates/ingress/src/proxy/mod.rs:40` `loop { match rx.recv().await ... }`.
`broadcast::Receiver` has no `IntoIterator`, and the loop must skip `Lagged` while it
breaks on `Closed`. `crates/ingress/src/cluster.rs:48` and
`crates/ingress/src/json_rpc.rs:199` need the same skip-and-continue shape.
`crates/ingress/src/bin/kardamom-ingress/recorders.rs:45` already uses `map` and `unzip`.

## Tests

### R1 comments
| file:line | snippet (max 80 chars) | why | fix |
|---|---|---|---|
| crates/ingress/tests/sig_verify_batch.rs:114 | "Two calibrations, both learned from CI failures (issue #252)" | Issue reference. | State the two present rules: use thread CPU time; use a ratio bound. |
| crates/ingress/tests/sig_verify_batch.rs:115 | "The old timer assert measured the OS scheduler." | Describes a deleted assert. | Say wall clock measures the scheduler, so the test uses CPU time. |
| crates/ingress/tests/replicated_cluster_test.rs:10 | "D1 through D4 of docs/agents/resilient-ingress-spec.md" | Spec document and clause ids. | Name the invariants in the file: one publish per submit, one execution per hash. |
| crates/ingress/tests/protocol_limits_test.rs:2 | "(W1b, docs/agents/l1-client-suite-port-spec.md)" | Spec document by name. | Delete the reference. The EIP numbers below it are self-contained. |
| crates/ingress/src/pending/tests.rs:79 | "Before this fix, a silent evict permanently gapped the sender" | History. | Say an `Evicted` error must release the parked submit. |
| crates/ingress/src/pending/tests.rs:357 | "--- Cancelled-future cleanup, the #81 follow-up ---" | Issue reference in a section banner. | "--- Cancelled-future cleanup ---". |

### R3 large files
None found. `crates/ingress/src/pending/tests.rs` is the largest test file at 461 raw
lines, well under the threshold.

### R6 dynamic dispatch
None found.

### R10 imperative style
| file:line | snippet | functional form |
|---|---|---|
| crates/ingress/tests/replicated_cluster_test.rs:131 | `let mut drains = Vec::new(); for (shard, mut rx) in receivers.into_iter().enumerate() { drains.push(tokio::spawn(...)); }` | `let drains: Vec<_> = receivers.into_iter().enumerate().map(\|(shard, mut rx)\| tokio::spawn(async move { ... })).collect();` |
| crates/ingress/tests/end_to_end_test.rs:47 | `let mut handles = Vec::new(); for mut rx in partition_rx.drain(..) { handles.push(...) }` | `let handles: Vec<_> = partition_rx.drain(..).map(\|mut rx\| tokio::spawn(async move { ... })).collect();` |
| crates/ingress/tests/batched_sig_verify_test.rs:16 | Three `Vec`s (`single_results`, `batched_futs`, `expected`) filled in one loop | Build one `Vec<(Address, B256, _)>` with `map`, then `unzip` or destructure. One pass, one collection. |

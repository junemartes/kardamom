# ingress

## Summary

The test fixtures are the largest duplication in this group. Six copies of the
same "sign a legacy tx and RLP-encode it" block exist, and nine copies of the
same fake sequencer and executor drain loop exist. One `test_support` module
inside the crate, behind a `test-support` feature, replaces all of them. It
also serves the two benches and `crates/bench`, which `tests/common/mod.rs`
cannot reach. The production code is much tighter. The largest production
shapes are the three `PumpSource` impls, the two five-method
`IngressSubscription` forwarders, and the clone dance at the five
`spawn_broadcast_watcher` call sites. Estimated reduction: about 80 lines in
production code and about 380 lines in tests and benches, so about 460 lines
for the group. Another 68 lines drop in peer crates if the `/metrics` scrape
helper moves to `kardamom-obs`. Prior-audit status: `obs::bin`,
`AeronRuntime::spawn`, the MDS `open_auto` helper, and the recorder-thread
helper are all done. The flattenable `ObsArgs` is still open across six bins.

## R14 production code

| sites | shape | helper (name, signature, home) | lines saved |
| --- | --- | --- | --- |
| `crates/ingress/src/aeron_adapters.rs:116-121`, `:123-128`, `:130-135` | Three `impl PumpSource` blocks differ only in the handle type and the destructured name. | `macro_rules! impl_pump_source { ($handle:ty => $item:ty) }` in `crates/ingress/src/aeron_adapters.rs` | 12 |
| `crates/ingress/src/channels.rs:127-143`, `crates/ingress/src/aeron_adapters.rs:237-253` | Both types hold five `broadcast::Sender` fields and forward five `subscribe()` calls. | `pub struct SubscriptionBuses { receipts, watermarks, local_fsync, block_boundaries, tx_errors }` plus `impl<T: AsRef<SubscriptionBuses>> IngressSubscription for T`, in `crates/ingress/src/channels.rs` | 16 |
| `crates/ingress/src/proxy/watchers.rs:19+21-25`, `:46-52`, `:87-97`, `:125-127`, `:133-135` | Each watcher clones its captured `Arc`s a second time inside the `FnMut` closure. | Change to `fn spawn_broadcast_watcher<T, C, F, Fut>(rx: broadcast::Receiver<T>, ctx: C, f: F) where C: Clone + Send + 'static, F: Fn(C, T) -> Fut` in `crates/ingress/src/proxy/mod.rs` | 14 |
| `crates/ingress/src/error.rs:44-64` and 13 sites of `IngressError::Internal(format!("...: {e}"))` (`aeron_adapters.rs:48,65,72,74,181,200,210,215`; `json_rpc.rs:356,359,365`; `proxy/mod.rs:280`) | The same wrap-with-context closure repeats at every fallible open or bind. | `impl IngressError { pub fn internal<E: Display>(ctx: &'static str) -> impl FnOnce(E) -> Self }` in `crates/ingress/src/error.rs` | 10 |
| `crates/ingress/src/binary.rs:47-57`, `:59-79` | The same accept-then-spawn-then-handle loop runs for TCP and for UDS. | `fn spawn_accept_loop<L: Accept>(listener: L, proxy: IngressProxy<P, S>) -> JoinHandle<io::Result<()>>` plus a small `Accept` trait, in `crates/ingress/src/binary.rs` | 8 |
| `crates/ingress/src/channels.rs:84-90` | A `for _ in 0..shards` loop fills two `Vec`s that a single `unzip` gives. | Use `(0..shards).map(\|_\| mpsc::unbounded_channel()).unzip()`; no new helper. | 5 |
| `crates/ingress/src/tx_error_dedup.rs:166-170`, `:183-187` | The same "pop the queue entry, compare the timestamp, remove the map row" block runs in `purge` and in `insert`. | `fn remove_if_current(&mut self, key: (Address, u64), at: Instant)` in `crates/ingress/src/tx_error_dedup.rs` (`impl Inner`) | 4 |
| `crates/ingress/src/json_rpc.rs:145-149`, `:174-178` | Both submit handlers read `PEER_ADDR` and fall back to loopback. | `fn client_ip() -> IpAddr` in `crates/ingress/src/json_rpc.rs` | 4 |
| `crates/ingress/src/pending/mod.rs:218-220`, `:350-353`, `crates/ingress/src/sig_verify.rs:182-184` | The same `lock().unwrap_or_else(PoisonError::into_inner)` idiom repeats. | `fn lock_ignore_poison<T>(m: &std::sync::Mutex<T>) -> MutexGuard<'_, T>` in a new `crates/ingress/src/sync_util.rs` | 4 |
| `crates/ingress/src/json_rpc.rs:211-214`, `:229-232` | Both arms of the subscription `select!` map `Lagged(n)` to a lag frame and break on `Closed`. | `macro_rules! lag_or_break` local to `crates/ingress/src/json_rpc.rs` | 3 |
| `crates/interop-feed/src/lib.rs:186-190`, `:221-225` | `CallbackDto` and `Callback` copy three fields one by one, in both directions. | `impl From<Callback> for CallbackDto` and `impl From<CallbackDto> for Callback` in `crates/interop-feed/src/lib.rs` | 2 (removes wire-contract drift) |
| `crates/ingress/src/bin/kardamom-ingress/main.rs:98-103` plus five peer bins (`batcher:150,154`; `da_watcher:114,117`; `executor/args.rs:138,141`; `sequencer:86,89`; `validator/args.rs:101,104`) | Every bin declares `metrics_addr` and `host_id` with the same env names and defaults. | `#[derive(clap::Args)] pub struct ObsArgs { metrics_addr, host_id }` in `crates/obs/src/bin.rs`, flattened per bin. | 25 (workspace) |
| `crates/ingress/src/receipt_cache.rs:58-81`, `crates/ingress/src/seen_receipts.rs:60-72`, `crates/ingress/src/tx_error_dedup.rs:157-189` | Three bounded caches each evict on insert. | KEEP, differs in eviction rule: arbitrary DashMap victim, FIFO ring, and TTL plus FIFO. The `ReceiptCache` body also encodes a deadlock fix that a generic form would hide. | 0 |
| `crates/ingress/src/binary.rs:130-139`, `crates/ingress/src/error.rs:46-62` | Two exhaustive classifications of `IngressError`, one to a status byte and one to a JSON-RPC code. | KEEP, differs in wire protocol: the two code spaces are independent, and each must stay pinned. Note the drift risk: both use a catch-all arm, so a new variant silently becomes "internal" in both. | 0 |
| `crates/interop-feed/src/lib.rs:74-85`, `:254-265` | `OutboxCursor` and `AttestationCursor` are the same single-`u64` cursor with a `new`. | KEEP, differs in wire field name: `seq` and `blockNumber` are pinned shapes, and each extends on its own axis. | 0 |
| `crates/ingress/src/proxy/watchers.rs:122-129`, `:131-138` | The two watermark watchers differ only in the subscribe call and the update call. | KEEP, differs in nothing structural, but a merged form needs two closures and saves no lines. The row above (`spawn_broadcast_watcher` context) already shortens both. | 0 |

## R14 tests

| sites | shape | helper (name, signature, home) | lines saved |
| --- | --- | --- | --- |
| `crates/ingress/tests/common/mod.rs:15-36`, `:44-63`, `:66-89`, `crates/ingress/src/sig_verify.rs:254-276`, `crates/ingress/tests/sig_verify_batch.rs:18-39`, `crates/ingress/benches/latency.rs:17-36`, `crates/ingress/benches/throughput.rs:18-37`, `crates/ingress/tests/stage_costs.rs:10-29` | Six copies build a `TxLegacy`, sign the prehash, convert the parity, and RLP-encode it. The gas and blob variants repeat the same nine-line tail. | `pub fn sign_and_encode<T>(tx: T, s: &PrivateKeySigner) -> (TxEnvelope, Bytes)` plus `sign_legacy`, `sign_legacy_tx`, `sign_legacy_with_gas`, `sign_eip4844`, in a new `crates/ingress/src/test_support.rs`, gated `#[cfg(any(test, feature = "test-support"))]`. This deletes `tests/common/mod.rs`. | 110 |
| `crates/ingress/tests/end_to_end_test.rs:46-74`, `:139-158`, `:216-237`, `:303-333`, `crates/ingress/tests/routing_test.rs:37-65`, `crates/ingress/benches/latency.rs:57-86`, `crates/ingress/benches/throughput.rs:59-88`, `crates/ingress/tests/replicated_cluster_test.rs:88-98`, `crates/bench/tests/alloc_profile_ingress.rs:73-98` | Nine copies drain each shard, build the same eleven-field `Receipt`, and send it on `receipt_bus`, usually with a watermark. | `pub fn receipt_for(env: &TxEnvelope, pos: BPosition) -> Receipt` and `pub fn spawn_fake_executor(mock: &MockChannels, rx: Vec<UnboundedReceiver<TxEnvelope>>) -> Vec<JoinHandle<()>>`, in `crates/ingress/src/test_support.rs` | 120 |
| `crates/ingress/src/pending/tests.rs:32-34`, `:53-55`, `:86-88`, `:116-118`, `:151-153`, `:185-187`, `:235-237`, `:249-252`, `:274-276`, `:293-295`, `:322-324`, `:454-456` | Twelve tests call `register`, spawn a waiter task, and sleep 10ms. | `fn park(p: &Arc<PendingReceipts>, s: Address, n: u64) -> JoinHandle<Result<ReceiptResponse, IngressError>>` in `crates/ingress/src/pending/tests.rs` | 24 |
| `crates/ingress/tests/end_to_end_test.rs:161-166`, `:239-244`, `:335-340`, `:387-392`, `crates/ingress/tests/replicated_cluster_test.rs:196-203` | Four tests open-code the "reject random signers until one routes to shard 0" loop. One test file already has the helper. | Move `pub fn signer_for_shard(target: u32, m: u32) -> PrivateKeySigner` to `crates/ingress/src/test_support.rs` | 20 |
| `crates/ingress/tests/end_to_end_test.rs:18-23`, `crates/ingress/tests/replicated_cluster_test.rs:35-40`, `crates/ingress/tests/routing_test.rs:98-104`, `crates/ingress/benches/latency.rs:38-42`, `crates/ingress/benches/throughput.rs:39-43` | Five copies decode an RLP tx only to read its nonce. One copy is renamed `extract_nonce`. | `pub fn nonce_of(raw: &bytes::Bytes) -> u64` in `crates/ingress/src/test_support.rs` | 20 |
| `crates/ingress/src/receipt_cache.rs:107-120`, `crates/ingress/tests/receipt_subscription_test.rs:50-63`, `crates/ingress/src/pending/tests.rs:4-14`, `:16-21` | Three test builders make a `Receipt` from a sender, a nonce, a hash, and an offset. One also wraps `BPosition`. | `pub fn receipt(sender: Address, nonce: u64, tx_hash: B256, at: i32) -> Receipt` and `pub fn pos(offset: i32) -> BPosition` in `crates/ingress/src/test_support.rs` | 20 |
| `crates/ingress/src/pending/tests.rs:255-259`, `:305-309`, `:328-332` and the five multi-line `on_tx_error(.., DuplicatedTx { expected_nonce })` calls at `:57-62`, `:121-126`, `:155-160`, `:193-198`, `:214-221` | The same two watcher calls spread over five lines each. | `async fn local_wm(p: &PendingReceipts, position: BPosition)` and `async fn reject_dup(p: &PendingReceipts, s: Address, n: u64, expected: u64)` in `crates/ingress/src/pending/tests.rs` | 15 |
| `crates/ingress/tests/metrics_endpoint.rs:30-47` and the four peer copies (`batcher`, `da_watcher`, `executor`, `sequencer` `tests/metrics_endpoint.rs`) | `free_port` and `scrape` are byte-identical in five crates. | `pub fn free_port() -> SocketAddr` and `pub async fn scrape(url: &str) -> String` in a new `crates/obs/src/testing.rs`, behind a `testing` feature. | 17 in ingress, 68 workspace-wide |
| `crates/ingress/tests/receipt_subscription_test.rs:33-48`, `:70-81`, `crates/ingress/src/json_rpc.rs:444-455`, `crates/ingress/src/binary.rs:148-160` | The same "build config, make `MockChannels`, make the proxy, bind, build a client" sequence. | `pub async fn start_test_server(cfg: IngressConfig) -> (MockChannels, Vec<UnboundedReceiver<TxEnvelope>>, SocketAddr, ServerHandle)` and `pub fn http_client(addr: SocketAddr) -> HttpClient`, in `crates/ingress/src/test_support.rs` | 15 |
| `crates/ingress/tests/receipt_subscription_test.rs:173-177`, `:185-189`, `:225-229` | The same timeout-then-unwrap-three-times chain reads one subscription frame. | `async fn next_event(sub: &mut Subscription<Value>, what: &str) -> Value` in that test file | 9 |
| `crates/ingress/tests/docker_e2e.rs:31-45`, `crates/sequencer/tests/e2e_docker.rs:26-33` | Both assert the harness endpoints start with `127.0.0.1:`. | `pub fn assert_local_endpoints(cluster: &AeronTestCluster, node: usize)` in `kardamom_log::testing` | 10 (workspace) |
| `crates/ingress/tests/common/mod.rs:5` | `#![allow(dead_code)]` hides that each of the six test binaries that declare `mod common;` compiles all four helpers but uses only one or two. | The `test_support` module in the first row removes the file, so the crate compiles one copy instead of six. | counted above |
| `crates/ingress/tests/stage_costs.rs:61-64`, `:70-73` | Two `match e { TxEnvelope::Legacy(t) => .., _ => unreachable!("legacy fixtures") }` arms. | `fn legacy(e: &TxEnvelope) -> &Signed<TxLegacy>` local to that test | 4 |
| 12 sites of `submit_raw("127.0.0.1".parse().unwrap(), raw)` across `end_to_end_test.rs`, `routing_test.rs`, `replicated_cluster_test.rs`, `pending_receipts_test.rs` | Every call re-parses the same loopback address. | KEEP, differs in nothing, but a helper saves no lines. Use a `const LOCAL: IpAddr` in `test_support` for clarity only. | 0 |

## Prior audit items

| item | status (open / done / partly) | note |
| --- | --- | --- |
| `obs::bin::init_tracing()` | done | `crates/obs/src/bin.rs:12`; ingress calls it at `main.rs:167`. |
| `obs::bin::wait_for_shutdown()` | done | `crates/obs/src/bin.rs:23`; ingress calls it at `main.rs:309`. |
| Flattenable `ObsArgs { metrics_addr, host_id }` | open | All six bins still declare both flags. See the production table. |
| `init_service(...)` wrapper for the six `kardamom_obs::init` calls | done | `kardamom_obs::init_service!` at `main.rs:169`. |
| `AeronRuntime::spawn(dir: Option<&Path>)` | done | Used at `main.rs:206` and `:277`; no `match aeron_dir` block is left in ingress. |
| `open_tx_receipts` MDS fan-in helper | done | `TxReceiptsSubscriberHandle::open_auto` in `crates/log/src/aeron_live/handles/tx_receipts.rs:259`; ingress uses it at `aeron_adapters.rs:180` and `:209`. |
| Recorder-thread and ready-barrier helper | done | `kardamom_log::recorder::record_stream_until_stopped`; ingress keeps only the per-shard fan-out in `recorders.rs:38-80`. |
| Shell metrics scrape (`lib-metrics.sh`) | out of group | Not in `crates/ingress` or `crates/interop-feed`. The Rust `/metrics` test copy is still open; see the tests table. |
| `bin_support` recovery additions, consensus-critical duplication, Java harness, e2e scenario helpers | out of group | No site in this group. |

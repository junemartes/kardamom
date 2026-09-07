# sequencer

## Summary

The biggest shape in this group is the epoch pump duplicated as the remote-epoch
pump. `epoch.rs` and `remote_epoch.rs` hold the same trait, the same pump, the
same scripted fake, and five parallel tests, for two record types. The second
biggest shape is test fixtures: `signer`, `signed_envelope`, `pos` and
`one_partition_cfg` are copied into six test files and one bench. Two smaller
shapes repeat many times: `metrics.rs` holds seven dead `record_*` helpers that
`HotMetrics` replaced, and `wire/ingress.rs` plus `wire/egress.rs` write the
`WireError::TooShort { .. }` literal 11 times, although `wire/mod.rs:245` already
owns a `too_short` helper. The four `Live*` adapter structs in the binary only
forward `try_recv`; the library can implement the traits on the log handles
directly. The estimated reduction is about 270 lines of production code and
about 310 lines of test code, so about 580 lines in all. Of the prior-audit
items that touch this group, the `obs::bin` helpers, `AeronRuntime::spawn`, the
tx_receipts fan-in helper and the shared LE byte readers are all done. The
`ObsArgs` clap struct is still open, and `partition_for` is still a consensus
rule in two copies.

## R14 production code

| sites | shape | helper (name, signature, home) | lines saved |
| --- | --- | --- | --- |
| `crates/sequencer/src/bin/kardamom-sequencer/adapters.rs:15-63`, `:71-87` | Four newtype wrappers. Each holds one `kardamom_log` handle and forwards `poll`/`publish`. | Delete the wrappers. Add `impl TxDataSubscriber for TxDataSubscriberHandle` in `crates/sequencer/src/inbound.rs`, `impl EpochSubscriber for TxDepositsSubscriberHandle` in `epoch.rs`, `impl RemoteEpochSubscriber for TxRemoteEpochsSubscriberHandle` in `remote_epoch.rs`, `impl TxErrorPublisher for TxErrorsPublisherHandle` in `outbound.rs`. The trait is local and the type is foreign, so the orphan rule allows it. `kardamom-log` is already a non-optional dependency (`Cargo.toml:31`). | 55 |
| `crates/sequencer/src/epoch.rs:60-97`, `crates/sequencer/src/remote_epoch.rs:65-102` | The same scripted queue fake for two record types: `queue`, `closed`, `push`, `close`, and a `poll` that pops or reports `IngressDisconnected`. | `pub struct ScriptedQueue<T> { queue: Arc<Mutex<VecDeque<(BPosition, T)>>>, closed: Arc<Mutex<bool>> }` with `push`, `close`, `next()` in a new `crates/sequencer/src/fakes.rs`. Each module keeps a two-line trait impl that calls `next()`. | 40 |
| `crates/cluster-adapter/src/wire/ingress.rs:133,142,149,184,191,207`, `crates/cluster-adapter/src/wire/egress.rs:45,87,107,113,121` | The `WireError::TooShort { at, need, have: buf.len().saturating_sub(at) }` literal, written out 11 times. | Make `too_short(b: &[u8], at: usize, need: usize) -> WireError` (`wire/mod.rs:245`) `pub(super)`. Add `rd_slice(b: &[u8], at: usize, len: usize) -> Result<&[u8], WireError>` beside it for the six slice reads. | 40 |
| `crates/sequencer/src/metrics.rs:78-104` | Seven `record_*` helpers that duplicate `HotMetrics` (`:52-76`). No caller outside the smoke test at `:165-180`. | Delete both. `HotMetrics::new` already owns the names. | 35 |
| `crates/sequencer/src/metrics.rs:106-163` | Ten one-line recorders of the form `counter!(NAME, "partition" => p.to_string()).increment(n)` or `gauge!(...).set(v)`. | `fn bump(name: &'static str, partition: u32, n: u64)` and `fn set(name: &'static str, partition: u32, v: f64)` in the same module. Each recorder becomes one call. | 10 |
| `crates/sequencer/src/bin/kardamom-sequencer/feeds.rs:298-315`, `:319-336` | The same pump loop for two origin types: check shutdown, call the process function, reset or sleep the backoff, map `Backpressure` and `IngressDisconnected`. | `fn spawn_origin_pump<F>(shutdown: Shutdown, step: F) -> LoopHandle where F: FnMut() -> Result<bool, SequencerError> + Send + 'static` in `feeds.rs`. | 15 |
| `crates/sequencer/src/resync/mod.rs:273-313`, `:339-368`; `crates/cluster-adapter/src/live/session_loop.rs:253-293`, `:478-504` | The bounded `try_recv` drain: loop, handle the item, break on `Empty`, latch a "dead" flag and warn once on `Disconnected`. | `fn drain_bounded<T>(rx: &Receiver<T>, dead: &mut bool, max: usize, on_dead: &str, mut f: impl FnMut(T))` in each crate. The two crates keep separate copies, because the log messages and the max differ. | 15 |
| `crates/sequencer/src/partition.rs:15-20`, `crates/ingress/src/routing.rs:10-15` | One consensus rule in two copies: `keccak256(sender)[..8]` as big-endian `u64`, then `% m`. Only the `debug_assert` text differs. The doc at `partition.rs:3-7` says the two must match byte for byte. | `pub fn partition_for(sender: Address, m: u32) -> u32` in a new `kardamom_types::routing`. Both crates depend on `kardamom-types` already (`crates/ingress/Cargo.toml:34`). Each crate re-exports it, so call sites do not change. The ingress copy is outside this group. | 12 |
| `crates/cluster-adapter/src/live/mod.rs:168-214` | Four `connect*` wrappers. Each forwards to `connect_inner` with a different `(replay, subscribe, kind_filter)` triple. | `pub struct ConnectOptions { replay: Option<ReplayOnConnect>, subscribe: bool, egress_kind_filter: Option<Vec<u8>> }` with `Default`, plus `pub fn connect_with(rt, cfg, opts)`. Keep `connect` as the one short wrapper. | 8 |
| `crates/cluster-adapter/src/config.rs:51-61` | `to_live` copies six fields one by one into a struct with the same six fields. | `impl From<ClusterConfig> for LiveClusterConfig` after `defaults_applied`, or give `LiveClusterConfig` the serde derives and drop one of the two structs. | 8 |
| `crates/cluster-client/src/session/mod.rs:282-301`, `:318`, `:328-334` | The foreign-session filter, written three times: when connected, compare `cluster_session_id`; when establishing, compare `correlation_id`. | `fn event_is_ours(&self, ev: &SessionEvent) -> bool` on `SessionDriver`. This also removes a drift risk in a safety filter that the doc at `:266-279` calls load bearing. | 10 |
| `crates/sequencer/src/bin/kardamom-sequencer/main.rs:314-330` | Three `match join_X.await` blocks. Each logs the same three outcomes with a different label. | `async fn log_join(h: feeds::LoopHandle, what: &str)` in `feeds.rs`. | 12 |
| `crates/sequencer/src/unconfirmed.rs:70-79`, `:100-111` | Collect the keys of a per-sender nonce range, then remove them. | `fn keys_in(&self, sender: Address, lo: u64, hi: u64) -> Vec<UnconfirmedKey>` on `UnconfirmedLedger`. | 6 |
| `crates/sequencer/src/nonce_decode.rs:85-92`, `:94-101` | The same "read `ll` big-endian length bytes into `l`" loop, in two `skip_rlp_item` match arms. This is the seed's "same loop, second copy". | `fn be_len(b: &[u8], at: usize, ll: usize) -> Option<usize>` in `nonce_decode.rs`. | 6 |
| `crates/sequencer/src/outbound/cluster.rs:86-95`, `:97-105` | Encode a record, map the encode error to `EncodeFailed`, then offer. | `fn offer_encoded(&mut self, r: Result<Vec<u8>, wire::WireError>, what: &str) -> Result<(), SequencerError>` on `ClusterRefPublisher`. | 5 |
| `crates/sequencer/src/bin/kardamom-sequencer/main.rs:85-90` | The `metrics_addr` and `host_id` clap arguments, repeated in all six service binaries. This is the open half of the prior audit's `obs::bin` item. | `#[derive(Parser)] pub struct ObsArgs { metrics_addr, host_id }` in `crates/obs/src/bin.rs`, flattened with `#[command(flatten)]`. Cross-crate. | 5 (in this group) |
| KEEP | `crates/cluster-adapter/src/wire/mod.rs:245` and `crates/cluster-client/src/protocol.rs:109` both hold a 7-line `too_short`. | KEEP, differs in error type. `crates/cluster-client/src/bytes.rs:1-12` states the split on purpose: a session-protocol fault and an app-envelope fault have different handlers. | 0 |
| KEEP | `crates/cluster-adapter/src/watermark.rs:25-28`, `:31-34` | KEEP, differs in meaning. One takes a 0-based index and adds one. The other takes a count. Merging them would hide the off-by-one rule. | 0 |
| KEEP | `crates/sequencer/src/epoch.rs:48-58`, `crates/sequencer/src/remote_epoch.rs:52-63` | KEEP, differs in the metric. The remote pump also records the relay counter. A generic pump would need a callback that costs more than it saves. | 0 |

## R14 tests

| sites | shape | helper (name, signature, home) | lines saved |
| --- | --- | --- | --- |
| `tests/resync_filter.rs:23-66`, `tests/sequencer_step.rs:18-64`, `tests/multi_sequencer_dual_write.rs:41-67`, `tests/replicated_shard_racing.rs:50-84`, `tests/sequencer_integration.rs:31-66`, `tests/alloc_profile.rs:55-86`, `benches/throughput.rs:33-59` | `signer(seed)` is verbatim in seven files. `signed_envelope` is near-verbatim in the same seven, and differs only in `gas_limit`, the calldata, and whether `tx_hash` is real or defaulted. `pos`/`pos_n` and `one_partition_cfg` repeat too. | `pub mod testkit` in `crates/sequencer/src/lib.rs`, behind the existing `testing` feature: `pub fn signer(seed: u64) -> PrivateKeySigner`, `pub fn signed_envelope(s: &PrivateKeySigner, nonce: u64, correlation_id: u64) -> TxEnvelope` (always stamps the real `keccak256` hash), `pub fn envelope_with(spec: EnvelopeSpec, ...)` for the calldata case, `pub fn pos(offset: i32) -> BPosition`, `pub fn one_partition_cfg() -> SequencerConfig`. | 120 |
| `src/resync/tests.rs:104-110,127-133,147-153,166-172,187-193,224-230,233-239`; `tests/resync_filter.rs:111-118,179-186,221-228,252-259` | The `FloorUpdate { deposit, sender, executed_nonce, skip_reason }` literal, written out 13 times. | Constructors on the type in `crates/sequencer/src/resync/mod.rs`: `FloorUpdate::executed(sender, nonce)`, `::skip(sender, nonce, reason)`, `::deposit(sender)`. Each site becomes one line. | 45 |
| `src/session/tests.rs:32-35,93-95,121-123,137-140,156-158,187-189,219-221,241-243,257-259,310-312,337-339,382-384` | The same four-line prologue: build a `SessionDriver`, poll one frame, decode it, take the correlation id. | `fn connecting() -> (SessionDriver, i64)` in `crates/cluster-client/src/session/tests.rs`. The existing `connected(session)` at `:31-39` already does the same for the connected case. | 30 |
| `src/session/tests.rs:160-168`, `:314-322` | The redirect `SessionEvent` literal, twice. This is the seed's 24-line item. | `fn redirect_event(correlation_id: i64, leader: i32, endpoints: &str) -> Vec<u8>` beside `ok_event` and `event` in the same module. | 12 |
| `src/wire/tests.rs:45-51,62-69,212-227,239-252` and two more | `match decode_egress(&b).unwrap() { EgressItem::X { .. } => { asserts }, other => panic!(...) }`, six times. | No helper is needed. `EgressItem` derives `PartialEq` (`wire/egress.rs:21`), so each block becomes one `assert_eq!(decode_egress(&b).unwrap(), EgressItem::X { .. })`. | 35 |
| `tests/replicated_shard_racing.rs:126-136`, `tests/sequencer_integration.rs:107-115`, `tests/multi_sequencer_dual_write.rs`, `tests/alloc_profile.rs:142`, `benches/throughput.rs:94` | Build the rig (scripted inbound, ref publisher, error publisher, sequencer), then drive `run_once` until it returns false, then read the refs. | `pub fn drive_to_idle(cfg: SequencerConfig, stream: &[(TxDataLoc, TxEnvelope)]) -> (Vec<TxRef>, Vec<TxError>)` in the same `testkit` module. | 30 |
| `tests/metrics_endpoint.rs:29-46` (plus batcher, da_watcher, executor, ingress) | `free_port()` and the retrying `scrape(url)`. Five crates hold the same copy. | `pub fn free_port() -> SocketAddr` and `pub async fn scrape(url: &str) -> String` in `crates/obs/src/testing.rs`, behind a `testing` feature. Cross-crate. | 18 (in this group) |
| `src/resync/tests.rs:26-29,34-36,46-48,68-70,80-82,91-93` | Build the controller, take a start instant, then call `calm_down`. | `fn calm_controller() -> (ResyncController, SharedWatermark, Instant)` in the same module, wrapping `mk` and `calm_down`. | 10 |
| `src/epoch.rs:99-181`, `src/remote_epoch.rs:104-199` | Five parallel tests per module: forwards verbatim, idle reports no work, closed reports disconnect, backpressure propagates. Only the record builder and the fake differ. | Once the generic `ScriptedQueue<T>` above lands, `fn pump_contract<S, T>(...)` in `crates/sequencer/src/fakes.rs` can own the three plumbing tests. Each module keeps its own record-specific test. | 15 |
| `src/wire/tests.rs:11-21`, `src/outbound/cluster.rs:157-167`, `tests/end_to_end.rs:78-88` | A `txref()` fixture, three copies with different constants. | `pub fn txref(tag: u8) -> TxRef` in `crates/cluster-adapter/src/wire/mod.rs`, behind the existing `testing` feature. | 15 |
| KEEP | `tests/partition_routing.rs:6-21` and `crates/ingress/src/routing.rs:31-46` both check the partition distribution. | KEEP, differs on purpose. Each crate must prove its own copy of the rule holds. If `partition_for` moves to `kardamom-types` (see above), both tests move with it and this becomes one test. | 0 |

## Prior audit items

| item | status (open / partly / done) | note |
| --- | --- | --- |
| `obs::bin` module: `init_tracing()` and `wait_for_shutdown()` | done | `crates/obs/src/bin.rs:12` and `:22`. The bin calls both at `main.rs:173` and `:311`. |
| `obs::bin`: `init_service(...)` wrapping the six identical incantations | done | `kardamom_obs::init_service!` macro (`crates/obs/src/lib.rs:20`), used at `main.rs:175`. |
| `obs::bin`: flattenable `ObsArgs { metrics_addr, host_id }` | open | The bin still declares both arguments inline at `main.rs:85-90`. |
| `AeronRuntime::spawn(dir: Option<&Path>)` in kardamom-log | done | The three sequencer sites all call `AeronRuntime::spawn(args.aeron_dir.as_deref())` (`main.rs:199`, `:234`, `:271`). No `match aeron_dir` block is left. |
| `open_tx_receipts` MDS fan-in helper | done | The bin calls `TxReceiptsSubscriberHandle::open_auto` (`main.rs:277`). The hand-rolled fan-in is gone. |
| LE byte readers duplicated across cluster crates, shared `bytes` module | done | `crates/cluster-client/src/bytes.rs` exists. `wire/mod.rs:39` and `protocol.rs:105-128` both use it. The residual `too_short` copy is deliberate; see the KEEP row above. |
| "wire.rs and protocol.rs are distinct layers, nothing else to merge" | still true | The two files share no further shape. |
| Consensus-critical duplication list | not in this group | The listed sites are in validator, exec-core, engine, state and the Java sealer. |

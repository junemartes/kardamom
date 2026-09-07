# log

## Summary
The largest shape is a whole second implementation. `crates/log/src/publisher.rs` and
`crates/log/src/subscriber.rs` (456 lines) rebuild the open, offer, decode, and poll
sequences that `aeron_live` already owns. No crate in the workspace uses them. Delete
both, and the twin `decode_position`, the twin header decoder, and the twin CString
helpers go with them. The second shape is the archive catalog page loop, written three
times (recorder, replay, refetch) with a hand-rolled `Handler::leak`/`release` pair each
time; that pattern already caused one leak bug. The third shape is the rkyv trait-bound
block, repeated four times in `aeron_live/runtime.rs`. In tests, the three docker e2e
files repeat one bring-up (docker probe, cluster, config, runtime, receive loop) almost
verbatim, and `TxEnvelope`/`TxRef` literals are written 13 times. Estimated reduction:
about 600 lines in production code and about 255 lines in tests, about 855 in total.
Every prior-audit item that touches this group is done: `obs::bin`, `AeronRuntime::spawn`,
`open_auto`, and `record_stream_until_stopped` all exist and all call sites use them.
The archive-catalog-paging item is the one prior item still open.

## R14 production code

| sites | shape | helper (name, signature, home) | lines saved |
| --- | --- | --- | --- |
| `crates/log/src/publisher.rs:1-248`, `crates/log/src/subscriber.rs:1-208` | A second full transport implementation. Both files repeat the open, offer-with-retry, decode, and poll steps that `aeron_live/{runtime,thread,handles}` already own. No file outside them names `TxDataPublisher`, `TxOrderingPublisher`, `QuorumPublisher`, `TypedSubscriber`, or `Subscribers`. | Delete both files and their two `pub mod` lines in `crates/log/src/lib.rs:33,37`. `aeron_live` is the surviving home. Confirm no crate outside this workspace imports them first. | -450 |
| `crates/log/src/recorder.rs:561-599`, `crates/log/src/replay.rs:472-529`, `crates/log/src/refetch.rs:436-462` | Page the archive catalog: `CString::new("")`, `Handler::leak(consumer)`, `list_recordings_for_uri`, `release()`, break on a short page, else advance to `max_id + 1`. | `pub(crate) fn for_each_recording(archive: &Archive, stream_id: i32, f: impl FnMut(&AeronArchiveRecordingDescriptor)) -> Result<(), LogError>` in a new `crates/log/src/archive_catalog.rs`. | -27 |
| `crates/log/src/aeron_live/runtime.rs:275-286`, `:302-313`, `:331-342`, `:382-387` | The same 6-line rkyv `where` clause (`Archive + Send + 'static`, `Deserialize`, `CheckBytes`) on four items. | `pub trait TypedMsg: rkyv::Archive + Send + 'static { }` with a blanket impl, in `crates/log/src/codec.rs`. Each `where` block becomes `T: TypedMsg`. | -24 |
| `crates/log/src/recorder.rs:56-64`, `:120-128`, `crates/log/src/aeron_live/runtime.rs:143-158` | Turn an `aeron.dir` `Path` into a `CString`: check UTF-8, then check the NUL byte, with one error message each. | `pub(crate) fn dir_cstring(dir: &Path) -> Result<CString, LogError>` in a new `crates/log/src/ffi.rs`. | -16 |
| `crates/log/src/aeron_live/thread.rs:248`, `:255`, `:287`, `crates/log/src/recorder.rs:139`, `:141`, `:346`, `crates/log/src/replay.rs:309`, `:325`, `:327`, `:332`, `crates/log/src/refetch.rs:478` | Build a `CString` from a channel or endpoint URI and map the NUL error to `LogError::Aeron`. Eleven copies, each with its own wording. | `pub(crate) fn c_uri(uri: &str, what: &str) -> Result<CString, LogError>` in `crates/log/src/ffi.rs`. One wording for all. | -15 |
| `crates/log/src/aeron_live/handles/tx_data.rs:10-69` | The publisher plus subscriber pair that `declare_channel_handles!` already stamps out five times, written by hand because the item type is `(TxDataLoc, TxEnvelope)`, not `(BPosition, T)`. | Add an optional `item = <ty>; subscribe = <expr>` arm to `declare_channel_handles!` in `crates/log/src/aeron_live/handles/simple.rs`, then move tx_data into it. | -35 |
| `crates/log/src/aeron_live/handles/tx_receipts.rs:259-275`, `:385-401`; guards at `:280-285`, `:363-368`; `recv`/`try_recv` at `:310-316`, `:412-418` | The receipts and boundary subscribers repeat one `open_auto` body, one "MDS not configured" guard, and one `recv`/`try_recv` pair. | `trait MdsSubscriber { const KIND: &'static str; fn open(..); fn open_mds(..); fn endpoint_of(ch, i); fn open_auto(..) { default body } }` plus `fn require_mds(ch) -> Result<(), LogError>`, both in `tx_receipts.rs`. | -25 |
| `crates/log/src/recorder.rs:53-70` | `connect_client` builds an Aeron client that nothing calls. Its body repeats `connect_archive_with_timeout`'s context setup on the other client type. | Delete `connect_client`. | -18 |
| `crates/log/src/aeron_live/thread.rs:121-127`, `:145-158` | The same four-arm `cmd_rx.recv_timeout` match (Shutdown, cmd, Timeout, Disconnected) in both idle branches. | `fn wait_for_cmd(cmd_rx, wait, ..) -> Result<bool, LogError>` in `crates/log/src/aeron_live/thread.rs`; `false` means stop. | -10 |
| `crates/log/src/config/mod.rs:302-307`, `:329-334` | `tx_receipts_endpoint` and `tx_receipts_boundary_endpoint` differ only by `+ 0` and `+ 1`. | `fn tx_receipts_slot(&self, replica_idx: u32, slot: u32) -> Option<String>` in `ChannelsConfig`; both public methods call it. | -6 |
| `crates/log/src/refetch.rs:169-200`, `:252-280` | KEEP, differs in policy. `fetch_tx_data` picks one recording by session and rotates when the drain delivers nothing; `fetch_deposits` loops over every recording and does not rotate. A shared helper would hide the session-keying rule. | Extract only the shared 6-line "build sub_uri, get or open the subscription" step as `fn replay_sub_uri(endpoint: &str, session_id: i32) -> String`. | -8 |
| `crates/log/src/aeron_live/thread.rs:327-335`, `crates/log/src/replay.rs:538-546` | KEEP, differs in type. Both read `(BPosition, session_id)` from a header, but one takes `rusteron_client::AeronHeader` and the other `rusteron_archive::AeronHeader`. The crates expose no shared trait. | None. Keep the two copies and keep the cross-reference comment at `replay.rs:537`. | 0 |

## R14 tests

| sites | shape | helper (name, signature, home) | lines saved |
| --- | --- | --- | --- |
| `crates/obs/tests/init.rs:7-13,48-55`, `crates/obs/tests/init_port_in_use.rs:23-24,43-44,53-63`, `crates/obs/tests/init_without_runtime.rs:33-39,57-76` | Pick a free port, then scrape `/metrics`. Three scrape bodies exist: reqwest, raw `TcpStream` once, and raw `TcpStream` with a retry loop. | `crates/obs/tests/common/mod.rs` with `pub fn free_port() -> SocketAddr` and `pub async fn scrape(addr: SocketAddr) -> String`; each test file adds `mod common;`. | -40 |
| `crates/log/tests/testing_fakes.rs:105-113`, `:114-122`, `:167-172`, `:175-180`, `:183-188`, `:191-196`, `crates/log/tests/codec_roundtrip.rs:30-38`, `:61-69` | A `TxRef` literal with `tx_hash: B256::ZERO` and `tx_data_session_id: 0`, written eight times. | `pub fn tx_ref(shard_id: u8, pos: BPosition) -> TxRef` in `crates/log/src/testing.rs`. | -30 |
| `crates/log/src/testing.rs:195-227`, `:319-360`, `:402-436` | The three fake subscriptions repeat one decode-then-call-back body: `rkyv::from_bytes`, build a `BPosition` from the header, invoke `f`. | Make `FakeTxOrderingSubscription` a thin wrapper over `FakeTypedSubscription<TxOrderingMessage>`, and give `FakeSubscription` a `poll_decoded<T>` method that both others call. | -30 |
| `crates/log/tests/aeron_live_e2e.rs:50-70`, `crates/log/tests/offer_connect_race.rs:53-65`, `crates/log/tests/offer_starvation.rs:86-97` | Bring up the e2e stack: start the cluster, read `aeron_dir_host`, build a `LogConfig`, set the tx_data template and stream base, spawn the runtime. | `pub async fn single_node_runtime(stream_id_base: i32) -> (AeronTestCluster, AeronRuntime, LogConfig)` in `crates/log/src/testing.rs`, behind `docker-e2e`. | -30 |
| `crates/log/tests/aeron_live_e2e.rs:99-107`, `crates/log/tests/offer_connect_race.rs:98-106`, `crates/log/tests/offer_starvation.rs:114-123`, `:154-163` | Receive with a deadline: loop on `tokio::time::timeout(50ms, sub.recv())` until a count, a match, or the deadline. | `pub async fn recv_within<T>(sub: &mut TxDataSubscriberHandle, budget: Duration, want: impl Fn(&TxEnvelope) -> bool) -> Option<TxEnvelope>` in `crates/log/src/testing.rs`. | -25 |
| `crates/log/src/config/tests.rs:205-214`, `:241-249`, `:263-271`, `:278-286`, plus `write_tmp` at nine call sites | The same MDS `[channels]` TOML block, with only the base port and the count changed, and the same `write_tmp` then `from_toml_path` pair. | `fn load(toml: &str) -> Result<LogConfig, LogError>` and `fn mds_toml(base_port: &str, count: u32) -> String` in `crates/log/src/config/tests.rs`. | -25 |
| `crates/log/tests/aeron_live_e2e.rs:28-36,45-48`, `crates/log/tests/offer_connect_race.rs:32-40,48-51`, `crates/log/tests/offer_starvation.rs:51-59,81-84` | `docker_available()` is copied verbatim three times, and each caller wraps it in the same `assert!` with the same message. | `pub async fn require_docker()` in `crates/log/src/testing.rs`, behind `docker-e2e`. | -24 |
| `crates/log/tests/testing_fakes.rs:69-76`, `crates/log/tests/offer_starvation.rs:61-68`, `crates/log/tests/aeron_live_e2e.rs:85-90`, `crates/log/tests/offer_connect_race.rs:77-82`, `crates/log/tests/codec_roundtrip.rs:8-13` | A `TxEnvelope` built from one correlation id and one fill byte. Two files define the same private `env` helper; three inline the literal. | `pub fn tx_envelope(correlation_id: u64, fill: u8) -> TxEnvelope` in `crates/log/src/testing.rs`. | -18 |
| `crates/log/src/testing.rs:579-591`, `:593-605` | `archive_control_endpoint` and `archive_response_endpoint` differ only by the port, 8010 and 8011. | `async fn endpoint(&self, i: usize, port: u16) -> String` in `AeronTestCluster`; both public methods call it. | -13 |
| `crates/log/src/testing.rs:175-178`, `:203-206`, `:290-293`, `:326-329`, `:373-376`, `:409-412` | Struct literals that build a fake handle from `bus.stream(channel, stream_id)`. Three for the publication, three for the subscription. | `FakeConcurrentPublication::new(bus, channel, stream_id)` and `FakeSubscription::new(bus, channel, stream_id)` in `crates/log/src/testing.rs`. | -12 |
| `crates/log/src/testing.rs:105-108`, `:217-220`, `:348-351`, `:425-428` | `BPosition { term_id: header.term_id(), term_offset: header.term_offset() }`, written four times. | `pub fn position(&self) -> BPosition` on `FakeHeader` in `crates/log/src/testing.rs`. | -12 |

## Prior audit items

| item | status (open / done / partly) | note |
| --- | --- | --- |
| `obs::bin` module (`init_tracing`, `wait_for_shutdown`) | done | `crates/obs/src/bin.rs:12,22`. No other `fn init_tracing` or `fn wait_for_shutdown` exists in the service crates. |
| `ObsArgs` flattenable struct and `init_service(...)` wrapper | partly | `init_service!` exists at `crates/obs/src/lib.rs:20`. No `ObsArgs` struct exists; each binary still declares `metrics_addr` and `host_id`. |
| `AeronRuntime::spawn(dir: Option<&Path>)` (13 copies) | done | `crates/log/src/aeron_live/runtime.rs:123`. The only remaining hand-written match is `crates/log/src/refetch.rs:290-293`. |
| `open_tx_receipts` MDS fan-in helper | done | `TxReceiptsSubscriberHandle::open_auto` at `crates/log/src/aeron_live/handles/tx_receipts.rs:259`. Ingress, sequencer, and validator all call it. |
| Recorder-thread plus ready-barrier helper (about 200 lines) | done | `record_stream_until_stopped` at `crates/log/src/recorder.rs:227`. Ingress and the DA watcher both call it. |
| Archive catalog paging triplicated | open | Still three copies: `recorder.rs:561-599`, `replay.rs:472-529`, `refetch.rs:436-462`. See the first table. |
| `with_leaked_handler` RAII guard (4 hand-rolled sites) | open | Four `Handler::leak` then `release()` pairs remain: `recorder.rs:501-511`, `recorder.rs:577-587`, `replay.rs:485-495`, `refetch.rs:441-449`. A guard folds into the paging helper above. |
| `declare_channel_handles!` macro dedups five handles | partly | The macro covers the five simple pairs. `tx_data.rs` and `tx_receipts.rs` still hand-write the same shape. |

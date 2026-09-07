# batcher (batcher, da_watcher, deployer)

## Summary

The tests hold the largest shapes. Six anvil e2e files each build the same
ERC-7955 anvil fixture, and three of them also deploy the same settlement plus
oracle pair. Seven test files copy the same `fn pos`. Two files copy the whole
M+1 synthetic archive writer. In production code the biggest shapes are the
three poll-loop binaries, the two `BACK_PRESSURED` publisher wrappers in the
da-watcher binary, the three `oracle.settlement() -> batches(n)` reads, and the
`ClosedBlock -> BlockFrame` mapping that exists in three places (one is the DA
wire rule, so the copies are a drift risk). The estimated reduction is about
870 lines: about 300 in production code and about 570 in tests. Of the prior
audit items that touch this group, `AeronRuntime::spawn` and
`connect_cluster_ordering` are done. The `obs::bin` module is partly done, the
recorder-thread helper is partly done, and the `env:VAR` key parser is still
open.

## R14 production code

| sites | shape | helper (name, signature, home) | lines saved |
|---|---|---|---|
| `crates/batcher/src/bin/kardamom-batch-claimer.rs:35-46,61-64`; `kardamom-batch-watcher.rs:39-46,76-79`; `kardamom-proof-submitter.rs:42-49,65-68` | Same poll binary: init tracing, parse key, build wallet provider, loop, `interval_secs == 0` exit, sleep. | `run_poll_loop(rpc, key, interval, tick)` plus `wallet_http_provider(rpc, key)`; new `kardamom_batcher::driver` module. Keep the per-bin `Args`: the env var names differ per role. | 30 |
| `crates/da_watcher/src/bin/kardamom-da-watcher.rs:452-469`, `:485-509` | Same offer-error mapping and the same 3-line comment: `BACK_PRESSURED` becomes `Backpressure`, else `Transport`. | `pub fn publish_error(e: &dyn std::fmt::Display) -> PublishError`; `kardamom_da_watcher::publisher`. Both wrapper structs then hold one line each. | 16 |
| `crates/da_watcher/src/rpc_source.rs:157-165`, `:203-211` | Same three `ok_or_else` reads of `block_hash`, `log_index`, `block_number`, with the same error strings. | `fn log_ids(log: &RpcLog) -> Result<(u64, B256, u64), L1SourceError>`; same module. | 12 |
| `crates/da_watcher/src/rpc_source.rs:142-151`, `:186-194` | Same topic0 guard; only the expected signature differs. | `fn expect_topic0(log: &RpcLog, want: B256) -> Result<(), L1SourceError>`; same module. | 10 |
| `crates/da_watcher/src/interop/source.rs:263-280`, `:281-295`, `:296-310` | Three arms that warn, increment `REMOTE_FEED_RESUBSCRIBE_TOTAL` with a cause label, then `drop_session()`. | `fn resubscribe(&mut self, cause: &'static str, why: &str)`; `WsRemoteChainSource`. | 22 |
| `crates/da_watcher/src/watcher.rs:227-250` (4 arms); `crates/da_watcher/src/interop/watcher.rs:234-286` (4 arms) | Every arm writes the same 4-line `counter!(TICK, "origin" => .., "outcome" => ..)` block. | `fn tick(origin: &str, outcome: &'static str)` in `kardamom_da_watcher::metrics`, plus a no-label `fn l1_tick(outcome)`. | 25 |
| `crates/batcher/src/optimistic.rs:71-84`, `:158-168`; `crates/batcher/src/prover_submit.rs:61-74` | Same read: `oracle.settlement()`, build `IKardamomL2Settlement`, `batches(index)`, same error strings. | `async fn settlement_entry<P: Provider>(provider: &P, oracle: &IKardamomProofOracle::Instance, index: u64) -> Result<BatchEntry, BatcherError>`; `kardamom_batcher::optimistic` or a new `oracle` module. Removes drift on the "no batch posted" check. | 18 |
| `crates/batcher/src/optimistic.rs:102-113`, `:246-263`; `crates/batcher/src/prover_submit.rs:99-111`; `crates/batcher/src/l1.rs:108-118` | Same tail: `.send()`, map error, `.get_receipt()`, map error, `if !receipt.status()` fail. | `async fn confirm(pending: PendingTransactionBuilder<Ethereum>, what: &str) -> Result<TransactionReceipt, BatcherError>`; `kardamom_batcher::l1`. | 18 |
| `crates/batcher/src/batcher.rs:173-191`; `crates/batcher/tests/recon_roundtrip.rs:40-59`; `crates/batcher/tests/recon_proptest.rs:44-60` | Same `ClosedBlock -> BlockFrame` field copy, including the tx list. This is the DA payload rule, so three copies can drift. | `impl From<&ClosedBlock> for BlockFrame`; `kardamom_batcher::frame`. `build_payload` then maps with `BlockFrame::from`. | 35 |
| `crates/batcher/src/rereplicate.rs:75-80`, `:155-159`, `:190-196`, `:203-208` | Same "read_dir, keep `*.rec`, take the file name" filter. | `fn rec_segments(dir: &Path) -> Result<impl Iterator<Item = (String, PathBuf)>, BatcherError>`; same module. | 12 |
| `crates/deployer/src/deployer.rs:225-230`, `:261-265`, `:206-208` | Same "derive factory address, check code, else `FactoryNotDeployed`" guard. | `async fn require_factory(&self) -> Result<Address, DeployError>`; `Deployer`. | 8 |
| `crates/deployer/src/spec.rs:75-105` | Two match arms of `build_spec` differ only in `action`, `init_data` and the salt version. The `Op` accessors already give the rest. | Rebuild one `DeploymentSpec` with `op.id()`, `op.l2_chain_id()`, `op.version()`, and two small matches; same module. | 10 |
| `crates/deployer/src/main.rs:236-237`, `:263-264`, `:333-334` | Same pair: build the deployer with a key, then `operator.expect(...)`. | `fn signing_deployer(rpc, owner, key) -> Result<(Deployer<DynProvider>, Address)>`; same file. Drops three `expect` calls. | 5 |
| `crates/deployer/src/deployer.rs:51-67` | Three `From<E> for DeployError` impls with the same `Provider(e.to_string())` body. | `macro_rules! provider_error_from { ($($t:ty),*) }`; same module. | 8 |
| `crates/batcher/build.rs:33-52`; `crates/deployer/build.rs:191-210` (plus the rerun blocks at `batcher/build.rs:12-31` and `deployer/build.rs:43-69`) | `walk_sol_files` and `walk_sol_files_into` are byte-identical, and both scripts emit the same rerun triggers. | New build-dep crate `kardamom-contracts-build` with `pub fn sol_rerun_triggers(workspace_root: &Path)`; both `build.rs` files call it. | 25 |
| `crates/deployer/build.rs:166-186` | Hand-rolled `hex_decode` and `hex_nibble`. | Use `const_hex::decode` as a build dependency; delete both functions. | 21 |
| `crates/deployer/src/main.rs:381-389`; `crates/validator/src/bin/kardamom-validator/args.rs:180-187` | Same `env:VAR` key convention parser. | `fn resolve_env_key(spec: &str) -> Result<String>` in `kardamom_obs::bin` (both crates can depend on obs); the deployer keeps the `0x` strip and the signer parse. | 8 |
| `crates/da_watcher/src/bin/kardamom-da-watcher.rs:280-335`; `crates/ingress/src/bin/kardamom-ingress/recorders.rs:38-117` | Same recorder start: named thread, `record_stream_until_stopped` with a ready callback, then a 60 s ready barrier with the same four outcome arms. | `spawn_recorder(...) -> (JoinHandle<()>, ReadyRx)` and `await_recorders(Vec<ReadyRx>, Duration)`; `kardamom_log::recorder`. | 40 |
| `crates/batcher/src/live.rs:110-117`; `crates/da_watcher/src/interop/cursor.rs:99-127` | Both persist a cursor with write-temp-then-rename. | KEEP, differs in: the interop cursor fsyncs the file and the parent directory; the batcher cursor does not. Make them the same only with a deliberate durability decision. | 0 |
| `crates/deployer/src/embedded.rs:17-46` | Six accessors with one body shape, plus five `is_nonempty` tests at `:52-75` that `ids.rs:180-189` already covers through `ContractId::ALL`. | Delete the three redundant tests that `ContractId::ALL` covers; keep the factory and proxy ones. A macro for the accessors is not worth it. | 15 |

## R14 tests

| sites | shape | helper (name, signature, home) | lines saved |
|---|---|---|---|
| `crates/batcher/tests/anvil_e2e.rs:21-59`; `crates/batcher/tests/section6_conformance.rs:156-193`; `crates/deployer/tests/deploy_e2e.rs:26-55`; `crates/batcher/tests/optimistic_e2e.rs:79-104`; `crates/batcher/tests/proof_submission_e2e.rs:58-83`; `crates/batcher/tests/optimistic_proof_e2e.rs:117-138` | Same anvil fixture: spawn anvil, `anvil_setCode` for ERC-7955, fund and impersonate the owner and the batcher. Two of them are byte-identical. | `pub async fn anvil_with_erc7955(fund: &[Address]) -> Option<(AnvilInstance, impl Provider + Clone)>` in a new `kardamom_deployer::testkit` module behind a `test-support` feature. The batcher tests already depend on `kardamom-deployer`. | 120 |
| `crates/batcher/tests/optimistic_e2e.rs:105-152`; `crates/batcher/tests/proof_submission_e2e.rs:86-134`; `crates/batcher/tests/optimistic_proof_e2e.rs:144-191` | Same deploy sequence: `ensure_factory`, apply the settlement, read `addresses()[0].proxy`, apply the proof oracle, then find the oracle proxy by id. | `pub async fn deploy_settlement_and_oracle(dep: &Deployer<P>, owner, batcher, verifier: Address, args: OracleInitArgs) -> (Address, Address)`; same `kardamom_deployer::testkit`. The verifier address stays a parameter, because the bindings differ. | 60 |
| `crates/batcher/tests/metrics_endpoint.rs:40-57`; `crates/da_watcher/tests/metrics_endpoint.rs:31-48`; `crates/executor/tests/metrics_endpoint.rs`; `crates/ingress/tests/metrics_endpoint.rs`; `crates/sequencer/tests/metrics_endpoint.rs` | `free_port()` and `scrape(url)` are byte-identical in five crates. The test bodies also share the init-touch-scrape-assert shape. | `kardamom_obs::testkit::{free_port, scrape}` behind a `testing` feature; `reqwest` becomes an optional dependency. | 55 |
| `crates/batcher/tests/section6_conformance.rs:51-56`; `batch_accumulator.rs:8-13`; `recon_proptest.rs:14-19`; `recon_roundtrip.rs:13-18`; `archive_reader.rs:13-18`; `multi_archive_reader.rs:31-36`; `docker_e2e.rs:50-55` | `fn pos(o: i32) -> BPosition` is copied seven times. | No new helper: `BPosition::from_index(n)` (`crates/types/src/position.rs:50`) already returns the same value for a non-negative offset. Delete all seven. | 40 |
| `crates/batcher/tests/section6_conformance.rs:60-154`; `crates/batcher/tests/docker_e2e.rs:61-174` | Same M+1 archive writer: N envelopes per sequencer, round-robin `TxRef` records on B, one `BoundaryStart`, three files written. | `pub fn write_m_plus_one_archives(dir: &Path, sequencers: u8, txs_per_seq: usize, block: u64) -> (PathBuf, HashMap<u8, PathBuf>)` in a new `crates/batcher/tests/common/mod.rs`. | 110 |
| `crates/batcher/tests/recon_roundtrip.rs:40-59`; `crates/batcher/tests/recon_proptest.rs:44-60` | Both rebuild the expected `BlockFrame` list by hand. | Use the `impl From<&ClosedBlock> for BlockFrame` proposed above. Counted once, in the production table. | 0 |
| `crates/batcher/benches/pack_throughput.rs:14-39`; `crates/batcher/tests/recon_roundtrip.rs:20-38`; `crates/batcher/tests/recon_proptest.rs:21-42` | Three builders of a `ClosedBlock` with N synthetic txs of a given size. | `pub fn block_with_txs(number: u64, n_txs: usize, raw_len: usize) -> ClosedBlock` in `kardamom_batcher::batch`, behind a `testing` feature (benches and tests can both use it). | 40 |
| `crates/deployer/tests/chainstate_genesis_predeploy.rs:19-56`; `withdrawals_genesis_predeploy.rs:15-48`; `interop_genesis_predeploy.rs:20-55`; `dev_genesis_predeploy.rs:19-39` | Same steps: read a `chains/*.toml`, parse and validate `Genesis`, find the alloc entry, read `deployedBytecode.object`, compare. `interop_genesis_predeploy.rs` already holds the good helpers. | Move `load_genesis(name)`, `artifact_runtime(workspace, contract)` and `assert_predeploy(...)` to `crates/deployer/tests/common/mod.rs`; the other three files then call them. | 45 |
| `crates/batcher/tests/docker_e2e.rs:180-188`; `crates/state/tests/docker_e2e.rs:46-53` | Same cluster start plus the same endpoint assertion. | `AeronTestCluster::single_node_checked() -> Result<Self>` in `kardamom_log::testing`. | 16 |
| `crates/batcher/tests/archive_reader.rs:20-27`; `crates/batcher/tests/multi_archive_reader.rs:38-45`; and `env`/`env_tx` at `batch_accumulator.rs`, `optimistic_e2e.rs:47-55`, `proof_submission_e2e.rs:47-55` | Same synthetic `TxEnvelope` builder, twice byte-identical and twice near-identical. | `pub fn tx_envelope(i: u64) -> TxEnvelope` in `kardamom_types` behind a `testing` feature, or in the new `tests/common/mod.rs` for the batcher-only sites. | 25 |
| `crates/batcher/tests/multi_archive_reader.rs:47-66` used by `archive_reader.rs:29-80` and `section6_conformance.rs:60-154` | `write_segment(dir, name, frames)` already exists in one file. The other two write the buffer and the file by hand. | Move `write_segment` into `crates/batcher/tests/common/mod.rs` and use it in all three. | 20 |
| `crates/da_watcher/src/watcher.rs:311-575` (12 tests) | Every test opens with the same three lines: build the fake publisher, build `MockL1Source`, push a tip. | `fn harness(tip: u64) -> (InMemoryEpochPublisher, MockL1Source)` in the same test module. | 20 |
| `crates/batcher/src/rereplicate.rs:295-462` (8 tests) | Every test creates two temp dirs, writes segments, then mirrors. | `fn mirrored(files: &[(&str, &[u8])]) -> (TempDir, TempDir)` in the same test module. | 15 |
| `crates/batcher/tests/optimistic_e2e.rs:76-78`; `anvil_e2e.rs:63-69`, `:165-171`; `proof_submission_e2e.rs:57-61`; `optimistic_proof_e2e.rs:104-108`; `section6_conformance.rs:262-266`; `deploy_e2e.rs:59-65`, `:66-72`, `:98-104` | Same "skip if anvil is absent" block, in two spellings. | Fold into the `anvil_with_erc7955` helper above, plus a `skip_without_anvil!` macro for the print-and-return arm. | 25 |

## Prior audit items

| item | status | note |
|---|---|---|
| `obs::bin` module: `init_tracing` + `wait_for_shutdown` | partly | Both exist and the da-watcher binary uses them. The three batcher poll bins still call `tracing_subscriber::fmt::init()`; `kardamom-batcher.rs:178` calls `bin_support::init_tracing`. |
| Flattenable `ObsArgs { metrics_addr, host_id }` + `init_service` | partly | `kardamom_obs::init_service!` exists and `kardamom-batcher.rs:180` uses it. The da-watcher binary still calls the 6-argument `kardamom_obs::init` at `:233-241`. Both crates redeclare the two flags (`kardamom-batcher.rs:149-155`, `kardamom-da-watcher.rs:113-118`). |
| `AeronRuntime::spawn(dir)` | done | `crates/batcher/src/live.rs:511` and `crates/da_watcher/src/bin/kardamom-da-watcher.rs:247`. |
| `bin_support::connect_cluster_ordering` (batcher site) | done | `crates/batcher/src/live.rs:524`. |
| Recorder-thread + ready-barrier helper | partly | The thread body is shared (`record_stream_until_stopped`). The spawn plus 60 s barrier is still duplicated: `kardamom-da-watcher.rs:280-335` vs `ingress/.../recorders.rs:38-117`. |
| `env:VAR` key parser (deployer vs validator) | open | Now at `deployer/src/main.rs:381-389` and `validator/.../args.rs:180-187`. |
| `open_tx_receipts` MDS fan-in | n/a | No site in this group. |
| Consensus-critical duplication (validator, exec-core, engine, state, sealer) | n/a | No site in this group. |

# engine

## Summary

The largest shape is the actor test fixture. 18 test sites call `spawn_exec` with
the same 12-argument list, and 18 sites build a `BlockBoundaryStart` send by hand.
One `ExecRig` builder plus three message constructors in `test_support.rs` remove
about 400 lines. The `reader.rs` unit tests repeat a spawn-join-drain block 8
times; one `run_ordering` helper removes about 84 lines. In production code the
biggest shapes are the lazy `ExecScope` init (copied verbatim in `on_tx` and
`on_deposit`), the never-block `try_send` handoff (BAL and shadow), and the
Epoch / RemoteEpoch expansion in the tx_ordering reader. The estimated total is
about 615 lines removed: about 85 in production code and about 530 in tests. Of
the prior-audit items that touch this group, the engine settle sweep is done, the
`obs::bin` and `AeronRuntime::spawn` moves are done, and the `bin_support`
recovery move is partly done: `resume_point` is still duplicated between the
executor and validator binaries.

## R14 production code

| sites | shape | helper (name, signature, home) | lines saved |
|---|---|---|---|
| `crates/engine/src/actor/exec_thread.rs:414-426`, `:517-529` | Verbatim lazy `ExecScope` build: `match self.scope.as_mut()`, else `Executor::new(snapshot_after(block-1), parent, env)` then `seed_layer(&self.delta)` then `insert`. | `fn block_scope(&mut self, env: ExecEnv) -> Result<&mut crate::executor::Executor<S::Db>, ExecutorError>` — `impl ExecState` in the same file | 11 |
| `exec_thread.rs:821-838`, `:851-865` | Never-block handoff: `try_send`, warn on `Full`, ignore `Disconnected`. The shadow copy also bumps a drop counter. | `fn try_handoff<T>(tx: &Sender<T>, item: T, block: u64, what: &str, on_full: impl FnOnce())` — new free fn in `actor/exec_thread.rs` | 12 |
| `crates/engine/src/reader.rs:573-634`, `:635-686` | Record expansion: dedup check, take the item list, send the marker with its own index, then send one message per item at `BPosition::from_index(tx_idx.0)`. | `fn expand_record<S: ExecSink, T>(out: &S, next: &mut TxIndex, position: BPosition, marker: ReaderToExec, items: Vec<T>, mk: impl Fn(TxIndex, T) -> ReaderToExec) -> Result<(), SinkClosed>` — `crate::reader` | 25 |
| `reader.rs:562-571`, `:591-600`, `:607-632`, `:654-663`, `:670-684`, `:712-714` | `if exec_out.send(..).is_err() { return Ok(()) }` repeated six times. | `macro_rules! send_or_stop` in `crate::reader` (most sites disappear with the row above) | 8 |
| `crates/executor/src/bin/kardamom-executor/state.rs:84-88`, `crates/validator/src/bin/kardamom-validator/main.rs:121-125` | Field-by-field build of `ResumePoint` from `kardamom_state::RecoveryPoint`. Two copies of a consensus-critical resume cursor. | `impl From<kardamom_state::RecoveryPoint> for ResumePoint` — `crates/engine/src/actor/types.rs` | 8 |
| `crates/engine/src/replay.rs:224-230`, `:250-256` | Seven verbatim post-execution lines: apply write set, add gas, push receipt, bump four counters. | `fn record_replayed(&mut self, counters: &mut Counters, receipt: Receipt, ws: WriteSet)` on a small `BlockAcc` struct — `crate::replay` | 10 |
| `exec_thread.rs:559-585`, `replay.rs:265-274` | Block-close read closure: parent layer then snapshot, with the same `"block-close read {addr}/{slot}"` error text. Replay has no parent layer, so it passes `None`. Merging removes a drift risk on the read order, which is consensus-critical. | `fn layered_storage_read(parent: Option<&PendingDelta>, snap: &impl StateDatabase) -> impl Fn(Address, U256) -> Result<U256, ExecutorError>` — `crate::replay` or a shared `engine` module | 6 |
| `exec_thread.rs:402-411`, `:504-511`, `:633-645` | Whole-block deferral guard: `if self.block_exec.is_some() { self.buffered.push(...); return Ok(Flow::Continue); }`. | `fn defer(&mut self, rec: BufferedRecord) -> bool` — `impl ExecState` | 6 |
| `crates/engine/src/actor.rs:162-175`, `exec_thread.rs:199-211`, `:897-909`, `:920-933` | The same 12-item argument list is written four times, on the path from `Executor::run` to `ExecState::new`. | Pass `Outbound<W>` and `RoleHooks<W>` down, instead of destructured fields. Note: the comment at `exec_thread.rs:892-895` defends the current shape. Apply only if the group accepts the change. | 25 (optional) |
| `crates/engine/src/actor/ports.rs:66-70`, `:74-82`, `reader.rs:97-105`, `:107-111`, `:381-385` | Five `impl Trait for Box<dyn Trait>` forwarders. | **KEEP, differs in method set.** A `forward_boxed!` macro invocation would need each signature written out again. It would cost more lines than the five impls hold. | 0 |
| `crates/engine/src/persist.rs:34-38`, `:88-92` | `MdbxSnapshotSource::new` and `MdbxWriterSignal::new` both wrap a `SnapshotReceiver`. | **KEEP, differs in role.** The two adapters implement different traits. A shared wrapper would add a type, not remove one. | 0 |

## R14 tests

| sites | shape | helper (name, signature, home) | lines saved |
|---|---|---|---|
| `exec_tests.rs:61,163,233,300,357,442,501,569,691,812,871`; `exec_pipeline_tests.rs:81,155,227`; `exec_resume_tests.rs:71,146,214,270` | The same 12-argument `spawn_exec` call, about 14 lines each. Nine of the twelve arguments are `None` or `None::<NoEpochCheck>` at almost every site. | `struct ExecRig<S, Q, P>` with `fn new(snapshots: S, signal: Q, queue: P) -> Self` (every hook defaults to `None`), builder methods `bal`, `shadow`, `block_exec`, `remote`, `start`, and `fn spawn(self, rx, tx) -> JoinHandle<Result<(), ExecutorError>>` — `crates/engine/src/actor/test_support.rs` | 170 |
| `exec_tests.rs:51,154,224,280,348,432,491,559,680,757`; `exec_pipeline_tests.rs:55,71,145,218`; `exec_resume_tests.rs:62,137,205,261` | Hand-built boundary send: `tx_r2e.send(ReaderToExec::Boundary(BlockBoundaryStart { .. })).unwrap()`, about 9 lines each. | `fn boundary_msg(block_number: u64, end_count: i32, l2_timestamp: u64) -> ReaderToExec` — `test_support.rs` | 118 |
| `exec_tests.rs:35,42,147,217,271,340`; `exec_resume_tests.rs:55,130,198,254`; `exec_pipeline_tests.rs:48,64` | Hand-built tx send: `ReaderToExec::Tx { tx_idx: TxIndex(i), envelope: legacy(..), position: pos(i) }`, about 6 lines each. | `fn tx_msg(signer: &PrivateKeySigner, to: Address, idx: u64, nonce: u64, value: u64) -> ReaderToExec` — `test_support.rs` | 54 |
| `reader.rs:967-974`, `:1036-1050`, `:1098-1112`, `:1154-1168`, `:1220-1234`, `:1268-1277`, `:1329-1336`, `:1439-1448` | Spawn the tx_ordering reader, join it, then drain the receiver into a `Vec` with a `while let Ok(m) = rx.recv()` loop. | `fn run_ordering(queue: Vec<Result<(BPosition, TxOrderingMessage), ExecutorError>>, buf: JoinBuffer, cfg: ReaderConfig) -> Result<Vec<ReaderToExec>, ExecutorError>` — the `tests` module of `crate::reader` | 84 |
| `exec_tests.rs:26-28,207-209,290-297,329-331`; `exec_resume_tests.rs:39-46,115-122,186-193,241-248`; `exec_pipeline_tests.rs:38-40` | `MockStateDatabase::builder().account(addr, U256::from(10u128.pow(18)), nonce, KECCAK_EMPTY).build()`: the funded-signer snapshot. | `fn funded(signer: &PrivateKeySigner, nonce: u64) -> MockStateDatabase` — `test_support.rs` | 35 |
| `test_support.rs:27-52`, `reader.rs:845-865`, `replay.rs:314-334` | Build a signed legacy transfer, then wrap it as a `kardamom_types::TxEnvelope`. Three copies inside this crate. About 30 more copies exist across the workspace (`crates/executor/tests`, `crates/validator/tests`, `crates/exec-core/src/executor/scope.rs:756-775`). | `pub fn signed_legacy(signer: &PrivateKeySigner, to: Address, nonce: u64, value: u64) -> TxEnvelope` — a shared dev-dependency home, for example a `testing` module in `kardamom-types` or a new `kardamom-testkit` crate. Inside engine, `test_support::legacy` becomes the single copy. | 40 |
| `commit_tests.rs:27-30,32-40,86-89,90-98,122-125,166-172,276-279` | Inline `Receipt` and `BPosition { term_id: 0, term_offset: n }` literals. The file already has a `receipt(tag, offset)` helper at `:120-133`, and `test_support::pos` exists. | Move `receipt` above the first test and import `test_support::pos`. No new helper. | 20 |
| `reader/cluster/tests.rs:44-52`, `:113-120` | Drain `sub.next()` into tag strings, with a `match` on the message kind. | `fn drain_tags<E: ClusterEgress>(sub: &mut ClusterTxOrderingSubscription<E>) -> Vec<String>` — the same `tests` module | 8 |
| `exec_tests.rs:29,125,210,264,332,426,476,542,674,751`; and each test's two `bounded::<..>` lines | `Arc::new(Mutex::new(Vec::new()))` plus the two channel constructions, 3 lines per test, 18 tests. | Absorb into `ExecRig::new`, which owns the writer log and both channels. | 40 |
| `persist.rs:134-142`, `replay.rs:336-343` | Open a `SafeNoSync` temp `StateEnv`. | **KEEP, differs in return.** One also spawns the writer thread; the other returns the raw env. The shared part is 5 lines. | 0 |

## Prior audit items

| item | status (open / done / partly) | note |
|---|---|---|
| Settle sweep duplicated in engine (`actor.rs:573-604` vs `:798-844`) | done | The sweep is now one method, `ExecState::settle_ready`, at `crates/engine/src/actor/exec_settle.rs:352-388`. The idle probe (`:421-439`) and the boundary arm (`:397-416`) both call it. |
| `obs::bin`: `init_tracing` + `wait_for_shutdown` | done | `crates/engine/src/bin_support.rs:475` re-exports both from `kardamom_obs::bin`. |
| `AeronRuntime::spawn(dir)` replaces the `match aeron_dir` block | done (engine's part) | `bin_support.rs:375` calls `AeronRuntime::spawn(aeron_dir)`. |
| `bin_support` additions: `restore_checkpoint_if_fresh`, `resume_point`, `connect_cluster_ordering`, `replay_unavailable_fallback` | partly | Three are done: `restore_or_fetch_checkpoint` (`:330-345`), `connect_cluster_ordering` (`:370-378`), `replay_unavailable_fallback` (`:396-466`). `resume_point` is still open: see the `From<RecoveryPoint>` row above. |
| CacheDB layer seeding → `seed_cache_layer` (exec-core) | done | `crates/exec-core/src/executor/db.rs:138`. The engine reaches it through `Executor::seed_layer` at `exec_thread.rs:423`, `:526`, `:669`. |
| Test-harness signer/envelope fixtures duplicated across crates | open | Three copies inside engine; about 30 across the workspace. See the `signed_legacy` row above. |

# engine

## Summary
Two files carry almost all of the debt: `reader.rs` (1032 code lines) and
`actor/exec_thread.rs` (629 code lines). `reader.rs` is only 489 code lines of
source; an inline 543-line test module makes it a large file, so the split is
easy. `exec_thread.rs` holds one 125-line `on_boundary` method and a 12-argument
`spawn_exec` with 4 type parameters; `Executor::run` already shows the fix
pattern with its `EngineWiring` supertrait. The crate carries 24 comments with
historical or legacy context, and several of them are now wrong (`metrics.rs`
names emission sites that moved, `reader.rs:94` names a return type that no
longer exists). All 32 `drop()` calls sit in test code, and every one closes a
channel sender to signal EOF, so they are hard to remove. Sync use is clean: one
`DashMap`, two `AtomicU64`s, and crossbeam channels, all JUSTIFIED. The main
dynamic-dispatch defect is `Box<dyn RemoteEpochObserver>`: its L1 sibling
`EpochObserver` is a generic `W::Epoch`, so the interop hook should be
`W::RemoteEpoch`.

Counts: R1 24, R2 10, R3 3 (+2 KEEP), R4 32 (all test), R5 12, R6 18, R7 3,
R8 9, R9 9, R10 12.

## R1 comments
| file:line | snippet (max 80 chars) | why | fix |
|---|---|---|---|
| crates/engine/src/reader.rs:3 | `The executor used to have one tx_ordering reader thread. It pulled full` | Legacy narrative of a past refactor. | State the topology in present tense. Delete the "used to" history. |
| crates/engine/src/reader.rs:94 | `Boxed trait objects are subscriptions too. So callers holding Box<dyn ...>` | Names "the return type of the `bin_support` open_* helpers". `open_tx_data_subs` returns `Vec<LiveTxDataSub>`. The comment is wrong. | Say the impl lets a caller pick a subscription at run time. |
| crates/engine/src/reader.rs:182 | `used to be terminal: the bounded join fired and the whole process died.` | Historical failure story plus "This bet ... but it could not". | Keep the present rule: a recovery refills the buffer; the timeout stays final. |
| crates/engine/src/reader.rs:352 | `a chain that disagrees (phase 1 of` | References a spec doc by name and a phase number. | State the rule. Delete the phase and the doc path. |
| crates/engine/src/reader.rs:611 | `Sharing the record's position gave every deposit of one epoch the same` | 16-line bug story with a ticket id ("caught by S14's two-message batch"). | Keep one sentence: each expanded item gets its own slot position. |
| crates/engine/src/reader.rs:688 | `Retired by the epoch switch-over. Deposits now travel inside an epoch` | "Retired by", "predates the cutover" carry migration history. | Say: this chain carries deposits inside epochs, so a `DepositRef` is invalid. |
| crates/engine/src/reader.rs:722 | `Spin-wait for (sequencer_id, session_id, tx_data_position) to appear in` | Stale doc line left from `wait_for_envelope`. It names a `timeout` parameter that `join_envelope` does not have. | Delete the first two lines. Keep the "Join a `TxRef`" paragraph. |
| crates/engine/src/actor/exec_thread.rs:37 | `Settling used to happen only at the next boundary. This works under` | Historical design note. | Say why the probe exists: an idle tail must still settle. |
| crates/engine/src/actor/exec_thread.rs:63 | `Each ReaderToExec arm is now a method, instead of inline code.` | "now ... instead of" restates a past edit. | Delete. The code shows this. |
| crates/engine/src/actor/exec_thread.rs:118 | `A single slow fsync no longer touches execution at all. At depth 1 it` | "no longer" plus a description of the old depth-1 behavior. | State the present property: a slow fsync does not stall execution. |
| crates/engine/src/actor/exec_thread.rs:164 | `old position-based key broke under load for this reason.` | Historical bug reference. | Keep the invariant: the key is a count, not a byte position. |
| crates/engine/src/actor/exec_thread.rs:816 | `slow or stuck publisher pump once back-pressured this thread` | "once back-pressured" is a past incident. | Say: `try_send` keeps the BAL handoff off the critical path. |
| crates/engine/src/actor/exec_thread.rs:879 | `// New block opens with empty per-block counters.` | Restates the two zero assignments below. | Delete. |
| crates/engine/src/actor/exec_settle.rs:11 | `Before this split, the sweep existed twice, in the idle-probe arm and` | Describes the pre-split layout. | Delete. Keep "the sweep is one method". |
| crates/engine/src/actor/exec_settle.rs:124 | `boundary's sweep, the same as before this probe existed.` | Compares against a past state. | End the sentence at "defers to the next boundary's sweep". |
| crates/engine/src/actor/wiring.rs:3 | `[Executor::run] used to take 13 positional arguments, three of them` | Historical argument count. | Open with the two present ideas: one wiring type, and grouped inputs. |
| crates/engine/src/actor/types.rs:121 | `The buffered-record and block-output types moved to the no_std exec` | "moved ... with the phase-3 stateless driver", "every pre-move path". | Say: the shapes live in `kardamom-exec-core`; this re-exports them. |
| crates/engine/src/actor/types.rs:136 | `against old state. Under load, this caused the validator to skip` | Bug story: "a proven divergence, found in the first DeFi gate". | Keep the rule: the strategy must read the parent layer. |
| crates/engine/src/actor/commit_thread.rs:109 | `keeps the same semantics as the old one-publish-per-receipt loop.` | Compares against a removed loop. | Say: the resume starts at the first unpublished receipt. |
| crates/engine/src/actor/ports.rs:30 | `The per-receipt ack round trip was the commit thread's biggest cost.` | Past-tense measurement note. | Say: a batch pays one encode and one ack. |
| crates/engine/src/bin_support.rs:11 | `This code used to be copied between the two binaries, and had begun to` | Legacy duplication history. Repeated at line 303. | Say: this module is the single copy for both binaries. |
| crates/engine/src/bin_support.rs:185 | `old resume path opened an archive replay-merge against the local node's` | Describes a removed code path. | Keep the present rule: always use live multicast; refetch covers gaps. |
| crates/engine/src/bin_support.rs:472 | `This moved to kardamom_obs::bin, so every service, not only the` | Move history and "keeps old callers working". | Delete. The `pub use` line is self-explanatory. |
| crates/engine/src/metrics.rs:3 | `Emission sites: - actor.rs: block-apply duration, state-commit duration` | Stale file map. The three metrics now come from `exec_thread.rs:758` and `exec_settle.rs:58,106`. Line 16 also names `reader/cluster.rs`, now `reader/cluster/mod.rs`. | Delete the site list. A grep finds the sites. |
| crates/engine/src/lib.rs:27 | `These re-exports keep old paths working, such as` | "old paths" is migration history. Same at `state.rs:9`. | Say: these paths re-export the pure state-transition slice. |
| crates/engine/src/replay.rs:282 | `carry the origin yet (KAR2 adds it — see the deposit-derivation` | Ticket id plus a spec-doc reference. | Say: reconstruction derives no deposits, so `l1_origin` is 0. |

## R2 long methods
| file:line | fn name | approx code lines | helpers to extract |
|---|---|---|---|
| crates/engine/src/reader.rs:480 | `spawn_tx_ordering_reader` | 183 | `on_tx_ref` (dedup, join, growth warn, send); `expand_epoch`; `expand_remote_epoch`; `warn_on_buffer_growth`. The thread body then reads as a 6-arm dispatch. |
| crates/engine/src/reader.rs:733 | `join_envelope` | 72 | `refetch_once(r, key)` (the warn, the sink, the two log arms); `next_slice(deadline, cfg)`. |
| crates/engine/src/actor/exec_thread.rs:674 | `on_boundary` | 125 | `check_alignment(block_number, end_tx_idx)`; `run_block_exec(block_number)`; `handoff_bal(&boundary, &pending)`; `handoff_shadow(block_number)`. |
| crates/engine/src/actor/exec_thread.rs:386 | `on_tx` | 73 | `scope_or_init(env)` (identical to the block at `on_deposit:517`); `capture_shadow(&envelope, &result, touches)`; `log_bal_progress(&ws)`. |
| crates/engine/src/actor/commit_thread.rs:59 | `spawn_commit` (thread body) | 60 | `collect_batch(&rx) -> (Vec<Receipt>, Option<BlockBoundary>, bool)`; `publish_batch(&mut pub, &receipts)`; `publish_boundary(&mut pub, b)`. |
| crates/engine/src/actor.rs:112 | `Executor::run` | 64 | `spawn_readers(inbound, &buffer, &cfg, start)`; `join_pipeline(ordering, exec, commit)`; `join_tx_data(handles)`. |
| crates/engine/src/replay.rs:174 | `drive_block` | 90 | `apply_remote_epochs(...)`; `apply_txs(...)`; `seal_block(...)`. The two apply loops share a body; one `apply_one` helper serves both. |
| crates/engine/src/shadow.rs:112 | `process_block` | 74 | `to_observations(captures, block_number) -> Vec<TxObs>`; `emit_metrics(&g, accumulator_reads)`; `log_summary(block_number, &g, serial)`. |
| crates/engine/src/reader/cluster/mod.rs:140 | `ingest` | 66 | `ingest_record(index, msg)`; `ingest_boundary(b)`; `check_pending_overflow()`. |
| crates/engine/src/bin_support.rs:396 | `replay_unavailable_fallback` | 71 | `stage_peer_checkpoint(...)`; `log_resync_prepared(block, adopted_unverified)`. |

## R3 large files
| file | code lines | split plan or KEEP (reason) |
|---|---|---|
| crates/engine/src/reader.rs | 1032 (489 source + 543 test) | Four steps. **(1)** Move `#[cfg(test)] mod tests` (lines 832-1466) to `src/reader/tests.rs`, declared as `#[cfg(test)] mod tests;`. This matches `reader/cluster/tests.rs`, the convention this crate already uses. The file then holds 489 code lines and is under the limit. **(2)** `reader/ports.rs`: `TxDataSubscription`, `TxOrderingSubscription` and their boxed forwarding impls (64-111); `JoinRecovery`, `JoinRecoveryFactory` (177-225); `EpochObserver`, `NoEpochCheck`, `RemoteEpochObserver` (347-403); `SinkClosed`, `ExecSink` and its two impls (441-464). **(3)** `reader/join.rs`: `JoinBuffer` (113-175), `ReaderConfig` (227-266), `DedupWindow` (405-439), `join_envelope` and `wait_for_envelope` (722-830). **(4)** `reader/threads.rs`: `ReaderToExec` (268-319), `spawn_tx_data_reader` (321-345), `spawn_tx_ordering_reader` (466-720). `reader/mod.rs` keeps the module doc, `pub mod cluster`, and the re-exports. Every public path stays the same. |
| crates/engine/src/actor/exec_thread.rs | 629 | Split the inherent impl across files, the way `exec_settle.rs` already does. **(1)** `actor/exec_state.rs`: the `ExecState` struct and its field docs, plus `new` (62-245). **(2)** `actor/exec_records.rs`: the payload arms `on_tx`, `on_deposit`, `on_xchain`, with the shared `check_in_order`, `exec_env`, `record_applied`, and a new `scope_or_init` helper (314-546, 625-672). **(3)** `actor/exec_markers.rs`: `on_epoch` and `on_remote_epoch` (472-495, 599-623). Both are observer-only arms that apply no transaction, so they are cohesive. **(4)** `actor/exec_boundary.rs`: `apply_block_close_actions` and `on_boundary` (548-597, 674-889). `exec_thread.rs` keeps `IDLE_SETTLE_PROBE`, `Recv`, `Flow`, `run`, `recv_next`, and `spawn_exec`: the loop and its spawn. Each part lands near 150 code lines. |
| crates/engine/src/bin_support.rs | 345 | KEEP. Under the limit, and already sectioned by banner comments (genesis, tx_data bridges, refetch wiring, checkpoint ladder). A split would put the two binaries' shared wiring in four files with no gain. |
| crates/engine/src/replay.rs | 312 | KEEP. Under the limit. `replay_blocks` and `drive_block` are one offline pipeline with one shared `Counters` state. |
| crates/engine/src/reader/cluster/mod.rs | 193 | KEEP. One subscription type with its cursor, buffers, and connect helper. |

## R4 manual drops
All 32 `drop()` calls sit in test code. No production code in this crate calls
`drop`. Every call closes a channel sender so a reader thread sees EOF and
exits, so all of them are hard to remove.

| file:line | snippet | class | fix |
|---|---|---|---|
| crates/engine/src/persist.rs:193, 211, 254 | `drop(queue); // release the delta sender to let the writer exit` | channel sender close | Hard to remove. `handle.shutdown()` joins the writer, which exits only after every `delta_tx` clone drops. A `with_queue(handle, |q| ...)` helper whose scope ends the borrow removes all three. |
| crates/engine/src/persist.rs:284 | `drop(handle);` | resource release | Hard to remove. The test proves `wait_committed` errors after the writer stops. Wrap the writer in a scope, then assert after it. |
| crates/engine/src/actor/exec_tests.rs:58, 161, 231, 287, 355, 440, 499, 567, 688, 764, 869 | `drop(tx_r2e);` | channel sender close | Hard to remove: it signals EOF so the exec thread stops. A `feed(records) -> Receiver<ReaderToExec>` helper in `test_support.rs` ends the sender's scope and removes all 11. |
| crates/engine/src/actor/exec_tests.rs:76, 372, 459, 521, 589 | `drop(rx_e2c);` | channel sender close | Same helper. The receiver drop lets the exec thread stop early. |
| crates/engine/src/actor/exec_pipeline_tests.rs:78, 153, 269 | `drop(tx_r2e);` | channel sender close | Same `feed` helper. |
| crates/engine/src/actor/exec_resume_tests.rs:69, 144, 212, 268 | `drop(tx_r2e);` | channel sender close | Same `feed` helper. |
| crates/engine/src/actor/commit_tests.rs:49, 100, 175, 236, 283 | `drop(tx);` | channel sender close | Same shape, on the exec-to-commit channel. Add a `feed_commits` helper. |

## R5 sync primitives and channels
| file:line | primitive | verdict | reason | fix |
|---|---|---|---|---|
| crates/engine/src/reader.rs:133 | `Arc<DashMap<(u8,i32,BPosition), TxEnvelope>>` | JUSTIFIED | M tx_data reader threads insert; one tx_ordering reader takes. The map is never iterated, so per-shard locks fit the pattern. | None. |
| crates/engine/src/reader.rs:454 | `impl ExecSink for crossbeam Sender<ReaderToExec>` | JUSTIFIED | Hands records from the reader thread to the exec thread. | None. |
| crates/engine/src/reader.rs:460 | `impl ExecSink for tokio mpsc Sender<ReaderToExec>` | JUSTIFIED | Same handoff to an async consumer, through `blocking_send`. | None. |
| crates/engine/src/reader/cluster/mod.rs:32 | `next_index: Arc<AtomicU64>` | JUSTIFIED | The subscription writes it; the cluster session thread reads it to build `REPLAY_FROM`. | None. |
| crates/engine/src/reader/cluster/mod.rs:33 | `next_block: Arc<AtomicU64>` | JUSTIFIED | Same cross-thread cursor. | None. |
| crates/engine/src/actor.rs:139 | `bounded::<ReaderToExec>(cfg.receipt_queue_depth)` | JUSTIFIED | Reader thread to exec thread, with back-pressure. | None. |
| crates/engine/src/actor.rs:140 | `bounded::<ExecToCommit>(cfg.receipt_queue_depth)` | JUSTIFIED | Exec thread to commit thread. | None. |
| crates/engine/src/actor/exec_thread.rs:71 | `bal_tx: Option<Sender<BalHandoff>>` | JUSTIFIED | Exec thread to the BAL publisher thread, with `try_send` so it never blocks. | None. |
| crates/engine/src/actor/exec_thread.rs:75 | `shadow_tx: Option<Sender<ShadowBlock>>` | JUSTIFIED | Exec thread to the footprint-shadow thread, same `try_send` rule. | None. |
| crates/engine/src/shadow.rs:92 | `bounded::<ShadowBlock>(8)` | JUSTIFIED | Feeds the shadow thread. Depth 8 bounds the memory a stalled grader holds. | None. |
| crates/engine/src/persist.rs:61 | `delta_tx: Sender<WriteBatch>` | JUSTIFIED | Exec thread to the mdbx writer thread. The bound is the intended back-pressure. | None. |
| crates/engine/src/bin_support.rs:163 | `tokio mpsc UnboundedReceiver<(TxDataLoc, TxEnvelope)>` | JUSTIFIED | The Aeron poll task cannot block, so the queue must not push back. Note the risk: an unbounded queue grows without limit if the engine reader stalls. Add a depth gauge, or bound it and count drops. | Keep unbounded. Add a queue-depth metric. |

## R6 dynamic dispatch

### R6 addendum (after review)

The KEEP verdicts below were revisited against the consumers of each site. Every one
can go. Four forwarding impls (`reader.rs:97, 107, 381`, `actor/ports.rs:66`) have no
user, because every `EngineWiring` impl names concrete types. `BlockExec<D>` becomes a
`BlockExecStrategy<D>` trait plus `type BlockExec` on the wiring. `JoinRecoveryFactory`
becomes a trait with `type Recovery: JoinRecovery` and `fn build(self)`, plus
`type JoinRecovery` on the wiring; the sinks on `JoinRecovery` then become `impl FnMut`.
The builders return named structs, not `impl Trait`, because the wiring must name the
type. See the R6 section of `README.md` for the ordered plan.
No `Box<dyn Error>` exists in this crate. Errors already use `thiserror`
(`ExecutorError`) and `anyhow` at the binary seam. The remaining sites are
deliberate runtime-choice seams, with one exception.

| file:line | snippet | class | generic replacement |
|---|---|---|---|
| crates/engine/src/actor/exec_thread.rs:89 | `remote_epoch_observer: Option<Box<dyn RemoteEpochObserver>>` | trait-object dispatch | **The one real defect.** Its L1 sibling is the generic `epoch_observer: Option<E>`. Add `type RemoteEpoch: RemoteEpochObserver + 'static` to `EngineWiring`, make the field `Option<R>`, and add a forwarding `impl RemoteEpochObserver for Box<dyn RemoteEpochObserver>`, matching `reader.rs:381`. A caller that needs a runtime choice then names the boxed type itself. |
| crates/engine/src/actor/exec_thread.rs:211 | `remote_epoch_observer: Option<Box<dyn RemoteEpochObserver>>` | trait-object dispatch | Same change, in `ExecState::new`. |
| crates/engine/src/actor/exec_thread.rs:909 | `remote_epoch_observer: Option<Box<dyn RemoteEpochObserver>>` | trait-object dispatch | Same change, in `spawn_exec`. |
| crates/engine/src/actor/wiring.rs:137 | `pub remote_epoch_observer: Option<Box<dyn crate::reader::RemoteEpochObserver>>` | trait-object dispatch | Becomes `Option<W::RemoteEpoch>`. |
| crates/engine/src/reader.rs:97 | `impl TxDataSubscription for Box<dyn TxDataSubscription>` | trait-object dispatch | See addendum. Previously KEEP: This is the forwarding impl that lets the caller, not the API, choose boxing. |
| crates/engine/src/reader.rs:107 | `impl TxOrderingSubscription for Box<dyn TxOrderingSubscription>` | trait-object dispatch | See addendum. Previously KEEP: Same seam. |
| crates/engine/src/reader.rs:381 | `impl EpochObserver for Box<dyn EpochObserver>` | trait-object dispatch | See addendum. Previously KEEP: Same seam. |
| crates/engine/src/actor/ports.rs:66 | `impl StateWriterQueue for Box<dyn StateWriterQueue>` | trait-object dispatch | See addendum. Previously KEEP: Same seam. |
| crates/engine/src/actor/ports.rs:74 | `impl TxReceiptsPublication for Box<dyn TxReceiptsPublication>` | trait-object dispatch | See addendum. Previously KEEP: Same seam. |
| crates/engine/src/reader.rs:209 | `sink: &mut dyn FnMut(TxDataLoc, TxEnvelope)` | stored `dyn Fn` | See addendum. Previously KEEP: A generic `F: FnMut` would make `JoinRecovery` non-object-safe, and the trait must be boxed because the Aeron client is thread-bound. Off the hot path. |
| crates/engine/src/reader.rs:218 | `sink: &mut dyn FnMut(BPosition, Deposit)` | stored `dyn Fn` | See addendum. Previously KEEP, same reason. |
| crates/engine/src/reader.rs:225 | `pub type JoinRecoveryFactory = Box<dyn FnOnce() -> Option<Box<dyn JoinRecovery>> + Send>` | stored `dyn Fn` | See addendum. Previously KEEP: A closure type has no name, so a wiring impl cannot write it out. It runs once per thread. |
| crates/engine/src/reader.rs:495 | `let mut recovery: Option<Box<dyn JoinRecovery>>` | trait-object dispatch | See addendum. Previously KEEP: It follows from the factory above. |
| crates/engine/src/reader.rs:735 | `recovery: &mut Option<Box<dyn JoinRecovery>>` | trait-object dispatch | See addendum. Previously KEEP, same reason. |
| crates/engine/src/actor/types.rs:139 | `pub type BlockExec<D> = Box<dyn Fn(...) -> Result<BlockExecOutput, ExecutorError> + Send>` | stored `dyn Fn` | See addendum. Previously KEEP: Documented at `wiring.rs:26`: a closure has no nameable type, and this runs once per block. |
| crates/engine/src/bin_support.rs:232 | `sink: &mut dyn FnMut(TxDataLoc, TxEnvelope)` | stored `dyn Fn` | See addendum. Previously KEEP: It implements the trait above. |
| crates/engine/src/bin_support.rs:247 | `sink: &mut dyn FnMut(BPosition, Deposit)` | stored `dyn Fn` | See addendum. Previously KEEP, same. |
| crates/engine/src/bin_support.rs:297 | `}) as Box<dyn JoinRecovery>)` | trait-object dispatch | See addendum. Previously KEEP: It builds the factory the reader thread calls. |

## R7 too many generics
| file:line | item | params/bounds | supertrait proposal |
|---|---|---|---|
| crates/engine/src/actor/exec_thread.rs:897 | `spawn_exec<S, Q, P, E>` | 4 type params, 4 bounds, 12 arguments | Add `ExecPorts`, with associated types `Snapshots: SnapshotSource`, `WriterSignal: StateWriterSignal`, `WriterQueue: StateWriterQueue`, `Epoch: EpochObserver`, and `RemoteEpoch: RemoteEpochObserver` (see R6). Declare `EngineWiring: ExecPorts` so `Executor::run` passes its own `W` straight through. Then group the 12 arguments into three structs that mirror the ones `Executor::run` already destructures: `ExecChannels { rx, tx }`, `ExecPortsValues { snapshots, sw_signal, sw_queue }`, and `ExecHooks { bal_tx, shadow_tx, block_exec, epoch_observer, remote_epoch_observer }`. The signature becomes `spawn_exec<W: ExecPorts>(cfg, channels, ports, start, hooks)`: 5 arguments and 1 type parameter. The comment at line 892 argues a struct only re-wraps what the caller unwrapped, but `Executor::run` does not unwrap `Outbound` or `RoleHooks` for any other reason. Passing them whole removes both the unwrap and the `#[allow(clippy::too_many_arguments)]`. |
| crates/engine/src/actor/exec_thread.rs:64 | `struct ExecState<S: SnapshotSource, Q, P, E>` | 4 type params, 4 bounds at the impl (183-189) | Becomes `ExecState<W: ExecPorts>`, with fields typed as `W::Snapshots`, `W::WriterSignal`, `W::WriterQueue`, `W::Epoch`, `W::RemoteEpoch`. |
| crates/engine/src/actor/exec_settle.rs:31 | `impl<S, Q, P, E> ExecState<S, Q, P, E>` | 4 type params, 4 bounds, repeated verbatim | The same header appears twice, in two files. One `ExecPorts` bound removes the duplication and any risk that the two copies drift. |

## R8 unnecessary pub
| file:line | item | evidence (who uses it) | fix |
|---|---|---|---|
| crates/engine/src/bin_support.rs:49 | `pub fn load_genesis` | Only `resolve_genesis` at `bin_support.rs:64` calls it. A grep over `crates/` finds no other caller. | Make it private. |
| crates/engine/src/shadow.rs:73 | `pub fn write_cells` | Only `exec_thread.rs:466` calls it. No hit outside `crates/engine`. | `pub(crate)`. |
| crates/engine/src/shadow.rs:42 | `pub const FEE_SINK` | Read only at `shadow.rs:104` and `shadow.rs:121`. `kardamom-stm` defines its own `FEE_SINK` at `crates/stm/src/lib.rs:111`. | `pub(crate)`. |
| crates/engine/src/shadow.rs:54-57 | `ShadowTxCapture` fields `envelope`, `gas_used`, `touches`, `write_cells` | Written at `exec_thread.rs:462`, read at `shadow.rs:121-141`. Both are in this crate. The type must stay `pub` because `spawn_from_env` returns `Sender<ShadowBlock>`. | `pub(crate)` on the fields. |
| crates/engine/src/shadow.rs:62-67 | `ShadowBlock` fields `block_number`, `captures`, `serial_records` | Written at `exec_thread.rs:846`, read at `shadow.rs:113-180`. No external reader. | `pub(crate)` on the fields. |
| crates/engine/src/reader/cluster/mod.rs:32-33 | `ReplayCursor` fields `next_index`, `next_block` | Read at `cluster/mod.rs:102-133` and `cluster/mod.rs:282`. `crates/batcher/src/live.rs:527` builds its own cursor with `ReplayCursor::new`; it does not touch these fields. | `pub(crate)`, or keep them private and add accessors. |
| crates/engine/src/reader.rs:168 | `JoinBuffer::len` | Read at `reader.rs:549` and in this file's tests. No hit in `crates/executor`, `crates/batcher`, or `crates/validator`. | `pub(crate)`. Keep `is_empty` beside it for `clippy::len_without_is_empty`. |
| crates/engine/src/reader.rs:172 | `JoinBuffer::is_empty` | No caller anywhere in the workspace. | `pub(crate)`, paired with `len` above. |
| crates/engine/src/metrics.rs:57 | `pub const HEALTH_BEACON_BEATS_TOTAL` | Used only at `exec_thread.rs:588`. No hit outside the crate. It is also missing from `describe()` at line 71, so the metric ships with no help text. | `pub(crate)`, and add a `describe_counter!` line. |

Note on two items that must stay `pub`: `ExecSink` (`reader.rs:450`) and
`SinkClosed` (`reader.rs:443`) are never named outside this crate, but they
appear in the public bound and signature of `spawn_tx_ordering_reader`. Making
them `pub(crate)` triggers the `private_bounds` lint. Leave them.

## R9 defensive validation
| file:line | snippet | why | typed replacement |
|---|---|---|---|
| crates/engine/src/bin_support.rs:75 | `anyhow::bail!("--chain-id {} conflicts with genesis chain_id {}", ...)` | A check-then-error on two raw `u64`s, run after loading, not at the CLI boundary. The magic value `1` at line 72 also encodes "flag not set". | Parse once into `ChainId(NonZeroU64)` at clap parse time, with `--chain-id` as `Option<ChainId>`. Then `resolve_genesis` returns a `ChainId` and cannot conflict. |
| crates/engine/src/bin_support.rs:273 | `let (Some(response_endpoint), Some(replay_endpoint)) = (...) else { warn; return None }` | Four loosely typed inputs (`Option<&str>` twice, plus two endpoint vectors) get checked here, deep inside the wiring. A half-configured node disables refetch with only a log line. | Build a `RefetchEndpoints { response, replay }` newtype in the args module, with a fallible constructor. `archive_join_recovery` then takes `Option<RefetchEndpoints>` and needs no check. |
| crates/engine/src/reader.rs:229-253 | `pub dedup_window: usize` and `pub buffer_warn_threshold: usize` | Raw `usize` fields with no constructor and no check. `DedupWindow::new(0)` evicts every id at once (`reader.rs:432`), so dedup turns off silently. Duplicate transactions would then execute twice. | Type both as `NonZeroUsize`, or give `ReaderConfig` a fallible `new`. `DedupWindow::new` then needs no guard. |
| crates/engine/src/reader.rs:739-743, 809-815 | `let (shard, session, pos) = (...)` then six positional arguments | `sequencer_id: u8`, `session_id: i32`, and `tx_data_position: BPosition` travel as loose scalars through `join_envelope`, `wait_for_envelope`, `JoinBuffer::insert`, and `JoinBuffer::take`. Two integer arguments sit next to each other and can be swapped with no compile error. | One `TxDataKey { shard: ShardId, session: SessionId, position: BPosition }` newtype, built once from the `TxRef`. Every signature then takes one argument, and the swap cannot compile. |
| crates/engine/src/reader.rs:700 | `ExecutorError::State(format!("legacy DepositRef {:?}: ...", ...))` | A protocol condition becomes a formatted string, so no caller can match on it. | Add `ExecutorError::LegacyDepositRef { source_hash }`. |
| crates/engine/src/reader/cluster/mod.rs:225 | `ExecutorError::State("cluster catch-up buffer overflow ...".into())` | Same stringly-typed error for a distinct, testable condition. | Add `ExecutorError::ReplayBufferOverflow { pending }`. |
| crates/engine/src/reader/cluster/mod.rs:104-107 | `if let Some(b) = self.pending_boundaries.get(&nb) ... let b = self.pending_boundaries.remove(&nb).unwrap();` | Check-then-unwrap: two map lookups, and the `unwrap` relies on the guard above staying correct. | Use `if let Some(b) = self.pending_boundaries.remove(&nb) { if b.end_tx_idx.as_index() <= ni { ... } else { self.pending_boundaries.insert(nb, b); } }`, or a `BTreeMap` `Entry`. |
| crates/engine/src/actor/exec_settle.rs:52-57 | `while self.inflight.front().is_some_and(...) { let (b, _) = self.inflight.pop_front().expect("front checked"); }` | The same check-then-unwrap shape. The `expect` message states the invariant, which shows the type does not carry it. | Restructure as `while let Some((b, _)) = self.inflight.pop_front_if(|(b, _)| b.block_number <= durable)`, or pop first and push back on a miss. |
| crates/engine/src/actor/exec_thread.rs:396 and src/reader/cluster/mod.rs:184 vs src/actor/exec_thread.rs:698 | `verify_record_identity`; `BoundaryMisaligned` raised in two layers | Two cases of one check at several layers. Identity is re-derived in the engine, in the validator, and in the zk guest. `BoundaryMisaligned` is raised by the cluster subscription against its delivery cursor, then again by the exec thread against its applied count. | The identity check is documented as deliberate defense in depth (`types.rs:84`); keep it, but note the price is one ecrecover per transaction. The two `BoundaryMisaligned` sites test different quantities, so keep both, but give them distinct variants so an operator can tell which layer fired. |

## R10 imperative style
| file:line | snippet | functional form |
|---|---|---|
| crates/engine/src/shadow.rs:74 | `let mut cells = Vec::with_capacity(...); for (addr, _) in ws.accounts.iter() { cells.push(...) }` | `ws.accounts.keys().map(\|a\| Cell::Account(*a)).chain(ws.storage.keys().map(\|(a, k)\| Cell::Slot(*a, *k))).collect()`. |
| crates/engine/src/shadow.rs:103 | `let mut exclude = HashSet::new(); exclude.insert(Cell::Account(FEE_SINK));` | `let exclude = HashSet::from([Cell::Account(FEE_SINK)]);` |
| crates/engine/src/shadow.rs:196 | `for o in &obs { stats.learn_obs(o); }` | `obs.iter().for_each(\|o\| stats.learn_obs(o));` |
| crates/engine/src/bin_support.rs:93 | `let mut accounts = Vec::new(); let mut code = Vec::new(); for entry in &g.alloc { ... }` | Two chains: `g.alloc.iter().map(to_account_change).collect()` and `g.alloc.iter().filter_map(to_code_entry).collect()`. Move the `tracing::info!` into a `inspect` step, or into `to_account_change`. |
| crates/engine/src/reader.rs:509 | `loop { let (position, msg) = match tx_ordering_sub.next() { ... }; match msg { ... } }` | A hand-rolled iterator over a `next()` trait method. Give `TxOrderingSubscription` a `fn records(self) -> impl Iterator<Item = Result<...>>` adaptor, then write `for record in sub.records() { ... }`. The same shape repeats at `reader.rs:336` for the tx_data reader. |
| crates/engine/src/reader.rs:604 and src/reader.rs:667 | `for deposit in deposits { ... if exec_out.send(...).is_err() { return Ok(()); } }` | Keep the imperative form: early return with a side effect, which the brief exempts. But the two loops are near-identical. Extract `send_expanded(exec_out, &mut next_tx_idx, items, make_msg) -> Result<Flow, _>` and call it from both arms. |
| crates/engine/src/reader.rs:774 | `let mut recovered = 0u64; ... &mut \|loc, env\| { buffer.insert(...); recovered += 1; }` | Keep. The sink is a `&mut dyn FnMut` by trait contract, so the counter must be a captured mutable. |
| crates/engine/src/actor/commit_thread.rs:76-94 | `let mut receipts = Vec::new(); let mut boundary = None; let mut closed = false; while ... { match rx.try_recv() { ... } }` | Three mutable accumulators plus a flag. Extract `collect_batch(&rx) -> Batch { receipts, boundary, closed }` and return one value. The loop itself must stay imperative: it must stop at a boundary and at the batch cap. |
| crates/engine/src/actor/commit_thread.rs:116 | `let mut from = 0usize; while from < receipts.len() { ... from += published; }` | Keep. The index carries the must-deliver resume point across retries, so a slice iterator cannot express it. Add a comment that says exactly that. |
| crates/engine/src/actor/exec_thread.rs:747 | `for r in out.receipts { self.block_receipts.push(r.clone()); if self.tx.send(...).is_err() { return Ok(Flow::Stop); } }` | Keep: early return with a side effect. |
| crates/engine/src/replay.rs:194-257 | `let mut cumulative_gas = 0u64; let mut tx_index_in_block = 0u64; for record in ... { ... }` | The two loops (remote-epoch messages, then transactions) carry the same four mutable counters and have the same body shape. Fold both into one `try_fold` over a chained iterator of records, with a small `BlockAcc` state struct. This also removes the duplication R2 flags. |
| crates/engine/src/actor/exec_settle.rs:73 | `self.parent = self.inflight.iter().fold(None, ...)` | Already functional. Good example. |

## Tests

### R1 comments
| file:line | snippet (max 80 chars) | why | fix |
|---|---|---|---|
| crates/engine/src/actor/exec_resume_tests.rs:1 | `//! Phase 2 recovery: skip-count replay.` | A phase label. The body then says "the exec thread receives the canonical stream replayed from record 0", which contradicts `types.rs:17`: the cluster now delivers from the cursor, and the exec thread seeds its counters. | Retitle as "Resume from a mid-chain cursor", and rewrite the body to match the current `ResumePoint` contract. |
| crates/engine/src/actor/exec_tests.rs:381 | `keeps the fee sink out (legacy sets gas_price = 0).` | Fine. Present tense, explains a non-obvious fixture rule. | Keep. |
| crates/engine/src/actor/exec_tests.rs:537 | `boundary's timestamp used to execute its transactions. So a feature` | Reads as history, but it states the present rule (block N uses boundary N-1's stamp). | Reword to "the timestamp that executes its transactions". |
| crates/engine/src/actor/exec_tests.rs:737 | `the slice-2 gap where this arm used to fail-stop the engine. The strategy` | "slice-2" is a work-plan label, and "used to fail-stop" is history. | State the present behavior the test guards. |
| crates/engine/src/reader.rs:1367 | `envelope: no overwrite, no cross-wire. Before this fix, the key was` | Describes the pre-fix key. Repeated at line 1389. | Keep the invariant sentence. Delete "Before this fix". |
| crates/engine/src/actor/test_support.rs:106 | `Records every submitted block, and applies it to a shared MockStateDatabase.` | Good: it explains a real trap in present tense (a static snapshot loses settled state). | Keep. |

### R3 large files
| file | code lines | split plan or KEEP (reason) |
|---|---|---|
| crates/engine/src/actor/exec_tests.rs | 750 | Split into a directory, by the themes its own banner comments already mark. `actor/exec_tests/streaming.rs`: lines 20-196 (two-tx block, slim boundary, deposit visibility). `actor/exec_tests/bal_shadow.rs`: 197-402 (BAL handoff, shadow captures). `actor/exec_tests/block_close.rs`: 403-600 (feature-flag block-close actions, with the `beacon_in` helper). `actor/exec_tests/interop.rs`: 601-893 (remote epochs, the `RecordingRemoteObserver`, and `send_remote_epoch`). Move the shared `remote_epoch_fixture` and `send_remote_epoch` helpers into `test_support.rs`, next to `legacy`, `pos`, and `drain_commits`. |
| crates/engine/src/actor/exec_pipeline_tests.rs | 210 | KEEP. Under the limit and single-theme. |
| crates/engine/src/actor/commit_tests.rs | 238 | KEEP. Under the limit and single-theme. |

### R6 dynamic dispatch
| file:line | snippet | class | generic replacement |
|---|---|---|---|
| crates/engine/src/actor/exec_tests.rs:703 | `Some(Box::new(RecordingRemoteObserver(observed.clone())))` | trait-object dispatch | Once `EngineWiring` gains a `RemoteEpoch` associated type (see R6 above), this becomes `Some(RecordingRemoteObserver(...))` with no box. This test is the only caller that supplies a remote observer, so the boxing exists for one test. |

### R10 imperative style
| file:line | snippet | functional form |
|---|---|---|
| crates/engine/src/reader.rs:971, 1047, 1109, 1165, 1231, 1273, 1333, 1445 | `let mut out = Vec::new(); while let Ok(m) = rx.recv() { out.push(m); }` | `let out: Vec<_> = rx.iter().collect();` The same eight lines repeat eight times in one test module. One `fn drain(rx: Receiver<ReaderToExec>) -> Vec<ReaderToExec>` helper replaces all of them, matching `test_support::drain_commits`. |
| crates/engine/src/actor/exec_tests.rs:707, 772 | `let mut receipts = Vec::new(); while let Ok(m) = rx_e2c.try_recv() { if let ExecToCommit::Receipt(r) = m { receipts.push(r) } }` | `rx_e2c.try_iter().filter_map(\|m\| match m { ExecToCommit::Receipt(r) => Some(r), _ => None }).collect()`. |
| crates/engine/src/actor/exec_tests.rs:181 | `let mut saw_transfer_success = false;` | `receipts.iter().any(\|r\| ...)`. |
| crates/engine/src/actor/exec_pipeline_tests.rs:100, 249 | `let mut kinds = Vec::new();` / `let mut boundaries = Vec::new();` | Same `try_iter().filter_map(...).collect()` chain. |
| crates/engine/src/reader/cluster/tests.rs:44, 113 | `let mut got = Vec::new();` | `std::iter::from_fn(\|\| sub.next().ok()).map(label).collect()`. |
| crates/engine/src/actor/test_support.rs:95 | `let mut receipts = Vec::new(); let mut boundaries = Vec::new(); while let Ok(m) = rx.recv() { ... }` | `rx.iter().partition_map(...)` needs itertools. Without it, keep the loop: it splits one stream into two vectors, and this is the shared helper the tests above should call. |

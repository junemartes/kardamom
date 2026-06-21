# Executor libMDBX state persistence — spec

## Goal

The `kardamom-executor` currently keeps all L2 chain state (accounts, storage,
code, receipts, tx-hash index, block cursors) in the in-memory
`MockStateDatabase`; a process exit loses everything. Make the libMDBX-backed
`kardamom-state` crate the **production default** state backend so chain state is
durably persisted, wired into the executor's existing
`SnapshotSource` / `StateWriterQueue` / `StateWriterSignal` seams. **Phase 1**
(durable writes + mdbx default) is done; **Phase 2** (crash-recovery resume) is
specced in the "Phase 2" section below.

## Non-Goals

- **Phase 2 crash-recovery resume** (replay `tx_ordering` from the archive +
  idempotent skip-count). Phase 1 ships a guard that *refuses to start* against a
  non-empty DB (`last_committed_block > 0`) so we never double-apply blocks. Phase 2
  removes the guard. Rationale: correct resume needs a new log-layer archive-replay
  subscription (the subscriber API has no positional open, and
  `last_fsynced_b_position` is a *logical record count*, not an Aeron byte position).
- **Wiring RPC/ingress reads to the persisted DB.** Scope is executor write-durability
  only; the read API (`StateSnapshot: StateDatabase`) exists but is not consumed by
  the node/ingress here.
- **No state-root / MPT commitment** (unchanged from v0; `storage_root = B256::ZERO`).

## Design (simplest viable)

The executor already abstracts its state backend behind three traits consumed by
`Executor::run` (`crates/executor/src/actor.rs`). `kardamom-state` already provides
the full libMDBX implementation (writer thread + snapshot-swap channel). Phase 1 is
therefore three thin adapter structs plus binary wiring — no new storage logic.

**Adapters** (`crates/executor/src/persist.rs`), each wrapping a clone of the
`WriterHandle`'s public fields (`delta_tx: Sender<WriteBatch>`,
`snapshot_rx: SnapshotReceiver`):

| Adapter | Trait | Behavior |
|---|---|---|
| `MdbxSnapshotSource` | `SnapshotSource` (`Db = StateSnapshot`) | `snapshot_after(_)` returns `snapshot_rx.current()` — the latest published MVCC snapshot. The trait returns an owned `Db` (no `Result`); called only after `wait_committed` returns (and the writer publishes an initial snapshot at spawn), so a snapshot is always present and ≥ the requested block — a missing one is an unreachable-in-practice panic. |
| `MdbxWriterQueue` | `StateWriterQueue` | `submit(b, d)` sends `WriteBatch::new(b, d)` on `delta_tx` (bounded `HORIZON_BLOCKS`=4 → correct backpressure). |
| `MdbxWriterSignal` | `StateWriterSignal` | `wait_committed(n)` peeks `current()`, else blocks on `recv()`, until a published snapshot has `block_number() >= n`. `None` (writer dropped) → `ExecutorError::State`. |

`MdbxSnapshotSource` + `MdbxWriterSignal` each hold a *clone* of the writer's
`SnapshotReceiver`, so they read the same `Arc`-shared latest-snapshot pointer and pull
from the same bounded(1) notify channel. That is safe because only the single exec
thread drives both, strictly sequentially (`wait_committed` then `snapshot_after`), so
the `current()` peek and the blocking `recv()` never run concurrently or steal each
other's wake-ups.

**Genesis seeding** (`crates/state` — new `genesis.rs`): `seed_genesis(env, accounts,
code) -> Result<bool>` writes the alloc set into `accounts`/`code` in one RW txn, gated
on a new `meta` flag `KEY_GENESIS_APPLIED` (idempotent across restarts). Returns
`true` if it seeded. Must run **before** `StateWriter::spawn` so the writer's initial
published snapshot reflects genesis. `last_committed_block` is left at 0 (genesis is
"block 0").

**Binary wiring** (`crates/executor/src/bin/kardamom-executor.rs`): new `--state-dir`
(default `/opt/kardamom/state`, matching the Nomad mount) and `--state-durability
durable|safe-no-sync` (default `durable`); open `StateEnv`; `read_recovery_point`;
**Phase 1 guard** (`bail!` if `last_committed_block > 0`); build alloc set from
`Genesis` and `seed_genesis`; `StateWriter::spawn`; build the three adapters from the
handle; `initial_block = recovery.last_committed_block`; run; `writer_handle.shutdown()`
on teardown.

## Interfaces

```rust
// kardamom-state (new, genesis.rs)
pub fn seed_genesis(env: &StateEnv, accounts: &[AccountChange], code: &[CodeEntry])
    -> Result<bool, StateError>;          // Ok(true)=seeded, Ok(false)=already applied
pub fn genesis_applied(env: &StateEnv) -> Result<bool, StateError>;
// meta.rs
pub const KEY_GENESIS_APPLIED: &[u8] = b"genesis_applied";

// kardamom-executor (new, persist.rs)
pub struct MdbxSnapshotSource { /* SnapshotReceiver */ }   // impl SnapshotSource
pub struct MdbxWriterQueue   { /* Sender<WriteBatch> */ }  // impl StateWriterQueue
pub struct MdbxWriterSignal  { /* SnapshotReceiver */ }    // impl StateWriterSignal
```

Reused as-is: `StateEnvBuilder`/`StateEnv`/`Durability`, `StateWriter::spawn`,
`WriteBatch`, `WriterHandle{delta_tx,snapshot_rx}`, `StateSnapshot::block_number`,
`SnapshotReceiver::{current,recv}`, `read_recovery_point`/`RecoveryPoint`.

## Ethereum spec references

No EVM-semantics change. The persisted account model is post-Merge EOA/contract
state `(nonce, balance, code_hash)` + storage slots; receipts carry
`status`/`gas_used`/`logs` per the typed-receipt format. No legacy hardfork paths.

## Testing strategy

Deterministic, serial, no sleeps. mdbx tests use a `tempfile::TempDir` env opened
`Durability::SafeNoSync`; the writer thread is driven by a bounded channel and the
snapshot-swap channel — we synchronize on `wait_committed`, never on wall-clock time.

**Unit — `kardamom-state` genesis (`genesis.rs` `#[cfg(test)]`)**
- `seed_genesis_writes_accounts_and_code` — after seeding, an opened `StateSnapshot`
  returns the seeded `basic()` + `code_by_hash()`.
- `seed_genesis_is_idempotent` — second `seed_genesis` returns `Ok(false)`; state
  unchanged; `genesis_applied()` is `true` after the first call, `false` before.
- `seed_genesis_empty_alloc_sets_flag` — empty alloc still sets the flag (returns
  `true` once, `false` after).

**Unit — executor adapters (`persist.rs` `#[cfg(test)]`, real temp `StateEnv` + `StateWriter`)**
- `submit_then_wait_then_snapshot_roundtrips` — `MdbxWriterQueue::submit` a block-1
  delta with one `AccountChange`; `MdbxWriterSignal::wait_committed(1)` returns 1;
  `MdbxSnapshotSource::snapshot_after(1).basic(addr)` reflects the change.
- `wait_committed_returns_ge_requested` — committing block 2 lets `wait_committed(1)`
  and `wait_committed(2)` both return ≥ the request without blocking forever.
- `wait_committed_errors_when_writer_dropped` — drop the `WriterHandle`; a pending
  `wait_committed` for an un-committed block returns `ExecutorError::State`.
- `snapshot_source_returns_initial_snapshot_before_any_commit` — the writer publishes an
  initial snapshot at spawn, so `snapshot_after(0)` (no commits yet) yields a usable
  block-0 view.
- `state_persists_across_writer_restart` — the core "chain data is now persisted" proof:
  commit blocks 1..=N via the adapters, `shutdown()` (dropping the queue's `delta_tx`
  clone first so the writer can exit), reopen the *same* env path, and assert
  `read_recovery_point().last_committed_block == N` plus the committed account state is
  still present. A `persist.rs` unit test (`StateEnv` + `StateWriter`, no Aeron).

**Regression / unchanged**
- Existing `actor.rs` unit tests and `tests/replay_integration.rs` keep using
  `MockStateDatabase` (retained as a test fixture) and must stay green.
- e2e `multiprocess` executor subprocess now passes `--state-dir <tempdir>
  --state-durability safe-no-sync`; receipts/boundaries output unchanged from baseline.

Deferred (Phase 2): kill-mid-chain + restart recovery test; archive replay-merge tests.

## Alternatives considered

- **Make mdbx opt-in behind `--state-dir`, mock as default.** Rejected per scoping
  decision (mdbx is the production default). Lower blast radius but leaves prod on the
  in-memory mock.
- **Seed genesis by submitting a "block 0" `WriteBatch` through the writer channel.**
  Reuses `apply`, but needs separate sync to know the commit landed before building
  the snapshot source, and conflates genesis with the block cursor. A dedicated
  one-txn `seed_genesis` (gated by a meta flag) is simpler and atomically idempotent.
- **Persist an exact Aeron archive byte-position per commit to enable a direct seek.**
  Rejected for v0: byte positions are fragile across the multi-publisher merge (the
  very reason `BPosition` is a logical count). Deferred as a Phase 2 optimization.

---

# Phase 2 — Crash-recovery resume

## Goal

Remove the Phase 1 "refuse a non-empty DB" guard and let a restarted executor
**resume the chain**: re-read the canonical input it already consumed, skip what it
already committed, re-execute any transactions sealed while it was down, and continue
live — without double-applying or losing state.

## Key constraints (drive the design)

- **Recovery = replay-merge + idempotent skip-count, not a byte seek.**
  `last_fsynced_b_position` is a *logical cumulative record count* (`BPosition::from_index`),
  not an Aeron position. So the executor replays `tx_ordering` from the earliest archived
  record and **skips** records it already applied, then resumes.
- **Replay-merge is UDP-MDC-only.** `rusteron_archive::AeronArchiveReplayMerge` (replay a
  recording from a position, then transition seamlessly to the live stream in one
  multi-destination subscription) requires the live publication to be a **dynamic-MDC UDP**
  channel and the subscription to be `control-mode=manual`. Aeron does **not** replay
  history to a late IPC subscriber. Therefore executor crash-recovery only works on the
  **UDP-MDC + archive** transport (the Nomad cluster), not the single-host IPC default. The
  full persistence smoke test consequently lives in the `cluster-e2e` job.
- **Only `tx_ordering` is recorded today.** `tx_data` / `tx_deposits` are not archived
  (`tx_deposits` is "RAM only"); full backlog catch-up (re-executing txs sealed during
  downtime) requires adding `tx_data`/`tx_deposits` archive recording first (M2.1).

## Design — milestones

**M1 — single-executor resume** (`tx_ordering` replay-merge + skip-count).
- `crates/log/src/replay.rs`: `ReplayMergeSubscriber` wraps `AeronArchiveReplayMerge`
  (thread-confined archive client, reusing `recorder::connect_archive` +
  `list_recordings_for_uri`), delivering `(BPosition, TxOrderingMessage)` in the same shape
  as `aeron_live::open_subscription`. `TxOrderingReplaySubscriberHandle::open`.
- Executor exec thread (`actor.rs spawn_exec`) gains a `resume: Option<ResumePoint>` where
  `ResumePoint { block: B, record_count: N }` (from `RecoveryPoint`): open
  `snapshot_after(B)`, replay from record 0, and **skip** (advance bookkeeping; do **not**
  execute / emit receipts / submit deltas) every `Tx`/`Deposit` with cumulative index `< N`
  and every `BoundaryStart` with `block_number <= B`. The boundary-alignment check
  (`want == have`) still runs during replay. Past the cursor, resume normal execute+commit.
- Binary: when `last_committed_block > 0` **and MDC is enabled**, open the replay-merge
  tx_ordering subscriber and pass `ResumePoint`; remove the Phase 1 guard. Over IPC
  (no archive replay possible) recovery stays refused with a clear message.

**M2 — full backlog catch-up** (`tx_data` + `tx_deposits` replay).
- `RecorderKind::{TxData{sequencer_id},TxDeposits}` + `Recorder::start_stream` (done).
  The recorders co-locate with the **publishers**: `tx_data` is published by the
  **ingress** (M per-shard publishers) and `tx_deposits` by the **da-watcher** — so the
  tx_data recorders live in the ingress process and the tx_deposits recorder in the
  da-watcher (NOT the sequencer, which only *consumes* tx_data), behind `--archive-durability`
  (M2.1, done).
- Executor opens replay-merge for all tx_data shards + tx_deposits on recovery
  (`replay::open_tx_data_replay` / `open_tx_deposits_replay`; tx_data/tx_deposits are
  multicast, so the live destination is the publication's own multicast channel — unlike
  tx_ordering's MDC `endpoint|control` form). The existing join buffer joins replayed refs
  against replayed envelopes unchanged; the reader **join timeout is relaxed to 30 s while
  resuming** (the streams replay independently and catch up at different rates) (M2.2, done).
- `cluster-e2e` persistence validation: the cluster job's **chaos suite**
  (`deploy/cluster/scripts/chaos.sh`, cases `graceful-executor` / `hard-executor`) kills an
  executor under steady `--assert-all-delivered` load and asserts it auto-restarts AND every
  accepted tx still receipts. On the same-node restart the executor reopens its persistent
  `/opt/kardamom/state` and runs this Phase-2 recovery — so a broken recovery surfaces as a
  missed receipt or a crash-loop that never restores the executor count. The nomad jobs pass
  the executor's `--replay/--live-destination-endpoint` (`${meta.node_ip}:40130/40131`) and
  ingress/da-watcher `--archive-durability` so those chaos cases actually exercise recovery
  (M2.3, done). (Rebased onto main #56, which introduced the chaos suite; an earlier
  hand-rolled block-height assertion was dropped in favor of it.)

## Testing strategy (Phase 2)

Deterministic where possible; Aeron-touching tests gated on the archive driver.

**Unit — skip-count recovery (`actor.rs` `#[cfg(test)]`, no Aeron, deterministic)**
- `recovery_skips_already_applied_txs` — feed a replay prefix (records `0..N`, blocks
  `1..=B`) then new records; assert **zero** receipts/deltas emitted for the prefix, and
  that post-cursor txs execute and commit. The core correctness proof.
- `recovery_skips_empty_block_backlog` — `record_count = 0`, blocks `1..=B` all empty
  (boundaries only); assert all are skipped, alignment holds, block `B+1` commits.
- `recovery_boundary_alignment_still_checked` — a misaligned replayed boundary still
  returns `BoundaryMisaligned` (replay doesn't bypass the invariant).
- `no_resume_behaves_as_genesis` — `resume = None` is byte-identical to today's behavior.

**Integration — replay-merge subscriber (`crates/log`, gated on `KARDAMOM_AERON_DIR` +
`full-pipeline-e2e`/`docker-e2e`)**
- `replay_merge_delivers_recorded_then_live` — record a `tx_ordering` stream, open the
  replay-merge subscriber, assert all historical + live `TxOrderingMessage`s arrive in
  order. Runnable locally via the native ArchivingMediaDriver.

**End-to-end — cluster persistence smoke (`cluster-e2e`, CI-validated)**
- Restart an executor alloc mid-load; assert the restarted replica resumes (metrics block
  height catches up; receipts/write-set hashes match peers). Not runnable on darwin.

## Alternatives considered (Phase 2)

- **Plain archive replay (no merge), then switch to a live subscription.** Simpler, but has
  a gap window between "replay reached tail" and "live subscription attached" where records
  are lost. Replay-merge exists precisely to close that gap atomically. Rejected.
- **Persist an exact archive byte-position per commit and seek directly.** Avoids replaying
  from the earliest record, but byte positions are fragile across the multi-publisher merge
  (the reason `BPosition` is a logical count). Deferred as an optimization.
- **Make recovery work over IPC.** Not possible: Aeron never replays pre-subscription
  history to an IPC subscriber, and replay-merge requires UDP MDC. Recovery is therefore
  scoped to the MDC+archive transport.

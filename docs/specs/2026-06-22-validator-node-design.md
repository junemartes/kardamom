# Validator Nodes — Design

**Date:** 2026-06-22
**Status:** Approved (brainstorm) → planning
**Scope (milestone 1):** A validator node that follows the sequencer, re-executes
every block independently, produces a full Ethereum MPT state root, and
cross-checks itself against the sequencer's published receipts and BALs.

---

## 1. Motivation

Today Kardamom is a consensus-less, single-sequencer rollup. The executor
re-executes the canonical transaction order and persists state to libMDBX, but it
**does not compute a state root** (deliberate v0 choice) and produces no
independently-verifiable commitment. There is no node whose job is to *verify*
the sequencer.

A **validator node** follows the sequencer by subscribing to the live channels,
re-executes each block through the same execution core, and:

1. produces a **full Ethereum MPT world-state root** from its own execution, and
2. **cross-checks** its results against the sequencer's published receipts
   (`tx_receipts`) and per-block write-sets (the new **BAL**).

Validators are **monolithic** (single process, no HA) and **off the hot path** —
they never gate sequencing or block production, so they may favour simplicity and
correctness over latency.

## 2. Goals / Non-goals

**Goals (milestone 1)**
- Extract a role-agnostic execution core (`kardamom-engine`) shared by the
  sequencer-side executor and the validator.
- Executor **publishes a BAL** (per-block write-set) on a new channel.
- A `kardamom-validator` crate + binary that re-executes from genesis, keeps its
  own libMDBX state, and computes a **full Ethereum MPT state root** via an
  incremental, persisted trie.
- Dual divergence detection (per-tx vs. `tx_receipts`, per-block vs. BAL),
  **fail-stop** on mismatch.
- Crash-recoverable, from-genesis re-execution (reuses the engine recovery path).
- **Cluster smoke tests** proving a validator syncs the network and keeps up.

**Non-goals (later milestones)**
- Sequencer-side state roots and L1 settlement of roots.
- Cross-validator attestation / quorum / consensus.
- BAL-driven parallel (Block-STM) re-execution.
- Proof-serving RPC / light-client endpoints.
- Snapshot / fast-sync (tip-only catch-up). Milestone 1 is from-genesis only.

## 3. Key decisions (resolved during brainstorm)

| # | Decision | Choice |
|---|----------|--------|
| 1 | Re-execution model / BAL role | **Executor emits BALs now**; validator re-executes and cross-checks against them. |
| 2 | State root semantics | **Full Ethereum MPT root** (canonical secure-trie world state). |
| 3 | Shared-code shape | **Extract a `kardamom-engine` core crate** from `kardamom-executor`; both roles depend on it. |
| 4 | MPT build strategy | **Incremental persisted trie** over mdbx using `alloy-trie`. |
| 5 | Validator subscriptions | `tx_data`, `tx_ordering`, `tx_deposits`, `tx_receipts`, **+ BAL channel**. |
| 6 | Catch-up | **From genesis** only (no fast-sync). |
| 7 | Divergence response | **Fail-stop** (record + metric + halt). |

## 4. Architecture

```
        tx_data[ ]  tx_ordering  tx_deposits          (existing channels)
            │           │            │
            ▼           ▼            ▼
   ┌─────────────────────────────────────────────┐
   │              kardamom-engine                 │  role-agnostic
   │  readers + join → exec(execute_tx) → BlockSink trait
   │  + StateWriterQueue / StateWriterSignal seam (existing)
   └─────────────────────────────────────────────┘
          ▲                              ▲
   ExecutorSink                    ValidatorSink
   (kardamom-executor)             (kardamom-validator)
     publishes:                      builds: MPT root (kardamom-state trie)
       tx_receipts (per tx)          consumes: tx_receipts + BAL → cross-check
       BlockBoundary (per block)     submits: BlockDelta → own mdbx (trie-aware writer)
       BAL = BlockDelta (per block)  fail-stop on divergence
```

**Core insight:** a validator is an **executor replica** that, at block close,
builds an MPT root and cross-checks itself instead of publishing receipts. Same
channels in, same re-execution, different `BlockSink`.

### 4.1 Crate layout

- **`kardamom-engine`** (new, extracted from `kardamom-executor`): pure execution
  (`execute_tx`, `execute_deposit_tx`, `SnapshotRef`, `ExecEnv`), delta/write-set
  types and hashing (`WriteSet`, `PendingDelta`, `BlockDelta` accumulation),
  reader/join topology (M `tx_data` + `tx_ordering` + `tx_deposits` + buffers),
  the reader→exec→commit orchestration generalized over a `BlockSink` trait, the
  mdbx persistence bridges (`StateWriterQueue` / `StateWriterSignal` /
  `SnapshotSource`), and `EngineError`.
- **`kardamom-executor`** (slimmed): depends on `kardamom-engine`; keeps Aeron
  wiring, archive-replay recovery, and supplies `ExecutorSink` (now also publishes
  the BAL).
- **`kardamom-validator`** (new): binary `kardamom-validator`; same Aeron wiring,
  supplies `ValidatorSink` + a trie-aware writer; no receipt publication.
- **`kardamom-state`** (extended): new mdbx trie tables + a `StateRoot` updater
  (`alloy-trie`); a trie-aware writer variant the validator plugs into the engine.

### 4.2 The `BlockSink` seam

```rust
pub trait BlockSink: Send {
    fn on_tx(&mut self, idx: TxIndex, receipt: &Receipt, ws: &WriteSet)
        -> Result<(), EngineError>;
    fn on_block_close(&mut self, b: &BlockBoundary, delta: &BlockDelta)
        -> Result<(), EngineError>;
}
```

The engine owns the common path (readers, join, `execute_tx`, accumulate
`BlockDelta`, submit to `StateWriterQueue`, snapshot-swap). The sink supplies role
behaviour:

| hook | `ExecutorSink` | `ValidatorSink` |
|------|----------------|-----------------|
| `on_tx` | publish `Receipt` → `tx_receipts` | buffer subscribed `tx_receipts` by `tx_idx`; assert equality vs. recomputed receipt |
| `on_block_close` | publish `BlockBoundary`; publish `BlockDelta` as **BAL** | cross-check `BlockDelta` vs. subscribed BAL; record new root |
| state writer | plain `kardamom-state` writer | **trie-aware writer** (delta + trie nodes in one mdbx txn) |

The state-writer is the **existing** `StateWriterQueue` seam, so trie updates land
in the same atomic mdbx txn as the `BlockDelta` with no change to the engine loop.

## 5. The BAL channel

- The BAL payload is the existing **`BlockDelta`** wire type
  (`crates/types/src/delta.rs`): `block_number`, `accounts`, `storage`, `code`,
  `receipts` — already rkyv-serializable and already built by the executor for the
  state writer. "Emit a BAL" = publish it.
- **New dedicated channel** (`tx_bal`, default stream id allocated in
  `kardamom-log` config), so `tx_receipts` stays lean for ingress and validators
  subscribe to exactly what they need. Published from the executor's commit/writer
  path (separate AeronRuntime if needed to avoid back-pressure, mirroring the
  receipts-publication isolation).
- `kardamom-log` gains a `BalPublisher` and `BalSubscriber` (typed wrappers over
  `BlockDelta`), plus config entries (channel template + stream id) with IPC
  defaults and cluster UDP template.

## 6. State root — incremental persisted MPT

**Storage (new in `kardamom-state`):** two mdbx tables of hashed trie nodes keyed
by nibble path (reth layout):
- `account_trie` — world-state trie nodes (key `keccak(address)`)
- `storage_trie` — per-account storage trie nodes (key `account_hash ++ keccak(slot)`)

The existing `accounts.storage_root` field (today hard-coded `ZERO`) becomes
**populated** by this module.

**Per-block update (driven by the block write-set):**
1. For each account with storage changes: feed changed slots + a cursor over the
   stored storage-trie nodes into `alloy-trie`'s `HashBuilder` → new
   `storage_root`; persist touched nodes.
2. For each changed account: leaf = `RLP(nonce, balance, storage_root, code_hash)`;
   feed changed accounts + a cursor over stored account-trie nodes into
   `HashBuilder` → new **state root**; persist touched nodes.
3. Commit trie-node updates in the **same mdbx txn** as the `BlockDelta` (atomic;
   state + trie advance together, crash-consistent).

**`alloy-trie` provides:** node encoding, keccak/RLP hashing, `HashBuilder`.
**We build:** the persisted-node storage + the "walk only changed paths" cursor
(mirrors reth's state-root algorithm). Highest-effort, highest-risk component →
hard TDD (see §8).

**Performance:** off the hot path; incremental design keeps it ~O(writes/block)
rather than O(state), which is sufficient.

## 7. Data flow & lifecycle (validator)

1. **Bootstrap:** open own mdbx, `seed_genesis()`, read recovery point.
2. **Catch-up:** reuse engine archive-replay + skip-count to re-execute from block
   0 (or last committed block on restart) up to the tip.
3. **Live follow:** readers + join reconstruct the canonical ordered stream
   (identical to the executor, because it's the same `tx_ordering`).
4. **Execute:** `execute_tx` / `execute_deposit_tx` per record; accumulate
   `BlockDelta` + per-tx receipts.
5. **Block close (`ValidatorSink`):**
   - cross-check `BlockDelta` vs. subscribed BAL; cross-check per-tx receipts vs.
     subscribed `tx_receipts`;
   - submit `BlockDelta` to the trie-aware writer → atomic mdbx commit of state +
     trie nodes → new **state root**;
   - record root (metric + `meta` row keyed by block number).
6. **Divergence:** record block + diff, increment `validator_divergence_total`,
   log structured error, **halt**.

## 8. Testing

- **Regression:** existing `kardamom-executor` tests stay green through the engine
  extraction (the extraction is a behaviour-preserving move).
- **Trie unit/TDD:** empty trie root == `EMPTY_ROOT_HASH`; genesis state root vs.
  an independent reference; single-account / single-slot insert/update/delete;
  **incremental-vs-full-rebuild equivalence** over N blocks.
- **`BAL` round-trip:** publish `BlockDelta` → decode == submitted delta.
- **`ValidatorSink`:** inject mismatched BAL / receipt → assert halt + metric.
- **Integration:** in-process sequencer→sealer→executor→validator over IPC; run N
  blocks; assert the root advances and no false divergence.
- **Cluster smoke (goal):** extend the nomad/container cluster e2e to launch a
  validator alongside the cluster, drive sustained load, and assert:
  - the validator **syncs from genesis**,
  - it **keeps up** — committed-block lag behind the executor stays within a
    bounded threshold under sustained load,
  - state roots advance monotonically,
  - `validator_divergence_total == 0`.

## 9. Risks

- **Engine extraction churn** on a hot-path crate → mitigated by treating it as a
  pure move validated by the existing executor test suite.
- **MPT correctness** (extension nodes, RLP, secure-trie hashing) → mitigated by
  building on `alloy-trie` primitives + known-vector TDD + incremental-vs-rebuild
  equivalence.
- **BAL publication back-pressure** on the executor hot path → mitigated by a
  separate AeronRuntime, as with receipts.
- **Catch-up time from genesis** under a long history → acceptable for milestone 1
  (off hot path); fast-sync is a later milestone.

## 10. Milestone cut

**IN:** engine extraction; BAL publication; `kardamom-validator`; trie-aware
writer + MPT root; dual cross-check; fail-stop; from-genesis re-execution +
recovery; cluster smoke tests.

**OUT (later):** sequencer-side roots + L1 settlement; cross-validator attestation
/ quorum; BAL-driven parallel execution; proof RPC; snapshot / fast-sync.

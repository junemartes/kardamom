# `no_std` exec core — zk-provable execution, phase 1 (2026-08-04)

Goal of the series: generate zk validity proofs of kardamom's state
transition inside a batcher/validator node. A zkVM guest (SP1 / RISC Zero /
Jolt / OpenVM class) must link the **exact** execution code the live executor
and validator run — a reimplementation would be a second consensus
implementation to keep in lockstep forever. That requires the execution core
to compile `no_std` (+ `alloc`): no Aeron, no libmdbx, no tokio, no clocks,
no entropy, no OS.

Phase 1 (this spec, PR 1 of the series) is the pure-refactor extraction. No
behavior change, no proving yet.

## What moved

New crate `crates/exec-core` (`kardamom-exec-core`), `#![no_std]` +
`extern crate alloc`, holding the pure state-transition slice previously in
`kardamom-engine`:

| module | contents |
|---|---|
| `executor` | `ExecScope`, `execute_tx`, `execute_deposit_tx`, `SnapshotRef`/`SnapshotDb` revm adapters, `invalid_skip` (#92 semantics) |
| `delta` | `WriteSet` (sort-on-build canonical order + streamed keccak hash), `PendingDelta` |
| `block_env` | `ExecEnv` → deterministic revm `BlockEnv`/`CfgEnv` (invariant I3) |
| `bal_ladder` | EIP-7928 BAL quantization (`chunk_of`, `quantize`) |
| `exec_types` | `TxIndex`, `CMessage`, `ReceiptStatus` |
| `error` | `ExecutorError`/`EngineError` (pure data; transport-flavored variants stay because splitting the enum would ripple through every actor call site) |
| `state` (std-only) | `MockStateDatabase`, `StaticSnapshotSource`, `MutatingSnapshotSource` |
| `metrics` (std-only) | the invalid-tx-skip counter — emitted from inside `invalid_skip`, so it lives with it |

`kardamom-engine` re-exports all of it from its root (`pub use
kardamom_exec_core::{bal_ladder, block_env, delta, error, exec_types,
executor};` plus the flat item re-exports), so **no consumer changed a single
import path**. The engine keeps the orchestration: `actor`, `reader`,
`persist`, `replay`, `bin_support`, the metric namespace, and
`WriterApplyingQueue` (implements the actor's `StateWriterQueue` seam, so it
cannot live in the core).

`kardamom-types` is now `#![cfg_attr(not(feature = "std"), no_std)]` with a
default `std` feature. It was already pure data; the changes are `alloc`
imports, `core::error::Error` for the `StateError` supertrait, and feature
plumbing.

## `std` feature contract

`default = ["std"]` on both crates; engine consumers see identical behavior.
With `--no-default-features`:

- the `invalid_skip` tracing/metrics emission compiles out — the skip
  **receipt** (`status=false, gas_used=0`, empty write set) is the consensus
  artifact and is produced identically;
- the `state` mocks and `metrics` module vanish;
- everything else is the same code, byte-for-byte semantics.

Determinism note: the one `std::collections::HashMap` on the deposit-mint
path was replaced by a single-entry `once(…).collect()` into revm's own map
type — the exec core now has **zero** `RandomState` iteration anywhere.

## Dependency posture

Deps are declared directly (not `workspace = true`) in both crates because
workspace entries carry default features a member cannot subtract; version
requirements are kept in sync with the workspace root (single resolved copy,
enforced by `--locked` in CI). rkyv runs `no_std` as
`default-features = false, features = ["alloc", "bytecheck", "bytes-1"]`.

## CI gate

`ci.yml` job `no-std`: `cargo check -p kardamom-types -p kardamom-exec-core
--no-default-features --target riscv32imac-unknown-none-elf --locked`. A
bare-metal target is the only reliable gate — a host-target check with
`--no-default-features` still links std transitively and passes.

## Known gaps deferred to later phases

These are **soundness** items the proof must internalize, tracked here so
phase 1's "no behavior change" claim is explicit about what it did NOT do:

1. **KZG point-evaluation precompile (0x0A).** revm without `c-kzg` (a C
   library — unavailable in guest builds) omits 0x0A entirely, while the
   live engine build includes it, and `CfgEnv::default()` selects the latest
   spec (Cancun+, so 0x0A is active). The guest integration must pin the
   chain spec explicitly and either ship a pure-Rust kzg backend or spec
   0x0A out of the chain. Related: defaulting to revm's latest `SpecId`
   means a revm upgrade can silently change chain semantics — pinning
   deserves its own change with a regression test.
2. **Sender recovery.** `TxEnvelope.sender` is trusted from the proxy (S0/S1).
   A proof must ecrecover from the raw signature (zkVM secp256k1
   precompiles; alloy-consensus `k256` feature, not the C `secp256k1`).
3. **tx_hash.** Copied from the envelope (S0); the guest must recompute
   `keccak256(raw_tx)` and, for deposits, re-derive `source_hash` from L1
   data (`kardamom_types::epoch`).
4. **BLOCKHASH = zero.** `SnapshotRef::block_hash_ref` returns `B256::ZERO`
   (no ancestor cache). Consistent if the guest does the same, but once
   proven it is a consensus rule — document or fix before phase 3.

## Phase plan (series)

- **PR 1 (this)** — extraction + CI gate. Pure refactor.
- **PR 2 (delivered)** — stateless execution over a captured witness:
  - `kardamom-types::witness` — `ExecutionWitness` wire type (rkyv, sorted
    canonical order, keccak `digest()`), with EXPLICIT absence: proven-absent
    accounts (`exists = false`) and explicit zero slots. A key missing from
    the witness is an incompleteness error, never a default.
  - `kardamom-exec-core::witness` — `WitnessDb` (`no_std`, fail-closed
    `StateDatabase` over the witness) + `WitnessRecorder` (std, the
    validator-side collector: a first-touch recording decorator at the
    snapshot seam — `CacheDB` memoizes reads, so the snapshot sees exactly
    the pre-state slice). Empty-code hashes (`KECCAK_EMPTY`/`ZERO`) resolve
    structurally and never enter the witness.
  - `kardamom-validator::witness` — `capture_block_witness` /
    `reexecute_stateless` over the existing sequential driver; the state DB
    keeps its three-consumer rule (the batcher stays state-free).
  - `tests/stateless_reexec.rs` — the round-trip contract: transfer +
    contract call (code load, storage read/write) + deposit (mint,
    proven-absent recipient) captured and replayed from the witness alone;
    identical receipts/delta and post-state root via the pure trie oracle
    (`kardamom-state::{state_root, storage_root}`); witness minimality
    (untouched accounts never leak in); tampered witnesses fail closed.
  - Capture runs BELOW the parent/seed layers, so pipelined-commit parent
    reads surface as ordinary witness entries; per-batch capture at K > 1
    composes with claim seeds but the phase-2 contract is block-granular.
- **PR 3a (delivered)** — the `no_std` stateless block driver + in-guest
  soundness hardening:
  - `kardamom-exec-core::stateless` — `execute_block` (the single-scope
    sequential driver, hoisted verbatim from the validator; the validator's
    `execute_block_sequential` now DELEGATES here, so live re-execution and
    the guest link one code path by construction) and
    `execute_block_stateless` (the guest entry: identity verification +
    fail-closed `WitnessDb`). `BufferedRecord`/`BlockExecOutput` moved with
    it (re-exported from `engine::actor` at their old paths).
  - `verify_record_identity` closes the S0 trust boundary in-guest:
    `tx_hash = keccak256(raw_tx)` recomputed, `sender` recovered from the
    secp256k1 signature via pure-Rust k256 (`alloy-consensus/k256`, compiles
    on the riscv32 no_std gate). Forged hash/sender/signature aborts with
    `ExecutorError::RecordIdentity`. Deposit identity (`source_hash`) stays
    a trusted input until the witness is L1-anchored (derivation D/E).
  - Spec pinned: `CHAIN_SPEC = SpecId::OSAKA` set explicitly on `CfgEnv`
    (behavior-preserving — OSAKA is what `CfgEnv::default()` resolved to);
    a regression test flags any future revm-default drift. Gap 1 (0x0A KZG
    backend) remains open and documented on the constant.
  - BLOCKHASH-returns-zero elevated to a documented consensus rule at the
    single adapter every profile flows through.
- **PR 3b** — witness MPT anchoring: account/storage proofs against
  `pre_state_root`, absence proofs, sparse post-state-root recompute over
  `alloy-trie` in the guest.
- **PR 3c** — the zkVM guest program (SP1/RISC Zero) + async prover harness
  behind a flag; guest-build kzg decision (gap 1).
- **PR 4** — batch-boundary wiring: one proof per posted batch aligned with
  the live batcher's L1-as-truth cursor; L1 submission/verification.

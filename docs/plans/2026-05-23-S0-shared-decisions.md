# Shared Decisions Across Subsystem Plans

**Status:** Authoritative. Overrides any conflicting decision made in the individual S1–S7 plans.
**Date:** 2026-05-23
**Scope:** Cross-cutting decisions that emerged after parallel-drafting of the S1–S7 implementation plans surfaced conflicts.

This document is short by design. Each individual plan stays the source of truth for its own subsystem's tasks; this doc only resolves disagreements where two or more plans landed on different answers.

---

## D-Sh1: Crate layout

Three foundation crates that everything else depends on:

- **`crates/kardamom-types`** — pure data types and trait definitions. No I/O. No dependencies on Aeron, libmdbx, alloy-provider, jsonrpsee, etc. Owns:
  - `BPosition { term_id: i32, term_offset: i32 }` + ordering impls
  - `TxEnvelope { correlation_id: u64, raw_tx: Bytes }`
  - `Receipt { tx_idx: BPosition, status: bool, gas_used: u64, logs: Vec<Log>, write_set_hash: B256 }`
  - `BlockBoundaryStart { block_number: u64, end_tx_idx: BPosition, l2_timestamp: u64 }`
  - `BlockBoundary { block_number, end_tx_idx, l2_timestamp, state_root_commitment: B256 }`
  - `FsyncWatermark { recorder_id: u8, position: BPosition }`
  - `QuorumWatermark { position: BPosition }`
  - `CachedReceipt { sender: Address, nonce: u64, tx_hash: B256, receipt: Receipt }` — receipt-cache message
  - `BlockDelta` — the block-write payload from executor to state writer (account/storage/code changes + receipts)
  - `StateDatabase` trait — `revm::Database`-compatible read interface with snapshot semantics
  - `SnapshotSource` trait — gives executor a fresh post-block snapshot when state writer signals
- **`crates/kardamom-log`** — Aeron channel implementations + recorder + fsync sidecar + quorum watermark aggregator + receipt-cache channel. Depends on `kardamom-types`. Defines no new wire types — all messages come from `kardamom-types`.
- **`crates/kardamom-leases`** — lease primitive used by sequencer hot-standby (S2), sealer leader election (S5), and L1 batcher leader election (S7). V0 impl: deterministic lowest-host-id-among-caught-up-recorders, derived from per-recorder `FsyncWatermark` streams. No external KV. Future versions may add an Aeron Cluster backend.

**Supersedes:**
- S6's plan to put `StateDatabase` in `kardamom-types` is **adopted**.
- S4's plan to put `StateDatabase` in `crates/kardamom-executor/src/state.rs` is **overridden**.
- S3's plan to define all shared message types in `kardamom-log` is **modified**: data types move to `kardamom-types`; `kardamom-log` only owns the channel implementations.
- S4's locally-stubbed types are **deleted from the plan** — import from `kardamom-types` instead.
- S6's locally-stubbed `Receipt` / `BlockBoundary` are **deleted from the plan** — import from `kardamom-types`.
- S7's plan to hoist a `kardamom-leases` crate from a side-effect of its own work is **promoted to a first-class crate**, listed here, and shared with S2 and S5.

## D-Sh2: Wire codec

`bincode` v2 with fixed-int encoding for all Aeron-channel messages. Chosen by S3, adopted globally. `kardamom-types` owns the `serde::Serialize`/`Deserialize` impls; `kardamom-log` adds the framing layer. Other crates do not need to import `bincode`.

## D-Sh3: Sender recovery and trust

The **proxy** (S1) recovers `sender` from the secp256k1 signature during batched verification and caches it in `TxEnvelope` (add field: `cached_sender: Option<Address>`). The **sequencer** (S2) trusts this cached value (CFT model — proxy is operator-controlled) and falls back to `recover_signer()` only when absent (B-replay, test paths).

This keeps the §3 sequencer nonce-check budget tight (no secp256k1 in the hot path).

**Update to S2 plan:** include a `--paranoid-sender-check` CLI flag that probabilistically re-verifies 1-in-N senders for production debugging. Default off.

## D-Sh4: `eth_getTransactionReceipt` by hash

V0 must support lookup by tx hash (Ethereum tooling depends on it). Resolution:

- **Executor** (S4): emit `tx_hash` as part of `Receipt` (compute via `keccak256(raw_tx)` once during execution; cheap, deterministic). Add `tx_hash: B256` to `Receipt` in `kardamom-types`.
- **State writer** (S6): maintain a new libmdbx table `tx_hash_index: tx_hash → BPosition`. Populated during block commit.
- **Proxy** (S1): `eth_getTransactionReceipt(hash)` queries this index via a `StateDatabase` read; if found, follows up with the `Receipt` lookup. Returns `None` if not yet committed.

**Updates required:**
- `Receipt` in S0 gets `tx_hash: B256` (added above).
- S4 plan: include `tx_hash = keccak256(raw_tx)` computation in the receipt-emit step.
- S6 plan: add `tx_hash_index` table to the schema; populate in block-commit task.
- S1 plan: remove the v0-returns-`None` stub for `eth_getTransactionReceipt(hash)`; implement via state DB lookup.

## D-Sh5: `eth_blockNumber` for the proxy

S1 v0 currently returns `U256::ZERO`. Replace with: proxy subscribes to channel C; tracks the highest `BlockBoundary.block_number` it has seen; serves that from `eth_blockNumber`. Simple addition to S1.

## D-Sh6: Branch and PR strategy

- **PR #12** (current) stays scoped to the system spec + this set of plans. After PR #12 merges, each component PR is implementation-only (the plan is already on `main` for context).
- Each component branches from `main` (post-merge) and opens its own PR. Component branch names already chosen in each plan: `claude/s1-ingress-proxy`, etc.
- Each component PR may include light edits to its own plan (clarifications surfaced during implementation), but should not edit other plans.

## D-Sh7: Implementation order

Per the spec §"Decomposition into implementation specs," the critical path is:

1. **S3 canonical log** + the three foundation crates (`kardamom-types`, `kardamom-log`, `kardamom-leases`) — first PR.
2. **S1 ingress proxy** — second; only depends on the foundation crates.
3. **S2 sequencer** — third; needs S3 channels working in real Aeron.
4. **S4 v0 sequential executor** — fourth; needs S3 + the `StateDatabase` trait from `kardamom-types`. Uses in-memory `StateDatabase` impl until S6 lands.
5. **S5 block sealer** — fifth; needs S3.
6. **S6 state writer** — sixth; provides the libmdbx `StateDatabase` impl. After this, S4 swaps from in-memory to libmdbx-backed.
7. **S7 L1 batcher** — seventh; needs S3 archive replay + S5 boundaries on C.

Parallelism opportunities: S2/S1, S5/S6 can be drafted concurrently after S3 + S4 are merged.

## D-Sh8: Test-infrastructure crate

The 7 plans each propose a "mock log" helper for their tests. To avoid duplication, **`kardamom-log` ships a `testing` feature** that exposes in-memory pub/sub fakes with the same trait surface as the real Aeron-backed channels. Every other crate's tests import `kardamom-log` with `features = ["testing"]` as a `dev-dependency`.

**Update to S3 plan:** add a task for the `testing` feature module with in-memory channel fakes covering `Publication`, `Subscription`, `ConcurrentPublication`, and `FsyncWatermark` streams.

## D-Sh9: Open questions deferred

Items raised in multiple plans that are not blockers but need follow-up:

1. **Aeron binary distribution** (S3): vendor / Docker / install script — operator-facing concern; pick at first deployment.
2. **State-root commitment v1** (S4): delta-hash is v0; MPT or incremental commitment for v1 needs its own spike.
3. **PTP vs NTP for sealer wall-clock** (S5): NTP+chrony fine for v0 (250ms blocks tolerate ms-scale skew); revisit for tighter cadence.
4. **libmdbx license** (S6): `libmdbx = "0.6"` is MPL-2.0. If the workspace standardizes on MIT/Apache, switch to `signet-libmdbx = "0.8"`. Default: stay with `libmdbx`.
5. **Posting cadence under L1 stall** (S7): no replacement-by-fee in v0; hot standby retakes lease and reposts. Acceptable; revisit after v0 stabilizes.

---

## How to use this document

When implementing any S-plan, **read S0 first.** If S0 conflicts with the S-plan, S0 wins. The implementer should update the S-plan inline (small PR or as part of the implementation PR) to reflect the resolved decision, so future readers don't get confused.

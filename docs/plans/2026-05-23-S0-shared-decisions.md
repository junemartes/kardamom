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
  - `TxEnvelope { correlation_id: u64, raw_tx: Bytes, sender: Address, tx_hash: B256 }` — `sender` and `tx_hash` are *always* populated by the proxy (S1) at decode time; never `Option`. Downstream consumers trust both fields unconditionally (CFT — proxy is operator-controlled).
  - `Receipt { tx_idx: BPosition, tx_hash: B256, status: bool, gas_used: u64, logs: Vec<Log>, write_set_hash: B256 }` — `tx_hash` propagated from the envelope; executor copies, does not recompute.
  - `BlockBoundaryStart { block_number: u64, end_tx_idx: BPosition, l2_timestamp: u64 }`
  - `BlockBoundary { block_number, end_tx_idx, l2_timestamp }` — **no state-root commitment.** State-root attestation is a validator concern, deferred (D-Sh11).
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

`rkyv` v0.8 (zero-copy archival serialization) for all Aeron-channel messages. `kardamom-types` derives `rkyv::Archive`, `rkyv::Serialize`, `rkyv::Deserialize` on each wire type; `kardamom-log` reads `Archived<T>` zero-copy from Aeron buffers (no allocation, no decode pass) and only materializes to `T` when downstream callers need an owned value. Other crates do not need to import `rkyv` directly — they consume archived views via helpers exposed by `kardamom-log`.

**Overrides** the earlier (and now-deleted) `bincode` choice in the S3 plan. Justification: messages are read once per recipient on the hot path; zero-copy access eliminates the per-message decode allocation that would otherwise dominate after Aeron's IPC cost.

## D-Sh3: Sender recovery and trust

The **proxy** (S1) recovers `sender` from the secp256k1 signature during batched verification and writes it into `TxEnvelope.sender` (typed `Address`, **never `Option`**). The **sequencer** (S2), **executor** (S4), and every other downstream consumer trust this value unconditionally — no fallback, no re-verification. CFT model: proxy is operator-controlled, equally trusted as the rest of the pipeline.

This keeps the §3 sequencer nonce-check budget tight (no secp256k1 in the hot path) and removes a class of "what if sender is missing" branch logic from every consumer.

**Updates required:**
- S1 plan: proxy always populates `TxEnvelope.sender` post-verification; failure to recover = reject the tx at the proxy boundary (return RPC error before publishing).
- S2 plan: remove all `recover_signer()` fallback paths and the previously-proposed `--paranoid-sender-check` CLI flag. Sequencer reads `envelope.sender` directly.
- B-replay paths (executor cold-start, hot-standby tailers) get `sender` from the recorded `TxEnvelope` on B — the field is present because the proxy populated it before original publication.

## D-Sh4: `tx_hash` provenance and `eth_getTransactionReceipt` by hash

`tx_hash` is computed **by the proxy** (S1) at the same time it does sig verify — keccak256 over `raw_tx` is essentially free alongside ECDSA recovery, and produces a single canonical hash at the system boundary. It is written into `TxEnvelope.tx_hash` (typed `B256`, always populated) and from there propagates unchanged through B → executor → `Receipt.tx_hash` → C. **No other component recomputes it.**

`eth_getTransactionReceipt(hash)` lookup path:
- **State writer** (S6): maintain a libmdbx table `tx_hash_index: tx_hash → BPosition`. Populated during block commit (one entry per receipt).
- **Proxy** (S1): `eth_getTransactionReceipt(hash)` does `StateDatabase::get_tx_position(tx_hash)` then `StateDatabase::get_receipt(position)`. Returns `null` (JSON-RPC convention) if not yet committed.

**Updates required:**
- S1 plan: proxy computes `tx_hash = keccak256(raw_tx)` in the sig-verify path; writes it into the envelope before publishing. Implements `eth_getTransactionReceipt(hash)` via the state-DB lookup above (drop the v0 stub).
- S4 plan: **remove** any `tx_hash` computation from the executor — copy the field directly from the inbound `TxEnvelope` into the outgoing `Receipt`.
- S6 plan: add `tx_hash_index` table to the schema; populate in the block-commit task.

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
7. **S7 L1 batcher** — seventh; **can be developed and deployed independently of the live pipeline** (D-Sh10). Only blocker is S3 publishing recoverable Archive segment files on disk.

Parallelism opportunities: S2/S1, S5/S6 can be drafted concurrently after S3 + S4 are merged.

## D-Sh8: Test-infrastructure crate

The 7 plans each propose a "mock log" helper for their tests. To avoid duplication, **`kardamom-log` ships a `testing` feature** that exposes in-memory pub/sub fakes with the same trait surface as the real Aeron-backed channels. Every other crate's tests import `kardamom-log` with `features = ["testing"]` as a `dev-dependency`.

**Mock vs. real Aeron — strict rule:**
- **Unit tests** may use the `testing` feature's in-memory fakes. This is the right tool for testing one component's logic in isolation; running real Aeron for every unit test is too slow.
- **E2E tests MUST use a real Aeron backend running in Docker.** Mocks are not acceptable at the e2e layer. The Aeron Media Driver + Aeron Archive Java processes are containerized; the test harness spins up the containers, points the Rust code at them, runs the scenario, tears down. CI runs Docker e2e tests on every PR.

**Update to S3 plan:** add a task for the `testing` feature module with in-memory channel fakes covering `Publication`, `Subscription`, `ConcurrentPublication`, and `FsyncWatermark` streams. **Additionally** add a task for a `docker/aeron/` directory (Dockerfile or compose) that builds Media Driver + Archive containers, plus a `testcontainers`-based Rust harness (`crates/kardamom-log/tests/docker_e2e.rs`) that other crates re-export for their own e2e tests. CI workflow updated to run these tests.

**Update to every other plan (S1, S2, S4–S7):** the existing "Integration test" task that uses the mock log is fine, but each plan must also add an **`e2e` test task** that uses the real Aeron Docker harness from S3 and exercises the component end-to-end against a live channel.

## D-Sh10: L1 batcher is offline / archive-driven

The L1 batcher (S7) **does not query the live sequencer** for tx data. It reads from the **on-disk Aeron Archive segment files** (or via the Aeron Archive standard replay protocol, which is offline-friendly and does not back-pressure live publishers) on whichever recorder host(s) it has read access to.

Consequences:
- The batcher is **temporally decoupled** from the live pipeline. It can be down for hours; tx flow continues; when it comes back, it catches up by reading the archive.
- The batcher needs no Aeron *publication* infrastructure — it's a pure consumer of fsynced disk data plus an L1 RPC client. It can run on a separate host (or even outside the cluster, given archive file access).
- The batcher does **not** subscribe to channel C. All inputs it needs — raw `TxEnvelope` bytes and `BlockBoundaryStart` markers — live on channel B's archive (the sealer publishes boundaries onto B, not C, per spec §2.6).

**Overrides** the earlier plan that had the batcher subscribing to C and assuming a live `ChannelBArchive::replay_range` API in `kardamom-log`.

**Updates required:**
- S7 plan: source is **B archive only** (segment-file read or Aeron Archive replay protocol). No C subscription. No live coordination with the running sequencer.
- S3 plan: remove the proposed live "channel-B replay" API from `kardamom-log`. The Aeron Archive itself already exposes the replay protocol; S3 owns the recorder and segment files, not a custom replay API on top.

## D-Sh11: State root is **not** computed by the kardamom node

The spec originally had the executor compute a `state_root_commitment` and emit it in `BlockBoundary`, and the L1 batcher anchor that commitment on L1. **This is removed.**

Justification: state-root attestation is a validator concern, not a sequencer concern. The kardamom node is a *data producer*; finality and state-root commitments are produced by independent validators who replay the canonical log and post their own attestations. v0 ships data production only; the validator role is a separate, future subsystem.

Consequences:
- `BlockBoundary` no longer carries `state_root_commitment` (already removed from the type definition in D-Sh1).
- S4 executor does not compute a state root. Determinism is enforced per-tx via `Receipt.write_set_hash` (replicas producing different `write_set_hash` for the same `tx_idx` = panic, halt — unchanged).
- S6 state writer maintains state internally in libmdbx but does not surface a committed root.
- S7 L1 batcher posts raw txs + block metadata only — no state-root field in the L1 payload. The L1 settlement contract becomes a pure data-availability sink.
- The spec's mention of "validity/fraud-proof flow" in §2.8 is fully deferred — there is no v0 settlement story beyond data posting.

**Updates required:**
- Spec §2.4 (executor): remove state-root computation step.
- Spec §2.5 (channel C `BlockBoundary` message): remove `state_root_commitment` field.
- Spec §2.8 (L1 batcher): replace "L1 settlement contract holds state-root commitments" with "L1 settlement contract is a data-availability sink; state-root attestation is a separate validator subsystem (deferred)."
- S4 plan: remove `state_root_commitment` from the `BlockBoundary` it emits; remove the delta-hash / state-root computation task.
- S7 plan: remove state-root field from the L1 payload format; update the `KardamomL2Settlement.sol` design accordingly.
- This document's previous "delta-hash state root (MPT deferred to v1)" wording in D-Sh1/Sh9 is **withdrawn** — state root is not a v0 concern at all.

## D-Sh9: Open questions deferred

Items raised in multiple plans that are not blockers but need follow-up:

1. **Aeron binary distribution** (S3): vendor / Docker / install script — operator-facing concern; pick at first deployment.
2. **Validator subsystem design** (replaces former state-root v1 question): independent validators that replay the archive, compute state, and post attestations to L1 — entirely future-spec.
3. **PTP vs NTP for sealer wall-clock** (S5): NTP+chrony fine for v0 (250ms blocks tolerate ms-scale skew); revisit for tighter cadence.
4. **libmdbx license** (S6): `libmdbx = "0.6"` is MPL-2.0. If the workspace standardizes on MIT/Apache, switch to `signet-libmdbx = "0.8"`. Default: stay with `libmdbx`.
5. **Posting cadence under L1 stall** (S7): no replacement-by-fee in v0; hot standby retakes lease and reposts. Acceptable; revisit after v0 stabilizes.

---

## How to use this document

When implementing any S-plan, **read S0 first.** If S0 conflicts with the S-plan, S0 wins. The implementer should update the S-plan inline (small PR or as part of the implementation PR) to reflect the resolved decision, so future readers don't get confused.

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
  - Spec pinned: `SpecId::OSAKA` set explicitly on `CfgEnv`. This landed
    twice independently — this branch's `CHAIN_SPEC` and #165's W1b
    `block_env::SPEC_ID` (the stronger form: `CfgEnv::new_with_spec` so the
    gas table is built from the pin, full-struct-literal `BlockEnv`,
    `cfg_pinning` golden tests). The rebase adopted #165's `SPEC_ID` as the
    single constant. Gap 1 (0x0A KZG backend) remains open and documented
    on it.
  - BLOCKHASH-returns-zero elevated to a documented consensus rule at the
    single adapter every profile flows through.
  - **The BAL is a proof input**: `execute_block_stateless` takes the
    published frame's access list + granularity, re-derives its own through
    the SAME capture hooks the live executor publishes from
    (`execute_block_with_bal`), quantizes through the shared `bal_ladder`,
    and fail-stops on structural inequality (`ExecutorError::Divergence` —
    the same class the live validator halts on). `bal_commitment` =
    keccak256 of the canonical RLP (byte-identical to `BalFrame.bal_rlp`)
    is the public output an L1 verifier checks against the posted frame.
    One-code-path invariant: guest, validator sequential fallback, and the
    executor's published artifact all run the same monomorphized exec-core
    functions — the proof attests exactly what the validator validates.
- **PR 3a.1** — identity checks in the LIVE validator (the forged-sender
  blind spot): `ExecutorConfig::verify_record_identity` runs
  `exec_core::stateless::verify_record_identity` at record arrival in the
  engine actor (one seam covering the validator's streaming AND whole-block
  modes); the validator binary enables it unconditionally and classifies
  `ExecutorError::RecordIdentity` as an INTEGRITY halt (divergence latch →
  exit 2, the page-the-humans signal), not an availability restart. The
  executor keeps the flag off: after 3a.1 a forged envelope in the canonical
  stream cannot commit unnoticed — the validator halts with proof — so
  sequencer-side checking is defense-in-depth with a latency cost, a
  separate decision. Forged-envelope chaos test drives the real
  `Executor::run` pipeline with a signature-vs-sender forgery (the theft
  shape: envelope.sender = victim, signature by attacker) and asserts halt +
  latch with the flag on, and — the documented blind spot — a committed
  theft with it off.
- **PR 3b** — witness MPT anchoring: account/storage proofs against
  `pre_state_root`, absence proofs, sparse post-state-root recompute over
  `alloy-trie` in the guest. Design notes below.
- **PR 3c** — the zkVM guest program (SP1/RISC Zero) + async prover harness
  behind a flag; guest-build kzg decision (gap 1).
- **PR 4** — batch-boundary wiring: one proof per posted batch aligned with
  the live batcher's L1-as-truth cursor; L1 submission/verification.

## Phase 3b design — MPT-anchoring the witness (design notes, 2026-08-08)

Design only; no implementation yet. Status of the inputs it builds on:
`ExecutionWitness.pre_state_root: Option<B256>` already exists on the wire
type (phase 2 left the anchor point ready, currently unpopulated);
`kardamom-state::{state_root, storage_root}` is the pure full-trie oracle the
tests cross-check against; `alloy-trie` provides `no_std`
`proof::verify_proof` (inclusion AND exclusion) and is already a dependency
of the EEST runner's oracle path.

### What 3b must add

Today the witness is fail-closed but UNANCHORED: `WitnessDb` refuses reads
the witness doesn't carry, but nothing ties what it DOES carry to the chain's
actual pre-state — a prover could witness a fictional state and prove a
fictional (internally consistent) block. 3b makes the witness
self-authenticating against `pre_state_root`:

1. **Proof transport.** A companion `WitnessProofs` (account-trie proof nodes
   + per-account storage-trie proof nodes, deduplicated as a flat sorted
   node set — MPT proofs share prefixes heavily) rather than per-entry proof
   vectors. Keeps the phase-2 `ExecutionWitness` wire type intact; the
   digest gains nothing (proof nodes are recomputable commitments, not
   state).
2. **In-guest verification, before execution.** For every witness account:
   inclusion proof of `rlp(nonce, balance, storage_root, code_hash)` at
   `keccak(address)` in the account trie — or an EXCLUSION proof for
   `exists = false` entries. For every witness slot: inclusion/exclusion at
   `keccak(key)` under that account's proven `storage_root` (explicit-zero
   slots are exclusions). Code needs no proof: `keccak(bytes) == code_hash`
   and the code_hash is inside the proven account leaf. Any verification
   failure aborts before the first EVM step — same fail-closed philosophy as
   `WitnessDb`, one error class (`WitnessUnanchored`).
3. **Post-state root recompute (the open design question — see below).** The
   proof's public outputs become `(pre_state_root, post_state_root,
   bal_commitment, block_number)`: an inductive root chain from genesis,
   which is exactly the piece S0 deliberately never committed on the wire
   (BlockBoundary is slim). The L1 verifier contract holds the running root.

### The open question: sparse post-root recompute

The guest holds only the touched slice, so the full-trie oracle shape
(`state_root` over every account) is unavailable by construction. The
candidate approaches:

- **(a) Partial-trie recompute over the carried proof nodes — the current
  lean.** The union of all read/absence proof nodes forms a partial trie
  rooted at `pre_state_root`. Apply the block's delta to that partial trie
  and re-hash upward; the root is correct IFF every node the delta's writes
  restructure is present. Reads already carry their own paths (execution
  cannot write an account it never read under our capture — first-touch
  records reads), and exclusion proofs carry the insertion point for fresh
  keys. The gap is DELETION: a storage write to zero REMOVES the leaf, which
  can collapse a branch node into an extension — correct re-hashing then
  needs the collapsing SIBLING node, which is on no read path. (Account
  deletion is out of scope v0: selfdestruct is speccced out, and balance-0
  accounts don't leave the trie post-EIP-158 only if never created — treat
  account-delete as `Unsupported` and fail closed if one occurs. Note
  storage-zeroing does NOT structurally delete from the ACCOUNT trie — the
  account leaf's `storage_root` field just changes value — so collapse
  handling is a storage-trie-only concern.) So capture must be write-aware:
  the validator-side capturer must add the would-be-orphaned siblings to
  `WitnessProofs`. Deterministic superset, verified in-guest like every
  other node (hash-linked into the pre-root), so a malicious prover cannot
  smuggle state through it.

  **How capture finds the siblings — recompute-guided completion, not case
  enumeration (2026-08-09).** Hand-walking the delta for collapse shapes on
  the capture side would ENUMERATE the cases (branch→extension,
  branch→leaf, root collapse, multi-delete cascades under one branch…) in a
  second place — precisely where sparse implementations rot, and a
  capture-side miss is unfixable in-guest (the proof just fails for honest
  blocks). Instead, make the shared sparse-recompute function the single
  owner of that knowledge: it already knows exactly which node it needs the
  moment it needs one (`MissingNode(hash/path)`). Capture runs THE SAME
  monomorphized sparse recompute the guest will run, over the candidate
  node set, in a fixed-point loop: on `MissingNode`, fetch that node from
  the full trie, add it to the set, retry. Terminates (bounded by trie
  depth × writes), handles cascaded collapses for free, and yields
  completeness BY CONSTRUCTION — the capture-side recompute succeeding is
  the proof that the guest's identical recompute will. The collapse-case
  test matrix then only targets the one sparse-recompute implementation,
  and the randomized cross-check (below) covers capture and guest at once.
  Cost note: the loop re-enters the recompute per missing node (worst case
  one retry per deletion); if profiling ever cares, the recompute can
  return ALL missing nodes per pass instead of the first.

  Guest-side minimality stays a non-goal: extra hash-linked nodes in the
  set cannot alter the recomputed root (they are reachable-or-ignored,
  verified by hash on use); unreferenced junk only bloats witness bytes,
  which the existing witness-size metrics already watch.
- **(b) Port/depend on a sparse-trie implementation (reth's sparse trie).**
  Solves (a) generically but imports a large surface into the no_std
  boundary; reth's is not no_std today. Rejected for 3b unless (a)'s
  implementation complexity surprises us.
- **(c) Defer the post-root: prove only `bal_commitment` + delta digest.**
  Punts the root chain to L1-side reconstruction — reintroduces the trusted
  gap 3b exists to close. Rejected.

Property obligation either way: sparse recompute MUST equal the full oracle.
Randomized cross-check tests exercising the WHOLE pipeline shape — arbitrary
genesis + delta → fixed-point capture → in-guest-shape verify + sparse
recompute == `kardamom-state::state_root` of the post state — are the 3b
acceptance gate, with deletion-collapse cases (branch→extension,
branch→leaf, root collapse, cascaded multi-delete under one branch)
enumerated explicitly against the one sparse-recompute implementation —
that's where sparse implementations rot.

### Second open question: who computes `pre_state_root` live

The validator must POPULATE `pre_state_root` at capture time to hand the
prover an anchored witness. Computing the full root per block via the oracle
is O(state) — fine for tests and the current cluster scale, wrong
asymptotically. Options, to be decided at 3b implementation time against
measured cluster state sizes: incremental trie maintenance in the state DB
(the honest fix, real work), or full-oracle recompute behind a cadence knob
(prove every Nth block first; the batcher's proof-per-batch cursor in PR 4
already tolerates gaps). This decision gates NOTHING in the guest design —
the guest only consumes the root.

### Deferred alongside (unchanged by 3b)

- Deposit `source_hash` re-derivation stays trusted until the witness is
  L1-anchored (derivation phases D/E).
- Cluster-level forged-envelope chaos case (byzantine-ingress injection):
  3a.1's chaos test drives the real engine pipeline in-crate; a
  cluster-suite case needs an injection vector that can place a forged
  envelope on the live A-stream — either an env-gated forge hook in the
  ingress binary (precedent: none today; the chaos suite is process-level
  kill/wipe only) or a standalone Aeron publisher tool. Worth doing when the
  chaos suite next grows; requires a `CHAOS_CASES` edit in
  `cluster-e2e.yml`, which the bot cannot push (operator applies from a
  `docs/ci/` draft).

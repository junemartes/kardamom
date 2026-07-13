# Validator Nodes Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add validator nodes that follow the sequencer, re-execute every block through a shared `kardamom-engine` core, produce a full Ethereum MPT state root, and cross-check against the sequencer's receipts + a new per-block BAL — proven by cluster smoke tests.

**Architecture:** Extract a role-agnostic `kardamom-engine` from `kardamom-executor` exposing a `BlockSink` seam. The executor supplies `ExecutorSink` (publishes receipts + a new BAL = `BlockDelta`). A new `kardamom-validator` supplies `ValidatorSink` + a trie-aware state writer. `kardamom-state` gains an incremental persisted MPT (account + storage trie tables) built with `alloy-trie`.

**Tech Stack:** Rust (edition 2024), revm, alloy-trie, rkyv wire types, libmdbx (`kardamom-state`), Aeron (`kardamom-log`), nomad/container cluster e2e.

**Reference:** `docs/specs/2026-06-22-validator-node-design.md`

**Conventions for every task:** run `just check` (cargo check --workspace --all-features) and the relevant `cargo test -p <crate>` before each commit. Commit with `jj describe -m "<msg>"` then `jj new` to open the next change (this repo uses Jujutsu; the bookmark `claude/validator-nodes` is advanced before push).

---

## Phase 1 — Extract `kardamom-engine` (behaviour-preserving)

Goal: move the role-agnostic execution core into a new crate with **zero behaviour change**, proven by the existing executor test suite staying green.

### Task 1.1: Scaffold the engine crate

**Files:**
- Create: `crates/engine/Cargo.toml`
- Create: `crates/engine/src/lib.rs` (temporary re-export shim)
- Modify: `Cargo.toml` (workspace already globs `crates/*`)

- [ ] **Step 1:** Create `crates/engine/Cargo.toml` mirroring `crates/executor/Cargo.toml`'s `[dependencies]` (revm, alloy-*, rkyv, crossbeam-channel, dashmap, tracing, metrics, thiserror, `kardamom-types`, `kardamom-state`, `kardamom-log`), package name `kardamom-engine`. Copy the `[features]` block (notably `aeron-live`).
- [ ] **Step 2:** Add `kardamom-engine = { path = "crates/engine" }` to `[workspace.dependencies]` in root `Cargo.toml`.
- [ ] **Step 3:** Create `crates/engine/src/lib.rs` with `//! Role-agnostic execution engine.` and empty module list (filled next task).
- [ ] **Step 4:** Run `cargo check -p kardamom-engine`. Expected: PASS (empty crate).
- [ ] **Step 5:** Commit: `feat(engine): scaffold kardamom-engine crate`.

### Task 1.2: Move the pure-execution + reader modules

**Files (git move, content unchanged except `crate::` paths):**
- Move: `crates/executor/src/{executor.rs,block_env.rs,delta.rs,exec_types.rs,reader.rs,error.rs,persist.rs,metrics.rs}` → `crates/engine/src/`
- Move: `crates/executor/src/actor.rs` → `crates/engine/src/actor.rs`
- Move: `crates/executor/src/state.rs` (mock/test sources) → `crates/engine/src/state.rs`
- Modify: `crates/engine/src/lib.rs`, `crates/executor/src/lib.rs`

- [ ] **Step 1:** `jj` tracks moves automatically; physically move the files with the Bash `mv` then update `crates/engine/src/lib.rs` to declare `pub mod actor; pub mod block_env; pub mod delta; pub mod error; pub mod exec_types; pub mod executor; pub mod metrics; pub mod persist; pub mod reader; pub mod state;` and re-export the same symbols `crates/executor/src/lib.rs` previously did (copy lines 49–71 of the old executor lib verbatim, renaming the `ExecutorError` re-export to keep the name).
- [ ] **Step 2:** Rename the public error type alias: in `crates/engine/src/error.rs` keep the type as `ExecutorError` but add `pub type EngineError = ExecutorError;` at the bottom so new engine code can use `EngineError` while existing references keep compiling.
- [ ] **Step 3:** Replace `crates/executor/src/lib.rs` body with `pub use kardamom_engine::*;` plus the executor-only modules that remain (none yet — the binary stays). Keep the crate doc comment.
- [ ] **Step 4:** Update `crates/executor/Cargo.toml` to depend on `kardamom-engine = { workspace = true }`; remove deps now only used by moved modules (keep ones the binary still needs: `kardamom-log`, `clap`, `tokio`, etc.).
- [ ] **Step 5:** Update `crates/executor/src/bin/kardamom-executor.rs` imports from `kardamom_executor::...` (unchanged — the re-export keeps them valid) — verify no `crate::` internal paths leak.
- [ ] **Step 6:** Run `cargo check --workspace`. Fix path breakages (`crate::` → within-engine paths). Expected: PASS.
- [ ] **Step 7:** Run `cargo test -p kardamom-engine` and `cargo test -p kardamom-executor`. Expected: all previously-passing tests still pass (the moved `#[cfg(test)]` modules now run under the engine crate).
- [ ] **Step 8:** Commit: `refactor(engine): move execution core out of executor (no behaviour change)`.

### Task 1.3: Verify the extraction holds the line

- [ ] **Step 1:** Run `just check` (workspace, all features) and `just clippy`. Expected: PASS / no new warnings.
- [ ] **Step 2:** Run `cargo test -p kardamom-executor --test determinism` and `--test m_plus_one_join` (the determinism + join regression suites). Expected: PASS.
- [ ] **Step 3:** Commit any clippy fixups: `chore(engine): clippy after extraction`.

---

## Phase 2 — BAL channel (`BlockDelta` publication)

### Task 2.1: Add BAL channel config to `kardamom-log`

**Files:**
- Modify: `crates/log/src/config.rs` (add `tx_bal_channel` + `tx_bal_stream_id`, IPC default `aeron:ipc?alias=tx-bal`, stream id `1004`; add cluster UDP template fields)
- Modify: `deploy/cluster/config/channels.toml.tpl` (add `tx_bal_*` entries)
- Test: `crates/log/src/config.rs` `#[cfg(test)]`

- [ ] **Step 1: failing test** — add to config tests:
```rust
#[test]
fn tx_bal_defaults_present() {
    let c = LogConfig::default();
    assert_eq!(c.channels.tx_bal_stream_id, 1004);
    assert!(c.channels.tx_bal_channel.contains("tx-bal"));
}
```
- [ ] **Step 2:** Run `cargo test -p kardamom-log tx_bal_defaults_present`. Expected: FAIL (field missing).
- [ ] **Step 3:** Add `pub tx_bal_channel: String` and `pub tx_bal_stream_id: i32` to the channels config struct + defaults (mirror the `tx_receipts_*` pattern exactly, incl. serde defaults).
- [ ] **Step 4:** Run the test. Expected: PASS. Then `cargo test -p kardamom-log`. Expected: PASS.
- [ ] **Step 5:** Add `tx_bal_channel`/`tx_bal_stream_id` (+ UDP unicast endpoint like receipts) to `deploy/cluster/config/channels.toml.tpl`.
- [ ] **Step 6:** Commit: `feat(log): add tx_bal channel config`.

### Task 2.2: Add `BalPublisher` + `BalSubscriber`

**Files:**
- Modify: `crates/log/src/publisher.rs` (add `BalPublisher` over `BlockDelta`)
- Modify: `crates/log/src/subscriber.rs` (add `pub type BalSubscriber = TypedSubscriber<BlockDelta>;` + `Subscribers::bal()`)
- Test: `crates/log/tests/` round-trip (IPC or in-memory per existing log test pattern)

- [ ] **Step 1: failing test** — new `crates/log/tests/bal_roundtrip.rs`:
```rust
// Publish one BlockDelta over IPC, subscribe, assert decoded == published.
// Mirror the existing receipts/tx_data roundtrip test harness in this dir.
```
(Reuse the helper that boots an embedded media driver from the existing log integration tests; copy its setup.)
- [ ] **Step 2:** Run it. Expected: FAIL (`BalPublisher` undefined).
- [ ] **Step 3:** Implement `BalPublisher::open(aeron, channels)` and `publish(&BlockDelta) -> Result<BPosition, LogError>` modeled on `TxOrderingPublisher` (encode via `codec::encode::<BlockDelta>`). Add `BalSubscriber` type alias + `Subscribers::bal()`.
- [ ] **Step 4:** Run the test. Expected: PASS.
- [ ] **Step 5:** Commit: `feat(log): BalPublisher/BalSubscriber for BlockDelta`.

### Task 2.3: Executor publishes the BAL at block close

**Files:**
- Modify: `crates/engine/src/actor.rs` (add an optional `on_block_close` BAL hook — see Phase 4 trait; for now add a `BalPublication` trait param defaulting to a no-op)
- Modify: `crates/executor/src/bin/kardamom-executor.rs` (construct a `BalPublisher`, wire it; publish on its own AeronRuntime to avoid back-pressure, mirroring receipts)
- Test: `crates/engine/src/actor.rs` `#[cfg(test)]` with a fake BAL sink

- [ ] **Step 1: failing test** — in actor tests, drive 2 blocks through the in-memory pipeline with a `Vec`-collecting fake BAL publisher; assert it received one `BlockDelta` per block equal to the submitted delta.
- [ ] **Step 2:** Run it. Expected: FAIL.
- [ ] **Step 3:** Add a `BalPublication` trait (`fn publish_bal(&mut self, delta: &BlockDelta) -> Result<(), EngineError>`) and call it from the commit path right where the delta is submitted to the state writer. Provide a `NoBal` no-op impl as the default for callers that don't publish.
- [ ] **Step 4:** Run the test. Expected: PASS. Run `cargo test -p kardamom-engine`. Expected: PASS.
- [ ] **Step 5:** Wire the real `BalPublisher` in the executor binary (separate `AeronRuntime`). Run `just check`. Expected: PASS.
- [ ] **Step 6:** Commit: `feat(executor): publish per-block BAL on tx_bal`.

---

## Phase 3 — Incremental persisted MPT in `kardamom-state`

### Task 3.1: Trie node tables + schema

**Files:**
- Modify: `crates/state/src/schema.rs` (add `account_trie`, `storage_trie` tables; nibble-path keys → encoded `alloy_trie` nodes)
- Modify: `crates/state/Cargo.toml` (`alloy-trie = "..."`, `alloy-rlp`)
- Create: `crates/state/src/trie.rs`
- Modify: `crates/state/src/lib.rs` (`pub mod trie;`)

- [ ] **Step 1: failing test** — `crates/state/src/trie.rs` test:
```rust
#[test]
fn empty_state_root_is_canonical() {
    let dir = tempfile::tempdir().unwrap();
    let env = StateEnvBuilder::new(dir.path()).durability(Durability::SafeNoSync).open().unwrap();
    let root = StateTrie::open(&env).unwrap().root(&env).unwrap();
    assert_eq!(root, alloy_trie::EMPTY_ROOT_HASH);
}
```
- [ ] **Step 2:** Run it. Expected: FAIL (no `StateTrie`).
- [ ] **Step 3:** Add the two tables to schema; add `alloy-trie`/`alloy-rlp` deps; create `StateTrie` with `open` + `root` returning `EMPTY_ROOT_HASH` for an empty DB.
- [ ] **Step 4:** Run it. Expected: PASS.
- [ ] **Step 5:** Commit: `feat(state): trie node tables + empty-root`.

### Task 3.2: Apply a write-set → new root (single-block, full-rebuild reference)

**Files:**
- Modify: `crates/state/src/trie.rs` (add `apply(&BlockDelta) -> B256` and a `root_full_rebuild` reference path used only in tests)
- Test: `crates/state/src/trie.rs`

- [ ] **Step 1: failing tests** — known vectors:
```rust
#[test]
fn single_account_root_matches_reference() {
    // One account (addr, nonce, balance, empty code, empty storage).
    // Expected root computed independently via alloy_trie HashBuilder over
    // [(keccak(addr), rlp(account))]; assert apply() == that value.
}
#[test]
fn incremental_equals_full_rebuild_over_blocks() {
    // Apply 20 randomized blocks incrementally; after each, assert
    // StateTrie::root == root_full_rebuild(all accounts/storage so far).
}
```
- [ ] **Step 2:** Run. Expected: FAIL.
- [ ] **Step 3:** Implement `apply`: for each account with storage changes, update its storage trie (changed slots → `rlp(value)` leaves) via a persisted-node cursor + `HashBuilder` → `storage_root`; then update the account trie (`rlp(nonce, balance, storage_root, code_hash)` leaves) → state root. Persist touched nodes. Implement `root_full_rebuild` by walking all `accounts`/`storage` rows into fresh `HashBuilder`s.
- [ ] **Step 4:** Run. Expected: PASS. Add delete-path test (account/slot set to zero/removed) and make it pass.
- [ ] **Step 5:** Commit: `feat(state): incremental MPT apply with rebuild-equivalence tests`.

### Task 3.3: Trie-aware writer

**Files:**
- Modify: `crates/state/src/writer.rs` (add `StateWriter::spawn_with_trie(env)` / builder flag; in `apply()` call `StateTrie::apply` inside the same RW txn and store the root in `meta` keyed by block)
- Modify: `crates/state/src/meta.rs` (add `KEY_STATE_ROOT(block)` codec)
- Test: `crates/state/src/writer.rs`

- [ ] **Step 1: failing test** — spawn a trie-aware writer, submit 3 blocks, assert `meta` state-root for block 3 equals an independent full-rebuild root, and survives reopen.
- [ ] **Step 2:** Run. Expected: FAIL.
- [ ] **Step 3:** Thread an optional `StateTrie` through the writer; when enabled, after writing accounts/storage/code tables, call `trie.apply(delta)` in the same txn, write the root to `meta`. The plain `StateWriter::spawn` path is unchanged (executor untouched).
- [ ] **Step 4:** Run. Expected: PASS. Run `cargo test -p kardamom-state`. Expected: PASS.
- [ ] **Step 5:** Commit: `feat(state): trie-aware writer commits root atomically`.

---

## Phase 4 — `kardamom-validator`

### Task 4.1: `BlockSink` trait in the engine

**Files:**
- Modify: `crates/engine/src/actor.rs` (introduce `BlockSink`; refactor commit path to call `on_tx` / `on_block_close`; provide an `ExecutorSink` adapter wrapping the existing receipt+BAL publication so executor behaviour is identical)
- Test: `crates/engine/src/actor.rs`

- [ ] **Step 1: failing test** — a fake `BlockSink` records `on_tx`/`on_block_close` calls; drive 2 blocks; assert call counts + payloads match the executed txs/blocks.
- [ ] **Step 2:** Run. Expected: FAIL.
- [ ] **Step 3:** Define `BlockSink` (see spec §4.2). Make `Executor::run` generic over `S: BlockSink`. Implement `ExecutorSink` that calls the existing `TxReceiptsPublication` + `BalPublication`. Existing executor tests must still pass (they now construct an `ExecutorSink`).
- [ ] **Step 4:** Run `cargo test -p kardamom-engine -p kardamom-executor`. Expected: PASS.
- [ ] **Step 5:** Commit: `feat(engine): BlockSink seam + ExecutorSink`.

### Task 4.2: Scaffold the validator crate + `ValidatorSink`

**Files:**
- Create: `crates/validator/Cargo.toml`, `crates/validator/src/lib.rs`, `crates/validator/src/sink.rs`
- Test: `crates/validator/src/sink.rs`

- [ ] **Step 1: failing test** — `ValidatorSink::on_block_close` with a matching BAL returns Ok and records the root; with a mismatched BAL returns `EngineError`/sets a divergence flag.
```rust
#[test]
fn divergence_on_bal_mismatch_halts() {
    let mut sink = ValidatorSink::new_for_test(/* trie-aware writer + bal buffer */);
    sink.push_reference_bal(make_bal(/* differs in one storage slot */));
    let err = sink.on_block_close(&boundary(1), &computed_delta(1)).unwrap_err();
    assert!(sink.diverged());
}
```
- [ ] **Step 2:** Run. Expected: FAIL.
- [ ] **Step 3:** Implement `ValidatorSink`: holds the trie-aware `StateWriterQueue`/`Signal`, a buffer of subscribed BALs keyed by block, a buffer of subscribed receipts keyed by `tx_idx`. `on_tx` compares recomputed receipt vs. buffered `tx_receipts`. `on_block_close` compares recomputed `BlockDelta` vs. buffered BAL, submits to the writer, reads back the root, increments `validator_divergence_total` + returns error on mismatch (fail-stop).
- [ ] **Step 4:** Run. Expected: PASS.
- [ ] **Step 5:** Commit: `feat(validator): ValidatorSink with dual cross-check + fail-stop`.

### Task 4.3: Validator binary

**Files:**
- Create: `crates/validator/src/bin/kardamom-validator.rs`
- Modify: `crates/validator/Cargo.toml` (`[[bin]]`)

- [ ] **Step 1:** Build the binary by adapting `crates/executor/src/bin/kardamom-executor.rs`: same CLI (`--config`, `--log-config`, `--state-dir`, `--chain`, `--chain-id`, `--shards`, `--metrics-addr`), open own mdbx, `seed_genesis`, `read_recovery_point`, wire `tx_data[..]`+`tx_ordering`+`tx_deposits`+`tx_receipts`+`tx_bal` subscriptions, construct `ValidatorSink` with a `spawn_with_trie` writer, call `Executor::run(sink, ...)`. No receipt/BAL publication.
- [ ] **Step 2:** Run `just check`. Expected: PASS.
- [ ] **Step 3:** Add an in-process integration test `crates/validator/tests/follows_pipeline.rs`: boot sequencer→sealer→executor→validator over IPC, submit N txs, assert validator commits N-block state, root advances, `validator_divergence_total == 0`. (Reuse the executor's existing in-process e2e harness as the template.)
- [ ] **Step 4:** Run it. Expected: PASS.
- [ ] **Step 5:** Commit: `feat(validator): kardamom-validator binary + in-process e2e`.

---

## Phase 5 — Cluster smoke tests (the goal)

### Task 5.1: Add a validator service to the cluster topology

**Files:**
- Modify: `deploy/cluster/` job/topology (add a `kardamom-validator` allocation; subscribe it to the cluster channels incl. `tx_bal`)
- Modify: `deploy/cluster/docker/ci-service.Dockerfile` (build/include the validator binary)
- Modify: `scripts/ci-cluster.sh` (start the validator)

- [ ] **Step 1:** Add the validator to the cluster compose/nomad topology mirroring the executor service (own state volume, metrics port).
- [ ] **Step 2:** Ensure the validator binary is built into the CI service image.
- [ ] **Step 3:** Bring the cluster up locally (`just` cluster recipe / `scripts/ci-cluster.sh`); confirm the validator boots and its metrics endpoint is reachable.
- [ ] **Step 4:** Commit: `feat(cluster): run a validator node in the cluster topology`.

### Task 5.2: Smoke test — sync + keep-up

**Files:**
- Modify/Create: `crates/e2e/tests/` (add `validator_sync` cluster test alongside the existing pipeline e2e)

- [ ] **Step 1: test** — under sustained load (reuse the existing load driver), poll the validator's metrics:
  - validator committed-block number reaches the executor's within a bounded lag (e.g. ≤ N blocks) and **stays** bounded for the duration → "keeps up";
  - validator state root advances monotonically;
  - `validator_divergence_total == 0`;
  - final validator committed block ≥ target.
- [ ] **Step 2:** Run the cluster e2e locally. Expected: PASS. Tune the lag threshold from observed steady-state.
- [ ] **Step 3:** Commit: `test(e2e): validator syncs network + keeps up under load`.

### Task 5.3: Wire into CI

**Files:**
- Modify: existing cluster-e2e CI workflow **iff** it is not auto-discovered. NOTE: the bot cannot push `.github/workflows/*.yml`; if a workflow edit is required, stage it as `docs/ci/<name>.yml.draft` and ask the operator to move it. Prefer making the new test run under the existing `cluster-e2e` job with no workflow change.

- [ ] **Step 1:** Confirm whether the existing cluster-e2e job auto-runs the new test (likely yes if it runs the e2e crate's cluster tests). If yes, no workflow change.
- [ ] **Step 2:** If a workflow change is unavoidable, write `docs/ci/cluster-e2e-validator.yml.draft` and flag for the operator.
- [ ] **Step 3:** Commit: `ci: include validator smoke in cluster-e2e` (or the draft + note).

---

## Phase 6 — PR + green CI

- [ ] **Step 1:** `just check && just clippy && just test` locally green.
- [ ] **Step 2:** Advance the bookmark: `jj bookmark set claude/validator-nodes -r @-` (last non-empty change), `jj git push --bookmark claude/validator-nodes`.
- [ ] **Step 3:** `gh pr create` with a body summarizing the design (link the spec), the milestone cut, and the new cluster smoke test.
- [ ] **Step 4:** Poll `gh pr checks` until all green; fix failures and re-push before moving on (per repo policy: CI must pass every step).
- [ ] **Step 5:** Report the PR URL + check status.

---

## Self-review notes

- **Spec coverage:** engine extraction (P1), BAL emission (P2), MPT root (P3), validator + dual cross-check + fail-stop (P4), cluster sync/keep-up smoke (P5), PR+CI (P6) — all spec §3 decisions and §8 tests mapped.
- **Deposits:** validator subscribes to `tx_deposits` (Task 4.3) — required for faithful re-execution.
- **From-genesis catch-up:** reuses engine recovery/replay (Task 4.3); no fast-sync (out of scope).
- **Hot-path safety:** executor writer path untouched (trie is opt-in, Task 3.3); BAL publishes on a separate AeronRuntime (Task 2.3).

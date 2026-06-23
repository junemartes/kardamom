# Branch-Node-Incremental State Trie — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans (or subagent-driven-development) to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax.

**Goal:** Replace the validator's O(all-accounts)/block state-root rebuild with a node-incremental trie (stored `BranchNodeCompact` nodes + hashed-state mirror, `alloy-trie` `HashBuilder`), for both the account trie and per-account storage tries, with an opt-in shadow-check.

**Architecture:** reth's state-root model reimplemented on `alloy-trie` primitives in `crates/state/src/trie/`. The writer maintains a hashed-state mirror and stored branch nodes in the same atomic block txn; the root is computed by walking only the changed key-prefixes. Correctness is pinned by an incremental-vs-full-rebuild equivalence test and a runtime shadow-check.

**Tech Stack:** Rust 2024, `alloy-trie` 0.9 (`HashBuilder`, `BranchNodeCompact`, `Nibbles`), `alloy-rlp`, signet-libmdbx, the #63 validator/trie scaffolding.

**Reference:** `docs/specs/2026-06-23-incremental-trie-design.md`. **Stacks on** `claude/validator-nodes` (#63).

**Conventions:** TDD; `cargo test -p kardamom-state` + `cargo clippy -p kardamom-state` before each commit; `cargo fmt --all` before every commit (CI gates rustfmt). Commit with `jj describe` then `jj new`.

---

## Phase 1 — Storage model: tables + node codec

### Task 1.1: Add the four tables + bump schema version

**Files:** Modify `crates/state/src/schema.rs`, `crates/state/src/meta.rs`

- [ ] **Step 1:** Add table-name consts to `schema.rs`: `TABLE_ACCOUNT_TRIE = "account_trie"`, `TABLE_STORAGE_TRIE = "storage_trie"`, `TABLE_HASHED_ACCOUNTS = "hashed_accounts"`, `TABLE_HASHED_STORAGE = "hashed_storage"`; append all four to `ALL_TABLES`.
- [ ] **Step 2:** In `meta.rs` bump `SCHEMA_VERSION` from `1` to `2`.
- [ ] **Step 3:** `cargo test -p kardamom-state --lib schema` → PASS (existing schema tests unaffected). Commit: `feat(state): trie + hashed-state tables, schema v2`.

### Task 1.2: `BranchNodeCompact` mdbx codec

**Files:** Create `crates/state/src/trie/node.rs`; create `crates/state/src/trie/mod.rs` (`pub mod node;`); modify `crates/state/src/lib.rs` (replace `pub mod trie;` file with the new `trie/` dir — move the existing `trie.rs` body into `trie/mod.rs`).

- [ ] **Step 1: failing test** in `node.rs`:
```rust
#[cfg(test)]
mod tests {
    use super::*;
    use alloy_primitives::B256;
    use alloy_trie::{BranchNodeCompact, TrieMask};
    #[test]
    fn branch_node_roundtrips() {
        let n = BranchNodeCompact::new(
            TrieMask::new(0b1011), TrieMask::new(0b0010), TrieMask::new(0b1001),
            vec![B256::repeat_byte(0x11), B256::repeat_byte(0x22)],
            Some(B256::repeat_byte(0x33)),
        );
        let bytes = encode_branch_node(&n);
        assert_eq!(decode_branch_node(&bytes).unwrap(), n);
    }
}
```
- [ ] **Step 2:** Run `cargo test -p kardamom-state --lib node::` → FAIL.
- [ ] **Step 3:** Implement `encode_branch_node(&BranchNodeCompact) -> Vec<u8>` / `decode_branch_node(&[u8]) -> Result<BranchNodeCompact, StateError>`: fixed layout `state_mask(u16 BE) ++ tree_mask(u16 BE) ++ hash_mask(u16 BE) ++ has_root(u8) ++ [root_hash 32B if has_root] ++ hashes_len(u16 BE) ++ hashes(32B each)`. `TrieMask` exposes `.get()` (u16); reconstruct via `TrieMask::new(u16)`.
- [ ] **Step 4:** Run → PASS. Add a zero-hashes + no-root-hash case. `cargo fmt --all`. Commit: `feat(state): BranchNodeCompact mdbx codec`.

---

## Phase 2 — Cursors

### Task 2.1: `TrieCursor` over stored branch nodes

**Files:** Create `crates/state/src/trie/cursor.rs` (`pub mod cursor;` in `trie/mod.rs`)

- [ ] **Step 1: failing test** — open a fresh env, write two `account_trie` rows (paths `Nibbles::from_nibbles([0x1])` and `[0x1,0x2]`) via raw `txn.put`, then assert `TrieCursor::new(&txn, account_trie_db, None).seek(&Nibbles::from_nibbles([0x1])).unwrap()` returns the node at path `[0x1]`, and `next()` returns `[0x1,0x2]`.
- [ ] **Step 2:** Run → FAIL.
- [ ] **Step 3:** Implement `TrieCursor` wrapping an mdbx cursor: key = packed nibbles (`Nibbles::pack` → bytes; store with a 1-byte length prefix so `[0x1]` and `[0x1,0x0]` don't collide — `len(u8) ++ packed`). For storage tries, prepend `account_hash(32)` and range-scope. Methods: `seek(&Nibbles) -> Option<(Nibbles, BranchNodeCompact)>`, `next() -> Option<(Nibbles, BranchNodeCompact)>`, `get(&Nibbles) -> Option<BranchNodeCompact>` (exact).
- [ ] **Step 4:** Run → PASS. Add a storage-trie variant test (two accounts, assert range isolation). `cargo fmt --all`. Commit: `feat(state): TrieCursor over stored branch nodes`.

### Task 2.2: `HashedCursor` over leaves

**Files:** Modify `crates/state/src/trie/cursor.rs`

- [ ] **Step 1: failing test** — write three `hashed_accounts` rows (keys `keccak`-sized B256s out of order) via raw put; assert `HashedAccountCursor::new(&txn, db).seek(B256::ZERO)` then repeated `next()` yields them in ascending key order; decode each value to `AccountTrieParts`.
- [ ] **Step 2:** Run → FAIL.
- [ ] **Step 3:** Implement `HashedAccountCursor` (key `keccak(addr)` → `AccountTrieParts` encoded as `nonce(u64 BE) ++ balance(32B) ++ code_hash(32B) ++ storage_root(32B)`) and `HashedStorageCursor` (range-scoped by `account_hash`, key `keccak(slot)` → `U256` 32B). Methods `seek(B256)`, `next()`.
- [ ] **Step 4:** Run → PASS. `cargo fmt --all`. Commit: `feat(state): HashedCursor over the hashed-state mirror`.

---

## Phase 3 — Prefix set + walker + `StateRoot::incremental`

### Task 3.1: `PrefixSet`

**Files:** Create `crates/state/src/trie/prefix_set.rs`

- [ ] **Step 1: failing test** — `PrefixSet` built from `[keccak(addr_a), keccak(addr_b)]`; assert `contains_prefix(&Nibbles::from_nibbles(first nibble of addr_a))` is true and a nibble matching neither is false.
- [ ] **Step 2:** Run → FAIL.
- [ ] **Step 3:** Implement `PrefixSet { keys: Vec<Nibbles> /* sorted */ }` with `from_b256s(impl IntoIterator<Item=B256>)` (unpack each to `Nibbles`, sort, dedup) and `contains_prefix(&Nibbles) -> bool` (any stored key starts with the given prefix — binary search the sorted vec for the range). Also `is_empty()`.
- [ ] **Step 4:** Run → PASS. `cargo fmt --all`. Commit: `feat(state): PrefixSet`.

### Task 3.2: The walker + `StateRoot::incremental` (account + storage)

**Files:** Create `crates/state/src/trie/walker.rs`; extend `crates/state/src/trie/mod.rs` with `StateRoot` + `TrieUpdates`.

This is the correctness core. Build it against the equivalence oracle in Task 3.3 — implement the simplest correct walker first, then keep it green.

- [ ] **Step 1:** Define types in `mod.rs`:
```rust
pub struct TrieUpdates {
    pub upserts: alloy_trie::HashMap<alloy_trie::Nibbles, alloy_trie::BranchNodeCompact>,
    pub removals: Vec<alloy_trie::Nibbles>,
}
pub struct StateRoot;
```
- [ ] **Step 2:** Implement a generic `fn subtrie_root(prefix, prefix_set, trie_cursor, hashed_cursor, updates) -> B256`:
  - If `!prefix_set.contains_prefix(prefix)` and a stored node/hash exists for `prefix`: return the stored hash (no work).
  - Else rebuild via a fresh `HashBuilder::default().with_updates(true)`: walk the relevant hashed-leaf range for `prefix` from `hashed_cursor`, `add_leaf(key, value)` for each; for child subtries known-unchanged (stored node present, not in prefix set) `add_branch(child_path, stored_hash, true)` to skip; `root()` then fold `split()`'s map into `updates.upserts`. Track removed paths in `updates.removals`.
  - START SIMPLE: for the first green pass it is acceptable to rebuild a changed subtrie fully from its leaves (correct, coarser); refine the skip granularity afterward while keeping Task 3.3 green.
- [ ] **Step 3:** Implement `StateRoot::storage_root_incremental(txn, account_hash, prefix_set) -> (B256, TrieUpdates)` (storage trie for one account) and `StateRoot::state_root_incremental(txn, account_prefix_set) -> (B256, TrieUpdates)` (account trie, account leaves = `RLP(TrieAccount{nonce,balance,storage_root,code_hash})` from `hashed_accounts`).
- [ ] **Step 4:** `cargo fmt --all`. Commit: `feat(state): incremental trie walker + StateRoot`.

### Task 3.3: Incremental-vs-rebuild equivalence (the correctness gate)

**Files:** Test in `crates/state/src/trie/mod.rs`

- [ ] **Step 1: test** — a harness that maintains a model (`BTreeMap<B256, AccountTrieParts>` for hashed accounts, `BTreeMap<(B256,B256), U256>` for hashed storage) and a fresh env. For each of 30 deterministic pseudo-random blocks (seed the RNG by index, since `rand` is fine in tests): apply a mix of account inserts/updates/deletes and storage inserts/updates/zeroes; update the hashed-state tables + run `StateRoot::*_incremental` with the block's prefix sets; then assert the incremental account root equals `trie::state_root(model accounts with recomputed storage roots)` (the #63 rebuild oracle). Cover: account create, account delete (selfdestruct), storage-only change, slot zeroing (delete), and a single-child collapse (two keys sharing a long prefix, then delete one).
```rust
#[test]
fn incremental_equals_full_rebuild_over_random_blocks() { /* as above; 30 blocks */ }
```
- [ ] **Step 2:** Run `cargo test -p kardamom-state --lib incremental_equals_full_rebuild` → iterate the walker until PASS. This is where walker bugs surface; fix until green.
- [ ] **Step 3:** Add focused vectors: `empty_incremental_root_is_canonical` (no changes → `EMPTY_ROOT_HASH`); `single_account_then_delete_returns_empty`. Make them pass.
- [ ] **Step 4:** `cargo fmt --all`. Commit: `test(state): incremental-vs-rebuild equivalence (30 random blocks + vectors)`.

---

## Phase 4 — Writer integration + genesis + shadow-check

### Task 4.1: `TrieMode` + hashed-mirror maintenance in the writer

**Files:** Modify `crates/state/src/writer.rs`, `crates/state/src/lib.rs`

- [ ] **Step 1:** Add `pub enum TrieMode { Off, Incremental, ShadowCheck { every_n: u64 } }` (export from lib). Change `spawn_with_trie(env) -> spawn_with_trie(env, mode: TrieMode)`; the field `compute_trie: bool` becomes `trie_mode: TrieMode`. `spawn` (executor) passes `TrieMode::Off`.
- [ ] **Step 2:** Rewrite the writer's trie block (`apply`): when `trie_mode != Off`, after writing the raw `accounts`/`storage`, also (a) upsert/delete `hashed_accounts[keccak(addr)]` and `hashed_storage[account_hash ++ keccak(slot)]` for each change (zero slot ⇒ delete), (b) build prefix sets, (c) for each account with storage changes call `StateRoot::storage_root_incremental` and stamp the new `storage_root` into its `hashed_accounts` row + apply storage `TrieUpdates` to `storage_trie`, (d) call `StateRoot::state_root_incremental` and apply account `TrieUpdates` to `account_trie`, write `meta[KEY_STATE_ROOT]`. Apply `TrieUpdates` = upsert each node (`encode_branch_node`) + delete each removal path. The #63 per-account-`storage_root`-in-`accounts` logic is removed (storage_root now lives in `hashed_accounts`).
- [ ] **Step 3:** Keep the #63 `trie_writer_root_matches_model_and_persists` test (update it to `spawn_with_trie(env, TrieMode::Incremental)`); it must still pass (now via the incremental path). Run → PASS.
- [ ] **Step 4:** `cargo fmt --all`. Commit: `feat(state): writer maintains hashed mirror + incremental root`.

### Task 4.2: Extend `seed_genesis` for the hashed mirror + initial root

**Files:** Modify `crates/state/src/genesis.rs`

- [ ] **Step 1: failing test** — seed a 2-account genesis with a trie env, then open a `StateSnapshot` and assert `state_root()` equals `trie::state_root` over those two accounts (so a from-genesis validator starts at the correct root before block 1).
- [ ] **Step 2:** Run → FAIL.
- [ ] **Step 3:** In `seed_genesis`, when seeding accounts, also populate `hashed_accounts` and run one `StateRoot::state_root_incremental` over the all-accounts prefix set to build `account_trie` + write `meta[KEY_STATE_ROOT]`. (Genesis has no storage in v0 allocs; storage tries start empty.) Gate this on the trie tables existing (always, at schema v2).
- [ ] **Step 4:** Run → PASS. `cargo fmt --all`. Commit: `feat(state): seed_genesis builds the initial hashed mirror + root`.

### Task 4.3: Shadow-check

**Files:** Modify `crates/state/src/writer.rs`; add `StateError::ShadowMismatch`

- [ ] **Step 1: failing test** — spawn a writer in `ShadowCheck { every_n: 1 }`, submit a block, and assert it commits (roots agree). Then, via a test-only hook (`#[cfg(test)] fn force_wrong_root`), inject a wrong incremental root and assert the writer returns `StateError::ShadowMismatch` (the writer thread halts).
- [ ] **Step 2:** Run → FAIL.
- [ ] **Step 3:** In the writer, when `ShadowCheck { every_n }` and `block % every_n == 0`: after the incremental root, compute the full rebuild root (`trie::state_root` over a scan of `hashed_accounts` with each account's `storage_root`), and if they differ return `Err(StateError::ShadowMismatch { block, incremental, rebuilt })` (halts the writer). Expose a count via a `metrics` counter `kardamom_state_trie_shadow_checks_total`.
- [ ] **Step 4:** Run → PASS. `cargo fmt --all`. Commit: `feat(state): opt-in trie shadow-check (fail-stop on mismatch)`.

---

## Phase 5 — Validator wiring

### Task 5.1: `--trie-shadow-check` flag + metric

**Files:** Modify `crates/validator/src/bin/kardamom-validator.rs`, `crates/validator/src/metrics.rs`

- [ ] **Step 1:** Add CLI `--trie-shadow-check <N>` (optional u64; absent ⇒ `TrieMode::Incremental`, present ⇒ `ShadowCheck { every_n: N }`, `N=1` = every block). Pass the resulting `TrieMode` to `StateWriter::spawn_with_trie(env, mode)`.
- [ ] **Step 2:** Add a validator metric `validator_trie_shadow_mismatch_total` and describe it (the writer returning `ShadowMismatch` already fail-stops the process; the metric is for observability — increment in the error path before exit).
- [ ] **Step 3:** `just check` → PASS. `cargo fmt --all`. Commit: `feat(validator): --trie-shadow-check flag`.

---

## Phase 6 — e2e smoke (the goal)

### Task 6.1: Validator cluster smoke with shadow-check

**Files:** Modify `crates/e2e/tests/multiprocess_e2e.rs`

- [ ] **Step 1:** Add a second validator-sync test (or parameterize the #63 one) `multiprocess_e2e_validator_incremental_trie_shadow_check`: identical to `multiprocess_e2e_validator_syncs_and_keeps_up` but pass `--trie-shadow-check 1` to the validator. Assert (as #63) it syncs from genesis, keeps up within the lag bound, `validator_divergence_total == 0`, **`validator_trie_shadow_mismatch_total == 0`**, and `validator_state_root_block` advances — proving the incremental trie matches the rebuild every block under real load.
- [ ] **Step 2:** Run locally with Docker: `cargo test -p e2e --features full-pipeline-e2e --test multiprocess_e2e -- --ignored --nocapture multiprocess_e2e_validator_incremental_trie_shadow_check` → PASS (tune nothing if it passes; a shadow mismatch or failure to keep up = real bug).
- [ ] **Step 3:** Add the CI step to `docs/ci/docker-e2e.yml.draft` (bot can't push `.github/workflows/*.yml`). `cargo fmt --all`. Commit: `test(e2e): incremental-trie validator shadow-check smoke`.

---

## Phase 7 — PR + green CI

- [ ] **Step 1:** `just clippy` (all-features `-D warnings`) + `just test` green locally. `cargo fmt --all --check` clean.
- [ ] **Step 2:** `jj bookmark set claude/incremental-trie -r @`; `jj git push --bookmark claude/incremental-trie`.
- [ ] **Step 3:** `gh pr create --base claude/validator-nodes` (stacked PR onto #63) summarizing the design + linking the spec; flag the CI draft for the operator.
- [ ] **Step 4:** Poll `gh pr checks` until green; fix failures before declaring done.

---

## Self-review

- **Spec coverage:** tables+mirror (P1/4), node codec (P1), cursors (P2), prefix set+walker+StateRoot (P3), writer+genesis+shadow-check (P4), validator flag (P5), e2e smoke (P6), PR/CI (P7) — all spec §4–§9 mapped.
- **Correctness gate:** Task 3.3 equivalence (30 random blocks incl. delete/collapse/storage-only) + runtime shadow-check (P4.3) + e2e shadow-check (P6).
- **Granularity note:** Task 3.2 permits a coarser-but-correct first walker; the interface (`StateRoot::*_incremental` + `TrieUpdates`) and storage model are fixed, so finer skipping is an internal refinement guarded by the same equivalence test. Log/flag if the first landed walker is coarser than full-path-incremental.
- **Migration:** schema v2 + refuse old DB (P1.1) — fresh-from-genesis only, matches spec §8.
